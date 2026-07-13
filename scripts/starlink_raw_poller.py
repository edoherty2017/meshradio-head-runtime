#!/usr/bin/env python3
"""Starlink raw telemetry poller — polls dish gRPC every 15 s.

Runs as a persistent systemd service on the HEAD Pi. Queries
192.168.100.1:9200 via starlink-grpc-tools, writes one JSONL row per poll
to $OUT_DIR/starlink/raw/starlink_status_history.jsonl.

On any RPC failure, injects an explicitly unsuccessful heartbeat row with an
unknown link state and all numerics null. A failed measurement is never
relabelled as evidence that the dish was disconnected.

Key constraints from spec:
  - Poll interval strictly < 900 s (dish retains only 15 min of history).
  - Fail visibly on malformed/history RPC responses.
  - Bound each poll so a wedged RPC cannot stall the service indefinitely.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import signal
import sys
import time
from pathlib import Path

try:
    import grpc
    import starlink_grpc
    from starlink_chrony_qc import append_qc_row, sample_chrony
except ImportError as exc:
    print(f"FATAL: starlink-grpc-tools not installed — {exc}", file=sys.stderr)
    sys.exit(1)

DISH_TARGET      = os.environ.get("STARLINK_TARGET",  "192.168.100.1:9200")
POLL_INTERVAL_S  = int(os.environ.get("STARLINK_POLL_S", "15"))
RPC_TIMEOUT_S     = float(os.environ.get("STARLINK_RPC_TIMEOUT_S", "10"))
CHRONY_EVERY_POLLS = int(os.environ.get("CHRONY_SAMPLE_EVERY_POLLS", "4"))
OUT_DIR          = os.environ.get("OUT_DIR",           "/home/pump/telemetry_head")
TRIAL_ID         = os.environ.get("TRIAL_ID",          "trial-live")
NODE_ID          = os.environ.get("NODE_ID",           "meshradiohead2")

RAW_JSONL = Path(OUT_DIR) / "starlink" / "raw" / "starlink_status_history.jsonl"

if not 1 <= POLL_INTERVAL_S < 900:
    raise SystemExit("STARLINK_POLL_S must be between 1 and 899 seconds")
if not 0 < RPC_TIMEOUT_S < POLL_INTERVAL_S:
    raise SystemExit("STARLINK_RPC_TIMEOUT_S must be positive and less than STARLINK_POLL_S")
if CHRONY_EVERY_POLLS <= 0:
    raise SystemExit("CHRONY_SAMPLE_EVERY_POLLS must be greater than zero")

# State strings → normalized schema values
_DEGRADED_STATES  = {"OBSTRUCTED", "NO_PINGS"}
_DISCONN_STATES   = {"SEARCHING", "NO_SATS", "NO_DOWNLINK", "BOOT"}
_DEGRADED_ALERTS  = {
    "alert_thermal_throttle", "alert_mast_not_near_vertical",
    "thermalThrottle", "mastNotNearVertical",
    "alert_motors_stuck", "motorsStuck",
}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def map_link_status(state: str | None, alerts: dict) -> str:
    if state is None:
        return "unknown"
    s = str(state).upper().replace("STATE_", "")
    if s == "CONNECTED":
        if any(alerts.get(k) for k in _DEGRADED_ALERTS):
            return "degraded"
        return "connected"
    if s in _DEGRADED_STATES:
        return "degraded"
    if s in _DISCONN_STATES:
        return "disconnected"
    if "alert_" in state.lower():
        return "degraded"
    return "unknown"


def get_status_fields(ctx: "starlink_grpc.ChannelContext") -> dict:
    """Return flat status fields or raise; callers record the failure."""
    groups = starlink_grpc.status_data(ctx)

    merged: dict = {}
    for g in (groups if isinstance(groups, (list, tuple)) else [groups]):
        if g is None:
            continue
        if hasattr(g, "_asdict"):
            merged.update(g._asdict())
        elif isinstance(g, dict):
            merged.update(g)
    return merged


def get_history_samples(ctx: "starlink_grpc.ChannelContext", n: int) -> tuple[list, list]:
    """Return (latency_ms_list, drop_bool_list) for the last n seconds."""
    history = starlink_grpc.get_history(ctx)

    try:
        current    = int(history.current)
        lats_buf   = list(history.pop_ping_latency_ms)
        drops_buf  = list(history.pop_ping_drop_rate)
        buf_size   = len(lats_buf)

        if buf_size == 0 or n <= 0:
            return [], []
        if len(drops_buf) != buf_size:
            raise ValueError("Starlink latency/drop history buffers have different lengths")

        n    = min(n, buf_size)
        end  = current % buf_size
        start = (end - n) % buf_size

        if n == buf_size:
            lats  = lats_buf[end:] + lats_buf[:end]
            drops = drops_buf[end:] + drops_buf[:end]
        elif start < end:
            lats  = lats_buf[start:end]
            drops = drops_buf[start:end]
        else:
            lats  = lats_buf[start:] + lats_buf[:end]
            drops = drops_buf[start:] + drops_buf[:end]

        return lats, drops

    except (TypeError, AttributeError, IndexError) as exc:
        raise ValueError(f"invalid Starlink history response: {exc}") from exc


def build_heartbeat(error_code: str = "") -> dict:
    return {
        "poll_timestamp_utc":  utc_now(),
        "trial_id":            TRIAL_ID,
        "node_id":             NODE_ID,
        "satellite_link_status": "unknown",
        "satellite_down_mbps":   None,
        "satellite_up_mbps":     None,
        "satellite_rtt_ms_fallback": None,
        "satellite_obstruction_pct": None,
        "_raw_latencies":      [],
        "_raw_drops":          [],
        "_pop_ping_drop_rate": None,
        "_count_full_ping_drop": None,
        "_heartbeat":          True,
        "_measurement_status": "error",
        "_poll_interval_s":    POLL_INTERVAL_S,
        "_grpc_error":         error_code,
    }


def poll_once(ctx: "starlink_grpc.ChannelContext") -> dict:
    status = get_status_fields(ctx)
    if not status:
        raise ValueError("status_data returned no usable fields")

    # Connection state
    raw_state = status.get("state") or status.get("dish_state")
    alerts = {k: v for k, v in status.items() if "alert" in str(k).lower() and v}
    link_status = map_link_status(str(raw_state) if raw_state is not None else None, alerts)

    # Throughput (bps → Mbps)
    dl_bps = status.get("downlink_throughput_bps")
    ul_bps = status.get("uplink_throughput_bps")
    down_mbps = round(float(dl_bps) / 1e6, 4) if dl_bps is not None else None
    up_mbps   = round(float(ul_bps) / 1e6, 4) if ul_bps is not None else None

    # Latency fallback (p50 will be computed by aggregator from raw samples)
    pop_latency = status.get("pop_ping_latency_ms")
    rtt_fallback = round(float(pop_latency), 2) if pop_latency is not None else None

    # Obstruction
    obs = next(
        (status.get(key) for key in (
            "fraction_obstructed", "obstructed_fraction", "obstruction_fraction"
        ) if status.get(key) is not None),
        None,
    )
    obstruction_pct = round(float(obs) * 100, 3) if obs is not None else None

    # Raw per-second history for percentile aggregation
    lats, drops = get_history_samples(ctx, POLL_INTERVAL_S)

    # Ping drop counters from status
    total_drop = status.get("pop_ping_drop_rate")  # float 0-1 rate
    full_drop  = status.get("count_full_ping_drop")

    return {
        "poll_timestamp_utc":      utc_now(),
        "trial_id":                TRIAL_ID,
        "node_id":                 NODE_ID,
        "satellite_link_status":   link_status,
        "satellite_down_mbps":     down_mbps,
        "satellite_up_mbps":       up_mbps,
        "satellite_rtt_ms_fallback": rtt_fallback,  # used by aggregator if raw unavailable
        "satellite_obstruction_pct": obstruction_pct,
        "_raw_latencies":          lats,
        "_raw_drops":              drops,
        "_pop_ping_drop_rate":     float(total_drop) if total_drop is not None else None,
        "_count_full_ping_drop":   int(full_drop) if full_drop is not None else None,
        "_heartbeat":              False,
        "_measurement_status":     "ok",
        "_poll_interval_s":        POLL_INTERVAL_S,
        "_grpc_error":             "",
    }


class RpcDeadline:
    """POSIX wall-clock deadline for starlink-grpc-tools calls."""

    def __enter__(self):
        def timeout_handler(_signum, _frame):
            raise TimeoutError(f"Starlink RPC exceeded {RPC_TIMEOUT_S}s")

        self._previous = signal.signal(signal.SIGALRM, timeout_handler)
        signal.setitimer(signal.ITIMER_REAL, RPC_TIMEOUT_S)
        return self

    def __exit__(self, exc_type, exc, tb):
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, self._previous)
        return False


def main() -> None:
    RAW_JSONL.parent.mkdir(parents=True, exist_ok=True)
    err_path = Path(OUT_DIR) / "starlink" / "poller_errors.log"
    err_path.parent.mkdir(parents=True, exist_ok=True)

    def log_err(msg: str) -> None:
        with err_path.open("a", encoding="utf-8") as f:
            f.write(f"{utc_now()} {msg.replace(chr(10), ' ').replace(chr(13), ' ')}\n")
            f.flush()
            os.fsync(f.fileno())

    print(f"[starlink_raw_poller] starting — target={DISH_TARGET} poll={POLL_INTERVAL_S}s")
    print(f"[starlink_raw_poller] writing → {RAW_JSONL}")

    ctx: "starlink_grpc.ChannelContext | None" = None
    poll_number = 0
    clock_sample: dict = {}

    while True:
        loop_start = time.monotonic()
        row: dict | None = None
        poll_number += 1

        try:
            if ctx is None:
                ctx = starlink_grpc.ChannelContext(target=DISH_TARGET)
            with RpcDeadline():
                row = poll_once(ctx)
        except grpc.RpcError as exc:
            code = exc.code().name if hasattr(exc, "code") else "RPC_ERROR"
            log_err(f"grpc {code}: {exc.details() if hasattr(exc, 'details') else exc}")
            row = build_heartbeat(code)
            # Recreate channel on next iteration
            try:
                if ctx:
                    ctx.close()
            except Exception:
                pass
            ctx = None
        except Exception as exc:
            log_err(f"{type(exc).__name__}: {exc}")
            row = build_heartbeat(type(exc).__name__)
            try:
                if ctx:
                    ctx.close()
            except Exception:
                pass
            ctx = None

        if row:
            try:
                if poll_number == 1 or poll_number % CHRONY_EVERY_POLLS == 0:
                    clock_sample = sample_chrony()
                    append_qc_row(clock_sample)
                row["_clock_ok"] = clock_sample.get("clock_ok")
                row["_clock_error_bound_s"] = clock_sample.get("clock_error_bound_s")
                row["_clock_sampled_at_utc"] = clock_sample.get("sampled_at_utc")
                row["_clock_error"] = clock_sample.get("error", "")
                with RAW_JSONL.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(row, allow_nan=False, separators=(",", ":")) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            except Exception as exc:
                log_err(f"write_error: {exc}")

        elapsed = time.monotonic() - loop_start
        sleep_s = max(0, POLL_INTERVAL_S - elapsed)
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
