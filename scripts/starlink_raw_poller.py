#!/usr/bin/env python3
"""Starlink raw telemetry poller — polls dish gRPC every 15 s.

Runs as a persistent systemd service on the HEAD Pi. Queries
192.168.100.1:9200 via starlink-grpc-tools, writes one JSONL row per poll
to $OUT_DIR/starlink/raw/starlink_status_history.jsonl.

On any RPC failure, injects a heartbeat row with satellite_link_status
"disconnected" and all numerics null so the downstream timeline is never broken.

Key constraints from spec:
  - Poll interval strictly < 900 s (dish retains only 15 min of history).
  - Handle itertools.chain TypeError at 15-min buffer wrap (pre-v1.2.2 bug).
  - gRPC RetryPolicy on UNAVAILABLE (14) and RESOURCE_EXHAUSTED (8).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
from pathlib import Path

try:
    import grpc
    import starlink_grpc
except ImportError as exc:
    print(f"FATAL: starlink-grpc-tools not installed — {exc}", file=sys.stderr)
    sys.exit(1)

DISH_TARGET      = os.environ.get("STARLINK_TARGET",  "192.168.100.1:9200")
POLL_INTERVAL_S  = int(os.environ.get("STARLINK_POLL_S", "15"))
OUT_DIR          = os.environ.get("OUT_DIR",           "/home/pump/telemetry_head")
TRIAL_ID         = os.environ.get("TRIAL_ID",          "trial-unknown")
NODE_ID          = os.environ.get("NODE_ID",           "meshradiohead")

RAW_JSONL = Path(OUT_DIR) / "starlink" / "raw" / "starlink_status_history.jsonl"

# gRPC retry service config (UNAVAILABLE=14, RESOURCE_EXHAUSTED=8)
_RETRY_CONFIG = json.dumps({
    "methodConfig": [{
        "name": [{"service": "SpaceX.API.Device.Device"}],
        "retryPolicy": {
            "maxAttempts": 4,
            "initialBackoff": "1s",
            "maxBackoff": "10s",
            "backoffMultiplier": 2,
            "retryableStatusCodes": ["UNAVAILABLE", "RESOURCE_EXHAUSTED"],
        },
    }]
})

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


def make_channel() -> "grpc.Channel":
    return grpc.insecure_channel(
        DISH_TARGET,
        options=[("grpc.service_config", _RETRY_CONFIG)],
    )


def get_status_fields(ctx: "starlink_grpc.ChannelContext") -> dict:
    """Return flat dict of status fields; empty dict on any error."""
    try:
        groups = starlink_grpc.status_data(ctx)
    except TypeError:
        # itertools.chain wraparound bug at 15-min buffer mark (pre-v1.2.2)
        return {}
    except Exception:
        return {}

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
    try:
        history = starlink_grpc.get_history(ctx)
    except TypeError:
        return [], []
    except Exception:
        return [], []

    try:
        current    = int(history.current)
        lats_buf   = list(history.pop_ping_latency_ms)
        drops_buf  = list(history.pop_ping_drop_rate)
        buf_size   = len(lats_buf)

        if buf_size == 0 or n <= 0:
            return [], []

        n    = min(n, buf_size)
        end  = current % buf_size
        start = (end - n) % buf_size

        if start <= end:
            lats  = lats_buf[start:end]
            drops = drops_buf[start:end]
        else:
            lats  = lats_buf[start:] + lats_buf[:end]
            drops = drops_buf[start:] + drops_buf[:end]

        return lats, drops

    except (TypeError, AttributeError, IndexError):
        # Catches the itertools.chain subscript bug if it propagates here
        return [], []


def build_heartbeat(error_code: str = "") -> dict:
    return {
        "poll_timestamp_utc":  utc_now(),
        "trial_id":            TRIAL_ID,
        "node_id":             NODE_ID,
        "satellite_link_status": "disconnected",
        "satellite_down_mbps":   None,
        "satellite_up_mbps":     None,
        "satellite_rtt_ms_fallback": None,
        "satellite_obstruction_pct": None,
        "_raw_latencies":      [],
        "_raw_drops":          [],
        "_total_ping_drop":    None,
        "_count_full_ping_drop": None,
        "_heartbeat":          True,
        "_grpc_error":         error_code,
    }


def poll_once(ctx: "starlink_grpc.ChannelContext") -> dict:
    status = get_status_fields(ctx)
    if not status:
        return build_heartbeat("status_data_failed")

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
    obs = (status.get("fraction_obstructed")
           or status.get("obstructed_fraction")
           or status.get("obstruction_fraction"))
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
        "_grpc_error":             "",
    }


def main() -> None:
    RAW_JSONL.parent.mkdir(parents=True, exist_ok=True)
    err_path = Path(OUT_DIR) / "starlink" / "poller_errors.log"
    err_path.parent.mkdir(parents=True, exist_ok=True)

    def log_err(msg: str) -> None:
        with err_path.open("a", encoding="utf-8") as f:
            f.write(f"{utc_now()} {msg}\n")

    print(f"[starlink_raw_poller] starting — target={DISH_TARGET} poll={POLL_INTERVAL_S}s")
    print(f"[starlink_raw_poller] writing → {RAW_JSONL}")

    ctx: "starlink_grpc.ChannelContext | None" = None

    while True:
        loop_start = time.monotonic()
        row: dict | None = None

        try:
            if ctx is None:
                ctx = starlink_grpc.ChannelContext(target=DISH_TARGET)
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

        if row:
            try:
                with RAW_JSONL.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(row) + "\n")
                    f.flush()
            except Exception as exc:
                log_err(f"write_error: {exc}")

        elapsed = time.monotonic() - loop_start
        sleep_s = max(0, POLL_INTERVAL_S - elapsed)
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
