#!/usr/bin/env python3
"""Starlink 60-second tumbling window aggregator.

Reads raw JSONL from starlink_raw_poller.py and emits one derived row per
60-second polling batch to $OUT_DIR/starlink/derived/starlink_window_metrics.jsonl.
The output records both the 60-second target and the actual first-to-last poll
span; the latter is normally about 45 seconds for four 15-second polls whose
history arrays collectively cover the target interval.

Field derivations (per spec):
  satellite_link_status   : majority vote across window rows
  satellite_down_mbps     : mean of non-null values
  satellite_up_mbps       : mean of non-null values
  satellite_rtt_ms_p50    : p50 of valid (non-dropped) latency samples;
                            falls back to mean of pop_ping_latency_ms if no raw
  satellite_rtt_ms_p95    : p95 of valid samples; null if unavailable —
                            NEVER interpolated from mean
  satellite_packet_loss_pct: mean per-second drop fraction × 100
  satellite_obstruction_pct: mean of non-null obstruction values
  satellite_outage_seconds : count of seconds where drop==1 or status==disconnected
"""
from __future__ import annotations

import datetime as dt
import json
import math
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

OUT_DIR   = os.environ.get("OUT_DIR",  "/home/pump/telemetry_head")
TRIAL_ID  = os.environ.get("TRIAL_ID", "trial-live")
NODE_ID   = os.environ.get("NODE_ID",  "meshradiohead2")

WINDOW_S      = 60
TAIL_INTERVAL = 5   # seconds between file tail checks

RAW_JSONL     = Path(OUT_DIR) / "starlink" / "raw"     / "starlink_status_history.jsonl"
DERIVED_JSONL = Path(OUT_DIR) / "starlink" / "derived" / "starlink_window_metrics.jsonl"
STATE_JSON    = Path(OUT_DIR) / "starlink" / "qc" / "aggregator_state.json"


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def parse_ts(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    try:
        if not isinstance(s, str) or not (s.endswith("Z") or s.endswith("+00:00")):
            return None
        parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
            return None
        return parsed.astimezone(dt.timezone.utc)
    except Exception:
        return None


def majority_status(statuses: list[str]) -> str:
    if not statuses:
        return "unknown"
    counts: dict[str, int] = {}
    for s in statuses:
        counts[s] = counts.get(s, 0) + 1
    # Deterministic conservative tie-break; dict insertion order must not decide
    # the reported state.
    priority = {"disconnected": 0, "degraded": 1, "connected": 2, "unknown": 3}
    return max(counts, key=lambda k: (counts[k], -priority.get(k, 3)))


def aggregate_window(rows: list[dict]) -> dict:
    """Compute all satellite_* fields from a list of raw poll rows."""
    if not rows:
        raise ValueError("cannot aggregate an empty Starlink window")
    trial_ids = {row.get("trial_id") for row in rows}
    node_ids = {row.get("node_id") for row in rows}
    if trial_ids != {TRIAL_ID} or node_ids != {NODE_ID}:
        raise ValueError(
            f"raw Starlink identity mismatch: trials={trial_ids!r} nodes={node_ids!r}"
        )
    first_ts = parse_ts(rows[0].get("poll_timestamp_utc"))
    last_ts = parse_ts(rows[-1].get("poll_timestamp_utc"))
    if first_ts is None or last_ts is None or last_ts < first_ts:
        raise ValueError("invalid Starlink window timestamps")
    # Collect per-row samples
    all_latencies: list[float] = []
    all_drops: list[float]     = []
    all_down: list[float]      = []
    all_up: list[float]        = []
    all_obs: list[float]       = []
    statuses: list[str]        = []
    fallback_latencies: list[float] = []
    outage_seconds             = 0
    heartbeat_count            = 0

    for row in rows:
        status = row.get("satellite_link_status", "unknown")
        if status not in {"connected", "degraded", "disconnected", "unknown"}:
            raise ValueError(f"invalid satellite_link_status: {status!r}")
        statuses.append(status)
        if row.get("_measurement_status") != "ok" or row.get("_heartbeat"):
            heartbeat_count += 1

        raw_lats  = row.get("_raw_latencies") or []
        raw_drops = row.get("_raw_drops") or []
        if len(raw_lats) != len(raw_drops):
            raise ValueError("Starlink latency/drop arrays have different lengths")

        # Count outage seconds: rows where disconnected OR all drops==1
        if status == "disconnected":
            # Entire poll interval counts as outage
            outage_seconds += len(raw_drops) if raw_drops else int(row.get("_poll_interval_s", 0) or 0)
        else:
            outage_seconds += sum(1 for d in raw_drops if float(d) >= 1.0)

        # Valid latencies: samples where drop < 1
        for lat, drop in zip(raw_lats, raw_drops):
            d = float(drop)
            l = float(lat)
            if not math.isfinite(d) or not 0.0 <= d <= 1.0:
                raise ValueError(f"invalid Starlink drop fraction: {drop!r}")
            if not math.isfinite(l) or l < 0:
                raise ValueError(f"invalid Starlink latency: {lat!r}")
            if d < 1.0 and l > 0:
                all_latencies.append(l)
            all_drops.append(d)

        # Throughput
        dl = row.get("satellite_down_mbps")
        ul = row.get("satellite_up_mbps")
        if dl is not None:
            numeric_dl = float(dl)
            if not math.isfinite(numeric_dl) or numeric_dl < 0:
                raise ValueError(f"invalid downlink throughput: {dl!r}")
            all_down.append(numeric_dl)
        if ul is not None:
            numeric_ul = float(ul)
            if not math.isfinite(numeric_ul) or numeric_ul < 0:
                raise ValueError(f"invalid uplink throughput: {ul!r}")
            all_up.append(numeric_ul)

        # Obstruction
        obs = row.get("satellite_obstruction_pct")
        if obs is not None:
            numeric_obs = float(obs)
            if not math.isfinite(numeric_obs) or not 0 <= numeric_obs <= 100:
                raise ValueError(f"invalid obstruction percentage: {obs!r}")
            all_obs.append(numeric_obs)

        # Fallback RTT
        fb = row.get("satellite_rtt_ms_fallback")
        if fb is not None:
            numeric_fb = float(fb)
            if not math.isfinite(numeric_fb) or numeric_fb < 0:
                raise ValueError(f"invalid fallback latency: {fb!r}")
            fallback_latencies.append(numeric_fb)

    # Packet loss
    total_samples = len(all_drops)
    dropped_equivalent = sum(all_drops)
    packet_loss_pct = round(statistics.mean(all_drops) * 100, 3) if total_samples else None

    # RTT p50
    if all_latencies:
        rtt_p50 = round(statistics.median(all_latencies), 2)
    elif fallback_latencies:
        rtt_p50 = round(statistics.mean(fallback_latencies), 2)
    else:
        rtt_p50 = None

    # RTT p95 — null if insufficient data; NEVER interpolated
    if len(all_latencies) >= 4:
        all_latencies_sorted = sorted(all_latencies)
        idx = max(0, int(math.ceil(0.95 * len(all_latencies_sorted))) - 1)
        rtt_p95 = round(all_latencies_sorted[min(idx, len(all_latencies_sorted) - 1)], 2)
    else:
        rtt_p95 = None  # Not enough samples — emit null per spec

    link_status     = majority_status(statuses)
    down_mbps       = round(statistics.mean(all_down), 4) if all_down else None
    up_mbps         = round(statistics.mean(all_up), 4) if all_up else None
    obstruction_pct = round(statistics.mean(all_obs), 3) if all_obs else None

    # Clamp outage seconds to window size
    outage_seconds = min(outage_seconds, WINDOW_S)

    return {
        "window_start_utc":         rows[0].get("poll_timestamp_utc"),
        "window_end_utc":           rows[-1].get("poll_timestamp_utc"),
        "window_seconds":           WINDOW_S,
        "observed_poll_span_seconds": round((last_ts - first_ts).total_seconds(), 3),
        "poll_count":               len(rows),
        "trial_id":                 TRIAL_ID,
        "node_id":                  NODE_ID,
        "satellite_link_status":    link_status,
        "satellite_down_mbps":      down_mbps,
        "satellite_up_mbps":        up_mbps,
        "satellite_rtt_ms_p50":     rtt_p50,
        "satellite_rtt_ms_p95":     rtt_p95,
        "satellite_packet_loss_pct": packet_loss_pct,
        "satellite_obstruction_pct": obstruction_pct,
        "satellite_outage_seconds":  outage_seconds,
        "_sample_count_total":       total_samples,
        "_sample_count_valid_latency": len(all_latencies),
        "_dropped_sample_equivalent": round(dropped_equivalent, 6),
        "_measurement_success_count": len(rows) - heartbeat_count,
        "_heartbeat_count":            heartbeat_count,
    }


def tail_new_lines(path: Path, byte_pos: int) -> tuple[list[str], int]:
    """Read only complete UTF-8 lines; never consume a partial final record."""
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return [], byte_pos
    if size < byte_pos:
        raise ValueError("raw Starlink evidence was truncated or replaced")
    if size == byte_pos:
        return [], byte_pos
    lines: list[str] = []
    new_pos = byte_pos
    with path.open("rb") as f:
        f.seek(byte_pos)
        while True:
            line_start = f.tell()
            raw = f.readline()
            if not raw:
                break
            if not raw.endswith(b"\n"):
                new_pos = line_start
                break
            lines.append(raw.decode("utf-8").strip())
            new_pos = f.tell()
    return [line for line in lines if line], new_pos


def atomic_write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(value, f, separators=(",", ":"), allow_nan=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def load_state() -> tuple[int, list[dict], dt.datetime | None, str | None]:
    if not STATE_JSON.exists():
        return 0, [], None, None
    state = json.loads(
        STATE_JSON.read_text(encoding="utf-8"),
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
    )
    if not isinstance(state, dict):
        raise ValueError("aggregator state is not an object")
    rows = state.get("window_rows", [])
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("invalid persisted window_rows")
    last_seen = state.get("last_seen_timestamp_utc")
    if last_seen is not None and parse_ts(last_seen) is None:
        raise ValueError("invalid persisted last_seen_timestamp_utc")
    byte_pos = state.get("byte_pos", 0)
    if isinstance(byte_pos, bool) or not isinstance(byte_pos, int) or byte_pos < 0:
        raise ValueError("invalid persisted byte_pos")
    return (
        byte_pos,
        rows,
        parse_ts(state.get("window_start_utc")),
        last_seen,
    )


def save_state(byte_pos: int, rows: list[dict], window_start: dt.datetime | None, last_seen: str | None) -> None:
    atomic_write_json(STATE_JSON, {
        "saved_at_utc": utc_now(),
        "raw_jsonl": str(RAW_JSONL),
        "byte_pos": byte_pos,
        "window_start_utc": window_start.isoformat() if window_start else None,
        "window_rows": rows,
        "last_seen_timestamp_utc": last_seen,
    })


def append_jsonl(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, separators=(",", ":"), allow_nan=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def load_derived_window_keys() -> set[tuple[str, str, str, str]]:
    if not DERIVED_JSONL.exists():
        return set()
    keys: set[tuple[str, str, str, str]] = set()
    with DERIVED_JSONL.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            if not line.strip():
                continue
            row = json.loads(
                line,
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
            )
            if not isinstance(row, dict):
                raise ValueError(f"derived record {line_no} is not an object")
            key_values = tuple(row.get(field) for field in ("trial_id", "node_id", "window_start_utc", "window_end_utc"))
            if not all(isinstance(value, str) and value for value in key_values):
                raise ValueError(f"derived record {line_no} has an invalid identity/window key")
            key = key_values  # type: ignore[assignment]
            if key in keys:
                raise ValueError(f"duplicate derived Starlink window at line {line_no}")
            keys.add(key)
    return keys


def derived_window_key(row: dict) -> tuple[str, str, str, str]:
    return (
        str(row["trial_id"]),
        str(row["node_id"]),
        str(row["window_start_utc"]),
        str(row["window_end_utc"]),
    )


def main() -> None:
    DERIVED_JSONL.parent.mkdir(parents=True, exist_ok=True)
    err_path = Path(OUT_DIR) / "starlink" / "aggregator_errors.log"

    def log_err(msg: str) -> None:
        with err_path.open("a", encoding="utf-8") as f:
            f.write(f"{utc_now()} {msg.replace(chr(10), ' ').replace(chr(13), ' ')}\n")
            f.flush()
            os.fsync(f.fileno())

    print(f"[starlink_window_aggregator] starting — window={WINDOW_S}s")
    print(f"[starlink_window_aggregator] raw  ← {RAW_JSONL}")
    print(f"[starlink_window_aggregator] derived → {DERIVED_JSONL}")

    try:
        byte_pos, window_rows, window_start_ts, last_seen_ts = load_state()
        emitted_window_keys = load_derived_window_keys()
    except Exception as exc:
        log_err(f"state_load_error: {type(exc).__name__}: {exc}")
        raise

    while True:
        try:
            new_lines, candidate_pos = tail_new_lines(RAW_JSONL, byte_pos)
            parsed_rows = []
            for line in new_lines:
                row = json.loads(
                    line,
                    parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
                )
                if not isinstance(row, dict):
                    raise ValueError("raw Starlink record is not an object")
                if parse_ts(row.get("poll_timestamp_utc")) is None:
                    raise ValueError("raw Starlink record has invalid poll_timestamp_utc")
                if row.get("trial_id") != TRIAL_ID or row.get("node_id") != NODE_ID:
                    raise ValueError("raw Starlink record identity does not match runtime configuration")
                parsed_rows.append(row)
        except Exception as exc:
            # Do not advance the cursor past corrupt or partial evidence.
            log_err(f"raw_parse_error_at_byte_{byte_pos}: {type(exc).__name__}: {exc}")
            time.sleep(TAIL_INTERVAL)
            continue

        for row in parsed_rows:
            row_ts_text = str(row["poll_timestamp_utc"])
            ts = parse_ts(row_ts_text)
            assert ts is not None
            last_seen_dt = parse_ts(last_seen_ts) if last_seen_ts else None
            if last_seen_dt and ts <= last_seen_dt:
                raise ValueError("raw Starlink timestamps are duplicate or out of order")

            if window_start_ts is None:
                window_start_ts = ts

            age = (ts - window_start_ts).total_seconds()

            if age < WINDOW_S:
                window_rows.append(row)
            else:
                # Window boundary crossed — emit aggregate
                if window_rows:
                    try:
                        agg = aggregate_window(window_rows)
                        key = derived_window_key(agg)
                        if key not in emitted_window_keys:
                            append_jsonl(DERIVED_JSONL, agg)
                            emitted_window_keys.add(key)
                    except Exception as exc:
                        log_err(f"aggregate_error: {exc}")
                        raise

                # Start new window with current row
                window_rows      = [row]
                window_start_ts  = ts
            last_seen_ts = row_ts_text

        if candidate_pos != byte_pos or parsed_rows:
            byte_pos = candidate_pos
            try:
                save_state(byte_pos, window_rows, window_start_ts, last_seen_ts)
            except Exception as exc:
                log_err(f"state_write_error: {type(exc).__name__}: {exc}")
                raise

        time.sleep(TAIL_INTERVAL)


if __name__ == "__main__":
    main()
