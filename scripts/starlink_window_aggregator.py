#!/usr/bin/env python3
"""Starlink 60-second tumbling window aggregator.

Reads raw JSONL from starlink_raw_poller.py and emits one derived row per
60-second epoch to $OUT_DIR/starlink/derived/starlink_window_metrics.jsonl.

Field derivations (per spec):
  satellite_link_status   : majority vote across window rows
  satellite_down_mbps     : mean of non-null values
  satellite_up_mbps       : mean of non-null values
  satellite_rtt_ms_p50    : p50 of valid (non-dropped) latency samples;
                            falls back to mean of pop_ping_latency_ms if no raw
  satellite_rtt_ms_p95    : p95 of valid samples; null if unavailable —
                            NEVER interpolated from mean
  satellite_packet_loss_pct: (dropped samples / total samples) × 100
  satellite_obstruction_pct: mean of non-null obstruction values
  satellite_outage_seconds : count of seconds where drop==1 or status==disconnected
"""
from __future__ import annotations

import datetime as dt
import json
import os
import statistics
import sys
import time
from pathlib import Path

OUT_DIR   = os.environ.get("OUT_DIR",  "/home/pump/telemetry_head")
TRIAL_ID  = os.environ.get("TRIAL_ID", "trial-unknown")
NODE_ID   = os.environ.get("NODE_ID",  "meshradiohead")

WINDOW_S      = 60
TAIL_INTERVAL = 5   # seconds between file tail checks

RAW_JSONL     = Path(OUT_DIR) / "starlink" / "raw"     / "starlink_status_history.jsonl"
DERIVED_JSONL = Path(OUT_DIR) / "starlink" / "derived" / "starlink_window_metrics.jsonl"


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def parse_ts(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def majority_status(statuses: list[str]) -> str:
    if not statuses:
        return "unknown"
    counts: dict[str, int] = {}
    for s in statuses:
        counts[s] = counts.get(s, 0) + 1
    # Priority: disconnected > degraded > connected > unknown
    for priority in ("disconnected", "degraded", "connected", "unknown"):
        if counts.get(priority, 0) > 0:
            # Use majority only if it has the highest count among same priority tier
            pass
    return max(counts, key=lambda k: counts[k])


def aggregate_window(rows: list[dict]) -> dict:
    """Compute all satellite_* fields from a list of raw poll rows."""
    # Collect per-row samples
    all_latencies: list[float] = []
    all_drops: list[float]     = []
    all_down: list[float]      = []
    all_up: list[float]        = []
    all_obs: list[float]       = []
    statuses: list[str]        = []
    fallback_latencies: list[float] = []
    outage_seconds             = 0

    for row in rows:
        status = row.get("satellite_link_status", "unknown")
        statuses.append(status)

        raw_lats  = row.get("_raw_latencies") or []
        raw_drops = row.get("_raw_drops") or []

        # Count outage seconds: rows where disconnected OR all drops==1
        if status == "disconnected":
            # Entire poll interval counts as outage
            outage_seconds += len(raw_drops) if raw_drops else 15
        else:
            outage_seconds += sum(1 for d in raw_drops if float(d) >= 1.0)

        # Valid latencies: samples where drop < 1
        for lat, drop in zip(raw_lats, raw_drops):
            try:
                d = float(drop)
                l = float(lat)
                if d < 1.0 and l > 0:
                    all_latencies.append(l)
                all_drops.append(d)
            except (TypeError, ValueError):
                pass

        # Throughput
        dl = row.get("satellite_down_mbps")
        ul = row.get("satellite_up_mbps")
        if dl is not None:
            try: all_down.append(float(dl))
            except (TypeError, ValueError): pass
        if ul is not None:
            try: all_up.append(float(ul))
            except (TypeError, ValueError): pass

        # Obstruction
        obs = row.get("satellite_obstruction_pct")
        if obs is not None:
            try: all_obs.append(float(obs))
            except (TypeError, ValueError): pass

        # Fallback RTT
        fb = row.get("satellite_rtt_ms_fallback")
        if fb is not None:
            try: fallback_latencies.append(float(fb))
            except (TypeError, ValueError): pass

    # Packet loss
    total_samples = len(all_drops)
    dropped       = sum(1 for d in all_drops if d >= 1.0)
    packet_loss_pct = round(dropped / total_samples * 100, 3) if total_samples else None

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
        idx = int(0.95 * len(all_latencies_sorted))
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
    }


def tail_new_lines(path: Path, byte_pos: int) -> tuple[list[str], int]:
    """Read new lines from path starting at byte_pos. Returns (lines, new_pos)."""
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return [], byte_pos
    if size <= byte_pos:
        return [], byte_pos
    try:
        with path.open("rb") as f:
            f.seek(byte_pos)
            raw = f.read()
        new_pos = byte_pos + len(raw)
        lines = raw.decode("utf-8", errors="ignore").splitlines()
        return [l for l in lines if l.strip()], new_pos
    except Exception:
        return [], byte_pos


def main() -> None:
    DERIVED_JSONL.parent.mkdir(parents=True, exist_ok=True)
    err_path = Path(OUT_DIR) / "starlink" / "aggregator_errors.log"

    def log_err(msg: str) -> None:
        with err_path.open("a", encoding="utf-8") as f:
            f.write(f"{utc_now()} {msg}\n")

    print(f"[starlink_window_aggregator] starting — window={WINDOW_S}s")
    print(f"[starlink_window_aggregator] raw  ← {RAW_JSONL}")
    print(f"[starlink_window_aggregator] derived → {DERIVED_JSONL}")

    byte_pos          = 0
    window_rows: list[dict] = []
    window_start_ts: dt.datetime | None = None

    # On restart, seek to end of existing file so we don't reprocess old data
    try:
        byte_pos = RAW_JSONL.stat().st_size
    except FileNotFoundError:
        pass

    while True:
        new_lines, byte_pos = tail_new_lines(RAW_JSONL, byte_pos)

        for line in new_lines:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            ts = parse_ts(row.get("poll_timestamp_utc"))
            if ts is None:
                continue

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
                        with DERIVED_JSONL.open("a", encoding="utf-8") as f:
                            f.write(json.dumps(agg) + "\n")
                            f.flush()
                    except Exception as exc:
                        log_err(f"aggregate_error: {exc}")

                # Start new window with current row
                window_rows      = [row]
                window_start_ts  = ts

        time.sleep(TAIL_INTERVAL)


if __name__ == "__main__":
    main()
