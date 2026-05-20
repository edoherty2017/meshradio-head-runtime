#!/usr/bin/env python3
"""Phase 1 gate check for the Starlink collector integration.

Passes when:
  - Raw JSONL exists and has ≥95% row alignment at the expected 15s poll interval
  - No fatal exceptions in the poller error log (last 60 min)
  - satellite_packet_loss_pct is within [0, 100] for all rows
  - All throughput values are < 1000 Mbps
  - Collector has run for at least 60 continuous minutes

Exits 0 (PASS) or 1 (FAIL) with a machine-readable JSON report to stdout.

Usage:
  python3 scripts/starlink_phase1_gate.py [--out report.json]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

OUT_DIR         = os.environ.get("OUT_DIR",           "/home/pump/telemetry_head")
POLL_INTERVAL_S = int(os.environ.get("STARLINK_POLL_S", "15"))
MIN_RUN_MINUTES = 60

RAW_JSONL  = Path(OUT_DIR) / "starlink" / "raw"    / "starlink_status_history.jsonl"
ERR_LOG    = Path(OUT_DIR) / "starlink" / "poller_errors.log"


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_ts(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def load_rows(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows


def check_row_alignment(rows: list[dict]) -> dict:
    """Check that ≥95% of consecutive row gaps are within 1.5× poll interval."""
    if len(rows) < 2:
        return {"ok": False, "note": "too_few_rows", "alignment_pct": 0.0}

    timestamps = [parse_ts(r.get("poll_timestamp_utc")) for r in rows]
    timestamps = [t for t in timestamps if t is not None]
    if len(timestamps) < 2:
        return {"ok": False, "note": "no_valid_timestamps", "alignment_pct": 0.0}

    expected_gap = dt.timedelta(seconds=POLL_INTERVAL_S)
    max_gap      = dt.timedelta(seconds=POLL_INTERVAL_S * 1.5)
    aligned      = 0
    gaps         = []

    for i in range(1, len(timestamps)):
        gap = timestamps[i] - timestamps[i - 1]
        gaps.append(gap.total_seconds())
        if abs(gap - expected_gap) <= (expected_gap * 0.5):
            aligned += 1

    total      = len(timestamps) - 1
    pct        = aligned / total * 100
    mean_gap   = sum(gaps) / len(gaps)

    return {
        "ok":            pct >= 95.0,
        "alignment_pct": round(pct, 2),
        "row_count":     len(timestamps),
        "mean_gap_s":    round(mean_gap, 2),
        "expected_gap_s": POLL_INTERVAL_S,
    }


def check_run_duration(rows: list[dict]) -> dict:
    """Check that data spans at least MIN_RUN_MINUTES."""
    timestamps = [parse_ts(r.get("poll_timestamp_utc")) for r in rows]
    timestamps = sorted(t for t in timestamps if t is not None)
    if len(timestamps) < 2:
        return {"ok": False, "span_minutes": 0}
    span_s = (timestamps[-1] - timestamps[0]).total_seconds()
    span_m = span_s / 60
    return {
        "ok":            span_m >= MIN_RUN_MINUTES,
        "span_minutes":  round(span_m, 1),
        "required_minutes": MIN_RUN_MINUTES,
        "first_ts":      timestamps[0].isoformat(),
        "last_ts":       timestamps[-1].isoformat(),
    }


def check_value_ranges(rows: list[dict]) -> dict:
    issues = []
    for i, row in enumerate(rows):
        if row.get("_heartbeat"):
            continue
        loss = row.get("satellite_packet_loss_pct")
        if loss is not None and not (0 <= float(loss) <= 100):
            issues.append({"row": i, "field": "satellite_packet_loss_pct", "value": loss})
        for field in ("satellite_down_mbps", "satellite_up_mbps"):
            val = row.get(field)
            if val is not None and float(val) >= 1000:
                issues.append({"row": i, "field": field, "value": val})
    return {
        "ok":            len(issues) == 0,
        "issue_count":   len(issues),
        "samples":       issues[:5],
    }


def check_error_log(path: Path) -> dict:
    if not path.exists():
        return {"ok": True, "note": "no_error_log"}
    cutoff = utc_now() - dt.timedelta(hours=1)
    fatal_lines = []
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # Parse timestamp from log prefix
                try:
                    ts_str = line.split(" ")[0]
                    ts     = parse_ts(ts_str)
                    if ts and ts > cutoff:
                        fatal_lines.append(line[-200:])
                except Exception:
                    pass
    except Exception:
        return {"ok": False, "note": "error_log_unreadable"}

    return {
        "ok":                  len(fatal_lines) == 0,
        "recent_errors":       len(fatal_lines),
        "samples":             fatal_lines[:3],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    checks: list[dict] = []
    now = utc_now()

    # 1. Raw JSONL exists
    if not RAW_JSONL.exists():
        report = {
            "pass": False,
            "probed_at_utc": now.isoformat(),
            "checks": [{"name": "raw_jsonl_exists", "ok": False, "path": str(RAW_JSONL)}],
            "error": "raw_jsonl_not_found — is starlink_raw_poller.service running?",
        }
        print(json.dumps(report, indent=2))
        return 1

    rows = load_rows(RAW_JSONL)
    checks.append({"name": "raw_jsonl_exists", "ok": True, "row_count": len(rows)})

    # 2. Row alignment ≥95%
    alignment = check_row_alignment(rows)
    checks.append({"name": "row_alignment", **alignment})

    # 3. Run duration ≥60 min
    duration = check_run_duration(rows)
    checks.append({"name": "run_duration", **duration})

    # 4. Value ranges valid
    ranges = check_value_ranges(rows)
    checks.append({"name": "value_ranges_valid", **ranges})

    # 5. Error log clean (last 60 min)
    err_check = check_error_log(ERR_LOG)
    checks.append({"name": "error_log_clean", **err_check})

    overall = all(c["ok"] for c in checks)
    report = {
        "pass":           overall,
        "probed_at_utc":  now.isoformat(),
        "raw_jsonl":      str(RAW_JSONL),
        "checks":         checks,
    }

    out = json.dumps(report, indent=2)
    print(out)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(out + "\n", encoding="utf-8")

    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
