#!/usr/bin/env python3
"""Phase 1 gate check for the Starlink collector integration.

Passes only when the latest contiguous run:
  - is fresh, monotonic, at least 60 minutes long, and ≥95% interval-aligned;
  - contains ≥95% real successful measurements (not RPC-error heartbeats);
  - contains ≥95% rows sampled under an acceptable chrony clock bound;
  - has valid raw drop fractions/latencies and plausible throughput values; and
  - has no poller errors in the last 60 minutes.

Exits 0 (PASS) or 1 (FAIL) with a machine-readable JSON report to stdout.

Usage:
  python3 scripts/starlink_phase1_gate.py [--out report.json]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
import tempfile
from pathlib import Path

OUT_DIR         = os.environ.get("OUT_DIR",           "/home/pump/telemetry_head")
POLL_INTERVAL_S = int(os.environ.get("STARLINK_POLL_S", "15"))
TRIAL_ID         = os.environ.get("TRIAL_ID", "trial-live")
NODE_ID          = os.environ.get("NODE_ID", "meshradiohead2")
MAX_CLOCK_ERROR_S = float(os.environ.get("MAX_CLOCK_ERROR_S", "5.0"))
MIN_RUN_MINUTES = 60

RAW_JSONL  = Path(OUT_DIR) / "starlink" / "raw"    / "starlink_status_history.jsonl"
ERR_LOG    = Path(OUT_DIR) / "starlink" / "poller_errors.log"


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_ts(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    try:
        if not isinstance(s, str) or not (s.endswith("Z") or s.endswith("+00:00")):
            return None
        parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
            return None
        return parsed
    except Exception:
        return None


def load_rows(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
            except (json.JSONDecodeError, ValueError) as exc:
                raise ValueError(f"invalid JSON on line {line_no}: {exc}") from exc
            if not isinstance(row, dict):
                raise ValueError(f"non-object record on line {line_no}")
            rows.append(row)
    return rows


def write_report(path: str, report: dict) -> None:
    if not path:
        return
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, destination)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def emit_report(report: dict, output_path: str) -> None:
    print(json.dumps(report, indent=2, allow_nan=False))
    write_report(output_path, report)


def latest_contiguous_rows(rows: list[dict]) -> list[dict]:
    """Return the current uninterrupted poll run, ending at the latest row."""
    valid = [(parse_ts(row.get("poll_timestamp_utc")), row) for row in rows]
    valid = [(ts, row) for ts, row in valid if ts is not None]
    if not valid:
        return []
    selected = [valid[-1]]
    max_gap_s = POLL_INTERVAL_S * 1.5
    for item in reversed(valid[:-1]):
        gap_s = (selected[-1][0] - item[0]).total_seconds()
        if gap_s <= 0 or gap_s > max_gap_s:
            break
        selected.append(item)
    selected.reverse()
    return [row for _, row in selected]


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
        if row.get("trial_id") != TRIAL_ID:
            issues.append({"row": i, "field": "trial_id", "value": row.get("trial_id")})
        if row.get("node_id") != NODE_ID:
            issues.append({"row": i, "field": "node_id", "value": row.get("node_id")})
        status = row.get("satellite_link_status")
        if status not in {"connected", "degraded", "disconnected", "unknown"}:
            issues.append({"row": i, "field": "satellite_link_status", "value": status})
        if row.get("_poll_interval_s") != POLL_INTERVAL_S:
            issues.append({"row": i, "field": "_poll_interval_s", "value": row.get("_poll_interval_s")})
        if row.get("_measurement_status") not in {"ok", "error"}:
            issues.append({"row": i, "field": "_measurement_status", "value": row.get("_measurement_status")})
        if not isinstance(row.get("_heartbeat"), bool):
            issues.append({"row": i, "field": "_heartbeat", "value": row.get("_heartbeat")})
        if not isinstance(row.get("_clock_ok"), bool):
            issues.append({"row": i, "field": "_clock_ok", "value": row.get("_clock_ok")})
        clock_bound = row.get("_clock_error_bound_s")
        if row.get("_clock_ok") is True:
            if (
                isinstance(clock_bound, bool)
                or not isinstance(clock_bound, (int, float))
                or not math.isfinite(clock_bound)
                or not 0 <= clock_bound <= MAX_CLOCK_ERROR_S
            ):
                issues.append({"row": i, "field": "_clock_error_bound_s", "value": clock_bound})
        if row.get("_heartbeat") and row.get("satellite_link_status") != "unknown":
            issues.append({"row": i, "field": "heartbeat_link_status", "value": row.get("satellite_link_status")})
        loss = row.get("_pop_ping_drop_rate")
        if loss is not None:
            try:
                if isinstance(loss, bool) or not isinstance(loss, (int, float)):
                    raise ValueError
                numeric_loss = float(loss)
                if not math.isfinite(numeric_loss) or not (0 <= numeric_loss <= 1):
                    raise ValueError
            except (TypeError, ValueError):
                issues.append({"row": i, "field": "_pop_ping_drop_rate", "value": loss})
        raw_drops = row.get("_raw_drops")
        raw_latencies = row.get("_raw_latencies")
        if not isinstance(raw_drops, list):
            issues.append({"row": i, "field": "_raw_drops", "value": raw_drops})
            raw_drops = []
        if not isinstance(raw_latencies, list):
            issues.append({"row": i, "field": "_raw_latencies", "value": raw_latencies})
            raw_latencies = []
        if len(raw_drops) != len(raw_latencies):
            issues.append({"row": i, "field": "raw_history_length_mismatch", "value": [len(raw_latencies), len(raw_drops)]})
        for sample in raw_drops:
            try:
                if isinstance(sample, bool) or not isinstance(sample, (int, float)):
                    raise ValueError
                numeric_sample = float(sample)
                if not math.isfinite(numeric_sample) or not (0 <= numeric_sample <= 1):
                    raise ValueError
            except (TypeError, ValueError):
                issues.append({"row": i, "field": "_raw_drops", "value": sample})
        for sample in raw_latencies:
            try:
                if isinstance(sample, bool) or not isinstance(sample, (int, float)):
                    raise ValueError
                numeric_sample = float(sample)
                if not math.isfinite(numeric_sample) or not (0 <= numeric_sample <= 100000):
                    raise ValueError
            except (TypeError, ValueError):
                issues.append({"row": i, "field": "_raw_latencies", "value": sample})
        for field in ("satellite_down_mbps", "satellite_up_mbps"):
            val = row.get(field)
            if val is not None:
                try:
                    if isinstance(val, bool) or not isinstance(val, (int, float)):
                        raise ValueError
                    numeric = float(val)
                    if not math.isfinite(numeric) or not (0 <= numeric <= 10000):
                        raise ValueError
                except (TypeError, ValueError):
                    issues.append({"row": i, "field": field, "value": val})
        obstruction = row.get("satellite_obstruction_pct")
        if obstruction is not None:
            try:
                if isinstance(obstruction, bool) or not isinstance(obstruction, (int, float)):
                    raise ValueError
                numeric_obstruction = float(obstruction)
                if not math.isfinite(numeric_obstruction) or not (0 <= numeric_obstruction <= 100):
                    raise ValueError
            except (TypeError, ValueError):
                issues.append({"row": i, "field": "satellite_obstruction_pct", "value": obstruction})
        fallback_rtt = row.get("satellite_rtt_ms_fallback")
        if fallback_rtt is not None:
            try:
                if isinstance(fallback_rtt, bool) or not isinstance(fallback_rtt, (int, float)):
                    raise ValueError
                numeric_rtt = float(fallback_rtt)
                if not math.isfinite(numeric_rtt) or not (0 <= numeric_rtt <= 100000):
                    raise ValueError
            except (TypeError, ValueError):
                issues.append({"row": i, "field": "satellite_rtt_ms_fallback", "value": fallback_rtt})
    return {
        "ok":            len(issues) == 0,
        "issue_count":   len(issues),
        "samples":       issues[:5],
    }


def check_measurement_success(rows: list[dict]) -> dict:
    if not rows:
        return {"ok": False, "success_pct": 0.0, "successful_rows": 0, "row_count": 0}
    successful = sum(
        1 for row in rows
        if row.get("_measurement_status") == "ok"
        and not row.get("_heartbeat")
        and row.get("satellite_link_status") in {"connected", "degraded", "disconnected"}
    )
    pct = 100.0 * successful / len(rows)
    return {
        "ok": pct >= 95.0,
        "success_pct": round(pct, 2),
        "successful_rows": successful,
        "row_count": len(rows),
        "required_pct": 95.0,
    }


def check_freshness(rows: list[dict]) -> dict:
    if not rows:
        return {"ok": False, "note": "no_rows"}
    latest = parse_ts(rows[-1].get("poll_timestamp_utc"))
    if latest is None:
        return {"ok": False, "note": "latest_timestamp_invalid"}
    age_s = (utc_now() - latest).total_seconds()
    max_age_s = max(60.0, POLL_INTERVAL_S * 2.5)
    return {
        "ok": 0 <= age_s <= max_age_s,
        "age_seconds": round(age_s, 2),
        "max_age_seconds": max_age_s,
        "latest_timestamp_utc": latest.isoformat(),
    }


def check_clock_integrity(rows: list[dict]) -> dict:
    if not rows:
        return {"ok": False, "clock_ok_pct": 0.0, "row_count": 0}
    good = sum(1 for row in rows if row.get("_clock_ok") is True)
    pct = 100.0 * good / len(rows)
    return {
        "ok": pct >= 95.0,
        "clock_ok_pct": round(pct, 2),
        "clock_ok_rows": good,
        "row_count": len(rows),
        "required_pct": 95.0,
    }


def check_timestamp_order(rows: list[dict]) -> dict:
    timestamps = [parse_ts(row.get("poll_timestamp_utc")) for row in rows]
    invalid = sum(timestamp is None for timestamp in timestamps)
    non_increasing = 0
    valid = [timestamp for timestamp in timestamps if timestamp is not None]
    for previous, current in zip(valid, valid[1:]):
        if current <= previous:
            non_increasing += 1
    return {
        "ok": invalid == 0 and non_increasing == 0,
        "invalid_timestamp_count": invalid,
        "non_increasing_timestamp_count": non_increasing,
    }


def check_error_log(path: Path) -> dict:
    if not path.exists():
        return {"ok": True, "note": "no_error_log"}
    cutoff = utc_now() - dt.timedelta(hours=1)
    fatal_lines = []
    malformed_lines = []
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # Parse timestamp from log prefix
                ts_str = line.split(" ")[0]
                ts = parse_ts(ts_str)
                if ts is None:
                    malformed_lines.append(line[-200:])
                elif ts > cutoff:
                    fatal_lines.append(line[-200:])
    except Exception:
        return {"ok": False, "note": "error_log_unreadable"}

    return {
        "ok":                  len(fatal_lines) == 0 and len(malformed_lines) == 0,
        "recent_errors":       len(fatal_lines),
        "malformed_lines":     len(malformed_lines),
        "samples":             fatal_lines[:3],
        "malformed_samples":   malformed_lines[:3],
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
        emit_report(report, args.out)
        return 1

    try:
        all_rows = load_rows(RAW_JSONL)
    except (OSError, UnicodeError, ValueError) as exc:
        report = {
            "pass": False,
            "probed_at_utc": now.isoformat(),
            "checks": [{"name": "raw_jsonl_parseable", "ok": False, "error": str(exc)}],
        }
        emit_report(report, args.out)
        return 1
    rows = latest_contiguous_rows(all_rows)
    checks.append({
        "name": "raw_jsonl_exists",
        "ok": bool(rows),
        "row_count_total": len(all_rows),
        "row_count_current_contiguous_run": len(rows),
    })
    order = check_timestamp_order(all_rows)
    checks.append({"name": "timestamp_order", **order})

    # 2. Row alignment ≥95%
    alignment = check_row_alignment(rows)
    checks.append({"name": "row_alignment", **alignment})

    # 3. Run duration ≥60 min
    duration = check_run_duration(rows)
    checks.append({"name": "run_duration", **duration})

    # 4. Value ranges valid
    ranges = check_value_ranges(rows)
    checks.append({"name": "value_ranges_valid", **ranges})

    # A perfectly timed run of RPC-error heartbeats is not a successful dish
    # integration. Require actual measurements in the current run.
    success = check_measurement_success(rows)
    checks.append({"name": "measurement_success", **success})

    freshness = check_freshness(rows)
    checks.append({"name": "latest_row_fresh", **freshness})

    clock = check_clock_integrity(rows)
    checks.append({"name": "clock_integrity", **clock})

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

    emit_report(report, args.out)

    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
