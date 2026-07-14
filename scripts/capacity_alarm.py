#!/usr/bin/env python3
"""Disk/inode capacity alarm for HEAD evidence storage.

Emits one structured JSON event per checked path plus a summary, and exits with
the worst severity found (0 OK, 1 WARN, 2 CRITICAL) so a systemd unit or monitor
can react. Nothing here deletes data; rotation/retention is
``evidence_rotate.py``'s job. This only observes and reports.

A path is CRITICAL if used space is at/over the critical percent OR free space
is at/under ``--min-free-bytes`` OR inode usage is at/over the inode critical
percent; WARN uses the warn thresholds; otherwise OK.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any

OK, WARN, CRITICAL = "OK", "WARN", "CRITICAL"
_SEVERITY_RANK = {OK: 0, WARN: 1, CRITICAL: 2}
_EXIT_CODE = {OK: 0, WARN: 1, CRITICAL: 2}


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _pct(used: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(100.0 * used / total, 3)


def evaluate_path(
    path: Path,
    *,
    warn_percent: float,
    critical_percent: float,
    min_free_bytes: int,
    inode_warn_percent: float,
    inode_critical_percent: float,
) -> dict[str, Any]:
    if not path.exists():
        return {
            "timestamp_utc": utc_now_iso(),
            "event_type": "CAPACITY_UNAVAILABLE",
            "path": str(path),
            "severity": CRITICAL,
            "error": "path_does_not_exist",
        }
    try:
        usage = shutil.disk_usage(str(path))
    except OSError as exc:
        return {
            "timestamp_utc": utc_now_iso(),
            "event_type": "CAPACITY_UNAVAILABLE",
            "path": str(path),
            "severity": CRITICAL,
            "error": f"{type(exc).__name__}: {exc}",
        }

    used_percent = _pct(usage.used, usage.total)

    inode_total = inode_used = inode_free = None
    inode_percent = None
    try:
        vfs = os.statvfs(str(path))
        inode_total = vfs.f_files
        inode_free = vfs.f_favail
        if inode_total and inode_total > 0:
            inode_used = inode_total - inode_free
            inode_percent = _pct(inode_used, inode_total)
    except (OSError, AttributeError):
        pass  # inode stats unavailable on this platform/filesystem

    severity = OK
    reasons: list[str] = []
    if used_percent >= critical_percent:
        severity = CRITICAL
        reasons.append("bytes_critical")
    elif used_percent >= warn_percent:
        severity = WARN
        reasons.append("bytes_warn")
    if min_free_bytes > 0 and usage.free <= min_free_bytes:
        severity = CRITICAL
        reasons.append("min_free_bytes")
    if inode_percent is not None:
        if inode_percent >= inode_critical_percent:
            severity = CRITICAL
            reasons.append("inodes_critical")
        elif inode_percent >= inode_warn_percent and severity != CRITICAL:
            severity = WARN
            reasons.append("inodes_warn")

    event_type = {
        OK: "CAPACITY_OK",
        WARN: "CAPACITY_WARN",
        CRITICAL: "CAPACITY_CRITICAL",
    }[severity]

    return {
        "timestamp_utc": utc_now_iso(),
        "event_type": event_type,
        "path": str(path),
        "severity": severity,
        "reasons": reasons,
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "used_percent": used_percent,
        "inode_total": inode_total,
        "inode_used": inode_used,
        "inode_free": inode_free,
        "inode_used_percent": inode_percent,
        "warn_percent": warn_percent,
        "critical_percent": critical_percent,
        "min_free_bytes": min_free_bytes,
    }


def emit(events: list[dict[str, Any]], out_path: str | None) -> None:
    lines = [json.dumps(e, separators=(",", ":"), sort_keys=True) for e in events]
    if out_path:
        p = Path(out_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            for line in lines:
                f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())
    for line in lines:
        sys.stdout.write(line + "\n")


def run(args: argparse.Namespace) -> int:
    events: list[dict[str, Any]] = []
    worst = OK
    for raw_path in args.path:
        event = evaluate_path(
            Path(raw_path),
            warn_percent=args.warn_percent,
            critical_percent=args.critical_percent,
            min_free_bytes=args.min_free_bytes,
            inode_warn_percent=args.inode_warn_percent,
            inode_critical_percent=args.inode_critical_percent,
        )
        events.append(event)
        if _SEVERITY_RANK[event["severity"]] > _SEVERITY_RANK[worst]:
            worst = event["severity"]

    events.append({
        "timestamp_utc": utc_now_iso(),
        "event_type": "CAPACITY_SUMMARY",
        "worst_severity": worst,
        "checked_paths": len(args.path),
    })
    emit(events, args.out or None)
    return _EXIT_CODE[worst]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Emit structured disk/inode capacity alarms for HEAD storage.")
    p.add_argument("--path", action="append", default=None, help="Filesystem path to check (repeatable)")
    p.add_argument("--warn-percent", type=float, default=float(os.environ.get("CAPACITY_WARN_PERCENT", "80")))
    p.add_argument("--critical-percent", type=float, default=float(os.environ.get("CAPACITY_CRITICAL_PERCENT", "90")))
    p.add_argument("--min-free-bytes", type=int, default=int(os.environ.get("CAPACITY_MIN_FREE_BYTES", "0")))
    p.add_argument("--inode-warn-percent", type=float, default=float(os.environ.get("CAPACITY_INODE_WARN_PERCENT", "80")))
    p.add_argument("--inode-critical-percent", type=float, default=float(os.environ.get("CAPACITY_INODE_CRITICAL_PERCENT", "90")))
    p.add_argument("--out", default=os.environ.get("CAPACITY_ALARM_EVENTS", ""), help="Append events to this JSONL file in addition to stdout")
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.path:
        args.path = [os.environ.get("OUT_DIR", "/home/pump/telemetry_head")]
    if not (0 < args.warn_percent <= 100) or not (0 < args.critical_percent <= 100):
        parser.error("warn/critical percents must be within (0, 100]")
    if args.warn_percent > args.critical_percent:
        parser.error("--warn-percent must be <= --critical-percent")
    if args.min_free_bytes < 0:
        parser.error("--min-free-bytes must be non-negative")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
