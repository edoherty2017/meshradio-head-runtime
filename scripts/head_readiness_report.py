#!/usr/bin/env python3
"""Machine-readable readiness report for the HEAD node.

Runs ON the Pi HEAD node. Checks service state, serial port, lock state,
JSONL freshness, and sync backlog. Exits 0 if all checks pass, 1 otherwise.

Usage:
  python3 scripts/head_readiness_report.py [--out /path/to/report.json]
"""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import json
import os
import subprocess
import sys
from pathlib import Path

OUT_DIR          = os.environ.get("OUT_DIR",          "/home/pump/telemetry_head")
SERIAL_PORT      = os.environ.get("LORA_PORT",        "/dev/lora_radio")
SERIAL_LOCK_PATH = os.environ.get("SERIAL_LOCK_PATH", "/run/lock/manet_lora_serial.lock")
NODE_ID          = os.environ.get("NODE_ID",          "meshradiohead")

SERVICES = [
    "telemetry_collector_head.service",
    "telemetry_sync_spool.timer",
    "meshtastic_fallback_worker.timer",
]

JSONL_STALE_SECONDS = 300
BACKLOG_WARN_BYTES  = 50 * 1024 * 1024  # 50 MB


def svc_active(name: str) -> bool:
    r = subprocess.run(["systemctl", "is-active", name], capture_output=True, text=True)
    return r.stdout.strip() == "active"


def check_serial() -> dict:
    present = Path(SERIAL_PORT).exists()
    real = str(Path(SERIAL_PORT).resolve()) if present else None
    return {"ok": present, "present": present, "device": real}


def check_serial_lock() -> dict:
    lock_path = Path(SERIAL_LOCK_PATH)
    if not lock_path.exists():
        return {"ok": True, "held": False, "note": "lock_file_absent"}
    fh = None
    try:
        fh = lock_path.open("w")
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fh, fcntl.LOCK_UN)
        return {"ok": True, "held": False}
    except BlockingIOError:
        return {"ok": True, "held": True, "note": "collector_holds_port"}
    except Exception as exc:
        return {"ok": False, "held": None, "error": str(exc)}
    finally:
        if fh:
            try:
                fh.close()
            except Exception:
                pass


def check_jsonl(jsonl_path: Path) -> dict:
    if not jsonl_path.exists():
        return {"ok": False, "exists": False}
    size = jsonl_path.stat().st_size
    last_ts: str | None = None
    try:
        with jsonl_path.open("rb") as f:
            f.seek(max(0, size - 8192))
            tail = f.read().decode("utf-8", errors="ignore").splitlines()
        for line in reversed(tail):
            try:
                obj = json.loads(line)
                ts = obj.get("timestamp_utc")
                if ts:
                    last_ts = ts
                    break
            except Exception:
                pass
    except Exception:
        pass

    age_s: float | None = None
    if last_ts:
        try:
            ts_dt = dt.datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
            age_s = (dt.datetime.now(dt.timezone.utc) - ts_dt).total_seconds()
        except Exception:
            pass

    stale = age_s is None or age_s > JSONL_STALE_SECONDS
    warn_backlog = size > BACKLOG_WARN_BYTES
    return {
        "ok": not stale,
        "exists": True,
        "size_bytes": size,
        "last_ts": last_ts,
        "age_seconds": round(age_s) if age_s is not None else None,
        "stale": stale,
        "backlog_warning": warn_backlog,
    }


def check_connectivity_mode(out_dir: Path) -> dict:
    state_path = out_dir / "connectivity_mode.state"
    if not state_path.exists():
        return {"ok": False, "mode": None, "note": "state_file_absent"}
    mode = state_path.read_text(encoding="utf-8").strip()
    return {"ok": bool(mode), "mode": mode or None}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="", help="Write JSON report to this path (also printed to stdout)")
    ap.add_argument("--out-dir", default=OUT_DIR)
    ap.add_argument("--jsonl", default="")
    args = ap.parse_args()

    out_dir  = Path(args.out_dir)
    jsonl    = Path(args.jsonl) if args.jsonl else out_dir / "jsonl" / "telemetry_stream.jsonl"

    checks = []

    for svc in SERVICES:
        active = svc_active(svc)
        checks.append({"name": svc, "ok": active, "active": active})

    serial = check_serial()
    checks.append({"name": "serial_port", **serial})

    lock = check_serial_lock()
    checks.append({"name": "serial_lock", **lock})

    jsonl_check = check_jsonl(jsonl)
    checks.append({"name": "jsonl_fresh", **jsonl_check})

    conn = check_connectivity_mode(out_dir)
    checks.append({"name": "connectivity_mode", **conn})

    overall_ok = all(c["ok"] for c in checks)

    report = {
        "ok": overall_ok,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "node_id": NODE_ID,
        "checks": checks,
    }

    out_str = json.dumps(report, indent=2)
    print(out_str)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(out_str + "\n", encoding="utf-8")

    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())
