#!/usr/bin/env python3
"""PASS/FAIL probe for sole serial ownership and collector IPC health.

Runs ON the Pi HEAD node. Exits 0 (PASS) if all checks clear, 1 (FAIL) otherwise.

Checks:
  - /dev/lora_radio is present
  - collector service is active (holds the lock expectedly)
  - serial lock state matches collector state
  - fallback worker timer is active
  - collector Unix socket responds without transmitting a packet

Usage:
  python3 scripts/serial_probe.py
"""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import json
import os
import socket
import subprocess
import sys
from pathlib import Path

SERIAL_PORT      = os.environ.get("LORA_PORT",        "/dev/lora_radio")
SERIAL_LOCK_PATH = os.environ.get("SERIAL_LOCK_PATH", "/run/manet/lora_serial.lock")
IPC_SOCKET_PATH  = os.environ.get("COLLECTOR_IPC_SOCKET", "/run/manet/collector.sock")
OUT_DIR          = os.environ.get("OUT_DIR",          "/home/pump/telemetry_head")


def svc_state(name: str) -> str:
    r = subprocess.run(["systemctl", "is-active", name], capture_output=True, text=True)
    return r.stdout.strip()


def lock_held(path: str) -> bool | None:
    p = Path(path)
    if not p.exists():
        return None  # lock file absent — uncertain
    fh = None
    try:
        fh = p.open("a+")
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fh, fcntl.LOCK_UN)
        return False
    except BlockingIOError:
        return True
    except Exception:
        return None
    finally:
        if fh:
            try:
                fh.close()
            except Exception:
                pass


def probe_ipc() -> dict:
    """Use an unsupported operation as a non-transmitting liveness request."""
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(3)
            client.connect(IPC_SOCKET_PATH)
            client.sendall(b'{"operation":"health_check"}\n')
            raw = client.recv(4096)
        response = json.loads(raw.split(b"\n", 1)[0].decode("utf-8"))
        ok = response.get("ok") is False and response.get("error") == "unsupported_operation"
        return {"ok": ok, "socket": IPC_SOCKET_PATH, "response": response, "packet_transmitted": False}
    except Exception as exc:
        return {"ok": False, "socket": IPC_SOCKET_PATH, "error": f"{type(exc).__name__}: {exc}"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default=OUT_DIR)
    args = ap.parse_args()

    results: list[dict] = []
    now = dt.datetime.now(dt.timezone.utc).isoformat()

    # 1. Serial port present
    port_present = Path(SERIAL_PORT).exists()
    results.append({
        "check": "serial_port_present",
        "ok": port_present,
        "path": SERIAL_PORT,
        "device": str(Path(SERIAL_PORT).resolve()) if port_present else None,
    })

    # 2. Collector service active
    collector_state = svc_state("telemetry_collector_head.service")
    collector_active = collector_state == "active"
    results.append({
        "check": "collector_service_active",
        "ok": collector_active,
        "state": collector_state,
    })

    # 3. Lock state consistent with collector state
    held = lock_held(SERIAL_LOCK_PATH)
    # Consistent means: collector active → lock held; collector inactive → lock free
    if held is None:
        lock_consistent = not collector_active  # if no lock file, ok only if collector is also down
        lock_note = "lock_file_absent"
    else:
        lock_consistent = (held == collector_active)
        lock_note = "held" if held else "free"
    results.append({
        "check": "serial_lock_consistent",
        "ok": lock_consistent,
        "held": held,
        "collector_active": collector_active,
        "note": lock_note,
    })

    # 4. Fallback worker timer active
    fw_state = svc_state("meshtastic_fallback_worker.timer")
    results.append({
        "check": "fallback_worker_timer_active",
        "ok": fw_state == "active",
        "state": fw_state,
    })

    # 5. Sync timer active
    sync_state = svc_state("telemetry_sync_spool.timer")
    results.append({
        "check": "sync_timer_active",
        "ok": sync_state == "active",
        "state": sync_state,
    })

    # 6. Non-transmitting IPC liveness request
    results.append({"check": "collector_ipc_health", **probe_ipc()})

    overall = all(r["ok"] for r in results)
    report = {
        "pass": overall,
        "probed_at_utc": now,
        "checks": results,
    }
    print(json.dumps(report, indent=2))
    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
