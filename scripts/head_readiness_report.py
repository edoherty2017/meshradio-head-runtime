#!/usr/bin/env python3
"""Machine-readable, fail-closed readiness report for the HEAD node."""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import json
import os
import shutil
import socket
import stat
import subprocess
import sys
from pathlib import Path

OUT_DIR = os.environ.get("OUT_DIR", "/home/pump/telemetry_head")
SERIAL_PORT = os.environ.get("LORA_PORT", "/dev/lora_radio")
SERIAL_LOCK_PATH = os.environ.get("SERIAL_LOCK_PATH", "/run/manet/lora_serial.lock")
IPC_SOCKET_PATH = os.environ.get("COLLECTOR_IPC_SOCKET", "/run/manet/collector.sock")
NODE_ID = os.environ.get("NODE_ID", "meshradiohead2")
VALID_MODES = {"IP_FULL", "IP_DEGRADED", "MESH_ONLY"}
SERVICES = [
    "telemetry_collector_head.service",
    "telemetry_sync_spool.timer",
    "meshtastic_fallback_worker.timer",
    "starlink_raw_poller.service",
    "starlink_window_aggregator.service",
]
JSONL_STALE_SECONDS = 300
SYNC_STALE_SECONDS = 600
MIN_FREE_BYTES = 1024 * 1024 * 1024
MIN_FREE_PERCENT = 10.0


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_utc(value: object) -> dt.datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
            return None
        return parsed.astimezone(dt.timezone.utc)
    except Exception:
        return None


def svc_active(name: str) -> bool:
    result = subprocess.run(["systemctl", "is-active", name], capture_output=True, text=True, timeout=5)
    return result.stdout.strip() == "active"


def check_jsonl(path: Path, timestamp_field: str = "timestamp_utc") -> dict:
    if not path.is_file():
        return {"ok": False, "exists": False, "path": str(path)}
    size = path.stat().st_size
    with path.open("rb") as f:
        start = max(0, size - 64 * 1024)
        f.seek(start)
        data = f.read()
    if start and b"\n" in data:
        data = data.split(b"\n", 1)[1]
    complete_final = not data or data.endswith(b"\n")
    lines = [line for line in data.splitlines() if line.strip()]
    sampled = lines[-20:]
    errors: list[str] = []
    records: list[dict] = []
    for index, raw in enumerate(sampled, start=1):
        try:
            record = json.loads(raw.decode("utf-8"), parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
            if not isinstance(record, dict):
                raise ValueError("record_not_object")
            if parse_utc(record.get(timestamp_field)) is None:
                raise ValueError(f"invalid_{timestamp_field}")
            if record.get("node_id") != NODE_ID:
                raise ValueError("unexpected_node_id")
            if timestamp_field == "timestamp_utc":
                for field in ("trial_id", "head_id", "line"):
                    if not isinstance(record.get(field), str) or not record[field].strip():
                        raise ValueError(f"invalid_{field}")
            records.append(record)
        except Exception as exc:
            errors.append(f"sample_{index}:{type(exc).__name__}")
    latest = parse_utc(records[-1].get(timestamp_field)) if records else None
    age = (now_utc() - latest).total_seconds() if latest else None
    measurement_status = records[-1].get("_measurement_status") if records else None
    measurement_ok = timestamp_field != "poll_timestamp_utc" or (
        measurement_status == "ok" and records[-1].get("_clock_ok") is True
    )
    ok = bool(records) and complete_final and not errors and measurement_ok and age is not None and 0 <= age <= JSONL_STALE_SECONDS
    return {
        "ok": ok,
        "exists": True,
        "path": str(path),
        "size_bytes": size,
        "sampled_records": len(sampled),
        "sample_errors": errors,
        "partial_final_record": not complete_final,
        "measurement_status": measurement_status,
        "clock_ok": records[-1].get("_clock_ok") if records else None,
        "latest_timestamp_utc": latest.isoformat() if latest else None,
        "age_seconds": round(age, 1) if age is not None else None,
    }


def check_serial_owner() -> dict:
    port_present = Path(SERIAL_PORT).exists()
    lock_path = Path(SERIAL_LOCK_PATH)
    if not port_present or not lock_path.exists():
        return {"ok": False, "port_present": port_present, "lock_exists": lock_path.exists()}
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            held = True
        else:
            held = False
            fcntl.flock(lock_file, fcntl.LOCK_UN)
    socket_path = Path(IPC_SOCKET_PATH)
    socket_ok = socket_path.exists() and stat.S_ISSOCK(socket_path.stat().st_mode)
    ipc_live = False
    ipc_error = None
    if socket_ok:
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(3)
                client.connect(str(socket_path))
                client.sendall(b'{"operation":"health_check"}\n')
                response = json.loads(client.recv(4096).split(b"\n", 1)[0].decode("utf-8"))
            ipc_live = response.get("ok") is False and response.get("error") == "unsupported_operation"
        except Exception as exc:
            ipc_error = f"{type(exc).__name__}: {exc}"
    return {
        "ok": held and socket_ok and ipc_live,
        "port_present": True,
        "collector_holds_lock": held,
        "ipc_socket_ok": socket_ok,
        "ipc_live": ipc_live,
        "ipc_error": ipc_error,
    }


def check_connectivity(out_dir: Path) -> dict:
    path = out_dir / "connectivity_mode.state"
    mode = path.read_text(encoding="utf-8").strip() if path.exists() else None
    events_path = out_dir / "connectivity_events.jsonl"
    event = None
    event_error = None
    if events_path.is_file():
        try:
            size = events_path.stat().st_size
            with events_path.open("rb") as handle:
                start = max(0, size - 64 * 1024)
                handle.seek(start)
                data = handle.read()
            if start and b"\n" in data:
                data = data.split(b"\n", 1)[1]
            if data and not data.endswith(b"\n"):
                raise ValueError("partial_final_connectivity_event")
            lines = [line for line in data.splitlines() if line.strip()]
            if not lines:
                raise ValueError("no_connectivity_events")
            event = json.loads(
                lines[-1].decode("utf-8"),
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
            )
            if not isinstance(event, dict):
                raise ValueError("connectivity_event_not_object")
        except Exception as exc:
            event_error = f"{type(exc).__name__}: {exc}"
    else:
        event_error = "connectivity_events_missing"
    event_timestamp = parse_utc(event.get("timestamp_utc")) if event else None
    event_age = (now_utc() - event_timestamp).total_seconds() if event_timestamp else None
    event_consistent = bool(
        event
        and event_error is None
        and event.get("node_id") == NODE_ID
        and event.get("connectivity_mode") == mode
        and event.get("event_type")
        in {"CONNECTIVITY_MODE_CHANGE", "CONNECTIVITY_MODE_OBSERVED"}
        and event_age is not None
        and 0 <= event_age <= SYNC_STALE_SECONDS
    )
    marker = out_dir / "last_sync_success.json"
    last_success = None
    marker_error = None
    if marker.exists():
        try:
            marker_record = json.loads(
                marker.read_text(encoding="utf-8"),
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
            )
            if (
                not isinstance(marker_record, dict)
                or marker_record.get("node_id") != NODE_ID
                or marker_record.get("status") != "success"
            ):
                raise ValueError("invalid_sync_success_marker")
            last_success = parse_utc(marker_record.get("timestamp_utc"))
            if last_success is None:
                raise ValueError("invalid_sync_success_timestamp")
        except Exception as exc:
            marker_error = f"{type(exc).__name__}: {exc}"
    age = (now_utc() - last_success).total_seconds() if last_success else None
    consistent = mode in VALID_MODES and marker_error is None and event_consistent
    if mode == "IP_FULL":
        consistent = consistent and age is not None and 0 <= age <= SYNC_STALE_SECONDS
    return {
        "ok": consistent,
        "mode": mode,
        "degraded": mode != "IP_FULL",
        "latest_event_utc": event_timestamp.isoformat() if event_timestamp else None,
        "latest_event_age_seconds": round(event_age, 1) if event_age is not None else None,
        "latest_event_type": event.get("event_type") if event else None,
        "event_consistent": event_consistent,
        "event_error": event_error,
        "last_sync_success_utc": last_success.isoformat() if last_success else None,
        "last_sync_age_seconds": round(age, 1) if age is not None else None,
        "marker_error": marker_error,
    }


def check_storage(out_dir: Path) -> dict:
    if not out_dir.exists():
        return {"ok": False, "error": "out_dir_missing", "path": str(out_dir)}
    usage = shutil.disk_usage(out_dir)
    free_pct = 100.0 * usage.free / usage.total
    evidence_bytes = sum(path.stat().st_size for path in out_dir.rglob("*") if path.is_file())
    return {
        "ok": usage.free >= MIN_FREE_BYTES and free_pct >= MIN_FREE_PERCENT,
        "free_bytes": usage.free,
        "free_percent": round(free_pct, 2),
        "evidence_bytes": evidence_bytes,
        "minimum_free_bytes": MIN_FREE_BYTES,
        "minimum_free_percent": MIN_FREE_PERCENT,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="")
    parser.add_argument("--out-dir", default=OUT_DIR)
    parser.add_argument("--jsonl", default="")
    args = parser.parse_args()
    out_dir = Path(args.out_dir)
    jsonl = Path(args.jsonl) if args.jsonl else out_dir / "jsonl" / "telemetry_stream.jsonl"

    checks: list[dict] = []
    for service in SERVICES:
        active = svc_active(service)
        checks.append({"name": service, "ok": active, "active": active})
    checks.append({"name": "serial_single_owner", **check_serial_owner()})
    checks.append({"name": "telemetry_jsonl", **check_jsonl(jsonl)})
    checks.append({
        "name": "starlink_raw_jsonl",
        **check_jsonl(out_dir / "starlink" / "raw" / "starlink_status_history.jsonl", "poll_timestamp_utc"),
    })
    checks.append({"name": "connectivity_evidence", **check_connectivity(out_dir)})
    checks.append({"name": "storage", **check_storage(out_dir)})

    report = {
        "ok": all(check["ok"] for check in checks),
        "generated_at_utc": now_utc().isoformat(),
        "node_id": NODE_ID,
        "checks": checks,
    }
    output = json.dumps(report, indent=2)
    print(output)
    if args.out:
        path = Path(args.out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(output + "\n", encoding="utf-8")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
