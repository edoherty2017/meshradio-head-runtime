#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import json
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(
                line,
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
            )
            if not isinstance(record, dict):
                raise ValueError(f"queue record on line {len(rows) + 1} is not an object")
            rows.append(record)
    return rows


def lock_path_for(path: Path) -> Path:
    return Path(f"{path}.lock")


@contextlib.contextmanager
def queue_lock(path: Path, *, exclusive: bool):
    lock_path = lock_path_for(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock_file, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            os.fchmod(f.fileno(), 0o600)
            for row in rows:
                f.write(json.dumps(row, separators=(",", ":"), allow_nan=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def cmd_enqueue(args: argparse.Namespace) -> int:
    queue_path = Path(args.queue_path)
    command_bytes = len(args.command.encode("utf-8"))
    if command_bytes > args.max_command_bytes:
        raise SystemExit(
            f"command is {command_bytes} bytes; maximum is {args.max_command_bytes} bytes"
        )

    rec = {
        "command_id": f"cmd-{uuid.uuid4().hex[:12]}",
        "issued_at_utc": utc_now(),
        "target_node_id": args.target,
        "command": args.command,
        "ttl_sec": args.ttl_sec,
        "ack_required": int(not args.no_ack),
        "connectivity_mode_hint": args.mode,
        "status": "queued",
        "status_source": "queue",
    }

    with queue_lock(queue_path, exclusive=True):
        rows = load_jsonl(queue_path)
        rows.append(rec)
        write_jsonl(queue_path, rows)
    print(json.dumps({"queued": 1, "command_id": rec["command_id"], "queue_path": str(queue_path)}))
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    queue_path = Path(args.queue_path)
    with queue_lock(queue_path, exclusive=False):
        rows = load_jsonl(queue_path)
    if args.only_pending:
        rows = [r for r in rows if r.get("status") in {"queued", "sent"}]
    print(json.dumps({"count": len(rows), "queue_path": str(queue_path), "commands": rows}, indent=2))
    return 0


def cmd_mark_sent(args: argparse.Namespace) -> int:
    queue_path = Path(args.queue_path)
    with queue_lock(queue_path, exclusive=True):
        rows = load_jsonl(queue_path)
        updated = 0
        for rec in rows:
            if rec.get("command_id") == args.command_id and rec.get("status") == "queued":
                rec["status"] = "sent"
                rec["sent_at_utc"] = utc_now()
                rec["sent_via"] = args.via
                rec["status_source"] = "local_operator_override"
                updated += 1
        write_jsonl(queue_path, rows)
    print(json.dumps({"updated": updated, "command_id": args.command_id, "status": "sent"}))
    return 0 if updated else 1


def cmd_ack(args: argparse.Namespace) -> int:
    queue_path = Path(args.queue_path)
    with queue_lock(queue_path, exclusive=True):
        rows = load_jsonl(queue_path)
        updated = 0
        for rec in rows:
            if rec.get("command_id") == args.command_id and rec.get("status") in {"queued", "sent"}:
                rec["status"] = "operator_acknowledged"
                rec["acked_at_utc"] = utc_now()
                rec["ack_payload"] = args.payload
                rec["ack_source"] = "local_operator_override"
                updated += 1
        write_jsonl(queue_path, rows)
    print(json.dumps({"updated": updated, "command_id": args.command_id, "status": "operator_acknowledged"}))
    return 0 if updated else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Queue fallback control commands for MESH_ONLY windows.")
    p.add_argument(
        "--queue-path",
        default=os.environ.get("FALLBACK_COMMAND_QUEUE", "/home/pump/telemetry_head/fallback_commands.jsonl"),
        help="Path to JSONL queue file",
    )

    sub = p.add_subparsers(dest="subcommand", required=True)

    e = sub.add_parser("enqueue", help="Append a new queued command")
    e.add_argument("--target", required=True, help="Target node id")
    e.add_argument("--command", required=True, help="Command payload/body")
    e.add_argument("--ttl-sec", type=positive_int, default=600, help="Command TTL in seconds")
    e.add_argument(
        "--max-command-bytes",
        type=positive_int,
        default=int(os.environ.get("MAX_FALLBACK_COMMAND_BYTES", "160")),
        help="Maximum UTF-8 command-body size (the worker adds a correlation tag)",
    )
    e.add_argument("--mode", default="MESH_ONLY", choices=["IP_FULL", "IP_DEGRADED", "MESH_ONLY"])
    e.add_argument("--no-ack", action="store_true", help="Do not require an ACK")
    e.set_defaults(func=cmd_enqueue)

    l = sub.add_parser("list", help="List queue entries")
    l.add_argument("--only-pending", action="store_true", help="Show only queued commands")
    l.set_defaults(func=cmd_list)

    s = sub.add_parser("mark-sent", help="Operator override: mark a queued command as sent")
    s.add_argument("--command-id", required=True)
    s.add_argument("--via", default="meshtastic", help="Transport label")
    s.set_defaults(func=cmd_mark_sent)

    a = sub.add_parser("ack", help="Operator override: acknowledge without a transport-authenticated ACK")
    a.add_argument("--command-id", required=True)
    a.add_argument("--payload", default="", help="Optional ACK payload")
    a.set_defaults(func=cmd_ack)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
