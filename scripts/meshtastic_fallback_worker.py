#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any


DEFAULT_QUEUE = "/home/pump/telemetry_head/fallback_commands.jsonl"
DEFAULT_OUT_DIR = "/home/pump/telemetry_head"


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def parse_ts(ts: str | None) -> dt.datetime | None:
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return dt.datetime.fromisoformat(ts)
    except Exception:
        return None


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")


def append_event(path: Path, event: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, separators=(",", ":")) + "\n")


class TransportAdapter:
    def send(self, target: str, payload: str, command_id: str) -> dict[str, Any]:
        raise NotImplementedError


class DryRunAdapter(TransportAdapter):
    def send(self, target: str, payload: str, command_id: str) -> dict[str, Any]:
        return {
            "ok": True,
            "transport": "dry_run",
            "target": target,
            "payload": payload,
            "command_id": command_id,
            "note": "No packet transmitted; dry-run mode.",
        }


class MeshtasticCliAdapter(TransportAdapter):
    def __init__(self, cli: str, extra_args: str = "") -> None:
        self.cli = cli
        self.extra_args = shlex.split(extra_args) if extra_args else []

    def send(self, target: str, payload: str, command_id: str) -> dict[str, Any]:
        tagged = f"[cmd:{command_id}] {payload}"
        cmd = [self.cli, "--dest", target, "--sendtext", tagged, *self.extra_args]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        ok = proc.returncode == 0
        return {
            "ok": ok,
            "transport": "meshtastic_cli",
            "command": cmd,
            "returncode": proc.returncode,
            "stdout": proc.stdout[-2000:],
            "stderr": proc.stderr[-2000:],
        }


def build_adapter(args: argparse.Namespace) -> TransportAdapter:
    if args.dry_run:
        return DryRunAdapter()
    cli = args.meshtastic_cli
    if cli and Path(cli).exists():
        return MeshtasticCliAdapter(cli=cli, extra_args=args.meshtastic_extra_args)
    # fallback to PATH check
    if cli:
        found = shutil_which(cli)
        if found:
            return MeshtasticCliAdapter(cli=found, extra_args=args.meshtastic_extra_args)
    found = shutil_which("meshtastic")
    if found:
        return MeshtasticCliAdapter(cli=found, extra_args=args.meshtastic_extra_args)
    return DryRunAdapter()


def shutil_which(binary: str) -> str | None:
    for p in os.environ.get("PATH", "").split(":"):
        cand = Path(p) / binary
        if cand.exists() and os.access(cand, os.X_OK):
            return str(cand)
    return None


def should_expire(rec: dict[str, Any], now: dt.datetime) -> bool:
    ttl = int(rec.get("ttl_sec", 0) or 0)
    issued = parse_ts(rec.get("issued_at_utc"))
    if ttl <= 0 or not issued:
        return False
    return (now - issued).total_seconds() > ttl


def should_retry(rec: dict[str, Any], now: dt.datetime, base: float, max_backoff: float) -> bool:
    attempts = int(rec.get("send_attempts", 0) or 0)
    next_after = parse_ts(rec.get("next_attempt_after_utc"))
    if next_after and now < next_after:
        return False
    if attempts <= 0:
        return True
    backoff = min(max_backoff, base * (2 ** (attempts - 1)))
    last = parse_ts(rec.get("last_attempt_at_utc"))
    if not last:
        return True
    return (now - last).total_seconds() >= backoff


def ingest_acks(rows: list[dict[str, Any]], ack_ids: set[str], ack_payload_map: dict[str, Any], events_path: Path) -> int:
    updated = 0
    for rec in rows:
        cid = rec.get("command_id")
        if cid in ack_ids and rec.get("status") in {"queued", "sent"}:
            rec["status"] = "acked"
            rec["acked_at_utc"] = utc_now_iso()
            if cid in ack_payload_map:
                rec["ack_payload"] = ack_payload_map[cid]
            append_event(events_path, {
                "timestamp_utc": utc_now_iso(),
                "event_type": "ACK_INGESTED",
                "command_id": cid,
            })
            updated += 1
    return updated


def parse_manual_ack_file(path: Path) -> tuple[set[str], dict[str, Any]]:
    ids: set[str] = set()
    payloads: dict[str, Any] = {}
    if not path.exists():
        return ids, payloads
    for rec in load_jsonl(path):
        cid = rec.get("command_id")
        if cid:
            ids.add(cid)
            payloads[cid] = rec
    return ids, payloads


def parse_rx_log(path: Path) -> tuple[set[str], dict[str, Any]]:
    ids: set[str] = set()
    payloads: dict[str, Any] = {}
    if not path.exists():
        return ids, payloads
    cmd_re = re.compile(r"cmd-[0-9a-f]{12}")
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if "ACK" not in line.upper():
                continue
            m = cmd_re.search(line)
            if not m:
                continue
            cid = m.group(0)
            ids.add(cid)
            payloads[cid] = {"source": "rx_log", "line": line.strip()[-1000:]}
    return ids, payloads


def main() -> int:
    p = argparse.ArgumentParser(description="Meshtastic fallback worker for queued control commands")
    p.add_argument("--queue-path", default=os.environ.get("FALLBACK_COMMAND_QUEUE", DEFAULT_QUEUE))
    p.add_argument("--out-dir", default=os.environ.get("OUT_DIR", DEFAULT_OUT_DIR))
    p.add_argument("--connectivity-mode-file", default=os.environ.get("CONNECTIVITY_MODE_FILE", ""))
    p.add_argument("--required-mode", default="MESH_ONLY")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--once", action="store_true", help="single pass")
    p.add_argument("--loop-seconds", type=float, default=10.0)
    p.add_argument("--max-send-per-pass", type=int, default=10)
    p.add_argument("--retry-base-sec", type=float, default=5.0)
    p.add_argument("--retry-max-sec", type=float, default=120.0)
    p.add_argument("--meshtastic-cli", default=os.environ.get("MESHTASTIC_CLI", "meshtastic"))
    p.add_argument("--meshtastic-extra-args", default=os.environ.get("MESHTASTIC_EXTRA_ARGS", ""))
    p.add_argument("--ack-file", default=os.environ.get("FALLBACK_ACK_FILE", ""), help="JSONL records with command_id")
    p.add_argument("--rx-log", default=os.environ.get("MESHTASTIC_RX_LOG", ""), help="Meshtastic RX log to parse for ACK lines")

    args = p.parse_args()

    queue_path = Path(args.queue_path)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    events_path = out_dir / "fallback_worker_events.jsonl"
    lock_path = out_dir / "meshtastic_fallback_worker.lock"

    adapter = build_adapter(args)

    def run_pass() -> None:
        now = utc_now()

        # Respect MESH_ONLY window if requested.
        if args.connectivity_mode_file:
            mode_path = Path(args.connectivity_mode_file)
            mode = mode_path.read_text(encoding="utf-8").strip() if mode_path.exists() else ""
            if mode and mode != args.required_mode:
                append_event(events_path, {
                    "timestamp_utc": utc_now_iso(),
                    "event_type": "SKIP_MODE_MISMATCH",
                    "current_mode": mode,
                    "required_mode": args.required_mode,
                })
                return

        rows = load_jsonl(queue_path)

        # ACK ingestion pass first.
        ack_ids: set[str] = set()
        ack_payloads: dict[str, Any] = {}
        if args.ack_file:
            ids, payloads = parse_manual_ack_file(Path(args.ack_file))
            ack_ids |= ids
            ack_payloads.update(payloads)
        if args.rx_log:
            ids, payloads = parse_rx_log(Path(args.rx_log))
            ack_ids |= ids
            ack_payloads.update(payloads)
        acked = ingest_acks(rows, ack_ids, ack_payloads, events_path)

        sent_count = 0
        expired_count = 0
        error_count = 0
        for rec in rows:
            status = rec.get("status")
            if status == "acked":
                continue

            if should_expire(rec, now):
                if rec.get("status") != "expired":
                    rec["status"] = "expired"
                    rec["expired_at_utc"] = utc_now_iso()
                    append_event(events_path, {
                        "timestamp_utc": utc_now_iso(),
                        "event_type": "COMMAND_EXPIRED",
                        "command_id": rec.get("command_id"),
                    })
                    expired_count += 1
                continue

            if status not in {"queued", "sent"}:
                continue

            ack_required = int(rec.get("ack_required", 1) or 0)
            if status == "sent" and not ack_required:
                rec["status"] = "acked"
                rec["acked_at_utc"] = utc_now_iso()
                continue

            # retry/send logic
            if sent_count >= args.max_send_per_pass:
                break
            if not should_retry(rec, now, args.retry_base_sec, args.retry_max_sec):
                continue

            cid = str(rec.get("command_id", ""))
            target = str(rec.get("target_node_id", ""))
            payload = str(rec.get("command", ""))
            if not cid or not target or not payload:
                rec["status"] = "error"
                rec["last_error"] = "missing_fields"
                rec["last_error_at_utc"] = utc_now_iso()
                error_count += 1
                continue

            result = adapter.send(target=target, payload=payload, command_id=cid)
            rec["send_attempts"] = int(rec.get("send_attempts", 0) or 0) + 1
            rec["last_attempt_at_utc"] = utc_now_iso()
            rec["last_transport"] = result.get("transport")
            rec["last_transport_result"] = result

            if result.get("ok"):
                rec["status"] = "sent"
                rec["sent_at_utc"] = rec.get("sent_at_utc") or utc_now_iso()
                rec["sent_via"] = result.get("transport")
                sent_count += 1
                if not ack_required:
                    rec["status"] = "acked"
                    rec["acked_at_utc"] = utc_now_iso()
                append_event(events_path, {
                    "timestamp_utc": utc_now_iso(),
                    "event_type": "COMMAND_SENT",
                    "command_id": cid,
                    "target_node_id": target,
                    "attempt": rec["send_attempts"],
                    "transport": result.get("transport"),
                })
            else:
                backoff = min(args.retry_max_sec, args.retry_base_sec * (2 ** (rec["send_attempts"] - 1)))
                rec["next_attempt_after_utc"] = (utc_now() + dt.timedelta(seconds=backoff)).isoformat()
                rec["last_error"] = result.get("stderr") or "transport_send_failed"
                rec["last_error_at_utc"] = utc_now_iso()
                error_count += 1
                append_event(events_path, {
                    "timestamp_utc": utc_now_iso(),
                    "event_type": "COMMAND_SEND_FAILED",
                    "command_id": cid,
                    "attempt": rec["send_attempts"],
                    "next_attempt_after_utc": rec["next_attempt_after_utc"],
                })

        write_jsonl(queue_path, rows)
        append_event(events_path, {
            "timestamp_utc": utc_now_iso(),
            "event_type": "WORKER_PASS_SUMMARY",
            "queue_path": str(queue_path),
            "acked_ingested": acked,
            "sent_count": sent_count,
            "expired_count": expired_count,
            "error_count": error_count,
            "adapter": adapter.__class__.__name__,
        })

    with lock_path.open("w", encoding="utf-8") as lockf:
        try:
            fcntl.flock(lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            append_event(events_path, {
                "timestamp_utc": utc_now_iso(),
                "event_type": "SKIP_LOCK_HELD",
                "lock_path": str(lock_path),
            })
            return 0

        run_pass()
        if args.once:
            return 0
        try:
            while True:
                import time

                time.sleep(args.loop_seconds)
                run_pass()
        except KeyboardInterrupt:
            return 0


if __name__ == "__main__":
    sys.exit(main())
