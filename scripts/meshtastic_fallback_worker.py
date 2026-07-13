#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import hmac
import json
import os
import re
import shlex
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


DEFAULT_QUEUE = "/home/pump/telemetry_head/fallback_commands.jsonl"
DEFAULT_OUT_DIR = "/home/pump/telemetry_head"
BROADCAST_TARGETS = {"*", "all", "broadcast", "^all"}


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
        parsed = dt.datetime.fromisoformat(ts)
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(dt.timezone.utc)
    except Exception:
        return None


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            record = json.loads(
                line,
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
            )
            if not isinstance(record, dict):
                raise ValueError(f"record on line {line_no} of {path} is not an object")
            rows.append(record)
    return rows


def lock_path_for(path: Path) -> Path:
    return Path(f"{path}.lock")


@contextlib.contextmanager
def queue_lock(path: Path):
    lock_path = lock_path_for(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock_file, fcntl.LOCK_EX)
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


def append_event(path: Path, event: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, separators=(",", ":"), allow_nan=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def load_node_map(path: Path | None) -> dict[str, str]:
    if not path or not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"node map is not an object: {path}")

    mapping: dict[str, str] = {}

    aliases = raw.get("aliases")
    if isinstance(aliases, dict):
        for k, v in aliases.items():
            if isinstance(k, str) and isinstance(v, str):
                mapping[k.strip()] = v.strip()

    nodes = raw.get("nodes")
    if isinstance(nodes, list):
        for item in nodes:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            node_id = item.get("id")
            if isinstance(name, str) and isinstance(node_id, str):
                mapping[name.strip()] = node_id.strip()

    return mapping


def is_valid_mesh_dest(target: str) -> bool:
    t = target.strip()
    if t in BROADCAST_TARGETS:
        return True
    if re.fullmatch(r"!?[0-9a-fA-F]{8}", t):
        return True
    if re.fullmatch(r"[0-9]+", t):
        return True
    return False


def resolve_target(target: str, node_map: dict[str, str]) -> tuple[str, str]:
    t = target.strip()
    if t in BROADCAST_TARGETS:
        return t, "broadcast"
    if is_valid_mesh_dest(t):
        return t, "direct"
    mapped = node_map.get(t)
    if mapped and is_valid_mesh_dest(mapped):
        return mapped, "mapped"
    return t, "unresolved"


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
    def __init__(self, cli_cmd: str, extra_args: str = "", port: str = "", timeout: float = 10.0) -> None:
        self.cli_cmd = shlex.split(cli_cmd)
        self.extra_args = shlex.split(extra_args) if extra_args else []
        self.port = port
        self.timeout = timeout

    def send(self, target: str, payload: str, command_id: str) -> dict[str, Any]:
        tagged = f"[cmd:{command_id}] {payload}"
        cmd = [*self.cli_cmd]
        if self.port:
            cmd.extend(["--port", self.port])
        if target not in BROADCAST_TARGETS:
            cmd.extend(["--dest", target])
        cmd.extend(["--sendtext", tagged, *self.extra_args])
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=self.timeout)
        except subprocess.TimeoutExpired as exc:
            return {
                "ok": False,
                "transport": "meshtastic_cli",
                "error": "transport_timeout",
                "stderr": str(exc),
            }
        ok = proc.returncode == 0
        return {
            "ok": ok,
            "transport": "meshtastic_cli",
            "command": cmd,
            "returncode": proc.returncode,
            "stdout": proc.stdout[-2000:],
            "stderr": proc.stderr[-2000:],
        }


class CollectorIpcAdapter(TransportAdapter):
    def __init__(self, socket_path: str, timeout: float = 10.0) -> None:
        self.socket_path = socket_path
        self.timeout = timeout

    def send(self, target: str, payload: str, command_id: str) -> dict[str, Any]:
        tagged = f"[cmd:{command_id}] {payload}"
        request = json.dumps(
            {"operation": "send_text", "target": target, "payload": tagged},
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(self.timeout)
                client.connect(self.socket_path)
                client.sendall(request)
                raw = b""
                while b"\n" not in raw and len(raw) <= 8192:
                    chunk = client.recv(8192 - len(raw))
                    if not chunk:
                        break
                    raw += chunk
            response = json.loads(raw.split(b"\n", 1)[0].decode("utf-8"))
            if not isinstance(response, dict):
                raise ValueError("collector response is not an object")
            response.setdefault("transport", "collector_ipc")
            return response
        except Exception as exc:
            return {
                "ok": False,
                "transport": "collector_ipc",
                "error": f"{type(exc).__name__}: {exc}",
            }


class UnavailableAdapter(TransportAdapter):
    def send(self, target: str, payload: str, command_id: str) -> dict[str, Any]:
        return {"ok": False, "transport": "unavailable", "error": "no_transport_configured"}


def build_adapter(args: argparse.Namespace) -> TransportAdapter:
    if args.dry_run:
        return DryRunAdapter()
    if args.collector_ipc_socket:
        return CollectorIpcAdapter(args.collector_ipc_socket, args.transport_timeout_sec)
    cli = args.meshtastic_cli
    if cli:
        # Accept full command strings (e.g. "python3 -m meshtastic")
        cli_head = shlex.split(cli)[0]
        if Path(cli_head).exists() or shutil_which(cli_head):
            return MeshtasticCliAdapter(cli_cmd=cli, extra_args=args.meshtastic_extra_args, port=args.meshtastic_port, timeout=args.transport_timeout_sec)
    found = shutil_which("meshtastic")
    if found:
        return MeshtasticCliAdapter(cli_cmd=found, extra_args=args.meshtastic_extra_args, port=args.meshtastic_port, timeout=args.transport_timeout_sec)
    # common fallback when only the Python module is installed
    if shutil_which("python3"):
        return MeshtasticCliAdapter(cli_cmd="python3 -m meshtastic", extra_args=args.meshtastic_extra_args, port=args.meshtastic_port, timeout=args.transport_timeout_sec)
    return UnavailableAdapter()


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
            payload = ack_payload_map.get(cid)
            ack_source = (
                "unauthenticated_rx_log"
                if isinstance(payload, dict) and payload.get("source") == "rx_log"
                else "authenticated_hmac_file"
            )
            rec["ack_source"] = ack_source
            rec["status_source"] = ack_source
            if cid in ack_payload_map:
                rec["ack_payload"] = ack_payload_map[cid]
            append_event(events_path, {
                "timestamp_utc": utc_now_iso(),
                "event_type": "ACK_INGESTED",
                "command_id": cid,
                "ack_source": ack_source,
            })
            updated += 1
    return updated


def ack_message(rec: dict[str, Any]) -> bytes:
    authenticated = {k: v for k, v in rec.items() if k != "auth_tag"}
    return json.dumps(authenticated, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def parse_manual_ack_file(path: Path, key: bytes) -> tuple[set[str], dict[str, Any]]:
    ids: set[str] = set()
    payloads: dict[str, Any] = {}
    if not path.exists():
        return ids, payloads
    for rec in load_jsonl(path):
        cid = rec.get("command_id")
        supplied = str(rec.get("auth_tag", ""))
        expected = hmac.new(key, ack_message(rec), hashlib.sha256).hexdigest()
        if cid and hmac.compare_digest(supplied, expected):
            ids.add(cid)
            payloads[cid] = rec
    return ids, payloads


def parse_rx_log(path: Path) -> tuple[set[str], dict[str, Any]]:
    ids: set[str] = set()
    payloads: dict[str, Any] = {}
    if not path.exists():
        return ids, payloads
    cmd_re = re.compile(r"cmd-[0-9a-f]{12}")
    with path.open("r", encoding="utf-8") as f:
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
    p.add_argument(
        "--connectivity-mode-file",
        default=os.environ.get(
            "CONNECTIVITY_MODE_FILE",
            "/home/pump/telemetry_head/connectivity_mode.state",
        ),
        help="Required observed connectivity state; missing/empty state prevents sending",
    )
    p.add_argument("--required-mode", default="MESH_ONLY", choices=["IP_FULL", "IP_DEGRADED", "MESH_ONLY"])
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--once", action="store_true", help="single pass")
    p.add_argument("--loop-seconds", type=float, default=10.0)
    p.add_argument("--max-send-per-pass", type=int, default=10)
    p.add_argument("--retry-base-sec", type=float, default=5.0)
    p.add_argument("--retry-max-sec", type=float, default=120.0)
    p.add_argument("--meshtastic-cli", default=os.environ.get("MESHTASTIC_CLI", "meshtastic"))
    p.add_argument("--meshtastic-extra-args", default=os.environ.get("MESHTASTIC_EXTRA_ARGS", ""))
    p.add_argument("--meshtastic-port", default=os.environ.get("MESHTASTIC_PORT", ""))
    p.add_argument("--collector-ipc-socket", default=os.environ.get("COLLECTOR_IPC_SOCKET", "/run/manet/collector.sock"), help="Unix socket exposed by the sole serial-owning collector; set empty only for legacy CLI mode")
    p.add_argument("--transport-timeout-sec", type=float, default=10.0)
    p.add_argument("--max-command-bytes", type=int, default=int(os.environ.get("MAX_FALLBACK_COMMAND_BYTES", "160")))
    p.add_argument("--ack-file", default=os.environ.get("FALLBACK_ACK_FILE", ""), help="JSONL records with command_id")
    p.add_argument("--ack-hmac-key-file", default=os.environ.get("FALLBACK_ACK_HMAC_KEY_FILE", ""), help="Required HMAC-SHA256 key for ACK-file records")
    p.add_argument("--rx-log", default=os.environ.get("MESHTASTIC_RX_LOG", ""), help="Meshtastic RX log to parse for ACK lines")
    p.add_argument("--allow-unauthenticated-rx-ack", action="store_true", help="Explicitly accept unauthenticated ACK text from an RX log (unsafe for production)")
    p.add_argument("--node-map-file", default=os.environ.get("MESHTASTIC_NODE_MAP_FILE", ""), help="JSON file mapping logical targets to Meshtastic node IDs")
    p.add_argument("--serial-lock-path", default=os.environ.get("SERIAL_LOCK_PATH", "/run/manet/lora_serial.lock"), help="Legacy CLI-only serial lock; normal deployment uses collector IPC")

    args = p.parse_args()
    if args.max_send_per_pass <= 0:
        p.error("--max-send-per-pass must be greater than zero")
    if args.loop_seconds <= 0 or args.retry_base_sec <= 0 or args.retry_max_sec <= 0:
        p.error("loop and retry intervals must be greater than zero")
    if args.retry_max_sec < args.retry_base_sec:
        p.error("--retry-max-sec must be at least --retry-base-sec")
    if args.transport_timeout_sec <= 0 or args.max_command_bytes <= 0:
        p.error("transport timeout and maximum command bytes must be greater than zero")

    queue_path = Path(args.queue_path)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    events_path = out_dir / "fallback_worker_events.jsonl"
    lock_path = out_dir / "meshtastic_fallback_worker.lock"

    adapter = build_adapter(args)

    def run_pass() -> None:
        now = utc_now()
        node_map = load_node_map(Path(args.node_map_file)) if args.node_map_file else {}

        # The collector owns the serial interface and sends on behalf of the
        # worker over IPC. The lock path is retained only for explicitly
        # configured legacy CLI transports.
        serial_lock_fh = None
        if isinstance(adapter, MeshtasticCliAdapter):
            try:
                serial_lock_path = Path(args.serial_lock_path)
                serial_lock_fh = serial_lock_path.open("a+", encoding="utf-8")
                fcntl.flock(serial_lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except Exception as exc:
                append_event(events_path, {
                    "timestamp_utc": utc_now_iso(),
                    "event_type": "SKIP_SERIAL_LOCK_UNAVAILABLE",
                    "lock_path": str(args.serial_lock_path),
                    "error": f"{type(exc).__name__}: {exc}",
                })
                return

        try:
            _run_pass_inner(now, node_map)
        finally:
            if serial_lock_fh:
                try:
                    fcntl.flock(serial_lock_fh, fcntl.LOCK_UN)
                    serial_lock_fh.close()
                except Exception:
                    pass

    def _run_pass_inner(now: "dt.datetime", node_map: "dict[str, str]") -> None:
        # Sending is mode-gated, but authenticated ACK ingestion is not. An ACK
        # received just before IP connectivity recovers must still close its
        # command instead of waiting for a future outage.
        mode_allows_send = True
        mode = ""
        if args.connectivity_mode_file:
            mode_path = Path(args.connectivity_mode_file)
            mode = mode_path.read_text(encoding="utf-8").strip() if mode_path.exists() else ""
            mode_allows_send = mode == args.required_mode

        with queue_lock(queue_path):
            rows = load_jsonl(queue_path)

            # ACK ingestion is fail-closed. File ACKs require an HMAC key;
            # RX-log ACKs require an explicit unsafe development flag.
            ack_ids: set[str] = set()
            ack_payloads: dict[str, Any] = {}
            if args.ack_file and args.ack_hmac_key_file:
                key = Path(args.ack_hmac_key_file).read_bytes()
                if not key:
                    raise ValueError("ACK HMAC key is empty")
                ids, payloads = parse_manual_ack_file(Path(args.ack_file), key)
                ack_ids |= ids
                ack_payloads.update(payloads)
            elif args.ack_file:
                append_event(events_path, {
                    "timestamp_utc": utc_now_iso(),
                    "event_type": "ACK_FILE_IGNORED_NO_HMAC_KEY",
                    "ack_file": args.ack_file,
                })
            if args.rx_log and args.allow_unauthenticated_rx_ack:
                ids, payloads = parse_rx_log(Path(args.rx_log))
                ack_ids |= ids
                ack_payloads.update(payloads)
            acked = ingest_acks(rows, ack_ids, ack_payloads, events_path)

            if not mode_allows_send:
                if acked:
                    write_jsonl(queue_path, rows)
                append_event(events_path, {
                    "timestamp_utc": utc_now_iso(),
                    "event_type": "SKIP_MODE_MISMATCH",
                    "current_mode": mode,
                    "required_mode": args.required_mode,
                    "acked_ingested": acked,
                })
                return

            sent_count = 0
            attempt_count = 0
            dry_run_count = 0
            expired_count = 0
            error_count = 0
            queued_rows = [rec for rec in rows if rec.get("status") == "queued"]
            retry_rows = [rec for rec in rows if rec.get("status") == "sent"]
            pending_rows: list[dict[str, Any]] = []
            for index in range(max(len(queued_rows), len(retry_rows))):
                if index < len(queued_rows):
                    pending_rows.append(queued_rows[index])
                if index < len(retry_rows):
                    pending_rows.append(retry_rows[index])

            # Interleave new sends and ACK-required retries so either class can
            # make progress under max-send-per-pass.
            for rec in pending_rows:
                status = rec.get("status")

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

                ack_required = int(rec.get("ack_required", 1) or 0)
                if status == "sent" and not ack_required:
                    rec["status"] = "sent_unconfirmed"
                    rec["delivery_confirmation"] = "not_requested"
                    rec["status_source"] = "transport_result"
                    continue

            # retry/send logic
                if attempt_count >= args.max_send_per_pass:
                    continue
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
                if len(payload.encode("utf-8")) > args.max_command_bytes:
                    rec["status"] = "error"
                    rec["last_error"] = "command_too_large"
                    rec["last_error_at_utc"] = utc_now_iso()
                    error_count += 1
                    continue

                resolved_target, target_source = resolve_target(target, node_map)
                rec["resolved_target_node_id"] = resolved_target
                rec["target_resolution_source"] = target_source
                if target_source == "unresolved":
                    rec["status"] = "error"
                    rec["last_error"] = "unresolved_target_node_id"
                    rec["last_error_at_utc"] = utc_now_iso()
                    error_count += 1
                    append_event(events_path, {
                        "timestamp_utc": utc_now_iso(),
                        "event_type": "COMMAND_TARGET_UNRESOLVED",
                        "command_id": cid,
                        "target_node_id": target,
                        "node_map_file": args.node_map_file,
                    })
                    continue

                attempt_count += 1
                result = adapter.send(target=resolved_target, payload=payload, command_id=cid)
                if result.get("transport") == "dry_run":
                    rec["last_dry_run_at_utc"] = utc_now_iso()
                    rec["last_dry_run_result"] = result
                    dry_run_count += 1
                    append_event(events_path, {
                        "timestamp_utc": utc_now_iso(),
                        "event_type": "COMMAND_DRY_RUN",
                        "command_id": cid,
                        "status_unchanged": rec.get("status"),
                    })
                    continue
                rec["send_attempts"] = int(rec.get("send_attempts", 0) or 0) + 1
                rec["last_attempt_at_utc"] = utc_now_iso()
                rec["last_transport"] = result.get("transport")
                rec["last_transport_result"] = result

                if result.get("ok"):
                    rec["status"] = "sent"
                    rec["status_source"] = "transport_result"
                    rec["sent_at_utc"] = rec.get("sent_at_utc") or utc_now_iso()
                    rec["sent_via"] = result.get("transport")
                    sent_count += 1
                    if not ack_required:
                        rec["status"] = "sent_unconfirmed"
                        rec["delivery_confirmation"] = "not_requested"
                    append_event(events_path, {
                        "timestamp_utc": utc_now_iso(),
                        "event_type": "COMMAND_SENT",
                        "command_id": cid,
                        "target_node_id": target,
                        "resolved_target_node_id": resolved_target,
                        "target_resolution_source": target_source,
                        "attempt": rec["send_attempts"],
                        "transport": result.get("transport"),
                    })
                else:
                    backoff = min(args.retry_max_sec, args.retry_base_sec * (2 ** (rec["send_attempts"] - 1)))
                    rec["next_attempt_after_utc"] = (utc_now() + dt.timedelta(seconds=backoff)).isoformat()
                    rec["last_error"] = result.get("error") or result.get("stderr") or "transport_send_failed"
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
            "attempt_count": attempt_count,
            "dry_run_count": dry_run_count,
            "expired_count": expired_count,
            "error_count": error_count,
            "adapter": adapter.__class__.__name__,
            "node_map_file": args.node_map_file,
            "node_map_entries": len(node_map),
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
