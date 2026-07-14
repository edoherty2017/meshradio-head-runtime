#!/usr/bin/env python3
"""Authenticated inbound-command responder for the fallback control plane.

This is a hardware-free library. The radio is injected as a ``transport`` object
exposing ``send(target, payload, command_id) -> dict`` (the same shape the
fallback worker's adapters return, ``{"ok": bool, ...}``). Nothing here imports a
serial or Meshtastic module, so it runs and is fully testable on any host.

Responsibilities
----------------
- **Authentication**: every inbound command carries an HMAC-SHA256 ``auth_tag``
  (see ``command_auth``). A missing/forged tag is rejected before any effect.
- **Replay protection**: per-sender monotonic nonce with a bounded reordering
  window plus a seen-nonce cache. Reused or stale nonces are rejected.
- **Duplicate suppression**: a command_id already decided returns the *cached*
  decision without re-running the handler (at-most-once execution). The cached
  response is re-sent so a lost ACK still gets redelivered (at-least-once ACK).
- **Timeouts**: commands past ``issued_at_utc + ttl_sec`` are rejected as
  expired; commands dated too far in the future are rejected as clock-skewed.
- **Retries**: the outbound response is retried across transient transport
  failures with bounded backoff (injectable sleeper for tests).
- **Restart safety**: all of the above lives in a durable JSON state file
  written atomically; a fresh instance pointed at the same path resumes with
  full dedup/replay history.
- **Operator controls**: a kill-switch file halts all responses, and an
  allowlist restricts which command verbs are permitted.

The decision recorded for each command is persisted *before* the response is
transmitted, so a crash mid-send never causes the handler to run twice.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import hmac
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Protocol

# Make the sibling command_auth importable whether this file is run as a script
# or loaded by path (importlib) in tests.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
import command_auth

STATE_VERSION = 1


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def parse_ts(ts: Any) -> dt.datetime | None:
    if not isinstance(ts, str) or not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        parsed = dt.datetime.fromisoformat(ts)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(dt.timezone.utc)


class Transport(Protocol):
    def send(self, target: str, payload: str, command_id: str) -> dict[str, Any]:
        ...


class Rejected(Exception):
    """Raised internally to short-circuit handling with a machine reason."""

    def __init__(self, reason: str, detail: str | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


def default_handler(record: dict[str, Any]) -> dict[str, Any]:
    """Trivial application handler: acknowledge receipt of the command verb."""
    verb = str(record.get("command", "")).split(maxsplit=1)[0] if record.get("command") else ""
    return {"status": "ok", "verb": verb}


def _worker_ack_canonical_bytes(record: dict[str, Any]) -> bytes:
    """All-fields-except-auth_tag canonical bytes.

    Matches ``meshtastic_fallback_worker.ack_message`` so ACK records emitted
    here can be ingested by the existing worker ``--ack-file`` path. Kept as a
    local copy to avoid importing the worker (which parses argv on import path
    only under ``__main__``, but the import still pulls its transport stack).
    """
    authenticated = {k: v for k, v in record.items() if k != command_auth.AUTH_TAG_FIELD}
    return json.dumps(
        authenticated, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


class CommandResponder:
    def __init__(
        self,
        *,
        key: bytes,
        transport: Transport,
        state_path: str | os.PathLike[str],
        responder_id: str = "responder",
        handler: Callable[[dict[str, Any]], dict[str, Any]] = default_handler,
        allowlist: set[str] | None = None,
        kill_switch_path: str | os.PathLike[str] | None = None,
        tag_len: int = command_auth.FULL_TAG_LEN,
        nonce_window: int = 64,
        max_clock_skew_sec: float = 120.0,
        default_ttl_sec: float = 900.0,
        max_send_retries: int = 3,
        retry_backoff_sec: float = 0.5,
        retry_backoff_max_sec: float = 8.0,
        resend_on_duplicate: bool = True,
        max_processed: int = 5000,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not key:
            raise ValueError("HMAC key must be non-empty")
        if nonce_window < 0:
            raise ValueError("nonce_window must be >= 0")
        if max_send_retries < 0:
            raise ValueError("max_send_retries must be >= 0")
        self.key = key
        self.transport = transport
        self.state_path = Path(state_path)
        self.responder_id = responder_id
        self.handler = handler
        self.allowlist = set(allowlist) if allowlist is not None else None
        self.kill_switch_path = Path(kill_switch_path) if kill_switch_path else None
        self.tag_len = tag_len
        self.nonce_window = nonce_window
        self.max_clock_skew_sec = max_clock_skew_sec
        self.default_ttl_sec = default_ttl_sec
        self.max_send_retries = max_send_retries
        self.retry_backoff_sec = retry_backoff_sec
        self.retry_backoff_max_sec = retry_backoff_max_sec
        self.resend_on_duplicate = resend_on_duplicate
        self.max_processed = max_processed
        self.sleeper = sleeper
        self.state = self._load_state()

    # ------------------------------------------------------------------ state
    def _load_state(self) -> dict[str, Any]:
        if self.state_path.exists():
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError(f"responder state at {self.state_path} is not an object")
            raw.setdefault("version", STATE_VERSION)
            raw.setdefault("processed", {})
            raw.setdefault("nonce_high_water", {})
            raw.setdefault("seen_nonces", {})
            return raw
        return {
            "version": STATE_VERSION,
            "responder_id": self.responder_id,
            "processed": {},
            "nonce_high_water": {},
            "seen_nonces": {},
        }

    def _persist(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{self.state_path.name}.", dir=self.state_path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                os.fchmod(f.fileno(), 0o600)
                json.dump(self.state, f, separators=(",", ":"), allow_nan=False, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_name, self.state_path)
            dir_fd = os.open(self.state_path.parent, os.O_RDONLY)
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

    # ------------------------------------------------------------- validation
    def _kill_switch_engaged(self) -> bool:
        if not self.kill_switch_path or not self.kill_switch_path.exists():
            return False
        try:
            content = self.kill_switch_path.read_text(encoding="utf-8").strip().upper()
        except OSError:
            # Unreadable kill switch fails closed: treat presence as engaged.
            return True
        # An empty file (touch) engages; explicit "DISARMED"/"OFF" stands down.
        if content in {"DISARMED", "OFF", "0", "FALSE"}:
            return False
        return True

    @staticmethod
    def _parse_record(raw: Any) -> dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8")
        if isinstance(raw, str):
            obj = json.loads(
                raw, parse_constant=lambda v: (_ for _ in ()).throw(ValueError(v))
            )
            if not isinstance(obj, dict):
                raise Rejected("malformed", "command is not a JSON object")
            return obj
        raise Rejected("malformed", f"unsupported command type {type(raw).__name__}")

    def _validate_fields(self, rec: dict[str, Any]) -> None:
        command_id = rec.get("command_id")
        sender = rec.get("sender")
        command = rec.get("command")
        nonce = rec.get("nonce")
        if not isinstance(command_id, str) or not command_id:
            raise Rejected("missing_fields", "command_id")
        if not isinstance(sender, str) or not sender:
            raise Rejected("missing_fields", "sender")
        if not isinstance(command, str) or not command:
            raise Rejected("missing_fields", "command")
        # bool is an int subclass; reject it explicitly.
        if not isinstance(nonce, int) or isinstance(nonce, bool):
            raise Rejected("missing_fields", "nonce")

    def _check_ttl(self, rec: dict[str, Any], now: dt.datetime) -> None:
        issued = parse_ts(rec.get("issued_at_utc"))
        if issued is None:
            raise Rejected("missing_fields", "issued_at_utc")
        skew = (issued - now).total_seconds()
        if skew > self.max_clock_skew_sec:
            raise Rejected("future_dated", f"issued {skew:.0f}s in the future")
        ttl = rec.get("ttl_sec", self.default_ttl_sec)
        try:
            ttl = float(ttl)
        except (TypeError, ValueError):
            raise Rejected("missing_fields", "ttl_sec")
        if ttl > 0 and (now - issued).total_seconds() > ttl:
            raise Rejected("expired", f"age {(now - issued).total_seconds():.0f}s > ttl {ttl:.0f}s")

    def _check_allowlist(self, rec: dict[str, Any]) -> None:
        if self.allowlist is None:
            return
        verb = str(rec["command"]).split(maxsplit=1)[0]
        if verb not in self.allowlist:
            raise Rejected("command_not_allowed", verb)

    def _check_replay(self, rec: dict[str, Any]) -> None:
        sender = rec["sender"]
        nonce = int(rec["nonce"])
        seen = self.state["seen_nonces"].get(sender, [])
        high_water = int(self.state["nonce_high_water"].get(sender, -(1 << 62)))
        if nonce in seen:
            raise Rejected("replay_nonce_reused", f"nonce {nonce} already seen for {sender}")
        if self.nonce_window >= 0 and nonce <= high_water - self.nonce_window:
            raise Rejected(
                "replay_nonce_stale",
                f"nonce {nonce} below window (high_water {high_water}, window {self.nonce_window})",
            )

    def _record_nonce(self, sender: str, nonce: int) -> None:
        high_water = max(int(self.state["nonce_high_water"].get(sender, -(1 << 62))), nonce)
        self.state["nonce_high_water"][sender] = high_water
        floor = high_water - self.nonce_window
        seen = [n for n in self.state["seen_nonces"].get(sender, []) if n > floor]
        if nonce > floor and nonce not in seen:
            seen.append(nonce)
        self.state["seen_nonces"][sender] = sorted(seen)

    # -------------------------------------------------------------- transport
    def _build_response(self, rec: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
        response = {
            "command_id": rec["command_id"],
            "nonce": int(rec["nonce"]),
            "sender": self.responder_id,
            "issued_at_utc": utc_now_iso(),
            "ttl_sec": 0,
            "target": rec.get("sender", ""),
            "command": f"ACK {json.dumps(result, separators=(',', ':'), sort_keys=True)}",
        }
        response[command_auth.AUTH_TAG_FIELD] = command_auth.sign(
            self.key, response, tag_len=self.tag_len
        )
        return response

    def worker_ack_record(self, rec: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
        """Emit an ACK record compatible with the worker ``--ack-file`` path.

        The worker authenticates ACK-file records over *all* fields except
        ``auth_tag`` (see ``meshtastic_fallback_worker.ack_message``), so this
        uses that convention rather than the subset-field command signature.
        """
        record = {
            "command_id": rec["command_id"],
            "payload": {"responder_id": self.responder_id, "result": result},
            "acked_at_utc": utc_now_iso(),
        }
        record[command_auth.AUTH_TAG_FIELD] = hmac.new(
            self.key, _worker_ack_canonical_bytes(record), hashlib.sha256
        ).hexdigest()
        return record

    def _send_with_retries(self, rec: dict[str, Any], response: dict[str, Any]) -> dict[str, Any]:
        target = str(rec.get("sender", ""))
        payload = json.dumps(response, separators=(",", ":"), sort_keys=True, allow_nan=False)
        command_id = str(rec["command_id"])
        last_result: dict[str, Any] = {"ok": False, "error": "no_attempt"}
        attempts = self.max_send_retries + 1
        for attempt in range(1, attempts + 1):
            try:
                result = self.transport.send(target=target, payload=payload, command_id=command_id)
            except Exception as exc:  # transport raised: treat as retriable failure
                result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
            last_result = result
            if isinstance(result, dict) and result.get("ok"):
                result.setdefault("attempts", attempt)
                return result
            if attempt < attempts:
                backoff = min(
                    self.retry_backoff_max_sec,
                    self.retry_backoff_sec * (2 ** (attempt - 1)),
                )
                self.sleeper(backoff)
        if isinstance(last_result, dict):
            last_result.setdefault("attempts", attempts)
        return last_result if isinstance(last_result, dict) else {"ok": False, "error": "send_failed"}

    # ------------------------------------------------------------------ entry
    def handle(self, raw: Any) -> dict[str, Any]:
        """Authenticate, act on, and respond to a single inbound command.

        Returns a machine-readable decision dict. Never raises for an
        untrusted/malformed message; raises only on genuine I/O faults.
        """
        now = utc_now()
        try:
            rec = self._parse_record(raw)
        except Rejected as rej:
            return self._reject(None, rej)
        except (ValueError, UnicodeDecodeError) as exc:
            return self._reject(None, Rejected("malformed", str(exc)))

        if self._kill_switch_engaged():
            return {
                "accepted": False,
                "reason": "kill_switch_engaged",
                "command_id": rec.get("command_id"),
                "response_sent": False,
            }

        try:
            self._validate_fields(rec)
        except Rejected as rej:
            return self._reject(rec, rej)

        if not command_auth.verify(self.key, rec, tag_len=self.tag_len):
            return self._reject(rec, Rejected("bad_hmac", "auth_tag verification failed"))

        command_id = rec["command_id"]
        sender = rec["sender"]

        # Duplicate suppression: return the cached decision, do not re-run the
        # handler. Optionally resend the cached response for lost-ACK recovery.
        cached = self.state["processed"].get(command_id)
        if cached is not None:
            if cached.get("sender") not in (None, sender):
                # command_id reused by a different sender: authenticated but
                # ambiguous — refuse rather than serve another sender's result.
                return self._reject(rec, Rejected("command_id_conflict", command_id))
            response_sent = bool(cached.get("response_sent"))
            if self.resend_on_duplicate and cached.get("response") is not None:
                resend = self._send_with_retries(rec, cached["response"])
                if resend.get("ok"):
                    response_sent = True
                    if not cached.get("response_sent"):
                        cached["response_sent"] = True
                        self._persist()
            return {
                "accepted": True,
                "duplicate": True,
                "reason": "duplicate_suppressed",
                "command_id": command_id,
                "response": cached.get("response"),
                "response_sent": response_sent,
                "handler_invoked": False,
            }

        try:
            self._check_ttl(rec, now)
            self._check_replay(rec)
            self._check_allowlist(rec)
        except Rejected as rej:
            return self._reject(rec, rej)

        # Execute the application handler exactly once, then durably record the
        # decision BEFORE transmitting so a mid-send crash cannot re-run it.
        result = self.handler(rec)
        response = self._build_response(rec, result)
        self._record_nonce(sender, int(rec["nonce"]))
        self.state["processed"][command_id] = {
            "nonce": int(rec["nonce"]),
            "sender": sender,
            "decided_at_utc": utc_now_iso(),
            "response": response,
            "response_sent": False,
        }
        self._prune_processed()
        self._persist()

        send_result = self._send_with_retries(rec, response)
        response_sent = bool(send_result.get("ok"))
        if response_sent:
            self.state["processed"][command_id]["response_sent"] = True
            self._persist()

        return {
            "accepted": True,
            "duplicate": False,
            "reason": "processed",
            "command_id": command_id,
            "result": result,
            "response": response,
            "response_sent": response_sent,
            "send_result": send_result,
            "handler_invoked": True,
        }

    def _reject(self, rec: dict[str, Any] | None, rej: Rejected) -> dict[str, Any]:
        return {
            "accepted": False,
            "reason": rej.reason,
            "detail": rej.detail,
            "command_id": (rec or {}).get("command_id"),
            "response_sent": False,
        }

    def _prune_processed(self) -> None:
        processed = self.state["processed"]
        if len(processed) <= self.max_processed:
            return
        # Drop the oldest decisions by decided_at_utc.
        ordered = sorted(processed.items(), key=lambda kv: kv[1].get("decided_at_utc", ""))
        for command_id, _ in ordered[: len(processed) - self.max_processed]:
            del processed[command_id]


# --------------------------------------------------------------------- CLI ---
class _StdoutTransport:
    """Trivial CLI transport: prints the outbound response as one JSON line."""

    def send(self, target: str, payload: str, command_id: str) -> dict[str, Any]:
        sys.stdout.write(json.dumps({"outbound": json.loads(payload), "target": target}) + "\n")
        return {"ok": True, "transport": "stdout"}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Authenticate and respond to inbound control commands.")
    p.add_argument("--key-file", required=True, help="File containing the raw HMAC key bytes")
    p.add_argument("--state-path", required=True, help="Durable responder state JSON path")
    p.add_argument("--responder-id", default=os.environ.get("RESPONDER_ID", "responder"))
    p.add_argument("--allow", action="append", default=None, help="Permitted command verb (repeatable)")
    p.add_argument("--kill-switch-file", default=os.environ.get("RESPONDER_KILL_SWITCH", None))
    p.add_argument("--tag-len", type=int, default=command_auth.FULL_TAG_LEN)
    p.add_argument("--nonce-window", type=int, default=64)
    p.add_argument("--command-file", default="-", help="JSON command file, or '-' for stdin")
    args = p.parse_args(argv)

    key = Path(args.key_file).read_bytes()
    if args.command_file == "-":
        raw = sys.stdin.read()
    else:
        raw = Path(args.command_file).read_text(encoding="utf-8")

    responder = CommandResponder(
        key=key,
        transport=_StdoutTransport(),
        state_path=args.state_path,
        responder_id=args.responder_id,
        allowlist=set(args.allow) if args.allow else None,
        kill_switch_path=args.kill_switch_file,
        tag_len=args.tag_len,
        nonce_window=args.nonce_window,
    )
    decision = responder.handle(raw)
    print(json.dumps(decision, separators=(",", ":"), sort_keys=True))
    return 0 if decision.get("accepted") else 2


if __name__ == "__main__":
    raise SystemExit(main())
