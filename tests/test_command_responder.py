from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


command_auth = _load("command_auth", "command_auth.py")
cr = _load("command_responder", "command_responder.py")
worker = _load("fallback_worker_ci", "meshtastic_fallback_worker.py")

KEY = b"unit-test-secret-key"


class RecordingTransport:
    """In-memory transport; records sends and can fail the first N attempts."""

    def __init__(self, fail_first: int = 0, raise_first: int = 0) -> None:
        self.sends: list[dict] = []
        self.fail_first = fail_first
        self.raise_first = raise_first
        self.attempts = 0

    def send(self, target, payload, command_id):
        self.attempts += 1
        self.sends.append({"target": target, "payload": payload, "command_id": command_id})
        if self.attempts <= self.raise_first:
            raise ConnectionError("simulated transport crash")
        if self.attempts <= self.raise_first + self.fail_first:
            return {"ok": False, "error": "simulated_nak"}
        return {"ok": True, "transport": "recording"}


def signed_command(command_id="cmd-000000000001", nonce=1, sender="head",
                   command="status", target="!1234abcd", ttl_sec=600,
                   issued_at=None, tag_len=command_auth.FULL_TAG_LEN, key=KEY):
    rec = {
        "command_id": command_id,
        "nonce": nonce,
        "sender": sender,
        "issued_at_utc": issued_at or cr.utc_now_iso(),
        "ttl_sec": ttl_sec,
        "target": target,
        "command": command,
    }
    rec["auth_tag"] = command_auth.sign(key, rec, tag_len=tag_len)
    return rec


def make_responder(tmp_path, transport=None, **kw):
    # mesh_id matches signed_command()'s default target: a responder attached to
    # a radio answers to its own !xxxxxxxx address. tag_len defaults to the full
    # digest here because signed_command() signs full-length by default; the
    # deployed 16-char wire tag is covered by the tag_len=16 tests below and by
    # the cross-runtime round-trip test.
    kw.setdefault("mesh_id", "!1234abcd")
    kw.setdefault("tag_len", command_auth.FULL_TAG_LEN)
    return cr.CommandResponder(
        key=KEY,
        transport=transport or RecordingTransport(),
        state_path=tmp_path / "state.json",
        sleeper=lambda _s: None,
        **kw,
    )


def test_valid_command_is_processed_and_response_sent(tmp_path):
    t = RecordingTransport()
    r = make_responder(tmp_path, t)
    decision = r.handle(signed_command())
    assert decision["accepted"] is True
    assert decision["handler_invoked"] is True
    assert decision["response_sent"] is True
    assert len(t.sends) == 1


def test_bad_hmac_rejected(tmp_path):
    r = make_responder(tmp_path)
    rec = signed_command()
    rec["command"] = "reboot"  # tamper after signing -> tag no longer matches
    decision = r.handle(rec)
    assert decision["accepted"] is False
    assert decision["reason"] == "bad_hmac"


def test_missing_or_empty_tag_rejected(tmp_path):
    r = make_responder(tmp_path)
    rec = signed_command()
    rec["auth_tag"] = ""
    assert r.handle(rec)["reason"] == "bad_hmac"


def test_replay_reused_nonce_rejected(tmp_path):
    r = make_responder(tmp_path)
    assert r.handle(signed_command(command_id="cmd-a", nonce=5))["accepted"]
    # Different command_id, same nonce -> replay of a captured nonce.
    decision = r.handle(signed_command(command_id="cmd-b", nonce=5))
    assert decision["accepted"] is False
    assert decision["reason"] == "replay_nonce_reused"


def test_replay_stale_nonce_rejected(tmp_path):
    r = make_responder(tmp_path, nonce_window=2)
    assert r.handle(signed_command(command_id="cmd-a", nonce=10))["accepted"]
    decision = r.handle(signed_command(command_id="cmd-b", nonce=1))
    assert decision["accepted"] is False
    assert decision["reason"] == "replay_nonce_stale"


def test_duplicate_command_id_suppressed_handler_runs_once(tmp_path):
    calls = {"n": 0}

    def handler(rec):
        calls["n"] += 1
        return {"status": "ok", "n": calls["n"]}

    t = RecordingTransport()
    r = make_responder(tmp_path, t, handler=handler)
    first = r.handle(signed_command())
    second = r.handle(signed_command())  # identical redelivery
    assert first["accepted"] and first["duplicate"] is False
    assert second["accepted"] and second["duplicate"] is True
    assert second["handler_invoked"] is False
    assert calls["n"] == 1  # handler executed exactly once
    # Cached response is resent for lost-ACK recovery.
    assert len(t.sends) == 2


def test_expired_command_rejected(tmp_path):
    r = make_responder(tmp_path)
    old = (cr.utc_now() - cr.dt.timedelta(seconds=3600)).isoformat()
    decision = r.handle(signed_command(issued_at=old, ttl_sec=60))
    assert decision["accepted"] is False
    assert decision["reason"] == "expired"


def test_future_dated_command_rejected(tmp_path):
    r = make_responder(tmp_path, max_clock_skew_sec=30)
    future = (cr.utc_now() + cr.dt.timedelta(seconds=600)).isoformat()
    decision = r.handle(signed_command(issued_at=future))
    assert decision["reason"] == "future_dated"


def test_allowlist_blocks_unlisted_verb(tmp_path):
    r = make_responder(tmp_path, allowlist={"status", "ping"})
    assert r.handle(signed_command(command="status"))["accepted"]
    blocked = r.handle(signed_command(command_id="cmd-x", nonce=2, command="reboot now"))
    assert blocked["accepted"] is False
    assert blocked["reason"] == "command_not_allowed"


def test_kill_switch_halts_all_responses(tmp_path):
    ks = tmp_path / "kill"
    ks.write_text("ENGAGED")
    t = RecordingTransport()
    r = make_responder(tmp_path, t, kill_switch_path=ks)
    decision = r.handle(signed_command())
    assert decision["accepted"] is False
    assert decision["reason"] == "kill_switch_engaged"
    assert t.sends == []
    # Disarming the switch restores operation.
    ks.write_text("DISARMED")
    assert r.handle(signed_command())["accepted"] is True


def test_retry_then_success(tmp_path):
    t = RecordingTransport(fail_first=2, raise_first=1)  # crash, nak, nak, ok
    r = make_responder(tmp_path, t, max_send_retries=5)
    decision = r.handle(signed_command())
    assert decision["response_sent"] is True
    assert t.attempts == 4
    assert decision["send_result"]["attempts"] == 4


def test_retry_exhausted_marks_unsent_but_records_decision(tmp_path):
    t = RecordingTransport(fail_first=99)
    r = make_responder(tmp_path, t, max_send_retries=2)
    decision = r.handle(signed_command())
    assert decision["accepted"] is True  # handler ran, decision durable
    assert decision["response_sent"] is False
    assert t.attempts == 3  # initial + 2 retries


def test_restart_resumes_dedup_and_nonce_state(tmp_path):
    calls = {"n": 0}

    def handler(rec):
        calls["n"] += 1
        return {"status": "ok"}

    state = tmp_path / "state.json"
    r1 = cr.CommandResponder(key=KEY, mesh_id="!1234abcd", tag_len=command_auth.FULL_TAG_LEN, transport=RecordingTransport(), nonce_window=2,
                             state_path=state, handler=handler, sleeper=lambda _s: None)
    r1.handle(signed_command(command_id="cmd-a", nonce=7))
    assert calls["n"] == 1

    # Fresh instance (simulated restart) pointed at the same durable state.
    r2 = cr.CommandResponder(key=KEY, mesh_id="!1234abcd", tag_len=command_auth.FULL_TAG_LEN, transport=RecordingTransport(), nonce_window=2,
                             state_path=state, handler=handler, sleeper=lambda _s: None)
    dup = r2.handle(signed_command(command_id="cmd-a", nonce=7))
    assert dup["duplicate"] is True
    assert calls["n"] == 1  # handler NOT re-run across restart
    # Nonce high-water also survived: an old nonce is now stale.
    stale = r2.handle(signed_command(command_id="cmd-b", nonce=1))
    assert stale["accepted"] is False
    assert stale["reason"].startswith("replay_nonce")


def test_restart_resends_response_when_prior_send_never_confirmed(tmp_path):
    state = tmp_path / "state.json"
    # First run: transport always fails, so response_sent stays False but the
    # decision is durably recorded.
    r1 = cr.CommandResponder(key=KEY, mesh_id="!1234abcd", tag_len=command_auth.FULL_TAG_LEN, transport=RecordingTransport(fail_first=99),
                             state_path=state, max_send_retries=0, sleeper=lambda _s: None)
    first = r1.handle(signed_command())
    assert first["response_sent"] is False

    # Restart with a healthy transport: redelivery resends the cached response.
    t2 = RecordingTransport()
    r2 = cr.CommandResponder(key=KEY, mesh_id="!1234abcd", tag_len=command_auth.FULL_TAG_LEN, transport=t2, state_path=state, sleeper=lambda _s: None)
    dup = r2.handle(signed_command())
    assert dup["duplicate"] is True
    assert dup["response_sent"] is True
    assert len(t2.sends) == 1


def test_malformed_input_rejected(tmp_path):
    r = make_responder(tmp_path)
    assert r.handle("{not json")["reason"] == "malformed"
    assert r.handle("[1,2,3]")["reason"] == "malformed"
    assert r.handle(signed_and_drop_field())["reason"] == "missing_fields"


def signed_and_drop_field():
    rec = signed_command()
    rec.pop("command_id")
    return rec


def test_command_id_conflict_between_senders(tmp_path):
    r = make_responder(tmp_path)
    assert r.handle(signed_command(command_id="cmd-shared", nonce=1, sender="head"))["accepted"]
    other = signed_command(command_id="cmd-shared", nonce=1, sender="relay")
    decision = r.handle(other)
    assert decision["accepted"] is False
    assert decision["reason"] == "command_id_conflict"


def test_truncated_wire_tag_roundtrip(tmp_path):
    r = make_responder(tmp_path, tag_len=16)
    rec = signed_command(tag_len=16)
    assert len(rec["auth_tag"]) == 16
    assert r.handle(rec)["accepted"] is True


def test_worker_signed_command_verifies_at_responder(tmp_path):
    """End-to-end: the worker's outbound signing is authenticated by the responder."""
    rec = {
        "command_id": "cmd-abcabcabcabc",
        "issued_at_utc": cr.utc_now_iso(),
        "target_node_id": "!db51af80",
        "command": "status",
        "ttl_sec": 600,
    }
    nonce_seq = iter([42])
    envelope = worker.ensure_command_signed(
        rec, key=KEY, sender="head", tag_len=16, alloc_nonce=lambda: next(nonce_seq)
    )
    # The worker retained the full tag as evidence and stamped the record.
    assert rec["auth_nonce"] == 42
    assert len(rec["auth_tag_full"]) == command_auth.FULL_TAG_LEN
    assert rec["auth_tag_len"] == 16

    # The worker signs the addressee from the queue record's target_node_id, so
    # the responder must answer to that mesh address.
    r = make_responder(tmp_path, tag_len=16, mesh_id="!db51af80")
    decision = r.handle(envelope)
    assert decision["accepted"] is True

    # A tampered command body fails authentication at the responder.
    forged = dict(envelope)
    forged["command"] = "reboot"
    assert r.handle(forged)["reason"] == "bad_hmac"


def test_worker_ack_record_matches_worker_ack_convention(tmp_path):
    r = make_responder(tmp_path)
    rec = signed_command()
    ack = r.worker_ack_record(rec, {"status": "ok"})
    # Verify with the worker's own ack_message canonicalization + key.
    import hashlib
    import hmac
    expected = hmac.new(KEY, worker.ack_message(ack), hashlib.sha256).hexdigest()
    assert ack["auth_tag"] == expected
