from __future__ import annotations

import hashlib
import hmac
import importlib.util
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
QUEUE_SCRIPT = ROOT / "scripts" / "fallback_command_queue.py"
WORKER_SCRIPT = ROOT / "scripts" / "meshtastic_fallback_worker.py"


def load_worker():
    spec = importlib.util.spec_from_file_location("fallback_worker", WORKER_SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


worker = load_worker()


def enqueue(queue: Path, command: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable, str(QUEUE_SCRIPT), "--queue-path", str(queue),
            "enqueue", "--target", "!1234abcd", "--command", command,
        ],
        capture_output=True,
        text=True,
    )


def test_concurrent_enqueues_do_not_lose_commands(tmp_path):
    queue = tmp_path / "queue.jsonl"
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda i: enqueue(queue, f"command-{i}"), range(20)))

    assert all(result.returncode == 0 for result in results)
    rows = [json.loads(line) for line in queue.read_text().splitlines()]
    assert len(rows) == 20
    assert {row["command"] for row in rows} == {f"command-{i}" for i in range(20)}


def test_dry_run_does_not_mark_command_sent(tmp_path):
    queue = tmp_path / "queue.jsonl"
    out_dir = tmp_path / "out"
    mode_path = tmp_path / "mode.state"
    mode_path.write_text("MESH_ONLY\n")
    assert enqueue(queue, "status").returncode == 0

    result = subprocess.run(
        [
            sys.executable, str(WORKER_SCRIPT), "--queue-path", str(queue),
            "--out-dir", str(out_dir), "--dry-run", "--once",
            "--connectivity-mode-file", str(mode_path),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    row = json.loads(queue.read_text().splitlines()[0])
    assert row["status"] == "queued"
    assert "last_dry_run_at_utc" in row


def test_missing_connectivity_state_fails_closed(tmp_path):
    queue = tmp_path / "queue.jsonl"
    out_dir = tmp_path / "out"
    assert enqueue(queue, "status").returncode == 0

    result = subprocess.run(
        [
            sys.executable, str(WORKER_SCRIPT), "--queue-path", str(queue),
            "--out-dir", str(out_dir), "--dry-run", "--once",
            "--connectivity-mode-file", str(tmp_path / "missing.state"),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    row = json.loads(queue.read_text().splitlines()[0])
    assert row["status"] == "queued"
    events = [json.loads(line) for line in (out_dir / "fallback_worker_events.jsonl").read_text().splitlines()]
    assert events[-1]["event_type"] == "SKIP_MODE_MISMATCH"


def test_ack_file_requires_valid_hmac(tmp_path):
    path = tmp_path / "acks.jsonl"
    key = b"test-secret"
    record = {"command_id": "cmd-abcdef123456", "payload": "ok"}
    record["auth_tag"] = hmac.new(key, worker.ack_message(record), hashlib.sha256).hexdigest()
    invalid = {"command_id": "cmd-deadbeef1234", "payload": "forged", "auth_tag": "00"}
    path.write_text(json.dumps(record) + "\n" + json.dumps(invalid) + "\n")

    ids, _payloads = worker.parse_manual_ack_file(path, key)

    assert ids == {"cmd-abcdef123456"}


def test_authenticated_ack_is_ingested_after_mode_recovers(tmp_path):
    queue = tmp_path / "queue.jsonl"
    out_dir = tmp_path / "out"
    assert enqueue(queue, "status").returncode == 0
    command_id = json.loads(queue.read_text().splitlines()[0])["command_id"]

    key = b"test-secret"
    key_path = tmp_path / "ack.key"
    key_path.write_bytes(key)
    ack = {"command_id": command_id, "payload": "received"}
    ack["auth_tag"] = hmac.new(key, worker.ack_message(ack), hashlib.sha256).hexdigest()
    ack_path = tmp_path / "acks.jsonl"
    ack_path.write_text(json.dumps(ack) + "\n")
    mode_path = tmp_path / "mode.state"
    mode_path.write_text("IP_FULL\n")

    result = subprocess.run(
        [
            sys.executable, str(WORKER_SCRIPT), "--queue-path", str(queue),
            "--out-dir", str(out_dir), "--dry-run", "--once",
            "--connectivity-mode-file", str(mode_path),
            "--ack-file", str(ack_path), "--ack-hmac-key-file", str(key_path),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(queue.read_text().splitlines()[0])["status"] == "acked"
    events = [json.loads(line) for line in (out_dir / "fallback_worker_events.jsonl").read_text().splitlines()]
    assert events[-1]["event_type"] == "SKIP_MODE_MISMATCH"
    assert events[-1]["acked_ingested"] == 1
