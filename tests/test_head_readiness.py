from __future__ import annotations

import datetime as dt
import importlib.util
import json
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "head_readiness_report.py"
_spec = importlib.util.spec_from_file_location("head_readiness_report", MODULE_PATH)
assert _spec and _spec.loader
head_readiness = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(head_readiness)


def write_mode_evidence(root: Path, timestamp: str) -> None:
    (root / "connectivity_mode.state").write_text("MESH_ONLY\n", encoding="utf-8")
    event = {
        "timestamp_utc": timestamp,
        "node_id": head_readiness.NODE_ID,
        "event_type": "CONNECTIVITY_MODE_OBSERVED",
        "connectivity_mode": "MESH_ONLY",
    }
    (root / "connectivity_events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")


def test_recent_degraded_state_requires_recent_matching_event(tmp_path, monkeypatch):
    fixed_now = dt.datetime(2026, 7, 13, 12, 5, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(head_readiness, "now_utc", lambda: fixed_now)
    write_mode_evidence(tmp_path, "2026-07-13T12:00:30Z")

    result = head_readiness.check_connectivity(tmp_path)

    assert result["ok"] is True
    assert result["event_consistent"] is True


def test_stale_degraded_state_cannot_report_ready(tmp_path, monkeypatch):
    fixed_now = dt.datetime(2026, 7, 13, 12, 30, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(head_readiness, "now_utc", lambda: fixed_now)
    write_mode_evidence(tmp_path, "2026-07-13T12:00:00Z")

    result = head_readiness.check_connectivity(tmp_path)

    assert result["ok"] is False
    assert result["event_consistent"] is False
