from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


aggregator = load_module("starlink_window_aggregator", "scripts/starlink_window_aggregator.py")
gate = load_module("starlink_phase1_gate", "scripts/starlink_phase1_gate.py")


def test_majority_status_has_conservative_deterministic_tie_break():
    assert aggregator.majority_status(["connected", "disconnected"]) == "disconnected"
    assert aggregator.majority_status(["connected", "degraded"]) == "degraded"


def test_packet_loss_uses_fractional_drop_rates():
    rows = [
        {
            "poll_timestamp_utc": "2026-07-13T12:00:00+00:00",
            "trial_id": "trial-live",
            "node_id": "meshradiohead2",
            "satellite_link_status": "connected",
            "_measurement_status": "ok",
            "_heartbeat": False,
            "_clock_ok": True,
            "_poll_interval_s": 15,
            "_raw_latencies": [20, 30, 40, 50],
            "_raw_drops": [0.0, 0.25, 0.5, 1.0],
        }
    ]

    result = aggregator.aggregate_window(rows)

    assert result["satellite_packet_loss_pct"] == 43.75
    assert result["_measurement_success_count"] == 1


def test_aggregator_refuses_to_relabel_wrong_identity():
    rows = [
        {
            "poll_timestamp_utc": "2026-07-13T12:00:00+00:00",
            "trial_id": "different-trial",
            "node_id": "meshradiohead2",
            "satellite_link_status": "connected",
            "_measurement_status": "ok",
            "_heartbeat": False,
            "_clock_ok": True,
            "_poll_interval_s": 15,
            "_raw_latencies": [],
            "_raw_drops": [],
        }
    ]

    import pytest

    with pytest.raises(ValueError, match="identity mismatch"):
        aggregator.aggregate_window(rows)


def test_unknown_state_does_not_contaminate_derived_metrics():
    rows = [{
        "poll_timestamp_utc": "2026-07-13T12:00:00Z",
        "trial_id": "trial-live",
        "node_id": "meshradiohead2",
        "satellite_link_status": "unknown",
        "satellite_down_mbps": 999.0,
        "_measurement_status": "ok",
        "_heartbeat": False,
        "_clock_ok": True,
        "_poll_interval_s": 15,
        "_raw_latencies": [10.0],
        "_raw_drops": [0.0],
    }]

    result = aggregator.aggregate_window(rows)

    assert result["satellite_link_status"] == "unknown"
    assert result["satellite_down_mbps"] is None
    assert result["satellite_rtt_ms_p50"] is None
    assert result["_measurement_success_count"] == 0
    assert result["_unusable_measurement_count"] == 1


def test_tail_does_not_consume_partial_final_record(tmp_path):
    path = tmp_path / "raw.jsonl"
    first = '{"poll_timestamp_utc":"2026-07-13T12:00:00+00:00"}\n'
    partial = '{"poll_timestamp_utc":"2026-07-13T12:00'
    path.write_bytes((first + partial).encode())

    lines, position = aggregator.tail_new_lines(path, 0)

    assert lines == [first.strip()]
    assert position == len(first.encode())


def test_phase_gate_rejects_aligned_error_heartbeats():
    rows = [
        {
            "poll_timestamp_utc": f"2026-07-13T12:{minute:02d}:00+00:00",
            "_heartbeat": True,
            "_measurement_status": "error",
            "satellite_link_status": "unknown",
        }
        for minute in range(60)
    ]

    result = gate.check_measurement_success(rows)

    assert result["ok"] is False
    assert result["success_pct"] == 0.0


def test_phase_gate_does_not_count_unknown_state_as_successful_measurement():
    rows = [
        {
            "_heartbeat": False,
            "_measurement_status": "ok",
            "satellite_link_status": "unknown",
        }
        for _ in range(20)
    ]

    result = gate.check_measurement_success(rows)

    assert result["ok"] is False
    assert result["successful_rows"] == 0


def test_latest_contiguous_rows_ignores_old_run():
    rows = [
        {"poll_timestamp_utc": "2026-07-13T10:00:00+00:00"},
        {"poll_timestamp_utc": "2026-07-13T10:00:15+00:00"},
        {"poll_timestamp_utc": "2026-07-13T12:00:00+00:00"},
        {"poll_timestamp_utc": "2026-07-13T12:00:15+00:00"},
    ]

    selected = gate.latest_contiguous_rows(rows)

    assert [row["poll_timestamp_utc"] for row in selected] == [
        "2026-07-13T12:00:00+00:00",
        "2026-07-13T12:00:15+00:00",
    ]


def test_phase_gate_rejects_missing_clock_integrity():
    missing = [{"_measurement_status": "ok"} for _ in range(20)]
    valid = [{"_measurement_status": "ok", "_clock_ok": True} for _ in range(20)]

    assert gate.check_clock_integrity(missing)["ok"] is False
    assert gate.check_clock_integrity(valid)["ok"] is True
