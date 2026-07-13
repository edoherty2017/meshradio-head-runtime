from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "merge_starlink_into_telemetry.py"


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def run_merge(manet: Path, starlink: Path, output: Path):
    return subprocess.run(
        [
            sys.executable, str(SCRIPT), "--manet-jsonl", str(manet),
            "--starlink-derived", str(starlink), "--out", str(output),
        ],
        capture_output=True,
        text=True,
    )


def derived_window(**overrides):
    row = {
        "window_start_utc": "2026-07-13T11:59:30Z",
        "window_end_utc": "2026-07-13T12:00:15Z",
        "window_seconds": 60,
        "observed_poll_span_seconds": 45.0,
        "trial_id": "trial-live",
        "node_id": "meshradiohead2",
        "satellite_link_status": "connected",
        "satellite_down_mbps": 100.0,
        "satellite_up_mbps": 10.0,
        "satellite_rtt_ms_p50": 30.0,
        "satellite_rtt_ms_p95": 50.0,
        "satellite_packet_loss_pct": 0.0,
        "satellite_obstruction_pct": 0.0,
        "satellite_outage_seconds": 0,
        "poll_count": 4,
        "_expected_poll_count": 4,
        "_measurement_success_count": 4,
        "_unusable_measurement_count": 0,
        "_heartbeat_count": 0,
        "_clock_ok_count": 4,
        "_clock_sample_count": 4,
    }
    row.update(overrides)
    return row


def test_merge_matches_on_trial_and_node_identity(tmp_path):
    manet = tmp_path / "manet.jsonl"
    starlink = tmp_path / "starlink.jsonl"
    output = tmp_path / "out.jsonl"
    write_jsonl(manet, [{
        "timestamp_utc": "2026-07-13T12:00:30Z",
        "trial_id": "trial-live",
        "node_id": "meshradiohead2",
        "head_id": "meshradiohead2",
        "line": "packet",
    }])
    write_jsonl(starlink, [derived_window()])

    result = run_merge(manet, starlink, output)

    assert result.returncode == 0
    row = json.loads(output.read_text())
    assert row["satellite_link_status"] == "connected"


def test_merge_fails_when_only_timestamp_matches_wrong_node(tmp_path):
    manet = tmp_path / "manet.jsonl"
    starlink = tmp_path / "starlink.jsonl"
    output = tmp_path / "out.jsonl"
    write_jsonl(manet, [{
        "timestamp_utc": "2026-07-13T12:00:30Z", "trial_id": "trial-live",
        "node_id": "meshnode1", "head_id": "meshradiohead2", "line": "packet",
    }])
    write_jsonl(starlink, [derived_window()])

    result = run_merge(manet, starlink, output)

    assert result.returncode == 1
    assert not output.exists()


def test_merge_rejects_corrupt_input(tmp_path):
    manet = tmp_path / "manet.jsonl"
    starlink = tmp_path / "starlink.jsonl"
    output = tmp_path / "out.jsonl"
    manet.write_text("not-json\n")
    write_jsonl(starlink, [])

    result = run_merge(manet, starlink, output)

    assert result.returncode == 1
    assert not output.exists()


def test_merge_rejects_aligned_but_clock_bad_window(tmp_path):
    manet = tmp_path / "manet.jsonl"
    starlink = tmp_path / "starlink.jsonl"
    output = tmp_path / "out.jsonl"
    write_jsonl(manet, [{
        "timestamp_utc": "2026-07-13T12:00:30Z",
        "trial_id": "trial-live",
        "node_id": "meshradiohead2",
        "head_id": "meshradiohead2",
        "line": "packet",
    }])
    write_jsonl(starlink, [derived_window(_clock_ok_count=3)])

    result = run_merge(manet, starlink, output)

    assert result.returncode == 1
    assert "low-quality Starlink window" in result.stderr
    assert not output.exists()
