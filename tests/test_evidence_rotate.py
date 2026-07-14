from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


er = _load("evidence_rotate", "evidence_rotate.py")


def read_manifest(archive: Path):
    manifest = archive / "MANIFEST.jsonl"
    if not manifest.exists():
        return []
    return [json.loads(line) for line in manifest.read_text().splitlines() if line.strip()]


def test_size_rotation_archives_and_hashes(tmp_path):
    src = tmp_path / "telemetry_stream.jsonl"
    content = b"x" * 2048
    src.write_bytes(content)
    archive = tmp_path / "archive"

    rc = er.main(["--source", str(src), "--archive-dir", str(archive), "--max-bytes", "1024", "--max-age-sec", "0"])
    assert rc == 0

    rows = read_manifest(archive)
    assert len(rows) == 1
    row = rows[0]
    assert row["reason"] == "size"
    assert row["sha256"] == hashlib.sha256(content).hexdigest()
    assert row["uncompressed_bytes"] == len(content)
    # Source recreated empty for the next writer; archived copy holds the data.
    assert src.exists() and src.stat().st_size == 0
    assert Path(row["archived_path"]).read_bytes() == content


def test_no_rotation_when_below_thresholds(tmp_path):
    src = tmp_path / "small.jsonl"
    src.write_bytes(b"tiny")
    archive = tmp_path / "archive"
    er.main(["--source", str(src), "--archive-dir", str(archive), "--max-bytes", "1024", "--max-age-sec", "0"])
    assert read_manifest(archive) == []
    assert src.read_bytes() == b"tiny"


def test_age_rotation_uses_state_clock(tmp_path):
    src = tmp_path / "events.jsonl"
    src.write_bytes(b"one line\n")
    archive = tmp_path / "archive"
    state = tmp_path / "state.json"
    # Seed a last-rotation far in the past so the age trigger fires despite a
    # tiny file well under max-bytes.
    old = (er.utc_now() - er.dt.timedelta(hours=48)).isoformat()
    state.write_text(json.dumps({"rotated": {str(src.resolve()): old}}))

    er.main([
        "--source", str(src), "--archive-dir", str(archive),
        "--state-file", str(state), "--max-bytes", "0", "--max-age-sec", "3600",
    ])
    rows = read_manifest(archive)
    assert len(rows) == 1
    assert rows[0]["reason"] == "age"
    # State clock advanced so it will not immediately rotate again.
    new_state = json.loads(state.read_text())
    assert new_state["rotated"][str(src.resolve())] != old


def test_gzip_hash_is_over_uncompressed_bytes(tmp_path):
    src = tmp_path / "raw.log"
    content = b"payload-" * 500
    src.write_bytes(content)
    archive = tmp_path / "archive"
    er.main(["--source", str(src), "--archive-dir", str(archive), "--max-bytes", "1", "--gzip"])
    row = read_manifest(archive)[0]
    assert row["compression"] == "gzip"
    assert row["sha256"] == hashlib.sha256(content).hexdigest()
    assert gzip.open(row["archived_path"], "rb").read() == content


def test_dry_run_changes_nothing(tmp_path):
    src = tmp_path / "big.jsonl"
    src.write_bytes(b"y" * 4096)
    archive = tmp_path / "archive"
    er.main(["--source", str(src), "--archive-dir", str(archive), "--max-bytes", "1024", "--dry-run"])
    assert read_manifest(archive) == []
    assert src.stat().st_size == 4096


def test_glob_sources_and_multiple_files(tmp_path):
    d = tmp_path / "jsonl"
    d.mkdir()
    (d / "a.jsonl").write_bytes(b"a" * 2048)
    (d / "b.jsonl").write_bytes(b"b" * 2048)
    (d / "c.jsonl").write_bytes(b"c")  # under threshold
    archive = tmp_path / "archive"
    er.main(["--source-glob", str(d / "*.jsonl"), "--archive-dir", str(archive), "--max-bytes", "1024"])
    rows = read_manifest(archive)
    assert {Path(r["source_path"]).name for r in rows} == {"a.jsonl", "b.jsonl"}


def test_retention_prunes_old_archives(tmp_path):
    archive = tmp_path / "archive"
    archive.mkdir()
    old_file = archive / "old.jsonl.20200101T000000.000000Z"
    old_file.write_bytes(b"old")
    import os
    import time
    stale = time.time() - 10 * 24 * 3600
    os.utime(old_file, (stale, stale))

    src = tmp_path / "live.jsonl"
    src.write_bytes(b"z" * 2048)
    er.main([
        "--source", str(src), "--archive-dir", str(archive),
        "--max-bytes", "1024", "--retention-sec", str(3 * 24 * 3600),
    ])
    assert not old_file.exists()  # pruned
    # New archive from this run is retained.
    assert any(p.name != "MANIFEST.jsonl" and p.is_file() for p in archive.iterdir())
