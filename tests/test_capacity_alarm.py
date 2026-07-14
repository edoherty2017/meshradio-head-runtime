from __future__ import annotations

import importlib.util
import json
from collections import namedtuple
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ca = _load("capacity_alarm", "capacity_alarm.py")

Usage = namedtuple("Usage", "total used free")
Vfs = namedtuple("Vfs", "f_files f_favail")


def fake_disk(used_percent, total=1000):
    used = int(total * used_percent / 100)
    return lambda _p: Usage(total=total, used=used, free=total - used)


def no_inodes(_p):
    raise OSError("no statvfs")


def fake_inodes(used_percent, total=1000):
    used = int(total * used_percent / 100)
    return lambda _p: Vfs(f_files=total, f_favail=total - used)


def evaluate(monkeypatch, tmp_path, *, bytes_pct, inode=None, **kw):
    monkeypatch.setattr(ca.shutil, "disk_usage", fake_disk(bytes_pct))
    monkeypatch.setattr(ca.os, "statvfs", fake_inodes(inode) if inode is not None else no_inodes)
    defaults = dict(warn_percent=80, critical_percent=90, min_free_bytes=0,
                    inode_warn_percent=80, inode_critical_percent=90)
    defaults.update(kw)
    return ca.evaluate_path(tmp_path, **defaults)


def test_ok_below_warn(monkeypatch, tmp_path):
    ev = evaluate(monkeypatch, tmp_path, bytes_pct=50)
    assert ev["severity"] == "OK"
    assert ev["event_type"] == "CAPACITY_OK"
    assert ev["used_percent"] == 50.0


def test_warn_band(monkeypatch, tmp_path):
    ev = evaluate(monkeypatch, tmp_path, bytes_pct=85)
    assert ev["severity"] == "WARN"
    assert ev["event_type"] == "CAPACITY_WARN"
    assert "bytes_warn" in ev["reasons"]


def test_critical_band(monkeypatch, tmp_path):
    ev = evaluate(monkeypatch, tmp_path, bytes_pct=95)
    assert ev["severity"] == "CRITICAL"
    assert "bytes_critical" in ev["reasons"]


def test_min_free_bytes_forces_critical(monkeypatch, tmp_path):
    # 50% used (would be OK) but only 500 bytes free, floor is 600.
    ev = evaluate(monkeypatch, tmp_path, bytes_pct=50, min_free_bytes=600)
    assert ev["severity"] == "CRITICAL"
    assert "min_free_bytes" in ev["reasons"]


def test_inode_exhaustion_critical(monkeypatch, tmp_path):
    ev = evaluate(monkeypatch, tmp_path, bytes_pct=10, inode=97)
    assert ev["severity"] == "CRITICAL"
    assert "inodes_critical" in ev["reasons"]
    assert ev["inode_used_percent"] == 97.0


def test_nonexistent_path_is_critical(monkeypatch, tmp_path):
    ev = ca.evaluate_path(tmp_path / "missing", warn_percent=80, critical_percent=90,
                          min_free_bytes=0, inode_warn_percent=80, inode_critical_percent=90)
    assert ev["severity"] == "CRITICAL"
    assert ev["event_type"] == "CAPACITY_UNAVAILABLE"


def test_run_exit_code_and_events_file(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(ca.shutil, "disk_usage", fake_disk(95))
    monkeypatch.setattr(ca.os, "statvfs", no_inodes)
    events_file = tmp_path / "events.jsonl"
    rc = ca.main(["--path", str(tmp_path), "--out", str(events_file)])
    assert rc == 2  # CRITICAL -> exit 2

    lines = [json.loads(l) for l in events_file.read_text().splitlines() if l.strip()]
    assert lines[-1]["event_type"] == "CAPACITY_SUMMARY"
    assert lines[-1]["worst_severity"] == "CRITICAL"
    # Same events also went to stdout.
    stdout_lines = [json.loads(l) for l in capsys.readouterr().out.splitlines() if l.strip()]
    assert any(e["event_type"] == "CAPACITY_CRITICAL" for e in stdout_lines)


def test_run_ok_exit_zero(monkeypatch, tmp_path):
    monkeypatch.setattr(ca.shutil, "disk_usage", fake_disk(40))
    monkeypatch.setattr(ca.os, "statvfs", no_inodes)
    assert ca.main(["--path", str(tmp_path)]) == 0
