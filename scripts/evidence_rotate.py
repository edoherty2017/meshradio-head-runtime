#!/usr/bin/env python3
"""Size/age-based rotation and archival of HEAD evidence files.

Each rotated file is moved into an archive directory under a UTC-stamped name,
its SHA-256 is recorded, and an entry is appended to a durable JSONL manifest so
the chain of custody for every archived evidence file is auditable.

Rotation triggers, per source, when EITHER:
  * the file is at least ``--max-bytes``; or
  * ``--max-age-sec`` has elapsed since the file was last rotated (tracked in a
    small state file; first sighting seeds the clock).

Writer assumption: this tool renames the live file aside and recreates an empty
one with the same mode. That is safe for this repo's evidence writers, which
open-append-fsync-close per record (the collector and fallback worker), so the
next record lands in the fresh file. It is not safe for a writer that holds a
long-lived file descriptor across many writes.
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob as globmod
import gzip
import hashlib
import json
import os
import shutil
import stat
import sys
import tempfile
from pathlib import Path
from typing import Any

READ_CHUNK = 1 << 20  # 1 MiB


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def _stamp(now: dt.datetime) -> str:
    return now.strftime("%Y%m%dT%H%M%S.%fZ")


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"rotated": {}}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"rotation state at {path} is not an object")
    raw.setdefault("rotated", {})
    return raw


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            os.fchmod(f.fileno(), 0o600)
            json.dump(state, f, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def resolve_sources(sources: list[str], source_globs: list[str]) -> list[Path]:
    resolved: list[Path] = []
    seen: set[str] = set()
    for s in sources:
        p = Path(s)
        key = str(p.resolve())
        if key not in seen:
            seen.add(key)
            resolved.append(p)
    for pattern in source_globs:
        for match in sorted(globmod.glob(pattern, recursive=True)):
            p = Path(match)
            if not p.is_file():
                continue
            key = str(p.resolve())
            if key not in seen:
                seen.add(key)
                resolved.append(p)
    return resolved


def rotation_reason(
    source: Path,
    *,
    size: int,
    max_bytes: int,
    max_age_sec: float,
    last_rotated: dt.datetime | None,
    now: dt.datetime,
) -> str | None:
    if size <= 0:
        return None
    if max_bytes > 0 and size >= max_bytes:
        return "size"
    if max_age_sec > 0 and last_rotated is not None:
        if (now - last_rotated).total_seconds() >= max_age_sec:
            return "age"
    return None


def archive_file(
    source: Path,
    archive_dir: Path,
    *,
    reason: str,
    gzip_output: bool,
    now: dt.datetime,
) -> dict[str, Any]:
    """Rename ``source`` into ``archive_dir`` and hash it. Returns a manifest row."""
    archive_dir.mkdir(parents=True, exist_ok=True)
    try:
        mode = stat.S_IMODE(os.stat(source).st_mode)
    except OSError:
        mode = 0o600

    dest_name = f"{source.name}.{_stamp(now)}"
    staged = archive_dir / (dest_name + ".partial")
    # Rename first so the file stops receiving new records, then process the
    # now-stable inode. Recreate the live path immediately for the next writer.
    os.replace(source, staged)
    _recreate_empty(source, mode)

    final_name = dest_name + (".gz" if gzip_output else "")
    final_path = archive_dir / final_name
    sha256 = hashlib.sha256()
    size_bytes = 0
    if gzip_output:
        with open(staged, "rb") as src, gzip.open(final_path, "wb") as dst:
            while True:
                chunk = src.read(READ_CHUNK)
                if not chunk:
                    break
                size_bytes += len(chunk)
                sha256.update(chunk)
                dst.write(chunk)
        os.unlink(staged)
    else:
        with open(staged, "rb") as src:
            while True:
                chunk = src.read(READ_CHUNK)
                if not chunk:
                    break
                size_bytes += len(chunk)
                sha256.update(chunk)
        os.replace(staged, final_path)
    try:
        os.chmod(final_path, 0o400)
    except OSError:
        pass

    return {
        "rotated_at_utc": now.isoformat(),
        "source_path": str(source),
        "archived_path": str(final_path),
        "reason": reason,
        "uncompressed_bytes": size_bytes,
        "sha256": sha256.hexdigest(),
        "compression": "gzip" if gzip_output else "none",
    }


def _recreate_empty(source: Path, mode: int) -> None:
    fd = os.open(source, os.O_CREAT | os.O_WRONLY, mode)
    try:
        os.fchmod(fd, mode)
    finally:
        os.close(fd)


def append_manifest(manifest_path: Path, row: dict[str, Any]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, separators=(",", ":"), sort_keys=True, allow_nan=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def prune_archive(archive_dir: Path, retention_sec: float, now: dt.datetime) -> list[str]:
    removed: list[str] = []
    if retention_sec <= 0 or not archive_dir.exists():
        return removed
    cutoff = now.timestamp() - retention_sec
    for entry in sorted(archive_dir.iterdir()):
        if entry.name == "MANIFEST.jsonl" or not entry.is_file():
            continue
        try:
            if entry.stat().st_mtime < cutoff:
                entry.unlink()
                removed.append(str(entry))
        except OSError:
            continue
    return removed


def run(args: argparse.Namespace) -> int:
    now = utc_now()
    archive_dir = Path(args.archive_dir)
    manifest_path = archive_dir / "MANIFEST.jsonl"
    state_path = Path(args.state_file) if args.state_file else archive_dir / ".rotate_state.json"
    state = load_state(state_path)

    events: list[dict[str, Any]] = []
    rotated_count = 0
    for source in resolve_sources(args.source or [], args.source_glob or []):
        if not source.exists() or not source.is_file():
            continue
        size = source.stat().st_size
        key = str(source.resolve())
        last_iso = state["rotated"].get(key)
        last_rotated = None
        if isinstance(last_iso, str):
            try:
                last_rotated = dt.datetime.fromisoformat(last_iso)
            except ValueError:
                last_rotated = None
        if last_rotated is None:
            # Seed the age clock on first sighting so age-based rotation has a
            # reference point; size-based rotation still fires immediately.
            state["rotated"].setdefault(key, now.isoformat())
            last_rotated = now

        reason = rotation_reason(
            source,
            size=size,
            max_bytes=args.max_bytes,
            max_age_sec=args.max_age_sec,
            last_rotated=last_rotated,
            now=now,
        )
        if reason is None:
            continue

        if args.dry_run:
            events.append({
                "timestamp_utc": utc_now_iso(),
                "event_type": "EVIDENCE_ROTATE_DRYRUN",
                "source_path": str(source),
                "reason": reason,
                "size_bytes": size,
            })
            continue

        row = archive_file(source, archive_dir, reason=reason, gzip_output=args.gzip, now=now)
        append_manifest(manifest_path, row)
        state["rotated"][key] = now.isoformat()
        rotated_count += 1
        events.append({
            "timestamp_utc": utc_now_iso(),
            "event_type": "EVIDENCE_ROTATED",
            **row,
        })

    removed = prune_archive(archive_dir, args.retention_sec, now) if not args.dry_run else []
    for path in removed:
        events.append({
            "timestamp_utc": utc_now_iso(),
            "event_type": "EVIDENCE_PRUNED",
            "archived_path": path,
        })

    if not args.dry_run:
        save_state(state_path, state)

    summary = {
        "timestamp_utc": utc_now_iso(),
        "event_type": "EVIDENCE_ROTATE_SUMMARY",
        "archive_dir": str(archive_dir),
        "rotated_count": rotated_count,
        "pruned_count": len(removed),
        "dry_run": bool(args.dry_run),
    }
    events.append(summary)
    for event in events:
        sys.stdout.write(json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Rotate and archive HEAD evidence files with SHA-256 manifests.")
    p.add_argument("--source", action="append", default=[], help="Evidence file to consider (repeatable)")
    p.add_argument("--source-glob", action="append", default=[], help="Glob of evidence files (repeatable, supports **)")
    p.add_argument("--archive-dir", required=True, help="Destination archive directory")
    p.add_argument("--max-bytes", type=int, default=int(os.environ.get("EVIDENCE_MAX_BYTES", str(64 * 1024 * 1024))), help="Rotate when a file reaches this size (0 disables)")
    p.add_argument("--max-age-sec", type=float, default=float(os.environ.get("EVIDENCE_MAX_AGE_SEC", str(24 * 3600))), help="Rotate when this many seconds have elapsed since last rotation (0 disables)")
    p.add_argument("--retention-sec", type=float, default=float(os.environ.get("EVIDENCE_RETENTION_SEC", "0")), help="Delete archived files older than this many seconds (0 keeps everything)")
    p.add_argument("--state-file", default=os.environ.get("EVIDENCE_ROTATE_STATE", ""), help="Rotation state file (defaults to <archive-dir>/.rotate_state.json)")
    p.add_argument("--gzip", action="store_true", help="Gzip archived files")
    p.add_argument("--dry-run", action="store_true", help="Report what would rotate without moving anything")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.max_bytes < 0 or args.max_age_sec < 0 or args.retention_sec < 0:
        build_parser().error("size/age/retention thresholds must be non-negative")
    if not args.source and not args.source_glob:
        build_parser().error("at least one --source or --source-glob is required")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
