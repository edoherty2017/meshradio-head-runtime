#!/usr/bin/env python3
"""Chrony clock integrity check for the Starlink collection pipeline.

Runs `chronyc -c tracking`, parses the 14-field CSV, computes the
Clock Error Bound (epsilon), and appends one QC row to
artifacts/starlink/qc/starlink_collection_health.csv.

ClockErrorBound = |LastOffset| + RootDispersion + (0.5 × RootDelay)

If ClockErrorBound > MAX_CLOCK_ERROR_S (default 5.0 s), the row is flagged
as temporally unreliable. Downstream pipeline excludes flagged rows from
causal claims.

Typically called from starlink_raw_poller.py once per poll cycle, or run
standalone for diagnostics.

chronyc -c tracking CSV fields (0-indexed):
  0  Reference ID (hex)
  1  Reference IP/name
  2  Stratum
  3  Ref time (NTP epoch float)
  4  System time offset (s) — positive = system fast
  5  Last offset (s)
  6  RMS offset (s)
  7  Frequency (ppm)
  8  Residual freq (ppm)
  9  Skew (ppm)
  10 Root delay (s)
  11 Root dispersion (s)
  12 Update interval (s)
  13 Leap status
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import os
import subprocess
import sys
from pathlib import Path

OUT_DIR           = os.environ.get("OUT_DIR", "/home/pump/telemetry_head")
MAX_CLOCK_ERROR_S = float(os.environ.get("MAX_CLOCK_ERROR_S", "5.0"))

QC_CSV = Path(OUT_DIR) / "starlink" / "qc" / "starlink_collection_health.csv"

QC_HEADERS = [
    "sampled_at_utc",
    "reference_id",
    "reference_ip",
    "stratum",
    "system_time_offset_s",
    "last_offset_s",
    "root_delay_s",
    "root_dispersion_s",
    "clock_error_bound_s",
    "clock_ok",
    "leap_status",
]


def sample_chrony() -> dict:
    """Run chronyc -c tracking and parse the CSV output."""
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    try:
        r = subprocess.run(
            ["chronyc", "-c", "tracking"],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode != 0:
            return {"sampled_at_utc": now, "error": f"chronyc rc={r.returncode}: {r.stderr.strip()}"}

        fields = next(csv.reader(io.StringIO(r.stdout.strip())))
        if len(fields) < 12:
            return {"sampled_at_utc": now, "error": f"unexpected chronyc output: {r.stdout.strip()[:100]}"}

        ref_id          = fields[0].strip()
        ref_ip          = fields[1].strip()
        stratum         = int(fields[2])
        sys_offset      = float(fields[4])
        last_offset     = float(fields[5])
        root_delay      = float(fields[10])
        root_dispersion = float(fields[11])
        leap_status     = fields[13].strip() if len(fields) > 13 else ""

        clock_error_bound = abs(last_offset) + root_dispersion + 0.5 * root_delay
        clock_ok = clock_error_bound <= MAX_CLOCK_ERROR_S

        return {
            "sampled_at_utc":        now,
            "reference_id":          ref_id,
            "reference_ip":          ref_ip,
            "stratum":               stratum,
            "system_time_offset_s":  round(sys_offset,      6),
            "last_offset_s":         round(last_offset,     6),
            "root_delay_s":          round(root_delay,      6),
            "root_dispersion_s":     round(root_dispersion, 6),
            "clock_error_bound_s":   round(clock_error_bound, 6),
            "clock_ok":              clock_ok,
            "leap_status":           leap_status,
        }

    except FileNotFoundError:
        return {"sampled_at_utc": now, "error": "chronyc not found — install chrony"}
    except Exception as exc:
        return {"sampled_at_utc": now, "error": f"{type(exc).__name__}: {exc}"}


def append_qc_row(row: dict) -> None:
    QC_CSV.parent.mkdir(parents=True, exist_ok=True)
    write_header = not QC_CSV.exists()
    with QC_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=QC_HEADERS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def main() -> int:
    row = sample_chrony()
    print(json.dumps(row, indent=2))
    append_qc_row(row)

    ok = row.get("clock_ok")
    if ok is False:
        ceb = row.get("clock_error_bound_s", "?")
        print(f"WARN: ClockErrorBound={ceb}s exceeds {MAX_CLOCK_ERROR_S}s — timestamps unreliable",
              file=sys.stderr)
        return 1
    if "error" in row:
        print(f"ERROR: {row['error']}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
