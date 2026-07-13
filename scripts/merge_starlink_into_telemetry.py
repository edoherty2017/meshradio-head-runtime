#!/usr/bin/env python3
"""Merge Starlink 60-second window metrics into the MANET telemetry stream.

Uses pandas.merge_asof (backward, 65s tolerance) to attach the most recently
completed Starlink window to each MANET telemetry row. MANET packets with no
valid Starlink record within the preceding 65 seconds receive explicit null
values for all satellite_* fields.

Output: artifacts/starlink/derived/telemetry_enriched_starlink.jsonl
        (also optionally written back to the ingest path for pipeline ingestion)

Usage:
  python3 scripts/merge_starlink_into_telemetry.py \
      --manet-jsonl /home/doher/manet_ingest/meshradiohead/jsonl/telemetry_stream.jsonl \
      --starlink-derived /home/doher/manet_ingest/meshradiohead/starlink/derived/starlink_window_metrics.jsonl \
      --out artifacts/starlink/derived/telemetry_enriched_starlink.jsonl
"""
from __future__ import annotations

import argparse
import json
import math
import numbers
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("FATAL: pandas not installed", file=sys.stderr)
    sys.exit(1)

SATELLITE_COLS = [
    "satellite_link_status",
    "satellite_down_mbps",
    "satellite_up_mbps",
    "satellite_rtt_ms_p50",
    "satellite_rtt_ms_p95",
    "satellite_packet_loss_pct",
    "satellite_obstruction_pct",
    "satellite_outage_seconds",
]

MERGE_TOLERANCE = pd.Timedelta("65s")  # 60s window + 5s jitter


def load_jsonl(path: Path) -> pd.DataFrame:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
            except (json.JSONDecodeError, ValueError) as exc:
                raise ValueError(f"invalid JSON in {path} line {line_no}: {exc}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"non-object record in {path} line {line_no}")
            rows.append(record)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def coerce_utc(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Cast timestamp column to timezone-aware UTC datetime — required by merge_asof."""
    raw = df[col]
    invalid_suffix = raw.map(
        lambda value: not isinstance(value, str)
        or not (value.endswith("Z") or value.endswith("+00:00"))
    )
    parsed = pd.to_datetime(raw, utc=True, errors="coerce")
    if invalid_suffix.any() or parsed.isna().any():
        raise ValueError(f"{col} contains invalid/non-UTC timestamps")
    df[col] = parsed
    return df.sort_values(col).reset_index(drop=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manet-jsonl",       required=True)
    ap.add_argument("--starlink-derived",  required=True)
    ap.add_argument("--out",               required=True)
    ap.add_argument("--tolerance-s",       type=int, default=65,
                    help="Max seconds a Starlink window can lag a MANET packet (default 65)")
    ap.add_argument("--min-match-pct", type=float, default=95.0)
    args = ap.parse_args()
    if args.tolerance_s <= 0:
        print("ERROR: --tolerance-s must be greater than zero", file=sys.stderr)
        return 1
    if not 0 <= args.min_match_pct <= 100:
        print("ERROR: --min-match-pct must be between 0 and 100", file=sys.stderr)
        return 1

    manet_path    = Path(args.manet_jsonl)
    starlink_path = Path(args.starlink_derived)
    out_path      = Path(args.out)

    if not manet_path.exists():
        print(f"ERROR: manet JSONL not found: {manet_path}", file=sys.stderr)
        return 1
    if not starlink_path.exists():
        print(f"ERROR: starlink derived JSONL not found: {starlink_path}", file=sys.stderr)
        return 1

    print(f"[merge_starlink] loading MANET  ← {manet_path}")
    try:
        manet_df = load_jsonl(manet_path)
    except (UnicodeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    if manet_df.empty:
        print("ERROR: MANET JSONL is empty", file=sys.stderr)
        return 1

    print(f"[merge_starlink] loading Starlink ← {starlink_path}")
    try:
        starlink_df = load_jsonl(starlink_path)
    except (UnicodeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    if starlink_df.empty:
        print("ERROR: Starlink derived JSONL is empty", file=sys.stderr)
        return 1

    # --- Strict prerequisites for merge_asof ---

    # 1. UTC datetime coercion on both sides
    manet_ts_col    = "timestamp_utc"
    starlink_ts_col = "window_end_utc"

    if manet_ts_col not in manet_df.columns:
        print(f"ERROR: MANET JSONL missing '{manet_ts_col}' column", file=sys.stderr)
        return 1
    if starlink_ts_col not in starlink_df.columns:
        print(f"ERROR: Starlink JSONL missing '{starlink_ts_col}' column", file=sys.stderr)
        return 1
    for identity in ("trial_id", "node_id"):
        if identity not in manet_df.columns or identity not in starlink_df.columns:
            print(f"ERROR: both inputs must contain '{identity}'", file=sys.stderr)
            return 1

    try:
        manet_df    = coerce_utc(manet_df,    manet_ts_col)
        starlink_df = coerce_utc(starlink_df, starlink_ts_col)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    if manet_df.empty or starlink_df.empty:
        print("ERROR: no rows remain after strict UTC timestamp coercion", file=sys.stderr)
        return 1
    for identity in ("trial_id", "node_id"):
        if (
            manet_df[identity].isna().any()
            or starlink_df[identity].isna().any()
            or not manet_df[identity].map(lambda value: isinstance(value, str) and bool(value.strip())).all()
            or not starlink_df[identity].map(lambda value: isinstance(value, str) and bool(value.strip())).all()
        ):
            print(f"ERROR: invalid identity values in '{identity}'", file=sys.stderr)
            return 1
    if starlink_df.duplicated(subset=["trial_id", "node_id", starlink_ts_col]).any():
        print("ERROR: duplicate Starlink window key", file=sys.stderr)
        return 1

    print(f"[merge_starlink] MANET rows:    {len(manet_df)}")
    print(f"[merge_starlink] Starlink rows: {len(starlink_df)}")

    # 2. Require the actual aggregator contract; missing fields must not turn
    # into a superficially successful all-null enrichment.
    quality_columns = ["poll_count", "_measurement_success_count", "_heartbeat_count"]
    missing_starlink = set(SATELLITE_COLS + quality_columns) - set(starlink_df.columns)
    if missing_starlink:
        print(f"ERROR: Starlink derived input missing columns: {sorted(missing_starlink)}", file=sys.stderr)
        return 1
    invalid_quality = (
        pd.to_numeric(starlink_df["poll_count"], errors="coerce").isna()
        | pd.to_numeric(starlink_df["_measurement_success_count"], errors="coerce").isna()
        | pd.to_numeric(starlink_df["_heartbeat_count"], errors="coerce").isna()
        | (pd.to_numeric(starlink_df["poll_count"], errors="coerce") <= 0)
        | (pd.to_numeric(starlink_df["_measurement_success_count"], errors="coerce") <= 0)
        | ~starlink_df["satellite_link_status"].isin({"connected", "degraded", "disconnected"})
    )
    if invalid_quality.any():
        print(f"ERROR: invalid/unmeasured Starlink windows: {int(invalid_quality.sum())}", file=sys.stderr)
        return 1
    for _, window in starlink_df.iterrows():
        poll_count = window["poll_count"]
        success_count = window["_measurement_success_count"]
        heartbeat_count = window["_heartbeat_count"]
        if any(
            isinstance(value, bool)
            or not isinstance(value, numbers.Integral)
            or value < 0
            for value in (poll_count, success_count, heartbeat_count)
        ) or success_count + heartbeat_count != poll_count:
            print("ERROR: inconsistent Starlink window quality counters", file=sys.stderr)
            return 1
    numeric_ranges = {
        "satellite_down_mbps": (0, 10000),
        "satellite_up_mbps": (0, 10000),
        "satellite_rtt_ms_p50": (0, 100000),
        "satellite_rtt_ms_p95": (0, 100000),
        "satellite_packet_loss_pct": (0, 100),
        "satellite_obstruction_pct": (0, 100),
        "satellite_outage_seconds": (0, 86400),
    }
    for field, (minimum, maximum) in numeric_ranges.items():
        for value in starlink_df[field].dropna():
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or not minimum <= float(value) <= maximum
            ):
                print(f"ERROR: invalid {field} in Starlink derived input", file=sys.stderr)
                return 1

    # Existing satellite columns may be stale output from an earlier merge.
    # Drop them so the current reviewed window data owns the canonical names.
    stale_satellite_columns = [
        column
        for canonical in SATELLITE_COLS
        for column in (canonical, f"{canonical}_starlink")
        if column in manet_df.columns
    ]
    manet_df = manet_df.drop(columns=stale_satellite_columns)

    # Keep only the columns we need from starlink side (avoid column collision)
    keep_starlink = [starlink_ts_col, "trial_id", "node_id"] + SATELLITE_COLS + quality_columns
    starlink_slim = starlink_df[keep_starlink].copy()

    # 3. merge_asof: backward direction, 65s tolerance
    tolerance = pd.Timedelta(f"{args.tolerance_s}s")
    merged = pd.merge_asof(
        manet_df,
        starlink_slim,
        left_on=manet_ts_col,
        right_on=starlink_ts_col,
        direction="backward",
        tolerance=tolerance,
        by=["trial_id", "node_id"],
        suffixes=("", "_starlink"),
    )

    matched = int(merged[starlink_ts_col].notna().sum())
    total = len(merged)
    match_pct = 100.0 * matched / total if total else 0.0
    # Drop the starlink timestamp helper column if present
    if starlink_ts_col in merged.columns and starlink_ts_col != manet_ts_col:
        merged = merged.drop(columns=[starlink_ts_col])

    # Matched and unmatched stats
    print(f"[merge_starlink] matched {matched}/{total} MANET rows to a Starlink window ({matched/total:.1%})")
    if matched == 0 or match_pct < args.min_match_pct:
        print(
            f"ERROR: Starlink match coverage {match_pct:.2f}% is below {args.min_match_pct:.2f}%",
            file=sys.stderr,
        )
        return 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    _write_jsonl(merged, out_path)
    print(f"[merge_starlink] enriched JSONL → {out_path}")

    return 0


def _write_jsonl(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for rec in df.to_dict(orient="records"):
                # Convert Timestamps back to ISO strings; null out NaN
                cleaned = {}
                for k, v in rec.items():
                    if pd.isna(v) if not isinstance(v, (list, dict)) else False:
                        cleaned[k] = None
                    elif hasattr(v, "isoformat"):
                        cleaned[k] = v.isoformat()
                    else:
                        cleaned[k] = v
                f.write(json.dumps(cleaned, allow_nan=False, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


if __name__ == "__main__":
    sys.exit(main())
