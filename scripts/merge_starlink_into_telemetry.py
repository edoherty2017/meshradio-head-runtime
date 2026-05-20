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
import sys
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
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def coerce_utc(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Cast timestamp column to timezone-aware UTC datetime — required by merge_asof."""
    df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    df = df.dropna(subset=[col])
    return df.sort_values(col).reset_index(drop=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manet-jsonl",       required=True)
    ap.add_argument("--starlink-derived",  required=True)
    ap.add_argument("--out",               required=True)
    ap.add_argument("--tolerance-s",       type=int, default=65,
                    help="Max seconds a Starlink window can lag a MANET packet (default 65)")
    args = ap.parse_args()

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
    manet_df = load_jsonl(manet_path)
    if manet_df.empty:
        print("ERROR: MANET JSONL is empty", file=sys.stderr)
        return 1

    print(f"[merge_starlink] loading Starlink ← {starlink_path}")
    starlink_df = load_jsonl(starlink_path)
    if starlink_df.empty:
        print("WARN: Starlink derived JSONL is empty — all satellite fields will be null")
        for col in SATELLITE_COLS:
            manet_df[col] = None
        out_path.parent.mkdir(parents=True, exist_ok=True)
        _write_jsonl(manet_df, out_path)
        return 0

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

    manet_df    = coerce_utc(manet_df,    manet_ts_col)
    starlink_df = coerce_utc(starlink_df, starlink_ts_col)

    print(f"[merge_starlink] MANET rows:    {len(manet_df)}")
    print(f"[merge_starlink] Starlink rows: {len(starlink_df)}")

    # 2. Ensure satellite_* columns exist in starlink_df (fill missing with None)
    for col in SATELLITE_COLS:
        if col not in starlink_df.columns:
            starlink_df[col] = None

    # Keep only the columns we need from starlink side (avoid column collision)
    keep_starlink = [starlink_ts_col] + SATELLITE_COLS
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
        suffixes=("", "_starlink"),
    )

    # Drop the starlink timestamp helper column if present
    if starlink_ts_col in merged.columns and starlink_ts_col != manet_ts_col:
        merged = merged.drop(columns=[starlink_ts_col])

    # Matched and unmatched stats
    matched = merged[SATELLITE_COLS[0]].notna().sum()
    total   = len(merged)
    print(f"[merge_starlink] matched {matched}/{total} MANET rows to a Starlink window ({matched/total:.1%})")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    _write_jsonl(merged, out_path)
    print(f"[merge_starlink] enriched JSONL → {out_path}")

    return 0


def _write_jsonl(df: pd.DataFrame, path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
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
            f.write(json.dumps(cleaned) + "\n")


if __name__ == "__main__":
    sys.exit(main())
