#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import os
import re
import time
from typing import Optional, Tuple

import serial

PORT = os.environ.get("LORA_PORT", "/dev/lora_radio")
BAUD = int(os.environ.get("LORA_BAUD", "115200"))
TRIAL_ID = os.environ.get("TRIAL_ID", "trial-unknown")
NODE_ID = os.environ.get("NODE_ID", "meshradiohead")
HEAD_ID = os.environ.get("HEAD_ID", "meshnodehead")
OUT_DIR = os.environ.get("OUT_DIR", "/home/pump/telemetry_head")

raw_dir = os.path.join(OUT_DIR, "raw")
json_dir = os.path.join(OUT_DIR, "jsonl")
os.makedirs(raw_dir, exist_ok=True)
os.makedirs(json_dir, exist_ok=True)

raw_path = os.path.join(raw_dir, "serial_raw.log")
json_path = os.path.join(json_dir, "telemetry_stream.jsonl")
err_path = os.path.join(OUT_DIR, "collector_errors.log")

ansi_re = re.compile(r"\x1b\[[0-9;]*m")

patterns = {
    "battery_mv": re.compile(r"batMv\s*[=:]\s*(\d+)", re.I),
    "battery_pct": re.compile(r"batPct\s*[=:]\s*(\d+)", re.I),
    "usb_power": re.compile(r"(?:USB\s*power|usbPower)\s*[=:]\s*(\d+)", re.I),
    "is_charging": re.compile(r"isCharging\s*[=:]\s*(\d+)", re.I),
    "rssi_dbm": re.compile(r"RSSI\s*[:=]\s*(-?\d+)", re.I),
    "rsrp_dbm": re.compile(r"RSRP\s*[:=]\s*(-?\d+(?:\.\d+)?)", re.I),
    "snr_db": re.compile(r"SNR\s*[:=]\s*(-?\d+(?:\.\d+)?)", re.I),
    "weather_tag": re.compile(r"weather(?:_tag)?\s*[:=]\s*([a-zA-Z0-9_\-]+)", re.I),
    "satellite_link_status": re.compile(r"sat(?:ellite)?(?:_link)?(?:_status)?\s*[:=]\s*([a-zA-Z0-9_\-]+)", re.I),
    # decimal format e.g. lat=44.123 lon=-71.456
    "lat_lon_decimal": re.compile(
        r"lat\s*[:=]\s*(-?\d+\.\d+).{0,60}?lon\s*[:=]\s*(-?\d+\.\d+)", re.I
    ),
    # scaled-int format e.g. lat=426508288 lon=-711720960 (1e7 scaling)
    "lat_lon_scaled": re.compile(r"lat\s*[:=]\s*(-?\d{6,12}).{0,60}?lon\s*[:=]\s*(-?\d{6,12})", re.I),
    "elev_m": re.compile(r"(?:elev|alt|msl)\s*[:=]\s*(-?\d+(?:\.\d+)?)", re.I),
}


def ts() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def decode_line(raw_line: str) -> str:
    return ansi_re.sub("", raw_line).strip()


def nmea_checksum_status(line: str) -> Tuple[int, int, int]:
    """
    Returns (checksum_ok, checksum_bad, malformed_frame).

    Policy:
    - Non-NMEA lines => (0,0,0)
    - NMEA with valid checksum => (1,0,0)
    - NMEA with invalid checksum => (0,1,0)
    - NMEA-like malformed frame => (0,0,1)
    """
    if not line.startswith("$"):
        return 0, 0, 0

    star = line.rfind("*")
    if star == -1 or star + 2 >= len(line):
        return 0, 0, 1

    payload = line[1:star]
    chk_txt = line[star + 1 : star + 3]
    if len(chk_txt) != 2:
        return 0, 0, 1

    try:
        expected = int(chk_txt, 16)
    except ValueError:
        return 0, 0, 1

    calc = 0
    for ch in payload:
        calc ^= ord(ch)

    if calc == expected:
        return 1, 0, 0
    return 0, 1, 0


def parse_lat_lon(line: str) -> Tuple[Optional[float], Optional[float]]:
    m = patterns["lat_lon_decimal"].search(line)
    if m:
        try:
            return float(m.group(1)), float(m.group(2))
        except Exception:
            return None, None

    m = patterns["lat_lon_scaled"].search(line)
    if m:
        try:
            lat = int(m.group(1)) / 1e7
            lon = int(m.group(2)) / 1e7
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return lat, lon
        except Exception:
            pass

    return None, None


def maybe_int(pattern_name: str, line: str) -> Optional[int]:
    m = patterns[pattern_name].search(line)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def maybe_float(pattern_name: str, line: str) -> Optional[float]:
    m = patterns[pattern_name].search(line)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def maybe_str(pattern_name: str, line: str) -> Optional[str]:
    m = patterns[pattern_name].search(line)
    if not m:
        return None
    return m.group(1).strip().lower()


while True:
    try:
        ser = serial.Serial(PORT, BAUD, timeout=0.5)
        ser.dtr = False
        ser.rts = False

        with open(raw_path, "ab", buffering=0) as rawf, open(json_path, "a", encoding="utf-8") as jf:
            while True:
                b = ser.read(4096)
                if not b:
                    continue
                rawf.write(b)

                for ln in b.decode("utf-8", "ignore").splitlines():
                    line = decode_line(ln)
                    if not line:
                        continue

                    checksum_ok, checksum_bad, malformed_frame = nmea_checksum_status(line)
                    lat, lon = parse_lat_lon(line)

                    rec = {
                        "timestamp_utc": ts(),
                        "trial_id": TRIAL_ID,
                        "node_id": NODE_ID,
                        "head_id": HEAD_ID,
                        "line": line,
                        "checksum_ok": checksum_ok,
                        "checksum_bad": checksum_bad,
                        "malformed_frame": malformed_frame,
                    }

                    for key in ("battery_mv", "battery_pct", "usb_power", "is_charging", "rssi_dbm"):
                        v = maybe_int(key, line)
                        if v is not None:
                            rec[key] = v

                    for key in ("snr_db", "rsrp_dbm", "elev_m"):
                        v = maybe_float(key, line)
                        if v is not None:
                            rec[key] = v

                    if lat is not None and lon is not None:
                        rec["lat"] = lat
                        rec["lon"] = lon

                    sat = maybe_str("satellite_link_status", line)
                    if sat is not None:
                        rec["satellite_link_status"] = sat

                    wt = maybe_str("weather_tag", line)
                    if wt is not None:
                        rec["weather_tag"] = wt

                    jf.write(json.dumps(rec) + "\n")
                    jf.flush()

    except Exception as e:
        with open(err_path, "a", encoding="utf-8") as ef:
            ef.write(f"{ts()} collector_error {type(e).__name__}: {e}\n")
        time.sleep(2)
