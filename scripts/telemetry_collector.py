#!/usr/bin/env python3
"""Meshtastic-native telemetry collector.

Replaces raw serial regex parsing with the Meshtastic Python library.
Every over-the-air packet is logged as a JSONL row with RSSI, SNR, and
decoded portnum-specific fields (position, battery telemetry).
Schema stays compatible with schemas/telemetry.schema.json.
"""
from __future__ import annotations

import datetime as dt
import fcntl
import json
import os
import time

import meshtastic.serial_interface
from pubsub import pub

PORT             = os.environ.get("LORA_PORT",        "/dev/lora_radio")
BAUD             = int(os.environ.get("LORA_BAUD",    "115200"))
TRIAL_ID         = os.environ.get("TRIAL_ID",         "trial-unknown")
NODE_ID          = os.environ.get("NODE_ID",          "meshradiohead")
HEAD_ID          = os.environ.get("HEAD_ID",          "meshnodehead")
OUT_DIR          = os.environ.get("OUT_DIR",          "/home/pump/telemetry_head")
SERIAL_LOCK_PATH = os.environ.get("SERIAL_LOCK_PATH", "/run/lock/manet_lora_serial.lock")

own_mesh_id: str | None = None  # set at interface startup

json_dir = os.path.join(OUT_DIR, "jsonl")
err_path = os.path.join(OUT_DIR, "collector_errors.log")
os.makedirs(json_dir, exist_ok=True)

json_path = os.path.join(json_dir, "telemetry_stream.jsonl")


def ts() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def log_err(msg: str) -> None:
    with open(err_path, "a", encoding="utf-8") as f:
        f.write(f"{ts()} collector_error {msg}\n")


def on_receive(packet, interface) -> None:
    try:
        decoded  = packet.get("decoded", {})
        portnum  = str(decoded.get("portnum", "UNKNOWN_APP"))
        rssi      = packet.get("rxRssi")
        snr       = packet.get("rxSnr")
        from_id   = packet.get("fromId")
        hop_limit = packet.get("hopLimit")
        hop_start = packet.get("hopStart")  # absent on older firmware; default 3

        rec: dict = {
            "timestamp_utc":  ts(),
            "trial_id":       TRIAL_ID,
            "node_id":        NODE_ID,
            "head_id":        HEAD_ID,
            "from_mesh_id":   from_id,
            "is_own_node":    (from_id == own_mesh_id) if own_mesh_id else None,
            "portnum":        portnum,
            # 'line' kept for schema compatibility — human-readable summary
            "line":           f"{portnum} from={from_id} rssi={rssi} snr={snr}",
            "checksum_ok":    0,
            "checksum_bad":   0,
            "malformed_frame": 0,
        }

        if rssi is not None:
            rec["rssi_dbm"] = int(rssi)
        if snr is not None:
            rec["snr_db"] = float(snr)
        if hop_limit is not None:
            rec["hop_limit"] = int(hop_limit)
        if hop_start is not None:
            rec["hop_start"] = int(hop_start)
        # hops_away = relays taken; if hop_start absent assume Meshtastic default of 3
        if hop_limit is not None:
            hs = int(hop_start) if hop_start is not None else 3
            rec["hops_away"] = max(0, hs - int(hop_limit))

        if "POSITION" in portnum:
            pos   = decoded.get("position", {})
            lat_i = pos.get("latitudeI")
            lon_i = pos.get("longitudeI")
            alt   = pos.get("altitudeHae") if pos.get("altitudeHae") is not None else pos.get("altitude")
            prec  = pos.get("PDOP") or pos.get("hdop")
            # Always emit lat/lon/elev_m for POSITION packets so downstream can
            # distinguish "got packet, no fix yet" from "not a position packet."
            rec["lat"]    = round(lat_i / 1e7, 7) if lat_i else None
            rec["lon"]    = round(lon_i / 1e7, 7) if lon_i else None
            rec["elev_m"] = float(alt) if alt is not None else None
            if prec is not None:
                rec["gps_pdop"] = float(prec)

        if "TELEMETRY" in portnum:
            tel = decoded.get("telemetry", {})
            dm  = tel.get("deviceMetrics", {})
            if dm.get("batteryLevel") is not None:
                rec["battery_pct"] = int(dm["batteryLevel"])
            if dm.get("voltage") is not None and dm["voltage"] > 0:
                rec["battery_mv"] = int(round(dm["voltage"] * 1000))
            if dm.get("channelUtilization") is not None:
                rec["channel_util_pct"] = round(float(dm["channelUtilization"]), 3)
            if dm.get("airUtilTx") is not None:
                rec["air_util_tx_pct"] = round(float(dm["airUtilTx"]), 3)

        with open(json_path, "a", encoding="utf-8") as jf:
            jf.write(json.dumps(rec) + "\n")
            jf.flush()

    except Exception as exc:
        log_err(f"on_receive {type(exc).__name__}: {exc}")


def main() -> None:
    global own_mesh_id
    pub.subscribe(on_receive, "meshtastic.receive")

    while True:
        iface = None
        lock_fh = None
        try:
            # Acquire exclusive serial lock before opening the interface.
            # Blocks briefly if the fallback worker holds the lock for a send.
            lock_fh = open(SERIAL_LOCK_PATH, "w", encoding="utf-8")
            fcntl.flock(lock_fh, fcntl.LOCK_EX)

            iface = meshtastic.serial_interface.SerialInterface(PORT)
            info = iface.getMyNodeInfo()
            own_mesh_id = (info or {}).get("user", {}).get("id")
            # Block until the interface disconnects or errors
            while iface and not getattr(iface, "_closed", False):
                time.sleep(5)
        except Exception as exc:
            log_err(f"interface {type(exc).__name__}: {exc}")
        finally:
            try:
                if iface:
                    iface.close()
            except Exception:
                pass
            try:
                if lock_fh:
                    fcntl.flock(lock_fh, fcntl.LOCK_UN)
                    lock_fh.close()
            except Exception:
                pass
        time.sleep(5)


if __name__ == "__main__":
    main()
