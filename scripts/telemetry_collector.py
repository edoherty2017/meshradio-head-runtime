#!/usr/bin/env python3
"""Meshtastic-native telemetry collector.

Replaces raw serial regex parsing with the Meshtastic Python library.
Every successfully decoded packet delivered to the Meshtastic receive callback
is logged as a JSONL row with RSSI, SNR, and portnum-specific fields. Frames
rejected below that callback boundary are not raw evidence in this stream.
Schema stays compatible with schemas/telemetry.schema.json.
"""
from __future__ import annotations

import datetime as dt
import fcntl
import json
import os
import re
import socket
import threading
import time

import meshtastic.serial_interface
from pubsub import pub

PORT             = os.environ.get("LORA_PORT",        "/dev/lora_radio")
TRIAL_ID         = os.environ.get("TRIAL_ID",         "trial-live")
NODE_ID          = os.environ.get("NODE_ID",          "meshradiohead2")
HEAD_ID          = os.environ.get("HEAD_ID",          "meshradiohead2")
OUT_DIR          = os.environ.get("OUT_DIR",          "/home/pump/telemetry_head")
SERIAL_LOCK_PATH = os.environ.get("SERIAL_LOCK_PATH", "/run/manet/lora_serial.lock")
IPC_SOCKET_PATH  = os.environ.get("COLLECTOR_IPC_SOCKET", "/run/manet/collector.sock")
MAX_COMMAND_BYTES = int(os.environ.get("MAX_COMMAND_BYTES", "200"))
if MAX_COMMAND_BYTES <= 0:
    raise RuntimeError("MAX_COMMAND_BYTES must be greater than zero")

own_mesh_id: str | None = None  # set at interface startup
active_interface = None
interface_lock = threading.RLock()
file_write_lock = threading.Lock()

json_dir = os.path.join(OUT_DIR, "jsonl")
err_path = os.path.join(OUT_DIR, "collector_errors.log")
os.makedirs(json_dir, exist_ok=True)

json_path = os.path.join(json_dir, "telemetry_stream.jsonl")


def ts() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def log_err(msg: str) -> None:
    with file_write_lock:
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
        hop_start = packet.get("hopStart")

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
            # Meshtastic only publishes successfully decoded protobuf packets to
            # this callback. These compatibility fields describe that boundary;
            # corrupt serial frames are counted by the library, not observable here.
            "checksum_ok":    1,
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
        # A configured start limit is not observable when hopStart is absent, so
        # do not invent one from Meshtastic's usual default.
        if hop_limit is not None and hop_start is not None:
            rec["hops_away"] = max(0, int(hop_start) - int(hop_limit))

        if "POSITION" in portnum:
            pos   = decoded.get("position", {})
            lat_i = pos.get("latitudeI")
            lon_i = pos.get("longitudeI")
            alt   = pos.get("altitudeHae") if pos.get("altitudeHae") is not None else pos.get("altitude")
            prec  = pos.get("PDOP")
            # Always emit lat/lon/elev_m for POSITION packets so downstream can
            # distinguish "got packet, no fix yet" from "not a position packet."
            rec["lat"]    = round(lat_i / 1e7, 7) if lat_i is not None else None
            rec["lon"]    = round(lon_i / 1e7, 7) if lon_i is not None else None
            rec["elev_m"] = float(alt) if alt is not None else None
            if prec is not None:
                # Meshtastic protobuf encodes PDOP/HDOP in hundredths.
                rec["gps_pdop"] = round(float(prec) / 100.0, 2)

        if "TELEMETRY" in portnum:
            tel = decoded.get("telemetry", {})
            dm  = tel.get("deviceMetrics", {})
            if dm.get("batteryLevel") is not None:
                battery_level = int(dm["batteryLevel"])
                if battery_level == 101:  # Meshtastic external-power sentinel
                    rec["battery_pct"] = None
                    rec["usb_power"] = 1
                else:
                    rec["battery_pct"] = battery_level
            if dm.get("voltage") is not None and dm["voltage"] > 0:
                rec["battery_mv"] = int(round(dm["voltage"] * 1000))
            if dm.get("channelUtilization") is not None:
                rec["channel_util_pct"] = round(float(dm["channelUtilization"]), 3)
            if dm.get("airUtilTx") is not None:
                rec["air_util_tx_pct"] = round(float(dm["airUtilTx"]), 3)

        encoded = json.dumps(rec, separators=(",", ":"), allow_nan=False) + "\n"
        with file_write_lock:
            with open(json_path, "a", encoding="utf-8") as jf:
                jf.write(encoded)
                jf.flush()
                os.fsync(jf.fileno())

    except Exception as exc:
        log_err(f"on_receive {type(exc).__name__}: {exc}")


def _ipc_response(conn: socket.socket, response: dict) -> None:
    conn.sendall((json.dumps(response, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8"))


def _handle_ipc_connection(conn: socket.socket) -> None:
    conn.settimeout(5.0)
    try:
        raw = b""
        while b"\n" not in raw and len(raw) <= 4096:
            chunk = conn.recv(4096 - len(raw))
            if not chunk:
                break
            raw += chunk
        request = json.loads(raw.split(b"\n", 1)[0].decode("utf-8"))
        if not isinstance(request, dict):
            _ipc_response(conn, {"ok": False, "error": "request_not_object"})
            return
        if request.get("operation") != "send_text":
            _ipc_response(conn, {"ok": False, "error": "unsupported_operation"})
            return
        target_value = request.get("target", "")
        payload_value = request.get("payload", "")
        if not isinstance(target_value, str) or not isinstance(payload_value, str):
            _ipc_response(conn, {"ok": False, "error": "invalid_field_type"})
            return
        target = target_value.strip()
        payload = payload_value
        payload_size = len(payload.encode("utf-8"))
        if not payload or payload_size > MAX_COMMAND_BYTES:
            _ipc_response(conn, {"ok": False, "error": "invalid_payload_length", "bytes": payload_size})
            return
        if not target:
            _ipc_response(conn, {"ok": False, "error": "missing_target"})
            return
        if target not in {"*", "all", "broadcast", "^all"} and not re.fullmatch(r"(?:!?[0-9a-fA-F]{8}|[0-9]+)", target):
            _ipc_response(conn, {"ok": False, "error": "invalid_target"})
            return
        with interface_lock:
            iface = active_interface
            if iface is None or getattr(iface, "_closed", False):
                _ipc_response(conn, {"ok": False, "error": "radio_unavailable"})
                return
            destination = None if target in {"*", "all", "broadcast", "^all"} else target
            iface.sendText(payload, destinationId=destination, wantAck=destination is not None)
        _ipc_response(conn, {"ok": True, "transport": "collector_ipc", "target": target or "broadcast"})
    except Exception as exc:
        try:
            _ipc_response(conn, {"ok": False, "error": f"{type(exc).__name__}: {exc}"})
        except Exception:
            pass


def ipc_server() -> None:
    socket_path = os.path.abspath(IPC_SOCKET_PATH)
    os.makedirs(os.path.dirname(socket_path), mode=0o750, exist_ok=True)
    try:
        os.unlink(socket_path)
    except FileNotFoundError:
        pass
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(socket_path)
        os.chmod(socket_path, 0o660)
        server.listen(8)
        while True:
            conn, _ = server.accept()
            with conn:
                _handle_ipc_connection(conn)


def main() -> None:
    global own_mesh_id, active_interface
    # Take the ownership lock before publishing the IPC endpoint. A duplicate
    # collector must fail instead of unlinking the active collector's socket and
    # then blocking behind its serial lock.
    lock_fh = open(SERIAL_LOCK_PATH, "a+", encoding="utf-8")
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_fh.close()
        log_err(f"serial ownership lock already held: {SERIAL_LOCK_PATH}")
        raise SystemExit(1)

    pub.subscribe(on_receive, "meshtastic.receive")
    threading.Thread(target=ipc_server, name="collector-ipc", daemon=True).start()

    while True:
        iface = None
        try:
            iface = meshtastic.serial_interface.SerialInterface(PORT)
            info = iface.getMyNodeInfo()
            own_mesh_id = (info or {}).get("user", {}).get("id")
            with interface_lock:
                active_interface = iface
            # Block until the interface disconnects or errors
            while iface and not getattr(iface, "_closed", False):
                time.sleep(5)
        except Exception as exc:
            log_err(f"interface {type(exc).__name__}: {exc}")
        finally:
            with interface_lock:
                active_interface = None
            try:
                if iface:
                    iface.close()
            except Exception:
                pass
        time.sleep(5)


if __name__ == "__main__":
    main()
