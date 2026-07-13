from __future__ import annotations

import importlib.util
import json
import socket
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_collector(monkeypatch, tmp_path: Path):
    serial_interface = types.ModuleType("meshtastic.serial_interface")
    serial_interface.SerialInterface = object
    meshtastic = types.ModuleType("meshtastic")
    meshtastic.serial_interface = serial_interface
    pubsub = types.ModuleType("pubsub")
    pubsub.pub = types.SimpleNamespace(subscribe=lambda *_args, **_kwargs: None)
    monkeypatch.setitem(sys.modules, "meshtastic", meshtastic)
    monkeypatch.setitem(sys.modules, "meshtastic.serial_interface", serial_interface)
    monkeypatch.setitem(sys.modules, "pubsub", pubsub)
    monkeypatch.setenv("OUT_DIR", str(tmp_path))
    spec = importlib.util.spec_from_file_location("head_collector_test", ROOT / "scripts" / "telemetry_collector.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_position_zero_and_external_power_sentinel_are_normalized(monkeypatch, tmp_path):
    collector = load_collector(monkeypatch, tmp_path)
    collector.on_receive(
        {
            "decoded": {
                "portnum": "POSITION_APP",
                "position": {"latitudeI": 0, "longitudeI": 0, "altitudeHae": 0, "PDOP": 125},
            },
            "fromId": "!1234abcd",
            "rxRssi": -100,
            "rxSnr": 2.5,
        },
        None,
    )
    collector.on_receive(
        {
            "decoded": {
                "portnum": "TELEMETRY_APP",
                "telemetry": {"deviceMetrics": {"batteryLevel": 101, "voltage": 4.2}},
            },
            "fromId": "!1234abcd",
        },
        None,
    )

    rows = [json.loads(line) for line in (tmp_path / "jsonl" / "telemetry_stream.jsonl").read_text().splitlines()]
    assert rows[0]["lat"] == 0.0
    assert rows[0]["lon"] == 0.0
    assert rows[0]["elev_m"] == 0.0
    assert rows[0]["gps_pdop"] == 1.25
    assert rows[1]["battery_pct"] is None
    assert rows[1]["usb_power"] == 1
    assert rows[1]["battery_mv"] == 4200


def test_hops_away_is_not_invented_without_hop_start(monkeypatch, tmp_path):
    collector = load_collector(monkeypatch, tmp_path)
    collector.on_receive(
        {
            "decoded": {"portnum": "TEXT_MESSAGE_APP"},
            "fromId": "!1234abcd",
            "hopLimit": 1,
        },
        None,
    )

    row = json.loads((tmp_path / "jsonl" / "telemetry_stream.jsonl").read_text())
    assert row["hop_limit"] == 1
    assert "hop_start" not in row
    assert "hops_away" not in row


def test_ipc_rejects_implicit_empty_broadcast_target(monkeypatch, tmp_path):
    collector = load_collector(monkeypatch, tmp_path)
    client, server = socket.socketpair()
    try:
        client.sendall(b'{"operation":"send_text","target":"","payload":"status"}\n')
        collector._handle_ipc_connection(server)
        response = json.loads(client.recv(4096).decode("utf-8"))
    finally:
        client.close()
        server.close()

    assert response == {"ok": False, "error": "missing_target"}
