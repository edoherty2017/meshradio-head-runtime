# MeshRadio Head Runtime

Runtime code/config for the carried HEAD node.

## Includes
- telemetry collection (`scripts/telemetry_collector.py`)
- store-and-forward sync (`scripts/telemetry_sync_spool.sh`)
- head-specific systemd services (`services/*.service`, `services/*.timer`)

## Offline/mobile behavior
- Telemetry collection is local-first (serial -> local `raw/` + `jsonl/`).
- If Wi-Fi/hotspot drops, collection continues locally.
- Sync retries automatically on timer; data is forwarded once connectivity returns.

## Deploy sync spool
Copy files to target Pi and enable timer:

```bash
sudo install -m 0755 scripts/telemetry_sync_spool.sh /home/pump/telemetry_sync_spool.sh
sudo install -m 0644 services/telemetry_sync_spool.service /etc/systemd/system/telemetry_sync_spool.service
sudo install -m 0644 services/telemetry_sync_spool.timer /etc/systemd/system/telemetry_sync_spool.timer
sudo systemctl daemon-reload
sudo systemctl enable --now telemetry_sync_spool.timer
sudo systemctl start telemetry_sync_spool.service
sudo systemctl status telemetry_sync_spool.timer --no-pager
```

## iPhone hotspot
Yes — the HEAD Pi can use an iPhone hotspot as upstream internet (normal Wi-Fi client mode).
This does not change local LoRa capture behavior; it only affects when sync can forward data.
