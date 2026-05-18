# MeshRadio Head Runtime

Runtime code/config for the carried HEAD node.

## Includes
- telemetry collection (`scripts/telemetry_collector.py`)
- store-and-forward sync (`scripts/telemetry_sync_spool.sh`)
- fallback command queue helper (`scripts/fallback_command_queue.py`)
- head-specific systemd services (`services/*.service`, `services/*.timer`)

## Offline/mobile behavior
- Telemetry collection is local-first (serial -> local `raw/` + `jsonl/`).
- If Wi-Fi/hotspot drops, collection continues locally.
- Sync retries automatically on timer; data is forwarded once connectivity returns.
- Sync spool now emits connectivity transition events to `connectivity_events.jsonl` with:
  - `CONNECTIVITY_MODE_CHANGE` (`IP_FULL`, `IP_DEGRADED`, `MESH_ONLY`)
  - `CONTROL_PLANE_DOWN_START`
  - `CONTROL_PLANE_DOWN_END`

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

## Fallback command queue (Meshtastic bridge handoff)
Use this helper to queue lightweight control commands when in `MESH_ONLY` windows:

```bash
python3 scripts/fallback_command_queue.py enqueue \
  --target meshhikernode1 \
  --command "status" \
  --ttl-sec 600 \
  --mode MESH_ONLY

python3 scripts/fallback_command_queue.py list --only-pending
```

A future transport bridge can consume this queue and deliver via Meshtastic, then mark `sent`/`acked`.
