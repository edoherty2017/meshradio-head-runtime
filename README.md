# MeshRadio Head Runtime

Runtime code/config for the carried HEAD node.

## Includes
- telemetry collection (`scripts/telemetry_collector.py`)
- store-and-forward sync (`scripts/telemetry_sync_spool.sh`)
- fallback command queue helper (`scripts/fallback_command_queue.py`)
- Meshtastic fallback worker (`scripts/meshtastic_fallback_worker.py`)
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

## Meshtastic fallback worker (queue consumer)
The worker consumes queued commands, sends via Meshtastic transport adapter, and updates command lifecycle:

- `queued -> sent` on successful transport submission
- `sent -> acked` on ACK ingestion
- `queued|sent -> expired` when TTL is exceeded
- retries with exponential backoff for transient send failures

### One-shot dry-run smoke
```bash
python3 scripts/meshtastic_fallback_worker.py \
  --once \
  --dry-run \
  --queue-path /home/pump/telemetry_head/fallback_commands.jsonl \
  --out-dir /home/pump/telemetry_head \
  --connectivity-mode-file /home/pump/telemetry_head/connectivity_mode.state \
  --ack-file /home/pump/telemetry_head/fallback_acks.jsonl
```

### Real Meshtastic CLI mode
If `meshtastic` CLI is in `PATH`, the worker uses it automatically (or set `--meshtastic-cli /path/to/meshtastic`).
Use `--meshtastic-extra-args` for adapter-specific flags.

### ACK ingestion paths
- Manual ACK JSONL file (`--ack-file`): entries containing `command_id`
- RX log parse (`--rx-log`): lines containing both `ACK` and command IDs like `cmd-abcdef123456`

### Structured worker events
Worker appends JSONL to:
- `/home/pump/telemetry_head/fallback_worker_events.jsonl`

Event types include:
- `COMMAND_SENT`
- `COMMAND_SEND_FAILED`
- `ACK_INGESTED`
- `COMMAND_EXPIRED`
- `WORKER_PASS_SUMMARY`
- `SKIP_MODE_MISMATCH`
- `SKIP_LOCK_HELD`

### systemd deploy
```bash
sudo install -m 0755 scripts/meshtastic_fallback_worker.py /home/pump/meshradio-head-runtime/scripts/meshtastic_fallback_worker.py
sudo install -m 0644 services/meshtastic_fallback_worker.service /etc/systemd/system/meshtastic_fallback_worker.service
sudo install -m 0644 services/meshtastic_fallback_worker.timer /etc/systemd/system/meshtastic_fallback_worker.timer
sudo systemctl daemon-reload
sudo systemctl enable --now meshtastic_fallback_worker.timer
sudo systemctl start meshtastic_fallback_worker.service
```

This worker is mode-gated for MESH_ONLY windows by reading `connectivity_mode.state`.
