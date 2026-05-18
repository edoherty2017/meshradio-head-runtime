Place systemd unit files here (head-only services).

## Meshtastic fallback worker units

Install and enable the timer-driven worker:

```bash
sudo install -m 0644 services/meshtastic_fallback_worker.service /etc/systemd/system/meshtastic_fallback_worker.service
sudo install -m 0644 services/meshtastic_fallback_worker.timer /etc/systemd/system/meshtastic_fallback_worker.timer
sudo systemctl daemon-reload
sudo systemctl enable --now meshtastic_fallback_worker.timer
sudo systemctl start meshtastic_fallback_worker.service
sudo systemctl status meshtastic_fallback_worker.timer --no-pager
```

Override environment values with a drop-in:

```bash
sudo systemctl edit meshtastic_fallback_worker.service
# [Service]
# Environment=FALLBACK_COMMAND_QUEUE=/home/pump/telemetry_head/fallback_commands.jsonl
# Environment=CONNECTIVITY_MODE_FILE=/home/pump/telemetry_head/connectivity_mode.state
# Environment=FALLBACK_ACK_FILE=/home/pump/telemetry_head/fallback_acks.jsonl
```
