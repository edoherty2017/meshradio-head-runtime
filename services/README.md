# HEAD systemd units

Install these units root-owned in `/etc/systemd/system` via
`ops/provision_head2.sh`. They execute root-owned code from `/opt/manet/head`,
run as `pump`, use `/run/manet` for locks/IPC, and write only to the evidence
directory.

Site configuration belongs in `/etc/manet/head.env`; do not edit unit files to
embed credentials, destinations, or user-writable Python paths. SSH sync is not
ready until `/etc/manet/ssh_known_hosts` contains an independently verified key.

After changing configuration:

```bash
sudo systemctl daemon-reload
sudo systemctl restart telemetry_collector_head.service
sudo systemctl start telemetry_sync_spool.service
sudo systemctl start meshtastic_fallback_worker.service
```
