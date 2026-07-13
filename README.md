# MeshRadio Head Runtime

Runtime collection, fallback control, Starlink telemetry, and verified evidence
sync for the carried HEAD node.

## Security and ownership model

- Executable code is installed root-owned under `/opt/manet/head/scripts`.
- Services run unprivileged as `pump`; the collector additionally receives the
  `dialout` group needed for `/dev/lora_radio`.
- The collector is the sole serial owner. Fallback commands are submitted over
  `/run/manet/collector.sock`; the worker never opens the radio in the normal
  deployment.
- Mutable evidence remains under `/home/pump/telemetry_head` and is never
  deleted by provisioning or sync.
- SSH sync uses `/etc/manet/ssh_known_hosts` with strict host verification.
- Site-specific values belong in root-owned `/etc/manet/head.env`. In
  particular, set `REMOTE_TARGET=user@host:/absolute/ingest/path`; no sync
  destination is embedded in a unit file.

## Evidence synchronized upstream

The spool transfers `jsonl/`, `raw/`, `starlink/`, connectivity transitions,
fallback lifecycle events/queue/ACKs, and the node map. `IP_FULL` is recorded
only after every requested evidence transfer succeeds. Reachable SSH with a
failed transfer is `IP_DEGRADED`; an unreachable host is `MESH_ONLY`.

## Fallback command path

Queue a small command (160 UTF-8 bytes by default):

```bash
python3 /opt/manet/head/scripts/fallback_command_queue.py \
  --queue-path /home/pump/telemetry_head/fallback_commands.jsonl \
  enqueue --target '!db51af80' --command status --ttl-sec 600
```

Queue updates use a shared lock and atomic replacement. Missing connectivity
state, unresolved destinations, unavailable IPC, oversized payloads, and
malformed queue records fail closed. `--dry-run` is explicit and must never be
used as transmission evidence.

ACK-file records require `FALLBACK_ACK_HMAC_KEY_FILE` in `head.env`. The
`auth_tag` is lowercase HMAC-SHA256 over canonical compact JSON of the full ACK
object with `auth_tag` omitted (keys sorted). RX-log ACK text is ignored unless
the unsafe development-only flag is explicitly passed.

## Deployment and verification

Run `ops/provision_head2.sh` with a reviewed `STARLINK_GRPC_COMMIT`. The
provisioner pins that checkout, records the resolved commit and Python package
freeze under `/opt/manet/software-manifest`, preserves evidence, and refuses to
enable sync until a destination and verified host key exist.

Provisioning also fails unless the existing system Python can import
`meshtastic.serial_interface`, `pubsub`, and `serial`. It records the Python
version and the installed `meshtastic`, `pypubsub`, and `pyserial` distribution
versions in the runtime manifest. It does not silently install or upgrade this
stack; review those recorded versions as part of each deployment.

```bash
python3 /opt/manet/head/scripts/head_readiness_report.py
python3 /opt/manet/head/scripts/starlink_phase1_gate.py
```

Readiness checks services, sole serial ownership/IPC, recent parseable records,
real (non-heartbeat) Starlink measurement state, connectivity-state consistency,
last successful sync, and free storage. The canonical full-stream schema gate
is `resilient-emergency-manets/scripts/validation/schema_validate.py`.
