# TODO Anchor — meshradio-head-runtime

This anchor was pruned to active, execution-critical items only.

## Priority Order (strict)

## [P1] Keep runtime stable and commandable
- [x] Connectivity-mode and control-plane outage events emitted by spool.
- [x] Meshtastic fallback queue worker deployed with retries/backoff/TTL/ACK lifecycle.
- [x] Enforce one serial owner: collector holds `/run/manet/lora_serial.lock`; fallback worker sends through `/run/manet/collector.sock`.
- [ ] Re-run a hardware PASS/FAIL probe for collector IPC send plus authenticated ACK after deploying the hardened units. `--dry-run` is not evidence.

## [P2] Telemetry schema parity + readiness evidence
- [x] Collector parser integrity counters present (`checksum_ok`, `checksum_bad`, `malformed_frame`).
- [x] Enforce required-field shape with explicit null semantics for missing GNSS/cellular fields. POSITION packets always emit `lat`/`lon`/`elev_m` (null when no GPS fix).
- [x] Emit machine-readable head readiness report (service state, schema parity, backlog health). → `scripts/head_readiness_report.py`

## [P3] Cellular telemetry ingestion (head)
- [ ] Add host-side cellular telemetry collector (ModemManager/NM based, null-safe when modem absent).
- [ ] Merge cellular status into runtime telemetry export path.
- [ ] Add validation matrix for modem present/absent and attached/detached states.

## [P2.5] Starlink gRPC telemetry integration
- [x] `scripts/starlink_raw_poller.py` — polls 192.168.100.1:9200 every 15s, heartbeat on failure, handles itertools.chain bug
- [x] `scripts/starlink_window_aggregator.py` — 60s tumbling windows, p50/p95 (null if insufficient), outage seconds
- [x] `scripts/merge_starlink_into_telemetry.py` — merge_asof 65s tolerance, backward, UTC coercion
- [x] `scripts/starlink_chrony_qc.py` — ClockErrorBound computation, QC CSV append
- [x] `scripts/starlink_phase1_gate.py` — Phase 1 gate: ≥95% row alignment, 60min run, value ranges
- [x] `services/starlink_raw_poller.service` + `services/starlink_window_aggregator.service`
- [ ] Deploy the reviewed Starlink dependency into the hardened root-owned paths
  (`/opt/manet/vendor/starlink-grpc-tools` and `/opt/manet/venvs/starlink`) and
  record the exact commit plus installed-package manifest.
- [ ] Re-run Phase 1 gate after deploying the hardened collector. The 2026-05-20 result is invalidated because the old gate could pass aligned RPC-error heartbeats.
- [ ] Re-run Phase 2 only against a new hardened Phase 1 PASS. The 2026-05-20
  artifact-production check does not establish that its source rows were real
  Starlink measurements.

## Completion condition for this repo
- [ ] Re-establish completion with a canonical schema report, real IPC send/authenticated ACK, transfer-failure drill, and hardened Starlink Phase 1 PASS.
