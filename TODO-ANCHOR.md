# TODO Anchor — meshradio-head-runtime

This anchor was pruned to active, execution-critical items only.

## Priority Order (strict)

## [P1] Keep runtime stable and commandable
- [x] Connectivity-mode and control-plane outage events emitted by spool.
- [x] Meshtastic fallback queue worker deployed with retries/backoff/TTL/ACK lifecycle.
- [x] Add deterministic serial arbitration policy between collector and Meshtastic CLI worker (no port contention). Lockfile at `SERIAL_LOCK_PATH` (`/run/lock/manet_lora_serial.lock`); collector holds LOCK_EX while interface open; worker logs SKIP_SERIAL_HELD if blocked.
- [x] Produce repeatable PASS/FAIL probe script for serial ownership + worker send/ack path. → `scripts/serial_probe.py`

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
- [x] Deploy starlink-grpc-tools venv on HEAD Pi (`/home/pump/.venvs/starlink/`) — done 2026-05-20
- [x] Run Phase 1 gate: `python3 scripts/starlink_phase1_gate.py` after 60 continuous minutes with dish on — PASS 2026-05-20 (571 rows, 97.54% alignment, 264.7 min)
- [x] Run Phase 2: `python3 scripts/merge_starlink_into_telemetry.py` and verify airmap_live_trial.py produces satellite artifacts — PASS 2026-05-20

## Completion condition for this repo
- [x] Head runtime can: (1) collect schema-valid telemetry, (2) survive control-plane outages, (3) send fallback commands over Meshtastic, and (4) expose Starlink satellite telemetry state for analysis.
