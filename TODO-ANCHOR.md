# TODO Anchor — meshradio-head-runtime

This anchor was pruned to active, execution-critical items only.

## Priority Order (strict)

## [P1] Keep runtime stable and commandable
- [x] Connectivity-mode and control-plane outage events emitted by spool.
- [x] Meshtastic fallback queue worker deployed with retries/backoff/TTL/ACK lifecycle.
- [ ] Add deterministic serial arbitration policy between collector and Meshtastic CLI worker (no port contention).
- [ ] Produce repeatable PASS/FAIL probe script for serial ownership + worker send/ack path.

## [P2] Telemetry schema parity + readiness evidence
- [x] Collector parser integrity counters present (`checksum_ok`, `checksum_bad`, `malformed_frame`).
- [ ] Enforce required-field shape with explicit null semantics for missing GNSS/cellular fields.
- [ ] Emit machine-readable head readiness report (service state, schema parity, backlog health).

## [P3] Cellular telemetry ingestion (head)
- [ ] Add host-side cellular telemetry collector (ModemManager/NM based, null-safe when modem absent).
- [ ] Merge cellular status into runtime telemetry export path.
- [ ] Add validation matrix for modem present/absent and attached/detached states.

## Completion condition for this repo
- [ ] Head runtime can: (1) collect schema-valid telemetry, (2) survive control-plane outages, (3) send fallback commands over Meshtastic, and (4) expose cellular telemetry state for analysis.
