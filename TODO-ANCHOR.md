# TODO Anchor — meshradio-head-runtime

This repo owns head-node runtime collection, parser robustness, and spool/sync reliability.

## Priority Order (Do in sequence)

## [P1] Runtime hardening (head)
- [ ] P1-C Add parser status counters to collector (`checksum_ok`, `checksum_bad`, `malformed_frame`) in `scripts/telemetry_collector.py`.
- [ ] Ensure collector output conforms to canonical telemetry schema from orchestration repo.
- [ ] Validate serial device detection compatibility with validation pack.
- [ ] Verify spool resilience in `scripts/telemetry_sync_spool.sh` (retry/backlog flush behavior).
- [ ] Produce head-side PASS/FAIL evidence for device + field-population + outage recovery.

## [P2] Runtime support for topo/geology pipeline inputs
- [ ] Ensure required raw telemetry fields for downstream DEM/geology stages are always present or explicitly null-labeled.
- [ ] Add/verify provenance tags required by downstream manifests.

## [P3] Runtime support for inference path
- [ ] Guarantee stable export cadence and schema for live inference ingestion.
- [ ] Confirm fallback metric fields (`rsrp_dbm`, `rssi_dbm`) are emitted with clear null semantics.

## [P4] Runtime support for sentinel/quantifier
- [ ] Ensure collector emits fields needed for anomaly and stratified error analysis.
- [ ] Add any missing diagnostic counters required by sentinel gates.

## [P5] Runtime support for weather guard signals
- [ ] Ensure runtime can consume/act on hold/caution signals from orchestration layer (if implemented via config/event file).

## [P6] Release readiness
- [ ] Pass integrated dry-run/live-trial runtime checks.
- [ ] Tag release cut with verified runtime evidence.

## Completion condition for this repo
- [ ] Head runtime is schema-stable, outage-resilient, and fully compatible with P1–P6 orchestration gates.
