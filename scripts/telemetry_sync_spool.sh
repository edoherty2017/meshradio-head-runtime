#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${OUT_DIR:-/home/pump/telemetry_head}"
REMOTE_TARGET="${REMOTE_TARGET:-}"   # e.g. doher@100.102.102.63:/home/doher/manet_ingest/meshradiohead
SSH_OPTS="${SSH_OPTS:--o BatchMode=yes -o ConnectTimeout=8 -o ServerAliveInterval=5 -o ServerAliveCountMax=2 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null}"
LOCKFILE="/tmp/telemetry_sync_spool.lock"
LOGFILE="${OUT_DIR}/sync_spool.log"

mkdir -p "${OUT_DIR}"
exec 9>"${LOCKFILE}"
if ! flock -n 9; then
  exit 0
fi

ts(){ date -u +"%Y-%m-%dT%H:%M:%SZ"; }

if [[ -z "${REMOTE_TARGET}" ]]; then
  echo "$(ts) sync skipped: REMOTE_TARGET not set" >> "${LOGFILE}"
  exit 0
fi

REMOTE_HOST="${REMOTE_TARGET%%:*}"
REMOTE_PATH="${REMOTE_TARGET#*:}"

if ! ssh ${SSH_OPTS} "${REMOTE_HOST}" "echo ok" >/dev/null 2>&1; then
  echo "$(ts) sync deferred: remote unavailable ${REMOTE_HOST}" >> "${LOGFILE}"
  exit 0
fi

ssh ${SSH_OPTS} "${REMOTE_HOST}" "mkdir -p '${REMOTE_PATH}/jsonl' '${REMOTE_PATH}/raw'"

if [[ -d "${OUT_DIR}/jsonl" ]]; then
  rsync -az --append-verify -e "ssh ${SSH_OPTS}" "${OUT_DIR}/jsonl/" "${REMOTE_HOST}:${REMOTE_PATH}/jsonl/"
fi
if [[ -d "${OUT_DIR}/raw" ]]; then
  rsync -az --append-verify -e "ssh ${SSH_OPTS}" "${OUT_DIR}/raw/" "${REMOTE_HOST}:${REMOTE_PATH}/raw/"
fi

echo "$(ts) sync ok -> ${REMOTE_TARGET}" >> "${LOGFILE}"
