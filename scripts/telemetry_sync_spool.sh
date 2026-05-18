#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${OUT_DIR:-/home/pump/telemetry_head}"
REMOTE_TARGET="${REMOTE_TARGET:-}"   # e.g. doher@100.102.102.63:/home/doher/manet_ingest/meshradiohead
SSH_OPTS="${SSH_OPTS:--o BatchMode=yes -o ConnectTimeout=8 -o ServerAliveInterval=5 -o ServerAliveCountMax=2 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null}"
LOCKFILE="/tmp/telemetry_sync_spool.lock"
LOGFILE="${OUT_DIR}/sync_spool.log"
EVENTS_FILE="${OUT_DIR}/connectivity_events.jsonl"
STATE_FILE="${OUT_DIR}/connectivity_mode.state"
NODE_ID="${NODE_ID:-meshradiohead}"
MODE_IP_FULL="IP_FULL"
MODE_IP_DEGRADED="IP_DEGRADED"
MODE_FALLBACK="${MODE_FALLBACK:-MESH_ONLY}"

mkdir -p "${OUT_DIR}"
exec 9>"${LOCKFILE}"
if ! flock -n 9; then
  exit 0
fi

ts(){ date -u +"%Y-%m-%dT%H:%M:%SZ"; }

emit_event(){
  local event_type="$1"
  local mode="$2"
  local detail="$3"
  printf '{"timestamp_utc":"%s","node_id":"%s","remote_host":"%s","event_type":"%s","connectivity_mode":"%s","detail":"%s"}\n' \
    "$(ts)" "${NODE_ID}" "${REMOTE_HOST:-unknown}" "${event_type}" "${mode}" "${detail}" >> "${EVENTS_FILE}"
}

set_mode(){
  local new_mode="$1"
  local reason="$2"
  local previous_mode=""
  if [[ -f "${STATE_FILE}" ]]; then
    previous_mode="$(cat "${STATE_FILE}" 2>/dev/null || true)"
  fi

  if [[ "${previous_mode}" != "${new_mode}" ]]; then
    if [[ "${new_mode}" == "${MODE_FALLBACK}" ]]; then
      emit_event "CONTROL_PLANE_DOWN_START" "${new_mode}" "${reason}"
    elif [[ "${previous_mode}" == "${MODE_FALLBACK}" ]]; then
      emit_event "CONTROL_PLANE_DOWN_END" "${new_mode}" "${reason}"
    fi
    emit_event "CONNECTIVITY_MODE_CHANGE" "${new_mode}" "${reason}"
    echo "${new_mode}" > "${STATE_FILE}"
    echo "$(ts) connectivity_mode=${new_mode} reason=${reason}" >> "${LOGFILE}"
  fi
}

if [[ -z "${REMOTE_TARGET}" ]]; then
  set_mode "${MODE_IP_DEGRADED}" "REMOTE_TARGET_NOT_SET"
  echo "$(ts) sync skipped: REMOTE_TARGET not set" >> "${LOGFILE}"
  exit 0
fi

REMOTE_HOST="${REMOTE_TARGET%%:*}"
REMOTE_PATH="${REMOTE_TARGET#*:}"

if ! ssh ${SSH_OPTS} "${REMOTE_HOST}" "echo ok" >/dev/null 2>&1; then
  set_mode "${MODE_FALLBACK}" "REMOTE_UNAVAILABLE"
  echo "$(ts) sync deferred: remote unavailable ${REMOTE_HOST}" >> "${LOGFILE}"
  exit 0
fi

set_mode "${MODE_IP_FULL}" "REMOTE_REACHABLE"

ssh ${SSH_OPTS} "${REMOTE_HOST}" "mkdir -p '${REMOTE_PATH}/jsonl' '${REMOTE_PATH}/raw'"

if [[ -d "${OUT_DIR}/jsonl" ]]; then
  rsync -az --append-verify -e "ssh ${SSH_OPTS}" "${OUT_DIR}/jsonl/" "${REMOTE_HOST}:${REMOTE_PATH}/jsonl/"
fi
if [[ -d "${OUT_DIR}/raw" ]]; then
  rsync -az --append-verify -e "ssh ${SSH_OPTS}" "${OUT_DIR}/raw/" "${REMOTE_HOST}:${REMOTE_PATH}/raw/"
fi

echo "$(ts) sync ok -> ${REMOTE_TARGET}" >> "${LOGFILE}"
