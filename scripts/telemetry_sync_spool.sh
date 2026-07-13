#!/usr/bin/env bash
set -euo pipefail

for required_command in flock ssh rsync getent; do
  if ! command -v "${required_command}" >/dev/null 2>&1; then
    printf 'telemetry_sync_spool: required command missing: %s\n' "${required_command}" >&2
    exit 2
  fi
done

OUT_DIR="${OUT_DIR:-/home/pump/telemetry_head}"
REMOTE_TARGET="${REMOTE_TARGET:-}"
SSH_OPTS="${SSH_OPTS:--o BatchMode=yes -o ConnectTimeout=8 -o ServerAliveInterval=5 -o ServerAliveCountMax=2 -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/etc/manet/ssh_known_hosts}"
LOCKFILE="${SYNC_LOCK_FILE:-${RUNTIME_DIRECTORY:-${OUT_DIR}}/telemetry_sync_spool.lock}"
LOGFILE="${OUT_DIR}/sync_spool.log"
EVENTS_FILE="${OUT_DIR}/connectivity_events.jsonl"
STATE_FILE="${OUT_DIR}/connectivity_mode.state"
SUCCESS_FILE="${OUT_DIR}/last_sync_success.json"
NODE_ID="${NODE_ID:-meshradiohead2}"
MODE_IP_FULL="IP_FULL"
MODE_IP_DEGRADED="IP_DEGRADED"
MODE_FALLBACK="MESH_ONLY"

if [[ ! "${NODE_ID}" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  printf 'telemetry_sync_spool: unsafe NODE_ID: %s\n' "${NODE_ID}" >&2
  exit 2
fi

mkdir -p "${OUT_DIR}" "$(dirname "${LOCKFILE}")"
exec 9>"${LOCKFILE}"
if ! flock -n 9; then
  exit 0
fi

read -r -a SSH_ARGS <<<"${SSH_OPTS}"
ts(){ date -u +"%Y-%m-%dT%H:%M:%SZ"; }

emit_event(){
  local event_type="$1"
  local mode="$2"
  local detail="$3"
  printf '{"timestamp_utc":"%s","node_id":"%s","remote_host":"%s","event_type":"%s","connectivity_mode":"%s","detail":"%s"}\n' \
    "$(ts)" "${NODE_ID}" "${REMOTE_HOST:-unknown}" "${event_type}" "${mode}" "${detail}" >>"${EVENTS_FILE}"
}

atomic_write_line(){
  local target="$1"
  local value="$2"
  local tmp
  tmp="$(mktemp "${OUT_DIR}/.$(basename "${target}").XXXXXX")"
  printf '%s\n' "${value}" >"${tmp}"
  chmod 0600 "${tmp}"
  mv -f "${tmp}" "${target}"
}

set_mode(){
  local new_mode="$1"
  local reason="$2"
  local previous_mode=""
  if [[ -f "${STATE_FILE}" ]]; then
    previous_mode="$(<"${STATE_FILE}")"
  fi
  if [[ "${previous_mode}" != "${new_mode}" ]]; then
    if [[ "${new_mode}" == "${MODE_FALLBACK}" ]]; then
      emit_event "CONTROL_PLANE_DOWN_START" "${new_mode}" "${reason}"
    elif [[ "${previous_mode}" == "${MODE_FALLBACK}" ]]; then
      emit_event "CONTROL_PLANE_DOWN_END" "${new_mode}" "${reason}"
    fi
    emit_event "CONNECTIVITY_MODE_CHANGE" "${new_mode}" "${reason}"
    atomic_write_line "${STATE_FILE}" "${new_mode}"
    printf '%s connectivity_mode=%s reason=%s\n' "$(ts)" "${new_mode}" "${reason}" >>"${LOGFILE}"
  else
    # Emit a heartbeat so downstream evidence can bound how long an observed
    # state is carried forward; a months-old transition is not current proof.
    emit_event "CONNECTIVITY_MODE_OBSERVED" "${new_mode}" "${reason}"
  fi
}

reject_target(){
  local reason="$1"
  set_mode "${MODE_IP_DEGRADED}" "${reason}"
  printf '%s sync rejected: %s\n' "$(ts)" "${reason}" >>"${LOGFILE}"
  exit 1
}

if [[ -z "${REMOTE_TARGET}" ]]; then
  reject_target "REMOTE_TARGET_NOT_SET"
fi
if [[ "${REMOTE_TARGET}" != *:* ]]; then
  reject_target "REMOTE_TARGET_INVALID"
fi

REMOTE_HOST="${REMOTE_TARGET%%:*}"
REMOTE_PATH="${REMOTE_TARGET#*:}"
REMOTE_ADDR="${REMOTE_HOST#*@}"
if [[ ! "${REMOTE_HOST}" =~ ^[A-Za-z0-9_.@-]+$ || ! "${REMOTE_PATH}" =~ ^/[A-Za-z0-9_./-]+$ ]]; then
  reject_target "REMOTE_TARGET_UNSAFE"
fi
if [[ "${REMOTE_ADDR}" == "localhost" || "${REMOTE_ADDR}" == 127.* ]]; then
  reject_target "REMOTE_TARGET_IS_LOOPBACK"
fi

# Refuse a destination that resolves to this machine. This makes an inventory
# typo visible instead of reporting a successful node-to-itself upload.
for local_identity in "$(hostname)" "$(hostname -f 2>/dev/null || true)" $(hostname -I 2>/dev/null || true); do
  if [[ -n "${local_identity}" && "${REMOTE_ADDR}" == "${local_identity}" ]]; then
    reject_target "REMOTE_TARGET_IS_LOCAL_HOST"
  fi
done
while read -r resolved_addr _; do
  if [[ "${resolved_addr}" == 127.* || "${resolved_addr}" == "::1" ]]; then
    reject_target "REMOTE_TARGET_RESOLVES_TO_LOOPBACK"
  fi
  for local_addr in $(hostname -I 2>/dev/null || true); do
    if [[ "${resolved_addr}" == "${local_addr}" ]]; then
      reject_target "REMOTE_TARGET_RESOLVES_TO_LOCAL_HOST"
    fi
  done
done < <(getent ahosts "${REMOTE_ADDR}" 2>/dev/null || true)

if ! ssh_error="$(ssh "${SSH_ARGS[@]}" "${REMOTE_HOST}" true 2>&1)"; then
  if [[ "${ssh_error}" == *"No route to host"* \
     || "${ssh_error}" == *"Network is unreachable"* \
     || "${ssh_error}" == *"Connection timed out"* \
     || "${ssh_error}" == *"Could not resolve hostname"* ]]; then
    set_mode "${MODE_FALLBACK}" "REMOTE_NETWORK_UNAVAILABLE"
  else
    set_mode "${MODE_IP_DEGRADED}" "SSH_TRUST_AUTH_OR_SERVICE_FAILURE"
  fi
  ssh_error_one_line="${ssh_error//$'\n'/ }"
  printf '%s sync deferred: SSH unavailable %s (%.500s)\n' "$(ts)" "${REMOTE_HOST}" "${ssh_error_one_line}" >>"${LOGFILE}"
  exit 1
fi

if ! ssh "${SSH_ARGS[@]}" "${REMOTE_HOST}" \
  "mkdir -p '${REMOTE_PATH}/jsonl' '${REMOTE_PATH}/raw' '${REMOTE_PATH}/starlink/raw' '${REMOTE_PATH}/starlink/derived' '${REMOTE_PATH}/starlink/qc'"; then
  set_mode "${MODE_IP_DEGRADED}" "REMOTE_PREPARE_FAILED"
  exit 1
fi

transfer_failed=0
for subdir in jsonl raw starlink/raw starlink/derived; do
  if [[ -d "${OUT_DIR}/${subdir}" ]]; then
    if ! rsync -az --partial --append-verify -e "ssh ${SSH_OPTS}" \
      "${OUT_DIR}/${subdir}/" "${REMOTE_HOST}:${REMOTE_PATH}/${subdir}/"; then
      transfer_failed=1
    fi
  fi
done
if [[ -d "${OUT_DIR}/starlink/qc" ]]; then
  if ! rsync -az --partial -e "ssh ${SSH_OPTS}" \
    "${OUT_DIR}/starlink/qc/" "${REMOTE_HOST}:${REMOTE_PATH}/starlink/qc/"; then
    transfer_failed=1
  fi
fi

starlink_root_files=()
for path in "${OUT_DIR}"/starlink/*.log; do
  [[ -f "${path}" ]] && starlink_root_files+=("${path}")
done
if (( ${#starlink_root_files[@]} > 0 )); then
  if ! rsync -az --partial -e "ssh ${SSH_OPTS}" "${starlink_root_files[@]}" \
    "${REMOTE_HOST}:${REMOTE_PATH}/starlink/"; then
    transfer_failed=1
  fi
fi

state_files=()
for name in connectivity_events.jsonl fallback_worker_events.jsonl fallback_commands.jsonl fallback_acks.jsonl connectivity_mode.state last_sync_success.json meshtastic_node_map.json; do
  if [[ -f "${OUT_DIR}/${name}" ]]; then
    state_files+=("${OUT_DIR}/${name}")
  fi
done
if (( ${#state_files[@]} > 0 )); then
  if ! rsync -az --partial -e "ssh ${SSH_OPTS}" "${state_files[@]}" \
    "${REMOTE_HOST}:${REMOTE_PATH}/"; then
    transfer_failed=1
  fi
fi

if (( transfer_failed )); then
  set_mode "${MODE_IP_DEGRADED}" "EVIDENCE_TRANSFER_FAILED"
  printf '%s sync failed during evidence transfer -> %s\n' "$(ts)" "${REMOTE_TARGET}" >>"${LOGFILE}"
  exit 1
fi

set_mode "${MODE_IP_FULL}" "EVIDENCE_TRANSFER_SUCCEEDED"
sync_timestamp="$(ts)"
atomic_write_line "${SUCCESS_FILE}" \
  "{\"timestamp_utc\":\"${sync_timestamp}\",\"node_id\":\"${NODE_ID}\",\"remote_target\":\"${REMOTE_TARGET}\",\"status\":\"success\"}"
printf '%s sync ok -> %s\n' "${sync_timestamp}" "${REMOTE_TARGET}" >>"${LOGFILE}"
