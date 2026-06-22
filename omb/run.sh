#!/usr/bin/env bash
###############################################################################
# omb/run.sh — Lance un run OMB depuis l'hôte via docker exec
#
# Usage :
#   ./omb/run.sh <workload-file> <driver-name> [extra-omb-args...]
#
# Exemples :
#   ./omb/run.sh workloads/A-realtime-transactional.yaml pulsar
#   ./omb/run.sh workloads/smoke.yaml rabbitmq
#   ./omb/run.sh workloads/smoke.yaml artemis
#
# Le JSON de résultat est déposé dans results/<driver>-<workload>-<ts>.json
###############################################################################
set -euo pipefail

WORKLOAD="${1:?Usage: $0 <workload.yaml> <driver> [extra-args...]}"
DRIVER="${2:?Usage: $0 <workload.yaml> <driver> [extra-args...]}"
shift 2

CONTAINER="omb-runner"
TS=$(date +%Y%m%dT%H%M%S)
WORKLOAD_BASE=$(basename "${WORKLOAD}" .yaml)
RESULT_FILE="/omb/results/${DRIVER}-${WORKLOAD_BASE}-${TS}.json"

echo "==> Run  : workload=${WORKLOAD_BASE}  driver=${DRIVER}  ts=${TS}"
echo "==> Output: results/${DRIVER}-${WORKLOAD_BASE}-${TS}.json"

# Verify the omb-runner container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "ERROR: container '${CONTAINER}' is not running."
  echo "Start it first: docker compose --profile bench up -d"
  exit 1
fi

# Execute OMB inside the container.
# Driver file path inside container: /omb/drivers/<driver>.yaml (mounted from host)
docker exec "${CONTAINER}" \
  /omb/bin/benchmark \
    --drivers "/omb/drivers/${DRIVER}.yaml" \
    --workloads "/omb/workloads/${WORKLOAD_BASE}.yaml" \
    --output "${RESULT_FILE}" \
    "$@"

echo "==> Done. Result: results/${DRIVER}-${WORKLOAD_BASE}-${TS}.json"
