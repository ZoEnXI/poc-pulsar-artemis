#!/usr/bin/env bash
###############################################################################
# scripts/capture-stats.sh — Échantillonne docker stats pendant un run OMB
#
# Usage :
#   ./scripts/capture-stats.sh <tag> [interval_seconds]
#
# Exemples :
#   ./scripts/capture-stats.sh A-pulsar 5     # capture toutes les 5 s
#   ./scripts/capture-stats.sh B-rabbitmq
#
# Sortie : results/stats-<tag>-<timestamp>.log
# Format : timestamp,name,cpu%,mem_usage,mem_limit,mem%,net_io,block_io
###############################################################################
set -euo pipefail

TAG="${1:?Usage: $0 <tag> [interval_seconds]}"
INTERVAL="${2:-5}"
TS=$(date +%Y%m%dT%H%M%S)
OUTFILE="results/stats-${TAG}-${TS}.log"

echo "==> Capturing docker stats → ${OUTFILE} (Ctrl-C pour arrêter)"
echo "timestamp,name,cpu_pct,mem_usage,mem_limit,mem_pct,net_io,block_io" > "${OUTFILE}"

while true; do
    NOW=$(date +%Y-%m-%dT%H:%M:%S)
    docker stats --no-stream --format \
        "${NOW},{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}}" \
        pulsar rabbitmq artemis 2>/dev/null >> "${OUTFILE}" || true
    sleep "${INTERVAL}"
done
