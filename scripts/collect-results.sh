#!/usr/bin/env bash
###############################################################################
# scripts/collect-results.sh — Résumé des résultats OMB dans results/
#
# Lit tous les JSON de résultats et affiche un tableau de synthèse
# (publish rate, publish latency p50/p99/p99.9, E2E latency).
#
# Usage :
#   ./scripts/collect-results.sh
#   ./scripts/collect-results.sh results/pulsar-*.json
###############################################################################
set -euo pipefail

FILES="${*:-results/*.json}"

if ! command -v jq &>/dev/null; then
    echo "ERROR: jq requis. Installer: apt-get install jq  ou  brew install jq"
    exit 1
fi

printf "%-40s %12s %10s %10s %10s %10s\n" \
    "Fichier" "Rate(msg/s)" "PubP50(ms)" "PubP99(ms)" "P99.9(ms)" "E2EP99(ms)"
echo "$(printf '%0.s-' {1..105})"

for f in ${FILES}; do
    [ -f "${f}" ] || continue
    NAME=$(basename "${f}" .json)
    RATE=$(jq -r '.publishRate // "N/A"' "${f}" 2>/dev/null)
    PUB_P50=$(jq -r '.publishLatency.quantiles["0.5"] // "N/A"' "${f}" 2>/dev/null)
    PUB_P99=$(jq -r '.publishLatency.quantiles["0.99"] // "N/A"' "${f}" 2>/dev/null)
    PUB_P999=$(jq -r '.publishLatency.quantiles["0.999"] // "N/A"' "${f}" 2>/dev/null)
    E2E_P99=$(jq -r '.endToEndLatency.quantiles["0.99"] // "N/A"' "${f}" 2>/dev/null)
    printf "%-40s %12s %10s %10s %10s %10s\n" \
        "${NAME:0:40}" "${RATE}" "${PUB_P50}" "${PUB_P99}" "${PUB_P999}" "${E2E_P99}"
done
