#!/usr/bin/env bash
# Testa a API publicada. O endpoint do Vertex AI precisa estar com o modelo implantado.
# Uso: bash api/testar_api.sh
set -euo pipefail
REGION="${REGION:-us-central1}"
URL="$(gcloud run services describe api-volatilidade --region="$REGION" --format='value(status.url)')"
echo "API: $URL"
# A API é privada: cada chamada leva um token de identidade da conta logada no gcloud.
TOKEN="$(gcloud auth print-identity-token)"
AUTH="Authorization: Bearer $TOKEN"

echo; echo "1) Saúde"
curl -s -H "$AUTH" "$URL/health"; echo

echo; echo "2) Batch: hora calma (01/08/2026 00h, a mesma do vertex/testar_endpoint.sh)"
curl -s -H "$AUTH" -X POST "$URL/prever" -H "Content-Type: application/json" \
  -d '{"hour": "2026-08-01T00:00:00Z", "fonte": "batch"}'; echo

echo; echo "3) Batch: hora turbulenta (26/06/2026 12h)"
curl -s -H "$AUTH" -X POST "$URL/prever" -H "Content-Type: application/json" \
  -d '{"hour": "2026-06-26T12:00:00Z", "fonte": "batch"}'; echo

echo; echo "4) Stream: hora mais recente pronta na btc_stream_gold"
curl -s -H "$AUTH" -X POST "$URL/prever" -H "Content-Type: application/json" -d '{"fonte": "stream"}'; echo

echo; echo "5) Entrada inválida (feature faltando) -> deve dar 422"
curl -s -H "$AUTH" -o /dev/null -w "HTTP %{http_code}\n" -X POST "$URL/prever/features" \
  -H "Content-Type: application/json" -d '{"features": {"return_1h": 0.001}}'
