#!/usr/bin/env bash
# Para cada hora, pede a previsão pela Gold do stream e pela Gold do Trabalho 1: têm que ser iguais.
# Pré-requisitos: endpoint com modelo implantado e o stream já reproduzido (pelo menos 26 horas).
# Uso: bash api/comparar_stream_batch.sh [DIA] [HORAS]   ex.: bash api/comparar_stream_batch.sh 2026-06-26 "08 09 10 11 12 13"
# Resultado obtido (26/06 08h a 13h): previsão idêntica nas 6 horas; o modelo acertou 4 de 6.
set -euo pipefail
REGION="${REGION:-us-central1}"
DIA="${1:-2026-06-26}"
HORAS="${2:-08 09 10 11 12 13}"
URL="$(gcloud run services describe api-volatilidade --region="$REGION" --format='value(status.url)')"
TOKEN="$(gcloud auth print-identity-token)"

prever() {  # $1 = hora ISO, $2 = fonte
  curl -s -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"hour\": \"$1\", \"fonte\": \"$2\"}" "$URL/prever"
}
for H in $HORAS; do
  HORA="${DIA}T${H}:00:00Z"
  S="$(prever "$HORA" stream | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['previsao'], d['real'])")"
  B="$(prever "$HORA" batch  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['previsao'])")"
  echo "$DIA ${H}h  stream: previsão/real = $S   |   batch: previsão = $B"
done
