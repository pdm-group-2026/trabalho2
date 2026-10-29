#!/usr/bin/env bash
# Dispara o simulador na nuvem.
# Uso: bash simulador/executar.sh [INICIO] [FIM] [VELOCIDADE]
#   ex.: bash simulador/executar.sh 2026-06-27T08:00:00Z 2026-06-27T13:00:00Z 120
set -euo pipefail
REGION="${REGION:-us-central1}"
INICIO="${1:-2026-06-26T00:00:00Z}"
FIM="${2:-2026-06-27T00:00:00Z}"
VELOCIDADE="${3:-60}"
gcloud run jobs execute simulador-btc --region="$REGION" --async \
  --update-env-vars="INICIO=$INICIO,FIM=$FIM,VELOCIDADE=$VELOCIDADE"
