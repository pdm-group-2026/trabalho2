#!/usr/bin/env bash
# Publica o simulador como Cloud Run Job, com conta de serviço própria.
# Uso: bash simulador/deploy.sh   (a partir da raiz do repositório)
set -euo pipefail
PROJECT="${PROJECT:-pdm-trabalho-508212}"
REGION="${REGION:-us-central1}"
PN="$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')"
SA_SIM="sa-simulador@$PROJECT.iam.gserviceaccount.com"

gcloud iam service-accounts describe "$SA_SIM" >/dev/null 2>&1 || \
  gcloud iam service-accounts create sa-simulador --display-name="Simulador de mercado"
sleep 10  # o IAM leva alguns segundos para enxergar uma conta nova

for ROLE in roles/bigquery.jobUser roles/bigquery.dataViewer roles/pubsub.publisher; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$SA_SIM" \
    --role="$ROLE" --condition=None -q >/dev/null || { echo "ERRO ao conceder $ROLE: só o Proprietário do projeto pode dar permissões" >&2; exit 1; }; echo "ok: $ROLE"
done
# o Cloud Build monta a imagem com a conta padrão do Compute Engine
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:$PN-compute@developer.gserviceaccount.com" \
  --role=roles/cloudbuild.builds.builder --condition=None -q >/dev/null || { echo "ERRO ao conceder cloud build: só o Proprietário do projeto pode dar permissões" >&2; exit 1; }; echo "ok: cloud build"

gcloud run jobs deploy simulador-btc \
  --source simulador \
  --region="$REGION" \
  --service-account="$SA_SIM" \
  --set-env-vars="PROJECT=$PROJECT,TOPIC=btc-klines,VELOCIDADE=60" \
  --task-timeout=60m \
  --max-retries=0
