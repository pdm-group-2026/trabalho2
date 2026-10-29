#!/usr/bin/env bash
# Cria a conta de serviço do Dataflow (se preciso) e lança o job de streaming.
# Uso: bash dataflow/lancar.sh   (a partir da raiz, com o venv ativo e o requirements instalado)
set -euo pipefail
PROJECT="${PROJECT:-pdm-trabalho-508212}"
REGION="${REGION:-us-central1}"
SA_DF="sa-dataflow@$PROJECT.iam.gserviceaccount.com"

gcloud storage buckets describe "gs://$PROJECT-dataflow" >/dev/null 2>&1 || \
  gcloud storage buckets create "gs://$PROJECT-dataflow" --location="$REGION"

if ! gcloud iam service-accounts describe "$SA_DF" >/dev/null 2>&1; then
  gcloud iam service-accounts create sa-dataflow --display-name="Pipeline Dataflow"
  sleep 10  # o IAM leva alguns segundos para enxergar uma conta nova
fi
for ROLE in roles/dataflow.worker roles/pubsub.subscriber roles/pubsub.viewer \
            roles/bigquery.dataEditor roles/storage.objectAdmin; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$SA_DF" \
    --role="$ROLE" --condition=None -q >/dev/null && echo "ok: $ROLE"
done

# descarta mensagens antigas antes de começar
gcloud pubsub subscriptions seek btc-klines-dataflow --time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

python dataflow/pipeline.py \
  --runner=DataflowRunner \
  --project="$PROJECT" --region="$REGION" \
  --job_name=velas-btc \
  --temp_location="gs://$PROJECT-dataflow/temp" \
  --staging_location="gs://$PROJECT-dataflow/staging" \
  --service_account_email="$SA_DF" \
  --machine_type=e2-standard-2 --max_num_workers=1 \
  --subscription="projects/$PROJECT/subscriptions/btc-klines-dataflow" \
  --tabela_bronze="$PROJECT:btc.btc_stream_bronze" \
  --tabela_silver="$PROJECT:btc.btc_stream_silver"
