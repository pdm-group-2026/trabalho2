#!/usr/bin/env bash
# Registra o model.joblib no Model Registry e publica no endpoint (leva de 10 a 20 min).
# Pré-requisito: notebooks/treino_vertex.ipynb rodado, gerando gs://<projeto>-modelos/rf-vol-4h/v1/model.joblib
set -euo pipefail
PROJECT="${PROJECT:-pdm-trabalho-508212}"
REGION="${REGION:-us-central1}"
MODELO=rf-volatilidade-4h
ENDPOINT=rf-volatilidade-4h-endpoint

MODEL_ID="$(gcloud ai models list --region="$REGION" --filter="display_name=$MODELO" --format='value(name.basename())' | head -1)"
if [ -z "$MODEL_ID" ]; then
  gcloud ai models upload --region="$REGION" --display-name="$MODELO" \
    --artifact-uri="gs://$PROJECT-modelos/rf-vol-4h/v1/" \
    --container-image-uri=us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-6:latest \
    --version-aliases=campeao
  MODEL_ID="$(gcloud ai models list --region="$REGION" --filter="display_name=$MODELO" --format='value(name.basename())' | head -1)"
fi

ENDPOINT_ID="$(gcloud ai endpoints list --region="$REGION" --filter="display_name=$ENDPOINT" --format='value(name.basename())' | head -1)"
if [ -z "$ENDPOINT_ID" ]; then
  gcloud ai endpoints create --region="$REGION" --display-name="$ENDPOINT"
  ENDPOINT_ID="$(gcloud ai endpoints list --region="$REGION" --filter="display_name=$ENDPOINT" --format='value(name.basename())' | head -1)"
fi

gcloud ai endpoints deploy-model "$ENDPOINT_ID" --region="$REGION" --model="$MODEL_ID" \
  --display-name=rf-vol-4h-v1 --machine-type=n1-standard-2 \
  --min-replica-count=1 --max-replica-count=1 --traffic-split=0=100
echo "modelo $MODEL_ID publicado no endpoint $ENDPOINT_ID"
