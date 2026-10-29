#!/usr/bin/env bash
# Tira o modelo do endpoint (o endpoint com modelo implantado cobra 24 h por dia).
set -euo pipefail
REGION="${REGION:-us-central1}"
ENDPOINT_ID="$(gcloud ai endpoints list --region="$REGION" --filter="display_name=rf-volatilidade-4h-endpoint" --format='value(name.basename())' | head -1)"
for DEPLOYED in $(gcloud ai endpoints describe "$ENDPOINT_ID" --region="$REGION" --format='value(deployedModels[].id)' | tr ';' ' '); do
  gcloud ai endpoints undeploy-model "$ENDPOINT_ID" --region="$REGION" --deployed-model-id="$DEPLOYED"
done
echo "modelos implantados agora: $(gcloud ai endpoints describe "$ENDPOINT_ID" --region="$REGION" --format='value(deployedModels)')"
