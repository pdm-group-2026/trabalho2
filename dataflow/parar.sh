#!/usr/bin/env bash
# Cancela todos os jobs ativos do Dataflow (o job em streaming cobra enquanto está ligado).
set -euo pipefail
REGION="${REGION:-us-central1}"
for JOB in $(gcloud dataflow jobs list --region="$REGION" --status=active --format='value(id)'); do
  gcloud dataflow jobs cancel "$JOB" --region="$REGION"
done
gcloud dataflow jobs list --region="$REGION" --status=active
