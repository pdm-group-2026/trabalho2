#!/usr/bin/env bash
# Publica a API como serviço do Cloud Run, com conta de serviço própria.
# Uso: bash api/deploy.sh (a partir da raiz do repositório)
# Pré-requisitos: sql/streaming/05_stream_gold.sql rodado e endpoint do Vertex AI criado (vertex/registrar_e_publicar.sh).
set -euo pipefail
PROJECT="${PROJECT:-pdm-trabalho-508212}"
REGION="${REGION:-us-central1}"
SERVICO=api-volatilidade
SA_API="sa-api@$PROJECT.iam.gserviceaccount.com"

# Conta de serviço só com o necessário (menor privilégio):
#   bigquery.jobUser    -> rodar consultas
#   bigquery.dataViewer -> ler as tabelas e views do dataset
#   aiplatform.user     -> chamar o endpoint do Vertex AI
gcloud iam service-accounts describe "$SA_API" >/dev/null 2>&1 || \
  gcloud iam service-accounts create sa-api --display-name="API de predição (Cloud Run)"
sleep 10 # o IAM leva alguns segundos para enxergar uma conta nova

for ROLE in roles/bigquery.jobUser roles/bigquery.dataViewer roles/aiplatform.user; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$SA_API" \
    --role="$ROLE" --condition=None -q >/dev/null && echo "ok: $ROLE"
done

ENDPOINT_ID="$(gcloud ai endpoints list --region="$REGION" \
  --filter="display_name=rf-volatilidade-4h-endpoint" --format='value(name.basename())' | head -1)"
echo "endpoint do Vertex AI: $ENDPOINT_ID"

# --source: o Cloud Build monta a imagem a partir do Dockerfile da pasta api/ e guarda no Artifact Registry.
# --no-allow-unauthenticated: só quem tem permissão no projeto chama a API (com token do Google).
#   Para abrir a URL ao público (ex.: link para o professor), um membro do grupo roda:
#   gcloud run services add-iam-policy-binding api-volatilidade --region=us-central1 \
#     --member=allUsers --role=roles/run.invoker
# --max-instances=2: teto de custo; --min-instances=0: sem tráfego, não cobra nada.
gcloud run deploy "$SERVICO" \
  --source api \
  --region="$REGION" \
  --service-account="$SA_API" \
  --set-env-vars="PROJECT=$PROJECT,REGION=$REGION,ENDPOINT_ID=$ENDPOINT_ID" \
  --no-allow-unauthenticated \
  --memory=512Mi --min-instances=0 --max-instances=2

echo "URL: $(gcloud run services describe "$SERVICO" --region="$REGION" --format='value(status.url)')"
