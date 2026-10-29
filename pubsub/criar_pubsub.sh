#!/usr/bin/env bash
# Cria o schema, o tópico e as assinaturas do streaming.
# Uso: bash pubsub/criar_pubsub.sh   (a partir da raiz do repositório)
set -euo pipefail
PROJECT="${PROJECT:-pdm-trabalho-508212}"
gcloud config set project "$PROJECT"

# schema Avro: o tópico recusa qualquer mensagem fora do formato de uma vela de 1 minuto
gcloud pubsub schemas create btc-kline-1m --type=avro --definition-file=pubsub/kline_schema.json
gcloud pubsub topics create btc-klines --schema=btc-kline-1m --message-encoding=json

# assinatura lida pelo Dataflow (sem dead-letter: o Dataflow não é compatível com essa política)
gcloud pubsub subscriptions create btc-klines-dataflow --topic=btc-klines

# assinatura de debug, para inspecionar as mensagens durante a demo
gcloud pubsub subscriptions create btc-klines-debug --topic=btc-klines --message-retention-duration=1h
