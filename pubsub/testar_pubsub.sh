#!/usr/bin/env bash
# Publica uma mensagem válida e uma inválida e lê a assinatura de debug.
set -uo pipefail
gcloud pubsub topics publish btc-klines --attribute=ts=2026-06-26T12:00:00Z \
  --message='{"minute":"2026-06-26T12:00:00Z","open":100.0,"high":101.0,"low":99.5,"close":100.5,"trade_count":10,"volume_btc":1.5,"volume_usdt":150.0,"taker_buy_volume_usdt":80.0,"sum_price_qty":150.0}'

echo "--- a mensagem abaixo deve ser RECUSADA pelo schema ---"
gcloud pubsub topics publish btc-klines --message='{"minute":"2026-06-26T12:01:00Z"}'

gcloud pubsub subscriptions pull btc-klines-debug --auto-ack --limit=5

# descarta a mensagem de teste na assinatura do Dataflow
gcloud pubsub subscriptions seek btc-klines-dataflow --time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
