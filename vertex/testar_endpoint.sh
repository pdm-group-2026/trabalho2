#!/usr/bin/env bash
# Manda 4 horas reais do período de teste ao endpoint, na ordem fixa das 13 features.
# Uso: bash vertex/testar_endpoint.sh ["2026-08-01"]
set -euo pipefail
REGION="${REGION:-us-central1}"
DESDE="${1:-2026-08-01}"
ENDPOINT_ID="$(gcloud ai endpoints list --region="$REGION" --filter="display_name=rf-volatilidade-4h-endpoint" --format='value(name.basename())' | head -1)"

bq query --use_legacy_sql=false --format=json "
SELECT hour, high_volatility_4h AS real,
  return_1h, return_4h, return_8h, volatility_24h, volume_ratio_24h, trade_count,
  order_flow_ratio, candle_range, candle_body, hour_of_day, day_of_week, hour_sin, hour_cos
FROM btc.btc_trades_gold
WHERE hour >= '$DESDE' ORDER BY hour LIMIT 4" > /tmp/linhas.json

python3 - <<'PY'
import json
F = ["return_1h", "return_4h", "return_8h", "volatility_24h", "volume_ratio_24h", "trade_count",
     "order_flow_ratio", "candle_range", "candle_body", "hour_of_day", "day_of_week", "hour_sin", "hour_cos"]
linhas = json.load(open("/tmp/linhas.json"))
json.dump({"instances": [[float(r[f]) for f in F] for r in linhas]}, open("/tmp/request.json", "w"))
for r in linhas:
    print(r["hour"], "-> real:", r["real"])
PY
gcloud ai endpoints predict "$ENDPOINT_ID" --region="$REGION" --json-request=/tmp/request.json
