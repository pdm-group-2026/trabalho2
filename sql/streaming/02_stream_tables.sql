-- Tabelas de destino do Dataflow.
-- Sem partição: são pequenas, e a inserção em streaming não aceita partições de datas antigas.

-- Bronze do stream: cada vela de 1 minuto consumida do Pub/Sub, crua, no momento em que chega.
CREATE TABLE IF NOT EXISTS `btc.btc_stream_bronze` (
  minute TIMESTAMP,
  open FLOAT64, high FLOAT64, low FLOAT64, close FLOAT64,
  trade_count INT64,
  volume_btc FLOAT64, volume_usdt FLOAT64, taker_buy_volume_usdt FLOAT64, sum_price_qty FLOAT64,
  message_id STRING,
  recebido_em TIMESTAMP
);

-- Silver do stream: velas de 1 hora, mesmas colunas da btc_trades_silver + controle de qualidade.
CREATE TABLE IF NOT EXISTS `btc.btc_stream_silver` (
  hour TIMESTAMP,
  open FLOAT64, high FLOAT64, low FLOAT64, close FLOAT64,
  trade_count INT64,
  volume_btc FLOAT64, volume_usdt FLOAT64,
  taker_buy_volume_usdt FLOAT64, taker_sell_volume_usdt FLOAT64,
  order_flow_imbalance_usdt FLOAT64, vwap FLOAT64,
  n_minutos INT64,          -- 60 = hora completa
  processado_em TIMESTAMP
);
