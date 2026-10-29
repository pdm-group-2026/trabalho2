-- Fita do streaming: velas de 1 minuto do período de teste (a partir de 10/04/2026),
-- montadas a partir da Bronze de trades do Trabalho 1. É o que o simulador reproduz no Pub/Sub.
-- O filtro por data aproveita o particionamento da Bronze (lê ~20% da tabela).
-- sum_price_qty permite recompor a VWAP da hora sem erro.
CREATE OR REPLACE TABLE `btc.btc_klines_1m`
PARTITION BY DATE(minute)
AS
SELECT
  TIMESTAMP_TRUNC(datetime, MINUTE) AS minute,
  ARRAY_AGG(price ORDER BY datetime ASC,  id ASC  LIMIT 1)[OFFSET(0)] AS open,
  MAX(price) AS high,
  MIN(price) AS low,
  ARRAY_AGG(price ORDER BY datetime DESC, id DESC LIMIT 1)[OFFSET(0)] AS close,
  COUNT(*)                               AS trade_count,
  SUM(qty)                               AS volume_btc,
  SUM(quoteQty)                          AS volume_usdt,
  SUM(IF(NOT isBuyerMaker, quoteQty, 0)) AS taker_buy_volume_usdt,
  SUM(price * qty)                       AS sum_price_qty
FROM `btc.btc_trades_bronze`
WHERE datetime >= '2026-04-10'
GROUP BY minute;

-- Conferência: agrupadas por hora, as velas de 1 minuto têm que reproduzir a Silver.
-- Resultado obtido: 3.528 horas, 0 divergências.
WITH h AS (
  SELECT TIMESTAMP_TRUNC(minute, HOUR) AS hour,
         SUM(trade_count) AS trade_count, SUM(volume_usdt) AS volume_usdt,
         MAX(high) AS high, MIN(low) AS low
  FROM `btc.btc_klines_1m`
  GROUP BY hour
)
SELECT
  COUNT(*)                                                           AS horas,
  COUNTIF(h.trade_count != s.trade_count)                            AS divergem_trades,
  COUNTIF(ABS(h.volume_usdt - s.volume_usdt) > 1e-9 * s.volume_usdt) AS divergem_volume,
  COUNTIF(h.high != s.high OR h.low != s.low)                        AS divergem_preco
FROM h
JOIN `btc.btc_trades_silver` s USING (hour);
