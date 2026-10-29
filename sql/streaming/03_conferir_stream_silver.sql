-- O stream tem que reproduzir a Silver do Trabalho 1: mesmas velas de 1 hora, montadas em tempo real.
-- QUALIFY: se um teste repetido gravar a mesma hora duas vezes, fica a mais recente.
-- Resultado obtido (27/06 08h-12h a 120x): 5 horas, n_minutos = 60, todas as conferências true.
WITH stream AS (
  SELECT * FROM `btc.btc_stream_silver`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY hour ORDER BY processado_em DESC) = 1
)
SELECT
  s.hour, s.n_minutos, s.processado_em,
  s.trade_count = v.trade_count                                               AS trades_ok,
  s.open = v.open AND s.close = v.close AND s.high = v.high AND s.low = v.low AS ohlc_ok,
  ABS(s.volume_usdt - v.volume_usdt) <= 1e-9 * v.volume_usdt                  AS volume_ok,
  ABS(s.vwap - v.vwap) <= 1e-9 * v.vwap                                       AS vwap_ok
FROM stream s
JOIN `btc.btc_trades_silver` v USING (hour)
ORDER BY s.hour;
