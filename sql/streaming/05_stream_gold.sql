-- Gold do stream (view): as 13 features do modelo sobre a btc_stream_silver, com as mesmas regras da btc_trades_gold.
-- pronta_para_prever = hora com 60 minutos e 25 horas seguidas de histórico no stream.
CREATE OR REPLACE VIEW `btc.btc_stream_gold` AS
WITH silver AS (
  -- Se o mesmo período for reproduzido duas vezes, fica a versão mais recente de cada hora.
  SELECT * FROM `btc.btc_stream_silver`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY hour ORDER BY processado_em DESC) = 1
),
f1 AS (
  SELECT
    hour, n_minutos, close, trade_count,
    -- Retornos logarítmicos: ln(preço agora / preço de N horas atrás).
    LN(close / LAG(close, 1) OVER w) AS return_1h,
    LN(close / LAG(close, 4) OVER w) AS return_4h,
    LN(close / LAG(close, 8) OVER w) AS return_8h,
    -- Volume da hora dividido pela média das últimas 24 horas (incluindo a atual). > 1 = hora mais movimentada que o normal.
    SAFE_DIVIDE(volume_usdt, AVG(volume_usdt) OVER (w ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)) AS volume_ratio_24h,
    -- Fluxo de ordens: (compras agressoras - vendas agressoras) / volume. Vai de -1 a 1.
    SAFE_DIVIDE(order_flow_imbalance_usdt, volume_usdt) AS order_flow_ratio,
    -- Formato da vela: amplitude relativa ao fechamento e corpo relativo à abertura (igual ao Trabalho 1).
    SAFE_DIVIDE(high - low, close) AS candle_range,
    SAFE_DIVIDE(close - open, open) AS candle_body,
    -- Calendário (UTC). DAYOFWEEK: domingo = 1 ... sábado = 7.
    EXTRACT(HOUR FROM hour) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM hour) AS day_of_week,
    -- Hora em seno e cosseno, para o modelo entender que 23h e 0h são vizinhas.
    SIN(2 * ACOS(-1) * EXTRACT(HOUR FROM hour) / 24) AS hour_sin,
    COS(2 * ACOS(-1) * EXTRACT(HOUR FROM hour) / 24) AS hour_cos,
    -- Janela completa: as 24 linhas anteriores existem e são exatamente as 24 horas anteriores (sem buraco).
    -- São 25 horas seguidas porque a volatilidade usa 24 retornos e cada retorno precisa da hora anterior.
    IFNULL(TIMESTAMP_DIFF(hour, LAG(hour, 24) OVER w, HOUR) = 24, FALSE) AS janela_ok
  FROM silver
  WINDOW w AS (ORDER BY hour)
)
SELECT
  * EXCEPT (janela_ok),
  -- Volatilidade: desvio-padrão (amostral) dos retornos de 1 hora nas últimas 24 horas.
  STDDEV_SAMP(return_1h) OVER (ORDER BY hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS volatility_24h,
  -- Só dá para prever com confiança se a hora atual está completa (60 velas de 1 min) e a janela não tem buraco.
  janela_ok AND n_minutos = 60 AS pronta_para_prever
FROM f1;
