-- Gold do stream x Gold do Trabalho 1: as features da mesma hora têm que coincidir.
-- Tolerância relativa de 0,01%. Motivo: a Silver do Trabalho 1 desempata trades no mesmo
-- milissegundo sem critério fixo (ORDER BY datetime), e o stream desempata pelo id do trade.
-- Em horas com empate no último trade o fechamento muda na 6a casa decimal, e a volatilidade
-- de 24 h difere ~5e-7 (0,004% do valor). Retornos, volumes e fluxo de ordens: diferença zero
-- ou de arredondamento (1e-16).
-- Resultado obtido (26/06 08h a 13h): 6 horas, todas iguais dentro da tolerância.
SELECT
  s.hour,
  s.trade_count = g.trade_count
    AND ABS(s.return_1h - g.return_1h)               <= 1e-4 * ABS(g.return_1h) + 1e-9
    AND ABS(s.return_4h - g.return_4h)               <= 1e-4 * ABS(g.return_4h) + 1e-9
    AND ABS(s.return_8h - g.return_8h)               <= 1e-4 * ABS(g.return_8h) + 1e-9
    AND ABS(s.volatility_24h - g.volatility_24h)     <= 1e-4 * g.volatility_24h
    AND ABS(s.volume_ratio_24h - g.volume_ratio_24h) <= 1e-4 * g.volume_ratio_24h
    AND ABS(s.order_flow_ratio - g.order_flow_ratio) <= 1e-4 * ABS(g.order_flow_ratio) + 1e-9
    AND ABS(s.candle_range - g.candle_range)         <= 1e-4 * g.candle_range
    AND ABS(s.candle_body - g.candle_body)           <= 1e-4 * ABS(g.candle_body) + 1e-9 AS features_iguais_a_gold,
  ABS(s.volatility_24h - g.volatility_24h) / g.volatility_24h                              AS dif_relativa_volatilidade,
  g.high_volatility_4h                                                                     AS real
FROM `btc.btc_stream_gold` s
JOIN `btc.btc_trades_gold` g USING (hour)
WHERE s.pronta_para_prever
ORDER BY s.hour;
