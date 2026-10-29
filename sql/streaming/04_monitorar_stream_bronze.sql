-- Acompanhamento ao vivo: rode várias vezes durante a simulação e veja o número subir.
SELECT COUNT(*) AS velas_consumidas, MIN(minute) AS primeira, MAX(minute) AS ultima,
       MAX(recebido_em) AS ultima_chegada
FROM `btc.btc_stream_bronze`;
