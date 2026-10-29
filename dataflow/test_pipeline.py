"""Testes da lógica do pipeline, sem nuvem: python dataflow/test_pipeline.py

Simula um stream com duas horas: uma completa (60 velas, chegando fora de ordem)
e uma com um minuto faltando (59 velas), e confere a vela de 1 hora de cada uma.
"""
import json
import math
import os
import random
import sys
from datetime import datetime, timedelta, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pipeline import mensagem_para_bronze, velas_de_1_hora  # noqa: E402


def vela(minuto, i):
    preco = 100.0 + i
    return {"minute": minuto.strftime("%Y-%m-%dT%H:%M:%SZ"), "open": preco, "high": preco + 0.5,
            "low": preco - 0.5, "close": preco + 0.25, "trade_count": 10, "volume_btc": 1.0,
            "volume_usdt": preco, "taker_buy_volume_usdt": preco * 0.6, "sum_price_qty": preco}


BASE = datetime(2026, 6, 27, 8, 0, tzinfo=timezone.utc)
HORA_CHEIA = [vela(BASE + timedelta(minutes=i), i) for i in range(60)]
HORA_COM_BURACO = [vela(BASE + timedelta(hours=1, minutes=i), 60 + i) for i in range(60) if i != 30]
random.seed(2)
random.shuffle(HORA_CHEIA)


def conferir_silver(saida):
    por_hora = {linha["hour"]: linha for linha in saida}
    assert len(saida) == 2, f"esperava 2 linhas, veio: {[(l['hour'], l['n_minutos']) for l in saida]}"
    h8 = por_hora["2026-06-27 08:00:00 UTC"]
    volume = sum(100.0 + i for i in range(60))
    assert h8["n_minutos"] == 60
    assert h8["open"] == 100.0 and h8["close"] == 159.25, "abertura/fechamento fora de ordem"
    assert h8["high"] == 159.5 and h8["low"] == 99.5 and h8["trade_count"] == 600
    assert math.isclose(h8["vwap"], volume / 60)
    assert math.isclose(h8["order_flow_imbalance_usdt"], 0.2 * volume)
    assert por_hora["2026-06-27 09:00:00 UTC"]["n_minutos"] == 59


def testar_silver():
    stream = (TestStream()
              .add_elements(HORA_CHEIA[:30]).advance_processing_time(5)
              .add_elements(HORA_CHEIA[30:] + HORA_COM_BURACO[:10]).advance_processing_time(5)
              .add_elements(HORA_COM_BURACO[10:]).advance_processing_time(500)
              .advance_watermark_to_infinity())
    opcoes = PipelineOptions()
    opcoes.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=opcoes) as p:
        assert_that(velas_de_1_hora(p | stream, espera_hora=120), conferir_silver)
    print("ok: silver")


def testar_bronze():
    class Mensagem:  # imita o PubsubMessage
        data = json.dumps(HORA_COM_BURACO[0]).encode()
        message_id = "123"
    linha = mensagem_para_bronze(Mensagem())
    assert linha["minute"] == "2026-06-27 09:00:00 UTC"
    assert linha["message_id"] == "123" and linha["trade_count"] == 10
    print("ok: bronze")


if __name__ == "__main__":
    testar_bronze()
    testar_silver()
