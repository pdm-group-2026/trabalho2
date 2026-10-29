"""Pipeline de streaming do Trabalho 2: Pub/Sub -> Bronze e Silver do stream no BigQuery.

Cada mensagem do tópico btc-klines é uma vela de 1 minuto (JSON validado pelo schema).
O pipeline tem dois ramos:

  Bronze  -> cada vela de 1 minuto, crua, gravada assim que é consumida
             (prova de que o stream gerado é de fato consumido).
  Silver  -> velas de 1 hora, com as mesmas regras da btc_trades_silver do Trabalho 1.
             As velas são agrupadas pela hora que vem DENTRO da mensagem (campo minute),
             e a vela da hora é emitida assim que chegam os seus 60 minutos. Se a hora tiver
             um buraco nos dados, ela é emitida após `--espera_hora` segundos.
"""
import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms import trigger
from apache_beam.transforms.window import GlobalWindows

CAMPOS_VELA = ["open", "high", "low", "close", "trade_count",
               "volume_btc", "volume_usdt", "taker_buy_volume_usdt", "sum_price_qty"]
SOMAS = ["trade_count", "volume_btc", "volume_usdt", "taker_buy_volume_usdt", "sum_price_qty"]
MINUTOS_POR_HORA = 60


def agora_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def iso_para_bq(texto):
    """'2026-06-27T08:05:00Z' -> '2026-06-27 08:05:00 UTC' (formato aceito pelo BigQuery)."""
    return texto.replace("T", " ").replace("Z", "") + " UTC"


# ---------- Bronze ----------

def mensagem_para_bronze(mensagem):
    vela = json.loads(mensagem.data.decode("utf-8"))
    linha = {"minute": iso_para_bq(vela["minute"])}
    linha.update({campo: vela[campo] for campo in CAMPOS_VELA})
    linha["message_id"] = getattr(mensagem, "message_id", None)
    linha["recebido_em"] = agora_utc()
    return linha


# ---------- Silver ----------

def juntar(a, b):
    """Junta duas velas parciais (ou acumuladores) numa só, respeitando a ordem no tempo."""
    if a is None:
        return b
    if b is None:
        return a
    primeira = a if a["primeiro_minuto"] <= b["primeiro_minuto"] else b
    ultima = a if a["ultimo_minuto"] >= b["ultimo_minuto"] else b
    resultado = {
        "primeiro_minuto": primeira["primeiro_minuto"], "open": primeira["open"],
        "ultimo_minuto": ultima["ultimo_minuto"], "close": ultima["close"],
        "high": max(a["high"], b["high"]), "low": min(a["low"], b["low"]),
        "n_minutos": a["n_minutos"] + b["n_minutos"],
    }
    for campo in SOMAS:
        resultado[campo] = a[campo] + b[campo]
    return resultado


def vela_minuto_para_parcial(vela):
    parcial = {
        "primeiro_minuto": vela["minute"], "open": vela["open"],
        "ultimo_minuto": vela["minute"], "close": vela["close"],
        "high": vela["high"], "low": vela["low"], "n_minutos": 1,
    }
    for campo in SOMAS:
        parcial[campo] = vela[campo]
    return parcial


class MontarVelaHora(beam.CombineFn):
    """Junta as velas de 1 minuto de uma hora na vela de 1 hora."""

    def create_accumulator(self):
        return None

    def add_input(self, acumulador, vela):
        return juntar(acumulador, vela_minuto_para_parcial(vela))

    def merge_accumulators(self, acumuladores):
        resultado = None
        for acumulador in acumuladores:
            resultado = juntar(resultado, acumulador)
        return resultado

    def extract_output(self, acumulador):
        return acumulador


def vela_hora_para_silver(chave_e_vela):
    chave, vela = chave_e_vela          # chave = '2026-06-27T08'
    if vela is None:                    # painel vazio do gatilho: nada a gravar
        return
    compra, total = vela["taker_buy_volume_usdt"], vela["volume_usdt"]
    yield {
        "hour": chave.replace("T", " ") + ":00:00 UTC",
        "open": vela["open"], "high": vela["high"], "low": vela["low"], "close": vela["close"],
        "trade_count": vela["trade_count"],
        "volume_btc": vela["volume_btc"], "volume_usdt": total,
        "taker_buy_volume_usdt": compra,
        "taker_sell_volume_usdt": total - compra,
        "order_flow_imbalance_usdt": 2 * compra - total,
        "vwap": vela["sum_price_qty"] / vela["volume_btc"] if vela["volume_btc"] else None,
        "n_minutos": vela["n_minutos"],
        "processado_em": agora_utc(),
    }


def velas_de_1_hora(velas_1m, espera_hora):
    """Transformação central da Silver, separada da leitura e da escrita para poder ser testada."""
    return (
        velas_1m
        | "Chave = hora da vela" >> beam.Map(lambda vela: (vela["minute"][:13], vela))
        | "Gatilho: 60 minutos ou espera" >> beam.WindowInto(
            GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterAny(
                trigger.AfterCount(MINUTOS_POR_HORA),
                trigger.AfterProcessingTime(espera_hora),
            )),
            accumulation_mode=trigger.AccumulationMode.DISCARDING,
        )
        | "Montar vela da hora" >> beam.CombinePerKey(MontarVelaHora())
        | "Formatar linha Silver" >> beam.FlatMap(vela_hora_para_silver)
    )


def gravar(tabela):
    return beam.io.WriteToBigQuery(
        tabela,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
    )


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--subscription", required=True, help="projects/<projeto>/subscriptions/<assinatura>")
    parser.add_argument("--tabela_bronze", required=True, help="<projeto>:<dataset>.<tabela>")
    parser.add_argument("--tabela_silver", required=True, help="<projeto>:<dataset>.<tabela>")
    parser.add_argument("--espera_hora", type=int, default=120,
                        help="segundos de espera para emitir uma hora incompleta")
    args, beam_args = parser.parse_known_args(argv)

    options = PipelineOptions(beam_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=options)
    mensagens = p | "Ler Pub/Sub" >> beam.io.ReadFromPubSub(subscription=args.subscription, with_attributes=True)

    bronze = mensagens | "Mensagem para Bronze" >> beam.Map(mensagem_para_bronze)
    bronze | "Gravar Bronze" >> gravar(args.tabela_bronze)

    velas_1m = mensagens | "JSON para dict" >> beam.Map(lambda m: json.loads(m.data.decode("utf-8")))
    velas_de_1_hora(velas_1m, args.espera_hora) | "Gravar Silver" >> gravar(args.tabela_silver)

    p.run()  # em streaming no Dataflow, o job segue rodando na nuvem depois deste comando


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
