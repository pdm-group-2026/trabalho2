"""Simulador de mercado.

Lê velas de 1 minuto da tabela btc.btc_klines_1m e publica cada uma no tópico
btc-klines do Pub/Sub, respeitando o intervalo entre elas dividido pela velocidade.
Com velocidade 60, um minuto de mercado vira um segundo real (uma hora por minuto).

O horário original da vela vai no atributo `ts`: é ele que o Dataflow usa como
tempo do evento para montar as janelas de 1 hora.

Uso:
    python simulador.py --inicio 2026-06-26T00:00:00Z --fim 2026-06-27T00:00:00Z --velocidade 60
Os mesmos parâmetros podem vir das variáveis de ambiente INICIO, FIM e VELOCIDADE
(é assim que o Cloud Run Job recebe os valores).
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone

from google.cloud import bigquery, pubsub_v1

PROJECT = os.environ.get("PROJECT", "pdm-trabalho-508212")
TOPIC = os.environ.get("TOPIC", "btc-klines")
TABELA = f"{PROJECT}.btc.btc_klines_1m"
CAMPOS = [
    "open", "high", "low", "close", "trade_count",
    "volume_btc", "volume_usdt", "taker_buy_volume_usdt", "sum_price_qty",
]


def ler_argumentos():
    p = argparse.ArgumentParser(description="Reproduz velas de 1 minuto no Pub/Sub.")
    p.add_argument("--inicio", default=os.environ.get("INICIO", "2026-06-26T00:00:00Z"))
    p.add_argument("--fim", default=os.environ.get("FIM", "2026-06-27T00:00:00Z"))
    p.add_argument("--velocidade", type=float, default=float(os.environ.get("VELOCIDADE", "60")))
    return p.parse_args()


def para_datetime(texto):
    return datetime.fromisoformat(texto.replace("Z", "+00:00"))


def carregar_velas(inicio, fim):
    sql = f"""
        SELECT minute, {", ".join(CAMPOS)}
        FROM `{TABELA}`
        WHERE minute >= @inicio AND minute < @fim
        ORDER BY minute
    """
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("inicio", "TIMESTAMP", para_datetime(inicio)),
        bigquery.ScalarQueryParameter("fim", "TIMESTAMP", para_datetime(fim)),
    ])
    return list(bigquery.Client(project=PROJECT).query(sql, job_config=config).result())


def main():
    args = ler_argumentos()
    velas = carregar_velas(args.inicio, args.fim)
    if not velas:
        raise SystemExit(f"Nenhuma vela entre {args.inicio} e {args.fim}.")
    duracao = len(velas) * 60 / args.velocidade
    print(f"{len(velas)} velas de {args.inicio} a {args.fim} | velocidade {args.velocidade:g}x "
          f"| duração estimada {duracao / 60:.1f} min", flush=True)

    publisher = pubsub_v1.PublisherClient()
    topico = publisher.topic_path(PROJECT, TOPIC)

    inicio_real = time.monotonic()
    inicio_evento = velas[0]["minute"]
    pendentes = []
    for i, vela in enumerate(velas):
        # espera o instante proporcional ao tempo de mercado decorrido
        alvo = (vela["minute"] - inicio_evento).total_seconds() / args.velocidade
        espera = alvo - (time.monotonic() - inicio_real)
        if espera > 0:
            time.sleep(espera)

        ts = vela["minute"].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        mensagem = {"minute": ts, **{c: vela[c] for c in CAMPOS}}
        mensagem["trade_count"] = int(mensagem["trade_count"])
        pendentes.append(publisher.publish(topico, json.dumps(mensagem).encode("utf-8"), ts=ts))

        if i % 60 == 0:
            print(f"publicando {ts} ({i + 1}/{len(velas)})", flush=True)

    for futuro in pendentes:
        futuro.result()  # levanta erro se alguma publicação falhou
    print(f"fim: {len(velas)} velas publicadas em {time.monotonic() - inicio_real:.0f} s", flush=True)


if __name__ == "__main__":
    main()
