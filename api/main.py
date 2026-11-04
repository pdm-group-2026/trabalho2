"""API REST de predição de volatilidade do BTC (Trabalho 2).

Fluxo de uma predição:
    cliente --POST /prever--> esta API (Cloud Run)
        1. lê as 13 features da hora pedida na Gold do BigQuery
           (btc_stream_gold = dados que chegaram pelo stream; btc_trades_gold = batch do Trabalho 1)
        2. manda as features, na ordem fixa do treino, para o endpoint do Vertex AI
        3. devolve 0 ou 1 em JSON (1 = variação >= 0,5% nas próximas 4 horas)

A API não carrega o modelo: quem guarda e executa o Random Forest é o Vertex AI.
Assim a API fica leve (sobe rápido no Cloud Run) e trocar o modelo não exige novo deploy da API.
"""
import math
import os
import time
from datetime import datetime
from typing import Literal, Optional

import google.auth
import google.auth.transport.requests
import requests
from fastapi import FastAPI, HTTPException
from google.cloud import bigquery
from pydantic import BaseModel, Field

PROJECT = os.environ.get("PROJECT", "pdm-trabalho-508212")
REGION = os.environ.get("REGION", "us-central1")
DATASET = os.environ.get("DATASET", "btc")
ENDPOINT_ID = os.environ.get("ENDPOINT_ID", "")  # preenchido pelo deploy.sh

# Ordem EXATA das colunas usadas no treino (contrato do endpoint, ver README).
# O scikit-learn recebe uma lista de números sem nomes: trocar a ordem dá predição errada sem erro nenhum.
FEATURES = [
    "return_1h", "return_4h", "return_8h", "volatility_24h", "volume_ratio_24h", "trade_count",
    "order_flow_ratio", "candle_range", "candle_body", "hour_of_day", "day_of_week", "hour_sin", "hour_cos",
]

# De onde vêm as features de cada fonte.
TABELAS = {
    "stream": f"{PROJECT}.{DATASET}.btc_stream_gold",  # view sobre o que o Dataflow gravou (Trabalho 2)
    "batch": f"{PROJECT}.{DATASET}.btc_trades_gold",   # tabela do Trabalho 1 (a mesma do treino)
}

app = FastAPI(
    title="API de volatilidade do BTC",
    description="Prevê se o BTC vai variar 0,5% ou mais nas próximas 4 horas (modelo rf-volatilidade-4h no Vertex AI).",
    version="1.0",
)

# Clientes criados uma vez só, quando o container sobe, e reaproveitados entre requisições.
# No Cloud Run as credenciais vêm da conta de serviço da API (sa-api), sem chave em arquivo.
bq = bigquery.Client(project=PROJECT)
_credenciais, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])


# ---------- formatos de entrada ----------

class PedidoPrevisao(BaseModel):
    hour: Optional[datetime] = Field(
        None,
        description="Hora (UTC) da vela de 1 hora já fechada. A previsão vale para as 4 horas seguintes. "
                    "Se vier vazio, usa a hora mais recente pronta na fonte escolhida.",
        examples=["2026-06-27T12:00:00Z"],
    )
    fonte: Literal["stream", "batch"] = Field(
        "stream", description="stream = velas que chegaram pelo Pub/Sub e Dataflow; batch = Gold do Trabalho 1.")


class PedidoFeatures(BaseModel):
    features: dict[str, float] = Field(..., description="As 13 features pelo nome. A API põe na ordem certa.")


# ---------- peças internas ----------

def chamar_vertex(linhas: list[list[float]]) -> list[int]:
    """Chama o endpoint do Vertex AI pela API REST e devolve uma classe (0 ou 1) por linha."""
    if not ENDPOINT_ID:
        raise HTTPException(500, "ENDPOINT_ID não configurado na API.")
    if not _credenciais.valid:
        _credenciais.refresh(google.auth.transport.requests.Request())  # token OAuth dura ~1 h
    url = (f"https://{REGION}-aiplatform.googleapis.com/v1/projects/{PROJECT}"
           f"/locations/{REGION}/endpoints/{ENDPOINT_ID}:predict")
    r = requests.post(url, json={"instances": linhas},
                      headers={"Authorization": f"Bearer {_credenciais.token}"}, timeout=30)
    if r.status_code != 200:
        # O caso mais comum é o endpoint sem modelo implantado (desligado para não gerar custo).
        raise HTTPException(503, f"Vertex AI respondeu {r.status_code}: {r.text[:300]}")
    return [int(p) for p in r.json()["predictions"]]


def validar(valores: dict) -> list[float]:
    """Confere que as 13 features existem e são números finitos, e devolve a lista na ordem do treino."""
    faltando = [f for f in FEATURES if valores.get(f) is None]
    if faltando:
        raise HTTPException(422, f"Features ausentes ou nulas: {faltando}")
    linha = [float(valores[f]) for f in FEATURES]
    if not all(math.isfinite(v) for v in linha):
        raise HTTPException(422, "Há feature infinita ou NaN.")
    return linha


def buscar_features(fonte: str, hour: Optional[datetime]) -> dict:
    """Lê do BigQuery a linha de features da hora pedida (ou a mais recente pronta)."""
    tabela = TABELAS[fonte]
    # Na stream a view diz se a janela de 25 horas está completa; no batch toda hora com volatilidade calculada serve.
    pronta = "pronta_para_prever" if fonte == "stream" else "volatility_24h IS NOT NULL AND return_8h IS NOT NULL"
    colunas = ", ".join(FEATURES)
    if hour is None:
        sql = (f"SELECT hour, {pronta} AS pronta, {colunas} FROM `{tabela}` "
               f"WHERE {pronta} ORDER BY hour DESC LIMIT 1")
        params = []
    else:
        # Parâmetro (@hour) em vez de colar o texto no SQL: evita SQL injection.
        sql = f"SELECT hour, {pronta} AS pronta, {colunas} FROM `{tabela}` WHERE hour = @hour"
        params = [bigquery.ScalarQueryParameter("hour", "TIMESTAMP", hour)]
    linhas = list(bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())
    if not linhas:
        raise HTTPException(404, f"Nenhuma hora pronta encontrada em {fonte}"
                                 + (f" para {hour.isoformat()}" if hour else ""))
    linha = dict(linhas[0])
    if not linha["pronta"]:
        raise HTTPException(422, f"A hora {linha['hour'].isoformat()} existe em {fonte}, mas a janela de 25 horas "
                                 "seguidas ainda não está completa (ou a hora não fechou os 60 minutos).")
    return linha


def resultado_real(hour: datetime) -> Optional[int]:
    """O que de fato aconteceu, segundo a Gold do Trabalho 1. Serve só para conferir a previsão na demo."""
    sql = f"SELECT high_volatility_4h FROM `{TABELAS['batch']}` WHERE hour = @hour"
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("hour", "TIMESTAMP", hour)])
    linhas = list(bq.query(sql, job_config=cfg).result())
    return linhas[0]["high_volatility_4h"] if linhas else None


def significado(classe: int) -> str:
    if classe == 1:
        return "alta volatilidade: variação de 0,5% ou mais esperada nas próximas 4 horas"
    return "baixa volatilidade: variação abaixo de 0,5% esperada nas próximas 4 horas"


# ---------- rotas ----------

@app.get("/")
def info():
    return {
        "servico": "API de volatilidade do BTC",
        "modelo": "rf-volatilidade-4h (Random Forest, scikit-learn, Vertex AI)",
        "endpoint_vertex": ENDPOINT_ID,
        "rotas": {
            "POST /prever": "lê as features da hora no BigQuery e prevê",
            "POST /prever/features": "prevê a partir das 13 features enviadas no corpo",
            "GET /docs": "documentação interativa (Swagger)",
        },
        "ordem_das_features": FEATURES,
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/prever")
def prever(pedido: PedidoPrevisao):
    inicio = time.time()
    linha = buscar_features(pedido.fonte, pedido.hour)
    classe = chamar_vertex([validar(linha)])[0]
    return {
        "hour": linha["hour"].isoformat(),
        "fonte": pedido.fonte,
        "previsao": classe,
        "significado": significado(classe),
        "real": resultado_real(linha["hour"]),  # null se as 4 horas seguintes ainda não estão na base
        "features": {f: linha[f] for f in FEATURES},
        "latencia_ms": round((time.time() - inicio) * 1000),
    }


@app.post("/prever/features")
def prever_features(pedido: PedidoFeatures):
    linha = validar(pedido.features)
    classe = chamar_vertex([linha])[0]
    return {"previsao": classe, "significado": significado(classe)}
