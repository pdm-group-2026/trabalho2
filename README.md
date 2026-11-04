# Trabalho 2: streaming e deploy de modelo (volatilidade do Bitcoin)

Evolução do Trabalho 1. O modelo que prevê **alta volatilidade do BTC nas próximas 4 horas**
(variação de 0,5% ou mais) agora recebe dados em tempo real, está publicado no Vertex AI e é servido
por uma API REST no Cloud Run.

Projeto GCP: `pdm-trabalho-508212` · Dataset BigQuery: `btc` (US) · Região dos serviços: `us-central1`

API no Cloud Run: https://api-volatilidade-t6p7bcqvbq-uc.a.run.app (privada: as chamadas levam token, ver abaixo)

## Arquitetura

```
BATCH (Trabalho 1)
  Binance (trades) -> GCS Parquet -> Bronze -> Silver (1 h) -> Gold -> treino do modelo
                                        |
                                        v
                                 btc_klines_1m (velas de 1 min do período de teste)
                                        |
STREAMING (Trabalho 2)                  v
  Simulador (Cloud Run Job) -> Pub/Sub btc-klines (schema Avro) -> Dataflow (Apache Beam)
                                                                     |              |
                                                                     v              v
                                                      btc_stream_bronze    btc_stream_silver (1 h)
                                                         (1 min, cru)               |
                                                                                    v
                                                                     btc_stream_gold (view, 13 features)

PREDIÇÃO
  Cliente -> API REST (Cloud Run) -> features da Gold (stream ou batch)
                                  -> Vertex AI rf-volatilidade-4h -> 0 ou 1
```

- **Streaming simulado** (liberado pelo professor): o simulador reproduz velas reais de 1 minuto do
  período de teste (a partir de 10/04/2026), que o modelo nunca viu, em velocidade acelerada.
  A 60x, uma hora de mercado passa em um minuto.
- **Bronze do stream:** cada vela de 1 minuto consumida é gravada crua, no momento em que chega.
- **Silver do stream:** o Dataflow agrupa as velas pela hora que vem dentro da mensagem e emite a vela
  de 1 hora assim que chegam os seus 60 minutos (ou após uma espera, se a hora tiver um buraco nos dados).
  As regras são as mesmas da `btc_trades_silver` do Trabalho 1.
- **Gold do stream:** a view `btc_stream_gold` calcula as 13 features do modelo sobre a Silver do stream,
  com as mesmas regras da `btc_trades_gold`. Uma hora fica `pronta_para_prever` quando tem os 60 minutos
  e 25 horas seguidas de histórico no stream.

## Estrutura

| Pasta | Conteúdo |
|---|---|
| `sql/streaming/` | Fita de velas de 1 minuto, tabelas e view do stream, consultas de conferência |
| `pubsub/` | Schema Avro, criação do tópico e das assinaturas, teste do schema |
| `simulador/` | Simulador em Python, Dockerfile e scripts de deploy e execução no Cloud Run |
| `dataflow/` | Pipeline Apache Beam, testes locais e scripts para lançar e parar o job |
| `vertex/` | Registro, deploy, teste e desligamento do endpoint |
| `notebooks/` | Treino do Random Forest em scikit-learn para o Vertex AI |
| `api/` | API REST (FastAPI) no Cloud Run: `main.py`, `Dockerfile`, `requirements.txt`, deploy e teste |

## Como executar (a partir da raiz, no Cloud Shell)

```bash
# 0. ambiente
python3 -m venv ~/venv && source ~/venv/bin/activate
pip install -r simulador/requirements.txt -r dataflow/requirements.txt

# 1. BigQuery: rodar sql/streaming/01_klines_1m.sql, 02_stream_tables.sql e 05_stream_gold.sql

# 2. Pub/Sub
bash pubsub/criar_pubsub.sh
bash pubsub/testar_pubsub.sh              # a segunda mensagem deve ser recusada pelo schema

# 3. Simulador na nuvem
bash simulador/deploy.sh

# 4. Dataflow (esperar o job ficar "Em execução", cerca de 4 min)
python dataflow/test_pipeline.py          # testes locais, sem nuvem
bash dataflow/lancar.sh

# 5. Modelo no Vertex AI
bash vertex/registrar_e_publicar.sh       # de 10 a 20 min
bash vertex/testar_endpoint.sh

# 6. API REST no Cloud Run
bash api/deploy.sh
bash api/testar_api.sh

# 7. Disparar o stream e acompanhar (pelo menos 26 horas, para a Gold do stream ter horas prontas)
bash simulador/executar.sh 2026-06-25T12:00:00Z 2026-06-26T14:00:00Z 120
#    BigQuery: sql/streaming/04_monitorar_stream_bronze.sql (ao vivo)
#              sql/streaming/03_conferir_stream_silver.sql  (stream x Trabalho 1)
```

Regra do simulador: uma hora reproduzida duas vezes gera duas linhas na Silver do stream. A consulta 03
e a view `btc_stream_gold` ficam com a mais recente.

## API REST

| Rota | Entrada | O que faz |
|---|---|---|
| `GET /` | | Descrição da API, do modelo e da ordem das features |
| `GET /health` | | Verificação de vida |
| `POST /prever` | `{"hour": "2026-06-26T12:00:00Z", "fonte": "stream"}` | Lê as 13 features da hora na Gold (`stream` = `btc_stream_gold`, `batch` = `btc_trades_gold`), chama o Vertex AI e devolve a previsão, o resultado real e a latência. Sem `hour`, usa a hora mais recente pronta. |
| `POST /prever/features` | `{"features": {...13 valores pelo nome...}}` | Previsão a partir de features prontas |
| `GET /docs` | | Documentação interativa (Swagger) |

A API não carrega o modelo: quem executa o Random Forest é o Vertex AI. Ela só busca as features,
põe na ordem do treino e chama o endpoint. Trocar o modelo não exige novo deploy da API.

O serviço não é público. Para chamar:

```bash
TOKEN=$(gcloud auth print-identity-token)
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"hour": "2026-06-26T12:00:00Z", "fonte": "batch"}' \
  https://api-volatilidade-t6p7bcqvbq-uc.a.run.app/prever
```

Para abrir o Swagger no navegador: `gcloud run services proxy api-volatilidade --region=us-central1 --port=8080`
e, no Cloud Shell, **Visualização na Web → porta 8080** → `/docs`.

## Resultados

| Verificação | Resultado |
|---|---|
| Velas de 1 minuto x Silver do Trabalho 1 | 3.528 horas, 0 divergências |
| Schema do tópico | Mensagem incompleta recusada (`INVALID_JSON_AVRO_MESSAGE`) |
| Stream de ponta a ponta (27/06 08h a 12h, 120x) | 5 velas de 1 hora, `n_minutos = 60`, iguais à Silver do Trabalho 1, uma a cada 30 s |
| Modelo no Vertex AI (scikit-learn) | AUC 0,667 no teste (BigQuery ML: 0,669); baseline de acurácia 0,618 |
| Endpoint | Horas calmas `[0,0,0,0]` e horas turbulentas de 26/06 `[1,1,1,1]`, todas corretas |
| Gold do stream x Gold do Trabalho 1 (26/06 08h a 13h) | 6 horas iguais; retornos com diferença zero, volatilidade com 0,004% (ver abaixo) |
| Previsão pela API: stream x batch (26/06 08h a 13h) | Idêntica nas 6 horas; o modelo acertou 4 de 6 |
| Latência da API (BigQuery + Vertex AI) | Cerca de 2 s por previsão |

**Achado da conferência stream x batch:** a Silver do Trabalho 1 escolhe o fechamento da hora entre trades do mesmo milissegundo sem critério fixo; o stream desempata pelo `id` do trade. Nas horas com esse empate, o fechamento muda na 6ª casa decimal, sem efeito nas previsões. Testes: `sql/streaming/06_conferir_stream_gold.sql` e `api/comparar_stream_batch.sh`.

Contrato do endpoint: 13 features **por posição**, nesta ordem:
`return_1h, return_4h, return_8h, volatility_24h, volume_ratio_24h, trade_count, order_flow_ratio,
candle_range, candle_body, hour_of_day, day_of_week, hour_sin, hour_cos`.

## Custos: desligue depois de usar

O job do Dataflow e o endpoint com modelo implantado cobram por hora enquanto estão ligados.
O Cloud Run (API e simulador) só cobra quando recebe chamadas ou executa.

```bash
bash dataflow/parar.sh
bash vertex/desligar_endpoint.sh
```
