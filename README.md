# Trabalho 2: streaming e deploy de modelo (volatilidade do Bitcoin)

Evolução do Trabalho 1. O modelo que prevê **alta volatilidade do BTC nas próximas 4 horas**
(variação de 0,5% ou mais) agora recebe dados em tempo real e está publicado no Vertex AI.

Projeto GCP: `pdm-trabalho-508212` · Dataset BigQuery: `btc` (US) · Região dos serviços: `us-central1`

## Arquitetura

```
BigQuery btc_klines_1m ──► Simulador (Cloud Run Job, 60x) ──► Pub/Sub btc-klines (schema Avro)
                                                                   │
                                                                   ▼
                                                     Dataflow velas-btc (Apache Beam)
                                                       │                         │
                                                       ▼                         ▼
                                     btc_stream_bronze (1 min, cru)   btc_stream_silver (1 hora)

Cliente ──► API REST (Cloud Run) ──► 24 h de histórico no BigQuery ──► Vertex AI rf-volatilidade-4h ──► 0 ou 1
```

- **Streaming simulado** (liberado pelo professor): o simulador reproduz velas reais de 1 minuto do
  período de teste (a partir de 10/04/2026), que o modelo nunca viu, em velocidade acelerada.
  A 60x, uma hora de mercado passa em um minuto.
- **Bronze do stream:** cada vela de 1 minuto consumida é gravada crua, no momento em que chega.
- **Silver do stream:** o Dataflow agrupa as velas pela hora que vem dentro da mensagem e emite a vela
  de 1 hora assim que chegam os seus 60 minutos (ou após uma espera, se a hora tiver um buraco nos dados).
  As regras são as mesmas da `btc_trades_silver` do Trabalho 1.

## Estrutura

| Pasta | Conteúdo |
|---|---|
| `sql/streaming/` | Fita de velas de 1 minuto, tabelas do stream e consultas de conferência |
| `pubsub/` | Schema Avro, criação do tópico e das assinaturas, teste do schema |
| `simulador/` | Simulador em Python, Dockerfile e scripts de deploy e execução no Cloud Run |
| `dataflow/` | Pipeline Apache Beam, testes locais e scripts para lançar e parar o job |
| `vertex/` | Registro, deploy, teste e desligamento do endpoint |
| `notebooks/` | Treino do Random Forest em scikit-learn para o Vertex AI |
| `api/` | API REST no Cloud Run (em construção) |

## Como executar (a partir da raiz, no Cloud Shell)

```bash
# 0. ambiente
python3 -m venv ~/venv && source ~/venv/bin/activate
pip install -r simulador/requirements.txt -r dataflow/requirements.txt

# 1. BigQuery: rodar sql/streaming/01_klines_1m.sql e 02_stream_tables.sql

# 2. Pub/Sub
bash pubsub/criar_pubsub.sh
bash pubsub/testar_pubsub.sh              # a segunda mensagem deve ser recusada pelo schema

# 3. Simulador na nuvem
bash simulador/deploy.sh

# 4. Dataflow (esperar o job ficar "Em execução", cerca de 4 min)
python dataflow/test_pipeline.py          # testes locais, sem nuvem
bash dataflow/lancar.sh

# 5. Disparar o stream e acompanhar
bash simulador/executar.sh 2026-06-27T08:00:00Z 2026-06-27T13:00:00Z 120
#    BigQuery: sql/streaming/04_monitorar_stream_bronze.sql (ao vivo)
#              sql/streaming/03_conferir_stream_silver.sql  (stream x Trabalho 1)

# 6. Modelo no Vertex AI
bash vertex/registrar_e_publicar.sh       # de 10 a 20 min
bash vertex/testar_endpoint.sh
```

Regra do simulador: com o job do Dataflow ligado, reproduza cada período uma vez só, ou relance o job
antes de repetir. Uma hora reproduzida duas vezes gera duas linhas na Silver do stream (a consulta 03
fica com a mais recente).

## Resultados

| Verificação | Resultado |
|---|---|
| Velas de 1 minuto x Silver do Trabalho 1 | 3.528 horas, 0 divergências |
| Schema do tópico | Mensagem incompleta recusada (`INVALID_JSON_AVRO_MESSAGE`) |
| Stream de ponta a ponta (27/06 08h a 12h, 120x) | 5 velas de 1 hora, `n_minutos = 60`, iguais à Silver do Trabalho 1, uma a cada 30 s |
| Modelo no Vertex AI (scikit-learn) | AUC 0,667 no teste (BigQuery ML: 0,669); baseline de acurácia 0,618 |
| Endpoint | Horas calmas `[0,0,0,0]` e horas turbulentas de 26/06 `[1,1,1,1]`, todas corretas |

Contrato do endpoint: 13 features **por posição**, nesta ordem:
`return_1h, return_4h, return_8h, volatility_24h, volume_ratio_24h, trade_count, order_flow_ratio,
candle_range, candle_body, hour_of_day, day_of_week, hour_sin, hour_cos`.

## Custos: desligue depois de usar

O job do Dataflow e o endpoint com modelo implantado cobram por hora enquanto estão ligados.

```bash
bash dataflow/parar.sh
bash vertex/desligar_endpoint.sh
```
