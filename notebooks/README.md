Coloque aqui o `treino_vertex.ipynb`, exportado do BigQuery Studio (Arquivo -> Baixar .ipynb).

Ele lê a `btc.btc_trades_gold`, treina o Random Forest em scikit-learn 1.6 com as 13 features
e o corte temporal de 80/20, e salva o `model.joblib` em `gs://pdm-trabalho-508212-modelos/rf-vol-4h/v1/`.
