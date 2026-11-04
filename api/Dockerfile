FROM python:3.12-slim
WORKDIR /app
# Dependências primeiro: se só o main.py mudar, o Docker reaproveita esta camada e o build fica rápido.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
# O Cloud Run informa a porta na variável PORT (normalmente 8080).
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}
