FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY baseline ./baseline
COPY ft ./ft
COPY hardware ./hardware
COPY deploy ./deploy
ENV DATA_DIR=/data TOPOLOGY_FILE=/data/topology.json PYTHONUNBUFFERED=1 PYTHONPATH=/app APP=ft.gateway
EXPOSE 8000
CMD ["sh", "-c", "exec uvicorn ${APP}:app --host 0.0.0.0 --port 8000 --log-level warning --no-access-log"]
