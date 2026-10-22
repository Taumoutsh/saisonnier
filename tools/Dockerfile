# Image des simulateurs (WebSocket + serveurs TCP), pour docker compose.
FROM python:3.13-slim
RUN pip install --no-cache-dir websockets==15.0.1
WORKDIR /tools
COPY *.py ./
ENV PYTHONUNBUFFERED=1
