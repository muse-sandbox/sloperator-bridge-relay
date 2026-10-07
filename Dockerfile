FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY relay/ /app/relay/

RUN useradd -u 10095 -m relay && mkdir -p /data && chown relay /data
USER relay

# One process on purpose: the long-poll wake-ups live in this process's memory.
CMD ["uvicorn", "app:app", "--app-dir", "/app/relay", "--host", "0.0.0.0", "--port", "8095", \
     "--workers", "1", "--timeout-keep-alive", "75", "--no-server-header", "--proxy-headers", \
     "--forwarded-allow-ips", "*"]
