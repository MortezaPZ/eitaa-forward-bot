# LEGACY: only relevant to TRANSPORT=http mode on Linux. Production uses TRANSPORT=web on Windows — see WINDOWS_DEPLOY.md
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Tehran

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./

RUN mkdir -p /app/data/logs /app/data/files \
    && useradd --create-home --uid 10001 botuser \
    && chown -R botuser:botuser /app
USER botuser

STOPSIGNAL SIGTERM
CMD ["python", "-u", "main.py"]
