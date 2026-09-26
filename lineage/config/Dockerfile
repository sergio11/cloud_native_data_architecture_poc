# ==============================================================================
# Dockerfile for OpenLineage Webhook & Active Metadata Service
# ==============================================================================
FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Final Runtime Image
FROM python:3.11-slim AS runtime

WORKDIR /app

# Create non-root user for container security
RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -s /bin/bash -m appuser

# Copy installed python wheels from builder
COPY --from=builder /root/.local /home/appuser/.local
COPY lineage_webhook_service.py .
COPY alert-rules.yaml .

# Set environment variables
ENV PATH=/home/appuser/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PORT=8000 \
    HOST=0.0.0.0 \
    LINEAGE_DB_PATH=/data/lineage_events.db \
    ALERT_RULES_PATH=/app/alert-rules.yaml

# Data directory for SQLite database storage
RUN mkdir -p /data && chown -R appuser:appgroup /data /app

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

ENTRYPOINT ["uvicorn", "lineage_webhook_service:app"]
CMD ["--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
