# Multi-stage build for PowerApps ETL Pipeline
# Demonstrates production-ready containerization practices

# Stage 1: Builder stage for dependencies
FROM python:3.9-slim as builder

# Set working directory
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Stage 2: Production stage
FROM python:3.9-slim

# Create non-root user for security
RUN groupadd -r pipeline && useradd -r -g pipeline pipeline

# Set working directory
WORKDIR /app

# Copy Python dependencies from builder
COPY --from=builder /root/.local /home/pipeline/.local

# Update PATH
ENV PATH=/home/pipeline/.local/bin:$PATH

# Copy application code
COPY export_simulator.py .
COPY transform_processor.py .
COPY load_to_database.py .
COPY pipeline_orchestrator.py .
COPY config.yaml .

# Create necessary directories with proper permissions
RUN mkdir -p /app/data/exports \
    /app/data/processed \
    /app/data/database \
    /app/logs \
    /app/config \
    && chown -R pipeline:pipeline /app

# Switch to non-root user
USER pipeline

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIPELINE_ENV=production \
    DATA_DIR=/app/data \
    CONFIG_FILE=/app/config.yaml

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sqlite3; conn = sqlite3.connect('/app/data/database/data_warehouse.db'); conn.close()" || exit 1

# Default command (can be overridden)
CMD ["python", "pipeline_orchestrator.py", "--days", "1"]

# Metadata
LABEL maintainer="Tom Leja" \
      version="1.0.0" \
      description="PowerApps ETL Pipeline Container"
