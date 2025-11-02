#!/usr/bin/env bash
set -e

echo "=== 🔧 Preparing local environment ==="

# Copy env templates if missing
if [ ! -f ".env" ]; then
  echo "Creating .env from template..."
  cp .env.example .env
fi

if [ ! -f ".dlt/secrets.toml" ]; then
  echo "Creating DLT secrets.toml from example..."
  mkdir -p .dlt
  cp .dlt/secrets.example.toml .dlt/secrets.toml
fi

echo "=== 🐍 Installing dependencies with uv ==="
uv sync --all-extras

echo "=== 🐘 Starting PostgreSQL container ==="
docker compose up -d

echo "Waiting for PostgreSQL to be ready..."
sleep 10

echo "=== 🚀 Running data pipeline ==="
uv run python ./efo_ingestion_pipeline.py

echo "=== ✅ Pipeline run completed successfully ==="
