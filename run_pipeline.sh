#!/usr/bin/env bash
set -e

echo "=== 🐍 Installing dependencies with uv ==="
uv sync --all-extras

echo "=== 🐘 Starting PostgreSQL container ==="
docker compose up -d

echo "Waiting for PostgreSQL to be ready..."
sleep 10

echo "=== 🚀 Running data pipeline ==="
uv run python ./efo_ingestion_pipeline.py

echo "=== ✅ Pipeline run completed successfully ==="
