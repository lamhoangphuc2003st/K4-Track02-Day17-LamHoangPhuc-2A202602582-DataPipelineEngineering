"""Central config. Everything runs locally on DuckDB — no API keys, no cloud."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"            # what the source systems deliver, one file per day
LAKE_DIR = ROOT / "lake"            # stand-in for S3 / GCS
BRONZE_DIR = LAKE_DIR / "bronze"    # Bronze = immutable Parquet, one file per (source, day)
WAREHOUSE = ROOT / "warehouse.duckdb"   # Silver + Gold live here
SUBMISSION_DIR = ROOT / "submission"

FIRST_DAY = "2026-08-10"
LAST_DAY = "2026-08-16"
RERUN_DAY = "2026-08-12"            # the "old day" the grader re-runs three times

# raw file per source per ingest day (what Kafka Connect / the exporter wrote)
SOURCES = {
    "tickets": "cdc/tickets/{day}.jsonl",      # Debezium CDC (Postgres), via Kafka
    "events": "events/{day}.jsonl",            # Kafka topic support.events
    "transcripts": "transcripts/{day}.json",   # hourly S3 dumps, merged per day
}

# How many days back every daily run recomputes gold_feature_daily.
# Measured from Bronze (python main.py --lateness): P99 of (_ingested_at - event_time)
# is 3 calendar days (offline mobile clients), so lookback = ceil(P99) = 3.
LOOKBACK_DAYS = 3

EMBEDDING_MODEL_VERSION = "hash-embed-v1"
CHUNK_WORDS = 40
CHUNK_OVERLAP = 8

GOLD_TABLES = ("gold_feature_daily", "gold_training_set", "gold_doc_chunks")


def days_between(start: str, end: str) -> list[str]:
    d0, d1 = date.fromisoformat(start), date.fromisoformat(end)
    return [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]


def shift(day: str, days: int) -> str:
    return (date.fromisoformat(day) + timedelta(days=days)).isoformat()
