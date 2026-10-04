"""Staging — typed views over Bronze. No business logic, no dedup, no deletes applied.

Reading Debezium correctly is half of CDC work. One Kafka record per change:

    key   = {"ticket_id": ...}
    value = {"before": {...} | null,   # row before the change (null for c / r)
             "after":  {...} | null,   # row after the change  (null for d)
             "source": {"lsn": ..., "ts_ms": ...},
             "op": "c" | "u" | "d" | "r"}

After a delete Debezium also sends a *tombstone*: same key, value = null, so
Kafka log compaction can forget the key. Tombstones carry no change.

Timestamps in `after` use Debezium's default for Postgres TIMESTAMP:
microseconds since epoch (io.debezium.time.MicroTimestamp).
"""
from __future__ import annotations

from .bronze import bronze_scan


def _where(upto: str | None, batch: str | None) -> str:
    conds = []
    if upto:
        conds.append(f"_batch_id <= '{upto}'")
    if batch:
        conds.append(f"_batch_id = '{batch}'")
    return ("WHERE " + " AND ".join(conds)) if conds else ""


def ticket_changes_sql(upto: str | None = None, batch: str | None = None) -> str:
    """One typed row per CDC change of the `tickets` table (redeliveries included).

    `upto`  -> only batches landed on or before that day (as-of / time travel)
    `batch` -> only that day's batch (what a daily incremental run sees)
    """
    return f"""
    SELECT * FROM (
        SELECT
            -- a delete (op = 'd') has after = null: the key lives in `before`
            coalesce(j->'value'->'after'->>'ticket_id',
                     j->'value'->'before'->>'ticket_id')            AS ticket_id,
            _op,
            (j->'value'->'source'->>'lsn')::BIGINT                  AS _lsn,
            make_timestamp((j->'value'->'source'->>'ts_ms')::BIGINT * 1000) AS _changed_at,
            j->'value'->'after'->>'user_id'                         AS user_id,
            j->'value'->'after'->>'subject'                         AS subject,
            j->'value'->'after'->>'body'                            AS body,
            j->'value'->'after'->>'priority'                        AS priority,
            j->'value'->'after'->>'status'                          AS status,
            j->'value'->'after'->>'category'                        AS category,
            make_timestamp((j->'value'->'after'->>'created_at')::BIGINT) AS created_at,
            make_timestamp((j->'value'->'after'->>'updated_at')::BIGINT) AS updated_at,
            _batch_id,
            _ingested_at,
            _kafka_offset
        FROM (SELECT *, _payload::JSON AS j FROM {bronze_scan('tickets')} {_where(upto, batch)})
        WHERE _op IS NOT NULL            -- Kafka tombstone (value = null): nothing changed
    )
    WHERE ticket_id IS NOT NULL
    """


def event_records_sql(batch: str) -> str:
    """Raw event records of one batch, for record-level validation in Python."""
    return f"""
    SELECT _payload, _kafka_partition, _kafka_offset, _ingested_at, _batch_id
    FROM {bronze_scan('events')}
    WHERE _batch_id = '{batch}'
    ORDER BY _kafka_partition, _kafka_offset, _ingested_at
    """


def transcript_records_sql(batch: str) -> str:
    return f"""
    SELECT _payload, _ingested_at, _batch_id
    FROM {bronze_scan('transcripts')}
    WHERE _batch_id = '{batch}'
    ORDER BY _ingested_at
    """


def event_lateness_sql() -> str:
    """Calendar days between when an event happened and when its batch landed."""
    return f"""
    SELECT date_diff('day',
                     CAST(replace(_payload::JSON->'value'->>'event_time', 'Z', '') AS TIMESTAMP)::DATE,
                     _ingested_at::DATE) AS lateness_days
    FROM {bronze_scan('events')}
    WHERE _payload::JSON->'value'->>'event_time' IS NOT NULL
    """
