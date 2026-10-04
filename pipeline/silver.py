"""Silver — "một hàng = một thực thể", có khoá, đúng kiểu, PII đã xử lý.

    silver_tickets         1 row per ticket_id (a deleted ticket becomes a tombstone)
    silver_ticket_history  SCD Type 2: every version of every ticket (valid_from/valid_to)
    silver_events          1 row per event_id (Kafka redeliveries removed, bad records quarantined)
    silver_transcripts     1 row per ticket_id (latest export wins)

Every daily run feeds ONLY that day's Bronze batch into these tables, so every
write here must be idempotent: run a day once or ten times, re-run an old day
after newer days — the end state must be the same.
"""
from __future__ import annotations

import json

import duckdb

from .quality import validate_events
from .staging import (
    event_records_sql, ticket_changes_sql, transcript_records_sql,
)

# PII masking used for every free-text column that leaves Bronze.
EMAIL_RE = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
PHONE_RE = r"(\+84|0)[ .-]?[0-9]{2,3}[ .-]?[0-9]{3}[ .-]?[0-9]{3,4}"

DDL = [
    f"""CREATE OR REPLACE MACRO mask_pii(s) AS
        regexp_replace(regexp_replace(s, '{EMAIL_RE}', '<EMAIL>', 'g'),
                       '{PHONE_RE}', '<PHONE>', 'g')""",
    """CREATE TABLE IF NOT EXISTS silver_tickets (
        ticket_id   VARCHAR,
        user_id     VARCHAR,
        subject     VARCHAR,
        body        VARCHAR,
        priority    VARCHAR,
        status      VARCHAR,
        category    VARCHAR,
        created_at  TIMESTAMP,
        updated_at  TIMESTAMP,
        is_deleted  BOOLEAN,
        _lsn        BIGINT,
        _batch_id   VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS silver_events (
        event_id VARCHAR, user_id VARCHAR, ticket_id VARCHAR, type VARCHAR,
        rating VARCHAR, page VARCHAR, event_time TIMESTAMP,
        _ingested_at TIMESTAMP, _batch_id VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS quarantine_events (
        _batch_id VARCHAR, _kafka_partition INTEGER, _kafka_offset BIGINT,
        event_id VARCHAR, reason VARCHAR, _payload VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS silver_transcripts (
        ticket_id VARCHAR, exported_at TIMESTAMP, n_turns INTEGER, text VARCHAR,
        _batch_id VARCHAR)""",
]


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    for stmt in DDL:
        con.execute(stmt)


# ── tickets ─────────────────────────────────────────────────────────────────

def upsert_silver_tickets(con: duckdb.DuckDBPyConnection, day: str) -> dict:
    """Apply one day's CDC batch to silver_tickets."""
    # Within one batch a ticket can change several times (and Kafka may deliver
    # the same change twice): keep only its latest change, by LSN.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _latest_changes AS
        SELECT ticket_id, user_id,
               mask_pii(subject) AS subject, mask_pii(body) AS body,
               priority, status, category, created_at, updated_at,
               (_op = 'd') AS is_deleted, _lsn, _batch_id
        FROM ({ticket_changes_sql(batch=day)})
        QUALIFY row_number() OVER (PARTITION BY ticket_id ORDER BY _lsn DESC) = 1
    """)
    (n_changes,) = con.execute("SELECT count(*) FROM _latest_changes").fetchone()

    # MERGE on the key: one row per ticket_id. The LSN guard makes replaying an
    # older batch after a newer one a no-op, so a stale state never wins.
    # A delete arrives as a change with is_deleted = true and every PII column null.
    con.execute("""
        MERGE INTO silver_tickets AS t
        USING _latest_changes AS s
        ON t.ticket_id = s.ticket_id
        WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET
            user_id = s.user_id, subject = s.subject, body = s.body,
            priority = s.priority, status = s.status, category = s.category,
            created_at = s.created_at, updated_at = s.updated_at,
            is_deleted = s.is_deleted, _lsn = s._lsn, _batch_id = s._batch_id
        WHEN NOT MATCHED THEN INSERT VALUES (
            s.ticket_id, s.user_id, s.subject, s.body, s.priority, s.status, s.category,
            s.created_at, s.updated_at, s.is_deleted, s._lsn, s._batch_id)
    """)
    (n_rows,) = con.execute("SELECT count(*) FROM silver_tickets").fetchone()
    return {"changes_in_batch": n_changes, "silver_rows": n_rows}


def build_ticket_history(con: duckdb.DuckDBPyConnection) -> int:
    """SCD Type 2 over everything landed so far: one row per version of a ticket.
    Rebuilt from Bronze every run (cheap here) -> trivially idempotent."""
    con.execute(f"""
        CREATE OR REPLACE TABLE silver_ticket_history AS
        WITH changes AS (
            SELECT * FROM ({ticket_changes_sql()})
            QUALIFY row_number() OVER (PARTITION BY ticket_id, _lsn ORDER BY _ingested_at) = 1
        )
        SELECT ticket_id, priority, status, category,
               (_op = 'd')                                              AS is_deleted,
               _changed_at                                              AS valid_from,
               lead(_changed_at) OVER (PARTITION BY ticket_id ORDER BY _lsn) AS valid_to,
               lead(_changed_at) OVER (PARTITION BY ticket_id ORDER BY _lsn) IS NULL AS is_current,
               _lsn
        FROM changes
    """)
    (n,) = con.execute("SELECT count(*) FROM silver_ticket_history").fetchone()
    return n


# ── events ──────────────────────────────────────────────────────────────────

def upsert_silver_events(con: duckdb.DuckDBPyConnection, day: str) -> dict:
    records = con.execute(event_records_sql(day)).fetchall()
    valid, bad = validate_events(records)

    # quarantine is rewritten per batch (overwrite-partition) -> idempotent
    con.execute("DELETE FROM quarantine_events WHERE _batch_id = ?", [day])
    if bad:
        con.executemany(
            "INSERT INTO quarantine_events VALUES (?, ?, ?, ?, ?, ?)",
            [(b["_batch_id"], b["_kafka_partition"], b["_kafka_offset"], b["event_id"],
              b["reason"], b["_payload"]) for b in bad])

    con.execute("""CREATE OR REPLACE TEMP TABLE _events_in (
        event_id VARCHAR, user_id VARCHAR, ticket_id VARCHAR, type VARCHAR, rating VARCHAR,
        page VARCHAR, event_time TIMESTAMP, _ingested_at TIMESTAMP, _batch_id VARCHAR,
        _kafka_partition INTEGER, _kafka_offset BIGINT)""")
    if valid:
        con.executemany(
            "INSERT INTO _events_in VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(v["event_id"], v["user_id"], v["ticket_id"], v["type"], v["rating"], v["page"],
              v["event_time"], v["_ingested_at"], v["_batch_id"], v["_kafka_partition"],
              v["_kafka_offset"]) for v in valid])
    # Events are immutable facts: insert the ones we have never seen, ignore redeliveries.
    con.execute("""
        MERGE INTO silver_events AS t
        USING (
            SELECT * EXCLUDE (_kafka_partition, _kafka_offset) FROM _events_in
            QUALIFY row_number() OVER (PARTITION BY event_id
                                       ORDER BY _ingested_at, _kafka_offset) = 1
        ) AS s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT VALUES (
            s.event_id, s.user_id, s.ticket_id, s.type, s.rating, s.page,
            s.event_time, s._ingested_at, s._batch_id)
    """)
    (n_rows,) = con.execute("SELECT count(*) FROM silver_events").fetchone()
    return {"records_in_batch": len(records), "valid": len(valid),
            "quarantined": len(bad), "silver_rows": n_rows}


# ── transcripts ─────────────────────────────────────────────────────────────

def upsert_silver_transcripts(con: duckdb.DuckDBPyConnection, day: str) -> dict:
    rows = []
    for payload, ingested_at, batch_id in con.execute(transcript_records_sql(day)).fetchall():
        obj = json.loads(payload)
        text = "\n".join(f"{t['role']}: {t['text']}" for t in obj["turns"])
        rows.append((obj["ticket_id"], ingested_at, len(obj["turns"]), text, batch_id))
    con.execute("""CREATE OR REPLACE TEMP TABLE _transcripts_in (
        ticket_id VARCHAR, exported_at TIMESTAMP, n_turns INTEGER, text VARCHAR,
        _batch_id VARCHAR)""")
    if rows:
        con.executemany("INSERT INTO _transcripts_in VALUES (?, ?, ?, ?, ?)", rows)
    con.execute("""
        MERGE INTO silver_transcripts AS t
        USING (
            SELECT ticket_id, exported_at, n_turns, mask_pii(text) AS text, _batch_id
            FROM _transcripts_in
            QUALIFY row_number() OVER (PARTITION BY ticket_id ORDER BY exported_at DESC) = 1
        ) AS s
        ON t.ticket_id = s.ticket_id
        WHEN MATCHED AND s.exported_at > t.exported_at THEN UPDATE SET
            exported_at = s.exported_at, n_turns = s.n_turns, text = s.text,
            _batch_id = s._batch_id
        WHEN NOT MATCHED THEN INSERT VALUES (
            s.ticket_id, s.exported_at, s.n_turns, s.text, s._batch_id)
    """)
    (n_rows,) = con.execute("SELECT count(*) FROM silver_transcripts").fetchone()
    return {"exports_in_batch": len(rows), "silver_rows": n_rows}
