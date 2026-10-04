"""BONUS — an LLM inside the pipeline (slide "LLM là một bước transform").

The support team wants an LLM pre-triage label on every live ticket
(gold_ticket_labels), to compare with the human `category` and to triage new
tickets faster. An LLM step is a transform like any other — except it is
expensive, slow and NOT deterministic, so the slide's four rules apply:

  1. key = hash(input) + model + prompt version  -> a re-run makes 0 LLM calls;
     changing the prompt re-labels everything ON PURPOSE
  2. force a structured output, validate it; invalid -> quarantine, never Gold
  3. estimate the cost BEFORE running (rows x tokens x price)
  4. LLM labels are versioned data (model + prompt_version stored on every row)

The shipped `label_tickets` was the NAIVE version (a call for every ticket on
every run, whatever came back written to Gold). It now follows the four rules
above: llm_label_cache keyed by hash + model + prompt version, off-schema
answers in llm_label_quarantine, and a cost estimate for the cache misses
before any call. `python -m scripts.bonus_llm` prints BONUS PASS. Zero-key: `FakeLLM` stands in for a
real model (swap in any provider via .env if you like — the pipeline is the same).
"""
from __future__ import annotations

import hashlib
import json
import re

import duckdb

MODEL = "fake-llm-2026-09"
PROMPT_VERSION = "triage-v1"
ALLOWED_LABELS = ("bug", "billing", "other")
PRICE_PER_1K_TOKENS_USD = 0.002          # pretend price, for the cost estimate


PROMPT_TEMPLATE = """You triage customer-support tickets.
Answer ONLY with JSON: {{"label": "bug" | "billing" | "other"}}.
Ticket: {text}"""


class FakeLLM:
    """Deterministic stand-in for a chat model. Counts calls and tokens."""

    def __init__(self, model: str = MODEL) -> None:
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        text = prompt.lower()
        if "xuất" in text:
            return 'Sure! Here is the label: {"label": "export"}'   # off-schema answer
        if re.search(r"crash|lỗi|sso|đăng nhập|chatbot", text):
            return '{"label": "bug"}'
        if re.search(r"tiền|hoá đơn|thanh toán|gói|vat", text):
            return '{"label": "billing"}'
        return '{"label": "other"}'


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(PROMPT_TEMPLATE.format(text=t).split()) + 8 for t in texts)


def parse_label(raw: str) -> str | None:
    """Pull {"label": ...} out of the model's answer; None if it is not valid."""
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        return None
    try:
        label = json.loads(m.group(0)).get("label")
    except json.JSONDecodeError:
        return None
    return label if label in ALLOWED_LABELS else None


def live_tickets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""
        SELECT ticket_id, subject || '. ' || body AS text
        FROM silver_tickets
        WHERE NOT is_deleted
        ORDER BY ticket_id
    """).fetchall()


def input_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def label_tickets(con: duckdb.DuckDBPyConnection, llm: FakeLLM) -> dict:
    """Label every live ticket, calling the LLM only for (input, model, prompt) never seen.

    Cache key = sha256(ticket text) + model + prompt version. The raw answer is cached
    too (valid or not), so a re-run makes 0 calls; a new PROMPT_VERSION misses the
    cache for every ticket and re-labels on purpose. Off-schema answers never reach
    Gold: they go to llm_label_quarantine with the raw answer for review.
    """
    # read module globals at call time, so a bumped PROMPT_VERSION takes effect
    model, prompt_version = MODEL, PROMPT_VERSION
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_cache (
        input_hash VARCHAR, model VARCHAR, prompt_version VARCHAR,
        raw_answer VARCHAR, label VARCHAR,
        PRIMARY KEY (input_hash, model, prompt_version))""")

    tickets = [(tid, text, input_hash(text)) for tid, text in live_tickets(con)]
    cached = {h for (h,) in con.execute(
        "SELECT input_hash FROM llm_label_cache WHERE model = ? AND prompt_version = ?",
        [model, prompt_version]).fetchall()}
    # several tickets may share a text: call once per distinct missing hash
    missing = {h: text for _, text, h in tickets if h not in cached}

    # rule 3: estimate the cost of what we are ABOUT to call, before calling it
    est_tokens = estimate_tokens(list(missing.values()))
    est_usd = est_tokens / 1000 * PRICE_PER_1K_TOKENS_USD

    calls_before = llm.calls
    for h, text in sorted(missing.items()):
        raw = llm.complete(PROMPT_TEMPLATE.format(text=text))
        con.execute("INSERT INTO llm_label_cache VALUES (?, ?, ?, ?, ?)",
                    [h, model, prompt_version, raw, parse_label(raw)])

    # Gold + quarantine are rebuilt from the cache for the current model + prompt
    # (overwrite, not append) -> idempotent, and every row carries its version.
    con.execute("CREATE OR REPLACE TEMP TABLE _tickets_in (ticket_id VARCHAR, input_hash VARCHAR)")
    if tickets:
        con.executemany("INSERT INTO _tickets_in VALUES (?, ?)",
                        [(tid, h) for tid, _, h in tickets])
    con.execute("""
        CREATE OR REPLACE TABLE gold_ticket_labels AS
        SELECT t.ticket_id, c.label, c.model, c.prompt_version
        FROM _tickets_in t
        JOIN llm_label_cache c
          ON c.input_hash = t.input_hash AND c.model = ? AND c.prompt_version = ?
        WHERE c.label IS NOT NULL
        ORDER BY t.ticket_id
    """, [model, prompt_version])
    con.execute("""
        CREATE OR REPLACE TABLE llm_label_quarantine AS
        SELECT t.ticket_id, c.input_hash, c.model, c.prompt_version, c.raw_answer,
               'off-schema answer: label not in ' || ? AS reason
        FROM _tickets_in t
        JOIN llm_label_cache c
          ON c.input_hash = t.input_hash AND c.model = ? AND c.prompt_version = ?
        WHERE c.label IS NULL
        ORDER BY t.ticket_id
    """, [str(ALLOWED_LABELS), model, prompt_version])

    (labeled,) = con.execute("SELECT count(*) FROM gold_ticket_labels").fetchone()
    (quarantined,) = con.execute("SELECT count(*) FROM llm_label_quarantine").fetchone()
    return {"tickets": len(tickets), "labeled": labeled, "quarantined": quarantined,
            "calls": llm.calls - calls_before, "est_tokens": est_tokens,
            "est_usd": round(est_usd, 6)}
