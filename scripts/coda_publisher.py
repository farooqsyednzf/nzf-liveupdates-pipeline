"""
Coda publisher.

Implements the async-safe sequence required by the Coda API:
- DELETE existing rows in batches, poll until empty, buffer wait
- INSERT new rows in small batches with rate-limit backoff
- Verify post-insert that no numeric CaseId values remain (precision loss check)
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date

import requests

from config.settings import (
    CODA_BASE_URL,
    CODA_COLUMNS,
    CODA_DELETE_BATCH_SIZE,
    CODA_DELETE_BUFFER_AFTER_EMPTY,
    CODA_DELETE_POLL_INTERVAL,
    CODA_DOC_ID,
    CODA_INSERT_BACKOFF_BASE,
    CODA_INSERT_BATCH_SIZE,
    CODA_INSERT_BATCH_SLEEP,
    CODA_INSERT_MAX_RETRIES,
    CODA_INSERT_VERIFY_DELAY,
    CODA_TABLE_ID,
    ZWNJ,
)

log = logging.getLogger(__name__)


class CodaError(RuntimeError):
    pass


def _headers() -> dict:
    token = os.environ.get("CODA_API_TOKEN")
    if not token:
        raise RuntimeError("CODA_API_TOKEN not set")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _rows_url() -> str:
    return f"{CODA_BASE_URL}/docs/{CODA_DOC_ID}/tables/{CODA_TABLE_ID}/rows"


def _fetch_all_rows() -> list[dict]:
    """Fetch all rows from the table via pagination."""
    rows: list[dict] = []
    url = _rows_url()
    params = {"limit": 200}
    while url:
        resp = requests.get(url, headers=_headers(), params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        rows.extend(payload.get("items", []))
        next_link = payload.get("nextPageLink")
        if next_link:
            url = next_link
            params = None
        else:
            break
    return rows


def _row_count() -> int:
    resp = requests.get(
        _rows_url(), headers=_headers(), params={"limit": 1}, timeout=60
    )
    resp.raise_for_status()
    payload = resp.json()
    return len(payload.get("items", []))


def delete_all_rows() -> None:
    """Delete every row in the table, polling until the table is empty."""
    log.info("Fetching rows for deletion")
    existing = _fetch_all_rows()
    row_ids = [r["id"] for r in existing]
    if not row_ids:
        log.info("Table already empty")
        return

    log.info("Deleting %d rows in batches of %d", len(row_ids), CODA_DELETE_BATCH_SIZE)
    for i in range(0, len(row_ids), CODA_DELETE_BATCH_SIZE):
        batch = row_ids[i : i + CODA_DELETE_BATCH_SIZE]
        resp = requests.delete(
            _rows_url(),
            headers=_headers(),
            json={"rowIds": batch},
            timeout=60,
        )
        if resp.status_code not in (200, 202):
            raise CodaError(
                f"Delete batch failed: {resp.status_code} {resp.text[:200]}"
            )

    log.info("Polling until table is empty")
    deadline = time.time() + 180
    while time.time() < deadline:
        rows = _fetch_all_rows()
        if not rows:
            break
        log.info("%d rows still present, waiting", len(rows))
        time.sleep(CODA_DELETE_POLL_INTERVAL)
    else:
        raise CodaError("Delete polling timed out, rows still present")

    log.info("Table confirmed empty, buffering %ds", CODA_DELETE_BUFFER_AFTER_EMPTY)
    time.sleep(CODA_DELETE_BUFFER_AFTER_EMPTY)


def _build_row(message_type: str, message: str, case_id: str) -> dict:
    """Build a Coda row payload with ZWNJ-prefixed CaseId."""
    today_us = date.today().strftime("%m/%d/%Y")
    case_id_safe = ZWNJ + str(case_id).lstrip(ZWNJ)
    return {
        "cells": [
            {"column": CODA_COLUMNS["published_date"], "value": today_us},
            {"column": CODA_COLUMNS["application_type"], "value": message_type},
            {"column": CODA_COLUMNS["message"], "value": message},
            {"column": CODA_COLUMNS["case_id"], "value": case_id_safe},
        ]
    }


def insert_rows(messages: list[tuple[str, str, str]]) -> None:
    """
    Insert rows in small batches with backoff on 429.

    messages: list of (type, message, case_id) tuples.
    """
    log.info("Inserting %d rows in batches of %d", len(messages), CODA_INSERT_BATCH_SIZE)
    for i in range(0, len(messages), CODA_INSERT_BATCH_SIZE):
        batch = messages[i : i + CODA_INSERT_BATCH_SIZE]
        rows_payload = [
            _build_row(msg_type, msg, cid) for (msg_type, msg, cid) in batch
        ]
        body = {"rows": rows_payload, "keyColumns": []}

        for attempt in range(1, CODA_INSERT_MAX_RETRIES + 1):
            resp = requests.post(
                _rows_url(),
                headers=_headers(),
                json=body,
                timeout=60,
            )
            if resp.status_code in (200, 202):
                break
            if resp.status_code == 429:
                wait = CODA_INSERT_BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning(
                    "Rate limited on batch %d, sleeping %ds (attempt %d/%d)",
                    i // CODA_INSERT_BATCH_SIZE,
                    wait,
                    attempt,
                    CODA_INSERT_MAX_RETRIES,
                )
                time.sleep(wait)
                continue
            if resp.status_code in (500, 502, 503, 504):
                wait = CODA_INSERT_BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning(
                    "Server error %d on batch, sleeping %ds (attempt %d/%d)",
                    resp.status_code,
                    wait,
                    attempt,
                    CODA_INSERT_MAX_RETRIES,
                )
                time.sleep(wait)
                continue
            raise CodaError(
                f"Insert failed: {resp.status_code} {resp.text[:200]}"
            )
        else:
            raise CodaError(f"Insert batch {i} exhausted retries")

        time.sleep(CODA_INSERT_BATCH_SLEEP)

    log.info("Insert complete, waiting %ds before verification", CODA_INSERT_VERIFY_DELAY)
    time.sleep(CODA_INSERT_VERIFY_DELAY)


def verify_no_precision_loss() -> int:
    """
    Fetch all rows and confirm no CaseId is stored as int/float.
    Returns the total row count.
    """
    log.info("Verifying no numeric CaseIds remain")
    rows = _fetch_all_rows()
    bad: list = []
    case_id_col = CODA_COLUMNS["case_id"]
    for row in rows:
        v = row.get("values", {}).get(case_id_col)
        if isinstance(v, (int, float)):
            bad.append(v)
    if bad:
        raise CodaError(
            f"Precision loss detected: {len(bad)} rows have numeric CaseIds"
        )
    log.info("Verified %d rows, no precision loss", len(rows))
    return len(rows)
