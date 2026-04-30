"""
Zoho Analytics export.

Pulls the website updates view in date-windowed chunks to avoid timeouts.
Returns a list of dict rows from the underlying CSV.

Environment variables required:
    ZOHO_ANALYTICS_REFRESH_TOKEN
    ZOHO_ANALYTICS_CLIENT_ID
    ZOHO_ANALYTICS_CLIENT_SECRET
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests

from config.settings import (
    EXPORT_LOOKBACK_DAYS,
    EXPORT_WINDOW_DAYS,
    ZOHO_ACCOUNTS_URL,
    ZOHO_ANALYTICS_BASE_URL,
    ZOHO_ORG_ID,
    ZOHO_VIEW_NAME,
    ZOHO_WORKSPACE_ID,
)

log = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 4
POLL_TIMEOUT_SECONDS = 300
DOWNLOAD_RETRY_LIMIT = 3


class ZohoExportError(RuntimeError):
    pass


def _get_access_token() -> str:
    """Exchange refresh token for short-lived access token."""
    refresh_token = os.environ["ZOHO_ANALYTICS_REFRESH_TOKEN"]
    client_id = os.environ["ZOHO_ANALYTICS_CLIENT_ID"]
    client_secret = os.environ["ZOHO_ANALYTICS_CLIENT_SECRET"]

    resp = requests.post(
        ZOHO_ACCOUNTS_URL,
        data={
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if "access_token" not in payload:
        raise ZohoExportError(f"No access token in response: {payload}")
    return payload["access_token"]


def _format_zoho_date(d: datetime) -> str:
    """Format date for Zoho SQL: 'Apr 06, 2026'."""
    return d.strftime("%b %d, %Y")


def _date_windows(lookback_days: int, window_days: int):
    """Yield (start, end) date pairs covering the lookback period."""
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=lookback_days)
    cursor = start
    while cursor <= today:
        window_end = min(cursor + timedelta(days=window_days - 1), today)
        yield cursor, window_end
        cursor = window_end + timedelta(days=1)


def _create_export_job(access_token: str, sql: str) -> str:
    url = (
        f"{ZOHO_ANALYTICS_BASE_URL}/workspaces/{ZOHO_WORKSPACE_ID}/data"
    )
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "ZANALYTICS-ORGID": ZOHO_ORG_ID,
    }
    config = {
        "sqlQuery": sql,
        "responseFormat": "csv",
    }
    data = {"CONFIG": json.dumps(config)}
    resp = requests.post(url, headers=headers, data=data, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    job_id = payload.get("data", {}).get("jobId")
    if not job_id:
        raise ZohoExportError(f"No jobId in response: {payload}")
    return job_id


def _poll_job(access_token: str, job_id: str) -> dict:
    url = f"{ZOHO_ANALYTICS_BASE_URL}/bulk/{job_id}"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "ZANALYTICS-ORGID": ZOHO_ORG_ID,
    }
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    while time.time() < deadline:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json().get("data", {})
        status = payload.get("jobStatus", "")
        if status == "JOB COMPLETED":
            return payload
        if status in ("JOB FAILED", "JOB CANCELLED"):
            raise ZohoExportError(f"Job {job_id} failed: {payload}")
        time.sleep(POLL_INTERVAL_SECONDS)
    raise ZohoExportError(f"Job {job_id} timed out after {POLL_TIMEOUT_SECONDS}s")


def _download_csv(access_token: str, job_id: str) -> str:
    url = f"{ZOHO_ANALYTICS_BASE_URL}/download/{job_id}"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "ZANALYTICS-ORGID": ZOHO_ORG_ID,
    }
    last_error: Exception | None = None
    for attempt in range(1, DOWNLOAD_RETRY_LIMIT + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=120)
            resp.raise_for_status()
            return resp.content.decode("utf-8-sig")
        except requests.RequestException as exc:
            last_error = exc
            log.warning("Download attempt %d failed: %s", attempt, exc)
            time.sleep(2 * attempt)
    raise ZohoExportError(f"Download failed after {DOWNLOAD_RETRY_LIMIT} attempts: {last_error}")


def _csv_to_rows(csv_text: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(csv_text))
    return [row for row in reader]


def export_window(access_token: str, start: datetime, end: datetime) -> list[dict]:
    """Export a single date window."""
    sql = (
        f'SELECT * FROM "{ZOHO_VIEW_NAME}" '
        f"WHERE case_Created_dt >= '{_format_zoho_date(start)}' "
        f"AND case_Created_dt <= '{_format_zoho_date(end)}'"
    )
    log.info("Exporting window %s to %s", start, end)
    job_id = _create_export_job(access_token, sql)
    _poll_job(access_token, job_id)
    csv_text = _download_csv(access_token, job_id)
    rows = _csv_to_rows(csv_text)
    log.info("Window %s to %s returned %d rows", start, end, len(rows))
    return rows


def export_all() -> list[dict]:
    """Run the full chunked export and return combined rows."""
    access_token = _get_access_token()
    all_rows: list[dict] = []
    for start, end in _date_windows(EXPORT_LOOKBACK_DAYS, EXPORT_WINDOW_DAYS):
        rows = export_window(
            access_token,
            datetime.combine(start, datetime.min.time()),
            datetime.combine(end, datetime.min.time()),
        )
        all_rows.extend(rows)
    log.info("Total rows across all windows: %d", len(all_rows))
    return all_rows
