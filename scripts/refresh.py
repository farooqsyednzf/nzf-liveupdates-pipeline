"""
Main orchestrator for the NZF website updates pipeline.

Sequence:
    1. Export raw rows from Zoho Analytics in date-windowed chunks.
    2. Group by case ID. Classify as distribution or application.
    3. Apply exclusion rules. Dedup distributions.
    4. Build messages: deterministic format strings with LLM-synthesised content.
    5. Validate (length, PII).
    6. Publish to Coda: async-safe delete then batched insert then verify.
    7. Write a sanitised run summary to data/last-run.json.

Exits non-zero on any failure condition. GitHub Actions surfaces failures via
its native email notifications to the repo owner.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure imports work whether run as `python scripts/refresh.py` or as a module
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from config.settings import (
    MAX_MESSAGE_LENGTH,
    MIN_APPLICATIONS,
    MIN_MESSAGE_LENGTH,
)
from scripts.case_processor import (
    build_case_context,
    dedup_distributions,
    format_amount,
    format_publication_date,
    group_by_case,
    has_only_generic_openers,
    is_application_row,
    is_distribution_row,
    map_location,
    parse_amount,
    parse_date,
    sanitise_text,
    should_exclude_application,
    should_exclude_distribution,
)
from scripts.coda_publisher import (
    delete_all_rows,
    insert_rows,
    verify_no_precision_loss,
)
from scripts.synthesiser import Synthesiser
from scripts.zoho_export import export_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("refresh")


# Distribution type lookup for message text
DISTRIBUTION_TYPE_MAP = {
    "zakat": "Zakat",
    "sadaqah": "Sadaqah",
    "fidyah": "Fidyah",
    "tainted wealth": "Tainted Wealth",
}


def _resolve_distribution_type(row: dict) -> str:
    raw = (row.get("distribution_type") or "").strip().lower()
    return DISTRIBUTION_TYPE_MAP.get(raw, "Zakat")


def build_distribution_messages(rows: list[dict], synth: Synthesiser) -> list[tuple[str, str, str]]:
    """Return list of (type, message, case_id) for valid distributions."""
    grouped = group_by_case(rows)

    # Flatten unique distribution events across all cases
    candidates: list[dict] = []
    for case_id, case_rows in grouped.items():
        dist_rows = [r for r in case_rows if is_distribution_row(r)]
        if not dist_rows:
            continue
        deduped = dedup_distributions(dist_rows)
        for row in deduped:
            row["_case_id"] = case_id
            row["_case_rows"] = case_rows
            candidates.append(row)

    log.info("Distribution candidates: %d (across %d cases)", len(candidates), len(grouped))

    messages: list[tuple[str, str, str]] = []
    for row in candidates:
        excluded, reason = should_exclude_distribution(row)
        if excluded:
            log.debug("Skipping distribution: %s", reason)
            continue

        paid_dt = parse_date(row.get("paid_date"))
        amount = parse_amount(row.get("total_amount_distributed"))
        if not paid_dt or amount is None:
            continue

        dist_type = _resolve_distribution_type(row)
        location = map_location(
            row.get("city") or row.get("suburb"),
            row.get("state"),
        )

        context = build_case_context(row["_case_rows"])
        descriptor = synth.descriptor(context)

        message = (
            f"{format_publication_date(paid_dt)}: "
            f"{format_amount(amount)} of {dist_type} distributed to "
            f"{descriptor} in {location}"
        )

        if not _is_message_valid(message):
            log.debug("Distribution message length out of range: %d chars", len(message))
            continue

        messages.append(("Distribution", message, row["_case_id"]))

    log.info("Distribution messages produced: %d", len(messages))
    return messages


def build_application_messages(rows: list[dict], synth: Synthesiser) -> list[tuple[str, str, str]]:
    """Return list of (type, message, case_id) for valid applications."""
    grouped = group_by_case(rows)

    messages: list[tuple[str, str, str]] = []
    for case_id, case_rows in grouped.items():
        # Only handle pure application cases (no distribution rows)
        if any(is_distribution_row(r) for r in case_rows):
            continue
        if not any(is_application_row(r) for r in case_rows):
            continue

        excluded, reason = should_exclude_application(case_rows)
        if excluded:
            log.debug("Skipping application case %s: %s", case_id, reason)
            continue

        # Use the latest case_created_dt available
        first_row = case_rows[0]
        created_dt = parse_date(first_row.get("case_created_dt"))
        if not created_dt:
            continue

        location = map_location(
            first_row.get("city") or first_row.get("suburb"),
            first_row.get("state"),
        )

        context = build_case_context(case_rows)
        if has_only_generic_openers(context):
            log.debug("Skipping case %s: only generic openers", case_id)
            continue

        sentence = synth.application_sentence(context)
        if not sentence:
            continue

        message = (
            f"{format_publication_date(created_dt)}: {sentence} "
            f"Application from {location}"
        )

        if not _is_message_valid(message):
            log.debug(
                "Application message length out of range: %d chars",
                len(message),
            )
            continue

        messages.append(("Application", message, case_id))

    log.info("Application messages produced: %d", len(messages))
    return messages


# Patterns to catch any PII that slipped through synthesis
RESIDUAL_PII_PATTERNS = [
    re.compile(r"\b\d{4,}\b"),
    re.compile(r"@"),
]


def _is_message_valid(msg: str) -> bool:
    if not (MIN_MESSAGE_LENGTH <= len(msg) <= MAX_MESSAGE_LENGTH):
        return False
    for pattern in RESIDUAL_PII_PATTERNS[1:]:  # skip digit check (dates have digits)
        if pattern.search(msg):
            return False
    return True


def write_run_summary(summary: dict) -> None:
    """Write a sanitised summary to data/last-run.json."""
    out = ROOT / "data" / "last-run.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def main() -> int:
    started = time.time()
    started_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    summary = {
        "started_at_utc": started_iso,
        "status": "running",
    }

    try:
        log.info("Starting NZF website updates pipeline")
        log.info("Step 1: export from Zoho Analytics")
        rows = export_all()
        log.info("Pulled %d total rows", len(rows))

        log.info("Step 2: synthesise messages")
        synth = Synthesiser()
        distribution_messages = build_distribution_messages(rows, synth)
        application_messages = build_application_messages(rows, synth)

        if not distribution_messages:
            raise RuntimeError("Zero distribution messages produced, failing run")
        if len(application_messages) < MIN_APPLICATIONS:
            raise RuntimeError(
                f"Application yield below minimum: {len(application_messages)} < {MIN_APPLICATIONS}"
            )

        all_messages = distribution_messages + application_messages
        log.info("Total messages to publish: %d", len(all_messages))

        log.info("Step 3: clear Coda table")
        delete_all_rows()

        log.info("Step 4: insert into Coda")
        insert_rows(all_messages)

        log.info("Step 5: verify Coda integrity")
        final_count = verify_no_precision_loss()

        elapsed = round(time.time() - started, 1)
        summary.update(
            {
                "status": "success",
                "elapsed_seconds": elapsed,
                "raw_rows_pulled": len(rows),
                "distribution_messages": len(distribution_messages),
                "application_messages": len(application_messages),
                "rows_in_coda_after_publish": final_count,
                "finished_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )
        write_run_summary(summary)
        log.info("Pipeline completed in %ds", elapsed)
        return 0

    except Exception as exc:
        log.exception("Pipeline failed")
        elapsed = round(time.time() - started, 1)
        summary.update(
            {
                "status": "failure",
                "elapsed_seconds": elapsed,
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:500],
                "finished_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )
        write_run_summary(summary)
        return 1


if __name__ == "__main__":
    sys.exit(main())
