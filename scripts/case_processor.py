"""
Deterministic case processing.

Groups raw Zoho rows by case ID, classifies them as distribution or application
records, applies exclusion rules, deduplicates distributions, maps suburbs to
regional descriptors, and prepares trimmed case context for the synthesiser.

This module contains zero LLM calls. Everything here is testable and rule-based.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import datetime
from typing import Iterable

from config.settings import (
    EXCLUDED_APPLICATION_STAGES,
    EXCLUDED_DESCRIPTION_TOKENS,
    EXCLUDED_DISTRIBUTION_TYPES,
    EXCLUDED_PRODUCT_CATEGORIES,
    EXCLUDED_PROGRAMS,
    GENERIC_OPENERS,
    MAX_DISTRIBUTION_AMOUNT,
    MIN_DISTRIBUTION_AMOUNT,
)

log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Location mapping
# -----------------------------------------------------------------------------

SUBURB_TO_REGION: dict[tuple[str, str], str] = {
    # NSW
    ("bankstown", "NSW"): "South Western Suburbs",
    ("liverpool", "NSW"): "South Western Suburbs",
    ("punchbowl", "NSW"): "South Western Suburbs",
    ("casula", "NSW"): "South Western Suburbs",
    ("auburn", "NSW"): "Western Suburbs",
    ("toongabbie", "NSW"): "Western Suburbs",
    ("mount druitt", "NSW"): "Western Suburbs",
    ("westmead", "NSW"): "Western Suburbs",
    ("westmesd", "NSW"): "Western Suburbs",
    ("marsfield", "NSW"): "Northern Suburbs",
    ("redfern", "NSW"): "Inner City Suburbs",
    ("figtree", "NSW"): "Illawarra Region",
    ("maryland", "NSW"): "Newcastle Region",
    # VIC
    ("carlton", "VIC"): "Inner City Suburbs",
    ("frankston", "VIC"): "Bayside Suburbs",
    ("frankstone", "VIC"): "Bayside Suburbs",
    ("hoppers crossing", "VIC"): "Western Suburbs",
    ("werribee", "VIC"): "Western Suburbs",
    ("seabrook", "VIC"): "Western Suburbs",
    ("tarneit", "VIC"): "Western Suburbs",
    ("craigieburn", "VIC"): "Northern Suburbs",
    ("craigeburn", "VIC"): "Northern Suburbs",
    ("thomastown", "VIC"): "Northern Suburbs",
    ("hadfield", "VIC"): "Northern Suburbs",
    ("campbellfield", "VIC"): "Northern Suburbs",
    ("heidelberg", "VIC"): "Northern Suburbs",
    ("preston", "VIC"): "Northern Suburbs",
    ("south morang", "VIC"): "Northern Suburbs",
    ("cranbourne", "VIC"): "South Eastern Suburbs",
    ("noble park", "VIC"): "South Eastern Suburbs",
    ("doveton", "VIC"): "South Eastern Suburbs",
    ("hampton park", "VIC"): "South Eastern Suburbs",
    ("ascot vale", "VIC"): "Inner Western Suburbs",
    ("shepparton", "VIC"): "Regional Victoria",
    # QLD
    ("oxenford", "QLD"): "Gold Coast Region",
    ("springwood", "QLD"): "Southern Suburbs",
    ("slacks creek", "QLD"): "Southern Suburbs",
    ("coopers plains", "QLD"): "Southern Suburbs",
    ("herston", "QLD"): "Inner Northern Suburbs",
    ("beaches", "QLD"): "Sunshine Coast Region",
    # SA
    ("elizabeth downs", "SA"): "Northern Suburbs",
    ("northfield", "SA"): "Northern Suburbs",
    ("adelaide", "SA"): "Adelaide / Inner Western Suburbs",
    ("croydon", "SA"): "Adelaide / Inner Western Suburbs",
    ("wingfield", "SA"): "Northern Suburbs",
    # WA
    ("booragoon", "WA"): "Southern Suburbs",
    ("winthrop", "WA"): "Southern Suburbs",
    ("willetton", "WA"): "Southern Suburbs",
    ("northbridge", "WA"): "Inner City Suburbs",
}

# Override: South Morang sometimes appears as 'SA' but is in VIC
SUBURB_OVERRIDES: dict[str, tuple[str, str]] = {
    "south morang": ("Northern Suburbs", "VIC"),
}

STATE_FALLBACK = {
    "NSW": "Sydney Region",
    "VIC": "Melbourne Region",
    "QLD": "South East Queensland",
    "SA": "Adelaide Region",
    "WA": "Perth Region",
    "TAS": "Tasmania",
    "ACT": "Australian Capital Territory",
    "NT": "Northern Territory",
}

# States where output should not repeat the abbreviation (no suburb mapping exists)
NO_REPEAT_STATE = {"TAS", "ACT", "NT"}


def map_location(suburb: str | None, state: str | None) -> str:
    """Map a (suburb, state) pair to a publication-safe location string."""
    state_clean = (state or "").strip().upper()
    suburb_clean = (suburb or "").strip().lower()

    # Hard overrides for known mislabelled suburbs
    if suburb_clean in SUBURB_OVERRIDES:
        region, real_state = SUBURB_OVERRIDES[suburb_clean]
        return f"{region}, {real_state}"

    # Direct lookup
    if suburb_clean and state_clean:
        region = SUBURB_TO_REGION.get((suburb_clean, state_clean))
        if region:
            return f"{region}, {state_clean}"

    # State-level fallback
    if state_clean in NO_REPEAT_STATE:
        return STATE_FALLBACK.get(state_clean, state_clean)
    if state_clean in STATE_FALLBACK:
        return f"{STATE_FALLBACK[state_clean]}, {state_clean}"

    # Final fallback
    return "Australia"


# -----------------------------------------------------------------------------
# Date parsing
# -----------------------------------------------------------------------------

DATE_FORMATS = (
    "%b %d, %Y %I:%M %p",   # paid_date: 'Mar 19, 2026 02:25 PM'
    "%d %b, %Y %H:%M:%S",   # case_created_dt: '24 Mar, 2026 00:00:00'
    "%b %d, %Y",
    "%d %b, %Y",
)


def parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    s = value.strip()
    if not s:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def format_publication_date(dt: datetime) -> str:
    """Format a date as '19 Mar 2026' for use in messages."""
    return dt.strftime("%-d %b %Y") if hasattr(dt, "strftime") else str(dt)


# -----------------------------------------------------------------------------
# Amount parsing
# -----------------------------------------------------------------------------


def parse_amount(value: str | None) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("$", "").replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def format_amount(amount: float) -> str:
    """Format an amount as '$800' or '$1,200' (no cents)."""
    return f"${int(round(amount)):,}"


# -----------------------------------------------------------------------------
# PII sanitisation
# -----------------------------------------------------------------------------

EMAIL_PATTERN = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")
PHONE_PATTERN = re.compile(r"\b(?:\+?61|0)\s?[2-478](?:[\s-]?\d){8}\b")
BSB_PATTERN = re.compile(r"\b\d{3}\s?-?\s?\d{3}\b")
ACCT_PATTERN = re.compile(r"\b\d{6,10}\b")


def sanitise_text(text: str) -> str:
    """Strip emails, phone numbers, and bank-like digit sequences from text."""
    if not text:
        return ""
    cleaned = EMAIL_PATTERN.sub("[redacted]", text)
    cleaned = PHONE_PATTERN.sub("[redacted]", cleaned)
    cleaned = BSB_PATTERN.sub("[redacted]", cleaned)
    cleaned = ACCT_PATTERN.sub("[redacted]", cleaned)
    return cleaned


# -----------------------------------------------------------------------------
# Classification and exclusion
# -----------------------------------------------------------------------------


def is_distribution_row(row: dict) -> bool:
    paid = (row.get("paid_date") or "").strip()
    amount = (row.get("total_amount_distributed") or "").strip()
    return bool(paid) and bool(amount)


def is_application_row(row: dict) -> bool:
    paid = (row.get("paid_date") or "").strip()
    amount = (row.get("total_amount_distributed") or "").strip()
    return not paid and not amount


def should_exclude_distribution(row: dict) -> tuple[bool, str]:
    """Return (excluded, reason). Apply all distribution exclusion rules."""
    program = (row.get("distribution_program_name") or "").strip()
    if program in EXCLUDED_PROGRAMS:
        return True, f"excluded program: {program}"

    distribution_type = (row.get("distribution_type") or "").strip()
    if distribution_type in EXCLUDED_DISTRIBUTION_TYPES:
        return True, f"excluded type: {distribution_type}"

    product_category = (row.get("product_category") or "").strip()
    if product_category in EXCLUDED_PRODUCT_CATEGORIES:
        return True, f"excluded product category: {product_category}"

    description = (row.get("description") or "").lower()
    for token in EXCLUDED_DESCRIPTION_TOKENS:
        if token in description:
            return True, f"description matches excluded token: {token}"

    amount = parse_amount(row.get("total_amount_distributed"))
    if amount is None:
        return True, "no parseable amount"
    if amount < MIN_DISTRIBUTION_AMOUNT:
        return True, f"amount below threshold: {amount}"
    if amount > MAX_DISTRIBUTION_AMOUNT:
        return True, f"amount above threshold: {amount}"

    return False, ""


def should_exclude_application(case_rows: list[dict]) -> tuple[bool, str]:
    """Return (excluded, reason). Inspect all rows for the case."""
    if not case_rows:
        return True, "no rows"
    stages = {(r.get("stage") or "").strip() for r in case_rows}
    excluded_stages = stages & EXCLUDED_APPLICATION_STAGES
    if excluded_stages:
        return True, f"excluded stage: {sorted(excluded_stages)[0]}"
    return False, ""


def has_only_generic_openers(text: str) -> bool:
    """True if the only available text is one of the known generic openers with nothing else."""
    if not text:
        return True
    cleaned = re.sub(r"\s+", " ", text.strip().lower())
    if not cleaned:
        return True
    # If the text consists almost entirely of generic phrases, skip.
    for opener in GENERIC_OPENERS:
        if cleaned.startswith(opener) and len(cleaned) < len(opener) + 40:
            return True
    return False


# -----------------------------------------------------------------------------
# Distribution dedup
# -----------------------------------------------------------------------------


def dedup_distributions(rows: Iterable[dict]) -> list[dict]:
    """Dedup by (paid_date prefix, amount, program_name)."""
    seen: dict[tuple, dict] = {}
    for row in rows:
        paid_date = (row.get("paid_date") or "")[:12].strip()
        amount = (row.get("total_amount_distributed") or "").strip()
        program = (row.get("distribution_program_name") or "").strip()
        key = (paid_date, amount, program)
        if key not in seen:
            seen[key] = row
    return list(seen.values())


# -----------------------------------------------------------------------------
# Grouping
# -----------------------------------------------------------------------------


def group_by_case(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        case_id = (row.get("case_id") or "").strip()
        if case_id:
            grouped[case_id].append(row)
    return grouped


def build_case_context(case_rows: list[dict]) -> str:
    """Concatenate description and notes for an application case, sanitised."""
    parts: list[str] = []
    seen_text: set[str] = set()
    for row in case_rows:
        description = (row.get("description") or "").strip()
        if description and description not in seen_text:
            parts.append(f"DESCRIPTION: {description}")
            seen_text.add(description)
        note = (row.get("notes") or "").strip()
        if note and note not in seen_text:
            parts.append(f"NOTE: {note}")
            seen_text.add(note)
    raw = "\n\n".join(parts)
    return sanitise_text(raw)


def build_distribution_context(case_rows: list[dict]) -> str:
    """Concatenate description and notes for a distribution case."""
    return build_case_context(case_rows)
