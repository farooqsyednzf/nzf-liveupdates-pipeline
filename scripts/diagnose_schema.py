"""
Diagnostic helper: pull a small sample from Zoho and print the schema.

Run this when message yields look wrong. It prints:
- All column names found in the export
- For each column, whether it's mostly empty or populated
- A few sample distribution rows (paid_date populated)
- A few sample application rows (paid_date empty)

PII is redacted before printing. Output goes to stdout only.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import logging
import re

from scripts.case_processor import sanitise_text
from scripts.zoho_export import _get_access_token, export_window
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


def redact_value(val: str) -> str:
    """Strip PII and truncate long values."""
    if not val:
        return ""
    cleaned = sanitise_text(str(val))
    # Strip anything that looks like a name following common phrases
    cleaned = re.sub(r"\b(my name is|i am|i'm)\s+\w+\s*\w*", "[name]", cleaned, flags=re.IGNORECASE)
    if len(cleaned) > 120:
        return cleaned[:120] + "..."
    return cleaned


def main() -> int:
    token = _get_access_token()
    today = datetime.now(timezone.utc)
    start = today - timedelta(days=3)
    rows = export_window(token, start, today)

    if not rows:
        print("No rows returned for the last 3 days. Try a wider window.")
        return 0

    print(f"\n{'='*70}")
    print(f"COLUMNS FOUND ({len(rows[0].keys())} total)")
    print(f"{'='*70}")
    columns = list(rows[0].keys())
    for col in columns:
        non_empty = sum(1 for r in rows if (r.get(col) or "").strip())
        print(f"  {col:50s}  {non_empty}/{len(rows)} populated")

    # Pick samples
    paid_rows = [r for r in rows if (r.get("paid_date") or r.get("Paid_date") or r.get("paid_Date") or "").strip()]
    unpaid_rows = [
        r
        for r in rows
        if not any((r.get(k) or "").strip() for k in ("paid_date", "Paid_date", "paid_Date"))
    ]

    print(f"\n{'='*70}")
    print(f"SAMPLE DISTRIBUTION ROWS (paid_date populated): {len(paid_rows)} found")
    print(f"{'='*70}")
    for i, row in enumerate(paid_rows[:2]):
        print(f"\n--- Distribution sample {i+1} ---")
        for k, v in row.items():
            if v and str(v).strip():
                print(f"  {k}: {redact_value(v)}")

    print(f"\n{'='*70}")
    print(f"SAMPLE APPLICATION ROWS (paid_date empty): {len(unpaid_rows)} found")
    print(f"{'='*70}")
    for i, row in enumerate(unpaid_rows[:3]):
        print(f"\n--- Application sample {i+1} ---")
        for k, v in row.items():
            if v and str(v).strip():
                print(f"  {k}: {redact_value(v)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
