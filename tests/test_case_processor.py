"""Tests for case processor utilities: dedup, date/amount parsing, PII sanitisation."""

from datetime import datetime

from scripts.case_processor import (
    dedup_distributions,
    format_amount,
    parse_amount,
    parse_date,
    sanitise_text,
)


class TestDateParsing:
    def test_paid_date_format(self):
        result = parse_date("Mar 19, 2026 02:25 PM")
        assert result == datetime(2026, 3, 19, 14, 25)

    def test_created_date_format(self):
        result = parse_date("24 Mar, 2026 00:00:00")
        assert result == datetime(2026, 3, 24, 0, 0, 0)

    def test_short_paid_format(self):
        result = parse_date("Mar 19, 2026")
        assert result == datetime(2026, 3, 19)

    def test_empty_returns_none(self):
        assert parse_date("") is None
        assert parse_date(None) is None
        assert parse_date("   ") is None

    def test_garbage_returns_none(self):
        assert parse_date("not a date") is None


class TestAmountParsing:
    def test_plain_number(self):
        assert parse_amount("800") == 800.0

    def test_with_dollar_sign(self):
        assert parse_amount("$800") == 800.0

    def test_with_commas(self):
        assert parse_amount("1,200") == 1200.0
        assert parse_amount("$1,200.50") == 1200.5

    def test_empty(self):
        assert parse_amount("") is None
        assert parse_amount(None) is None


class TestAmountFormatting:
    def test_basic(self):
        assert format_amount(800) == "$800"

    def test_thousands(self):
        assert format_amount(1200) == "$1,200"

    def test_rounds(self):
        assert format_amount(799.6) == "$800"


class TestDeduplication:
    def test_exact_duplicates_collapsed(self):
        rows = [
            {
                "paid_date": "Mar 19, 2026 02:25 PM",
                "total_amount_distributed": "800",
                "distribution_program_name": "Local Zakat",
            },
            {
                "paid_date": "Mar 19, 2026 02:25 PM",
                "total_amount_distributed": "800",
                "distribution_program_name": "Local Zakat",
            },
        ]
        assert len(dedup_distributions(rows)) == 1

    def test_different_amounts_kept(self):
        rows = [
            {
                "paid_date": "Mar 19, 2026 02:25 PM",
                "total_amount_distributed": "800",
                "distribution_program_name": "Local Zakat",
            },
            {
                "paid_date": "Mar 19, 2026 02:25 PM",
                "total_amount_distributed": "1000",
                "distribution_program_name": "Local Zakat",
            },
        ]
        assert len(dedup_distributions(rows)) == 2

    def test_different_programs_kept(self):
        rows = [
            {
                "paid_date": "Mar 19, 2026 02:25 PM",
                "total_amount_distributed": "800",
                "distribution_program_name": "Local Zakat",
            },
            {
                "paid_date": "Mar 19, 2026 02:25 PM",
                "total_amount_distributed": "800",
                "distribution_program_name": "Beyond Borders",
            },
        ]
        assert len(dedup_distributions(rows)) == 2


class TestPIISanitisation:
    def test_strips_email(self):
        text = "Contact me at jane.doe@example.com please"
        result = sanitise_text(text)
        assert "jane.doe@example.com" not in result
        assert "[redacted]" in result

    def test_strips_australian_mobile(self):
        text = "My phone is 0412 345 678 thanks"
        result = sanitise_text(text)
        assert "0412" not in result

    def test_strips_bsb(self):
        text = "BSB 062 196 account 11378252"
        result = sanitise_text(text)
        assert "062 196" not in result

    def test_preserves_normal_text(self):
        text = "I have three children and need urgent help with rent"
        assert sanitise_text(text) == text

    def test_handles_empty(self):
        assert sanitise_text("") == ""
