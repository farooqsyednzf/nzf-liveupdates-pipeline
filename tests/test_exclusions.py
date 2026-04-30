"""Tests for distribution and application exclusion rules."""

from scripts.case_processor import (
    has_only_generic_openers,
    is_application_row,
    is_distribution_row,
    should_exclude_application,
    should_exclude_distribution,
)


def make_dist_row(**kwargs) -> dict:
    base = {
        "case_id": "12345",
        "paid_date": "Mar 19, 2026 02:25 PM",
        "total_amount_distributed": "800",
        "distribution_program_name": "Local Zakat",
        "distribution_type": "Zakat",
        "product_category": "Cost of Living",
        "distribution_description": "rent assistance",
    }
    base.update(kwargs)
    return base


class TestRowClassification:
    def test_distribution_row(self):
        row = make_dist_row()
        assert is_distribution_row(row)
        assert not is_application_row(row)

    def test_application_row(self):
        row = {
            "case_id": "12345",
            "paid_date": "",
            "total_amount_distributed": "",
            "case_stage": "Intake",
        }
        assert is_application_row(row)
        assert not is_distribution_row(row)


class TestDistributionExclusions:
    def test_fitr_program_excluded(self):
        row = make_dist_row(distribution_program_name="Fitr 2026")
        excluded, reason = should_exclude_distribution(row)
        assert excluded
        assert "fitr" in reason.lower()

    def test_local_fitr_excluded(self):
        row = make_dist_row(distribution_program_name="Local Fitr")
        excluded, _ = should_exclude_distribution(row)
        assert excluded

    def test_zakat_ul_fitr_type_excluded(self):
        row = make_dist_row(distribution_type="Zakat ul Fitr")
        excluded, reason = should_exclude_distribution(row)
        assert excluded
        assert "type" in reason.lower()

    def test_gaza_2023_bulk_excluded(self):
        row = make_dist_row(distribution_program_name="Gaza 2023")
        excluded, _ = should_exclude_distribution(row)
        assert excluded

    def test_individual_gaza_case_included(self):
        # Beyond Borders is fine
        row = make_dist_row(distribution_program_name="Beyond Borders")
        excluded, _ = should_exclude_distribution(row)
        assert not excluded

    def test_staff_expenses_excluded(self):
        row = make_dist_row(product_category="NZF Staff Expenses")
        excluded, _ = should_exclude_distribution(row)
        assert excluded

    def test_amount_too_low(self):
        row = make_dist_row(total_amount_distributed="25")
        excluded, _ = should_exclude_distribution(row)
        assert excluded

    def test_amount_too_high(self):
        row = make_dist_row(total_amount_distributed="20000")
        excluded, _ = should_exclude_distribution(row)
        assert excluded

    def test_amount_in_range(self):
        row = make_dist_row(total_amount_distributed="500")
        excluded, _ = should_exclude_distribution(row)
        assert not excluded

    def test_eid_gift_card_description_excluded(self):
        row = make_dist_row(distribution_description="Eid gift card distribution")
        excluded, _ = should_exclude_distribution(row)
        assert excluded


class TestApplicationExclusions:
    def test_funded_stage_excluded(self):
        rows = [{"case_stage": "Closed - Funded"}]
        excluded, _ = should_exclude_application(rows)
        assert excluded

    def test_ongoing_funding_excluded(self):
        rows = [{"case_stage": "Ongoing Funding"}]
        excluded, _ = should_exclude_application(rows)
        assert excluded

    def test_ready_for_allocation_excluded(self):
        rows = [{"case_stage": "Ready for Allocation"}]
        excluded, _ = should_exclude_application(rows)
        assert excluded

    def test_intake_stage_included(self):
        rows = [{"case_stage": "Intake"}]
        excluded, _ = should_exclude_application(rows)
        assert not excluded

    def test_nm_approval_included(self):
        rows = [{"case_stage": "NM Approval"}]
        excluded, _ = should_exclude_application(rows)
        assert not excluded


class TestGenericOpeners:
    def test_pure_generic_opener(self):
        text = "I am writing to request financial assistance"
        assert has_only_generic_openers(text)

    def test_generic_opener_with_real_content(self):
        text = (
            "I am writing to request financial assistance. "
            "I am a single mother of three with no income after fleeing domestic violence."
        )
        assert not has_only_generic_openers(text)

    def test_im_in_need_alone(self):
        text = "I'm in need please help"
        assert has_only_generic_openers(text)

    def test_empty_text(self):
        assert has_only_generic_openers("")
        assert has_only_generic_openers(None)
