"""Tests for suburb to regional descriptor mapping."""

from scripts.case_processor import map_location


class TestKnownSuburbs:
    def test_bankstown_nsw(self):
        assert map_location("Bankstown", "NSW") == "South Western Suburbs, NSW"

    def test_auburn_nsw(self):
        assert map_location("Auburn", "NSW") == "Western Suburbs, NSW"

    def test_carlton_vic(self):
        assert map_location("Carlton", "VIC") == "Inner City Suburbs, VIC"

    def test_werribee_vic(self):
        assert map_location("Werribee", "VIC") == "Western Suburbs, VIC"

    def test_oxenford_qld(self):
        assert map_location("Oxenford", "QLD") == "Gold Coast Region, QLD"

    def test_northbridge_wa(self):
        assert map_location("Northbridge", "WA") == "Inner City Suburbs, WA"


class TestSpellingVariants:
    def test_frankston_typo(self):
        assert map_location("Frankstone", "VIC") == "Bayside Suburbs, VIC"

    def test_craigieburn_typo(self):
        assert map_location("Craigeburn", "VIC") == "Northern Suburbs, VIC"

    def test_westmead_typo(self):
        assert map_location("Westmesd", "NSW") == "Western Suburbs, NSW"


class TestStateOverrides:
    def test_south_morang_marked_as_sa_returns_vic(self):
        # South Morang sometimes appears in dataset as SA but is actually VIC
        result = map_location("South Morang", "SA")
        assert result == "Northern Suburbs, VIC"


class TestStateFallbacks:
    def test_unknown_suburb_nsw(self):
        assert map_location("Random Suburb", "NSW") == "Sydney Region, NSW"

    def test_unknown_suburb_vic(self):
        assert map_location("Unknown Place", "VIC") == "Melbourne Region, VIC"

    def test_tasmania_no_repeat(self):
        # TAS, ACT, NT should not repeat the abbreviation
        result = map_location("Hobart", "TAS")
        assert result == "Tasmania"
        assert "TAS, TAS" not in result

    def test_act_no_repeat(self):
        result = map_location("Canberra", "ACT")
        assert result == "Australian Capital Territory"


class TestEdgeCases:
    def test_empty_inputs(self):
        assert map_location("", "") == "Australia"

    def test_none_inputs(self):
        assert map_location(None, None) == "Australia"

    def test_none_city_with_state(self):
        # 'NONE' city WA case mentioned in spec
        assert map_location("NONE", "WA") == "Perth Region, WA"

    def test_case_insensitive(self):
        assert map_location("BANKSTOWN", "NSW") == "South Western Suburbs, NSW"
        assert map_location("bankstown", "nsw") == "South Western Suburbs, NSW"
