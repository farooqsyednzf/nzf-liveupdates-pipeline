"""
Configuration constants for the NZF website updates pipeline.

Tweak these values rather than editing logic in scripts/.
"""

from datetime import timedelta

# Zoho Analytics
ZOHO_ORG_ID = "668395719"
ZOHO_WORKSPACE_ID = "1715382000001002475"
ZOHO_VIEW_NAME = "Cases x Distribution x Notes - Website Updates"
ZOHO_ANALYTICS_BASE_URL = "https://analyticsapi.zoho.com/restapi/v2"
ZOHO_ACCOUNTS_URL = "https://accounts.zoho.com/oauth/v2/token"

# Date window for the export. We pull the trailing N days, chunked into
# windows of EXPORT_WINDOW_DAYS to avoid MCP/API timeouts.
EXPORT_LOOKBACK_DAYS = 14
EXPORT_WINDOW_DAYS = 3

# Coda
CODA_DOC_ID = "M0mdlJE1S_"
CODA_TABLE_ID = "grid-dnfb1xzIxr"
CODA_BASE_URL = "https://coda.io/apis/v1"
CODA_COLUMNS = {
    "published_date": "c-8EvMdV6Yr0",
    "application_type": "c-EoHbRlCmEC",
    "message": "c-FlsQysJ9c-",
    "case_id": "c-Js9J6GT30Z",
    "application_date": "c-zSiWmtwMEG",
    "suburb": "c-_L2OiTrKne",
    "state": "c-ih7JhvmAAY",
}

# ZWNJ prefix to stop Coda's float64 truncation of 18-digit IDs
ZWNJ = "\u200c"

# Anthropic
ANTHROPIC_MODEL = "claude-sonnet-4-5"
ANTHROPIC_MAX_TOKENS = 200

# Pipeline thresholds
TARGET_DISTRIBUTIONS = 50
TARGET_APPLICATIONS = 35
MIN_APPLICATIONS = 10  # Below this, fail the run
MIN_MESSAGE_LENGTH = 80
MAX_MESSAGE_LENGTH = 165

# Distribution amount thresholds
MIN_DISTRIBUTION_AMOUNT = 50
MAX_DISTRIBUTION_AMOUNT = 15000

# Excluded program names (Fitr / bulk / Gaza bulk)
EXCLUDED_PROGRAMS = {
    "Fitr 2026",
    "Local Fitr",
    "Fitr Program 2026",
    "Gaza 2023",
    "BHB Food Bank",
}
EXCLUDED_DESCRIPTION_TOKENS = {
    "fitr distribution",
    "fitr 2026",
    "eid gift card",
}
EXCLUDED_DISTRIBUTION_TYPES = {
    "Zakat ul Fitr",
}
EXCLUDED_PRODUCT_CATEGORIES = {
    "NZF Staff Expenses",
}

# Application stages to exclude
EXCLUDED_APPLICATION_STAGES = {
    "Funding",
    "Ongoing Funding",
    "Closed - Funded",
    "Closed - Declined",
    "Post Funding",
    "Ready for Allocation",
}

# Coda async timing (in seconds)
CODA_DELETE_BATCH_SIZE = 100
CODA_DELETE_POLL_INTERVAL = 3
CODA_DELETE_BUFFER_AFTER_EMPTY = 10
CODA_INSERT_BATCH_SIZE = 5
CODA_INSERT_BATCH_SLEEP = 1.2
CODA_INSERT_VERIFY_DELAY = 8
CODA_INSERT_MAX_RETRIES = 4
CODA_INSERT_BACKOFF_BASE = 4

# Generic phrases that on their own are not enough to base a message on
GENERIC_OPENERS = [
    "i hope this message finds you well",
    "i am writing to request",
    "i am writing to apply",
    "i'm in need please help",
    "please help me",
    "asking for help",
]
