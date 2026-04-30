# NZF Website Updates Pipeline

Daily automated job that pulls case data from Zoho Analytics, generates anonymised website update messages for Distributions and Applications, and publishes them to the Coda table that feeds the public NZF website.

## Owner

Farooq Syed, NZF Australia.

## What it does

Runs once per day. Exports the `Cases x Distribution x Notes - Website Updates` view from Zoho Analytics, applies NZF's exclusion rules, deduplicates distribution records, maps suburbs to regional descriptors, and uses Claude (Sonnet) to synthesise empathetic descriptors for distributions and one-sentence summaries for applications. Publishes the result to Coda, replacing the previous day's rows.

Output: typically 50 distribution messages and 20 to 35 application messages per run.

## Hosted run

GitHub Actions, in this repo. There is no web frontend. Output goes to Coda.

Coda doc: <https://coda.io/d/M0mdlJE1S_>
Coda table: `grid-dnfb1xzIxr`

## Refresh schedule

Daily at 21:30 UTC (`30 21 * * *`), which is 7:30 AM AEST (UTC+10).

Note: during Australian Eastern Daylight Time (early October to early April), Melbourne local time will be 8:30 AM. This is intentional. The cron is locked to literal AEST.

Manual trigger: any maintainer can re-run via the Actions tab using `workflow_dispatch`.

## Local dev

```bash
git clone https://github.com/farooqsyednzf/nzf-website-updates-pipeline.git
cd nzf-website-updates-pipeline
python -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements.txt
```

Copy `.env.example` to `.env` and populate with credentials. Never commit `.env`.

Run locally:

```bash
python scripts/refresh.py
```

Run tests:

```bash
pytest tests/
```

## Required GitHub Actions secrets

| Secret | Source |
|---|---|
| `ZOHO_ANALYTICS_REFRESH_TOKEN` | Zoho API console |
| `ZOHO_ANALYTICS_CLIENT_ID` | Zoho API console |
| `ZOHO_ANALYTICS_CLIENT_SECRET` | Zoho API console |
| `CODA_API_TOKEN` | Coda account settings |
| `ANTHROPIC_API_KEY` | Anthropic console |

## Failure handling

The action exits non-zero on any of:

- Zoho export failure after retries
- Zero distribution messages produced
- Application yield below 10
- Coda verify finding residual numeric CaseId values
- Any uncaught exception

Failed runs trigger GitHub's standard email notification to the repo owner. Make sure your GitHub notification settings have Actions failures enabled.

Each run also commits `data/last-run.json` with sanitised counts and timings (no case content).

## Architecture

```
Zoho Analytics  -->  GitHub Actions runner  -->  Coda
                       |
                       +--  Anthropic API (Claude Sonnet) for synthesis
                       +--  data/last-run.json (committed audit log)
```

No browser, no exposed API keys, no live calls from public surfaces. Coda is the only destination.

## Files

```
.github/workflows/publish-website-updates.yml   scheduled job
scripts/refresh.py                              orchestrator
scripts/zoho_export.py                          chunked SQL export
scripts/case_processor.py                       dedup, exclusions, location
scripts/synthesiser.py                          Claude API calls
scripts/coda_publisher.py                       async-safe Coda push
prompts/distribution_descriptor_v1.md           prompt for descriptors
prompts/application_synthesis_v1.md             prompt for app summaries
config/settings.py                              tuneable config
tests/                                          unit tests for deterministic logic
data/last-run.json                              committed run summary
```

## Contact

Farooq Syed, NZF Australia. <farooq.syed@nzf.org.au>
