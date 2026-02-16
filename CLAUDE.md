# CLAUDE.md

## Project Overview

Pip-installable SEC EDGAR XBRL parsing library. Fetches filing metadata from SEC EDGAR and parses XBRL documents using Arelle. Designed to be imported by a Modal pipeline that writes parsed data to Snowflake.

## Install

```bash
pip install -e .                   # local editable
pip install git+https://github.com/...  # from git
```

## Usage

```python
from sec_pipeline import SECAPIClient, XBRLParserService

client = SECAPIClient(user_agent_name="My App", user_agent_email="me@co.com")
parser = XBRLParserService(user_agent_name="My App", user_agent_email="me@co.com")

filings = await client.get_company_filings("MSFT")
xbrl_data = await parser.parse_xbrl_from_url(filings.filings[0].xbrl_instance_url)
# xbrl_data is a plain dict -> write to Snowflake
```

## Commands

```bash
# Run tests (Arelle needs Docker)
docker compose up -d
docker compose exec app pytest tests/ -v -s

# Run locally (fetch tests only, no Arelle)
pytest tests/test_sec_pipeline.py::TestSECFetch -v -s
```

## Structure

```
sec_pipeline/
├── __init__.py              # Top-level exports
├── core/
│   ├── __init__.py
│   └── config.py            # SEC User-Agent settings (fallback)
├── ingestion/
│   ├── __init__.py
│   ├── sec_api.py           # SECAPIClient - fetch filings from SEC EDGAR
│   ├── schemas.py           # Pydantic: XBRLFiling, XBRLFilingsResponse
│   └── sec_url_builder.py   # URL construction helpers
└── transformation/
    ├── __init__.py
    └── xbrl_parser.py       # XBRLParserService - Arelle XBRL parser
```

## Public API

| Import | Description |
|--------|-------------|
| `SECAPIClient` | Fetch filing lists from SEC EDGAR |
| `XBRLParserService` | Parse XBRL documents via Arelle |
| `XBRLFiling` | Pydantic model for a single filing |
| `XBRLFilingsResponse` | Pydantic model for company filing list |
| `build_sec_viewer_url` | Build SEC inline viewer URL |
| `build_fact_sec_urls` | Build all SEC URLs for a fact |

## Key Constraints

- **Arelle runs only in Docker** -- not installed locally. Tests that invoke the XBRL parser must run inside the Docker container.
- **SEC requires User-Agent header** -- pass `user_agent_name` and `user_agent_email` to constructors, or set `SEC_USER_AGENT_NAME` / `SEC_USER_AGENT_EMAIL` env vars.
- **Parser output is a plain dict** -- JSON-serializable, ready for Snowflake variant columns or DataFrame conversion.
