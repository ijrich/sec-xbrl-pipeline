"""
Generate statement type mappings by scraping XBRL filings for all companies
and classifying statement descriptions with Claude.

Two phases:
  1. Extract (Modal, parallel): fetch SEC submissions → parse with Arelle → extract descriptions
     Each Modal container has its own IP, so SEC rate limits don't apply.
  2. Classify + Merge (local): deduplicate → classify with Claude → merge into seed file

Usage:
    pip install modal anthropic   # one-time
    modal token new               # one-time auth
    modal run scripts/generate_mappings/generate_mappings.py
"""

from __future__ import annotations

import csv
import json
import logging
import re
import time
from pathlib import Path

import modal

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Paths are resolved lazily — only valid when running locally, not in Modal containers
def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]

def _companies_csv() -> Path:
    return _repo_root() / "companies.csv"

def _seed_file() -> Path:
    return _repo_root() / "sec_pipeline" / "config" / "statement_type_mappings.json"

# Load .env locally (not in Modal containers where dotenv isn't installed)
try:
    from dotenv import load_dotenv
    load_dotenv(_repo_root() / ".env")
except (ImportError, IndexError):
    pass

SEC_BASE_URL = "https://data.sec.gov"
SEC_ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data"

USER_AGENT = "SEC XBRL Pipeline generate_mappings@artemis.com"
SEC_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
}

# Regex from sec_pipeline/config/__init__.py — matches "NNNNNN - Category - Description"
_DEFINITION_RE = re.compile(r"^\d+\s*-\s*(.+?)\s*-\s*(.+)$")

CANONICAL_TYPES = [
    "Balance Sheet",
    "Income Statement",
    "Statement of Comprehensive Income",
    "Cash Flow Statement",
    "Statement of Stockholders' Equity",
]

# ---------------------------------------------------------------------------
# Modal image
# ---------------------------------------------------------------------------

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("gcc")
    .pip_install("arelle-release", "httpx")
)

app = modal.App("generate-mappings", image=image)

# ---------------------------------------------------------------------------
# Inlined helpers (avoids importing sec_pipeline which pulls in Arelle)
# ---------------------------------------------------------------------------


def parse_role_definition(definition: str) -> tuple[str, str] | None:
    """Parse an EDGAR role definition into (category, description)."""
    m = _DEFINITION_RE.match(definition)
    if m is None:
        return None
    return m.group(1).strip(), m.group(2).strip()


def build_xbrl_url(cik: str, accession_number: str, primary_doc: str, is_inline: bool) -> str | None:
    """Construct the XBRL instance URL from filing metadata."""
    if not primary_doc:
        return None
    accession_no_dashes = accession_number.replace("-", "")
    base_url = f"{SEC_ARCHIVES_URL}/{cik}/{accession_no_dashes}"
    if is_inline and primary_doc.endswith(".htm"):
        xbrl_filename = primary_doc.replace(".htm", "_htm.xml")
        return f"{base_url}/{xbrl_filename}"
    elif primary_doc.endswith(".xml"):
        return f"{base_url}/{primary_doc}"
    return None


# ---------------------------------------------------------------------------
# Step 1 — Read companies.csv
# ---------------------------------------------------------------------------


def load_companies() -> list[dict]:
    """Load companies from CSV, filtering out CIK=-1 entries."""
    companies = []
    with open(_companies_csv(), newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cik = row["CIK"].strip()
            if cik == "-1":
                logger.info(f"Skipping {row['TICKER']} (CIK=-1)")
                continue
            companies.append({
                "ticker": row["TICKER"].strip(),
                "cik": cik,
                "company_name": row["COMPANY_NAME"].strip(),
            })
    logger.info(f"Loaded {len(companies)} companies from {_companies_csv()}")
    return companies


# ---------------------------------------------------------------------------
# Step 2+3 — Fetch SEC submissions + parse XBRL (all in Modal, one per container)
# ---------------------------------------------------------------------------


@app.function(max_containers=50, timeout=300)
def fetch_and_parse(company: dict) -> dict:
    """
    Fetch SEC submissions, find latest 10-K/10-Q, load XBRL via Arelle
    (directly from URL so it resolves schemas), and extract statement descriptions.

    Each Modal container has its own IP, so SEC sees one request per IP — no rate limits.
    """
    import httpx
    from arelle import Cntlr, XbrlConst

    ticker = company["ticker"]
    cik = company["cik"]

    # --- Fetch SEC submissions ---
    padded_cik = cik.zfill(10)
    submissions_url = f"{SEC_BASE_URL}/submissions/CIK{padded_cik}.json"
    try:
        resp = httpx.get(submissions_url, headers=SEC_HEADERS, timeout=30, follow_redirects=True)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return {"ticker": ticker, "descriptions": [], "error": f"SEC fetch failed: {e}"}

    # --- Find most recent 10-K or 10-Q with XBRL ---
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accession_numbers = recent.get("accessionNumber", [])
    primary_documents = recent.get("primaryDocument", [])
    is_xbrl_list = recent.get("isXBRL", [])
    is_inline_xbrl_list = recent.get("isInlineXBRL", [])

    xbrl_url = None
    for i in range(len(forms)):
        if forms[i] not in ("10-K", "10-Q"):
            continue
        is_xbrl = is_xbrl_list[i] if i < len(is_xbrl_list) else False
        if not is_xbrl:
            continue
        is_inline = is_inline_xbrl_list[i] if i < len(is_inline_xbrl_list) else False
        primary_doc = primary_documents[i] if i < len(primary_documents) else None
        accession_no = accession_numbers[i]

        xbrl_url = build_xbrl_url(cik, accession_no, primary_doc, is_inline)
        if xbrl_url:
            break

    if not xbrl_url:
        return {"ticker": ticker, "descriptions": [], "error": "No 10-K/10-Q XBRL filing found"}

    # --- Load XBRL directly from URL with Arelle (resolves schemas automatically) ---
    model_xbrl = None
    try:
        controller = Cntlr.Cntlr(logFileName="logToPrint")
        controller.webCache.timeout = 60
        controller.webCache.userAgentHeader = USER_AGENT

        model_xbrl = controller.modelManager.load(xbrl_url)

        if model_xbrl is None:
            return {"ticker": ticker, "descriptions": [], "error": "Arelle returned None"}

        # Extract statement descriptions (mirrors xbrl_parser.py:551-630)
        pres_rel_set = model_xbrl.relationshipSet(XbrlConst.parentChild)
        active_roles = set()
        for rel in pres_rel_set.modelRelationships:
            active_roles.add(rel.linkrole)

        descriptions = []
        for role_uri in sorted(active_roles):
            role_types = model_xbrl.roleTypes.get(role_uri, [])
            if role_types:
                role_type = role_types[0]
                definition = role_type.definition if hasattr(role_type, "definition") else None
            else:
                definition = None

            if not definition:
                continue
            parsed = parse_role_definition(definition)
            if parsed is None:
                continue
            category, description = parsed

            if category.lower() != "statement":
                continue
            if "parenthetical" in role_uri.lower():
                continue

            descriptions.append(description)

        return {"ticker": ticker, "descriptions": list(set(descriptions)), "error": None}

    except Exception as e:
        return {"ticker": ticker, "descriptions": [], "error": f"Arelle parse failed: {e}"}
    finally:
        if model_xbrl is not None:
            model_xbrl.close()
        controller.close()


# ---------------------------------------------------------------------------
# Step 5 — Classify with Claude (local)
# ---------------------------------------------------------------------------


def classify_descriptions(descriptions: list[str]) -> dict[str, str]:
    """
    Use Claude to classify statement descriptions into canonical types.
    Returns mapping of description -> statement type.
    """
    import anthropic

    if not descriptions:
        return {}

    desc_list = "\n".join(f"- {d}" for d in sorted(descriptions))

    prompt = f"""Classify each XBRL statement description into exactly one of these canonical financial statement types:

1. Balance Sheet
2. Income Statement
3. Statement of Comprehensive Income
4. Cash Flow Statement
5. Statement of Stockholders' Equity

If a description is ambiguous or does not clearly map to one of the above, classify it as "Unclassified".

Descriptions to classify:
{desc_list}

Respond with a JSON object mapping each description (exactly as given) to its classification.
Example: {{"Consolidated Balance Sheets": "Balance Sheet", "STATEMENTS OF INCOME": "Income Statement"}}

Return ONLY the JSON object, no other text."""

    client = anthropic.Anthropic()
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=16384,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text.strip()

    # Try to parse JSON — handle potential markdown code blocks
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]  # remove opening ```json
        raw = raw.rsplit("```", 1)[0]  # remove closing ```
        raw = raw.strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        print(f"\n⚠ Claude returned invalid JSON. Raw response:\n{raw}")
        print("Please classify manually and update the seed file.")
        return {}


# ---------------------------------------------------------------------------
# Step 6 — Merge into seed file
# ---------------------------------------------------------------------------


def merge_into_seed_file(new_mappings: dict[str, str]) -> int:
    """
    Merge new classifications into the seed file.
    Skips "Unclassified" entries. Returns count of added mappings.
    """
    data = json.loads(_seed_file().read_text(encoding="utf-8"))
    existing = data["mappings"]
    added = 0

    unclassified = []
    for desc, stype in sorted(new_mappings.items()):
        if stype == "Unclassified":
            unclassified.append(desc)
            continue
        if desc not in existing:
            existing[desc] = stype
            added += 1

    # Sort keys alphabetically
    data["mappings"] = dict(sorted(existing.items()))

    _seed_file().write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    if unclassified:
        print(f"\n⚠ {len(unclassified)} descriptions classified as 'Unclassified' (not merged):")
        for d in sorted(unclassified):
            print(f"  - {d}")

    return added


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


@app.local_entrypoint()
def main():
    start = time.time()

    # Step 1: Load companies
    companies = load_companies()
    print(f"\n📋 Loaded {len(companies)} companies")

    # Step 2+3: Fetch + parse in Modal (each container has its own IP)
    print(f"🚀 Fetching SEC data and parsing XBRL for {len(companies)} companies on Modal...")
    results = list(fetch_and_parse.map(companies))

    # Step 4: Aggregate + deduplicate
    all_descriptions: set[str] = set()
    errors = []
    companies_with_data = 0

    for r in results:
        if r["error"]:
            errors.append(f"  {r['ticker']}: {r['error']}")
        if r["descriptions"]:
            companies_with_data += 1
            all_descriptions.update(r["descriptions"])

    # Load existing mappings to find what's new
    existing_data = json.loads(_seed_file().read_text(encoding="utf-8"))
    existing_mappings = existing_data["mappings"]
    new_descriptions = [d for d in sorted(all_descriptions) if d not in existing_mappings]

    print(f"\n📊 Summary:")
    print(f"  Companies with data: {companies_with_data}/{len(companies)}")
    print(f"  Total unique descriptions: {len(all_descriptions)}")
    print(f"  Already mapped: {len(all_descriptions) - len(new_descriptions)}")
    print(f"  New (unmapped): {len(new_descriptions)}")
    if errors:
        print(f"  Errors: {len(errors)}")
        for e in sorted(errors):
            print(e)

    if not new_descriptions:
        print("\n✅ All descriptions already mapped. Nothing to do.")
        return

    # Step 5: Classify with Claude
    print(f"\n🤖 Classifying {len(new_descriptions)} descriptions with Claude...")
    classifications = classify_descriptions(new_descriptions)

    if not classifications:
        print("❌ Classification failed. Exiting.")
        return

    # Step 6: Merge into seed file
    added = merge_into_seed_file(classifications)

    elapsed = time.time() - start
    print(f"\n✅ Done in {elapsed:.0f}s. Added {added} new mappings to {_seed_file().name}")
