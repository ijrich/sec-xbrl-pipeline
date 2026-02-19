import json
import importlib.resources
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_CONFIG_DIR = Path(__file__).parent

_statement_type_mappings: dict[str, str] | None = None

# Matches the SEC EDGAR role definition convention: "NNNNNN - Category - Description"
_DEFINITION_RE = re.compile(r"^\d+\s*-\s*(.+?)\s*-\s*(.+)$")


def parse_role_definition(definition: str) -> tuple[str, str] | None:
    """Parse an EDGAR role definition into (category, description).

    Expects the ``NNNNNN - Category - Description`` convention.
    Returns ``None`` if the definition does not conform.
    """
    m = _DEFINITION_RE.match(definition)
    if m is None:
        return None
    return m.group(1).strip(), m.group(2).strip()


def load_statement_type_mappings() -> dict[str, str]:
    """Load description-to-statement-type mappings from the seed file.

    Returns the ``mappings`` dict (description -> statement_type).
    The result is cached at module level so the file is only read once.
    """
    global _statement_type_mappings
    if _statement_type_mappings is None:
        ref = importlib.resources.files("sec_pipeline.config").joinpath(
            "statement_type_mappings.json"
        )
        data = json.loads(ref.read_text(encoding="utf-8"))
        _statement_type_mappings = data["mappings"]
    return _statement_type_mappings


_UNMAPPED_PATH = _CONFIG_DIR / "unmapped_statement_descriptions.json"


def save_unmapped_descriptions(
    descriptions: list[str],
    cik: str | None = None,
    company_name: str | None = None,
) -> None:
    """Append unmapped statement descriptions to the unmapped JSON file.

    Each call adds one entry with the descriptions, filing metadata,
    and a timestamp.  Existing entries are preserved.
    """
    if not descriptions:
        return

    # Load existing entries
    if _UNMAPPED_PATH.exists():
        data = json.loads(_UNMAPPED_PATH.read_text(encoding="utf-8"))
    else:
        data = []

    data.append({
        "cik": cik,
        "company_name": company_name,
        "run_date": datetime.now(timezone.utc).isoformat(),
        "descriptions": descriptions,
    })

    _UNMAPPED_PATH.write_text(
        json.dumps(data, indent=2) + "\n", encoding="utf-8"
    )
    logger.info(f"Wrote {len(descriptions)} unmapped descriptions for CIK {cik}")
