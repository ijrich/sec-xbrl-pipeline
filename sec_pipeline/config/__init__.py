1import logging
import re

logger = logging.getLogger(__name__)

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
