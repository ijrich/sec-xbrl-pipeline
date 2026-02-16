"""
SEC URL Builder Service

Builds URLs to SEC EDGAR filing documents and the inline XBRL viewer
with optional anchor links to specific facts.
"""
from typing import Optional


def build_sec_viewer_url(
    cik: str,
    accession_number: str,
    fact_anchor_id: Optional[str] = None
) -> str:
    """
    Build URL to SEC's inline XBRL viewer with optional anchor to specific fact.

    This is the recommended way to link to SEC filings as it provides
    the interactive XBRL viewer experience.

    Args:
        cik: Company CIK (e.g., "0000320193")
        accession_number: Filing accession number with dashes (e.g., "0000320193-25-000073")
        fact_anchor_id: Optional HTML anchor ID for specific fact (e.g., "fact-identifier-12345")

    Returns:
        URL to SEC viewer, optionally with anchor to specific fact

    Example:
        >>> build_sec_viewer_url("0000320193", "0000320193-25-000073", "fact-123")
        'https://www.sec.gov/cgi-bin/viewer?action=view&cik=0000320193&accession_number=0000320193-25-000073&xbrl_type=v#fact-123'
    """
    # Ensure CIK is 10 digits with leading zeros
    cik_padded = str(cik).zfill(10)

    # Build base viewer URL
    url = (
        f"https://www.sec.gov/cgi-bin/viewer"
        f"?action=view"
        f"&cik={cik_padded}"
        f"&accession_number={accession_number}"
        f"&xbrl_type=v"
    )

    # Add anchor if provided
    if fact_anchor_id:
        url += f"#{fact_anchor_id}"

    return url


def build_sec_document_url(
    cik: str,
    accession_number: str,
    primary_document: str,
    fact_anchor_id: Optional[str] = None
) -> str:
    """
    Build URL to raw HTML filing document with optional anchor to specific fact.

    This provides direct access to the HTML filing without the XBRL viewer.
    Useful as a fallback or for direct document access.

    Args:
        cik: Company CIK (e.g., "0000320193")
        accession_number: Filing accession number with dashes (e.g., "0000320193-25-000073")
        primary_document: Primary HTML filename (e.g., "aapl-20250628.htm")
        fact_anchor_id: Optional HTML anchor ID for specific fact

    Returns:
        URL to raw HTML document, optionally with anchor to specific fact

    Example:
        >>> build_sec_document_url("0000320193", "0000320193-25-000073", "aapl-20250628.htm", "fact-123")
        'https://www.sec.gov/Archives/edgar/data/0000320193/000032019325000073/aapl-20250628.htm#fact-123'
    """
    # Remove dashes from accession number for file path
    accession_no_clean = accession_number.replace("-", "")

    # Ensure CIK is 10 digits with leading zeros
    cik_padded = str(cik).zfill(10)

    # Build document URL
    url = (
        f"https://www.sec.gov/Archives/edgar/data/"
        f"{cik_padded}/{accession_no_clean}/{primary_document}"
    )

    # Add anchor if provided
    if fact_anchor_id:
        url += f"#{fact_anchor_id}"

    return url


def build_sec_filing_index_url(cik: str, accession_number: str) -> str:
    """
    Build URL to SEC filing index page (shows all documents in the filing).

    Args:
        cik: Company CIK (e.g., "0000320193")
        accession_number: Filing accession number with dashes (e.g., "0000320193-25-000073")

    Returns:
        URL to filing index page

    Example:
        >>> build_sec_filing_index_url("0000320193", "0000320193-25-000073")
        'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193&type=&dateb=&owner=exclude&count=100&search_text='
    """
    # Remove dashes from accession number for file path
    accession_no_clean = accession_number.replace("-", "")

    # Ensure CIK is 10 digits with leading zeros
    cik_padded = str(cik).zfill(10)

    # Build index URL
    url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany"
        f"&CIK={cik_padded}"
        f"&type=&dateb=&owner=exclude&count=100"
    )

    return url


def build_fact_sec_urls(
    cik: str,
    accession_number: str,
    primary_document: Optional[str] = None,
    html_anchor_id: Optional[str] = None,
    concept_label: Optional[str] = None
) -> dict:
    """
    Build all SEC URLs for a fact in one call.

    Note: XBRL anchor IDs may link to statement sections rather than
    specific fact locations. Users may need to search within the document
    using Ctrl+F to find specific values.

    Args:
        cik: Company CIK
        accession_number: Filing accession number with dashes
        primary_document: Optional primary HTML filename
        html_anchor_id: Optional HTML anchor ID (may not pinpoint exact location)
        concept_label: Optional concept label for search hints

    Returns:
        Dictionary with SEC URLs and usage guidance

    Example:
        >>> urls = build_fact_sec_urls(
        ...     "0000320193",
        ...     "0000320193-25-000073",
        ...     "aapl-20250628.htm",
        ...     "fact-123",
        ...     "Cash and Cash Equivalents"
        ... )
        >>> urls['viewer_url']
        'https://www.sec.gov/cgi-bin/viewer?...'
        >>> urls['search_hint']
        'Search for: Cash and Cash Equivalents'
    """
    urls = {
        "viewer_url": build_sec_viewer_url(cik, accession_number, html_anchor_id),
        "filing_index_url": build_sec_filing_index_url(cik, accession_number),
        "note": (
            "Anchor link may show statement section, not exact cell. "
            "Use browser search (Ctrl+F) to find specific values."
        )
    }

    # Only include document URL if we have the primary document filename
    if primary_document:
        urls["document_url"] = build_sec_document_url(
            cik, accession_number, primary_document, html_anchor_id
        )

    # Add search hint if concept label is provided
    if concept_label:
        urls["search_hint"] = f"Search for: {concept_label}"

    return urls
