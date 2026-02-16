import httpx
import logging
from typing import Optional
from sec_pipeline.ingestion.schemas import XBRLFiling, XBRLFilingsResponse

logger = logging.getLogger(__name__)


class SECAPIClient:
    """Client for interacting with SEC EDGAR API."""

    BASE_URL = "https://data.sec.gov"
    COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

    def __init__(
        self,
        user_agent_name: str | None = None,
        user_agent_email: str | None = None,
    ):
        # Accept explicit values; fall back to settings (which reads env vars / .env)
        if user_agent_name is None or user_agent_email is None:
            from sec_pipeline.core.config import settings
            user_agent_name = user_agent_name or settings.SEC_USER_AGENT_NAME
            user_agent_email = user_agent_email or settings.SEC_USER_AGENT_EMAIL

        # SEC requires a User-Agent header with valid contact information
        user_agent = f"{user_agent_name} {user_agent_email}"
        self.headers = {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate"
        }
        logger.info(f"Initialized SEC API client with User-Agent: {user_agent}")

    async def get_company_cik(self, ticker: str) -> Optional[dict]:
        """
        Get company CIK and name from ticker symbol.

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')

        Returns:
            Dict with CIK and company name, or None if not found
        """
        url = f"{self.BASE_URL}/submissions/CIK{ticker.upper()}.json"

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=self.headers)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError:
                # Try ticker lookup file
                return await self._lookup_ticker(ticker)

    async def _lookup_ticker(self, ticker: str) -> Optional[dict]:
        """
        Lookup CIK from ticker using SEC's company tickers JSON.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Dict with CIK and company name, or None if not found
        """
        url = self.COMPANY_TICKERS_URL

        async with httpx.AsyncClient() as client:
            try:
                logger.info(f"Fetching SEC company tickers from: {url}")
                logger.info(f"Using headers: {self.headers}")
                response = await client.get(url, headers=self.headers)
                logger.info(f"Response status: {response.status_code}")
                response.raise_for_status()
                data = response.json()
                logger.info(f"Successfully fetched {len(data)} companies from SEC")

                # Search for ticker in the data
                for key, company in data.items():
                    if company.get("ticker", "").upper() == ticker.upper():
                        cik = str(company["cik_str"]).zfill(10)
                        logger.info(f"Found ticker {ticker}: CIK={cik}, Name={company['title']}")
                        return {
                            "cik": cik,
                            "name": company["title"]
                        }

                logger.warning(f"Ticker '{ticker}' not found in {len(data)} SEC companies")
                return None
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error looking up ticker '{ticker}': {e.response.status_code} - {e.response.text}")
                return None
            except Exception as e:
                logger.error(f"Unexpected error looking up ticker '{ticker}': {type(e).__name__}: {str(e)}")
                return None

    async def get_company_filings(self, ticker: str) -> XBRLFilingsResponse:
        """
        Get all XBRL filings for a given ticker.

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')

        Returns:
            XBRLFilingsResponse with company info and filings

        Raises:
            ValueError: If ticker not found
            httpx.HTTPError: If API request fails
        """
        # First, get company CIK
        company_info = await self._lookup_ticker(ticker)

        if not company_info:
            raise ValueError(f"Ticker '{ticker}' not found in SEC database")

        cik = company_info["cik"]
        company_name = company_info["name"]

        # Get company submissions
        url = f"{self.BASE_URL}/submissions/CIK{cik}.json"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()

        # Extract company metadata
        sic_code = data.get("sic")
        sic_description = data.get("sicDescription")
        entity_type = data.get("entityType")
        state_of_incorporation = data.get("stateOfIncorporation")
        fiscal_year_end = data.get("fiscalYearEnd")

        # Tickers/exchanges are arrays; take first if available
        tickers = data.get("tickers", [])
        exchanges = data.get("exchanges", [])
        exchange = exchanges[0] if exchanges else None

        # Extract only XBRL filings
        recent_filings = data.get("filings", {}).get("recent", {})

        filings = []

        forms = recent_filings.get("form", [])
        filing_dates = recent_filings.get("filingDate", [])
        accession_numbers = recent_filings.get("accessionNumber", [])
        report_dates = recent_filings.get("reportDate", [])
        file_numbers = recent_filings.get("fileNumber", [])
        film_numbers = recent_filings.get("filmNumber", [])
        primary_documents = recent_filings.get("primaryDocument", [])
        primary_doc_descriptions = recent_filings.get("primaryDocDescription", [])
        is_xbrl_list = recent_filings.get("isXBRL", [])
        is_inline_xbrl_list = recent_filings.get("isInlineXBRL", [])

        for i in range(len(forms)):
            # Only include filings that have XBRL data
            is_xbrl = is_xbrl_list[i] if i < len(is_xbrl_list) else False

            if is_xbrl:
                is_inline = is_inline_xbrl_list[i] if i < len(is_inline_xbrl_list) else False
                primary_doc = primary_documents[i] if i < len(primary_documents) else None
                accession_no = accession_numbers[i]
                accession_no_dashes = accession_no.replace("-", "")

                # Construct URLs
                xbrl_instance_url = None
                primary_doc_url = None

                if primary_doc:
                    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}"
                    primary_doc_url = f"{base_url}/{primary_doc}"

                    # Construct XBRL instance URL
                    if is_inline and primary_doc.endswith(".htm"):
                        # Inline XBRL: append _htm.xml to the primary document name
                        xbrl_filename = primary_doc.replace(".htm", "_htm.xml")
                        xbrl_instance_url = f"{base_url}/{xbrl_filename}"
                    elif primary_doc.endswith(".xml"):
                        # Traditional XBRL instance document
                        xbrl_instance_url = primary_doc_url

                filing = XBRLFiling(
                    accession_number=accession_no,
                    filing_date=filing_dates[i],
                    report_date=report_dates[i] if i < len(report_dates) else None,
                    form_type=forms[i],
                    file_number=file_numbers[i] if i < len(file_numbers) else None,
                    film_number=film_numbers[i] if i < len(film_numbers) else None,
                    primary_document=primary_doc,
                    primary_doc_description=primary_doc_descriptions[i] if i < len(primary_doc_descriptions) else None,
                    is_xbrl=is_xbrl,
                    is_inline_xbrl=is_inline,
                    xbrl_instance_url=xbrl_instance_url,
                    primary_document_url=primary_doc_url,
                )
                filings.append(filing)

        return XBRLFilingsResponse(
            ticker=ticker.upper(),
            cik=cik,
            company_name=company_name,
            filings=filings,
            total_filings=len(filings),
            sic_code=sic_code,
            sic_description=sic_description,
            entity_type=entity_type,
            state_of_incorporation=state_of_incorporation,
            fiscal_year_end=fiscal_year_end,
            exchange=exchange,
        )


_sec_client: SECAPIClient | None = None


def get_sec_client(**kwargs) -> SECAPIClient:
    """Get or create the SEC API client singleton."""
    global _sec_client
    if _sec_client is None:
        _sec_client = SECAPIClient(**kwargs)
    return _sec_client


class _LazyClient:
    """Proxy that defers SECAPIClient construction until first use."""
    def __getattr__(self, name):
        return getattr(get_sec_client(), name)


sec_client = _LazyClient()
