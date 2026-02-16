from pydantic import BaseModel, Field
from typing import Optional


class XBRLFiling(BaseModel):
    """Schema for a single XBRL filing from SEC EDGAR."""

    accession_number: str = Field(..., description="SEC accession number for the filing")
    filing_date: str = Field(..., description="Date the filing was submitted to SEC")
    report_date: Optional[str] = Field(None, description="Report period end date")
    form_type: str = Field(..., description="Form type (e.g., 10-K, 10-Q, 8-K)")
    file_number: Optional[str] = Field(None, description="SEC file number")
    film_number: Optional[str] = Field(None, description="SEC film number")
    primary_document: Optional[str] = Field(None, description="Primary document filename")
    primary_doc_description: Optional[str] = Field(None, description="Description of primary document")
    is_xbrl: bool = Field(False, description="Whether filing contains XBRL data")
    is_inline_xbrl: bool = Field(False, description="Whether filing uses Inline XBRL format")
    xbrl_instance_url: Optional[str] = Field(None, description="Direct URL to XBRL instance document (.xml)")
    primary_document_url: Optional[str] = Field(None, description="Direct URL to primary HTML document")

    class Config:
        json_schema_extra = {
            "example": {
                "accession_number": "0000320193-23-000077",
                "filing_date": "2023-08-04",
                "report_date": "2023-07-01",
                "form_type": "10-Q",
                "file_number": "001-36743",
                "film_number": "231146597",
                "primary_document": "aapl-20230701.htm",
                "primary_doc_description": "10-Q",
                "is_xbrl": True,
                "is_inline_xbrl": True,
                "xbrl_instance_url": "https://www.sec.gov/Archives/edgar/data/0000320193/000032019323000077/aapl-20230701_htm.xml",
                "primary_document_url": "https://www.sec.gov/Archives/edgar/data/0000320193/000032019323000077/aapl-20230701.htm"
            }
        }


class XBRLFilingsResponse(BaseModel):
    """Schema for the response containing XBRL filings for a company."""

    ticker: str = Field(..., description="Stock ticker symbol")
    cik: str = Field(..., description="SEC Central Index Key (CIK)")
    company_name: str = Field(..., description="Official company name")
    filings: list[XBRLFiling] = Field(..., description="List of XBRL filings")
    total_filings: int = Field(..., description="Total number of filings returned")

    # Company metadata from /submissions/CIK{cik}.json
    sic_code: Optional[str] = Field(None, description="Standard Industrial Classification code")
    sic_description: Optional[str] = Field(None, description="SIC code description")
    entity_type: Optional[str] = Field(None, description="Entity type (e.g., 'operating')")
    state_of_incorporation: Optional[str] = Field(None, description="State of incorporation abbreviation")
    fiscal_year_end: Optional[str] = Field(None, description="Fiscal year end as MMDD string")
    exchange: Optional[str] = Field(None, description="Primary exchange (e.g., 'Nasdaq', 'NYSE')")

    class Config:
        json_schema_extra = {
            "example": {
                "ticker": "AAPL",
                "cik": "0000320193",
                "company_name": "Apple Inc.",
                "filings": [
                    {
                        "accession_number": "0000320193-23-000077",
                        "filing_date": "2023-08-04",
                        "report_date": "2023-07-01",
                        "form_type": "10-Q",
                        "file_number": "001-36743",
                        "film_number": "231146597",
                        "primary_document": "aapl-20230701.htm",
                        "primary_doc_description": "10-Q"
                    }
                ],
                "total_filings": 1
            }
        }
