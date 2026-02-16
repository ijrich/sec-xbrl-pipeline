"""
Ingestion Layer

External data sources and API clients for fetching financial data.
"""
from sec_pipeline.ingestion.sec_api import sec_client, SECAPIClient, get_sec_client
from sec_pipeline.ingestion.schemas import XBRLFiling, XBRLFilingsResponse
from sec_pipeline.ingestion.sec_url_builder import (
    build_sec_viewer_url,
    build_sec_document_url,
    build_sec_filing_index_url,
    build_fact_sec_urls,
)

__all__ = [
    "sec_client",
    "SECAPIClient",
    "get_sec_client",
    "XBRLFiling",
    "XBRLFilingsResponse",
    "build_sec_viewer_url",
    "build_sec_document_url",
    "build_sec_filing_index_url",
    "build_fact_sec_urls",
]
