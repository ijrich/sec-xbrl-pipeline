"""
Tests for the SEC pipeline core — the importable surface that Modal uses.

Tests the fetch → parse pipeline and validates output structure matches
what's needed for Snowflake ingestion.

NOTE: XBRL parsing tests require Arelle (run inside Docker):
    docker compose exec app pytest tests/test_sec_pipeline.py -v -s
"""
import pytest
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. SEC API Fetch Tests (no Arelle needed, runs anywhere with network)
# ---------------------------------------------------------------------------

class TestSECFetch:
    """Test SEC EDGAR API client — fetches filing metadata."""

    @pytest.fixture
    def sec_client(self):
        from sec_pipeline import SECAPIClient
        return SECAPIClient(
            user_agent_name="SEC Pipeline Tests",
            user_agent_email="test@example.com",
        )

    async def test_fetch_microsoft_filings(self, sec_client):
        """Fetch MSFT filings and validate the Pydantic response model."""
        from sec_pipeline import XBRLFilingsResponse, XBRLFiling

        response = await sec_client.get_company_filings("MSFT")

        # Validate it's the right Pydantic model
        assert isinstance(response, XBRLFilingsResponse)
        assert response.ticker == "MSFT"
        assert response.cik is not None
        assert response.company_name is not None
        assert response.total_filings > 0

        # Company metadata populated
        assert response.sic_code is not None
        assert response.exchange is not None

        logger.info(f"Fetched {response.total_filings} XBRL filings for {response.company_name}")

    async def test_filings_have_xbrl_urls(self, sec_client):
        """Verify at least some filings have XBRL instance URLs."""
        response = await sec_client.get_company_filings("MSFT")

        filings_with_xbrl = [f for f in response.filings if f.xbrl_instance_url]
        assert len(filings_with_xbrl) > 0, "No filings have XBRL instance URLs"

        # Grab the most recent 10-Q
        ten_qs = [f for f in filings_with_xbrl if f.form_type == "10-Q"]
        assert len(ten_qs) > 0, "No 10-Q filings found"

        latest_10q = ten_qs[0]
        assert latest_10q.is_xbrl is True
        assert latest_10q.xbrl_instance_url.endswith(".xml")
        assert latest_10q.accession_number is not None
        assert latest_10q.filing_date is not None

        logger.info(f"Latest 10-Q: {latest_10q.accession_number} filed {latest_10q.filing_date}")
        logger.info(f"  XBRL URL: {latest_10q.xbrl_instance_url}")

    async def test_filing_schema_fields(self, sec_client):
        """Validate all expected fields on XBRLFiling are populated."""
        response = await sec_client.get_company_filings("MSFT")

        filing = response.filings[0]
        assert filing.accession_number is not None
        assert filing.filing_date is not None
        assert filing.form_type is not None
        assert filing.is_xbrl is True

        # Verify the model can serialize cleanly (this is what goes to Snowflake)
        filing_dict = filing.model_dump()
        assert "accession_number" in filing_dict
        assert "filing_date" in filing_dict
        assert "xbrl_instance_url" in filing_dict
        logger.info(f"Filing model_dump keys: {sorted(filing_dict.keys())}")


# ---------------------------------------------------------------------------
# 2. XBRL Parse Tests (requires Arelle — run inside Docker)
# ---------------------------------------------------------------------------

class TestXBRLParse:
    """Test XBRL parsing via Arelle — validates output data structure.

    These tests hit SEC EDGAR and parse real XBRL filings.
    Run inside Docker: docker compose exec app pytest tests/test_sec_pipeline.py::TestXBRLParse -v -s
    """

    @pytest.fixture(scope="class")
    async def parsed_10q(self):
        """Fetch the latest MSFT 10-Q and parse it. Session-scoped to avoid re-parsing."""
        from sec_pipeline import SECAPIClient, XBRLParserService

        client = SECAPIClient(
            user_agent_name="SEC Pipeline Tests",
            user_agent_email="test@example.com",
        )
        parser = XBRLParserService(
            user_agent_name="SEC Pipeline Tests",
            user_agent_email="test@example.com",
        )

        # Fetch filings
        response = await client.get_company_filings("MSFT")
        ten_qs = [f for f in response.filings if f.form_type == "10-Q" and f.xbrl_instance_url]
        assert len(ten_qs) > 0, "No 10-Q filings with XBRL URLs found"

        latest = ten_qs[0]
        logger.info(f"Parsing MSFT 10-Q: {latest.accession_number} ({latest.filing_date})")
        logger.info(f"  URL: {latest.xbrl_instance_url}")

        # Parse XBRL
        xbrl_data = await parser.parse_xbrl_from_url(latest.xbrl_instance_url)

        return {
            "filing": latest,
            "xbrl_data": xbrl_data,
            "company": response,
        }

    async def test_xbrl_output_has_required_keys(self, parsed_10q):
        """The XBRL parser output dict must have all expected top-level keys."""
        xbrl_data = parsed_10q["xbrl_data"]

        required_keys = [
            "document_info",
            "contexts",
            "units",
            "facts",
            "concepts",
            "labels",
            "statement_roles",
            "presentation_relationships",
            "calculation_relationships",
            "definition_relationships",
            "summary",
        ]

        for key in required_keys:
            assert key in xbrl_data, f"Missing required key: {key}"
            logger.info(f"  {key}: {type(xbrl_data[key]).__name__} ({len(xbrl_data[key]) if isinstance(xbrl_data[key], list) else 'dict'})")

    async def test_facts_structure(self, parsed_10q):
        """Each fact should have the fields needed for Snowflake."""
        facts = parsed_10q["xbrl_data"]["facts"]

        assert len(facts) > 100, f"Expected 100+ facts, got {len(facts)}"
        logger.info(f"Total facts: {len(facts)}")

        # Check first fact structure
        fact = facts[0]
        required_fact_fields = ["concept", "concept_name", "context_ref", "value"]
        for field in required_fact_fields:
            assert field in fact, f"Fact missing required field: {field}"

        # Check that numeric facts have proper metadata
        numeric_facts = [f for f in facts if f.get("is_numeric")]
        assert len(numeric_facts) > 50, f"Expected 50+ numeric facts, got {len(numeric_facts)}"

        # Spot check: there should be revenue or net income
        concept_names = {f["concept_name"] for f in facts}
        financial_concepts = {"Revenue", "Revenues", "NetIncomeLoss", "Assets", "CashAndCashEquivalentsAtCarryingValue"}
        found = concept_names & financial_concepts
        assert len(found) > 0, f"Expected at least one of {financial_concepts}, found: {concept_names & financial_concepts}"
        logger.info(f"Found key financial concepts: {found}")

    async def test_facts_have_period_info(self, parsed_10q):
        """Facts should have period information (critical for time-series in Snowflake)."""
        facts = parsed_10q["xbrl_data"]["facts"]

        facts_with_period = [f for f in facts if "period" in f and f["period"]]
        assert len(facts_with_period) > len(facts) * 0.8, "Most facts should have period info"

        # Check period types
        period_types = {f["period"]["type"] for f in facts_with_period if "type" in f["period"]}
        assert "instant" in period_types, "Should have instant periods (balance sheet items)"
        assert "duration" in period_types, "Should have duration periods (income statement items)"
        logger.info(f"Period types found: {period_types}")

    async def test_facts_have_labels(self, parsed_10q):
        """Most facts should have human-readable labels (for Snowflake display)."""
        facts = parsed_10q["xbrl_data"]["facts"]

        facts_with_labels = [f for f in facts if f.get("label")]
        label_pct = len(facts_with_labels) / len(facts) * 100
        assert label_pct > 70, f"Only {label_pct:.0f}% of facts have labels, expected >70%"
        logger.info(f"Facts with labels: {len(facts_with_labels)}/{len(facts)} ({label_pct:.0f}%)")

    async def test_contexts_structure(self, parsed_10q):
        """Contexts define the reporting periods and dimensional breakdowns."""
        contexts = parsed_10q["xbrl_data"]["contexts"]

        assert len(contexts) > 10, f"Expected 10+ contexts, got {len(contexts)}"

        context = contexts[0]
        assert "id" in context
        assert "entity" in context
        assert "period" in context
        logger.info(f"Total contexts: {len(contexts)}")

    async def test_units_structure(self, parsed_10q):
        """Units should include USD, shares, and USD/share."""
        units = parsed_10q["xbrl_data"]["units"]

        assert len(units) > 0, "No units found"

        unit_types = {u.get("unit_type") for u in units}
        logger.info(f"Unit types: {unit_types}")

        # Should have at least simple units (USD, shares)
        assert "simple" in unit_types, "Missing simple units (USD, shares)"

        # Check for common measure names
        all_measures = []
        for u in units:
            if u.get("measure"):
                all_measures.append(u["measure"])
            if u.get("numerator_measure"):
                all_measures.append(u["numerator_measure"])

        measures_str = " ".join(all_measures).lower()
        assert "usd" in measures_str, "USD unit missing"
        logger.info(f"Total units: {len(units)}, measures: {all_measures}")

    async def test_concepts_structure(self, parsed_10q):
        """Concepts are taxonomy definitions — needed for joining across filings."""
        concepts = parsed_10q["xbrl_data"]["concepts"]

        assert len(concepts) > 100, f"Expected 100+ concepts, got {len(concepts)}"

        concept = concepts[0]
        assert "qname" in concept
        assert "local_name" in concept
        assert "is_numeric" in concept
        logger.info(f"Total concepts: {len(concepts)}")

    async def test_statement_roles_identify_financial_statements(self, parsed_10q):
        """Statement roles should have correct structure and recognizable statement names."""
        roles = parsed_10q["xbrl_data"]["statement_roles"]

        assert len(roles) > 0, "No statement roles found"

        # Validate required fields on each role
        required_fields = {"role_uri", "definition", "statement_name", "display_order"}
        for role in roles:
            for field in required_fields:
                assert field in role, f"Role missing required field: {field}"

        # statement_name values should contain recognizable financial statement descriptions
        statement_names = {r["statement_name"] for r in roles}
        logger.info(f"Statement names: {statement_names}")

        # A 10-Q should mention at least a couple of these keywords in its statement names
        keywords = ["balance sheet", "income", "cash flow", "operations", "equity", "financial position"]
        names_lower = " ".join(statement_names).lower()
        matched = [kw for kw in keywords if kw in names_lower]
        assert len(matched) >= 2, f"Expected at least 2 financial statement keywords in names, matched: {matched}"

    async def test_presentation_relationships_form_hierarchy(self, parsed_10q):
        """Presentation relationships define the line-item hierarchy."""
        pres = parsed_10q["xbrl_data"]["presentation_relationships"]

        assert len(pres) > 50, f"Expected 50+ presentation relationships, got {len(pres)}"

        rel = pres[0]
        assert "parent_concept" in rel
        assert "child_concept" in rel
        assert "depth" in rel
        assert "role_uri" in rel
        logger.info(f"Total presentation relationships: {len(pres)}")

    async def test_calculation_relationships_have_weights(self, parsed_10q):
        """Calculation relationships should have weights (+1 or -1)."""
        calcs = parsed_10q["xbrl_data"]["calculation_relationships"]

        assert len(calcs) > 0, "No calculation relationships found"

        weights = {c.get("weight") for c in calcs}
        assert 1.0 in weights, "Missing positive weight (+1)"
        logger.info(f"Total calculation relationships: {len(calcs)}, weights: {weights}")

    async def test_labels_from_linkbase(self, parsed_10q):
        """Labels from the label linkbase provide company-specific names."""
        labels = parsed_10q["xbrl_data"]["labels"]

        assert len(labels) > 50, f"Expected 50+ labels, got {len(labels)}"

        label = labels[0]
        assert "concept_qname" in label
        assert "label_text" in label
        assert "label_role" in label
        logger.info(f"Total labels: {len(labels)}")

    async def test_full_output_is_json_serializable(self, parsed_10q):
        """The entire output must be JSON-serializable (for Snowflake variant column)."""
        import json

        xbrl_data = parsed_10q["xbrl_data"]
        json_str = json.dumps(xbrl_data)
        assert len(json_str) > 1000, "JSON output too small"
        logger.info(f"Total JSON size: {len(json_str):,} bytes")


# ---------------------------------------------------------------------------
# 3. Integration: End-to-End Pipeline (fetch → parse → validate for Snowflake)
# ---------------------------------------------------------------------------

class TestEndToEnd:
    """Full pipeline test: fetch filing list → pick a 10-Q → parse → validate output."""

    async def test_pipeline_produces_snowflake_ready_output(self):
        """Simulate what the Modal pipeline does: fetch, parse, validate for Snowflake."""
        from sec_pipeline import SECAPIClient, XBRLParserService

        # Step 1: Create clients (like Modal worker would)
        client = SECAPIClient(
            user_agent_name="SEC Pipeline E2E Test",
            user_agent_email="test@example.com",
        )
        parser = XBRLParserService(
            user_agent_name="SEC Pipeline E2E Test",
            user_agent_email="test@example.com",
        )

        # Step 2: Fetch filings
        response = await client.get_company_filings("MSFT")
        ten_qs = [f for f in response.filings if f.form_type == "10-Q" and f.xbrl_instance_url]
        latest = ten_qs[0]

        # Step 3: Parse XBRL
        xbrl_data = await parser.parse_xbrl_from_url(latest.xbrl_instance_url)

        # Step 4: Validate Snowflake-ready structure
        # This is what you'd transform into DataFrames and write_to_sf()

        # Facts table
        facts = xbrl_data["facts"]
        assert len(facts) > 0
        for fact in facts[:5]:
            # Every fact should have enough info for a Snowflake row
            assert fact.get("concept") is not None
            assert fact.get("value") is not None or fact.get("is_numeric") is False

        # Filing metadata (from Pydantic model)
        filing_meta = latest.model_dump()
        assert filing_meta["accession_number"]
        assert filing_meta["form_type"] == "10-Q"

        # Company metadata
        assert response.ticker == "MSFT"
        assert response.cik

        logger.info("=" * 60)
        logger.info("End-to-End Pipeline Results:")
        logger.info(f"  Company: {response.company_name} ({response.ticker})")
        logger.info(f"  Filing:  {latest.form_type} {latest.accession_number}")
        logger.info(f"  Facts:   {len(facts)}")
        logger.info(f"  Contexts: {len(xbrl_data['contexts'])}")
        logger.info(f"  Units:   {len(xbrl_data['units'])}")
        logger.info(f"  Concepts: {len(xbrl_data['concepts'])}")
        logger.info(f"  Statement Roles: {len(xbrl_data['statement_roles'])}")
        logger.info(f"  Presentation Rels: {len(xbrl_data['presentation_relationships'])}")
        logger.info(f"  Calculation Rels: {len(xbrl_data['calculation_relationships'])}")
        logger.info(f"  Definition Rels: {len(xbrl_data['definition_relationships'])}")
        logger.info(f"  Labels: {len(xbrl_data['labels'])}")
        logger.info("=" * 60)
