import json
import importlib.resources

_statement_type_mappings: dict[str, str] | None = None


def load_statement_type_mappings() -> dict[str, str]:
    """Load role-URI-to-statement-type mappings from the seed file.

    Returns the ``mappings`` dict (role_uri -> statement_type).
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
