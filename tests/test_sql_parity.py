"""SQL fragments that the pipeline (src/build_bronze_silver_gold) and the task build_business_central_tables must share.

The files are read as text and parsed with ast: pyspark is not installed in the test environment, and the pipeline
files only run inside a Lakeflow pipeline.
"""

import ast
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src"


def constant(path: Path, name: str):
    """Value of the module-level literal assignment `name = ...` in path."""
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {path}")


def test_provider_vendor_number_matches_business_central_tables():
    """dim_supplier.supplier_key PV<code> must equal the Business Central vendor number of build_business_central_tables."""
    gold = constant(SRC / "build_bronze_silver_gold" / "gold.py", "PROVIDER_VENDOR_NUMBER")
    bc = constant(SRC / "build_business_central_tables.py", "PROVIDER_VENDOR_NUMBER")
    assert gold.format(alias="s") == bc


def test_service_arrays_match_business_central_tables():
    silver = constant(SRC / "build_bronze_silver_gold" / "silver.py", "SERVICE_ARRAYS")
    bc = constant(SRC / "build_business_central_tables.py", "SERVICE_ARRAYS")
    assert silver == bc


def test_build_business_central_documents_shares_service_constants():
    """Service arrays, manual keys and country rule of silver.booking_services; type codes and provider vendor number
    of build_business_central_tables (the purchase_key and vendor number of the purchase documents)."""
    docs = SRC / "build_business_central_documents.py"
    silver = SRC / "build_bronze_silver_gold" / "silver.py"
    bc = SRC / "build_business_central_tables.py"
    for name in ("SERVICE_ARRAYS", "MANUAL_KEYS", "COUNTRY_SUFFIX"):
        assert constant(docs, name) == constant(silver, name), name
    for name in ("TYPE_CODES", "PROVIDER_VENDOR_NUMBER"):
        assert constant(docs, name) == constant(bc, name), name


def test_setup_catalog_creates_the_code_map_like_the_task():
    """sql/setup-catalog.sql and build_business_central_documents create ref.business_central_code_map alike."""
    setup = (SRC.parent / "sql" / "setup-catalog.sql").read_text()
    ddl = setup.split("ref.business_central_code_map (", 1)[1].split(")\nCOMMENT", 1)[0]
    columns = constant(SRC / "build_business_central_documents.py", "CODE_MAP_COLUMNS")
    assert [line.strip().rstrip(",") for line in ddl.strip().splitlines()] == [
        f"{n} {t} COMMENT '{c}'" for n, t, c in columns
    ]
