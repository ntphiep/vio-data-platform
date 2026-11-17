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
