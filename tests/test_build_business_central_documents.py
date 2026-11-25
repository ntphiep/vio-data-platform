"""Task build_business_central_documents without Databricks: main() runs against a fake SparkSession that records every
statement, and each statement must parse as Databricks SQL. The versioning and document rules themselves run only on
Databricks; the queries to check them after a run are in the README section Business Central integration.
Run: uv run --no-project --with pytest --with requests --with sqlglot python -m pytest tests"""

import datetime
import sys
import types
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import build_business_central_documents as m  # noqa: E402


class Result:
    """Answer of the fake spark.sql(): one row that serves every read main() makes."""

    def __init__(self, row):
        self.row = row

    def first(self):
        return self.row

    def collect(self):
        return [self.row]


class FakeSpark:
    def __init__(self, last_token=None, watermark=None):
        self.statements, self.last_token, self.watermark = [], last_token, watermark
        self.builder = self
        self.written = []

    def getOrCreate(self):  # noqa: N802 - pyspark API name
        return self

    def sql(self, q):
        self.statements.append(q)
        if "max_by(silver_token" in q:
            return Result({"t": self.last_token})
        if "max(last_seen_ingested_at)" in q:
            return Result({"w": self.watermark})
        if "collect_list(k)" in q:
            return Result({"n": 0, "s": []})
        return Result({0: "0:0", "t": None, "w": None, "n": 0, "s": []})

    def createDataFrame(self, rows, schema):  # noqa: N802 - pyspark API name
        self.written.append(rows)
        return types.SimpleNamespace(
            selectExpr=lambda *a: types.SimpleNamespace(
                write=types.SimpleNamespace(mode=lambda *_: types.SimpleNamespace(saveAsTable=lambda *_: None))
            )
        )


def run_main(monkeypatch, spark, *args):
    pyspark, pyspark_sql = types.ModuleType("pyspark"), types.ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = spark
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)
    monkeypatch.setattr(sys, "argv", ["build_business_central_documents.py", "--run_id", "1", *args])
    m.main()
    return spark


@pytest.mark.parametrize(
    ("args", "watermark"),
    [(["--rebuild", "true"], None), ([], datetime.datetime(2026, 11, 25, 5, 0, 1, 123456))],
)
def test_every_statement_parses_as_databricks_sql(monkeypatch, args, watermark):
    spark = run_main(monkeypatch, FakeSpark(watermark=watermark), *args)
    assert any("MERGE INTO vio_travel.business_central.booking_versions" in q for q in spark.statements)
    assert any("INSERT OVERWRITE vio_travel.business_central.purchase_documents" in q for q in spark.statements)
    for q in spark.statements:
        sqlglot.parse_one(q, dialect="databricks", error_level=sqlglot.ErrorLevel.RAISE)
    landed = next(q for q in spark.statements if "TEMP VIEW landed AS" in q)
    assert ("TIMESTAMP'2026-11-25 05:00:01.123456'" in landed) == (watermark is not None)
    # 11 checks, written once to ops.dq_results
    assert [len(rows) for rows in spark.written] == [len(m.checks_sql("x"))] == [11]


def test_unchanged_token_skips_the_build(monkeypatch):
    first = run_main(monkeypatch, FakeSpark())
    token = next(q for q in first.statements if q.startswith("INSERT INTO vio_travel.ops.build_state")).split("'")[-2]
    again = run_main(monkeypatch, FakeSpark(last_token=token))
    assert not any("MERGE INTO" in q or "INSERT OVERWRITE" in q for q in again.statements)


def test_every_created_column_has_a_comment(monkeypatch):
    spark = run_main(monkeypatch, FakeSpark())
    created = [q for q in spark.statements if q.startswith("CREATE TABLE IF NOT EXISTS") and " COMMENT " in q]
    assert len(created) == 4
    for q in created:
        cols = [c for c in sqlglot.parse_one(q, dialect="databricks").this.expressions if isinstance(c, exp.ColumnDef)]
        assert len(cols) >= 6
        for col in cols:
            assert col.find(exp.CommentColumnConstraint), f"{col.name} has no COMMENT in {q[:60]}"


def test_insert_columns_match_table_columns():
    """INSERT OVERWRITE is positional: its SELECT must have one expression per column, in table order."""
    for statements, columns in (
        (m.sales_sql("c", "b"), m.SALES_COLUMNS),
        (m.purchase_sql("c", "b"), m.PURCHASE_COLUMNS),
    ):
        insert = sqlglot.parse_one(statements[-1], dialect="databricks")
        assert len(insert.expression.expressions) == len(columns)


def test_version_columns_and_hash():
    names = [n for n, _, _ in m.VERSION_COLUMNS]
    assert len(names) == len(set(names))
    assert m.HASHED_HEADER[0] == "tc_booking_id" and m.HASHED_HEADER[-1] == "services"
    assert "last_update_at" not in m.hashed_services_sql("services")


def test_invoice_number_is_yydddnnn():
    assert m.invoice_number(datetime.date(2026, 11, 25), 7) == "26329007"
    assert m.invoice_number(datetime.date(2027, 1, 1), 123) == "27001123"
