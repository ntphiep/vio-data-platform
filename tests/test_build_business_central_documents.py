"""Task build_business_central_documents without Databricks: main() runs against a fake SparkSession that records every
statement, and each statement must parse as Databricks SQL. tests/test_business_central_documents_spark.py runs the
document rules on a local Spark session.
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

    def __init__(self, row, rows=None, columns=None):
        self.row, self.rows, self.columns = row, rows, columns

    def first(self):
        return self.row

    def collect(self):
        return self.rows if self.rows is not None else [self.row]


class FakeSpark:
    def __init__(self, last_token=None, watermark=None, pending=1, emitted=1, columns=None):
        self.statements, self.last_token, self.watermark = [], last_token, watermark
        self.pending, self.emitted, self.columns = pending, emitted, columns
        self.builder = self
        self.written = []

    def getOrCreate(self):  # noqa: N802 - pyspark API name
        return self

    def sql(self, q):
        self.statements.append(q)
        if "max_by(silver_token" in q:
            return Result({"t": self.last_token})
        if q.startswith("SHOW TBLPROPERTIES"):
            rows = [{"key": m.DOCUMENTS_WATERMARK, "value": self.watermark}] if self.watermark else []
            return Result(None, rows=rows)
        if q.startswith("DESCRIBE HISTORY"):
            return Result({"version": 7})
        if " LIMIT 0" in q:
            table = q.split(".")[-1].split()[0]
            names = [n for n, _, _ in (m.SALES_COLUMNS if table == "sales_documents" else m.PURCHASE_COLUMNS)]
            return Result(None, columns=self.columns or names)
        if "FROM vio_travel.bronze.tc_bookings" in q:
            return Result({"n": self.pending})
        if "FROM (SELECT 1 FROM vio_travel.business_central.sales_documents LIMIT 1)" in q:
            return Result({"n": self.emitted})
        if "CAST(max(last_seen_ingested_at) AS STRING)" in q:
            return Result({"w": "2026-11-25 05:00:01.123456"})
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
    ("args", "watermark", "reads_all"),
    [
        (["--rebuild", "true"], "2026-11-25 04:00:00", True),
        ([], None, True),
        ([], "2026-11-25 04:00:00.5", False),
    ],
)
def test_every_statement_parses_as_databricks_sql(monkeypatch, args, watermark, reads_all):
    token = run_main(monkeypatch, FakeSpark()).statements[-1].split("'")[-2]
    spark = run_main(monkeypatch, FakeSpark(last_token=token, watermark=watermark), *args)
    assert any("MERGE INTO vio_travel.business_central.booking_versions" in q for q in spark.statements)
    assert any("MERGE INTO vio_travel.business_central.sales_documents" in q for q in spark.statements)
    assert any("MERGE INTO vio_travel.business_central.purchase_documents" in q for q in spark.statements)
    assert not any("INSERT OVERWRITE" in q for q in spark.statements)
    for q in spark.statements:
        sqlglot.parse_one(q, dialect="databricks", error_level=sqlglot.ErrorLevel.RAISE)
    # watermarks are compared in SQL, never written from a Python datetime
    landed = next(q for q in spark.statements if "TEMP VIEW landed AS" in q)
    assert "max(last_seen_ingested_at)" in landed and "TIMESTAMP'2026" not in landed
    candidates = next(q for q in spark.statements if "TEMP VIEW candidates AS" in q)
    assert ("WHERE" in candidates) == (not reads_all)
    assert spark.statements[-2] == (
        "ALTER TABLE vio_travel.business_central.booking_versions "
        f"SET TBLPROPERTIES ('{m.DOCUMENTS_WATERMARK}' = '2026-11-25 05:00:01.123456')"
    )
    assert any("VERSION AS OF 7" in q for q in spark.statements)
    # 15 checks, written once to ops.dq_results
    assert [len(rows) for rows in spark.written] == [len(m.checks_sql("x", 7))] == [15]


def test_nothing_landed_and_same_mappings_skips_the_build(monkeypatch):
    token = run_main(monkeypatch, FakeSpark()).statements[-1].split("'")[-2]
    again = run_main(monkeypatch, FakeSpark(last_token=token, watermark="2026-11-25 04:00:00", pending=0))
    assert not any("MERGE INTO" in q for q in again.statements)
    changed = run_main(monkeypatch, FakeSpark(last_token=token[::-1], watermark="2026-11-25 04:00:00", pending=0))
    assert any("MERGE INTO vio_travel.business_central.sales_documents" in q for q in changed.statements)


def test_documents_of_an_earlier_shape_stop_the_task(monkeypatch):
    with pytest.raises(RuntimeError, match="drop it"):
        run_main(monkeypatch, FakeSpark(columns=["integration_key", "dwh_version"]))


def test_every_created_column_has_a_comment(monkeypatch):
    spark = run_main(monkeypatch, FakeSpark())
    created = [q for q in spark.statements if q.startswith("CREATE TABLE IF NOT EXISTS") and " COMMENT " in q]
    assert len(created) == 4
    for q in created:
        cols = [c for c in sqlglot.parse_one(q, dialect="databricks").this.expressions if isinstance(c, exp.ColumnDef)]
        assert len(cols) >= 6
        for col in cols:
            assert col.find(exp.CommentColumnConstraint), f"{col.name} has no COMMENT in {q[:60]}"


def test_version_columns_and_hash():
    names = [n for n, _, _ in m.VERSION_COLUMNS]
    assert len(names) == len(set(names))
    assert m.HASHED_HEADER[0] == "tc_booking_id" and m.HASHED_HEADER[-1] == "services"
    assert "last_update_at" not in m.hashed_services_sql("services")


def test_invoice_prefix_is_yyddd_of_the_build_day():
    assert m.invoice_prefix(datetime.date(2026, 11, 25)) == "26329"
    assert m.invoice_prefix(datetime.date(2027, 1, 1)) == "27001"
    # 17:30 UTC on 25 November is 26 November in Asia/Ho_Chi_Minh
    assert m.build_day(datetime.datetime(2026, 11, 25, 17, 30, tzinfo=datetime.UTC)) == datetime.date(2026, 11, 26)


def test_paid_amount_stays_null_until_the_marker_is_confirmed():
    assert m.PAID_SYSTEM is None
    view = next(q for q in m.sales_sql("c", "b", datetime.date(2026, 11, 25), "1") if "VIEW sales_versions" in q)
    assert "CAST(NULL AS DECIMAL(18,2)) AS paid" in view
