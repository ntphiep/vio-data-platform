"""SparkSQL.q of task push_to_business_central on a local Spark session: every value comes back as a string or None,
the shape the callers parse (truthy, Decimal, json.loads, string order of last_update_at). Needs pyspark and Java 17
or later: uv run --no-project --with pytest --with pyspark==4.0.1 python -m pytest tests/test_push_to_business_central_spark.py
"""

import json
import sys
from decimal import Decimal
from pathlib import Path

import pytest

pyspark_sql = pytest.importorskip("pyspark.sql")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import push_to_business_central as m  # noqa: E402


@pytest.fixture(scope="module")
def db(tmp_path_factory):
    s = (
        pyspark_sql.SparkSession.builder.master("local[1]")
        .appName("push_to_business_central")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        # not UTC, so the test shows that the TIMESTAMP literals of the result rows carry their own zone
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .getOrCreate()
    )
    yield m.SparkSQL(s)
    s.stop()


def test_q_returns_strings_like_the_statement_api(db):
    rows = db.q("""SELECT true AS ever_sent, false AS f, CAST(NULL AS BOOLEAN) AS n, DATE'2026-11-09' AS invoice_date,
                          CAST(1340.50 AS DECIMAL(18, 2)) AS amount, 3L AS failures, 26000.5D AS per_usd,
                          1e-05D AS tiny, TIMESTAMP'2026-11-01 08:00:00' AS last_update_at,
                          '{"sales_invoices": 4}' AS versions, array('a', 'b') AS arr, named_struct('x', 1) AS st""")
    assert rows == [
        {
            "ever_sent": "true",
            "f": "false",
            "n": None,
            "invoice_date": "2026-11-09",
            "amount": "1340.50",
            "failures": "3",
            "per_usd": "26000.5",
            "tiny": "1.0E-5",
            "last_update_at": "2026-11-01 08:00:00",
            "versions": '{"sales_invoices": 4}',
            "arr": '["a","b"]',
            "st": '{"x":1}',
        }
    ]
    r = rows[0]
    assert all(isinstance(v, str) for v in r.values() if v is not None)
    assert m.truthy(r["ever_sent"]) and not m.truthy(r["f"]) and not m.truthy(r["n"])
    assert Decimal(r["amount"]) == Decimal("1340.50") and Decimal(r["tiny"]) == Decimal("0.00001")
    assert int(r["failures"]) == 3 and float(r["amount"]) == 1340.5
    assert json.loads(r["versions"]) == {"sales_invoices": 4}


def test_q_keeps_every_column_of_a_select_with_repeated_names(db):
    assert db.q("SELECT 1 AS a, 2 AS b, 3 AS a") == [{"a": "3", "b": "2"}]


def test_q_on_statements_without_rows(db):
    assert db.q("CREATE TABLE IF NOT EXISTS t_ddl (a STRING) USING parquet") == []
    assert db.q("SELECT * FROM t_ddl") == []


def test_result_rows_insert_in_utc_and_sort_by_synced_at(db):
    db.q("""CREATE TABLE sync (synced_at TIMESTAMP, doc_type STRING, doc_key STRING, payload_hash STRING,
            bc_company STRING, bc_id STRING, bc_number STRING, result STRING, error STRING) USING parquet""")
    rows = [
        f"(TIMESTAMP'2026-11-26T01:00:00.{n:06d}Z', 'sales', 'B1', 'H1', 'My Company', NULL, NULL, "
        f"{m.lit(result)}, {m.lit(error)})"
        for n, (result, error) in enumerate([("FAILED", "it's \\ broken"), ("CREATED", None)])
    ]
    db.q("INSERT INTO sync VALUES " + ",".join(rows))
    out = db.q("""SELECT max_by(result, synced_at) AS last, min_by(error, synced_at) AS first_error,
                         CAST(max(synced_at) AS STRING) AS local_time FROM sync""")
    # 01:00 UTC is 08:00 in Asia/Ho_Chi_Minh
    assert out == [{"last": "CREATED", "first_error": "it's \\ broken", "local_time": "2026-11-26 08:00:00.000001"}]
