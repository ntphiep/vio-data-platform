"""Document rules of build_business_central_documents run on a local Spark session over seeded booking_versions rows.

The document SQL reads no VARIANT, so it runs on open-source Spark; only the MERGE statements are Databricks (Delta)
statements, and the tests append the temp views sales_new and purchase_new with INSERT INTO instead. Needs pyspark
and Java 17 or later:
uv run --no-project --with pytest --with pyspark==4.0.1 python -m pytest tests/test_business_central_documents_spark.py
"""

import datetime
import sys
from decimal import Decimal
from pathlib import Path

import pytest

pyspark_sql = pytest.importorskip("pyspark.sql")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import build_business_central_documents as m  # noqa: E402

C = "spark_catalog"
B = f"{C}.business_central"
DAY1 = datetime.date(2026, 11, 25)
DAY2 = datetime.date(2026, 11, 26)
T0 = datetime.datetime(2026, 11, 1, 8, 0, tzinfo=datetime.UTC)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    s = (
        pyspark_sql.SparkSession.builder.master("local[1]")
        .appName("business_central_documents")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        # small data: interpreted plans start faster than generated code
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture
def db(spark):
    """Empty tables of the task and of the inputs it reads."""
    for schema in ("ref", "silver", "business_central"):
        spark.sql(f"DROP DATABASE IF EXISTS {C}.{schema} CASCADE")
        spark.sql(f"CREATE DATABASE {C}.{schema}")
    spark.sql(f"CREATE TABLE {C}.ref.business_central_code_map ({m.ddl(m.CODE_MAP_COLUMNS)}) USING parquet")
    spark.sql(f"CREATE TABLE {C}.ref.umbrella_agency (agency_id STRING) USING parquet")
    spark.sql(f"CREATE TABLE {C}.ref.customer_map (agency_reference_key STRING, bc_customer_no STRING) USING parquet")
    spark.sql(
        f"CREATE TABLE {C}.ref.vendor_map (source_kind STRING, source_key STRING, bc_vendor_no STRING) USING parquet"
    )
    spark.sql(f"CREATE TABLE {C}.silver.agencies (agency_id STRING) USING parquet")
    spark.sql(f"CREATE TABLE {C}.silver.suppliers (supplier_id STRING) USING parquet")
    for table, columns in (
        ("booking_versions", m.VERSION_COLUMNS),
        ("sales_documents", m.SALES_COLUMNS),
        ("purchase_documents", m.PURCHASE_COLUMNS),
    ):
        spark.sql(f"CREATE TABLE {B}.{table} ({m.ddl(columns)}) USING parquet")
    spark.sql(f"""INSERT INTO {C}.ref.business_central_code_map VALUES
        ('ITEM', 'SALES', 'TOUR', 'Tour', 'LAB SEED', NULL),
        ('ITEM', 'CANCELLATION', 'CANCEL', 'Cancellation', 'LAB SEED', NULL),
        ('ITEM', 'hotelservice', 'HOTEL', 'Hotel', 'LAB SEED', NULL),
        ('ITEM', 'transferservice', 'TRANSFER', 'Transfer', 'LAB SEED', NULL)""")
    spark.sql(f"INSERT INTO {C}.silver.agencies VALUES ('1'), ('2')")
    spark.sql(f"INSERT INTO {C}.silver.suppliers VALUES ('S1')")
    spark.sql(f"INSERT INTO {C}.ref.umbrella_agency VALUES ('9')")
    return spark


def service(sid, status="BOOKED", cost="0", service_type="hotelservice", **kw):
    """One element of booking_versions.services."""
    row = dict.fromkeys(n for n, _ in m.SERVICE_FIELDS)
    row.update(
        service_type=service_type,
        service_id=sid,
        purchase_key=f"{sid}-{m.TYPE_CODES[service_type]}",
        is_manual=service_type.startswith("manual_"),
        status=status,
        name=f"Hotel {sid}",
        supplier_id="S1",
        start_date=datetime.date(2027, 1, 14),
        end_date=datetime.date(2027, 1, 16),
        cost_currency="USD",
        cost_amount=Decimal(cost),
        amendment_sale_amount=Decimal("0"),
        amendment_cost_amount=Decimal("0"),
        amendment_cost_other_currency=0,
        destination_name="Siem Reap",
    )
    row.update(kw)
    return row


def version(ref, v, total, status="BOOKED", services=(), **kw):
    """One row of booking_versions; version v landed v hours after T0."""
    row = dict.fromkeys(n for n, _, _ in m.VERSION_COLUMNS)
    landed = T0 + datetime.timedelta(hours=v, minutes=int(ref.split("-")[1]))
    row.update(
        booking_ref=ref,
        version=v,
        content_hash=f"{ref}-{v}",
        last_update_at=landed,
        ingested_at=landed,
        run_id="r",
        max_seen_last_update_at=landed,
        last_seen_ingested_at=landed,
        tc_booking_id="900" + ref.split("-")[1],
        status=status,
        created_at=datetime.datetime(2026, 10, 5, 3, 0, tzinfo=datetime.UTC),
        start_date=datetime.date(2027, 1, 14),
        end_date=datetime.date(2027, 1, 25),
        pax=2,
        agency_id="1",
        username="ts.user",
        currency="USD",
        total_price=Decimal(total),
        payment_amount=Decimal(total) * Decimal("0.3"),
        deferred_amount=Decimal(total) * Decimal("0.7"),
        deferred_limit_date=datetime.date(2026, 12, 15),
        destinations="1: Manila",
        service_summary="Hotel Manila 14/01/2027",
        services=list(services) or None,
    )
    row.update(kw)
    return row


def land(spark, *rows):
    schema = spark.table(f"{B}.booking_versions").schema
    spark.createDataFrame(list(rows), schema).write.insertInto(f"{B}.booking_versions")


def build(spark, day, run_id, watermark=None, paid_system=None):
    """One run of the document part of main(): the MERGE statements become INSERT INTO of their source view."""
    spark.sql(m.candidates_sql(B, watermark))
    for statements, view, table in (
        (m.sales_sql(C, B, day, run_id, paid_system), "sales_new", "sales_documents"),
        (m.purchase_sql(C, B, day, run_id), "purchase_new", "purchase_documents"),
    ):
        assert statements[-1].startswith(f"MERGE INTO {B}.{table}")
        for q in statements[:-1]:
            spark.sql(q)
        assert spark.table(view).columns == spark.table(f"{B}.{table}").columns
        # materialise first: Spark does not insert into a table that the inserted query reads
        new = spark.createDataFrame(spark.table(view).collect(), spark.table(f"{B}.{table}").schema)
        new.write.insertInto(f"{B}.{table}")
    failed = {}
    for name, (severity, q) in m.checks_sql(B).items():
        keys = sorted(r["k"] for r in spark.sql(q).collect())
        if keys:
            failed[name] = (severity, keys)
    assert not {n: f for n, f in failed.items() if f[0] == "block"}, failed
    return failed


def mark(spark):
    """DOCUMENTS_WATERMARK as main() records it after a successful build."""
    return spark.sql(f"SELECT CAST(max(last_seen_ingested_at) AS STRING) AS w FROM {B}.booking_versions").first()["w"]


def sales(spark, ref=None):
    where = f"WHERE external_document_no = '{ref}'" if ref else ""
    return spark.sql(f"SELECT * FROM {B}.sales_documents {where} ORDER BY dwh_version, doc_seq").collect()


def purchases(spark, ref):
    q = f"SELECT * FROM {B}.purchase_documents WHERE booking_reference = '{ref}' ORDER BY service_reference, dwh_version, doc_seq"
    return spark.sql(q).collect()


def shape(rows):
    return [
        (r["integration_key"], r["change_type"], r["document_role"], r["document_type"], r["document_amount"])
        for r in rows
    ]


def test_new_increase_decrease_and_rerun(db):
    land(db, version("EXA-1", 1, "1000", services=[service("11", cost="600")]))
    build(db, DAY1, "run1")
    w = mark(db)
    land(db, version("EXA-1", 2, "1200", services=[service("11", cost="700")]))
    build(db, DAY1, "run2", watermark=w)
    w = mark(db)
    land(db, version("EXA-1", 3, "1100", services=[service("11", cost="650")]))
    build(db, DAY2, "run3", watermark=w)

    docs = sales(db, "EXA-1")
    assert shape(docs) == [
        ("EXA-1-1-1", "NEW", "FULL", "Invoice", Decimal("1000.00")),
        ("EXA-1-2-1", "INCREASE", "ADJUSTMENT", "Invoice", Decimal("200.00")),
        ("EXA-1-3-1", "DECREASE", "ADJUSTMENT", "Credit Memo", Decimal("100.00")),
    ]
    # YYDDD of the build day, then the next number of that day; no number on a credit memo
    assert [d["invoice_no"] for d in docs] == ["26329001", "26329002", None]
    full = docs[0]
    assert [(line["line_type"], line["unit_price"]) for line in full["sales_lines"]] == [
        ("Deposit", Decimal("300.00")),
        ("Balance", Decimal("700.00")),
        (None, Decimal("0.00")),
    ]
    assert full["sales_lines"][0]["description"] == "1: Manila commencing 14/1/2027"
    assert full["posting_date"] == datetime.date(2026, 10, 5) and full["due_date"] == datetime.date(2026, 12, 15)
    assert full["currency_code"] == "" and full["booking_currency"] == "USD"
    assert full["paid_amount"] is None and full["payment_method"] is None
    assert docs[1]["posting_date"] == datetime.date(2026, 11, 1)

    assert shape(purchases(db, "EXA-1")) == [
        ("HOTEL-EXA-1-11-1-1", "NEW", "FULL", "Invoice", Decimal("600.00")),
        ("HOTEL-EXA-1-11-2-1", "INCREASE", "ADJUSTMENT", "Invoice", Decimal("100.00")),
        ("HOTEL-EXA-1-11-3-1", "DECREASE", "ADJUSTMENT", "Credit Memo", Decimal("50.00")),
    ]
    line = purchases(db, "EXA-1")[1]["purchase_lines"][0]
    assert line["description"] == "Cost adjustment - HOTEL-EXA-1-11 Hotel 11"
    assert line["description_2"] == "Siem Reap"
    assert purchases(db, "EXA-1")[0]["purchase_key"] == "11-H"

    # a rerun, incremental or over every booking, emits nothing and keeps every number
    before = [r.asDict(recursive=True) for r in sales(db)]
    build(db, DAY2, "run4", watermark=mark(db))
    build(db, DAY2, "run5")
    assert [r.asDict(recursive=True) for r in sales(db)] == before
    assert len(purchases(db, "EXA-1")) == 3


def test_incremental_run_reads_only_bookings_landed_after_the_watermark(db):
    land(db, version("EXA-1", 1, "1000"), version("EXA-2", 1, "500", agency_id="2"))
    build(db, DAY1, "run1")
    w = mark(db)
    # a mapping change without a new landing is not seen by an incremental run of a booking that did not land
    db.sql(f"INSERT INTO {C}.ref.umbrella_agency VALUES ('1')")
    land(db, version("EXA-2", 2, "600", agency_id="2"))
    build(db, DAY1, "run2", watermark=w)
    assert [r["integration_key"] for r in sales(db, "EXA-1")] == ["EXA-1-1-1"]
    assert [r["change_type"] for r in sales(db, "EXA-2")] == ["NEW", "INCREASE"]


def test_cancel_with_and_without_fee(db):
    hotel, transfer = service("31", cost="300"), service("32", cost="100", service_type="transferservice")
    land(db, version("EXA-3", 1, "500", services=[hotel, transfer]))
    land(db, version("EXA-4", 1, "400", currency="EUR"))
    build(db, DAY1, "run1")
    w = mark(db)
    cancelled_on = datetime.date(2026, 11, 20)
    land(
        db,
        version(
            "EXA-3",
            2,
            "50",
            status="CANCELED",
            cancellation_date=cancelled_on,
            services=[
                service("31", status="CANCELED", cost="30", cancellation_date=cancelled_on),
                service("32", status="CANCELED", cost="0", service_type="transferservice"),
            ],
        ),
        version("EXA-4", 2, "0", status="CANCELED", currency="EUR"),
    )
    warned = build(db, DAY1, "run2", watermark=w)

    docs = sales(db, "EXA-3")
    assert shape(docs)[1:] == [
        ("EXA-3-2-1", "CANCEL", "REVERSAL", "Credit Memo", Decimal("500.00")),
        ("EXA-3-2-2", "CANCEL", "CANCELLATION_FEE", "Invoice", Decimal("50.00")),
    ]
    assert {d["posting_date"] for d in docs[1:]} == {cancelled_on}
    assert docs[2]["sales_lines"][0]["no"] == "CANCEL" and docs[2]["invoice_no"] == "26329003"
    assert shape(sales(db, "EXA-4"))[1:] == [("EXA-4-2-1", "CANCEL", "REVERSAL", "Credit Memo", Decimal("400.00"))]
    assert sales(db, "EXA-4")[0]["currency_code"] == "EUR"

    # API_Purchases_T01 row 6 item 4: credit memo for the cost invoiced, invoice for the cost Travel Compositor keeps
    assert shape(purchases(db, "EXA-3")) == [
        ("HOTEL-EXA-3-31-1-1", "NEW", "FULL", "Invoice", Decimal("300.00")),
        ("HOTEL-EXA-3-31-2-1", "CANCEL", "REVERSAL", "Credit Memo", Decimal("300.00")),
        ("HOTEL-EXA-3-31-2-2", "CANCEL", "CANCELLATION_FEE", "Invoice", Decimal("30.00")),
        ("TRANSFER-EXA-3-32-1-1", "NEW", "FULL", "Invoice", Decimal("100.00")),
        ("TRANSFER-EXA-3-32-2-1", "CANCEL", "REVERSAL", "Credit Memo", Decimal("100.00")),
    ]
    assert warned["business_central_cancelled_service_keeps_cost"] == ("warn", ["HOTEL-EXA-3-31"])
    assert warned["business_central_cancellation_without_date"] == ("warn", ["EXA-4-2-1"])


def test_reissue_after_a_mapping_edit_corrects_without_rewriting(db):
    umbrella = {"agency_id": "9", "agency_booking_reference": "Gate1"}
    land(
        db,
        version("EXA-5", 1, "800", **umbrella),
        version("EXA-6", 1, "700", agency_booking_reference="x", agency_id="9"),
    )
    db.sql(f"INSERT INTO {C}.ref.customer_map VALUES ('x', 'C00001')")
    warned = build(db, DAY1, "run1")
    held = sales(db, "EXA-5")
    assert [(d["hold_reason"], d["invoice_no"]) for d in held] == [("CUSTOMER_UNMAPPED", None)]
    assert warned["business_central_document_held"] == ("warn", ["EXA-5-1-1"])
    first_invoice = sales(db, "EXA-6")[0]["invoice_no"]

    # Finance maps Gate1 and moves x to another customer: a run over every booking corrects both on the build day
    db.sql(f"INSERT OVERWRITE {C}.ref.customer_map VALUES ('gate1', 'C00020'), ('x', 'C00002')")
    build(db, DAY2, "run2")
    assert [
        (d["integration_key"], d["change_type"], d["document_type"], d["sell_to_customer_no"], d["hold_reason"])
        for d in sales(db, "EXA-5")
    ] == [
        ("EXA-5-1-1", "NEW", "Invoice", None, "CUSTOMER_UNMAPPED"),
        ("EXA-5-1-2", "REISSUE", "Credit Memo", None, "CUSTOMER_UNMAPPED"),
        ("EXA-5-1-3", "REISSUE", "Invoice", "C00020", None),
    ]
    reissued = sales(db, "EXA-6")
    assert [(d["document_type"], d["sell_to_customer_no"], d["invoice_no"]) for d in reissued] == [
        ("Invoice", "C00001", first_invoice),
        ("Credit Memo", "C00001", None),
        ("Invoice", "C00002", "26330002"),
    ]
    assert {d["posting_date"] for d in reissued[1:]} == {DAY2}
    assert sales(db, "EXA-5")[2]["invoice_no"] == "26330001"


def test_paid_amount_only_on_the_full_invoice_where_it_changed(db):
    paid = {"payment_system": "TMT"}
    land(db, version("EXA-7", 1, "1000", **paid), version("EXA-7", 2, "1100", **paid))
    land(db, version("EXA-7", 3, "1100", agency_id="2", **paid, payment_amount=Decimal("500")))
    build(db, DAY1, "run1", paid_system="TMT")
    assert [
        (d["document_role"], d["document_type"], d["paid_amount"], d["payment_method"]) for d in sales(db, "EXA-7")
    ] == [
        ("FULL", "Invoice", Decimal("300.00"), "Trust My Travel"),
        ("ADJUSTMENT", "Invoice", None, None),
        ("REVERSAL", "Credit Memo", None, None),
        ("FULL", "Invoice", Decimal("200.00"), "Trust My Travel"),
    ]


def test_trip_without_start_date_keeps_a_line_description(db):
    land(db, version("EXA-8", 1, "100", start_date=None))
    warned = build(db, DAY1, "run1")
    assert sales(db, "EXA-8")[0]["sales_lines"][0]["description"] == "1: Manila"
    assert warned["business_central_sales_document_without_start_date"] == ("warn", ["EXA-8-1-1"])


def test_country_fallback_matches_the_silver_rule(spark):
    """A service without a country takes the country of the first service (start date, id) with the same name."""
    struct = "named_struct(" + ", ".join(f"'{n}', CAST(NULL AS {t})" for n, t in m.SERVICE_FIELDS) + ")"

    def s(sid, start, name, country):
        values = {
            "service_id": f"'{sid}'",
            "start_date": f"DATE'{start}'",
            "destination_name": f"'{name}'" if name else "CAST(NULL AS STRING)",
            "destination_country": f"'{country}'" if country else "CAST(NULL AS STRING)",
        }
        return "named_struct(" + ", ".join(f"'{n}', {values.get(n, 'n.' + n)}" for n, _ in m.SERVICE_FIELDS) + ")"

    services = ", ".join(
        [
            s("1", "2027-01-02", "Siem Reap", "KH"),
            s("2", "2027-01-01", "Siem Reap", "TH"),
            s("3", "2027-01-05", "siem reap", None),
            s("4", "2027-01-05", None, None),
            s("5", "2027-01-05", "Hanoi", None),
        ]
    )
    q = f"""SELECT transform({m.country_fallback_sql("a")}, x -> x.destination_country) AS c
            FROM (SELECT array({services}) AS a FROM (SELECT {struct} AS n))"""
    assert spark.sql(q).first()["c"] == ["KH", "TH", "TH", None, None]
