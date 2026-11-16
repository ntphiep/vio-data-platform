"""Task star_schema - analytics model (Kimball star schema plus one big table), catalog <catalog>, schema gold:

dim_date                one row per calendar day, 2015-01-01 to 2030-12-31
dim_agency              one row per agency (Travel Compositor snapshot, plus agencies seen only on bookings)
dim_supplier            one row per supplier: Travel Compositor supplier ID, provider code, or manual supplier name
dim_service_type        one row per service type (hotelservice, manual_hotel, ...)
fact_booking            grain: one booking, current state; measures in the booking currency
fact_booking_service    grain: one service of a booking; sale and cost amounts, cost also in the booking currency
obt_booking_service     one big table: fact_booking_service joined with every dimension and booking attribute,
                        for ad-hoc queries and Genie without joins

Dimensions are type 1 (current values only): Travel Compositor returns the current state of agencies and
suppliers, not their history.

Write-audit-publish: every table is first defined as a temporary view, the checks run on the views, and the gold
tables are replaced only when no blocking check fails. A failed check leaves the previous gold tables in place.

The task skips the rebuild when silver and ref have not changed since the last gold build (ops.build_state);
job parameter rebuild=true forces it, for example after a code change.
"""

import argparse

SCHEMA = "gold"
UNKNOWN = "-1"
SERVICE_ARRAYS = [
    "hotelservice",
    "transferservice",
    "transportservice",
    "ticketservice",
    "closedtourservice",
    "carservice",
    "insuranceservice",
    "cruiseservice",
    "itemservice",
]
MANUAL_KEYS = [
    "hotel",
    "transfer",
    "transport",
    "ticket",
    "closedTour",
    "carRental",
    "cruise",
    "insurance",
    "other",
    "accountingAdjustment",
]
CATEGORY = {
    "hotelservice": "Hotel",
    "transferservice": "Transfer",
    "transportservice": "Transport",
    "ticketservice": "Ticket",
    "closedtourservice": "Closed tour",
    "carservice": "Car rental",
    "insuranceservice": "Insurance",
    "cruiseservice": "Cruise",
    "itemservice": "Item",
    "manual_hotel": "Hotel",
    "manual_transfer": "Transfer",
    "manual_transport": "Transport",
    "manual_ticket": "Ticket",
    "manual_closedTour": "Closed tour",
    "manual_carRental": "Car rental",
    "manual_cruise": "Cruise",
    "manual_insurance": "Insurance",
    "manual_other": "Other",
    "manual_accountingAdjustment": "Accounting adjustment",
}
# Order matters: dimensions first, then facts, then the one big table built on them.
TABLES = [
    "dim_date",
    "dim_agency",
    "dim_supplier",
    "dim_service_type",
    "fact_booking",
    "fact_booking_service",
    "obt_booking_service",
]
PII_TABLES = ["dim_agency", "dim_supplier", "obt_booking_service"]
ACTIVE = "('BOOKED', 'RQ')"


def supplier_key_sql(alias: str) -> str:
    """Same SP and PV numbers as the vendor numbers of bc_tables.py, before any ref.vendor_map override."""
    return (
        f"CASE WHEN {alias}.supplier_id IS NOT NULL THEN 'SP' || {alias}.supplier_id "
        f"WHEN NOT {alias}.is_manual AND {alias}.provider IS NOT NULL "
        f"THEN left('PV' || regexp_replace({alias}.provider, '[^A-Z0-9]', ''), 20) "
        f"WHEN nullif(trim({alias}.supplier_name), '') IS NOT NULL THEN 'MN:' || lower(trim({alias}.supplier_name)) "
        f"ELSE '{UNKNOWN}' END"
    )


def silver_unchanged(sql, c: str, task: str, rebuild: str) -> tuple[bool, str]:
    """True when silver and ref have not changed since this task last built; also returns the current token."""
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ops.build_state (
        task STRING, run_id STRING, built_at TIMESTAMP, silver_token STRING)""")
    last = {
        r["task"]: r["t"]
        for r in sql(f"""SELECT task, max_by(silver_token, built_at) AS t
        FROM {c}.ops.build_state WHERE task IN ('bronze_silver', '{task}') GROUP BY task""").collect()
    }
    token = last.get("bronze_silver") or ""
    return (rebuild.lower() != "true" and token != "" and last.get(task) == token), token


def record_build(sql, c: str, task: str, run_id: str, token: str) -> None:
    sql(f"INSERT INTO {c}.ops.build_state SELECT '{task}', '{run_id}', current_timestamp(), '{token}'")


def main():
    from pyspark.sql import SparkSession

    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="vio_travel")
    ap.add_argument("--run_id", default="")
    ap.add_argument("--rebuild", default="false", help="true: rebuild even when silver has not changed")
    a = ap.parse_args()
    spark = SparkSession.builder.getOrCreate()
    c = a.catalog
    sql = spark.sql
    s = f"{c}.silver"
    unchanged, token = silver_unchanged(sql, c, "star_schema", a.rebuild)
    if unchanged:
        print("silver and ref unchanged since the last gold build, nothing to do")
        return

    # ---------------- dimensions ----------------------------------------------------------------
    sql("""CREATE OR REPLACE TEMP VIEW v_dim_date AS
        SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS date, year(d) AS year,
               quarter(d) AS quarter, month(d) AS month, date_format(d, 'MMM') AS month_name,
               date_format(d, 'yyyy-MM') AS year_month, weekofyear(d) AS iso_week,
               dayofweek(d) AS day_of_week, date_format(d, 'EEE') AS day_name, dayofweek(d) IN (1, 7) AS is_weekend
        FROM (SELECT explode(sequence(DATE'2015-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS d)""")
    sql(f"""CREATE OR REPLACE TEMP VIEW v_dim_agency AS
        SELECT a.agency_id AS agency_key, a.name AS agency_name, a.business_name, a.country, a.city, a.active,
               a.invoice_type, a.deferred_payment_allowed, a.deferred_days, u.agency_id IS NOT NULL AS is_umbrella,
               'snapshot' AS source
        FROM {s}.agencies a LEFT JOIN {c}.ref.umbrella_agency u ON u.agency_id = a.agency_id
        UNION ALL
        -- agencies that appear on bookings but not in the current snapshot (created or removed since)
        SELECT agency_id, max(agency_name), NULL, NULL, NULL, NULL, NULL, NULL, NULL, false, 'booking'
        FROM {s}.bookings b WHERE b.agency_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM {s}.agencies x WHERE x.agency_id = b.agency_id)
        GROUP BY agency_id
        UNION ALL
        SELECT '{UNKNOWN}', 'Unknown', NULL, NULL, NULL, NULL, NULL, NULL, NULL, false, 'unknown'""")
    sql(f"""CREATE OR REPLACE TEMP VIEW v_dim_supplier AS
        SELECT 'SP' || p.supplier_id AS supplier_key, 'Travel Compositor supplier' AS supplier_source,
               coalesce(nullif(trim(p.commercial_name), ''), p.legal_name) AS supplier_name, p.legal_name,
               p.country, p.active, p.supplier_id AS tc_supplier_id, CAST(NULL AS STRING) AS provider_code
        FROM {s}.suppliers p
        UNION ALL
        SELECT k, max(src), max(name), NULL, NULL, NULL, max(sid), max(prov) FROM (
          SELECT {supplier_key_sql("x")} AS k,
                 CASE WHEN x.supplier_id IS NOT NULL THEN 'Travel Compositor supplier'
                      WHEN NOT x.is_manual AND x.provider IS NOT NULL THEN 'Provider'
                      WHEN x.is_manual THEN 'Manual service' ELSE 'Supplier name only' END AS src,
                 coalesce(x.supplier_name, x.provider) AS name, x.supplier_id AS sid,
                 CASE WHEN NOT x.is_manual THEN x.provider END AS prov
          FROM {s}.booking_services x)
        WHERE k <> '{UNKNOWN}'
          AND NOT EXISTS (SELECT 1 FROM {s}.suppliers p2 WHERE 'SP' || p2.supplier_id = k)
        GROUP BY k
        UNION ALL
        SELECT '{UNKNOWN}', 'Unknown', 'Unknown', NULL, NULL, NULL, NULL, NULL""")
    cat_rows = ", ".join(f"('{k}', '{v}', {str(k.startswith('manual_')).lower()})" for k, v in CATEGORY.items())
    sql(f"""CREATE OR REPLACE TEMP VIEW v_dim_service_type AS
        SELECT service_type, service_category, is_manual FROM VALUES {cat_rows}
          AS t(service_type, service_category, is_manual)
        UNION ALL
        SELECT DISTINCT service_type, 'Other', service_type LIKE 'manual_%' FROM {s}.booking_services
        WHERE service_type NOT IN ({", ".join(f"'{k}'" for k in CATEGORY)})""")

    # ---------------- facts ---------------------------------------------------------------------
    sql(f"""CREATE OR REPLACE TEMP VIEW v_fact_booking AS
        SELECT b.booking_ref, coalesce(b.agency_id, '{UNKNOWN}') AS agency_key,
               CAST(date_format(b.created_at, 'yyyyMMdd') AS INT) AS created_date_key,
               CAST(date_format(b.start_date, 'yyyyMMdd') AS INT) AS start_date_key,
               CAST(date_format(b.end_date, 'yyyyMMdd') AS INT) AS end_date_key,
               b.status, b.status IN {ACTIVE} AS is_active, b.status = 'CANCELED' AS is_cancelled,
               b.trip_type, b.sales_channel, b.payload:sourceMarket.id::string AS source_market_code,
               b.payload:sourceMarket.name::string AS source_market_name, b.currency, b.pax,
               b.payload:nightsCount::int AS nights, b.payload:destinationCount::int AS destination_count,
               b.total_price, b.net_provider, b.operator_fee, b.operator_management_fee,
               b.total_price - b.net_provider AS gross_margin, b.paid_amount, b.deferred_amount,
               coalesce(n.service_count, 0) AS service_count, b.created_at, b.last_update_at
        FROM {s}.bookings b
        LEFT JOIN (SELECT booking_ref, count(*) AS service_count FROM {s}.booking_services GROUP BY 1) n
          ON n.booking_ref = b.booking_ref""")
    # Destination of each service, read from the booking payload (hotel, ticket, transfer, transport and
    # manual services carry a country code; hotels also carry a destination name).
    dest = " UNION ALL ".join(
        [
            f"""SELECT b.booking_ref, '{t}' AS service_type, x.value:id::string AS id,
                    x.value:country::string AS country,
                    coalesce(x.value:destinationName::string, x.value:locationName::string) AS dname
             FROM {s}.bookings b, LATERAL variant_explode(b.payload:{t}) x"""
            for t in SERVICE_ARRAYS
        ]
        + [
            f"""SELECT b.booking_ref, 'manual_{k}', x.value:id::string, x.value:country::string,
                    x.value:destination::string
             FROM {s}.bookings b, LATERAL variant_explode(b.payload:manualServices.{k}) x"""
            for k in MANUAL_KEYS
        ]
    )
    sql(f"""CREATE OR REPLACE TEMP VIEW v_service_destination AS
        SELECT booking_ref, service_type, id, max(upper(country)) AS country, max(initcap(dname)) AS destination_name
        FROM ({dest}) WHERE id IS NOT NULL GROUP BY 1, 2, 3""")
    sql(f"""CREATE OR REPLACE TEMP VIEW v_fact_booking_service AS
        SELECT x.booking_ref || '|' || x.service_type || '|' || x.service_id AS service_key,
               x.booking_ref, x.service_id, x.service_type, {supplier_key_sql("x")} AS supplier_key,
               coalesce(b.agency_id, '{UNKNOWN}') AS agency_key,
               CAST(date_format(b.created_at, 'yyyyMMdd') AS INT) AS booking_created_date_key,
               CAST(date_format(x.start_date, 'yyyyMMdd') AS INT) AS service_start_date_key,
               x.status, x.status IN {ACTIVE} AS is_active, x.is_manual,
               d.country AS destination_country, d.destination_name,
               x.sale_currency, x.sale_amount, x.cost_currency, x.cost_amount,
               b.currency AS booking_currency, x.cost_in_sale_currency AS cost_in_booking_currency,
               CASE WHEN x.sale_currency = b.currency THEN x.sale_amount - x.cost_in_sale_currency END
                 AS margin_in_booking_currency,
               x.start_date, x.end_date
        FROM {s}.booking_services x
        JOIN {s}.bookings b ON b.booking_ref = x.booking_ref
        LEFT JOIN v_service_destination d
          ON d.booking_ref = x.booking_ref AND d.service_type = x.service_type AND d.id = x.service_id""")
    sql("""CREATE OR REPLACE TEMP VIEW v_obt_booking_service AS
        SELECT f.*, fb.status AS booking_status, fb.is_active AS booking_is_active, fb.trip_type, fb.sales_channel,
               fb.source_market_code, fb.source_market_name, fb.pax, fb.total_price AS booking_total_price,
               cd.date AS booking_created_date, cd.year_month AS booking_created_month,
               sd.year_month AS service_start_month,
               ag.agency_name, ag.country AS agency_country, ag.is_umbrella,
               sp.supplier_name, sp.supplier_source, sp.country AS supplier_country,
               st.service_category
        FROM v_fact_booking_service f
        JOIN v_fact_booking fb ON fb.booking_ref = f.booking_ref
        LEFT JOIN v_dim_date cd ON cd.date_key = f.booking_created_date_key
        LEFT JOIN v_dim_date sd ON sd.date_key = f.service_start_date_key
        LEFT JOIN v_dim_agency ag ON ag.agency_key = f.agency_key
        LEFT JOIN v_dim_supplier sp ON sp.supplier_key = f.supplier_key
        LEFT JOIN v_dim_service_type st ON st.service_type = f.service_type""")

    # ---------------- audit: checks on the views before anything is replaced -------------------
    checks = {
        "gold_dim_agency_key_unique": (
            "block",
            "SELECT agency_key AS k FROM v_dim_agency GROUP BY 1 HAVING count(*) > 1",
        ),
        "gold_dim_supplier_key_unique": (
            "block",
            "SELECT supplier_key AS k FROM v_dim_supplier GROUP BY 1 HAVING count(*) > 1",
        ),
        "gold_fact_booking_grain": (
            "block",
            "SELECT booking_ref AS k FROM v_fact_booking GROUP BY 1 HAVING count(*) > 1",
        ),
        "gold_fact_service_grain": (
            "block",
            "SELECT service_key AS k FROM v_fact_booking_service GROUP BY 1 HAVING count(*) > 1",
        ),
        "gold_fact_without_agency": (
            "block",
            """SELECT booking_ref AS k FROM v_fact_booking f
            LEFT ANTI JOIN v_dim_agency d ON d.agency_key = f.agency_key""",
        ),
        "gold_fact_without_supplier": (
            "block",
            """SELECT service_key AS k FROM v_fact_booking_service f
            LEFT ANTI JOIN v_dim_supplier d ON d.supplier_key = f.supplier_key""",
        ),
        "gold_fact_without_service_type": (
            "block",
            """SELECT service_key AS k FROM v_fact_booking_service f
            LEFT ANTI JOIN v_dim_service_type d ON d.service_type = f.service_type""",
        ),
        "gold_fact_date_out_of_range": (
            "block",
            """SELECT booking_ref AS k FROM v_fact_booking f
            LEFT ANTI JOIN v_dim_date d ON d.date_key = f.created_date_key""",
        ),
        # the model must keep every booking and service of silver, and the same revenue
        "gold_booking_count_matches_silver": (
            "block",
            f"""SELECT 'fact_booking' AS k FROM (SELECT 1)
            WHERE (SELECT count(*) FROM v_fact_booking) <> (SELECT count(*) FROM {s}.bookings)""",
        ),
        "gold_service_count_matches_silver": (
            "block",
            f"""SELECT 'fact_booking_service' AS k FROM (SELECT 1)
            WHERE (SELECT count(*) FROM v_fact_booking_service) <> (SELECT count(*) FROM {s}.booking_services)""",
        ),
        "gold_revenue_matches_silver": (
            "block",
            f"""SELECT 'total_price' AS k FROM (SELECT 1)
            WHERE abs((SELECT coalesce(sum(total_price), 0) FROM v_fact_booking)
                    - (SELECT coalesce(sum(total_price), 0) FROM {s}.bookings)) > 0.01""",
        ),
        "gold_obt_row_count": (
            "block",
            """SELECT 'obt_booking_service' AS k FROM (SELECT 1)
            WHERE (SELECT count(*) FROM v_obt_booking_service) <> (SELECT count(*) FROM v_fact_booking_service)""",
        ),
        "gold_service_without_destination": (
            "warn",
            """SELECT service_key AS k FROM v_fact_booking_service
            WHERE destination_country IS NULL AND is_active""",
        ),
        "gold_supplier_unknown": (
            "warn",
            f"""SELECT service_key AS k FROM v_fact_booking_service
            WHERE supplier_key = '{UNKNOWN}' AND is_active""",
        ),
    }
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ops.dq_results (
        run_id STRING, checked_at TIMESTAMP, check_name STRING, severity STRING, failed_rows BIGINT, sample STRING)""")
    rows_out, blocked = [], []
    for name, (severity, q) in checks.items():
        r = sql(f"SELECT count(*) AS n, slice(collect_list(k), 1, 5) AS s FROM ({q})").collect()[0]
        rows_out.append((a.run_id, name, severity, r["n"], ",".join(r["s"] or [])))
        print(f"DQ {severity:5} {name}: {r['n']} sample={r['s']}")
        if r["n"] and severity == "block":
            blocked.append(name)
    spark.createDataFrame(
        rows_out, "run_id STRING, check_name STRING, severity STRING, failed_rows BIGINT, sample STRING"
    ).selectExpr(
        "run_id", "current_timestamp() AS checked_at", "check_name", "severity", "failed_rows", "sample"
    ).write.mode("append").saveAsTable(f"{c}.ops.dq_results")
    if blocked:
        raise RuntimeError(f"blocking data-quality checks failed, gold tables left unchanged: {blocked}")

    # ---------------- publish: replace the gold tables from the checked views -------------------
    cluster = {
        "fact_booking": "booking_ref",
        "fact_booking_service": "booking_ref",
        "obt_booking_service": "booking_created_month",
    }
    for t in TABLES:
        by = f" CLUSTER BY ({cluster[t]})" if t in cluster else ""
        sql(f"CREATE OR REPLACE TABLE {c}.{SCHEMA}.{t}{by} AS SELECT * FROM v_{t}")
        print(t, sql(f"SELECT count(*) FROM {c}.{SCHEMA}.{t}").collect()[0][0])
    for t in PII_TABLES:
        sql(f"ALTER TABLE {c}.{SCHEMA}.{t} SET TAGS ('contains_pii' = 'true')")
    record_build(sql, c, "star_schema", a.run_id, token)


if __name__ == "__main__":
    main()
