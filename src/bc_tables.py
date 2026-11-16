"""Task bc_tables - integration output shaped for the Business Central API, catalog <catalog>:

business_central.*    customers, vendors, sales and purchase invoices and lines, each document with a payload
                      hash and an action (UPSERT, or CANCEL for cancelled bookings and services)
ops.dq_results        data-quality results of this run
ops.publish           write-audit-publish: table versions are published only when every blocking check passes;
                      local/push_to_bc.py reads the tables AS OF the last published versions, so it never sees a
                      half-written or failed build, and header and line tables always match.

Check names start with the layer they test (silver_, bronze_, bc_). End-to-end freshness checks run in the last
task, data_quality.py. The task skips the rebuild when silver and ref have not changed since the last publish
(content token in ops.build_state, recorded on every run); job parameter rebuild=true forces it.
"""

import argparse
import hashlib
import json

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
SCHEMA = "business_central"
TABLES = ["customers", "vendors", "sales_invoices", "sales_lines", "purchase_invoices", "purchase_lines"]
PII_TABLES = ["customers", "vendors"]


def silver_token(sql, c: str) -> str:
    """Token that changes only when the data this task reads changes.

    Bookings: row count and a hash of every (booking_ref, last_update_at); services change only with their booking.
    Agencies and suppliers: count and a hash of every current row. Ref tables: their Delta version, which changes
    only on an edit. Pipeline tables are refreshed every run, so their Delta version is not a change signal.
    """
    parts = [
        str(
            sql(f"""SELECT count(*) || ':' || coalesce(sum(CAST(xxhash64(booking_ref, last_update_at) AS DECIMAL(38, 0))), 0)
                         FROM {c}.silver.bookings""").first()[0]
        )
    ]
    parts += [
        str(
            sql(f"""SELECT count(*) || ':' || coalesce(sum(CAST(xxhash64(*) AS DECIMAL(38, 0))), 0)
                         FROM {c}.silver.{t}""").first()[0]
        )
        for t in ("agencies", "suppliers")
    ]
    parts += [
        str(sql(f"DESCRIBE HISTORY {c}.ref.{t} LIMIT 1").first()["version"])
        for t in ("umbrella_agency", "customer_map", "vendor_map")
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def silver_unchanged(sql, c: str, task: str, rebuild: str) -> tuple[bool, str]:
    """True when silver and ref have not changed since this task last built; also returns the current token."""
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ops.build_state (
        task STRING, run_id STRING, built_at TIMESTAMP, silver_token STRING)""")
    last = sql(f"""SELECT max_by(silver_token, built_at) AS t FROM {c}.ops.build_state
                   WHERE task = '{task}'""").first()["t"]
    token = silver_token(sql, c)
    return (rebuild.lower() != "true" and last == token), token


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
    b = f"{c}.{SCHEMA}"
    # ref: mapping tables maintained by Vio Finance, created empty on the first run
    sql(f"CREATE TABLE IF NOT EXISTS {c}.ref.umbrella_agency (agency_id STRING, note STRING)")
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ref.customer_map (
        agency_reference_key STRING, bc_customer_no STRING, display_name STRING, approved_by STRING, note STRING)""")
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ref.vendor_map (
        source_kind STRING, source_key STRING, bc_vendor_no STRING, display_name STRING, approved_by STRING,
        note STRING)""")
    unchanged, token = silver_unchanged(sql, c, "bc_tables", a.rebuild)
    if unchanged:
        # recorded anyway, so data_quality sees that this task checked the current silver
        record_build(sql, c, "bc_tables", a.run_id, token)
        print("silver and ref unchanged since the last publish, nothing to do")
        return

    # ---------------- business_central: shapes of the Business Central API ---------------------------------------------
    # Number prefixes keep Travel Compositor keys visible in Business Central and inside the 20-character
    # limit of "No." fields.
    sql(f"""CREATE OR REPLACE TABLE {b}.customers AS
        SELECT 'AG' || agency_id AS bc_number, agency_id AS tc_agency_id,
               left(coalesce(nullif(trim(name),''), business_name), 100) AS display_name,
               left(regexp_extract(coalesce(billing_email, email), '[^,; ]+@[^,; ]+', 0), 80) AS email,
               left(country, 10) AS country, left(city, 30) AS city, left(address, 100) AS address_line1,
               left(postal_code, 20) AS postal_code,
               sha2(to_json(named_struct('n', name, 'e', coalesce(billing_email, email), 'c', country,
                                         'ci', city, 'a', address)), 256) AS payload_hash
        FROM {c}.silver.agencies
        UNION ALL
        SELECT DISTINCT m.bc_customer_no, NULL, m.display_name, NULL, NULL, NULL, NULL, NULL,
               sha2(m.display_name, 256)
        FROM {c}.ref.customer_map m
        WHERE m.bc_customer_no NOT IN (SELECT 'AG' || agency_id FROM {c}.silver.agencies)""")
    # Documents are created for BOOKED and RQ; CANCELED only updates what was sent before
    # (YVS spec API_Sales_T01 row 6).
    sql(f"""CREATE OR REPLACE TEMP VIEW booking_customer AS
        SELECT b.*, lower(trim(b.payload:agencyBookingReference::string)) AS agency_reference_key,
               CASE WHEN u.agency_id IS NULL THEN 'AG' || b.agency_id ELSE m.bc_customer_no END AS customer_number,
               CASE WHEN u.agency_id IS NOT NULL AND m.bc_customer_no IS NULL THEN 'CUSTOMER_UNMAPPED'
                    WHEN u.agency_id IS NULL AND ag.agency_id IS NULL THEN 'AGENCY_NOT_IN_SNAPSHOT' END
                 AS hold_reason,
               CASE WHEN b.status IN ('BOOKED', 'RQ') THEN 'UPSERT' ELSE 'CANCEL' END AS doc_action
        FROM {c}.silver.bookings b
        LEFT JOIN {c}.ref.umbrella_agency u ON u.agency_id = b.agency_id
        LEFT JOIN {c}.ref.customer_map m
          ON m.agency_reference_key = lower(trim(b.payload:agencyBookingReference::string))
        LEFT JOIN {c}.silver.agencies ag ON ag.agency_id = b.agency_id
        WHERE b.status IN ('BOOKED', 'RQ', 'CANCELED')""")
    sql(f"""CREATE OR REPLACE TABLE {b}.sales_lines AS
        SELECT s.booking_ref, s.service_id,
               row_number() OVER (PARTITION BY s.booking_ref ORDER BY s.service_id, s.service_type) AS line_no,
               left(s.service_type || ' - ' || trim(regexp_replace(regexp_replace(coalesce(s.name, ''), '<[^>]*>', ' '),
                    '\\\\s+', ' ')), 100) AS description,
               s.sale_amount AS amount, s.sale_currency AS currency
        FROM {c}.silver.booking_services s
        JOIN booking_customer b ON b.booking_ref = s.booking_ref AND b.doc_action = 'UPSERT'
        WHERE s.status IN ('BOOKED', 'RQ') AND s.sale_amount <> 0""")
    sql(f"""CREATE OR REPLACE TABLE {b}.sales_invoices AS
        SELECT b.booking_ref AS external_document_number, b.doc_action, b.customer_number, b.hold_reason, b.status,
               b.agency_id, b.payload:agencyBookingReference::string AS agency_reference,
               to_date(b.created_at) AS invoice_date, b.start_date AS service_start_date, b.currency AS currency_code,
               b.total_price, coalesce(l.lines_total, 0) AS lines_total, l.line_currencies, b.last_update_at,
               -- the hash of an UPSERT document keeps the shape used since the first push, so a code change
               -- does not make every synced document look new; a CANCEL gets its own hash
               sha2(CASE WHEN b.doc_action = 'CANCEL' THEN 'CANCEL ' ELSE '' END ||
                    to_json(named_struct('c', b.customer_number, 'cur', b.currency, 'd', to_date(b.created_at),
                                         'l', l.lines)), 256) AS payload_hash
        FROM booking_customer b
        LEFT JOIN (SELECT booking_ref, sum(amount) AS lines_total,
                          array_join(array_sort(collect_set(currency)), ',') AS line_currencies,
                          sort_array(collect_list(named_struct('n', line_no, 'd', description, 'a', amount))) AS lines
                   FROM {b}.sales_lines GROUP BY booking_ref) l ON l.booking_ref = b.booking_ref
        WHERE b.doc_action = 'CANCEL' OR l.booking_ref IS NOT NULL""")
    # Purchases: one invoice per service (YVS spec API_Purchases_T01 row 5). Vendor: Travel Compositor
    # supplier ID; for API services without one, the provider code; for manual services, ref.vendor_map.
    sql(f"""CREATE OR REPLACE TEMP VIEW service_vendor AS
        SELECT s.*, b.created_at AS booking_created_at,
               CASE WHEN b.doc_action = 'UPSERT' AND s.status IN ('BOOKED', 'RQ') THEN 'UPSERT' ELSE 'CANCEL' END
                 AS doc_action,
               CASE WHEN s.supplier_id IS NOT NULL THEN 'SP' || s.supplier_id
                    WHEN NOT s.is_manual AND s.provider IS NOT NULL
                      THEN coalesce(mp.bc_vendor_no, left('PV' || regexp_replace(s.provider, '[^A-Z0-9]', ''), 20))
                    ELSE mm.bc_vendor_no END AS vendor_number,
               CASE WHEN s.supplier_id IS NULL AND (s.is_manual OR s.provider IS NULL) AND mm.bc_vendor_no IS NULL
                    THEN 'VENDOR_UNMAPPED'
                    WHEN s.supplier_id IS NOT NULL AND sp.supplier_id IS NULL THEN 'SUPPLIER_NOT_IN_SNAPSHOT' END
                 AS hold_reason
        FROM {c}.silver.booking_services s
        JOIN booking_customer b ON b.booking_ref = s.booking_ref
        LEFT JOIN {c}.ref.vendor_map mp ON mp.source_kind = 'PROVIDER' AND mp.source_key = s.provider
        LEFT JOIN {c}.ref.vendor_map mm ON mm.source_kind = 'MANUAL_NAME'
                                        AND mm.source_key = lower(trim(s.supplier_name))
        LEFT JOIN {c}.silver.suppliers sp ON sp.supplier_id = s.supplier_id
        WHERE s.cost_amount IS NOT NULL AND s.cost_amount <> 0""")
    sql(f"""CREATE OR REPLACE TABLE {b}.purchase_invoices AS
        -- Travel Compositor service IDs are unique only inside one service type, so the key adds a type code.
        SELECT service_id || '-' || type_code AS purchase_key, doc_action, booking_ref, service_id, service_type,
               is_manual, vendor_number, hold_reason, status, supplier_id, provider, supplier_name,
               cost_currency AS currency_code, service_id || '-' || type_code AS vendor_invoice_number,
               provider_reference AS vendor_order_number, to_date(booking_created_at) AS invoice_date,
               cost_amount AS lines_total,
               sha2(CASE WHEN doc_action = 'CANCEL' THEN 'CANCEL ' ELSE '' END ||
                    to_json(named_struct('v', vendor_number, 'c', cost_currency, 'a', cost_amount,
                                         'd', to_date(booking_created_at))), 256) AS payload_hash
        FROM (SELECT *, CASE service_type WHEN 'hotelservice' THEN 'H' WHEN 'transferservice' THEN 'TF'
                 WHEN 'transportservice' THEN 'TP' WHEN 'ticketservice' THEN 'TK' WHEN 'closedtourservice' THEN 'CT'
                 WHEN 'carservice' THEN 'CR' WHEN 'insuranceservice' THEN 'IN' WHEN 'cruiseservice' THEN 'CU'
                 WHEN 'itemservice' THEN 'IT'
                 ELSE 'M' || upper(left(replace(service_type, 'manual_', ''), 2)) END AS type_code FROM service_vendor)""")
    sql(f"""CREATE OR REPLACE TABLE {b}.purchase_lines AS
        SELECT purchase_key, booking_ref, 1 AS line_no,
               left(service_type || ' - ' || coalesce(supplier_name, provider, ''), 100) AS description,
               lines_total AS amount, currency_code AS currency
        FROM {b}.purchase_invoices WHERE doc_action = 'UPSERT'""")
    sql(f"""CREATE OR REPLACE TABLE {b}.vendors AS
        SELECT 'SP' || supplier_id AS bc_number, supplier_id AS tc_supplier_id,
               left(coalesce(nullif(trim(legal_name),''), commercial_name), 100) AS display_name,
               left(regexp_extract(email, '[^,; ]+@[^,; ]+', 0), 80) AS email, left(country, 10) AS country,
               left(address, 100) AS address_line1, left(tax_id, 20) AS tax_registration_number,
               sha2(to_json(named_struct('n', coalesce(legal_name, commercial_name), 'e', email, 'c', country,
                                         'a', address, 't', tax_id)), 256) AS payload_hash
        FROM {c}.silver.suppliers
        UNION ALL
        SELECT bc_number, NULL, display_name, NULL, NULL, NULL, NULL, sha2(display_name, 256) FROM (
          SELECT DISTINCT vendor_number AS bc_number, provider AS display_name FROM {b}.purchase_invoices
          WHERE supplier_id IS NULL AND NOT is_manual AND vendor_number LIKE 'PV%'
          UNION SELECT DISTINCT bc_vendor_no, display_name FROM {c}.ref.vendor_map) v
        WHERE bc_number NOT IN (SELECT 'SP' || supplier_id FROM {c}.silver.suppliers)""")
    for t in PII_TABLES:
        sql(f"ALTER TABLE {b}.{t} SET TAGS ('contains_pii' = 'true')")

    # ---------------- audit: data quality -------------------------------------------------------
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ops.dq_results (
        run_id STRING, checked_at TIMESTAMP, check_name STRING, severity STRING, failed_rows BIGINT, sample STRING)""")
    send_sales = f"(SELECT * FROM {b}.sales_invoices WHERE hold_reason IS NULL AND doc_action = 'UPSERT')"
    send_purch = f"(SELECT * FROM {b}.purchase_invoices WHERE hold_reason IS NULL AND doc_action = 'UPSERT')"
    known = ",".join(f"'{k}'" for k in SERVICE_ARRAYS + ["manualServices"])
    checks = {
        # block: the push step would send broken or ambiguous documents, nothing is published
        "silver_booking_ref_unique": (
            "block",
            f"""SELECT booking_ref AS k FROM {c}.silver.bookings
            GROUP BY booking_ref HAVING count(*) > 1""",
        ),
        "bc_sales_key_unique": (
            "block",
            f"""SELECT external_document_number AS k FROM {b}.sales_invoices
            GROUP BY 1 HAVING count(*) > 1""",
        ),
        "bc_purchase_key_unique": (
            "block",
            f"""SELECT purchase_key AS k FROM {b}.purchase_invoices
            GROUP BY 1 HAVING count(*) > 1""",
        ),
        "bc_party_number_unique": (
            "block",
            f"""SELECT bc_number AS k FROM {b}.customers GROUP BY 1 HAVING count(*) > 1
            UNION ALL SELECT bc_number FROM {b}.vendors GROUP BY 1 HAVING count(*) > 1""",
        ),
        "bc_sales_invoice_without_customer": (
            "block",
            f"""SELECT external_document_number AS k FROM {send_sales} i
            LEFT ANTI JOIN {b}.customers p ON p.bc_number = i.customer_number""",
        ),
        "bc_purchase_invoice_without_vendor": (
            "block",
            f"""SELECT purchase_key AS k FROM {send_purch} i
            LEFT ANTI JOIN {b}.vendors p ON p.bc_number = i.vendor_number""",
        ),
        "bc_document_without_currency": (
            "block",
            f"""SELECT external_document_number AS k FROM {send_sales}
            WHERE currency_code IS NULL UNION ALL SELECT purchase_key FROM {send_purch} WHERE currency_code IS NULL""",
        ),
        "bc_sales_lines_mixed_currency": (
            "block",
            f"""SELECT external_document_number AS k FROM {send_sales}
            WHERE line_currencies <> currency_code""",
        ),
        "bc_field_length_over_limit": (
            "block",
            f"""SELECT bc_number AS k FROM {b}.customers WHERE length(bc_number) > 20
            UNION ALL SELECT bc_number FROM {b}.vendors WHERE length(bc_number) > 20
            UNION ALL SELECT purchase_key FROM {send_purch} WHERE length(vendor_invoice_number) > 35
            UNION ALL SELECT external_document_number FROM {send_sales} WHERE length(external_document_number) > 35""",
        ),
        # warn: recorded in ops.dq_results only
        "bronze_malformed_line": (
            "warn",
            f"""SELECT source_file AS k FROM {c}.bronze.tc_bookings WHERE doc IS NULL
            UNION ALL SELECT source_file FROM {c}.bronze.tc_agencies WHERE doc IS NULL
            UNION ALL SELECT source_file FROM {c}.bronze.tc_suppliers WHERE doc IS NULL""",
        ),
        "silver_booking_without_last_update": (
            "warn",
            f"""SELECT booking_ref AS k FROM {c}.silver.bookings
            WHERE last_update_at IS NULL""",
        ),
        "silver_unknown_service_array": (
            "warn",
            f"""SELECT DISTINCT e.key AS k FROM {c}.silver.bookings b,
            LATERAL variant_explode(b.payload) e WHERE lower(e.key) LIKE '%service%' AND e.key NOT IN ({known})""",
        ),
        "silver_active_service_without_amount": (
            "warn",
            f"""SELECT booking_ref || '/' || service_id AS k
            FROM {c}.silver.booking_services WHERE status IN ('BOOKED','RQ') AND (sale_amount IS NULL OR cost_amount IS NULL)""",
        ),
        "bc_active_booking_without_lines": (
            "warn",
            f"""SELECT b.booking_ref AS k FROM {c}.silver.bookings b
            LEFT ANTI JOIN {b}.sales_invoices i ON i.external_document_number = b.booking_ref
            WHERE b.status IN ('BOOKED','RQ')""",
        ),
        "bc_sales_held_customer_unmapped": (
            "warn",
            f"""SELECT external_document_number AS k
            FROM {b}.sales_invoices WHERE hold_reason = 'CUSTOMER_UNMAPPED' AND doc_action = 'UPSERT'""",
        ),
        "bc_purchase_held_vendor_unmapped": (
            "warn",
            f"""SELECT purchase_key AS k
            FROM {b}.purchase_invoices WHERE hold_reason = 'VENDOR_UNMAPPED' AND doc_action = 'UPSERT'""",
        ),
        "bc_held_party_not_in_snapshot": (
            "warn",
            f"""SELECT external_document_number AS k FROM {b}.sales_invoices
            WHERE hold_reason = 'AGENCY_NOT_IN_SNAPSHOT' UNION ALL SELECT purchase_key FROM {b}.purchase_invoices
            WHERE hold_reason = 'SUPPLIER_NOT_IN_SNAPSHOT'""",
        ),
        "bc_booking_total_differs_from_lines": (
            "warn",
            f"""SELECT external_document_number AS k FROM {send_sales}
            WHERE abs(total_price - lines_total) > 0.05""",
        ),
        "bc_negative_document_total": (
            "warn",
            f"""SELECT external_document_number AS k FROM {send_sales}
            WHERE lines_total < 0 UNION ALL SELECT purchase_key FROM {send_purch} WHERE lines_total < 0""",
        ),
    }
    failed = {"block": []}
    rows_out = []
    for name, (severity, q) in checks.items():
        r = sql(f"SELECT count(*) AS n, slice(collect_list(k), 1, 5) AS s FROM ({q})").collect()[0]
        rows_out.append((a.run_id, name, severity, r["n"], ",".join(r["s"] or [])))
        print(f"DQ {severity:5} {name}: {r['n']} sample={r['s']}")
        if r["n"] and severity in failed:
            failed[severity].append(name)
    spark.createDataFrame(
        rows_out, "run_id STRING, check_name STRING, severity STRING, failed_rows BIGINT, sample STRING"
    ).selectExpr(
        "run_id", "current_timestamp() AS checked_at", "check_name", "severity", "failed_rows", "sample"
    ).write.mode("append").saveAsTable(f"{c}.ops.dq_results")

    # ---------------- publish: only a build that passed every blocking check --------------------
    sql(f"CREATE TABLE IF NOT EXISTS {c}.ops.publish (run_id STRING, published_at TIMESTAMP, versions STRING)")
    for t in TABLES:
        print(t, sql(f"SELECT count(*) FROM {b}.{t}").collect()[0][0])
    if failed["block"]:
        raise RuntimeError(f"blocking data-quality checks failed, nothing published: {failed['block']}")
    versions = {t: sql(f"DESCRIBE HISTORY {b}.{t} LIMIT 1").collect()[0]["version"] for t in TABLES}
    spark.createDataFrame([(a.run_id, json.dumps(versions))], "run_id STRING, versions STRING").selectExpr(
        "run_id", "current_timestamp() AS published_at", "versions"
    ).write.mode("append").saveAsTable(f"{c}.ops.publish")
    print("published", versions)
    record_build(sql, c, "bc_tables", a.run_id, token)


if __name__ == "__main__":
    main()
