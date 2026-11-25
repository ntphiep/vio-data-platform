"""Task build_business_central_tables - integration output shaped for the Business Central API, catalog <catalog>:

business_central.*    customers, vendors, sales and purchase invoices and lines, each document with a payload
                      hash and an action (UPSERT, or CANCEL for cancelled bookings and services)
ops.dq_results        data-quality results of this run
ops.publish           write-audit-publish: table versions are published only when every blocking check passes;
                      task push_to_business_central reads the tables AS OF the last published versions, so it never sees a
                      half-written or failed build, and header and line tables always match.

Check names start with the layer they test (silver_, bronze_, business_central_). End-to-end freshness checks run in the last
task, check_data_quality.py. The task skips the rebuild when silver and ref have not changed since the last publish
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
# Table comments, set again after every CREATE OR REPLACE (which drops them).
TABLE_COMMENTS = {
    "customers": "Business Central customers: one per Travel Compositor agency (AG<id>), plus customers of ref.customer_map.",
    "vendors": "Business Central vendors: Travel Compositor suppliers (SP<id>), providers (PV<code>), ref.vendor_map.",
    "sales_invoices": "One sales invoice per BOOKED, RQ or CANCELED booking (externalDocumentNumber); CANCEL when the booking is cancelled or has no line left; held when hold_reason is set.",
    "sales_lines": "Lines of sales_invoices: one per BOOKED or RQ service with a non-zero sale amount (sale price plus amendments), in the sale currency.",
    "purchase_invoices": "One purchase invoice per service with a non-zero cost (vendorInvoiceNumber = purchase_key), plus CANCEL rows for cancelled services; held when hold_reason is set.",
    "purchase_lines": "One line per UPSERT purchase invoice: the service cost plus amendments in the cost currency.",
}
PII_TABLES = ["customers", "vendors"]
# Type code in purchase_key and vendor_invoice_number: Travel Compositor service IDs are unique only inside one
# service type. Codes of synced documents must not change (the push would treat the old key as removed), so
# every manual type keeps the code of the former rule 'M' || upper(left(type, 2)), except manual_transport, which
# shared MTR with manual_transfer and now takes MTP, after TP of transportservice.
TYPE_CODES = {
    "hotelservice": "H",
    "transferservice": "TF",
    "transportservice": "TP",
    "ticketservice": "TK",
    "closedtourservice": "CT",
    "carservice": "CR",
    "insuranceservice": "IN",
    "cruiseservice": "CU",
    "itemservice": "IT",
    "manual_hotel": "MHO",
    "manual_transfer": "MTR",
    "manual_transport": "MTP",
    "manual_ticket": "MTI",
    "manual_closedTour": "MCL",
    "manual_carRental": "MCA",
    "manual_cruise": "MCR",
    "manual_insurance": "MIN",
    "manual_other": "MOT",
    "manual_accountingAdjustment": "MAC",
}
# Vendor number of an API service without supplier ID: the provider code upper-cased before non-alphanumeric
# characters are removed, so lowercase letters are kept (Business Central Code fields are upper case). Two
# providers that still map to one number make blocking check business_central_party_number_unique fail; ref.vendor_map
# (source_kind PROVIDER) then gives one of them its own number.
PROVIDER_VENDOR_NUMBER = "left('PV' || regexp_replace(upper(s.provider), '[^A-Z0-9]', ''), 20)"


def type_code_sql(col: str = "service_type") -> str:
    """CASE expression for TYPE_CODES; an unknown manual type falls back to the former rule."""
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in TYPE_CODES.items())
    return f"CASE {col} {whens} ELSE 'M' || upper(left(replace({col}, 'manual_', ''), 2)) END"


def silver_token(sql, c: str) -> str:
    """Token that changes only when the data this task reads changes.

    Bookings: row count and a hash of every (booking_ref, last_update_at, payload): Travel Compositor sometimes
    changes amounts or services without moving lastUpdateDate, so the payload is part of the token.
    Agencies and suppliers: count and a hash of every current row. Ref tables: their Delta version, which changes
    only on an edit. Pipeline tables are refreshed every run, so their Delta version is not a change signal.
    """
    parts = [
        str(
            sql(f"""SELECT count(*) || ':' || coalesce(sum(CAST(xxhash64(booking_ref, last_update_at, CAST(payload AS STRING))
                                                       AS DECIMAL(38, 0))), 0)
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
    unchanged, token = silver_unchanged(sql, c, "build_business_central_tables", a.rebuild)
    if unchanged:
        # recorded anyway, so check_data_quality sees that this task checked the current silver
        record_build(sql, c, "build_business_central_tables", a.run_id, token)
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
               left(postal_code, 20) AS postal_code, left(phone, 30) AS phone_number,
               left(tax_id, 20) AS tax_registration_number, invoice_type, deferred_payment_allowed,
               deferred_days, deferred_limit AS credit_limit,
               sha2(to_json(named_struct('n', name, 'e', coalesce(billing_email, email), 'c', country,
                                         'ci', city, 'a', address)), 256) AS payload_hash
        FROM {c}.silver.agencies
        UNION ALL
        SELECT DISTINCT m.bc_customer_no, NULL, m.display_name, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
               NULL, NULL, NULL, sha2(m.display_name, 256)
        FROM {c}.ref.customer_map m
        WHERE m.bc_customer_no NOT IN (SELECT 'AG' || agency_id FROM {c}.silver.agencies)""")
    # Documents are created for BOOKED and RQ; CANCELED only updates what was sent before
    # (implementation partner spec API_Sales_T01 row 6).
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
               s.sale_amount + coalesce(s.amendment_sale_amount, 0) AS amount, s.sale_currency AS currency
        FROM {c}.silver.booking_services s
        JOIN booking_customer b ON b.booking_ref = s.booking_ref AND b.doc_action = 'UPSERT'
        WHERE s.status IN ('BOOKED', 'RQ') AND s.sale_amount + coalesce(s.amendment_sale_amount, 0) <> 0""")
    # A booking keeps its sales invoice row while it is BOOKED, RQ or CANCELED. When it has no line left (every
    # service cancelled or sold at 0) the row stays with doc_action CANCEL, so the push empties a draft it sent
    # before through the normal path; a document that vanished from the table would count as an unexplained removal.
    # A CANCEL row is never held.
    sql(f"""CREATE OR REPLACE TABLE {b}.sales_invoices AS
        SELECT b.booking_ref AS external_document_number, b.doc_action, b.customer_number,
               CASE WHEN b.doc_action = 'UPSERT' THEN b.hold_reason END AS hold_reason, b.status,
               b.agency_id, b.payload:agencyBookingReference::string AS agency_reference,
               to_date(b.created_at) AS invoice_date, b.start_date AS service_start_date, b.currency AS currency_code,
               b.total_price, coalesce(l.lines_total, 0) AS lines_total, l.line_currencies, b.last_update_at,
               -- fields of the partner sales specification (API_Sales_T01) that Travel Compositor provides; the
               -- push does not send them yet, they wait for the partner endpoints and dimensions
               b.main_booking_ref, b.referral_id AS your_reference, b.sales_channel, b.microsite, b.trip_type,
               b.end_date AS service_end_date, b.pax, b.username AS salesperson_username, b.agency_management_fee,
               b.paid_amount AS deposit_amount, b.deferred_amount AS balance_amount,
               b.deferred_limit_date AS balance_due_date, sv.cancellation_date, sv.destination_country,
               -- the hash of an UPSERT document keeps the shape used since the first push, so a code change
               -- does not make every synced document look new; a CANCEL gets its own hash
               sha2(CASE WHEN b.doc_action = 'CANCEL' THEN 'CANCEL ' ELSE '' END ||
                    to_json(named_struct('c', b.customer_number, 'cur', b.currency, 'd', to_date(b.created_at),
                                         'l', l.lines)), 256) AS payload_hash
        FROM (SELECT bc.* EXCEPT (doc_action),
                     CASE WHEN bc.doc_action = 'UPSERT' AND bl.booking_ref IS NULL THEN 'CANCEL' ELSE bc.doc_action END
                       AS doc_action
              FROM booking_customer bc
              LEFT JOIN (SELECT DISTINCT booking_ref FROM {b}.sales_lines) bl ON bl.booking_ref = bc.booking_ref) b
        LEFT JOIN (SELECT booking_ref, sum(amount) AS lines_total,
                          array_join(array_sort(collect_set(currency)), ',') AS line_currencies,
                          sort_array(collect_list(named_struct('n', line_no, 'd', description, 'a', amount))) AS lines
                   FROM {b}.sales_lines GROUP BY booking_ref) l ON l.booking_ref = b.booking_ref
        LEFT JOIN (SELECT booking_ref, max(cancellation_date) AS cancellation_date,
                          min_by(destination_country, start_date) AS destination_country
                   FROM {c}.silver.booking_services GROUP BY booking_ref) sv ON sv.booking_ref = b.booking_ref""")
    # Purchases: one invoice per service (implementation partner spec API_Purchases_T01 row 5). Vendor: Travel Compositor
    # supplier ID; for API services without one, the provider code; for manual services, ref.vendor_map.
    sql(f"""CREATE OR REPLACE TEMP VIEW service_vendor AS
        SELECT s.* EXCEPT (cost_amount), s.cost_amount + coalesce(s.amendment_cost_amount, 0) AS cost_amount,
               b.created_at AS booking_created_at,
               CASE WHEN b.doc_action = 'UPSERT' AND s.status IN ('BOOKED', 'RQ') THEN 'UPSERT' ELSE 'CANCEL' END
                 AS doc_action,
               CASE WHEN s.supplier_id IS NOT NULL THEN 'SP' || s.supplier_id
                    WHEN NOT s.is_manual AND s.provider IS NOT NULL
                      THEN coalesce(mp.bc_vendor_no, {PROVIDER_VENDOR_NUMBER})
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
        WHERE s.cost_amount + coalesce(s.amendment_cost_amount, 0) <> 0
           OR NOT (b.doc_action = 'UPSERT' AND s.status IN ('BOOKED', 'RQ'))""")
    sql(f"""CREATE OR REPLACE TABLE {b}.purchase_invoices AS
        -- Travel Compositor service IDs are unique only inside one service type, so the key adds a type code.
        SELECT service_id || '-' || type_code AS purchase_key, doc_action, booking_ref, service_id, service_type,
               is_manual, vendor_number, CASE WHEN doc_action = 'UPSERT' THEN hold_reason END AS hold_reason, status, supplier_id, provider, supplier_name,
               cost_currency AS currency_code, service_id || '-' || type_code AS vendor_invoice_number,
               provider_reference AS vendor_order_number, to_date(booking_created_at) AS invoice_date,
               -- fields of the partner purchase specification (API_Purchases_T01), not sent by the push yet
               supplier_confirmation, cancellation_date, exchange_rate_microsite AS currency_factor,
               amendment_count, start_date AS service_start_date, end_date AS service_end_date,
               cost_amount AS lines_total,
               sha2(CASE WHEN doc_action = 'CANCEL' THEN 'CANCEL ' ELSE '' END ||
                    to_json(named_struct('v', vendor_number, 'c', cost_currency, 'a', cost_amount,
                                         'd', to_date(booking_created_at))), 256) AS payload_hash
        FROM (SELECT *, {type_code_sql()} AS type_code,
                     coalesce(cost_amount, 0) <> 0 AS has_cost FROM service_vendor)
        -- Travel Compositor usually sets the net cost of a cancelled service to 0. Such a service stays as a CANCEL row,
        -- so the push empties the draft it sent before; one without a cost is kept only when its key is not taken
        -- by a service with a cost and once per key, so blocking check business_central_purchase_key_unique
        -- still tests the services with a cost only.
        QUALIFY has_cost OR (NOT max(has_cost) OVER (PARTITION BY service_id || '-' || type_code)
                             AND row_number() OVER (PARTITION BY service_id || '-' || type_code
                                                    ORDER BY booking_created_at DESC) = 1)""")
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
               left(phone, 30) AS phone_number,
               sha2(to_json(named_struct('n', coalesce(legal_name, commercial_name), 'e', email, 'c', country,
                                         'a', address, 't', tax_id)), 256) AS payload_hash
        FROM {c}.silver.suppliers
        UNION ALL
        SELECT bc_number, NULL, display_name, NULL, NULL, NULL, NULL, NULL, sha2(display_name, 256) FROM (
          SELECT DISTINCT vendor_number AS bc_number, provider AS display_name FROM {b}.purchase_invoices
          WHERE supplier_id IS NULL AND NOT is_manual AND vendor_number LIKE 'PV%' AND lines_total <> 0
          UNION SELECT DISTINCT bc_vendor_no, display_name FROM {c}.ref.vendor_map) v
        WHERE bc_number NOT IN (SELECT 'SP' || supplier_id FROM {c}.silver.suppliers)""")
    for t, text in TABLE_COMMENTS.items():
        sql(f"COMMENT ON TABLE {b}.{t} IS '{text}'")
    for t in PII_TABLES:
        sql(f"ALTER TABLE {b}.{t} SET TAGS ('contains_pii' = 'true')")
    # The push reads the last published version; while blocking checks keep failing that version ages, so its data
    # files are kept 30 days instead of the default 7 before VACUUM may remove them.
    for t in TABLES:
        sql(f"ALTER TABLE {b}.{t} SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days')")

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
        "business_central_sales_key_unique": (
            "block",
            f"""SELECT external_document_number AS k FROM {b}.sales_invoices
            GROUP BY 1 HAVING count(*) > 1""",
        ),
        "business_central_purchase_key_unique": (
            "block",
            f"""SELECT purchase_key AS k FROM {b}.purchase_invoices
            GROUP BY 1 HAVING count(*) > 1""",
        ),
        "business_central_party_number_unique": (
            "block",
            f"""SELECT bc_number AS k FROM {b}.customers GROUP BY 1 HAVING count(*) > 1
            UNION ALL SELECT bc_number FROM {b}.vendors GROUP BY 1 HAVING count(*) > 1""",
        ),
        "business_central_sales_invoice_without_customer": (
            "block",
            f"""SELECT external_document_number AS k FROM {send_sales} i
            LEFT ANTI JOIN {b}.customers p ON p.bc_number = i.customer_number""",
        ),
        "business_central_purchase_invoice_without_vendor": (
            "block",
            f"""SELECT purchase_key AS k FROM {send_purch} i
            LEFT ANTI JOIN {b}.vendors p ON p.bc_number = i.vendor_number""",
        ),
        "business_central_document_without_currency": (
            "block",
            f"""SELECT external_document_number AS k FROM {send_sales}
            WHERE currency_code IS NULL UNION ALL SELECT purchase_key FROM {send_purch} WHERE currency_code IS NULL""",
        ),
        "business_central_sales_lines_mixed_currency": (
            "block",
            f"""SELECT external_document_number AS k FROM {send_sales}
            WHERE line_currencies <> currency_code""",
        ),
        "business_central_field_length_over_limit": (
            "block",
            f"""SELECT bc_number AS k FROM {b}.customers WHERE length(bc_number) > 20
            UNION ALL SELECT bc_number FROM {b}.vendors WHERE length(bc_number) > 20
            UNION ALL SELECT purchase_key FROM {send_purch} WHERE length(vendor_invoice_number) > 35
            UNION ALL SELECT external_document_number FROM {send_sales} WHERE length(external_document_number) > 35""",
        ),
        # warn: recorded in ops.dq_results only
        "silver_unknown_service_array": (
            "warn",
            f"""SELECT DISTINCT e.key AS k FROM {c}.silver.bookings b,
            LATERAL variant_explode(b.payload) e WHERE lower(e.key) LIKE '%service%' AND e.key NOT IN ({known})""",
        ),
        "business_central_active_booking_without_lines": (
            "warn",
            f"""SELECT external_document_number AS k FROM {b}.sales_invoices
            WHERE status IN ('BOOKED','RQ') AND doc_action = 'CANCEL'""",
        ),
        "business_central_sales_held_customer_unmapped": (
            "warn",
            f"""SELECT external_document_number AS k
            FROM {b}.sales_invoices WHERE hold_reason = 'CUSTOMER_UNMAPPED' AND doc_action = 'UPSERT'""",
        ),
        "business_central_purchase_held_vendor_unmapped": (
            "warn",
            f"""SELECT purchase_key AS k
            FROM {b}.purchase_invoices WHERE hold_reason = 'VENDOR_UNMAPPED' AND doc_action = 'UPSERT'""",
        ),
        "business_central_held_party_not_in_snapshot": (
            "warn",
            f"""SELECT external_document_number AS k FROM {b}.sales_invoices
            WHERE hold_reason = 'AGENCY_NOT_IN_SNAPSHOT' UNION ALL SELECT purchase_key FROM {b}.purchase_invoices
            WHERE hold_reason = 'SUPPLIER_NOT_IN_SNAPSHOT'""",
        ),
        "business_central_booking_total_differs_from_lines": (
            "warn",
            f"""SELECT external_document_number AS k FROM {send_sales}
            WHERE abs(total_price - lines_total) > 0.05""",
        ),
        "business_central_negative_document_total": (
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
    record_build(sql, c, "build_business_central_tables", a.run_id, token)


if __name__ == "__main__":
    main()
