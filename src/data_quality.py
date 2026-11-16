"""Task data_quality - the last task of every job run, catalog <catalog>. Writes ops.dq_results.

Four kinds of checks meet in one table, so the dashboard and the SQL alert read one place:
- expectations of the pipeline vio_travel_lakehouse (bronze_, silver_ rows dropped or warned), copied from the
  pipeline event log ops.pipeline_event_log for the latest update, severity `expect`;
- gold_ checks that a row-level expectation cannot express: key uniqueness, orphans, totals against silver;
- bc_ checks stay in bc_tables, which blocks the publish of business_central;
- e2e_ checks of whether each stage is up to date with the one before it:

    Travel Compositor API -> ops.ingest_runs -> pipeline (bronze, silver, gold) -> business_central -> Business Central

An alert check fails this task, which sends the job's failure e-mail; the tables are already in place by then.
The SQL alert pipeline_freshness covers the case where the job itself stops running.
"""

import argparse
import json

SYNCED = "('CREATED', 'UPDATED', 'RECREATED', 'CANCELLED_DRAFT_EMPTIED')"  # same as local/push_to_bc.py
MAX_FAILURES = 3
# Minutes after which the last successful extract of an entity is stale: twice its cadence plus margin.
STALE_MINUTES = {
    "bookings": 75,
    "pending_payments": 75,
    "purchase_attempts": 75,
    "agencies": 13 * 60,
    "suppliers": 13 * 60,
    "users": 13 * 60,
    "travel_ideas": 13 * 60,
    "accommodation_datasheets": 13 * 60,
    "accommodations": 15 * 24 * 60,
}
UNKNOWN = "-1"


def main():
    from pyspark.sql import SparkSession

    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="vio_travel")
    ap.add_argument("--run_id", default="")
    ap.add_argument("--bc_company", default="My Company")
    ap.add_argument("--pipeline_stale_minutes", type=int, default=75, help="the job runs every 30 min")
    ap.add_argument("--push_stale_minutes", type=int, default=120)
    ap.add_argument("--push_scope_days", type=int, default=30, help="same as scope_sql in local/config.json")
    a = ap.parse_args()
    spark = SparkSession.builder.getOrCreate()
    c = a.catalog
    sql = spark.sql
    company = a.bc_company.replace("'", "''")

    pub = sql(f"SELECT versions FROM {c}.ops.publish ORDER BY published_at DESC LIMIT 1").collect()
    ver = json.loads(pub[0]["versions"]) if pub else {}
    sales = (
        f"{c}.business_central.sales_invoices VERSION AS OF {ver['sales_invoices']}"
        if "sales_invoices" in ver
        else f"(SELECT * FROM {c}.business_central.sales_invoices WHERE false)"
    )
    # Sales documents of the last publish that the push has not delivered yet: the same rule and the same
    # invoice-date scope as local/push_to_bc.py.
    pending_sales = f"""(SELECT i.external_document_number AS k FROM {sales} i
        LEFT ANTI JOIN (SELECT DISTINCT doc_key, payload_hash FROM {c}.ops.bc_sync
                        WHERE doc_type = 'sales' AND result IN {SYNCED} AND bc_company = '{company}') s
          ON s.doc_key = i.external_document_number AND s.payload_hash = i.payload_hash
        LEFT ANTI JOIN (SELECT doc_key, payload_hash FROM {c}.ops.bc_sync
                        WHERE doc_type = 'sales' AND result = 'FAILED' AND bc_company = '{company}'
                        GROUP BY ALL HAVING count(*) >= {MAX_FAILURES}) d
          ON d.doc_key = i.external_document_number AND d.payload_hash = i.payload_hash
        WHERE i.hold_reason IS NULL AND i.doc_action = 'UPSERT'
          AND i.invoice_date >= date_sub(current_date(), {a.push_scope_days}))"""
    # last data change of silver.bookings (maintenance commits do not count)
    try:
        hist = sql(f"DESCRIBE HISTORY {c}.silver.bookings LIMIT 50").collect()
    except Exception as e:  # noqa: BLE001 - the check below then never fires; the failure is printed
        print(f"DESCRIBE HISTORY silver.bookings failed, e2e_business_central_behind_silver skipped: {e}")
        hist = []
    silver_changed_at = max(
        (h["timestamp"] for h in hist if h["operation"] not in ("OPTIMIZE", "VACUUM START", "VACUUM END", "ANALYZE")),
        default="1970-01-01 00:00:00",
    )
    stale = " ".join(f"WHEN '{e}' THEN {m}" for e, m in STALE_MINUTES.items())
    events = f"{c}.ops.pipeline_event_log"
    g, sv = f"{c}.gold", f"{c}.silver"

    checks = {
        # stage 1: Travel Compositor API -> landing. The latest extract of an entity failed, or its last success
        # is older than twice its cadence.
        "e2e_extract_failing_or_stale": (
            "alert",
            f"""SELECT entity || ' ' || last_status AS k FROM (
            SELECT entity, max_by(status, finished_at) AS last_status,
                   max(CASE WHEN status = 'SUCCESS' THEN finished_at END) AS last_success
            FROM {c}.ops.ingest_runs GROUP BY entity)
            WHERE last_status = 'FAILED'
               OR coalesce(last_success, TIMESTAMP'1970-01-01')
                  < current_timestamp() - make_interval(0, 0, 0, 0, 0, CASE entity {stale} ELSE 780 END)""",
        ),
        # the source itself went quiet: no booking changed in Travel Compositor for 48 hours
        "e2e_source_not_updated_48h": (
            "alert",
            f"""SELECT 'silver.bookings' AS k FROM {sv}.bookings
            HAVING max(last_update_at) < current_timestamp() - INTERVAL 48 HOURS""",
        ),
        # stage 2: landing -> bronze, silver, gold. The last pipeline update failed, or none completed lately.
        "e2e_pipeline_failed_or_stale": (
            "alert",
            f"""SELECT coalesce(last_state, 'no update') AS k FROM (
            SELECT max_by(details:update_progress.state::string, timestamp) AS last_state,
                   max(CASE WHEN details:update_progress.state::string = 'COMPLETED' THEN timestamp END) AS last_ok
            FROM {events} WHERE event_type = 'update_progress'
              AND details:update_progress.state::string IN ('COMPLETED', 'FAILED', 'CANCELED'))
            WHERE last_state <> 'COMPLETED'
               OR coalesce(last_ok, TIMESTAMP'1970-01-01')
                  < current_timestamp() - make_interval(0, 0, 0, 0, 0, {a.pipeline_stale_minutes})""",
        ),
        # stage 3: silver -> business_central. bc_tables records a token on every run; silver changed after it.
        "e2e_business_central_behind_silver": (
            "alert",
            f"""SELECT 'business_central' AS k FROM (SELECT 1)
            WHERE coalesce((SELECT max(built_at) FROM {c}.ops.build_state WHERE task = 'bc_tables'),
                           TIMESTAMP'1970-01-01')
                < TIMESTAMP'{silver_changed_at}'""",
        ),
        # gold: what row-level expectations cannot express (materialized views publish without a block)
        "gold_dim_keys_unique": (
            "alert",
            f"""SELECT 'dim_agency ' || agency_key AS k FROM {g}.dim_agency GROUP BY agency_key HAVING count(*) > 1
            UNION ALL SELECT 'dim_supplier ' || supplier_key FROM {g}.dim_supplier GROUP BY 1 HAVING count(*) > 1
            UNION ALL SELECT 'dim_user ' || user_key FROM {g}.dim_user GROUP BY 1 HAVING count(*) > 1
            UNION ALL SELECT 'dim_travel_idea ' || idea_key FROM {g}.dim_travel_idea GROUP BY 1 HAVING count(*) > 1
            UNION ALL SELECT 'dim_accommodation ' || accommodation_key FROM {g}.dim_accommodation
              GROUP BY 1 HAVING count(*) > 1""",
        ),
        "gold_fact_grain": (
            "alert",
            f"""SELECT 'fact_booking ' || booking_ref AS k FROM {g}.fact_booking GROUP BY booking_ref HAVING count(*) > 1
            UNION ALL SELECT 'fact_booking_service ' || service_key FROM {g}.fact_booking_service
              GROUP BY service_key HAVING count(*) > 1""",
        ),
        "gold_fact_orphans": (
            "alert",
            f"""SELECT 'agency ' || f.booking_ref AS k FROM {g}.fact_booking f
              LEFT ANTI JOIN {g}.dim_agency d ON d.agency_key = f.agency_key
            UNION ALL SELECT 'user ' || f.booking_ref FROM {g}.fact_booking f
              LEFT ANTI JOIN {g}.dim_user d ON d.user_key = f.user_key
            UNION ALL SELECT 'idea ' || f.booking_ref FROM {g}.fact_booking f
              LEFT ANTI JOIN {g}.dim_travel_idea d ON d.idea_key = f.idea_key
            UNION ALL SELECT 'supplier ' || f.service_key FROM {g}.fact_booking_service f
              LEFT ANTI JOIN {g}.dim_supplier d ON d.supplier_key = f.supplier_key
            UNION ALL SELECT 'service_type ' || f.service_key FROM {g}.fact_booking_service f
              LEFT ANTI JOIN {g}.dim_service_type d ON d.service_type = f.service_type
            UNION ALL SELECT 'accommodation ' || f.service_key FROM {g}.fact_booking_service f
              LEFT ANTI JOIN {g}.dim_accommodation d ON d.accommodation_key = f.accommodation_key""",
        ),
        "gold_totals_match_silver": (
            "alert",
            f"""SELECT 'fact_booking rows' AS k FROM (SELECT 1)
              WHERE (SELECT count(*) FROM {g}.fact_booking) <> (SELECT count(*) FROM {sv}.bookings)
            UNION ALL SELECT 'fact_booking_service rows' FROM (SELECT 1)
              WHERE (SELECT count(*) FROM {g}.fact_booking_service) <> (SELECT count(*) FROM {sv}.booking_services)
            UNION ALL SELECT 'total_price' FROM (SELECT 1)
              WHERE abs((SELECT coalesce(sum(total_price), 0) FROM {g}.fact_booking)
                      - (SELECT coalesce(sum(total_price), 0) FROM {sv}.bookings)) > 0.01
            UNION ALL SELECT 'obt rows' FROM (SELECT 1)
              WHERE (SELECT count(*) FROM {g}.obt_booking_service) <> (SELECT count(*) FROM {g}.fact_booking_service)""",
        ),
        "gold_service_without_destination": (
            "warn",
            f"SELECT service_key AS k FROM {g}.fact_booking_service WHERE destination_country IS NULL AND is_active",
        ),
        "gold_supplier_unknown": (
            "warn",
            f"SELECT service_key AS k FROM {g}.fact_booking_service WHERE supplier_key = '{UNKNOWN}' AND is_active",
        ),
        "gold_accommodation_without_datasheet": (
            "warn",
            f"SELECT accommodation_key AS k FROM {g}.dim_accommodation WHERE NOT has_datasheet AND accommodation_key <> '{UNKNOWN}'",
        ),
        "gold_payment_overdue_90_days": (
            "warn",
            f"SELECT booking_ref AS k FROM {g}.fact_pending_payment WHERE days_overdue > 90",
        ),
        # stage 4: business_central -> Business Central. Documents wait while the push has not run for too long.
        "e2e_push_stalled": (
            "alert",
            f"""SELECT 'ops.bc_sync' AS k FROM (SELECT 1)
            WHERE EXISTS (SELECT 1 FROM {pending_sales})
              AND coalesce((SELECT max(synced_at) FROM {c}.ops.bc_sync WHERE bc_company = '{company}'),
                           TIMESTAMP'1970-01-01')
                  < current_timestamp() - make_interval(0, 0, 0, 0, 0, {a.push_stale_minutes})""",
        ),
        # how much is still waiting for the push, for the dashboard
        "e2e_sales_waiting_for_push": ("warn", f"SELECT k FROM {pending_sales}"),
    }
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ops.dq_results (
        run_id STRING, checked_at TIMESTAMP, check_name STRING, severity STRING, failed_rows BIGINT, sample STRING)""")
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ops.build_state (
        task STRING, run_id STRING, built_at TIMESTAMP, silver_token STRING)""")
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ops.bc_sync (synced_at TIMESTAMP, doc_type STRING, doc_key STRING,
             payload_hash STRING, bc_company STRING, bc_id STRING, bc_number STRING, result STRING, error STRING)""")
    rows_out, alerts = [], []
    for name, (severity, q) in checks.items():
        r = sql(f"SELECT count(*) AS n, slice(collect_list(k), 1, 5) AS s FROM ({q})").collect()[0]
        rows_out.append((a.run_id, name, severity, r["n"], ",".join(r["s"] or [])))
        print(f"DQ {severity:5} {name}: {r['n']} sample={r['s']}")
        if r["n"] and severity == "alert":
            alerts.append(name)
    # expectations of the latest pipeline update, one row per expectation and dataset
    if spark.catalog.tableExists(events):
        for r in sql(f"""
            WITH latest AS (SELECT max_by(origin.update_id, timestamp) AS u FROM {events} WHERE event_type = 'create_update')
            SELECT x.name, x.dataset, sum(x.failed_records) AS failed, sum(x.passed_records) AS passed
            FROM {events} ev JOIN latest ON ev.origin.update_id = latest.u,
                 LATERAL explode(from_json(ev.details:flow_progress.data_quality.expectations::string,
                   'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) t(x)
            WHERE ev.event_type = 'flow_progress'
            GROUP BY ALL""").collect():
            rows_out.append((a.run_id, r["name"], "expect", int(r["failed"] or 0), r["dataset"]))
            print(f"DQ expect {r['name']} on {r['dataset']}: failed={r['failed']} passed={r['passed']}")
    spark.createDataFrame(
        rows_out, "run_id STRING, check_name STRING, severity STRING, failed_rows BIGINT, sample STRING"
    ).selectExpr(
        "run_id", "current_timestamp() AS checked_at", "check_name", "severity", "failed_rows", "sample"
    ).write.mode("append").saveAsTable(f"{c}.ops.dq_results")
    if alerts:
        raise RuntimeError(f"data-quality alert: {alerts}")


if __name__ == "__main__":
    main()
