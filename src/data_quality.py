"""Task data_quality - end-to-end checks across the stages, the last task of every job run, catalog <catalog>.

The producing tasks keep their own blocking checks: star_schema before it replaces gold, bc_tables before it
publishes business_central. This task checks what no single task can see, whether each stage is up to date with
the stage before it:

    Travel Compositor API -> ops.ingest_runs -> silver -> gold (dashboard) and business_central -> Business Central

Results go to ops.dq_results with the prefix e2e_. An alert check fails this task, which sends the job's failure
e-mail; the gold and business_central tables are already in place by then. The SQL alert pipeline_freshness
covers the case where the job itself stops running.
"""

import argparse
import json

SYNCED = "('CREATED', 'UPDATED', 'RECREATED', 'CANCELLED_DRAFT_EMPTIED')"  # same as local/push_to_bc.py
MAX_FAILURES = 3


def main():
    from pyspark.sql import SparkSession

    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="vio_travel")
    ap.add_argument("--run_id", default="")
    ap.add_argument("--bc_company", default="My Company")
    ap.add_argument("--extract_stale_minutes", type=int, default=75, help="bookings extract; the job runs every 30 min")
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
    last_token = f"""(SELECT task, max_by(silver_token, built_at) AS t FROM {c}.ops.build_state GROUP BY task)"""

    checks = {
        # stage 1: Travel Compositor API -> landing. The latest extract failed, or its last success is too old.
        "e2e_extract_failing_or_stale": (
            "alert",
            f"""SELECT entity || ' ' || last_status AS k FROM (
            SELECT entity, max_by(status, finished_at) AS last_status,
                   max(CASE WHEN status = 'SUCCESS' THEN finished_at END) AS last_success
            FROM {c}.ops.ingest_runs GROUP BY entity)
            WHERE last_status <> 'SUCCESS'
               OR (entity = 'bookings'
                   AND last_success < current_timestamp() - make_interval(0, 0, 0, 0, 0, {a.extract_stale_minutes}))
               OR last_success < current_timestamp() - INTERVAL 8 HOURS""",
        ),
        # the source itself went quiet: no booking changed in Travel Compositor for 48 hours
        "e2e_source_not_updated_48h": (
            "alert",
            f"""SELECT 'silver.bookings' AS k FROM {c}.silver.bookings
            HAVING max(last_update_at) < current_timestamp() - INTERVAL 48 HOURS""",
        ),
        # stage 2: silver -> gold and business_central. A task did not rebuild from the current silver.
        "e2e_gold_behind_silver": (
            "alert",
            f"""SELECT 'gold' AS k FROM {last_token} s
            LEFT JOIN {last_token} g ON g.task = 'star_schema'
            WHERE s.task = 'bronze_silver' AND g.t IS DISTINCT FROM s.t""",
        ),
        "e2e_business_central_behind_silver": (
            "alert",
            f"""SELECT 'business_central' AS k FROM {last_token} s
            LEFT JOIN {last_token} b ON b.task = 'bc_tables'
            WHERE s.task = 'bronze_silver' AND b.t IS DISTINCT FROM s.t""",
        ),
        # stage 3: business_central -> Business Central. Documents wait while the push has not run for too long.
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
    spark.createDataFrame(
        rows_out, "run_id STRING, check_name STRING, severity STRING, failed_rows BIGINT, sample STRING"
    ).selectExpr(
        "run_id", "current_timestamp() AS checked_at", "check_name", "severity", "failed_rows", "sample"
    ).write.mode("append").saveAsTable(f"{c}.ops.dq_results")
    if alerts:
        raise RuntimeError(f"end-to-end freshness alert: {alerts}")


if __name__ == "__main__":
    main()
