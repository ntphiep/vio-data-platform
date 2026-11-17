"""Job vio_travel_governance - Unity Catalog governance and the semantic layer of catalog <catalog>.

Idempotent; runs after each deploy and weekly, so tags, comments and grants follow new tables.

- Masking: function governance.mask_pii hides personal values unless the reader is in the group vio_pii_readers.
  The current silver tables and dim_user declare the mask in their pipeline schema; this job adds it to the
  history tables, which the pipeline creates without a schema.
- Tags: table tag contains_pii and column tag pii (email, phone, name, address) on every personal column, so the
  columns can be found with information_schema.column_tags.
- Grants: every workspace user (`account users`) reads gold (masked) and the metric views; the deploying engineer
  reads every layer. vio_pii_readers is a workspace group, which Unity Catalog accepts in is_member() of the mask
  but not in GRANT.
- Plain views: silver.booking_invoices, silver.booking_price_components and silver.accommodations. They have no
  reader inside the pipeline, and a view costs one Unity Catalog table slot where a pipeline materialized view
  costs two (the table and its hidden __materialization table); the Free Edition metastore holds 500 tables.
- Metric views: gold.booking_metrics, gold.service_metrics, gold.receivable_metrics (YAML, version 1.1), the
  definitions Genie and the dashboard share.
"""

import argparse

PII_COLUMNS = {
    # table: {column: pii kind}
    "silver.agencies": {"email": "email", "billing_email": "email", "phone": "phone", "address": "address"},
    "silver.agencies_history": {"email": "email", "billing_email": "email", "phone": "phone", "address": "address"},
    "silver.suppliers": {"email": "email", "phone": "phone"},
    "silver.suppliers_history": {"email": "email", "phone": "phone"},
    "silver.users": {"username": "name", "name": "name", "surname": "name", "email": "email", "telephone": "phone"},
    "silver.users_history": {
        "username": "name",
        "name": "name",
        "surname": "name",
        "email": "email",
        "telephone": "phone",
    },
    "silver.purchase_attempts": {"username": "name", "email": "email"},
    "silver.purchase_attempts_history": {"username": "name", "email": "email"},
    "gold.dim_user": {"username": "name", "full_name": "name", "email": "email"},
}
# Streaming tables created by AUTO CDC without a declared schema get the mask here.
MASK_AFTER = [t for t in PII_COLUMNS if t.endswith("_history")]
PII_TABLES = [
    "bronze.tc_bookings",
    "bronze.tc_agencies",
    "bronze.tc_suppliers",
    "bronze.tc_users",
    "bronze.tc_purchase_attempts",
    "silver.bookings",
    *PII_COLUMNS,
    "business_central.customers",
    "business_central.vendors",
]
STREAMING = ("bronze.", "silver.bookings", "silver.accommodation_details") + tuple(MASK_AFTER)

SILVER_VIEWS = {
    "booking_invoices": (
        "Invoices issued by Travel Compositor for a booking (number, target, creation date).",
        """SELECT b.booking_ref, x.value:number::string AS invoice_number, x.value:target::string AS target,
               to_date(x.value:creationDate::string) AS created_date
        FROM {c}.silver.bookings b, LATERAL variant_explode(b.payload:invoices) x""",
    ),
    "booking_price_components": (
        "Price breakdown of a booking, one row per component (totalPrice, netProvider, operatorFee ...).",
        """SELECT b.booking_ref, x.key AS component,
               x.value:microsite.currency::string AS currency, x.value:microsite.amount::decimal(18,2) AS amount,
               x.value:operator.currency::string AS operator_currency,
               x.value:operator.amount::decimal(18,2) AS operator_amount
        FROM {c}.silver.bookings b, LATERAL variant_explode(b.payload:pricebreakdown) x
        WHERE x.value:microsite.amount IS NOT NULL""",
    ),
    "accommodations": (
        "Accommodation catalog of Travel Compositor, current version (open rows of accommodations_history).",
        """SELECT * EXCEPT (__START_AT, __END_AT) FROM {c}.silver.accommodations_history WHERE __END_AT IS NULL""",
    ),
}

# Join names differ from every dimension name: identifiers are case-insensitive, so a join called agency next to
# the dimension Agency resolves agency.agency_name against the dimension (INVALID_EXTRACT_BASE_FIELD_TYPE).
METRIC_VIEWS = {
    "booking_metrics": """
version: 1.1
comment: Bookings of Vio Travel by creation date, agency and status. Amounts in the booking currency (USD).
source: {c}.gold.fact_booking
joins:
  - name: dim_agency
    source: {c}.gold.dim_agency
    on: source.agency_key = dim_agency.agency_key
  - name: created_date
    source: {c}.gold.dim_date
    on: source.created_date_key = created_date.date_key
  - name: dim_travel_idea
    source: {c}.gold.dim_travel_idea
    on: source.idea_key = dim_travel_idea.idea_key
dimensions:
  - name: Created month
    expr: created_date.year_month
  - name: Created year
    expr: created_date.year
  - name: Agency
    expr: dim_agency.agency_name
  - name: Agency country
    expr: dim_agency.country
  - name: Status
    expr: status
  - name: Trip type
    expr: trip_type
  - name: Source market
    expr: source_market_name
  - name: Travel idea
    expr: dim_travel_idea.title
measures:
  - name: Bookings
    expr: COUNT(1)
  - name: Active bookings
    expr: COUNT(1) FILTER (WHERE is_active)
  - name: Cancellation rate
    expr: COUNT(1) FILTER (WHERE is_cancelled) / COUNT(1)
  - name: Revenue
    expr: SUM(total_price) FILTER (WHERE is_active)
  - name: Gross margin
    expr: SUM(gross_margin) FILTER (WHERE is_active)
  - name: Margin rate
    expr: SUM(gross_margin) FILTER (WHERE is_active) / SUM(total_price) FILTER (WHERE is_active)
  - name: Passengers
    expr: SUM(pax) FILTER (WHERE is_active)
  - name: Pending amount
    expr: SUM(pending_amount)
""",
    "service_metrics": """
version: 1.1
comment: Services of active bookings by supplier, destination and service category.
source: {c}.gold.obt_booking_service
filter: is_active AND booking_is_active
dimensions:
  - name: Booking month
    expr: booking_created_month
  - name: Service start month
    expr: service_start_month
  - name: Service category
    expr: service_category
  - name: Supplier
    expr: supplier_name
  - name: Supplier source
    expr: supplier_source
  - name: Destination country
    expr: destination_country
  - name: Destination
    expr: destination_name
  - name: Accommodation
    expr: accommodation_name
  - name: Accommodation category
    expr: accommodation_category
measures:
  - name: Services
    expr: COUNT(1)
  - name: Bookings
    expr: COUNT(DISTINCT booking_ref)
  - name: Sale amount
    expr: SUM(sale_amount)
  - name: Cost in booking currency
    expr: SUM(cost_in_booking_currency)
  - name: Margin in booking currency
    expr: SUM(margin_in_booking_currency)
""",
    "receivable_metrics": """
version: 1.1
comment: Payments still due on bookings, by agency, due date and ageing bucket.
source: {c}.gold.fact_pending_payment
joins:
  - name: dim_agency
    source: {c}.gold.dim_agency
    on: source.agency_key = dim_agency.agency_key
dimensions:
  - name: Agency
    expr: dim_agency.agency_name
  - name: Ageing bucket
    expr: ageing_bucket
  - name: Due month
    expr: date_format(due_date, 'yyyy-MM')
  - name: Currency
    expr: currency
  - name: Booking status
    expr: booking_status
measures:
  - name: Pending payments
    expr: COUNT(1)
  - name: Amount to pay
    expr: SUM(amount_to_pay)
  - name: Overdue amount
    expr: SUM(amount_to_pay) FILTER (WHERE days_overdue > 0)
  - name: Average days overdue
    expr: AVG(days_overdue) FILTER (WHERE days_overdue > 0)
""",
}


def kind(table: str) -> str:
    """SQL keyword for ALTER: pipeline tables are streaming tables or materialized views."""
    if table.startswith(("business_central.",)):
        return "TABLE"
    return "STREAMING TABLE" if table.startswith(STREAMING) else "MATERIALIZED VIEW"


def main():
    from pyspark.sql import SparkSession

    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="vio_travel")
    a = ap.parse_args()
    spark = SparkSession.builder.getOrCreate()
    c = a.catalog
    done, failed = [], []

    def run(stmt: str) -> None:
        try:
            spark.sql(stmt)
            done.append(stmt.split("\n")[0][:120])
        except Exception as e:  # noqa: BLE001 - one missing table must not stop the other statements
            failed.append(f"{stmt.split(chr(10))[0][:120]} -> {str(e).splitlines()[0][:200]}")

    def exists(t: str) -> bool:
        return spark.catalog.tableExists(f"{c}.{t}")

    run(f"CREATE SCHEMA IF NOT EXISTS {c}.governance COMMENT 'Masking functions and governance helpers'")
    run(f"""CREATE OR REPLACE FUNCTION {c}.governance.mask_pii(v STRING)
        RETURNS STRING
        COMMENT 'Returns the value for members of vio_pii_readers; otherwise keeps the domain of an e-mail and the
                 last 3 characters of anything else'
        RETURN CASE
          WHEN v IS NULL THEN NULL
          WHEN is_account_group_member('vio_pii_readers') OR is_member('vio_pii_readers') THEN v
          WHEN v LIKE '%@%' THEN regexp_replace(v, '^[^@]+', '***')
          ELSE concat(repeat('*', greatest(length(v) - 3, 0)), right(v, 3)) END""")

    for t in PII_TABLES:
        if exists(t):
            run(f"ALTER {kind(t)} {c}.{t} SET TAGS ('contains_pii' = 'true')")
    # The service principal owns the pipeline tables. The catalog owner (the engineer running this job) grants
    # itself MODIFY on the history tables, which ALTER ... SET MASK requires.
    engineer = spark.sql("SELECT current_user()").first()[0]
    for t in MASK_AFTER:
        if exists(t):
            run(f"GRANT MODIFY ON TABLE {c}.{t} TO `{engineer}`")
    for t, cols in PII_COLUMNS.items():
        if not exists(t):
            continue
        for col, pii in cols.items():
            run(f"ALTER {kind(t)} {c}.{t} ALTER COLUMN {col} SET TAGS ('pii' = '{pii}')")
            if t in MASK_AFTER:
                run(f"ALTER {kind(t)} {c}.{t} ALTER COLUMN {col} SET MASK {c}.governance.mask_pii")

    for name, (comment, query) in SILVER_VIEWS.items():
        run(f"CREATE OR REPLACE VIEW {c}.silver.{name} COMMENT '{comment}' AS {query.format(c=c)}")

    for name, yaml in METRIC_VIEWS.items():
        run(f"CREATE OR REPLACE VIEW {c}.gold.{name} WITH METRICS LANGUAGE YAML AS $$\n{yaml.format(c=c)}$$")

    # Least privilege: readers get the model, not the raw layers. The engineer who deploys the bundle (this job runs
    # as that user) owns the catalog but not the tables the service principal creates, so reads every layer through
    # an explicit grant.
    for stmt in [
        f"GRANT USE CATALOG ON CATALOG {c} TO `account users`",
        f"GRANT USE SCHEMA, SELECT ON SCHEMA {c}.gold TO `account users`",
        f"GRANT USE SCHEMA, SELECT, READ VOLUME, EXECUTE ON CATALOG {c} TO `{engineer}`",
    ]:
        run(stmt)

    print(f"governance: {len(done)} statements applied")
    for f in failed:
        print("FAILED", f)
    if failed:
        raise RuntimeError(f"{len(failed)} governance statement(s) failed, see the output above")


if __name__ == "__main__":
    main()
