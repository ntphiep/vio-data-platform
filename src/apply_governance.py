"""Job vio_travel_apply_governance - Unity Catalog governance and the semantic layer of catalog <catalog>.

Idempotent; runs after each deploy and weekly, so tags, table comments and grants follow new tables.

- Masking: function governance.mask_pii hides personal values unless the reader is in the group vio_pii_readers.
  Every silver table (current and history) and gold.dim_user declares the mask in its pipeline schema, together
  with its column comments, so this job creates the function and sets no mask on pipeline tables. The tables of
  business_central are not masked: local/push_to_business_central.py must send the real e-mail and address to Business Central,
  and only the service principal and the catalog owner can read that schema.
- Tags: table tag contains_pii and column tag pii (email, phone, name, address) on every personal column, so the
  columns can be found with information_schema.column_tags.
- Comments: table comments on the ops and ref tables, which tasks create with CREATE TABLE IF NOT EXISTS; column
  comments of silver and gold come from the pipeline schemas, those of the plain views below from this job.
- Grants: every workspace user (`account users`) reads gold (masked) and the metric views; the deploying engineer
  reads every layer. vio_pii_readers is a workspace group, which Unity Catalog accepts in is_member() of the mask
  but not in GRANT.
- Plain views: silver.booking_invoices, silver.booking_price_components, silver.booking_service_amendments and
  silver.accommodations. They have no reader inside the pipeline, and a view costs one Unity Catalog table slot
  where a pipeline materialized view costs two (the table and its hidden __materialization table); the Free
  Edition metastore holds 500 tables.
- Metric views: gold.booking_metrics, gold.service_metrics, gold.receivable_metrics (YAML, version 1.1), the
  definitions Genie and the dashboard share.
- Anomaly detection: a data quality monitor on the schemas silver, gold and business_central (MONITORED_SCHEMAS)
  checks freshness and completeness of every table; the bundle has no resource type for it, so this job creates
  the monitors through the data quality API. https://docs.databricks.com/aws/en/data-quality-monitoring/anomaly-detection/
"""

import argparse

PII_COLUMNS = {
    # table: {column: pii kind}
    "silver.agencies": {"email": "email", "billing_email": "email", "phone": "phone", "address": "address"},
    "silver.agencies_history": {"email": "email", "billing_email": "email", "phone": "phone", "address": "address"},
    "silver.suppliers": {"email": "email", "phone": "phone", "address": "address"},
    "silver.suppliers_history": {"email": "email", "phone": "phone", "address": "address"},
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
    # the username is often an e-mail address
    "silver.bookings": {"username": "name"},
    "silver.travel_ideas": {"username": "name"},
    "gold.dim_user": {"username": "name", "full_name": "name", "email": "email"},
}
PII_TABLES = [
    "bronze.tc_bookings",
    "bronze.tc_agencies",
    "bronze.tc_suppliers",
    "bronze.tc_users",
    "bronze.tc_purchase_attempts",
    "bronze.tc_travel_ideas",
    *PII_COLUMNS,
    "business_central.customers",
    "business_central.vendors",
]
HISTORY = tuple(t for t in PII_COLUMNS if t.endswith("_history"))
STREAMING = ("bronze.", "silver.bookings", "silver.accommodation_details", "silver.accommodations_history") + HISTORY

# Tables created by job tasks with CREATE TABLE IF NOT EXISTS; ops.pipeline_event_log belongs to the pipeline.
TABLE_COMMENTS = {
    "ops.api_calls": "One row per HTTP attempt of extract_travel_compositor to the Travel Compositor API: path, parameters, status, "
    "bytes, seconds, attempt, error. No credentials.",
    "ops.ingest_runs": "One row per entity and read window of an extract_travel_compositor run; status SUCCESS, PARTIAL or FAILED. "
    "A snapshot entity counts only when SUCCESS; silver applies snapshots by run_id and finished_at.",
    "ops.watermark": "Last booking update date read completely by extract_travel_compositor; the next run starts there minus the "
    "overlap.",
    "ops.accommodation_fetch": "One row per accommodation datasheet fetch of extract_travel_compositor: time, status, run.",
    "ops.build_state": "Content token of silver and ref per task build; build_business_central_tables skips the rebuild when it is unchanged.",
    "ops.dq_results": "Data-quality results per job run: check name, severity, failed rows, sample keys.",
    "ops.publish": "Write-audit-publish log: business_central table versions published after the blocking checks "
    "passed; local/push_to_business_central.py reads the tables AS OF the latest row.",
    "ops.business_central_sync": "One row per document pushed by local/push_to_business_central.py to Business Central: key, payload hash, "
    "company, Business Central id and number, result, error.",
    "ref.umbrella_agency": "Agencies whose real customer is the agency booking reference of each booking, mapped by "
    "ref.customer_map. Maintained by Vio Finance.",
    "ref.customer_map": "Business Central customer number per agency booking reference (lower case) of an umbrella "
    "agency. Maintained by Vio Finance.",
    "ref.vendor_map": "Business Central vendor number per provider code (PROVIDER) or manual supplier name in lower "
    "case (MANUAL_NAME). Maintained by Vio Finance.",
}

SILVER_VIEWS = {
    "booking_invoices": (
        "Invoices issued by Travel Compositor for a booking (number, target, creation date).",
        {
            "booking_ref": "Travel Compositor booking reference.",
            "invoice_number": "Invoice number in Travel Compositor.",
            "target": "Party the invoice is addressed to (Travel Compositor invoice target).",
            "created_date": "Creation date of the invoice.",
        },
        """SELECT b.booking_ref, x.value:number::string AS invoice_number, x.value:target::string AS target,
               to_date(x.value:creationDate::string) AS created_date
        FROM {c}.silver.bookings b, LATERAL variant_explode(b.payload:invoices) x""",
    ),
    "booking_price_components": (
        "Price breakdown of a booking, one row per component (totalPrice, netProvider, operatorFee ...).",
        {
            "booking_ref": "Travel Compositor booking reference.",
            "component": "Price breakdown key (totalPrice, netProvider, operatorFee ...).",
            "currency": "ISO 4217 currency of amount (microsite currency).",
            "amount": "Component amount in the microsite currency.",
            "operator_currency": "ISO 4217 currency of operator_amount.",
            "operator_amount": "Component amount in the operator currency.",
        },
        """SELECT b.booking_ref, x.key AS component,
               x.value:microsite.currency::string AS currency, x.value:microsite.amount::decimal(18,2) AS amount,
               x.value:operator.currency::string AS operator_currency,
               x.value:operator.amount::decimal(18,2) AS operator_amount
        FROM {c}.silver.bookings b, LATERAL variant_explode(b.payload:pricebreakdown) x
        WHERE x.value:microsite.amount IS NOT NULL""",
    ),
    "booking_service_amendments": (
        "Amendments of booking services (price changes after booking), one row per amendment; manual services have none.",
        {
            "booking_ref": "Travel Compositor booking reference.",
            "service_type": "Service array of the payload (hotelservice ...).",
            "service_id": "Travel Compositor service id, unique inside one service type.",
            "amendment_id": "Travel Compositor amendment id.",
            "reference": "Amendment reference.",
            "description": "Amendment description.",
            "sale_currency": "Currency of the sale amounts.",
            "sale_amount": "Change of the sale price made by the amendment; Travel Compositor adds it to the booking total.",
            "original_sale_amount": "Sale amount of originalPriceBreakDown when the payload carries it.",
            "cost_currency": "Currency the supplier charges.",
            "cost_amount": "Change of the net supplier cost made by the amendment, in cost_currency.",
            "original_cost_amount": "Net supplier cost of originalPriceBreakDown when the payload carries it.",
        },
        """SELECT b.booking_ref, t.key AS service_type, s.value:id::string AS service_id,
               a.value:id::string AS amendment_id, a.value:reference::string AS reference,
               a.value:description::string AS description,
               a.value:priceBreakDown.totalPrice.microsite.currency::string AS sale_currency,
               a.value:priceBreakDown.totalPrice.microsite.amount::decimal(18,2) AS sale_amount,
               a.value:originalPriceBreakDown.totalPrice.microsite.amount::decimal(18,2) AS original_sale_amount,
               a.value:priceBreakDown.providerNetPrice.currency::string AS cost_currency,
               a.value:priceBreakDown.providerNetPrice.amount::decimal(18,2) AS cost_amount,
               a.value:originalPriceBreakDown.providerNetPrice.amount::decimal(18,2) AS original_cost_amount
        FROM {c}.silver.bookings b, LATERAL variant_explode(b.payload) t, LATERAL variant_explode(t.value) s,
             LATERAL variant_explode(s.value:amendments) a
        WHERE t.key LIKE '%service' AND t.key <> 'manualServices'""",
    ),
    "accommodations": (
        "Accommodation catalog of Travel Compositor, current version (open rows of accommodations_history).",
        {
            "accommodation_id": "Travel Compositor accommodation id.",
            "name": "Accommodation name.",
            "country": "Country code of the accommodation.",
            "latitude": "Latitude in decimal degrees.",
            "longitude": "Longitude in decimal degrees.",
            "last_update_at": "lastUpdate of the catalog entry, else the landing time.",
        },
        """SELECT accommodation_id, name, country, latitude, longitude, last_update_at FROM {c}.silver.accommodations_history WHERE __END_AT IS NULL""",
    ),
}

# Money: every metric view that sums amounts has a dimension Currency; amounts of different currencies must not be
# added, so a total is only meaningful grouped or filtered by Currency (bookings are USD except a few EUR ones).
# Days overdue and the ageing bucket depend on today, so they are computed here at query time: in the materialized
# view fact_pending_payment, current_date() in the SELECT list would freeze the value at the last full recompute
# and prevent incremental refresh.
# Join names differ from every dimension name: identifiers are case-insensitive, so a join called agency next to
# the dimension Agency resolves agency.agency_name against the dimension (INVALID_EXTRACT_BASE_FIELD_TYPE).
METRIC_VIEWS = {
    "booking_metrics": """
version: 1.1
comment: Bookings of Vio Travel by creation date, agency and status. Amounts are in the booking currency; group or filter by Currency before adding them.
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
  - name: Currency
    expr: currency
measures:
  - name: Bookings
    expr: COUNT(1)
  - name: Active bookings
    expr: COUNT(1) FILTER (WHERE is_active)
  - name: Cancelled bookings
    expr: COUNT(1) FILTER (WHERE is_cancelled)
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
comment: Services of active bookings by supplier, destination and service category. Amounts are in the booking currency; group or filter by Currency before adding them.
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
  - name: Currency
    expr: booking_currency
measures:
  - name: Services
    expr: COUNT(1)
  - name: Bookings
    expr: COUNT(DISTINCT booking_ref)
  - name: Sale amount
    expr: SUM(sale_amount) FILTER (WHERE sale_currency = booking_currency)
  - name: Cost in booking currency
    expr: SUM(cost_in_booking_currency)
  - name: Margin in booking currency
    expr: SUM(margin_in_booking_currency)
""",
    "receivable_metrics": """
version: 1.1
comment: Payments still due on bookings, by agency, due date and ageing bucket; overdue counts from the day after due_date, as of today. Amounts are in the payment currency; group or filter by Currency before adding them.
source: {c}.gold.fact_pending_payment
joins:
  - name: dim_agency
    source: {c}.gold.dim_agency
    on: source.agency_key = dim_agency.agency_key
dimensions:
  - name: Agency
    expr: dim_agency.agency_name
  - name: Ageing bucket
    expr: >-
      CASE WHEN due_date IS NULL THEN 'No due date'
           WHEN due_date >= current_date() THEN 'Not due'
           WHEN datediff(current_date(), due_date) <= 30 THEN '1-30 days'
           WHEN datediff(current_date(), due_date) <= 60 THEN '31-60 days'
           WHEN datediff(current_date(), due_date) <= 90 THEN '61-90 days'
           ELSE 'Over 90 days' END
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
    expr: SUM(amount_to_pay) FILTER (WHERE due_date < current_date())
  - name: Average days overdue
    expr: AVG(datediff(current_date(), due_date)) FILTER (WHERE due_date < current_date())
""",
}


def kind(table: str) -> str:
    """SQL keyword for ALTER: pipeline tables are streaming tables or materialized views."""
    if table.startswith(("business_central.", "ops.", "ref.")):
        return "TABLE"
    return "STREAMING TABLE" if table.startswith(STREAMING) else "MATERIALIZED VIEW"


MONITORED_SCHEMAS = ("silver", "gold", "business_central")


def ensure_anomaly_detection(catalog: str) -> list[str]:
    """Create the schema monitor where it is missing; returns the schemas it created a monitor for."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound

    w, created = WorkspaceClient(), []
    for schema in MONITORED_SCHEMAS:
        schema_id = w.schemas.get(f"{catalog}.{schema}").schema_id
        try:
            w.api_client.do("GET", f"/api/data-quality/v1/monitors/schema/{schema_id}")
        except NotFound:
            body = {"object_type": "schema", "object_id": schema_id, "anomaly_detection_config": {}}
            w.api_client.do("POST", "/api/data-quality/v1/monitors", body=body)
            created.append(schema)
    return created


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
    # The service principal owns the pipeline and task tables. The catalog owner (the engineer running this job)
    # grants itself MODIFY on the streaming tables it tags and on the tables it comments, which COMMENT ON requires.
    engineer = spark.sql("SELECT current_user()").first()[0]
    for t in [*(t for t in PII_COLUMNS if kind(t) == "STREAMING TABLE"), *TABLE_COMMENTS]:
        if exists(t):
            run(f"GRANT MODIFY ON TABLE {c}.{t} TO `{engineer}`")
    for t, cols in PII_COLUMNS.items():
        if not exists(t):
            continue
        for col, pii in cols.items():
            run(f"ALTER {kind(t)} {c}.{t} ALTER COLUMN {col} SET TAGS ('pii' = '{pii}')")
    for t, text in TABLE_COMMENTS.items():
        if exists(t):
            run(f"COMMENT ON TABLE {c}.{t} IS '{text}'")

    for name, (comment, columns, query) in SILVER_VIEWS.items():
        cols = ", ".join(f"{col} COMMENT '{text}'" for col, text in columns.items())
        run(f"CREATE OR REPLACE VIEW {c}.silver.{name} ({cols}) COMMENT '{comment}' AS {query.format(c=c)}")

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

    try:
        done.extend(f"anomaly detection on {s}" for s in ensure_anomaly_detection(c))
    except Exception as e:  # noqa: BLE001 - reported with the failed statements
        failed.append(f"anomaly detection -> {str(e).splitlines()[0][:200]}")

    print(f"governance: {len(done)} statements applied")
    for f in failed:
        print("FAILED", f)
    if failed:
        raise RuntimeError(f"{len(failed)} governance statement(s) failed, see the output above")


if __name__ == "__main__":
    main()
