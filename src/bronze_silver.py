"""Task bronze_silver - load landed files and keep the current state, catalog <catalog>:

raw.landing (volume)  gzip NDJSON pages written by ingest_tc.py
bronze.tc_*           one row per landed record, payload kept whole as VARIANT, append-only (Auto Loader)
silver.bookings       current state per booking (MERGE by reference, newest lastUpdateDate wins)
silver.booking_services
                      services of the bookings that really changed, replaced in one atomic MERGE
silver.agencies, silver.suppliers
                      latest complete snapshot of the reference data
ref.*                 mapping tables maintained by Vio Finance (created empty here)

Downstream tasks star_schema.py (analytics model) and bc_tables.py (integration output) read silver.
"""
import argparse
import shutil

SERVICE_ARRAYS = ["hotelservice", "transferservice", "transportservice", "ticketservice", "closedtourservice",
                  "carservice", "insuranceservice", "cruiseservice", "itemservice"]
MANUAL_KEYS = ["hotel", "transfer", "transport", "ticket", "closedTour", "carRental", "cruise", "insurance",
               "other", "accountingAdjustment"]
PII_TABLES = ["bronze.tc_bookings", "bronze.tc_agencies", "bronze.tc_suppliers", "silver.bookings",
              "silver.agencies", "silver.suppliers"]
OWNED_FOR_REPLAY = ["bronze.tc_bookings", "bronze.tc_agencies", "bronze.tc_suppliers", "silver.bookings",
                    "silver.booking_services", "ops.silver_progress"]

def main():
    from pyspark.sql import SparkSession

    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="vio_travel")
    ap.add_argument("--run_id", default="")
    ap.add_argument("--full_refresh", default="false", help="true: rebuild bronze and silver from every landed file")
    a = ap.parse_args()
    spark = SparkSession.builder.getOrCreate()
    c = a.catalog
    vol = f"/Volumes/{c}/raw/landing"
    chk = f"/Volumes/{c}/ops/checkpoints"
    sql = spark.sql

    if a.full_refresh.lower() == "true":
        # Replay every landed file. Only tables and checkpoints owned by this job are reset; ref and ops
        # history (api_calls, ingest_runs, watermark, dq_results, publish, bc_sync) stay.
        for t in OWNED_FOR_REPLAY:
            sql(f"DROP TABLE IF EXISTS {c}.{t}")  # noqa: S608 - fixed list of tables in this catalog
        shutil.rmtree(chk, ignore_errors=True)
        print("full refresh: bronze, silver and checkpoints reset")

    # ---------------- bronze: Auto Loader, exactly-once file tracking via checkpoint ------------
    for entity in ("bookings", "agencies", "suppliers"):
        (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "text")
         .option("pathGlobFilter", "*.ndjson.gz")
         .load(f"{vol}/{entity}")
         .selectExpr("try_parse_json(value) AS doc", "value AS raw_line", "_metadata.file_path AS source_file",
                     "current_timestamp() AS ingested_at")
         # A malformed line is kept with doc = NULL and its text, so one bad line never stops the stream.
         .selectExpr("doc", "CASE WHEN doc IS NULL THEN raw_line END AS bad_line",
                     "doc:_meta.run_id::string AS run_id", "source_file", "ingested_at")
         .writeStream
         .option("checkpointLocation", f"{chk}/bronze_tc_{entity}")
         .trigger(availableNow=True)
         .toTable(f"{c}.bronze.tc_{entity}")
         .awaitTermination())

    # ---------------- silver: bookings (current state) ------------------------------------------
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.silver.bookings (
        booking_ref STRING NOT NULL, status STRING, trip_type STRING, microsite STRING, agency_id STRING,
        agency_name STRING, sales_channel STRING, source_market STRING, created_at TIMESTAMP,
        last_update_at TIMESTAMP, start_date DATE, end_date DATE, pax INT, currency STRING,
        total_price DECIMAL(18,2), net_provider DECIMAL(18,2), operator_fee DECIMAL(18,2),
        operator_management_fee DECIMAL(18,2), paid_amount DECIMAL(18,2), deferred_amount DECIMAL(18,2),
        deferred_limit_date DATE, payload VARIANT, run_id STRING, source_file STRING)
        CLUSTER BY (booking_ref)""")
    sql(f"CREATE TABLE IF NOT EXISTS {c}.ops.silver_progress (processed_until TIMESTAMP, run_id STRING)")
    # Bronze rows not merged yet. The upper bound is fixed before reading, so rows that arrive during this
    # run are picked up by the next run instead of being skipped.
    lo = sql(f"SELECT coalesce(max(processed_until), TIMESTAMP'1970-01-01') AS t FROM {c}.ops.silver_progress"
             ).collect()[0]["t"]
    hi = sql(f"SELECT max(ingested_at) AS t FROM {c}.bronze.tc_bookings").collect()[0]["t"] or lo
    sql(f"""CREATE OR REPLACE TEMP VIEW bookings_changed AS
        SELECT * FROM (
          SELECT doc:record AS r, run_id, source_file,
                 row_number() OVER (PARTITION BY doc:record.bookingReference::string
                                    ORDER BY doc:record.lastUpdateDate::timestamp DESC, ingested_at DESC) AS rn
          FROM {c}.bronze.tc_bookings
          WHERE ingested_at > TIMESTAMP'{lo}' AND ingested_at <= TIMESTAMP'{hi}'
            AND doc:record.bookingReference IS NOT NULL
        ) WHERE rn = 1""")
    # Bookings whose incoming version is newer than silver. The 1-day overlap re-reads unchanged bookings
    # every run; they are skipped here, so their services are not rewritten.
    changed = [r["ref"] for r in sql(f"""
        SELECT DISTINCT b.r:bookingReference::string AS ref FROM bookings_changed b
        LEFT JOIN {c}.silver.bookings t ON t.booking_ref = b.r:bookingReference::string
        WHERE t.booking_ref IS NULL OR b.r:lastUpdateDate::timestamp > t.last_update_at""").collect()]
    spark.createDataFrame([(x,) for x in changed], "ref STRING").createOrReplaceTempView("changed_refs")
    print(f"bookings with a newer version in this batch: {len(changed)}")
    sql(f"""MERGE INTO {c}.silver.bookings t
        USING (
          SELECT r:bookingReference::string AS booking_ref, r:status::string AS status,
                 r:tripType::string AS trip_type, r:user.microsite::string AS microsite,
                 r:user.agency.externalId::string AS agency_id, r:user.agency.name::string AS agency_name,
                 r:salesChannel::string AS sales_channel, r:sourceMarket.id::string AS source_market,
                 r:creationDate::timestamp AS created_at, r:lastUpdateDate::timestamp AS last_update_at,
                 to_date(r:startDate::string) AS start_date, to_date(r:endDate::string) AS end_date,
                 coalesce(r:adultCount::int,0)+coalesce(r:childCount::int,0)+coalesce(r:infantCount::int,0) AS pax,
                 r:pricebreakdown.totalPrice.microsite.currency::string AS currency,
                 r:pricebreakdown.totalPrice.microsite.amount::decimal(18,2) AS total_price,
                 r:pricebreakdown.netProvider.microsite.amount::decimal(18,2) AS net_provider,
                 r:pricebreakdown.operatorFee.microsite.amount::decimal(18,2) AS operator_fee,
                 r:pricebreakdown.operatorManagementFee.microsite.amount::decimal(18,2) AS operator_management_fee,
                 r:payment.amount::decimal(18,2) AS paid_amount,
                 r:payment.deferredAmount::decimal(18,2) AS deferred_amount,
                 to_date(r:payment.deferredLimitDate::string) AS deferred_limit_date,
                 r AS payload, run_id, source_file
          FROM bookings_changed b SEMI JOIN changed_refs x ON x.ref = b.r:bookingReference::string) s
        ON t.booking_ref = s.booking_ref
        WHEN MATCHED AND s.last_update_at > t.last_update_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")

    # ---------------- silver: services of the changed bookings ----------------------------------
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.silver.booking_services (
        booking_ref STRING NOT NULL, service_id STRING, service_type STRING, is_manual BOOLEAN, status STRING,
        name STRING, supplier_id STRING, supplier_name STRING, provider STRING, provider_reference STRING,
        start_date DATE, end_date DATE, sale_currency STRING, sale_amount DECIMAL(18,2),
        cost_currency STRING, cost_amount DECIMAL(18,2), cost_in_sale_currency DECIMAL(18,2),
        last_update_at TIMESTAMP) CLUSTER BY (booking_ref)""")
    # Services are exploded from the silver row (the winning version), never from an older bronze copy.
    typed = " UNION ALL ".join(
        f"""SELECT b.booking_ref, '{t}' AS service_type, false AS is_manual, s.value AS s, b.last_update_at
            FROM {c}.silver.bookings b SEMI JOIN changed_refs x ON x.ref = b.booking_ref,
            LATERAL variant_explode(b.payload:{t}) s""" for t in SERVICE_ARRAYS)
    manual = " UNION ALL ".join(
        f"""SELECT b.booking_ref, 'manual_{k}' AS service_type, true AS is_manual, s.value AS s, b.last_update_at
            FROM {c}.silver.bookings b SEMI JOIN changed_refs x ON x.ref = b.booking_ref,
            LATERAL variant_explode(b.payload:manualServices.{k}) s""" for k in MANUAL_KEYS)
    sql(f"""CREATE OR REPLACE TEMP VIEW services_changed AS
        SELECT booking_ref, coalesce(s:id::string, booking_ref || '-' || service_type) AS service_id, service_type,
               is_manual, s:status::string AS status,
               coalesce(s:hotelName::string, s:name::string, s:airlineName::string, s:description::string) AS name,
               s:supplierId::string AS supplier_id,
               coalesce(s:supplierName::string, s:providerName::string) AS supplier_name,
               s:provider::string AS provider,
               coalesce(s:providerBookingReference::string, s:providerTransferCode::string) AS provider_reference,
               to_date(left(s:startDate::string,10)) AS start_date, to_date(left(s:endDate::string,10)) AS end_date,
               s:pricebreakdown.totalPrice.microsite.currency::string AS sale_currency,
               s:pricebreakdown.totalPrice.microsite.amount::decimal(18,2) AS sale_amount,
               coalesce(s:pricebreakdown.providerNetPrice.currency::string,
                        s:pricebreakdown.netProviderPrice.currency::string) AS cost_currency,
               coalesce(s:pricebreakdown.providerNetPrice.amount::decimal(18,2),
                        s:pricebreakdown.netProviderPrice.amount::decimal(18,2)) AS cost_amount,
               s:pricebreakdown.netProvider.microsite.amount::decimal(18,2) AS cost_in_sale_currency,
               last_update_at
        FROM ({typed} UNION ALL {manual})""")
    # One atomic MERGE replaces the services of every changed booking. The source holds one delete marker per
    # changed booking (matches and deletes all its old rows) and the new service rows (never match, inserted).
    sql(f"""MERGE INTO {c}.silver.booking_services t
        USING (SELECT ref AS marker_ref, CAST(NULL AS STRING) AS booking_ref, NULL AS service_id,
                      NULL AS service_type, NULL AS is_manual, NULL AS status, NULL AS name, NULL AS supplier_id,
                      NULL AS supplier_name, NULL AS provider, NULL AS provider_reference, NULL AS start_date,
                      NULL AS end_date, NULL AS sale_currency, NULL AS sale_amount, NULL AS cost_currency,
                      NULL AS cost_amount, NULL AS cost_in_sale_currency, NULL AS last_update_at
               FROM changed_refs
               UNION ALL
               SELECT NULL, * FROM services_changed) s
        ON s.marker_ref IS NOT NULL AND t.booking_ref = s.marker_ref
        WHEN MATCHED THEN DELETE
        WHEN NOT MATCHED AND s.marker_ref IS NULL THEN INSERT
          (booking_ref, service_id, service_type, is_manual, status, name, supplier_id, supplier_name, provider,
           provider_reference, start_date, end_date, sale_currency, sale_amount, cost_currency, cost_amount,
           cost_in_sale_currency, last_update_at)
          VALUES (s.booking_ref, s.service_id, s.service_type, s.is_manual, s.status, s.name, s.supplier_id,
           s.supplier_name, s.provider, s.provider_reference, s.start_date, s.end_date, s.sale_currency,
           s.sale_amount, s.cost_currency, s.cost_amount, s.cost_in_sale_currency, s.last_update_at)""")
    sql(f"INSERT INTO {c}.ops.silver_progress VALUES (TIMESTAMP'{hi}', '{a.run_id}')")

    # ---------------- silver: reference data = latest complete snapshot -------------------------
    ref_cols = {
        "agencies": """doc:record.id::string AS agency_id, doc:record.name::string AS name,
                 doc:record.businessName::string AS business_name, doc:record.active::boolean AS active,
                 doc:record.email::string AS email, doc:record.billingEmail::string AS billing_email,
                 doc:record.country::string AS country, doc:record.city::string AS city,
                 doc:record.addressText::string AS address, doc:record.postalCode::string AS postal_code,
                 doc:record.phoneNumber::string AS phone, doc:record.documentNumber::string AS tax_id,
                 doc:record.invoiceType::string AS invoice_type,
                 doc:record.deferredPaymentAllowed::boolean AS deferred_payment_allowed,
                 doc:record.deferredDays::int AS deferred_days""",
        "suppliers": """doc:record.id::string AS supplier_id, doc:record.commercialName::string AS commercial_name,
                 doc:record.legalName::string AS legal_name, doc:record.active::boolean AS active,
                 doc:record.email::string AS email, doc:record.country.id::string AS country,
                 doc:record.taxpayerId::string AS tax_id, doc:record.phoneNumber::string AS phone,
                 doc:record.address.address::string AS address"""}
    for entity, cols in ref_cols.items():
        sql(f"""CREATE OR REPLACE TABLE {c}.silver.{entity} AS
            SELECT * EXCEPT (rn) FROM (
              SELECT {cols}, ingested_at, run_id,
                     row_number() OVER (PARTITION BY doc:record.id::string ORDER BY ingested_at DESC) AS rn
              FROM {c}.bronze.tc_{entity}
              WHERE run_id = (SELECT max_by(run_id, finished_at) FROM {c}.ops.ingest_runs
                              WHERE entity = '{entity}' AND status = 'SUCCESS'))
            WHERE rn = 1""")

    # ---------------- ref: mapping tables maintained by Vio Finance -----------------------------
    sql(f"CREATE TABLE IF NOT EXISTS {c}.ref.umbrella_agency (agency_id STRING, note STRING)")
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ref.customer_map (
        agency_reference_key STRING, bc_customer_no STRING, display_name STRING, approved_by STRING, note STRING)""")
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ref.vendor_map (
        source_kind STRING, source_key STRING, bc_vendor_no STRING, display_name STRING, approved_by STRING,
        note STRING)""")
    for t in PII_TABLES:
        sql(f"ALTER TABLE {c}.{t} SET TAGS ('contains_pii' = 'true')")


if __name__ == "__main__":
    main()
