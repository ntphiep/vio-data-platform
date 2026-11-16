"""Pipeline vio_travel_lakehouse, layer silver: cleaned current state and history, catalog from `vio.catalog`.

Bookings (incremental source)
    silver.bookings            SCD type 1, one row per booking, AUTO CDC from the bronze stream; the newest
                               lastUpdateDate wins, so a late or replayed page never overwrites a newer version
    silver.bookings_history    SCD type 2 of status, amounts, dates and pax: one row per tracked change
    silver.booking_services, silver.booking_payments, silver.booking_invoices, silver.booking_price_components
                               materialized views exploded from the current booking payload

Snapshot entities (agencies, suppliers, users, travel ideas, pending payments, purchase attempts)
    silver.<entity>_history    SCD type 2 by AUTO CDC FROM SNAPSHOT over the latest complete snapshot
                               (status SUCCESS in ops.ingest_runs): new keys inserted, changed rows versioned,
                               keys missing from the snapshot closed (a paid pending payment disappears this way)
    silver.<entity>            materialized view of the open versions, the current state

Accommodations
    silver.accommodations_history  SCD type 2 of the catalog by AUTO CDC on lastUpdate (the weekly snapshot has
                               282,842 rows, so only the changed rows of the stream are applied)
    silver.accommodations      open versions
    silver.accommodation_details   SCD type 1 of the datasheets of booked hotels

Expectations: names start with silver_. Rows without a key are dropped (expect_or_drop); the rest warn.
Personal columns of the current tables are masked by the function governance.mask_pii unless the reader is in the
group vio_pii_readers (the pipeline and job service principal vio-travel-pipeline is).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

C = spark.conf.get("vio.catalog")
S = f"{C}.silver"
MASK = f"MASK {C}.governance.mask_pii"
CDF = {"delta.enableChangeDataFeed": "true", "delta.enableRowTracking": "true"}

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

# ---------------------------------------------------------------- bookings ----------------------------------------
BOOKING_COLUMNS = [
    "r:bookingReference::string AS booking_ref",
    "r:status::string AS status",
    "r:tripType::string AS trip_type",
    "r:user.microsite::string AS microsite",
    "r:user.agency.externalId::string AS agency_id",
    "r:user.agency.name::string AS agency_name",
    "r:user.username::string AS username",
    "r:salesChannel::string AS sales_channel",
    "r:sourceMarket.id::string AS source_market",
    "r:creationDate::timestamp AS created_at",
    "r:lastUpdateDate::timestamp AS last_update_at",
    "to_date(r:startDate::string) AS start_date",
    "to_date(r:endDate::string) AS end_date",
    "coalesce(r:adultCount::int, 0) + coalesce(r:childCount::int, 0) + coalesce(r:infantCount::int, 0) AS pax",
    "r:pricebreakdown.totalPrice.microsite.currency::string AS currency",
    "r:pricebreakdown.totalPrice.microsite.amount::decimal(18,2) AS total_price",
    "r:pricebreakdown.netProvider.microsite.amount::decimal(18,2) AS net_provider",
    "r:pricebreakdown.operatorFee.microsite.amount::decimal(18,2) AS operator_fee",
    "r:pricebreakdown.operatorManagementFee.microsite.amount::decimal(18,2) AS operator_management_fee",
    "r:payment.amount::decimal(18,2) AS paid_amount",
    "r:payment.deferredAmount::decimal(18,2) AS deferred_amount",
    "to_date(r:payment.deferredLimitDate::string) AS deferred_limit_date",
    "try_cast(regexp_extract(r:ideaUrl::string, '/idea/([0-9]+)', 1) AS STRING) AS idea_id",
    "r AS payload",
    "run_id",
    "source_file",
    "ingested_at",
]


@dp.temporary_view(name="bookings_changes")
@dp.expect_or_drop("silver_booking_ref_present", "booking_ref IS NOT NULL")
@dp.expect_or_drop("silver_booking_last_update_present", "last_update_at IS NOT NULL")
@dp.expect("silver_booking_has_agency", "agency_id IS NOT NULL")
@dp.expect("silver_booking_dates_ordered", "end_date IS NULL OR start_date IS NULL OR end_date >= start_date")
@dp.expect("silver_booking_has_currency", "currency IS NOT NULL")
def bookings_changes():
    return (
        spark.readStream.table(f"{C}.bronze.tc_bookings")
        .where("doc IS NOT NULL")
        .selectExpr("doc:record AS r", "run_id", "source_file", "ingested_at")
        .selectExpr(*BOOKING_COLUMNS)
    )


dp.create_streaming_table(
    name=f"{S}.bookings",
    comment="Current state of every booking (SCD type 1). payload keeps the whole API record. Contains personal data.",
    cluster_by=["booking_ref"],
    table_properties=CDF,
)
dp.create_auto_cdc_flow(
    target=f"{S}.bookings",
    source="bookings_changes",
    keys=["booking_ref"],
    sequence_by=F.struct("last_update_at", "ingested_at"),
    except_column_list=["ingested_at"],
    stored_as_scd_type=1,
    name="bookings_scd1",
)

dp.create_streaming_table(
    name=f"{S}.bookings_history",
    comment="One row per tracked change of a booking (SCD type 2): status, amounts, dates, pax.",
    cluster_by=["booking_ref"],
    table_properties=CDF,
)
dp.create_auto_cdc_flow(
    target=f"{S}.bookings_history",
    source="bookings_changes",
    keys=["booking_ref"],
    sequence_by="last_update_at",
    column_list=[
        "booking_ref",
        "status",
        "agency_id",
        "currency",
        "total_price",
        "net_provider",
        "paid_amount",
        "deferred_amount",
        "start_date",
        "end_date",
        "pax",
        "last_update_at",
    ],
    track_history_column_list=[
        "status",
        "total_price",
        "net_provider",
        "paid_amount",
        "deferred_amount",
        "start_date",
        "end_date",
        "pax",
    ],
    stored_as_scd_type=2,
    name="bookings_scd2",
)


def _explode_services(cols: str) -> str:
    typed = [
        f"""SELECT b.booking_ref, '{t}' AS service_type, false AS is_manual, s.value AS s, b.last_update_at
            FROM {S}.bookings b, LATERAL variant_explode(b.payload:{t}) s"""
        for t in SERVICE_ARRAYS
    ]
    manual = [
        f"""SELECT b.booking_ref, 'manual_{k}' AS service_type, true AS is_manual, s.value AS s, b.last_update_at
            FROM {S}.bookings b, LATERAL variant_explode(b.payload:manualServices.{k}) s"""
        for k in MANUAL_KEYS
    ]
    return f"SELECT {cols} FROM ({' UNION ALL '.join(typed + manual)})"


@dp.materialized_view(
    name=f"{S}.booking_services",
    comment="One row per service of a booking (hotel, transfer, ticket, manual ...), from the current payload.",
    cluster_by=["booking_ref"],
)
@dp.expect(
    "silver_service_has_amounts",
    "status NOT IN ('BOOKED','RQ') OR (sale_amount IS NOT NULL AND cost_amount IS NOT NULL)",
)
def booking_services():
    return spark.sql(
        _explode_services("""booking_ref, coalesce(s:id::string, booking_ref || '-' || service_type) AS service_id,
           service_type, is_manual, s:status::string AS status,
           coalesce(s:hotelName::string, s:name::string, s:airlineName::string, s:description::string) AS name,
           s:supplierId::string AS supplier_id,
           coalesce(s:supplierName::string, s:providerName::string) AS supplier_name,
           s:provider::string AS provider,
           coalesce(s:providerBookingReference::string, s:providerTransferCode::string) AS provider_reference,
           s:hotelId::string AS accommodation_id,
           to_date(left(s:startDate::string,10)) AS start_date, to_date(left(s:endDate::string,10)) AS end_date,
           s:pricebreakdown.totalPrice.microsite.currency::string AS sale_currency,
           s:pricebreakdown.totalPrice.microsite.amount::decimal(18,2) AS sale_amount,
           coalesce(s:pricebreakdown.providerNetPrice.currency::string,
                    s:pricebreakdown.netProviderPrice.currency::string) AS cost_currency,
           coalesce(s:pricebreakdown.providerNetPrice.amount::decimal(18,2),
                    s:pricebreakdown.netProviderPrice.amount::decimal(18,2)) AS cost_amount,
           s:pricebreakdown.netProvider.microsite.amount::decimal(18,2) AS cost_in_sale_currency,
           last_update_at""")
    )


@dp.materialized_view(
    name=f"{S}.booking_payments",
    comment="Payments recorded on a booking: first payment, second payment, additional and manual payments.",
    cluster_by=["booking_ref"],
)
@dp.expect("silver_payment_has_amount", "amount IS NOT NULL")
def booking_payments():
    return spark.sql(f"""
        SELECT booking_ref, 'first' AS payment_kind, payload:payment.id::string AS payment_id,
               payload:payment.orderNumber::string AS order_number, payload:payment.paymentSystem::string AS payment_system,
               payload:payment.paymentType::string AS payment_type,
               payload:payment.paymentConfigurationDescription::string AS description,
               payload:payment.currency::string AS currency, payload:payment.amount::decimal(18,2) AS amount,
               payload:payment.deferredAmount::decimal(18,2) AS deferred_amount,
               to_date(payload:payment.deferredLimitDate::string) AS deferred_limit_date,
               CAST(NULL AS TIMESTAMP) AS paid_at, payload:payment.paymentRefundStatus::string AS refund_status
        FROM {S}.bookings WHERE payload:payment IS NOT NULL
        UNION ALL
        SELECT booking_ref, 'second', payload:payment.id::string || '-2', payload:payment.orderNumber::string,
               payload:payment.paymentSystem::string, payload:payment.paymentType::string, 'Second payment',
               payload:payment.secondPaymentCurrency::string, payload:payment.secondPaymentAmount::decimal(18,2),
               NULL, NULL,
               try_to_timestamp(payload:payment.secondPaymentDate::string || ' ' ||
                                coalesce(payload:payment.secondPaymentTime::string, '00:00:00')),
               payload:payment.secondPaymentRefundStatus::string
        FROM {S}.bookings WHERE payload:payment.secondPaymentAmount IS NOT NULL
        UNION ALL
        SELECT b.booking_ref, 'additional', x.value:id::string, x.value:orderNumber::string, NULL, NULL, NULL,
               x.value:amount.currency::string, x.value:amount.amount::decimal(18,2),
               x.value:deferredAmount.amount::decimal(18,2),
               to_date(left(x.value:deferredPaymentLimitDate::string, 10)), x.value:date::timestamp, NULL
        FROM {S}.bookings b, LATERAL variant_explode(b.payload:payment.additionalPayments) x
        UNION ALL
        SELECT b.booking_ref, 'manual', x.value:id::string, x.value:orderNumber::string, NULL, NULL,
               x.value:description::string,
               coalesce(x.value:amount.currency::string, x.value:currency::string),
               coalesce(x.value:amount.amount::decimal(18,2), x.value:amount::decimal(18,2)), NULL, NULL,
               x.value:date::timestamp, NULL
        FROM {S}.bookings b, LATERAL variant_explode(b.payload:manualPayments) x""")


@dp.materialized_view(
    name=f"{S}.booking_invoices",
    comment="Invoices issued by Travel Compositor for a booking (number, target, creation date).",
)
def booking_invoices():
    return spark.sql(f"""
        SELECT b.booking_ref, x.value:number::string AS invoice_number, x.value:target::string AS target,
               to_date(x.value:creationDate::string) AS created_date
        FROM {S}.bookings b, LATERAL variant_explode(b.payload:invoices) x""")


@dp.materialized_view(
    name=f"{S}.booking_price_components",
    comment="Price breakdown of a booking, one row per component (totalPrice, netProvider, operatorFee ...).",
    cluster_by=["booking_ref"],
)
def booking_price_components():
    return spark.sql(f"""
        SELECT b.booking_ref, x.key AS component,
               x.value:microsite.currency::string AS currency, x.value:microsite.amount::decimal(18,2) AS amount,
               x.value:operator.currency::string AS operator_currency,
               x.value:operator.amount::decimal(18,2) AS operator_amount
        FROM {S}.bookings b, LATERAL variant_explode(b.payload:pricebreakdown) x
        WHERE x.value:microsite.amount IS NOT NULL""")


# ---------------------------------------------------------------- snapshot entities -------------------------------
# entity: (keys, select list over r = doc:record, comment, schema of the current table with masks or None)
SNAPSHOTS = {
    "agencies": (
        ["agency_id"],
        """r:id::string AS agency_id, r:name::string AS name, r:businessName::string AS business_name,
           r:active::boolean AS active, r:email::string AS email, r:billingEmail::string AS billing_email,
           r:country::string AS country, r:city::string AS city, r:addressText::string AS address,
           r:postalCode::string AS postal_code, r:phoneNumber::string AS phone, r:documentNumber::string AS tax_id,
           r:invoiceType::string AS invoice_type, r:deferredPaymentAllowed::boolean AS deferred_payment_allowed,
           r:deferredDays::int AS deferred_days, r:deferredLimit::decimal(18,2) AS deferred_limit""",
        "Agencies (customers of Vio) as in the latest snapshot. Contains personal data.",
        f"""agency_id STRING NOT NULL, name STRING, business_name STRING, active BOOLEAN, email STRING {MASK},
            billing_email STRING {MASK}, country STRING, city STRING, address STRING {MASK}, postal_code STRING,
            phone STRING {MASK}, tax_id STRING, invoice_type STRING, deferred_payment_allowed BOOLEAN,
            deferred_days INT, deferred_limit DECIMAL(18,2),
            CONSTRAINT agencies_pk PRIMARY KEY (agency_id) RELY""",
    ),
    "suppliers": (
        ["supplier_id"],
        """r:id::string AS supplier_id, r:commercialName::string AS commercial_name,
           r:legalName::string AS legal_name, r:active::boolean AS active, r:email::string AS email,
           r:country.id::string AS country, r:taxpayerId::string AS tax_id, r:phoneNumber::string AS phone,
           r:address.street::string AS address""",
        "Contract suppliers as in the latest snapshot.",
        f"""supplier_id STRING NOT NULL, commercial_name STRING, legal_name STRING, active BOOLEAN,
            email STRING {MASK}, country STRING, tax_id STRING, phone STRING {MASK}, address STRING,
            CONSTRAINT suppliers_pk PRIMARY KEY (supplier_id) RELY""",
    ),
    "users": (
        ["user_key"],
        """coalesce(r:agency::string, '-') || ':' || r:username::string AS user_key, r:agency::string AS agency_id,
           r:username::string AS username, r:name::string AS name, r:surname::string AS surname,
           r:email::string AS email, r:telephone::string AS telephone, r:country::string AS country,
           r:profile::string AS profile, r:active::boolean AS active, r:b2c::boolean AS b2c,
           r:newsletter::boolean AS newsletter""",
        "Users of the Travel Compositor microsite (agency staff and B2C customers). Contains personal data.",
        f"""user_key STRING NOT NULL, agency_id STRING, username STRING {MASK}, name STRING {MASK},
            surname STRING {MASK}, email STRING {MASK}, telephone STRING {MASK}, country STRING, profile STRING,
            active BOOLEAN, b2c BOOLEAN, newsletter BOOLEAN,
            CONSTRAINT users_pk PRIMARY KEY (user_key) RELY""",
    ),
    "travel_ideas": (
        ["idea_id"],
        """r:id::string AS idea_id, r:title::string AS title, r:largeTitle::string AS large_title,
           r:tripType::string AS trip_type, r:active::boolean AS active,
           to_date(r:creationDate::string) AS created_date, to_date(r:departureDate::string) AS departure_date,
           r:lastUpdateDate::timestamp AS last_update_at, r:user::string AS username,
           r:pricePerPerson.currency::string AS currency, r:pricePerPerson.amount::decimal(18,2) AS price_per_person,
           r:totalPrice.amount::decimal(18,2) AS total_price, r:counters.destinations::int AS destination_count,
           r:counters.hotelNights::int AS hotel_nights, r:counters.adults::int AS adults,
           r:counters.children::int AS children,
           array_join(transform(from_json(r:destinations::string, 'array<struct<name:string>>'), d -> d.name), ', ')
             AS destinations,
           r:ideaUrl::string AS idea_url""",
        "Travel ideas (packaged itineraries) published on the microsite, as in the latest snapshot.",
        None,
    ),
    "pending_payments": (
        ["payment_id"],
        """r:paymentId::string AS payment_id, r:bookingReference::string AS booking_ref,
           r:paymentNumber::int AS payment_number, r:amountToPay.currency::string AS currency,
           r:amountToPay.amount::decimal(18,2) AS amount_to_pay,
           r:deferredAmount.amount::decimal(18,2) AS deferred_amount,
           to_timestamp(r:deferredLimitDateTime::string) AS deferred_limit_at""",
        "Payments still due on bookings; a row closes in the history when the payment is no longer pending.",
        None,
    ),
    "purchase_attempts": (
        ["attempt_id"],
        """r:id::string AS attempt_id, to_date(r:creationDate::string) AS created_date, r:user::string AS username,
           r:email::string AS email, r:tripType::string AS trip_type, r:active::boolean AS active,
           r:totalPrice.currency::string AS currency, r:totalPrice.amount::decimal(18,2) AS total_price,
           r:counters.destinations::int AS destination_count, r:counters.hotelNights::int AS hotel_nights,
           r:counters.adults::int AS adults, r:title::string AS title""",
        "Purchase attempts (checkouts started but not booked), as in the latest snapshot. Contains personal data.",
        f"""attempt_id STRING NOT NULL, created_date DATE, username STRING {MASK}, email STRING {MASK},
            trip_type STRING, active BOOLEAN, currency STRING, total_price DECIMAL(18,2), destination_count INT,
            hotel_nights INT, adults INT, title STRING,
            CONSTRAINT purchase_attempts_pk PRIMARY KEY (attempt_id) RELY""",
    ),
}


def snapshot_entity(entity: str, keys: list[str], cols: str, comment: str, schema: str | None) -> None:
    @dp.temporary_view(name=f"{entity}_snapshot")
    def latest_snapshot():
        # The latest complete snapshot only: a PARTIAL or FAILED read would close rows that still exist.
        return spark.sql(f"""
            SELECT {cols} FROM (
              SELECT doc:record AS r FROM {C}.bronze.tc_{entity}
              WHERE doc IS NOT NULL AND run_id = (
                SELECT max_by(run_id, finished_at) FROM {C}.ops.ingest_runs
                WHERE entity = '{entity}' AND status = 'SUCCESS'))""").dropDuplicates(keys)

    dp.create_streaming_table(
        name=f"{S}.{entity}_history",
        comment=f"History (SCD type 2) of {entity.replace('_', ' ')}: one row per version, __END_AT NULL = current.",
        cluster_by_auto=True,
        table_properties=CDF,
    )
    dp.create_auto_cdc_from_snapshot_flow(
        target=f"{S}.{entity}_history",
        source=f"{entity}_snapshot",
        keys=keys,
        stored_as_scd_type=2,
    )

    @dp.materialized_view(name=f"{S}.{entity}", comment=comment, cluster_by_auto=True, schema=schema)
    @dp.expect_all_or_drop({f"silver_{entity}_key_present": " AND ".join(f"{k} IS NOT NULL" for k in keys)})
    def current():
        return spark.read.table(f"{S}.{entity}_history").where("__END_AT IS NULL").drop("__START_AT", "__END_AT")


for _entity, (_keys, _cols, _comment, _schema) in SNAPSHOTS.items():
    snapshot_entity(_entity, _keys, _cols, _comment, _schema)


# ---------------------------------------------------------------- accommodations ----------------------------------
@dp.temporary_view(name="accommodations_changes")
@dp.expect_or_drop("silver_accommodation_id_present", "accommodation_id IS NOT NULL")
def accommodations_changes():
    return (
        spark.readStream.table(f"{C}.bronze.tc_accommodations")
        .where("doc IS NOT NULL")
        .selectExpr(
            "doc:record.id::string AS accommodation_id",
            "doc:record.name::string AS name",
            "doc:record.countryCode::string AS country",
            "doc:record.geolocation.latitude::double AS latitude",
            "doc:record.geolocation.longitude::double AS longitude",
            "coalesce(try_to_timestamp(doc:record.lastUpdate::string, 'yyyy-MM-dd HH:mm'), ingested_at) AS last_update_at",
        )
    )


dp.create_streaming_table(
    name=f"{S}.accommodations_history",
    comment="History (SCD type 2) of the Travel Compositor accommodation catalog, by lastUpdate.",
    cluster_by=["country", "accommodation_id"],
    table_properties=CDF,
)
dp.create_auto_cdc_flow(
    target=f"{S}.accommodations_history",
    source="accommodations_changes",
    keys=["accommodation_id"],
    sequence_by="last_update_at",
    stored_as_scd_type=2,
    track_history_except_column_list=["last_update_at"],
    name="accommodations_scd2",
)


@dp.materialized_view(
    name=f"{S}.accommodations",
    comment="Accommodation catalog of Travel Compositor, current version (about 283,000 hotels).",
    cluster_by=["country"],
)
def accommodations():
    return spark.read.table(f"{S}.accommodations_history").where("__END_AT IS NULL").drop("__START_AT", "__END_AT")


@dp.temporary_view(name="accommodation_details_changes")
@dp.expect_or_drop("silver_accommodation_detail_id_present", "accommodation_id IS NOT NULL")
def accommodation_details_changes():
    return (
        spark.readStream.table(f"{C}.bronze.tc_accommodation_datasheets")
        .where("doc IS NOT NULL")
        .selectExpr("doc:record AS r", "doc:_meta.fetched_at::timestamp AS fetched_at")
        .selectExpr(
            "r:id::string AS accommodation_id",
            "r:name::string AS name",
            "r:accommodationType::string AS accommodation_type",
            "r:accommodationSubtype::string AS accommodation_subtype",
            "r:category::string AS category",
            "r:chain::string AS chain",
            "r:destination.code::string AS destination_code",
            "r:destination.name::string AS destination_name",
            "r:address::string AS address",
            "r:geolocation.latitude::double AS latitude",
            "r:geolocation.longitude::double AS longitude",
            "transform(from_json(r:ratings::string, 'array<struct<numReviews:int,score:string,source:string>>'),"
            " x -> named_struct('score', try_cast(x.score AS DOUBLE) * CASE WHEN x.source = 'Tripadvisor' THEN 2 ELSE 1 END,"
            " 'reviews', x.numReviews)) AS ratings",
            "size(from_json(r:images::string, 'array<string>')) AS image_count",
            "transform(from_json(r:accommodationFacilities::string, 'array<struct<translations:map<string,string>>>'),"
            " f -> f.translations['EN']) AS facilities",
            "fetched_at",
        )
        # rating on a 10 scale (Tripadvisor scores are out of 5), mean over the sources that give a score
        .selectExpr(
            "* EXCEPT (ratings)",
            "aggregate(filter(ratings, x -> x.score IS NOT NULL), 0D, (a, x) -> a + x.score)"
            " / nullif(size(filter(ratings, x -> x.score IS NOT NULL)), 0) AS rating_score_10",
            "aggregate(ratings, 0, (a, x) -> a + coalesce(x.reviews, 0)) AS rating_reviews",
        )
    )


dp.create_streaming_table(
    name=f"{S}.accommodation_details",
    comment="Datasheet of each booked accommodation (type, category, chain, destination, ratings), SCD type 1.",
    cluster_by=["accommodation_id"],
    table_properties=CDF,
)
dp.create_auto_cdc_flow(
    target=f"{S}.accommodation_details",
    source="accommodation_details_changes",
    keys=["accommodation_id"],
    sequence_by="fetched_at",
    stored_as_scd_type=1,
    name="accommodation_details_scd1",
)
