"""Pipeline vio_travel_build_bronze_silver_gold, layer silver: cleaned current state and history, catalog from `vio.catalog`.

Bookings (incremental source)
    silver.bookings            SCD type 1, one row per booking, AUTO CDC from the bronze stream; the newest
                               lastUpdateDate wins, so a late or replayed page never overwrites a newer version
    silver.bookings_history    SCD type 2 of status, amounts, dates and pax: one row per tracked change
    silver.booking_services, silver.booking_payments
                               materialized views exploded from the current booking payload
    silver.booking_invoices, silver.booking_price_components
                               plain views over the payload, created by job vio_travel_apply_governance (no reader in the
                               pipeline, and a view costs one Unity Catalog table slot where a materialized view
                               costs two: the Free Edition metastore holds 500)

Snapshot entities (agencies, suppliers, users, travel ideas, pending payments, purchase attempts)
    silver.<entity>_history    SCD type 2 by AUTO CDC FROM SNAPSHOT: new keys inserted, changed rows versioned,
                               keys missing from the snapshot closed (a paid pending payment disappears this way).
                               The source is a function that returns one complete snapshot (status SUCCESS in
                               ops.ingest_runs) at a time, versioned by the finished_at of its extract run, so each
                               snapshot is applied once, in run order, and an update without a new snapshot applies
                               nothing. The pipeline keeps the last applied version across updates and restarts.
    silver.<entity>            materialized view of the open versions, the current state

Accommodations
    silver.accommodations_history  SCD type 2 of the catalog by AUTO CDC on lastUpdate (the weekly snapshot has
                               282,842 rows, so only the changed rows of the stream are applied)
    silver.accommodations      open versions, a plain view created by job vio_travel_apply_governance
    silver.accommodation_details   SCD type 1 of the datasheets of booked hotels

Expectations: names start with silver_. Rows without a key are dropped (expect_or_drop); the rest warn.
Personal columns are masked by the function governance.mask_pii unless the reader is in the group vio_pii_readers
(the pipeline and job service principal vio-travel-pipeline is). Masks and column comments are declared in the schema
DDL of each table through schema() below; temporary views carry neither.
"""

import datetime as dt

from pyspark import pipelines as dp
from pyspark.sql import functions as F

C = spark.conf.get("vio.catalog")
S = f"{C}.silver"
MASK = f"MASK {C}.governance.mask_pii"
CDF = {"delta.enableChangeDataFeed": "true", "delta.enableRowTracking": "true"}
# SCD type 2 columns; the snapshot flows use the TIMESTAMP version below, the booking history a struct (sequence_by).
SCD2 = """__START_AT TIMESTAMP
        __END_AT TIMESTAMP"""
DOCS = {
    "booking_ref": "Travel Compositor booking reference.",
    "status": "Travel Compositor status: BOOKED, RQ (on request), CANCELED ...",
    "agency_id": "Travel Compositor agency id.",
    "currency": "ISO 4217 currency of the amounts of this row.",
    "total_price": "Total sale price, in currency.",
    "net_provider": "Net cost of all services, in currency.",
    "paid_amount": "Amount paid on the first payment, in currency.",
    "deferred_amount": "Amount whose payment is deferred, in currency.",
    "start_date": "First day of the trip or service.",
    "end_date": "Last day of the trip or service.",
    "pax": "Passengers: adults + children + infants.",
    "last_update_at": "Last update time in Travel Compositor.",
    "trip_type": "Travel Compositor trip type.",
    "active": "Active flag in Travel Compositor.",
    "country": "Country code.",
    "email": "E-mail address. Masked.",
    "phone": "Phone number. Masked.",
    "address": "Street address. Masked.",
    "tax_id": "Tax registration number.",
    "username": "Microsite username, often an e-mail address. Masked.",
    "title": "Title of the trip.",
    "created_date": "Creation date.",
    "destination_count": "Number of destinations in the trip.",
    "hotel_nights": "Number of hotel nights in the trip.",
    "adults": "Number of adults.",
    "name": "Name.",
    "latitude": "Latitude in decimal degrees.",
    "longitude": "Longitude in decimal degrees.",
    "accommodation_id": "Travel Compositor accommodation id.",
    "destination_name": "Destination (city or area).",
    "__START_AT": "Version of the snapshot that started this row: finished_at of its extract run (SCD type 2).",
    "__END_AT": "Version of the snapshot that ended this row; NULL = current (SCD type 2).",
}


def schema(cols: str, docs: dict[str, str] | None = None, constraints: str = "") -> str:
    """Schema DDL with a COMMENT on every column: one column per line, comment from docs, else DOCS.

    The COMMENT goes before a MASK clause, the order of the column specification of CREATE TABLE.
    """
    lookup = {**DOCS, **(docs or {})}
    out = []
    for line in cols.strip().splitlines():
        spec = line.strip().rstrip(",")
        head, sep, mask = spec.partition(" MASK ")
        text = lookup[spec.split()[0]].replace("'", "")
        out.append(f"{head} COMMENT '{text}'{sep}{mask}")
    return ",\n".join(out + ([constraints] if constraints else []))


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
    # added for the Business Central specification of YVS; new columns go last, where Delta appends them
    "r:pricebreakdown.agencyManagementFee.microsite.amount::decimal(18,2) AS agency_management_fee",
    "coalesce(nullif(trim(r:bookingAssociatedTo::string), ''), r:bookingReference::string) AS main_booking_ref",
    "nullif(trim(r:referralId::string), '') AS referral_id",
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


BOOKING_DOCS = {
    "trip_type": "Travel Compositor trip type of the booking.",
    "microsite": "Travel Compositor microsite the booking was made on.",
    "agency_name": "Name of the agency that sold the booking.",
    "sales_channel": "Travel Compositor sales channel.",
    "source_market": "Source market id.",
    "created_at": "Creation time of the booking.",
    "currency": "ISO 4217 currency of every amount of the booking (microsite price).",
    "total_price": "Sale price of the booking to the agency, in currency.",
    "operator_fee": "Operator fee of Vio, in currency.",
    "operator_management_fee": "Operator management fee of Vio, in currency.",
    "deferred_limit_date": "Last day to pay the deferred amount.",
    "agency_management_fee": "Agency management fee of the booking (pricebreakdown.agencyManagementFee), booking currency.",
    "main_booking_ref": "Main booking (bookingAssociatedTo) that this booking belongs to; its own reference otherwise.",
    "referral_id": "Travel Compositor referralId; Your Reference of the Business Central sales invoice.",
    "idea_id": "Travel idea id taken from ideaUrl; NULL when the booking has no idea.",
    "payload": "Whole booking record of the API as VARIANT. Contains personal data.",
    "run_id": "Extract run (ops.ingest_runs.run_id) that landed this version.",
    "source_file": "Landing file this version was read from.",
}
dp.create_streaming_table(
    name=f"{S}.bookings",
    comment="Current state of every booking (SCD type 1). payload keeps the whole API record. Contains personal data.",
    cluster_by=["booking_ref"],
    table_properties=CDF,
    schema=schema(
        f"""booking_ref STRING
        status STRING
        trip_type STRING
        microsite STRING
        agency_id STRING
        agency_name STRING
        username STRING {MASK}
        sales_channel STRING
        source_market STRING
        created_at TIMESTAMP
        last_update_at TIMESTAMP
        start_date DATE
        end_date DATE
        pax INT
        currency STRING
        total_price DECIMAL(18,2)
        net_provider DECIMAL(18,2)
        operator_fee DECIMAL(18,2)
        operator_management_fee DECIMAL(18,2)
        paid_amount DECIMAL(18,2)
        deferred_amount DECIMAL(18,2)
        deferred_limit_date DATE
        idea_id STRING
        payload VARIANT
        run_id STRING
        source_file STRING
        agency_management_fee DECIMAL(18,2)
        main_booking_ref STRING
        referral_id STRING""",
        BOOKING_DOCS,
    ),
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
    # __START_AT and __END_AT have the type of sequence_by
    schema=schema(
        """booking_ref STRING
        status STRING
        agency_id STRING
        currency STRING
        total_price DECIMAL(18,2)
        net_provider DECIMAL(18,2)
        paid_amount DECIMAL(18,2)
        deferred_amount DECIMAL(18,2)
        start_date DATE
        end_date DATE
        pax INT
        last_update_at TIMESTAMP
        __START_AT STRUCT<last_update_at:TIMESTAMP,ingested_at:TIMESTAMP>
        __END_AT STRUCT<last_update_at:TIMESTAMP,ingested_at:TIMESTAMP>""",
        BOOKING_DOCS
        | {
            "__START_AT": "lastUpdateDate and landing time of the version that started this row.",
            "__END_AT": "lastUpdateDate and landing time of the next version; NULL = current.",
        },
    ),
)
dp.create_auto_cdc_flow(
    target=f"{S}.bookings_history",
    source="bookings_changes",
    keys=["booking_ref"],
    # ingested_at breaks ties between two landings of the same version; __START_AT and __END_AT are then structs
    # of (last_update_at, ingested_at)
    sequence_by=F.struct("last_update_at", "ingested_at"),
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
    """Two scans of silver.bookings (typed arrays, then manualServices) instead of one scan per array.

    A materialized view keeps one row-id field pair per UNION branch; with one branch per array (19) those fields
    took the 32 statistics columns of Delta and the clustering key booking_ref lost its statistics
    (DELTA_CLUSTERING_COLUMN_MISSING_STATS).
    """
    typed = ", ".join(f"'{t}'" for t in SERVICE_ARRAYS)
    manual = ", ".join(f"'{k}'" for k in MANUAL_KEYS)
    return f"""SELECT {cols} FROM (
        SELECT b.booking_ref, t.key AS service_type, false AS is_manual, s.value AS s, b.last_update_at
        FROM {S}.bookings b, LATERAL variant_explode(b.payload) t, LATERAL variant_explode(t.value) s
        WHERE t.key IN ({typed})
        UNION ALL
        SELECT b.booking_ref, 'manual_' || t.key, true, s.value, b.last_update_at
        FROM {S}.bookings b, LATERAL variant_explode(b.payload:manualServices) t, LATERAL variant_explode(t.value) s
        WHERE t.key IN ({manual}))"""


@dp.materialized_view(
    name=f"{S}.booking_services",
    comment="One row per service of a booking (hotel, transfer, ticket, manual ...), from the current payload.",
    cluster_by=["booking_ref"],
    schema=schema(
        """booking_ref STRING
        service_id STRING
        service_type STRING
        is_manual BOOLEAN
        status STRING
        name STRING
        supplier_id STRING
        supplier_name STRING
        provider STRING
        provider_reference STRING
        accommodation_id STRING
        destination_country STRING
        destination_name STRING
        start_date DATE
        end_date DATE
        sale_currency STRING
        sale_amount DECIMAL(18,2)
        cost_currency STRING
        cost_amount DECIMAL(18,2)
        cost_in_sale_currency DECIMAL(18,2)
        last_update_at TIMESTAMP
        supplier_confirmation STRING
        cancellation_date DATE
        exchange_rate_microsite DECIMAL(24,12)
        amendment_count INT""",
        {
            "service_id": "Travel Compositor service id, unique only inside one service type; booking_ref-type if absent.",
            "service_type": "Service array of the payload (hotelservice ...) or manual_<kind> for manualServices.",
            "is_manual": "True for a service entered by hand (manualServices).",
            "status": "Travel Compositor status of the service.",
            "name": "Hotel, service, airline or description name.",
            "supplier_id": "Travel Compositor contract supplier id; NULL for provider and manual services.",
            "supplier_name": "Supplier or provider name on the service.",
            "provider": "Provider code of an API service.",
            "provider_reference": "Booking reference at the provider.",
            "accommodation_id": "Travel Compositor accommodation id of a hotel service.",
            "destination_country": "Destination country code (upper case).",
            "sale_currency": "Currency of sale_amount (microsite price).",
            "sale_amount": "Sale price of the service, in sale_currency.",
            "cost_currency": "Currency the supplier charges.",
            "cost_amount": "Net cost charged by the supplier, in cost_currency.",
            "cost_in_sale_currency": "Net cost converted by Travel Compositor into the microsite currency.",
            "last_update_at": "Last update time of the booking.",
            "supplier_confirmation": "Confirmation number the supplier gave (service bookingReference).",
            "cancellation_date": "Day the service was cancelled (cancelationDate); NULL when not cancelled.",
            "exchange_rate_microsite": "Travel Compositor exchangeRateMicrosite of the service price; YVS maps it to the Currency Factor.",
            "amendment_count": "Number of amendments of the service; details in silver.booking_service_amendments.",
        },
    ),
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
           s:hotelId::string AS accommodation_id, upper(s:country::string) AS destination_country,
           initcap(coalesce(s:destinationName::string, s:locationName::string, s:destination::string))
             AS destination_name,
           to_date(left(s:startDate::string,10)) AS start_date, to_date(left(s:endDate::string,10)) AS end_date,
           s:pricebreakdown.totalPrice.microsite.currency::string AS sale_currency,
           s:pricebreakdown.totalPrice.microsite.amount::decimal(18,2) AS sale_amount,
           coalesce(s:pricebreakdown.providerNetPrice.currency::string,
                    s:pricebreakdown.netProviderPrice.currency::string) AS cost_currency,
           coalesce(s:pricebreakdown.providerNetPrice.amount::decimal(18,2),
                    s:pricebreakdown.netProviderPrice.amount::decimal(18,2)) AS cost_amount,
           s:pricebreakdown.netProvider.microsite.amount::decimal(18,2) AS cost_in_sale_currency,
           last_update_at, nullif(trim(s:bookingReference::string), '') AS supplier_confirmation,
           to_date(left(s:cancelationDate::string, 10)) AS cancellation_date,
           s:pricebreakdown.exchangeRateMicrosite::decimal(24,12) AS exchange_rate_microsite,
           coalesce(array_size(cast(s:amendments AS ARRAY<VARIANT>)), 0) AS amendment_count""")
    )


@dp.materialized_view(
    name=f"{S}.booking_payments",
    comment="Payments recorded on a booking: first payment, second payment, additional and manual payments.",
    cluster_by=["booking_ref"],
    schema=schema(
        """booking_ref STRING
        payment_kind STRING
        payment_id STRING
        order_number STRING
        payment_system STRING
        payment_type STRING
        description STRING
        currency STRING
        amount DECIMAL(18,2)
        deferred_amount DECIMAL(18,2)
        deferred_limit_date DATE
        paid_at TIMESTAMP
        refund_status STRING""",
        {
            "payment_kind": "first, second, additional or manual.",
            "payment_id": "Travel Compositor payment id (first payment id with -2 for the second payment).",
            "order_number": "Order number at the payment gateway.",
            "payment_system": "Payment system (gateway).",
            "payment_type": "Travel Compositor payment type.",
            "description": "Payment configuration or manual payment description.",
            "currency": "ISO 4217 currency of amount and deferred_amount.",
            "amount": "Amount of the payment, in currency.",
            "deferred_limit_date": "Last day to pay the deferred amount.",
            "paid_at": "Payment time (second, additional and manual payments).",
            "refund_status": "Travel Compositor refund status.",
        },
    ),
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


# ---------------------------------------------------------------- snapshot entities -------------------------------
# entity: (keys, select list over r = doc:record, table comment, columns one per line, column comments,
#          constraints of the current table)
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
        f"""agency_id STRING NOT NULL
        name STRING
        business_name STRING
        active BOOLEAN
        email STRING {MASK}
        billing_email STRING {MASK}
        country STRING
        city STRING
        address STRING {MASK}
        postal_code STRING
        phone STRING {MASK}
        tax_id STRING
        invoice_type STRING
        deferred_payment_allowed BOOLEAN
        deferred_days INT
        deferred_limit DECIMAL(18,2)""",
        {
            "name": "Agency name.",
            "business_name": "Legal business name.",
            "billing_email": "E-mail address for invoices. Masked.",
            "city": "City.",
            "postal_code": "Postal code.",
            "invoice_type": "Travel Compositor invoice type.",
            "deferred_payment_allowed": "True when the agency may pay after booking.",
            "deferred_days": "Days the agency may defer a payment.",
            "deferred_limit": "Credit limit for deferred payments (Travel Compositor deferredLimit).",
        },
        "CONSTRAINT agencies_pk PRIMARY KEY (agency_id) RELY",
    ),
    "suppliers": (
        ["supplier_id"],
        """r:id::string AS supplier_id, r:commercialName::string AS commercial_name,
           r:legalName::string AS legal_name, r:active::boolean AS active, r:email::string AS email,
           r:country.id::string AS country, r:taxpayerId::string AS tax_id, r:phoneNumber::string AS phone,
           r:address.street::string AS address""",
        "Contract suppliers as in the latest snapshot. Contains personal data.",
        f"""supplier_id STRING NOT NULL
        commercial_name STRING
        legal_name STRING
        active BOOLEAN
        email STRING {MASK}
        country STRING
        tax_id STRING
        phone STRING {MASK}
        address STRING {MASK}""",
        {
            "supplier_id": "Travel Compositor supplier id.",
            "commercial_name": "Commercial name of the supplier.",
            "legal_name": "Legal name of the supplier.",
            "country": "Country id of the supplier.",
        },
        "CONSTRAINT suppliers_pk PRIMARY KEY (supplier_id) RELY",
    ),
    "users": (
        ["user_key"],
        """sha2(coalesce(r:agency::string, '-') || ':' || r:username::string, 256) AS user_key, r:agency::string AS agency_id,
           r:username::string AS username, r:name::string AS name, r:surname::string AS surname,
           r:email::string AS email, r:telephone::string AS telephone, r:country::string AS country,
           r:profile::string AS profile, r:active::boolean AS active, r:b2c::boolean AS b2c,
           r:newsletter::boolean AS newsletter""",
        "Users of the Travel Compositor microsite (agency staff and B2C customers). Contains personal data.",
        f"""user_key STRING NOT NULL
        agency_id STRING
        username STRING {MASK}
        name STRING {MASK}
        surname STRING {MASK}
        email STRING {MASK}
        telephone STRING {MASK}
        country STRING
        profile STRING
        active BOOLEAN
        b2c BOOLEAN
        newsletter BOOLEAN""",
        {
            "user_key": "SHA-256 of agency id and username.",
            "agency_id": "Travel Compositor agency id of the user.",
            "name": "First name. Masked.",
            "surname": "Surname. Masked.",
            "telephone": "Phone number. Masked.",
            "profile": "Travel Compositor user profile.",
            "b2c": "True for a B2C customer, false for agency staff.",
            "newsletter": "True when the user subscribed to the newsletter.",
        },
        "CONSTRAINT users_pk PRIMARY KEY (user_key) RELY",
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
        f"""idea_id STRING
        title STRING
        large_title STRING
        trip_type STRING
        active BOOLEAN
        created_date DATE
        departure_date DATE
        last_update_at TIMESTAMP
        username STRING {MASK}
        currency STRING
        price_per_person DECIMAL(18,2)
        total_price DECIMAL(18,2)
        destination_count INT
        hotel_nights INT
        adults INT
        children INT
        destinations STRING
        idea_url STRING""",
        {
            "idea_id": "Travel Compositor idea id.",
            "title": "Title of the idea.",
            "large_title": "Long title of the idea.",
            "departure_date": "Departure date of the idea.",
            "username": "Microsite user who created the idea; often an e-mail address, masked.",
            "currency": "ISO 4217 currency of the prices.",
            "price_per_person": "Advertised price per person, in currency.",
            "total_price": "Advertised total price, in currency.",
            "children": "Number of children.",
            "destinations": "Destination names, comma separated.",
            "idea_url": "URL of the idea on the microsite.",
        },
        "",
    ),
    "pending_payments": (
        ["payment_id"],
        """r:paymentId::string AS payment_id, r:bookingReference::string AS booking_ref,
           r:paymentNumber::int AS payment_number, r:amountToPay.currency::string AS currency,
           r:amountToPay.amount::decimal(18,2) AS amount_to_pay,
           r:deferredAmount.amount::decimal(18,2) AS deferred_amount,
           to_timestamp(r:deferredLimitDateTime::string) AS deferred_limit_at""",
        "Payments still due on bookings; a row closes in the history when the payment is no longer pending.",
        """payment_id STRING
        booking_ref STRING
        payment_number INT
        currency STRING
        amount_to_pay DECIMAL(18,2)
        deferred_amount DECIMAL(18,2)
        deferred_limit_at TIMESTAMP""",
        {
            "payment_id": "Travel Compositor payment id.",
            "payment_number": "Number of the payment within the booking.",
            "currency": "ISO 4217 currency of amount_to_pay and deferred_amount.",
            "amount_to_pay": "Amount still due, in currency.",
            "deferred_limit_at": "Deadline of the payment; overdue after it.",
        },
        "",
    ),
    "purchase_attempts": (
        ["attempt_id"],
        """r:id::string AS attempt_id, to_date(r:creationDate::string) AS created_date, r:user::string AS username,
           r:email::string AS email, r:tripType::string AS trip_type, r:active::boolean AS active,
           r:totalPrice.currency::string AS currency, r:totalPrice.amount::decimal(18,2) AS total_price,
           r:counters.destinations::int AS destination_count, r:counters.hotelNights::int AS hotel_nights,
           r:counters.adults::int AS adults, r:title::string AS title""",
        "Purchase attempts (checkouts started but not booked), as in the latest snapshot. Contains personal data.",
        f"""attempt_id STRING NOT NULL
        created_date DATE
        username STRING {MASK}
        email STRING {MASK}
        trip_type STRING
        active BOOLEAN
        currency STRING
        total_price DECIMAL(18,2)
        destination_count INT
        hotel_nights INT
        adults INT
        title STRING""",
        {
            "attempt_id": "Travel Compositor purchase attempt id (the idea id of the trip).",
            "created_date": "Date the checkout started.",
            "currency": "ISO 4217 currency of total_price.",
            "total_price": "Price of the trip at checkout, in currency.",
        },
        "CONSTRAINT purchase_attempts_pk PRIMARY KEY (attempt_id) RELY",
    ),
}
# Version of the first snapshot when no extract run has succeeded yet: an empty snapshot, so the first update of a
# new catalog does not fail (a version function that returns None on its first call fails the update).
NO_SNAPSHOT = dt.datetime(1970, 1, 1)


def snapshot_entity(
    entity: str, keys: list[str], cols: str, comment: str, columns: str, docs: dict[str, str], constraints: str
) -> None:
    def next_snapshot(latest_version):
        """(DataFrame, version) of the next complete snapshot after latest_version, or None when there is none.

        The version is the finished_at of the extract run cut to whole seconds (TIMESTAMP, the type of __START_AT
        and __END_AT). The pipeline stores the returned version with less than microsecond precision, so a version
        with microseconds would compare greater than its own stored copy and the same snapshot would be applied
        again on every call. Extract runs are minutes apart, so whole seconds keep them distinct. Only
        complete reads count: a PARTIAL or FAILED read would close rows that still exist. Without a stored version
        (first update, or a flow reset) only the newest snapshot is applied: the history already holds the older
        ones, and replaying them would version every row again.
        """
        runs = spark.sql(f"""
            SELECT run_id, date_trunc('SECOND', max(finished_at)) AS version FROM {C}.ops.ingest_runs
            WHERE entity = '{entity}' AND status = 'SUCCESS' GROUP BY run_id""")
        if latest_version is None:
            run = runs.orderBy(F.col("version").desc()).first()
        else:
            run = runs.where(F.col("version") > F.date_trunc("second", F.lit(latest_version))).orderBy("version").first()
        if run is None:
            if latest_version is not None:
                return None
            return spark.sql(f"SELECT {cols} FROM (SELECT CAST(NULL AS VARIANT) AS r) WHERE false"), NO_SNAPSHOT
        # The landed files of that run, not bronze: a snapshot source function may not read a dataset of its own
        # pipeline. Landing keeps 90 days of files, far more than the 30 minutes between updates.
        files = f"/Volumes/{C}/raw/landing/{entity}/ingest_date=*/run={run['run_id']}/*.ndjson.gz"
        snapshot = spark.sql(f"""
            SELECT {cols} FROM (
              SELECT doc:record AS r FROM (SELECT try_parse_json(value) AS doc FROM read_files('{files}', format => 'text'))
              WHERE doc IS NOT NULL)""")
        return snapshot.dropDuplicates(keys), run["version"]

    dp.create_streaming_table(
        name=f"{S}.{entity}_history",
        comment=f"History (SCD type 2) of {entity.replace('_', ' ')}: one row per version, __END_AT NULL = current.",
        cluster_by_auto=True,
        # A full refresh would rebuild the history from the latest snapshot only, so it is not allowed.
        table_properties={**CDF, "pipelines.reset.allowed": "false"},
        schema=schema(columns.replace(" NOT NULL", "") + "\n" + SCD2, docs),
    )
    dp.create_auto_cdc_from_snapshot_flow(
        target=f"{S}.{entity}_history",
        source=next_snapshot,
        keys=keys,
        stored_as_scd_type=2,
    )

    @dp.materialized_view(
        name=f"{S}.{entity}", comment=comment, cluster_by_auto=True, schema=schema(columns, docs, constraints)
    )
    @dp.expect_all_or_drop({f"silver_{entity}_key_present": " AND ".join(f"{k} IS NOT NULL" for k in keys)})
    def current():
        return spark.read.table(f"{S}.{entity}_history").where("__END_AT IS NULL").drop("__START_AT", "__END_AT")


for _entity, (_keys, _cols, _comment, _columns, _docs, _constraints) in SNAPSHOTS.items():
    snapshot_entity(_entity, _keys, _cols, _comment, _columns, _docs, _constraints)


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
    schema=schema(
        """accommodation_id STRING
        name STRING
        country STRING
        latitude DOUBLE
        longitude DOUBLE
        last_update_at TIMESTAMP
        __START_AT TIMESTAMP
        __END_AT TIMESTAMP""",
        {
            "name": "Accommodation name.",
            "country": "Country code of the accommodation.",
            "last_update_at": "lastUpdate of the catalog entry, else the landing time.",
            "__START_AT": "last_update_at of the version that started this row (SCD type 2).",
            "__END_AT": "last_update_at of the next version; NULL = current (SCD type 2).",
        },
    ),
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
    schema=schema(
        """accommodation_id STRING
        name STRING
        accommodation_type STRING
        accommodation_subtype STRING
        category STRING
        chain STRING
        destination_code STRING
        destination_name STRING
        address STRING
        latitude DOUBLE
        longitude DOUBLE
        image_count INT
        facilities ARRAY<STRING>
        fetched_at TIMESTAMP
        rating_score_10 DOUBLE
        rating_reviews INT""",
        {
            "name": "Accommodation name.",
            "accommodation_type": "Accommodation type.",
            "accommodation_subtype": "Accommodation subtype.",
            "category": "Hotel category (stars or keys).",
            "chain": "Hotel chain.",
            "destination_code": "Travel Compositor destination code.",
            "address": "Street address of the hotel (a business address, not masked).",
            "image_count": "Number of images in the datasheet.",
            "facilities": "Facility names in English.",
            "fetched_at": "Time the datasheet was fetched from the API.",
            "rating_score_10": "Mean review score on a 10 scale over the rating sources (Tripadvisor scaled x2).",
            "rating_reviews": "Number of reviews over all rating sources.",
        },
    ),
)
dp.create_auto_cdc_flow(
    target=f"{S}.accommodation_details",
    source="accommodation_details_changes",
    keys=["accommodation_id"],
    sequence_by="fetched_at",
    stored_as_scd_type=1,
    name="accommodation_details_scd1",
)
