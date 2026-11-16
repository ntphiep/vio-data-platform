"""Pipeline vio_travel_lakehouse, layer gold: Kimball star schema and one big table, catalog from `vio.catalog`.

Dimensions (type 1 = current values; *_history = type 2 with valid_from, valid_to)
    dim_date, dim_agency, dim_agency_history, dim_supplier, dim_service_type, dim_user, dim_travel_idea,
    dim_accommodation
Facts
    fact_booking               one booking, current state
    fact_booking_service       one service of a booking
    fact_booking_payment       one payment recorded on a booking
    fact_pending_payment       one payment still due (current snapshot), with days overdue
    fact_booking_status_change one tracked change of a booking (from silver.bookings_history)
    fact_purchase_attempt      one checkout started on the microsite, with whether it became a booking
One big table
    obt_booking_service        fact_booking_service joined with every dimension, for ad-hoc SQL and Genie

Every table is a materialized view: serverless pipelines refresh them incrementally when the plan allows it and
recompute them otherwise. Primary and foreign keys are informational (RELY), which lets the optimizer and Genie use
them; they are not enforced, so the uniqueness and orphan checks run in the job task data_quality (gold_ checks).
Unknown members use key '-1' so every fact row joins to every dimension.
"""

from pyspark import pipelines as dp

C = spark.conf.get("vio.catalog")
S, G = f"{C}.silver", f"{C}.gold"
MASK = f"MASK {C}.governance.mask_pii"
UNKNOWN = "-1"
ACTIVE = "('BOOKED', 'RQ')"
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


def date_key(expr: str) -> str:
    return f"CAST(date_format({expr}, 'yyyyMMdd') AS INT)"


def supplier_key_sql(alias: str) -> str:
    """Same SP and PV numbers as the vendor numbers of bc_tables.py, before any ref.vendor_map override."""
    return (
        f"CASE WHEN {alias}.supplier_id IS NOT NULL THEN 'SP' || {alias}.supplier_id "
        f"WHEN NOT {alias}.is_manual AND {alias}.provider IS NOT NULL "
        f"THEN left('PV' || regexp_replace({alias}.provider, '[^A-Z0-9]', ''), 20) "
        f"WHEN nullif(trim({alias}.supplier_name), '') IS NOT NULL THEN 'MN:' || lower(trim({alias}.supplier_name)) "
        f"ELSE '{UNKNOWN}' END"
    )


# ---------------------------------------------------------------- dimensions --------------------------------------
@dp.materialized_view(
    name=f"{G}.dim_date",
    comment="One row per calendar day, 2015-01-01 to 2030-12-31; date_key is yyyyMMdd.",
    schema="""date_key INT NOT NULL, date DATE, year INT, quarter INT, month INT, month_name STRING,
              year_month STRING, iso_week INT, day_of_week INT, day_name STRING, is_weekend BOOLEAN,
              CONSTRAINT dim_date_pk PRIMARY KEY (date_key) RELY""",
)
def dim_date():
    return spark.sql(f"""
        SELECT {date_key("d")} AS date_key, d AS date, year(d) AS year, quarter(d) AS quarter, month(d) AS month,
               date_format(d, 'MMM') AS month_name, date_format(d, 'yyyy-MM') AS year_month,
               weekofyear(d) AS iso_week, dayofweek(d) AS day_of_week, date_format(d, 'EEE') AS day_name,
               dayofweek(d) IN (1, 7) AS is_weekend
        FROM (SELECT explode(sequence(DATE'2015-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS d)""")


@dp.materialized_view(
    name=f"{G}.dim_agency",
    comment="One row per agency (customer of Vio): latest snapshot, agencies seen only on bookings, and -1 Unknown.",
    schema="""agency_key STRING NOT NULL, agency_name STRING, business_name STRING, country STRING, city STRING,
              active BOOLEAN, invoice_type STRING, deferred_payment_allowed BOOLEAN, deferred_days INT,
              deferred_limit DECIMAL(18,2), is_umbrella BOOLEAN, source STRING,
              CONSTRAINT dim_agency_pk PRIMARY KEY (agency_key) RELY""",
)
def dim_agency():
    return spark.sql(f"""
        SELECT a.agency_id AS agency_key, a.name AS agency_name, a.business_name, a.country, a.city, a.active,
               a.invoice_type, a.deferred_payment_allowed, a.deferred_days, a.deferred_limit,
               u.agency_id IS NOT NULL AS is_umbrella, 'snapshot' AS source
        FROM {S}.agencies a LEFT JOIN {C}.ref.umbrella_agency u ON u.agency_id = a.agency_id
        UNION ALL
        SELECT agency_id, max(agency_name), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, false, 'booking'
        FROM {S}.bookings b WHERE agency_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM {S}.agencies x WHERE x.agency_id = b.agency_id)
        GROUP BY agency_id
        UNION ALL
        SELECT '{UNKNOWN}', 'Unknown', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, false, 'unknown'""")


@dp.materialized_view(
    name=f"{G}.dim_agency_history",
    comment="Agency versions (type 2): valid_from, valid_to (NULL = current). Use for as-of analysis.",
    cluster_by_auto=True,
)
def dim_agency_history():
    return spark.sql(f"""
        SELECT agency_id AS agency_key, name AS agency_name, country, active, deferred_payment_allowed,
               deferred_days, deferred_limit, invoice_type, __START_AT AS valid_from, __END_AT AS valid_to,
               __END_AT IS NULL AS is_current
        FROM {S}.agencies_history""")


@dp.materialized_view(
    name=f"{G}.dim_supplier",
    comment="One row per supplier: SP<id> contract supplier, PV<code> provider, MN:<name> manual service, -1 Unknown.",
    schema="""supplier_key STRING NOT NULL, supplier_source STRING, supplier_name STRING, legal_name STRING,
              country STRING, active BOOLEAN, tc_supplier_id STRING, provider_code STRING,
              CONSTRAINT dim_supplier_pk PRIMARY KEY (supplier_key) RELY""",
)
def dim_supplier():
    return spark.sql(f"""
        SELECT 'SP' || p.supplier_id AS supplier_key, 'Travel Compositor supplier' AS supplier_source,
               coalesce(nullif(trim(p.commercial_name), ''), p.legal_name) AS supplier_name, p.legal_name,
               p.country, p.active, p.supplier_id AS tc_supplier_id, CAST(NULL AS STRING) AS provider_code
        FROM {S}.suppliers p
        UNION ALL
        SELECT k, max(src), max(name), NULL, NULL, NULL, max(sid), max(prov) FROM (
          SELECT {supplier_key_sql("x")} AS k,
                 CASE WHEN x.supplier_id IS NOT NULL THEN 'Travel Compositor supplier'
                      WHEN NOT x.is_manual AND x.provider IS NOT NULL THEN 'Provider'
                      WHEN x.is_manual THEN 'Manual service' ELSE 'Supplier name only' END AS src,
                 coalesce(x.supplier_name, x.provider) AS name, x.supplier_id AS sid,
                 CASE WHEN NOT x.is_manual THEN x.provider END AS prov
          FROM {S}.booking_services x)
        WHERE k <> '{UNKNOWN}' AND NOT EXISTS (SELECT 1 FROM {S}.suppliers p2 WHERE 'SP' || p2.supplier_id = k)
        GROUP BY k
        UNION ALL
        SELECT '{UNKNOWN}', 'Unknown', 'Unknown', NULL, NULL, NULL, NULL, NULL""")


@dp.materialized_view(
    name=f"{G}.dim_service_type",
    comment="One row per service type (hotelservice, manual_hotel ...) with its category.",
    schema="""service_type STRING NOT NULL, service_category STRING, is_manual BOOLEAN,
              CONSTRAINT dim_service_type_pk PRIMARY KEY (service_type) RELY""",
)
def dim_service_type():
    rows = ", ".join(f"('{k}', '{v}', {str(k.startswith('manual_')).lower()})" for k, v in CATEGORY.items())
    known = ", ".join(f"'{k}'" for k in CATEGORY)
    return spark.sql(f"""
        SELECT service_type, service_category, is_manual
        FROM VALUES {rows} AS t(service_type, service_category, is_manual)
        UNION ALL
        SELECT DISTINCT service_type, 'Other', service_type LIKE 'manual_%' FROM {S}.booking_services
        WHERE service_type NOT IN ({known})""")


@dp.materialized_view(
    name=f"{G}.dim_user",
    comment="One row per microsite user (agency staff or B2C customer) and -1 Unknown. Contains personal data.",
    schema=f"""user_key STRING NOT NULL, agency_key STRING, username STRING {MASK}, full_name STRING {MASK},
               email STRING {MASK}, country STRING, profile STRING, active BOOLEAN, b2c BOOLEAN,
               CONSTRAINT dim_user_pk PRIMARY KEY (user_key) RELY""",
)
def dim_user():
    return spark.sql(f"""
        SELECT user_key, agency_id AS agency_key, username, concat_ws(' ', name, surname) AS full_name, email,
               country, profile, active, b2c
        FROM {S}.users
        UNION ALL
        SELECT b.agency_id || ':' || b.username, b.agency_id, max(b.username), NULL, NULL, NULL, NULL, NULL, NULL
        FROM {S}.bookings b
        WHERE b.username IS NOT NULL AND b.agency_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM {S}.users u WHERE u.user_key = b.agency_id || ':' || b.username)
        GROUP BY 1, 2
        UNION ALL
        SELECT '{UNKNOWN}', NULL, 'Unknown', NULL, NULL, NULL, NULL, NULL, NULL""")


@dp.materialized_view(
    name=f"{G}.dim_travel_idea",
    comment="One row per travel idea (packaged itinerary on the microsite) and -1 Unknown.",
    schema="""idea_key STRING NOT NULL, title STRING, trip_type STRING, active BOOLEAN, created_date DATE,
              departure_date DATE, currency STRING, price_per_person DECIMAL(18,2), total_price DECIMAL(18,2),
              destination_count INT, hotel_nights INT, destinations STRING, idea_url STRING,
              CONSTRAINT dim_travel_idea_pk PRIMARY KEY (idea_key) RELY""",
)
def dim_travel_idea():
    return spark.sql(f"""
        SELECT idea_id, title, trip_type, active, created_date, departure_date, currency, price_per_person,
               total_price, destination_count, hotel_nights, destinations, idea_url
        FROM {S}.travel_ideas
        UNION ALL
        SELECT DISTINCT b.idea_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        FROM {S}.bookings b
        WHERE b.idea_id IS NOT NULL AND b.idea_id <> ''
          AND NOT EXISTS (SELECT 1 FROM {S}.travel_ideas i WHERE i.idea_id = b.idea_id)
        UNION ALL
        SELECT '{UNKNOWN}', 'Unknown', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL""")


@dp.materialized_view(
    name=f"{G}.dim_accommodation",
    comment="One row per accommodation booked by Vio: datasheet attributes when fetched, else the name on the service.",
    schema="""accommodation_key STRING NOT NULL, accommodation_name STRING, accommodation_type STRING,
              category STRING, chain STRING, destination_name STRING, country STRING, latitude DOUBLE,
              longitude DOUBLE, rating_score_10 DOUBLE, rating_reviews INT, has_datasheet BOOLEAN,
              CONSTRAINT dim_accommodation_pk PRIMARY KEY (accommodation_key) RELY""",
)
def dim_accommodation():
    return spark.sql(f"""
        WITH booked AS (
          SELECT h.value:hotelId::string AS id, max(h.value:hotelName::string) AS name,
                 max(upper(h.value:country::string)) AS country, max(initcap(h.value:destinationName::string)) AS dest
          FROM {S}.bookings b, LATERAL variant_explode(b.payload:hotelservice) h
          WHERE h.value:hotelId IS NOT NULL GROUP BY 1)
        SELECT k.id, coalesce(d.name, k.name), d.accommodation_type, d.category, d.chain,
               coalesce(d.destination_name, k.dest), k.country, d.latitude, d.longitude, d.rating_score_10,
               d.rating_reviews, d.accommodation_id IS NOT NULL
        FROM booked k LEFT JOIN {S}.accommodation_details d ON d.accommodation_id = k.id
        UNION ALL
        SELECT '{UNKNOWN}', 'Unknown', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, false""")


# ---------------------------------------------------------------- facts -------------------------------------------
@dp.materialized_view(
    name=f"{G}.fact_booking",
    comment="One booking, current state; amounts in the booking currency.",
    cluster_by=["created_date_key"],
    schema="""booking_ref STRING NOT NULL, agency_key STRING NOT NULL, user_key STRING NOT NULL,
              idea_key STRING NOT NULL, created_date_key INT, start_date_key INT, end_date_key INT, status STRING,
              is_active BOOLEAN, is_cancelled BOOLEAN, trip_type STRING, sales_channel STRING,
              source_market_code STRING, source_market_name STRING, currency STRING, pax INT, nights INT,
              destination_count INT, total_price DECIMAL(18,2), net_provider DECIMAL(18,2),
              operator_fee DECIMAL(18,2), operator_management_fee DECIMAL(18,2), gross_margin DECIMAL(18,2),
              paid_amount DECIMAL(18,2), deferred_amount DECIMAL(18,2), pending_amount DECIMAL(18,2),
              service_count INT, created_at TIMESTAMP, last_update_at TIMESTAMP,
              CONSTRAINT fact_booking_pk PRIMARY KEY (booking_ref) RELY,
              CONSTRAINT fact_booking_agency_fk FOREIGN KEY (agency_key) REFERENCES {G}.dim_agency,
              CONSTRAINT fact_booking_user_fk FOREIGN KEY (user_key) REFERENCES {G}.dim_user,
              CONSTRAINT fact_booking_idea_fk FOREIGN KEY (idea_key) REFERENCES {G}.dim_travel_idea,
              CONSTRAINT fact_booking_created_fk FOREIGN KEY (created_date_key) REFERENCES {G}.dim_date""".replace(
        "{G}", G
    ),
)
def fact_booking():
    return spark.sql(f"""
        SELECT b.booking_ref, coalesce(b.agency_id, '{UNKNOWN}') AS agency_key,
               CASE WHEN b.username IS NOT NULL AND b.agency_id IS NOT NULL
                    THEN b.agency_id || ':' || b.username ELSE '{UNKNOWN}' END AS user_key,
               coalesce(nullif(b.idea_id, ''), '{UNKNOWN}') AS idea_key,
               {date_key("b.created_at")} AS created_date_key, {date_key("b.start_date")} AS start_date_key,
               {date_key("b.end_date")} AS end_date_key, b.status, b.status IN {ACTIVE} AS is_active,
               b.status = 'CANCELED' AS is_cancelled, b.trip_type, b.sales_channel,
               b.payload:sourceMarket.id::string AS source_market_code,
               b.payload:sourceMarket.name::string AS source_market_name, b.currency, b.pax,
               b.payload:nightsCount::int AS nights, b.payload:destinationCount::int AS destination_count,
               b.total_price, b.net_provider, b.operator_fee, b.operator_management_fee,
               CAST(b.total_price - b.net_provider AS DECIMAL(18,2)) AS gross_margin, b.paid_amount,
               b.deferred_amount, CAST(coalesce(p.pending, 0) AS DECIMAL(18,2)) AS pending_amount,
               CAST(coalesce(n.service_count, 0) AS INT) AS service_count, b.created_at, b.last_update_at
        FROM {S}.bookings b
        LEFT JOIN (SELECT booking_ref, count(*) AS service_count FROM {S}.booking_services GROUP BY 1) n
          ON n.booking_ref = b.booking_ref
        LEFT JOIN (SELECT booking_ref, sum(amount_to_pay) AS pending FROM {S}.pending_payments GROUP BY 1) p
          ON p.booking_ref = b.booking_ref""")


def _service_destination_sql() -> str:
    parts = [
        f"""SELECT b.booking_ref, '{t}' AS service_type, x.value:id::string AS id, x.value:country::string AS country,
                   coalesce(x.value:destinationName::string, x.value:locationName::string) AS dname
            FROM {S}.bookings b, LATERAL variant_explode(b.payload:{t}) x"""
        for t in SERVICE_ARRAYS
    ] + [
        f"""SELECT b.booking_ref, 'manual_{k}', x.value:id::string, x.value:country::string, x.value:destination::string
            FROM {S}.bookings b, LATERAL variant_explode(b.payload:manualServices.{k}) x"""
        for k in MANUAL_KEYS
    ]
    return f"""SELECT booking_ref, service_type, id, max(upper(country)) AS country,
                      max(initcap(dname)) AS destination_name
               FROM ({" UNION ALL ".join(parts)}) WHERE id IS NOT NULL GROUP BY 1, 2, 3"""


@dp.materialized_view(
    name=f"{G}.fact_booking_service",
    comment="One service of a booking; sale and cost amounts, cost also in the booking currency.",
    cluster_by=["booking_created_date_key"],
    schema="""service_key STRING NOT NULL, booking_ref STRING NOT NULL, service_id STRING, service_type STRING NOT NULL,
              supplier_key STRING NOT NULL, agency_key STRING NOT NULL, accommodation_key STRING NOT NULL,
              booking_created_date_key INT, service_start_date_key INT, status STRING, is_active BOOLEAN,
              is_manual BOOLEAN, destination_country STRING, destination_name STRING, sale_currency STRING,
              sale_amount DECIMAL(18,2), cost_currency STRING, cost_amount DECIMAL(18,2), booking_currency STRING,
              cost_in_booking_currency DECIMAL(18,2), margin_in_booking_currency DECIMAL(18,2), start_date DATE,
              end_date DATE,
              CONSTRAINT fact_booking_service_pk PRIMARY KEY (service_key) RELY,
              CONSTRAINT fact_booking_service_booking_fk FOREIGN KEY (booking_ref) REFERENCES {G}.fact_booking,
              CONSTRAINT fact_booking_service_supplier_fk FOREIGN KEY (supplier_key) REFERENCES {G}.dim_supplier,
              CONSTRAINT fact_booking_service_agency_fk FOREIGN KEY (agency_key) REFERENCES {G}.dim_agency,
              CONSTRAINT fact_booking_service_type_fk FOREIGN KEY (service_type) REFERENCES {G}.dim_service_type,
              CONSTRAINT fact_booking_service_accommodation_fk FOREIGN KEY (accommodation_key)
                REFERENCES {G}.dim_accommodation""".replace("{G}", G),
)
def fact_booking_service():
    return spark.sql(f"""
        WITH dest AS ({_service_destination_sql()})
        SELECT x.booking_ref || '|' || x.service_type || '|' || x.service_id AS service_key,
               x.booking_ref, x.service_id, x.service_type, {supplier_key_sql("x")} AS supplier_key,
               coalesce(b.agency_id, '{UNKNOWN}') AS agency_key,
               CASE WHEN x.service_type IN ('hotelservice') AND x.accommodation_id IS NOT NULL
                    THEN x.accommodation_id ELSE '{UNKNOWN}' END AS accommodation_key,
               {date_key("b.created_at")} AS booking_created_date_key,
               {date_key("x.start_date")} AS service_start_date_key,
               x.status, x.status IN {ACTIVE} AS is_active, x.is_manual,
               d.country AS destination_country, d.destination_name,
               x.sale_currency, x.sale_amount, x.cost_currency, x.cost_amount,
               b.currency AS booking_currency, x.cost_in_sale_currency AS cost_in_booking_currency,
               CAST(CASE WHEN x.sale_currency = b.currency THEN x.sale_amount - x.cost_in_sale_currency END
                    AS DECIMAL(18,2)) AS margin_in_booking_currency,
               x.start_date, x.end_date
        FROM {S}.booking_services x
        JOIN {S}.bookings b ON b.booking_ref = x.booking_ref
        LEFT JOIN dest d ON d.booking_ref = x.booking_ref AND d.service_type = x.service_type AND d.id = x.service_id""")


@dp.materialized_view(
    name=f"{G}.fact_booking_payment",
    comment="One payment recorded on a booking (first, second, additional, manual), in the payment currency.",
    cluster_by_auto=True,
)
def fact_booking_payment():
    return spark.sql(f"""
        SELECT p.booking_ref || '|' || p.payment_kind || '|' || coalesce(p.payment_id, '') AS payment_key,
               p.booking_ref, coalesce(b.agency_id, '{UNKNOWN}') AS agency_key, p.payment_kind, p.payment_id,
               p.payment_system, p.payment_type, p.currency, p.amount, p.deferred_amount,
               {date_key("p.paid_at")} AS paid_date_key, {date_key("p.deferred_limit_date")} AS deferred_limit_date_key,
               p.refund_status
        FROM {S}.booking_payments p JOIN {S}.bookings b ON b.booking_ref = p.booking_ref""")


@dp.materialized_view(
    name=f"{G}.fact_pending_payment",
    comment="One payment still due according to the latest pending-payments snapshot; days_overdue > 0 is late.",
    cluster_by_auto=True,
)
def fact_pending_payment():
    return spark.sql(f"""
        SELECT p.payment_id, p.booking_ref, coalesce(b.agency_id, '{UNKNOWN}') AS agency_key, p.payment_number,
               p.currency, p.amount_to_pay, p.deferred_amount, CAST(p.deferred_limit_at AS DATE) AS due_date,
               {date_key("p.deferred_limit_at")} AS due_date_key,
               greatest(datediff(current_date(), CAST(p.deferred_limit_at AS DATE)), 0) AS days_overdue,
               CASE WHEN p.deferred_limit_at IS NULL THEN 'No due date'
                    WHEN p.deferred_limit_at >= current_date() THEN 'Not due'
                    WHEN datediff(current_date(), p.deferred_limit_at) <= 30 THEN '1-30 days'
                    WHEN datediff(current_date(), p.deferred_limit_at) <= 60 THEN '31-60 days'
                    WHEN datediff(current_date(), p.deferred_limit_at) <= 90 THEN '61-90 days'
                    ELSE 'Over 90 days' END AS ageing_bucket,
               b.status AS booking_status
        FROM {S}.pending_payments p LEFT JOIN {S}.bookings b ON b.booking_ref = p.booking_ref""")


@dp.materialized_view(
    name=f"{G}.fact_booking_status_change",
    comment="One tracked change of a booking (status, amounts, dates, pax) with the previous status.",
    cluster_by_auto=True,
)
def fact_booking_status_change():
    return spark.sql(f"""
        SELECT booking_ref, __START_AT AS valid_from, __END_AT AS valid_to, {date_key("__START_AT")} AS change_date_key,
               lag(status) OVER (PARTITION BY booking_ref ORDER BY __START_AT) AS previous_status, status,
               coalesce(agency_id, '{UNKNOWN}') AS agency_key, currency, total_price,
               total_price - lag(total_price) OVER (PARTITION BY booking_ref ORDER BY __START_AT) AS total_price_change,
               paid_amount, pax, start_date, end_date
        FROM {S}.bookings_history""")


@dp.materialized_view(
    name=f"{G}.fact_purchase_attempt",
    comment="One checkout started on the microsite; is_converted when a booking carries the same idea id.",
    cluster_by_auto=True,
)
def fact_purchase_attempt():
    return spark.sql(f"""
        SELECT a.attempt_id, {date_key("a.created_date")} AS created_date_key, a.trip_type, a.currency,
               a.total_price, a.destination_count, a.hotel_nights, a.adults,
               b.booking_ref AS converted_booking_ref, b.booking_ref IS NOT NULL AS is_converted
        FROM {S}.purchase_attempts a
        LEFT JOIN (SELECT idea_id, min(booking_ref) AS booking_ref FROM {S}.bookings GROUP BY 1) b
          ON b.idea_id = a.attempt_id""")


# ---------------------------------------------------------------- one big table -----------------------------------
@dp.materialized_view(
    name=f"{G}.obt_booking_service",
    comment="fact_booking_service joined with every dimension and booking attribute, for ad-hoc SQL and Genie.",
    cluster_by=["booking_created_month"],
)
def obt_booking_service():
    return spark.sql(f"""
        SELECT f.*, fb.status AS booking_status, fb.is_active AS booking_is_active, fb.trip_type, fb.sales_channel,
               fb.source_market_code, fb.source_market_name, fb.pax, fb.total_price AS booking_total_price,
               cd.date AS booking_created_date, cd.year_month AS booking_created_month,
               sd.year_month AS service_start_month,
               ag.agency_name, ag.country AS agency_country, ag.is_umbrella,
               sp.supplier_name, sp.supplier_source, sp.country AS supplier_country,
               st.service_category,
               ac.accommodation_name, ac.category AS accommodation_category, ac.chain AS accommodation_chain,
               ac.rating_score_10 AS accommodation_rating
        FROM {G}.fact_booking_service f
        JOIN {G}.fact_booking fb ON fb.booking_ref = f.booking_ref
        LEFT JOIN {G}.dim_date cd ON cd.date_key = f.booking_created_date_key
        LEFT JOIN {G}.dim_date sd ON sd.date_key = f.service_start_date_key
        LEFT JOIN {G}.dim_agency ag ON ag.agency_key = f.agency_key
        LEFT JOIN {G}.dim_supplier sp ON sp.supplier_key = f.supplier_key
        LEFT JOIN {G}.dim_service_type st ON st.service_type = f.service_type
        LEFT JOIN {G}.dim_accommodation ac ON ac.accommodation_key = f.accommodation_key""")
