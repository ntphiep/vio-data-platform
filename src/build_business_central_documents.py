"""Task build_business_central_documents - posted sales and purchase documents in the shape the implementation partner
specifies (VIO_Data warhouse_Integration mapping fields-V1.2.xlsx, sheets API_Sales_T01 and API_Purchases_T01), catalog
<catalog>:

business_central.booking_versions    one row per DWH version of a booking: a landing in bronze.tc_bookings whose
                                     fields sent to Business Central differ from the version before; built
                                     incrementally from the bronze rows landed since the last build (MERGE)
business_central.sales_documents     one row per sales document a version produces (rebuilt every run)
business_central.purchase_documents  one row per purchase document a version produces for one service (rebuilt)
ref.business_central_code_map        item, salesperson and business unit codes owned by Vio Finance, created empty
ops.dq_results                       business_central_ checks of these tables

The partner specification has no orders: every version of a booking produces posted documents.

    change of a booking (status BOOKED, RQ or CANCELED)       sales documents
    first version with a positive total (NEW)                 Invoice: deposit line, balance line, comment line
    total up or down (INCREASE, DECREASE)                     Invoice or Credit Memo for the difference
    status becomes CANCELED (CANCEL)                          Credit Memo for the total invoiced, Invoice for the fee kept
    customer or currency changes (REISSUE)                    Credit Memo for the total invoiced, new Invoice

Purchase documents follow the same rules per service (status BOOKED creates, CANCELED or removed reduces, a change of
vendor or currency reissues). The signed amounts of the documents of a booking or service always add up to its
current total; blocking checks test that, the keys and the invoice numbers. The tables are not read by
local/push_to_business_central.py, so they take no part in its write-audit-publish: a blocking failure fails this task.

Versions (DWH version, the IntegrationKey suffix): the bronze rows of a booking are read in landing order (ingested_at,
then lastUpdateDate, then file). A row whose lastUpdateDate is older than one landed before it is ignored, so a
late or replayed page never brings back an older version. A row starts a new version when the hash of its fields sent to Business Central
differs from the last row that was not ignored. The same rule runs on the whole of bronze (job parameter rebuild=true)
and on the rows landed since the last build, so an incremental build and a rebuild give the same versions.
"""

import argparse
import hashlib

SCHEMA = "business_central"
TASK = "build_business_central_documents"
# Same lists as src/build_bronze_silver_gold/silver.py (tests/test_sql_parity.py).
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
# Same values as src/build_business_central_tables.py (tests/test_sql_parity.py): the service reference of the
# partner (booking reference, service type, origin API or manual, line id) is the purchase_key of that task.
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
PROVIDER_VENDOR_NUMBER = "left('PV' || regexp_replace(upper(s.provider), '[^A-Z0-9]', ''), 20)"
COUNTRY_SUFFIX = "', *([A-Z]{2}) *$'"
ACTIVE = "('BOOKED', 'RQ')"
# Date format of the invoice description in the Xero exports of the AR Masterfile: '1: Cebu, 2: El Nido commencing 27/12/2026'.
DESCRIPTION_DATE = "dd/M/yyyy"
# Payment system of Travel Compositor whose payments the partner calls Paid Amount (API_Sales_T01 row 45).
TMT_SYSTEM = "TRUST_MY_TRAVEL"
TMT_TEXT = "Trust My Travel"

# One service of a version: (field, type). Fields first that order sort_array, so the array is stable for the hash.
SERVICE_FIELDS = [
    ("service_type", "STRING"),
    ("service_id", "STRING"),
    ("purchase_key", "STRING"),
    ("is_manual", "BOOLEAN"),
    ("status", "STRING"),
    ("name", "STRING"),
    ("supplier_id", "STRING"),
    ("supplier_name", "STRING"),
    ("provider", "STRING"),
    ("provider_reference", "STRING"),
    ("supplier_confirmation", "STRING"),
    ("start_date", "DATE"),
    ("end_date", "DATE"),
    ("cancellation_date", "DATE"),
    ("last_update_at", "TIMESTAMP"),
    ("sale_currency", "STRING"),
    ("sale_amount", "DECIMAL(18,2)"),
    ("cost_currency", "STRING"),
    ("cost_amount", "DECIMAL(18,2)"),
    ("amendment_sale_amount", "DECIMAL(18,2)"),
    ("amendment_cost_amount", "DECIMAL(18,2)"),
    ("amendment_cost_other_currency", "INT"),
    ("exchange_rate_microsite", "DECIMAL(24,12)"),
    ("destination_country", "STRING"),
    ("destination_name", "STRING"),
]
SERVICE_TYPE = "ARRAY<STRUCT<" + ", ".join(f"{n}: {t}" for n, t in SERVICE_FIELDS) + ">>"
SALES_LINE_TYPE = (
    "ARRAY<STRUCT<rec_id: STRING, line_type: STRING, type: STRING, no: STRING, description: STRING, "
    "quantity: INT, unit_price: DECIMAL(18,2), line_due_date: DATE, comment: STRING>>"
)
PURCHASE_LINE_TYPE = (
    "ARRAY<STRUCT<type: STRING, no: STRING, description: STRING, description_2: STRING, quantity: INT, "
    "direct_unit_cost: DECIMAL(18,2)>>"
)

# business_central.booking_versions: (column, type, comment). The columns after last_seen_ingested_at are the fields
# of the booking; content_hash covers them.
VERSION_COLUMNS = [
    ("booking_ref", "STRING", "Travel Compositor booking reference (bookingReference)."),
    (
        "version",
        "INT",
        "DWH version of the booking: 1 for its first landing, +1 when a field sent to Business Central changes.",
    ),
    ("content_hash", "STRING", "SHA-256 of the booking fields below; a new version starts when it changes."),
    ("last_update_at", "TIMESTAMP", "lastUpdateDate of the landing that started this version."),
    ("ingested_at", "TIMESTAMP", "Landing time in bronze.tc_bookings of the row that started this version."),
    ("run_id", "STRING", "Extract run (ops.ingest_runs.run_id) that landed the row that started this version."),
    (
        "max_seen_last_update_at",
        "TIMESTAMP",
        "Latest lastUpdateDate landed while this version was current; a landing older than it is ignored.",
    ),
    (
        "last_seen_ingested_at",
        "TIMESTAMP",
        "Latest landing time read while this version was current; the next build reads bronze after the maximum.",
    ),
    ("tc_booking_id", "STRING", "Travel Compositor internal booking id; RecID of the tour lines and the comment line."),
    ("status", "STRING", "Travel Compositor booking status: BOOKED, RQ (on request), CANCELED ..."),
    ("created_at", "TIMESTAMP", "Creation time of the booking (creationDate), as Travel Compositor sends it."),
    ("start_date", "DATE", "First day of the trip."),
    ("end_date", "DATE", "Last day of the trip."),
    ("pax", "INT", "Passengers: adults + children + infants."),
    ("trip_type", "STRING", "Travel Compositor trip type."),
    ("sales_channel", "STRING", "Travel Compositor sales channel."),
    ("microsite", "STRING", "Travel Compositor microsite of the booking user."),
    ("agency_id", "STRING", "Travel Compositor agency id (user.agency.externalId)."),
    (
        "agency_booking_reference",
        "STRING",
        "agencyBookingReference: the real customer when the agency is an umbrella agency (ref.customer_map).",
    ),
    ("referral_id", "STRING", "referralId: the agent reference, Your Reference of the sales documents."),
    ("main_booking_ref", "STRING", "bookingAssociatedTo, else the booking itself: dimension MAIN BOOKING."),
    ("username", "STRING", "Microsite username of the booking creator. Personal data."),
    ("user_email", "STRING", "E-mail of the booking creator (user.email). Personal data."),
    ("currency", "STRING", "ISO 4217 currency of every amount of the booking (microsite price)."),
    (
        "total_price",
        "DECIMAL(18,2)",
        "Booking total (pricebreakdown.totalPrice): sale of every service, cancelled ones with a kept fee included, plus amendments and the agency management fee.",
    ),
    (
        "agency_management_fee",
        "DECIMAL(18,2)",
        "Agency management fee of the booking (pricebreakdown.agencyManagementFee), in currency.",
    ),
    ("payment_amount", "DECIMAL(18,2)", "Amount of the first payment (payment.amount), in currency."),
    ("deferred_amount", "DECIMAL(18,2)", "Amount whose payment is deferred (payment.deferredAmount), in currency."),
    (
        "deferred_limit_date",
        "DATE",
        "Last day to pay the deferred amount (payment.deferredLimitDate): due date of the balance line.",
    ),
    ("payment_system", "STRING", "Payment system of the first payment: DEPOSIT, REPAYD, TRUST_MY_TRAVEL ..."),
    ("second_payment_amount", "DECIMAL(18,2)", "Amount of the second payment, in currency."),
    ("second_payment_system", "STRING", "Payment system of the second payment."),
    (
        "destinations",
        "STRING",
        "Hotel stays in check-in order, repeated stays merged: '1: Tokyo, 2: Kyoto'. NULL without an active hotel.",
    ),
    ("destination_country", "STRING", "Country of the first active service that has one: dimension DESTINATION."),
    ("cancellation_date", "DATE", "Latest cancelationDate of the services when the booking is CANCELED, else NULL."),
    (
        "service_summary",
        "STRING",
        "Active services in start order, one per line: name and start date, no prices (comment line, up to 5000 characters).",
    ),
    (
        "services",
        SERVICE_TYPE,
        "Every service of the version (API arrays and manualServices) with its amounts; purchase_key is the partner service reference. NULL when the booking has no service.",
    ),
]
SALES_COLUMNS = [
    (
        "integration_key",
        "STRING",
        "IntegrationKey of API_Sales_T01: booking reference and DWH version, as booking_ref-version.",
    ),
    (
        "dwh_version",
        "INT",
        "DWH version of the booking (booking_versions.version), for a partner that keys on the booking reference alone.",
    ),
    (
        "doc_seq",
        "INT",
        "Order of the document within its version: 1, then 2 for the invoice that follows a credit memo.",
    ),
    (
        "change_type",
        "STRING",
        "NEW, INCREASE, DECREASE, CANCEL (status became CANCELED) or REISSUE (customer or currency changed).",
    ),
    (
        "document_role",
        "STRING",
        "FULL (whole booking), ADJUSTMENT (difference), REVERSAL (credit of the total invoiced) or CANCELLATION_FEE.",
    ),
    (
        "document_type",
        "STRING",
        "Invoice or Credit Memo (row 15); the two documents of a cancellation are the option Cancel of the partner.",
    ),
    ("external_document_no", "STRING", "External Document No.: the booking reference."),
    (
        "sell_to_customer_no",
        "STRING",
        "Customer No.: AG<agency id>, or ref.customer_map for an umbrella agency; for a reversal the customer invoiced before.",
    ),
    (
        "hold_reason",
        "STRING",
        "CUSTOMER_UNMAPPED or AGENCY_NOT_IN_SNAPSHOT when the customer is not known yet; NULL when the document can be sent.",
    ),
    (
        "order_date",
        "DATE",
        "Order Date: booking creation date as Travel Compositor sends it; the time zone rule is open (scenario DT-01).",
    ),
    (
        "document_date",
        "DATE",
        "Document Date: creation date for a document of the first version, else the date of lastUpdateDate of the version.",
    ),
    (
        "posting_date",
        "DATE",
        "Posting Date (row 22): creation date for a document of the first version, cancellation date for a cancellation, else the document date.",
    ),
    ("due_date", "DATE", "Due Date: due date of the balance line of a full invoice (scenario DT-02); NULL otherwise."),
    ("service_start_date", "DATE", "Service Start Date: first day of the trip."),
    ("service_end_date", "DATE", "Service End Date: last day of the trip."),
    ("currency_code", "STRING", "Currency of every amount of the document: the Travel Compositor booking currency."),
    (
        "agent_currency_code",
        "STRING",
        "Agent invoice currency (row 27). NULL: Travel Compositor holds no agent currency; Finance is to supply it.",
    ),
    (
        "currency_factor",
        "DECIMAL(24,12)",
        "Exchange rate of the booking (row 28). NULL: Travel Compositor holds no agent rate; Business Central uses its daily rate.",
    ),
    ("your_reference", "STRING", "Your Reference: referralId."),
    ("booking_status", "STRING", "Booking Status: BOOKED, RQ or CANCELED."),
    ("cancellation_date", "DATE", "Cancellation Date: latest service cancellation date when the booking is CANCELED."),
    (
        "tcode",
        "STRING",
        "TCODE, Credit or Refund on a credit memo (row 32). NULL: Travel Compositor holds no refund type.",
    ),
    ("last_updated_at", "TIMESTAMP", "Last Updated: lastUpdateDate of the version."),
    ("total_passengers", "INT", "Total Passengers: adults + children + infants."),
    ("trip_type", "STRING", "Trip Type."),
    (
        "destinations",
        "STRING",
        "Destinations: hotel stays in check-in order ('1: Tokyo, 2: Kyoto'); NULL without an active hotel.",
    ),
    (
        "destination_country",
        "STRING",
        "Destination Country, dimension DESTINATION: country of the first active service.",
    ),
    ("sales_channel", "STRING", "Sales Channel, dimension SALES CHANNEL."),
    ("microsite", "STRING", "Brand / Microsite, dimension BRAND: Travel Compositor microsite."),
    ("main_booking", "STRING", "Main Booking, dimension MAIN BOOKING: bookingAssociatedTo, else the booking itself."),
    (
        "salesperson_code",
        "STRING",
        "Salesperson Code of the booking creator from ref.business_central_code_map (SALESPERSON); NULL until Finance maps the user.",
    ),
    ("sell_to_email", "STRING", "E-Mail of the booking creator. Personal data."),
    (
        "booking_total_amount",
        "DECIMAL(18,2)",
        "Booking Total Amount: booking total after the change, in currency_code.",
    ),
    ("agency_management_fee", "DECIMAL(18,2)", "Agency Management Fee of the booking."),
    (
        "paid_amount",
        "DECIMAL(18,2)",
        "Paid Amount: payments of the booking through Trust My Travel (payment system TRUST_MY_TRAVEL); NULL when none.",
    ),
    ("payment_method", "STRING", "Payment Method text: Trust My Travel when paid_amount is set."),
    (
        "invoice_no",
        "STRING",
        "No. of an invoice, YYDDDNNN: year, day of the year and sequence of the posting date. NULL on a credit memo: their numbering is open (row 47).",
    ),
    (
        "document_amount",
        "DECIMAL(18,2)",
        "Sum of the lines (quantity x unit price), positive; a credit memo reduces the amount invoiced.",
    ),
    (
        "sales_lines",
        SALES_LINE_TYPE,
        "Sales lines: Deposit and Balance tour lines (item, unit price, line due date) and a comment line with the service summary (type NULL, price 0).",
    ),
]
PURCHASE_COLUMNS = [
    (
        "integration_key",
        "STRING",
        "IntegrationKey of API_Purchases_T01: service reference and DWH version, as purchase_key-version.",
    ),
    ("dwh_version", "INT", "DWH version of the booking that carried the change (booking_versions.version)."),
    (
        "doc_seq",
        "INT",
        "Order of the document within its version: 1, then 2 for the invoice that follows a credit memo.",
    ),
    (
        "change_type",
        "STRING",
        "NEW, INCREASE, DECREASE, CANCEL (service cancelled or removed) or REISSUE (vendor or currency changed).",
    ),
    (
        "document_role",
        "STRING",
        "FULL (whole cost), ADJUSTMENT (difference) or REVERSAL (credit of the cost invoiced).",
    ),
    ("document_type", "STRING", "Invoice or Credit Memo."),
    ("purchase_type", "STRING", "Purchase Type: Trading for every service (Opex comes from another source)."),
    (
        "your_reference",
        "STRING",
        "Service Reference: service id and type code (the line id contains the booking reference); purchase_key of purchase_invoices.",
    ),
    ("booking_reference", "STRING", "Booking Reference, also the dimension BOOKING."),
    (
        "buy_from_vendor_no",
        "STRING",
        "Vendor No.: SP<supplier id>, PV<provider code> or ref.vendor_map; for a reversal the vendor invoiced before.",
    ),
    (
        "hold_reason",
        "STRING",
        "VENDOR_UNMAPPED or SUPPLIER_NOT_IN_SNAPSHOT when the vendor is not known yet; NULL when the document can be sent.",
    ),
    (
        "vendor_order_no",
        "STRING",
        "Vendor Order No.: supplier confirmation number, else the provider booking reference.",
    ),
    (
        "vendor_invoice_no",
        "STRING",
        "Vendor Invoice No. (row 22). NULL: Travel Compositor holds no supplier invoice number; the source is open.",
    ),
    (
        "vendor_cr_memo_no",
        "STRING",
        "Vendor Cr. Memo No. (row 23). NULL: Travel Compositor holds no supplier credit note number.",
    ),
    ("order_date", "DATE", "Order Date: booking creation date."),
    (
        "document_date",
        "DATE",
        "Document Date: booking creation date for a service of the first version, else the date of lastUpdateDate of the booking version that carried the change.",
    ),
    ("posting_date", "DATE", "Posting Date: as document_date, but the service cancellation date for a cancellation."),
    ("service_start_date", "DATE", "Service Start Date."),
    ("service_end_date", "DATE", "Service End Date."),
    ("currency_code", "STRING", "Currency Code: the currency the supplier charges (providerNetPrice)."),
    (
        "currency_factor",
        "DECIMAL(24,12)",
        "Currency Factor: exchangeRateMicrosite of the service; its meaning is not confirmed by Travel Compositor.",
    ),
    ("service_status", "STRING", "Service Status: BOOKED, CANCELED, or REMOVED when the service left the booking."),
    (
        "tcode",
        "STRING",
        "TCODE, Credit or Refund on a credit memo (row 35). NULL: Travel Compositor holds no refund type.",
    ),
    ("last_updated_at", "TIMESTAMP", "Last Updated: lastUpdateDate of the service, else of the booking."),
    (
        "business_unit",
        "STRING",
        "Business Unit dimension of the booking creator from ref.business_central_code_map (BUSINESS_UNIT); NULL: Travel Compositor holds no business unit.",
    ),
    ("main_booking", "STRING", "Main Booking, dimension MAIN BOOKING."),
    (
        "purchaser_code",
        "STRING",
        "Purchaser Code of the booking creator from ref.business_central_code_map (SALESPERSON).",
    ),
    (
        "service_total_cost",
        "DECIMAL(18,2)",
        "Service Total Cost: supplier cost after the change plus the amendment costs in that currency; 0 when cancelled at no cost or removed.",
    ),
    ("internal_no", "STRING", "No. of a purchase invoice (row 41). NULL: the number format is open."),
    ("document_amount", "DECIMAL(18,2)", "Sum of the lines (quantity x direct unit cost), positive."),
    (
        "purchase_lines",
        PURCHASE_LINE_TYPE,
        "Purchase line: item of the service type, description '<service reference> <name>', quantity 1, direct unit cost.",
    ),
]
CODE_MAP_COLUMNS = [
    ("map_kind", "STRING", "ITEM, SALESPERSON or BUSINESS_UNIT."),
    (
        "source_key",
        "STRING",
        "ITEM: SALES (tour item), CANCELLATION (cancellation item) or a service type such as hotelservice. SALESPERSON and BUSINESS_UNIT: lower-case username of the booking creator (personal data).",
    ),
    (
        "bc_code",
        "STRING",
        "Business Central code: item no., salesperson/purchaser code or BUSINESS UNIT dimension value.",
    ),
    ("display_name", "STRING", "Name of the code."),
    ("approved_by", "STRING", "Who in Vio Finance approved the row, or LAB SEED for rows that only serve the lab."),
    ("note", "STRING", "Free text."),
]
TABLE_COMMENTS = {
    "booking_versions": "One row per DWH version of a booking, read from bronze.tc_bookings: a new version starts when a field sent to Business Central changes. History starts with the first landing of the lab.",
    "sales_documents": "Posted sales documents of the implementation partner specification API_Sales_T01: Invoice or Credit Memo per booking version, with deposit, balance and comment lines.",
    "purchase_documents": "Posted purchase documents of the implementation partner specification API_Purchases_T01: Invoice or Credit Memo per service and booking version.",
}
CODE_MAP_COMMENT = (
    "Business Central codes owned by Vio Finance that Travel Compositor does not hold: item per service type, "
    "salesperson/purchaser and business unit per booking creator. Read by build_business_central_documents."
)


def ddl(columns: list[tuple[str, str, str]]) -> str:
    """Column list of CREATE TABLE with a COMMENT on every column."""
    return ",\n".join(f"{n} {t} COMMENT '{c.replace(chr(39), '')}'" for n, t, c in columns)


def type_code_sql(col: str) -> str:
    """CASE expression for TYPE_CODES; an unknown manual type falls back to 'M' and its first two letters."""
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in TYPE_CODES.items())
    return f"CASE {col} {whens} ELSE 'M' || upper(left(replace({col}, 'manual_', ''), 2)) END"


def clean_text(expr: str) -> str:
    """HTML tags removed and white space collapsed, as in the sales line descriptions of build_business_central_tables."""
    return f"trim(regexp_replace(regexp_replace(coalesce({expr}, ''), '<[^>]*>', ' '), '\\\\s+', ' '))"


def invoice_number(day, sequence: int) -> str:
    """YYDDDNNN of the AR Masterfile: year, day of the year, sequence of the day. Mirrors INVOICE_NUMBER_SQL."""
    return f"{day:%y}{day.timetuple().tm_yday:03d}{sequence:03d}"


def invoice_number_sql(day: str, sequence: str) -> str:
    """SQL of invoice_number(); a sequence above 999 gives 9 digits, which the blocking check rejects."""
    return (
        f"date_format({day}, 'yy') || lpad(CAST(dayofyear({day}) AS STRING), 3, '0') || "
        f"CASE WHEN {sequence} > 999 THEN CAST({sequence} AS STRING) ELSE lpad(CAST({sequence} AS STRING), 3, '0') END"
    )


def service_struct_sql() -> str:
    """named_struct of SERVICE_FIELDS from one service VARIANT `s` of a landed row `l`; types cast to SERVICE_TYPE."""
    cost_currency = "coalesce(s:pricebreakdown.providerNetPrice.currency::string, s:pricebreakdown.netProviderPrice.currency::string)"
    cost_amount_of = (
        "coalesce(variant_get(a, '$.priceBreakDown.providerNetPrice.amount', 'decimal(18,2)'), "
        "variant_get(a, '$.priceBreakDown.netProviderPrice.amount', 'decimal(18,2)'))"
    )
    cost_currency_of = (
        "coalesce(variant_get(a, '$.priceBreakDown.providerNetPrice.currency', 'string'), "
        "variant_get(a, '$.priceBreakDown.netProviderPrice.currency', 'string'))"
    )
    amendments = "cast(s:amendments AS ARRAY<VARIANT>)"
    zero = "CAST(0 AS DECIMAL(18,2))"
    exprs = {
        "service_type": "service_type",
        "service_id": "coalesce(s:id::string, l.booking_ref || '-' || service_type)",
        "purchase_key": f"coalesce(s:id::string, l.booking_ref || '-' || service_type) || '-' || {type_code_sql('service_type')}",
        "is_manual": "is_manual",
        "status": "s:status::string",
        "name": "coalesce(s:hotelName::string, s:name::string, s:airlineName::string, s:description::string)",
        "supplier_id": "s:supplierId::string",
        "supplier_name": "coalesce(s:supplierName::string, s:providerName::string)",
        "provider": "s:provider::string",
        "provider_reference": "coalesce(s:providerBookingReference::string, s:providerTransferCode::string)",
        # the confirmation number of the supplier; a bookingReference that starts with EXA is Vio's own reference
        "supplier_confirmation": "CASE WHEN nullif(trim(s:bookingReference::string), '') NOT LIKE 'EXA-%' "
        "THEN trim(s:bookingReference::string) END",
        "start_date": "to_date(left(s:startDate::string, 10))",
        "end_date": "to_date(left(s:endDate::string, 10))",
        "cancellation_date": "to_date(left(s:cancelationDate::string, 10))",
        "last_update_at": "try_cast(s:lastUpdateDate::string AS TIMESTAMP)",
        "sale_currency": "s:pricebreakdown.totalPrice.microsite.currency::string",
        "sale_amount": "s:pricebreakdown.totalPrice.microsite.amount::decimal(18,2)",
        "cost_currency": cost_currency,
        "cost_amount": "coalesce(s:pricebreakdown.providerNetPrice.amount::decimal(18,2), "
        "s:pricebreakdown.netProviderPrice.amount::decimal(18,2))",
        "amendment_sale_amount": f"coalesce(aggregate({amendments}, {zero}, (acc, a) -> CAST(acc + coalesce("
        f"variant_get(a, '$.priceBreakDown.totalPrice.microsite.amount', 'decimal(18,2)'), 0) AS DECIMAL(18,2))), {zero})",
        "amendment_cost_amount": f"coalesce(aggregate({amendments}, {zero}, (acc, a) -> CAST(acc + coalesce("
        f"CASE WHEN {cost_currency_of} = {cost_currency} THEN {cost_amount_of} END, 0) AS DECIMAL(18,2))), {zero})",
        "amendment_cost_other_currency": f"coalesce(size(filter({amendments}, a -> {cost_currency_of} <> "
        f"{cost_currency} AND coalesce({cost_amount_of}, 0) <> 0)), 0)",
        "exchange_rate_microsite": "s:pricebreakdown.exchangeRateMicrosite::decimal(24,12)",
        "destination_country": f"coalesce(upper(s:country::string), "
        f"nullif(regexp_extract(s:destination::string, {COUNTRY_SUFFIX}, 1), ''), "
        f"nullif(regexp_extract(s:origin::string, {COUNTRY_SUFFIX}, 1), ''))",
        "destination_name": "initcap(coalesce(s:destinationName::string, s:locationName::string, s:destination::string))",
    }
    if list(exprs) != [n for n, _ in SERVICE_FIELDS]:
        raise ValueError("service_struct_sql must list SERVICE_FIELDS in order")
    return "named_struct(" + ", ".join(f"'{n}', CAST({exprs[n]} AS {t})" for n, t in SERVICE_FIELDS) + ")"


def hashed_services_sql(col: str) -> str:
    """The services of a version without their lastUpdateDate, which is not a change sent to Business Central."""
    fields = ", ".join(f"'{n}', x.{n}" for n, _ in SERVICE_FIELDS if n != "last_update_at")
    return f"transform({col}, x -> named_struct({fields}))"


# The booking fields start at tc_booking_id; content_hash covers them.
HASHED_HEADER = [n for n, _, _ in VERSION_COLUMNS[[n for n, _, _ in VERSION_COLUMNS].index("tc_booking_id") :]]


def versions_sql(c: str, b: str, watermark) -> list[str]:
    """Statements that MERGE the versions of the bronze rows landed after watermark (all rows when None)."""
    landed_filter = f"AND ingested_at > TIMESTAMP'{watermark}'" if watermark is not None else ""
    typed = ", ".join(f"'{t}'" for t in SERVICE_ARRAYS)
    manual = ", ".join(f"'{k}'" for k in MANUAL_KEYS)
    active_hotels = "filter(services, x -> x.service_type IN ('hotelservice', 'manual_hotel') AND x.status IN " + ACTIVE
    active = "filter(services, x -> x.status IN " + ACTIVE
    header_cols = ", ".join(HASHED_HEADER)
    hashed = ", ".join(f"'{n}', {hashed_services_sql('services') if n == 'services' else n}" for n in HASHED_HEADER)
    return [
        # bronze rows landed since the last build, one per booking version read by the extract
        f"""CREATE OR REPLACE TEMP VIEW landed AS
        SELECT r:bookingReference::string AS booking_ref, r:lastUpdateDate::timestamp AS last_update_at, ingested_at,
               source_file, run_id, r
        FROM (SELECT doc:record AS r, ingested_at, source_file, run_id FROM {c}.bronze.tc_bookings
              WHERE doc IS NOT NULL {landed_filter})
        WHERE r:bookingReference::string IS NOT NULL AND r:lastUpdateDate::timestamp IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY r:bookingReference::string, ingested_at, source_file,
                                                r:lastUpdateDate::timestamp ORDER BY CAST(r AS STRING)) = 1""",
        # services of each landed row, two scans (typed arrays, then manualServices) as in silver.booking_services
        f"""CREATE OR REPLACE TEMP VIEW landed_services AS
        SELECT l.booking_ref, l.ingested_at, l.source_file, l.last_update_at,
               sort_array(collect_list({service_struct_sql()})) AS services
        FROM (SELECT l.booking_ref, l.ingested_at, l.source_file, l.last_update_at, t.key AS service_type,
                     false AS is_manual, s.value AS s
              FROM landed l, LATERAL variant_explode(l.r) t, LATERAL variant_explode(t.value) s
              WHERE t.key IN ({typed})
              UNION ALL
              SELECT l.booking_ref, l.ingested_at, l.source_file, l.last_update_at, 'manual_' || t.key, true, s.value
              FROM landed l, LATERAL variant_explode(l.r:manualServices) t, LATERAL variant_explode(t.value) s
              WHERE t.key IN ({manual})) l
        GROUP BY l.booking_ref, l.ingested_at, l.source_file, l.last_update_at""",
        # booking fields of each landed row, derived fields from its services
        f"""CREATE OR REPLACE TEMP VIEW landed_versions AS
        WITH h AS (
          SELECT l.booking_ref, l.last_update_at, l.ingested_at, l.source_file, l.run_id,
                 l.r:id::string AS tc_booking_id, l.r:status::string AS status,
                 l.r:creationDate::timestamp AS created_at, to_date(l.r:startDate::string) AS start_date,
                 to_date(l.r:endDate::string) AS end_date,
                 coalesce(l.r:adultCount::int, 0) + coalesce(l.r:childCount::int, 0) + coalesce(l.r:infantCount::int, 0) AS pax,
                 l.r:tripType::string AS trip_type, l.r:salesChannel::string AS sales_channel,
                 l.r:user.microsite::string AS microsite, l.r:user.agency.externalId::string AS agency_id,
                 l.r:agencyBookingReference::string AS agency_booking_reference,
                 nullif(trim(l.r:referralId::string), '') AS referral_id,
                 coalesce(nullif(trim(l.r:bookingAssociatedTo::string), ''), l.booking_ref) AS main_booking_ref,
                 l.r:user.username::string AS username, l.r:user.email::string AS user_email,
                 l.r:pricebreakdown.totalPrice.microsite.currency::string AS currency,
                 l.r:pricebreakdown.totalPrice.microsite.amount::decimal(18,2) AS total_price,
                 l.r:pricebreakdown.agencyManagementFee.microsite.amount::decimal(18,2) AS agency_management_fee,
                 l.r:payment.amount::decimal(18,2) AS payment_amount,
                 l.r:payment.deferredAmount::decimal(18,2) AS deferred_amount,
                 to_date(l.r:payment.deferredLimitDate::string) AS deferred_limit_date,
                 l.r:payment.paymentSystem::string AS payment_system,
                 l.r:payment.secondPaymentAmount::decimal(18,2) AS second_payment_amount,
                 l.r:payment.secondPaymentSystem::string AS second_payment_system,
                 ls.services
          FROM landed l
          LEFT JOIN landed_services ls ON ls.booking_ref = l.booking_ref AND ls.ingested_at = l.ingested_at
            AND ls.source_file = l.source_file AND ls.last_update_at = l.last_update_at),
        d AS (
          SELECT h.*,
                 transform(sort_array(transform({active_hotels} AND x.destination_name IS NOT NULL),
                                                x -> named_struct('d', x.start_date, 'id', x.service_id,
                                                                  'n', x.destination_name))), x -> x.n) AS stays
          FROM h)
        SELECT booking_ref, last_update_at, ingested_at, source_file, run_id, tc_booking_id, status, created_at,
               start_date, end_date, pax, trip_type, sales_channel, microsite, agency_id, agency_booking_reference,
               referral_id, main_booking_ref, username, user_email, currency, total_price, agency_management_fee,
               payment_amount, deferred_amount, deferred_limit_date, payment_system, second_payment_amount,
               second_payment_system,
               CASE WHEN size(stays) > 0 THEN left(array_join(transform(
                 filter(sequence(1, size(stays)), i -> i = 1 OR try_element_at(stays, i) <> try_element_at(stays, i - 1)),
                 (i, k) -> (k + 1) || ': ' || try_element_at(stays, i)), ', '), 250) END AS destinations,
               try_element_at(sort_array(transform({active} AND x.destination_country IS NOT NULL),
                 x -> named_struct('d', x.start_date, 'id', x.service_id, 'c', x.destination_country))), 1).c
                 AS destination_country,
               CASE WHEN status = 'CANCELED' THEN array_max(transform(services, x -> x.cancellation_date)) END
                 AS cancellation_date,
               nullif(left(array_join(transform(sort_array(transform({active}),
                 x -> named_struct('d', x.start_date, 'id', x.service_id,
                                   't', concat_ws(' ', nullif({clean_text("x.name")}, ''),
                                                  date_format(x.start_date, 'dd/MM/yyyy'))))), x -> x.t), chr(10)),
                 5000), '') AS service_summary,
               services
        FROM d""",
        # one row per new version, plus the current version of each booking it continues (MERGE updates its two
        # seen columns); see the module docstring for the rule
        f"""CREATE OR REPLACE TEMP VIEW version_updates AS
        WITH state AS (
          SELECT booking_ref, version AS state_version, true AS is_state, last_seen_ingested_at AS seq_at,
                 max_seen_last_update_at AS seq_update, '' AS source_file, content_hash, last_update_at, ingested_at,
                 run_id, {header_cols}
          FROM {b}.booking_versions
          WHERE booking_ref IN (SELECT booking_ref FROM landed_versions)
          QUALIFY row_number() OVER (PARTITION BY booking_ref ORDER BY version DESC) = 1),
        rows AS (
          SELECT * FROM state
          UNION ALL
          SELECT booking_ref, CAST(NULL AS INT), false, ingested_at, last_update_at, source_file,
                 sha2(to_json(named_struct({hashed})), 256), last_update_at, ingested_at, run_id, {header_cols}
          FROM landed_versions),
        seq AS (
          SELECT *, max(seq_update) OVER (PARTITION BY booking_ref ORDER BY seq_at, seq_update, source_file
                                          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max
          FROM rows),
        acc AS (SELECT *, is_state OR prev_max IS NULL OR seq_update >= prev_max AS accepted FROM seq),
        prv AS (
          SELECT *, last_value(CASE WHEN accepted THEN content_hash END, true) OVER (
                      PARTITION BY booking_ref ORDER BY seq_at, seq_update, source_file
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_hash
          FROM acc),
        st AS (SELECT *, accepted AND (is_state OR prev_hash IS NULL OR prev_hash <> content_hash) AS starts FROM prv),
        gp AS (
          SELECT *, sum(CASE WHEN starts THEN 1 ELSE 0 END) OVER (
                      PARTITION BY booking_ref ORDER BY seq_at, seq_update, source_file ROWS UNBOUNDED PRECEDING) AS grp,
                    max(state_version) OVER (PARTITION BY booking_ref) AS base_version
          FROM st),
        fin AS (
          SELECT *, max(seq_update) OVER (PARTITION BY booking_ref, grp) AS group_update,
                    max(seq_at) OVER (PARTITION BY booking_ref, grp) AS group_seen
          FROM gp)
        SELECT booking_ref, CAST(coalesce(base_version, 1) - 1 + grp AS INT) AS version, content_hash,
               last_update_at, ingested_at, run_id, group_update AS max_seen_last_update_at,
               group_seen AS last_seen_ingested_at, {header_cols}
        FROM fin WHERE starts""",
        f"""MERGE INTO {b}.booking_versions t
        USING version_updates s ON t.booking_ref = s.booking_ref AND t.version = s.version
        WHEN MATCHED THEN UPDATE SET t.max_seen_last_update_at = s.max_seen_last_update_at,
                                     t.last_seen_ingested_at = s.last_seen_ingested_at
        WHEN NOT MATCHED THEN INSERT *""",
    ]


def document_dates(first_version: str) -> str:
    """order_date, document_date and posting_date of row 22: a new document of the first version carries the creation
    date, a later change the date of the version, a cancellation the cancellation date."""
    return f"""to_date(created_at) AS order_date,
                 CASE WHEN change_type = 'NEW' AND version = {first_version} THEN to_date(created_at)
                      ELSE to_date(version_update_at) END AS document_date,
                 CASE WHEN change_type = 'NEW' AND version = {first_version} THEN to_date(created_at)
                      WHEN change_type = 'CANCEL' THEN coalesce(cancellation_date, to_date(version_update_at))
                      ELSE to_date(version_update_at) END AS posting_date"""


def sales_sql(c: str, b: str) -> list[str]:
    """Statements that rebuild sales_documents from booking_versions and the current mappings."""
    base = f"coalesce(destinations, booking_ref) || ' commencing ' || date_format(start_date, '{DESCRIPTION_DATE}')"
    no_date, no_text = "CAST(NULL AS DATE)", "CAST(NULL AS STRING)"
    lines = f"""CASE WHEN document_role = 'FULL' THEN filter(array(
              named_struct('rec_id', tc_booking_id, 'line_type', 'Deposit', 'type', 'Item', 'no', sales_item,
                           'description', left({base}, 100), 'quantity', 1, 'unit_price', deposit_amount,
                           'line_due_date', {no_date}, 'comment', {no_text}),
              named_struct('rec_id', tc_booking_id, 'line_type', 'Balance', 'type', 'Item', 'no', sales_item,
                           'description', left({base}, 100), 'quantity', 1,
                           'unit_price', CAST(amount - deposit_amount AS DECIMAL(18,2)),
                           'line_due_date', deferred_limit_date, 'comment', {no_text}),
              named_struct('rec_id', tc_booking_id, 'line_type', {no_text}, 'type', {no_text}, 'no', {no_text},
                           'description', left(split(service_summary, chr(10))[0], 100), 'quantity', 0,
                           'unit_price', CAST(0 AS DECIMAL(18,2)), 'line_due_date', {no_date},
                           'comment', CASE WHEN change_type = 'NEW' THEN service_summary END)),
              l -> l.unit_price > 0 OR l.comment IS NOT NULL)
            ELSE array(named_struct('rec_id', tc_booking_id, 'line_type', 'Balance', 'type', 'Item',
                           'no', CASE WHEN document_role = 'CANCELLATION_FEE' THEN cancellation_item ELSE sales_item END,
                           'description', left(CASE document_role WHEN 'ADJUSTMENT' THEN 'Price adjustment - '
                                                    WHEN 'REVERSAL' THEN 'Reversal - '
                                                    ELSE 'Cancellation fee - ' END || {base}, 100),
                           'quantity', 1, 'unit_price', amount, 'line_due_date', {no_date}, 'comment', {no_text})) END"""
    return [
        f"""CREATE OR REPLACE TEMP VIEW code_map AS
        SELECT map_kind, source_key, bc_code FROM {c}.ref.business_central_code_map
        QUALIFY row_number() OVER (PARTITION BY map_kind, source_key ORDER BY bc_code) = 1""",
        # customer and hold rule of build_business_central_tables, applied with the current mappings to every version
        f"""CREATE OR REPLACE TEMP VIEW sales_versions AS
        SELECT v.* EXCEPT (services), v.last_update_at AS version_update_at, coalesce(v.total_price, 0) AS total,
               CASE WHEN u.agency_id IS NULL THEN 'AG' || v.agency_id ELSE m.bc_customer_no END AS customer_number,
               CASE WHEN u.agency_id IS NOT NULL AND m.bc_customer_no IS NULL THEN 'CUSTOMER_UNMAPPED'
                    WHEN u.agency_id IS NULL AND ag.agency_id IS NULL THEN 'AGENCY_NOT_IN_SNAPSHOT' END AS hold_reason,
               sp.bc_code AS salesperson_code
        FROM {b}.booking_versions v
        LEFT JOIN (SELECT DISTINCT agency_id FROM {c}.ref.umbrella_agency) u ON u.agency_id = v.agency_id
        LEFT JOIN (SELECT agency_reference_key, max(bc_customer_no) AS bc_customer_no FROM {c}.ref.customer_map
                   GROUP BY 1) m ON m.agency_reference_key = lower(trim(v.agency_booking_reference))
        LEFT JOIN {c}.silver.agencies ag ON ag.agency_id = v.agency_id
        LEFT JOIN code_map sp ON sp.map_kind = 'SALESPERSON' AND sp.source_key = lower(trim(v.username))""",
        # Versions sent: from the first BOOKED or RQ version with a positive total; afterwards BOOKED, RQ and CANCELED
        # (row 30: a booking cancelled before it was sent is skipped; PENDING_UPDATE, NOT_BOOKED, BOOK_ERROR are not sent).
        f"""CREATE OR REPLACE TEMP VIEW sales_changes AS
        WITH f AS (
          SELECT *, min(CASE WHEN status IN {ACTIVE} AND total > 0 THEN version END)
                      OVER (PARTITION BY booking_ref) AS first_sent,
                    min(version) OVER (PARTITION BY booking_ref) AS first_version
          FROM sales_versions),
        e AS (SELECT * FROM f WHERE version >= first_sent AND status IN ('BOOKED', 'RQ', 'CANCELED')),
        p AS (
          SELECT e.*, lag(version) OVER w AS prev_version, lag(status) OVER w AS prev_status,
                 lag(total) OVER w AS prev_total, lag(customer_number) OVER w AS prev_customer,
                 lag(currency) OVER w AS prev_currency, lag(hold_reason) OVER w AS prev_hold_reason
          FROM e WINDOW w AS (PARTITION BY booking_ref ORDER BY version))
        SELECT p.*,
               CASE WHEN prev_version IS NULL THEN 'NEW'
                    WHEN status = 'CANCELED' AND prev_status <> 'CANCELED' THEN 'CANCEL'
                    WHEN NOT (customer_number <=> prev_customer AND currency <=> prev_currency) THEN 'REISSUE'
                    WHEN total > prev_total THEN 'INCREASE'
                    WHEN total < prev_total THEN 'DECREASE' END AS change_type
        FROM p""",
        # Each document carries a signed amount (positive = Invoice); the documents of a version move the amount
        # invoiced from the previous total to the current one, so the documents of a booking add up to its total.
        """CREATE OR REPLACE TEMP VIEW sales_docs AS
        SELECT x.*, d.doc_seq, d.document_role, d.signed, d.doc_customer, d.doc_currency, d.doc_hold
        FROM sales_changes x
        LATERAL VIEW explode(filter(array(
          named_struct('doc_seq', 1, 'document_role', 'REVERSAL', 'signed', CAST(-x.prev_total AS DECIMAL(18,2)),
                       'doc_customer', x.prev_customer, 'doc_currency', x.prev_currency, 'doc_hold', x.prev_hold_reason,
                       'emit', x.change_type IN ('CANCEL', 'REISSUE')),
          named_struct('doc_seq', 2, 'document_role',
                       CASE WHEN x.status = 'CANCELED' THEN 'CANCELLATION_FEE' ELSE 'FULL' END,
                       'signed', CAST(x.total AS DECIMAL(18,2)), 'doc_customer', x.customer_number,
                       'doc_currency', x.currency, 'doc_hold', x.hold_reason,
                       'emit', x.change_type IN ('NEW', 'CANCEL', 'REISSUE')),
          named_struct('doc_seq', 1, 'document_role', 'ADJUSTMENT',
                       'signed', CAST(x.total - x.prev_total AS DECIMAL(18,2)), 'doc_customer', x.customer_number,
                       'doc_currency', x.currency, 'doc_hold', x.hold_reason,
                       'emit', x.change_type IN ('INCREASE', 'DECREASE'))),
          y -> y.emit AND y.signed <> 0)) t AS d""",
        f"""INSERT OVERWRITE {b}.sales_documents
        WITH s AS (
          SELECT x.*, CASE WHEN x.signed > 0 THEN 'Invoice' ELSE 'Credit Memo' END AS document_type,
                 CAST(abs(x.signed) AS DECIMAL(18,2)) AS amount,
                 CAST(CASE WHEN x.signed > 0 THEN least(greatest(coalesce(x.payment_amount, 0), 0), x.signed) ELSE 0 END
                      AS DECIMAL(18,2)) AS deposit_amount,
                 {document_dates("first_version")},
                 it.bc_code AS sales_item, ci.bc_code AS cancellation_item,
                 CAST(CASE WHEN x.payment_system = '{TMT_SYSTEM}' THEN coalesce(x.payment_amount, 0) ELSE 0 END
                      + CASE WHEN x.second_payment_system = '{TMT_SYSTEM}' THEN coalesce(x.second_payment_amount, 0)
                             ELSE 0 END AS DECIMAL(18,2)) AS tmt_paid
          FROM sales_docs x
          LEFT JOIN code_map it ON it.map_kind = 'ITEM' AND it.source_key = 'SALES'
          LEFT JOIN code_map ci ON ci.map_kind = 'ITEM' AND ci.source_key = 'CANCELLATION'),
        n AS (
          SELECT s.*, {lines} AS sales_lines,
                 row_number() OVER (PARTITION BY posting_date, document_type
                                    ORDER BY ingested_at, booking_ref, version, doc_seq) AS day_seq
          FROM s)
        SELECT booking_ref || '-' || version, version, doc_seq, change_type, document_role, document_type, booking_ref,
               doc_customer, doc_hold, order_date, document_date, posting_date,
               CASE WHEN document_role = 'FULL' AND document_type = 'Invoice' AND amount > deposit_amount
                    THEN deferred_limit_date END,
               start_date, end_date, doc_currency, CAST(NULL AS STRING), CAST(NULL AS DECIMAL(24,12)), referral_id,
               status, cancellation_date, CAST(NULL AS STRING), last_update_at, pax, trip_type, destinations,
               destination_country, sales_channel, microsite, main_booking_ref, salesperson_code, user_email, total,
               agency_management_fee, CASE WHEN tmt_paid > 0 THEN tmt_paid END,
               CASE WHEN tmt_paid > 0 THEN '{TMT_TEXT}' END,
               CASE WHEN document_type = 'Invoice' THEN {invoice_number_sql("posting_date", "day_seq")} END,
               aggregate(sales_lines, CAST(0 AS DECIMAL(18,2)),
                         (acc, l) -> CAST(acc + l.quantity * l.unit_price AS DECIMAL(18,2))),
               sales_lines
        FROM n""",
    ]


def purchase_sql(c: str, b: str) -> list[str]:
    """Statements that rebuild purchase_documents from the services of booking_versions."""
    provider_vendor = PROVIDER_VENDOR_NUMBER.replace("s.provider", "x.s.provider")
    return [
        # one row per service and booking version from the first version that holds the service; a service missing
        # from a later version is REMOVED and keeps its last known fields
        f"""CREATE OR REPLACE TEMP VIEW service_states AS
        WITH sv AS (
          SELECT v.booking_ref, v.version, s FROM {b}.booking_versions v LATERAL VIEW explode(v.services) t AS s),
        k AS (SELECT booking_ref, s.purchase_key, min(version) AS first_version FROM sv GROUP BY 1, 2),
        g AS (
          SELECT v.booking_ref, v.version, v.ingested_at, v.last_update_at AS version_update_at, v.created_at,
                 v.username, v.main_booking_ref, k.purchase_key, k.first_version, sv.s AS found
          FROM {b}.booking_versions v
          JOIN k ON k.booking_ref = v.booking_ref AND v.version >= k.first_version
          LEFT JOIN sv ON sv.booking_ref = v.booking_ref AND sv.version = v.version
            AND sv.s.purchase_key = k.purchase_key)
        SELECT booking_ref, version, ingested_at, version_update_at, created_at, username, main_booking_ref,
               purchase_key, first_version, found IS NULL AS removed,
               last_value(found, true) OVER (PARTITION BY booking_ref, purchase_key ORDER BY version
                                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s
        FROM g""",
        f"""CREATE OR REPLACE TEMP VIEW purchase_versions AS
        SELECT x.booking_ref, x.version, x.ingested_at, x.version_update_at, x.purchase_key, x.first_version,
               x.created_at, x.main_booking_ref,
               CASE WHEN x.removed THEN 'REMOVED' ELSE x.s.status END AS service_status,
               CAST(CASE WHEN x.removed THEN 0 ELSE coalesce(x.s.cost_amount, 0) + x.s.amendment_cost_amount END
                    AS DECIMAL(18,2)) AS cost,
               x.s.service_type, x.s.name, x.s.cost_currency AS currency, x.s.exchange_rate_microsite,
               x.s.start_date, x.s.end_date, x.s.cancellation_date,
               coalesce(x.s.last_update_at, x.version_update_at) AS last_update_at,
               coalesce(x.s.supplier_confirmation, x.s.provider_reference) AS vendor_order_no,
               CASE WHEN x.s.supplier_id IS NOT NULL THEN 'SP' || x.s.supplier_id
                    WHEN NOT x.s.is_manual AND x.s.provider IS NOT NULL THEN coalesce(mp.bc_vendor_no, {provider_vendor})
                    ELSE mm.bc_vendor_no END AS vendor_number,
               CASE WHEN x.s.supplier_id IS NULL AND (x.s.is_manual OR x.s.provider IS NULL)
                         AND mm.bc_vendor_no IS NULL THEN 'VENDOR_UNMAPPED'
                    WHEN x.s.supplier_id IS NOT NULL AND sp.supplier_id IS NULL THEN 'SUPPLIER_NOT_IN_SNAPSHOT' END
                 AS hold_reason,
               pc.bc_code AS purchaser_code, bu.bc_code AS business_unit, it.bc_code AS item_no
        FROM service_states x
        LEFT JOIN (SELECT source_key, max(bc_vendor_no) AS bc_vendor_no FROM {c}.ref.vendor_map
                   WHERE source_kind = 'PROVIDER' GROUP BY 1) mp ON mp.source_key = x.s.provider
        LEFT JOIN (SELECT source_key, max(bc_vendor_no) AS bc_vendor_no FROM {c}.ref.vendor_map
                   WHERE source_kind = 'MANUAL_NAME' GROUP BY 1) mm ON mm.source_key = lower(trim(x.s.supplier_name))
        LEFT JOIN {c}.silver.suppliers sp ON sp.supplier_id = x.s.supplier_id
        LEFT JOIN code_map pc ON pc.map_kind = 'SALESPERSON' AND pc.source_key = lower(trim(x.username))
        LEFT JOIN code_map bu ON bu.map_kind = 'BUSINESS_UNIT' AND bu.source_key = lower(trim(x.username))
        LEFT JOIN code_map it ON it.map_kind = 'ITEM' AND it.source_key = x.s.service_type""",
        # Sent from the first BOOKED version with a cost (row 34: On Request is not sent, a service cancelled before
        # it was sent is skipped); afterwards BOOKED, CANCELED and REMOVED.
        """CREATE OR REPLACE TEMP VIEW purchase_changes AS
        WITH f AS (
          SELECT *, min(CASE WHEN service_status = 'BOOKED' AND cost > 0 THEN version END)
                      OVER (PARTITION BY booking_ref, purchase_key) AS first_sent
          FROM purchase_versions),
        e AS (SELECT * FROM f WHERE version >= first_sent AND service_status IN ('BOOKED', 'CANCELED', 'REMOVED')),
        p AS (
          SELECT e.*, lag(version) OVER w AS prev_version, lag(service_status) OVER w AS prev_status,
                 lag(cost) OVER w AS prev_cost, lag(vendor_number) OVER w AS prev_vendor,
                 lag(currency) OVER w AS prev_currency, lag(hold_reason) OVER w AS prev_hold_reason
          FROM e WINDOW w AS (PARTITION BY booking_ref, purchase_key ORDER BY version))
        SELECT p.*,
               CASE WHEN prev_version IS NULL THEN 'NEW'
                    WHEN NOT (vendor_number <=> prev_vendor AND currency <=> prev_currency) THEN 'REISSUE'
                    WHEN service_status <> 'BOOKED' AND prev_status = 'BOOKED' THEN 'CANCEL'
                    WHEN cost > prev_cost THEN 'INCREASE'
                    WHEN cost < prev_cost THEN 'DECREASE' END AS change_type
        FROM p""",
        """CREATE OR REPLACE TEMP VIEW purchase_docs AS
        SELECT x.*, d.doc_seq, d.document_role, d.signed, d.doc_vendor, d.doc_currency, d.doc_hold
        FROM purchase_changes x
        LATERAL VIEW explode(filter(array(
          named_struct('doc_seq', 1, 'document_role', 'REVERSAL', 'signed', CAST(-x.prev_cost AS DECIMAL(18,2)),
                       'doc_vendor', x.prev_vendor, 'doc_currency', x.prev_currency, 'doc_hold', x.prev_hold_reason,
                       'emit', x.change_type = 'REISSUE'),
          named_struct('doc_seq', 2, 'document_role', 'FULL', 'signed', x.cost, 'doc_vendor', x.vendor_number,
                       'doc_currency', x.currency, 'doc_hold', x.hold_reason,
                       'emit', x.change_type IN ('NEW', 'REISSUE')),
          named_struct('doc_seq', 1, 'document_role', 'ADJUSTMENT',
                       'signed', CAST(x.cost - x.prev_cost AS DECIMAL(18,2)), 'doc_vendor', x.vendor_number,
                       'doc_currency', x.currency, 'doc_hold', x.hold_reason,
                       'emit', x.change_type IN ('CANCEL', 'INCREASE', 'DECREASE'))),
          y -> y.emit AND y.signed <> 0)) t AS d""",
        f"""INSERT OVERWRITE {b}.purchase_documents
        WITH s AS (
          SELECT *, CASE WHEN signed > 0 THEN 'Invoice' ELSE 'Credit Memo' END AS document_type,
                 CAST(abs(signed) AS DECIMAL(18,2)) AS amount,
                 {document_dates("first_version")},
                 CASE document_role WHEN 'ADJUSTMENT' THEN 'Cost adjustment - ' WHEN 'REVERSAL' THEN 'Reversal - '
                      ELSE '' END || purchase_key || ' ' || {clean_text("name")} AS full_description
          FROM purchase_docs)
        SELECT purchase_key || '-' || version, version, doc_seq, change_type, document_role, document_type, 'Trading',
               purchase_key, booking_ref, doc_vendor, doc_hold, vendor_order_no, CAST(NULL AS STRING),
               CAST(NULL AS STRING), order_date, document_date, posting_date, start_date, end_date, doc_currency,
               exchange_rate_microsite, service_status, CAST(NULL AS STRING), last_update_at, business_unit,
               main_booking_ref, purchaser_code, cost, CAST(NULL AS STRING), amount,
               array(named_struct('type', 'Item', 'no', item_no, 'description', left(full_description, 100),
                                  'description_2', nullif(substr(full_description, 101, 50), ''), 'quantity', 1,
                                  'direct_unit_cost', amount))
        FROM s""",
    ]


def checks_sql(b: str) -> dict[str, tuple[str, str]]:
    """business_central_ checks of this task: block fails the task, warn is recorded only."""
    sales, purch, vers = f"{b}.sales_documents", f"{b}.purchase_documents", f"{b}.booking_versions"
    latest = f"(SELECT * FROM {vers} QUALIFY version = max(version) OVER (PARTITION BY booking_ref))"
    signed = "CASE WHEN document_type = 'Invoice' THEN document_amount ELSE -document_amount END"
    return {
        "business_central_version_sequence_broken": (
            "block",
            f"""SELECT booking_ref AS k FROM {vers} GROUP BY booking_ref
            HAVING count(*) <> max(version) OR min(version) <> 1 OR count(DISTINCT version) <> count(*)""",
        ),
        "business_central_document_key_unique": (
            "block",
            f"""SELECT integration_key || '#' || doc_seq AS k FROM {sales} GROUP BY 1 HAVING count(*) > 1
            UNION ALL SELECT integration_key || '#' || doc_seq FROM {purch} GROUP BY 1 HAVING count(*) > 1""",
        ),
        "business_central_invoice_number_invalid": (
            "block",
            f"""SELECT invoice_no AS k FROM {sales} WHERE invoice_no IS NOT NULL GROUP BY 1 HAVING count(*) > 1
            UNION ALL SELECT integration_key FROM {sales}
            WHERE document_type = 'Invoice' AND NOT coalesce(invoice_no RLIKE '^[0-9]{{8}}$', false)""",
        ),
        # the signed documents of a booking add up to its total in the last version that produced a document
        "business_central_sales_documents_not_reconciled": (
            "block",
            f"""SELECT external_document_no AS k FROM {sales} GROUP BY external_document_no
            HAVING abs(sum({signed}) - max_by(booking_total_amount, dwh_version * 10 + doc_seq)) > 0.005""",
        ),
        "business_central_purchase_documents_not_reconciled": (
            "block",
            f"""SELECT your_reference AS k FROM {purch} GROUP BY your_reference
            HAVING abs(sum({signed}) - max_by(service_total_cost, dwh_version * 10 + doc_seq)) > 0.005""",
        ),
        "business_central_deposit_balance_differs_from_total": (
            "block",
            f"""SELECT integration_key AS k FROM {sales}
            WHERE document_role = 'FULL' AND document_type = 'Invoice'
              AND aggregate(filter(sales_lines, l -> l.line_type IN ('Deposit', 'Balance')), CAST(0 AS DECIMAL(18,2)),
                            (acc, l) -> CAST(acc + l.unit_price AS DECIMAL(18,2))) <> booking_total_amount""",
        ),
        "business_central_sales_document_without_destinations": (
            "warn",
            f"SELECT integration_key AS k FROM {sales} WHERE document_role = 'FULL' AND destinations IS NULL",
        ),
        "business_central_document_item_unmapped": (
            "warn",
            f"""SELECT integration_key AS k FROM {sales} WHERE exists(sales_lines, l -> l.type = 'Item' AND l.no IS NULL)
            UNION ALL SELECT integration_key FROM {purch} WHERE exists(purchase_lines, l -> l.no IS NULL)""",
        ),
        "business_central_document_held": (
            "warn",
            f"""SELECT integration_key AS k FROM {sales} WHERE hold_reason IS NOT NULL
            UNION ALL SELECT integration_key FROM {purch} WHERE hold_reason IS NOT NULL""",
        ),
        # Travel Compositor property: payment.amount + payment.deferredAmount is not always the booking total; the
        # deposit line takes payment.amount (at most the total) and the balance line the rest
        "business_central_payment_split_differs_from_total": (
            "warn",
            f"""SELECT booking_ref AS k FROM {latest}
            WHERE status IN {ACTIVE}
              AND abs(coalesce(payment_amount, 0) + coalesce(deferred_amount, 0) - coalesce(total_price, 0)) > 0.005""",
        ),
        "business_central_amendment_cost_other_currency": (
            "warn",
            f"SELECT booking_ref AS k FROM {latest} WHERE exists(services, x -> x.amendment_cost_other_currency > 0)",
        ),
    }


def build_token(sql, c: str) -> str:
    """Token that changes only when what this task reads changes: bronze bookings, mappings, agencies, suppliers."""
    parts = [
        str(sql(f"SELECT count(*) || ':' || coalesce(max(ingested_at), '') FROM {c}.bronze.tc_bookings").first()[0])
    ]
    parts += [
        str(
            sql(f"""SELECT count(*) || ':' || coalesce(sum(CAST(xxhash64(*) AS DECIMAL(38, 0))), 0)
                    FROM {t}""").first()[0]
        )
        for t in (
            f"{c}.ref.umbrella_agency",
            f"{c}.ref.customer_map",
            f"{c}.ref.vendor_map",
            f"{c}.ref.business_central_code_map",
            f"{c}.silver.agencies",
            f"{c}.silver.suppliers",
        )
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def main():
    from pyspark.sql import SparkSession

    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="vio_travel")
    ap.add_argument("--run_id", default="")
    ap.add_argument("--rebuild", default="false", help="true: recompute every version from the whole of bronze")
    a = ap.parse_args()
    spark = SparkSession.builder.getOrCreate()
    c = a.catalog
    sql = spark.sql
    b = f"{c}.{SCHEMA}"
    rebuild = a.rebuild.lower() == "true"

    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ref.business_central_code_map ({ddl(CODE_MAP_COLUMNS)})
        COMMENT '{CODE_MAP_COMMENT}'""")
    sql(f"""CREATE TABLE IF NOT EXISTS {b}.booking_versions ({ddl(VERSION_COLUMNS)})
        CLUSTER BY (booking_ref) COMMENT '{TABLE_COMMENTS["booking_versions"]}'""")
    sql(f"""CREATE TABLE IF NOT EXISTS {b}.sales_documents ({ddl(SALES_COLUMNS)})
        CLUSTER BY (external_document_no) COMMENT '{TABLE_COMMENTS["sales_documents"]}'""")
    sql(f"""CREATE TABLE IF NOT EXISTS {b}.purchase_documents ({ddl(PURCHASE_COLUMNS)})
        CLUSTER BY (booking_reference) COMMENT '{TABLE_COMMENTS["purchase_documents"]}'""")
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ops.build_state (
        task STRING, run_id STRING, built_at TIMESTAMP, silver_token STRING)""")

    token = build_token(sql, c)
    last = sql(f"SELECT max_by(silver_token, built_at) AS t FROM {c}.ops.build_state WHERE task = '{TASK}'").first()[
        "t"
    ]
    if not rebuild and last == token:
        sql(f"INSERT INTO {c}.ops.build_state SELECT '{TASK}', '{a.run_id}', current_timestamp(), '{token}'")
        print("bronze bookings and mappings unchanged since the last build, nothing to do")
        return

    # ---------------- versions: incremental, or from the whole of bronze on rebuild ---------------------------------
    if rebuild:
        sql(f"TRUNCATE TABLE {b}.booking_versions")
    watermark = sql(f"SELECT max(last_seen_ingested_at) AS w FROM {b}.booking_versions").first()["w"]
    for q in versions_sql(c, b, watermark):
        sql(q)
    # ---------------- documents: rebuilt from the versions -----------------------------------------------------------
    for q in sales_sql(c, b) + purchase_sql(c, b):
        sql(q)

    # ---------------- audit: data quality ----------------------------------------------------------------------------
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ops.dq_results (
        run_id STRING, checked_at TIMESTAMP, check_name STRING, severity STRING, failed_rows BIGINT, sample STRING)""")
    rows_out, blocked = [], []
    for name, (severity, q) in checks_sql(b).items():
        r = sql(f"SELECT count(*) AS n, slice(collect_list(k), 1, 5) AS s FROM ({q})").collect()[0]
        rows_out.append((a.run_id, name, severity, r["n"], ",".join(r["s"] or [])))
        print(f"DQ {severity:5} {name}: {r['n']} sample={r['s']}")
        if r["n"] and severity == "block":
            blocked.append(name)
    spark.createDataFrame(
        rows_out, "run_id STRING, check_name STRING, severity STRING, failed_rows BIGINT, sample STRING"
    ).selectExpr(
        "run_id", "current_timestamp() AS checked_at", "check_name", "severity", "failed_rows", "sample"
    ).write.mode("append").saveAsTable(f"{c}.ops.dq_results")
    for t in ("booking_versions", "sales_documents", "purchase_documents"):
        print(t, sql(f"SELECT count(*) FROM {b}.{t}").collect()[0][0])
    if blocked:
        raise RuntimeError(f"blocking data-quality checks failed: {blocked}")
    sql(f"INSERT INTO {c}.ops.build_state SELECT '{TASK}', '{a.run_id}', current_timestamp(), '{token}'")


if __name__ == "__main__":
    main()
