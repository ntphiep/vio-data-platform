"""Task build_business_central_documents - posted sales and purchase documents in the shape the implementation partner
specifies (VIO_Data warhouse_Integration mapping fields-V1.2.xlsx, sheets API_Sales_T01 and API_Purchases_T01), catalog
<catalog>:

business_central.booking_versions    one row per DWH version of a booking: a landing in bronze.tc_bookings whose
                                     fields sent to Business Central differ from the version before; built
                                     incrementally from the bronze rows landed since the last build (MERGE)
business_central.sales_documents     one row per sales document; append-only: a document is never rewritten
business_central.purchase_documents  one row per purchase document of one service; append-only
ref.business_central_code_map        item, salesperson and business unit codes owned by Vio Finance, created empty
ops.dq_results                       business_central_ checks of these tables

The partner specification has no orders: every change of a booking produces posted documents.

    change of a booking (status BOOKED, RQ or CANCELED)       sales documents
    first version with a positive total (NEW)                 Invoice: deposit line, balance line, comment line
    total up or down (INCREASE, DECREASE)                     Invoice or Credit Memo for the difference
    status becomes CANCELED (CANCEL)                          Credit Memo for the total invoiced, Invoice for the fee kept
    customer or currency changes, or a held document can be   Credit Memo for the total invoiced, new Invoice
    released (REISSUE)

Purchase documents follow the same rules per service (status BOOKED creates; CANCELED or removed credits the cost
invoiced and invoices the cost Travel Compositor keeps; a change of vendor or currency reissues).

Documents are append-only. Each run compares, for the bookings it reads, the versions after the last version that
produced a document with what the documents already emitted add up to (amount, customer or vendor, currency, hold),
and MERGEs only the documents that move the emitted state to the current one. A change of ref.customer_map,
ref.vendor_map, ref.umbrella_agency, silver.agencies or silver.suppliers makes the run read every booking, so a
mapping that changes an emitted customer or vendor produces a Credit Memo and a new Invoice dated the build day; the
emitted documents keep their values. Otherwise the run reads only the bookings with a version landed after the
watermark of the last successful build (table property DOCUMENTS_WATERMARK of booking_versions). The Invoice No.
(YYDDDNNN) is given once, when the invoice is emitted: day of the build in Asia/Ho_Chi_Minh, then the next number of
that day. The signed amounts of the documents of a booking or service add up to its current total; blocking checks
test that, the keys and the invoice numbers, and that no emitted invoice number changed since the previous version
of the table. The tables are not read by task push_to_business_central yet.

Versions (DWH version): the bronze rows of a booking are read in landing order (ingested_at, then lastUpdateDate, then
file). A row whose lastUpdateDate is older than one landed before it is ignored, so a late or replayed page never
brings back an older version. A row starts a new version when the hash of its fields sent to Business Central differs
from the last row that was not ignored. The same rule runs on the whole of bronze (job parameter rebuild=true) and on
the rows landed since the last build, so an incremental build and a rebuild give the same versions. A rebuild
recomputes the versions and reads every booking; it keeps the emitted documents and their numbers and adds only the
documents that are missing.
"""

import argparse
import datetime
import hashlib
from zoneinfo import ZoneInfo

SCHEMA = "business_central"
TASK = "build_business_central_documents"
# Day of the invoice number (YYDDD) and of the documents a mapping change produces.
BUILD_TIME_ZONE = "Asia/Ho_Chi_Minh"
# Table property of booking_versions: max(last_seen_ingested_at) when documents were last built successfully.
DOCUMENTS_WATERMARK = "vio.documents_built_through"
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
# Same values as src/build_business_central_tables.py (tests/test_sql_parity.py): purchase_key (service id and type
# code) is the purchase_key of that task.
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
# Category of the Service Reference (API_Purchases_T01 row 17, sample HOTEL-EXA-35844-6): category, booking reference,
# Travel Compositor line id. A manual line starts with M, because an API line and a manual line can share a line id.
SERVICE_CATEGORIES = {
    "hotelservice": "HOTEL",
    "transferservice": "TRANSFER",
    "transportservice": "TRANSPORT",
    "ticketservice": "TICKET",
    "closedtourservice": "CLOSEDTOUR",
    "carservice": "CAR",
    "insuranceservice": "INSURANCE",
    "cruiseservice": "CRUISE",
    "itemservice": "ITEM",
    "manual_hotel": "MHOTEL",
    "manual_transfer": "MTRANSFER",
    "manual_transport": "MTRANSPORT",
    "manual_ticket": "MTICKET",
    "manual_closedTour": "MCLOSEDTOUR",
    "manual_carRental": "MCAR",
    "manual_cruise": "MCRUISE",
    "manual_insurance": "MINSURANCE",
    "manual_other": "MOTHER",
    "manual_accountingAdjustment": "MADJUSTMENT",
}
PROVIDER_VENDOR_NUMBER = "left('PV' || regexp_replace(upper(s.provider), '[^A-Z0-9]', ''), 20)"
COUNTRY_SUFFIX = "', *([A-Z]{2}) *$'"
ACTIVE = "('BOOKED', 'RQ')"
# Date format of the invoice description in the Xero exports of the AR Masterfile: '1: Cebu, 2: El Nido commencing 27/12/2026'.
DESCRIPTION_DATE = "dd/M/yyyy"
# Payment system of Travel Compositor whose payments are the Paid Amount (API_Sales_T01 row 45, Trust My Travel).
# Unconfirmed: the live bookings show only DEPOSIT and REPAYD, so no value is known and paid_amount stays NULL. Set it
# once Vio confirms the marker; the paid amount then goes on the full invoice of the version where it changed.
PAID_SYSTEM = None
PAID_METHOD_TEXT = "Trust My Travel"
# Accounting currency of Business Central: the partner sends it as a blank Currency Code (API_Sales_T01 row 27,
# API_Purchases_T01 row 32).
BLANK_CURRENCY = "USD"

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
    ("payment_system", "STRING", "Payment system of the first payment: DEPOSIT, REPAYD ..."),
    ("second_payment_amount", "DECIMAL(18,2)", "Amount of the second payment, in currency."),
    ("second_payment_system", "STRING", "Payment system of the second payment."),
    (
        "destinations",
        "STRING",
        "Hotel stays in check-in order, repeated stays merged: '1: Tokyo, 2: Kyoto'. NULL without an active hotel.",
    ),
    (
        "destination_country",
        "STRING",
        "Country of the first active service that has one (after the same-name fallback of services): dimension DESTINATION.",
    ),
    ("cancellation_date", "DATE", "Latest cancelationDate of the services when the booking is CANCELED, else NULL."),
    (
        "service_summary",
        "STRING",
        "Active services in start order, one per line: name and start date, no prices (comment line, up to 5000 characters).",
    ),
    (
        "services",
        SERVICE_TYPE,
        "Every service of the version (API arrays and manualServices) with its amounts; purchase_key is the purchase_key of build_business_central_tables. NULL when the booking has no service.",
    ),
]
SALES_COLUMNS = [
    (
        "integration_key",
        "STRING",
        "IntegrationKey of API_Sales_T01, one per document: booking reference, DWH version and doc_seq, as booking_ref-version-doc_seq.",
    ),
    (
        "dwh_version",
        "INT",
        "DWH version of the booking (booking_versions.version) the document brings Business Central to.",
    ),
    (
        "doc_seq",
        "INT",
        "Order of the document within its version: 1, then 2 for the invoice that follows a credit memo; a correction of an emitted version continues the numbering.",
    ),
    (
        "change_type",
        "STRING",
        "NEW, INCREASE, DECREASE, CANCEL (status became CANCELED) or REISSUE (customer or currency changed, or a held document can be released).",
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
        "CUSTOMER_UNMAPPED or AGENCY_NOT_IN_SNAPSHOT when the customer is not known; NULL when the document can be sent. A held document is never released: a later REISSUE credits it and invoices again.",
    ),
    (
        "order_date",
        "DATE",
        "Order Date: booking creation date as Travel Compositor sends it; the time zone rule is open (scenario DT-01).",
    ),
    (
        "document_date",
        "DATE",
        "Document Date: creation date for a document of the first version, the build day for a correction of an emitted version, else the date of lastUpdateDate of the version.",
    ),
    (
        "posting_date",
        "DATE",
        "Posting Date (row 22): as document_date, but the cancellation date for a cancellation of a new version.",
    ),
    ("due_date", "DATE", "Due Date: due date of the balance line of a full invoice (scenario DT-02); NULL otherwise."),
    ("service_start_date", "DATE", "Service Start Date: first day of the trip."),
    ("service_end_date", "DATE", "Service End Date: last day of the trip."),
    (
        "currency_code",
        "STRING",
        "Currency Code (row 27): booking_currency, blank when it is USD, the accounting currency.",
    ),
    ("booking_currency", "STRING", "Currency of every amount of the document: the Travel Compositor booking currency."),
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
    (
        "cancellation_date",
        "DATE",
        "Cancellation Date: latest service cancellation date when the booking is CANCELED (row 31: mandatory for a cancellation; check business_central_cancellation_without_date).",
    ),
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
        "Salesperson Code of the booking creator from ref.business_central_code_map (SALESPERSON) when the document was emitted; NULL until Finance maps the user.",
    ),
    ("sell_to_email", "STRING", "E-Mail of the booking creator. Personal data."),
    (
        "booking_total_amount",
        "DECIMAL(18,2)",
        "Booking Total Amount: booking total after the change, in booking_currency.",
    ),
    ("agency_management_fee", "DECIMAL(18,2)", "Agency Management Fee of the booking."),
    (
        "paid_amount",
        "DECIMAL(18,2)",
        "Paid Amount (row 45): payments through Trust My Travel added since the last full invoice, on the full invoice of the version only. NULL: Travel Compositor shows no Trust My Travel payment system (only DEPOSIT and REPAYD); Vio is to confirm the marker.",
    ),
    ("payment_method", "STRING", "Payment Method text (row 46): Trust My Travel when paid_amount is above 0."),
    (
        "invoice_no",
        "STRING",
        "No. of an invoice (row 47), YYDDDNNN, given once when the invoice is emitted: year and day of the build day in Asia/Ho_Chi_Minh, then the next number of that day. NULL on a credit memo (numbering open) and on a held invoice, which is never sent.",
    ),
    (
        "document_amount",
        "DECIMAL(18,2)",
        "Sum of the lines (quantity x unit price), positive; a credit memo reduces the amount invoiced.",
    ),
    (
        "sales_lines",
        SALES_LINE_TYPE,
        "Sales lines: Deposit and Balance tour lines (item, unit price, line due date) and a comment line with the service summary (type NULL, price 0). The description reads '<destinations or booking> commencing <start date>', without the date when the trip has no start date.",
    ),
    ("emitted_at", "TIMESTAMP", "Time the build that emitted the document wrote it; the row is never changed after."),
    ("emitted_run_id", "STRING", "Job run (ops.ingest_runs.run_id) of the build that emitted the document."),
]
PURCHASE_COLUMNS = [
    (
        "integration_key",
        "STRING",
        "IntegrationKey of API_Purchases_T01, one per document: service_reference, DWH version and doc_seq, as service_reference-version-doc_seq.",
    ),
    ("dwh_version", "INT", "DWH version of the booking that carried the change (booking_versions.version)."),
    (
        "doc_seq",
        "INT",
        "Order of the document within its version: 1, then 2 for the invoice that follows a credit memo; a correction of an emitted version continues the numbering.",
    ),
    (
        "change_type",
        "STRING",
        "NEW, INCREASE, DECREASE, CANCEL (service cancelled or removed) or REISSUE (vendor or currency changed, or a held document can be released).",
    ),
    (
        "document_role",
        "STRING",
        "FULL (whole cost), ADJUSTMENT (difference), REVERSAL (credit of the cost invoiced) or CANCELLATION_FEE (cost Travel Compositor keeps on a cancelled service, scenario PO-CAN-02).",
    ),
    ("document_type", "STRING", "Invoice or Credit Memo."),
    ("purchase_type", "STRING", "Purchase Type: Trading for every service (Opex comes from another source)."),
    (
        "service_reference",
        "STRING",
        "Service Reference (row 17), sent as Your Reference: category, booking reference and Travel Compositor line id, as HOTEL-EXA-35844-6; a manual line has category M<type>.",
    ),
    (
        "purchase_key",
        "STRING",
        "Service id and type code: purchase_key of business_central.purchase_invoices.",
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
        "VENDOR_UNMAPPED or SUPPLIER_NOT_IN_SNAPSHOT when the vendor is not known; NULL when the document can be sent. A held document is never released: a later REISSUE credits it and invoices again.",
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
        "Document Date: booking creation date for a document of the first booking version, the build day for a correction of an emitted version, else the date of lastUpdateDate of the booking version that carried the change.",
    ),
    (
        "posting_date",
        "DATE",
        "Posting Date: as document_date, but the service cancellation date for a cancellation of a new version.",
    ),
    ("service_start_date", "DATE", "Service Start Date."),
    ("service_end_date", "DATE", "Service End Date."),
    (
        "currency_code",
        "STRING",
        "Currency Code (row 32): cost_currency, blank when it is USD, the accounting currency.",
    ),
    (
        "cost_currency",
        "STRING",
        "Currency the supplier charges (providerNetPrice): currency of every amount of the document.",
    ),
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
        "Business Unit dimension of the booking creator from ref.business_central_code_map (BUSINESS_UNIT) when the document was emitted; NULL: Travel Compositor holds no business unit.",
    ),
    ("main_booking", "STRING", "Main Booking, dimension MAIN BOOKING."),
    (
        "purchaser_code",
        "STRING",
        "Purchaser Code of the booking creator from ref.business_central_code_map (SALESPERSON) when the document was emitted.",
    ),
    (
        "service_total_cost",
        "DECIMAL(18,2)",
        "Service Total Cost: supplier cost after the change plus the amendment costs in that currency; for a cancelled service the cost Travel Compositor keeps; 0 when removed.",
    ),
    ("internal_no", "STRING", "No. of a purchase invoice (row 41). NULL: the number format is open."),
    ("document_amount", "DECIMAL(18,2)", "Sum of the lines (quantity x direct unit cost), positive."),
    (
        "purchase_lines",
        PURCHASE_LINE_TYPE,
        "Purchase line (rows 52 to 56): item of the service type, description '<prefix><service reference> <name>' (100 characters), description_2 the rest of it and the destination name (50 characters), quantity 1, direct unit cost.",
    ),
    ("emitted_at", "TIMESTAMP", "Time the build that emitted the document wrote it; the row is never changed after."),
    ("emitted_run_id", "STRING", "Job run (ops.ingest_runs.run_id) of the build that emitted the document."),
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
    "sales_documents": "Posted sales documents of the implementation partner specification API_Sales_T01: Invoice or Credit Memo per booking version, with deposit, balance and comment lines. Append-only: a document is never rewritten.",
    "purchase_documents": "Posted purchase documents of the implementation partner specification API_Purchases_T01: Invoice or Credit Memo per service and booking version. Append-only: a document is never rewritten.",
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


def invoice_prefix(day: datetime.date) -> str:
    """YYDDD of the AR Masterfile invoice number YYDDDNNN: year and day of the year of the build day."""
    return f"{day:%y}{day.timetuple().tm_yday:03d}"


def build_day(now: datetime.datetime | None = None) -> datetime.date:
    """Day of the build in BUILD_TIME_ZONE."""
    return (now or datetime.datetime.now(datetime.UTC)).astimezone(ZoneInfo(BUILD_TIME_ZONE)).date()


def service_category_sql(col: str) -> str:
    """CASE expression for SERVICE_CATEGORIES; an unknown manual type falls back to M and its name in capitals."""
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in SERVICE_CATEGORIES.items())
    return f"CASE {col} {whens} ELSE 'M' || upper(replace({col}, 'manual_', '')) END"


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


def country_fallback_sql(col: str) -> str:
    """The services of col with the destination_country rule of silver.booking_services: a service without a country
    of its own takes the country of the first service of the booking (start date, then service id) that has the same
    destination name and a country of its own (tests/test_sql_parity.py)."""
    same_name = (
        f"try_element_at(transform(sort_array(transform(filter({col}, y -> y.destination_country IS NOT NULL "
        "AND lower(y.destination_name) = lower(x.destination_name)), "
        "y -> named_struct('d', y.start_date, 'id', y.service_id, 'c', y.destination_country))), y -> y.c), 1)"
    )
    fields = ", ".join(
        f"'{n}', coalesce(x.{n}, {same_name})" if n == "destination_country" else f"'{n}', x.{n}"
        for n, _ in SERVICE_FIELDS
    )
    return f"transform({col}, x -> named_struct({fields}))"


def versions_sql(c: str, b: str) -> list[str]:
    """Statements that MERGE the versions of the bronze rows landed after the latest landing booking_versions has read
    (max(last_seen_ingested_at), compared in SQL); every row when booking_versions is empty."""
    landed_filter = (
        "AND ingested_at > (SELECT coalesce(max(last_seen_ingested_at), TIMESTAMP'1900-01-01 00:00:00') "
        f"FROM {b}.booking_versions)"
    )
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
                 {country_fallback_sql("ls.services")} AS services
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


def candidates_sql(b: str, watermark: str | None) -> str:
    """Temp view candidates: the bookings this run reads. Every booking when watermark is None (first build, rebuild or
    a changed mapping), else the bookings with a version landed after watermark (DOCUMENTS_WATERMARK)."""
    where = "" if watermark is None else f"WHERE ingested_at > CAST('{watermark}' AS TIMESTAMP)"
    return f"""CREATE OR REPLACE TEMP VIEW candidates AS
        SELECT DISTINCT booking_ref FROM {b}.booking_versions {where}"""


def document_dates() -> str:
    """order_date, document_date and posting_date of row 22: a document of the first version carries the creation
    date, a correction of an emitted version the build day, a later change the date of the version, a cancellation
    the cancellation date."""
    return """to_date(created_at) AS order_date,
                 CASE WHEN is_correction THEN build_day WHEN change_type = 'NEW' AND version = 1 THEN to_date(created_at)
                      ELSE to_date(version_update_at) END AS document_date,
                 CASE WHEN is_correction THEN build_day WHEN change_type = 'NEW' AND version = 1 THEN to_date(created_at)
                      WHEN change_type = 'CANCEL' THEN coalesce(cancellation_date, to_date(version_update_at))
                      ELSE to_date(version_update_at) END AS posting_date"""


def previous_sql(window: str, fields: list[str]) -> str:
    """prev_<field> of each row: the row before it in window, and for the first row the emitted state e_<field>."""
    return ",\n".join(
        f"CASE WHEN row_number() OVER {window} = 1 THEN e_{f} ELSE lag({f}) OVER {window} END AS prev_{f}"
        for f in fields
    )


def version_docs_sql(signed_amount: str) -> list[str]:
    """Documents of one version as an array: (doc_seq, role, signed amount, emit, of the previous state or not).
    A REVERSAL credits what the emitted documents add up to; then a FULL or CANCELLATION_FEE invoices the current
    amount; an ADJUSTMENT moves the amount by the difference."""
    return [
        f"named_struct('doc_seq', 1, 'document_role', 'REVERSAL', 'signed', CAST(-x.prev_{signed_amount} AS DECIMAL(18,2)), "
        "'previous', true, 'emit', x.change_type IN ('CANCEL', 'REISSUE'))",
        f"named_struct('doc_seq', 2, 'document_role', CASE WHEN x.is_cancelled THEN 'CANCELLATION_FEE' ELSE 'FULL' END, "
        f"'signed', CAST(x.{signed_amount} AS DECIMAL(18,2)), 'previous', false, "
        "'emit', x.change_type IN ('NEW', 'CANCEL', 'REISSUE'))",
        "named_struct('doc_seq', 1, 'document_role', 'ADJUSTMENT', "
        f"'signed', CAST(x.{signed_amount} - x.prev_{signed_amount} AS DECIMAL(18,2)), 'previous', false, "
        "'emit', x.change_type IN ('INCREASE', 'DECREASE'))",
    ]


def sales_sql(c: str, b: str, day: datetime.date, run_id: str, paid_system: str | None = PAID_SYSTEM) -> list[str]:
    """Statements that MERGE the new sales documents of the candidate bookings into sales_documents. The last
    statement is the MERGE; the ones before it only create temp views (sales_new holds the new documents)."""
    base = f"coalesce(destinations, booking_ref) || coalesce(' commencing ' || date_format(start_date, '{DESCRIPTION_DATE}'), '')"
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
    if paid_system:
        paid = (
            f"CAST(CASE WHEN v.payment_system = '{paid_system}' THEN coalesce(v.payment_amount, 0) ELSE 0 END "
            f"+ CASE WHEN v.second_payment_system = '{paid_system}' THEN coalesce(v.second_payment_amount, 0) ELSE 0 END "
            "AS DECIMAL(18,2))"
        )
    else:
        paid = "CAST(NULL AS DECIMAL(18,2))"
    order = "dwh_version * 1000 + doc_seq"
    prefix = invoice_prefix(day)
    window = "(PARTITION BY booking_ref ORDER BY version)"
    return [
        f"""CREATE OR REPLACE TEMP VIEW code_map AS
        SELECT map_kind, source_key, bc_code FROM (
          SELECT map_kind, source_key, bc_code,
                 row_number() OVER (PARTITION BY map_kind, source_key ORDER BY bc_code) AS rn
          FROM {c}.ref.business_central_code_map)
        WHERE rn = 1""",
        # what the emitted documents of each candidate booking add up to, and the customer, currency, hold and status
        # of its latest document
        f"""CREATE OR REPLACE TEMP VIEW sales_emitted AS
        SELECT external_document_no AS booking_ref, max(dwh_version) AS e_version,
               max_by(doc_seq, {order}) AS e_seq,
               CAST(sum(CASE WHEN document_type = 'Invoice' THEN document_amount ELSE -document_amount END)
                    AS DECIMAL(18,2)) AS e_total,
               max_by(sell_to_customer_no, {order}) AS e_customer_number, max_by(booking_currency, {order}) AS e_currency,
               max_by(hold_reason, {order}) AS e_hold_reason, max_by(booking_status, {order}) AS e_status,
               CAST(sum(coalesce(paid_amount, 0)) AS DECIMAL(18,2)) AS e_paid
        FROM {b}.sales_documents
        WHERE external_document_no IN (SELECT booking_ref FROM candidates)
        GROUP BY external_document_no""",
        # customer and hold rule of build_business_central_tables, with the current mappings, for the versions from
        # the last one that produced a document (every version of a booking without documents)
        f"""CREATE OR REPLACE TEMP VIEW sales_versions AS
        SELECT v.* EXCEPT (services), v.last_update_at AS version_update_at, coalesce(v.total_price, 0) AS total,
               CASE WHEN u.agency_id IS NULL THEN 'AG' || v.agency_id ELSE m.bc_customer_no END AS customer_number,
               CASE WHEN u.agency_id IS NOT NULL AND m.bc_customer_no IS NULL THEN 'CUSTOMER_UNMAPPED'
                    WHEN u.agency_id IS NULL AND ag.agency_id IS NULL THEN 'AGENCY_NOT_IN_SNAPSHOT' END AS hold_reason,
               sp.bc_code AS salesperson_code, {paid} AS paid,
               e.e_version, e.e_seq, e.e_total, e.e_customer_number, e.e_currency, e.e_hold_reason, e.e_status, e.e_paid
        FROM {b}.booking_versions v
        JOIN candidates k ON k.booking_ref = v.booking_ref
        LEFT JOIN sales_emitted e ON e.booking_ref = v.booking_ref
        LEFT JOIN (SELECT DISTINCT agency_id FROM {c}.ref.umbrella_agency) u ON u.agency_id = v.agency_id
        LEFT JOIN (SELECT agency_reference_key, max(bc_customer_no) AS bc_customer_no FROM {c}.ref.customer_map
                   GROUP BY 1) m ON m.agency_reference_key = lower(trim(v.agency_booking_reference))
        LEFT JOIN {c}.silver.agencies ag ON ag.agency_id = v.agency_id
        LEFT JOIN code_map sp ON sp.map_kind = 'SALESPERSON' AND sp.source_key = lower(trim(v.username))
        WHERE v.version >= coalesce(e.e_version, 0)""",
        # Versions sent: from the first BOOKED or RQ version with a positive total; afterwards BOOKED, RQ and CANCELED
        # (row 30: a booking cancelled before it was sent is skipped; PENDING_UPDATE, NOT_BOOKED, BOOK_ERROR are not
        # sent). The first version read is compared with the emitted state, each later one with the version before.
        f"""CREATE OR REPLACE TEMP VIEW sales_changes AS
        WITH f AS (
          SELECT *, min(CASE WHEN status IN {ACTIVE} AND total > 0 THEN version END)
                      OVER (PARTITION BY booking_ref) AS first_sent
          FROM sales_versions),
        e AS (SELECT * FROM f WHERE (e_version IS NOT NULL OR version >= first_sent)
                                AND status IN ('BOOKED', 'RQ', 'CANCELED')),
        p AS (
          SELECT e.*, {previous_sql(window, ["version", "status", "total", "customer_number", "currency", "hold_reason"])}
          FROM e)
        SELECT p.*, status = 'CANCELED' AS is_cancelled, coalesce(version = e_version, false) AS is_correction,
               CASE WHEN prev_version IS NULL THEN 'NEW'
                    WHEN status = 'CANCELED' AND prev_status <> 'CANCELED' THEN 'CANCEL'
                    WHEN NOT (customer_number <=> prev_customer_number AND currency <=> prev_currency)
                         OR (prev_hold_reason IS NOT NULL AND hold_reason IS NULL) THEN 'REISSUE'
                    WHEN total > prev_total THEN 'INCREASE'
                    WHEN total < prev_total THEN 'DECREASE' END AS change_type
        FROM p""",
        # Each document carries a signed amount (positive = Invoice); the documents of a version move the amount
        # invoiced from the previous total to the current one, so the documents of a booking add up to its total.
        f"""CREATE OR REPLACE TEMP VIEW sales_docs AS
        SELECT x.*, d.document_role, d.signed,
               CAST(row_number() OVER (PARTITION BY x.booking_ref, x.version ORDER BY d.doc_seq)
                    + CASE WHEN x.is_correction THEN x.e_seq ELSE 0 END AS INT) AS doc_seq,
               CASE WHEN d.previous THEN x.prev_customer_number ELSE x.customer_number END AS doc_customer,
               CASE WHEN d.previous THEN x.prev_currency ELSE x.currency END AS doc_currency,
               CASE WHEN d.previous THEN x.prev_hold_reason ELSE x.hold_reason END AS doc_hold
        FROM sales_changes x
        LATERAL VIEW explode(filter(array({", ".join(version_docs_sql("total"))}), y -> y.emit AND y.signed <> 0)) t AS d""",
        f"""CREATE OR REPLACE TEMP VIEW sales_new AS
        WITH s AS (
          SELECT x.*, DATE'{day.isoformat()}' AS build_day,
                 CASE WHEN x.signed > 0 THEN 'Invoice' ELSE 'Credit Memo' END AS document_type,
                 CAST(abs(x.signed) AS DECIMAL(18,2)) AS amount,
                 CAST(CASE WHEN x.signed > 0 THEN least(greatest(coalesce(x.payment_amount, 0), 0), x.signed) ELSE 0 END
                      AS DECIMAL(18,2)) AS deposit_amount,
                 it.bc_code AS sales_item, ci.bc_code AS cancellation_item
          FROM sales_docs x
          LEFT JOIN code_map it ON it.map_kind = 'ITEM' AND it.source_key = 'SALES'
          LEFT JOIN code_map ci ON ci.map_kind = 'ITEM' AND ci.source_key = 'CANCELLATION'),
        d AS (SELECT s.*, {document_dates()} FROM s),
        n AS (
          SELECT d.*, {lines} AS sales_lines,
                 document_type = 'Invoice' AND doc_hold IS NULL AS numbered,
                 document_role = 'FULL' AND document_type = 'Invoice' AS full_invoice
          FROM d),
        q AS (
          SELECT n.*,
                 row_number() OVER (PARTITION BY numbered ORDER BY booking_ref, version, doc_seq) AS day_seq,
                 CASE WHEN full_invoice THEN CAST(paid - coalesce(lag(paid) OVER (
                        PARTITION BY booking_ref, full_invoice ORDER BY version, doc_seq), e_paid, 0) AS DECIMAL(18,2))
                      END AS paid_change
          FROM n),
        numbered_before AS (
          SELECT coalesce(max(try_cast(substr(invoice_no, 6) AS INT)), 0) AS last_seq FROM {b}.sales_documents
          WHERE invoice_no LIKE '{prefix}%')
        SELECT booking_ref || '-' || version || '-' || doc_seq AS integration_key, version AS dwh_version, doc_seq,
               change_type, document_role, document_type, booking_ref AS external_document_no,
               doc_customer AS sell_to_customer_no, doc_hold AS hold_reason, order_date, document_date, posting_date,
               CASE WHEN document_role = 'FULL' AND document_type = 'Invoice' AND amount > deposit_amount
                    THEN deferred_limit_date END AS due_date,
               start_date AS service_start_date, end_date AS service_end_date,
               CASE WHEN doc_currency = '{BLANK_CURRENCY}' THEN '' ELSE doc_currency END AS currency_code,
               doc_currency AS booking_currency, CAST(NULL AS STRING) AS agent_currency_code,
               CAST(NULL AS DECIMAL(24,12)) AS currency_factor, referral_id AS your_reference,
               status AS booking_status, cancellation_date, CAST(NULL AS STRING) AS tcode,
               last_update_at AS last_updated_at, pax AS total_passengers, trip_type, destinations, destination_country,
               sales_channel, microsite, main_booking_ref AS main_booking, salesperson_code, user_email AS sell_to_email,
               total AS booking_total_amount, agency_management_fee,
               CASE WHEN paid_change <> 0 THEN paid_change END AS paid_amount,
               CASE WHEN paid_change > 0 THEN '{PAID_METHOD_TEXT}' END AS payment_method,
               CASE WHEN numbered THEN '{prefix}' || CASE WHEN last_seq + day_seq > 999
                                                         THEN CAST(last_seq + day_seq AS STRING)
                                                         ELSE lpad(CAST(last_seq + day_seq AS STRING), 3, '0') END
                    END AS invoice_no,
               aggregate(sales_lines, CAST(0 AS DECIMAL(18,2)),
                         (acc, l) -> CAST(acc + l.quantity * l.unit_price AS DECIMAL(18,2))) AS document_amount,
               sales_lines, current_timestamp() AS emitted_at, '{run_id}' AS emitted_run_id
        FROM q CROSS JOIN numbered_before""",
        f"""MERGE INTO {b}.sales_documents t
        USING sales_new s ON t.integration_key = s.integration_key
        WHEN NOT MATCHED THEN INSERT *""",
    ]


def purchase_sql(c: str, b: str, day: datetime.date, run_id: str) -> list[str]:
    """Statements that MERGE the new purchase documents of the services of the candidate bookings into
    purchase_documents. The last statement is the MERGE; purchase_new holds the new documents. Needs the temp views
    candidates and code_map of sales_sql."""
    provider_vendor = PROVIDER_VENDOR_NUMBER.replace("s.provider", "x.s.provider")
    order = "dwh_version * 1000 + doc_seq"
    window = "(PARTITION BY booking_ref, purchase_key ORDER BY version)"
    return [
        f"""CREATE OR REPLACE TEMP VIEW purchase_emitted AS
        SELECT booking_reference AS booking_ref, purchase_key, max(dwh_version) AS e_version,
               max_by(doc_seq, {order}) AS e_seq,
               CAST(sum(CASE WHEN document_type = 'Invoice' THEN document_amount ELSE -document_amount END)
                    AS DECIMAL(18,2)) AS e_cost,
               max_by(buy_from_vendor_no, {order}) AS e_vendor_number, max_by(cost_currency, {order}) AS e_currency,
               max_by(hold_reason, {order}) AS e_hold_reason, max_by(service_status, {order}) AS e_service_status
        FROM {b}.purchase_documents
        WHERE booking_reference IN (SELECT booking_ref FROM candidates)
        GROUP BY booking_reference, purchase_key""",
        # one row per service and booking version from the first version that holds the service; a service missing
        # from a later version is REMOVED and keeps its last known fields
        f"""CREATE OR REPLACE TEMP VIEW service_states AS
        WITH bv AS (
          SELECT v.* FROM {b}.booking_versions v JOIN candidates k ON k.booking_ref = v.booking_ref),
        sv AS (SELECT bv.booking_ref, bv.version, s FROM bv LATERAL VIEW explode(bv.services) t AS s),
        k AS (SELECT booking_ref, s.purchase_key, min(version) AS first_version FROM sv GROUP BY 1, 2),
        g AS (
          SELECT v.booking_ref, v.version, v.last_update_at AS version_update_at, v.created_at, v.username,
                 v.main_booking_ref, k.purchase_key, sv.s AS found
          FROM bv v
          JOIN k ON k.booking_ref = v.booking_ref AND v.version >= k.first_version
          LEFT JOIN sv ON sv.booking_ref = v.booking_ref AND sv.version = v.version
            AND sv.s.purchase_key = k.purchase_key)
        SELECT booking_ref, version, version_update_at, created_at, username, main_booking_ref, purchase_key,
               found IS NULL AS removed,
               last_value(found, true) OVER (PARTITION BY booking_ref, purchase_key ORDER BY version
                                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s
        FROM g""",
        f"""CREATE OR REPLACE TEMP VIEW purchase_versions AS
        SELECT x.booking_ref, x.version, x.version_update_at, x.purchase_key, x.created_at, x.main_booking_ref,
               {service_category_sql("x.s.service_type")} || '-' || x.booking_ref || '-' || x.s.service_id
                 AS service_reference,
               CASE WHEN x.removed THEN 'REMOVED' ELSE x.s.status END AS service_status,
               CAST(CASE WHEN x.removed THEN 0 ELSE coalesce(x.s.cost_amount, 0) + x.s.amendment_cost_amount END
                    AS DECIMAL(18,2)) AS cost,
               x.s.service_type, x.s.name, x.s.destination_name, x.s.cost_currency AS currency,
               x.s.exchange_rate_microsite, x.s.start_date, x.s.end_date, x.s.cancellation_date,
               coalesce(x.s.last_update_at, x.version_update_at) AS last_update_at,
               coalesce(x.s.supplier_confirmation, x.s.provider_reference) AS vendor_order_no,
               CASE WHEN x.s.supplier_id IS NOT NULL THEN 'SP' || x.s.supplier_id
                    WHEN NOT x.s.is_manual AND x.s.provider IS NOT NULL THEN coalesce(mp.bc_vendor_no, {provider_vendor})
                    ELSE mm.bc_vendor_no END AS vendor_number,
               CASE WHEN x.s.supplier_id IS NULL AND (x.s.is_manual OR x.s.provider IS NULL)
                         AND mm.bc_vendor_no IS NULL THEN 'VENDOR_UNMAPPED'
                    WHEN x.s.supplier_id IS NOT NULL AND sp.supplier_id IS NULL THEN 'SUPPLIER_NOT_IN_SNAPSHOT' END
                 AS hold_reason,
               pc.bc_code AS purchaser_code, bu.bc_code AS business_unit, it.bc_code AS item_no,
               e.e_version, e.e_seq, e.e_cost, e.e_vendor_number, e.e_currency, e.e_hold_reason, e.e_service_status
        FROM service_states x
        LEFT JOIN purchase_emitted e ON e.booking_ref = x.booking_ref AND e.purchase_key = x.purchase_key
        LEFT JOIN (SELECT source_key, max(bc_vendor_no) AS bc_vendor_no FROM {c}.ref.vendor_map
                   WHERE source_kind = 'PROVIDER' GROUP BY 1) mp ON mp.source_key = x.s.provider
        LEFT JOIN (SELECT source_key, max(bc_vendor_no) AS bc_vendor_no FROM {c}.ref.vendor_map
                   WHERE source_kind = 'MANUAL_NAME' GROUP BY 1) mm ON mm.source_key = lower(trim(x.s.supplier_name))
        LEFT JOIN {c}.silver.suppliers sp ON sp.supplier_id = x.s.supplier_id
        LEFT JOIN code_map pc ON pc.map_kind = 'SALESPERSON' AND pc.source_key = lower(trim(x.username))
        LEFT JOIN code_map bu ON bu.map_kind = 'BUSINESS_UNIT' AND bu.source_key = lower(trim(x.username))
        LEFT JOIN code_map it ON it.map_kind = 'ITEM' AND it.source_key = x.s.service_type
        WHERE x.version >= coalesce(e.e_version, 0)""",
        # Sent from the first BOOKED version with a cost (row 34: On Request is not sent, a service cancelled before
        # it was sent is skipped); afterwards BOOKED, CANCELED and REMOVED. Same comparison as sales_changes; a
        # cancelled service credits the cost invoiced and invoices the cost Travel Compositor keeps (row 6 item 4,
        # scenario PO-CAN-02).
        f"""CREATE OR REPLACE TEMP VIEW purchase_changes AS
        WITH f AS (
          SELECT *, min(CASE WHEN service_status = 'BOOKED' AND cost > 0 THEN version END)
                      OVER (PARTITION BY booking_ref, purchase_key) AS first_sent
          FROM purchase_versions),
        e AS (SELECT * FROM f WHERE (e_version IS NOT NULL OR version >= first_sent)
                                AND service_status IN ('BOOKED', 'CANCELED', 'REMOVED')),
        p AS (
          SELECT e.*, {previous_sql(window, ["version", "service_status", "cost", "vendor_number", "currency", "hold_reason"])}
          FROM e)
        SELECT p.*, service_status <> 'BOOKED' AS is_cancelled, coalesce(version = e_version, false) AS is_correction,
               CASE WHEN prev_version IS NULL THEN 'NEW'
                    WHEN service_status <> 'BOOKED' AND prev_service_status = 'BOOKED' THEN 'CANCEL'
                    WHEN NOT (vendor_number <=> prev_vendor_number AND currency <=> prev_currency)
                         OR (prev_hold_reason IS NOT NULL AND hold_reason IS NULL) THEN 'REISSUE'
                    WHEN cost > prev_cost THEN 'INCREASE'
                    WHEN cost < prev_cost THEN 'DECREASE' END AS change_type
        FROM p""",
        f"""CREATE OR REPLACE TEMP VIEW purchase_docs AS
        SELECT x.*, d.document_role, d.signed,
               CAST(row_number() OVER (PARTITION BY x.booking_ref, x.purchase_key, x.version ORDER BY d.doc_seq)
                    + CASE WHEN x.is_correction THEN x.e_seq ELSE 0 END AS INT) AS doc_seq,
               CASE WHEN d.previous THEN x.prev_vendor_number ELSE x.vendor_number END AS doc_vendor,
               CASE WHEN d.previous THEN x.prev_currency ELSE x.currency END AS doc_currency,
               CASE WHEN d.previous THEN x.prev_hold_reason ELSE x.hold_reason END AS doc_hold
        FROM purchase_changes x
        LATERAL VIEW explode(filter(array({", ".join(version_docs_sql("cost"))}), y -> y.emit AND y.signed <> 0)) t AS d""",
        f"""CREATE OR REPLACE TEMP VIEW purchase_new AS
        WITH s AS (
          SELECT *, DATE'{day.isoformat()}' AS build_day,
                 CASE WHEN signed > 0 THEN 'Invoice' ELSE 'Credit Memo' END AS document_type,
                 CAST(abs(signed) AS DECIMAL(18,2)) AS amount
          FROM purchase_docs),
        d AS (
          SELECT s.*, {document_dates()},
                 CASE document_role WHEN 'ADJUSTMENT' THEN 'Cost adjustment - ' WHEN 'REVERSAL' THEN 'Reversal - '
                      WHEN 'CANCELLATION_FEE' THEN 'Cancellation fee - ' ELSE '' END
                 || service_reference || ' ' || {clean_text("name")} AS full_description
          FROM s)
        SELECT service_reference || '-' || version || '-' || doc_seq AS integration_key, version AS dwh_version,
               doc_seq, change_type, document_role, document_type, 'Trading' AS purchase_type, service_reference,
               purchase_key, booking_ref AS booking_reference, doc_vendor AS buy_from_vendor_no,
               doc_hold AS hold_reason, vendor_order_no, CAST(NULL AS STRING) AS vendor_invoice_no,
               CAST(NULL AS STRING) AS vendor_cr_memo_no, order_date, document_date, posting_date,
               start_date AS service_start_date, end_date AS service_end_date,
               CASE WHEN doc_currency = '{BLANK_CURRENCY}' THEN '' ELSE doc_currency END AS currency_code,
               doc_currency AS cost_currency, exchange_rate_microsite AS currency_factor, service_status,
               CAST(NULL AS STRING) AS tcode, last_update_at AS last_updated_at, business_unit,
               main_booking_ref AS main_booking, purchaser_code, cost AS service_total_cost,
               CAST(NULL AS STRING) AS internal_no, amount AS document_amount,
               array(named_struct('type', 'Item', 'no', item_no, 'description', left(trim(full_description), 100),
                                  'description_2', nullif(left(concat_ws(' - ', nullif(trim(substr(full_description, 101)), ''),
                                                                         destination_name), 50), ''),
                                  'quantity', 1, 'direct_unit_cost', amount)) AS purchase_lines,
               current_timestamp() AS emitted_at, '{run_id}' AS emitted_run_id
        FROM d""",
        f"""MERGE INTO {b}.purchase_documents t
        USING purchase_new s ON t.integration_key = s.integration_key
        WHEN NOT MATCHED THEN INSERT *""",
    ]


def checks_sql(b: str, sales_version: int | None = None) -> dict[str, tuple[str, str]]:
    """business_central_ checks of this task: block fails the task, warn is recorded only. Checks on versions and the
    reconciliation read the candidate bookings of the run; the key and invoice number checks read every document.
    sales_version: Delta version of sales_documents before this run's MERGE, for the invoice number check."""
    sales, purch, vers = f"{b}.sales_documents", f"{b}.purchase_documents", f"{b}.booking_versions"
    read = "IN (SELECT booking_ref FROM candidates)"
    latest = f"""(SELECT * FROM (SELECT *, max(version) OVER (PARTITION BY booking_ref) AS last_version
                                  FROM {vers} WHERE booking_ref {read}) WHERE version = last_version)"""
    signed = "CASE WHEN document_type = 'Invoice' THEN document_amount ELSE -document_amount END"
    order = "dwh_version * 1000 + doc_seq"
    checks = {
        "business_central_version_sequence_broken": (
            "block",
            f"""SELECT booking_ref AS k FROM {vers} GROUP BY booking_ref
            HAVING count(*) <> max(version) OR min(version) <> 1 OR count(DISTINCT version) <> count(*)""",
        ),
        "business_central_document_key_unique": (
            "block",
            f"""SELECT integration_key AS k FROM {sales} GROUP BY 1 HAVING count(*) > 1
            UNION ALL SELECT integration_key FROM {purch} GROUP BY 1 HAVING count(*) > 1""",
        ),
        "business_central_invoice_number_invalid": (
            "block",
            f"""SELECT invoice_no AS k FROM {sales} WHERE invoice_no IS NOT NULL GROUP BY 1 HAVING count(*) > 1
            UNION ALL SELECT integration_key FROM {sales}
            WHERE CASE WHEN document_type = 'Invoice' AND hold_reason IS NULL
                       THEN NOT coalesce(invoice_no RLIKE '^[0-9]{{8}}$', false)
                       ELSE invoice_no IS NOT NULL END""",
        ),
        # the signed documents of a booking add up to its total in the last version that produced a document
        "business_central_sales_documents_not_reconciled": (
            "block",
            f"""SELECT external_document_no AS k FROM {sales} WHERE external_document_no {read}
            GROUP BY external_document_no
            HAVING abs(sum({signed}) - max_by(booking_total_amount, {order})) > 0.005""",
        ),
        "business_central_purchase_documents_not_reconciled": (
            "block",
            f"""SELECT service_reference AS k FROM {purch} WHERE booking_reference {read} GROUP BY service_reference
            HAVING abs(sum({signed}) - max_by(service_total_cost, {order})) > 0.005""",
        ),
        "business_central_deposit_balance_differs_from_total": (
            "block",
            f"""SELECT integration_key AS k FROM {sales}
            WHERE document_role = 'FULL' AND document_type = 'Invoice' AND external_document_no {read}
              AND aggregate(filter(sales_lines, l -> l.line_type IN ('Deposit', 'Balance')), CAST(0 AS DECIMAL(18,2)),
                            (acc, l) -> CAST(acc + l.unit_price AS DECIMAL(18,2))) <> document_amount""",
        ),
        "business_central_sales_document_without_destinations": (
            "warn",
            f"SELECT integration_key AS k FROM {sales} WHERE document_role = 'FULL' AND destinations IS NULL",
        ),
        # row 31: the cancellation date is mandatory on a cancellation
        "business_central_cancellation_without_date": (
            "warn",
            f"SELECT integration_key AS k FROM {sales} WHERE change_type = 'CANCEL' AND cancellation_date IS NULL",
        ),
        # the line description then reads '<destinations>' without ' commencing <date>'
        "business_central_sales_document_without_start_date": (
            "warn",
            f"SELECT integration_key AS k FROM {sales} WHERE service_start_date IS NULL",
        ),
        # scenario PO-CAN-02: Finance to confirm that the cost Travel Compositor keeps is the supplier fee
        "business_central_cancelled_service_keeps_cost": (
            "warn",
            f"SELECT DISTINCT service_reference AS k FROM {purch} WHERE document_role = 'CANCELLATION_FEE'",
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
    if sales_version is not None:
        # documents are append-only: an invoice emitted before this run keeps its number (Delta time travel)
        checks["business_central_invoice_number_changed"] = (
            "block",
            f"""SELECT p.integration_key AS k FROM {sales} VERSION AS OF {int(sales_version)} p
            LEFT JOIN {sales} n ON n.integration_key = p.integration_key
            WHERE n.integration_key IS NULL OR NOT (n.invoice_no <=> p.invoice_no)""",
        )
    return checks


def mapping_token(sql, c: str) -> str:
    """Token of the inputs that can change an emitted customer, vendor or hold: the ref mappings, and the ids of
    silver.agencies and silver.suppliers (a hold reason tests only that an id is there). When it changes, the run reads
    every booking. Bronze is not part of it; new versions are found by their landing time."""
    inputs = [
        (f"{c}.ref.umbrella_agency", "*"),
        (f"{c}.ref.customer_map", "*"),
        (f"{c}.ref.vendor_map", "*"),
        (f"{c}.silver.agencies", "agency_id"),
        (f"{c}.silver.suppliers", "supplier_id"),
    ]
    parts = [
        str(
            sql(f"""SELECT count(*) || ':' || coalesce(sum(CAST(xxhash64({cols}) AS DECIMAL(38, 0))), 0)
                    FROM {t}""").first()[0]
        )
        for t, cols in inputs
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def table_version(sql, table: str) -> int:
    """Current Delta version of table: the highest version of its history (one commit per run, 30 days kept)."""
    return max(int(r["version"]) for r in sql(f"DESCRIBE HISTORY {table}").collect())


def main():
    from pyspark.sql import SparkSession

    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="vio_travel")
    ap.add_argument("--run_id", default="")
    ap.add_argument(
        "--rebuild",
        default="false",
        help="true: recompute every version from the whole of bronze and read every booking; emitted documents stay",
    )
    a = ap.parse_args()
    spark = SparkSession.builder.getOrCreate()
    c = a.catalog
    sql = spark.sql
    b = f"{c}.{SCHEMA}"
    rebuild = a.rebuild.lower() == "true"
    day = build_day()

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
    # The documents written before the tables became append-only have other columns and numbers; they are never
    # dropped here (README section Posted documents per booking version).
    for t, columns in (("sales_documents", SALES_COLUMNS), ("purchase_documents", PURCHASE_COLUMNS)):
        if sql(f"SELECT * FROM {b}.{t} LIMIT 0").columns != [n for n, _, _ in columns]:
            raise RuntimeError(f"{b}.{t} has the columns of an earlier version of this task: drop it, then run again")

    token = mapping_token(sql, c)
    last = sql(f"SELECT max_by(silver_token, built_at) AS t FROM {c}.ops.build_state WHERE task = '{TASK}'").first()[
        "t"
    ]
    props = {r["key"]: r["value"] for r in sql(f"SHOW TBLPROPERTIES {b}.booking_versions").collect()}
    watermark = props.get(DOCUMENTS_WATERMARK)
    emitted = sql(f"SELECT count(*) AS n FROM (SELECT 1 FROM {b}.sales_documents LIMIT 1)").first()["n"]
    read_all = rebuild or watermark is None or last != token or not emitted
    if not read_all:
        pending = sql(f"""SELECT count(*) AS n FROM (
            SELECT 1 FROM {c}.bronze.tc_bookings
            WHERE ingested_at > (SELECT coalesce(max(last_seen_ingested_at), TIMESTAMP'1900-01-01 00:00:00')
                                 FROM {b}.booking_versions)
            UNION ALL SELECT 1 FROM {b}.booking_versions WHERE ingested_at > CAST('{watermark}' AS TIMESTAMP)
            LIMIT 1)""").first()["n"]
        if not pending:
            sql(f"INSERT INTO {c}.ops.build_state SELECT '{TASK}', '{a.run_id}', current_timestamp(), '{token}'")
            print("no booking landed and no mapping changed since the last build, nothing to do")
            return

    # ---------------- versions: incremental, or from the whole of bronze on rebuild ---------------------------------
    if rebuild:
        sql(f"TRUNCATE TABLE {b}.booking_versions")
    for q in versions_sql(c, b):
        sql(q)
    # ---------------- documents: appended for the bookings read -----------------------------------------------------
    sql(candidates_sql(b, None if read_all else watermark))
    sales_version = table_version(sql, f"{b}.sales_documents")
    for q in sales_sql(c, b, day, a.run_id) + purchase_sql(c, b, day, a.run_id):
        sql(q)

    # ---------------- audit: data quality ----------------------------------------------------------------------------
    sql(f"""CREATE TABLE IF NOT EXISTS {c}.ops.dq_results (
        run_id STRING, checked_at TIMESTAMP, check_name STRING, severity STRING, failed_rows BIGINT, sample STRING)""")
    rows_out, blocked = [], []
    for name, (severity, q) in checks_sql(b, sales_version).items():
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
        # the watermark stays, so the next run reads the same bookings again; it emits no document twice
        raise RuntimeError(f"blocking data-quality checks failed: {blocked}")
    built = sql(f"SELECT CAST(max(last_seen_ingested_at) AS STRING) AS w FROM {b}.booking_versions").first()["w"]
    if built is not None:
        sql(f"ALTER TABLE {b}.booking_versions SET TBLPROPERTIES ('{DOCUMENTS_WATERMARK}' = '{built}')")
    sql(f"INSERT INTO {c}.ops.build_state SELECT '{TASK}', '{a.run_id}', current_timestamp(), '{token}'")


if __name__ == "__main__":
    main()
