"""Generate genie/vio_travel_genie.geniespace.json, the serialized Genie space of the bundle resource vio_travel_genie.

Edit this script, not the JSON. IDs are derived from the text (uuid5), so regenerating gives the same file.
Run: python3 genie/build_genie_space.py
"""

import json
import sys
import uuid
from pathlib import Path

# Genie data sources are full names and the bundle does not template this file; run with the catalog of the
# target (python3 genie/build_genie_space.py vio_travel_dev) before deploying a target other than prod.
CATALOG = sys.argv[1] if len(sys.argv) > 1 else "vio_travel"
NS = uuid.UUID("6f1c2d4e-0000-4000-8000-000000000001")
TABLES = {
    "gold.obt_booking_service": "One service of a booking with every attribute; use for supplier, destination, "
    "service category and accommodation questions. Filter is_active AND booking_is_active for sold services.",
    "gold.fact_booking": "One booking. is_active = status BOOKED or RQ. Amounts in the booking currency (column "
    "currency): USD except a handful of EUR bookings.",
    "gold.dim_agency": "Agencies (customers of Vio). agency_key -1 = Unknown.",
    "gold.dim_date": "Calendar; date_key is yyyyMMdd.",
    "gold.dim_travel_idea": "Travel ideas (packaged itineraries) linked to bookings by idea_key.",
    "gold.dim_accommodation": "Booked hotels with category, chain and rating on a 10 scale.",
    "gold.fact_pending_payment": "Payments still due, amounts in currency. Overdue when due_date < current_date(); "
    "days overdue = datediff(current_date(), due_date). Ageing buckets are in receivable_metrics.",
    "gold.fact_booking_payment": "Payments recorded on bookings (first, second, additional, manual).",
    "gold.fact_purchase_attempt": "Checkouts started on the microsite; is_converted when they became a booking.",
    "gold.fact_booking_status_change": "History of booking status and amount changes.",
    "gold.booking_metrics": "Metric view: bookings, revenue, gross margin, cancellation rate; dimension Currency.",
    "gold.service_metrics": "Metric view: services, sale amount, cost and margin by supplier and destination, in the "
    "booking currency; dimension Currency.",
    "gold.receivable_metrics": "Metric view: amounts due and overdue as of today by agency and ageing bucket; "
    "dimension Currency.",
}
INSTRUCTIONS = [
    "Vio Travel is a destination management company; bookings come from Travel Compositor.",
    "Revenue is total_price of active bookings (is_active). Gross margin is total_price minus net_provider.",
    "Amounts are in the currency of their row: USD for every booking except a handful in EUR. Never add amounts of "
    "different currencies: group by or filter on the currency column (Currency in the metric views) and show it.",
    "Days overdue and ageing depend on today: use receivable_metrics, or compute from due_date with current_date().",
    "Prefer the metric views booking_metrics, service_metrics and receivable_metrics with MEASURE() for totals.",
    "Personal columns (names, e-mails, phones) are masked for most readers; do not try to unmask them.",
]
QUESTIONS = [
    "How many active bookings and how much revenue per month in 2026?",
    "Which 10 suppliers have the highest sale amount this year?",
    "Which agencies have the most overdue payments, and how much is overdue?",
    "What share of purchase attempts became bookings?",
    "Which hotels were booked most, with their rating?",
]
EXAMPLES = {
    "Revenue and gross margin by month": f"""SELECT `Created month`, Currency, MEASURE(Revenue) AS revenue,
  MEASURE(`Gross margin`) AS gross_margin
FROM {CATALOG}.gold.booking_metrics GROUP BY ALL ORDER BY 1, 2""",
    "Overdue amount by ageing bucket": f"""SELECT `Ageing bucket`, Currency, MEASURE(`Amount to pay`) AS amount_to_pay
FROM {CATALOG}.gold.receivable_metrics GROUP BY ALL ORDER BY 1, 2""",
}


def uid(text: str) -> str:
    return uuid.uuid5(NS, text).hex


def build() -> dict:
    return {
        "version": 2,
        "config": {"sample_questions": [{"id": uid(q), "question": [q]} for q in QUESTIONS]},
        "data_sources": {
            "tables": [{"identifier": f"{CATALOG}.{t}", "description": [d]} for t, d in sorted(TABLES.items())]
        },
        "instructions": {
            "text_instructions": [{"id": uid("instructions"), "content": INSTRUCTIONS}],
            "example_question_sqls": [{"id": uid(q), "question": [q], "sql": [s]} for q, s in EXAMPLES.items()],
        },
    }


if __name__ == "__main__":
    out = Path(__file__).with_name("vio_travel_genie.geniespace.json")
    out.write_text(json.dumps(build(), indent=2) + "\n")
    print(out)
