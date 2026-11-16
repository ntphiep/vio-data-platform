"""Generate dashboards/vio_travel_overview.lvdash.json, the AI/BI dashboard deployed by the bundle.

The dashboard reads the star schema in gold, the integration tables in business_central and the run logs in ops.
Queries name schema.table only; the catalog comes from dataset_catalog in resources/vio_travel_overview.dashboard.yml,
so the dev target reads vio_travel_dev.

Usage: python3 dashboards/build_dashboard.py
Edit this file, not the JSON: the JSON is generated.
"""

import json
from pathlib import Path

OUT = Path(__file__).with_name("vio_travel_overview.lvdash.json")

DATASETS = {
    # ---------------- page: Bookings and revenue ----------------
    "booking_kpis": """
        SELECT count_if(is_active) AS active_bookings,
               sum(CASE WHEN is_active THEN total_price END) AS revenue_usd,
               sum(CASE WHEN is_active THEN gross_margin END) AS gross_margin_usd,
               count_if(is_cancelled) AS cancelled_bookings
        FROM gold.fact_booking""",
    "bookings_by_created_month": """
        SELECT date_trunc('MONTH', d.date) AS created_month, f.status, count(*) AS bookings,
               sum(f.total_price) AS revenue_usd
        FROM gold.fact_booking f JOIN gold.dim_date d ON d.date_key = f.created_date_key
        GROUP BY ALL""",
    "revenue_by_travel_month": """
        SELECT date_trunc('MONTH', d.date) AS travel_month, sum(f.total_price) AS revenue_usd,
               sum(f.gross_margin) AS gross_margin_usd
        FROM gold.fact_booking f JOIN gold.dim_date d ON d.date_key = f.start_date_key
        WHERE f.is_active
        GROUP BY ALL""",
    "top_agencies": """
        SELECT a.agency_name, count(*) AS bookings, sum(f.total_price) AS revenue_usd
        FROM gold.fact_booking f JOIN gold.dim_agency a ON a.agency_key = f.agency_key
        WHERE f.is_active
        GROUP BY ALL ORDER BY revenue_usd DESC LIMIT 15""",
    "bookings_by_trip_type": """
        SELECT trip_type, count(*) AS bookings FROM gold.fact_booking WHERE is_active GROUP BY ALL""",
    "top_destinations": """
        SELECT destination_country, count(DISTINCT booking_ref) AS bookings, count(*) AS services,
               sum(sale_amount) AS sale_usd
        FROM gold.obt_booking_service
        WHERE is_active AND booking_is_active AND destination_country IS NOT NULL
          AND sale_currency = booking_currency
        GROUP BY ALL ORDER BY bookings DESC LIMIT 20""",
    # ---------------- page: Suppliers and cost ----------------
    "supplier_kpis": """
        SELECT count(DISTINCT supplier_key) AS suppliers_used,
               sum(cost_in_booking_currency) AS cost_usd,
               sum(margin_in_booking_currency) / nullif(sum(CASE WHEN margin_in_booking_currency IS NOT NULL
                                                                  THEN sale_amount END), 0) AS service_margin_pct
        FROM gold.obt_booking_service WHERE is_active AND booking_is_active""",
    "cost_by_category": """
        SELECT service_category, sum(cost_in_booking_currency) AS cost_usd,
               sum(CASE WHEN sale_currency = booking_currency THEN sale_amount END) AS sale_usd
        FROM gold.obt_booking_service WHERE is_active AND booking_is_active
        GROUP BY ALL""",
    "top_suppliers": """
        SELECT supplier_name, supplier_source, count(*) AS services, sum(cost_in_booking_currency) AS cost_usd
        FROM gold.obt_booking_service WHERE is_active AND booking_is_active
        GROUP BY ALL ORDER BY cost_usd DESC LIMIT 20""",
    "cost_by_destination_country": """
        SELECT destination_country, sum(cost_in_booking_currency) AS cost_usd
        FROM gold.obt_booking_service
        WHERE is_active AND booking_is_active AND destination_country IS NOT NULL
        GROUP BY ALL ORDER BY cost_usd DESC LIMIT 15""",
    "supplier_source_mix": """
        SELECT supplier_source, count(*) AS services, sum(cost_in_booking_currency) AS cost_usd
        FROM gold.obt_booking_service WHERE is_active AND booking_is_active GROUP BY ALL""",
    # ---------------- page: Pipeline operations ----------------
    "extract_runs": """
        SELECT finished_at, entity, status, records, pages, round(bytes / 1048576, 2) AS mb,
               distinct_keys, expected, window_from, window_to
        FROM ops.ingest_runs WHERE finished_at >= timestampadd(DAY, -7, current_timestamp())""",
    "freshness": """
        SELECT timestampdiff(MINUTE, (SELECT max(finished_at) FROM ops.ingest_runs
                                      WHERE entity = 'bookings' AND status = 'SUCCESS'), current_timestamp())
                 AS minutes_since_extract,
               timestampdiff(MINUTE, (SELECT max(synced_at) FROM ops.bc_sync), current_timestamp())
                 AS minutes_since_push,
               (SELECT max_by(failed_rows, checked_at) FROM ops.dq_results
                WHERE check_name = 'e2e_sales_waiting_for_push') AS sales_waiting_for_push""",
    # latest result of every check that ran in the last day; star_schema and bc_tables skip unchanged runs,
    # so their latest results can come from an earlier run than data_quality's
    "dq_latest": """
        SELECT check_name, severity, failed_rows, sample, checked_at, run_id FROM (
          SELECT *, row_number() OVER (PARTITION BY check_name ORDER BY checked_at DESC) AS rn
          FROM ops.dq_results WHERE checked_at >= timestampadd(DAY, -1, current_timestamp()))
        WHERE rn = 1 ORDER BY severity, failed_rows DESC""",
    "bc_sync_daily": """
        SELECT date_trunc('DAY', synced_at) AS day, result, count(*) AS documents
        FROM ops.bc_sync GROUP BY ALL""",
    "bc_held": """
        SELECT 'sales invoice' AS document, hold_reason, count(*) AS documents
        FROM business_central.sales_invoices WHERE hold_reason IS NOT NULL GROUP BY ALL
        UNION ALL
        SELECT 'purchase invoice', hold_reason, count(*)
        FROM business_central.purchase_invoices WHERE hold_reason IS NOT NULL GROUP BY ALL""",
    "publish_log": """
        SELECT published_at, run_id, versions FROM ops.publish
        WHERE published_at >= timestampadd(DAY, -7, current_timestamp())""",
}


def field(name, expr=None):
    return {"name": name, "expression": expr or f"`{name}`"}


def widget(name, dataset, fields, spec, pos):
    return {
        "widget": {
            "name": name,
            "queries": [
                {"name": "main_query", "query": {"datasetName": dataset, "fields": fields, "disaggregated": False}}
            ],
            "spec": spec,
        },
        "position": dict(zip(("x", "y", "width", "height"), pos, strict=True)),
    }


def counter(name, dataset, col, title, pos):
    agg = f"sum({col})"
    return widget(
        name,
        dataset,
        [field(agg, f"SUM(`{col}`)")],
        {
            "version": 2,
            "widgetType": "counter",
            "encodings": {"value": {"fieldName": agg, "displayName": title}},
            "frame": {"showTitle": True, "title": title},
        },
        pos,
    )


def bar(name, dataset, x, y, title, pos, color=None, temporal=False, horizontal=False):
    y_agg = f"sum({y})"
    fields = [field(x), field(y_agg, f"SUM(`{y}`)")]
    enc = {
        "x": {"fieldName": x, "displayName": x, "scale": {"type": "temporal" if temporal else "categorical"}},
        "y": {"fieldName": y_agg, "displayName": y, "scale": {"type": "quantitative"}},
    }
    if horizontal:
        enc = {
            "y": {"fieldName": x, "displayName": x, "scale": {"type": "categorical", "sort": {"by": "x-reversed"}}},
            "x": {"fieldName": y_agg, "displayName": y, "scale": {"type": "quantitative"}},
        }
    if color:
        fields.append(field(color))
        enc["color"] = {"fieldName": color, "displayName": color, "scale": {"type": "categorical"}}
    return widget(
        name,
        dataset,
        fields,
        {"version": 3, "widgetType": "bar", "encodings": enc, "frame": {"showTitle": True, "title": title}},
        pos,
    )


def line(name, dataset, x, y, title, pos):
    fields = [field(x), field(f"sum({y})", f"SUM(`{y}`)")]
    enc = {
        "x": {"fieldName": x, "displayName": x, "scale": {"type": "temporal"}},
        "y": {"fieldName": f"sum({y})", "displayName": y, "scale": {"type": "quantitative"}},
    }
    return widget(
        name,
        dataset,
        fields,
        {"version": 3, "widgetType": "line", "encodings": enc, "frame": {"showTitle": True, "title": title}},
        pos,
    )


def pie(name, dataset, label, value, title, pos):
    v = f"sum({value})"
    return widget(
        name,
        dataset,
        [field(label), field(v, f"SUM(`{value}`)")],
        {
            "version": 3,
            "widgetType": "pie",
            "encodings": {
                "angle": {"fieldName": v, "displayName": value, "scale": {"type": "quantitative"}},
                "color": {"fieldName": label, "displayName": label, "scale": {"type": "categorical"}},
            },
            "frame": {"showTitle": True, "title": title},
        },
        pos,
    )


def table(name, dataset, cols, title, pos):
    return widget(
        name,
        dataset,
        [field(c) for c in cols],
        {
            "version": 1,
            "widgetType": "table",
            "itemsPerPage": 25,
            "condensed": True,
            "withRowNumber": False,
            "invisibleColumns": [],
            "allowHTMLByDefault": False,
            "paginationSize": "default",
            # column properties copied from a table widget exported from this workspace
            "encodings": {
                "columns": [
                    {
                        "fieldName": c,
                        "displayName": c,
                        "title": c,
                        "order": 10000 + i,
                        "visible": True,
                        "type": "string",
                        "displayAs": "string",
                        "alignContent": "left",
                        "allowHTML": False,
                        "allowSearch": False,
                        "booleanValues": ["false", "true"],
                        "dateTimeFormat": "YYYY-MM-DD HH:mm",
                        "highlightLinks": False,
                        "imageHeight": "",
                        "imageTitleTemplate": "{{ @ }}",
                        "imageUrlTemplate": "{{ @ }}",
                        "imageWidth": "",
                        "linkOpenInNewTab": True,
                        "linkTextTemplate": "{{ @ }}",
                        "linkTitleTemplate": "{{ @ }}",
                        "linkUrlTemplate": "{{ @ }}",
                        "preserveWhitespace": False,
                        "useMonospaceFont": False,
                    }
                    for i, c in enumerate(cols)
                ]
            },
            "frame": {"showTitle": True, "title": title},
        },
        pos,
    )


def text(name, lines, pos):
    return {
        "widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
        "position": dict(zip(("x", "y", "width", "height"), pos, strict=True)),
    }


PAGES = [
    (
        "bookings_revenue",
        "Bookings and revenue",
        [
            text(
                "t_bookings",
                [
                    "## Bookings and revenue\n",
                    "Source: gold.fact_booking, gold.dim_date, gold.dim_agency, gold.obt_booking_service. "
                    "Active = status BOOKED or RQ. All bookings are priced in USD.\n",
                ],
                (0, 0, 6, 2),
            ),
            counter("c_active", "booking_kpis", "active_bookings", "Active bookings", (0, 2, 2, 3)),
            counter("c_revenue", "booking_kpis", "revenue_usd", "Revenue of active bookings (USD)", (2, 2, 2, 3)),
            counter("c_margin", "booking_kpis", "gross_margin_usd", "Gross margin (USD)", (4, 2, 2, 3)),
            bar(
                "b_created",
                "bookings_by_created_month",
                "created_month",
                "bookings",
                "Bookings by creation month",
                (0, 5, 6, 7),
                color="status",
                temporal=True,
            ),
            line(
                "l_travel",
                "revenue_by_travel_month",
                "travel_month",
                "revenue_usd",
                "Revenue of active bookings by travel start month (USD)",
                (0, 12, 6, 7),
            ),
            bar(
                "b_agencies",
                "top_agencies",
                "agency_name",
                "revenue_usd",
                "Top 15 agencies by revenue (USD)",
                (0, 19, 3, 9),
                horizontal=True,
            ),
            pie(
                "p_trip",
                "bookings_by_trip_type",
                "trip_type",
                "bookings",
                "Active bookings by trip type",
                (3, 19, 3, 9),
            ),
            table(
                "t_dest",
                "top_destinations",
                ["destination_country", "bookings", "services", "sale_usd"],
                "Top destination countries",
                (0, 28, 6, 8),
            ),
        ],
    ),
    (
        "suppliers_cost",
        "Suppliers and cost",
        [
            text(
                "t_suppliers",
                [
                    "## Suppliers and cost\n",
                    "Source: gold.obt_booking_service. Cost is shown in the booking currency (USD), "
                    "converted by Travel Compositor. Active services of active bookings only.\n",
                ],
                (0, 0, 6, 2),
            ),
            counter("c_suppliers", "supplier_kpis", "suppliers_used", "Suppliers used", (0, 2, 2, 3)),
            counter("c_cost", "supplier_kpis", "cost_usd", "Service cost (USD)", (2, 2, 2, 3)),
            counter(
                "c_margin_pct", "supplier_kpis", "service_margin_pct", "Service margin (share of sale)", (4, 2, 2, 3)
            ),
            bar(
                "b_category",
                "cost_by_category",
                "service_category",
                "cost_usd",
                "Cost by service category (USD)",
                (0, 5, 3, 8),
            ),
            pie(
                "p_source",
                "supplier_source_mix",
                "supplier_source",
                "services",
                "Services by supplier source",
                (3, 5, 3, 8),
            ),
            bar(
                "b_suppliers",
                "top_suppliers",
                "supplier_name",
                "cost_usd",
                "Top 20 suppliers by cost (USD)",
                (0, 13, 3, 10),
                horizontal=True,
            ),
            bar(
                "b_country",
                "cost_by_destination_country",
                "destination_country",
                "cost_usd",
                "Cost by destination country (USD)",
                (3, 13, 3, 10),
                horizontal=True,
            ),
            table(
                "t_suppliers_detail",
                "top_suppliers",
                ["supplier_name", "supplier_source", "services", "cost_usd"],
                "Top suppliers",
                (0, 23, 6, 8),
            ),
        ],
    ),
    (
        "pipeline_ops",
        "Pipeline operations",
        [
            text(
                "t_ops",
                [
                    "## Pipeline operations\n",
                    "Source: ops.ingest_runs, ops.dq_results, ops.publish, ops.bc_sync, business_central.*. "
                    "Job vio_travel_pipeline runs every 30 minutes, at minute 05 and 35; local/push_to_bc.py "
                    "pushes within 5 minutes after each finished run.\n",
                ],
                (0, 0, 6, 2),
            ),
            counter("c_extract", "freshness", "minutes_since_extract", "Minutes since the last extract", (0, 2, 2, 3)),
            counter("c_push", "freshness", "minutes_since_push", "Minutes since the last push", (2, 2, 2, 3)),
            counter(
                "c_waiting", "freshness", "sales_waiting_for_push", "Sales invoices waiting for the push", (4, 2, 2, 3)
            ),
            bar(
                "b_extract",
                "extract_runs",
                "finished_at",
                "records",
                "Records extracted per run (last 7 days)",
                (0, 5, 6, 7),
                color="entity",
                temporal=True,
            ),
            table(
                "t_dq",
                "dq_latest",
                ["check_name", "severity", "failed_rows", "sample", "checked_at"],
                "Data-quality checks of the latest job run",
                (0, 12, 6, 10),
            ),
            bar(
                "b_sync",
                "bc_sync_daily",
                "day",
                "documents",
                "Documents pushed into Business Central per day",
                (0, 22, 3, 8),
                color="result",
                temporal=True,
            ),
            bar(
                "b_held",
                "bc_held",
                "hold_reason",
                "documents",
                "Documents held, by reason",
                (3, 22, 3, 8),
                color="document",
            ),
            table(
                "t_runs",
                "extract_runs",
                [
                    "finished_at",
                    "entity",
                    "status",
                    "records",
                    "distinct_keys",
                    "expected",
                    "pages",
                    "mb",
                    "window_from",
                    "window_to",
                ],
                "Extract runs (last 7 days)",
                (0, 30, 6, 8),
            ),
            table(
                "t_publish",
                "publish_log",
                ["published_at", "run_id", "versions"],
                "Business Central tables published (last 7 days)",
                (0, 38, 6, 6),
            ),
        ],
    ),
]


def build():
    datasets = [
        {"name": k, "displayName": k, "queryLines": [ln.strip() + "\n" for ln in v.strip().splitlines()]}
        for k, v in DATASETS.items()
    ]
    pages = [{"name": n, "displayName": d, "pageType": "PAGE_TYPE_CANVAS", "layout": lay} for n, d, lay in PAGES]
    used = {
        w["widget"]["queries"][0]["query"]["datasetName"]
        for _, _, lay in PAGES
        for w in lay
        if "queries" in w["widget"]
    }
    missing = used - DATASETS.keys()
    if missing:
        raise SystemExit(f"widgets use unknown datasets: {missing}")
    OUT.write_text(json.dumps({"datasets": datasets, "pages": pages}, indent=2) + "\n")
    print(f"written {OUT} ({len(datasets)} datasets, {sum(len(p[2]) for p in PAGES)} widgets)")


if __name__ == "__main__":
    build()
