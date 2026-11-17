"""Unit tests for the Business Central push helpers (no network, no Databricks).
Run: uv run --no-project --with pytest --with requests pytest pipeline/tests"""

import json
import sys
from pathlib import Path

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "local"))
sys.path.insert(0, str(ROOT / "src"))
import extend_number_series as ns  # noqa: E402
import push_to_bc as m  # noqa: E402

import bc_tables  # noqa: E402


class Resp:
    def __init__(self, status, payload=None, headers=None):
        self.status_code, self.headers = status, headers or {}
        self._payload = payload if payload is not None else {}
        self.content = b"x"
        self.text = str(self._payload)

    @property
    def ok(self):
        return self.status_code < 400

    def json(self):
        return self._payload

    def raise_for_status(self):
        if not self.ok:
            raise requests.HTTPError(str(self.status_code))


class Session:
    def __init__(self, outcomes):
        self.outcomes, self.calls, self.headers, self.log = list(outcomes), [], {}, []

    def request(self, method, url, **kw):
        self.calls.append(method)
        self.log.append((method, url, kw))
        o = self.outcomes.pop(0)
        if isinstance(o, Exception):
            raise o
        return o


def bc(outcomes, monkeypatch):
    monkeypatch.setattr(m.BC, "_token", lambda self: self.s.headers.update({"Authorization": "Bearer t"}))
    sleeps = []
    cfg = {"bc_environment": "Production", "bc_company": "My Company", "bc_tenant": "x", "bc_local_currency": "AUD"}
    s = Session([Resp(200, {"value": [{"id": "c1", "name": "My Company"}]})] + outcomes)
    client = m.BC(cfg, session=s, sleep=sleeps.append)
    s.calls.clear()
    s.log.clear()
    return client, s, sleeps


def batch_ops(s, n=-1):
    """(method, url) of the inner requests of the n-th logged $batch call."""
    method, url, kw = s.log[n]
    assert method == "POST" and url.endswith("/api/v2.0/$batch")
    assert kw["headers"]["Isolation"] == "snapshot"
    return [(r["method"], r["url"]) for r in json.loads(kw["data"])["requests"]]


def ok_batch(n):
    return Resp(200, {"responses": [{"id": str(i), "status": 204} for i in range(1, n + 1)]})


def test_lit_escapes_quotes_and_backslashes():
    assert m.lit("O'Brien \\ x") == "'O\\'Brien \\\\ x'"
    assert m.lit(None) == "NULL"


def test_retry_after_seconds_and_http_date():
    assert m.retry_after_seconds("7", 1) == 7.0
    assert 1 <= m.retry_after_seconds("Wed, 21 Oct 2015 07:28:00 GMT", 1) <= 120
    assert m.retry_after_seconds(None, 3) == 16


def test_post_is_not_retried_on_503(monkeypatch):
    client, s, sleeps = bc([Resp(503)], monkeypatch)
    r = client.call("POST", "https://x/salesInvoices")
    assert r.status_code == 503 and s.calls == ["POST"] and sleeps == []


def test_post_is_retried_on_429(monkeypatch):
    client, s, sleeps = bc([Resp(429, headers={"Retry-After": "3"}), Resp(201, {"id": 1})], monkeypatch)
    assert client.call("POST", "https://x/salesInvoices").status_code == 201 and sleeps == [3.0]


def test_get_is_retried_on_network_error_and_502(monkeypatch):
    client, s, sleeps = bc([requests.ConnectionError("x"), Resp(502), Resp(200)], monkeypatch)
    assert client.call("GET", "https://x/a").status_code == 200 and len(sleeps) == 2


def test_post_network_error_is_raised_not_retried(monkeypatch):
    client, s, _ = bc([requests.ReadTimeout("x")], monkeypatch)
    with pytest.raises(requests.ReadTimeout):
        client.call("POST", "https://x/a")
    assert s.calls == ["POST"]


def test_401_renews_token_once(monkeypatch):
    client, s, sleeps = bc([Resp(401), Resp(200)], monkeypatch)
    assert client.call("GET", "https://x/a").status_code == 200 and sleeps == []


def test_find_rejects_duplicates(monkeypatch):
    client, _, _ = bc([Resp(200, {"value": [{"id": 1}, {"id": 2}]})], monkeypatch)
    with pytest.raises(RuntimeError, match="DUPLICATE_IN_BC"):
        client.find("salesInvoices", "externalDocumentNumber", "EXA-1")


def test_find_ignores_a_posted_empty_invoice_under_the_same_number(monkeypatch):
    rows = [
        {"id": "p1", "status": "Paid", "totalAmountIncludingTax": 0},
        {"id": "d1", "status": "Draft", "totalAmountIncludingTax": 1340.5},
    ]
    client, _, _ = bc([Resp(200, {"value": rows})], monkeypatch)
    assert client.find("salesInvoices", "externalDocumentNumber", "EXA-1")["id"] == "d1"
    two_live = [{"id": "p1", "status": "Paid", "totalAmountIncludingTax": 10}, rows[1]]
    client, _, _ = bc([Resp(200, {"value": two_live})], monkeypatch)
    with pytest.raises(RuntimeError, match="DUPLICATE_IN_BC"):
        client.find("salesInvoices", "externalDocumentNumber", "EXA-1")


def test_posted_document_is_never_changed(monkeypatch):
    client, s, _ = bc([Resp(200, {"value": [{"id": "d1", "status": "Paid"}]})], monkeypatch)
    result, _ = m.push_document(client, "sales", "externalDocumentNumber", "EXA-1", {}, [])
    assert result == "NEEDS_CREDIT_MEMO" and s.calls == ["GET"]


def test_cancel_empties_draft_lines_without_deleting_header(monkeypatch):
    # the line deletes now go in one transactional $batch (finding 5); the header is still never deleted
    client, s, _ = bc(
        [
            Resp(200, {"value": [{"id": "d1", "status": "Draft"}]}),
            Resp(200, {"value": [{"id": "l1"}, {"id": "l2"}]}),
            ok_batch(2),
        ],
        monkeypatch,
    )
    result, _ = m.push_document(client, "sales", "externalDocumentNumber", "EXA-1", {}, [], cancel=True)
    assert result == "CANCELLED_DRAFT_EMPTIED" and s.calls == ["GET", "GET", "POST"]
    assert batch_ops(s) == [
        ("DELETE", "companies(c1)/salesInvoices(d1)/salesInvoiceLines(l1)"),
        ("DELETE", "companies(c1)/salesInvoices(d1)/salesInvoiceLines(l2)"),
    ]


def test_exchange_rate_missing_at_source_skips_only_that_currency(monkeypatch):
    # Business Central has no rate for FJD; the rate source answers 404 for FJD, so no package is created
    client, s, _ = bc([Resp(200, {"value": []})], monkeypatch)
    client.cfg["fx_source"] = "https://fx.example"
    monkeypatch.setattr(m.requests, "get", lambda url, params, timeout: Resp(404))
    loaded, unavailable = client.ensure_exchange_rates({("FJD", "2026-11-09"), ("AUD", "2026-11-09")})
    assert loaded == []
    assert unavailable == {("FJD", "2026-11-09")}
    assert s.calls == ["GET"]


def test_exchange_rate_missing_at_source_uses_fallback(monkeypatch):
    # the ECB has no FJD; the Travel Compositor fallback supplies it and the package carries that value
    client, _, _ = bc([Resp(200, {"value": []})], monkeypatch)
    client.cfg["fx_source"] = "https://fx.example"
    monkeypatch.setattr(m.requests, "get", lambda url, params, timeout: Resp(404))
    seen = []
    monkeypatch.setattr(client, "apply_package", lambda *a: seen.append(a))
    asked = []

    def fallback(pairs):
        asked.extend(pairs)
        return {("FJD", "2026-11-09"): "1.468000"}

    loaded, unavailable = client.ensure_exchange_rates({("FJD", "2026-11-09")}, fallback=fallback)
    assert asked == [("FJD", "2026-11-09")]
    assert loaded == [("FJD", "2026-11-09", "1.468000", "Travel Compositor")] and unavailable == set()
    assert "<ExchangeRateAmount>1.468000</ExchangeRateAmount>" in seen[0][3]


def test_tc_implied_rate_converts_the_usd_median_to_lcy(monkeypatch):
    class FakeDB:
        def __init__(self):
            self.sql = ""

        def q(self, sql):
            self.sql = sql
            return [{"cur": "VND", "day": "2026-10-05", "per_usd": "26000.0", "services": "12"}]

    monkeypatch.setattr(m.requests, "get", lambda url, params, timeout: Resp(200, {"rates": {"USD": 0.65}}))
    db = FakeDB()
    cfg = {"bc_local_currency": "AUD", "catalog": "vio_travel", "fx_source": "https://fx.example"}
    out = m.tc_implied_rates(db, cfg, [("VND", "2026-10-05")])
    assert out == {("VND", "2026-10-05"): "16900.000000"}
    assert "('VND', DATE'2026-10-05')" in db.sql and "cost_in_sale_currency >= 10" in db.sql
    assert m.tc_implied_rates(db, cfg, []) == {}


# ------------------------------------------------------------------ planning (findings 1, 4, 7, 13)
def doc(**kw):
    row = {"payload_hash": "H1", "doc_action": "UPSERT", "hold_reason": None, "failures": "0"}
    row.update(kw)
    return row


def test_document_back_to_an_earlier_synced_hash_is_sent_again():
    # H1 synced, then H2 synced, then the booking returns to H1: only the latest attempt counts
    assert m.doc_decision(doc(attempt_hash="H2", attempt_result="UPDATED"), None) == "SEND"
    assert m.doc_decision(doc(attempt_hash="H1", attempt_result="UPDATED"), None) == "DONE"
    st = m.state_sql("vio_travel", "'My Company'")
    assert "max_by(payload_hash, synced_at) FILTER (WHERE result IN" in st


def test_failed_latest_attempt_is_retried_even_when_an_older_attempt_had_the_same_hash():
    assert m.doc_decision(doc(attempt_hash="H1", attempt_result="FAILED", failures="1"), None) == "SEND"


@pytest.mark.parametrize("result", ["NEEDS_CREDIT_MEMO", "NOTHING_TO_CANCEL", "NO_EXCHANGE_RATE", "REMOVED_POSTED"])
def test_terminal_results_are_not_retried_while_the_hash_is_unchanged(result):
    assert m.doc_decision(doc(attempt_hash="H1", attempt_result=result), None) == "DONE"
    assert m.doc_decision(doc(attempt_hash="H0", attempt_result=result), None) == "SEND"


def test_transient_results_become_dead_letters_after_max_failures():
    assert "SKIPPED" in m.TRANSIENT and "NOT_EDITABLE" in m.TRANSIENT
    assert m.doc_decision(doc(failures=str(m.MAX_FAILURES - 1)), None) == "SEND"
    assert m.doc_decision(doc(failures=str(m.MAX_FAILURES)), None) == "DEAD"


def test_retry_option_resends_a_terminal_or_dead_result():
    no_rate = doc(attempt_hash="H1", attempt_result="NO_EXCHANGE_RATE")
    assert m.doc_decision(no_rate, None) == "DONE"
    assert m.doc_decision(no_rate, None, frozenset({"NO_EXCHANGE_RATE"})) == "SEND"
    dead = doc(attempt_hash="H1", attempt_result="FAILED", failures=str(m.MAX_FAILURES))
    assert m.doc_decision(dead, None) == "DEAD"
    assert m.doc_decision(dead, None, frozenset({"FAILED"})) == "SEND"
    assert m.doc_decision(no_rate, "VENDOR_UNMAPPED", frozenset({"NO_EXCHANGE_RATE"})) == "HELD"


def test_held_row_is_logged_only_when_hash_or_reason_changes():
    logged = doc(log_result="HELD", log_hash="H1", log_error="VENDOR_UNMAPPED")
    assert m.doc_decision(logged, "VENDOR_UNMAPPED") == "HELD_LOGGED"
    assert m.doc_decision({**logged, "payload_hash": "H2"}, "VENDOR_UNMAPPED") == "HELD"
    assert m.doc_decision(logged, "SUPPLIER_NOT_IN_SNAPSHOT") == "HELD"


def test_purchase_of_held_sales_invoice_is_recorded_held_with_reason():
    sales = [doc(external_document_number="B1", hold_reason="CUSTOMER_UNMAPPED", last_update_at="2026-11-01")]
    purchases = [doc(purchase_key="7-H", booking_ref="B1", sales_hold_reason="CUSTOMER_UNMAPPED", payload_hash="P1")]
    todo, held, pending = m.plan(sales, purchases, 10)
    assert todo == [] and pending == 0
    assert held == [
        ("sales", "B1", "H1", "CUSTOMER_UNMAPPED"),
        ("purchase", "7-H", "P1", "SALES_HELD CUSTOMER_UNMAPPED"),
    ]


def test_plan_skips_done_bookings_before_the_limit_so_new_ones_are_not_starved():
    done = [
        doc(
            external_document_number=f"OLD{n}",
            attempt_hash="H1",
            attempt_result="NEEDS_CREDIT_MEMO",
            last_update_at=f"2026-01-0{n}",
        )
        for n in range(1, 4)
    ]
    new = doc(external_document_number="NEW", last_update_at="2026-11-01")
    todo, _, pending = m.plan(done + [new], [], 2)
    assert [h["external_document_number"] for h in todo] == ["NEW"] and pending == 1


# ------------------------------------------------------------------ scope (finding 2)
def test_scope_includes_bookings_changed_recently_and_the_column_exists():
    scope = m.load_cfg()["scope_sql"]
    assert "i.last_update_at" in scope and " OR " in scope
    src = (ROOT / "src" / "bc_tables.py").read_text()
    sales_invoices = src[src.index("CREATE OR REPLACE TABLE {b}.sales_invoices") :]
    assert "b.last_update_at" in sales_invoices[: sales_invoices.index("FROM booking_customer")]


# ------------------------------------------------------------------ removed documents (finding 3)
def test_removed_draft_is_emptied_not_deleted(monkeypatch):
    client, s, _ = bc(
        [
            Resp(200, {"value": [{"id": "d1", "status": "Draft"}]}),
            Resp(200, {"value": [{"id": "l1"}]}),
            ok_batch(1),
        ],
        monkeypatch,
    )
    result, _ = m.remove_document(client, "purchase", "42-H")
    assert result == "REMOVED_DRAFT_EMPTIED"
    assert "vendorInvoiceNumber eq '42-H'" in s.log[0][2]["params"]["$filter"]
    assert batch_ops(s) == [("DELETE", "companies(c1)/purchaseInvoices(d1)/purchaseInvoiceLines(l1)")]


def test_removed_posted_document_is_only_recorded(monkeypatch):
    client, s, _ = bc([Resp(200, {"value": [{"id": "d1", "status": "Open"}]})], monkeypatch)
    rows = []
    assert m.process_removals(client, [{"doc_type": "sales", "doc_key": "B1"}], 50, lambda *a: rows.append(a))
    assert s.calls == ["GET"]
    assert rows[0][:3] == ("sales", "B1", m.REMOVED_HASH) and rows[0][4] == "REMOVED_POSTED"


def test_removed_document_not_found_is_recorded(monkeypatch):
    client, _, _ = bc([Resp(200, {"value": []})], monkeypatch)
    assert m.remove_document(client, "sales", "B1") == ("REMOVED_NOT_FOUND", None)


def test_too_many_removals_change_nothing(monkeypatch):
    client, s, _ = bc([], monkeypatch)
    rows = [{"doc_type": "sales", "doc_key": f"B{n}"} for n in range(3)]
    recorded = []
    assert not m.process_removals(client, rows, 2, lambda *a: recorded.append(a))
    assert s.calls == [] and recorded == []


def test_removed_documents_query_skips_closed_keys_and_present_keys():
    g = {t: f"cat.business_central.{t} VERSION AS OF 1" for t in ("sales_invoices", "purchase_invoices")}
    q = m.doc_queries("cat", "'My Company'", g, "TRUE")["removed"]
    assert "LEFT ANTI JOIN" in q and "st.ever_sent" in q
    assert all(r in q for r in m.CLOSED)


# ------------------------------------------------------------------ atomic draft update (finding 5)
def test_draft_update_is_one_transactional_batch(monkeypatch):
    client, s, _ = bc(
        [
            Resp(200, {"value": [{"id": "d1", "status": "Draft"}]}),
            Resp(200, {"value": [{"id": "l1"}]}),
            ok_batch(3),
            Resp(200, {"id": "d1", "number": "S-1"}),
        ],
        monkeypatch,
    )
    result, rec = m.push_document(
        client, "sales", "externalDocumentNumber", "B1", {"customerNumber": "AG1"}, [{"description": "x"}]
    )
    assert result == "UPDATED" and rec["number"] == "S-1"
    assert s.calls == ["GET", "GET", "POST", "GET"]
    assert batch_ops(s, 2) == [
        ("DELETE", "companies(c1)/salesInvoices(d1)/salesInvoiceLines(l1)"),
        ("PATCH", "companies(c1)/salesInvoices(d1)"),
        ("POST", "companies(c1)/salesInvoices(d1)/salesInvoiceLines"),
    ]


def test_failed_inner_request_raises(monkeypatch):
    client, _, _ = bc(
        [
            Resp(
                200,
                {
                    "responses": [
                        {"id": "1", "status": 204},
                        {"id": "2", "status": 400, "body": {"error": {"message": "bad"}}},
                    ]
                },
            )
        ],
        monkeypatch,
    )
    with pytest.raises(RuntimeError, match="rolled back: request 2 400 bad"):
        client.batch([("DELETE", "a", None), ("POST", "b", {})])


def test_batch_over_the_limit_is_refused_before_any_call(monkeypatch):
    client, s, _ = bc([], monkeypatch)
    with pytest.raises(RuntimeError, match="exceed the \\$batch limit"):
        client.batch([("POST", "x", {})] * (m.MAX_BATCH + 1))
    assert s.calls == []


# ------------------------------------------------------------------ exchange rates (findings 6, 8)
def test_one_failing_currency_does_not_block_the_others(monkeypatch):
    client, _, _ = bc([Resp(200, {"value": []}), Resp(200, {"value": []})], monkeypatch)
    client.cfg["fx_source"] = "https://fx.example"
    monkeypatch.setattr(m.requests, "get", lambda url, params, timeout: Resp(200, {"rates": {params["symbols"]: 0.5}}))

    def apply_package(prefix, table_id, list_tag, records_xml):
        if "USD" in prefix:
            raise RuntimeError("package USD failed")

    monkeypatch.setattr(client, "apply_package", apply_package)
    errors = {}
    loaded, unavailable = client.ensure_exchange_rates({("EUR", "2026-11-09"), ("USD", "2026-11-09")}, errors)
    assert loaded == [("EUR", "2026-11-09", "0.5", "ECB")] and unavailable == set()
    assert list(errors) == ["USD"]


def test_exchange_rate_goes_in_with_full_precision(monkeypatch):
    client, _, _ = bc([Resp(200, {"value": []})], monkeypatch)
    client.cfg["fx_source"] = "https://fx.example"
    monkeypatch.setattr(m.requests, "get", lambda url, params, timeout: Resp(200, {"rates": {"JPY": 97.1234567891}}))
    seen = []
    monkeypatch.setattr(client, "apply_package", lambda *a: seen.append(a))
    loaded, _ = client.ensure_exchange_rates({("JPY", "2026-11-09")})
    assert loaded == [("JPY", "2026-11-09", "97.1234567891", "ECB")]
    xml = seen[0][3]
    assert "<ExchangeRateAmount>97.1234567891</ExchangeRateAmount>" in xml
    assert "<RelationalExchRateAmount>1</RelationalExchRateAmount>" in xml
    assert m.decimal_text(1e-05) == "0.00001"


# ------------------------------------------------------------------ paging (finding 9)
def test_get_follows_odata_next_link(monkeypatch):
    nxt = "https://x/api/v2.0/companies(c1)/salesInvoices?$skiptoken=abc"
    client, s, _ = bc(
        [Resp(200, {"value": [{"id": 1}], "@odata.nextLink": nxt}), Resp(200, {"value": [{"id": 2}]})], monkeypatch
    )
    assert [r["id"] for r in client.get_all("https://x/first", params={"$filter": "a"})] == [1, 2]
    assert s.log[1][1] == nxt and s.log[1][2]["params"] is None


# ------------------------------------------------------------------ statuses (finding 10)
@pytest.mark.parametrize("status", ["Open", "Paid", "Canceled", "Corrective"])
def test_posted_statuses_need_a_credit_memo(monkeypatch, status):
    client, s, _ = bc([Resp(200, {"value": [{"id": "d1", "status": status}]})], monkeypatch)
    assert m.push_document(client, "sales", "externalDocumentNumber", "B1", {}, [])[0] == "NEEDS_CREDIT_MEMO"


@pytest.mark.parametrize("status", ["In Review", " "])
def test_in_review_is_not_posted_and_not_changed(monkeypatch, status):
    client, s, _ = bc([Resp(200, {"value": [{"id": "d1", "status": status}]})], monkeypatch)
    assert m.push_document(client, "sales", "externalDocumentNumber", "B1", {}, [])[0] == "NOT_EDITABLE"
    assert s.calls == ["GET"]


# ------------------------------------------------------------------ number series (finding 11)
def ns_client(outcomes, monkeypatch):
    client, s, _ = bc(outcomes, monkeypatch)
    applied = []
    monkeypatch.setattr(client, "apply_package", lambda *a: applied.append(a))
    return client, s, applied


def test_number_series_line_that_exists_is_not_overwritten(monkeypatch):
    client, _, applied = ns_client([Resp(200, {"value": [{"Series_Code": "P-INV+", "Line_No": 20000}]})], monkeypatch)
    assert not ns.extend(client, "NoSeriesLines", "P-INV+", 20000, "110000", "199999", "2015-01-01")
    assert applied == []


def test_number_series_new_line_is_added(monkeypatch):
    client, s, applied = ns_client([Resp(200, {"value": [{"Series_Code": "P-INV+", "Line_No": 10000}]})], monkeypatch)
    assert ns.extend(client, "NoSeriesLines", "P-INV+", 20000, "110000", "199999", "2015-01-01")
    assert "Series_Code eq 'P-INV+'" in s.log[0][2]["params"]["$filter"]
    assert "<LineNo>20000</LineNo>" in applied[0][3]


def test_number_series_refuses_when_lines_cannot_be_read(monkeypatch):
    client, _, applied = ns_client([Resp(404, {"error": "not published"})], monkeypatch)
    with pytest.raises(RuntimeError, match="publish page 457"):
        ns.extend(client, "NoSeriesLines", "P-INV+", 20000, "110000", "199999", "2015-01-01")
    assert applied == []


# ------------------------------------------------------------------ bc_tables (findings 12, 14)
def test_provider_vendor_number_keeps_lowercase_letters():
    assert "regexp_replace(upper(s.provider), '[^A-Z0-9]', '')" in bc_tables.PROVIDER_VENDOR_NUMBER
    src = (ROOT / "src" / "bc_tables.py").read_text()
    assert "{PROVIDER_VENDOR_NUMBER}" in src and "regexp_replace(s.provider," not in src


def test_every_service_type_has_its_own_type_code():
    codes = bc_tables.TYPE_CODES
    assert len(set(codes.values())) == len(codes)
    assert codes["manual_transfer"] == "MTR" and codes["manual_transport"] == "MTP"
    assert all(k in codes for k in bc_tables.SERVICE_ARRAYS)
    case = bc_tables.type_code_sql()
    assert "WHEN 'manual_transport' THEN 'MTP'" in case and "WHEN 'manual_transfer' THEN 'MTR'" in case


def test_data_quality_uses_the_push_result_groups():
    import ast

    import data_quality as dq

    assert set(ast.literal_eval(dq.TERMINAL)) == set(m.TERMINAL)
    assert set(ast.literal_eval(dq.ATTEMPT)) == set(m.ATTEMPT)
    assert dq.MAX_FAILURES == m.MAX_FAILURES
