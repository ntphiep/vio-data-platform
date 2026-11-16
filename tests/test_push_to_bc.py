"""Unit tests for the Business Central push helpers (no network, no Databricks).
Run: uv run --no-project --with pytest --with requests pytest pipeline/tests"""
import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
import push_to_bc as m  # noqa: E402


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


class Session:
    def __init__(self, outcomes):
        self.outcomes, self.calls, self.headers = list(outcomes), [], {}

    def request(self, method, url, **kw):
        self.calls.append(method)
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
    return client, s, sleeps


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


def test_posted_document_is_never_changed(monkeypatch):
    client, s, _ = bc([Resp(200, {"value": [{"id": "d1", "status": "Paid"}]})], monkeypatch)
    result, _ = m.push_document(client, "sales", "externalDocumentNumber", "EXA-1", {}, [])
    assert result == "NEEDS_CREDIT_MEMO" and s.calls == ["GET"]


def test_cancel_empties_draft_lines_without_deleting_header(monkeypatch):
    client, s, _ = bc([Resp(200, {"value": [{"id": "d1", "status": "Draft"}]}),
                       Resp(200, {"value": [{"id": "l1"}, {"id": "l2"}]}), Resp(204), Resp(204)], monkeypatch)
    result, _ = m.push_document(client, "sales", "externalDocumentNumber", "EXA-1", {}, [], cancel=True)
    assert result == "CANCELLED_DRAFT_EMPTIED" and s.calls == ["GET", "GET", "DELETE", "DELETE"]
