"""Unit tests for the extract helpers. Run: uv run --no-project --with pytest --with requests pytest tests"""

import datetime as dt
import json
import sys
import threading
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import ingest_tc as m  # noqa: E402


class Resp:
    def __init__(self, status, body=b"{}", headers=None):
        self.status_code, self.content, self.headers = status, body, headers or {}

    def json(self):
        import json

        return json.loads(self.content)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


class Session:
    """Replays a scripted list of GET outcomes; authenticate always succeeds."""

    def __init__(self, gets):
        self.gets, self.auth_calls, self.get_calls = list(gets), 0, 0

    def post(self, url, **kw):
        self.auth_calls += 1
        return Resp(200, b'{"token": "t%d"}' % self.auth_calls)

    def get(self, url, **kw):
        self.get_calls += 1
        g = self.gets.pop(0)
        if isinstance(g, Exception):
            raise g
        return g


def client(gets):
    sleeps, logs = [], []
    s = Session(gets)
    tc = m.TravelCompositor(
        "https://x/resources", "u", "p", "ms", lambda *a: logs.append(a), session=s, sleep=sleeps.append
    )
    return tc, s, sleeps, logs


def test_retry_on_429_honours_retry_after():
    tc, s, sleeps, logs = client([Resp(429, headers={"Retry-After": "7"}), Resp(200, b'{"ok": 1}')])
    assert tc.get("/p") == {"ok": 1}
    assert sleeps == [7.0]
    assert [row[2] for row in logs] == [429, 200]


def test_retry_on_5xx_and_network_error_then_success():
    tc, s, sleeps, logs = client([Resp(502), requests.ConnectionError("reset"), Resp(200, b"[]")])
    assert tc.get("/p") == []
    assert len(sleeps) == 2 and all(1 <= x <= m.MAX_SLEEP for x in sleeps)
    assert logs[1][6].startswith("ConnectionError")


def test_401_gets_new_token_without_sleep():
    tc, s, sleeps, _ = client([Resp(401), Resp(200, b"{}")])
    tc.get("/p")
    assert s.auth_calls == 2 and sleeps == []


def test_4xx_fails_fast():
    tc, s, sleeps, _ = client([Resp(400, b"bad")])
    with pytest.raises(RuntimeError, match="returned 400"):
        tc.get("/p")
    assert s.get_calls == 1


def test_gives_up_after_max_attempts_without_final_sleep():
    tc, s, sleeps, _ = client([Resp(503)] * m.MAX_ATTEMPTS)
    with pytest.raises(RuntimeError, match="failed after"):
        tc.get("/p")
    assert s.get_calls == m.MAX_ATTEMPTS and len(sleeps) == m.MAX_ATTEMPTS - 1


def test_truncated_json_is_retried():
    tc, s, sleeps, _ = client([Resp(200, b'{"bookedTrip": [{"a"'), Resp(200, b'{"ok": 2}')])
    assert tc.get("/p") == {"ok": 2} and len(sleeps) == 1


def test_token_renewed_before_expiry(monkeypatch):
    tc, s, _, _ = client([Resp(200, b"{}"), Resp(200, b"{}")])
    tc.get("/p")
    tc.token_expires = 0  # simulate a token that is about to expire
    tc.get("/p")
    assert s.auth_calls == 2


def test_day_windows_cover_range_without_gaps():
    w = m.day_windows(dt.date(2026, 1, 1), dt.date(2026, 1, 20), 7)
    assert w == [
        (dt.date(2026, 1, 1), dt.date(2026, 1, 7)),
        (dt.date(2026, 1, 8), dt.date(2026, 1, 14)),
        (dt.date(2026, 1, 15), dt.date(2026, 1, 20)),
    ]
    assert m.day_windows(dt.date(2026, 1, 1), dt.date(2026, 1, 1), 7) == [(dt.date(2026, 1, 1),) * 2]


def test_page_bookings_reports_shortfall_when_offset_shifts():
    # totalResults says 3, but a change between pages makes booking B appear twice and C never.
    pages = [
        Resp(
            200,
            b'{"pagination": {"totalResults": 3}, "bookedTrip": [{"bookingReference": "A"},'
            b' {"bookingReference": "B"}]}',
        ),
        Resp(200, b'{"pagination": {"totalResults": 3}, "bookedTrip": [{"bookingReference": "B"}]}'),
    ]
    tc, *_ = client(pages)
    landed = []
    p, r, b, refs, total = m.page_bookings(
        tc, (dt.date(2026, 1, 1), dt.date(2026, 1, 2)), 2, lambda recs, meta: landed.append(recs) or 10
    )
    assert (p, r, total) == (2, 3, 3) and refs == {"A", "B"} and len(refs) < total


class PagedSession(Session):
    """Answers a paged list from `rows` by the first/limit parameters, so parallel readers get the right page."""

    def __init__(self, rows, list_key, fail_first=None):
        super().__init__([])
        self.rows, self.list_key, self.fail_first, self.lock = rows, list_key, fail_first, threading.Lock()

    def get(self, url, params=None, **kw):
        with self.lock:
            self.get_calls += 1
        first, limit = params["first"], params["limit"]
        if first == self.fail_first:
            return Resp(400, b"bad page")
        body = {"pagination": {"totalResults": len(self.rows)}, self.list_key: self.rows[first : first + limit]}
        return Resp(200, json.dumps(body).encode())


def paged_client(rows, list_key, fail_first=None):
    s = PagedSession(rows, list_key, fail_first)
    tc = m.TravelCompositor("https://x", "u", "p", "ms", lambda *a: None, session=s, sleep=lambda x: None)
    return tc, s


def test_record_key_single_and_composite():
    assert m.record_key({"id": 5}, ("id",)) == 5
    assert m.record_key({"agency": "A1", "username": "u"}, ("agency", "username")) == ("A1", "u")


def test_snapshot_reads_every_page_in_parallel():
    rows = [{"agency": "A", "username": f"u{i}"} for i in range(250)]
    tc, s = paged_client(rows, "user")
    landed = []
    pages, records, nbytes, keys, total = m.read_snapshot(
        tc, m.SNAPSHOTS["users"], lambda recs, meta: landed.append(len(recs)) or 1, threads=4
    )
    assert (pages, records, total, len(keys)) == (3, 250, 250, 250) and sorted(landed) == [50, 100, 100]
    assert s.get_calls == 3


def test_snapshot_page_failure_raises_so_snapshot_is_not_used():
    rows = [{"id": i} for i in range(300)]
    tc, _ = paged_client(rows, "agency", fail_first=200)
    with pytest.raises(m.ApiError, match="returned 400"):
        m.read_snapshot(tc, m.SNAPSHOTS["agencies"], lambda recs, meta: 1, threads=2)


def test_snapshot_params_fill_today():
    seen = []

    class S(PagedSession):
        def get(self, url, params=None, **kw):
            seen.append(params)
            return super().get(url, params, **kw)

    s = S([{"id": 1}], "purchaseAttempt")
    tc = m.TravelCompositor("https://x", "u", "p", "ms", lambda *a: None, session=s, sleep=lambda x: None)
    m.read_snapshot(tc, m.SNAPSHOTS["purchase_attempts"], lambda recs, meta: 1, today=dt.date(2026, 11, 16))
    assert seen[0]["toCreationDate"] == "20261116" and seen[0]["fromCreationDate"] == "20150101"


def test_single_call_snapshot_without_paging():
    tc, s, _, _ = client([Resp(200, b'[{"id": 1}, {"id": 2}]')])
    pages, records, _, keys, total = m.read_snapshot(tc, m.SNAPSHOTS["suppliers"], lambda recs, meta: 1)
    assert (pages, records, total, keys) == (1, 2, 2, {1, 2})


def test_datasheets_mark_not_found_and_failed_per_id():
    tc, _, _, _ = client([Resp(200, b'{"id": "A", "auditData": {}}'), Resp(404, b"no"), Resp(403, b"no")])
    landed = []
    out = m.read_datasheets(tc, ["A", "B", "C"], lambda recs, meta: landed.append(recs) or 1)
    assert out == {"A": "OK", "B": "NOT_FOUND", "C": "FAILED"}
    assert landed == [[{"id": "A"}]]  # auditData is dropped


def test_unknown_entity_list_is_complete():
    assert set(m.DEFAULT_ENTITIES) < set(m.ALL_ENTITIES) and "accommodations" not in m.DEFAULT_ENTITIES


def test_snapshot_without_pagination_is_not_reconciled():
    tc, _, _, _ = client([Resp(200, b'{"agency": [{"id": 1}]}')])
    *_, total = m.read_snapshot(tc, m.SNAPSHOTS["agencies"], lambda recs, meta: 1)
    assert total is None
