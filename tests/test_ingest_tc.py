"""Unit tests for the extract helpers. Run: uv run --no-project --with pytest --with requests pytest pipeline/tests"""
import datetime as dt
import sys
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
    tc = m.TravelCompositor("https://x/resources", "u", "p", "ms", lambda *a: logs.append(a), session=s,
                            sleep=sleeps.append)
    return tc, s, sleeps, logs


def test_retry_on_429_honours_retry_after():
    tc, s, sleeps, logs = client([Resp(429, headers={"Retry-After": "7"}), Resp(200, b'{"ok": 1}')])
    assert tc.get("/p") == {"ok": 1}
    assert sleeps == [7.0]
    assert [l[2] for l in logs] == [429, 200]


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
    assert w == [(dt.date(2026, 1, 1), dt.date(2026, 1, 7)), (dt.date(2026, 1, 8), dt.date(2026, 1, 14)),
                 (dt.date(2026, 1, 15), dt.date(2026, 1, 20))]
    assert m.day_windows(dt.date(2026, 1, 1), dt.date(2026, 1, 1), 7) == [(dt.date(2026, 1, 1),) * 2]


def test_page_bookings_reports_shortfall_when_offset_shifts():
    # totalResults says 3, but a change between pages makes booking B appear twice and C never.
    pages = [Resp(200, b'{"pagination": {"totalResults": 3}, "bookedTrip": [{"bookingReference": "A"},'
                       b' {"bookingReference": "B"}]}'),
             Resp(200, b'{"pagination": {"totalResults": 3}, "bookedTrip": [{"bookingReference": "B"}]}')]
    tc, *_ = client(pages)
    landed = []
    p, r, b, refs, total = m.page_bookings(tc, (dt.date(2026, 1, 1), dt.date(2026, 1, 2)), 2,
                                           lambda recs, meta: landed.append(recs) or 10)
    assert (p, r, total) == (2, 3, 3) and refs == {"A", "B"} and len(refs) < total
