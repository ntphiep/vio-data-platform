"""Unit tests for the backfill planner. Run: uv run --no-project --with pytest pytest tests"""

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import plan_backfill_windows as m  # noqa: E402


def test_month_windows_cover_range_without_gaps():
    w = m.month_windows(dt.date(2025, 11, 20), dt.date(2026, 2, 3))
    assert w == [
        {"from": "2025-11-20", "to": "2025-11-30"},
        {"from": "2025-12-01", "to": "2025-12-31"},
        {"from": "2026-01-01", "to": "2026-01-31"},
        {"from": "2026-02-01", "to": "2026-02-03"},
    ]


def test_month_windows_of_several_months_and_single_day():
    assert m.month_windows(dt.date(2026, 1, 1), dt.date(2026, 6, 30), 3) == [
        {"from": "2026-01-01", "to": "2026-03-31"},
        {"from": "2026-04-01", "to": "2026-06-30"},
    ]
    assert m.month_windows(dt.date(2026, 5, 5), dt.date(2026, 5, 5)) == [{"from": "2026-05-05", "to": "2026-05-05"}]
