"""Job vio_travel_backfill, task plan_windows - split a backfill range into windows for the for-each task.

Sets the task value `windows`, a list of {"from": "yyyy-mm-dd", "to": "yyyy-mm-dd"}; the for-each task
extract_window runs extract_tc once per window (bookings only, --backfill_from/--backfill_to, watermark untouched),
a few windows at a time. Windows are calendar months by default, so a backfill of 2015 to today is about 140
independent, retryable iterations instead of one long run.
"""

import argparse
import datetime as dt


def month_windows(start: dt.date, end: dt.date, months: int = 1) -> list[dict]:
    """Consecutive inclusive windows that start on the 1st of a month (except the first) and cover [start, end]."""
    out, cur = [], start
    while cur <= end:
        y, m = divmod(cur.month - 1 + months, 12)
        nxt = dt.date(cur.year + y, m + 1, 1)
        stop = min(end, nxt - dt.timedelta(days=1))
        out.append({"from": cur.isoformat(), "to": stop.isoformat()})
        cur = nxt
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill_from", required=True)
    ap.add_argument("--backfill_to", default="")
    ap.add_argument("--months", type=int, default=1)
    a = ap.parse_args()
    end = dt.date.fromisoformat(a.backfill_to) if a.backfill_to else dt.date.today()
    windows = month_windows(dt.date.fromisoformat(a.backfill_from), end, a.months)
    print(f"{len(windows)} windows: {windows[0]} .. {windows[-1]}")
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    DBUtils(SparkSession.builder.getOrCreate()).jobs.taskValues.set(key="windows", value=windows)


if __name__ == "__main__":
    main()
