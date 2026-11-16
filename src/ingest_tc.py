"""Task 1 - Extract: Travel Compositor API -> raw files in the Unity Catalog volume <catalog>.raw.landing.

Design
- Read-only GET calls (plus the authenticate POST). Credentials come from the Databricks secret scope `vio-tc`.
- Bookings are pulled incrementally by last-update date (`ufrom`/`uto`, day granularity only) from the watermark in
  <catalog>.ops.watermark, with a 1-day overlap. The range is split into windows of --window_days; each finished
  window advances the watermark, so a long backfill resumes where it stopped instead of starting again.
- Offset paging (`first`/`limit`) can shift when bookings change while a window is being read. Every window is
  reconciled: distinct booking references landed vs `totalResults`. A short window is read once more; if it is
  still short it is recorded as PARTIAL and the watermark stays, so the next run reads the window again.
  Duplicates from overlaps are expected and removed downstream (MERGE on bookingReference, newest lastUpdateDate).
- Each API page is landed unchanged as one gzip NDJSON file (one record per line, with run metadata), so parsing
  bugs are fixed by replaying raw files instead of calling the API again.
- Reference data (agencies, suppliers) is a full snapshot every --ref_refresh_hours (default 6), so a supplier or
  agency created today reaches silver the same day; until then its documents are held, not failed.
- Retries: exponential backoff with full jitter on 429, 5xx and network errors, honouring Retry-After; a 401 gets
  a new token. Every HTTP attempt is logged to <catalog>.ops.api_calls (no credentials, no token).
- Landing files older than --retention_days are removed after their content is in bronze.

Parameters: --catalog --page_size --window_days --overlap_days --backfill_from --retention_days --ref_refresh_hours
"""

import argparse
import datetime as dt
import gzip
import json
import os
import random
import shutil
import time
import uuid

import requests

API_TIMEOUT = (10, 180)  # connect, read
MAX_ATTEMPTS = 6
MAX_SLEEP = 120


def backoff_seconds(attempt, retry_after=None):
    """Retry-After wins when the server sends it; otherwise exponential backoff with full jitter."""
    if retry_after:
        try:
            return min(MAX_SLEEP, max(1.0, float(retry_after)))
        except ValueError:
            pass
    return random.uniform(1, min(MAX_SLEEP, 2**attempt))  # noqa: S311 - backoff jitter, not security


def day_windows(start, end, days):
    """Split [start, end] into consecutive inclusive windows of at most `days` days."""
    out, cur = [], start
    while cur <= end:
        stop = min(end, cur + dt.timedelta(days=days - 1))
        out.append((cur, stop))
        cur = stop + dt.timedelta(days=1)
    return out


class TravelCompositor:
    def __init__(self, base_url, username, password, microsite, log, session=None, sleep=time.sleep):
        self.base, self.user, self.pwd, self.ms, self.log = base_url.rstrip("/"), username, password, microsite, log
        self.s = session or requests.Session()
        self.sleep = sleep
        self.token, self.token_expires = None, 0.0

    def _authenticate(self):
        r = self.s.post(
            f"{self.base}/authentication/authenticate",
            timeout=API_TIMEOUT,
            json={"username": self.user, "password": self.pwd, "micrositeId": self.ms},
        )
        r.raise_for_status()
        d = r.json()
        self.token = d["token"]
        # Renew 5 minutes before the token expires (expirationInSeconds is 7200 today).
        self.token_expires = time.time() + float(d.get("expirationInSeconds") or 3600) - 300

    def get(self, path, params=None):
        for attempt in range(1, MAX_ATTEMPTS + 1):
            t0 = time.time()
            status, err, body, retry_after = None, None, b"", None
            try:
                if not self.token or time.time() >= self.token_expires:
                    self._authenticate()
                r = self.s.get(
                    f"{self.base}{path}",
                    params=params,
                    timeout=API_TIMEOUT,
                    headers={"auth-token": self.token, "Accept-Encoding": "gzip"},
                )
                status, body, retry_after = r.status_code, r.content, r.headers.get("Retry-After")
            except requests.RequestException as e:
                err = type(e).__name__ + ": " + str(e)[:300]
            self.log(path, params, status, len(body), round(time.time() - t0, 2), attempt, err)
            if status == 200:
                try:
                    return json.loads(body)
                except ValueError as e:  # truncated body: retry like a network error
                    status, err = None, f"invalid JSON: {e}"
                    self.log(path, params, 200, len(body), 0.0, attempt, err)
            if status == 401:
                self.token = None
                continue
            if status is None or status == 429 or status >= 500:
                if attempt < MAX_ATTEMPTS:
                    self.sleep(backoff_seconds(attempt, retry_after))
                continue
            raise RuntimeError(f"GET {path} returned {status}: {body[:300]!r}")
        raise RuntimeError(f"GET {path} failed after {MAX_ATTEMPTS} attempts")


def page_bookings(tc, window, page_size, land):
    """Read one update-date window page by page. Returns (pages, records, bytes, distinct refs, totalResults)."""
    ufrom, uto = window
    first, pages, records, nbytes, total, refs = 0, 0, 0, 0, None, set()
    while total is None or first < total:
        params = {
            "microsite": tc.ms,
            "from": "20150101",
            "to": dt.date.today().strftime("%Y%m%d"),
            "ufrom": ufrom.strftime("%Y%m%d"),
            "uto": uto.strftime("%Y%m%d"),
            "first": first,
            "limit": page_size,
        }
        data = tc.get("/booking/getBookings", params)
        trips = data.get("bookedTrip") or []
        total = (data.get("pagination") or {}).get("totalResults", 0) if total is None else total
        if not trips:
            break
        pages += 1
        nbytes += land(
            trips,
            {"endpoint": "/booking/getBookings", "ufrom": str(ufrom), "uto": str(uto), "first": first, "total": total},
        )
        records += len(trips)
        refs.update(t.get("bookingReference") for t in trips)
        first += len(trips)
    return pages, records, nbytes, refs, total or 0


def main():
    from pyspark.sql import SparkSession  # imported here so the helpers above are testable without Spark

    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="vio_travel")
    ap.add_argument("--page_size", type=int, default=50)
    ap.add_argument("--window_days", type=int, default=7)
    ap.add_argument("--overlap_days", type=int, default=1)
    ap.add_argument("--backfill_from", default="", help="yyyy-mm-dd; ignore the watermark and start here")
    ap.add_argument("--retention_days", type=int, default=90)
    ap.add_argument("--ref_refresh_hours", type=int, default=6)
    a = ap.parse_args()

    spark = SparkSession.builder.getOrCreate()
    from pyspark.dbutils import DBUtils  # available on Databricks runtimes

    dbutils = DBUtils(spark)
    ops = f"{a.catalog}.ops"
    landing = f"/Volumes/{a.catalog}/raw/landing"
    run_id = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    today = dt.date.today()
    now = lambda: dt.datetime.now(dt.UTC).replace(tzinfo=None)  # noqa: E731

    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ops}.api_calls (
        run_id STRING, called_at TIMESTAMP, path STRING, params STRING, status INT, bytes BIGINT,
        seconds DOUBLE, attempt INT, error STRING)""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ops}.ingest_runs (
        run_id STRING, started_at TIMESTAMP, finished_at TIMESTAMP, entity STRING, window_from DATE,
        window_to DATE, pages INT, records INT, distinct_keys INT, expected INT, bytes BIGINT, status STRING,
        error STRING)""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ops}.watermark (
        entity STRING, watermark DATE, updated_at TIMESTAMP, run_id STRING)""")

    calls, summaries = [], []

    def log(path, params, status, nbytes, secs, attempt, err):
        calls.append((run_id, now(), path, json.dumps(params or {}), status, nbytes, float(secs), attempt, err))

    def lander(entity):
        folder = f"{landing}/{entity}/ingest_date={today.isoformat()}/run={run_id}"
        os.makedirs(folder, exist_ok=True)
        counter = {"n": 0}

        def land(records, meta):
            """Write one page as gzip NDJSON; each line = {"_meta": ..., "record": ...}."""
            counter["n"] += 1
            meta = {**meta, "run_id": run_id, "fetched_at": now().isoformat() + "Z"}
            lines = "\n".join(json.dumps({"_meta": meta, "record": r}, ensure_ascii=False) for r in records)
            data = gzip.compress(lines.encode("utf-8"))
            tmp = f"{folder}/.page={counter['n']:05d}.tmp"
            with open(tmp, "wb") as f:  # UC volumes are a POSIX path on serverless
                f.write(data)
            os.replace(tmp, f"{folder}/page={counter['n']:05d}.ndjson.gz")  # Auto Loader only sees whole files
            return len(data)

        return land

    def flush():
        if calls:
            spark.createDataFrame(
                calls,
                "run_id STRING, called_at TIMESTAMP, path STRING, params STRING, "
                "status INT, bytes BIGINT, seconds DOUBLE, attempt INT, error STRING",
            ).write.mode("append").saveAsTable(f"{ops}.api_calls")
            calls.clear()
        if summaries:
            spark.createDataFrame(
                summaries,
                "run_id STRING, started_at TIMESTAMP, finished_at TIMESTAMP, "
                "entity STRING, window_from DATE, window_to DATE, pages INT, "
                "records INT, distinct_keys INT, expected INT, bytes BIGINT, "
                "status STRING, error STRING",
            ).write.mode("append").saveAsTable(f"{ops}.ingest_runs")
            for s in summaries:
                print(
                    f"{s[3]}: {s[11]} window={s[4]}..{s[5]} pages={s[6]} records={s[7]} distinct={s[8]} "
                    f"expected={s[9]} bytes={s[10]} error={s[12]}"
                )
            done.extend(summaries)
            summaries.clear()

    done = []
    tc = TravelCompositor(
        dbutils.secrets.get("vio-tc", "tc_base_url"),
        dbutils.secrets.get("vio-tc", "tc_username"),
        dbutils.secrets.get("vio-tc", "tc_password"),
        dbutils.secrets.get("vio-tc", "tc_microsite_id"),
        log,
    )

    try:
        # --- bookings, incremental by last update date, window by window ----------------------
        wm = spark.sql(f"SELECT max(watermark) AS w FROM {ops}.watermark WHERE entity='bookings'").collect()[0]["w"]
        if a.backfill_from:
            start = dt.date.fromisoformat(a.backfill_from)
        elif wm:
            start = wm - dt.timedelta(days=a.overlap_days)
        else:
            start = today - dt.timedelta(days=7)  # first run: last 7 days of changes
        land = lander("bookings")
        for window in day_windows(start, today, a.window_days):
            t0, status, error = now(), "SUCCESS", None
            pages = records = nbytes = total = 0
            refs = set()
            try:
                for _ in range(2):  # second pass only when offset paging came back short
                    p, r, b, got, total = page_bookings(tc, window, a.page_size, land)
                    pages, records, nbytes = pages + p, records + r, nbytes + b
                    refs |= got
                    if len(refs) >= total:
                        break
                if len(refs) < total:
                    status, error = "PARTIAL", f"{len(refs)} distinct of {total} expected"
            except Exception as e:  # noqa: BLE001 - keep what was landed, record the failure
                status, error = "FAILED", repr(e)[:1000]
            summaries.append(
                (
                    run_id,
                    t0,
                    now(),
                    "bookings",
                    window[0],
                    window[1],
                    pages,
                    records,
                    len(refs),
                    total,
                    nbytes,
                    status,
                    error,
                )
            )
            if status != "SUCCESS":
                break  # later windows wait, the watermark stays at the last complete window
            spark.createDataFrame(
                [("bookings", window[1], now(), run_id)],
                "entity STRING, watermark DATE, updated_at TIMESTAMP, run_id STRING",
            ).write.mode("append").saveAsTable(f"{ops}.watermark")
            flush()

        # --- reference data: full snapshot every --ref_refresh_hours -------------------------
        for entity in ("agencies", "suppliers"):
            have = spark.sql(f"""SELECT count(*) AS n FROM {ops}.ingest_runs WHERE entity='{entity}'
                                 AND status='SUCCESS'
                                 AND finished_at > current_timestamp() - INTERVAL {a.ref_refresh_hours} HOURS
                              """).collect()[0]["n"]
            if have:
                continue
            land = lander(entity)
            t0, pages, records, nbytes, st, err, total, keys = now(), 0, 0, 0, "SUCCESS", None, 0, set()
            try:
                if entity == "suppliers":
                    rows = tc.get("/suppliers")  # one call, no paging (contract supplier list)
                    pages, records, total = 1, len(rows), len(rows)
                    keys = {r.get("id") for r in rows}
                    nbytes = land(rows, {"endpoint": "/suppliers", "snapshot": True})
                else:
                    first, total = 0, None
                    while total is None or first < total:
                        data = tc.get(f"/agency/{tc.ms}", {"first": first, "limit": 100})  # API maximum is 100
                        rows = data.get("agency") or []
                        total = (data.get("pagination") or {}).get("totalResults", 0) if total is None else total
                        if not rows:
                            break
                        pages += 1
                        nbytes += land(rows, {"endpoint": f"/agency/{tc.ms}", "first": first, "snapshot": True})
                        records += len(rows)
                        keys |= {r.get("id") for r in rows}
                        first += len(rows)
                if len(keys) < (total or 0):
                    st, err = "PARTIAL", f"{len(keys)} distinct of {total} expected"
            except Exception as e:  # noqa: BLE001 - record the failure, the next run retries
                st, err = "FAILED", repr(e)[:1000]
            summaries.append(
                (run_id, t0, now(), entity, today, today, pages, records, len(keys), total or 0, nbytes, st, err)
            )
            flush()

        # --- retention of landing files ------------------------------------------------------
        cutoff = today - dt.timedelta(days=a.retention_days)
        for entity in ("bookings", "agencies", "suppliers"):
            base = f"{landing}/{entity}"
            for d in os.listdir(base) if os.path.isdir(base) else []:
                if d.startswith("ingest_date=") and d[len("ingest_date=") :] < cutoff.isoformat():
                    shutil.rmtree(f"{base}/{d}")
                    print(f"retention: removed {base}/{d}")
    finally:
        flush()

    bad = [s for s in done if s[11] == "FAILED"]
    if bad:
        raise RuntimeError(f"{len(bad)} extract(s) failed, see {ops}.ingest_runs run_id={run_id}")


if __name__ == "__main__":
    main()
