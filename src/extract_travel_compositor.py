"""Task extract_travel_compositor - Travel Compositor API -> raw files in the Unity Catalog volume <catalog>.raw.landing.

Design
- Read-only GET calls (plus the authenticate POST). Credentials come from the Databricks secret scope `vio-tc`.
- One registry, ENTITIES, says how each entity is read:
    incremental  bookings, by last-update date (`ufrom`/`uto`, day granularity only) from the watermark in
                 <catalog>.ops.watermark with a 1-day overlap, in windows of --window_days; each finished window
                 advances the watermark, so a scheduled run resumes where the last one stopped. A bounded range
                 (--backfill_to) never moves the watermark; job vio_travel_backfill_bookings reads long ranges month by
                 month, and a failed month is read again with "Repair run".
    snapshot     agencies, suppliers, users, travel ideas, pending payments, purchase attempts and the
                 accommodation catalog: every page of the list, at most every `refresh_hours`. The pipeline
                 vio_travel_build_bronze_silver_gold turns consecutive snapshots into type 2 history and detects deletions, so a
                 snapshot counts only when it is complete (status SUCCESS in ops.ingest_runs).
    by_id        accommodation datasheets of the hotels that appear on bookings, fetched once and refreshed after
                 --datasheet_refresh_days, at most --datasheet_cap per run.
- Offset paging (`first`/`limit`) can shift while a list changes. Every window and snapshot is reconciled: distinct
  keys landed vs `totalResults`. A short booking window is read once more; if it is still short it is recorded as
  PARTIAL and the watermark stays. A short snapshot is read a second time: the same keys again means the count
  overstates and the snapshot is used; other keys make it PARTIAL. An empty snapshot is EMPTY. PARTIAL and EMPTY
  are never used, because the pipeline closes every key missing from a snapshot.
- Each API page is landed unchanged as one gzip NDJSON file (one record per line, with run metadata), so parsing
  bugs are fixed by replaying raw files instead of calling the API again.
- Retries: exponential backoff with full jitter on 429, 5xx and network errors, honouring Retry-After; a 401 gets
  a new token. Every HTTP attempt is logged to <catalog>.ops.api_calls (no credentials, no token).
- Large lists (the accommodation catalog, 283 pages of 1000) are read with --threads parallel requests.
- Landing files older than --retention_days are removed; bronze keeps every record (pipelines.reset.allowed=false).

Parameters: --catalog --entities --page_size --window_days --overlap_days --backfill_from --backfill_to
            --retention_days --threads --datasheet_cap --datasheet_refresh_days
"""

import argparse
import datetime as dt
import gzip
import json
import os
import random
import shutil
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import requests

API_TIMEOUT = (10, 180)  # connect, read
MAX_ATTEMPTS = 6
MAX_SLEEP = 120


class ApiError(RuntimeError):
    def __init__(self, path, status, body):
        super().__init__(f"GET {path} returned {status}: {body[:300]!r}")
        self.status = status


@dataclass(frozen=True)
class Snapshot:
    """A list endpoint read whole. `path` may contain {ms}; `key` names the record field(s) that identify a row."""

    path: str
    list_key: str | None  # None: the response is the list itself
    key: tuple[str, ...]
    page_size: int = 100  # the API maximum for most lists is 100
    refresh_hours: float = 6
    params: dict = field(default_factory=dict)


# Entities read by default every run (subject to refresh_hours). `accommodations` (283 pages) is read only when
# --entities names it, by the weekly job vio_travel_ingest_accommodation_catalog.
SNAPSHOTS = {
    "agencies": Snapshot("/agency/{ms}", "agency", ("id",)),
    "suppliers": Snapshot("/suppliers", None, ("id",), page_size=0),
    "users": Snapshot("/user/{ms}", "user", ("agency", "username")),
    "travel_ideas": Snapshot("/travelidea/{ms}", "idea", ("id",), params={"lang": "en"}),
    "pending_payments": Snapshot("/pending-payments/{ms}", "pendingPayments", ("paymentId",), refresh_hours=0),
    "purchase_attempts": Snapshot(
        "/purchase-attempts/{ms}",
        "purchaseAttempt",
        ("id",),
        refresh_hours=0,
        params={"fromCreationDate": "20150101", "toCreationDate": "{today}"},
    ),
    "accommodations": Snapshot("/accommodations", "accommodations", ("id",), page_size=1000, refresh_hours=144),
}
DEFAULT_ENTITIES = ["bookings", *[e for e in SNAPSHOTS if e != "accommodations"], "accommodation_datasheets"]
ALL_ENTITIES = ["bookings", *SNAPSHOTS, "accommodation_datasheets"]


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


def record_key(record, key):
    return tuple(str(record.get(k)) for k in key) if len(key) > 1 else record.get(key[0])


class TravelCompositor:
    def __init__(self, base_url, username, password, microsite, log, session=None, sleep=time.sleep):
        self.base, self.user, self.pwd, self.ms, self.log = base_url.rstrip("/"), username, password, microsite, log
        self.s = session or requests.Session()
        self.sleep = sleep
        self.token, self.token_expires = None, 0.0
        self._auth_lock = threading.Lock()

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

    def _ensure_token(self):
        with self._auth_lock:  # parallel page readers share one token
            if not self.token or time.time() >= self.token_expires:
                self._authenticate()
            return self.token

    def get(self, path, params=None):
        for attempt in range(1, MAX_ATTEMPTS + 1):
            t0 = time.time()
            status, err, body, retry_after = None, None, b"", None
            try:
                token = self._ensure_token()
                r = self.s.get(
                    f"{self.base}{path}",
                    params=params,
                    timeout=API_TIMEOUT,
                    headers={"auth-token": token, "Accept-Encoding": "gzip"},
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
                with self._auth_lock:
                    self.token = None
                continue
            if status is None or status == 429 or status >= 500:
                if attempt < MAX_ATTEMPTS:
                    self.sleep(backoff_seconds(attempt, retry_after))
                continue
            raise ApiError(path, status, body)
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


def read_snapshot(tc, spec, land, threads=1, today=None):
    """Read every page of a list endpoint. Returns (pages, records, bytes, distinct keys, totalResults).

    The first page gives totalResults; the remaining offsets are read with `threads` parallel requests. Each page
    is landed as soon as it arrives. A page that fails raises, so a snapshot is either complete or not used.
    """
    today = today or dt.date.today()
    path = spec.path.format(ms=tc.ms)
    params = {k: v.format(today=today.strftime("%Y%m%d")) for k, v in spec.params.items()}
    if not spec.page_size:  # one call, no paging
        rows = tc.get(path, params or None)
        rows = rows if spec.list_key is None else rows.get(spec.list_key) or []
        nbytes = land(rows, {"endpoint": path, "snapshot": True}) if rows else 0
        return 1, len(rows), nbytes, {record_key(r, spec.key) for r in rows}, len(rows)

    lock = threading.Lock()
    acc = {"pages": 0, "records": 0, "bytes": 0, "keys": set()}

    def fetch(first):
        data = tc.get(path, {**params, "first": first, "limit": spec.page_size})
        rows = data.get(spec.list_key) or []
        total = (data.get("pagination") or {}).get("totalResults")
        if rows:
            nbytes = land(rows, {"endpoint": path, "first": first, "snapshot": True})
            with lock:
                acc["pages"] += 1
                acc["records"] += len(rows)
                acc["bytes"] += nbytes
                acc["keys"].update(record_key(r, spec.key) for r in rows)
        return total

    total = fetch(0)
    if total is None:  # no pagination block: the response cannot be reconciled, so it is not a snapshot
        return acc["pages"], acc["records"], acc["bytes"], acc["keys"], None
    offsets = list(range(spec.page_size, total, spec.page_size))
    if threads > 1 and len(offsets) > 1:
        with ThreadPoolExecutor(max_workers=threads) as pool:
            list(pool.map(fetch, offsets))  # list() re-raises the first failure
    else:
        for first in offsets:
            fetch(first)
    return acc["pages"], acc["records"], acc["bytes"], acc["keys"], total


def snapshot_status(keys, total, again):
    """Status of a snapshot that came back with fewer distinct keys than totalResults, after a second read."""
    if again == keys:
        return "SUCCESS", f"stable: {len(keys)} distinct on two reads, totalResults says {total}"
    return "PARTIAL", f"{len(keys)} distinct of {total} expected; a second read gave {len(again)} different keys"


def read_datasheets(tc, ids, land, threads=1):
    """Fetch /accommodations/{id}/datasheet for each id. Returns {id: status} with OK, NOT_FOUND or FAILED."""
    out, lock = {}, threading.Lock()

    def fetch(acc_id):
        try:
            sheet = tc.get(f"/accommodations/{acc_id}/datasheet", {"lang": "en"})
            sheet.pop("auditData", None)
            land([sheet], {"endpoint": "/accommodations/{id}/datasheet", "id": acc_id})
            st = "OK"
        except ApiError as e:
            st = "NOT_FOUND" if e.status in (400, 404) else "FAILED"
        except RuntimeError:
            st = "FAILED"
        with lock:
            out[acc_id] = st

    if threads > 1:
        with ThreadPoolExecutor(max_workers=threads) as pool:
            list(pool.map(fetch, ids))
    else:
        for i in ids:
            fetch(i)
    return out


def main():
    from pyspark.sql import SparkSession  # imported here so the helpers above are testable without Spark

    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="vio_travel")
    ap.add_argument("--entities", default="default", help="comma list from ALL_ENTITIES, or 'default'")
    ap.add_argument("--page_size", type=int, default=50)
    ap.add_argument("--window_days", type=int, default=7)
    ap.add_argument("--overlap_days", type=int, default=1)
    ap.add_argument("--backfill_from", default="", help="yyyy-mm-dd; ignore the watermark and start here")
    ap.add_argument("--backfill_to", default="", help="yyyy-mm-dd; with --backfill_from, read only this range")
    ap.add_argument("--retention_days", type=int, default=90)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--datasheet_cap", type=int, default=200)
    ap.add_argument("--datasheet_refresh_days", type=int, default=30)
    ap.add_argument("--force_snapshot", default="false", help="true: ignore refresh_hours")
    a = ap.parse_args()
    entities = DEFAULT_ENTITIES if a.entities in ("", "default") else [e.strip() for e in a.entities.split(",")]
    unknown = set(entities) - set(ALL_ENTITIES)
    if unknown:
        raise ValueError(f"unknown entities {sorted(unknown)}; known: {ALL_ENTITIES}")
    # A bounded backfill reads a fixed range and must not move the watermark of the regular runs.
    bounded = bool(a.backfill_from and a.backfill_to)

    spark = SparkSession.builder.getOrCreate()
    from pyspark.dbutils import DBUtils  # available on Databricks runtimes

    dbutils = DBUtils(spark)
    c, ops = a.catalog, f"{a.catalog}.ops"
    landing = f"/Volumes/{c}/raw/landing"
    run_id = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    today = dt.date.today()
    now = lambda: dt.datetime.now(dt.UTC).replace(tzinfo=None)  # noqa: E731

    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ops}.api_calls (
        run_id STRING, called_at TIMESTAMP, path STRING, params STRING, status INT, bytes BIGINT,
        seconds DOUBLE, attempt INT, error STRING) CLUSTER BY AUTO""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ops}.ingest_runs (
        run_id STRING, started_at TIMESTAMP, finished_at TIMESTAMP, entity STRING, window_from DATE,
        window_to DATE, pages INT, records INT, distinct_keys INT, expected INT, bytes BIGINT, status STRING,
        error STRING) CLUSTER BY AUTO""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ops}.watermark (
        entity STRING, watermark DATE, updated_at TIMESTAMP, run_id STRING)""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ops}.accommodation_fetch (
        accommodation_id STRING, fetched_at TIMESTAMP, status STRING, run_id STRING)""")

    for e in ALL_ENTITIES:  # Auto Loader in the pipeline needs every entity folder, even before its first file
        os.makedirs(f"{landing}/{e}", exist_ok=True)
    calls, summaries, done = [], [], []

    def log(path, params, status, nbytes, secs, attempt, err):
        calls.append((run_id, now(), path, json.dumps(params or {}), status, nbytes, float(secs), attempt, err))

    def lander(entity):
        folder = f"{landing}/{entity}/ingest_date={today.isoformat()}/run={run_id}"
        os.makedirs(folder, exist_ok=True)
        counter, lock = {"n": 0}, threading.Lock()

        def land(records, meta):
            """Write one page as gzip NDJSON; each line = {"_meta": ..., "record": ...}."""
            with lock:
                counter["n"] += 1
                n = counter["n"]
            meta = {**meta, "run_id": run_id, "fetched_at": now().isoformat() + "Z"}
            lines = "\n".join(json.dumps({"_meta": meta, "record": r}, ensure_ascii=False) for r in records)
            data = gzip.compress(lines.encode("utf-8"))
            tmp = f"{folder}/.page={n:05d}.tmp"
            with open(tmp, "wb") as f:  # UC volumes are a POSIX path on serverless
                f.write(data)
            os.replace(tmp, f"{folder}/page={n:05d}.ndjson.gz")  # Auto Loader only sees whole files
            return len(data)

        return land

    def flush():
        if calls:
            spark.createDataFrame(
                list(calls),
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

    tc = TravelCompositor(
        dbutils.secrets.get("vio-tc", "tc_base_url"),
        dbutils.secrets.get("vio-tc", "tc_username"),
        dbutils.secrets.get("vio-tc", "tc_password"),
        dbutils.secrets.get("vio-tc", "tc_microsite_id"),
        log,
    )

    try:
        # --- bookings, incremental by last update date, window by window ----------------------
        if "bookings" in entities:
            wm = spark.sql(f"SELECT max(watermark) AS w FROM {ops}.watermark WHERE entity='bookings'").first()["w"]
            if a.backfill_from:
                start = dt.date.fromisoformat(a.backfill_from)
            elif wm:
                start = wm - dt.timedelta(days=a.overlap_days)
            else:
                start = today - dt.timedelta(days=7)  # first run: last 7 days of changes
            end = dt.date.fromisoformat(a.backfill_to) if bounded else today
            land = lander("bookings")
            for window in day_windows(start, end, a.window_days):
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
                    (run_id, t0, now(), "bookings", window[0], window[1], pages, records, len(refs), total, nbytes)
                    + (status, error)
                )
                if status != "SUCCESS":
                    break  # later windows wait, the watermark stays at the last complete window
                if not bounded:
                    spark.createDataFrame(
                        [("bookings", window[1], now(), run_id)],
                        "entity STRING, watermark DATE, updated_at TIMESTAMP, run_id STRING",
                    ).write.mode("append").saveAsTable(f"{ops}.watermark")
                flush()

        # --- snapshots: every page of a list, at most every refresh_hours ----------------------
        for entity, spec in SNAPSHOTS.items():
            if entity not in entities or bounded:
                continue
            if spec.refresh_hours and a.force_snapshot.lower() != "true":
                fresh = spark.sql(f"""SELECT count(*) AS n FROM {ops}.ingest_runs
                    WHERE entity = '{entity}' AND status = 'SUCCESS'
                      AND finished_at > current_timestamp() - make_interval(0, 0, 0, 0, {spec.refresh_hours})
                """).first()["n"]
                if fresh:
                    continue
            t0, st, err = now(), "SUCCESS", None
            pages = records = nbytes = total = 0
            keys = set()
            try:
                pages, records, nbytes, keys, total = read_snapshot(tc, spec, lander(entity), a.threads, today)
                # The pipeline closes every key missing from a SUCCESS snapshot, so an unreconciled or empty
                # answer is never SUCCESS: an empty list after a 200 is more likely an API fault than no data.
                if total is None:
                    st, err = "PARTIAL", "response without pagination.totalResults"
                elif not keys:
                    st, err = "EMPTY", "no records; previous snapshot stays in force"
                elif len(keys) < total:
                    # Read the list again without landing it: the same keys twice means totalResults overstates
                    # (pending payments report 575 and list 574); different keys mean the list moved under paging.
                    again = read_snapshot(tc, spec, lambda recs, meta: 0, a.threads, today)[3]
                    st, err = snapshot_status(keys, total, again)
            except Exception as e:  # noqa: BLE001 - record the failure, the next run retries
                st, err = "FAILED", repr(e)[:1000]
            summaries.append(
                (run_id, t0, now(), entity, today, today, pages, records, len(keys), total or 0, nbytes, st, err)
            )
            flush()

        # --- accommodation datasheets of booked hotels -----------------------------------------
        if "accommodation_datasheets" in entities and not bounded and spark.catalog.tableExists(f"{c}.silver.bookings"):
            ids = [
                r["id"]
                for r in spark.sql(f"""
                WITH booked AS (
                  SELECT h.value:hotelId::string AS id, max(b.created_at) AS last_booked
                  FROM {c}.silver.bookings b, LATERAL variant_explode(b.payload:hotelservice) h
                  GROUP BY 1)
                SELECT k.id FROM booked k
                LEFT JOIN (SELECT accommodation_id, max_by(status, fetched_at) AS status, max(fetched_at) AS at
                           FROM {ops}.accommodation_fetch GROUP BY 1) f ON f.accommodation_id = k.id
                WHERE k.id IS NOT NULL
                  AND (f.at IS NULL OR f.status = 'FAILED'
                       OR f.at < current_timestamp() - make_interval(0, 0, 0, {a.datasheet_refresh_days}))
                ORDER BY f.at NULLS FIRST, k.last_booked DESC
                LIMIT {a.datasheet_cap}""").collect()
            ]
            t0 = now()
            result = read_datasheets(tc, ids, lander("accommodation_datasheets"), a.threads) if ids else {}
            if result:
                spark.createDataFrame(
                    [(i, now(), s, run_id) for i, s in result.items()],
                    "accommodation_id STRING, fetched_at TIMESTAMP, status STRING, run_id STRING",
                ).write.mode("append").saveAsTable(f"{ops}.accommodation_fetch")
            ok = sum(s == "OK" for s in result.values())
            failed = sum(s == "FAILED" for s in result.values())
            st = "FAILED" if result and failed == len(result) else "SUCCESS"
            summaries.append(
                (run_id, t0, now(), "accommodation_datasheets", today, today, len(result), ok, ok, len(ids), 0)
                + (st, f"{failed} failed" if failed else None)
            )
            flush()

        # --- retention of landing files (not in bounded backfills, which run in parallel) -----
        cutoff = today - dt.timedelta(days=a.retention_days)
        for entity in [] if bounded else ALL_ENTITIES:
            base = f"{landing}/{entity}"
            for d in os.listdir(base) if os.path.isdir(base) else []:
                if d.startswith("ingest_date=") and d[len("ingest_date=") :] < cutoff.isoformat():
                    shutil.rmtree(f"{base}/{d}")
                    print(f"retention: removed {base}/{d}")
    finally:
        flush()

    # A bounded backfill window that is PARTIAL also fails the task: the job vio_travel_backfill_bookings is resumed with
    # "Repair run", which re-runs only the failed for_each iterations. A regular run leaves PARTIAL to the next run
    # (the watermark stays) and to the check e2e_extract_failing_or_stale.
    bad = [s for s in done if s[11] == "FAILED" or (bounded and s[11] == "PARTIAL")]
    if bad:
        raise RuntimeError(f"{len(bad)} extract(s) failed, see {ops}.ingest_runs run_id={run_id}")


if __name__ == "__main__":
    main()
