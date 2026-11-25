"""Task push_to_business_central - push the published business_central tables of catalog <catalog> into Business Central.

Sign-in: the tenant of the Business Central trial allows no app registration, so the task signs in with the delegated
login of the trial owner. Secret scope vio-business-central, key refresh_token, holds the refresh token of that login
(Azure CLI public client); local/store_business_central_token.py stores it after `az login`. The task trades it for an
access token with msal, renews the access token before it expires and on HTTP 401, and stores the rotated refresh
token back in the secret. The job runs as a service principal with WRITE on the scope.

Flow per run
1. Read the last row of <catalog>.ops.publish and read every business_central table AS OF those versions, so
   the run sees one consistent build that passed the blocking data-quality checks.
2. Read the sync state of every document key from ops.business_central_sync: the hash and result of the latest attempt, the
   latest logged row, and the failures since the latest terminal result. A document is sent when its current
   payload_hash differs from the hash of its latest attempt, or the latest attempt failed. Terminal results
   (TERMINAL) are not retried while the hash is unchanged. Transient results (TRANSIENT) are retried until
   MAX_FAILURES with the same hash, then the document waits as a dead letter until the data changes.
   Up to max_bookings_per_run bookings with something to send are pushed, oldest change first.
3. Held documents (hold_reason, or a purchase invoice whose sales invoice is held) are recorded as HELD and never
   sent; a HELD row is written again only when the hash or the reason changes.
4. Upsert customers and vendors whose latest synced hash differs, and complete posting groups through the
   ODataV4 pages workflowCustomers / workflowVendors (API v2.0 has no posting group fields).
5. Load missing exchange rates (configuration package, one per currency). Only documents in a currency whose
   rate could not be loaded are skipped.
6. Create or update draft invoices. Matching key: externalDocumentNumber (sales), vendorInvoiceNumber
   (purchase). A draft is never deleted (deleting a numbered draft creates an empty posted invoice). A posted
   document is never touched and is reported as NEEDS_CREDIT_MEMO. A cancelled booking or service empties the
   lines of its draft so it cannot be posted (CANCELLED_DRAFT_EMPTIED).
7. A document synced before that is no longer in the published tables (booking left BOOKED/RQ/CANCELED, active
   service cost dropped to 0, key changed) has its draft emptied (REMOVED_DRAFT_EMPTIED); a posted one is only recorded
   (REMOVED_POSTED). More than max_removals_per_run such documents stop the removal step for a human check; alert
   check e2e_push_removals_over_limit of check_data_quality reports it.
8. Write the result rows into ops.business_central_sync after every booking, so a crash loses at most one booking's rows; a
   document created but not recorded is found again by its key on the next run and updated, not duplicated.

The task fails when a document or party failed, so the job sends its failure e-mail.
Config: push_to_business_central.json next to this file (company, G/L accounts, posting groups, scope, limits).
"""

import argparse
import base64
import datetime as dt
import email.utils
import gzip
import json
import time
from decimal import Decimal
from pathlib import Path

import requests

# Results that leave a document in Business Central (RECREATED: first lab version).
SENT = ("CREATED", "UPDATED", "RECREATED", "CANCELLED_DRAFT_EMPTIED")
# Terminal results: the document is not retried while its payload_hash is unchanged.
TERMINAL = SENT + (
    "NEEDS_CREDIT_MEMO",  # posted in Business Central, never touched
    "NOTHING_TO_CANCEL",  # cancelled, and no document exists for the key
    "NO_EXCHANGE_RATE",  # the rate source does not publish the currency for that date
    "REMOVED_DRAFT_EMPTIED",
    "REMOVED_POSTED",
    "REMOVED_NOT_FOUND",
)
# Transient results: retried, and counted toward MAX_FAILURES for the same payload_hash.
TRANSIENT = ("FAILED", "SKIPPED", "NOT_EDITABLE")
# Results that state what Business Central holds for a key: the latest of these decides whether to resend.
ATTEMPT = TERMINAL + ("FAILED",)
# Results after which a document may exist in Business Central (a POST that failed may have been committed).
EVER_SENT = SENT + ("FAILED", "NEEDS_CREDIT_MEMO")
# Results after which nothing is left to empty when the document disappears from the published tables.
CLOSED = (
    "CANCELLED_DRAFT_EMPTIED",
    "NOTHING_TO_CANCEL",
    "REMOVED_DRAFT_EMPTIED",
    "REMOVED_POSTED",
    "REMOVED_NOT_FOUND",
)
REMOVED_HASH = "REMOVED"
MAX_FAILURES = 3
# Posted statuses of salesInvoices / purchaseInvoices. "Draft" is editable; " " and "In Review" are neither
# editable here nor posted. https://learn.microsoft.com/en-us/dynamics365/business-central/dev-itpro/api-reference/v2.0/resources/dynamics_salesinvoice
POSTED_STATUSES = ("Open", "Paid", "Canceled", "Corrective")
TOTAL = "totalAmountIncludingTax"  # field of salesInvoices and purchaseInvoices
# Max operations in one OData $batch request (Business Central online).
# https://learn.microsoft.com/en-us/dynamics365/business-central/dev-itpro/administration/operational-limits-online
MAX_BATCH = 100


CONFIG = "push_to_business_central.json"
# Delegated sign-in: refresh token of the Azure CLI public client, stored by local/store_business_central_token.py.
SECRET_SCOPE, SECRET_KEY = "vio-business-central", "refresh_token"
AZURE_CLI_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
BC_SCOPE = ["https://api.businesscentral.dynamics.com/.default"]
# An access token lives 60 to 90 minutes; it is renewed this many seconds before it expires.
RENEW_BEFORE_SECONDS = 300


def load_cfg(path=None):
    return json.loads(Path(path or Path(__file__).with_name(CONFIG)).read_text())


def lit(v):
    """SQL string literal for Databricks SQL (backslash is an escape character there)."""
    if v is None:
        return "NULL"
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def odata_str(v):
    return str(v).replace("'", "''")


def sql_list(values):
    return "(" + ", ".join(lit(v) for v in values) + ")"


def truthy(v):
    """Booleans come back from the SQL Statement Execution API as the strings 'true' / 'false'."""
    return v is True or str(v).lower() == "true"


def decimal_text(v):
    """Plain decimal text of a JSON number, without float rounding or scientific notation."""
    return format(Decimal(str(v)), "f")


def retry_after_seconds(value, attempt, cap=120):
    if value:
        try:
            return min(cap, max(1.0, float(value)))
        except ValueError:
            try:
                when = email.utils.parsedate_to_datetime(value)
                return min(cap, max(1.0, (when - dt.datetime.now(dt.UTC)).total_seconds()))
            except (TypeError, ValueError):
                pass
    return min(cap, 2**attempt * 2)


def fx_get(url, params, attempts=4, sleep=time.sleep):
    """GET on the rate source (Frankfurter), retried on timeouts, connection errors, 429 and 5xx (it answered
    522 and timed out on 17/11). A 404 or another 4xx is returned at once: the caller reads it as no rate."""
    for attempt in range(attempts):
        try:
            r = requests.get(url, params=params, timeout=30)
        except (requests.Timeout, requests.ConnectionError):
            if attempt == attempts - 1:
                raise
            sleep(retry_after_seconds(None, attempt, cap=30))
            continue
        if (r.status_code == 429 or r.status_code >= 500) and attempt < attempts - 1:
            sleep(retry_after_seconds(r.headers.get("Retry-After"), attempt, cap=30))
            continue
        return r
    return r


# ------------------------------------------------------------------ Databricks SQL
class SparkSQL:
    """Runs SQL on the Spark session of the task. q(sql) returns rows as dicts whose values are strings or None,
    the shape the SQL Statement Execution API returned when the push ran outside Databricks, so the callers parse
    numbers with Decimal and booleans with truthy. Booleans come back as 'true' / 'false', DATE as YYYY-MM-DD,
    TIMESTAMP as yyyy-MM-dd HH:mm:ss[.SSSSSS] in the session time zone, arrays, maps and structs as JSON."""

    def __init__(self, spark):
        self.spark = spark

    def q(self, sql):
        from pyspark.sql import functions as F
        from pyspark.sql.types import ArrayType, MapType, StructType

        df = self.spark.sql(sql)
        names = df.columns
        if not names:
            return []
        # positional names: a SELECT may return two columns with the same name
        df = df.toDF(*[f"c{n}" for n in range(len(names))])
        cols = [
            F.to_json(F.col(f.name)) if isinstance(f.dataType, ArrayType | MapType | StructType) else F.col(f.name)
            for f in df.schema.fields
        ]
        rows = df.select([c.cast("string").alias(f"c{n}") for n, c in enumerate(cols)]).collect()
        return [dict(zip(names, row, strict=True)) for row in rows]


# ------------------------------------------------------------------ Business Central sign-in
class RefreshTokenAuth:
    """Business Central access tokens from the refresh token in secret scope vio-business-central.

    token() returns the current access token and renews it RENEW_BEFORE_SECONDS before it expires; token(force=True)
    renews it at once (after HTTP 401). Every renewal stores the refresh token Microsoft Entra ID returns, which
    replaces the one used, back in the secret. No token is ever printed.
    secrets: the SecretsAPI of a WorkspaceClient (get_secret returns the value base64-encoded); app: an msal
    PublicClientApplication. Both are parameters so tests can pass fakes."""

    def __init__(self, tenant, secrets=None, app=None, clock=time.time):
        if secrets is None:
            from databricks.sdk import WorkspaceClient

            secrets = WorkspaceClient().secrets
        if app is None:
            import msal

            app = msal.PublicClientApplication(
                AZURE_CLI_CLIENT_ID, authority=f"https://login.microsoftonline.com/{tenant}"
            )
        self.secrets, self.app, self.clock = secrets, app, clock
        self.refresh_token, self.access_token, self.expires_at = None, None, 0.0

    def token(self, force=False):
        if force or self.access_token is None or self.clock() >= self.expires_at - RENEW_BEFORE_SECONDS:
            self._renew()
        return self.access_token

    def _renew(self):
        if self.refresh_token is None:
            secret = self.secrets.get_secret(scope=SECRET_SCOPE, key=SECRET_KEY)
            self.refresh_token = base64.b64decode(secret.value).decode()
        res = self.app.acquire_token_by_refresh_token(self.refresh_token, scopes=BC_SCOPE)
        if "access_token" not in res:
            reason = (res.get("error_description") or "").splitlines()[0][:200] if res.get("error_description") else ""
            raise RuntimeError(
                f"Microsoft Entra ID refused the refresh token in secret {SECRET_SCOPE}/{SECRET_KEY} "
                f"({res.get('error')}: {reason}). Run `az login --tenant <bc_tenant>` as the Business Central user, "
                "then local/store_business_central_token.py"
            )
        rotated = res.get("refresh_token")
        if rotated and rotated != self.refresh_token:
            self.secrets.put_secret(scope=SECRET_SCOPE, key=SECRET_KEY, string_value=rotated)
            self.refresh_token = rotated
        self.access_token = res["access_token"]
        self.expires_at = self.clock() + float(res.get("expires_in") or 3600)


# ------------------------------------------------------------------ Business Central
class BC:
    """Business Central API client. token(force=False) returns an access token (RefreshTokenAuth.token by default);
    it is asked before every request, so a long run renews the token before it expires."""

    def __init__(self, cfg, token=None, session=None, sleep=time.sleep):
        self.cfg, self.sleep = cfg, sleep
        self.token = token or RefreshTokenAuth(cfg["bc_tenant"]).token
        self.root = f"https://api.businesscentral.dynamics.com/v2.0/{cfg['bc_environment']}"
        self.s = session or requests.Session()
        self.s.headers.update({"Content-Type": "application/json"})
        comps = self.ok(self.call("GET", f"{self.root}/api/v2.0/companies"), "GET companies")["value"]
        cid = next(x["id"] for x in comps if x["name"] == cfg["bc_company"])
        self.company_id = cid
        self.api = f"{self.root}/api/v2.0/companies({cid})"
        self.odata = f"{self.root}/ODataV4/Company('{cfg['bc_company']}')"
        self.currencies = None

    def call(self, method, url, **kw):
        """Retries 429 for every method. 5xx and network errors are retried only for GET, PATCH and DELETE:
        a POST that timed out may have been committed, so it is not repeated; the next run finds the record
        by its key instead of creating a second one."""
        idempotent = method != "POST"
        renewed = renew = False
        for attempt in range(1, 7):
            self.s.headers["Authorization"] = f"Bearer {self.token(force=renew)}"
            renew = False
            try:
                r = self.s.request(method, url, timeout=(10, 120), **kw)
            except requests.RequestException:
                if idempotent and attempt < 6:
                    self.sleep(retry_after_seconds(None, attempt))
                    continue
                raise
            if r.status_code == 401 and not renewed:
                renewed = renew = True
                continue
            if r.status_code == 429 or (idempotent and r.status_code in (500, 502, 503, 504)):
                if attempt < 6:
                    self.sleep(retry_after_seconds(r.headers.get("Retry-After"), attempt))
                    continue
            return r
        return r

    @staticmethod
    def ok(r, what):
        if not r.ok:
            raise RuntimeError(f"{what}: {r.status_code} {r.text[:400]}")
        return r.json() if r.content else None

    def get_all(self, url, params=None, what="GET"):
        """Every row of a collection: Business Central pages results (server-driven paging, at most 20,000
        entities per page online) and links the next page in @odata.nextLink, which already carries the query.
        https://learn.microsoft.com/en-us/dynamics365/business-central/dev-itpro/webservices/server-driven-paging-in-odata-web-services
        """
        rows = []
        while url:
            page = self.ok(self.call("GET", url, params=params), what) or {}
            rows.extend(page.get("value", []))
            url, params = page.get("@odata.nextLink"), None
        return rows

    def batch(self, ops):
        """Run ops = [(method, url relative to /api/v2.0/, body or None)] as one transactional OData $batch.

        With the header `Isolation: snapshot` the whole batch runs in one session and one transaction, so when
        an inner request fails, the changes of the inner requests before it are rolled back. The exception is AL
        code that calls COMMIT inside an inner request; callers therefore also keep their operations idempotent.
        https://learn.microsoft.com/en-us/dynamics365/business-central/dev-itpro/webservices/use-odata-batch
        https://learn.microsoft.com/en-us/dynamics365/business-central/dev-itpro/developer/devenv-connect-apps-tips
        """
        if len(ops) > MAX_BATCH:
            raise RuntimeError(f"{len(ops)} operations exceed the $batch limit of {MAX_BATCH}")
        reqs = []
        for n, (method, url, body) in enumerate(ops, 1):
            headers = {"Content-Type": "application/json"}
            if method in ("PATCH", "DELETE"):
                headers["If-Match"] = "*"
            req = {"id": str(n), "method": method, "url": url, "headers": headers}
            if body is not None:
                req["body"] = body
            reqs.append(req)
        out = self.ok(
            self.call(
                "POST",
                f"{self.root}/api/v2.0/$batch",
                data=json.dumps({"requests": reqs}),
                headers={"Isolation": "snapshot", "Accept": "application/json"},
            ),
            "$batch",
        )
        responses = (out or {}).get("responses", [])
        for x in responses:
            if int(x.get("status") or 0) >= 400:
                msg = ((x.get("body") or {}).get("error") or {}).get("message", "")
                raise RuntimeError(f"$batch rolled back: request {x.get('id')} {x.get('status')} {msg}"[:400])
        if len(responses) != len(ops):
            raise RuntimeError(f"$batch answered {len(responses)} of {len(ops)} requests")
        return responses

    def find(self, entity, field, value):
        v = self.get_all(
            f"{self.api}/{entity}", params={"$filter": f"{field} eq '{odata_str(value)}'"}, what=f"GET {entity}"
        )
        if len(v) > 1:
            # Deleting a numbered draft makes Business Central post an empty invoice under the same external
            # number (seen on 15/11 with the first lab version). Such a posted zero document is not a duplicate.
            live = [x for x in v if not (x.get("status") in POSTED_STATUSES and not float(x.get(TOTAL, 0) or 0))]
            if len(live) != 1:
                raise RuntimeError(f"DUPLICATE_IN_BC: {len(v)} {entity} with {field} = {value}")
            return live[0]
        return v[0] if v else None

    def upsert_party(self, entity, page, body, setup):
        found = self.find(entity, "number", body["number"])
        if not found:
            rec, result = (
                self.ok(self.call("POST", f"{self.api}/{entity}", data=json.dumps(body)), f"POST {entity}"),
                "CREATED",
            )
        else:
            diff = {k: v for k, v in body.items() if k != "number" and found.get(k) != v}
            rec, result = found, "UNCHANGED"
            if diff:
                rec = self.ok(
                    self.call(
                        "PATCH", f"{self.api}/{entity}({found['id']})", data=json.dumps(diff), headers={"If-Match": "*"}
                    ),
                    f"PATCH {entity}",
                )
                result = "UPDATED"
        cur = self.ok(self.call("GET", f"{self.odata}/{page}({rec['id']})"), f"GET {page}")
        patch = {k: v for k, v in setup.items() if cur.get(k) != v}
        if patch:
            self.ok(
                self.call(
                    "PATCH", f"{self.odata}/{page}({rec['id']})", data=json.dumps(patch), headers={"If-Match": "*"}
                ),
                f"PATCH {page}",
            )
        return rec, result

    def has_currency(self, code):
        if self.currencies is None:
            self.currencies = {x["code"] for x in self.get_all(f"{self.api}/currencies", what="GET currencies")}
        return code == self.cfg["bc_local_currency"] or code in self.currencies

    def ensure_exchange_rates(self, needs, errors=None, fallback=None):
        """needs = {(currency, date)}. API v2.0 cannot insert exchange rates (entity set currencyExchangeRates
        is read-only), so missing rates are loaded as a RapidStart configuration package (table 330 Currency
        Exchange Rate) through the automation API: create package, upload file, import, apply, delete.
        Rates are ECB reference rates (fx_source) with base LCY, so the published value goes in unchanged as
        Exchange Rate Amount (foreign) against Relational Exch. Rate Amount 1 (LCY): no division, no rounding.
        Both fields are Decimal in table 330.
        https://learn.microsoft.com/en-us/dynamics365/business-central/application/base-application/table/microsoft.finance.currency.currency-exchange-rate

        Each currency is loaded on its own. When errors is a dict, a currency that fails is recorded there
        (currency -> message) and the others still load; when errors is None, the first failure is raised.

        fallback(pairs) -> {(currency, date): foreign per 1 LCY} supplies the pairs the ECB does not publish (VND
        and FJD, for example); see tc_implied_rates.

        Returns (loaded, unavailable): the rates loaded as (currency, date, foreign per 1 LCY, source), and the
        (currency, date) pairs no source has."""
        lcy = self.cfg["bc_local_currency"]
        days = {}
        for cur, day in needs:
            if cur and cur != lcy:
                days.setdefault(cur, set()).add(day)
        loaded, unavailable = [], set()
        for cur in sorted(days):
            try:
                have = {
                    r["startingDate"]
                    for r in self.get_all(
                        f"{self.api}/currencyExchangeRates",
                        params={"$filter": f"currencyCode eq '{odata_str(cur)}'"},
                        what="GET rates",
                    )
                }
                rows, missing = [], []
                for day in sorted(days[cur] - have):
                    r = fx_get(f"{self.cfg['fx_source']}/{day}", {"base": lcy, "symbols": cur})
                    if r.status_code == 404 or (r.ok and cur not in r.json().get("rates", {})):
                        missing.append(day)
                        continue
                    r.raise_for_status()
                    rows.append((cur, day, decimal_text(r.json()["rates"][cur]), "ECB"))  # 1 LCY = x foreign
                derived = fallback([(cur, d) for d in missing]) if fallback and missing else {}
                for day in missing:
                    if (cur, day) in derived:
                        rows.append((cur, day, derived[(cur, day)], "Travel Compositor"))
                    else:
                        unavailable.add((cur, day))
                if rows:
                    recs = "".join(
                        f"<CurrencyExchangeRate><CurrencyCode>{c}</CurrencyCode><StartingDate>{d}</StartingDate>"
                        f"<ExchangeRateAmount>{x}</ExchangeRateAmount><AdjustmentExchRateAmount>{x}"
                        f"</AdjustmentExchRateAmount><RelationalExchRateAmount>1</RelationalExchRateAmount>"
                        f"<RelationalAdjmtExchRateAmt>1</RelationalAdjmtExchRateAmt></CurrencyExchangeRate>"
                        for c, d, x, _ in rows
                    )
                    self.apply_package("FX" + cur, 330, "CurrencyExchangeRateList", recs)
                    loaded += rows
            except Exception as e:  # noqa: BLE001
                if errors is None:
                    raise
                errors[cur] = str(e)[:500]
        return loaded, unavailable

    def apply_package(self, prefix, table_id, list_tag, records_xml):
        """Insert or modify rows of one Business Central table with a RapidStart configuration package through
        the automation API: create package, upload file, import, apply, delete. records_xml holds one element per
        row, named after the table, with one child per field (field name without spaces and dots)."""
        code = prefix + dt.datetime.now(dt.UTC).strftime("%m%d%H%M%S")
        xml = (
            f'<?xml version="1.0" encoding="UTF-16" standalone="yes"?><DataList Code="{code}" '
            f'PackageName="Vio {code}" LanguageID="1033" ProcessingOrder="0" ExcludeConfigTables="1">'
            f"<{list_tag}><TableID>{table_id}</TableID><PackageCode>{code}</PackageCode>{records_xml}"
            f"</{list_tag}></DataList>"
        )
        auto = f"{self.root}/api/microsoft/automation/v2.0/companies({self.cfg['bc_company_id']})"
        pkg = self.ok(
            self.call(
                "POST",
                f"{auto}/configurationPackages",
                data=json.dumps({"code": code, "packageName": f"Vio {code}"}),
            ),
            "create package",
        )
        pid = pkg["id"]
        try:
            self.ok(
                self.call(
                    "PATCH",
                    f"{auto}/configurationPackages({pid})/file('{code}')/content",
                    data=gzip.compress(xml.encode("utf-16")),
                    headers={"If-Match": "*", "Content-Type": "application/octet-stream"},
                ),
                "upload package",
            )
            for action, field in (("import", "importStatus"), ("apply", "applyStatus")):
                self.ok(self.call("POST", f"{auto}/configurationPackages({pid})/Microsoft.NAV.{action}"), action)
                st = {}
                for _ in range(60):
                    st = self.ok(self.call("GET", f"{auto}/configurationPackages({pid})"), "GET package")
                    if st.get(field) in ("Completed", "Error"):
                        break
                    self.sleep(3)
                if st.get(field) != "Completed" or st.get("numberOfErrors"):
                    raise RuntimeError(
                        f"configuration package {code} {action}: {st.get(field)}, errors {st.get('numberOfErrors')}"
                    )
        finally:
            # a package left behind holds a copy of the rates and shows in Configuration Packages; report it
            gone = self.call("DELETE", f"{auto}/configurationPackages({pid})", headers={"If-Match": "*"})
            if gone.status_code >= 300:
                print(f"configuration package {code} not deleted: HTTP {gone.status_code}; delete it by hand")


def tc_implied_rates(db, cfg, pairs, window_days=30, min_services=3):
    """Rates for currencies the ECB does not publish, as foreign per 1 LCY on each (currency, date) of pairs.

    Travel Compositor prices every service in its supplier currency (cost_amount, cost_currency) and also in the
    booking currency (cost_in_sale_currency, sale_currency USD), so cost_amount / cost_in_sale_currency is the
    rate Vio's own booking platform used. Silver keeps the current version of each booking only, so the rate is the
    median over the services in that currency whose last update is nearest to the date (within window_days of the
    nearest one; at least min_services; services under 10 USD skipped because of 2-decimal rounding), then
    multiplied by the ECB USD rate of that date to reach the LCY base Business Central expects."""
    lcy, out = cfg["bc_local_currency"], {}
    if not pairs:
        return out
    values = ",".join(f"('{odata_str(c)}', DATE'{d}')" for c, d in pairs)
    rows = db.q(f"""
        WITH need(cur, day) AS (VALUES {values}),
        cand AS (
            SELECT n.cur, n.day, s.cost_amount / s.cost_in_sale_currency AS rate,
                   abs(datediff(to_date(s.last_update_at), n.day)) AS dist
            FROM need n JOIN {cfg["catalog"]}.silver.booking_services s
              ON s.cost_currency = n.cur AND s.sale_currency = 'USD' AND s.cost_amount > 0
             AND s.cost_in_sale_currency >= 10),
        nearest AS (SELECT cur, day, min(dist) AS d0 FROM cand GROUP BY ALL)
        SELECT c.cur, CAST(c.day AS STRING) AS day, percentile(c.rate, 0.5) AS per_usd, count(*) AS services
        FROM cand c JOIN nearest x ON x.cur = c.cur AND x.day = c.day
        WHERE c.dist <= x.d0 + {int(window_days)}
        GROUP BY ALL HAVING count(*) >= {int(min_services)}""")
    for r in rows:
        u = fx_get(f"{cfg['fx_source']}/{r['day']}", {"base": lcy, "symbols": "USD"})
        u.raise_for_status()
        usd_per_lcy = Decimal(str(u.json()["rates"]["USD"]))
        out[(r["cur"], r["day"])] = format((Decimal(r["per_usd"]) * usd_per_lcy).quantize(Decimal("0.000001")), "f")
    return out


def replace_lines(bc, entity, lines_key, doc_id, header, lines):
    """Delete every line of a draft, patch its header (unless header is None) and add the new lines, in one
    transactional $batch, so a failure leaves the draft as it was instead of half updated.

    Why $batch and not separate calls: with separate calls a failure after the deletes left an empty or partial
    draft that a user could post before the next run. The order is also idempotent (all current lines are read
    and deleted, then the target lines are added), so if AL code commits inside the batch and a later request
    fails, the next run reads the draft again and converges on the same result; the document is recorded FAILED
    and retried up to MAX_FAILURES."""
    rel = f"companies({bc.company_id})/{entity}({doc_id})"
    old = bc.get_all(f"{bc.api}/{entity}({doc_id})/{lines_key}", what="GET lines")
    ops = [("DELETE", f"{rel}/{lines_key}({x['id']})", None) for x in old]
    if header is not None:
        ops.append(("PATCH", rel, header))
    ops += [("POST", f"{rel}/{lines_key}", line) for line in lines]
    if ops:
        bc.batch(ops)


def push_document(bc, kind, key_field, key, body, lines, cancel=False):
    """Create a draft document, update the existing draft in place, or empty it when cancelled.

    A draft is never deleted: when the invoice number series is also the posted invoice number series
    (true in My Company), deleting a numbered draft makes Business Central create an empty posted
    invoice to fill the gap (BCApps, ERMPurchaseInvoice.Codeunit.al, scenario 226743).
    Only the posted statuses (POSTED_STATUSES) give NEEDS_CREDIT_MEMO; any other status than Draft
    (" ", "In Review") is left alone as NOT_EDITABLE, a transient result retried up to MAX_FAILURES."""
    entity = "salesInvoices" if kind == "sales" else "purchaseInvoices"
    lines_key = "salesInvoiceLines" if kind == "sales" else "purchaseInvoiceLines"
    existing = bc.find(entity, key_field, key)
    if existing and existing.get("status") in POSTED_STATUSES:
        return "NEEDS_CREDIT_MEMO", existing
    if existing and existing.get("status") != "Draft":
        return "NOT_EDITABLE", existing
    if cancel:
        if not existing:
            return "NOTHING_TO_CANCEL", None
        replace_lines(bc, entity, lines_key, existing["id"], None, [])
        return "CANCELLED_DRAFT_EMPTIED", existing
    if not existing:
        return "CREATED", bc.ok(
            bc.call("POST", f"{bc.api}/{entity}", data=json.dumps({**body, lines_key: lines})), f"POST {entity}"
        )
    replace_lines(bc, entity, lines_key, existing["id"], body, lines)
    return "UPDATED", bc.ok(bc.call("GET", f"{bc.api}/{entity}({existing['id']})"), f"GET {entity}")


REMOVED_RESULT = {
    "CANCELLED_DRAFT_EMPTIED": "REMOVED_DRAFT_EMPTIED",
    "NOTHING_TO_CANCEL": "REMOVED_NOT_FOUND",
    "NEEDS_CREDIT_MEMO": "REMOVED_POSTED",
}


def remove_document(bc, kind, key):
    """A document synced before that is no longer in the published tables: empty its draft (never delete it,
    see push_document), only record a posted one. The purchase key is also the vendorInvoiceNumber
    (src/build_business_central_tables.py builds both from service_id and type_code)."""
    key_field = "externalDocumentNumber" if kind == "sales" else "vendorInvoiceNumber"
    result, rec = push_document(bc, kind, key_field, key, None, [], cancel=True)
    return REMOVED_RESULT.get(result, result), rec


def process_removals(bc, rows, max_removals, record):
    """Remove the documents in rows ({doc_type, doc_key}). Returns False, and changes nothing, when there are
    more than max_removals: that many at once points to a broken build or a key change, which a person checks."""
    if len(rows) > max_removals:
        sample = ", ".join(f"{r['doc_type']}/{r['doc_key']}" for r in rows[:5])
        print(f"{len(rows)} synced documents are no longer published, more than max_removals_per_run = {max_removals}")
        print(f"removal step skipped; check the build, then raise max_removals_per_run. Sample: {sample}")
        return False
    for r in rows:
        try:
            result, rec = remove_document(bc, r["doc_type"], r["doc_key"])
            error = {
                "REMOVED_POSTED": "posted document no longer in the source, needs a credit memo",
                "NOT_EDITABLE": f"status {(rec or {}).get('status')!r}",
            }.get(result)
            record(r["doc_type"], r["doc_key"], REMOVED_HASH, rec, result, error)
        except Exception as e:  # noqa: BLE001
            record(r["doc_type"], r["doc_key"], REMOVED_HASH, None, "FAILED", str(e)[:500])
    return True


# ------------------------------------------------------------------ what to send
def doc_decision(row, hold_reason, retry=frozenset()):
    """HELD, HELD_LOGGED, DONE, DEAD or SEND for one document row joined with its sync state (state_sql).
    retry: results (from --retry) whose documents are sent again although their hash did not change, for example
    NO_EXCHANGE_RATE after a new rate source, or dead-lettered FAILED or SKIPPED after the cause was fixed."""
    h = row["payload_hash"]
    if hold_reason and row["doc_action"] == "UPSERT":
        logged = row.get("log_result") == "HELD" and row.get("log_hash") == h and row.get("log_error") == hold_reason
        return "HELD_LOGGED" if logged else "HELD"
    if row.get("attempt_result") in retry or row.get("log_result") in retry:
        return "SEND"
    if row.get("attempt_result") in TERMINAL and row.get("attempt_hash") == h:
        return "DONE"
    if int(row.get("failures") or 0) >= MAX_FAILURES:
        return "DEAD"
    if row["doc_action"] == "CANCEL" and not truthy(row.get("ever_sent")):
        return "DONE"  # a cancelled document is only sent when something was sent for its key before
    return "SEND"


def plan(sales, purchases, limit, retry=frozenset()):
    """Decide per document. Returns (todo, held, pending):
    todo    up to limit sales rows, oldest change first, of bookings with a sales or purchase document to send;
            each carries _send (bool) and _purchases (purchase rows to send)
    held    (doc_type, key, payload_hash, reason) to record as HELD
    pending number of bookings with something to send (more than len(todo) means a backlog)"""
    held, by_ref = [], {}
    for s in sales:
        s["_decision"] = doc_decision(s, s.get("hold_reason"), retry)
        s["_send"] = s["_decision"] == "SEND"
        s["_purchases"] = []
        by_ref[s["external_document_number"]] = s
        if s["_decision"] == "HELD":
            held.append(("sales", s["external_document_number"], s["payload_hash"], s["hold_reason"]))
    for p in purchases:
        reason = p.get("hold_reason") or (
            f"SALES_HELD {p['sales_hold_reason']}" if p.get("sales_hold_reason") else None
        )
        d = doc_decision(p, reason, retry)
        if d == "HELD":
            held.append(("purchase", p["purchase_key"], p["payload_hash"], reason))
        elif d == "SEND":
            # a booking whose services all sell at 0 has no sales invoice but can still owe its suppliers
            stub = {"external_document_number": p["booking_ref"], "doc_action": None, "_decision": "NONE"}
            stub.update(_send=False, _purchases=[], last_update_at=p.get("invoice_date"))
            by_ref.setdefault(p["booking_ref"], stub)["_purchases"].append(p)
    pending = sorted(
        (s for s in by_ref.values() if s["_send"] or s["_purchases"]), key=lambda s: str(s.get("last_update_at") or "")
    )
    return pending[:limit], held, len(pending)


def state_sql(c, company):
    """CTEs over ops.business_central_sync. st: per document key, the hash and result of the latest attempt (ATTEMPT), the
    latest row of any result, and whether a document may exist in Business Central. fl: per key and hash,
    the transient results since the latest terminal one."""
    return f"""WITH st AS (
        SELECT doc_type, doc_key,
               max_by(payload_hash, synced_at) FILTER (WHERE result IN {sql_list(ATTEMPT)}) AS attempt_hash,
               max_by(result, synced_at) FILTER (WHERE result IN {sql_list(ATTEMPT)}) AS attempt_result,
               max(synced_at) FILTER (WHERE result IN {sql_list(TERMINAL)}) AS terminal_at,
               max_by(payload_hash, synced_at) AS log_hash,
               max_by(result, synced_at) AS log_result,
               max_by(error, synced_at) AS log_error,
               bool_or(result IN {sql_list(EVER_SENT)}) AS ever_sent
        FROM {c}.ops.business_central_sync WHERE bc_company = {company} AND doc_type IN ('sales', 'purchase')
        GROUP BY doc_type, doc_key),
      fl AS (
        SELECT f.doc_type, f.doc_key, f.payload_hash, count(*) AS failures
        FROM {c}.ops.business_central_sync f JOIN st ON st.doc_type = f.doc_type AND st.doc_key = f.doc_key
        WHERE f.bc_company = {company} AND f.result IN {sql_list(TRANSIENT)}
          AND (st.terminal_at IS NULL OR f.synced_at > st.terminal_at)
        GROUP BY f.doc_type, f.doc_key, f.payload_hash)
    """


STATE_COLS = "st.attempt_hash, st.attempt_result, st.log_hash, st.log_result, st.log_error, st.ever_sent"


def doc_queries(c, company, g, scope):
    """SQL for the sales and purchase documents in scope with their sync state, and for the documents synced
    before that are no longer in the published tables. scope filters sales_invoices, alias i, and bounds new
    documents only: a document sent before whose payload changed is always selected, so a late cancellation of an
    old booking still empties its draft."""
    with_state = state_sql(c, company)
    # a purchase whose booking has no sales invoice (every service sold at 0) falls back to its own invoice date
    purchase_scope = scope.replace("i.invoice_date", "coalesce(i.invoice_date, p.invoice_date)")
    sales = f"""{with_state}
        SELECT i.*, {STATE_COLS}, coalesce(fl.failures, 0) AS failures
        FROM {g["sales_invoices"]} i
        LEFT JOIN st ON st.doc_type = 'sales' AND st.doc_key = i.external_document_number
        LEFT JOIN fl ON fl.doc_type = 'sales' AND fl.doc_key = i.external_document_number
                    AND fl.payload_hash = i.payload_hash
        WHERE ({scope}) OR (st.ever_sent AND st.attempt_hash IS DISTINCT FROM i.payload_hash)"""
    purchases = f"""{with_state}
        SELECT p.*, i.hold_reason AS sales_hold_reason, {STATE_COLS}, coalesce(fl.failures, 0) AS failures
        FROM {g["purchase_invoices"]} p
        LEFT JOIN {g["sales_invoices"]} i ON i.external_document_number = p.booking_ref
        LEFT JOIN st ON st.doc_type = 'purchase' AND st.doc_key = p.purchase_key
        LEFT JOIN fl ON fl.doc_type = 'purchase' AND fl.doc_key = p.purchase_key AND fl.payload_hash = p.payload_hash
        WHERE ({purchase_scope}) OR (st.ever_sent AND st.attempt_hash IS DISTINCT FROM p.payload_hash)"""
    removed = f"""{with_state}
        SELECT st.doc_type, st.doc_key, st.attempt_result FROM st
        LEFT ANTI JOIN (SELECT 'sales' AS doc_type, external_document_number AS doc_key FROM {g["sales_invoices"]}
                        UNION ALL SELECT 'purchase', purchase_key FROM {g["purchase_invoices"]}) cur
          ON cur.doc_type = st.doc_type AND cur.doc_key = st.doc_key
        WHERE st.ever_sent AND st.attempt_result NOT IN {sql_list(CLOSED)}
        ORDER BY st.doc_type, st.doc_key"""
    return {"sales": sales, "purchases": purchases, "removed": removed}


def main():
    from pyspark.sql import SparkSession

    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="vio_travel")
    ap.add_argument("--run_id", default="")
    ap.add_argument("--config", help=f"path of {CONFIG}; default: next to this file")
    ap.add_argument(
        "--retry",
        default="",
        help="comma-separated results to send again although the payload did not change, e.g. NO_EXCHANGE_RATE,FAILED",
    )
    a = ap.parse_args()
    cfg = {**load_cfg(a.config), "catalog": a.catalog}
    retry = frozenset(x.strip() for x in a.retry.split(",") if x.strip())
    failed = run(cfg, SparkSQL(SparkSession.builder.getOrCreate()), a.run_id, retry)
    if failed:
        # fails the task, so the job sends its failure e-mail
        raise RuntimeError(f"{failed} documents or parties failed; see result FAILED in ops.business_central_sync")


def run(cfg, db, run_id="", retry=frozenset(), bc=None):
    """One push. Returns the number of FAILED results."""
    limit = cfg.get("max_bookings_per_run", 100)
    c = cfg["catalog"]
    pub = db.q(f"SELECT run_id, versions FROM {c}.ops.publish ORDER BY published_at DESC LIMIT 1")
    if not pub:
        print("no published business_central build yet")
        return 0
    ver = json.loads(pub[0]["versions"])
    g = {t: f"{c}.business_central.{t} VERSION AS OF {v}" for t, v in ver.items()}
    print(f"job run {run_id}: reading business_central published by run {pub[0]['run_id']}: {ver}")
    db.q(f"""CREATE TABLE IF NOT EXISTS {c}.ops.business_central_sync (synced_at TIMESTAMP, doc_type STRING, doc_key STRING,
             payload_hash STRING, bc_company STRING, bc_id STRING, bc_number STRING, result STRING, error STRING)""")
    company = lit(cfg["bc_company"])
    q = doc_queries(c, company, g, cfg.get("scope_sql", "TRUE"))
    todo, held, pending = plan(db.q(q["sales"]), db.q(q["purchases"]), limit, retry)
    removed = db.q(q["removed"])
    if not todo and not held and not removed:
        print("nothing to push")
        return 0
    p_heads = [p for h in todo for p in h["_purchases"]]

    def party_synced(kind):
        # latest synced hash per party, so a party that returns to an earlier state is upserted again
        return f"""(SELECT doc_key, max_by(payload_hash, synced_at) AS payload_hash FROM {c}.ops.business_central_sync
                    WHERE doc_type = '{kind}' AND result IN ('CREATED', 'UPDATED', 'UNCHANGED', 'UPSERTED')
                      AND bc_company = {company} GROUP BY doc_key)"""

    cust_nos = ",".join(lit(h["customer_number"]) for h in todo if h["_send"] and h["doc_action"] == "UPSERT") or "''"
    vend_nos = ",".join(lit(p["vendor_number"]) for p in p_heads if p["doc_action"] == "UPSERT") or "''"
    custs = db.q(f"""SELECT x.* FROM {g["customers"]} x
        LEFT ANTI JOIN {party_synced("customer")} s ON s.doc_key = x.bc_number AND s.payload_hash = x.payload_hash
        WHERE x.bc_number IN ({cust_nos})""")
    vends = db.q(f"""SELECT x.* FROM {g["vendors"]} x
        LEFT ANTI JOIN {party_synced("vendor")} s ON s.doc_key = x.bc_number AND s.payload_hash = x.payload_hash
        WHERE x.bc_number IN ({vend_nos})""")
    refs = ",".join(lit(h["external_document_number"]) for h in todo) or "''"
    s_lines = db.q(f"SELECT * FROM {g['sales_lines']} WHERE booking_ref IN ({refs}) ORDER BY booking_ref, line_no")
    p_lines = db.q(f"""SELECT * FROM {g["purchase_lines"]} WHERE booking_ref IN ({refs})
                       ORDER BY purchase_key, line_no""")
    print(
        f"to push: {len(todo)} of {pending} bookings, {sum(h['_send'] for h in todo)} sales and {len(p_heads)} "
        f"purchase docs, {len(held)} held to record, {len(removed)} no longer published, "
        f"{len(custs)} customers and {len(vends)} vendors to upsert"
    )
    bc = bc or BC(cfg)
    lcy = cfg["bc_local_currency"]
    results, failed_total = [], 0

    def record(doc_type, key, phash, rec, result, error=None):
        nonlocal failed_total
        failed_total += result == "FAILED"
        # microseconds keep rows of one key in order for max_by(..., synced_at)
        now = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        results.append(
            f"(TIMESTAMP'{now}', {lit(doc_type)}, {lit(key)}, {lit(phash)}, {company}, "
            f"{lit((rec or {}).get('id'))}, {lit((rec or {}).get('number'))}, {lit(result)}, {lit(error)})"
        )
        print(f"{doc_type:9} {key:28} {result:24} {(rec or {}).get('number', '')} {error or ''}")

    def flush():
        if results:
            db.q(f"INSERT INTO {c}.ops.business_central_sync VALUES " + ",".join(results))
            results.clear()

    try:
        needs = {(h["currency_code"], h["invoice_date"]) for h in todo if h["_send"] and h["doc_action"] == "UPSERT"}
        needs |= {(p["currency_code"], p["invoice_date"]) for p in p_heads if p["doc_action"] == "UPSERT"}
        fx_errors = {}
        loaded, no_rate = bc.ensure_exchange_rates(
            needs, errors=fx_errors, fallback=lambda pairs: tc_implied_rates(db, cfg, pairs)
        )
        for cur, day, x, source in loaded:
            record("fx_rate", f"{cur}/{day}", None, None, "LOADED", f"{x} {cur} = 1 {lcy} ({source})")
        for cur, day in sorted(no_rate):
            record(
                "fx_rate", f"{cur}/{day}", None, None, "UNAVAILABLE", "no ECB rate and too few services to derive one"
            )
        for cur, day in sorted(p for p in needs if p[0] in fx_errors):
            record("fx_rate", f"{cur}/{day}", None, None, "FAILED", fx_errors[cur])

        failed_parties = set()
        for kind, rows in (("customer", custs), ("vendor", vends)):
            for x in rows:
                body = {"number": x["bc_number"], "displayName": x["display_name"], "email": x["email"] or ""}
                if kind == "customer":
                    body.update({"type": "Company", "city": x["city"] or "", "addressLine1": x["address_line1"] or ""})
                try:
                    rec, result = bc.upsert_party(f"{kind}s", f"workflow{kind.title()}s", body, cfg[f"{kind}_setup"])
                    record(kind, x["bc_number"], x["payload_hash"], rec, result)
                except Exception as e:  # noqa: BLE001
                    failed_parties.add(x["bc_number"])
                    record(kind, x["bc_number"], x["payload_hash"], None, "FAILED", str(e)[:500])
        for doc_type, key, phash, reason in held:
            record(doc_type, key, phash, None, "HELD", reason)
        flush()
        # A tripped guard needs a person, not a retry: the task does not fail for it; alert check
        # e2e_push_removals_over_limit of check_data_quality reports it once, and again at the 08:00 run.
        process_removals(bc, removed, cfg.get("max_removals_per_run", 50), record)
        flush()

        def send(doc_type, key, phash, party, cur, day, fn):
            if party in failed_parties:
                return record(doc_type, key, phash, None, "SKIPPED", f"{party} upsert failed")
            if (cur, day) in no_rate:
                return record(doc_type, key, phash, None, "NO_EXCHANGE_RATE", f"no exchange rate {cur} {day}")
            if cur in fx_errors:
                return record(doc_type, key, phash, None, "SKIPPED", f"exchange rate load failed for {cur}")
            try:
                if cur and not bc.has_currency(cur):
                    raise RuntimeError(f"currency {cur} missing in Business Central")
                result, rec = fn()
                error = f"status {(rec or {}).get('status')!r}" if result == "NOT_EDITABLE" else None
                record(doc_type, key, phash, rec, result, error)
            except Exception as e:  # noqa: BLE001
                record(doc_type, key, phash, None, "FAILED", str(e)[:500])

        for h in todo:
            ref = h["external_document_number"]
            cancel = h["doc_action"] == "CANCEL"
            if h["_send"]:
                lines = [
                    {
                        "lineType": "Account",
                        "lineObjectNumber": cfg["sales_gl_account"],
                        "description": ln["description"],
                        "quantity": 1,
                        "unitPrice": float(ln["amount"]),
                    }
                    for ln in s_lines
                    if ln["booking_ref"] == ref
                ]
                body = {
                    "externalDocumentNumber": ref,
                    "customerNumber": h["customer_number"],
                    "invoiceDate": h["invoice_date"],
                    "postingDate": h["invoice_date"],
                    "currencyCode": "" if h["currency_code"] == lcy else h["currency_code"],
                }
                send(
                    "sales",
                    ref,
                    h["payload_hash"],
                    None if cancel else h["customer_number"],
                    None if cancel else h["currency_code"],
                    h["invoice_date"],
                    lambda ref=ref, body=body, lines=lines, cancel=cancel: push_document(
                        bc, "sales", "externalDocumentNumber", ref, body, lines, cancel
                    ),
                )
            for p in h["_purchases"]:
                pc = p["doc_action"] == "CANCEL"
                lines = [
                    {
                        "lineType": "Account",
                        "lineObjectNumber": cfg["purchase_gl_account"],
                        "description": ln["description"],
                        "quantity": 1,
                        "unitCost": float(ln["amount"]),
                    }
                    for ln in p_lines
                    if ln["purchase_key"] == p["purchase_key"]
                ]
                body = {
                    "vendorInvoiceNumber": p["vendor_invoice_number"],
                    "vendorNumber": p["vendor_number"],
                    "invoiceDate": p["invoice_date"],
                    "postingDate": p["invoice_date"],
                    "currencyCode": "" if p["currency_code"] == lcy else p["currency_code"],
                }
                send(
                    "purchase",
                    p["purchase_key"],
                    p["payload_hash"],
                    None if pc else p["vendor_number"],
                    None if pc else p["currency_code"],
                    p["invoice_date"],
                    lambda p=p, body=body, lines=lines, pc=pc: push_document(
                        bc, "purchase", "vendorInvoiceNumber", p["vendor_invoice_number"], body, lines, pc
                    ),
                )
            flush()
    finally:
        flush()
    print(f"done: results written to {c}.ops.business_central_sync, {failed_total} failed")
    return failed_total


if __name__ == "__main__":
    main()
