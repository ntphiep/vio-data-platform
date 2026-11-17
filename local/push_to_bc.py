"""Push the published business_central tables from Databricks into Business Central (runs on this machine).

Why local: the Business Central token is a delegated Azure CLI login (`az login` as the trial owner).
A Databricks job cannot do an interactive login, and an app registration needs a tenant admin.

Flow per run
1. Read the last row of <catalog>.ops.publish and read every business_central table AS OF those versions, so
   the run sees one consistent build that passed the blocking data-quality checks.
2. Read the sync state of every document key from ops.bc_sync: the hash and result of the latest attempt, the
   latest logged row, and the failures since the latest terminal result. A document is sent when its current
   payload_hash differs from the hash of its latest attempt, or the latest attempt failed. Terminal results
   (TERMINAL) are not retried while the hash is unchanged. Transient results (TRANSIENT) are retried until
   MAX_FAILURES with the same hash, then the document waits as a dead letter until the data changes.
   Up to --limit bookings with something to send are pushed, oldest change first.
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
7. A document synced before that is no longer in the published tables (booking left BOOKED/RQ/CANCELED, service
   cost dropped to 0, key changed) has its draft emptied (REMOVED_DRAFT_EMPTIED); a posted one is only recorded
   (REMOVED_POSTED). More than max_removals_per_run such documents stop the removal step for a human check.
8. Write the result rows into ops.bc_sync after every booking, so a crash loses at most one booking's rows; a
   document created but not recorded is found again by its key on the next run and updated, not duplicated.

Timer mode (--on-new-run, used by the systemd timer every 5 minutes): the run asks the Jobs API, which needs no
SQL warehouse, whether job vio_travel_pipeline finished a run since the last push, and exits at once when it did
not and the last push left nothing behind. State lives in .push_state.json next to this file.

Usage: uv run --no-project --with databricks-sdk --with requests push_to_bc.py [--limit N] [--dry-run] [--on-new-run]
Config: config.json next to this file. The systemd unit wraps the run in flock, so two runs never overlap.
"""

import argparse
import datetime as dt
import email.utils
import gzip
import json
import subprocess
import sys
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
# Max operations in one OData $batch request (Business Central online).
# https://learn.microsoft.com/en-us/dynamics365/business-central/dev-itpro/administration/operational-limits-online
MAX_BATCH = 100


STATE = Path(__file__).parent / ".push_state.json"


def load_cfg(path=None):
    return json.loads(Path(path or Path(__file__).parent / "config.json").read_text())


def job_run_to_push(cfg, state):
    """Timer mode: the latest finished run of the job when there is something new to push, else None."""
    from databricks.sdk import WorkspaceClient

    runs = list(WorkspaceClient().jobs.list_runs(job_id=cfg["job_id"], completed_only=True, limit=1))
    latest = runs[0].run_id if runs else None
    if latest is not None and (latest != state.get("last_job_run") or state.get("backlog")):
        return latest
    return None


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


# ------------------------------------------------------------------ Databricks SQL
class DBX:
    def __init__(self, cfg):
        from databricks.sdk import WorkspaceClient

        self.w, self.cfg = WorkspaceClient(), cfg
        self.c = cfg["catalog"]

    def q(self, sql, deadline_s=600):
        from databricks.sdk.service.sql import StatementState

        ex = self.w.statement_execution
        r = ex.execute_statement(statement=sql, warehouse_id=self.cfg["warehouse_id"], wait_timeout="50s")
        t_end = time.time() + deadline_s
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.time() > t_end:
                ex.cancel_execution(r.statement_id)
                raise RuntimeError(f"SQL timed out after {deadline_s}s")
            time.sleep(3)
            r = ex.get_statement(r.statement_id)
        if r.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(f"SQL failed: {r.status.error.message if r.status.error else r.status.state}")
        cols = [col.name for col in r.manifest.schema.columns] if r.manifest and r.manifest.schema else []
        rows, res = [], r.result
        while res is not None:  # follow every result chunk
            rows.extend(res.data_array or [])
            nxt = res.next_chunk_index
            res = ex.get_statement_result_chunk_n(r.statement_id, nxt) if nxt is not None else None
        return [dict(zip(cols, row, strict=True)) for row in rows]


# ------------------------------------------------------------------ Business Central
class BC:
    def __init__(self, cfg, session=None, sleep=time.sleep):
        self.cfg, self.sleep = cfg, sleep
        self.root = f"https://api.businesscentral.dynamics.com/v2.0/{cfg['bc_environment']}"
        self.s = session or requests.Session()
        self.s.headers.update({"Content-Type": "application/json"})
        self._token()
        comps = self.ok(self.call("GET", f"{self.root}/api/v2.0/companies"), "GET companies")["value"]
        cid = next(x["id"] for x in comps if x["name"] == cfg["bc_company"])
        self.company_id = cid
        self.api = f"{self.root}/api/v2.0/companies({cid})"
        self.odata = f"{self.root}/ODataV4/Company('{cfg['bc_company']}')"
        self.currencies = None

    def _token(self):
        out = subprocess.run(
            [
                "az",
                "account",
                "get-access-token",
                "--resource",
                "https://api.businesscentral.dynamics.com",
                "--tenant",
                self.cfg["bc_tenant"],
                "--query",
                "accessToken",
                "-o",
                "tsv",
            ],
            capture_output=True,
            text=True,
        )
        if out.returncode != 0:
            raise RuntimeError("Azure CLI has no valid login for the Business Central tenant; run `az login`")
        self.s.headers["Authorization"] = f"Bearer {out.stdout.strip()}"

    def call(self, method, url, **kw):
        """Retries 429 for every method. 5xx and network errors are retried only for GET, PATCH and DELETE:
        a POST that timed out may have been committed, so it is not repeated; the next run finds the record
        by its key instead of creating a second one."""
        idempotent = method != "POST"
        renewed = False
        for attempt in range(1, 7):
            try:
                r = self.s.request(method, url, timeout=(10, 120), **kw)
            except requests.RequestException:
                if idempotent and attempt < 6:
                    self.sleep(retry_after_seconds(None, attempt))
                    continue
                raise
            if r.status_code == 401 and not renewed:
                self._token()
                renewed = True
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
            raise RuntimeError(f"DUPLICATE_IN_BC: {len(v)} {entity} with {field} = {value}")
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

    def ensure_exchange_rates(self, needs, errors=None):
        """needs = {(currency, date)}. API v2.0 cannot insert exchange rates (entity set currencyExchangeRates
        is read-only), so missing rates are loaded as a RapidStart configuration package (table 330 Currency
        Exchange Rate) through the automation API: create package, upload file, import, apply, delete.
        Rates are ECB reference rates (fx_source) with base LCY, so the published value goes in unchanged as
        Exchange Rate Amount (foreign) against Relational Exch. Rate Amount 1 (LCY): no division, no rounding.
        Both fields are Decimal in table 330.
        https://learn.microsoft.com/en-us/dynamics365/business-central/application/base-application/table/microsoft.finance.currency.currency-exchange-rate

        Each currency is loaded on its own. When errors is a dict, a currency that fails is recorded there
        (currency -> message) and the others still load; when errors is None, the first failure is raised.

        Returns (loaded, unavailable): the rates loaded as (currency, date, foreign per 1 LCY), and the
        (currency, date) pairs the rate source does not publish (ECB has no VND or FJD, for example)."""
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
                rows = []
                for day in sorted(days[cur] - have):
                    r = requests.get(f"{self.cfg['fx_source']}/{day}", params={"base": lcy, "symbols": cur}, timeout=30)
                    if r.status_code == 404 or (r.ok and cur not in r.json().get("rates", {})):
                        unavailable.add((cur, day))
                        continue
                    r.raise_for_status()
                    rows.append((cur, day, decimal_text(r.json()["rates"][cur])))  # 1 LCY = x foreign
                if rows:
                    recs = "".join(
                        f"<CurrencyExchangeRate><CurrencyCode>{c}</CurrencyCode><StartingDate>{d}</StartingDate>"
                        f"<ExchangeRateAmount>{x}</ExchangeRateAmount><AdjustmentExchRateAmount>{x}"
                        f"</AdjustmentExchRateAmount><RelationalExchRateAmount>1</RelationalExchRateAmount>"
                        f"<RelationalAdjmtExchRateAmt>1</RelationalAdjmtExchRateAmt></CurrencyExchangeRate>"
                        for c, d, x in rows
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
            self.call("DELETE", f"{auto}/configurationPackages({pid})", headers={"If-Match": "*"})


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
    (src/bc_tables.py builds both from service_id and type_code)."""
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
def doc_decision(row, hold_reason):
    """HELD, HELD_LOGGED, DONE, DEAD or SEND for one document row joined with its sync state (state_sql)."""
    h = row["payload_hash"]
    if hold_reason and row["doc_action"] == "UPSERT":
        logged = row.get("log_result") == "HELD" and row.get("log_hash") == h and row.get("log_error") == hold_reason
        return "HELD_LOGGED" if logged else "HELD"
    if row.get("attempt_result") in TERMINAL and row.get("attempt_hash") == h:
        return "DONE"
    if int(row.get("failures") or 0) >= MAX_FAILURES:
        return "DEAD"
    if row["doc_action"] == "CANCEL" and not truthy(row.get("ever_sent")):
        return "DONE"  # a cancelled document is only sent when something was sent for its key before
    return "SEND"


def plan(sales, purchases, limit):
    """Decide per document. Returns (todo, held, pending):
    todo    up to limit sales rows, oldest change first, of bookings with a sales or purchase document to send;
            each carries _send (bool) and _purchases (purchase rows to send)
    held    (doc_type, key, payload_hash, reason) to record as HELD
    pending number of bookings with something to send (more than len(todo) means a backlog)"""
    held, by_ref = [], {}
    for s in sales:
        s["_decision"] = doc_decision(s, s.get("hold_reason"))
        s["_send"] = s["_decision"] == "SEND"
        s["_purchases"] = []
        by_ref[s["external_document_number"]] = s
        if s["_decision"] == "HELD":
            held.append(("sales", s["external_document_number"], s["payload_hash"], s["hold_reason"]))
    for p in purchases:
        reason = p.get("hold_reason") or (
            f"SALES_HELD {p['sales_hold_reason']}" if p.get("sales_hold_reason") else None
        )
        d = doc_decision(p, reason)
        if d == "HELD":
            held.append(("purchase", p["purchase_key"], p["payload_hash"], reason))
        elif d == "SEND" and p["booking_ref"] in by_ref:
            by_ref[p["booking_ref"]]["_purchases"].append(p)
    pending = sorted((s for s in sales if s["_send"] or s["_purchases"]), key=lambda s: s.get("last_update_at") or "")
    return pending[:limit], held, len(pending)


def state_sql(c, company):
    """CTEs over ops.bc_sync. st: per document key, the hash and result of the latest attempt (ATTEMPT), the
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
        FROM {c}.ops.bc_sync WHERE bc_company = {company} AND doc_type IN ('sales', 'purchase')
        GROUP BY doc_type, doc_key),
      fl AS (
        SELECT f.doc_type, f.doc_key, f.payload_hash, count(*) AS failures
        FROM {c}.ops.bc_sync f JOIN st ON st.doc_type = f.doc_type AND st.doc_key = f.doc_key
        WHERE f.bc_company = {company} AND f.result IN {sql_list(TRANSIENT)}
          AND (st.terminal_at IS NULL OR f.synced_at > st.terminal_at)
        GROUP BY f.doc_type, f.doc_key, f.payload_hash)
    """


STATE_COLS = "st.attempt_hash, st.attempt_result, st.log_hash, st.log_result, st.log_error, st.ever_sent"


def doc_queries(c, company, g, scope):
    """SQL for the sales and purchase documents in scope with their sync state, and for the documents synced
    before that are no longer in the published tables. scope filters sales_invoices, alias i."""
    with_state = state_sql(c, company)
    sales = f"""{with_state}
        SELECT i.*, {STATE_COLS}, coalesce(fl.failures, 0) AS failures
        FROM {g["sales_invoices"]} i
        LEFT JOIN st ON st.doc_type = 'sales' AND st.doc_key = i.external_document_number
        LEFT JOIN fl ON fl.doc_type = 'sales' AND fl.doc_key = i.external_document_number
                    AND fl.payload_hash = i.payload_hash
        WHERE {scope}"""
    purchases = f"""{with_state}
        SELECT p.*, i.hold_reason AS sales_hold_reason, {STATE_COLS}, coalesce(fl.failures, 0) AS failures
        FROM {g["purchase_invoices"]} p
        JOIN {g["sales_invoices"]} i ON i.external_document_number = p.booking_ref
        LEFT JOIN st ON st.doc_type = 'purchase' AND st.doc_key = p.purchase_key
        LEFT JOIN fl ON fl.doc_type = 'purchase' AND fl.doc_key = p.purchase_key AND fl.payload_hash = p.payload_hash
        WHERE {scope}"""
    removed = f"""{with_state}
        SELECT st.doc_type, st.doc_key, st.attempt_result FROM st
        LEFT ANTI JOIN (SELECT 'sales' AS doc_type, external_document_number AS doc_key FROM {g["sales_invoices"]}
                        UNION ALL SELECT 'purchase', purchase_key FROM {g["purchase_invoices"]}) cur
          ON cur.doc_type = st.doc_type AND cur.doc_key = st.doc_key
        WHERE st.ever_sent AND st.attempt_result NOT IN {sql_list(CLOSED)}
        ORDER BY st.doc_type, st.doc_key"""
    return {"sales": sales, "purchases": purchases, "removed": removed}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int)
    ap.add_argument("--scope_sql", help="override the SQL filter on sales invoices, e.g. for a backfill push")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--on-new-run", action="store_true", help="exit unless the job finished a run since the last push")
    a = ap.parse_args()
    cfg = load_cfg()
    limit = a.limit or cfg.get("max_bookings_per_run", 100)
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    job_run = None
    if a.on_new_run:
        job_run = job_run_to_push(cfg, state)
        if job_run is None:
            print("no new job run and no backlog, nothing to do")
            return
    db = DBX(cfg)
    c = cfg["catalog"]
    pub = db.q(f"SELECT run_id, versions FROM {c}.ops.publish ORDER BY published_at DESC LIMIT 1")
    if not pub:
        print("no published business_central build yet")
        return
    ver = json.loads(pub[0]["versions"])
    if any(t.startswith("bc_") for t in ver):
        # rows written before the tables moved from gold.bc_* to business_central.*
        print("the last publish uses the old gold.bc_* layout; wait for the next job run")
        return
    g = {t: f"{c}.business_central.{t} VERSION AS OF {v}" for t, v in ver.items()}
    print(f"reading business_central published by run {pub[0]['run_id']}: {ver}")
    db.q(f"""CREATE TABLE IF NOT EXISTS {c}.ops.bc_sync (synced_at TIMESTAMP, doc_type STRING, doc_key STRING,
             payload_hash STRING, bc_company STRING, bc_id STRING, bc_number STRING, result STRING, error STRING)""")
    company = lit(cfg["bc_company"])
    scope = a.scope_sql or cfg.get("scope_sql", "TRUE")
    q = doc_queries(c, company, g, scope)
    todo, held, pending = plan(db.q(q["sales"]), db.q(q["purchases"]), limit)
    removed = db.q(q["removed"])
    if not todo and not held and not removed:
        print("nothing to push")
        if job_run is not None:
            STATE.write_text(json.dumps({"last_job_run": job_run, "backlog": False}))
        return
    p_heads = [p for h in todo for p in h["_purchases"]]

    def party_synced(kind):
        # latest synced hash per party, so a party that returns to an earlier state is upserted again
        return f"""(SELECT doc_key, max_by(payload_hash, synced_at) AS payload_hash FROM {c}.ops.bc_sync
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
    if a.dry_run:
        for h in todo:
            print(json.dumps({k: v for k, v in h.items() if k != "_purchases"}, ensure_ascii=False))
        for r in removed:
            print("no longer published:", json.dumps(r, ensure_ascii=False))
        return

    bc = BC(cfg)
    lcy = cfg["bc_local_currency"]
    results, failed_total = [], 0

    def record(doc_type, key, phash, rec, result, error=None):
        nonlocal failed_total
        failed_total += result == "FAILED"
        # microseconds keep rows of one key in order for max_by(..., synced_at)
        now = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")
        results.append(
            f"(TIMESTAMP'{now}', {lit(doc_type)}, {lit(key)}, {lit(phash)}, {company}, "
            f"{lit((rec or {}).get('id'))}, {lit((rec or {}).get('number'))}, {lit(result)}, {lit(error)})"
        )
        print(f"{doc_type:9} {key:28} {result:24} {(rec or {}).get('number', '')} {error or ''}")

    def flush():
        if results:
            db.q(f"INSERT INTO {c}.ops.bc_sync VALUES " + ",".join(results))
            results.clear()

    try:
        needs = {(h["currency_code"], h["invoice_date"]) for h in todo if h["_send"] and h["doc_action"] == "UPSERT"}
        needs |= {(p["currency_code"], p["invoice_date"]) for p in p_heads if p["doc_action"] == "UPSERT"}
        fx_errors = {}
        loaded, no_rate = bc.ensure_exchange_rates(needs, errors=fx_errors)
        for cur, day, x in loaded:
            record("fx_rate", f"{cur}/{day}", None, None, "LOADED", f"{x} {cur} = 1 {lcy}")
        for cur, day in sorted(no_rate):
            record("fx_rate", f"{cur}/{day}", None, None, "UNAVAILABLE", f"{cfg['fx_source']} has no rate")
        for cur, err in sorted(fx_errors.items()):
            record("fx_rate", cur, None, None, "FAILED", err)

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
        if not process_removals(bc, removed, cfg.get("max_removals_per_run", 50), record):
            failed_total += 1
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
        if job_run is not None:
            # more bookings waiting than this run took: the next timer tick continues without a new job run
            STATE.write_text(json.dumps({"last_job_run": job_run, "backlog": pending > len(todo)}))
    print(f"done: results written to {c}.ops.bc_sync, {failed_total} failed")
    sys.exit(1 if failed_total else 0)


if __name__ == "__main__":
    main()
