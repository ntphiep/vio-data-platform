"""Push the published business_central tables from Databricks into Business Central (runs on this machine).

Why local: the Business Central token is a delegated Azure CLI login (`az login` as the trial owner).
A Databricks job cannot do an interactive login, and an app registration needs a tenant admin.

Flow per run
1. Read the last row of <catalog>.ops.publish and read every business_central table AS OF those versions, so
   the run sees one consistent build that passed the blocking data-quality checks.
2. Pick up to --limit bookings, oldest change first, whose sales document or sendable purchase documents have a
   payload_hash not yet synced (ops.bc_sync). Documents that failed 3 times with the same hash are skipped as
   DEAD_LETTER until the data changes. Held documents are recorded as HELD and never sent.
3. Upsert customers and vendors whose hash changed, and complete posting groups through the ODataV4 pages
   workflowCustomers / workflowVendors (API v2.0 has no posting group fields).
4. Load missing exchange rates (configuration package). Documents whose rate could not be loaded are skipped.
5. Create or update draft invoices. Matching key: externalDocumentNumber (sales), vendorInvoiceNumber
   (purchase). A draft is never deleted (deleting a numbered draft creates an empty posted invoice). A posted
   document is never touched and is reported as NEEDS_CREDIT_MEMO. A cancelled booking or service empties the
   lines of its draft so it cannot be posted (CANCELLED_DRAFT_EMPTIED).
6. Write the result rows into ops.bc_sync after every booking, so a crash loses at most one booking's rows; a
   document created but not recorded is found again by its key on the next run and updated, not duplicated.

Usage: uv run --no-project --with databricks-sdk --with requests push_to_bc.py [--limit 5] [--dry-run]
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
from pathlib import Path

import requests

SYNCED = ("CREATED", "UPDATED", "RECREATED", "CANCELLED_DRAFT_EMPTIED")  # RECREATED: first lab version
MAX_FAILURES = 3


def load_cfg(path=None):
    return json.loads(Path(path or Path(__file__).parent / "config.json").read_text())


def lit(v):
    """SQL string literal for Databricks SQL (backslash is an escape character there)."""
    if v is None:
        return "NULL"
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def odata_str(v):
    return str(v).replace("'", "''")


def retry_after_seconds(value, attempt, cap=120):
    if value:
        try:
            return min(cap, max(1.0, float(value)))
        except ValueError:
            try:
                when = email.utils.parsedate_to_datetime(value)
                return min(cap, max(1.0, (when - dt.datetime.now(dt.timezone.utc)).total_seconds()))
            except (TypeError, ValueError):
                pass
    return min(cap, 2 ** attempt * 2)


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
        return [dict(zip(cols, row)) for row in rows]


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
        self.api = f"{self.root}/api/v2.0/companies({cid})"
        self.odata = f"{self.root}/ODataV4/Company('{cfg['bc_company']}')"
        self.currencies = None

    def _token(self):
        out = subprocess.run(["az", "account", "get-access-token", "--resource",
                              "https://api.businesscentral.dynamics.com", "--tenant", self.cfg["bc_tenant"],
                              "--query", "accessToken", "-o", "tsv"], capture_output=True, text=True)
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

    def find(self, entity, field, value):
        v = self.ok(self.call("GET", f"{self.api}/{entity}",
                              params={"$filter": f"{field} eq '{odata_str(value)}'"}), f"GET {entity}")["value"]
        if len(v) > 1:
            raise RuntimeError(f"DUPLICATE_IN_BC: {len(v)} {entity} with {field} = {value}")
        return v[0] if v else None

    def upsert_party(self, entity, page, body, setup):
        found = self.find(entity, "number", body["number"])
        if not found:
            rec, result = self.ok(self.call("POST", f"{self.api}/{entity}", data=json.dumps(body)),
                                  f"POST {entity}"), "CREATED"
        else:
            diff = {k: v for k, v in body.items() if k != "number" and found.get(k) != v}
            rec, result = found, "UNCHANGED"
            if diff:
                rec = self.ok(self.call("PATCH", f"{self.api}/{entity}({found['id']})", data=json.dumps(diff),
                                        headers={"If-Match": "*"}), f"PATCH {entity}")
                result = "UPDATED"
        cur = self.ok(self.call("GET", f"{self.odata}/{page}({rec['id']})"), f"GET {page}")
        patch = {k: v for k, v in setup.items() if cur.get(k) != v}
        if patch:
            self.ok(self.call("PATCH", f"{self.odata}/{page}({rec['id']})", data=json.dumps(patch),
                              headers={"If-Match": "*"}), f"PATCH {page}")
        return rec, result

    def has_currency(self, code):
        if self.currencies is None:
            self.currencies = {x["code"] for x in self.ok(self.call("GET", f"{self.api}/currencies"),
                                                          "GET currencies")["value"]}
        return code == self.cfg["bc_local_currency"] or code in self.currencies

    def ensure_exchange_rates(self, needs):
        """needs = {(currency, date)}. API v2.0 cannot insert exchange rates (entity set currencyExchangeRates
        is read-only), so missing rates are loaded as a RapidStart configuration package (table 330 Currency
        Exchange Rate) through the automation API: create package, upload file, import, apply, delete.
        Rates are ECB reference rates (fx_source), expressed as LCY per 1 unit of the foreign currency."""
        lcy = self.cfg["bc_local_currency"]
        wanted = {(cur, day) for cur, day in needs if cur and cur != lcy}
        have = set()
        for cur in {x for x, _ in wanted}:
            got = self.ok(self.call("GET", f"{self.api}/currencyExchangeRates",
                                    params={"$filter": f"currencyCode eq '{odata_str(cur)}'"}), "GET rates")
            have |= {(r["currencyCode"], r["startingDate"]) for r in got.get("value", [])}
        missing = sorted(wanted - have)
        if not missing:
            return []
        rows = []
        for cur, day in missing:
            r = requests.get(f"{self.cfg['fx_source']}/{day}", params={"base": lcy, "symbols": cur}, timeout=30)
            r.raise_for_status()
            per_lcy = r.json()["rates"][cur]            # 1 LCY = per_lcy foreign
            rows.append((cur, day, round(1 / per_lcy, 6)))  # 1 foreign = x LCY
        code = "VIOFX" + dt.datetime.now(dt.timezone.utc).strftime("%m%d%H%M%S")
        recs = "".join(
            f"<CurrencyExchangeRate><CurrencyCode>{c}</CurrencyCode><StartingDate>{d}</StartingDate>"
            f"<ExchangeRateAmount>1</ExchangeRateAmount><AdjustmentExchRateAmount>1</AdjustmentExchRateAmount>"
            f"<RelationalExchRateAmount>{x}</RelationalExchRateAmount>"
            f"<RelationalAdjmtExchRateAmt>{x}</RelationalAdjmtExchRateAmt></CurrencyExchangeRate>" for c, d, x in rows)
        xml = (f'<?xml version="1.0" encoding="UTF-16" standalone="yes"?><DataList Code="{code}" '
               f'PackageName="Vio FX {code}" LanguageID="1033" ProcessingOrder="0" ExcludeConfigTables="1">'
               f'<CurrencyExchangeRateList><TableID>330</TableID><PackageCode>{code}</PackageCode>{recs}'
               f'</CurrencyExchangeRateList></DataList>')
        auto = f"{self.root}/api/microsoft/automation/v2.0/companies({self.cfg['bc_company_id']})"
        pkg = self.ok(self.call("POST", f"{auto}/configurationPackages",
                                data=json.dumps({"code": code, "packageName": f"Vio FX {code}"})), "create package")
        pid = pkg["id"]
        try:
            self.ok(self.call("PATCH", f"{auto}/configurationPackages({pid})/file('{code}')/content",
                              data=gzip.compress(xml.encode("utf-16")),
                              headers={"If-Match": "*", "Content-Type": "application/octet-stream"}), "upload package")
            for action, field in (("import", "importStatus"), ("apply", "applyStatus")):
                self.ok(self.call("POST", f"{auto}/configurationPackages({pid})/Microsoft.NAV.{action}"), action)
                st = {}
                for _ in range(60):
                    st = self.ok(self.call("GET", f"{auto}/configurationPackages({pid})"), "GET package")
                    if st.get(field) in ("Completed", "Error"):
                        break
                    self.sleep(3)
                if st.get(field) != "Completed" or st.get("numberOfErrors"):
                    raise RuntimeError(f"exchange rate package {code} {action}: {st.get(field)}, "
                                       f"errors {st.get('numberOfErrors')}")
        finally:
            self.call("DELETE", f"{auto}/configurationPackages({pid})", headers={"If-Match": "*"})
        return rows


def push_document(bc, kind, key_field, key, body, lines, cancel=False):
    """Create a draft document, update the existing draft in place, or empty it when cancelled.

    A draft is never deleted: when the invoice number series is also the posted invoice number series
    (true in My Company), deleting a numbered draft makes Business Central create an empty posted
    invoice to fill the gap (BCApps, ERMPurchaseInvoice.Codeunit.al, scenario 226743)."""
    entity = "salesInvoices" if kind == "sales" else "purchaseInvoices"
    lines_key = "salesInvoiceLines" if kind == "sales" else "purchaseInvoiceLines"
    existing = bc.find(entity, key_field, key)
    if existing and existing["status"] != "Draft":
        return "NEEDS_CREDIT_MEMO", existing
    if cancel:
        if not existing:
            return "NOTHING_TO_CANCEL", None
        doc = f"{bc.api}/{entity}({existing['id']})"
        for line in bc.ok(bc.call("GET", f"{doc}/{lines_key}"), "GET lines")["value"]:
            bc.ok(bc.call("DELETE", f"{doc}/{lines_key}({line['id']})", headers={"If-Match": "*"}), "DELETE line")
        return "CANCELLED_DRAFT_EMPTIED", existing
    if not existing:
        return "CREATED", bc.ok(bc.call("POST", f"{bc.api}/{entity}", data=json.dumps({**body, lines_key: lines})),
                                f"POST {entity}")
    doc = f"{bc.api}/{entity}({existing['id']})"
    for line in bc.ok(bc.call("GET", f"{doc}/{lines_key}"), "GET lines")["value"]:
        bc.ok(bc.call("DELETE", f"{doc}/{lines_key}({line['id']})", headers={"If-Match": "*"}), "DELETE line")
    bc.ok(bc.call("PATCH", doc, data=json.dumps(body), headers={"If-Match": "*"}), f"PATCH {entity}")
    for line in lines:
        bc.ok(bc.call("POST", f"{doc}/{lines_key}", data=json.dumps(line)), "POST line")
    return "UPDATED", bc.ok(bc.call("GET", doc), f"GET {entity}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int)
    ap.add_argument("--scope_sql", help="override the SQL filter on sales invoices, e.g. for a backfill push")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    cfg = load_cfg()
    limit = a.limit or cfg.get("max_bookings_per_run", 5)
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
    synced = f"""(SELECT DISTINCT doc_type, doc_key, payload_hash FROM {c}.ops.bc_sync
                  WHERE result IN {SYNCED} AND bc_company = {company})"""
    dead = f"""(SELECT doc_type, doc_key, payload_hash FROM {c}.ops.bc_sync
                WHERE result = 'FAILED' AND bc_company = {company}
                GROUP BY ALL HAVING count(*) >= {MAX_FAILURES})"""
    ever_sent = f"(SELECT DISTINCT doc_type, doc_key FROM {c}.ops.bc_sync WHERE result IN {SYNCED} AND bc_company = {company})"
    # A cancelled document is only sent when something was sent for that key before.
    pending_purch = f"""(SELECT p.* FROM {g['purchase_invoices']} p
        LEFT JOIN {synced} s ON s.doc_type = 'purchase' AND s.doc_key = p.purchase_key AND s.payload_hash = p.payload_hash
        LEFT JOIN {dead} d ON d.doc_type = 'purchase' AND d.doc_key = p.purchase_key AND d.payload_hash = p.payload_hash
        LEFT JOIN {ever_sent} e ON e.doc_type = 'purchase' AND e.doc_key = p.purchase_key
        WHERE s.doc_key IS NULL AND d.doc_key IS NULL AND p.hold_reason IS NULL
          AND (p.doc_action = 'UPSERT' OR e.doc_key IS NOT NULL))"""
    heads = db.q(f"""SELECT i.*, (s.doc_key IS NOT NULL OR d.doc_key IS NOT NULL
                                  OR (i.doc_action = 'CANCEL' AND e.doc_key IS NULL)) AS sales_done
        FROM {g['sales_invoices']} i
        LEFT JOIN {synced} s ON s.doc_type = 'sales' AND s.doc_key = i.external_document_number
                            AND s.payload_hash = i.payload_hash
        LEFT JOIN {dead} d ON d.doc_type = 'sales' AND d.doc_key = i.external_document_number
                          AND d.payload_hash = i.payload_hash
        LEFT JOIN {ever_sent} e ON e.doc_type = 'sales' AND e.doc_key = i.external_document_number
        WHERE i.hold_reason IS NULL AND {scope}
          AND (NOT (s.doc_key IS NOT NULL OR d.doc_key IS NOT NULL OR (i.doc_action = 'CANCEL' AND e.doc_key IS NULL))
               OR EXISTS (SELECT 1 FROM {pending_purch} p WHERE p.booking_ref = i.external_document_number))
        ORDER BY i.last_update_at ASC LIMIT {limit}""")
    if not heads:
        print("nothing to push")
        return
    refs = ",".join(lit(h["external_document_number"]) for h in heads)
    s_lines = db.q(f"SELECT * FROM {g['sales_lines']} WHERE booking_ref IN ({refs}) ORDER BY booking_ref, line_no")
    p_heads = db.q(f"SELECT * FROM {pending_purch} WHERE booking_ref IN ({refs})")
    held = db.q(f"""SELECT purchase_key, payload_hash, hold_reason FROM {g['purchase_invoices']}
                    WHERE booking_ref IN ({refs}) AND hold_reason IS NOT NULL AND doc_action = 'UPSERT'""")
    p_lines = db.q(f"""SELECT * FROM {g['purchase_lines']} WHERE booking_ref IN ({refs})
                       ORDER BY purchase_key, line_no""")
    def party_synced(kind):
        return f"""(SELECT DISTINCT doc_key, payload_hash FROM {c}.ops.bc_sync WHERE doc_type = '{kind}'
                    AND result IN ('CREATED', 'UPDATED', 'UNCHANGED', 'UPSERTED') AND bc_company = {company})"""
    cust_nos = ",".join(lit(h["customer_number"]) for h in heads if h["doc_action"] == "UPSERT") or "''"
    vend_nos = ",".join(lit(p["vendor_number"]) for p in p_heads if p["doc_action"] == "UPSERT") or "''"
    custs = db.q(f"""SELECT x.* FROM {g['customers']} x
        LEFT ANTI JOIN {party_synced('customer')} s ON s.doc_key = x.bc_number AND s.payload_hash = x.payload_hash
        WHERE x.bc_number IN ({cust_nos})""")
    vends = db.q(f"""SELECT x.* FROM {g['vendors']} x
        LEFT ANTI JOIN {party_synced('vendor')} s ON s.doc_key = x.bc_number AND s.payload_hash = x.payload_hash
        WHERE x.bc_number IN ({vend_nos})""")
    print(f"to push: {len(heads)} bookings, {len(p_heads)} purchase docs ({len(held)} held), "
          f"{len(custs)} customers and {len(vends)} vendors to upsert")
    if a.dry_run:
        for h in heads:
            print(json.dumps(h, ensure_ascii=False))
        return

    bc = BC(cfg)
    lcy = cfg["bc_local_currency"]
    results, failed_total = [], 0

    def record(doc_type, key, phash, rec, result, error=None):
        nonlocal failed_total
        failed_total += result == "FAILED"
        now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        results.append(f"(TIMESTAMP'{now}', {lit(doc_type)}, {lit(key)}, {lit(phash)}, {company}, "
                       f"{lit((rec or {}).get('id'))}, {lit((rec or {}).get('number'))}, {lit(result)}, {lit(error)})")
        print(f"{doc_type:9} {key:28} {result:24} {(rec or {}).get('number', '')} {error or ''}")

    def flush():
        if results:
            db.q(f"INSERT INTO {c}.ops.bc_sync VALUES " + ",".join(results))
            results.clear()

    try:
        bad_fx = set()
        needs = {(h["currency_code"], h["invoice_date"]) for h in heads if h["doc_action"] == "UPSERT"} | \
                {(p["currency_code"], p["invoice_date"]) for p in p_heads if p["doc_action"] == "UPSERT"}
        try:
            for cur, day, x in bc.ensure_exchange_rates(needs):
                record("fx_rate", f"{cur}/{day}", None, None, "LOADED", f"1 {cur} = {x} {lcy}")
        except Exception as e:  # noqa: BLE001
            bad_fx = {n for n in needs if n[0] != lcy}
            record("fx_rate", "batch", None, None, "FAILED", str(e)[:500])

        failed_parties = set()
        for kind, rows in (("customer", custs), ("vendor", vends)):
            for x in rows:
                body = {"number": x["bc_number"], "displayName": x["display_name"], "email": x["email"] or ""}
                if kind == "customer":
                    body.update({"type": "Company", "city": x["city"] or "", "addressLine1": x["address_line1"] or ""})
                try:
                    rec, result = bc.upsert_party(f"{kind}s", f"workflow{kind.title()}s", body,
                                                  cfg[f"{kind}_setup"])
                    record(kind, x["bc_number"], x["payload_hash"], rec, result)
                except Exception as e:  # noqa: BLE001
                    failed_parties.add(x["bc_number"])
                    record(kind, x["bc_number"], x["payload_hash"], None, "FAILED", str(e)[:500])
        for p in held:
            record("purchase", p["purchase_key"], p["payload_hash"], None, "HELD", p["hold_reason"])
        flush()

        def send(doc_type, key, phash, party, cur, day, fn):
            if party in failed_parties:
                return record(doc_type, key, phash, None, "SKIPPED", f"{party} upsert failed")
            if (cur, day) in bad_fx:
                return record(doc_type, key, phash, None, "SKIPPED", f"no exchange rate {cur} {day}")
            try:
                if cur and not bc.has_currency(cur):
                    raise RuntimeError(f"currency {cur} missing in Business Central")
                result, rec = fn()
                record(doc_type, key, phash, rec, result)
            except Exception as e:  # noqa: BLE001
                record(doc_type, key, phash, None, "FAILED", str(e)[:500])

        for h in heads:
            ref = h["external_document_number"]
            cancel = h["doc_action"] == "CANCEL"
            if h["sales_done"] != "true":
                lines = [{"lineType": "Account", "lineObjectNumber": cfg["sales_gl_account"],
                          "description": ln["description"], "quantity": 1, "unitPrice": float(ln["amount"])}
                         for ln in s_lines if ln["booking_ref"] == ref]
                body = {"externalDocumentNumber": ref, "customerNumber": h["customer_number"],
                        "invoiceDate": h["invoice_date"], "postingDate": h["invoice_date"],
                        "currencyCode": "" if h["currency_code"] == lcy else h["currency_code"]}
                send("sales", ref, h["payload_hash"], h["customer_number"], None if cancel else h["currency_code"],
                     h["invoice_date"], lambda: push_document(bc, "sales", "externalDocumentNumber", ref, body, lines,
                                                              cancel))
            for p in [p for p in p_heads if p["booking_ref"] == ref]:
                pc = p["doc_action"] == "CANCEL"
                lines = [{"lineType": "Account", "lineObjectNumber": cfg["purchase_gl_account"],
                          "description": ln["description"], "quantity": 1, "unitCost": float(ln["amount"])}
                         for ln in p_lines if ln["purchase_key"] == p["purchase_key"]]
                body = {"vendorInvoiceNumber": p["vendor_invoice_number"], "vendorNumber": p["vendor_number"],
                        "invoiceDate": p["invoice_date"], "postingDate": p["invoice_date"],
                        "currencyCode": "" if p["currency_code"] == lcy else p["currency_code"]}
                send("purchase", p["purchase_key"], p["payload_hash"], p["vendor_number"],
                     None if pc else p["currency_code"], p["invoice_date"],
                     lambda p=p, body=body, lines=lines, pc=pc: push_document(
                         bc, "purchase", "vendorInvoiceNumber", p["vendor_invoice_number"], body, lines, pc))
            flush()
    finally:
        flush()
    print(f"done: results written to {c}.ops.bc_sync, {failed_total} failed")
    sys.exit(1 if failed_total else 0)


if __name__ == "__main__":
    main()
