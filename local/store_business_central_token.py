"""Store the Business Central refresh token in a Databricks secret, for task push_to_business_central.

The tenant of the Business Central trial allows no app registration, so the task signs in with the delegated
login of the trial owner: the refresh token that `az login` left in the Azure CLI token cache. Run this script once
after `az login --tenant <bc_tenant>`, and again whenever the task reports that the refresh token is no longer
valid (90 days without use, password change, revoked sessions). The task rotates the secret on every run.

The script never prints a token. It checks the token against Business Central (GET companies) before storing it,
creates the secret scope when missing, and gives the job's service principal WRITE on it, so the task can read the
token and store the rotated one.

Usage: uv run --no-project --with msal --with requests --with databricks-sdk store_business_central_token.py
"""

import argparse
import json
from pathlib import Path

import msal
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import AclPermission

AZURE_CLI_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"  # public client id of the Azure CLI
SCOPE, KEY = "vio-business-central", "refresh_token"
BC_SCOPE = ["https://api.businesscentral.dynamics.com/.default"]
CACHE = Path.home() / ".azure" / "msal_token_cache.json"
ROOT = Path(__file__).resolve().parent.parent


def main():
    ap = argparse.ArgumentParser()
    # same as variable pipeline_sp in databricks.yml: the service principal that runs the job
    ap.add_argument("--service_principal", default="82205f93-7db0-4ca5-9d78-63bd916ac9db")
    pipeline_sp = ap.parse_args().service_principal
    cfg = json.loads((ROOT / "src" / "push_to_business_central.json").read_text())
    app = msal.PublicClientApplication(
        AZURE_CLI_CLIENT_ID, authority=f"https://login.microsoftonline.com/{cfg['bc_tenant']}"
    )
    cache = json.loads(CACHE.read_text())
    candidates = [v["secret"] for v in cache.get("RefreshToken", {}).values() if v["client_id"] == AZURE_CLI_CLIENT_ID]
    for rt in candidates:
        res = app.acquire_token_by_refresh_token(rt, scopes=BC_SCOPE)
        if "access_token" not in res:
            continue
        r = requests.get(
            f"https://api.businesscentral.dynamics.com/v2.0/{cfg['bc_environment']}/api/v2.0/companies",
            headers={"Authorization": f"Bearer {res['access_token']}"},
            timeout=60,
        )
        if r.status_code == 200 and any(c["name"] == cfg["bc_company"] for c in r.json()["value"]):
            break
    else:
        raise SystemExit(f"no refresh token in {CACHE} reaches company {cfg['bc_company']}; run az login first")
    w = WorkspaceClient()
    if SCOPE not in {s.name for s in w.secrets.list_scopes()}:
        w.secrets.create_scope(SCOPE)
    w.secrets.put_secret(SCOPE, KEY, string_value=res["refresh_token"])
    w.secrets.put_acl(SCOPE, pipeline_sp, AclPermission.WRITE)
    print(f"stored secret {SCOPE}/{KEY}; service principal {pipeline_sp} has WRITE on scope {SCOPE}")


if __name__ == "__main__":
    main()
