"""Add a line to a Business Central number series when the current line has run out of numbers.

The trial company My Company ships purchase invoice series P-INV+ ending at 109999; once those numbers are used,
every POST purchaseInvoices fails with "You cannot assign new numbers from the number series P-INV+". API v2.0
has no number series entity, so the line goes in as a RapidStart configuration package (table 309 No. Series
Line), the same way src/push_to_business_central.py loads exchange rates. A new line with a later starting date takes over from
the exhausted one.

Applying a configuration package modifies a row that already exists, so the script first reads the lines of the
series and refuses when --line_no is taken. The read goes through page 457 "No. Series Lines" published as an
ODataV4 web service (Business Central page Web Services, object type Page, object ID 457, service name as in
src/push_to_business_central.json key no_series_lines_service). When the service is missing or the lines cannot be read, the script
refuses too: it never writes a line it could not check.

A one-off admin tool, run on a workstation with the Azure CLI signed in as the trial owner (`az login`); it reads
the config of task push_to_business_central and does not touch the refresh token in secret scope vio-business-central.

Usage:
    uv run --no-project --with requests extend_number_series.py \
        --series P-INV+ --line_no 20000 --starting_no 110000 --ending_no 199999
"""

import argparse
import datetime as dt
import subprocess
import sys
from pathlib import Path
from xml.sax.saxutils import escape

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from push_to_business_central import BC, load_cfg, odata_str  # noqa: E402


def az_token(tenant):
    """Token function for BC: an access token of the Azure CLI login, asked again on every call (the Azure CLI
    caches and renews it)."""

    def token(force=False):
        out = subprocess.run(
            [
                "az",
                "account",
                "get-access-token",
                "--resource",
                "https://api.businesscentral.dynamics.com",
                "--tenant",
                tenant,
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
        return out.stdout.strip()

    return token


def existing_line_numbers(bc, service, series):
    """Line numbers of one series, read through the ODataV4 web service of page 457. Raises when they cannot be
    read, so the caller never writes blind."""
    try:
        rows = bc.get_all(
            f"{bc.odata}/{service}",
            params={"$filter": f"Series_Code eq '{odata_str(series)}'"},
            what=f"GET {service}",
        )
    except RuntimeError as e:
        raise RuntimeError(
            f"cannot read the lines of number series {series} through ODataV4 service {service!r}; publish page 457 "
            f"No. Series Lines as web service {service!r} in Business Central first ({e})"
        ) from e
    if any("Line_No" not in r for r in rows):
        raise RuntimeError(f"ODataV4 service {service!r} returns no Line_No field; cannot check the line number")
    return {int(r["Line_No"]) for r in rows}


def extend(bc, service, series, line_no, starting_no, ending_no, starting_date):
    """Add the line, or return False without writing when the series already has line line_no."""
    if line_no in existing_line_numbers(bc, service, series):
        print(f"number series {series} already has line {line_no}; nothing written, choose another --line_no")
        return False
    rec = (
        f"<NoSeriesLine><SeriesCode>{escape(series)}</SeriesCode><LineNo>{line_no}</LineNo>"
        f"<StartingDate>{starting_date}</StartingDate><StartingNo>{escape(starting_no)}</StartingNo>"
        f"<EndingNo>{escape(ending_no)}</EndingNo><IncrementbyNo>1</IncrementbyNo></NoSeriesLine>"
    )
    bc.apply_package("VIONS", 309, "NoSeriesLineList", rec)
    print(f"number series {series}: line {line_no} {starting_no}..{ending_no} from {starting_date}")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True)
    ap.add_argument("--line_no", type=int, required=True, help="a line number not used in the series yet")
    ap.add_argument("--starting_no", required=True)
    ap.add_argument("--ending_no", required=True)
    ap.add_argument("--starting_date", default=dt.date.today().isoformat())
    a = ap.parse_args()
    cfg = load_cfg()
    service = cfg.get("no_series_lines_service", "NoSeriesLines")
    if not extend(
        BC(cfg, token=az_token(cfg["bc_tenant"])),
        service,
        a.series,
        a.line_no,
        a.starting_no,
        a.ending_no,
        a.starting_date,
    ):
        sys.exit(1)


if __name__ == "__main__":
    main()
