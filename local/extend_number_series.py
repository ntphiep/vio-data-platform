"""Add a line to a Business Central number series when the current line has run out of numbers.

The trial company My Company ships purchase invoice series P-INV+ ending at 109999; once those numbers are used,
every POST purchaseInvoices fails with "You cannot assign new numbers from the number series P-INV+". API v2.0
has no number series entity, so the line goes in as a RapidStart configuration package (table 309 No. Series
Line), the same way push_to_bc.py loads exchange rates. A new line with a later starting date takes over from
the exhausted one.

Usage (needs `az login` as the trial owner, same config.json as push_to_bc.py):
    uv run --no-project --with databricks-sdk --with requests extend_number_series.py \
        --series P-INV+ --line_no 20000 --starting_no 110000 --ending_no 199999
"""

import argparse
import datetime as dt
from xml.sax.saxutils import escape

from push_to_bc import BC, load_cfg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True)
    ap.add_argument("--line_no", type=int, required=True, help="a line number not used in the series yet")
    ap.add_argument("--starting_no", required=True)
    ap.add_argument("--ending_no", required=True)
    ap.add_argument("--starting_date", default=dt.date.today().isoformat())
    a = ap.parse_args()
    rec = (
        f"<NoSeriesLine><SeriesCode>{escape(a.series)}</SeriesCode><LineNo>{a.line_no}</LineNo>"
        f"<StartingDate>{a.starting_date}</StartingDate><StartingNo>{escape(a.starting_no)}</StartingNo>"
        f"<EndingNo>{escape(a.ending_no)}</EndingNo><IncrementbyNo>1</IncrementbyNo></NoSeriesLine>"
    )
    BC(load_cfg()).apply_package("VIONS", 309, "NoSeriesLineList", rec)
    print(f"number series {a.series}: line {a.line_no} {a.starting_no}..{a.ending_no} from {a.starting_date}")


if __name__ == "__main__":
    main()
