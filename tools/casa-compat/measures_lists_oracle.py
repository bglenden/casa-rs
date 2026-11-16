#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's `me.obslist`, `me.sourcelist` and `me.linelist`, for casa-rs's tests (#1053).

Run with CASA 6.7.6.14's Python:

    CASAPY -I tools/casa-compat/measures_lists_oracle.py \\
        --out crates/casars-python/python/tests/data/casa_measures_lists.json

The lists come from CASA's measures data (`geodetic/Observatories`,
`ephemerides/Sources` and `ephemerides/Lines`). casa-rs reads the same
tables from its own runtime, so the record also names the tables' row
counts, which the tests check first.
"""

from __future__ import annotations

import argparse
import json

import casatools


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    me = casatools.measures()
    tb = casatools.table()
    tables = {}
    for name in ("geodetic/Observatories", "ephemerides/Sources", "ephemerides/Lines"):
        tb.open(casatools.ctsys.resolve(name))
        tables[name] = [str(value) for value in tb.getcol("Name")]
        tb.close()
    record = {
        "casa_version": casatools.version_string(),
        "generator": "tools/casa-compat/measures_lists_oracle.py",
        "obslist": me.obslist(),
        "sourcelist": me.sourcelist(),
        "linelist": me.linelist(),
        # How CASA makes the lists from its tables, which casa-rs's data
        # may hold in another version.
        "rows": {name: len(names) for name, names in tables.items()},
        "obslist_is_the_table": me.obslist() == tables["geodetic/Observatories"],
        "sourcelist_is_the_table_joined": me.sourcelist() == " ".join(tables["ephemerides/Sources"]),
        "linelist_is_the_table_joined": me.linelist() == " ".join(tables["ephemerides/Lines"]),
    }
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=1)
        handle.write("\n")


if __name__ == "__main__":
    main()
