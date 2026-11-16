#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's `me.observatory` for every observatory, for casa-rs's tests (#1106).

Run with CASA 6.7.6.14's Python:

    CASAPY -I tools/casa-compat/observatories_oracle.py \\
        --out crates/casars-python/python/tests/data/casa_observatories.json

For each name `me.obslist` gives, the record holds `me.observatory(name)`
as CASA returns it (its tabulated frame) and the table row it came from, so
a test can see which columns casacore reads.
"""

from __future__ import annotations

import argparse
import json

import casatools


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    me, tb = casatools.measures(), casatools.table()
    tb.open(casatools.ctsys.resolve("geodetic/Observatories"))
    columns = {name: tb.getcol(name) for name in ("Name", "Type", "Long", "Lat", "Height", "X", "Y", "Z")}
    tb.close()
    rows = {str(name): index for index, name in enumerate(columns["Name"])}
    observatories = {}
    for name in me.obslist():
        position = me.observatory(name)
        row = rows[name]
        observatories[name] = {
            "position": {key: position[key] for key in ("m0", "m1", "m2", "refer")},
            "row": {key: (str(values[row]) if key in ("Name", "Type") else float(values[row]))
                    for key, values in columns.items()},
        }
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump({"casa_version": casatools.version_string(),
                   "generator": "tools/casa-compat/observatories_oracle.py",
                   "observatories": observatories}, handle, indent=1)
        handle.write("\n")


if __name__ == "__main__":
    main()
