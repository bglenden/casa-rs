#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Which entry CASA's `me` tool finds for a name it is given, for casa-rs (#1110).

Run with CASA 6.7.6.14's Python:

    CASAPY -I tools/casa-compat/measures_names_oracle.py \\
        --out crates/casars-python/python/tests/data/casa_measures_names.json

casacore finds an observatory, source or spectral line with
`MUString::minimaxNC`. An exact match, ignoring case, comes first. Failing
that, the name may match the one entry that agrees with it over the shorter
of their two lengths. For each name below, the record holds what
`me.observatory`, `me.source` or `me.spectralline` returns, or `{}` when it
finds nothing:
- every prefix of every observatory and spectral line name, upper and lower
  case;
- names longer than an entry, and with spaces around them;
- a few sources, whose table differs between data versions.
"""

from __future__ import annotations

import argparse
import json

import casatools

SOURCES = ["0002-478", "0002-47", "0002", "0002-478X", "3C273", "3c27", "3C", " 0002-478", "0002-478 ", "\t0002-478", "BOGUS"]
EXTRA = ["ALMAX", "VLA2", "VLA-X", " ALMA", "ALMA ", "", " ", "BOGUS", "HI ", " HI", "HIX"]


def prefixes(names: list[str]) -> list[str]:
    found = []
    for name in names:
        for end in range(1, len(name) + 1):
            for case in (name[:end], name[:end].lower()):
                if case not in found:
                    found.append(case)
    return found


def plain(value):
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    if hasattr(value, "tolist"):
        return value.tolist()
    return value


def lookups(method, names: list[str]) -> dict:
    found = {}
    for name in names:
        try:
            found[name] = plain(method(name))
        except Exception as error:  # noqa: BLE001 - recording CASA's refusal
            found[name] = {"error": str(error)}
    return found


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    me, tb = casatools.measures(), casatools.table()
    tb.open(casatools.ctsys.resolve("ephemerides/Lines"))
    lines = [str(name) for name in tb.getcol("Name")]
    tb.close()
    observatories = [str(name) for name in me.obslist()]
    record = {
        "casa_version": casatools.version_string(),
        "generator": "tools/casa-compat/measures_names_oracle.py",
        "observatory": lookups(me.observatory, prefixes(observatories) + EXTRA),
        "spectralline": lookups(me.spectralline, prefixes(lines) + EXTRA),
        "source": lookups(me.source, SOURCES),
    }
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=1)
        handle.write("\n")


if __name__ == "__main__":
    main()
