#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Independent CASA scalar arithmetic used by simulation row predicates (#1053)."""

import argparse
import json
import math
import pathlib


EXPRESSIONS = [
    "7/2", "-7/2", "7//2", "-7//2", "7%-3", "-7%3", "-7%-3",
    "-7.0%3.0", "7.0%-3.0", "2**63", "1/0", "0/0",
    "(0.0/0.0)<1", "(0.0/0.0)<=1", "(0.0/0.0)>1", "(0.0/0.0)>=1",
    "-0.0<0.0", "-0.0>=0.0", "-0.0==0.0",
    "TRUE+TRUE", "TRUE+1", "TRUE-TRUE", "TRUE*TRUE", "TRUE/TRUE",
    "TRUE%TRUE", "TRUE**TRUE",
]


def encode(value):
    if isinstance(value, float) and not math.isfinite(value):
        return {"nonfinite": "nan" if math.isnan(value) else "+inf" if value > 0 else "-inf"}
    return value


def main():
    import casatasks
    from casatools import table

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ms", type=pathlib.Path, required=True)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    args = parser.parse_args()
    assert casatasks.version_string() == "6.7.6.14"
    tb = table()
    tb.open(str(args.ms))
    cases = []
    try:
        for expression in EXPRESSIONS:
            try:
                value = tb.calc(expression)
                cases.append({"expression": expression, "type": str(value.dtype),
                              "values": [encode(item) for item in value.tolist()]})
            except Exception as error:
                cases.append({"expression": expression, "error_type": type(error).__name__})
    finally:
        tb.close()
    args.out.write_text(json.dumps({"casa_version": casatasks.version_string(), "cases": cases},
                                   indent=1, allow_nan=False) + "\n")
    print("Recorded", len(cases), "independent scalar arithmetic outcomes", flush=True)


if __name__ == "__main__":
    main()
