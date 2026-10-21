#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Dump the installed CASA `simulator` tool's method signatures to JSON (#841).

Run with CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3 -I`).
For the constructor and every public method of `casatools.simulator`, the script
records the `inspect.signature` parameters (name, kind and the `repr` of the
default) and the CASA version. The installed binding's defaults are the
contract `casars.tools.simulator` reproduces (ADR-0019); the CASA XML text is
not. `crates/casars-python/python/tests/test_tools_simulator.py` compares
the casa-rs class with the checked-in output,
`crates/casars-python/python/tests/data/casa_simulator_signatures.json`.
"""

from __future__ import annotations

import argparse
import inspect
import json

import casatools


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="JSON file to write")
    return parser.parse_args()


def describe(function: object) -> list[dict[str, str | None]]:
    """The parameters of `function` without `self`."""
    parameters = []
    for parameter in inspect.signature(function).parameters.values():
        if parameter.name == "self":
            continue
        default = None if parameter.default is inspect.Parameter.empty else repr(parameter.default)
        parameters.append({"name": parameter.name, "kind": parameter.kind.name, "default": default})
    return parameters


def main() -> None:
    args = parse_args()
    tool = casatools.simulator
    methods = {
        name: describe(member)
        for name, member in inspect.getmembers(tool, inspect.isfunction)
        if not name.startswith("_")
    }
    record = {
        "casa_version": casatools.version_string(),
        "tool": "simulator",
        "constructor": describe(tool.__init__),
        "methods": methods,
    }
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=2, sort_keys=True)
        handle.write("\n")


if __name__ == "__main__":
    main()
