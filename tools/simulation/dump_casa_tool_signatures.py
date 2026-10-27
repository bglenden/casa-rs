#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Dump the installed CASA tools' method signatures to JSON (#841).

Run with CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3 -I`).
For the constructor and every public method of a `casatools` class (or of
`casatasks.private.simutil.simutil`, which is a Python helper class rather than
a tool), the script records the `inspect.signature` parameters (name, kind and
the `repr` of the default) and the CASA version. The installed binding's
defaults are the contract the `casars` classes reproduce (ADR-0019); the CASA
XML text is not. The Python tests under `crates/casars-python/python/tests/`
compare each casa-rs class with the checked-in output in
`crates/casars-python/python/tests/data/casa_<tool>_signatures.json`.

For `simutil` only the methods casa-rs declares are recorded (`--methods`):
the rest of the helper belongs to simulation Phase 5.

`--task simobserve` records a task's call signature instead
(`casa_simobserve_task_signature.json`), which casa-rs's catalog surface
reproduces (Phase 5c).

    python3 -I dump_casa_tool_signatures.py --tool measures --out casa_measures_signatures.json
    python3 -I dump_casa_tool_signatures.py --task simobserve --out casa_simobserve_task_signature.json
"""

from __future__ import annotations

import argparse
import inspect
import json

import casatools

TOOLS = ("simulator", "measures", "quanta", "componentlist", "simutil")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tool", choices=TOOLS, default="simulator", help="the class to dump")
    parser.add_argument("--task", choices=("simobserve",), help="dump a task's signature instead")
    parser.add_argument("--methods", nargs="*", help="record only these methods")
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


def tool_class(name: str) -> type:
    if name == "simutil":
        from casatasks.private import simutil

        return simutil.simutil
    return getattr(casatools, name)


def dump_task(name: str, out: str) -> None:
    """The task's call signature, CASA's parameters in order with their
    defaults, as `casatasks.<name>` takes them."""
    import casatasks

    record = {
        "casa_version": casatools.version_string(),
        "task": name,
        "parameters": describe(type(getattr(casatasks, name)).__call__),
    }
    with open(out, "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> None:
    args = parse_args()
    if args.task:
        dump_task(args.task, args.out)
        return
    tool = tool_class(args.tool)
    methods = {
        name: describe(member)
        for name, member in inspect.getmembers(tool, inspect.isfunction)
        if not name.startswith("_") and (not args.methods or name in args.methods)
    }
    record = {
        "casa_version": casatools.version_string(),
        "tool": args.tool,
        "constructor": describe(tool.__init__),
        "methods": methods,
    }
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=2, sort_keys=True)
        handle.write("\n")


if __name__ == "__main__":
    main()
