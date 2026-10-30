#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA oracle for the `table` tool (`tb`), for #1025's table part (#1028).

Run with CASA 6.7.6.14's Python, for example
`~/ext/DeveloperTools/CASA.app/Contents/MacOS/python3 -I`:

    table_tool_oracle.py FIXTURE_TABLE MS --json OUT.json

FIXTURE_TABLE is `python_fixture.table`, the table
`cargo run -p casars-python --example make_python_fixtures -- DIR` writes, and
MS is the MeasurementSet `crates/casa-ms/tests/fixtures/mssel_test_small.ms.tgz`
unpacks to. Each scenario in `SCENARIOS` starts from a fresh copy of its
fixture (`table` or `ms`) and replays its steps on a `tb` tool. A step is `[method, args, kwargs]`; the string
`"<TABLE>"` in an argument stands for the scenario's copy of the table. The
record keeps, for every step, either the encoded value `tb` returned
(`"value"`) or the exception it raised (`"error"`: its type and message).

`crates/casars-python/python/tests/test_tools_table.py` replays the recorded
steps on `casars.tools.table` and compares with `encode` below; keep
`encode` in step with `test_tools_table.encode_value`. The fixture's path is
written as `<TABLE>` wherever it appears in a message or a string.
"""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from pathlib import Path

import numpy as np
from casatools import table
import casatools

PLACEHOLDER = "<TABLE>"
COLUMNS = ["id", "label", "gain", "spectrum", "vary", "meta"]


def encode(value, path):
    """`value` as JSON that tells int from float, tuple from list and keeps
    NumPy dtypes, shapes and values (flat, in C order)."""
    if isinstance(value, (bool, np.bool_)):
        return {"bool": bool(value)}
    if isinstance(value, (int, np.integer)):
        return {"int": int(value), "type": type(value).__name__}
    if isinstance(value, (float, np.floating)):
        return {"float": repr(float(value)), "type": type(value).__name__}
    if isinstance(value, (complex, np.complexfloating)):
        return {"complex": [repr(value.real), repr(value.imag)], "type": type(value).__name__}
    if isinstance(value, str):
        return {"str": value.replace(path, PLACEHOLDER)}
    if isinstance(value, np.ndarray):
        flat = value.ravel(order="C")
        if value.dtype.kind in "US":
            data = [str(item).replace(path, PLACEHOLDER) for item in flat]
        elif value.dtype.kind == "c":
            data = [[repr(item.real), repr(item.imag)] for item in flat.tolist()]
        elif value.dtype.kind == "f":
            data = [repr(item) for item in flat.tolist()]
        else:
            data = flat.tolist()
        return {"ndarray": str(value.dtype), "shape": list(value.shape), "data": data}
    if isinstance(value, dict):
        return {"dict": [[str(key), encode(item, path)] for key, item in value.items()]}
    if isinstance(value, tuple):
        return {"tuple": [encode(item, path) for item in value]}
    if isinstance(value, list):
        return {"list": [encode(item, path) for item in value]}
    if value is None:
        return {"none": True}
    raise TypeError(f"cannot encode {type(value)}")


def steps_for_each_column(method, *extra, **keywords):
    return [[method, [column, *extra], keywords] for column in COLUMNS]


#: The scenarios: a name, and its steps.
SCENARIOS = {
    # What the tool says about an open table, column by column.
    "describe": [
        ["open", [PLACEHOLDER], {}],
        ["ok", [], {}],
        ["name", [], {}],
        ["nrows", [], {}],
        ["ncols", [], {}],
        ["colnames", [], {}],
        ["iswritable", [], {}],
        ["haslock", [], {}],
        ["haslock", [], {"write": False}],
        ["lockoptions", [], {}],
        ["info", [], {}],
        *steps_for_each_column("isscalarcol"),
        *steps_for_each_column("isvarcol"),
        *steps_for_each_column("coldatatype"),
        *steps_for_each_column("colarraytype"),
        ["isscalarcol", ["nosuchcolumn"], {}],
        ["close", [], {}],
        ["ok", [], {}],
    ],
    # Cells and columns.
    "cells": [
        ["open", [PLACEHOLDER], {}],
        *[["getcell", [column, row], {}] for column in COLUMNS for row in range(3)],
        ["getcell", ["id"], {}],
        *steps_for_each_column("getcol"),
        *steps_for_each_column("getcol", startrow=1, nrow=2),
        *steps_for_each_column("getcol", startrow=0, nrow=-1, rowincr=2),
        *steps_for_each_column("getcolshapestring"),
        *[["iscelldefined", [column, row], {}] for column in COLUMNS for row in range(3)],
        ["getcell", ["nosuchcolumn", 0], {}],
        ["getcell", ["id", 3], {}],
        ["getcell", ["id", -1], {}],
        ["getcol", ["id", 5], {}],
        ["getvarcol", ["vary"], {}],
        ["getvarcol", ["id"], {}],
        ["close", [], {}],
        ["getcell", ["id", 0], {}],
    ],
    # Keywords.
    "keywords": [
        ["open", [PLACEHOLDER], {}],
        ["keywordnames", [], {}],
        ["getkeywords", [], {}],
        ["getkeyword", ["observer"], {}],
        ["getkeyword", [0], {}],
        ["getkeyword", ["nosuchkeyword"], {}],
        ["fieldnames", [], {}],
        ["fieldnames", ["observer"], {}],
        *steps_for_each_column("colkeywordnames"),
        *steps_for_each_column("getcolkeywords"),
        ["getcolkeyword", ["spectrum", "unit"], {}],
        ["getcolkeyword", ["spectrum", 0], {}],
        ["getcolkeyword", ["spectrum", "nosuchkeyword"], {}],
        ["colfieldnames", ["spectrum"], {}],
        ["colfieldnames", ["spectrum", "unit"], {}],
        ["getcolkeywords", ["nosuchcolumn"], {}],
        ["close", [], {}],
    ],
    # Writing table keywords, with every Python type a keyword can be, then
    # reading them back from a table reopened.
    "keywords_write": [
        ["open", [PLACEHOLDER], {"nomodify": False}],
        ["putkeyword", ["version", 2], {}],
        ["putkeyword", ["newint", 7], {}],
        ["putkeyword", ["newfloat", 1.5], {}],
        ["putkeyword", ["newbool", True], {}],
        ["putkeyword", ["newstr", "text"], {}],
        ["putkeyword", ["newcomplex", 1 + 2j], {}],
        ["putkeyword", ["arr_int", np.array([1, 2, 3])], {}],
        ["putkeyword", ["arr_float", np.arange(6.0).reshape(2, 3)], {}],
        ["putkeyword", ["arr_str", np.array(["a", "bc"])], {}],
        ["putkeyword", ["arr_bool", np.array([True, False])], {}],
        ["putkeyword", ["arr_complex", np.array([1 + 1j, 2 - 2j])], {}],
        ["putkeyword", ["list_int", [1, 2, 3]], {}],
        ["putkeyword", ["list_float", [1.5, 2.5]], {}],
        ["putkeyword", ["list_str", ["x", "y"]], {}],
        ["putkeyword", ["rec", {"a": 1, "b": "two", "c": {"d": 3.5}}], {}],
        ["getkeyword", ["rec.c.d"], {}],
        ["putkeyword", ["rec.c.d", 9.5], {}],
        ["putkeyword", ["rec.added", 1], {}],
        ["putkeyword", ["norec.x", 1], {}],
        ["putkeyword", ["norec.x", 1], {"makesubrecord": True}],
        ["putkeyword", ["observer", 5], {}],
        ["putkeyword", ["newint", 8.5], {}],
        ["putkeyword", ["newfloat", 3], {}],
        ["putkeyword", ["newstr", 5], {}],
        ["putkeyword", ["newbool", 1], {}],
        ["putkeyword", ["newint", np.int32(9)], {}],
        ["putkeyword", ["newfloat", np.float32(2.5)], {}],
        ["putkeyword", ["newint", {"a": 1}], {}],
        ["putkeyword", ["rec", 5], {}],
        ["putkeyword", ["arr_int", np.array([4, 5])], {}],
        ["putkeyword", ["arr_int", np.array([1.5])], {}],
        ["putkeyword", ["arr_int", 7], {}],
        ["putkeyword", ["newint", np.array([7])], {}],
        ["getkeyword", ["version"], {}],
        ["putkeyword", [1, "by index"], {}],
        ["putkeyword", [99, "by index"], {}],
        ["getkeywords", [], {}],
        ["putkeywords", [{"k1": 1, "k2": "v"}], {}],
        ["keywordnames", [], {}],
        ["removekeyword", ["k1"], {}],
        ["removekeyword", ["nosuchkeyword"], {}],
        ["removekeyword", ["rec.c"], {}],
        ["removekeyword", [99], {}],
        ["keywordnames", [], {}],
        ["getkeywords", [], {}],
        ["close", [], {}],
        ["open", [PLACEHOLDER], {}],
        ["keywordnames", [], {}],
        ["getkeywords", [], {}],
        ["close", [], {}],
    ],
    # CASA's own faults, which casa-rs does not follow (docs/CASA (C++)
    # bugs.md): a Python int beyond 32 bits is stored truncated, as 0.
    "casa_bugs": [
        ["open", [PLACEHOLDER], {"nomodify": False}],
        ["putkeyword", ["newbig", 2**40], {}],
        ["getkeyword", ["newbig"], {}],
        ["putkeyword", ["version", 2**40], {}],
        ["getkeyword", ["version"], {}],
        ["close", [], {}],
    ],
    # Writing a column's keywords.
    "column_keywords_write": [
        ["open", [PLACEHOLDER], {"nomodify": False}],
        ["putcolkeyword", ["spectrum", "unit", "mJy"], {}],
        ["putcolkeyword", ["id", "newkw", 3], {}],
        ["putcolkeyword", ["id", "arr", np.array([1.5, 2.5])], {}],
        ["putcolkeyword", ["id", "sub", {"p": 1, "q": "r"}], {}],
        ["putcolkeywords", ["label", {"a": 1, "b": [1.5, 2.5]}], {}],
        ["getcolkeywords", ["spectrum"], {}],
        ["getcolkeywords", ["id"], {}],
        ["getcolkeywords", ["label"], {}],
        ["removecolkeyword", ["spectrum", "frame"], {}],
        ["removecolkeyword", ["spectrum", "nosuchkeyword"], {}],
        ["removecolkeyword", ["id", "sub"], {}],
        ["putcolkeyword", ["nosuchcolumn", "x", 1], {}],
        ["putcolkeywords", ["nosuchcolumn", {"x": 1}], {}],
        ["removecolkeyword", ["nosuchcolumn", "x"], {}],
        ["colkeywordnames", ["spectrum"], {}],
        ["colkeywordnames", ["id"], {}],
        ["close", [], {}],
        ["open", [PLACEHOLDER], {}],
        ["getcolkeywords", ["spectrum"], {}],
        ["getcolkeywords", ["id"], {}],
        ["getcolkeywords", ["label"], {}],
        ["close", [], {}],
    ],
    # Writing the table's info: type, subtype and readme lines.
    "info_write": [
        ["open", [PLACEHOLDER], {"nomodify": False}],
        ["info", [], {}],
        ["putinfo", [{"type": "MyType", "subType": "sub", "readme": "line1\nline2\n"}], {}],
        ["info", [], {}],
        ["addreadmeline", ["line3"], {}],
        ["info", [], {}],
        ["addreadmeline", ["line4\n"], {}],
        ["info", [], {}],
        ["putinfo", [{"type": "OnlyType"}], {}],
        ["info", [], {}],
        ["putinfo", [{"readme": "r"}], {}],
        ["info", [], {}],
        ["putinfo", ["not a record"], {}],
        ["close", [], {}],
        ["open", [PLACEHOLDER], {}],
        ["info", [], {}],
        ["close", [], {}],
    ],
    # What a read-only table and an unlocked user-locked table say to a write.
    "write_refusals": [
        ["open", [PLACEHOLDER], {}],
        ["putkeyword", ["x", 1], {}],
        ["putkeywords", [{"x": 1}], {}],
        ["removekeyword", ["observer"], {}],
        ["putcolkeyword", ["id", "x", 1], {}],
        ["putcolkeywords", ["id", {"x": 1}], {}],
        ["removecolkeyword", ["spectrum", "unit"], {}],
        ["putinfo", [{"type": "T"}], {}],
        ["addreadmeline", ["r"], {}],
        ["getkeywords", [], {}],
        ["close", [], {}],
        ["open", [PLACEHOLDER], {"lockoptions": {"option": "user"}, "nomodify": False}],
        ["putkeyword", ["x", 1], {}],
        ["haslock", [], {}],
        ["lock", [], {}],
        ["putkeyword", ["x", 1], {}],
        ["haslock", [], {}],
        ["unlock", [], {}],
        ["close", [], {}],
        ["open", [PLACEHOLDER], {}],
        ["getkeyword", ["x"], {}],
        ["close", [], {}],
        ["putkeyword", ["x", 1], {}],
        ["putinfo", [{"type": "T"}], {}],
    ],
    # What each read method says to a column or row that is not there, and
    # to a row selection that does not fit.
    "errors": [
        ["open", [PLACEHOLDER], {}],
        ["iscelldefined", ["nosuchcolumn", 0], {}],
        ["iscelldefined", ["id", 3], {}],
        ["iscelldefined", ["id", -1], {}],
        ["coldatatype", ["nosuchcolumn"], {}],
        ["colarraytype", ["nosuchcolumn"], {}],
        ["isvarcol", ["nosuchcolumn"], {}],
        ["getcol", ["nosuchcolumn"], {}],
        ["getcolshapestring", ["nosuchcolumn"], {}],
        ["getvarcol", ["nosuchcolumn"], {}],
        ["getvarcol", ["vary"], {"startrow": 5}],
        ["getvarcol", ["meta"], {}],
        ["colkeywordnames", ["nosuchcolumn"], {}],
        ["getcolkeyword", ["nosuchcolumn", "unit"], {}],
        ["colfieldnames", ["nosuchcolumn"], {}],
        ["getkeyword", [], {}],
        ["getkeyword", [""], {}],
        ["getkeyword", [[]], {}],
        ["getkeyword", [5], {}],
        ["fieldnames", ["nosuchkeyword"], {}],
        ["getcolkeyword", ["spectrum"], {}],
        ["getcol", ["label"], {"startrow": -1}],
        ["getcol", ["label"], {"startrow": 1, "nrow": 5}],
        ["getcol", ["label"], {"startrow": 0, "nrow": 2, "rowincr": 0}],
        ["getcol", ["label"], {"startrow": 0, "nrow": 2, "rowincr": 2}],
        ["getcol", ["label"], {"startrow": 0, "nrow": 0}],
        ["getcol", ["spectrum"], {"startrow": 0, "nrow": 0}],
        ["getcell", ["label", 1.5], {}],
        ["getcell", [5, 0], {}],
        ["close", [], {}],
    ],
    # Every read method with no table open.
    "closed": [
        ["ok", [], {}],
        ["name", [], {}],
        ["nrows", [], {}],
        ["ncols", [], {}],
        ["colnames", [], {}],
        ["iswritable", [], {}],
        ["info", [], {}],
        ["lockoptions", [], {}],
        ["haslock", [], {}],
        ["keywordnames", [], {}],
        ["getkeywords", [], {}],
        ["getkeyword", ["observer"], {}],
        ["fieldnames", [], {}],
        ["isscalarcol", ["id"], {}],
        ["isvarcol", ["id"], {}],
        ["coldatatype", ["id"], {}],
        ["colarraytype", ["spectrum"], {}],
        ["getcell", ["id", 0], {}],
        ["getcol", ["id"], {}],
        ["getvarcol", ["vary"], {}],
        ["getcolshapestring", ["spectrum"], {}],
        ["iscelldefined", ["id", 0], {}],
        ["colkeywordnames", ["spectrum"], {}],
        ["getcolkeywords", ["spectrum"], {}],
        ["getcolkeyword", ["spectrum", "unit"], {}],
        ["colfieldnames", ["spectrum"], {}],
        ["close", [], {}],
        ["done", [], {}],
        ["lock", [], {}],
        ["unlock", [], {}],
    ],
    # open's own cases, and the lock calls.
    "open": [
        ["open", ["/no/such/table"], {}],
        ["ok", [], {}],
        ["open", [PLACEHOLDER], {"nomodify": False}],
        ["iswritable", [], {}],
        ["haslock", [], {}],
        ["haslock", [], {"write": False}],
        ["lockoptions", [], {}],
        ["done", [], {}],
        ["ok", [], {}],
        ["open", [PLACEHOLDER], {"lockoptions": "user"}],
        ["ok", [], {}],
        ["lockoptions", [], {}],
        ["haslock", [], {}],
        ["open", [PLACEHOLDER], {"lockoptions": {"option": "usernoread"}}],
        ["lockoptions", [], {}],
        ["close", [], {}],
        *[
            step
            for option in ["user", "usernoread", "permanent", "auto", "autonoread", "default"]
            for step in [
                ["open", [PLACEHOLDER], {"lockoptions": {"option": option}, "nomodify": False}],
                ["lockoptions", [], {}],
                ["haslock", [], {}],
                ["haslock", [], {"write": False}],
                ["lock", [], {}],
                ["haslock", [], {}],
                ["haslock", [], {"write": False}],
                ["unlock", [], {}],
                ["haslock", [], {}],
                ["haslock", [], {"write": False}],
                ["lock", [], {"write": False}],
                ["haslock", [], {}],
                ["haslock", [], {"write": False}],
                ["unlock", [], {}],
                ["close", [], {}],
            ]
        ],
        ["open", [PLACEHOLDER], {"lockoptions": {"option": "user", "interval": 2.5, "maxwait": 7}}],
        ["lockoptions", [], {}],
        ["close", [], {}],
        ["open", [PLACEHOLDER], {"lockoptions": {"option": "nosuchoption"}}],
        ["ok", [], {}],
    ],
}

#: The scenario the MeasurementSet gets: for each column of the main table,
#: its description and, for the first rows, its cells. Built from the table
#: itself, so a column is probed whatever the fixture holds.
MS_ROWS = 3


def ms_steps(source):
    probe = table()
    probe.open(str(source))
    columns = list(probe.colnames())
    arrays = {column: not probe.isscalarcol(column) for column in columns}
    probe.done()
    steps = [["open", [PLACEHOLDER], {}], ["nrows", [], {}], ["colnames", [], {}]]
    for column in columns:
        steps += [
            ["isscalarcol", [column], {}],
            ["isvarcol", [column], {}],
            ["coldatatype", [column], {}],
            ["colarraytype", [column], {}],
            ["getcolkeywords", [column], {}],
            ["getcell", [column, 0], {}],
            ["iscelldefined", [column, 0], {}],
            ["getcol", [column], {"startrow": 0, "nrow": MS_ROWS}],
            ["getcol", [column], {"startrow": 1, "nrow": 2, "rowincr": 1}],
        ]
        if arrays[column]:
            steps.append(["getcolshapestring", [column], {"startrow": 0, "nrow": MS_ROWS}])
    steps += [["keywordnames", [], {}], ["getkeywords", [], {}], ["info", [], {}]]
    return steps


def run_scenario(steps, source):
    work = Path(tempfile.mkdtemp(prefix="tb_oracle_"))
    path = str(work / Path(source).name)
    shutil.copytree(source, path)
    tool = table()
    record = []
    for method, args, keywords in steps:
        real_args = [path if isinstance(arg, str) and arg == PLACEHOLDER else arg for arg in args]
        outcome = {
            "method": method,
            "args": [encode(arg, path) for arg in args],
            "kwargs": {name: encode(value, path) for name, value in keywords.items()},
        }
        try:
            outcome["value"] = encode(getattr(tool, method)(*real_args, **keywords), path)
        except Exception as error:  # noqa: BLE001 - recording whatever CASA raises
            outcome["error"] = {
                "type": type(error).__name__,
                "message": str(error).replace(path, PLACEHOLDER),
            }
        record.append(outcome)
    try:
        tool.done()
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("fixture", type=Path)
    parser.add_argument("ms", type=Path)
    parser.add_argument("--json", type=Path, required=True)
    arguments = parser.parse_args()
    scenarios = {
        name: {"fixture": "table", "steps": steps, "source": arguments.fixture}
        for name, steps in SCENARIOS.items()
    }
    scenarios["ms"] = {"fixture": "ms", "steps": ms_steps(arguments.ms), "source": arguments.ms}
    record = {
        "casa_version": ".".join(str(part) for part in casatools.version()[:4]),
        "scenarios": {
            name: {
                "fixture": scenario["fixture"],
                "steps": run_scenario(scenario["steps"], scenario["source"]),
            }
            for name, scenario in scenarios.items()
        },
    }
    arguments.json.write_text(json.dumps(record, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
