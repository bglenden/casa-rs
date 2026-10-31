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
fixture (`table` or `ms`) and replays its steps on a `tb` tool. A step is `[method, args, kwargs]`, with
a fourth element `{"on": NAME, "bind": NAME}` when it calls a tool a step before returned (`bind` names the tool
a step returns, `on` the tool a step calls); the string
`"<TABLE>"` in an argument stands for the scenario's copy of the table (`"<TABLE>.out"` is a sibling of it). The
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
    if type(value).__name__ == "table":
        return {"tool": True}
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

#: Cell and column writes on the python fixture table (more steps are added
#: to `SCENARIOS` below this dict's definition, and read back after a reopen).
CELL_WRITES = [
    ["open", [PLACEHOLDER], {"nomodify": False}],
    ["putcell", ["id", 0, 99], {}],
    ["getcell", ["id", 0], {}],
    ["putcell", ["id", 1, np.int32(98)], {}],
    ["putcell", ["id", 2, 7.0], {}],
    ["putcell", ["id", 2, 7.5], {}],
    ["putcell", ["id", 2, "text"], {}],
    ["putcell", ["label", 1, "new"], {}],
    ["putcell", ["label", 1, 5], {}],
    ["putcell", ["gain", 2, 5 + 1j], {}],
    ["putcell", ["gain", 0, 3.0], {}],
    ["putcell", ["spectrum", 0, np.arange(4.0).reshape(2, 2)], {}],
    ["putcell", ["spectrum", 1, np.arange(4, dtype=np.int32).reshape(2, 2)], {}],
    ["putcell", ["spectrum", 2, np.arange(6.0).reshape(2, 3)], {}],
    ["putcell", ["spectrum", 2, 5.0], {}],
    ["putcell", ["vary", 1, np.array([1.0, 2.0, 3.0, 4.0])], {}],
    ["putcell", ["vary", 0, 5.0], {}],
    ["putcell", ["meta", 0, {"label": "zzz", "weight": 7}], {}],
    ["putcell", ["meta", 1, 5], {}],
    ["putcell", ["id", 3, 1], {}],
    ["putcell", ["id", -1, 1], {}],
    ["putcell", ["nosuchcolumn", 0, 1], {}],
    ["getcol", ["id"], {}],
    ["getcol", ["label"], {}],
    ["getcol", ["gain"], {}],
    ["getcol", ["spectrum"], {}],
    ["getvarcol", ["vary"], {}],
    ["getcell", ["meta", 0], {}],
    ["putcol", ["id", np.array([7, 8, 9])], {}],
    ["getcol", ["id"], {}],
    ["putcol", ["id", np.array([1, 2])], {}],
    ["putcol", ["id", np.array([4, 5, 6, 7])], {}],
    ["putcol", ["id", np.array([11, 12])], {"startrow": 1, "nrow": 2}],
    ["getcol", ["id"], {}],
    ["putcol", ["id", np.array([20, 30])], {"startrow": 0, "nrow": 2, "rowincr": 2}],
    ["getcol", ["id"], {}],
    ["putcol", ["label", np.array(["a", "bb", "ccc"])], {}],
    ["getcol", ["label"], {}],
    ["putcol", ["label", ["p", "q", "r"]], {}],
    ["getcol", ["label"], {}],
    ["putcol", ["gain", np.array([1 + 1j, 2 + 2j, 3 + 3j])], {}],
    ["getcol", ["gain"], {}],
    ["putcol", ["spectrum", np.arange(12.0).reshape(2, 2, 3)], {}],
    ["getcol", ["spectrum"], {}],
    ["putcol", ["spectrum", np.arange(12.0).reshape(3, 2, 2)], {}],
    ["putcol", ["id", 5], {}],
    ["putcol", ["nosuchcolumn", np.array([1, 2, 3])], {}],
    ["putvarcol", ["vary", {"r1": np.array([1.0]), "r2": np.array([2.0, 3.0]), "r3": np.array([4.0, 5.0, 6.0])}], {}],
    ["putvarcol", ["vary", {"r1": np.array([[1.0]]), "r2": np.array([[2.0], [3.0]]), "r3": np.array([[4.0], [5.0], [6.0]])}], {}],
    ["getvarcol", ["vary"], {}],
    ["close", [], {}],
    ["open", [PLACEHOLDER], {}],
    ["getcol", ["id"], {}],
    ["getcol", ["label"], {}],
    ["getcol", ["gain"], {}],
    ["getcol", ["spectrum"], {}],
    ["getvarcol", ["vary"], {}],
    ["getcell", ["meta", 0], {}],
    ["putcell", ["id", 0, 1], {}],
    ["putcol", ["id", np.array([1, 2, 3])], {}],
    ["close", [], {}],
]

#: Slices of cells and columns on the python fixture table: `spectrum` is a
#: 2 x 2 float array in each of 3 rows, `vary` a one-dimensional double
#: array of a different length in each, `id` a scalar column.
SLICES = [
    ["open", [PLACEHOLDER], {"nomodify": False}],
    ["getcellslice", ["spectrum", 0, [0, 0], [1, 1], [1, 1]], {}],
    ["getcellslice", ["spectrum", 1, [0, 0], [0, 1], [1, 1]], {}],
    ["getcellslice", ["spectrum", 1, [1, 0], [1, 1], [1, 1]], {}],
    ["getcellslice", ["spectrum", 2, [0, 0], [-1, -1], [1, 1]], {}],
    ["getcellslice", ["spectrum", 2, [0, 0], [1, 1], [2, 1]], {}],
    ["getcellslice", ["spectrum", 3, [0, 0], [1, 1], [1, 1]], {}],
    ["getcellslice", ["nosuchcolumn", 0, [0, 0], [1, 1], [1, 1]], {}],
    ["getcellslice", ["vary", 1, [1], [2]], {}],
    ["getcolslice", ["spectrum", [0, 0], [0, 1], [1, 1]], {}],
    ["getcolslice", ["spectrum", [0, 0], [1, 1], [1, 1]], {"startrow": 1, "nrow": 2}],
    ["getcolslice", ["spectrum", [0, 0], [1, 1], [1, 1]], {"startrow": 0, "nrow": -1, "rowincr": 2}],
    ["getcolslice", ["spectrum", [0, 0], [-1, -1], [1, 1]], {}],
    ["getcolslice", ["vary", [0], [0], [1]], {}],
    ["getcolslice", ["id", [0], [0], [1]], {}],
    ["putcellslice", ["spectrum", 0, np.array([[9.0, 8.0]]), [0, 0], [0, 1], [1, 1]], {}],
    ["getcell", ["spectrum", 0], {}],
    ["putcellslice", ["spectrum", 0, np.array([[7, 6]], dtype=np.int32), [1, 0], [1, 1], [1, 1]], {}],
    ["getcell", ["spectrum", 0], {}],
    ["putcellslice", ["spectrum", 0, np.array([[1.0, 2.0, 3.0]]), [0, 0], [0, 1], [1, 1]], {}],
    ["putcellslice", ["spectrum", 0, np.array([[5.0, 5.0]]), [0, 0], [0, 1], [1, 1]], {}],
    ["putcellslice", ["spectrum", 1, np.array([[4.0], [4.0]]), [0, 0], [1, 0], [1, 1]], {}],
    ["getcell", ["spectrum", 1], {}],
    ["putcellslice", ["spectrum", 0, 3.5, [0, 0], [0, 0], [1, 1]], {}],
    ["getcell", ["spectrum", 0], {}],
    ["putcellslice", ["spectrum", 3, np.array([[1.0]]), [0, 0], [0, 0], [1, 1]], {}],
    ["putcolslice", ["spectrum", np.zeros((1, 2, 3)), [0, 0], [0, 1], [1, 1]], {}],
    ["getcol", ["spectrum"], {}],
    ["putcolslice", ["spectrum", np.ones((1, 2, 2)), [1, 0], [1, 1], [1, 1]], {"startrow": 1, "nrow": 2}],
    ["getcol", ["spectrum"], {}],
    ["putcolslice", ["spectrum", np.ones((1, 2, 2)), [0, 0], [0, 1], [1, 1]], {}],
    ["close", [], {}],
    ["open", [PLACEHOLDER], {}],
    ["getcol", ["spectrum"], {}],
    ["putcellslice", ["spectrum", 0, np.array([[1.0, 1.0]]), [0, 0], [0, 1], [1, 1]], {}],
    ["close", [], {}],
]

#: Slices of the MeasurementSet's complex `DATA` and bool `FLAG` arrays
#: (4 x 64 in the rows probed).
MS_SLICES = [
    ["open", [PLACEHOLDER], {"nomodify": False}],
    ["getcellslice", ["DATA", 0, [0, 0], [1, 3], [1, 1]], {}],
    ["getcellslice", ["DATA", 0, [0, 62], [3, -1], [1, 1]], {}],
    ["getcellslice", ["FLAG", 0, [0, 0], [3, 0], [1, 1]], {}],
    ["getcolslice", ["DATA", [0, 0], [1, 3], [1, 1]], {"startrow": 0, "nrow": 2}],
    ["getcolslice", ["FLAG", [0, 0], [3, 1], [1, 1]], {"startrow": 1, "nrow": 2}],
    ["putcellslice", ["DATA", 0, np.zeros((2, 4), dtype=np.complex128), [0, 0], [1, 3], [1, 1]], {}],
    ["getcellslice", ["DATA", 0, [0, 0], [1, 3], [1, 1]], {}],
    ["putcellslice", ["DATA", 1, np.ones((2, 4)), [2, 10], [3, 13], [1, 1]], {}],
    ["getcellslice", ["DATA", 1, [2, 10], [3, 13], [1, 1]], {}],
    ["putcellslice", ["FLAG", 0, np.ones((4, 1), dtype=bool), [0, 5], [3, 5], [1, 1]], {}],
    ["getcellslice", ["FLAG", 0, [0, 4], [3, 6], [1, 1]], {}],
    ["putcolslice", ["DATA", np.ones((2, 2, 2), dtype=np.complex64) * (1 + 1j), [0, 0], [1, 1], [1, 1]], {"startrow": 0, "nrow": 2}],
    ["getcolslice", ["DATA", [0, 0], [1, 1], [1, 1]], {"startrow": 0, "nrow": 2}],
    ["close", [], {}],
    ["open", [PLACEHOLDER], {}],
    ["getcellslice", ["DATA", 0, [0, 0], [1, 1], [1, 1]], {}],
    ["getcellslice", ["DATA", 1, [2, 10], [3, 13], [1, 1]], {}],
    ["getcellslice", ["FLAG", 0, [0, 4], [3, 6], [1, 1]], {}],
    ["close", [], {}],
]

#: Column descriptions `addcols` takes, one column each and each named for
#: what it probes, in `TableProxy::makeTableDesc`'s order of checks. Every call
#: is followed by `colnames`, and every name by `getcoldesc` at the end: the
#: refusals say what CASA raises, the others what it made.
ADDCOLS_CASES = {
    "shape_is_fixed": {"valueType": "float", "ndim": 2, "shape": [2, 3]},
    "c_order": {"valueType": "float", "ndim": 2, "shape": [2, 3], "_c_order": True},
    "rank_mismatch": {"valueType": "float", "ndim": 1, "shape": [2, 3]},
    "negative_extent": {"valueType": "float", "ndim": 2, "shape": [2, -3]},
    "zero_extent": {"valueType": "float", "ndim": 2, "shape": [2, 0]},
    "shape_with_zero_ndim": {"valueType": "float", "ndim": 0, "shape": [2]},
    "shape_with_negative_ndim": {"valueType": "float", "ndim": -1, "shape": [2]},
    "unspecified_ndim": {"valueType": "float", "ndim": -1},
    "ndim_string": {"valueType": "float", "ndim": "2"},
    "keywords_int": {"valueType": "float", "keywords": 3},
    "byte_scalar": {"valueType": "byte"},
    "integer_scalar": {"valueType": "integer"},
    "byte_array": {"valueType": "byte", "ndim": 1},
    "integer_array": {"valueType": "integer", "ndim": 1},
    "mixed_case": {"valueType": "Double"},
    "valuetype_int": {"valueType": 3},
    "no_valuetype": {"comment": "c"},
    "record_array": {"valueType": "record", "ndim": 2},
    "record_scalar": {"valueType": "record"},
    "option_direct": {"valueType": "int", "option": 1},
    "option_undefined": {"valueType": "int", "option": 2},
    "option_string": {"valueType": "int", "option": "x"},
    "maxlen": {"valueType": "string", "maxlen": 5},
    "ndim_float": {"valueType": "float", "ndim": 2.0},
    "ndim_bool": {"valueType": "float", "ndim": True},
    "shape_floats": {"valueType": "float", "ndim": 2, "shape": [2.0, 3.0]},
    "shape_scalar": {"valueType": "float", "ndim": 1, "shape": 3},
    "comment_int": {"valueType": "int", "comment": 5},
    "unknown_field": {"valueType": "int", "frobnicate": 1},
    "unknown_type": {"valueType": "int128"},
}

ADDCOLS_DESCRIPTIONS = [
    ["open", [PLACEHOLDER], {"nomodify": False}],
    *[
        step
        for name, description in ADDCOLS_CASES.items()
        for step in (["addcols", [{name: description}], {}], ["colnames", [], {}])
    ],
    # Fields that are not columns.
    ["addcols", [{"_keywords_": {"a": 1}, "special_a": {"valueType": "int"}}], {}],
    ["addcols", [{"_private_keywords_": {"a": 1}, "special_b": {"valueType": "int"}}], {}],
    ["addcols", [{"_define_dminfo_": {"a": 1}, "special_c": {"valueType": "int"}}], {}],
    ["addcols", [{"_keywords_": 3}], {}],
    ["colnames", [], {}],
    *[["getcoldesc", [name], {}] for name in ADDCOLS_CASES],
    *[["getcoldesc", [name], {}] for name in ("special_a", "special_b", "special_c")],
    ["close", [], {}],
]

#: Adding and removing rows on the python fixture table.
ROWS = [
    ["open", [PLACEHOLDER], {"nomodify": False}],
    ["nrows", [], {}],
    ["addrows", [2], {}],
    ["nrows", [], {}],
    ["getcol", ["label"], {}],
    # (CASA leaves the cells of an added array uninitialised: not read.)
    ["iscelldefined", ["spectrum", 3], {}],
    ["iscelldefined", ["vary", 3], {}],
    ["iscelldefined", ["id", 3], {}],
    ["getcell", ["id", 3], {}],
    ["getcell", ["meta", 3], {}],
    ["putcell", ["id", 3, 33], {}],
    ["putcell", ["label", 4, "five"], {}],
    ["addrows", [], {}],
    ["nrows", [], {}],
    ["removerows", [[3, 4]], {}],
    ["nrows", [], {}],
    ["removerows", [[0]], {}],
    ["nrows", [], {}],
    ["getcell", ["id", 0], {}],
    ["getcell", ["label", 0], {}],
    ["removerows", [[99]], {}],
    ["removerows", [[]], {}],
    ["removerows", [[0, 0]], {}],
    ["nrows", [], {}],
    ["addrows", [0], {}],
    ["nrows", [], {}],
    ["addrows", [3], {}],
    ["putcol", ["id", np.array([1, 2, 3])], {}],
    ["putcol", ["label", ["a", "b", "c"]], {}],
    ["addrows", [3], {}],
    *[["putcell", ["id", row, 10 + row], {}] for row in range(7)],
    # Rows out of order; a bad row number alone, and among good ones.
    ["removerows", [[5, 1]], {}],
    ["nrows", [], {}],
    ["getcell", ["id", 1], {}],
    ["getcell", ["id", 3], {}],
    ["removerows", [[-1]], {}],
    ["nrows", [], {}],
    ["removerows", [[1, 99]], {}],
    ["nrows", [], {}],
    ["getcell", ["id", 1], {}],
    ["removerows", [[2, 3, 50, 60]], {}],
    ["nrows", [], {}],
    ["removerows", [3], {}],
    ["nrows", [], {}],
    ["getcell", ["id", 3], {}],
    ["close", [], {}],
    ["open", [PLACEHOLDER], {}],
    ["nrows", [], {}],
    ["getcell", ["id", 0], {}],
    ["getcell", ["id", 2], {}],
    ["addrows", [1], {}],
    ["removerows", [[0]], {}],
    ["nrows", [], {}],
    ["close", [], {}],
]

#: Renaming and removing columns of the python fixture table, and what the
#: table says of them after a reopen.
COLUMNS = [
    ["open", [PLACEHOLDER], {"nomodify": False}],
    ["getcoldesc", ["id"], {}],
    ["getcoldesc", ["label"], {}],
    ["getcoldesc", ["spectrum"], {}],
    ["getcoldesc", ["vary"], {}],
    ["getcoldesc", ["meta"], {}],
    ["getcoldesc", ["nosuchcolumn"], {}],
    ["renamecol", ["label", "name"], {}],
    ["colnames", [], {}],
    ["getcell", ["name", 0], {}],
    ["getcoldesc", ["name"], {}],
    ["renamecol", ["nosuch", "z"], {}],
    ["renamecol", ["name", "id"], {}],
    ["renamecol", ["spectrum", "cube"], {}],
    ["getcell", ["cube", 0], {}],
    ["getcolkeywords", ["cube"], {}],
    ["getcoldesc", ["cube"], {}],
    ["getcoldesc", ["spectrum"], {}],
    ["removecols", [["gain"]], {}],
    ["colnames", [], {}],
    ["removecols", [["nosuch"]], {}],
    # A list with a missing column among them removes none of them.
    ["removecols", [["vary", "nosuch"]], {}],
    ["removecols", [["nosuch", "vary"]], {}],
    ["colnames", [], {}],
    ["removecols", ["meta"], {}],
    ["removecols", [[]], {}],
    ["colnames", [], {}],
    ["ncols", [], {}],
    ["getcell", ["id", 1], {}],
    ["close", [], {}],
    ["open", [PLACEHOLDER], {}],
    ["colnames", [], {}],
    ["getcell", ["name", 0], {}],
    ["getcell", ["cube", 0], {}],
    ["getcolkeywords", ["cube"], {}],
    ["getcell", ["id", 1], {}],
    ["renamecol", ["id", "ident"], {}],
    ["removecols", [["id"]], {}],
    ["close", [], {}],
]

#: Column descriptions, and adding and removing rows and columns, on the
#: python fixture table. (CASA leaves the cells of an added array or complex
#: column uninitialised, so they are never read here.)
ROWS_COLUMNS = [
    ["open", [PLACEHOLDER], {"nomodify": False}],
    ["getcoldesc", ["id"], {}],
    ["getcoldesc", ["label"], {}],
    ["getcoldesc", ["spectrum"], {}],
    ["getcoldesc", ["vary"], {}],
    ["getcoldesc", ["meta"], {}],
    ["getcoldesc", ["nosuchcolumn"], {}],
    ["getdminfo", [], {}],
    ["addrows", [2], {}],
    ["nrows", [], {}],
    ["getcol", ["label"], {}],
    ["iscelldefined", ["spectrum", 3], {}],
    ["iscelldefined", ["id", 3], {}],
    ["getcell", ["id", 3], {}],
    ["putcell", ["id", 3, 33], {}],
    ["putcell", ["label", 4, "five"], {}],
    ["addrows", [], {}],
    ["nrows", [], {}],
    ["removerows", [[3, 4]], {}],
    ["nrows", [], {}],
    ["removerows", [[0]], {}],
    ["nrows", [], {}],
    ["getcell", ["id", 0], {}],
    ["removerows", [[99]], {}],
    ["removerows", [[]], {}],
    ["removerows", [[0, 0]], {}],
    ["nrows", [], {}],
    ["addrows", [3], {}],
    ["putcol", ["id", np.array([1, 2, 3])], {}],
    ["putcol", ["label", ["a", "b", "c"]], {}],
    ["addcols", [{"x": {"valueType": "double", "comment": "a new column"}}], {}],
    ["ncols", [], {}],
    ["colnames", [], {}],
    ["getcoldesc", ["x"], {}],
    # (CASA leaves the cells of a column it adds uninitialised, an int or a
    # double as much as an array: only a string's and those written are read.)
    ["putcell", ["x", 0, 2.5], {}],
    ["addcols", [{"arr": {"valueType": "float", "ndim": 2, "shape": [2, 3], "option": 4}}], {}],
    ["getcoldesc", ["arr"], {}],
    ["addcols", [{"s": {"valueType": "string"}, "b": {"valueType": "boolean"}, "i": {"valueType": "int"}, "c": {"valueType": "dcomplex"}}], {}],
    ["colnames", [], {}],
    ["getcell", ["s", 0], {}],
    ["addcols", [{"x": {"valueType": "double"}}], {}],
    ["addcols", [{"bad": {"valueType": "nosuchtype"}}], {}],
    ["renamecol", ["x", "y"], {}],
    ["colnames", [], {}],
    ["getcell", ["y", 0], {}],
    ["renamecol", ["nosuch", "z"], {}],
    ["renamecol", ["y", "label"], {}],
    ["removecols", [["y"]], {}],
    ["colnames", [], {}],
    ["removecols", [["nosuch"]], {}],
    ["removecols", ["s"], {}],
    ["colnames", [], {}],
    # A list with a missing column among them: do the others go?
    ["removecols", [["b", "nosuch"]], {}],
    ["colnames", [], {}],
    ["removecols", [["nosuch", "c"]], {}],
    ["colnames", [], {}],
    ["removecols", [[]], {}],
    # Without a dimension a column is a scalar one; with one, an array of
    # variable shape whose cells start undefined.
    ["addcols", [{"free": {"valueType": "float"}}], {}],
    ["getcoldesc", ["free"], {}],
    ["addcols", [{"two": {"valueType": "float", "ndim": 2}}], {}],
    ["getcoldesc", ["two"], {}],
    ["iscelldefined", ["two", 0], {}],
    ["addcols", [{"kw": {"valueType": "double", "keywords": {"unit": "Jy", "n": 3}}}], {}],
    ["getcoldesc", ["kw"], {}],
    ["getcolkeywords", ["kw"], {}],
    ["getdminfo", [], {}],
    # A list with a column that exists among them: are the others added?
    ["addcols", [{"fresh": {"valueType": "int"}, "free": {"valueType": "int"}}], {}],
    ["colnames", [], {}],
    ["addcols", [{}], {}],
    ["addcols", [{"nodesc": 3}], {}],
    # Rows out of order; a bad row number alone; a row number alone.
    ["addrows", [3], {}],
    ["putcell", ["id", 0, 10], {}],
    ["putcell", ["id", 1, 11], {}],
    ["putcell", ["id", 2, 12], {}],
    ["putcell", ["id", 3, 13], {}],
    ["putcell", ["id", 4, 14], {}],
    ["putcell", ["id", 5, 15], {}],
    ["putcell", ["id", 6, 16], {}],
    ["removerows", [[5, 1]], {}],
    ["nrows", [], {}],
    ["getcell", ["id", 1], {}],
    ["getcell", ["id", 3], {}],
    ["removerows", [[-1]], {}],
    ["nrows", [], {}],
    # Atomic: a bad row number among good ones removes none.
    ["removerows", [[1, 99]], {}],
    ["nrows", [], {}],
    ["getcell", ["id", 1], {}],
    ["removerows", [[2, 3, 50, 60]], {}],
    ["nrows", [], {}],
    ["removerows", [3], {}],
    ["nrows", [], {}],
    ["getcell", ["id", 3], {}],
    ["close", [], {}],
    ["open", [PLACEHOLDER], {}],
    ["nrows", [], {}],
    ["colnames", [], {}],
    ["getcell", ["id", 0], {}],
    ["addrows", [1], {}],
    ["addcols", [{"q": {"valueType": "double"}}], {}],
    ["removerows", [[0]], {}],
    ["renamecol", ["id", "ident"], {}],
    ["removecols", [["id"]], {}],
    ["close", [], {}],
]

#: The python fixture table's columns (`COLUMNS` is a scenario by now).
CELL_COLUMNS = ["id", "label", "gain", "spectrum", "vary", "meta"]


def on(tool, steps):
    """`steps` called on the tool bound to `tool`."""
    return [[*step[:3], {"on": tool}] for step in steps]


def cells(tool, rows=3):
    """Every column's cell in the first `rows` rows, read from `tool`."""
    return on(tool, [["getcell", [column, row], {}] for row in range(rows) for column in CELL_COLUMNS])


def inspect(tool):
    """What a table tool the table tool returned says about its table."""
    return on(tool, [
        ["name", [], {}],
        ["iswritable", [], {}],
        ["lockoptions", [], {}],
        ["nrows", [], {}],
        ["colnames", [], {}],
        ["endianformat", [], {}],
        ["getkeywords", [], {}],
        ["info", [], {}],
        ["getdminfo", [], {}],
    ])


#: Calls that report or move the table's state, on the python fixture table,
#: before it is open, open for update, after, and open read-only.
STATE = [
    ["endianformat", [], {}],
    ["flush", [], {}],
    ["resync", [], {}],
    ["datachanged", [], {}],
    ["ismultiused", [], {}],
    ["setmaxcachesize", ["id", 10], {}],
    ["clearlocks", [], {}],
    ["open", [PLACEHOLDER], {"nomodify": False}],
    ["endianformat", [], {}],
    ["flush", [], {}],
    ["resync", [], {}],
    ["datachanged", [], {}],
    ["datachanged", [], {}],
    ["ismultiused", [], {}],
    ["ismultiused", [True], {}],
    ["ismultiused", [], {"checksubtables": True}],
    ["setmaxcachesize", ["spectrum", 1000], {}],
    ["setmaxcachesize", ["id", 1000], {}],
    ["setmaxcachesize", ["nosuch", 1000], {}],
    ["setmaxcachesize", ["spectrum", -1], {}],
    ["setmaxcachesize", ["spectrum", 0], {}],
    ["setmaxcachesize", ["", 1000], {}],
    ["setmaxcachesize", [], {"columnname": "label", "nbytes": 5}],
    ["clearlocks", [], {}],
    ["haslock", [], {"write": False}],
    ["putcell", ["id", 0, 5], {}],
    ["flush", [], {}],
    ["getcell", ["id", 0], {}],
    ["datachanged", [], {}],
    ["close", [], {}],
    ["endianformat", [], {}],
    ["flush", [], {}],
    ["resync", [], {}],
    ["datachanged", [], {}],
    ["ismultiused", [], {}],
    ["setmaxcachesize", ["id", 10], {}],
    ["clearlocks", [], {}],
    ["open", [PLACEHOLDER], {}],
    ["flush", [], {}],
    ["resync", [], {}],
    ["datachanged", [], {}],
    ["close", [], {}],
] + [
    step
    for option in ("default", "auto", "user", "usernoread", "permanent", "autonoread")
    for step in [
        ["open", [PLACEHOLDER], {"lockoptions": {"option": option}, "nomodify": False}],
        ["datachanged", [], {}],
        ["datachanged", [], {}],
        ["flush", [], {}],
        ["resync", [], {}],
        ["clearlocks", [], {}],
        ["haslock", [], {}],
        ["haslock", [], {"write": False}],
        ["close", [], {}],
    ]
]

#: The same calls on the MeasurementSet, whose files are big-endian.
STATE_MS = [
    ["open", [PLACEHOLDER], {}],
    ["endianformat", [], {}],
    ["datachanged", [], {}],
    ["ismultiused", [], {}],
    ["flush", [], {}],
    ["resync", [], {}],
    ["setmaxcachesize", ["TIME", 100], {}],
    ["close", [], {}],
]

#: `copy`: the tool it returns, and what each combination of its options
#: makes. The table is copied by name (`<TABLE>.cN`); a copy's tool is
#: closed before the name is used again.
COPIES = [
    ["open", [PLACEHOLDER], {"nomodify": False}],
    ["copy", [PLACEHOLDER + ".c1"], {}, {"bind": "c1"}],
    *inspect("c1"),
    *cells("c1"),
    ["putcell", ["label", 0, "w"], {}, {"on": "c1"}],
    ["close", [], {}, {"on": "c1"}],
    ["copy", [PLACEHOLDER + ".c2"], {"deep": True}, {"bind": "c2"}],
    *inspect("c2"),
    ["close", [], {}, {"on": "c2"}],
    ["copy", [PLACEHOLDER + ".c3"], {"valuecopy": True}, {"bind": "c3"}],
    *inspect("c3"),
    *cells("c3"),
    ["close", [], {}, {"on": "c3"}],
    ["copy", [PLACEHOLDER + ".c4"], {"norows": True}, {"bind": "c4"}],
    *inspect("c4"),
    ["getcoldesc", ["spectrum"], {}, {"on": "c4"}],
    ["getcoldesc", ["meta"], {}, {"on": "c4"}],
    ["getcell", ["id", 0], {}, {"on": "c4"}],
    ["close", [], {}, {"on": "c4"}],
    ["copy", [PLACEHOLDER + ".c5"], {"valuecopy": True, "endian": "big"}, {"bind": "c5"}],
    *inspect("c5"),
    *cells("c5"),
    ["close", [], {}, {"on": "c5"}],
    ["copy", [PLACEHOLDER + ".c6"], {"deep": True, "valuecopy": True, "endian": "LITTLE"}, {"bind": "c6"}],
    ["endianformat", [], {}, {"on": "c6"}],
    ["close", [], {}, {"on": "c6"}],
    # Copying to a name that is a table (closed) replaces it.
    ["copy", [PLACEHOLDER + ".c3"], {"norows": True}, {"bind": "c3b"}],
    ["nrows", [], {}, {"on": "c3b"}],
    ["close", [], {}, {"on": "c3b"}],
    ["copy", [PLACEHOLDER + ".c7"], {"endian": "bogus"}],
    ["copy", [PLACEHOLDER + ".c7"], {"valuecopy": True, "endian": "bogus"}],
    ["copy", [""], {}],
    # Copying a table to its own name.
    ["copy", [PLACEHOLDER], {}, {"bind": "same"}],
    ["name", [], {}, {"on": "same"}],
    ["nrows", [], {}, {"on": "same"}],
    ["close", [], {}, {"on": "same"}],
    ["copy", [PLACEHOLDER], {"norows": True}],
    ["copy", [PLACEHOLDER], {"valuecopy": True}],
    ["nrows", [], {}],
    ["close", [], {}],
    ["copy", [PLACEHOLDER + ".c8"], {}],
]

#: `copyrows` into a table named by its path: all the rows, a range,
#: appended, past the end, into a table with other columns, and into itself.
COPYROWS = [
    ["open", [PLACEHOLDER], {"nomodify": False}],
    ["copy", [PLACEHOLDER + ".out"], {"norows": True}, {"bind": "o"}],
    ["close", [], {}, {"on": "o"}],
    ["copy", [PLACEHOLDER + ".other"], {"norows": True}, {"bind": "p"}],
    ["close", [], {}, {"on": "p"}],
    ["copyrows", [PLACEHOLDER + ".out"], {}],
    ["open", [PLACEHOLDER + ".out"], {}],
    ["nrows", [], {}],
    *[["getcell", [column, row], {}] for row in range(3) for column in CELL_COLUMNS],
    ["open", [PLACEHOLDER], {"nomodify": False}],
    ["copyrows", [PLACEHOLDER + ".out"], {}],
    ["copyrows", [PLACEHOLDER + ".out", 1, 0, 2], {}],
    ["copyrows", [PLACEHOLDER + ".out", 2], {}],
    ["copyrows", [PLACEHOLDER + ".out"], {"startrowin": 1, "nrow": 1}],
    ["copyrows", [PLACEHOLDER + ".out", 0, 100, 1], {}],
    ["copyrows", [PLACEHOLDER + ".out", 100], {}],
    ["copyrows", [PLACEHOLDER + ".out", 0, -1, 100], {}],
    ["copyrows", [PLACEHOLDER + ".out", -1], {}],
    ["copyrows", [PLACEHOLDER + ".out", 0, 0, 0], {}],
    ["copyrows", [PLACEHOLDER + ".nosuch"], {}],
    ["open", [PLACEHOLDER + ".out"], {}],
    ["nrows", [], {}],
    ["getcol", ["label"], {}],
    ["getcol", ["gain"], {}],
    *[["getcell", [column, row], {}] for row in range(10) for column in ("label", "gain", "id")],
    ["getcell", ["spectrum", 7], {}],
    ["getcell", ["meta", 8], {}],
    ["iscelldefined", ["vary", 9], {}],
    # A table with other columns takes the ones it shares.
    ["open", [PLACEHOLDER + ".other"], {"nomodify": False}],
    ["removecols", [["gain", "meta"]], {}],
    ["renamecol", ["label", "name"], {}],
    ["open", [PLACEHOLDER], {}],
    ["copyrows", [PLACEHOLDER + ".other"], {}],
    ["open", [PLACEHOLDER + ".other"], {}],
    ["nrows", [], {}],
    ["colnames", [], {}],
    ["getcol", ["gain"], {}],
    ["getcell", ["spectrum", 2], {}],
    # A table to itself: the rows are appended.
    ["open", [PLACEHOLDER], {"nomodify": False}],
    ["copyrows", [PLACEHOLDER], {}],
    ["nrows", [], {}],
    ["getcell", ["label", 4], {}],
    ["copyrows", [PLACEHOLDER, 1, 1, 1], {}],
    ["getcell", ["label", 1], {}],
    ["close", [], {}],
    ["copyrows", [PLACEHOLDER + ".out"], {}],
]

#: `copyrows` of a table onto itself when the rows written include rows still
#: to be read (casacore copies row by row), and when they do not: each case on
#: a fresh copy of the table.
COPYROWS_ORDER = [
    step
    for number, (start_in, start_out, count) in enumerate(
        [(0, 1, 2), (0, 1, 3), (1, 0, 2), (0, -1, 2), (0, 0, 3), (1, 2, 1), (0, 2, 3), (0, 3, 2)]
    )
    for step in [
        ["open", [PLACEHOLDER], {}],
        ["copy", [PLACEHOLDER + f".r{number}"], {}, {"bind": f"r{number}"}],
        ["close", [], {}, {"on": f"r{number}"}],
        ["open", [PLACEHOLDER + f".r{number}"], {"nomodify": False}],
        ["copyrows", [PLACEHOLDER + f".r{number}", start_in, start_out, count], {}],
        ["nrows", [], {}],
        *[["getcell", [column, row], {}] for row in range(8) for column in ("id", "label")],
        ["close", [], {}],
    ]
]

#: The tool of a copy onto the table's own name, and `copyrows` of a table
#: opened `nomodify` onto itself (CASA opens the output for update).
SELF_COPIES = [
    ["open", [PLACEHOLDER], {}],
    ["iswritable", [], {}],
    ["copy", [PLACEHOLDER], {}, {"bind": "ro"}],
    *inspect("ro"),
    ["close", [], {}, {"on": "ro"}],
    ["copyrows", [PLACEHOLDER], {}],
    ["nrows", [], {}],
    ["iswritable", [], {}],
    ["getcell", ["id", 4], {}],
    ["open", [PLACEHOLDER], {"nomodify": False}],
    ["copy", [PLACEHOLDER], {}, {"bind": "rw"}],
    *inspect("rw"),
    ["close", [], {}, {"on": "rw"}],
    ["open", [PLACEHOLDER], {"nomodify": False, "lockoptions": {"option": "user"}}],
    ["copy", [PLACEHOLDER], {"deep": True}, {"bind": "us"}],
    *inspect("us"),
    ["close", [], {}, {"on": "us"}],
    ["open", [PLACEHOLDER], {"nomodify": False, "lockoptions": {"option": "usernoread", "maxwait": 7}}],
    ["copy", [PLACEHOLDER], {}, {"bind": "un"}],
    *on("un", [["lockoptions", [], {}], ["iswritable", [], {}]]),
    ["close", [], {}, {"on": "un"}],
    ["close", [], {}],
]

#: A table that keeps its write lock announces what it has written when it
#: is flushed, once; the lock is kept.
FLUSH_PUBLISHES = [
    step
    for option in ("user", "permanent")
    for step in [
        ["open", [PLACEHOLDER], {"nomodify": False, "lockoptions": {"option": option}}],
        *([["lock", [], {"write": True}]] if option == "user" else []),
        ["datachanged", [], {}],
        ["datachanged", [], {}],
        ["putcell", ["id", 0, 11], {}],
        ["datachanged", [], {}],
        ["flush", [], {}],
        ["haslock", [], {"write": True}],
        ["datachanged", [], {}],
        ["datachanged", [], {}],
        ["flush", [], {}],
        ["datachanged", [], {}],
        ["putcell", ["id", 1, 12], {}],
        ["flush", [], {}],
        ["flush", [], {}],
        ["datachanged", [], {}],
        ["datachanged", [], {}],
        ["getcell", ["id", 1], {}],
        ["close", [], {}],
    ]
]

#: `clearlocks` releases the automatic locks of every table of the process,
#: and leaves a lock the user took.
CLEARLOCKS = [
    ["open", [PLACEHOLDER], {"nomodify": False, "lockoptions": {"option": "user"}}],
    ["lock", [], {"write": False}],
    ["copy", [PLACEHOLDER + ".k1"], {}, {"bind": "a"}],
    ["copy", [PLACEHOLDER + ".k2"], {}, {"bind": "b"}],
    ["getcell", ["id", 0], {}, {"on": "a"}],
    ["getcell", ["id", 0], {}, {"on": "b"}],
    ["haslock", [], {"write": False}],
    ["haslock", [], {"write": False}, {"on": "a"}],
    ["haslock", [], {"write": False}, {"on": "b"}],
    ["clearlocks", [], {}, {"on": "a"}],
    ["haslock", [], {"write": False}],
    ["haslock", [], {"write": False}, {"on": "a"}],
    ["haslock", [], {"write": False}, {"on": "b"}],
    ["getcell", ["id", 1], {}, {"on": "b"}],
    ["haslock", [], {"write": False}, {"on": "b"}],
    ["clearlocks", [], {}],
    ["haslock", [], {"write": False}],
    ["haslock", [], {"write": False}, {"on": "b"}],
    ["close", [], {}, {"on": "a"}],
    ["close", [], {}, {"on": "b"}],
    ["close", [], {}],
]

#: `copyrows` between columns of the same name and different types: the
#: output has its column made again with the type given, the input is the
#: python fixture table (`id` is an Int64, `gain` a DComplex, `label` a
#: string, `spectrum` a fixed 2x2 Float array, `vary` a variable Double array).
#: The last field says whether CASA accepts the conversion. After a refusal
#: CASA has added the rows and left their cells unset (garbage), which is not
#: compared: casa-rs refuses before it changes anything.
COPYROWS_TYPES_CASES = [
    ("id", {"valueType": "double"}, True),
    ("id", {"valueType": "int"}, True),
    ("id", {"valueType": "uint"}, True),
    ("id", {"valueType": "short"}, False),
    ("id", {"valueType": "boolean"}, False),
    ("id", {"valueType": "string"}, False),
    ("id", {"valueType": "complex"}, True),
    ("gain", {"valueType": "complex"}, True),
    ("gain", {"valueType": "double"}, False),
    ("label", {"valueType": "int"}, False),
    ("spectrum", {"valueType": "double", "ndim": 2, "shape": [2, 2]}, True),
    ("spectrum", {"valueType": "dcomplex", "ndim": 2, "shape": [2, 2]}, True),
    ("spectrum", {"valueType": "int", "ndim": 2, "shape": [2, 2]}, False),
    ("vary", {"valueType": "float", "ndim": 1}, True),
    ("vary", {"valueType": "int", "ndim": 1}, False),
    ("vary", {"valueType": "complex", "ndim": 1}, True),
]

COPYROWS_TYPES = [
    step
    for number, (column, description, accepted) in enumerate(COPYROWS_TYPES_CASES)
    for step in [
        ["open", [PLACEHOLDER], {}],
        ["copy", [PLACEHOLDER + f".t{number}"], {"norows": True}, {"bind": f"t{number}"}],
        ["close", [], {}, {"on": f"t{number}"}],
        ["open", [PLACEHOLDER + f".t{number}"], {"nomodify": False}],
        ["removecols", [[column]], {}],
        ["addcols", [{column: description}], {}],
        ["open", [PLACEHOLDER], {}],
        ["copyrows", [PLACEHOLDER + f".t{number}"], {}],
        *(
            [
                ["open", [PLACEHOLDER + f".t{number}"], {}],
                ["nrows", [], {}],
                ["getcoldesc", [column], {}],
                *[["getcell", [column, row], {}] for row in range(3)],
            ]
            if accepted
            else []
        ),
        ["close", [], {}],
    ]
]

#: The scenario the MeasurementSet gets: for each column of the main table,
#: its description and, for the first rows, its cells. Built from the table
#: itself, so a column is probed whatever the fixture holds.
MS_ROWS = 3


def ms_steps(source):
    from casatools import table  # CASA's Python only: `encode` is imported without it

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


def ms_coldesc_steps(source):
    """`getcoldesc` of every column of the MeasurementSet's main table: every
    value type it holds, and the storage manager (type and group) of each."""
    from casatools import table

    probe = table()
    probe.open(str(source))
    columns = list(probe.colnames())
    probe.done()
    steps = [["open", [PLACEHOLDER], {}]]
    steps += [["getcoldesc", [column], {}] for column in columns]
    return steps + [["close", [], {}]]


#: Cell and column writes on the MeasurementSet's main table, whose columns
#: are of int, double, bool, float and complex-float types.
MS_CELL_WRITES = [
    ["open", [PLACEHOLDER], {"nomodify": False}],
    ["putcell", ["ANTENNA1", 0, 5], {}],
    ["putcell", ["ANTENNA1", 1, np.int64(6)], {}],
    ["putcell", ["ANTENNA1", 2, 7.0], {}],
    ["getcol", ["ANTENNA1"], {"nrow": 3}],
    ["putcell", ["TIME", 1, 1.5e9], {}],
    ["putcell", ["TIME", 2, np.float32(2.5)], {}],
    ["putcell", ["TIME", 0, 5], {}],
    ["getcol", ["TIME"], {"nrow": 3}],
    ["putcell", ["FLAG_ROW", 0, False], {}],
    ["putcell", ["FLAG_ROW", 1, 0], {}],
    ["getcol", ["FLAG_ROW"], {"nrow": 3}],
    ["putcell", ["WEIGHT", 0, np.array([1.0, 2.0, 3.0, 4.0])], {}],
    ["putcell", ["WEIGHT", 1, np.array([1.0, 2.0])], {}],
    ["getcell", ["WEIGHT", 0], {}],
    ["getcell", ["WEIGHT", 1], {}],
    ["putcell", ["UVW", 0, np.array([1.0, 2.0, 3.0])], {}],
    ["putcell", ["UVW", 1, np.array([1.0, 2.0])], {}],
    ["getcell", ["UVW", 0], {}],
    ["putcell", ["DATA", 0, np.ones((4, 64), dtype=np.complex128) * (1 + 2j)], {}],
    ["putcell", ["DATA", 1, np.ones((4, 64))], {}],
    ["putcell", ["DATA", 2, np.ones((4, 63), dtype=np.complex128)], {}],
    ["getcell", ["DATA", 0], {}],
    ["getcell", ["DATA", 1], {}],
    ["putcell", ["FLAG", 0, np.zeros((4, 64), dtype=bool)], {}],
    ["putcell", ["FLAG", 1, np.zeros((4, 64), dtype=np.int32)], {}],
    ["getcell", ["FLAG", 0], {}],
    ["putcol", ["ANTENNA1", np.array([1, 2, 3])], {"startrow": 0, "nrow": 3}],
    ["getcol", ["ANTENNA1"], {"nrow": 3}],
    ["putcol", ["TIME", [1.0, 2.0, 3.0]], {"startrow": 0, "nrow": 3}],
    ["getcol", ["TIME"], {"nrow": 3}],
    ["putcol", ["WEIGHT", np.ones((4, 3))], {"startrow": 0, "nrow": 3}],
    ["getcol", ["WEIGHT"], {"nrow": 3}],
    ["close", [], {}],
    ["open", [PLACEHOLDER], {}],
    ["getcol", ["ANTENNA1"], {"nrow": 3}],
    ["getcol", ["TIME"], {"nrow": 3}],
    ["getcell", ["DATA", 0], {}],
    ["getcell", ["FLAG", 1], {}],
    ["getcell", ["UVW", 0], {}],
    ["getcol", ["WEIGHT"], {"nrow": 3}],
    ["nrows", [], {}],
    ["close", [], {}],
]


#: What Astra's review of #1034 asked to pin down, each its own scenario so
#: that one which aborts CASA costs only itself (record with `--only`).
#: `fixture` is the table the scenario starts from.
REVIEW_SCENARIOS = {
    # putvarcol writes the record's fields, in order, to the selected rows;
    # the field names are labels, not row numbers.
    "varcol_selection": ("table", [
        ["open", [PLACEHOLDER], {"nomodify": False}],
        ["getvarcol", ["vary"], {}],
        ["putvarcol", ["vary", {"r1": np.array([[99.0]])}], {"startrow": 2, "nrow": 1}],
        ["getvarcol", ["vary"], {}],
        ["putvarcol", ["vary", {"z": np.array([[88.0]])}], {"startrow": 1, "nrow": 1}],
        ["getvarcol", ["vary"], {}],
        ["putvarcol", ["vary", {"a": np.array([[1.0], [2.0]]), "b": np.array([[3.0]])}],
         {"startrow": 0, "nrow": 2, "rowincr": 2}],
        ["getvarcol", ["vary"], {}],
        ["putvarcol", ["vary", {"only": np.array([[7.0]])}], {}],
        ["putvarcol", ["vary", {"a": np.array([[1.0]]), "b": np.array([[2.0]]),
                               "c": np.array([[3.0]]), "d": np.array([[4.0]])}], {}],
        ["putvarcol", ["vary", {"x": np.array([[5.0]])}], {"startrow": 3, "nrow": 1}],
        ["putvarcol", ["vary", {"x": np.array([[5.0]]), "y": np.array([[6.0]])}],
         {"startrow": 1, "nrow": -1}],
        ["getvarcol", ["vary"], {}],
        ["putvarcol", ["vary", {"x": np.array([[5.0]])}], {"startrow": 0, "nrow": 1, "rowincr": 0}],
        ["putvarcol", ["vary", {}], {"startrow": 0, "nrow": 0}],
        ["getvarcol", ["vary"], {}],
        ["close", [], {}],
        ["open", [PLACEHOLDER], {}],
        ["getvarcol", ["vary"], {}],
        ["close", [], {}],
    ]),
    # putcell takes one row number or a vector of them.
    # (`id` is read cell by cell: CASA's getcol refuses its int64 column.)
    "rows_vector": ("table", [
        ["open", [PLACEHOLDER], {"nomodify": False}],
        ["putcell", ["id", [1, 2], 77], {}],
        *[["getcell", ["id", row], {}] for row in range(3)],
        ["putcell", ["id", np.array([0, 2]), 55], {}],
        *[["getcell", ["id", row], {}] for row in range(3)],
        ["putcell", ["id", [], 33], {}],
        *[["getcell", ["id", row], {}] for row in range(3)],
        # CASA writes the rows before the missing one and then fails; casa-rs
        # writes none, so row 0 is put back to what it was before reading.
        ["putcell", ["id", [0, 3], 22], {}],
        ["putcell", ["id", [0], 55], {}],
        *[["getcell", ["id", row], {}] for row in range(3)],
        ["putcell", ["id", [2, 0], 11], {}],
        *[["getcell", ["id", row], {}] for row in range(3)],
        ["putcell", ["label", [0, 2], "same"], {}],
        ["getcol", ["label"], {}],
        ["putcell", ["spectrum", [0, 2], np.ones((2, 2))], {}],
        ["getcol", ["spectrum"], {}],
        ["putcell", ["spectrum", [0, 1], np.ones((2, 3))], {}],
        ["getcol", ["spectrum"], {}],
        ["close", [], {}],
        ["open", [PLACEHOLDER], {}],
        *[["getcell", ["id", row], {}] for row in range(3)],
        ["getcol", ["label"], {}],
        ["close", [], {}],
    ]),
    # A call that fails part way has done part of its work in CASA:
    # `removerows([2, 2])` removes the first row 2 and fails on the second
    # (two rows are left); `addcols` adds the columns before the one that
    # exists (`aaa` sorts before `id`) and fails there. casa-rs checks the
    # whole call first and changes nothing.
    "structure_partial": ("table", [
        ["open", [PLACEHOLDER], {"nomodify": False}],
        ["nrows", [], {}],
        ["removerows", [[2, 2]], {}],
        ["nrows", [], {}],
        ["addcols", [{"aaa": {"valueType": "int"}, "id": {"valueType": "int"}}], {}],
        ["colnames", [], {}],
        ["close", [], {}],
    ]),
    # addrows with a negative count takes the count as a huge unsigned number
    # and the row count wraps: addrows(-1) removes a row. casa-rs refuses.
    "addrows_negative": ("table", [
        ["open", [PLACEHOLDER], {"nomodify": False}],
        ["nrows", [], {}],
        ["addrows", [-1], {}],
        ["nrows", [], {}],
        ["getcell", ["id", 0], {}],
        ["getcell", ["label", 1], {}],
        ["addrows", [-2], {}],
        ["nrows", [], {}],
        ["close", [], {}],
        ["open", [PLACEHOLDER], {}],
        ["nrows", [], {}],
        ["getcell", ["id", 0], {}],
        ["close", [], {}],
    ]),
    # A floating value written to a Bool column is true when it is not zero;
    # an integer beyond 32 bits written to an int32 cell wraps.
    "bool_coercion": ("ms", [
        ["open", [PLACEHOLDER], {"nomodify": False}],
        ["putcell", ["FLAG_ROW", 0, 1.5], {}],
        ["putcell", ["FLAG_ROW", 1, 0.0], {}],
        ["putcell", ["FLAG_ROW", 2, np.float32(2.0)], {}],
        ["getcol", ["FLAG_ROW"], {"nrow": 3}],
        ["putcell", ["FLAG_ROW", 0, -0.5], {}],
        ["putcell", ["FLAG_ROW", 1, 1 + 1j], {}],
        ["putcell", ["FLAG_ROW", 2, "text"], {}],
        ["getcol", ["FLAG_ROW"], {"nrow": 3}],
        ["putcell", ["FLAG", 0, np.full((4, 64), 0.5)], {}],
        ["putcell", ["FLAG", 1, np.zeros((4, 64))], {}],
        ["getcell", ["FLAG", 0], {}],
        ["getcell", ["FLAG", 1], {}],
        ["putcol", ["FLAG_ROW", np.array([1.5, 0.0, -1.0])], {"startrow": 0, "nrow": 3}],
        ["getcol", ["FLAG_ROW"], {"nrow": 3}],
        ["putcol", ["FLAG_ROW", [0.0, 2.5, 0.0]], {"startrow": 0, "nrow": 3}],
        ["getcol", ["FLAG_ROW"], {"nrow": 3}],
        ["close", [], {}],
    ]),
    # CASA's own fault, which casa-rs does not follow (docs/CASA (C++)
    # bugs.md): an integer beyond 32 bits written to an int32 cell wraps.
    "int_overflow": ("ms", [
        ["open", [PLACEHOLDER], {"nomodify": False}],
        ["putcell", ["ANTENNA1", 0, 2147483648], {}],
        ["putcell", ["ANTENNA1", 1, -2147483649], {}],
        ["putcell", ["ANTENNA1", 2, 4294967297], {}],
        ["getcol", ["ANTENNA1"], {"nrow": 3}],
        ["putcol", ["ANTENNA1", np.array([2**31, 1, 2])], {"startrow": 0, "nrow": 3}],
        ["getcol", ["ANTENNA1"], {"nrow": 3}],
        ["close", [], {}],
    ]),
    # A selection with no rows, and a selection checked before the fixed
    # shape answers.
    "empty_selection": ("table", [
        ["open", [PLACEHOLDER], {}],
        ["getcol", ["label"], {"startrow": 3, "nrow": 0}],
        ["getcol", ["label"], {"startrow": 3}],
        ["getcol", ["label"], {"startrow": 2, "nrow": 0}],
        ["getcol", ["label"], {"startrow": 4}],
        ["getcol", ["label"], {"startrow": 5, "nrow": 0}],
        ["getcol", ["label"], {"startrow": 1, "nrow": 0}],
        ["getcol", ["spectrum"], {"startrow": 3, "nrow": 0}],
        ["getcol", ["spectrum"], {"startrow": 3}],
        ["getcol", ["spectrum"], {"startrow": 4}],
        ["getcol", ["gain"], {"startrow": 3, "nrow": 0}],
        ["getcol", ["gain"], {"startrow": 3}],
        ["getcolslice", ["spectrum", [0, 0], [0, 0], [1, 1]], {"nrow": 0}],
        ["getcolslice", ["spectrum", [0, 0], [0, 0], [1, 1]], {"startrow": 3}],
        ["getcolslice", ["spectrum", [0, 0], [0, 0], [1, 1]], {"startrow": 3, "nrow": 0}],
        ["getcolshapestring", ["spectrum"], {"rowincr": 0}],
        ["getcolshapestring", ["spectrum"], {"startrow": 3}],
        ["getcolshapestring", ["spectrum"], {"startrow": 7, "nrow": 2}],
        ["getcolshapestring", ["vary"], {"startrow": 3}],
        ["getcolshapestring", ["vary"], {"rowincr": 0}],
        ["getcol", ["label"], {"rowincr": -1}],
        ["getvarcol", ["vary"], {"startrow": 3}],
        ["getvarcol", ["vary"], {"startrow": 3, "nrow": 0}],
        ["close", [], {}],
    ]),
    # What a keyword takes as its type from the NumPy value given: CASA
    # promotes a 32-bit float to double and a 64-bit complex to double
    # complex, so a later update by a 64-bit value is of the same type.
    "keyword_types": ("table", [
        ["open", [PLACEHOLDER], {"nomodify": False}],
        ["putkeyword", ["f32", np.float32(1.25)], {}],
        ["getkeyword", ["f32"], {}],
        ["putkeyword", ["f32", np.float32(2.5)], {}],
        ["putkeyword", ["f32", 4.5], {}],
        ["getkeyword", ["f32"], {}],
        ["putkeyword", ["f32arr", np.array([1, 2], dtype=np.float32)], {}],
        ["getkeyword", ["f32arr"], {}],
        ["putkeyword", ["f32arr", np.array([3.0, 4.0, 5.0])], {}],
        ["getkeyword", ["f32arr"], {}],
        ["putkeyword", ["c64arr", np.array([1 + 1j], dtype=np.complex64)], {}],
        ["getkeyword", ["c64arr"], {}],
        ["putkeyword", ["c64arr", np.array([2 + 2j, 3 + 3j])], {}],
        ["getkeyword", ["c64arr"], {}],
        ["putkeyword", ["i16", np.int16(5)], {}],
        ["getkeyword", ["i16"], {}],
        ["putkeyword", ["i64", np.int64(7)], {}],
        ["getkeyword", ["i64"], {}],
        ["putkeyword", ["i64", 8], {}],
        ["putkeyword", ["i64", 8.5], {}],
        ["putkeyword", ["f32", 3], {}],
        ["putkeyword", ["version", np.float64(2.0)], {}],
        ["getkeywords", [], {}],
        ["close", [], {}],
        ["open", [PLACEHOLDER], {}],
        ["getkeywords", [], {}],
        ["close", [], {}],
    ]),
    # A string that names a readable table is stored as a table reference.
    "table_keywords": ("ms", [
        ["open", [PLACEHOLDER], {"nomodify": False}],
        ["getkeyword", ["ANTENNA"], {}],
        ["putkeyword", ["ANTENNA", "Table: <TABLE>/ANTENNA"], {}],
        ["putkeyword", ["ANTENNA", "Table: <TABLE>/FIELD"], {}],
        ["getkeyword", ["ANTENNA"], {}],
        ["putkeyword", ["NEWREF", "Table: <TABLE>/FIELD"], {}],
        ["getkeyword", ["NEWREF"], {}],
        ["putkeyword", ["NEWSTR", "Table: /no/such/table"], {}],
        ["getkeyword", ["NEWSTR"], {}],
        ["putkeyword", ["NEWSTR", "Table: <TABLE>/FIELD"], {}],
        ["putkeyword", ["ANTENNA", "plain text"], {}],
        ["putkeyword", ["FIELD", 5], {}],
        ["putkeywords", [{"REC": {"INNER": "Table: <TABLE>/SPECTRAL_WINDOW", "TXT": "t"}}], {}],
        ["getkeyword", ["REC"], {}],
        ["putkeyword", ["REC.INNER", "Table: <TABLE>/ANTENNA"], {}],
        ["getkeyword", ["REC.INNER"], {}],
        ["close", [], {}],
        ["open", [PLACEHOLDER], {}],
        ["getkeyword", ["ANTENNA"], {}],
        ["getkeyword", ["NEWREF"], {}],
        ["getkeyword", ["NEWSTR"], {}],
        ["getkeyword", ["REC"], {}],
        ["close", [], {}],
    ]),
    # What a read leaves held, under each locking mode, and what a read-only
    # open takes however it locks.
    "read_locks": ("table", [
        *[
            step
            for option in ["default", "auto", "autonoread", "user", "usernoread", "permanent"]
            for step in [
                ["open", [PLACEHOLDER], {"lockoptions": {"option": option}}],
                ["haslock", [], {}],
                ["haslock", [], {"write": False}],
                ["nrows", [], {}],
                ["haslock", [], {}],
                ["haslock", [], {"write": False}],
                ["getkeyword", ["observer"], {}],
                ["haslock", [], {}],
                ["haslock", [], {"write": False}],
                ["getcell", ["id", 0], {}],
                ["haslock", [], {}],
                ["haslock", [], {"write": False}],
                ["unlock", [], {}],
                ["haslock", [], {}],
                ["haslock", [], {"write": False}],
                ["getcell", ["id", 1], {}],
                ["haslock", [], {}],
                ["haslock", [], {"write": False}],
                ["close", [], {}],
            ]
        ],
        *[
            step
            for option in ["permanent", "permanentwait", "user", "auto"]
            for step in [
                ["open", [PLACEHOLDER], {"lockoptions": {"option": option}, "nomodify": True}],
                ["haslock", [], {}],
                ["haslock", [], {"write": False}],
                ["getcell", ["id", 0], {}],
                ["haslock", [], {}],
                ["haslock", [], {"write": False}],
                ["close", [], {}],
            ]
        ],
    ]),
}


def realise(value, path):
    """`value` with the placeholder replaced by `path` in every string in it."""
    if isinstance(value, str):
        return value.replace(PLACEHOLDER, path)
    if isinstance(value, dict):
        return {key: realise(item, path) for key, item in value.items()}
    if isinstance(value, list):
        return [realise(item, path) for item in value]
    return value


def run_scenario(steps, source):
    work = Path(tempfile.mkdtemp(prefix="tb_oracle_"))
    path = str(work / Path(source).name)
    shutil.copytree(source, path)
    from casatools import table

    tool = table()
    tools = {"": tool}
    record = []
    for method, args, keywords, *rest in steps:
        options = rest[0] if rest else {}
        real_args = [realise(arg, path) for arg in args]
        outcome = {
            "method": method,
            "args": [encode(arg, path) for arg in args],
            "kwargs": {name: encode(value, path) for name, value in keywords.items()},
        }
        if options:
            outcome["options"] = options
        try:
            value = getattr(tools[options.get("on", "")], method)(*real_args, **keywords)
            outcome["value"] = encode(value, path)
            if "bind" in options:
                tools[options["bind"]] = value
        except Exception as error:  # noqa: BLE001 - recording whatever CASA raises
            outcome["error"] = {
                "type": type(error).__name__,
                "message": str(error).replace(path, PLACEHOLDER),
            }
        record.append(outcome)
    try:
        for bound in tools.values():
            bound.done()
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return record


def dump(record):
    """The recording as JSON with one step per line, which a reviewer can read
    and `git diff` can follow."""
    names = list(record["scenarios"])
    lines = [
        "{",
        f' "casa_version": {json.dumps(record["casa_version"])},',
        ' "scenarios": {',
    ]
    for index, name in enumerate(names):
        scenario = record["scenarios"][name]
        lines.append(
            f'  {json.dumps(name)}: {{"fixture": {json.dumps(scenario["fixture"])}, "steps": ['
        )
        steps = scenario["steps"]
        for position, step in enumerate(steps):
            comma = "," if position < len(steps) - 1 else ""
            lines.append("   " + json.dumps(step, separators=(",", ":")) + comma)
        lines.append("  ]}" + ("," if index < len(names) - 1 else ""))
    lines += [" }", "}"]
    return "\n".join(lines) + "\n"


def main():
    import casatools

    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("fixture", type=Path)
    parser.add_argument("ms", type=Path)
    parser.add_argument("--json", type=Path, required=True)
    parser.add_argument(
        "--only",
        help="comma-separated scenario names: record just these, merged into --json if it exists "
        "(a scenario that crashes CASA then costs only itself)",
    )
    arguments = parser.parse_args()
    scenarios = {
        name: {"fixture": "table", "steps": steps, "source": arguments.fixture}
        for name, steps in SCENARIOS.items()
    }
    scenarios["cells_write"] = {"fixture": "table", "steps": CELL_WRITES, "source": arguments.fixture}
    scenarios["ms"] = {"fixture": "ms", "steps": ms_steps(arguments.ms), "source": arguments.ms}
    scenarios["ms_coldesc"] = {"fixture": "ms", "steps": ms_coldesc_steps(arguments.ms), "source": arguments.ms}
    scenarios["ms_cells_write"] = {"fixture": "ms", "steps": MS_CELL_WRITES, "source": arguments.ms}
    scenarios["slices"] = {"fixture": "table", "steps": SLICES, "source": arguments.fixture}
    scenarios["ms_slices"] = {"fixture": "ms", "steps": MS_SLICES, "source": arguments.ms}
    scenarios["addcols_descriptions"] = {
        "fixture": "table",
        "steps": ADDCOLS_DESCRIPTIONS,
        "source": arguments.fixture,
    }
    scenarios["rows"] = {"fixture": "table", "steps": ROWS, "source": arguments.fixture}
    scenarios["columns"] = {"fixture": "table", "steps": COLUMNS, "source": arguments.fixture}
    scenarios["rows_columns"] = {"fixture": "table", "steps": ROWS_COLUMNS, "source": arguments.fixture}
    scenarios["state"] = {"fixture": "table", "steps": STATE, "source": arguments.fixture}
    scenarios["state_ms"] = {"fixture": "ms", "steps": STATE_MS, "source": arguments.ms}
    scenarios["copies"] = {"fixture": "table", "steps": COPIES, "source": arguments.fixture}
    scenarios["copyrows"] = {"fixture": "table", "steps": COPYROWS, "source": arguments.fixture}
    scenarios["copyrows_order"] = {"fixture": "table", "steps": COPYROWS_ORDER, "source": arguments.fixture}
    scenarios["self_copies"] = {"fixture": "table", "steps": SELF_COPIES, "source": arguments.fixture}
    scenarios["flush_publishes"] = {"fixture": "table", "steps": FLUSH_PUBLISHES, "source": arguments.fixture}
    scenarios["clearlocks"] = {"fixture": "table", "steps": CLEARLOCKS, "source": arguments.fixture}
    scenarios["copyrows_types"] = {"fixture": "table", "steps": COPYROWS_TYPES, "source": arguments.fixture}
    for name, (fixture, steps) in REVIEW_SCENARIOS.items():
        source = arguments.ms if fixture == "ms" else arguments.fixture
        scenarios[name] = {"fixture": fixture, "steps": steps, "source": source}
    names = arguments.only.split(",") if arguments.only else list(scenarios)
    if arguments.only and arguments.json.exists():
        record = json.loads(arguments.json.read_text(encoding="utf-8"))
    else:
        record = {"scenarios": {}}
    record["casa_version"] = ".".join(str(part) for part in casatools.version()[:4])
    for name in names:
        scenario = scenarios[name]
        record["scenarios"][name] = {
            "fixture": scenario["fixture"],
            "steps": run_scenario(scenario["steps"], scenario["source"]),
        }
    arguments.json.write_text(dump(record), encoding="utf-8")


if __name__ == "__main__":
    main()
