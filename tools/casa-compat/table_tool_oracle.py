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


def run_scenario(steps, source):
    work = Path(tempfile.mkdtemp(prefix="tb_oracle_"))
    path = str(work / Path(source).name)
    shutil.copytree(source, path)
    from casatools import table

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
    scenarios["ms_cells_write"] = {"fixture": "ms", "steps": MS_CELL_WRITES, "source": arguments.ms}
    scenarios["slices"] = {"fixture": "table", "steps": SLICES, "source": arguments.fixture}
    scenarios["ms_slices"] = {"fixture": "ms", "steps": MS_SLICES, "source": arguments.ms}
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
    arguments.json.write_text(json.dumps(record, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
