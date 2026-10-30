#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Interoperability checks for what `casars.tools.table` writes (#1025, #1028).

casa-rs writes a table; CASA 6.7.6.14 must open it and read what casa-rs
reads, and the other way round. Subcommands, split by which Python runs them
(`casars` is imported only by the first group, `casatools` by the second):

With the casa-rs package (a venv with `pip install -e crates/casars-python`):

`write FIXTURE OUT_TABLE OUT_JSON`
    Copies the python fixture table to OUT_TABLE, replays on it the writes
    recorded in `casa_table_tool.json` (`keywords_write`,
    `column_keywords_write`, `info_write`) through `casars.tools.table`, and
    writes what casa-rs reads back (keyword names, keywords, info and every
    column's keywords) to OUT_JSON.

`variants FIXTURE OUT_DIR`
    One table per single write, `OUT_DIR/v_<name>.table`: a keyword of each
    type (a Boolean array was the one CASA refused until #1025), column
    keywords, the info, removals.

`rsread TABLE OUT_JSON`
    What casa-rs reads of any table (the same dump `write` ends with), for a
    table CASA wrote.

`compare RS_JSON CASA_JSON`
    Prints the records on which the two dumps differ.

With CASA's Python (`~/ext/DeveloperTools/CASA.app/Contents/MacOS/python3 -I`
on the Mac mini; copy this file and `table_tool_oracle.py` side by side):

`read TABLE OUT_JSON`
    What CASA reads of a casa-rs-written table, in the same form.

`opens DIR`
    Which of the `v_*.table` tables CASA opens and reads (`OK`/`FAIL`).

`write-bools TABLE`
    CASA writes Boolean-array keywords into TABLE (a copy of the fixture);
    then `read` it here and `rsread` it with casa-rs, and `compare`.

A passing run: `write` and `read` give dumps `compare` finds no difference
in, `opens` reports every variant `OK`, and `write-bools` then `rsread` and
`read` give dumps `compare` finds no difference in.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from table_tool_oracle import encode  # noqa: E402

RECORD = Path(__file__).resolve().parents[2] / "crates/casars-python/python/tests"


def dump(tool, path):
    """What the tool reads of the table, encoded as the oracle encodes."""
    result = {
        "keywordnames": tool.keywordnames(),
        "keywords": tool.getkeywords(),
        "info": tool.info(),
    }
    for column in tool.colnames():
        result[f"colkeywords:{column}"] = tool.getcolkeywords(column)
    return {key: encode(value, path) for key, value in result.items()}


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def command_write(fixture, out_table, out_json):
    sys.path.insert(0, str(RECORD))
    import test_tools_table as replay  # noqa: PLC0415
    from casars import tools  # noqa: PLC0415

    out_table = Path(out_table)
    shutil.rmtree(out_table, ignore_errors=True)
    shutil.copytree(fixture, out_table)
    path = str(out_table.resolve())
    tool = tools.table()
    for scenario in ("keywords_write", "column_keywords_write", "info_write"):
        for step in replay.RECORD["scenarios"][scenario]["steps"]:
            replay.run_step(tool, step, path)
    tool.done()
    reader = tools.table()
    reader.open(path)
    write_json(out_json, dump(reader, path))
    reader.done()


def command_rsread(table_path, out_json):
    from casars import tools  # noqa: PLC0415

    path = str(Path(table_path).resolve())
    tool = tools.table()
    tool.open(path)
    write_json(out_json, dump(tool, path))
    tool.done()


def command_variants(fixture, out_dir):
    import numpy as np  # noqa: PLC0415
    from casars import tools  # noqa: PLC0415

    variants = {
        "none": [],
        "int": [("putkeyword", ["newint", 7])],
        "bigint": [("putkeyword", ["newbig", 2**40])],
        "float": [("putkeyword", ["newfloat", 1.5])],
        "bool": [("putkeyword", ["newbool", True])],
        "str": [("putkeyword", ["newstr", "text"])],
        "complex": [("putkeyword", ["newcomplex", 1 + 2j])],
        "arr_int": [("putkeyword", ["arr_int", np.array([1, 2, 3])])],
        "arr_float2d": [("putkeyword", ["arr_float", np.arange(6.0).reshape(2, 3)])],
        "arr_str": [("putkeyword", ["arr_str", np.array(["a", "bc"])])],
        "arr_bool": [("putkeyword", ["arr_bool", np.array([True, False])])],
        "arr_bool_9": [("putkeyword", ["arr_bool9", np.arange(9) % 2 == 0])],
        "arr_complex": [("putkeyword", ["arr_complex", np.array([1 + 1j, 2 - 2j])])],
        "record": [("putkeyword", ["rec", {"a": 1, "b": "two", "c": {"d": 3.5}}])],
        "overwrite_int": [("putkeyword", ["version", 2])],
        "remove": [("removekeyword", ["version"])],
        "colkw_str": [("putcolkeyword", ["spectrum", "unit", "mJy"])],
        "colkw_new_on_plain": [("putcolkeyword", ["id", "newkw", 3])],
        "colkw_array": [("putcolkeyword", ["id", "arr", np.array([1.5, 2.5])])],
        "colkw_bools": [("putcolkeyword", ["id", "flags", np.array([True, False, True])])],
        "colkw_record": [("putcolkeyword", ["id", "sub", {"p": 1, "q": "r"}])],
        "colkw_remove": [("removecolkeyword", ["spectrum", "frame"])],
        "info_type": [("putinfo", [{"type": "MyType", "subType": "sub"}])],
        "info_readme": [("putinfo", [{"readme": "line1\nline2\n"}])],
        "readmeline": [("addreadmeline", ["line3"])],
    }
    out_dir = Path(out_dir)
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    for name, calls in variants.items():
        table = out_dir / f"v_{name}.table"
        shutil.copytree(fixture, table)
        tool = tools.table()
        tool.open(str(table.resolve()), nomodify=False)
        for method, args in calls:
            getattr(tool, method)(*args)
        tool.done()
    print(len(variants), "variants in", out_dir)


def command_compare(rs_json, casa_json):
    first = json.loads(Path(rs_json).read_text(encoding="utf-8"))
    second = json.loads(Path(casa_json).read_text(encoding="utf-8"))

    def normal(value):
        return re.sub(r'(/[^"]*/)?[\w.]*\.(table|ms)', "<T>", json.dumps(value, sort_keys=True))

    different = [key for key in first if normal(first[key]) != normal(second.get(key))]
    print("records compared:", len(first), "| differences:", different)
    for key in different:
        print(key, "\n  casa-rs", normal(first[key])[:300], "\n  casa   ", normal(second.get(key))[:300])
    return 1 if different else 0


def command_read(table_path, out_json):
    from casatools import table  # noqa: PLC0415

    tool = table()
    tool.open(table_path)
    write_json(out_json, dump(tool, table_path))
    tool.done()


def command_opens(directory):
    from casatools import table  # noqa: PLC0415

    failed = 0
    for path in sorted(Path(directory).glob("v_*.table")):
        tool = table()
        try:
            tool.open(str(path))
            tool.getkeywords()
            for column in tool.colnames():
                tool.getcolkeywords(column)
            tool.info()
            print("OK   ", path.name)
        except Exception as error:  # noqa: BLE001 - reporting what CASA refuses
            failed += 1
            print("FAIL ", path.name, str(error).splitlines()[0][:90])
        finally:
            tool.done()
    return 1 if failed else 0


def command_write_bools(table_path):
    import numpy as np  # noqa: PLC0415
    from casatools import table  # noqa: PLC0415

    tool = table()
    tool.open(table_path, nomodify=False)
    tool.putkeyword("flags9", np.arange(9) % 3 == 0)
    tool.putkeyword("flags_2d", np.array([[True, False, False], [True, True, False]]))
    tool.putcolkeyword("id", "colflags", np.arange(10) % 4 < 2)
    tool.close()


COMMANDS = {
    "write": (command_write, 3),
    "variants": (command_variants, 2),
    "compare": (command_compare, 2),
    "read": (command_read, 2),
    "opens": (command_opens, 1),
    "write-bools": (command_write_bools, 1),
    "rsread": (command_rsread, 2),
}


def main(argv):
    if not argv or argv[0] not in COMMANDS or len(argv) - 1 != COMMANDS[argv[0]][1]:
        print(__doc__)
        return 2
    return COMMANDS[argv[0]][0](*argv[1:]) or 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
