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

`cells-write KIND SOURCE OUT_TABLE OUT_JSON`
    KIND is `table` (the python fixture) or `ms` (the small MeasurementSet).
    Copies SOURCE to OUT_TABLE, and again beside it for the second scenario
    (`OUT_TABLE.slices`, or `.ms_slices`), replays on each the recorded cell
    writes (`cells_write` and `slices`, or `ms_cells_write` and `ms_slices`)
    through `casars.tools.table`, checking every step against the recording,
    and writes what casa-rs reads of the cells (`cells-dump` form) to
    OUT_JSON.

`rsread TABLE OUT_JSON`
    What casa-rs reads of any table (the same dump `write` ends with), for a
    table CASA wrote.

`typed FIXTURE OUT_TABLE`
    Copies the python fixture to OUT_TABLE (and OUT_TABLE.sub, a table to
    refer to) and writes keywords whose stored type a value comparison cannot
    see: a `float32` scalar and array, a `complex64` array, an integer, a
    reference to the readable table and a string that only looks like one.

`edit TABLE`
    One unrelated keyword write by casa-rs into a table CASA wrote
    (`write-variable`), which rewrites the keyword record CASA created.

`structure-write FIXTURE MS OUT_DIR OUT_JSON`
    In OUT_DIR: `rows.table`, `columns.table` and `rows_columns.table`
    (which adds columns too), copies of the python fixture on which the
    recorded `rows`, `columns` and `rows_columns` scenarios are replayed
    (as far as their first close) through `casars.tools.table`, checking every
    step; `renamed.ms`, a copy of the small MeasurementSet with four columns
    renamed; `removed.ms`, with two columns removed; the `copy_*` tables that
    `copy` and `copyrows` make. OUT_JSON is what casa-rs
    reads of each (`structure_dump`: names, descriptions, the first cells).

`structure-rsread DIR OUT_JSON`
    The same dump by casa-rs, of the tables in DIR (also those CASA wrote).

`compare RS_JSON CASA_JSON`
    Prints the records on which the two dumps differ.

With CASA's Python (`~/ext/DeveloperTools/CASA.app/Contents/MacOS/python3 -I`
on the Mac mini; copy this file and `table_tool_oracle.py` side by side):

`read TABLE OUT_JSON`
    What CASA reads of a casa-rs-written table, in the same form.

`opens DIR`
    Which of the `v_*.table` tables CASA opens and reads (`OK`/`FAIL`).

`cells-read KIND TABLE OUT_JSON`
    What CASA reads of the cells the `cells-write` tables hold (TABLE and its
    sibling), in the same form, to `compare` with the `cells-write` dump.

`write-bools TABLE`
    CASA writes Boolean-array keywords into TABLE (a copy of the fixture);
    then `read` it here and `rsread` it with casa-rs, and `compare`.

`resize DIR`
    CASA puts a differently shaped array into each array keyword of the
    `v_arr_*.table` tables `variants` wrote. casacore refuses that for an
    array keyword whose descriptor is fixed-shape, which casa-rs wrote until
    Astra's review of #1034 (finding 3).

`structure-read DIR OUT_JSON`
    What CASA reads of the tables `structure-write` left in DIR (and of
    `casa_renamed.ms` and `casa_removed.ms`, below), in the same form, to
    `compare` with casa-rs's dump of them.

`structure-casa-write MS DIR FIXTURE`
    CASA copies the python fixture and the MeasurementSet (`copy`,
    `copyrows`: `casa_copy_values.table`, `casa_copyrows.table`,
    `casa_copy_ms.ms`), and renames and removes the same columns of two copies
    of the small MeasurementSet, `DIR/casa_renamed.ms` and `DIR/casa_removed.ms`;
    `structure-rsread` them with casa-rs (a renamed column keeps its old name
    in the column set, which casa-rs failed to find before #1025's table
    part) and `compare` with `structure-read`.

`write-variable TABLE`
    CASA writes array keywords (variable-shape, as CASA makes them) into
    TABLE, a copy of the fixture; `edit` it with casa-rs, then
    `resize-variable` it.

`resize-variable TABLE`
    CASA resizes the arrays `write-variable` wrote: they must still be
    variable-shape after casa-rs rewrote the keyword record.

`typed-check TABLE`
    Updates each keyword `typed` wrote with a value of the type casatools
    gives it (casacore refuses a value of another type than the stored one),
    and a table reference with a plain string (refused), a string with
    another (accepted). Prints `OK`/`FAIL` for each.

A passing run: `write` and `read` give dumps `compare` finds no difference
in, `opens` reports every variant `OK`, `write-bools` then `rsread` and
`read` give dumps `compare` finds no difference in, `resize` and
`resize-variable` (after `edit`) report every resize `OK`, and `typed-check`
reports every probe `OK`. A replayed write that does not give what the
recording says stops the run: the dumps would otherwise agree on a table
nothing was written to.
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


def replay_step(replay, tool, step, path, where):
    """Runs a recorded step and checks it against the recording, as the
    replay test does: a write that was refused would leave both readers
    looking at the same untouched table."""
    got = replay.run_step(tool, step, path)
    if "error" in step:
        if step["error"]["message"] == "Unknown casa DataType!":
            assert "value" in got and got["value"]["ndarray"] == "int64", where
        else:
            assert "error" in got, f"{where}: CASA raised {step['error']}, casa-rs gave {got}"
            replay.same_error(got["error"], step["error"], where)
    else:
        assert "value" in got, f"{where}: CASA gave a value, casa-rs raised {got['error']}"
        replay.same_value(got["value"], step["value"], where)


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
        for index, step in enumerate(replay.RECORD["scenarios"][scenario]["steps"]):
            replay_step(replay, tool, step, path, f"{scenario}[{index}] {step['method']}")
    tool.done()
    reader = tools.table()
    reader.open(path)
    write_json(out_json, dump(reader, path))
    reader.done()


CELL_READS = {
    "table": [
        *[("getcell", [column, row]) for column in ("id", "label", "gain", "spectrum", "meta") for row in range(3)],
        ("getvarcol", ["vary"]),
        ("getcol", ["label"]),
        ("getcol", ["gain"]),
        ("getcol", ["spectrum"]),
    ],
    "ms": [
        ("getcol", ["ANTENNA1", 0, 3]),
        ("getcol", ["TIME", 0, 3]),
        ("getcol", ["FLAG_ROW", 0, 3]),
        ("getcol", ["WEIGHT", 0, 3]),
        ("getcell", ["WEIGHT", 1]),
        ("getcell", ["UVW", 0]),
        ("getcell", ["DATA", 0]),
        ("getcell", ["DATA", 1]),
        ("getcell", ["FLAG", 0]),
        ("getcell", ["FLAG", 1]),
    ],
}


def cell_dump(tool, kind, path):
    """What the tool reads of the cells the recorded writes touch."""
    result = {}
    for index, (method, args) in enumerate(CELL_READS[kind]):
        key = f"{index}:{method}{args}"
        try:
            result[key] = encode(getattr(tool, method)(*args), path)
        except Exception as error:  # noqa: BLE001 - CASA cannot read an int64 column's getcol
            result[key] = {"error": str(error).splitlines()[0][:80]}
    return result


def scenario_tables(kind, table_path):
    """The tables a cells-write holds: one per scenario, each a fresh copy of
    the source (the recording of `slices` is of a fresh table), the first at
    `table_path` and the others beside it, named after their scenario."""
    names = ["cells_write", "slices"] if kind == "table" else ["ms_cells_write", "ms_slices"]
    table_path = Path(table_path)
    return [
        (name, table_path if number == 0 else table_path.with_name(f"{table_path.name}.{name}"))
        for number, name in enumerate(names)
    ]


def command_cells_write(kind, source, out_table, out_json):
    sys.path.insert(0, str(RECORD))
    import test_tools_table as replay  # noqa: PLC0415
    from casars import tools  # noqa: PLC0415

    dump_of = {}
    for name, table_path in scenario_tables(kind, out_table):
        shutil.rmtree(table_path, ignore_errors=True)
        shutil.copytree(source, table_path)
        path = str(table_path.resolve())
        tool = tools.table()
        for index, step in enumerate(replay.RECORD["scenarios"][name]["steps"]):
            if step["method"] == "close":
                break
            replay_step(replay, tool, step, path, f"{name}[{index}] {step['method']}")
        tool.done()
        reader = tools.table()
        reader.open(path)
        dump_of.update(
            {f"{name}:{key}": value for key, value in cell_dump(reader, kind, path).items()}
        )
        reader.done()
    write_json(out_json, dump_of)


def command_cells_read(kind, table_path, out_json):
    from casatools import table  # noqa: PLC0415

    dump_of = {}
    for name, path in scenario_tables(kind, table_path):
        tool = table()
        tool.open(str(path))
        dump_of.update(
            {f"{name}:{key}": value for key, value in cell_dump(tool, kind, str(path)).items()}
        )
        tool.done()
    write_json(out_json, dump_of)


def command_rsread(table_path, out_json):
    from casars import tools  # noqa: PLC0415

    path = str(Path(table_path).resolve())
    tool = tools.table()
    tool.open(path)
    write_json(out_json, dump(tool, path))
    tool.done()


#: The tables `structure-write` makes (and `structure-casa-write` adds to) in
#: its directory.
STRUCTURE_TABLES = (
    "rows.table", "columns.table", "rows_columns.table", "renamed.ms", "removed.ms",
    "casa_renamed.ms", "casa_removed.ms", "casa_named.table", "casa_named_aipsio.table",
    "shrink_user.table", "shrink_permanent.table", "tiled_multi.table",
    "copy_plain.table", "copy_values_big.table", "copy_rows.table", "copy_self.table",
    "copy_ms.ms", "casa_copy_values.table", "casa_copyrows.table", "casa_copy_ms.ms",
)

#: What the structural calls do to the MeasurementSet's main table, by name:
#: the columns renamed, then the ones removed (those the renames left alone).
MS_RENAMES = [("ANTENNA1", "ANT1"), ("DATA", "DATA2"), ("FLAG_ROW", "FR"), ("TIME", "T")]
MS_REMOVED = ["DATA", "FLAG_ROW"]


#: Columns whose cells the dump does not read, each with the issue that says
#: why: casa-rs's lazy read of a `TiledDataStMan` id column is wrong (#1038).
UNREAD_CELLS = {("tiled_multi.table", "id"): "#1038"}


def structure_dump(tool, path, table_name=""):
    """What the tool reads of the table's rows and columns: the names, each
    column's description (with its storage manager's type and name, and its
    keywords) and its cells (every row of a small table, the first three of a
    larger one). A cell is read only when `iscelldefined` says it is defined,
    so that a read that fails for any other reason ends the run, as the same
    failure of both tools would otherwise pass unseen. The managers
    (`getdminfo`) are dumped where casa-rs answers."""
    rows = tool.nrows()
    result = {"nrows": rows, "colnames": list(tool.colnames())}
    try:
        result["dminfo"] = tool.getdminfo()
    except NotImplementedError:  # the managers casa-rs does not read the SPEC of
        pass
    for column in result["colnames"]:
        description = tool.getcoldesc(column)
        result[f"desc:{column}"] = {
            key: description[key]
            for key in (
                "valueType", "option", "ndim", "shape", "dataManagerType", "dataManagerGroup",
                "keywords", "maxlen", "comment",
            )
            if key in description
        }
        cells = []
        if (table_name, column) in UNREAD_CELLS:
            result[f"cells:{column}"] = f"not read: {UNREAD_CELLS[(table_name, column)]}"
            continue
        for row in range(rows if rows <= 16 else 3):
            if tool.iscelldefined(column, row):
                cells.append(encode(tool.getcell(column, row), path))
            else:
                cells.append("undefined")
        result[f"cells:{column}"] = cells
    return {key: encode(value, path) for key, value in result.items()}


def structure_dumps(tool_class, directory):
    """The `structure_dump` of each table of `STRUCTURE_TABLES` in `directory`."""
    dumps = {}
    for name in STRUCTURE_TABLES:
        path = Path(directory) / name
        if not path.exists():
            continue
        tool = tool_class()
        tool.open(str(path.resolve()))
        dumps.update(
            {
                f"{name}:{key}": value
                for key, value in structure_dump(tool, str(path.resolve()), name).items()
            }
        )
        tool.done()
    return dumps


def command_structure_write(fixture, ms, out_dir, out_json):
    sys.path.insert(0, str(RECORD))
    import test_tools_table as replay  # noqa: PLC0415
    from casars import tools  # noqa: PLC0415

    out_dir = Path(out_dir)
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    # The recorded rows and columns scenarios, as far as the first close: CASA
    # then reads the table casa-rs left.
    for name, scenario in (
        ("rows.table", "rows"),
        ("columns.table", "columns"),
        ("rows_columns.table", "rows_columns"),
    ):
        shutil.copytree(fixture, out_dir / name)
        path = str((out_dir / name).resolve())
        tool = tools.table()
        for index, step in enumerate(replay.RECORD["scenarios"][scenario]["steps"]):
            if step["method"] == "close":
                break
            replay_step(replay, tool, step, path, f"{scenario}[{index}] {step['method']}")
        tool.done()
    shutil.copytree(ms, out_dir / "renamed.ms")
    tool = tools.table()
    assert tool.open(str((out_dir / "renamed.ms").resolve()), nomodify=False)
    for old, new in MS_RENAMES:
        assert tool.renamecol(old, new)
    tool.done()
    shutil.copytree(ms, out_dir / "removed.ms")
    tool = tools.table()
    assert tool.open(str((out_dir / "removed.ms").resolve()), nomodify=False)
    assert tool.removecols(MS_REMOVED)
    tool.done()
    # Rows removed, then every other kind of write, under a write lock that is
    # kept (Astra's review of #1036, finding 1): CASA does the same sequence.
    for name, option in (("shrink_user.table", "user"), ("shrink_permanent.table", "permanent")):
        shutil.copytree(fixture, out_dir / name)
        tool = tools.table()
        assert tool.open(
            str((out_dir / name).resolve()), lockoptions={"option": option}, nomodify=False
        )
        if option == "user":
            assert tool.lock()
        assert tool.removerows([2])
        assert tool.putkeyword("k", 1)
        assert tool.putcell("id", 0, 9)
        assert tool.renamecol("label", "name")
        assert tool.addcols({"extra": {"valueType": "int"}})
        if option == "user":
            assert tool.unlock()
        tool.done()
    # casacore's TiledDataStMan table with two data columns and an id column:
    # a data column is renamed, the id column refused (its name is also in the
    # manager's hypercube values).
    tiled = (
        Path(__file__).resolve().parents[2]
        / "crates/casa-tables/tests/data/tiled_data_stman/multi/big"
    )
    target = out_dir / "tiled_multi.table"
    target.mkdir()
    for file in ("table.dat", "table.f0", "table.f0_TSM0", "table.info"):
        shutil.copy(tiled / file, target / file)
    tool = tools.table()
    assert tool.open(str(target.resolve()), nomodify=False)
    assert tool.renamecol("a", "aa")
    try:
        tool.renamecol("id", "iz")
    except RuntimeError:
        pass
    else:
        raise SystemExit("the id column of a tiled manager was renamed")
    tool.done()
    # `copy` and `copyrows`: a plain copy of the python fixture and of the
    # MeasurementSet (their files as they are), a copy of the values with the
    # byte order changed, a copy without rows given two rows of the original,
    # and a table given its own rows.
    source = tools.table()
    assert source.open(str(Path(fixture).resolve()))
    source.copy(str((out_dir / "copy_plain.table").resolve())).done()
    source.copy(
        str((out_dir / "copy_values_big.table").resolve()), valuecopy=True, endian="big"
    ).done()
    empty = out_dir / "copy_rows.table"
    source.copy(str(empty.resolve()), norows=True).done()
    assert source.copyrows(str(empty.resolve()), 1, 0, 2)
    source.done()
    shutil.copytree(fixture, out_dir / "copy_self.table")
    tool = tools.table()
    assert tool.open(str((out_dir / "copy_self.table").resolve()), nomodify=False)
    assert tool.copyrows(str((out_dir / "copy_self.table").resolve()))
    tool.done()
    tool = tools.table()
    assert tool.open(str(Path(ms).resolve()))
    tool.copy(str((out_dir / "copy_ms.ms").resolve())).done()
    tool.done()
    write_json(out_json, structure_dumps(tools.table, out_dir))


def command_structure_read(directory, out_json):
    from casatools import table  # noqa: PLC0415

    write_json(out_json, structure_dumps(table, directory))


def command_structure_rsread(directory, out_json):
    from casars import tools  # noqa: PLC0415

    write_json(out_json, structure_dumps(tools.table, directory))
    # A StManAipsIO with a name of its own is not rewritten in full: the name
    # would be lost. The row calls refuse it, and change nothing.
    named = Path(directory) / "casa_named_aipsio.table"
    if named.exists():
        before = {p.name: p.read_bytes() for p in named.iterdir() if p.name != "table.lock"}
        tool = tools.table()
        assert tool.open(str(named.resolve()), nomodify=False)
        for call in (lambda: tool.addrows(1), lambda: tool.removerows([0]),
                     lambda: tool.addcols({"c": {"valueType": "int"}})):
            try:
                call()
            except NotImplementedError:
                continue
            raise SystemExit("a named StManAipsIO table was rewritten")
        tool.done()
        after = {p.name: p.read_bytes() for p in named.iterdir() if p.name != "table.lock"}
        assert before == after, "a refused call changed the named StManAipsIO table"
        print("named StManAipsIO: row and column calls refused, table unchanged")


def command_structure_casa_write(ms, out_dir, fixture):
    """CASA renames and removes the same columns of a copy of the MS, and
    copies tables (`copy`, `copyrows`) the way `structure-write` does."""
    from casatools import table  # noqa: PLC0415

    out_dir = Path(out_dir)
    for name, work in (
        ("casa_renamed.ms", lambda tool: [tool.renamecol(old, new) for old, new in MS_RENAMES]),
        ("casa_removed.ms", lambda tool: tool.removecols(MS_REMOVED)),
    ):
        shutil.rmtree(out_dir / name, ignore_errors=True)
        shutil.copytree(ms, out_dir / name)
        tool = table()
        tool.open(str(out_dir / name), nomodify=False)
        work(tool)
        tool.close()
    # Tables whose managers are named: a tiled manager bound to the NAME
    # `actual` while its column declares the group `declared` (`getcoldesc`
    # and `getdminfo` say `actual`), and a `StManAipsIO` named `mine` (whose
    # name casa-rs's full save would drop, so the tool must not rewrite it).
    import numpy as np  # noqa: PLC0415

    named = out_dir / "casa_named.table"
    shutil.rmtree(named, ignore_errors=True)
    tool = table()
    tool.create(
        str(named),
        {
            "a": {"valueType": "int"},
            "cube": {
                "valueType": "float", "ndim": 2, "shape": [2, 3], "option": 4,
                "dataManagerType": "TiledShapeStMan", "dataManagerGroup": "declared",
            },
        },
        nrow=3,
        dminfo={
            "*1": {
                "TYPE": "TiledShapeStMan", "NAME": "actual",
                "SPEC": {"DEFAULTTILESHAPE": [2, 3, 2]}, "COLUMNS": ["cube"],
            }
        },
    )
    for row in range(3):
        tool.putcell("a", row, row + 5)
        tool.putcell("cube", row, np.arange(6, dtype=np.float32).reshape(2, 3) + row)
    tool.close()
    named_aipsio = out_dir / "casa_named_aipsio.table"
    shutil.rmtree(named_aipsio, ignore_errors=True)
    tool = table()
    tool.create(
        str(named_aipsio),
        {"a": {"valueType": "int"}, "b": {"valueType": "double"}},
        nrow=3,
        dminfo={"*1": {"TYPE": "StManAipsIO", "NAME": "mine", "SPEC": {}, "COLUMNS": ["a", "b"]}},
    )
    for row in range(3):
        tool.putcell("a", row, row + 1)
        tool.putcell("b", row, 0.5 * row)
    tool.close()
    source = table()
    source.open(str(fixture))
    for name in ("casa_copy_values.table", "casa_copyrows.table", "casa_copy_ms.ms"):
        shutil.rmtree(out_dir / name, ignore_errors=True)
    source.copy(str(out_dir / "casa_copy_values.table"), valuecopy=True, endian="big").done()
    source.copy(str(out_dir / "casa_copyrows.table"), norows=True).done()
    assert source.copyrows(str(out_dir / "casa_copyrows.table"), 1, 0, 2)
    source.close()
    source.open(str(ms))
    source.copy(str(out_dir / "casa_copy_ms.ms")).done()
    source.done()


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
            assert getattr(tool, method)(*args), f"{name}: {method}{args} was refused"
        tool.done()
    print(len(variants), "variants in", out_dir)


def command_compare(rs_json, casa_json):
    first = json.loads(Path(rs_json).read_text(encoding="utf-8"))
    second = json.loads(Path(casa_json).read_text(encoding="utf-8"))

    def normal(value):
        text = re.sub(r'(/[^"]*/)?[\w.]*\.(table|ms)', "<T>", json.dumps(value, sort_keys=True))
        # An empty string array has a NumPy width of its own in each tool
        # (`<U1`, `<U5`); the width of nothing is not compared.
        return re.sub(r'("data": \[\], "ndarray": )"<U\d+"', r'\1"<U"', text)

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


def command_resize(directory):
    import numpy as np  # noqa: PLC0415
    from casatools import table  # noqa: PLC0415

    # A different length, and for the 2-D one a different shape: a fixed-shape
    # descriptor refuses both (`Record::define - fixed array conformance`).
    bigger = {
        "arr_int": ("arr_int", np.array([1, 2, 3, 4, 5])),
        "arr_float2d": ("arr_float", np.arange(12.0).reshape(3, 4)),
        "arr_str": ("arr_str", np.array(["a", "bc", "def"])),
        "arr_bool": ("arr_bool", np.array([True, False, True])),
        "arr_bool_9": ("arr_bool9", np.arange(17) % 2 == 0),
        "arr_complex": ("arr_complex", np.array([1 + 1j, 2 - 2j, 3j])),
    }
    failed = 0
    for name, (keyword, value) in bigger.items():
        path = Path(directory) / f"v_{name}.table"
        tool = table()
        try:
            tool.open(str(path), nomodify=False)
            tool.putkeyword(keyword, value)
            got = tool.getkeyword(keyword)
            assert np.array_equal(got, value) and got.shape == value.shape, got
            print("OK   resize", path.name)
        except Exception as error:  # noqa: BLE001 - reporting what CASA refuses
            failed += 1
            print("FAIL resize", path.name, str(error).splitlines()[0][:90])
        finally:
            tool.done()
    return 1 if failed else 0


def command_write_variable(table_path):
    import numpy as np  # noqa: PLC0415
    from casatools import table  # noqa: PLC0415

    tool = table()
    tool.open(table_path, nomodify=False)
    tool.putkeyword("vararr", np.array([1.0, 2.0]))
    tool.putkeyword("varint", np.array([1, 2, 3]))
    tool.putkeyword("varstr", np.array(["a", "b"]))
    tool.close()


def command_edit(table_path):
    from casars import tools  # noqa: PLC0415

    tool = tools.table()
    assert tool.open(str(Path(table_path).resolve()), nomodify=False)
    assert tool.putkeyword("unrelated", 1)
    assert tool.putcolkeyword("id", "unrelated", 2)
    tool.done()


def command_resize_variable(table_path):
    import numpy as np  # noqa: PLC0415
    from casatools import table  # noqa: PLC0415

    tool = table()
    failed = 0
    tool.open(table_path, nomodify=False)
    for keyword, value in [
        ("vararr", np.array([1.0, 2.0, 3.0, 4.0])),
        ("varint", np.array([1, 2])),
        ("varstr", np.array(["a", "b", "c"])),
    ]:
        try:
            tool.putkeyword(keyword, value)
            assert np.array_equal(tool.getkeyword(keyword), value)
            print("OK   resize", keyword)
        except Exception as error:  # noqa: BLE001 - reporting what CASA refuses
            failed += 1
            print("FAIL resize", keyword, str(error).splitlines()[0][:90])
    tool.done()
    return 1 if failed else 0


TYPED_UPDATES = [
    # keyword, a value of the type casatools gives it, whether CASA takes it
    ("f32", 2.5, True),
    ("f32arr", [3.0, 4.0, 5.0], True),
    ("c64arr", [1 + 2j, 3 + 0j], True),
    ("i", 8, True),
    ("SUB", "plain text", False),  # a table reference is not a string
    ("NOTREF", "plain text", True),  # a string that only looks like a reference
]


def command_typed(fixture, out_table):
    import numpy as np  # noqa: PLC0415
    from casars import tools  # noqa: PLC0415

    out_table = Path(out_table)
    sub = Path(f"{out_table}.sub")
    for path in (out_table, sub):
        shutil.rmtree(path, ignore_errors=True)
        shutil.copytree(fixture, path)
    tool = tools.table()
    assert tool.open(str(out_table.resolve()), nomodify=False)
    assert tool.putkeyword("f32", np.float32(1.25))
    assert tool.putkeyword("f32arr", np.array([1, 2], dtype=np.float32))
    assert tool.putkeyword("c64arr", np.array([1 + 1j], dtype=np.complex64))
    assert tool.putkeyword("i", 7)
    assert tool.putkeyword("SUB", f"Table: {sub.resolve()}")
    assert tool.putkeyword("NOTREF", "Table: /no/such/table")
    tool.done()


def command_typed_check(table_path):
    import numpy as np  # noqa: PLC0415
    from casatools import table  # noqa: PLC0415

    failed = 0
    for keyword, value, accepted in TYPED_UPDATES:
        tool = table()
        tool.open(table_path, nomodify=False)
        try:
            tool.putkeyword(keyword, np.array(value) if isinstance(value, list) else value)
            outcome = True
        except Exception as error:  # noqa: BLE001 - the refusal is the answer
            outcome = False
            reason = str(error).splitlines()[0][:70]
        tool.done()
        good = outcome == accepted
        failed += not good
        print("OK  " if good else "FAIL", "typed", keyword, "accepted" if outcome else f"refused ({reason})")
    return 1 if failed else 0


COMMANDS = {
    "write": (command_write, 3),
    "variants": (command_variants, 2),
    "compare": (command_compare, 2),
    "read": (command_read, 2),
    "opens": (command_opens, 1),
    "write-bools": (command_write_bools, 1),
    "rsread": (command_rsread, 2),
    "cells-write": (command_cells_write, 4),
    "cells-read": (command_cells_read, 3),
    "resize": (command_resize, 1),
    "write-variable": (command_write_variable, 1),
    "edit": (command_edit, 1),
    "resize-variable": (command_resize_variable, 1),
    "typed": (command_typed, 2),
    "typed-check": (command_typed_check, 1),
    "structure-write": (command_structure_write, 4),
    "structure-read": (command_structure_read, 2),
    "structure-rsread": (command_structure_rsread, 2),
    "structure-casa-write": (command_structure_casa_write, 3),
}


def main(argv):
    if not argv or argv[0] not in COMMANDS or len(argv) - 1 != COMMANDS[argv[0]][1]:
        print(__doc__)
        return 2
    return COMMANDS[argv[0]][0](*argv[1:]) or 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
