"""`casars.tools.table`: CASA's `tb` tool (ADR-0019, #1025's table part, #1028).

Behaviour is compared with what CASA 6.7.6.14 returned for the scripted calls
in `data/casa_table_tool.json`, written by
`tools/casa-compat/table_tool_oracle.py`. Each scenario starts from a fresh
copy of its fixture (the python fixture table, or the small MeasurementSet in
`crates/casa-ms/tests/fixtures`) and replays its steps on a tool; a step's
value must equal CASA's in type, NumPy dtype, shape and contents, or raise
the error type CASA raised with CASA's message. The path of the table
appears in the record as `<TABLE>`.

Departures, each recorded in `docs/CASA (C++) bugs.md`:

* CASA's `getcol` of a true `int64` column raises "Unknown casa DataType!";
  casa-rs returns the `int64` array.
* A cell a storage manager cannot read raises CASA's "Table DataManager
  error: Invalid operation: ..." with the storage manager's own wording
  ("TSM: ..."); only the start of the message is compared.
* An empty string array has a NumPy width of its own in CASA; only its kind
  is compared.

Log messages (CASA's INFO "No table opened." and the like) go to the Python
logger `casars.tools.table` and are not compared.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from casars import tools

from _casa_tools import DATA

REPO_ROOT = Path(__file__).resolve().parents[4]
RECORD = json.loads((DATA / "casa_table_tool.json").read_text(encoding="utf-8"))
PLACEHOLDER = "<TABLE>"

#: Errors whose text names the storage manager: only this much is compared.
DATA_MANAGER = "Table DataManager error: Invalid operation: "


def encode_value(value: Any, path: str) -> Any:
    """`value` as the oracle's `encode` writes it; keep the two in step."""
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
        return {"dict": [[str(key), encode_value(item, path)] for key, item in value.items()]}
    if isinstance(value, tuple):
        return {"tuple": [encode_value(item, path) for item in value]}
    if isinstance(value, list):
        return {"list": [encode_value(item, path) for item in value]}
    if value is None:
        return {"none": True}
    raise TypeError(f"cannot encode {type(value)}")


def decode_value(encoded: Any, path: str) -> Any:
    """The Python value `encoded` stands for (the inverse of `encode_value`)."""
    if "bool" in encoded:
        return encoded["bool"]
    if "none" in encoded:
        return None
    for key, native in (("int", int), ("float", float)):
        if key in encoded:
            kind = encoded["type"]
            value = native(encoded[key])
            return value if kind == native.__name__ else getattr(np, kind)(value)
    if "complex" in encoded:
        value = complex(float(encoded["complex"][0]), float(encoded["complex"][1]))
        return value if encoded["type"] == "complex" else getattr(np, encoded["type"])(value)
    if "str" in encoded:
        return encoded["str"].replace(PLACEHOLDER, path)
    if "ndarray" in encoded:
        dtype = np.dtype(encoded["ndarray"])
        data = encoded["data"]
        if dtype.kind == "c":
            data = [complex(float(re), float(im)) for re, im in data]
        elif dtype.kind == "f":
            data = [float(item) for item in data]
        return np.array(data, dtype=dtype).reshape(encoded["shape"])
    if "dict" in encoded:
        return {key: decode_value(item, path) for key, item in encoded["dict"]}
    if "list" in encoded:
        return [decode_value(item, path) for item in encoded["list"]]
    if "tuple" in encoded:
        return tuple(decode_value(item, path) for item in encoded["tuple"])
    raise TypeError(f"cannot decode {encoded}")


@pytest.fixture(scope="module")
def fixtures(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    """The python fixture table and the small MeasurementSet, once."""
    root = tmp_path_factory.mktemp("table_tool_fixtures").resolve()
    subprocess.run(
        ["cargo", "run", "-q", "-p", "casars-python", "--example", "make_python_fixtures",
         "--", str(root / "fixtures")],
        cwd=REPO_ROOT,
        check=True,
    )
    with tarfile.open(
        REPO_ROOT / "crates/casa-ms/tests/fixtures/mssel_test_small.ms.tgz", "r:gz"
    ) as archive:
        # `filter` is missing from Python 3.10 before 3.10.12 and 3.11 before 3.11.4.
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(root / "ms", **extract)
    return {
        "table": root / "fixtures" / "python_fixture.table",
        "ms": root / "ms" / "mssel_test_small.ms",
    }


def without_empty_widths(value: Any) -> Any:
    """`value` with an empty string array's NumPy width dropped, wherever it is."""
    if isinstance(value, dict):
        if value.get("ndarray", "").startswith("<U") and value["data"] == []:
            return {**value, "ndarray": "<U"}
        return {key: without_empty_widths(item) for key, item in value.items()}
    if isinstance(value, list):
        return [without_empty_widths(item) for item in value]
    return value


def same_value(got: Any, want: Any, where: str) -> None:
    """`got` equals the recorded `want`, up to the departures in the docstring."""
    assert without_empty_widths(got) == without_empty_widths(want), where


def same_error(got: dict[str, str], want: dict[str, str], where: str) -> None:
    assert got["type"] == want["type"], f"{where}: {got} against CASA's {want}"
    if want["message"].startswith(DATA_MANAGER):
        assert got["message"].startswith(DATA_MANAGER), where
    else:
        # CASA appends "... thrown by <function>" lines naming its own code to
        # some messages; only the first line is compared.
        assert got["message"].split("\n")[0] == want["message"].split("\n")[0], where


def run_step(tool: Any, step: dict[str, Any], path: str) -> dict[str, Any]:
    args = [decode_value(arg, path) for arg in step["args"]]
    kwargs = {name: decode_value(value, path) for name, value in step["kwargs"].items()}
    try:
        return {"value": encode_value(getattr(tool, step["method"])(*args, **kwargs), path)}
    except Exception as error:  # noqa: BLE001 - compared with what CASA raised
        return {"error": {"type": type(error).__name__, "message": str(error).replace(path, PLACEHOLDER)}}


#: Recorded from CASA but not implemented yet (`getcoldesc`, `getdminfo`,
#: `addrows`, `removerows`, `addcols`, `renamecol`, `removecols`); a scenario
#: leaves this set as its methods land.
PENDING: set[str] = {"rows_columns"}

#: Recorded from CASA but not followed: its own faults
#: (`docs/CASA (C++) bugs.md`), each with a test of its own below.
CASA_BUGS: set[str] = {"casa_bugs", "int_overflow"}

REPLAYED = sorted(set(RECORD["scenarios"]) - CASA_BUGS - PENDING)


@pytest.mark.parametrize("scenario", REPLAYED)
def test_the_tool_answers_as_casa_does(
    scenario: str, fixtures: dict[str, Path], tmp_path: Path
) -> None:
    recorded = RECORD["scenarios"][scenario]
    source = fixtures[recorded["fixture"]]
    path = str((tmp_path / source.name).resolve())
    shutil.copytree(source, path)
    tool = tools.table()
    for index, step in enumerate(recorded["steps"]):
        where = f"{scenario}[{index}] {step['method']}{tuple(step['args'])} {step['kwargs']}"
        got = run_step(tool, step, path)
        if "error" in step:
            if step["error"]["message"] == "Unknown casa DataType!":
                assert "value" in got and got["value"]["ndarray"] == "int64", where
            else:
                assert "error" in got, f"{where}: CASA raised {step['error']}, casa-rs gave {got}"
                same_error(got["error"], step["error"], where)
        else:
            assert "value" in got, f"{where}: CASA gave a value, casa-rs raised {got['error']}"
            same_value(got["value"], step["value"], where)
    tool.done()


def test_every_call_releases_the_gil(fixtures: dict[str, Path], tmp_path: Path) -> None:
    """A long read does not freeze other Python threads (ADR-0019 item 4)."""
    import threading
    import time

    path = str((tmp_path / "ms").resolve())
    shutil.copytree(fixtures["ms"], path)
    tool = tools.table()
    assert tool.open(path)
    ticks: list[float] = []
    stop = threading.Event()

    def tick() -> None:
        while not stop.is_set():
            ticks.append(time.perf_counter())
            time.sleep(0.001)

    thread = threading.Thread(target=tick)
    thread.start()
    try:
        started = time.perf_counter()
        while time.perf_counter() - started < 0.2:
            tool.getcol("TIME")
    finally:
        stop.set()
        thread.join()
    during = [tick for tick in ticks if tick >= started]
    assert len(during) > 20, "the ticker thread starved while the tool read"
    tool.done()


def table_files(root: Path) -> dict[str, bytes]:
    """Every file of the table directory, by relative path."""
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != "table.lock"
    }


def test_writes_to_keywords_and_info_leave_the_storage_managers_alone(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """Only `table.dat` and `table.info` change: a flush or an unlock that
    rewrote the table would replace the data managers' files too (#1025)."""
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    before = table_files(path)
    tool = tools.table()
    assert tool.open(str(path), nomodify=False)
    assert tool.putkeyword("added", 7)
    assert tool.putcolkeyword("spectrum", "unit", "mJy")
    assert tool.putinfo({"type": "Edited"})
    assert tool.addreadmeline("a line")
    assert tool.removekeyword("added")
    assert tool.done()
    after = table_files(path)
    assert set(after) == set(before)
    changed = {name for name in before if before[name] != after[name]}
    assert changed <= {"table.dat", "table.info"}, changed
    assert "table.dat" in changed
    reader = tools.table()
    assert reader.open(str(path))
    assert reader.getcolkeywords("spectrum")["unit"] == "mJy"
    assert reader.info()["type"] == "Edited"
    assert reader.getcol("label").tolist() == ["alpha", "beta", "gamma"]
    reader.done()


@pytest.mark.parametrize(
    ("option", "lock_first", "ending"),
    [
        ("auto", False, "drop"),
        ("auto", False, "done"),
        ("permanent", False, "close"),
        ("permanent", False, "drop"),
        ("user", True, "close"),
        ("user", True, "unlock"),
        ("user", True, "drop"),
    ],
)
def test_a_write_leaves_the_storage_managers_alone_however_the_table_ends(
    option: str, lock_first: bool, ending: str, fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """On a MeasurementSet, whose storage managers a rewrite would visibly
    change: a table that holds the write lock when it is closed or dropped (a
    `permanent` one, a `user` one not unlocked) is written in full by
    casa-tables unless the tool left nothing unflushed (#1025)."""
    import gc

    path = tmp_path.resolve() / "mssel_test_small.ms"
    shutil.copytree(fixtures["ms"], path)
    before = table_files(path)
    tool = tools.table()
    assert tool.open(str(path), lockoptions={"option": option}, nomodify=False)
    if lock_first:
        assert tool.lock()
    assert tool.putkeyword("added", 7)
    assert tool.putcolkeyword("TIME", "note", "x")
    if ending == "unlock":
        assert tool.unlock()
        assert tool.close()
    elif ending in ("close", "done"):
        assert getattr(tool, ending)()
    else:
        del tool
        gc.collect()
    after = table_files(path)
    assert set(after) == set(before)
    assert {name for name in before if before[name] != after[name]} == {"table.dat"}
    reader = tools.table()
    assert reader.open(str(path))
    assert reader.getkeyword("added") == 7
    assert reader.getcolkeyword("TIME", "note") == "x"
    assert reader.nrows() == 1058
    reader.done()


@pytest.mark.parametrize("option", ["auto", "permanent", "user"])
@pytest.mark.parametrize(
    ("column", "write", "manager_file", "written"),
    [
        (
            "ANTENNA1",
            lambda tool: tool.putcell("ANTENNA1", 0, 5),
            "table.f14",
            lambda tool: (tool.getcell("ANTENNA1", 0), tool.getcell("ANTENNA1", 1)),
        ),
        (
            "TIME",
            lambda tool: tool.putcell("TIME", 1, 1.5e9),
            "table.f12",
            lambda tool: (tool.getcell("TIME", 1), tool.getcell("TIME", 0)),
        ),
        (
            "DATA",
            lambda tool: tool.putcell("DATA", 0, np.ones((4, 64), dtype=np.complex128)),
            "table.f17_TSM1",
            lambda tool: (tool.getcell("DATA", 0).sum(), tool.getcell("DATA", 1).sum()),
        ),
        (
            "FLAG",
            lambda tool: tool.putcell("FLAG", 0, np.zeros((4, 64), dtype=bool)),
            "table.f22_TSM1",
            lambda tool: (tool.getcell("FLAG", 0).sum(), tool.getcell("FLAG", 1).sum()),
        ),
        (
            "WEIGHT",
            lambda tool: tool.putcol("WEIGHT", np.ones((4, 3)), 0, 3),
            "table.f23_TSM1",
            lambda tool: (tool.getcell("WEIGHT", 2).sum(), tool.getcell("WEIGHT", 3).sum()),
        ),
        (
            "DATA slice",
            lambda tool: tool.putcellslice(
                "DATA", 1, np.ones((2, 4), dtype=np.complex128), [2, 10], [3, 13], [1, 1]
            ),
            "table.f17_TSM1",
            lambda tool: (
                tool.getcellslice("DATA", 1, [2, 10], [3, 13], [1, 1]).sum(),
                tool.getcellslice("DATA", 0, [2, 10], [3, 13], [1, 1]).sum(),
            ),
        ),
        (
            "FLAG column slice",
            lambda tool: tool.putcolslice(
                "FLAG", np.zeros((4, 1, 2), dtype=bool), [0, 5], [3, 5], [1, 1], 0, 2
            ),
            "table.f22_TSM1",
            lambda tool: (
                tool.getcolslice("FLAG", [0, 5], [3, 5], [1, 1], 0, 2).sum(),
                tool.getcellslice("FLAG", 2, [0, 5], [3, 5], [1, 1]).sum(),
            ),
        ),
    ],
)
def test_a_cell_write_changes_only_its_columns_storage_manager_file(
    option: str,
    column: str,
    write: Any,
    manager_file: str,
    written: Any,
    fixtures: dict[str, Path],
    tmp_path: Path,
) -> None:
    """A MeasurementSet's cell write is made in place: `table.dat` and the
    file of the storage manager that holds the column change, that one and no
    other, nothing is added or removed, in every lock mode; and what a reader
    then sees is the value written and the neighbouring cell untouched
    (#1025)."""
    path = tmp_path.resolve() / "mssel_test_small.ms"
    shutil.copytree(fixtures["ms"], path)
    before = table_files(path)
    untouched = tools.table()
    assert untouched.open(str(path))
    neighbours_before = written(untouched)
    untouched.done()
    tool = tools.table()
    assert tool.open(str(path), lockoptions={"option": option}, nomodify=False)
    if option == "user":
        assert tool.lock()
    assert write(tool)
    assert tool.close()
    after = table_files(path)
    assert set(after) == set(before), column
    changed = {name for name in before if before[name] != after[name]}
    assert changed == {"table.dat", manager_file}, (column, sorted(changed))
    reader = tools.table()
    assert reader.open(str(path))
    now = written(reader)
    reader.done()
    assert now[0] != neighbours_before[0], f"{column}: the value written is not there"
    assert now[1] == neighbours_before[1], f"{column}: the neighbouring cell changed"


def test_an_int_beyond_32_bits_is_kept_where_casa_truncates_it(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """CASA 6.7.6.14 stores such a Python int as 0 (`docs/CASA (C++) bugs.md`);
    casa-rs keeps it in a new keyword and refuses it for a 32-bit one."""
    steps = RECORD["scenarios"]["casa_bugs"]["steps"]
    assert [step["value"]["int"] for step in steps if step["method"] == "getkeyword"] == [0, 0]

    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    tool = tools.table()
    assert tool.open(str(path), nomodify=False)
    assert tool.putkeyword("newbig", 2**40)
    assert tool.getkeyword("newbig") == 2**40
    with pytest.raises(RuntimeError, match="does not fit the integer keyword version"):
        tool.putkeyword("version", 2**40)
    assert tool.getkeyword("version") == 1
    tool.done()


def test_an_int_beyond_32_bits_is_refused_in_a_cell_where_casa_wraps_it(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """CASA 6.7.6.14 writes 2147483648 to an int32 cell as -2147483648
    (`docs/CASA (C++) bugs.md`); casa-rs refuses it and leaves the cell."""
    steps = RECORD["scenarios"]["int_overflow"]["steps"]
    wrapped = [step["value"]["data"] for step in steps if step["method"] == "getcol"]
    assert wrapped == [[-2147483648, 2147483647, 1], [-2147483648, 1, 2]]

    path = tmp_path.resolve() / "mssel_test_small.ms"
    shutil.copytree(fixtures["ms"], path)
    tool = tools.table()
    assert tool.open(str(path), nomodify=False)
    before = tool.getcol("ANTENNA1", nrow=3).tolist()
    for row, value in [(0, 2**31), (1, -(2**31) - 1), (2, 2**32 + 1)]:
        with pytest.raises(RuntimeError, match="out of range for a column of Int"):
            tool.putcell("ANTENNA1", row, value)
    with pytest.raises(RuntimeError, match="out of range for a column of Int"):
        tool.putcol("ANTENNA1", np.array([2**31, 1, 2]), 0, 3)
    assert tool.getcol("ANTENNA1", nrow=3).tolist() == before
    tool.done()


def test_putcell_with_a_missing_row_among_several_writes_none_of_them(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """CASA writes the rows before the missing one and then fails; casa-rs
    refuses the call whole, in memory and on disk."""
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    tool = tools.table()
    assert tool.open(str(path), nomodify=False)
    before = [tool.getcell("id", row) for row in range(3)]
    with pytest.raises(RuntimeError, match="no such row"):
        tool.putcell("id", [0, 3], 22)
    assert [tool.getcell("id", row) for row in range(3)] == before
    assert tool.putcell("id", [], 1)
    assert [tool.getcell("id", row) for row in range(3)] == before
    assert tool.close()
    reader = tools.table()
    assert reader.open(str(path))
    assert [reader.getcell("id", row) for row in range(3)] == before
    reader.done()


def stored_big_endian(table: Path) -> bool:
    """Whether `table.dat` says the table's files are big-endian."""
    data = (table / "table.dat").read_bytes()
    assert data[:4] == b"\xbe\xbe\xbe\xbe"
    offset = 12 + int.from_bytes(data[8:12], "big")
    version = int.from_bytes(data[offset : offset + 4], "big")
    offset += 4 + (8 if version >= 3 else 4)
    return int.from_bytes(data[offset : offset + 4], "big") == 0


@pytest.mark.parametrize("name", ["python_fixture.table", "python_fixture.table.big"])
def test_a_write_keeps_the_byte_order_of_a_table_it_must_save_in_full(
    name: str, fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """A table with a record column cannot be saved in part, so a cell write
    saves its one StManAipsIO group in full: the byte order is kept (the
    `.big` fixture's is not the host's), and a second write through the same
    handle then works (#1034, the review's finding 6)."""
    path = tmp_path.resolve() / name
    shutil.copytree(fixtures["table"].parent / name, path)
    big_endian = stored_big_endian(path)
    # The `.big` fixture is big-endian; the other is the host's.
    assert big_endian == (name.endswith(".big") or sys.byteorder == "big")
    tool = tools.table()
    assert tool.open(str(path), nomodify=False)
    assert tool.putcell("label", 1, "new")
    assert tool.putcell("id", 2, 77)
    assert tool.putcol("gain", np.array([1 + 1j, 2 + 2j, 3 + 3j]))
    assert tool.putkeyword("added", 7)
    assert tool.close()
    assert stored_big_endian(path) == big_endian
    reader = tools.table()
    assert reader.open(str(path))
    assert reader.getcol("label").tolist() == ["alpha", "new", "gamma"]
    assert reader.getcell("id", 2) == 77
    assert reader.getcol("gain").tolist() == [1 + 1j, 2 + 2j, 3 + 3j]
    assert reader.getkeyword("added") == 7
    reader.done()


WRITER = """
import sys
from casars import tools

tool = tools.table()
assert tool.open(sys.argv[1], nomodify=False)
assert tool.putkeyword("counter", 2)
assert tool.putcell("id", 0, 41)
assert tool.close()
"""


@pytest.mark.parametrize("option", ["default", "auto", "user"])
def test_a_read_sees_what_another_process_wrote_since_the_lock_was_released(
    option: str, fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """A read takes the read lock again and synchronizes, as casacore's
    `TableProxy::syncTable` does: after an unlock another process writes the
    keywords and a cell, and the first handle, which had both loaded, reads
    the new values (#1034, the review's finding 2)."""
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    tool = tools.table()
    assert tool.open(str(path), lockoptions={"option": option}, nomodify=False)
    if option == "user":
        assert tool.lock()
    assert tool.putkeyword("counter", 1)
    original = tool.getcell("id", 0)
    assert tool.getkeyword("counter") == 1
    assert tool.unlock()
    assert not tool.haslock(write=False)

    subprocess.run([sys.executable, "-c", WRITER, str(path)], check=True, timeout=120)

    assert tool.getkeyword("counter") == 2
    assert tool.getcell("id", 0) == 41 != original
    assert tool.haslock(write=False)
    assert tool.close()


def test_readers_of_a_table_opened_nomodify_coexist_in_the_permanent_modes(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """A read-only open holds a read lock, not the write lock, even with
    permanent locking, as CASA's does (#1034, the review's finding 9)."""
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    readers = [tools.table(), tools.table()]
    for reader in readers:
        assert reader.open(str(path), lockoptions={"option": "permanent"}, nomodify=True)
        assert reader.haslock(write=False)
        assert not reader.haslock(write=True)
    assert [reader.getcell("id", 0) for reader in readers] == [1, 1]
    for reader in readers:
        reader.done()
    writer = tools.table()
    assert writer.open(str(path), lockoptions={"option": "permanent"}, nomodify=False)
    assert writer.haslock(write=True)
    writer.done()


def test_a_failed_write_leaves_the_table_as_it_was_however_it_ends(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """A write refused part way, in a mode where the table keeps its write
    lock, discards what it changed in memory and leaves no full rewrite for
    the close to make: on a MeasurementSet only `table.dat` may change."""
    path = tmp_path.resolve() / "mssel_test_small.ms"
    shutil.copytree(fixtures["ms"], path)
    before = table_files(path)
    tool = tools.table()
    assert tool.open(str(path), lockoptions={"option": "permanent"}, nomodify=False)
    assert tool.putcol("ANTENNA1", np.array([1, 2, 3]), 0, 3)
    with pytest.raises(RuntimeError, match="no such row"):
        tool.putcell("ANTENNA1", [0, 99999], 4)
    assert tool.getcol("ANTENNA1", nrow=3).tolist() == [1, 2, 3]
    assert tool.close()
    after = table_files(path)
    assert set(after) == set(before)
    assert {name for name in before if before[name] != after[name]} <= {"table.dat", "table.f14"}


CONVERSION_PROBE = """
import resource
import sys

import numpy as np
from casars import tools

tool = tools.table()
assert tool.open(sys.argv[1], nomodify=False)
assert tool.putkeyword("warm", 1)
data = np.ones((4, 64, 4000), dtype=np.complex128) * (1 + 2j)
unit = 1 if sys.platform == "darwin" else 1024  # ru_maxrss: bytes on macOS, else KiB
base = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * unit
assert tool.putkeyword("big", data)
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * unit
print(data.nbytes, peak - base)
"""


def test_a_large_array_is_copied_in_bulk_not_as_a_python_object_per_element(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """Giving the tool a 16 MB complex array, and saving it as a keyword,
    grows the process by 5 times the array (the copy, the encoded keyword
    record and the file's buffer). Expanding it into a Python object per
    element, with `tolist()` as the first version did, added 11 times the
    array more (#1034, the review's finding 13)."""
    path = tmp_path.resolve() / "mssel_test_small.ms"
    shutil.copytree(fixtures["ms"], path)
    done = subprocess.run(
        [sys.executable, "-c", CONVERSION_PROBE, str(path)],
        check=True,
        capture_output=True,
        text=True,
        timeout=300,
    )
    size, growth = (int(word) for word in done.stdout.split())
    assert growth < 8 * size, f"peak memory grew {growth / size:.1f} times the array"
    reader = tools.table()
    assert reader.open(str(path))
    assert reader.getkeyword("big").shape == (4, 64, 4000)
    reader.done()
