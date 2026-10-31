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
    if type(value).__name__ == "table":
        return {"tool": True}
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


def run_step(
    tool: Any, step: dict[str, Any], path: str, bound: dict[str, Any] | None = None
) -> dict[str, Any]:
    """One recorded step on `tool`, or on the tool the step's `on` names; the
    tool it returns is kept under the step's `bind`, in `bound`."""
    args = [decode_value(arg, path) for arg in step["args"]]
    kwargs = {name: decode_value(value, path) for name, value in step["kwargs"].items()}
    options = step.get("options", {})
    target = bound[options["on"]] if "on" in options else tool
    try:
        value = getattr(target, step["method"])(*args, **kwargs)
        if "bind" in options:
            bound[options["bind"]] = value
        return {"value": encode_value(value, path)}
    except Exception as error:  # noqa: BLE001 - compared with what CASA raised
        return {"error": {"type": type(error).__name__, "message": str(error).replace(path, PLACEHOLDER)}}


def same_cells_as_getcell(
    tool: Any, step: dict[str, Any], value: dict[str, Any], path: str, where: str
) -> None:
    """CASA cannot return a `getcol` of an `int64` column, so the array
    casa-rs returns is checked against the same rows read cell by cell
    (`getcell` of that column is CASA's and recorded): its shape and every
    value, not only its dtype."""
    column = decode_value(step["args"][0], path)
    kwargs = {name: decode_value(item, path) for name, item in step["kwargs"].items()}
    first, count, step_ = kwargs.get("startrow", 0), kwargs.get("nrow", -1), kwargs.get("rowincr", 1)
    last = tool.nrows() if count < 0 else min(tool.nrows(), first + count * step_)
    rows = range(first, last, step_)
    expected = [tool.getcell(column, row) for row in rows]
    assert value["shape"] == [len(expected)], where
    assert value["data"] == expected, where


#: Recorded from CASA but not implemented yet; a scenario leaves this set as
#: its methods land. (`rows_columns`, every structural call in one sequence,
#: waits for nothing now.)
PENDING: set[str] = set()

#: Recorded from CASA but not followed: its own faults
#: (`docs/CASA (C++) bugs.md`), each with a test of its own below.
CASA_BUGS: set[str] = {"casa_bugs", "int_overflow", "addrows_negative", "structure_partial"}

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
    bound: dict[str, Any] = {}
    for index, step in enumerate(recorded["steps"]):
        where = f"{scenario}[{index}] {step['method']}{tuple(step['args'])} {step['kwargs']}"
        got = run_step(tool, step, path, bound)
        if "error" in step:
            if step["error"]["message"] == "Unknown casa DataType!":
                assert "value" in got and got["value"]["ndarray"] == "int64", where
                same_cells_as_getcell(tool, step, got["value"], path, where)
            else:
                assert "error" in got, f"{where}: CASA raised {step['error']}, casa-rs gave {got}"
                same_error(got["error"], step["error"], where)
        else:
            assert "value" in got, f"{where}: CASA gave a value, casa-rs raised {got['error']}"
            same_value(got["value"], step["value"], where)
    tool.done()
    for other in bound.values():
        other.done()


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


@pytest.mark.parametrize("option", ["auto", "permanent", "user"])
def test_columns_renamed_or_removed_change_table_dat_and_the_files_of_removed_managers_only(
    option: str, fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """On a MeasurementSet, CASA changes `table.dat` alone to rename a column
    and also deletes the files of a storage manager left with no column to
    remove one (`tools/casa-compat` probe on the mini, 2026-10-31); no row is
    rewritten. casa-rs does the same in every lock mode, and a reader sees the
    columns under their new names (#1025)."""
    path = tmp_path.resolve() / "mssel_test_small.ms"
    shutil.copytree(fixtures["ms"], path)
    before = table_files(path)
    reader = tools.table()
    assert reader.open(str(path))
    antenna1, data = reader.getcol("ANTENNA1"), reader.getcell("DATA", 3)
    reader.done()

    tool = tools.table()
    assert tool.open(str(path), lockoptions={"option": option}, nomodify=False)
    if option == "user":
        assert tool.lock()
    assert tool.renamecol("ANTENNA1", "ANT1")
    assert tool.renamecol("DATA", "DATA2")
    assert tool.renamecol("FLAG_ROW", "FR")
    assert tool.close()
    renamed = table_files(path)
    assert set(renamed) == set(before)
    assert {name for name in before if before[name] != renamed[name]} == {"table.dat"}
    reader = tools.table()
    assert reader.open(str(path))
    assert "ANTENNA1" not in reader.colnames() and "ANT1" in reader.colnames()
    assert (reader.getcol("ANT1") == antenna1).all()
    assert (reader.getcell("DATA2", 3) == data).all()
    reader.done()

    tool = tools.table()
    assert tool.open(str(path), lockoptions={"option": option}, nomodify=False)
    if option == "user":
        assert tool.lock()
    assert tool.removecols(["DATA2", "FR"])
    assert tool.close()
    removed = table_files(path)
    gone = set(renamed) - set(removed)
    assert gone == {
        "table.f17", "table.f17_TSM1", "table.f17_TSM2",  # DATA's tiled manager
        "table.f6",  # FLAG_ROW's standard manager
    }, sorted(gone)
    assert set(removed) <= set(renamed)
    kept = {name for name in removed if removed[name] != renamed[name]}
    assert kept == {"table.dat"}, sorted(kept)
    reader = tools.table()
    assert reader.open(str(path))
    assert "DATA2" not in reader.colnames() and "FR" not in reader.colnames()
    assert (reader.getcol("ANT1") == antenna1).all()
    assert reader.nrows() == 1058
    reader.done()


def test_a_table_of_several_managers_refuses_rows_before_anything_changes(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """casa-tables cannot change the rows of a MeasurementSet in place, and a
    full rewrite would change its layout: `addrows` is not implemented for it
    (`NotImplementedError`), and `removerows` says what CASA says, since its
    incremental and tiled managers cannot remove rows. Every file is as it was
    and nothing was loaded to find out (#1025)."""
    path = tmp_path.resolve() / "mssel_test_small.ms"
    shutil.copytree(fixtures["ms"], path)
    before = table_files(path)
    tool = tools.table()
    assert tool.open(str(path), nomodify=False)
    with pytest.raises(NotImplementedError, match="StManAipsIO group"):
        tool.addrows(2)
    with pytest.raises(NotImplementedError, match="StManAipsIO group"):
        tool.addcols({"extra": {"valueType": "double"}})
    with pytest.raises(NotImplementedError, match="getdminfo.*IncrementalStMan|getdminfo.*Tiled"):
        tool.getdminfo()
    with pytest.raises(RuntimeError, match="Rows cannot be removed from table .*do not support it"):
        tool.removerows([0])
    assert tool.nrows() == 1058
    assert tool.close()
    assert table_files(path) == before


@pytest.mark.parametrize("option", ["default", "auto", "user", "permanent"])
def test_rows_removed_from_a_table_are_gone_for_a_handle_that_opens_it_afterwards(
    option: str, fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """A table written in full after rows were removed must not be left
    claiming the rows it had: the handle that wrote it keeps the lock (`user`,
    `permanent`) or has released it, and a second handle opening the table at
    once sees the rows that are there, before and after the first closes. The
    rows last published in `table.lock` are the old count until the release;
    a flush at the release used to raise `table.dat` back to it (#1025)."""
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    tool = tools.table()
    assert tool.open(str(path), lockoptions={"option": option}, nomodify=False)
    if option == "user":
        assert tool.lock()
    assert tool.addrows(2)
    assert tool.putcell("id", 4, 44)
    assert tool.removerows([0, 3])
    assert tool.nrows() == 3
    if option == "user":
        assert tool.unlock()
    elif option == "permanent":
        pass  # the lock stays held until close
    if option != "permanent":
        reader = tools.table()
        assert reader.open(str(path))
        assert reader.nrows() == 3
        assert [reader.getcell("id", row) for row in range(3)] == [2, 3, 44]
        reader.done()
    assert tool.close()
    reader = tools.table()
    assert reader.open(str(path))
    assert reader.nrows() == 3
    assert [reader.getcell("id", row) for row in range(3)] == [2, 3, 44]
    reader.done()


def test_addrows_with_a_negative_count_is_refused_where_casa_removes_rows(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """CASA 6.7.6.14 takes the count as an unsigned number that wraps:
    `addrows(-1)` removes the last row and `addrows(-2)` two
    (`docs/CASA (C++) bugs.md`). casa-rs refuses and changes nothing."""
    counts = [
        step["value"]["int"]
        for step in RECORD["scenarios"]["addrows_negative"]["steps"]
        if step["method"] == "nrows"
    ]
    assert counts == [3, 2, 0, 0], "CASA: 3 rows, after addrows(-1) 2, after addrows(-2) 0"
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    before = table_files(path)
    tool = tools.table()
    assert tool.open(str(path), nomodify=False)
    with pytest.raises(RuntimeError, match="must not be negative"):
        tool.addrows(-1)
    assert tool.nrows() == 3
    assert tool.close()
    assert table_files(path) == before


def test_a_structural_call_that_would_fail_part_way_changes_nothing_where_casa_changes_part(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """CASA does the part of a failing call before the failure: `removerows(
    [2, 2])` removes the first row 2 and then raises, `addcols` adds the
    columns that sort before the one that exists and then raises (Astra's
    review of #1036, finding 6; `docs/CASA (C++) bugs.md`). casa-rs raises the
    same errors and leaves the table as it was: the whole call is checked
    first."""
    steps = RECORD["scenarios"]["structure_partial"]["steps"]
    recorded_rows = [step["value"]["int"] for step in steps if step["method"] == "nrows"]
    assert recorded_rows == [3, 2], "CASA left two rows"
    recorded_columns = next(
        step["value"]["list"] for step in steps if step["method"] == "colnames"
    )
    assert {"str": "aaa"} in recorded_columns, "CASA added the column before the failing one"

    path = str((tmp_path / "python_fixture.table").resolve())
    shutil.copytree(fixtures["table"], path)
    before = table_files(Path(path))
    tool = tools.table()
    assert tool.open(path, nomodify=False)
    for step in steps:
        if step["method"] in ("removerows", "addcols"):
            got = run_step(tool, step, path)
            assert "error" in got, step
            same_error(got["error"], step["error"], step["method"])
    assert tool.nrows() == 3
    assert "aaa" not in tool.colnames()
    assert tool.close()
    assert table_files(Path(path)) == before


@pytest.mark.parametrize("option", ["default", "auto", "user"])
def test_a_reader_sees_the_columns_another_process_renamed_and_removed(
    option: str, fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """After another process renames a column and removes another, a handle
    that synchronizes on its next read describes the managers as they now are:
    `getcoldesc` of the new name names its manager, and `getdminfo` lists the
    new columns and not the old ones (the lock's reload took the schema and
    the rows but not the managers; Astra's review of #1036, finding 7)."""
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    tool = tools.table()
    assert tool.open(str(path), lockoptions={"option": option})
    if option == "user":
        assert tool.lock(write=False)
    assert tool.getcoldesc("id")["dataManagerType"] == "StManAipsIO"
    assert "id" in tool.getdminfo()["*1"]["COLUMNS"]
    assert tool.unlock()  # the other process needs the write lock

    subprocess.run(
        [sys.executable, "-c", STRUCTURE_WRITER, str(path)], check=True, timeout=120
    )

    assert tool.getcoldesc("identifier")["dataManagerType"] == "StManAipsIO"
    columns = list(tool.getdminfo()["*1"]["COLUMNS"])
    assert "identifier" in columns
    assert "id" not in columns and "gain" not in columns
    assert tool.close()


STRUCTURE_WRITER = """
import sys
from casars import tools

tool = tools.table()
assert tool.open(sys.argv[1], nomodify=False)
assert tool.renamecol("id", "identifier")
assert tool.removecols(["gain"])
assert tool.close()
"""


@pytest.mark.parametrize("option", ["user", "permanent"])
def test_writes_after_a_row_removal_keep_the_rows_removed_under_the_same_lock(
    option: str, fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """A full rewrite that removes rows, then a keyword, a cell, a rename and
    an added column, all under the write lock a `user` or `permanent` handle
    keeps: each of the later writes read the published row count back, which
    still held the old count and raised `table.dat` to it, so that the table
    claimed rows its manager lacked (Astra's review of #1036, finding 1). CASA
    does the same sequence without trouble."""
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    # A first lock period that writes, so that `table.lock` publishes three rows.
    warm = tools.table()
    assert warm.open(str(path), nomodify=False)
    assert warm.putkeyword("warm", 1)
    assert warm.close()

    tool = tools.table()
    assert tool.open(str(path), lockoptions={"option": option}, nomodify=False)
    if option == "user":
        assert tool.lock()
    assert tool.removerows([2])
    assert tool.nrows() == 2
    assert tool.putkeyword("k", 1)
    assert tool.nrows() == 2
    assert tool.putcell("id", 0, 9)
    assert tool.renamecol("label", "name")
    assert tool.addcols({"extra": {"valueType": "int"}})
    assert tool.nrows() == 2
    assert tool.getcol("name").tolist() == ["alpha", "beta"]
    if option == "user":
        assert tool.unlock()
    assert tool.close()

    reader = tools.table()
    assert reader.open(str(path))
    assert reader.nrows() == 2
    assert reader.getcell("id", 0) == 9
    assert reader.getkeyword("k") == 1
    assert reader.getcol("name").tolist() == ["alpha", "beta"]
    assert "extra" in reader.colnames()
    reader.done()


HOLDER = """
import sys
import time
from casars import tools

tool = tools.table()
assert tool.open(sys.argv[1], nomodify=False, lockoptions={"option": "permanent"})
print("held", flush=True)
time.sleep(float(sys.argv[2]))
"""


@pytest.mark.parametrize("option", ["default", "auto", "user"])
def test_datachanged_is_true_once_after_another_process_has_written(
    option: str, fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """The modify counter that `datachanged` compares moves when another
    process releases a write lock: the answer is True once, then False
    (recorded for one handle by `state`; two processes cannot be replayed)."""
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    tool = tools.table()
    assert tool.open(str(path), lockoptions={"option": option}, nomodify=False)
    tool.datachanged()
    assert not tool.datachanged()
    assert tool.unlock()
    subprocess.run([sys.executable, "-c", WRITER, str(path)], check=True, timeout=120)
    assert tool.datachanged()
    assert not tool.datachanged()
    tool.done()


def test_datachanged_is_true_while_another_process_holds_the_write_lock(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """Another process writing means the data is changing, as CASA says
    (`Table::hasDataChanged` gives up its try for a read lock)."""
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    tool = tools.table()
    assert tool.open(str(path), nomodify=False, lockoptions={"option": "user"})
    tool.datachanged()
    assert not tool.datachanged()
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(path), "4"], stdout=subprocess.PIPE, text=True
    )
    try:
        assert holder.stdout is not None and holder.stdout.readline().strip() == "held"
        assert tool.datachanged()
        assert not tool.haslock(write=False)
    finally:
        holder.wait(timeout=60)
    tool.done()


def test_a_copy_is_the_directory_of_the_table_and_leaves_the_source_alone(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """Without values or rows asked for, `copy` copies the table's files as
    they are, whatever its storage managers: a MeasurementSet's copy has the
    same files with the same bytes."""
    source = tmp_path.resolve() / "mssel_test_small.ms"
    shutil.copytree(fixtures["ms"], source)
    before = table_files(source)
    tool = tools.table()
    assert tool.open(str(source))
    copied = tool.copy(str(tmp_path.resolve() / "copy.ms"), deep=True)
    assert copied.name() == str(tmp_path.resolve() / "copy.ms")
    assert copied.nrows() == tool.nrows()
    assert not copied.iswritable()
    assert table_files(source) == before
    got = table_files(tmp_path.resolve() / "copy.ms")
    for name, content in before.items():
        if name != "table.lock":
            assert got[name] == content, name
    copied.done()
    tool.done()


@pytest.mark.parametrize("options", [{"valuecopy": True}, {"norows": True}])
def test_a_copy_that_would_write_a_table_of_several_managers_afresh_changes_nothing(
    options: dict[str, bool], fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """casa-tables cannot write the MeasurementSet's managers afresh: the
    copy is refused before the target is touched (a table already at the
    target stays)."""
    source = tmp_path.resolve() / "mssel_test_small.ms"
    shutil.copytree(fixtures["ms"], source)
    target = tmp_path.resolve() / "target.ms"
    shutil.copytree(fixtures["ms"], target)
    kept = table_files(target)
    tool = tools.table()
    assert tool.open(str(source))
    with pytest.raises(NotImplementedError, match="not one StManAipsIO group"):
        tool.copy(str(target), **options)
    with pytest.raises(NotImplementedError, match="not one StManAipsIO group"):
        tool.copy(str(tmp_path.resolve() / "new.ms"), **options)
    assert table_files(target) == kept
    assert not (tmp_path / "new.ms").exists()
    tool.done()


@pytest.mark.parametrize(
    "options",
    [{"memorytable": True}, {"dminfo": {"*1": {"TYPE": "StandardStMan", "NAME": "S", "COLUMNS": []}}}],
)
def test_a_copy_to_memory_or_with_new_managers_is_not_implemented(
    options: dict[str, Any], fixtures: dict[str, Path], tmp_path: Path
) -> None:
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    tool = tools.table()
    assert tool.open(str(path))
    with pytest.raises(NotImplementedError):
        tool.copy(str(tmp_path.resolve() / "new.table"), **options)
    assert not (tmp_path / "new.table").exists()
    tool.done()


def test_a_copy_replaces_a_table_at_the_target_and_refuses_a_file(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    tool = tools.table()
    assert tool.open(str(path))
    target = tmp_path.resolve() / "target.table"
    tool.copy(str(target), norows=True).done()
    assert tools.table().open(str(target))
    again = tool.copy(str(target))
    assert again.nrows() == 3
    again.done()
    plain_file = tmp_path.resolve() / "plain"
    plain_file.write_text("not a table")
    with pytest.raises(RuntimeError, match="not a true table directory"):
        tool.copy(str(plain_file))
    assert plain_file.read_text() == "not a table"
    tool.done()


def test_a_copy_without_rows_keeps_the_group_name_of_a_stman_aipsio_table(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """The manager's group, as `getcoldesc` reports it, is kept when the
    table is written afresh (the library's shallow copy dropped it)."""
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    tool = tools.table()
    assert tool.open(str(path))
    expected = tool.getcoldesc("label")["dataManagerGroup"]
    copied = tool.copy(str(tmp_path.resolve() / "empty.table"), norows=True)
    assert copied.nrows() == 0
    assert copied.getcoldesc("label")["dataManagerGroup"] == expected
    copied.done()
    tool.done()


def test_copyrows_into_a_table_of_several_managers_refuses_before_a_cell_is_read(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """Its rows cannot be added in place yet (#1037): the output table is left
    as it is, and so is the input."""
    ms = tmp_path.resolve() / "mssel_test_small.ms"
    shutil.copytree(fixtures["ms"], ms)
    before = table_files(ms)
    tool = tools.table()
    assert tool.open(str(ms))
    with pytest.raises(NotImplementedError, match="not one StManAipsIO group"):
        tool.copyrows(str(ms))
    assert table_files(ms) == before
    other = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], other)
    # No column in common: nothing to copy, nothing refused.
    assert tool.copyrows(str(other))
    tool.done()


def test_copyrows_into_a_table_another_tool_has_open_does_not_hang(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """CASA shares one table between its tools in a process; casa-rs's tools
    each have their own handle, and an automatic handle keeps the read lock
    it took for its last read, which the write of the copy would wait for
    for ever. The copy must end, here with a refusal (run in a process of
    its own, so that a hang is a failed test, not a stuck one)."""
    source = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], source)
    out = tmp_path.resolve() / "out.table"
    maker = tools.table()
    assert maker.open(str(source))
    maker.copy(str(out), norows=True).done()
    maker.done()
    script = (
        "import sys\nfrom casars import tools\n"
        "t = tools.table(); assert t.open(sys.argv[1], nomodify=False)\n"
        "out = tools.table(); assert out.open(sys.argv[2])\n"
        "assert out.nrows() == 0\n"
        "try:\n    print(t.copyrows(sys.argv[2]))\nexcept Exception as e:\n"
        "    print(type(e).__name__, e)\n"
        "print(out.nrows())\n"
    )
    done = subprocess.run(
        [sys.executable, "-c", script, str(source), str(out)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert done.returncode == 0, done.stderr
    # (The other handle holds its read lock and does not see the rows until
    # it takes the lock again: CASA's tools of one process share a table.)
    assert done.stdout.split()[0] == "True", done.stdout


def test_a_copy_removes_a_table_or_an_empty_directory_at_the_target_and_nothing_else(
    fixtures: dict[str, Path], tmp_path: Path
) -> None:
    """casacore removes whatever directory is at the target; a slip of the name
    must not delete a directory of other files, so casa-rs refuses one that is
    no table, and a copy that lies inside the table (or the table inside it)."""
    path = tmp_path.resolve() / "python_fixture.table"
    shutil.copytree(fixtures["table"], path)
    tool = tools.table()
    assert tool.open(str(path))
    other = tmp_path.resolve() / "notes"
    other.mkdir()
    (other / "keep.txt").write_text("keep")
    with pytest.raises(RuntimeError, match="not a true table directory"):
        tool.copy(str(other))
    assert (other / "keep.txt").read_text() == "keep"
    empty = tmp_path.resolve() / "empty"
    empty.mkdir()
    tool.copy(str(empty)).done()
    assert (empty / "table.dat").is_file()
    with pytest.raises(RuntimeError, match="one inside the other"):
        tool.copy(str(path / "inside.table"))
    with pytest.raises(RuntimeError, match="one inside the other"):
        tool.copy(str(tmp_path.resolve()))
    assert (path / "table.dat").is_file() and not (path / "inside.table").exists()
    tool.done()
