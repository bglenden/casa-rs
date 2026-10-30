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
        archive.extractall(root / "ms", filter="data")
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
        assert got["message"] == want["message"], where


def run_step(tool: Any, step: dict[str, Any], path: str) -> dict[str, Any]:
    args = [decode_value(arg, path) for arg in step["args"]]
    kwargs = {name: decode_value(value, path) for name, value in step["kwargs"].items()}
    try:
        return {"value": encode_value(getattr(tool, step["method"])(*args, **kwargs), path)}
    except Exception as error:  # noqa: BLE001 - compared with what CASA raised
        return {"error": {"type": type(error).__name__, "message": str(error).replace(path, PLACEHOLDER)}}


REPLAYED = sorted(set(RECORD["scenarios"]) - {"casa_bugs"})


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
