"""Tests for tablerow, getcoliter, #870, and #1038.

The 11 getcoliter and row cases from CASA's test_tool_table.py at
cee5117dce use a small table in place of n08c1_swap1.ms. It preserves
strided reads, variable channel counts, and four complex polarizations.
The two slice cases are in test_tools_table_slices.py.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import numpy as np
import pytest

from casars import tools


TILED_MULTI_FIXTURE = (
    Path(__file__).resolve().parents[4]
    / "libs/casacore/casa-tables"
    / "tests"
    / "data"
    / "tiled_data_stman"
    / "multi"
    / "big"
)


@pytest.fixture
def visibility_table(tmp_path: Path):
    path = str(tmp_path / "visibility.table")
    tb = tools.table()
    description = {
        "ANTENNA1": {"valueType": "int"},
        "DATA": {"valueType": "complex", "ndim": 2},
        "FLAG_ROW": {"valueType": "boolean"},
        "TIME": {"valueType": "double"},
    }
    assert tb.create(path, description, nrow=24)
    assert tb.putcol("TIME", np.arange(1, 25, dtype=np.float64) * 10)
    polarizations = np.arange(1, 5, dtype=np.float32)[:, None]
    for row in range(24):
        channels = 32 if row % 2 == 0 else 2
        visibility = np.full((4, channels), (row + 1) * (3 + 4j), dtype=np.complex64)
        assert tb.putcell("DATA", row, visibility * polarizations)
    tb.close()
    assert tb.open(path, nomodify=False)
    yield tb
    tb.done()


@pytest.fixture
def visibility_rows(visibility_table: tools.table):
    rows = visibility_table.row()
    yield rows
    rows.done()


def _create_sample_table(path: Path, nrow: int = 4) -> tools.table:
    tb = tools.table()
    desc = {
        "sb": {"valueType": "boolean"},
        "si": {"valueType": "int"},
        "sf": {"valueType": "float"},
        "sd": {"valueType": "double"},
        "sc": {"valueType": "complex"},
        "ss": {"valueType": "string"},
        "sr": {"valueType": "record"},
        "f1": {"valueType": "int", "ndim": 1, "shape": [2]},
        "v1": {"valueType": "float", "ndim": 1},
        "v2": {"valueType": "int", "ndim": 2},
        "vu": {"valueType": "double", "ndim": -1},
        "vs": {"valueType": "string", "ndim": 1},
    }
    assert tb.create(str(path), desc, nrow=nrow)
    if nrow >= 3:
        assert tb.putcol("si", [10 + 10 * i for i in range(nrow)])
        assert tb.putcol("sf", [1.5 + i for i in range(nrow)])
        assert tb.putcol("sd", [2.5 + i for i in range(nrow)])
        assert tb.putcol("ss", [f"r{i}" for i in range(nrow)])
        assert tb.putcol("sb", [i % 2 == 0 for i in range(nrow)])
        assert tb.putcol("sc", [complex(i + 1, -(i + 1)) for i in range(nrow)])
        assert tb.putcell("sr", 0, {"k": 7})
        assert tb.putcell("f1", 0, [7, 8])
        assert tb.putcell("v1", 0, np.array([1.0, 2.0], dtype=np.float32))
        assert tb.putcell("v1", 2, np.array([3.0, 4.0, 5.0], dtype=np.float32))
        assert tb.putcell("v2", 0, np.array([[1, 2], [3, 4]], dtype=np.int32))
        assert tb.putcell("vu", 0, np.array([9.0], dtype=np.float64))
        assert tb.putcell("vs", 0, np.array(["hi", "there"]))
    return tb


def test_tiled_data_stman_lazy_scalar_id_read_issue_1038(tmp_path: Path) -> None:
    copy_dir = tmp_path / "tiled_multi"
    shutil.copytree(TILED_MULTI_FIXTURE, copy_dir)

    tb = tools.table()
    assert tb.open(str(copy_dir))
    assert list(tb.getcol("id")) == [10, 10, 20, 20]
    assert [tb.getcell("id", row) for row in range(4)] == [10, 10, 20, 20]
    iter_vals = [int(arr.item()) for arr in tb.getcoliter("id")]
    assert iter_vals == [10, 10, 20, 20]
    row = tb.row(["id"])
    assert [row.get(r)["id"] for r in range(4)] == [10, 10, 20, 20]
    row.done()
    tb.close()


def test_undefined_array_cells_parity_issue_870(tmp_path: Path) -> None:
    tb = _create_sample_table(tmp_path / "undef.table", nrow=3)

    for col in ("v1", "v2", "vu", "vs"):
        assert not tb.iscelldefined(col, 1)
        with pytest.raises(RuntimeError, match="no array in row 1"):
            tb.getcell(col, 1)
        with pytest.raises(RuntimeError, match="no array in row 1"):
            tb.getcol(col)
        with pytest.raises(RuntimeError, match="no array in row 1"):
            tb.getcolshapestring(col)

    var_v1 = tb.getvarcol("v1")
    assert list(var_v1) == ["r1", "r2", "r3"]
    assert np.array_equal(var_v1["r1"], np.array([[1.0], [2.0]]))
    assert var_v1["r2"] is False
    assert np.array_equal(var_v1["r3"], np.array([[3.0], [4.0], [5.0]]))

    it_plain = list(tb.getcoliter("v1"))
    assert len(it_plain) == 3
    assert np.array_equal(it_plain[0], np.array([[1.0], [2.0]]))
    assert isinstance(it_plain[1], np.ndarray) and it_plain[1].shape == () and it_plain[1].dtype == bool
    assert bool(it_plain[1]) is False
    assert np.array_equal(it_plain[2], np.array([[3.0], [4.0], [5.0]]))

    it_rec = list(tb.getcoliter("v1", torecord=True))
    assert it_rec[1]["v1"].shape == () and bool(it_rec[1]["v1"]) is False

    r = tb.row()
    undef = r.get(1)
    assert undef["v1"].dtype == np.float64 and undef["v1"].shape == (0,)
    assert undef["v2"].dtype == np.int64 and undef["v2"].shape == (0, 0)
    assert undef["vu"].dtype == np.float64 and undef["vu"].shape == (0,)
    assert undef["vs"].dtype.kind == "U" and undef["vs"].shape == (0,)
    assert undef["sr"] == {}

    # Round-tripping row.get on an undefined row stores empty 0-sized arrays cleanly.
    assert r.put(1, undef)
    assert tb.iscelldefined("v1", 1)
    assert tb.getcolshapestring("v1", 1, 1) == ["[0]"]
    r.done()
    tb.close()


def test_getcoliter_semantics_and_edges(tmp_path: Path) -> None:
    empty = _create_sample_table(tmp_path / "empty.table", nrow=0)
    assert list(empty.getcoliter("si")) == []
    empty.close()

    tb = _create_sample_table(tmp_path / "iter.table", nrow=5)

    assert [int(x[0]) for x in tb.getcoliter("si", 1, 2, 2, False)] == [20, 40]
    assert [int(x["si"][0]) for x in tb.getcoliter(["si"], 1, 2, 2, True)] == [20, 40]

    multi_tuple = list(tb.getcoliter(["si", "v1"], 0, 3, 1, False))
    assert len(multi_tuple) == 3
    assert isinstance(multi_tuple[0], tuple)
    assert int(multi_tuple[0][0][0]) == 10
    assert np.array_equal(multi_tuple[0][1], np.array([[1.0], [2.0]]))
    assert multi_tuple[1][1].shape == () and bool(multi_tuple[1][1]) is False

    multi_rec = list(tb.getcoliter(["si", "v1"], 0, 2, 1, True))
    assert multi_rec[0]["si"].tolist() == [10]
    assert bool(multi_rec[1]["v1"]) is False

    assert list(tb.getcoliter([], 0, 2)) == [None, None]
    assert list(tb.getcoliter(123, 0, 2)) == [None, None]

    with pytest.raises(RuntimeError, match="column containing records"):
        tb.getcoliter("sr")

    assert list(tb.getcoliter("si", 0, 0, 1)) == []
    assert [int(x[0]) for x in tb.getcoliter("si", -2, 2, 1)] == [10, 20]
    assert list(tb.getcoliter("si", 5, 0, 1)) == []
    with pytest.raises(RuntimeError, match="no such row"):
        tb.getcoliter("si", 5, -1, 1)
    with pytest.raises(RuntimeError, match="no such row"):
        tb.getcoliter("si", 5, 1, 1)
    with pytest.raises(RuntimeError, match="rowincr<=0"):
        tb.getcoliter("si", 0, 2, 0)
    assert [int(x[0]) for x in tb.getcoliter("si", 0, 2, -1)] == [10, 20]

    overrun = tb.getcoliter("si", 0, 10, 1)
    with pytest.raises(RuntimeError, match="loading values from table failed"):
        list(overrun)

    # Works through selectrows views and stays valid after closing the parent tool.
    sub = tb.selectrows([3, 1])
    assert [int(x[0]) for x in sub.getcoliter("si")] == [40, 20]
    sub.close()

    held = tb.getcoliter("si", 0, 3, 1)
    tb.putcell("si", 1, 999)
    tb.close()
    assert [int(x[0]) for x in held] == [10, 999, 30]


def test_getcoliter_consistency(visibility_table: tools.table) -> None:
    tb = visibility_table
    values = tb.getcol("TIME", 0, 7, 3)
    iterator = tb.getcoliter("TIME", 0, 7, 3)
    assert all(values == iterator)
    with pytest.raises(StopIteration):
        next(iterator)

    iterator = tb.getcoliter("TIME")
    assert (iterator < 15).tolist() == [True]
    np.testing.assert_array_equal(next(iterator), [20])

    for row, chunk in zip(range(10), tb.getcoliter("DATA", 0, 10), strict=True):
        np.testing.assert_array_equal(chunk, tb.getcol("DATA", row, 1))

    columns = ["TIME", "DATA"]
    tuples = list(tb.getcoliter(columns, 0, 7, 3))
    records = list(tb.getcoliter(columns, 0, 7, 3, torecord=True))
    assert len(tuples) == len(records) == 7
    for values, record in zip(tuples, records, strict=True):
        assert type(values) is tuple and type(record) is dict
        assert set(record) == set(columns)
        for value, column in zip(values, columns, strict=True):
            np.testing.assert_array_equal(value, record[column])


def test_getcoliter_values(visibility_table: tools.table) -> None:
    magnitudes = [
        np.abs(chunk[:, :, 0]).sum(axis=1)
        for chunk in visibility_table.getcoliter("DATA", 0, 7, 3)
    ]
    # Each row has magnitudes 5 * row number (one-based) in the first
    # polarization, summed over its 32 or 2 channels.
    first_polarization = [160, 40, 1120, 100, 2080, 160, 3040]
    np.testing.assert_allclose(magnitudes, np.outer(first_polarization, [1, 2, 3, 4]))


def test_getcoliter_errors(visibility_table: tools.table) -> None:
    with pytest.raises(RuntimeError, match='column "data" does not exist'):
        visibility_table.getcoliter("data")
    with pytest.raises(RuntimeError, match="no opened table available"):
        tools.table().getcoliter("TIME")


def test_getcoliter_element_match(visibility_table: tools.table) -> None:
    tb = visibility_table
    values = tb.getcol(columnname="TIME", startrow=0, nrow=18, rowincr=3)
    iterator = tb.getcoliter(columnname="TIME", startrow=0, nrow=8, rowincr=3)
    assert values.shape == (8,)
    np.testing.assert_array_equal(np.concatenate(list(iterator)), values)
    with pytest.raises(StopIteration):
        next(iterator)


def test_tablerow_get(visibility_rows) -> None:
    assert abs(visibility_rows.get(20)["DATA"].sum()) == pytest.approx(33600)


def test_tablerow_shape(visibility_rows) -> None:
    assert visibility_rows.get(20)["DATA"].shape == (4, 32)


def test_tablerow_put(visibility_table: tools.table) -> None:
    tb = visibility_table
    rows = tb.row()
    original = rows.get(22)
    updated = np.full_like(original["DATA"], 1.0 / original["DATA"].size)
    assert rows.put(22, {"DATA": updated})
    rows.done()
    path = tb.name()
    tb.close()

    reader = tools.table()
    assert reader.open(path)
    rows = reader.row()
    stored = rows.get(22)
    np.testing.assert_array_equal(stored["DATA"], updated)
    assert abs(stored["DATA"].sum()) == pytest.approx(1)
    assert stored["TIME"] == original["TIME"]
    rows.done()
    reader.done()


def test_tablerow_all_rows(visibility_rows) -> None:
    rows = visibility_rows[:]
    assert len(rows) == 24
    assert sum(abs(row["DATA"].sum()) for row in rows) == pytest.approx(246000)


def test_tablerow_some_rows(visibility_rows) -> None:
    rows = visibility_rows[:15]
    assert len(rows) == 15
    assert sum(abs(row["DATA"].sum()) for row in rows) == pytest.approx(108000)


def test_tablerow_columnnames_include(visibility_table: tools.table) -> None:
    rows = visibility_table.row(columnnames=["DATA"])
    assert [list(row) for row in rows[:5]] == [["DATA"]] * 5
    rows.done()


def test_tablerow_columnnames_exclude(visibility_table: tools.table) -> None:
    rows = visibility_table.row(columnnames=["DATA"], exclude=True)
    assert set(rows.get(0)) == {"ANTENNA1", "FLAG_ROW", "TIME"}
    rows.done()


def test_tablerow_get_put_and_lifecycle(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR, logger="casars.tools.table")
    closed = tools.table()
    with pytest.raises(RuntimeError, match="row access from unitialized table"):
        closed.row()
    assert any("Row access from unitialized table" in rec.message for rec in caplog.records)

    with pytest.raises(RuntimeError, match="invalid table passed for parameter one"):
        tools.tablerow()
    with pytest.raises(RuntimeError, match="invalid table passed for parameter one"):
        tools.tablerow(closed)

    tb = _create_sample_table(tmp_path / "row.table", nrow=4)

    with pytest.raises(RuntimeError, match="column missing is unknown"):
        tb.row(["missing"])
    with pytest.raises(RuntimeError, match="already has been defined"):
        tb.row(["si", "si"])

    excl = tb.row(["si", "missing_ignored"], exclude=True)
    assert "si" not in excl.get(0) and "sf" in excl.get(0)
    excl.done()

    direct = tools.tablerow(tb, ["si", "sd"])
    assert len(direct) == 4
    assert direct.iswritable() is True
    assert direct.get(0) == {"sd": 2.5, "si": 10}
    direct.done()

    r = tb.row(["si", "sf", "f1", "sr"])
    assert r[0]["si"] == 10 and r[0]["sf"] == 1.5 and np.array_equal(r[0]["f1"], [7, 8])
    assert r[0]["sr"] == {"k": 7}
    assert r[-1]["si"] == 40
    assert [item["si"] for item in r[0:2]] == [10, 20]
    assert [item["si"] for item in r[::-1]] == [40, 30, 20, 10]
    assert [item["si"] for item in r[[3, 1]]] == [40, 20]
    assert [item["si"] for item in r[np.array([3, 1])]] == [40, 20]
    assert r[np.int64(0)]["si"] == 10
    with pytest.raises(IndexError):
        _ = r[10]

    # Record column read/write through tablerow (strictly requires dict).
    assert r.put(1, {"sr": {"nested": [1, 2], "tag": "ok"}}, matchingfields=True)
    assert r.get(1)["sr"]["tag"] == "ok" and list(r.get(1)["sr"]["nested"]) == [1, 2]
    with pytest.raises(RuntimeError, match="incorrect data type used for field sr"):
        r.put(1, {"sr": 42}, matchingfields=True)

    # matchingfields=True allows subsets and ignores extras, but enforces strict types.
    assert r.put(0, {"si": 101, "extra": "ignored"}, matchingfields=True)
    assert r.put(0, {"sf": 5}, matchingfields=True)
    assert r.get(0)["si"] == 101 and r.get(0)["sf"] == 5.0
    with pytest.raises(RuntimeError, match=r"RecordInterface::asInt - invalid data type"):
        r.put(0, {"si": 5.5}, matchingfields=True)
    with pytest.raises(RuntimeError, match=r"RecordInterface::asInt - invalid data type"):
        r.put(0, {"si": "bad"}, matchingfields=True)
    with pytest.raises(RuntimeError, match="Table array conformance error"):
        r.put(0, {"f1": [1, 2, 3]}, matchingfields=True)

    # matchingfields=False requires exact set match.
    with pytest.raises(RuntimeError, match="TableRow::put; names not conforming"):
        r.put(0, {"si": 1}, matchingfields=False)
    with pytest.raises(RuntimeError, match="TableRow::put; names not conforming"):
        r.put(
            0,
            {"si": 1, "sf": 2.0, "f1": [1, 2], "sr": {"x": 1}, "extra": 3},
            matchingfields=False,
        )
    assert r.put(
        0, {"si": 11, "sf": 2.25, "f1": [9, 10], "sr": {"k": 8}}, matchingfields=False
    )
    assert (
        r.get(0)["si"] == 11
        and np.array_equal(r.get(0)["f1"], [9, 10])
        and r.get(0)["sr"] == {"k": 8}
    )

    with pytest.raises(RuntimeError, match="does not exist"):
        r.get(-1)
    with pytest.raises(RuntimeError, match="does not exist"):
        r.put(4, {"si": 1})

    # selectrows views translate row indices on get and put.
    sub = tb.selectrows([2, 0])
    sr = sub.row(["si"])
    assert len(sr) == 2
    assert sr.get(0) == {"si": 30}
    assert sr.put(0, {"si": 300})
    assert tb.getcell("si", 2) == 300
    sr.done()
    sub.close()

    # Closing or reopening the parent invalidates active tablerow handles.
    tb.close()
    for call in (r.iswritable, lambda: r.get(0), lambda: r.put(0, {"si": 1}), lambda: len(r)):
        with pytest.raises(RuntimeError, match="use of uninitialized table row"):
            call()
    assert r.done() is None
    assert r.done() is None

    # Read-only open rejects tablerow.put.
    ro = tools.table()
    assert ro.open(str(tmp_path / "row.table"), nomodify=True)
    rr = ro.row(["si"])
    assert rr.iswritable() is False
    with pytest.raises(RuntimeError, match="not writable"):
        rr.put(0, {"si": 1})
    rr.done()
    ro.close()
