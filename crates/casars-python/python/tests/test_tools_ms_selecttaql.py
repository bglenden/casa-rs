"""Pinned CASA behavior for direct MeasurementSet TaQL selection."""

import shutil

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


def test_ms_selecttaql_defaults_and_argument_types(native_dataset):
    casa = pinned_casa()
    for api in (casa, tools):
        selector = api.ms()
        assert selector.selecttaql("ANTENNA1 == 1") is False
        assert selector.open(str(native_dataset))
        rows = selector.nrow(True)
        assert selector.msselect({"field": "0", "scan": "0"}, onlyparse=True)
        assert selector.selecttaql() is False
        indices = selector.msselectedindices()
        np.testing.assert_array_equal(indices["field"], np.array([0], dtype=np.int64), strict=True)
        assert indices["scan"].shape == (0,)
        assert selector.selecttaql("") is False
        for invalid in (None, 0, ["ANTENNA1 == 1"]):
            with pytest.raises(TypeError, match="argument _msselect must be a string"):
                selector.selecttaql(invalid)
        assert selector.nrow(True) == rows
        assert selector.selecttaql(msselect=b"ANTENNA1 == 1")
        assert 0 < selector.nrow(True) < rows
        selected = selector.nrow(True)
        assert selector.selecttaql(np.str_("ANTENNA1 == 1"))
        assert selector.nrow(True) == selected
        assert selector.done()


def test_ms_selecttaql_composes_current_rows_and_binds_empty_results(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    selected = np.flatnonzero(raw["antenna1"] == 1)
    for api in (casa, tools):
        selector = api.ms()
        assert selector.open(str(native_dataset))
        assert selector.selectinit()
        assert selector.selecttaql("ANTENNA1 == 1")
        np.testing.assert_array_equal(selector.getdata("data")["data"], raw["data"][..., selected], strict=True)
        assert selector.selecttaql("ROWID() IN [2, 0, 1, 2, 999]")
        assert selector.selecttaql("TRUE")
        np.testing.assert_array_equal(selector.getdata("data")["data"], raw["data"][..., selected[:3]], strict=True)
        assert selector.selecttaql("1 >= ROWID()")
        np.testing.assert_array_equal(selector.getdata("data")["data"], raw["data"][..., selected[:2]], strict=True)
        assert selector.selecttaql("FALSE") is False
        assert selector.nrow(True) == 0
        assert selector.selecttaql("TRUE") is False
        assert not selector.getdata("data")
        assert selector.reset()
        assert selector.nrow(True) == raw["data"].shape[-1]
        assert selector.selecttaql("ROWID() < 3")
        assert selector.selecttaql("TRUE")
        assert selector.nrow(True) == 3
        assert selector.done()


def test_ms_selecttaql_reports_parse_and_scalar_errors_without_changing_rows(native_dataset):
    casa = pinned_casa()
    for api in (casa, tools):
        selector = api.ms()
        assert selector.open(str(native_dataset))
        assert selector.msselect({"field": "0"}, onlyparse=True)
        assert selector.selecttaql("ANTENNA1 == 1")
        selected = selector.nrow(True)
        for bare in ("ANTENNA1", "FLAG"):
            with pytest.raises(RuntimeError, match="not (Bool|a Boolean) scalar"):
                selector.selecttaql(bare)
            assert selector.nrow(True) == selected
            np.testing.assert_array_equal(selector.msselectedindices()["field"], [0])
        for invalid in (" ", "ANTENNA1 == ", "NO_SUCH_COLUMN > 0", "ROWID == 2", "t", "f"):
            with pytest.raises(RuntimeError, match="Error in select expression") as failed:
                selector.selecttaql(invalid)
            with pytest.raises(RuntimeError) as indices:
                selector.msselectedindices()
            assert str(indices.value) == str(failed.value)
            assert selector.nrow(True) == selected
        assert selector.selecttaql("TRUE")
        np.testing.assert_array_equal(selector.msselectedindices()["field"], [0])
        assert selector.done()


def test_ms_selecttaql_preserves_saved_reference_order(baseline_fixture):
    casa, make_view, raw = baseline_fixture
    physical = np.array([29, 2, 4, 13, 0, 2])
    path = make_view("taql-order.ms", physical)
    selected = physical[raw["antenna1"][physical] != 2]
    for api in (casa, tools):
        selector = api.ms()
        assert selector.open(path)
        assert selector.selecttaql("ANTENNA1 != 2")
        np.testing.assert_array_equal(selector.getdata("data")["data"], raw["data"][..., selected], strict=True)
        assert selector.done()


def test_ms_selecttaql_accepts_uppercase_boolean_literals(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    for api in (casa, tools):
        selector = api.ms()
        assert selector.open(str(native_dataset))
        assert selector.selecttaql("FLAG_ROW == F")
        assert selector.selecttaql("T")
        got = selector.getdata("data")["data"]
        np.testing.assert_array_equal(got, raw["data"][..., ~raw["flag_row"]], strict=True)
        assert selector.selecttaql("F") is False
        assert selector.nrow(True) == 0
        assert selector.done()


def test_ms_selecttaql_distinguishes_rowid_column_from_function(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    path = str(native_dataset)
    table = casa.table()
    assert table.open(path, nomodify=False)
    assert table.addcols({"ROWID": table.getcoldesc("ANTENNA1")})
    values = np.full(raw["data"].shape[-1], -1, dtype=np.int32)
    values[7] = 2
    assert table.putcol("ROWID", values)
    assert table.done()
    for api in (casa, tools):
        selector = api.ms()
        assert selector.open(path)
        for predicate, row in (("ROWID == 2", 7), ("ROWID() == 2", 2)):
            assert selector.selecttaql(predicate)
            result = selector.getdata("data")["data"]
            np.testing.assert_array_equal(result, raw["data"][..., row:row + 1], strict=True)
            assert selector.reset()
        assert selector.done()


@pytest.mark.parametrize("method,predicate,live", [
    ("selecttaql", "TRUE", True),
    ("selecttaql", "1 == 1", True),
    ("msselect", "TRUE", True),
    ("selecttaql", "ROWID() < 30", False),
    ("selectinit", None, False),
])
def test_ms_taql_retains_live_and_fixed_views_after_append(
    baseline_fixture, native_dataset, tmp_path, method, predicate, live,
):
    casa, _, raw = baseline_fixture
    count = raw["data"].shape[-1]
    for api in (casa, tools):
        path = str(tmp_path / f"{api.__name__}.ms")
        shutil.copytree(native_dataset, path)
        selector = api.ms()
        assert selector.open(path)
        if method == "selecttaql":
            assert selector.selecttaql(predicate)
        elif method == "msselect":
            assert selector.msselect({"taql": predicate})
        else:
            assert selector.selectinit()
        if not live:
            assert selector.selecttaql("TRUE")
        assert selector.nrow(True) == count

        writer = casa.table()
        assert writer.open(path, nomodify=False)
        assert writer.addrows(1)
        assert writer.nrows() == count + 1
        assert writer.done()
        assert selector.nrow(True) == count + int(live)
        assert selector.selecttaql(f"ROWID() == {count}") is live
        assert selector.nrow(True) == int(live)
        assert selector.done()
