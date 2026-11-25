"""Pinned CASA controls for the deprecated VisibilityIterator controls."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms_putdata import writer_api
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


def test_vi1_reference_rows_and_captured_selection(baseline_fixture, writer_api):
    _, view, original = baseline_fixture
    selected = np.array([19, 13, 0, 6, 2, 11, 3, 8], dtype=np.int64)
    path = view("vi1-reference.ms", selected)
    reader = writer_api.ms()
    assert reader.open(path)
    first = reader.ngetdata(["ROWS", "TIME", "DATA", "FLAG", "WEIGHT"],
                            True, np.int64(-2), 7, True)
    np.testing.assert_array_equal(np.sort(first["rows"]), np.arange(len(selected)), strict=True)
    physical = selected[first["rows"]]
    assert np.all(first["time"][1:] >= first["time"][:-1])
    for name in ("time", "data", "flag"):
        np.testing.assert_array_equal(first[name], original[name][..., physical], strict=True)
    hand = original["weight"][:, physical].astype(np.float32)
    np.testing.assert_array_equal(first["weight"], ((hand[0] + hand[-1]) / 2).astype(np.float64),
                                  strict=True)
    with pytest.raises(RuntimeError, match="Unsupported item requrested"):
        reader.ngetdata(["data", "uvw"])
    assert reader.ngetdata() == {}
    assert reader.msselect({"taql": "ANTENNA1 == 0"})
    np.testing.assert_array_equal(reader.ngetdata("rows")["rows"], first["rows"], strict=True)
    assert reader.niterinit(["ignored"], maxrows=np.int64(2), adddefaultsortcolumns=False)
    chunk = reader.ngetdata(["rows", "antenna1"])
    assert len(chunk["rows"]) == min(reader.nrow(True), 3)
    np.testing.assert_array_equal(chunk["antenna1"], np.zeros(len(chunk["rows"]), dtype=np.int64),
                                  strict=True)
    assert not reader.niterend()
    assert reader.niternext()
    assert reader.niterend()
    assert not reader.niternext()
    np.testing.assert_array_equal(reader.ngetdata("rows")["rows"], chunk["rows"], strict=True)
    assert reader.niterorigin()
    assert not reader.niterend()
    np.testing.assert_array_equal(reader.ngetdata("rows")["rows"], chunk["rows"], strict=True)
    assert reader.done()


@pytest.mark.parametrize("observed_columns", ["complex", "both", "float"])
def test_vi1_column_values_and_antenna2_correction(
    baseline_fixture, native_dataset, writer_api, observed_columns,
):
    casa, _, expected = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    desc = table.getcoldesc("DATA")
    assert table.addcols({"MODEL_DATA": desc, "CORRECTED_DATA": desc})
    corrected = expected["data"] * 2
    model = expected["data"] * (0.5 - 0.5j)
    assert table.putcol("CORRECTED_DATA", corrected.astype(np.complex64))
    assert table.putcol("MODEL_DATA", model.astype(np.complex64))
    observed = expected["data"]
    if observed_columns != "complex":
        assert table.addcols({"FLOAT_DATA": {**desc, "valueType": "float"}})
        observed = (expected["data"].real - 3).astype(np.complex128)
        assert table.putcol("FLOAT_DATA", observed.real.astype(np.float32))
    if observed_columns == "float":
        assert table.removecols("DATA")
    assert table.done()


    reader = writer_api.ms()
    assert reader.open(str(native_dataset), nomodify=False)
    assert reader.niterinit(adddefaultsortcolumns=False)
    result = reader.ngetdata(["DATA", "model_data", "CORRECTED_DATA", "ANTENNA1", "antenna2",
                              "rows"])
    rows = result["rows"]
    np.testing.assert_array_equal(result["data"], observed[..., rows], strict=True)
    np.testing.assert_array_equal(result["model_data"], model[..., rows], strict=True)
    np.testing.assert_array_equal(result["corrected_data"], corrected[..., rows], strict=True)
    assert np.any(expected["antenna1"][rows] != expected["antenna2"][rows])
    np.testing.assert_array_equal(result["antenna1"], expected["antenna1"][rows], strict=True)
    actual_antenna = "antenna2" if writer_api is tools else "antenna1"
    np.testing.assert_array_equal(result["antenna2"], expected[actual_antenna][rows], strict=True)
    assert reader.done()
    assert table.open(str(native_dataset))
    assert not {"SORT_COLUMNS", "SORTED_TABLE"}.intersection(table.keywordnames())
    assert table.done()


def test_vi1_keeps_its_writable_reference_cursor_across_other_iterators(
    baseline_fixture, writer_api,
):
    _, view, _ = baseline_fixture
    path = view("vi1-independent-cursor.ms", [19, 13, 0, 6, 2, 11, 3, 8])
    reader = writer_api.ms()
    assert reader.open(path, nomodify=False)
    assert reader.niterinit(maxrows=2, adddefaultsortcolumns=False)
    first = reader.ngetdata(["time", "data", "rows"])
    assert reader.iterinit()
    assert reader.iterorigin()
    assert reader.iterinitold(["ANTENNA2", "TIME"], adddefaultsortcolumns=False)
    assert reader.iteroriginold()
    assert reader.iterendold()
    assert reader.iterend()
    again = reader.ngetdata(list(first))
    for name in first:
        np.testing.assert_array_equal(again[name], first[name], strict=True)
    assert reader.done()


def test_vi1_row_numbers_use_the_last_position_of_duplicate_reference_rows(
    baseline_fixture, writer_api,
):
    _, view, original = baseline_fixture
    path = view("vi1-duplicates.ms", [0, 0, 1, 5])
    reader = writer_api.ms()
    assert reader.open(path)
    assert reader.niterinit(adddefaultsortcolumns=False)
    actual = reader.ngetdata(["rows", "time", "data", "flag"])
    np.testing.assert_array_equal(actual["rows"], np.array([1, 1, 2, 3]), strict=True)
    for name in ("time", "data", "flag"):
        np.testing.assert_array_equal(actual[name], original[name][..., [0, 0, 1, 5]],
                                      strict=True)
    assert reader.done()


def test_vi1_first_buffer_uses_a_matching_full_table_sort_cache(
    baseline_fixture, native_dataset, writer_api,
):
    _, view, original = baseline_fixture
    physical = np.arange(len(original["time"]) - 1, -1, -1, dtype=np.int64)
    path = view("vi1-reversed.ms", physical)
    reader = writer_api.ms()
    assert reader.open(path)
    assert reader.niterinit(maxrows=1, adddefaultsortcolumns=False)
    uncached = reader.ngetdata(["rows", "data"])
    time_order = np.argsort(original["time"][physical], kind="stable")
    np.testing.assert_array_equal(uncached["rows"], time_order[:2], strict=True)
    np.testing.assert_array_equal(uncached["data"], original["data"][..., physical[time_order[:2]]],
                                  strict=True)
    assert reader.done()

    assert reader.open(str(native_dataset), nomodify=False)
    assert reader.iterinitold(["TIME"], adddefaultsortcolumns=False)
    assert reader.done()

    assert reader.open(path)
    assert reader.niterinit(maxrows=1, adddefaultsortcolumns=False)
    cached = reader.ngetdata(["rows", "data"])
    ordered = np.argsort(original["time"], kind="stable")[:2]
    np.testing.assert_array_equal(cached["rows"], len(physical) - 1 - ordered, strict=True)
    np.testing.assert_array_equal(cached["data"], original["data"][..., ordered], strict=True)
    assert reader.done()


def test_vi1_uninitialized_and_missing_column_are_python_errors(baseline_fixture, native_dataset):
    reader = tools.ms()
    assert not reader.niterend()
    assert not reader.niternext()
    assert not reader.niterorigin()
    with pytest.raises(RuntimeError, match="zero selected rows"):
        reader.niterinit()
    with pytest.raises(RuntimeError, match="zero selected rows"):
        reader.ngetdata("rows")
    assert reader.open(str(native_dataset))
    for control in (reader.niterend, reader.niternext, reader.niterorigin):
        with pytest.raises(RuntimeError, match="niterinit"):
            control()
    for column in ("model_data", "corrected_data"):
        with pytest.raises(RuntimeError, match=column.upper()):
            reader.ngetdata(column)
    assert not reader.niterend()
    assert reader.done()
