"""Pinned controls for the legacy cursor, block view and persisted sort cache."""

from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_tools_ms_putdata import assert_main_columns, make_multi_ms, writer_api
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


def test_legacy_cursor_exposes_positioned_rows_ranges_and_unchanged_modern_view(
    baseline_fixture, native_dataset, writer_api,
):
    _, _, original = baseline_fixture
    rows = np.flatnonzero(original["time"] == original["time"][0])[:8]
    assert len(rows) == 8
    reader = writer_api.ms()
    for control in (reader.iteroriginold, reader.iternextold, reader.iterendold):
        assert control() is False
    assert reader.open(str(native_dataset))
    assert reader.selectold({"rows": rows})
    assert reader.iterinitold(["TIME"], maxrows=np.int64(3), adddefaultsortcolumns=False)
    assert reader.nrowold(True) == len(rows)
    assert reader.iteroriginold()
    for first in range(0, len(rows), 3):
        chunk = rows[first:first + 3]
        assert reader.nrowold(True) == len(chunk)
        assert reader.nrow(True) == len(original["time"])
        actual = reader.rangeold(["rows", "time"])
        np.testing.assert_array_equal(actual["rows"], chunk, strict=True)
        np.testing.assert_array_equal(
            actual["time"], np.full(2, original["time"][chunk[0]]), strict=True,
        )
        current = reader.getdataold(["data", "flag"])
        for name in current:
            np.testing.assert_array_equal(current[name], original[name][..., chunk], strict=True)
        assert reader.iternextold() is (first + 3 < len(rows))
    assert reader.nrowold(True) == len(rows)
    assert reader.iternextold() is False
    assert reader.iterendold() is True
    assert reader.iteroriginold() is True
    assert reader.nrowold(True) == 3
    assert reader.done()


def test_legacy_iterator_persists_a_readable_sorted_reference_and_writes_only_the_active_block(
    baseline_fixture, native_dataset, writer_api,
):
    casa, _, original = baseline_fixture
    writer = writer_api.ms()
    assert writer.open(str(native_dataset), nomodify=False)
    keys = ["ANTENNA2", "TIME"]
    assert writer.iterinitold(keys, maxrows=3, adddefaultsortcolumns=False)
    assert writer.iteroriginold()
    assert writer.iterinitold(["missing_column"], adddefaultsortcolumns=False) is False
    assert writer.iternextold()
    active = writer.rangeold("rows")["rows"]
    selected = writer.getdataold("data")["data"] * 2
    assert 0 < len(active) <= 3
    assert writer.putdataold({"data": selected})
    assert writer.iterendold()
    assert writer.nrowold(True) == len(original["time"])
    assert writer.done()

    expected = original["data"].copy()
    expected[..., active] = selected
    assert_main_columns(casa, native_dataset, {"DATA": expected, "FLAG": original["flag"]})
    order = np.lexsort((original["time"], original["antenna2"]))
    for api in (casa, tools):
        table = api.table()
        assert table.open(str(native_dataset))
        assert table.getkeyword("SORT_COLUMNS").tolist() == keys
        assert "SORTED_TABLE" in table.getkeyword("SORTED_TABLE")
        assert table.done()
        assert table.open(str(native_dataset / "SORTED_TABLE"))
        np.testing.assert_array_equal(table.getcol("DATA"), expected[..., order], strict=True)
        assert table.done()

    moved = native_dataset.rename(native_dataset.with_name("relocated.ms"))
    for api in (casa, tools):
        table = api.table()
        assert table.open(str(moved / "SORTED_TABLE"))
        np.testing.assert_array_equal(table.getcol("DATA"), expected[..., order], strict=True)
        assert table.done()


def test_legacy_cursor_omits_the_empty_block_after_an_exact_multiple(
    baseline_fixture, native_dataset,
):
    casa, _, original = baseline_fixture
    rows = np.flatnonzero(original["time"] == original["time"][0])[:8]
    assert len(rows) == 8
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectold({"rows": rows})
        assert reader.iterinitold(["TIME"], maxrows=4, adddefaultsortcolumns=False)
        assert reader.iteroriginold()
        counts = [reader.nrowold(True)]
        while reader.iternextold():
            counts.append(reader.nrowold(True))
        assert counts == ([4, 4, 0] if api is casa else [4, 4])
        assert reader.nrowold(True) == 8
        assert reader.done()


def test_legacy_writable_multims_uses_a_cursor_without_a_saved_sort_reference(
    baseline_fixture, native_dataset, tmp_path, writer_api,
):
    casa, _, original = baseline_fixture
    members = [tmp_path / name for name in ("part-z.ms", "part-a.ms")]
    for member in members:
        copytree(native_dataset, member)
    path = make_multi_ms(casa, tmp_path / "multi.ms", members)
    reader = writer_api.ms()
    assert reader.open(path, nomodify=False)
    assert reader.iterinitold(["TIME"], adddefaultsortcolumns=False)
    assert reader.iteroriginold()
    assert reader.nrowold(True) == 2 * len(original["time"])
    record = reader.getdataold("time")
    np.testing.assert_array_equal(record["time"], np.sort(np.tile(original["time"], 2)), strict=True)
    assert reader.iternextold() is False
    assert reader.iterendold()
    assert reader.done()
    assert not list(tmp_path.rglob("SORTED_TABLE"))
    table = casa.table()
    assert table.open(path)
    assert not {"SORT_COLUMNS", "SORTED_TABLE"}.intersection(table.keywordnames())
    assert table.done()


def test_writable_legacy_reference_caches_root_rows_and_restores_view_order(
    baseline_fixture, native_dataset, writer_api,
):
    casa, view, original = baseline_fixture
    rows = np.array([13, 7, 0, 2, 5], dtype=np.int64)
    path = view("ordered-reference.ms", rows)
    reader = writer_api.ms()
    assert reader.open(path, nomodify=False)
    assert reader.iterinitold(["TIME", "ANTENNA1"], adddefaultsortcolumns=False)
    assert reader.iteroriginold()
    np.testing.assert_array_equal(reader.rangeold("rows")["rows"], np.array([0, 2]), strict=True)
    assert reader.iterendold()
    np.testing.assert_array_equal(reader.rangeold("rows")["rows"], rows, strict=True)
    assert reader.done()

    expected = rows[np.lexsort((original["antenna1"][rows], original["time"][rows]))]
    for api in (casa, tools):
        table = api.table()
        assert table.open(str(native_dataset / "SORTED_TABLE"))
        np.testing.assert_array_equal(table.getcol("DATA"), original["data"][..., expected], strict=True)
        assert table.done()
