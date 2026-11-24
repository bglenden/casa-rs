"""Stable ordering and publication for the public MeasurementSet sorts."""

from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


def read_columns(path, *columns):
    table = pinned_casa().table()
    assert table.open(str(path))
    result = [table.getcol(column) for column in columns]
    assert table.done()
    return result


@pytest.mark.parametrize("method, arguments, inplace", [
    ("sort", (["ANTENNA1", "ANTENNA2"],), False),
    ("sort", ("TIME",), False),
    ("timesort", (), False),
    ("timesort", (), True),
    ("sort", ([],), False),
])
def test_ms_sort_matches_pinned_order_history_and_attachment(native_dataset, tmp_path,
                                                             method, arguments, inplace):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    ids = np.arange(table.nrows(), dtype=np.int32)
    assert table.putcol("SCAN_NUMBER", ids)
    assert table.putcol("TIME", table.getcol("TIME")[::-1])
    assert table.putcolkeyword("TIME", "QuantumUnits", ["d"])
    assert table.putcolkeyword("FLAG_CATEGORY", "CATEGORY", ["ONLINE"])
    assert table.done()
    original = read_columns(native_dataset, "SCAN_NUMBER", "TIME", "ANTENNA1", "ANTENNA2")
    if method == "timesort" or arguments == ("TIME",):
        expected_order = np.argsort(original[1], kind="stable")
    elif arguments[0]:
        expected_order = np.lexsort((original[3], original[2]))
    else:
        expected_order = ids

    samples = []
    for namespace, api in enumerate((casa, tools)):
        directory = tmp_path / str(namespace)
        directory.mkdir()
        source = directory / "source.ms"
        copytree(native_dataset, source)
        output = source if inplace else directory / "ordered.ms"
        tool = api.ms()
        assert getattr(tool, method)() is False
        assert tool.open(str(source), nomodify=True, lock=inplace)
        assert getattr(tool, method)("" if inplace else str(output), *arguments)
        assert tool.name() == str(source)
        assert tool.iswritable() is inplace
        assert tool.nrow() == len(ids)
        assert tool.done()
        if not inplace:
            np.testing.assert_array_equal(read_columns(source, "SCAN_NUMBER")[0], ids)
        native_copy = tools.ms()
        assert native_copy.open(str(output))
        assert native_copy.nrow() == len(ids)
        assert native_copy.done()
        np.testing.assert_array_equal(read_columns(output, "SCAN_NUMBER")[0], ids[expected_order])
        assert table.open(str(output))
        assert list(table.getcolkeyword("TIME", "QuantumUnits")) == ["d"]
        assert list(table.getcolkeyword("FLAG_CATEGORY", "CATEGORY")) == ["ONLINE"]
        assert table.done()
        history = ["APPLICATION", "MESSAGE", "ORIGIN", "PRIORITY", "OBJECT_ID", "OBSERVATION_ID"]
        row = [column[-1] for column in read_columns(output / "HISTORY", *history)]
        row[1] = row[1].replace(str(source), "source.ms")
        assert row[0] == "ms"
        assert row[2:] == [f"ms::{method}()", "INFO", 0, -1]
        samples.append((row, read_columns(output, "TIME", "DATA", "FLAG")))
    assert samples[0][0] == samples[1][0]
    for expected, actual in zip(samples[0][1], samples[1][1]):
        np.testing.assert_array_equal(actual, expected)
        assert actual.dtype == expected.dtype


def test_ms_sorted_named_selection_preserves_source_and_unrelated_target(native_dataset, tmp_path):
    tool = tools.ms()
    assert tool.open(str(native_dataset))
    assert tool.select({"rows": [21, 3, 8, 1]})
    selected_data = tool.getdata(["data", "time"])
    blocked = tmp_path / "blocked"
    blocked.mkdir()
    marker = blocked / "important.txt"
    marker.write_text("keep")
    (blocked / "table.dat").write_text("a file, rather than a casacore descriptor")
    with pytest.raises(RuntimeError):
        tool.timesort(str(blocked))
    assert marker.read_text() == "keep"
    with pytest.raises(RuntimeError, match="unselected rows"):
        tool.timesort()
    assert tool.nrow(True) == 4
    output = tmp_path / "selected.ms"
    assert tool.timesort(bytes(output))
    assert tool.nrow(True) == 4
    actual_time, actual_data = read_columns(output, "TIME", "DATA")
    order = np.argsort(selected_data["time"], kind="stable")
    np.testing.assert_array_equal(actual_time, selected_data["time"][order])
    np.testing.assert_array_equal(actual_data, selected_data["data"][:, :, order])
    assert tool.done()


def test_ms_sort_protects_backing_tables_and_copies_nested_subtables(native_dataset):
    casa = pinned_casa()
    extra = native_dataset / "FIELD" / "EXTRA_FEED"
    copytree(native_dataset / "FEED", extra)
    table = casa.table()
    assert table.open(str(native_dataset / "FIELD"), nomodify=False)
    assert table.putkeyword("EXTRA", f"Table: {extra}")
    assert table.done()
    tool = tools.ms()
    assert tool.open(str(native_dataset), nomodify=False, lock=True)
    with pytest.raises(RuntimeError, match="overlaps a source table"):
        tool.timesort(str(native_dataset / "ANTENNA"))
    output = native_dataset / "ordered.ms"
    assert tool.timesort(str(output))
    assert table.open(str(output / "FIELD"))
    assert table.getkeyword("EXTRA") == f"Table: {output / 'FIELD' / 'EXTRA_FEED'}"
    assert table.done()
    assert table.open(str(output / "FIELD" / "EXTRA_FEED"))
    assert table.nrows() > 0
    assert table.done()
    output.rename(native_dataset.with_name("verified-sorted-copy.ms"))
    assert tool.timesort()
    assert tool.iswritable()
    assert tool.done()


def test_ms_sort_uses_casa_complex_scalar_order(native_dataset, tmp_path):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    count = table.nrows()
    table.addcols({"COMPLEX_KEY": dict(valueType="complex", dataManagerType="StandardStMan",
                                       dataManagerGroup="sort_keys", comment="scalar keys")})
    keys = (np.arange(count, 0, -1) * (1 + 3j)).astype(np.complex64)
    assert table.putcol("COMPLEX_KEY", keys)
    assert table.done()
    for index, api in enumerate((casa, tools)):
        output = tmp_path / f"ordered-{index}.ms"
        tool = api.ms()
        assert tool.open(str(native_dataset))
        assert tool.sort(str(output), "COMPLEX_KEY")
        np.testing.assert_array_equal(read_columns(output, "COMPLEX_KEY")[0], keys[::-1])
        assert tool.done()


def test_ms_sort_omits_links_to_its_own_root(native_dataset, tmp_path):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    referenced = table.selectrows([0, 1])
    view_path = tmp_path / "same-root.table"
    persisted = referenced.copy(str(view_path))
    assert persisted.done()
    assert referenced.done()
    assert table.putkeyword("SAME_ROOT", f"Table: {view_path}")
    assert table.done()
    for index, api in enumerate((casa, tools)):
        output = tmp_path / f"sorted-{index}.ms"
        tool = api.ms()
        assert tool.open(str(native_dataset))
        assert tool.timesort(str(output))
        assert tool.done()
        assert table.open(str(output))
        assert "SAME_ROOT" not in table.keywordnames()
        assert table.done()


@pytest.mark.parametrize("column_link", [False, True])
def test_ms_sort_materializes_custom_table_keywords(native_dataset, tmp_path, column_link):
    casa = pinned_casa()
    expected = read_columns(native_dataset / "FEED", "ANTENNA_ID", "NUM_RECEPTORS")
    for index, api in enumerate((casa, tools)):
        case = tmp_path / str(index)
        case.mkdir()
        source = case / "source.ms"
        extra = case / "EXTRA_FEED"
        output = case / "sorted.ms"
        copytree(native_dataset, source)
        copytree(native_dataset / "FEED", extra)
        table = casa.table()
        assert table.open(str(source), nomodify=False)
        if column_link:
            assert table.putcolkeyword("DATA", "CUSTOM", f"Table: {extra}")
        else:
            assert table.putkeyword("CUSTOM", f"Table: {extra}")
        assert table.done()
        tool = api.ms()
        assert tool.open(str(source))
        assert tool.timesort(str(output))
        assert tool.done()

        extra.rename(case / "previous-extra")
        source.rename(case / "previous-source.ms")
        assert table.open(str(output))
        link = (table.getcolkeyword("DATA", "CUSTOM") if column_link
                else table.getkeyword("CUSTOM"))
        assert link == f"Table: {output / 'EXTRA_FEED'}"
        assert table.done()
        for read, wanted in zip(read_columns(output / "EXTRA_FEED", "ANTENNA_ID", "NUM_RECEPTORS"),
                                expected):
            np.testing.assert_array_equal(read, wanted)
