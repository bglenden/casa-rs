"""Behavioral checks for metadata tools obtained from an MS attachment."""

import shutil

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import ephemeris_dataset, measures, native_dataset


def test_ms_metadata_follows_selection_without_retargeting(native_dataset, tmp_path):
    casa = pinned_casa()
    replacement = tmp_path / "small-view.ms"
    table = casa.table()
    assert table.open(str(native_dataset))
    full_count = table.nrows()
    a, b = table.getcol("ANTENNA1"), table.getcol("ANTENNA2")
    baseline_count = np.count_nonzero(((a == 0) & (b == 1)) | ((a == 1) & (b == 0)))
    assert 0 < baseline_count < full_count
    subset = table.selectrows(np.array([2, 0], dtype=np.int64), str(replacement))
    assert subset.done() and table.done()

    for api in (casa, tools):
        ms = api.ms()
        assert ms.reset()
        detached = ms.metadata()
        assert isinstance(detached, api.msmetadata)
        with pytest.raises(RuntimeError, match="Tool is not attached"):
            detached.nspw()
        assert detached.done()

        assert ms.open(str(native_dataset))
        metadata = ms.metadata(cachesize=np.float32(0))
        assert metadata.nrows() == float(full_count)
        assert metadata.nspw() == 1
        assert ms.msselect({"baseline": "A1&A2"}, onlyparse=True)
        assert metadata.nrows() == float(full_count)
        assert ms.msselect({"baseline": "A1&A2"})
        assert metadata.nrows() == float(baseline_count)
        selected_metadata = ms.metadata(cachesize=0.0)
        assert selected_metadata.nrows() == float(baseline_count)
        assert ms.reset()
        assert metadata.nrows() == selected_metadata.nrows() == float(full_count)
        assert ms.msselect({"baseline": "A1&A2"})
        assert ms.done()

        assert ms.open(str(replacement))
        assert ms.nrow() == 2
        if api is tools:
            assert metadata.name() == str(native_dataset)
            assert selected_metadata.nrows() == float(baseline_count)
        assert selected_metadata.done() and metadata.done()
        current = ms.metadata()
        assert current.done()
        assert ms.nrow() == 2
        assert ms.done()


def test_ms_metadata_on_genuine_multi_ms(native_dataset, tmp_path):
    casa = pinned_casa()
    a = tmp_path / "a.ms"
    b = tmp_path / "b.ms"
    for part in (a, b):
        shutil.copytree(native_dataset, part)
    table = casa.table()
    assert table.open(str(b / "FIELD"), nomodify=False)
    assert table.putcell("NAME", 0, "SECOND")
    assert table.done()

    mms = tmp_path / "combined.mms"
    creator = casa.ms()
    assert creator.createmultims(
        str(mms), [str(a), str(b)], ["ANTENNA", "DATA_DESCRIPTION", "FEED", "FIELD"]
    )
    assert creator.done()
    assert not (mms / "FIELD").exists()

    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(mms))
        original_rows = ms.nrow()
        metadata = ms.metadata(cachesize=0.0)
        assert metadata.fieldnames() == ["F", "SECOND"]
        assert metadata.nrows() == original_rows
        assert ms.msselect({"baseline": "A1&A2"})
        assert 0 < ms.nrow(True) < original_rows
        assert metadata.nrows() == ms.nrow(True)
        assert ms.reset()
        assert metadata.nrows() == original_rows
        assert metadata.done() and ms.done()

    native = tools.ms()
    assert native.open(str(mms))
    metadata = native.metadata(cachesize=0)
    original_rows = metadata.nrows()
    assert table.open(str(mms / "SUBMSS" / a.name))
    assert table.copyrows(str(mms / "SUBMSS" / b.name), 0, -1, 1)
    assert table.done()
    assert metadata.nrows() == metadata.summary()["nrows"] == original_rows + 1
    assert metadata.done() and native.done()


def test_ms_metadata_selected_reference_preserves_field_ephemeris(ephemeris_dataset):
    casa = pinned_casa()
    dataset, mjd = ephemeris_dataset
    view = dataset.with_name("ephemeris-reference.ms")
    table = casa.table()
    assert table.open(str(dataset))
    assert table.selectrows(np.arange(table.nrows(), dtype=np.int64), str(view)).done()
    assert table.done()
    assert not (view / "FIELD").exists()

    epoch = {"type": "epoch", "refer": "UTC", "m0": {"value": mjd + 0.75, "unit": "d"}}
    directions = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(view))
        metadata = ms.metadata(cachesize=0.0)
        assert ms.msselect({"baseline": "A1&A2"})
        assert 0 < metadata.nrows() == ms.nrow(True) < ms.nrow(False)
        directions.append(metadata.phasecenter(0, epoch))
        assert metadata.done() and ms.done()
    expected, actual = directions
    assert (actual["type"], actual["refer"]) == (expected["type"], expected["refer"])
    for axis in ("m0", "m1"):
        assert actual[axis]["unit"] == expected[axis]["unit"]
        assert actual[axis]["value"] == pytest.approx(expected[axis]["value"], abs=2e-11)


def test_ms_metadata_resolves_each_multi_ms_field_ephemeris(ephemeris_dataset, tmp_path):
    first, mjd = ephemeris_dataset
    second = tmp_path / "second.ms"
    shutil.copytree(first, second)
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(second / "FIELD" / "EPHEM0_TEST.tab"), nomodify=False)
    assert table.putcol("RA", table.getcol("RA") + 0.4)
    assert table.done()

    epoch = {"type": "epoch", "refer": "UTC", "m0": {"value": mjd + 0.75, "unit": "d"}}
    expected = []
    for member in (first, second):
        standalone = casa.ms()
        assert standalone.open(str(member))
        metadata = standalone.metadata()
        expected.append(metadata.phasecenter(0, epoch))
        assert metadata.done() and standalone.done()
    assert abs(expected[1]["m0"]["value"] - expected[0]["m0"]["value"]) > 0.005

    combined = tmp_path / "with-ephemerides.mms"
    creator = casa.ms()
    assert creator.createmultims(
        str(combined), [str(first), str(second)],
        ["ANTENNA", "DATA_DESCRIPTION", "FEED", "FIELD"],
    )
    assert creator.done()
    native = tools.ms()
    assert native.open(str(combined))
    metadata = native.metadata()
    for field, reference in enumerate(expected):
        actual = metadata.phasecenter(field, epoch)
        assert (actual["type"], actual["refer"]) == (reference["type"], reference["refer"])
        for axis in ("m0", "m1"):
            assert actual[axis]["unit"] == reference[axis]["unit"]
            assert actual[axis]["value"] == pytest.approx(reference[axis]["value"], abs=2e-11)
    assert metadata.done() and native.done()


def test_ms_metadata_without_cache_reads_published_field_changes(native_dataset):
    casa = pinned_casa()
    table = casa.table()
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(native_dataset))
        assert table.open(str(native_dataset / "FIELD"), nomodify=False)
        original = "long_field_name_" * 20
        assert table.putcell("NAME", 0, original)
        assert table.done()
        readers = [ms.metadata(cachesize=budget) for budget in (0, 0.000002, 50)]
        for metadata in readers:
            assert metadata.fieldnames() == [original]
        assert table.open(str(native_dataset / "FIELD"), nomodify=False)
        assert table.putcell("NAME", 0, "Updated")
        assert table.done()
        assert [metadata.fieldnames() for metadata in readers] == [
            ["Updated"], ["Updated"], [original],
        ]
        assert all(metadata.done() for metadata in readers) and ms.done()


def test_ms_metadata_keeps_spectral_window_counts_consistent(native_dataset, tmp_path):
    casa = pinned_casa()
    table = casa.table()
    for api in (casa, tools):
        dataset = tmp_path / (api.__name__ + ".ms")
        shutil.copytree(native_dataset, dataset)
        ms = api.ms()
        assert ms.open(str(dataset))
        cached = ms.metadata()
        live = ms.metadata(cachesize=0)
        assert cached.nspw() == cached.nspw(False) == live.nspw() == 1
        assert table.open(str(dataset / "SPECTRAL_WINDOW"), nomodify=False)
        row = [(key, table.getcell(key, 0)) for key in table.colnames()
               if table.iscelldefined(key, 0)]
        assert table.addrows(1)
        for key, value in row:
            assert table.putcell(key, 1, value)
        assert table.done()
        assert cached.nspw() == cached.nspw(False) == 1
        assert live.nspw() == live.nspw(False) == 1
        fresh = ms.metadata()
        assert fresh.nspw() == 2
        assert cached.done() and live.done() and fresh.done() and ms.done()


def test_ms_metadata_refuses_a_replacement_at_the_original_path(native_dataset, tmp_path):
    replacement = tmp_path / "replacement.ms"
    shutil.copytree(native_dataset, replacement)
    table = tools.table()
    assert table.open(str(replacement / "FIELD"), nomodify=False)
    assert table.putcell("NAME", 0, "Replacement")
    assert table.done()

    ms = tools.ms()
    assert ms.open(str(native_dataset))
    children = [ms.metadata(cachesize=budget) for budget in (0, 50)]
    assert all(metadata.fieldnames() == ["F"] for metadata in children)
    assert ms.done()
    native_dataset.rename(tmp_path / "original.ms")
    replacement.rename(native_dataset)
    assert ms.open(str(native_dataset))
    replacement_metadata = ms.metadata()
    assert replacement_metadata.fieldnames() == ["Replacement"]
    for metadata in children:
        assert metadata.name() == str(native_dataset)
        for query in (metadata.fieldnames, metadata.nrows, metadata.nspw):
            with pytest.raises(RuntimeError, match="original MeasurementSet attachment"):
                query()
        assert metadata.done()
    assert replacement_metadata.done() and ms.done()


def test_ms_metadata_clears_cached_scan_index_when_the_selection_changes(native_dataset):
    table = tools.table()
    assert table.open(str(native_dataset), nomodify=False)
    left, right = table.getcol("ANTENNA1"), table.getcol("ANTENNA2")
    selected = ((left == 0) & (right == 1)) | ((left == 1) & (right == 0))
    assert np.any(selected) and np.any(~selected)
    assert table.putcol("SCAN_NUMBER", np.where(selected, 3, 7).astype(np.int32))
    assert table.done()

    ms = tools.ms()
    assert ms.open(str(native_dataset))
    metadata = ms.metadata()
    np.testing.assert_array_equal(metadata.scannumbers(), np.array([3, 7], dtype=np.int64))
    assert ms.msselect({"baseline": "A1&A2"})
    np.testing.assert_array_equal(metadata.scannumbers(), np.array([3], dtype=np.int64))
    assert ms.reset()
    np.testing.assert_array_equal(metadata.scannumbers(), np.array([3, 7], dtype=np.int64))
    assert metadata.done() and ms.done()
