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


def test_ms_metadata_without_cache_reads_published_field_changes(native_dataset):
    casa = pinned_casa()
    table = casa.table()
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(native_dataset))
        metadata = ms.metadata(cachesize=0)
        assert metadata.fieldnames() == ["F"]
        assert table.open(str(native_dataset / "FIELD"), nomodify=False)
        assert table.putcell("NAME", 0, "Updated")
        assert table.done()
        assert metadata.fieldnames() == ["Updated"]
        assert metadata.done() and ms.done()
        assert table.open(str(native_dataset / "FIELD"), nomodify=False)
        assert table.putcell("NAME", 0, "F")
        assert table.done()


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
