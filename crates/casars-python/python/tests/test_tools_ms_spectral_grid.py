"""Reference spectral grids computed by the legacy public MS tool."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms_createmultims import pinned_casa
from test_tools_ms_testconcatenate import concat_api
from test_tools_msmetadata import measures, native_dataset


def test_cvelfreqs_projects_the_requested_channel_axis(native_dataset, concat_api):
    table = tools.table()
    assert table.open(str(native_dataset / "SPECTRAL_WINDOW"))
    original = table.getcell("CHAN_FREQ", 0)
    assert table.close()
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    defaults = ms.cvelfreqs(verbose=False)
    assert defaults.dtype == np.float64
    np.testing.assert_array_equal(defaults, original)
    expected = (original[::2] + original[1::2]) / 2
    for width in (2, 2.9, "2.9"):
        actual = ms.cvelfreqs(spwids=np.array([0], dtype=np.int32), width=width, verbose=False)
        np.testing.assert_array_equal(actual, expected)
    np.testing.assert_array_equal(ms.cvelfreqs(width=[2], verbose=False), original)
    for kwargs in ({"spwids": []}, {"mode": "bad"}, {"outframe": "BAD"}, {"phasec": 99}):
        empty = ms.cvelfreqs(**kwargs, verbose=False)
        assert empty.shape == (0,)
        assert empty.dtype == np.float64
    with pytest.raises(RuntimeError):
        ms.cvelfreqs(spwids=[99], verbose=False)
    with pytest.raises(SystemError):
        ms.cvelfreqs(spwids=(0,), verbose=False)
    with pytest.raises(SystemError):
        ms.cvelfreqs(nchan=None, verbose=False)
    with pytest.raises(TypeError, match="argument _nchan must be an integer"):
        ms.cvelfreqs(nchan=2.5, verbose=False)
    np.testing.assert_array_equal(ms.cvelfreqs(nchan=np.int32(2), verbose=False), original[:2])
    assert ms.close()
    assert ms.cvelfreqs(verbose=False).shape == (0,)
    assert table.open(str(native_dataset / "SPECTRAL_WINDOW"))
    np.testing.assert_array_equal(table.getcell("CHAN_FREQ", 0), original)
    assert table.close()


def test_cvelfreqs_keeps_observatory_position_independent_of_verbosity(native_dataset):
    reference = pinned_casa()
    args = dict(
        outframe="BARY",
        obstime="2020/01/01/06:00:00",
        phasec="J2000 12h00m00 -30d00m00",
        nchan=2,
    )
    results = []
    for api in (reference, tools):
        ms = api.ms()
        assert ms.open(str(native_dataset))
        centres = ms.cvelfreqs(**args, verbose=True)
        quiet = ms.cvelfreqs(**args, verbose=False)
        assert centres.shape == (2,)
        assert centres.dtype == np.float64
        if api is tools:
            np.testing.assert_array_equal(quiet, centres)
        else:
            assert np.max(np.abs(quiet - centres)) > 1.0
        results.append(centres)
        assert ms.close()
    np.testing.assert_allclose(results[1], results[0], rtol=0, atol=0.05)


@pytest.mark.parametrize("writer", ["casa", "casa-rs"])
def test_cvelfreqs_reads_implicit_epoch_from_persisted_reference(native_dataset, writer):
    casa = pinned_casa()
    table = (casa if writer == "casa" else tools).table()
    path = native_dataset.with_name("working.ms")
    assert table.open(str(native_dataset))
    selection = table.selectrows(np.array([5, 2, 5], dtype=np.int64), str(path))
    assert selection.done()
    assert table.done()

    axes = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(path))
        axis = ms.cvelfreqs(outframe="BARY", nchan=3)
        assert axis.shape == (3,)
        assert axis.dtype == np.float64
        assert ms.nrow() == 3
        assert ms.done()
        axes.append(axis)
    np.testing.assert_allclose(axes[1], axes[0], rtol=0, atol=0.05)


@pytest.mark.parametrize("writer", ["casa", "casa-rs"])
def test_cvelfreqs_reads_updated_metadata_on_the_same_attachment(native_dataset, writer):
    casa = pinned_casa()
    api = casa if writer == "casa" else tools
    ms = tools.ms()
    assert ms.open(str(native_dataset))
    params = dict(outframe="BARY", nchan=2, verbose=True)
    previous = ms.cvelfreqs(**params)
    np.testing.assert_array_equal(ms.cvelfreqs(**params), previous)

    for subtable, column, update in (
        ("SPECTRAL_WINDOW", "CHAN_FREQ", lambda value: value + 1e6),
        ("FIELD", "PHASE_DIR", lambda value: value + 0.2),
        ("", "TIME", lambda value: value - 7 * 86400.),
    ):
        table = api.table()
        assert table.open(str(native_dataset / subtable), nomodify=False)
        assert table.putcell(column, 0, update(table.getcell(column, 0)))
        assert table.done()
        reference = casa.ms()
        assert reference.open(str(native_dataset))
        expected = reference.cvelfreqs(**params)
        assert reference.done()
        updated = ms.cvelfreqs(**params)
        assert np.max(np.abs(expected - previous)) > 1.0
        np.testing.assert_allclose(updated, expected, rtol=0, atol=0.05)
        np.testing.assert_array_equal(ms.cvelfreqs(**params), updated)
        previous = updated
    assert ms.done()


def test_cvelfreqs_prefers_an_appended_earlier_epoch(native_dataset):
    casa = pinned_casa()
    reference = casa.ms()
    native = tools.ms()
    path = str(native_dataset)
    assert reference.open(path)
    assert native.open(path)
    params = dict(outframe="BARY", nchan=2, verbose=True)
    previous = native.cvelfreqs(**params)
    np.testing.assert_allclose(previous, reference.cvelfreqs(**params), rtol=0, atol=0.05)

    table = casa.table()
    assert table.open(path, nomodify=False)
    row = table.nrows()
    copied = {column: table.getcell(column, 0) for column in table.colnames()
              if table.iscelldefined(column, 0)}
    copied["TIME"] -= 14 * 86400.0
    assert table.addrows(1)
    for column, value in copied.items():
        assert table.putcell(column, row, value)
    assert table.done()

    expected = reference.cvelfreqs(**params)
    assert np.max(np.abs(previous - expected)) > 1.0
    np.testing.assert_allclose(native.cvelfreqs(**params), expected, rtol=0, atol=0.05)
    assert native.nrow() == row + 1
    assert native.done() and reference.done()


def test_cvelfreqs_keeps_an_empty_reference_attached(native_dataset):
    path = str(native_dataset.with_name("empty.ms"))
    table = tools.table()
    assert table.open(str(native_dataset))
    assert table.selectrows(np.array([], dtype=np.int64), path).done()
    assert table.done()
    assert table.open(str(native_dataset / "SPECTRAL_WINDOW"))
    expected = table.getcell("CHAN_FREQ", 0)
    assert table.done()

    ms = tools.ms()
    assert ms.open(path)
    np.testing.assert_array_equal(ms.cvelfreqs(verbose=False), expected, strict=True)
    np.testing.assert_array_equal(ms.cvelfreqs(outframe="BARY", verbose=False),
                                  np.empty(0, dtype=np.float64), strict=True)
    assert ms.nrow() == 0
    assert ms.done()
