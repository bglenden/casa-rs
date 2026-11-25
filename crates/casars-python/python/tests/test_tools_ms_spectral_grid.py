"""Reference spectral grids computed by the legacy public MS tool."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms_createmultims import pinned_casa
from test_tools_ms_testconcatenate import concat_api
from test_tools_msmetadata import ephemeris_dataset, measures, native_dataset


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
    for kwargs in ({"spwids": []}, {"spwids": [0, 99]}, {"mode": "bad"},
                   {"outframe": "BAD"}, {"phasec": 99}):
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
    append_observatory(native_dataset, "ALMA")
    ms = tools.ms()
    assert ms.open(str(native_dataset))
    params = dict(outframe="BARY", nchan=2, verbose=True)
    previous = ms.cvelfreqs(**params)
    np.testing.assert_array_equal(ms.cvelfreqs(**params), previous)

    for subtable, column, update in (
        ("SPECTRAL_WINDOW", "CHAN_FREQ", lambda value: value + 1e6),
        ("FIELD", "PHASE_DIR", lambda value: value + 0.2),
        ("", "OBSERVATION_ID", lambda value: 1),
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


def append_observatory(dataset, name):
    table = tools.table()
    assert table.open(str(dataset / "OBSERVATION"), nomodify=False)
    row = table.nrows()
    copied = {
        column: table.getcell(column, 0)
        for column in table.colnames()
        if table.iscelldefined(column, 0)
    }
    copied["TELESCOPE_NAME"] = name
    assert table.addrows(1)
    for column, value in copied.items():
        assert table.putcell(column, row, value)
    assert table.done()


def test_cvelfreqs_uses_polynomial_phase_and_the_working_observatory(native_dataset):
    casa = pinned_casa()
    append_observatory(native_dataset, "ALMA")
    table = tools.table()
    assert table.open(str(native_dataset), nomodify=False)
    assert table.putcell("OBSERVATION_ID", 5, 1)
    origin = float(table.getcell("TIME", 0))
    assert table.done()
    assert table.open(str(native_dataset / "FIELD"), nomodify=False)
    assert table.putcell("NUM_POLY", 0, 1)
    assert table.putcell("TIME", 0, origin)
    coefficients = np.array([[0.15, 5.0e-5], [-0.5, -2.0e-5]])
    for column in ("PHASE_DIR", "REFERENCE_DIR", "DELAY_DIR"):
        assert table.putcell(column, 0, coefficients)
    assert table.done()

    axes = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(native_dataset))
        assert ms.msselect({"observation": "1"})
        axis = ms.cvelfreqs(obstime="2020/01/01/00:10:05", outframe="BARY", nchan=3)
        assert axis.shape == (3,)
        axes.append(axis)
        assert ms.done()
    np.testing.assert_allclose(axes[1], axes[0], rtol=0, atol=0.05)


@pytest.mark.parametrize("outframe", ["", "BARY", "SOURCE"])
def test_cvelfreqs_uses_the_attached_ephemeris(ephemeris_dataset, outframe):
    casa = pinned_casa()
    dataset, _ = ephemeris_dataset
    axes = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(dataset))
        axis = ms.cvelfreqs(obstime="2020/01/01/12:00:00", outframe=outframe, nchan=3)
        assert axis.shape == (3,)
        axes.append(axis)
        with pytest.raises(RuntimeError, match="No valid (field )?ephemeris entry"):
            ms.cvelfreqs(obstime="2020/02/01/06:00:00", outframe=outframe, nchan=3)
        assert ms.done()
    np.testing.assert_allclose(axes[1], axes[0], rtol=0, atol=0.05)


def test_cvelfreqs_repeats_implicit_epoch_with_variable_reference(native_dataset):
    casa = pinned_casa()
    table = tools.table()
    assert table.open(str(native_dataset), nomodify=False)
    assert table.addcols({"TIME_REF": {"valueType": "string"}})
    assert table.putcol("TIME_REF", np.full(table.nrows(), "UTC"))
    assert table.putcolkeyword("TIME", "MEASINFO", {"type": "epoch", "VarRefCol": "TIME_REF"})
    assert table.done()

    first_axes = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(native_dataset))
        first = ms.cvelfreqs(outframe="BARY", nchan=2)
        assert first.shape == (2,)
        np.testing.assert_array_equal(ms.cvelfreqs(outframe="BARY", nchan=2), first, strict=True)
        first_axes.append(first)
        assert ms.done()
    np.testing.assert_allclose(first_axes[1], first_axes[0], rtol=0, atol=0.05)


def test_cvelfreqs_uses_meas_freq_ref_instead_of_ref_frequency_descriptor(native_dataset):
    casa = pinned_casa()
    table = tools.table()
    assert table.open(str(native_dataset / "SPECTRAL_WINDOW"), nomodify=False)
    assert table.getcell("MEAS_FREQ_REF", 0) == 5  # TOPO
    assert table.putcolkeyword("REF_FREQUENCY", "MEASINFO", {"type": "frequency", "Ref": "BARY"})
    assert table.done()
    axes = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(native_dataset))
        axes.append(ms.cvelfreqs(obstime="2020/01/01/06:00:00", outframe="BARY", nchan=3))
        assert ms.done()
    np.testing.assert_allclose(axes[1], axes[0], rtol=0, atol=0.05)


def test_cvelfreqs_accepts_phase_direction_offsets(native_dataset):
    casa = pinned_casa()

    def direction(longitude, latitude):
        return {
            "type": "direction",
            "refer": "J2000",
            "m0": {"value": longitude, "unit": "rad"},
            "m1": {"value": latitude, "unit": "rad"},
        }

    phase = direction(0.1, 0.2)
    phase["offset"] = direction(0.6, -0.7)
    axes = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(native_dataset))
        axis = ms.cvelfreqs(obstime="2020/01/01/06:00:00", outframe="BARY", nchan=2, phasec=phase)
        assert axis.shape == (2,)
        axes.append(axis)
        assert ms.done()
    np.testing.assert_allclose(axes[1], axes[0], rtol=0, atol=0.05)
