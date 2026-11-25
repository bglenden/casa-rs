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
    for width in (2, 2.9):
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
