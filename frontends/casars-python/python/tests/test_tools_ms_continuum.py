"""Pinned CASA comparisons for the three public MS continuum subtractions."""

from contextlib import closing
from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset
from test_tools_simulator import configure


def columns(api, path, names):
    with closing(api.table()) as table:
        assert table.open(str(path))
        return {name: table.getcol(name) for name in names}


@pytest.mark.parametrize("method", ["continuumsub", "continuumsubold"])
@pytest.mark.parametrize("options", [
    {},
    {"field": "F", "fitspw": "0:0~2", "spw": "0:2~3", "solint": "int", "mode": "replace", "fitorder": 1},
    {"field": [0], "fitspw": [0], "solint": "inf", "mode": "model"},
    {"field": 0, "fitspw": "*", "solint": "20s", "mode": "sub", "fitorder": 2},
])
def test_continuum_scratch_columns_reopen_in_both_readers(
    baseline_fixture, native_dataset, tmp_path, method, options,
):
    casa, _, _ = baseline_fixture
    observed = ("DATA", "FLAG", "FLAG_ROW", "WEIGHT", "SIGMA")
    original = columns(casa, native_dataset, observed)
    fits = []
    for api, name in ((casa, "oracle"), (tools, "native")):
        path = tmp_path / f"{name}.ms"
        copytree(native_dataset, path)
        with closing(api.ms()) as ms:
            assert ms.open(str(path), nomodify=False)
            assert getattr(ms, method)(**options) is True
            assert ms.name() == str(path)
            assert ms.nrow() == original["DATA"].shape[-1]
        saved = columns(casa, path, observed + ("MODEL_DATA", "CORRECTED_DATA"))
        for key, value in columns(tools, path, saved).items():
            np.testing.assert_array_equal(value, saved[key], strict=True, err_msg=key)
        for key in observed:
            np.testing.assert_array_equal(saved[key], original[key], strict=True, err_msg=key)
        fits.append(saved)
    for key in ("MODEL_DATA", "CORRECTED_DATA"):
        expected, actual = fits[0][key], fits[1][key]
        assert expected.shape == actual.shape == original["DATA"].shape
        assert expected.dtype == actual.dtype == np.complex128
        difference = np.linalg.norm(expected - actual) / np.linalg.norm(expected)
        assert difference < 1e-6, (key, difference)


def test_contsub_keeps_the_union_ids_when_it_only_writes_one_window(tmp_path, measures):
    casa = pinned_casa()
    source = tmp_path / "bands.ms"
    with closing(tools.simulator()) as sm:
        assert sm.open(str(source))
        configure(sm)
        assert sm.setspwindow(spwname="H", freq="1.5GHz", deltafreq="1MHz",
                              freqresolution="1MHz", nchannels=4, stokes="RR LL")
        assert sm.observe("F", "L", starttime="0s", stoptime="30s")
        assert sm.observe("F", "H", starttime="0s", stoptime="30s")
    frequencies = columns(casa, source / "SPECTRAL_WINDOW", ("CHAN_FREQ",))["CHAN_FREQ"]
    with closing(casa.table()) as table:
        assert table.open(str(source), nomodify=False)
        windows = table.getcol("DATA_DESC_ID")
        time = table.getcol("TIME")
        time[windows == 1] = time[windows == 0]
        assert table.putcol("TIME", time) and table.putcol("TIME_CENTROID", time)
        assert table.putcol("SCAN_NUMBER", np.ones(len(time), dtype=np.int32))
        baseline = table.getcol("ANTENNA1") + 2 * table.getcol("ANTENNA2")
        slope = (frequencies[:, windows] - 1.4e9) * 1e-7 + (baseline[None, :] + 1) / 2
        spectrum = np.stack((slope + 1.75j, 2 * slope - 2j))
        assert table.putcol("DATA", spectrum)
        assert table.putcol("FLAG", np.zeros_like(spectrum, dtype=bool))
        assert table.putcol("FLAG_ROW", np.zeros(len(time), dtype=bool))
    for api, name in ((casa, "oracle"), (tools, "native")):
        product = tmp_path / f"{name}.ms"
        with closing(api.ms()) as ms:
            assert ms.open(str(source))
            assert ms.contsub(str(product), fitspw="0", fitorder=1, combine="spw", spw="1",
                              unionspw="0,1", whichcol="DATA")
            assert ms.name() == str(source)
            assert not ms.iswritable()
        for reader in (casa, tools):
            actual = columns(reader, product, ("DATA", "DATA_DESC_ID"))
            np.testing.assert_array_equal(actual["DATA_DESC_ID"], np.ones(30, dtype=np.int64),
                                          strict=True)
            assert actual["DATA"].shape == (2, 4, 30)
            assert actual["DATA"].dtype == np.complex128
            np.testing.assert_allclose(actual["DATA"], 0, atol=2e-5)
            axis = columns(reader, product / "SPECTRAL_WINDOW", ("CHAN_FREQ",))["CHAN_FREQ"]
            np.testing.assert_array_equal(axis, frequencies, strict=True)
