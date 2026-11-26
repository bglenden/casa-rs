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
    {"fitspw": "0:1", "fitorder": 2},
    {"mode": "MODEL", "solint": 20},
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


def test_legacy_continuum_averages_each_hand_with_its_own_weight(
    baseline_fixture, native_dataset, tmp_path,
):
    casa, _, raw = baseline_fixture
    weight = 1 + np.arange(raw["weight"].size, dtype=np.float32).reshape(raw["weight"].shape)
    with closing(casa.table()) as table:
        assert table.open(str(native_dataset), nomodify=False)
        assert table.putcol("WEIGHT", weight)
    rows = (raw["antenna1"] == 0) & (raw["antenna2"] == 1)
    signal = raw["data"][..., rows]
    weights = np.where(raw["flag"][..., rows], 0, weight[:, None, rows])
    average = np.sum(signal * weights, axis=-1) / np.sum(weights, axis=-1)
    expected = average.mean(axis=1)[:, None, None] * np.ones(signal.shape)
    for api, name, method in ((casa, "modern", "continuumsub"),
                              (casa, "old", "continuumsubold"),
                              (tools, "native", "continuumsubold")):
        path = tmp_path / f"{name}.ms"
        copytree(native_dataset, path)
        with closing(api.ms()) as ms:
            assert ms.open(str(path), nomodify=False)
            assert getattr(ms, method)(solint="inf", mode="model") is True
        actual = columns(casa, path, ("MODEL_DATA",))["MODEL_DATA"][..., rows]
        if name == "old":
            old_casa_model = np.array([121.28571319580078, 136.3333282470703])[:, None, None]
            np.testing.assert_allclose(actual.real, np.broadcast_to(old_casa_model, actual.shape),
                                       strict=True, rtol=1e-6, atol=0)
        else:
            np.testing.assert_allclose(actual, expected, strict=True, rtol=1e-6, atol=0)


@pytest.mark.parametrize("method", ["continuumsub", "continuumsubold"])
@pytest.mark.parametrize("options", [{"field": "missing"}, {"fitspw": "99"}, {"solint": "unknown"}])
def test_failed_scratch_selection_preserves_the_attachment(baseline_fixture, native_dataset,
                                                            tmp_path, method, options):
    casa, _, _ = baseline_fixture
    names = ("DATA", "FLAG", "FLAG_ROW", "WEIGHT", "SIGMA")
    original = columns(casa, native_dataset, names)
    outcomes = []
    for backend, name in ((casa, "oracle"), (tools, "native")):
        path = tmp_path / f"{name}.ms"
        copytree(native_dataset, path)
        with closing(backend.ms()) as ms:
            assert ms.open(str(path))
            with pytest.raises(RuntimeError):
                getattr(ms, method)()
            assert ms.name() == str(path) and not ms.iswritable()
            assert ms.nrow() == original["DATA"].shape[-1]
            assert ms.close()
            assert ms.open(str(path), nomodify=False)
            with pytest.raises(RuntimeError):
                getattr(ms, method)(**options)
            assert ms.name() == str(path) and ms.iswritable()
            assert ms.nrow() == original["DATA"].shape[-1]
        saved = columns(tools, path, names + ("MODEL_DATA", "CORRECTED_DATA"))
        for key in names:
            np.testing.assert_array_equal(saved[key], original[key], strict=True)
        outcomes.append(saved)
    for key in ("MODEL_DATA", "CORRECTED_DATA"):
        np.testing.assert_array_equal(outcomes[0][key], outcomes[1][key], strict=True)


@pytest.mark.parametrize("method", ["continuumsub", "continuumsubold"])
@pytest.mark.parametrize("permanent", [False, True])
def test_saved_reference_scratch_cells_are_initialized_only_on_its_rows(
    baseline_fixture, native_dataset, tmp_path, method, permanent,
):
    casa, _, _ = baseline_fixture
    rows = np.array([1, 2, 6, 3], dtype=np.int64)
    outcomes = []
    for api, name in ((casa, "oracle"), (tools, "native")):
        original = tmp_path / f"{name}.ms"
        saved = tmp_path / f"{name}-saved.ms"
        copytree(native_dataset, original)
        with closing(casa.table()) as table:
            assert table.open(str(original), nomodify=False)
            with closing(table.selectrows(rows, str(saved))):
                pass
        with closing(api.ms()) as ms:
            assert ms.open(str(saved), nomodify=False, lock=permanent)
            assert getattr(ms, method)() is True
            assert ms.name() == str(saved) and ms.nrow() == len(rows)
        names = ("DATA", "FLAG", "MODEL_DATA", "CORRECTED_DATA")
        values = columns(casa, saved, names)
        for key, value in columns(tools, saved, names).items():
            np.testing.assert_array_equal(value, values[key], strict=True, err_msg=key)
        with closing(tools.table()) as table:
            assert table.open(str(original))
            expected = set(rows)
            for column in ("MODEL_DATA", "CORRECTED_DATA"):
                assert {row for row in range(table.nrows()) if table.iscelldefined(column, row)} == expected
        outcomes.append(values)
    for key in ("MODEL_DATA", "CORRECTED_DATA"):
        np.testing.assert_allclose(outcomes[0][key], outcomes[1][key], atol=3e-5, rtol=1e-6,
                                   strict=True, err_msg=key)


@pytest.mark.parametrize("method", ["continuumsub", "continuumsubold"])
@pytest.mark.parametrize("is_view", [False, True], ids=["direct-mms", "saved-mms"])
def test_scratch_fits_can_span_two_physical_members(
    baseline_fixture, native_dataset, tmp_path, method, is_view,
):
    casa, _, raw = baseline_fixture
    count = raw["time"].size
    selected = np.array([1, 11, 21, 1 + count, 11 + count, 21 + count], dtype=np.int64)
    results = []
    for api, name in ((casa, "oracle"), (tools, "native")):
        members = [tmp_path / f"{name}-{index}.ms" for index in range(2)]
        for index, path in enumerate(members):
            copytree(native_dataset, path)
            with closing(casa.table()) as table:
                assert table.open(str(path), nomodify=False)
                assert table.putcol("FLAG", np.zeros_like(raw["flag"]))
                assert table.putcol("FLAG_ROW", np.zeros_like(raw["flag_row"]))
                assert table.putcol("DATA", raw["data"] + 100 * index)
                for column in ("TIME", "TIME_CENTROID"):
                    assert table.putcol(column, table.getcol(column) + 30 * index)
            if is_view and api is casa:
                # CASA's first fit on a saved MMS view crashes when scratch
                # columns are absent. Initialize them on its constituents.
                with closing(casa.ms()) as tool:
                    assert tool.open(str(path), nomodify=False)
                    assert tool.continuumsub(mode="skip-fit")
        parent = tmp_path / f"{name}.mms"
        with closing(casa.ms()) as tool:
            assert tool.createmultims(str(parent), list(map(str, members)),
                                      ["ANTENNA", "DATA_DESCRIPTION", "FEED"])
            assert tool.open(str(parent))
            physical = tool.getreferencedtables()
        path = parent
        if is_view:
            path = tmp_path / f"{name}-view.ms"
            with closing(casa.table()) as table:
                assert table.open(str(parent))
                assert table.selectrows(selected, str(path)).done()
        with closing(api.ms()) as ms:
            assert ms.open(str(path), nomodify=False)
            assert getattr(ms, method)() is True
            assert ms.name() == str(path) and ms.ismultims()
            assert ms.nrow() == (len(selected) if is_view else 2 * count)
        names = ("DATA", "FLAG", "WEIGHT", "MODEL_DATA", "CORRECTED_DATA")
        output = columns(casa, path, names)
        for key, value in columns(tools, path, names).items():
            np.testing.assert_array_equal(output[key], value, strict=True, err_msg=key)
        results.append(output)
        if is_view and api is tools:
            for index, part in enumerate(physical):
                with closing(casa.table()) as table:
                    assert table.open(part)
                    keep = {row - index * count for row in selected if row // count == index}
                    assert {row for row in range(count) if table.iscelldefined("MODEL_DATA", row)} == keep
    for key in results[0]:
        np.testing.assert_allclose(results[0][key], results[1][key], strict=True,
                                   rtol=1e-6, atol=3e-5, err_msg=key)


def test_contsub_can_keep_the_continuum(baseline_fixture, native_dataset, tmp_path):
    casa, _, _ = baseline_fixture
    for api, name in ((casa, "oracle"), (tools, "native")):
        output = tmp_path / f"{name}.ms"
        with closing(api.ms()) as ms:
            assert ms.open(str(native_dataset))
            assert ms.contsub(str(output), fitorder=-1, whichcol="data", spw="0:2~3") is True
            assert ms.name() == str(native_dataset) and not ms.iswritable()
        names = ("DATA", "FLAG", "FLAG_ROW", "WEIGHT", "SIGMA")
        actual = columns(casa, output, names)
        for key, value in columns(tools, output, names).items():
            np.testing.assert_array_equal(actual[key], value, strict=True, err_msg=key)
        if api is casa:
            expected = actual
        else:
            for key, value in actual.items():
                np.testing.assert_array_equal(expected[key], value, strict=True, err_msg=key)


def test_contsub_writes_iterator_order_from_an_unsorted_reference(baseline_fixture, tmp_path):
    casa, view, raw = baseline_fixture
    source = view("unsorted.ms", [21, 3, 1, 12])
    order = [3, 1, 12, 21]
    products = []
    for api, name in ((casa, "oracle"), (tools, "native")):
        output = tmp_path / f"{name}.ms"
        with closing(api.ms()) as ms:
            assert ms.open(source)
            assert ms.contsub(str(output), fitspw="0:0~3", whichcol="data") is True
            assert ms.name() == source and ms.nrow() == 4
        saved = columns(casa, output, ("TIME", "ANTENNA1", "ANTENNA2", "DATA", "FLAG"))
        for key in ("TIME", "ANTENNA1", "ANTENNA2"):
            np.testing.assert_array_equal(saved[key], raw[key.lower()][order], strict=True)
        for key, value in columns(tools, output, saved).items():
            np.testing.assert_array_equal(saved[key], value, strict=True)
        products.append(saved)
    np.testing.assert_allclose(products[0]["DATA"], products[1]["DATA"], atol=2e-5,
                               rtol=1e-6, strict=True)
    np.testing.assert_array_equal(products[0]["FLAG"], products[1]["FLAG"], strict=True)


def test_contsub_keeps_recorded_correlations_and_flags_an_unfitted_row(
    baseline_fixture, native_dataset, tmp_path,
):
    casa, _, observed = baseline_fixture
    products = []
    for api, name in ((casa, "oracle"), (tools, "native")):
        output = tmp_path / f"{name}.ms"
        with closing(api.ms()) as ms:
            assert ms.open(str(native_dataset))
            assert ms.contsub(str(output), fitspw="0", fitorder=0, whichcol="data",
                              correlation="ll", obs="0", scan=[1]) is True
            assert not ms.iswritable() and ms.name() == str(native_dataset)
        names = ("DATA", "FLAG", "FLAG_ROW", "WEIGHT", "SIGMA")
        saved = columns(casa, output, names)
        assert saved["DATA"].shape == observed["data"].shape
        assert np.all(saved["FLAG"][..., 0])
        assert np.any(~observed["flag"][..., 0])
        for key, value in columns(tools, output, names).items():
            np.testing.assert_array_equal(saved[key], value, strict=True, err_msg=key)
        products.append(saved)
    for key in products[0]:
        np.testing.assert_allclose(products[0][key], products[1][key], atol=2e-5, rtol=1e-6,
                                   strict=True, err_msg=key)


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
