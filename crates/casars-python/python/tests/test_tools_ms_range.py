"""Pinned controls for the public MS range query on selected rows."""
import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_selection_indices import two_spws_dataset
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


def extrema(values):
    return np.array([values.min(), values.max()], dtype=np.float64)


@pytest.mark.parametrize("useflags,blocksize", [(True, 1), (False, 10), (True, -1)])
def test_ms_range_uses_selected_rows_and_recorded_axes(baseline_fixture, native_dataset,
                                                       useflags, blocksize):
    casa, _, raw = baseline_fixture
    rows = np.flatnonzero(raw["antenna1"] == 0)
    samples = raw["data"][..., rows].astype(np.complex64)
    samples = samples[~raw["flag"][..., rows]] if useflags else samples.ravel()
    coords = raw["uvw"][:, rows]
    result = {
        "rows": rows,
        "time": extrema(raw["time"][rows]),
        "times": np.unique(raw["time"][rows]),
        "antenna1": np.unique(raw["antenna1"][rows]),
        "antenna2": np.unique(raw["antenna2"][rows]),
        "ifr_number": np.unique(raw["antenna1"][rows] * 1000 + raw["antenna2"][rows]),
        "amplitude": extrema(np.abs(samples)),
        "phase": extrema(np.angle(samples)),
        "real": extrema(samples.real),
        "imaginary": extrema(samples.imag),
        "u": extrema(coords[0]),
        "v": extrema(coords[1]),
        "w": extrema(coords[2]),
        "uvdist": extrema(np.hypot(coords[0], coords[1])),
        "sigma": extrema(raw["sigma"][..., rows]),
        "weight": extrema(raw["weight"][..., rows]),
        "corr_names": np.array([["RR"], ["LL"]]),
        "num_chan": np.array([4]),
        "num_corr": np.array([2]),
    }
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.msselect({"taql": "ANTENNA1 == 0"})
        assert reader.selectchannel(2, 0, 1, 2)
        assert reader.selectpolarization("LL")
        assert reader.iterinit(maxrows=4)
        assert reader.iterorigin()
        cursor = reader.getdata("time")["time"].copy()
        items = list(result) + ["DATA", "unrecognized", "Time"]
        selected = reader.range(items, useflags=useflags, blocksize=np.int64(blocksize))
        assert selected.keys() == result.keys()
        for key, expected in result.items():
            if expected.dtype.kind in "f":
                np.testing.assert_allclose(selected[key], expected, rtol=1e-6, atol=1e-12,
                                           strict=True, err_msg=key)
            else:
                np.testing.assert_array_equal(selected[key], expected, strict=True, err_msg=key)
        np.testing.assert_array_equal(reader.getdata("time")["time"], cursor, strict=True)
        assert reader.nrow(True) == len(rows)
        assert reader.done()


def test_ms_range_reads_live_reference_row_numbers(baseline_fixture):
    casa, view, raw = baseline_fixture
    rows = np.array([19, 13, 0, 6, 2, 11, 3, 8], dtype=np.int64)
    path = view("range-ref.ms", rows)
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        result = reader.range(["rows", "time", "times"])
        np.testing.assert_array_equal(result["rows"], rows, strict=True)
        np.testing.assert_array_equal(result["times"], np.unique(raw["time"][rows]), strict=True)
        np.testing.assert_array_equal(result["time"], extrema(raw["time"][rows]), strict=True)
        assert reader.done()


def test_ms_range_skips_an_entire_flagged_block_before_valid_extrema(baseline_fixture, native_dataset):
    casa, view, raw = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    assert table.putcell("FLAG", 0, np.ones(raw["flag"].shape[:2], dtype=bool))
    assert table.putcell("FLAG", 1, np.zeros(raw["flag"].shape[:2], dtype=bool))
    assert table.done()
    row_cells = int(np.prod(raw["data"].shape[:2]))
    rows = [0] * (1_000_000 // (8 * row_cells)) + [1]
    path = view("range-fully-flagged-first-block.ms", rows)
    values = raw["data"][..., 1].astype(np.complex64)
    expected = {"amplitude": extrema(np.abs(values)), "real": extrema(values.real),
                "imaginary": extrema(values.imag)}
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        for size in (1, 10):
            result = reader.range(list(expected), blocksize=size)
            for field, extent in expected.items():
                if api is casa and size == 1:
                    extent = np.array([0.0, extent[1]])
                np.testing.assert_allclose(result[field], extent, rtol=1e-6, strict=True)
        assert reader.done()



def test_ms_range_reports_each_description_without_forcing_a_uniform_shape(two_spws_dataset):
    casa = pinned_casa()
    keys = ["time", "num_chan", "num_corr", "chan_freq", "corr_names", "weight", "sigma",
            "amplitude", "ref_frequency"]
    for api in (casa, tools):
        reader = api.ms()
        assert reader.range("time") == {}
        assert reader.open(two_spws_dataset)
        assert reader.range(("TIME",)) == {}
        with pytest.raises(SystemError):
            reader.range("time", blocksize=None)
        with pytest.raises(TypeError):
            reader.range("time", blocksize=1.0)
        with pytest.raises(TypeError):
            reader.range("time", useflags=1)
        mixed = reader.range(keys)
        assert mixed.keys() == {"num_chan", "num_corr", "ref_frequency", "time"}
        np.testing.assert_array_equal(mixed["num_chan"], [4, 3], strict=True)
        np.testing.assert_array_equal(mixed["num_corr"], [2, 2], strict=True)
        np.testing.assert_allclose(mixed["ref_frequency"], [1.4e9, 2e9], rtol=1e-15,
                                   strict=True)
        assert reader.nrow(True) == 60
        assert reader.selectinit(1)
        uniform = reader.range(keys)
        assert uniform.keys() == set(keys)
        np.testing.assert_array_equal(uniform["num_chan"], [3], strict=True)
        np.testing.assert_array_equal(uniform["num_corr"], [2], strict=True)
        np.testing.assert_array_equal(uniform["corr_names"], [["RR"], ["LL"]], strict=True)
        np.testing.assert_allclose(uniform["chan_freq"], 2e9 + np.arange(3)[:, None] * 2e6,
                                   rtol=1e-15, strict=True)
        assert reader.nrow(True) == 30
        assert reader.done()


def test_ms_range_returns_linked_metadata(baseline_fixture, native_dataset):
    casa, _, _ = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset / "FIELD"))
    fields = table.getcol("NAME")
    directions = table.getcol("PHASE_DIR")[:, 0, :]
    reference = table.getcolkeyword("PHASE_DIR", "MEASINFO")["Ref"]
    assert table.done()
    assert table.open(str(native_dataset / "ANTENNA"))
    antennas = table.getcol("NAME")
    assert table.done()
    assert table.open(str(native_dataset / "POLARIZATION"))
    codes = table.getcol("CORR_TYPE")
    assert table.done()
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.msselect({"taql": "ANTENNA1 == 0"})
        result = reader.range(["fields", "antennas", "phase_dir", "corr_types"])
        np.testing.assert_array_equal(result["fields"], fields, strict=True)
        np.testing.assert_array_equal(result["antennas"], antennas, strict=True)
        np.testing.assert_array_equal(result["corr_types"], codes, strict=True)
        np.testing.assert_allclose(result["phase_dir"]["direction"], directions,
                                   rtol=1e-14, atol=1e-14, strict=True)
        assert result["phase_dir"]["epoch"] == reference
        assert reader.done()


@pytest.mark.parametrize("useflags", [False, True])
def test_ms_range_reduces_corrected_model_and_float_products(
    baseline_fixture, native_dataset, useflags,
):
    casa, _, raw = baseline_fixture
    observed = raw["data"].astype(np.complex64)
    model = np.full(observed.shape, 2 + 0.5j, dtype=np.complex64)
    corrected = observed * np.complex64(0.75 + 0.1j)
    single_dish = observed.real - 123.0
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    for name, data in [
        ("MODEL_DATA", model),
        ("CORRECTED_DATA", corrected),
        ("FLOAT_DATA", single_dish),
    ]:
        description = dict(table.getcoldesc("DATA"), dataManagerType="StandardStMan",
                           dataManagerGroup=name)
        if data.dtype.kind == "f":
            description["valueType"] = "float"
        assert table.addcols({name: description})
        assert table.putcol(name, data)
    assert table.done()

    samples = ~raw["flag"] if useflags else np.ones(observed.shape, dtype=bool)
    values = {
        "model_imaginary": model.imag,
        "corrected_real": corrected.real,
        "ratio_amplitude": np.abs(corrected / model),
        "residual_phase": np.angle(corrected - model),
        "obs_residual_amplitude": np.abs(observed - model),
        "float_data": single_dish,
    }
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        result = reader.range(list(values), useflags=useflags, blocksize=1)
        assert result.keys() == values.keys()
        for name, data in values.items():
            np.testing.assert_allclose(result[name], extrema(data[samples]), rtol=3e-6,
                                       atol=1e-7, strict=True, err_msg=name)
        assert reader.done()
