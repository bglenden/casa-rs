"""Pinned controls for the public MS range query on selected rows."""
import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_selection_indices import two_spws_dataset
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import ephemeris_dataset, measures, native_dataset


def extrema(values):
    return np.array([values.min(), values.max()], dtype=np.float64)


@pytest.mark.parametrize("polarization", [None, "LL", "XX"])
@pytest.mark.parametrize("useflags", [False, True])
def test_legacy_range_uses_projected_samples_and_recorded_spectral_axes(
    baseline_fixture, native_dataset, polarization, useflags
):
    casa, _, raw = baseline_fixture
    flagged = raw["flag"].copy()
    flagged[:, :2, 19] = True
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    assert table.putcol("FLAG", flagged)
    assert table.done()
    items = ["real", "imaginary", "amplitude", "phase", "weight", "sigma",
             "chan_freq", "num_chan", "num_corr", "corr_names", "rows"]
    order = [19, 0, 13, 19, 7]
    independent = None
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectinitold()
        if polarization:
            assert reader.selectpolarizationold(polarization)
        assert reader.selectchannelold(2, 0, 2, 2)
        assert reader.selectold({"rows": order})
        result = reader.rangeold(items, useflags=useflags, blocksize=np.int64(1))
        assert result.keys() == set(items)
        if api is casa:
            selected = reader.getdataold(["data", "flag", "weight", "sigma"])
            values = selected["data"]
            values = values[~selected["flag"]] if useflags else values.ravel()
            independent = {
                "real": extrema(values.real),
                "imaginary": extrema(values.imag),
                "amplitude": extrema(np.abs(values)),
                "phase": extrema(np.angle(values)),
                "weight": extrema(selected["weight"]),
                "sigma": extrema(selected["sigma"]),
                "chan_freq": reader.range("chan_freq")["chan_freq"],
                "num_chan": np.array([4]),
                "num_corr": np.array([2]),
                "corr_names": np.array([["RR"], ["LL"]]),
                "rows": np.array(order),
            }
        for item in items:
            expected = independent[item]
            if expected.dtype.kind == "f":
                np.testing.assert_allclose(result[item], expected, rtol=1e-6, atol=1e-12,
                                           strict=True, err_msg=item)
            else:
                np.testing.assert_array_equal(result[item], expected, strict=True, err_msg=item)
        assert reader.nrow(True) == len(raw["time"])
        assert reader.nrowold(True) == len(order)
        assert reader.done()


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


def test_legacy_float_range_keeps_flags_on_the_recorded_correlation_axis(native_dataset):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    shape = table.getcol("DATA").shape
    descriptor = table.getcoldesc("DATA")
    descriptor.update(valueType="float", dataManagerType="StandardStMan", dataManagerGroup="FLOAT_DATA")
    assert table.addcols({"FLOAT_DATA": descriptor})
    values = np.arange(1, np.prod(shape) + 1, dtype=np.float32).reshape(shape, order="F")
    assert table.putcol("FLOAT_DATA", values)
    flags = np.zeros(shape, dtype=bool)
    flags[0, 0, 0] = True
    assert table.putcol("FLAG", flags)
    assert table.done()

    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectpolarizationold("XX")
        result = reader.rangeold("float_data", useflags=False)
        np.testing.assert_array_equal(result["float_data"], extrema(values), strict=True)
        if api is tools:
            masked = reader.rangeold("float_data")
            np.testing.assert_array_equal(masked["float_data"], extrema(values[~flags]), strict=True)
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


def test_ms_range_retains_selected_row_numbers_after_main_shrinks(native_dataset, tmp_path):
    casa = pinned_casa()
    for api in (casa, tools):
        path = str(tmp_path / f"{api.__name__}-standard.ms")
        table = casa.table()
        assert table.open(str(native_dataset))
        manager = {"*1": {"TYPE": "StandardStMan", "NAME": "Main",
                           "COLUMNS": table.colnames(), "SPEC": {}}}
        expected = np.flatnonzero(table.getcol("ANTENNA1") == 0)
        assert len(expected) > 1
        copy = table.copy(path, deep=True, valuecopy=True, dminfo=manager)
        assert copy.done()
        assert table.done()
        reader = api.ms()
        assert reader.open(path)
        assert reader.msselect({"taql": "ANTENNA1 == 0"})
        np.testing.assert_array_equal(reader.range("rows")["rows"], expected, strict=True)

        assert table.open(path, nomodify=False)
        assert table.removerows(list(range(1, table.nrows())))
        assert table.done()
        np.testing.assert_array_equal(reader.range("rows")["rows"], expected, strict=True)
        assert reader.nrow() == 1
        assert reader.done()


def test_ms_reference_weight_and_sigma_keep_projected_rows(baseline_fixture, native_dataset):
    casa, view, raw = baseline_fixture
    weights = np.arange(1, raw["weight"].size + 1, dtype=np.float32)
    weights = weights.reshape(raw["weight"].shape)
    values = {"weight": weights, "sigma": 2 * weights + 1}
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    for name, vector in values.items():
        assert table.putcol(name.upper(), vector)
    assert table.done()
    rows = [29, 3, 0, 11, 3, 25]
    path = view("vector-order.ms", rows)
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        reads = reader.getdata(list(values))
        ranges = reader.range(list(values))
        for name, vector in values.items():
            expected = vector[:, rows].astype(np.float64)
            np.testing.assert_array_equal(reads[name], expected, strict=True)
            np.testing.assert_array_equal(ranges[name], extrema(expected), strict=True)
        assert reader.done()


def test_ms_range_reads_only_selected_incremental_cells(baseline_fixture, native_dataset, tmp_path):
    casa, _, raw = baseline_fixture
    path = tmp_path / "incremental.ms"
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    weights = np.arange(1, raw["weight"].size + 1, dtype=np.float32).reshape(raw["weight"].shape)
    assert table.putcol("WEIGHT", weights)
    managers = table.getdminfo()
    for manager in managers.values():
        if {"DATA", "WEIGHT"}.intersection(manager["COLUMNS"]):
            manager.update(TYPE="IncrementalStMan", SPEC={})
    copy = table.copy(str(path), deep=True, valuecopy=True, dminfo=managers)
    reference = copy.selectrows([0], str(tmp_path / "first-row.ms"))
    assert reference.done()
    sequence = next(manager["SEQNR"] for manager in copy.getdminfo().values()
                    if "DATA" in manager["COLUMNS"])
    assert copy.done()
    assert table.done()

    readers = [api.ms() for api in (casa, tools)]
    for reader in readers:
        assert reader.open(str(tmp_path / "first-row.ms"))
    # Damage only the final, unselected cell of this disposable copy. Reading
    # the selected row must never require the rest of the indirect array file.
    payload = path / f"table.f{sequence}i"
    with payload.open("r+b") as file:
        file.truncate(payload.stat().st_size - 32)
    for reader in readers:
        result = reader.range(["real", "weight"], useflags=False, blocksize=1)
        np.testing.assert_array_equal(result["real"], extrema(raw["data"][..., 0].real), strict=True)
        np.testing.assert_array_equal(result["weight"], extrema(weights[:, 0]), strict=True)
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


def test_ms_range_phase_direction_uses_ephemeris_column_reference(ephemeris_dataset):
    dataset, _ = ephemeris_dataset
    answers = []
    for api in (pinned_casa(), tools):
        reader = api.ms()
        assert reader.open(str(dataset))
        phase = reader.range("phase_dir")["phase_dir"]
        assert phase["epoch"] == "J2000"
        answers.append(phase["direction"])
        assert reader.done()
    np.testing.assert_allclose(*answers, rtol=1e-13, strict=True)


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


def test_ms_range_uses_unity_at_zero_model_samples(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    shape = raw["data"].shape
    corrected = np.full(shape, 10 + 10j, dtype=np.complex64)
    model = np.full(shape, 2 + 0j, dtype=np.complex64)
    model[0, 0, 0] = 0.0
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    for name, data in [("MODEL_DATA", model), ("CORRECTED_DATA", corrected)]:
        description = dict(table.getcoldesc("DATA"), dataManagerType="StandardStMan",
                           dataManagerGroup=name)
        assert table.addcols({name: description})
        assert table.putcol(name, data)
    assert table.done()
    result = {"ratio_amplitude": [1.0, np.sqrt(50.0)], "ratio_real": [1.0, 5.0],
              "ratio_imaginary": [0.0, 5.0], "ratio_phase": [0.0, np.pi / 4]}
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        actual = reader.range(list(result), useflags=False)
        assert actual.keys() == result.keys()
        for name, expected in result.items():
            np.testing.assert_allclose(actual[name], expected, rtol=1e-6, strict=True)
        assert reader.done()


def test_ms_float_range_keeps_reader_alive_when_all_samples_are_flagged(baseline_fixture,
                                                                        native_dataset):
    casa, _, raw = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    description = dict(table.getcoldesc("DATA"), valueType="float",
                       dataManagerType="StandardStMan", dataManagerGroup="FLOAT_DATA")
    assert table.addcols({"FLOAT_DATA": description})
    samples = raw["data"].real.astype(np.float32)
    assert table.putcol("FLOAT_DATA", samples)
    assert table.putcol("FLAG", np.ones(raw["flag"].shape, dtype=bool))
    assert table.done()
    # CASA 6.7.6.14 aborts in MaskedArray::minMax with no unmasked sample.
    reader = tools.ms()
    assert reader.open(str(native_dataset))
    np.testing.assert_array_equal(reader.range("float_data")["float_data"], [0., 0.], strict=True)
    np.testing.assert_array_equal(reader.range("float_data", useflags=False)["float_data"],
                                  extrema(samples), strict=True)
    assert reader.done()


def test_ms_range_observes_published_subtable_edits(baseline_fixture, native_dataset):
    casa, make_view, _ = baseline_fixture
    path = make_view("range-live-metadata.ms", [0, 10, 20])
    readers = [api.ms() for api in (casa, tools)]
    for reader in readers:
        assert reader.open(path)
    original = readers[0].range("chan_freq")["chan_freq"]
    np.testing.assert_array_equal(readers[1].range("chan_freq")["chan_freq"],
                                  original, strict=True)
    expected = original + 2500.0
    table = casa.table()
    assert table.open(str(native_dataset / "SPECTRAL_WINDOW"), nomodify=False)
    assert table.putcell("CHAN_FREQ", 0, expected[:, 0])
    assert table.done()
    for reader in readers:
        np.testing.assert_array_equal(reader.range("chan_freq")["chan_freq"],
                                      expected, strict=True)
        assert reader.done()
