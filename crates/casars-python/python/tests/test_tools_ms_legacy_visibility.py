"""Exercise the pre-iterator CASA MSSelector visibility axes."""

from pathlib import Path

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_selection_indices import two_spws_dataset
from test_tools_ms_visibility import same_visibility
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


@pytest.mark.parametrize("rows, increment", [([0, 3, 2, 0], 1), ([19, 13, 0, 6, 2, 11, 3, 8, 7], 2)])
def test_legacy_visibility_keeps_row_order_and_baseline_slots(
    native_dataset, baseline_fixture, rows, increment
):
    casa, _, raw = baseline_fixture
    expected_rows = rows[::increment][:len(rows) // increment]
    requested = ["data", "flag", "flag_row", "flag_sum", "antenna1", "antenna2",
                 "ifr_number", "time", "data_desc_id", "weight", "sigma", "axis_info"]
    results = []
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectold({"rows": rows})
        ordered = reader.getdataold(["data", "time", "ifr_number"], increment=increment)
        np.testing.assert_array_equal(ordered["data"], raw["data"][..., expected_rows], strict=True)
        np.testing.assert_array_equal(ordered["time"], raw["time"][expected_rows], strict=True)
        grid = reader.getdataold(requested, ifraxis=True, ifraxisgap=1, increment=increment)
        assert grid["data"].ndim == 4
        assert grid["flag"].shape == grid["data"].shape
        np.testing.assert_array_equal(grid["flag_sum"], np.count_nonzero(grid["flag"], axis=-1),
                                      strict=True)
        gaps = grid["ifr_number"] == -1
        np.testing.assert_array_equal(grid["ifr_number"][~gaps],
                                      grid["axis_info"]["ifr_axis"]["ifr_number"], strict=True)
        np.testing.assert_array_equal(grid["axis_info"]["ifr_axis"]["baseline"][gaps],
                                      np.full(gaps.sum(), -1.0), strict=True)
        assert grid["flag"][:, :, gaps, :].all()
        assert reader.nrowold(True) == len(rows)
        assert reader.nrow(True) == len(raw["time"])
        assert reader.done()
        results.append(grid)
    same_visibility(results[1], results[0])


def test_legacy_visibility_channel_and_polarization_products(native_dataset, baseline_fixture):
    casa, _, raw = baseline_fixture
    channel_results = []
    polarization_results = []
    requested = ["data", "flag", "weight", "sigma", "axis_info"]
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectchannelold(2, 0, 2, 2)
        assert not reader.selectchannelold(4, 7, 2, 1)
        channels = reader.getdataold(requested)
        assert channels["data"].shape == (2, 2, len(raw["time"]))
        np.testing.assert_array_equal(channels["weight"], raw["weight"], strict=True)
        np.testing.assert_array_equal(channels["flag"],
                                      raw["flag"].reshape(2, 2, 2, -1).all(axis=2), strict=True)
        assert reader.selectinitold(reset=True)
        for polarization in ("LL", ["LL", "RR"], "XX"):
            assert reader.selectpolarizationold(polarization)
            result = reader.getdataold(requested)
            if polarization == "LL":
                np.testing.assert_array_equal(result["data"], raw["data"][1:2], strict=True)
                np.testing.assert_array_equal(result["weight"], raw["weight"], strict=True)
            polarization_results.append(result)
        assert reader.done()
        channel_results.append(channels)
    same_visibility(*channel_results[::-1])
    for reference, native in zip(polarization_results[:3], polarization_results[3:]):
        same_visibility(native, reference)


def test_legacy_polarization_after_a_channel_bin_reinitializes_cleanly(native_dataset, baseline_fixture):
    _, _, raw = baseline_fixture
    reader = tools.ms()
    assert reader.open(str(native_dataset))
    assert reader.selectchannelold(2, 0, 2, 2)
    assert reader.selectpolarizationold("LL")
    selected = reader.getdataold(["data", "axis_info"])
    np.testing.assert_array_equal(selected["data"], raw["data"][1:2], strict=True)
    np.testing.assert_array_equal(selected["axis_info"]["corr_axis"], ["LL"], strict=True)
    assert reader.done()


def test_legacy_visibility_argument_forms_and_labels(native_dataset):
    for api in (pinned_casa(), tools):
        reader = api.ms()
        assert reader.getdataold("time") == {}
        assert reader.selectchannelold() is False
        assert reader.selectpolarizationold() is False
        assert reader.open(str(native_dataset))
        for value in (None, ("time",), b"time", np.array(["time"])):
            assert reader.getdataold(value) == {}
        with pytest.raises(TypeError, match="_items must contain strings"):
            reader.getdataold([1])
        with pytest.raises(TypeError, match="_wantedpol must contain strings"):
            reader.selectpolarizationold([1])
        for value in (None, np.bool_(True)):
            with pytest.raises(SystemError, match="ms_selectchannelold"):
                reader.selectchannelold(value)
        assert reader.selectchannelold(np.int64(2))
        assert reader.selectchannelold(2**32 + 2)
        assert reader.getdataold("axis_info")["axis_info"]["freq_axis"]["chan_freq"].shape == (2, 1)
        assert reader.selectpolarizationold("ll")
        labels = reader.getdataold("axis_info")["axis_info"]["corr_axis"]
        np.testing.assert_array_equal(labels, np.array(["ll"]), strict=True)
        assert reader.selectpolarizationold([])
        labels = reader.getdataold("axis_info")["axis_info"]["corr_axis"]
        np.testing.assert_array_equal(labels, np.array([], dtype="U5"), strict=True)
        assert reader.done()


def test_legacy_negative_baseline_gap_uses_zero_gaps(native_dataset):
    reader = tools.ms()
    assert reader.open(str(native_dataset))
    request = ["data", "axis_info", "ifr_number", "time"]
    regular = reader.getdataold(request, ifraxis=True, ifraxisgap=0)
    negative = reader.getdataold(request, ifraxis=True, ifraxisgap=-2)
    same_visibility(negative, regular)
    assert reader.done()


def test_legacy_read_of_missing_visibility_columns(native_dataset):
    request = ["corrected_data", "corrected_amplitude", "model_data", "model_real",
               "ratio_data", "residual_data", "float_data", "unknown", "avgdata"]
    expected = {
        name: np.array([], dtype=np.complex128 if name.endswith("_data") else np.float64)
        for name in request[:6]
    }
    for api in (pinned_casa(), tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        same_visibility(reader.getdataold(request), expected)
        assert reader.done()

    reader = tools.ms()
    assert reader.open(str(native_dataset))
    missing = np.array([], dtype=np.complex128)
    same_visibility(reader.getdataold(["obs_residual_data"]), {"obs_residual_data": missing})
    same_visibility(reader.getdataold(["corrected_data"], average=True), {"corrected_data": missing})
    assert reader.getdataold("data")["data"].size > 0
    assert reader.done()


def test_legacy_unrecognized_polarization_leaves_the_reader_usable(native_dataset):
    reader = tools.ms()
    assert reader.open(str(native_dataset))
    previous = reader.getdataold(["data", "axis_info"])
    assert reader.selectpolarizationold("bogus") is False
    same_visibility(reader.getdataold(["data", "axis_info"]), previous)
    assert reader.done()


@pytest.mark.parametrize("ifraxis", [False, True])
def test_legacy_average_uses_each_correlations_own_weight(native_dataset, baseline_fixture, ifraxis):
    casa, _, raw = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    index = np.arange(len(raw["time"]), dtype=np.float32) + 1
    weight = np.stack((index, (index[::-1] + 3) ** 2))
    assert table.putcol("WEIGHT", weight)
    assert table.done()

    reference = casa.ms()
    assert reference.open(str(native_dataset))
    stored = reference.getdataold(["data", "flag", "weight", "axis_info"], ifraxis=ifraxis)
    legacy_average = reference.getdataold(["data", "axis_info"], ifraxis=ifraxis, average=True)
    assert reference.done()
    valid = np.where(stored["flag"], 0.0, stored["weight"][:, None, ...])
    scale = valid.sum(axis=-1)
    sums = (stored["data"] * valid).sum(axis=-1)
    expected = np.divide(sums, scale, out=np.zeros_like(sums), where=scale > 0)
    error = np.linalg.norm((legacy_average["data"] - expected).ravel())
    assert error / np.linalg.norm(expected.ravel()) > 0.01

    native = tools.ms()
    assert native.open(str(native_dataset))
    result = native.getdataold(["data", "axis_info"], ifraxis=ifraxis, average=True)
    np.testing.assert_allclose(result["data"], expected, rtol=1e-7, atol=1e-12, strict=True)
    if ifraxis:
        slots = stored["axis_info"]["time_axis"]["MJDseconds"]
        old_time = legacy_average["axis_info"]["time_axis"]["MJDseconds"]
        new_time = result["axis_info"]["time_axis"]["MJDseconds"]
        np.testing.assert_array_equal(old_time, [slots.sum()], strict=True)
        np.testing.assert_array_equal(new_time, [slots.mean()], strict=True)
    assert native.done()


def test_legacy_polarization_uses_the_active_description(two_spws_dataset):
    casa = pinned_casa()
    path = Path(two_spws_dataset)
    table = casa.table()
    assert table.open(str(path / "POLARIZATION"), nomodify=False)
    other = table.nrows()
    assert table.addrows(1)
    for name in table.colnames():
        value = np.array([9, 12], dtype=np.int32) if name == "CORR_TYPE" else table.getcell(name, 0)
        assert table.putcell(name, other, value)
    assert table.done()
    assert table.open(str(path / "DATA_DESCRIPTION"), nomodify=False)
    assert table.putcell("POLARIZATION_ID", 1, other)
    assert table.done()

    results = []
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(path))
        assert reader.selecttaql("DATA_DESC_ID == 0")
        assert reader.selectpolarizationold("LL")
        result = reader.getdataold(["data", "weight", "sigma", "axis_info"])
        assert result["data"].shape == (1, 4, 30)
        assert result["weight"].shape == result["sigma"].shape == (2, 30)
        assert reader.done()
        results.append(result)
    same_visibility(results[1], results[0])


def test_legacy_converted_flag_sums_keep_the_converted_axis(native_dataset, baseline_fixture):
    casa, _, raw = baseline_fixture
    expected = np.count_nonzero(raw["flag"].any(axis=0, keepdims=True), axis=-1)
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectpolarizationold("XX")
        result = reader.getdataold(["data", "flag", "flag_sum"], average=True)
        assert result["data"].shape == (1, raw["data"].shape[1])
        np.testing.assert_array_equal(result["flag_sum"], expected, strict=True)
        assert reader.done()


@pytest.mark.parametrize("convert", [False, True])
def test_legacy_model_ratio_converts_operands_and_fills_zero_model(
    native_dataset, baseline_fixture, convert
):
    casa, _, raw = baseline_fixture
    corrected = raw["data"].astype(np.complex64) + np.complex64(5 + 10j)
    factors = np.array([1 + 2j, 3 + 4j], dtype=np.complex64)
    model = np.broadcast_to(factors[:, None, None], corrected.shape).copy()
    if not convert:
        model[:, :2, 0] = 0
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    column = table.getcoldesc("DATA")
    assert table.addcols({"CORRECTED_DATA": column, "MODEL_DATA": column})
    assert table.putcol("CORRECTED_DATA", corrected)
    assert table.putcol("MODEL_DATA", model)
    assert table.done()

    if convert:
        corrected = np.stack([(corrected[0] + corrected[1]) / 2,
                              (corrected[0] - corrected[1]) / 2])
        model = np.stack([(model[0] + model[1]) / 2, (model[0] - model[1]) / 2])
    expected = np.divide(corrected, model, out=np.ones_like(corrected), where=model != 0)
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        if convert:
            assert reader.selectpolarizationold(["I", "V"])
        result = reader.getdataold("ratio_data")["ratio_data"]
        np.testing.assert_allclose(result, expected.astype(np.complex128),
                                   rtol=3e-7, atol=1e-7, strict=True)
        assert reader.done()


def test_legacy_explicit_baseline_axis_survives_row_cuts(native_dataset, baseline_fixture):
    casa, _, _ = baseline_fixture
    # 3002 is a valid pair that is not present in this MS.
    requested = [3003, 3002, 0, 3003]
    records = []
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectold({"ifr_number": requested})
        assert reader.selecttaqlold("ANTENNA1 == 0")
        result = reader.getdataold(["data", "flag", "ifr_number", "axis_info"], ifraxis=True)
        np.testing.assert_array_equal(result["ifr_number"], requested)
        np.testing.assert_array_equal(result["axis_info"]["ifr_axis"]["ifr_number"], requested)
        assert result["flag"][:, :, [0, 1, 3], :].all()
        records.append(result)
        assert reader.done()
    same_visibility(records[1], records[0])


def test_legacy_averaged_hour_angle_uses_first_field_at_mean_epoch(
    native_dataset, baseline_fixture
):
    casa, _, raw = baseline_fixture
    rows = np.flatnonzero((raw["antenna1"] == 0) & (raw["antenna2"] == 0))[:2]
    first, second = map(int, rows)
    mean = float(raw["time"][rows].mean())

    fields = casa.table()
    assert fields.open(str(native_dataset / "FIELD"), nomodify=False)
    assert fields.addrows(1)
    for name in fields.colnames():
        if fields.iscelldefined(name, 0):
            assert fields.putcell(name, 1, fields.getcell(name, 0))
    direction = fields.getcell("PHASE_DIR", 0)
    direction[0, 0] += 0.3
    assert fields.putcell("PHASE_DIR", 1, direction)
    assert fields.done()
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    assert table.putcell("FIELD_ID", second, 1)
    assert table.done()

    reader = tools.ms()
    assert reader.open(str(native_dataset))
    assert reader.selectold({"rows": rows.tolist()})
    result = reader.getdataold(["axis_info", "ha"], ifraxis=True, average=True)
    axis = result["axis_info"]["time_axis"]
    np.testing.assert_array_equal(axis["MJDseconds"], [mean], strict=True)
    assert reader.done()

    # Use CASA's unaveraged coordinate at the corrected epoch and first direction.
    assert table.open(str(native_dataset), nomodify=False)
    assert table.putcell("TIME", first, mean)
    assert table.done()
    reference = casa.ms()
    assert reference.open(str(native_dataset))
    assert reference.selectold({"rows": [first]})
    expected = reference.getdataold(["axis_info", "ha"], ifraxis=True)
    np.testing.assert_allclose(axis["HA"], expected["axis_info"]["time_axis"]["HA"],
                               rtol=0, atol=0.015, strict=True)
    assert reference.done()
