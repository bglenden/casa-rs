"""Exercise the pre-iterator CASA MSSelector visibility axes."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
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
