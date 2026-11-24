"""Pinned controls for public MS visibility row and channel selection."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_selection_indices import two_spws_dataset
from test_tools_ms_visibility import same_visibility
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


def test_ms_selectinit_uses_the_active_view_and_resets_to_original(two_spws_dataset):
    casa = pinned_casa()
    for api in (casa, tools):
        reader = api.ms()
        assert reader.selectinit() is False
        assert reader.open(two_spws_dataset)
        total = reader.nrow(True)
        assert reader.selectinit()
        first = reader.nrow(True)
        assert 0 < first < total
        assert reader.getdata("data")["data"].shape == (2, 4, first)
        assert reader.selectchannel(2, 1, 1, 2)
        subset = reader.getdata(["data", "axis_info"])
        assert subset["data"].shape == (2, 2, first)
        assert reader.selectinit()
        same_visibility(reader.getdata(["data", "axis_info"]), subset)
        for invalid in (-1, None, np.bool_(False)):
            with pytest.raises(RuntimeError, match="positive integers"):
                reader.selectinit(invalid)
        assert reader.nrow(True) == first
        assert reader.selectinit(1) is False
        assert reader.nrow(True) == 0
        assert reader.selectinit(-1, reset=True)
        assert reader.nrow(True) == total
        assert reader.selectinit(np.int64(1))
        assert reader.getdata("data")["data"].shape == (2, 3, total - first)
        assert reader.selectchannel(2, 0, 1, 2)
        assert reader.getdata("data")["data"].shape == (2, 2, total - first)
        assert reader.done()


@pytest.mark.parametrize("start,width,increment", [(0, 1, 2), (0, 2, 2), (0, 3, 1)])
def test_ms_channel_selection_scientific_bins_and_reset(baseline_fixture, native_dataset, start, width, increment):
    casa, _, expected = baseline_fixture
    rows = len(expected["time"])
    cases = []
    count = 2
    data = expected["data"]
    flags = expected["flag"]
    means = []
    counts = []
    for channel in range(count):
        first = start + channel * increment
        group = slice(first, first + width)
        selected_data = data[:, group, :]
        selected_flags = flags[:, group, :]
        valid = (~selected_flags).sum(axis=1)
        if width == 1:
            means.append(selected_data[:, 0, :])
        else:
            summed = np.where(selected_flags, 0, selected_data).sum(axis=1)
            means.append(np.divide(summed, valid, where=valid > 0, out=np.zeros_like(summed)))
        counts.append(valid == 0)
    want_data = np.stack(means, axis=1).astype(np.complex64).astype(np.complex128)
    want_flags = np.stack(counts, axis=1)
    for api in (casa, tools):
        reader = api.ms()
        assert reader.selectchannel() is False
        assert reader.open(str(native_dataset))
        assert reader.selectchannel()
        assert reader.getdata("data")["data"].shape == (2, 1, rows)
        for invalid in (None, np.bool_(True)):
            with pytest.raises(SystemError):
                reader.selectchannel(invalid)
        with pytest.raises(TypeError, match="must be an integer"):
            reader.selectchannel(1.0)
        assert reader.getdata("data")["data"].shape == (2, 1, rows)
        assert reader.selectchannel(count, start, width, increment)
        fields = ["data", "flag", "axis_info", "weight", "sigma", "time"]
        reply = reader.getdata(fields)
        np.testing.assert_allclose(reply["data"], want_data, atol=1e-7, rtol=2e-7)
        np.testing.assert_array_equal(reply["flag"], want_flags, strict=True)
        assert reader.nrow(True) == rows
        assert reader.selectchannel(count, 10, width, increment) is False
        same_visibility(reader.getdata(fields), reply)
        assert reader.reset()
        np.testing.assert_array_equal(reader.getdata("data")["data"], data, strict=True)
        assert reader.done()
        cases.append(reply)
    same_visibility(cases[1], cases[0])


def test_ms_channel_selection_rejects_end_at_channel_count(baseline_fixture, native_dataset):
    _, _, expected = baseline_fixture
    reader = tools.ms()
    assert reader.open(str(native_dataset))
    assert reader.selectchannel(1, 1, 1, 1)
    assert reader.selectchannel(3, 0, 1, 2) is False
    result = reader.getdata("data")["data"]
    np.testing.assert_array_equal(result, expected["data"][:, 1:2], strict=True)
    assert reader.done()


def test_ms_channel_means_use_observed_sigma_and_corrected_weights(baseline_fixture, native_dataset):
    casa, _, expected = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    complex_desc = table.getcoldesc("DATA")
    float_desc = {**complex_desc, "valueType": "float"}
    assert table.addcols({"CORRECTED_DATA": complex_desc, "WEIGHT_SPECTRUM": float_desc,
                          "SIGMA_SPECTRUM": float_desc})
    visibility = expected["data"].astype(np.complex64)
    corrected = visibility + np.complex64(10j)
    shape = visibility.shape
    sigma = np.broadcast_to(np.array([1, 2, 1, 2], dtype=np.float32)[None, :, None], shape).copy()
    weights = np.broadcast_to(np.array([2, 8, 2, 8], dtype=np.float32)[None, :, None], shape).copy()
    assert table.putcol("CORRECTED_DATA", corrected)
    assert table.putcol("SIGMA_SPECTRUM", sigma)
    assert table.putcol("WEIGHT_SPECTRUM", weights)
    assert table.done()

    def weighted(data, weights):
        flag = expected["flag"].reshape(shape[0], 2, 2, shape[2])
        data = data.reshape(flag.shape)
        weights = np.where(flag, 0, weights.reshape(flag.shape))
        total = weights.sum(axis=2)
        sums = (weights * data).sum(axis=2)
        return np.divide(sums, total, where=total > 0, out=np.zeros_like(sums)).astype(np.complex128)

    results = []
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectchannel(2, 0, 2, 2)
        result = reader.getdata(["data", "corrected_data", "flag", "weight", "sigma"])
        np.testing.assert_allclose(result["data"], weighted(visibility, 1 / sigma ** 2), rtol=3e-7)
        np.testing.assert_allclose(result["corrected_data"], weighted(corrected, weights), rtol=3e-7)
        assert reader.done()
        results.append(result)
    same_visibility(results[1], results[0])


@pytest.mark.parametrize("empty_first_cell", [False, True])
def test_ms_channel_means_use_row_weights_for_unpopulated_spectra(
    baseline_fixture, native_dataset, empty_first_cell,
):
    casa, _, _ = baseline_fixture
    fields = ["data", "flag", "weight", "sigma"]
    reference = casa.ms()
    assert reference.open(str(native_dataset))
    assert reference.selectchannel(2, 0, 2, 2)
    expected = reference.getdata(fields)
    assert reference.done()

    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    desc = table.getcoldesc("DATA")
    desc.pop("shape", None)
    desc.update(option=0, ndim=2, valueType="float")
    columns = ("WEIGHT_SPECTRUM", "SIGMA_SPECTRUM")
    assert table.addcols({column: desc for column in columns})
    if empty_first_cell:
        for column in columns:
            assert table.putcell(column, 0, np.zeros((2, 0), np.float32))
    assert table.done()

    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectchannel(2, 0, 2, 2)
        same_visibility(reader.getdata(fields), expected)
        assert reader.done()


def test_ms_channel_selection_stably_orders_reference_rows_by_time(baseline_fixture):
    casa, view, expected = baseline_fixture
    rows = [19, 13, 0, 6, 2, 11, 3, 8]
    sorted_rows = sorted(rows, key=lambda row: expected["time"][row])
    path = view("selected-channels-ref.ms", rows)
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        np.testing.assert_array_equal(reader.getdata("time")["time"], expected["time"][rows], strict=True)
        assert reader.selectchannel(2, 0, 1, 2)
        selected = reader.getdata(["data", "time", "flag", "ifr_number"])
        np.testing.assert_array_equal(selected["data"],
                                      expected["data"][:, ::2, sorted_rows], strict=True)
        np.testing.assert_array_equal(selected["time"], expected["time"][sorted_rows], strict=True)
        np.testing.assert_array_equal(selected["flag"], expected["flag"][:, ::2, sorted_rows], strict=True)
        baselines = 1000 * expected["antenna1"][sorted_rows] + expected["antenna2"][sorted_rows]
        np.testing.assert_array_equal(selected["ifr_number"], baselines.astype(np.int64), strict=True)
        assert reader.nrow(True) == len(rows)
        assert reader.done()


def test_ms_channel_groups_feed_baseline_and_time_averaging(baseline_fixture, native_dataset):
    casa, _, expected = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    assert table.putcol("FLAG", np.zeros_like(expected["flag"]))
    assert table.done()
    grouped = []
    averaged = []
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectchannel(2, 0, 2, 2)
        items = ["data", "flag", "weight", "sigma", "time", "axis_info"]
        grid = reader.getdata(items, ifraxis=True)
        mean = reader.getdata(items, ifraxis=True, average=True)
        assert grid["data"].shape == (2, 2, 10, 3)
        assert not grid["flag"].any() and not mean["flag"].any()
        np.testing.assert_array_equal(grid["weight"], np.full((2, 10, 3), 2.0), strict=True)
        np.testing.assert_allclose(mean["data"], grid["data"].mean(axis=-1), rtol=2e-7)
        assert reader.nrow(True) == 30
        assert reader.done()
        grouped.append(grid)
        averaged.append(mean)
    same_visibility(grouped[1], grouped[0])
    same_visibility(averaged[1], averaged[0])


def test_ms_channel_averaging_recalculates_row_flags(baseline_fixture, native_dataset):
    casa, _, expected = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    flags = np.zeros_like(expected["flag"])
    flags[..., 0] = True
    assert table.putcol("FLAG", flags)
    row_flags = np.zeros(flags.shape[-1], dtype=bool)
    row_flags[2] = True
    assert table.putcol("FLAG_ROW", row_flags)
    assert table.done()

    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectchannel(2, 0, 1, 2)
        np.testing.assert_array_equal(reader.getdata("flag_row")["flag_row"], row_flags, strict=True)
        assert reader.selectchannel(2, 0, 2, 2)
        reply = reader.getdata(["flag_row", "flag", "weight", "sigma"])
        np.testing.assert_array_equal(reply["flag_row"], flags.all(axis=(0, 1)), strict=True)
        np.testing.assert_array_equal(reply["weight"], np.full((2, len(row_flags)), 2.0), strict=True)
        np.testing.assert_allclose(reply["sigma"], 1 / np.sqrt(2), rtol=1e-7)
        assert reader.done()
