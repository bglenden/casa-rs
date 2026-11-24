"""Pinned and independent baseline-axis averaging cases for the next slice."""

import numpy as np

from casars import tools
from test_tools_ms_visibility import same_visibility
from test_tools_ms_visibility_ifraxis import baseline_fixture, scatter
from test_tools_msmetadata import measures, native_dataset


def test_ms_getdata_average_complete_baseline_grid(baseline_fixture):
    casa, view, expected = baseline_fixture
    rows = list(range(len(expected["time"])))
    path = view("complete-integrations.ms", rows[::-1])
    fields = ["data", "amplitude", "phase", "flag", "flag_sum", "weight", "sigma",
              "axis_info", "time", "uvw", "u", "antenna1", "antenna2", "scan_number",
              "ifr_number"]
    baseline = sorted(set(1000 * expected["antenna1"] + expected["antenna2"]))
    times = sorted(set(expected["time"]))
    samples = scatter(expected, "data", rows, times, baseline)
    flags = scatter(expected, "flag", rows, times, baseline)
    good = (~flags).sum(axis=-1)
    sample_sum = np.where(flags, 0, samples).sum(axis=-1)
    means = np.divide(sample_sum, good, out=np.zeros_like(sample_sum), where=good > 0)
    means = means.astype(np.complex64).astype(np.complex128)
    replies = []
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        first_items = ["data", "flag", "weight"]
        first = reader.getdata(first_items, ifraxis=True, average=True)
        result = reader.getdata(fields, ifraxis=True, average=True)
        same_visibility(first, {name: result[name] for name in first_items})
        assert reader.nrow(True) == len(rows)
        assert reader.done()
        np.testing.assert_allclose(result["data"], means, rtol=3e-6, atol=1e-7)
        np.testing.assert_array_equal(result["flag"], good == 0, strict=True)
        np.testing.assert_array_equal(result["flag_sum"], flags.sum(axis=-1), strict=True)
        np.testing.assert_array_equal(result["time"], np.array([np.mean(times)]), strict=True)
        np.testing.assert_array_equal(result["weight"],
                                      scatter(expected, "weight", rows, times, baseline), strict=True)
        np.testing.assert_allclose(result["sigma"], np.sqrt(len(times)), rtol=1e-7)
        replies.append(result)
    same_visibility(replies[1], replies[0])


def test_ms_getdata_average_sparse_baselines_use_their_correlation_weights(baseline_fixture, native_dataset):
    casa, view, expected = baseline_fixture
    reader = casa.table()
    assert reader.open(str(native_dataset), nomodify=False)
    weight = np.array([1 + np.arange(len(expected["time"])),
                       10 + 3 * np.arange(len(expected["time"]))], dtype=np.float32)
    assert reader.putcol("WEIGHT", weight)
    assert reader.done()
    expected["weight"] = weight.astype(np.float64)
    rows = [19, 13, 0, 6, 2, 11, 3, 8, 7, 17]
    path = view("sparse-weighted.ms", rows)
    baselines = [0, 1, 2, 3, -1, 1003, -1, 2002, 2003, -1, 3003]
    times = sorted({expected["time"][row] for row in rows})
    data = scatter(expected, "data", rows, times, baselines)
    flags = scatter(expected, "flag", rows, times, baselines)
    weights = scatter(expected, "weight", rows, times, baselines)
    good = (~flags) & (weights[:, None, :, :] > 0)
    total = np.where(good, weights[:, None, :, :], 0).sum(axis=-1)
    sums = (np.where(good, data, 0) * weights[:, None, :, :]).sum(axis=-1)
    means = np.divide(sums, total, out=np.zeros_like(sums), where=total > 0).astype(np.complex64)

    native = tools.ms()
    assert native.open(path)
    items = ["data", "phase", "amplitude", "flag", "flag_sum", "weight", "axis_info"]
    result = native.getdata(items, ifraxis=True, ifraxisgap=1, average=True)
    np.testing.assert_array_equal(result["data"], means.astype(np.complex128), strict=True)
    np.testing.assert_array_equal(result["flag"], ~good.any(axis=-1), strict=True)
    np.testing.assert_array_equal(result["flag_sum"], flags.sum(axis=-1), strict=True)
    np.testing.assert_array_equal(result["weight"], weights, strict=True)
    np.testing.assert_allclose(result["amplitude"], np.abs(means), rtol=2e-7)
    np.testing.assert_allclose(result["phase"], np.angle(means), rtol=2e-7)
    assert result["axis_info"]["ifr_axis"]["ifr_number"].tolist() == baselines
    assert native.nrow(True) == len(rows)
    assert native.done()


def test_ms_getdata_average_single_integration_retains_baselines(baseline_fixture):
    _, view, expected = baseline_fixture
    rows = [3, 2, 0, 4]
    reader = tools.ms()
    assert reader.open(view("one-averaged-integration.ms", rows))
    result = reader.getdata(["data", "flag", "sigma", "axis_info", "ut"], ifraxis=True, average=True)
    ordered = sorted(rows, key=lambda row: (expected["antenna1"][row], expected["antenna2"][row]))
    flags = expected["flag"][..., ordered]
    data = np.where(flags, 0, expected["data"][..., ordered])
    np.testing.assert_array_equal(result["data"], data, strict=True)
    np.testing.assert_array_equal(result["flag"], flags, strict=True)
    np.testing.assert_array_equal(result["sigma"], expected["sigma"][..., ordered], strict=True)
    np.testing.assert_array_equal(result["axis_info"]["time_axis"]["UT"], np.array([5.0]), strict=True)
    assert reader.done()
