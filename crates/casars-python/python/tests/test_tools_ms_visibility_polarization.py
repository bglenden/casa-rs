"""Pinned CASA and analytic controls for public MS polarization selection."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_selection_indices import two_spws_dataset
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset
from test_tools_simulator import configure


@pytest.mark.parametrize(
    "argument,indices,labels",
    [
        ("RR", [0], ["RR"]),
        ("0:ll", [1], ["LL"]),
        (["LL", "RR"], [1, 0], ["LL", "RR"]),
        (["RR", "RR"], [0], ["RR"]),
    ],
)
def test_ms_stored_polarizations_gather_each_column_in_reference_time_order(
    baseline_fixture, argument, indices, labels,
):
    casa, select_rows, expected = baseline_fixture
    rows = [19, 13, 0, 6, 2, 11, 3, 8]
    path = select_rows("polarization-order.ms", rows)
    ordered = sorted(rows, key=lambda row: expected["time"][row])
    requested = ["axis_info", "data", "flag", "weight", "sigma", "time", "antenna1", "ifr_number"]
    for api in (casa, tools):
        reader = api.ms()
        assert not reader.selectpolarization(argument)
        assert reader.open(path)
        assert reader.selectpolarization(argument)
        record = reader.getdata(requested)
        np.testing.assert_array_equal(record["axis_info"]["corr_axis"], labels)
        for name in ("data", "flag", "weight", "sigma"):
            np.testing.assert_array_equal(record[name], expected[name][indices][..., ordered], strict=True)
        for name in ("time", "antenna1"):
            np.testing.assert_array_equal(record[name], expected[name][ordered], strict=True)
        baselines = (1000 * expected["antenna1"] + expected["antenna2"])[ordered].astype(np.int64)
        np.testing.assert_array_equal(record["ifr_number"], baselines, strict=True)
        assert reader.nrow(True) == len(rows)
        assert reader.reset()
        np.testing.assert_array_equal(reader.getdata("data")["data"], expected["data"][..., rows], strict=True)
        assert reader.done()


@pytest.mark.parametrize("labels", [["I", "V"], ["Q", "U"], ["XX"], ["LL", "I"]])
def test_ms_parallel_hand_conversion_preserves_auxiliary_columns(
    baseline_fixture, native_dataset, labels,
):
    casa, _, expected = baseline_fixture
    rr, ll = expected["data"].astype(np.complex64)
    intensity, circular = (rr + ll) / 2, (rr - ll) / 2
    zero = np.zeros_like(intensity)
    analytic = {"I": intensity, "V": circular, "Q": zero, "U": zero,
                "XX": intensity, "LL": ll}
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectpolarization(labels)
        record = reader.getdata(["axis_info", "data", "flag", "weight", "sigma"])
        np.testing.assert_array_equal(record["axis_info"]["corr_axis"], labels)
        np.testing.assert_allclose(record["data"], np.stack([analytic[name] for name in labels]),
                                   rtol=1e-7, atol=1e-7)
        assert record["data"].dtype == np.complex128
        for name in ("flag", "weight", "sigma"):
            np.testing.assert_array_equal(record[name], expected[name], strict=True)
        assert reader.done()


@pytest.mark.parametrize("products", ["RR RL LR LL", "XX XY YX YY"])
def test_ms_four_stored_products_convert_to_stokes_with_selected_channels(
    tmp_path, measures, products,
):
    path = str(tmp_path / "full-polarizations.ms")
    simulator = tools.simulator()
    assert simulator.open(path)
    configure(simulator, stokes=products)
    assert simulator.observe("F", "L", starttime="0s", stoptime="30s")
    assert simulator.close()
    casa = pinned_casa()
    table = casa.table()
    assert table.open(path, nomodify=False)
    shape = table.getcol("DATA").shape
    values = np.arange(1, np.prod(shape) + 1, dtype=np.float32).reshape(shape, order="F")
    visibilities = (values + 0.25j * values[::-1]).astype(np.complex64)
    assert table.putcol("DATA", visibilities)
    assert table.done()
    a, b, c, d = visibilities[:, ::2]
    if products.startswith("RR"):
        iquv = [(a + d) / 2, (b + c) / 2, (b - c) / (2j), (a - d) / 2]
    else:
        iquv = [(a + d) / 2, (a - d) / 2, (b + c) / 2, (b - c) / (2j)]
    intensity, q, u, v = iquv
    ptotal = np.sqrt(np.abs(q)**2 + np.abs(u)**2 + np.abs(v)**2)
    pangle = np.arctan2(u.real, q.real) / 2

    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        assert reader.selectchannel(2, 0, 1, 2)
        for labels, analytic in (
            (["I", "Q", "U", "V"], iquv),
            (["Ptotal", "PFtotal", "Pangle"], [ptotal, ptotal / np.abs(intensity), pangle]),
        ):
            assert reader.selectpolarization(labels)
            result = reader.getdata(["data", "axis_info"])
            np.testing.assert_array_equal(result["axis_info"]["corr_axis"], labels)
            np.testing.assert_allclose(result["data"], analytic, atol=2e-7, rtol=2e-7)
        assert reader.done()


def test_ms_polarization_selectors_preserve_or_clear_state_like_casa(native_dataset):
    for api in (pinned_casa(), tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectpolarization(["I"])
        for invalid in ([], None, "", ("RR",), np.array(["RR"]), b"RR"):
            assert reader.selectpolarization(invalid) is False
            np.testing.assert_array_equal(reader.getdata("axis_info")["axis_info"]["corr_axis"], ["I"])
        with pytest.raises(TypeError, match="list _wantedpol must contain strings"):
            reader.selectpolarization(["RR", 1])
        assert reader.selectpolarization(["missing"]) is False
        np.testing.assert_array_equal(reader.getdata("axis_info")["axis_info"]["corr_axis"], ["RR", "LL"])
        assert reader.selectpolarization("0:ll")
        np.testing.assert_array_equal(reader.getdata("axis_info")["axis_info"]["corr_axis"], ["LL"])
        assert reader.done()


def test_ms_polarization_empty_request_still_initializes_mixed_shapes(two_spws_dataset):
    for api in (pinned_casa(), tools):
        reader = api.ms()
        assert reader.open(two_spws_dataset)
        original = reader.nrow(True)
        with pytest.raises(TypeError, match="list _wantedpol must contain strings"):
            reader.selectpolarization([1])
        assert reader.nrow(True) == original
        assert reader.selectpolarization() is False
        assert 0 < reader.nrow(True) < original
        assert reader.getdata("data")["data"].shape == (2, 4, reader.nrow(True))
        assert reader.done()


@pytest.mark.parametrize("ifraxis", [False, True])
def test_ms_averages_converted_stokes_with_corresponding_flags_and_variance(
    baseline_fixture, native_dataset, ifraxis,
):
    casa, _, raw = baseline_fixture
    path = str(native_dataset)
    row = np.arange(raw["data"].shape[-1], dtype=np.float32)
    weights = np.array([1 + row % 7, 3 + row % 11], dtype=np.float32)
    table = casa.table()
    assert table.open(path, nomodify=False)
    assert table.putcol("WEIGHT", weights)
    assert table.done()

    rr, ll = raw["data"].astype(np.complex64)
    visibility = (rr + ll) * 0.5
    propagated = 1 / (0.25 / weights[0] + 0.25 / weights[1])
    good = ~raw["flag"].any(axis=0)
    numbers = raw["antenna1"] * 1000 + raw["antenna2"]
    groups = ([numbers == baseline for baseline in np.unique(numbers)] if ifraxis
              else [np.ones_like(numbers, dtype=bool)])
    means = []
    flags = []
    for group in groups:
        weighted = np.where(good[:, group], propagated[group], 0)
        denominators = weighted.sum(axis=-1)
        sums = (visibility[:, group] * weighted).sum(axis=-1)
        means.append(np.divide(sums, denominators, where=denominators > 0,
                               out=np.zeros_like(sums)))
        flags.append(denominators == 0)
    data = np.stack(means, axis=-1)[None, ...]
    masks = np.stack(flags, axis=-1)[None, ...]
    if not ifraxis:
        data = data[..., 0]

    fields = ["data", "flag", "weight", "sigma"]
    reference = casa.ms()
    assert reference.open(path)
    assert reference.selectpolarization(["I"])
    original = reference.getdata(fields, average=True, ifraxis=ifraxis)
    assert np.linalg.norm(original["data"] - data) / np.linalg.norm(data) > 0.01
    assert reference.done()

    reader = tools.ms()
    assert reader.open(path)
    assert reader.selectpolarization(["I"])
    result = reader.getdata(fields, average=True, ifraxis=ifraxis)
    assert result["data"].dtype == np.complex128
    np.testing.assert_allclose(result["data"], data, rtol=3e-7)
    np.testing.assert_array_equal(result["flag"], masks, strict=True)
    np.testing.assert_array_equal(result["weight"], original["weight"], strict=True)
    np.testing.assert_allclose(result["sigma"], original["sigma"], rtol=1e-7)
    assert reader.done()
