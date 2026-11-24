"""Pinned CASA and analytic controls for public MS polarization selection."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_selection_indices import two_spws_dataset
from test_tools_ms_visibility_channel import add_description
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
    path, casa, visibilities = full_polarization_dataset(tmp_path, products)
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


def full_polarization_dataset(directory, products):
    path = str(directory / "full-polarizations.ms")
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
    return path, casa, visibilities


@pytest.mark.parametrize("left_circular", [True, False])
def test_ms_mixed_receptor_basis_rotates_to_stokes(tmp_path, measures, left_circular):
    path, casa, visibilities = full_polarization_dataset(tmp_path, "RR RL LR LL")
    table = casa.table()
    assert table.open(f"{path}/POLARIZATION", nomodify=False)
    code = 13 if left_circular else 17
    assert table.putcell("CORR_TYPE", 0, np.arange(code, code + 4, dtype=np.int32))
    assert table.done()

    circular = np.array([[1, 1j], [1, -1j]]) / np.sqrt(2)
    linear = np.eye(2)
    left, right = (circular, linear) if left_circular else (linear, circular)
    shape = visibilities.shape
    coherency = np.einsum("ia,ijcr,jb->abcr", left.conj(), visibilities.reshape(2, 2, *shape[1:]), right)
    xx, xy, yx, yy = coherency.reshape(shape) * (np.sqrt(2) / 4)
    stokes = [xx + yy, xx - yy, xy + yx, (xy - yx) / 1j]
    labels = ["I", "Q", "U", "V"]

    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        assert reader.selectpolarization(labels)
        result = reader.getdata(["data", "axis_info"])
        np.testing.assert_array_equal(result["axis_info"]["corr_axis"], labels)
        np.testing.assert_allclose(result["data"], stokes, atol=1e-6, rtol=3e-7)
        assert reader.done()


def test_ms_correlation_clause_selects_first_matching_shared_description(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    add_description(casa, native_dataset)
    path = str(native_dataset)
    table = casa.table()
    assert table.open(path, nomodify=False)
    descriptions = np.arange(len(raw["time"]), dtype=np.int32) % 2
    assert table.putcol("DATA_DESC_ID", descriptions)
    assert table.done()
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        assert reader.selectpolarization("RR")
        selected = np.count_nonzero(descriptions == 0)
        assert reader.nrow(True) == selected
        np.testing.assert_array_equal(reader.msselectedindices()["poldd"], [0])
        result = reader.getdata(["data_desc_id", "time"])
        np.testing.assert_array_equal(result["data_desc_id"], np.zeros(selected, np.int64), strict=True)
        np.testing.assert_array_equal(result["time"], raw["time"][::2], strict=True)
        assert reader.done()


def test_ms_polarization_converts_each_complex_visibility_product(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    description = table.getcoldesc("DATA")
    assert table.addcols({"CORRECTED_DATA": description, "MODEL_DATA": description})
    observed = raw["data"].astype(np.complex64)
    corrected = observed + np.complex64(5 + 10j)
    factors = np.array([1 + 2j, 3 + 4j], dtype=np.complex64)
    model = np.broadcast_to(factors[:, None, None], observed.shape).copy()
    assert table.putcol("CORRECTED_DATA", corrected)
    assert table.putcol("MODEL_DATA", model)
    assert table.done()
    products = {
        "data": observed, "corrected_data": corrected, "model_data": model,
        "ratio_data": corrected / model, "residual_data": corrected - model,
        "obs_residual_data": observed - model,
    }
    predictions = {
        name: np.stack([(values[0] + values[1]) / 2, (values[0] - values[1]) / 2])
        for name, values in products.items()
    }
    fields = list(products) + ["ratio_amplitude", "model_imaginary"]
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectpolarization(["I", "V"])
        result = reader.getdata(fields)
        for name, expected in predictions.items():
            np.testing.assert_allclose(result[name], expected, rtol=3e-7, atol=2e-6)
        np.testing.assert_allclose(result["ratio_amplitude"], np.abs(predictions["ratio_data"]), rtol=3e-7)
        np.testing.assert_array_equal(result["model_imaginary"], predictions["model_data"].imag)
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


def test_ms_channel_binned_stokes_uses_its_own_flag_shape(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    path = str(native_dataset)
    flags = np.zeros_like(raw["flag"])
    flags[1, :2, 0] = True
    table = casa.table()
    assert table.open(path, nomodify=False)
    assert table.putcol("FLAG", flags)
    assert table.done()

    rows = flags.shape[-1]
    recorded_flags = flags.reshape(2, 2, 2, rows).all(axis=2)
    recorded_data = raw["data"].astype(np.complex64).reshape(2, 2, 2, rows).mean(axis=2)
    stokes_i = np.where(recorded_flags.any(axis=0), 0, recorded_data.mean(axis=0))
    reader = tools.ms()
    assert reader.open(path)
    assert reader.selectchannel(2, 0, 2, 2)
    assert reader.selectpolarization(["I"])
    record = reader.getdata(["data", "flag", "axis_info"])
    np.testing.assert_array_equal(record["axis_info"]["corr_axis"], ["I"])
    np.testing.assert_array_equal(record["data"], stokes_i[None].astype(np.complex128), strict=True)
    np.testing.assert_array_equal(record["flag"], recorded_flags, strict=True)
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
