"""Public baseline-axis reads from CASA-written MeasurementSet views."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_visibility import same_visibility
from test_tools_msmetadata import measures, native_dataset


@pytest.fixture
def baseline_fixture(native_dataset, tmp_path):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    shape = table.getcol("DATA").shape
    values = np.arange(1, np.prod(shape) + 1, dtype=np.float32).reshape(shape, order="F")
    data = (values + 0.125j * values).astype(np.complex64)
    flags = values.astype(np.int64) % 7 == 0
    assert table.putcol("DATA", data)
    assert table.putcol("FLAG", flags)
    assert table.putcol("FLAG_ROW", np.arange(shape[-1]) % 3 == 0)
    expected = {
        "time": table.getcol("TIME"),
        "antenna1": table.getcol("ANTENNA1"),
        "antenna2": table.getcol("ANTENNA2"),
        "data": data.astype(np.complex128),
        "flag": flags,
        "flag_row": table.getcol("FLAG_ROW"),
        "weight": table.getcol("WEIGHT").astype(np.float64),
        "sigma": table.getcol("SIGMA").astype(np.float64),
        "uvw": table.getcol("UVW"),
    }
    assert table.done()

    def view(name, rows):
        path = tmp_path / name
        table = casa.table()
        assert table.open(str(native_dataset))
        reference = table.selectrows(np.array(rows, dtype=np.int64), str(path))
        assert reference.done()
        assert table.done()
        return str(path)

    return casa, view, expected


def scatter(expected, name, rows, times, baselines):
    values = expected[name]
    result = np.zeros((*values.shape[:-1], len(baselines), len(times)), dtype=values.dtype)
    for row in rows:
        baseline = 1000 * expected["antenna1"][row] + expected["antenna2"][row]
        result[..., baselines.index(baseline), times.index(expected["time"][row])] = values[..., row]
    return result


@pytest.mark.parametrize("gap,increment", [(0, 1), (1, 1), (-4, 2), (2, 3)])
def test_ms_getdata_baseline_layout(baseline_fixture, gap, increment):
    casa, view, expected = baseline_fixture
    selected = [19, 13, 0, 6, 2, 11, 3, 8]
    path = view("scrambled.ms", selected)
    rows = selected[::increment][:len(selected) // increment]
    times = sorted({expected["time"][row] for row in rows})
    numbers = sorted({1000 * expected["antenna1"][row] + expected["antenna2"][row] for row in rows})
    baselines = []
    for number in numbers:
        if baselines and number // 1000 != baselines[-1] // 1000:
            baselines.extend([-1] * max(gap, 0))
        baselines.append(number)
    fields = ["data", "flag", "flag_row", "flag_sum", "weight", "sigma", "uvw",
              "u", "v", "w", "uvdist", "phase", "amplitude", "time", "antenna1", "antenna2",
              "ifr_number", "scan_number", "array_id", "field_id", "feed1", "feed2",
              "data_desc_id", "float_data"]
    if gap <= 0:
        fields.append("axis_info")
    results = []
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        result = reader.getdata(fields, ifraxis=True, ifraxisgap=np.int64(gap), increment=increment)
        assert reader.nrow(True) == len(selected)
        np.testing.assert_array_equal(result["time"], np.array(times), strict=True)
        np.testing.assert_array_equal(result["ifr_number"], np.array(baselines, dtype=np.int64), strict=True)
        for name in ("data", "flag", "flag_row", "uvw", "weight", "sigma", "antenna1", "antenna2"):
            wanted = scatter(expected, name, rows, times, baselines)
            if name.startswith("antenna"):
                wanted = wanted[..., 0].astype(np.int64)
            np.testing.assert_array_equal(result[name], wanted, strict=True, err_msg=name)
        np.testing.assert_array_equal(result["flag_sum"], np.count_nonzero(result["flag"], axis=-1), strict=True)
        assert reader.getdata("time")["time"].tolist() == expected["time"][selected].tolist()
        assert reader.done()
        results.append(result)
    same_visibility(results[1], results[0])


def test_ms_getdata_baseline_single_time_overwrites_earlier_duplicate(baseline_fixture, native_dataset):
    casa, view, expected = baseline_fixture
    path = view("singleton.ms", [2, 0, 0, 3])
    wanted = expected["data"][..., [0, 2, 3]]
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        result = reader.getdata(["data", "time", "axis_info"], ifraxis=True)
        np.testing.assert_array_equal(result["data"], wanted, strict=True)
        assert result["time"].shape == (1,)
        assert reader.done()

    assert expected["time"][0] == expected["time"][1]
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    for name in ("ANTENNA1", "ANTENNA2"):
        assert table.putcell(name, 1, table.getcell(name, 0))
    assert table.done()
    for name, order in (("forward.ms", [0, 1]), ("reverse.ms", [1, 0])):
        path = view(name, order)
        for api in (casa, tools):
            reader = api.ms()
            assert reader.open(path)
            result = reader.getdata(["data", "ifr_number"], ifraxis=True)
            np.testing.assert_array_equal(result["data"], expected["data"][..., order[-1:]], strict=True)
            assert reader.done()


def test_ms_getdata_baseline_gap_metadata_has_empty_placeholders(baseline_fixture):
    _, view, expected = baseline_fixture
    path = view("gaps.ms", [19, 13, 0, 6, 2, 11, 3, 8])
    reader = tools.ms()
    assert reader.open(path)
    requested = ["axis_info", "flag_sum"]
    result = reader.getdata(requested, ifraxis=True, ifraxisgap=1)
    assert set(result) == set(requested)
    ifr_axis = result["axis_info"]["ifr_axis"]
    gaps = ifr_axis["ifr_number"] == -1
    assert np.flatnonzero(gaps).tolist() == [4, 6, 8]
    assert list(ifr_axis["ifr_name"][gaps]) == [""] * 3
    assert list(ifr_axis["ifr_shortname"][gaps]) == [""] * 3
    assert list(ifr_axis["baseline"][gaps]) == [0.0] * 3
    assert not result["flag_sum"][..., gaps].any()
    assert reader.nrow(True) == 8
    assert reader.done()

    path = view("singleton-flags.ms", [0, 1, 2, 3])
    assert reader.open(path)
    counts = reader.getdata("flag_sum", ifraxis=True)["flag_sum"]
    np.testing.assert_array_equal(counts, expected["flag"][..., :4].astype(np.int64), strict=True)
    assert reader.done()


def test_ms_getdata_baseline_time_coordinates(baseline_fixture):
    casa, view, _ = baseline_fixture
    results = []
    path = view("time-coordinates.ms", [19, 13, 0, 6, 2, 11, 3, 8])
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        items = ["axis_info", "HA", "LAST", "UT"]
        result = reader.getdata(items, ifraxis=True)
        same_visibility(reader.getdata(items, ifraxis=True), result)
        assert set(result) == {"axis_info"}
        axis = result["axis_info"]["time_axis"]
        assert set(axis) == {"MJDseconds", "UT", "HA", "LAST"}
        np.testing.assert_array_equal(axis["UT"], np.array([5.0, 15.0]), strict=True)
        results.append(axis)
        assert reader.done()
    got, want = results[1], results[0]
    for name in want:
        assert got[name].dtype == want[name].dtype
        assert got[name].shape == want[name].shape
    np.testing.assert_array_equal(got["MJDseconds"], want["MJDseconds"], strict=True)
    np.testing.assert_allclose(got["HA"], want["HA"], rtol=0, atol=0.015)
    np.testing.assert_allclose(got["LAST"], want["LAST"], rtol=0, atol=0.015 / 86400)

    reader = tools.ms()
    assert reader.open(view("one-time-coordinate.ms", [0, 1, 2, 3]))
    single = reader.getdata(["axis_info", "ut"], ifraxis=True)
    np.testing.assert_array_equal(single["axis_info"]["time_axis"]["UT"], np.array([5.0]), strict=True)
    assert reader.done()


def test_ms_getdata_duplicate_baseline_items_keep_one_axis(baseline_fixture):
    _, view, _ = baseline_fixture
    reader = tools.ms()
    assert reader.open(view("repeated-fields.ms", [19, 13, 0, 6, 2, 11, 3, 8]))
    fields = ["data", "flag", "time", "u", "axis_info", "flag_sum"]
    want = reader.getdata(fields, ifraxis=True)
    got = reader.getdata(fields + [name.upper() for name in fields], ifraxis=True)
    same_visibility(got, want)
    assert got["data"].shape == (2, 4, 7, 2)
    assert reader.nrow(True) == 8
    assert reader.done()


@pytest.mark.parametrize("keep_complex_data", [True, False])
def test_ms_getdata_baseline_observed_products_use_float_data(
    baseline_fixture, native_dataset, keep_complex_data
):
    casa, view, expected = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    complex_desc = table.getcoldesc("DATA")
    float_desc = {**complex_desc, "valueType": "float"}
    assert table.addcols({"FLOAT_DATA": float_desc, "MODEL_DATA": complex_desc})
    samples = expected["data"].real.astype(np.float32) - 70.5
    model = np.full(samples.shape, 2 - 3j, dtype=np.complex64)
    assert table.putcol("FLOAT_DATA", samples)
    assert table.putcol("MODEL_DATA", model)
    if not keep_complex_data:
        assert table.removecols("DATA")
    assert table.done()

    selected = [19, 13, 0, 6, 2, 11, 3, 8]
    path = view("float-observed.ms", selected)
    times = sorted(set(expected["time"][selected]))
    baselines = [0, 1, 2, 3, 1003, 2003, 3003]
    expected["observed"] = samples.astype(np.complex128)
    expected["residual"] = (samples.astype(np.complex64) - model).astype(np.complex128)
    observed = scatter(expected, "observed", selected, times, baselines)
    residual = scatter(expected, "residual", selected, times, baselines)
    results = []
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        items = ["data", "float_data", "amplitude", "phase", "real", "imaginary",
                 "avg_amplitude", "obs_residual_data", "obs_residual_imaginary"]
        result = reader.getdata(items, ifraxis=True)
        np.testing.assert_array_equal(result["data"], observed, strict=True)
        np.testing.assert_array_equal(result["float_data"], observed.real, strict=True)
        np.testing.assert_array_equal(result["avg_amplitude"], observed, strict=True)
        np.testing.assert_array_equal(result["amplitude"], np.abs(observed), strict=True)
        np.testing.assert_allclose(result["phase"], np.angle(observed), rtol=1e-7)
        np.testing.assert_array_equal(result["obs_residual_data"], residual, strict=True)
        if keep_complex_data:
            np.testing.assert_array_equal(reader.getdata("data")["data"], expected["data"][..., selected],
                                          strict=True)
        assert reader.nrow(True) == len(selected)
        assert reader.done()
        results.append(result)
    same_visibility(results[1], results[0])


@pytest.mark.parametrize("measure", ["antenna_position", "time_reference"])
def test_ms_getdata_baseline_uses_stored_measure_frames(baseline_fixture, native_dataset, measure):
    casa, view, expected = baseline_fixture
    table = casa.table()
    if measure == "antenna_position":
        assert table.open(str(native_dataset / "ANTENNA"), nomodify=False)
        assert table.putcolkeyword("POSITION", "MEASINFO", {"type": "position", "Ref": "WGS84"})
        assert table.putcolkeyword("POSITION", "QuantumUnits", ["m", "rad", "rad"])
        position = np.repeat([[2000.0], [-1.878], [0.59]], int(table.nrows()), axis=1)
        assert table.putcol("POSITION", position)
    else:
        assert table.open(str(native_dataset), nomodify=False)
        ref_desc = {**table.getcoldesc("FIELD_ID"), "valueType": "string",
                    "dataManagerType": "StandardStMan", "dataManagerGroup": "TIME_REF"}
        assert table.addcols({"TIME_REF": ref_desc})
        refs = np.full(len(expected["time"]), "UTC")
        # The reference comes from the first selected row, even though its
        # integration is later than the others.
        refs[19] = "TAI"
        assert table.putcol("TIME_REF", refs)
        assert table.putcolkeyword("TIME", "MEASINFO", {"type": "epoch", "VarRefCol": "TIME_REF"})
    assert table.done()
    path = view("stored-measures.ms", [19, 13, 0, 6, 2, 11, 3, 8])
    axes = []
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(path)
        result = reader.getdata(["axis_info", "ha", "last"], ifraxis=True)
        axes.append(result["axis_info"]["time_axis"])
        assert reader.done()
    np.testing.assert_array_equal(axes[1]["MJDseconds"], axes[0]["MJDseconds"], strict=True)
    np.testing.assert_allclose(axes[1]["HA"], axes[0]["HA"], rtol=0, atol=0.015)
    np.testing.assert_allclose(axes[1]["LAST"], axes[0]["LAST"], rtol=0, atol=0.015 / 86400)


def test_ms_getdata_baseline_legacy_number_collision_preserves_each_pair(baseline_fixture, native_dataset):
    casa, view, expected = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset / "ANTENNA"), nomodify=False)
    assert table.addrows(1002 - int(table.nrows()))
    assert table.putcell("NAME", 1001, "A1002")
    assert table.putcell("POSITION", 1001, table.getcell("POSITION", 0) + [100.0, 0.0, 0.0])
    assert table.done()
    assert table.open(str(native_dataset), nomodify=False)
    assert table.putcell("ANTENNA2", 0, 1001)
    for name in ("ANTENNA1", "ANTENNA2"):
        assert table.putcell(name, 1, 1)
    assert table.done()

    reader = tools.ms()
    assert reader.open(view("colliding-baseline-numbers.ms", [0, 1]))
    result = reader.getdata(["data", "axis_info", "ifr_number"], ifraxis=True, ifraxisgap=1)
    wanted = np.zeros((2, 4, 3), dtype=np.complex128)
    wanted[..., [0, 2]] = expected["data"][..., :2]
    np.testing.assert_array_equal(result["data"], wanted, strict=True)
    np.testing.assert_array_equal(result["ifr_number"], np.array([1001, -1, 1001]), strict=True)
    assert result["axis_info"]["ifr_axis"]["ifr_name"].tolist() == ["A1-A1002", "", "A2-A2"]
    assert reader.nrow(True) == 2
    assert reader.done()
