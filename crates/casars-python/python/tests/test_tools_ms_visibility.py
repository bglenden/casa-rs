"""Comparison of public row-axis MeasurementSet visibility records."""

import shutil

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_selection_indices import two_spws_dataset
from test_tools_msmetadata import measures, native_dataset


def same_visibility(actual, expected, field="getdata"):
    if isinstance(expected, dict):
        assert set(actual) == set(expected)
        for name, value in expected.items():
            same_visibility(actual[name], value, f"{field}.{name}")
    else:
        assert actual.dtype == expected.dtype, field
        assert actual.shape == expected.shape, field
        if expected.dtype.kind in "fc":
            np.testing.assert_allclose(actual, expected, rtol=3e-6, atol=1e-7, err_msg=field)
        else:
            np.testing.assert_array_equal(actual, expected, strict=True, err_msg=field)


def test_ms_getdata_column_products_on_casa_reference_table(native_dataset, tmp_path):
    casa = pinned_casa()
    parent = casa.table()
    assert parent.open(str(native_dataset), nomodify=False)
    data = parent.getcol("DATA").astype(np.complex64)
    samples = np.arange(data.size, dtype=np.float32).reshape(data.shape, order="F")
    data[:] = (samples / 4 + 2) + 1j * (samples / 3 - 1)
    assert parent.putcol("DATA", data)
    desc = parent.getcoldesc("DATA")
    expected = {
        "CORRECTED_DATA": data - 13j,
        "MODEL_DATA": data / 5 + 3j,
    }
    assert parent.addcols({key: desc for key in expected})
    for key, values in expected.items():
        assert parent.putcol(key, values)
    flags = parent.getcol("FLAG")
    flags[:] = (samples.astype(np.int64) % 7 == 0)
    assert parent.putcol("FLAG", flags)
    reference = tmp_path / "views.ms"
    mapped = np.array([12, 4, 17, 9, 1, 7], dtype=np.int64)
    view = parent.selectrows(mapped, str(reference))
    assert view.done()
    assert parent.done()

    items = ["DATA", "corrected_data", "model_data", "ratio_data", "residual_data",
             "obs_residual_data", "ratio_phase", "amplitude", "imaginary", "real", "Flag",
             "flag_row", "flag_sum", "weight", "sigma", "uvw", "u", "v", "w", "uvdist",
             "time", "axis_info", "antenna1", "antenna2", "ifr_number", "float_data", "unknown"]
    results = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(reference))
        result = ms.getdata(items, increment=2, ifraxisgap=3)
        selected = mapped[::2]
        np.testing.assert_array_equal(result["data"], data[:, :, selected].astype(np.complex128), strict=True)
        np.testing.assert_array_equal(result["flag"], flags[:, :, selected], strict=True)
        np.testing.assert_array_equal(result["flag_sum"], np.count_nonzero(flags[:, :, selected], axis=-1), strict=True)
        np.testing.assert_allclose(result["ratio_data"],
            expected["CORRECTED_DATA"][:, :, selected] / expected["MODEL_DATA"][:, :, selected], rtol=3e-6)
        assert result["float_data"].dtype == np.float64 and result["float_data"].shape == (0,)
        assert "unknown" not in result
        assert ms.nrow(True) == len(mapped)
        assert ms.done()
        results.append(result)
    same_visibility(*results[::-1])


def test_ms_getdata_argument_forms_and_row_state(native_dataset):
    casa = pinned_casa()
    results = []
    for api in (casa, tools):
        tool = api.ms()
        assert tool.getdata(["TIME"]) == {}
        assert tool.open(str(native_dataset))
        assert tool.getdata() == {}
        for empty in (None, b"time", ("time",), np.array(["time"]), {"time": True}):
            assert tool.getdata(empty) == {}
        for items in ([b"time"], ["time", 1]):
            with pytest.raises(TypeError, match="list _items must contain strings"):
                tool.getdata(items)
        with pytest.raises(TypeError):
            tool.getdata(["time"], average=np.bool_(False))
        for argument in ("ifraxisgap", "increment"):
            with pytest.raises(SystemError):
                tool.getdata(["time"], **{argument: None})
        rows = tool.nrow(True)
        full = tool.getdata("time")
        assert full["time"].shape == (rows,)
        same_visibility(tool.getdata(["Time", "time"], increment=rows + 1), full)
        same_visibility(tool.getdata(["time"], increment=np.int64(3)),
                        {"time": full["time"][::3][:rows // 3]})
        skipped = tool.getdata(["data", "time"], increment=(1 << 32) + 1)
        assert skipped["data"].dtype == np.complex128 and skipped["data"].shape == (0,)
        assert skipped["time"].dtype == np.float64 and skipped["time"].shape == (0,)
        assert tool.msselect({"taql": "ANTENNA1 < 0"}) is False
        assert tool.nrow(True) == 0
        assert tool.getdata(["TIME", "DATA"]) == {}
        assert tool.reset() and tool.nrow(True) == rows
        assert tool.done()
        results.append(full)
    same_visibility(*results[::-1])


def test_ms_getdata_preserves_casa_multi_ms_member_order(native_dataset, tmp_path):
    casa = pinned_casa()
    members = []
    expected = []
    for index, name in enumerate(("part-z.ms", "part-a.ms")):
        path = tmp_path / name
        shutil.copytree(native_dataset, path)
        members.append(str(path))
        table = casa.table()
        assert table.open(str(path), nomodify=False)
        data = table.getcol("DATA").astype(np.complex64)
        data[:] = data + (index * (2 + 3j))
        assert table.putcol("DATA", data)
        expected.append(data)
        assert table.done()
    dataset = str(tmp_path / "ordered.mms")
    writer = casa.ms()
    assert writer.createmultims(dataset, members, ["ANTENNA", "DATA_DESCRIPTION", "FEED"])
    assert writer.done()

    results = []
    packed = np.concatenate(expected, axis=-1).astype(np.complex128)
    want = packed[:, :, ::7][:, :, :packed.shape[-1] // 7]
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(dataset)
        assert reader.ismultims()
        items = ["data", "flag", "flag_sum", "weight", "sigma", "time", "uvw",
                 "antenna1", "antenna2", "ifr_number"]
        record = reader.getdata(items, increment=7)
        np.testing.assert_array_equal(record["data"], want, strict=True)
        same_visibility(reader.getdata(items, increment=7), record)
        single = reader.getdata(["data"], increment=packed.shape[-1] - 1)
        np.testing.assert_array_equal(single["data"], packed[:, :, :1], strict=True)
        results.append(record)
        assert reader.done()
    same_visibility(*results[::-1])

    live = tools.ms()
    assert live.open(dataset)
    before = live.getdata("data")
    same_visibility(live.getdata("data"), before)
    second_member = live.getreferencedtables()[1]
    writer = casa.table()
    assert writer.open(second_member, nomodify=False)
    updated = np.full(packed.shape[:2], 29 + 31j, dtype=np.complex64)
    assert writer.putcell("DATA", writer.nrows() - 1, updated)
    assert writer.done()
    after = live.getdata("data")["data"]
    np.testing.assert_array_equal(after[:, :, :-1], before["data"][:, :, :-1], strict=True)
    np.testing.assert_array_equal(after[:, :, -1], updated.astype(np.complex128), strict=True)
    assert live.done()


def test_ms_getdata_empty_stride_keeps_safe_shapes_and_active_rows(native_dataset):
    tool = tools.ms()
    assert tool.open(str(native_dataset))
    rows = tool.nrow(True)
    result = tool.getdata(["flag_sum", "u", "v", "w", "uvdist"], increment=(1 << 32) + 1)
    assert result["flag_sum"].dtype == np.int64 and result["flag_sum"].shape == (0, 0)
    for key in ("u", "v", "w", "uvdist"):
        assert result[key].dtype == np.float64 and result[key].shape == (0,)
    with pytest.raises(RuntimeError):
        tool.getdata(["axis_info"], increment=(1 << 32) + 1)
    assert tool.nrow(True) == rows
    assert tool.done()


def test_ms_getdata_initializes_heterogeneous_descriptions(two_spws_dataset):
    casa = pinned_casa()
    records = []
    for api in (casa, tools):
        tool = api.ms()
        assert tool.open(two_spws_dataset)
        assert tool.nrow(True) == 60
        assert tool.getdata() == {}
        assert tool.nrow(True) == 30
        first = tool.getdata(["Data", "AXIS_INFO", "TIME"])
        assert first["data"].shape == (2, 4, 30)
        same_visibility(tool.getdata(["Data", "TIME"]), {key: first[key] for key in ("data", "time")})
        assert tool.reset()
        assert tool.nrow(True) == 60
        assert tool.msselect({"spw": "1"})
        second = tool.getdata(["data", "axis_info", "time"])
        assert second["data"].shape == (2, 3, 30)
        same_visibility(tool.getdata(["data", "time"]), {key: second[key] for key in ("data", "time")})
        assert tool.done()
        records.append((first, second))
    for native, reference in zip(records[1], records[0], strict=True):
        same_visibility(native, reference)


def test_ms_getdata_ratio_preserves_infinite_components(native_dataset):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    column = table.getcoldesc("DATA")
    assert table.addcols({"CORRECTED_DATA": column, "MODEL_DATA": column})
    data = table.getcol("DATA").astype(np.complex64)
    numerator = np.ones_like(data)
    denominator = np.ones_like(data)
    numerator[:, :2, 0] = [[1 + 2j, 2j], [1, 0]]
    denominator[:, :2, 0] = 0
    assert table.putcol("CORRECTED_DATA", numerator)
    assert table.putcol("MODEL_DATA", denominator)
    assert table.done()

    for api in (casa, tools):
        tool = api.ms()
        assert tool.open(str(native_dataset))
        result = tool.getdata(["ratio_data", "ratio_amplitude", "ratio_phase"], increment=tool.nrow() - 1)
        ratio = result["ratio_data"][:, :2, 0]
        np.testing.assert_array_equal(np.isinf(ratio.real), [[True, False], [True, False]])
        np.testing.assert_array_equal(np.isinf(ratio.imag), [[True, True], [False, False]])
        np.testing.assert_array_equal(np.isnan(ratio.real), [[False, True], [False, True]])
        np.testing.assert_array_equal(np.isnan(ratio.imag), [[False, False], [True, True]])
        assert ratio[0, 0].real > 0 and ratio[0, 0].imag > 0
        assert result["ratio_phase"][0, 0, 0] == pytest.approx(np.pi / 4, rel=1e-6)
        assert np.isinf(result["ratio_amplitude"][0, 0, 0])
        assert tool.done()


def test_ms_getdata_reads_single_precision_float_visibilities(native_dataset):
    casa = pinned_casa()
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    description = dict(table.getcoldesc("DATA"), valueType="float",
                       dataManagerType="StandardStMan", dataManagerGroup="FLOAT_DATA")
    assert table.addcols({"FLOAT_DATA": description})
    shape = table.getcol("DATA").shape
    samples = np.arange(np.prod(shape), dtype=np.float32).reshape(shape, order="F") / 5 - 2
    assert table.putcol("FLOAT_DATA", samples)
    assert table.done()

    for api in (casa, tools):
        tool = api.ms()
        assert tool.open(str(native_dataset))
        record = tool.getdata(["FLOAT_DATA"], increment=3)
        expected = samples[:, :, ::3][:, :, :shape[-1] // 3].astype(np.float64)
        np.testing.assert_array_equal(record["float_data"], expected, strict=True)
        assert tool.done()


def test_ms_getdata_literal_average_prefix_and_missing_items(native_dataset):
    casa = pinned_casa()
    items = ["Data", "AVG_AMPLITUDE", "avg_time", "avg_flag_sum", "model_unknown",
             "unknown_corrected_data", "unknown_float_data", "unrecognized"]
    records = []
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        record = reader.getdata(items, increment=7)
        assert set(record) == {"data", "avg_amplitude", "avg_time", "model_unknown",
                               "unknown_corrected_data", "unknown_float_data"}
        for alias in ("avg_amplitude", "avg_time"):
            np.testing.assert_array_equal(record[alias], record["data"], strict=True)
        for key, dtype in [("model_unknown", np.float64), ("unknown_corrected_data", np.complex128),
                           ("unknown_float_data", np.float64)]:
            assert record[key].shape == (0,) and record[key].dtype == dtype
        records.append(record)
        assert reader.done()
    same_visibility(*records[::-1])


def test_ms_getdata_reads_rows_and_columns_published_by_casa(native_dataset):
    casa = pinned_casa()
    reader = tools.ms()
    assert reader.open(str(native_dataset))
    before = reader.getdata(["time", "model_data"])
    assert before["model_data"].shape == (0,)
    same_visibility(reader.getdata(["time", "model_data"]), before)

    writer = casa.table()
    assert writer.open(str(native_dataset), nomodify=False)
    row_count = writer.nrows()
    assert writer.addrows(1)
    new_time = before["time"][-1] + 90
    assert writer.putcell("TIME", row_count, new_time)
    description = writer.getcoldesc("DATA")
    assert writer.addcols({"MODEL_DATA": description})
    shape = writer.getcell("DATA", 0).shape + (row_count + 1,)
    model = np.full(shape, 3 - 4j, dtype=np.complex64)
    assert writer.putcol("MODEL_DATA", model)
    assert writer.done()

    result = reader.getdata(["time", "model_data"])
    np.testing.assert_array_equal(result["time"], np.append(before["time"], new_time), strict=True)
    np.testing.assert_array_equal(result["model_data"], model.astype(np.complex128), strict=True)
    assert reader.done()


def test_ms_getdata_reads_published_referenced_cells(native_dataset, tmp_path):
    casa = pinned_casa()
    writer = casa.table()
    assert writer.open(str(native_dataset))
    reference = str(tmp_path / "published.ms")
    selected = np.array([12, 4, 17, 9], dtype=np.int64)
    view = writer.selectrows(selected, reference)
    assert view.done()
    assert writer.done()

    reader = tools.ms()
    assert reader.open(reference)
    before = reader.getdata(["data", "time"], increment=2)
    same_visibility(reader.getdata(["data", "time"], increment=2), before)

    assert writer.open(str(native_dataset), nomodify=False)
    updated = np.full(before["data"].shape[:2], 13 + 17j, dtype=np.complex64)
    physical_row = int(selected[2])
    assert writer.putcell("DATA", physical_row, updated)
    assert writer.putcell("TIME", physical_row, before["time"][1] + 90)
    assert writer.done()

    expected = casa.ms()
    assert expected.open(reference)
    fresh = expected.getdata(["data", "time"], increment=2)
    np.testing.assert_array_equal(fresh["data"][:, :, 1], updated.astype(np.complex128), strict=True)
    assert fresh["time"][1] == before["time"][1] + 90
    same_visibility(reader.getdata(["data", "time"], increment=2), fresh)
    assert expected.done()
    assert reader.done()


def test_ms_getdata_reads_forwarded_vectors(native_dataset, tmp_path):
    casa = pinned_casa()
    writer = casa.table()
    assert writer.open(str(native_dataset))
    description = writer.getdesc()
    names = writer.colnames()
    rows = writer.nrows()
    metadata = writer.getkeywords()
    info = writer.info()
    vectors = {name: writer.getcol(name.upper()) for name in ('weight', 'sigma', 'uvw')}
    assert writer.done()
    forwarded = str(tmp_path / 'forwarded.ms')
    assert writer.create(forwarded, description, nrow=rows, dminfo={
        '*1': {'TYPE': 'ForwardColumnEngine', 'NAME': 'ForwardColumnEngine',
               'SPEC': {'FORWARDTABLE': str(native_dataset)}, 'COLUMNS': names},
    })
    for name, value in metadata.items():
        if isinstance(value, str) and value.startswith('Table: '):
            value = 'Table: ' + str(native_dataset / name)
        assert writer.putkeyword(name, value)
    assert writer.putinfo(info)
    assert writer.done()
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(forwarded)
        data = reader.getdata(list(vectors))
        for name, array in vectors.items():
            np.testing.assert_array_equal(data[name], array.astype(np.float64), strict=True)
        assert reader.done()
