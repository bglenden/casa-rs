"""CASA row-axis averaging and independent scientific visibility checks."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_visibility import same_visibility
from test_tools_msmetadata import measures, native_dataset


def test_ms_getdata_average_uses_weights_for_each_row_and_correlation(native_dataset, tmp_path):
    casa = pinned_casa()
    writer = casa.table()
    assert writer.open(str(native_dataset), nomodify=False)
    rows = np.arange(6, dtype=np.int64)
    shape = writer.getcell("DATA", 0).shape + (len(rows),)
    real = np.arange(1, 1 + np.prod(shape), dtype=np.float32).reshape(shape, order="F")
    imaginary = real * np.where(rows % 2 == 0, 1.25, -0.75)
    data = (real + 1j * imaginary).astype(np.complex64)
    flags = np.zeros(shape, dtype=bool)
    flags[0, 0, 1] = True
    flags[1, 0, -1] = True
    flags[1, -1, :] = True
    weights = np.array([[1, 3, 5, 7, 9, 11], [2, 4, 6, 8, 10, 12]], dtype=np.float32)
    assert writer.putcol("DATA", data, nrow=len(rows))
    assert writer.putcol("FLAG", flags, nrow=len(rows))
    assert writer.putcol("WEIGHT", weights, nrow=len(rows))
    reference = str(tmp_path / "weighted.ms")
    view = writer.selectrows(rows, reference)
    assert view.done() and writer.done()

    sample_weights = np.where(flags, 0, weights[:, None, :]).astype(np.float64)
    total = sample_weights.sum(axis=-1)
    numerator = (data.astype(np.complex128) * sample_weights).sum(axis=-1)
    expected = np.divide(numerator, total, out=np.zeros_like(numerator), where=total != 0)
    expected = expected.astype(np.complex64).astype(np.complex128)
    assert expected[0, 0].real == pytest.approx(30.575758, rel=1e-6)
    mean_amplitude = (np.abs(data[0, 0]) * sample_weights[0, 0]).sum() / total[0, 0]
    assert mean_amplitude - abs(expected[0, 0]) > 1

    reference_reader = casa.ms()
    assert reference_reader.open(reference)
    assert reference_reader.getdata("data", average=True)["data"][0, 0].real == pytest.approx(
        29.631578, rel=1e-6,
    )
    assert reference_reader.done()

    reader = tools.ms()
    assert reader.open(reference)
    items = ["data", "amplitude", "phase", "real", "imaginary", "flag", "weight", "flag_sum"]
    observed = reader.getdata(items, average=True)
    assert observed["data"].dtype == np.complex128 and observed["data"].shape == shape[:2]
    np.testing.assert_allclose(observed["data"], expected, rtol=1e-7)
    np.testing.assert_allclose(observed["amplitude"], np.abs(expected), rtol=2e-7)
    np.testing.assert_allclose(observed["phase"], np.angle(expected), rtol=2e-7)
    np.testing.assert_array_equal(observed["real"], expected.real, strict=True)
    np.testing.assert_array_equal(observed["imaginary"], expected.imag, strict=True)
    np.testing.assert_array_equal(observed["flag"], flags.all(axis=-1, keepdims=True), strict=True)
    np.testing.assert_array_equal(observed["weight"], weights.astype(np.float64), strict=True)
    np.testing.assert_array_equal(observed["flag_sum"], flags.sum(axis=-1), strict=True)
    assert reader.nrow(True) == len(rows)
    same_visibility(reader.getdata(items, average=True), observed)
    assert reader.done()


def test_ms_getdata_average_products_and_auxiliary_fields_match_casa(native_dataset):
    casa = pinned_casa()
    writer = casa.table()
    assert writer.open(str(native_dataset), nomodify=False)
    shape = writer.getcol("DATA").shape
    samples = np.arange(1, 1 + np.prod(shape), dtype=np.float32).reshape(shape, order="F")
    observed = (samples + 0.25j * samples).astype(np.complex64)
    flags = samples.astype(np.int64) % 8 == 0
    flags[-1, -1, :] = True
    desc = writer.getcoldesc("DATA")
    float_desc = dict(desc, valueType="float", dataManagerType="StandardStMan",
                      dataManagerGroup="FLOAT_DATA")
    assert writer.addcols({"CORRECTED_DATA": desc, "MODEL_DATA": desc, "FLOAT_DATA": float_desc})
    assert writer.putcol("DATA", observed)
    assert writer.putcol("CORRECTED_DATA", observed + (5 - 8j))
    assert writer.putcol("MODEL_DATA", np.full(shape, 2 + 3j, dtype=np.complex64))
    assert writer.putcol("FLOAT_DATA", samples * 0.5)
    assert writer.putcol("FLAG", flags)
    vector_shape = (shape[0], shape[-1])
    assert writer.putcol("WEIGHT", np.full(vector_shape, 1.5, dtype=np.float32))
    sigma = np.arange(1, 1 + np.prod(vector_shape), dtype=np.float32).reshape(vector_shape)
    assert writer.putcol("SIGMA", sigma)
    assert writer.done()

    product_items = ["data", "amplitude", "phase", "real", "imaginary", "corrected_data",
                     "model_data", "ratio_data", "residual_data", "obs_residual_data", "float_data"]
    metadata_items = ["time", "antenna1", "antenna2", "array_id", "data_desc_id", "ifr_number",
                      "weight", "sigma", "flag", "flag_row", "flag_sum", "uvw", "axis_info"]
    results = []
    selected_count = shape[-1] // 3
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        packs = []
        for items in (product_items + metadata_items, ["data", "flag"], metadata_items):
            packs.append(reader.getdata(items, increment=3, average=True))
        assert "data_desc_id" not in packs[0]
        assert packs[0]["flag"].shape == shape[:2] + (1,)
        assert packs[0]["sigma"].shape == (shape[0],)
        assert packs[0]["time"].shape == (1,)
        assert packs[0]["uvw"].shape == (3, selected_count)
        assert packs[1]["avgweight"].shape == (shape[0], selected_count)
        assert packs[2]["flag"].shape == shape[:2] + (selected_count,)
        assert reader.nrow(True) == shape[-1]
        assert reader.done()
        results.append(packs)
    for native, expected in zip(results[1], results[0], strict=True):
        same_visibility(native, expected)


def test_ms_getdata_average_item_aliases_errors_and_repeated_sigma(native_dataset):
    casa = pinned_casa()
    records = []
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        row_count = reader.nrow(True)
        with pytest.raises(RuntimeError, match="field avg_data is unknown"):
            reader.getdata(["data", "DATA"], average=True)
        with pytest.raises(RuntimeError, match="field time is unknown"):
            reader.getdata(["avg_time"], average=True)
        assert reader.nrow(True) == row_count
        record = reader.getdata(["avg_time", "time", "avg_foo", "avg_amplitude"], average=True)
        assert set(record) == {"avg_time", "time", "avg_foo"}
        raw = reader.getdata("data")["data"]
        aliases = reader.getdata(["metadata", "realpart", "amplitudes"], average=True)
        assert set(aliases) == {"avg_metadata", "avg_realpart", "avg_amplitudes"}
        for alias in aliases.values():
            np.testing.assert_array_equal(alias, raw, strict=True)
        mixed = reader.getdata(["data", "metadata"], average=True)
        assert set(mixed) == {"data", "avgweight", "avg_metadata"}
        assert mixed["data"].shape == raw.shape[:2]
        np.testing.assert_array_equal(mixed["avg_metadata"], raw, strict=True)
        records.append(record)
        assert reader.done()
    same_visibility(*records[::-1])

    table = casa.table()
    assert table.open(str(native_dataset))
    sigma = table.getcol("SIGMA")
    assert table.done()
    reader = tools.ms()
    assert reader.open(str(native_dataset))
    # CASA can keep allocating when these aliases apply SIGMA reduction twice.
    result = reader.getdata(["avg_sigma", "sigma"], average=True)
    np.testing.assert_allclose(result["sigma"], np.sqrt((sigma ** 2).sum(axis=-1)), rtol=2e-7)
    assert result["avg_sigma"].shape[-1] == row_count
    assert reader.nrow(True) == row_count
    with pytest.raises(RuntimeError, match="field avg_data is unknown"):
        reader.getdata(["data", "DATA"], increment=3, average=True)
    assert reader.nrow(True) == row_count
    assert reader.done()


def test_ms_getdata_average_handles_empty_stride_without_reading_missing_axes(native_dataset):
    reader = tools.ms()
    assert reader.open(str(native_dataset))
    row_count = reader.nrow(True)
    result = reader.getdata(["data", "flag", "flag_sum", "time"],
                            increment=(1 << 32) + 1, average=True)
    assert result["data"].dtype == np.complex128 and result["data"].shape == (0, 0)
    assert result["flag"].dtype == np.bool_ and result["flag"].shape == (0, 0, 1)
    assert result["flag_sum"].dtype == np.int64 and result["flag_sum"].shape == (0, 0)
    assert result["avgweight"].dtype == np.float64 and result["avgweight"].shape == (0,)
    assert result["time"].shape == (1,) and np.isnan(result["time"][0])
    assert reader.nrow(True) == row_count
    assert reader.done()
