"""In-place spectral writes read back through both MS table implementations."""

from contextlib import closing
import os
from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_tools_ms_addephemeris import assert_external_write_refused
from test_tools_ms_createmultims import pinned_casa
from test_tools_msmetadata import measures, native_dataset


def read_cells(api, path, columns):
    with closing(api.table()) as table:
        assert table.open(str(path))
        return {column: [table.getcell(column, row) for row in range(table.nrows())]
                for column in columns}


@pytest.mark.parametrize("descending", [False, True], ids=["ascending", "descending"])
@pytest.mark.parametrize("method,arguments", [
    ("cvel", {"mode": "channel", "width": 2, "phasec": [0], "hanning": False}),
    ("regridspw", {"outframe": "", "mode": "chan", "chanwidth": 2, "hanning": False}),
])
def test_spectral_rewrite_retains_physical_rows_and_reopens_in_casa(
    native_dataset, tmp_path, method, arguments, descending
):
    casa = pinned_casa()
    if descending:
        with closing(casa.table()) as table:
            assert table.open(str(native_dataset / "SPECTRAL_WINDOW"), nomodify=False)
            assert table.putcell("CHAN_FREQ", 0, table.getcell("CHAN_FREQ", 0)[::-1])
            assert table.putcell("CHAN_WIDTH", 0, -table.getcell("CHAN_WIDTH", 0)[::-1])
    columns = ("TIME", "DATA_DESC_ID", "DATA", "FLAG", "SIGMA", "WEIGHT")
    with closing(casa.table()) as table:
        assert table.open(str(native_dataset), nomodify=False)
        original = [table.getcell("TIME", row) for row in range(table.nrows())]
        times = [original[0] + 10 * row for row in reversed(range(len(original)))]
        for row, time in enumerate(times):
            assert table.putcell("TIME", row, time)
            data = (np.arange(8).reshape(2, 4) + 10 * row).astype(np.complex64)
            assert table.putcell("DATA", row, data)
    datasets = [tmp_path / f"{backend}.ms" for backend in ("reference", "native")]
    for path in datasets:
        copytree(native_dataset, path)

    for api, path in zip((casa, tools), datasets):
        with closing(api.ms()) as ms:
            assert ms.open(str(path), nomodify=False)
            assert getattr(ms, method)(**arguments) is True
            assert ms.name() == str(path)
            assert ms.nrow() == len(times)
        assert read_cells(casa, path, ("TIME",))["TIME"] == times
        history = read_cells(casa, path / "HISTORY", ("MESSAGE", "ORIGIN", "APPLICATION", "CLI_COMMAND"))
        assert history["MESSAGE"][-1] == f"Transformed/regridded with {method}"
        assert history["ORIGIN"][-1] == f"ms::{method}()"
        assert history["APPLICATION"][-1] == "ms"
        command = history["CLI_COMMAND"][-1][0]
        assert "Number of channels = 2" in command or "== 2 new channels" in command

    reference = read_cells(casa, datasets[0], columns)
    for column, value in read_cells(tools, datasets[0], columns).items():
        np.testing.assert_array_equal(value, reference[column], strict=True)
    expected = dict(reference)
    if descending:
        # A linear visibility ramp on a descending grid averages consecutive
        # input samples in the stored order. CASA keeps that order in CHAN_FREQ
        # but attaches the averages to the opposite frequencies.
        expected["DATA"] = np.stack([
            np.arange(8).reshape(2, 2, 2).mean(axis=-1) + 10 * row
            for row in range(len(times))
        ]).astype(reference["DATA"][0].dtype)
        np.testing.assert_array_equal(np.asarray(reference["DATA"])[..., ::-1], expected["DATA"])
    for reader in (casa, tools):
        actual = read_cells(reader, datasets[1], columns)
        for column in columns:
            np.testing.assert_allclose(actual[column], expected[column], rtol=1e-6, atol=0, strict=True)
    axis_columns = ("CHAN_FREQ", "CHAN_WIDTH", "NUM_CHAN", "TOTAL_BANDWIDTH")
    native_axis = read_cells(casa, datasets[1] / "SPECTRAL_WINDOW", axis_columns)
    reference_axis = read_cells(tools, datasets[0] / "SPECTRAL_WINDOW", axis_columns)
    for column in axis_columns:
        np.testing.assert_allclose(native_axis[column], reference_axis[column], rtol=0, atol=1e-6)
    assert np.all(np.asarray(native_axis["CHAN_WIDTH"]) > 0)


def test_spectral_noops_and_failed_arguments_preserve_attachment(native_dataset):
    cols = ("CHAN_FREQ", "CHAN_WIDTH", "NUM_CHAN", "TOTAL_BANDWIDTH")
    window = native_dataset / "SPECTRAL_WINDOW"
    history = native_dataset / "HISTORY"
    original = read_cells(tools, window, cols)
    sequence = read_cells(tools, history, ("MESSAGE",))
    with closing(tools.ms()) as ms:
        assert ms.open(str(native_dataset), nomodify=False)
        assert ms.cvel(hanning=False) is True
        assert ms.cvel(mode="invalid", width=2) is False
        assert ms.regridspw(outframe="") is True
        assert ms.regridspw(start=0, center=0, bandwidth=4) is False
        with pytest.raises(RuntimeError, match="Regridding failed"):
            ms.regridspw(mode="vrad")
        assert ms.name() == str(native_dataset)

    for column, previous in original.items():
        np.testing.assert_array_equal(read_cells(tools, window, (column,))[column], previous)
    assert read_cells(tools, history, ("MESSAGE",)) == sequence


@pytest.mark.parametrize("resample", [False, True], ids=["frame-only", "resample"])
def test_barycentric_regrid_builds_one_window_per_field(native_dataset, tmp_path, resample):
    casa = pinned_casa()
    with closing(casa.table()) as table:
        assert table.open(str(native_dataset / "FIELD"), nomodify=False)
        copy = {column: table.getcell(column, 0) for column in table.colnames()
                if table.iscelldefined(column, 0)}
        copy["NAME"] = "F2"
        for column in ("PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"):
            copy[column] = copy[column] + np.array([[0.11], [0.25]])
        assert table.addrows(1)
        for column, value in copy.items():
            assert table.putcell(column, 1, value)
        assert table.done()
        assert table.open(str(native_dataset), nomodify=False)
        epoch = table.getcell("TIME", 0)
        count = table.nrows()
        for row in range(count):
            assert table.putcell("FIELD_ID", row, row % 2)
            assert table.putcell("TIME", row, epoch + 130 * row)
            data = (np.arange(8).reshape(2, 4) + 10 * row).astype(np.complex64)
            assert table.putcell("DATA", row, data)
    main_columns = ("TIME", "DATA", "FLAG", "WEIGHT", "SIGMA")
    original = read_cells(casa, native_dataset, main_columns)
    records = []
    main_records = []
    for api, backend in ((casa, "reference"), (tools, "native")):
        path = tmp_path / f"{backend}.ms"
        copytree(native_dataset, path)
        with closing(api.ms()) as ms:
            assert ms.open(str(path), nomodify=False)
            arguments = (dict(mode="chan", center=2, bandwidth=4, chanwidth=2, hanning=False)
                         if resample else dict(mode=""))
            assert ms.regridspw(outframe="BARY", **arguments) is True
            assert ms.nrow() == count
        actual = read_cells(casa, path, main_columns)
        np.testing.assert_array_equal(actual["TIME"], original["TIME"], strict=True)
        if resample:
            assert all(row.shape == (2, 2) for row in actual["DATA"])
            assert any(np.any(data != old[:, :2]) for data, old in zip(actual["DATA"], original["DATA"]))
        else:
            for column in main_columns:
                np.testing.assert_array_equal(actual[column], original[column], strict=True)
        main_records.append(actual)
        ddids = read_cells(casa, path, ("DATA_DESC_ID",))["DATA_DESC_ID"]
        assert ddids == [row % 2 for row in range(count)]
        if api is tools:
            dd = read_cells(casa, path / "DATA_DESCRIPTION", ("FLAG_ROW",))
            assert dd["FLAG_ROW"] == [False, False]
        axis = read_cells(casa, path / "SPECTRAL_WINDOW",
                          ("CHAN_FREQ", "CHAN_WIDTH", "EFFECTIVE_BW", "RESOLUTION",
                           "REF_FREQUENCY", "TOTAL_BANDWIDTH", "MEAS_FREQ_REF"))
        assert len(axis["CHAN_FREQ"]) == 2
        assert np.max(abs(axis["CHAN_FREQ"][0] - axis["CHAN_FREQ"][1])) > 1.0
        records.append(axis)
    for column in records[0]:
        np.testing.assert_allclose(records[1][column], records[0][column], atol=0.05, rtol=0)
    for column in main_columns:
        expected, actual = (result[column] for result in main_records)
        if column in ("DATA", "WEIGHT", "SIGMA"):
            np.testing.assert_allclose(actual, expected, rtol=1e-5, atol=1e-6)
        else:
            np.testing.assert_array_equal(actual, expected, strict=True)


def test_spectral_writers_preserve_read_only_and_selected_attachments(native_dataset):
    columns = ("DATA", "FLAG", "DATA_DESC_ID", "TIME")
    before = read_cells(tools, native_dataset, columns)
    spw_columns = ("CHAN_FREQ", "CHAN_WIDTH", "NUM_CHAN")
    window = native_dataset / "SPECTRAL_WINDOW"
    grid = read_cells(tools, window, spw_columns)
    history = native_dataset / "HISTORY"
    history_before = read_cells(tools, history, ("MESSAGE",))

    with closing(tools.ms()) as ms:
        assert ms.open(str(native_dataset), nomodify=True)
        assert ms.cvel(width=2) is False
        assert ms.regridspw(outframe="", chanwidth=2) is False
        assert ms.name() == str(native_dataset)
        assert ms.nrow() == len(before["DATA"])

    with closing(tools.ms()) as ms:
        assert ms.open(str(native_dataset), nomodify=False)
        assert ms.msselect({"taql": "ROWID() < 2"})
        assert ms.nrow(selected=True) == 2
        assert ms.regridspw(outframe="", chanwidth=2) is False
        with pytest.raises(RuntimeError, match="complete attached MeasurementSet"):
            ms.cvel(width=2, hanning=False)
        assert ms.nrow(selected=True) == 2

    for column, value in read_cells(tools, native_dataset, columns).items():
        np.testing.assert_array_equal(value, before[column], strict=True)
    for column, value in read_cells(tools, window, spw_columns).items():
        np.testing.assert_array_equal(value, grid[column], strict=True)
    assert read_cells(tools, history, ("MESSAGE",)) == history_before



def test_cvel_reads_numeric_frequency_arguments_in_hertz(native_dataset, tmp_path):
    casa = pinned_casa()
    spectral_columns = ("CHAN_FREQ", "CHAN_WIDTH")
    axis = read_cells(casa, native_dataset / "SPECTRAL_WINDOW", spectral_columns)
    first = axis["CHAN_FREQ"][0][0]
    width = axis["CHAN_WIDTH"][0][0]
    outputs = []
    for api, name in ((casa, "reference"), (tools, "native")):
        path = tmp_path / f"{name}.ms"
        copytree(native_dataset, path)
        with closing(api.ms()) as ms:
            assert ms.open(str(path), nomodify=False)
            assert ms.cvel(mode="frequency", nchan=2, start=float(first + width / 2),
                           width=float(2 * width), restfreq="1s", hanning=False) is True
            assert ms.name() == str(path)
        actual = read_cells(casa, path / "SPECTRAL_WINDOW", spectral_columns)
        np.testing.assert_allclose(actual["CHAN_FREQ"][0], first + width * np.array([0.5, 2.5]),
                                   atol=1e-6, rtol=0)
        outputs.append(read_cells(casa, path, ("DATA", "FLAG", "WEIGHT", "SIGMA")))
    for column in outputs[0]:
        np.testing.assert_allclose(outputs[0][column], outputs[1][column], atol=1e-6, rtol=1e-6)


def test_regridspw_unknown_frame_and_identical_grid_leave_history_unchanged(native_dataset, tmp_path):
    casa = pinned_casa()
    cols = ("NUM_CHAN", "CHAN_FREQ", "REF_FREQUENCY", "TOTAL_BANDWIDTH", "MEAS_FREQ_REF")
    spw = read_cells(casa, native_dataset / "SPECTRAL_WINDOW", cols)
    history = read_cells(casa, native_dataset / "HISTORY", ("MESSAGE",))
    for api, name in ((casa, "reference"), (tools, "native")):
        path = tmp_path / f"{name}.ms"
        copytree(native_dataset, path)
        with closing(api.ms()) as ms:
            assert ms.open(str(path), nomodify=False)
            assert ms.regridspw(outframe="unknown", chanwidth=2, hanning=False) is True
            assert ms.regridspw(outframe="", center=2, bandwidth=4, chanwidth=1) is True
            assert ms.name() == str(path)
        for column, value in read_cells(casa, path / "SPECTRAL_WINDOW", cols).items():
            np.testing.assert_array_equal(value, spw[column], strict=True)
        assert read_cells(casa, path / "HISTORY", ("MESSAGE",)) == history


def test_failed_rest_conversion_leaves_native_tool_attached(native_dataset, tmp_path):
    casa = pinned_casa()
    before = read_cells(casa, native_dataset, ("DATA", "FLAG", "DATA_DESC_ID"))
    for api, name in ((casa, "reference"), (tools, "native")):
        path = tmp_path / f"{name}.ms"
        copytree(native_dataset, path)
        with closing(api.ms()) as ms:
            assert ms.open(str(path), nomodify=False)
            with pytest.raises(RuntimeError, match="No MeasFrame specified for conversion of Frequency"):
                ms.cvel(outframe="REST", width=2, hanning=False)
            if api is tools:
                assert ms.name() == str(path)
                assert ms.nrow() == len(before["DATA"])
        for column, value in read_cells(casa, path, before).items():
            np.testing.assert_array_equal(value, before[column], strict=True)


def test_cvel_reports_detached_and_inconsistent_spectral_windows(native_dataset):
    casa = pinned_casa()
    with closing(casa.table()) as table:
        assert table.open(str(native_dataset / "SPECTRAL_WINDOW"), nomodify=False)
        frequencies = table.getcell("CHAN_FREQ", 0)
        frequencies[1] += 1000
        assert table.putcell("CHAN_FREQ", 0, frequencies)
    for api in (casa, tools):
        with closing(api.ms()) as ms:
            with pytest.raises(RuntimeError):
                ms.cvel(width=2, hanning=False)
            assert ms.open(str(native_dataset), nomodify=False)
            with pytest.raises(RuntimeError, match="Inconsistent SPECTRAL_WINDOW table in input MS"):
                ms.cvel(width=2, hanning=False)
            assert ms.name() == str(native_dataset)
        actual = read_cells(casa, native_dataset / "SPECTRAL_WINDOW", ("CHAN_FREQ",))
        np.testing.assert_array_equal(actual["CHAN_FREQ"][0], frequencies, strict=True)


@pytest.mark.parametrize("method,arguments", [("cvel", {"width": 2}), ("regridspw", {"chanwidth": 2})])
def test_spectral_writers_leave_all_flagged_antenna_inputs_unchanged(native_dataset, method, arguments):
    casa = pinned_casa()
    with closing(casa.table()) as table:
        assert table.open(str(native_dataset / "ANTENNA"), nomodify=False)
        assert table.putcol("FLAG_ROW", np.ones(table.nrows(), dtype=bool))
    axis = ("CHAN_FREQ", "CHAN_WIDTH", "NUM_CHAN")
    path = native_dataset / "SPECTRAL_WINDOW"
    original = read_cells(casa, path, axis)
    for api in (casa, tools):
        with closing(api.ms()) as ms:
            assert ms.open(str(native_dataset), nomodify=False)
            assert getattr(ms, method)(hanning=False, **arguments) is True
            assert ms.name() == str(native_dataset)
        for column, value in read_cells(casa, path, axis).items():
            np.testing.assert_array_equal(value, original[column], strict=True)


@pytest.mark.skipif(os.name != "posix", reason="casacore POSIX table locks")
def test_regridspw_retains_the_attachment_lock(native_dataset):
    with closing(tools.ms()) as ms:
        assert ms.open(str(native_dataset), nomodify=False, lock=True)
        assert_external_write_refused(native_dataset / "FIELD")
        assert ms.regridspw(outframe="", chanwidth=2, hanning=False)
        assert_external_write_refused(native_dataset / "FIELD")


def test_spectral_writers_accept_an_empty_main(native_dataset, tmp_path):
    casa = pinned_casa()
    path = tmp_path / "empty.ms"
    with closing(casa.table()) as table:
        assert table.open(str(native_dataset))
        with closing(table.selectrows(np.empty(0, dtype=np.int64))) as view:
            with closing(view.copy(str(path), deep=True, valuecopy=True)) as output:
                assert output.nrows() == 0
    for api in (casa, tools):
        with closing(api.ms()) as ms:
            assert ms.open(str(path), nomodify=False)
            assert ms.cvel(width=2, hanning=False) is True
            assert ms.regridspw(chanwidth=2, hanning=False) is True
            assert ms.nrow() == 0
            assert ms.name() == str(path)


def test_regridspw_retains_existing_virtual_model_keywords(native_dataset, tmp_path):
    casa = pinned_casa()
    model = {"definedmodel_field_0": "model_field_0", "model_field_0": {"marker": 1030}}
    with closing(casa.table()) as table:
        assert table.open(str(native_dataset), nomodify=False)
        for key, value in model.items():
            assert table.putkeyword(key, value)
    for api, name in ((casa, "reference"), (tools, "native")):
        path = tmp_path / f"{name}.ms"
        copytree(native_dataset, path)
        with closing(api.ms()) as ms:
            assert ms.open(str(path), nomodify=False)
            assert ms.regridspw(outframe="", chanwidth=2, hanning=False)
        with closing(casa.table()) as table:
            assert table.open(str(path))
            assert {key: table.getkeyword(key) for key in model} == model


def test_cvel_converts_at_the_earliest_integration_without_reordering_rows(native_dataset, tmp_path):
    casa = pinned_casa()
    reference = tmp_path / "reference.ms"
    native = tmp_path / "native.ms"
    with closing(casa.table()) as table:
        assert table.open(str(native_dataset), nomodify=False)
        original = table.getcol("TIME")
        times = original.min() + 10 * np.arange(len(original) - 1, -1, -1, dtype=np.float64)
        assert table.putcol("TIME", times)
    for path in (reference, native):
        copytree(native_dataset, path)
    for api, path in ((casa, reference), (tools, native)):
        with closing(api.ms()) as ms:
            assert ms.open(str(path), nomodify=False)
            assert ms.cvel(outframe="BARY", width=2, hanning=False)
            assert ms.nrow() == len(times)
        np.testing.assert_array_equal(read_cells(casa, path, ("TIME",))["TIME"], times)
    axis = ("CHAN_FREQ", "CHAN_WIDTH")
    expected = read_cells(casa, reference / "SPECTRAL_WINDOW", axis)
    actual = read_cells(casa, native / "SPECTRAL_WINDOW", axis)
    for column in axis:
        np.testing.assert_allclose(actual[column], expected[column], atol=1e-5, rtol=0)
