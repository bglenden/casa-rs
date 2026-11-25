"""In-place spectral writes read back through both MS table implementations."""

from contextlib import closing
from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_tools_ms_createmultims import pinned_casa
from test_tools_msmetadata import measures, native_dataset


def read_cells(api, path, columns):
    with closing(api.table()) as table:
        assert table.open(str(path))
        return {column: [table.getcell(column, row) for row in range(table.nrows())]
                for column in columns}


@pytest.mark.parametrize("method,arguments", [
    ("cvel", {"mode": "channel", "width": 2, "phasec": [0], "hanning": False}),
    ("regridspw", {"outframe": "", "mode": "chan", "chanwidth": 2, "hanning": False}),
])
def test_spectral_rewrite_retains_physical_rows_and_reopens_in_casa(
    native_dataset, tmp_path, method, arguments
):
    casa = pinned_casa()
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

    reference = read_cells(casa, datasets[0], columns)
    for reader in (casa, tools):
        actual = read_cells(reader, datasets[1], columns)
        for column in columns:
            np.testing.assert_allclose(actual[column], reference[column], rtol=1e-6, atol=0)
    axis_columns = ("CHAN_FREQ", "CHAN_WIDTH", "NUM_CHAN", "TOTAL_BANDWIDTH")
    native_axis = read_cells(casa, datasets[1] / "SPECTRAL_WINDOW", axis_columns)
    reference_axis = read_cells(tools, datasets[0] / "SPECTRAL_WINDOW", axis_columns)
    for column in axis_columns:
        np.testing.assert_allclose(native_axis[column], reference_axis[column], rtol=0, atol=1e-6)


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


def test_frame_only_regrid_keeps_data_and_builds_one_window_per_field(native_dataset, tmp_path):
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
    for api, backend in ((casa, "reference"), (tools, "native")):
        path = tmp_path / f"{backend}.ms"
        copytree(native_dataset, path)
        with closing(api.ms()) as ms:
            assert ms.open(str(path), nomodify=False)
            assert ms.regridspw(mode="", outframe="BARY") is True
            assert ms.nrow() == count
        actual = read_cells(casa, path, main_columns)
        for column in main_columns:
            np.testing.assert_array_equal(actual[column], original[column], strict=True)
        ddids = read_cells(casa, path, ("DATA_DESC_ID",))["DATA_DESC_ID"]
        assert ddids == [row % 2 for row in range(count)]
        axis = read_cells(casa, path / "SPECTRAL_WINDOW",
                          ("CHAN_FREQ", "CHAN_WIDTH", "EFFECTIVE_BW", "RESOLUTION",
                           "REF_FREQUENCY", "TOTAL_BANDWIDTH", "MEAS_FREQ_REF"))
        assert len(axis["CHAN_FREQ"]) == 2
        assert np.max(abs(axis["CHAN_FREQ"][0] - axis["CHAN_FREQ"][1])) > 1.0
        records.append(axis)
    for column in records[0]:
        np.testing.assert_allclose(records[1][column], records[0][column], atol=0.05, rtol=0)
