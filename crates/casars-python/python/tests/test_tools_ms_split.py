"""Compacting MeasurementSet selections with the legacy split tool."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms_concatenate import change_table
from test_tools_ms_createmultims import pinned_casa
from test_tools_ms_partition import copy_first_row, two_fields
from test_tools_ms_testconcatenate import concat_api, counts
from test_tools_msmetadata import measures, native_dataset


@pytest.mark.parametrize("selection", ["field-id", "working-view"])
def test_split_compacts_ids_replaces_output_and_records_history(
    native_dataset, tmp_path, concat_api, selection,
):
    two_fields(native_dataset)
    output = tmp_path / "split.ms"
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    original = ms.nrow()
    arguments = {"field": np.array([1], dtype=np.int32)}
    if selection == "working-view":
        assert ms.msselect({"field": "1"})
        arguments = {}
    assert ms.split(str(output), **arguments)
    assert ms.split(str(output), **arguments)
    assert ms.nrow() == original
    assert ms.close()
    native_dataset.rename(tmp_path / "relocated-input.ms")
    fields, field_id = (2, 1) if selection == "working-view" else (1, 0)
    for reader in (tools, pinned_casa()):
        assert counts(reader, output, (".", "FIELD", "HISTORY")) == [original // 2, fields, 1]
        table = reader.table()
        assert table.open(str(output))
        np.testing.assert_array_equal(table.getcol("FIELD_ID"), np.full(original // 2, field_id))
        assert table.getcell("DATA", 0).shape == (2, 4)
        assert table.getcoldesc("DATA")["valueType"] == "complex"
        assert table.close()
        assert table.open(str(output / "HISTORY"))
        assert table.getcell("ORIGIN", 0) == "ms::split()"
        assert table.getcell("APPLICATION", 0) == "ms"
        message = table.getcell("MESSAGE", 0)
        if selection == "working-view" and concat_api is not tools:
            assert message.startswith(f"{output} split from ")
            assert message.rsplit("/", 1)[-1].startswith("tab")
        else:
            assert message == f"{output} split from {native_dataset}"
        requested = "1" if selection == "field-id" else ""
        assert table.getcell("CLI_COMMAND", 0).tolist() == [
            f"fieldids={requested} spwids=* step=[1] which='DATA'",
        ]
        assert table.close()


def test_split_retains_partial_bin_and_uses_nominal_width_for_noise(
    native_dataset, tmp_path, concat_api,
):
    samples = np.array([[1, 2, 3, 4], [11, 12, 13, 14]], dtype=np.complex64)
    flags = np.array([[False, False, True, False], [False, False, False, False]])

    def seed(table):
        for row in range(table.nrows()):
            assert table.putcell("DATA", row, samples)
            assert table.putcell("FLAG", row, flags)
            assert table.putcell("FLAG_ROW", row, False)
            assert table.putcell("SIGMA", row, np.array([2, 4], dtype=np.float32))

    change_table(native_dataset, seed)
    output = tmp_path / "split.ms"
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    assert ms.split(str(output), step=[3])
    assert ms.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(output))
        np.testing.assert_allclose(table.getcell("DATA", 0), [[1.5, 4], [12, 14]])
        np.testing.assert_array_equal(table.getcell("FLAG", 0), np.zeros((2, 2), dtype=bool))
        np.testing.assert_allclose(table.getcell("WEIGHT", 0), [0.75, 0.1875], rtol=2e-7)
        np.testing.assert_allclose(table.getcell("SIGMA", 0), np.array([2, 4]) / np.sqrt(3), rtol=2e-7)
        assert table.close()
        assert table.open(str(output / "SPECTRAL_WINDOW"))
        assert table.getcell("NUM_CHAN", 0) == 2
        np.testing.assert_allclose(table.getcell("CHAN_FREQ", 0), [1.401e9, 1.403e9], rtol=1e-10)
        for name in ("CHAN_WIDTH", "EFFECTIVE_BW", "RESOLUTION"):
            np.testing.assert_array_equal(table.getcell(name, 0), [3e6, 1e6])
        assert table.close()


def test_split_weights_the_channel_average_and_zeros_flagged_bins(native_dataset, tmp_path, concat_api):
    samples = np.array([[1, 3, 5, 7], [2, 4, 6, 8]], dtype=np.complex64)
    flags = np.array([[False, False, True, True], [False, False, False, False]])
    weights = np.array([[1, 3, 1, 3], [1, 3, 1, 3]], dtype=np.float32)

    def seed(table):
        if "WEIGHT_SPECTRUM" not in table.colnames():
            desc = table.getcoldesc("DATA")
            desc.update(valueType="float", dataManagerType="StandardStMan",
                        dataManagerGroup="WEIGHT_SPECTRUM")
            assert table.addcols({"WEIGHT_SPECTRUM": desc})
        for row in range(table.nrows()):
            assert table.putcell("DATA", row, samples)
            assert table.putcell("FLAG", row, flags)
            assert table.putcell("FLAG_ROW", row, row == 1)
            assert table.putcell("WEIGHT_SPECTRUM", row, weights)

    change_table(native_dataset, seed)
    output = tmp_path / "weighted.ms"
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    assert ms.split(str(output), step=[2])
    assert ms.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(output))
        np.testing.assert_allclose(table.getcell("DATA", 0), [[2.5, 0], [3.5, 7.5]])
        np.testing.assert_array_equal(table.getcell("FLAG", 0), [[False, True], [False, False]])
        np.testing.assert_array_equal(table.getcell("WEIGHT_SPECTRUM", 0), [[4, 0], [4, 4]])
        np.testing.assert_array_equal(table.getcell("DATA", 1), np.zeros((2, 2)))
        np.testing.assert_array_equal(table.getcell("FLAG", 1), np.ones((2, 2), dtype=bool))
        assert table.close()


def test_split_selects_correlation_axis(native_dataset, tmp_path, concat_api):
    output = tmp_path / "rr.ms"
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    assert ms.split(str(output), correlation="rr")
    assert ms.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(output))
        assert table.getcell("DATA", 0).shape == (1, 4)
        assert table.close()
        assert table.open(str(output / "POLARIZATION"))
        np.testing.assert_array_equal(table.getcell("CORR_TYPE", 0), [5])
        assert table.close()

def test_split_time_uses_flags_on_the_selected_correlation(native_dataset, tmp_path, concat_api):
    epochs = []

    def seed(table):
        times = table.getcol("TIME")
        epochs.append(float(np.min(times)))
        for row, time in enumerate(times):
            first = time == epochs[0]
            flags = np.zeros((2, 4), dtype=bool)
            flags[0, :] = not first
            assert table.putcell("DATA", row, np.full((2, 4), 7 if first else 99, dtype=np.complex64))
            assert table.putcell("FLAG", row, flags)
            assert table.putcell("FLAG_ROW", row, False)
            assert table.putcell("WEIGHT", row, np.ones(2, dtype=np.float32))
            assert table.putcell("SIGMA", row, np.ones(2, dtype=np.float32))
            assert table.putcell("EXPOSURE", row, 10.0)

    change_table(native_dataset, seed)
    output = tmp_path / "rr-time.ms"
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    assert ms.split(str(output), correlation="RR", timebin="120s")
    assert ms.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(output))
        assert table.nrows() == 10
        assert table.getcol("DATA").shape == (1, 4, 10)
        np.testing.assert_array_equal(table.getcol("DATA"), 7)
        np.testing.assert_array_equal(table.getcol("TIME"), epochs[0])
        np.testing.assert_array_equal(table.getcol("EXPOSURE"), 10)
        assert table.close()


def test_split_omitted_output_keeps_input_history(native_dataset, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    ms = tools.ms()
    assert ms.open(str(native_dataset))
    assert ms.split()
    assert ms.close()
    assert list(tmp_path.glob("*.ms")) == [native_dataset]
    assert counts(tools, native_dataset, ("HISTORY",)) == [0]


def test_split_refuses_time_with_channel_stride_before_replacing_output(native_dataset, tmp_path):
    output = tmp_path / "split.ms"
    ms = tools.ms()
    assert ms.open(str(native_dataset))
    assert ms.partition(str(output))
    count = counts(tools, output, (".", "HISTORY"))
    with pytest.raises(RuntimeError, match="Simultaneous time and channel averaging"):
        ms.split(str(output), spw="0:0~3^2", timebin="120s")
    assert counts(tools, output, (".", "HISTORY")) == count
    assert ms.close()


def test_split_refuses_repeated_channel_ranges_and_negative_width(native_dataset, tmp_path, concat_api):
    output = tmp_path / "split.ms"
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    assert ms.split(str(output), spw="0:0~1;2~3", step=[1, 2]) is False
    assert ms.split(str(output), spw="0", step=[2, -1]) is False
    assert not output.exists()
    with pytest.raises(RuntimeError, match="Cannot channel average with width < 1"):
        ms.split(str(output), step=[-1])
    assert output.exists() == (concat_api is not tools)
    assert ms.close()


def test_split_refuses_two_polarization_setups_for_one_window(native_dataset, tmp_path, concat_api):
    change_table(native_dataset / "POLARIZATION", copy_first_row)
    change_table(
        native_dataset / "DATA_DESCRIPTION",
        lambda table: copy_first_row(table, POLARIZATION_ID=1),
    )
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    output = tmp_path / "split.ms"
    assert ms.split(str(output), spw="0") is False
    assert not output.exists()
    assert ms.close()


@pytest.mark.parametrize("step", [2, 2.9, [2.0], np.array([2], dtype=np.int32)])
def test_split_accepts_scalar_and_numeric_vector_steps(native_dataset, tmp_path, concat_api, step):
    output = tmp_path / "split.ms"
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    assert ms.split(str(output), step=step)
    assert ms.close()
    table = tools.table()
    assert table.open(str(output))
    assert table.getcell("DATA", 0).shape == (2, 2)
    assert table.close()


def test_split_rejects_unattached_tool(concat_api):
    with pytest.raises(RuntimeError, match="Table column DATA_DESC_ID is unknown"):
        concat_api.ms().split()


def test_split_rejects_invalid_step_without_creating_output(native_dataset, tmp_path):
    output = tmp_path / "split.ms"
    ms = tools.ms()
    assert ms.open(str(native_dataset))
    with pytest.raises(SystemError, match="returned a result with an exception set"):
        ms.split(str(output), step=(2,))
    with pytest.raises(TypeError, match="argument _step must not be a string"):
        ms.split(str(output), step="2")
    assert not output.exists()
    assert ms.close()


def test_split_averages_complete_and_partial_channel_bins_before_time(native_dataset, tmp_path):
    samples = np.array([[1, 2, 3, 4], [11, 12, 13, 14]], dtype=np.complex64)

    def seed(table):
        for row in range(table.nrows()):
            assert table.putcell("DATA", row, samples)
            assert table.putcell("FLAG", row, np.zeros((2, 4), dtype=bool))
            assert table.putcell("FLAG_ROW", row, False)
            assert table.putcell("WEIGHT", row, np.ones(2, dtype=np.float32))
            assert table.putcell("SIGMA", row, np.ones(2, dtype=np.float32))

    change_table(native_dataset, seed)
    output = tmp_path / "split.ms"
    ms = tools.ms()
    assert ms.open(str(native_dataset))
    assert ms.split(str(output), step=[3], timebin="120s")
    assert ms.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(output))
        assert table.nrows() == 10
        data = table.getcol("DATA")
        expected = np.array([[2, 4], [12, 14]])
        np.testing.assert_allclose(data, np.broadcast_to(expected[:, :, None], data.shape))
        np.testing.assert_allclose(table.getcol("WEIGHT"), 9, rtol=2e-7)
        np.testing.assert_allclose(table.getcol("SIGMA"), 1 / 3, rtol=2e-7)
        assert table.close()
