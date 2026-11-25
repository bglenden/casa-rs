"""Compacting MeasurementSet selections with the legacy split tool."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms_concatenate import change_table
from test_tools_ms_createmultims import pinned_casa
from test_tools_ms_partition import two_fields
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
