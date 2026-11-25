"""Selected MeasurementSet copies with the original partition IDs."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms_concatenate import change_table
from test_tools_ms_createmultims import pinned_casa
from test_tools_ms_testconcatenate import concat_api, counts
from test_tools_msmetadata import measures, native_dataset


def copy_first_row(table, **changes):
    cells = {
        name: table.getcell(name, 0)
        for name in table.colnames()
        if table.iscelldefined(name, 0)
    }
    count = table.nrows()
    assert table.addrows(1)
    for name, value in cells.items():
        assert table.putcell(name, count, changes.get(name, value))


def two_fields(path):
    change_table(path / "FIELD", lambda table: copy_first_row(table, NAME="second"))
    change_table(path, lambda table: table.putcol(
        "FIELD_ID", np.arange(table.nrows(), dtype=np.int32) % 2,
    ))


@pytest.mark.parametrize("selection", ["field-id", "working-view"])
def test_named_partition_retains_ids_and_attachment(native_dataset, tmp_path, concat_api, selection):
    two_fields(native_dataset)
    output = tmp_path / "partition.ms"
    casa = pinned_casa()
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    original = ms.nrow()
    expected = original // 2
    if selection == "working-view":
        assert ms.msselect({"field": "1"})
        kwargs = {}
    else:
        kwargs = {"field": np.array([1], dtype=np.int32)}
    assert ms.partition(str(output), **kwargs)
    assert ms.partition(str(output), **kwargs)
    assert ms.nrow() == original
    assert ms.close()
    native_dataset.rename(tmp_path / "relocated-input.ms")
    for reader in (tools, casa):
        assert counts(reader, output, (".", "FIELD", "HISTORY")) == [expected, 2, 0]
        table = reader.table()
        assert table.open(str(output))
        np.testing.assert_array_equal(table.getcol("FIELD_ID"), np.ones(expected, dtype=np.int32))
        data = table.getcell("DATA", 0)
        assert data.shape == (2, 4)
        assert data.dtype.kind == "c"
        assert table.getcoldesc("DATA")["valueType"] == "complex"
        assert table.close()


def test_partition_refuses_channel_selection_and_no_rows(native_dataset, tmp_path, concat_api):
    output = tmp_path / "partition.ms"
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    assert ms.partition(str(output), spw="0:1~2") is False
    assert ms.partition(str(output), taql="ROWID() < 0") is False
    assert not output.exists()
    output.mkdir()
    marker = output / "marker"
    marker.write_text("keep")
    with pytest.raises(RuntimeError):
        ms.partition(str(output))
    assert marker.read_text() == "keep"
    assert ms.close()


def test_partition_argument_defaults(native_dataset, tmp_path, monkeypatch, concat_api):
    monkeypatch.chdir(tmp_path)
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    assert ms.partition()
    assert sorted(tmp_path.glob("*.ms")) == [native_dataset]
    with pytest.raises(TypeError, match="outputms"):
        ms.partition(None)
    with pytest.raises(TypeError, match="whichcol"):
        ms.partition("bad.ms", whichcol=None)
    assert not (tmp_path / "bad.ms").exists()
    assert ms.close()


def test_partition_selects_multiple_named_visibility_columns(native_dataset, tmp_path, concat_api):
    expected = {
        "MODEL_DATA": np.full((2, 4), 2 + 3j, dtype=np.complex64),
        "CORRECTED_DATA": np.full((2, 4), 4 + 5j, dtype=np.complex64),
    }

    def seed(table):
        for name, cell in expected.items():
            desc = table.getcoldesc("DATA")
            desc.update(dataManagerType="StandardStMan", dataManagerGroup=name)
            assert table.addcols({name: desc})
            for row in range(table.nrows()):
                assert table.putcell(name, row, cell)

    change_table(native_dataset, seed)
    output = tmp_path / "partition.ms"
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    assert ms.partition(str(output), whichcol="model,corrected")
    assert ms.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(output))
        for name, cell in expected.items():
            np.testing.assert_array_equal(table.getcell(name, 0), cell)
        assert table.close()


def test_partition_preserves_flag_commands_and_unaveraged_noise(native_dataset, tmp_path, concat_api):
    output = tmp_path / "partition.ms"
    command = "mode='manual' spw='0:1~2'"
    change_table(native_dataset / "SPECTRAL_WINDOW", copy_first_row)
    change_table(native_dataset / "DATA_DESCRIPTION", lambda table: copy_first_row(
        table, SPECTRAL_WINDOW_ID=1,
    ))
    def seed_main(table):
        assert table.putcol("DATA_DESC_ID", np.arange(table.nrows(), dtype=np.int32) % 2)
        for row in range(table.nrows()):
            assert table.putcell("WEIGHT", row, np.array([3, 4], dtype=np.float32))
            assert table.putcell("SIGMA", row, np.array([7, 8], dtype=np.float32))

    change_table(native_dataset, seed_main)

    def write_command(table):
        assert table.addrows(1)
        assert table.putcell("COMMAND", 0, command)

    change_table(native_dataset / "FLAG_CMD", write_command)
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    assert ms.partition(str(output), spw="1")
    assert ms.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(output / "FLAG_CMD"))
        assert table.getcol("COMMAND").tolist() == [command]
        assert table.close()
        assert table.open(str(output))
        assert np.unique(table.getcol("DATA_DESC_ID")).tolist() == [1]
        np.testing.assert_array_equal(table.getcell("WEIGHT", 0), [3, 4])
        np.testing.assert_array_equal(table.getcell("SIGMA", 0), [7, 8])
        assert table.close()


def test_partition_time_average_matches_an_independent_analytic_bin(native_dataset, tmp_path):
    output = tmp_path / "partition.ms"
    inputs = {}

    def seed(table):
        for row in range(table.nrows()):
            sample = np.float32(row + 1) + np.complex64(2j)
            assert table.putcell("DATA", row, np.full((2, 4), sample, dtype=np.complex64))
            assert table.putcell("FLAG", row, np.zeros((2, 4), dtype=bool))
            assert table.putcell("FLAG_ROW", row, False)
            assert table.putcell("WEIGHT", row, np.ones(2, dtype=np.float32))
            assert table.putcell("SIGMA", row, np.ones(2, dtype=np.float32))
            assert table.putcell("EXPOSURE", row, 10.0)
            baseline = tuple(table.getcell(name, row) for name in ("ANTENNA1", "ANTENNA2"))
            inputs.setdefault(baseline, []).append((table.getcell("TIME", row), sample))

    change_table(native_dataset, seed)
    ms = tools.ms()
    assert ms.open(str(native_dataset))
    original = ms.nrow()
    assert ms.partition(str(output), timebin={"value": 120, "unit": "s"})
    assert ms.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(output))
        assert 0 < table.nrows() == len(inputs) < original
        seen = set()
        for row in range(table.nrows()):
            baseline = tuple(table.getcell(name, row) for name in ("ANTENNA1", "ANTENNA2"))
            assert baseline not in seen
            seen.add(baseline)
            epochs, samples = zip(*inputs[baseline], strict=True)
            np.testing.assert_allclose(table.getcell("DATA", row), np.mean(samples), rtol=1e-7)
            assert table.getcell("TIME", row) == pytest.approx(np.mean(epochs), abs=1e-5, rel=0)
            assert table.getcell("EXPOSURE", row) == pytest.approx(10 * len(samples))
            np.testing.assert_allclose(table.getcell("WEIGHT", row), len(samples), rtol=1e-7)
            np.testing.assert_allclose(table.getcell("SIGMA", row), len(samples) ** -0.5, rtol=1e-7)
        assert table.close()
