"""Selected MeasurementSet copies with the original partition IDs."""

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
import os
from pathlib import Path
from shutil import copytree
import subprocess
import sys

import numpy as np
import pytest

from casars import tools
from test_tools_ms_addephemeris import assert_external_write_refused
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
    with pytest.raises(RuntimeError, match="[Ff]ield"):
        ms.partition(str(output), field=[99999])
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


@pytest.mark.parametrize(
    ("requested", "actual"),
    [([2, 2, 2], [2, 2, 2]), ([0, 2, 2], [2, 2, 2]), ([2, 2, 0], [2, 2, 8192])],
)
def test_partition_uses_requested_tile_shape(native_dataset, tmp_path, concat_api, requested, actual):
    output = tmp_path / "partition.ms"
    inherited = tmp_path / "inherited.ms"
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    assert ms.partition(str(output), tileshape=requested)
    assert ms.close()
    assert ms.open(str(output))
    assert ms.partition(str(inherited))
    assert ms.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        for path, default in ((output, requested), (inherited, actual)):
            assert table.open(str(path))
            data_manager, = [
                manager for manager in table.getdminfo().values()
                if "DATA" in manager["COLUMNS"]
            ]
            assert data_manager["TYPE"] == "TiledShapeStMan"
            details = data_manager["SPEC"]
            np.testing.assert_array_equal(details["DEFAULTTILESHAPE"], default)
            cube, = details["HYPERCUBES"].values()
            np.testing.assert_array_equal(cube["TileShape"], actual)
            assert table.close()


@pytest.mark.parametrize("defined", [False, True])
def test_partition_copies_external_links_and_preserves_the_weight_spectrum(
    native_dataset, tmp_path, concat_api, defined,
):
    external = tmp_path / "external.tab"
    copytree(native_dataset / "FIELD", external)
    spectrum = np.arange(1, 9, dtype=np.float32).reshape(2, 4)

    def seed(table):
        assert table.putkeyword("AUXILIARY", f"Table: {external}")
        if defined:
            desc = table.getcoldesc("DATA")
            desc.update(valueType="float", dataManagerType="StandardStMan",
                        dataManagerGroup="WEIGHT_SPECTRUM")
            if "WEIGHT_SPECTRUM" not in table.colnames():
                assert table.addcols({"WEIGHT_SPECTRUM": desc})
            for row in range(table.nrows()):
                assert table.putcell("WEIGHT_SPECTRUM", row, spectrum)

    change_table(native_dataset, seed)
    output = tmp_path / "partition.ms"
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    assert ms.msselect({"field": "0"})
    assert ms.partition(str(output))
    assert ms.close()
    native_dataset.rename(tmp_path / "relocated-input.ms")
    external.rename(tmp_path / "relocated-external.tab")
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(output))
        link = Path(table.getkeyword("AUXILIARY").removeprefix("Table: "))
        assert link.parent == output
        assert "WEIGHT_SPECTRUM" in table.colnames()
        assert "SIGMA_SPECTRUM" not in table.colnames()
        assert table.iscelldefined("WEIGHT_SPECTRUM", 0) is defined
        if defined:
            np.testing.assert_array_equal(table.getcell("WEIGHT_SPECTRUM", 0), spectrum)
        assert table.close()
        assert table.open(str(link))
        assert table.nrows() == 1
        assert table.close()


def test_partition_keeps_row_order_and_promotes_the_only_visibility_column(
    native_dataset, tmp_path, concat_api,
):
    seeded = {}

    def seed(table):
        desc = table.getcoldesc("DATA")
        desc.update(dataManagerType="StandardStMan", dataManagerGroup="CORRECTED_DATA")
        assert table.addcols({"CORRECTED_DATA": desc})
        seeded["DATA"] = table.getcol("DATA") + np.arange(table.nrows(), dtype=np.float32)
        assert table.putcol("CORRECTED_DATA", seeded["DATA"])
        assert table.removecols("DATA")
        assert table.addcols({"CUSTOM_ID": {
            "valueType": "int", "dataManagerType": "StandardStMan", "dataManagerGroup": "CUSTOM_ID",
        }})
        assert table.putcol("CUSTOM_ID", np.arange(table.nrows(), dtype=np.int32))
        integrations, positions = np.unique(table.getcol("TIME"), return_inverse=True)
        assert integrations.size == 3
        seeded["SCAN_NUMBER"] = np.array([1, 2, 1], dtype=np.int32)[positions]
        assert table.putcol("SCAN_NUMBER", seeded["SCAN_NUMBER"])

    change_table(native_dataset, seed)
    output = tmp_path / "partition.ms"
    ms = concat_api.ms()
    assert ms.open(str(native_dataset))
    assert ms.partition(str(output), whichcol="all")
    assert ms.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(output))
        assert "CUSTOM_ID" not in table.colnames()
        assert "CORRECTED_DATA" not in table.colnames()
        for name, expected in seeded.items():
            np.testing.assert_array_equal(table.getcol(name), expected)
        assert table.close()


@pytest.mark.skipif(os.name != "posix", reason="casacore POSIX table locks")
def test_partition_keeps_the_attachment_locks(native_dataset, tmp_path):
    ms = tools.ms()
    assert ms.open(str(native_dataset), nomodify=False, lock=True)
    assert_external_write_refused(native_dataset / "FIELD")
    assert ms.partition(str(tmp_path / "partition.ms"))
    assert_external_write_refused(native_dataset / "FIELD")
    assert ms.close()


@pytest.mark.skipif(os.name != "posix", reason="casacore POSIX table locks")
def test_partition_waits_for_the_field_writer(native_dataset, tmp_path):
    ms = tools.ms()
    assert ms.open(str(native_dataset))
    output = tmp_path / "partition.ms"
    holder = (
        "import sys; from casars.tools import table; t=table(); "
        "assert t.open(sys.argv[1], lockoptions={'option':'user'}, nomodify=False); "
        "assert t.lock(write=True, nattempts=1); print('ready', flush=True); "
        "sys.stdin.readline(); assert t.done()"
    )
    with subprocess.Popen(
        [sys.executable, "-c", holder, str(native_dataset / "FIELD")],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    ) as process, ThreadPoolExecutor(max_workers=1) as pool:
        assert process.stdout.readline().strip() == "ready"
        result = pool.submit(ms.partition, str(output))
        try:
            with pytest.raises(FutureTimeout):
                result.result(timeout=0.5)
            assert not output.exists()
        finally:
            process.communicate("\n", timeout=10)
        assert process.returncode == 0
        assert result.result(timeout=10)
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


@pytest.mark.parametrize("case", ["feeds", "weights", "zero-weight"])
def test_partition_time_bins_use_feed_identity_and_legacy_channel_weights(
    native_dataset, tmp_path, case,
):
    feeds = case == "feeds"
    spectral = case == "weights"
    if feeds:
        def add_feed(table):
            columns = table.colnames()
            entries = [
                {name: table.getcell(name, row) for name in columns if table.iscelldefined(name, row)}
                for row in range(table.nrows())
            ]
            assert table.addrows(len(entries))
            for row, entry in enumerate(entries, len(entries)):
                for name, value in entry.items():
                    assert table.putcell(name, row, 1 if name == "FEED_ID" else value)

        change_table(native_dataset / "FEED", add_feed)

    def seed(table):
        _, slots = np.unique(table.getcol("TIME"), return_inverse=True)
        assert np.unique(slots).size == 3
        if spectral and "WEIGHT_SPECTRUM" not in table.colnames():
            desc = table.getcoldesc("DATA")
            desc.update(valueType="float", dataManagerType="StandardStMan",
                        dataManagerGroup="WEIGHT_SPECTRUM")
            assert table.addcols({"WEIGHT_SPECTRUM": desc})
        for row, slot in enumerate(slots):
            sample = [1, 1001, 201][slot] if not spectral else [0, 100, 0][slot]
            assert table.putcell("DATA", row, np.full((2, 4), sample, dtype=np.complex64))
            assert table.putcell("FLAG", row, np.zeros((2, 4), dtype=bool))
            assert table.putcell("FLAG_ROW", row, False)
            assert table.putcell("WEIGHT", row, np.ones(2, dtype=np.float32))
            sigma = 0 if case == "zero-weight" and slot == 1 else 1
            assert table.putcell("SIGMA", row, np.full(2, sigma, dtype=np.float32))
            assert table.putcell("EXPOSURE", row, float(5 * (slot + 1)))
            if feeds:
                for column in ("FEED1", "FEED2"):
                    assert table.putcell(column, row, slot % 2)
            elif spectral:
                weights = np.array([8, 1, 1, 2] if slot == 1 else [1] * 4, dtype=np.float32)
                assert table.putcell("WEIGHT_SPECTRUM", row, np.tile(weights, (2, 1)))

    change_table(native_dataset, seed)
    output = tmp_path / "partition.ms"
    ms = tools.ms()
    assert ms.open(str(native_dataset))
    assert ms.partition(str(output), timebin="120s")
    assert ms.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(output))
        data = table.getcol("DATA")
        if feeds:
            assert table.nrows() == 20
            feed = table.getcol("FEED1")
            np.testing.assert_array_equal(feed, table.getcol("FEED2"))
            for feed_id, value in enumerate([101, 1001]):
                assert np.count_nonzero(feed == feed_id) == 10
                np.testing.assert_allclose(data[:, :, feed == feed_id], value)
        elif spectral:
            assert table.nrows() == 10
            channel_weights = np.array([8, 1, 1, 2], dtype=np.float64) / 12
            total_weights = channel_weights + 0.5
            expected = 100 * channel_weights / total_weights
            np.testing.assert_allclose(
                data, np.broadcast_to(expected[None, :, None], data.shape), rtol=2e-7,
            )
            scalar_weight = np.sort(total_weights)[1]
            np.testing.assert_allclose(table.getcol("WEIGHT"), scalar_weight, rtol=2e-7)
            np.testing.assert_allclose(table.getcol("SIGMA"), 1 / np.sqrt(scalar_weight), rtol=2e-7)
        else:
            assert table.nrows() == 10
            np.testing.assert_allclose(data, 101)
            np.testing.assert_allclose(table.getcol("EXPOSURE"), 30)
            np.testing.assert_allclose(table.getcol("WEIGHT"), 2)
        assert table.close()
