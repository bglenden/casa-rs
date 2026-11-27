# SPDX-License-Identifier: LGPL-3.0-or-later

import json
from pathlib import Path
import sys
import tarfile

import numpy as np
import pytest

from casars import tasks
from casars.tools import table


TRIAL = (
    Path(__file__).resolve().parents[4]
    / "libs/casacore/casa-ms/tests/fixtures/ms_trial"
)


def read_calibration(path):
    reader = table()
    assert reader.open(path)
    try:
        arrays = (reader.getcol(name) for name in ("ANTENNA1", "ANTENNA2", "CPARAM", "FLAG"))
        return tuple(arrays)
    finally:
        reader.close()


def reverse_antenna_ids(path):
    writer = table()
    assert writer.open(f"{path}/ANTENNA", nomodify=False)
    try:
        count = writer.nrows()
        for name in writer.colnames():
            writer.putcol(name, writer.getcol(name)[..., ::-1])
    finally:
        writer.close()
    assert writer.open(path, nomodify=False)
    try:
        for name in ("ANTENNA1", "ANTENNA2"):
            writer.putcol(name, count - 1 - writer.getcol(name))
    finally:
        writer.close()


@pytest.fixture
def trial_calibration(tmp_path, monkeypatch):
    for name in ("ms_trial.ms", "ms_trial.ps.zarr"):
        with tarfile.open(TRIAL / f"{name}.tgz") as archive:
            options = {"filter": "data"} if sys.version_info >= (3, 12) else {}
            archive.extractall(tmp_path, **options)
    monkeypatch.chdir(tmp_path)
    caltable = "original.cal"
    tasks.gaincal(
        vis="ms_trial.ms",
        caltable=caltable,
        refant="0",
        solint="inf",
        minsnr=0.0,
        smodel=[1.0, 0.0, 0.0, 0.0],
    )
    return caltable


@pytest.mark.parametrize("store", ["ms_trial.ms", "ms_trial.ps.zarr"])
def test_rerefant_reads_either_measurement_set_format(trial_calibration, store):
    caltable = trial_calibration
    # An MSv4 catalogue does not supply the table's persisted antenna IDs.
    # Move W32 from row 3 to row 1 in the caltable and update its solutions.
    reference_id = 3
    if store.endswith(".zarr"):
        reverse_antenna_ids(caltable)
        reference_id = 1
    antenna, _, gains, flags = read_calibration(caltable)
    assert not flags.any()
    reference = gains[..., antenna == reference_id]
    assert reference.shape[-1] == 1
    expected = gains / (reference / np.abs(reference))

    tasks.rerefant(vis=store, tablein=caltable, caltable="out.cal", refant="W32", refantmode="strict")

    rows, references, actual, actual_flags = read_calibration("out.cal")
    order = np.argsort(rows)
    source_order = np.argsort(antenna)
    np.testing.assert_array_equal(rows[order], antenna[source_order])
    np.testing.assert_array_equal(references, np.full_like(references, reference_id))
    np.testing.assert_array_equal(actual_flags[..., order], flags[..., source_order])
    np.testing.assert_allclose(actual[..., order], expected[..., source_order], rtol=1e-6)


def test_rerefant_refuses_an_unsupported_msv4_antenna_frame(trial_calibration):
    path = Path("ms_trial.ps.zarr/ms_trial_0/antenna_xds/ANTENNA_POSITION/zarr.json")
    metadata = json.loads(path.read_text())
    metadata["attributes"]["frame"] = "ICRS"
    path.write_text(json.dumps(metadata))

    with pytest.raises(tasks.TaskParameterError) as failure:
        tasks.rerefant(
            vis="ms_trial.ps.zarr", tablein=trial_calibration, caltable="out.cal", refant="W32"
        )
    assert failure.value.completion.parameter == "vis"
    assert "MSv4 antenna positions in other than ITRS" in str(failure.value)
    assert not Path("out.cal").exists()


def test_rerefant_rejects_duplicate_msv4_reference_rows(trial_calibration):
    caltable = trial_calibration
    writer = table()
    antenna_table = f"{caltable}/ANTENNA"
    assert writer.open(antenna_table, nomodify=False)
    duplicate_id = writer.nrows()
    assert writer.copyrows(antenna_table, startrowin=3, nrow=1)
    writer.close()

    assert writer.open(caltable, nomodify=False)
    source_row = int(np.flatnonzero(writer.getcol("ANTENNA1") == 3)[0])
    duplicate_row = writer.nrows()
    assert writer.copyrows(caltable, startrowin=source_row, nrow=1)
    assert writer.putcell("ANTENNA1", duplicate_row, duplicate_id)
    assert writer.putcell("CPARAM", duplicate_row, -writer.getcell("CPARAM", source_row))
    writer.close()

    with pytest.raises(tasks.TaskExecutionError, match="both match MSv4 antenna"):
        tasks.rerefant(
            vis="ms_trial.ps.zarr", tablein=caltable, caltable="out.cal",
            refant="W32", refantmode="strict",
        )
    assert not Path("out.cal").exists()


@pytest.mark.parametrize("position_reference", ["WGS84", "variable ITRF"])
def test_rerefant_msv2_reference_selection_preserves_table_ordinals(
    trial_calibration, position_reference
):
    writer = table()
    assert writer.open("ms_trial.ms/ANTENNA", nomodify=False)
    count = writer.nrows()
    if position_reference == "WGS84":
        description = {"type": "position", "Ref": "WGS84"}
        assert writer.putcolkeyword("POSITION", "QuantumUnits", ["m", "rad", "rad"])
        positions = np.repeat([[2000.0], [-1.878], [0.59]], count, axis=1)
        assert writer.putcol("POSITION", positions)
    else:
        description = {"type": "position", "VarRefCol": "POSITION_REF"}
        assert writer.addcols({"POSITION_REF": {"valueType": "string"}})
        assert writer.putcol("POSITION_REF", np.full(count, "ITRF"))
    assert writer.putcolkeyword("POSITION", "MEASINFO", description)
    writer.close()

    tasks.rerefant(
        vis="ms_trial.ms", tablein=trial_calibration, caltable="out.cal",
        refant="3", refantmode="strict",
    )
    _, references, _, _ = read_calibration("out.cal")
    np.testing.assert_array_equal(references, np.full_like(references, 3))
