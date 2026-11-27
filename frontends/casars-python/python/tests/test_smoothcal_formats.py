# SPDX-License-Identifier: LGPL-3.0-or-later

import json
from pathlib import Path
import sys
import tarfile

import numpy as np
import pytest

from casars import tasks, tools


FIXTURES = (
    Path(__file__).resolve().parents[4]
    / "libs/casacore/casa-ms/tests/fixtures"
)


def create_calibration_table(field_id):
    calibrater = tools.calibrater()
    assert calibrater.open("mssel_test_small.ms", addcorr=False, addmodel=False)
    assert calibrater.createcaltable("source.cal", "Complex", "G Jones", True)
    assert calibrater.close()

    table = tools.table()
    if field_id == 0:
        assert table.open("source.cal/FIELD", nomodify=False)
        for column in table.colnames():
            assert table.putcol(column, table.getcol(column)[..., ::-1])
        assert table.close()

    assert table.open("source.cal", nomodify=False)
    assert table.addrows(6)
    for name, values in {
        "TIME": np.tile(4.9e9 + np.array([0.0, 10.0, 20.0]), 2),
        "INTERVAL": np.full(6, 10.0),
        "FIELD_ID": np.repeat([1 - field_id, field_id], 3),
        "ANTENNA1": np.zeros(6, dtype=np.int32),
        "ANTENNA2": np.ones(6, dtype=np.int32),
        "OBSERVATION_ID": np.zeros(6, dtype=np.int32),
        "SCAN_NUMBER": np.ones(6, dtype=np.int32),
        "SPECTRAL_WINDOW_ID": np.ones(6, dtype=np.int32),
        "CPARAM": np.array([1, 4, 7, 2, 5, 8], dtype=np.complex64).reshape(1, 1, 6),
        "FLAG": np.zeros((1, 1, 6), dtype=bool),
        "PARAMERR": np.ones((1, 1, 6), dtype=np.float32),
        "SNR": np.ones((1, 1, 6), dtype=np.float32),
        "WEIGHT": np.ones((1, 1, 6), dtype=np.float32),
    }.items():
        assert table.putcol(name, values)
    assert table.close()


def prepare_smoothing(tmp_path, monkeypatch, field_id):
    archives = [FIXTURES / "mssel_test_small.ms.tgz"]
    if field_id == 0:
        archives.append(FIXTURES / "ms_trial/mssel_test_small.ps.zarr.tgz")
    for path in archives:
        extract_fixture(path, tmp_path)
    monkeypatch.chdir(tmp_path)
    create_calibration_table(field_id)


def extract_fixture(path, directory):
    with tarfile.open(path) as archive:
        extract_opts = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(directory, **extract_opts)


def set_zarr_direction(array, longitude, latitude):
    header = array / "zarr.json"
    metadata = json.loads(header.read_text())
    assert metadata["shape"] == metadata["chunk_grid"]["configuration"]["chunk_shape"]
    assert metadata["shape"][1] == 2
    # Write an uncompressed Zarr v3 bytes chunk in this extracted fixture.
    metadata["codecs"] = [{"name": "bytes", "configuration": {"endian": "little"}}]
    header.write_text(json.dumps(metadata))
    values = np.tile(np.array([longitude, latitude], dtype="<f8"), (metadata["shape"][0], 1))
    (array / "c/0/0").write_bytes(values.tobytes())


def set_fixture_phase_centre(longitude, latitude):
    store = Path("mssel_test_small.ps.zarr")
    for array in store.glob("*/field_and_source_base_xds/FIELD_PHASE_CENTER_DIRECTION"):
        set_zarr_direction(array, longitude, latitude)

    table = tools.table()
    assert table.open("source.cal/FIELD", nomodify=False)
    assert table.putcell("PHASE_DIR", 0, np.array([[longitude], [latitude]]))
    assert table.close()


@pytest.mark.parametrize(
    ("store", "field_id", "phase_centre"),
    [
        ("mssel_test_small.ms", 1, None),
        ("mssel_test_small.ps.zarr", 0, None),
        ("mssel_test_small.ps.zarr", 0, (0.1, 0.1)),
    ],
)
def test_smoothcal_selects_the_intended_caltable_field(
    tmp_path, monkeypatch, store, field_id, phase_centre,
):
    prepare_smoothing(tmp_path, monkeypatch, field_id)
    if phase_centre is not None:
        set_fixture_phase_centre(*phase_centre)
    tasks.smoothcal(
        vis=store, tablein="source.cal", caltable="smoothed.cal",
        field=str(field_id), smoothtype="mean", smoothtime=60.0,
    )

    table = tools.table()
    assert table.open("smoothed.cal")
    try:
        fields = table.getcol("FIELD_ID")
        gains = table.getcol("CPARAM")
        np.testing.assert_array_equal(gains[..., fields == field_id].ravel(), [5, 5, 5])
        np.testing.assert_array_equal(gains[..., fields != field_id].ravel(), [1, 4, 7])
    finally:
        table.close()


def test_smoothcal_refuses_ambiguous_msv4_field(tmp_path, monkeypatch):
    prepare_smoothing(tmp_path, monkeypatch, 0)
    set_fixture_phase_centre(0.1, 0.1)
    table = tools.table()
    assert table.open("source.cal/FIELD", nomodify=False)
    assert table.copyrows("source.cal/FIELD", startrowin=0, nrow=1)
    assert table.putcell("PHASE_DIR", 2, np.array([[0.1 + 1e-15], [0.1]]))
    assert table.close()

    with pytest.raises(tasks.TaskExecutionError, match="ambiguous MSv4 identity"):
        tasks.smoothcal(
            vis="mssel_test_small.ps.zarr", tablein="source.cal", caltable="smoothed.cal",
            field="0", smoothtype="mean", smoothtime=60.0,
        )
    assert not Path("smoothed.cal").exists()


def test_smoothcal_refuses_ephemeris_offset_as_a_fixed_field(tmp_path, monkeypatch):
    prepare_smoothing(tmp_path, monkeypatch, 0)
    extract_fixture(FIXTURES / "ms_trial/ms_trial_ephemeris.ps.zarr.tgz", tmp_path)
    store = Path("ms_trial_ephemeris.ps.zarr")
    field = store / "ms_trial_ephemeris_0/field_and_source_base_xds"
    for name in ("SOURCE_DIRECTION", "FIELD_PHASE_CENTER_DIRECTION"):
        set_zarr_direction(field / name, 0.0, 0.0)

    table = tools.table()
    assert table.open("source.cal/FIELD", nomodify=False)
    assert table.putcell("NAME", 0, "trial")
    assert table.putcell("PHASE_DIR", 0, np.zeros((2, 1)))
    assert table.close()

    with pytest.raises(tasks.TaskParameterError, match="ephemeris-tracking MSv4 field") as failure:
        tasks.smoothcal(
            vis=str(store), tablein="source.cal", caltable="smoothed.cal",
            field="0", smoothtype="mean", smoothtime=60.0,
        )
    assert failure.value.completion.parameter == "vis"
    assert not Path("smoothed.cal").exists()


def test_smoothcal_refuses_polynomial_calibration_field_on_msv4(tmp_path, monkeypatch):
    prepare_smoothing(tmp_path, monkeypatch, 0)
    set_fixture_phase_centre(0.0, 0.0)
    table = tools.table()
    assert table.open("source.cal/FIELD", nomodify=False)
    assert table.putcell("NUM_POLY", 0, 1)
    assert table.putcell("PHASE_DIR", 0, np.array([[0.0, 1e-3], [0.0, 0.0]]))
    assert table.close()

    with pytest.raises(tasks.TaskParameterError, match="polynomial calibration FIELD") as failure:
        tasks.smoothcal(
            vis="mssel_test_small.ps.zarr", tablein="source.cal", caltable="smoothed.cal",
            field="0", smoothtype="mean", smoothtime=60.0,
        )
    assert failure.value.completion.parameter == "vis"
    assert not Path("smoothed.cal").exists()


def test_smoothcal_refuses_unsupported_msv4_schema(tmp_path, monkeypatch):
    prepare_smoothing(tmp_path, monkeypatch, 0)
    path = Path("mssel_test_small.ps.zarr/mssel_test_small_0/zarr.json")
    metadata = json.loads(path.read_text())
    metadata["attributes"]["schema_version"] = "999.0.0"
    path.write_text(json.dumps(metadata))

    with pytest.raises(tasks.TaskParameterError) as failure:
        tasks.smoothcal(
            vis="mssel_test_small.ps.zarr", tablein="source.cal", caltable="smoothed.cal",
            field="0", smoothtype="mean", smoothtime=60.0,
        )
    assert failure.value.completion.parameter == "vis"
    assert "MSv4 schema version other than 4.0.1" in str(failure.value)
    assert not Path("smoothed.cal").exists()
