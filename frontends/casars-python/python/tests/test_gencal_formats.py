# SPDX-License-Identifier: LGPL-3.0-or-later

import json
from pathlib import Path
import sys
import tarfile

import numpy as np
import pytest

from casars import tasks, tools


TRIAL = (
    Path(__file__).resolve().parents[4]
    / "libs/casacore/casa-ms/tests/fixtures/ms_trial"
)


def extract_store(name, destination):
    with tarfile.open(TRIAL / f"{name}.tgz") as archive:
        kwargs = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(destination, **kwargs)


@pytest.fixture(params=["ms_trial.ms", "ms_trial.ps.zarr"])
def store(request, tmp_path, monkeypatch):
    extract_store(request.param, tmp_path)
    monkeypatch.chdir(tmp_path)
    return request.param


@pytest.fixture
def antpos_stores(tmp_path, monkeypatch):
    ms_v2 = "ms_trial_lines.ms"
    extract_store(ms_v2, tmp_path)
    monkeypatch.chdir(tmp_path)
    table = tools.table()
    assert table.open(f"{ms_v2}/OBSERVATION", nomodify=False)
    assert table.putcell("TELESCOPE_NAME", 0, "ALMA")
    assert table.close()
    ms_v4 = "paired.ps.zarr"
    conversion = tasks.msconvert(vis=ms_v2, outputvis=ms_v4, outformat="msv4", withpointing=False)
    assert conversion.successful, conversion.describe()
    return ms_v2, ms_v4


def calibration_bytes(directory):
    return {path: path.read_bytes() for path in Path(directory).rglob("table.*")
            if path.is_file() and path.name != "table.lock"}


def read_parameters(path, column, source):
    table = tools.table()
    assert table.open(path)
    try:
        assert table.getkeyword("MSName") == source
        np.testing.assert_array_equal(table.getcol("ANTENNA1"), np.arange(5))
        np.testing.assert_array_equal(table.getcol("SPECTRAL_WINDOW_ID"), np.zeros(5))
        assert not table.getcol("FLAG").any()
        return table.getcol(column)[:, 0, :]
    finally:
        assert table.close()


def test_specified_gain_create_and_phase_append(store):
    assert tasks.gencal(
        vis=store, caltable="gain.cal", caltype="amp", spw="0",
        antenna="0~1", pol="X", parameter=[0.8, 1.2],
    ) is None
    gains = np.ones((2, 5), dtype=np.complex64)
    gains[0, :2] = [0.8, 1.2]
    np.testing.assert_allclose(read_parameters("gain.cal", "CPARAM", store), gains)
    assert tasks.gencal(
        vis=store, caltable="gain.cal", caltype="ph", spw="0",
        antenna="1", pol="X", parameter=[20.0],
    ) is None
    gains[0, 1] *= np.exp(1j * np.deg2rad(20.0))
    np.testing.assert_allclose(read_parameters("gain.cal", "CPARAM", store), gains, rtol=3e-7)


@pytest.mark.parametrize(("mode", "value", "polarization", "receptor_count"), [
    ("sbd", 2.0, "Y", 2),
    ("mbd", 3.0, "Y", 2),
    ("opac", 0.02, "", 1),
])
def test_specified_real_prior(store, mode, value, polarization, receptor_count):
    assert tasks.gencal(
        vis=store, caltable="prior.cal", caltype=mode, spw="0",
        antenna="1", pol=polarization, parameter=[value],
    ) is None
    values = np.zeros((receptor_count, 5), dtype=np.float32)
    values[-1, 1] = value
    np.testing.assert_allclose(read_parameters("prior.cal", "FPARAM", store), values)


@pytest.mark.parametrize("store", ["ms_trial.ps.zarr"], indirect=True)
def test_specified_prior_refuses_unrepresentable_msv4_namespace(store):
    header = (
        Path(store)
        / "ms_trial_0/field_and_source_base_xds/FIELD_PHASE_CENTER_DIRECTION/zarr.json"
    )
    metadata = json.loads(header.read_text())
    metadata["attributes"]["frame"] = "icrs"
    header.write_text(json.dumps(metadata))
    with pytest.raises(tasks.TaskParameterError, match="UVW frame") as failure:
        tasks.gencal(vis=store, caltable="prior.cal", caltype="amp", parameter=[1.0])
    assert failure.value.completion.parameter == "vis"
    assert not Path("prior.cal").exists()


@pytest.mark.parametrize(("store", "table", "columns"), [
    ("ms_trial.ps.zarr", "ANTENNA", ("NAME", "STATION", "POSITION")),
    ("ms_trial_lines.ps.zarr", "SPECTRAL_WINDOW", (
        "NAME", "CHAN_FREQ", "CHAN_WIDTH", "MEAS_FREQ_REF", "REF_FREQUENCY",
    )),
], indirect=["store"])
def test_specified_msv4_append_refuses_reordered_subtables(store, table, columns):
    assert tasks.gencal(
        vis=store, caltable="gain.cal", caltype="amp",
        spw="0" if table == "ANTENNA" else "0,1",
        antenna="0,1" if table == "ANTENNA" else "0",
        pol="X", parameter=[2.0, 3.0],
    ) is None

    prior = tools.table()
    assert prior.open(f"gain.cal/{table}", nomodify=False)
    for column in columns:
        first, second = (prior.getcell(column, row) for row in (0, 1))
        assert prior.putcell(column, 0, second)
        assert prior.putcell(column, 1, first)
    assert prior.close()

    before = calibration_bytes("gain.cal")
    assert before
    with pytest.raises(tasks.TaskExecutionError, match=f"incompatible {table} identity"):
        tasks.gencal(
            vis=store, caltable="gain.cal", caltype="ph",
            antenna="1", spw="0", pol="X", parameter=[20.0],
        )
    assert calibration_bytes("gain.cal") == before


@pytest.mark.parametrize("store", ["ms_trial_ephemeris.ps.zarr"], indirect=True)
def test_specified_prior_refuses_msv4_moving_field_before_output(store):
    with pytest.raises(tasks.TaskParameterError, match="moving FIELD") as failure:
        tasks.gencal(vis=store, caltable="prior.cal", caltype="amp", parameter=[1.0])
    assert failure.value.completion.parameter == "vis"
    assert not Path("prior.cal").exists()


@pytest.mark.parametrize("store", ["ms_trial.ps.zarr"], indirect=True)
def test_specified_prior_does_not_require_decimal_msv4_scan_names(store):
    label = "calibrator-A"
    scan = Path(store) / "ms_trial_0/scan_name"
    header = scan / "zarr.json"
    metadata = json.loads(header.read_text())
    assert metadata["chunk_grid"]["configuration"]["chunk_shape"] == metadata["shape"]
    metadata["data_type"]["configuration"]["length_bytes"] = 4 * len(label)
    metadata["codecs"] = metadata["codecs"][:1]
    header.write_text(json.dumps(metadata))
    (scan / "c/0").write_bytes(np.full(metadata["shape"], label, dtype=f"<U{len(label)}").tobytes())

    assert tasks.gencal(vis=store, caltable="prior.cal", caltype="amp", parameter=[2.0]) is None
    table = tools.table()
    assert table.open("prior.cal")
    try:
        assert table.nrows() == 5
        np.testing.assert_array_equal(table.getcol("SCAN_NUMBER"), np.full(5, -1))
        np.testing.assert_array_equal(table.getcol("CPARAM"), np.full((2, 1, 5), 2.0 + 0.0j))
    finally:
        assert table.close()

    with pytest.raises(tasks.TaskExecutionError, match="no casacore scan number"):
        tasks.msconvert(vis=store, outputvis="numbered.ms", outformat="msv2")
    assert not Path("numbered.ms").exists()


@pytest.mark.parametrize(("mode", "initial", "updated"), [
    ("antpos", [0.4, 0.59, 0.63], [0.41, 0.61, 0.66]),
    ("antposvla", [0.44125542, -0.55981570, 0.63], [0.45729056, -0.57540011, 0.66]),
])
def test_manual_antenna_offsets_create_and_add(antpos_stores, mode, initial, updated):
    def parameters(path, observation):
        table = tools.table()
        assert table.open(path)
        try:
            assert "VLATrDelCorr" not in table.keywordnames()
            np.testing.assert_array_equal(table.getcol("ANTENNA1"), np.arange(3))
            np.testing.assert_array_equal(table.getcol("SPECTRAL_WINDOW_ID"), np.zeros(3))
            np.testing.assert_array_equal(table.getcol("OBSERVATION_ID"), np.full(3, observation))
            np.testing.assert_array_equal(table.getcol("SCAN_NUMBER"), np.full(3, -1))
            assert not table.getcol("FLAG").any()
            return table.getcol("FPARAM")[:, 0, :]
        finally:
            assert table.close()

    for index, vis in enumerate(antpos_stores):
        output = f"offsets-{index}.cal"
        assert tasks.gencal(
            vis=vis, caltable=output, caltype=mode, antenna="1,0", spw="1",
            parameter=[0.17, 0.28, 0.34, 0.4, 0.59, 0.63],
        ) is None
        before = parameters(output, 0)
        np.testing.assert_allclose(before[:, 0], initial, rtol=1e-6)
        np.testing.assert_array_equal(before[:, 2], np.zeros(3))
        assert tasks.gencal(
            vis=vis, caltable=output, caltype=mode, antenna="0", parameter=[0.01, 0.02, 0.03],
        ) is None
        after = parameters(output, -1)
        np.testing.assert_allclose(after[:, 0], updated, rtol=1e-6)
        np.testing.assert_array_equal(after[:, 1:], before[:, 1:])


@pytest.mark.parametrize(("subtable", "columns"), [
    ("ANTENNA", ("NAME", "STATION", "POSITION")),
    ("SPECTRAL_WINDOW", ("NAME", "CHAN_FREQ", "CHAN_WIDTH", "MEAS_FREQ_REF", "REF_FREQUENCY")),
])
def test_msv4_manual_antenna_offsets_refuse_reordered_update(antpos_stores, subtable, columns):
    _, vis = antpos_stores
    options = dict(vis=vis, caltable="offsets.cal", caltype="antpos", antenna="0")
    assert tasks.gencal(**options, parameter=[2.0, 3.0, 4.0]) is None
    table = tools.table()
    assert table.open(f"offsets.cal/{subtable}", nomodify=False)
    for column in columns:
        first, second = (table.getcell(column, row) for row in (0, 1))
        assert table.putcell(column, 0, second)
        assert table.putcell(column, 1, first)
    assert table.close()
    before = calibration_bytes("offsets.cal")
    assert before
    with pytest.raises(tasks.TaskExecutionError, match=f"incompatible {subtable} identity"):
        tasks.gencal(**options, parameter=[0.01, 0.02, 0.03])
    assert calibration_bytes("offsets.cal") == before


@pytest.mark.parametrize(("mode", "options", "reason"), [
    ("antpos", {"antenna": "0"}, "stated OBSERVATION.TIME_RANGE"),
    ("antposvla", {"antenna": "0"}, "stated OBSERVATION.TIME_RANGE"),
    ("antpos", {}, "automatic or file-based antenna position offsets"),
    ("antpos", {"infile": "offsets.txt"}, "automatic or file-based antenna position offsets"),
])
@pytest.mark.parametrize("store", ["ms_trial.ps.zarr"], indirect=True)
def test_msv4_antenna_offsets_refuse_unrepresented_metadata(store, mode, options, reason):
    Path("offsets.txt").write_text("A0 0.01 0.02 0.03\n")
    with pytest.raises(tasks.TaskParameterError, match=reason) as failure:
        tasks.gencal(vis=store, caltable="offsets.cal", caltype=mode,
                     parameter=[0.01, 0.02, 0.03], **options)
    assert failure.value.completion.parameter == "vis"
    assert not Path("offsets.cal").exists()


def test_unreadable_msv2_manual_antenna_offsets_preserve_diagnostic(tmp_path):
    ms = tmp_path / "unreadable.ms"
    ms.mkdir()
    caltable = tmp_path / "offsets.cal"
    with pytest.raises(tasks.TaskExecutionError) as failure:
        tasks.gencal(vis=str(ms), caltable=str(caltable), caltype="antpos",
                     antenna="0", parameter=[0.01, 0.02, 0.03])
    assert failure.value.completion.message.startswith(
        f"failed to open measurement set {ms}: storage error: table control file is missing"
    )
    assert not caltable.exists()
