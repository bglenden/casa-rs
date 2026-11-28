# SPDX-License-Identifier: LGPL-3.0-or-later

import json
from pathlib import Path
from shutil import copytree
import sys
import tarfile

import numpy as np
import pytest

from casars import tasks, tools


TRIAL = (
    Path(__file__).resolve().parents[4]
    / "libs/casacore/casa-ms/tests/fixtures/ms_trial"
)


def prepare_store(tmp_path, monkeypatch, store):
    with tarfile.open(TRIAL / f"{store}.tgz") as archive:
        extract_opts = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract_opts)
    monkeypatch.chdir(tmp_path)


@pytest.mark.parametrize("store", ["ms_trial.ms", "ms_trial.ps.zarr"])
@pytest.mark.parametrize("calmode", ["ap", "p", "a"])
def test_time_binned_point_source_gaincal(tmp_path, monkeypatch, store, calmode):
    prepare_store(tmp_path, monkeypatch, store)
    result = tasks.gaincal(
        vis=store, caltable="solved.cal", gaintype="G", calmode=calmode, solint="60s",
        refant="0", refantmode="strict", smodel=[1.0, 0.0, 0.0, 0.0], minsnr=3.0,
    )

    selected = result["selectvis"]
    np.testing.assert_array_equal(selected["antennas"], np.arange(5))
    for axis in ("field", "observation", "spw"):
        np.testing.assert_array_equal(selected[axis], [0])
    np.testing.assert_array_equal(selected["scan"], [1])
    np.testing.assert_array_equal(selected["intents"], ["TARGET.ON_SOURCE"])
    np.testing.assert_array_equal(result["solvestats"]["expected"], [2, 2])

    table = tools.table()
    assert table.open("solved.cal")
    try:
        assert table.nrows() == 10
        times = table.getcol("TIME")
        antennas = table.getcol("ANTENNA1")
        gains = table.getcol("CPARAM")[:, 0, :].T
        assert np.unique(times).size == 2
        assert not table.getcol("FLAG").any()
        for column in ("ANTENNA2", "FIELD_ID", "SPECTRAL_WINDOW_ID", "OBSERVATION_ID"):
            np.testing.assert_array_equal(table.getcol(column), np.zeros(10))
        np.testing.assert_array_equal(table.getcol("SCAN_NUMBER"), np.ones(10))
    finally:
        table.close()

    fixture = json.loads((TRIAL / "manifest.json").read_text())
    truth = fixture["corruption"]["true_gains"]
    true_gains = np.array(truth["re"]) + 1j * np.array(truth["im"])
    reference = true_gains[:, :1, :]
    referred = true_gains * reference.conj() / np.abs(reference)
    if calmode == "p":
        np.testing.assert_allclose(np.abs(gains), 1, atol=5e-7, rtol=0)
        referred /= np.abs(referred)
    if calmode == "a":
        np.testing.assert_array_equal(gains.imag, 0)
        assert np.all(gains.real > 0)
        referred = np.abs(referred)
    interval = np.searchsorted(np.unique(times), times)
    ratio = gains / referred[interval, antennas, :]
    # The fixture has a second faint source outside the supplied point model.
    assert np.max(np.abs(np.angle(ratio))) < 0.2
    assert np.max(np.abs(np.abs(ratio) - 1.0)) < 0.2


@pytest.mark.parametrize("store", ["ms_trial.ms", "ms_trial.ps.zarr"])
def test_phase_only_gaincal_persists_unity_for_flagged_solutions(tmp_path, monkeypatch, store):
    prepare_store(tmp_path, monkeypatch, store)
    tasks.gaincal(
        vis=store, caltable="solved.cal", gaintype="G", calmode="p", solint="60s",
        refant="0", refantmode="strict", smodel=[1.0, 0.0, 0.0, 0.0], minsnr=22,
    )
    table = tools.table()
    assert table.open("solved.cal")
    try:
        assert table.nrows() == 10
        flags = table.getcol("FLAG")
        gains = table.getcol("CPARAM")
        assert flags.any() and (~flags).any()
        np.testing.assert_array_equal(gains[flags], 1.0 + 0.0j)
        np.testing.assert_allclose(np.abs(gains[~flags]), 1, atol=5e-7, rtol=0)
    finally:
        assert table.close()


@pytest.mark.parametrize("store", ["ms_trial.ms", "ms_trial.ps.zarr"])
def test_time_binned_gaincal_uses_casas_interval_origin(tmp_path, monkeypatch, store):
    prepare_store(tmp_path, monkeypatch, store)
    result = tasks.gaincal(
        vis=store, caltable="solved.cal", gaintype="G", calmode="ap", solint="10.0005s",
        refant="0", refantmode="strict", smodel=[1.0, 0.0, 0.0, 0.0], minsnr=0, minblperant=1,
    )
    # CASA offsets the first solution interval by 10 ms. For 10 s
    # integrations and this interval width it expects all 12 intervals.
    np.testing.assert_array_equal(result["solvestats"]["expected"], [12, 12])


@pytest.mark.parametrize("store", ["ms_trial.ms", "ms_trial.ps.zarr"])
def test_gaincal_resolves_field_and_scan_through_the_store(tmp_path, monkeypatch, store):
    prepare_store(tmp_path, monkeypatch, store)
    result = tasks.gaincal(
        vis=store, caltable="solved.cal", gaintype="G", calmode="ap", solint="60s",
        field="trial*", scan=">0&<2", refant="0", smodel=[1, 0, 0, 0],
    )
    np.testing.assert_array_equal(result["selectvis"]["field"], [0])
    np.testing.assert_array_equal(result["selectvis"]["scan"], [1])
    table = tools.table()
    assert table.open("solved.cal")
    try:
        assert table.nrows() == 10
        np.testing.assert_array_equal(table.getcol("FIELD_ID"), np.zeros(10))
        np.testing.assert_array_equal(table.getcol("SCAN_NUMBER"), np.ones(10))
    finally:
        assert table.close()


def test_gaincal_uses_only_fields_of_the_base_group(tmp_path, monkeypatch):
    store = "ms_trial.ps.zarr"
    prepare_store(tmp_path, monkeypatch, store)
    options = dict(vis=store, field="0", solint="60s", refant="0", smodel=[1, 0, 0, 0])
    tasks.gaincal(caltable="reference.cal", **options)

    # A second partition has the selected field in its rephased group only.
    # Its centroid representation would be unsupported if gaincal bound it.
    copied = Path(store) / "ms_trial_1"
    copytree(Path(store) / "ms_trial_0", copied)
    base_fields = copied / "field_and_source_base_xds"
    rephased_fields = copied / "field_and_source_rephased_xds"
    copytree(base_fields, rephased_fields)
    phases = base_fields / "FIELD_PHASE_CENTER_DIRECTION"
    metadata = json.loads((phases / "zarr.json").read_text())
    assert metadata["shape"] == [1, 2]
    assert metadata["chunk_grid"]["configuration"]["chunk_shape"] == [1, 2]
    metadata["codecs"] = metadata["codecs"][:1]
    (phases / "zarr.json").write_text(json.dumps(metadata))
    (phases / "c/0/0").write_bytes(np.array([[0.2, 0.1]], dtype="<f8").tobytes())
    header = copied / "zarr.json"
    metadata = json.loads(header.read_text())
    groups = metadata["attributes"]["data_groups"]
    groups["rephased"] = dict(groups["base"], field_and_source=rephased_fields.name)
    header.write_text(json.dumps(metadata))
    copytree(copied / "TIME_CENTROID", copied / "TIME_CENTROID_EXTRA_PRECISION")

    reply = tasks.gaincal(caltable="field.cal", **options)
    tasks.gaincal(caltable="field-and-scan.cal", scan="1", **options)
    np.testing.assert_array_equal(reply["selectvis"]["field"], [0])

    def read_columns(path):
        table = tools.table()
        assert table.open(path)
        try:
            assert table.nrows() == 10
            return [table.getcol(col) for col in ("FIELD_ID", "SCAN_NUMBER", "CPARAM", "FLAG")]
        finally:
            assert table.close()

    reference = read_columns("reference.cal")
    for path in ("field.cal", "field-and-scan.cal"):
        for actual, expected in zip(read_columns(path), reference):
            np.testing.assert_array_equal(actual, expected)


def test_gaincal_field_preserves_msv2_missing_flag_row_error(tmp_path, monkeypatch):
    store = "ms_trial.ms"
    prepare_store(tmp_path, monkeypatch, store)
    table = tools.table()
    assert table.open(f"{store}/FIELD", nomodify=False)
    assert table.removecols("FLAG_ROW")
    assert table.close()

    with pytest.raises(tasks.TaskExecutionError, match="FLAG_ROW"):
        tasks.gaincal(
            vis=store, caltable="solved.cal", field="trial*", solint="60s", smodel=[1, 0, 0, 0],
        )
    assert not Path("solved.cal").exists()


@pytest.mark.parametrize(("parameter", "value"), [("field", "missing-field*"), ("scan", "scan-A"), ("scan", "999")])
def test_gaincal_reports_unavailable_msv4_selectors_before_output(tmp_path, monkeypatch, parameter, value):
    store = "ms_trial.ps.zarr"
    prepare_store(tmp_path, monkeypatch, store)
    with pytest.raises(tasks.TaskParameterError, match=f"invalid {parameter} selector") as result:
        tasks.gaincal(vis=store, caltable="solved.cal", solint="60s", smodel=[1, 0, 0, 0],
                      **{parameter: value})
    assert result.value.completion.parameter == parameter
    assert not Path("solved.cal").exists()


@pytest.mark.parametrize(
    ("gaintype", "solint", "diagnostic"),
    [
        ("G", "int", "outside the time-binned G"),
        ("GSPLINE", "60s", "GSPLINE gaincal on an MSv4"),
        ("XY+QU", "60s", "XY\\+QU is unavailable for MSv4"),
        ("XYf+QU", "60s", "XYf\\+QU is unavailable for MSv4"),
    ],
)
def test_gaincal_refuses_an_unavailable_msv4_mode_before_publication(
    tmp_path, monkeypatch, gaintype, solint, diagnostic,
):
    store = "ms_trial.ps.zarr"
    prepare_store(tmp_path, monkeypatch, store)

    with pytest.raises(tasks.TaskParameterError, match=diagnostic) as failure:
        tasks.gaincal(
            vis=store, caltable="solved.cal", gaintype=gaintype, calmode="ap", solint=solint,
            refant="0", smodel=[1.0, 0.0, 0.0, 0.0],
        )

    assert failure.value.completion.parameter == "vis"
    assert not Path("solved.cal").exists()


@pytest.mark.parametrize(
    ("scan_names", "diagnostic"),
    [
        (["1", "01"] * 6, "distinct scan_name values to the same CASA scan number"),
        (["AB"] * 12, "every scan_name to be a decimal scan number"),
    ],
)
def test_gaincal_refuses_unrepresentable_msv4_scans(tmp_path, monkeypatch, scan_names, diagnostic):
    store = "ms_trial.ps.zarr"
    prepare_store(tmp_path, monkeypatch, store)
    scan = Path(store) / "ms_trial_0/scan_name"
    header = scan / "zarr.json"
    metadata = json.loads(header.read_text())
    assert metadata["shape"] == [len(scan_names)]
    assert metadata["chunk_grid"]["configuration"]["chunk_shape"] == metadata["shape"]
    width = metadata["data_type"]["configuration"]["length_bytes"] // 4
    metadata["codecs"] = metadata["codecs"][:1]
    header.write_text(json.dumps(metadata))
    (scan / "c/0").write_bytes(np.asarray(scan_names, dtype=f"<U{width}").tobytes())

    with pytest.raises(tasks.TaskParameterError, match=diagnostic) as failure:
        tasks.gaincal(
            vis=store, caltable="solved.cal", gaintype="G", calmode="ap", solint="60s",
            refant="0", smodel=[1.0, 0.0, 0.0, 0.0],
        )

    assert failure.value.completion.parameter == "vis"
    assert not Path("solved.cal").exists()
