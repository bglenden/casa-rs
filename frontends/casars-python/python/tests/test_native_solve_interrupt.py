# SPDX-License-Identifier: LGPL-3.0-or-later
"""Interrupt public calibration solves during real work and private publication."""

import shutil

import numpy as np
import pytest

from casars import _core, tasks, tools
from test_gaincal_formats import prepare_store
from test_tools_simulator import configure, measures, needs_measures
from test_native_imstat_interrupt import image_contents
from test_native_transform_interrupt import messages


pytestmark = pytest.mark.skipif(
    not hasattr(_core, "_arm_solve_test"), reason="requires solve-test-hooks on POSIX"
)

GAIN = dict(gaintype="G", calmode="ap", solint="60s", refantmode="strict")
REQUESTS = {
    "gain": ("gaincal", GAIN),
    "legacy-gain": ("gaincal", dict(GAIN, solint="int")),
    "bandpass": ("bandpass", dict(bandtype="B", solint="inf")),
    "bpoly": ("bandpass", dict(bandtype="BPOLY", solint="inf", degamp=2, degphase=2, maskedge=0)),
}
EARLY_CHECKPOINTS = {
    "observation-batch": ("ObservationBatch", 0),
    "legacy-batch": ("LegacyBatch", 0),
    "bandpass-channel": ("BandpassChannel", 7),
    "polynomial-spectrum": ("PolynomialSpectrum", 0),
}


def read_caltable(path, polynomial):
    columns = ("ANTENNA1", "FIELD_ID", "TIME", "FLAG") + (
        ("VALID_DOMAIN", "POLY_COEFF_AMP", "POLY_COEFF_PHASE")
        if polynomial else ("SPECTRAL_WINDOW_ID", "CPARAM")
    )
    reader = tools.table()
    assert reader.open(str(path))
    try:
        return reader.info()["subType"], {name: reader.getcol(name) for name in columns}
    finally:
        assert reader.done()


def assert_solutions(path, reference, polynomial, repetitions):
    actual_type, actual = read_caltable(path, polynomial)
    assert actual_type == reference[0]
    # Compare the solution multiset in antenna and interval order. The staged
    # append has a different physical row order than independent single solves.
    expected = {name: np.repeat(values, repetitions, axis=-1)
                for name, values in reference[1].items()}
    order = lambda data: np.lexsort((data["ANTENNA1"], data["FIELD_ID"], data["TIME"]))
    actual_order = order(actual)
    expected_order = order(expected)
    for name, values in expected.items():
        observed = actual[name][..., actual_order]
        wanted = values[..., expected_order]
        if name in ("CPARAM", "POLY_COEFF_AMP", "POLY_COEFF_PHASE"):
            np.testing.assert_allclose(observed, wanted, atol=3e-6, rtol=1e-5, err_msg=name)
        else:
            np.testing.assert_array_equal(observed, wanted, err_msg=name)


@pytest.mark.parametrize(
    ("profile", "stage", "existing", "append", "store"),
    [
        pytest.param("gain", "observation-batch", False, False, "ms_trial.ms", id="msv2-batch"),
        pytest.param("gain", "observation-batch", False, False, "ms_trial.ps.zarr", id="msv4-batch"),
        pytest.param("legacy-gain", "legacy-batch", False, False, "ms_trial.ms", id="legacy-gain-batch"),
        pytest.param("bandpass", "bandpass-channel", True, False, "ms_trial.ms", id="bandpass-channel"),
        pytest.param("bpoly", "polynomial-spectrum", False, False, "ms_trial.ms", id="bpoly-spectrum"),
        pytest.param("gain", "before-publication", False, False, "ms_trial.ms", id="gain-create"),
        pytest.param("gain", "before-publication", True, True, "ms_trial.ms", id="gain-append"),
        pytest.param("bandpass", "before-publication", True, False, "ms_trial.ms", id="bandpass-replace"),
        pytest.param("bpoly", "before-publication", True, True, "ms_trial.ms", id="bpoly-append"),
        pytest.param("gain", "publication-accepted", True, True, "ms_trial.ms", id="accepted-append"),
    ],
)
def test_solve_sigint_respects_calculation_and_publication(
    tmp_path, monkeypatch, profile, stage, existing, append, store
):
    prepare_store(tmp_path, monkeypatch, store)
    surface, options = REQUESTS[profile]
    solve = getattr(tasks, surface)
    polynomial = profile == "bpoly"
    request = dict(vis=store, refant="0", smodel=[1.0], minsnr=0.0, minblperant=1, **options)
    source = tmp_path / store
    destination = tmp_path / "output.cal"
    original_source = image_contents(source)
    reference_path = tmp_path / "reference.cal"
    solve(caltable=str(reference_path), **request)
    reference = read_caltable(reference_path, polynomial)
    assert reference[1]["ANTENNA1"].size > 0
    if existing:
        shutil.copytree(reference_path, destination, ignore=shutil.ignore_patterns("table.lock"))
        original_destination = image_contents(destination)

    trace = tmp_path / "solve.logtable"
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "off")
    monkeypatch.setenv("CASA_RS_LOG_TABLE_PRIORITY", "DEBUG")
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(trace))
    _core._arm_solve_test(stage, 7)
    with pytest.raises(KeyboardInterrupt):
        solve(caltable=str(destination), append=append, **request)

    log = messages(trace)
    prefix = "solve test checkpoint "
    checkpoints = [line.removeprefix(prefix) for line in log if line.startswith(prefix)]
    if stage == "publication-accepted":
        assert checkpoints[-2:] == ["BeforePublication", "PublicationAccepted"]
        assert f"{surface} completed task={surface}" in log
        assert_solutions(destination, reference, polynomial, 2)
    else:
        if stage in EARLY_CHECKPOINTS:
            phase, index = EARLY_CHECKPOINTS[stage]
            assert [label for label in checkpoints if label.startswith(phase)] == [
                f"{phase}Started({index})", f"{phase}Finished({index})"
            ]
            assert checkpoints[-1] == f"{phase}Finished({index})"
        else:
            assert checkpoints[-1] == "BeforePublication"
        assert f"{surface} interrupted task={surface} reason={surface} interrupted" in log
        if existing:
            assert image_contents(destination) == original_destination
        else:
            assert not destination.exists()

    assert not list(tmp_path.glob(".output.cal.casa-rs-*"))
    assert image_contents(source) == original_source
    solve(caltable=str(destination), append=append, **request)
    repeats = 3 if stage == "publication-accepted" else (2 if append else 1)
    assert_solutions(destination, reference, polynomial, repeats)
    assert not list(tmp_path.glob(".output.cal.casa-rs-*"))


def test_gaincal_sigint_stops_prior_calibration_preview(tmp_path, monkeypatch):
    prepare_store(tmp_path, monkeypatch, "ms_trial.ms")
    common = dict(vis="ms_trial.ms", refant="0", smodel=[1.0], minsnr=0.0, minblperant=1)
    tasks.bandpass(caltable="prior.cal", bandtype="B", **common)
    prior = image_contents(tmp_path / "prior.cal")
    request = dict(**common, **GAIN, gaintable=["prior.cal"])
    tasks.gaincal(caltable="reference.cal", **request)
    trace = tmp_path / "prior.logtable"
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "off")
    monkeypatch.setenv("CASA_RS_LOG_TABLE_PRIORITY", "DEBUG")
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(trace))

    _core._arm_solve_test("prior-batch", 0)
    with pytest.raises(KeyboardInterrupt):
        tasks.gaincal(caltable="output.cal", **request)
    log = messages(trace)
    prefix = "solve test checkpoint "
    assert [line.removeprefix(prefix) for line in log if line.startswith(prefix)] == [
        "PriorBatchStarted(0)", "PriorBatchFinished(0)",
    ]
    assert "gaincal interrupted task=gaincal reason=gaincal interrupted" in log
    assert image_contents(tmp_path / "prior.cal") == prior
    assert not (tmp_path / "output.cal").exists()
    assert not list(tmp_path.glob(".output.cal.casa-rs-*"))
    tasks.gaincal(caltable="output.cal", **request)
    assert_solutions("output.cal", read_caltable("reference.cal", False), False, 1)


def test_bpoly_sigint_after_masked_spectra_reports_interruption(tmp_path, monkeypatch):
    prepare_store(tmp_path, monkeypatch, "ms_trial.ms")
    trace = tmp_path / "masked.logtable"
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "off")
    monkeypatch.setenv("CASA_RS_LOG_TABLE_PRIORITY", "DEBUG")
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(trace))

    _, options = REQUESTS["bpoly"]
    request = dict(vis="ms_trial.ms", caltable="output.cal", refant="0", smodel=[1.0],
                   minsnr=0.0, minblperant=1, **options)
    _core._arm_solve_test("polynomial-spectra-finished", 0)
    with pytest.raises(KeyboardInterrupt):
        tasks.bandpass(**request, maskcenter=8)
    log = messages(trace)
    checkpoints = [line for line in log if line.startswith("solve test checkpoint ")]
    assert checkpoints[-1] == "solve test checkpoint PolynomialSpectraFinished"
    assert "bandpass interrupted task=bandpass reason=bandpass interrupted" in log
    assert not (tmp_path / "output.cal").exists()
    assert not list(tmp_path.glob(".output.cal.casa-rs-*"))
    # An uninterrupted empty fit is still successful and leaves the path free.
    tasks.bandpass(**request, maskcenter=8)
    assert not (tmp_path / "output.cal").exists()
    tasks.bandpass(**request)
    assert read_caltable("output.cal", True)[1]["ANTENNA1"].size == 5


@needs_measures
def test_legacy_gaincal_sigint_joins_the_pipelined_reader(tmp_path, monkeypatch):
    import os
    import subprocess
    import sys


    available = (len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity")
                 else os.cpu_count() or 1)
    if available < 2:
        pytest.skip("the input pipeline requires two available CPUs")
    monkeypatch.chdir(tmp_path)
    simulator = tools.simulator()
    assert simulator.open("pipeline.ms")
    configure(simulator, autocorrelations=False)
    assert simulator.observe("F", "L", starttime="0s", stoptime="600s")
    assert simulator.close()
    reader = tools.table()
    assert reader.open("pipeline.ms", nomodify=False)
    assert reader.nrows() > 256
    gains = (1 + .04 * np.arange(4)) * np.exp(.13j * np.arange(4))
    a, b = (reader.getcol(column) for column in ("ANTENNA1", "ANTENNA2"))
    data = np.broadcast_to(gains[a] * gains[b].conj(), reader.getcol("DATA").shape).astype("complex64")
    assert reader.putcol("DATA", data)
    assert reader.putcol("FLAG", np.zeros(data.shape, dtype=bool))
    assert reader.putcol("FLAG_ROW", np.zeros(a.size, dtype=bool))
    assert reader.done()

    trace = tmp_path / "pipeline.logtable"
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "off")
    monkeypatch.setenv("CASA_RS_LOG_TABLE_PRIORITY", "DEBUG")
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(trace))
    monkeypatch.setenv("RAYON_NUM_THREADS", "2")
    worker = """
from casars import _core, tasks
_core._arm_solve_test('legacy-batch', 0)
try:
    tasks.gaincal(
        vis='pipeline.ms', caltable='output.cal', gaintype='G', solint='int',
        refant='0', refantmode='strict', smodel=[1.0], minsnr=0.0, minblperant=1,
    )
except KeyboardInterrupt:
    pass
else:
    raise AssertionError('gaincal did not raise KeyboardInterrupt')
"""
    result = subprocess.run([sys.executable, "-c", worker], timeout=20,
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    log = messages(trace)
    prefix = "solve test checkpoint "
    assert [line.removeprefix(prefix) for line in log if line.startswith(prefix)] == [
        "LegacyPipelineStarted", "LegacyBatchStarted(0)", "LegacyBatchFinished(0)",
    ]
    assert "gaincal interrupted task=gaincal reason=gaincal interrupted" in log
    assert not (tmp_path / "output.cal").exists()
    assert not list(tmp_path.glob(".output.cal.casa-rs-*"))
    tasks.gaincal(
        vis="pipeline.ms", caltable="output.cal", refant="0", smodel=[1.0],
        minsnr=0.0, minblperant=1, **dict(GAIN, solint="int")
    )
    _, solution = read_caltable("output.cal", False)
    assert not solution["FLAG"].any()
    expected = np.broadcast_to(gains[solution["ANTENNA1"]], solution["CPARAM"].shape)
    np.testing.assert_allclose(solution["CPARAM"], expected, rtol=1e-3, atol=2e-5)
