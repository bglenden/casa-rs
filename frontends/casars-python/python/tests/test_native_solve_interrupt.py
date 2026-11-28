# SPDX-License-Identifier: LGPL-3.0-or-later
"""Interrupt public calibration solves during real work and private publication."""

import shutil

import numpy as np
import pytest

from casars import _core, tasks, tools
from test_gaincal_formats import prepare_store
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
