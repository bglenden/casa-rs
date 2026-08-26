#!/usr/bin/env python3
"""Run the focused T24/T25 CASA minor-cycle oracle.

Usage (inside a CASA Python environment):

    python tools/science/casa_solver_oracle.py INPUT.ms OUTPUT_DIR

The selected bundled fixture must contain field 1 / SPW 1.  This recipe first
makes the measured, Nyquist-sampled PSF and then gives Clark and multiscale the
same controlled point-plus-extended dirty plane.  It intentionally isolates
minor-cycle science from gridding and does not run a performance workload.
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import sys

import numpy as np
from casatasks import tclean
from casatasks.private.imagerhelpers.input_parameters import ImagerParameters
from casatools import iterbotsink, synthesisdeconvolver

from casa_solver_support import copy_seed, normalize, read_plane, write_plane


def plane_summary(values: np.ndarray) -> dict:
    f32 = np.asarray(values, dtype="<f4")
    return {
        "sha256_f32le": hashlib.sha256(f32.tobytes(order="C")).hexdigest(),
        "sum": float(f32.astype(np.float64).sum()),
        "l1": float(np.abs(f32.astype(np.float64)).sum()),
        "maximum": float(f32.max()),
        "minimum": float(f32.min()),
        "nonzero": int(np.count_nonzero(f32)),
    }


def verify_expected(evidence: dict, expected_path: pathlib.Path) -> None:
    expected = json.loads(expected_path.read_text())
    fixture = expected["fixture"]
    if evidence["shape"] != fixture["shape"]:
        raise AssertionError("CASA solver oracle shape changed")
    for source in ("psf", "dirty", "true_model"):
        if evidence[source]["sha256_f32le"] != fixture[f"{source}_sha256_f32le"]:
            raise AssertionError(f"CASA solver oracle {source} changed")
    for solver in ("clark", "multiscale"):
        actual = evidence[solver]
        wanted = expected[solver]
        summary = actual["summary"]
        if summary["iterdone"] != wanted["iterations"]:
            raise AssertionError(f"{solver} iteration count changed")
        if summary["stopcode"] != wanted["stopcode"]:
            raise AssertionError(f"{solver} stop reason changed")
        peak, flux, threshold = (
            summary["summaryminor"][index][0] for index in (1, 2, 3)
        )
        for name, actual_value, expected_value in (
            ("terminal peak", peak, wanted["terminal_peak"]),
            ("model flux", flux, wanted["summary_model_flux"]),
            ("cycle threshold", threshold, wanted["cycle_threshold"]),
        ):
            if not np.isclose(actual_value, expected_value, rtol=1.0e-7, atol=1.0e-7):
                raise AssertionError(f"{solver} {name} changed")
        for product in ("model", "residual"):
            if actual[product]["sha256_f32le"] != wanted[f"{product}_sha256_f32le"]:
                raise AssertionError(f"{solver} {product} changed")
        if actual["model"]["nonzero"] != wanted["model_nonzero_pixels"]:
            raise AssertionError(f"{solver} scale support changed")


def seed_psf(ms_path: pathlib.Path, output: pathlib.Path) -> pathlib.Path:
    prefix = output / "seed"
    tclean(
        vis=str(ms_path),
        field="1",
        spw="1",
        imagename=str(prefix),
        imsize=[64, 64],
        cell="0.02arcsec",
        phasecenter=1,
        specmode="mfs",
        gridder="standard",
        stokes="I",
        weighting="natural",
        deconvolver="clark",
        # One throwaway iteration asks CASA to materialize the complete image
        # store (including model and mask); the controlled solve below replaces
        # both model and residual before any measured result is recorded.
        niter=1,
        cycleniter=1,
        gain=0.1,
        threshold="0Jy",
        usemask="user",
        restoration=False,
        pbcor=False,
        savemodel="none",
        calcpsf=True,
        calcres=True,
        interactive=False,
        verbose=False,
    )
    return prefix


def solve(
    ms_path: pathlib.Path,
    seed: pathlib.Path,
    output: pathlib.Path,
    dirty: np.ndarray,
    deconvolver: str,
    scales: list[int] | None = None,
) -> dict:
    prefix = output / deconvolver
    copy_seed(seed, prefix, ("psf", "residual", "model", "sumwt", "pb", "mask"))
    write_plane(pathlib.Path(f"{prefix}.model"), np.zeros(dirty.shape))
    write_plane(pathlib.Path(f"{prefix}.residual"), dirty)
    write_plane(pathlib.Path(f"{prefix}.mask"), np.ones(dirty.shape))

    parameters = ImagerParameters(
        msname=str(ms_path),
        field="1",
        spw="1",
        imagename=str(prefix),
        imsize=list(dirty.shape),
        cell=["0.02arcsec", "0.02arcsec"],
        phasecenter=1,
        specmode="mfs",
        gridder="standard",
        stokes="I",
        weighting="natural",
        deconvolver=deconvolver,
        scales=scales or [],
        scalebias=0.0,
        niter=8,
        cycleniter=8,
        loopgain=0.2,
        threshold="0Jy",
        cyclefactor=1.0,
        minpsffraction=0.05,
        maxpsffraction=0.8,
        usemask="user",
        interactive=False,
        calcpsf=False,
        calcres=False,
        savemodel="none",
    )
    deconvolution = parameters.getDecPars()["0"]
    deconvolution["noRequireSumwt"] = True
    solver = synthesisdeconvolver()
    controller = iterbotsink()
    try:
        solver.setupdeconvolution(decpars=deconvolution)
        controller.setupiteration(iterpars=parameters.getIterPars())
        solver.initminorcycle()
        solver.setupmask()
        initial = solver.initminorcycle()
        controller.mergeinitrecord(initial, 0)
        controls = controller.getminorcyclecontrols()
        execution = solver.executeminorcycle(iterbotrecord=controls)
        controller.mergeexecrecord(execution, 0)
        stopcode = controller.cleanComplete()
        summary = controller.getiterationsummary()
    finally:
        solver.done()
        controller.done()

    return {
        "initial": normalize(initial),
        "controls": normalize(controls),
        "execution": normalize(execution),
        "global_stopcode": normalize(stopcode),
        "summary": normalize(summary),
        "model": plane_summary(read_plane(pathlib.Path(f"{prefix}.model"))),
        "residual": plane_summary(read_plane(pathlib.Path(f"{prefix}.residual"))),
    }


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: casa_solver_oracle.py INPUT.ms OUTPUT_DIR")
    ms_path = pathlib.Path(sys.argv[1]).resolve()
    output = pathlib.Path(sys.argv[2]).resolve()
    output.mkdir(parents=True, exist_ok=True)
    seed = seed_psf(ms_path, output)
    psf = read_plane(pathlib.Path(f"{seed}.psf"))
    center = tuple(extent // 2 for extent in psf.shape)
    if tuple(np.unravel_index(int(np.argmax(psf)), psf.shape)) != center or psf[center] != 1.0:
        raise RuntimeError("measured PSF is not centered and normalized")

    axis0, axis1 = np.indices(psf.shape)
    sky = np.zeros(psf.shape, dtype=np.float64)
    sky[22, 21] = 4.0
    sky += 0.2 * np.exp(-((axis1 - 43.0) ** 2 + (axis0 - 42.0) ** 2) / 32.0)
    dirty = np.fft.ifft2(np.fft.fft2(sky) * np.fft.fft2(np.fft.ifftshift(psf))).real
    evidence = {
        "schema": "casa-rs-direct-solver-oracle-v1",
        "measurement_set": str(ms_path),
        "shape": list(psf.shape),
        "psf": plane_summary(psf),
        "dirty": plane_summary(dirty),
        "true_model": plane_summary(sky),
        "clark": solve(ms_path, seed, output, dirty, "clark"),
        "multiscale": solve(ms_path, seed, output, dirty, "multiscale", [0, 7]),
    }
    evidence_path = output / "solver-oracle.json"
    evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    verify_expected(
        evidence,
        pathlib.Path(__file__).with_name("casa_solver_oracle_expected.json"),
    )
    print(evidence_path)


if __name__ == "__main__":
    main()
