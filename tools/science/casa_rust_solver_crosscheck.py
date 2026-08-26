#!/usr/bin/env python3
"""Focused T24-T30 CASA/Rust solver, product, and MODEL_DATA cross-check.

Run this inside CASA 6.7.6.14 from the repository root:

    python tools/science/casa_rust_solver_crosscheck.py INPUT.ms OUTPUT_DIR

The gate runs only three 64x64, eight-iteration cases. It compares conventional
products by normalized RMS, mask topology exactly, and the complete MODEL_DATA
column samplewise (including samples excluded by field/SPW selection).
"""

from __future__ import annotations

import json
import pathlib
import shutil
import subprocess
import sys

import numpy as np
from casatasks import tclean
from casatools import image, table


TOLERANCE = 1.0e-3


def plane(path: pathlib.Path) -> np.ndarray:
    tool = image()
    tool.open(str(path))
    try:
        return np.asarray(tool.getchunk()).squeeze().astype(np.float64)
    finally:
        tool.close()


def normalized_rms(actual: np.ndarray, expected: np.ndarray) -> float:
    scale = max(float(np.sqrt(np.mean(expected * expected))), np.finfo(float).tiny)
    return float(np.sqrt(np.mean((actual - expected) ** 2)) / scale)


def model_data(path: pathlib.Path) -> np.ndarray:
    tool = table()
    tool.open(str(path))
    try:
        return np.asarray(tool.getcol("MODEL_DATA"))
    finally:
        tool.close()


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, text=True, capture_output=True)


def rust_case(ms_path: pathlib.Path, prefix: pathlib.Path, case: dict) -> str:
    run([
        "cargo", "run", "--quiet", "-p", "casa-ms", "--example",
        "initialize_imaging_owner", "--", str(ms_path),
    ])
    command = [
        "cargo", "run", "--quiet", "-p", "casars-imager", "--",
        "--ms", str(ms_path), "--imagename", str(prefix),
        "--imsize", "64", "--cell-arcsec", "0.02", "--field", "1",
        "--spw", "1", "--deconvolver", case["solver"], "--niter", "8",
        "--minor-cycle-length", "8", "--nmajor", "1", "--gain", "0.2",
        "--threshold-jy", "0", "--cyclefactor", "1.0",
        "--minpsffraction", "0.05", "--maxpsffraction", "0.8",
        "--maximum-model-update-jy", "100", "--savemodel", "modelcolumn",
        "--fullsummary",
    ]
    if case["solver"] == "multiscale":
        command += ["--scales", "0,7", "--smallscalebias", "0"]
    if case["mask"] == "auto-multithresh":
        command += ["--usemask", "auto-multithresh"]
    return run(command).stdout


def casa_case(ms_path: pathlib.Path, prefix: pathlib.Path, case: dict) -> None:
    tclean(
        vis=str(ms_path), field="1", spw="1", imagename=str(prefix),
        imsize=[64, 64], cell="0.02arcsec", phasecenter=1, specmode="mfs",
        gridder="standard", stokes="I", weighting="natural",
        deconvolver=case["solver"], scales=[0, 7] if case["solver"] == "multiscale" else [],
        scalebias=0.0, niter=8, cycleniter=8, nmajor=1, gain=0.2,
        threshold="0Jy", cyclefactor=1.0, minpsffraction=0.05,
        maxpsffraction=0.8, usemask=case["mask"], restoration=True,
        pbcor=False, savemodel="modelcolumn", interactive=False, verbose=False,
    )


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: casa_rust_solver_crosscheck.py INPUT.ms OUTPUT_DIR")
    source = pathlib.Path(sys.argv[1]).resolve()
    output = pathlib.Path(sys.argv[2]).resolve()
    output.mkdir(parents=True, exist_ok=True)
    cases = [
        {"name": "clark", "solver": "clark", "mask": "user"},
        {"name": "multiscale", "solver": "multiscale", "mask": "user"},
        {"name": "automask", "solver": "clark", "mask": "auto-multithresh"},
    ]
    evidence = {"schema": "casa-rs-solver-crosscheck-v1", "cases": {}}
    for case in cases:
        root = output / case["name"]
        casa_ms = root / "casa.ms"
        rust_ms = root / "rust.ms"
        root.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, casa_ms)
        shutil.copytree(source, rust_ms)
        casa_prefix = root / "casa"
        rust_prefix = root / "rust"
        casa_case(casa_ms, casa_prefix, case)
        rust_stdout = rust_case(rust_ms, rust_prefix, case)
        metrics = {}
        for member in ("model", "residual", "image"):
            metric = normalized_rms(
                plane(pathlib.Path(f"{rust_prefix}.{member}")),
                plane(pathlib.Path(f"{casa_prefix}.{member}")),
            )
            if metric > TOLERANCE:
                raise AssertionError(f"{case['name']} {member} normalized RMS {metric}")
            metrics[member] = metric
        casa_mask = plane(pathlib.Path(f"{casa_prefix}.mask")) != 0
        rust_mask = plane(pathlib.Path(f"{rust_prefix}.mask")) != 0
        if not np.array_equal(casa_mask, rust_mask):
            mismatch = int(np.count_nonzero(casa_mask != rust_mask))
            raise AssertionError(f"{case['name']} mask differs at {mismatch} pixels")
        casa_model = model_data(casa_ms)
        rust_model = model_data(rust_ms)
        if not np.allclose(rust_model, casa_model, rtol=TOLERANCE, atol=1.0e-7):
            raise AssertionError(f"{case['name']} MODEL_DATA differs samplewise")
        evidence["cases"][case["name"]] = {
            "normalized_rms": metrics,
            "mask_pixels": int(np.count_nonzero(rust_mask)),
            "model_data_samples": int(rust_model.size),
            "rust_summary": rust_stdout,
        }
    (output / "evidence.json").write_text(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
