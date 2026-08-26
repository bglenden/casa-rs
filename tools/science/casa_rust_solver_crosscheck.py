#!/usr/bin/env python3
"""Focused T24-T30 CASA/Rust solver, product, and MODEL_DATA cross-check.

Run this inside CASA 6.7.6.14 from the repository root:

    python tools/science/casa_rust_solver_crosscheck.py INPUT.ms OUTPUT_DIR

The gate runs bounded 64x64 cases against identical owner inputs. It compares
solver histories and mask topology directly, then checks prediction and
residual visibilities samplewise with the same selection and flag census.
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys

import numpy as np


TOLERANCE = 1.0e-3
RUST_STOP_CODES = {
    "IterationLimitReached": 1,
    "GlobalThresholdReached": 2,
    "NsigmaThresholdReached": 2,
    "CycleThresholdReached": 3,
    "NoCleanablePixels": 7,
    "MajorCycleLimitReached": 9,
    "DivergenceDetected": 10,
}


def load_casa() -> None:
    """Load CASA only in the isolated scientific worker process.

    CASA's C++ configuration reader owns a process-global lock. Keeping these
    imports out of the orchestration parent prevents it from contending with
    the child when auto-multithresh queries the configured memory allowance.
    """
    global clearcal, tclean, ImagerParameters
    global image, iterbotsink, synthesisdeconvolver, table

    from casatasks import clearcal as casa_clearcal
    from casatasks import tclean as casa_tclean
    from casatasks.private.imagerhelpers.input_parameters import (
        ImagerParameters as CasaImagerParameters,
    )
    from casatools import image as casa_image
    from casatools import iterbotsink as casa_iterbotsink
    from casatools import synthesisdeconvolver as casa_synthesisdeconvolver
    from casatools import table as casa_table

    clearcal = casa_clearcal
    tclean = casa_tclean
    ImagerParameters = CasaImagerParameters
    image = casa_image
    iterbotsink = casa_iterbotsink
    synthesisdeconvolver = casa_synthesisdeconvolver
    table = casa_table


def json_value(value):
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    return value


def plane(path: pathlib.Path) -> np.ndarray:
    tool = image()
    tool.open(str(path))
    try:
        return np.asarray(tool.getchunk()).squeeze().astype(np.float64)
    finally:
        tool.close()


def write_plane(path: pathlib.Path, values: np.ndarray) -> None:
    tool = image()
    tool.open(str(path))
    try:
        tool.putchunk(np.asarray(values, dtype=np.float32)[:, :, None, None])
    finally:
        tool.close()


def normalized_rms(actual: np.ndarray, expected: np.ndarray) -> float:
    scale = max(
        float(np.sqrt(np.mean(np.abs(expected) ** 2))), np.finfo(float).tiny
    )
    return float(np.sqrt(np.mean(np.abs(actual - expected) ** 2)) / scale)


def peak_normalized_rms(actual: np.ndarray, expected: np.ndarray) -> float:
    """Return difference RMS normalized by the reference absolute peak.

    Sparse CLEAN models and late-cycle residuals have deliberately small
    plane RMS values.  Their declared cross-pipeline denominator is therefore
    the CASA reference peak; the identical-input direct-solver check below
    retains the stricter reference-RMS denominator.
    """
    scale = max(float(np.max(np.abs(expected))), np.finfo(float).tiny)
    return float(np.sqrt(np.mean(np.abs(actual - expected) ** 2)) / scale)


def mask_topology_digest(mask: np.ndarray) -> str:
    """Hash canonical x-major mask support independently of image storage."""
    support = np.ascontiguousarray(np.asarray(mask, dtype=np.uint8))
    return hashlib.sha256(support.tobytes(order="C")).hexdigest()


def residual_after_model(
    initial: np.ndarray, psf: np.ndarray, model: np.ndarray
) -> np.ndarray:
    """Apply CASA's centered circular PSF normal operator to one model."""
    residual = np.array(initial, dtype=np.float64, copy=True)
    peak = tuple(int(value) for value in np.unravel_index(np.argmax(psf), psf.shape))
    for model_pixel in np.argwhere(model != 0):
        model_x, model_y = (int(value) for value in model_pixel)
        flux = float(model[model_x, model_y])
        residual -= flux * np.roll(
            psf,
            (model_x - peak[0], model_y - peak[1]),
            axis=(0, 1),
        )
    return residual


def assert_close(label: str, actual: float, expected: float) -> None:
    if not np.isclose(actual, expected, rtol=TOLERANCE, atol=1.0e-12):
        raise AssertionError(
            f"first divergence: {label}: Rust={actual:.17g}, CASA={expected:.17g}"
        )


def casa_predict_fixed_model(
    ms_path: pathlib.Path,
    prefix: pathlib.Path,
    model_path: pathlib.Path,
) -> None:
    """Ask CASA's production imager to predict one identical fixed model."""
    tclean(
        vis=str(ms_path),
        imagename=str(prefix),
        field="1",
        spw="1",
        imsize=[64, 64],
        cell=["0.02arcsec", "0.02arcsec"],
        phasecenter=1,
        specmode="mfs",
        gridder="standard",
        stokes="I",
        weighting="natural",
        deconvolver="clark",
        niter=0,
        startmodel=str(model_path),
        savemodel="modelcolumn",
        datacolumn="data",
        calcpsf=True,
        calcres=True,
        restoration=False,
        interactive=False,
    )


def visibility_census_and_parity(
    casa_ms: pathlib.Path,
    rust_ms: pathlib.Path,
    diagnostic: dict,
) -> dict:
    """Compare paired products at every MS cell and explain exclusions."""
    casa = table()
    rust = table()
    casa.open(str(casa_ms))
    rust.open(str(rust_ms))
    try:
        if casa.nrows() != rust.nrows():
            raise AssertionError(
                f"first divergence: MODEL_DATA row count: Rust={rust.nrows()}, "
                f"CASA={casa.nrows()}"
            )

        description = table()
        description.open(str(casa_ms / "DATA_DESCRIPTION"))
        try:
            spw_by_description = [
                int(description.getcell("SPECTRAL_WINDOW_ID", row))
                for row in range(description.nrows())
            ]
        finally:
            description.close()

        polarization = table()
        polarization.open(str(casa_ms / "POLARIZATION"))
        try:
            correlations_by_polarization = [
                [int(value) for value in polarization.getcell("CORR_TYPE", row)]
                for row in range(polarization.nrows())
            ]
        finally:
            polarization.close()
        description.open(str(casa_ms / "DATA_DESCRIPTION"))
        try:
            polarization_by_description = [
                int(description.getcell("POLARIZATION_ID", row))
                for row in range(description.nrows())
            ]
        finally:
            description.close()

        parallel_hands = {1, 5, 8, 9, 12}
        census = {
            "selected_unflagged_parallel": 0,
            "selected_flagged_parallel": 0,
            "selected_cross_hand": 0,
            "outside_selection": 0,
            "nonfinite_selected_parallel": 0,
        }
        selected_casa_model = []
        selected_rust_model = []
        selected_casa_residual = []
        selected_rust_residual = []
        selected_addresses = []
        excluded_nonzero = 0

        for row in range(casa.nrows()):
            field = int(casa.getcell("FIELD_ID", row))
            description_id = int(casa.getcell("DATA_DESC_ID", row))
            spw = spw_by_description[description_id]
            correlations = correlations_by_polarization[
                polarization_by_description[description_id]
            ]
            parallel_indices = [
                index
                for index, correlation_type in enumerate(correlations)
                if correlation_type in parallel_hands
            ]
            casa_data = np.asarray(casa.getcell("DATA", row))
            rust_data = np.asarray(rust.getcell("DATA", row))
            casa_model = np.asarray(casa.getcell("MODEL_DATA", row))
            rust_model = np.asarray(rust.getcell("MODEL_DATA", row))
            flags = np.asarray(casa.getcell("FLAG", row), dtype=bool)
            row_flag = bool(casa.getcell("FLAG_ROW", row))
            if not np.array_equal(casa_data, rust_data):
                raise AssertionError(f"first divergence: input DATA differs at row {row}")
            if casa_model.shape != rust_model.shape or casa_model.shape != casa_data.shape:
                raise AssertionError(
                    f"first divergence: visibility shape at row {row}: "
                    f"Rust={rust_model.shape}, CASA={casa_model.shape}, DATA={casa_data.shape}"
                )

            for correlation, correlation_type in enumerate(correlations):
                for channel in range(casa_data.shape[1]):
                    selected = field == 1 and spw == 1
                    parallel = correlation_type in parallel_hands
                    flagged = row_flag or (
                        any(bool(flags[index, channel]) for index in parallel_indices)
                        if parallel
                        else bool(flags[correlation, channel])
                    )
                    finite = bool(
                        np.isfinite(casa_data[correlation, channel].real)
                        and np.isfinite(casa_data[correlation, channel].imag)
                    )
                    if not selected:
                        category = "outside_selection"
                    elif not parallel:
                        category = "selected_cross_hand"
                    elif flagged:
                        category = "selected_flagged_parallel"
                    elif not finite:
                        category = "nonfinite_selected_parallel"
                    else:
                        category = "selected_unflagged_parallel"
                    census[category] += 1

                    casa_prediction = casa_model[correlation, channel]
                    rust_prediction = rust_model[correlation, channel]
                    if category == "selected_unflagged_parallel":
                        selected_addresses.append((row, channel, correlation, correlation_type))
                        selected_casa_model.append(casa_prediction)
                        selected_rust_model.append(rust_prediction)
                        selected_casa_residual.append(
                            casa_data[correlation, channel] - casa_prediction
                        )
                        selected_rust_residual.append(
                            rust_data[correlation, channel] - rust_prediction
                        )
                    else:
                        if casa_prediction != 0 or rust_prediction != 0:
                            excluded_nonzero += 1
                        if casa_prediction != rust_prediction:
                            raise AssertionError(
                                "first divergence: excluded MODEL_DATA "
                                f"row={row} channel={channel} correlation={correlation} "
                                f"category={category}: Rust={rust_prediction!r}, "
                                f"CASA={casa_prediction!r}"
                            )

        expected_samples = census["selected_unflagged_parallel"]
        if diagnostic["sample_count"] != expected_samples:
            raise AssertionError(
                "first divergence: visibility authority sample census: "
                f"Rust={diagnostic['sample_count']}, table={expected_samples}"
            )
        for field in (
            "problem_id",
            "final_model_generation",
            "selected_generation",
            "weighting_generation",
            "model_product",
            "residual_product",
        ):
            value = diagnostic[field]
            if len(value) != 64 or set(value) == {"0"}:
                raise AssertionError(
                    f"first divergence: invalid visibility provenance {field}={value!r}"
                )
        if diagnostic["model_product"] == diagnostic["residual_product"]:
            raise AssertionError("first divergence: model/residual product identities alias")

        casa_model_values = np.asarray(selected_casa_model)
        rust_model_values = np.asarray(selected_rust_model)
        casa_residual_values = np.asarray(selected_casa_residual)
        rust_residual_values = np.asarray(selected_rust_residual)
        # Compare the complex samples themselves, not only their magnitudes:
        # phase errors and conjugation mistakes are scientific differences.
        model_metric = normalized_rms(rust_model_values, casa_model_values)
        residual_metric = normalized_rms(rust_residual_values, casa_residual_values)
        if model_metric > TOLERANCE or residual_metric > TOLERANCE:
            difference = np.abs(rust_model_values - casa_model_values)
            sample = int(np.argmax(difference))
            row, channel, correlation, correlation_type = selected_addresses[sample]
            raise AssertionError(
                "first divergence: selected MODEL_DATA "
                f"sample={sample} row={row} channel={channel} "
                f"correlation={correlation} correlation_type={correlation_type}: "
                f"Rust={rust_model_values[sample]!r}, "
                f"CASA={casa_model_values[sample]!r}, "
                f"model normalized RMS={model_metric:.17g}, "
                f"residual normalized RMS={residual_metric:.17g}"
            )
        return {
            "census": census,
            "excluded_nonzero_predictions": excluded_nonzero,
            "model_normalized_rms": model_metric,
            "residual_normalized_rms": residual_metric,
            "provenance": diagnostic,
            "operator_contract": {
                "field": "1",
                "spw": "1",
                "phasecenter": 1,
                "cell_arcsec": 0.02,
                "stokes": "I",
                "gridder": "standard",
                "specmode": "mfs",
                "datacolumn": "data",
            },
        }
    finally:
        rust.close()
        casa.close()


def remove_fully_undefined_optional_spectra(path: pathlib.Path) -> None:
    """Make the historical fixture express its actual scalar-weight contract."""
    tool = table()
    tool.open(str(path), nomodify=False)
    try:
        for column in ("WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"):
            if column in tool.colnames() and not any(
                tool.iscelldefined(column, row) for row in range(tool.nrows())
            ):
                tool.removecols(column)
    finally:
        tool.close()


def run(
    command: list[str], *, environment: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command, text=True, capture_output=True, env=environment
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed ({completed.returncode}): {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed


def rust_case(
    ms_path: pathlib.Path,
    prefix: pathlib.Path,
    case: dict,
    *,
    iterations: int,
    save_model: bool,
    initialize_owner: bool = True,
) -> str:
    if initialize_owner:
        run([
            "cargo", "run", "--quiet", "-p", "casa-ms", "--example",
            "initialize_imaging_owner", "--", str(ms_path),
        ])
    gain = case.get("gain", 0.2)
    cycle_iterations = case.get("cycle_iterations", max(iterations, 1))
    nmajor = case.get("nmajor", 1)
    threshold_jy = case.get("threshold_jy", 0.0)
    nsigma = case.get("nsigma", 0.0)
    cyclefactor = case.get("cyclefactor", 1.0)
    command = [
        "cargo", "run", "--release", "--quiet", "-p", "casars-imager", "--",
        "--ms", str(ms_path), "--imagename", str(prefix),
        "--imsize", "64", "--cell-arcsec", "0.02", "--field", "1",
        "--spw", "1", "--deconvolver", case["solver"], "--niter", str(iterations),
        "--minor-cycle-length", str(cycle_iterations), "--nmajor", str(nmajor),
        "--gain", str(gain), "--threshold-jy", str(threshold_jy),
        "--nsigma", str(nsigma), "--cyclefactor", str(cyclefactor),
        "--minpsffraction", "0.05", "--maxpsffraction", "0.8",
        "--maximum-model-update-jy", "100", "--savemodel",
        "modelcolumn" if save_model else "none",
        "--managed-output", "true",
    ]
    if case["solver"] == "multiscale":
        command += [
            "--scales",
            ",".join(str(scale) for scale in case["scales"]),
            "--smallscalebias",
            "0",
        ]
    if case["mask"] == "auto-multithresh":
        command += [
            "--usemask", "auto-multithresh",
            "--sidelobethreshold", str(case.get("sidelobethreshold", 3.0)),
            "--noisethreshold", str(case.get("noisethreshold", 5.0)),
            "--lownoisethreshold", str(case.get("lownoisethreshold", 1.5)),
            "--negativethreshold", str(case.get("negativethreshold", 0.0)),
            "--minbeamfrac", str(case.get("minbeamfrac", 0.3)),
            "--growiterations", str(case.get("growiterations", 75)),
        ]
    elif case["mask"] == "box":
        command += ["--mask-box", ",".join(str(value) for value in case["box"])]
    elif case["mask"] == "image":
        command += ["--mask-image", str(case["mask_image"])]
    environment = os.environ.copy()
    environment.update({
        "CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND": "1000000000",
        "CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND": "1000000000",
    })
    return run(command, environment=environment).stdout


def copy_seed(seed: pathlib.Path, target: pathlib.Path) -> None:
    for suffix in ("psf", "residual", "model", "sumwt", "mask"):
        source = pathlib.Path(f"{seed}.{suffix}")
        destination = pathlib.Path(f"{target}.{suffix}")
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(source, destination)


def casa_case(
    ms_path: pathlib.Path,
    seed: pathlib.Path,
    prefix: pathlib.Path,
    case: dict,
) -> dict:
    def prepare_direct_mask(bound_prefix: pathlib.Path, shape: tuple[int, ...]) -> None:
        target = pathlib.Path(f"{bound_prefix}.mask")
        if case["mask"] == "user":
            write_plane(target, np.ones(shape))
        elif case["mask"] == "auto-multithresh":
            # The direct deconvolver owns creation of this generation. Do not
            # let the copied Rust dirty-run mask become prior CASA support.
            write_plane(target, np.zeros(shape))
        else:
            write_plane(target, plane(case["materialized_mask"]))

    def run_bound(bound_prefix: pathlib.Path, iteration_bound: int) -> dict:
        copy_seed(seed, bound_prefix)
        shape = plane(pathlib.Path(f"{bound_prefix}.residual")).shape
        write_plane(pathlib.Path(f"{bound_prefix}.model"), np.zeros(shape))
        prepare_direct_mask(bound_prefix, shape)

        parameters = ImagerParameters(
            msname=str(ms_path), field="1", spw="1", imagename=str(bound_prefix),
            imsize=list(shape), cell=["0.02arcsec", "0.02arcsec"], phasecenter=1,
            specmode="mfs", gridder="standard", stokes="I", weighting="natural",
            deconvolver=case["solver"],
            scales=case.get("scales", []),
            scalebias=0.0, niter=iteration_bound, cycleniter=iteration_bound,
            loopgain=case.get("gain", 0.2),
        threshold=f"{case.get('threshold_jy', 0.0)}Jy",
        nsigma=case.get("nsigma", 0.0), cyclefactor=case.get("cyclefactor", 1.0),
        minpsffraction=0.05,
            maxpsffraction=0.8,
            usemask=(
                "user" if case["mask"] in ("box", "image") else case["mask"]
            ),
            interactive=False,
        sidelobethreshold=case.get("sidelobethreshold", 3.0),
        noisethreshold=case.get("noisethreshold", 5.0),
        lownoisethreshold=case.get("lownoisethreshold", 1.5),
        negativethreshold=case.get("negativethreshold", 0.0),
        minbeamfrac=case.get("minbeamfrac", 0.3),
        growiterations=case.get("growiterations", 75),
        calcpsf=False, calcres=False, savemodel="none",
        fullsummary=True,
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
            "initial": initial,
            "controls": controls,
            "execution": execution,
            "global_stopcode": stopcode,
            "summary": summary,
        }

    # MatrixCleaner retains a different finite-update path when several
    # components are accepted by one call. CASA's full-summary record exposes
    # only the terminal row, so recover the real multi-iteration trajectory by
    # running deterministic prefix bounds 1..N from the identical seed. A
    # repeated one-iteration call is not equivalent: finalizeDeconvolver then
    # recomputes a circular full-image residual between components.
    if case["solver"] == "multiscale":
        component_trace = []
        final = None
        selected_peak = None
        cumulative_flux = 0.0
        for iteration_bound in range(1, case["cycle_iterations"] + 1):
            bound_prefix = (
                prefix
                if iteration_bound == case["cycle_iterations"]
                else prefix.with_name(f"{prefix.name}-bound-{iteration_bound}")
            )
            bounded = run_bound(bound_prefix, iteration_bound)
            if selected_peak is None:
                selected_peak = float(bounded["initial"]["peakresidual"])
            peaks = bounded["execution"]["summaryminor"][1]
            if len(peaks) != 1:
                raise AssertionError(
                    f"first divergence: CASA {case['name']} terminal trace "
                    f"length at bound {iteration_bound}: peaks={len(peaks)}"
                )
            terminal_peak = float(peaks[-1])
            next_cumulative_flux = float(
                np.sum(plane(pathlib.Path(f"{bound_prefix}.model")))
            )
            component_trace.append({
                "selected_peak": selected_peak,
                "selected_component_strength": abs(
                    (next_cumulative_flux - cumulative_flux)
                    / case.get("gain", 0.2)
                ),
                "terminal_peak": terminal_peak,
                "cumulative_model_flux": next_cumulative_flux,
            })
            cumulative_flux = next_cumulative_flux
            selected_peak = terminal_peak
            final = bounded
        assert final is not None
    else:
        # Point-clean paths have a component trace in CASA's cumulative
        # summary when one solver instance advances one component at a time.
        copy_seed(seed, prefix)
        shape = plane(pathlib.Path(f"{prefix}.residual")).shape
        write_plane(pathlib.Path(f"{prefix}.model"), np.zeros(shape))
        prepare_direct_mask(prefix, shape)
        parameters = ImagerParameters(
            msname=str(ms_path), field="1", spw="1", imagename=str(prefix),
            imsize=list(shape), cell=["0.02arcsec", "0.02arcsec"], phasecenter=1,
            specmode="mfs", gridder="standard", stokes="I", weighting="natural",
            deconvolver=case["solver"], scales=case.get("scales", []),
            scalebias=0.0, niter=case["cycle_iterations"],
            cycleniter=case["cycle_iterations"],
            loopgain=case.get("gain", 0.2),
            threshold=f"{case.get('threshold_jy', 0.0)}Jy",
            nsigma=case.get("nsigma", 0.0), cyclefactor=case.get("cyclefactor", 1.0),
            minpsffraction=0.05,
            maxpsffraction=0.8,
            usemask=(
                "user" if case["mask"] in ("box", "image") else case["mask"]
            ),
            interactive=False, sidelobethreshold=case.get("sidelobethreshold", 3.0),
            noisethreshold=case.get("noisethreshold", 5.0),
            lownoisethreshold=case.get("lownoisethreshold", 1.5),
            negativethreshold=case.get("negativethreshold", 0.0),
            minbeamfrac=case.get("minbeamfrac", 0.3),
            growiterations=case.get("growiterations", 75), calcpsf=False,
            calcres=False, savemodel="none", fullsummary=True,
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
            peaks = execution["summaryminor"][1]
            fluxes = execution["summaryminor"][2]
            if len(peaks) != case["cycle_iterations"] or len(fluxes) != len(peaks):
                raise AssertionError(
                    f"first divergence: CASA {case['name']} component trace "
                    f"length: peaks={len(peaks)}, flux={len(fluxes)}"
                )
            selected_peaks = [float(initial["peakresidual"])] + [
                float(peak) for peak in peaks[:-1]
            ]
            component_trace = [
                {
                    "selected_peak": selected_peak,
                    "terminal_peak": float(terminal_peak),
                    "cumulative_model_flux": float(cumulative_flux),
                }
                for selected_peak, terminal_peak, cumulative_flux in zip(
                    selected_peaks, peaks, fluxes
                )
            ]
            controller.mergeexecrecord(execution, 0)
            stopcode = controller.cleanComplete()
            summary = controller.getiterationsummary()
        finally:
            solver.done()
            controller.done()
        final = {
            "initial": initial,
            "controls": controls,
            "execution": execution,
            "global_stopcode": stopcode,
            "summary": summary,
        }
    return json_value({
        "initial": final["initial"],
        "controls": final["controls"],
        "execution": final["execution"],
        "component_trace": component_trace,
        "global_stopcode": final["global_stopcode"],
        "summary": final["summary"],
    })


def casa_full_case(
    ms_path: pathlib.Path,
    prefix: pathlib.Path,
    case: dict,
) -> dict:
    """Run CASA's full major/minor controller with the same bounded controls."""
    mask = ""
    if case["mask"] == "box":
        mask = str(case["casa_mask_image"])
    elif case["mask"] == "image":
        mask = str(case["mask_image"])
    result = tclean(
        vis=str(ms_path), imagename=str(prefix), field="1", spw="1",
        imsize=[64, 64], cell=["0.02arcsec", "0.02arcsec"], phasecenter=1,
        specmode="mfs", gridder="standard", stokes="I", weighting="natural",
        deconvolver=case["solver"], scales=case.get("scales", []),
        smallscalebias=0.0, niter=case["iterations"],
        cycleniter=case["cycle_iterations"], nmajor=case["nmajor"],
        gain=case.get("gain", 0.2), threshold=f"{case.get('threshold_jy', 0.0)}Jy",
        nsigma=case.get("nsigma", 0.0), cyclefactor=case.get("cyclefactor", 1.0),
        minpsffraction=0.05, maxpsffraction=0.8, usemask=case["casa_usemask"],
        mask=mask, sidelobethreshold=case.get("sidelobethreshold", 3.0),
        noisethreshold=case.get("noisethreshold", 5.0),
        lownoisethreshold=case.get("lownoisethreshold", 1.5),
        negativethreshold=case.get("negativethreshold", 0.0),
        minbeamfrac=case.get("minbeamfrac", 0.3),
        growiterations=case.get("growiterations", 75), calcpsf=True, calcres=True,
        restoration=False, interactive=False, fullsummary=True,
    )
    if not isinstance(result, dict):
        raise AssertionError(
            f"first divergence: CASA {case['name']} omitted full controller summary"
        )
    return json_value(result)


def make_reprojected_mask(reference: pathlib.Path, output: pathlib.Path) -> None:
    """Create a smaller CASA image whose support must be reprojected to 64x64."""
    if output.exists():
        shutil.rmtree(output)
    source = image()
    source.open(str(reference))
    try:
        regridded = source.regrid(
            outfile=str(output), shape=[32, 32, 1, 1], axes=[0, 1],
            method="nearest", overwrite=True,
        )
    finally:
        source.close()
    try:
        support = np.zeros((32, 32, 1, 1), dtype=np.float32)
        support[6:26, 8:24, 0, 0] = 1.0
        regridded.putchunk(support)
    finally:
        regridded.close()


def materialize_reprojected_mask(
    source_path: pathlib.Path, reference_path: pathlib.Path, output: pathlib.Path
) -> None:
    """Ask CASA to reproject one source mask onto the target image WCS."""
    if output.exists():
        shutil.rmtree(output)
    reference = image()
    reference.open(str(reference_path))
    try:
        target_shape = list(reference.shape())
        target_coordinates = reference.coordsys().torecord()
    finally:
        reference.close()
    source = image()
    source.open(str(source_path))
    try:
        regridded = source.regrid(
            outfile=str(output),
            shape=target_shape,
            csys=target_coordinates,
            axes=[0, 1],
            method="nearest",
            overwrite=True,
        )
    finally:
        source.close()
    regridded.close()


def make_box_mask(
    reference: pathlib.Path, output: pathlib.Path, box: list[int]
) -> None:
    """Materialize the exact inclusive Rust box as a CASA image mask."""
    if output.exists():
        shutil.rmtree(output)
    shutil.copytree(reference, output)
    support = np.zeros(plane(output).shape, dtype=np.float32)
    x0, y0, x1, y1 = box
    support[x0 : x1 + 1, y0 : y1 + 1] = 1.0
    write_plane(output, support)


def compare_minor_reference(case: dict, casa_summary: dict, rust_cycle: dict) -> None:
    """Compare one bounded first minor-cycle trajectory component by component."""
    casa_cycle = casa_summary["summary"]
    if rust_cycle["iterations"] != casa_cycle["iterdone"]:
        raise AssertionError(
            f"first divergence: {case['name']} first-cycle iterations: "
            f"Rust={rust_cycle['iterations']}, CASA={casa_cycle['iterdone']}"
        )
    components = rust_cycle["components"]
    casa_trace = casa_summary["component_trace"]
    if len(components) != case["cycle_iterations"]:
        raise AssertionError(
            f"first divergence: {case['name']} component history length: "
            f"Rust={len(components)}, expected={case['cycle_iterations']}"
        )
    if len(casa_trace) != len(components):
        raise AssertionError(
            f"first divergence: {case['name']} CASA history length: "
            f"CASA={len(casa_trace)}, Rust={len(components)}"
        )
    gain = case.get("gain", 0.2)
    cumulative_flux = 0.0
    for index, (component, casa_component) in enumerate(zip(components, casa_trace)):
        selected_strength = (
            casa_component["selected_component_strength"]
            if case["solver"] == "multiscale"
            else abs(casa_component["selected_peak"])
        )
        assert_close(
            f"{case['name']} component {index} selected peak",
            abs(component["flux"]) / gain,
            selected_strength,
        )
        cumulative_flux += component["flux"]
        assert_close(
            f"{case['name']} component {index} cumulative model flux",
            cumulative_flux,
            casa_component["cumulative_model_flux"],
        )
    assert_close(
        f"{case['name']} terminal peak",
        rust_cycle["final_peak_flux"],
        casa_trace[-1]["terminal_peak"],
    )
    if case["solver"] == "multiscale" and components[0]["scale_px"] != case["scales"][0]:
        raise AssertionError(
            f"first divergence: {case['name']} selected scale: "
            f"Rust={components[0]['scale_px']}, expected={case['scales'][0]}"
        )
    assert_close(
        f"{case['name']} cycle threshold",
        rust_cycle["cycle_threshold"],
        casa_cycle["cyclethreshold"],
    )
    if rust_cycle["stop_reason"] != "iteration_bound" or casa_cycle["stopcode"] != 1:
        raise AssertionError(
            f"first divergence: {case['name']} first-cycle stop: "
            f"Rust={rust_cycle['stop_reason']}, CASA={casa_cycle['stopcode']}"
        )


def validate_runtime_mask_evidence(
    case: dict, rust_run: dict, rust_mask: np.ndarray
) -> None:
    """Bind the published topology to the exact current-cycle owner evidence."""
    previous_support = np.zeros(rust_mask.shape, dtype=bool)
    for rust_cycle in rust_run["minor_cycles"]:
        rust_support = np.asarray(rust_cycle["mask_support"], dtype=bool).reshape(
            rust_mask.shape
        )
        auto_evidence = rust_cycle["auto_mask"]
        if case["mask"] == "auto-multithresh":
            if auto_evidence is None:
                raise AssertionError(
                    "first divergence: automask execution omitted current owner evidence"
                )
            changed_pixels = int(np.count_nonzero(rust_support != previous_support))
            if auto_evidence["changed_pixels"] != changed_pixels:
                raise AssertionError(
                    "first divergence: automask changed-pixel evidence does not "
                    "match the current generated transition"
                )
            for field in (
                "median",
                "robust_rms",
                "positive_threshold",
                "low_noise_threshold",
            ):
                if not np.isfinite(auto_evidence[field]):
                    raise AssertionError(
                        f"first divergence: automask {field} is not finite"
                    )
        elif auto_evidence is not None:
            raise AssertionError(
                f"first divergence: static-mask case {case['name']} emitted automask evidence"
            )
        previous_support = rust_support
    if not np.array_equal(previous_support, rust_mask):
        mismatch = int(np.count_nonzero(previous_support != rust_mask))
        raise AssertionError(
            f"first divergence: {case['name']} runtime mask evidence differs "
            f"from its published mask at {mismatch} pixels"
        )


def validate_transitive_automask_artifact(
    baseline_path: pathlib.Path, current_mask: np.ndarray
) -> dict:
    """Prove current Rust topology equals a recorded CASA-matched artifact."""
    baseline_path = baseline_path.resolve()
    evidence_path = baseline_path.parent.parent / "evidence.json"
    if not baseline_path.exists() or not evidence_path.exists():
        raise AssertionError(
            "T27 transitive proof requires both the baseline Rust mask and its "
            f"recorded CASA evidence: {baseline_path}, {evidence_path}"
        )
    baseline_evidence = json.loads(evidence_path.read_text())
    if baseline_evidence.get("schema") != "casa-rs-solver-crosscheck-v1":
        raise AssertionError("T27 baseline uses an unknown evidence schema")
    baseline_case = baseline_evidence.get("cases", {}).get("automask")
    if (
        not isinstance(baseline_case, dict)
        or baseline_case.get("casa_summary") is None
        or baseline_case.get("rust_summary") is None
        or baseline_case.get("model_normalized_rms", float("inf")) > TOLERANCE
    ):
        raise AssertionError("T27 baseline does not record a passing CASA/Rust artifact")
    baseline_mask = plane(baseline_path) != 0
    baseline_pixels = int(np.count_nonzero(baseline_mask))
    if baseline_case.get("mask_pixels") != baseline_pixels:
        raise AssertionError("T27 baseline evidence does not bind its mask pixel count")
    if baseline_mask.shape != current_mask.shape or not np.array_equal(
        baseline_mask, current_mask
    ):
        mismatch = (
            -1
            if baseline_mask.shape != current_mask.shape
            else int(np.count_nonzero(baseline_mask != current_mask))
        )
        raise AssertionError(
            "first divergence: current Rust automask differs from the previously "
            f"CASA-matched Rust artifact at {mismatch} pixels"
        )
    digest = mask_topology_digest(current_mask)
    return {
        "baseline_evidence": str(evidence_path.resolve()),
        "baseline_mask": str(baseline_path),
        "baseline_mask_topology_sha256": digest,
        "current_mask_topology_sha256": digest,
        "bit_identical": True,
    }


def main() -> None:
    if len(sys.argv) not in (3, 4):
        raise SystemExit(
            "usage: casa_rust_solver_crosscheck.py INPUT.ms OUTPUT_DIR [CASE]"
        )
    source = pathlib.Path(sys.argv[1]).resolve()
    output = pathlib.Path(sys.argv[2]).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if len(sys.argv) == 3:
        # CASA's synthesisdeconvolver implementations retain incompatible
        # process-global C++ state across solver kinds. Give each scientific
        # comparison a fresh CASA process, then combine their machine-readable
        # evidence. This is isolation only; each case still runs exactly once.
        combined = {"schema": "casa-rs-solver-crosscheck-v1", "cases": {}}
        for name in (
            "clark", "multiscale", "automask", "controls-box", "controls-image",
        ):
            print(f"running isolated {name} CASA/Rust cross-check", flush=True)
            run([
                sys.executable,
                str(pathlib.Path(__file__).resolve()),
                str(source),
                str(output),
                name,
            ])
            evidence_path = output / "evidence.json"
            case_evidence = json.loads(evidence_path.read_text())
            combined["cases"].update(case_evidence["cases"])
        (output / "evidence.json").write_text(
            json.dumps(combined, indent=2, sort_keys=True) + "\n"
        )
        return
    load_casa()
    prepared_source = output / "prepared-input.ms"
    if not prepared_source.exists():
        run([
            "cargo", "run", "--quiet", "-p", "casa-ms", "--example",
            "rewrite_measurement_set", "--", str(source), str(prepared_source),
        ])
        remove_fully_undefined_optional_spectra(prepared_source)
    cases = [
        {
            "name": "clark", "solver": "clark", "mask": "user",
            "casa_usemask": "user", "iterations": 6, "cycle_iterations": 3,
            "nmajor": 3, "minor_reference": True, "require_multiple_major": True,
        },
        {
            "name": "multiscale",
            "solver": "multiscale",
            "mask": "user",
            "casa_usemask": "user",
            "iterations": 6,
            "cycle_iterations": 3,
            "nmajor": 2,
            "minor_reference": True,
            "scales": [7],
        },
        {
            "name": "automask",
            "solver": "clark",
            "mask": "auto-multithresh",
            "casa_usemask": "auto-multithresh",
            "iterations": 4,
            "cycle_iterations": 2,
            "nmajor": 2,
            "minor_reference": True,
            # The tiny fixture has no default-threshold emission. Exercise a
            # substantive owner-generated mask with the same explicit CASA
            # and Rust controls rather than inheriting a full seed mask.
            "sidelobethreshold": 0.5,
            "noisethreshold": 1.0,
            "lownoisethreshold": 1.0,
            "minbeamfrac": 0.0,
            "growiterations": 1,
        },
        {
            "name": "controls-box", "solver": "clark", "mask": "box",
            "casa_usemask": "user", "box": [7, 9, 54, 51],
            "iterations": 7, "cycle_iterations": 1, "nmajor": 3,
            "gain": 0.17, "threshold_jy": 1.0e-6, "cyclefactor": 1.4,
            "minor_reference": True, "direct_controls_oracle": True,
        },
        {
            "name": "controls-image", "solver": "clark", "mask": "image",
            "casa_usemask": "user", "iterations": 8, "cycle_iterations": 1,
            "nmajor": 3, "gain": 0.13, "threshold_jy": 2.0e-6,
            "nsigma": 2.5, "cyclefactor": 1.7, "minor_reference": True,
            "direct_controls_oracle": True,
        },
    ]
    if len(sys.argv) == 4:
        requested = sys.argv[3]
        cases = [case for case in cases if case["name"] == requested]
        if not cases:
            raise SystemExit(f"unknown cross-check case: {requested}")
    evidence = {"schema": "casa-rs-solver-crosscheck-v1", "cases": {}}
    for case in cases:
        baseline_mask_path = (
            pathlib.Path(os.environ["CASA_RS_T27_BASELINE_MASK"])
            if case["name"] == "automask"
            and os.environ.get("CASA_RS_T27_BASELINE_MASK")
            else None
        )
        transitive_automask = baseline_mask_path is not None
        root = output / case["name"]
        casa_ms = root / "casa.ms"
        casa_minor_ms = root / "casa-minor.ms"
        rust_ms = root / "rust.ms"
        rust_minor_ms = root / "rust-minor.ms"
        seed_ms = root / "seed.ms"
        root.mkdir(parents=True, exist_ok=True)
        shutil.copytree(prepared_source, seed_ms)
        casa_prefix = root / "casa"
        casa_minor_prefix = root / "casa-minor"
        rust_prefix = root / "rust"
        rust_minor_prefix = root / "rust-minor"
        seed_prefix = root / "seed"
        # Establish the same CASA-compatible MODEL_DATA generation before
        # either prediction owner runs. Otherwise the two owners legitimately
        # choose different first-creation values for excluded cells, which
        # obscures the required exclusion proof.
        clearcal(vis=str(seed_ms), addmodel=True)
        seed_case = dict(case)
        seed_case.update({"mask": "user", "casa_usemask": "user"})
        rust_case(seed_ms, seed_prefix, seed_case, iterations=0, save_model=False)
        if case["mask"] == "image":
            mask_image = root / "reprojected-source.mask"
            make_reprojected_mask(pathlib.Path(f"{seed_prefix}.mask"), mask_image)
            case["mask_image"] = mask_image
            materialized_mask = root / "reprojected-target.mask"
            materialize_reprojected_mask(
                mask_image,
                pathlib.Path(f"{seed_prefix}.mask"),
                materialized_mask,
            )
            case["materialized_mask"] = materialized_mask
        elif case["mask"] == "box":
            mask_image = root / "box-source.mask"
            make_box_mask(pathlib.Path(f"{seed_prefix}.mask"), mask_image, case["box"])
            case["casa_mask_image"] = mask_image
            case["materialized_mask"] = mask_image
        if not transitive_automask:
            shutil.copytree(seed_ms, casa_ms)
        if case["minor_reference"] and not transitive_automask:
            shutil.copytree(seed_ms, casa_minor_ms)
            shutil.copytree(seed_ms, rust_minor_ms)
        shutil.copytree(seed_ms, rust_ms)
        direct_controls_oracle = case.get("direct_controls_oracle", False)
        casa_summary = (
            None
            if transitive_automask or direct_controls_oracle
            else casa_full_case(casa_ms, casa_prefix, case)
        )
        casa_minor_summary = (
            casa_case(casa_minor_ms, seed_prefix, casa_minor_prefix, case)
            if case["minor_reference"] and not transitive_automask
            else None
        )
        rust_minor_summary = None
        if case["minor_reference"] and not transitive_automask:
            minor_case = dict(case)
            minor_case["nmajor"] = 1
            rust_minor_summary = json.loads(rust_case(
                rust_minor_ms,
                rust_minor_prefix,
                minor_case,
                iterations=case["cycle_iterations"],
                save_model=False,
                initialize_owner=False,
            ))
        rust_summary = None
        if not direct_controls_oracle:
            rust_stdout = rust_case(
                rust_ms,
                rust_prefix,
                case,
                iterations=case["iterations"],
                save_model=True,
                initialize_owner=False,
            )
            rust_summary = json.loads(rust_stdout)
        if casa_summary is not None:
            (root / "casa-summary.json").write_text(
                json.dumps(casa_summary, indent=2, sort_keys=True) + "\n"
            )
        if casa_minor_summary is not None:
            (root / "casa-minor-summary.json").write_text(
                json.dumps(casa_minor_summary, indent=2, sort_keys=True) + "\n"
            )
            (root / "rust-minor-summary.json").write_text(
                json.dumps(rust_minor_summary, indent=2, sort_keys=True) + "\n"
            )
        if rust_summary is not None:
            (root / "rust-summary.json").write_text(
                json.dumps(rust_summary, indent=2, sort_keys=True) + "\n"
            )
        if transitive_automask:
            rust_run = rust_summary["run"]
            rust_mask = plane(pathlib.Path(f"{rust_prefix}.mask")) != 0
            validate_runtime_mask_evidence(case, rust_run, rust_mask)
            first_cycle_mask = np.asarray(
                rust_run["minor_cycles"][0]["mask_support"], dtype=bool
            ).reshape(rust_mask.shape)
            transitive_artifact = validate_transitive_automask_artifact(
                baseline_mask_path, first_cycle_mask
            )
            transitive_artifact.update({
                "comparison_cycle": 1,
                "final_current_mask_topology_sha256": mask_topology_digest(rust_mask),
                "final_current_mask_pixels": int(np.count_nonzero(rust_mask)),
            })
            evidence["cases"][case["name"]] = {
                "controls": {
                    "gain": case.get("gain", 0.2),
                    "threshold_jy": case.get("threshold_jy", 0.0),
                    "nsigma": case.get("nsigma", 0.0),
                    "niter": case["iterations"],
                    "cycleniter": case["cycle_iterations"],
                    "nmajor": case["nmajor"],
                    "cyclefactor": case.get("cyclefactor", 1.0),
                    "mask": case["mask"],
                },
                "mask_pixels": int(np.count_nonzero(rust_mask)),
                "rust_summary": rust_summary,
                "transitive_artifact": transitive_artifact,
            }
            continue
        if direct_controls_oracle:
            assert casa_minor_summary is not None
            assert rust_minor_summary is not None
            rust_minor_run = rust_minor_summary["run"]
            if len(rust_minor_run["minor_cycles"]) != 1:
                raise AssertionError(
                    f"first divergence: {case['name']} Rust direct reference "
                    f"ran {len(rust_minor_run['minor_cycles'])} minor cycles"
                )
            rust_minor_model = plane(pathlib.Path(f"{rust_minor_prefix}.model"))
            casa_minor_model = plane(pathlib.Path(f"{casa_minor_prefix}.model"))
            rust_minor_residual = residual_after_model(
                plane(pathlib.Path(f"{seed_prefix}.residual")),
                plane(pathlib.Path(f"{seed_prefix}.psf")),
                rust_minor_model,
            )
            casa_minor_residual = plane(pathlib.Path(f"{casa_minor_prefix}.residual"))
            direct_model_metric = normalized_rms(rust_minor_model, casa_minor_model)
            direct_residual_metric = normalized_rms(
                rust_minor_residual, casa_minor_residual
            )
            if direct_model_metric > TOLERANCE or direct_residual_metric > TOLERANCE:
                raise AssertionError(
                    f"first divergence: {case['name']} direct masked controls: "
                    f"model normalized RMS={direct_model_metric:.17g}, "
                    f"residual normalized RMS={direct_residual_metric:.17g}"
                )
            compare_minor_reference(
                case, casa_minor_summary, rust_minor_run["minor_cycles"][0]
            )
            casa_mask = plane(pathlib.Path(f"{casa_minor_prefix}.mask")) != 0
            rust_mask = plane(pathlib.Path(f"{rust_minor_prefix}.mask")) != 0
            if not np.array_equal(casa_mask, rust_mask):
                mismatch = int(np.count_nonzero(casa_mask != rust_mask))
                raise AssertionError(
                    f"{case['name']} direct mask differs at {mismatch} pixels"
                )
            validate_runtime_mask_evidence(case, rust_minor_run, rust_mask)
            evidence["cases"][case["name"]] = {
                "oracle": "CASA synthesisdeconvolver direct masked-controls oracle",
                "controls": {
                    "gain": case.get("gain", 0.2),
                    "threshold_jy": case.get("threshold_jy", 0.0),
                    "nsigma": case.get("nsigma", 0.0),
                    "niter": case["cycle_iterations"],
                    "cyclefactor": case.get("cyclefactor", 1.0),
                    "mask": case["mask"],
                },
                "mask_pixels": int(np.count_nonzero(rust_mask)),
                "model_normalized_rms": direct_model_metric,
                "residual_normalized_rms": direct_residual_metric,
                "casa_summary": casa_minor_summary,
                "rust_summary": rust_minor_summary,
            }
            continue
        assert casa_summary is not None
        rust_model_plane = plane(pathlib.Path(f"{rust_prefix}.model"))
        casa_model_plane = plane(pathlib.Path(f"{casa_prefix}.model"))
        rust_residual_plane = plane(pathlib.Path(f"{rust_prefix}.residual"))
        casa_residual_plane = plane(pathlib.Path(f"{casa_prefix}.residual"))
        model_metric = peak_normalized_rms(rust_model_plane, casa_model_plane)
        residual_metric = peak_normalized_rms(rust_residual_plane, casa_residual_plane)
        if model_metric > TOLERANCE or residual_metric > TOLERANCE:
            difference = np.abs(rust_model_plane - casa_model_plane)
            pixel = tuple(int(value) for value in np.unravel_index(np.argmax(difference), difference.shape))
            raise AssertionError(
                f"first divergence: {case['name']} model pixel {pixel}: "
                f"Rust={rust_model_plane[pixel]:.17g}, CASA={casa_model_plane[pixel]:.17g}, "
                f"model peak-normalized RMS={model_metric:.17g}, "
                f"residual peak-normalized RMS={residual_metric:.17g}"
            )
        rust_run = rust_summary["run"]
        casa_iterations = int(casa_summary["iterdone"])
        casa_major_cycles = int(casa_summary["nmajordone"])
        if rust_run["minor_iterations"] != casa_iterations:
            raise AssertionError(
                f"first divergence: {case['name']} controller iterations: "
                f"Rust={rust_run['minor_iterations']}, CASA={casa_iterations}"
            )
        if rust_run["major_cycles"] != casa_major_cycles:
            raise AssertionError(
                f"first divergence: {case['name']} major-cycle count: "
                f"Rust={rust_run['major_cycles']}, CASA={casa_major_cycles}"
            )
        if case.get("require_multiple_major", False) and rust_run["major_cycles"] <= 1:
            raise AssertionError(
                f"first divergence: {case['name']} did not execute multiple major reconciliations"
            )
        rust_stop = rust_run["clean_stop_reason"]
        casa_stop = int(casa_summary["stopcode"])
        if rust_stop not in RUST_STOP_CODES or RUST_STOP_CODES[rust_stop] != casa_stop:
            raise AssertionError(
                f"first divergence: {case['name']} controller stop: "
                f"Rust={rust_stop!r}, CASA={casa_stop}"
            )
        if casa_minor_summary is not None:
            rust_minor_run = rust_minor_summary["run"]
            if len(rust_minor_run["minor_cycles"]) != 1:
                raise AssertionError(
                    f"first divergence: {case['name']} Rust direct reference "
                    f"ran {len(rust_minor_run['minor_cycles'])} minor cycles"
                )
            rust_minor_model = plane(pathlib.Path(f"{rust_minor_prefix}.model"))
            casa_minor_model = plane(pathlib.Path(f"{casa_minor_prefix}.model"))
            rust_minor_residual = residual_after_model(
                plane(pathlib.Path(f"{seed_prefix}.residual")),
                plane(pathlib.Path(f"{seed_prefix}.psf")),
                rust_minor_model,
            )
            casa_minor_residual = plane(pathlib.Path(f"{casa_minor_prefix}.residual"))
            direct_model_metric = normalized_rms(rust_minor_model, casa_minor_model)
            direct_residual_metric = normalized_rms(
                rust_minor_residual, casa_minor_residual
            )
            if direct_model_metric > TOLERANCE or direct_residual_metric > TOLERANCE:
                difference = np.abs(rust_minor_model - casa_minor_model)
                pixel = tuple(int(value) for value in np.unravel_index(
                    np.argmax(difference), difference.shape
                ))
                raise AssertionError(
                    f"first divergence: {case['name']} direct minor model pixel {pixel}: "
                    f"Rust={rust_minor_model[pixel]:.17g}, "
                    f"CASA={casa_minor_model[pixel]:.17g}, "
                    f"model normalized RMS={direct_model_metric:.17g}, "
                    f"residual normalized RMS={direct_residual_metric:.17g}"
                )
            compare_minor_reference(
                case, casa_minor_summary, rust_minor_run["minor_cycles"][0]
            )
        if case["name"] == "clark" and sum(
            cycle["clark_refreshes"] for cycle in rust_run["minor_cycles"]
        ) == 0:
            raise AssertionError(
                "first divergence: Clark gate did not cross a patch-refresh boundary"
            )
        casa_mask = plane(pathlib.Path(f"{casa_prefix}.mask")) != 0
        rust_mask = plane(pathlib.Path(f"{rust_prefix}.mask")) != 0
        if not np.array_equal(casa_mask, rust_mask):
            mismatch = int(np.count_nonzero(casa_mask != rust_mask))
            raise AssertionError(f"{case['name']} mask differs at {mismatch} pixels")
        transitive_artifact = None
        if case["name"] == "automask":
            current_digest = mask_topology_digest(rust_mask)
            transitive_artifact = {
                "current_mask_topology_sha256": current_digest,
            }
            baseline_path = os.environ.get("CASA_RS_T27_BASELINE_MASK")
            if baseline_path:
                baseline_mask = plane(pathlib.Path(baseline_path)) != 0
                if baseline_mask.shape != rust_mask.shape or not np.array_equal(
                    baseline_mask, rust_mask
                ):
                    mismatch = (
                        -1
                        if baseline_mask.shape != rust_mask.shape
                        else int(np.count_nonzero(baseline_mask != rust_mask))
                    )
                    raise AssertionError(
                        "first divergence: current Rust automask differs from the "
                        f"previously CASA-matched Rust artifact at {mismatch} pixels"
                    )
                transitive_artifact.update(
                    {
                        "baseline_mask": str(pathlib.Path(baseline_path).resolve()),
                        "baseline_mask_topology_sha256": mask_topology_digest(
                            baseline_mask
                        ),
                        "bit_identical": True,
                    }
                )
        validate_runtime_mask_evidence(case, rust_run, rust_mask)
        case_evidence = {
            "controls": {
                "gain": case.get("gain", 0.2),
                "threshold_jy": case.get("threshold_jy", 0.0),
                "nsigma": case.get("nsigma", 0.0),
                "niter": case["iterations"],
                "cycleniter": case["cycle_iterations"],
                "nmajor": case["nmajor"],
                "cyclefactor": case.get("cyclefactor", 1.0),
                "mask": case["mask"],
            },
            "model_normalized_rms": model_metric,
            "residual_normalized_rms": residual_metric,
            "product_normalization": "CASA reference absolute peak",
            "mask_pixels": int(np.count_nonzero(rust_mask)),
            "casa_summary": casa_summary,
            "casa_minor_summary": casa_minor_summary,
            "rust_minor_summary": rust_minor_summary,
            "rust_summary": rust_summary,
        }
        if transitive_artifact is not None:
            case_evidence["transitive_artifact"] = transitive_artifact
        if casa_minor_summary is not None:
            case_evidence["direct_minor"] = {
                "model_normalized_rms": direct_model_metric,
                "residual_normalized_rms": direct_residual_metric,
                "normalization": "CASA reference plane RMS",
            }
        if case["name"] == "clark":
            diagnostic = rust_summary["run"].get("visibility_products")
            if diagnostic is None:
                raise AssertionError(
                    "first divergence: Rust omitted final visibility-product provenance"
                )
            fixed_model = root / "fixed-final.model"
            shutil.copytree(pathlib.Path(f"{rust_prefix}.model"), fixed_model)
            casa_predict_fixed_model(casa_ms, root / "casa-predict", fixed_model)
            case_evidence["visibility_products"] = visibility_census_and_parity(
                casa_ms, rust_ms, diagnostic
            )
        evidence["cases"][case["name"]] = case_evidence
    (output / "evidence.json").write_text(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
