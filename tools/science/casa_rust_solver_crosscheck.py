#!/usr/bin/env python3
"""Focused T24-T30 CASA/Rust solver, product, and MODEL_DATA cross-check.

Run this inside CASA 6.7.6.14 from the repository root:

    python tools/science/casa_rust_solver_crosscheck.py INPUT.ms OUTPUT_DIR

The gate runs bounded 64x64 cases against identical owner inputs. It compares
solver histories and mask topology directly, then checks prediction and
residual visibilities samplewise with the same selection and flag census.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import sys

import numpy as np


TOLERANCE = 1.0e-3


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
    scale = max(float(np.sqrt(np.mean(expected * expected))), np.finfo(float).tiny)
    return float(np.sqrt(np.mean((actual - expected) ** 2)) / scale)


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
        model_metric = normalized_rms(
            np.abs(rust_model_values), np.abs(casa_model_values)
        )
        residual_metric = normalized_rms(
            np.abs(rust_residual_values), np.abs(casa_residual_values)
        )
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
    command = [
        "cargo", "run", "--quiet", "-p", "casars-imager", "--",
        "--ms", str(ms_path), "--imagename", str(prefix),
        "--imsize", "64", "--cell-arcsec", "0.02", "--field", "1",
        "--spw", "1", "--deconvolver", case["solver"], "--niter", str(iterations),
        "--minor-cycle-length", str(max(iterations, 1)), "--nmajor", "1", "--gain", "0.2",
        "--threshold-jy", "0", "--cyclefactor", "1.0",
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
    copy_seed(seed, prefix)
    shape = plane(pathlib.Path(f"{prefix}.residual")).shape
    write_plane(pathlib.Path(f"{prefix}.model"), np.zeros(shape))
    if case["mask"] == "user":
        write_plane(pathlib.Path(f"{prefix}.mask"), np.ones(shape))
    else:
        # The direct deconvolver owns creation of this generation. Do not let
        # the copied Rust dirty-run mask become prior CASA support.
        write_plane(pathlib.Path(f"{prefix}.mask"), np.zeros(shape))

    parameters = ImagerParameters(
        msname=str(ms_path), field="1", spw="1", imagename=str(prefix),
        imsize=list(shape), cell=["0.02arcsec", "0.02arcsec"], phasecenter=1,
        specmode="mfs", gridder="standard", stokes="I", weighting="natural",
        deconvolver=case["solver"],
        scales=case.get("scales", []),
        scalebias=0.0, niter=case["iterations"], cycleniter=case["iterations"], loopgain=0.2,
        threshold="0Jy", cyclefactor=1.0, minpsffraction=0.05,
        maxpsffraction=0.8, usemask=case["mask"], interactive=False,
        sidelobethreshold=case.get("sidelobethreshold", 3.0),
        noisethreshold=case.get("noisethreshold", 5.0),
        lownoisethreshold=case.get("lownoisethreshold", 1.5),
        negativethreshold=case.get("negativethreshold", 0.0),
        minbeamfrac=case.get("minbeamfrac", 0.3),
        growiterations=case.get("growiterations", 75),
        calcpsf=False, calcres=False, savemodel="none",
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
    return json_value({
        "initial": initial,
        "controls": controls,
        "execution": execution,
        "global_stopcode": stopcode,
        "summary": summary,
    })


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
        for name in ("clark", "multiscale", "automask"):
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
        {"name": "clark", "solver": "clark", "mask": "user", "iterations": 1},
        {
            "name": "multiscale",
            "solver": "multiscale",
            "mask": "user",
            "iterations": 1,
            "scales": [7],
        },
        {
            "name": "automask",
            "solver": "clark",
            "mask": "auto-multithresh",
            "iterations": 1,
            # The tiny fixture has no default-threshold emission. Exercise a
            # substantive owner-generated mask with the same explicit CASA
            # and Rust controls rather than inheriting a full seed mask.
            "sidelobethreshold": 0.5,
            "noisethreshold": 1.0,
            "lownoisethreshold": 1.0,
            "minbeamfrac": 0.0,
            "growiterations": 1,
        },
    ]
    if len(sys.argv) == 4:
        requested = sys.argv[3]
        cases = [case for case in cases if case["name"] == requested]
        if not cases:
            raise SystemExit(f"unknown cross-check case: {requested}")
    evidence = {"schema": "casa-rs-solver-crosscheck-v1", "cases": {}}
    for case in cases:
        root = output / case["name"]
        casa_ms = root / "casa.ms"
        rust_ms = root / "rust.ms"
        seed_ms = root / "seed.ms"
        root.mkdir(parents=True, exist_ok=True)
        shutil.copytree(prepared_source, seed_ms)
        casa_prefix = root / "casa"
        rust_prefix = root / "rust"
        seed_prefix = root / "seed"
        # Establish the same CASA-compatible MODEL_DATA generation before
        # either prediction owner runs. Otherwise the two owners legitimately
        # choose different first-creation values for excluded cells, which
        # obscures the required exclusion proof.
        clearcal(vis=str(seed_ms), addmodel=True)
        rust_case(seed_ms, seed_prefix, case, iterations=0, save_model=False)
        shutil.copytree(seed_ms, casa_ms)
        shutil.copytree(seed_ms, rust_ms)
        casa_summary = casa_case(casa_ms, seed_prefix, casa_prefix, case)
        rust_stdout = rust_case(
            rust_ms,
            rust_prefix,
            case,
            iterations=case["iterations"],
            save_model=True,
            initialize_owner=False,
        )
        rust_summary = json.loads(rust_stdout)
        (root / "casa-summary.json").write_text(
            json.dumps(casa_summary, indent=2, sort_keys=True) + "\n"
        )
        (root / "rust-summary.json").write_text(
            json.dumps(rust_summary, indent=2, sort_keys=True) + "\n"
        )
        rust_model_plane = plane(pathlib.Path(f"{rust_prefix}.model"))
        casa_model_plane = plane(pathlib.Path(f"{casa_prefix}.model"))
        model_metric = normalized_rms(rust_model_plane, casa_model_plane)
        if model_metric > TOLERANCE:
            difference = np.abs(rust_model_plane - casa_model_plane)
            pixel = tuple(int(value) for value in np.unravel_index(np.argmax(difference), difference.shape))
            raise AssertionError(
                f"first divergence: {case['name']} model pixel {pixel}: "
                f"Rust={rust_model_plane[pixel]:.17g}, CASA={casa_model_plane[pixel]:.17g}, "
                f"normalized RMS={model_metric:.17g}"
            )
        casa_cycle = casa_summary["summary"]
        rust_cycle = rust_summary["run"]["minor_cycles"][0]
        if rust_cycle["iterations"] != casa_cycle["iterdone"]:
            raise AssertionError(
                f"first divergence: {case['name']} iterations: "
                f"Rust={rust_cycle['iterations']}, CASA={casa_cycle['iterdone']}"
            )
        if case["solver"] == "clark":
            assert_close(
                f"{case['name']} initial peak",
                abs(rust_cycle["components"][0]["flux"]) / 0.2,
                casa_summary["initial"]["peakresidual"],
            )
            assert_close(
                f"{case['name']} terminal peak",
                rust_cycle["final_peak_flux"],
                casa_cycle["summaryminor"][1][-1],
            )
        elif rust_cycle["components"][0]["scale_px"] != case["scales"][0]:
            raise AssertionError(
                f"first divergence: {case['name']} selected scale: "
                f"Rust={rust_cycle['components'][0]['scale_px']}, "
                f"expected={case['scales'][0]}"
            )
        assert_close(
            f"{case['name']} cycle threshold",
            rust_cycle["cycle_threshold"],
            casa_cycle["cyclethreshold"],
        )
        assert_close(
            f"{case['name']} signed model flux",
            float(rust_model_plane.sum()),
            casa_cycle["summaryminor"][2][-1],
        )
        if rust_cycle["stop_reason"] != "iteration_bound" or casa_cycle["stopcode"] != 1:
            raise AssertionError(
                f"first divergence: {case['name']} stop: "
                f"Rust={rust_cycle['stop_reason']}, CASA={casa_cycle['stopcode']}"
            )
        casa_mask = plane(pathlib.Path(f"{casa_prefix}.mask")) != 0
        rust_mask = plane(pathlib.Path(f"{rust_prefix}.mask")) != 0
        if not np.array_equal(casa_mask, rust_mask):
            mismatch = int(np.count_nonzero(casa_mask != rust_mask))
            raise AssertionError(f"{case['name']} mask differs at {mismatch} pixels")
        case_evidence = {
            "model_normalized_rms": model_metric,
            "mask_pixels": int(np.count_nonzero(rust_mask)),
            "casa_summary": casa_summary,
            "rust_summary": rust_summary,
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
