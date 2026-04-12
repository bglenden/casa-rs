#!/usr/bin/env python3
"""Benchmark CASA imaging phases through PySynthesisImager."""

from __future__ import annotations

import os
import statistics
import tempfile
import time
from typing import Callable, Dict, List, Tuple

from casatasks.private.imagerhelpers.imager_base import PySynthesisImager
from casatasks.private.imagerhelpers.input_parameters import ImagerParameters


def env_int(name: str) -> int:
    return int(os.environ[name])


def env_float(name: str) -> float:
    return float(os.environ[name])


def env_str(name: str) -> str:
    return os.environ[name]


def millis(seconds: float) -> float:
    return seconds * 1_000.0


def median(values: List[float]) -> float:
    return statistics.median(values)


def median_int(values: List[int]) -> int:
    ordered = sorted(values)
    return ordered[len(ordered) // 2]


def timed(callable_obj: Callable, *args, **kwargs) -> Tuple[float, object]:
    started = time.perf_counter()
    result = callable_obj(*args, **kwargs)
    return time.perf_counter() - started, result


def main() -> None:
    vis = env_str("CASA_RS_BENCH_MS_PATH")
    repeats = env_int("CASA_RS_BENCH_REPEATS")
    field = env_str("CASA_RS_BENCH_FIELD")
    spw = env_str("CASA_RS_BENCH_SPW")
    chan_start = env_int("CASA_RS_BENCH_CHANNEL_START")
    chan_count = env_int("CASA_RS_BENCH_CHANNEL_COUNT")
    specmode = env_str("CASA_RS_BENCH_SPECMODE")
    imsize = env_int("CASA_RS_BENCH_IMSIZE")
    cell_arcsec = env_str("CASA_RS_BENCH_CELL_ARCSEC")
    weighting = env_str("CASA_RS_BENCH_WEIGHTING")
    robust = env_float("CASA_RS_BENCH_ROBUST")
    deconvolver = env_str("CASA_RS_BENCH_DECONVOLVER")
    scales_env = env_str("CASA_RS_BENCH_SCALES")
    niter = env_int("CASA_RS_BENCH_NITER")
    gain = env_float("CASA_RS_BENCH_GAIN")
    threshold_jy = env_str("CASA_RS_BENCH_THRESHOLD_JY")
    nsigma = env_float("CASA_RS_BENCH_NSIGMA")
    psfcutoff = env_float("CASA_RS_BENCH_PSFCUTOFF")
    cycleniter = env_int("CASA_RS_BENCH_MINOR_CYCLE_LENGTH")
    cyclefactor = env_float("CASA_RS_BENCH_CYCLEFACTOR")
    minpsffraction = env_float("CASA_RS_BENCH_MIN_PSFFRACTION")
    maxpsffraction = env_float("CASA_RS_BENCH_MAX_PSFFRACTION")
    interpolation = env_str("CASA_RS_BENCH_INTERPOLATION")

    scales = [] if scales_env == "" else [int(float(value)) for value in scales_env.split(",")]
    spw_selector = (
        f"{spw}:{chan_start}"
        if chan_count == 1
        else f"{spw}:{chan_start}~{chan_start + chan_count - 1}"
    )
    threshold = f"{threshold_jy}Jy"
    cell = [f"{cell_arcsec}arcsec", f"{cell_arcsec}arcsec"]
    imsize_vec = [imsize, imsize]
    restoration = True

    stage_names = [
        "parameter_setup",
        "construct_imager",
        "initialize_imagers",
        "initialize_normalizers",
        "set_weighting",
        "initialize_deconvolvers",
        "estimate_memory",
        "initialize_iteration_control",
        "make_psf",
        "make_pb",
        "calcres_major_cycle",
        "update_mask",
        "has_converged",
        "minor_cycle",
        "clean_major_cycle",
        "restore_images",
        "delete_tools",
        "total",
    ]
    stage_values: Dict[str, List[float]] = {name: [] for name in stage_names}
    clean_major_counts: List[int] = []
    minor_cycle_counts: List[int] = []

    with tempfile.TemporaryDirectory() as tempdir:
        for run_index in range(repeats):
            per_stage = {name: 0.0 for name in stage_names}
            clean_major_cycles = 0
            minor_cycles = 0
            total_started = time.perf_counter()
            imager = None
            try:
                parameter_kwargs = dict(
                    msname=vis,
                    imagename=os.path.join(tempdir, f"run-{run_index}"),
                    field=field,
                    spw=spw_selector if specmode == "mfs" else spw,
                    datacolumn="data",
                    imsize=imsize_vec,
                    cell=cell,
                    stokes="I",
                    projection="SIN",
                    specmode=specmode,
                    interpolation="nearest",
                    gridder="standard",
                    restart=True,
                    weighting=weighting,
                    robust=robust,
                    niter=niter,
                    cycleniter=cycleniter,
                    loopgain=gain,
                    threshold=threshold,
                    nsigma=nsigma,
                    cyclefactor=cyclefactor,
                    minpsffraction=minpsffraction,
                    maxpsffraction=maxpsffraction,
                    deconvolver=deconvolver,
                    scales=scales,
                    usemask="user",
                    mask="",
                    calcres=True,
                    calcpsf=True,
                    savemodel="none",
                    parallel=False,
                    psfcutoff=psfcutoff,
                )
                if specmode == "cube":
                    parameter_kwargs.update(
                        nchan=chan_count,
                        start=chan_start,
                        width=1,
                        interpolation=interpolation,
                    )
                elapsed, param_list = timed(ImagerParameters, **parameter_kwargs)
                per_stage["parameter_setup"] += elapsed

                elapsed, imager = timed(PySynthesisImager, params=param_list)
                per_stage["construct_imager"] += elapsed

                elapsed, _ = timed(imager.initializeImagers)
                per_stage["initialize_imagers"] += elapsed
                elapsed, _ = timed(imager.initializeNormalizers)
                per_stage["initialize_normalizers"] += elapsed
                elapsed, _ = timed(imager.setWeighting)
                per_stage["set_weighting"] += elapsed

                if niter > 0 or restoration:
                    elapsed, _ = timed(imager.initializeDeconvolvers)
                    per_stage["initialize_deconvolvers"] += elapsed

                elapsed, _ = timed(imager.estimatememory)
                per_stage["estimate_memory"] += elapsed

                if niter > 0:
                    elapsed, _ = timed(imager.initializeIterationControl)
                    per_stage["initialize_iteration_control"] += elapsed

                elapsed, _ = timed(imager.makePSF)
                per_stage["make_psf"] += elapsed
                elapsed, _ = timed(imager.makePB)
                per_stage["make_pb"] += elapsed

                elapsed, _ = timed(imager.runMajorCycle, isCleanCycle=False)
                per_stage["calcres_major_cycle"] += elapsed

                if niter > 0:
                    elapsed, converged = timed(imager.hasConverged)
                    per_stage["has_converged"] += elapsed
                    elapsed, _ = timed(imager.updateMask)
                    per_stage["update_mask"] += elapsed
                    elapsed, converged = timed(imager.hasConverged)
                    per_stage["has_converged"] += elapsed

                    while not converged:
                        elapsed, done_minor = timed(imager.runMinorCycle)
                        per_stage["minor_cycle"] += elapsed
                        minor_cycles += 1

                        if done_minor:
                            elapsed, _ = timed(imager.runMajorCycle)
                            per_stage["clean_major_cycle"] += elapsed
                            clean_major_cycles += 1

                        elapsed, _ = timed(imager.updateMask)
                        per_stage["update_mask"] += elapsed
                        elapsed, converged = timed(imager.hasConverged)
                        per_stage["has_converged"] += elapsed
                        converged = converged or (not done_minor)

                if restoration:
                    elapsed, _ = timed(imager.restoreImages)
                    per_stage["restore_images"] += elapsed
            finally:
                if imager is not None:
                    elapsed, _ = timed(imager.deleteTools)
                    per_stage["delete_tools"] += elapsed

            per_stage["total"] = time.perf_counter() - total_started
            for name in stage_names:
                stage_values[name].append(per_stage[name])
            clean_major_counts.append(clean_major_cycles)
            minor_cycle_counts.append(minor_cycles)
            print(
                "run={} total_ms={:.3f} param_setup_ms={:.3f} construct_imager_ms={:.3f} init_imagers_ms={:.3f} init_normalizers_ms={:.3f} set_weighting_ms={:.3f} init_deconvolvers_ms={:.3f} estimate_memory_ms={:.3f} init_iteration_ms={:.3f} make_psf_ms={:.3f} make_pb_ms={:.3f} calcres_major_ms={:.3f} update_mask_ms={:.3f} has_converged_ms={:.3f} minor_cycle_ms={:.3f} clean_major_cycle_ms={:.3f} restore_ms={:.3f} delete_tools_ms={:.3f}".format(
                    run_index + 1,
                    millis(per_stage["total"]),
                    millis(per_stage["parameter_setup"]),
                    millis(per_stage["construct_imager"]),
                    millis(per_stage["initialize_imagers"]),
                    millis(per_stage["initialize_normalizers"]),
                    millis(per_stage["set_weighting"]),
                    millis(per_stage["initialize_deconvolvers"]),
                    millis(per_stage["estimate_memory"]),
                    millis(per_stage["initialize_iteration_control"]),
                    millis(per_stage["make_psf"]),
                    millis(per_stage["make_pb"]),
                    millis(per_stage["calcres_major_cycle"]),
                    millis(per_stage["update_mask"]),
                    millis(per_stage["has_converged"]),
                    millis(per_stage["minor_cycle"]),
                    millis(per_stage["clean_major_cycle"]),
                    millis(per_stage["restore_images"]),
                    millis(per_stage["delete_tools"]),
                )
            )

    print("stage medians (ms):")
    for name in stage_names:
        print(f"  {name}={millis(median(stage_values[name])):.3f}")
    print(
        "result medians: clean_major_cycles={} minor_cycles={}".format(
            median_int(clean_major_counts), median_int(minor_cycle_counts)
        )
    )


if __name__ == "__main__":
    main()
