#!/usr/bin/env python3
"""Trace where a CASA/casa-rs heavy MFS residual difference first appears."""

from __future__ import annotations

import argparse
import json
import math
import os
import pathlib
import shutil
import subprocess
import sys
import time
from typing import Any

import numpy as np
from casatasks import tclean
from casatools import image

import perf_paths


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_MS = pathlib.Path(
    "/Volumes/GLENDENNING/casa-rs-imperformance/wave1/vla/single/medium/ms/"
    "wave1-vla-single-medium.ms"
)
DEFAULT_OUTPUT = perf_paths.artifact_path("wave3", "residual-divergence")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ms", type=pathlib.Path, default=DEFAULT_MS)
    parser.add_argument("--output-dir", type=pathlib.Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--cases",
        default="dirty,one,first-cycle",
        help="comma-separated cases: dirty, one, first-cycle, full",
    )
    parser.add_argument("--imsize", type=int, default=1024)
    parser.add_argument("--cell-arcsec", type=float, default=0.25)
    parser.add_argument("--field", default="0")
    parser.add_argument("--spw", default="0")
    parser.add_argument("--channel-start", type=int, default=0)
    parser.add_argument("--channel-count", type=int, default=64)
    parser.add_argument("--weighting", default="briggs")
    parser.add_argument("--robust", type=float, default=0.5)
    parser.add_argument("--deconvolver", default="hogbom", choices=("hogbom", "clark"))
    parser.add_argument("--gain", type=float, default=0.1)
    parser.add_argument("--threshold-jy", type=float, default=0.0)
    parser.add_argument("--cyclefactor", type=float, default=1.0)
    parser.add_argument("--minpsffraction", type=float, default=0.05)
    parser.add_argument("--maxpsffraction", type=float, default=0.8)
    parser.add_argument("--psfcutoff", type=float, default=0.35)
    parser.add_argument(
        "--hogbom-iteration-mode",
        default="strict",
        choices=("strict", "casa", "casa_inclusive"),
        help="casa-rs Hogbom accounting mode used for the Rust side",
    )
    parser.add_argument(
        "--keep-existing",
        action="store_true",
        help="reuse products whose directories already exist",
    )
    return parser.parse_args()


def run_command(
    argv: list[str],
    *,
    cwd: pathlib.Path,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(
        argv,
        cwd=cwd,
        env=merged_env,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def read_image(path: pathlib.Path) -> np.ndarray:
    ia = image()
    ia.open(str(path))
    try:
        return np.squeeze(np.asarray(ia.getchunk(dropdeg=False, getmask=False), dtype=np.float64))
    finally:
        ia.close()


def image_stats(rust_path: pathlib.Path, casa_path: pathlib.Path) -> dict[str, Any]:
    rust = read_image(rust_path)
    casa = read_image(casa_path)
    diff = rust - casa
    flat_rust = rust.ravel()
    flat_casa = casa.ravel()
    corr = float(np.corrcoef(flat_rust, flat_casa)[0, 1])
    peak_index = np.unravel_index(int(np.nanargmax(np.abs(diff))), diff.shape)
    casa_rms = float(np.sqrt(np.nanmean(casa * casa)))
    diff_rms = float(np.sqrt(np.nanmean(diff * diff)))
    return {
        "shape": list(diff.shape),
        "rms": diff_rms,
        "max_abs": float(np.nanmax(np.abs(diff))),
        "relative_rms": diff_rms / casa_rms if casa_rms else math.nan,
        "correlation": corr,
        "peak_index": [int(value) for value in peak_index],
        "peak_rust": float(rust[peak_index]),
        "peak_casa": float(casa[peak_index]),
        "peak_diff": float(diff[peak_index]),
        "abs_quantiles": {
            str(q): float(np.nanquantile(np.abs(diff), q))
            for q in (0.5, 0.9, 0.99, 0.999, 0.9999)
        },
    }


def top_components(path: pathlib.Path, limit: int = 12) -> list[dict[str, Any]]:
    data = read_image(path)
    flat = np.abs(data).ravel()
    if flat.size == 0:
        return []
    count = min(limit, flat.size)
    indices = np.argpartition(flat, -count)[-count:]
    ordered = sorted(indices, key=lambda index: flat[index], reverse=True)
    result = []
    for index in ordered:
        coord = np.unravel_index(int(index), data.shape)
        value = float(data[coord])
        if value == 0.0:
            continue
        result.append(
            {
                "index": [int(value) for value in coord],
                "value": value,
            }
        )
    return result


def case_controls(case: str) -> dict[str, Any]:
    if case == "dirty":
        return {"niter": 0, "cycleniter": 1, "dirty_only": True}
    if case == "one":
        return {"niter": 1, "cycleniter": 1, "dirty_only": False}
    if case == "first-cycle":
        return {"niter": 50, "cycleniter": 50, "dirty_only": False}
    if case == "full":
        return {"niter": 500, "cycleniter": 50, "dirty_only": False}
    if case == "deep-cycle":
        return {"niter": 2000, "cycleniter": 2000, "dirty_only": False}
    if case == "cycle1479":
        return {"niter": 1479, "cycleniter": 1479, "dirty_only": False}
    raise ValueError(f"unsupported case {case!r}")


def rust_weighting(weighting: str, robust: float) -> dict[str, Any]:
    normalized = weighting.lower()
    if normalized == "natural":
        return {"kind": "natural"}
    if normalized == "uniform":
        return {"kind": "uniform"}
    if normalized == "briggs":
        return {"kind": "briggs", "robust": robust}
    raise ValueError(f"unsupported weighting {weighting!r}")


def run_rust(args: argparse.Namespace, case: str, prefix: pathlib.Path) -> dict[str, Any]:
    controls = case_controls(case)
    request = {
        "measurement_set": str(args.ms),
        "image_name": str(prefix),
        "image_size": args.imsize,
        "cell_arcsec": args.cell_arcsec,
        "field_ids": [int(args.field)],
        "spw_selector": args.spw,
        "channel_start": args.channel_start,
        "channel_count": args.channel_count,
        "data_column": "DATA",
        "spectral_mode": "mfs",
        "weighting": rust_weighting(args.weighting, args.robust),
        "deconvolver": args.deconvolver,
        "nterms": 1,
        "niter": controls["niter"],
        "fullsummary": True,
        "gain": args.gain,
        "threshold_jy": args.threshold_jy,
        "nsigma": 0.0,
        "psf_cutoff": args.psfcutoff,
        "minor_cycle_length": controls["cycleniter"],
        "cyclefactor": args.cyclefactor,
        "min_psf_fraction": args.minpsffraction,
        "max_psf_fraction": args.maxpsffraction,
        "hogbom_iteration_mode": "casa_inclusive"
        if args.hogbom_iteration_mode == "casa"
        else args.hogbom_iteration_mode,
        "dirty_only": controls["dirty_only"],
        "write_preview_pngs": False,
    }
    env = {
        "CASA_RS_STANDARD_MFS_BACKEND": "cpu",
        "CASA_RS_STANDARD_MFS_GRID_THREADS": "1",
        "CASA_RS_STANDARD_MFS_RESIDUAL_BACKEND": "cpu",
        "CASA_RS_STANDARD_MFS_INITIAL_DIRTY_BACKEND": "cpu",
        "CASA_RS_STANDARD_MFS_METAL_GROUPED_INPUT_CACHE": "false",
    }
    if args.deconvolver == "clark":
        env["CASA_RS_CLARK_TRACE"] = str(prefix.parent / f"{prefix.name}.clark-trace.jsonl")
    started = time.perf_counter()
    envelope = {"kind": "run", "request": request}
    (prefix.parent / f"{prefix.name}.request.json").write_text(
        json.dumps(envelope, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    completed = run_command(
        [str(REPO_ROOT / "target" / "release" / "casars-imager"), "--json-run", "-"],
        cwd=REPO_ROOT,
        env=env,
        input_text=json.dumps(envelope),
    )
    elapsed = time.perf_counter() - started
    (prefix.parent / f"{prefix.name}.stdout").write_text(completed.stdout, encoding="utf-8")
    (prefix.parent / f"{prefix.name}.stderr").write_text(completed.stderr, encoding="utf-8")
    if completed.returncode != 0:
        raise RuntimeError(f"casa-rs {case} failed: {completed.stderr}")
    result = json.loads(completed.stdout)
    result["elapsed_seconds"] = elapsed
    return result


def run_casa(args: argparse.Namespace, case: str, prefix: pathlib.Path) -> dict[str, Any]:
    controls = case_controls(case)
    spw_selector = (
        f"{args.spw}:{args.channel_start}"
        if args.channel_count == 1
        else f"{args.spw}:{args.channel_start}~{args.channel_start + args.channel_count - 1}"
    )
    trace_path = prefix.parent / f"{prefix.name}.cpp-clark-trace.jsonl"
    os.environ["SAVE_ALL_RESIMS"] = "true"
    if args.deconvolver == "clark":
        os.environ["CASA_CPP_CLARK_TRACE"] = str(trace_path)
    started = time.perf_counter()
    try:
        result = tclean(
            vis=str(args.ms),
            imagename=str(prefix),
            datacolumn="data",
            field=args.field,
            spw=spw_selector,
            stokes="I",
            specmode="mfs",
            gridder="standard",
            weighting=args.weighting,
            robust=args.robust,
            deconvolver=args.deconvolver,
            nterms=1,
            imsize=args.imsize,
            cell=f"{args.cell_arcsec}arcsec",
            niter=controls["niter"],
            cycleniter=controls["cycleniter"],
            gain=args.gain,
            threshold=f"{args.threshold_jy}Jy",
            nsigma=0.0,
            cyclefactor=args.cyclefactor,
            minpsffraction=args.minpsffraction,
            maxpsffraction=args.maxpsffraction,
            restoration=True,
            calcpsf=True,
            calcres=True,
            restart=True,
            interactive=False,
            parallel=False,
            pbcor=False,
            usemask="user",
            mask="",
            savemodel="none",
            psfcutoff=args.psfcutoff,
            fullsummary=True,
        )
    finally:
        os.environ.pop("SAVE_ALL_RESIMS", None)
        os.environ.pop("CASA_CPP_CLARK_TRACE", None)
    elapsed = time.perf_counter() - started
    return {
        "elapsed_seconds": elapsed,
        "iterdone": int(result.get("iterdone", 0)),
        "nmajordone": int(result.get("nmajordone", 0)),
        "stopcode": int(result.get("stopcode", 0)),
        "summaryminor": result.get("summaryminor"),
        "cpp_clark_trace": str(trace_path) if trace_path.exists() else None,
    }


def compare_case(case_dir: pathlib.Path) -> dict[str, Any]:
    rust_prefix = case_dir / "rust" / "rust"
    casa_prefix = case_dir / "casa" / "casa"
    products = {}
    for suffix in ("psf", "model", "residual", "image"):
        rust_path = pathlib.Path(f"{rust_prefix}.{suffix}")
        casa_path = pathlib.Path(f"{casa_prefix}.{suffix}")
        if rust_path.exists() and casa_path.exists():
            products[suffix] = image_stats(rust_path, casa_path)
    if "image" in products and "residual" in products:
        rust_image = read_image(pathlib.Path(f"{rust_prefix}.image"))
        rust_residual = read_image(pathlib.Path(f"{rust_prefix}.residual"))
        casa_image = read_image(pathlib.Path(f"{casa_prefix}.image"))
        casa_residual = read_image(pathlib.Path(f"{casa_prefix}.residual"))
        restored_component_diff = (rust_image - rust_residual) - (casa_image - casa_residual)
        residual_diff = rust_residual - casa_residual
        products["restored_component_minus_residual_decomposition"] = {
            "restored_component_diff_rms": float(
                np.sqrt(np.nanmean(restored_component_diff * restored_component_diff))
            ),
            "residual_diff_rms": float(np.sqrt(np.nanmean(residual_diff * residual_diff))),
            "correlation": float(
                np.corrcoef(restored_component_diff.ravel(), residual_diff.ravel())[0, 1]
            ),
        }
    return {
        "products": products,
        "top_model_components": {
            "rust": top_components(pathlib.Path(f"{rust_prefix}.model"))
            if pathlib.Path(f"{rust_prefix}.model").exists()
            else [],
            "casa": top_components(pathlib.Path(f"{casa_prefix}.model"))
            if pathlib.Path(f"{casa_prefix}.model").exists()
            else [],
        },
    }


def main() -> None:
    args = parse_args()
    if not args.ms.exists():
        raise SystemExit(f"MeasurementSet not found: {args.ms}")
    cases = [case.strip() for case in args.cases.split(",") if case.strip()]
    perf_paths.mark_safe_to_delete(perf_paths.default_artifact_root())
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "measurement_set": str(args.ms),
        "cases": {},
    }
    build = run_command(
        ["cargo", "build", "--release", "-p", "casars-imager", "--bin", "casars-imager"],
        cwd=REPO_ROOT,
    )
    if build.returncode != 0:
        sys.stderr.write(build.stdout)
        sys.stderr.write(build.stderr)
        raise SystemExit(build.returncode)
    for case in cases:
        case_dir = args.output_dir / case
        rust_dir = case_dir / "rust"
        casa_dir = case_dir / "casa"
        if not args.keep_existing and case_dir.exists():
            shutil.rmtree(case_dir)
        rust_dir.mkdir(parents=True, exist_ok=True)
        casa_dir.mkdir(parents=True, exist_ok=True)
        print(f"running case={case}", flush=True)
        rust_result = run_rust(args, case, rust_dir / "rust")
        casa_result = run_casa(args, case, casa_dir / "casa")
        comparison = compare_case(case_dir)
        manifest["cases"][case] = {
            "rust": {
                "elapsed_seconds": rust_result["elapsed_seconds"],
                "iterdone": rust_result["result"]["run"]["iterdone"],
                "nmajordone": rust_result["result"]["run"]["nmajordone"],
                "stopcode": rust_result["result"]["run"]["stopcode"],
                "summaryminor": rust_result["result"]["run"]["summaryminor"],
            },
            "casa": casa_result,
            "comparison": comparison,
        }
        (args.output_dir / "residual-divergence-summary.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    print(args.output_dir / "residual-divergence-summary.json")


if __name__ == "__main__":
    main()
