#!/usr/bin/env python3
"""Run bounded CASA MT-MFS deconvolution from retained image-domain inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import time
from pathlib import Path
from typing import Any

import numpy as np
from casatasks import casalog, deconvolve


PSF_SUFFIXES = (
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
)
RESIDUAL_SUFFIXES = (
    ".residual.tt0",
    ".residual.tt1",
)
MODEL_SUFFIXES = (
    ".model.tt0",
    ".model.tt1",
)
INPUT_SUFFIXES = PSF_SUFFIXES + RESIDUAL_SUFFIXES


def json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-prefix", required=True, type=Path)
    parser.add_argument(
        "--psf-prefix",
        type=Path,
        help="optional PSF source prefix for mixed-input sensitivity probes",
    )
    parser.add_argument(
        "--residual-prefix",
        type=Path,
        help="optional residual source prefix for mixed-input sensitivity probes",
    )
    parser.add_argument(
        "--model-prefix",
        type=Path,
        help="optional existing model prefix for a continued minor-cycle probe",
    )
    parser.add_argument("--output-prefix", required=True, type=Path)
    parser.add_argument(
        "--casa-log",
        type=Path,
        help="optional dedicated CASA log path for the frozen diagnostic",
    )
    parser.add_argument(
        "--mask-source",
        type=Path,
        help="CASA mask image; defaults to INPUT_PREFIX.mask",
    )
    parser.add_argument("--niter", required=True, type=int)
    args = parser.parse_args()
    if args.niter < 1:
        raise SystemExit("--niter must be positive")
    mask_source = args.mask_source or Path(f"{args.input_prefix}.mask")
    psf_prefix = args.psf_prefix or args.input_prefix
    residual_prefix = args.residual_prefix or args.input_prefix
    source_by_suffix = {
        **{suffix: psf_prefix for suffix in PSF_SUFFIXES},
        **{suffix: residual_prefix for suffix in RESIDUAL_SUFFIXES},
    }

    for suffix in INPUT_SUFFIXES:
        source = Path(f"{source_by_suffix[suffix]}{suffix}")
        if not source.is_dir():
            raise RuntimeError(f"required input product is missing: {source}")
    if not mask_source.is_dir():
        raise RuntimeError(f"required mask image is missing: {mask_source}")
    if args.model_prefix is not None:
        for suffix in MODEL_SUFFIXES:
            source = Path(f"{args.model_prefix}{suffix}")
            if not source.is_dir():
                raise RuntimeError(f"required model product is missing: {source}")
    if any(args.output_prefix.parent.glob(f"{args.output_prefix.name}.*")):
        raise RuntimeError(
            f"refusing to overwrite existing products: {args.output_prefix}.*"
        )
    if args.casa_log is not None and args.casa_log.exists():
        raise RuntimeError(f"refusing to overwrite existing CASA log: {args.casa_log}")

    args.output_prefix.parent.mkdir(parents=True, exist_ok=True)
    if args.casa_log is not None:
        args.casa_log.parent.mkdir(parents=True, exist_ok=True)
        casalog.setlogfile(str(args.casa_log))
    for suffix in INPUT_SUFFIXES:
        shutil.copytree(
            Path(f"{source_by_suffix[suffix]}{suffix}"),
            Path(f"{args.output_prefix}{suffix}"),
        )
    if args.model_prefix is not None:
        for suffix in MODEL_SUFFIXES:
            shutil.copytree(
                Path(f"{args.model_prefix}{suffix}"),
                Path(f"{args.output_prefix}{suffix}"),
            )
    shutil.copytree(mask_source, Path(f"{args.output_prefix}.mask"))

    parameters = {
        "imagename": str(args.output_prefix),
        "deconvolver": "mtmfs",
        "scales": [0, 5, 12],
        "nterms": 2,
        "smallscalebias": 0.0,
        "restoration": False,
        "niter": args.niter,
        "gain": 0.1,
        "threshold": 0.0,
        "nsigma": 0.0,
        "interactive": False,
        "fullsummary": True,
        "usemask": "user",
        # The mask has already been cloned to imagename.mask. CASA rejects
        # supplying the same path explicitly because that would ambiguously
        # request both an existing mask and a new input mask.
        "mask": "",
    }
    encoded = json.dumps(parameters, sort_keys=True, separators=(",", ":")).encode()
    started = time.monotonic()
    summary = deconvolve(**parameters)
    elapsed_s = time.monotonic() - started
    products = sorted(
        str(path)
        for path in args.output_prefix.parent.glob(f"{args.output_prefix.name}.*")
        if path.is_dir()
    )
    result = {
        "kind": "vlass_image_domain_mtmfs_component_trace",
        "role": "bounded_correctness_trace_not_performance_evidence",
        "casa_version": "6.7.5.18",
        "elapsed_s": elapsed_s,
        "parameters_sha256": hashlib.sha256(encoded).hexdigest(),
        "parameters": parameters,
        "input_prefix": str(args.input_prefix),
        "psf_prefix": str(psf_prefix),
        "residual_prefix": str(residual_prefix),
        "model_prefix": (
            None if args.model_prefix is None else str(args.model_prefix)
        ),
        "mask_source": str(mask_source),
        "casa_log": None if args.casa_log is None else str(args.casa_log),
        "summary": json_value(summary),
        "products": products,
    }
    receipt = args.output_prefix.parent / f"{args.output_prefix.name}.receipt.json"
    receipt.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
