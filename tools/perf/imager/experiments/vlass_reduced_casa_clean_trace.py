#!/usr/bin/env python3
"""Run a bounded CASA MT-MFS trace for component-parity work."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from typing import Any

import numpy as np
from casatasks import casalog, tclean

from vlass_reduced_casa_clean_4096_four_spw import (
    CACHE,
    MASK,
    MS,
    ROOT,
    TCLEAN_PARAMETERS,
)


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
    parser.add_argument(
        "--niter",
        type=int,
        required=True,
        help="bounded total and per-cycle iteration limit",
    )
    args = parser.parse_args()
    if args.niter < 0:
        raise SystemExit("--niter must be non-negative")
    image = ROOT / f"casa-reduced-clean/4096-four-spw-trace-{args.niter}/casa"
    receipt = (
        ROOT
        / "receipts/runs"
        / f"20260728-vlass-reduced-casa-clean-4096-four-spw-trace-{args.niter}.json"
    )

    for path, label in ((MS, "MeasurementSet"), (MASK, "mask"), (CACHE, "CF cache")):
        if not path.is_dir():
            raise RuntimeError(f"{label} is missing: {path}")
    if any(image.parent.glob(f"{image.name}.*")):
        raise RuntimeError(f"refusing to overwrite existing products: {image}.*")

    parameters = dict(TCLEAN_PARAMETERS)
    parameters.update(
        {
            "imagename": str(image),
            "niter": args.niter,
            "cycleniter": args.niter,
            "fullsummary": True,
        }
    )
    image.parent.mkdir(parents=True, exist_ok=True)
    receipt.parent.mkdir(parents=True, exist_ok=True)
    parameters_json = json.dumps(
        parameters, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    casalog.filter("INFO")
    started = time.monotonic()
    summary = tclean(**parameters)
    elapsed_s = time.monotonic() - started
    products = sorted(
        str(path)
        for path in image.parent.glob(f"{image.name}.*")
        if path.is_dir()
    )
    result = {
        "kind": "vlass_reduced_casa_clean_component_trace",
        "role": "bounded_correctness_trace_not_performance_evidence",
        "casa_version": "6.7.5.18",
        "elapsed_s": elapsed_s,
        "parameters_sha256": hashlib.sha256(parameters_json).hexdigest(),
        "parameters": parameters,
        "summary": json_value(summary),
        "products": products,
        "pid": os.getpid(),
    }
    receipt.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
