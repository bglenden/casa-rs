#!/usr/bin/env python3
"""Recompute one CASA residual from a checksum-pinned common MT-MFS model.

This is a bounded correctness probe, not performance evidence.  It clones an
existing zero-model CASA image bundle, replaces its Taylor-model products with
the exact products emitted by casa-rs, and asks CASA for one residual
calculation without a PSF calculation, minor cycle, or restoration.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
from casatasks import casalog, tclean
from casatools import image

IMAGER_TOOLS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(IMAGER_TOOLS))

from perf_harness.casa_tclean import (  # noqa: E402
    normalize_archived_parameters,
    parse_literal_assignment_recipe,
)


MODEL_SUFFIXES = (".model.tt0", ".model.tt1")
IMMUTABLE_SUFFIXES = (
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
    ".sumwt.tt0",
    ".sumwt.tt1",
    ".sumwt.tt2",
)
MODEL_RELATIVE_L2_CEILING = 5.0e-7
SUMWT_RELATIVE_L2_CEILING = 5.0e-6


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


def tree_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    for entry in sorted(path.rglob("*"), key=lambda item: str(item.relative_to(path))):
        relative = str(entry.relative_to(path)).encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        if entry.is_symlink():
            target = str(entry.readlink()).encode("utf-8")
            digest.update(b"L")
            digest.update(len(target).to_bytes(8, "big"))
            digest.update(target)
        elif entry.is_file():
            digest.update(b"F")
            with entry.open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    digest.update(chunk)
        elif entry.is_dir():
            digest.update(b"D")
    return digest.hexdigest()


def image_content_sha256(path: Path) -> str:
    pixels, mask = image_pixels_and_mask(path)
    digest = hashlib.sha256()
    for array in (pixels, mask):
        shape = np.asarray(array.shape, dtype=np.int64)
        digest.update(array.dtype.str.encode("ascii"))
        digest.update(shape.tobytes())
        digest.update(array.tobytes())
    return digest.hexdigest()


def image_pixels_and_mask(path: Path) -> tuple[np.ndarray, np.ndarray]:
    tool = image()
    try:
        if not tool.open(str(path)):
            raise RuntimeError(f"failed to open CASA image {path}")
        pixels = np.ascontiguousarray(tool.getchunk())
        mask = np.ascontiguousarray(tool.getchunk(getmask=True))
    finally:
        tool.close()
    return pixels, mask


def preservation_comparison(
    source: Path,
    candidate: Path,
    *,
    relative_l2_ceiling: float,
) -> dict[str, Any]:
    expected_pixels, expected_mask = image_pixels_and_mask(source)
    actual_pixels, actual_mask = image_pixels_and_mask(candidate)
    shape_exact = expected_pixels.shape == actual_pixels.shape
    dtype_exact = expected_pixels.dtype == actual_pixels.dtype
    mask_shape_exact = expected_mask.shape == actual_mask.shape
    mask_exact = mask_shape_exact and np.array_equal(expected_mask, actual_mask)
    if shape_exact:
        difference = np.asarray(actual_pixels, dtype=np.float64) - np.asarray(
            expected_pixels,
            dtype=np.float64,
        )
        difference_l2 = float(np.linalg.norm(difference.reshape(-1)))
        reference_l2 = float(
            np.linalg.norm(np.asarray(expected_pixels, dtype=np.float64).reshape(-1))
        )
        relative_l2 = (
            difference_l2 / reference_l2
            if reference_l2 > 0.0
            else (0.0 if difference_l2 == 0.0 else float("inf"))
        )
        max_abs = float(np.max(np.abs(difference), initial=0.0))
    else:
        difference_l2 = float("inf")
        reference_l2 = float("nan")
        relative_l2 = float("inf")
        max_abs = float("inf")
    passed = (
        shape_exact
        and dtype_exact
        and mask_exact
        and np.isfinite(relative_l2)
        and relative_l2 <= relative_l2_ceiling
    )
    return {
        "source": str(source),
        "candidate": str(candidate),
        "shape_exact": shape_exact,
        "dtype_exact": dtype_exact,
        "mask_shape_exact": mask_shape_exact,
        "mask_exact": mask_exact,
        "reference_l2": reference_l2,
        "difference_l2": difference_l2,
        "relative_l2": relative_l2,
        "max_abs": max_abs,
        "relative_l2_ceiling": relative_l2_ceiling,
        "passed": passed,
    }


def protected_product_preservation(
    *,
    zero_prefix: Path,
    model_prefix: Path,
    output_prefix: Path,
) -> dict[str, Any]:
    products: dict[str, Any] = {}
    for suffix in MODEL_SUFFIXES:
        products[suffix] = preservation_comparison(
            Path(f"{model_prefix}{suffix}"),
            Path(f"{output_prefix}{suffix}"),
            relative_l2_ceiling=MODEL_RELATIVE_L2_CEILING,
        )
    for suffix in IMMUTABLE_SUFFIXES:
        products[suffix] = preservation_comparison(
            Path(f"{zero_prefix}{suffix}"),
            Path(f"{output_prefix}{suffix}"),
            relative_l2_ceiling=(
                SUMWT_RELATIVE_L2_CEILING if ".sumwt." in suffix else 0.0
            ),
        )
    return {
        "contract": {
            "shape": "exact",
            "dtype": "exact",
            "mask_topology": "exact",
            "psf_relative_l2_ceiling": 0.0,
            "model_relative_l2_ceiling": MODEL_RELATIVE_L2_CEILING,
            "sumwt_relative_l2_ceiling": SUMWT_RELATIVE_L2_CEILING,
        },
        "products": products,
        "passed": all(product["passed"] for product in products.values()),
    }


def prefixed_directories(prefix: Path) -> list[Path]:
    return sorted(
        (path for path in prefix.parent.glob(f"{prefix.name}.*") if path.is_dir()),
        key=str,
    )


def effective_parameters_from_request(request_path: Path) -> dict[str, Any]:
    request = json.loads(request_path.read_text(encoding="utf-8"))
    if (
        not isinstance(request, dict)
        or request.get("kind") != "casa_tclean_request"
        or request.get("action") != "run"
    ):
        raise RuntimeError("request is not a retained CASA tclean run request")
    recipe = request.get("recipe")
    overrides = request.get("overrides")
    if not isinstance(recipe, dict) or not isinstance(overrides, dict):
        raise RuntimeError("request does not bind its recipe and overrides")
    recipe_path = Path(str(recipe.get("path", ""))).expanduser()
    if not recipe_path.is_file():
        raise RuntimeError(f"retained CASA recipe is missing: {recipe_path}")
    recipe_sha256 = hashlib.sha256(recipe_path.read_bytes()).hexdigest()
    if recipe_sha256 != recipe.get("sha256"):
        raise RuntimeError(
            "retained CASA recipe hash differs: "
            f"{recipe_sha256} != {recipe.get('sha256')}"
        )
    assignments = parse_literal_assignment_recipe(
        recipe_path.read_text(encoding="utf-8"),
        source=str(recipe_path),
    )
    archived = {
        name: value for name, value in assignments.items() if name != "taskname"
    }
    effective, _, _ = normalize_archived_parameters(archived, overrides)
    return effective


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--zero-prefix", required=True, type=Path)
    parser.add_argument("--model-prefix", required=True, type=Path)
    parser.add_argument("--output-prefix", required=True, type=Path)
    parser.add_argument(
        "--request-json",
        required=True,
        type=Path,
        help="retained request.json for the exact CASA row being diagnosed",
    )
    parser.add_argument("--casa-log", required=True, type=Path)
    args = parser.parse_args()
    if args.casa_log.exists():
        raise RuntimeError(f"refusing to overwrite CASA log: {args.casa_log}")

    zero_products = prefixed_directories(args.zero_prefix)
    if not zero_products:
        raise RuntimeError(f"zero-model CASA bundle is missing: {args.zero_prefix}.*")
    for suffix in MODEL_SUFFIXES:
        source = Path(f"{args.model_prefix}{suffix}")
        if not source.is_dir():
            raise RuntimeError(f"common-model product is missing: {source}")
    for suffix in IMMUTABLE_SUFFIXES:
        source = Path(f"{args.zero_prefix}{suffix}")
        if not source.is_dir():
            raise RuntimeError(f"zero-model reference product is missing: {source}")
    if prefixed_directories(args.output_prefix):
        raise RuntimeError(
            f"refusing to overwrite existing products: {args.output_prefix}.*"
        )

    args.output_prefix.parent.mkdir(parents=True, exist_ok=True)
    for source in zero_products:
        suffix = source.name[len(args.zero_prefix.name) :]
        if suffix in MODEL_SUFFIXES:
            continue
        shutil.copytree(source, Path(f"{args.output_prefix}{suffix}"))
    for suffix in MODEL_SUFFIXES:
        shutil.copytree(
            Path(f"{args.model_prefix}{suffix}"),
            Path(f"{args.output_prefix}{suffix}"),
        )

    protected_suffixes = MODEL_SUFFIXES + IMMUTABLE_SUFFIXES
    content_hashes_before = {
        suffix: image_content_sha256(Path(f"{args.output_prefix}{suffix}"))
        for suffix in protected_suffixes
    }
    tree_hashes_before = {
        suffix: tree_sha256(Path(f"{args.output_prefix}{suffix}"))
        for suffix in protected_suffixes
    }
    parameters = effective_parameters_from_request(args.request_json)
    parameters.update(
        {
            "imagename": str(args.output_prefix),
            "niter": 0,
            "cycleniter": 1,
            "nmajor": 0,
            "calcres": True,
            "calcpsf": False,
            "restoration": False,
            "restart": True,
            "savemodel": "none",
            "fullsummary": True,
        }
    )
    encoded = json.dumps(parameters, sort_keys=True, separators=(",", ":")).encode()
    args.casa_log.parent.mkdir(parents=True, exist_ok=True)
    casalog.setlogfile(str(args.casa_log))
    casalog.filter("INFO")
    started = time.monotonic()
    summary = tclean(**parameters)
    elapsed_s = time.monotonic() - started
    content_hashes_after = {
        suffix: image_content_sha256(Path(f"{args.output_prefix}{suffix}"))
        for suffix in protected_suffixes
    }
    tree_hashes_after = {
        suffix: tree_sha256(Path(f"{args.output_prefix}{suffix}"))
        for suffix in protected_suffixes
    }
    changed_protected_content = sorted(
        suffix
        for suffix in protected_suffixes
        if content_hashes_after[suffix] != content_hashes_before[suffix]
    )
    changed_protected_trees = sorted(
        suffix
        for suffix in protected_suffixes
        if tree_hashes_after[suffix] != tree_hashes_before[suffix]
    )
    preservation = protected_product_preservation(
        zero_prefix=args.zero_prefix,
        model_prefix=args.model_prefix,
        output_prefix=args.output_prefix,
    )
    result = {
        "kind": "vlass_common_model_major_cycle",
        "role": "bounded_correctness_trace_not_performance_evidence",
        "casa_version": "6.7.5.18",
        "elapsed_s": elapsed_s,
        "parameters_sha256": hashlib.sha256(encoded).hexdigest(),
        "parameters": parameters,
        "request_json": str(args.request_json.resolve()),
        "request_json_sha256": hashlib.sha256(
            args.request_json.read_bytes()
        ).hexdigest(),
        "zero_prefix": str(args.zero_prefix),
        "model_prefix": str(args.model_prefix),
        "output_prefix": str(args.output_prefix),
        "casa_log": str(args.casa_log.resolve()),
        "casa_log_sha256": hashlib.sha256(args.casa_log.read_bytes()).hexdigest(),
        "protected_content_hashes_before": content_hashes_before,
        "protected_content_hashes_after": content_hashes_after,
        "changed_protected_content": changed_protected_content,
        "protected_tree_hashes_before": tree_hashes_before,
        "protected_tree_hashes_after": tree_hashes_after,
        "changed_protected_trees": changed_protected_trees,
        "protected_product_preservation": preservation,
        "summary": json_value(summary),
        "products": [str(path) for path in prefixed_directories(args.output_prefix)],
    }
    receipt = args.output_prefix.parent / f"{args.output_prefix.name}.receipt.json"
    receipt.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True), flush=True)
    if not preservation["passed"]:
        failed = sorted(
            suffix
            for suffix, product in preservation["products"].items()
            if not product["passed"]
        )
        raise RuntimeError(
            "CASA changed protected common-model/PSF/sumwt products beyond the "
            "numerical-preservation contract: "
            + ", ".join(failed)
        )


if __name__ == "__main__":
    main()
