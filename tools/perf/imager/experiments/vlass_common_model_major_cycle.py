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
import time
from pathlib import Path
from typing import Any

import numpy as np
from casatasks import casalog, tclean
from casatools import image

from vlass_reduced_casa_clean_4096_four_spw import TCLEAN_PARAMETERS


MODEL_SUFFIXES = (".model.tt0", ".model.tt1")
IMMUTABLE_SUFFIXES = (
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
    ".sumwt.tt0",
    ".sumwt.tt1",
    ".sumwt.tt2",
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
    tool = image()
    try:
        if not tool.open(str(path)):
            raise RuntimeError(f"failed to open CASA image {path}")
        pixels = np.ascontiguousarray(tool.getchunk())
        mask = np.ascontiguousarray(tool.getchunk(getmask=True))
    finally:
        tool.close()
    digest = hashlib.sha256()
    for array in (pixels, mask):
        shape = np.asarray(array.shape, dtype=np.int64)
        digest.update(array.dtype.str.encode("ascii"))
        digest.update(shape.tobytes())
        digest.update(array.tobytes())
    return digest.hexdigest()


def prefixed_directories(prefix: Path) -> list[Path]:
    return sorted(
        (
            path
            for path in prefix.parent.glob(f"{prefix.name}.*")
            if path.is_dir()
        ),
        key=str,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--zero-prefix", required=True, type=Path)
    parser.add_argument("--model-prefix", required=True, type=Path)
    parser.add_argument("--output-prefix", required=True, type=Path)
    args = parser.parse_args()

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
    parameters = dict(TCLEAN_PARAMETERS)
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
    result = {
        "kind": "vlass_common_model_major_cycle",
        "role": "bounded_correctness_trace_not_performance_evidence",
        "casa_version": "6.7.5.18",
        "elapsed_s": elapsed_s,
        "parameters_sha256": hashlib.sha256(encoded).hexdigest(),
        "parameters": parameters,
        "zero_prefix": str(args.zero_prefix),
        "model_prefix": str(args.model_prefix),
        "output_prefix": str(args.output_prefix),
        "protected_content_hashes_before": content_hashes_before,
        "protected_content_hashes_after": content_hashes_after,
        "changed_protected_content": changed_protected_content,
        "protected_tree_hashes_before": tree_hashes_before,
        "protected_tree_hashes_after": tree_hashes_after,
        "changed_protected_trees": changed_protected_trees,
        "summary": json_value(summary),
        "products": [str(path) for path in prefixed_directories(args.output_prefix)],
    }
    receipt = args.output_prefix.parent / f"{args.output_prefix.name}.receipt.json"
    receipt.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True), flush=True)
    if changed_protected_content:
        raise RuntimeError(
            "CASA changed protected common-model/PSF/sumwt image content: "
            + ", ".join(changed_protected_content)
        )


if __name__ == "__main__":
    main()
