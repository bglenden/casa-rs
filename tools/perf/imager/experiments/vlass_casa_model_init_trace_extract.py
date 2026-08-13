#!/usr/bin/env python3
"""Extract and receipt a bounded CASA VLASS model-initialization A-D trace.

The CASA source patch writes four native CASA images:

* A: Float model after SIImageStoreMultiTerm weight normalization;
* B: Complex forward grid after Stokes-to-correlation conversion;
* C: Complex AWProject grid immediately before LatticeFFT::cfft2d; and
* D: Complex AWProject grid immediately after LatticeFFT::cfft2d.

This reader selects one polarization/channel plane and emits canonical raw
little-endian payloads compatible with ``vlass_complex_grid_compare.py``.
The logical array shape is ``(x, y)`` and y is contiguous:
``component_offset = ((x * ny) + y) * components + component``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
from pathlib import Path
from typing import Any

import numpy as np


STAGES = {
    "A": {"dtype": "<f4", "components": 1},
    "B": {"dtype": "<f4", "components": 2},
    "C": {"dtype": "<f4", "components": 2},
    "D": {"dtype": "<f4", "components": 2},
}

REQUIRED_A_METADATA = {
    "schema_version",
    "term",
    "normtype",
    "pblimit_f32_bits",
    "pb_scale_factor_f64_bits",
}

REQUIRED_CD_METADATA = {
    "schema_version",
    "role",
    "term",
    "shape",
    "vb_polarization_frame",
    "sensitivity_pattern_qualifier",
    "sensitivity_pattern_qualifier_string",
    "do_pb_correction",
    "aw_avg_pb_ready",
    "cf_cache_present",
    "cf_cache_avg_pb_ready_default",
    "cf_cache_avg_pb_ready_qualified",
    "avg_pb_branch_ran",
    "pb_correction_ran",
    "conv_sampling",
    "pb_limit_f32_bits",
    "avg_pb_min_f32_bits",
    "avg_pb_max_f32_bits",
    "applied_pb_min_f32_bits",
    "applied_pb_max_f32_bits",
    "host_cpus",
    "host_cpus_aipsrc",
    "fft_call",
    "fft_forward",
}


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def parse_metadata(path: Path, required: set[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line:
            continue
        if "=" not in line:
            raise RuntimeError(f"{path}:{line_number}: metadata line lacks '='")
        key, value = line.split("=", 1)
        if not key or key in values:
            raise RuntimeError(f"{path}:{line_number}: invalid or duplicate key {key!r}")
        values[key] = value
    missing = sorted(required - values.keys())
    if missing:
        raise RuntimeError(f"{path} is missing metadata keys: {', '.join(missing)}")
    return values


def canonical_float_payload(plane: np.ndarray) -> bytes:
    values = np.ascontiguousarray(plane, dtype="<f4")
    if values.ndim != 2:
        raise RuntimeError(f"expected a two-dimensional Float plane, got {values.shape}")
    return values.tobytes(order="C")


def canonical_complex_payload(plane: np.ndarray) -> bytes:
    values = np.asarray(plane, dtype="<c8")
    if values.ndim != 2:
        raise RuntimeError(f"expected a two-dimensional Complex plane, got {values.shape}")
    components = np.empty(values.shape + (2,), dtype="<f4")
    components[..., 0] = values.real
    components[..., 1] = values.imag
    return np.ascontiguousarray(components).tobytes(order="C")


def selected_plane(
    pixels: np.ndarray,
    *,
    polarization: int,
    channel: int,
) -> np.ndarray:
    if pixels.ndim != 4:
        raise RuntimeError(f"expected CASA axes [x,y,pol,chan], got {pixels.shape}")
    if not 0 <= polarization < pixels.shape[2]:
        raise RuntimeError(
            f"polarization {polarization} is outside image shape {pixels.shape}"
        )
    if not 0 <= channel < pixels.shape[3]:
        raise RuntimeError(f"channel {channel} is outside image shape {pixels.shape}")
    return np.asarray(pixels[:, :, polarization, channel])


def read_casa_image(path: Path) -> tuple[np.ndarray, np.ndarray, list[int]]:
    try:
        from casatools import image
    except ImportError as error:
        raise RuntimeError(
            "casatools is required; run this extractor with the CASA Python environment"
        ) from error

    tool = image()
    try:
        if not tool.open(str(path)):
            raise RuntimeError(f"failed to open CASA image {path}")
        shape = [int(value) for value in tool.shape()]
        pixels = np.asarray(tool.getchunk())
        mask = np.asarray(tool.getchunk(getmask=True), dtype=np.bool_)
    finally:
        tool.close()
    if list(pixels.shape) != shape:
        raise RuntimeError(
            f"CASA image {path} reported shape {shape}, returned {pixels.shape}"
        )
    return pixels, mask, shape


def image_content_sha256(pixels: np.ndarray, mask: np.ndarray) -> str:
    digest = hashlib.sha256()
    for values in (np.ascontiguousarray(pixels), np.ascontiguousarray(mask)):
        digest.update(values.dtype.str.encode("ascii"))
        digest.update(np.asarray(values.shape, dtype=np.int64).tobytes())
        digest.update(values.tobytes())
    return digest.hexdigest()


def source_image_receipt(path: Path) -> dict[str, Any]:
    pixels, mask, shape = read_casa_image(path)
    return {
        "path": str(path),
        "shape": shape,
        "pixel_dtype": pixels.dtype.str,
        "mask_dtype": mask.dtype.str,
        "mask_true": int(np.count_nonzero(mask)),
        "content_sha256": image_content_sha256(pixels, mask),
    }


def stage_image_path(trace_prefix: Path, stage: str, term: int) -> Path:
    if stage in {"A", "B"}:
        return Path(f"{trace_prefix}.{stage}.tt{term}")
    return Path(f"{trace_prefix}.tt{term}.{stage}")


def stage_raw_path(raw_prefix: Path, stage: str, term: int, pol: int, chan: int) -> Path:
    suffix = "f32" if stage == "A" else "c32"
    return Path(f"{raw_prefix}.{stage}.tt{term}.p{pol}.c{chan}.{suffix}")


def write_exclusive(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(payload)


def f32_bits(value: float) -> int:
    return struct.unpack("<I", struct.pack("<f", value))[0]


def extract_stage(
    *,
    trace_prefix: Path,
    raw_prefix: Path,
    stage: str,
    term: int,
    polarization: int,
    channel: int,
) -> dict[str, Any]:
    image_path = stage_image_path(trace_prefix, stage, term)
    pixels, mask, image_shape = read_casa_image(image_path)
    plane = selected_plane(pixels, polarization=polarization, channel=channel)
    plane_mask = selected_plane(mask, polarization=polarization, channel=channel)
    payload = (
        canonical_float_payload(plane)
        if stage == "A"
        else canonical_complex_payload(plane)
    )
    raw_path = stage_raw_path(
        raw_prefix, stage, term, polarization, channel
    )
    write_exclusive(raw_path, payload)
    expected_bytes = (
        plane.shape[0]
        * plane.shape[1]
        * STAGES[stage]["components"]
        * np.dtype(STAGES[stage]["dtype"]).itemsize
    )
    if len(payload) != expected_bytes:
        raise RuntimeError(
            f"stage {stage} serialized {len(payload)} bytes, expected {expected_bytes}"
        )
    return {
        "stage": stage,
        "source_casa_image": str(image_path),
        "source_image_shape": image_shape,
        "source_image_content_sha256": image_content_sha256(pixels, mask),
        "selected_plane": {
            "polarization": polarization,
            "channel": channel,
            "shape": [int(value) for value in plane.shape],
            "mask_true": int(np.count_nonzero(plane_mask)),
        },
        "raw_path": str(raw_path),
        "raw_bytes": len(payload),
        "raw_sha256": hashlib.sha256(payload).hexdigest(),
        "raw_scalar_dtype": "ieee754-f32-little-endian",
        "raw_components": (
            ["value"] if stage == "A" else ["real", "imag"]
        ),
        "raw_layout": {
            "logical_axes": ["x", "y"],
            "contiguous_axis": "y",
            "component_offset": "((x * ny) + y) * components + component",
        },
    }


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"{path} does not contain a JSON object")
    return value


def casa_version_receipt() -> dict[str, Any]:
    try:
        import casatools
    except ImportError as error:
        raise RuntimeError(
            "casatools is required; run this extractor with the CASA Python environment"
        ) from error
    return {
        "version": [int(value) for value in casatools.version()],
        "version_string": str(casatools.version_string()),
    }


def run_self_test() -> None:
    float_plane = np.asarray(
        [[1.0, -2.5], [np.float32(0.0), np.float32(np.inf)]], dtype=np.float32
    )
    float_payload = canonical_float_payload(float_plane)
    assert np.frombuffer(float_payload, dtype="<f4").view("<u4").tolist() == [
        f32_bits(1.0),
        f32_bits(-2.5),
        f32_bits(0.0),
        f32_bits(np.inf),
    ]

    complex_plane = np.asarray(
        [[1.0 + 2.0j, -3.0 + 4.0j], [5.0 - 6.0j, -7.0 - 8.0j]],
        dtype=np.complex64,
    )
    components = np.frombuffer(canonical_complex_payload(complex_plane), dtype="<f4")
    assert components.tolist() == [1.0, 2.0, -3.0, 4.0, 5.0, -6.0, -7.0, -8.0]

    # Use a non-square plane so an accidental transpose or x-contiguous
    # serialization cannot pass. This is the same order used by the Rust
    # dump_mosaic_complex_grid loop: x is outer, y is inner/contiguous.
    orientation_plane = np.asarray(
        [
            [10.0 + 110.0j, 11.0 + 111.0j, 12.0 + 112.0j],
            [20.0 + 120.0j, 21.0 + 121.0j, 22.0 + 122.0j],
        ],
        dtype=np.complex64,
    )
    orientation_components = np.frombuffer(
        canonical_complex_payload(orientation_plane), dtype="<f4"
    )
    assert orientation_components.tolist() == [
        10.0,
        110.0,
        11.0,
        111.0,
        12.0,
        112.0,
        20.0,
        120.0,
        21.0,
        121.0,
        22.0,
        122.0,
    ]
    transposed_components = np.frombuffer(
        canonical_complex_payload(orientation_plane.T), dtype="<f4"
    )
    assert not np.array_equal(orientation_components, transposed_components)

    cube = np.arange(2 * 3 * 2, dtype=np.float32).reshape((2, 3, 2, 1))
    assert selected_plane(cube, polarization=1, channel=0).tolist() == [
        [1.0, 3.0, 5.0],
        [7.0, 9.0, 11.0],
    ]
    assert stage_image_path(Path("/trace"), "A", 0) == Path("/trace.A.tt0")
    assert stage_image_path(Path("/trace"), "D", 1) == Path("/trace.tt1.D")
    print("vlass_casa_model_init_trace_extract self-test: PASS", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace-prefix", required=False, type=Path)
    parser.add_argument("--raw-prefix", required=False, type=Path)
    parser.add_argument("--prediction-receipt", required=False, type=Path)
    parser.add_argument("--casa-log", required=False, type=Path)
    parser.add_argument("--patch-file", required=False, type=Path)
    parser.add_argument("--casatools-git-sha")
    parser.add_argument("--output", required=False, type=Path)
    parser.add_argument("--term", type=int, default=0)
    parser.add_argument("--polarization", type=int, default=0)
    parser.add_argument("--channel", type=int, default=0)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        run_self_test()
        return

    required_args = {
        "--trace-prefix": args.trace_prefix,
        "--raw-prefix": args.raw_prefix,
        "--prediction-receipt": args.prediction_receipt,
        "--casa-log": args.casa_log,
        "--patch-file": args.patch_file,
        "--casatools-git-sha": args.casatools_git_sha,
        "--output": args.output,
    }
    missing = [name for name, value in required_args.items() if value is None]
    if missing:
        parser.error("required outside --self-test: " + ", ".join(missing))
    if args.term < 0 or args.polarization < 0 or args.channel < 0:
        parser.error("term, polarization, and channel must be non-negative")

    assert args.trace_prefix is not None
    assert args.raw_prefix is not None
    assert args.prediction_receipt is not None
    assert args.casa_log is not None
    assert args.patch_file is not None
    assert args.casatools_git_sha is not None
    assert args.output is not None
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite receipt: {args.output}")

    a_meta_path = Path(f"{args.trace_prefix}.A.tt{args.term}.meta")
    cd_meta_path = Path(f"{args.trace_prefix}.tt{args.term}.CD.meta")
    a_metadata = parse_metadata(a_meta_path, REQUIRED_A_METADATA)
    cd_metadata = parse_metadata(cd_meta_path, REQUIRED_CD_METADATA)
    if int(a_metadata["term"]) != args.term or int(cd_metadata["term"]) != args.term:
        raise RuntimeError("metadata Taylor term does not match --term")

    stages = [
        extract_stage(
            trace_prefix=args.trace_prefix,
            raw_prefix=args.raw_prefix,
            stage=stage,
            term=args.term,
            polarization=args.polarization,
            channel=args.channel,
        )
        for stage in STAGES
    ]
    expected_shape = [
        int(value) for value in cd_metadata["shape"].split(",")
    ]
    if len(expected_shape) != 4:
        raise RuntimeError(
            f"C/D metadata shape must have four axes, got {cd_metadata['shape']!r}"
        )
    for stage in stages:
        if stage["source_image_shape"] != expected_shape:
            raise RuntimeError(
                f"stage {stage['stage']} shape {stage['source_image_shape']} "
                f"does not match C/D metadata shape {expected_shape}"
            )

    prediction_receipt = load_json(args.prediction_receipt)
    if prediction_receipt.get("kind") != "vlass_casa_awproject_prediction_trace":
        raise RuntimeError(
            "prediction receipt kind is not vlass_casa_awproject_prediction_trace"
        )
    prediction_output_text = prediction_receipt.get("output_prefix")
    if not isinstance(prediction_output_text, str) or not prediction_output_text:
        raise RuntimeError("prediction receipt is missing a string output_prefix")
    prediction_output_prefix = Path(prediction_output_text)
    prediction_inputs = {
        "model": source_image_receipt(
            Path(f"{prediction_output_prefix}.model.tt{args.term}")
        ),
        "weight": source_image_receipt(
            Path(f"{prediction_output_prefix}.weight.tt0")
        ),
        "sumwt": source_image_receipt(
            Path(f"{prediction_output_prefix}.sumwt.tt0")
        ),
    }
    weight_pixels, weight_mask, _ = read_casa_image(
        Path(f"{prediction_output_prefix}.weight.tt0")
    )
    if not np.any(weight_mask):
        raise RuntimeError("prediction weight image has no valid pixels")
    weight_peak = np.max(np.asarray(weight_pixels[weight_mask], dtype=np.float32))
    expected_pb_scale = np.sqrt(np.float32(weight_peak), dtype=np.float32)
    expected_pb_scale_f64_bits = struct.unpack(
        "<Q", struct.pack("<d", float(expected_pb_scale))
    )[0]
    if int(a_metadata["pb_scale_factor_f64_bits"]) != expected_pb_scale_f64_bits:
        raise RuntimeError(
            "stage-A PB scale does not match Float sqrt(max(weight.tt0))"
        )

    result = {
        "kind": "vlass_casa_aw_model_initialization_trace",
        "role": "isolated_arithmetic_diagnostic_not_promotion_evidence",
        "schema_version": 1,
        "casa": casa_version_receipt(),
        "casatools_git_sha": args.casatools_git_sha,
        "source_patch": {
            "path": str(args.patch_file),
            "sha256": sha256_path(args.patch_file),
        },
        "prediction_run": {
            "receipt_path": str(args.prediction_receipt),
            "receipt_sha256": sha256_path(args.prediction_receipt),
            "receipt": prediction_receipt,
            "casa_log_path": str(args.casa_log),
            "casa_log_sha256": sha256_path(args.casa_log),
            "normalization_inputs": prediction_inputs,
            "weight_peak_f32_bits": f32_bits(float(weight_peak)),
            "expected_pb_scale_factor_f64_bits": expected_pb_scale_f64_bits,
        },
        "trace_prefix": str(args.trace_prefix),
        "selection": {
            "term": args.term,
            "polarization": args.polarization,
            "channel": args.channel,
        },
        "a_metadata": a_metadata,
        "cd_metadata": cd_metadata,
        "metadata_files": {
            "a": {
                "path": str(a_meta_path),
                "sha256": sha256_path(a_meta_path),
            },
            "cd": {
                "path": str(cd_meta_path),
                "sha256": sha256_path(cd_meta_path),
            },
        },
        "stages": stages,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, indent=2, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
