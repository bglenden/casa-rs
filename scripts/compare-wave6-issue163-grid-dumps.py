#!/usr/bin/env python3
"""Compare CASA C++ and casa-rs pre-FFT mosaic grid dumps for issue #163.

Both dump formats are intentionally simple binary files:

* CASA C++: complex128 values ordered channel, polarization, x, y.
* casa-rs: complex128 values ordered x, y, one file per role/frequency.

The script compares every CASA call/channel against every Rust role/channel so
the residual/PSF call ordering can be inferred from the smallest deltas.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def load_casa_grid(path: Path, meta: dict) -> np.ndarray:
    shape = meta["shape"]
    if len(shape) != 4:
        raise ValueError(f"{path}: expected CASA shape [nx, ny, npol, nchan], got {shape}")
    nx, ny, npol, nchan = (int(value) for value in shape)
    raw = np.fromfile(path, dtype="<f8")
    expected = nchan * npol * nx * ny * 2
    if raw.size != expected:
        raise ValueError(f"{path}: expected {expected} f64 values, got {raw.size}")
    complex_values = raw[0::2] + 1j * raw[1::2]
    return complex_values.reshape((nchan, npol, nx, ny))


def load_rust_grid(path: Path, meta: dict) -> np.ndarray:
    shape = meta["shape"]
    if len(shape) != 2:
        raise ValueError(f"{path}: expected Rust shape [nx, ny], got {shape}")
    nx, ny = (int(value) for value in shape)
    raw = np.fromfile(path, dtype="<f8")
    expected = nx * ny * 2
    if raw.size != expected:
        raise ValueError(f"{path}: expected {expected} f64 values, got {raw.size}")
    complex_values = raw[0::2] + 1j * raw[1::2]
    return complex_values.reshape((nx, ny))


def finite_metrics(casa: np.ndarray, rust: np.ndarray) -> dict:
    if casa.shape != rust.shape:
        raise ValueError(f"shape mismatch {casa.shape} versus {rust.shape}")
    mask = np.isfinite(casa.real) & np.isfinite(casa.imag) & np.isfinite(rust.real) & np.isfinite(rust.imag)
    if not np.any(mask):
        return {"finite_pixels": 0}
    c = casa[mask]
    r = rust[mask]
    diff = r - c
    abs_casa = np.abs(c)
    abs_diff = np.abs(diff)
    peak = float(np.max(abs_casa)) if abs_casa.size else 0.0
    denom = peak if peak > 0.0 else 1.0
    dot = np.vdot(c, r)
    norm = np.sqrt(np.vdot(c, c).real * np.vdot(r, r).real)
    corr = dot / norm if norm > 0.0 else 0.0 + 0.0j
    active = abs_casa > (np.max(abs_casa) * 1.0e-12 if peak > 0.0 else 0.0)
    active_diff = abs_diff[active]
    flat_indices = np.argpartition(abs_diff, -10)[-10:]
    flat_indices = flat_indices[np.argsort(abs_diff[flat_indices])[::-1]]
    top_cells = []
    width = casa.shape[1]
    for flat in flat_indices:
        x = int(flat // width)
        y = int(flat % width)
        top_cells.append(
            {
                "x": x,
                "y": y,
                "casa": [float(c[x * width + y].real), float(c[x * width + y].imag)],
                "rust": [float(r[x * width + y].real), float(r[x * width + y].imag)],
                "abs_diff": float(abs_diff[flat]),
                "frac_casa_peak": float(abs_diff[flat] / denom),
            }
        )
    metrics = {
        "finite_pixels": int(mask.sum()),
        "active_pixels": int(active.sum()),
        "casa_peak_abs": peak,
        "rust_peak_abs": float(np.max(np.abs(r))) if r.size else 0.0,
        "max_abs": float(np.max(abs_diff)),
        "p99_abs": float(np.percentile(abs_diff, 99.0)),
        "rms_abs": float(np.sqrt(np.mean(abs_diff * abs_diff))),
        "max_frac_casa_peak": float(np.max(abs_diff) / denom),
        "p99_frac_casa_peak": float(np.percentile(abs_diff, 99.0) / denom),
        "rms_frac_casa_peak": float(np.sqrt(np.mean(abs_diff * abs_diff)) / denom),
        "mean_complex_diff": [float(diff.real.mean()), float(diff.imag.mean())],
        "complex_correlation": [float(np.real(corr)), float(np.imag(corr))],
        "top_cells": top_cells,
    }
    if active_diff.size:
        metrics.update(
            {
                "active_max_frac_casa_peak": float(np.max(active_diff) / denom),
                "active_p99_frac_casa_peak": float(np.percentile(active_diff, 99.0) / denom),
                "active_rms_frac_casa_peak": float(
                    np.sqrt(np.mean(active_diff * active_diff)) / denom
                ),
            }
        )
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifact_dir", type=Path)
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--output-md", type=Path)
    args = parser.parse_args()

    artifact_dir = args.artifact_dir
    casa_grid_dir = artifact_dir / "casa-grid"
    rust_grid_dir = artifact_dir / "rust-grid"
    output_json = args.output_json or artifact_dir / "grid-dump-comparison.json"
    output_md = args.output_md or artifact_dir / "grid-dump-comparison.md"

    casa_entries = []
    for meta_path in sorted(casa_grid_dir.glob("casa-grid-call*-prefft.json")):
        meta = load_json(meta_path)
        data = load_casa_grid(meta_path.with_suffix(".bin"), meta)
        casa_entries.append({"path": str(meta_path), "meta": meta, "data": data})

    rust_entries = []
    for meta_path in sorted(rust_grid_dir.glob("rust-*.json")):
        meta = load_json(meta_path)
        data = load_rust_grid(meta_path.with_suffix(".bin"), meta)
        rust_entries.append({"path": str(meta_path), "meta": meta, "data": data})

    comparisons = []
    for casa_entry in casa_entries:
        casa_data = casa_entry["data"]
        nchan = casa_data.shape[0]
        for chan in range(nchan):
            casa_plane = casa_data[chan, 0, :, :]
            for rust_entry in rust_entries:
                metrics = finite_metrics(casa_plane, rust_entry["data"])
                comparisons.append(
                    {
                        "casa_call_index": casa_entry["meta"].get("call_index"),
                        "casa_channel_index": chan,
                        "casa_sum_weight": casa_entry["meta"].get("sum_weight", [[None]])[0][chan],
                        "rust_role": rust_entry["meta"].get("role"),
                        "rust_frequency_hz": rust_entry["meta"].get("frequency_hz"),
                        "metrics": metrics,
                    }
                )

    comparisons.sort(
        key=lambda item: (
            item["casa_call_index"],
            item["casa_channel_index"],
            item["metrics"].get("p99_frac_casa_peak", float("inf")),
        )
    )
    report = {
        "artifact_dir": str(artifact_dir),
        "casa_entries": [
            {"path": entry["path"], "meta": entry["meta"]} for entry in casa_entries
        ],
        "rust_entries": [
            {"path": entry["path"], "meta": entry["meta"]} for entry in rust_entries
        ],
        "comparisons": comparisons,
    }
    output_json.write_text(json.dumps(report, indent=2, sort_keys=True))

    lines = [
        "# Wave 6 Issue 163 Grid Dump Comparison",
        "",
        f"Artifact directory: `{artifact_dir}`",
        "",
        "| CASA call | CASA chan | Rust role | Rust freq Hz | CASA sumwt | p99 frac peak | max frac peak | rms frac peak | corr real | corr imag |",
        "|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in comparisons:
        metrics = item["metrics"]
        corr = metrics.get("complex_correlation", [None, None])
        lines.append(
            "| {call} | {chan} | `{role}` | {freq:.6g} | {sumwt:.9g} | {p99:.6g} | {maxf:.6g} | {rms:.6g} | {corr_re:.9g} | {corr_im:.9g} |".format(
                call=item["casa_call_index"],
                chan=item["casa_channel_index"],
                role=item["rust_role"],
                freq=float(item["rust_frequency_hz"]),
                sumwt=float(item["casa_sum_weight"]),
                p99=metrics.get("p99_frac_casa_peak", float("nan")),
                maxf=metrics.get("max_frac_casa_peak", float("nan")),
                rms=metrics.get("rms_frac_casa_peak", float("nan")),
                corr_re=float(corr[0]) if corr[0] is not None else float("nan"),
                corr_im=float(corr[1]) if corr[1] is not None else float("nan"),
            )
        )
    output_md.write_text("\n".join(lines) + "\n")
    print(output_json)
    print(output_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
