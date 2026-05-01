#!/usr/bin/env python3
"""Compare Wave 5 simulation MeasurementSets against a CASA reference.

Run this with the CASA Python environment so `casatools` is available:

    /path/to/casa-python scripts/wave5-simulation-parity.py RUST.ms CASA.ms OUTDIR
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from casatools import table


def main() -> int:
    if len(sys.argv) != 4:
        print(
            "usage: wave5-simulation-parity.py RUST_MS CASA_MS OUTDIR",
            file=sys.stderr,
        )
        return 2
    rust_ms = Path(sys.argv[1])
    casa_ms = Path(sys.argv[2])
    outdir = Path(sys.argv[3])
    outdir.mkdir(parents=True, exist_ok=True)

    rust = read_main(rust_ms)
    casa = read_main(casa_ms)
    report = build_report(rust, casa)
    report["rust_ms"] = str(rust_ms)
    report["casa_ms"] = str(casa_ms)

    (outdir / "wave5-simulation-parity.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_plots(rust, casa, outdir)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def read_main(path: Path) -> dict[str, Any]:
    tb = table()
    tb.open(str(path))
    try:
        return {
            "nrows": tb.nrows(),
            "TIME": tb.getcol("TIME"),
            "ANTENNA1": tb.getcol("ANTENNA1"),
            "ANTENNA2": tb.getcol("ANTENNA2"),
            "UVW": tb.getcol("UVW"),
            "DATA": tb.getcol("DATA"),
            "WEIGHT": tb.getcol("WEIGHT"),
            "SIGMA": tb.getcol("SIGMA"),
            "FLAG": tb.getcol("FLAG"),
        }
    finally:
        tb.close()


def build_report(rust: dict[str, Any], casa: dict[str, Any]) -> dict[str, Any]:
    report: dict[str, Any] = {
        "rows": {"rust": int(rust["nrows"]), "casa": int(casa["nrows"])},
        "columns": {},
    }
    for name in ["TIME", "ANTENNA1", "ANTENNA2", "UVW", "DATA", "WEIGHT", "SIGMA", "FLAG"]:
        report["columns"][name] = compare_array(rust[name], casa[name])
    if rust["DATA"].shape == casa["DATA"].shape:
        rust_amp = np.abs(rust["DATA"])
        casa_amp = np.abs(casa["DATA"])
        report["derived"] = {
            "amplitude": compare_array(rust_amp, casa_amp),
            "uv_distance_m": compare_array(
                np.sqrt(rust["UVW"][0] ** 2 + rust["UVW"][1] ** 2),
                np.sqrt(casa["UVW"][0] ** 2 + casa["UVW"][1] ** 2),
            ),
        }
    return report


def compare_array(rust: np.ndarray, casa: np.ndarray) -> dict[str, Any]:
    result: dict[str, Any] = {
        "rust_shape": list(rust.shape),
        "casa_shape": list(casa.shape),
        "same_shape": rust.shape == casa.shape,
    }
    if rust.shape != casa.shape:
        return result
    if rust.dtype == np.bool_:
        result["mismatch_count"] = int(np.count_nonzero(rust != casa))
        return result
    diff = np.asarray(rust) - np.asarray(casa)
    mag = np.abs(diff)
    result.update(
        {
            "max_abs_diff": finite_float(np.max(mag)) if mag.size else 0.0,
            "mean_abs_diff": finite_float(np.mean(mag)) if mag.size else 0.0,
            "p95_abs_diff": finite_float(np.percentile(mag, 95)) if mag.size else 0.0,
            "p99_9_abs_diff": finite_float(np.percentile(mag, 99.9)) if mag.size else 0.0,
            "count_abs_diff_gt_1e_6": int(np.count_nonzero(mag > 1.0e-6)),
            "allclose_1e_6": bool(np.allclose(rust, casa, rtol=1e-6, atol=1e-6)),
        }
    )
    return result


def finite_float(value: Any) -> float | str:
    value = float(value)
    if math.isfinite(value):
        return value
    return str(value)


def write_plots(rust: dict[str, Any], casa: dict[str, Any], outdir: Path) -> None:
    rust_uvdist = np.sqrt(rust["UVW"][0] ** 2 + rust["UVW"][1] ** 2)
    casa_uvdist = np.sqrt(casa["UVW"][0] ** 2 + casa["UVW"][1] ** 2)
    rust_amp = np.abs(rust["DATA"][0, 0, :])
    casa_amp = np.abs(casa["DATA"][0, 0, :])

    fig, axes = plt.subplots(2, 2, figsize=(13, 9), constrained_layout=True)
    axes[0, 0].scatter(casa_uvdist, casa_amp, s=5, alpha=0.55, label="CASA C++")
    axes[0, 0].scatter(rust_uvdist, rust_amp, s=5, alpha=0.55, label="casa-rs")
    axes[0, 0].set_title("Amplitude vs uv distance")
    axes[0, 0].set_xlabel("uv distance (m)")
    axes[0, 0].set_ylabel("amplitude (Jy)")
    axes[0, 0].legend()

    axes[0, 1].plot(np.abs(rust["DATA"][0, 0, :] - casa["DATA"][0, 0, :]), linewidth=0.8)
    axes[0, 1].set_title("Complex DATA absolute difference")
    axes[0, 1].set_xlabel("row")
    axes[0, 1].set_ylabel("abs diff (Jy)")

    axes[1, 0].plot((rust["UVW"] - casa["UVW"]).T, linewidth=0.7)
    axes[1, 0].set_title("UVW component differences")
    axes[1, 0].set_xlabel("row")
    axes[1, 0].set_ylabel("delta (m)")
    axes[1, 0].legend(["u", "v", "w"])

    axes[1, 1].scatter(casa_amp, rust_amp, s=5, alpha=0.6)
    max_amp = max(float(np.max(casa_amp)), float(np.max(rust_amp)))
    axes[1, 1].plot([0.0, max_amp], [0.0, max_amp], color="black", linewidth=0.8)
    axes[1, 1].set_title("Amplitude parity")
    axes[1, 1].set_xlabel("CASA C++ amplitude (Jy)")
    axes[1, 1].set_ylabel("casa-rs amplitude (Jy)")

    fig.savefig(outdir / "wave5-simulation-parity.png", dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    raise SystemExit(main())
