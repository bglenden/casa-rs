#!/usr/bin/env python3
"""Stage an ImPerformance Wave 3 AW/wide-field synthetic benchmark dataset."""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import struct
import sys
import time
from typing import Any

from perf_harness import atomic_write_json
from perf_harness.subprocesses import run_command

try:
    import numpy as np
except ImportError as exc:  # pragma: no cover - local tooling requires numpy.
    raise SystemExit("numpy is required to stage the Wave 3 AW dataset") from exc


TOOL_DIR = pathlib.Path(__file__).resolve().parent
REPO_ROOT = TOOL_DIR.parents[2]
DEFAULT_DATA_ROOT = pathlib.Path("/Volumes/GLENDENNING/casa-rs-imperformance")
DEFAULT_BINARY = REPO_ROOT / "target" / "release" / "simobserve"
DATASET_ID = "wave3-vla-aw-widefield-medium"

VLA_LAT_DEG = 34.07875
PHASE_CENTER_RA_DEG = 180.0
PHASE_CENTER_DEC_DEG = VLA_LAT_DEG
START_FREQUENCY_HZ = 1.5e9
CHANNEL_WIDTH_HZ = 1.0e6
CHANNEL_COUNT = 512
MODEL_PIXELS = 1024
MODEL_CELL_ARCSEC = 3.2
IMAGE_PIXELS = 4096
IMAGE_CELL_ARCSEC = 0.8
DURATION_SECONDS = 120_000.0
INTEGRATION_SECONDS = 10.0
NOISE_SIMPLENOISE_JY = 0.001
VLA_DIAMETER_M = 25.0
VLA_B_MAX_BASELINE_M = 11_000.0


class StageError(Exception):
    """User-facing staging error."""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=pathlib.Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--binary", type=pathlib.Path, default=DEFAULT_BINARY)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--materialize", action="store_true")
    parser.add_argument("--run-native", action="store_true")
    args = parser.parse_args()

    try:
        dataset_dir = args.data_root / "wave3" / "vla" / "aw-widefield" / "medium"
        paths = dataset_paths(dataset_dir)
        manifest = build_manifest(paths)
        if args.dry_run:
            print(json.dumps(manifest, indent=2, sort_keys=True))
            return
        if args.materialize or args.run_native:
            materialize_inputs(paths, manifest)
        if args.run_native:
            run_native_simobserve(args.binary, paths)
            manifest["actual"] = collect_actual(paths)
            atomic_write_json(paths["manifest"], manifest)
        else:
            atomic_write_json(paths["manifest"], manifest)
        print(paths["manifest"])
    except StageError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None


def dataset_paths(dataset_dir: pathlib.Path) -> dict[str, pathlib.Path]:
    return {
        "dataset_dir": dataset_dir,
        "model": dataset_dir / "models" / "widefield-sky.fits",
        "request": dataset_dir / "requests" / "casars-simobserve.json",
        "manifest": dataset_dir / "wave3-aw-widefield-manifest.json",
        "ms": dataset_dir / "ms" / f"{DATASET_ID}.ms",
    }


def build_manifest(paths: dict[str, pathlib.Path]) -> dict[str, Any]:
    samples = math.ceil(DURATION_SECONDS / INTEGRATION_SECONDS)
    baseline_count = 27 * 26 // 2
    estimated_data_bytes = samples * baseline_count * CHANNEL_COUNT * 2 * 8
    wavelength_m = 299_792_458.0 / START_FREQUENCY_HZ
    w_relevance = wavelength_m * VLA_B_MAX_BASELINE_M / (VLA_DIAMETER_M * VLA_DIAMETER_M)
    return {
        "schema_version": 1,
        "id": DATASET_ID,
        "issue": 281,
        "purpose": (
            "Wave 3 AW/wide-field medium benchmark: VLA B, L band, one pointing, "
            "bright off-axis sources across the primary beam, and enough rows/channels "
            "to produce an about-memory MeasurementSet."
        ),
        "oracle_basis": [
            "Jagannathan-Widefield2024.pdf slide 15: A-projection applies antenna A-term kernels during gridding and divides by the PB/normalization image after FFT.",
            "Jagannathan-Widefield2024.pdf slide 19: w-term relevance for primary-beam FoV scales as lambda * B / D^2 > 1.",
            "Jagannathan-Widefield2024.pdf slide 23: choose image cell size by oversampling synthesized resolution by roughly 4-5.",
            "Jagannathan-Widefield2024.pdf slide 26: gridding cost scales as Nvis * support^2, making large visibility count and nontrivial support essential for performance evidence.",
        ],
        "paths": {key: str(value) for key, value in paths.items() if key != "dataset_dir"},
        "shape": {
            "instrument": "VLA",
            "array_configuration": "vla.b.cfg",
            "model_pixels": MODEL_PIXELS,
            "model_cell_arcsec": MODEL_CELL_ARCSEC,
            "image_pixels": IMAGE_PIXELS,
            "image_cell_arcsec": IMAGE_CELL_ARCSEC,
            "channels": CHANNEL_COUNT,
            "duration_seconds": DURATION_SECONDS,
            "integration_seconds": INTEGRATION_SECONDS,
            "estimated_time_samples": samples,
            "estimated_baselines": baseline_count,
            "estimated_main_rows": samples * baseline_count,
            "estimated_data_bytes": estimated_data_bytes,
            "estimated_data_gib": estimated_data_bytes / 1024.0**3,
            "w_relevance_lambda_b_over_d2": w_relevance,
            "primary_beam_fwhm_arcmin_approx": 45.0 / (START_FREQUENCY_HZ / 1.0e9),
            "synthesized_beam_arcsec_approx": wavelength_m / VLA_B_MAX_BASELINE_M * 206_264.806,
        },
        "source_model": {
            "description": "central compact/extended emission plus bright off-axis sources at primary-beam edge and outlier radii",
            "components": widefield_components(),
        },
    }


def materialize_inputs(paths: dict[str, pathlib.Path], manifest: dict[str, Any]) -> None:
    for key in ("model", "request", "manifest", "ms"):
        paths[key].parent.mkdir(parents=True, exist_ok=True)
    write_widefield_fits(paths["model"])
    atomic_write_json(paths["request"], build_request(paths))
    atomic_write_json(paths["manifest"], manifest)


def build_request(paths: dict[str, pathlib.Path]) -> dict[str, Any]:
    return {
        "kind": "run",
        "request": {
            "model_image": str(paths["model"]),
            "model_peak_jy_per_pixel": 1.0,
            "output_ms": str(paths["ms"]),
            "overwrite": True,
            "telescope_name": "VLA",
            "field_name": DATASET_ID,
            "phase_center_rad": [math.radians(PHASE_CENTER_RA_DEG), math.radians(PHASE_CENTER_DEC_DEG)],
            "antennas": vla_b_antennas(),
            "duration_seconds": DURATION_SECONDS,
            "integration_seconds": INTEGRATION_SECONDS,
            "elevation_limit_rad": math.radians(20.0),
            "allow_below_elevation_limit": False,
            "spectral_setup": {
                "name": "lband-widefield",
                "start_frequency_hz": START_FREQUENCY_HZ,
                "channel_width_hz": CHANNEL_WIDTH_HZ,
                "channel_count": CHANNEL_COUNT,
            },
            "predict_model": True,
            "corruption": {
                "seed": 281003,
                "noise": {
                    "mode": "simplenoise",
                    "simplenoise_jy": NOISE_SIMPLENOISE_JY,
                },
            },
        },
    }


def vla_b_antennas() -> list[dict[str, Any]]:
    config = pathlib.Path.home() / ".casa" / "data" / "alma" / "simmos" / "vla.b.cfg"
    if not config.exists():
        raise StageError(f"missing CASA VLA B config: {config}")
    antennas = []
    for raw_line in config.read_text(encoding="ascii").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 5:
            raise StageError(f"invalid antenna config row in {config}: {raw_line!r}")
        x_m, y_m, z_m = (float(parts[0]), float(parts[1]), float(parts[2]))
        diameter_m = float(parts[3])
        station = parts[4]
        antennas.append(
            {
                "name": station,
                "station": station,
                "position_m": [x_m, y_m, z_m],
                "dish_diameter_m": diameter_m,
            }
        )
    if len(antennas) != 27:
        raise StageError(f"expected 27 VLA B antennas in {config}, found {len(antennas)}")
    return antennas


def write_widefield_fits(path: pathlib.Path) -> None:
    plane = widefield_plane(MODEL_PIXELS)
    cards = [
        ("SIMPLE", "T"),
        ("BITPIX", "-32"),
        ("NAXIS", "2"),
        ("NAXIS1", str(MODEL_PIXELS)),
        ("NAXIS2", str(MODEL_PIXELS)),
        ("CTYPE1", "'RA---SIN'"),
        ("CTYPE2", "'DEC--SIN'"),
        ("CUNIT1", "'deg'"),
        ("CUNIT2", "'deg'"),
        ("RADESYS", "'FK5'"),
        ("EQUINOX", "2000.0"),
        ("CRPIX1", f"{MODEL_PIXELS / 2 + 0.5:.6f}"),
        ("CRPIX2", f"{MODEL_PIXELS / 2 + 0.5:.6f}"),
        ("CRVAL1", f"{PHASE_CENTER_RA_DEG:.9f}"),
        ("CRVAL2", f"{PHASE_CENTER_DEC_DEG:.9f}"),
        ("CDELT1", f"{-MODEL_CELL_ARCSEC / 3600.0:.12g}"),
        ("CDELT2", f"{MODEL_CELL_ARCSEC / 3600.0:.12g}"),
        ("BUNIT", "'Jy/pixel'"),
        ("OBJECT", "'WAVE3 VLA AW WIDEFIELD'"),
    ]
    header = "".join(format_card(key, value) for key, value in cards)
    header += "END".ljust(80)
    data = plane.astype(">f4", copy=False).T.tobytes(order="C")
    with path.open("wb") as handle:
        handle.write(pad_block(header.encode("ascii")))
        handle.write(data)
        handle.write(b"\0" * ((-len(data)) % 2880))


def widefield_plane(pixels: int) -> Any:
    coords_arcmin = (np.arange(pixels, dtype=np.float32) + 0.5 - pixels / 2.0) * MODEL_CELL_ARCSEC / 60.0
    x, y = np.meshgrid(coords_arcmin, coords_arcmin, indexing="ij")
    plane = np.zeros((pixels, pixels), dtype=np.float32)
    for component in widefield_components():
        amp = component["jy_per_pixel"]
        sigma = component["sigma_arcmin"]
        dx = x - component["dra_arcmin"]
        dy = y - component["ddec_arcmin"]
        if sigma <= 0.0:
            ix = int(round(pixels / 2.0 + component["dra_arcmin"] * 60.0 / MODEL_CELL_ARCSEC - 0.5))
            iy = int(round(pixels / 2.0 + component["ddec_arcmin"] * 60.0 / MODEL_CELL_ARCSEC - 0.5))
            if 0 <= ix < pixels and 0 <= iy < pixels:
                plane[ix, iy] += amp
        else:
            plane += amp * np.exp(-0.5 * (dx * dx + dy * dy) / (sigma * sigma))
    return plane


def widefield_components() -> list[dict[str, float | str]]:
    return [
        {"name": "central-core", "dra_arcmin": 0.0, "ddec_arcmin": 0.0, "sigma_arcmin": 0.20, "jy_per_pixel": 1.0},
        {"name": "central-extended", "dra_arcmin": -1.2, "ddec_arcmin": 0.8, "sigma_arcmin": 1.40, "jy_per_pixel": 0.08},
        {"name": "half-power-east", "dra_arcmin": 12.0, "ddec_arcmin": 1.5, "sigma_arcmin": 0.12, "jy_per_pixel": 0.65},
        {"name": "half-power-northwest", "dra_arcmin": -9.0, "ddec_arcmin": 9.5, "sigma_arcmin": 0.18, "jy_per_pixel": 0.45},
        {"name": "edge-southwest", "dra_arcmin": -16.0, "ddec_arcmin": -13.0, "sigma_arcmin": 0.10, "jy_per_pixel": 0.9},
        {"name": "outlier-northeast", "dra_arcmin": 20.5, "ddec_arcmin": 17.0, "sigma_arcmin": 0.0, "jy_per_pixel": 1.2},
        {"name": "outlier-west", "dra_arcmin": -23.0, "ddec_arcmin": 3.0, "sigma_arcmin": 0.0, "jy_per_pixel": 0.8},
    ]


def run_native_simobserve(binary: pathlib.Path, paths: dict[str, pathlib.Path]) -> None:
    if not binary.exists():
        raise StageError(f"simobserve binary does not exist: {binary}")
    completed = run_command(
        [str(binary), "--json-run", str(paths["request"])],
        cwd=REPO_ROOT,
        merge_stderr=False,
    )
    (paths["dataset_dir"] / "simobserve-stdout.json").write_text(completed.stdout, encoding="utf-8")
    (paths["dataset_dir"] / "simobserve-stderr.log").write_text(completed.stderr, encoding="utf-8")
    if completed.returncode != 0:
        raise StageError(f"simobserve failed with exit {completed.returncode}; see {paths['dataset_dir']}")


def collect_actual(paths: dict[str, pathlib.Path]) -> dict[str, Any]:
    size_bytes = directory_size(paths["ms"])
    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ms_size_bytes": size_bytes,
        "ms_size_gib": size_bytes / 1024.0**3,
    }


def directory_size(path: pathlib.Path) -> int:
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return total


def format_card(key: str, value: str) -> str:
    if value.startswith("'") and value.endswith("'"):
        return f"{key:<8}= {value:<20}".ljust(80)
    return f"{key:<8}= {value:>20}".ljust(80)


def pad_block(data: bytes) -> bytes:
    return data + b" " * ((-len(data)) % 2880)


if __name__ == "__main__":
    main()
