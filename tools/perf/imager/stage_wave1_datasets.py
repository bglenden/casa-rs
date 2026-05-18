#!/usr/bin/env python3
"""Prepare deterministic ImPerformance Wave 1 simulated dataset inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pathlib
import struct
import sys
import time
from typing import Any

try:
    import numpy as np
except ImportError:  # pragma: no cover - exercised only in minimal Python installs.
    np = None


TOOL_DIR = pathlib.Path(__file__).resolve().parent
REGISTRY_PATH = TOOL_DIR / "wave1_dataset_registry.json"
DEFAULT_OUTPUT_DIR = pathlib.Path("target/imperformance-wave1/datasets")
EXTERNAL_PREFIX = pathlib.Path("/Volumes/GLENDENNING")
MODEL_PHASE_CENTER_RA_DEG = 180.0
DEFAULT_ELEVATION_LIMIT_DEG = 20.0
OBSERVATORY_ZENITH_DEC_DEG = {
    "alma": -23.029,
    "vla": 34.07875,
}


class DatasetError(Exception):
    """Error that should be reported without a traceback."""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--registry",
        type=pathlib.Path,
        default=REGISTRY_PATH,
        help="dataset registry JSON",
    )
    parser.add_argument("--dataset", action="append", help="dataset id to include")
    parser.add_argument("--tier", action="append", help="tier to include")
    parser.add_argument("--instrument", action="append", help="instrument to include")
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT_DIR,
        help="directory for generated plan JSON",
    )
    parser.add_argument(
        "--data-root",
        type=pathlib.Path,
        default=None,
        help="explicit staging root; otherwise the registry root_env must be set",
    )
    parser.add_argument(
        "--materialize-models",
        action="store_true",
        help="write continuum model FITS files and spectral-profile JSON files",
    )
    parser.add_argument(
        "--materialize-workloads",
        action="store_true",
        help="write benchmark workload manifests for each planned dataset",
    )
    parser.add_argument(
        "--allow-non-external-large-root",
        action="store_true",
        help="allow medium/large tiers outside /Volumes/GLENDENNING",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate and write the staging plan without materializing files",
    )
    args = parser.parse_args()

    try:
        registry = read_json(args.registry)
        specs = select_datasets(
            registry,
            dataset_ids=args.dataset,
            tiers=args.tier,
            instruments=args.instrument,
        )
        data_root = resolve_data_root(registry, args.data_root)
        plan = build_plan(registry, specs, data_root, args.allow_non_external_large_root)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        plan_path = args.output_dir / "wave1-dataset-plan.json"
        if args.materialize_models and not args.dry_run:
            materialize_models(plan)
        if args.materialize_workloads and not args.dry_run:
            materialize_workloads(plan, args.output_dir / "workloads")
        write_json(plan_path, plan)
        print(plan_path)
    except DatasetError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None


def read_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise DatasetError(f"read {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise DatasetError(f"parse {path}: {error}") from error
    if not isinstance(value, dict):
        raise DatasetError(f"{path} must contain a JSON object")
    return value


def select_datasets(
    registry: dict[str, Any],
    *,
    dataset_ids: list[str] | None,
    tiers: list[str] | None,
    instruments: list[str] | None,
) -> list[dict[str, Any]]:
    datasets = registry.get("datasets")
    if not isinstance(datasets, list):
        raise DatasetError("registry missing datasets array")
    selected = []
    wanted_ids = set(dataset_ids or [])
    wanted_tiers = set(tiers or [])
    wanted_instruments = set(instruments or [])
    for item in datasets:
        if not isinstance(item, dict):
            raise DatasetError("dataset entries must be objects")
        dataset_id = required_str(item, "id")
        if wanted_ids and dataset_id not in wanted_ids:
            continue
        if wanted_tiers and required_str(item, "tier") not in wanted_tiers:
            continue
        if wanted_instruments and required_str(item, "instrument") not in wanted_instruments:
            continue
        selected.append(item)
    if wanted_ids:
        found = {required_str(item, "id") for item in selected}
        missing = sorted(wanted_ids - found)
        if missing:
            raise DatasetError(f"unknown dataset id(s): {', '.join(missing)}")
    if not selected:
        raise DatasetError("no datasets selected")
    return selected


def resolve_data_root(registry: dict[str, Any], explicit: pathlib.Path | None) -> pathlib.Path:
    if explicit is not None:
        return explicit.expanduser().resolve()
    root_env = required_str(registry, "root_env")
    value = os.environ.get(root_env)
    if not value:
        hint = registry.get("external_root_hint", "<set explicit data root>")
        raise DatasetError(f"{root_env} is required; for this system use {hint}")
    return pathlib.Path(value).expanduser().resolve()


def build_plan(
    registry: dict[str, Any],
    specs: list[dict[str, Any]],
    data_root: pathlib.Path,
    allow_non_external_large_root: bool,
) -> dict[str, Any]:
    tiers = required_object(registry, "tiers")
    instruments = required_object(registry, "instruments")
    families = required_object(registry, "families")
    validate_large_tier_policy(registry, specs)
    planned = []
    for spec in specs:
        tier_name = required_str(spec, "tier")
        tier = required_object(tiers, tier_name)
        instrument_name = required_str(spec, "instrument")
        instrument = required_object(instruments, instrument_name)
        family_name = required_str(spec, "family")
        family = required_object(families, family_name)
        if bool(tier.get("external_root_required")) and not allow_non_external_large_root:
            try:
                data_root.relative_to(EXTERNAL_PREFIX)
            except ValueError as error:
                raise DatasetError(
                    f"{required_str(spec, 'id')} is a {tier_name} tier dataset; "
                    f"stage it under {EXTERNAL_PREFIX} or pass "
                    "--allow-non-external-large-root"
                ) from error

        dataset_dir = data_root / "wave1" / instrument_name / family_name / tier_name
        continuum_model = dataset_dir / "models" / "structured-continuum.fits"
        spectral_profile = dataset_dir / "models" / "spectral-profile.json"
        casars_request = dataset_dir / "requests" / "casars-simobserve.json"
        casa_request = dataset_dir / "requests" / "casa-simulation-plan.json"
        output_ms = dataset_dir / "ms" / f"{required_str(spec, 'id')}.ms"
        selected_modes = spec.get("selected_modes", family.get("selected_modes", []))
        if not isinstance(selected_modes, list):
            raise DatasetError(f"{required_str(spec, 'id')} selected_modes must be an array")
        native_status = native_status_for(spec, instrument, family)
        planned.append(
            {
                "id": required_str(spec, "id"),
                "instrument": instrument_name,
                "family": family_name,
                "tier": tier_name,
                "target_size_bytes": int_value(tier, "target_size_bytes"),
                "storage_label": required_str(tier, "storage_label"),
                "selected_modes": selected_modes,
                "paths": {
                    "dataset_dir": str(dataset_dir),
                    "continuum_model_fits": str(continuum_model),
                    "spectral_profile_json": str(spectral_profile),
                    "casars_simobserve_request": str(casars_request),
                    "casa_simulation_plan": str(casa_request),
                    "output_ms": str(output_ms),
                },
                "shape": {
                    "model_pixels": int_value(spec, "model_pixels"),
                    "image_pixels": int_value(spec, "image_pixels"),
                    "channels": int_value(spec, "channels"),
                    "pointing_count": int_value(family, "pointing_count"),
                    "duration_seconds": float_value(spec, "duration_seconds"),
                    "integration_seconds": float_value(spec, "integration_seconds"),
                    "estimated_main_rows": estimated_main_rows(spec, family),
                    "estimated_data_bytes": estimated_data_bytes(spec, family),
                },
                "source_model": {
                    "continuum": registry["source_model"]["continuum_components"],
                    "cube": registry["source_model"]["cube_components"],
                    "noise_model": registry["source_model"]["noise_model"],
                    "noise_simplenoise_jy": float_value(spec, "noise_simplenoise_jy"),
                },
                "support": native_status,
                "provenance": {
                    "registry": str(REGISTRY_PATH),
                    "generated_by": str(pathlib.Path(__file__).resolve()),
                    "generated_at": utc_now(),
                },
            }
        )
    return {
        "schema_version": 1,
        "root_env": required_str(registry, "root_env"),
        "data_root": str(data_root),
        "external_root_hint": registry.get("external_root_hint"),
        "large_tier_policy": registry.get("large_tier_policy"),
        "datasets": planned,
    }


def validate_large_tier_policy(registry: dict[str, Any], specs: list[dict[str, Any]]) -> None:
    policy = registry.get("large_tier_policy")
    if not isinstance(policy, dict):
        return
    expected_count = policy.get("dataset_count")
    expected_id = policy.get("dataset_id")
    large_specs = [spec for spec in specs if required_str(spec, "tier") == "large"]
    if not large_specs:
        return
    if not isinstance(expected_count, int):
        raise DatasetError("large_tier_policy.dataset_count must be an integer")
    if expected_count != len(large_specs):
        raise DatasetError(
            "large tier policy expects "
            f"{expected_count} dataset(s), selected {len(large_specs)}"
        )
    if isinstance(expected_id, str):
        large_ids = {required_str(spec, "id") for spec in large_specs}
        if expected_id not in large_ids:
                raise DatasetError(
                    f"large tier policy requires dataset {expected_id!r}"
                )


def native_status_for(
    spec: dict[str, Any], instrument: dict[str, Any], family: dict[str, Any]
) -> dict[str, str]:
    instrument_status = required_str(instrument, "native_casars_status")
    return {
        "casars_simulation": "supported",
        "casa_simulation": "generation-path",
        "reason": f"native simobserve can generate this {instrument_status} benchmark shape; CASA side is retained for simulation performance/parity checks",
    }


def materialize_models(plan: dict[str, Any]) -> None:
    for dataset in plan["datasets"]:
        paths = dataset["paths"]
        model_path = pathlib.Path(paths["continuum_model_fits"])
        profile_path = pathlib.Path(paths["spectral_profile_json"])
        request_path = pathlib.Path(paths["casars_simobserve_request"])
        casa_plan_path = pathlib.Path(paths["casa_simulation_plan"])
        model_path.parent.mkdir(parents=True, exist_ok=True)
        profile_path.parent.mkdir(parents=True, exist_ok=True)
        request_path.parent.mkdir(parents=True, exist_ok=True)
        write_structured_fits(
            model_path,
            pixels=int(dataset["shape"]["model_pixels"]),
            channels=int(dataset["shape"]["channels"]),
            instrument=dataset["instrument"],
            family=dataset["family"],
        )
        spectral_profile = build_spectral_profile(dataset)
        write_json(profile_path, spectral_profile)
        write_json(request_path, build_casars_simobserve_request(dataset))
        write_json(casa_plan_path, build_casa_simulation_plan(dataset))
        dataset["artifacts"] = {
            "continuum_model_sha256": sha256_file(model_path),
            "spectral_profile_sha256": sha256_file(profile_path),
            "casars_request_sha256": sha256_file(request_path),
            "casa_plan_sha256": sha256_file(casa_plan_path),
        }


def materialize_workloads(plan: dict[str, Any], output_dir: pathlib.Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for dataset in plan["datasets"]:
        for mode_id in dataset["selected_modes"]:
            workload = build_workload_manifest(dataset, mode_id)
            write_json(output_dir / f"{dataset['id']}-{mode_id}.json", workload)


def build_workload_manifest(dataset: dict[str, Any], mode_id: str) -> dict[str, Any]:
    specmode = "cube" if "cube" in mode_id else "mfs"
    gridder = "mosaic" if mode_id.startswith("mosaic") else "standard"
    is_clean = "clean" in mode_id or mode_id.startswith("mtmfs")
    deconvolver = "multiscale" if is_clean else "hogbom"
    channels = workload_channel_count(dataset, mode_id, specmode)
    if dataset["tier"] == "small":
        niter = 25 if is_clean else 0
    elif dataset["tier"] == "medium":
        niter = 100 if is_clean else 0
    else:
        niter = 250 if is_clean else 0
    return {
        "id": f"{dataset['id']}-{mode_id}",
        "mode_id": mode_id,
        "description": f"{dataset['id']} {mode_id} generated from ImPerformance Wave 1 dataset plan.",
        "dataset": {
            "key": dataset["id"],
            "root_env": "CASA_RS_IMPERF_DATA_ROOT",
            "relative_path": relative_to_wave_root(dataset["paths"]["output_ms"]),
        },
        "imaging": {
            "mode": "clean" if is_clean else "dirty",
            "specmode": specmode,
            "gridder": gridder,
            "field": "" if gridder == "mosaic" else "0",
            "phasecenter_field": 0 if gridder == "mosaic" else None,
            "spw": "0",
            "channel_start": 0,
            "channel_count": channels,
            "imsize": int(dataset["shape"]["image_pixels"]),
            "cell_arcsec": 0.1 if dataset["instrument"] == "alma" else 0.5,
            "weighting": "briggs" if is_clean else "natural",
            "robust": 0.5,
            "deconvolver": deconvolver,
            "scales": [0, 5, 15] if deconvolver == "multiscale" else "",
            "niter": niter,
            "wterm": "none",
        },
        "run": {
            "repeats": 3,
            "run_label": "warm",
            "storage_label": dataset["storage_label"],
        },
    }


def workload_channel_count(dataset: dict[str, Any], mode_id: str, specmode: str) -> int:
    channels = int(dataset["shape"]["channels"])
    if specmode != "cube":
        return channels
    if "bounded" in mode_id:
        return min(channels, 32)
    return channels


def relative_to_wave_root(path_text: str) -> str:
    parts = pathlib.Path(path_text).parts
    if "wave1" in parts:
        return str(pathlib.Path(*parts[parts.index("wave1") :]))
    return path_text


def build_casars_simobserve_request(dataset: dict[str, Any]) -> dict[str, Any]:
    fields = build_casars_fields(dataset)
    request: dict[str, Any] = {
        "kind": "run",
        "request": {
            "model_image": dataset["paths"]["continuum_model_fits"],
            "output_ms": dataset["paths"]["output_ms"],
            "overwrite": True,
            "telescope_name": dataset["instrument"].upper(),
            "field_name": dataset["id"],
            "phase_center_rad": phase_center_rad(dataset["instrument"], 0.0, 0.0),
            "fields": fields,
            "duration_seconds": dataset["shape"]["duration_seconds"],
            "integration_seconds": dataset["shape"]["integration_seconds"],
            "elevation_limit_rad": math.radians(DEFAULT_ELEVATION_LIMIT_DEG),
            "allow_below_elevation_limit": False,
            "spectral_setup": {
                "name": "wave1",
                "start_frequency_hz": start_frequency_hz(dataset["instrument"]),
                "channel_width_hz": channel_width_hz(dataset["instrument"]),
                "channel_count": dataset["shape"]["channels"],
            },
            "predict_model": True,
            "corruption": {
                "seed": stable_seed(dataset["id"]),
                "noise": {
                    "mode": "simplenoise",
                    "simplenoise_jy": dataset["source_model"]["noise_simplenoise_jy"],
                },
            },
        },
    }
    if dataset["instrument"] == "alma":
        request["request"]["antennas"] = alma_antennas()
    elif dataset["instrument"] == "vla":
        request["request"]["antennas"] = vla_d_antennas()
    return request


def phase_center_deg(instrument: str, dra_arcsec: float, ddec_arcsec: float) -> tuple[float, float]:
    try:
        dec_deg = OBSERVATORY_ZENITH_DEC_DEG[instrument]
    except KeyError as exc:
        raise DatasetError(f"no zenith-transit default phase center for {instrument}") from exc
    return (
        MODEL_PHASE_CENTER_RA_DEG + dra_arcsec / 3600.0,
        dec_deg + ddec_arcsec / 3600.0,
    )


def phase_center_rad(instrument: str, dra_arcsec: float, ddec_arcsec: float) -> list[float]:
    ra_deg, dec_deg = phase_center_deg(instrument, dra_arcsec, ddec_arcsec)
    ra_rad = math.radians(ra_deg)
    if ra_rad > math.pi:
        ra_rad -= math.tau
    return [
        ra_rad,
        math.radians(dec_deg),
    ]


def build_casars_fields(dataset: dict[str, Any]) -> list[dict[str, Any]]:
    count = int(dataset["shape"]["pointing_count"])
    if count <= 1:
        return []
    spacing_arcsec = 24.0 if dataset["instrument"] == "alma" else 120.0
    offsets = [(0.0, 0.0)]
    for index in range(6):
        angle = math.tau * index / 6.0
        offsets.append((spacing_arcsec * math.cos(angle), spacing_arcsec * math.sin(angle)))
    fields = []
    for index, (dra_arcsec, ddec_arcsec) in enumerate(offsets[:count]):
        fields.append(
            {
                "name": f"{dataset['id']}_field_{index}",
                "phase_center_rad": phase_center_rad(dataset["instrument"], dra_arcsec, ddec_arcsec),
            }
        )
    return fields


def build_casa_simulation_plan(dataset: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": dataset["support"]["casa_simulation"],
        "dataset": dataset["id"],
        "purpose": "CASA C++ simulation parity and performance check for the same source model and observing shape.",
        "model_image": dataset["paths"]["continuum_model_fits"],
        "spectral_profile": dataset["paths"]["spectral_profile_json"],
        "output_ms": dataset["paths"]["output_ms"].replace(".ms", ".casa.ms"),
        "instrument": dataset["instrument"],
        "family": dataset["family"],
        "tier": dataset["tier"],
        "shape": dataset["shape"],
        "noise_simplenoise_jy": dataset["source_model"]["noise_simplenoise_jy"],
        "notes": [
            "Use CASA simobserve/simalma where available for the instrument and family.",
            "Record CASA ran, skipped, or blocked with reason before claiming simulation parity or performance.",
            "Do not use this plan as a calibration test; the intended corruption is simple deterministic visibility noise.",
        ],
    }


def write_structured_fits(
    path: pathlib.Path, *, pixels: int, channels: int, instrument: str, family: str
) -> None:
    if pixels < 8:
        raise DatasetError("model FITS must be at least 8x8 pixels")
    if channels < 1:
        raise DatasetError("model FITS must have at least one channel")
    ra_deg, dec_deg = phase_center_deg(instrument, 0.0, 0.0)
    cards = [
        ("SIMPLE", "T"),
        ("BITPIX", "-32"),
        ("NAXIS", "4" if channels > 1 else "2"),
        ("NAXIS1", str(pixels)),
        ("NAXIS2", str(pixels)),
    ]
    if channels > 1:
        cards.extend(
            [
                ("NAXIS3", "1"),
                ("NAXIS4", str(channels)),
            ]
        )
    cards.extend(
        [
            ("CTYPE1", "'RA---SIN'"),
            ("CTYPE2", "'DEC--SIN'"),
            ("CUNIT1", "'deg'"),
            ("CUNIT2", "'deg'"),
            ("RADESYS", "'FK5'"),
            ("EQUINOX", "2000.0"),
            ("CRPIX1", f"{pixels / 2 + 0.5:.6f}"),
            ("CRPIX2", f"{pixels / 2 + 0.5:.6f}"),
            ("CRVAL1", f"{ra_deg:.9f}"),
            ("CRVAL2", f"{dec_deg:.9f}"),
            ("CDELT1", f"{-cell_deg(instrument):.12g}"),
            ("CDELT2", f"{cell_deg(instrument):.12g}"),
        ]
    )
    if channels > 1:
        cards.extend(
            [
                ("CTYPE3", "'STOKES'"),
                ("CRPIX3", "1.0"),
                ("CRVAL3", "1.0"),
                ("CDELT3", "1.0"),
                ("CTYPE4", "'FREQ'"),
                ("CUNIT4", "'Hz'"),
                ("CRPIX4", "1.0"),
                ("CRVAL4", f"{start_frequency_hz(instrument):.12E}"),
                ("CDELT4", f"{channel_width_hz(instrument):.12E}"),
            ]
        )
    cards.extend(
        [
            ("BUNIT", "'Jy/pixel'"),
            ("OBJECT", f"'{instrument.upper()} WAVE1 {family.upper()}'"),
        ]
    )
    header = "".join(format_card(key, value) for key, value in cards)
    header += "END".ljust(80)
    header = pad_block(header.encode("ascii"))
    with path.open("wb") as handle:
        handle.write(header)
        if np is not None:
            base = source_plane(pixels, family)
            for channel in range(channels):
                plane = (base * spectral_total_scale(channels, channel)).astype(">f4", copy=False)
                handle.write(plane.T.tobytes(order="C"))
        else:
            for channel in range(channels):
                scale = spectral_total_scale(channels, channel)
                for y in range(pixels):
                    row = bytearray()
                    for x in range(pixels):
                        row.extend(struct.pack(">f", source_pixel(x, y, pixels, family) * scale))
                    handle.write(row)
        pad = (-(pixels * pixels * channels * 4)) % 2880
        if pad:
            handle.write(b"\0" * pad)


def spectral_total_scale(channels: int, channel: int) -> float:
    if channels == 1:
        return 1.0
    center = 0.5 * max(0, channels - 1)
    x = (channel - center) / max(1.0, center)
    continuum = max(0.05, 1.0 + x) ** -0.7
    broad_line = 0.45 * math.exp(-0.5 * ((x - 0.05) / 0.22) ** 2)
    narrow_line = 0.22 * math.exp(-0.5 * ((x + 0.38) / 0.08) ** 2)
    absorption = -0.18 * math.exp(-0.5 * ((x - 0.32) / 0.06) ** 2)
    return max(0.05, continuum + broad_line + narrow_line + absorption)


def source_pixel(x: int, y: int, pixels: int, family: str) -> float:
    cx = (x + 0.5 - pixels / 2.0) / pixels
    cy = (y + 0.5 - pixels / 2.0) / pixels
    radius = math.hypot(cx, cy)
    theta = math.atan2(cy, cx)
    core = gaussian(cx, cy, 0.0, 0.0, 0.018)
    knot1 = 0.42 * gaussian(cx, cy, -0.14, 0.09, 0.028)
    knot2 = 0.31 * gaussian(cx, cy, 0.18, -0.11, 0.022)
    ring = 0.34 * math.exp(-((radius - 0.21) ** 2) / (2.0 * 0.018**2))
    arm1 = spiral_arm(radius, theta, phase=0.20, pitch=14.0, radius0=0.18, width=0.28, radial_width=0.11)
    arm2 = 0.78 * spiral_arm(
        radius,
        theta,
        phase=math.pi + 0.55,
        pitch=12.0,
        radius0=0.20,
        width=0.30,
        radial_width=0.12,
    )
    halo = 0.06 * math.exp(-(radius**2) / (2.0 * 0.26**2))
    ripple = 0.015 * (1.0 + math.sin(37.0 * cx + 19.0 * cy))
    mosaic_gradient = 1.0 + (0.10 * cx - 0.07 * cy if is_mosaic_family(family) else 0.0)
    value = (core + knot1 + knot2 + ring + arm1 + arm2 + halo + ripple) * mosaic_gradient
    return max(0.0, float(value))


def source_plane(pixels: int, family: str) -> Any:
    coords = (np.arange(pixels, dtype=np.float32) + 0.5 - pixels / 2.0) / pixels
    cx, cy = np.meshgrid(coords, coords, indexing="ij")
    radius = np.hypot(cx, cy)
    theta = np.arctan2(cy, cx)
    core = np.exp(-((cx**2 + cy**2) / (2.0 * 0.018**2)))
    knot1 = 0.42 * np.exp(-(((cx + 0.14) ** 2 + (cy - 0.09) ** 2) / (2.0 * 0.028**2)))
    knot2 = 0.31 * np.exp(-(((cx - 0.18) ** 2 + (cy + 0.11) ** 2) / (2.0 * 0.022**2)))
    ring = 0.34 * np.exp(-((radius - 0.21) ** 2) / (2.0 * 0.018**2))
    arm1 = spiral_arm_array(radius, theta, phase=0.20, pitch=14.0, radius0=0.18, width=0.28, radial_width=0.11)
    arm2 = 0.78 * spiral_arm_array(
        radius,
        theta,
        phase=math.pi + 0.55,
        pitch=12.0,
        radius0=0.20,
        width=0.30,
        radial_width=0.12,
    )
    halo = 0.06 * np.exp(-(radius**2) / (2.0 * 0.26**2))
    ripple = 0.015 * (1.0 + np.sin(37.0 * cx + 19.0 * cy))
    mosaic_gradient = 1.0 + (0.10 * cx - 0.07 * cy if is_mosaic_family(family) else 0.0)
    return np.maximum(
        (core + knot1 + knot2 + ring + arm1 + arm2 + halo + ripple) * mosaic_gradient,
        0.0,
    ).astype("float32")


def spiral_arm(
    radius: float,
    theta: float,
    *,
    phase: float,
    pitch: float,
    radius0: float,
    width: float,
    radial_width: float,
) -> float:
    target = phase + pitch * (radius - radius0)
    angular = math.atan2(math.sin(theta - target), math.cos(theta - target))
    angular_profile = math.exp(-(angular**2) / (2.0 * width**2))
    radial_profile = math.exp(-((radius - radius0) ** 2) / (2.0 * radial_width**2))
    return 0.18 * angular_profile * radial_profile


def spiral_arm_array(
    radius: Any,
    theta: Any,
    *,
    phase: float,
    pitch: float,
    radius0: float,
    width: float,
    radial_width: float,
) -> Any:
    target = phase + pitch * (radius - radius0)
    angular = np.arctan2(np.sin(theta - target), np.cos(theta - target))
    angular_profile = np.exp(-(angular**2) / (2.0 * width**2))
    radial_profile = np.exp(-((radius - radius0) ** 2) / (2.0 * radial_width**2))
    return 0.18 * angular_profile * radial_profile


def is_mosaic_family(family: str) -> bool:
    return family in {"mosaic", "mosaic-large"}


def build_spectral_profile(dataset: dict[str, Any]) -> dict[str, Any]:
    channels = int(dataset["shape"]["channels"])
    profile = []
    center = 0.5 * max(0, channels - 1)
    for channel in range(channels):
        x = 0.0 if channels == 1 else (channel - center) / max(1.0, center)
        continuum = max(0.05, 1.0 + x) ** -0.7
        broad_line = 0.45 * math.exp(-0.5 * ((x - 0.05) / 0.22) ** 2)
        narrow_line = 0.22 * math.exp(-0.5 * ((x + 0.38) / 0.08) ** 2)
        absorption = -0.18 * math.exp(-0.5 * ((x - 0.32) / 0.06) ** 2)
        profile.append(
            {
                "channel": channel,
                "relative_frequency_offset": x,
                "continuum_scale": continuum,
                "line_scale": broad_line + narrow_line + absorption,
                "total_scale": max(0.05, continuum + broad_line + narrow_line + absorption),
            }
        )
    return {
        "schema_version": 1,
        "dataset": dataset["id"],
        "description": "Deterministic cube spectral structure for detecting channel ordering and interpolation regressions.",
        "channels": channels,
        "components": dataset["source_model"]["cube"],
        "profile": profile,
    }


def estimated_main_rows(spec: dict[str, Any], family: dict[str, Any]) -> int:
    antenna_count = 27 if required_str(spec, "instrument") == "vla" else 43
    baseline_count = antenna_count * (antenna_count - 1) // 2
    samples = math.ceil(float_value(spec, "duration_seconds") / float_value(spec, "integration_seconds"))
    return baseline_count * samples


def estimated_data_bytes(spec: dict[str, Any], family: dict[str, Any]) -> int:
    return estimated_main_rows(spec, family) * int_value(spec, "channels") * 2 * 8


def alma_antennas() -> list[dict[str, Any]]:
    config = casa_array_config_path("alma.cycle8.5.cfg")
    if config is not None:
        return antennas_from_casa_array_config(config, loc_converter=alma_loc_to_itrf)

    center = [2_225_142.18027137, -5_440_307.37035444, -2_481_029.85184099]
    antennas = []
    for index in range(43):
        arm = index % 3
        radius = 18.0 + 14.0 * index
        angle = (2.0 * math.pi * arm / 3.0) + index * 0.37
        x_offset = radius * math.cos(angle)
        y_offset = radius * math.sin(angle)
        z_offset = 0.08 * radius * math.sin(angle * 0.5)
        antennas.append(
            {
                "name": f"DA{index + 1:02d}",
                "station": f"A{index + 1:03d}",
                "position_m": [
                    center[0] + x_offset,
                    center[1] + y_offset,
                    center[2] + z_offset,
                ],
                "dish_diameter_m": 12.0,
            }
        )
    return antennas


def vla_d_antennas() -> list[dict[str, Any]]:
    config = casa_array_config_path("vla.d.cfg")
    if config is not None:
        return antennas_from_casa_array_config(config)

    rows = [
        (-1_601_188.989351, -5_042_000.518599, 3_554_843.384480, "W01"),
        (-1_601_225.230987, -5_041_980.390730, 3_554_855.657987, "W02"),
        (-1_601_265.110332, -5_041_982.563379, 3_554_834.816409, "W03"),
        (-1_601_315.874282, -5_041_985.324465, 3_554_808.263784, "W04"),
        (-1_601_376.950042, -5_041_988.682890, 3_554_776.344871, "W05"),
        (-1_601_447.176774, -5_041_992.529191, 3_554_739.647266, "W06"),
        (-1_601_526.335275, -5_041_996.876364, 3_554_698.284889, "W07"),
        (-1_601_614.061201, -5_042_001.676547, 3_554_652.455603, "W08"),
        (-1_601_709.987416, -5_042_006.942534, 3_554_602.306306, "W09"),
        (-1_601_192.424192, -5_042_022.883542, 3_554_810.383317, "E01"),
        (-1_601_150.027460, -5_042_000.630731, 3_554_860.703495, "E02"),
        (-1_601_114.318178, -5_042_023.187696, 3_554_844.922416, "E03"),
        (-1_601_068.771188, -5_042_051.929370, 3_554_824.767363, "E04"),
        (-1_601_014.405657, -5_042_086.261585, 3_554_800.768970, "E05"),
        (-1_600_951.545716, -5_042_125.927280, 3_554_772.987195, "E06"),
        (-1_600_880.545264, -5_042_170.376845, 3_554_741.425036, "E07"),
        (-1_600_801.880602, -5_042_219.386677, 3_554_706.382285, "E08"),
        (-1_600_715.918854, -5_042_273.142150, 3_554_668.128757, "E09"),
        (-1_601_185.553970, -5_041_978.191573, 3_554_876.382645, "N01"),
        (-1_601_180.820941, -5_041_947.459898, 3_554_921.573373, "N02"),
        (-1_601_177.368455, -5_041_925.069104, 3_554_954.532566, "N03"),
        (-1_601_173.903632, -5_041_902.679083, 3_554_987.485762, "N04"),
        (-1_601_168.735762, -5_041_869.062707, 3_555_036.885577, "N05"),
        (-1_601_162.553007, -5_041_829.021602, 3_555_095.854771, "N06"),
        (-1_601_155.593706, -5_041_783.860938, 3_555_162.327771, "N07"),
        (-1_601_147.885235, -5_041_733.855114, 3_555_235.914849, "N08"),
        (-1_601_139.483292, -5_041_679.021042, 3_555_316.478099, "N09"),
    ]
    return [
        {
            "name": name,
            "station": name,
            "position_m": [x_m, y_m, z_m],
            "dish_diameter_m": 25.0,
        }
        for x_m, y_m, z_m, name in rows
    ]


def casa_array_config_path(file_name: str) -> pathlib.Path | None:
    candidates = []
    if root := os.environ.get("CASA_RS_ARRAY_CONFIG_ROOT"):
        candidates.append(pathlib.Path(root) / file_name)
    candidates.append(pathlib.Path.home() / ".casa" / "data" / "alma" / "simmos" / file_name)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def antennas_from_casa_array_config(
    path: pathlib.Path,
    *,
    loc_converter: Any | None = None,
) -> list[dict[str, Any]]:
    antennas = []
    for raw_line in path.read_text(encoding="ascii").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 5:
            raise DatasetError(f"invalid antenna config row in {path}: {raw_line!r}")
        x_m, y_m, z_m = (float(parts[0]), float(parts[1]), float(parts[2]))
        diameter_m = float(parts[3])
        station = parts[4]
        position = (
            loc_converter(x_m, y_m, z_m)
            if loc_converter is not None
            else [x_m, y_m, z_m]
        )
        antennas.append(
            {
                "name": station,
                "station": station,
                "position_m": position,
                "dish_diameter_m": diameter_m,
            }
        )
    if not antennas:
        raise DatasetError(f"antenna config {path} contained no antennas")
    return antennas


def alma_loc_to_itrf(x_m: float, y_m: float, z_m: float) -> list[float]:
    # CASA's ALMA configs use coordsys=LOC.  simutil.readantenna converts those
    # rows with locxyz2itrf(obslat, obslon, obsalt, x, y, z), where obslat/lon/alt
    # come from me.measure(me.observatory("ALMA"), "WGS84").
    latitude_rad = math.radians(-23.022886)
    longitude_rad = math.radians(-67.754929)
    altitude_m = 5056.8
    sin_lat = math.sin(latitude_rad)
    cos_lat = math.cos(latitude_rad)
    cos_lon = math.cos(longitude_rad)
    sin_lon = math.sin(longitude_rad)
    semi_major_m = 6_378_137.0
    semi_minor_m = 6_356_752.3142
    eccentric_angle = math.acos(semi_minor_m / semi_major_m)
    radius = semi_major_m / math.sqrt(1.0 - (math.sin(eccentric_angle) * sin_lat) ** 2)
    local_radius = (radius + z_m + altitude_m) * cos_lat - y_m * sin_lat
    return [
        local_radius * cos_lon - x_m * sin_lon,
        local_radius * sin_lon + x_m * cos_lon,
        (radius * (semi_minor_m / semi_major_m) ** 2 + z_m + altitude_m) * sin_lat
        + y_m * cos_lat,
    ]


def format_card(key: str, value: str) -> str:
    if value.startswith("'") and value.endswith("'"):
        return f"{key:<8}= {value:<20}".ljust(80)
    return f"{key:<8}= {value:>20}".ljust(80)


def pad_block(data: bytes) -> bytes:
    return data + b" " * ((-len(data)) % 2880)


def gaussian(x: float, y: float, x0: float, y0: float, sigma: float) -> float:
    return math.exp(-(((x - x0) ** 2 + (y - y0) ** 2) / (2.0 * sigma**2)))


def cell_deg(instrument: str) -> float:
    return (0.08 if instrument == "alma" else 0.35) / 3600.0


def start_frequency_hz(instrument: str) -> float:
    return 230.0e9 if instrument == "alma" else 8.0e9


def channel_width_hz(instrument: str) -> float:
    return 2.0e6


def stable_seed(text: str) -> int:
    return int(hashlib.sha256(text.encode("utf-8")).hexdigest()[:12], 16)


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def write_json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def required_object(obj: dict[str, Any], key: str) -> dict[str, Any]:
    value = obj.get(key)
    if not isinstance(value, dict):
        raise DatasetError(f"missing object field {key!r}")
    return value


def required_str(obj: dict[str, Any], key: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or value == "":
        raise DatasetError(f"missing string field {key!r}")
    return value


def int_value(obj: dict[str, Any], key: str) -> int:
    value = obj.get(key)
    if not isinstance(value, int):
        raise DatasetError(f"{key!r} must be an integer")
    return value


def float_value(obj: dict[str, Any], key: str) -> float:
    value = obj.get(key)
    if not isinstance(value, (int, float)):
        raise DatasetError(f"{key!r} must be numeric")
    return float(value)


if __name__ == "__main__":
    main()
