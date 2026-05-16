#!/usr/bin/env python3
"""Generate CASA MeasurementSets and preview images for Wave 1 datasets.

Run this with the CASA-capable Python listed in AGENTS.md, not with the
default project Python.
"""

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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan", type=pathlib.Path, help="wave1-dataset-plan.json")
    parser.add_argument("--dataset", action="append", help="dataset id to generate")
    parser.add_argument("--skip-existing", action="store_true", default=True)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--preview-max-pixels", type=int, default=1024)
    parser.add_argument("--no-preview", action="store_true")
    args = parser.parse_args()

    if args.overwrite:
        args.skip_existing = False

    plan = read_json(args.plan)
    datasets = plan["datasets"]
    if args.dataset:
        wanted = set(args.dataset)
        datasets = [dataset for dataset in datasets if dataset["id"] in wanted]
        missing = wanted - {dataset["id"] for dataset in datasets}
        if missing:
            raise SystemExit(f"unknown dataset id(s): {', '.join(sorted(missing))}")

    results = []
    for dataset in datasets:
        result = generate_dataset(
            dataset,
            skip_existing=args.skip_existing,
            overwrite=args.overwrite,
            preview=not args.no_preview,
            preview_max_pixels=args.preview_max_pixels,
        )
        results.append(result)
        print(json.dumps(result, sort_keys=True), flush=True)

    summary_path = pathlib.Path(plan["data_root"]) / "wave1" / "generation-summary.json"
    write_json(summary_path, {"generated_at": utc_now(), "datasets": results})
    print(summary_path)


def generate_dataset(
    dataset: dict[str, Any],
    *,
    skip_existing: bool,
    overwrite: bool,
    preview: bool,
    preview_max_pixels: int,
) -> dict[str, Any]:
    dataset_id = dataset["id"]
    paths = dataset["paths"]
    canonical_ms = pathlib.Path(paths["output_ms"])
    dataset_dir = pathlib.Path(paths["dataset_dir"])
    metadata_dir = dataset_dir / "metadata"
    preview_dir = dataset_dir / "previews"
    casa_dir = dataset_dir / "casa"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    preview_dir.mkdir(parents=True, exist_ok=True)
    casa_dir.mkdir(parents=True, exist_ok=True)

    if canonical_ms.exists() and skip_existing:
        shape = inspect_ms(canonical_ms)
        shape_path = metadata_dir / "ms-shape.json"
        write_json(shape_path, shape)
        preview_path = None
        if preview and not (preview_dir / "dirty-mfs-panel.png").exists():
            preview_path = make_preview(dataset, canonical_ms, preview_dir, preview_max_pixels)
        return {
            "dataset": dataset_id,
            "status": "reused",
            "ms": str(canonical_ms),
            "shape": shape,
            "shape_json": str(shape_path),
            "preview_png": str(preview_path) if preview_path else None,
        }

    if canonical_ms.exists() and overwrite:
        remove_path(canonical_ms)

    model_image = create_model_image(dataset)
    pointings = write_pointings(dataset, casa_dir)
    project_name = dataset_id
    project_dir = casa_dir / project_name
    if project_dir.exists() and overwrite:
        remove_path(project_dir)
    project_dir.parent.mkdir(parents=True, exist_ok=True)

    run_simobserve(dataset, model_image, pointings, project_dir)
    produced_ms = find_single_ms(project_dir)
    remove_duplicate_measurement_sets(project_dir, produced_ms)
    canonical_ms.parent.mkdir(parents=True, exist_ok=True)
    if canonical_ms.exists():
        remove_path(canonical_ms)
    os.symlink(produced_ms, canonical_ms, target_is_directory=True)

    shape = inspect_ms(canonical_ms)
    shape_path = metadata_dir / "ms-shape.json"
    write_json(shape_path, shape)
    preview_path = make_preview(dataset, canonical_ms, preview_dir, preview_max_pixels) if preview else None
    return {
        "dataset": dataset_id,
        "status": "generated",
        "project_dir": str(project_dir),
        "produced_ms": str(produced_ms),
        "ms": str(canonical_ms),
        "shape": shape,
        "shape_json": str(shape_path),
        "preview_png": str(preview_path) if preview_path else None,
    }


def create_model_image(dataset: dict[str, Any]) -> pathlib.Path:
    from casatools import coordsys, image

    shape = dataset["shape"]
    paths = dataset["paths"]
    pixels = int(shape["model_pixels"])
    channels = int(shape["channels"])
    out = pathlib.Path(paths["continuum_model_fits"]).with_name(
        f"structured-cube-{channels}ch.image"
    )
    if out.exists():
        return out

    base = structured_plane(pixels, dataset["family"])
    profile = spectral_profile(channels)

    cs = coordsys()
    cs.newcoordsys(direction=True, stokes=["I"], spectral=True)
    cs.setunits(["deg", "deg"], type="direction")
    cs.setincrement(
        [
            -cell_arcsec(dataset["instrument"]) / 3600.0,
            cell_arcsec(dataset["instrument"]) / 3600.0,
        ],
        type="direction",
    )
    cs.setreferencepixel([pixels / 2.0 - 0.5, pixels / 2.0 - 0.5], type="direction")
    cs.setreferencevalue([270.000129, -22.999889], type="direction")
    cs.setreferencevalue(f"{start_frequency_hz(dataset['instrument'])}Hz", type="spectral")
    cs.setincrement(f"{channel_width_hz(dataset['instrument'])}Hz", type="spectral")
    ia = image()
    out.parent.mkdir(parents=True, exist_ok=True)
    ia.fromshape(
        outfile=str(out),
        shape=[pixels, pixels, 1, channels],
        csys=cs.torecord(),
        overwrite=True,
        type="f",
    )
    try:
        for channel, scale in enumerate(profile):
            plane = (base * scale)[:, :, np.newaxis, np.newaxis]
            ia.putchunk(pixels=plane, blc=[0, 0, 0, channel])
        ia.setbrightnessunit("Jy/pixel")
    finally:
        ia.close()
    return out


def structured_plane(pixels: int, family: str) -> np.ndarray:
    coords = (np.arange(pixels, dtype=np.float32) + 0.5 - pixels / 2.0) / pixels
    cx, cy = np.meshgrid(coords, coords, indexing="ij")
    radius = np.hypot(cx, cy)
    theta = np.arctan2(cy, cx)
    core = gaussian_array(cx, cy, 0.0, 0.0, 0.018)
    knot1 = 0.42 * gaussian_array(cx, cy, -0.14, 0.09, 0.028)
    knot2 = 0.31 * gaussian_array(cx, cy, 0.18, -0.11, 0.022)
    ring = 0.34 * np.exp(-((radius - 0.21) ** 2) / (2.0 * 0.018**2))
    arm1 = 0.18 * np.exp(-((radius - (0.10 + 0.040 * theta)) ** 2) / (2.0 * 0.020**2))
    arm2 = 0.14 * np.exp(-((radius - (0.18 - 0.035 * theta)) ** 2) / (2.0 * 0.024**2))
    halo = 0.06 * np.exp(-(radius**2) / (2.0 * 0.26**2))
    ripple = 0.015 * (1.0 + np.sin(37.0 * cx + 19.0 * cy))
    if "mosaic" in family or family == "shared-large":
        gradient = 1.0 + 0.10 * cx - 0.07 * cy
    else:
        gradient = 1.0
    return np.maximum(
        0.0,
        (core + knot1 + knot2 + ring + arm1 + arm2 + halo + ripple) * gradient,
    ).astype(np.float32)


def source_pixel(cx: float, cy: float, family: str) -> float:
    radius = math.hypot(cx, cy)
    theta = math.atan2(cy, cx)
    core = gaussian(cx, cy, 0.0, 0.0, 0.018)
    knot1 = 0.42 * gaussian(cx, cy, -0.14, 0.09, 0.028)
    knot2 = 0.31 * gaussian(cx, cy, 0.18, -0.11, 0.022)
    ring = 0.34 * math.exp(-((radius - 0.21) ** 2) / (2.0 * 0.018**2))
    arm1 = 0.18 * math.exp(-((radius - (0.10 + 0.040 * theta)) ** 2) / (2.0 * 0.020**2))
    arm2 = 0.14 * math.exp(-((radius - (0.18 - 0.035 * theta)) ** 2) / (2.0 * 0.024**2))
    halo = 0.06 * math.exp(-(radius**2) / (2.0 * 0.26**2))
    ripple = 0.015 * (1.0 + math.sin(37.0 * cx + 19.0 * cy))
    gradient = 1.0 + (0.10 * cx - 0.07 * cy if "mosaic" in family or family == "shared-large" else 0.0)
    return max(0.0, (core + knot1 + knot2 + ring + arm1 + arm2 + halo + ripple) * gradient)


def spectral_profile(channels: int) -> list[float]:
    if channels == 1:
        return [1.0]
    center = 0.5 * max(0, channels - 1)
    values = []
    for channel in range(channels):
        x = 0.0 if channels == 1 else (channel - center) / max(1.0, center)
        continuum = max(0.05, 1.0 + x) ** -0.7
        broad_line = 0.45 * math.exp(-0.5 * ((x - 0.05) / 0.22) ** 2)
        narrow_line = 0.22 * math.exp(-0.5 * ((x + 0.38) / 0.08) ** 2)
        absorption = -0.18 * math.exp(-0.5 * ((x - 0.32) / 0.06) ** 2)
        values.append(max(0.05, continuum + broad_line + narrow_line + absorption))
    return values


def write_pointings(dataset: dict[str, Any], casa_dir: pathlib.Path) -> pathlib.Path:
    count = int(dataset["shape"]["pointing_count"])
    spacing_arcsec = 24.0 if dataset["instrument"] == "alma" else 120.0
    offsets = [(0.0, 0.0)]
    if count > 1:
        for index in range(6):
            angle = math.tau * index / 6.0
            offsets.append((spacing_arcsec * math.cos(angle), spacing_arcsec * math.sin(angle)))
    out = casa_dir / f"{dataset['id']}.ptg.txt"
    lines = []
    for dra_arcsec, ddec_arcsec in offsets[:count]:
        lines.append(format_direction(270.000129 + dra_arcsec / 3600.0, -22.999889 + ddec_arcsec / 3600.0))
    out.write_text("\n".join(lines) + "\n", encoding="ascii")
    return out


def format_direction(ra_deg: float, dec_deg: float) -> str:
    ra_hours = ra_deg / 15.0
    hh = int(ra_hours)
    mm_float = (ra_hours - hh) * 60.0
    mm = int(mm_float)
    ss = (mm_float - mm) * 60.0
    sign = "-" if dec_deg < 0 else "+"
    dec_abs = abs(dec_deg)
    dd = int(dec_abs)
    dm_float = (dec_abs - dd) * 60.0
    dm = int(dm_float)
    ds = (dm_float - dm) * 60.0
    return f"J2000 {hh:02d}h{mm:02d}m{ss:08.5f}s {sign}{dd:02d}d{dm:02d}m{ds:07.4f}s 10.0"


def run_simobserve(
    dataset: dict[str, Any],
    model_image: pathlib.Path,
    pointings: pathlib.Path,
    project_dir: pathlib.Path,
) -> None:
    from casatasks import simobserve

    shape = dataset["shape"]
    project_dir.parent.mkdir(parents=True, exist_ok=True)
    old_cwd = pathlib.Path.cwd()
    try:
        os.chdir(project_dir.parent)
        simobserve(
            project=project_dir.name,
            skymodel=str(model_image),
            incell=f"{cell_arcsec(dataset['instrument'])}arcsec",
            incenter=f"{spw_center_frequency_hz(dataset)}Hz",
            inwidth=f"{channel_width_hz(dataset['instrument'])}Hz",
            setpointings=False,
            ptgfile=str(pointings),
            integration=f"{shape['integration_seconds']}s",
            totaltime=f"{shape['duration_seconds']}s",
            antennalist=antenna_list(dataset["instrument"]),
            thermalnoise=thermalnoise_mode(dataset),
            seed=stable_seed(dataset["id"]),
            graphics="none",
            verbose=False,
            overwrite=True,
        )
    finally:
        os.chdir(old_cwd)


def thermalnoise_mode(dataset: dict[str, Any]) -> str:
    noise_jy = float(dataset.get("source_model", {}).get("noise_simplenoise_jy", 0.0))
    return "tsys-atm" if noise_jy > 0.0 else ""


def make_preview(
    dataset: dict[str, Any],
    ms: pathlib.Path,
    preview_dir: pathlib.Path,
    preview_max_pixels: int,
) -> pathlib.Path:
    from casatasks import tclean
    from casatools import image

    imsize = min(int(dataset["shape"]["image_pixels"]), preview_max_pixels)
    imagename = preview_dir / "dirty-mfs"
    cleanup_image_products(imagename)
    tclean(
        vis=str(ms),
        imagename=str(imagename),
        datacolumn="data",
        field="" if dataset["family"] in {"mosaic", "shared-large"} else "0",
        spw=preview_spw(dataset),
        stokes="I",
        specmode="mfs",
        gridder="mosaic" if dataset["family"] in {"mosaic", "shared-large"} else "standard",
        weighting="natural",
        imsize=imsize,
        cell=f"{cell_arcsec(dataset['instrument'])}arcsec",
        niter=0,
        interactive=False,
        parallel=False,
    )
    image_path = pathlib.Path(str(imagename) + ".image")
    png_path = preview_dir / "dirty-mfs-panel.png"
    ia = image()
    ia.open(str(image_path))
    pixels = np.squeeze(ia.getchunk())
    ia.close()
    write_panel_png(dataset, pixels, png_path)
    return png_path


def write_panel_png(dataset: dict[str, Any], dirty: np.ndarray, out: pathlib.Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    model = structured_plane(min(dirty.shape), dataset["family"])
    if model.shape != dirty.shape:
        model = model[: dirty.shape[0], : dirty.shape[1]]
    diff_scale = max(float(np.nanmax(np.abs(dirty))), 1e-12)
    fig, axes = plt.subplots(1, 3, figsize=(12, 4), constrained_layout=True)
    panels = [
        ("model", model, None),
        ("dirty MFS", dirty, None),
        ("dirty / peak", dirty / diff_scale, (-1.0, 1.0)),
    ]
    for axis, (title, data, limits) in zip(axes, panels, strict=True):
        kwargs = {"origin": "lower", "cmap": "magma"}
        if limits:
            kwargs.update({"vmin": limits[0], "vmax": limits[1], "cmap": "coolwarm"})
        image = axis.imshow(np.asarray(data).T, **kwargs)
        axis.set_title(title)
        axis.set_xticks([])
        axis.set_yticks([])
        fig.colorbar(image, ax=axis, fraction=0.046)
    fig.suptitle(dataset["id"])
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150)
    plt.close(fig)


def inspect_ms(ms: pathlib.Path) -> dict[str, Any]:
    from casatools import table

    tb = table()
    tb.open(str(ms))
    nrows = int(tb.nrows())
    data_shape = list(tb.getcell("DATA", 0).shape) if nrows else []
    tb.close()
    result = {
        "path": str(ms),
        "size_bytes": du_bytes(ms),
        "main_rows": nrows,
        "data_cell_shape": data_shape,
    }
    for subtable in ["ANTENNA", "FIELD", "SPECTRAL_WINDOW", "DATA_DESCRIPTION"]:
        sub = ms / subtable
        if sub.exists():
            tb.open(str(sub))
            result[subtable.lower() + "_rows"] = int(tb.nrows())
            tb.close()
    return result


def find_single_ms(project_dir: pathlib.Path) -> pathlib.Path:
    matches = sorted(project_dir.glob("*.ms"))
    noisy = [path for path in matches if path.name.endswith(".noisy.ms")]
    if len(noisy) == 1:
        return noisy[0]
    if len(matches) != 1:
        raise RuntimeError(f"expected one MS under {project_dir}, found {matches}")
    return matches[0]


def remove_duplicate_measurement_sets(project_dir: pathlib.Path, keep: pathlib.Path) -> None:
    for path in project_dir.glob("*.ms"):
        if path != keep:
            remove_path(path)


def preview_spw(dataset: dict[str, Any]) -> str:
    channels = int(dataset["shape"]["channels"])
    if dataset["tier"] == "large" and channels > 64:
        return "0:0~63"
    return "0"


def cleanup_image_products(prefix: pathlib.Path) -> None:
    for path in prefix.parent.glob(prefix.name + ".*"):
        remove_path(path)


def remove_path(path: pathlib.Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


def du_bytes(path: pathlib.Path) -> int:
    output = subprocess.check_output(["du", "-sk", str(path.resolve())], text=True)
    return int(output.split()[0]) * 1024


def antenna_list(instrument: str) -> str:
    return "alma.cycle8.5.cfg" if instrument == "alma" else "vla.d.cfg"


def cell_arcsec(instrument: str) -> float:
    return 0.08 if instrument == "alma" else 0.35


def start_frequency_hz(instrument: str) -> float:
    return 230.0e9 if instrument == "alma" else 44.0e9


def channel_width_hz(instrument: str) -> float:
    return 2.0e6 if instrument == "alma" else 128.0e6


def spw_center_frequency_hz(dataset: dict[str, Any]) -> float:
    channels = int(dataset["shape"]["channels"])
    return start_frequency_hz(dataset["instrument"]) + (channels // 2) * channel_width_hz(
        dataset["instrument"]
    )


def stable_seed(text: str) -> int:
    import hashlib

    return int(hashlib.sha256(text.encode("utf-8")).hexdigest()[:8], 16)


def gaussian(x: float, y: float, x0: float, y0: float, sigma: float) -> float:
    return math.exp(-(((x - x0) ** 2 + (y - y0) ** 2) / (2.0 * sigma**2)))


def gaussian_array(
    x: np.ndarray, y: np.ndarray, x0: float, y0: float, sigma: float
) -> np.ndarray:
    return np.exp(-(((x - x0) ** 2 + (y - y0) ** 2) / (2.0 * sigma**2)))


def read_json(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


if __name__ == "__main__":
    main()
