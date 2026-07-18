#!/usr/bin/env python3
"""Run the ALMA First Look Imaging science-target non-interactive clean tutorial section."""

from __future__ import annotations

import argparse
import html
import json
import math
import os
import shutil
import subprocess
import textwrap
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PACK_ROOT = (
    Path.home()
    / "SoftwareProjects"
    / "casa-tutorial-data"
    / "tutorial-parity"
    / "alma"
    / "first-look"
    / "twhya"
    / "imaging"
    / "alma-first-look-imaging.pack"
)
DEFAULT_CASA_PYTHON = Path("/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python")
DEFAULT_GUI_APP_BINARY = REPO_ROOT / "apps" / "casars-mac" / ".build" / "debug" / "casars-mac"
DEFAULT_PYTHON = Path("/opt/homebrew/bin/python3.14")
DEFAULT_CASARS_BINARY = REPO_ROOT / "target" / "debug" / "casars"
DEFAULT_GHOSTTY_CAPTURE_BINARY = Path("/private/tmp/ghostty-surface-capture")
SECTION_ID = "07-science-target-auto-clean"
MS_PATH = "twhya_smoothed.cli.ms"
CASA_GUIDE_URL = "https://casaguides.nrao.edu/index.php/First_Look_at_Imaging"


def run_command(
    args: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    timeout_seconds: float | None = None,
    input_text: str | None = None,
) -> dict[str, Any]:
    start = time.perf_counter()
    try:
        result = subprocess.run(
            args,
            cwd=cwd,
            env=env,
            input=input_text,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        stdout = error.stdout.decode("utf-8", "replace") if isinstance(error.stdout, bytes) else (error.stdout or "")
        stderr = error.stderr.decode("utf-8", "replace") if isinstance(error.stderr, bytes) else (error.stderr or "")
        return {
            "args": args,
            "exit_code": None,
            "timed_out": True,
            "elapsed_seconds": time.perf_counter() - start,
            "stdout": stdout,
            "stderr": stderr,
        }
    return {
        "args": args,
        "exit_code": result.returncode,
        "timed_out": False,
        "elapsed_seconds": time.perf_counter() - start,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def require_success(label: str, run: dict[str, Any]) -> None:
    if run["exit_code"] != 0:
        raise SystemExit(
            f"{label} failed with exit {run['exit_code']}\nSTDOUT:\n{run['stdout']}\nSTDERR:\n{run['stderr']}"
        )


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_marked_json(stdout: str) -> dict[str, Any]:
    start = stdout.find("JSON_RESULT_START")
    end = stdout.find("JSON_RESULT_END")
    if start < 0 or end < 0 or end <= start:
        raise ValueError(f"marked JSON payload not found in stdout:\n{stdout}")
    return json.loads(stdout[start + len("JSON_RESULT_START") : end].strip())


def product_records(prefix: Path, pack_root: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for suffix in [".image", ".model", ".pb", ".psf", ".residual", ".sumwt"]:
        path = Path(f"{prefix}{suffix}")
        records[suffix] = {
            "path": str(path.relative_to(pack_root)),
            "exists": path.exists(),
            "size_bytes": path.stat().st_size if path.exists() else None,
        }
    return records


def gui_environment(imager_binary: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["CASARS_IMAGER_BIN"] = str(imager_binary)
    env["CASA_RS_REPO_ROOT"] = str(REPO_ROOT)
    return env


def remove_product_set(prefix: Path) -> None:
    for suffix in [
        ".image",
        ".mask",
        ".model",
        ".pb",
        ".psf",
        ".residual",
        ".sumwt",
        ".image.png",
        ".residual.png",
        ".psf.png",
        ".model.png",
        ".sumwt.png",
    ]:
        path = Path(f"{prefix}{suffix}")
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists() or path.is_symlink():
            path.unlink()


def casa_oracle_code(casa_prefix: str) -> str:
    return f"""
import json
from casatasks import tclean

def sanitize(value):
    if hasattr(value, "tolist"):
        return value.tolist()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, dict):
        return {{str(key): sanitize(item) for key, item in value.items()}}
    if isinstance(value, (list, tuple)):
        return [sanitize(item) for item in value]
    return value

result = tclean(
    vis={MS_PATH!r},
    imagename={casa_prefix!r},
    field="0",
    specmode="mfs",
    deconvolver="hogbom",
    gridder="standard",
    imsize=[250, 250],
    cell=["0.1arcsec"],
    weighting="briggs",
    robust=0.5,
    threshold="15mJy",
    niter=10000,
    interactive=False,
    mask="box[[100pix,100pix],[150pix,150pix]]",
)
print("JSON_RESULT_START")
print(json.dumps(sanitize(result), indent=2, sort_keys=True))
print("JSON_RESULT_END")
"""


def python_surface_code(python_prefix: str, casars_binary: str) -> str:
    return f"""
import json
from casars import tasks

result = tasks.imager(
    vis={MS_PATH!r},
    imagename={python_prefix!r},
    imsize=250,
    cell="0.1arcsec",
    field="0",
    phasecenter_field="0",
    specmode="mfs",
    weighting="briggs",
    robust=0.5,
    deconvolver="hogbom",
    niter=10000,
    threshold="0.015Jy",
    pblimit=0.2,
    write_pb=True,
    dirty_only=False,
    mask_box="100,100,150,150",
    binary={casars_binary!r},
)
print(json.dumps(json.loads(result.stdout), indent=2, sort_keys=True))
"""


def comparison_code(casa_prefix: str, native_prefixes: dict[str, str], comparison_png: str) -> str:
    return f"""
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from casatools import image

casa_prefix = {casa_prefix!r}
native_prefixes = json.loads({json.dumps(json.dumps(native_prefixes, sort_keys=True))})
comparison_png = Path({comparison_png!r})
products = [".image", ".residual", ".psf", ".model", ".sumwt", ".pb"]

def open_image(path):
    ia = image()
    if not Path(path).exists():
        return None
    try:
        ia.open(path)
        data = np.asarray(ia.getchunk(), dtype=np.float64)
        unit = ia.brightnessunit()
    finally:
        ia.close()
    return {{"shape": list(data.shape), "data": data, "unit": unit}}

def summarize(data):
    finite = data[np.isfinite(data)]
    if finite.size == 0:
        return {{"finite_count": 0}}
    return {{
        "finite_count": int(finite.size),
        "min": float(np.min(finite)),
        "max": float(np.max(finite)),
        "peak_abs": float(np.max(np.abs(finite))),
        "mean": float(np.mean(finite)),
        "rms": float(math.sqrt(float(np.mean(finite * finite)))),
    }}

result = {{
    "status": "passed",
    "surfaces": {{}},
    "missing_native_products": [],
    "thresholds": {{
        "core_max_rel_to_casa_peak": 5.0e-2,
        "sumwt_max_rel_to_casa_peak": 1.0e-3,
        "pb_rms_rel_to_casa_peak": 1.0e-2,
    }},
}}

for surface, prefix in native_prefixes.items():
    surface_result = {{"products": {{}}, "status": "passed"}}
    for suffix in products:
        casa_path = casa_prefix + suffix
        native_path = prefix + suffix
        casa = open_image(casa_path)
        native = open_image(native_path)
        entry = {{
            "casa_exists": casa is not None,
            "native_exists": native is not None,
            "casa_path": casa_path,
            "native_path": native_path,
        }}
        if casa is not None:
            entry["casa"] = {{"shape": casa["shape"], "unit": casa["unit"], **summarize(casa["data"])}}
        if native is not None:
            entry["native"] = {{"shape": native["shape"], "unit": native["unit"], **summarize(native["data"])}}
        if casa is None or native is None:
            if casa is not None and native is None:
                result["missing_native_products"].append({{"surface": surface, "product": suffix}})
                entry["status"] = "product-gap"
                surface_result["status"] = "product-gap"
            else:
                entry["status"] = "not-comparable"
        else:
            diff = native["data"] - casa["data"]
            peak = max(float(np.nanmax(np.abs(casa["data"]))), 1.0e-30)
            max_abs = float(np.nanmax(np.abs(diff)))
            rms_abs = float(math.sqrt(float(np.nanmean(diff * diff))))
            entry.update(
                {{
                    "status": "passed",
                    "max_abs_diff": max_abs,
                    "rms_abs_diff": rms_abs,
                    "max_rel_to_casa_peak": max_abs / peak,
                    "rms_rel_to_casa_peak": rms_abs / peak,
                }}
            )
            if suffix in [".image", ".residual", ".psf", ".model"] and entry["max_rel_to_casa_peak"] > 5.0e-2:
                entry["status"] = "failed"
                surface_result["status"] = "failed"
            if suffix == ".sumwt" and entry["max_rel_to_casa_peak"] > 1.0e-3:
                entry["status"] = "failed"
                surface_result["status"] = "failed"
            if suffix == ".pb" and entry["rms_rel_to_casa_peak"] > 1.0e-2:
                entry["status"] = "failed"
                surface_result["status"] = "failed"
        surface_result["products"][suffix] = entry
    result["surfaces"][surface] = surface_result

if result["missing_native_products"]:
    result["status"] = "product-gap"
if any(surface["status"] == "failed" for surface in result["surfaces"].values()):
    result["status"] = "failed"

fig, axes = plt.subplots(2, 3, figsize=(14, 8), constrained_layout=True)
plot_specs = [
    ("CASA clean .image", casa_prefix + ".image", None),
    ("casa-rs CLI clean .image", native_prefixes["cli"] + ".image", None),
    ("CLI - CASA clean .image", native_prefixes["cli"] + ".image", casa_prefix + ".image"),
    ("CASA clean .residual", casa_prefix + ".residual", None),
    ("casa-rs CLI clean .residual", native_prefixes["cli"] + ".residual", None),
    ("CLI - CASA clean .residual", native_prefixes["cli"] + ".residual", casa_prefix + ".residual"),
]
for ax, (title, path, diff_path) in zip(axes.ravel(), plot_specs):
    current = open_image(path)
    if current is None:
        ax.text(0.5, 0.5, "missing", ha="center", va="center")
        ax.set_title(title)
        ax.set_axis_off()
        continue
    data = np.squeeze(current["data"])
    if diff_path:
        reference = open_image(diff_path)
        data = data - np.squeeze(reference["data"])
        vmax = float(np.nanmax(np.abs(data))) or 1.0
        image_artist = ax.imshow(data, origin="lower", cmap="coolwarm", vmin=-vmax, vmax=vmax)
    else:
        image_artist = ax.imshow(data, origin="lower", cmap="inferno")
    ax.set_title(title)
    ax.set_xlabel("x pixel")
    ax.set_ylabel("y pixel")
    fig.colorbar(image_artist, ax=ax, shrink=0.8)
comparison_png.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(comparison_png, dpi=140)
plt.close(fig)

for label, path in [
    ("casa-image", casa_prefix + ".image"),
    ("casa-residual", casa_prefix + ".residual"),
    ("cli-image", native_prefixes["cli"] + ".image"),
    ("cli-residual", native_prefixes["cli"] + ".residual"),
    ("python-image", native_prefixes["python"] + ".image"),
]:
    current = open_image(path)
    if current is None:
        continue
    data = np.squeeze(current["data"])
    fig, ax = plt.subplots(figsize=(6, 5), constrained_layout=True)
    artist = ax.imshow(data, origin="lower", cmap="inferno")
    ax.set_title(label)
    ax.set_xlabel("x pixel")
    ax.set_ylabel("y pixel")
    fig.colorbar(artist, ax=ax, shrink=0.8)
    fig.savefig(Path(path + ".png"), dpi=140)
    plt.close(fig)

print("JSON_RESULT_START")
print(json.dumps(result, indent=2, sort_keys=True))
print("JSON_RESULT_END")
"""


def write_docs(pack_root: Path, evidence: dict[str, Any]) -> None:
    docs_dir = pack_root / "docs" / "sections"
    docs_dir.mkdir(parents=True, exist_ok=True)
    comparison = evidence["comparison"]
    comparison_pre = html.escape(json.dumps(comparison, indent=2, sort_keys=True))
    comparison_png = ".casa-rs/evidence/07-science-target-auto-clean-comparison.png"
    gui_run_png = ".casa-rs/screenshots/gui/07-science-target-auto-clean-gui-run.png"
    gui_parameters_png = ".casa-rs/screenshots/gui/07-science-target-auto-clean-gui-parameters.png"
    gui_imexplore_png = ".casa-rs/screenshots/gui/07-science-target-auto-clean-gui-imexplore.png"
    tui_png = ".casa-rs/screenshots/headless/07-science-target-auto-clean-tui.png"
    cli_image_png = ".casa-rs/workspace/native/07-science-target-auto-clean/twhya_cont_auto.cli.image.png"
    cli_residual_png = ".casa-rs/workspace/native/07-science-target-auto-clean/twhya_cont_auto.cli.residual.png"
    python_image_png = ".casa-rs/workspace/native/07-science-target-auto-clean/twhya_cont_auto.python.image.png"
    tui_image_png = ".casa-rs/workspace/native/07-science-target-auto-clean/twhya_cont_auto.tui.image.png"
    gui_image_png = ".casa-rs/workspace/native/07-science-target-auto-clean/twhya_cont_auto.gui.image.png"
    pb_gap = bool(comparison.get("missing_native_products"))
    status_class = "gap" if pb_gap else "ok"
    status_text = "product gap: one or more native products are not written" if pb_gap else comparison.get("status", "unknown")

    diff_rows = []
    for surface_name, surface in comparison.get("surfaces", {}).items():
        for suffix in [".image", ".residual", ".psf", ".model", ".sumwt", ".pb"]:
            product = surface.get("products", {}).get(suffix, {})
            diff_rows.append(
                {
                    "surface": surface_name,
                    "product": suffix,
                    "status": product.get("status", "not-run"),
                    "max_abs_diff": product.get("max_abs_diff"),
                    "rms_abs_diff": product.get("rms_abs_diff"),
                    "max_rel_to_casa_peak": product.get("max_rel_to_casa_peak"),
                    "casa_peak": product.get("casa", {}).get("peak_abs"),
                    "native_peak": product.get("native", {}).get("peak_abs"),
                }
            )
    diff_table_markdown = "\n".join(
        "| {surface} | `{product}` | {status} | {casa_peak} | {native_peak} | {max_abs_diff} | {rms_abs_diff} | {max_rel_to_casa_peak} |".format(
            surface=row["surface"],
            product=row["product"],
            status=row["status"],
            casa_peak="-" if row["casa_peak"] is None else f"{row['casa_peak']:.6g}",
            native_peak="-" if row["native_peak"] is None else f"{row['native_peak']:.6g}",
            max_abs_diff="-" if row["max_abs_diff"] is None else f"{row['max_abs_diff']:.6g}",
            rms_abs_diff="-" if row["rms_abs_diff"] is None else f"{row['rms_abs_diff']:.6g}",
            max_rel_to_casa_peak="-" if row["max_rel_to_casa_peak"] is None else f"{row['max_rel_to_casa_peak']:.6g}",
        )
        for row in diff_rows
    )
    diff_table_html = "\n".join(
        "<tr><td>{surface}</td><td><code>{product}</code></td><td>{status}</td><td>{casa_peak}</td><td>{native_peak}</td><td>{max_abs_diff}</td><td>{rms_abs_diff}</td><td>{max_rel_to_casa_peak}</td></tr>".format(
            surface=html.escape(row["surface"]),
            product=html.escape(row["product"]),
            status=html.escape(row["status"]),
            casa_peak="-" if row["casa_peak"] is None else f"{row['casa_peak']:.6g}",
            native_peak="-" if row["native_peak"] is None else f"{row['native_peak']:.6g}",
            max_abs_diff="-" if row["max_abs_diff"] is None else f"{row['max_abs_diff']:.6g}",
            rms_abs_diff="-" if row["rms_abs_diff"] is None else f"{row['rms_abs_diff']:.6g}",
            max_rel_to_casa_peak="-" if row["max_rel_to_casa_peak"] is None else f"{row['max_rel_to_casa_peak']:.6g}",
        )
        for row in diff_rows
    )

    markdown = f"""# 07 - Clean the science target

CASA tutorial chunk: after splitting the science target, the CASA guide uses an
interactive mask around the central point source and then shows an equivalent
non-interactive CLEAN/deconvolution run. For regression execution this section
uses that deterministic form: `niter=10000`, `threshold="15mJy"`, and a
rectangular clean mask around the source.

CASA source page: {CASA_GUIDE_URL}

## Parameters

| Surface | Task | Parameters |
| --- | --- | --- |
| CASA | `tclean` | `vis="twhya_smoothed.cli.ms"`, `imagename=".casa-rs/workspace/oracle/07-science-target-auto-clean/twhya_cont_auto.casa"`, `field="0"`, auto data-column selection, `specmode="mfs"`, `deconvolver="hogbom"`, `gridder="standard"`, `imsize=[250,250]`, `cell=["0.1arcsec"]`, `weighting="briggs"`, `robust=0.5`, `niter=10000`, `threshold="15mJy"`, `interactive=False`, `mask="box[[100pix,100pix],[150pix,150pix]]"` |
| CLI | `casars-imager` | `--ms twhya_smoothed.cli.ms --imagename .casa-rs/workspace/native/07-science-target-auto-clean/twhya_cont_auto.cli --imsize 250 --cell-arcsec 0.1 --field 0 --phasecenter-field 0 --specmode mfs --deconvolver hogbom --weighting briggs --robust 0.5 --threshold-jy 0.015 --niter 10000 --mask-box 100,100,150,150 --pblimit 0.2 --write-pb` |
| Python | `casars.tasks.imager` | `vis="twhya_smoothed.cli.ms"`, `imagename=".casa-rs/workspace/native/07-science-target-auto-clean/twhya_cont_auto.python"`, `imsize=250`, `cell="0.1arcsec"`, `field="0"`, `phasecenter_field="0"`, `weighting="briggs"`, `robust=0.5`, `deconvolver="hogbom"`, `niter=10000`, `threshold="0.015Jy"`, `pblimit=0.2`, `write_pb=True`, `dirty_only=False`, `mask_box="100,100,150,150"` |
| TUI | `casars imager` | Same parameter values as CLI; the captured run presses `r` twice to confirm execution. |
| GUI | `Tasks > Imager` and `Image Explorer` | Same parameter values as CLI; the GUI task is run with `write_pb=True`, then Image Explorer opens the GUI-produced clean image. |

## Observable Result

The science-target non-interactive clean image should retain the compact central source but
with substantially reduced dirty-beam sidelobe structure. The residual image
should have a lower peak than the dirty image, while the model image contains
the clean components inside the mask.

## Evidence

![CASA vs casa-rs clean image comparison](../../{comparison_png})

![casa-rs GUI imager parameters](../../{gui_parameters_png})

![casa-rs GUI imager completed run](../../{gui_run_png})

![casa-rs GUI Image Explorer clean image](../../{gui_imexplore_png})

![casa-rs TUI imager clean run](../../{tui_png})

![casa-rs CLI clean image preview](../../{cli_image_png})

![casa-rs CLI clean residual preview](../../{cli_residual_png})

![casa-rs Python clean image preview](../../{python_image_png})

![casa-rs TUI clean image preview](../../{tui_image_png})

![casa-rs GUI clean image preview](../../{gui_image_png})

## Numeric/Product Comparison

Status: `{status_text}`.

| Surface | Product | Status | CASA peak | Native peak | Peak abs diff | RMS diff | Peak rel to CASA peak |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
{diff_table_markdown}

```json
{json.dumps(comparison, indent=2, sort_keys=True)}
```
"""
    (docs_dir / f"{SECTION_ID}.md").write_text(markdown, encoding="utf-8")

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ALMA First Look Imaging - 07 Clean science target</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; line-height: 1.45; margin: 2rem; max-width: 1280px; }}
table {{ border-collapse: collapse; width: 100%; }}
td, th {{ border: 1px solid #c7c7c7; padding: 0.45rem; vertical-align: top; }}
pre {{ background: #f6f6f6; border: 1px solid #ddd; padding: 1rem; overflow: auto; }}
img {{ max-width: 100%; border: 1px solid #ddd; }}
.ok {{ color: #176b2c; font-weight: 600; }}
.gap {{ color: #8a5a00; font-weight: 600; }}
.grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; }}
.wide {{ grid-column: 1 / -1; }}
</style>
</head>
<body>
<h1>ALMA First Look Imaging: 07 Clean science target</h1>
<p>The CASA guide moves from the science-target split to a mask-driven CLEAN. This evidence run uses the non-interactive CASA equivalent with <code>niter=10000</code>, <code>threshold="15mJy"</code>, and <code>mask="box[[100pix,100pix],[150pix,150pix]]"</code>.</p>
<p>CASA source page: <a href="{CASA_GUIDE_URL}">{CASA_GUIDE_URL}</a></p>
<h2>Parameters</h2>
<table>
<tr><th>Surface</th><th>Task</th><th>Parameters</th></tr>
<tr><td>CASA</td><td><code>tclean</code></td><td><code>field=0</code>, auto data-column selection, <code>imsize=[250,250]</code>, <code>cell=["0.1arcsec"]</code>, <code>weighting=briggs</code>, <code>robust=0.5</code>, <code>niter=10000</code>, <code>threshold=15mJy</code>, <code>mask=box[[100pix,100pix],[150pix,150pix]]</code></td></tr>
<tr><td>CLI</td><td><code>casars-imager</code></td><td><code>--field 0 --phasecenter-field 0 --imsize 250 --cell-arcsec 0.1 --weighting briggs --robust 0.5 --niter 10000 --threshold-jy 0.015 --mask-box 100,100,150,150 --pblimit 0.2 --write-pb</code></td></tr>
<tr><td>Python</td><td><code>casars.tasks.imager</code></td><td><code>field=0</code>, <code>phasecenter_field=0</code>, <code>niter=10000</code>, <code>threshold=0.015Jy</code>, <code>pblimit=0.2</code>, <code>write_pb=True</code>, <code>mask_box=100,100,150,150</code></td></tr>
<tr><td>TUI</td><td><code>casars imager</code></td><td>Same parameter values as CLI; captured through <code>tools/ghostty-surface-capture</code>.</td></tr>
<tr><td>GUI</td><td><code>Tasks &gt; Imager</code></td><td>Same parameter values as CLI; the GUI task is run and the GUI-produced image is opened in Image Explorer.</td></tr>
</table>
<h2>Observable Result</h2>
<p>The cleaned image should show the same compact science target with reduced sidelobe structure relative to the dirty image. The model should be non-zero inside the mask, and the residual should be lower than the dirty residual.</p>
<h2>Evidence</h2>
<div class="grid">
<figure class="wide"><img src="../../{comparison_png}" alt="CASA vs casa-rs clean comparison"><figcaption>CASA oracle and casa-rs clean image/residual comparison.</figcaption></figure>
<figure><img src="../../{gui_parameters_png}" alt="GUI clean imager parameters"><figcaption>GUI parameters for the clean run.</figcaption></figure>
<figure><img src="../../{gui_run_png}" alt="GUI clean imager completed run"><figcaption>GUI task after running the clean imager.</figcaption></figure>
<figure><img src="../../{gui_imexplore_png}" alt="GUI clean image explorer"><figcaption>GUI Image Explorer on the cleaned image.</figcaption></figure>
<figure><img src="../../{tui_png}" alt="TUI clean imager run"><figcaption>TUI imager run after confirmed execution.</figcaption></figure>
<figure><img src="../../{cli_image_png}" alt="CLI clean image"><figcaption>CLI cleaned image preview.</figcaption></figure>
<figure><img src="../../{cli_residual_png}" alt="CLI clean residual"><figcaption>CLI residual preview.</figcaption></figure>
<figure><img src="../../{python_image_png}" alt="Python clean image"><figcaption>Python cleaned image preview.</figcaption></figure>
<figure><img src="../../{tui_image_png}" alt="TUI clean image"><figcaption>TUI cleaned image preview.</figcaption></figure>
<figure><img src="../../{gui_image_png}" alt="GUI clean image"><figcaption>GUI cleaned image preview.</figcaption></figure>
</div>
<h2>Numeric/Product Comparison</h2>
<p class="{status_class}">Status: {html.escape(status_text)}</p>
<table>
<tr><th>Surface</th><th>Product</th><th>Status</th><th>CASA peak</th><th>Native peak</th><th>Peak abs diff</th><th>RMS diff</th><th>Peak rel to CASA peak</th></tr>
{diff_table_html}
</table>
<h2>Comparison JSON</h2>
<pre>{comparison_pre}</pre>
</body>
</html>
"""
    (docs_dir / f"{SECTION_ID}.html").write_text(html_doc, encoding="utf-8")


def update_pack_manifest(pack_root: Path) -> None:
    path = pack_root / "pack.json"
    if not path.exists():
        return
    manifest = json.loads(path.read_text(encoding="utf-8"))
    sections = manifest.setdefault("sections", [])
    sections = [section for section in sections if section.get("id") != SECTION_ID]
    template_path = REPO_ROOT / "resources" / "tutorial-packs" / "alma-first-look-imaging.template.json"
    template = json.loads(template_path.read_text(encoding="utf-8"))
    template_section = next(section for section in template["sections"] if section["id"] == SECTION_ID)
    sections.append(template_section)
    manifest["sections"] = sorted(sections, key=lambda section: section.get("sequence", 999))
    write_json(path, manifest)


def run_section(args: argparse.Namespace) -> dict[str, Any]:
    pack_root = args.pack_root.expanduser().resolve()
    imager_binary = args.imager_binary.expanduser().resolve()
    casars_binary = args.casars_binary.expanduser().resolve()
    gui_app_binary = args.gui_app_binary.expanduser().resolve()
    gui_env = gui_environment(imager_binary)
    casa_python = args.casa_python.expanduser()
    python = args.python.expanduser()

    native_dir = pack_root / ".casa-rs" / "workspace" / "native" / SECTION_ID
    oracle_dir = pack_root / ".casa-rs" / "workspace" / "oracle" / SECTION_ID
    evidence_dir = pack_root / ".casa-rs" / "evidence"
    gui_dir = pack_root / ".casa-rs" / "screenshots" / "gui"
    headless_dir = pack_root / ".casa-rs" / "screenshots" / "headless"
    for directory in [native_dir, oracle_dir, evidence_dir, gui_dir, headless_dir]:
        directory.mkdir(parents=True, exist_ok=True)
    update_pack_manifest(pack_root)

    casa_prefix = oracle_dir / "twhya_cont_auto.casa"
    cli_prefix = native_dir / "twhya_cont_auto.cli"
    python_prefix = native_dir / "twhya_cont_auto.python"
    tui_prefix = native_dir / "twhya_cont_auto.tui"
    gui_prefix = native_dir / "twhya_cont_auto.gui"
    for prefix in [casa_prefix, cli_prefix, python_prefix, tui_prefix, gui_prefix]:
        remove_product_set(prefix)

    env = os.environ.copy()
    env.setdefault("DISPLAY", ":99")
    env.setdefault("QT_QPA_PLATFORM", "offscreen")
    env.setdefault("MPLBACKEND", "Agg")
    env.setdefault("MPLCONFIGDIR", str(pack_root / ".casa-rs" / "workspace" / "oracle" / "matplotlib"))

    oracle_run = run_command(
        [str(casa_python), "-c", casa_oracle_code(str(casa_prefix))],
        cwd=pack_root,
        env=env,
        timeout_seconds=args.timeout_seconds,
    )
    require_success("CASA tclean oracle", oracle_run)
    oracle_result = parse_marked_json(oracle_run["stdout"])

    cli_args = [
        str(imager_binary),
        "--ms",
        MS_PATH,
        "--imagename",
        str(cli_prefix.relative_to(pack_root)),
        "--imsize",
        "250",
        "--cell-arcsec",
        "0.1",
        "--field",
        "0",
        "--phasecenter-field",
        "0",
        "--specmode",
        "mfs",
        "--deconvolver",
        "hogbom",
        "--weighting",
        "briggs",
        "--robust",
        "0.5",
        "--threshold-jy",
        "0.015",
        "--niter",
        "10000",
        "--mask-box",
        "100,100,150,150",
        "--pblimit",
        "0.2",
        "--write-pb",
    ]
    cli_run = run_command(cli_args, cwd=pack_root, env=os.environ.copy(), timeout_seconds=args.timeout_seconds)
    require_success("casa-rs CLI imager", cli_run)

    python_env = os.environ.copy()
    python_env["PYTHONPATH"] = (
        str(REPO_ROOT / "crates" / "casars-python" / "python")
        + os.pathsep
        + python_env.get("PYTHONPATH", "")
    )
    python_env["CASARS_IMAGER_BIN"] = str(imager_binary)
    python_run = run_command(
        [str(python), "-c", python_surface_code(str(python_prefix.relative_to(pack_root)), str(args.casars_binary))],
        cwd=pack_root,
        env=python_env,
        timeout_seconds=args.timeout_seconds,
    )
    require_success("casa-rs Python imager", python_run)

    tui_capture_run = run_command(
        [
            str(args.ghostty_capture_binary),
            "--cwd",
            str(pack_root),
            "--output",
            str(headless_dir / "07-science-target-auto-clean-tui.png"),
            "--width",
            "2400",
            "--height",
            "1600",
            "--font-size",
            "12",
            "--settle-seconds",
            str(min(max(30, args.timeout_seconds * 0.25), 90)),
            "--input-event",
            "500:r",
            "--input-event",
            "1500:r",
            "--input-event",
            "3500:r",
            "--",
            str(casars_binary),
            "imager",
            "--ms",
            MS_PATH,
            "--imagename",
            str(tui_prefix.relative_to(pack_root)),
            "--imsize",
            "250",
            "--cell-arcsec",
            "0.1",
            "--field",
            "0",
            "--phasecenter-field",
            "0",
            "--specmode",
            "mfs",
            "--deconvolver",
            "hogbom",
            "--weighting",
            "briggs",
            "--robust",
            "0.5",
            "--threshold-jy",
            "0.015",
            "--niter",
            "10000",
            "--mask-box",
            "100,100,150,150",
            "--pblimit",
            "0.2",
            "--write-pb",
        ],
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        timeout_seconds=args.timeout_seconds + 30,
    )
    require_success("casa-rs TUI imager capture", tui_capture_run)

    gui_run = run_command(
        [
            str(gui_app_binary),
            "--capture-gui-evidence",
            "--capture-kind",
            "imager-run",
            "--open-tutorial-pack",
            str(pack_root),
            "--dataset",
            MS_PATH,
            "--field",
            "0",
            "--imagename",
            str(gui_prefix.relative_to(pack_root)),
            "--niter",
            "10000",
            "--threshold-jy",
            "0.015",
            "--mask-box",
            "100,100,150,150",
            "--pblimit",
            "0.2",
            "--write-pb",
            "true",
            "--dirty-only",
            "false",
            "--output",
            str(gui_dir / "07-science-target-auto-clean-gui-run.png"),
            "--width",
            "1800",
            "--height",
            "1200",
        ],
        cwd=REPO_ROOT,
        env=gui_env,
        timeout_seconds=args.timeout_seconds + 60,
    )
    require_success("GUI imager run", gui_run)

    comparison_run = run_command(
        [
            str(casa_python),
            "-c",
            comparison_code(
                str(casa_prefix),
                {
                    "cli": str(cli_prefix),
                    "python": str(python_prefix),
                    "tui": str(tui_prefix),
                    "gui": str(gui_prefix),
                },
                str(evidence_dir / "07-science-target-auto-clean-comparison.png"),
            ),
        ],
        cwd=pack_root,
        env=env,
        timeout_seconds=args.timeout_seconds,
    )
    require_success("clean image comparison", comparison_run)
    comparison = parse_marked_json(comparison_run["stdout"])

    gui_parameters_run = run_command(
        [
            str(gui_app_binary),
            "--capture-gui-evidence",
            "--capture-kind",
            "imager-parameters",
            "--open-tutorial-pack",
            str(pack_root),
            "--dataset",
            MS_PATH,
            "--field",
            "0",
            "--imagename",
            str(cli_prefix.relative_to(pack_root)),
            "--niter",
            "10000",
            "--threshold-jy",
            "0.015",
            "--mask-box",
            "100,100,150,150",
            "--pblimit",
            "0.2",
            "--write-pb",
            "true",
            "--dirty-only",
            "false",
            "--output",
            str(gui_dir / "07-science-target-auto-clean-gui-parameters.png"),
            "--width",
            "1800",
            "--height",
            "1200",
        ],
        cwd=REPO_ROOT,
        env=gui_env,
        timeout_seconds=60,
    )
    require_success("GUI imager parameter screenshot", gui_parameters_run)

    gui_imexplore_run = run_command(
        [
            str(gui_app_binary),
            "--capture-gui-evidence",
            "--capture-kind",
            "image-explorer",
            "--open-tutorial-pack",
            str(pack_root),
            "--dataset",
            MS_PATH,
            "--image",
            str(Path(f"{gui_prefix}.image")),
            "--output",
            str(gui_dir / "07-science-target-auto-clean-gui-imexplore.png"),
            "--width",
            "1800",
            "--height",
            "1200",
        ],
        cwd=REPO_ROOT,
        env=gui_env,
        timeout_seconds=60,
    )
    require_success("GUI image explorer screenshot", gui_imexplore_run)

    evidence = {
        "section_id": SECTION_ID,
        "status": comparison["status"],
        "parameters": {
            "field": "0",
            "imsize": 250,
            "cell_arcsec": 0.1,
            "weighting": "briggs",
            "robust": 0.5,
            "niter": 10000,
            "threshold_jy": 0.015,
            "write_pb": True,
            "mask_box": [100, 100, 150, 150],
        },
        "oracle": {
            "run": oracle_run,
            "result": oracle_result,
            "products": product_records(casa_prefix, pack_root),
        },
        "cli": {
            "run": cli_run,
            "products": product_records(cli_prefix, pack_root),
        },
        "python": {
            "run": python_run,
            "result": json.loads(python_run["stdout"]),
            "products": product_records(python_prefix, pack_root),
        },
        "tui": {
            "run": tui_capture_run,
            "screenshot": str((headless_dir / "07-science-target-auto-clean-tui.png").relative_to(pack_root)),
            "products": product_records(tui_prefix, pack_root),
        },
        "gui": {
            "run_screenshot": str((gui_dir / "07-science-target-auto-clean-gui-run.png").relative_to(pack_root)),
            "parameters_screenshot": str((gui_dir / "07-science-target-auto-clean-gui-parameters.png").relative_to(pack_root)),
            "imexplore_screenshot": str((gui_dir / "07-science-target-auto-clean-gui-imexplore.png").relative_to(pack_root)),
            "products": product_records(gui_prefix, pack_root),
            "runs": {
                "run": gui_run,
                "parameters": gui_parameters_run,
                "imexplore": gui_imexplore_run,
            },
        },
        "comparison": comparison,
    }

    write_json(evidence_dir / f"{SECTION_ID}.json", evidence)
    write_json(
        pack_root / ".casa-rs" / "evidence" / "review" / f"{SECTION_ID}.json",
        {
            "schema_version": "tutorial-pack-review.v0",
            "section_id": SECTION_ID,
            "status": "pending-human-review",
            "human_evaluation": {
                "outcome": "pending",
                "comments": "Ready for review after CASA/casa-rs clean image numeric comparison and GUI screenshots are inspected.",
                "required_changes": [],
            },
            "evidence": {
                "html": f"docs/sections/{SECTION_ID}.html",
                "json": f".casa-rs/evidence/{SECTION_ID}.json",
            },
        },
    )
    write_docs(pack_root, evidence)
    update_pack_manifest(pack_root)
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pack-root", type=Path, default=DEFAULT_PACK_ROOT)
    parser.add_argument("--casa-python", type=Path, default=DEFAULT_CASA_PYTHON)
    parser.add_argument("--python", type=Path, default=DEFAULT_PYTHON)
    parser.add_argument("--imager-binary", type=Path, default=REPO_ROOT / "target" / "debug" / "casars-imager")
    parser.add_argument("--casars-binary", type=Path, default=DEFAULT_CASARS_BINARY)
    parser.add_argument("--gui-app-binary", type=Path, default=DEFAULT_GUI_APP_BINARY)
    parser.add_argument("--ghostty-capture-binary", type=Path, default=DEFAULT_GHOSTTY_CAPTURE_BINARY)
    parser.add_argument("--timeout-seconds", type=float, default=600.0)
    args = parser.parse_args()
    evidence = run_section(args)
    print(json.dumps({"section_id": SECTION_ID, "status": evidence["status"]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
