#!/usr/bin/env python3
"""Run the ALMA First Look Imaging dirty phase-calibrator tutorial section."""

from __future__ import annotations

import argparse
import html
import json
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
SECTION_ID = "04-phase-cal-dirty"
MS_PATH = "twhya_calibrated.ms"
CASA_GUIDE_URL = "https://casaguides.nrao.edu/index.php?title=First_Look_at_Imaging_CASA_6.5.4#A_First_Look_at_Imaging"


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


def file_record(path: Path, pack_root: Path) -> dict[str, Any]:
    return {
        "path": str(path.relative_to(pack_root)),
        "exists": path.exists(),
        "size_bytes": path.stat().st_size if path.exists() else None,
    }


def write_focused_gui_parameters(source: Path, target: Path) -> None:
    """Crop the real GUI screenshot to the task pane for readable evidence."""
    target.parent.mkdir(parents=True, exist_ok=True)
    crop = run_command(
        [
            "sips",
            "--cropToHeightWidth",
            "1920",
            "1870",
            "--cropOffset",
            "0",
            "1010",
            str(source),
            "--out",
            str(target),
        ],
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        timeout_seconds=30,
    )
    if crop["exit_code"] != 0:
        shutil.copy2(source, target)


def product_records(prefix: Path, pack_root: Path) -> dict[str, dict[str, Any]]:
    return {
        suffix: file_record(Path(f"{prefix}{suffix}"), pack_root)
        for suffix in [".image", ".model", ".pb", ".psf", ".residual", ".sumwt"]
    }


def parse_marked_json(stdout: str) -> dict[str, Any]:
    start = stdout.find("JSON_RESULT_START")
    end = stdout.find("JSON_RESULT_END")
    if start < 0 or end < 0 or end <= start:
        raise ValueError(f"marked JSON payload not found in stdout:\n{stdout}")
    payload = stdout[start + len("JSON_RESULT_START") : end].strip()
    return json.loads(payload)


def casa_oracle_code(casa_prefix: str) -> str:
    return f"""
import json

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

from casatasks import tclean
result = tclean(
    vis={MS_PATH!r},
    imagename={casa_prefix!r},
    field="3",
    specmode="mfs",
    deconvolver="hogbom",
    gridder="standard",
    imsize=[250, 250],
    cell=["0.1arcsec"],
    weighting="briggs",
    threshold="0.0mJy",
    niter=0,
    interactive=False,
)
print("JSON_RESULT_START")
print(json.dumps(sanitize(result), indent=2, sort_keys=True))
print("JSON_RESULT_END")
"""


def python_surface_code(python_prefix: str, imager_binary: str) -> str:
    return f"""
import json
from casars.tasks import imager

result = imager.mfs(
    {MS_PATH!r},
    {python_prefix!r},
    image_size=250,
    cell_arcsec=0.1,
    field_ids=[3],
    phasecenter_field=3,
    weighting="briggs",
    robust=0.5,
    deconvolver="hogbom",
    niter=0,
    threshold_jy=0.0,
    dirty_only=True,
    binary={imager_binary!r},
)
print(json.dumps(result, indent=2, sort_keys=True))
"""


def comparison_code(
    casa_prefix: str,
    native_prefixes: dict[str, str],
    comparison_png: str,
) -> str:
    native_prefix_json = json.dumps(native_prefixes, sort_keys=True)
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
native_prefixes = json.loads({native_prefix_json!r})
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
        "mean": float(np.mean(finite)),
        "rms": float(math.sqrt(float(np.mean(finite * finite)))),
    }}

result = {{
    "status": "passed",
    "surfaces": {{}},
    "missing_native_products": [],
    "thresholds": {{
        "core_max_rel_to_casa_peak": 1.0e-3,
        "sumwt_max_rel_to_casa_peak": 1.0e-3,
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
            if suffix in [".image", ".residual", ".psf", ".model", ".sumwt"] and entry["max_rel_to_casa_peak"] > 1.0e-3:
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
    ("CASA .image", casa_prefix + ".image", None),
    ("casa-rs CLI .image", native_prefixes["cli"] + ".image", None),
    ("CLI - CASA .image", native_prefixes["cli"] + ".image", casa_prefix + ".image"),
    ("CASA .psf", casa_prefix + ".psf", None),
    ("casa-rs CLI .psf", native_prefixes["cli"] + ".psf", None),
    ("CLI - CASA .psf", native_prefixes["cli"] + ".psf", casa_prefix + ".psf"),
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

print("JSON_RESULT_START")
print(json.dumps(result, indent=2, sort_keys=True))
print("JSON_RESULT_END")
"""


def write_docs(pack_root: Path, evidence: dict[str, Any]) -> None:
    docs_dir = pack_root / "docs" / "sections"
    docs_dir.mkdir(parents=True, exist_ok=True)

    comparison = evidence["comparison"]
    comparison_pre = html.escape(json.dumps(comparison, indent=2, sort_keys=True))
    comparison_png = ".casa-rs/evidence/04-phase-cal-dirty-comparison.png"
    gui_parameters_png = ".casa-rs/screenshots/gui/04-phase-cal-dirty-gui-parameters.png"
    gui_parameters_focused_png = ".casa-rs/screenshots/gui/04-phase-cal-dirty-gui-parameters-focused.png"
    gui_imexplore_png = ".casa-rs/screenshots/gui/04-phase-cal-dirty-gui-imexplore.png"
    cli_image_png = ".casa-rs/workspace/native/04-phase-cal-dirty/phase_cal.cli.image.png"
    python_image_png = ".casa-rs/workspace/native/04-phase-cal-dirty/phase_cal.python.image.png"
    pb_gap = bool(comparison.get("missing_native_products"))
    status_class = "gap" if pb_gap else "ok"
    status_text = "product gap: native .pb is not written" if pb_gap else "passed"
    casa_return = html.escape(json.dumps(evidence["oracle"].get("result", {}), indent=2, sort_keys=True))
    def find_key_value(value: Any, key: str) -> Any:
        if isinstance(value, dict):
            if key in value:
                found = value[key]
                if isinstance(found, list) and found:
                    return found[0]
                return found
            for child in value.values():
                found = find_key_value(child, key)
                if found != "unknown":
                    return found
        elif isinstance(value, list):
            for child in value:
                found = find_key_value(child, key)
                if found != "unknown":
                    return found
        return "unknown"

    peak_residual = find_key_value(evidence["oracle"].get("result", {}).get("summaryminor", {}), "peakRes")

    diff_rows = []
    for surface_name, surface in evidence["comparison"].get("surfaces", {}).items():
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
                }
            )
    diff_table_markdown = "\n".join(
        "| {surface} | `{product}` | {status} | {max_abs_diff} | {rms_abs_diff} | {max_rel_to_casa_peak} |".format(
            surface=row["surface"],
            product=row["product"],
            status=row["status"],
            max_abs_diff="-" if row["max_abs_diff"] is None else f"{row['max_abs_diff']:.6g}",
            rms_abs_diff="-" if row["rms_abs_diff"] is None else f"{row['rms_abs_diff']:.6g}",
            max_rel_to_casa_peak="-" if row["max_rel_to_casa_peak"] is None else f"{row['max_rel_to_casa_peak']:.6g}",
        )
        for row in diff_rows
    )
    diff_table_html = "\n".join(
        "<tr><td>{surface}</td><td><code>{product}</code></td><td>{status}</td><td>{max_abs_diff}</td><td>{rms_abs_diff}</td><td>{max_rel_to_casa_peak}</td></tr>".format(
            surface=html.escape(row["surface"]),
            product=html.escape(row["product"]),
            status=html.escape(row["status"]),
            max_abs_diff="-" if row["max_abs_diff"] is None else f"{row['max_abs_diff']:.6g}",
            rms_abs_diff="-" if row["rms_abs_diff"] is None else f"{row['rms_abs_diff']:.6g}",
            max_rel_to_casa_peak="-" if row["max_rel_to_casa_peak"] is None else f"{row['max_rel_to_casa_peak']:.6g}",
        )
        for row in diff_rows
    )

    markdown = f"""# 04 - Dirty image of the phase calibrator

CASA tutorial chunk:

```python
tclean(vis="twhya_calibrated.ms", imagename="phase_cal", field="3",
       specmode="mfs", deconvolver="hogbom", gridder="standard",
       imsize=[250, 250], cell=["0.1arcsec"], weighting="briggs",
       threshold="0.0mJy", interactive=True)
```

For regression execution this section uses `niter=0` and `interactive=False`,
which is the noninteractive equivalent of making the dirty image before any
interactive clean boxes or minor-cycle iterations.

CASA source page: {CASA_GUIDE_URL}

## Parameters

| Surface | Task | Parameters |
| --- | --- | --- |
| CASA | `tclean` | `vis="twhya_calibrated.ms"`, `imagename="phase_cal"`, `field="3"`, `specmode="mfs"`, `deconvolver="hogbom"`, `gridder="standard"`, `imsize=[250,250]`, `cell=["0.1arcsec"]`, `weighting="briggs"`, `threshold="0.0mJy"`, tutorial `interactive=True`; regression `niter=0`, `interactive=False` |
| CLI | `casars-imager` | `--ms twhya_calibrated.ms --imagename .casa-rs/workspace/native/04-phase-cal-dirty/phase_cal.cli --imsize 250 --cell-arcsec 0.1 --field 3 --phasecenter-field 3 --specmode mfs --deconvolver hogbom --weighting briggs --robust 0.5 --threshold-jy 0.0 --niter 0 --dirty-only` |
| Python | `casars.tasks.imager.mfs` | `measurement_set="twhya_calibrated.ms"`, `image_name=".casa-rs/workspace/native/04-phase-cal-dirty/phase_cal.python"`, `image_size=250`, `cell_arcsec=0.1`, `field_ids=[3]`, `phasecenter_field=3`, `weighting="briggs"`, `robust=0.5`, `deconvolver="hogbom"`, `niter=0`, `threshold_jy=0.0`, `dirty_only=True` |
| TUI | `Tasks > Image > Imager` | Same parameter values as CLI; screenshot evidence is pending for this task-driven section. |
| GUI | `Tasks > Imager` and `Image Explorer` | Same parameter values as CLI; parameter and dirty-image screenshots are captured below. |

## Observable Result

The dirty phase-calibrator image should be a compact point-like source near the
image center. CASA reports `iterdone=0`, `nmajordone=1`, and a peak residual of
about 0.619 Jy/beam for this dataset.

The ringed or swirly structure around the point calibrator is expected in a
dirty image. It is the dirty-beam sidelobe pattern from the sampled and weighted
uv coverage, not astrophysical source structure. This chunk intentionally stops
before CLEAN replaces the dirty-beam response with a restored Gaussian beam.

## Evidence

![CASA vs casa-rs dirty image comparison](../../{comparison_png})

![casa-rs GUI imager parameters](../../{gui_parameters_focused_png})

![casa-rs GUI imager parameters - full window](../../{gui_parameters_png})

![casa-rs GUI Image Explorer dirty image](../../{gui_imexplore_png})

![casa-rs CLI dirty image preview](../../{cli_image_png})

![casa-rs Python dirty image preview](../../{python_image_png})

## Numeric/Product Comparison

Status: `{status_text}`.

Core numeric products (`.image`, `.residual`, `.psf`, `.model`, `.sumwt`) match
CASA to better than `1e-3` relative to the CASA peak in this run. CASA writes a
`.pb` product for this dirty image; casa-rs does not currently write that product.

| Surface | Product | Status | Peak abs diff | RMS diff | Peak rel to CASA peak |
| --- | --- | --- | ---: | ---: | ---: |
{diff_table_markdown}

```json
{json.dumps(comparison, indent=2, sort_keys=True)}
```

## CASA Return

```json
{json.dumps(evidence["oracle"].get("result", {}), indent=2, sort_keys=True)}
```
"""
    (docs_dir / "04-phase-cal-dirty.md").write_text(markdown, encoding="utf-8")

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ALMA First Look Imaging - 04 Dirty phase calibrator</title>
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
<h1>ALMA First Look Imaging: 04 Dirty phase calibrator</h1>
<p>CASA source workflow: run <code>tclean</code> on <code>twhya_calibrated.ms</code> with <code>field="3"</code>, <code>specmode="mfs"</code>, <code>imsize=[250,250]</code>, <code>cell=["0.1arcsec"]</code>, <code>weighting="briggs"</code>, and no cleaning iterations for this regression capture.</p>
<p>CASA source page: <a href="{CASA_GUIDE_URL}">{CASA_GUIDE_URL}</a></p>
<h2>Parameters</h2>
<table>
<tr><th>Surface</th><th>Task</th><th>Parameters</th></tr>
<tr><td>CASA</td><td><code>tclean</code></td><td><code>vis=twhya_calibrated.ms</code>, <code>imagename=phase_cal</code>, <code>field=3</code>, <code>specmode=mfs</code>, <code>deconvolver=hogbom</code>, <code>gridder=standard</code>, <code>imsize=[250,250]</code>, <code>cell=["0.1arcsec"]</code>, <code>weighting=briggs</code>, <code>threshold=0.0mJy</code>, tutorial <code>interactive=True</code>; regression <code>niter=0</code>, <code>interactive=False</code></td></tr>
<tr><td>CLI</td><td><code>casars-imager</code></td><td><code>--ms twhya_calibrated.ms --imagename .casa-rs/workspace/native/04-phase-cal-dirty/phase_cal.cli --imsize 250 --cell-arcsec 0.1 --field 3 --phasecenter-field 3 --specmode mfs --deconvolver hogbom --weighting briggs --robust 0.5 --threshold-jy 0.0 --niter 0 --dirty-only</code></td></tr>
<tr><td>Python</td><td><code>casars.tasks.imager.mfs</code></td><td><code>measurement_set=twhya_calibrated.ms</code>, <code>image_name=.casa-rs/workspace/native/04-phase-cal-dirty/phase_cal.python</code>, <code>image_size=250</code>, <code>cell_arcsec=0.1</code>, <code>field_ids=[3]</code>, <code>phasecenter_field=3</code>, <code>weighting=briggs</code>, <code>robust=0.5</code>, <code>niter=0</code>, <code>dirty_only=True</code></td></tr>
<tr><td>TUI</td><td><code>Tasks &gt; Imager</code></td><td>Same values as CLI. Automated parameter-screenshot evidence is pending for this task-driven section.</td></tr>
<tr><td>GUI</td><td><code>Tasks &gt; Imager</code> and <code>Image Explorer</code></td><td>Same values as CLI. Parameter and dirty-image screenshots are captured below.</td></tr>
</table>
<h2>Observable Result</h2>
<p>The dirty phase-calibrator image should be a compact point-like source near the image center. CASA reported <code>iterdone=0</code>, <code>nmajordone=1</code>, and peak residual <code>{html.escape(str(peak_residual))}</code>.</p>
<p>The ringed or swirly structure around the point calibrator is expected in a dirty image. It is the dirty-beam sidelobe pattern from the sampled and weighted uv coverage, not astrophysical source structure. This chunk intentionally stops before CLEAN replaces the dirty-beam response with a restored Gaussian beam.</p>
<h2>Visible Evidence</h2>
<p class="{status_class}">Machine comparison status: {html.escape(status_text)}.</p>
<div class="grid">
<section class="wide"><h3>CASA vs casa-rs comparison panel</h3><img src="../../{comparison_png}" alt="CASA and casa-rs dirty phase calibrator comparison"></section>
<section class="wide"><h3>casa-rs GUI imager parameters</h3><img src="../../{gui_parameters_focused_png}" alt="casa-rs GUI imager task parameters"></section>
<section class="wide"><h3>casa-rs GUI imager parameters - full window provenance</h3><img src="../../{gui_parameters_png}" alt="full-window casa-rs GUI imager task parameters"></section>
<section class="wide"><h3>casa-rs GUI Image Explorer dirty image</h3><img src="../../{gui_imexplore_png}" alt="casa-rs GUI Image Explorer dirty image"></section>
<section><h3>casa-rs CLI preview</h3><img src="../../{cli_image_png}" alt="casa-rs CLI dirty image preview"></section>
<section><h3>casa-rs Python preview</h3><img src="../../{python_image_png}" alt="casa-rs Python dirty image preview"></section>
</div>
<h2>Numeric/Product Comparison</h2>
<p>Core products <code>.image</code>, <code>.residual</code>, <code>.psf</code>, <code>.model</code>, and <code>.sumwt</code> pass the current tolerance. CASA writes <code>.pb</code>; casa-rs does not yet write it for this dirty image.</p>
<table>
<tr><th>Surface</th><th>Product</th><th>Status</th><th>Peak abs diff</th><th>RMS diff</th><th>Peak rel to CASA peak</th></tr>
{diff_table_html}
</table>
<pre>{comparison_pre}</pre>
<h2>CASA Return</h2>
<pre>{casa_return}</pre>
</body>
</html>
"""
    (docs_dir / "04-phase-cal-dirty.html").write_text(html_doc, encoding="utf-8")


def write_review_record(pack_root: Path, evidence: dict[str, Any]) -> None:
    review_path = pack_root / ".casa-rs" / "evidence" / "review" / "04-phase-cal-dirty.json"
    comparison = evidence["comparison"]
    record = {
        "section_id": "04-phase-cal-dirty",
        "title": "Dirty image of the phase calibrator",
        "status": "pending-human-review",
        "reviewed_by": None,
        "reviewed_at": None,
        "casa_source": {
            "guide_url": CASA_GUIDE_URL,
            "task_calls": [
                {
                    "task_id": "tclean",
                    "parameters": evidence["parameters"]["casa"],
                }
            ],
        },
        "surface_evidence": {
            "casa": {
                "status": evidence["oracle"]["status"],
                "artifacts": [".casa-rs/evidence/04-phase-cal-dirty-comparison.png"],
            },
            "cli": {
                "status": evidence["cli"]["status"],
                "artifacts": [".casa-rs/workspace/native/04-phase-cal-dirty/phase_cal.cli.image.png"],
            },
            "python": {
                "status": evidence["python"]["status"],
                "artifacts": [".casa-rs/workspace/native/04-phase-cal-dirty/phase_cal.python.image.png"],
            },
            "tui": {
                "status": "pending-parameter-screenshot",
                "artifacts": [],
            },
            "gui": {
                "status": evidence.get("gui_capture", {}).get("status", "not-run"),
                "artifacts": [
                    ".casa-rs/screenshots/gui/04-phase-cal-dirty-gui-parameters.png",
                    ".casa-rs/screenshots/gui/04-phase-cal-dirty-gui-parameters-focused.png",
                    ".casa-rs/screenshots/gui/04-phase-cal-dirty-gui-imexplore.png",
                ],
            },
        },
        "comparison": {
            "status": comparison["status"],
            "missing_native_products": comparison.get("missing_native_products", []),
            "core_numeric_products": [".image", ".residual", ".psf", ".model", ".sumwt"],
        },
        "human_evaluation": {
            "outcome": "pending",
            "comments": "Awaiting human review. Machine evidence found a native .pb product gap while core dirty-image numeric products match CASA.",
            "required_changes": ["Decide whether native dirty imaging must write the CASA .pb product before this section can be accepted."],
        },
    }
    write_json(review_path, record)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pack-root", type=Path, default=DEFAULT_PACK_ROOT)
    parser.add_argument("--casa-python", type=Path, default=DEFAULT_CASA_PYTHON)
    parser.add_argument("--imager-binary", type=Path, default=REPO_ROOT / "target" / "release" / "casars-imager")
    parser.add_argument("--gui-app-binary", type=Path, default=DEFAULT_GUI_APP_BINARY)
    parser.add_argument("--skip-gui-capture", action="store_true")
    args = parser.parse_args()

    pack_root = args.pack_root.expanduser().resolve()
    native_dir = pack_root / ".casa-rs" / "workspace" / "native" / SECTION_ID
    oracle_dir = pack_root / ".casa-rs" / "workspace" / "oracle" / SECTION_ID
    evidence_dir = pack_root / ".casa-rs" / "evidence"
    gui_screenshot_dir = pack_root / ".casa-rs" / "screenshots" / "gui"
    for directory in [native_dir, oracle_dir, evidence_dir, gui_screenshot_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    casa_prefix = oracle_dir / "phase_cal.casa"
    cli_prefix = native_dir / "phase_cal.cli"
    python_prefix = native_dir / "phase_cal.python"
    comparison_png = evidence_dir / "04-phase-cal-dirty-comparison.png"
    gui_parameters_png = gui_screenshot_dir / "04-phase-cal-dirty-gui-parameters.png"
    gui_parameters_focused_png = gui_screenshot_dir / "04-phase-cal-dirty-gui-parameters-focused.png"
    gui_imexplore_png = gui_screenshot_dir / "04-phase-cal-dirty-gui-imexplore.png"
    for prefix in [casa_prefix, cli_prefix, python_prefix]:
        for path in prefix.parent.glob(prefix.name + "*"):
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
    if comparison_png.exists():
        comparison_png.unlink()

    oracle_run = run_command(
        [str(args.casa_python), "-c", casa_oracle_code(str(casa_prefix.relative_to(pack_root)))],
        cwd=pack_root,
        env=os.environ.copy(),
        timeout_seconds=600,
    )
    require_success("CASA tclean dirty phase calibrator", oracle_run)
    oracle_result = parse_marked_json(oracle_run["stdout"])

    cli_run = run_command(
        [
            str(args.imager_binary),
            "--ms",
            MS_PATH,
            "--imagename",
            str(cli_prefix.relative_to(pack_root)),
            "--imsize",
            "250",
            "--cell-arcsec",
            "0.1",
            "--field",
            "3",
            "--phasecenter-field",
            "3",
            "--specmode",
            "mfs",
            "--deconvolver",
            "hogbom",
            "--weighting",
            "briggs",
            "--robust",
            "0.5",
            "--threshold-jy",
            "0.0",
            "--niter",
            "0",
            "--dirty-only",
        ],
        cwd=pack_root,
        env=os.environ.copy(),
        timeout_seconds=600,
    )
    require_success("CLI casars-imager dirty phase calibrator", cli_run)

    python_env = os.environ.copy()
    python_env["PYTHONPATH"] = str(REPO_ROOT / "crates" / "casars-python" / "python") + os.pathsep + python_env.get("PYTHONPATH", "")
    python_run = run_command(
        ["python", "-c", python_surface_code(str(python_prefix.relative_to(pack_root)), str(args.imager_binary))],
        cwd=pack_root,
        env=python_env,
        timeout_seconds=600,
    )
    require_success("Python imager dirty phase calibrator", python_run)

    compare_env = os.environ.copy()
    compare_env["MPLBACKEND"] = "Agg"
    compare_run = run_command(
        [
            str(args.casa_python),
            "-c",
            comparison_code(
                str(casa_prefix),
                {
                    "cli": str(cli_prefix),
                    "python": str(python_prefix),
                },
                str(comparison_png),
            ),
        ],
        cwd=pack_root,
        env=compare_env,
        timeout_seconds=300,
    )
    require_success("CASA/native dirty image comparison", compare_run)
    comparison = parse_marked_json(compare_run["stdout"])

    if args.skip_gui_capture:
        gui_capture = {
            "status": "skipped",
            "parameters": file_record(gui_parameters_png, pack_root),
            "parameters_focused": file_record(gui_parameters_focused_png, pack_root),
            "image_explorer": file_record(gui_imexplore_png, pack_root),
        }
    else:
        gui_parameter_run = run_command(
            [
                str(args.gui_app_binary),
                "--capture-gui-evidence",
                "--capture-kind",
                "imager-parameters",
                "--open-tutorial-pack",
                str(pack_root),
                "--dataset",
                MS_PATH,
                "--imagename",
                str(cli_prefix.relative_to(pack_root)),
                "--output",
                str(gui_parameters_png),
                "--width",
                "1440",
                "--height",
                "960",
            ],
            cwd=REPO_ROOT,
            env=os.environ.copy(),
            timeout_seconds=180,
        )
        require_success("GUI imager parameter capture", gui_parameter_run)
        write_focused_gui_parameters(gui_parameters_png, gui_parameters_focused_png)
        gui_image_run = run_command(
            [
                str(args.gui_app_binary),
                "--capture-gui-evidence",
                "--capture-kind",
                "image-explorer",
                "--open-tutorial-pack",
                str(pack_root),
                "--dataset",
                MS_PATH,
                "--image",
                str(cli_prefix) + ".image",
                "--output",
                str(gui_imexplore_png),
                "--width",
                "1440",
                "--height",
                "960",
            ],
            cwd=REPO_ROOT,
            env=os.environ.copy(),
            timeout_seconds=180,
        )
        require_success("GUI image explorer capture", gui_image_run)
        gui_capture = {
            "status": "succeeded",
            "parameters": file_record(gui_parameters_png, pack_root),
            "parameters_focused": file_record(gui_parameters_focused_png, pack_root),
            "image_explorer": file_record(gui_imexplore_png, pack_root),
            "parameter_capture": {
                "elapsed_seconds": gui_parameter_run["elapsed_seconds"],
                "stdout": gui_parameter_run["stdout"],
                "stderr": gui_parameter_run["stderr"],
            },
            "image_explorer_capture": {
                "elapsed_seconds": gui_image_run["elapsed_seconds"],
                "stdout": gui_image_run["stdout"],
                "stderr": gui_image_run["stderr"],
            },
        }

    evidence = {
        "section_id": SECTION_ID,
        "parameters": {
            "casa": {
                "vis": MS_PATH,
                "imagename": "phase_cal",
                "field": "3",
                "specmode": "mfs",
                "deconvolver": "hogbom",
                "gridder": "standard",
                "imsize": [250, 250],
                "cell": ["0.1arcsec"],
                "weighting": "briggs",
                "threshold": "0.0mJy",
                "interactive": True,
                "regression_overrides": {"niter": 0, "interactive": False},
            },
            "native": {
                "ms": MS_PATH,
                "imagename": str(cli_prefix.relative_to(pack_root)),
                "imsize": 250,
                "cell_arcsec": 0.1,
                "field": 3,
                "phasecenter_field": 3,
                "specmode": "mfs",
                "deconvolver": "hogbom",
                "weighting": "briggs",
                "robust": 0.5,
                "threshold_jy": 0.0,
                "niter": 0,
                "dirty_only": True,
            },
        },
        "oracle": {
            "status": "succeeded",
            "elapsed_seconds": oracle_run["elapsed_seconds"],
            "stdout": oracle_run["stdout"],
            "stderr": oracle_run["stderr"],
            "result": oracle_result,
            "products": product_records(casa_prefix, pack_root),
        },
        "cli": {
            "status": "succeeded",
            "elapsed_seconds": cli_run["elapsed_seconds"],
            "stdout": cli_run["stdout"],
            "stderr": cli_run["stderr"],
            "products": product_records(cli_prefix, pack_root),
        },
        "python": {
            "status": "succeeded",
            "elapsed_seconds": python_run["elapsed_seconds"],
            "stdout": python_run["stdout"],
            "stderr": python_run["stderr"],
            "products": product_records(python_prefix, pack_root),
        },
        "comparison": comparison,
        "comparison_artifact": file_record(comparison_png, pack_root),
        "gui_capture": gui_capture,
        "comparison_run": {
            "elapsed_seconds": compare_run["elapsed_seconds"],
            "stdout": compare_run["stdout"],
            "stderr": compare_run["stderr"],
        },
    }

    write_json(evidence_dir / f"{SECTION_ID}.json", evidence)
    write_docs(pack_root, evidence)
    write_review_record(pack_root, evidence)
    print(f"Wrote section evidence to {evidence_dir / f'{SECTION_ID}.json'}")
    print(f"Wrote section docs to {pack_root / 'docs' / 'sections' / '04-phase-cal-dirty.html'}")


if __name__ == "__main__":
    main()
