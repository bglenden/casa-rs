#!/usr/bin/env python3
"""Run the ALMA First Look Imaging primary-beam correction tutorial section."""

from __future__ import annotations

import argparse
import html
import json
import math
import os
import shutil
import subprocess
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
DEFAULT_PYTHON = Path("/opt/homebrew/bin/python3.14")
DEFAULT_CASARS_BINARY = REPO_ROOT / "target" / "debug" / "casars"
DEFAULT_IMPBCOR_BINARY = REPO_ROOT / "target" / "debug" / "impbcor"
DEFAULT_GUI_APP_BINARY = REPO_ROOT / "apps" / "casars-mac" / ".build" / "debug" / "casars-mac"
DEFAULT_GHOSTTY_CAPTURE_BINARY = Path("/private/tmp/ghostty-surface-capture")
SECTION_ID = "08-primary-beam-correction"
PREVIOUS_SECTION_ID = "07-science-target-auto-clean"
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


def remove_image(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists() or path.is_symlink():
        path.unlink()
    png = Path(str(path) + ".png")
    if png.exists():
        png.unlink()


def image_record(path: Path, pack_root: Path) -> dict[str, Any]:
    return {
        "path": str(path.relative_to(pack_root)),
        "exists": path.exists(),
        "size_bytes": path.stat().st_size if path.exists() else None,
    }


def casa_oracle_code(imagename: str, pbimage: str, outfile: str) -> str:
    return f"""
import json
from casatasks import impbcor

result = impbcor(
    imagename={imagename!r},
    pbimage={pbimage!r},
    outfile={outfile!r},
    overwrite=True,
)
print("JSON_RESULT_START")
print(json.dumps(result if result is not None else {{}}, indent=2, sort_keys=True))
print("JSON_RESULT_END")
"""


def python_surface_code(imagename: str, pbimage: str, outfile: str, binary: str) -> str:
    return f"""
import json
from casars import tasks

result = tasks.impbcor(
    imagename={imagename!r},
    pbimage={pbimage!r},
    outfile={outfile!r},
    overwrite=True,
    binary={binary!r},
)
print(json.dumps(json.loads(result.stdout), indent=2, sort_keys=True))
"""


def comparison_code(casa_path: str, native_paths: dict[str, str], comparison_png: str) -> str:
    return f"""
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from casatools import image

casa_path = {casa_path!r}
native_paths = json.loads({json.dumps(json.dumps(native_paths, sort_keys=True))})
comparison_png = Path({comparison_png!r})

def open_image(path):
    ia = image()
    if not Path(path).exists():
        return None
    try:
        ia.open(path)
        data = np.asarray(ia.getchunk(), dtype=np.float64)
        unit = ia.brightnessunit()
        mask = np.asarray(ia.getchunk(getmask=True), dtype=bool)
    finally:
        ia.close()
    return {{"shape": list(data.shape), "data": data, "unit": unit, "mask": mask}}

def summarize(data, mask):
    finite = data[np.isfinite(data) & mask]
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

casa = open_image(casa_path)
result = {{"status": "passed", "surfaces": {{}}, "thresholds": {{"max_rel_to_casa_peak": 5.0e-2, "rms_rel_to_casa_peak": 2.0e-2}}}}
if casa is None:
    result["status"] = "failed"
else:
    casa_summary = {{"shape": casa["shape"], "unit": casa["unit"], **summarize(casa["data"], casa["mask"])}}
    casa_valid = np.isfinite(casa["data"]) & casa["mask"]
    casa_peak = max(float(np.nanmax(np.abs(casa["data"][casa_valid]))), 1.0e-30)
    for surface, native_path in native_paths.items():
        native = open_image(native_path)
        entry = {{"casa_path": casa_path, "native_path": native_path, "casa_exists": True, "native_exists": native is not None, "casa": casa_summary}}
        if native is None:
            entry["status"] = "missing"
            result["status"] = "failed"
        else:
            native_valid = np.isfinite(native["data"]) & native["mask"]
            common = casa_valid & native_valid
            diff = native["data"][common] - casa["data"][common]
            max_abs = float(np.nanmax(np.abs(diff))) if diff.size else float("nan")
            rms_abs = float(math.sqrt(float(np.nanmean(diff * diff)))) if diff.size else float("nan")
            entry.update({{
                "status": "passed",
                "native": {{"shape": native["shape"], "unit": native["unit"], **summarize(native["data"], native["mask"])}},
                "common_valid_pixels": int(common.sum()),
                "casa_only_valid_pixels": int((casa_valid & ~native_valid).sum()),
                "native_only_valid_pixels": int((native_valid & ~casa_valid).sum()),
                "max_abs_diff": max_abs,
                "rms_abs_diff": rms_abs,
                "max_rel_to_casa_peak": max_abs / casa_peak,
                "rms_rel_to_casa_peak": rms_abs / casa_peak,
            }})
            if entry["max_rel_to_casa_peak"] > result["thresholds"]["max_rel_to_casa_peak"]:
                entry["status"] = "failed"
                result["status"] = "failed"
            if entry["rms_rel_to_casa_peak"] > result["thresholds"]["rms_rel_to_casa_peak"]:
                entry["status"] = "failed"
                result["status"] = "failed"
        result["surfaces"][surface] = entry

fig, axes = plt.subplots(2, 3, figsize=(14, 8), constrained_layout=True)
plot_specs = [("CASA PB-corrected", casa_path, None)]
for surface in ["cli", "python", "tui", "gui"]:
    plot_specs.append((f"casa-rs {{surface}} PB-corrected", native_paths[surface], None))
plot_specs.append(("CLI - CASA", native_paths["cli"], casa_path))
for ax, (title, path, diff_path) in zip(axes.ravel(), plot_specs):
    current = open_image(path)
    if current is None:
        ax.text(0.5, 0.5, "missing", ha="center", va="center")
        ax.set_axis_off()
        ax.set_title(title)
        continue
    data = np.squeeze(current["data"])
    if diff_path:
        reference = open_image(diff_path)
        data = data - np.squeeze(reference["data"])
        vmax = float(np.nanmax(np.abs(data))) or 1.0
        artist = ax.imshow(data, origin="lower", cmap="coolwarm", vmin=-vmax, vmax=vmax)
    else:
        artist = ax.imshow(data, origin="lower", cmap="inferno")
    ax.set_title(title)
    ax.set_xlabel("x pixel")
    ax.set_ylabel("y pixel")
    fig.colorbar(artist, ax=ax, shrink=0.8)
comparison_png.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(comparison_png, dpi=140)
plt.close(fig)

for label, path in [("casa", casa_path), *[(surface, native_paths[surface]) for surface in ["cli", "python", "tui", "gui"]]]:
    current = open_image(path)
    if current is None:
        continue
    fig, ax = plt.subplots(figsize=(6, 5), constrained_layout=True)
    artist = ax.imshow(np.squeeze(current["data"]), origin="lower", cmap="inferno")
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


def diff_rows(comparison: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for surface_name, entry in comparison.get("surfaces", {}).items():
        rows.append(
            {
                "surface": surface_name,
                "status": entry.get("status", "not-run"),
                "casa_peak": entry.get("casa", {}).get("peak_abs"),
                "native_peak": entry.get("native", {}).get("peak_abs"),
                "common_valid_pixels": entry.get("common_valid_pixels"),
                "max_abs_diff": entry.get("max_abs_diff"),
                "rms_abs_diff": entry.get("rms_abs_diff"),
                "max_rel_to_casa_peak": entry.get("max_rel_to_casa_peak"),
                "rms_rel_to_casa_peak": entry.get("rms_rel_to_casa_peak"),
            }
        )
    return rows


def format_number(value: Any) -> str:
    return "-" if value is None else f"{value:.6g}"


def write_docs(pack_root: Path, evidence: dict[str, Any]) -> None:
    docs_dir = pack_root / "docs" / "sections"
    docs_dir.mkdir(parents=True, exist_ok=True)
    comparison = evidence["comparison"]
    rows = diff_rows(comparison)
    markdown_rows = "\n".join(
        "| {surface} | {status} | {casa_peak} | {native_peak} | {common_valid_pixels} | {max_abs_diff} | {rms_abs_diff} | {max_rel} | {rms_rel} |".format(
            surface=row["surface"],
            status=row["status"],
            casa_peak=format_number(row["casa_peak"]),
            native_peak=format_number(row["native_peak"]),
            common_valid_pixels="-" if row["common_valid_pixels"] is None else row["common_valid_pixels"],
            max_abs_diff=format_number(row["max_abs_diff"]),
            rms_abs_diff=format_number(row["rms_abs_diff"]),
            max_rel=format_number(row["max_rel_to_casa_peak"]),
            rms_rel=format_number(row["rms_rel_to_casa_peak"]),
        )
        for row in rows
    )
    html_rows = "\n".join(
        "<tr><td>{surface}</td><td>{status}</td><td>{casa_peak}</td><td>{native_peak}</td><td>{common}</td><td>{max_abs}</td><td>{rms_abs}</td><td>{max_rel}</td><td>{rms_rel}</td></tr>".format(
            surface=html.escape(row["surface"]),
            status=html.escape(row["status"]),
            casa_peak=format_number(row["casa_peak"]),
            native_peak=format_number(row["native_peak"]),
            common="-" if row["common_valid_pixels"] is None else row["common_valid_pixels"],
            max_abs=format_number(row["max_abs_diff"]),
            rms_abs=format_number(row["rms_abs_diff"]),
            max_rel=format_number(row["max_rel_to_casa_peak"]),
            rms_rel=format_number(row["rms_rel_to_casa_peak"]),
        )
        for row in rows
    )
    comparison_json = html.escape(json.dumps(comparison, indent=2, sort_keys=True))
    comparison_png = ".casa-rs/evidence/08-primary-beam-correction-comparison.png"
    gui_params = ".casa-rs/screenshots/gui/08-primary-beam-correction-gui-parameters.png"
    gui_run = ".casa-rs/screenshots/gui/08-primary-beam-correction-gui-run.png"
    gui_imexplore = ".casa-rs/screenshots/gui/08-primary-beam-correction-gui-imexplore.png"
    tui_png = ".casa-rs/screenshots/headless/08-primary-beam-correction-tui.png"
    cli_png = ".casa-rs/workspace/native/08-primary-beam-correction/twhya_cont_auto.pbcor.cli.image.png"
    python_png = ".casa-rs/workspace/native/08-primary-beam-correction/twhya_cont_auto.pbcor.python.image.png"
    tui_image_png = ".casa-rs/workspace/native/08-primary-beam-correction/twhya_cont_auto.pbcor.tui.image.png"
    gui_image_png = ".casa-rs/workspace/native/08-primary-beam-correction/twhya_cont_auto.pbcor.gui.image.png"

    markdown = f"""# 08 - Primary beam correction

CASA tutorial chunk: apply `impbcor` to the cleaned science-target image using
the primary-beam image written by the clean step.

CASA source page: {CASA_GUIDE_URL}

## Parameters

| Surface | Task | Parameters |
| --- | --- | --- |
| CASA | `impbcor` | `imagename=".casa-rs/workspace/oracle/07-science-target-auto-clean/twhya_cont_auto.casa.image"`, `pbimage=".casa-rs/workspace/oracle/07-science-target-auto-clean/twhya_cont_auto.casa.pb"`, `outfile=".casa-rs/workspace/oracle/08-primary-beam-correction/twhya_cont_auto.pbcor.casa.image"`, `overwrite=True` |
| CLI | `impbcor` | `--imagename .casa-rs/workspace/native/07-science-target-auto-clean/twhya_cont_auto.cli.image --pbimage .casa-rs/workspace/native/07-science-target-auto-clean/twhya_cont_auto.cli.pb --outfile .casa-rs/workspace/native/08-primary-beam-correction/twhya_cont_auto.pbcor.cli.image --overwrite` |
| Python | `casars.tasks.impbcor` | Same image, PB, output, and overwrite values as CLI. |
| TUI | `casars impbcor` | Same image, PB, output, and overwrite values as CLI; captured through the TUI harness. |
| GUI | `Tasks > Primary Beam Correction` | Same image, PB, output, and overwrite values as CLI; the result is opened in Image Explorer. |

## Observable Result

The PB-corrected image should brighten pixels away from the pointing center
according to the reciprocal primary-beam response while preserving the source
structure from the clean image.

## Evidence

![CASA vs casa-rs PB-corrected comparison](../../{comparison_png})

![casa-rs GUI impbcor parameters](../../{gui_params})

![casa-rs GUI impbcor run](../../{gui_run})

![casa-rs GUI Image Explorer PB-corrected image](../../{gui_imexplore})

![casa-rs TUI impbcor run](../../{tui_png})

![casa-rs CLI PB-corrected image](../../{cli_png})

![casa-rs Python PB-corrected image](../../{python_png})

![casa-rs TUI PB-corrected image](../../{tui_image_png})

![casa-rs GUI PB-corrected image](../../{gui_image_png})

## Numeric/Product Comparison

Status: `{comparison.get("status", "unknown")}`.

| Surface | Status | CASA peak | Native peak | Common valid pixels | Peak abs diff | RMS diff | Peak rel | RMS rel |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
{markdown_rows}

```json
{json.dumps(comparison, indent=2, sort_keys=True)}
```
"""
    (docs_dir / f"{SECTION_ID}.md").write_text(markdown, encoding="utf-8")

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ALMA First Look Imaging - 08 Primary beam correction</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; line-height: 1.45; margin: 2rem; max-width: 1280px; }}
table {{ border-collapse: collapse; width: 100%; }}
td, th {{ border: 1px solid #c7c7c7; padding: 0.45rem; vertical-align: top; }}
pre {{ background: #f6f6f6; border: 1px solid #ddd; padding: 1rem; overflow: auto; }}
img {{ max-width: 100%; border: 1px solid #ddd; }}
.grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; }}
.wide {{ grid-column: 1 / -1; }}
</style>
</head>
<body>
<h1>ALMA First Look Imaging: 08 Primary beam correction</h1>
<p>This section applies CASA-style <code>impbcor</code> to the cleaned science-target image with the primary-beam image from the previous clean step.</p>
<h2>Parameters</h2>
<table>
<tr><th>Surface</th><th>Task</th><th>Parameters</th></tr>
<tr><td>CASA</td><td><code>impbcor</code></td><td><code>imagename=twhya_cont_auto.casa.image</code>, <code>pbimage=twhya_cont_auto.casa.pb</code>, <code>overwrite=True</code></td></tr>
<tr><td>CLI</td><td><code>impbcor</code></td><td><code>--imagename ...cli.image --pbimage ...cli.pb --outfile ...pbcor.cli.image --overwrite</code></td></tr>
<tr><td>Python</td><td><code>casars.tasks.impbcor</code></td><td>Same image, PB, output, and overwrite values as CLI.</td></tr>
<tr><td>TUI</td><td><code>casars impbcor</code></td><td>Same image, PB, output, and overwrite values as CLI.</td></tr>
<tr><td>GUI</td><td><code>Tasks &gt; Primary Beam Correction</code></td><td>Same image, PB, output, and overwrite values as CLI.</td></tr>
</table>
<h2>Visual Evidence</h2>
<div class="grid">
<figure class="wide"><img src="../../{comparison_png}" alt="CASA vs casa-rs PB-corrected comparison"><figcaption>CASA and casa-rs PB-corrected images plus CLI-CASA difference.</figcaption></figure>
<figure><img src="../../{gui_params}" alt="GUI impbcor parameters"><figcaption>GUI primary-beam correction parameters.</figcaption></figure>
<figure><img src="../../{gui_run}" alt="GUI impbcor run"><figcaption>GUI completed primary-beam correction run.</figcaption></figure>
<figure><img src="../../{gui_imexplore}" alt="GUI image explorer"><figcaption>GUI Image Explorer for the PB-corrected image.</figcaption></figure>
<figure><img src="../../{tui_png}" alt="TUI impbcor run"><figcaption>TUI primary-beam correction run.</figcaption></figure>
<figure><img src="../../{cli_png}" alt="CLI PB-corrected image"><figcaption>CLI PB-corrected image.</figcaption></figure>
<figure><img src="../../{python_png}" alt="Python PB-corrected image"><figcaption>Python PB-corrected image.</figcaption></figure>
<figure><img src="../../{tui_image_png}" alt="TUI PB-corrected image"><figcaption>TUI PB-corrected image.</figcaption></figure>
<figure><img src="../../{gui_image_png}" alt="GUI PB-corrected image"><figcaption>GUI PB-corrected image.</figcaption></figure>
</div>
<h2>Numeric/Product Comparison</h2>
<p>Status: <code>{html.escape(comparison.get("status", "unknown"))}</code>.</p>
<table>
<tr><th>Surface</th><th>Status</th><th>CASA peak</th><th>Native peak</th><th>Common valid pixels</th><th>Peak abs diff</th><th>RMS diff</th><th>Peak rel</th><th>RMS rel</th></tr>
{html_rows}
</table>
<h2>Comparison JSON</h2>
<pre>{comparison_json}</pre>
</body>
</html>
"""
    (docs_dir / f"{SECTION_ID}.html").write_text(html_doc, encoding="utf-8")


def update_pack_manifest(pack_root: Path) -> None:
    path = pack_root / "pack.json"
    if not path.exists():
        return
    manifest = json.loads(path.read_text(encoding="utf-8"))
    sections = [section for section in manifest.setdefault("sections", []) if section.get("id") != SECTION_ID]
    template = json.loads((REPO_ROOT / "resources" / "tutorial-packs" / "alma-first-look-imaging.template.json").read_text(encoding="utf-8"))
    sections.append(next(section for section in template["sections"] if section["id"] == SECTION_ID))
    manifest["sections"] = sorted(sections, key=lambda section: section.get("sequence", 999))
    write_json(path, manifest)


def run_section(args: argparse.Namespace) -> dict[str, Any]:
    pack_root = args.pack_root.expanduser().resolve()
    casa_python = args.casa_python.expanduser()
    python = args.python.expanduser()
    impbcor_binary = args.impbcor_binary.expanduser().resolve()
    casars_binary = args.casars_binary.expanduser().resolve()
    gui_app_binary = args.gui_app_binary.expanduser().resolve()

    native_dir = pack_root / ".casa-rs" / "workspace" / "native" / SECTION_ID
    oracle_dir = pack_root / ".casa-rs" / "workspace" / "oracle" / SECTION_ID
    evidence_dir = pack_root / ".casa-rs" / "evidence"
    headless_dir = pack_root / ".casa-rs" / "screenshots" / "headless"
    gui_dir = pack_root / ".casa-rs" / "screenshots" / "gui"
    for directory in [native_dir, oracle_dir, evidence_dir, headless_dir, gui_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    previous_native = pack_root / ".casa-rs" / "workspace" / "native" / PREVIOUS_SECTION_ID
    previous_oracle = pack_root / ".casa-rs" / "workspace" / "oracle" / PREVIOUS_SECTION_ID
    inputs = {
        "casa": (previous_oracle / "twhya_cont_auto.casa.image", previous_oracle / "twhya_cont_auto.casa.pb"),
        "cli": (previous_native / "twhya_cont_auto.cli.image", previous_native / "twhya_cont_auto.cli.pb"),
        "python": (previous_native / "twhya_cont_auto.python.image", previous_native / "twhya_cont_auto.python.pb"),
        "tui": (previous_native / "twhya_cont_auto.tui.image", previous_native / "twhya_cont_auto.tui.pb"),
        "gui": (previous_native / "twhya_cont_auto.gui.image", previous_native / "twhya_cont_auto.gui.pb"),
    }
    missing = [str(path.relative_to(pack_root)) for pair in inputs.values() for path in pair if not path.exists()]
    if missing:
        raise SystemExit(f"required clean/PB products from {PREVIOUS_SECTION_ID} are missing: {missing}")

    outputs = {
        "casa": oracle_dir / "twhya_cont_auto.pbcor.casa.image",
        "cli": native_dir / "twhya_cont_auto.pbcor.cli.image",
        "python": native_dir / "twhya_cont_auto.pbcor.python.image",
        "tui": native_dir / "twhya_cont_auto.pbcor.tui.image",
        "gui": native_dir / "twhya_cont_auto.pbcor.gui.image",
    }
    for output in outputs.values():
        remove_image(output)

    env = os.environ.copy()
    env["DISPLAY"] = env.get("DISPLAY", ":99")
    env["QT_QPA_PLATFORM"] = env.get("QT_QPA_PLATFORM", "offscreen")
    env["MPLBACKEND"] = env.get("MPLBACKEND", "Agg")

    oracle_run = run_command(
        [
            str(casa_python),
            "-c",
            casa_oracle_code(
                str(inputs["casa"][0].relative_to(pack_root)),
                str(inputs["casa"][1].relative_to(pack_root)),
                str(outputs["casa"].relative_to(pack_root)),
            ),
        ],
        cwd=pack_root,
        env=env,
        timeout_seconds=args.timeout_seconds,
    )
    require_success("CASA impbcor oracle", oracle_run)
    oracle_result = parse_marked_json(oracle_run["stdout"])

    cli_run = run_command(
        [
            str(impbcor_binary),
            "--imagename",
            str(inputs["cli"][0].relative_to(pack_root)),
            "--pbimage",
            str(inputs["cli"][1].relative_to(pack_root)),
            "--outfile",
            str(outputs["cli"].relative_to(pack_root)),
            "--overwrite",
        ],
        cwd=pack_root,
        env=os.environ.copy(),
        timeout_seconds=args.timeout_seconds,
    )
    require_success("casa-rs CLI impbcor", cli_run)

    python_env = os.environ.copy()
    python_env["PYTHONPATH"] = str(REPO_ROOT / "crates" / "casars-python" / "python") + os.pathsep + python_env.get("PYTHONPATH", "")
    python_env["CASA_RS_IMPBCOR_BIN"] = str(impbcor_binary)
    python_run = run_command(
        [
            str(python),
            "-c",
            python_surface_code(
                str(inputs["python"][0].relative_to(pack_root)),
                str(inputs["python"][1].relative_to(pack_root)),
                str(outputs["python"].relative_to(pack_root)),
                str(args.casars_binary),
            ),
        ],
        cwd=pack_root,
        env=python_env,
        timeout_seconds=args.timeout_seconds,
    )
    require_success("casa-rs Python impbcor", python_run)

    tui_capture_run = run_command(
        [
            str(args.ghostty_capture_binary),
            "--cwd",
            str(pack_root),
            "--output",
            str(headless_dir / "08-primary-beam-correction-tui.png"),
            "--width",
            "2400",
            "--height",
            "1600",
            "--font-size",
            "12",
            "--settle-seconds",
            "25",
            "--input-event",
            "500:r",
            "--input-event",
            "1500:r",
            "--",
            str(casars_binary),
            "impbcor",
            "--imagename",
            str(inputs["tui"][0].relative_to(pack_root)),
            "--pbimage",
            str(inputs["tui"][1].relative_to(pack_root)),
            "--outfile",
            str(outputs["tui"].relative_to(pack_root)),
            "--overwrite",
        ],
        cwd=REPO_ROOT,
        env={**os.environ.copy(), "CASA_RS_IMPBCOR_BIN": str(impbcor_binary)},
        timeout_seconds=args.timeout_seconds + 30,
    )
    require_success("casa-rs TUI impbcor capture", tui_capture_run)

    gui_env = os.environ.copy()
    gui_env["CASA_RS_IMPBCOR_BIN"] = str(impbcor_binary)
    gui_env["CASA_RS_REPO_ROOT"] = str(REPO_ROOT)
    gui_run = run_command(
        [
            str(gui_app_binary),
            "--capture-gui-evidence",
            "--capture-kind",
            "impbcor-run",
            "--open-tutorial-pack",
            str(pack_root),
            "--image",
            str(inputs["gui"][0].relative_to(pack_root)),
            "--pbimage",
            str(inputs["gui"][1].relative_to(pack_root)),
            "--outfile",
            str(outputs["gui"].relative_to(pack_root)),
            "--output",
            str(gui_dir / "08-primary-beam-correction-gui-run.png"),
            "--width",
            "1800",
            "--height",
            "1200",
        ],
        cwd=REPO_ROOT,
        env=gui_env,
        timeout_seconds=args.timeout_seconds + 30,
    )
    require_success("GUI impbcor run", gui_run)

    gui_parameters_run = run_command(
        [
            str(gui_app_binary),
            "--capture-gui-evidence",
            "--capture-kind",
            "impbcor-parameters",
            "--open-tutorial-pack",
            str(pack_root),
            "--image",
            str(inputs["gui"][0].relative_to(pack_root)),
            "--pbimage",
            str(inputs["gui"][1].relative_to(pack_root)),
            "--outfile",
            str(outputs["gui"].relative_to(pack_root)),
            "--output",
            str(gui_dir / "08-primary-beam-correction-gui-parameters.png"),
            "--width",
            "1800",
            "--height",
            "1200",
        ],
        cwd=REPO_ROOT,
        env=gui_env,
        timeout_seconds=60,
    )
    require_success("GUI impbcor parameter screenshot", gui_parameters_run)

    comparison_run = run_command(
        [
            str(casa_python),
            "-c",
            comparison_code(
                str(outputs["casa"]),
                {surface: str(outputs[surface]) for surface in ["cli", "python", "tui", "gui"]},
                str(evidence_dir / "08-primary-beam-correction-comparison.png"),
            ),
        ],
        cwd=pack_root,
        env=env,
        timeout_seconds=args.timeout_seconds,
    )
    require_success("PB-corrected image comparison", comparison_run)
    comparison = parse_marked_json(comparison_run["stdout"])

    gui_imexplore_run = run_command(
        [
            str(gui_app_binary),
            "--capture-gui-evidence",
            "--capture-kind",
            "image-explorer",
            "--open-tutorial-pack",
            str(pack_root),
            "--image",
            str(outputs["gui"]),
            "--output",
            str(gui_dir / "08-primary-beam-correction-gui-imexplore.png"),
            "--width",
            "1800",
            "--height",
            "1200",
        ],
        cwd=REPO_ROOT,
        env=gui_env,
        timeout_seconds=60,
    )
    require_success("GUI PB-corrected image explorer screenshot", gui_imexplore_run)

    evidence = {
        "section_id": SECTION_ID,
        "status": comparison["status"],
        "parameters": {
            "mode": "divide",
            "cutoff": -1.0,
            "overwrite": True,
        },
        "oracle": {
            "run": oracle_run,
            "result": oracle_result,
            "output": image_record(outputs["casa"], pack_root),
        },
        "cli": {"run": cli_run, "output": image_record(outputs["cli"], pack_root)},
        "python": {
            "run": python_run,
            "result": json.loads(python_run["stdout"]),
            "output": image_record(outputs["python"], pack_root),
        },
        "tui": {
            "run": tui_capture_run,
            "screenshot": str((headless_dir / "08-primary-beam-correction-tui.png").relative_to(pack_root)),
            "output": image_record(outputs["tui"], pack_root),
        },
        "gui": {
            "run_screenshot": str((gui_dir / "08-primary-beam-correction-gui-run.png").relative_to(pack_root)),
            "parameters_screenshot": str((gui_dir / "08-primary-beam-correction-gui-parameters.png").relative_to(pack_root)),
            "imexplore_screenshot": str((gui_dir / "08-primary-beam-correction-gui-imexplore.png").relative_to(pack_root)),
            "output": image_record(outputs["gui"], pack_root),
            "runs": {"run": gui_run, "parameters": gui_parameters_run, "imexplore": gui_imexplore_run},
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
                "comments": "Ready for review after PB-corrected CASA/casa-rs images and GUI/TUI evidence are inspected.",
                "required_changes": [],
            },
            "evidence": {"html": f"docs/sections/{SECTION_ID}.html", "json": f".casa-rs/evidence/{SECTION_ID}.json"},
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
    parser.add_argument("--impbcor-binary", type=Path, default=DEFAULT_IMPBCOR_BINARY)
    parser.add_argument("--casars-binary", type=Path, default=DEFAULT_CASARS_BINARY)
    parser.add_argument("--gui-app-binary", type=Path, default=DEFAULT_GUI_APP_BINARY)
    parser.add_argument("--ghostty-capture-binary", type=Path, default=DEFAULT_GHOSTTY_CAPTURE_BINARY)
    parser.add_argument("--timeout-seconds", type=float, default=300.0)
    args = parser.parse_args()

    evidence = run_section(args)
    print(json.dumps({"section_id": SECTION_ID, "status": evidence["status"]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
