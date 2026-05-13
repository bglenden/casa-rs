#!/usr/bin/env python3
"""Run the ALMA First Look Imaging UV-coverage tutorial-pack section."""

from __future__ import annotations

import argparse
import html
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

from PIL import Image

from tutorial_plot_manifest import compare_manifest_to_casa_txt, compare_manifests


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
DEFAULT_GHOSTTY_CAPTURE_BUILD_SCRIPT = REPO_ROOT / "tools" / "ghostty-surface-capture" / "build.sh"
DEFAULT_GHOSTTY_CAPTURE_BINARY = Path("/private/tmp/ghostty-surface-capture")
DEFAULT_GHOSTTYKIT_XCFRAMEWORK = Path("/private/tmp/ghostty-source/macos/GhosttyKit.xcframework")
DEFAULT_GUI_APP_BINARY = REPO_ROOT / "apps" / "casars-mac" / ".build" / "debug" / "casars-mac"
SECTION_ID = "02-uv-coverage"
MS_PATH = "twhya_calibrated.ms"
CASA_GUIDE_UV_COVERAGE_IMAGE = (
    "https://casaguides.nrao.edu/images/7/7c/Imaging-tutorial-uv-coverage_6.5.4.jpg"
)


def run_command(args: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> dict[str, Any]:
    start = time.perf_counter()
    result = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True, check=False)
    return {
        "args": args,
        "exit_code": result.returncode,
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


def image_record(path: Path, pack_root: Path) -> dict[str, Any]:
    return {
        "path": str(path.relative_to(pack_root)),
        "exists": path.exists(),
        "size_bytes": path.stat().st_size if path.exists() else None,
    }


def file_record(path: Path, pack_root: Path) -> dict[str, Any]:
    return {
        "path": str(path.relative_to(pack_root)),
        "exists": path.exists(),
        "size_bytes": path.stat().st_size if path.exists() else None,
    }


def colored_pixel_metrics(path: Path, *, bins: int = 32) -> dict[str, Any]:
    image = Image.open(path).convert("RGB")
    width, height = image.size
    xs: list[int] = []
    ys: list[int] = []
    occupancy = [[0 for _ in range(bins)] for _ in range(bins)]
    radial = [0 for _ in range(bins)]
    for y in range(height):
        for x in range(width):
            r, g, b = image.getpixel((x, y))
            mx = max(r, g, b)
            mn = min(r, g, b)
            saturation = 0.0 if mx == 0 else (mx - mn) / mx
            brightness = mx / 255.0
            if saturation < 0.18 or brightness < 0.18 or brightness > 0.98:
                continue
            xs.append(x)
            ys.append(y)
            bx = min(bins - 1, max(0, int(x / width * bins)))
            by = min(bins - 1, max(0, int(y / height * bins)))
            occupancy[by][bx] += 1
    if not xs:
        return {
            "path": str(path),
            "width": width,
            "height": height,
            "colored_pixel_count": 0,
            "status": "blank-or-no-colored-data",
        }
    min_x = min(xs)
    max_x = max(xs)
    min_y = min(ys)
    max_y = max(ys)
    centroid_x = sum(xs) / len(xs)
    centroid_y = sum(ys) / len(ys)
    max_radius = max(
        ((x - centroid_x) ** 2 + (y - centroid_y) ** 2) ** 0.5
        for x, y in zip(xs, ys, strict=False)
    )
    if max_radius > 0:
        for x, y in zip(xs, ys, strict=False):
            radius = ((x - centroid_x) ** 2 + (y - centroid_y) ** 2) ** 0.5
            radial[min(bins - 1, int(radius / max_radius * bins))] += 1
    bbox_width = max_x - min_x + 1
    bbox_height = max_y - min_y + 1
    return {
        "path": str(path),
        "width": width,
        "height": height,
        "colored_pixel_count": len(xs),
        "colored_fraction": len(xs) / (width * height),
        "bbox": {
            "x_min_fraction": min_x / width,
            "x_max_fraction": max_x / width,
            "y_min_fraction": min_y / height,
            "y_max_fraction": max_y / height,
            "width_fraction": bbox_width / width,
            "height_fraction": bbox_height / height,
            "aspect": bbox_width / bbox_height if bbox_height else None,
        },
        "centroid": {
            "x_fraction": centroid_x / width,
            "y_fraction": centroid_y / height,
        },
        "occupancy": occupancy,
        "radial_histogram": radial,
        "status": "ok",
    }


def cosine_similarity(left: list[float], right: list[float]) -> float | None:
    dot = sum(a * b for a, b in zip(left, right, strict=False))
    left_norm = sum(a * a for a in left) ** 0.5
    right_norm = sum(b * b for b in right) ** 0.5
    if left_norm == 0 or right_norm == 0:
        return None
    return dot / (left_norm * right_norm)


def flatten(matrix: list[list[int]]) -> list[float]:
    return [float(item) for row in matrix for item in row]


def compare_images(casa_png: Path, casars_png: Path) -> dict[str, Any]:
    casa = colored_pixel_metrics(casa_png)
    casars = colored_pixel_metrics(casars_png)
    if casa["status"] != "ok" or casars["status"] != "ok":
        return {"status": "not-comparable", "casa": casa, "casars": casars}
    bbox_delta = {
        key: abs(casa["bbox"][key] - casars["bbox"][key])
        for key in [
            "x_min_fraction",
            "x_max_fraction",
            "y_min_fraction",
            "y_max_fraction",
            "width_fraction",
            "height_fraction",
            "aspect",
        ]
    }
    centroid_delta = {
        key: abs(casa["centroid"][key] - casars["centroid"][key])
        for key in ["x_fraction", "y_fraction"]
    }
    occupancy_similarity = cosine_similarity(flatten(casa["occupancy"]), flatten(casars["occupancy"]))
    radial_similarity = cosine_similarity(
        [float(v) for v in casa["radial_histogram"]],
        [float(v) for v in casars["radial_histogram"]],
    )
    checks = {
        "casa_nonblank": casa["colored_pixel_count"] > 1000,
        "casars_nonblank": casars["colored_pixel_count"] > 1000,
        "bbox_width_close": bbox_delta["width_fraction"] <= 0.25,
        "bbox_height_close": bbox_delta["height_fraction"] <= 0.25,
        "centroid_close": max(centroid_delta.values()) <= 0.18,
        "occupancy_similar": occupancy_similarity is not None and occupancy_similarity >= 0.55,
        "radial_similar": radial_similarity is not None and radial_similarity >= 0.75,
    }
    return {
        "status": "pass" if all(checks.values()) else "needs-review",
        "accepted_differences": [
            "font rendering",
            "color palette",
            "axis tick intervals",
            "image dimensions",
            "anti-aliasing",
            "legend/default annotation layout",
        ],
        "checks": checks,
        "metrics": {
            "bbox_delta": bbox_delta,
            "centroid_delta": centroid_delta,
            "occupancy_cosine_similarity": occupancy_similarity,
            "radial_cosine_similarity": radial_similarity,
        },
        "casa": casa,
        "casars": casars,
    }


def write_docs(pack_root: Path, evidence: dict[str, Any]) -> None:
    docs_dir = pack_root / "docs" / "sections"
    docs_dir.mkdir(parents=True, exist_ok=True)
    cli_png = ".casa-rs/workspace/native/02-uv-coverage/cli-msexplore-uv-coverage.png"
    python_png = ".casa-rs/workspace/native/02-uv-coverage/python-msexplore-uv-coverage.png"
    casa_png = ".casa-rs/workspace/oracle/02-uv-coverage/casa-plotms-uv-coverage.png"
    tui_png = ".casa-rs/screenshots/headless/02-uv-coverage-tui.png"
    gui_png = ".casa-rs/screenshots/gui/02-uv-coverage-gui-workbench-dark.png"
    guide_url = "https://casaguides.nrao.edu/index.php?title=First_Look_at_Imaging_CASA_6.5.4#A_First_Look_at_the_Data"
    comparison = evidence.get("comparison", {})
    comparison_status = comparison.get("status", "not-run")
    occupancy_similarity = comparison.get("metrics", {}).get("occupancy_cosine_similarity")
    radial_similarity = comparison.get("metrics", {}).get("radial_cosine_similarity")
    numerical = evidence.get("numerical_comparison", {})
    casa_numerical = evidence.get("casa_numerical_comparison", {})
    numerical_class = "ok" if numerical.get("byte_identical") else "pending"
    numerical_pre = html.escape(json.dumps(numerical, indent=2, sort_keys=True))
    casa_numerical_pre = html.escape(json.dumps(casa_numerical, indent=2, sort_keys=True))
    point_count = numerical.get("reference", {}).get("point_count", "unknown")
    casa_numerical_status = casa_numerical.get("status", "not-run")
    markdown = f"""# 02 - Plot calibrated MeasurementSet UV coverage

CASA tutorial chunk:

```python
plotms(vis="twhya_calibrated.ms", xaxis="u", yaxis="v",
       avgchannel="10000", avgspw=False, avgtime="1e9",
       avgscan=False, coloraxis="field")
```

CASA source page: {guide_url}

## Parameters

| Surface | Task | Parameters |
| --- | --- | --- |
| CASA | `plotms` | `vis="twhya_calibrated.ms"`, `xaxis="u"`, `yaxis="v"`, `avgchannel="10000"`, `avgspw=False`, `avgtime="1e9"`, `avgscan=False`, `coloraxis="field"` |
| CLI | `msexplore` | `--preset uv_coverage --avgchannel 10000 --avgtime 1e9 --color-by field --plot-output {cli_png} --plot-width 1200 --plot-height 1200 --overwrite twhya_calibrated.ms` |
| Python | `casars.tasks.msexplore.plot` | `measurement_set="twhya_calibrated.ms"`, `output_path="{python_png}"`, `preset="uv_coverage"`, `avgchannel=10000`, `avgtime=1e9`, `avgspw=False`, `avgscan=False`, `color_by="Field"`, `width=1200`, `height=1200` |
| TUI | `MSExplore > Plots` | `Preset=UV Coverage`, `MeasurementSet=twhya_calibrated.ms`, `Average channels=10000`, `Average time=1e9`, `Average SPW=false`, `Average scan=false`, `Color by=Field` |
| GUI | `MeasurementSet Explorer > Plots` | `Preset=UV Coverage`, `MeasurementSet=twhya_calibrated.ms`, `Average channels=10000`, `Average time=1e9`, `Average SPW=false`, `Average scan=false`, `Color by=Field` |

## Observable Result

The UV coverage plot should show a roughly circular/spiral ALMA uv-track distribution with equal U/V axis scaling and field-colored tracks. The casa-rs CLI and Python surfaces write 1200 x 1200 PNGs for regression comparison.

## Evidence

| Surface | Artifact |
| --- | --- |
| CASA Guide | `{CASA_GUIDE_UV_COVERAGE_IMAGE}` |
| CASA C++ | `{casa_png}` generated with Qt offscreen `plotms` export |
| CLI | `{cli_png}` |
| Python | `{python_png}` |
| TUI | `{tui_png}` captured from GhosttyKit's macOS Metal renderer layer |
| GUI | `{gui_png}` captured with the macOS offscreen GUI evidence renderer |

## Tolerant Comparison

- status: `{comparison_status}`
- UV occupancy cosine similarity: `{occupancy_similarity}`
- radial distribution cosine similarity: `{radial_similarity}`
- accepted visual differences: font rendering, color palette, axis tick intervals, image dimensions, anti-aliasing, legend/default annotation layout

## Numerical/Structural Regression Evidence

The CLI and Python surfaces also export a `txt` manifest containing the exact plotted `u, v` points after the tutorial averaging parameters are applied. For this run the manifests are byte-identical: `{numerical.get("byte_identical")}`. Point count: `{point_count}`.

The casa-rs CLI manifest is also compared with CASA `plotms` `expformat="txt"` output using point counts plus absolute and relative value tolerances. CASA numeric comparison status: `{casa_numerical_status}`.

![CASA Guide UV coverage]({CASA_GUIDE_UV_COVERAGE_IMAGE})

![CASA C++ offscreen plotms UV coverage](../../{casa_png})

![casa-rs CLI UV coverage](../../{cli_png})

![casa-rs Python UV coverage](../../{python_png})

![casa-rs TUI GhosttyKit renderer capture](../../{tui_png})

![casa-rs GUI UV coverage](../../{gui_png})

## Review State

CASA oracle, CLI, Python, TUI, and GUI evidence has been generated. CASA `plotms` oracle status: `{evidence["oracle"]["status"]}`. TUI capture status: `{evidence.get("tui_capture", {}).get("status", "not-run")}`. GUI capture status: `{evidence.get("gui_capture", {}).get("status", "not-run")}`.
"""
    (docs_dir / "02-uv-coverage.md").write_text(markdown, encoding="utf-8")
    oracle_stderr = html.escape(evidence["oracle"].get("stderr", ""))
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ALMA First Look Imaging - 02 UV coverage</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; line-height: 1.45; margin: 2rem; max-width: 1280px; }}
table {{ border-collapse: collapse; width: 100%; }}
td, th {{ border: 1px solid #c7c7c7; padding: 0.45rem; vertical-align: top; }}
pre {{ background: #f6f6f6; border: 1px solid #ddd; padding: 1rem; overflow: auto; }}
.grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; }}
.wide {{ grid-column: 1 / -1; }}
img {{ max-width: 100%; border: 1px solid #ddd; }}
.pending {{ color: #8a5a00; font-weight: 600; }}
.ok {{ color: #176b2c; font-weight: 600; }}
.placeholder {{ border: 1px dashed #999; min-height: 260px; display: flex; align-items: center; justify-content: center; color: #666; background: #f7f7f7; text-align: center; padding: 1rem; }}
iframe {{ width: 100%; height: 720px; border: 1px solid #ddd; background: #101214; }}
</style>
</head>
<body>
<h1>ALMA First Look Imaging: 02 UV coverage</h1>
<p>CASA source call: <code>plotms(vis="twhya_calibrated.ms", xaxis="u", yaxis="v", avgchannel="10000", avgspw=False, avgtime="1e9", avgscan=False, coloraxis="field")</code>.</p>
<p>CASA source page: <a href="{guide_url}">{guide_url}</a></p>
<h2>Parameters</h2>
<table>
<tr><th>Surface</th><th>Task</th><th>Parameters</th></tr>
<tr><td>CASA</td><td><code>plotms</code></td><td><code>vis=twhya_calibrated.ms</code>, <code>xaxis=u</code>, <code>yaxis=v</code>, <code>avgchannel=10000</code>, <code>avgspw=False</code>, <code>avgtime=1e9</code>, <code>avgscan=False</code>, <code>coloraxis=field</code></td></tr>
<tr><td>CLI</td><td><code>msexplore</code></td><td><code>--preset uv_coverage --avgchannel 10000 --avgtime 1e9 --color-by field --plot-output {cli_png} --plot-width 1200 --plot-height 1200 --overwrite twhya_calibrated.ms</code></td></tr>
<tr><td>Python</td><td><code>casars.tasks.msexplore.plot</code></td><td><code>preset=uv_coverage</code>, <code>avgchannel=10000</code>, <code>avgtime=1e9</code>, <code>avgspw=False</code>, <code>avgscan=False</code>, <code>color_by=Field</code>, <code>width=1200</code>, <code>height=1200</code></td></tr>
<tr><td>TUI</td><td><code>MSExplore &gt; Plots</code></td><td><code>Preset=UV Coverage</code>, <code>Average channels=10000</code>, <code>Average time=1e9</code>, <code>Color by=Field</code>. The regression capture runs under <code>TERM=xterm-ghostty</code> in a GhosttyKit offscreen macOS surface and writes the PNG from Ghostty's renderer layer.</td></tr>
<tr><td>GUI</td><td><code>MeasurementSet Explorer &gt; Plots</code></td><td><code>Preset=UV Coverage</code>, <code>Average channels=10000</code>, <code>Average time=1e9</code>, <code>Color by=Field</code></td></tr>
</table>
<h2>Visible Evidence</h2>
<p class="ok">CASA oracle status: <code>{html.escape(evidence["oracle"]["status"])}</code>. TUI capture status: <code>{html.escape(evidence.get("tui_capture", {}).get("status", "not-run"))}</code>. GUI capture status: <code>{html.escape(evidence.get("gui_capture", {}).get("status", "not-run"))}</code>.</p>
<h2>Numerical/Structural Regression Evidence</h2>
<p class="{numerical_class}">CLI and Python point manifests byte-identical: <code>{html.escape(str(numerical.get("byte_identical")))}</code>. Point count: <code>{html.escape(str(point_count))}</code>.</p>
<p>The UV manifest records every plotted <code>u, v</code> point after the tutorial parameters are applied. It is compared both across casa-rs surfaces and against CASA <code>plotms</code> text export.</p>
<pre>{numerical_pre}</pre>
<h3>CASA plotms Text Export Comparison</h3>
<p>Status: <code>{html.escape(casa_numerical_status)}</code>.</p>
<pre>{casa_numerical_pre}</pre>
<div class="grid">
<section><h3>CASA Guide GUI Snapshot</h3><img src="{CASA_GUIDE_UV_COVERAGE_IMAGE}" alt="CASA Guide UV coverage"></section>
<section><h3>CASA C++ plotms Offscreen Export</h3><img src="../../{casa_png}" alt="CASA plotms UV coverage"></section>
<section><h3>casa-rs CLI</h3><img src="../../{cli_png}" alt="casa-rs CLI UV coverage"></section>
<section><h3>casa-rs Python</h3><img src="../../{python_png}" alt="casa-rs Python UV coverage"></section>
<section class="wide"><h3>casa-rs TUI</h3><img src="../../{tui_png}" alt="casa-rs TUI UV coverage GhosttyKit renderer capture"><p>Captured from GhosttyKit's macOS Metal renderer layer; no separate plot image was overlaid.</p></section>
<section><h3>casa-rs GUI</h3><img src="../../{gui_png}" alt="casa-rs GUI UV coverage capture"></section>
</div>
<h2>Tolerant Comparison</h2>
<p>Status: <code>{html.escape(comparison_status)}</code>. This comparison checks broad plotted-data geometry in the rendered PNGs, not pixel-perfect equality.</p>
<table>
<tr><th>Metric</th><th>Value</th></tr>
<tr><td>UV occupancy cosine similarity</td><td><code>{html.escape(str(occupancy_similarity))}</code></td></tr>
<tr><td>Radial distribution cosine similarity</td><td><code>{html.escape(str(radial_similarity))}</code></td></tr>
<tr><td>Accepted differences</td><td>font rendering, color palette, axis tick intervals, image dimensions, anti-aliasing, legend/default annotation layout</td></tr>
</table>
<h2>CASA Oracle Note</h2>
<pre>{oracle_stderr}</pre>
</body>
</html>
"""
    (docs_dir / "02-uv-coverage.html").write_text(html_doc, encoding="utf-8")


def write_review_record(pack_root: Path) -> None:
    review = {
        "schema_version": "tutorial-pack-review.v0",
        "pack_id": "alma-first-look-imaging",
        "tutorial_id": "alma/first-look/twhya/imaging",
        "section_id": SECTION_ID,
        "status": "pending-human-review",
        "casa_source": {
            "guide_url": "https://casaguides.nrao.edu/index.php/First_Look_at_Imaging",
            "section_anchor": "A_First_Look_at_the_Data",
            "task_calls": [
                {
                    "task_id": "plotms",
                    "parameters": {
                        "vis": MS_PATH,
                        "xaxis": "u",
                        "yaxis": "v",
                        "avgchannel": "10000",
                        "avgspw": False,
                        "avgtime": "1e9",
                        "avgscan": False,
                        "coloraxis": "field",
                    },
                }
            ],
            "expected_observable_result": "UV coverage plot for twhya_calibrated.ms with equal U/V aspect ratio and field-colored tracks.",
        },
        "casars_equivalents": {
            "cli": {
                "provider_kind": "native-rust",
                "task_id": "msexplore",
                "command_template": "msexplore --preset uv_coverage --avgchannel 10000 --avgtime 1e9 --color-by field --plot-output .casa-rs/workspace/native/02-uv-coverage/cli-msexplore-uv-coverage.png --plot-width 1200 --plot-height 1200 --overwrite twhya_calibrated.ms",
                "parameters": {
                    "ms_path": MS_PATH,
                    "preset": "uv_coverage",
                    "avgchannel": 10000,
                    "avgtime": 1e9,
                    "color_by": "field",
                    "plot_output": ".casa-rs/workspace/native/02-uv-coverage/cli-msexplore-uv-coverage.png",
                    "plot_width": 1200,
                    "plot_height": 1200,
                    "overwrite": True,
                },
            },
            "python": {
                "provider_kind": "native-rust",
                "task_id": "msexplore.plot",
                "parameters": {
                    "measurement_set": MS_PATH,
                    "output_path": ".casa-rs/workspace/native/02-uv-coverage/python-msexplore-uv-coverage.png",
                    "preset": "uv_coverage",
                    "avgchannel": 10000,
                    "avgtime": 1e9,
                    "avgspw": False,
                    "avgscan": False,
                    "color_by": "Field",
                    "width": 1200,
                    "height": 1200,
                },
            },
            "tui": {
                "provider_kind": "native-rust",
                "task_id": "msexplore",
                "ui_path": "Measurement Sets > MSExplore > Plots",
                "screenshot_refs": [
                    ".casa-rs/screenshots/headless/02-uv-coverage-tui.png",
                ],
                "parameters": {
                    "ms_path": MS_PATH,
                    "preset": "UV Coverage",
                    "average_channels": "10000",
                    "average_time": "1e9",
                    "color_by": "Field",
                },
            },
            "gui": {
                "provider_kind": "native-rust",
                "task_id": "msexplore",
                "ui_path": "MeasurementSet Explorer > Plots",
                "screenshot_refs": [".casa-rs/screenshots/gui/02-uv-coverage-gui-workbench-dark.png"],
                "parameters": {
                    "ms_path": MS_PATH,
                    "preset": "UV Coverage",
                    "average_channels": "10000",
                    "average_time": "1e9",
                    "color_by": "Field",
                },
            },
        },
        "observable_products": {
            "casa_refs": [".casa-rs/workspace/oracle/02-uv-coverage/casa-plotms-uv-coverage.png"],
            "casars_refs": [
                ".casa-rs/workspace/native/02-uv-coverage/cli-msexplore-uv-coverage.png",
                ".casa-rs/workspace/native/02-uv-coverage/python-msexplore-uv-coverage.png",
            ],
            "comparison_refs": [".casa-rs/evidence/02-uv-coverage.json"],
            "timing_refs": [".casa-rs/evidence/02-uv-coverage.json"],
        },
        "regression_evidence": {
            "input_manifest_refs": [".casa-rs/evidence/data-manifest.json"],
            "native_run_refs": [
                ".casa-rs/workspace/native/02-uv-coverage/cli-msexplore-uv-coverage.png",
                ".casa-rs/workspace/native/02-uv-coverage/python-msexplore-uv-coverage.png",
            ],
            "oracle_run_refs": [".casa-rs/evidence/02-uv-coverage.json"],
            "provider_provenance_refs": [".casa-rs/evidence/provider-provenance.json"],
            "screenshot_spec_refs": [
                ".casa-rs/screenshots/headless/02-uv-coverage-tui.png",
                ".casa-rs/screenshots/gui/02-uv-coverage-gui-workbench-dark.png",
            ],
        },
        "human_evaluation": {
            "outcome": "pending",
            "reviewed_by": None,
            "reviewed_at": None,
            "comments": "CASA offscreen plotms oracle plus CLI, Python, headless TUI, and macOS GUI plot evidence generated.",
            "required_changes": [],
            "follow_up_issue_refs": [],
        },
    }
    write_json(pack_root / ".casa-rs" / "evidence" / "review" / f"{SECTION_ID}.json", review)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pack-root", type=Path, default=DEFAULT_PACK_ROOT)
    parser.add_argument("--msexplore-binary", type=Path, default=REPO_ROOT / "target" / "release" / "msexplore")
    parser.add_argument("--casars-binary", type=Path, default=REPO_ROOT / "target" / "debug" / "casars")
    parser.add_argument("--ghostty-capture-binary", type=Path, default=DEFAULT_GHOSTTY_CAPTURE_BINARY)
    parser.add_argument("--ghostty-capture-build-script", type=Path, default=DEFAULT_GHOSTTY_CAPTURE_BUILD_SCRIPT)
    parser.add_argument("--ghosttykit-xcframework", type=Path, default=DEFAULT_GHOSTTYKIT_XCFRAMEWORK)
    parser.add_argument("--skip-ghostty-capture-build", action="store_true")
    parser.add_argument("--skip-tui-screenshot", action="store_true")
    parser.add_argument("--gui-app-binary", type=Path, default=DEFAULT_GUI_APP_BINARY)
    parser.add_argument("--skip-gui-capture", action="store_true")
    parser.add_argument("--casa-python", type=Path, default=DEFAULT_CASA_PYTHON)
    args = parser.parse_args()

    pack_root = args.pack_root.expanduser().resolve()
    native_dir = pack_root / ".casa-rs" / "workspace" / "native" / SECTION_ID
    oracle_dir = pack_root / ".casa-rs" / "workspace" / "oracle" / SECTION_ID
    evidence_dir = pack_root / ".casa-rs" / "evidence"
    screenshot_dir = pack_root / ".casa-rs" / "screenshots" / "headless"
    gui_screenshot_dir = pack_root / ".casa-rs" / "screenshots" / "gui"
    native_dir.mkdir(parents=True, exist_ok=True)
    oracle_dir.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    screenshot_dir.mkdir(parents=True, exist_ok=True)
    gui_screenshot_dir.mkdir(parents=True, exist_ok=True)

    cli_png = native_dir / "cli-msexplore-uv-coverage.png"
    python_png = native_dir / "python-msexplore-uv-coverage.png"
    cli_txt = native_dir / "cli-msexplore-uv-coverage.txt"
    python_txt = native_dir / "python-msexplore-uv-coverage.txt"
    casa_png = oracle_dir / "casa-plotms-uv-coverage.png"
    casa_txt = oracle_dir / "casa-plotms-uv-coverage.txt"
    tui_png = screenshot_dir / "02-uv-coverage-tui.png"
    gui_png = gui_screenshot_dir / "02-uv-coverage-gui-workbench-dark.png"

    runs: list[dict[str, Any]] = []
    cli_run = run_command(
        [
            str(args.msexplore_binary),
            "--preset",
            "uv_coverage",
            "--avgchannel",
            "10000",
            "--avgtime",
            "1e9",
            "--color-by",
            "field",
            "--plot-output",
            str(cli_png.relative_to(pack_root)),
            "--plot-width",
            "1200",
            "--plot-height",
            "1200",
            "--overwrite",
            MS_PATH,
        ],
        cwd=pack_root,
    )
    require_success("CLI msexplore UV coverage", cli_run)
    runs.append({"surface": "cli", "task": "msexplore", "elapsed_seconds": cli_run["elapsed_seconds"]})

    cli_txt_run = run_command(
        [
            str(args.msexplore_binary),
            "--preset",
            "uv_coverage",
            "--avgchannel",
            "10000",
            "--avgtime",
            "1e9",
            "--color-by",
            "field",
            "--plot-format",
            "txt",
            "--plot-output",
            str(cli_txt.relative_to(pack_root)),
            "--overwrite",
            MS_PATH,
        ],
        cwd=pack_root,
    )
    require_success("CLI msexplore UV coverage manifest", cli_txt_run)

    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT / "crates" / "casars-python" / "python") + os.pathsep + env.get("PYTHONPATH", "")
    python_code = (
        "from casars.tasks import msexplore; "
        "msexplore.plot("
        f"{MS_PATH!r}, {str(python_png.relative_to(pack_root))!r}, "
        "preset='uv_coverage', avgchannel=10000, avgtime=1e9, "
        "avgspw=False, avgscan=False, color_by='Field', width=1200, height=1200, "
        f"binary={str(args.msexplore_binary)!r})"
    )
    python_run = run_command(["python", "-c", python_code], cwd=pack_root, env=env)
    require_success("Python msexplore UV coverage", python_run)
    runs.append({"surface": "python", "task": "msexplore.plot", "elapsed_seconds": python_run["elapsed_seconds"]})

    python_txt_code = (
        "from casars.tasks import msexplore; "
        "msexplore.plot("
        f"{MS_PATH!r}, {str(python_txt.relative_to(pack_root))!r}, "
        "preset='uv_coverage', avgchannel=10000, avgtime=1e9, "
        "avgspw=False, avgscan=False, color_by='Field', format='txt', width=1200, height=1200, "
        f"binary={str(args.msexplore_binary)!r})"
    )
    python_txt_run = run_command(["python", "-c", python_txt_code], cwd=pack_root, env=env)
    require_success("Python msexplore UV coverage manifest", python_txt_run)
    numerical_comparison = compare_manifests(cli_txt, python_txt)

    oracle_env = os.environ.copy()
    oracle_env["DISPLAY"] = oracle_env.get("DISPLAY", ":99")
    oracle_env["QT_QPA_PLATFORM"] = oracle_env.get("QT_QPA_PLATFORM", "offscreen")
    oracle_env["MPLBACKEND"] = oracle_env.get("MPLBACKEND", "Agg")
    oracle_run = run_command(
        [
            str(args.casa_python),
            "-c",
            (
                "from casaplotms import plotms; "
                "plotms(vis='twhya_calibrated.ms', xaxis='u', yaxis='v', "
                "avgchannel='10000', avgspw=False, avgtime='1e9', avgscan=False, "
                "coloraxis='field', showgui=False, "
                f"plotfile={str(casa_png.relative_to(pack_root))!r}, expformat='png', overwrite=True)"
            ),
        ],
        cwd=pack_root,
        env=oracle_env,
    )
    oracle = {
        "status": "succeeded" if oracle_run["exit_code"] == 0 and casa_png.exists() else "not-generated",
        "exit_code": oracle_run["exit_code"],
        "stdout": oracle_run["stdout"],
        "stderr": oracle_run["stderr"],
        "artifact": image_record(casa_png, pack_root),
    }
    if oracle["status"] == "not-generated" and "DISPLAY environment variable is not set" in oracle_run["stderr"]:
        oracle["status"] = "blocked-no-display"

    casa_txt_run = run_command(
        [
            str(args.casa_python),
            "-c",
            (
                "from casaplotms import plotms; "
                "plotms(vis='twhya_calibrated.ms', xaxis='u', yaxis='v', "
                "avgchannel='10000', avgspw=False, avgtime='1e9', avgscan=False, "
                "coloraxis='field', showgui=False, "
                f"plotfile={str(casa_txt.relative_to(pack_root))!r}, expformat='txt', overwrite=True, verbose=True)"
            ),
        ],
        cwd=pack_root,
        env=oracle_env,
    )
    require_success("CASA plotms UV coverage text export", casa_txt_run)
    casa_numerical_comparison = compare_manifest_to_casa_txt(cli_txt, casa_txt)

    if args.skip_tui_screenshot:
        tui_capture = {
            "status": "skipped",
            "artifacts": [file_record(tui_png, pack_root)],
        }
    else:
        build_run: dict[str, Any] | None = None
        if not args.skip_ghostty_capture_build:
            build_env = os.environ.copy()
            build_env["GHOSTTYKIT_XCFRAMEWORK"] = str(args.ghosttykit_xcframework)
            build_run = run_command(
                [str(args.ghostty_capture_build_script), str(args.ghostty_capture_binary)],
                cwd=REPO_ROOT,
                env=build_env,
            )
            require_success("GhosttyKit TUI capture build", build_run)

        capture_env = os.environ.copy()
        capture_env["XDG_CACHE_HOME"] = capture_env.get("XDG_CACHE_HOME", "/private/tmp/ghostty-cache")
        tui_run = run_command(
            [
                str(args.ghostty_capture_binary),
                "--cwd",
                str(pack_root),
                "--output",
                str(tui_png),
                "--width",
                "2200",
                "--height",
                "1400",
                "--font-size",
                "12",
                "--settle-seconds",
                "12",
                "--",
                str(args.casars_binary),
                "msexplore",
                "--preset",
                "uv_coverage",
                "--avgchannel",
                "10000",
                "--avgtime",
                "1e9",
                "--color-by",
                "field",
                str(pack_root / MS_PATH),
            ],
            cwd=pack_root,
            env=capture_env,
        )
        tui_capture = {
            "status": "succeeded" if tui_run["exit_code"] == 0 and tui_png.exists() else "failed",
            "exit_code": tui_run["exit_code"],
            "stdout": tui_run["stdout"],
            "stderr": tui_run["stderr"],
            "artifacts": [file_record(tui_png, pack_root)],
            "build": build_run,
            "capture_provenance": "GhosttyKit macOS Metal renderer IOSurface/CGImage layer",
        }
        if tui_capture["status"] == "succeeded":
            runs.append(
                {
                    "surface": "tui",
                    "task": "msexplore",
                    "elapsed_seconds": tui_run["elapsed_seconds"],
                }
            )

    if args.skip_gui_capture:
        gui_capture = {
            "status": "skipped",
            "artifact": file_record(gui_png, pack_root),
        }
    else:
        gui_run = run_command(
            [
                str(args.gui_app_binary),
                "--capture-gui-evidence",
                "--open-tutorial-pack",
                str(pack_root),
                "--dataset",
                MS_PATH,
                "--output",
                str(gui_png),
                "--width",
                "1440",
                "--height",
                "960",
            ],
            cwd=REPO_ROOT,
            env=os.environ.copy(),
        )
        gui_capture = {
            "status": "succeeded" if gui_run["exit_code"] == 0 and gui_png.exists() else "failed",
            "exit_code": gui_run["exit_code"],
            "stdout": gui_run["stdout"],
            "stderr": gui_run["stderr"],
            "artifact": file_record(gui_png, pack_root),
        }
        if gui_capture["status"] == "succeeded":
            runs.append(
                {
                    "surface": "gui",
                    "task": "msexplore",
                    "elapsed_seconds": gui_run["elapsed_seconds"],
                }
            )

    comparison = compare_images(casa_png, cli_png) if casa_png.exists() and cli_png.exists() else {
        "status": "not-run",
        "reason": "CASA or casa-rs PNG is missing",
    }

    evidence = {
        "section_id": SECTION_ID,
        "parameters": {
            "vis": MS_PATH,
            "xaxis": "u",
            "yaxis": "v",
            "avgchannel": "10000",
            "avgspw": False,
            "avgtime": "1e9",
            "avgscan": False,
            "coloraxis": "field",
        },
        "native_artifacts": [
            image_record(cli_png, pack_root),
            image_record(python_png, pack_root),
            file_record(cli_txt, pack_root),
            file_record(python_txt, pack_root),
        ],
        "oracle": oracle,
        "tui_capture": tui_capture,
        "gui_capture": gui_capture,
        "comparison": comparison,
        "numerical_comparison": numerical_comparison,
        "casa_numerical_comparison": casa_numerical_comparison,
        "timings": runs,
    }
    write_json(evidence_dir / "02-uv-coverage.json", evidence)
    write_docs(pack_root, evidence)
    write_review_record(pack_root)


if __name__ == "__main__":
    main()
