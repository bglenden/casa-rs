#!/usr/bin/env python3
"""Run the ALMA First Look Imaging amplitude-vs-UV-distance tutorial section."""

from __future__ import annotations

import argparse
import html
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

from tutorial_plot_manifest import compare_iterated_manifest_to_casa_txts, compare_manifests


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
SECTION_ID = "03-amplitude-uvdist-by-field"
MS_PATH = "twhya_calibrated.ms"
CASA_GUIDE_IMAGE = "https://casaguides.nrao.edu/images/c/ca/Imaging-tutorial-amp-uvdist-ceres_6.5.4.jpg"
CASA_GUIDE_URL = "https://casaguides.nrao.edu/index.php?title=First_Look_at_Imaging_CASA_6.5.4#A_First_Look_at_the_Data"
FIELD_ID_BY_LABEL = {
    "J0522-364": "0",
    "Ceres": "2",
    "J1037-295": "3",
    "TW Hya": "5",
    "3c279": "6",
}


def run_command(
    args: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    start = time.perf_counter()
    try:
        result = subprocess.run(
            args,
            cwd=cwd,
            env=env,
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


def image_tags(paths: list[str], alt_prefix: str) -> str:
    if not paths:
        return "<p class=\"warn\">No image artifact was generated.</p>"
    return "\n".join(
        f'<img src="../../{html.escape(path)}" alt="{html.escape(alt_prefix)} {index + 1}">'
        for index, path in enumerate(paths)
    )


def generated_plotms_pages(base_png: Path) -> list[Path]:
    return sorted(base_png.parent.glob(f"{base_png.stem}*.png"))


def write_docs(pack_root: Path, evidence: dict[str, Any]) -> None:
    docs_dir = pack_root / "docs" / "sections"
    docs_dir.mkdir(parents=True, exist_ok=True)
    cli_png = ".casa-rs/workspace/native/03-amplitude-uvdist-by-field/cli-msexplore-amplitude-uvdist-field.png"
    python_png = ".casa-rs/workspace/native/03-amplitude-uvdist-by-field/python-msexplore-amplitude-uvdist-field.png"
    casa_png = ".casa-rs/workspace/oracle/03-amplitude-uvdist-by-field/casa-plotms-amplitude-uvdist-field.png"
    tui_png = ".casa-rs/screenshots/headless/03-amplitude-uvdist-by-field-tui.png"
    gui_png = ".casa-rs/screenshots/gui/03-amplitude-uvdist-by-field-gui-workbench-dark.png"
    oracle = evidence.get("oracle", {})
    numerical = evidence.get("numerical_comparison", {})
    casa_numerical = evidence.get("casa_numerical_comparison", {})
    numerical_pre = html.escape(json.dumps(numerical, indent=2, sort_keys=True))
    casa_numerical_pre = html.escape(json.dumps(casa_numerical, indent=2, sort_keys=True))
    numerical_status = "passed" if numerical.get("byte_identical") else "failed"
    casa_numerical_status = casa_numerical.get("status", "not-run")
    point_count = numerical.get("reference", {}).get("point_count", "unknown")
    casa_artifacts = [
        item["path"]
        for item in oracle.get("artifacts", [])
        if item.get("exists") and item.get("path", "").endswith(".png")
    ]
    markdown = f"""# 03 - Plot amplitude vs. UV distance by field

CASA tutorial chunk:

```python
plotms(vis="twhya_calibrated.ms", xaxis="uvdist", yaxis="amp",
       avgchannel="10000", avgspw=False, avgtime="1e9",
       avgscan=False, coloraxis="field", iteraxis="field")
```

CASA source page: {CASA_GUIDE_URL}

## Parameters

| Surface | Task | Parameters |
| --- | --- | --- |
| CASA | `plotms` | `vis="twhya_calibrated.ms"`, `xaxis="uvdist"`, `yaxis="amp"`, `avgchannel="10000"`, `avgspw=False`, `avgtime="1e9"`, `avgscan=False`, `coloraxis="field"`, Page tab `Iteration Axis=Field`, export range `all` |
| CLI | `msexplore` | `--preset amplitude_vs_uv_distance --avgchannel 10000 --avgtime 1e9 --color-by field --iteraxis field --plot-output {cli_png} --plot-width 1600 --plot-height 1200 --overwrite twhya_calibrated.ms` |
| Python | `casars.tasks.msexplore.plot` | `measurement_set="twhya_calibrated.ms"`, `output_path="{python_png}"`, `preset="amplitude_vs_uv_distance"`, `avgchannel=10000`, `avgtime=1e9`, `color_by="Field"`, `iteraxis="field"`, `width=1600`, `height=1200` |
| TUI | `MSExplore > Views` | `Preset=Amplitude vs UV Distance`, `Average channels=10000`, `Average time=1e9`, `Color by=Field`, `Iterate by=Field` |
| GUI | `MeasurementSet Explorer > Plots > Selections` | `Plot=Amplitude vs UV Distance`, `Avg channel=10000`, `Avg time=1e9`, `Iterate by=Field`, `Generate` |

## Observable Result

The iterated plot should include one panel per field. The Ceres panel should show a clear amplitude decrease with increasing UV distance, matching the CASA tutorial observation that Ceres is resolved by ALMA.

## Evidence

| Surface | Artifact |
| --- | --- |
| CASA Guide | `{CASA_GUIDE_IMAGE}` |
| CASA C++ | Qt offscreen `plotms` export pages; current status `{oracle.get("status", "not-run")}` |
| CLI | `{cli_png}` |
| Python | `{python_png}` |
| TUI | `{tui_png}` captured from GhosttyKit's macOS Metal renderer layer |
| GUI | `{gui_png}` captured with the macOS offscreen GUI evidence renderer |

## Numerical/Structural Regression Evidence

The CLI and Python surfaces also export a `txt` manifest containing the exact plotted points after the tutorial averaging and iteration parameters are applied. For this run the manifests are byte-identical: `{numerical.get("byte_identical")}`. Point count: `{point_count}`.

The casa-rs iterated manifest is also compared panel-by-panel with CASA `plotms` `expformat="txt"` exports for each field using point counts plus absolute and relative value tolerances. CASA numeric comparison status: `{casa_numerical_status}`.

![CASA Guide amplitude vs UV distance, Ceres page]({CASA_GUIDE_IMAGE})

{chr(10).join(f"![CASA C++ local plotms export]({ '../../' + path })" for path in casa_artifacts) if casa_artifacts else "CASA C++ local plotms export was not generated in this run."}

![casa-rs CLI amplitude vs UV distance by field](../../{cli_png})

![casa-rs Python amplitude vs UV distance by field](../../{python_png})

![casa-rs TUI GhosttyKit renderer capture](../../{tui_png})

![casa-rs GUI amplitude vs UV distance by field](../../{gui_png})

## Review State

CASA guide, CLI, Python, TUI, and GUI evidence has been generated. Local CASA `plotms` oracle status: `{oracle.get("status", "not-run")}`.
"""
    (docs_dir / "03-amplitude-uvdist-by-field.md").write_text(markdown, encoding="utf-8")

    oracle_stderr = html.escape(oracle.get("stderr", ""))
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ALMA First Look Imaging - 03 Amplitude vs UV distance</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; line-height: 1.45; margin: 2rem; max-width: 1280px; }}
table {{ border-collapse: collapse; width: 100%; }}
td, th {{ border: 1px solid #c7c7c7; padding: 0.45rem; vertical-align: top; }}
pre {{ background: #f6f6f6; border: 1px solid #ddd; padding: 1rem; overflow: auto; }}
.grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; }}
.wide {{ grid-column: 1 / -1; }}
img {{ max-width: 100%; border: 1px solid #ddd; }}
.ok {{ color: #176b2c; font-weight: 600; }}
.pending {{ color: #8a5a00; font-weight: 600; }}
</style>
</head>
<body>
<h1>ALMA First Look Imaging: 03 Amplitude vs UV distance by field</h1>
<p>CASA source workflow: in <code>plotms</code>, set <code>X Axis=UVDist</code>, <code>Data=Amp</code>, average channels/time as in the previous plot, then set the Page tab <code>Iteration Axis=Field</code>. The tutorial's Figure 2 shows the Ceres page.</p>
<p>CASA source page: <a href="{CASA_GUIDE_URL}">{CASA_GUIDE_URL}</a></p>
<h2>Parameters</h2>
<table>
<tr><th>Surface</th><th>Task</th><th>Parameters</th></tr>
<tr><td>CASA</td><td><code>plotms</code></td><td><code>vis=twhya_calibrated.ms</code>, <code>xaxis=uvdist</code>, <code>yaxis=amp</code>, <code>avgchannel=10000</code>, <code>avgtime=1e9</code>, <code>avgscan=False</code>, <code>avgspw=False</code>, <code>coloraxis=field</code>, <code>iteraxis=field</code>, <code>exprange=all</code></td></tr>
<tr><td>CLI</td><td><code>msexplore</code></td><td><code>--preset amplitude_vs_uv_distance --avgchannel 10000 --avgtime 1e9 --color-by field --iteraxis field --plot-output {cli_png} --plot-width 1600 --plot-height 1200 --overwrite twhya_calibrated.ms</code></td></tr>
<tr><td>Python</td><td><code>casars.tasks.msexplore.plot</code></td><td><code>preset=amplitude_vs_uv_distance</code>, <code>avgchannel=10000</code>, <code>avgtime=1e9</code>, <code>color_by=Field</code>, <code>iteraxis=field</code>, <code>width=1600</code>, <code>height=1200</code></td></tr>
<tr><td>TUI</td><td><code>MSExplore &gt; Views</code></td><td><code>Preset=Amplitude vs UV Distance</code>, <code>Avg channel=10000</code>, <code>Avg time=1e9</code>, <code>Color by=Field</code>, <code>Iterate by=Field</code></td></tr>
<tr><td>GUI</td><td><code>MeasurementSet Explorer &gt; Plots</code></td><td><code>Plot=Amplitude vs UV Distance</code>, <code>Avg channel=10000</code>, <code>Avg time=1e9</code>, <code>Iterate by=Field</code></td></tr>
</table>
<h2>Visible Evidence</h2>
<p class="ok">CLI: <code>{html.escape(evidence["cli"]["status"])}</code>. Python: <code>{html.escape(evidence["python"]["status"])}</code>. TUI: <code>{html.escape(evidence.get("tui_capture", {}).get("status", "not-run"))}</code>. GUI: <code>{html.escape(evidence.get("gui_capture", {}).get("status", "not-run"))}</code>. Local CASA plotms: <code>{html.escape(oracle.get("status", "not-run"))}</code>.</p>
<h2>Numerical/Structural Regression Evidence</h2>
<p class="{"ok" if numerical_status == "passed" else "pending"}">CLI and Python point manifests: <code>{html.escape(numerical_status)}</code>. Point count: <code>{html.escape(str(point_count))}</code>.</p>
<p>The manifest records the exact plotted points after <code>avgchannel=10000</code>, <code>avgtime=1e9</code>, <code>color_by=Field</code>, and <code>iteraxis=Field</code>. This catches data-vector, panel-label, series-label, axis-range, and wrapper-routing regressions separately from the rendered PNG comparison.</p>
<pre>{numerical_pre}</pre>
<h3>CASA plotms Text Export Comparison</h3>
<p>Status: <code>{html.escape(casa_numerical_status)}</code>.</p>
<pre>{casa_numerical_pre}</pre>
<div class="grid">
<section><h3>CASA Guide GUI Snapshot</h3><img src="{CASA_GUIDE_IMAGE}" alt="CASA Guide Ceres amplitude vs UV distance"></section>
<section><h3>CASA C++ Offscreen Export</h3>{image_tags(casa_artifacts, "CASA plotms offscreen export")}</section>
<section><h3>casa-rs CLI</h3><img src="../../{cli_png}" alt="casa-rs CLI amplitude vs UV distance"></section>
<section><h3>casa-rs Python</h3><img src="../../{python_png}" alt="casa-rs Python amplitude vs UV distance"></section>
<section class="wide"><h3>casa-rs TUI</h3><img src="../../{tui_png}" alt="casa-rs TUI amplitude vs UV distance"><p>Captured from GhosttyKit's macOS Metal renderer layer; no separate plot image was overlaid.</p></section>
<section><h3>casa-rs GUI</h3><img src="../../{gui_png}" alt="casa-rs GUI amplitude vs UV distance"></section>
</div>
<h2>CASA Oracle Note</h2>
<pre>{oracle_stderr}</pre>
</body>
</html>
"""
    (docs_dir / "03-amplitude-uvdist-by-field.html").write_text(html_doc, encoding="utf-8")


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
    for directory in [native_dir, oracle_dir, evidence_dir, screenshot_dir, gui_screenshot_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    cli_png = native_dir / "cli-msexplore-amplitude-uvdist-field.png"
    python_png = native_dir / "python-msexplore-amplitude-uvdist-field.png"
    cli_txt = native_dir / "cli-msexplore-amplitude-uvdist-field.txt"
    python_txt = native_dir / "python-msexplore-amplitude-uvdist-field.txt"
    casa_png = oracle_dir / "casa-plotms-amplitude-uvdist-field.png"
    tui_png = screenshot_dir / "03-amplitude-uvdist-by-field-tui.png"
    gui_png = gui_screenshot_dir / "03-amplitude-uvdist-by-field-gui-workbench-dark.png"
    runs: list[dict[str, Any]] = []

    cli_run = run_command(
        [
            str(args.msexplore_binary),
            "--preset",
            "amplitude_vs_uv_distance",
            "--avgchannel",
            "10000",
            "--avgtime",
            "1e9",
            "--color-by",
            "field",
            "--iteraxis",
            "field",
            "--plot-output",
            str(cli_png.relative_to(pack_root)),
            "--plot-width",
            "1600",
            "--plot-height",
            "1200",
            "--overwrite",
            MS_PATH,
        ],
        cwd=pack_root,
    )
    require_success("CLI msexplore amplitude vs UV distance", cli_run)
    runs.append({"surface": "cli", "task": "msexplore", "elapsed_seconds": cli_run["elapsed_seconds"]})

    cli_txt_run = run_command(
        [
            str(args.msexplore_binary),
            "--preset",
            "amplitude_vs_uv_distance",
            "--avgchannel",
            "10000",
            "--avgtime",
            "1e9",
            "--color-by",
            "field",
            "--iteraxis",
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
    require_success("CLI msexplore amplitude vs UV distance manifest", cli_txt_run)

    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT / "crates" / "casars-python" / "python") + os.pathsep + env.get("PYTHONPATH", "")
    python_code = (
        "from casars.tasks import msexplore; "
        "msexplore.plot("
        f"{MS_PATH!r}, {str(python_png.relative_to(pack_root))!r}, "
        "preset='amplitude_vs_uv_distance', avgchannel=10000, avgtime=1e9, "
        "color_by='Field', iteraxis='field', width=1600, height=1200, "
        f"binary={str(args.msexplore_binary)!r})"
    )
    python_run = run_command(["python", "-c", python_code], cwd=pack_root, env=env)
    require_success("Python msexplore amplitude vs UV distance", python_run)
    runs.append({"surface": "python", "task": "msexplore.plot", "elapsed_seconds": python_run["elapsed_seconds"]})

    python_txt_code = (
        "from casars.tasks import msexplore; "
        "msexplore.plot("
        f"{MS_PATH!r}, {str(python_txt.relative_to(pack_root))!r}, "
        "preset='amplitude_vs_uv_distance', avgchannel=10000, avgtime=1e9, "
        "color_by='Field', iteraxis='field', format='txt', width=1600, height=1200, "
        f"binary={str(args.msexplore_binary)!r})"
    )
    python_txt_run = run_command(["python", "-c", python_txt_code], cwd=pack_root, env=env)
    require_success("Python msexplore amplitude vs UV distance manifest", python_txt_run)
    numerical_comparison = compare_manifests(cli_txt, python_txt)

    oracle_env = os.environ.copy()
    oracle_env["DISPLAY"] = oracle_env.get("DISPLAY", ":99")
    oracle_env["QT_QPA_PLATFORM"] = oracle_env.get("QT_QPA_PLATFORM", "offscreen")
    oracle_env["MPLBACKEND"] = oracle_env.get("MPLBACKEND", "Agg")
    oracle_env["MPLCONFIGDIR"] = oracle_env.get("MPLCONFIGDIR", str(pack_root / ".casa-rs" / "workspace" / "oracle" / "matplotlib"))
    for stale_page in generated_plotms_pages(casa_png):
        stale_page.unlink()
    oracle_run = run_command(
        [
            str(args.casa_python),
            "-c",
            (
                "from casaplotms import plotms; "
                "plotms(vis='twhya_calibrated.ms', xaxis='uvdist', yaxis='amp', "
                "avgchannel='10000', avgspw=False, avgtime='1e9', avgscan=False, "
                "coloraxis='field', iteraxis='field', exprange='all', showgui=False, "
                f"plotfile={str(casa_png.relative_to(pack_root))!r}, expformat='png', "
                "overwrite=True, clearplots=True, width=1600, height=1200, dpi=72)"
            ),
        ],
        cwd=pack_root,
        env=oracle_env,
        timeout_seconds=180,
    )
    oracle_pages = generated_plotms_pages(casa_png)
    casa_txt_by_panel: dict[str, Path] = {}
    for field_label, field_id in FIELD_ID_BY_LABEL.items():
        casa_txt = oracle_dir / f"casa-plotms-amplitude-uvdist-field-{field_id}.txt"
        casa_txt_run = run_command(
            [
                str(args.casa_python),
                "-c",
                (
                    "from casaplotms import plotms; "
                    "plotms(vis='twhya_calibrated.ms', xaxis='uvdist', yaxis='amp', "
                    f"field={field_id!r}, "
                    "avgchannel='10000', avgspw=False, avgtime='1e9', avgscan=False, "
                    "coloraxis='field', showgui=False, "
                    f"plotfile={str(casa_txt.relative_to(pack_root))!r}, expformat='txt', "
                    "overwrite=True, clearplots=True, verbose=True)"
                ),
            ],
            cwd=pack_root,
            env=oracle_env,
            timeout_seconds=180,
        )
        require_success(f"CASA plotms amplitude vs UV distance text export {field_label}", casa_txt_run)
        casa_txt_by_panel[field_label] = casa_txt
    casa_numerical_comparison = compare_iterated_manifest_to_casa_txts(cli_txt, casa_txt_by_panel)
    oracle = {
        "status": "succeeded" if oracle_run["exit_code"] == 0 and oracle_pages else "not-generated",
        "exit_code": oracle_run["exit_code"],
        "timed_out": oracle_run.get("timed_out", False),
        "stdout": oracle_run["stdout"],
        "stderr": oracle_run["stderr"],
        "artifact": file_record(oracle_pages[0] if oracle_pages else casa_png, pack_root),
        "artifacts": [file_record(path, pack_root) for path in oracle_pages],
    }
    if oracle["status"] == "not-generated" and oracle["timed_out"]:
        oracle["status"] = "timed-out"
    if oracle["status"] == "not-generated" and "DISPLAY environment variable is not set" in oracle_run["stderr"]:
        oracle["status"] = "blocked-no-display"

    if args.skip_tui_screenshot:
        tui_capture = {"status": "skipped", "artifacts": [file_record(tui_png, pack_root)]}
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
                "60",
                "--",
                str(args.casars_binary),
                "msexplore",
                "--preset",
                "amplitude_vs_uv_distance",
                "--avgchannel",
                "10000",
                "--avgtime",
                "1e9",
                "--color-by",
                "field",
                "--iteraxis",
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
            runs.append({"surface": "tui", "task": "msexplore", "elapsed_seconds": tui_run["elapsed_seconds"]})

    if args.skip_gui_capture:
        gui_capture = {"status": "skipped", "artifact": file_record(gui_png, pack_root)}
    else:
        gui_run = run_command(
            [
                str(args.gui_app_binary),
                "--capture-gui-evidence",
                "--open-tutorial-pack",
                str(pack_root),
                "--dataset",
                MS_PATH,
                "--preset",
                "amplitude_vs_uv_distance",
                "--iteraxis",
                "field",
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
            runs.append({"surface": "gui", "task": "msexplore", "elapsed_seconds": gui_run["elapsed_seconds"]})

    evidence = {
        "section_id": SECTION_ID,
        "parameters": {
            "vis": MS_PATH,
            "xaxis": "uvdist",
            "yaxis": "amp",
            "avgchannel": "10000",
            "avgspw": False,
            "avgtime": "1e9",
            "avgscan": False,
            "coloraxis": "field",
            "iteraxis": "field",
        },
        "cli": {"status": "succeeded", "artifact": file_record(cli_png, pack_root), "manifest": file_record(cli_txt, pack_root), "stdout": cli_run["stdout"], "stderr": cli_run["stderr"]},
        "python": {"status": "succeeded", "artifact": file_record(python_png, pack_root), "manifest": file_record(python_txt, pack_root), "stdout": python_run["stdout"], "stderr": python_run["stderr"]},
        "numerical_comparison": numerical_comparison,
        "oracle": oracle,
        "casa_numerical_comparison": casa_numerical_comparison,
        "tui_capture": tui_capture,
        "gui_capture": gui_capture,
        "timings": runs,
    }
    write_json(evidence_dir / f"{SECTION_ID}.json", evidence)
    write_docs(pack_root, evidence)


if __name__ == "__main__":
    main()
