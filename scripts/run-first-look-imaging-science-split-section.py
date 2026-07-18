#!/usr/bin/env python3
"""Run the ALMA First Look Imaging science-target split/listobs section."""

from __future__ import annotations

import argparse
import html
import json
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
DEFAULT_MSTRANSFORM_BINARY = REPO_ROOT / "target" / "debug" / "mstransform"
DEFAULT_MSEXPLORE_BINARY = REPO_ROOT / "target" / "debug" / "msexplore"
DEFAULT_GUI_APP_BINARY = REPO_ROOT / "apps" / "casars-mac" / ".build" / "debug" / "casars-mac"
DEFAULT_GHOSTTY_CAPTURE_BINARY = Path("/private/tmp/ghostty-surface-capture")
SECTION_ID = "06-science-target-split"
MS_PATH = "twhya_calibrated.ms"
CASA_OUTPUT = ".casa-rs/workspace/oracle/06-science-target-split/casa-twhya_smoothed.ms"
SURFACE_OUTPUTS = {
    "cli": "twhya_smoothed.cli.ms",
    "python": "twhya_smoothed.python.ms",
    "tui": "twhya_smoothed.tui.ms",
    "gui": "twhya_smoothed.gui.ms",
}
CASA_GUIDE_URL = "https://casaguides.nrao.edu/index.php/First_Look_at_Imaging"


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


def parse_marked_json(stdout: str) -> dict[str, Any]:
    start = stdout.find("JSON_RESULT_START")
    end = stdout.find("JSON_RESULT_END")
    if start < 0 or end < 0 or end <= start:
        raise ValueError(f"marked JSON payload not found in stdout:\n{stdout}")
    return json.loads(stdout[start + len("JSON_RESULT_START") : end].strip())


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists() or path.is_symlink():
        path.unlink()


def first_lines(path: Path, count: int = 36) -> str:
    if not path.exists():
        return ""
    return "\n".join(path.read_text(encoding="utf-8", errors="replace").splitlines()[:count])


def file_record(path: Path, pack_root: Path) -> dict[str, Any]:
    return {
        "path": str(path.relative_to(pack_root)),
        "exists": path.exists(),
        "size_bytes": path.stat().st_size if path.exists() and path.is_file() else None,
    }


def python_surface_code(outputvis: str, binary: str) -> str:
    return (
        "import json; "
        "from casars import tasks; "
        "result = tasks.split(vis='twhya_calibrated.ms', "
        f"outputvis={outputvis!r}, field='5', width=8, datacolumn='data', binary={binary!r}); "
        "print(json.dumps(json.loads(result.stdout), sort_keys=True))"
    )


def comparison_code(casa_output: str, surface_outputs: dict[str, str], output_json: str) -> str:
    return f"""
from casatools import table
import json
import numpy as np
from pathlib import Path

casa_output = Path({casa_output!r})
surface_outputs = {{k: Path(v) for k, v in {surface_outputs!r}.items()}}
output_json = Path({output_json!r})
tb = table()

def ms_summary(path):
    tb.open(str(path))
    try:
        data0 = tb.getcell("DATA", 0)
        flag0 = tb.getcell("FLAG", 0)
        summary = {{
            "main_rows": int(tb.nrows()),
            "data_shape_row0": list(data0.shape),
            "field_id_counts": {{}},
            "data_desc_id_counts": {{}},
        }}
        for value in tb.getcol("FIELD_ID").tolist():
            key = str(int(value))
            summary["field_id_counts"][key] = summary["field_id_counts"].get(key, 0) + 1
        for value in tb.getcol("DATA_DESC_ID").tolist():
            key = str(int(value))
            summary["data_desc_id_counts"][key] = summary["data_desc_id_counts"].get(key, 0) + 1
    finally:
        tb.close()
    tb.open(str(path / "FIELD"))
    try:
        summary["field_names"] = [str(name) for name in tb.getcol("NAME").tolist()]
    finally:
        tb.close()
    tb.open(str(path / "SPECTRAL_WINDOW"))
    try:
        summary["spw_rows"] = int(tb.nrows())
        summary["num_chan"] = [int(v) for v in tb.getcol("NUM_CHAN").tolist()]
        summary["chan_freq_first5"] = [float(v) for v in tb.getcell("CHAN_FREQ", 0)[:5].tolist()]
        summary["chan_width_first5"] = [float(v) for v in tb.getcell("CHAN_WIDTH", 0)[:5].tolist()]
    finally:
        tb.close()
    return summary, data0, flag0

casa_summary, casa_data, casa_flag = ms_summary(casa_output)
surfaces = {{}}
status = "passed"
for name, path in surface_outputs.items():
    summary, data, flag = ms_summary(path)
    diff = np.abs(casa_data - data)
    item = {{
        "summary": summary,
        "diff": {{
            "data_row0_max_abs": float(np.nanmax(diff)),
            "data_row0_rms_abs": float(np.sqrt(np.nanmean(diff ** 2))),
            "flag_row0_equal": bool(np.array_equal(casa_flag, flag)),
            "row_delta": int(summary["main_rows"] - casa_summary["main_rows"]),
            "num_chan_equal": summary["num_chan"] == casa_summary["num_chan"],
            "field_names_equal": summary["field_names"] == casa_summary["field_names"],
        }},
    }}
    if (
        item["diff"]["data_row0_max_abs"] > 1.0e-6
        or not item["diff"]["flag_row0_equal"]
        or item["diff"]["row_delta"] != 0
        or not item["diff"]["num_chan_equal"]
        or not item["diff"]["field_names_equal"]
    ):
        status = "failed"
    surfaces[name] = item

payload = {{
    "status": status,
    "casa": {{"summary": casa_summary}},
    "surfaces": surfaces,
}}
output_json.parent.mkdir(parents=True, exist_ok=True)
output_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\\n", encoding="utf-8")
print("JSON_RESULT_START")
print(json.dumps(payload, indent=2, sort_keys=True))
print("JSON_RESULT_END")
"""


def gui_env(mstransform_binary: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["CASARS_MSTRANSFORM_BIN"] = str(mstransform_binary)
    env["CASA_RS_REPO_ROOT"] = str(REPO_ROOT)
    return env


def write_docs(pack_root: Path, evidence: dict[str, Any]) -> None:
    docs_dir = pack_root / "docs" / "sections"
    docs_dir.mkdir(parents=True, exist_ok=True)
    casa_listobs = first_lines(pack_root / ".casa-rs/workspace/oracle/06-science-target-split/casa-listobs.txt")
    cli_listobs = first_lines(pack_root / ".casa-rs/workspace/native/06-science-target-split/cli-listobs.txt")
    comparison = evidence["comparison"]
    gui_run = evidence["gui"]["run_screenshot"]
    gui_summary = evidence["gui"]["summary_screenshot"]
    tui_png = evidence["tui"]["screenshot"]
    rows = comparison["casa"]["summary"]["main_rows"]
    chans = comparison["casa"]["summary"]["num_chan"]
    field_names = ", ".join(comparison["casa"]["summary"]["field_names"])
    surface_rows = "\n".join(
        f"<tr><td>{html.escape(name)}</td><td>{item['diff']['row_delta']}</td>"
        f"<td>{item['diff']['data_row0_max_abs']:.3g}</td>"
        f"<td>{item['diff']['data_row0_rms_abs']:.3g}</td>"
        f"<td>{item['diff']['flag_row0_equal']}</td></tr>"
        for name, item in comparison["surfaces"].items()
    )
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ALMA First Look Imaging - 06 science target split</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; line-height: 1.45; margin: 2rem; max-width: 1220px; }}
table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; }}
td, th {{ border: 1px solid #c7c7c7; padding: 0.45rem; vertical-align: top; }}
pre {{ background: #f6f6f6; border: 1px solid #ddd; padding: 1rem; overflow: auto; }}
img {{ max-width: 100%; border: 1px solid #bbb; }}
.grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }}
.ok {{ color: #116611; font-weight: 600; }}
</style>
</head>
<body>
<h1>ALMA First Look Imaging: 06 science-target split/listobs</h1>
<p>CASA source: <code>split(vis='twhya_calibrated.ms', field='5', width='8', outputvis='twhya_smoothed.ms', datacolumn='data')</code>, then <code>listobs('twhya_smoothed.ms')</code>.</p>
<p class="ok">Status: <code>{html.escape(comparison["status"])}</code></p>
<h2>Parameters</h2>
<table>
<tr><th>Surface</th><th>Task</th><th>Parameters</th></tr>
<tr><td>CASA</td><td><code>split</code></td><td><code>vis=twhya_calibrated.ms</code>, <code>field=5</code>, <code>width=8</code>, <code>outputvis=twhya_smoothed.ms</code>, <code>datacolumn=data</code></td></tr>
<tr><td>CLI</td><td><code>mstransform</code>/<code>split</code></td><td><code>--vis twhya_calibrated.ms --outputvis twhya_smoothed.cli.ms --field 5 --width 8 --datacolumn DATA</code></td></tr>
<tr><td>Python</td><td><code>casars.tasks.split</code></td><td><code>vis='twhya_calibrated.ms'</code>, <code>outputvis='twhya_smoothed.python.ms'</code>, <code>field='5'</code>, <code>width=8</code>, <code>datacolumn='data'</code></td></tr>
<tr><td>TUI</td><td><code>casars split</code></td><td>Same parameters as CLI; captured with <code>tools/ghostty-surface-capture</code>.</td></tr>
<tr><td>GUI</td><td><code>Tasks &gt; Split</code></td><td><code>Input MS=twhya_calibrated.ms</code>, <code>Output MS=twhya_smoothed.gui.ms</code>, <code>Field=5</code>, <code>Spectral Window=0</code> (equivalent to CASA's omitted/default SPW for this one-SPW dataset), <code>Channel Width=8</code>, <code>Data Column=DATA</code>, <code>Keep Fully Flagged Rows=true</code>.</td></tr>
</table>
<h2>Observable Result</h2>
<ul>
<li>rows: <code>{rows}</code></li>
<li>field names: <code>{html.escape(field_names)}</code></li>
<li>spectral-window channels: <code>{html.escape(str(chans))}</code></li>
</ul>
<h2>Numeric/Product Comparison</h2>
<table>
<tr><th>Surface</th><th>Row delta vs CASA</th><th>DATA row 0 max abs</th><th>DATA row 0 RMS abs</th><th>FLAG row 0 equal</th></tr>
{surface_rows}
</table>
<h2>Visible Evidence</h2>
<div class="grid">
<section><h3>CASA listobs excerpt</h3><pre>{html.escape(casa_listobs)}</pre></section>
<section><h3>casa-rs CLI msexplore excerpt</h3><pre>{html.escape(cli_listobs)}</pre></section>
<section><h3>casa-rs TUI split run</h3><img src="../../{html.escape(tui_png)}" alt="casa-rs TUI split run"></section>
<section><h3>casa-rs GUI split run</h3><img src="../../{html.escape(gui_run)}" alt="casa-rs GUI split run"></section>
<section><h3>casa-rs GUI summary of output MS</h3><img src="../../{html.escape(gui_summary)}" alt="casa-rs GUI msexplore summary"></section>
</div>
</body>
</html>
"""
    (docs_dir / f"{SECTION_ID}.html").write_text(html_doc, encoding="utf-8")

    markdown = f"""# 06 - Split science target and inspect listobs

CASA tutorial chunk:

```python
split(vis='twhya_calibrated.ms', field='5', width='8',
      outputvis='twhya_smoothed.ms', datacolumn='data')
listobs('twhya_smoothed.ms')
```

Status: `{comparison["status"]}`

Rows: `{rows}`
Field names: `{field_names}`
Spectral-window channels: `{chans}`

See `{SECTION_ID}.html` for GUI/TUI screenshots and the CASA/casa-rs comparison table.
"""
    (docs_dir / f"{SECTION_ID}.md").write_text(markdown, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pack-root", type=Path, default=DEFAULT_PACK_ROOT)
    parser.add_argument("--casa-python", type=Path, default=DEFAULT_CASA_PYTHON)
    parser.add_argument("--python", type=Path, default=DEFAULT_PYTHON)
    parser.add_argument("--casars-binary", type=Path, default=DEFAULT_CASARS_BINARY)
    parser.add_argument("--mstransform-binary", type=Path, default=DEFAULT_MSTRANSFORM_BINARY)
    parser.add_argument("--msexplore-binary", type=Path, default=DEFAULT_MSEXPLORE_BINARY)
    parser.add_argument("--gui-app-binary", type=Path, default=DEFAULT_GUI_APP_BINARY)
    parser.add_argument("--ghostty-capture-binary", type=Path, default=DEFAULT_GHOSTTY_CAPTURE_BINARY)
    parser.add_argument("--timeout-seconds", type=float, default=360.0)
    args = parser.parse_args()

    pack_root = args.pack_root.expanduser().resolve()
    evidence_dir = pack_root / ".casa-rs" / "evidence"
    native_dir = pack_root / ".casa-rs" / "workspace" / "native" / SECTION_ID
    oracle_dir = pack_root / ".casa-rs" / "workspace" / "oracle" / SECTION_ID
    screenshot_dir = pack_root / ".casa-rs" / "screenshots"
    gui_dir = screenshot_dir / "gui"
    headless_dir = screenshot_dir / "headless"
    for path in [native_dir, oracle_dir, gui_dir, headless_dir]:
        path.mkdir(parents=True, exist_ok=True)

    casa_output = pack_root / CASA_OUTPUT
    remove_path(casa_output)
    for output in SURFACE_OUTPUTS.values():
        remove_path(pack_root / output)

    oracle_run = run_command(
        [
            str(args.casa_python),
            "-c",
            (
                "from casatasks import split; "
                f"split(vis={MS_PATH!r}, field='5', width='8', outputvis={CASA_OUTPUT!r}, datacolumn='data')"
            ),
        ],
        cwd=pack_root,
        timeout_seconds=args.timeout_seconds,
    )
    require_success("CASA split oracle", oracle_run)

    casa_listobs_run = run_command(
        [
            str(args.casa_python),
            "-c",
            (
                "from casatasks import listobs; "
                f"listobs(vis={CASA_OUTPUT!r}, listfile='.casa-rs/workspace/oracle/06-science-target-split/casa-listobs.txt', overwrite=True)"
            ),
        ],
        cwd=pack_root,
        timeout_seconds=args.timeout_seconds,
    )
    require_success("CASA listobs oracle", casa_listobs_run)

    cli_run = run_command(
        [
            str(args.mstransform_binary),
            "--vis",
            MS_PATH,
            "--outputvis",
            SURFACE_OUTPUTS["cli"],
            "--field",
            "5",
            "--width",
            "8",
            "--datacolumn",
            "DATA",
        ],
        cwd=pack_root,
        timeout_seconds=args.timeout_seconds,
    )
    require_success("casa-rs CLI split", cli_run)

    python_env = os.environ.copy()
    python_env["PYTHONPATH"] = (
        str(REPO_ROOT / "crates" / "casars-python" / "python")
        + os.pathsep
        + python_env.get("PYTHONPATH", "")
    )
    python_env["CASARS_MSTRANSFORM_BIN"] = str(args.mstransform_binary)
    python_run = run_command(
        [
            str(args.python),
            "-c",
            python_surface_code(SURFACE_OUTPUTS["python"], str(args.casars_binary)),
        ],
        cwd=pack_root,
        env=python_env,
        timeout_seconds=args.timeout_seconds,
    )
    require_success("casa-rs Python split", python_run)

    tui_capture_run = run_command(
        [
            str(args.ghostty_capture_binary),
            "--cwd",
            str(pack_root),
            "--output",
            str(headless_dir / f"{SECTION_ID}-tui.png"),
            "--width",
            "2200",
            "--height",
            "1400",
            "--font-size",
            "12",
            "--settle-seconds",
            "180",
            "--input-event",
            "500:r",
            "--input-event",
            "1500:r",
            "--input-event",
            "3500:r",
            "--",
            str(args.casars_binary),
            "split",
            "--vis",
            MS_PATH,
            "--outputvis",
            SURFACE_OUTPUTS["tui"],
            "--field",
            "5",
            "--width",
            "8",
            "--datacolumn",
            "DATA",
        ],
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        timeout_seconds=args.timeout_seconds + 60,
    )
    require_success("casa-rs TUI split capture", tui_capture_run)

    gui_run = run_command(
        [
            str(args.gui_app_binary),
            "--capture-gui-evidence",
            "--capture-kind",
            "split-run",
            "--open-tutorial-pack",
            str(pack_root),
            "--dataset",
            MS_PATH,
            "--output",
            str(gui_dir / f"{SECTION_ID}-gui-run.png"),
            "--width",
            "1800",
            "--height",
            "1200",
        ],
        cwd=REPO_ROOT,
        env=gui_env(args.mstransform_binary),
        timeout_seconds=args.timeout_seconds + 60,
    )
    require_success("casa-rs GUI split capture", gui_run)

    summary_runs = {}
    for surface, output in SURFACE_OUTPUTS.items():
        text_path = native_dir / f"{surface}-listobs.txt"
        json_path = native_dir / f"{surface}-listobs.json"
        text_run = run_command(
            [
                str(args.msexplore_binary),
                "--format",
                "text",
                "--output",
                str(text_path.relative_to(pack_root)),
                "--overwrite",
                output,
            ],
            cwd=pack_root,
            timeout_seconds=args.timeout_seconds,
        )
        require_success(f"{surface} msexplore text", text_run)
        json_run = run_command(
            [
                str(args.msexplore_binary),
                "--format",
                "json",
                "--output",
                str(json_path.relative_to(pack_root)),
                "--overwrite",
                output,
            ],
            cwd=pack_root,
            timeout_seconds=args.timeout_seconds,
        )
        require_success(f"{surface} msexplore json", json_run)
        summary_runs[surface] = {"text": text_run, "json": json_run}

    gui_summary_run = run_command(
        [
            str(args.gui_app_binary),
            "--capture-gui-evidence",
            "--capture-kind",
            "measurement-set-summary",
            "--open-tutorial-pack",
            str(pack_root),
            "--dataset",
            SURFACE_OUTPUTS["gui"],
            "--output",
            str(gui_dir / f"{SECTION_ID}-gui-summary.png"),
            "--width",
            "1800",
            "--height",
            "1200",
        ],
        cwd=REPO_ROOT,
        env=gui_env(args.mstransform_binary),
        timeout_seconds=90,
    )
    require_success("casa-rs GUI summary capture", gui_summary_run)

    comparison_run = run_command(
        [
            str(args.casa_python),
            "-c",
            comparison_code(
                CASA_OUTPUT,
                SURFACE_OUTPUTS,
                str(evidence_dir / f"{SECTION_ID}-comparison.json"),
            ),
        ],
        cwd=pack_root,
        timeout_seconds=args.timeout_seconds,
    )
    require_success("split output comparison", comparison_run)
    comparison = parse_marked_json(comparison_run["stdout"])

    evidence = {
        "section_id": SECTION_ID,
        "status": comparison["status"],
        "casa_source": {
            "guide_url": CASA_GUIDE_URL,
            "task_calls": [
                {
                    "task_id": "split",
                    "parameters": {
                        "vis": MS_PATH,
                        "field": "5",
                        "width": "8",
                        "outputvis": "twhya_smoothed.ms",
                        "datacolumn": "data",
                    },
                },
                {"task_id": "listobs", "parameters": {"vis": "twhya_smoothed.ms"}},
            ],
        },
        "oracle": {
            "split_run": oracle_run,
            "listobs_run": casa_listobs_run,
            "output": file_record(casa_output, pack_root),
            "listobs": file_record(oracle_dir / "casa-listobs.txt", pack_root),
        },
        "cli": {
            "split_run": cli_run,
            "output": file_record(pack_root / SURFACE_OUTPUTS["cli"], pack_root),
        },
        "python": {
            "split_run": python_run,
            "output": file_record(pack_root / SURFACE_OUTPUTS["python"], pack_root),
        },
        "tui": {
            "split_run": tui_capture_run,
            "screenshot": str((headless_dir / f"{SECTION_ID}-tui.png").relative_to(pack_root)),
            "output": file_record(pack_root / SURFACE_OUTPUTS["tui"], pack_root),
        },
        "gui": {
            "split_run": gui_run,
            "summary_run": gui_summary_run,
            "run_screenshot": str((gui_dir / f"{SECTION_ID}-gui-run.png").relative_to(pack_root)),
            "summary_screenshot": str((gui_dir / f"{SECTION_ID}-gui-summary.png").relative_to(pack_root)),
            "output": file_record(pack_root / SURFACE_OUTPUTS["gui"], pack_root),
        },
        "summaries": summary_runs,
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
                "comments": "Ready for review after split/listobs numeric comparison and GUI/TUI screenshots are inspected.",
                "required_changes": [],
            },
            "evidence": {
                "html": f"docs/sections/{SECTION_ID}.html",
                "json": f".casa-rs/evidence/{SECTION_ID}.json",
            },
        },
    )
    write_docs(pack_root, evidence)
    print(json.dumps({"section_id": SECTION_ID, "status": comparison["status"]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
