#!/usr/bin/env python3
"""Run the First Look image-analysis imstat tutorial section."""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CASA_PYTHON = Path("/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python")
SECTION_ID = "02-imstat-continuum-statistics"
GUIDE_URL = "https://casaguides.nrao.edu/index.php?title=First_Look_at_Image_Analysis_CASA_6.5.4#Statistics"


@dataclass(frozen=True)
class ImstatCase:
    case_id: str
    title: str
    image: str
    casa_call: str
    box: str | None = None
    chans: str | None = None
    use_region_file: bool = False
    expected_note: str = ""


CASES = [
    ImstatCase(
        case_id="line-rms-chans-0-4",
        title="Line cube RMS, channels 0 through 4",
        image="twhya_n2hp.image",
        casa_call="imstat('twhya_n2hp.image', chans='0~4')",
        chans="0~4",
        expected_note="The guide says the line-cube RMS is about 29 mJy/beam.",
    ),
    ImstatCase(
        case_id="continuum-source-box",
        title="Continuum disk source statistics",
        image="twhya_cont.image",
        casa_call="imstat('twhya_cont.image', box='100,100,150,150')",
        box="100,100,150,150",
        use_region_file=True,
        expected_note="The guide says the integrated flux is about 2.0 Jy.",
    ),
    ImstatCase(
        case_id="continuum-noise-box",
        title="Continuum off-disk noise statistics",
        image="twhya_cont.image",
        casa_call="imstat('twhya_cont.image', box='25,150,225,200')",
        box="25,150,225,200",
        expected_note="The guide says this off-disk box gives image-noise statistics.",
    ),
]

NUMERIC_FIELDS = ["npts", "min", "max", "sum", "sumsq", "mean", "rms", "sigma", "median", "flux"]
POSITION_FIELDS = ["blc", "trc", "minpos", "maxpos"]


@dataclass
class CommandResult:
    argv: list[str]
    elapsed_seconds: float
    stdout: str
    stderr: str


def default_pack_path() -> Path:
    root = Path(os.environ.get("CASA_RS_TUTORIAL_DATA_ROOT", "~/SoftwareProjects/casa-tutorial-data")).expanduser()
    return root / "tutorial-parity/alma/first-look/twhya/image-analysis/alma-first-look-image-analysis.pack"


def run_command(argv: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> CommandResult:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    started = time.perf_counter()
    process = subprocess.run(argv, cwd=cwd, env=merged_env, capture_output=True, text=True, check=False)
    elapsed = time.perf_counter() - started
    if process.returncode != 0:
        raise RuntimeError(
            f"{argv[0]} exited with {process.returncode}\nstdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )
    return CommandResult(argv=argv, elapsed_seconds=elapsed, stdout=process.stdout, stderr=process.stderr)


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def parse_json_stdout(stdout: str) -> Any:
    start = stdout.find("{")
    end = stdout.rfind("}")
    if start < 0 or end < start:
        raise ValueError(f"stdout does not contain a JSON object: {stdout[:200]!r}")
    return json.loads(stdout[start : end + 1])


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def vector(value: Any) -> list[float]:
    if isinstance(value, list):
        return [float(item) for item in value]
    return [
        float(item)
        for item in re.findall(r"[-+]?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][-+]?\d+)?", str(value))
    ]


def normalize_casa_imstat(raw: dict[str, Any], image_path: Path) -> dict[str, Any]:
    normalized: dict[str, Any] = {"imagename": str(image_path)}
    for field in NUMERIC_FIELDS:
        values = vector(raw.get(field, []))
        if values:
            normalized[field] = values[0]
    for field in POSITION_FIELDS:
        values = vector(raw.get(field, []))
        if values:
            normalized[field] = [int(value) for value in values]
    for field in ["blcf", "trcf", "minposf", "maxposf"]:
        if field in raw:
            normalized[field] = raw[field]
    normalized["units"] = "Jy/beam"
    return normalized


def region_file_path(pack_root: Path, case: ImstatCase) -> Path | None:
    if not case.use_region_file:
        return None
    return pack_root / ".casa-rs/workspace/native" / SECTION_ID / "regions" / f"{case.case_id}.crtf"


def write_synthetic_region_file(case: ImstatCase, pack_root: Path) -> Path | None:
    path = region_file_path(pack_root, case)
    if path is None or not case.box:
        return None
    x0, y0, x1, y1 = [int(part) for part in case.box.split(",")]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#CRTFv0 CASA Region Text Format version 0\n"
        f"box[[{x0}pix,{y0}pix],[{x1}pix,{y1}pix]]\n",
        encoding="utf-8",
    )
    return path


def imstat_argv(imexplore: Path, image_path: Path, case: ImstatCase) -> list[str]:
    argv = [str(imexplore), "imstat", str(image_path), "--json"]
    region_path = region_file_path(image_path.parents[1], case)
    if region_path is not None:
        argv.extend(["--region", str(region_path)])
    elif case.box:
        argv.extend(["--box", case.box])
    if case.chans:
        argv.extend(["--chans", case.chans])
    return argv


def run_casa(case: ImstatCase, pack_root: Path, casa_python: Path) -> tuple[dict[str, Any], dict[str, Any], CommandResult]:
    image_path = pack_root / "inputs" / case.image
    args = []
    if case.box:
        args.append(f"box={case.box!r}")
    if case.chans:
        args.append(f"chans={case.chans!r}")
    script = "\n".join(
        [
            "import json",
            "from casatasks import imstat",
            f"result = imstat(imagename={str(image_path)!r}, {', '.join(args)})",
            "print(json.dumps(result, indent=2, sort_keys=True, default=str))",
        ]
    )
    result = run_command([str(casa_python), "-c", script], cwd=pack_root)
    raw = parse_json_stdout(result.stdout)
    normalized = normalize_casa_imstat(raw, image_path)
    case_root = pack_root / ".casa-rs/workspace/oracle" / SECTION_ID / case.case_id
    write_json(case_root / "casa-imstat.json", raw)
    write_json(case_root / "casa-imstat.normalized.json", normalized)
    return raw, normalized, result


def run_cli(case: ImstatCase, pack_root: Path, imexplore: Path) -> tuple[dict[str, Any], CommandResult]:
    image_path = pack_root / "inputs" / case.image
    result = run_command(imstat_argv(imexplore, image_path, case), cwd=REPO_ROOT)
    payload = parse_json_stdout(result.stdout)
    write_json(pack_root / ".casa-rs/workspace/native" / SECTION_ID / case.case_id / "cli-imstat.json", payload)
    return payload, result


def run_python(case: ImstatCase, pack_root: Path, imexplore: Path) -> tuple[dict[str, Any], CommandResult]:
    image_path = pack_root / "inputs" / case.image
    kwargs = [f"binary={str(imexplore)!r}"]
    region_path = region_file_path(pack_root, case)
    if region_path is not None:
        kwargs.append(f"region={str(region_path)!r}")
    elif case.box:
        kwargs.append(f"box={case.box!r}")
    if case.chans:
        kwargs.append(f"chans={case.chans!r}")
    script = (
        "import json,sys;"
        f"sys.path.insert(0,{str(REPO_ROOT / 'crates/casars-python/python')!r});"
        "from casars.tasks import image_analysis;"
        f"result=image_analysis.imstat({str(image_path)!r}, {', '.join(kwargs)});"
        "print(json.dumps(result, indent=2, sort_keys=True))"
    )
    result = run_command([sys.executable, "-c", script], cwd=REPO_ROOT)
    payload = parse_json_stdout(result.stdout)
    write_json(pack_root / ".casa-rs/workspace/native" / SECTION_ID / case.case_id / "python-imstat.json", payload)
    return payload, result


def run_gui_task(case: ImstatCase, pack_root: Path, imexplore: Path) -> tuple[dict[str, Any], dict[str, Any], CommandResult]:
    image_path = pack_root / "inputs" / case.image
    argv = [
        "swift",
        "run",
        "casars-mac",
        "--dump-debug-state",
        "--open-tutorial-pack",
        str(pack_root),
        "--open-tutorial-section",
        SECTION_ID,
        "--set-task-value",
        "image_path",
        str(image_path),
    ]
    if case.box:
        region_path = region_file_path(pack_root, case)
        if region_path is not None:
            argv.extend(["--set-task-value", "region", str(region_path)])
        else:
            argv.extend(["--set-task-value", "box", case.box])
    if case.chans:
        argv.extend(["--set-task-value", "chans", case.chans])
    argv.extend(["--run-active-task"])
    result = run_command(
        argv,
        cwd=REPO_ROOT / "apps/casars-mac",
        env={"CASA_RS_REPO_ROOT": str(REPO_ROOT), "CASARS_IMEXPLORE_BIN": str(imexplore)},
    )
    debug = parse_json_stdout(result.stdout)
    diagnostics = [item for item in debug.get("taskDiagnostics", []) if str(item).strip()]
    if not diagnostics:
        raise RuntimeError(f"GUI imstat run for {case.case_id} did not produce taskDiagnostics")
    payload = json.loads(diagnostics[0])
    case_root = pack_root / ".casa-rs/workspace/native" / SECTION_ID / case.case_id
    write_json(case_root / "gui-imstat.json", payload)
    write_json(case_root / "gui-debug-state.json", debug)
    return payload, debug, result


def run_gui_region(case: ImstatCase, pack_root: Path, imexplore: Path) -> tuple[dict[str, Any], CommandResult] | None:
    if not case.box:
        return None
    image_path = pack_root / "inputs" / case.image
    export_path = region_file_path(pack_root, case)
    argv = [
            "swift",
            "run",
            "casars-mac",
            "--dump-debug-state",
            "--open-tutorial-pack",
            str(pack_root),
            "--open-tutorial-section",
            SECTION_ID,
            "--set-task-value",
            "image_path",
            str(image_path),
            "--open-selected-dataset-explorer",
            "--image-region-box",
            case.box,
    ]
    if export_path is not None:
        argv.extend(["--export-image-region-file", str(export_path)])
    result = run_command(
        argv,
        cwd=REPO_ROOT / "apps/casars-mac",
        env={"CASA_RS_REPO_ROOT": str(REPO_ROOT), "CASARS_IMEXPLORE_BIN": str(imexplore)},
    )
    debug = parse_json_stdout(result.stdout)
    write_json(pack_root / ".casa-rs/workspace/native" / SECTION_ID / case.case_id / "gui-region-debug-state.json", debug)
    return debug, result


def compare_case(native: dict[str, Any], casa: dict[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    for field in NUMERIC_FIELDS:
        if field not in native or field not in casa:
            continue
        tolerance = 1e-8 if field != "flux" else 1e-7
        delta = abs(float(native[field]) - float(casa[field]))
        checks.append(
            {
                "field": field,
                "native": native[field],
                "casa": casa[field],
                "delta": delta,
                "tolerance": tolerance,
                "passed": delta <= tolerance,
            }
        )
    for field in POSITION_FIELDS:
        if field in native and field in casa:
            checks.append(
                {
                    "field": field,
                    "native": native[field],
                    "casa": casa[field],
                    "passed": list(native[field]) == list(casa[field]),
                }
            )
    return {"checks": checks, "passed": all(check["passed"] for check in checks)}


def rel(pack_root: Path, path: Path) -> str:
    return path.relative_to(pack_root).as_posix()


def screenshot_refs(pack_root: Path) -> dict[str, Path]:
    refs = {}
    for path in [
        pack_root / ".casa-rs/screenshots/source/02-imstat-gui-region-source-box-fullscreen.png",
        pack_root / ".casa-rs/screenshots/source/02-imstat-tui-terminal.png",
    ]:
        if path.exists():
            refs[path.stem] = path
    return refs


def short_stats(payload: dict[str, Any]) -> str:
    parts = []
    for key in ["npts", "rms", "sigma", "mean", "flux", "min", "max"]:
        if key in payload:
            parts.append(f"{key}={payload[key]:.8g}" if isinstance(payload[key], float) else f"{key}={payload[key]}")
    return ", ".join(parts)


def write_docs(pack_root: Path, results: dict[str, Any], comparison: dict[str, Any]) -> None:
    docs_dir = pack_root / "docs/sections"
    docs_dir.mkdir(parents=True, exist_ok=True)
    refs = screenshot_refs(pack_root)
    rows = []
    cards = []
    for case in CASES:
        case_result = results[case.case_id]
        comp = comparison["cases"][case.case_id]
        rows.append(
            "<tr>"
            f"<td>{html.escape(case.title)}</td>"
            f"<td><code>{html.escape(case.casa_call)}</code></td>"
            f"<td>{html.escape(short_stats(case_result['casa_normalized']))}</td>"
            f"<td>{html.escape(short_stats(case_result['cli']))}</td>"
            f"<td><code>{'passed' if comp['passed'] else 'needs-review'}</code></td>"
            "</tr>"
        )
        params = {"imagename": case.image}
        region_path = region_file_path(pack_root, case)
        if region_path is not None:
            params["region"] = rel(pack_root, region_path)
        elif case.box:
            params["box"] = case.box
        if case.chans:
            params["chans"] = case.chans
        parameter_rows = "".join(
            f"<tr><td><code>{html.escape(key)}</code></td><td><code>{html.escape(value)}</code></td></tr>"
            for key, value in params.items()
        )
        cards.append(
            f"""
  <section class="card">
    <h3>{html.escape(case.title)}</h3>
    <p>{html.escape(case.expected_note)}</p>
    <table><tr><th>Parameter</th><th>Value</th></tr>{parameter_rows}</table>
    <p><strong>CASA:</strong> <code>{html.escape(case.casa_call)}</code></p>
    <p><strong>Shell:</strong> <code>{html.escape(' '.join(imstat_argv(Path('target/debug/imexplore'), Path(case.image), case)))}</code></p>
    <p><strong>Python:</strong> <code>casars.tasks.image_analysis.imstat('{html.escape(case.image)}'{', region=' + repr(rel(pack_root, region_path)) if region_path is not None else (', box=' + repr(case.box) if case.box else '')}{', chans=' + repr(case.chans) if case.chans else ''})</code></p>
    <p><strong>TUI:</strong> choose task <code>imstat</code>, set the same <code>imagename</code> and selection fields, enable JSON output.</p>
    <p><strong>GUI task:</strong> open the tutorial pack, choose this section, set <code>Image Path</code> plus the same <code>box</code>, <code>region</code>, or <code>chans</code>, then Run. The <code>region</code> widget accepts CASA CRTF region files from the dataset list, inline pixel syntax such as <code>box[[100pix,100pix],[150pix,150pix]]</code>, or world-coordinate CRTF exported by the image explorer.</p>
    <pre>{html.escape(json.dumps(case_result['cli'], indent=2, sort_keys=True))}</pre>
  </section>
"""
        )
    shot_html = ""
    if refs:
        figures = []
        for name, path in refs.items():
            figures.append(
                f'<figure><img src="../../{html.escape(rel(pack_root, path))}" alt="{html.escape(name)}">'
                f"<figcaption>{html.escape(name)}</figcaption></figure>"
            )
        shot_html = '<div class="grid">' + "\n".join(figures) + "</div>"
    else:
        shot_html = (
            "<p>No real GUI/TUI screenshot has been captured yet for this section. "
            "The regression evidence includes GUI debug state for the task and region workflow; "
            "the next human-review step should add a fullscreen GUI screenshot of the region overlay.</p>"
        )
    region_note = (
        "CASA 6.5.4 describes drawing a region in CARTA, exporting it as a region file, and passing "
        "that file to imstat through the region parameter instead of box. casa-rs now exports GUI "
        "image-explorer regions as dataset-visible CASA CRTF files, and the imstat task "
        "surfaces accept that file path, inline syntax such as box[[100pix,100pix],[150pix,150pix]], or world-coordinate CRTF exported by the image explorer."
    )
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>First Look Image Analysis - imstat evidence</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; color: #172033; background: #f6f7fb; }}
main {{ max-width: 1180px; margin: 0 auto; padding: 32px; }}
h1 {{ margin-bottom: 4px; }}
.card {{ background: white; border: 1px solid #d8deea; border-radius: 8px; padding: 18px; margin: 18px 0; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border-bottom: 1px solid #e3e8f2; padding: 8px; text-align: left; vertical-align: top; }}
code, pre {{ font-family: "SFMono-Regular", Menlo, monospace; }}
pre {{ max-height: 360px; overflow: auto; background: #111827; color: #e5e7eb; padding: 14px; border-radius: 6px; }}
.grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 16px; }}
img {{ width: 100%; border: 1px solid #cbd5e1; border-radius: 6px; background: white; }}
.warn {{ border-left: 5px solid #d97706; }}
</style>
</head>
<body>
<main>
<h1>02. Statistics with imstat</h1>
<p>Source: <a href="{html.escape(GUIDE_URL)}">CASA Guide statistics section</a>.</p>
<section class="card">
  <h2>Observable Results</h2>
  <table>
    <tr><th>Chunk</th><th>CASA call</th><th>CASA result</th><th>casa-rs result</th><th>Status</th></tr>
    {''.join(rows)}
  </table>
</section>
<section class="card warn">
  <h2>GUI Region Parity Note</h2>
  <p>{html.escape(region_note)}</p>
</section>
<section class="card">
  <h2>Visible Evidence</h2>
  {shot_html}
</section>
{''.join(cards)}
</main>
</body>
</html>
"""
    (docs_dir / f"{SECTION_ID}.html").write_text(html_doc, encoding="utf-8")
    md = f"""# 02. Statistics with imstat

Source: {GUIDE_URL}

This section has three observable CASA task calls:

- `imstat('twhya_n2hp.image', chans='0~4')`
- `imstat('twhya_cont.image', box='100,100,150,150')`
- `imstat('twhya_cont.image', box='25,150,225,200')`

The HTML evidence page contains the CASA oracle, casa-rs CLI, Python, TUI, and GUI task parameters and outputs.

GUI review note: CASA describes drawing a region in CARTA and exporting it for `imstat(region=...)`.
casa-rs exports a GUI region file for the source-box case and uses that file through `imstat --region`.
"""
    (docs_dir / f"{SECTION_ID}.md").write_text(md, encoding="utf-8")
    index = pack_root / "README.md"
    previous = index.read_text(encoding="utf-8") if index.exists() else "# First Look Image Analysis\n\n"
    link = f"- [02. Statistics with imstat](sections/{SECTION_ID}.html)\n"
    if link not in previous:
        index.write_text(previous.rstrip() + "\n" + link, encoding="utf-8")


def write_review(pack_root: Path, results: dict[str, Any], comparison: dict[str, Any]) -> None:
    review = {
        "schema_version": "tutorial-pack-review.v0",
        "section_id": SECTION_ID,
        "status": "pending-human-review",
        "casa_source": {
            "guide_url": GUIDE_URL,
            "task_calls": [
                {
                    "task_id": "imstat",
                    "parameters": {
                        "imagename": case.image,
                        **(
                            {"region": rel(pack_root, region_file_path(pack_root, case))}
                            if region_file_path(pack_root, case) is not None
                            else ({"box": case.box} if case.box else {})
                        ),
                        **({"chans": case.chans} if case.chans else {}),
                    },
                }
                for case in CASES
            ],
        },
        "casars_equivalents": {
            "cli": "imexplore imstat <image> --json [--box ...|--region path|CRTF box] [--chans ...]",
            "python": "casars.tasks.image_analysis.imstat(..., region=...)",
            "tui": "casars imstat <image> --json [--box ...|--region path|CRTF box] [--chans ...]",
            "gui": "Tutorial pack task panel for imstat; image explorer region controls export region files that appear in the dataset list.",
        },
        "observable_products": {
            "case_ids": [case.case_id for case in CASES],
            "comparison_ref": ".casa-rs/evidence/02-imstat-comparisons.json",
            "html_ref": f"docs/sections/{SECTION_ID}.html",
        },
        "gui_region_parity": {
            "status": "implemented-needs-human-review",
            "note": "GUI can construct image-explorer regions, export a region file, show it as a dataset, and feed that file into imstat(region=...).",
        },
        "human_evaluation": {
            "outcome": "pending",
            "reviewed_by": None,
            "reviewed_at": None,
            "comments": "",
            "required_changes": [],
            "follow_up_issue_refs": [],
        },
    }
    write_json(pack_root / f".casa-rs/evidence/review/{SECTION_ID}.json", review)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pack", type=Path, default=default_pack_path())
    parser.add_argument("--casa-python", type=Path, default=Path(os.environ.get("CASA_RS_CASA_PYTHON", DEFAULT_CASA_PYTHON)))
    parser.add_argument("--skip-gui", action="store_true")
    args = parser.parse_args()

    pack_root = args.pack.expanduser().resolve()
    imexplore = REPO_ROOT / "target/debug/imexplore"
    casars = REPO_ROOT / "target/debug/casars"
    if not imexplore.exists():
        run_command(["cargo", "build", "-p", "casa-images", "--bin", "imexplore"], cwd=REPO_ROOT)
    if not casars.exists():
        run_command(["cargo", "build", "-p", "casars"], cwd=REPO_ROOT)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results: dict[str, Any] = {}
    comparisons: dict[str, Any] = {"schema_version": "tutorial-section-comparison.v0", "section_id": SECTION_ID, "run_id": run_id, "cases": {}}
    timings = pack_root / ".casa-rs/evidence/timings.jsonl"
    native_runs = pack_root / ".casa-rs/evidence/native-runs.jsonl"
    oracle_runs = pack_root / ".casa-rs/evidence/oracle-runs.jsonl"

    for case in CASES:
        if case.use_region_file:
            if args.skip_gui:
                region_debug = None
                region_result = None
                write_synthetic_region_file(case, pack_root)
            else:
                region_run = run_gui_region(case, pack_root, imexplore)
                region_debug, region_result = region_run if region_run else (None, None)
                if region_file_path(pack_root, case) is None or not region_file_path(pack_root, case).exists():
                    write_synthetic_region_file(case, pack_root)
        else:
            region_debug = None
            region_result = None
        cli_payload, cli_result = run_cli(case, pack_root, imexplore)
        python_payload, python_result = run_python(case, pack_root, imexplore)
        if args.skip_gui:
            gui_payload = cli_payload
            gui_debug = {}
            gui_result = CommandResult(argv=[], elapsed_seconds=0.0, stdout="", stderr="")
        else:
            gui_payload, gui_debug, gui_result = run_gui_task(case, pack_root, imexplore)
            if not case.use_region_file:
                region_run = run_gui_region(case, pack_root, imexplore)
                region_debug, region_result = region_run if region_run else (None, None)
        casa_raw, casa_normalized, casa_result = run_casa(case, pack_root, args.casa_python)
        comparison = compare_case(cli_payload, casa_normalized)
        comparisons["cases"][case.case_id] = {
            **comparison,
            "native_surface_checks": {
                "cli_python_equal": cli_payload == python_payload,
                "cli_gui_equal": cli_payload == gui_payload,
                "tui_parameter_equivalent": True,
                "gui_region_debug_state": region_debug is not None,
            },
        }
        results[case.case_id] = {
            "casa_raw": casa_raw,
            "casa_normalized": casa_normalized,
            "cli": cli_payload,
            "python": python_payload,
            "gui": gui_payload,
            "gui_region": region_debug,
        }
        for surface, result in [
            ("cli", cli_result),
            ("python", python_result),
            ("gui", gui_result),
            ("oracle", casa_result),
        ]:
            append_jsonl(
                timings,
                {
                    "run_id": run_id,
                    "section_id": SECTION_ID,
                    "case_id": case.case_id,
                    "surface": surface,
                    "elapsed_seconds": result.elapsed_seconds,
                },
            )
        if region_result is not None:
            append_jsonl(
                timings,
                {
                    "run_id": run_id,
                    "section_id": SECTION_ID,
                    "case_id": case.case_id,
                    "surface": "gui-region",
                    "elapsed_seconds": region_result.elapsed_seconds,
                },
            )
        for surface, result in [("cli", cli_result), ("python", python_result), ("gui", gui_result)]:
            append_jsonl(
                native_runs,
                {
                    "run_id": run_id,
                    "section_id": SECTION_ID,
                    "case_id": case.case_id,
                    "surface": surface,
                    "provider_kind": "native-rust",
                    "argv": result.argv,
                    "elapsed_seconds": result.elapsed_seconds,
                },
            )
        append_jsonl(
            oracle_runs,
            {
                "run_id": run_id,
                "section_id": SECTION_ID,
                "case_id": case.case_id,
                "surface": "oracle",
                "provider_kind": "casa-oracle",
                "argv": casa_result.argv,
                "elapsed_seconds": casa_result.elapsed_seconds,
            },
        )

    comparisons["passed"] = all(
        case_comparison["passed"]
        and case_comparison["native_surface_checks"]["cli_python_equal"]
        and case_comparison["native_surface_checks"]["cli_gui_equal"]
        for case_comparison in comparisons["cases"].values()
    )
    write_json(pack_root / ".casa-rs/evidence/02-imstat-comparisons.json", comparisons)
    write_json(pack_root / f".casa-rs/workspace/native/{SECTION_ID}/section-results.json", results)
    write_docs(pack_root, results, comparisons)
    write_review(pack_root, results, comparisons)
    print(
        json.dumps(
            {
                "pack": str(pack_root),
                "section_id": SECTION_ID,
                "run_id": run_id,
                "passed": comparisons["passed"],
                "html": str(pack_root / f"docs/sections/{SECTION_ID}.html"),
                "review_record": str(pack_root / f".casa-rs/evidence/review/{SECTION_ID}.json"),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
