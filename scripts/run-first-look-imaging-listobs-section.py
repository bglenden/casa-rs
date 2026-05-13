#!/usr/bin/env python3
"""Run the ALMA First Look Imaging listobs tutorial-pack section."""

from __future__ import annotations

import argparse
import json
import os
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
SECTION_ID = "01-listobs-calibrated-ms"
MS_PATH = "twhya_calibrated.ms"


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_command(
    args: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    capture_stdout: Path | None = None,
) -> dict[str, Any]:
    start = time.perf_counter()
    result = subprocess.run(
        args,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    elapsed = time.perf_counter() - start
    if capture_stdout is not None:
        capture_stdout.parent.mkdir(parents=True, exist_ok=True)
        capture_stdout.write_text(result.stdout, encoding="utf-8")
    return {
        "args": args,
        "exit_code": result.returncode,
        "elapsed_seconds": elapsed,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def require_success(label: str, run: dict[str, Any]) -> None:
    if run["exit_code"] != 0:
        raise SystemExit(
            f"{label} failed with exit {run['exit_code']}\nSTDOUT:\n{run['stdout']}\nSTDERR:\n{run['stderr']}"
        )


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def compact_summary(payload: dict[str, Any]) -> dict[str, Any]:
    ms = payload["measurement_set"]
    return {
        "row_count": ms["row_count"],
        "field_count": ms["field_count"],
        "spectral_window_count": ms["spectral_window_count"],
        "antenna_count": ms["antenna_count"],
        "start_mjd_seconds": ms["start_mjd_seconds"],
        "end_mjd_seconds": ms["end_mjd_seconds"],
        "field_names": [field["name"] for field in payload.get("fields", [])],
        "scan_numbers": sorted({scan["scan_number"] for scan in payload.get("scans", [])}),
        "spw_channel_counts": [spw["num_channels"] for spw in payload.get("spectral_windows", [])],
    }


def first_lines(path: Path, count: int) -> str:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[:count])


def html_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def write_section_docs(pack_root: Path, comparison: dict[str, Any]) -> None:
    docs_dir = pack_root / "docs" / "sections"
    docs_dir.mkdir(parents=True, exist_ok=True)
    casa_excerpt = first_lines(
        pack_root / ".casa-rs/workspace/oracle/01-listobs-calibrated-ms/casa-listobs.txt",
        40,
    )
    native_excerpt = first_lines(
        pack_root / ".casa-rs/workspace/native/01-listobs-calibrated-ms/cli-msexplore-listobs.txt",
        40,
    )
    summary = comparison["native_summary"]
    markdown = f"""# 01 - Inspect calibrated MeasurementSet summary

CASA tutorial chunk: `listobs(vis="twhya_calibrated.ms")`

casa-rs equivalent task: `msexplore` summary mode.

## Parameters

| Surface | Task | Parameters |
| --- | --- | --- |
| CASA | `listobs` | `vis="twhya_calibrated.ms"`, `listfile=".casa-rs/workspace/oracle/01-listobs-calibrated-ms/casa-listobs.txt"`, `overwrite=True` |
| CLI | `msexplore` | `--format json --output .casa-rs/workspace/native/01-listobs-calibrated-ms/cli-msexplore-listobs.json --overwrite twhya_calibrated.ms` |
| Python | `casars.tasks.msexplore.summary` | `measurement_set="twhya_calibrated.ms"`, `format="json"`, `output_path=".casa-rs/workspace/native/01-listobs-calibrated-ms/python-msexplore-listobs.json"`, `overwrite=True` |
| TUI | `Measurement Sets > MSExplore` | `MeasurementSet Path=twhya_calibrated.ms`, `Output Format=json`, `Output Path=.casa-rs/workspace/native/01-listobs-calibrated-ms/tui-msexplore-listobs.json`, `Overwrite Output=true` |
| GUI | `Tasks > MSExplore` | `MeasurementSet Path=twhya_calibrated.ms`, `Output Format=json`, `Output Path=.casa-rs/workspace/native/01-listobs-calibrated-ms/gui-msexplore-listobs.json`, `Overwrite Output=true` |

## Observable Result

- rows: `{summary["row_count"]}`
- fields: `{summary["field_count"]}` (`{", ".join(summary["field_names"])}`)
- spectral windows: `{summary["spectral_window_count"]}`
- antennas: `{summary["antenna_count"]}`
- scan numbers: `{", ".join(str(scan) for scan in summary["scan_numbers"])}`

## CASA `listobs` Excerpt

```text
{casa_excerpt}
```

## casa-rs `msexplore --format text` Excerpt

```text
{native_excerpt}
```

## Review State

CLI and Python evidence has been generated. GUI and TUI screenshot capture is still required before human sign-off.
"""
    (docs_dir / "01-listobs-calibrated-ms.md").write_text(markdown, encoding="utf-8")
    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ALMA First Look Imaging - 01 listobs</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; line-height: 1.45; margin: 2rem; max-width: 1180px; }}
table {{ border-collapse: collapse; width: 100%; }}
td, th {{ border: 1px solid #c7c7c7; padding: 0.45rem; vertical-align: top; }}
pre {{ background: #f6f6f6; border: 1px solid #ddd; padding: 1rem; overflow: auto; }}
.grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }}
.pending {{ color: #8a5a00; font-weight: 600; }}
</style>
</head>
<body>
<h1>ALMA First Look Imaging: 01 listobs</h1>
<p>CASA source: <code>listobs(vis="twhya_calibrated.ms")</code>. casa-rs equivalent: <code>msexplore</code> summary mode.</p>
<h2>Parameters</h2>
<table>
<tr><th>Surface</th><th>Task</th><th>Parameters</th></tr>
<tr><td>CASA</td><td><code>listobs</code></td><td><code>vis=twhya_calibrated.ms</code>, <code>listfile=.casa-rs/workspace/oracle/01-listobs-calibrated-ms/casa-listobs.txt</code>, <code>overwrite=True</code></td></tr>
<tr><td>CLI</td><td><code>msexplore</code></td><td><code>--format json --output .casa-rs/workspace/native/01-listobs-calibrated-ms/cli-msexplore-listobs.json --overwrite twhya_calibrated.ms</code></td></tr>
<tr><td>Python</td><td><code>casars.tasks.msexplore.summary</code></td><td><code>measurement_set=twhya_calibrated.ms</code>, <code>format=json</code>, <code>output_path=.casa-rs/workspace/native/01-listobs-calibrated-ms/python-msexplore-listobs.json</code>, <code>overwrite=True</code></td></tr>
<tr><td>TUI</td><td><code>Measurement Sets &gt; MSExplore</code></td><td><code>MeasurementSet Path=twhya_calibrated.ms</code>, <code>Output Format=json</code>, <code>Output Path=.casa-rs/workspace/native/01-listobs-calibrated-ms/tui-msexplore-listobs.json</code>, <code>Overwrite Output=true</code></td></tr>
<tr><td>GUI</td><td><code>Tasks &gt; MSExplore</code></td><td><code>MeasurementSet Path=twhya_calibrated.ms</code>, <code>Output Format=json</code>, <code>Output Path=.casa-rs/workspace/native/01-listobs-calibrated-ms/gui-msexplore-listobs.json</code>, <code>Overwrite Output=true</code></td></tr>
</table>
<h2>Observable Result</h2>
<ul>
<li>rows: <code>{summary["row_count"]}</code></li>
<li>fields: <code>{summary["field_count"]}</code> ({html_escape(", ".join(summary["field_names"]))})</li>
<li>spectral windows: <code>{summary["spectral_window_count"]}</code></li>
<li>antennas: <code>{summary["antenna_count"]}</code></li>
<li>scan numbers: <code>{html_escape(", ".join(str(scan) for scan in summary["scan_numbers"]))}</code></li>
</ul>
<p class="pending">GUI and TUI screenshot capture is pending; this page currently contains executable CLI/Python/oracle evidence and the required shared UI parameters.</p>
<div class="grid">
<section><h2>CASA listobs excerpt</h2><pre>{html_escape(casa_excerpt)}</pre></section>
<section><h2>casa-rs msexplore excerpt</h2><pre>{html_escape(native_excerpt)}</pre></section>
</div>
</body>
</html>
"""
    (docs_dir / "01-listobs-calibrated-ms.html").write_text(html, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pack-root", type=Path, default=DEFAULT_PACK_ROOT)
    parser.add_argument(
        "--msexplore-binary",
        type=Path,
        default=REPO_ROOT / "target" / "release" / "msexplore",
    )
    parser.add_argument("--casa-python", type=Path, default=DEFAULT_CASA_PYTHON)
    args = parser.parse_args()

    pack_root = args.pack_root.expanduser().resolve()
    native_dir = pack_root / ".casa-rs" / "workspace" / "native" / SECTION_ID
    oracle_dir = pack_root / ".casa-rs" / "workspace" / "oracle" / SECTION_ID
    evidence_dir = pack_root / ".casa-rs" / "evidence"
    native_dir.mkdir(parents=True, exist_ok=True)
    oracle_dir.mkdir(parents=True, exist_ok=True)
    (evidence_dir / "review").mkdir(parents=True, exist_ok=True)

    cli_json = native_dir / "cli-msexplore-listobs.json"
    cli_text = native_dir / "cli-msexplore-listobs.txt"
    python_json = native_dir / "python-msexplore-listobs.json"
    casa_text = oracle_dir / "casa-listobs.txt"

    timings: list[dict[str, Any]] = []
    cli_run = run_command(
        [
            str(args.msexplore_binary),
            "--format",
            "json",
            "--output",
            str(cli_json.relative_to(pack_root)),
            "--overwrite",
            MS_PATH,
        ],
        cwd=pack_root,
    )
    require_success("CLI msexplore JSON", cli_run)
    timings.append({"surface": "cli", "task": "msexplore", "elapsed_seconds": cli_run["elapsed_seconds"]})

    text_run = run_command(
        [
            str(args.msexplore_binary),
            "--format",
            "text",
            "--output",
            str(cli_text.relative_to(pack_root)),
            "--overwrite",
            MS_PATH,
        ],
        cwd=pack_root,
    )
    require_success("CLI msexplore text", text_run)

    env = os.environ.copy()
    python_path = str(REPO_ROOT / "crates" / "casars-python" / "python")
    env["PYTHONPATH"] = python_path + os.pathsep + env.get("PYTHONPATH", "")
    python_run = run_command(
        [
            str(args.casa_python),
            "-c",
            (
                "from casars.tasks import msexplore; "
                "msexplore.summary('twhya_calibrated.ms', format='json', "
                "output_path='.casa-rs/workspace/native/01-listobs-calibrated-ms/python-msexplore-listobs.json', "
                "overwrite=True, binary='"
                + str(args.msexplore_binary)
                + "')"
            ),
        ],
        cwd=pack_root,
        env=env,
    )
    require_success("Python msexplore summary", python_run)
    timings.append({"surface": "python", "task": "msexplore", "elapsed_seconds": python_run["elapsed_seconds"]})

    casa_run = run_command(
        [
            str(args.casa_python),
            "-c",
            (
                "from casatasks import listobs; "
                "listobs(vis='twhya_calibrated.ms', "
                "listfile='.casa-rs/workspace/oracle/01-listobs-calibrated-ms/casa-listobs.txt', "
                "overwrite=True)"
            ),
        ],
        cwd=pack_root,
    )
    require_success("CASA listobs", casa_run)
    timings.append({"surface": "oracle", "task": "listobs", "elapsed_seconds": casa_run["elapsed_seconds"]})

    cli_payload = load_json(cli_json)
    python_payload = load_json(python_json)
    comparison = {
        "schema_version": "tutorial-pack-comparison.v0",
        "pack_id": "alma-first-look-imaging",
        "section_id": SECTION_ID,
        "native_summary": compact_summary(cli_payload),
        "python_summary": compact_summary(python_payload),
        "checks": {
            "cli_python_json_equal": cli_payload == python_payload,
            "casa_text_contains_tw_hya": "TW Hya" in casa_text.read_text(encoding="utf-8", errors="replace"),
            "native_text_contains_tw_hya": "TW Hya" in cli_text.read_text(encoding="utf-8", errors="replace"),
        },
    }
    write_json(evidence_dir / "comparisons.json", comparison)
    write_json(evidence_dir / "timings.json", {"section_id": SECTION_ID, "runs": timings})
    write_json(
        evidence_dir / "provider-provenance.json",
        {
            "schema_version": "tutorial-pack-provider-provenance.v0",
            "pack_id": "alma-first-look-imaging",
            "section_id": SECTION_ID,
            "native_provider": "native-rust",
            "native_binary": str(args.msexplore_binary),
            "python_wrapper": "casars.tasks.msexplore.summary",
            "oracle_provider": "casa-oracle",
            "oracle_python": str(args.casa_python),
        },
    )
    write_json(
        pack_root / ".casa-rs" / "screenshots" / "specs" / "01-listobs-calibrated-ms.json",
        {
            "schema_version": "tutorial-pack-screenshot-spec.v0",
            "section_id": SECTION_ID,
            "pending": [
                {
                    "surface": "gui",
                    "view": "Tasks > MSExplore",
                    "parameters_to_annotate": ["MeasurementSet Path", "Output Format", "Output Path", "Overwrite Output"],
                },
                {
                    "surface": "tui",
                    "view": "Measurement Sets > MSExplore",
                    "parameters_to_annotate": ["MeasurementSet Path", "Output Format", "Output Path", "Overwrite Output"],
                },
            ],
        },
    )
    review = {
        "schema_version": "tutorial-pack-review.v0",
        "pack_id": "alma-first-look-imaging",
        "tutorial_id": "alma/first-look/twhya/imaging",
        "section_id": SECTION_ID,
        "status": "pending-human-review",
        "casa_source": {
            "guide_url": "https://casaguides.nrao.edu/index.php/First_Look_at_Imaging",
            "section_anchor": "A_First_Look_at_the_Data",
            "task_calls": [{"task_id": "listobs", "parameters": {"vis": MS_PATH}}],
            "expected_observable_result": "Listobs-style MeasurementSet metadata for twhya_calibrated.ms.",
        },
        "casars_equivalents": {
            "cli": {
                "provider_kind": "native-rust",
                "task_id": "msexplore",
                "command_template": "msexplore --format json --output .casa-rs/workspace/native/01-listobs-calibrated-ms/cli-msexplore-listobs.json --overwrite twhya_calibrated.ms",
                "parameters": {
                    "ms_path": MS_PATH,
                    "format": "json",
                    "output": ".casa-rs/workspace/native/01-listobs-calibrated-ms/cli-msexplore-listobs.json",
                    "overwrite": True,
                },
            },
            "python": {
                "provider_kind": "native-rust",
                "task_id": "msexplore",
                "parameters": {
                    "measurement_set": MS_PATH,
                    "format": "json",
                    "output_path": ".casa-rs/workspace/native/01-listobs-calibrated-ms/python-msexplore-listobs.json",
                    "overwrite": True,
                },
            },
            "tui": {
                "provider_kind": "native-rust",
                "task_id": "msexplore",
                "ui_path": "Measurement Sets > MSExplore",
                "parameters": {
                    "ms_path": MS_PATH,
                    "format": "json",
                    "output": ".casa-rs/workspace/native/01-listobs-calibrated-ms/tui-msexplore-listobs.json",
                    "overwrite": True,
                },
                "screenshot_refs": [],
            },
            "gui": {
                "provider_kind": "native-rust",
                "task_id": "msexplore",
                "ui_path": "Tasks > MSExplore",
                "parameters": {
                    "ms_path": MS_PATH,
                    "format": "json",
                    "output": ".casa-rs/workspace/native/01-listobs-calibrated-ms/gui-msexplore-listobs.json",
                    "overwrite": True,
                },
                "screenshot_refs": [],
            },
        },
        "observable_products": {
            "casa_refs": [".casa-rs/workspace/oracle/01-listobs-calibrated-ms/casa-listobs.txt"],
            "casars_refs": [
                ".casa-rs/workspace/native/01-listobs-calibrated-ms/cli-msexplore-listobs.json",
                ".casa-rs/workspace/native/01-listobs-calibrated-ms/python-msexplore-listobs.json",
                ".casa-rs/workspace/native/01-listobs-calibrated-ms/cli-msexplore-listobs.txt",
            ],
            "comparison_refs": [".casa-rs/evidence/comparisons.json"],
            "timing_refs": [".casa-rs/evidence/timings.json"],
        },
        "regression_evidence": {
            "input_manifest_refs": [".casa-rs/evidence/data-manifest.json"],
            "native_run_refs": [".casa-rs/workspace/native/01-listobs-calibrated-ms/cli-msexplore-listobs.json"],
            "oracle_run_refs": [".casa-rs/workspace/oracle/01-listobs-calibrated-ms/casa-listobs.txt"],
            "provider_provenance_refs": [".casa-rs/evidence/provider-provenance.json"],
            "screenshot_spec_refs": [".casa-rs/screenshots/specs/01-listobs-calibrated-ms.json"],
        },
        "human_evaluation": {
            "outcome": "pending",
            "reviewed_by": None,
            "reviewed_at": None,
            "comments": "CLI/Python/native and CASA oracle evidence generated. GUI and TUI screenshot capture remains pending.",
            "required_changes": [],
            "follow_up_issue_refs": [],
        },
    }
    write_json(evidence_dir / "review" / "01-listobs-calibrated-ms.json", review)
    write_section_docs(pack_root, comparison)
    print(json.dumps({"pack_root": str(pack_root), "comparison": comparison}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
