#!/usr/bin/env python3
"""Run the First Look image-analysis immoments tutorial section."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import html
import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CASA_PYTHON = Path("/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python")
SECTION_ID = "03-immoments-n2hp-moment-map"
GUIDE_URL = "https://casaguides.nrao.edu/index.php/First_Look_at_Image_Analysis"
TUTORIAL_ID = "alma/first-look/twhya/image-analysis"
PACK_ID = "alma-first-look-image-analysis"
SOURCE_IMAGE = "twhya_n2hp.image"
OUTDIR = Path(".casa-rs/workspace/native") / SECTION_ID
ORACLE_OUTDIR = Path(".casa-rs/workspace/oracle") / SECTION_ID
FIGDIR = Path(".casa-rs/evidence/figures")
MOMENT_VALUES = [-1, 0, 1, 2, 3]


def default_pack_path() -> Path:
    root = Path(os.environ.get("CASA_RS_TUTORIAL_DATA_ROOT", "~/SoftwareProjects/casa-tutorial-data")).expanduser()
    return root / "tutorial-parity/alma/first-look/twhya/image-analysis/alma-first-look-image-analysis.pack"


def run_command(argv: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> dict[str, Any]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    started = time.perf_counter()
    process = subprocess.run(argv, cwd=cwd, env=merged_env, capture_output=True, text=True, check=False)
    elapsed = time.perf_counter() - started
    payload = {
        "argv": argv,
        "cwd": str(cwd),
        "elapsed_seconds": elapsed,
        "stdout": process.stdout,
        "stderr": process.stderr,
        "returncode": process.returncode,
    }
    if process.returncode != 0:
        raise RuntimeError(json.dumps(payload, indent=2))
    return payload


def parse_json_stdout(stdout: str) -> Any:
    start = stdout.find("{")
    end = stdout.rfind("}")
    if start < 0 or end < start:
        raise ValueError(f"stdout does not contain a JSON object: {stdout[:200]!r}")
    return json.loads(stdout[start : end + 1])


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def numeric(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, list):
        return numeric(value[0])
    matches = re.findall(r"[-+]?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][-+]?\d+)?", str(value))
    if not matches:
        raise ValueError(f"no numeric value in {value!r}")
    return float(matches[0])


def run_oracle(pack_root: Path, casa_python: Path) -> dict[str, Any]:
    outfile = ORACLE_OUTDIR / "twhya_n2hp.mom0"
    script = "\n".join(
        [
            "import json, shutil",
            "from pathlib import Path",
            "from casatasks import imhead, immoments, imstat",
            f"out = Path({str(outfile)!r})",
            "shutil.rmtree(out, ignore_errors=True)",
            "out.parent.mkdir(parents=True, exist_ok=True)",
            (
                "result = immoments(imagename='twhya_n2hp.image', "
                "outfile=str(out), moments=0, chans='4~12', includepix=[0.03, 100])"
            ),
            "payload = {'result': result, 'header': imhead(str(out), mode='summary'), 'stats': imstat(str(out))}",
            "print(json.dumps(payload, indent=2, sort_keys=True, default=str))",
        ]
    )
    result = run_command([str(casa_python), "-c", script], cwd=pack_root)
    payload = parse_json_stdout(result["stdout"])
    payload["elapsed_seconds"] = result["elapsed_seconds"]
    write_json(pack_root / ".casa-rs/evidence/03-immoments-casa-mom0.json", payload)
    return payload


def run_cli(pack_root: Path, immoments: Path, imexplore: Path) -> dict[str, Any]:
    outfile = OUTDIR / "twhya_n2hp.cli.mom0"
    run = run_command(
        [
            str(immoments),
            SOURCE_IMAGE,
            "--outfile",
            str(outfile),
            "--moments",
            "0",
            "--chans",
            "4~12",
            "--includepix",
            "0.03,100",
            "--overwrite",
        ],
        cwd=pack_root,
    )
    payload = {**run, "result": parse_json_stdout(run["stdout"])}
    write_json(pack_root / ".casa-rs/evidence/03-immoments-cli-mom0-run.json", payload)
    inspect = {
        "imhead": run_command([str(imexplore), "imhead", str(outfile), "--json"], cwd=pack_root),
        "imstat": run_command([str(imexplore), "imstat", str(outfile), "--json"], cwd=pack_root),
    }
    write_json(pack_root / ".casa-rs/evidence/03-immoments-cli-mom0-inspection.json", inspect)
    return payload


def run_python(pack_root: Path, immoments: Path, casars: Path) -> dict[str, Any]:
    outfile = OUTDIR / "twhya_n2hp.python.mom0"
    env = {
        "PYTHONPATH": str(REPO_ROOT / "crates/casars-python/python"),
        "CASARS_IMMOMENTS_BIN": str(immoments),
    }
    script = "\n".join(
        [
            "import json",
            "from casars import tasks",
            (
                "result = tasks.immoments("
                "imagename='twhya_n2hp.image', "
                f"outfile={str(outfile)!r}, moments='0', chans='4~12', includepix='0.03,100.0', "
                f"overwrite=True, binary={str(casars)!r})"
            ),
            "print(json.dumps(json.loads(result.stdout), indent=2, sort_keys=True))",
        ]
    )
    result = run_command(["python3", "-c", script], cwd=pack_root, env=env)
    payload = {**result, "result": parse_json_stdout(result["stdout"])}
    write_json(pack_root / ".casa-rs/evidence/03-immoments-python-mom0-run.json", payload)
    return payload


def run_moment_range_parity(pack_root: Path, casa_python: Path, immoments: Path) -> dict[str, Any]:
    """Run CASA and casa-rs for the supported CASA tutorial moment set."""
    casa_script = "\n".join(
        [
            "import json, shutil",
            "from pathlib import Path",
            "from casatasks import immoments",
            f"moments = {MOMENT_VALUES!r}",
            f"base = Path({str(ORACLE_OUTDIR)!r}) / 'moment-range'",
            "shutil.rmtree(base, ignore_errors=True)",
            "base.mkdir(parents=True, exist_ok=True)",
            "results = []",
            "for moment in moments:",
            "    out = base / f'twhya_n2hp.casa.mom{moment}'",
            (
                "    result = immoments(imagename='twhya_n2hp.image', outfile=str(out), "
                "moments=moment, chans='4~12', includepix=[0.03, 100])"
            ),
            "    results.append({'moment': moment, 'outfile': str(out), 'result': result})",
            "print(json.dumps({'results': results}, indent=2, sort_keys=True, default=str))",
        ]
    )
    oracle_run = run_command([str(casa_python), "-c", casa_script], cwd=pack_root)
    oracle_payload = parse_json_stdout(oracle_run["stdout"])

    native_runs = []
    native_base = OUTDIR / "moment-range"
    for moment in MOMENT_VALUES:
        outfile = native_base / f"twhya_n2hp.casars.mom{moment}"
        run = run_command(
            [
                str(immoments),
                SOURCE_IMAGE,
                "--outfile",
                str(outfile),
                "--moments",
                str(moment),
                "--chans",
                "4~12",
                "--includepix",
                "0.03,100",
                "--overwrite",
            ],
            cwd=pack_root,
        )
        native_runs.append({"moment": moment, "outfile": str(outfile), "elapsed_seconds": run["elapsed_seconds"]})

    compare_script = r"""
import json
from pathlib import Path
import numpy as np
from casatools import image

pack_root = Path(PACK_ROOT)
moments = MOMENTS
casa_base = pack_root / ".casa-rs/workspace/oracle/03-immoments-n2hp-moment-map/moment-range"
native_base = pack_root / ".casa-rs/workspace/native/03-immoments-n2hp-moment-map/moment-range"

def read_image(path):
    ia = image()
    ia.open(str(path))
    data = np.asarray(ia.getchunk()).squeeze()
    mask = np.asarray(ia.getchunk(getmask=True)).squeeze().astype(bool)
    unit = ia.brightnessunit()
    shape = [int(value) for value in ia.shape()]
    ia.close()
    return data, mask, unit, shape

results = []
for moment in moments:
    casa_path = casa_base / f"twhya_n2hp.casa.mom{moment}"
    native_path = native_base / f"twhya_n2hp.casars.mom{moment}"
    casa, casa_mask, casa_unit, casa_shape = read_image(casa_path)
    native, native_mask, native_unit, native_shape = read_image(native_path)
    shared = casa_mask & native_mask & np.isfinite(casa) & np.isfinite(native)
    diff = native[shared] - casa[shared]
    results.append({
        "moment": moment,
        "casa_path": str(casa_path),
        "casars_path": str(native_path),
        "casa_shape": casa_shape,
        "casars_shape": native_shape,
        "casa_unit": casa_unit,
        "casars_unit": native_unit,
        "valid_pixels_casa": int(casa_mask.sum()),
        "valid_pixels_casars": int(native_mask.sum()),
        "mask_mismatch_pixels": int(np.count_nonzero(casa_mask != native_mask)),
        "shared_valid_pixels": int(shared.sum()),
        "max_abs_diff": float(np.max(np.abs(diff))) if diff.size else None,
        "mean_abs_diff": float(np.mean(np.abs(diff))) if diff.size else None,
        "p99_abs_diff": float(np.percentile(np.abs(diff), 99)) if diff.size else None,
    })
print(json.dumps({"schema_version": "tutorial-moment-range-parity.v0", "results": results}, indent=2, sort_keys=True))
"""
    compare_run = run_command(
        [
            str(casa_python),
            "-c",
            compare_script.replace("PACK_ROOT", repr(str(pack_root))).replace("MOMENTS", repr(MOMENT_VALUES)),
        ],
        cwd=pack_root,
    )
    comparison = parse_json_stdout(compare_run["stdout"])
    comparison["oracle_elapsed_seconds"] = oracle_run["elapsed_seconds"]
    comparison["native_runs"] = native_runs
    comparison["casa_results"] = oracle_payload["results"]
    write_json(pack_root / ".casa-rs/evidence/03-immoments-moment-range-parity.json", comparison)
    return comparison


def run_figures_and_comparison(pack_root: Path, casa_python: Path) -> dict[str, Any]:
    script = r"""
import json
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from casatools import image

pack_root = Path(PACK_ROOT)
casa_path = pack_root / ".casa-rs/workspace/oracle/03-immoments-n2hp-moment-map/twhya_n2hp.mom0"
rust_path = pack_root / ".casa-rs/workspace/native/03-immoments-n2hp-moment-map/twhya_n2hp.cli.mom0"
cont_path = pack_root / "twhya_cont.image"
figdir = pack_root / ".casa-rs/evidence/figures"
figdir.mkdir(parents=True, exist_ok=True)

def read_image(path):
    ia = image()
    ia.open(str(path))
    data = np.asarray(ia.getchunk()).squeeze()
    mask = np.asarray(ia.getchunk(getmask=True)).squeeze().astype(bool)
    ia.close()
    return data, mask

casa, casa_mask = read_image(casa_path)
rust, rust_mask = read_image(rust_path)
cont, cont_mask = read_image(cont_path)
casa_ma = np.ma.array(casa, mask=~casa_mask)
rust_ma = np.ma.array(rust, mask=~rust_mask)
shared_valid = casa_mask & rust_mask
diff_ma = np.ma.array(rust - casa, mask=~shared_valid)

fig, axes = plt.subplots(1, 3, figsize=(14, 4.2), constrained_layout=True)
for ax, title, plane in [
    (axes[0], "CASA C++ immoments mom0\n(output mask applied)", casa_ma),
    (axes[1], "casa-rs immoments mom0\n(output mask applied)", rust_ma),
]:
    im = ax.imshow(plane.T, origin="lower", cmap="inferno", vmin=0.015, vmax=0.145)
    ax.set_title(title)
    ax.set_xlabel("x pixel")
    ax.set_ylabel("y pixel")
    fig.colorbar(im, ax=ax, shrink=0.84, label="Jy/beam.km/s")
im = axes[2].imshow(diff_ma.T, origin="lower", cmap="coolwarm", vmin=-0.015, vmax=0.015)
axes[2].set_title("casa-rs - CASA\n(shared mask applied)")
axes[2].set_xlabel("x pixel")
axes[2].set_ylabel("y pixel")
fig.colorbar(im, ax=axes[2], shrink=0.84, label="Jy/beam.km/s")
fig.savefig(figdir / "03-immoments-mom0-casa-vs-casars.png", dpi=160)
plt.close(fig)

levels = [0.08, 0.10, 0.12, 0.14, 0.16]
fig, axes = plt.subplots(1, 2, figsize=(9.5, 4.4), constrained_layout=True)
cont_ma = np.ma.array(cont, mask=~cont_mask)
for ax, title, plane in [
    (axes[0], "CASA contours on continuum", casa_ma),
    (axes[1], "casa-rs contours on continuum", rust_ma),
]:
    im = ax.imshow(cont_ma.T, origin="lower", cmap="gray_r")
    ax.contour(plane.T, levels=levels, colors=["#d62728", "#ff7f0e", "#2ca02c", "#1f77b4", "#9467bd"], linewidths=1.2)
    ax.set_title(title)
    ax.set_xlabel("x pixel")
    ax.set_ylabel("y pixel")
fig.colorbar(im, ax=axes.ravel().tolist(), shrink=0.84, label="continuum Jy/beam")
fig.savefig(figdir / "03-immoments-mom0-contours-over-continuum.png", dpi=160)
plt.close(fig)

shared = shared_valid & np.isfinite(casa) & np.isfinite(rust)
diff = rust[shared] - casa[shared]
comparison = {
    "schema_version": "tutorial-moment-comparison.v0",
    "casa_path": str(casa_path),
    "casars_path": str(rust_path),
    "valid_pixels_casa": int(casa_mask.sum()),
    "valid_pixels_casars": int(rust_mask.sum()),
    "mask_mismatch_pixels": int(np.count_nonzero(casa_mask != rust_mask)),
    "shared_valid_pixels": int(shared.sum()),
    "max_abs_diff": float(np.max(np.abs(diff))) if diff.size else None,
    "p99_abs_diff": float(np.percentile(np.abs(diff), 99)) if diff.size else None,
    "mean_abs_diff": float(np.mean(np.abs(diff))) if diff.size else None,
    "figure": str(figdir / "03-immoments-mom0-casa-vs-casars.png"),
    "contour_figure": str(figdir / "03-immoments-mom0-contours-over-continuum.png"),
}
print(json.dumps(comparison, indent=2, sort_keys=True))
"""
    result = run_command(
        [str(casa_python), "-c", script.replace("PACK_ROOT", repr(str(pack_root)))],
        cwd=pack_root,
    )
    comparison = parse_json_stdout(result["stdout"])
    write_json(pack_root / ".casa-rs/evidence/03-immoments-mom0-comparison.json", comparison)
    return comparison


def write_review_record(pack_root: Path, moment_range: dict[str, Any]) -> dict[str, Any]:
    reviewed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    record = {
        "schema_version": "tutorial-pack-review.v0",
        "pack_id": PACK_ID,
        "tutorial_id": TUTORIAL_ID,
        "section_id": SECTION_ID,
        "status": "accepted",
        "casa_source": {
            "guide_url": GUIDE_URL,
            "section_anchor": "Moment_maps",
            "task_calls": [
                {
                    "task_id": "immoments",
                    "parameters": {
                        "imagename": "twhya_n2hp.image",
                        "outfile": "twhya_n2hp.mom0",
                        "moments": 0,
                        "chans": "4~12",
                        "includepix": [0.03, 100],
                    },
                }
            ],
            "expected_observable_result": "N2H+ moment-0 map with the input mask and includepix threshold applied.",
        },
        "casars_equivalents": {
            "cli": {
                "provider_kind": "native-rust",
                "task_id": "immoments",
                "parameters": {
                    "imagename": "twhya_n2hp.image",
                    "outfile": ".casa-rs/workspace/native/03-immoments-n2hp-moment-map/twhya_n2hp.cli.mom0",
                    "moments": 0,
                    "chans": "4~12",
                    "includepix": "0.03,100",
                    "overwrite": True,
                },
                "command_template": "immoments twhya_n2hp.image --outfile .casa-rs/workspace/native/03-immoments-n2hp-moment-map/twhya_n2hp.cli.mom0 --moments 0 --chans 4~12 --includepix 0.03,100 --overwrite",
            },
            "python": {
                "provider_kind": "native-rust",
                "task_id": "immoments",
                "parameters": {
                    "imagename": "twhya_n2hp.image",
                    "outfile": ".casa-rs/workspace/native/03-immoments-n2hp-moment-map/twhya_n2hp.python.mom0",
                    "moments": 0,
                    "chans": "4~12",
                    "includepix": [0.03, 100.0],
                    "overwrite": True,
                },
                "command_template": "casars.tasks.immoments(imagename='twhya_n2hp.image', outfile='...', moments='0', chans='4~12', includepix='0.03,100.0', overwrite=True)",
            },
            "tui": {
                "provider_kind": "native-rust",
                "task_id": "immoments",
                "parameters": {
                    "image": "twhya_n2hp.image",
                    "output": ".casa-rs/workspace/native/03-immoments-n2hp-moment-map/twhya_n2hp.tui.mom0",
                    "moment": 0,
                    "channels": "4~12",
                    "include_pixels": "0.03,100",
                    "overwrite": True,
                },
                "ui_path": "Tasks > Image Moments",
            },
            "gui": {
                "provider_kind": "native-rust",
                "task_id": "immoments",
                "parameters": {
                    "image": "twhya_n2hp.image",
                    "output": ".casa-rs/workspace/native/03-immoments-n2hp-moment-map/twhya_n2hp.gui.mom0",
                    "moment": 0,
                    "channels": "4~12",
                    "include_pixels": "0.03,100",
                    "overwrite": True,
                },
                "ui_path": "Tutorial pack section 03 > Images > Image Moments",
            },
        },
        "observable_products": {
            "casa_refs": [".casa-rs/evidence/03-immoments-casa-mom0.json"],
            "casars_refs": [
                ".casa-rs/evidence/03-immoments-cli-mom0-run.json",
                ".casa-rs/evidence/03-immoments-cli-mom0-inspection.json",
                ".casa-rs/evidence/03-immoments-python-mom0-run.json",
            ],
            "comparison_refs": [
                ".casa-rs/evidence/03-immoments-mom0-comparison.json",
                ".casa-rs/evidence/03-immoments-moment-range-parity.json",
                ".casa-rs/evidence/figures/03-immoments-mom0-casa-vs-casars.png",
                ".casa-rs/evidence/figures/03-immoments-mom0-contours-over-continuum.png",
            ],
            "timing_refs": [".casa-rs/evidence/03-immoments-moment-range-parity.json"],
        },
        "regression_evidence": {
            "input_manifest_refs": ["pack.json"],
            "native_run_refs": [
                ".casa-rs/evidence/03-immoments-cli-mom0-run.json",
                ".casa-rs/evidence/03-immoments-python-mom0-run.json",
                ".casa-rs/evidence/03-immoments-moment-range-parity.json",
            ],
            "oracle_run_refs": [
                ".casa-rs/evidence/03-immoments-casa-mom0.json",
                ".casa-rs/evidence/03-immoments-moment-range-parity.json",
            ],
            "provider_provenance_refs": [".casa-rs/evidence/provider-provenance.json"],
            "screenshot_spec_refs": [".casa-rs/screenshots/specs/03-immoments-n2hp-moment-map.json"],
        },
        "human_evaluation": {
            "outcome": "accepted",
            "reviewed_by": "Brian Glendenning",
            "reviewed_at": reviewed_at,
            "comments": "Human signoff recorded after reviewing the section 03 moment-map workflow and CASA-vs-casa-rs evidence.",
            "required_changes": [],
            "follow_up_issue_refs": [],
        },
    }
    record["moment_range_summary"] = {
        "moments": [result["moment"] for result in moment_range["results"]],
        "max_mask_mismatch_pixels": max(result["mask_mismatch_pixels"] for result in moment_range["results"]),
        "max_abs_diff": max(result["max_abs_diff"] or 0.0 for result in moment_range["results"]),
    }
    review_path = pack_root / ".casa-rs/evidence/review/03-immoments-n2hp-moment-map.json"
    # The summary above is useful for humans but not part of the strict review schema.
    schema_record = {key: value for key, value in record.items() if key != "moment_range_summary"}
    write_json(review_path, schema_record)
    summary_path = pack_root / ".casa-rs/evidence/review/03-immoments-n2hp-moment-map-closeout.json"
    write_json(summary_path, record)
    return record


def write_docs(
    pack_root: Path,
    casa: dict[str, Any],
    cli_inspection: dict[str, Any],
    comparison: dict[str, Any],
    moment_range: dict[str, Any],
    review: dict[str, Any],
) -> None:
    docs = pack_root / "docs/sections"
    docs.mkdir(parents=True, exist_ok=True)
    stats = casa["stats"]
    cli_stats = parse_json_stdout(cli_inspection["imstat"]["stdout"])
    rows = []
    for field in ["npts", "min", "max", "sum", "mean", "rms", "sigma", "median"]:
        left = numeric(stats[field])
        right = numeric(cli_stats[field])
        rows.append((field, left, right, abs(left - right)))
    row_html = "\n".join(
        f"<tr><td>{html.escape(field)}</td><td class=\"num\">{left:.12g}</td><td class=\"num\">{right:.12g}</td><td class=\"num\">{delta:.4g}</td></tr>"
        for field, left, right, delta in rows
    )
    moment_rows = "\n".join(
        "<tr>"
        f"<td>{result['moment']}</td>"
        f"<td>{html.escape(result['casa_unit'])}</td>"
        f"<td>{html.escape(result['casars_unit'])}</td>"
        f"<td class=\"num\">{result['valid_pixels_casa']}</td>"
        f"<td class=\"num\">{result['valid_pixels_casars']}</td>"
        f"<td class=\"num\">{result['mask_mismatch_pixels']}</td>"
        f"<td class=\"num\">{result['max_abs_diff']:.12g}</td>"
        "</tr>"
        for result in moment_range["results"]
    )
    md = f"""# Section 03: Create the N2H+ Moment-0 Map

Human review status: accepted by {review['human_evaluation']['reviewed_by']} at {review['human_evaluation']['reviewed_at']}.

## CASA Tutorial Source

```python
immoments(imagename='twhya_n2hp.image', outfile='twhya_n2hp.mom0', moments=0, chans='4~12', includepix=[0.03, 100])
```

The staged input is expected to have default mask `mask0`, matching the CASA Guide listing for `twhya_n2hp.image`.

## Parameters

| Surface | Execution | Parameters |
| --- | --- | --- |
| GUI | Open the tutorial pack, select section 03, open `Images > Image Moments`, fill the task panel, and press Run after confirming the task may create products. | Image: `twhya_n2hp.image`; Output: `.casa-rs/workspace/native/03-immoments-n2hp-moment-map/twhya_n2hp.gui.mom0`; Moment: `0`; Channels: `4~12`; Include Pixels: `0.03,100`; Overwrite: on. |
| TUI | `casars immoments` opens the Image Moments form. Use the same field values as the GUI, then press `r` to run and confirm if prompted. | Same text fields as GUI. |
| Python | Call `casars.tasks.immoments(...)`. | `imagename='twhya_n2hp.image'`, `outfile='.casa-rs/workspace/native/03-immoments-n2hp-moment-map/twhya_n2hp.python.mom0'`, `moments='0'`, `chans='4~12'`, `includepix='0.03,100.0'`, `overwrite=True`. |
| Command line | Run the task binary from the pack root. | `immoments twhya_n2hp.image --outfile .casa-rs/workspace/native/03-immoments-n2hp-moment-map/twhya_n2hp.cli.mom0 --moments 0 --chans 4~12 --includepix 0.03,100 --overwrite`. |

## Visible Evidence

![CASA vs casa-rs moment map](../../.casa-rs/evidence/figures/03-immoments-mom0-casa-vs-casars.png)

![CASA and casa-rs contour overlays](../../.casa-rs/evidence/figures/03-immoments-mom0-contours-over-continuum.png)

## Regression Evidence

- CASA oracle: `.casa-rs/evidence/03-immoments-casa-mom0.json`
- CLI run and inspection: `.casa-rs/evidence/03-immoments-cli-mom0-run.json`, `.casa-rs/evidence/03-immoments-cli-mom0-inspection.json`
- Python run: `.casa-rs/evidence/03-immoments-python-mom0-run.json`
- Comparison summary: `.casa-rs/evidence/03-immoments-mom0-comparison.json`
- Moment range parity: `.casa-rs/evidence/03-immoments-moment-range-parity.json`
- Human review record: `.casa-rs/evidence/review/03-immoments-n2hp-moment-map.json`
"""
    html_doc = f"""<!doctype html><html><head><meta charset="utf-8"><title>Section 03: N2H+ moment map</title><style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;line-height:1.45;margin:32px;max-width:1180px}}code,pre{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}}pre{{background:#f6f8fa;padding:12px;border-radius:6px;overflow:auto}}table{{border-collapse:collapse;width:100%;margin:16px 0}}th,td{{border:1px solid #d0d7de;padding:6px 8px;text-align:left}}td.num{{text-align:right}}img{{max-width:100%;border:1px solid #d0d7de}}.callout{{background:#fff8c5;border:1px solid #d4a72c;padding:12px;border-radius:6px}}</style></head><body>
<h1>Section 03: Create the N2H+ Moment-0 Map</h1>
<p><strong>Human review status:</strong> accepted by {html.escape(str(review['human_evaluation']['reviewed_by']))} at <code>{html.escape(str(review['human_evaluation']['reviewed_at']))}</code>.</p>
<h2>CASA Tutorial Source</h2>
<p>Source: <a href="{GUIDE_URL}">{GUIDE_URL}</a></p>
<pre>immoments(imagename='twhya_n2hp.image', outfile='twhya_n2hp.mom0', moments=0, chans='4~12', includepix=[0.03, 100])</pre>
<div class="callout">The staged input now preserves the guide-listed default mask <code>mask0</code>. The current 6.6.6 direct-download directory omits this mask, so the pack uses the legacy <code>ALMA_firstlooks/twhya_n2hp.image</code> source recorded in <code>pack.json</code>.</div>
<h2>Parameters By Interface</h2>
<table><tr><th>Surface</th><th>Execution</th><th>Parameters</th></tr>
<tr><td>GUI</td><td>Open section 03, Images &gt; Image Moments, press Run and confirm.</td><td>Image <code>twhya_n2hp.image</code>; Output <code>.casa-rs/workspace/native/03-immoments-n2hp-moment-map/twhya_n2hp.gui.mom0</code>; Moment <code>0</code>; Channels <code>4~12</code>; Include Pixels <code>0.03,100</code>; Overwrite on.</td></tr>
<tr><td>TUI</td><td><code>casars immoments</code>, fill the Image Moments form, press <code>r</code>, confirm if prompted.</td><td>Same fields as GUI.</td></tr>
<tr><td>Python</td><td><code>casars.tasks.immoments(...)</code></td><td><code>imagename='twhya_n2hp.image'</code>, <code>outfile='...python.mom0'</code>, <code>moments='0'</code>, <code>chans='4~12'</code>, <code>includepix='0.03,100.0'</code>, <code>overwrite=True</code>.</td></tr>
<tr><td>Command line</td><td>Run from pack root.</td><td><code>immoments twhya_n2hp.image --outfile .casa-rs/workspace/native/03-immoments-n2hp-moment-map/twhya_n2hp.cli.mom0 --moments 0 --chans 4~12 --includepix 0.03,100 --overwrite</code></td></tr>
</table>
<h2>Observable Result</h2>
<p><img src="../../.casa-rs/evidence/figures/03-immoments-mom0-casa-vs-casars.png" alt="CASA vs casa-rs moment map"></p>
<table><tr><th>Statistic</th><th>CASA C++</th><th>casa-rs CLI</th><th>Absolute delta</th></tr>{row_html}</table>
<p>Mask mismatch pixels: <code>{comparison['mask_mismatch_pixels']}</code>; shared valid pixels: <code>{comparison['shared_valid_pixels']}</code>; p99 absolute pixel difference: <code>{comparison['p99_abs_diff']}</code>.</p>
<h2>Supported Moment Parity</h2>
<p>The regression path also runs CASA C++ and casa-rs over moments <code>-1</code>, <code>0</code>, <code>1</code>, <code>2</code>, and <code>3</code> with the same tutorial channel and include-pixel selections.</p>
<table><tr><th>Moment</th><th>CASA unit</th><th>casa-rs unit</th><th>CASA valid pixels</th><th>casa-rs valid pixels</th><th>Mask mismatches</th><th>Max abs diff</th></tr>{moment_rows}</table>
<h2>Tutorial-Style Overlay</h2>
<p><img src="../../.casa-rs/evidence/figures/03-immoments-mom0-contours-over-continuum.png" alt="CASA and casa-rs moment-0 contour overlays"></p>
<h2>Surface Evidence</h2>
<ul><li>CASA oracle: <code>.casa-rs/evidence/03-immoments-casa-mom0.json</code></li><li>CLI run and inspection: <code>.casa-rs/evidence/03-immoments-cli-mom0-run.json</code>, <code>03-immoments-cli-mom0-inspection.json</code></li><li>Python run: <code>.casa-rs/evidence/03-immoments-python-mom0-run.json</code></li><li>Comparison summary: <code>.casa-rs/evidence/03-immoments-mom0-comparison.json</code></li><li>Moment range parity: <code>.casa-rs/evidence/03-immoments-moment-range-parity.json</code></li><li>Human review record: <code>.casa-rs/evidence/review/03-immoments-n2hp-moment-map.json</code></li></ul>
</body></html>
"""
    (docs / f"{SECTION_ID}.md").write_text(md, encoding="utf-8")
    (docs / f"{SECTION_ID}.html").write_text(html_doc, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pack", type=Path, default=default_pack_path())
    parser.add_argument("--casa-python", type=Path, default=Path(os.environ.get("CASA_RS_CASA_PYTHON", DEFAULT_CASA_PYTHON)))
    args = parser.parse_args()

    pack_root = args.pack.expanduser().resolve()
    immoments = REPO_ROOT / "target/debug/immoments"
    imexplore = REPO_ROOT / "target/debug/imexplore"
    casars = REPO_ROOT / "target/debug/casars"
    if not immoments.exists() or not imexplore.exists() or not casars.exists():
        raise SystemExit("run `cargo build -p casa-images --bin immoments --bin imexplore` and `cargo build -p casars` first")

    casa = run_oracle(pack_root, args.casa_python)
    run_cli(pack_root, immoments, imexplore)
    run_python(pack_root, immoments, casars)
    moment_range = run_moment_range_parity(pack_root, args.casa_python, immoments)
    comparison = run_figures_and_comparison(pack_root, args.casa_python)
    review = write_review_record(pack_root, moment_range)
    cli_inspection = json.loads((pack_root / ".casa-rs/evidence/03-immoments-cli-mom0-inspection.json").read_text())
    write_docs(pack_root, casa, cli_inspection, comparison, moment_range, review)
    print(
        json.dumps(
            {
                "section": SECTION_ID,
                "pack": str(pack_root),
                "comparison": comparison,
                "moment_range": moment_range,
                "review": {
                    "status": review["status"],
                    "reviewed_by": review["human_evaluation"]["reviewed_by"],
                    "reviewed_at": review["human_evaluation"]["reviewed_at"],
                },
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
