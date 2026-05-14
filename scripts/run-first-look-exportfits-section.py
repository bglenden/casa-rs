#!/usr/bin/env python3
"""Run the First Look image-analysis exportfits tutorial section."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import html
import json
import math
import os
import shutil
import struct
import subprocess
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CASA_PYTHON = Path("/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python")
SECTION_ID = "04-exportfits-products"
GUIDE_URL = "https://casaguides.nrao.edu/index.php/First_Look_at_Image_Analysis"
TUTORIAL_ID = "alma/first-look/twhya/image-analysis"
PACK_ID = "alma-first-look-image-analysis"
OUTDIR = Path(".casa-rs/workspace/native") / SECTION_ID
ORACLE_OUTDIR = Path(".casa-rs/workspace/oracle") / SECTION_ID
FIGDIR = Path(".casa-rs/evidence/figures")
PRODUCTS = [
    {
        "id": "continuum",
        "imagename": "twhya_cont.image",
        "fits_basename": "twhya_cont.fits",
        "velocity": False,
        "tutorial_call": "exportfits(imagename='twhya_cont.image', fitsimage='twhya_cont.fits', overwrite=True)",
    },
    {
        "id": "n2hp-cube",
        "imagename": "twhya_n2hp.image",
        "fits_basename": "twhya_n2hp.fits",
        "velocity": True,
        "tutorial_call": "exportfits(imagename='twhya_n2hp.image', fitsimage='twhya_n2hp.fits', velocity=True, overwrite=True)",
    },
]
HEADER_KEYS = [
    "NAXIS",
    "NAXIS1",
    "NAXIS2",
    "NAXIS3",
    "NAXIS4",
    "BUNIT",
    "BMAJ",
    "BMIN",
    "BPA",
    "CTYPE1",
    "CTYPE2",
    "CTYPE3",
    "CTYPE4",
    "CRVAL1",
    "CRVAL2",
    "CRVAL3",
    "CRVAL4",
    "CRPIX1",
    "CRPIX2",
    "CRPIX3",
    "CRPIX4",
    "CDELT1",
    "CDELT2",
    "CDELT3",
    "CDELT4",
    "CUNIT1",
    "CUNIT2",
    "CUNIT3",
    "CUNIT4",
    "SPECSYS",
    "RESTFRQ",
]


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


def fits_card_value(raw: str) -> Any:
    raw = raw.strip()
    if not raw:
        return None
    if raw.startswith("'"):
        end = raw.find("'", 1)
        return raw[1:end].strip() if end >= 0 else raw.strip("' ")
    token = raw.split()[0]
    if token == "T":
        return True
    if token == "F":
        return False
    try:
        if any(char in token for char in ".eEdD"):
            return float(token.replace("D", "E").replace("d", "e"))
        return int(token)
    except ValueError:
        return token


def read_fits_primary(path: Path) -> tuple[dict[str, Any], list[float], list[int]]:
    data = path.read_bytes()
    cards: list[str] = []
    offset = 0
    while True:
        block = data[offset : offset + 2880]
        if len(block) != 2880:
            raise ValueError(f"{path} ended before FITS header END card")
        offset += 2880
        for index in range(0, 2880, 80):
            card = block[index : index + 80].decode("ascii", "replace")
            cards.append(card)
            if card.startswith("END"):
                header: dict[str, Any] = {}
                for item in cards:
                    key = item[:8].strip()
                    if not key or key in {"END", "COMMENT", "HISTORY"}:
                        continue
                    if item[8:10] != "= ":
                        continue
                    value = item[10:].split("/", 1)[0]
                    header[key] = fits_card_value(value)
                shape = [int(header[f"NAXIS{i}"]) for i in range(1, int(header.get("NAXIS", 0)) + 1)]
                count = math.prod(shape)
                bitpix = int(header["BITPIX"])
                if bitpix != -32:
                    raise ValueError(f"{path} has unsupported BITPIX={bitpix}; expected -32")
                payload = data[offset : offset + count * 4]
                values = list(struct.unpack(f">{count}f", payload))
                return header, values, shape


def finite_mask(values: list[float]) -> list[bool]:
    return [math.isfinite(value) for value in values]


def compare_values(casa_values: list[float], native_values: list[float]) -> dict[str, Any]:
    if len(casa_values) != len(native_values):
        return {
            "same_length": False,
            "casa_count": len(casa_values),
            "casars_count": len(native_values),
        }
    casa_mask = finite_mask(casa_values)
    native_mask = finite_mask(native_values)
    shared_diffs = [
        native - casa
        for casa, native, casa_ok, native_ok in zip(casa_values, native_values, casa_mask, native_mask, strict=True)
        if casa_ok and native_ok
    ]
    abs_diffs = sorted(abs(value) for value in shared_diffs)
    p99 = abs_diffs[int(0.99 * (len(abs_diffs) - 1))] if abs_diffs else None
    return {
        "same_length": True,
        "pixel_count": len(casa_values),
        "finite_pixels_casa": sum(casa_mask),
        "finite_pixels_casars": sum(native_mask),
        "finite_mask_mismatch_pixels": sum(left != right for left, right in zip(casa_mask, native_mask, strict=True)),
        "shared_finite_pixels": len(shared_diffs),
        "max_abs_diff": max(abs_diffs) if abs_diffs else None,
        "mean_abs_diff": (sum(abs(value) for value in shared_diffs) / len(shared_diffs)) if shared_diffs else None,
        "p99_abs_diff": p99,
    }


def compare_headers(casa_header: dict[str, Any], native_header: dict[str, Any]) -> dict[str, Any]:
    rows = []
    mismatches = []
    for key in HEADER_KEYS:
        casa_value = casa_header.get(key)
        native_value = native_header.get(key)
        if isinstance(casa_value, float) or isinstance(native_value, float):
            matched = (
                isinstance(casa_value, (int, float))
                and isinstance(native_value, (int, float))
                and abs(float(casa_value) - float(native_value)) <= 1e-7 * max(1.0, abs(float(casa_value)))
            )
        else:
            matched = casa_value == native_value
        rows.append({"key": key, "casa": casa_value, "casars": native_value, "matched": matched})
        if not matched:
            mismatches.append(key)
    return {"rows": rows, "mismatched_keys": mismatches}


def run_oracle(pack_root: Path, casa_python: Path) -> dict[str, Any]:
    script = "\n".join(
        [
            "import json, shutil",
            "from pathlib import Path",
            "from casatasks import exportfits",
            f"products = {PRODUCTS!r}",
            f"outdir = Path({str(ORACLE_OUTDIR)!r})",
            "shutil.rmtree(outdir, ignore_errors=True)",
            "outdir.mkdir(parents=True, exist_ok=True)",
            "results = []",
            "for product in products:",
            "    fits_path = outdir / product['fits_basename']",
            "    result = exportfits(imagename=product['imagename'], fitsimage=str(fits_path), velocity=product['velocity'], overwrite=True)",
            "    results.append({'id': product['id'], 'imagename': product['imagename'], 'fitsimage': str(fits_path), 'velocity': product['velocity'], 'result': result})",
            "print(json.dumps({'results': results}, indent=2, sort_keys=True, default=str))",
        ]
    )
    result = run_command([str(casa_python), "-c", script], cwd=pack_root)
    payload = parse_json_stdout(result["stdout"])
    payload["elapsed_seconds"] = result["elapsed_seconds"]
    write_json(pack_root / ".casa-rs/evidence/04-exportfits-casa-runs.json", payload)
    return payload


def run_cli(pack_root: Path, exportfits: Path) -> dict[str, Any]:
    results = []
    (pack_root / OUTDIR).mkdir(parents=True, exist_ok=True)
    for product in PRODUCTS:
        fits_path = OUTDIR / product["fits_basename"]
        argv = [
            str(exportfits),
            product["imagename"],
            str(fits_path),
            "--overwrite",
        ]
        if product["velocity"]:
            argv.append("--velocity")
        run = run_command(argv, cwd=pack_root)
        results.append({**run, "id": product["id"], "fitsimage": str(fits_path), "result": parse_json_stdout(run["stdout"])})
    payload = {"results": results}
    write_json(pack_root / ".casa-rs/evidence/04-exportfits-cli-runs.json", payload)
    return payload


def run_python(pack_root: Path, exportfits: Path) -> dict[str, Any]:
    results = []
    (pack_root / OUTDIR).mkdir(parents=True, exist_ok=True)
    env = {"PYTHONPATH": str(REPO_ROOT / "crates/casars-python/python")}
    for product in PRODUCTS:
        fits_path = OUTDIR / f"{Path(product['fits_basename']).stem}.python.fits"
        script = "\n".join(
            [
                "import json",
                "from casars.tasks import image_analysis",
                (
                    "result = image_analysis.exportfits("
                    f"{product['imagename']!r}, {str(fits_path)!r}, "
                    f"velocity={product['velocity']!r}, overwrite=True, binary={str(exportfits)!r})"
                ),
                "print(json.dumps(result, indent=2, sort_keys=True))",
            ]
        )
        run = run_command(["python3", "-c", script], cwd=pack_root, env=env)
        results.append({**run, "id": product["id"], "fitsimage": str(fits_path), "result": parse_json_stdout(run["stdout"])})
    payload = {"results": results}
    write_json(pack_root / ".casa-rs/evidence/04-exportfits-python-runs.json", payload)
    return payload


def run_comparison(pack_root: Path) -> dict[str, Any]:
    results = []
    for product in PRODUCTS:
        casa_path = pack_root / ORACLE_OUTDIR / product["fits_basename"]
        native_path = pack_root / OUTDIR / product["fits_basename"]
        casa_header, casa_values, casa_shape = read_fits_primary(casa_path)
        native_header, native_values, native_shape = read_fits_primary(native_path)
        results.append(
            {
                "id": product["id"],
                "imagename": product["imagename"],
                "velocity": product["velocity"],
                "casa_fits": str(casa_path),
                "casars_fits": str(native_path),
                "casa_shape": casa_shape,
                "casars_shape": native_shape,
                "header_comparison": compare_headers(casa_header, native_header),
                "pixel_comparison": compare_values(casa_values, native_values),
            }
        )
    payload = {"schema_version": "tutorial-exportfits-comparison.v0", "results": results}
    write_json(pack_root / ".casa-rs/evidence/04-exportfits-comparison.json", payload)
    return payload


def run_figures(pack_root: Path, casa_python: Path) -> dict[str, Any]:
    script = r"""
import json
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

pack_root = Path(PACK_ROOT)
figdir = pack_root / ".casa-rs/evidence/figures"
figdir.mkdir(parents=True, exist_ok=True)

def read_fits(path):
    import math, struct
    data = path.read_bytes()
    offset = 0
    cards = []
    while True:
        block = data[offset:offset+2880]
        offset += 2880
        for index in range(0, 2880, 80):
            card = block[index:index+80].decode("ascii", "replace")
            cards.append(card)
            if card.startswith("END"):
                header = {}
                for item in cards:
                    key = item[:8].strip()
                    if not key or item[8:10] != "= ":
                        continue
                    raw = item[10:].split("/", 1)[0].strip()
                    if raw.startswith("'"):
                        value = raw.strip("' ")
                    elif any(ch in raw for ch in ".eEdD"):
                        value = float(raw.split()[0].replace("D", "E"))
                    else:
                        try:
                            value = int(raw.split()[0])
                        except Exception:
                            value = raw
                    header[key] = value
                shape = [int(header[f"NAXIS{i}"]) for i in range(1, int(header["NAXIS"]) + 1)]
                count = math.prod(shape)
                values = np.asarray(struct.unpack(f">{count}f", data[offset:offset+count*4]), dtype=np.float32)
                return header, values.reshape(tuple(reversed(shape)))

products = [
    ("continuum", "twhya_cont.fits", "Continuum FITS"),
    ("n2hp-cube", "twhya_n2hp.fits", "N2H+ cube FITS channel 0"),
]
figures = {}
for product_id, filename, title in products:
    casa_header, casa = read_fits(pack_root / ".casa-rs/workspace/oracle/04-exportfits-products" / filename)
    native_header, native = read_fits(pack_root / ".casa-rs/workspace/native/04-exportfits-products" / filename)
    while casa.ndim > 2:
        casa = casa[0]
        native = native[0]
    diff = native - casa
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.2), constrained_layout=True)
    vmin = np.nanpercentile(casa, 1)
    vmax = np.nanpercentile(casa, 99)
    for ax, label, plane in [(axes[0], "CASA C++", casa), (axes[1], "casa-rs", native)]:
        im = ax.imshow(plane, origin="lower", cmap="inferno", vmin=vmin, vmax=vmax)
        ax.set_title(f"{label} {title}")
        ax.set_xlabel("FITS x pixel")
        ax.set_ylabel("FITS y pixel")
        fig.colorbar(im, ax=ax, shrink=0.84)
    limit = max(float(np.nanmax(np.abs(diff))), 1e-12)
    im = axes[2].imshow(diff, origin="lower", cmap="coolwarm", vmin=-limit, vmax=limit)
    axes[2].set_title("casa-rs - CASA")
    axes[2].set_xlabel("FITS x pixel")
    axes[2].set_ylabel("FITS y pixel")
    fig.colorbar(im, ax=axes[2], shrink=0.84)
    figure = figdir / f"04-exportfits-{product_id}-casa-vs-casars.png"
    fig.savefig(figure, dpi=160)
    plt.close(fig)
    figures[product_id] = str(figure)
print(json.dumps({"figures": figures}, indent=2, sort_keys=True))
"""
    result = run_command([str(casa_python), "-c", script.replace("PACK_ROOT", repr(str(pack_root)))], cwd=pack_root)
    payload = parse_json_stdout(result["stdout"])
    write_json(pack_root / ".casa-rs/evidence/04-exportfits-figures.json", payload)
    return payload


def write_review_record(pack_root: Path, comparison: dict[str, Any]) -> dict[str, Any]:
    record = {
        "schema_version": "tutorial-pack-review.v0",
        "pack_id": PACK_ID,
        "tutorial_id": TUTORIAL_ID,
        "section_id": SECTION_ID,
        "status": "pending-human-review",
        "casa_source": {
            "guide_url": GUIDE_URL,
            "section_anchor": "FITS_export",
            "task_calls": [
                {
                    "task_id": "exportfits",
                    "parameters": {
                        "imagename": product["imagename"],
                        "fitsimage": product["fits_basename"],
                        **({"velocity": True} if product["velocity"] else {}),
                        "overwrite": True,
                    },
                }
                for product in PRODUCTS
            ],
            "expected_observable_result": "Continuum and N2H+ cube FITS products readable by external FITS tooling, with WCS/header fields and image arrays compared against CASA.",
        },
        "casars_equivalents": {
            "cli": {
                "provider_kind": "native-rust",
                "task_id": "exportfits",
                "parameters": {
                    "products": [
                        {
                            "imagename": product["imagename"],
                            "fitsimage": f".casa-rs/workspace/native/{SECTION_ID}/{product['fits_basename']}",
                            "velocity": product["velocity"],
                            "overwrite": True,
                        }
                        for product in PRODUCTS
                    ]
                },
                "command_template": "exportfits <imagename> <fitsimage> [--velocity] --overwrite",
            },
            "python": {
                "provider_kind": "native-rust",
                "task_id": "exportfits",
                "parameters": {
                    "products": [
                        {
                            "imagename": product["imagename"],
                            "fitsimage": f".casa-rs/workspace/native/{SECTION_ID}/{Path(product['fits_basename']).stem}.python.fits",
                            "velocity": product["velocity"],
                            "overwrite": True,
                        }
                        for product in PRODUCTS
                    ]
                },
                "command_template": "casars.tasks.image_analysis.exportfits(imagename, fitsimage, velocity=..., overwrite=True)",
            },
            "tui": {
                "provider_kind": "native-rust",
                "task_id": "exportfits",
                "parameters": {
                    "products": [
                        {
                            "Image": product["imagename"],
                            "FITS": f".casa-rs/workspace/native/{SECTION_ID}/{Path(product['fits_basename']).stem}.tui.fits",
                            "Velocity Axis": product["velocity"],
                            "Overwrite": True,
                        }
                        for product in PRODUCTS
                    ]
                },
                "ui_path": "Tasks > Export FITS",
            },
            "gui": {
                "provider_kind": "native-rust",
                "task_id": "exportfits",
                "parameters": {
                    "products": [
                        {
                            "Image": product["imagename"],
                            "FITS": f".casa-rs/workspace/native/{SECTION_ID}/{Path(product['fits_basename']).stem}.gui.fits",
                            "Velocity Axis": product["velocity"],
                            "Overwrite": True,
                        }
                        for product in PRODUCTS
                    ]
                },
                "ui_path": "Tutorial pack section 04 > Images > Export FITS",
            },
        },
        "observable_products": {
            "casa_refs": [".casa-rs/evidence/04-exportfits-casa-runs.json"],
            "casars_refs": [
                ".casa-rs/evidence/04-exportfits-cli-runs.json",
                ".casa-rs/evidence/04-exportfits-python-runs.json",
            ],
            "comparison_refs": [
                ".casa-rs/evidence/04-exportfits-comparison.json",
                ".casa-rs/evidence/04-exportfits-figures.json",
                ".casa-rs/evidence/figures/04-exportfits-continuum-casa-vs-casars.png",
                ".casa-rs/evidence/figures/04-exportfits-n2hp-cube-casa-vs-casars.png",
            ],
            "timing_refs": [
                ".casa-rs/evidence/04-exportfits-casa-runs.json",
                ".casa-rs/evidence/04-exportfits-cli-runs.json",
                ".casa-rs/evidence/04-exportfits-python-runs.json",
            ],
        },
        "regression_evidence": {
            "input_manifest_refs": ["pack.json"],
            "native_run_refs": [
                ".casa-rs/evidence/04-exportfits-cli-runs.json",
                ".casa-rs/evidence/04-exportfits-python-runs.json",
            ],
            "oracle_run_refs": [".casa-rs/evidence/04-exportfits-casa-runs.json"],
            "provider_provenance_refs": [".casa-rs/evidence/provider-provenance.json"],
            "screenshot_spec_refs": [".casa-rs/screenshots/specs/04-exportfits-products.json"],
        },
        "human_evaluation": {
            "outcome": "pending",
            "reviewed_by": None,
            "reviewed_at": None,
            "comments": "Awaiting human review of section 04 FITS export evidence.",
            "required_changes": [],
            "follow_up_issue_refs": [],
        },
    }
    summary = {
        **record,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "comparison_summary": {
            product["id"]: {
                "header_mismatches": product["header_comparison"]["mismatched_keys"],
                "pixel": product["pixel_comparison"],
            }
            for product in comparison["results"]
        },
    }
    write_json(pack_root / ".casa-rs/evidence/review/04-exportfits-products.json", record)
    write_json(pack_root / ".casa-rs/evidence/review/04-exportfits-products-closeout.json", summary)
    return summary


def write_docs(pack_root: Path, comparison: dict[str, Any], figures: dict[str, Any], review: dict[str, Any]) -> None:
    docs = pack_root / "docs/sections"
    docs.mkdir(parents=True, exist_ok=True)
    source_calls = "\n\n".join(f"```python\n{product['tutorial_call']}\n```" for product in PRODUCTS)
    product_rows_md = "\n".join(
        (
            f"| `{result['id']}` | `{result['casa_shape']}` | `{result['casars_shape']}` | "
            f"`{', '.join(result['header_comparison']['mismatched_keys']) or 'none'}` | "
            f"`{result['pixel_comparison']['finite_mask_mismatch_pixels']}` | "
            f"`{result['pixel_comparison']['max_abs_diff']}` |"
        )
        for result in comparison["results"]
    )
    product_rows_html = "\n".join(
        "<tr>"
        f"<td>{html.escape(result['id'])}</td>"
        f"<td><code>{html.escape(str(result['casa_shape']))}</code></td>"
        f"<td><code>{html.escape(str(result['casars_shape']))}</code></td>"
        f"<td><code>{html.escape(', '.join(result['header_comparison']['mismatched_keys']) or 'none')}</code></td>"
        f"<td class=\"num\">{result['pixel_comparison']['finite_mask_mismatch_pixels']}</td>"
        f"<td class=\"num\">{result['pixel_comparison']['max_abs_diff']}</td>"
        f"<td class=\"num\">{result['pixel_comparison']['p99_abs_diff']}</td>"
        "</tr>"
        for result in comparison["results"]
    )
    header_sections = []
    for result in comparison["results"]:
        rows = "\n".join(
            "<tr>"
            f"<td><code>{html.escape(row['key'])}</code></td>"
            f"<td><code>{html.escape(str(row['casa']))}</code></td>"
            f"<td><code>{html.escape(str(row['casars']))}</code></td>"
            f"<td>{'yes' if row['matched'] else 'no'}</td>"
            "</tr>"
            for row in result["header_comparison"]["rows"]
        )
        header_sections.append(
            f"<h3>{html.escape(result['id'])} FITS Header Fields</h3>"
            f"<table><tr><th>Key</th><th>CASA C++</th><th>casa-rs</th><th>Match</th></tr>{rows}</table>"
        )
    md = f"""# Section 04: Export Image Products To FITS

Human review status: pending.

## CASA Tutorial Source

{source_calls}

The guide exports the continuum image directly and exports the N2H+ cube with `velocity=True`.

## Parameters

| Surface | Execution | Parameters |
| --- | --- | --- |
| GUI | Open the tutorial pack, select section 04, open `Images > Export FITS`, run once for each product. | Continuum: Image `twhya_cont.image`, FITS `.casa-rs/workspace/native/04-exportfits-products/twhya_cont.gui.fits`, Velocity Axis off, Overwrite on. Cube: Image `twhya_n2hp.image`, FITS `.casa-rs/workspace/native/04-exportfits-products/twhya_n2hp.gui.fits`, Velocity Axis on, Overwrite on. |
| TUI | `casars exportfits` opens the Export FITS form. Fill the same fields as the GUI, then press `r` to run and confirm if prompted. | Same text fields as GUI. |
| Python | Call `casars.tasks.image_analysis.exportfits(...)` twice. | `exportfits('twhya_cont.image', '.../twhya_cont.python.fits', overwrite=True)` and `exportfits('twhya_n2hp.image', '.../twhya_n2hp.python.fits', velocity=True, overwrite=True)`. |
| Command line | Run the task binary from the pack root twice. | `exportfits twhya_cont.image .casa-rs/workspace/native/04-exportfits-products/twhya_cont.fits --overwrite`; `exportfits twhya_n2hp.image .casa-rs/workspace/native/04-exportfits-products/twhya_n2hp.fits --velocity --overwrite`. |

## Visible Evidence

![Continuum FITS CASA vs casa-rs](../../.casa-rs/evidence/figures/04-exportfits-continuum-casa-vs-casars.png)

![N2H+ cube FITS CASA vs casa-rs](../../.casa-rs/evidence/figures/04-exportfits-n2hp-cube-casa-vs-casars.png)

## Comparison Summary

| Product | CASA shape | casa-rs shape | Header mismatches | finite-mask mismatches | max abs pixel diff |
| --- | --- | --- | --- | --- | --- |
{product_rows_md}

## Regression Evidence

- CASA oracle: `.casa-rs/evidence/04-exportfits-casa-runs.json`
- CLI runs: `.casa-rs/evidence/04-exportfits-cli-runs.json`
- Python runs: `.casa-rs/evidence/04-exportfits-python-runs.json`
- FITS comparison: `.casa-rs/evidence/04-exportfits-comparison.json`
- Human review record: `.casa-rs/evidence/review/04-exportfits-products.json`
"""
    html_doc = f"""<!doctype html><html><head><meta charset="utf-8"><title>Section 04: Export FITS products</title><style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;line-height:1.45;margin:32px;max-width:1180px}}code,pre{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}}pre{{background:#f6f8fa;padding:12px;border-radius:6px;overflow:auto}}table{{border-collapse:collapse;width:100%;margin:16px 0}}th,td{{border:1px solid #d0d7de;padding:6px 8px;text-align:left;vertical-align:top}}td.num{{text-align:right}}img{{max-width:100%;border:1px solid #d0d7de}}.callout{{background:#fff8c5;border:1px solid #d4a72c;padding:12px;border-radius:6px}}</style></head><body>
<h1>Section 04: Export Image Products To FITS</h1>
<p><strong>Human review status:</strong> pending.</p>
<h2>CASA Tutorial Source</h2>
<p>Source: <a href="{GUIDE_URL}">{GUIDE_URL}</a></p>
<pre>{html.escape(chr(10).join(product['tutorial_call'] for product in PRODUCTS))}</pre>
<div class="callout">The CASA guide exports the continuum image directly and exports the N2H+ cube with <code>velocity=True</code>.</div>
<h2>Parameters By Interface</h2>
<table><tr><th>Surface</th><th>Execution</th><th>Parameters</th></tr>
<tr><td>GUI</td><td>Open section 04, Images &gt; Export FITS, run once for each product.</td><td>Continuum: Image <code>twhya_cont.image</code>, FITS <code>.casa-rs/workspace/native/04-exportfits-products/twhya_cont.gui.fits</code>, Velocity Axis off, Overwrite on.<br>Cube: Image <code>twhya_n2hp.image</code>, FITS <code>.casa-rs/workspace/native/04-exportfits-products/twhya_n2hp.gui.fits</code>, Velocity Axis on, Overwrite on.</td></tr>
<tr><td>TUI</td><td><code>casars exportfits</code>, fill the Export FITS form, press <code>r</code>, confirm if prompted.</td><td>Same fields as GUI.</td></tr>
<tr><td>Python</td><td><code>casars.tasks.image_analysis.exportfits(...)</code></td><td><code>exportfits('twhya_cont.image', '.../twhya_cont.python.fits', overwrite=True)</code><br><code>exportfits('twhya_n2hp.image', '.../twhya_n2hp.python.fits', velocity=True, overwrite=True)</code></td></tr>
<tr><td>Command line</td><td>Run from pack root.</td><td><code>exportfits twhya_cont.image .casa-rs/workspace/native/04-exportfits-products/twhya_cont.fits --overwrite</code><br><code>exportfits twhya_n2hp.image .casa-rs/workspace/native/04-exportfits-products/twhya_n2hp.fits --velocity --overwrite</code></td></tr>
</table>
<h2>Observable Result</h2>
<p><img src="../../.casa-rs/evidence/figures/04-exportfits-continuum-casa-vs-casars.png" alt="Continuum FITS CASA vs casa-rs"></p>
<p><img src="../../.casa-rs/evidence/figures/04-exportfits-n2hp-cube-casa-vs-casars.png" alt="N2H+ cube FITS CASA vs casa-rs"></p>
<h2>Comparison Summary</h2>
<table><tr><th>Product</th><th>CASA shape</th><th>casa-rs shape</th><th>Header mismatches</th><th>finite-mask mismatches</th><th>Max abs diff</th><th>P99 abs diff</th></tr>{product_rows_html}</table>
{''.join(header_sections)}
<h2>Surface Evidence</h2>
<ul><li>CASA oracle: <code>.casa-rs/evidence/04-exportfits-casa-runs.json</code></li><li>CLI runs: <code>.casa-rs/evidence/04-exportfits-cli-runs.json</code></li><li>Python runs: <code>.casa-rs/evidence/04-exportfits-python-runs.json</code></li><li>FITS comparison: <code>.casa-rs/evidence/04-exportfits-comparison.json</code></li><li>Human review record: <code>.casa-rs/evidence/review/04-exportfits-products.json</code></li></ul>
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
    exportfits = REPO_ROOT / "target/debug/exportfits"
    if not exportfits.exists():
        raise SystemExit("run `cargo build -p casa-images --bin exportfits` first")

    shutil.rmtree(pack_root / OUTDIR, ignore_errors=True)
    shutil.rmtree(pack_root / ORACLE_OUTDIR, ignore_errors=True)
    run_oracle(pack_root, args.casa_python)
    run_cli(pack_root, exportfits)
    run_python(pack_root, exportfits)
    comparison = run_comparison(pack_root)
    figures = run_figures(pack_root, args.casa_python)
    review = write_review_record(pack_root, comparison)
    write_docs(pack_root, comparison, figures, review)
    print(
        json.dumps(
            {
                "section": SECTION_ID,
                "pack": str(pack_root),
                "comparison": comparison,
                "figures": figures,
                "review": {
                    "status": review["status"],
                    "outcome": review["human_evaluation"]["outcome"],
                },
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
