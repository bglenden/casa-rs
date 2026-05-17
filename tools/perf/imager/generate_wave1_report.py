#!/usr/bin/env python3
"""Build a portable HTML report for ImPerformance Wave 1 simobserve evidence."""

from __future__ import annotations

import argparse
import base64
import html
import json
import pathlib
from typing import Any


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=pathlib.Path, required=True)
    parser.add_argument("--generation-summary", type=pathlib.Path, required=True)
    parser.add_argument("--casa-smoke", type=pathlib.Path, required=True)
    parser.add_argument("--preview-summary", type=pathlib.Path, required=True)
    parser.add_argument("--parity-dir", type=pathlib.Path, required=True)
    parser.add_argument("--internal-io", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()

    plan = read_json(args.plan)
    generation = read_json(args.generation_summary)
    smoke = read_json(args.casa_smoke)
    previews = read_json(args.preview_summary)
    internal_io = read_json(args.internal_io)
    parity = load_parity(args.parity_dir)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        render_report(plan, generation, smoke, previews, parity, internal_io),
        encoding="utf-8",
    )
    print(args.output)


def read_json(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_parity(root: pathlib.Path) -> list[dict[str, Any]]:
    results = []
    for path in sorted(root.glob("*/simobserve-benchmark.json")):
        value = read_json(path)
        value["_path"] = str(path)
        results.append(value)
    return results


def render_report(
    plan: dict[str, Any],
    generation: dict[str, Any],
    smoke: dict[str, Any],
    previews: dict[str, Any],
    parity: list[dict[str, Any]],
    internal_io: dict[str, Any],
) -> str:
    generation_by_id = {item["dataset"]: item for item in generation["datasets"]}
    smoke_by_id = {item["dataset"]: item for item in smoke["results"]}
    preview_by_key = {
        (item["dataset"], item["mode"]): item
        for item in previews.get("runs", [])
        if item.get("returncode") == 0
    }

    dataset_rows = []
    for dataset in plan["datasets"]:
        actual = generation_by_id.get(dataset["id"], {})
        smoke_row = smoke_by_id.get(dataset["id"], {})
        timing = actual.get("report", {}).get("timing", {}).get("main_rows", {})
        dataset_rows.append(
            "<tr>"
            f"<td><code>{esc(dataset['id'])}</code></td>"
            f"<td>{esc(dataset['instrument'])}</td>"
            f"<td>{esc(dataset['family'])}</td>"
            f"<td>{esc(dataset['tier'])}</td>"
            f"<td>{bytes_text(dataset['target_size_bytes'])}</td>"
            f"<td>{bytes_text(actual.get('size_bytes'))}</td>"
            f"<td>{number(actual.get('elapsed_seconds'), 's')}</td>"
            f"<td>{int_text(actual.get('report', {}).get('main_row_count'))}</td>"
            f"<td>{int_text(actual.get('report', {}).get('channel_count'))}</td>"
            f"<td>{esc(smoke_row.get('status', 'missing'))}</td>"
            f"<td>{number(timing.get('prediction_millis'), 'ms')}</td>"
            f"<td>{number(timing.get('data_io_write_millis'), 'ms')}</td>"
            "</tr>"
        )

    preview_sections = []
    for dataset in plan["datasets"]:
        dirty = preview_by_key.get((dataset["id"], "dirty"))
        clean = preview_by_key.get((dataset["id"], "clean"))
        if not dirty and not clean:
            continue
        preview_sections.append(
            "<section class='preview'>"
            f"<h3>{esc(dataset['id'])}</h3>"
            "<div class='preview-grid'>"
            f"{preview_figure(dirty, 'Dirty image')}"
            f"{preview_figure(clean, 'Clean image')}"
            "</div>"
            "</section>"
        )

    parity_rows = []
    for item in parity:
        correctness = item.get("correctness", {})
        native = item.get("native_parallel", {})
        casa = item.get("casa", {})
        strict = correctness.get("strict_values") or {}
        parity_rows.append(
            "<tr>"
            f"<td><code>{esc(item.get('dataset'))}</code></td>"
            f"<td>{esc(correctness.get('status', 'missing'))}</td>"
            f"<td>{number(native.get('best_seconds'), 's')}</td>"
            f"<td>{number(casa.get('best_seconds'), 's')}</td>"
            f"<td>{number(item.get('speedup_vs_casa'), 'x')}</td>"
            f"<td>{number(strict.get('uvw', {}).get('max_abs'), 'm')}</td>"
            f"<td>{number(strict.get('data', {}).get('mean_abs'), 'Jy')}</td>"
            f"<td>{number(strict.get('data', {}).get('max_abs'), 'Jy')}</td>"
            f"<td>{esc('; '.join(correctness.get('reasons', [])) or 'none')}</td>"
            f"<td><code>{esc(item.get('_path'))}</code></td>"
            "</tr>"
        )

    io_native = internal_io.get("native_parallel", {})
    io_perf = internal_io.get("native_performance", {})
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ImPerformance Wave 1 Native simobserve Evidence</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #1f2933; }}
h1, h2, h3 {{ color: #101820; }}
table {{ border-collapse: collapse; width: 100%; margin: 16px 0 28px; font-size: 13px; }}
th, td {{ border: 1px solid #d7dde5; padding: 7px 8px; vertical-align: top; }}
th {{ background: #eef3f8; text-align: left; }}
code {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }}
.summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
.metric {{ border: 1px solid #d7dde5; padding: 12px; border-radius: 6px; background: #fbfcfe; }}
.metric strong {{ display: block; font-size: 20px; margin-top: 4px; }}
.preview-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 14px; }}
figure {{ margin: 0; border: 1px solid #d7dde5; padding: 8px; border-radius: 6px; background: white; }}
img {{ max-width: 100%; height: auto; display: block; }}
figcaption {{ margin-top: 6px; font-size: 12px; color: #52606d; }}
.note {{ background: #fff8e6; border-left: 4px solid #f0b429; padding: 10px 12px; }}
</style>
</head>
<body>
<h1>ImPerformance Wave 1 Native simobserve Evidence</h1>
<p>This report is generated from the Wave 1 dataset plan, native generation logs, CASA open/read smoke, parity runs, and native image preview products.</p>
<div class="summary">
<div class="metric">Datasets generated<strong>{len(generation.get('datasets', []))}</strong></div>
<div class="metric">Large dataset actual size<strong>{bytes_text(generation_by_id.get('wave1-alma-mosaic-large', {}).get('size_bytes'))}</strong></div>
<div class="metric">Internal write-path throughput<strong>{number(io_perf.get('data_io_mb_per_second'), 'MB/s')}</strong></div>
<div class="metric">Medium internal native throughput<strong>{number(io_perf.get('native_output_mb_per_second'), 'MB/s')}</strong></div>
</div>
<h2>Dataset Shapes, Sizes, and Generation Timing</h2>
<table>
<thead><tr><th>Dataset</th><th>Instrument</th><th>Family</th><th>Tier</th><th>Target</th><th>Actual</th><th>Elapsed</th><th>Rows</th><th>Channels</th><th>CASA open</th><th>Prediction</th><th>DATA write</th></tr></thead>
<tbody>{''.join(dataset_rows)}</tbody>
</table>
<h2>Representative Native Image Previews</h2>
<p class="note">Dirty and clean previews are shown for small representative datasets. A medium one-channel preview was intentionally stopped after 387 s with no products, which is recorded as a follow-up imaging read-path performance concern; the generated MS itself passed CASA open/read smoke.</p>
{''.join(preview_sections)}
<h2>CASA C++ Parity Runs</h2>
<table>
<thead><tr><th>Dataset</th><th>Status</th><th>Native</th><th>CASA</th><th>Speedup</th><th>UVW max</th><th>DATA mean abs</th><th>DATA max abs</th><th>Notes</th><th>JSON</th></tr></thead>
<tbody>{''.join(parity_rows)}</tbody>
</table>
<h2>Internal-Disk I/O Guard</h2>
<p>The medium VLA write-path guard ran with model prediction disabled on the internal disk. Native best time: {number(io_native.get('best_seconds'), 's')}; output size: {bytes_text(io_native.get('size_bytes'))}; streamed DATA/FLAG/UVW/WEIGHT/SIGMA bytes: {bytes_text(io_perf.get('data_io_bytes'))}; DATA writer time: {number(io_perf.get('data_io_write_millis'), 'ms')}.</p>
<h2>Evidence Files</h2>
<ul>
<li>Plan: <code>{esc(plan.get('data_root'))}</code></li>
<li>Generation summary: <code>{esc(generation.get('generated_at'))}</code></li>
<li>CASA smoke: <code>{esc(smoke.get('generated_at'))}</code></li>
</ul>
</body>
</html>
"""
    return html_doc


def preview_figure(run: dict[str, Any] | None, label: str) -> str:
    if not run:
        return f"<figure><figcaption>{esc(label)} not generated.</figcaption></figure>"
    prefix = pathlib.Path(run["prefix"])
    png = pathlib.Path(str(prefix) + ".image.png")
    if not png.exists():
        return (
            "<figure>"
            f"<figcaption>{esc(label)} missing preview PNG for <code>{esc(str(prefix))}</code>.</figcaption>"
            "</figure>"
        )
    data_uri = "data:image/png;base64," + base64.b64encode(png.read_bytes()).decode("ascii")
    return (
        "<figure>"
        f"<img src='{data_uri}' alt='{esc(label)} preview'>"
        f"<figcaption>{esc(label)}; elapsed {number(run.get('elapsed_seconds'), 's')}.</figcaption>"
        "</figure>"
    )


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def int_text(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{int(value):,}"


def number(value: Any, suffix: str) -> str:
    if value is None:
        return "n/a"
    try:
        number_value = float(value)
    except (TypeError, ValueError):
        return esc(value)
    if suffix == "x":
        return f"{number_value:.2f}x"
    if abs(number_value) >= 100:
        return f"{number_value:,.1f} {suffix}"
    return f"{number_value:.4g} {suffix}"


def bytes_text(value: Any) -> str:
    if value is None:
        return "n/a"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1000 or unit == "TB":
            return f"{size:.1f} {unit}"
        size /= 1000.0
    return f"{size:.1f} TB"


if __name__ == "__main__":
    main()
