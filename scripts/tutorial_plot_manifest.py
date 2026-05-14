#!/usr/bin/env python3
"""Helpers for tutorial plot manifest regression evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def manifest_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_manifest(path: Path) -> dict[str, Any]:
    headers: dict[str, str] = {}
    rows: list[dict[str, str]] = []
    columns: list[str] | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        if line.startswith("# "):
            body = line[2:]
            if "=" in body:
                key, value = body.split("=", 1)
                headers[key] = value
            continue
        if columns is None:
            columns = line.split("\t")
            continue
        values = line.split("\t")
        rows.append(dict(zip(columns, values, strict=False)))
    if columns is None:
        raise ValueError(f"{path} does not contain a tabular manifest")
    return {"path": str(path), "sha256": manifest_digest(path), "headers": headers, "columns": columns, "rows": rows}


def _as_float(row: dict[str, str], key: str) -> float | None:
    value = row.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _range(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "max": None}
    return {"min": min(values), "max": max(values)}


def summarize_manifest(path: Path) -> dict[str, Any]:
    manifest = parse_manifest(path)
    rows = manifest["rows"]
    x_values = [value for row in rows if (value := _as_float(row, "x")) is not None]
    y_values = [value for row in rows if (value := _as_float(row, "y")) is not None]
    by_panel: dict[str, int] = defaultdict(int)
    by_series: dict[str, int] = defaultdict(int)
    for row in rows:
        panel = row.get("panel_label") or row.get("panel_key")
        series = row.get("series_label") or row.get("series_key")
        if panel:
            by_panel[panel] += 1
        if series:
            by_series[series] += 1
    return {
        "path": str(path),
        "sha256": manifest["sha256"],
        "headers": manifest["headers"],
        "columns": manifest["columns"],
        "point_count": len(rows),
        "x_range": _range(x_values),
        "y_range": _range(y_values),
        "panel_counts": dict(sorted(by_panel.items())),
        "series_counts": dict(sorted(by_series.items())),
    }


def compare_manifests(reference: Path, candidate: Path) -> dict[str, Any]:
    reference_summary = summarize_manifest(reference)
    candidate_summary = summarize_manifest(candidate)
    reference_bytes = reference.read_bytes()
    candidate_bytes = candidate.read_bytes()
    return {
        "reference": reference_summary,
        "candidate": candidate_summary,
        "byte_identical": reference_bytes == candidate_bytes,
        "point_count_match": reference_summary["point_count"] == candidate_summary["point_count"],
        "headers_match": reference_summary["headers"] == candidate_summary["headers"],
        "columns_match": reference_summary["columns"] == candidate_summary["columns"],
        "ranges_match": reference_summary["x_range"] == candidate_summary["x_range"]
        and reference_summary["y_range"] == candidate_summary["y_range"],
        "panel_counts_match": reference_summary["panel_counts"] == candidate_summary["panel_counts"],
        "series_counts_match": reference_summary["series_counts"] == candidate_summary["series_counts"],
    }


def rust_manifest_points(path: Path, *, panel_label: str | None = None) -> list[tuple[float, float]]:
    manifest = parse_manifest(path)
    points: list[tuple[float, float]] = []
    for row in manifest["rows"]:
        if panel_label is not None and row.get("panel_label") != panel_label:
            continue
        x = _as_float(row, "x")
        y = _as_float(row, "y")
        if x is not None and y is not None:
            points.append((x, y))
    return points


def casa_plotms_txt_points(path: Path) -> list[tuple[float, float]]:
    points: list[tuple[float, float]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = stripped.split()
        if len(parts) < 2:
            continue
        try:
            points.append((float(parts[0]), float(parts[1])))
        except ValueError:
            continue
    return points


def compare_point_sets(
    reference_points: list[tuple[float, float]],
    candidate_points: list[tuple[float, float]],
    *,
    tolerance: float = 1.1e-3,
    relative_tolerance: float = 5.0e-4,
) -> dict[str, Any]:
    def close_enough(left: float, right: float) -> bool:
        diff = abs(left - right)
        scale = max(abs(left), abs(right), 1.0)
        return diff <= tolerance or diff / scale <= relative_tolerance

    def normalized(values: list[tuple[float, float]]) -> list[tuple[float, float]]:
        return sorted((round(x, 3), round(y, 3)) for x, y in values)

    reference = normalized(reference_points)
    candidate = normalized(candidate_points)
    point_count_match = len(reference) == len(candidate)
    pairwise_match = point_count_match and all(
        abs(left[0] - right[0]) <= tolerance and abs(left[1] - right[1]) <= tolerance
        for left, right in zip(reference, candidate, strict=False)
    )
    sorted_x_match = False
    sorted_y_match = False
    max_x_abs_diff: float | None = None
    max_y_abs_diff: float | None = None
    max_x_rel_diff: float | None = None
    max_y_rel_diff: float | None = None
    if point_count_match:
        reference_x = sorted(x for x, _ in reference)
        candidate_x = sorted(x for x, _ in candidate)
        reference_y = sorted(y for _, y in reference)
        candidate_y = sorted(y for _, y in candidate)
        x_diffs = [abs(left - right) for left, right in zip(reference_x, candidate_x, strict=False)]
        y_diffs = [abs(left - right) for left, right in zip(reference_y, candidate_y, strict=False)]
        x_rel_diffs = [
            diff / max(abs(left), abs(right), 1.0)
            for diff, left, right in zip(x_diffs, reference_x, candidate_x, strict=False)
        ]
        y_rel_diffs = [
            diff / max(abs(left), abs(right), 1.0)
            for diff, left, right in zip(y_diffs, reference_y, candidate_y, strict=False)
        ]
        max_x_abs_diff = max(x_diffs, default=0.0)
        max_y_abs_diff = max(y_diffs, default=0.0)
        max_x_rel_diff = max(x_rel_diffs, default=0.0)
        max_y_rel_diff = max(y_rel_diffs, default=0.0)
        sorted_x_match = all(close_enough(left, right) for left, right in zip(reference_x, candidate_x, strict=False))
        sorted_y_match = all(close_enough(left, right) for left, right in zip(reference_y, candidate_y, strict=False))
    return {
        "status": "passed" if point_count_match and (pairwise_match or (sorted_x_match and sorted_y_match)) else "failed",
        "point_count_match": point_count_match,
        "reference_point_count": len(reference),
        "candidate_point_count": len(candidate),
        "pairwise_match": pairwise_match,
        "sorted_x_match": sorted_x_match,
        "sorted_y_match": sorted_y_match,
        "max_x_abs_diff": max_x_abs_diff,
        "max_y_abs_diff": max_y_abs_diff,
        "max_x_rel_diff": max_x_rel_diff,
        "max_y_rel_diff": max_y_rel_diff,
        "tolerance": tolerance,
        "relative_tolerance": relative_tolerance,
    }


def compare_manifest_to_casa_txt(reference: Path, casa_txt: Path) -> dict[str, Any]:
    comparison = compare_point_sets(rust_manifest_points(reference), casa_plotms_txt_points(casa_txt))
    comparison["reference"] = summarize_manifest(reference)
    comparison["candidate"] = {
        "path": str(casa_txt),
        "sha256": manifest_digest(casa_txt),
        "point_count": len(casa_plotms_txt_points(casa_txt)),
    }
    return comparison


def compare_iterated_manifest_to_casa_txts(
    reference: Path,
    casa_txt_by_panel: dict[str, Path],
) -> dict[str, Any]:
    panels: dict[str, Any] = {}
    status = "passed"
    for panel_label, casa_txt in casa_txt_by_panel.items():
        comparison = compare_point_sets(
            rust_manifest_points(reference, panel_label=panel_label),
            casa_plotms_txt_points(casa_txt),
        )
        comparison["casa_txt"] = str(casa_txt)
        panels[panel_label] = comparison
        if comparison["status"] != "passed":
            status = "failed"
    return {
        "status": status,
        "reference": summarize_manifest(reference),
        "panels": panels,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reference", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    comparison = compare_manifests(args.reference, args.candidate)
    encoded = json.dumps(comparison, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    else:
        print(encoded, end="")
    return 0 if comparison["byte_identical"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
