"""Compare typed msexplore point manifests with CASA plotms text exports."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..model import RuntimeResources, SectionManifest

def _native_points(path: Path, panel_label: str | None = None) -> list[tuple[float, float]]:
    columns: list[str] | None = None
    points = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        if columns is None:
            columns = line.split("\t")
            continue
        row = dict(zip(columns, line.split("\t"), strict=False))
        if panel_label is not None and row.get("panel_label") != panel_label:
            continue
        try:
            points.append((float(row["x"]), float(row["y"])))
        except (KeyError, ValueError):
            continue
    if columns is None:
        raise ValueError(f"{path} is not an msexplore point manifest")
    return points


def _casa_points(path: Path) -> list[tuple[float, float]]:
    points = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        values = line.split()
        try:
            points.append((float(values[0]), float(values[1])))
        except (IndexError, ValueError):
            continue
    return points


def _compare_points(
    native: list[tuple[float, float]],
    oracle: list[tuple[float, float]],
    *,
    atol: float,
    rtol: float,
) -> dict[str, Any]:
    def close(left: float, right: float) -> bool:
        difference = abs(left - right)
        return difference <= atol + rtol * max(abs(left), abs(right), 1.0)

    native_sorted = sorted(native)
    oracle_sorted = sorted(oracle)
    count_match = len(native_sorted) == len(oracle_sorted)
    pairwise = count_match and all(
        close(left_x, right_x) and close(left_y, right_y)
        for (left_x, left_y), (right_x, right_y) in zip(native_sorted, oracle_sorted, strict=False)
    )
    x_differences = []
    y_differences = []
    if count_match:
        native_x = sorted(point[0] for point in native_sorted)
        oracle_x = sorted(point[0] for point in oracle_sorted)
        native_y = sorted(point[1] for point in native_sorted)
        oracle_y = sorted(point[1] for point in oracle_sorted)
        x_differences = [abs(left - right) for left, right in zip(native_x, oracle_x, strict=False)]
        y_differences = [abs(left - right) for left, right in zip(native_y, oracle_y, strict=False)]
        distributions = all(close(left, right) for left, right in zip(native_x, oracle_x, strict=False)) and all(
            close(left, right) for left, right in zip(native_y, oracle_y, strict=False)
        )
    else:
        distributions = False
    passed = count_match and (pairwise or distributions)
    return {
        "status": "passed" if passed else "failed",
        "native_point_count": len(native),
        "oracle_point_count": len(oracle),
        "point_count_match": count_match,
        "pairwise_match": pairwise,
        "distribution_match": distributions,
        "max_x_abs_diff": max(x_differences, default=None),
        "max_y_abs_diff": max(y_differences, default=None),
        "absolute_tolerance": atol,
        "relative_tolerance": rtol,
    }


def compare(manifest: SectionManifest, resources: RuntimeResources) -> dict[str, Any]:
    inputs = manifest.comparison.inputs
    native_path = resources.pack_root / inputs["native"]
    oracle_values = inputs["oracle"] if isinstance(inputs["oracle"], list) else [inputs["oracle"]]
    oracle_paths = [resources.pack_root / value for value in oracle_values]
    pngs = [resources.pack_root / value for value in inputs.get("pngs", [])]
    required = [native_path, *oracle_paths, *(pngs if manifest.comparison.config.get("require_png") else [])]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        return {"status": "unavailable", "plugin": "plot_products", "reason": f"missing inputs: {', '.join(missing)}"}
    config = manifest.comparison.config
    labels = config.get("panel_labels", [])
    if len(oracle_paths) > 1 and len(labels) != len(oracle_paths):
        return {"status": "failed", "plugin": "plot_products", "reason": "panel_labels must match oracle inputs"}
    comparisons = []
    for index, oracle_path in enumerate(oracle_paths):
        label = labels[index] if labels else None
        comparison = _compare_points(
            _native_points(native_path, panel_label=label),
            _casa_points(oracle_path),
            atol=float(config.get("absolute_tolerance", 0.0)),
            rtol=float(config.get("relative_tolerance", 0.0)),
        )
        comparison.update({"panel_label": label, "oracle": str(oracle_path)})
        comparisons.append(comparison)
    status = "passed" if comparisons and all(item["status"] == "passed" for item in comparisons) else "failed"
    return {
        "status": status,
        "plugin": "plot_products",
        "native": str(native_path),
        "comparisons": comparisons,
        "rendered_products": [str(path) for path in pngs],
    }
