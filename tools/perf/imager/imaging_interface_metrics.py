#!/usr/bin/env python3
"""Measure imaging interface and SLOC deltas for the #319 wave."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from subprocess import CalledProcessError
from typing import Iterable

from perf_harness.subprocesses import run_command


DEFAULT_PATHS = [
    "crates/casa-imaging/src",
    "crates/casars-imager/src",
    "crates/casa-ms/src/visibility_buffer.rs",
    "crates/casa-ms/src/spectral_selection.rs",
    "crates/casa-ms/src/selection.rs",
    "crates/casa-ms/src/selection_syntax.rs",
]

PUBLIC_RE = re.compile(r"^\s*pub\s+(fn|struct|enum|trait|type|const|static|mod|use)\b")
PUBLIC_FIELD_RE = re.compile(r"^\s*pub\s+[A-Za-z_][A-Za-z0-9_]*\s*:")
PUB_CRATE_RE = re.compile(
    r"^\s*pub\(crate\)\s+(fn|struct|enum|trait|type|const|static|mod|use)\b"
)
PRIVATE_ITEM_RE = re.compile(r"^\s*(fn|struct|enum|trait|type|const|static|mod)\s+")
APP_PRIVATE_FN_RE = re.compile(r"^fn\s+[A-Za-z0-9_]+\b")
PUBLIC_NAME_RE = re.compile(
    r"^\s*pub\s+(?:fn|struct|enum|trait|type|const|static|mod)\s+([A-Za-z0-9_]+)\b"
)

CANONICAL_STANDARD_MFS_RUNNERS = {
    "run_standard_mfs_plan",
    "run_standard_mfs_dirty_plan",
}

STANDARD_MFS_FACADE_NAMES = {
    "StandardMfsPlan",
    "StandardMfsDirtyPlan",
    "StandardMfsCleanPlan",
    "StandardMfsCleanFinishPlan",
    "StandardMfsCleanSession",
    "StandardMfsCleanControlStats",
    "StandardMfsAccelerationPolicy",
    "StandardMfsDirtyAccumulator",
    "StandardMfsDirtyAccumulatorRequest",
    "StandardMfsExecutionConfig",
    "StandardMfsMetalFixtureExportOptions",
    "StandardMfsMetalFixtureExportSummary",
    "StandardMfsMinorCycleBackend",
    "StandardMfsModelPredictor",
    "StandardMfsPairCollapseTransform",
    "StandardMfsPlannedSampleBlock",
    "StandardMfsPlannedSampleBuilder",
    "StandardMfsRoutedGridSample",
    "StandardMfsRoutedInputCache",
    "StandardMfsRoutedInputCachePrefill",
    "StandardMfsRoutedVisibilityAppendCounts",
    "StandardMfsRoutedVisibilityBlock",
    "StandardMfsStreamingWeightingPlan",
    "StandardMfsVisibilityPolarization",
    "StandardMfsVisibilityRow",
}

APP_HELPERS = [
    "select_main_rows",
    "read_visibility_source_columns",
    "read_columnar_prepared_source",
    "prepare_source_row_block_plane_inner",
    "collapse_paired_visibility",
    "collapse_paired_visibility_batch",
    "derive_stokes_pair_selection",
    "reported_sumwt_factor_for_paired_plane",
    "interpolate_explicit_cube_output_sample",
    "accumulate_standard_mfs_density_rows_without_data_parallel",
    "accumulate_standard_mfs_density_essentials_rows",
    "accumulate_standard_mfs_density_row",
    "run_joint_outlier_hogbom",
    "run_joint_hogbom_minor_cycle",
    "write_products",
    "build_coordinate_system",
    "robust_plane_stats",
    "build_auto_multithresh_cube_clean_mask",
]

APP_LEGACY_VISIBILITY_SOURCE_BLOCK_TYPES = [
    "VisibilitySourceBlock",
    "VisibilitySourceBlockSidecars",
    "VisibilitySourceRowColumns",
]

APP_LEGACY_GET_MS_VALUE_HELPERS = [
    "get_ms_values_into_processing_buffer",
    "get_ms_values_into_density_processing_buffer",
]

APP_STANDARD_MFS_DENSITY_ROW_HELPERS = [
    "standard_mfs_density_can_skip_data",
    "accumulate_standard_mfs_density_essentials_row_with_frequency_scale",
    "accumulate_standard_mfs_density_row_without_data",
    "accumulate_standard_mfs_density_row",
]

APP_PRODUCT_PLANE_HELPERS = [
    "extract_mfs_plane",
    "expand_joint_plane",
    "expand_plane_for_write",
    "count_clean_mask_pixels",
    "frontend_peak_abs_masked",
]

APP_CUBE_BRIGGS_FORMULA_HELPERS = [
    "casa_cube_briggs_f2",
    "casa_cube_briggs_density_cell",
    "casa_cube_briggs_weight_denominator",
    "casa_cube_briggs_bw_taper_uv_distance_factor",
    "casa_cube_briggs_gridft_density_cell_from_lambda",
    "casa_cube_briggs_density_cell_from_lambda",
    "casa_cube_briggs_streaming_f2",
]


@dataclass
class FileMetrics:
    path: str
    lines: int = 0
    rust_code_lines: int = 0
    public_items: int = 0
    public_fields: int = 0
    pub_crate_items: int = 0
    private_items: int = 0
    app_private_fns: int = 0


def run_git(args: list[str]) -> str:
    result = run_command(
        ["git", *args],
        merge_stderr=False,
        check=True,
    )
    return result.stdout


def expand_paths(paths: Iterable[str], rev: str | None) -> list[str]:
    files: set[str] = set()
    for raw_path in paths:
        path = raw_path.rstrip("/")
        if rev:
            try:
                entries = run_git(["ls-tree", "-r", "--name-only", rev, "--", path])
            except CalledProcessError:
                continue
            for entry in entries.splitlines():
                if entry.endswith(".rs") or entry.endswith(".html") or entry.endswith(".md"):
                    files.add(entry)
            continue

        local = Path(path)
        if local.is_dir():
            for child in local.rglob("*"):
                if child.suffix in {".rs", ".html", ".md"} and child.is_file():
                    files.add(child.as_posix())
        elif local.is_file():
            files.add(local.as_posix())
    return sorted(files)


def read_text(path: str, rev: str | None) -> str | None:
    if rev:
        try:
            return run_git(["show", f"{rev}:{path}"])
        except CalledProcessError:
            return None
    try:
        return Path(path).read_text(encoding="utf-8")
    except FileNotFoundError:
        return None


def is_rust_code_line(line: str, in_block_comment: bool) -> tuple[bool, bool]:
    stripped = line.strip()
    if not stripped:
        return False, in_block_comment

    if in_block_comment:
        if "*/" in stripped:
            stripped = stripped.split("*/", 1)[1].strip()
            in_block_comment = False
        else:
            return False, True

    while stripped.startswith("/*"):
        if "*/" not in stripped:
            return False, True
        stripped = stripped.split("*/", 1)[1].strip()

    if not stripped or stripped.startswith("//"):
        return False, in_block_comment
    return True, in_block_comment


def measure_file(path: str, text: str) -> FileMetrics:
    metrics = FileMetrics(path=path)
    in_block_comment = False
    for line in text.splitlines():
        metrics.lines += 1
        code_line, in_block_comment = is_rust_code_line(line, in_block_comment)
        if code_line:
            metrics.rust_code_lines += 1
        if PUBLIC_RE.search(line):
            metrics.public_items += 1
        if PUBLIC_FIELD_RE.search(line):
            metrics.public_fields += 1
        if PUB_CRATE_RE.search(line):
            metrics.pub_crate_items += 1
        if PRIVATE_ITEM_RE.search(line) and not PUBLIC_RE.search(line) and not PUB_CRATE_RE.search(line):
            metrics.private_items += 1
        if path == "crates/casars-imager/src/lib.rs" and APP_PRIVATE_FN_RE.search(line):
            metrics.app_private_fns += 1
    return metrics


def collect_named_public_items(files: dict[str, str]) -> list[str]:
    names: list[str] = []
    for text in files.values():
        for line in text.splitlines():
            match = PUBLIC_NAME_RE.search(line)
            if match:
                names.append(match.group(1))
    return sorted(names)


def collect_app_helper_presence(files: dict[str, str]) -> dict[str, bool]:
    text = files.get("crates/casars-imager/src/lib.rs", "")
    return {
        helper: bool(re.search(rf"^fn\s+{re.escape(helper)}\b", text, re.MULTILINE))
        for helper in APP_HELPERS
    }


def collect_app_fn_or_method_presence(
    files: dict[str, str], names: Iterable[str]
) -> dict[str, bool]:
    text = files.get("crates/casars-imager/src/lib.rs", "")
    return {
        name: bool(re.search(rf"^\s*fn\s+{re.escape(name)}\b", text, re.MULTILINE))
        for name in names
    }


def count_app_private_names(files: dict[str, str], names: Iterable[str]) -> int:
    text = files.get("crates/casars-imager/src/lib.rs", "")
    return sum(
        1
        for name in names
        if re.search(rf"^(?:fn|struct)\s+{re.escape(name)}\b", text, re.MULTILINE)
    )


def count_app_fn_or_method_names(files: dict[str, str], names: Iterable[str]) -> int:
    text = files.get("crates/casars-imager/src/lib.rs", "")
    return sum(
        1
        for name in names
        if re.search(rf"^\s*fn\s+{re.escape(name)}\b", text, re.MULTILINE)
    )


def count_public_fields_in_struct(files: dict[str, str], struct_name: str) -> int:
    start_re = re.compile(rf"^\s*pub\s+struct\s+{re.escape(struct_name)}\s*\{{")
    count = 0
    for text in files.values():
        in_struct = False
        for line in text.splitlines():
            if not in_struct:
                in_struct = bool(start_re.search(line))
                continue
            if line.lstrip().startswith("}"):
                in_struct = False
                continue
            if PUBLIC_FIELD_RE.search(line):
                count += 1
    return count


def collect_revision(paths: list[str], rev: str | None) -> dict[str, object]:
    expanded = expand_paths(paths, rev)
    file_texts: dict[str, str] = {}
    per_file: list[FileMetrics] = []
    for path in expanded:
        text = read_text(path, rev)
        if text is None:
            continue
        file_texts[path] = text
        per_file.append(measure_file(path, text))

    public_names = collect_named_public_items(file_texts)
    public_standard_mfs = [name for name in public_names if name.startswith("StandardMfs")]
    backend_standard_mfs = [
        name for name in public_standard_mfs if name not in STANDARD_MFS_FACADE_NAMES
    ]
    public_route_runners = [
        name
        for name in public_names
        if name.startswith("run_standard_mfs_")
        and name not in CANONICAL_STANDARD_MFS_RUNNERS
    ]

    totals = {
        "file_count": len(per_file),
        "lines": sum(item.lines for item in per_file),
        "rust_code_lines": sum(item.rust_code_lines for item in per_file),
        "public_items": sum(item.public_items for item in per_file),
        "public_fields": sum(item.public_fields for item in per_file),
        "pub_crate_items": sum(item.pub_crate_items for item in per_file),
        "private_items": sum(item.private_items for item in per_file),
        "casars_imager_private_fns": sum(item.app_private_fns for item in per_file),
        "app_legacy_get_ms_value_helpers": count_app_private_names(
            file_texts,
            APP_LEGACY_GET_MS_VALUE_HELPERS,
        ),
        "app_legacy_visibility_source_block_types": count_app_private_names(
            file_texts,
            APP_LEGACY_VISIBILITY_SOURCE_BLOCK_TYPES,
        ),
        "app_standard_mfs_density_row_helpers": count_app_fn_or_method_names(
            file_texts,
            APP_STANDARD_MFS_DENSITY_ROW_HELPERS,
        ),
        "app_product_plane_helpers": count_app_fn_or_method_names(
            file_texts,
            APP_PRODUCT_PLANE_HELPERS,
        ),
        "app_cube_briggs_formula_helpers": count_app_fn_or_method_names(
            file_texts,
            APP_CUBE_BRIGGS_FORMULA_HELPERS,
        ),
        "public_route_specific_standard_mfs_runners": len(public_route_runners),
        "public_standard_mfs_items": len(public_standard_mfs),
        "public_standard_mfs_backend_items": len(backend_standard_mfs),
        "public_standard_mfs_planned_weighted_sample_fields": count_public_fields_in_struct(
            file_texts,
            "StandardMfsPlannedWeightedSample",
        ),
    }
    return {
        "rev": rev or "WORKTREE",
        "paths": expanded,
        "totals": totals,
        "public_route_specific_standard_mfs_runners": public_route_runners,
        "public_standard_mfs_backend_items": backend_standard_mfs,
        "app_helper_presence": collect_app_helper_presence(file_texts),
        "app_standard_mfs_density_row_helper_presence": collect_app_fn_or_method_presence(
            file_texts,
            APP_STANDARD_MFS_DENSITY_ROW_HELPERS,
        ),
        "app_product_plane_helper_presence": collect_app_fn_or_method_presence(
            file_texts,
            APP_PRODUCT_PLANE_HELPERS,
        ),
        "app_cube_briggs_formula_helper_presence": collect_app_fn_or_method_presence(
            file_texts,
            APP_CUBE_BRIGGS_FORMULA_HELPERS,
        ),
        "per_file": [item.__dict__ for item in per_file],
    }


def delta(base: dict[str, object], head: dict[str, object]) -> dict[str, int]:
    base_totals = base["totals"]
    head_totals = head["totals"]
    assert isinstance(base_totals, dict)
    assert isinstance(head_totals, dict)
    keys = sorted(set(base_totals) | set(head_totals))
    return {
        key: int(head_totals.get(key, 0)) - int(base_totals.get(key, 0))
        for key in keys
    }


def print_markdown(base: dict[str, object] | None, head: dict[str, object]) -> None:
    if base is None:
        print(json.dumps(head, indent=2, sort_keys=True))
        return
    deltas = delta(base, head)
    base_totals = base["totals"]
    head_totals = head["totals"]
    assert isinstance(base_totals, dict)
    assert isinstance(head_totals, dict)
    print("| Metric | Base | Head | Delta |")
    print("| --- | ---: | ---: | ---: |")
    for key in sorted(deltas):
        print(
            f"| {key} | {base_totals.get(key, 0)} | "
            f"{head_totals.get(key, 0)} | {deltas[key]:+d} |"
        )
    print()
    print("Public route-specific standard-MFS runners:")
    print(f"- Base: {base['public_route_specific_standard_mfs_runners']}")
    print(f"- Head: {head['public_route_specific_standard_mfs_runners']}")
    print()
    print("Public StandardMfs backend items:")
    print(f"- Base: {base['public_standard_mfs_backend_items']}")
    print(f"- Head: {head['public_standard_mfs_backend_items']}")
    print()
    print("App helper presence:")
    base_helpers = base["app_helper_presence"]
    head_helpers = head["app_helper_presence"]
    assert isinstance(base_helpers, dict)
    assert isinstance(head_helpers, dict)
    for helper in APP_HELPERS:
        print(f"- {helper}: {base_helpers.get(helper)} -> {head_helpers.get(helper)}")
    print()
    print("App standard-MFS density row helper presence:")
    base_density_helpers = base["app_standard_mfs_density_row_helper_presence"]
    head_density_helpers = head["app_standard_mfs_density_row_helper_presence"]
    assert isinstance(base_density_helpers, dict)
    assert isinstance(head_density_helpers, dict)
    for helper in APP_STANDARD_MFS_DENSITY_ROW_HELPERS:
        print(
            f"- {helper}: {base_density_helpers.get(helper)} -> "
            f"{head_density_helpers.get(helper)}"
        )
    print()
    print("App product-plane helper presence:")
    base_product_helpers = base["app_product_plane_helper_presence"]
    head_product_helpers = head["app_product_plane_helper_presence"]
    assert isinstance(base_product_helpers, dict)
    assert isinstance(head_product_helpers, dict)
    for helper in APP_PRODUCT_PLANE_HELPERS:
        print(
            f"- {helper}: {base_product_helpers.get(helper)} -> "
            f"{head_product_helpers.get(helper)}"
        )
    print()
    print("App cube-Briggs formula helper presence:")
    base_cube_briggs_helpers = base["app_cube_briggs_formula_helper_presence"]
    head_cube_briggs_helpers = head["app_cube_briggs_formula_helper_presence"]
    assert isinstance(base_cube_briggs_helpers, dict)
    assert isinstance(head_cube_briggs_helpers, dict)
    for helper in APP_CUBE_BRIGGS_FORMULA_HELPERS:
        print(
            f"- {helper}: {base_cube_briggs_helpers.get(helper)} -> "
            f"{head_cube_briggs_helpers.get(helper)}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", help="Base git revision, for example origin/main")
    parser.add_argument("--head", help="Head git revision. Defaults to worktree.")
    parser.add_argument(
        "--path",
        action="append",
        dest="paths",
        help="Path or directory to include. May be repeated.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="markdown",
        help="Output format.",
    )
    args = parser.parse_args()

    paths = args.paths or DEFAULT_PATHS
    base = collect_revision(paths, args.base) if args.base else None
    head = collect_revision(paths, args.head)
    if args.format == "json":
        payload = {"head": head}
        if base is not None:
            payload["base"] = base
            payload["delta"] = delta(base, head)
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print_markdown(base, head)
    return 0


if __name__ == "__main__":
    sys.exit(main())
