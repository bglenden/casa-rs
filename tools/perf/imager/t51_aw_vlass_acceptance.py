#!/usr/bin/env python3
"""Run and validate T51 paired-AW acceptance against frozen VLASS products."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pathlib
import subprocess
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[3]
RUNNER = ROOT / "tools/perf/imager/run_workload.py"
DIRTY_WORKLOAD = ROOT / (
    "tools/perf/imager/workloads/"
    "vlass-fragment-all-fields-dirty-4096-full-16-spw-casa.json"
)
CLEAN_WORKLOAD = ROOT / (
    "tools/perf/imager/workloads/"
    "vlass-fragment-all-fields-clean-4096-full-16-spw-casa.json"
)
MEMORY_CEILING_BYTES = 32 * 1024**3
MINIMUM_SELECTED_SAMPLES = 1_000_000
MAXIMUM_NORMALIZED_RMS = 1.0e-3
EXPECTED_PREPARED_AW_CELLS = 1024
EXPECTED_PREPARED_AW_FREQUENCIES = 16
PREPARED_MANIFEST_BYTES_PER_CELL = 16 * 1024
AW_RESIDENT_MB = 384
AW_RESIDENT_BYTES = AW_RESIDENT_MB * 1024**2
SERIAL_CPU_IMAGING = {
    "parallel": False,
    "standard_mfs_acceleration": "cpu",
    "imaging_fft_precision": "auto",
    "imaging_fft_backend": "rustfft",
}
FORBIDDEN_RUNTIME_OVERRIDES = (
    "chanchunks",
    "standard_mfs_backend",
    "standard_mfs_grid_threads",
    "standard_mfs_tile_anchor",
    "standard_mfs_residual_backend",
    "standard_mfs_initial_dirty_backend",
    "standard_mfs_metal_minor_cycle_chunk",
    "standard_mfs_metal_grouped_input_cache",
    "standard_mfs_memory_target_mb",
    "standard_mfs_prepare_buffer_mb",
    "imaging_memory_target_mb",
    "imaging_memory_pressure_policy",
    "imaging_prepare_buffer_mb",
    "imaging_row_block_rows",
    "imaging_prepare_workers",
    "imaging_read_ahead_blocks",
)
CLI_PREFLIGHT_MARKER = "rust_cli_preflight=validated-before-measurement-set-open"


class GateError(RuntimeError):
    """A required T51 acceptance fact is absent or failed."""


def _object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GateError(f"{label} is missing")
    return value


def _positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise GateError(f"{label} must be a positive measured integer")
    return value


def _nonnegative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GateError(f"{label} must be a non-negative measured integer")
    return value


def _log_fields(value: Any, label: str) -> dict[str, Any]:
    return _object(_object(value, label).get("fields"), f"{label}.fields")


def validate_prepared_aw_receipts(
    results: dict[str, Any], *, workload_id: str
) -> dict[str, Any]:
    """Validate exact full-cache inventory and bounded lazy-reader evidence."""

    logs = _object(results.get("backend_plan_logs"), "results.backend_plan_logs")
    inventories = logs.get("aw_cache_inventory")
    if not isinstance(inventories, list) or len(inventories) != 1:
        raise GateError(f"{workload_id}: expected one AW cache inventory receipt")
    inventory = _log_fields(inventories[0], "AW cache inventory receipt")
    for name, expected in (
        ("paired_cells", EXPECTED_PREPARED_AW_CELLS),
        ("frequencies", EXPECTED_PREPARED_AW_FREQUENCIES),
        ("decoded_resident_ceiling_bytes", AW_RESIDENT_BYTES),
    ):
        if inventory.get(name) != expected:
            raise GateError(f"{workload_id}: AW inventory {name} must be {expected}")
    for name in (
        "w_values",
        "mueller_elements",
        "parallactic_angles",
        "prepared_cache_bytes",
    ):
        _positive_int(inventory.get(name), f"AW inventory {name}")

    entries = logs.get("prepared_artifact_readers")
    if not isinstance(entries, list) or not entries:
        raise GateError(f"{workload_id}: lazy AW reader receipts are absent")
    readers = [
        _log_fields(entry, f"AW reader receipt {ordinal}")
        for ordinal, entry in enumerate(entries)
    ]
    catalogs: set[str] = set()
    logical_sizes: set[int] = set()
    for ordinal, reader in enumerate(readers):
        label = f"{workload_id}: AW reader receipt {ordinal}"
        catalog = reader.get("catalog")
        if (
            not isinstance(catalog, str)
            or len(catalog) != 64
            or any(character not in "0123456789abcdef" for character in catalog)
        ):
            raise GateError(f"{label} has an invalid catalog identity")
        catalogs.add(catalog)
        logical_sizes.add(
            _positive_int(reader.get("logical_bytes"), f"{label} logical_bytes")
        )
        if reader.get("decoded_ceiling_bytes") != AW_RESIDENT_BYTES:
            raise GateError(f"{label} changed the 384 MiB decoded ceiling")
        total_ceiling = _positive_int(
            reader.get("total_ceiling_bytes"), f"{label} total_ceiling_bytes"
        )
        resident_peak = _positive_int(
            reader.get("resident_peak_bytes"), f"{label} resident_peak_bytes"
        )
        total_peak = _positive_int(
            reader.get("total_peak_resident_bytes"),
            f"{label} total_peak_resident_bytes",
        )
        pinned_peak = _positive_int(
            reader.get("pinned_peak_bytes"), f"{label} pinned_peak_bytes"
        )
        reads = _positive_int(reader.get("reads"), f"{label} reads")
        loads = _positive_int(reader.get("loads"), f"{label} loads")
        _positive_int(reader.get("read_bytes"), f"{label} read_bytes")
        _positive_int(reader.get("read_operations"), f"{label} read_operations")
        _positive_int(reader.get("copied_bytes"), f"{label} copied_bytes")
        _nonnegative_int(reader.get("hits"), f"{label} hits")
        _nonnegative_int(reader.get("evicted_bytes"), f"{label} evicted_bytes")
        if reader.get("aborted") is not False:
            raise GateError(f"{label} was aborted")
        if loads != reads:
            raise GateError(f"{label} load/read counts differ")
        if not pinned_peak <= resident_peak <= AW_RESIDENT_BYTES:
            raise GateError(f"{label} exceeded the decoded residency ceiling")
        if not resident_peak <= total_peak <= total_ceiling:
            raise GateError(f"{label} exceeded its total residency ceiling")
        if total_ceiling < AW_RESIDENT_BYTES:
            raise GateError(f"{label} total ceiling omits decoded residency")
        if total_ceiling > AW_RESIDENT_BYTES + 8 * 1024**2:
            raise GateError(f"{label} total ceiling exceeds the T50 streaming bound")
    if len(catalogs) != 1 or len(logical_sizes) != 1:
        raise GateError(
            f"{workload_id}: reader sessions changed catalog identity or size"
        )
    logical_size = next(iter(logical_sizes))
    expected_cache_bytes = logical_size + (
        EXPECTED_PREPARED_AW_CELLS * PREPARED_MANIFEST_BYTES_PER_CELL
    )
    if inventory["prepared_cache_bytes"] != expected_cache_bytes:
        raise GateError(
            f"{workload_id}: prepared-cache and reader logical sizes disagree"
        )
    return {
        "inventory": inventory,
        "reader_sessions": readers,
    }


def validate_manifest_contract(workload: dict[str, Any]) -> dict[str, Any]:
    """Validate the frozen T51 science surface and its current serial runtime seam."""

    workload_id = workload.get("id")
    imaging = _object(workload.get("imaging"), "manifest imaging")
    required_imaging = {
        "specmode": "mfs",
        "gridder": "awproject",
        "casa_gridder": "awproject",
        "wterm": "wproject",
        "wprojplanes": 32,
        "field": "1107~1127,1512~1532,1542~1562",
        "phasecenter_field": 1525,
        "spw": "2~17",
        "channel_start": 0,
        "channel_count": 64,
        "imsize": 4096,
        "cell_arcsec": 0.6,
        "datacolumn": "data",
        "stokes": "I",
        "projection": "SIN",
        "interpolation": "linear",
        "uvrange": "<12km",
        "intent": "OBSERVE_TARGET#UNSPECIFIED",
        "weighting": "briggs",
        "robust": 1.0,
        "perchanweightdensity": True,
        "deconvolver": "mtmfs",
        "nterms": 2,
        "scales": [0, 5, 12],
        "smallscalebias": 0.0,
        "gain": 0.1,
        "threshold_jy": 0.0,
        "nsigma": 5.0,
        "minor_cycle_length": 2000,
        "cyclefactor": 3.0,
        "min_psf_fraction": 0.05,
        "max_psf_fraction": 0.8,
        "facets": 1,
        "psfphasecenter": "",
        "vptable": "",
        "mosweight": False,
        "aterm": True,
        "psterm": False,
        "wbawp": True,
        "conjbeams": True,
        "usepointing": True,
        "computepastep": 360.0,
        "rotatepastep": 360.0,
        "pointingoffsetsigdev": 0.0,
        "pblimit": 0.0001,
        "normtype": "flatnoise",
        "write_pb": True,
        "pbcor": False,
        "restoration": True,
        "restoringbeam": "common",
        "interactive": False,
        "usemask": "user",
        "restart": False,
        "savemodel": "none",
        "calcres": True,
        "calcpsf": True,
        **SERIAL_CPU_IMAGING,
    }
    expected_mode = "dirty" if "dirty" in str(workload_id) else "clean"
    required_imaging.update(
        {
            "mode": expected_mode,
            "niter": 0 if expected_mode == "dirty" else 2000,
        }
    )
    for name, expected in required_imaging.items():
        if imaging.get(name) != expected:
            raise GateError(
                f"{workload_id}: manifest imaging.{name} must be {expected!r}"
            )
    for name in FORBIDDEN_RUNTIME_OVERRIDES:
        if name in imaging:
            raise GateError(
                f"{workload_id}: manifest imaging.{name} bypasses the current "
                "serial Resource Authority contract"
            )
    if expected_mode == "dirty":
        if "mask_image" in imaging or "mask_sha256" in imaging:
            raise GateError(f"{workload_id}: dirty imaging must not bind a clean mask")
    else:
        expected_mask = (
            "/Volumes/GLENDENNING/casa-rs-vlass/issue-446/masks/"
            "vlass-source-box-4096-spectral.mask"
        )
        if imaging.get("mask_image") != expected_mask:
            raise GateError(f"{workload_id}: clean imaging.mask_image differs")
        if imaging.get("mask_sha256") != (
            "8490acb911cbbba78f7a20ba4a1d379e227c3a42dfc7eefcc9b7fd5f4139572f"
        ):
            raise GateError(f"{workload_id}: clean imaging.mask_sha256 differs")
    run = _object(workload.get("run"), "manifest run")
    required_run = {
        "repeats": 1,
        "warmups": 0,
        "ms_staging": "direct",
        "skip_profile": "1",
        "cf_cache_role": "cold" if expected_mode == "dirty" else "warm",
    }
    for name, expected in required_run.items():
        if run.get(name) != expected:
            raise GateError(f"{workload_id}: manifest run.{name} must be {expected!r}")
    return imaging


def validate_receipt(
    receipt: dict[str, Any],
    *,
    expected_workload: dict[str, Any],
    expected_casa_prefix: pathlib.Path,
    expected_prepared_aw_casa_cache: pathlib.Path | None = None,
    expected_rust_prefix: pathlib.Path | None = None,
) -> dict[str, Any]:
    """Fail closed unless one production run satisfies the T51 contract."""

    workload_id = expected_workload["id"]
    if receipt.get("status") != "completed" or receipt.get("exit_code") != 0:
        raise GateError(f"{workload_id}: workload did not complete successfully")
    if _object(receipt.get("workload"), "workload").get("id") != workload_id:
        raise GateError(f"{workload_id}: receipt names another workload")

    imaging = validate_manifest_contract(expected_workload)

    mode = _object(receipt.get("mode"), "mode")
    if mode.get("image_shape") != [4096, 4096]:
        raise GateError(f"{workload_id}: production image is not 4096 square")
    if mode.get("gridder") != "awproject" or mode.get("nterms") != 2:
        raise GateError(f"{workload_id}: production AW/MT-MFS mode is absent")

    run = _object(receipt.get("run"), "run")
    if str(run.get("skip_casa")).lower() not in {"1", "true"}:
        raise GateError(f"{workload_id}: frozen CASA reuse was not selected")
    if str(run.get("skip_rust")).lower() not in {"0", "false"}:
        raise GateError(f"{workload_id}: production casa-rs execution was skipped")
    expected_cache_role = expected_workload["run"]["cf_cache_role"]
    if run.get("cf_cache_role") != expected_cache_role:
        raise GateError(f"{workload_id}: prepared-AW cache role differs")
    observed_prefix = pathlib.Path(str(run.get("reuse_casa_prefix", ""))).resolve()
    if observed_prefix != expected_casa_prefix.resolve():
        raise GateError(f"{workload_id}: receipt used a different CASA prefix")
    validate_aw_command_binding(
        receipt,
        imaging=imaging,
        expected_prepared_aw_casa_cache=expected_prepared_aw_casa_cache,
        expected_rust_prefix=expected_rust_prefix,
    )

    results = _object(receipt.get("results"), "results")
    if _object(results.get("rust"), "results.rust").get("status") != "ran":
        raise GateError(f"{workload_id}: production casa-rs run is absent")
    if _object(results.get("casa"), "results.casa").get("status") != "reused":
        raise GateError(f"{workload_id}: CASA was not reused from frozen products")
    prepared_aw = validate_prepared_aw_receipts(results, workload_id=workload_id)

    features = _object(receipt.get("benchmark_features"), "benchmark_features")
    visibility = _object(features.get("visibility"), "benchmark_features.visibility")
    rows = _positive_int(visibility.get("selected_rows"), "selected_rows")
    channels = _positive_int(visibility.get("selected_channels"), "selected_channels")
    correlations = _positive_int(visibility.get("correlations"), "correlations")
    selected_samples = _positive_int(
        visibility.get("visibility_work"), "visibility_work"
    )
    if selected_samples != rows * channels * correlations:
        raise GateError(f"{workload_id}: selected-sample telemetry is inconsistent")
    if selected_samples < MINIMUM_SELECTED_SAMPLES:
        raise GateError(f"{workload_id}: fewer than one million selected samples")

    resources = _object(features.get("resources"), "benchmark_features.resources")
    peak_rss = _positive_int(resources.get("peak_rss_bytes"), "peak_rss_bytes")
    if peak_rss >= MEMORY_CEILING_BYTES:
        raise GateError(f"{workload_id}: peak RSS is not below 32 GiB")

    comparison = _object(results.get("product_comparison"), "product_comparison")
    if comparison.get("status") != "completed":
        raise GateError(f"{workload_id}: frozen product comparison did not complete")
    expected_products = expected_workload["comparison"]["products"]
    products = _object(comparison.get("products"), "comparison.products")
    if list(products) != expected_products and set(products) != set(expected_products):
        raise GateError(f"{workload_id}: product inventory differs from the contract")
    inventory = _object(comparison.get("product_inventory"), "product_inventory")
    if (
        inventory.get("status") != "matched"
        or inventory.get("observed_match") is not True
    ):
        raise GateError(f"{workload_id}: exact product inventory did not match")

    for suffix in expected_products:
        product = _object(products.get(suffix), f"product {suffix}")
        if product.get("status") != "compared":
            raise GateError(f"{workload_id}: {suffix} was not compared")
        metadata = _object(product.get("metadata"), f"{suffix}.metadata")
        if metadata.get("status") != "matched":
            raise GateError(f"{workload_id}: {suffix} metadata differs")
        if product.get("topology_parity") is not True:
            raise GateError(f"{workload_id}: {suffix} validity topology differs")
        full = _object(product.get("full_array"), f"{suffix}.full_array")
        topology = _object(full.get("topology"), f"{suffix}.full_array.topology")
        if not all(
            topology.get(name) is True
            for name in ("finite_equal", "mask_equal", "nonfinite_kind_equal")
        ):
            raise GateError(f"{workload_id}: {suffix} full-array validity differs")
        nrms = full.get("diff_rms_over_right_rms")
        if (
            isinstance(nrms, bool)
            or not isinstance(nrms, int | float)
            or not math.isfinite(nrms)
            or nrms < 0.0
            or nrms > MAXIMUM_NORMALIZED_RMS
        ):
            raise GateError(f"{workload_id}: {suffix} normalized RMS exceeds 1e-3")

    tolerance = _object(comparison.get("tolerance_evaluation"), "tolerance_evaluation")
    if tolerance.get("status") != "passed":
        raise GateError(f"{workload_id}: frozen tolerance contract did not pass")

    return {
        "workload": workload_id,
        "cf_cache_role": expected_cache_role,
        "selected_samples": selected_samples,
        "peak_rss_bytes": peak_rss,
        "product_count": len(products),
        "prepared_aw": prepared_aw,
        "maximum_normalized_rms": max(
            float(products[suffix]["full_array"]["diff_rms_over_right_rms"])
            for suffix in expected_products
        ),
    }


def _load_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateError(f"cannot read JSON {path}: {error}") from error
    return _object(value, str(path))


def run_workload(
    *,
    workload_path: pathlib.Path,
    casa_prefix: pathlib.Path,
    output_dir: pathlib.Path,
    artifact_root: pathlib.Path,
    cf_cache_root: pathlib.Path,
    prepared_aw_casa_cache: pathlib.Path,
    prepared_aw_output_prefix: pathlib.Path,
    base_environment: dict[str, str],
    dry_run: bool = False,
) -> pathlib.Path:
    before = set(output_dir.glob(f"*-{workload_path.stem}-*.json"))
    env = dict(base_environment)
    env.update(
        {
            "CASA_RS_BENCH_SKIP_CASA": "1",
            "CASA_RS_BENCH_SKIP_RUST": "0",
            "CASA_RS_BENCH_REUSE_CASA_PREFIX": str(casa_prefix.resolve()),
            "CASA_RS_BENCH_PREPARED_AW_CASA_CACHE": str(
                prepared_aw_casa_cache.resolve()
            ),
            "CASA_RS_BENCH_PREPARED_AW_OUTPUT_PREFIX": str(
                prepared_aw_output_prefix.resolve()
            ),
            "CASA_RS_BENCH_AW_CF_RESIDENT_MB": str(AW_RESIDENT_MB),
        }
    )
    command = [
        sys.executable,
        str(RUNNER),
        str(workload_path),
        "--output-dir",
        str(output_dir),
        "--artifact-root",
        str(artifact_root),
        "--cf-cache-root",
        str(cf_cache_root),
        "--stream-log",
    ]
    if dry_run:
        command.append("--dry-run")
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        raise GateError(
            f"{workload_path.stem}: run_workload failed with {completed.returncode}"
        )
    candidates = sorted(set(output_dir.glob(f"*-{workload_path.stem}-*.json")) - before)
    if not candidates:
        raise GateError(f"{workload_path.stem}: runner did not publish a receipt")
    return candidates[-1]


def run_rust_cli_preflight(
    receipt: dict[str, Any], *, base_environment: dict[str, str]
) -> None:
    """Run the exact planned Rust argv through production validation without imaging."""

    command = _object(receipt.get("command"), "command")
    argv = command.get("argv")
    if (
        not isinstance(argv, list)
        or not argv
        or not all(isinstance(value, str) and value for value in argv)
    ):
        raise GateError("dry-run receipt omitted the production benchmark argv")
    env = dict(base_environment)
    env.update(_object(command.get("env"), "command.env"))
    env["IMAGER_BENCH_VALIDATE_RUST_CLI_ONLY"] = "1"
    completed = subprocess.run(
        argv,
        cwd=ROOT,
        env=env,
        check=False,
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise GateError(
            "production Rust CLI preflight failed before imaging"
            + (f": {detail}" if detail else "")
        )
    if CLI_PREFLIGHT_MARKER not in completed.stdout:
        raise GateError("production Rust CLI preflight omitted its validation marker")


def prepared_store_snapshot(
    private_root: pathlib.Path,
) -> dict[str, dict[str, int | str]]:
    """Return content and filesystem identities for every private-store file."""

    return {
        str(path.relative_to(private_root)): {
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "inode": path.stat().st_ino,
            "size": path.stat().st_size,
            "mtime_ns": path.stat().st_mtime_ns,
        }
        for path in sorted(private_root.rglob("*"))
        if path.is_file() and not path.is_symlink()
    }


def validate_preflight(
    receipt: dict[str, Any],
    *,
    expected_workload: dict[str, Any],
    expected_casa_prefix: pathlib.Path,
    expected_prepared_aw_casa_cache: pathlib.Path | None = None,
    expected_rust_prefix: pathlib.Path | None = None,
) -> dict[str, Any]:
    imaging = validate_manifest_contract(expected_workload)
    if receipt.get("status") != "dry_run" or receipt.get("exit_code") != 0:
        raise GateError("dry-run receipt did not complete")
    support = _object(receipt.get("run_support"), "run_support")
    rust = _object(
        _object(support.get("targets"), "run_support.targets").get("rust"),
        "run_support.targets.rust",
    )
    if support.get("status") != "runnable" or rust.get("status") != "runnable":
        raise GateError(
            f"paired-AW production route is unavailable: {rust.get('reason')}"
        )
    run = _object(receipt.get("run"), "run")
    if (
        pathlib.Path(str(run.get("reuse_casa_prefix", ""))).resolve()
        != expected_casa_prefix.resolve()
    ):
        raise GateError("dry-run receipt rebound the frozen CASA prefix")
    expected_cache_role = expected_workload["run"]["cf_cache_role"]
    if run.get("cf_cache_role") != expected_cache_role:
        raise GateError("dry-run receipt rebound the prepared-AW cache role")
    validate_aw_command_binding(
        receipt,
        imaging=imaging,
        expected_prepared_aw_casa_cache=expected_prepared_aw_casa_cache,
        expected_rust_prefix=expected_rust_prefix,
    )
    return {
        "workload": _object(receipt.get("workload"), "workload").get("id"),
        "status": "runnable",
        "cf_cache_role": expected_cache_role,
    }


def validate_aw_command_binding(
    receipt: dict[str, Any],
    *,
    imaging: dict[str, Any],
    expected_prepared_aw_casa_cache: pathlib.Path | None,
    expected_rust_prefix: pathlib.Path | None,
) -> None:
    if expected_prepared_aw_casa_cache is None or expected_rust_prefix is None:
        return
    command = _object(receipt.get("command"), "command")
    argv = command.get("argv")
    expected_runner = ROOT / "scripts/bench-imager-vs-casa.sh"
    if (
        not isinstance(argv, list)
        or not argv
        or pathlib.Path(argv[0]) != expected_runner
    ):
        raise GateError("recipe plan did not bind the checked-in Rust benchmark runner")
    env = _object(command.get("env"), "command.env")
    expected = {
        "IMAGER_BENCH_PREPARED_AW_CASA_CACHE": str(
            expected_prepared_aw_casa_cache.resolve()
        ),
        "IMAGER_BENCH_RUST_OUTPUT_PREFIX": str(expected_rust_prefix.resolve()),
        "IMAGER_BENCH_AW_CF_RESIDENT_MB": "384",
        "IMAGER_BENCH_ATERM": "1" if imaging["aterm"] else "0",
        "IMAGER_BENCH_PSTERM": "1" if imaging.get("psterm", False) else "0",
        "IMAGER_BENCH_WBAWP": "1" if imaging["wbawp"] else "0",
        "IMAGER_BENCH_CONJBEAMS": "1" if imaging["conjbeams"] else "0",
        "IMAGER_BENCH_USEPOINTING": "1" if imaging["usepointing"] else "0",
        "IMAGER_BENCH_COMPUTEPASTEP": str(imaging["computepastep"]),
        "IMAGER_BENCH_ROTATEPASTEP": str(imaging["rotatepastep"]),
        "IMAGER_BENCH_POINTINGOFFSETSIGDEV": str(imaging["pointingoffsetsigdev"]),
        "IMAGER_BENCH_NORMTYPE": str(imaging["normtype"]),
        "IMAGER_BENCH_MOSWEIGHT": "1" if imaging["mosweight"] else "0",
        "IMAGER_BENCH_PSFPHASECENTER": str(imaging["psfphasecenter"]),
        "IMAGER_BENCH_VPTABLE": str(imaging["vptable"]),
        "IMAGER_BENCH_GRIDDER": str(imaging["gridder"]),
        "IMAGER_BENCH_SPECMODE": str(imaging["specmode"]),
        "IMAGER_BENCH_FIELD": str(imaging["field"]),
        "IMAGER_BENCH_PHASECENTER_FIELD": str(imaging["phasecenter_field"]),
        "IMAGER_BENCH_SPW": str(imaging["spw"]),
        "IMAGER_BENCH_CHANNEL_START": str(imaging["channel_start"]),
        "IMAGER_BENCH_CHANNEL_COUNT": str(imaging["channel_count"]),
        "IMAGER_BENCH_IMSIZE": str(imaging["imsize"]),
        "IMAGER_BENCH_CELL_ARCSEC": str(imaging["cell_arcsec"]),
        "IMAGER_BENCH_STOKES": str(imaging["stokes"]),
        "IMAGER_BENCH_INTERPOLATION": str(imaging["interpolation"]),
        "IMAGER_BENCH_WEIGHTING": str(imaging["weighting"]),
        "IMAGER_BENCH_ROBUST": str(imaging["robust"]),
        "IMAGER_BENCH_PERCHANWEIGHTDENSITY": "1",
        "IMAGER_BENCH_DECONVOLVER": str(imaging["deconvolver"]),
        "IMAGER_BENCH_NTERMS": str(imaging["nterms"]),
        "IMAGER_BENCH_SCALES": ",".join(str(value) for value in imaging["scales"]),
        "IMAGER_BENCH_NITER": str(imaging["niter"]),
        "IMAGER_BENCH_NMAJOR": "-1",
        "IMAGER_BENCH_GAIN": str(imaging["gain"]),
        "IMAGER_BENCH_THRESHOLD_JY": str(imaging["threshold_jy"]),
        "IMAGER_BENCH_NSIGMA": str(imaging["nsigma"]),
        "IMAGER_BENCH_MINOR_CYCLE_LENGTH": str(imaging["minor_cycle_length"]),
        "IMAGER_BENCH_CYCLEFACTOR": str(imaging["cyclefactor"]),
        "IMAGER_BENCH_MIN_PSFFRACTION": str(imaging["min_psf_fraction"]),
        "IMAGER_BENCH_MAX_PSFFRACTION": str(imaging["max_psf_fraction"]),
        "IMAGER_BENCH_PBLIMIT": str(imaging["pblimit"]),
        "IMAGER_BENCH_WRITE_PB": "1",
        "IMAGER_BENCH_PBCOR": "0",
        "IMAGER_BENCH_USEMASK": str(imaging["usemask"]),
        "IMAGER_BENCH_SAVEMODEL": str(imaging["savemodel"]),
        "IMAGER_BENCH_STANDARD_MFS_ACCELERATION": "cpu",
        "IMAGER_BENCH_IMAGING_FFT_PRECISION": "auto",
        "IMAGER_BENCH_IMAGING_FFT_BACKEND": str(imaging["imaging_fft_backend"]),
        "IMAGER_BENCH_PARALLEL": "0",
        "IMAGER_BENCH_WTERM": str(imaging["wterm"]),
        "IMAGER_BENCH_WPROJPLANES": str(imaging["wprojplanes"]),
        "IMAGER_BENCH_FACETS": str(imaging["facets"]),
        "IMAGER_BENCH_UVRANGE": str(imaging["uvrange"]),
        "IMAGER_BENCH_INTENT": str(imaging["intent"]),
        "IMAGER_BENCH_MASK_IMAGE": str(imaging.get("mask_image", "")),
        "IMAGER_BENCH_SMALL_SCALE_BIAS": str(imaging["smallscalebias"]),
        "IMAGER_BENCH_RESTORING_BEAM": str(imaging["restoringbeam"]),
        "IMAGER_BENCH_MS_STAGING": "direct",
        "IMAGER_BENCH_SKIP_CASA": "1",
        "IMAGER_BENCH_SKIP_RUST": "0",
        "IMAGER_BENCH_SKIP_PROFILE": "1",
    }
    for name, value in expected.items():
        if env.get(name) != value:
            raise GateError(f"recipe plan did not bind exact {name}={value}")
    forbidden_env = {
        "IMAGER_BENCH_CHANCHUNKS",
        "IMAGER_BENCH_STANDARD_MFS_GRID_THREADS",
        "IMAGER_BENCH_STANDARD_MFS_METAL_MINOR_CYCLE_CHUNK",
        "IMAGER_BENCH_IMAGING_MEMORY_TARGET_MB",
        "IMAGER_BENCH_IMAGING_PREPARE_BUFFER_MB",
        "IMAGER_BENCH_IMAGING_ROW_BLOCK_ROWS",
        "IMAGER_BENCH_IMAGING_PREPARE_WORKERS",
        "IMAGER_BENCH_IMAGING_READ_AHEAD_BLOCKS",
    }
    present = sorted(forbidden_env.intersection(env))
    if present:
        raise GateError(
            "recipe plan retained unsupported T51 runtime overrides: "
            + ", ".join(present)
        )
    products = _object(receipt.get("products"), "products")
    if pathlib.Path(str(products.get("rust_prefix", ""))).resolve() != (
        expected_rust_prefix.resolve()
    ):
        raise GateError("recipe plan rebound the shared native Rust output prefix")
    if receipt.get("status") == "completed":
        result_paths = _object(
            _object(receipt.get("results"), "results").get("product_paths"),
            "results.product_paths",
        )
        if pathlib.Path(str(result_paths.get("rust_prefix", ""))).resolve() != (
            expected_rust_prefix.resolve()
        ):
            raise GateError("production result used a different Rust prefix")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dirty-casa-prefix", type=pathlib.Path, required=True)
    parser.add_argument("--clean-casa-prefix", type=pathlib.Path, required=True)
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    parser.add_argument("--artifact-root", type=pathlib.Path, required=True)
    parser.add_argument("--cf-cache-root", type=pathlib.Path, required=True)
    parser.add_argument(
        "--prepared-aw-casa-cache",
        type=pathlib.Path,
        required=True,
        help="validated paired CASA CFS/WTCFS source used only by native AW preparation",
    )
    parser.add_argument(
        "--prepared-aw-shared-parent",
        type=pathlib.Path,
        required=True,
        help="fresh same-filesystem parent shared by dirty and clean Rust prefixes",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate both production plans without executing imaging or comparison",
    )
    args = parser.parse_args()

    for path in (args.dirty_casa_prefix, args.clean_casa_prefix):
        if not path.parent.is_dir():
            parser.error(f"frozen CASA prefix is unavailable: {path}")
    if not args.prepared_aw_casa_cache.is_dir():
        parser.error(
            f"validated paired AW CASA cache is unavailable: {args.prepared_aw_casa_cache}"
        )
    if args.prepared_aw_casa_cache.resolve() == args.cf_cache_root.resolve():
        parser.error("--prepared-aw-casa-cache must be distinct from --cf-cache-root")
    if not args.dry_run and args.prepared_aw_shared_parent.exists():
        parser.error(
            "--prepared-aw-shared-parent must be a fresh, absent directory for the "
            "cold-to-warm proof"
        )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.artifact_root.mkdir(parents=True, exist_ok=True)
    args.cf_cache_root.mkdir(parents=True, exist_ok=True)
    if not args.dry_run:
        args.prepared_aw_shared_parent.mkdir(parents=True)
        if (
            os.stat(args.prepared_aw_shared_parent).st_dev
            != os.stat(args.artifact_root).st_dev
        ):
            parser.error(
                "prepared-AW shared parent and artifact root must be on one filesystem"
            )

    rows = []
    cold_snapshot: dict[str, dict[str, int | str]] | None = None
    private_root = args.prepared_aw_shared_parent / ".casa-rs-aw-prepared"
    for workload_path, casa_prefix in (
        (DIRTY_WORKLOAD, args.dirty_casa_prefix),
        (CLEAN_WORKLOAD, args.clean_casa_prefix),
    ):
        workload = _load_json(workload_path)
        validate_manifest_contract(workload)
        rust_prefix = args.prepared_aw_shared_parent / expected_workload_role(
            workload_path
        )
        receipt_path = run_workload(
            workload_path=workload_path,
            casa_prefix=casa_prefix,
            output_dir=args.output_dir,
            artifact_root=args.artifact_root,
            cf_cache_root=args.cf_cache_root,
            prepared_aw_casa_cache=args.prepared_aw_casa_cache,
            prepared_aw_output_prefix=rust_prefix,
            base_environment=os.environ,
            dry_run=args.dry_run,
        )
        receipt = _load_json(receipt_path)
        if args.dry_run:
            row = validate_preflight(
                receipt,
                expected_workload=workload,
                expected_casa_prefix=casa_prefix,
                expected_prepared_aw_casa_cache=args.prepared_aw_casa_cache,
                expected_rust_prefix=rust_prefix,
            )
            run_rust_cli_preflight(receipt, base_environment=os.environ)
            row["rust_cli_preflight"] = "passed"
        else:
            row = validate_receipt(
                receipt,
                expected_workload=workload,
                expected_casa_prefix=casa_prefix,
                expected_prepared_aw_casa_cache=args.prepared_aw_casa_cache,
                expected_rust_prefix=rust_prefix,
            )
        row["receipt"] = str(receipt_path.resolve())
        if not args.dry_run:
            snapshot = prepared_store_snapshot(private_root)
            if not snapshot:
                raise GateError(
                    "native AW preparation produced no durable private store"
                )
            manifest_count = sum(
                pathlib.Path(name).name == "manifest.json" for name in snapshot
            )
            if manifest_count != EXPECTED_PREPARED_AW_CELLS:
                raise GateError(
                    "native AW preparation did not retain the complete "
                    f"{EXPECTED_PREPARED_AW_CELLS}-cell catalog"
                )
            if cold_snapshot is None:
                cold_snapshot = snapshot
                row["prepared_aw_operation"] = "cold-load-read"
            elif snapshot != cold_snapshot:
                raise GateError(
                    "warm native AW preparation mutated the frozen private store"
                )
            else:
                row["prepared_aw_operation"] = "warm-reuse-read"
            row["prepared_aw_manifest_count"] = manifest_count
            row["prepared_aw_store_sha256"] = hashlib.sha256(
                json.dumps(snapshot, sort_keys=True).encode("utf-8")
            ).hexdigest()
        rows.append(row)

    if [row["cf_cache_role"] for row in rows] != ["cold", "warm"]:
        raise GateError("dirty and clean runs do not form a cold-to-warm sequence")

    summary = {
        "schema": "casa-rs-t51-aw-vlass-acceptance-v2",
        "status": "dry_run" if args.dry_run else "pass",
        "casa_execution": "reused-frozen-products-no-casa-rerun",
        "memory_ceiling_bytes_exclusive": MEMORY_CEILING_BYTES,
        "minimum_selected_samples": MINIMUM_SELECTED_SAMPLES,
        "maximum_normalized_rms": MAXIMUM_NORMALIZED_RMS,
        "casa_oracle_cache_root": str(args.cf_cache_root.resolve()),
        "prepared_aw_casa_cache": str(args.prepared_aw_casa_cache.resolve()),
        "prepared_aw_shared_parent": str(args.prepared_aw_shared_parent.resolve()),
        "prepared_aw_cache_sequence": ["cold", "warm"],
        "runs": rows,
    }
    summary_path = args.output_dir / "t51-aw-vlass-acceptance.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(summary_path)


def expected_workload_role(workload_path: pathlib.Path) -> str:
    return "dirty" if "dirty" in workload_path.stem else "clean"


if __name__ == "__main__":
    try:
        main()
    except GateError as error:
        print(f"T51 AW VLASS acceptance failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None
