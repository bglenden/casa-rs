#!/usr/bin/env python3
"""Run and validate T51 paired-AW acceptance against frozen VLASS products."""

from __future__ import annotations

import argparse
import hashlib
import json
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
EXPECTED_PREPARED_AW_CELLS = 64
AW_RESIDENT_MB = 384


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

    imaging = _object(expected_workload.get("imaging"), "manifest imaging")
    required_imaging = {
        "gridder": "awproject",
        "wterm": "wproject",
        "wprojplanes": 32,
        "field": "1107~1127,1512~1532,1542~1562",
        "spw": "2~17",
        "channel_count": 64,
        "imsize": 4096,
        "nterms": 2,
        "aterm": True,
        "wbawp": True,
        "conjbeams": True,
        "usepointing": True,
        "normtype": "flatnoise",
        "imaging_fft_backend": "rustfft",
    }
    for name, expected in required_imaging.items():
        if imaging.get(name) != expected:
            raise GateError(
                f"{workload_id}: manifest imaging.{name} must be {expected!r}"
            )

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
        if not isinstance(nrms, int | float) or nrms > MAXIMUM_NORMALIZED_RMS:
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


def prepared_store_snapshot(private_root: pathlib.Path) -> dict[str, dict[str, int | str]]:
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
        imaging=_object(expected_workload.get("imaging"), "manifest imaging"),
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
        "IMAGER_BENCH_IMAGING_FFT_BACKEND": str(imaging["imaging_fft_backend"]),
        "IMAGER_BENCH_WTERM": str(imaging["wterm"]),
        "IMAGER_BENCH_WPROJPLANES": str(imaging["wprojplanes"]),
        "IMAGER_BENCH_FACETS": str(imaging["facets"]),
        "IMAGER_BENCH_UVRANGE": str(imaging["uvrange"]),
        "IMAGER_BENCH_INTENT": str(imaging["intent"]),
        "IMAGER_BENCH_MASK_IMAGE": str(imaging.get("mask_image", "")),
        "IMAGER_BENCH_SMALL_SCALE_BIAS": str(imaging["smallscalebias"]),
        "IMAGER_BENCH_RESTORING_BEAM": str(imaging["restoringbeam"]),
    }
    for name, value in expected.items():
        if env.get(name) != value:
            raise GateError(f"recipe plan did not bind exact {name}={value}")
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
        parser.error(
            "--prepared-aw-casa-cache must be distinct from --cf-cache-root"
        )
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
        if os.stat(args.prepared_aw_shared_parent).st_dev != os.stat(
            args.artifact_root
        ).st_dev:
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
        row = (
            validate_preflight(
                receipt,
                expected_workload=workload,
                expected_casa_prefix=casa_prefix,
                expected_prepared_aw_casa_cache=args.prepared_aw_casa_cache,
                expected_rust_prefix=rust_prefix,
            )
            if args.dry_run
            else validate_receipt(
                receipt,
                expected_workload=workload,
                expected_casa_prefix=casa_prefix,
                expected_prepared_aw_casa_cache=args.prepared_aw_casa_cache,
                expected_rust_prefix=rust_prefix,
            )
        )
        row["receipt"] = str(receipt_path.resolve())
        if not args.dry_run:
            snapshot = prepared_store_snapshot(private_root)
            if not snapshot:
                raise GateError("native AW preparation produced no durable private store")
            manifest_count = sum(
                pathlib.Path(name).name == "manifest.json" for name in snapshot
            )
            if manifest_count != EXPECTED_PREPARED_AW_CELLS:
                raise GateError(
                    "native AW preparation did not retain the complete 64-cell catalog"
                )
            if cold_snapshot is None:
                cold_snapshot = snapshot
                row["prepared_aw_operation"] = "cold-load-consume"
            elif snapshot != cold_snapshot:
                raise GateError("warm native AW preparation mutated the frozen private store")
            else:
                row["prepared_aw_operation"] = "warm-reuse-consume"
            row["prepared_aw_manifest_count"] = manifest_count
            row["prepared_aw_store_sha256"] = hashlib.sha256(
                json.dumps(snapshot, sort_keys=True).encode("utf-8")
            ).hexdigest()
        rows.append(row)

    if [row["cf_cache_role"] for row in rows] != ["cold", "warm"]:
        raise GateError("dirty and clean runs do not form a cold-to-warm sequence")

    summary = {
        "schema": "casa-rs-t51-aw-vlass-acceptance-v1",
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
