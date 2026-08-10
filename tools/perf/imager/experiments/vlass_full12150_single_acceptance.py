#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run the bounded 12,150-pixel single-field VLASS acceptance row."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import subprocess
import sys
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
IMAGER_DIR = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(IMAGER_DIR) not in sys.path:
    sys.path.insert(0, str(IMAGER_DIR))

import vlass_full12150_acceptance as shared  # noqa: E402
from perf_harness.host_telemetry import HostTelemetryError  # noqa: E402
from perf_harness.image_compare import compare_products  # noqa: E402


CASA_WALL_SECONDS = 3_798.0688115420053
RUST_WALL_LIMIT_SECONDS = CASA_WALL_SECONDS / shared.MINIMUM_SPEEDUP
FIELD = "1525"
EXPECTED_ROWS = "10400"
MANIFEST_RELATIVE = Path(
    "tools/perf/imager/workloads/vlass-fragment-single-field-clean-casa.json"
)
MANIFEST_SHA256 = "5da8ce24c92b2d47e53784e8600976bf37708086309820cb1b61af6f8982bd9e"
CF_TREE_SHA256 = "fd473479f9b4b0a7a1e21f26a44c458eb1ecf4b785d8dbdd3fadf339fb675fa5"
CASA_RECEIPT_SHA256 = "f9216878e3372ecb4a81f565e33e6b5b2729abf20d0c1d7313892ac4db6a680d"
CF_RELATIVE = Path(
    "cf-cache/6.7.5.9/8e5679681214158629c7eb6113bc3b062d6105fbae64471905aa73de50080a69"
)
CASA_RECEIPT_RELATIVE = Path(
    "recovery-references/casa-a-single-clean-n2000/receipts/"
    "20260803T182722Z-vlass-fragment-single-field-clean-casa-399f12d9.json"
)
CASA_PREFIX_RELATIVE = Path(
    "recovery-references/casa-a-single-clean-n2000/artifacts/"
    "vlass-fragment-single-field-clean-casa/casa_deterministic_clean_fiducial/"
    "20260803T182722Z-vlass-fragment-single-field-clean-casa-399f12d9/"
    "casa/measured-001/casa"
)


def default_paths(root: Path = shared.DEFAULT_ROOT) -> shared.Paths:
    return shared.Paths(
        root=root,
        ms=root / shared.MS_RELATIVE,
        cf_cache=root / CF_RELATIVE,
        mask=root / shared.MASK_RELATIVE,
        casa_receipt=root / CASA_RECEIPT_RELATIVE,
        casa_prefix=root / CASA_PREFIX_RELATIVE,
        manifest=shared.REPO_ROOT / MANIFEST_RELATIVE,
        contract=shared.REPO_ROOT / shared.CONTRACT_RELATIVE,
        casa_python=shared.DEFAULT_CASA_PYTHON,
        fftw_dir=shared.DEFAULT_FFTW_DIR,
    )


def validate_manifest(paths: shared.Paths) -> dict[str, Any]:
    for path in (
        paths.ms,
        paths.cf_cache,
        paths.mask,
        paths.casa_receipt,
        paths.casa_prefix.parent,
        paths.manifest,
        paths.contract,
        paths.casa_python,
        paths.fftw_dir,
    ):
        if not path.exists():
            raise shared.AcceptanceError(
                f"required acceptance input does not exist: {path}"
            )
    if shared.sha256_file(paths.manifest) != MANIFEST_SHA256:
        raise shared.AcceptanceError("frozen CASA-A manifest hash differs")
    if shared.sha256_file(paths.contract) != shared.CONTRACT_SHA256:
        raise shared.AcceptanceError("scientific-equivalence contract hash differs")
    if shared.sha256_file(paths.casa_receipt) != CASA_RECEIPT_SHA256:
        raise shared.AcceptanceError("frozen CASA-A receipt hash differs")
    manifest = json.loads(paths.manifest.read_text(encoding="utf-8"))
    imaging = manifest.get("imaging", {})
    required = {
        "field": FIELD,
        "phasecenter_field": 1525,
        "spw": "2~17",
        "imsize": 12_150,
        "channel_count": 64,
        "niter": 2_000,
        "usepointing": True,
        "mask_sha256": shared.MASK_TREE_SHA256,
    }
    if any(imaging.get(key) != value for key, value in required.items()):
        raise shared.AcceptanceError("frozen manifest no longer matches CASA-A science")
    comparison = manifest.get("comparison", {})
    if tuple(comparison.get("products", ())) != shared.EXPECTED_PRODUCTS:
        raise shared.AcceptanceError(
            "frozen manifest product inventory is not the ordered 19-set"
        )
    comparison["tolerances"] = json.loads(paths.contract.read_text(encoding="utf-8"))
    return manifest


def validate_input_identities(paths: shared.Paths) -> dict[str, dict[str, Any]]:
    observed = {
        "ms": shared.compact_tree_identity_uncached(paths.ms),
        "cf_cache": shared.casa_tree_inventory_uncached(paths.cf_cache),
        "mask": shared.compact_tree_identity_uncached(
            paths.mask, excluded_names={"table.lock"}
        ),
    }
    expected = {
        "ms": shared.MS_TREE_SHA256,
        "cf_cache": CF_TREE_SHA256,
        "mask": shared.MASK_TREE_SHA256,
    }
    for name, expected_hash in expected.items():
        field = "stable_tree_sha256" if name == "cf_cache" else "tree_sha256"
        if observed[name].get(field) != expected_hash:
            raise shared.AcceptanceError(f"{name} tree identity differs")
    return observed


def _replace_option(command: list[str], option: str, value: str) -> None:
    command[command.index(option) + 1] = value


def common_imager_command(
    binary: Path,
    paths: shared.Paths,
    output_prefix: Path,
    target_mib: int,
    *,
    memory_pressure_policy: str,
) -> list[str]:
    command = shared.common_imager_command(
        binary,
        paths,
        output_prefix,
        target_mib,
        memory_pressure_policy=memory_pressure_policy,
    )
    _replace_option(command, "--field", FIELD)
    _replace_option(command, "--niter", "2000")
    return command


def validate_probe_log(
    text: str,
    target_mib: int,
    *,
    memory_pressure_policy: str,
    require_target_within_headroom: bool,
) -> dict[str, Any]:
    preflight = shared.matching_lines(text, "standard_mfs_planner_preflight ")
    resources = shared.matching_lines(text, "standard_mfs_planning_resources ")
    runtime = shared.matching_lines(text, "standard_mfs_runtime_plan ")
    decisions = shared.matching_lines(text, "standard_mfs_execution_decision ")
    grouped = shared.matching_lines(text, "awproject_grouped_replay_plan ")
    frequency_edges = shared.matching_lines(text, "casa_mfs_frequency_edge_range ")
    if len(preflight) != 1 or preflight[0].get("status") != "admitted":
        raise shared.AcceptanceError(
            "single-field probe did not emit one admitted preflight"
        )
    expected = {
        "grouped_metal_status": "admitted",
        "rows_total": EXPECTED_ROWS,
        "ddids": "16",
        "selected_channels": "64",
        "correlations": "4",
        "memory_pressure_policy": memory_pressure_policy,
        "visibility_streamed": "false",
        "replay_compiled": "false",
        "grids_allocated": "false",
        "products_materialized": "false",
    }
    if any(preflight[0].get(key) != value for key, value in expected.items()):
        raise shared.AcceptanceError(
            "single-field probe topology or allocation-free receipt differs"
        )
    if len(resources) != 1:
        raise shared.AcceptanceError("single-field probe omitted planning resources")
    target_bytes = target_mib * shared.MIB
    expected_origin = (
        "cli-intentional-oversubscription"
        if memory_pressure_policy == "oversubscribe"
        else "cli-imaging"
    )
    if resources[0].get("memory_target_bytes") != str(target_bytes):
        raise shared.AcceptanceError("single-field probe changed the requested target")
    if resources[0].get("memory_target_origin") != expected_origin:
        raise shared.AcceptanceError("single-field probe changed target origin")
    if require_target_within_headroom and target_bytes > int(
        resources[0].get("no_swap_headroom_bytes", "-1")
    ):
        raise shared.AcceptanceError("single-field target exceeds fresh headroom")
    if len(runtime) != 1:
        raise shared.AcceptanceError("single-field probe omitted runtime plan")
    if runtime[0].get("initial_dirty_backend") != "metal-row-run-grouped":
        raise shared.AcceptanceError(
            "single-field probe did not select Metal initial gridding"
        )
    if runtime[0].get("residual_backend") != "metal-row-run-grouped":
        raise shared.AcceptanceError(
            "single-field probe did not select Metal residual replay"
        )
    if len(grouped) != 1:
        raise shared.AcceptanceError("single-field probe omitted grouped replay")
    if grouped[0].get("architecture") != "source-order-grouped-tile-v1":
        raise shared.AcceptanceError("single-field grouped architecture differs")
    if grouped[0].get("tile_side") != "11":
        raise shared.AcceptanceError("single-field grouped tile side differs")
    if float(grouped[0].get("omitted_squared_l2_energy", "nan")) != 0.0:
        raise shared.AcceptanceError("single-field grouped replay is not exact-support")
    if len(frequency_edges) != 16 or any(
        edge.get(endpoint) != FIELD
        for edge in frequency_edges
        for endpoint in ("low_field", "high_field")
    ):
        raise shared.AcceptanceError(
            "single-field probe did not bind every SPW edge to field 1525"
        )
    by_name = {entry.get("name"): entry.get("value") for entry in decisions}
    expected_decisions = {
        "awproject_selected_field_count": "1",
        "awproject_initial_grid_backend": "source-major-grouped-metal-f64",
        "awproject_source_major_architecture": "direct-source-major-v8-single-slot-i16-residual",
        "awproject_source_major_initial_accumulation": "high-limb-only",
        "awproject_source_major_initial_grid_bytes": "9447840000",
        "awproject_multifield_initial_grid_admission": "admitted",
        "awproject_grouped_replay_replaced_generic_caches": "true",
        "awproject_grouped_metal_generic_scratch_bytes": "0",
        "awproject_grouped_metal_residual_output_bytes": "2361960000",
        "awproject_grouped_metal_residual_compensation_bytes": "2361960000",
        "awproject_grouped_metal_model_wrapper_bytes": "2361960000",
        "awproject_grouped_metal_safety_reserve_bytes": str(64 * shared.MIB),
    }
    if any(by_name.get(key) != value for key, value in expected_decisions.items()):
        raise shared.AcceptanceError("single-field source-major decisions differ")
    return {
        "preflight": preflight[0],
        "resources": resources[0],
        "runtime": runtime[0],
        "frequency_edges": frequency_edges,
        "decisions": by_name,
        "grouped": grouped[0],
    }


def run_probe(
    command: list[str],
    environment: dict[str, str],
    log_path: Path,
    target_mib: int,
    *,
    memory_pressure_policy: str,
    require_target_within_headroom: bool,
) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        cwd=shared.REPO_ROOT,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=900.0,
        check=False,
    )
    log_path.write_text(completed.stdout, encoding="utf-8")
    if completed.returncode != 0:
        raise shared.AcceptanceError(
            f"single-field plan probe exited {completed.returncode}"
        )
    return validate_probe_log(
        completed.stdout,
        target_mib,
        memory_pressure_policy=memory_pressure_policy,
        require_target_within_headroom=require_target_within_headroom,
    )


def validate_runtime_log(text: str) -> dict[str, Any]:
    return shared.validate_runtime_log(text)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=shared.DEFAULT_ROOT)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--allow-pressure-experiment", action="store_true")
    return parser.parse_args(argv)


def run_acceptance(args: argparse.Namespace) -> Path:
    paths = default_paths(args.root.resolve())
    run_root = paths.root / "recovery-candidates/acceptance" / args.run_id
    if run_root.exists():
        raise shared.AcceptanceError(
            f"refusing to overwrite acceptance run root: {run_root}"
        )
    run_root.mkdir(parents=True)
    manifest = validate_manifest(paths)
    shared.validate_disk_headroom(paths)
    frozen = shared.build_and_freeze_binary(run_root)
    binary = Path(frozen.path)
    identities_before = validate_input_identities(paths)
    baseline = shared.capture_baseline(
        allow_pressure_experiment=args.allow_pressure_experiment
    )
    output_prefix = run_root / "products/rust"
    output_prefix.parent.mkdir(parents=True)
    policy = (
        "oversubscribe" if args.allow_pressure_experiment else "conservative-no-swap"
    )
    common = common_imager_command(
        binary,
        paths,
        output_prefix,
        baseline.target_mib,
        memory_pressure_policy=policy,
    )
    environment = shared.restricted_environment(paths)
    probe_log = run_root / "probe.log"
    probe = run_probe(
        shared.probe_command(common),
        environment,
        probe_log,
        baseline.target_mib,
        memory_pressure_policy=policy,
        require_target_within_headroom=not args.allow_pressure_experiment,
    )
    if shared.sha256_file(binary) != frozen.sha256:
        raise shared.AcceptanceError(
            "frozen binary changed during the single-field probe"
        )
    preflight = {
        "kind": "vlass_full12150_single_acceptance_preflight",
        "status": "admitted",
        "execute_requested": args.execute,
        "evidence_class": "pressure-experiment"
        if args.allow_pressure_experiment
        else "acceptance",
        "acceptance_eligible": not args.allow_pressure_experiment,
        "memory_pressure_policy": policy,
        "paths": {key: str(value) for key, value in asdict(paths).items()},
        "manifest_sha256": MANIFEST_SHA256,
        "contract_sha256": shared.CONTRACT_SHA256,
        "casa_receipt_sha256": CASA_RECEIPT_SHA256,
        "casa_wall_seconds": CASA_WALL_SECONDS,
        "binary": asdict(frozen),
        "identities_before": identities_before,
        "baseline": asdict(baseline),
        "environment": environment,
        "common_command": common,
        "probe_command": shared.probe_command(common),
        "probe": probe,
        "probe_log_sha256": shared.sha256_file(probe_log),
    }
    shared.atomic_json(run_root / "preflight.json", preflight)
    if not args.execute:
        return run_root / "preflight.json"

    launch_baseline = shared.capture_baseline(
        allow_pressure_experiment=args.allow_pressure_experiment
    )
    if (
        not args.allow_pressure_experiment
        and launch_baseline.target_mib < baseline.target_mib
    ):
        raise shared.AcceptanceError(
            "host headroom worsened after the single-field probe"
        )
    if shared.sha256_file(binary) != frozen.sha256:
        raise shared.AcceptanceError(
            "frozen binary changed before single-field execution"
        )
    progress_path = run_root / "progress.jsonl"
    run_log = run_root / "casa-rs.log"
    monitor = shared.monitor_run(
        shared.run_command(common, progress_path),
        environment=environment,
        baseline=launch_baseline,
        log_path=run_log,
        progress_path=progress_path,
        telemetry_path=run_root / "telemetry.json",
        interval_seconds=(
            shared.PRESSURE_EXPERIMENT_MONITOR_INTERVAL_SECONDS
            if args.allow_pressure_experiment
            else shared.MONITOR_INTERVAL_SECONDS
        ),
        # Compression remains in telemetry; the pressure experiment stops on
        # destructive swap/throttling/pressure signals instead of compression
        # alone.
        max_compressed_growth_bytes=None,
        allow_sustained_pressure_warning=args.allow_pressure_experiment,
        wall_limit_seconds=RUST_WALL_LIMIT_SECONDS,
    )
    execution = {
        "monitor": asdict(monitor),
        "binary_sha256_after": shared.sha256_file(binary),
        "log_sha256": shared.sha256_file(run_log),
        "speedup": CASA_WALL_SECONDS / monitor.wall_seconds,
    }
    shared.atomic_json(run_root / "execution.json", execution)
    if monitor.stop_reason is not None or monitor.exit_code != 0:
        raise shared.AcceptanceError(
            f"bounded single-field execution failed: exit={monitor.exit_code}, stop={monitor.stop_reason}"
        )
    if monitor.wall_seconds > RUST_WALL_LIMIT_SECONDS:
        raise shared.AcceptanceError("single-field execution missed the 10x wall")
    runtime = validate_runtime_log(run_log.read_text(encoding="utf-8"))
    identities_after = validate_input_identities(paths)
    if identities_after != identities_before:
        raise shared.AcceptanceError("a single-field immutable input changed")
    request = shared.comparison_request(
        manifest, output_prefix, paths.casa_prefix, run_root
    )
    comparison = compare_products(
        casa_python=paths.casa_python,
        request=request,
        artifact_prefix=run_root / "comparison/result",
        cwd=shared.REPO_ROOT,
    )
    evaluation = comparison.get("tolerance_evaluation", {})
    if comparison.get("status") != "completed" or evaluation.get("status") != "passed":
        shared.atomic_json(run_root / "comparison.json", comparison)
        raise shared.AcceptanceError("single-field 19-product comparison did not pass")
    receipt = {
        "kind": "vlass_full12150_single_acceptance",
        "status": "passed-pressure-experiment"
        if args.allow_pressure_experiment
        else "passed",
        "acceptance_eligible": not args.allow_pressure_experiment,
        "preflight": preflight,
        "launch_baseline": asdict(launch_baseline),
        "execution": execution,
        "runtime_contract": runtime,
        "identities_after": identities_after,
        "comparison_request": request,
        "comparison": comparison,
    }
    shared.atomic_json(run_root / "receipt.json", receipt)
    return run_root / "receipt.json"


def main(argv: list[str] | None = None) -> int:
    try:
        path = run_acceptance(parse_args(argv))
    except (
        shared.AcceptanceError,
        HostTelemetryError,
        OSError,
        subprocess.SubprocessError,
    ) as error:
        print(f"vlass full12150 single acceptance: {error}", file=sys.stderr)
        return 2
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
