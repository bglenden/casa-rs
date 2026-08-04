#!/usr/bin/env python3
"""Run and guard the frozen five-minute VLASS autoresearch workload."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import resource
import subprocess
import sys
import time
import uuid
from typing import Any


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
IMAGER_TOOLS_ROOT = SCRIPT_DIR.parent
REPO_ROOT = SCRIPT_DIR.parents[3]
if str(IMAGER_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(IMAGER_TOOLS_ROOT))

from autoresearch.vlass_5m_core import (  # noqa: E402
    ContractError,
    canonical_sha256,
    comparison_request,
    evaluate_receipt,
    load_contract,
    load_json_object,
    parse_runtime_log,
    runtime_command,
    runtime_environment,
    sha256_file,
)
from perf_harness.artifacts import atomic_write_json  # noqa: E402
from perf_harness.host_telemetry import DarwinHostTelemetrySampler  # noqa: E402
from perf_harness.image_compare import compare_products  # noqa: E402
from perf_harness.tree_identity import tree_identity  # noqa: E402


DEFAULT_CONTRACT = SCRIPT_DIR / "vlass_5m_contract.json"
CASA_SITE_CONFIG = IMAGER_TOOLS_ROOT / "experiments" / "casasiteconfig_vlass.py"
SELECTION_PROBE = IMAGER_TOOLS_ROOT / "ms_selection_accounting.py"


def run_git(*args: str, check: bool = True) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=check,
        capture_output=True,
    )


def source_state() -> dict[str, Any]:
    head = run_git("rev-parse", "HEAD").stdout.decode().strip()
    diff = run_git("diff", "--binary", "HEAD", "--").stdout
    untracked = (
        run_git("ls-files", "--others", "--exclude-standard", "-z")
        .stdout.decode()
        .split("\0")
    )
    untracked = sorted(path for path in untracked if path)
    untracked_identity = []
    for relative in untracked:
        path = REPO_ROOT / relative
        if not path.is_file() or path.is_symlink():
            raise ContractError(f"unsupported untracked source-state entry: {path}")
        untracked_identity.append(
            {
                "path": relative,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    value = {
        "head": head,
        "tracked_diff_sha256": hashlib.sha256(diff).hexdigest(),
        "untracked": untracked_identity,
    }
    value["state_sha256"] = canonical_sha256(value)
    value["status"] = run_git(
        "status", "--short", "--untracked-files=all"
    ).stdout.decode()
    return value


def build_release(contract: dict[str, Any], run_root: pathlib.Path) -> dict[str, Any]:
    build = contract["build"]
    target_dir = pathlib.Path(build["target_dir"])
    target_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_root / "release-build.log"
    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(target_dir)
    environment["CARGO_INCREMENTAL"] = "0"
    started = time.monotonic()
    completed = subprocess.run(
        build["command"],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )
    build_seconds = time.monotonic() - started
    log_path.write_text(
        (completed.stdout or "") + (completed.stderr or ""), encoding="utf-8"
    )
    if completed.returncode != 0:
        raise ContractError(
            f"release build exited {completed.returncode}; see {log_path}"
        )
    binary = target_dir / build["binary_relative_path"]
    if not binary.is_file():
        raise ContractError(f"release build did not create {binary}")
    return {
        "profile": "release",
        "command": build["command"],
        "target_dir": str(target_dir),
        "binary": str(binary),
        "binary_sha256": sha256_file(binary),
        "build_seconds": build_seconds,
        "timed_build_seconds": 0.0,
        "completed_before_timed_region": True,
        "log": str(log_path),
        "log_sha256": sha256_file(log_path),
    }


def validate_inputs(contract: dict[str, Any]) -> dict[str, Any]:
    dataset = contract["dataset"]
    accounting_path = REPO_ROOT / dataset["selection_accounting"]
    expected_accounting = dataset["selection_accounting_sha256"]
    if not isinstance(expected_accounting, str):
        raise ContractError(
            "selection accounting is not frozen; run freeze-selection and "
            "record its SHA-256 in the contract"
        )
    actual_accounting = sha256_file(accounting_path)
    if actual_accounting != expected_accounting:
        raise ContractError(
            f"selection accounting mismatch: expected {expected_accounting}, "
            f"got {actual_accounting}"
        )
    accounting = load_json_object(accounting_path, label="selection accounting")
    measurement_set = pathlib.Path(dataset["measurement_set"])
    measurement_set_identity = tree_identity(
        measurement_set, excluded_names={"table.lock"}
    )
    if (
        measurement_set_identity["tree_sha256"]
        != dataset["measurement_set_tree_sha256"]
    ):
        raise ContractError("VLASS MeasurementSet identity changed")
    mask = pathlib.Path(dataset["mask"])
    mask_identity = tree_identity(mask, excluded_names={"table.lock"})
    if mask_identity["tree_sha256"] != dataset["mask_tree_sha256"]:
        raise ContractError("VLASS deterministic mask identity changed")
    model_prefix = contract["inputs"]["frozen_model_prefix"]
    model_identity = {}
    for term in (0, 1):
        path = pathlib.Path(f"{model_prefix}.model.tt{term}")
        model_identity[f".model.tt{term}"] = tree_identity(
            path, excluded_names={"table.lock"}
        )
    for required in (
        pathlib.Path(dataset["measurement_set"]),
        pathlib.Path(dataset["cf_cache"]),
        pathlib.Path(contract["inputs"]["fftw_library_dir"]),
        pathlib.Path(contract["inputs"]["measures_dir"]),
    ):
        if not required.exists():
            raise ContractError(
                f"required frozen workload input is missing: {required}"
            )
    return {
        "selection_accounting_path": str(accounting_path),
        "selection_accounting_sha256": actual_accounting,
        "selection_accounting": accounting,
        "measurement_set": {
            "path": str(measurement_set),
            **measurement_set_identity,
        },
        "mask": {"path": str(mask), **mask_identity},
        "frozen_model": model_identity,
    }


def selection_receipt(
    contract: dict[str, Any], inputs: dict[str, Any]
) -> dict[str, Any]:
    accounting = inputs["selection_accounting"]
    return {
        "field_ids": contract["workload"]["field_ids"],
        "spw_ids": contract["workload"]["spw_ids"],
        "channel_start": contract["workload"]["channel_start"],
        "channel_count": contract["workload"]["channel_count"],
        "accounting_path": inputs["selection_accounting_path"],
        "accounting_sha256": inputs["selection_accounting_sha256"],
        "by_spw": accounting["by_spw"],
    }


def output_identities(
    contract: dict[str, Any], output_prefix: pathlib.Path
) -> dict[str, Any]:
    identities: dict[str, Any] = {}
    for suffix in contract["workload"]["products"]:
        path = pathlib.Path(f"{output_prefix}{suffix}")
        identities[suffix] = {
            "path": str(path),
            **tree_identity(path, excluded_names={"table.lock"}),
        }
    return identities


def rusage_delta(
    before: resource.struct_rusage, after: resource.struct_rusage
) -> dict[str, Any]:
    return {
        "user_cpu_seconds": max(0.0, after.ru_utime - before.ru_utime),
        "system_cpu_seconds": max(0.0, after.ru_stime - before.ru_stime),
        "minor_page_faults": max(0, after.ru_minflt - before.ru_minflt),
        "major_page_faults": max(0, after.ru_majflt - before.ru_majflt),
        "voluntary_context_switches": max(0, after.ru_nvcsw - before.ru_nvcsw),
        "involuntary_context_switches": max(0, after.ru_nivcsw - before.ru_nivcsw),
        "children_maxrss_after": after.ru_maxrss,
    }


def run_imager(
    contract: dict[str, Any],
    *,
    build: dict[str, Any],
    run_root: pathlib.Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    output = run_root / "rust"
    log_path = run_root / "casa-rs.log"
    command = runtime_command(
        contract, binary=pathlib.Path(build["binary"]), output=output
    )
    environment = runtime_environment(contract, home=os.environ["HOME"])
    sampler = DarwinHostTelemetrySampler(interval_seconds=5.0)
    sampler.start()
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    timed_started = time.monotonic()
    timed_out = False
    process: subprocess.Popen[str] | None = None
    try:
        with log_path.open("w", encoding="utf-8") as log:
            process = subprocess.Popen(
                command,
                cwd=run_root,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            sampler.attach_targets(
                process_pid=process.pid, spill_volume_path="/Volumes/GLENDENNING"
            )
            try:
                exit_code = process.wait(
                    timeout=float(contract["metric"]["max_process_wall_seconds"])
                )
            except subprocess.TimeoutExpired:
                timed_out = True
                process.terminate()
                try:
                    exit_code = process.wait(timeout=30.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    exit_code = process.wait()
    finally:
        telemetry = sampler.stop()
    process_wall_seconds = time.monotonic() - timed_started
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    if process is None:
        raise ContractError("release casars-imager could not be started")
    process_receipt = {
        "command": command,
        "environment": environment,
        "pid": process.pid,
        "exit_code": exit_code,
        "timed_out": timed_out,
        "wall_seconds": process_wall_seconds,
        "rusage": rusage_delta(before, after),
        "log": str(log_path),
        "log_sha256": sha256_file(log_path),
        "output_prefix": str(output),
    }
    if timed_out:
        atomic_write_json(
            run_root / "runtime-failure.json",
            {
                "failure": "timeout",
                "process": process_receipt,
                "host_telemetry": telemetry,
            },
        )
        raise ContractError(
            f"VLASS proxy exceeded {contract['metric']['max_process_wall_seconds']} s"
        )
    if exit_code != 0:
        atomic_write_json(
            run_root / "runtime-failure.json",
            {
                "failure": "nonzero_exit",
                "process": process_receipt,
                "host_telemetry": telemetry,
            },
        )
        raise ContractError(f"release casars-imager exited {exit_code}; see {log_path}")
    runtime = parse_runtime_log(log_path.read_text(encoding="utf-8"))
    return process_receipt, telemetry, runtime


def compare_to_baseline(
    contract: dict[str, Any],
    *,
    output_prefix: pathlib.Path,
    run_root: pathlib.Path,
) -> dict[str, Any] | None:
    baseline = contract["baseline"]
    if baseline["status"] == "qualification":
        return None
    if baseline["status"] != "frozen":
        raise ContractError(f"unsupported baseline status {baseline['status']!r}")
    baseline_receipt_path = pathlib.Path(baseline["receipt"])
    if sha256_file(baseline_receipt_path) != baseline["receipt_sha256"]:
        raise ContractError("frozen baseline receipt identity changed")
    baseline_receipt = load_json_object(
        baseline_receipt_path, label="frozen proxy baseline receipt"
    )
    baseline_prefix = pathlib.Path(baseline["output_prefix"])
    if baseline_receipt.get("process", {}).get("output_prefix") != str(baseline_prefix):
        raise ContractError("frozen baseline output prefix is not receipt-bound")
    os.environ.setdefault("CASASITECONFIG", str(CASA_SITE_CONFIG))
    return compare_products(
        casa_python=contract["comparison"]["casa_python"],
        request=comparison_request(
            contract,
            candidate_prefix=output_prefix,
            baseline_prefix=baseline_prefix,
            run_root=run_root,
        ),
        artifact_prefix=run_root / "proxy",
        cwd=run_root,
    )


def new_run_root(contract: dict[str, Any]) -> pathlib.Path:
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"{timestamp}-{uuid.uuid4().hex[:12]}"
    run_root = pathlib.Path(contract["runs_root"]) / run_id
    run_root.mkdir(parents=True, exist_ok=False)
    return run_root


def measure(contract_path: pathlib.Path) -> int:
    contract = load_contract(contract_path)
    run_root = new_run_root(contract)
    try:
        inputs = validate_inputs(contract)
        contract_sha256 = sha256_file(contract_path)
        build = build_release(contract, run_root)
        source = source_state()
        process, telemetry, runtime = run_imager(
            contract, build=build, run_root=run_root
        )
        output_prefix = pathlib.Path(process["output_prefix"])
        outputs = output_identities(contract, output_prefix)
        comparison = compare_to_baseline(
            contract, output_prefix=output_prefix, run_root=run_root
        )
        receipt = {
            "schema_version": 1,
            "workload_id": contract["workload_id"],
            "run_id": run_root.name,
            "recorded_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
            "contract": {
                "path": str(contract_path),
                "sha256": contract_sha256,
            },
            "source": source,
            "build": build,
            "inputs": {
                key: value
                for key, value in inputs.items()
                if key != "selection_accounting"
            },
            "selection": selection_receipt(contract, inputs),
            "process": process,
            "host_telemetry": telemetry,
            "runtime": runtime,
            "outputs": outputs,
            "comparison": comparison,
        }
        receipt_path = run_root / "receipt.json"
        atomic_write_json(receipt_path, receipt)
        receipt_sha256 = sha256_file(receipt_path)
        latest_path = pathlib.Path(contract["runs_root"]).parent / "latest.json"
        atomic_write_json(
            latest_path,
            {
                "schema_version": 1,
                "workload_id": contract["workload_id"],
                "receipt": str(receipt_path),
                "receipt_sha256": receipt_sha256,
            },
        )
        print(
            json.dumps(
                {
                    "metric": runtime["metric"]["seconds"],
                    "receipt": str(receipt_path),
                    "receipt_sha256": receipt_sha256,
                    "release_binary_sha256": build["binary_sha256"],
                    "guard_candidate": True,
                },
                sort_keys=True,
            )
        )
        return 0
    except Exception as error:
        failure_path = run_root / "measure-failure.json"
        atomic_write_json(
            failure_path,
            {
                "schema_version": 1,
                "workload_id": contract.get("workload_id"),
                "error_type": type(error).__name__,
                "error": str(error),
            },
        )
        print(
            json.dumps(
                {
                    "error": str(error),
                    "failure": str(failure_path),
                    "guard_candidate": False,
                },
                sort_keys=True,
            )
        )
        return 2


def guard(contract_path: pathlib.Path, receipt_path: pathlib.Path | None) -> int:
    contract = load_contract(contract_path)
    if receipt_path is None:
        latest_path = pathlib.Path(contract["runs_root"]).parent / "latest.json"
        latest = load_json_object(latest_path, label="latest VLASS proxy pointer")
        receipt_path = pathlib.Path(latest["receipt"])
        expected_receipt_sha256 = latest["receipt_sha256"]
    else:
        expected_receipt_sha256 = sha256_file(receipt_path)
    actual_receipt_sha256 = sha256_file(receipt_path)
    receipt = load_json_object(receipt_path, label="VLASS proxy receipt")
    current_source = source_state()
    errors = evaluate_receipt(
        contract,
        receipt,
        expected_receipt_sha256=expected_receipt_sha256,
        actual_receipt_sha256=actual_receipt_sha256,
        current_source_state_sha256=current_source["state_sha256"],
    )
    binary_path = pathlib.Path(str(receipt.get("build", {}).get("binary", "")))
    if not binary_path.is_file():
        errors.append("receipt-bound release executable is missing")
    elif sha256_file(binary_path) != receipt["build"]["binary_sha256"]:
        errors.append("receipt-bound release executable identity changed")
    log_path = pathlib.Path(str(receipt.get("process", {}).get("log", "")))
    if not log_path.is_file():
        errors.append("receipt-bound runtime log is missing")
    elif sha256_file(log_path) != receipt["process"]["log_sha256"]:
        errors.append("receipt-bound runtime log identity changed")
    result = {
        "guard": not errors,
        "metric": receipt.get("runtime", {}).get("metric", {}).get("seconds"),
        "receipt": str(receipt_path),
        "receipt_sha256": actual_receipt_sha256,
        "release_binary_sha256": receipt.get("build", {}).get("binary_sha256"),
        "errors": errors,
    }
    print(json.dumps(result, sort_keys=True))
    return 0 if not errors else 1


def extract_json_stdout(stdout: str) -> dict[str, Any]:
    start = stdout.find("{")
    end = stdout.rfind("}")
    if start < 0 or end < start:
        raise ContractError("MS selection probe did not emit JSON")
    value = json.loads(stdout[start : end + 1])
    if not isinstance(value, dict):
        raise ContractError("MS selection probe output is not an object")
    return value


def freeze_selection(contract_path: pathlib.Path, output: pathlib.Path) -> int:
    contract = load_contract(contract_path)
    if output.exists() or output.is_symlink():
        raise ContractError(f"refusing to overwrite selection accounting: {output}")
    casa_python = contract["comparison"]["casa_python"]
    environment = os.environ.copy()
    environment["CASASITECONFIG"] = str(CASA_SITE_CONFIG)
    by_spw = {}
    logs = {}
    for spw in contract["workload"]["spw_ids"]:
        command = [
            casa_python,
            str(SELECTION_PROBE),
            "--ms",
            contract["dataset"]["measurement_set"],
            "--field",
            contract["workload"]["field_selection"],
            "--spw",
            str(spw),
            "--specmode",
            "mfs",
            "--channel-start",
            str(contract["workload"]["channel_start"]),
            "--channel-count",
            str(contract["workload"]["channel_count"]),
        ]
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            env=environment,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise ContractError(
                f"MS selection accounting failed for SPW {spw}: {completed.stderr}"
            )
        by_spw[str(spw)] = extract_json_stdout(completed.stdout)
        logs[str(spw)] = {
            "command": command,
            "stderr": completed.stderr,
        }
    value = {
        "schema_version": 1,
        "workload_id": contract["workload_id"],
        "measurement_set": contract["dataset"]["measurement_set"],
        "measurement_set_tree_sha256": contract["dataset"][
            "measurement_set_tree_sha256"
        ],
        "field_ids": contract["workload"]["field_ids"],
        "spw_ids": contract["workload"]["spw_ids"],
        "channel_start": contract["workload"]["channel_start"],
        "channel_count": contract["workload"]["channel_count"],
        "by_spw": by_spw,
        "probe_logs": logs,
    }
    atomic_write_json(output, value)
    digest = sha256_file(output)
    print(json.dumps({"output": str(output), "sha256": digest}, sort_keys=True))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=pathlib.Path, default=DEFAULT_CONTRACT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("measure")
    guard_parser = subparsers.add_parser("guard")
    guard_parser.add_argument("--receipt", type=pathlib.Path)
    freeze_parser = subparsers.add_parser("freeze-selection")
    freeze_parser.add_argument(
        "--output",
        type=pathlib.Path,
        default=SCRIPT_DIR / "vlass_5m_selection_accounting.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "measure":
            return measure(args.contract.resolve())
        if args.command == "guard":
            return guard(args.contract.resolve(), args.receipt)
        if args.command == "freeze-selection":
            return freeze_selection(args.contract.resolve(), args.output.resolve())
    except (
        ContractError,
        OSError,
        subprocess.SubprocessError,
        json.JSONDecodeError,
    ) as error:
        print(json.dumps({"error": str(error), "guard": False}, sort_keys=True))
        return 2
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
