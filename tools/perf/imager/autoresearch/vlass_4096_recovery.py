#!/usr/bin/env python3
"""Measure real 4096-square VLASS CLEAN and guard single/all-field parity."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
IMAGER_TOOLS = SCRIPT_DIR.parent
REPO_ROOT = SCRIPT_DIR.parents[3]
DEFAULT_CONTRACT = SCRIPT_DIR / "vlass_4096_recovery_contract.json"
CONTROLLER_RESULTS = "autoresearch-results"
WALL_RE = re.compile(r"^real (?P<seconds>[0-9]+(?:\.[0-9]+)?)$")
COMPLETION_RE = re.compile(
    r"^Wrote CASA-compatible products at prefix (?P<prefix>.+) "
    r"\((?P<samples>[0-9]+) gridded samples, "
    r"(?P<major>[0-9]+) major cycles, "
    r"(?P<minor>[0-9]+) minor iterations, stop=(?P<stop>.+)\)$"
)
KV_RE = re.compile(r"(?P<key>[A-Za-z0-9_]+)=(?P<value>\S+)")

sys.path.insert(0, str(IMAGER_TOOLS))
import vlass_landmark_guard  # noqa: E402


class ContractError(RuntimeError):
    """The performance experiment contract was not satisfied."""


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ContractError(f"{path} must contain a JSON object")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def non_controller_status() -> list[str]:
    lines = git("status", "--short", "--untracked-files=all").splitlines()
    return [
        line
        for line in lines
        if line[3:] != CONTROLLER_RESULTS
        and not line[3:].startswith(f"{CONTROLLER_RESULTS}/")
    ]


def validate_contract(contract: dict[str, Any]) -> None:
    if contract.get("schema_version") != 1:
        raise ContractError("unsupported VLASS 4096 recovery contract")
    single = contract.get("single_field")
    all_fields = contract.get("all_fields")
    if not isinstance(single, dict) or not isinstance(all_fields, dict):
        raise ContractError("single-field and all-field contracts are required")
    if float(single["matched_casa_wall_seconds"]) / 100.0 != float(
        single["target_wall_seconds"]
    ):
        raise ContractError("single-field target must equal the exact 100x CASA wall")
    if (
        float(all_fields["maximum_wall_seconds"])
        != float(all_fields["baseline_wall_seconds"]) * 1.05
    ):
        raise ContractError("all-field guard must be exactly 5% above its baseline")
    if int(all_fields["segment_target_bytes"]) != 512 * 1024 * 1024:
        raise ContractError("all-field guard must force 512 MiB grouped segments")


def validate_inputs(contract: dict[str, Any]) -> None:
    required = [
        Path(contract["casa_python"]),
        Path(contract["fftw_library_dir"]),
        Path(contract["measures_dir"]),
        REPO_ROOT / contract["scientific_contract"],
    ]
    for row in (contract["single_field"], contract["all_fields"]):
        required.extend(
            [
                REPO_ROOT / row["runner"],
                REPO_ROOT / row["workload"],
                Path(f"{row['casa_prefix']}.image.tt0"),
            ]
        )
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise ContractError("missing frozen experiment inputs: " + ", ".join(missing))


def build_release(run_dir: Path) -> tuple[Path, dict[str, Any]]:
    if non_controller_status():
        raise ContractError("source tree must be clean outside autoresearch-results")
    head = git("rev-parse", "HEAD")
    command = [
        "cargo",
        "build",
        "--locked",
        "--release",
        "-p",
        "casars-imager",
        "--bin",
        "casars-imager",
    ]
    environment = os.environ.copy()
    environment["CARGO_INCREMENTAL"] = "0"
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )
    build_log = run_dir / "release-build.log"
    build_log.write_text(
        (completed.stdout or "") + (completed.stderr or ""), encoding="utf-8"
    )
    if completed.returncode != 0:
        raise ContractError(f"release build failed; see {build_log}")
    if git("rev-parse", "HEAD") != head or non_controller_status():
        raise ContractError("source state changed during the release build")
    binary = REPO_ROOT / "target/release/casars-imager"
    if not binary.is_file():
        raise ContractError("release build did not produce casars-imager")
    return binary, {
        "command": command,
        "head": head,
        "binary": str(binary),
        "binary_sha256": sha256_file(binary),
        "log": str(build_log),
        "timed_build_seconds": 0.0,
    }


def parse_wall(log_path: Path) -> float:
    matches = []
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if match := WALL_RE.fullmatch(line):
            matches.append(float(match.group("seconds")))
    if len(matches) != 1 or matches[0] <= 0.0:
        raise ContractError(f"{log_path} must contain exactly one positive real line")
    return matches[0]


def run_checked(
    command: list[str],
    *,
    environment: dict[str, str],
    log_path: Path,
    accepted_returncodes: tuple[int, ...] = (0,),
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )
    log_path.write_text(
        (completed.stdout or "") + (completed.stderr or ""), encoding="utf-8"
    )
    if completed.returncode not in accepted_returncodes:
        raise ContractError(
            f"command exited {completed.returncode}; see {log_path}: {' '.join(command)}"
        )
    return completed


def base_environment(contract: dict[str, Any], binary: Path) -> dict[str, str]:
    return {
        "HOME": os.environ["HOME"],
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
        "CASA_RS_VLASS_EXPERIMENT_ROOT": contract["experiment_root"],
        "CASA_RS_VLASS_EXPERIMENT_BINARY": str(binary),
        "CASA_RS_VLASS_FFTW_LIBRARY_DIR": contract["fftw_library_dir"],
        "CASA_RS_VLASS_MEASURES_DIR": contract["measures_dir"],
    }


def single_output_prefix(contract: dict[str, Any], label: str) -> Path:
    return Path(contract["experiment_root"]) / "artifacts/products" / label / "rust"


def run_single(
    contract: dict[str, Any], binary: Path, run_dir: Path, token: str
) -> dict[str, Any]:
    row = contract["single_field"]
    label = f"autoresearch-single-4096-{git('rev-parse', '--short=12', 'HEAD')}-{token}"
    receipt_date = dt.datetime.now(dt.UTC).strftime("%Y%m%d")
    environment = base_environment(contract, binary)
    environment.update(
        {
            "CASA_RS_VLASS_RECEIPT_DATE": receipt_date,
            "CASA_RS_VLASS_LABEL_OVERRIDE": label,
            "CASA_RS_VLASS_NITER": "2000",
            "CASA_RS_VLASS_FFTW_THREADS": "8",
            "CASA_RS_VLASS_GRID_THREADS": "2",
            "CASA_RS_VLASS_MODEL_FFT_THREADS": "8",
            "CASA_RS_VLASS_STANDARD_MFS_ACCELERATION": "metal",
            "CASA_RS_VLASS_IMAGE_RESPONSE_CACHE": "1",
            "CASA_RS_VLASS_MODEL_DELTA_CENSUS": "1",
            "CASA_RS_VLASS_RADIX_MADFM": "1",
            "CASA_RS_VLASS_CACHE_REFRESHED_NSIGMA": "1",
            "CASA_RS_VLASS_SPARSE_MASK_PEAK_SEARCH": "1",
            "CASA_RS_VLASS_PARALLEL_MODEL_TERM_FFT": "1",
            "CASA_RS_VLASS_SPARSE_MODEL_PREP": "1",
        }
    )
    wrapper_log = run_dir / "single-runner.log"
    completed = run_checked(
        ["bash", str(REPO_ROOT / row["runner"])],
        environment=environment,
        log_path=wrapper_log,
    )
    emitted = [line for line in completed.stdout.splitlines() if line.strip()]
    if len(emitted) != 1:
        raise ContractError("single-field runner must print exactly one log path")
    runtime_log = Path(emitted[0])
    wall = parse_wall(runtime_log)
    landmark = vlass_landmark_guard.find_landmark(
        load_object(REPO_ROOT / "tools/perf/imager/vlass_recovery_contract.json"),
        "VLASS-LANDMARK-SINGLE-4096-4SPW-CLEAN-N2000-v1",
    )
    runtime = vlass_landmark_guard.parse_log(runtime_log.read_text(encoding="utf-8"))
    errors = vlass_landmark_guard.evaluate(
        landmark, runtime, binary=binary, wall_seconds=wall
    )
    errors = [error for error in errors if not error.startswith("wall time ")]
    if errors:
        raise ContractError("single-field activity guard failed: " + "; ".join(errors))
    return {
        "wall_seconds": wall,
        "runtime_log": str(runtime_log),
        "runtime_log_sha256": sha256_file(runtime_log),
        "output_prefix": str(single_output_prefix(contract, label)),
        "activity": runtime,
    }


def parse_key_values(line: str) -> dict[str, str]:
    return {match.group("key"): match.group("value") for match in KV_RE.finditer(line)}


def validate_all_field_log(contract: dict[str, Any], log_path: Path) -> dict[str, Any]:
    row = contract["all_fields"]
    lines = log_path.read_text(encoding="utf-8").splitlines()
    completions = [COMPLETION_RE.fullmatch(line) for line in lines]
    completions = [match for match in completions if match is not None]
    if len(completions) != 1:
        raise ContractError("all-field log must contain exactly one completion")
    completion = completions[0].groupdict()
    expected = {
        "samples": str(row["gridded_samples"]),
        "major": str(row["major_cycles"]),
        "minor": str(row["minor_iterations"]),
    }
    for key, value in expected.items():
        if completion[key] != value:
            raise ContractError(f"all-field completion {key} changed")
    if "NsigmaThresholdReached" not in completion["stop"]:
        raise ContractError("all-field CLEAN no longer stops at the n-sigma threshold")
    required_fragments = [
        f"ddids={row['ddids']}",
        "selected_channels=64",
        "component=POINTING index",
        f"awproject_selected_field_count selected_fields={row['selected_fields']}",
        "architecture=source-order-grouped-tile-v1",
        f"segment_target_bytes={row['segment_target_bytes']}",
        "omitted_squared_l2_energy=0.000000000e0",
        "usepointing=true",
    ]
    for fragment in required_fragments:
        if not any(fragment in line for line in lines):
            raise ContractError(f"all-field runtime contract is missing {fragment!r}")
    summaries = [
        parse_key_values(line)
        for line in lines
        if line.startswith("awproject_metal_resident_grouped_replay_summary ")
    ]
    if not summaries:
        raise ContractError("all-field grouped resident replay was not exercised")
    for summary in summaries:
        for key in (
            "spill_read_bytes",
            "runtime_grouping_builds",
            "runtime_sort_builds",
            "runtime_route_builds",
        ):
            if summary.get(key) != "0":
                raise ContractError(f"all-field grouped replay changed {key}")
    if any(
        "swapout_bytes_delta=" in line and "swapout_bytes_delta=0" not in line
        for line in lines
    ):
        raise ContractError("all-field run recorded swapout activity")
    receipts = [
        parse_key_values(line)
        for line in lines
        if line.startswith("awproject_aot_grouped_tile_receipt ")
    ]
    if not receipts or any(
        receipt.get("omitted_energy_fraction_bits") != "0" for receipt in receipts
    ):
        raise ContractError("all-field AOT replay is not exact support")
    return {
        "completion": completion,
        "segments": len(receipts),
        "refreshes": len(summaries),
    }


def run_all_fields(
    contract: dict[str, Any], binary: Path, run_dir: Path, token: str
) -> dict[str, Any]:
    row = contract["all_fields"]
    all_run = run_dir / f"all63-{token}"
    environment = base_environment(contract, binary)
    environment.update(
        {
            "CASA_RS_VLASS_RUN_ID": all_run.name,
            "CASA_RS_VLASS_RUN_ROOT": str(all_run),
            "CASA_RS_VLASS_SELECTED_EXACT_HYBRID": "1",
            "CASA_RS_VLASS_WINDOWED_HYBRID_CLEAN": "0",
            "CASA_RS_VLASS_GROUPED_SEGMENT_TARGET_BYTES": str(
                row["segment_target_bytes"]
            ),
            "CASA_RS_VLASS_GRID_THREADS": "2",
            "CASA_RS_VLASS_STANDARD_MFS_ACCELERATION": "metal",
            "CASA_RS_VLASS_NITER": "2000",
        }
    )
    completed = run_checked(
        ["bash", str(REPO_ROOT / row["runner"])],
        environment=environment,
        log_path=run_dir / "all-field-runner.log",
    )
    emitted = [line for line in completed.stdout.splitlines() if line.strip()]
    if emitted != [str(all_run)]:
        raise ContractError("all-field runner did not return its exact run root")
    runtime_log = all_run / "casa-rs.log"
    wall = parse_wall(runtime_log)
    if wall > float(row["maximum_wall_seconds"]):
        raise ContractError(
            f"all-field wall {wall:.6f}s exceeds {row['maximum_wall_seconds']:.6f}s"
        )
    runtime = validate_all_field_log(contract, runtime_log)
    return {
        "wall_seconds": wall,
        "runtime_log": str(runtime_log),
        "runtime_log_sha256": sha256_file(runtime_log),
        "output_prefix": str(all_run / "rust"),
        "activity": runtime,
    }


def compare_row(
    contract: dict[str, Any], row: dict[str, Any], prefix: Path, artifact: Path
) -> dict[str, Any]:
    compare_log = artifact.with_suffix(".driver.log")
    run_checked(
        [
            sys.executable,
            str(IMAGER_TOOLS / "experiments/vlass_compare_frozen_products.py"),
            str(REPO_ROOT / row["workload"]),
            str(prefix),
            row["casa_prefix"],
            str(artifact),
            "--casa-python",
            contract["casa_python"],
        ],
        environment=os.environ.copy(),
        log_path=compare_log,
        # The raw comparator intentionally exits one for bounded mask-only
        # topology differences. The immutable v2 reassessment below is the
        # authoritative scientific gate and fails closed on every other case.
        accepted_returncodes=(0, 1),
    )
    source_request = artifact.with_suffix(".comparison-input.json")
    source_output = artifact.with_suffix(".comparison.json")
    validation = artifact.with_suffix(".validation.json")
    run_checked(
        [
            sys.executable,
            str(IMAGER_TOOLS / "experiments/vlass_reassess_frozen_comparison.py"),
            "--source-request",
            str(source_request),
            "--source-output",
            str(source_output),
            "--contract",
            str(REPO_ROOT / contract["scientific_contract"]),
            "--output",
            str(validation),
        ],
        environment=os.environ.copy(),
        log_path=artifact.with_suffix(".validation.log"),
    )
    receipt = load_object(validation)
    if receipt.get("status") != "completed":
        raise ContractError(f"scientific comparison failed: {validation}")
    return {
        "validation": str(validation),
        "validation_sha256": sha256_file(validation),
        "status": receipt["status"],
    }


def run_token() -> str:
    return (
        dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    )


def latest_path(contract: dict[str, Any]) -> Path:
    return Path(contract["run_root"]) / "latest-measurement.json"


def measure(contract: dict[str, Any]) -> float:
    token = run_token()
    run_dir = Path(contract["run_root"]) / "runs" / token
    run_dir.mkdir(parents=True, exist_ok=False)
    binary, build = build_release(run_dir)
    single = run_single(contract, binary, run_dir, token[-8:])
    receipt = {
        "schema_version": 1,
        "token": token,
        "run_dir": str(run_dir),
        "source_head": build["head"],
        "build": build,
        "single_field": single,
    }
    measurement = run_dir / "measurement.json"
    measurement.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    pointer = latest_path(contract)
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text(
        json.dumps(
            {
                "measurement": str(measurement),
                "measurement_sha256": sha256_file(measurement),
                "source_head": build["head"],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return float(single["wall_seconds"])


def guard(contract: dict[str, Any]) -> None:
    pointer = load_object(latest_path(contract))
    if pointer.get("source_head") != git("rev-parse", "HEAD"):
        raise ContractError("latest measurement belongs to a different source commit")
    measurement_path = Path(pointer["measurement"])
    if sha256_file(measurement_path) != pointer["measurement_sha256"]:
        raise ContractError("latest measurement receipt hash changed")
    measurement = load_object(measurement_path)
    run_dir = Path(measurement["run_dir"])
    binary = Path(measurement["build"]["binary"])
    if sha256_file(binary) != measurement["build"]["binary_sha256"]:
        raise ContractError("release binary changed between measurement and guard")
    single_comparison = compare_row(
        contract,
        contract["single_field"],
        Path(measurement["single_field"]["output_prefix"]),
        run_dir / "single-casa",
    )
    all_fields = run_all_fields(contract, binary, run_dir, measurement["token"][-8:])
    all_comparison = compare_row(
        contract,
        contract["all_fields"],
        Path(all_fields["output_prefix"]),
        run_dir / "all63-casa",
    )
    guard_receipt = {
        "schema_version": 1,
        "status": "passed",
        "source_head": measurement["source_head"],
        "binary_sha256": measurement["build"]["binary_sha256"],
        "single_field": {
            **measurement["single_field"],
            "comparison": single_comparison,
        },
        "all_fields": {**all_fields, "comparison": all_comparison},
    }
    output = run_dir / "guard.json"
    output.write_text(json.dumps(guard_receipt, indent=2, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("measure", "guard"))
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    args = parser.parse_args()
    try:
        contract = load_object(args.contract)
        validate_contract(contract)
        validate_inputs(contract)
        if args.action == "measure":
            metric = measure(contract)
            print(f"{metric:.9f}")
        else:
            guard(contract)
    except (ContractError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"VLASS 4096 autoresearch error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
