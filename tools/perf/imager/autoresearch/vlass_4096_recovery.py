#!/usr/bin/env python3
"""Measure real 4096-square VLASS CLEAN and guard single/all-field parity."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import time
import uuid
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
IMAGER_TOOLS = SCRIPT_DIR.parent
REPO_ROOT = SCRIPT_DIR.parents[3]
DEFAULT_CONTRACT = SCRIPT_DIR / "vlass_4096_recovery_contract.json"
CONTROLLER_RESULTS = "autoresearch-results"
WALL_RE = re.compile(r"^real (?P<seconds>[0-9]+(?:\.[0-9]+)?)$")
TIME_COUNTER_RE = re.compile(
    r"^\s*(?P<value>[0-9]+)\s+"
    r"(?P<name>instructions retired|cycles elapsed)\s*$"
)
PROCESS_TIME_RE = re.compile(r"^(?P<name>user|sys) (?P<seconds>[0-9]+(?:\.[0-9]+)?)$")
CPU_IDLE_RE = re.compile(r"^CPU usage: .* (?P<idle>[0-9]+(?:\.[0-9]+)?)% idle\s*$")
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
    if int(contract["phase_two_single_guard_cooldown_seconds"]) != 120:
        raise ContractError("phase-two single-field guard cooldown must be 120 seconds")
    host_idle = contract.get("host_idle")
    if not isinstance(host_idle, dict):
        raise ContractError("host-idle precondition is required")
    if not 0.0 < float(host_idle["minimum_idle_cpu_percent"]) < 100.0:
        raise ContractError("host-idle threshold must be between zero and 100 percent")
    if (
        min(
            int(host_idle["consecutive_samples"]),
            int(host_idle["poll_interval_seconds"]),
            int(host_idle["timeout_seconds"]),
        )
        <= 0
    ):
        raise ContractError("host-idle sampling controls must be positive")
    if int(single["warmup_runs"]) != 1 or int(single["timed_repetitions"]) != 3:
        raise ContractError(
            "single-field series must use one warmup and three timed runs"
        )
    if int(single["inter_run_quiescence_seconds"]) != 60:
        raise ContractError("single-field inter-run quiescence must be 60 seconds")
    if float(single["matched_casa_wall_seconds"]) / 100.0 != float(
        single["target_wall_seconds"]
    ):
        raise ContractError("single-field target must equal the exact 100x CASA wall")
    if not math.isclose(
        float(single["stability_maximum_wall_seconds"]),
        float(single["target_wall_seconds"]) * 1.05,
        rel_tol=0.0,
        abs_tol=1.0e-12,
    ):
        raise ContractError("single-field stability ceiling must be 5% above target")
    if float(all_fields["matched_casa_wall_seconds"]) / 100.0 != float(
        all_fields["target_wall_seconds"]
    ):
        raise ContractError("all-field target must equal the exact 100x CASA wall")
    if (
        float(all_fields["sequential_guard_maximum_wall_seconds"])
        != float(all_fields["sequential_guard_baseline_wall_seconds"]) * 1.05
    ):
        raise ContractError(
            "all-field sequential guard must be exactly 5% above its baseline"
        )
    for baseline, maximum in (
        ("cold_instructions_retired", "maximum_instructions_retired"),
        ("cold_cycles_elapsed", "maximum_cycles_elapsed"),
    ):
        if int(all_fields[maximum]) != int(int(all_fields[baseline]) * 1.05):
            raise ContractError(f"all-field {maximum} must be 5% above {baseline}")
    if int(all_fields["segment_target_bytes"]) != 512 * 1024 * 1024:
        raise ContractError("all-field guard must force 512 MiB grouped segments")
    if int(all_fields["requested_grid_threads"]) != 7:
        raise ContractError(
            "all-field benchmark must request seven sparse-grid workers"
        )


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


def parse_time_counters(log_path: Path) -> dict[str, int]:
    matches: dict[str, list[int]] = {
        "instructions_retired": [],
        "cycles_elapsed": [],
    }
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if match := TIME_COUNTER_RE.fullmatch(line):
            key = match.group("name").replace(" ", "_")
            matches[key].append(int(match.group("value")))
    result: dict[str, int] = {}
    for key, values in matches.items():
        if len(values) != 1 or values[0] <= 0:
            raise ContractError(f"{log_path} must contain exactly one positive {key}")
        result[key] = values[0]
    return result


def parse_process_times(log_path: Path) -> dict[str, float]:
    matches: dict[str, list[float]] = {"user_seconds": [], "sys_seconds": []}
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if match := PROCESS_TIME_RE.fullmatch(line):
            matches[f"{match.group('name')}_seconds"].append(
                float(match.group("seconds"))
            )
    result: dict[str, float] = {}
    for key, values in matches.items():
        if len(values) != 1 or values[0] < 0.0:
            raise ContractError(
                f"{log_path} must contain exactly one nonnegative {key}"
            )
        result[key] = values[0]
    return result


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
        ["/usr/bin/time", "-lp", "bash", str(REPO_ROOT / row["runner"])],
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
        "outer_time_counters": parse_time_counters(wrapper_log),
        "outer_process_times": parse_process_times(wrapper_log),
    }


def assert_no_competing_imager() -> None:
    completed = subprocess.run(
        ["pgrep", "-x", "casars-imager"],
        capture_output=True,
        text=True,
    )
    if completed.returncode == 0 and completed.stdout.strip():
        raise ContractError("another casars-imager process is running")
    if completed.returncode not in (0, 1):
        raise ContractError("could not check for a competing casars-imager process")


def sample_host_idle_cpu_percent() -> float:
    completed = subprocess.run(
        ["top", "-l", "2", "-s", "1", "-n", "0"],
        check=True,
        capture_output=True,
        text=True,
    )
    samples = [
        float(match.group("idle"))
        for line in completed.stdout.splitlines()
        if (match := CPU_IDLE_RE.fullmatch(line)) is not None
    ]
    if len(samples) < 2:
        raise ContractError("host-idle precondition did not receive two CPU samples")
    return samples[-1]


def wait_for_host_idle(contract: dict[str, Any]) -> dict[str, Any]:
    policy = contract["host_idle"]
    minimum = float(policy["minimum_idle_cpu_percent"])
    consecutive_required = int(policy["consecutive_samples"])
    poll_seconds = int(policy["poll_interval_seconds"])
    attempts = 1 + int(policy["timeout_seconds"]) // poll_seconds
    consecutive = 0
    observations: list[float] = []
    for attempt in range(attempts):
        assert_no_competing_imager()
        idle = sample_host_idle_cpu_percent()
        observations.append(idle)
        consecutive = consecutive + 1 if idle >= minimum else 0
        if consecutive >= consecutive_required:
            return {
                "status": "idle",
                "minimum_idle_cpu_percent": minimum,
                "observed_idle_cpu_percent": observations,
            }
        if attempt + 1 < attempts:
            time.sleep(poll_seconds)
    raise ContractError(
        "host remained busy before imaging: "
        f"required {consecutive_required} consecutive samples at >= {minimum:.1f}% idle; "
        f"observed {observations}"
    )


def quiesce(contract: dict[str, Any], seconds: int) -> dict[str, Any]:
    subprocess.run(["sync"], check=True)
    time.sleep(seconds)
    return wait_for_host_idle(contract)


def validate_single_series(
    contract: dict[str, Any], timed: list[dict[str, Any]]
) -> dict[str, Any]:
    row = contract["single_field"]
    if len(timed) != int(row["timed_repetitions"]):
        raise ContractError("single-field timed series cardinality changed")
    walls = [float(run["wall_seconds"]) for run in timed]
    median_wall = float(statistics.median(walls))
    maximum_wall = max(walls)
    if maximum_wall > float(row["stability_maximum_wall_seconds"]):
        raise ContractError(
            f"single-field maximum wall {maximum_wall:.6f}s exceeds "
            f"{row['stability_maximum_wall_seconds']:.6f}s"
        )
    # macOS /usr/bin/time reports child RSS and CPU time, but its hardware
    # counters describe the wrapper process rather than the imager child.
    # Preserve them as diagnostics; do not misclassify them as workload gates.
    user_values = [float(run["outer_process_times"]["user_seconds"]) for run in timed]
    median_user = float(statistics.median(user_values))
    if any(abs(value - median_user) > median_user * 0.05 for value in user_values):
        raise ContractError("single-field user CPU series is unstable")
    median_index = sorted(range(len(timed)), key=lambda index: walls[index])[1]
    return {
        "median_wall_seconds": median_wall,
        "maximum_wall_seconds": maximum_wall,
        "median_run_index": median_index,
        "walls_seconds": walls,
    }


def load_reusable_single_landmark(
    contract: dict[str, Any], binary: Path
) -> dict[str, Any] | None:
    landmark = contract.get("single_field_landmark")
    if not isinstance(landmark, dict):
        return None
    binary_sha256 = sha256_file(binary)
    if landmark.get("binary_sha256") != binary_sha256:
        return None
    timed = []
    activity_contract = vlass_landmark_guard.find_landmark(
        load_object(REPO_ROOT / "tools/perf/imager/vlass_recovery_contract.json"),
        "VLASS-LANDMARK-SINGLE-4096-4SPW-CLEAN-N2000-v1",
    )
    for recorded in landmark.get("timed", []):
        runtime_log = Path(recorded["runtime_log"])
        if sha256_file(runtime_log) != recorded["runtime_log_sha256"]:
            raise ContractError(f"single-field landmark log changed: {runtime_log}")
        wall = parse_wall(runtime_log)
        if wall != float(recorded["wall_seconds"]):
            raise ContractError(f"single-field landmark wall changed: {runtime_log}")
        activity = vlass_landmark_guard.parse_log(
            runtime_log.read_text(encoding="utf-8")
        )
        errors = vlass_landmark_guard.evaluate(
            activity_contract, activity, binary=binary, wall_seconds=wall
        )
        errors = [error for error in errors if not error.startswith("wall time ")]
        if errors:
            raise ContractError(
                "single-field landmark activity failed: " + "; ".join(errors)
            )
        output_prefix = Path(recorded["output_prefix"])
        if not Path(f"{output_prefix}.image.tt0").is_dir():
            raise ContractError(
                f"single-field landmark products are missing: {output_prefix}"
            )
        timed.append(
            {
                **recorded,
                "activity": activity,
                "outer_process_times": parse_process_times(runtime_log),
            }
        )
    summary = validate_single_series(contract, timed)
    validation = Path(landmark["validation"])
    if sha256_file(validation) != landmark["validation_sha256"]:
        raise ContractError("single-field landmark validation receipt changed")
    validation_receipt = load_object(validation)
    if validation_receipt.get("status") != "completed":
        raise ContractError("single-field landmark validation is not completed")
    return {
        "status": "reused-identical-release-binary",
        "binary_sha256": binary_sha256,
        "timed": timed,
        "summary": summary,
        "comparison": {
            "validation": str(validation),
            "validation_sha256": landmark["validation_sha256"],
            "status": "completed",
        },
    }


def load_reusable_all_fields_landmark(
    contract: dict[str, Any], binary: Path
) -> dict[str, Any] | None:
    landmark = contract.get("all_fields_landmark")
    if not isinstance(landmark, dict):
        return None
    binary_sha256 = sha256_file(binary)
    if landmark.get("binary_sha256") != binary_sha256:
        return None
    measurement_path = Path(landmark["measurement"])
    if sha256_file(measurement_path) != landmark["measurement_sha256"]:
        raise ContractError("all-field landmark measurement receipt changed")
    measurement = load_object(measurement_path)
    if measurement.get("build", {}).get("binary_sha256") != binary_sha256:
        raise ContractError("all-field landmark executable identity changed")
    all_fields = measurement.get("all_fields")
    if not isinstance(all_fields, dict):
        raise ContractError("all-field landmark measurement is incomplete")
    runtime_log = Path(all_fields["runtime_log"])
    if sha256_file(runtime_log) != all_fields["runtime_log_sha256"]:
        raise ContractError("all-field landmark runtime log changed")
    activity = validate_all_field_log(contract, runtime_log)
    output_prefix = Path(all_fields["output_prefix"])
    if not Path(f"{output_prefix}.image.tt0").is_dir():
        raise ContractError(f"all-field landmark products are missing: {output_prefix}")
    validation = Path(landmark["validation"])
    if sha256_file(validation) != landmark["validation_sha256"]:
        raise ContractError("all-field landmark validation receipt changed")
    validation_receipt = load_object(validation)
    if validation_receipt.get("status") != "completed":
        raise ContractError("all-field landmark validation is not completed")
    return {
        **all_fields,
        "status": "reused-identical-release-binary",
        "activity": activity,
        "comparison": {
            "validation": str(validation),
            "validation_sha256": landmark["validation_sha256"],
            "status": "completed",
        },
    }


def run_single_series(
    contract: dict[str, Any],
    binary: Path,
    run_dir: Path,
    token: str,
    *,
    initial_cooldown_seconds: int = 0,
) -> dict[str, Any]:
    if initial_cooldown_seconds:
        time.sleep(initial_cooldown_seconds)
    idle_receipts = [wait_for_host_idle(contract)]
    warmup = run_single(contract, binary, run_dir, f"{token}-warmup")
    timed = []
    for index in range(int(contract["single_field"]["timed_repetitions"])):
        idle_receipts.append(
            quiesce(
                contract,
                int(contract["single_field"]["inter_run_quiescence_seconds"]),
            )
        )
        timed.append(run_single(contract, binary, run_dir, f"{token}-timed{index}"))
    return {
        "initial_cooldown_seconds": initial_cooldown_seconds,
        "host_idle_preconditions": idle_receipts,
        "warmup": warmup,
        "timed": timed,
        "summary": validate_single_series(contract, timed),
    }


def parse_key_values(line: str) -> dict[str, str]:
    return {match.group("key"): match.group("value") for match in KV_RE.finditer(line)}


def validate_all_field_log(
    contract: dict[str, Any],
    log_path: Path,
    *,
    enforce_sequential_guard: bool = False,
) -> dict[str, Any]:
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
    execution_plans = [
        parse_key_values(line)
        for line in lines
        if line.startswith("standard_mfs_execution_plan ")
    ]
    if len(execution_plans) != 1:
        raise ContractError("all-field runtime must emit one execution plan")
    workers = int(execution_plans[0].get("workers", "0"))
    if workers < 2 or workers > int(row["requested_grid_threads"]):
        raise ContractError(
            f"all-field effective workers {workers} are outside the requested worker envelope"
        )
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
    raw_sealed = [
        parse_key_values(line)
        for line in lines
        if line.startswith("awproject_raw_fused_metal_admission phase=sealed ")
    ]
    raw_runtime = [
        parse_key_values(line)
        for line in lines
        if line.startswith("awproject_raw_fused_metal_admission phase=runtime ")
    ]
    raw_replays = [
        parse_key_values(line)
        for line in lines
        if line.startswith("awproject_raw_fused_replay ")
    ]
    if receipts:
        if raw_sealed or raw_runtime or raw_replays:
            raise ContractError(
                "all-field replay mixed AOT and raw fused architectures"
            )
        if any(
            receipt.get("omitted_energy_fraction_bits") != "0" for receipt in receipts
        ):
            raise ContractError("all-field AOT replay is not exact support")
        segment_count = len(receipts)
    else:
        if not raw_sealed or not raw_runtime or not raw_replays:
            raise ContractError("all-field raw fused replay receipts are incomplete")
        if any(
            receipt.get("all_fit") != "true" for receipt in raw_sealed + raw_runtime
        ):
            raise ContractError("all-field raw fused Metal admission did not fit")
        if any(
            receipt.get("exact_support") != "true"
            or receipt.get("host_readback_bytes") != "0"
            or receipt.get("architecture") != "raw-source-major-fused-v1"
            for receipt in raw_replays
        ):
            raise ContractError(
                "all-field raw fused replay changed its exact architecture"
            )
        sealed_segments = {int(receipt["segment"]) for receipt in raw_sealed}
        runtime_segments = {int(receipt["segment"]) for receipt in raw_runtime}
        replay_segments = {int(receipt["segment"]) for receipt in raw_replays}
        if sealed_segments != set(range(len(raw_sealed))):
            raise ContractError(
                "all-field raw fused sealed segments are not contiguous"
            )
        if runtime_segments != sealed_segments or replay_segments != sealed_segments:
            raise ContractError("all-field raw fused runtime segment coverage changed")
        segment_count = len(raw_sealed)
        expected_dispatches = segment_count * len(summaries)
        if (
            len(raw_runtime) != expected_dispatches
            or len(raw_replays) != expected_dispatches
        ):
            raise ContractError(
                "all-field raw fused replay dispatch cardinality changed"
            )
    counters = parse_time_counters(log_path)
    if enforce_sequential_guard:
        for key, maximum_key in (
            ("instructions_retired", "maximum_instructions_retired"),
            ("cycles_elapsed", "maximum_cycles_elapsed"),
        ):
            if counters[key] > int(row[maximum_key]):
                raise ContractError(
                    f"all-field {key} {counters[key]} exceeds {row[maximum_key]}"
                )
    return {
        "completion": completion,
        "segments": segment_count,
        "refreshes": len(summaries),
        "effective_workers": workers,
        **counters,
    }


def run_all_fields(
    contract: dict[str, Any],
    binary: Path,
    run_dir: Path,
    token: str,
    *,
    enforce_sequential_guard: bool,
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
            "CASA_RS_VLASS_GRID_THREADS": str(row["requested_grid_threads"]),
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
    if enforce_sequential_guard and wall > float(
        row["sequential_guard_maximum_wall_seconds"]
    ):
        raise ContractError(
            f"all-field wall {wall:.6f}s exceeds "
            f"{row['sequential_guard_maximum_wall_seconds']:.6f}s"
        )
    runtime = validate_all_field_log(
        contract,
        runtime_log,
        enforce_sequential_guard=enforce_sequential_guard,
    )
    provenance = all_run / "provenance.txt"
    if f"grid_threads\t{row['requested_grid_threads']}" not in provenance.read_text(
        encoding="utf-8"
    ):
        raise ContractError("all-field provenance did not bind requested grid workers")
    return {
        "wall_seconds": wall,
        "runtime_log": str(runtime_log),
        "runtime_log_sha256": sha256_file(runtime_log),
        "output_prefix": str(all_run / "rust"),
        "provenance": str(provenance),
        "provenance_sha256": sha256_file(provenance),
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


def latest_path(contract: dict[str, Any], phase: str) -> Path:
    return Path(contract["run_root"]) / f"latest-{phase}-measurement.json"


def write_measurement(
    contract: dict[str, Any], phase: str, receipt: dict[str, Any]
) -> None:
    run_dir = Path(receipt["run_dir"])
    measurement = run_dir / f"{phase}-measurement.json"
    measurement.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    pointer = latest_path(contract, phase)
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text(
        json.dumps(
            {
                "measurement": str(measurement),
                "measurement_sha256": sha256_file(measurement),
                "source_head": receipt["source_head"],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def load_measurement(contract: dict[str, Any], phase: str) -> dict[str, Any]:
    pointer = load_object(latest_path(contract, phase))
    if pointer.get("source_head") != git("rev-parse", "HEAD"):
        raise ContractError("latest measurement belongs to a different source commit")
    measurement_path = Path(pointer["measurement"])
    if sha256_file(measurement_path) != pointer["measurement_sha256"]:
        raise ContractError("latest measurement receipt hash changed")
    measurement = load_object(measurement_path)
    binary = Path(measurement["build"]["binary"])
    if sha256_file(binary) != measurement["build"]["binary_sha256"]:
        raise ContractError("release binary changed between measurement and guard")
    return measurement


def measure_single(contract: dict[str, Any]) -> float:
    token = run_token()
    run_dir = Path(contract["run_root"]) / "runs" / token
    run_dir.mkdir(parents=True, exist_ok=False)
    binary, build = build_release(run_dir)
    single_series = run_single_series(contract, binary, run_dir, token[-8:])
    receipt = {
        "schema_version": 1,
        "token": token,
        "run_dir": str(run_dir),
        "source_head": build["head"],
        "build": build,
        "single_field_series": single_series,
    }
    write_measurement(contract, "single", receipt)
    return float(single_series["summary"]["median_wall_seconds"])


def guard_single(contract: dict[str, Any]) -> None:
    measurement = load_measurement(contract, "single")
    run_dir = Path(measurement["run_dir"])
    binary = Path(measurement["build"]["binary"])
    single_series = measurement["single_field_series"]
    single_summary = validate_single_series(contract, single_series["timed"])
    median_run = single_series["timed"][single_summary["median_run_index"]]
    single_comparison = compare_row(
        contract,
        contract["single_field"],
        Path(median_run["output_prefix"]),
        run_dir / "single-casa",
    )
    all_field_idle = wait_for_host_idle(contract)
    all_fields = run_all_fields(
        contract,
        binary,
        run_dir,
        measurement["token"][-8:],
        enforce_sequential_guard=True,
    )
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
        "single_field_series": {
            **single_series,
            "summary": single_summary,
            "comparison": single_comparison,
        },
        "all_fields": {
            **all_fields,
            "host_idle_precondition": all_field_idle,
            "comparison": all_comparison,
        },
    }
    output = run_dir / "guard.json"
    output.write_text(json.dumps(guard_receipt, indent=2, sort_keys=True) + "\n")


def measure_all_fields(contract: dict[str, Any]) -> float:
    token = run_token()
    run_dir = Path(contract["run_root"]) / "runs" / token
    run_dir.mkdir(parents=True, exist_ok=False)
    binary, build = build_release(run_dir)
    all_fields = load_reusable_all_fields_landmark(contract, binary)
    if all_fields is None:
        host_idle = wait_for_host_idle(contract)
        all_fields = run_all_fields(
            contract,
            binary,
            run_dir,
            token[-8:],
            enforce_sequential_guard=False,
        )
        all_fields = {**all_fields, "host_idle_precondition": host_idle}
    receipt = {
        "schema_version": 1,
        "token": token,
        "run_dir": str(run_dir),
        "source_head": build["head"],
        "build": build,
        "all_fields": all_fields,
    }
    write_measurement(contract, "all-fields", receipt)
    return float(all_fields["wall_seconds"])


def guard_all_fields(contract: dict[str, Any]) -> None:
    measurement = load_measurement(contract, "all-fields")
    run_dir = Path(measurement["run_dir"])
    binary = Path(measurement["build"]["binary"])
    all_comparison = measurement["all_fields"].get("comparison")
    if all_comparison is None:
        all_comparison = compare_row(
            contract,
            contract["all_fields"],
            Path(measurement["all_fields"]["output_prefix"]),
            run_dir / "all63-casa",
        )
    single_landmark = load_reusable_single_landmark(contract, binary)
    if single_landmark is None:
        cooldown_seconds = int(contract["phase_two_single_guard_cooldown_seconds"])
        single_series = run_single_series(
            contract,
            binary,
            run_dir,
            measurement["token"][-8:],
            initial_cooldown_seconds=cooldown_seconds,
        )
        single_comparison = None
    else:
        single_series = single_landmark
        single_comparison = single_landmark["comparison"]
    target = float(contract["single_field"]["target_wall_seconds"])
    if float(single_series["summary"]["median_wall_seconds"]) > target:
        raise ContractError(
            "single-field median wall "
            f"{single_series['summary']['median_wall_seconds']:.6f}s exceeds "
            f"{target:.6f}s"
        )
    if single_comparison is None:
        median_run = single_series["timed"][
            single_series["summary"]["median_run_index"]
        ]
        single_comparison = compare_row(
            contract,
            contract["single_field"],
            Path(median_run["output_prefix"]),
            run_dir / "single-casa",
        )
    guard_receipt = {
        "schema_version": 1,
        "status": "passed",
        "source_head": measurement["source_head"],
        "binary_sha256": measurement["build"]["binary_sha256"],
        "all_fields": {
            **measurement["all_fields"],
            "comparison": all_comparison,
        },
        "single_field_series": {
            **single_series,
            "comparison": single_comparison,
        },
    }
    output = run_dir / "all-fields-guard.json"
    output.write_text(json.dumps(guard_receipt, indent=2, sort_keys=True) + "\n")


def guard_all_fields_primary(contract: dict[str, Any]) -> None:
    """Guard an all-field trial without making the single-field row a veto."""
    measurement = load_measurement(contract, "all-fields")
    run_dir = Path(measurement["run_dir"])
    all_fields = measurement["all_fields"]
    validate_all_field_log(
        contract,
        Path(all_fields["runtime_log"]),
        enforce_sequential_guard=False,
    )
    all_comparison = all_fields.get("comparison")
    if all_comparison is None:
        all_comparison = compare_row(
            contract,
            contract["all_fields"],
            Path(all_fields["output_prefix"]),
            run_dir / "all63-casa",
        )
    guard_receipt = {
        "schema_version": 1,
        "status": "passed",
        "authority": "all-fields-primary",
        "source_head": measurement["source_head"],
        "binary_sha256": measurement["build"]["binary_sha256"],
        "all_fields": {
            **all_fields,
            "comparison": all_comparison,
        },
    }
    output = run_dir / "all-fields-primary-guard.json"
    output.write_text(json.dumps(guard_receipt, indent=2, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=(
            "measure-single",
            "guard-single",
            "measure-all-fields",
            "guard-all-fields",
            "guard-all-fields-primary",
        ),
    )
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    args = parser.parse_args()
    try:
        contract = load_object(args.contract)
        validate_contract(contract)
        validate_inputs(contract)
        if args.action == "measure-single":
            metric = measure_single(contract)
            print(f"{metric:.9f}")
        elif args.action == "guard-single":
            guard_single(contract)
        elif args.action == "measure-all-fields":
            metric = measure_all_fields(contract)
            print(f"{metric:.9f}")
        elif args.action == "guard-all-fields-primary":
            guard_all_fields_primary(contract)
        else:
            guard_all_fields(contract)
    except (ContractError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"VLASS 4096 autoresearch error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
