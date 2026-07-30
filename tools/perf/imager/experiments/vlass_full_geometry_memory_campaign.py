#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Plan and run the bounded VLASS 12,150-pixel memory campaign.

This driver is intentionally narrower than a general benchmark launcher:

* a promoted 4096-pixel, full-16-SPW correctness receipt is mandatory;
* the five approved memory policies are each scheduled exactly once;
* CASA is removed from every derived workload and explicitly disabled;
* planner-only is the default, and 12,150-pixel execution requires an
  explicit ``--execute-12150`` opt-in;
* an unchanged experiment fingerprint cannot be claimed twice; and
* a full clean row additionally requires a separately reviewed, passed dirty
  policy promotion receipt.

The existing ``run_workload.py`` harness remains the execution and comparison
boundary. This file only adds ladder gates, duplicate rejection, bounded-stop
monitoring, and campaign receipts around that shared interface.
"""

from __future__ import annotations

import argparse
import copy
from dataclasses import asdict, dataclass
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import platform
import queue
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
IMAGER_TOOL_DIR = SCRIPT_DIR.parent
REPO_ROOT = SCRIPT_DIR.parents[3]
RUN_WORKLOAD = IMAGER_TOOL_DIR / "run_workload.py"
if str(IMAGER_TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(IMAGER_TOOL_DIR))
import run_workload as workload_harness  # noqa: E402

DEFAULT_DIRTY_WORKLOAD = (
    IMAGER_TOOL_DIR / "workloads" / "vlass-fragment-single-field-auto.json"
)
DEFAULT_CLEAN_WORKLOAD = (
    IMAGER_TOOL_DIR / "workloads" / "vlass-fragment-single-field-clean-serial.json"
)
DEFAULT_ALL_FIELDS_DIRTY_WORKLOAD = (
    IMAGER_TOOL_DIR / "workloads" / "vlass-fragment-all-fields-auto.json"
)
WORKLOAD_KINDS = ("single-field", "all-fields")
ALL_FIELDS_SELECTOR = "1107~1127,1512~1532,1542~1562"
WORKLOAD_GEOMETRY = {
    "single-field": {
        "dataset_selection": "single_field",
        "field": "1525",
        "field_count": 1,
    },
    "all-fields": {
        "dataset_selection": "all_fields",
        "field": ALL_FIELDS_SELECTOR,
        "field_count": 63,
    },
}
DEFAULT_WORKLOADS = {
    ("single-field", "dirty"): DEFAULT_DIRTY_WORKLOAD,
    ("single-field", "clean"): DEFAULT_CLEAN_WORKLOAD,
    ("all-fields", "dirty"): DEFAULT_ALL_FIELDS_DIRTY_WORKLOAD,
}
ACCEPTANCE_PHYSICAL_MEMORY_BYTES = 32 * 1024**3

POLICIES = (
    "conservative-no-swap",
    "aggressive",
    "oversubscribe",
    "stage-aware",
    "hybrid",
)
POLICY_RUNTIME_ACTIONS = {
    "conservative-no-swap": {
        "admission_action": "no-swap-headroom",
        "swap_action": "avoid-intentional-swap",
        "stage_lifetime_release_requested": False,
        "next_use_aware_replay_requested": False,
    },
    "aggressive": {
        "admission_action": "physical-process-ceiling",
        "swap_action": "allow-compression-or-incidental-swap",
        "stage_lifetime_release_requested": False,
        "next_use_aware_replay_requested": False,
    },
    "oversubscribe": {
        "admission_action": "explicit-oversubscription-target",
        "swap_action": "intentional-oversubscription",
        "stage_lifetime_release_requested": False,
        "next_use_aware_replay_requested": False,
    },
    "stage-aware": {
        "admission_action": "no-swap-headroom",
        "swap_action": "avoid-intentional-swap",
        "stage_lifetime_release_requested": True,
        "next_use_aware_replay_requested": False,
    },
    "hybrid": {
        "admission_action": "physical-process-ceiling",
        "swap_action": "allow-compression-or-incidental-swap",
        "stage_lifetime_release_requested": True,
        "next_use_aware_replay_requested": True,
    },
}
EXPECTED_19_PRODUCTS = (
    ".alpha",
    ".alpha.error",
    ".image.tt0",
    ".image.tt1",
    ".mask",
    ".model.tt0",
    ".model.tt1",
    ".pb.tt0",
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
    ".residual.tt0",
    ".residual.tt1",
    ".sumwt.tt0",
    ".sumwt.tt1",
    ".sumwt.tt2",
    ".weight.tt0",
    ".weight.tt1",
    ".weight.tt2",
)
EXPECTED_DIRTY_PRODUCTS = tuple(
    product for product in EXPECTED_19_PRODUCTS if product != ".mask"
)
PROMOTION_GATES = (
    "casa_component_trajectory",
    "major_cycle_trajectory",
    "numerical",
    "topology",
    "metadata",
    "product_inventory",
    "no_divergence",
)
MEMORY_EXECUTION_GATES = (
    "memory_and_swap_receipt",
    "per_stage_memory_telemetry",
    "no_unaccounted_allocation",
    "required_allocation_ledger",
    "stage_timings",
    "bounded_operation",
    "policy_selected",
    "requested_policy_actions_active",
    "memory_target_bound",
    "acceptance_host_32_gib",
)
DIRTY_POLICY_GATES = (
    "dirty_correctness",
    *MEMORY_EXECUTION_GATES,
)
CLEAN_EXECUTION_GATES = (
    "clean_correctness",
    *MEMORY_EXECUTION_GATES,
)
CLEAN_PROMOTION_GATES = (
    *PROMOTION_GATES,
    *MEMORY_EXECUTION_GATES,
)
MEMORY_CAMPAIGN_PROMOTION_SCOPE = "memory-campaign-only"
MEMORY_CAMPAIGN_PROMOTION_STATUS = "memory-candidate-promoted"
FINAL_WAVE_ACCEPTANCE_KIND = "vlass_final_four_row_10x_acceptance"
FINAL_WAVE_ACCEPTANCE_MINIMUM_SPEEDUP = 10.0
FINAL_WAVE_ACCEPTANCE_ROWS = (
    ("single-field", "dirty"),
    ("single-field", "clean"),
    ("all-fields", "dirty"),
    ("all-fields", "clean"),
)
REQUIRED_LIFETIME_STAGES = (
    "prepare",
    "source-ingest",
    "weighting",
    "initial-grid",
    "dirty-transform",
    "minor-cycle",
    "model-transform",
    "residual-grid",
    "residual-transform",
    "finish",
    "product-materialization",
    "product-write",
)
REQUIRED_DIRTY_TIMING_STAGES = (
    "total",
    "run_imaging",
    "prepare_plane_input",
    "weighting",
    "psf_grid",
    "psf_fft",
    "write_products",
)
REQUIRED_CLEAN_TIMING_STAGES = (
    *REQUIRED_DIRTY_TIMING_STAGES,
    "minor_cycle",
    "minor_cycle_solve",
    "major_cycle_refresh",
    "model_fft",
    "residual_degrid_grid",
    "residual_fft",
    "restore",
)
REQUIRED_COMMON_ALLOCATION_COMPONENTS = (
    "grids",
    "source row blocks",
    "FFT chunks",
    "AWProject MT-MFS run state",
    "AWProject CF pixels",
    "AWProject source-order tap scratch",
    "AWProject CF index",
    "POINTING index",
    "AWProject safety margin",
    "AWProject MT-MFS finish state",
    "AWProject MT-MFS product state",
    "product writer scratch",
)
REQUIRED_CLEAN_ALLOCATION_COMPONENTS = (
    "AWProject CASA-layout model FFT staging",
    "AWProject MT-MFS bounded multiscale scratch",
    "AWProject compact replay retention",
)
MIB = 1024**2
GIB = 1024**3
FULL_GEOMETRY_SIDE = 12_150
FULL_GEOMETRY_PIXELS = FULL_GEOMETRY_SIDE**2
FULL_GEOMETRY_EXACT_COMPONENT_BYTES = {
    "grids": FULL_GEOMETRY_PIXELS * 8 * 16,
    "AWProject MT-MFS run state": FULL_GEOMETRY_PIXELS * 61,
    "mosaic weighting density maps": FULL_GEOMETRY_PIXELS * 4,
    "AWProject CASA-layout model FFT staging": FULL_GEOMETRY_PIXELS * 2 * 8,
    "AWProject MT-MFS bounded multiscale scratch": FULL_GEOMETRY_PIXELS * 27,
    "AWProject MT-MFS finish state": FULL_GEOMETRY_PIXELS * 126,
    "AWProject MT-MFS product state": FULL_GEOMETRY_PIXELS * 76,
    "product writer scratch": FULL_GEOMETRY_PIXELS,
    "AWProject compensated f64 readback": FULL_GEOMETRY_PIXELS * 16,
}
FULL_GEOMETRY_RESIDUAL_GRID_BYTES = FULL_GEOMETRY_PIXELS * 2 * 16
FULL_GEOMETRY_FFT_BYTES_ALLOWED = {
    FULL_GEOMETRY_PIXELS * 4,
    FULL_GEOMETRY_PIXELS * 2 * 16,
}
REPLAY_WORKING_SET_REFERENCE_BYTES = round(7.31 * GIB)
REPLAY_WORKING_SET_MIN_BYTES = 7 * GIB
REPLAY_WORKING_SET_MAX_BYTES = 8 * GIB
REQUIRED_STAGE_MEMORY_FIELDS = (
    "process_physical_footprint_bytes",
    "stage_observed_peak_process_physical_footprint_bytes",
    "current_rss_bytes",
    "stage_observed_peak_rss_bytes",
    "current_cpu_allocated_bytes",
    "stage_observed_peak_cpu_allocated_bytes",
    "current_metal_allocated_bytes",
    "stage_observed_peak_metal_allocated_bytes",
    "current_unified_memory_allocated_bytes",
    "stage_observed_peak_unified_memory_allocated_bytes",
    "host_compressed_memory_bytes",
    "stage_observed_peak_host_compressed_memory_bytes",
    "swap_used_bytes",
    "stage_observed_peak_swap_used_bytes",
    "swapin_bytes_delta",
    "swapout_bytes_delta",
    "process_page_faults_delta",
    "process_disk_read_bytes_delta",
    "process_disk_write_bytes_delta",
    "external_disk_read_bytes_delta",
    "external_disk_write_bytes_delta",
    "gpu_stall_ms",
    "elapsed_monotonic_ms",
)
STORAGE_BANDWIDTH_PROBE_BYTES = 64 * MIB
STORAGE_BANDWIDTH_PROBE_BLOCK_BYTES = 4 * MIB
SPILL_READ_BANDWIDTH_ENV = "CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND"
SPILL_WRITE_BANDWIDTH_ENV = "CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND"
LABEL_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class CampaignError(ValueError):
    """The requested campaign violates a ladder or evidence invariant."""


def runtime_actions_match_policy(
    actions: dict[str, Any],
    *,
    policy: str,
) -> bool:
    """Return whether the immutable action receipt matches the named policy."""

    expected = POLICY_RUNTIME_ACTIONS.get(policy)
    return (
        expected is not None
        and actions.get("policy") == policy
        and all(actions.get(field) == value for field, value in expected.items())
        and actions.get("replay_prime_stage") == "residual-grid"
        and actions.get("known_last_use_release_active") is True
    )


def requested_runtime_actions_are_active(
    actions: dict[str, Any],
    *,
    policy: str,
) -> bool:
    """Reject stage-aware labels until their requested mechanisms are real."""

    if not runtime_actions_match_policy(actions, policy=policy):
        return False
    if policy not in {"stage-aware", "hybrid"}:
        return True
    return (
        actions.get("product_streaming_active") is True
        and actions.get("replay_spill_active") is True
        and actions.get("storage_demotion_active") is True
        and (
            policy != "hybrid"
            or actions.get("replay_retention_action") == "pinned-next-use-aware-subset"
        )
    )


@dataclass(frozen=True)
class StopThresholds:
    """Early-stop policy for one explicitly authorized full-size subprocess."""

    max_wall_seconds: float
    max_swapout_delta_bytes: int
    max_swap_io_bytes_per_second: float
    min_memory_free_percent: float
    max_no_output_seconds: float
    pressure_samples: int
    sample_interval_seconds: float
    terminate_grace_seconds: float

    def validate(self) -> None:
        if self.max_wall_seconds <= 0:
            raise CampaignError("--max-wall-seconds must be positive")
        if self.max_swapout_delta_bytes < 0:
            raise CampaignError("--max-swapout-delta-bytes must be non-negative")
        if self.max_swap_io_bytes_per_second < 0:
            raise CampaignError("--max-swap-io-bytes-per-second must be non-negative")
        if not 0 <= self.min_memory_free_percent <= 100:
            raise CampaignError("--min-memory-free-percent must be between 0 and 100")
        if self.max_no_output_seconds <= 0:
            raise CampaignError("--max-no-output-seconds must be positive")
        if self.pressure_samples < 1:
            raise CampaignError("--pressure-samples must be at least one")
        if self.sample_interval_seconds <= 0:
            raise CampaignError("--sample-interval-seconds must be positive")
        if self.terminate_grace_seconds < 0:
            raise CampaignError("--terminate-grace-seconds must be non-negative")


@dataclass(frozen=True)
class EvidenceRef:
    """Content-addressed reference to prerequisite evidence."""

    path: str
    sha256: str


@dataclass(frozen=True)
class ExperimentWorkloadBinding:
    """Exact executable result and embedded comparison used by one experiment."""

    workload_result_sha256: str
    product_comparison_sha256: str


@dataclass(frozen=True)
class MonitorResult:
    """Bounded outer-process observation for one workload-harness invocation."""

    exit_code: int
    elapsed_seconds: float
    stop_reason: str | None
    sample_count: int
    process_tree_resident_bytes_peak: int
    memory_free_percent_min: float | None
    swap_used_bytes_peak: int | None
    swapin_bytes_delta: int | None
    swapout_bytes_delta: int | None
    swap_io_bytes_per_second_max: float | None
    stdout_log_path: str
    stdout_log_sha256: str


def utc_now() -> str:
    """Return a stable UTC timestamp for receipts."""

    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_sha256(value: Any) -> str:
    """Hash a JSON value with deterministic separators and key ordering."""

    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def pending_final_wave_acceptance_contract() -> dict[str, Any]:
    """Describe the separate, not-yet-evaluated four-row 10x closeout gate."""

    return {
        "kind": FINAL_WAVE_ACCEPTANCE_KIND,
        "status": "not-evaluated",
        "satisfied": False,
        "required_rows": [
            {"workload_kind": workload_kind, "mode": mode}
            for workload_kind, mode in FINAL_WAVE_ACCEPTANCE_ROWS
        ],
        "minimum_independent_speedup": FINAL_WAVE_ACCEPTANCE_MINIMUM_SPEEDUP,
        "speedup_formula": "matched_casa_wall_seconds / casa_rs_wall_seconds",
        "requires_content_addressed_casa_baseline_per_row": True,
    }


def sha256_file(path: Path) -> str:
    """Hash one file without loading it into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path, *, label: str) -> dict[str, Any]:
    """Load one JSON object with a campaign-specific error."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise CampaignError(f"cannot read {label} {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise CampaignError(f"{label} is not valid JSON: {path}: {error}") from error
    if not isinstance(value, dict):
        raise CampaignError(f"{label} must contain a JSON object: {path}")
    return value


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    """Publish a receipt without exposing a partial JSON file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(value, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        handle.write(serialized)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def unavailable_storage_bandwidth_evidence(*, reason: str) -> dict[str, Any]:
    """Return an explicit absence receipt without inventing planner bandwidth."""

    return {
        "schema_version": 1,
        "status": "unavailable",
        "reason": reason,
        "receipt": None,
        "volume_path": None,
        "read_bytes_per_second": None,
        "write_bytes_per_second": None,
        "command_environment": {},
    }


def _probe_payload_block(block_bytes: int) -> bytes:
    pattern = b"casa-rs-vlass-storage-bandwidth-probe-v1\n"
    repeats = (block_bytes + len(pattern) - 1) // len(pattern)
    return (pattern * repeats)[:block_bytes]


def _disable_darwin_file_cache(descriptor: int) -> bool:
    if platform.system() != "Darwin":
        return False
    try:
        import fcntl  # noqa: PLC0415

        fcntl.fcntl(descriptor, 48, 1)  # F_NOCACHE on Darwin.
    except (ImportError, OSError):
        return False
    return True


def measure_storage_bandwidth(
    volume_path: Path,
    *,
    probe_bytes: int = STORAGE_BANDWIDTH_PROBE_BYTES,
    block_bytes: int = STORAGE_BANDWIDTH_PROBE_BLOCK_BYTES,
) -> dict[str, Any]:
    """Measure one bounded durable write and uncached read on the spill volume."""

    if probe_bytes < 1 or block_bytes < 1 or block_bytes > probe_bytes:
        raise CampaignError(
            "storage probe bytes must be positive and block bytes cannot exceed "
            "the total probe size"
        )
    volume_path = volume_path.expanduser().resolve()
    volume_path.mkdir(parents=True, exist_ok=True)
    volume_stat = volume_path.stat()
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".casa-rs-vlass-storage-probe-",
        suffix=".tmp",
        dir=volume_path,
    )
    temporary_path = Path(temporary_name)
    payload_block = _probe_payload_block(block_bytes)
    block_sha256 = hashlib.sha256(payload_block).hexdigest()
    cache_bypass_applied = False
    write_seconds = 0.0
    read_seconds = 0.0
    write_digest = hashlib.sha256()
    read_digest = hashlib.sha256()
    try:
        cache_bypass_applied = _disable_darwin_file_cache(descriptor)
        with os.fdopen(descriptor, "w+b", buffering=0) as handle:
            remaining = probe_bytes
            write_started = time.perf_counter()
            while remaining:
                chunk = payload_block[: min(remaining, len(payload_block))]
                written = handle.write(chunk)
                if written != len(chunk):
                    raise CampaignError(
                        "storage bandwidth probe did not complete a full write"
                    )
                write_digest.update(chunk)
                remaining -= written
            handle.flush()
            os.fsync(handle.fileno())
            write_seconds = max(time.perf_counter() - write_started, 1e-9)

            handle.seek(0)
            remaining = probe_bytes
            read_started = time.perf_counter()
            while remaining:
                chunk = handle.read(min(remaining, block_bytes))
                if not chunk:
                    raise CampaignError(
                        "storage bandwidth probe ended before the payload was read"
                    )
                read_digest.update(chunk)
                remaining -= len(chunk)
            read_seconds = max(time.perf_counter() - read_started, 1e-9)
    finally:
        temporary_path.unlink(missing_ok=True)

    if write_digest.digest() != read_digest.digest():
        raise CampaignError("storage bandwidth probe readback digest does not match")
    write_bytes_per_second = int(probe_bytes / write_seconds)
    read_bytes_per_second = int(probe_bytes / read_seconds)
    if write_bytes_per_second < 1 or read_bytes_per_second < 1:
        raise CampaignError("storage bandwidth probe produced a non-positive rate")
    return {
        "schema_version": 1,
        "kind": "vlass_storage_bandwidth_probe",
        "status": "measured",
        "measured_at": utc_now(),
        "volume_path": str(volume_path),
        "volume_device_id": volume_stat.st_dev,
        "platform": platform.system(),
        "probe": {
            "bytes": probe_bytes,
            "block_bytes": block_bytes,
            "block_sha256": block_sha256,
            "payload_sha256": write_digest.hexdigest(),
            "readback_sha256": read_digest.hexdigest(),
            "write_seconds": write_seconds,
            "read_seconds": read_seconds,
            "write_fsync_included": True,
            "darwin_f_nocache_applied": cache_bypass_applied,
            "temporary_file_removed": not temporary_path.exists(),
        },
        "write_bytes_per_second": write_bytes_per_second,
        "read_bytes_per_second": read_bytes_per_second,
    }


def validate_storage_bandwidth_receipt(
    path: Path,
    *,
    volume_path: Path,
    probe_bytes: int = STORAGE_BANDWIDTH_PROBE_BYTES,
    block_bytes: int = STORAGE_BANDWIDTH_PROBE_BLOCK_BYTES,
) -> dict[str, Any]:
    """Validate one reusable storage measurement against its exact mounted volume."""

    path = path.expanduser().resolve()
    receipt = load_json(path, label="storage bandwidth receipt")
    volume_path = volume_path.expanduser().resolve()
    try:
        volume_device_id = volume_path.stat().st_dev
    except OSError as error:
        raise CampaignError(
            f"cannot inspect storage probe volume {volume_path}: {error}"
        ) from error
    probe = receipt.get("probe")
    mismatches = []
    if (
        receipt.get("kind") != "vlass_storage_bandwidth_probe"
        or receipt.get("status") != "measured"
    ):
        mismatches.append("kind/status")
    if (
        receipt.get("volume_path") != str(volume_path)
        or receipt.get("volume_device_id") != volume_device_id
    ):
        mismatches.append("mounted volume")
    if not isinstance(probe, dict):
        mismatches.append("probe receipt")
        probe = {}
    if probe.get("bytes") != probe_bytes or probe.get("block_bytes") != block_bytes:
        mismatches.append("probe size")
    if (
        probe.get("write_fsync_included") is not True
        or probe.get("temporary_file_removed") is not True
    ):
        mismatches.append("durability/cleanup")
    if (
        receipt.get("platform") == "Darwin"
        and probe.get("darwin_f_nocache_applied") is not True
    ):
        mismatches.append("Darwin uncached I/O")
    for field in ("block_sha256", "payload_sha256", "readback_sha256"):
        if (
            not isinstance(probe.get(field), str)
            or re.fullmatch(r"[0-9a-f]{64}", probe[field]) is None
        ):
            mismatches.append(field)
    if probe.get("payload_sha256") != probe.get("readback_sha256"):
        mismatches.append("payload readback")
    for field in ("write_seconds", "read_seconds"):
        value = probe.get(field)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
            mismatches.append(field)
    expected_rates = {}
    if not mismatches:
        expected_rates = {
            "write_bytes_per_second": int(probe_bytes / probe["write_seconds"]),
            "read_bytes_per_second": int(probe_bytes / probe["read_seconds"]),
        }
    for field, expected in expected_rates.items():
        if receipt.get(field) != expected or expected < 1:
            mismatches.append(field)
    if mismatches:
        raise CampaignError(
            f"{path}: storage bandwidth receipt mismatch: "
            + ", ".join(sorted(set(mismatches)))
        )
    return receipt


def storage_bandwidth_evidence(
    receipt_path: Path,
    *,
    volume_path: Path,
    probe_bytes: int = STORAGE_BANDWIDTH_PROBE_BYTES,
    block_bytes: int = STORAGE_BANDWIDTH_PROBE_BLOCK_BYTES,
) -> dict[str, Any]:
    """Measure at most once, then return a content-addressed environment receipt."""

    receipt_path = receipt_path.expanduser().resolve()
    if not receipt_path.exists():
        measurement = measure_storage_bandwidth(
            volume_path,
            probe_bytes=probe_bytes,
            block_bytes=block_bytes,
        )
        atomic_write_json(receipt_path, measurement)
    receipt = validate_storage_bandwidth_receipt(
        receipt_path,
        volume_path=volume_path,
        probe_bytes=probe_bytes,
        block_bytes=block_bytes,
    )
    read_rate = receipt["read_bytes_per_second"]
    write_rate = receipt["write_bytes_per_second"]
    return {
        "schema_version": 1,
        "status": "measured",
        "receipt": {
            "path": str(receipt_path),
            "sha256": sha256_file(receipt_path),
        },
        "volume_path": receipt["volume_path"],
        "volume_device_id": receipt["volume_device_id"],
        "probe": receipt["probe"],
        "read_bytes_per_second": read_rate,
        "write_bytes_per_second": write_rate,
        "command_environment": {
            SPILL_READ_BANDWIDTH_ENV: str(read_rate),
            SPILL_WRITE_BANDWIDTH_ENV: str(write_rate),
        },
    }


def apply_storage_bandwidth_environment(
    manifest: dict[str, Any],
    *,
    evidence: dict[str, Any],
) -> None:
    """Bind a measured storage receipt to the workload's Rust environment."""

    if evidence.get("status") != "measured":
        return
    environment = evidence.get("command_environment")
    if not isinstance(environment, dict) or set(environment) != {
        SPILL_READ_BANDWIDTH_ENV,
        SPILL_WRITE_BANDWIDTH_ENV,
    }:
        raise CampaignError("measured storage evidence has no exact spill environment")
    run = manifest.get("run")
    run_environment = run.get("env") if isinstance(run, dict) else None
    if not isinstance(run_environment, dict):
        raise CampaignError("derived manifest run.env must be a JSON object")
    run_environment.update(environment)


def validate_experiment_storage_bandwidth(
    experiment: dict[str, Any],
    *,
    experiment_path: Path,
    workload_result: dict[str, Any],
) -> None:
    """Require the execute row to bind and use its one measured volume probe."""

    evidence = experiment.get("storage_bandwidth")
    if not isinstance(evidence, dict) or evidence.get("status") != "measured":
        raise CampaignError(
            f"{experiment_path}: execute-12150 requires measured storage bandwidth"
        )
    reference = evidence.get("receipt")
    if not isinstance(reference, dict):
        raise CampaignError(
            f"{experiment_path}: storage bandwidth receipt reference is missing"
        )
    receipt_path = resolve_receipt_reference(
        experiment_path,
        reference.get("path"),
        field="storage_bandwidth.receipt.path",
    )
    expected_sha256 = reference.get("sha256")
    if not isinstance(expected_sha256, str) or expected_sha256 != sha256_file(
        receipt_path
    ):
        raise CampaignError(
            f"{experiment_path}: storage bandwidth receipt hash does not match"
        )
    volume_path_value = evidence.get("volume_path")
    if not isinstance(volume_path_value, str) or not volume_path_value:
        raise CampaignError(f"{experiment_path}: storage volume path is missing")
    volume_path = Path(volume_path_value).expanduser().resolve()
    receipt = validate_storage_bandwidth_receipt(
        receipt_path,
        volume_path=volume_path,
    )
    if (
        evidence.get("volume_device_id") != receipt.get("volume_device_id")
        or evidence.get("probe") != receipt.get("probe")
        or evidence.get("read_bytes_per_second") != receipt.get("read_bytes_per_second")
        or evidence.get("write_bytes_per_second")
        != receipt.get("write_bytes_per_second")
    ):
        raise CampaignError(
            f"{experiment_path}: storage evidence does not match its receipt"
        )
    expected_environment = {
        SPILL_READ_BANDWIDTH_ENV: str(receipt["read_bytes_per_second"]),
        SPILL_WRITE_BANDWIDTH_ENV: str(receipt["write_bytes_per_second"]),
    }
    if evidence.get("command_environment") != expected_environment:
        raise CampaignError(
            f"{experiment_path}: storage evidence environment does not match"
        )
    experiment_environment = experiment.get("command_environment")
    if not isinstance(experiment_environment, dict) or any(
        experiment_environment.get(key) != value
        for key, value in expected_environment.items()
    ):
        raise CampaignError(
            f"{experiment_path}: campaign command omitted measured storage bandwidth"
        )
    command = workload_result.get("command")
    workload_environment = command.get("env") if isinstance(command, dict) else None
    if not isinstance(workload_environment, dict) or any(
        workload_environment.get(key) != value
        for key, value in expected_environment.items()
    ):
        raise CampaignError(
            f"{experiment_path}: workload command omitted measured storage bandwidth"
        )
    targets = experiment.get("targets")
    artifact_root = targets.get("artifact_root") if isinstance(targets, dict) else None
    if not isinstance(artifact_root, str) or not artifact_root:
        raise CampaignError(f"{experiment_path}: artifact-root target is missing")
    artifact_root_path = Path(artifact_root).expanduser().resolve()
    if (
        not artifact_root_path.is_relative_to(volume_path)
        or artifact_root_path.stat().st_dev != receipt["volume_device_id"]
    ):
        raise CampaignError(
            f"{experiment_path}: artifact root is not on the measured storage volume"
        )


def resolve_receipt_reference(receipt_path: Path, value: Any, *, field: str) -> Path:
    """Resolve a required path stored in a gate receipt."""

    if not isinstance(value, str) or not value.strip():
        raise CampaignError(f"{receipt_path}: {field} must be a non-empty path")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = receipt_path.parent / path
    path = path.resolve()
    if not path.is_file():
        raise CampaignError(f"{receipt_path}: {field} does not exist: {path}")
    return path


def require_content_hash(
    receipt: dict[str, Any],
    *,
    receipt_path: Path,
    field: str,
    referenced_path: Path,
) -> str:
    """Require one receipt to bind the exact bytes of a referenced artifact."""

    expected = receipt.get(field)
    if not isinstance(expected, str) or re.fullmatch(r"[0-9a-f]{64}", expected) is None:
        raise CampaignError(
            f"{receipt_path}: {field} must be a lowercase SHA-256 digest"
        )
    observed = sha256_file(referenced_path)
    if expected != observed:
        raise CampaignError(f"{receipt_path}: {field} does not match {referenced_path}")
    return observed


def require_true_gates(
    receipt: dict[str, Any],
    required: tuple[str, ...],
    *,
    receipt_path: Path,
) -> None:
    """Require an explicit true value for every reviewed gate."""

    gates = receipt.get("gates")
    if not isinstance(gates, dict):
        raise CampaignError(f"{receipt_path}: gates must be a JSON object")
    failed = [gate for gate in required if gates.get(gate) is not True]
    if failed:
        raise CampaignError(
            f"{receipt_path}: required gate(s) are not explicitly true: "
            + ", ".join(failed)
        )


def validate_comparison_receipt(path: Path) -> dict[str, Any]:
    """Validate the frozen 19-product comparator evidence."""

    comparison = load_json(path, label="4096 promotion comparison receipt")
    if comparison.get("status") != "completed":
        raise CampaignError(f"{path}: comparison status must be completed")
    requested = comparison.get("requested_products")
    if requested != list(EXPECTED_19_PRODUCTS):
        raise CampaignError(
            f"{path}: comparison must request the exact ordered 19-product contract"
        )
    inventory = comparison.get("product_inventory")
    if not isinstance(inventory, dict) or inventory.get("status") != "matched":
        raise CampaignError(f"{path}: exact product inventory must be matched")
    products = comparison.get("products")
    if not isinstance(products, dict) or any(
        not isinstance(products.get(product), dict)
        or products[product].get("status") != "compared"
        for product in EXPECTED_19_PRODUCTS
    ):
        raise CampaignError(f"{path}: every one of the 19 products must be compared")
    if comparison.get("require_metadata_parity") is not True:
        raise CampaignError(f"{path}: metadata parity must be required")
    if comparison.get("require_exact_product_inventory") is not True:
        raise CampaignError(f"{path}: exact product inventory must be required")
    tolerances = comparison.get("tolerances")
    default = tolerances.get("default") if isinstance(tolerances, dict) else None
    if (
        not isinstance(default, dict)
        or default.get("require_topology_parity") is not True
    ):
        raise CampaignError(f"{path}: topology parity must be required")
    tolerance_evaluation = comparison.get("tolerance_evaluation")
    if (
        not isinstance(tolerance_evaluation, dict)
        or tolerance_evaluation.get("status") != "passed"
    ):
        raise CampaignError(f"{path}: comparison tolerance evaluation did not pass")
    structured_review = comparison.get("structured_difference_review")
    if isinstance(structured_review, dict) and structured_review.get("label") in {
        "bad",
        "investigate",
    }:
        raise CampaignError(f"{path}: structured-difference review did not pass")
    return comparison


def validate_trajectory_receipt(
    path: Path,
    *,
    expected_geometry: dict[str, Any] | None = None,
    workload_result_sha256: str | None = None,
    comparison_receipt_sha256: str | None = None,
    product_comparison_sha256: str | None = None,
) -> dict[str, Any]:
    """Validate same-cycle CASA component/major-cycle evidence."""

    trajectory = load_json(path, label="4096 promotion trajectory receipt")
    if trajectory.get("status") != "completed":
        raise CampaignError(f"{path}: trajectory status must be completed")
    if expected_geometry is not None:
        geometry = trajectory.get("geometry")
        if not isinstance(geometry, dict) or any(
            geometry.get(key) != value for key, value in expected_geometry.items()
        ):
            raise CampaignError(
                f"{path}: trajectory geometry must bind {expected_geometry}"
            )
    coverage = trajectory.get("coverage")
    if not isinstance(coverage, dict) or any(
        coverage.get(field) is not True
        for field in ("casa_complete", "rust_complete", "same_cycle_count")
    ):
        raise CampaignError(f"{path}: trajectory coverage is not complete and aligned")
    parity = trajectory.get("discrete_parity")
    if not isinstance(parity, dict) or parity.get("status") != "passed":
        raise CampaignError(f"{path}: discrete trajectory parity did not pass")
    aligned = trajectory.get("aligned_cycles")
    if not isinstance(aligned, int) or isinstance(aligned, bool) or aligned < 1:
        raise CampaignError(f"{path}: aligned_cycles must be a positive integer")
    if (
        trajectory.get("casa_cycles") != aligned
        or trajectory.get("rust_cycles") != aligned
    ):
        raise CampaignError(f"{path}: CASA and casa-rs cycle counts must be aligned")
    if trajectory.get("no_divergence") is not True:
        raise CampaignError(f"{path}: trajectory must explicitly report no divergence")
    for field in ("component_selection", "major_cycle_residual"):
        evidence = trajectory.get(field)
        if not isinstance(evidence, dict) or evidence.get("status") != "passed":
            raise CampaignError(f"{path}: {field} trajectory evidence did not pass")
    if (
        workload_result_sha256 is not None
        and trajectory.get("workload_result_sha256") != workload_result_sha256
    ):
        raise CampaignError(
            f"{path}: trajectory workload-result binding does not match"
        )
    if (
        comparison_receipt_sha256 is not None
        and trajectory.get("comparison_receipt_sha256") != comparison_receipt_sha256
    ):
        raise CampaignError(f"{path}: trajectory comparison binding does not match")
    if (
        product_comparison_sha256 is not None
        and trajectory.get("product_comparison_sha256") != product_comparison_sha256
    ):
        raise CampaignError(
            f"{path}: trajectory embedded product-comparison binding does not match"
        )
    return trajectory


def _expected_geometry(workload_kind: str, *, imsize: int) -> dict[str, Any]:
    if workload_kind not in WORKLOAD_GEOMETRY:
        raise CampaignError(f"unsupported workload kind: {workload_kind}")
    workload = WORKLOAD_GEOMETRY[workload_kind]
    return {
        "imsize": imsize,
        "spw": "2~17",
        "field": workload["field"],
        "nterms": 2,
        "wprojplanes": 32,
        "product_count": 19,
    }


def _expected_bound_geometry(workload_kind: str, *, imsize: int) -> dict[str, Any]:
    geometry = _expected_geometry(workload_kind, imsize=imsize)
    workload = WORKLOAD_GEOMETRY[workload_kind]
    geometry.update(
        {
            "dataset_selection": workload["dataset_selection"],
            "field_count": workload["field_count"],
        }
    )
    return geometry


def _command_parameter(
    workload_result: dict[str, Any],
    *,
    parameter: str,
    environment: str,
) -> Any:
    command = workload_result.get("command")
    if not isinstance(command, dict):
        return None
    rust = command.get("rust")
    intended = rust.get("intended_parameters") if isinstance(rust, dict) else None
    if isinstance(intended, dict) and parameter in intended:
        return intended[parameter]
    command_environment = command.get("env")
    if isinstance(command_environment, dict):
        return command_environment.get(environment)
    return None


def validate_4096_workload_result(
    path: Path,
    *,
    workload_kind: str,
    comparison: dict[str, Any],
) -> dict[str, Any]:
    """Validate the executable result that produced the promoted comparison."""

    result = load_json(path, label="4096 promotion workload result")
    if result.get("kind") != "workload_run" or result.get("status") != "completed":
        raise CampaignError(f"{path}: 4096 workload must be a completed workload_run")
    mode = result.get("mode")
    if not isinstance(mode, dict):
        raise CampaignError(f"{path}: 4096 workload has no mode receipt")
    expected_mode = {
        "bench_mode": "clean",
        "image_shape": [4096, 4096],
        "channel_count": 64,
        "nterms": 2,
        "niter": 2000,
        "gridder": "awproject",
        "deconvolver": "mtmfs",
    }
    mode_mismatches = [
        f"{field}={mode.get(field)!r}"
        for field, expected in expected_mode.items()
        if mode.get(field) != expected
    ]
    if str(mode.get("wprojplanes")) != "32":
        mode_mismatches.append(f"wprojplanes={mode.get('wprojplanes')!r}")
    if mode_mismatches:
        raise CampaignError(
            f"{path}: 4096 workload mode/geometry mismatch: "
            + ", ".join(mode_mismatches)
        )

    expected_field = WORKLOAD_GEOMETRY[workload_kind]["field"]
    expected_parameters = {
        ("imsize", "IMAGER_BENCH_IMSIZE"): "4096",
        ("field", "IMAGER_BENCH_FIELD"): expected_field,
        ("spw", "IMAGER_BENCH_SPW"): "2~17",
        ("mode", "IMAGER_BENCH_MODE"): "clean",
        ("channel_count", "IMAGER_BENCH_CHANNEL_COUNT"): "64",
        ("nterms", "IMAGER_BENCH_NTERMS"): "2",
        ("niter", "IMAGER_BENCH_NITER"): "2000",
        ("wprojplanes", "IMAGER_BENCH_WPROJPLANES"): "32",
        ("gridder", "IMAGER_BENCH_GRIDDER"): "awproject",
        ("deconvolver", "IMAGER_BENCH_DECONVOLVER"): "mtmfs",
    }
    parameter_mismatches = []
    for (parameter, environment), expected in expected_parameters.items():
        observed = _command_parameter(
            result,
            parameter=parameter,
            environment=environment,
        )
        if str(observed).lower() != expected.lower():
            parameter_mismatches.append(f"{parameter}={observed!r}")
    for parameter, environment in (
        ("aterm", "IMAGER_BENCH_ATERM"),
        ("wbawp", "IMAGER_BENCH_WBAWP"),
        ("conjbeams", "IMAGER_BENCH_CONJBEAMS"),
        ("usepointing", "IMAGER_BENCH_USEPOINTING"),
    ):
        observed = _command_parameter(
            result,
            parameter=parameter,
            environment=environment,
        )
        if observed not in {True, 1, "1", "true"}:
            parameter_mismatches.append(f"{parameter}={observed!r}")
    if parameter_mismatches:
        raise CampaignError(
            f"{path}: 4096 workload data/field/SPW selection mismatch: "
            + ", ".join(parameter_mismatches)
        )

    comparison_contract = result.get("comparison")
    if (
        not isinstance(comparison_contract, dict)
        or comparison_contract.get("products") != list(EXPECTED_19_PRODUCTS)
        or comparison_contract.get("require_exact_product_inventory") is not True
        or comparison_contract.get("require_metadata_parity") is not True
    ):
        raise CampaignError(
            f"{path}: workload does not bind the exact 19-product contract"
        )
    embedded = result.get("results")
    embedded = (
        embedded.get("product_comparison") if isinstance(embedded, dict) else None
    )
    if not isinstance(embedded, dict) or canonical_sha256(embedded) != canonical_sha256(
        comparison
    ):
        raise CampaignError(
            f"{path}: workload product comparison is not the referenced comparison"
        )
    return result


def _validate_workload_kind_binding(
    receipt: dict[str, Any],
    *,
    receipt_path: Path,
    workload_kind: str,
) -> None:
    if receipt.get("workload_kind") != workload_kind:
        raise CampaignError(f"{receipt_path}: workload_kind must be {workload_kind!r}")


def validate_promoted_4096_receipt(
    path: Path,
    *,
    workload_kind: str = "single-field",
) -> EvidenceRef:
    """Validate the explicit 4096/full-16-SPW promotion gate."""

    path = path.expanduser().resolve()
    receipt = load_json(path, label="4096 promotion receipt")
    if receipt.get("kind") != "vlass_4096_full16_promotion":
        raise CampaignError(
            f"{path}: kind must be vlass_4096_full16_promotion; a bare comparison "
            "receipt is insufficient because it does not bind full-16-SPW geometry"
        )
    if receipt.get("status") != "promoted":
        raise CampaignError(f"{path}: promotion status must be promoted")
    _validate_workload_kind_binding(
        receipt,
        receipt_path=path,
        workload_kind=workload_kind,
    )
    geometry = receipt.get("geometry")
    expected = _expected_bound_geometry(workload_kind, imsize=4096)
    if not isinstance(geometry, dict) or any(
        geometry.get(key) != value for key, value in expected.items()
    ):
        raise CampaignError(f"{path}: geometry must bind {expected}")
    require_true_gates(receipt, PROMOTION_GATES, receipt_path=path)
    workload_result_path = resolve_receipt_reference(
        path,
        receipt.get("workload_result"),
        field="workload_result",
    )
    comparison_path = resolve_receipt_reference(
        path,
        receipt.get("comparison_receipt"),
        field="comparison_receipt",
    )
    trajectory_path = resolve_receipt_reference(
        path,
        receipt.get("trajectory_receipt"),
        field="trajectory_receipt",
    )
    workload_result_sha256 = require_content_hash(
        receipt,
        receipt_path=path,
        field="workload_result_sha256",
        referenced_path=workload_result_path,
    )
    comparison_receipt_sha256 = require_content_hash(
        receipt,
        receipt_path=path,
        field="comparison_receipt_sha256",
        referenced_path=comparison_path,
    )
    require_content_hash(
        receipt,
        receipt_path=path,
        field="trajectory_receipt_sha256",
        referenced_path=trajectory_path,
    )
    comparison = validate_comparison_receipt(comparison_path)
    validate_4096_workload_result(
        workload_result_path,
        workload_kind=workload_kind,
        comparison=comparison,
    )
    validate_trajectory_receipt(
        trajectory_path,
        expected_geometry=expected,
        workload_result_sha256=workload_result_sha256,
        comparison_receipt_sha256=comparison_receipt_sha256,
    )
    return EvidenceRef(path=str(path), sha256=sha256_file(path))


def validate_common_science_contract(
    manifest: dict[str, Any],
    *,
    mode: str,
    workload_kind: str = "single-field",
) -> None:
    """Reject a base workload that changes the approved VLASS science row."""

    imaging = manifest.get("imaging")
    if not isinstance(imaging, dict):
        raise CampaignError("base workload imaging must be a JSON object")
    if workload_kind not in WORKLOAD_GEOMETRY:
        raise CampaignError(f"unsupported workload kind: {workload_kind}")
    workload_geometry = WORKLOAD_GEOMETRY[workload_kind]
    expected = {
        "imsize": 12150,
        "cell_arcsec": 0.6,
        "field": workload_geometry["field"],
        "phasecenter_field": 1525,
        "spw": "2~17",
        "channel_start": 0,
        "channel_count": 64,
        "specmode": "mfs",
        "gridder": "awproject",
        "wterm": "wproject",
        "wprojplanes": 32,
        "interpolation": "linear",
        "weighting": "briggs",
        "robust": 1.0,
        "perchanweightdensity": True,
        "deconvolver": "mtmfs",
        "nterms": 2,
        "scales": [0, 5, 12],
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
        "minor_cycle_length": 2000,
        "cyclefactor": 3.0,
        "min_psf_fraction": 0.05,
        "max_psf_fraction": 0.8,
    }
    mismatches = [
        f"{key}={imaging.get(key)!r} (expected {value!r})"
        for key, value in expected.items()
        if imaging.get(key) != value
    ]
    expected_mode = "dirty" if mode == "dirty" else "clean"
    if imaging.get("mode") != expected_mode:
        mismatches.append(f"mode={imaging.get('mode')!r} (expected {expected_mode!r})")
    expected_niter = 0 if mode == "dirty" else 2000
    if imaging.get("niter") != expected_niter:
        mismatches.append(f"niter={imaging.get('niter')!r} (expected {expected_niter})")
    if mismatches:
        raise CampaignError(
            "base workload changes the approved VLASS contract: "
            + "; ".join(mismatches)
        )
    casa = manifest.get("casa")
    if (
        not isinstance(casa, dict)
        or casa.get("dataset_selection") != workload_geometry["dataset_selection"]
    ):
        raise CampaignError(
            "base workload must bind casa.dataset_selection="
            f"{workload_geometry['dataset_selection']!r} for {workload_kind}"
        )
    run = manifest.get("run")
    if not isinstance(run, dict) or not run.get("reuse_casa_prefix"):
        raise CampaignError("base workload must reuse a frozen CASA product prefix")
    comparison = manifest.get("comparison")
    products = comparison.get("products") if isinstance(comparison, dict) else None
    expected_products = (
        EXPECTED_19_PRODUCTS if mode == "clean" else EXPECTED_DIRTY_PRODUCTS
    )
    if products != list(expected_products):
        raise CampaignError(
            f"{mode} base workload must preserve the exact ordered "
            f"{len(expected_products)}-product frozen contract"
        )
    if mode == "clean":
        if not imaging.get("mask_image"):
            raise CampaignError(
                "clean base workload must preserve its deterministic mask"
            )


def derive_rust_only_manifest(
    base: dict[str, Any],
    *,
    mode: str,
    policy: str,
    campaign_label: str,
    memory_target_mb: int | None,
    workload_kind: str = "single-field",
) -> dict[str, Any]:
    """Create one single-run manifest while preserving science parameters."""

    if policy not in POLICIES:
        raise CampaignError(f"unsupported memory policy: {policy}")
    validate_common_science_contract(
        base,
        mode=mode,
        workload_kind=workload_kind,
    )
    manifest = copy.deepcopy(base)
    manifest["id"] = f"{campaign_label}-{mode}-{policy}"
    manifest.pop("casa", None)
    imaging = manifest["imaging"]
    imaging["imaging_memory_pressure_policy"] = policy
    if memory_target_mb is None:
        imaging.pop("imaging_memory_target_mb", None)
    else:
        if memory_target_mb < 1:
            raise CampaignError("--memory-target-mb must be positive")
        imaging["imaging_memory_target_mb"] = memory_target_mb
    run = manifest["run"]
    run.update(
        {
            "repeats": 1,
            "warmups": 0,
            "profile_repeats": 1,
            "stream_log": True,
            "skip_casa": "1",
            "skip_rust": "0",
            "skip_profile": "1",
            "run_label": f"{campaign_label}-{mode}-{policy}",
            "evidence_role": "vlass_full_geometry_memory_campaign",
            "preverified_warm_cache": True,
        }
    )
    run_env = run.setdefault("env", {})
    if not isinstance(run_env, dict):
        raise CampaignError("base workload run.env must be a JSON object")
    run_env.pop(SPILL_READ_BANDWIDTH_ENV, None)
    run_env.pop(SPILL_WRITE_BANDWIDTH_ENV, None)
    run_env["CASA_RS_STANDARD_MFS_PROFILE_DETAIL"] = "1"
    return manifest


def _non_negative_number(value: Any) -> bool:
    return (
        not isinstance(value, bool) and isinstance(value, (int, float)) and value >= 0
    )


def _sample_delta(
    samples: list[dict[str, Any]],
    field: str,
) -> int | None:
    observed = [
        sample[field]
        for sample in samples
        if isinstance(sample, dict)
        and isinstance(sample.get(field), int)
        and not isinstance(sample.get(field), bool)
    ]
    return max(0, observed[-1] - observed[0]) if observed else None


def _canonical_memory_stage(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value.replace("_", "-")


def required_runtime_memory_stages(mode: str) -> tuple[str, ...]:
    if mode == "clean":
        return REQUIRED_LIFETIME_STAGES
    return (
        "prepare",
        "source-ingest",
        "weighting",
        "initial-grid",
        "dirty-transform",
        "finish",
        "product-materialization",
        "product-write",
    )


def validate_stage_memory_contract(
    records: list[dict[str, Any]],
    *,
    mode: str,
) -> dict[str, Any]:
    """Validate a stage-local receipt for every memory and pressure dimension."""

    required_stages = required_runtime_memory_stages(mode)
    records_by_stage: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        stage = _canonical_memory_stage(record.get("stage"))
        if stage is not None:
            records_by_stage.setdefault(stage, []).append(record)

    missing_stages: list[str] = []
    missing_fields_by_stage: dict[str, list[str]] = {}
    incomplete_peak_stages: list[str] = []
    observed_stage_elapsed_ms: dict[str, float] = {}
    for stage in required_stages:
        stage_records = records_by_stage.get(stage, [])
        if not stage_records:
            missing_stages.append(stage)
            continue
        complete = [
            record
            for record in stage_records
            if all(
                _non_negative_number(record.get(field))
                for field in REQUIRED_STAGE_MEMORY_FIELDS
            )
        ]
        if not complete:
            observed_fields = {
                field
                for record in stage_records
                for field in REQUIRED_STAGE_MEMORY_FIELDS
                if _non_negative_number(record.get(field))
            }
            missing_fields_by_stage[stage] = [
                field
                for field in REQUIRED_STAGE_MEMORY_FIELDS
                if field not in observed_fields
            ]
            continue
        if not any(
            record.get("peak_observation_complete") is True for record in complete
        ):
            incomplete_peak_stages.append(stage)
            continue
        complete_peak_records = [
            record
            for record in complete
            if record.get("peak_observation_complete") is True
        ]
        observed_stage_elapsed_ms[stage] = max(
            float(record["elapsed_monotonic_ms"]) for record in complete_peak_records
        )

    ordered_elapsed = [
        observed_stage_elapsed_ms[stage]
        for stage in required_stages
        if stage in observed_stage_elapsed_ms
    ]
    elapsed_monotonic = (
        len(ordered_elapsed) == len(required_stages)
        and all(
            right >= left for left, right in zip(ordered_elapsed, ordered_elapsed[1:])
        )
        and (len(ordered_elapsed) < 2 or ordered_elapsed[-1] > ordered_elapsed[0])
    )

    return {
        "complete": not missing_stages
        and not missing_fields_by_stage
        and not incomplete_peak_stages
        and elapsed_monotonic,
        "required_stages": list(required_stages),
        "required_fields": list(REQUIRED_STAGE_MEMORY_FIELDS),
        "observed_stages": sorted(records_by_stage),
        "missing_stages": missing_stages,
        "missing_fields_by_stage": missing_fields_by_stage,
        "incomplete_peak_stages": incomplete_peak_stages,
        "observed_stage_elapsed_ms": observed_stage_elapsed_ms,
        "elapsed_monotonic": elapsed_monotonic,
        "production_schema_note": (
            "Each stage requires process footprint/RSS, CPU/Metal/unified allocations, "
            "compression/swap, faults, process/external I/O, GPU stall, and an explicit "
            "complete-peak receipt. Phase-transition-only samples are insufficient."
        ),
    }


def resolved_memory_target_evidence(
    planning_resources: dict[str, Any],
    *,
    policy: str,
    requested_memory_target_mb: int | None,
    execution_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate incremental-residency targets and process-total projections."""

    observed = planning_resources.get("memory_target_bytes")
    physical = planning_resources.get("physical_memory_bytes")
    headroom = planning_resources.get("no_swap_headroom_bytes")
    baseline = planning_resources.get("process_physical_footprint_bytes")
    process_baseline = planning_resources.get("process_baseline_bytes")
    process_ceiling = planning_resources.get("process_total_ceiling_bytes")
    process_ceiling_origin = planning_resources.get("process_total_ceiling_origin")
    operation_budget = planning_resources.get("incremental_operation_budget_bytes")
    target_projected_total = planning_resources.get(
        "target_projected_process_total_bytes"
    )
    target_projected_excess = planning_resources.get(
        "target_projected_process_excess_bytes"
    )
    semantics = planning_resources.get("memory_target_semantics")
    origin = planning_resources.get("memory_target_origin")
    requested: int | None = None
    expected: int | None = None
    expected_process_ceiling: int | None = None
    expected_operation_budget: int | None = None
    expected_target_projected_total: int | None = None
    expected_target_projected_excess: int | None = None
    expected_origins: set[str] = set()
    formula = "unresolved"
    missing_inputs: list[str] = []
    mismatches: list[str] = []

    def require_non_negative_integer(field: str, value: Any) -> bool:
        valid = isinstance(value, int) and not isinstance(value, bool) and value >= 0
        if not valid:
            missing_inputs.append(field)
        return valid

    if requested_memory_target_mb is not None:
        if (
            isinstance(requested_memory_target_mb, int)
            and not isinstance(requested_memory_target_mb, bool)
            and requested_memory_target_mb > 0
        ):
            requested = requested_memory_target_mb * MIB
        else:
            missing_inputs.append("explicit_memory_target")

    planning_values_valid = all(
        (
            require_non_negative_integer("memory_target_bytes", observed),
            require_non_negative_integer("physical_memory_bytes", physical),
            require_non_negative_integer("no_swap_headroom_bytes", headroom),
            require_non_negative_integer(
                "process_physical_footprint_bytes",
                baseline,
            ),
            require_non_negative_integer(
                "process_baseline_bytes",
                process_baseline,
            ),
            require_non_negative_integer(
                "process_total_ceiling_bytes",
                process_ceiling,
            ),
            require_non_negative_integer(
                "incremental_operation_budget_bytes",
                operation_budget,
            ),
            require_non_negative_integer(
                "target_projected_process_total_bytes",
                target_projected_total,
            ),
            require_non_negative_integer(
                "target_projected_process_excess_bytes",
                target_projected_excess,
            ),
        )
    )
    if semantics != "incremental-operation-residency":
        mismatches.append("memory_target_semantics")
    if planning_values_valid:
        expected_process_ceiling = min(physical, baseline + headroom)
        expected_operation_budget = max(0, expected_process_ceiling - baseline)
        expected_target_projected_total = baseline + observed
        expected_target_projected_excess = max(
            0,
            expected_target_projected_total - expected_process_ceiling,
        )
        if process_baseline != baseline:
            mismatches.append("process_baseline_bytes")
        if process_ceiling != expected_process_ceiling:
            mismatches.append("process_total_ceiling_bytes")
        if (
            process_ceiling_origin
            != "baseline-plus-no-swap-headroom-capped-to-physical"
        ):
            mismatches.append("process_total_ceiling_origin")
        if operation_budget != expected_operation_budget:
            mismatches.append("incremental_operation_budget_bytes")
        if target_projected_total != expected_target_projected_total:
            mismatches.append("target_projected_process_total_bytes")
        if target_projected_excess != expected_target_projected_excess:
            mismatches.append("target_projected_process_excess_bytes")

        if policy == "oversubscribe":
            formula = "explicit-request"
            if requested is None:
                missing_inputs.append("explicit_memory_target")
            else:
                expected = requested
                expected_origins.add("cli-intentional-oversubscription")
        elif policy in {
            "conservative-no-swap",
            "aggressive",
            "stage-aware",
            "hybrid",
        }:
            formula = (
                "min(explicit-request, incremental-operation-budget)"
                if requested is not None
                else "incremental-operation-budget"
            )
            expected = (
                expected_operation_budget
                if requested is None
                else min(requested, expected_operation_budget)
            )
            if policy in {"conservative-no-swap", "stage-aware"}:
                if requested is None:
                    expected_origins.add("available-memory-ledger")
                elif requested <= expected_operation_budget:
                    expected_origins.update({"cli-imaging", "cli-standard-mfs"})
                else:
                    expected_origins.add("cli-capped-to-no-swap-headroom")
            else:
                expected_origins.add(
                    (
                        f"{policy}-physical-ledger"
                        if requested is None
                        else f"cli-{policy}-physical-ceiling"
                    )
                )
        else:
            missing_inputs.append("known_memory_pressure_policy")

    if planning_values_valid and observed <= 0:
        mismatches.append("positive_memory_target_bytes")
    if planning_values_valid and observed != expected:
        mismatches.append("memory_target_bytes")
    if origin not in expected_origins:
        mismatches.append("memory_target_origin")

    execution_projection: dict[str, Any] = {
        "required": execution_plan is not None,
        "memory_target_semantics": None,
        "memory_target_bytes": None,
        "process_baseline_bytes": None,
        "process_total_ceiling_bytes": None,
        "planned_peak_bytes": None,
        "projected_process_total_bytes": None,
        "expected_projected_process_total_bytes": None,
        "projected_process_excess_bytes": None,
        "expected_projected_process_excess_bytes": None,
    }
    if execution_plan is not None:
        execution_semantics = execution_plan.get("memory_target_semantics")
        execution_target = execution_plan.get("memory_target_bytes")
        execution_baseline = execution_plan.get("process_baseline_bytes")
        execution_ceiling = execution_plan.get("process_total_ceiling_bytes")
        planned_peak = execution_plan.get("planned_peak_bytes")
        projected_total = execution_plan.get("projected_process_total_bytes")
        projected_excess = execution_plan.get("projected_process_excess_bytes")
        execution_projection.update(
            {
                "memory_target_semantics": execution_semantics,
                "memory_target_bytes": execution_target,
                "process_baseline_bytes": execution_baseline,
                "process_total_ceiling_bytes": execution_ceiling,
                "planned_peak_bytes": planned_peak,
                "projected_process_total_bytes": projected_total,
                "projected_process_excess_bytes": projected_excess,
            }
        )
        execution_values_valid = all(
            (
                require_non_negative_integer(
                    "execution_plan.memory_target_bytes",
                    execution_target,
                ),
                require_non_negative_integer(
                    "execution_plan.process_baseline_bytes",
                    execution_baseline,
                ),
                require_non_negative_integer(
                    "execution_plan.process_total_ceiling_bytes",
                    execution_ceiling,
                ),
                require_non_negative_integer(
                    "execution_plan.planned_peak_bytes",
                    planned_peak,
                ),
                require_non_negative_integer(
                    "execution_plan.projected_process_total_bytes",
                    projected_total,
                ),
                require_non_negative_integer(
                    "execution_plan.projected_process_excess_bytes",
                    projected_excess,
                ),
            )
        )
        if execution_semantics != "incremental-operation-residency":
            mismatches.append("execution_plan.memory_target_semantics")
        if execution_values_valid and planning_values_valid:
            expected_execution_total = baseline + planned_peak
            expected_execution_excess = max(
                0,
                expected_execution_total - expected_process_ceiling,
            )
            execution_projection.update(
                {
                    "expected_projected_process_total_bytes": (
                        expected_execution_total
                    ),
                    "expected_projected_process_excess_bytes": (
                        expected_execution_excess
                    ),
                }
            )
            if execution_target != observed:
                mismatches.append("execution_plan.memory_target_bytes")
            if execution_baseline != baseline:
                mismatches.append("execution_plan.process_baseline_bytes")
            if execution_ceiling != expected_process_ceiling:
                mismatches.append("execution_plan.process_total_ceiling_bytes")
            if projected_total != expected_execution_total:
                mismatches.append("execution_plan.projected_process_total_bytes")
            if projected_excess != expected_execution_excess:
                mismatches.append("execution_plan.projected_process_excess_bytes")

    missing_inputs = list(dict.fromkeys(missing_inputs))
    mismatches = list(dict.fromkeys(mismatches))

    return {
        "matches": not missing_inputs and not mismatches,
        "policy": policy,
        "formula": formula,
        "semantics": semantics,
        "expected_semantics": "incremental-operation-residency",
        "requested_bytes": requested,
        "observed_bytes": observed,
        "expected_bytes": expected,
        "origin": origin,
        "expected_origins": sorted(expected_origins),
        "physical_memory_bytes": physical,
        "no_swap_headroom_bytes": headroom,
        "process_physical_footprint_bytes": baseline,
        "process_baseline_bytes": process_baseline,
        "process_total_ceiling_bytes": process_ceiling,
        "expected_process_total_ceiling_bytes": expected_process_ceiling,
        "process_total_ceiling_origin": process_ceiling_origin,
        "incremental_operation_budget_bytes": operation_budget,
        "expected_incremental_operation_budget_bytes": (expected_operation_budget),
        "target_projected_process_total_bytes": target_projected_total,
        "expected_target_projected_process_total_bytes": (
            expected_target_projected_total
        ),
        "target_projected_process_excess_bytes": target_projected_excess,
        "expected_target_projected_process_excess_bytes": (
            expected_target_projected_excess
        ),
        "execution_projection": execution_projection,
        "missing_inputs": missing_inputs,
        "mismatches": mismatches,
    }


def _component_lifetime_rows(
    lifetimes: list[dict[str, Any]],
    component: str,
) -> list[dict[str, Any]]:
    rows = [
        row
        for row in lifetimes
        if isinstance(row, dict) and row.get("component") == component
    ]
    return sorted(
        rows,
        key=lambda row: (
            row.get("allocation_id", ""),
            row.get("residency_index", -1),
        ),
    )


def validate_full_geometry_lifetime_contract(
    *,
    mode: str,
    allocation_bytes_by_component: dict[str, int],
    lifetimes: list[dict[str, Any]],
    execution_plan: dict[str, Any],
    planning_resources: dict[str, Any],
    runtime_actions: dict[str, Any],
    compact_replay: dict[str, Any],
) -> dict[str, Any]:
    """Check deterministic 12,150-square allocation formulas and backing lifetimes."""

    mismatches: list[str] = []
    exact_expected = {
        component: bytes_
        for component, bytes_ in FULL_GEOMETRY_EXACT_COMPONENT_BYTES.items()
        if component
        not in {
            "mosaic weighting density maps",
            "AWProject compensated f64 readback",
            "AWProject CASA-layout model FFT staging",
            "AWProject MT-MFS bounded multiscale scratch",
        }
    }
    if mode == "clean":
        exact_expected.update(
            {
                component: FULL_GEOMETRY_EXACT_COMPONENT_BYTES[component]
                for component in (
                    "AWProject CASA-layout model FFT staging",
                    "AWProject MT-MFS bounded multiscale scratch",
                )
            }
        )
    metal_eligible = execution_plan.get("metal_eligible") is True
    if metal_eligible:
        exact_expected["AWProject compensated f64 readback"] = (
            FULL_GEOMETRY_EXACT_COMPONENT_BYTES["AWProject compensated f64 readback"]
        )
    target_bytes = planning_resources.get("memory_target_bytes")
    if isinstance(target_bytes, int) and not isinstance(target_bytes, bool):
        exact_expected["AWProject safety margin"] = target_bytes // 20
    else:
        mismatches.append("AWProject safety margin: planner target unavailable")

    for component, expected_bytes in exact_expected.items():
        observed = allocation_bytes_by_component.get(component)
        if observed != expected_bytes:
            mismatches.append(
                f"{component}: expected {expected_bytes} bytes, observed {observed!r}"
            )
    density_component = next(
        (
            component
            for component in ("mosaic weighting density maps", "weighting density")
            if component in allocation_bytes_by_component
        ),
        None,
    )
    expected_density = FULL_GEOMETRY_EXACT_COMPONENT_BYTES[
        "mosaic weighting density maps"
    ]
    if (
        density_component is None
        or allocation_bytes_by_component.get(density_component) != expected_density
    ):
        mismatches.append(
            "weighting density: expected one "
            f"{expected_density}-byte f32 image-density grid"
        )
    fft_bytes = allocation_bytes_by_component.get("FFT chunks")
    if fft_bytes not in FULL_GEOMETRY_FFT_BYTES_ALLOWED:
        mismatches.append(
            f"FFT chunks: observed {fft_bytes!r}, expected one admitted exact formula "
            f"{sorted(FULL_GEOMETRY_FFT_BYTES_ALLOWED)}"
        )

    for component in (
        "source row blocks",
        "AWProject CF pixels",
        "AWProject source-order tap scratch",
        "AWProject CF index",
        "POINTING index",
    ):
        if allocation_bytes_by_component.get(component, 0) <= 0:
            mismatches.append(f"{component}: positive allocation is required")
    if (
        metal_eligible
        and allocation_bytes_by_component.get("direct Metal host scratch", 0) <= 0
    ):
        mismatches.append("direct Metal host scratch: positive allocation is required")

    expected_grid_backing = "UnifiedMemory" if metal_eligible else "HostHeap"
    grid_rows = _component_lifetime_rows(lifetimes, "grids")
    expected_grid_rows = 2 if mode == "clean" else 1
    if len(grid_rows) != expected_grid_rows:
        mismatches.append(
            f"grids: expected {expected_grid_rows} residency interval(s), "
            f"observed {len(grid_rows)}"
        )
    else:
        expected_residencies = [
            (
                FULL_GEOMETRY_EXACT_COMPONENT_BYTES["grids"],
                "initial-grid",
                "dirty-transform",
            )
        ]
        if mode == "clean":
            expected_residencies.append(
                (
                    FULL_GEOMETRY_RESIDUAL_GRID_BYTES,
                    "residual-grid",
                    "residual-transform",
                )
            )
        for row, (bytes_, live_from, live_through) in zip(
            grid_rows,
            expected_residencies,
            strict=True,
        ):
            if (
                row.get("backing") != expected_grid_backing
                or row.get("resident_bytes") != bytes_
                or row.get("stored_bytes") != 0
                or row.get("live_from") != live_from
                or row.get("live_through") != live_through
            ):
                mismatches.append(
                    "grids: residency does not match exact "
                    f"{bytes_}-byte {expected_grid_backing} {live_from}..{live_through}"
                )

    def expect_residencies(
        component: str,
        expected: list[tuple[int, str, str, str]],
    ) -> None:
        rows = _component_lifetime_rows(lifetimes, component)
        if len(rows) != len(expected):
            mismatches.append(
                f"{component}: expected {len(expected)} exact residency interval(s), "
                f"observed {len(rows)}"
            )
            return
        for row, (bytes_, backing, live_from, live_through) in zip(
            rows,
            expected,
            strict=True,
        ):
            if (
                row.get("resident_bytes") != bytes_
                or row.get("stored_bytes") != 0
                or row.get("backing") != backing
                or row.get("live_from") != live_from
                or row.get("live_through") != live_through
            ):
                mismatches.append(
                    f"{component}: expected exact {bytes_}-byte {backing} "
                    f"{live_from}..{live_through} residency"
                )

    residual_last_use = "residual-grid" if mode == "clean" else "initial-grid"
    source_bytes = allocation_bytes_by_component.get("source row blocks", 0)
    source_residencies = [(source_bytes, "HostHeap", "source-ingest", "initial-grid")]
    if mode == "clean":
        source_residencies.append(
            (source_bytes, "HostHeap", "residual-grid", "residual-grid")
        )
    expect_residencies("source row blocks", source_residencies)

    fft_residencies = [
        (fft_bytes or 0, "HostHeap", "dirty-transform", "dirty-transform")
    ]
    if mode == "clean":
        fft_residencies.append(
            (
                fft_bytes or 0,
                "HostHeap",
                "residual-transform",
                "residual-transform",
            )
        )
    expect_residencies("FFT chunks", fft_residencies)
    expect_residencies(
        "AWProject MT-MFS run state",
        [
            (
                FULL_GEOMETRY_EXACT_COMPONENT_BYTES["AWProject MT-MFS run state"],
                "HostHeap",
                "dirty-transform",
                "finish",
            )
        ],
    )
    expect_residencies(
        density_component or "mosaic weighting density maps",
        [
            (
                expected_density,
                "HostHeap",
                "weighting",
                residual_last_use,
            )
        ],
    )
    for component in ("AWProject CF pixels", "AWProject CF index", "POINTING index"):
        expect_residencies(
            component,
            [
                (
                    allocation_bytes_by_component.get(component, 0),
                    "HostHeap",
                    "prepare",
                    residual_last_use,
                )
            ],
        )
    expect_residencies(
        "AWProject source-order tap scratch",
        [
            (
                allocation_bytes_by_component.get(
                    "AWProject source-order tap scratch",
                    0,
                ),
                "HostHeap",
                "initial-grid",
                residual_last_use,
            )
        ],
    )
    expect_residencies(
        "AWProject safety margin",
        [
            (
                exact_expected.get("AWProject safety margin", 0),
                "HostHeap",
                "prepare",
                "product-write",
            )
        ],
    )
    expect_residencies(
        "AWProject MT-MFS finish state",
        [
            (
                FULL_GEOMETRY_EXACT_COMPONENT_BYTES["AWProject MT-MFS finish state"],
                "HostHeap",
                "finish",
                "finish",
            )
        ],
    )
    storage_demotion_active = runtime_actions.get("storage_demotion_active") is True
    if not storage_demotion_active:
        expect_residencies(
            "AWProject MT-MFS product state",
            [
                (
                    FULL_GEOMETRY_EXACT_COMPONENT_BYTES[
                        "AWProject MT-MFS product state"
                    ],
                    "HostHeap",
                    "product-materialization",
                    "product-write",
                )
            ],
        )
    expect_residencies(
        "product writer scratch",
        [
            (
                FULL_GEOMETRY_EXACT_COMPONENT_BYTES["product writer scratch"],
                "HostHeap",
                "product-write",
                "product-write",
            )
        ],
    )
    if mode == "clean":
        expect_residencies(
            "AWProject CASA-layout model FFT staging",
            [
                (
                    FULL_GEOMETRY_EXACT_COMPONENT_BYTES[
                        "AWProject CASA-layout model FFT staging"
                    ],
                    "HostHeap",
                    "model-transform",
                    "model-transform",
                )
            ],
        )
        expect_residencies(
            "AWProject MT-MFS bounded multiscale scratch",
            [
                (
                    FULL_GEOMETRY_EXACT_COMPONENT_BYTES[
                        "AWProject MT-MFS bounded multiscale scratch"
                    ],
                    "HostHeap",
                    "minor-cycle",
                    "minor-cycle",
                )
            ],
        )
    if metal_eligible:
        readback_bytes = FULL_GEOMETRY_EXACT_COMPONENT_BYTES[
            "AWProject compensated f64 readback"
        ]
        readback_residencies = [
            (
                readback_bytes,
                "HostHeap",
                "dirty-transform",
                "dirty-transform",
            )
        ]
        if mode == "clean":
            readback_residencies.append(
                (
                    readback_bytes,
                    "HostHeap",
                    "residual-transform",
                    "residual-transform",
                )
            )
        expect_residencies(
            "AWProject compensated f64 readback",
            readback_residencies,
        )

    host_components = {
        *exact_expected,
        "FFT chunks",
        "source row blocks",
        "AWProject CF pixels",
        "AWProject source-order tap scratch",
        "AWProject CF index",
        "POINTING index",
        density_component,
    }
    host_components.discard("grids")
    host_components.discard(None)
    if storage_demotion_active:
        host_components.discard("AWProject MT-MFS product state")
    for component in sorted(host_components):
        rows = _component_lifetime_rows(lifetimes, component)
        if not rows:
            mismatches.append(f"{component}: no lifetime residency rows")
        elif any(row.get("backing") != "HostHeap" for row in rows):
            mismatches.append(f"{component}: expected HostHeap backing")

    logical_bytes_by_allocation: dict[str, int] = {}
    component_by_allocation: dict[str, str] = {}
    for row in lifetimes:
        allocation_id = row.get("allocation_id")
        component = row.get("component")
        logical_bytes = row.get("logical_bytes")
        resident_bytes = row.get("resident_bytes")
        stored_bytes = row.get("stored_bytes")
        backing = row.get("backing")
        if (
            not isinstance(allocation_id, str)
            or not allocation_id
            or not isinstance(component, str)
            or not component
            or not isinstance(logical_bytes, int)
            or isinstance(logical_bytes, bool)
            or logical_bytes < 0
        ):
            mismatches.append(
                "lifetime row has no valid allocation/component/logical bytes"
            )
            continue
        previous = logical_bytes_by_allocation.setdefault(
            allocation_id,
            logical_bytes,
        )
        previous_component = component_by_allocation.setdefault(
            allocation_id,
            component,
        )
        if previous != logical_bytes or previous_component != component:
            mismatches.append(
                f"{allocation_id}: lifetime rows disagree on component/logical bytes"
            )
        if (
            not isinstance(resident_bytes, int)
            or isinstance(resident_bytes, bool)
            or resident_bytes < 0
            or resident_bytes > logical_bytes
            or not isinstance(stored_bytes, int)
            or isinstance(stored_bytes, bool)
            or stored_bytes < 0
            or stored_bytes > logical_bytes
        ):
            mismatches.append(
                f"{allocation_id}: residency bytes must be bounded by logical bytes"
            )
        if backing not in {
            "HostHeap",
            "UnifiedMemory",
            "MetalPrivate",
            "MemoryMapped",
            "TemporarySpill",
        }:
            mismatches.append(f"{allocation_id}: unsupported backing {backing!r}")
    lifetime_logical_by_component: dict[str, int] = {}
    for allocation_id, logical_bytes in logical_bytes_by_allocation.items():
        component = component_by_allocation[allocation_id]
        lifetime_logical_by_component[component] = (
            lifetime_logical_by_component.get(component, 0) + logical_bytes
        )
    for component, allocated_bytes in allocation_bytes_by_component.items():
        if lifetime_logical_by_component.get(component) != allocated_bytes:
            mismatches.append(
                f"{component}: allocated {allocated_bytes} bytes but lifetime ledger "
                f"accounts {lifetime_logical_by_component.get(component)!r}"
            )

    if metal_eligible:
        direct_rows = _component_lifetime_rows(
            lifetimes,
            "direct Metal host scratch",
        )
        if not direct_rows or any(
            row.get("backing") != "UnifiedMemory" for row in direct_rows
        ):
            mismatches.append(
                "direct Metal host scratch: expected UnifiedMemory backing"
            )

    if not storage_demotion_active:
        unexpected_stored = [
            row.get("allocation_id")
            for row in lifetimes
            if isinstance(row, dict)
            and (
                row.get("stored_bytes") not in {0, None}
                or row.get("backing") in {"MemoryMapped", "TemporarySpill"}
            )
        ]
        if unexpected_stored:
            mismatches.append(
                "stored residency exists while storage demotion is inactive: "
                + ", ".join(str(value) for value in unexpected_stored)
            )
    else:
        product_rows = _component_lifetime_rows(
            lifetimes,
            "AWProject MT-MFS product state",
        )
        product_streamed_or_demoted = bool(product_rows) and (
            any(
                row.get("backing") in {"MemoryMapped", "TemporarySpill"}
                and isinstance(row.get("stored_bytes"), int)
                and row.get("stored_bytes", 0) > 0
                for row in product_rows
            )
            or all(
                isinstance(row.get("resident_bytes"), int)
                and row.get("resident_bytes", 0)
                < FULL_GEOMETRY_EXACT_COMPONENT_BYTES["AWProject MT-MFS product state"]
                for row in product_rows
            )
        )
        if not product_streamed_or_demoted:
            mismatches.append(
                "AWProject MT-MFS product state: active storage demotion must "
                "prove streaming or mapped/spilled residency"
            )

    replay_evidence: dict[str, Any] = {
        "required": mode == "clean",
        "reference_approximate_bytes": REPLAY_WORKING_SET_REFERENCE_BYTES,
    }
    if mode == "clean":
        final_replay = compact_replay.get("final")
        if not isinstance(final_replay, dict):
            final_replay = {}
        compiled_total_bytes = final_replay.get("compiled_total_bytes")
        compiled_total_bytes_complete = final_replay.get(
            "compiled_total_bytes_complete"
        )
        replay_evidence["compiled_total_bytes"] = compiled_total_bytes
        replay_evidence["compiled_total_bytes_complete"] = compiled_total_bytes_complete
        replay_evidence["compiled_total_bytes_emitted"] = isinstance(
            compiled_total_bytes, int
        ) and not isinstance(compiled_total_bytes, bool)
        if (
            not isinstance(compiled_total_bytes, int)
            or isinstance(compiled_total_bytes, bool)
            or not REPLAY_WORKING_SET_MIN_BYTES
            <= compiled_total_bytes
            <= REPLAY_WORKING_SET_MAX_BYTES
        ):
            mismatches.append(
                "AWProject compact replay: compiled_total_bytes is required and "
                "must account for the approximately 7.31-GiB 16-block working set"
            )
        if compiled_total_bytes_complete is not True:
            mismatches.append(
                "AWProject compact replay: compiled_total_bytes_complete=true is "
                "required to prove the emitted bytes cover the complete 16-block "
                "working set"
            )
        actual_resident = compact_replay.get("actual_resident_bytes")
        if (
            not isinstance(actual_resident, int)
            or isinstance(actual_resident, bool)
            or actual_resident <= 0
        ):
            mismatches.append(
                "AWProject compact replay: positive actual resident bytes are required"
            )
        elif (
            isinstance(compiled_total_bytes, int)
            and actual_resident > compiled_total_bytes
        ):
            mismatches.append(
                "AWProject compact replay: resident bytes exceed compiled total bytes"
            )
        replay_rows = _component_lifetime_rows(
            lifetimes,
            "AWProject compact replay retention",
        )
        resident_replay_rows = [
            row
            for row in replay_rows
            if row.get("backing") == "HostHeap"
            and row.get("live_from") == "residual-grid"
            and row.get("live_through") == "residual-transform"
            and isinstance(row.get("resident_bytes"), int)
            and row.get("resident_bytes", 0) > 0
        ]
        if not storage_demotion_active:
            if len(replay_rows) != 1 or len(resident_replay_rows) != 1:
                mismatches.append(
                    "AWProject compact replay: expected one HostHeap "
                    "residual-grid..residual-transform residency"
                )
        else:
            stored_replay_rows = [
                row
                for row in replay_rows
                if row.get("backing") in {"MemoryMapped", "TemporarySpill"}
                and isinstance(row.get("stored_bytes"), int)
                and row.get("stored_bytes", 0) > 0
                and row.get("live_through")
                in {
                    "initial-grid",
                    "dirty-transform",
                    "model-transform",
                    "minor-cycle",
                }
            ]
            if not resident_replay_rows or not stored_replay_rows:
                mismatches.append(
                    "AWProject compact replay: active demotion requires a "
                    "mapped/spilled pre-residual residency and a prefetched "
                    "HostHeap residual-grid..residual-transform residency"
                )

    return {
        "complete": not mismatches,
        "mismatches": mismatches,
        "exact_expected_bytes_by_component": exact_expected,
        "expected_initial_grid_bytes": FULL_GEOMETRY_EXACT_COMPONENT_BYTES["grids"],
        "expected_residual_grid_bytes": FULL_GEOMETRY_RESIDUAL_GRID_BYTES,
        "allowed_fft_bytes": sorted(FULL_GEOMETRY_FFT_BYTES_ALLOWED),
        "expected_grid_backing": expected_grid_backing,
        "lifetime_logical_bytes_by_component": lifetime_logical_by_component,
        "replay": replay_evidence,
    }


def extract_execution_memory_evidence(
    workload_result: dict[str, Any],
    *,
    mode: str,
    expected_policy: str,
    expected_memory_target_mb: int | None,
    outer_monitor: MonitorResult,
) -> dict[str, Any]:
    """Derive promotion gates from measured workload evidence, never assertions."""

    results = workload_result.get("results")
    if not isinstance(results, dict):
        raise CampaignError("workload result has no structured results")
    backend_logs = results.get("backend_plan_logs")
    memory = (
        backend_logs.get("memory_campaign") if isinstance(backend_logs, dict) else None
    )
    if not isinstance(memory, dict):
        raise CampaignError("workload result has no structured memory campaign logs")
    telemetry = results.get("host_telemetry")
    if not isinstance(telemetry, dict):
        raise CampaignError("workload result has no host telemetry receipt")
    try:
        workload_harness.validate_host_telemetry(telemetry)
    except Exception as error:
        raise CampaignError(f"workload host telemetry is invalid: {error}") from error
    telemetry_summary = telemetry.get("summary")
    if not isinstance(telemetry_summary, dict):
        telemetry_summary = {}
    samples = [
        sample for sample in telemetry.get("samples", []) if isinstance(sample, dict)
    ]

    planning_resources = memory.get("planning_resources")
    if not isinstance(planning_resources, dict):
        planning_resources = {}
    runtime_actions = memory.get("memory_runtime_actions")
    if not isinstance(runtime_actions, dict):
        runtime_actions = {}
    execution_plan = memory.get("execution_plan")
    if not isinstance(execution_plan, dict):
        execution_plan = {}
    ledger = memory.get("ledger_reconciliation")
    if not isinstance(ledger, dict):
        ledger = {}
    stage_memory = memory.get("stage_memory")
    if not isinstance(stage_memory, dict):
        stage_memory = {}
    gpu_waits = memory.get("gpu_waits")
    if not isinstance(gpu_waits, dict):
        gpu_waits = {}
    compact_replay = memory.get("compact_replay")
    if not isinstance(compact_replay, dict):
        compact_replay = {}
    compensated_readback = memory.get("compensated_readback")
    if not isinstance(compensated_readback, dict):
        compensated_readback = {}
    allocations = memory.get("allocations")
    if not isinstance(allocations, list):
        allocations = []
    lifetime_stages = memory.get("lifetime_stages")
    if not isinstance(lifetime_stages, list):
        lifetime_stages = []

    comparison = results.get("product_comparison")
    if not isinstance(comparison, dict):
        comparison = {}
    products = comparison.get("products")
    if not isinstance(products, dict):
        products = {}
    product_inventory = comparison.get("product_inventory")
    if not isinstance(product_inventory, dict):
        product_inventory = {}
    tolerance_evaluation = comparison.get("tolerance_evaluation")
    comparison_tolerance_passed = (
        isinstance(tolerance_evaluation, dict)
        and tolerance_evaluation.get("status") == "passed"
    )
    structured_review = comparison.get("structured_difference_review")
    structured_review_passed = not isinstance(structured_review, dict) or (
        structured_review.get("label") not in {"bad", "investigate"}
    )
    requested_products = comparison.get("requested_products")
    if mode not in {"dirty", "clean"}:
        raise CampaignError(f"unsupported execution evidence mode: {mode}")
    expected_products = (
        EXPECTED_19_PRODUCTS if mode == "clean" else EXPECTED_DIRTY_PRODUCTS
    )
    exact_products = requested_products == list(expected_products)
    product_rows_pass = exact_products and all(
        isinstance(products.get(product), dict)
        and products[product].get("status") == "compared"
        for product in expected_products
    )
    comparison_contract = workload_result.get("comparison")
    if not isinstance(comparison_contract, dict):
        comparison_contract = {}
    tolerances = comparison_contract.get("tolerances")
    default_tolerance = (
        tolerances.get("default") if isinstance(tolerances, dict) else None
    )
    numerical_topology_metadata_contract = (
        comparison_contract.get("products") == list(expected_products)
        and comparison_contract.get("require_exact_product_inventory") is True
        and comparison_contract.get("require_metadata_parity") is True
        and isinstance(default_tolerance, dict)
        and default_tolerance.get("require_topology_parity") is True
    )

    stage_medians = results.get("stage_medians_ms")
    rust_stage_medians = (
        stage_medians.get("rust") if isinstance(stage_medians, dict) else None
    )
    if not isinstance(rust_stage_medians, dict):
        rust_stage_medians = {}
    required_timing_stages = (
        REQUIRED_CLEAN_TIMING_STAGES
        if mode == "clean"
        else REQUIRED_DIRTY_TIMING_STAGES
    )
    missing_timing_stages = [
        stage
        for stage in required_timing_stages
        if not _non_negative_number(rust_stage_medians.get(stage))
    ]
    credible_stage_timings = (
        not missing_timing_stages
        and all(
            isinstance(stage, str) and stage and _non_negative_number(milliseconds)
            for stage, milliseconds in rust_stage_medians.items()
        )
        and all(
            isinstance(rust_stage_medians.get(stage), (int, float))
            and not isinstance(rust_stage_medians.get(stage), bool)
            and rust_stage_medians[stage] > 0
            for stage in ("total", "run_imaging")
        )
    )

    allocation_bytes_by_component: dict[str, int] = {}
    for allocation in allocations:
        if not isinstance(allocation, dict):
            continue
        component = allocation.get("component")
        bytes_ = allocation.get("bytes")
        if (
            isinstance(component, str)
            and component
            and isinstance(bytes_, int)
            and not isinstance(bytes_, bool)
            and bytes_ >= 0
        ):
            allocation_bytes_by_component[component] = (
                allocation_bytes_by_component.get(component, 0) + bytes_
            )
    required_allocation_components = list(REQUIRED_COMMON_ALLOCATION_COMPONENTS)
    if mode == "clean":
        required_allocation_components.extend(REQUIRED_CLEAN_ALLOCATION_COMPONENTS)
    metal_eligible = execution_plan.get("metal_eligible")
    if metal_eligible is True:
        required_allocation_components.append("AWProject compensated f64 readback")
    missing_allocation_components = [
        component
        for component in required_allocation_components
        if allocation_bytes_by_component.get(component, 0) <= 0
    ]
    observed_lifetime_stages = [
        stage.get("stage")
        for stage in lifetime_stages
        if isinstance(stage, dict) and isinstance(stage.get("stage"), str)
    ]
    missing_lifetime_stages = [
        stage
        for stage in REQUIRED_LIFETIME_STAGES
        if stage not in observed_lifetime_stages
    ]
    stage_memory_records = stage_memory.get("records")
    if not isinstance(stage_memory_records, list):
        stage_memory_records = []
    stage_memory_contract = validate_stage_memory_contract(
        [record for record in stage_memory_records if isinstance(record, dict)],
        mode=mode,
    )

    required_telemetry_fields = (
        "process_physical_footprint_bytes_peak",
        "process_resident_memory_bytes_peak",
        "process_page_faults_delta",
        "process_disk_read_bytes_delta",
        "process_disk_write_bytes_delta",
        "spill_volume_read_bytes_delta",
        "spill_volume_write_bytes_delta",
        "host_compressed_memory_bytes_peak",
        "swap_used_bytes_peak",
        "swapin_bytes_delta",
        "swapout_bytes_delta",
    )
    process_sample_fields = (
        "process_pid",
        "process_physical_footprint_bytes",
        "process_physical_footprint_bytes_lifetime_peak",
        "process_resident_memory_bytes",
        "process_page_faults",
        "process_disk_read_bytes",
        "process_disk_write_bytes",
    )
    process_samples = [
        sample
        for sample in samples
        if all(
            _non_negative_number(sample.get(field)) for field in process_sample_fields
        )
    ]
    spill_samples = [
        sample
        for sample in samples
        if _non_negative_number(sample.get("spill_volume_read_bytes"))
        and _non_negative_number(sample.get("spill_volume_write_bytes"))
        and isinstance(sample.get("spill_volume_device"), str)
        and bool(sample.get("spill_volume_device"))
    ]
    telemetry_complete = (
        telemetry.get("status") == "measured"
        and all(
            _non_negative_number(telemetry_summary.get(field))
            for field in required_telemetry_fields
        )
        and len(process_samples) >= 2
        and len(spill_samples) >= 2
    )

    baseline_footprint = planning_resources.get("process_physical_footprint_bytes")
    observed_footprint_peak = telemetry_summary.get(
        "process_physical_footprint_bytes_peak"
    )
    planned_peak = execution_plan.get("lifetime_peak_bytes")
    unaccounted_bytes: int | None = None
    if all(
        isinstance(value, int) and not isinstance(value, bool)
        for value in (baseline_footprint, observed_footprint_peak, planned_peak)
    ):
        observed_growth = max(0, observed_footprint_peak - baseline_footprint)
        unaccounted_bytes = max(0, observed_growth - planned_peak)

    gpu_waits_instrumented = metal_eligible is False or (
        isinstance(gpu_waits.get("record_count"), int)
        and gpu_waits.get("record_count", 0) > 0
    )
    compensated_readback_instrumented = metal_eligible is False or (
        isinstance(compensated_readback.get("record_count"), int)
        and compensated_readback.get("record_count", 0) > 0
        and compensated_readback.get("strategies") == ["sequential-plane"]
        and compensated_readback.get("modeled_overlap_reconciled") is True
        and compensated_readback.get("actual_f64_transient_within_planned_reservation")
        is True
    )
    replay_program_accounted = mode == "dirty" or (
        isinstance(compact_replay.get("actual_resident_bytes"), int)
        and not isinstance(compact_replay.get("actual_resident_bytes"), bool)
        and compact_replay.get("actual_resident_bytes", -1) >= 0
        and all(
            isinstance(compact_replay.get(field), int)
            and not isinstance(compact_replay.get(field), bool)
            and compact_replay.get(field, -1) >= 0
            for field in ("resident_blocks", "partial_blocks", "rejected_blocks")
        )
        and sum(
            compact_replay[field]
            for field in ("resident_blocks", "partial_blocks", "rejected_blocks")
        )
        == 16
    )
    result_mode = workload_result.get("mode")
    if not isinstance(result_mode, dict):
        result_mode = {}
    command = workload_result.get("command")
    command_environment = command.get("env") if isinstance(command, dict) else None
    if not isinstance(command_environment, dict):
        command_environment = {}
    expected_target_environment = (
        str(expected_memory_target_mb)
        if expected_memory_target_mb is not None
        else None
    )
    observed_target_environment = command_environment.get(
        "IMAGER_BENCH_IMAGING_MEMORY_TARGET_MB"
    )
    memory_target_bound = (
        "imaging_memory_target_mb" in result_mode
        and result_mode.get("imaging_memory_target_mb") == expected_memory_target_mb
        and observed_target_environment == expected_target_environment
    )
    target_resolution = resolved_memory_target_evidence(
        planning_resources,
        policy=expected_policy,
        requested_memory_target_mb=expected_memory_target_mb,
        execution_plan=execution_plan,
    )
    runtime = workload_result.get("environment")
    runtime = runtime.get("runtime") if isinstance(runtime, dict) else None
    if not isinstance(runtime, dict):
        runtime = {}
    runtime_platform = runtime.get("platform")
    runtime_machine = runtime.get("machine")
    sample_physical_memory = {
        sample.get("physical_memory_bytes")
        for sample in samples
        if isinstance(sample.get("physical_memory_bytes"), int)
        and not isinstance(sample.get("physical_memory_bytes"), bool)
    }
    acceptance_host_32_gib = (
        runtime.get("physical_memory_bytes") == ACCEPTANCE_PHYSICAL_MEMORY_BYTES
        and isinstance(runtime_platform, str)
        and (
            runtime_platform.lower().startswith("macos")
            or runtime_platform.lower().startswith("darwin")
        )
        and runtime_machine in {"arm64", "aarch64"}
        and sample_physical_memory == {ACCEPTANCE_PHYSICAL_MEMORY_BYTES}
    )
    lifetime_contract = validate_full_geometry_lifetime_contract(
        mode=mode,
        allocation_bytes_by_component=allocation_bytes_by_component,
        lifetimes=[
            lifetime
            for lifetime in memory.get("lifetimes", [])
            if isinstance(lifetime, dict)
        ],
        execution_plan=execution_plan,
        planning_resources=planning_resources,
        runtime_actions=runtime_actions,
        compact_replay=compact_replay,
    )
    correctness_gate_name = (
        "clean_correctness" if mode == "clean" else "dirty_correctness"
    )
    gates = {
        correctness_gate_name: workload_result.get("status") == "completed"
        and comparison.get("status") == "completed"
        and product_inventory.get("status") == "matched"
        and product_rows_pass
        and numerical_topology_metadata_contract
        and comparison_tolerance_passed
        and structured_review_passed,
        "memory_and_swap_receipt": telemetry_complete
        and ledger.get("complete") is True
        and stage_memory_contract["complete"]
        and gpu_waits_instrumented
        and compensated_readback_instrumented
        and runtime_actions_match_policy(runtime_actions, policy=expected_policy),
        "per_stage_memory_telemetry": stage_memory_contract["complete"],
        "no_unaccounted_allocation": ledger.get("complete") is True
        and unaccounted_bytes == 0
        and compact_replay.get("actual_resident_within_planned_reservation")
        is not False
        and compensated_readback.get("actual_f64_transient_within_planned_reservation")
        is not False
        and compensated_readback.get("modeled_overlap_reconciled") is not False,
        "required_allocation_ledger": ledger.get("complete") is True
        and not missing_allocation_components
        and not missing_lifetime_stages
        and replay_program_accounted
        and lifetime_contract["complete"],
        "stage_timings": credible_stage_timings,
        "bounded_operation": outer_monitor.stop_reason is None
        and outer_monitor.exit_code == 0,
        "policy_selected": planning_resources.get("memory_pressure_policy")
        == expected_policy
        and bool(execution_plan)
        and runtime_actions_match_policy(runtime_actions, policy=expected_policy),
        "requested_policy_actions_active": requested_runtime_actions_are_active(
            runtime_actions,
            policy=expected_policy,
        ),
        "memory_target_bound": memory_target_bound and target_resolution["matches"],
        "acceptance_host_32_gib": acceptance_host_32_gib,
    }
    positive = [gate for gate, passed in gates.items() if passed is True]
    negative = [gate for gate, passed in gates.items() if passed is not True]
    return {
        "schema_version": 1,
        "gates": gates,
        "positive_evidence": positive,
        "negative_evidence": negative,
        "planner_and_lifetime_ledger": memory,
        "allocation_contract": {
            "bytes_by_component": allocation_bytes_by_component,
            "required_components": required_allocation_components,
            "missing_components": missing_allocation_components,
            "required_lifetime_stages": list(REQUIRED_LIFETIME_STAGES),
            "observed_lifetime_stages": observed_lifetime_stages,
            "missing_lifetime_stages": missing_lifetime_stages,
            "initial_compensated_grid_exact_bytes": (
                FULL_GEOMETRY_EXACT_COMPONENT_BYTES["grids"]
            ),
            "residual_grid_exact_bytes": FULL_GEOMETRY_RESIDUAL_GRID_BYTES,
            "lifetime_contract": lifetime_contract,
        },
        "process_and_host_memory": {
            "telemetry_status": telemetry.get("status"),
            "telemetry_summary": telemetry_summary,
            "process_sample_count": len(process_samples),
            "spill_volume_sample_count": len(spill_samples),
            "host_compressed_memory_bytes_delta": _sample_delta(
                samples, "host_compressed_memory_bytes"
            ),
            "swap_used_bytes_delta": _sample_delta(samples, "swap_used_bytes"),
            "planner_process_footprint_baseline_bytes": baseline_footprint,
            "observed_process_footprint_peak_bytes": observed_footprint_peak,
            "planned_lifetime_peak_bytes": planned_peak,
            "unaccounted_allocation_bytes": unaccounted_bytes,
            "acceptance_physical_memory_bytes": ACCEPTANCE_PHYSICAL_MEMORY_BYTES,
            "runtime_platform": runtime_platform,
            "runtime_machine": runtime_machine,
            "runtime_physical_memory_bytes": runtime.get("physical_memory_bytes"),
            "sample_physical_memory_bytes": sorted(sample_physical_memory),
        },
        "stage_timings": {
            "milliseconds": rust_stage_medians,
            "required_stages": list(required_timing_stages),
            "missing_stages": missing_timing_stages,
            "memory_telemetry": stage_memory_contract,
        },
        "memory_target": {
            "requested_mb": expected_memory_target_mb,
            "workload_result_mb": result_mode.get("imaging_memory_target_mb"),
            "command_environment_mb": observed_target_environment,
            "planner_target_bytes": planning_resources.get("memory_target_bytes"),
            "resolution": target_resolution,
        },
        "memory_runtime_actions": runtime_actions,
        "gpu_waits": gpu_waits,
        "compact_replay": compact_replay,
        "compensated_readback": compensated_readback,
        "product_comparison": {
            "status": comparison.get("status"),
            "tolerance_evaluation": tolerance_evaluation,
            "structured_difference_review": structured_review,
            "requested_products": requested_products,
            "expected_products": list(expected_products),
            "product_inventory_status": product_inventory.get("status"),
            "exact_frozen_product_contract_compared": product_rows_pass,
        },
        "outer_monitor": asdict(outer_monitor),
    }


def require_derived_experiment_gates(
    experiment: dict[str, Any],
    *,
    experiment_path: Path,
    mode: str,
    workload_kind: str,
    expected_policy: str,
) -> ExperimentWorkloadBinding:
    """Recompute promotion gates from content-addressed underlying measurements."""

    result_reference = experiment.get("run_workload_result")
    if not isinstance(result_reference, dict):
        raise CampaignError(
            f"{experiment_path}: experiment has no workload-result reference"
        )
    workload_result_path = resolve_receipt_reference(
        experiment_path,
        result_reference.get("path"),
        field="run_workload_result.path",
    )
    expected_sha256 = result_reference.get("sha256")
    observed_sha256 = sha256_file(workload_result_path)
    if not isinstance(expected_sha256, str) or expected_sha256 != observed_sha256:
        raise CampaignError(
            f"{experiment_path}: workload-result content hash does not match"
        )
    workload_result = load_json(
        workload_result_path,
        label=f"{mode} workload result",
    )
    workload_results = workload_result.get("results")
    product_comparison = (
        workload_results.get("product_comparison")
        if isinstance(workload_results, dict)
        else None
    )
    if not isinstance(product_comparison, dict):
        raise CampaignError(
            f"{workload_result_path}: workload result has no embedded product comparison"
        )
    validate_rust_only_workload_result(
        workload_result_path,
        expected_status="completed",
    )
    validate_experiment_storage_bandwidth(
        experiment,
        experiment_path=experiment_path,
        workload_result=workload_result,
    )

    monitor_value = experiment.get("outer_monitor")
    expected_monitor_fields = set(MonitorResult.__dataclass_fields__)
    if (
        not isinstance(monitor_value, dict)
        or set(monitor_value) != expected_monitor_fields
    ):
        raise CampaignError(f"{experiment_path}: outer_monitor is missing or malformed")
    try:
        monitor = MonitorResult(**monitor_value)
    except TypeError as error:
        raise CampaignError(
            f"{experiment_path}: outer_monitor is malformed: {error}"
        ) from error
    recomputed = extract_execution_memory_evidence(
        workload_result,
        mode=mode,
        expected_policy=expected_policy,
        expected_memory_target_mb=experiment.get("requested_memory_target_mb"),
        outer_monitor=monitor,
    )
    evidence = experiment.get("memory_evidence")
    if not isinstance(evidence, dict) or canonical_sha256(evidence) != canonical_sha256(
        recomputed
    ):
        raise CampaignError(
            f"{experiment_path}: derived memory evidence does not match "
            "the content-addressed workload result"
        )
    gates = evidence.get("gates")
    if not isinstance(gates, dict):
        raise CampaignError(
            f"{experiment_path}: experiment has no derived memory evidence gates"
        )
    required_gates = CLEAN_EXECUTION_GATES if mode == "clean" else DIRTY_POLICY_GATES
    missing_or_failed = [gate for gate in required_gates if gates.get(gate) is not True]
    if missing_or_failed:
        raise CampaignError(
            f"{experiment_path}: measured gate(s) did not pass: "
            + ", ".join(missing_or_failed)
        )
    _validate_workload_kind_binding(
        experiment,
        receipt_path=experiment_path,
        workload_kind=workload_kind,
    )
    requested_memory_target_mb = experiment.get("requested_memory_target_mb")
    if requested_memory_target_mb is not None and (
        isinstance(requested_memory_target_mb, bool)
        or not isinstance(requested_memory_target_mb, int)
        or requested_memory_target_mb < 1
    ):
        raise CampaignError(
            f"{experiment_path}: requested_memory_target_mb must be null or positive"
        )
    return ExperimentWorkloadBinding(
        workload_result_sha256=observed_sha256,
        product_comparison_sha256=canonical_sha256(product_comparison),
    )


def validate_dirty_policy_promotion(
    path: Path,
    *,
    promoted_4096: EvidenceRef,
    workload_kind: str = "single-field",
) -> tuple[str, EvidenceRef]:
    """Validate reviewed dirty evidence before planning any full clean row."""

    path = path.expanduser().resolve()
    receipt = load_json(path, label="dirty-policy promotion receipt")
    if receipt.get("kind") != "vlass_full_geometry_dirty_policy_promotion":
        raise CampaignError(
            f"{path}: kind must be vlass_full_geometry_dirty_policy_promotion"
        )
    if receipt.get("status") != "passed":
        raise CampaignError(f"{path}: dirty-policy status must be passed")
    _validate_workload_kind_binding(
        receipt,
        receipt_path=path,
        workload_kind=workload_kind,
    )
    policy = receipt.get("policy")
    if policy not in POLICIES:
        raise CampaignError(
            f"{path}: dirty-policy receipt has unknown policy {policy!r}"
        )
    if receipt.get("promotion_4096_sha256") != promoted_4096.sha256:
        raise CampaignError(
            f"{path}: dirty-policy evidence was not gated by the supplied "
            "4096 promotion receipt"
        )
    experiment_path = resolve_receipt_reference(
        path,
        receipt.get("experiment_receipt"),
        field="experiment_receipt",
    )
    require_content_hash(
        receipt,
        receipt_path=path,
        field="experiment_receipt_sha256",
        referenced_path=experiment_path,
    )
    experiment = load_json(experiment_path, label="dirty-policy experiment receipt")
    if experiment.get("kind") != "vlass_full_geometry_memory_experiment":
        raise CampaignError(f"{experiment_path}: wrong experiment receipt kind")
    if (
        experiment.get("status") != "completed"
        or experiment.get("mode") != "dirty"
        or experiment.get("execution_intent") != "execute-12150"
        or experiment.get("policy") != policy
        or experiment.get("workload_kind") != workload_kind
    ):
        raise CampaignError(
            f"{experiment_path}: dirty experiment must be a completed 12,150 "
            "execution for the selected policy"
        )
    if experiment.get("promotion_4096_sha256") != promoted_4096.sha256:
        raise CampaignError(
            f"{experiment_path}: experiment promotion binding does not match"
        )
    require_derived_experiment_gates(
        experiment,
        experiment_path=experiment_path,
        mode="dirty",
        workload_kind=workload_kind,
        expected_policy=policy,
    )
    return policy, EvidenceRef(path=str(path), sha256=sha256_file(path))


def validate_full_geometry_trajectory_receipt(
    path: Path,
    *,
    workload_kind: str,
    workload_result_sha256: str,
    product_comparison_sha256: str,
) -> EvidenceRef:
    """Validate full-size component selection and major-cycle parity evidence."""

    path = path.expanduser().resolve()
    validate_trajectory_receipt(
        path,
        expected_geometry=_expected_bound_geometry(workload_kind, imsize=12150),
        workload_result_sha256=workload_result_sha256,
        product_comparison_sha256=product_comparison_sha256,
    )
    return EvidenceRef(path=str(path), sha256=sha256_file(path))


def validate_clean_promotion_receipt(
    path: Path,
    *,
    promoted_4096: EvidenceRef,
    dirty_policy: EvidenceRef,
    expected_policy: str,
    workload_kind: str,
) -> EvidenceRef:
    """Validate a non-final 12,150 clean memory-campaign promotion."""

    path = path.expanduser().resolve()
    receipt = load_json(path, label="full-geometry clean promotion receipt")
    if receipt.get("kind") != "vlass_full_geometry_clean_promotion":
        raise CampaignError(f"{path}: kind must be vlass_full_geometry_clean_promotion")
    if receipt.get("status") != MEMORY_CAMPAIGN_PROMOTION_STATUS:
        raise CampaignError(
            f"{path}: clean promotion status must be "
            f"{MEMORY_CAMPAIGN_PROMOTION_STATUS!r}"
        )
    if (
        receipt.get("promotion_scope") != MEMORY_CAMPAIGN_PROMOTION_SCOPE
        or receipt.get("final_wave_acceptance")
        != pending_final_wave_acceptance_contract()
    ):
        raise CampaignError(
            f"{path}: clean promotion must be memory-campaign-only and must "
            "preserve the separate unevaluated four-row 10x acceptance contract"
        )
    _validate_workload_kind_binding(
        receipt,
        receipt_path=path,
        workload_kind=workload_kind,
    )
    if receipt.get("policy") != expected_policy:
        raise CampaignError(
            f"{path}: clean promotion policy does not match reviewed dirty policy"
        )
    if receipt.get("promotion_4096_sha256") != promoted_4096.sha256:
        raise CampaignError(f"{path}: 4096 promotion binding does not match")
    if receipt.get("dirty_policy_receipt_sha256") != dirty_policy.sha256:
        raise CampaignError(f"{path}: dirty-policy promotion binding does not match")
    require_true_gates(receipt, CLEAN_PROMOTION_GATES, receipt_path=path)

    experiment_path = resolve_receipt_reference(
        path,
        receipt.get("experiment_receipt"),
        field="experiment_receipt",
    )
    if receipt.get("experiment_receipt_sha256") != sha256_file(experiment_path):
        raise CampaignError(f"{path}: clean experiment content hash does not match")
    experiment = load_json(
        experiment_path,
        label="full-geometry clean experiment receipt",
    )
    if (
        experiment.get("kind") != "vlass_full_geometry_memory_experiment"
        or experiment.get("status") != "completed"
        or experiment.get("mode") != "clean"
        or experiment.get("execution_intent") != "execute-12150"
        or experiment.get("policy") != expected_policy
        or experiment.get("workload_kind") != workload_kind
    ):
        raise CampaignError(
            f"{experiment_path}: clean experiment must be a completed 12,150 "
            "execution for the selected workload and policy"
        )
    if (
        experiment.get("promotion_4096_sha256") != promoted_4096.sha256
        or experiment.get("dirty_policy_receipt_sha256") != dirty_policy.sha256
    ):
        raise CampaignError(
            f"{experiment_path}: clean experiment prerequisite bindings do not match"
        )
    clean_binding = require_derived_experiment_gates(
        experiment,
        experiment_path=experiment_path,
        mode="clean",
        workload_kind=workload_kind,
        expected_policy=expected_policy,
    )
    if (
        receipt.get("workload_result_sha256") != clean_binding.workload_result_sha256
        or receipt.get("product_comparison_sha256")
        != clean_binding.product_comparison_sha256
    ):
        raise CampaignError(
            f"{path}: clean promotion workload/comparison binding does not match"
        )

    trajectory_path = resolve_receipt_reference(
        path,
        receipt.get("trajectory_receipt"),
        field="trajectory_receipt",
    )
    trajectory = validate_full_geometry_trajectory_receipt(
        trajectory_path,
        workload_kind=workload_kind,
        workload_result_sha256=clean_binding.workload_result_sha256,
        product_comparison_sha256=clean_binding.product_comparison_sha256,
    )
    if receipt.get("trajectory_receipt_sha256") != trajectory.sha256:
        raise CampaignError(f"{path}: clean trajectory content hash does not match")
    return EvidenceRef(path=str(path), sha256=sha256_file(path))


def build_workload_command(
    *,
    manifest_path: Path,
    output_dir: Path,
    artifact_root: Path,
    run_label: str,
    dry_run: bool,
) -> list[str]:
    """Build the shared harness command used for all non-planner rows."""

    command = [
        sys.executable,
        str(RUN_WORKLOAD),
        str(manifest_path),
        "--output-dir",
        str(output_dir),
        "--artifact-root",
        str(artifact_root),
        "--repeats",
        "1",
        "--run-label",
        run_label,
        "--stream-log",
    ]
    if dry_run:
        command.append("--dry-run")
    return command


def build_planner_preflight_invocation(
    *,
    manifest_path: Path,
    manifest: dict[str, Any],
    run_label: str,
) -> tuple[list[str], dict[str, str]]:
    """Build the real Rust planner invocation without allocating image products."""

    try:
        workload_harness.validate_workload_manifest(
            manifest,
            source=f"{manifest_path} planner preflight",
        )
    except workload_harness.ContractError as error:
        raise CampaignError(
            f"planner preflight manifest is invalid: {error}"
        ) from error
    plan = workload_harness.build_plan(
        manifest_path=manifest_path,
        manifest=manifest,
        repeats_override=1,
        run_label_override=run_label,
        storage_label_override=None,
        stream_log_override=True,
        dry_run=True,
    )
    command_plan = plan.get("command")
    if (
        not isinstance(command_plan, dict)
        or command_plan.get("kind") != "legacy_benchmark_script"
    ):
        raise CampaignError("planner preflight requires the Rust-only benchmark script")
    command = command_plan.get("argv")
    planned_environment = command_plan.get("env")
    if (
        not isinstance(command, list)
        or not command
        or not all(isinstance(part, str) and part for part in command)
        or not isinstance(planned_environment, dict)
        or not all(
            isinstance(key, str) and key and isinstance(value, str)
            for key, value in planned_environment.items()
        )
    ):
        raise CampaignError("planner preflight command plan is malformed")
    environment = dict(planned_environment)
    environment.update(
        {
            "IMAGER_BENCH_PLAN_ONLY": "1",
            "IMAGER_BENCH_SKIP_CASA": "1",
            "IMAGER_BENCH_SKIP_RUST": "0",
            "IMAGER_BENCH_SKIP_PROFILE": "1",
            "CASA_RS_STANDARD_MFS_PROFILE_DETAIL": "1",
        }
    )
    return list(command), environment


def run_planner_preflight_command(
    command: list[str],
    *,
    planned_environment: dict[str, str],
    stdout_log_path: Path,
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    """Execute one allocation-free planner probe and retain its complete log."""

    stdout_log_path.parent.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ)
    environment.update(planned_environment)
    try:
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        output = error.stdout or ""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        stdout_log_path.write_text(output, encoding="utf-8")
        raise CampaignError(
            f"planner preflight exceeded {timeout_seconds:.1f}s"
        ) from error
    stdout_log_path.write_text(completed.stdout, encoding="utf-8")
    return completed


def planner_preflight_evidence(
    stdout_log_path: Path,
    *,
    expected_policy: str,
    expected_memory_target_mb: int | None,
) -> dict[str, Any]:
    """Parse and validate a planner receipt that opened the real workload inputs."""

    try:
        text = stdout_log_path.read_text(encoding="utf-8")
    except OSError as error:
        raise CampaignError(
            f"cannot read planner preflight log {stdout_log_path}: {error}"
        ) from error
    parsed = workload_harness.parse_backend_plan_logs(text)
    memory = parsed.get("memory_campaign")
    if not isinstance(memory, dict):
        raise CampaignError(
            "planner preflight did not produce structured memory evidence"
        )
    preflight = memory.get("planner_preflight")
    ledger = memory.get("ledger_reconciliation")
    execution_plan = memory.get("execution_plan")
    planning_resources = memory.get("planning_resources")
    runtime_actions = memory.get("memory_runtime_actions")
    allocations = memory.get("allocations")
    if not isinstance(allocations, list):
        allocations = []
    allocation_bytes_by_component: dict[str, int] = {}
    for allocation in allocations:
        if not isinstance(allocation, dict):
            continue
        component = allocation.get("component")
        bytes_ = allocation.get("bytes")
        if (
            isinstance(component, str)
            and component
            and isinstance(bytes_, int)
            and not isinstance(bytes_, bool)
            and bytes_ >= 0
        ):
            allocation_bytes_by_component[component] = (
                allocation_bytes_by_component.get(component, 0) + bytes_
            )
    target_resolution = resolved_memory_target_evidence(
        planning_resources if isinstance(planning_resources, dict) else {},
        policy=expected_policy,
        requested_memory_target_mb=expected_memory_target_mb,
        execution_plan=(execution_plan if isinstance(execution_plan, dict) else {}),
    )
    lifetime_contract = validate_full_geometry_lifetime_contract(
        mode="dirty",
        allocation_bytes_by_component=allocation_bytes_by_component,
        lifetimes=[
            lifetime
            for lifetime in memory.get("lifetimes", [])
            if isinstance(lifetime, dict)
        ],
        execution_plan=execution_plan if isinstance(execution_plan, dict) else {},
        planning_resources=(
            planning_resources if isinstance(planning_resources, dict) else {}
        ),
        runtime_actions=runtime_actions if isinstance(runtime_actions, dict) else {},
        compact_replay=(
            memory.get("compact_replay")
            if isinstance(memory.get("compact_replay"), dict)
            else {}
        ),
    )
    gates = {
        "real_input_admitted": isinstance(preflight, dict)
        and preflight.get("status") == "admitted"
        and isinstance(preflight.get("rows_total"), int)
        and preflight.get("rows_total", 0) > 0
        and isinstance(preflight.get("ddids"), int)
        and preflight.get("ddids", 0) > 0,
        "stopped_before_execution": isinstance(preflight, dict)
        and preflight.get("visibility_streamed") is False
        and preflight.get("replay_compiled") is False
        and preflight.get("grids_allocated") is False
        and preflight.get("products_materialized") is False,
        "execution_plan_present": isinstance(execution_plan, dict)
        and bool(execution_plan),
        "lifetime_ledger_reconciled": isinstance(ledger, dict)
        and ledger.get("complete") is True,
        "policy_selected": isinstance(planning_resources, dict)
        and planning_resources.get("memory_pressure_policy") == expected_policy
        and isinstance(preflight, dict)
        and preflight.get("memory_pressure_policy") == expected_policy,
        "runtime_actions_truthful": isinstance(runtime_actions, dict)
        and runtime_actions_match_policy(runtime_actions, policy=expected_policy),
        "requested_policy_actions_active": isinstance(runtime_actions, dict)
        and requested_runtime_actions_are_active(
            runtime_actions,
            policy=expected_policy,
        ),
        "resolved_memory_target": target_resolution["matches"],
        "full_geometry_lifetime_contract": lifetime_contract["complete"],
    }
    return {
        "schema_version": 1,
        "kind": "casars_imager_planner_preflight",
        "log": {
            "path": str(stdout_log_path),
            "sha256": sha256_file(stdout_log_path),
        },
        "gates": gates,
        "status": "admitted" if all(gates.values()) else "rejected",
        "backend_plan_logs": memory,
        "resolved_memory_target": target_resolution,
        "full_geometry_lifetime_contract": lifetime_contract,
        "negative_evidence": [
            gate for gate, passed in gates.items() if passed is not True
        ],
    }


def experiment_fingerprint(
    *,
    phase: str,
    workload_kind: str,
    policy: str,
    campaign_label: str,
    base_workload: EvidenceRef,
    promoted_4096: EvidenceRef,
    dirty_policy: EvidenceRef | None,
    manifest: dict[str, Any],
    command: list[str],
    command_environment: dict[str, str],
    storage_bandwidth: dict[str, Any],
    stop_thresholds: StopThresholds,
    execution_intent: str,
) -> str:
    """Bind every input that can make a memory experiment meaningfully new."""

    return canonical_sha256(
        {
            "schema_version": 1,
            "phase": phase,
            "workload_kind": workload_kind,
            "policy": policy,
            "campaign_label": campaign_label,
            "base_workload": asdict(base_workload),
            "promoted_4096": asdict(promoted_4096),
            "dirty_policy": asdict(dirty_policy) if dirty_policy else None,
            "manifest": manifest,
            "command": command,
            "command_environment": command_environment,
            "storage_bandwidth": storage_bandwidth,
            "stop_thresholds": asdict(stop_thresholds),
            "execution_intent": execution_intent,
        }
    )


def find_duplicate_receipt(receipt_dir: Path, fingerprint: str) -> Path | None:
    """Find a prior claim for exactly the same experiment."""

    if not receipt_dir.exists():
        return None
    for path in receipt_dir.rglob("*.json"):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if (
            isinstance(value, dict)
            and value.get("kind") == "vlass_full_geometry_memory_experiment"
            and value.get("experiment_fingerprint") == fingerprint
        ):
            return path
    return None


def claim_receipt(path: Path, value: dict[str, Any]) -> None:
    """Create a receipt claim atomically and refuse an existing path."""

    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        descriptor = os.open(path, flags, 0o644)
    except FileExistsError as error:
        raise CampaignError(f"experiment receipt is already claimed: {path}") from error
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def process_tree_resident_bytes(root_pid: int) -> int:
    """Return RSS for a process and descendants using the Darwin ps interface."""

    completed = subprocess.run(
        ["/bin/ps", "-axo", "pid=,ppid=,rss="],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=10.0,
    )
    rows: dict[int, tuple[int, int]] = {}
    for line in completed.stdout.splitlines():
        parts = line.split()
        if len(parts) != 3:
            continue
        try:
            pid, ppid, rss_kib = (int(part) for part in parts)
        except ValueError:
            continue
        rows[pid] = (ppid, rss_kib)
    selected = {root_pid}
    changed = True
    while changed:
        changed = False
        for pid, (ppid, _) in rows.items():
            if pid not in selected and ppid in selected:
                selected.add(pid)
                changed = True
    return sum(rows.get(pid, (0, 0))[1] for pid in selected) * 1024


def terminate_process_group(
    process: subprocess.Popen[str], *, grace_seconds: float
) -> None:
    """Terminate one campaign subprocess and its descendants."""

    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=grace_seconds)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    process.wait()


def _stdout_reader(
    stream: Any,
    *,
    log_handle: Any,
    events: queue.SimpleQueue[float],
) -> None:
    """Drain subprocess output so a long benchmark cannot block on its pipe."""

    for line in iter(stream.readline, ""):
        log_handle.write(line)
        log_handle.flush()
        print(line, end="", flush=True)
        events.put(time.monotonic())
    stream.close()


def run_bounded_command(
    command: list[str],
    *,
    environment: dict[str, str],
    thresholds: StopThresholds,
    stdout_log_path: Path,
) -> MonitorResult:
    """Run one harness command and stop sustained destructive pressure."""

    if platform.system() != "Darwin":
        raise CampaignError("12,150-pixel execution is supported only on Darwin")
    sys.path.insert(0, str(IMAGER_TOOL_DIR))
    from perf_harness.host_telemetry import (  # noqa: PLC0415
        HostTelemetryError,
        read_darwin_host_snapshot,
    )

    stdout_log_path.parent.mkdir(parents=True, exist_ok=True)
    initial = read_darwin_host_snapshot()
    started = time.monotonic()
    last_output = started
    last_sample_at = started
    last_sample = initial
    process_rss_peak = 0
    memory_free_min = float(initial["memory_free_percent"])
    swap_used_peak = int(initial["swap_used_bytes"])
    swap_rate_max = 0.0
    pressure_count = 0
    sample_count = 1
    stop_reason: str | None = None
    output_events: queue.SimpleQueue[float] = queue.SimpleQueue()
    with stdout_log_path.open("w", encoding="utf-8") as log_handle:
        process = subprocess.Popen(
            command,
            env=environment,
            cwd=REPO_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            start_new_session=True,
        )
        assert process.stdout is not None
        reader = threading.Thread(
            target=_stdout_reader,
            kwargs={
                "stream": process.stdout,
                "log_handle": log_handle,
                "events": output_events,
            },
            daemon=True,
        )
        reader.start()
        while process.poll() is None:
            now = time.monotonic()
            while True:
                try:
                    last_output = output_events.get_nowait()
                except queue.Empty:
                    break
            elapsed = now - started
            if elapsed > thresholds.max_wall_seconds:
                stop_reason = (
                    f"wall time {elapsed:.1f}s exceeded "
                    f"{thresholds.max_wall_seconds:.1f}s"
                )
            elif now - last_output > thresholds.max_no_output_seconds:
                stop_reason = (
                    f"no harness output for {now - last_output:.1f}s exceeded "
                    f"{thresholds.max_no_output_seconds:.1f}s"
                )
            if stop_reason is not None:
                terminate_process_group(
                    process,
                    grace_seconds=thresholds.terminate_grace_seconds,
                )
                break
            sleep_for = max(
                0.05,
                thresholds.sample_interval_seconds - (now - last_sample_at),
            )
            time.sleep(min(sleep_for, 1.0))
            now = time.monotonic()
            if now - last_sample_at < thresholds.sample_interval_seconds:
                continue
            try:
                sample = read_darwin_host_snapshot()
                rss = process_tree_resident_bytes(process.pid)
            except (
                HostTelemetryError,
                OSError,
                subprocess.SubprocessError,
            ) as error:
                stop_reason = f"required memory-pressure sampling failed: {error}"
                terminate_process_group(
                    process,
                    grace_seconds=thresholds.terminate_grace_seconds,
                )
                break
            sample_count += 1
            process_rss_peak = max(process_rss_peak, rss)
            free_percent = float(sample["memory_free_percent"])
            memory_free_min = min(memory_free_min, free_percent)
            swap_used_peak = max(swap_used_peak, int(sample["swap_used_bytes"]))
            interval = max(now - last_sample_at, 1e-9)
            page_size = int(sample["page_size_bytes"])
            swap_pages = max(
                0,
                int(sample["swapins"]) - int(last_sample["swapins"]),
            ) + max(
                0,
                int(sample["swapouts"]) - int(last_sample["swapouts"]),
            )
            swap_rate = swap_pages * page_size / interval
            swap_rate_max = max(swap_rate_max, swap_rate)
            swapout_delta = (
                max(0, int(sample["swapouts"]) - int(initial["swapouts"])) * page_size
            )
            pressure = (
                free_percent < thresholds.min_memory_free_percent
                or swap_rate > thresholds.max_swap_io_bytes_per_second
                or swapout_delta > thresholds.max_swapout_delta_bytes
            )
            pressure_count = pressure_count + 1 if pressure else 0
            if pressure_count >= thresholds.pressure_samples:
                stop_reason = (
                    "sustained destructive memory pressure: "
                    f"free={free_percent:.1f}%, "
                    f"swap_io={swap_rate:.0f} B/s, "
                    f"swapout_delta={swapout_delta} B"
                )
                terminate_process_group(
                    process,
                    grace_seconds=thresholds.terminate_grace_seconds,
                )
                break
            last_sample = sample
            last_sample_at = now
        exit_code = process.wait()
        reader.join(timeout=5.0)
    final = last_sample
    page_size = int(final["page_size_bytes"])
    return MonitorResult(
        exit_code=exit_code,
        elapsed_seconds=time.monotonic() - started,
        stop_reason=stop_reason,
        sample_count=sample_count,
        process_tree_resident_bytes_peak=process_rss_peak,
        memory_free_percent_min=memory_free_min,
        swap_used_bytes_peak=swap_used_peak,
        swapin_bytes_delta=(
            max(0, int(final["swapins"]) - int(initial["swapins"])) * page_size
        ),
        swapout_bytes_delta=(
            max(0, int(final["swapouts"]) - int(initial["swapouts"])) * page_size
        ),
        swap_io_bytes_per_second_max=swap_rate_max,
        stdout_log_path=str(stdout_log_path),
        stdout_log_sha256=sha256_file(stdout_log_path),
    )


def locate_workload_result(output_dir: Path, stdout_log_path: Path) -> Path:
    """Locate the single new workload result mentioned by the harness."""

    candidates: list[Path] = []
    for line in stdout_log_path.read_text(encoding="utf-8").splitlines():
        path = Path(line.strip())
        if path.suffix == ".json" and path.is_file():
            candidates.append(path.resolve())
    for path in output_dir.glob("*.json"):
        if path.resolve() not in candidates:
            candidates.append(path.resolve())
    workload_results: list[Path] = []
    for path in candidates:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(value, dict) and value.get("kind") == "workload_run":
            workload_results.append(path)
    if len(workload_results) != 1:
        raise CampaignError(
            "expected exactly one workload_run result, found "
            f"{len(workload_results)} in {output_dir}"
        )
    return workload_results[0]


def validate_rust_only_workload_result(path: Path, *, expected_status: str) -> None:
    """Verify the shared harness honored the no-CASA boundary."""

    result = load_json(path, label="workload result")
    if result.get("status") != expected_status:
        raise CampaignError(
            f"{path}: workload status {result.get('status')!r}, "
            f"expected {expected_status!r}"
        )
    command = result.get("command")
    env = command.get("env") if isinstance(command, dict) else None
    if not isinstance(env, dict) or env.get("IMAGER_BENCH_SKIP_CASA") != "1":
        raise CampaignError(f"{path}: result does not prove CASA was disabled")
    if command.get("kind") in {"casa_tclean_protocol", "recipe_bound_benchmark"}:
        raise CampaignError(f"{path}: CASA-capable command kind is forbidden")


def receipt_template(
    *,
    mode: str,
    workload_kind: str,
    policy: str,
    campaign_label: str,
    execution_intent: str,
    fingerprint: str,
    promoted_4096: EvidenceRef,
    dirty_policy: EvidenceRef | None,
    base_workload: EvidenceRef,
    manifest_path: Path,
    command: list[str],
    command_environment: dict[str, str],
    storage_bandwidth: dict[str, Any],
    output_dir: Path,
    artifact_root: Path,
    thresholds: StopThresholds,
    requested_memory_target_mb: int | None,
) -> dict[str, Any]:
    """Build the durable claim before any subprocess can start."""

    return {
        "schema_version": 1,
        "kind": "vlass_full_geometry_memory_experiment",
        "status": "claimed",
        "created_at": utc_now(),
        "mode": mode,
        "workload_kind": workload_kind,
        "policy": policy,
        "requested_memory_target_mb": requested_memory_target_mb,
        "campaign_label": campaign_label,
        "execution_intent": execution_intent,
        "experiment_fingerprint": fingerprint,
        "promotion_4096_receipt": promoted_4096.path,
        "promotion_4096_sha256": promoted_4096.sha256,
        "dirty_policy_receipt": dirty_policy.path if dirty_policy else None,
        "dirty_policy_receipt_sha256": dirty_policy.sha256 if dirty_policy else None,
        "base_workload": asdict(base_workload),
        "derived_manifest_path": str(manifest_path),
        "derived_manifest_sha256": sha256_file(manifest_path),
        "command": command,
        "command_environment": command_environment,
        "storage_bandwidth": storage_bandwidth,
        "targets": {
            "imsize": 12150,
            "spw": "2~17",
            "dataset_selection": WORKLOAD_GEOMETRY[workload_kind]["dataset_selection"],
            "field": WORKLOAD_GEOMETRY[workload_kind]["field"],
            "field_count": WORKLOAD_GEOMETRY[workload_kind]["field_count"],
            "nterms": 2,
            "wprojplanes": 32,
            "memory_target_mb": requested_memory_target_mb,
            "memory_pressure_policy": policy,
            "output_dir": str(output_dir),
            "artifact_root": str(artifact_root),
        },
        "stop_thresholds": asdict(thresholds),
        "never_invoke_casa_tclean": True,
        "casa_use": (
            "frozen-image-comparator-only"
            if execution_intent == "execute-12150"
            else "none"
        ),
        "run_workload_result": None,
        "outer_monitor": None,
        "required_dirty_policy_promotion_receipt_template": (
            {
                "schema_version": 1,
                "kind": "vlass_full_geometry_dirty_policy_promotion",
                "status": "passed",
                "workload_kind": workload_kind,
                "policy": policy,
                "promotion_4096_sha256": promoted_4096.sha256,
                "experiment_receipt": "<this receipt path>",
                "experiment_receipt_sha256": "<sha256 after completion>",
                "review": {
                    "decision": "passed",
                    "note": (
                        "Promotion is accepted only when this experiment receipt's "
                        "derived memory_evidence.gates all pass."
                    ),
                },
            }
            if mode == "dirty" and execution_intent == "execute-12150"
            else None
        ),
        "required_clean_memory_campaign_promotion_receipt_template": (
            {
                "schema_version": 1,
                "kind": "vlass_full_geometry_clean_promotion",
                "status": MEMORY_CAMPAIGN_PROMOTION_STATUS,
                "promotion_scope": MEMORY_CAMPAIGN_PROMOTION_SCOPE,
                "final_wave_acceptance": pending_final_wave_acceptance_contract(),
                "workload_kind": workload_kind,
                "policy": policy,
                "promotion_4096_sha256": promoted_4096.sha256,
                "dirty_policy_receipt_sha256": (
                    dirty_policy.sha256 if dirty_policy else None
                ),
                "experiment_receipt": "<this receipt path>",
                "experiment_receipt_sha256": "<sha256 after completion>",
                "workload_result_sha256": "<sha256 of exact workload result>",
                "product_comparison_sha256": (
                    "<canonical sha256 of embedded product comparison>"
                ),
                "trajectory_receipt": "<full-size clean trajectory receipt path>",
                "trajectory_receipt_sha256": "<sha256>",
                "gates": {gate: True for gate in CLEAN_PROMOTION_GATES},
            }
            if mode == "clean" and execution_intent == "execute-12150"
            else None
        ),
    }


def parser() -> argparse.ArgumentParser:
    """Create the command-line contract."""

    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--mode",
        choices=("planner-only", "dirty", "clean", "validate-clean-promotion"),
        default="planner-only",
        help=(
            "planner-only opens the real MS/CF inputs and executes allocation-free "
            "Rust planner probes for all five policies; dirty validates or executes "
            "all five; clean validates or executes the selected reviewed dirty "
            "policy; validate-clean-promotion verifies a memory-campaign-only "
            "full-size clean candidate without launching a workload or satisfying "
            "the separate final four-row 10x acceptance contract"
        ),
    )
    result.add_argument(
        "--workload-kind",
        choices=WORKLOAD_KINDS,
        default="single-field",
        help="select the single field or exact connected 63-field VLASS workload",
    )
    result.add_argument("--promoted-4096-receipt", type=Path, required=True)
    result.add_argument("--dirty-policy-receipt", type=Path)
    result.add_argument("--clean-promotion-receipt", type=Path)
    result.add_argument("--receipt-dir", type=Path, required=True)
    result.add_argument(
        "--campaign-label",
        default="vlass-full-geometry-memory-v1",
        help="stable label; supply a new label only for a meaningfully new experiment",
    )
    result.add_argument("--dirty-workload", type=Path)
    result.add_argument("--clean-workload", type=Path)
    result.add_argument("--output-dir", type=Path)
    result.add_argument("--artifact-root", type=Path)
    result.add_argument("--memory-target-mb", type=int)
    result.add_argument(
        "--execute-12150",
        action="store_true",
        help="explicitly authorize the otherwise dry-run 12,150-pixel subprocess",
    )
    result.add_argument("--max-wall-seconds", type=float, default=7200.0)
    result.add_argument(
        "--max-swapout-delta-bytes",
        type=int,
        default=8 * 1024 * 1024 * 1024,
    )
    result.add_argument(
        "--max-swap-io-bytes-per-second",
        type=float,
        default=256 * 1024 * 1024,
    )
    result.add_argument("--min-memory-free-percent", type=float, default=1.0)
    result.add_argument("--max-no-output-seconds", type=float, default=900.0)
    result.add_argument("--pressure-samples", type=int, default=3)
    result.add_argument("--sample-interval-seconds", type=float, default=5.0)
    result.add_argument("--terminate-grace-seconds", type=float, default=10.0)
    return result


def run_campaign(args: argparse.Namespace) -> list[Path]:
    """Validate gates, claim each row, and optionally invoke the shared harness."""

    if len(POLICIES) != 5 or len(set(POLICIES)) != 5:
        raise CampaignError("internal error: approved policies must occur exactly once")
    if not LABEL_PATTERN.fullmatch(args.campaign_label):
        raise CampaignError(
            "--campaign-label must contain only letters, digits, dot, underscore, "
            "and hyphen"
        )
    if args.mode == "planner-only" and args.execute_12150:
        raise CampaignError("planner-only forbids --execute-12150")
    clean_modes = {"clean", "validate-clean-promotion"}
    if args.mode not in clean_modes and args.dirty_policy_receipt is not None:
        raise CampaignError(
            "--dirty-policy-receipt is valid only for clean or "
            "validate-clean-promotion mode"
        )
    if args.mode in clean_modes and args.dirty_policy_receipt is None:
        raise CampaignError(
            f"{args.mode} mode requires a passed --dirty-policy-receipt"
        )
    if (
        args.mode != "validate-clean-promotion"
        and args.clean_promotion_receipt is not None
    ):
        raise CampaignError(
            "--clean-promotion-receipt is valid only for validate-clean-promotion mode"
        )
    if args.mode == "validate-clean-promotion" and args.clean_promotion_receipt is None:
        raise CampaignError(
            "validate-clean-promotion requires --clean-promotion-receipt"
        )
    if args.mode == "validate-clean-promotion" and args.execute_12150:
        raise CampaignError("validate-clean-promotion forbids --execute-12150")
    if args.execute_12150 and args.artifact_root is None:
        raise CampaignError(
            "--execute-12150 requires an explicit --artifact-root on the intended disk"
        )
    if args.execute_12150:
        casa_python = os.environ.get("CASA_RS_CASA_PYTHON")
        if not casa_python or not Path(casa_python).expanduser().is_file():
            raise CampaignError(
                "--execute-12150 requires CASA_RS_CASA_PYTHON for the frozen "
                "CASA-image comparator; CASA tclean remains unconditionally disabled"
            )

    thresholds = StopThresholds(
        max_wall_seconds=args.max_wall_seconds,
        max_swapout_delta_bytes=args.max_swapout_delta_bytes,
        max_swap_io_bytes_per_second=args.max_swap_io_bytes_per_second,
        min_memory_free_percent=args.min_memory_free_percent,
        max_no_output_seconds=args.max_no_output_seconds,
        pressure_samples=args.pressure_samples,
        sample_interval_seconds=args.sample_interval_seconds,
        terminate_grace_seconds=args.terminate_grace_seconds,
    )
    thresholds.validate()
    if args.memory_target_mb is not None and args.memory_target_mb < 1:
        raise CampaignError("--memory-target-mb must be positive")
    promoted_4096 = validate_promoted_4096_receipt(
        args.promoted_4096_receipt,
        workload_kind=args.workload_kind,
    )
    dirty_policy: EvidenceRef | None = None
    if args.mode in clean_modes:
        policy, dirty_policy = validate_dirty_policy_promotion(
            args.dirty_policy_receipt,
            promoted_4096=promoted_4096,
            workload_kind=args.workload_kind,
        )
        if args.mode == "validate-clean-promotion":
            clean_promotion = validate_clean_promotion_receipt(
                args.clean_promotion_receipt,
                promoted_4096=promoted_4096,
                dirty_policy=dirty_policy,
                expected_policy=policy,
                workload_kind=args.workload_kind,
            )
            return [Path(clean_promotion.path)]
        policies = (policy,)
        science_mode = "clean"
    else:
        policies = POLICIES
        science_mode = "dirty"

    override_path = (
        args.clean_workload if science_mode == "clean" else args.dirty_workload
    )
    selected_path = override_path or DEFAULT_WORKLOADS.get(
        (args.workload_kind, science_mode)
    )
    if selected_path is None:
        raise CampaignError(
            f"no frozen default {science_mode} workload exists for "
            f"{args.workload_kind}; supply --{science_mode}-workload with an exact "
            "frozen CASA reuse prefix"
        )
    base_path = selected_path.expanduser().resolve()
    base = load_json(base_path, label=f"{science_mode} base workload")
    validate_common_science_contract(
        base,
        mode=science_mode,
        workload_kind=args.workload_kind,
    )
    base_ref = EvidenceRef(path=str(base_path), sha256=sha256_file(base_path))

    receipt_dir = args.receipt_dir.expanduser().resolve()
    output_dir = (
        args.output_dir.expanduser().resolve()
        if args.output_dir is not None
        else receipt_dir / "workload-runs"
    )
    artifact_root = (
        args.artifact_root.expanduser().resolve()
        if args.artifact_root is not None
        else receipt_dir / "planned-artifacts"
    )
    pure_planner = args.mode == "planner-only"
    harness_dry_run = not args.execute_12150
    execution_intent = (
        "planner-only"
        if pure_planner
        else ("dry-run" if harness_dry_run else "execute-12150")
    )
    if args.execute_12150:
        storage_bandwidth = storage_bandwidth_evidence(
            receipt_dir / "evidence" / f"{args.campaign_label}-storage-bandwidth.json",
            volume_path=artifact_root,
        )
    else:
        storage_bandwidth = unavailable_storage_bandwidth_evidence(
            reason=(
                "planner-only-does-not-measure-storage"
                if pure_planner
                else "execute-12150-not-authorized"
            )
        )

    rows: list[
        tuple[
            Path,
            Path,
            dict[str, Any],
            list[str],
            dict[str, str],
            str,
            str,
            Path,
            Path,
        ]
    ] = []
    for policy in policies:
        manifest = derive_rust_only_manifest(
            base,
            mode=science_mode,
            policy=policy,
            campaign_label=args.campaign_label,
            memory_target_mb=args.memory_target_mb,
            workload_kind=args.workload_kind,
        )
        apply_storage_bandwidth_environment(
            manifest,
            evidence=storage_bandwidth,
        )
        preliminary_manifest_sha = canonical_sha256(manifest)
        manifest_path = (
            receipt_dir
            / "manifests"
            / (
                f"{args.campaign_label}-{science_mode}-{policy}-"
                f"{preliminary_manifest_sha[:12]}.json"
            )
        )
        evidence_key = canonical_sha256(
            {
                "promoted_4096": asdict(promoted_4096),
                "dirty_policy": asdict(dirty_policy) if dirty_policy else None,
            }
        )[:12]
        row_output_dir = (
            output_dir / args.campaign_label / args.mode / policy / evidence_key
        )
        row_artifact_root = (
            artifact_root / args.campaign_label / args.mode / policy / evidence_key
        )
        run_label = f"{args.campaign_label}-{science_mode}-{policy}"
        if pure_planner:
            command, command_environment = build_planner_preflight_invocation(
                manifest_path=manifest_path,
                manifest=manifest,
                run_label=run_label,
            )
        else:
            command = build_workload_command(
                manifest_path=manifest_path,
                output_dir=row_output_dir,
                artifact_root=row_artifact_root,
                run_label=run_label,
                dry_run=harness_dry_run,
            )
            command_environment = dict(storage_bandwidth.get("command_environment", {}))
        fingerprint = experiment_fingerprint(
            phase=args.mode,
            workload_kind=args.workload_kind,
            policy=policy,
            campaign_label=args.campaign_label,
            base_workload=base_ref,
            promoted_4096=promoted_4096,
            dirty_policy=dirty_policy,
            manifest=manifest,
            command=command,
            command_environment=command_environment,
            storage_bandwidth=storage_bandwidth,
            stop_thresholds=thresholds,
            execution_intent=execution_intent,
        )
        duplicate = find_duplicate_receipt(receipt_dir, fingerprint)
        if duplicate is not None:
            raise CampaignError(
                "refusing unchanged repeated experiment "
                f"{policy}: prior receipt {duplicate}; supply a new campaign label "
                "or new prerequisite receipt only for a meaningfully new experiment"
            )
        receipt_path = (
            receipt_dir
            / "experiments"
            / (f"{args.campaign_label}-{args.mode}-{policy}-{fingerprint[:12]}.json")
        )
        rows.append(
            (
                manifest_path,
                receipt_path,
                manifest,
                command,
                command_environment,
                fingerprint,
                policy,
                row_output_dir,
                row_artifact_root,
            )
        )

    receipt_paths: list[Path] = []
    for (
        manifest_path,
        receipt_path,
        manifest,
        command,
        command_environment,
        fingerprint,
        policy,
        row_output_dir,
        row_artifact_root,
    ) in rows:
        atomic_write_json(manifest_path, manifest)
        receipt = receipt_template(
            mode=science_mode,
            workload_kind=args.workload_kind,
            policy=policy,
            campaign_label=args.campaign_label,
            execution_intent=execution_intent,
            fingerprint=fingerprint,
            promoted_4096=promoted_4096,
            dirty_policy=dirty_policy,
            base_workload=base_ref,
            manifest_path=manifest_path,
            command=command,
            command_environment=command_environment,
            storage_bandwidth=storage_bandwidth,
            output_dir=row_output_dir,
            artifact_root=row_artifact_root,
            thresholds=thresholds,
            requested_memory_target_mb=args.memory_target_mb,
        )
        claim_receipt(receipt_path, receipt)
        receipt_paths.append(receipt_path)
        if pure_planner:
            stdout_log = receipt_dir / "logs" / f"{fingerprint}.planner.log"
            receipt["status"] = "running_planner_preflight"
            receipt["started_at"] = utc_now()
            atomic_write_json(receipt_path, receipt)
            try:
                completed = run_planner_preflight_command(
                    command,
                    planned_environment=command_environment,
                    stdout_log_path=stdout_log,
                    timeout_seconds=thresholds.max_wall_seconds,
                )
                receipt["planner_process"] = {
                    "exit_code": completed.returncode,
                    "stdout_log_path": str(stdout_log),
                    "stdout_log_sha256": sha256_file(stdout_log),
                }
                if completed.returncode != 0:
                    raise CampaignError(
                        f"planner preflight exited with status {completed.returncode}"
                    )
                evidence = planner_preflight_evidence(
                    stdout_log,
                    expected_policy=policy,
                    expected_memory_target_mb=args.memory_target_mb,
                )
                receipt["memory_evidence"] = evidence
                if evidence.get("status") != "admitted":
                    failed = evidence.get("negative_evidence")
                    raise CampaignError(
                        "planner evidence did not pass"
                        + (f": {', '.join(failed)}" if isinstance(failed, list) else "")
                    )
                receipt["status"] = "planner_admitted"
            # Preserve one failed policy as evidence and continue the bounded sweep.
            except Exception as error:
                receipt["status"] = "failed"
                receipt["failure"] = f"{type(error).__name__}: {error}"
                receipt["negative_evidence"] = [
                    {
                        "stage": "planner_preflight",
                        "reason": receipt["failure"],
                    }
                ]
            receipt["completed_at"] = utc_now()
            atomic_write_json(receipt_path, receipt)
            continue

        run_output_dir = row_output_dir
        stdout_log = receipt_dir / "logs" / f"{fingerprint}.harness.log"
        environment = dict(os.environ)
        environment["CASA_RS_BENCH_SKIP_CASA"] = "1"
        environment.update(command_environment)
        receipt["status"] = "running" if args.execute_12150 else "validating_dry_run"
        receipt["started_at"] = utc_now()
        atomic_write_json(receipt_path, receipt)
        try:
            monitor = run_bounded_command(
                command,
                environment=environment,
                thresholds=thresholds,
                stdout_log_path=stdout_log,
            )
            receipt["outer_monitor"] = asdict(monitor)
            workload_result = locate_workload_result(run_output_dir, stdout_log)
            expected_status = "completed" if args.execute_12150 else "dry_run"
            validate_rust_only_workload_result(
                workload_result,
                expected_status=expected_status,
            )
            receipt["run_workload_result"] = {
                "path": str(workload_result),
                "sha256": sha256_file(workload_result),
                "status": expected_status,
            }
            if args.execute_12150:
                workload_value = load_json(
                    workload_result,
                    label="workload result",
                )
                receipt["memory_evidence"] = extract_execution_memory_evidence(
                    workload_value,
                    mode=science_mode,
                    expected_policy=policy,
                    expected_memory_target_mb=args.memory_target_mb,
                    outer_monitor=monitor,
                )
            if monitor.stop_reason is not None:
                receipt["status"] = "stopped"
                receipt["failure"] = monitor.stop_reason
            elif monitor.exit_code != 0:
                receipt["status"] = "failed"
                receipt["failure"] = (
                    f"run_workload exited with status {monitor.exit_code}"
                )
            else:
                receipt["status"] = (
                    "completed" if args.execute_12150 else "dry_run_validated"
                )
        # Preserve one failed policy as evidence and continue the bounded sweep.
        except Exception as error:
            receipt["status"] = "failed"
            receipt["failure"] = f"{type(error).__name__}: {error}"
            receipt["negative_evidence"] = [
                {
                    "stage": "workload_execution",
                    "reason": receipt["failure"],
                }
            ]
        receipt["completed_at"] = utc_now()
        atomic_write_json(receipt_path, receipt)
    return receipt_paths


def main() -> int:
    """CLI entrypoint."""

    try:
        receipt_paths = run_campaign(parser().parse_args())
    except CampaignError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    for path in receipt_paths:
        print(path)
    statuses = [
        load_json(path, label="campaign experiment receipt").get("status")
        for path in receipt_paths
    ]
    successful = {
        "planner_admitted",
        "completed",
        "dry_run_validated",
        MEMORY_CAMPAIGN_PROMOTION_STATUS,
    }
    return 0 if all(status in successful for status in statuses) else 2


if __name__ == "__main__":
    raise SystemExit(main())
