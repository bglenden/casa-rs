#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for the VLASS full-geometry memory campaign driver."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


MODULE_PATH = Path(__file__).with_name("vlass_full_geometry_memory_campaign.py")
SPEC = importlib.util.spec_from_file_location("vlass_memory_campaign", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
campaign = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = campaign
SPEC.loader.exec_module(campaign)
from perf_harness.host_telemetry import (  # noqa: E402
    SAMPLE_FIELDS,
    build_host_telemetry_result,
)


class VlassFullGeometryMemoryCampaignTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.workload_result_4096 = self.root / "workload-4096.json"
        self.comparison = self.root / "comparison.json"
        self.trajectory = self.root / "trajectory.json"
        self.promotion = self.root / "promotion.json"
        self.artifact_root = self.root / "external-artifacts"
        self.artifact_root.mkdir()
        self.artifact_root = self.artifact_root.resolve()
        self.storage_receipt = self.root / "storage-bandwidth.json"
        storage_probe = {
            "bytes": campaign.STORAGE_BANDWIDTH_PROBE_BYTES,
            "block_bytes": campaign.STORAGE_BANDWIDTH_PROBE_BLOCK_BYTES,
            "block_sha256": "a" * 64,
            "payload_sha256": "b" * 64,
            "readback_sha256": "b" * 64,
            "write_seconds": 1.0,
            "read_seconds": 2.0,
            "write_fsync_included": True,
            "darwin_f_nocache_applied": True,
            "temporary_file_removed": True,
        }
        storage_receipt = {
            "schema_version": 1,
            "kind": "vlass_storage_bandwidth_probe",
            "status": "measured",
            "measured_at": "2026-07-30T00:00:00Z",
            "volume_path": str(self.artifact_root),
            "volume_device_id": self.artifact_root.stat().st_dev,
            "platform": "Darwin",
            "probe": storage_probe,
            "write_bytes_per_second": campaign.STORAGE_BANDWIDTH_PROBE_BYTES,
            "read_bytes_per_second": (campaign.STORAGE_BANDWIDTH_PROBE_BYTES // 2),
        }
        self.storage_receipt.write_text(
            json.dumps(storage_receipt),
            encoding="utf-8",
        )
        self.storage = {
            "schema_version": 1,
            "status": "measured",
            "receipt": {
                "path": str(self.storage_receipt),
                "sha256": campaign.sha256_file(self.storage_receipt),
            },
            "volume_path": str(self.artifact_root),
            "volume_device_id": self.artifact_root.stat().st_dev,
            "probe": storage_probe,
            "read_bytes_per_second": storage_receipt["read_bytes_per_second"],
            "write_bytes_per_second": storage_receipt["write_bytes_per_second"],
            "command_environment": {
                campaign.SPILL_READ_BANDWIDTH_ENV: str(
                    storage_receipt["read_bytes_per_second"]
                ),
                campaign.SPILL_WRITE_BANDWIDTH_ENV: str(
                    storage_receipt["write_bytes_per_second"]
                ),
            },
        }
        comparison = {
            "status": "completed",
            "requested_products": list(campaign.EXPECTED_19_PRODUCTS),
            "product_inventory": {"status": "matched"},
            "products": {
                product: {"status": "compared"}
                for product in campaign.EXPECTED_19_PRODUCTS
            },
            "require_exact_product_inventory": True,
            "require_metadata_parity": True,
            "tolerances": {"default": {"require_topology_parity": True}},
            "tolerance_evaluation": {"status": "passed"},
            "structured_difference_review": {"label": "good"},
        }
        self.comparison.write_text(
            json.dumps(comparison),
            encoding="utf-8",
        )
        workload_result = {
            "kind": "workload_run",
            "status": "completed",
            "mode": {
                "bench_mode": "clean",
                "image_shape": [4096, 4096],
                "channel_count": 64,
                "nterms": 2,
                "niter": 2000,
                "gridder": "awproject",
                "deconvolver": "mtmfs",
                "wprojplanes": "32",
            },
            "command": {
                "kind": "legacy_benchmark_script",
                "env": {
                    "IMAGER_BENCH_IMSIZE": "4096",
                    "IMAGER_BENCH_FIELD": "1525",
                    "IMAGER_BENCH_SPW": "2~17",
                    "IMAGER_BENCH_MODE": "clean",
                    "IMAGER_BENCH_CHANNEL_COUNT": "64",
                    "IMAGER_BENCH_NTERMS": "2",
                    "IMAGER_BENCH_NITER": "2000",
                    "IMAGER_BENCH_WPROJPLANES": "32",
                    "IMAGER_BENCH_GRIDDER": "awproject",
                    "IMAGER_BENCH_DECONVOLVER": "mtmfs",
                    "IMAGER_BENCH_ATERM": "1",
                    "IMAGER_BENCH_WBAWP": "1",
                    "IMAGER_BENCH_CONJBEAMS": "1",
                    "IMAGER_BENCH_USEPOINTING": "1",
                },
            },
            "comparison": {
                "products": list(campaign.EXPECTED_19_PRODUCTS),
                "require_exact_product_inventory": True,
                "require_metadata_parity": True,
            },
            "results": {"product_comparison": comparison},
        }
        self.workload_result_4096.write_text(
            json.dumps(workload_result),
            encoding="utf-8",
        )
        workload_result_sha256 = campaign.sha256_file(self.workload_result_4096)
        comparison_sha256 = campaign.sha256_file(self.comparison)
        self.trajectory.write_text(
            json.dumps(
                {
                    "status": "completed",
                    "geometry": campaign._expected_bound_geometry(
                        "single-field",
                        imsize=4096,
                    ),
                    "coverage": {
                        "casa_complete": True,
                        "rust_complete": True,
                        "same_cycle_count": True,
                    },
                    "discrete_parity": {"status": "passed"},
                    "aligned_cycles": 5,
                    "casa_cycles": 5,
                    "rust_cycles": 5,
                    "no_divergence": True,
                    "component_selection": {"status": "passed"},
                    "major_cycle_residual": {"status": "passed"},
                    "workload_result_sha256": workload_result_sha256,
                    "comparison_receipt_sha256": comparison_sha256,
                }
            ),
            encoding="utf-8",
        )
        self.promotion.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "kind": "vlass_4096_full16_promotion",
                    "status": "promoted",
                    "workload_kind": "single-field",
                    "geometry": {
                        "imsize": 4096,
                        "spw": "2~17",
                        "field": "1525",
                        "dataset_selection": "single_field",
                        "field_count": 1,
                        "nterms": 2,
                        "wprojplanes": 32,
                        "product_count": 19,
                    },
                    "gates": {gate: True for gate in campaign.PROMOTION_GATES},
                    "workload_result": str(self.workload_result_4096),
                    "workload_result_sha256": workload_result_sha256,
                    "comparison_receipt": str(self.comparison),
                    "comparison_receipt_sha256": comparison_sha256,
                    "trajectory_receipt": str(self.trajectory),
                    "trajectory_receipt_sha256": campaign.sha256_file(self.trajectory),
                }
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def resolved_target(policy: str, memory_target_mb: int | None) -> tuple[int, str]:
        physical = campaign.ACCEPTANCE_PHYSICAL_MEMORY_BYTES
        headroom = 30 * campaign.GIB
        baseline = campaign.GIB
        process_total_ceiling = min(physical, baseline + headroom)
        operation_budget = max(0, process_total_ceiling - baseline)
        requested = (
            memory_target_mb * campaign.MIB if memory_target_mb is not None else None
        )
        if policy == "oversubscribe":
            return (
                requested or 0,
                (
                    "cli-intentional-oversubscription"
                    if requested is not None
                    else "oversubscribe-requires-explicit-target"
                ),
            )
        if policy in {"conservative-no-swap", "stage-aware"}:
            return (
                (
                    operation_budget
                    if requested is None
                    else min(requested, operation_budget)
                ),
                (
                    "available-memory-ledger"
                    if requested is None
                    else (
                        "cli-imaging"
                        if requested <= operation_budget
                        else "cli-capped-to-no-swap-headroom"
                    )
                ),
            )
        return (
            (
                operation_budget
                if requested is None
                else min(requested, operation_budget)
            ),
            (
                f"{policy}-physical-ledger"
                if requested is None
                else f"cli-{policy}-physical-ceiling"
            ),
        )

    @classmethod
    def planner_log(
        cls,
        policy: str,
        *,
        mode: str = "dirty",
        memory_target_mb: int | None = None,
    ) -> str:
        memory_target_bytes, memory_target_origin = cls.resolved_target(
            policy,
            memory_target_mb,
        )
        physical_memory_bytes = campaign.ACCEPTANCE_PHYSICAL_MEMORY_BYTES
        no_swap_headroom_bytes = 30 * campaign.GIB
        process_baseline_bytes = campaign.GIB
        process_total_ceiling_bytes = min(
            physical_memory_bytes,
            process_baseline_bytes + no_swap_headroom_bytes,
        )
        incremental_operation_budget_bytes = max(
            0,
            process_total_ceiling_bytes - process_baseline_bytes,
        )
        target_projected_process_total_bytes = (
            process_baseline_bytes + memory_target_bytes
        )
        target_projected_process_excess_bytes = max(
            0,
            (target_projected_process_total_bytes - process_total_ceiling_bytes),
        )
        components = {
            "grids": campaign.FULL_GEOMETRY_EXACT_COMPONENT_BYTES["grids"],
            "source row blocks": 64 * campaign.MIB,
            "FFT chunks": min(campaign.FULL_GEOMETRY_FFT_BYTES_ALLOWED),
            "AWProject MT-MFS run state": (
                campaign.FULL_GEOMETRY_EXACT_COMPONENT_BYTES[
                    "AWProject MT-MFS run state"
                ]
            ),
            "mosaic weighting density maps": (
                campaign.FULL_GEOMETRY_EXACT_COMPONENT_BYTES[
                    "mosaic weighting density maps"
                ]
            ),
            "AWProject CF pixels": 256 * campaign.MIB,
            "AWProject source-order tap scratch": 512 * campaign.MIB,
            "AWProject CF index": 16 * campaign.MIB,
            "POINTING index": 8 * campaign.MIB,
            "AWProject safety margin": memory_target_bytes // 20,
            "AWProject MT-MFS finish state": (
                campaign.FULL_GEOMETRY_EXACT_COMPONENT_BYTES[
                    "AWProject MT-MFS finish state"
                ]
            ),
            "AWProject MT-MFS product state": (
                campaign.FULL_GEOMETRY_EXACT_COMPONENT_BYTES[
                    "AWProject MT-MFS product state"
                ]
            ),
            "product writer scratch": (
                campaign.FULL_GEOMETRY_EXACT_COMPONENT_BYTES["product writer scratch"]
            ),
        }
        if mode == "clean":
            components.update(
                {
                    "AWProject CASA-layout model FFT staging": (
                        campaign.FULL_GEOMETRY_EXACT_COMPONENT_BYTES[
                            "AWProject CASA-layout model FFT staging"
                        ]
                    ),
                    "AWProject MT-MFS bounded multiscale scratch": (
                        campaign.FULL_GEOMETRY_EXACT_COMPONENT_BYTES[
                            "AWProject MT-MFS bounded multiscale scratch"
                        ]
                    ),
                    "AWProject compact replay retention": (
                        campaign.REPLAY_WORKING_SET_REFERENCE_BYTES
                    ),
                }
            )
        allocation_ids: dict[str, str] = {}
        allocation_rows = []
        lifetime_specs: list[dict[str, object]] = []
        for index, (component, bytes_) in enumerate(components.items()):
            allocation_id = (
                "standard-mfs-awproject-compact-replay-retention-1"
                if component == "AWProject compact replay retention"
                else f"allocation-{index}"
            )
            allocation_ids[component] = allocation_id
            allocation_rows.append(
                "standard_mfs_execution_allocation "
                f"allocation_id={allocation_id} component={component} "
                f"stage=run bytes={bytes_}"
            )

        def add_lifetime(
            component: str,
            resident_bytes: int,
            live_from: str,
            live_through: str,
            *,
            index: int = 0,
            next_use: str = "none",
        ) -> None:
            lifetime_specs.append(
                {
                    "allocation_id": allocation_ids[component],
                    "component": component,
                    "logical_bytes": components[component],
                    "residency_index": index,
                    "backing": "HostHeap",
                    "resident_bytes": resident_bytes,
                    "stored_bytes": 0,
                    "live_from": live_from,
                    "live_through": live_through,
                    "next_use": next_use,
                }
            )

        add_lifetime(
            "grids",
            components["grids"],
            "initial-grid",
            "dirty-transform",
            next_use="stage:residual-grid" if mode == "clean" else "none",
        )
        if mode == "clean":
            add_lifetime(
                "grids",
                campaign.FULL_GEOMETRY_RESIDUAL_GRID_BYTES,
                "residual-grid",
                "residual-transform",
                index=1,
            )
        add_lifetime(
            "source row blocks",
            components["source row blocks"],
            "source-ingest",
            "initial-grid",
            next_use="stage:residual-grid" if mode == "clean" else "none",
        )
        if mode == "clean":
            add_lifetime(
                "source row blocks",
                components["source row blocks"],
                "residual-grid",
                "residual-grid",
                index=1,
            )
        add_lifetime(
            "FFT chunks",
            components["FFT chunks"],
            "dirty-transform",
            "dirty-transform",
            next_use="stage:residual-transform" if mode == "clean" else "none",
        )
        if mode == "clean":
            add_lifetime(
                "FFT chunks",
                components["FFT chunks"],
                "residual-transform",
                "residual-transform",
                index=1,
            )
        add_lifetime(
            "AWProject MT-MFS run state",
            components["AWProject MT-MFS run state"],
            "dirty-transform",
            "finish",
        )
        density_through = "residual-grid" if mode == "clean" else "initial-grid"
        add_lifetime(
            "mosaic weighting density maps",
            components["mosaic weighting density maps"],
            "weighting",
            density_through,
        )
        for component in (
            "AWProject CF pixels",
            "AWProject CF index",
            "POINTING index",
        ):
            add_lifetime(
                component,
                components[component],
                "prepare",
                "residual-grid" if mode == "clean" else "initial-grid",
            )
        add_lifetime(
            "AWProject source-order tap scratch",
            components["AWProject source-order tap scratch"],
            "initial-grid",
            "residual-grid" if mode == "clean" else "initial-grid",
        )
        add_lifetime(
            "AWProject safety margin",
            components["AWProject safety margin"],
            "prepare",
            "product-write",
        )
        add_lifetime(
            "AWProject MT-MFS finish state",
            components["AWProject MT-MFS finish state"],
            "finish",
            "finish",
        )
        add_lifetime(
            "AWProject MT-MFS product state",
            components["AWProject MT-MFS product state"],
            "product-materialization",
            "product-write",
        )
        add_lifetime(
            "product writer scratch",
            components["product writer scratch"],
            "product-write",
            "product-write",
        )
        if mode == "clean":
            add_lifetime(
                "AWProject CASA-layout model FFT staging",
                components["AWProject CASA-layout model FFT staging"],
                "model-transform",
                "model-transform",
            )
            add_lifetime(
                "AWProject MT-MFS bounded multiscale scratch",
                components["AWProject MT-MFS bounded multiscale scratch"],
                "minor-cycle",
                "minor-cycle",
            )
            add_lifetime(
                "AWProject compact replay retention",
                components["AWProject compact replay retention"],
                "residual-grid",
                "residual-transform",
            )

        stage_index = {
            stage: index
            for index, stage in enumerate(campaign.REQUIRED_LIFETIME_STAGES)
        }
        stage_peaks = []
        for stage in campaign.REQUIRED_LIFETIME_STAGES:
            index = stage_index[stage]
            resident = sum(
                int(spec["resident_bytes"])
                for spec in lifetime_specs
                if stage_index[str(spec["live_from"])]
                <= index
                <= stage_index[str(spec["live_through"])]
            )
            stage_peaks.append((stage, resident))
        peak_bytes = max(resident for _, resident in stage_peaks)
        logical_bytes = sum(components.values())
        lifetime_rows = [
            "standard_mfs_execution_lifetime "
            f"allocation_id={spec['allocation_id']} component={spec['component']} "
            f"logical_bytes={spec['logical_bytes']} "
            f"residency_index={spec['residency_index']} backing={spec['backing']} "
            f"resident_bytes={spec['resident_bytes']} stored_bytes=0 "
            f"live_from={spec['live_from']} live_through={spec['live_through']} "
            f"next_use={spec['next_use']}"
            for spec in lifetime_specs
        ]
        stages = "\n".join(
            "standard_mfs_execution_lifetime_stage "
            f"stage={stage} resident_bytes={resident} stored_bytes=0 "
            f"host_heap_bytes={resident} unified_memory_bytes=0 metal_private_bytes=0 "
            "memory_mapped_bytes=0 temporary_spill_bytes=0 "
            "memory_mapped_stored_bytes=0 temporary_spill_stored_bytes=0"
            for stage, resident in stage_peaks
        )
        policy_actions = {
            "conservative-no-swap": (
                "admission_action=no-swap-headroom "
                "swap_action=avoid-intentional-swap "
                "stage_lifetime_release_requested=false "
                "next_use_aware_replay_requested=false"
            ),
            "aggressive": (
                "admission_action=physical-process-ceiling "
                "swap_action=allow-compression-or-incidental-swap "
                "stage_lifetime_release_requested=false "
                "next_use_aware_replay_requested=false"
            ),
            "oversubscribe": (
                "admission_action=explicit-oversubscription-target "
                "swap_action=intentional-oversubscription "
                "stage_lifetime_release_requested=false "
                "next_use_aware_replay_requested=false"
            ),
            "stage-aware": (
                "admission_action=no-swap-headroom "
                "swap_action=avoid-intentional-swap "
                "stage_lifetime_release_requested=true "
                "next_use_aware_replay_requested=false"
            ),
            "hybrid": (
                "admission_action=physical-process-ceiling "
                "swap_action=allow-compression-or-incidental-swap "
                "stage_lifetime_release_requested=true "
                "next_use_aware_replay_requested=true"
            ),
        }[policy]
        return "\n".join(
            (
                "standard_mfs_planning_resources "
                f"memory_pressure_policy={policy} "
                f"memory_target_bytes={memory_target_bytes} "
                "memory_target_semantics=incremental-operation-residency "
                f"memory_target_origin={memory_target_origin} "
                "incremental_operation_budget_bytes="
                f"{incremental_operation_budget_bytes} "
                f"process_baseline_bytes={process_baseline_bytes} "
                f"process_total_ceiling_bytes={process_total_ceiling_bytes} "
                "process_total_ceiling_origin="
                "baseline-plus-no-swap-headroom-capped-to-physical "
                "target_projected_process_total_bytes="
                f"{target_projected_process_total_bytes} "
                "target_projected_process_excess_bytes="
                f"{target_projected_process_excess_bytes} "
                f"physical_memory_bytes={physical_memory_bytes} "
                f"no_swap_headroom_bytes={no_swap_headroom_bytes} "
                "process_physical_footprint_bytes="
                f"{process_baseline_bytes}",
                "standard_mfs_memory_runtime_actions "
                f"policy={policy} {policy_actions} "
                "replay_prime_stage=residual-grid "
                "replay_retention_action=pinned-no-eviction-source-order "
                "known_last_use_release_active=true "
                "product_streaming_active=false replay_spill_active=false "
                "storage_demotion_active=false",
                "standard_mfs_execution_plan execution_mode=planner "
                f"rows_total=63 lifetime_logical_bytes={logical_bytes} "
                f"memory_target_bytes={memory_target_bytes} "
                "memory_target_semantics=incremental-operation-residency "
                f"process_baseline_bytes={process_baseline_bytes} "
                f"process_total_ceiling_bytes={process_total_ceiling_bytes} "
                "projected_process_total_bytes="
                f"{process_baseline_bytes + peak_bytes} "
                "projected_process_excess_bytes="
                f"{max(0, process_baseline_bytes + peak_bytes - process_total_ceiling_bytes)} "
                f"lifetime_peak_bytes={peak_bytes} planned_peak_bytes={peak_bytes} "
                "lifetime_stored_peak_bytes=0 lifetime_stored_peak_stage=none "
                "metal_eligible=false",
                *allocation_rows,
                stages,
                *lifetime_rows,
                *(
                    [
                        "awproject_compact_replay_cache "
                        f"resident_bytes={campaign.REPLAY_WORKING_SET_REFERENCE_BYTES} "
                        f"compiled_total_bytes={campaign.REPLAY_WORKING_SET_REFERENCE_BYTES} "
                        "compiled_total_bytes_complete=true "
                        "resident_blocks=16 partial_blocks=0 rejected_blocks=0"
                    ]
                    if mode == "clean"
                    else []
                ),
                "standard_mfs_planner_preflight status=admitted rows_total=63 "
                f"ddids=16 memory_pressure_policy={policy} "
                "visibility_streamed=false replay_compiled=false "
                "grids_allocated=false products_materialized=false",
            )
        )

    @classmethod
    def execution_log(
        cls,
        policy: str,
        *,
        mode: str,
        memory_target_mb: int | None,
    ) -> str:
        stages = campaign.required_runtime_memory_stages(mode)
        required_fields = " ".join(
            f"{field}=0"
            for field in campaign.REQUIRED_STAGE_MEMORY_FIELDS
            if field != "elapsed_monotonic_ms"
        )
        return (
            cls.planner_log(
                policy,
                mode=mode,
                memory_target_mb=memory_target_mb,
            )
            + "\n"
            + "\n".join(
                "standard_mfs_stage_memory "
                f"phase={stage}_end stage={stage} {required_fields} "
                f"elapsed_monotonic_ms={index + 1} peak_observation_complete=true"
                for index, stage in enumerate(stages)
            )
            + "\n"
        )

    @staticmethod
    def stage_timings(mode: str) -> dict[str, float]:
        required = (
            campaign.REQUIRED_CLEAN_TIMING_STAGES
            if mode == "clean"
            else campaign.REQUIRED_DIRTY_TIMING_STAGES
        )
        return {stage: float(index + 1) for index, stage in enumerate(required)}

    @staticmethod
    def measured_telemetry() -> dict:
        def sample(elapsed: float, process_peak: int) -> dict:
            value = {field: 0 for field in SAMPLE_FIELDS}
            value.update(
                {
                    "observed_at": f"2026-07-30T00:00:0{int(elapsed)}Z",
                    "elapsed_seconds": elapsed,
                    "page_size_bytes": 16_384,
                    "physical_memory_bytes": 32 * 1024**3,
                    "memory_free_percent": 25,
                    "host_compressed_memory_bytes": 100 + int(elapsed),
                    "swap_used_bytes": 200 + int(elapsed),
                    "process_pid": 42,
                    "process_physical_footprint_bytes": process_peak,
                    "process_physical_footprint_bytes_lifetime_peak": process_peak,
                    "process_resident_memory_bytes": process_peak,
                    "process_page_faults": 10 + int(elapsed),
                    "process_disk_read_bytes": 1000 + int(elapsed),
                    "process_disk_write_bytes": 2000 + int(elapsed),
                    "spill_volume_path": "/Volumes/EXTERNAL",
                    "spill_volume_device": "disk4",
                    "spill_volume_read_bytes": 3000 + int(elapsed),
                    "spill_volume_write_bytes": 4000 + int(elapsed),
                }
            )
            return value

        return build_host_telemetry_result(
            interval_seconds=1.0,
            samples=[sample(0.0, 25), sample(1.0, 100)],
            errors=[],
        )

    @staticmethod
    def monitor(root: Path) -> campaign.MonitorResult:
        return campaign.MonitorResult(
            exit_code=0,
            elapsed_seconds=1.0,
            stop_reason=None,
            sample_count=2,
            process_tree_resident_bytes_peak=100,
            memory_free_percent_min=25.0,
            swap_used_bytes_peak=201,
            swapin_bytes_delta=0,
            swapout_bytes_delta=0,
            swap_io_bytes_per_second_max=0.0,
            stdout_log_path=str(root / "outer.log"),
            stdout_log_sha256="a" * 64,
        )

    def workload_result(
        self,
        *,
        mode: str,
        policy: str,
        memory_target_mb: int | None,
    ) -> dict:
        expected_products = (
            campaign.EXPECTED_19_PRODUCTS
            if mode == "clean"
            else campaign.EXPECTED_DIRTY_PRODUCTS
        )
        return {
            "kind": "workload_run",
            "status": "completed",
            "mode": {"imaging_memory_target_mb": memory_target_mb},
            "comparison": {
                "products": list(expected_products),
                "require_exact_product_inventory": True,
                "require_metadata_parity": True,
                "tolerances": {"default": {"require_topology_parity": True}},
            },
            "environment": {
                "runtime": {
                    "platform": "macOS-15.5-arm64",
                    "machine": "arm64",
                    "physical_memory_bytes": campaign.ACCEPTANCE_PHYSICAL_MEMORY_BYTES,
                }
            },
            "command": {
                "kind": "legacy_benchmark_script",
                "env": {
                    "IMAGER_BENCH_SKIP_CASA": "1",
                    **self.storage["command_environment"],
                    **(
                        {"IMAGER_BENCH_IMAGING_MEMORY_TARGET_MB": str(memory_target_mb)}
                        if memory_target_mb is not None
                        else {}
                    ),
                },
            },
            "results": {
                "backend_plan_logs": campaign.workload_harness.parse_backend_plan_logs(
                    self.execution_log(
                        policy,
                        mode=mode,
                        memory_target_mb=memory_target_mb,
                    )
                ),
                "host_telemetry": self.measured_telemetry(),
                "stage_medians_ms": {
                    "rust": self.stage_timings(mode),
                    "casa": {},
                },
                "product_comparison": {
                    "status": "completed",
                    "requested_products": list(expected_products),
                    "product_inventory": {"status": "matched"},
                    "tolerance_evaluation": {"status": "passed"},
                    "structured_difference_review": {"label": "good"},
                    "products": {
                        product: {"status": "compared"} for product in expected_products
                    },
                },
            },
        }

    def write_experiment(
        self,
        *,
        name: str,
        mode: str,
        policy: str,
        promoted: campaign.EvidenceRef,
        dirty_policy: campaign.EvidenceRef | None = None,
        memory_target_mb: int | None = None,
    ) -> Path:
        workload = self.workload_result(
            mode=mode,
            policy=policy,
            memory_target_mb=memory_target_mb,
        )
        workload_path = self.root / f"{name}-workload.json"
        workload_path.write_text(json.dumps(workload), encoding="utf-8")
        monitor = self.monitor(self.root)
        experiment = {
            "kind": "vlass_full_geometry_memory_experiment",
            "status": "completed",
            "mode": mode,
            "workload_kind": "single-field",
            "execution_intent": "execute-12150",
            "policy": policy,
            "requested_memory_target_mb": memory_target_mb,
            "promotion_4096_sha256": promoted.sha256,
            "command_environment": dict(self.storage["command_environment"]),
            "storage_bandwidth": self.storage,
            "targets": {"artifact_root": str(self.artifact_root)},
            "dirty_policy_receipt_sha256": (
                dirty_policy.sha256 if dirty_policy is not None else None
            ),
            "run_workload_result": {
                "path": str(workload_path),
                "sha256": campaign.sha256_file(workload_path),
            },
            "outer_monitor": campaign.asdict(monitor),
            "memory_evidence": campaign.extract_execution_memory_evidence(
                workload,
                mode=mode,
                expected_policy=policy,
                expected_memory_target_mb=memory_target_mb,
                outer_monitor=monitor,
            ),
        }
        experiment_path = self.root / f"{name}-experiment.json"
        experiment_path.write_text(json.dumps(experiment), encoding="utf-8")
        return experiment_path

    def test_policy_order_is_the_five_approved_policies_exactly_once(self) -> None:
        self.assertEqual(
            campaign.POLICIES,
            (
                "conservative-no-swap",
                "aggressive",
                "oversubscribe",
                "stage-aware",
                "hybrid",
            ),
        )
        self.assertEqual(len(campaign.POLICIES), len(set(campaign.POLICIES)))

    def test_promotion_receipt_binds_geometry_and_both_evidence_rows(self) -> None:
        reference = campaign.validate_promoted_4096_receipt(self.promotion)
        self.assertEqual(reference.path, str(self.promotion.resolve()))
        self.assertEqual(len(reference.sha256), 64)

        value = json.loads(self.promotion.read_text(encoding="utf-8"))
        value["geometry"]["spw"] = "2,7,12,17"
        self.promotion.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(campaign.CampaignError, "geometry must bind"):
            campaign.validate_promoted_4096_receipt(self.promotion)

    def test_promotion_rejects_changed_referenced_content(self) -> None:
        value = json.loads(self.promotion.read_text(encoding="utf-8"))
        value["workload_result_sha256"] = "0" * 64
        self.promotion.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(campaign.CampaignError, "does not match"):
            campaign.validate_promoted_4096_receipt(self.promotion)

    def test_promotion_rejects_wrong_4096_workload_after_rehash(self) -> None:
        workload = json.loads(self.workload_result_4096.read_text(encoding="utf-8"))
        workload["mode"]["image_shape"] = [2048, 2048]
        self.workload_result_4096.write_text(
            json.dumps(workload),
            encoding="utf-8",
        )
        workload_sha256 = campaign.sha256_file(self.workload_result_4096)
        trajectory = json.loads(self.trajectory.read_text(encoding="utf-8"))
        trajectory["workload_result_sha256"] = workload_sha256
        self.trajectory.write_text(json.dumps(trajectory), encoding="utf-8")
        promotion = json.loads(self.promotion.read_text(encoding="utf-8"))
        promotion["workload_result_sha256"] = workload_sha256
        promotion["trajectory_receipt_sha256"] = campaign.sha256_file(self.trajectory)
        self.promotion.write_text(json.dumps(promotion), encoding="utf-8")
        with self.assertRaisesRegex(campaign.CampaignError, "mode/geometry mismatch"):
            campaign.validate_promoted_4096_receipt(self.promotion)

    def test_promotion_rejects_failed_comparison_tolerance_after_rehash(self) -> None:
        comparison = json.loads(self.comparison.read_text(encoding="utf-8"))
        comparison["tolerance_evaluation"]["status"] = "failed"
        self.comparison.write_text(json.dumps(comparison), encoding="utf-8")
        comparison_sha256 = campaign.sha256_file(self.comparison)

        workload = json.loads(self.workload_result_4096.read_text(encoding="utf-8"))
        workload["results"]["product_comparison"] = comparison
        self.workload_result_4096.write_text(
            json.dumps(workload),
            encoding="utf-8",
        )
        workload_sha256 = campaign.sha256_file(self.workload_result_4096)

        trajectory = json.loads(self.trajectory.read_text(encoding="utf-8"))
        trajectory["workload_result_sha256"] = workload_sha256
        trajectory["comparison_receipt_sha256"] = comparison_sha256
        self.trajectory.write_text(json.dumps(trajectory), encoding="utf-8")

        promotion = json.loads(self.promotion.read_text(encoding="utf-8"))
        promotion["workload_result_sha256"] = workload_sha256
        promotion["comparison_receipt_sha256"] = comparison_sha256
        promotion["trajectory_receipt_sha256"] = campaign.sha256_file(self.trajectory)
        self.promotion.write_text(json.dumps(promotion), encoding="utf-8")
        with self.assertRaisesRegex(
            campaign.CampaignError,
            "tolerance evaluation did not pass",
        ):
            campaign.validate_promoted_4096_receipt(self.promotion)

    def test_passed_tolerances_make_structure_review_diagnostic(self) -> None:
        comparison = json.loads(self.comparison.read_text(encoding="utf-8"))
        comparison["structured_difference_review"] = {
            "label": "investigate",
            "summary": "bounded low-amplitude coherent difference",
        }
        self.comparison.write_text(json.dumps(comparison), encoding="utf-8")

        validated = campaign.validate_comparison_receipt(self.comparison)

        self.assertEqual(
            "investigate",
            validated["structured_difference_review"]["label"],
        )

    def test_diagnostic_component_and_cycle_mismatch_does_not_block_promotion(
        self,
    ) -> None:
        trajectory = json.loads(self.trajectory.read_text(encoding="utf-8"))
        trajectory["coverage"]["same_cycle_count"] = False
        trajectory["discrete_parity"]["status"] = "diagnostic_mismatch"
        trajectory["rust_cycles"] = 6
        trajectory["component_selection"]["status"] = "diagnostic_mismatch"
        trajectory["major_cycle_residual"]["status"] = "diagnostic_mismatch"
        self.trajectory.write_text(json.dumps(trajectory), encoding="utf-8")
        promotion = json.loads(self.promotion.read_text(encoding="utf-8"))
        promotion["trajectory_receipt_sha256"] = campaign.sha256_file(self.trajectory)
        self.promotion.write_text(json.dumps(promotion), encoding="utf-8")

        reference = campaign.validate_promoted_4096_receipt(self.promotion)

        self.assertEqual(str(self.promotion.resolve()), reference.path)

    def test_promotion_rejects_incomplete_component_trajectory_after_rehash(
        self,
    ) -> None:
        trajectory = json.loads(self.trajectory.read_text(encoding="utf-8"))
        trajectory["component_selection"]["status"] = "failed"
        self.trajectory.write_text(json.dumps(trajectory), encoding="utf-8")
        promotion = json.loads(self.promotion.read_text(encoding="utf-8"))
        promotion["trajectory_receipt_sha256"] = campaign.sha256_file(self.trajectory)
        self.promotion.write_text(json.dumps(promotion), encoding="utf-8")
        with self.assertRaisesRegex(
            campaign.CampaignError,
            "component_selection trajectory evidence must pass",
        ):
            campaign.validate_promoted_4096_receipt(self.promotion)

    def test_workload_kind_binds_single_field_or_exact_63_field_selector(self) -> None:
        all_fields = campaign.load_json(
            campaign.DEFAULT_ALL_FIELDS_DIRTY_WORKLOAD,
            label="all-fields dirty workload",
        )
        campaign.validate_common_science_contract(
            all_fields,
            mode="dirty",
            workload_kind="all-fields",
        )
        with self.assertRaisesRegex(campaign.CampaignError, "approved VLASS contract"):
            campaign.validate_common_science_contract(
                all_fields,
                mode="dirty",
                workload_kind="single-field",
            )

        value = json.loads(self.promotion.read_text(encoding="utf-8"))
        value["workload_kind"] = "all-fields"
        value["geometry"].update(
            {
                "field": campaign.ALL_FIELDS_SELECTOR,
                "dataset_selection": "all_fields",
                "field_count": 63,
            }
        )
        workload = json.loads(self.workload_result_4096.read_text(encoding="utf-8"))
        workload["command"]["env"]["IMAGER_BENCH_FIELD"] = campaign.ALL_FIELDS_SELECTOR
        self.workload_result_4096.write_text(
            json.dumps(workload),
            encoding="utf-8",
        )
        value["workload_result_sha256"] = campaign.sha256_file(
            self.workload_result_4096
        )
        trajectory = json.loads(self.trajectory.read_text(encoding="utf-8"))
        trajectory["geometry"] = campaign._expected_bound_geometry(
            "all-fields",
            imsize=4096,
        )
        trajectory["workload_result_sha256"] = value["workload_result_sha256"]
        self.trajectory.write_text(json.dumps(trajectory), encoding="utf-8")
        value["trajectory_receipt_sha256"] = campaign.sha256_file(self.trajectory)
        self.promotion.write_text(json.dumps(value), encoding="utf-8")
        campaign.validate_promoted_4096_receipt(
            self.promotion,
            workload_kind="all-fields",
        )
        value["geometry"]["field_count"] = 62
        self.promotion.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(campaign.CampaignError, "field_count.*63"):
            campaign.validate_promoted_4096_receipt(
                self.promotion,
                workload_kind="all-fields",
            )

    def test_derived_manifest_is_rust_only_and_preserves_science(self) -> None:
        base = campaign.load_json(
            campaign.DEFAULT_CLEAN_WORKLOAD,
            label="clean base workload",
        )
        derived = campaign.derive_rust_only_manifest(
            base,
            mode="clean",
            policy="hybrid",
            campaign_label="test-campaign",
            memory_target_mb=None,
        )
        self.assertNotIn("casa", derived)
        self.assertEqual(derived["run"]["skip_casa"], "1")
        self.assertEqual(derived["run"]["repeats"], 1)
        self.assertEqual(derived["run"]["warmups"], 0)
        self.assertIs(derived["run"]["preverified_warm_cache"], True)
        self.assertEqual(derived["imaging"]["imsize"], 12150)
        self.assertEqual(derived["imaging"]["spw"], "2~17")
        self.assertEqual(derived["imaging"]["niter"], 2000)
        self.assertEqual(
            derived["comparison"]["products"],
            list(campaign.EXPECTED_19_PRODUCTS),
        )
        self.assertNotIn("imaging_memory_target_mb", derived["imaging"])
        self.assertEqual(
            derived["imaging"]["imaging_memory_pressure_policy"],
            "hybrid",
        )
        derived["imaging"]["imaging_memory_target_mb"] = 0
        with self.assertRaisesRegex(
            campaign.workload_harness.ContractError,
            "imaging_memory_target_mb must be >= 1",
        ):
            campaign.workload_harness.validate_workload_manifest(
                derived,
                source="zero target fixture",
            )

    def test_storage_probe_is_content_checked_reused_and_cleaned(self) -> None:
        volume = self.root / "external-artifacts"
        receipt = self.root / "measured-storage-bandwidth.json"
        first = campaign.storage_bandwidth_evidence(
            receipt,
            volume_path=volume,
            probe_bytes=8192,
            block_bytes=1024,
        )
        with mock.patch.object(campaign, "measure_storage_bandwidth") as measure:
            second = campaign.storage_bandwidth_evidence(
                receipt,
                volume_path=volume,
                probe_bytes=8192,
                block_bytes=1024,
            )
        measure.assert_not_called()
        self.assertEqual(first, second)
        self.assertEqual("measured", first["status"])
        self.assertGreater(first["read_bytes_per_second"], 0)
        self.assertGreater(first["write_bytes_per_second"], 0)
        self.assertEqual([], list(volume.glob(".casa-rs-vlass-storage-probe-*")))

        stored = json.loads(receipt.read_text(encoding="utf-8"))
        self.assertEqual(
            stored["probe"]["payload_sha256"],
            stored["probe"]["readback_sha256"],
        )
        self.assertIs(stored["probe"]["temporary_file_removed"], True)

        base = campaign.load_json(
            campaign.DEFAULT_DIRTY_WORKLOAD,
            label="dirty workload",
        )
        manifest = campaign.derive_rust_only_manifest(
            base,
            mode="dirty",
            policy="aggressive",
            campaign_label="storage-probe",
            memory_target_mb=None,
        )
        campaign.apply_storage_bandwidth_environment(
            manifest,
            evidence=first,
        )
        self.assertEqual(
            first["command_environment"],
            {key: manifest["run"]["env"][key] for key in first["command_environment"]},
        )

    def test_storage_probe_rejects_tampered_reused_rate(self) -> None:
        volume = self.root / "external-artifacts"
        receipt = self.root / "measured-storage-bandwidth.json"
        campaign.storage_bandwidth_evidence(
            receipt,
            volume_path=volume,
            probe_bytes=4096,
            block_bytes=512,
        )
        value = json.loads(receipt.read_text(encoding="utf-8"))
        value["read_bytes_per_second"] += 1
        receipt.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(campaign.CampaignError, "receipt mismatch"):
            campaign.storage_bandwidth_evidence(
                receipt,
                volume_path=volume,
                probe_bytes=4096,
                block_bytes=512,
            )

    def test_command_defaults_to_run_workload_dry_run(self) -> None:
        command = campaign.build_workload_command(
            manifest_path=self.root / "manifest.json",
            output_dir=self.root / "runs",
            artifact_root=self.root / "artifacts",
            run_label="bounded-row",
            dry_run=True,
        )
        self.assertEqual(Path(command[1]), campaign.RUN_WORKLOAD)
        self.assertIn("--dry-run", command)
        self.assertNotIn("casa", [part.lower() for part in command])

    def test_duplicate_receipt_rejects_identical_fingerprint(self) -> None:
        receipt_dir = self.root / "receipts"
        path = receipt_dir / "experiments" / "row.json"
        campaign.claim_receipt(
            path,
            {
                "kind": "vlass_full_geometry_memory_experiment",
                "experiment_fingerprint": "same",
            },
        )
        self.assertEqual(
            campaign.find_duplicate_receipt(receipt_dir, "same"),
            path,
        )
        self.assertIsNone(campaign.find_duplicate_receipt(receipt_dir, "new"))

    def test_clean_gate_requires_reviewed_executed_dirty_receipt(self) -> None:
        promoted = campaign.validate_promoted_4096_receipt(self.promotion)
        experiment = self.root / "dirty-experiment.json"
        policy = "aggressive"
        workload = {
            "kind": "workload_run",
            "status": "completed",
            "mode": {"imaging_memory_target_mb": None},
            "comparison": {
                "products": list(campaign.EXPECTED_DIRTY_PRODUCTS),
                "require_exact_product_inventory": True,
                "require_metadata_parity": True,
                "tolerances": {"default": {"require_topology_parity": True}},
            },
            "environment": {
                "runtime": {
                    "platform": "macOS-15.5-arm64",
                    "machine": "arm64",
                    "physical_memory_bytes": campaign.ACCEPTANCE_PHYSICAL_MEMORY_BYTES,
                }
            },
            "command": {
                "kind": "legacy_benchmark_script",
                "env": {
                    "IMAGER_BENCH_SKIP_CASA": "1",
                    **self.storage["command_environment"],
                },
            },
            "results": {
                "backend_plan_logs": campaign.workload_harness.parse_backend_plan_logs(
                    self.execution_log(
                        policy,
                        mode="dirty",
                        memory_target_mb=None,
                    )
                ),
                "host_telemetry": self.measured_telemetry(),
                "stage_medians_ms": {
                    "rust": self.stage_timings("dirty"),
                    "casa": {},
                },
                "product_comparison": {
                    "status": "completed",
                    "requested_products": list(campaign.EXPECTED_DIRTY_PRODUCTS),
                    "product_inventory": {"status": "matched"},
                    "tolerance_evaluation": {"status": "passed"},
                    "structured_difference_review": {"label": "good"},
                    "products": {
                        product: {"status": "compared"}
                        for product in campaign.EXPECTED_DIRTY_PRODUCTS
                    },
                },
            },
        }
        workload_path = self.root / "workload-result.json"
        workload_path.write_text(json.dumps(workload), encoding="utf-8")
        monitor = campaign.MonitorResult(
            exit_code=0,
            elapsed_seconds=1.0,
            stop_reason=None,
            sample_count=2,
            process_tree_resident_bytes_peak=100,
            memory_free_percent_min=25.0,
            swap_used_bytes_peak=201,
            swapin_bytes_delta=0,
            swapout_bytes_delta=0,
            swap_io_bytes_per_second_max=0.0,
            stdout_log_path=str(self.root / "outer.log"),
            stdout_log_sha256="a" * 64,
        )
        experiment.write_text(
            json.dumps(
                {
                    "kind": "vlass_full_geometry_memory_experiment",
                    "status": "completed",
                    "mode": "dirty",
                    "workload_kind": "single-field",
                    "execution_intent": "execute-12150",
                    "policy": policy,
                    "requested_memory_target_mb": None,
                    "promotion_4096_sha256": promoted.sha256,
                    "command_environment": dict(self.storage["command_environment"]),
                    "storage_bandwidth": self.storage,
                    "targets": {"artifact_root": str(self.artifact_root)},
                    "run_workload_result": {
                        "path": str(workload_path),
                        "sha256": campaign.sha256_file(workload_path),
                    },
                    "outer_monitor": campaign.asdict(monitor),
                    "memory_evidence": campaign.extract_execution_memory_evidence(
                        workload,
                        mode="dirty",
                        expected_policy=policy,
                        expected_memory_target_mb=None,
                        outer_monitor=monitor,
                    ),
                }
            ),
            encoding="utf-8",
        )
        dirty_gate = self.root / "dirty-policy.json"
        dirty_gate.write_text(
            json.dumps(
                {
                    "kind": "vlass_full_geometry_dirty_policy_promotion",
                    "status": "passed",
                    "workload_kind": "single-field",
                    "policy": policy,
                    "promotion_4096_sha256": promoted.sha256,
                    "experiment_receipt": str(experiment),
                    "experiment_receipt_sha256": campaign.sha256_file(experiment),
                    "gates": {gate: False for gate in campaign.DIRTY_POLICY_GATES},
                }
            ),
            encoding="utf-8",
        )
        policy, reference = campaign.validate_dirty_policy_promotion(
            dirty_gate,
            promoted_4096=promoted,
        )
        self.assertEqual(policy, "aggressive")
        self.assertEqual(reference.path, str(dirty_gate.resolve()))

        value = json.loads(experiment.read_text(encoding="utf-8"))
        value["memory_evidence"]["gates"]["stage_timings"] = False
        experiment.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(campaign.CampaignError, "does not match"):
            campaign.validate_dirty_policy_promotion(
                dirty_gate,
                promoted_4096=promoted,
            )

        workload["results"]["stage_medians_ms"]["rust"] = {}
        workload_path.write_text(json.dumps(workload), encoding="utf-8")
        value["run_workload_result"]["sha256"] = campaign.sha256_file(workload_path)
        value["memory_evidence"] = campaign.extract_execution_memory_evidence(
            workload,
            mode="dirty",
            expected_policy=policy,
            expected_memory_target_mb=None,
            outer_monitor=monitor,
        )
        experiment.write_text(json.dumps(value), encoding="utf-8")
        dirty_gate_value = json.loads(dirty_gate.read_text(encoding="utf-8"))
        dirty_gate_value["experiment_receipt_sha256"] = campaign.sha256_file(experiment)
        dirty_gate.write_text(json.dumps(dirty_gate_value), encoding="utf-8")
        with self.assertRaisesRegex(campaign.CampaignError, "measured gate"):
            campaign.validate_dirty_policy_promotion(
                dirty_gate,
                promoted_4096=promoted,
            )

        value["execution_intent"] = "dry-run"
        experiment.write_text(json.dumps(value), encoding="utf-8")
        dirty_gate_value["experiment_receipt_sha256"] = campaign.sha256_file(experiment)
        dirty_gate.write_text(json.dumps(dirty_gate_value), encoding="utf-8")
        with self.assertRaisesRegex(
            campaign.CampaignError,
            "completed 12,150 execution",
        ):
            campaign.validate_dirty_policy_promotion(
                dirty_gate,
                promoted_4096=promoted,
            )

    def test_execution_evidence_binds_target_host_stages_and_allocations(self) -> None:
        policy = "aggressive"
        workload = self.workload_result(
            mode="dirty",
            policy=policy,
            memory_target_mb=24 * 1024,
        )
        evidence = campaign.extract_execution_memory_evidence(
            workload,
            mode="dirty",
            expected_policy=policy,
            expected_memory_target_mb=24 * 1024,
            outer_monitor=self.monitor(self.root),
        )
        self.assertTrue(all(evidence["gates"].values()), evidence["negative_evidence"])
        self.assertEqual(
            campaign.FULL_GEOMETRY_EXACT_COMPONENT_BYTES["grids"],
            evidence["allocation_contract"]["bytes_by_component"]["grids"],
        )
        self.assertEqual([], evidence["stage_timings"]["missing_stages"])
        self.assertEqual(
            24 * 1024,
            evidence["memory_target"]["workload_result_mb"],
        )

        workload["mode"]["imaging_memory_target_mb"] = 8 * 1024
        mismatched = campaign.extract_execution_memory_evidence(
            workload,
            mode="dirty",
            expected_policy=policy,
            expected_memory_target_mb=24 * 1024,
            outer_monitor=self.monitor(self.root),
        )
        self.assertIs(False, mismatched["gates"]["memory_target_bound"])

        workload["mode"]["imaging_memory_target_mb"] = 24 * 1024
        workload["environment"]["runtime"]["physical_memory_bytes"] = 64 * 1024**3
        wrong_host = campaign.extract_execution_memory_evidence(
            workload,
            mode="dirty",
            expected_policy=policy,
            expected_memory_target_mb=24 * 1024,
            outer_monitor=self.monitor(self.root),
        )
        self.assertIs(False, wrong_host["gates"]["acceptance_host_32_gib"])

    def test_dirty_promotion_rejects_tampered_storage_binding(self) -> None:
        promoted = campaign.validate_promoted_4096_receipt(self.promotion)
        experiment = self.write_experiment(
            name="dirty-storage-binding",
            mode="dirty",
            policy="aggressive",
            promoted=promoted,
        )
        dirty_receipt = self.root / "dirty-storage-promotion.json"
        dirty_receipt.write_text(
            json.dumps(
                {
                    "kind": "vlass_full_geometry_dirty_policy_promotion",
                    "status": "passed",
                    "workload_kind": "single-field",
                    "policy": "aggressive",
                    "promotion_4096_sha256": promoted.sha256,
                    "experiment_receipt": str(experiment),
                    "experiment_receipt_sha256": campaign.sha256_file(experiment),
                }
            ),
            encoding="utf-8",
        )
        campaign.validate_dirty_policy_promotion(
            dirty_receipt,
            promoted_4096=promoted,
        )

        value = json.loads(experiment.read_text(encoding="utf-8"))
        value["storage_bandwidth"]["read_bytes_per_second"] += 1
        experiment.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(campaign.CampaignError, "does not match"):
            campaign.validate_dirty_policy_promotion(
                dirty_receipt,
                promoted_4096=promoted,
            )

        dirty_receipt_value = json.loads(dirty_receipt.read_text(encoding="utf-8"))
        dirty_receipt_value["experiment_receipt_sha256"] = campaign.sha256_file(
            experiment
        )
        dirty_receipt.write_text(
            json.dumps(dirty_receipt_value),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            campaign.CampaignError,
            "storage evidence does not match",
        ):
            campaign.validate_dirty_policy_promotion(
                dirty_receipt,
                promoted_4096=promoted,
            )

    def test_execution_evidence_rejects_unresolved_target_formula(self) -> None:
        workload = self.workload_result(
            mode="dirty",
            policy="aggressive",
            memory_target_mb=24 * 1024,
        )
        memory = workload["results"]["backend_plan_logs"]["memory_campaign"]
        memory["planning_resources"]["memory_target_bytes"] += campaign.MIB
        evidence = campaign.extract_execution_memory_evidence(
            workload,
            mode="dirty",
            expected_policy="aggressive",
            expected_memory_target_mb=24 * 1024,
            outer_monitor=self.monitor(self.root),
        )
        self.assertIs(False, evidence["gates"]["memory_target_bound"])
        self.assertIs(
            False,
            evidence["memory_target"]["resolution"]["matches"],
        )

        memory["planning_resources"]["memory_target_bytes"] -= campaign.MIB
        memory["planning_resources"]["memory_target_origin"] = (
            "aggressive-physical-ledger"
        )
        wrong_origin = campaign.extract_execution_memory_evidence(
            workload,
            mode="dirty",
            expected_policy="aggressive",
            expected_memory_target_mb=24 * 1024,
            outer_monitor=self.monitor(self.root),
        )
        self.assertIs(False, wrong_origin["gates"]["memory_target_bound"])
        self.assertEqual(
            ["cli-aggressive-physical-ceiling"],
            wrong_origin["memory_target"]["resolution"]["expected_origins"],
        )

    def test_resolved_target_separates_process_ceiling_from_operation_budget(
        self,
    ) -> None:
        gib = campaign.GIB
        planning_resources = {
            "memory_target_bytes": 12 * gib,
            "memory_target_semantics": "incremental-operation-residency",
            "memory_target_origin": "cli-aggressive-physical-ceiling",
            "physical_memory_bytes": 32 * gib,
            "no_swap_headroom_bytes": 12 * gib,
            "process_physical_footprint_bytes": 2 * gib,
            "process_baseline_bytes": 2 * gib,
            "process_total_ceiling_bytes": 14 * gib,
            "process_total_ceiling_origin": (
                "baseline-plus-no-swap-headroom-capped-to-physical"
            ),
            "incremental_operation_budget_bytes": 12 * gib,
            "target_projected_process_total_bytes": 14 * gib,
            "target_projected_process_excess_bytes": 0,
        }
        execution_plan = {
            "memory_target_bytes": 12 * gib,
            "memory_target_semantics": "incremental-operation-residency",
            "process_baseline_bytes": 2 * gib,
            "process_total_ceiling_bytes": 14 * gib,
            "planned_peak_bytes": 10 * gib,
            "projected_process_total_bytes": 12 * gib,
            "projected_process_excess_bytes": 0,
        }

        evidence = campaign.resolved_memory_target_evidence(
            planning_resources,
            policy="aggressive",
            requested_memory_target_mb=20 * 1024,
            execution_plan=execution_plan,
        )
        self.assertIs(True, evidence["matches"])
        self.assertEqual(12 * gib, evidence["expected_bytes"])
        self.assertEqual(
            14 * gib,
            evidence["expected_process_total_ceiling_bytes"],
        )
        self.assertEqual(
            12 * gib,
            evidence["expected_incremental_operation_budget_bytes"],
        )

        old_total_ceiling_target = dict(planning_resources)
        old_total_ceiling_target.update(
            {
                "memory_target_bytes": 14 * gib,
                "target_projected_process_total_bytes": 16 * gib,
                "target_projected_process_excess_bytes": 2 * gib,
            }
        )
        old_execution_plan = dict(execution_plan)
        old_execution_plan["memory_target_bytes"] = 14 * gib
        rejected = campaign.resolved_memory_target_evidence(
            old_total_ceiling_target,
            policy="aggressive",
            requested_memory_target_mb=20 * 1024,
            execution_plan=old_execution_plan,
        )
        self.assertIs(False, rejected["matches"])
        self.assertIn("memory_target_bytes", rejected["mismatches"])

    def test_resolved_target_validates_oversubscription_projection_excess(
        self,
    ) -> None:
        gib = campaign.GIB
        planning_resources = {
            "memory_target_bytes": 20 * gib,
            "memory_target_semantics": "incremental-operation-residency",
            "memory_target_origin": "cli-intentional-oversubscription",
            "physical_memory_bytes": 32 * gib,
            "no_swap_headroom_bytes": 12 * gib,
            "process_physical_footprint_bytes": 2 * gib,
            "process_baseline_bytes": 2 * gib,
            "process_total_ceiling_bytes": 14 * gib,
            "process_total_ceiling_origin": (
                "baseline-plus-no-swap-headroom-capped-to-physical"
            ),
            "incremental_operation_budget_bytes": 12 * gib,
            "target_projected_process_total_bytes": 22 * gib,
            "target_projected_process_excess_bytes": 8 * gib,
        }
        execution_plan = {
            "memory_target_bytes": 20 * gib,
            "memory_target_semantics": "incremental-operation-residency",
            "process_baseline_bytes": 2 * gib,
            "process_total_ceiling_bytes": 14 * gib,
            "planned_peak_bytes": 20 * gib,
            "projected_process_total_bytes": 22 * gib,
            "projected_process_excess_bytes": 8 * gib,
        }

        evidence = campaign.resolved_memory_target_evidence(
            planning_resources,
            policy="oversubscribe",
            requested_memory_target_mb=20 * 1024,
            execution_plan=execution_plan,
        )
        self.assertIs(True, evidence["matches"])
        self.assertEqual(
            8 * gib,
            evidence["expected_target_projected_process_excess_bytes"],
        )
        self.assertEqual(
            8 * gib,
            evidence["execution_projection"]["expected_projected_process_excess_bytes"],
        )

        for field, value, mismatch in (
            (
                "target_projected_process_excess_bytes",
                0,
                "target_projected_process_excess_bytes",
            ),
            (
                "memory_target_semantics",
                "process-total-residency",
                "memory_target_semantics",
            ),
        ):
            with self.subTest(planning_field=field):
                invalid_planning = dict(planning_resources)
                invalid_planning[field] = value
                rejected = campaign.resolved_memory_target_evidence(
                    invalid_planning,
                    policy="oversubscribe",
                    requested_memory_target_mb=20 * 1024,
                    execution_plan=execution_plan,
                )
                self.assertIs(False, rejected["matches"])
                self.assertIn(mismatch, rejected["mismatches"])

        invalid_execution = dict(execution_plan)
        invalid_execution["projected_process_excess_bytes"] = 0
        rejected_execution = campaign.resolved_memory_target_evidence(
            planning_resources,
            policy="oversubscribe",
            requested_memory_target_mb=20 * 1024,
            execution_plan=invalid_execution,
        )
        self.assertIs(False, rejected_execution["matches"])
        self.assertIn(
            "execution_plan.projected_process_excess_bytes",
            rejected_execution["mismatches"],
        )

    def test_execution_correctness_requires_passed_comparison_tolerance(self) -> None:
        workload = self.workload_result(
            mode="dirty",
            policy="aggressive",
            memory_target_mb=24 * 1024,
        )
        workload["results"]["product_comparison"]["tolerance_evaluation"]["status"] = (
            "failed"
        )
        evidence = campaign.extract_execution_memory_evidence(
            workload,
            mode="dirty",
            expected_policy="aggressive",
            expected_memory_target_mb=24 * 1024,
            outer_monitor=self.monitor(self.root),
        )
        self.assertIs(False, evidence["gates"]["dirty_correctness"])

    def test_execution_evidence_requires_complete_stage_peak_schema(self) -> None:
        workload = self.workload_result(
            mode="dirty",
            policy="aggressive",
            memory_target_mb=24 * 1024,
        )
        records = workload["results"]["backend_plan_logs"]["memory_campaign"][
            "stage_memory"
        ]["records"]
        records[0].pop("stage_observed_peak_metal_allocated_bytes")
        records[1]["peak_observation_complete"] = False
        evidence = campaign.extract_execution_memory_evidence(
            workload,
            mode="dirty",
            expected_policy="aggressive",
            expected_memory_target_mb=24 * 1024,
            outer_monitor=self.monitor(self.root),
        )
        telemetry = evidence["stage_timings"]["memory_telemetry"]
        self.assertIs(False, evidence["gates"]["per_stage_memory_telemetry"])
        self.assertIn("prepare", telemetry["missing_fields_by_stage"])
        self.assertIn("source-ingest", telemetry["incomplete_peak_stages"])

    def test_execution_evidence_rejects_explicitly_unavailable_stage_metric(
        self,
    ) -> None:
        workload = self.workload_result(
            mode="dirty",
            policy="aggressive",
            memory_target_mb=24 * 1024,
        )
        records = workload["results"]["backend_plan_logs"]["memory_campaign"][
            "stage_memory"
        ]["records"]
        records[0]["current_cpu_allocated_bytes"] = "unavailable"
        evidence = campaign.extract_execution_memory_evidence(
            workload,
            mode="dirty",
            expected_policy="aggressive",
            expected_memory_target_mb=24 * 1024,
            outer_monitor=self.monitor(self.root),
        )
        telemetry = evidence["stage_timings"]["memory_telemetry"]
        self.assertFalse(evidence["gates"]["per_stage_memory_telemetry"])
        self.assertIn("prepare", telemetry["missing_fields_by_stage"])
        self.assertIn(
            "current_cpu_allocated_bytes",
            telemetry["missing_fields_by_stage"]["prepare"],
        )

    def test_execution_evidence_requires_monotonic_stage_observations(self) -> None:
        workload = self.workload_result(
            mode="dirty",
            policy="aggressive",
            memory_target_mb=24 * 1024,
        )
        records = workload["results"]["backend_plan_logs"]["memory_campaign"][
            "stage_memory"
        ]["records"]
        records[-1]["elapsed_monotonic_ms"] = records[0]["elapsed_monotonic_ms"]
        evidence = campaign.extract_execution_memory_evidence(
            workload,
            mode="dirty",
            expected_policy="aggressive",
            expected_memory_target_mb=24 * 1024,
            outer_monitor=self.monitor(self.root),
        )
        telemetry = evidence["stage_timings"]["memory_telemetry"]
        self.assertIs(False, evidence["gates"]["per_stage_memory_telemetry"])
        self.assertIs(False, telemetry["elapsed_monotonic"])

    def test_clean_lifetime_rejects_eight_plane_residual_grid(self) -> None:
        workload = self.workload_result(
            mode="clean",
            policy="aggressive",
            memory_target_mb=28 * 1024,
        )
        memory = workload["results"]["backend_plan_logs"]["memory_campaign"]
        grid_rows = [
            row for row in memory["lifetimes"] if row.get("component") == "grids"
        ]
        self.assertEqual(2, len(grid_rows))
        grid_rows[1]["resident_bytes"] = campaign.FULL_GEOMETRY_EXACT_COMPONENT_BYTES[
            "grids"
        ]
        evidence = campaign.extract_execution_memory_evidence(
            workload,
            mode="clean",
            expected_policy="aggressive",
            expected_memory_target_mb=28 * 1024,
            outer_monitor=self.monitor(self.root),
        )
        lifetime = evidence["allocation_contract"]["lifetime_contract"]
        self.assertIs(False, evidence["gates"]["required_allocation_ledger"])
        self.assertTrue(
            any(
                "residual-grid..residual-transform" in row
                for row in lifetime["mismatches"]
            ),
            lifetime["mismatches"],
        )

    def test_lifetime_rejects_wrong_product_formula_and_backing(self) -> None:
        workload = self.workload_result(
            mode="dirty",
            policy="aggressive",
            memory_target_mb=24 * 1024,
        )
        memory = workload["results"]["backend_plan_logs"]["memory_campaign"]
        allocation = next(
            row
            for row in memory["allocations"]
            if row.get("component") == "AWProject MT-MFS product state"
        )
        allocation["bytes"] += 1
        lifetime_row = next(
            row
            for row in memory["lifetimes"]
            if row.get("component") == "AWProject MT-MFS product state"
        )
        lifetime_row["logical_bytes"] += 1
        lifetime_row["resident_bytes"] += 1
        lifetime_row["backing"] = "MemoryMapped"
        evidence = campaign.extract_execution_memory_evidence(
            workload,
            mode="dirty",
            expected_policy="aggressive",
            expected_memory_target_mb=24 * 1024,
            outer_monitor=self.monitor(self.root),
        )
        lifetime = evidence["allocation_contract"]["lifetime_contract"]
        self.assertIs(False, evidence["gates"]["required_allocation_ledger"])
        self.assertTrue(
            any(
                "AWProject MT-MFS product state" in row
                for row in lifetime["mismatches"]
            ),
            lifetime["mismatches"],
        )

    def test_clean_lifetime_requires_compiled_replay_working_set(self) -> None:
        workload = self.workload_result(
            mode="clean",
            policy="aggressive",
            memory_target_mb=28 * 1024,
        )
        replay = workload["results"]["backend_plan_logs"]["memory_campaign"][
            "compact_replay"
        ]
        replay["final"].pop("compiled_total_bytes")
        evidence = campaign.extract_execution_memory_evidence(
            workload,
            mode="clean",
            expected_policy="aggressive",
            expected_memory_target_mb=28 * 1024,
            outer_monitor=self.monitor(self.root),
        )
        lifetime = evidence["allocation_contract"]["lifetime_contract"]
        self.assertIs(False, evidence["gates"]["required_allocation_ledger"])
        self.assertTrue(
            any("compiled_total_bytes" in row for row in lifetime["mismatches"]),
            lifetime["mismatches"],
        )

    def test_clean_lifetime_requires_complete_compiled_replay_receipt(self) -> None:
        for completeness in (None, False):
            with self.subTest(completeness=completeness):
                workload = self.workload_result(
                    mode="clean",
                    policy="aggressive",
                    memory_target_mb=28 * 1024,
                )
                final_replay = workload["results"]["backend_plan_logs"][
                    "memory_campaign"
                ]["compact_replay"]["final"]
                if completeness is None:
                    final_replay.pop("compiled_total_bytes_complete")
                else:
                    final_replay["compiled_total_bytes_complete"] = completeness
                evidence = campaign.extract_execution_memory_evidence(
                    workload,
                    mode="clean",
                    expected_policy="aggressive",
                    expected_memory_target_mb=28 * 1024,
                    outer_monitor=self.monitor(self.root),
                )
                lifetime = evidence["allocation_contract"]["lifetime_contract"]
                self.assertFalse(evidence["gates"]["required_allocation_ledger"])
                self.assertTrue(
                    any(
                        "compiled_total_bytes_complete=true" in row
                        for row in lifetime["mismatches"]
                    ),
                    lifetime["mismatches"],
                )

    def test_full_clean_promotion_requires_exact_19_products_and_trajectory(
        self,
    ) -> None:
        promoted = campaign.validate_promoted_4096_receipt(self.promotion)
        policy = "aggressive"
        dirty_experiment = self.write_experiment(
            name="dirty",
            mode="dirty",
            policy=policy,
            promoted=promoted,
        )
        dirty_receipt_path = self.root / "dirty-promotion.json"
        dirty_receipt_path.write_text(
            json.dumps(
                {
                    "kind": "vlass_full_geometry_dirty_policy_promotion",
                    "status": "passed",
                    "workload_kind": "single-field",
                    "policy": policy,
                    "promotion_4096_sha256": promoted.sha256,
                    "experiment_receipt": str(dirty_experiment),
                    "experiment_receipt_sha256": campaign.sha256_file(dirty_experiment),
                }
            ),
            encoding="utf-8",
        )
        selected_policy, dirty_reference = campaign.validate_dirty_policy_promotion(
            dirty_receipt_path,
            promoted_4096=promoted,
        )
        self.assertEqual(policy, selected_policy)

        clean_experiment = self.write_experiment(
            name="clean",
            mode="clean",
            policy=policy,
            promoted=promoted,
            dirty_policy=dirty_reference,
            memory_target_mb=28 * 1024,
        )
        clean_experiment_value = json.loads(
            clean_experiment.read_text(encoding="utf-8")
        )
        self.assertIs(
            True,
            clean_experiment_value["memory_evidence"]["gates"]["clean_correctness"],
        )
        self.assertEqual(
            list(campaign.EXPECTED_19_PRODUCTS),
            clean_experiment_value["memory_evidence"]["product_comparison"][
                "expected_products"
            ],
        )
        incomplete_clean = self.workload_result(
            mode="clean",
            policy=policy,
            memory_target_mb=28 * 1024,
        )
        incomplete_clean["comparison"]["products"].remove(".mask")
        incomplete_clean["results"]["product_comparison"]["requested_products"].remove(
            ".mask"
        )
        incomplete_clean["results"]["product_comparison"]["products"].pop(".mask")
        incomplete_evidence = campaign.extract_execution_memory_evidence(
            incomplete_clean,
            mode="clean",
            expected_policy=policy,
            expected_memory_target_mb=28 * 1024,
            outer_monitor=self.monitor(self.root),
        )
        self.assertIs(False, incomplete_evidence["gates"]["clean_correctness"])

        clean_workload_path = Path(
            clean_experiment_value["run_workload_result"]["path"]
        )
        clean_workload = json.loads(clean_workload_path.read_text(encoding="utf-8"))
        workload_result_sha256 = campaign.sha256_file(clean_workload_path)
        product_comparison_sha256 = campaign.canonical_sha256(
            clean_workload["results"]["product_comparison"]
        )
        trajectory = self.root / "full-clean-trajectory.json"
        trajectory.write_text(
            json.dumps(
                {
                    "status": "completed",
                    "geometry": campaign._expected_bound_geometry(
                        "single-field",
                        imsize=12150,
                    ),
                    "coverage": {
                        "casa_complete": True,
                        "rust_complete": True,
                        "same_cycle_count": True,
                    },
                    "discrete_parity": {"status": "passed"},
                    "component_selection": {"status": "passed"},
                    "major_cycle_residual": {"status": "passed"},
                    "aligned_cycles": 5,
                    "casa_cycles": 5,
                    "rust_cycles": 5,
                    "no_divergence": True,
                    "workload_result_sha256": workload_result_sha256,
                    "product_comparison_sha256": product_comparison_sha256,
                }
            ),
            encoding="utf-8",
        )
        clean_promotion = self.root / "clean-promotion.json"
        clean_promotion.write_text(
            json.dumps(
                {
                    "kind": "vlass_full_geometry_clean_promotion",
                    "status": campaign.MEMORY_CAMPAIGN_PROMOTION_STATUS,
                    "promotion_scope": campaign.MEMORY_CAMPAIGN_PROMOTION_SCOPE,
                    "final_wave_acceptance": (
                        campaign.pending_final_wave_acceptance_contract()
                    ),
                    "workload_kind": "single-field",
                    "policy": policy,
                    "promotion_4096_sha256": promoted.sha256,
                    "dirty_policy_receipt_sha256": dirty_reference.sha256,
                    "experiment_receipt": str(clean_experiment),
                    "experiment_receipt_sha256": campaign.sha256_file(clean_experiment),
                    "workload_result_sha256": workload_result_sha256,
                    "product_comparison_sha256": product_comparison_sha256,
                    "trajectory_receipt": str(trajectory),
                    "trajectory_receipt_sha256": campaign.sha256_file(trajectory),
                    "gates": {gate: True for gate in campaign.CLEAN_PROMOTION_GATES},
                }
            ),
            encoding="utf-8",
        )
        clean_reference = campaign.validate_clean_promotion_receipt(
            clean_promotion,
            promoted_4096=promoted,
            dirty_policy=dirty_reference,
            expected_policy=policy,
            workload_kind="single-field",
        )
        self.assertEqual(str(clean_promotion.resolve()), clean_reference.path)
        final_contract = campaign.pending_final_wave_acceptance_contract()
        self.assertEqual(
            [
                {"workload_kind": "single-field", "mode": "dirty"},
                {"workload_kind": "single-field", "mode": "clean"},
                {"workload_kind": "all-fields", "mode": "dirty"},
                {"workload_kind": "all-fields", "mode": "clean"},
            ],
            final_contract["required_rows"],
        )
        self.assertEqual(10.0, final_contract["minimum_independent_speedup"])
        self.assertEqual("not-evaluated", final_contract["status"])
        self.assertIs(False, final_contract["satisfied"])

        trajectory_value = json.loads(trajectory.read_text(encoding="utf-8"))
        trajectory_value["workload_result_sha256"] = "0" * 64
        trajectory.write_text(json.dumps(trajectory_value), encoding="utf-8")
        clean_promotion_value = json.loads(clean_promotion.read_text(encoding="utf-8"))
        clean_promotion_value["trajectory_receipt_sha256"] = campaign.sha256_file(
            trajectory
        )
        clean_promotion.write_text(
            json.dumps(clean_promotion_value),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(campaign.CampaignError, "workload-result binding"):
            campaign.validate_clean_promotion_receipt(
                clean_promotion,
                promoted_4096=promoted,
                dirty_policy=dirty_reference,
                expected_policy=policy,
                workload_kind="single-field",
            )

        trajectory_value["workload_result_sha256"] = workload_result_sha256
        trajectory_value["product_comparison_sha256"] = "0" * 64
        trajectory.write_text(json.dumps(trajectory_value), encoding="utf-8")
        clean_promotion_value["trajectory_receipt_sha256"] = campaign.sha256_file(
            trajectory
        )
        clean_promotion.write_text(
            json.dumps(clean_promotion_value),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            campaign.CampaignError,
            "product-comparison binding",
        ):
            campaign.validate_clean_promotion_receipt(
                clean_promotion,
                promoted_4096=promoted,
                dirty_policy=dirty_reference,
                expected_policy=policy,
                workload_kind="single-field",
            )

        trajectory_value["product_comparison_sha256"] = product_comparison_sha256
        trajectory.write_text(json.dumps(trajectory_value), encoding="utf-8")
        clean_promotion_value["trajectory_receipt_sha256"] = campaign.sha256_file(
            trajectory
        )
        clean_promotion_value["final_wave_acceptance"]["satisfied"] = True
        clean_promotion.write_text(
            json.dumps(clean_promotion_value),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(campaign.CampaignError, "memory-campaign-only"):
            campaign.validate_clean_promotion_receipt(
                clean_promotion,
                promoted_4096=promoted,
                dirty_policy=dirty_reference,
                expected_policy=policy,
                workload_kind="single-field",
            )

        clean_promotion_value["final_wave_acceptance"] = (
            campaign.pending_final_wave_acceptance_contract()
        )
        clean_promotion.write_text(
            json.dumps(clean_promotion_value),
            encoding="utf-8",
        )
        trajectory_value["no_divergence"] = False
        trajectory.write_text(json.dumps(trajectory_value), encoding="utf-8")
        clean_promotion_value["trajectory_receipt_sha256"] = campaign.sha256_file(
            trajectory
        )
        clean_promotion.write_text(
            json.dumps(clean_promotion_value),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(campaign.CampaignError, "no divergence"):
            campaign.validate_clean_promotion_receipt(
                clean_promotion,
                promoted_4096=promoted,
                dirty_policy=dirty_reference,
                expected_policy=policy,
                workload_kind="single-field",
            )

    def test_planner_only_runs_five_real_preflight_receipts(self) -> None:
        args = campaign.parser().parse_args(
            [
                "--promoted-4096-receipt",
                str(self.promotion),
                "--receipt-dir",
                str(self.root / "campaign"),
            ]
        )

        def fake_planner(
            command,
            *,
            planned_environment,
            stdout_log_path,
            timeout_seconds,
        ):
            del timeout_seconds
            self.assertEqual(
                Path(command[0]),
                campaign.REPO_ROOT / "scripts" / "bench-imager-vs-casa.sh",
            )
            self.assertEqual("1", planned_environment["IMAGER_BENCH_PLAN_ONLY"])
            policy = planned_environment["IMAGER_BENCH_IMAGING_MEMORY_PRESSURE_POLICY"]
            stdout_log_path.parent.mkdir(parents=True, exist_ok=True)
            output = self.planner_log(policy)
            stdout_log_path.write_text(output, encoding="utf-8")
            return campaign.subprocess.CompletedProcess(command, 0, output)

        with mock.patch.object(
            campaign,
            "run_planner_preflight_command",
            side_effect=fake_planner,
        ) as planner:
            paths = campaign.run_campaign(args)
        self.assertEqual(len(paths), 5)
        self.assertEqual(5, planner.call_count)
        policies = []
        for path in paths:
            receipt = json.loads(path.read_text(encoding="utf-8"))
            expected_status = (
                "failed"
                if receipt["policy"] in {"oversubscribe", "stage-aware", "hybrid"}
                else "planner_admitted"
            )
            self.assertEqual(receipt["status"], expected_status)
            self.assertEqual(receipt["execution_intent"], "planner-only")
            self.assertEqual("single-field", receipt["workload_kind"])
            self.assertIsNone(receipt["requested_memory_target_mb"])
            self.assertEqual(1, receipt["targets"]["field_count"])
            self.assertEqual(
                "1", receipt["command_environment"]["IMAGER_BENCH_PLAN_ONLY"]
            )
            self.assertEqual(
                "unavailable",
                receipt["storage_bandwidth"]["status"],
            )
            self.assertEqual(
                "planner-only-does-not-measure-storage",
                receipt["storage_bandwidth"]["reason"],
            )
            self.assertNotIn(
                campaign.SPILL_READ_BANDWIDTH_ENV,
                receipt["command_environment"],
            )
            self.assertEqual(
                "rejected" if expected_status == "failed" else "admitted",
                receipt["memory_evidence"]["status"],
            )
            if expected_status == "failed":
                expected_gate = (
                    "resolved_memory_target"
                    if receipt["policy"] == "oversubscribe"
                    else "requested_policy_actions_active"
                )
                self.assertIn(
                    expected_gate,
                    receipt["memory_evidence"]["negative_evidence"],
                )
            self.assertTrue(receipt["never_invoke_casa_tclean"])
            self.assertEqual(receipt["casa_use"], "none")
            policies.append(receipt["policy"])
        self.assertEqual(tuple(policies), campaign.POLICIES)

        with self.assertRaisesRegex(campaign.CampaignError, "unchanged repeated"):
            campaign.run_campaign(args)

    def test_planner_policy_failure_does_not_skip_later_policies(self) -> None:
        args = campaign.parser().parse_args(
            [
                "--promoted-4096-receipt",
                str(self.promotion),
                "--receipt-dir",
                str(self.root / "campaign"),
            ]
        )
        calls = []

        def fake_planner(
            command,
            *,
            planned_environment,
            stdout_log_path,
            timeout_seconds,
        ):
            del timeout_seconds
            policy = planned_environment["IMAGER_BENCH_IMAGING_MEMORY_PRESSURE_POLICY"]
            calls.append(policy)
            output = self.planner_log(policy)
            stdout_log_path.parent.mkdir(parents=True, exist_ok=True)
            stdout_log_path.write_text(output, encoding="utf-8")
            return campaign.subprocess.CompletedProcess(
                command,
                17 if len(calls) == 1 else 0,
                output,
            )

        with mock.patch.object(
            campaign,
            "run_planner_preflight_command",
            side_effect=fake_planner,
        ):
            paths = campaign.run_campaign(args)

        self.assertEqual(list(campaign.POLICIES), calls)
        receipts = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
        statuses = [receipt["status"] for receipt in receipts]
        self.assertEqual("failed", statuses[0])
        self.assertEqual(
            "planner_preflight",
            receipts[0]["negative_evidence"][0]["stage"],
        )
        self.assertEqual(
            ["planner_admitted", "failed", "failed", "failed"],
            statuses[1:],
        )

    def test_explicit_execution_requires_artifact_root(self) -> None:
        args = campaign.parser().parse_args(
            [
                "--mode",
                "dirty",
                "--promoted-4096-receipt",
                str(self.promotion),
                "--receipt-dir",
                str(self.root / "campaign"),
                "--execute-12150",
            ]
        )
        with self.assertRaisesRegex(
            campaign.CampaignError,
            "explicit --artifact-root",
        ):
            campaign.run_campaign(args)

    def test_execute_campaign_measures_storage_once_and_injects_every_policy(
        self,
    ) -> None:
        casa_python = self.root / "casa-python"
        casa_python.write_text("#!/bin/sh\n", encoding="utf-8")
        artifact_root = self.root / "external-artifacts"
        receipt_dir = self.root / "execute-campaign"
        args = campaign.parser().parse_args(
            [
                "--mode",
                "dirty",
                "--promoted-4096-receipt",
                str(self.promotion),
                "--receipt-dir",
                str(receipt_dir),
                "--artifact-root",
                str(artifact_root),
                "--execute-12150",
            ]
        )
        storage = {
            "schema_version": 1,
            "status": "measured",
            "receipt": {
                "path": str(self.root / "storage.json"),
                "sha256": "a" * 64,
            },
            "volume_path": str(artifact_root),
            "read_bytes_per_second": 321,
            "write_bytes_per_second": 123,
            "command_environment": {
                campaign.SPILL_READ_BANDWIDTH_ENV: "321",
                campaign.SPILL_WRITE_BANDWIDTH_ENV: "123",
            },
        }
        with (
            mock.patch.dict(
                campaign.os.environ,
                {"CASA_RS_CASA_PYTHON": str(casa_python)},
                clear=False,
            ),
            mock.patch.object(
                campaign,
                "storage_bandwidth_evidence",
                return_value=storage,
            ) as probe,
            mock.patch.object(
                campaign,
                "run_bounded_command",
                side_effect=campaign.CampaignError("bounded test stop"),
            ),
        ):
            paths = campaign.run_campaign(args)
        probe.assert_called_once()
        self.assertEqual(5, len(paths))
        for path in paths:
            receipt = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(storage, receipt["storage_bandwidth"])
            self.assertEqual(
                storage["command_environment"],
                receipt["command_environment"],
            )
            manifest = json.loads(
                Path(receipt["derived_manifest_path"]).read_text(encoding="utf-8")
            )
            for key, value in storage["command_environment"].items():
                self.assertEqual(value, manifest["run"]["env"][key])


if __name__ == "__main__":
    unittest.main()
