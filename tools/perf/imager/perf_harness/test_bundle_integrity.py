# SPDX-License-Identifier: LGPL-3.0-or-later
"""Adversarial tests for fail-closed recipe bundle publication."""

from __future__ import annotations

import copy
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from perf_harness import (
    bundle_integrity,
    casa_image_compare as comparator,
    casa_tclean_workflow,
    image_compare,
)
from perf_harness.artifacts import atomic_write_json, prepare_atomic_directory_bundle
from perf_harness.casa_tclean import (
    RESULT_KIND,
    RESULT_SCHEMA_VERSION,
    canonical_sha256,
    inventory_product_siblings,
    summarize_completed_results,
)
from perf_harness.schema import RUN_RESULT_SCHEMA_VERSION, validate_run_result
from perf_harness.tree_identity import sha256_file
from test_support import canonical_test_environment


class RecipeBundleIntegrityTests(unittest.TestCase):
    def test_complete_revalidated_bundle_promotes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result, paths = _complete_bundle(pathlib.Path(temporary))

            with mock.patch.object(
                bundle_integrity, "validate_result_for_request"
            ) as validate_binding:
                published = casa_tclean_workflow.finalize_bundle_result(result)

            validate_binding.assert_called_once()
            self.assertEqual("complete", published["artifacts"]["bundle"]["state"])
            self.assertEqual(
                "passed", published["results"]["bundle_integrity"]["status"]
            )
            self.assertTrue(paths["final"].is_dir())
            self.assertFalse(paths["partial"].exists())

    def test_deleted_protocol_artifact_retains_typed_partial(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result, paths = _complete_bundle(pathlib.Path(temporary))
            paths["stdout"].unlink()

            with mock.patch.object(bundle_integrity, "validate_result_for_request"):
                failed = casa_tclean_workflow.finalize_bundle_result(result)

            self.assertEqual("failed_execution", failed["status"])
            self.assertEqual("integrity_failed", failed["artifacts"]["bundle"]["state"])
            self.assertEqual("artifact_integrity", failed["results"]["failure"]["kind"])
            validate_run_result(failed, source="synthetic integrity failure")
            self.assertTrue(paths["partial"].is_dir())
            self.assertFalse(paths["final"].exists())
            self.assertTrue((paths["partial"] / "receipt.json").is_file())

    def test_mutated_product_tree_retains_typed_partial(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result, paths = _complete_bundle(pathlib.Path(temporary))
            paths["product_data"].write_bytes(b"mutated science product")

            with mock.patch.object(bundle_integrity, "validate_result_for_request"):
                failed = casa_tclean_workflow.finalize_bundle_result(result)

            self.assertEqual("failed_execution", failed["status"])
            self.assertEqual("integrity_failed", failed["artifacts"]["bundle"]["state"])
            self.assertIn(
                "tree identity changed", failed["results"]["failure"]["reason"]
            )
            self.assertTrue(paths["partial"].is_dir())
            self.assertFalse(paths["final"].exists())

    def test_mutated_benchmark_log_retains_typed_partial(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result, paths = _complete_bundle(pathlib.Path(temporary))
            paths["benchmark_log"].write_text("mutated benchmark\n", encoding="utf-8")

            with mock.patch.object(bundle_integrity, "validate_result_for_request"):
                failed = casa_tclean_workflow.finalize_bundle_result(result)

            self.assertEqual("failed_execution", failed["status"])
            self.assertEqual("integrity_failed", failed["artifacts"]["bundle"]["state"])
            self.assertIn("SHA-256 mismatch", failed["results"]["failure"]["reason"])
            self.assertTrue(paths["partial"].is_dir())
            self.assertFalse(paths["final"].exists())

    def test_leftover_structure_workspace_retains_typed_partial(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result, paths = _complete_bundle(pathlib.Path(temporary))
            paths["structure_workspace"].mkdir()
            (paths["structure_workspace"] / "left.f64").write_bytes(b"partial")

            with mock.patch.object(bundle_integrity, "validate_result_for_request"):
                failed = casa_tclean_workflow.finalize_bundle_result(result)

            self.assertEqual("failed_execution", failed["status"])
            self.assertEqual("integrity_failed", failed["artifacts"]["bundle"]["state"])
            self.assertIn(
                "structure workspace remains",
                failed["results"]["failure"]["reason"],
            )
            self.assertTrue(paths["structure_workspace"].is_dir())
            self.assertFalse(paths["final"].exists())

    def test_mutated_measured_evidence_summary_retains_typed_partial(self) -> None:
        mutations = {
            "summary": lambda result: result["results"]["casa"]["evidence_summary"][
                "resources"
            ].__setitem__("peak_rss_bytes_max", 999),
            "stage_median": lambda result: result["results"]["stage_medians_ms"][
                "casa"
            ].__setitem__("tclean_task", 999.0),
            "benchmark_mirror": lambda result: result["benchmark_features"][
                "resources"
            ].__setitem__("casa_peak_rss_bytes", 999),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                result, paths = _complete_bundle(pathlib.Path(temporary))
                mutate(result)
                with mock.patch.object(bundle_integrity, "validate_result_for_request"):
                    failed = casa_tclean_workflow.finalize_bundle_result(result)
                self.assertEqual("failed_execution", failed["status"])
                self.assertEqual(
                    "integrity_failed", failed["artifacts"]["bundle"]["state"]
                )
                self.assertTrue(paths["partial"].is_dir())

    def test_mutated_casa_headline_retains_typed_partial(self) -> None:
        mutations = {
            "forged_runs": lambda result: result["results"]["casa"][
                "timings_seconds"
            ].__setitem__("runs", [123.0]),
            "forged_median": lambda result: result["results"]["casa"][
                "timings_seconds"
            ].__setitem__("median", 123.0),
            "warmup_count": lambda result: result["results"]["casa"].__setitem__(
                "warmup_count", 123
            ),
            "cache_role": lambda result: result["results"]["casa"].__setitem__(
                "cache_role", "cold"
            ),
            "status": lambda result: result["results"]["casa"].__setitem__(
                "status", "skipped"
            ),
            "reason": lambda result: result["results"]["casa"].__setitem__(
                "reason", "forged reason"
            ),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                result, paths = _complete_bundle(pathlib.Path(temporary))
                mutate(result)

                with mock.patch.object(bundle_integrity, "validate_result_for_request"):
                    failed = casa_tclean_workflow.finalize_bundle_result(result)

                self.assertEqual("failed_execution", failed["status"])
                self.assertEqual(
                    "integrity_failed", failed["artifacts"]["bundle"]["state"]
                )
                self.assertIn(
                    "headline timings/evidence",
                    failed["results"]["failure"]["reason"],
                )
                self.assertTrue(paths["partial"].is_dir())

    def test_warmup_wall_time_is_excluded_from_headline_timings(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result, paths = _complete_bundle(
                pathlib.Path(temporary), warmup_wall_seconds=123.0
            )

            self.assertEqual(
                {"runs": [1.0], "median": 1.0},
                result["results"]["casa"]["timings_seconds"],
            )
            with mock.patch.object(
                bundle_integrity, "validate_result_for_request"
            ) as validate_binding:
                published = casa_tclean_workflow.finalize_bundle_result(result)

            self.assertEqual(2, validate_binding.call_count)
            self.assertEqual("complete", published["artifacts"]["bundle"]["state"])
            self.assertEqual(1, published["results"]["casa"]["warmup_count"])
            self.assertTrue(paths["final"].is_dir())

    def test_warmup_wall_time_in_headline_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result, paths = _complete_bundle(
                pathlib.Path(temporary), warmup_wall_seconds=123.0
            )
            result["results"]["casa"]["timings_seconds"] = {
                "runs": [123.0, 1.0],
                "median": 62.0,
            }

            with mock.patch.object(bundle_integrity, "validate_result_for_request"):
                failed = casa_tclean_workflow.finalize_bundle_result(result)

            self.assertEqual("failed_execution", failed["status"])
            self.assertEqual("integrity_failed", failed["artifacts"]["bundle"]["state"])
            self.assertIn(
                "headline timings/evidence", failed["results"]["failure"]["reason"]
            )
            self.assertTrue(paths["partial"].is_dir())

    def test_claimed_warm_role_must_match_each_call_request(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result, paths = _complete_bundle(
                pathlib.Path(temporary), warmup_wall_seconds=123.0
            )
            result["run"]["cf_cache_role"] = "warm"
            result["results"]["casa"]["cache_role"] = "warm"
            groups = result["results"]["casa_tclean_calls"]
            for record in [*groups["warmups"], *groups["measured"]]:
                record["role"] = "warm"

            with mock.patch.object(bundle_integrity, "validate_result_for_request"):
                failed = casa_tclean_workflow.finalize_bundle_result(result)

            self.assertEqual("failed_execution", failed["status"])
            self.assertEqual("integrity_failed", failed["artifacts"]["bundle"]["state"])
            self.assertIn("request cache role", failed["results"]["failure"]["reason"])
            self.assertTrue(paths["partial"].is_dir())

    def test_repeatability_cannot_bind_a_warmup_call(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result, paths = _complete_bundle(
                pathlib.Path(temporary), warmup_wall_seconds=123.0
            )
            _retarget_single_comparison(result, call_name="warmup-001")

            with mock.patch.object(bundle_integrity, "validate_result_for_request"):
                failed = casa_tclean_workflow.finalize_bundle_result(result)

            self.assertEqual("failed_execution", failed["status"])
            self.assertEqual("integrity_failed", failed["artifacts"]["bundle"]["state"])
            self.assertIn("call topology", failed["results"]["failure"]["reason"])
            self.assertTrue(paths["partial"].is_dir())

    def test_repeatability_unknown_inner_comparison_kind_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result, paths = _complete_bundle(pathlib.Path(temporary))
            comparison = result["results"]["casa_repeatability_comparison"][
                "comparisons"
            ][0]
            comparison["comparison_kind"] = "not-even-a-real-kind"

            with mock.patch.object(bundle_integrity, "validate_result_for_request"):
                failed = casa_tclean_workflow.finalize_bundle_result(result)

            self.assertEqual("failed_execution", failed["status"])
            self.assertEqual("integrity_failed", failed["artifacts"]["bundle"]["state"])
            self.assertIn("comparison kind", failed["results"]["failure"]["reason"])
            self.assertTrue(paths["partial"].is_dir())

    def test_repeatability_outer_summary_is_exactly_derived(self) -> None:
        mutations = {
            "status": lambda value: value.__setitem__("status", "comparison_failed"),
            "reason": lambda value: value.__setitem__("reason", "forged"),
            "comparison_kind": lambda value: value.__setitem__(
                "comparison_kind", "all_measured_calls_repeatability"
            ),
            "baseline_call": lambda value: value.__setitem__(
                "baseline_call", "warmup-001"
            ),
            "compared_calls": lambda value: value.__setitem__("compared_calls", []),
            "comparison_mode": lambda value: value.__setitem__(
                "comparison_mode", "full"
            ),
            "source_regions": lambda value: value.__setitem__(
                "source_regions", [{"id": "forged"}]
            ),
            "tolerances": lambda value: value.__setitem__("tolerances", None),
            "products": lambda value: value.__setitem__("products", {}),
            "product_inventory": lambda value: value["product_inventory"].__setitem__(
                "status", "mismatch"
            ),
            "structured_difference_review": lambda value: value.__setitem__(
                "structured_difference_review", {"label": "good", "summary": "forged"}
            ),
            "unknown_field": lambda value: value.__setitem__("forged", True),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                result, _ = _complete_bundle(pathlib.Path(temporary))
                repeatability = result["results"]["casa_repeatability_comparison"]
                mutate(repeatability)

                with (
                    mock.patch.object(bundle_integrity, "validate_result_for_request"),
                    self.assertRaises(bundle_integrity.BundleIntegrityError),
                ):
                    bundle_integrity.validate_recipe_evidence_bundle(result)

    def test_structure_workspace_path_tampering_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            partial = root / "run.partial"
            comparison_root = partial / "comparisons"
            comparison_root.mkdir(parents=True)
            input_path = comparison_root / "casa-measured-001.comparison-input.json"
            input_path.write_text("{}\n", encoding="utf-8")

            cases = {
                "outside": root / "outside-workspace",
                "alternate_inside": comparison_root / "alternate-workspace",
            }
            for name, workspace in cases.items():
                with (
                    self.subTest(name=name),
                    self.assertRaises(bundle_integrity.BundleIntegrityError),
                ):
                    bundle_integrity._validate_structure_workspace(
                        {"structure_workspace_dir": str(workspace)},
                        input_path=input_path,
                        partial_root=partial,
                        right_call="measured-001",
                        label="fixture comparison",
                    )


def _complete_bundle(
    root: pathlib.Path, *, warmup_wall_seconds: float | None = None
) -> tuple[dict, dict[str, pathlib.Path]]:
    bundle = prepare_atomic_directory_bundle(root / "artifacts" / "run-1")
    partial = bundle.partial_path
    protocol_root = partial / "protocol" / "measured-001"
    product_prefix = partial / "casa" / "measured-001" / "casa"
    product_path = pathlib.Path(f"{product_prefix}.image.tt0")
    comparison_root = partial / "comparisons"
    panel_dir = comparison_root / "panels"
    structure_workspace = comparison_root / "casa-measured-001-structure-workspace"
    protocol_root.mkdir(parents=True)
    product_path.mkdir(parents=True)
    comparison_root.mkdir(parents=True)
    panel_dir.mkdir()

    product_data = product_path / "table.dat"
    product_data.write_bytes(b"frozen science product")
    (product_path / "table.lock").write_bytes(b"volatile lock")
    product_inventory = inventory_product_siblings(product_prefix)

    request_path = protocol_root / "request.json"
    result_path = protocol_root / "result.json"
    stdout_path = protocol_root / "stdout-stderr.log"
    casa_log_path = protocol_root / "casa-fixture.log"
    request = {
        "overrides": {"imagename": str(product_prefix)},
        "cache": {"role": "none"},
    }
    zero_resources = {
        "user_cpu_seconds": 0.0,
        "system_cpu_seconds": 0.0,
        "peak_rss_bytes": 1,
        "minor_page_faults": 0,
        "major_page_faults": 0,
        "block_input_operations": 0,
        "block_output_operations": 0,
        "disk_read_bytes": 0,
        "disk_write_bytes": 0,
        "voluntary_context_switches": 0,
        "involuntary_context_switches": 0,
    }
    effective_kwargs = {"imagename": str(product_prefix)}
    protocol_result = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "kind": RESULT_KIND,
        "status": "completed",
        "request_id": "fixture-measured-001",
        "action": "run",
        "casa": {},
        "recipe": {},
        "compatibility_normalizations": [],
        "version_defaults": {},
        "reproducibility_overrides": {},
        "effective_kwargs": effective_kwargs,
        "effective_kwargs_sha256": canonical_sha256(effective_kwargs),
        "cache": {
            "role": "none",
            "before": {"role": "none"},
            "after": {"role": "none"},
        },
        "mask_identity": None,
        "wall_seconds": 1.0,
        "stage_timings_seconds": {
            "protocol_preflight": 0.0,
            "tclean_task": 1.0,
            "product_inventory": 0.0,
            "cache_postcondition": 0.0,
            "protocol_total": 1.0,
        },
        "resources": {
            "schema_version": 1,
            "scope": "casa_python_process_during_protocol_execution",
            "peak_rss_source": "getrusage_rusage_self",
            "disk_io_source": "linux_proc_self_io",
            "before": zero_resources,
            "after": zero_resources,
            "delta": zero_resources,
        },
        "products": {"before": [], "after": product_inventory},
        "tclean_return": {"type": None, "present": False},
    }
    atomic_write_json(request_path, request)
    atomic_write_json(result_path, protocol_result)
    stdout_path.write_text("CASA stdout\n", encoding="utf-8")
    casa_log_path.write_text("CASA log\n", encoding="utf-8")

    suffixes = [".image.tt0"]
    tolerances = {
        "contract_version": 1,
        "require_full_array": False,
        "default": {"diff_rms_over_right_rms": 0.0},
        "products": {},
    }
    comparison_input = image_compare.normalize_comparison_request(
        {
            "mode": "sampled",
            "left_prefix": str(product_prefix),
            "right_prefix": str(product_prefix),
            "left_label": "CASA measured 1",
            "right_label": "CASA measured 1 product contract",
            "products": suffixes,
            "max_elements_per_product": 1024,
            "full_chunk_elements": 1024,
            "require_exact_product_inventory": True,
            "require_metadata_parity": True,
            "source_regions": [],
            "tolerances": tolerances,
            "panel_dir": str(panel_dir),
            "structure_workspace_dir": str(structure_workspace),
        }
    )
    panel_path = panel_dir / "image_tt0.review.png"
    panel_path.write_bytes(b"synthetic review png")
    exact_inventory = _exact_inventory(suffixes)
    raw_comparison = {
        "schema_version": image_compare.COMPARISON_SCHEMA_VERSION,
        "status": "completed",
        "reason": None,
        "request_binding": image_compare.comparison_request_binding(comparison_input),
        "request_sha256": comparison_input["request_sha256"],
        "comparison_mode": comparison_input["mode"],
        "max_elements_per_product": comparison_input["max_elements_per_product"],
        "full_chunk_elements": comparison_input["full_chunk_elements"],
        "left_prefix": comparison_input["left_prefix"],
        "right_prefix": comparison_input["right_prefix"],
        "left_label": comparison_input["left_label"],
        "right_label": comparison_input["right_label"],
        "requested_products": suffixes,
        "require_exact_product_inventory": True,
        "require_metadata_parity": True,
        "legacy_operand_aliases": False,
        "source_regions": [],
        "tolerances": tolerances,
        "panel_dir": str(panel_dir),
        "structure_workspace_dir": str(structure_workspace),
        "product_inventory": exact_inventory,
        "products": {
            ".image.tt0": {
                "status": "compared",
                "left_path": str(product_path),
                "right_path": str(product_path),
                "shape": [1, 1, 1, 1],
                "metadata_parity_required": True,
                "metadata": _matched_metadata([1, 1, 1, 1]),
                "diff_rms_over_right_rms": 0.0,
                "structured_difference": {
                    "status": "computed",
                    "review": {
                        "label": "good",
                        "summary": "fixture structured difference passed",
                        "checks": [],
                        "legend": comparator.structured_difference_review_legend(),
                    },
                },
                "review_panel": {
                    "status": "written",
                    "path": str(panel_path),
                    "sha256": sha256_file(panel_path),
                    "zoom_panel": {"status": "skipped", "reason": "fixture"},
                },
            }
        },
    }
    raw_comparison["structured_difference_review"] = (
        comparator.summarize_product_reviews(raw_comparison["products"])
    )
    image_compare.validate_comparison_output(raw_comparison, comparison_input)
    comparison_input_path = comparison_root / "comparison-input.json"
    comparison_output_path = comparison_root / "comparison.json"
    comparison_log_path = comparison_root / "comparison.log"
    atomic_write_json(comparison_input_path, comparison_input)
    atomic_write_json(comparison_output_path, raw_comparison)
    comparison_log_path.write_text("comparison log\n", encoding="utf-8")
    comparison = image_compare.apply_tolerance_contract(
        copy.deepcopy(raw_comparison), comparison_input
    )
    comparison.update(
        {
            "input": str(comparison_input_path),
            "input_sha256": sha256_file(comparison_input_path),
            "output": str(comparison_output_path),
            "output_sha256": sha256_file(comparison_output_path),
            "log": str(comparison_log_path),
            "log_sha256": sha256_file(comparison_log_path),
            "left_call": "measured-001",
            "right_call": "measured-001",
            "comparison_kind": "single_call_product_contract",
        }
    )

    benchmark_log = partial / "benchmark-summary.log"
    benchmark_log.write_text("benchmark\n", encoding="utf-8")
    call = {
        "name": "measured-001",
        "role": "none",
        "measured": True,
        "prefix": str(product_prefix),
        "request_path": str(request_path),
        "request_sha256": sha256_file(request_path),
        "result_path": str(result_path),
        "result_sha256": sha256_file(result_path),
        "stdout_stderr_path": str(stdout_path),
        "stdout_stderr_sha256": sha256_file(stdout_path),
        "exit_code": 0,
        "casa_log_paths": [str(casa_log_path)],
        "casa_log_identities": [
            {"path": str(casa_log_path), "sha256": sha256_file(casa_log_path)}
        ],
        "cache_receipt_sha256": None,
        "result": protocol_result,
    }
    evidence_summary = summarize_completed_results([call])
    evidence_resources = evidence_summary["resources"]
    stage_medians_ms = {
        name: seconds * 1000.0
        for name, seconds in evidence_summary["stage_seconds"]["median"].items()
    }
    result = {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": "workload_run",
        "status": "completed",
        "run_id": bundle.final_path.name,
        "created_at": "2026-07-20T00:00:00Z",
        "started_at": "2026-07-20T00:00:01Z",
        "completed_at": "2026-07-20T00:00:02Z",
        "exit_code": 0,
        "environment": canonical_test_environment(),
        "run": {"warmups": 0, "repeats": 1, "cf_cache_role": "none"},
        "comparison": {"products": suffixes},
        "artifacts": {
            "products_root": str(partial),
            "comparison_root": str(comparison_root),
            "protocol_root": str(partial / "protocol"),
            "bundle": {
                "state": "partial",
                "partial_root": str(partial),
                "final_root": str(bundle.final_path),
                "retained_root": str(partial),
                "execution_to_retained": {
                    "from": str(partial),
                    "to": str(partial),
                },
            },
        },
        "products": {"root": str(partial), "casa_prefix": str(product_prefix)},
        "logs": {
            "benchmark_log": str(benchmark_log),
            "benchmark_log_sha256": sha256_file(benchmark_log),
        },
        "benchmark_features": {
            "resources": {
                "casa_peak_rss_bytes": evidence_resources["peak_rss_bytes_max"],
                "casa_user_cpu_seconds_median": evidence_resources[
                    "user_cpu_seconds_median"
                ],
                "casa_system_cpu_seconds_median": evidence_resources[
                    "system_cpu_seconds_median"
                ],
                "casa_disk_read_bytes_median": evidence_resources[
                    "disk_read_bytes_median"
                ],
                "casa_disk_write_bytes_median": evidence_resources[
                    "disk_write_bytes_median"
                ],
                "casa_block_input_operations_median": evidence_resources[
                    "block_input_operations_median"
                ],
                "casa_block_output_operations_median": evidence_resources[
                    "block_output_operations_median"
                ],
                "casa_product_logical_bytes_median": evidence_resources[
                    "product_logical_bytes_median"
                ],
                "casa_cf_cache_logical_bytes_max": evidence_resources[
                    "cf_cache_logical_bytes_max"
                ],
                "casa_cf_cache_included_file_count_max": evidence_resources[
                    "cf_cache_included_file_count_max"
                ],
            }
        },
        "results": {
            "casa": {
                "status": "ran",
                "reason": None,
                "timings_seconds": {"runs": [1.0], "median": 1.0},
                "warmup_count": 0,
                "cache_role": "none",
                "evidence_summary": evidence_summary,
            },
            "stage_medians_ms": {"rust": {}, "casa": stage_medians_ms},
            "product_paths": {"casa_prefix": str(product_prefix)},
            "casa_tclean_calls": {"warmups": [], "measured": [call]},
            "casa_repeatability_comparison": {
                "status": "completed",
                "reason": None,
                "comparison_kind": "single_call_product_contract",
                "baseline_call": "measured-001",
                "compared_calls": ["measured-001"],
                "comparison_mode": "sampled",
                "source_regions": [],
                "tolerances": tolerances,
                "products": comparison["products"],
                "product_inventory": exact_inventory,
                "structured_difference_review": {
                    "label": "good",
                    "summary": "worst label across 1 comparison(s): good",
                },
                "comparisons": [comparison],
            },
        },
    }
    if warmup_wall_seconds is not None:
        _add_warmup_call(
            result,
            measured_call=call,
            partial=partial,
            wall_seconds=warmup_wall_seconds,
        )
    return result, {
        "partial": partial,
        "final": bundle.final_path,
        "stdout": stdout_path,
        "benchmark_log": benchmark_log,
        "product_data": product_data,
        "structure_workspace": structure_workspace,
    }


def _matched_metadata(shape: list[int]) -> dict[str, object]:
    operand = {
        "status": "complete",
        "shape": shape,
        "unit": "Jy/beam",
        "coordinates": {},
        "restoring_beam": {},
        "masks": [],
        "errors": [],
    }
    return {
        "status": "matched",
        "parity": True,
        "field_parity": {
            "shape": True,
            "unit": True,
            "coordinates": True,
            "restoring_beam": True,
            "masks": True,
        },
        "left": copy.deepcopy(operand),
        "right": copy.deepcopy(operand),
    }


def _add_warmup_call(
    result: dict,
    *,
    measured_call: dict,
    partial: pathlib.Path,
    wall_seconds: float,
) -> None:
    call_name = "warmup-001"
    protocol_root = partial / "protocol" / call_name
    product_prefix = partial / "casa" / call_name / "casa"
    product_path = pathlib.Path(f"{product_prefix}.image.tt0")
    protocol_root.mkdir(parents=True)
    product_path.mkdir(parents=True)
    (product_path / "table.dat").write_bytes(b"frozen warmup science product")
    (product_path / "table.lock").write_bytes(b"volatile lock")

    request_path = protocol_root / "request.json"
    result_path = protocol_root / "result.json"
    stdout_path = protocol_root / "stdout-stderr.log"
    casa_log_path = protocol_root / "casa-fixture.log"
    request = {
        "overrides": {"imagename": str(product_prefix)},
        "cache": {"role": "none"},
    }
    protocol_result = copy.deepcopy(measured_call["result"])
    effective_kwargs = {"imagename": str(product_prefix)}
    protocol_result.update(
        {
            "request_id": "fixture-warmup-001",
            "effective_kwargs": effective_kwargs,
            "effective_kwargs_sha256": canonical_sha256(effective_kwargs),
            "wall_seconds": wall_seconds,
            "stage_timings_seconds": {
                "protocol_preflight": 0.0,
                "tclean_task": wall_seconds,
                "product_inventory": 0.0,
                "cache_postcondition": 0.0,
                "protocol_total": wall_seconds,
            },
            "products": {
                "before": [],
                "after": inventory_product_siblings(product_prefix),
            },
        }
    )
    atomic_write_json(request_path, request)
    atomic_write_json(result_path, protocol_result)
    stdout_path.write_text("CASA warmup stdout\n", encoding="utf-8")
    casa_log_path.write_text("CASA warmup log\n", encoding="utf-8")
    warmup_call = {
        "name": call_name,
        "role": "none",
        "measured": False,
        "prefix": str(product_prefix),
        "request_path": str(request_path),
        "request_sha256": sha256_file(request_path),
        "result_path": str(result_path),
        "result_sha256": sha256_file(result_path),
        "stdout_stderr_path": str(stdout_path),
        "stdout_stderr_sha256": sha256_file(stdout_path),
        "exit_code": 0,
        "casa_log_paths": [str(casa_log_path)],
        "casa_log_identities": [
            {"path": str(casa_log_path), "sha256": sha256_file(casa_log_path)}
        ],
        "cache_receipt_sha256": None,
        "result": protocol_result,
    }
    result["run"]["warmups"] = 1
    result["results"]["casa"]["warmup_count"] = 1
    result["results"]["casa_tclean_calls"]["warmups"] = [warmup_call]


def _retarget_single_comparison(result: dict, *, call_name: str) -> None:
    """Rebind every raw artifact so only the measured-call topology is forged."""

    repeatability = result["results"]["casa_repeatability_comparison"]
    comparison = repeatability["comparisons"][0]
    calls = result["results"]["casa_tclean_calls"]
    target = next(
        record
        for record in [*calls["warmups"], *calls["measured"]]
        if record["name"] == call_name
    )
    prefix = target["prefix"]
    input_path = pathlib.Path(comparison["input"])
    output_path = pathlib.Path(comparison["output"])
    log_path = pathlib.Path(comparison["log"])
    request = json.loads(input_path.read_text(encoding="utf-8"))
    raw_output = json.loads(output_path.read_text(encoding="utf-8"))
    workspace = input_path.parent / f"casa-{call_name}-structure-workspace"
    request.update(
        {
            "left_prefix": prefix,
            "right_prefix": prefix,
            "left_label": f"CASA {call_name}",
            "right_label": f"CASA {call_name} product contract",
            "structure_workspace_dir": str(workspace),
        }
    )
    request_binding = image_compare.comparison_request_binding(request)
    request["request_binding"] = request_binding
    request["request_sha256"] = canonical_sha256(request_binding)
    raw_output.update(
        {
            "request_binding": copy.deepcopy(request_binding),
            "request_sha256": request["request_sha256"],
            "left_prefix": prefix,
            "right_prefix": prefix,
            "left_label": request["left_label"],
            "right_label": request["right_label"],
            "structure_workspace_dir": str(workspace),
        }
    )
    product = raw_output["products"][".image.tt0"]
    product["left_path"] = prefix + ".image.tt0"
    product["right_path"] = prefix + ".image.tt0"
    image_compare.validate_comparison_output(raw_output, request)
    atomic_write_json(input_path, request)
    atomic_write_json(output_path, raw_output)
    rebound = image_compare.apply_tolerance_contract(copy.deepcopy(raw_output), request)
    rebound.update(
        {
            "input": str(input_path),
            "input_sha256": sha256_file(input_path),
            "output": str(output_path),
            "output_sha256": sha256_file(output_path),
            "log": str(log_path),
            "log_sha256": sha256_file(log_path),
            "left_call": call_name,
            "right_call": call_name,
            "comparison_kind": "single_call_product_contract",
        }
    )
    repeatability["comparisons"] = [rebound]
    repeatability["products"] = rebound["products"]
    repeatability["product_inventory"] = rebound["product_inventory"]


def _exact_inventory(suffixes: list[str]) -> dict:
    return {
        "status": "matched",
        "required": True,
        "observed_match": True,
        "left_right_equal": True,
        "expected": suffixes,
        "left": suffixes,
        "right": suffixes,
        "left_missing": [],
        "left_extra": [],
        "right_missing": [],
        "right_extra": [],
    }


if __name__ == "__main__":
    unittest.main()
