# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for the recipe-backed CASA tclean workflow boundary."""

from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from perf_harness import casa_tclean, casa_tclean_workflow
from perf_harness.artifacts import prepare_atomic_directory_bundle
from perf_harness.errors import HarnessError
from perf_harness.evidence_storage import (
    requirement_for_workload,
    requirement_for_storage_label,
    validate_requirement_capacity,
    validate_requirement_paths,
)
from perf_harness.schema import RUN_RESULT_SCHEMA_VERSION, validate_run_result
from test_support import canonical_test_environment


class EvidenceStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        requirement = requirement_for_storage_label("GLENDENNING-vlass-archive")
        assert requirement is not None
        self.requirement = requirement
        self.root = pathlib.Path(self.requirement["required_root"])

    def test_exact_root_and_children_preserve_the_vlass_boundary(self) -> None:
        validate_requirement_paths(
            self.requirement,
            paths=[self.root, self.root / "artifacts", self.root / "cf-cache"],
        )
        with self.assertRaisesRegex(HarnessError, "must be under"):
            validate_requirement_paths(
                self.requirement,
                paths=[self.root.parent / "issue-447" / "artifacts"],
            )
        with self.assertRaisesRegex(HarnessError, "disposable"):
            validate_requirement_paths(
                self.requirement,
                paths=[self.root / "_tmp_safe_to_delete" / "artifacts"],
            )

    def test_minimum_free_space_is_an_inclusive_one_tib_floor(self) -> None:
        validate_requirement_capacity(self.requirement, available_bytes=1 << 40)
        with self.assertRaisesRegex(HarnessError, "stop threshold"):
            validate_requirement_capacity(
                self.requirement, available_bytes=(1 << 40) - 1
            )

    def test_requirement_record_cannot_rebind_the_canonical_root(self) -> None:
        modified = {**self.requirement, "required_root": "/tmp/vlass"}
        with self.assertRaisesRegex(HarnessError, "was modified"):
            validate_requirement_paths(modified, paths=[pathlib.Path("/tmp/vlass")])

    def test_unregistered_storage_label_has_no_specialized_policy(self) -> None:
        self.assertIsNone(requirement_for_storage_label("generic-benchmark"))

    def test_protected_dataset_cannot_drop_its_manifest_policy(self) -> None:
        with self.assertRaisesRegex(HarnessError, "requires storage policy"):
            requirement_for_workload(
                dataset_key="vlass-fragment-b80d5e87487a",
                storage_label="generic-benchmark",
            )

    def test_recipe_storage_preflight_includes_raw_and_resolved_dataset_path(
        self,
    ) -> None:
        dataset = pathlib.Path("/Volumes/GLENDENNING/outside/input.ms")
        roots = [
            self.root / "receipts" / "runs",
            self.root / "artifacts",
            self.root / "cf-cache",
        ]
        plan = {
            "dataset": {"path": str(dataset)},
            "command": {
                "kind": "casa_tclean_protocol",
                "evidence_storage": self.requirement,
                "casa": {
                    "mask_identity": {"path": str(self.root / "masks" / "clean.mask")}
                },
            },
        }
        calls: list[list[pathlib.Path]] = []

        def record_paths(requirement, *, paths):
            calls.append(list(paths))

        stat_result = mock.Mock(st_dev=7)
        statvfs_result = mock.Mock(f_bavail=1 << 20, f_frsize=1 << 20)
        with (
            mock.patch.object(
                casa_tclean_workflow, "validate_requirement_paths", record_paths
            ),
            mock.patch.object(casa_tclean_workflow, "validate_requirement_capacity"),
            mock.patch.object(pathlib.Path, "mkdir"),
            mock.patch.object(
                casa_tclean_workflow.os, "stat", return_value=stat_result
            ),
            mock.patch.object(
                casa_tclean_workflow.os,
                "statvfs",
                return_value=statvfs_result,
            ),
        ):
            casa_tclean_workflow.validate_storage_preconditions(
                plan,
                output_dir=roots[0],
                artifact_root=roots[1],
                cf_cache_root=roots[2],
            )

        self.assertEqual(dataset, calls[0][0])
        self.assertEqual(self.root / "masks" / "clean.mask", calls[0][1])
        self.assertEqual(dataset.resolve(), calls[1][0])
        self.assertEqual((self.root / "masks" / "clean.mask").resolve(), calls[1][1])


class CacheIdentityTests(unittest.TestCase):
    def test_vlass_full_cold_and_warm_manifests_share_exact_cf_plan(self) -> None:
        for stem in ("single-field", "all-fields"):
            manifests = []
            for suffix in (f"{stem}-cold", stem):
                path = (
                    casa_tclean_workflow.REPO_ROOT
                    / "tools"
                    / "perf"
                    / "imager"
                    / "workloads"
                    / f"vlass-fragment-{suffix}.json"
                )
                manifests.append(json.loads(path.read_text(encoding="utf-8")))
            commands = []
            for manifest in manifests:
                casa = manifest["casa"]
                recipe_path = casa_tclean_workflow.REPO_ROOT / casa["recipe_path"]
                commands.append(
                    casa_tclean_workflow.build_recipe_command_plan(
                        casa=casa,
                        recipe_path=recipe_path,
                        dataset=manifest["dataset"],
                        dataset_path=pathlib.Path("/Volumes/fixture/vlass.ms"),
                        imaging=manifest["imaging"],
                        run_support={
                            "targets": {
                                "rust": {
                                    "status": "unavailable",
                                    "missing_capabilities": ["fixture"],
                                }
                            }
                        },
                        casa_python=None,
                        dry_run=True,
                    )
                )
                role = manifest["run"]["evidence_role"]
                self.assertIn(role, manifest["review"]["required_evidence_roles"])
            self.assertEqual(
                commands[0]["casa"]["cache_plan"],
                commands[1]["casa"]["cache_plan"],
            )

    def test_geometry_cache_identity_excludes_revalidation_status_and_paths(
        self,
    ) -> None:
        dry = _geometry_evidence(
            source_path="/checkout-a/geometry.json",
            status="expected_only_dry_run",
            actual=None,
        )
        matched = _geometry_evidence(
            source_path="/checkout-b/geometry.json",
            status="matched",
            actual={
                "tree_sha256": "a" * 64,
                "file_count": 117,
                "size_bytes": 2_707_239_817,
            },
        )
        matched["dataset"]["expected"] = copy.deepcopy(dry["dataset"]["expected"])

        self.assertEqual(
            casa_tclean_workflow.cache_geometry_identity(dry),
            casa_tclean_workflow.cache_geometry_identity(matched),
        )
        identity = casa_tclean_workflow.cache_geometry_identity(dry)
        self.assertNotIn("source_path", identity)
        self.assertNotIn("status", identity["dataset"])
        self.assertNotIn("actual", identity["dataset"])

    def test_cf_identity_excludes_non_cf_and_path_dependent_controls(self) -> None:
        effective = {
            name: f"value-{name}"
            for name in casa_tclean_workflow.CF_CACHE_PARAMETER_FIELDS
        }
        effective.update(
            {
                "facets": 1,
                "imsize": [1024, 1024],
                "nchan": -1,
                "wprojplanes": 32,
                "aterm": True,
                "psterm": False,
                "wbawp": True,
                "conjbeams": True,
                "usepointing": True,
                "computepastep": 360.0,
                "rotatepastep": 360.0,
                "pointingoffsetsigdev": [0.0],
                "pblimit": 0.0001,
                "restfreq": [],
                "vptable": "",
                "mask": "/host-a/mask.image",
                "niter": 2000,
                "deconvolver": "mtmfs",
                "imagename": "/host-a/output",
            }
        )
        moved = copy.deepcopy(effective)
        moved.update(
            {
                "mask": "/host-b/same-mask.image",
                "niter": 0,
                "deconvolver": "hogbom",
                "imagename": "/host-b/output",
            }
        )

        first = casa_tclean_workflow.cf_cache_parameter_identity(effective)
        second = casa_tclean_workflow.cf_cache_parameter_identity(moved)

        self.assertEqual(first, second)
        self.assertNotIn("mask", first)
        self.assertNotIn("niter", first)
        moved["imsize"] = [12150, 12150]
        self.assertNotEqual(
            first, casa_tclean_workflow.cf_cache_parameter_identity(moved)
        )

    def test_nonempty_vptable_requires_content_addressed_identity(self) -> None:
        effective = {
            name: f"value-{name}"
            for name in casa_tclean_workflow.CF_CACHE_PARAMETER_FIELDS
        }
        effective["vptable"] = "/host/custom.vp"
        with self.assertRaisesRegex(HarnessError, "content-addressed CF identity"):
            casa_tclean_workflow.cf_cache_parameter_identity(effective)


class ProtocolEvidenceSummaryTests(unittest.TestCase):
    def test_measured_stage_resource_summary_uses_medians_and_peak_maximum(
        self,
    ) -> None:
        calls = [
            _protocol_record("measured-001", tclean_seconds=1.0, peak_rss_bytes=100),
            _protocol_record("measured-002", tclean_seconds=3.0, peak_rss_bytes=300),
            _protocol_record("measured-003", tclean_seconds=2.0, peak_rss_bytes=200),
        ]

        summary = casa_tclean.summarize_completed_results(calls)

        self.assertEqual(3, summary["call_count"])
        self.assertEqual(2.0, summary["stage_seconds"]["median"]["tclean_task"])
        self.assertEqual(300, summary["resources"]["peak_rss_bytes_max"])
        self.assertEqual(2.0, summary["resources"]["user_cpu_seconds_median"])
        medians_ms = {
            name: seconds * 1000.0
            for name, seconds in summary["stage_seconds"]["median"].items()
        }
        breakdown = casa_tclean_workflow.casa_protocol_stage_breakdown(
            medians_ms, rust_reason="unavailable"
        )
        self.assertEqual("reported", breakdown["casa"]["status"])
        self.assertIn(
            "does not claim internal attribution",
            breakdown["casa"]["categories"]["tclean_task"]["description"],
        )


class RecipeBundleLifecycleTests(unittest.TestCase):
    def test_repeatability_owns_a_deterministic_structure_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            comparison_root = pathlib.Path(temporary) / "run.partial" / "comparisons"
            captured = {}

            def comparison_runner(**kwargs):
                captured.update(kwargs["request"])
                return {
                    "status": "completed",
                    "products": {},
                    "product_inventory": {},
                }

            plan = {
                "artifacts": {"comparison_root": str(comparison_root)},
                "command": {"casa": {"python": "/casa/python"}},
                "comparison": {
                    "products": [".image.tt0"],
                    "max_elements_per_product": 1024,
                    "mode": "sampled",
                    "full_chunk_elements": 1024,
                    "require_exact_product_inventory": True,
                    "require_metadata_parity": True,
                    "source_regions": [],
                    "tolerances": None,
                },
            }
            calls = [
                {
                    "name": "measured-001",
                    "prefix": str(
                        comparison_root.parent / "casa" / "measured-001" / "casa"
                    ),
                }
            ]

            casa_tclean_workflow.compare_casa_repeatability(
                plan,
                calls,
                pathlib.Path(temporary) / "benchmark.log",
                comparison_runner=comparison_runner,
            )

            self.assertEqual(
                str(comparison_root / "casa-measured-001-structure-workspace"),
                captured["structure_workspace_dir"],
            )
            self.assertTrue(
                pathlib.Path(captured["structure_workspace_dir"]).is_absolute()
            )

    def test_complete_comparison_promotes_bundle_without_falsifying_execution_paths(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            final = pathlib.Path(temporary) / "artifacts" / "run-1"
            bundle = prepare_atomic_directory_bundle(final)
            final = bundle.final_path
            request = bundle.partial_path / "protocol" / "measured-001" / "request.json"
            request.parent.mkdir(parents=True)
            request.write_text('{"imagename":"run-1.partial/casa"}\n')
            request_hash = hashlib.sha256(request.read_bytes()).hexdigest()
            result = _bundle_result(bundle, comparison_status="completed")
            call = result["results"]["casa_tclean_calls"]["measured"][0]
            call["request_path"] = str(request)
            call["request_sha256"] = request_hash

            with mock.patch.object(
                casa_tclean_workflow,
                "validate_recipe_evidence_bundle",
                return_value=_bundle_integrity_receipt(),
            ):
                published = casa_tclean_workflow.finalize_bundle_result(result)

            self.assertEqual("complete", published["artifacts"]["bundle"]["state"])
            self.assertTrue(final.is_dir())
            self.assertFalse(bundle.partial_path.exists())
            published_call = published["results"]["casa_tclean_calls"]["measured"][0]
            self.assertEqual(str(request), published_call["request_path"])
            self.assertEqual(request_hash, published_call["request_sha256"])
            self.assertEqual(
                str(final / "protocol" / "measured-001" / "request.json"),
                published_call["retained_request_path"],
            )
            comparison = published["results"]["casa_repeatability_comparison"]
            published_product = comparison["products"][".image.tt0"]
            self.assertEqual(
                str(final / "casa" / "measured-001" / "casa.image.tt0"),
                published_product["retained_left_path"],
            )
            self.assertEqual(
                str(final / "comparisons" / "panels" / "image.review.png"),
                published_product["review_panel"]["retained_path"],
            )
            self.assertEqual(
                str(final / "comparisons" / "panels" / "image.zoom.review.png"),
                published_product["review_panel"]["zoom_panel"]["retained_path"],
            )
            constituent = comparison["comparisons"][0]
            self.assertEqual(
                str(final / "casa" / "measured-001" / "casa"),
                constituent["retained_left_prefix"],
            )
            self.assertEqual(
                str(final / "casa" / "measured-001" / "casa.psf.tt0"),
                constituent["beam_info"]["retained_psf_path"],
            )
            retained_request = final / "protocol" / "measured-001" / "request.json"
            self.assertEqual(
                request_hash, hashlib.sha256(retained_request.read_bytes()).hexdigest()
            )
            receipt = json.loads((final / "receipt.json").read_text())
            self.assertEqual("complete", receipt["artifacts"]["bundle"]["state"])

    def test_unavailable_or_out_of_tolerance_comparison_retains_partial_bundle(
        self,
    ) -> None:
        for status in ("unavailable", "out_of_tolerance"):
            with (
                self.subTest(status=status),
                tempfile.TemporaryDirectory() as temporary,
            ):
                final = pathlib.Path(temporary) / "run"
                bundle = prepare_atomic_directory_bundle(final)
                result = _bundle_result(bundle, comparison_status=status)
                result["results"]["casa_repeatability_comparison"]["status"] = status

                retained = casa_tclean_workflow.finalize_bundle_result(result)

                self.assertEqual("partial", retained["artifacts"]["bundle"]["state"])
                self.assertTrue(bundle.partial_path.is_dir())
                self.assertFalse(final.exists())
                self.assertTrue((bundle.partial_path / "receipt.json").is_file())

    def test_promotion_failure_is_typed_and_preserves_partial(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            final = pathlib.Path(temporary) / "run"
            bundle = prepare_atomic_directory_bundle(final)
            result = _bundle_result(bundle, comparison_status="completed")
            with (
                mock.patch.object(
                    casa_tclean_workflow,
                    "validate_recipe_evidence_bundle",
                    return_value=_bundle_integrity_receipt(),
                ),
                mock.patch.object(
                    casa_tclean_workflow,
                    "promote_atomic_directory_bundle",
                    side_effect=OSError("synthetic promotion failure"),
                ),
            ):
                failed = casa_tclean_workflow.finalize_bundle_result(result)

            self.assertEqual("failed_execution", failed["status"])
            self.assertEqual("artifact_promotion", failed["results"]["failure"]["kind"])
            self.assertTrue(bundle.partial_path.is_dir())


class WarmCacheContractTests(unittest.TestCase):
    def test_missing_warm_receipt_never_plans_a_cold_bootstrap(self) -> None:
        request = {
            "cache": {"expected_stable_tree_sha256": "<resolved-from-cold-receipt>"}
        }

        planned = casa_tclean_workflow.planned_casa_request(
            request, cache_receipt=pathlib.Path("/missing/receipt.json")
        )

        self.assertEqual("requires_cache_receipt", planned["status"])
        self.assertNotIn("cold_bootstrap_plan", planned)

    def test_warm_run_fails_before_any_call_when_cold_evidence_is_missing(
        self,
    ) -> None:
        plan = {
            "run": {"cf_cache_role": "warm", "repeats": 1, "warmups": 1},
            "command": {
                "casa": {
                    "cache_path": "/missing/cache",
                    "cache_receipt_path": "/missing/receipt.json",
                }
            },
        }
        services = casa_tclean_workflow.ExecutionServices(
            utc_now=lambda: "2026-07-20T00:00:00Z",
            empty_results=lambda **kwargs: {"casa": kwargs},
            empty_stage_breakdown=lambda reason: {"reason": reason},
            build_benchmark_feature_summary=lambda *args: {},
            comparison_evidence_status=lambda *args, **kwargs: ("completed", None),
            human_review_gate=lambda *args: {},
        )
        with (
            tempfile.TemporaryDirectory() as temporary,
            mock.patch.object(
                casa_tclean_workflow, "execute_casa_recipe_call"
            ) as execute,
        ):
            result = casa_tclean_workflow.run_recipe_plan(
                plan,
                pathlib.Path(temporary) / "run.log",
                services=services,
            )

        self.assertEqual("failed_execution", result["status"])
        self.assertIn(
            "independently completed cold cache", result["results"]["failure"]["reason"]
        )
        self.assertEqual(
            {"warmups": [], "measured": []},
            result["results"]["casa_tclean_calls"],
        )
        execute.assert_not_called()


class PublicationRecoveryWorkflowTests(unittest.TestCase):
    def test_warmup_recovery_stops_before_later_or_measured_calls(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            plan = _recovery_plan(pathlib.Path(temporary), warmups=2)
            recovered = _recovery_record("warmup-001", measured=False)
            with mock.patch.object(
                casa_tclean_workflow,
                "execute_casa_recipe_call",
                return_value=recovered,
            ) as execute:
                result = casa_tclean_workflow.run_recipe_plan(
                    plan,
                    pathlib.Path(temporary) / "benchmark-summary.log",
                    services=_recovery_services(),
                )

            self.assertEqual("recovered_publication", result["status"])
            self.assertEqual([], result["results"]["casa"]["timings_seconds"]["runs"])
            self.assertIsNone(result["results"]["casa"]["timings_seconds"]["median"])
            self.assertEqual(
                "warmup", result["results"]["publication_recovery"]["call_phase"]
            )
            self.assertFalse(
                result["results"]["publication_recovery"]["timing_accepted"]
            )
            self.assertEqual(1, execute.call_count)
            validate_run_result(result)

    def test_measured_recovery_retains_partial_receipt_and_published_cache(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            plan = _recovery_plan(root, warmups=1)
            cache_path = root / "external-cache" / "cache"
            receipt_path = root / "external-cache" / "receipt.json"
            cache_path.mkdir(parents=True)
            (cache_path / "table.dat").write_bytes(b"published cache")
            receipt_path.write_text('{"published":true}\n', encoding="utf-8")
            warmup = _completed_record("warmup-001", measured=False)
            recovered = _recovery_record(
                "measured-001",
                measured=True,
                cache_path=cache_path,
                receipt_path=receipt_path,
            )
            with mock.patch.object(
                casa_tclean_workflow,
                "execute_casa_recipe_call",
                side_effect=[warmup, recovered],
            ) as execute:
                result = casa_tclean_workflow.run_recipe_plan(
                    plan,
                    pathlib.Path(plan["artifacts"]["bundle"]["partial_root"])
                    / "benchmark-summary.log",
                    services=_recovery_services(),
                )

            result["logs"] = {
                "benchmark_log": str(
                    pathlib.Path(plan["artifacts"]["bundle"]["partial_root"])
                    / "benchmark-summary.log"
                )
            }
            retained = casa_tclean_workflow.finalize_bundle_result(result)

            partial = pathlib.Path(plan["artifacts"]["bundle"]["partial_root"])
            final = pathlib.Path(plan["artifacts"]["bundle"]["final_root"])
            self.assertEqual("recovered_publication", retained["status"])
            self.assertEqual("partial", retained["artifacts"]["bundle"]["state"])
            self.assertEqual(
                "measured", retained["results"]["publication_recovery"]["call_phase"]
            )
            self.assertEqual(2, execute.call_count)
            self.assertTrue(partial.is_dir())
            self.assertFalse(final.exists())
            self.assertTrue((partial / "receipt.json").is_file())
            self.assertTrue(cache_path.is_dir())
            self.assertTrue(receipt_path.is_file())
            validate_run_result(retained)


class CompletedOuterPublicationRecoveryTests(unittest.TestCase):
    def test_recovery_rebinds_existing_artifacts_without_reinvocation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            partial = root / "run.partial"
            partial.mkdir()
            plan = {
                "run_id": "run",
                "run": {"cf_cache_role": "cold", "warmups": 0, "repeats": 1},
                "command": {"kind": "casa_tclean_protocol"},
                "artifacts": {
                    "bundle": {
                        "partial_root": str(partial),
                        "final_root": str(root / "run"),
                    }
                },
            }
            failed = {
                "status": "failed_execution",
                "started_at": "2026-07-20T00:00:00Z",
                "results": {"failure": {"kind": "harness_internal"}},
            }
            measured = {"name": "measured-001", "measured": True}
            repeatability = {"status": "completed", "products": {"image": {}}}
            completed = {"status": "completed", "results": {}}
            services = mock.Mock()
            services.comparison_evidence_status.return_value = ("completed", None)
            log_path = partial / "benchmark-summary.log"

            with (
                mock.patch.object(casa_tclean_workflow, "validate_run_result"),
                mock.patch.object(
                    casa_tclean_workflow,
                    "_recipe_plan_from_failed_result",
                    return_value=plan,
                ),
                mock.patch.object(
                    casa_tclean_workflow,
                    "recover_casa_recipe_call",
                    return_value=measured,
                ) as recover_call,
                mock.patch.object(
                    casa_tclean_workflow,
                    "recover_casa_repeatability",
                    return_value=repeatability,
                ) as recover_comparison,
                mock.patch.object(
                    casa_tclean_workflow,
                    "completed_recipe_run_result",
                    return_value=completed,
                ),
                mock.patch.object(
                    casa_tclean_workflow, "write_recipe_summary_log"
                ) as write_log,
                mock.patch.object(
                    casa_tclean_workflow, "execute_casa_recipe_call"
                ) as execute_casa,
                mock.patch.object(
                    casa_tclean_workflow, "compare_image_products"
                ) as execute_comparator,
            ):
                result = casa_tclean_workflow.recover_completed_recipe_run(
                    failed,
                    log_path,
                    services=services,
                )

            self.assertIs(completed, result)
            recover_call.assert_called_once_with(
                plan,
                call_name="measured-001",
                call_role="cold",
                measured=True,
            )
            recover_comparison.assert_called_once_with(plan, [measured])
            write_log.assert_called_once_with(log_path, [], [measured])
            execute_casa.assert_not_called()
            execute_comparator.assert_not_called()
            self.assertIn("tclean_reinvoked=false", log_path.read_text())


def _recovery_services() -> casa_tclean_workflow.ExecutionServices:
    return casa_tclean_workflow.ExecutionServices(
        utc_now=lambda: "2026-07-20T00:00:01Z",
        empty_results=lambda *, casa_status, reason: {
            "rust": {
                "status": "not_run",
                "reason": reason,
                "timings_seconds": {"runs": [], "median": None},
            },
            "casa": {
                "status": casa_status,
                "reason": reason,
                "timings_seconds": {"runs": [], "median": None},
            },
            "stage_medians_ms": {"rust": {}, "casa": {}},
            "stage_breakdown": {
                "schema_version": 1,
                "units": "milliseconds",
                "instrumentation_scope": "test",
                "rust": {"status": "skipped", "categories": {}},
                "casa": {"status": "skipped", "categories": {}},
            },
            "product_paths": {},
            "product_comparison": {
                "status": "skipped",
                "reason": reason,
                "products": {},
            },
        },
        empty_stage_breakdown=lambda reason: {"reason": reason},
        build_benchmark_feature_summary=lambda *args: {},
        comparison_evidence_status=lambda *args, **kwargs: ("completed", None),
        human_review_gate=lambda *args: {},
    )


def _recovery_plan(root: pathlib.Path, *, warmups: int) -> dict[str, object]:
    bundle = prepare_atomic_directory_bundle(root / "artifacts" / "run-recovery")
    return {
        "run_id": "run-recovery",
        "created_at": "2026-07-20T00:00:00Z",
        "environment": canonical_test_environment(),
        "run": {"cf_cache_role": "cold", "repeats": 1, "warmups": warmups},
        "artifacts": {
            "bundle": {
                "state": "partial",
                "partial_root": str(bundle.partial_path),
                "final_root": str(bundle.final_path),
                "retained_root": str(bundle.partial_path),
                "execution_to_retained": {
                    "from": str(bundle.partial_path),
                    "to": str(bundle.partial_path),
                },
            }
        },
    }


def _completed_record(name: str, *, measured: bool) -> dict[str, object]:
    return {
        "name": name,
        "role": "cold",
        "measured": measured,
        "result": {"status": "completed", "wall_seconds": 1.0},
    }


def _recovery_record(
    name: str,
    *,
    measured: bool,
    cache_path: pathlib.Path | None = None,
    receipt_path: pathlib.Path | None = None,
) -> dict[str, object]:
    return {
        "name": name,
        "role": "cold",
        "measured": measured,
        "cache_receipt_sha256": "a" * 64,
        "result": {
            "status": "recovered_publication",
            "wall_seconds": 0.0,
            "casa": {
                "publication_recovery": {
                    "status": "completed",
                    "tclean_reinvoked": False,
                    "exact_request_replay_required": True,
                }
            },
            "cache": {
                "path": str(cache_path or pathlib.Path("/cache")),
                "receipt_path": str(receipt_path or pathlib.Path("/receipt.json")),
                "after": {
                    "role": "cold",
                    "inventory": {"stable_tree_sha256": "b" * 64},
                },
            },
        },
    }


def _bundle_result(bundle, *, comparison_status: str) -> dict[str, object]:
    partial = bundle.partial_path
    final = bundle.final_path
    left_prefix = str(partial / "casa" / "measured-001" / "casa")
    right_prefix = str(partial / "casa" / "measured-002" / "casa")
    panel_dir = str(partial / "comparisons" / "panels")
    request_binding = {
        "schema_version": 3,
        "mode": "full",
        "left_prefix": left_prefix,
        "right_prefix": right_prefix,
        "left_label": "CASA measured 1",
        "right_label": "CASA measured 2",
        "products": [".image.tt0"],
        "max_elements_per_product": 4,
        "full_chunk_elements": 3,
        "require_exact_product_inventory": False,
        "require_metadata_parity": False,
        "legacy_operand_aliases": True,
        "source_regions": [],
        "tolerances": None,
        "panel_dir": panel_dir,
    }
    comparison = {
        "schema_version": 3,
        "request_binding": request_binding,
        "request_sha256": casa_tclean.canonical_sha256(request_binding),
        "status": comparison_status,
        "input": str(partial / "comparisons" / "request.json"),
        "log": str(partial / "comparisons" / "comparison.log"),
        "panel_dir": panel_dir,
        "left_prefix": left_prefix,
        "right_prefix": right_prefix,
        "beam_info": {
            "status": "estimated",
            "psf_path": str(partial / "casa" / "measured-001" / "casa.psf.tt0"),
        },
        "products": _comparison_products(partial),
    }
    environment = canonical_test_environment()
    environment["migration"] = {
        "source_schema_version": 2,
        "method": "synthetic historical comparison fixture",
    }
    return {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": "workload_run",
        "status": "completed",
        "run_id": final.name,
        "created_at": "2026-07-20T00:00:00Z",
        "started_at": "2026-07-20T00:00:01Z",
        "completed_at": "2026-07-20T00:00:02Z",
        "exit_code": 0,
        "environment": environment,
        "artifacts": {
            "products_root": str(partial),
            "comparison_root": str(partial / "comparisons"),
            "protocol_root": str(partial / "protocol"),
            "bundle": {
                "state": "partial",
                "partial_root": str(partial),
                "final_root": str(final),
                "retained_root": str(partial),
                "execution_to_retained": {
                    "from": str(partial),
                    "to": str(partial),
                },
            },
        },
        "products": {
            "root": str(partial),
            "casa_prefix": str(partial / "casa" / "measured-001" / "casa"),
        },
        "logs": {"benchmark_log": str(partial / "benchmark-summary.log")},
        "results": {
            "product_paths": {
                "casa_prefix": str(partial / "casa" / "measured-001" / "casa")
            },
            "casa_repeatability_comparison": {
                "status": comparison_status,
                "products": _comparison_products(partial),
                "comparisons": [comparison],
            },
            "casa_tclean_calls": {
                "measured": [
                    {
                        "prefix": str(partial / "casa" / "measured-001" / "casa"),
                        "request_path": str(
                            partial / "protocol" / "measured-001" / "request.json"
                        ),
                        "result_path": str(
                            partial / "protocol" / "measured-001" / "result.json"
                        ),
                        "stdout_stderr_path": str(
                            partial / "protocol" / "measured-001" / "stdout-stderr.log"
                        ),
                        "casa_log_paths": [],
                    }
                ]
            },
        },
    }


def _bundle_integrity_receipt() -> dict[str, object]:
    return {
        "status": "passed",
        "validator_version": 1,
        "volatile_tree_exclusions": ["table.lock"],
        "call_count": 1,
        "product_tree_count": 1,
        "comparison_count": 1,
        "written_panel_count": 2,
        "cache_tree_count": 0,
    }


def _comparison_products(partial: pathlib.Path) -> dict[str, object]:
    panels = partial / "comparisons" / "panels"
    return {
        ".image.tt0": {
            "status": "compared",
            "left_path": str(partial / "casa" / "measured-001" / "casa.image.tt0"),
            "right_path": str(partial / "casa" / "measured-002" / "casa.image.tt0"),
            "rust_path": str(partial / "casa" / "measured-001" / "casa.image.tt0"),
            "casa_path": str(partial / "casa" / "measured-002" / "casa.image.tt0"),
            "review_panel": {
                "status": "written",
                "path": str(panels / "image.review.png"),
                "zoom_panel": {
                    "status": "written",
                    "path": str(panels / "image.zoom.review.png"),
                },
            },
        }
    }


def _protocol_record(
    name: str, *, tclean_seconds: float, peak_rss_bytes: int
) -> dict[str, object]:
    effective_kwargs = {"imagename": f"/tmp/{name}/casa"}
    before = {
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
    delta = {
        "user_cpu_seconds": tclean_seconds,
        "system_cpu_seconds": tclean_seconds / 2.0,
        "peak_rss_bytes": peak_rss_bytes,
        "minor_page_faults": int(tclean_seconds),
        "major_page_faults": 0,
        "block_input_operations": int(tclean_seconds),
        "block_output_operations": int(tclean_seconds),
        "disk_read_bytes": int(tclean_seconds * 100),
        "disk_write_bytes": int(tclean_seconds * 200),
        "voluntary_context_switches": int(tclean_seconds),
        "involuntary_context_switches": 0,
    }
    after = {
        name: (peak_rss_bytes if name == "peak_rss_bytes" else before[name] + value)
        for name, value in delta.items()
    }
    result = {
        "schema_version": casa_tclean.RESULT_SCHEMA_VERSION,
        "kind": casa_tclean.RESULT_KIND,
        "status": "completed",
        "request_id": name,
        "action": "run",
        "casa": {},
        "recipe": {},
        "compatibility_normalizations": [],
        "version_defaults": {},
        "reproducibility_overrides": {},
        "effective_kwargs": effective_kwargs,
        "effective_kwargs_sha256": casa_tclean.canonical_sha256(effective_kwargs),
        "cache": {
            "role": "none",
            "before": {"role": "none"},
            "after": {"role": "none"},
        },
        "mask_identity": None,
        "wall_seconds": tclean_seconds,
        "stage_timings_seconds": {
            "protocol_preflight": 0.1,
            "tclean_task": tclean_seconds,
            "product_inventory": 0.2,
            "cache_postcondition": 0.1,
            "protocol_total": tclean_seconds + 0.4,
        },
        "resources": {
            "schema_version": 1,
            "scope": "casa_python_process_during_protocol_execution",
            "peak_rss_source": "getrusage_rusage_self",
            "disk_io_source": "linux_proc_self_io",
            "before": before,
            "after": after,
            "delta": delta,
        },
        "products": {
            "before": [],
            "after": [
                {
                    "path": f"/tmp/{name}/casa.image.tt0",
                    "suffix": ".image.tt0",
                    "inventory": {"logical_bytes": 1000},
                }
            ],
        },
        "tclean_return": {"type": None, "present": False},
    }
    return {"name": name, "result": result}


def _geometry_evidence(
    *, source_path: str, status: str, actual: dict[str, object] | None
) -> dict[str, object]:
    expected = {
        "tree_sha256": "b" * 64,
        "file_count": 117,
        "size_bytes": 2_707_239_817,
    }
    return {
        "source_path": source_path,
        "source_sha256": "c" * 64,
        "dataset": {"status": status, "expected": expected, "actual": actual},
        "selection": {
            "field": "1525",
            "spw": "2~17",
            "spw_ids": list(range(2, 18)),
            "spectral_windows": [{"id": 2, "first_hz": 1.965e9}],
        },
        "geometry": {"main_row_count": 655_200},
        "source_receipts": {"dataset_receipt_sha256": "d" * 64},
    }


if __name__ == "__main__":
    unittest.main()
