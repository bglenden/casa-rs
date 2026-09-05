#!/usr/bin/env python3
"""Focused tests for the Wave 1 imager workload harness helpers."""

from __future__ import annotations

import copy
import hashlib
import json
import unittest
from unittest import mock
import os
from pathlib import Path
import tempfile
import sys
import subprocess
import types

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_workload
from perf_harness import casa_tclean
from perf_harness.image_compare import CASA_IMAGE_COMPARATOR
from test_support import canonical_test_environment, canonical_workload_result


IMAGE_COMPARATOR_SOURCE = CASA_IMAGE_COMPARATOR.read_text(encoding="utf-8")


class CliExitSemanticsTests(unittest.TestCase):
    def test_comparison_failure_writes_receipt_before_nonzero_exit(self) -> None:
        self._assert_main_status(
            "failed_comparison",
            expected_exit=1,
            failure={"kind": "comparison", "reason": "comparison unavailable"},
        )

    def test_tolerance_failure_writes_receipt_before_nonzero_exit(self) -> None:
        self._assert_main_status(
            "out_of_tolerance",
            expected_exit=1,
            failure={
                "kind": "comparison_tolerance",
                "reason": "frozen tolerance exceeded",
            },
        )

    def test_completed_receipt_and_process_succeed(self) -> None:
        self._assert_main_status("completed", expected_exit=0)

    def test_recovered_publication_receipt_and_process_succeed_without_timing(
        self,
    ) -> None:
        self._assert_main_status(
            "recovered_publication",
            expected_exit=0,
            results={
                "casa": {"timings_seconds": {"runs": [], "median": None}},
                "publication_recovery": {
                    "kind": "cold_cf_cache_publication",
                    "status": "completed",
                    "protocol_status": "recovered_publication",
                    "benchmark_eligible": False,
                    "timing_accepted": False,
                    "tclean_reinvoked": False,
                },
            },
        )

    def _assert_main_status(
        self,
        status: str,
        *,
        expected_exit: int,
        failure: dict[str, str] | None = None,
        results: dict[str, object] | None = None,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "receipts"
            args = mock.Mock(
                workload="fixture",
                set_imaging=[],
                repeats=None,
                run_label=None,
                storage_label=None,
                stream_log=None,
                dry_run=False,
                output_dir=output,
                artifact_root=root / "artifacts",
                cf_cache_root=root / "cf-cache",
            )
            parser = mock.Mock()
            parser.parse_args.return_value = args
            plan = {
                "run_id": "cli-test",
                "command": {"kind": "legacy_benchmark_script"},
            }
            result = canonical_workload_result(status=status, extra_results=results)
            result_results = result["results"]
            if failure is not None:
                result_results["failure"] = failure
            result["run_id"] = "cli-test"
            result["artifacts"] = {"checked_in_path": "fixture-result.json"}

            def fake_run_plan(_plan, log_path):
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text("fixture benchmark\n", encoding="utf-8")
                return result

            patches = (
                mock.patch.object(
                    run_workload, "build_arg_parser", return_value=parser
                ),
                mock.patch.object(
                    run_workload,
                    "resolve_workload",
                    return_value=root / "fixture.json",
                ),
                mock.patch.object(run_workload, "load_manifest", return_value={}),
                mock.patch.object(run_workload, "apply_imaging_overrides"),
                mock.patch.object(run_workload, "validate_workload_manifest"),
                mock.patch.object(run_workload, "build_plan", return_value=plan),
                mock.patch.object(run_workload, "attach_output_paths"),
                mock.patch.object(run_workload, "run_plan", side_effect=fake_run_plan),
                mock.patch.object(run_workload.perf_paths, "mark_safe_to_delete"),
                mock.patch("builtins.print"),
            )
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patches[7],
                patches[8],
                patches[9],
            ):
                if expected_exit == 0:
                    run_workload.main()
                else:
                    with self.assertRaises(SystemExit) as raised:
                        run_workload.main()
                    self.assertEqual(expected_exit, raised.exception.code)

            receipt_path = output / "cli-test.json"
            self.assertTrue(receipt_path.is_file())
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            self.assertEqual(status, receipt["status"])
            self.assertEqual(expected_exit, receipt["exit_code"])


class FrozenCasaRecipeExecutionTests(unittest.TestCase):
    def test_skip_casa_without_frozen_prefix_fails_before_any_process(self) -> None:
        plan = {
            "command": {"kind": "casa_tclean_protocol"},
            "run": {"skip_casa": "1", "reuse_casa_prefix": None},
        }
        with mock.patch.object(run_workload, "run_benchmark_command") as execute:
            with self.assertRaisesRegex(
                run_workload.HarnessError, "requires an exact.*reuse_casa_prefix"
            ):
                run_workload.run_casa_recipe_plan(plan, Path("unused.log"))
        execute.assert_not_called()

    def test_reuse_runs_rust_benchmark_without_casa_and_compares_products(self) -> None:
        frozen = "/frozen/casa/reference"
        rust = "/shared/aw/rust-dirty"
        plan = {
            "command": {
                "kind": "casa_tclean_protocol",
                "argv": [str(run_workload.BENCH_SCRIPT), "/data/vlass.ms"],
                "env": {
                    "IMAGER_BENCH_SKIP_CASA": "1",
                    "IMAGER_BENCH_REUSE_CASA_PREFIX": frozen,
                },
            },
            "run": {
                "skip_casa": "1",
                "reuse_casa_prefix": frozen,
                "stream_log": False,
            },
            "products": {"rust_prefix": rust, "casa_prefix": frozen},
            "comparison": {"tolerances": None},
        }
        output = "\n".join(
            [
                "Rust release CLI timings (seconds):",
                "  run=1 real=2.5",
                "  median=2.5",
                f"rust_prefix={rust}",
                f"casa_prefix={frozen}",
            ]
        )
        compared = {
            "status": "completed",
            "products": {".image": {"status": "compared"}},
        }
        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.object(
                run_workload,
                "run_benchmark_command",
                return_value=subprocess.CompletedProcess([], 0, output),
            ) as execute,
            mock.patch.object(
                run_workload, "compare_products", return_value=compared
            ) as compare,
            mock.patch.object(
                run_workload, "build_benchmark_feature_summary", return_value={}
            ),
            mock.patch.object(run_workload, "attach_stage_breakdown"),
            mock.patch.object(
                run_workload,
                "comparison_evidence_status",
                return_value=("completed", None),
            ),
            mock.patch.object(run_workload, "human_review_gate", return_value={}),
            mock.patch.object(
                run_workload.casa_tclean_workflow,
                "validate_reused_casa_prefix",
            ) as validate_reuse,
            mock.patch.object(
                run_workload.casa_tclean_workflow, "run_recipe_plan"
            ) as casa_protocol,
        ):
            result = run_workload.run_casa_recipe_plan(
                plan, Path(directory) / "benchmark.log"
            )

        execute.assert_called_once()
        validate_reuse.assert_called_once_with(plan, frozen)
        self.assertEqual(str(run_workload.BENCH_SCRIPT), execute.call_args.args[0][0])
        self.assertEqual("1", execute.call_args.kwargs["env"]["IMAGER_BENCH_SKIP_CASA"])
        self.assertEqual(
            frozen,
            execute.call_args.kwargs["env"]["IMAGER_BENCH_REUSE_CASA_PREFIX"],
        )
        casa_protocol.assert_not_called()
        compare.assert_called_once()
        self.assertEqual(frozen, compare.call_args.args[0]["products"]["casa_prefix"])
        self.assertEqual("ran", result["results"]["rust"]["status"])
        self.assertEqual("reused", result["results"]["casa"]["status"])
        self.assertIs(compared, result["results"]["product_comparison"])

    def test_reuse_accepts_matching_protocol_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            plan, prefix = self._reused_bundle(Path(directory))
            run_workload.casa_tclean_workflow.validate_reused_casa_prefix(plan, prefix)

    def test_reuse_missing_result_sidecar_fails_before_benchmark(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            plan, prefix = self._reused_bundle(Path(directory))
            (prefix.parent.parent.parent / "protocol" / "measured-001" / "result.json").unlink()
            with mock.patch.object(run_workload, "run_benchmark_command") as execute:
                with self.assertRaisesRegex(run_workload.HarnessError, "result sidecar is missing"):
                    run_workload.run_casa_recipe_plan(plan, Path(directory) / "run.log")
            execute.assert_not_called()

    def test_reuse_tampered_result_fails_before_benchmark(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            plan, prefix = self._reused_bundle(Path(directory))
            result_path = prefix.parent.parent.parent / "protocol" / "measured-001" / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))
            result["effective_kwargs"]["conjbeams"] = False
            result_path.write_text(json.dumps(result), encoding="utf-8")
            with mock.patch.object(run_workload, "run_benchmark_command") as execute:
                with self.assertRaisesRegex(run_workload.HarnessError, "effective_kwargs_sha256"):
                    run_workload.run_casa_recipe_plan(plan, Path(directory) / "run.log")
            execute.assert_not_called()

    def test_reuse_tampered_recipe_identity_fails_before_benchmark(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            plan, prefix = self._reused_bundle(Path(directory))
            result_path = (
                prefix.parent.parent.parent
                / "protocol"
                / "measured-001"
                / "result.json"
            )
            result = json.loads(result_path.read_text(encoding="utf-8"))
            result["recipe"]["sha256"] = "0" * 64
            result_path.write_text(json.dumps(result), encoding="utf-8")
            with mock.patch.object(run_workload, "run_benchmark_command") as execute:
                with self.assertRaisesRegex(
                    run_workload.HarnessError, "result.recipe does not match"
                ):
                    run_workload.run_casa_recipe_plan(plan, Path(directory) / "run.log")
            execute.assert_not_called()

    def test_reuse_tampered_cache_plan_fails_before_benchmark(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            plan, prefix = self._reused_bundle(Path(directory))
            result_path = (
                prefix.parent.parent.parent
                / "protocol"
                / "measured-001"
                / "result.json"
            )
            result = json.loads(result_path.read_text(encoding="utf-8"))
            result["cache"]["plan"]["recipe_sha256"] = "0" * 64
            result_path.write_text(json.dumps(result), encoding="utf-8")
            with mock.patch.object(run_workload, "run_benchmark_command") as execute:
                with self.assertRaisesRegex(
                    run_workload.HarnessError, "result.cache.plan does not match"
                ):
                    run_workload.run_casa_recipe_plan(plan, Path(directory) / "run.log")
            execute.assert_not_called()

    def test_reuse_effective_parameter_mismatch_fails_before_benchmark(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            plan, prefix = self._reused_bundle(Path(directory))
            plan["command"]["casa"]["base_overrides"]["niter"] = 0
            with mock.patch.object(run_workload, "run_benchmark_command") as execute:
                with self.assertRaisesRegex(
                    run_workload.HarnessError, "effective parameters do not match"
                ):
                    run_workload.run_casa_recipe_plan(plan, Path(directory) / "run.log")
            execute.assert_not_called()

    @staticmethod
    def _reused_bundle(root: Path) -> tuple[dict[str, object], Path]:
        recipe_path = (
            run_workload.REPO_ROOT / "tools/perf/imager/recipes/vlass-fragment-tclean.last"
        )
        recipe_text = recipe_path.read_text(encoding="utf-8")
        parsed = casa_tclean.parse_literal_assignment_recipe(recipe_text)
        recipe = {
            "path": str(recipe_path),
            "sha256": hashlib.sha256(recipe_text.encode("utf-8")).hexdigest(),
            "task": "tclean",
            "parameter_names": sorted(name for name in parsed if name != "taskname"),
        }
        historical_prefix = root / "partial" / "casa" / "measured-001" / "casa"
        retained_prefix = root / "casa" / "measured-001" / "casa"
        overrides = {
            "vis": str(root / "input.ms"),
            "field": "1525",
            "phasecenter": 1525,
            "datacolumn": "data",
            "interactive": False,
            "parallel": False,
            "restart": False,
            "niter": 2000,
            "imsize": [4096, 4096],
            "spw": "2~17",
            "imagename": str(historical_prefix),
            "cfcache": str(root / "cf-cache"),
        }
        effective, _, _ = casa_tclean.normalize_archived_parameters(
            {name: value for name, value in parsed.items() if name != "taskname"},
            overrides,
        )
        cache_plan = {
            "schema_version": 1,
            "kind": "casa_tclean_cf_plan",
            "casa_version": "6.7.5.9",
            "dataset": {"key": "fixture", "path": str(root / "input.ms")},
            "recipe_sha256": recipe["sha256"],
            "cf_parameters": casa_tclean.cf_cache_parameter_identity(effective),
        }
        request = {
            "schema_version": casa_tclean.REQUEST_SCHEMA_VERSION,
            "kind": casa_tclean.REQUEST_KIND,
            "request_id": "reused-fixture",
            "action": "run",
            "expected_casa_version": "6.7.5.9",
            "recipe": recipe,
            "overrides": overrides,
            "cache": {
                "role": "cold",
                "path": str(root / "cf-cache"),
                "plan": cache_plan,
                "plan_sha256": casa_tclean.canonical_sha256(cache_plan),
                "receipt_path": str(root / "cf-cache-receipt.json"),
            },
            "mask_identity": None,
        }
        def fake_tclean(**kwargs: object) -> None:
            cache = Path(str(kwargs["cfcache"]))
            (cache / "CFS0").mkdir(parents=True)
            (cache / "CFS0" / "table.dat").write_bytes(b"cache")
            product = Path(str(kwargs["imagename"]) + ".image.tt0")
            product.mkdir(parents=True)
            (product / "table.dat").write_bytes(b"image")

        result = casa_tclean.process_request(
            request, tclean_task=fake_tclean, casa_version="6.7.5.9"
        )
        call_root = root / "protocol" / "measured-001"
        call_root.mkdir(parents=True)
        (call_root / "request.json").write_text(
            json.dumps(request), encoding="utf-8"
        )
        (call_root / "result.json").write_text(
            json.dumps(result), encoding="utf-8"
        )
        retained_product = Path(str(retained_prefix) + ".image.tt0")
        retained_product.mkdir(parents=True)
        (retained_product / "table.dat").write_bytes(b"image")
        plan = {
            "command": {
                "kind": "casa_tclean_protocol",
                "argv": [str(run_workload.BENCH_SCRIPT), str(root / "input.ms")],
                "env": {},
                "casa": {
                    "expected_version": "6.7.5.9",
                    "recipe": recipe,
                    "base_overrides": {
                        **overrides,
                        "imagename": str(root / "current" / "casa"),
                    },
                },
            },
            "run": {"skip_casa": "1", "reuse_casa_prefix": str(retained_prefix)},
            "products": {"rust_prefix": str(root / "rust"), "casa_prefix": None},
            "comparison": {"tolerances": None},
        }
        return plan, retained_prefix


class EmbeddedCasaBenchmarkTests(unittest.TestCase):
    @staticmethod
    def _embedded_source() -> str:
        script = (run_workload.REPO_ROOT / "scripts/bench-imager-vs-casa.sh").read_text(
            encoding="utf-8"
        )
        marker = 'cat >"$tmpdir/casa-imager-bench.py" <<\'PY\'\n'
        source = script.split(marker, 1)[1].split("\nPY\n", 1)[0]
        compile(source, "casa-imager-bench.py", "exec")
        return source

    @staticmethod
    def _base_environment() -> dict[str, str]:
        return {
            "CASA_RS_BENCH_MS_PATH": "/tmp/input.ms",
            "CASA_RS_BENCH_REPEATS": "1",
            "CASA_RS_BENCH_FIELD": "0",
            "CASA_RS_BENCH_STOKES": "I",
            "CASA_RS_BENCH_USEPOINTING": "1",
            "CASA_RS_BENCH_PHASECENTER_FIELD": "",
            "CASA_RS_BENCH_SPW": "9",
            "CASA_RS_BENCH_CHANNEL_START": "0",
            "CASA_RS_BENCH_CHANNEL_COUNT": "1",
            "CASA_RS_BENCH_CUBE_START": "",
            "CASA_RS_BENCH_CUBE_WIDTH": "",
            "CASA_RS_BENCH_IMSIZE": "128",
            "CASA_RS_BENCH_CELL_ARCSEC": "0.6",
            "CASA_RS_BENCH_NITER": "0",
            "CASA_RS_BENCH_NMAJOR": "-1",
            "CASA_RS_BENCH_GAIN": "0.1",
            "CASA_RS_BENCH_THRESHOLD_JY": "0",
            "CASA_RS_BENCH_NSIGMA": "0",
            "CASA_RS_BENCH_PSFCUTOFF": "0.35",
            "CASA_RS_BENCH_PBLIMIT": "0.2",
            "CASA_RS_BENCH_PBCOR": "0",
            "CASA_RS_BENCH_MINOR_CYCLE_LENGTH": "2",
            "CASA_RS_BENCH_CYCLEFACTOR": "1",
            "CASA_RS_BENCH_MIN_PSFFRACTION": "0.05",
            "CASA_RS_BENCH_MAX_PSFFRACTION": "0.8",
            "CASA_RS_BENCH_WEIGHTING": "natural",
            "CASA_RS_BENCH_ROBUST": "0.5",
            "CASA_RS_BENCH_PERCHANWEIGHTDENSITY": "0",
            "CASA_RS_BENCH_DECONVOLVER": "mtmfs",
            "CASA_RS_BENCH_USEMASK": "user",
            "CASA_RS_BENCH_MASK_BOX": "",
            "CASA_RS_BENCH_SAVEMODEL": "none",
            "CASA_RS_BENCH_FITSPW": "",
            "CASA_RS_BENCH_FITORDER": "0",
            "CASA_RS_BENCH_SIDELOBETHRESHOLD": "3",
            "CASA_RS_BENCH_NOISETHRESHOLD": "5",
            "CASA_RS_BENCH_NTERMS": "2",
            "CASA_RS_BENCH_CASA_GRIDDER": "awproject",
            "CASA_RS_BENCH_GRIDDER": "awproject",
            "CASA_RS_BENCH_FACETS": "1",
            "CASA_RS_BENCH_WPROJPLANES": "32",
            "CASA_RS_BENCH_SCALES": "",
            "CASA_RS_BENCH_SMALL_SCALE_BIAS": "0.6",
            "CASA_RS_BENCH_RESTORING_BEAM": "common",
            "CASA_RS_BENCH_SPECMODE": "mfs",
            "CASA_RS_BENCH_INTERPOLATION": "linear",
            "CASA_RS_BENCH_KEEP_OUTPUT_ROOT": "",
            "CASA_RS_BENCH_CASA_RESULT_JSON": "",
            "CASA_RS_BENCH_CASA_LOG_FILE": "",
            "CASA_RS_BENCH_UVRANGE": "<12km",
            "CASA_RS_BENCH_INTENT": "OBSERVE_TARGET#UNSPECIFIED",
            "CASA_RS_BENCH_ATERM": "1",
            "CASA_RS_BENCH_PSTERM": "0",
            "CASA_RS_BENCH_WBAWP": "1",
            "CASA_RS_BENCH_CONJBEAMS": "1",
            "CASA_RS_BENCH_COMPUTEPASTEP": "360.0",
            "CASA_RS_BENCH_ROTATEPASTEP": "360.0",
            "CASA_RS_BENCH_POINTINGOFFSETSIGDEV": "0.0",
            "CASA_RS_BENCH_NORMTYPE": "flatnoise",
            "CASA_RS_BENCH_MOSWEIGHT": "0",
            "CASA_RS_BENCH_PSFPHASECENTER": "",
            "CASA_RS_BENCH_VPTABLE": "",
        }

    def _execute_embedded_casa(self, environment: dict[str, str]) -> dict[str, object]:
        captured: dict[str, dict[str, object]] = {}
        casa_tasks = types.ModuleType("casatasks")
        casa_tasks.casalog = mock.Mock()
        casa_tasks.uvcontsub = mock.Mock()

        def fake_tclean(**kwargs: object) -> dict[str, object]:
            captured["kwargs"] = kwargs
            return {}

        casa_tasks.tclean = fake_tclean
        with (
            mock.patch.dict(os.environ, environment, clear=True),
            mock.patch.dict(sys.modules, {"casatasks": casa_tasks}),
            mock.patch("builtins.print"),
        ):
            exec(compile(self._embedded_source(), "casa-imager-bench.py", "exec"), {})
        return captured["kwargs"]

    def test_embedded_casa_call_forwards_selection_and_aw_controls(self) -> None:
        kwargs = self._execute_embedded_casa(self._base_environment())

        self.assertEqual("<12km", kwargs["uvrange"])
        self.assertEqual("OBSERVE_TARGET#UNSPECIFIED", kwargs["intent"])
        self.assertTrue(kwargs["aterm"])
        self.assertFalse(kwargs["psterm"])
        self.assertTrue(kwargs["wbawp"])
        self.assertTrue(kwargs["conjbeams"])
        self.assertEqual(360.0, kwargs["computepastep"])
        self.assertEqual(360.0, kwargs["rotatepastep"])
        self.assertEqual([0.0], kwargs["pointingoffsetsigdev"])
        self.assertEqual("flatnoise", kwargs["normtype"])
        self.assertFalse(kwargs["mosweight"])

        bench = (run_workload.REPO_ROOT / "scripts/bench-imager-vs-casa.sh").read_text(
            encoding="utf-8"
        )
        for name, shell_value in (
            ("UVRANGE", "uvrange"),
            ("INTENT", "intent"),
            ("ATERM", "aterm"),
            ("PSTERM", "psterm"),
            ("WBAWP", "wbawp"),
            ("CONJBEAMS", "conjbeams"),
            ("COMPUTEPASTEP", "computepastep"),
            ("ROTATEPASTEP", "rotatepastep"),
            ("POINTINGOFFSETSIGDEV", "pointingoffsetsigdev"),
            ("NORMTYPE", "normtype"),
            ("MOSWEIGHT", "mosweight"),
            ("PSFPHASECENTER", "psfphasecenter"),
            ("VPTABLE", "vptable"),
        ):
            self.assertIn(f'CASA_RS_BENCH_{name}="${shell_value}"', bench)

    def test_embedded_casa_call_preserves_explicit_false_and_empty_controls(self) -> None:
        environment = self._base_environment()
        environment.update(
            {
                "CASA_RS_BENCH_UVRANGE": "",
                "CASA_RS_BENCH_INTENT": "",
                "CASA_RS_BENCH_ATERM": "0",
                "CASA_RS_BENCH_PSTERM": "1",
                "CASA_RS_BENCH_WBAWP": "0",
                "CASA_RS_BENCH_CONJBEAMS": "0",
                "CASA_RS_BENCH_MOSWEIGHT": "1",
            }
        )
        kwargs = self._execute_embedded_casa(environment)

        self.assertEqual("", kwargs["uvrange"])
        self.assertEqual("", kwargs["intent"])
        self.assertFalse(kwargs["aterm"])
        self.assertTrue(kwargs["psterm"])
        self.assertFalse(kwargs["wbawp"])
        self.assertFalse(kwargs["conjbeams"])
        self.assertTrue(kwargs["mosweight"])


class CompletedRecipeReceiptRecoveryTests(unittest.TestCase):
    def test_recovery_publishes_bound_result_to_original_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            partial = root / "run.partial"
            partial.mkdir()
            receipt_path = root / "run.json"
            failed = {"artifacts": {"bundle": {"partial_root": str(partial)}}}
            recovered = {
                "status": "completed",
                "exit_code": 1,
                "results": {},
            }
            with (
                mock.patch.object(run_workload, "load_run_result", return_value=failed),
                mock.patch.object(
                    run_workload.casa_tclean_workflow,
                    "recover_completed_recipe_run",
                    return_value=recovered,
                ) as recover,
                mock.patch.object(
                    run_workload.casa_tclean_workflow,
                    "benchmark_log_evidence",
                    return_value={
                        "benchmark_log": "bound",
                        "benchmark_log_sha256": "0" * 64,
                    },
                ),
                mock.patch.object(
                    run_workload.casa_tclean_workflow,
                    "finalize_bundle_result",
                    side_effect=lambda value: value,
                ),
                mock.patch.object(run_workload, "validate_run_result"),
                mock.patch.object(run_workload, "atomic_write_json") as write,
                mock.patch("builtins.print"),
            ):
                run_workload.recover_recipe_receipt(receipt_path)

            recover.assert_called_once()
            write.assert_called_once_with(receipt_path, recovered)
            self.assertEqual(0, recovered["exit_code"])


class StageBreakdownTests(unittest.TestCase):
    def test_continuum_residual_comparison_is_bound_to_the_run_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "continuum-residual-comparison.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": "casa-rs-continuum-residual-comparison-v1",
                        "status": "pass",
                        "continuum_residual": {"normalized_rms": 3.0e-5},
                    }
                )
            )
            parsed = run_workload.parse_benchmark_log(
                f"continuum_residual_comparison={path} status=pass nrms=3.000000000e-05\n"
            )

        comparison = parsed["continuum_residual_comparison"]
        self.assertEqual("pass", comparison["status"])
        self.assertEqual(str(path), comparison["path"])
        self.assertRegex(comparison["sha256"], r"^[0-9a-f]{64}$")

    def test_model_data_comparison_is_bound_to_the_run_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "model-data-comparison.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": "casa-rs-model-data-comparison-v1",
                        "status": "pass",
                        "model_data": {"normalized_rms": 2.5e-5},
                    }
                )
            )
            parsed = run_workload.parse_benchmark_log(
                f"model_data_comparison={path} status=pass nrms=2.500000000e-05\n"
            )

        self.assertEqual("pass", parsed["model_data_comparison"]["status"])
        self.assertEqual(str(path), parsed["model_data_comparison"]["path"])
        self.assertRegex(parsed["model_data_comparison"]["sha256"], r"^[0-9a-f]{64}$")

    def test_parse_log_marks_missing_timing_sections_without_claiming_runs(
        self,
    ) -> None:
        parsed = run_workload.parse_benchmark_log(
            """Rust release CLI timings (seconds):
  run=1 real=1.500
  median=1.500

CASA PySynthesisImager stage medians (milliseconds):
  total=42.000
"""
        )

        self.assertEqual("ran", parsed["rust"]["status"])
        self.assertIsNone(parsed["rust"]["reason"])
        self.assertEqual(1.5, parsed["rust"]["timings_seconds"]["median"])
        self.assertEqual("missing", parsed["casa"]["status"])
        self.assertIn("not reported", parsed["casa"]["reason"])
        self.assertIsNone(parsed["casa"]["timings_seconds"]["median"])

    def test_parse_casa_timing_section_tolerates_casa_warning_noise(self) -> None:
        parsed = run_workload.parse_benchmark_log(
            """CASA tclean timings (seconds):
WARNING: All log messages before absl::InitializeLog() is called are written to STDERR
2026-05-18 22:58:25 SEVERE ::casa

0%....10....100%
  run=1 real=56.589418
  median=56.589418
  kept_casa_prefix=/tmp/casa

Kept benchmark products:
  product_root=/tmp/products
"""
        )

        self.assertEqual("ran", parsed["casa"]["status"])
        self.assertEqual([56.589418], parsed["casa"]["timings_seconds"]["runs"])
        self.assertEqual(56.589418, parsed["casa"]["timings_seconds"]["median"])

    def test_parse_casa_stage_section_tolerates_warning_noise(self) -> None:
        parsed = run_workload.parse_benchmark_log(
            """CASA PySynthesisImager stage medians (milliseconds):
WARNING: All log messages before absl::InitializeLog() is called are written to STDERR
2026-05-19 01:55:09 SEVERE ::casa

0%....10....100%
  run=1 total_ms=530361.545 param_setup_ms=0.277 construct_imager_ms=0.007 make_psf_ms=220368.238 calcres_major_ms=305834.961 restore_ms=38.185
  stage medians (ms):
    parameter_setup=0.277
    construct_imager=0.007
    make_psf=220368.238
    calcres_major_cycle=305834.961
    restore_images=38.185
    total=530361.545
  result medians: clean_major_cycles=0 minor_cycles=0
"""
        )

        stages = parsed["stage_medians_ms"]["casa"]

        self.assertEqual(0.277, stages["parameter_setup"])
        self.assertEqual(220368.238, stages["make_psf"])
        self.assertEqual(305834.961, stages["calcres_major_cycle"])
        self.assertEqual(530361.545, stages["total"])
        self.assertNotIn("total_ms", stages)
        self.assertNotIn("make_psf_ms", stages)

    def test_parse_casa_phase_probe_includes_w4_attribution_fields(self) -> None:
        parsed = run_workload.parse_benchmark_log(
            """CASA PySynthesisImager stage medians (milliseconds):
  stage medians (ms):
    parameter_setup=0.277
    initialize_imagers=55.000
    select_data=12.500
    define_image=7.250
    normalizer_info=1.000
    cf_cache_setup=0.000
    set_weighting=8.500
    set_weighting_core=8.250
    make_psf=220368.238
    calcres_major_cycle=305834.961
    minor_cycle=42.000
    clean_major_cycle=99.000
    restore_images=38.185
    total=530361.545
  instrumentation notes:
    cube tuneSelectData and nSubCubeFitInMemory live inside CASA C++ cube major-cycle calls.
"""
        )

        stages = parsed["stage_medians_ms"]["casa"]

        self.assertEqual(12.5, stages["select_data"])
        self.assertEqual(7.25, stages["define_image"])
        self.assertEqual(8.25, stages["set_weighting_core"])
        self.assertEqual(99.0, stages["clean_major_cycle"])

    def test_parse_casa_phase_probe_clean_control_diagnostics(self) -> None:
        parsed = run_workload.parse_benchmark_log(
            """CASA PySynthesisImager stage medians (milliseconds):
  stage medians (ms):
    total=123.000
  clean_control_diagnostics_json=[{"minor_cycle": 1, "iterdone": 17, "summaryminor": {"total_iterations": 17.0}}]
"""
        )

        diagnostics = parsed["casa_clean_control_diagnostics"]

        self.assertEqual(1, diagnostics[0]["minor_cycle"])
        self.assertEqual(17.0, diagnostics[0]["summaryminor"]["total_iterations"])

    def test_casa_stage_breakdown_maps_w4_attribution_categories(self) -> None:
        parsed = {
            "stage_medians_ms": {
                "rust": {},
                "casa": {
                    "protocol_preflight": 5.0,
                    "tclean_task": 700.0,
                    "product_inventory": 20.0,
                    "cache_postcondition": 25.0,
                    "protocol_total": 750.0,
                    "select_data": 12.5,
                    "define_image": 7.25,
                    "normalizer_info": 1.0,
                    "cf_cache_setup": 0.0,
                    "set_weighting": 8.5,
                    "set_weighting_core": 8.25,
                    "make_psf": 220.0,
                    "calcres_major_cycle": 305.0,
                    "minor_cycle": 42.0,
                    "clean_major_cycle": 99.0,
                    "restore_images": 38.0,
                    "total": 750.0,
                },
            }
        }
        plan = {
            "mode": {
                "bench_mode": "clean",
                "niter": 10,
            }
        }

        run_workload.attach_stage_breakdown(plan, parsed)

        categories = parsed["stage_breakdown"]["casa"]["categories"]
        self.assertEqual(700.0, categories["opaque_tclean_task"]["total_ms"])
        self.assertEqual(45.0, categories["evidence_postconditions"]["total_ms"])
        self.assertIn(
            "does not claim internal attribution",
            categories["opaque_tclean_task"]["description"],
        )
        self.assertEqual(
            20.75,
            categories["ms_selection_and_image_definition"]["total_ms"],
        )
        self.assertEqual(
            624.0,
            categories["cube_major_cycle_algorithm_envelope"]["total_ms"],
        )
        self.assertIn(
            "nSubCubeFitInMemory",
            categories["cube_major_cycle_algorithm_envelope"]["description"],
        )
        self.assertEqual(
            442.0,
            categories["image_store_writeback_and_restore"]["total_ms"],
        )

    def test_benchmark_features_mirror_namespaced_casa_resources(self) -> None:
        casa_resources = {
            "peak_rss_bytes_max": 123,
            "user_cpu_seconds_median": 4.0,
            "system_cpu_seconds_median": 2.0,
            "disk_read_bytes_median": 1000,
            "disk_write_bytes_median": 2000,
            "block_input_operations_median": 3,
            "block_output_operations_median": 4,
            "product_logical_bytes_median": 5000,
            "cf_cache_logical_bytes_max": 6000,
            "cf_cache_included_file_count_max": 7,
        }
        features = run_workload.build_benchmark_feature_summary(
            {
                "mode": {"image_shape": [16, 16]},
                "comparison": {"products": []},
                "environment": {"runtime": {}},
                "command": {"env": {}},
            },
            {
                "casa": {
                    "evidence_summary": {"resources": casa_resources},
                },
                "stage_medians_ms": {"rust": {}},
            },
        )
        resources = features["resources"]
        self.assertEqual(123, resources["casa_peak_rss_bytes"])
        self.assertEqual(1000, resources["casa_disk_read_bytes_median"])
        self.assertEqual(6000, resources["casa_cf_cache_logical_bytes_max"])
        self.assertIsNone(resources["peak_rss_bytes"])

    def test_empty_results_include_reasons_for_both_sides(self) -> None:
        results = run_workload.empty_results(
            casa_status="blocked",
            reason="benchmark command exited 2",
        )

        self.assertEqual("not_run", results["rust"]["status"])
        self.assertEqual("benchmark command exited 2", results["rust"]["reason"])
        self.assertEqual("blocked", results["casa"]["status"])
        self.assertEqual("benchmark command exited 2", results["casa"]["reason"])

    def test_benchmark_failure_reason_preserves_error_line(self) -> None:
        reason = run_workload.benchmark_failure_reason(
            """Rust release CLI timings (seconds):
error: Rust casars-imager run 1 failed
Error: bounded source stream rejected production imaging request before visibility-column preparation: BriggsBwTaper has no shared bounded stream consumer for this mode
real 1.145408
""",
            1,
        )

        self.assertIn(
            "bounded source stream rejected production imaging request", reason
        )

    def test_wterm_manifest_can_be_dry_run_for_wave3_matrix(self) -> None:
        manifest = {
            "id": "unsupported-wterm",
            "mode_id": "standard-mfs-dirty-wterm",
            "dataset": {
                "key": "fake.ms",
                "path": "/tmp/fake.ms",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
                "wterm": "direct",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("dry_run_only", plan["run_support"]["status"])
        self.assertIn("wterm='direct'", plan["run_support"]["reason"])

    def test_stream_log_override_does_not_require_manifest_edit(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            dataset = Path(tempdir) / "input.ms"
            dataset.mkdir()
            manifest = {
                "id": "stream-log-override",
                "mode_id": "standard-mfs-dirty",
                "dataset": {
                    "key": "input.ms",
                    "path": str(dataset),
                },
                "imaging": {
                    "mode": "dirty",
                    "specmode": "mfs",
                    "gridder": "standard",
                },
                "run": {"stream_log": False},
            }

            plan = run_workload.build_plan(
                manifest_path=Path("manifest.json"),
                manifest=manifest,
                repeats_override=1,
                run_label_override=None,
                storage_label_override=None,
                dry_run=True,
                stream_log_override=True,
            )

        self.assertTrue(plan["run"]["stream_log"])

    def test_stream_log_cli_default_preserves_manifest_value(self) -> None:
        parser = run_workload.build_arg_parser()

        default_args = parser.parse_args(["stream-log-manifest"])
        forced_args = parser.parse_args(["stream-log-manifest", "--stream-log"])

        self.assertIsNone(default_args.stream_log)
        self.assertTrue(forced_args.stream_log)

    def test_non_runnable_wterm_fails_only_for_real_execution(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            dataset = Path(tempdir) / "input.ms"
            dataset.mkdir()
            manifest = {
                "id": "direct-wterm-smoke",
                "mode_id": "direct-wterm-mfs-dirty",
                "dataset": {
                    "key": "input.ms",
                    "path": str(dataset),
                },
                "imaging": {
                    "mode": "dirty",
                    "specmode": "mfs",
                    "gridder": "standard",
                    "wterm": "direct",
                },
            }

            with mock.patch.dict(
                "os.environ",
                {"CASA_RS_CASA_PYTHON": sys.executable},
                clear=True,
            ):
                with self.assertRaisesRegex(
                    run_workload.HarnessError,
                    "wterm='direct'",
                ):
                    run_workload.build_plan(
                        manifest_path=Path("manifest.json"),
                        manifest=manifest,
                        repeats_override=1,
                        run_label_override=None,
                        storage_label_override=None,
                        dry_run=False,
                    )

    def test_missing_casa_python_fails_before_running_benchmark(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            dataset = Path(tempdir) / "input.ms"
            dataset.mkdir()
            manifest = {
                "id": "requires-casa",
                "mode_id": "standard-mfs-dirty-control",
                "dataset": {
                    "key": "input.ms",
                    "path": str(dataset),
                },
                "imaging": {
                    "mode": "dirty",
                    "specmode": "mfs",
                    "gridder": "standard",
                },
            }

            with mock.patch.dict("os.environ", {}, clear=True):
                with self.assertRaisesRegex(
                    run_workload.HarnessError,
                    "CASA_RS_CASA_PYTHON is required",
                ):
                    run_workload.build_plan(
                        manifest_path=Path("manifest.json"),
                        manifest=manifest,
                        repeats_override=1,
                        run_label_override=None,
                        storage_label_override=None,
                        dry_run=False,
                    )

    def test_missing_dataset_fails_before_running_benchmark(self) -> None:
        manifest = {
            "id": "missing-dataset",
            "mode_id": "standard-mfs-dirty-control",
            "dataset": {
                "key": "missing.ms",
                "path": "/definitely/not/a/dataset.ms",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
            },
        }

        with mock.patch.dict(
            "os.environ",
            {"CASA_RS_CASA_PYTHON": sys.executable},
            clear=True,
        ):
            with self.assertRaisesRegex(
                run_workload.HarnessError,
                "dataset path does not exist",
            ):
                run_workload.build_plan(
                    manifest_path=Path("manifest.json"),
                    manifest=manifest,
                    repeats_override=1,
                    run_label_override=None,
                    storage_label_override=None,
                    dry_run=False,
                )

    def test_dirty_workload_marks_clean_only_categories_skipped(self) -> None:
        plan = {
            "mode": {
                "bench_mode": "dirty",
                "niter": 0,
            }
        }
        stages = {
            "open_measurement_set": 1.0,
            "prepare_plane_input": 2.0,
            "get_ms_values_into_processing_buffer": 2.5,
            "prepare_processing_buffer": 3.5,
            "extract_phase_center": 3.0,
            "weighting": 4.0,
            "psf_grid": 5.0,
            "psf_fft": 6.0,
            "psf_normalize": 7.0,
            "residual_degrid_grid": 8.0,
            "residual_fft": 9.0,
            "residual_normalize": 10.0,
            "minor_cycle_solve": 0.0,
            "major_cycle_refresh": 0.0,
            "beam_fit": 0.0,
            "restore": 0.0,
            "build_coordinate_system": 11.0,
            "write_products": 12.0,
            "frontend_total": 30.0,
            "total": 40.0,
        }

        breakdown = run_workload.build_rust_stage_breakdown(plan, stages)
        categories = breakdown["categories"]

        self.assertEqual(
            categories["frontend_ms_preparation"]["total_ms"],
            6.0,
        )
        self.assertEqual(categories["standard_mfs_source_read"]["total_ms"], 2.5)
        self.assertEqual(categories["standard_mfs_source_prepare"]["total_ms"], 3.5)
        self.assertEqual(categories["gridding_degridding"]["total_ms"], 13.0)
        self.assertEqual(categories["fft"]["total_ms"], 15.0)
        self.assertEqual(categories["normalization_pb_correction"]["total_ms"], 17.0)
        self.assertEqual(categories["deconvolution_minor_cycle"]["status"], "skipped")
        self.assertEqual(
            categories["model_prediction_and_residual_refresh"]["status"],
            "skipped",
        )
        self.assertEqual(categories["preview_sidecar_generation"]["status"], "skipped")

    def test_clean_workload_reports_clean_categories_when_measured(self) -> None:
        plan = {
            "mode": {
                "bench_mode": "clean",
                "niter": 25,
            }
        }
        stages = {
            "minor_cycle_solve": 3.5,
            "major_cycle_refresh": 8.0,
            "beam_fit": 1.0,
            "restore": 2.0,
        }

        breakdown = run_workload.build_rust_stage_breakdown(plan, stages)
        categories = breakdown["categories"]

        self.assertEqual(categories["deconvolution_minor_cycle"]["status"], "measured")
        self.assertEqual(categories["deconvolution_minor_cycle"]["total_ms"], 3.5)
        self.assertEqual(
            categories["model_prediction_and_residual_refresh"]["status"],
            "measured",
        )
        self.assertEqual(categories["restore_and_beam_fit"]["total_ms"], 3.0)

    def test_phasecenter_field_flows_to_environment(self) -> None:
        manifest = {
            "id": "mosaic-smoke",
            "mode_id": "mosaic-mfs-clean-primary",
            "dataset": {
                "key": "mosaic.ms",
                "relative_path": "wave1/vla/mosaic/small/ms/mosaic.ms",
                "root_env": "CASA_RS_IMPERF_DATA_ROOT",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "mosaic",
                "field": "",
                "stokes": "Q",
                "phasecenter_field": 0,
                "usepointing": True,
                "spw": "0",
                "deconvolver": "mtmfs",
                "hogbom_iteration_mode": "casa",
                "nterms": 2,
                "pblimit": 0.17,
                "write_pb": True,
                "pbcor": True,
            },
        }

        with mock.patch.dict(
            "os.environ",
            {
                "CASA_RS_IMPERF_DATA_ROOT": "/tmp",
                "CASA_RS_CASA_PYTHON": "/tmp/casa-python",
            },
            clear=False,
        ):
            plan = run_workload.build_plan(
                manifest_path=Path("manifest.json"),
                manifest=manifest,
                repeats_override=1,
                run_label_override=None,
                storage_label_override=None,
                dry_run=True,
            )

        self.assertEqual("0", plan["command"]["env"]["IMAGER_BENCH_PHASECENTER_FIELD"])
        self.assertEqual("Q", plan["command"]["env"]["IMAGER_BENCH_STOKES"])
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_USEPOINTING"])
        self.assertEqual("2", plan["command"]["env"]["IMAGER_BENCH_NTERMS"])
        self.assertEqual(
            "casa", plan["command"]["env"]["IMAGER_BENCH_HOGBOM_ITERATION_MODE"]
        )
        self.assertEqual("0.17", plan["command"]["env"]["IMAGER_BENCH_PBLIMIT"])
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_WRITE_PB"])
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_PBCOR"])
        self.assertEqual("casa", plan["mode"]["hogbom_iteration_mode"])
        self.assertEqual(2, plan["mode"]["nterms"])

    def test_wprojection_auto_uses_distinct_casa_auto_plane_value(self) -> None:
        manifest = {
            "id": "wprojection-auto",
            "mode_id": "wprojection-single-plane",
            "dataset": {"key": "wide.ms", "path": "/tmp/wide.ms"},
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "wproject",
                "wterm": "wproject",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        env = plan["command"]["env"]
        self.assertEqual("", env["IMAGER_BENCH_WPROJPLANES"])
        self.assertEqual("-1", env["IMAGER_BENCH_CASA_WPROJPLANES"])

        manifest["imaging"]["wprojplanes"] = 96
        explicit = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )["command"]["env"]
        self.assertEqual("96", explicit["IMAGER_BENCH_WPROJPLANES"])
        self.assertEqual("96", explicit["IMAGER_BENCH_CASA_WPROJPLANES"])

    def test_ms_staging_defaults_to_direct(self) -> None:
        manifest = {
            "id": "medium-direct",
            "mode_id": "standard-mfs-dirty-control",
            "dataset": {
                "key": "medium.ms",
                "path": "/tmp/medium.ms",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("direct", plan["run"]["ms_staging"])
        self.assertEqual("direct", plan["command"]["env"]["IMAGER_BENCH_MS_STAGING"])
        self.assertEqual("0", plan["run"]["phase_probe"])
        self.assertEqual("0", plan["command"]["env"]["IMAGER_BENCH_PHASE_PROBE"])
        self.assertEqual("0", plan["run"]["skip_casa"])
        self.assertEqual("0", plan["run"]["skip_rust"])
        self.assertIsNone(plan["run"]["reuse_rust_prefix"])
        self.assertIsNone(plan["run"]["reuse_casa_prefix"])
        self.assertEqual("0", plan["command"]["env"]["IMAGER_BENCH_SKIP_CASA"])
        self.assertEqual("0", plan["command"]["env"]["IMAGER_BENCH_SKIP_RUST"])
        self.assertNotIn("IMAGER_BENCH_REUSE_RUST_PREFIX", plan["command"]["env"])
        self.assertNotIn("IMAGER_BENCH_REUSE_CASA_PREFIX", plan["command"]["env"])
        self.assertEqual("0", plan["command"]["env"]["IMAGER_BENCH_SKIP_PROFILE"])
        self.assertEqual("runnable", plan["run_support"]["status"])
        self.assertEqual(
            "pending", run_workload.human_review_gate(plan, None)["status"]
        )
        self.assertEqual("Brian", plan["review"]["required_reviewer"])
        self.assertEqual(
            128 * 128 * 3, plan["benchmark_features"]["image"]["image_work"]
        )
        self.assertEqual(
            1, plan["benchmark_features"]["visibility"]["selected_channels"]
        )

    def test_skip_rust_can_reuse_existing_rust_products(self) -> None:
        manifest = {
            "id": "casa-only-compare",
            "mode_id": "standard-cube-line-clean-multiscale-final",
            "dataset": {
                "key": "medium.ms",
                "path": "/tmp/medium.ms",
            },
            "imaging": {
                "mode": "clean",
                "specmode": "cube",
                "gridder": "standard",
            },
            "run": {
                "skip_rust": "1",
                "reuse_rust_prefix": "/bench/products/current-rust/rust",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("1", plan["run"]["skip_rust"])
        self.assertEqual(
            "/bench/products/current-rust/rust", plan["run"]["reuse_rust_prefix"]
        )
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_SKIP_RUST"])
        self.assertEqual(
            "/bench/products/current-rust/rust",
            plan["command"]["env"]["IMAGER_BENCH_REUSE_RUST_PREFIX"],
        )

    def test_skip_casa_can_reuse_existing_casa_products(self) -> None:
        manifest = {
            "id": "rust-only-compare",
            "mode_id": "standard-cube-line-clean-multiscale-final",
            "dataset": {
                "key": "medium.ms",
                "path": "/tmp/medium.ms",
            },
            "imaging": {
                "mode": "clean",
                "specmode": "cube",
                "gridder": "standard",
            },
            "run": {
                "skip_casa": "1",
                "reuse_casa_prefix": "/bench/products/current-casa/casa",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("1", plan["run"]["skip_casa"])
        self.assertEqual(
            "/bench/products/current-casa/casa", plan["run"]["reuse_casa_prefix"]
        )
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_SKIP_CASA"])
        self.assertEqual(
            "/bench/products/current-casa/casa",
            plan["command"]["env"]["IMAGER_BENCH_REUSE_CASA_PREFIX"],
        )

    def test_faceted_widefield_plan_preserves_exact_command_controls(self) -> None:
        manifest_path = (
            run_workload.WORKLOAD_DIR / "issue518-t32-vla-faceted-clean.json"
        )
        manifest = run_workload.load_manifest(manifest_path)
        plan = run_workload.build_plan(
            manifest_path=manifest_path,
            manifest=manifest,
            repeats_override=None,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        env = plan["command"]["env"]
        self.assertEqual("widefield", env["IMAGER_BENCH_GRIDDER"])
        self.assertEqual("widefield", env["IMAGER_BENCH_CASA_GRIDDER"])
        self.assertEqual("2", env["IMAGER_BENCH_FACETS"])
        self.assertEqual("1", env["IMAGER_BENCH_WPROJPLANES"])
        self.assertEqual("1", env["IMAGER_BENCH_CASA_WPROJPLANES"])
        self.assertEqual("cpu", env["IMAGER_BENCH_STANDARD_MFS_ACCELERATION"])
        self.assertEqual("rustfft", env["IMAGER_BENCH_IMAGING_FFT_BACKEND"])
        self.assertEqual("auto", env["IMAGER_BENCH_IMAGING_FFT_PRECISION"])
        self.assertEqual("0", env["IMAGER_BENCH_PARALLEL"])
        self.assertEqual(2, plan["mode"]["facets"])
        self.assertEqual(24, plan["mode"]["channel_count"])
        self.assertEqual("copy", plan["run"]["ms_staging"])
        self.assertEqual("copy", env["IMAGER_BENCH_MS_STAGING"])
        self.assertEqual("full", plan["comparison"]["mode"])
        self.assertEqual(
            [".image", ".residual", ".model", ".psf"],
            plan["comparison"]["products"],
        )
        self.assertFalse(plan["comparison"]["require_exact_product_inventory"])
        self.assertFalse(plan["comparison"]["require_metadata_parity"])
        self.assertTrue(plan["comparison"]["require_direction_wcs_parity"])
        self.assertEqual(
            ["facet-seam-x", "facet-seam-y"],
            [region["id"] for region in plan["comparison"]["source_regions"]],
        )
        self.assertTrue(
            all(
                ".model" in region["products"]
                for region in plan["comparison"]["source_regions"]
            )
        )

        bench = (run_workload.REPO_ROOT / "scripts/bench-imager-vs-casa.sh").read_text()
        self.assertIn('--facets "$facets"', bench)
        self.assertIn('--stokes "$stokes"', bench)
        self.assertIn("rust_pointing_flags+=(--usepointing)", bench)
        self.assertIn("facets=facets,", bench)
        self.assertIn("stokes=stokes,", bench)
        self.assertIn("usepointing=usepointing,", bench)
        self.assertIn(
            'initialize_imaging_owner "$rust_ms_path"',
            bench,
        )
        self.assertIn('CASA_RS_BENCH_MS_PATH="$casa_ms_path"', bench)
        self.assertIn('casa_ms_path="$tmpdir/casa-benchmark.ms"', bench)
        self.assertIn('cp -R "$source_ms_path" "$casa_ms_path"', bench)

    def test_reuse_prefixes_imply_skipping_the_reused_implementation(self) -> None:
        manifest = {
            "id": "reuse-both",
            "mode_id": "standard-mfs-dirty-control",
            "dataset": {"key": "medium.ms", "path": "/tmp/medium.ms"},
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
            },
            "run": {
                "reuse_rust_prefix": "/bench/products/rust/rust",
                "reuse_casa_prefix": "/bench/products/casa/casa",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("1", plan["run"]["skip_rust"])
        self.assertEqual("1", plan["run"]["skip_casa"])
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_SKIP_RUST"])
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_SKIP_CASA"])

    def test_attach_output_paths_keeps_bulk_artifacts_out_of_result_dir(self) -> None:
        plan = {
            "run_id": "test-run",
            "command": {"env": {}},
        }
        result_dir = Path("/workspace/target/imperformance-wave4/current-runs")
        with tempfile.TemporaryDirectory() as tempdir:
            artifact_root = (
                Path(tempdir) / "_tmp_safe_to_delete" / "imperformance-artifacts"
            )

            run_workload.attach_output_paths(
                plan,
                result_dir,
                artifact_root,
                dry_run=False,
            )

        self.assertEqual(
            str(artifact_root / "products" / "test-run"),
            plan["products"]["root"],
        )
        self.assertEqual(str(result_dir), plan["artifacts"]["result_dir"])
        self.assertEqual(
            str(artifact_root / "comparisons" / "test-run"),
            plan["artifacts"]["comparison_root"],
        )
        self.assertEqual(
            str(artifact_root / "tmp"),
            plan["command"]["env"]["IMAGER_BENCH_TMP_ROOT"],
        )
        self.assertNotIn(
            str(result_dir),
            plan["command"]["env"]["IMAGER_BENCH_KEEP_OUTPUT_ROOT"],
        )
        self.assertIn(
            "_tmp_safe_to_delete",
            str(run_workload.perf_paths.DEFAULT_EXTERNAL_ARTIFACT_ROOT),
        )

    def test_generic_comparison_uses_a_bound_absolute_structure_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            plan = {
                "artifacts": {"comparison_root": str(root / "comparisons")},
                "products": {
                    "rust_prefix": str(root / "rust"),
                    "casa_prefix": str(root / "casa"),
                },
                "environment": {
                    "executables": {"casa_python": {"path": sys.executable}}
                },
                "comparison": {
                    "products": [".image"],
                    "max_elements_per_product": 100,
                    "mode": "full",
                    "full_chunk_elements": 50,
                    "require_exact_product_inventory": True,
                    "require_metadata_parity": True,
                    "source_regions": [],
                    "tolerances": None,
                },
            }
            comparison = {"status": "completed", "products": {}}
            runner = mock.Mock(return_value=comparison)

            with mock.patch.object(run_workload, "compare_image_products", runner):
                result = run_workload.compare_products(plan, {}, root / "run.log")

            request = runner.call_args.kwargs["request"]
            self.assertEqual(
                str(root / "comparisons" / "structure-workspace"),
                request["structure_workspace_dir"],
            )
            self.assertTrue(Path(request["structure_workspace_dir"]).is_absolute())
            self.assertEqual(str(root / "comparisons" / "panels"), result["panel_dir"])

    def test_generic_comparison_failure_closes_to_terminal_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            plan = {
                "artifacts": {"comparison_root": str(root / "comparisons")},
                "products": {
                    "rust_prefix": str(root / "rust"),
                    "casa_prefix": str(root / "casa"),
                },
                "environment": {
                    "executables": {"casa_python": {"path": sys.executable}}
                },
                "comparison": {
                    "products": [".model"],
                    "max_elements_per_product": 100,
                    "mode": "full",
                    "full_chunk_elements": 50,
                    "require_exact_product_inventory": False,
                    "require_direction_wcs_parity": True,
                    "require_metadata_parity": False,
                    "source_regions": [],
                    "tolerances": None,
                },
            }
            runner = mock.Mock(
                return_value={
                    "status": "failed_validation",
                    "reason": "exact-zero centroid was rejected",
                    "failure": {"kind": "comparison_protocol_binding"},
                    "products": {},
                    "input": "/comparison/input.json",
                }
            )

            with mock.patch.object(run_workload, "compare_image_products", runner):
                result = run_workload.compare_products(plan, {}, root / "run.log")

            self.assertEqual(
                {
                    "status": "failed_validation",
                    "reason": "exact-zero centroid was rejected",
                    "products": {},
                },
                result,
            )

    def test_vlass_recipe_plan_preserves_real_aw_and_reports_rust_runnable(
        self,
    ) -> None:
        manifest_path = run_workload.WORKLOAD_DIR / "vlass-fragment-smoke-cold.json"
        manifest = run_workload.load_manifest(manifest_path)
        with mock.patch.dict(
            os.environ,
            {
                "CASA_RS_VLASS_DATA_ROOT": "/Volumes/GLENDENNING/test-data",
                "CASA_RS_CASA_PYTHON": sys.executable,
                "CASA_RS_BENCH_PREPARED_AW_CASA_CACHE": "/validated/cfs",
                "CASA_RS_BENCH_PREPARED_AW_OUTPUT_PREFIX": "/shared/aw/dirty",
            },
            clear=False,
        ):
            plan = run_workload.build_plan(
                manifest_path=manifest_path,
                manifest=manifest,
                repeats_override=None,
                run_label_override=None,
                storage_label_override=None,
                dry_run=True,
            )
        run_workload.attach_output_paths(
            plan,
            Path("/tmp/vlass-results"),
            Path("/Volumes/GLENDENNING/casa-rs-vlass/issue-446/artifacts"),
            cf_cache_root=Path("/Volumes/GLENDENNING/casa-rs-vlass/issue-446/cf-cache"),
            dry_run=True,
        )

        self.assertEqual("runnable", plan["run_support"]["status"])
        self.assertEqual("runnable", plan["run_support"]["targets"]["rust"]["status"])
        self.assertEqual(
            [], plan["run_support"]["targets"]["rust"]["missing_capabilities"]
        )
        effective = plan["command"]["casa"]["effective_plan"]["effective_kwargs"]
        self.assertEqual("awproject", effective["gridder"])
        self.assertTrue(effective["aterm"])
        self.assertTrue(effective["wbawp"])
        self.assertTrue(effective["conjbeams"])
        self.assertTrue(effective["usepointing"])
        self.assertEqual("9", effective["spw"])
        self.assertEqual(
            "awproject", plan["command"]["rust"]["intended_parameters"]["gridder"]
        )
        self.assertEqual(
            [str(run_workload.BENCH_SCRIPT), plan["dataset"]["path"]],
            plan["command"]["argv"],
        )
        aw_env = plan["command"]["env"]
        self.assertEqual(
            "/validated/cfs", aw_env["IMAGER_BENCH_PREPARED_AW_CASA_CACHE"]
        )
        self.assertEqual("/shared/aw/dirty", aw_env["IMAGER_BENCH_RUST_OUTPUT_PREFIX"])
        self.assertEqual("384", aw_env["IMAGER_BENCH_AW_CF_RESIDENT_MB"])
        self.assertEqual("1", aw_env["IMAGER_BENCH_ATERM"])
        self.assertEqual("0", aw_env["IMAGER_BENCH_PSTERM"])
        self.assertEqual("1", aw_env["IMAGER_BENCH_WBAWP"])
        self.assertEqual("1", aw_env["IMAGER_BENCH_CONJBEAMS"])
        self.assertEqual("360.0", aw_env["IMAGER_BENCH_COMPUTEPASTEP"])
        self.assertEqual("360.0", aw_env["IMAGER_BENCH_ROTATEPASTEP"])
        self.assertEqual("flatnoise", aw_env["IMAGER_BENCH_NORMTYPE"])
        self.assertEqual("0", aw_env["IMAGER_BENCH_MOSWEIGHT"])
        self.assertEqual("", aw_env["IMAGER_BENCH_PSFPHASECENTER"])
        self.assertEqual("", aw_env["IMAGER_BENCH_VPTABLE"])
        self.assertEqual("/shared/aw/dirty", plan["products"]["rust_prefix"])
        self.assertEqual(
            "/Volumes/GLENDENNING/casa-rs-vlass/issue-446",
            plan["command"]["evidence_storage"]["required_root"],
        )
        self.assertEqual(
            1 << 40,
            plan["command"]["evidence_storage"]["minimum_free_bytes"],
        )
        validation_geometry = plan["command"]["casa"]["dataset_geometry"]
        cache_geometry = plan["command"]["casa"]["cache_plan"]["dataset_geometry"]
        self.assertEqual(
            "expected_only_dry_run", validation_geometry["dataset"]["status"]
        )
        self.assertIn("source_path", validation_geometry)
        self.assertNotIn("source_path", cache_geometry)
        self.assertNotIn("status", cache_geometry["dataset"])
        self.assertNotIn("path", plan["command"]["casa"]["cache_plan"]["dataset"])

    def test_vlass_recipe_rejects_unimplemented_aw_contract_variant(self) -> None:
        manifest_path = run_workload.WORKLOAD_DIR / "vlass-fragment-smoke-cold.json"
        manifest = run_workload.load_manifest(manifest_path)
        manifest["imaging"]["psterm"] = True
        support = run_workload.casa_tclean_workflow.recipe_run_support(
            workload_id=manifest["id"],
            imaging=manifest["imaging"],
            skip_casa=False,
            skip_rust=True,
        )

        self.assertEqual("casa_only", support["status"])
        self.assertEqual("unavailable", support["targets"]["rust"]["status"])
        self.assertIn(
            "true EVLA A-term",
            support["targets"]["rust"]["reason"],
        )

    def test_vlass_recipe_rejects_wrong_or_omitted_w_and_pa_controls(self) -> None:
        manifest_path = run_workload.WORKLOAD_DIR / "vlass-fragment-smoke-cold.json"
        baseline = run_workload.load_manifest(manifest_path)
        mutations = (
            ("omitted-wterm", "wterm", None),
            ("wrong-wterm", "wterm", "none"),
            ("omitted-wprojplanes", "wprojplanes", None),
            ("wrong-wprojplanes", "wprojplanes", 31),
            ("omitted-computepastep", "computepastep", None),
            ("wrong-computepastep", "computepastep", 180.0),
            ("omitted-rotatepastep", "rotatepastep", None),
            ("wrong-rotatepastep", "rotatepastep", 180.0),
        )
        for label, field, value in mutations:
            with self.subTest(label=label):
                imaging = copy.deepcopy(baseline["imaging"])
                if value is None:
                    imaging.pop(field)
                else:
                    imaging[field] = value
                support = run_workload.casa_tclean_workflow.recipe_run_support(
                    workload_id=baseline["id"],
                    imaging=imaging,
                    skip_casa=False,
                    skip_rust=True,
                )
                self.assertEqual("casa_only", support["status"])
                self.assertEqual("unavailable", support["targets"]["rust"]["status"])

    def test_vlass_cold_and_warm_smokes_share_one_complete_cf_plan(self) -> None:
        plan_hashes = []
        for workload in ("vlass-fragment-smoke-cold", "vlass-fragment-smoke-warm"):
            manifest_path = run_workload.WORKLOAD_DIR / f"{workload}.json"
            manifest = run_workload.load_manifest(manifest_path)
            with mock.patch.dict(
                os.environ,
                {"CASA_RS_VLASS_DATA_ROOT": "/Volumes/GLENDENNING/test-data"},
                clear=False,
            ):
                plan = run_workload.build_plan(
                    manifest_path=manifest_path,
                    manifest=manifest,
                    repeats_override=None,
                    run_label_override=None,
                    storage_label_override=None,
                    dry_run=True,
                )
            run_workload.attach_output_paths(
                plan,
                Path("/tmp/vlass-results"),
                Path("/Volumes/GLENDENNING/casa-rs-vlass/issue-446/artifacts"),
                cf_cache_root=Path(
                    "/Volumes/GLENDENNING/casa-rs-vlass/issue-446/cf-cache"
                ),
                dry_run=True,
            )
            plan_hashes.append(plan["command"]["casa"]["cache_plan_sha256"])

        self.assertEqual(plan_hashes[0], plan_hashes[1])

    def test_storage_label_override_cannot_disable_manifest_storage_policy(
        self,
    ) -> None:
        manifest_path = run_workload.WORKLOAD_DIR / "vlass-fragment-smoke-cold.json"
        manifest = run_workload.load_manifest(manifest_path)
        with mock.patch.dict(
            os.environ,
            {"CASA_RS_VLASS_DATA_ROOT": "/Volumes/GLENDENNING/test-data"},
            clear=False,
        ):
            plan = run_workload.build_plan(
                manifest_path=manifest_path,
                manifest=manifest,
                repeats_override=None,
                run_label_override=None,
                storage_label_override="display-label-only",
                dry_run=True,
            )

        self.assertEqual("display-label-only", plan["run"]["storage_label"])
        self.assertEqual(
            "GLENDENNING-vlass-archive",
            plan["command"]["evidence_storage"]["policy_id"],
        )

    def test_vlass_real_run_rejects_internal_evidence_paths(self) -> None:
        plan = {
            "command": {
                "kind": "casa_tclean_protocol",
                "evidence_storage": run_workload.casa_tclean_workflow.storage_requirement(
                    {"storage_label": "GLENDENNING-vlass-archive"},
                    {"key": "vlass-fragment-b80d5e87487a"},
                ),
            },
            "dataset": {
                "key": "vlass-fragment-b80d5e87487a",
                "path": "/Volumes/GLENDENNING/data/input.ms",
            },
        }
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            with self.assertRaisesRegex(run_workload.HarnessError, "must be under"):
                run_workload.casa_tclean_workflow.validate_storage_preconditions(
                    plan,
                    output_dir=root / "runs",
                    artifact_root=root / "artifacts",
                    cf_cache_root=root / "cf-cache",
                )

    def test_interrupted_run_writes_a_typed_partial_call_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            call_root = root / "protocol" / "warmup-001"
            call_root.mkdir(parents=True)
            (call_root / "request.json").write_text("{}", encoding="utf-8")
            (call_root / "stdout-stderr.log").write_text(
                "partial CASA output\n", encoding="utf-8"
            )
            plan = {
                "run_id": "interrupted-vlass",
                "created_at": "2026-07-20T20:25:26Z",
                "environment": canonical_test_environment(),
                "artifacts": {"protocol_root": str(root / "protocol")},
                "review": {},
            }
            log_path = root / "run.log"

            result = run_workload.casa_tclean_workflow.interrupted_run_result(
                plan,
                log_path=log_path,
                reason="memory safety stop",
                services=run_workload.recipe_execution_services(),
            )

            run_workload.validate_run_result(result, source="test interrupt")
            self.assertEqual("failed_execution", result["status"])
            self.assertEqual(130, result["exit_code"])
            self.assertEqual("operator_interrupt", result["results"]["failure"]["kind"])
            partial = result["results"]["casa_tclean_calls"]["partial"]
            self.assertEqual("warmup-001", partial[0]["name"])
            self.assertEqual(
                str(call_root / "stdout-stderr.log"), partial[0]["stdout_stderr_path"]
            )
            self.assertIn("memory safety stop", log_path.read_text(encoding="utf-8"))

    def test_shared_imaging_resource_controls_flow_to_environment(self) -> None:
        manifest = {
            "id": "resource-controls",
            "mode_id": "standard-mfs-dirty-control",
            "dataset": {
                "key": "medium.ms",
                "path": "/tmp/medium.ms",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
                "imaging_memory_target_mb": 2048,
                "imaging_prepare_buffer_mb": 128,
                "imaging_row_block_rows": 4096,
                "imaging_prepare_workers": 3,
                "imaging_read_ahead_blocks": 2,
                "imaging_fft_precision": "f32",
                "imaging_fft_backend": "metal-mpsgraph",
                "parallel": False,
                "chanchunks": 4,
                "standard_mfs_grid_threads": "1",
                "standard_mfs_metal_minor_cycle_chunk": "auto:1000",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        env = plan["command"]["env"]
        self.assertEqual("2048", env["IMAGER_BENCH_IMAGING_MEMORY_TARGET_MB"])
        self.assertEqual("128", env["IMAGER_BENCH_IMAGING_PREPARE_BUFFER_MB"])
        self.assertEqual("4096", env["IMAGER_BENCH_IMAGING_ROW_BLOCK_ROWS"])
        self.assertEqual("3", env["IMAGER_BENCH_IMAGING_PREPARE_WORKERS"])
        self.assertEqual("2", env["IMAGER_BENCH_IMAGING_READ_AHEAD_BLOCKS"])
        self.assertEqual("f32", env["IMAGER_BENCH_IMAGING_FFT_PRECISION"])
        self.assertEqual("metal-mpsgraph", env["IMAGER_BENCH_IMAGING_FFT_BACKEND"])
        self.assertEqual("0", env["IMAGER_BENCH_PARALLEL"])
        self.assertEqual("4", env["IMAGER_BENCH_CHANCHUNKS"])
        self.assertEqual("f32", plan["mode"]["imaging_fft_precision"])
        self.assertEqual("metal-mpsgraph", plan["mode"]["imaging_fft_backend"])
        self.assertIs(False, plan["mode"]["parallel"])
        self.assertEqual(4, plan["mode"]["chanchunks"])
        self.assertEqual(2, plan["mode"]["imaging_read_ahead_blocks"])
        self.assertEqual("1", env["IMAGER_BENCH_STANDARD_MFS_GRID_THREADS"])
        self.assertEqual(
            "auto:1000", env["IMAGER_BENCH_STANDARD_MFS_METAL_MINOR_CYCLE_CHUNK"]
        )
        self.assertEqual("1", env["CASA_RS_STANDARD_MFS_PROFILE_DETAIL"])

    def test_casa_unlimited_nmajor_flows_to_the_matched_benchmark(self) -> None:
        manifest = {
            "id": "unlimited-major-cycles",
            "mode_id": "standard-mfs-clean",
            "dataset": {"key": "medium.ms", "path": "/tmp/medium.ms"},
            "imaging": {
                "mode": "clean",
                "specmode": "mfs",
                "gridder": "standard",
                "nmajor": -1,
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("-1", plan["command"]["env"]["IMAGER_BENCH_NMAJOR"])
        self.assertEqual(-1, plan["mode"]["nmajor"])

    def test_imaging_overrides_support_backend_sweeps(self) -> None:
        manifest = {
            "id": "backend-sweep",
            "mode_id": "standard-mfs-clean",
            "dataset": {
                "key": "medium.ms",
                "path": "/tmp/medium.ms",
            },
            "imaging": {
                "mode": "clean",
                "specmode": "mfs",
                "gridder": "standard",
                "deconvolver": "hogbom",
                "standard_mfs_acceleration": "auto",
                "niter": 500,
                "scales": [0, 5, 15],
            },
        }

        run_workload.apply_imaging_overrides(
            manifest,
            [
                "standard_mfs_acceleration=multi-cpu",
                "imaging_fft_precision=f32",
                "imaging_fft_backend=accelerate",
                "deconvolver=clark",
                "niter=1000",
                "pbcor=true",
                "scales=0,10,30",
                "mask_box=208,208,303,303",
                "fitspw=0:0~7;16~23",
                "fitorder=1",
                "save_continuum_residual=true",
            ],
        )
        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("multi-cpu", plan["mode"]["standard_mfs_acceleration"])
        self.assertEqual("f32", plan["mode"]["imaging_fft_precision"])
        self.assertEqual(
            "f32", plan["command"]["env"]["IMAGER_BENCH_IMAGING_FFT_PRECISION"]
        )
        self.assertEqual("accelerate", plan["mode"]["imaging_fft_backend"])
        self.assertEqual(
            "accelerate", plan["command"]["env"]["IMAGER_BENCH_IMAGING_FFT_BACKEND"]
        )
        self.assertEqual("clark", plan["mode"]["deconvolver"])
        self.assertEqual(1000, plan["mode"]["niter"])
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_PBCOR"])
        self.assertEqual("0,10,30", plan["command"]["env"]["IMAGER_BENCH_SCALES"])
        self.assertEqual(
            "208,208,303,303", plan["command"]["env"]["IMAGER_BENCH_MASK_BOX"]
        )
        self.assertEqual("0:0~7;16~23", plan["command"]["env"]["IMAGER_BENCH_FITSPW"])
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_FITORDER"])
        self.assertEqual(
            "1", plan["command"]["env"]["IMAGER_BENCH_SAVE_CONTINUUM_RESIDUAL"]
        )

        run_workload.apply_imaging_overrides(manifest, ["mask_box=null"])
        self.assertNotIn("mask_box", manifest["imaging"])

    def test_imaging_overrides_are_forbidden_for_frozen_recipe_evidence(self) -> None:
        manifest = {
            "casa": {"recipe_path": "recipe.last"},
            "imaging": {"mode": "dirty", "specmode": "mfs", "gridder": "awproject"},
        }

        with self.assertRaisesRegex(
            run_workload.HarnessError, "forbidden for frozen recipe-backed evidence"
        ):
            run_workload.apply_imaging_overrides(manifest, ["field=1526"])

    def test_recipe_backed_run_rejects_unbound_environment(self) -> None:
        manifest_path = run_workload.WORKLOAD_DIR / "vlass-fragment-smoke-cold.json"
        manifest = copy.deepcopy(run_workload.load_manifest(manifest_path))
        manifest["run"]["env"] = {"OMP_NUM_THREADS": "8"}

        with (
            mock.patch.dict(
                os.environ,
                {"CASA_RS_VLASS_DATA_ROOT": "/Volumes/GLENDENNING/test-data"},
                clear=False,
            ),
            self.assertRaisesRegex(run_workload.HarnessError, "run.env is unsupported"),
        ):
            run_workload.build_plan(
                manifest_path=manifest_path,
                manifest=manifest,
                repeats_override=None,
                run_label_override=None,
                storage_label_override=None,
                dry_run=True,
            )

    def test_recipe_dry_plan_uses_an_absolute_data_independent_placeholder(
        self,
    ) -> None:
        manifest_path = run_workload.WORKLOAD_DIR / "vlass-fragment-smoke-cold.json"
        manifest = run_workload.load_manifest(manifest_path)
        with mock.patch.dict(os.environ, {}, clear=True):
            plan = run_workload.build_plan(
                manifest_path=manifest_path,
                manifest=manifest,
                repeats_override=None,
                run_label_override=None,
                storage_label_override=None,
                dry_run=True,
            )

        dataset = Path(plan["dataset"]["path"])
        self.assertTrue(dataset.is_absolute())
        self.assertIn("/__casa_rs_dry_run__/CASA_RS_VLASS_DATA_ROOT/", str(dataset))

    def test_imaging_overrides_preserve_spectral_selector_strings(self) -> None:
        manifest = {
            "id": "cube-width",
            "mode_id": "cube-one-channel",
            "dataset": {
                "key": "medium.ms",
                "path": "/tmp/medium.ms",
            },
            "imaging": {
                "mode": "clean",
                "specmode": "cube",
                "gridder": "standard",
                "channel_count": 1,
            },
        }

        run_workload.apply_imaging_overrides(
            manifest,
            [
                "width=64",
                "start=0",
                "niter=1000",
            ],
        )
        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("64", plan["mode"]["width"])
        self.assertEqual("0", plan["mode"]["start"])
        self.assertEqual(1000, plan["mode"]["niter"])
        self.assertEqual("64", plan["command"]["env"]["IMAGER_BENCH_CUBE_WIDTH"])

    def test_parse_backend_plan_logs_extracts_shape_and_runtime_summary(self) -> None:
        parsed = run_workload.parse_benchmark_log(
            """single_plane_execution_plan spectral=mfs projection=standard deconvolver=single-term weighting=briggs output_channels=1 one_output_channel=true source_stream=bounded source_stream_memory=planner pb_products=false pb_requirement=none output_products=.image,.residual,.model,.psf,.sumwt cpu_multi_worker_eligible=true cpu_multi_worker_reason=standard-mfs-fixed-tile-workers-4 gpu_metal_eligible=true gpu_metal_reason=standard-mfs-grouped-metal stage_timing_attribution=frontend-core-product-stages standard_mfs_regression_sentinel=true
standard_mfs_runtime_plan policy=auto imaging_fft_backend=metal-mpsgraph eligible=true auto_multi_cpu=true auto_metal=true metal_device_available=true backend=fixed_tile backend_source=planner grid_threads=4 grid_threads_source=auto density_threads=4 density_threads_source=auto tile_anchor=center_quadrants tile_anchor_source=planner residual_backend=fixed_tile residual_backend_source=planner initial_dirty_backend=fixed_tile initial_dirty_backend_source=planner metal_grouped_input_cache=false metal_grouped_input_cache_source=planner mtmfs_metal_backend=false mtmfs_metal_input_cache=false
standard_mfs_memory_plan_actual source_stream=bounded execution_mode=fixed_tile_streaming rows_total=8192 selected_channels=64 row_block_rows=2048 row_block_rows_source=heuristic heuristic_row_block_rows=2048 worker_buffers=4 system_memory_bytes=34359738368 memory_target_bytes=8589934592 memory_target_source=cli total_budget_bytes=8589934592 planned_reserved_bytes=100 planned_active_bytes=200 reserve_over_budget_bytes=0 prepare_buffer_floor_applied=false prepare_buffer_bytes=100 image_working_set_bytes=100 weighting_density_bytes=100 gridded_visibility_bytes=100 output_image_bytes=100 fixed_tile_resident_bytes=100 fixed_tile_resident_limit=true fixed_tile_edge=512 fixed_tile_anchor=center max_live_row_blocks=1 live_row_block_bytes=100 live_bucket_bytes=100 visibility_row_channel_bytes=13 visibility_row_fixed_bytes=28 visibility_row_fixed_resident_bytes=120 visibility_row_cache_overhead_bytes=92 queued_task_bytes=100 resident_tile_buffer_bytes=100 global_grid_bytes=100 tile_cell_bin_bytes=100 worker_staging_bytes=100 gpu_staging_bytes=100 routed_replay_cache_bytes=0 routed_replay_cache_enabled=false metal_grouped_input_cache_bytes=0 metal_grouped_input_cache_enabled=false executor_plan_bytes_estimate=100 local_grid_bytes_estimate=100 peak_rss_bytes=0 product_status=planned
imaging_source_read_ahead_summary mode=standard_mfs enabled=true max_live_row_blocks=2 queue_capacity=0 live_row_block_high_water=2 row_blocks=4 row_block_rows=2048 consumer_recv_blocked_ms=11.000 producer_send_blocked_ms=2.000 producer_consumer_overlap_ms=15.000 source_read_ms=40.000 source_route_ms=30.000 consumer_ms=20.000 source_prepare_ms=50.000 source_bytes=10485760 effective_read_bandwidth_mib_s=250.000 streamed_samples=500000
dirty_product_fft_timing use_case=dirty_psf_residual requested_backend=metal-mpsgraph selected_backend=metal-mpsgraph fallback_used=false reason=metal_mpsgraph_complex_f32_host_batch_supported precision=f32 direction=inverse rows=1024 columns=1024 transforms=2 chunk_size=2 chunk_count=1 plan_cache_hit=true plan_ms=0.100 pack_ms=2.000 transfer_to_device_ms=3.000 exec_ms=4.000 device_exec_ms=3.500 transfer_from_device_ms=2.500 sync_ms=0.200 total_ms=12.000
mosaic_dirty_product_gpu_resident products=1 requested_backend=metal-mpsgraph selected_backend=metal-mpsgraph fallback_used=false reason=metal_mpsgraph_complex_f32_host_batch_supported plan_ms=0.100 pack_ms=2.000 transfer_to_device_ms=3.000 exec_ms=4.000 device_exec_ms=3.500 transfer_from_device_ms=2.500 sync_ms=0.200 postprocess_ms=0.600 total_ms=12.000
standard_mfs_profile_run run=1 workload_ms=/tmp/input.ms field_ids=Some([0]) phasecenter_field=None ddid=Some(0) spw=Some(0) channel_start=Some(0) channel_count=Some(64) spectral_mode=Mfs weighting=Briggs deconvolver=Hogbom nterms=1 imsize=1024 niter=500 dirty_only=false gridded_samples=500000 major_cycles=10 minor_iterations=500 thread_env=4 row_block_rows_env=auto prepare_workers_env=auto ms_read_threads_env=auto frontend_total_ms=1000.000 core_total_ms=800.000 prepare_plane_input_ms=100.000 source_read_ms=40.000 source_prepare_ms=60.000 weighting_ms=20.000 psf_grid_ms=300.000 residual_degrid_grid_ms=200.000 major_cycle_refresh_ms=150.000 peak_rss_bytes=123456 product_status=written
image_product_write suffix=.image role=image shape=1024x1024x1x1 elements=1048576 elapsed_ms=12.500
image_product_write suffix=.image.pbcor role=image.pbcor shape=1024x1024x1x1 elements=1048576 elapsed_ms=14.250
Rust release CLI timings (seconds):
  run=1 real=1.500
"""
        )
        parsed["backend_plan_logs"] = run_workload.parse_backend_plan_logs(
            """single_plane_execution_plan spectral=mfs projection=standard deconvolver=single-term weighting=briggs output_channels=1 one_output_channel=true source_stream=bounded source_stream_memory=planner pb_products=false pb_requirement=none output_products=.image,.residual,.model,.psf,.sumwt cpu_multi_worker_eligible=true cpu_multi_worker_reason=standard-mfs-fixed-tile-workers-4 gpu_metal_eligible=true gpu_metal_reason=standard-mfs-grouped-metal stage_timing_attribution=frontend-core-product-stages standard_mfs_regression_sentinel=true
standard_mfs_runtime_plan policy=auto imaging_fft_backend=metal-mpsgraph eligible=true auto_multi_cpu=true auto_metal=true metal_device_available=true backend=fixed_tile backend_source=planner grid_threads=4 grid_threads_source=auto density_threads=4 density_threads_source=auto tile_anchor=center_quadrants tile_anchor_source=planner residual_backend=fixed_tile residual_backend_source=planner initial_dirty_backend=fixed_tile initial_dirty_backend_source=planner metal_grouped_input_cache=false metal_grouped_input_cache_source=planner mtmfs_metal_backend=false mtmfs_metal_input_cache=false
standard_mfs_memory_plan_actual source_stream=bounded execution_mode=fixed_tile_streaming rows_total=8192 selected_channels=64 row_block_rows=2048 row_block_rows_source=heuristic heuristic_row_block_rows=2048 worker_buffers=4 system_memory_bytes=34359738368 memory_target_bytes=8589934592 memory_target_source=cli total_budget_bytes=8589934592 planned_reserved_bytes=100 planned_active_bytes=200 reserve_over_budget_bytes=0 prepare_buffer_floor_applied=false prepare_buffer_bytes=100 image_working_set_bytes=100 weighting_density_bytes=100 gridded_visibility_bytes=100 output_image_bytes=100 fixed_tile_resident_bytes=100 fixed_tile_resident_limit=true fixed_tile_edge=512 fixed_tile_anchor=center max_live_row_blocks=1 live_row_block_bytes=100 live_bucket_bytes=100 visibility_row_channel_bytes=13 visibility_row_fixed_bytes=28 visibility_row_fixed_resident_bytes=120 visibility_row_cache_overhead_bytes=92 queued_task_bytes=100 resident_tile_buffer_bytes=100 global_grid_bytes=100 tile_cell_bin_bytes=100 worker_staging_bytes=100 gpu_staging_bytes=100 routed_replay_cache_bytes=0 routed_replay_cache_enabled=false metal_grouped_input_cache_bytes=0 metal_grouped_input_cache_enabled=false executor_plan_bytes_estimate=100 local_grid_bytes_estimate=100 peak_rss_bytes=0 product_status=planned
imaging_source_read_ahead_summary mode=standard_mfs enabled=true max_live_row_blocks=2 queue_capacity=0 live_row_block_high_water=2 row_blocks=4 row_block_rows=2048 consumer_recv_blocked_ms=11.000 producer_send_blocked_ms=2.000 producer_consumer_overlap_ms=15.000 source_read_ms=40.000 source_route_ms=30.000 consumer_ms=20.000 source_prepare_ms=50.000 source_bytes=10485760 effective_read_bandwidth_mib_s=250.000 streamed_samples=500000
dirty_product_fft_timing use_case=dirty_psf_residual requested_backend=metal-mpsgraph selected_backend=metal-mpsgraph fallback_used=false reason=metal_mpsgraph_complex_f32_host_batch_supported precision=f32 direction=inverse rows=1024 columns=1024 transforms=2 chunk_size=2 chunk_count=1 plan_cache_hit=true plan_ms=0.100 pack_ms=2.000 transfer_to_device_ms=3.000 exec_ms=4.000 device_exec_ms=3.500 transfer_from_device_ms=2.500 sync_ms=0.200 total_ms=12.000
standard_mfs_profile_run run=1 workload_ms=/tmp/input.ms field_ids=Some([0]) phasecenter_field=None ddid=Some(0) spw=Some(0) channel_start=Some(0) channel_count=Some(64) spectral_mode=Mfs weighting=Briggs deconvolver=Hogbom nterms=1 imsize=1024 niter=500 dirty_only=false gridded_samples=500000 major_cycles=10 minor_iterations=500 thread_env=4 row_block_rows_env=auto prepare_workers_env=auto ms_read_threads_env=auto frontend_total_ms=1000.000 core_total_ms=800.000 prepare_plane_input_ms=100.000 source_read_ms=40.000 source_prepare_ms=60.000 weighting_ms=20.000 psf_grid_ms=300.000 residual_degrid_grid_ms=200.000 major_cycle_refresh_ms=150.000 peak_rss_bytes=123456 product_status=written
image_product_write suffix=.image role=image shape=1024x1024x1x1 elements=1048576 elapsed_ms=12.500
image_product_write suffix=.image.pbcor role=image.pbcor shape=1024x1024x1x1 elements=1048576 elapsed_ms=14.250
"""
        )
        plan = {
            "mode": {
                "specmode": "mfs",
                "gridder": "standard",
                "deconvolver": "hogbom",
                "weighting": "briggs",
                "standard_mfs_acceleration": "auto",
                "image_shape": [1024, 1024],
                "channel_count": 64,
                "nterms": 1,
                "niter": 500,
            },
            "comparison": {"products": [".image", ".residual", ".psf", ".model"]},
            "command": {"env": {"IMAGER_BENCH_MINOR_CYCLE_LENGTH": "50"}},
            "environment": {"runtime": {"physical_cores": 10, "logical_cores": 10}},
        }

        features = run_workload.build_benchmark_feature_summary(plan, parsed)

        summary = parsed["backend_plan_logs"]["summary"]
        self.assertEqual("fixed_tile", summary["resolved_backend"])
        self.assertEqual(4, summary["resolved_grid_threads"])
        self.assertEqual(28, summary["visibility_row_fixed_bytes"])
        self.assertEqual(120, summary["visibility_row_fixed_resident_bytes"])
        self.assertEqual(92, summary["visibility_row_cache_overhead_bytes"])
        self.assertEqual(8192, features["visibility"]["selected_rows"])
        self.assertEqual(64, features["visibility"]["selected_channels"])
        self.assertEqual(500000, features["visibility"]["gridded_samples"])
        self.assertEqual(1024 * 1024 * 4, features["image"]["image_work"])
        self.assertEqual(4, features["resources"]["row_block_count"])
        self.assertIs(True, summary["source_read_ahead_enabled"])
        self.assertEqual("standard_mfs", summary["source_read_ahead_mode"])
        self.assertEqual(["standard_mfs"], summary["source_read_ahead_modes"])
        self.assertEqual(1, summary["source_read_ahead_summary_count"])
        self.assertEqual(0, summary["source_read_ahead_queue_capacity"])
        self.assertEqual(2, summary["source_read_ahead_live_row_block_high_water"])
        self.assertEqual(
            15.0, summary["source_read_ahead_producer_consumer_overlap_ms"]
        )
        self.assertEqual(
            250.0, summary["source_read_ahead_effective_read_bandwidth_mib_s"]
        )
        self.assertEqual("metal-mpsgraph", summary["requested_imaging_fft_backend"])
        self.assertEqual(
            "metal-mpsgraph", summary["dirty_product_fft_selected_backend"]
        )
        self.assertEqual(
            "metal-mpsgraph", features["backend"]["dirty_product_fft_selected_backend"]
        )
        self.assertEqual(12.0, features["resources"]["dirty_product_fft_total_ms"])
        self.assertEqual(
            11.0, features["resources"]["source_read_ahead_consumer_recv_blocked_ms"]
        )
        self.assertEqual(
            50.0, features["resources"]["source_read_ahead_source_prepare_ms"]
        )
        self.assertEqual(8589934392, features["resources"]["memory_headroom_bytes"])
        self.assertEqual(2, summary["image_product_write_count"])
        self.assertEqual(
            {".image": 12.5, ".image.pbcor": 14.25},
            summary["image_product_write_ms_by_suffix"],
        )
        self.assertEqual(
            {".image": "1024x1024x1x1", ".image.pbcor": "1024x1024x1x1"},
            summary["image_product_write_shape_by_suffix"],
        )

    def test_parse_backend_plan_logs_ignores_unversioned_gridded_replay_summary(
        self,
    ) -> None:
        parsed = run_workload.parse_backend_plan_logs(
            """imaging_gridded_replay_summary ordinal=1 blocks=31985 artifact_bytes=1574840232 payload_bytes=1572537216 read_bytes=1574840232 read_operations=94909 payload_copy_bytes=0 payload_copy_operations=0 buffer_allocations=2 buffer_reuses=31983 source_slots=2 workers=1 worker_threads_started=0 dispatch_waves=127940 active_worker_slots=1 minimum_partitions_per_active_worker=127940 maximum_partitions_per_active_worker=127940 partitions_executed=127940 commits_completed=127940 planned_source_capacity_bytes=262288 planned_kernel_window_capacity_bytes=115140 planned_gridded_route_maximum_frame_records=4096 planned_gridded_route_maximum_frame_groups=4096 planned_gridded_route_capacity_bytes=114708 frames_routed=31985 encoded_records=49141788 routed_record_memberships=49141788 prediction_groups=49141788 degrid_records=49141788 grid_records=49141788 sector_rescans=0 peak_physical_route_capacity_bytes=114708 peak_partial_dynamic_capacity_bytes=0 peak_worker_stack_capacity_bytes=0 peak_kernel_window_capacity_bytes=115068 peak_live_source_blocks=2 peak_live_source_current_bytes=131216 peak_live_source_capacity_bytes=262288 ready_queue_high_water=0 producer_wait_nanos=1964086 consumer_wait_nanos=142338671 source_starved_nanos=142323713 overlap_nanos=1322028272 source_fill_nanos=1436830268 prepare_nanos=683595493 execute_nanos=4125092333 commit_nanos=3312497 wall_nanos=5008788834
"""
        )

        self.assertNotIn("imaging_gridded_replay", parsed)
        self.assertNotIn("imaging_gridded_replay", parsed["collection_stats"])
        receipt = canonical_workload_result(extra_results={"backend_plan_logs": parsed})
        run_workload.validate_run_result(
            receipt,
            source="gridded replay summary remains log-only",
        )

    def test_parse_backend_plan_logs_retains_t51_aw_receipts(self) -> None:
        parsed = run_workload.parse_backend_plan_logs(
            """imaging_aw_cache_inventory_summary paired_cells=1024 frequencies=16 w_values=32 mueller_elements=2 parallactic_angles=1 prepared_cache_bytes=9663676416 decoded_resident_ceiling_bytes=402653184
imaging_prepared_artifact_reader_summary catalog=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa logical_bytes=9663676416 decoded_ceiling_bytes=402653184 total_ceiling_bytes=402718720 reads=4 read_bytes=1048576 read_operations=32 resident_peak_bytes=262144 total_peak_resident_bytes=327680 pinned_peak_bytes=131072 hits=7 loads=4 evicted_bytes=0 copied_bytes=262144 aborted=false
"""
        )

        self.assertEqual(
            1024, parsed["aw_cache_inventory"][0]["fields"]["paired_cells"]
        )
        self.assertEqual(
            1048576,
            parsed["prepared_artifact_readers"][0]["fields"]["read_bytes"],
        )
        receipt = canonical_workload_result(extra_results={"backend_plan_logs": parsed})
        run_workload.validate_run_result(receipt, source="T51 AW runtime receipts")

    def test_mosaic_resident_product_diagnostic_is_parsed(self) -> None:
        buckets = run_workload.parse_backend_plan_logs(
            "mosaic_dirty_product_gpu_resident products=1 "
            "requested_backend=metal-mpsgraph selected_backend=metal-mpsgraph "
            "fallback_used=false reason=metal_mpsgraph_complex_f32_host_batch_supported "
            "plan_ms=0.100 pack_ms=2.000 transfer_to_device_ms=3.000 "
            "exec_ms=4.000 device_exec_ms=3.500 transfer_from_device_ms=2.500 "
            "sync_ms=0.200 postprocess_ms=0.600 total_ms=12.000\n"
        )

        self.assertEqual(1, len(buckets["dirty_product_gpu_resident"]))
        summary = buckets["summary"]
        self.assertEqual(
            "metal-mpsgraph",
            summary["dirty_product_gpu_resident_selected_backend"],
        )
        self.assertEqual(
            "metal-mpsgraph",
            summary["dirty_product_gpu_resident_requested_backend"],
        )
        self.assertIs(False, summary["dirty_product_gpu_resident_fallback_used"])
        self.assertEqual(2.0, summary["dirty_product_gpu_resident_pack_ms"])
        self.assertEqual(
            3.5,
            summary["dirty_product_gpu_resident_device_exec_ms"],
        )
        self.assertEqual(
            2.5,
            summary["dirty_product_gpu_resident_transfer_from_device_ms"],
        )
        self.assertEqual(0.6, summary["dirty_product_gpu_resident_postprocess_ms"])
        self.assertEqual(12.0, summary["dirty_product_gpu_resident_total_ms"])

    def test_backend_diagnostic_samples_are_bounded_without_losing_counts(self) -> None:
        line_count = run_workload.MAX_BACKEND_LOG_ENTRIES_PER_BUCKET + 73
        parsed = run_workload.parse_backend_plan_logs(
            "\n".join(
                f"standard_mfs_metal_residual_refresh run={index} dispatch_ms=1.0"
                for index in range(line_count)
            )
        )

        retained = parsed["metal_diagnostics"]
        stats = parsed["collection_stats"]["metal_diagnostics"]
        self.assertEqual(run_workload.MAX_BACKEND_LOG_ENTRIES_PER_BUCKET, len(retained))
        self.assertEqual(line_count, stats["observed_count"])
        self.assertEqual(
            run_workload.MAX_BACKEND_LOG_ENTRIES_PER_BUCKET, stats["retained_count"]
        )
        self.assertIs(True, stats["truncated"])
        self.assertEqual(line_count, parsed["summary"]["metal_diagnostic_count"])
        self.assertEqual(0, retained[0]["fields"]["run"])
        self.assertEqual(line_count - 1, retained[-1]["fields"]["run"])

    def test_source_read_ahead_summary_aggregates_multi_slab_logs(self) -> None:
        parsed = run_workload.parse_backend_plan_logs(
            """imaging_source_read_ahead_summary mode=cube_slab enabled=true max_live_row_blocks=2 queue_capacity=0 live_row_block_high_water=2 row_blocks=8 row_block_rows=4096 consumer_recv_blocked_ms=10.000 producer_send_blocked_ms=1.000 producer_consumer_overlap_ms=1.500 source_read_ms=20.000 source_route_ms=0.000 consumer_ms=2.000 source_prepare_ms=2.000 source_bytes=5242880 effective_read_bandwidth_mib_s=250.000 streamed_samples=100
imaging_source_read_ahead_summary mode=cube_slab enabled=true max_live_row_blocks=2 queue_capacity=0 live_row_block_high_water=2 row_blocks=7 row_block_rows=4096 consumer_recv_blocked_ms=12.000 producer_send_blocked_ms=3.000 producer_consumer_overlap_ms=2.500 source_read_ms=22.000 source_route_ms=0.000 consumer_ms=4.000 source_prepare_ms=4.000 source_bytes=5242880 effective_read_bandwidth_mib_s=227.273 streamed_samples=200
"""
        )

        summary = parsed["summary"]

        self.assertEqual(["cube_slab"], summary["source_read_ahead_modes"])
        self.assertEqual(2, summary["source_read_ahead_summary_count"])
        self.assertEqual(15, summary["source_read_ahead_row_blocks"])
        self.assertEqual(22.0, summary["source_read_ahead_consumer_recv_blocked_ms"])
        self.assertEqual(4.0, summary["source_read_ahead_producer_send_blocked_ms"])
        self.assertEqual(4.0, summary["source_read_ahead_producer_consumer_overlap_ms"])
        self.assertEqual(2, summary["source_read_ahead_live_row_block_high_water"])
        self.assertEqual(42.0, summary["source_read_ahead_source_read_ms"])
        self.assertEqual(6.0, summary["source_read_ahead_consumer_ms"])
        self.assertEqual(6.0, summary["source_read_ahead_source_prepare_ms"])
        self.assertEqual(300, summary["source_read_ahead_streamed_samples"])
        self.assertEqual(10485760, summary["source_read_ahead_source_bytes"])
        self.assertAlmostEqual(
            238.095238,
            summary["source_read_ahead_effective_read_bandwidth_mib_s"],
            places=6,
        )

    def test_completed_run_promotes_enriched_benchmark_features(self) -> None:
        plan = {
            "run_id": "test-run",
            "run": {"stream_log": False},
            "review": {},
            "mode": {
                "bench_mode": "clean",
                "specmode": "mfs",
                "gridder": "standard",
                "deconvolver": "hogbom",
                "weighting": "briggs",
                "standard_mfs_acceleration": "auto",
                "image_shape": [1024, 1024],
                "channel_count": 64,
                "nterms": 1,
                "niter": 500,
            },
            "comparison": {"products": [".image", ".residual"]},
            "command": {
                "argv": ["bench"],
                "env": {"IMAGER_BENCH_MINOR_CYCLE_LENGTH": "50"},
            },
            "environment": {"runtime": {"physical_cores": 10, "logical_cores": 10}},
            "products": {},
            "benchmark_features": {
                "backend": {"resolved_backend": None},
                "visibility": {"selected_rows": None},
            },
        }
        output = """Rust release CLI timings (seconds):
  run=1 real=1.500
  median=1.500
Rust stage medians (milliseconds):
  stage medians (ms):
    prepare_plane_input=100.000
    get_ms_values_into_processing_buffer=40.000
    prepare_processing_buffer=60.000
    total=800.000
single_plane_execution_plan spectral=mfs projection=standard deconvolver=single-term weighting=briggs output_channels=1 one_output_channel=true source_stream=bounded source_stream_memory=planner pb_products=false pb_requirement=none output_products=.image,.residual cpu_multi_worker_eligible=true cpu_multi_worker_reason=standard-mfs-fixed-tile-workers-4 gpu_metal_eligible=false gpu_metal_reason=metal-device-unavailable
standard_mfs_runtime_plan policy=auto eligible=true auto_multi_cpu=true auto_metal=false metal_device_available=false backend=fixed_tile backend_source=planner grid_threads=4 grid_threads_source=auto tile_anchor=center_quadrants tile_anchor_source=planner residual_backend=cpu residual_backend_source=planner initial_dirty_backend=cpu initial_dirty_backend_source=planner metal_grouped_input_cache=planner metal_grouped_input_cache_source=planner
mosaic_cube_slab_plan schedule=slab_first executor_capabilities=mosaic_multi_plane_stream nplanes=8 active_planes=4 slab_count=2 worker_count=4 source_reuse=shared_selection_per_plane_source_stream product_state=product_backed_write_through
standard_mfs_memory_plan_actual source_stream=bounded execution_mode=fixed_tile_streaming rows_total=8192 selected_channels=64 row_block_rows=2048 memory_target_bytes=8589934592 planned_active_bytes=200 source_stream_buffer_bytes=393216 live_row_block_bytes=131072 live_bucket_bytes=65536 visibility_row_channel_bytes=13 visibility_row_fixed_bytes=28 visibility_row_fixed_resident_bytes=120 visibility_row_cache_overhead_bytes=92 modeled_source_read_bytes=123456
imaging_source_read_ahead_summary mode=cube_slab enabled=false max_live_row_blocks=1 queue_capacity=0 live_row_block_high_water=1 row_blocks=4 row_block_rows=2048 consumer_recv_blocked_ms=0.000 producer_send_blocked_ms=0.000 producer_consumer_overlap_ms=0.000 source_read_ms=40.000 source_route_ms=30.000 consumer_ms=20.000 source_prepare_ms=50.000 source_bytes=10485760 effective_read_bandwidth_mib_s=250.000 streamed_samples=500000
standard_mfs_profile_run run=1 gridded_samples=500000 major_cycles=10 minor_iterations=500 peak_rss_bytes=123456
"""

        with tempfile.TemporaryDirectory() as tempdir:
            with (
                mock.patch(
                    "run_workload.run_benchmark_command",
                    return_value=run_workload.subprocess.CompletedProcess(
                        ["bench"], 0, output, None
                    ),
                ),
                mock.patch(
                    "run_workload.compare_products",
                    return_value={
                        "status": "skipped",
                        "reason": "test",
                        "products": {},
                    },
                ),
            ):
                bundle = run_workload.run_plan(plan, Path(tempdir) / "bench.log")

        self.assertEqual("completed", bundle["status"])
        self.assertEqual(
            "fixed_tile", bundle["benchmark_features"]["backend"]["resolved_backend"]
        )
        self.assertEqual(
            "mosaic_multi_plane_stream",
            bundle["benchmark_features"]["backend"][
                "mosaic_cube_slab_executor_capabilities"
            ],
        )
        self.assertEqual(
            4,
            bundle["benchmark_features"]["backend"]["mosaic_cube_slab_worker_count"],
        )
        self.assertEqual(
            8192, bundle["benchmark_features"]["visibility"]["selected_rows"]
        )
        self.assertEqual(
            393216,
            bundle["results"]["backend_plan_logs"]["summary"][
                "source_stream_buffer_bytes"
            ],
        )
        self.assertIs(
            False,
            bundle["benchmark_features"]["resources"]["source_read_ahead_enabled"],
        )
        self.assertEqual(
            "cube_slab",
            bundle["benchmark_features"]["resources"]["source_read_ahead_mode"],
        )
        self.assertEqual(
            123456,
            bundle["results"]["backend_plan_logs"]["summary"][
                "modeled_source_read_bytes"
            ],
        )
        self.assertEqual(
            120,
            bundle["results"]["backend_plan_logs"]["summary"][
                "visibility_row_fixed_resident_bytes"
            ],
        )
        self.assertEqual(
            bundle["results"]["benchmark_features"],
            bundle["benchmark_features"],
        )

    def test_stream_log_enables_imager_progress_for_subprocess(self) -> None:
        plan = {
            "run_id": "test-run",
            "run": {"stream_log": True},
            "review": {},
            "mode": {
                "bench_mode": "dirty",
                "specmode": "cube",
                "gridder": "standard",
                "deconvolver": "hogbom",
                "weighting": "natural",
                "standard_mfs_acceleration": "auto",
                "image_shape": [1024, 1024],
                "channel_count": 64,
                "nterms": 1,
                "niter": 0,
            },
            "comparison": {"products": [".image"]},
            "command": {"argv": ["bench"], "env": {}},
            "environment": canonical_test_environment(),
            "products": {},
            "benchmark_features": {},
        }

        with tempfile.TemporaryDirectory() as tempdir:
            with mock.patch(
                "run_workload.run_benchmark_command",
                return_value=run_workload.subprocess.CompletedProcess(
                    ["bench"], 1, "Error: stopped\n", None
                ),
            ) as run_command:
                run_workload.run_plan(plan, Path(tempdir) / "bench.log")

        _, kwargs = run_command.call_args
        self.assertTrue(kwargs["stream_log"])
        self.assertEqual("1", kwargs["env"]["IMAGER_BENCH_STREAM_LOG"])
        self.assertEqual("1", kwargs["env"]["CASA_RS_IMAGING_PROGRESS"])

    def test_parse_backend_plan_logs_extracts_multiscale_minor_cycle_summary(
        self,
    ) -> None:
        parsed = run_workload.parse_backend_plan_logs(
            """standard_mfs_multiscale_minor_cycle_summary backend=cpu updates=100 reported_budget=100 stop_reason=Some(IterationLimitReached) scale_component_counts=0:0,1:0,2:100 peak_searches=303 full_window_peak_searches=9 subtract_updates=300 full_window_subtract_updates=3 pixels_searched=297682758 pixels_touched=294237664 peak_search_window_pixels_max=1048576 subtract_window_pixels_max=1048576 peak_search_ms=432.517 model_update_ms=0.327 subtract_ms=402.430 total_ms=831.951
"""
        )

        self.assertEqual(1, len(parsed["minor_cycle_diagnostics"]))
        self.assertEqual(1, len(parsed["multiscale_minor_cycle_diagnostics"]))
        fields = parsed["multiscale_minor_cycle_diagnostics"][0]["fields"]
        self.assertEqual("0:0,1:0,2:100", fields["scale_component_counts"])
        self.assertEqual(9, fields["full_window_peak_searches"])
        self.assertEqual(1048576, fields["subtract_window_pixels_max"])

    def test_parse_backend_plan_logs_extracts_spectral_slab_events(self) -> None:
        parsed = run_workload.parse_backend_plan_logs(
            """spectral_slab_plan schedule=source_first best_modeled_schedule=source_first executor_capabilities=full_slab_no_output_spill nplanes=8 image_shape=512x512 active_planes=8 slab_count=1 row_block_rows=32768 cache_budget_bytes=1048576 cache_kind=geometry_only visibility_cache_policy=disabled prepared_residency=row_block_stream visibility_cache_bytes=0 visibility_cache_source_channels=0 worker_count=4 backend=cpu_slab memory_target_bytes=536870912 fixed_frontend_bytes=33554432 source_stream_buffer_bytes=16777216 worker_staging_bytes=16777216 per_plane_state_bytes=2102272 component_memory_bytes=residual:1048576,psf:1048576 visibility_staging_bytes_per_plane=0 prepared_visibility_staging_bytes=0 live_prepared_visibility_bytes=888888 live_bucket_bytes=444444 product_scratch_bytes=3145728 product_batch_planes=2 gpu_staging_bytes=0 safety_margin_bytes=0 planned_active_bytes=456789 source_channel_visits=16 max_slab_source_channels=8 full_source_channel_count=16 source_cell_channel_count=64 corr_count=4 visibility_data_element_bytes=8 data_channel_read_granularity=requested_range flag_channel_read_granularity=full_cell weight_spectrum_channel_read_granularity=requested_range visibility_row_channel_bytes=52 visibility_row_fixed_bytes=184 visibility_row_fixed_resident_bytes=928 visibility_row_cache_overhead_bytes=744 visibility_resident_cache_layout=uvw,weight,field,spw,pol,is_cross,channel_origin,spectral_route best_modeled_total_io_bytes=7777 best_modeled_source_read_bytes=700 best_modeled_visibility_cache_io_bytes=0 best_modeled_output_spill_io_bytes=78 best_modeled_product_write_bytes=6999 best_modeled_active_planes=8 best_modeled_slab_count=1 best_modeled_source_channel_visits=16 modeled_total_io_bytes=7777 modeled_source_read_bytes=700 modeled_visibility_cache_fill_bytes=0 modeled_visibility_cache_read_bytes=0 modeled_visibility_cache_io_bytes=0 modeled_output_spill_read_bytes=39 modeled_output_spill_write_bytes=39 modeled_output_spill_io_bytes=78 modeled_product_write_bytes=6999 modeled_no_cache_source_read_bytes=2000 modeled_full_cache_source_read_bytes=700 visibility_cache_saved_read_bytes=1300 candidate_io_costs=source_first:total=7777,source=700,cache=0,spill=78,product=6999,product_groups=4,active_planes=8,slab_count=1,row_block_rows=32768,cache_policy=disabled,residency=row_block_stream,executable=true;hybrid:total=9999,source=1000,cache=3000,spill=0,product=5999,product_groups=4,active_planes=4,slab_count=2,row_block_rows=32768,cache_policy=full_source,residency=row_block_stream,executable=true warnings=none
mosaic_cube_slab_plan schedule=slab_first executor_capabilities=mosaic_multi_plane_stream nplanes=8 active_planes=4 slab_count=2 worker_count=4 source_reuse=shared_selection_per_plane_source_stream product_state=product_backed_write_through
cube_per_plane_backend_summary phase=clean_deconvolution output_planes=8 plane_worker_count=4 per_plane_grid_threads=2 policy=multi-cpu selected_backend=wave3_fixed_tile_cpu fixed_tile_cpu_eligible=true metal_eligible=false metal_device_available=false deconvolver=Hogbom fallback_reasons=metal_device_unavailable
spectral_slab_event mode=cube pass_kind=initial_dirty stage=source_read slab_id=0 plane_start=0 plane_end=4 row_block_rows=32768 bytes_read=2048 bytes_written=unset worker_count=4 backend=cpu_slab elapsed_ms=12 estimated_resident_bytes=123456
spectral_slab_memory mode=cube stage=after_slab_prepare slab_id=0 plane_start=0 plane_end=4 current_rss_bytes=1000000 peak_rss_bytes=1200000 delta_from_baseline_bytes=400000 delta_from_previous_bytes=300000 estimated_resident_bytes=123456 planned_active_bytes=456789 visibility_staging_bytes=222222 plane_state_bytes=111111 product_scratch_bytes=333333 cache_budget_bytes=1048576 note=prepared
spectral_slab_memory mode=cube stage=after_slab_run slab_id=0 plane_start=0 plane_end=4 current_rss_bytes=1500000 peak_rss_bytes=1700000 delta_from_baseline_bytes=900000 delta_from_previous_bytes=500000 estimated_resident_bytes=223456 planned_active_bytes=456789 visibility_staging_bytes=222222 plane_state_bytes=111111 product_scratch_bytes=333333 cache_budget_bytes=1048576 note=run_cube
mosaic_cube_slab_plane plane=0 batch=0 batch_tasks=4 worker_slot=1 worker_elapsed_ms=90.000 publish_elapsed_ms=7.500
mosaic_cube_slab_executor_summary slab_id=0 plane_start=0 plane_end=4 worker_count=4 completed=4 elapsed_ms=111.000 worker_sum_ms=300.000 worker_max_ms=90.000 product_write_ms=33.000 residency=ordered_plane_results
cube_shared_direct_plane_executor_summary slab_plane_start=0 slab_plane_end=4 worker_count=4 product_batch_planes=2 completed=4 elapsed_ms=100.000 worker_sum_ms=300.000 worker_max_ms=90.000 result_wait_ms=10.000 consume_ms=20.000 product_write_ms=30.000 product_role_ms=29.000 product_psf_ms=10.000 product_residual_ms=11.000 product_model_ms=0.000 product_image_ms=0.000 product_sumwt_ms=1.000 product_bytes=1024 product_groups=2 product_group_planes=4 writer_groups=2 writer_planes=4 writer_estimated_bytes=1024 tiled_c_order_calls=4 tiled_fortran_calls=2 tiled_tile_visits=128 tiled_copied_elements=2048 tiled_lru_hits=1 tiled_lru_misses=2 tiled_lru_zero_fill_tiles=64 tiled_lru_read_tiles=0 tiled_lru_read_bytes=0 tiled_lru_dirty_evictions=0 tiled_lru_flush_calls=0 tiled_lru_flush_write_tiles=0 tiled_lru_flush_write_bytes=0 tiled_lru_batch_flushes=2 tiled_lru_batch_flush_tiles=64 tiled_lru_batch_flush_bytes=4096 tiled_direct_write_calls=2 tiled_direct_write_tiles=128 tiled_direct_write_bytes=8192 tiled_direct_pack_ns=1200 tiled_direct_swap_ns=0 tiled_direct_write_ns=3400 tiled_flat_allocations=0 tiled_flat_allocated_bytes=0 tiled_flat_zero_fill_bytes=0 tiled_flat_bulk_read_bytes=0 tiled_flat_flush_calls=0 tiled_flat_flush_write_tiles=0 tiled_flat_flush_write_bytes=0 residency=streaming_plane_results
cube_resident_clean_stage_summary result_wait_ms=10.000 consume_ms=20.000 worker_sum_ms=700.000 worker_max_ms=200.000 controller_overhead_ms=30.000 weighting_ms=0.000 executor_build_ms=0.000 psf_grid_alloc_ms=0.000 planned_sample_replay_ms=0.000 grid_update_ms=0.000 psf_grid_ms=0.000 psf_fft_ms=0.000 psf_image_correction_ms=0.000 psf_normalize_ms=0.000 model_fft_ms=0.000 residual_grid_alloc_ms=0.000 residual_degrid_grid_ms=0.000 residual_fft_ms=0.000 residual_image_correction_ms=0.000 residual_normalize_ms=0.000 clean_cycle_setup_ms=0.000 deconvolver_setup_ms=0.000 minor_cycle_ms=40.000 minor_cycle_solve_ms=500.000 major_cycle_refresh_ms=50.000 residual_refresh_overhead_ms=5.000 multiscale_scale_refresh_ms=0.000 beam_fit_ms=0.000 restore_ms=60.000 total_ms=680.000
cube_resident_clean_finish_plane plane=0 blocks=10 skipped_minor_cycle=false gridded_samples=100 initial_peak=1.000000000e+00 final_peak=2.000000000e-01 trace_final_peak=3.000000000e-01 cycle_threshold=5.000000000e-01 stop_reason=Some(CycleThresholdReached) minor_iterations=7 minor_cycle_count=2 actual_updates=7 reported_updates=8 model_nonzero_pixels=3 model_sum_abs_jy=4.500000000e+00 model_peak_abs_jy=2.500000000e+00 prepare_ms=1.000 finish_ms=2.000 replay=[]
cube_resident_clean_finish_plane plane=1 blocks=10 skipped_minor_cycle=true gridded_samples=100 initial_peak=4.000000000e-01 final_peak=4.000000000e-01 trace_final_peak=4.000000000e-01 cycle_threshold=5.000000000e-01 stop_reason=Some(GlobalThresholdReached) minor_iterations=0 minor_cycle_count=0 actual_updates=0 reported_updates=0 model_nonzero_pixels=0 model_sum_abs_jy=0.000000000e+00 model_peak_abs_jy=0.000000000e+00 prepare_ms=1.000 finish_ms=2.000 replay=[]
standard_mfs_clean_residual_refresh_summary deconvolver=Clark residual_backend=metal-row-run-grouped refresh_ms=13.000 accounted_ms=11.500 overhead_ms=1.500 model_fft_ms=2.000 residual_degrid_grid_ms=6.000 residual_fft_ms=2.500 residual_normalize_ms=1.000 fixed_tile_use_planned_run_blocks=true metal_grouped_input_cache=true materialized_sample_plan_budget_bytes=1048576
standard_mfs_metal_row_run_grouped_residual_refresh chunks=2 chunk_lane_capacity=1048576 group_tile_edge=32 runs=3 logical_lanes=4 group_descs=5 lane_refs=6 input_cache_hit=true input_cache_fill=false input_cache_chunks=7 input_cache_host_bytes=8192 prepare_plus_dispatch_ms=12.500 dispatch_wait_ms=8.000 dispatch_gpu_ms=3.000 dispatch_kernel_ms=2.000 readback_ms=1.500
standard_mfs_metal_row_run_grouped_residual_refresh_detail model_pack_ms=0.100 model_buffer_ms=0.200 density_buffer_ms=0.300 grid_buffer_ms=0.400 replay_ms=0.500 append_total_ms=0.600 dispatch_input_buffers_ms=0.700 dispatch_params_buffer_ms=0.800 dispatch_encode_ms=0.900 dispatch_wait_ms=8.000 dispatch_gpu_ms=3.000 dispatch_kernel_ms=2.000 readback_ms=1.500 staged_bytes=4096 candidate_tap_visits=10 candidate_model_reads=20 exact_candidate_grid_atomic_adds=30 grouped_candidate_grid_atomic_adds=40 grouped_candidate_scan_tests=50 unsupported_runs=0 input_cache_hit=true input_cache_fill=false input_cache_chunks=7 input_cache_host_bytes=8192
standard_mfs_metal_row_run_grouped_append_detail setup_ms=0.010 lane_push_ms=0.020 data_flag_copy_ms=0.030 run_desc_ms=0.040 group_assign_ms=0.050 group_finalize_ms=0.060
cube_plane_state_store_summary kind=product_backed_write_through slab_id=0 plane_start=0 plane_end=4 planes=4 bytes_read=0 bytes_written=1024 elapsed_ms=30.000 cleanup_policy=drop_after_write product_write_state=written components=psf,residual,image,sumwt
visibility_geometry_cache_summary enabled=true budget_bytes=1048576 resident_bytes=2048 entries=1 fills=1 hits=3 misses=1 shares=3 bypasses=0 rejected_model_dependent=0 elapsed_ms=42.000
cube_slab_executor_limitation materialization=full_prepared_slab planner_row_block_rows=687 inner_prepare_row_block_rows=executor_default reason=small_planner_row_blocks_are_only_valid_for_streaming_consumers
cube_source_row_blocks rows_total=3086235 row_block_rows=32768 row_block_rows_source=shape-planner source_channels=62 prepared_samples=191346570 blocks=95 visibility_batches=5890 visibility_capacity=212952516 visibility_capacity_surplus=21605946 visibility_capacity_bytes=13628961024 density_samples=0 density_batches=0 density_capacity=0 density_capacity_surplus=0 model_samples=0 model_batches=0 model_capacity=0 model_capacity_surplus=0 geometry_columns_ms=0.026 read_wall_ms=9156.943 read_data_ms=4172.764 read_flag_ms=2464.324 read_weight_ms=501.464 read_weight_spectrum_ms=0.000 read_geometry_ms=516.441 prepare_ms=11355.156 merge_ms=0.733 wall_ms=20525.010
"""
        )

        self.assertEqual(1, len(parsed["spectral_slab_plans"]))
        self.assertEqual(1, len(parsed["mosaic_cube_slab_plans"]))
        self.assertEqual(1, len(parsed["spectral_slab_events"]))
        self.assertEqual(2, len(parsed["spectral_slab_memory"]))
        self.assertEqual(8, parsed["summary"]["spectral_active_planes"])
        self.assertEqual(1, parsed["summary"]["spectral_slab_count"])
        self.assertEqual("source_first", parsed["summary"]["spectral_schedule"])
        self.assertEqual("slab_first", parsed["summary"]["mosaic_cube_slab_schedule"])
        self.assertEqual(
            "mosaic_multi_plane_stream",
            parsed["summary"]["mosaic_cube_slab_executor_capabilities"],
        )
        self.assertEqual(8, parsed["summary"]["mosaic_cube_slab_nplanes"])
        self.assertEqual(4, parsed["summary"]["mosaic_cube_slab_active_planes"])
        self.assertEqual(2, parsed["summary"]["mosaic_cube_slab_count"])
        self.assertEqual(4, parsed["summary"]["mosaic_cube_slab_worker_count"])
        self.assertEqual(1, parsed["summary"]["mosaic_cube_slab_plane_count"])
        self.assertEqual(7.5, parsed["summary"]["mosaic_cube_slab_plane_publish_ms"])
        self.assertEqual(
            1, parsed["summary"]["mosaic_cube_slab_executor_summary_count"]
        )
        self.assertEqual(
            111.0, parsed["summary"]["mosaic_cube_slab_executor_elapsed_ms"]
        )
        self.assertEqual(
            300.0, parsed["summary"]["mosaic_cube_slab_executor_worker_sum_ms"]
        )
        self.assertEqual(
            90.0, parsed["summary"]["mosaic_cube_slab_executor_worker_max_ms"]
        )
        self.assertEqual(33.0, parsed["summary"]["mosaic_cube_slab_product_write_ms"])
        self.assertEqual(
            500.0, parsed["summary"]["cube_resident_clean_minor_cycle_solve_ms"]
        )
        self.assertEqual(
            50.0, parsed["summary"]["cube_resident_clean_major_cycle_refresh_ms"]
        )
        self.assertEqual(680.0, parsed["summary"]["cube_resident_clean_core_total_ms"])
        self.assertEqual(2, parsed["summary"]["cube_resident_clean_finish_plane_count"])
        self.assertEqual(
            1, parsed["summary"]["cube_resident_clean_finish_cleaned_plane_count"]
        )
        self.assertEqual(
            1, parsed["summary"]["cube_resident_clean_finish_skipped_plane_count"]
        )
        self.assertEqual(7, parsed["summary"]["cube_resident_clean_actual_updates"])
        self.assertEqual(8, parsed["summary"]["cube_resident_clean_reported_updates"])
        self.assertEqual(2, parsed["summary"]["cube_resident_clean_trace_minor_cycles"])
        self.assertEqual(
            7, parsed["summary"]["cube_resident_clean_minor_iterations_from_planes"]
        )
        self.assertEqual(
            7, parsed["summary"]["cube_resident_clean_max_actual_updates_per_plane"]
        )
        self.assertEqual(
            3, parsed["summary"]["cube_resident_clean_model_nonzero_pixels"]
        )
        self.assertEqual(
            1, parsed["summary"]["cube_resident_clean_model_nonzero_planes"]
        )
        self.assertEqual(
            0, parsed["summary"]["cube_resident_clean_skipped_model_nonzero_planes"]
        )
        self.assertEqual(4.5, parsed["summary"]["cube_resident_clean_model_sum_abs_jy"])
        self.assertEqual(
            2.5, parsed["summary"]["cube_resident_clean_model_peak_abs_jy"]
        )
        self.assertEqual(
            "Some(CycleThresholdReached):1,Some(GlobalThresholdReached):1",
            parsed["summary"]["cube_resident_clean_stop_reason_counts"],
        )
        self.assertEqual(4, parsed["summary"]["metal_diagnostic_count"])
        self.assertEqual(1, parsed["summary"]["clean_residual_refresh_calls"])
        self.assertEqual(
            "metal-row-run-grouped",
            parsed["summary"]["clean_residual_refresh_backend"],
        )
        self.assertEqual(13.0, parsed["summary"]["clean_residual_refresh_ms"])
        self.assertEqual(11.5, parsed["summary"]["clean_residual_refresh_accounted_ms"])
        self.assertEqual(
            6.0,
            parsed["summary"]["clean_residual_refresh_residual_degrid_grid_ms"],
        )
        self.assertEqual(1, parsed["summary"]["metal_residual_refresh_calls"])
        self.assertEqual(
            12.5, parsed["summary"]["metal_residual_refresh_prepare_plus_dispatch_ms"]
        )
        self.assertEqual(
            8.0, parsed["summary"]["metal_residual_refresh_dispatch_wait_ms"]
        )
        self.assertEqual(
            3.0, parsed["summary"]["metal_residual_refresh_dispatch_gpu_ms"]
        )
        self.assertEqual(4096, parsed["summary"]["metal_residual_refresh_staged_bytes"])
        self.assertEqual(
            1, parsed["summary"]["metal_residual_refresh_input_cache_hits"]
        )
        self.assertEqual(
            0, parsed["summary"]["metal_residual_refresh_input_cache_fills"]
        )
        self.assertEqual(
            0.05, parsed["summary"]["metal_grouped_append_group_assign_ms"]
        )
        self.assertEqual(
            "shared_selection_per_plane_source_stream",
            parsed["summary"]["mosaic_cube_slab_source_reuse"],
        )
        self.assertEqual(
            "product_backed_write_through",
            parsed["summary"]["mosaic_cube_slab_product_state"],
        )
        self.assertEqual(
            "source_first", parsed["summary"]["spectral_best_modeled_schedule"]
        )
        self.assertEqual(
            "full_slab_no_output_spill",
            parsed["summary"]["spectral_executor_capabilities"],
        )
        self.assertEqual(1048576, parsed["summary"]["spectral_cache_budget_bytes"])
        self.assertEqual(
            "disabled", parsed["summary"]["spectral_visibility_cache_policy"]
        )
        self.assertEqual(
            "row_block_stream", parsed["summary"]["spectral_prepared_residency"]
        )
        self.assertEqual(0, parsed["summary"]["spectral_visibility_cache_bytes"])
        self.assertEqual(16, parsed["summary"]["spectral_source_channel_visits"])
        self.assertEqual(16, parsed["summary"]["spectral_full_source_channel_count"])
        self.assertEqual(64, parsed["summary"]["spectral_source_cell_channel_count"])
        self.assertEqual(52, parsed["summary"]["spectral_visibility_row_channel_bytes"])
        self.assertEqual(184, parsed["summary"]["spectral_visibility_row_fixed_bytes"])
        self.assertEqual(
            928, parsed["summary"]["spectral_visibility_row_fixed_resident_bytes"]
        )
        self.assertEqual(
            744, parsed["summary"]["spectral_visibility_row_cache_overhead_bytes"]
        )
        self.assertEqual(
            "uvw,weight,field,spw,pol,is_cross,channel_origin,spectral_route",
            parsed["summary"]["spectral_visibility_resident_cache_layout"],
        )
        self.assertEqual(
            "requested_range",
            parsed["summary"]["spectral_data_channel_read_granularity"],
        )
        self.assertEqual(
            "full_cell",
            parsed["summary"]["spectral_flag_channel_read_granularity"],
        )
        self.assertEqual(
            "requested_range",
            parsed["summary"]["spectral_weight_spectrum_channel_read_granularity"],
        )
        self.assertEqual(95, parsed["summary"]["cube_source_row_blocks"])
        self.assertEqual(32768, parsed["summary"]["cube_source_row_block_rows"])
        self.assertEqual(
            13628961024,
            parsed["summary"]["cube_source_row_blocks_visibility_capacity_bytes"],
        )
        self.assertEqual(9156.943, parsed["summary"]["cube_source_row_blocks_read_ms"])
        self.assertEqual(
            11355.156, parsed["summary"]["cube_source_row_blocks_prepare_ms"]
        )
        self.assertEqual(
            "full_prepared_slab",
            parsed["summary"]["executor_limitation_materialization"],
        )
        self.assertEqual(
            "small_planner_row_blocks_are_only_valid_for_streaming_consumers",
            parsed["summary"]["executor_limitation_reason"],
        )
        self.assertEqual(
            7777, parsed["summary"]["spectral_best_modeled_total_io_bytes"]
        )
        self.assertEqual(
            700, parsed["summary"]["spectral_best_modeled_source_read_bytes"]
        )
        self.assertEqual(
            78, parsed["summary"]["spectral_best_modeled_output_spill_io_bytes"]
        )
        self.assertEqual(8, parsed["summary"]["spectral_best_modeled_active_planes"])
        self.assertEqual(1, parsed["summary"]["spectral_best_modeled_slab_count"])
        self.assertEqual(7777, parsed["summary"]["spectral_modeled_total_io_bytes"])
        self.assertEqual(700, parsed["summary"]["spectral_modeled_source_read_bytes"])
        self.assertEqual(
            0, parsed["summary"]["spectral_modeled_visibility_cache_io_bytes"]
        )
        self.assertEqual(
            78, parsed["summary"]["spectral_modeled_output_spill_io_bytes"]
        )
        self.assertEqual(
            6999, parsed["summary"]["spectral_modeled_product_write_bytes"]
        )
        self.assertEqual(
            700, parsed["summary"]["spectral_modeled_full_cache_source_read_bytes"]
        )
        self.assertEqual(
            1300, parsed["summary"]["spectral_visibility_cache_saved_read_bytes"]
        )
        self.assertIn(
            "source_first:total=7777",
            parsed["summary"]["spectral_candidate_io_costs"],
        )
        self.assertEqual("cpu_slab", parsed["summary"]["spectral_backend"])
        self.assertEqual(
            "wave3_fixed_tile_cpu", parsed["summary"]["cube_per_plane_backend"]
        )
        self.assertEqual(
            "clean_deconvolution", parsed["summary"]["cube_per_plane_phase"]
        )
        self.assertEqual(4, parsed["summary"]["cube_per_plane_workers"])
        self.assertEqual(2, parsed["summary"]["cube_per_plane_grid_threads"])
        self.assertEqual(
            "metal_device_unavailable",
            parsed["summary"]["cube_per_plane_fallback_reasons"],
        )
        self.assertEqual(
            1500000, parsed["summary"]["spectral_memory_max_current_rss_bytes"]
        )
        self.assertEqual(
            1700000, parsed["summary"]["spectral_memory_max_peak_rss_bytes"]
        )
        self.assertEqual(
            900000, parsed["summary"]["spectral_memory_max_delta_from_baseline_bytes"]
        )
        self.assertEqual(
            500000, parsed["summary"]["spectral_memory_max_delta_from_previous_bytes"]
        )
        self.assertEqual(
            "after_slab_run", parsed["summary"]["spectral_memory_max_delta_stage"]
        )
        self.assertEqual(2, parsed["summary"]["spectral_product_batch_planes"])
        self.assertEqual(1, parsed["summary"]["cube_product_summary_count"])
        self.assertEqual(30.0, parsed["summary"]["cube_product_write_ms"])
        self.assertEqual(1024, parsed["summary"]["cube_product_bytes"])
        self.assertEqual(2, parsed["summary"]["cube_product_groups"])
        self.assertEqual(4, parsed["summary"]["cube_product_group_planes"])
        self.assertEqual(128, parsed["summary"]["cube_product_tiled_tile_visits"])
        self.assertEqual(
            4096, parsed["summary"]["cube_product_tiled_lru_batch_flush_bytes"]
        )
        self.assertEqual(2, parsed["summary"]["cube_product_tiled_direct_write_calls"])
        self.assertEqual(
            128, parsed["summary"]["cube_product_tiled_direct_write_tiles"]
        )
        self.assertEqual(
            8192, parsed["summary"]["cube_product_tiled_direct_write_bytes"]
        )
        self.assertEqual(1200, parsed["summary"]["cube_product_tiled_direct_pack_ns"])
        self.assertEqual(0, parsed["summary"]["cube_product_tiled_direct_swap_ns"])
        self.assertEqual(3400, parsed["summary"]["cube_product_tiled_direct_write_ns"])
        self.assertEqual(1, parsed["summary"]["cube_plane_state_store_count"])
        self.assertEqual(0, parsed["summary"]["cube_plane_state_store_bytes_read"])
        self.assertEqual(
            1024, parsed["summary"]["cube_plane_state_store_bytes_written"]
        )
        self.assertEqual(30.0, parsed["summary"]["cube_plane_state_store_elapsed_ms"])
        self.assertEqual(
            "product_backed_write_through",
            parsed["summary"]["cube_plane_state_store_kind"],
        )
        self.assertEqual(
            "drop_after_write",
            parsed["summary"]["cube_plane_state_store_cleanup_policy"],
        )
        self.assertEqual(True, parsed["summary"]["visibility_geometry_cache_enabled"])
        self.assertEqual(
            1048576, parsed["summary"]["visibility_geometry_cache_budget_bytes"]
        )
        self.assertEqual(
            2048, parsed["summary"]["visibility_geometry_cache_resident_bytes"]
        )
        self.assertEqual(1, parsed["summary"]["visibility_geometry_cache_entries"])
        self.assertEqual(1, parsed["summary"]["visibility_geometry_cache_fills"])
        self.assertEqual(3, parsed["summary"]["visibility_geometry_cache_hits"])
        self.assertEqual(1, parsed["summary"]["visibility_geometry_cache_misses"])
        self.assertEqual(3, parsed["summary"]["visibility_geometry_cache_shares"])
        self.assertEqual(0, parsed["summary"]["visibility_geometry_cache_bypasses"])
        self.assertEqual(
            0,
            parsed["summary"]["visibility_geometry_cache_rejected_model_dependent"],
        )
        self.assertEqual(
            42.0, parsed["summary"]["visibility_geometry_cache_elapsed_ms"]
        )
        event = parsed["spectral_slab_events"][0]["fields"]
        self.assertEqual("initial_dirty", event["pass_kind"])
        self.assertEqual("source_read", event["stage"])
        self.assertEqual(32768, event["row_block_rows"])

    def test_turnaround_workload_can_skip_profile_rerun(self) -> None:
        manifest = {
            "id": "large-turnaround",
            "mode_id": "mosaic-cube-one-channel",
            "dataset": {
                "key": "large.ms",
                "path": "/tmp/large.ms",
            },
            "imaging": {
                "mode": "clean",
                "specmode": "cube",
                "gridder": "mosaic",
            },
            "run": {
                "skip_profile": "1",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_SKIP_PROFILE"])

    def test_profile_repeats_can_be_lower_than_wallclock_repeats(self) -> None:
        manifest = {
            "id": "profile-repeat-control",
            "mode_id": "standard-mfs-clean",
            "dataset": {
                "key": "medium.ms",
                "path": "/tmp/medium.ms",
            },
            "imaging": {
                "mode": "clean",
                "specmode": "mfs",
                "gridder": "standard",
            },
            "run": {
                "profile_repeats": 1,
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=3,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual(3, plan["run"]["repeats"])
        self.assertEqual(1, plan["run"]["profile_repeats"])
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_PROFILE_REPEATS"])

    def test_cubedata_is_accepted_as_runnable_wave3_mode(self) -> None:
        manifest = {
            "id": "cubedata-one-plane",
            "mode_id": "cubedata-one-channel",
            "dataset": {
                "key": "input.ms",
                "path": "/tmp/input.ms",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "cubedata",
                "gridder": "standard",
                "channel_count": 1,
            },
            "run": {
                "evidence_role": "after_gpu_metal",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("cubedata", plan["mode"]["specmode"])
        self.assertEqual("runnable", plan["run_support"]["status"])
        self.assertEqual(
            "1", plan["command"]["env"]["IMAGER_BENCH_PERCHANWEIGHTDENSITY"]
        )
        self.assertEqual("after_gpu_metal", plan["review"]["evidence_role"])

    def test_phase_probe_is_opt_in_for_casa_stage_diagnostics(self) -> None:
        manifest = {
            "id": "stage-probe",
            "mode_id": "standard-mfs-clean-current",
            "dataset": {
                "key": "medium.ms",
                "path": "/tmp/medium.ms",
            },
            "imaging": {
                "mode": "clean",
                "specmode": "mfs",
                "gridder": "standard",
            },
            "run": {
                "phase_probe": "1",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("1", plan["run"]["phase_probe"])
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_PHASE_PROBE"])

    def test_invalid_phase_probe_fails_before_running_benchmark(self) -> None:
        manifest = {
            "id": "bad-stage-probe",
            "mode_id": "standard-mfs-clean-current",
            "dataset": {
                "key": "medium.ms",
                "path": "/tmp/medium.ms",
            },
            "imaging": {
                "mode": "clean",
                "specmode": "mfs",
                "gridder": "standard",
            },
            "run": {
                "phase_probe": "sometimes",
            },
        }

        with self.assertRaisesRegex(run_workload.HarnessError, "phase_probe"):
            run_workload.build_plan(
                manifest_path=Path("manifest.json"),
                manifest=manifest,
                repeats_override=1,
                run_label_override=None,
                storage_label_override=None,
                dry_run=True,
            )

    def test_ms_staging_can_be_set_for_small_copy_runs(self) -> None:
        manifest = {
            "id": "small-copy",
            "mode_id": "standard-mfs-dirty-control",
            "dataset": {
                "key": "small.ms",
                "path": "/tmp/small.ms",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
            },
            "run": {
                "ms_staging": "copy",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("copy", plan["run"]["ms_staging"])
        self.assertEqual("copy", plan["command"]["env"]["IMAGER_BENCH_MS_STAGING"])

    def test_invalid_ms_staging_fails_before_running_benchmark(self) -> None:
        manifest = {
            "id": "bad-staging",
            "mode_id": "standard-mfs-dirty-control",
            "dataset": {
                "key": "input.ms",
                "path": "/tmp/input.ms",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
            },
            "run": {
                "ms_staging": "mirror",
            },
        }

        with self.assertRaisesRegex(run_workload.HarnessError, "ms_staging"):
            run_workload.build_plan(
                manifest_path=Path("manifest.json"),
                manifest=manifest,
                repeats_override=1,
                run_label_override=None,
                storage_label_override=None,
                dry_run=True,
            )

    def test_attach_stage_breakdown_does_not_require_casa_stage_data(self) -> None:
        plan = {
            "mode": {
                "bench_mode": "dirty",
                "niter": 0,
            }
        }
        parsed = {
            "stage_medians_ms": {
                "rust": {"psf_grid": 1.0},
                "casa": {},
            }
        }

        run_workload.attach_stage_breakdown(plan, parsed)

        self.assertEqual(parsed["stage_breakdown"]["schema_version"], 1)
        self.assertEqual(parsed["stage_breakdown"]["rust"]["status"], "reported")
        self.assertEqual(parsed["stage_breakdown"]["casa"]["status"], "missing")

    def test_human_review_gate_reports_panel_readiness(self) -> None:
        plan = {
            "review": {
                "required_reviewer": "Brian",
                "required_evidence_roles": ["before_baseline", "after_gpu_metal"],
                "evidence_role": "after_gpu_metal",
            }
        }
        comparison = {
            "status": "completed",
            "structured_difference_review": {
                "label": "investigate",
                "summary": "overall investigate; investigate: .image",
                "legend": {
                    "good": "No review action expected from this check.",
                    "investigate": "Plausible but needs review in context.",
                    "bad": "Structured or large enough difference; do not close without explanation.",
                    "unknown": "Check could not be evaluated for this product.",
                },
            },
            "products": {
                ".image": {
                    "status": "compared",
                    "review_panel": {"status": "written", "path": "/tmp/image.png"},
                }
            },
        }

        gate = run_workload.human_review_gate(plan, comparison)

        self.assertEqual("pending", gate["status"])
        self.assertEqual("ready", gate["panel_status"])
        self.assertEqual("after_gpu_metal", gate["evidence_role"])
        self.assertEqual("investigate", gate["structured_difference_label"])
        self.assertIn("overall investigate", gate["structured_difference_summary"])
        self.assertIn("bad", gate["structured_difference_legend"])

    def test_product_review_panels_are_square_and_labeled(self) -> None:
        script = IMAGE_COMPARATOR_SOURCE

        self.assertIn('aspect="equal"', script)
        self.assertIn("axis.set_box_aspect(1)", script)
        self.assertIn("label=value_label", script)
        self.assertIn('label=f"casa-rs - CASA ({value_label})"', script)
        self.assertIn('f"casa-rs {product_label}"', script)
        self.assertIn('f"CASA {product_label}"', script)
        self.assertIn('f"difference {product_label}', script)
        self.assertIn('return "Jy/beam"', script)

    def test_product_comparison_stride_preserves_spatial_aspect(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(IMAGE_COMPARATOR_SOURCE, namespace)

        self.assertEqual(
            [2, 2, 1, 1],
            namespace["stride_for"]([1024, 1024, 1, 1], 1_000_000),
        )

    def test_product_comparison_panels_use_spatial_display_stride(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(IMAGE_COMPARATOR_SOURCE, namespace)

        class FakeImageTool:
            def __init__(self):
                self.path = None

            def open(self, path):
                self.path = path

            def shape(self):
                return [2048, 2048, 1, 512]

            def getchunk(self, blc, trc, inc, dropdeg, getmask):
                shape = [
                    ((trc_value - blc_value) // inc_value) + 1
                    for blc_value, trc_value, inc_value in zip(blc, trc, inc)
                ]
                value = 2.0 if str(self.path).endswith("rust.image") else 1.0
                return np.full(shape, value, dtype=np.float64)

            def close(self):
                return None

        captured = {}

        def fake_write_review_panel(
            panel_dir,
            suffix,
            rust_data,
            casa_data,
            diff_data,
            review=None,
            display=None,
        ):
            captured["rust_data_shape"] = list(rust_data.shape)
            captured["display_data_shape"] = list(display["rust_data"].shape)
            captured["display_sample_stride"] = display["sample_stride"]
            return {"status": "written", "path": "/tmp/image.review.png"}

        namespace["image"] = FakeImageTool
        namespace["write_review_panel"] = fake_write_review_panel
        namespace["structured_difference_metrics"] = lambda **kwargs: {
            "status": "computed",
            "review": {"label": "good"},
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            rust_path = os.path.join(temp_dir, "rust.image")
            casa_path = os.path.join(temp_dir, "casa.image")
            os.makedirs(rust_path)
            os.makedirs(casa_path)
            result = namespace["compare_one"](
                rust_path,
                casa_path,
                1_000_000,
                temp_dir,
                ".image",
                {"status": "unavailable"},
            )

        self.assertEqual("compared", result["status"])
        self.assertEqual([47, 47, 1, 1], result["sample_stride"])
        self.assertEqual([44, 44, 1, 512], captured["rust_data_shape"])
        self.assertEqual([3, 3, 1, 1], captured["display_sample_stride"])
        self.assertEqual([683, 683, 1, 1], captured["display_data_shape"])

    def test_product_comparison_reports_beam_normalized_structure_metrics(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(IMAGE_COMPARATOR_SOURCE, namespace)

        y, x = np.indices((64, 64))
        casa = np.ones((64, 64), dtype=np.float64)
        rust = casa + 0.01 + 0.005 * (x / 63.0)
        diff = rust - casa
        beam_info = {
            "status": "estimated_from_psf",
            "beam_block_side_pixels": 4,
        }

        metrics = namespace["structured_difference_metrics"](
            ".weight",
            rust,
            casa,
            diff,
            beam_info,
        )

        self.assertEqual("computed", metrics["status"])
        self.assertEqual(4, metrics["beam_block_side_pixels"])
        self.assertEqual("weight_union_support", metrics["mask"]["type"])
        self.assertGreater(metrics["low_order_r2_quadratic"], 0.95)
        self.assertGreater(metrics["large_scale_power_fraction"]["fraction"], 0.5)
        self.assertGreater(len(metrics["beam_block_rms_by_scale"]), 2)
        self.assertIsNotNone(metrics["block_rms_decay_slope_vs_independent_beams"])
        self.assertIn(
            "normalized_block_mean_rms", metrics["beam_block_rms_by_scale"][0]
        )
        self.assertEqual("computed", metrics["scale_offset_gradient_fit"]["status"])
        self.assertIn("dx_pixels", metrics["scale_offset_gradient_fit"]["coefficients"])
        self.assertEqual("bad", metrics["classification"]["overall"])
        self.assertEqual("bad", metrics["classification"]["amplitude"])
        self.assertEqual("bad", metrics["classification"]["structure"])
        self.assertEqual(
            "bad",
            metrics["classification"]["structure_components"]["low_order_r2_quadratic"],
        )
        self.assertIn("thresholds", metrics["classification"])
        self.assertEqual("bad", metrics["review"]["label"])
        self.assertIn("correctness blocker", metrics["review"]["summary"])
        self.assertEqual(
            "bad",
            next(
                check
                for check in metrics["review"]["checks"]
                if check["name"] == "large_scale_power_fraction"
            )["label"],
        )
        self.assertIn("good", metrics["review"]["legend"])

    def test_product_comparison_does_not_escalate_numerical_floor_structure(
        self,
    ) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(IMAGE_COMPARATOR_SOURCE, namespace)

        y, x = np.indices((64, 64))
        casa = np.ones((64, 64), dtype=np.float64)
        rust = casa + 1.0e-7 * (x / 63.0) + 5.0e-8 * (y / 63.0)
        diff = rust - casa
        metrics = namespace["structured_difference_metrics"](
            ".image",
            rust,
            casa,
            diff,
            {"status": "estimated_from_psf", "beam_block_side_pixels": 4},
        )

        self.assertLess(metrics["normalized_diff_rms"], 1.0e-6)
        self.assertEqual("good", metrics["classification"]["amplitude"])
        self.assertEqual("good", metrics["classification"]["structure"])
        self.assertEqual("good", metrics["classification"]["overall"])
        self.assertTrue(
            metrics["classification"]["structure_suppressed_by_numerical_floor"]
        )
        self.assertEqual("good", metrics["review"]["label"])

    def test_product_comparison_handles_line_like_display_planes(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(IMAGE_COMPARATOR_SOURCE, namespace)

        casa = np.linspace(1.0, 2.0, 64, dtype=np.float64)[:, np.newaxis]
        rust = casa + 0.01
        diff = rust - casa

        metrics = namespace["structured_difference_metrics"](
            ".image",
            rust,
            casa,
            diff,
            {"status": "unavailable"},
        )

        self.assertEqual("computed", metrics["status"])
        self.assertEqual(
            "insufficient_dimensions",
            metrics["scale_offset_gradient_fit"]["status"],
        )
        self.assertEqual([64, 1], metrics["scale_offset_gradient_fit"]["shape"])

    def test_product_comparison_treats_sumwt_as_non_spatial(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(IMAGE_COMPARATOR_SOURCE, namespace)

        casa = np.linspace(6.0e6, 6.2e6, 64, dtype=np.float64).reshape(1, 1, 1, 64)
        rust = casa + 0.05
        diff = rust - casa

        metrics = namespace["structured_difference_metrics"](
            ".sumwt",
            rust,
            casa,
            diff,
            {"status": "unavailable"},
        )

        self.assertEqual("computed", metrics["status"])
        self.assertEqual("good", metrics["classification"]["overall"])
        self.assertEqual("not_applicable", metrics["classification"]["structure"])
        self.assertEqual([], metrics["beam_block_rms_by_scale"])
        self.assertEqual(
            "not_applicable", metrics["scale_offset_gradient_fit"]["status"]
        )
        self.assertEqual("good", metrics["review"]["label"])
        self.assertIn("non-spatial product amplitude", metrics["review"]["summary"])

    def test_product_comparison_display_plane_uses_middle_extra_axis(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(IMAGE_COMPARATOR_SOURCE, namespace)

        cube = np.zeros((4, 4, 1, 7), dtype=np.float64)
        cube[:, :, 0, 0] = 1.0
        cube[:, :, 0, 3] = 5.0

        plane = namespace["display_plane"](cube)

        self.assertEqual((4, 4), plane.shape)
        np.testing.assert_allclose(5.0, plane)

    def test_product_comparison_display_plane_bounds_select_center_cube_plane(
        self,
    ) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(IMAGE_COMPARATOR_SOURCE, namespace)

        blc, trc = namespace["display_plane_bounds"]([2048, 2048, 1, 64])

        self.assertEqual([0, 0, 0, 32], blc)
        self.assertEqual([2047, 2047, 0, 32], trc)

    def test_model_review_panel_uses_restored_beam_visualization(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(IMAGE_COMPARATOR_SOURCE, namespace)

        class FakeAxis:
            def __init__(self):
                self.images = []
                self.titles = []

            def imshow(self, *args, **kwargs):
                self.images.append((args, kwargs))
                return object()

            def set_title(self, title, *args, **kwargs):
                self.titles.append(title)
                return None

            def set_aspect(self, *args, **kwargs):
                return None

            def set_box_aspect(self, *args, **kwargs):
                return None

            def set_xticks(self, *args, **kwargs):
                return None

            def set_yticks(self, *args, **kwargs):
                return None

        class FakeFigure:
            def suptitle(self, *args, **kwargs):
                return None

            def colorbar(self, *args, **kwargs):
                return None

            def savefig(self, *args, **kwargs):
                return None

        class FakePlot:
            def __init__(self):
                self.axes = [FakeAxis(), FakeAxis(), FakeAxis()]

            def subplots(self, *args, **kwargs):
                return FakeFigure(), self.axes

            def close(self, *args, **kwargs):
                return None

        fake_plot = FakePlot()
        namespace["plt"] = fake_plot

        raw_rust = np.zeros((4, 4, 1, 3), dtype=np.float64)
        raw_casa = np.zeros((4, 4, 1, 3), dtype=np.float64)
        restored_rust = np.ones((4, 4, 1, 3), dtype=np.float64) * 2.0
        restored_casa = np.ones((4, 4, 1, 3), dtype=np.float64) * 1.5
        restored_diff = restored_rust - restored_casa

        with tempfile.TemporaryDirectory() as temp_dir:
            panel = namespace["write_review_panel"](
                temp_dir,
                ".model",
                raw_rust,
                raw_casa,
                raw_rust - raw_casa,
                display={
                    "status": "available",
                    "rust_data": restored_rust,
                    "casa_data": restored_casa,
                    "diff_data": restored_diff,
                    "transform": "restored_model_from_image_minus_residual",
                    "description": ".model visualized as restoring-beam-convolved model via .image - .residual",
                    "product_label": ".model restored-beam visualization",
                    "value_label": "Jy/beam",
                },
            )

        self.assertEqual("written", panel["status"])
        self.assertEqual("derived", panel["display_status"])
        self.assertEqual(
            "restored_model_from_image_minus_residual",
            panel["display_transform"],
        )
        self.assertEqual([1.5, 2.0], panel["casa_rs_and_casa_color_limits"])
        self.assertEqual([-0.5, 0.5], panel["difference_color_limits"])
        self.assertIn("restored-beam visualization", fake_plot.axes[0].titles[0])
        np.testing.assert_allclose(2.0, fake_plot.axes[0].images[0][0][0])

    def test_product_comparison_rolls_up_structured_review_labels(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(IMAGE_COMPARATOR_SOURCE, namespace)

        rollup = namespace["summarize_product_reviews"](
            {
                ".image": {
                    "structured_difference": {
                        "review": {
                            "label": "good",
                            "summary": ".image: good",
                            "checks": [
                                {"name": "normalized_diff_rms", "label": "good"}
                            ],
                        }
                    }
                },
                ".weight": {
                    "structured_difference": {
                        "review": {
                            "label": "investigate",
                            "summary": ".weight: investigate",
                            "checks": [
                                {
                                    "name": "large_scale_power_fraction",
                                    "label": "bad",
                                }
                            ],
                        }
                    }
                },
            }
        )

        self.assertEqual("investigate", rollup["label"])
        self.assertEqual("good", rollup["products"][".image"])
        self.assertEqual("investigate", rollup["products"][".weight"])
        self.assertIn("overall investigate", rollup["summary"])
        self.assertIn("normalized_diff_rms", rollup["thresholds"])
        self.assertEqual(
            "bad", rollup["checks_by_product"]["large_scale_power_fraction"][".weight"]
        )
        self.assertIn("bad", rollup["legend"])

    def test_review_panel_records_structured_difference_label(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(IMAGE_COMPARATOR_SOURCE, namespace)

        class FakeAxis:
            def imshow(self, *args, **kwargs):
                return object()

            def set_title(self, *args, **kwargs):
                return None

            def set_aspect(self, *args, **kwargs):
                return None

            def set_box_aspect(self, *args, **kwargs):
                return None

            def set_xticks(self, *args, **kwargs):
                return None

            def set_yticks(self, *args, **kwargs):
                return None

        class FakeFigure:
            def suptitle(self, *args, **kwargs):
                return None

            def colorbar(self, *args, **kwargs):
                return None

            def savefig(self, *args, **kwargs):
                return None

        class FakePlot:
            def subplots(self, *args, **kwargs):
                return FakeFigure(), [FakeAxis(), FakeAxis(), FakeAxis()]

            def close(self, *args, **kwargs):
                return None

        namespace["plt"] = FakePlot()

        rust = np.ones((8, 8), dtype=np.float64)
        casa = np.ones((8, 8), dtype=np.float64) * 0.99
        diff = rust - casa
        review = {
            "label": "investigate",
            "summary": ".weight: investigate; amplitude is good and structure is bad.",
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            panel = namespace["write_review_panel"](
                temp_dir,
                ".weight",
                rust,
                casa,
                diff,
                review=review,
            )

        self.assertEqual("written", panel["status"])
        self.assertEqual("investigate", panel["structured_difference_label"])
        self.assertIn(".weight: investigate", panel["structured_difference_summary"])

    def test_review_panel_skips_zoom_when_bounds_cover_full_plane(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(IMAGE_COMPARATOR_SOURCE, namespace)

        class FakeAxis:
            def imshow(self, *args, **kwargs):
                return object()

            def set_title(self, *args, **kwargs):
                return None

            def set_aspect(self, *args, **kwargs):
                return None

            def set_box_aspect(self, *args, **kwargs):
                return None

            def set_xticks(self, *args, **kwargs):
                return None

            def set_yticks(self, *args, **kwargs):
                return None

        class FakeFigure:
            def suptitle(self, *args, **kwargs):
                return None

            def colorbar(self, *args, **kwargs):
                return None

            def savefig(self, *args, **kwargs):
                return None

        class FakePlot:
            def subplots(self, *args, **kwargs):
                return FakeFigure(), [FakeAxis(), FakeAxis(), FakeAxis()]

            def close(self, *args, **kwargs):
                return None

        namespace["plt"] = FakePlot()

        rust = np.ones((64, 64), dtype=np.float64)
        casa = np.ones((64, 64), dtype=np.float64) * 0.99
        diff = rust - casa

        with tempfile.TemporaryDirectory() as temp_dir:
            panel = namespace["write_review_panel"](
                temp_dir, ".image", rust, casa, diff
            )

        self.assertEqual("written", panel["status"])
        self.assertEqual("skipped", panel["zoom_panel"]["status"])
        self.assertEqual(
            "zoom bounds cover the full review plane",
            panel["zoom_panel"]["reason"],
        )

    def test_parse_rust_stage_section_keeps_full_core_timing_set(self) -> None:
        log = """Rust stage medians (milliseconds):
  run=1 frontend_total_ms=100.000 open_ms=1.000 prepare_ms=2.000 phase_center_ms=3.000 imaging_ms=4.000 coords_ms=5.000 write_ms=6.000 core_total_ms=40.000 controller_ms=7.000 weighting_ms=8.000 major_refresh_ms=9.000 psf_grid_ms=10.000 psf_fft_ms=11.000 psf_normalize_ms=12.000 model_fft_ms=13.000 residual_grid_ms=14.000 residual_fft_ms=15.000 residual_normalize_ms=16.000 minor_ms=17.000 minor_solve_ms=18.000 beam_fit_ms=19.000 restore_ms=20.000
  frontend:
  open_measurement_set=1.000
  prepare_plane_input=2.000
  prepared_source_read=2.500
  prepared_source_prepare=3.500
  extract_phase_center=3.000
  run_imaging=4.000
  build_coordinate_system=5.000
  write_products=6.000
  frontend_total=100.000
  core:
  controller_overhead=7.000
  weighting=8.000
  psf_grid=10.000
  psf_fft=11.000
  psf_normalize=12.000
  model_fft=13.000
  residual_degrid_grid=14.000
  residual_fft=15.000
  residual_normalize=16.000
  major_cycle_refresh=9.000
  residual_refresh_overhead=9.500
  clean_cycle_setup=9.250
  deconvolver_setup=9.750
  multiscale_scale_refresh=9.125
  minor_cycle=17.000
  minor_cycle_solve=18.000
  beam_fit=19.000
  restore=20.000
  total=40.000

CASA tclean timings (seconds):
"""

        stages = run_workload.parse_stage_section(log, "Rust stage medians")

        for name in [
            "psf_normalize",
            "model_fft",
            "residual_normalize",
            "major_cycle_refresh",
            "residual_refresh_overhead",
            "clean_cycle_setup",
            "deconvolver_setup",
            "multiscale_scale_refresh",
            "minor_cycle",
            "minor_cycle_solve",
            "beam_fit",
            "restore",
        ]:
            self.assertIn(name, stages)
        self.assertEqual(stages["psf_normalize"], 12.0)
        self.assertEqual(stages["model_fft"], 13.0)
        self.assertEqual(stages["residual_refresh_overhead"], 9.5)
        self.assertEqual(stages["clean_cycle_setup"], 9.25)
        self.assertEqual(stages["deconvolver_setup"], 9.75)
        self.assertEqual(stages["multiscale_scale_refresh"], 9.125)
        self.assertEqual(stages["get_ms_values_into_processing_buffer"], 2.5)
        self.assertEqual(stages["prepare_processing_buffer"], 3.5)
        self.assertEqual(stages["restore"], 20.0)
        self.assertNotIn("niter", stages)
        self.assertNotIn("imsize", stages)
        self.assertNotIn("psf_grid_ms", stages)

    def test_comparison_evidence_status_distinguishes_failure_and_tolerance(
        self,
    ) -> None:
        failed_status, failed = run_workload.comparison_evidence_status(
            {"status": "failed_execution", "reason": "CASA exited 2", "products": {}}
        )
        self.assertEqual("failed_comparison", failed_status)
        self.assertEqual("comparison", failed["kind"])

        tolerance_status, tolerance = run_workload.comparison_evidence_status(
            {
                "status": "completed",
                "products": {".image": {"status": "compared"}},
                "structured_difference_review": {
                    "label": "bad",
                    "summary": "structured residual",
                },
            }
        )
        self.assertEqual("out_of_tolerance", tolerance_status)
        self.assertEqual("comparison_tolerance", tolerance["kind"])

        investigate_status, investigate = run_workload.comparison_evidence_status(
            {
                "status": "completed",
                "products": {".image": {"status": "compared"}},
                "structured_difference_review": {
                    "label": "investigate",
                    "summary": "unexplained structure",
                },
            }
        )
        self.assertEqual("out_of_tolerance", investigate_status)
        self.assertEqual("comparison_tolerance", investigate["kind"])

        bounded_status, bounded_failure = run_workload.comparison_evidence_status(
            {
                "status": "completed",
                "products": {".image": {"status": "compared"}},
                "structured_difference_review": {
                    "label": "investigate",
                    "summary": "low-amplitude coherent structure",
                },
                "tolerance_evaluation": {
                    "status": "passed",
                    "checks": [
                        {
                            "name": ".image.coherent_block_rms_over_right_rms",
                            "status": "passed",
                        }
                    ],
                },
            }
        )
        self.assertEqual("completed", bounded_status)
        self.assertIsNone(bounded_failure)

        completed_status, failure = run_workload.comparison_evidence_status(
            {"status": "unavailable", "reason": "CASA not configured", "products": {}}
        )
        self.assertEqual("completed", completed_status)
        self.assertIsNone(failure)

        required_status, required_failure = run_workload.comparison_evidence_status(
            {"status": "unavailable", "reason": "CASA not configured", "products": {}},
            required=True,
        )
        self.assertEqual("failed_comparison", required_status)
        self.assertEqual("comparison", required_failure["kind"])

    def test_repeatability_checks_every_measured_call_including_retained_final(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = {
                "artifacts": {"comparison_root": str(root / "comparisons")},
                "comparison": {
                    "products": [".image.tt0"],
                    "max_elements_per_product": 100,
                    "mode": "full",
                    "full_chunk_elements": 50,
                    "require_exact_product_inventory": True,
                    "require_metadata_parity": True,
                    "source_regions": [
                        {
                            "id": "bright-source",
                            "products": [".image.tt0"],
                            "blc": [10, 20],
                            "trc": [30, 40],
                        }
                    ],
                    "tolerances": {
                        "contract_version": 1,
                        "require_full_array": True,
                        "default": {"peak_relative": 0.01},
                        "products": {},
                    },
                },
                "command": {"casa": {"python": "/casa/python"}},
            }
            calls = [
                {"name": f"measured-{index:03d}", "prefix": f"/products/{index}"}
                for index in range(1, 4)
            ]

            def completed_comparison(**kwargs):
                request = kwargs["request"]
                return {
                    "status": "completed",
                    "products": {".image.tt0": {"status": "compared"}},
                    "product_inventory": {"status": "matched"},
                    "structured_difference_review": {"label": "good"},
                    "right_prefix": request["right_prefix"],
                }

            compare = mock.Mock(side_effect=completed_comparison)
            result = run_workload.casa_tclean_workflow.compare_casa_repeatability(
                plan,
                calls,
                root / "run.log",
                comparison_runner=compare,
            )

            self.assertEqual("completed", result["status"])
            self.assertEqual(["measured-002", "measured-003"], result["compared_calls"])
            self.assertEqual(2, compare.call_count)
            self.assertEqual(
                "/products/3",
                compare.call_args_list[-1].kwargs["request"]["right_prefix"],
            )
            self.assertEqual(
                plan["comparison"]["source_regions"],
                compare.call_args_list[-1].kwargs["request"]["source_regions"],
            )
            self.assertEqual(
                plan["comparison"]["tolerances"],
                result["tolerances"],
            )

    def test_single_cold_call_still_checks_exact_product_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = {
                "artifacts": {"comparison_root": str(root / "comparisons")},
                "comparison": {
                    "products": [".image.tt0", ".psf.tt0"],
                    "max_elements_per_product": 100,
                    "mode": "sampled",
                    "full_chunk_elements": 50,
                    "require_exact_product_inventory": True,
                    "require_metadata_parity": True,
                },
                "command": {"casa": {"python": "/casa/python"}},
            }
            call = {"name": "measured-001", "prefix": "/products/1"}
            comparison = {
                "status": "completed",
                "products": {
                    ".image.tt0": {"status": "compared"},
                    ".psf.tt0": {"status": "compared"},
                },
                "product_inventory": {"status": "matched"},
                "structured_difference_review": {"label": "good"},
            }
            compare = mock.Mock(return_value=comparison)
            result = run_workload.casa_tclean_workflow.compare_casa_repeatability(
                plan,
                [call],
                root / "run.log",
                comparison_runner=compare,
            )

            request = compare.call_args.kwargs["request"]
            self.assertEqual(request["left_prefix"], request["right_prefix"])
            self.assertTrue(request["require_exact_product_inventory"])
            self.assertTrue(request["require_metadata_parity"])
            self.assertEqual("single_call_product_contract", result["comparison_kind"])

    def test_comparator_validation_failure_closes_to_strict_terminal_summary(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = {
                "artifacts": {"comparison_root": str(root / "comparisons")},
                "comparison": {
                    "products": [".image.tt0"],
                    "max_elements_per_product": 100,
                    "mode": "sampled",
                    "full_chunk_elements": 50,
                    "require_exact_product_inventory": True,
                    "require_metadata_parity": True,
                },
                "command": {"casa": {"python": "/casa/python"}},
            }
            call = {"name": "measured-001", "prefix": "/products/1"}
            compare = mock.Mock(
                return_value={
                    "status": "failed_validation",
                    "reason": "non-finite source-region beam area",
                    "failure": {"kind": "comparison_protocol_binding"},
                    "products": {},
                    "input": "/comparison/input.json",
                }
            )

            result = run_workload.casa_tclean_workflow.compare_casa_repeatability(
                plan,
                [call],
                root / "run.log",
                comparison_runner=compare,
            )

            self.assertEqual("failed_validation", result["status"])
            self.assertEqual(
                {
                    "status": "failed_validation",
                    "reason": "non-finite source-region beam area",
                    "products": {},
                },
                result["comparisons"][0],
            )


class CanonicalSelectionTelemetryTests(unittest.TestCase):
    def source_line(self, **changes: object) -> str:
        fields = {
            "mode": "bounded_spectral",
            "stage": "weighted-replay",
            "phase": "initial-major",
            "ordinal": 0,
            "pass_count": 1,
            "row_blocks": 36,
            "stored_rows": 23_400,
            "stored_samples": 5_990_400,
            "selected_channel_runs": 1_497_600,
            "streamed_samples": 5_990_400,
        }
        fields.update(changes)
        return "imaging_source_read_ahead_summary " + " ".join(
            f"{name}={value}" for name, value in fields.items() if value is not None
        )

    def features(self, lines: list[str]) -> tuple[dict, dict]:
        logs = run_workload.parse_backend_plan_logs("\n".join(lines))
        plan = {
            "mode": {
                "specmode": "mfs",
                "gridder": "awproject",
                "deconvolver": "mtmfs",
                "weighting": "briggs",
                "channel_count": 64,
                "nterms": 2,
                "niter": 30,
            },
            "command": {"env": {"IMAGER_BENCH_STOKES": "I"}},
        }
        features = run_workload.build_benchmark_feature_summary(
            plan, {"backend_plan_logs": logs}
        )
        return features, logs

    def test_canonical_only_counts_four_input_correlations_not_one_stokes(self) -> None:
        features, logs = self.features([self.source_line()])

        self.assertEqual(23_400, features["visibility"]["selected_rows"])
        self.assertEqual(64, features["visibility"]["selected_channels"])
        self.assertEqual(4, features["visibility"]["correlations"])
        self.assertEqual(5_990_400, features["visibility"]["visibility_work"])
        self.assertEqual(
            "selected-observation-traversal",
            features["visibility"]["correlation_source"],
        )
        self.assertIsNone(features["visibility"]["flagged_fraction"])
        self.assertIsNone(features["visibility"]["gridded_samples"])
        self.assertIsNone(features["resources"]["peak_rss_bytes"])
        self.assertEqual(23_400, logs["summary"]["active_rows"])
        self.assertEqual(64, logs["summary"]["selected_channels"])
        receipt = canonical_workload_result(
            extra_results={"backend_plan_logs": logs, "benchmark_features": features}
        )
        receipt["benchmark_features"] = features
        run_workload.validate_run_result(receipt, source="canonical selection telemetry")

    def test_canonical_evidence_is_authoritative_over_legacy_scalar_features(self) -> None:
        features, _ = self.features(
            [
                self.source_line(),
                "standard_mfs_memory_plan_actual rows_total=100 selected_channels=1",
                "standard_mfs_profile_run gridded_samples=10",
            ]
        )

        self.assertEqual(23_400, features["visibility"]["selected_rows"])
        self.assertEqual(64, features["visibility"]["selected_channels"])
        self.assertEqual(4, features["visibility"]["correlations"])
        self.assertEqual(5_990_400, features["visibility"]["visibility_work"])
        self.assertIsNone(features["visibility"]["flagged_fraction"])
        self.assertIsNone(features["visibility"]["gridded_samples"])

    def test_duplicates_and_multiple_passes_do_not_inflate_selected_work(self) -> None:
        lines = [
            self.source_line(stage="density"),
            self.source_line(),
            self.source_line(stage="weighted-replay-channel-slab"),
            self.source_line(stage="selected-output", phase="final-major", ordinal=3),
        ]
        features, logs = self.features(lines * 3)

        self.assertEqual(5_990_400, features["visibility"]["visibility_work"])
        self.assertGreater(
            logs["summary"]["source_read_ahead_streamed_samples"], 5_990_400
        )

    def test_inconsistent_duplicate_complete_pass_fails_closed(self) -> None:
        with self.assertRaisesRegex(run_workload.HarnessError, "disagree across traversals"):
            self.features([self.source_line(), self.source_line(streamed_samples=2_995_200)])

    def test_inconsistent_complete_stages_fail_closed(self) -> None:
        with self.assertRaisesRegex(run_workload.HarnessError, "disagree across traversals"):
            self.features(
                [self.source_line(stage="density"), self.source_line(stored_rows=23_401)]
            )

    def test_conflicting_evidence_cannot_be_hidden_by_log_compaction(self) -> None:
        lines = [self.source_line()] * (run_workload.MAX_BACKEND_LOG_ENTRIES_PER_BUCKET + 1)
        lines[len(lines) // 2] = self.source_line(stored_samples=5_990_401)
        with self.assertRaisesRegex(run_workload.HarnessError, "disagree across traversals"):
            self.features(lines)

    def test_complete_selection_survives_compaction_of_its_only_raw_record(self) -> None:
        lines = [self.source_line(stage="weighted-replay-channel-slab")] * 129
        lines[64] = self.source_line(stage="density")
        features, logs = self.features(lines)

        self.assertEqual(128, len(logs["imaging_source_read_ahead"]))
        self.assertTrue(all(
            entry["fields"]["stage"] == "weighted-replay-channel-slab"
            for entry in logs["imaging_source_read_ahead"]
        ))
        self.assertEqual(23_400, features["visibility"]["selected_rows"])
        self.assertEqual(64, features["visibility"]["selected_channels"])
        self.assertEqual(4, features["visibility"]["correlations"])
        self.assertEqual(5_990_400, features["visibility"]["visibility_work"])

    def test_malformed_canonical_evidence_never_uses_legacy_factors(self) -> None:
        malformed = [
            {"stage": None},
            {"phase": None},
            {"ordinal": None},
            {"ordinal": -1},
            {"measurement_state": "missing"},
            {"pass_count": 0},
            {"row_blocks": None},
            {"stored_rows": -1},
            {"stored_samples": None},
            {"selected_channel_runs": 0},
            {"streamed_samples": True},
            {"streamed_samples": 5_990_400.0},
            {"streamed_samples": 5_990_401},
        ]
        for changes in malformed:
            with self.subTest(changes=changes), self.assertRaisesRegex(
                run_workload.HarnessError, "canonical selected-observation traversal"
            ):
                self.features(
                    [
                        "standard_mfs_memory_plan_actual rows_total=23400 selected_channels=64",
                        self.source_line(**changes),
                    ]
                )

    def test_nonuniform_channels_keep_exact_work_and_unknown_channel_factor(self) -> None:
        features, _ = self.features(
            [self.source_line(stored_rows=3, stored_samples=20, selected_channel_runs=5,
                              streamed_samples=20, row_blocks=1)]
        )

        self.assertEqual(3, features["visibility"]["selected_rows"])
        self.assertIsNone(features["visibility"]["selected_channels"])
        self.assertEqual(4, features["visibility"]["correlations"])
        self.assertEqual(20, features["visibility"]["visibility_work"])

    def test_nonuniform_correlations_keep_exact_work_and_unknown_correlation_factor(self) -> None:
        features, _ = self.features(
            [self.source_line(stored_rows=2, stored_samples=8, selected_channel_runs=4,
                              streamed_samples=6, row_blocks=1)]
        )

        self.assertEqual(2, features["visibility"]["selected_channels"])
        self.assertIsNone(features["visibility"]["correlations"])
        self.assertEqual(6, features["visibility"]["visibility_work"])

    def test_selected_samples_need_not_equal_physically_stored_samples(self) -> None:
        features, _ = self.features([self.source_line(streamed_samples=2_995_200)])

        self.assertEqual(2, features["visibility"]["correlations"])
        self.assertEqual(2_995_200, features["visibility"]["visibility_work"])

    def test_full_selection_does_not_require_partial_traversals_to_match(self) -> None:
        features, logs = self.features(
            [
                self.source_line(stage="density"),
                self.source_line(),
                self.source_line(
                    stage="weighted-replay-channel-slab",
                    stored_samples=1_497_600,
                    selected_channel_runs=374_400,
                    streamed_samples=1_497_600,
                ),
                self.source_line(
                    stage="selected-output",
                    phase="final-major",
                    ordinal=3,
                    stored_samples=2_995_200,
                    selected_channel_runs=748_800,
                    streamed_samples=2_995_200,
                ),
            ]
        )

        self.assertEqual(23_400, features["visibility"]["selected_rows"])
        self.assertEqual(64, features["visibility"]["selected_channels"])
        self.assertEqual(4, features["visibility"]["correlations"])
        self.assertEqual(5_990_400, features["visibility"]["visibility_work"])
        self.assertEqual(4, len(logs["imaging_source_read_ahead"]))

    def test_partial_only_canonical_evidence_keeps_full_selection_unknown(self) -> None:
        for stage in ("weighted-replay-channel-slab", "selected-output"):
            with self.subTest(stage=stage):
                features, logs = self.features(
                    [
                        "standard_mfs_memory_plan_actual rows_total=23400 selected_channels=64",
                        "standard_mfs_profile_run gridded_samples=5990400",
                        self.source_line(stage=stage),
                    ]
                )

                for field in ("selected_rows", "selected_channels", "correlations", "visibility_work"):
                    self.assertIsNone(features["visibility"][field], field)
                self.assertEqual(
                    "selected-observation-traversal",
                    features["visibility"]["correlation_source"],
                )
                self.assertIsNone(features["visibility"]["gridded_samples"])
                self.assertIsNone(logs["summary"]["active_rows"])
                self.assertIsNone(logs["summary"]["selected_channels"])


class TerminalRustResourceTelemetryTests(unittest.TestCase):
    def resource_line(self, **changes: object) -> str:
        fields = {
            "command": "casars-imager",
            "pid": 1234,
            "exit_code": 0,
            "peak_rss_bytes": 64 * 1024**2,
            "source": "wait4",
            "scope": "terminal_child",
        }
        fields.update(changes)
        return "imager_bench_process_resource " + " ".join(
            f"{key}={value}" for key, value in fields.items() if value is not None
        )

    def measured_peak(self, lines: list[str]) -> int | None:
        logs = run_workload.parse_backend_plan_logs("\n".join(lines))
        features = run_workload.build_benchmark_feature_summary(
            {}, {"backend_plan_logs": logs}
        )
        self.assertEqual(
            logs["summary"]["peak_rss_bytes"], features["resources"]["peak_rss_bytes"]
        )
        return features["resources"]["peak_rss_bytes"]

    def test_terminal_rust_peak_is_maximum_across_runs_not_sum_of_duplicates(self) -> None:
        smaller = self.resource_line()
        larger = self.resource_line(pid=1235, peak_rss_bytes=128 * 1024**2)
        self.assertEqual(
            128 * 1024**2,
            self.measured_peak([smaller, larger, smaller, larger, smaller]),
        )

    def test_foreign_failed_and_malformed_records_do_not_supply_rss(self) -> None:
        rejected = [
            {"command": "python3"},
            {"command": None},
            {"exit_code": 1},
            {"exit_code": -15},
            {"exit_code": None},
            {"exit_code": 0.0},
            {"pid": 0},
            {"pid": None},
            {"pid": "unknown"},
            {"source": "getrusage"},
            {"source": None},
            {"scope": "stream_boundary"},
            {"scope": None},
            {"peak_rss_bytes": None},
            {"peak_rss_bytes": 0},
            {"peak_rss_bytes": -1},
            {"peak_rss_bytes": 1024.0},
            {"peak_rss_bytes": True},
        ]
        for changes in rejected:
            with self.subTest(changes=changes):
                self.assertIsNone(self.measured_peak([self.resource_line(**changes)]))

    def test_terminal_rss_ignores_foreign_peaks_and_never_uses_phase_fallback(self) -> None:
        phase_logs = [
            "imaging_source_read_ahead_summary mode=cube_slab process_peak_rss_bytes=99999999999",
            "imaging_gridded_replay_summary process_peak_rss_bytes=99999999999",
            "standard_mfs_profile_run peak_rss_bytes=99999999999",
        ]
        self.assertIsNone(self.measured_peak(phase_logs))
        self.assertEqual(
            64 * 1024**2,
            self.measured_peak(phase_logs + [
                self.resource_line(),
                self.resource_line(command="python3", peak_rss_bytes=99999999999),
                self.resource_line(exit_code=1, peak_rss_bytes=99999999999),
            ]),
        )

    def test_missing_terminal_resource_record_keeps_rss_unknown(self) -> None:
        self.assertIsNone(self.measured_peak([]))


if __name__ == "__main__":
    unittest.main()
