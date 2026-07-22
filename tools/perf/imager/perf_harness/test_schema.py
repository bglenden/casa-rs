# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests for the canonical imaging evidence contracts."""

from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import tempfile
import unittest

import perf_harness.schema as schema_contract
from perf_harness import (
    ContractError,
    RUN_RESULT_SCHEMA_VERSION,
    atomic_write_json,
    load_run_result,
    load_workload_manifest,
    validate_workload_manifest,
)
from test_support import canonical_workload_result


REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
VLASS_RECIPE = REPO_ROOT / "tools/perf/imager/recipes/vlass-fragment-tclean.last"
VLASS_RECIPE_SHA256 = "a64e6213d66436fee6d602eb5bbda3ac8667b8df2491ea7310557748bbbf15b5"
VLASS_GEOMETRY = (
    REPO_ROOT / "tools/perf/imager/recipes/vlass-fragment-dataset-geometry.json"
)
VLASS_GEOMETRY_SHA256 = (
    "28b1350f2754e4439a0ac94480eb4efb054ecf03f221c805e98cf34c6b5f77f1"
)
VLASS_RUNTIME = REPO_ROOT / "tools/perf/imager/recipes/vlass-casa-runtime-identity.json"
VLASS_RUNTIME_SHA256 = (
    "7f1d97745f17c770b615b9fbd0ebfe9915b0333e4e9ce6bfb7099de7de721a0b"
)
ALTERNATING_RESULT = (
    REPO_ROOT / "tools/perf/imager/evidence/artifacts/"
    "20260710T152434Z-wave352-mfs-cpu-metal-counterbalanced.json"
)
VLASS_WORKLOADS = {
    "vlass-fragment-single-field.json",
    "vlass-fragment-single-field-cold.json",
    "vlass-fragment-all-fields.json",
    "vlass-fragment-all-fields-cold.json",
    "vlass-fragment-smoke-cold.json",
    "vlass-fragment-smoke-warm.json",
}
VLASS_TURNAROUND_WORKLOAD = "vlass-awproject-turnaround.json"


def explicit_aw_workload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "id": "aw-contract",
        "mode_id": "aw-mtmfs",
        "dataset": {"key": "fixture", "path": "/tmp/fixture.ms"},
        "imaging": {
            "mode": "dirty",
            "specmode": "mfs",
            "gridder": "awproject",
            "casa_gridder": "awproject",
            "wterm": "wproject",
            "wprojplanes": 32,
            "facets": 1,
            "psfphasecenter": "",
            "vptable": "",
            "aterm": True,
            "psterm": False,
            "wbawp": True,
            "conjbeams": True,
            "computepastep": 360.0,
            "rotatepastep": 360.0,
            "pointingoffsetsigdev": 0.0,
        },
        "run": {"cf_cache_role": "warm", "warmups": 1},
        "comparison": {"mode": "sampled"},
    }


def _write_result(path: pathlib.Path, value: dict[str, object]) -> pathlib.Path:
    atomic_write_json(path, value)
    return path


def _legacy_casa_tclean_result() -> dict[str, object]:
    effective_kwargs = {"imagename": "/tmp/casa", "niter": 0}
    encoded_kwargs = json.dumps(
        effective_kwargs,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    before = {
        "user_cpu_seconds": 1.0,
        "system_cpu_seconds": 0.5,
        "peak_rss_bytes": 100,
        "minor_page_faults": 10,
        "major_page_faults": 1,
        "block_inputs": 2,
        "block_outputs": 3,
        "voluntary_context_switches": 4,
        "involuntary_context_switches": 5,
    }
    after = {
        "user_cpu_seconds": 2.0,
        "system_cpu_seconds": 1.0,
        "peak_rss_bytes": 200,
        "minor_page_faults": 12,
        "major_page_faults": 1,
        "block_inputs": 5,
        "block_outputs": 4,
        "voluntary_context_switches": 10,
        "involuntary_context_switches": 8,
    }
    delta = {
        "user_cpu_seconds": 1.0,
        "system_cpu_seconds": 0.5,
        "peak_rss_bytes": 200,
        "minor_page_faults": 2,
        "major_page_faults": 0,
        "block_inputs": 3,
        "block_outputs": 1,
        "voluntary_context_switches": 6,
        "involuntary_context_switches": 3,
    }
    return {
        "schema_version": 2,
        "kind": "casa_tclean_result",
        "status": "completed",
        "request_id": "legacy-casa-result",
        "action": "run",
        "casa": {},
        "recipe": {},
        "compatibility_normalizations": [],
        "version_defaults": {},
        "reproducibility_overrides": {},
        "effective_kwargs": effective_kwargs,
        "effective_kwargs_sha256": hashlib.sha256(encoded_kwargs).hexdigest(),
        "cache": {},
        "mask_identity": None,
        "wall_seconds": 1.0,
        "resources": {"before": before, "after": after, "delta": delta},
        "products": {"before": [], "after": []},
        "tclean_return": {"present": True, "type": "dict"},
    }


class SchemaTests(unittest.TestCase):
    def test_vlass_turnaround_is_explicitly_reduced_but_mode_faithful(self) -> None:
        path = (
            REPO_ROOT
            / "tools/perf/imager/workloads"
            / VLASS_TURNAROUND_WORKLOAD
        )
        manifest = load_workload_manifest(path)

        self.assertIn("never final", manifest["description"])
        self.assertEqual(
            "CASA_RS_VLASS_TURNAROUND_ROOT", manifest["dataset"]["root_env"]
        )
        self.assertEqual("0,1", manifest["imaging"]["field"])
        self.assertEqual("0~3", manifest["imaging"]["spw"])
        self.assertEqual(3, manifest["imaging"]["channel_count"])
        self.assertEqual("awproject", manifest["imaging"]["gridder"])
        self.assertTrue(manifest["imaging"]["usepointing"])
        self.assertEqual("mtmfs", manifest["imaging"]["deconvolver"])
        self.assertEqual(2, manifest["imaging"]["nterms"])
        self.assertEqual("warm", manifest["run"]["cf_cache_role"])
        self.assertEqual(
            "reduced_turnaround_only", manifest["run"]["evidence_role"]
        )
        self.assertTrue(manifest["comparison"]["tolerances"]["require_full_array"])

    def test_current_product_contract_accepts_bound_source_region_evidence(
        self,
    ) -> None:
        product = {
            "status": "compared",
            "review_panel": {
                "status": "written",
                "zoom_panel": {
                    "status": "written",
                    "path": "/tmp/zoom.png",
                    "sha256": "1" * 64,
                    "left_label": "CASA measured 1",
                    "right_label": "CASA measured 1 product contract",
                    "bounds": {
                        "x_start": 1,
                        "x_end": 2,
                        "y_start": 3,
                        "y_end": 4,
                    },
                    "casa_rs_and_casa_color_limits": [-1.0, 1.0],
                    "left_and_right_color_limits": [-1.0, 1.0],
                    "difference_color_limits": [-0.1, 0.1],
                },
            },
            "source_regions": [
                {
                    "id": "bright-source",
                    "products": [".image.tt0"],
                    "blc": [1, 2],
                    "trc": [3, 4],
                    "method": "bounded-test",
                    "left": {},
                    "right": {},
                }
            ],
        }

        schema_contract._validate_comparison_product(
            product,
            protocol_variant=schema_contract.COMPARISON_SCHEMA_VERSION,
            source="test product",
        )

        with self.assertRaisesRegex(ContractError, "unknown field"):
            schema_contract._validate_comparison_product(
                product,
                protocol_variant=3,
                source="legacy product",
            )

        malformed = {"status": "compared", "source_regions": {}}
        with self.assertRaisesRegex(ContractError, "must be a list"):
            schema_contract._validate_comparison_product(
                malformed,
                protocol_variant=schema_contract.COMPARISON_SCHEMA_VERSION,
                source="malformed product",
            )

    def test_workload_rejects_unknown_and_unversioned_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "workload.json"
            path.write_text(json.dumps({"id": "old"}), encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "schema_version"):
                load_workload_manifest(path)
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "id": "test",
                        "mode_id": "test",
                        "dataset": {},
                        "imaging": {},
                        "legacy_alias": True,
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ContractError, "unknown workload"):
                load_workload_manifest(path)

    def test_workload_rejects_unknown_nested_fields_and_wrong_types(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "workload.json"
            base = {
                "schema_version": 1,
                "id": "test",
                "mode_id": "test",
                "dataset": {"key": "fixture", "path": "/tmp/fixture.ms"},
                "imaging": {
                    "specmode": "mfs",
                    "gridder": "standard",
                    "mode": "dirty",
                },
            }
            invalid = json.loads(json.dumps(base))
            invalid["imaging"]["private_alias"] = True
            atomic_write_json(path, invalid)
            with self.assertRaisesRegex(ContractError, "unknown field"):
                load_workload_manifest(path)
            invalid = json.loads(json.dumps(base))
            invalid["imaging"]["imsize"] = "1024"
            atomic_write_json(path, invalid)
            with self.assertRaisesRegex(ContractError, "imsize must be an integer"):
                load_workload_manifest(path)

    def test_result_rejects_legacy_version_and_nonfinite_json(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            result = root / "result.json"
            atomic_write_json(result, canonical_workload_result())
            self.assertEqual("completed", load_run_result(result)["status"])
            result.write_text(
                '{"schema_version": 1, "status": "completed", "results": {}}\n',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ContractError, "schema_version"):
                load_run_result(result)
            with self.assertRaises(ValueError):
                atomic_write_json(result, {"value": float("nan")})

    def test_failure_states_require_typed_failure_record(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "failure.json"
            base = canonical_workload_result(status="failed_execution")
            base["exit_code"] = 2
            atomic_write_json(path, base)
            with self.assertRaisesRegex(ContractError, "results.failure"):
                load_run_result(path)
            base["results"]["failure"] = {
                "kind": "execution",
                "reason": "process exited 2",
                "return_code": 2,
            }
            atomic_write_json(path, base)
            self.assertEqual("failed_execution", load_run_result(path)["status"])

    def test_result_rejects_wrong_canonical_field_types(self) -> None:
        invalid_fields = {
            "created_at": False,
            "completed_at": False,
            "exit_code": "not-an-int",
            "products": "not-an-object",
            "logs": 17,
        }
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "invalid.json"
            for field, value in invalid_fields.items():
                with self.subTest(field=field):
                    invalid = canonical_workload_result()
                    invalid[field] = value
                    atomic_write_json(path, invalid)
                    with self.assertRaises(ContractError):
                        load_run_result(path)

    def test_result_rejects_unknown_nested_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "invalid.json"
            for section in ("environment", "artifacts", "results"):
                with self.subTest(section=section):
                    invalid = canonical_workload_result()
                    invalid[section]["private_alias"] = True
                    atomic_write_json(path, invalid)
                    with self.assertRaisesRegex(ContractError, "unknown field"):
                        load_run_result(path)

    def test_environment_path_provenance_matches_live_or_migrated_variant(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "environment-variant.json"
            live = canonical_workload_result()
            live["environment"]["executables"]["imager"] = {
                "path": "/repo/imager",
                "exists_at_migration": True,
            }
            with self.assertRaisesRegex(
                ContractError, "environment variant requires exists provenance"
            ):
                load_run_result(_write_result(path, live))

            migrated = canonical_workload_result()
            migrated["environment"]["migration"] = {
                "source_schema_version": 2,
                "method": "unit-test",
            }
            migrated["environment"]["executables"]["imager"] = {
                "path": "/repo/imager",
                "exists": True,
                "kind": "file",
            }
            with self.assertRaisesRegex(
                ContractError,
                "environment variant requires exists_at_migration provenance",
            ):
                load_run_result(_write_result(path, migrated))

    def test_result_rejects_malformed_nested_comparison_and_call_evidence(
        self,
    ) -> None:
        mutations = (
            (
                "product comparison schema version",
                lambda result: result["results"]["product_comparison"].update(
                    {"schema_version": False}
                ),
            ),
            (
                "versioned comparison missing request binding",
                lambda result: result["results"]["product_comparison"].update(
                    {"schema_version": 4}
                ),
            ),
            (
                "unversioned comparison using current protocol field",
                lambda result: result["results"]["product_comparison"].update(
                    {"request_sha256": "1" * 64}
                ),
            ),
            (
                "schema-v3 comparison using schema-v4 workspace field",
                lambda result: result["results"]["product_comparison"].update(
                    {
                        "schema_version": 3,
                        "request_binding": {},
                        "structure_workspace_dir": "/tmp/structure",
                    }
                ),
            ),
            (
                "product comparison chunk budget",
                lambda result: result["results"]["product_comparison"].update(
                    {"full_chunk_elements": "bad"}
                ),
            ),
            (
                "product comparison product fields",
                lambda result: result["results"]["product_comparison"][
                    "products"
                ].update({".image": {"private_alias": True}}),
            ),
            (
                "product comparison products container",
                lambda result: result["results"]["product_comparison"].update(
                    {"products": []}
                ),
            ),
            (
                "product comparison product scalar",
                lambda result: result["results"]["product_comparison"][
                    "products"
                ].update({".image": {"status": "compared", "sampled_elements": "bad"}}),
            ),
            (
                "partial CASA call fields",
                lambda result: result["results"].update(
                    {"casa_tclean_calls": {"partial": [{"private_alias": True}]}}
                ),
            ),
            (
                "partial CASA calls container",
                lambda result: result["results"].update(
                    {"casa_tclean_calls": {"partial": {}}}
                ),
            ),
            (
                "measured CASA call fields",
                lambda result: result["results"].update(
                    {"casa_tclean_calls": {"measured": [{"private_alias": True}]}}
                ),
            ),
            (
                "measured CASA call scalar",
                lambda result: result["results"].update(
                    {"casa_tclean_calls": {"measured": [{"exit_code": False}]}}
                ),
            ),
            (
                "repeatability comparison fields",
                lambda result: result["results"].update(
                    {
                        "casa_repeatability_comparison": {
                            "status": "comparison_incomplete",
                            "comparisons": [{"private_alias": True}],
                            "products": {},
                        }
                    }
                ),
            ),
            (
                "repeatability comparisons container",
                lambda result: result["results"].update(
                    {
                        "casa_repeatability_comparison": {
                            "status": "comparison_incomplete",
                            "comparisons": {},
                            "products": {},
                        }
                    }
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "invalid.json"
            for status in ("completed", "failed_execution"):
                for label, mutate in mutations:
                    with self.subTest(status=status, mutation=label):
                        invalid = canonical_workload_result(status=status)
                        if status == "failed_execution":
                            invalid["exit_code"] = 2
                            invalid["results"]["failure"] = {
                                "kind": "execution",
                                "reason": "synthetic failure",
                            }
                        mutate(invalid)
                        with self.assertRaises(ContractError):
                            load_run_result(_write_result(path, invalid))

    def test_live_unversioned_comparison_is_only_a_terminal_non_evidence_summary(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "invalid-live-comparison.json"
            invalid_summaries = (
                {
                    "status": "completed",
                    "reason": "synthetic",
                    "products": {},
                },
                {
                    "status": "unavailable",
                    "reason": "synthetic",
                    "products": {".image": {"status": "compared"}},
                },
            )
            for summary in invalid_summaries:
                with self.subTest(summary=summary):
                    invalid = canonical_workload_result()
                    invalid["results"]["product_comparison"] = summary
                    with self.assertRaises(ContractError):
                        load_run_result(_write_result(path, invalid))

    def test_legacy_comparison_variants_reject_current_nested_fields(self) -> None:
        mutations = (
            lambda comparison: comparison["products"][".image"].update(
                {"full_array": {}}
            ),
            lambda comparison: comparison.update(
                {
                    "beam_info": {
                        "status": "estimated",
                        "coordinate_domain": "native",
                    }
                }
            ),
            lambda comparison: comparison["products"][".image"].update(
                {
                    "structured_difference": {
                        "status": "computed",
                        "native_spatial_evidence": {},
                    }
                }
            ),
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "invalid-v3-comparison.json"
            for mutate in mutations:
                invalid = canonical_workload_result()
                invalid["environment"]["migration"] = {
                    "source_schema_version": 2,
                    "method": "unit-test",
                }
                comparison = {
                    "schema_version": 3,
                    "request_binding": {},
                    "status": "completed",
                    "products": {".image": {"status": "compared"}},
                }
                mutate(comparison)
                invalid["results"]["product_comparison"] = comparison
                with self.assertRaisesRegex(ContractError, "unknown field"):
                    load_run_result(_write_result(path, invalid))

    def test_result_accepts_only_closed_schema_v2_embedded_casa_results(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "legacy-casa.json"
            legacy = _legacy_casa_tclean_result()
            valid = canonical_workload_result(
                extra_results={"casa_tclean_calls": {"measured": [{"result": legacy}]}}
            )
            self.assertEqual(
                "completed", load_run_result(_write_result(path, valid))["status"]
            )

            invalid = copy.deepcopy(valid)
            invalid["results"]["casa_tclean_calls"]["measured"][0]["result"][
                "private_alias"
            ] = True
            with self.assertRaisesRegex(ContractError, "unknown field"):
                load_run_result(_write_result(path, invalid))

            invalid = copy.deepcopy(valid)
            invalid["results"]["casa_tclean_calls"]["measured"][0]["result"][
                "resources"
            ]["delta"]["minor_page_faults"] = 3
            with self.assertRaisesRegex(ContractError, "does not match after-before"):
                load_run_result(_write_result(path, invalid))

    def test_result_kind_status_and_failure_shapes_are_discriminated(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "invalid.json"
            invalid = canonical_workload_result()
            invalid["kind"] = "simobserve_benchmark"
            with self.assertRaisesRegex(ContractError, "invalid for simobserve"):
                load_run_result(_write_result(path, invalid))

            invalid = canonical_workload_result()
            invalid["results"]["failure"] = {
                "kind": "execution",
                "reason": "unexpected",
            }
            with self.assertRaisesRegex(ContractError, "forbids results.failure"):
                load_run_result(_write_result(path, invalid))

            invalid = canonical_workload_result(status="failed_execution")
            invalid["exit_code"] = 2
            invalid["results"]["failure"] = {
                "kind": "execution",
                "reason": "failed",
                "private_alias": True,
            }
            with self.assertRaisesRegex(ContractError, "unknown field"):
                load_run_result(_write_result(path, invalid))

    def test_alternating_result_rejects_unknown_and_wrong_nested_fields(self) -> None:
        valid = json.loads(ALTERNATING_RESULT.read_text(encoding="utf-8"))
        self.assertEqual(RUN_RESULT_SCHEMA_VERSION, valid["schema_version"])
        details = valid["results"]["alternating_comparison"]
        self.assertTrue(details["schedule"])
        self.assertTrue(details["runs"])
        self.assertTrue(details["paired_deltas"])

        mutations = (
            (("configuration", "private_alias"), True),
            (("configuration", "measured_pair_count"), False),
            (("verdict", "private_alias"), True),
            (("verdict", "no_slowdown"), "yes"),
            (("schedule", 0, "private_alias"), True),
            (("runs", 0, "total_wall_seconds"), "slow"),
            (("paired_deltas", 0, "private_alias"), True),
            (("paired_delta_summary", "delta_seconds", "count"), False),
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "invalid.json"
            for pointer, replacement in mutations:
                with self.subTest(pointer=pointer):
                    invalid = copy.deepcopy(valid)
                    target = invalid["results"]["alternating_comparison"]
                    for component in pointer[:-1]:
                        target = target[component]
                    target[pointer[-1]] = replacement
                    with self.assertRaises(ContractError):
                        load_run_result(_write_result(path, invalid))

    def test_failed_repeatability_can_retain_nullable_comparison_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "failed-comparison.json"
            result = canonical_workload_result(status="failed_comparison")
            result["exit_code"] = 1
            result["results"]["failure"] = {
                "kind": "comparison",
                "reason": "comparison incomplete",
            }
            result["results"]["casa_repeatability_comparison"] = {
                "status": "comparison_incomplete",
                "reason": "comparison produced no product measurements",
                "products": {},
                "product_inventory": None,
                "structured_difference_review": None,
                "tolerances": None,
            }

            self.assertEqual(
                "failed_comparison",
                load_run_result(_write_result(path, result))["status"],
            )

            result["results"]["casa_repeatability_comparison"]["products"] = None
            with self.assertRaisesRegex(ContractError, "products must be an object"):
                load_run_result(_write_result(path, result))

    def test_checked_in_vlass_manifests_preserve_recipe_and_execution_scope(
        self,
    ) -> None:
        workloads = REPO_ROOT / "tools/perf/imager/workloads"
        loaded = {
            name: load_workload_manifest(workloads / name) for name in VLASS_WORKLOADS
        }

        recipe_digest = hashlib.sha256(VLASS_RECIPE.read_bytes()).hexdigest()
        self.assertEqual(VLASS_RECIPE_SHA256, recipe_digest)
        self.assertEqual(
            VLASS_GEOMETRY_SHA256,
            hashlib.sha256(VLASS_GEOMETRY.read_bytes()).hexdigest(),
        )
        self.assertEqual(
            VLASS_RUNTIME_SHA256,
            hashlib.sha256(VLASS_RUNTIME.read_bytes()).hexdigest(),
        )
        for manifest in loaded.values():
            self.assertEqual(1, manifest["schema_version"])
            self.assertEqual("CASA_RS_VLASS_DATA_ROOT", manifest["dataset"]["root_env"])
            self.assertEqual(
                "VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms",
                manifest["dataset"]["relative_path"],
            )
            self.assertEqual("1", manifest["run"]["skip_rust"])
            self.assertEqual(
                "tools/perf/imager/recipes/vlass-fragment-tclean.last",
                manifest["casa"]["recipe_path"],
            )
            self.assertEqual(VLASS_RECIPE_SHA256, manifest["casa"]["recipe_sha256"])
            self.assertEqual(
                VLASS_GEOMETRY_SHA256,
                manifest["casa"]["dataset_geometry_sha256"],
            )
            expected_selection = (
                "all_fields"
                if manifest["id"].startswith("vlass-fragment-all-fields")
                else "single_field_spw9"
                if "smoke" in manifest["id"]
                else "single_field"
            )
            self.assertEqual(expected_selection, manifest["casa"]["dataset_selection"])
            self.assertEqual(
                VLASS_RUNTIME_SHA256,
                manifest["casa"]["runtime_identity_sha256"],
            )
            self.assertEqual("awproject", manifest["imaging"]["casa_gridder"])
            self.assertEqual(1, manifest["imaging"]["chanchunks"])
            self.assertEqual("", manifest["imaging"]["psfphasecenter"])
            self.assertEqual("", manifest["imaging"]["vptable"])
            self.assertEqual(0.0, manifest["imaging"]["pointingoffsetsigdev"])
            self.assertTrue(manifest["comparison"]["require_exact_product_inventory"])
            self.assertTrue(manifest["comparison"]["require_metadata_parity"])
            self.assertEqual(
                {
                    ".alpha",
                    ".alpha.error",
                    ".image.tt0",
                    ".image.tt1",
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
                },
                set(manifest["comparison"]["products"]),
            )

        self.assertEqual(
            "1525", loaded["vlass-fragment-single-field.json"]["imaging"]["field"]
        )
        self.assertEqual(
            "1107~1127,1512~1532,1542~1562",
            loaded["vlass-fragment-all-fields.json"]["imaging"]["field"],
        )
        self.assertEqual(
            ("cold", 0),
            (
                loaded["vlass-fragment-smoke-cold.json"]["run"]["cf_cache_role"],
                loaded["vlass-fragment-smoke-cold.json"]["run"]["warmups"],
            ),
        )
        self.assertEqual(
            ("warm", 1),
            (
                loaded["vlass-fragment-smoke-warm.json"]["run"]["cf_cache_role"],
                loaded["vlass-fragment-smoke-warm.json"]["run"]["warmups"],
            ),
        )
        for stem in ("single-field", "all-fields"):
            cold = loaded[f"vlass-fragment-{stem}-cold.json"]
            warm = loaded[f"vlass-fragment-{stem}.json"]
            self.assertEqual(
                ("cold", 0), (cold["run"]["cf_cache_role"], cold["run"]["warmups"])
            )
            self.assertEqual(
                ("warm", 1), (warm["run"]["cf_cache_role"], warm["run"]["warmups"])
            )
            for manifest in (cold, warm):
                self.assertIn(
                    manifest["run"]["evidence_role"],
                    manifest["review"]["required_evidence_roles"],
                )
            cold_common = copy.deepcopy(cold)
            warm_common = copy.deepcopy(warm)
            for manifest in (cold_common, warm_common):
                manifest.pop("id")
                manifest.pop("description")
                for field in (
                    "cf_cache_role",
                    "evidence_role",
                    "repeats",
                    "run_label",
                    "warmups",
                ):
                    manifest["run"].pop(field)
            self.assertEqual(
                cold_common,
                warm_common,
                f"VLASS {stem} cold/warm manifests drifted outside run-role fields",
            )
        for name in (
            "vlass-fragment-smoke-cold.json",
            "vlass-fragment-smoke-warm.json",
        ):
            comparison = loaded[name]["comparison"]
            self.assertEqual("full", comparison["mode"])
            self.assertTrue(comparison["tolerances"]["require_full_array"])

    def test_casa_recipe_requires_path_and_lowercase_sha256(self) -> None:
        workload = explicit_aw_workload()
        workload["casa"] = {
            "recipe_path": "recipe.last",
            "recipe_sha256": VLASS_RECIPE_SHA256,
        }
        validate_workload_manifest(workload)

        invalid = copy.deepcopy(workload)
        invalid["casa"].pop("recipe_path")
        with self.assertRaisesRegex(ContractError, "recipe_path"):
            validate_workload_manifest(invalid)

        invalid = copy.deepcopy(workload)
        invalid["casa"]["recipe_sha256"] = VLASS_RECIPE_SHA256.upper()
        with self.assertRaisesRegex(ContractError, "lowercase SHA-256"):
            validate_workload_manifest(invalid)

        invalid = copy.deepcopy(workload)
        invalid["casa"]["runtime_identity_path"] = "runtime.json"
        with self.assertRaisesRegex(ContractError, "must be set together"):
            validate_workload_manifest(invalid)

        workload["casa"].update(
            {
                "runtime_identity_path": "runtime.json",
                "runtime_identity_sha256": "1" * 64,
                "dataset_geometry_path": "geometry.json",
                "dataset_geometry_sha256": "2" * 64,
                "dataset_selection": "single-field-1525",
            }
        )
        validate_workload_manifest(workload)

    def test_explicit_aw_surface_is_complete_and_cannot_downgrade_casa(self) -> None:
        workload = explicit_aw_workload()
        validate_workload_manifest(workload)

        missing_control = copy.deepcopy(workload)
        missing_control["imaging"].pop("psterm")
        with self.assertRaisesRegex(
            ContractError, "explicit AW imaging requires fields"
        ):
            validate_workload_manifest(missing_control)

        downgraded = copy.deepcopy(workload)
        downgraded["imaging"]["casa_gridder"] = "wproject"
        with self.assertRaisesRegex(ContractError, "cannot downgrade casa_gridder"):
            validate_workload_manifest(downgraded)

        no_cache = copy.deepcopy(workload)
        no_cache["run"] = {"cf_cache_role": "none", "warmups": 0}
        with self.assertRaisesRegex(ContractError, "require run.cf_cache_role"):
            validate_workload_manifest(no_cache)

        bad_pointing_sigma = copy.deepcopy(workload)
        bad_pointing_sigma["imaging"]["pointingoffsetsigdev"] = "0.0"
        with self.assertRaisesRegex(ContractError, "must be a finite number"):
            validate_workload_manifest(bad_pointing_sigma)

    def test_cf_cache_roles_enforce_warmup_contract(self) -> None:
        workload = explicit_aw_workload()

        no_warmup = copy.deepcopy(workload)
        no_warmup["run"]["warmups"] = 0
        with self.assertRaisesRegex(ContractError, "warm requires run.warmups >= 1"):
            validate_workload_manifest(no_warmup)

        cold_with_warmup = copy.deepcopy(workload)
        cold_with_warmup["run"] = {"cf_cache_role": "cold", "warmups": 1}
        with self.assertRaisesRegex(ContractError, "cold requires run.warmups=0"):
            validate_workload_manifest(cold_with_warmup)

        invalid_role = copy.deepcopy(workload)
        invalid_role["run"] = {"cf_cache_role": "ambient", "warmups": 0}
        with self.assertRaisesRegex(ContractError, "must be none, cold, or warm"):
            validate_workload_manifest(invalid_role)

    def test_rust_cf_cache_controls_are_typed_and_awproject_only(self) -> None:
        workload = explicit_aw_workload()
        workload["imaging"]["cfcache"] = "turnaround.cf"
        workload["imaging"]["cf_resident_mb"] = 384
        validate_workload_manifest(workload)

        zero_residency = copy.deepcopy(workload)
        zero_residency["imaging"]["cf_resident_mb"] = 0
        with self.assertRaisesRegex(ContractError, "cf_resident_mb must be >= 1"):
            validate_workload_manifest(zero_residency)

        missing_cache = copy.deepcopy(workload)
        del missing_cache["imaging"]["cfcache"]
        with self.assertRaisesRegex(ContractError, "requires imaging.cfcache"):
            validate_workload_manifest(missing_cache)

        wrong_gridder = copy.deepcopy(workload)
        wrong_gridder["imaging"]["gridder"] = "wproject"
        with self.assertRaisesRegex(ContractError, "requires imaging.gridder=awproject"):
            validate_workload_manifest(wrong_gridder)

    def test_deterministic_user_mask_requires_path_and_digest(self) -> None:
        workload = {
            "schema_version": 1,
            "id": "clean-mask",
            "mode_id": "standard-clean",
            "dataset": {"key": "fixture", "path": "/tmp/fixture.ms"},
            "imaging": {
                "mode": "clean",
                "specmode": "mfs",
                "gridder": "standard",
                "niter": 10,
                "usemask": "user",
            },
        }
        with self.assertRaisesRegex(ContractError, "deterministic user-mask clean"):
            validate_workload_manifest(workload)

        workload["imaging"]["mask_image"] = "masks/clean.mask"
        with self.assertRaisesRegex(ContractError, "must be set together"):
            validate_workload_manifest(workload)

        workload["imaging"]["mask_sha256"] = "1" * 64
        validate_workload_manifest(workload)

        workload["imaging"]["mask_sha256"] = "not-a-digest"
        with self.assertRaisesRegex(ContractError, "lowercase SHA-256"):
            validate_workload_manifest(workload)

    def test_full_comparison_requires_bounded_exact_metadata_contract(self) -> None:
        workload = {
            "schema_version": 1,
            "id": "full-comparison",
            "mode_id": "standard-dirty",
            "dataset": {"key": "fixture", "path": "/tmp/fixture.ms"},
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
            },
            "comparison": {
                "mode": "full",
                "full_chunk_elements": 1_000_000,
                "max_elements_per_product": 1_000_000,
                "products": [".image"],
                "require_exact_product_inventory": True,
                "require_metadata_parity": True,
                "tolerances": {
                    "contract_version": 1,
                    "require_full_array": True,
                    "default": {"diff_rms_over_right_rms": 1e-3},
                    "products": {},
                },
            },
        }
        validate_workload_manifest(workload)

        for key, message in (
            ("full_chunk_elements", "full_chunk_elements"),
            ("require_exact_product_inventory", "require_exact_product_inventory"),
            ("require_metadata_parity", "require_metadata_parity"),
            ("products", "explicit product inventory"),
            ("tolerances", "frozen tolerances"),
        ):
            invalid = copy.deepcopy(workload)
            invalid["comparison"].pop(key)
            with self.assertRaisesRegex(ContractError, message):
                validate_workload_manifest(invalid)

        invalid_mode = copy.deepcopy(workload)
        invalid_mode["comparison"]["mode"] = "stride"
        with self.assertRaisesRegex(ContractError, "must be full or sampled"):
            validate_workload_manifest(invalid_mode)

    def test_numerical_tolerance_contract_is_strictly_validated(self) -> None:
        workload = explicit_aw_workload()
        workload["comparison"]["tolerances"] = {
            "contract_version": 1,
            "require_full_array": False,
            "default": {
                "diff_rms_over_right_rms": 1e-3,
                "diff_abs_max_over_right_peak": 5e-3,
                "allowed_structure_labels": ["good"],
            },
            "products": {".image.tt0": {"beam_major_relative": 1e-3}},
        }
        validate_workload_manifest(workload)

        invalid = copy.deepcopy(workload)
        invalid["comparison"]["tolerances"]["default"]["magic"] = 1.0
        with self.assertRaisesRegex(ContractError, "unknown field"):
            validate_workload_manifest(invalid)

    def test_source_regions_are_full_mode_and_product_bound(self) -> None:
        workload = {
            "schema_version": 1,
            "id": "source-regions",
            "mode_id": "standard-dirty",
            "dataset": {"key": "fixture", "path": "/tmp/fixture.ms"},
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
            },
            "comparison": {
                "mode": "full",
                "full_chunk_elements": 1024,
                "products": [".image"],
                "require_exact_product_inventory": True,
                "require_metadata_parity": True,
                "source_regions": [
                    {
                        "id": "source-1",
                        "products": [".image"],
                        "blc": [10, 20],
                        "trc": [30, 40],
                    }
                ],
                "tolerances": {
                    "contract_version": 1,
                    "require_full_array": True,
                    "default": {"diff_rms_over_right_rms": 1e-3},
                    "products": {},
                },
            },
        }
        validate_workload_manifest(workload)

        invalid = copy.deepcopy(workload)
        invalid["comparison"]["source_regions"][0]["products"] = [".residual"]
        with self.assertRaisesRegex(ContractError, "not in comparison.products"):
            validate_workload_manifest(invalid)

        sampled = copy.deepcopy(workload)
        sampled["comparison"]["mode"] = "sampled"
        with self.assertRaisesRegex(ContractError, "require comparison.mode=full"):
            validate_workload_manifest(sampled)


if __name__ == "__main__":
    unittest.main()
