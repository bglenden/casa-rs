#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for bounded full CASA-image comparison."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import pathlib
import tempfile
import unittest
from unittest import mock

import numpy as np

from perf_harness import casa_image_compare as comparator
from perf_harness.image_compare import (
    apply_tolerance_contract,
    compare_products,
    normalize_comparison_request,
    validate_comparison_output,
)
from perf_harness.casa_protocol import CasaProtocolResult


class FakeImageTool:
    def __init__(self, factory):
        self.factory = factory
        self.path = None

    def open(self, path):
        self.path = str(path)

    def shape(self):
        return list(self.factory.data[self.path].shape)

    def getchunk(
        self,
        blc,
        trc,
        inc,
        axes=None,
        list=False,
        dropdeg=False,
        getmask=False,
    ):
        del axes, list, dropdeg
        slices = tuple(
            slice(start, end + 1, step) for start, end, step in zip(blc, trc, inc)
        )
        source = (
            self.factory.masks[self.path] if getmask else self.factory.data[self.path]
        )
        chunk = source[slices]
        key = (self.path, bool(getmask))
        self.factory.visits[key][slices] += 1
        self.factory.chunk_sizes.append(int(chunk.size))
        return chunk.copy()

    def close(self):
        return None


class FakeImageFactory:
    def __init__(self, data, masks):
        self.data = data
        self.masks = masks
        self.visits = {
            (path, getmask): np.zeros(values.shape, dtype=np.int64)
            for path, values in data.items()
            for getmask in (False, True)
        }
        self.chunk_sizes = []

    def __call__(self):
        return FakeImageTool(self)


class FakeCoordinateTool:
    def __init__(self, record):
        self.record = record

    def torecord(self):
        return self.record

    def done(self):
        return None


class FakeMetadataTool:
    def __init__(self, factory):
        self.factory = factory
        self.path = None

    def open(self, path):
        self.path = str(path)

    def shape(self):
        return self.factory.records[self.path]["shape"]

    def brightnessunit(self):
        return self.factory.records[self.path]["unit"]

    def coordsys(self):
        return FakeCoordinateTool(self.factory.records[self.path]["coordinates"])

    def restoringbeam(self):
        return self.factory.records[self.path]["restoring_beam"]

    def maskhandler(self, operation):
        if operation != "get":
            raise ValueError(operation)
        return self.factory.records[self.path]["masks"]

    def close(self):
        return None


class FakeMetadataFactory:
    def __init__(self, records):
        self.records = records

    def __call__(self):
        return FakeMetadataTool(self)


class ImageComparisonProtocolTests(unittest.TestCase):
    def test_host_and_casa_schema_v4_bind_the_same_workspace_request(self) -> None:
        request = normalize_comparison_request(comparison_request())

        self.assertEqual(4, request["schema_version"])
        self.assertEqual(request, comparator.normalized_request(copy.deepcopy(request)))

        moved = comparison_request()
        moved["structure_workspace_dir"] = "/evidence/other-structure-workspace"
        moved = normalize_comparison_request(moved)
        self.assertNotEqual(request["request_sha256"], moved["request_sha256"])

    def test_comparator_output_is_bound_to_every_normalized_request_field(self) -> None:
        request = normalize_comparison_request(comparison_request())
        output = comparison_output(request)
        validate_comparison_output(output, request)

        mutations = {
            "digest": lambda value: value.__setitem__("request_sha256", "0" * 64),
            "mode": lambda value: value.__setitem__("comparison_mode", "sampled"),
            "prefix": lambda value: value.__setitem__("left_prefix", "/wrong"),
            "label": lambda value: value.__setitem__("right_label", "wrong"),
            "product_set": lambda value: value["requested_products"].pop(),
            "inventory_policy": lambda value: value.__setitem__(
                "require_exact_product_inventory", False
            ),
            "source_id": lambda value: value["source_regions"][0].__setitem__(
                "id", "wrong"
            ),
            "source_blc": lambda value: value["source_regions"][0]["blc"].__setitem__(
                0, 7
            ),
            "source_trc": lambda value: value["source_regions"][0]["trc"].__setitem__(
                1, 99
            ),
            "source_products": lambda value: value["source_regions"][0][
                "products"
            ].pop(),
            "tolerances": lambda value: value["tolerances"]["default"].__setitem__(
                "diff_rms_over_right_rms", 0.5
            ),
            "missing_product": lambda value: value["products"].pop(".image.tt0"),
            "product_path": lambda value: value["products"][".image.tt0"].__setitem__(
                "left_path", "/wrong.image.tt0"
            ),
            "structure_workspace": lambda value: value.__setitem__(
                "structure_workspace_dir", "/wrong/workspace"
            ),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                changed = copy.deepcopy(output)
                mutate(changed)
                with self.assertRaisesRegex(ValueError, "does not match"):
                    validate_comparison_output(changed, request)

    def test_comparator_facade_exposes_hashes_for_request_output_and_log(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            artifact_prefix = root / "comparison"
            workspace = root / "structure-workspace"
            raw_request = comparison_request(tolerances=None)
            raw_request["structure_workspace_dir"] = str(workspace)

            def protocol(**kwargs):
                request = kwargs["request"]
                request_path = kwargs["request_path"]
                output_path = kwargs["output_path"]
                log_path = kwargs["log_path"]
                request_path.write_text(json.dumps(request), encoding="utf-8")
                output = comparison_output(request)
                write_pending_structure_workspace(request)
                payload = (json.dumps(output, sort_keys=True) + "\n").encode()
                output_path.write_bytes(payload)
                log_path.write_text("CASA comparator log\n", encoding="utf-8")
                return CasaProtocolResult(
                    status="completed",
                    return_code=0,
                    output=output,
                    output_sha256=hashlib.sha256(payload).hexdigest(),
                    reason=None,
                    request_path=request_path,
                    output_path=output_path,
                    log_path=log_path,
                )

            with mock.patch(
                "perf_harness.image_compare.run_json_file_protocol",
                side_effect=protocol,
            ):
                result = compare_products(
                    casa_python="/casa/python",
                    request=raw_request,
                    artifact_prefix=artifact_prefix,
                    cwd=root,
                )

            self.assertFalse(workspace.exists())

        self.assertEqual("completed", result["status"])
        for field in ("input", "output", "log"):
            self.assertTrue(
                result[field].endswith(
                    {
                        "input": ".comparison-input.json",
                        "output": ".comparison.json",
                        "log": ".comparison.log",
                    }[field]
                )
            )
            self.assertRegex(result[f"{field}_sha256"], r"^[0-9a-f]{64}$")

    def test_facade_persists_and_enforces_frozen_tolerances(self) -> None:
        contract = {
            "contract_version": 1,
            "require_full_array": True,
            "default": {
                "diff_rms_over_right_rms": 0.001,
                "require_topology_parity": True,
                "allowed_structure_labels": ["good"],
            },
            "products": {},
        }
        comparison = {
            "status": "completed",
            "comparison_mode": "full",
            "source_regions": [],
            "products": {
                ".image": {
                    "status": "compared",
                    "diff_rms_over_right_rms": 0.001,
                    "topology_parity": True,
                    "structured_difference": {
                        "status": "computed",
                        "classification": {"overall": "good"},
                        "review": {"label": "good"},
                    },
                }
            },
        }
        passed = apply_tolerance_contract(
            copy.deepcopy(comparison), {"tolerances": contract}
        )
        self.assertEqual("completed", passed["status"])
        self.assertEqual("passed", passed["tolerance_evaluation"]["status"])

        comparison["products"][".image"]["diff_rms_over_right_rms"] = 0.0011
        failed = apply_tolerance_contract(comparison, {"tolerances": contract})
        self.assertEqual("out_of_tolerance", failed["status"])
        self.assertEqual("failed", failed["tolerance_evaluation"]["status"])

        operational_failure = apply_tolerance_contract(
            {
                "status": "failed_execution",
                "reason": "comparison runtime unavailable",
                "products": {},
            },
            {"tolerances": contract},
        )
        self.assertEqual("failed_execution", operational_failure["status"])
        self.assertNotIn("tolerance_evaluation", operational_failure)

    def test_nonaccepted_structure_review_retains_exact_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            workspace = root / "structure-workspace"
            raw_request = comparison_request(tolerances=None)
            raw_request["structure_workspace_dir"] = str(workspace)

            def protocol(**kwargs):
                request = kwargs["request"]
                output = comparison_output(request)
                replace_structure_with_bad_review(output, ".image.tt0")
                return write_protocol_result(kwargs, output, request)

            with mock.patch(
                "perf_harness.image_compare.run_json_file_protocol",
                side_effect=protocol,
            ):
                result = compare_products(
                    casa_python="/casa/python",
                    request=raw_request,
                    artifact_prefix=root / "comparison",
                    cwd=root,
                )

            self.assertEqual("structure_review_not_accepted", result["status"])
            self.assertEqual("structured_difference_review", result["failure"]["kind"])
            self.assertTrue(workspace.is_dir())
            self.assertTrue((workspace / "failure.json").is_file())

    def test_validation_failure_retains_exact_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            workspace = root / "structure-workspace"
            raw_request = comparison_request(tolerances=None)
            raw_request["structure_workspace_dir"] = str(workspace)

            def protocol(**kwargs):
                request = kwargs["request"]
                output = comparison_output(request)
                output["products"][".image.tt0"]["source_regions"][0]["blc"] = [1, 0]
                return write_protocol_result(kwargs, output, request)

            with mock.patch(
                "perf_harness.image_compare.run_json_file_protocol",
                side_effect=protocol,
            ):
                result = compare_products(
                    casa_python="/casa/python",
                    request=raw_request,
                    artifact_prefix=root / "comparison",
                    cwd=root,
                )

            self.assertEqual("failed_validation", result["status"])
            self.assertEqual("comparison_protocol_binding", result["failure"]["kind"])
            self.assertTrue(workspace.is_dir())

    def test_tolerance_rejection_retains_exact_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            workspace = root / "structure-workspace"
            raw_request = comparison_request()
            raw_request["structure_workspace_dir"] = str(workspace)

            def protocol(**kwargs):
                request = kwargs["request"]
                output = comparison_output(request)
                return write_protocol_result(kwargs, output, request)

            with mock.patch(
                "perf_harness.image_compare.run_json_file_protocol",
                side_effect=protocol,
            ):
                result = compare_products(
                    casa_python="/casa/python",
                    request=raw_request,
                    artifact_prefix=root / "comparison",
                    cwd=root,
                )

            self.assertEqual("out_of_tolerance", result["status"])
            self.assertTrue(workspace.is_dir())
            self.assertTrue((workspace / "failure.json").is_file())

    def test_facade_normalizes_legacy_and_neutral_operands(self) -> None:
        legacy = normalize_comparison_request(
            {
                "rust_prefix": "/tmp/rust",
                "casa_prefix": "/tmp/casa",
                "products": [".image"],
                "panel_dir": "/tmp/panels",
                "structure_workspace_dir": "/tmp/sampled-structure-workspace",
            }
        )
        self.assertEqual("sampled", legacy["mode"])
        self.assertEqual("/tmp/rust", legacy["left_prefix"])
        self.assertEqual("casa-rs", legacy["left_label"])
        self.assertEqual("CASA", legacy["right_label"])
        self.assertTrue(legacy["legacy_operand_aliases"])

        neutral = normalize_comparison_request(
            {
                "mode": "full",
                "left_prefix": "/tmp/casa-run-a",
                "right_prefix": "/tmp/casa-run-b",
                "left_label": "CASA repeat 1",
                "right_label": "CASA repeat 2",
                "products": [".image.tt0"],
                "panel_dir": "/tmp/panels",
                "structure_workspace_dir": "/tmp/full-structure-workspace",
                "full_chunk_elements": 17,
                "require_exact_product_inventory": True,
                "require_metadata_parity": True,
            }
        )
        self.assertEqual("full", neutral["mode"])
        self.assertEqual(17, neutral["full_chunk_elements"])
        self.assertFalse(neutral["legacy_operand_aliases"])
        self.assertEqual("CASA repeat 1", neutral["left_label"])

    def test_full_mode_requires_an_absolute_structure_workspace(self) -> None:
        request = comparison_request()
        request.pop("structure_workspace_dir")
        with self.assertRaisesRegex(ValueError, "structure_workspace_dir"):
            normalize_comparison_request(request)

        request["structure_workspace_dir"] = "relative/workspace"
        with self.assertRaisesRegex(ValueError, "must be absolute"):
            normalize_comparison_request(request)

    def test_full_structure_evidence_is_exact_and_complete(self) -> None:
        request = normalize_comparison_request(comparison_request())
        output = comparison_output(request)
        structure = output["products"][".image.tt0"]["structured_difference"]

        missing = copy.deepcopy(output)
        missing_product = missing["products"][".image.tt0"]
        del missing_product["structured_difference"]["native_spatial_evidence"][
            "method"
        ]
        missing_product["full_array"]["structured_difference"] = copy.deepcopy(
            missing_product["structured_difference"]
        )
        with self.assertRaisesRegex(ValueError, "fields do not match"):
            validate_comparison_output(missing, request)

        incomplete = copy.deepcopy(output)
        incomplete["products"][".image.tt0"]["structured_difference"][
            "native_spatial_evidence"
        ]["coverage_complete"] = False
        incomplete["products"][".image.tt0"]["full_array"]["structured_difference"] = (
            incomplete["products"][".image.tt0"]["structured_difference"]
        )
        with self.assertRaisesRegex(ValueError, "coverage is incomplete"):
            validate_comparison_output(incomplete, request)

        self.assertEqual(
            "full_native_central_spatial_plane_disk_backed",
            structure["evidence_scope"],
        )

    def test_nested_product_results_are_rederived_before_acceptance(self) -> None:
        request = normalize_comparison_request(comparison_request())

        bad_metadata = comparison_output(request)
        bad_metadata["products"][".image.tt0"]["metadata"]["right"]["unit"] = "K"
        with self.assertRaisesRegex(ValueError, "metadata parity is not derived"):
            validate_comparison_output(bad_metadata, request)

        bad_source_box = comparison_output(request)
        bad_source_box["products"][".image.tt0"]["source_regions"][0]["trc"] = [
            1,
            0,
        ]
        with self.assertRaisesRegex(ValueError, "source-region trc"):
            validate_comparison_output(bad_source_box, request)

        bad_classification = comparison_output(request)
        product = bad_classification["products"][".image.tt0"]
        product["structured_difference"]["classification"]["overall"] = "good"
        product["full_array"]["structured_difference"] = copy.deepcopy(
            product["structured_difference"]
        )
        with self.assertRaisesRegex(ValueError, "exact-zero structure"):
            validate_comparison_output(bad_classification, request)

        bad_review = comparison_output(request)
        product = bad_review["products"][".image.tt0"]
        product["structured_difference"]["review"]["label"] = "good"
        product["full_array"]["structured_difference"] = copy.deepcopy(
            product["structured_difference"]
        )
        with self.assertRaisesRegex(ValueError, "exact-zero structure"):
            validate_comparison_output(bad_review, request)

    def test_exact_inventory_reports_missing_and_extra_siblings(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = pathlib.Path(temp_dir)
            left_prefix = root / "left"
            right_prefix = root / "right"
            (root / "left.image.tt0").mkdir()
            (root / "left.extra").mkdir()
            (root / "right.image.tt0").mkdir()

            inventory = comparator.compare_product_inventory(
                str(left_prefix),
                str(right_prefix),
                [".image.tt0", ".residual.tt0"],
                required=True,
            )

        self.assertEqual("mismatch", inventory["status"])
        self.assertEqual([".residual.tt0"], inventory["left_missing"])
        self.assertEqual([".extra"], inventory["left_extra"])
        self.assertEqual([".residual.tt0"], inventory["right_missing"])

    def test_full_reducer_visits_every_element_within_chunk_budget(self) -> None:
        shape = (5, 4, 3)
        right = np.arange(np.prod(shape), dtype=np.float64).reshape(shape)
        left = right + 1.0
        left[0, 0, 0] = np.nan
        right[0, 0, 0] = np.nan
        right[2, 2, 2] = np.inf
        left_mask = np.ones(shape, dtype=bool)
        right_mask = np.ones(shape, dtype=bool)
        left_mask[1, 1, 1] = False
        factory = FakeImageFactory(
            {"left": left, "right": right},
            {"left": left_mask, "right": right_mask},
        )

        result = comparator.full_array_statistics(
            "left", "right", max_elements=7, image_factory=factory
        )

        self.assertEqual("compared", result["status"])
        self.assertEqual(int(np.prod(shape)), result["elements_visited"])
        self.assertTrue(result["coverage_complete"])
        self.assertLessEqual(result["max_chunk_elements_observed"], 7)
        self.assertLessEqual(max(factory.chunk_sizes), 7)
        for visits in factory.visits.values():
            np.testing.assert_array_equal(np.ones(shape, dtype=np.int64), visits)
        self.assertEqual(1, result["topology"]["mask_mismatch_count"])
        self.assertEqual(1, result["topology"]["finite_topology_mismatch_count"])
        self.assertEqual(1.0, result["diff_rms"])
        self.assertEqual(1.0, result["diff_abs_max"])
        self.assertEqual(
            float(result["comparison_domain_count"]),
            result["difference"]["integrated_value"],
        )
        self.assertAlmostEqual(1.0, result["correlation"], places=12)

    def test_full_comparison_finds_sparse_overlap_missed_by_sample_stride(self) -> None:
        shape = (5, 5)
        left = np.full(shape, np.nan, dtype=np.float64)
        right = np.full(shape, np.nan, dtype=np.float64)
        left[1, 1] = 2.0
        right[1, 1] = 1.0
        masks = np.ones(shape, dtype=bool)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = pathlib.Path(temp_dir)
            left_path = root / "left.image.tt0"
            right_path = root / "right.image.tt0"
            left_path.mkdir()
            right_path.mkdir()
            factory = FakeImageFactory(
                {str(left_path): left, str(right_path): right},
                {str(left_path): masks, str(right_path): masks},
            )
            with mock.patch.object(comparator, "image", factory):
                result = comparator.compare_one(
                    str(left_path),
                    str(right_path),
                    max_elements=4,
                    panel_dir=str(root / "panels"),
                    suffix=".image.tt0",
                    beam_info={"status": "missing_psf"},
                    mode="full",
                    full_chunk_elements=6,
                    legacy_operand_aliases=False,
                )

        self.assertEqual([3, 3], result["sample_stride"])
        self.assertEqual(4, result["sampled_elements"])
        self.assertEqual(0, result["sampled_structured_difference"]["finite_overlap"])
        self.assertEqual(
            "full_native_central_spatial_plane_disk_backed",
            result["structured_difference"]["evidence_scope"],
        )
        self.assertEqual("compared", result["status"])
        self.assertEqual(1, result["finite_overlap"])
        self.assertEqual(1.0, result["diff_rms"])
        self.assertTrue(result["full_array"]["coverage_complete"])

    def test_full_topology_ignores_stored_values_behind_matching_false_masks(
        self,
    ) -> None:
        left = np.asarray([[1.0, np.nan], [3.0, 4.0]])
        right = np.asarray([[1.0, np.inf], [3.0, 4.0]])
        masks = np.asarray([[True, False], [True, True]])
        factory = FakeImageFactory(
            {"left": left, "right": right}, {"left": masks, "right": masks}
        )

        result = comparator.full_array_statistics(
            "left", "right", max_elements=2, image_factory=factory
        )

        self.assertTrue(result["topology"]["mask_equal"])
        self.assertTrue(result["topology"]["finite_equal"])
        self.assertTrue(result["topology"]["nonfinite_kind_equal"])

    def test_full_exact_zero_has_explicit_not_applicable_evidence(self) -> None:
        values = np.zeros((5, 4), dtype=np.float64)
        masks = np.ones_like(values, dtype=bool)
        masks[0, :] = False
        factory = FakeImageFactory(
            {"left": values, "right": values.copy()},
            {"left": masks, "right": masks.copy()},
        )

        result = comparator.full_array_statistics(
            "left",
            "right",
            max_elements=7,
            image_factory=factory,
            structure_suffix=".image.tt0",
            structure_beam_info={"status": "missing_psf"},
        )

        structure = result["structured_difference"]
        self.assertEqual("not_applicable_exact_zero", structure["status"])
        self.assertEqual("not_applicable_exact_zero", structure["review"]["label"])
        self.assertEqual(
            "full_native_central_spatial_plane_disk_backed",
            structure["evidence_scope"],
        )
        self.assertTrue(structure["native_spatial_evidence"]["coverage_complete"])
        self.assertEqual(16, result["comparison_domain_count"])

    def test_full_streamed_structure_detects_off_sampling_lattice_pattern(self) -> None:
        right = np.ones((9, 9), dtype=np.float64)
        left = right.copy()
        left[1:5, 1:5] += 0.1
        masks = np.ones_like(left, dtype=bool)
        factory = FakeImageFactory(
            {"left": left, "right": right},
            {"left": masks, "right": masks.copy()},
        )
        stride = comparator.stride_for(left.shape, max_elements=4)
        sampled_left = left[:: stride[0], :: stride[1]]
        sampled_right = right[:: stride[0], :: stride[1]]
        sampled = comparator.structured_difference_metrics(
            ".image.tt0",
            sampled_left,
            sampled_right,
            sampled_left - sampled_right,
            {"status": "missing_psf"},
        )
        self.assertEqual("good", sampled["review"]["label"])

        result = comparator.full_array_statistics(
            "left",
            "right",
            max_elements=7,
            image_factory=factory,
            structure_suffix=".image.tt0",
            structure_beam_info={"status": "missing_psf"},
        )

        structure = result["structured_difference"]
        self.assertIn(structure["review"]["label"], {"investigate", "bad"})
        evidence = structure["native_spatial_evidence"]
        self.assertTrue(evidence["coverage_complete"])
        self.assertEqual(81, evidence["spatial_pixels_visited"])
        self.assertEqual(81, evidence["paired_raw_finite_pixels"])

    def test_full_native_structure_excludes_genuine_nan_gradient_rows(
        self,
    ) -> None:
        right = np.full((24, 24), np.nan, dtype=np.float64)
        right[3:21, 3:21] = 1.0
        left = right.copy()
        masks = np.ones_like(left, dtype=bool)
        factory = FakeImageFactory(
            {"left": left, "right": right},
            {"left": masks, "right": masks.copy()},
        )

        result = comparator.full_array_statistics(
            "left",
            "right",
            max_elements=47,
            image_factory=factory,
            structure_suffix=".image.tt0",
            structure_beam_info={"status": "missing_psf"},
        )

        structure = result["structured_difference"]
        basis_fit = structure["scale_offset_gradient_fit"]
        self.assertEqual("computed", basis_fit["status"])
        self.assertEqual(324, basis_fit["masked_pixels"])
        self.assertEqual(256, basis_fit["fit_pixels"])
        self.assertEqual(68, basis_fit["excluded_nonfinite_basis_pixels"])
        self.assertEqual("good", structure["review"]["label"])

    def test_full_native_structure_preserves_masked_finite_stored_domain(
        self,
    ) -> None:
        axis = np.linspace(-1.0, 1.0, 1024, dtype=np.float64)
        right = 2.0 + axis[:, np.newaxis] + 0.5 * axis[np.newaxis, :]
        left = right.copy()
        masks = np.zeros_like(left, dtype=bool)
        masks[256:768, 256:768] = True
        factory = FakeImageFactory(
            {"left": left, "right": right},
            {"left": masks, "right": masks.copy()},
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            workspace = pathlib.Path(temp_dir) / "exact-structure"
            result = comparator.full_array_statistics(
                "left",
                "right",
                max_elements=262_144,
                image_factory=factory,
                structure_suffix=".alpha",
                structure_beam_info={"status": "missing_psf"},
                structure_scratch_root=workspace,
            )
            self.assertFalse(workspace.exists())

        structure = result["structured_difference"]
        evidence = structure["native_spatial_evidence"]
        self.assertEqual(1_048_576, evidence["spatial_pixels_visited"])
        self.assertEqual(1_048_576, evidence["paired_raw_finite_pixels"])
        self.assertEqual(262_144, evidence["paired_image_mask_finite_pixels"])
        self.assertEqual(262_144, result["comparison_domain_count"])
        self.assertTrue(evidence["coverage_complete"])
        self.assertEqual(
            1_048_576, structure["scale_offset_gradient_fit"]["fit_pixels"]
        )
        self.assertEqual(
            0,
            structure["scale_offset_gradient_fit"]["excluded_nonfinite_basis_pixels"],
        )
        self.assertEqual(
            "full_native_central_spatial_plane_disk_backed",
            structure["evidence_scope"],
        )

    def test_comparator_defers_successful_workspace_cleanup_to_validating_host(
        self,
    ) -> None:
        left = np.ones((4, 4), dtype=np.float64)
        right = left.copy()
        masks = np.ones_like(left, dtype=bool)
        factory = FakeImageFactory(
            {"left": left, "right": right},
            {"left": masks, "right": masks.copy()},
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            workspace = pathlib.Path(temp_dir) / "exact-structure"
            result = comparator.full_array_statistics(
                "left",
                "right",
                max_elements=7,
                image_factory=factory,
                structure_suffix=".image.tt0",
                structure_beam_info={"status": "missing_psf"},
                structure_scratch_root=workspace,
                defer_structure_cleanup=True,
            )

            self.assertEqual("compared", result["status"])
            self.assertEqual(
                {"left.f64", "right.f64", "diff.f64", "coverage.u8"},
                {path.name for path in workspace.iterdir()},
            )
            self.assertFalse((workspace / "failure.json").exists())

    def test_full_native_structure_preserves_within_old_bin_sinusoid(self) -> None:
        y, x = np.indices((256, 256), dtype=np.float64)
        del y
        right = np.ones((256, 256), dtype=np.float64)
        diff = 5.0e-4 * np.sin(2.0 * np.pi * (x + 0.5) / 16.0)
        left = right + diff
        masks = np.ones_like(left, dtype=bool)
        factory = FakeImageFactory(
            {"left": left, "right": right},
            {"left": masks, "right": masks.copy()},
        )
        beam_info = {
            "status": "estimated_from_psf",
            "coordinate_domain": "native_direction_pixels",
            "beam_block_side_pixels": 1,
        }
        direct = comparator.structured_difference_metrics(
            ".image.tt0", left, right, diff, beam_info
        )

        result = comparator.full_array_statistics(
            "left",
            "right",
            max_elements=4096,
            image_factory=factory,
            structure_suffix=".image.tt0",
            structure_beam_info=beam_info,
        )

        structure = result["structured_difference"]
        self.assertEqual("bad", direct["review"]["label"])
        self.assertEqual("bad", structure["review"]["label"])
        self.assertAlmostEqual(
            direct["normalized_diff_rms"], structure["normalized_diff_rms"], places=15
        )
        self.assertNotIn("streamed_spatial_reduction", structure)

    def test_native_beam_estimator_streams_unsampled_psf_cross_sections(self) -> None:
        psf = np.zeros((33, 35, 1, 1), dtype=np.float64)
        peak = (16, 17)
        psf[14:19, peak[1], 0, 0] = 1.0
        psf[peak[0], 16:19, 0, 0] = 1.0
        psf[peak[0], peak[1], 0, 0] = 2.0
        masks = np.ones_like(psf, dtype=bool)
        factory = FakeImageFactory({"psf": psf}, {"psf": masks})

        beam = comparator.estimate_native_beam_info(
            "psf", max_elements=17, image_factory=factory
        )

        self.assertEqual("estimated_from_psf", beam["status"])
        self.assertEqual("native_direction_pixels", beam["coordinate_domain"])
        self.assertEqual([1, 1, 1, 1], beam["sample_stride"])
        self.assertEqual([5, 3], beam["fwhm_pixels"])
        self.assertEqual(list(peak), beam["peak_location"])
        self.assertTrue(beam["native_plane_coverage"]["coverage_complete"])

    def test_native_structure_overlap_is_non_green_and_retained(self) -> None:
        left = np.ones((4, 8), dtype=np.float64)
        right = left.copy()
        masks = np.ones_like(left, dtype=bool)

        with tempfile.TemporaryDirectory() as temp_dir:
            workspace = pathlib.Path(temp_dir) / "failed-structure"
            reducer = comparator.FullArrayReducer(
                [8, 8],
                64,
                structure_suffix=".image.tt0",
                structure_beam_info={"status": "missing_psf"},
                structure_scratch_root=workspace,
            )
            reducer.add(left, right, masks, masks, [0, 0])
            reducer.add(left, right, masks, masks, [0, 0])
            result = reducer.result()
            reducer.close(retain=True, failure=result["status"])

            evidence = result["structured_difference"]["native_spatial_evidence"]
            self.assertEqual("structure_coverage_incomplete", result["status"])
            self.assertEqual(
                "unknown", result["structured_difference"]["review"]["label"]
            )
            self.assertEqual(64, evidence["spatial_pixels_visited"])
            self.assertEqual(32, evidence["covered_pixels"])
            self.assertEqual(32, evidence["overlap_write_pixels"])
            self.assertFalse(evidence["coverage_complete"])
            self.assertTrue((workspace / "failure.json").is_file())

    def test_metadata_parity_is_serializable_and_ignores_parent_path(self) -> None:
        common = {
            "shape": [4, 4, 1, 2],
            "unit": "Jy/beam",
            "restoring_beam": {"major": {"value": np.float64(1.2), "unit": "arcsec"}},
            "masks": ["mask1", "mask0"],
        }
        records = {
            "left": {
                **common,
                "coordinates": {
                    "parentName": "/tmp/left",
                    "referencepixel": np.asarray([2.0, 2.0, 0.0, 0.0]),
                },
            },
            "right": {
                **common,
                "coordinates": {
                    "parentName": "/tmp/right",
                    "referencepixel": np.asarray([2.0, 2.0, 0.0, 0.0]),
                },
            },
        }

        result = comparator.compare_image_metadata(
            "left", "right", image_factory=FakeMetadataFactory(records)
        )

        self.assertEqual("matched", result["status"])
        self.assertTrue(result["parity"])
        json_ready = comparator.normalize_serializable(result)
        self.assertEqual(["mask0", "mask1"], json_ready["left"]["masks"])

    def test_source_region_metrics_are_bounded_and_not_full_image_sums(self) -> None:
        shape = (6, 5, 1, 1)
        left = np.zeros(shape, dtype=np.float64)
        right = np.zeros(shape, dtype=np.float64)
        left[2, 2, 0, 0] = 10.0
        left[3, 2, 0, 0] = 2.0
        right[2, 2, 0, 0] = 10.0
        right[3, 2, 0, 0] = 1.0
        left[5, 4, 0, 0] = 1000.0
        right[5, 4, 0, 0] = 2000.0
        masks = np.ones(shape, dtype=bool)
        factory = FakeImageFactory(
            {"left": left, "right": right}, {"left": masks, "right": masks}
        )
        regions = [
            {
                "id": "bright-source",
                "products": [".image.tt0"],
                "blc": [1, 1],
                "trc": [3, 3],
            }
        ]

        result = comparator.compare_source_regions(
            "left",
            "right",
            regions,
            max_elements=4,
            image_factory=factory,
            left_beam_area_pixels=2.0,
            right_beam_area_pixels=2.0,
        )[0]

        self.assertEqual(12.0, result["left"]["integrated_pixel_sum"])
        self.assertEqual(11.0, result["right"]["integrated_pixel_sum"])
        self.assertEqual(6.0, result["left"]["integrated_flux"])
        self.assertEqual(5.5, result["right"]["integrated_flux"])
        self.assertAlmostEqual(2.0 + 2.0 / 12.0, result["left"]["centroid_pixels"][0])
        self.assertEqual([2, 2], result["right"]["peak_abs"]["location"])
        self.assertGreater(result["left"]["chunks"], 1)
        self.assertLessEqual(max(factory.chunk_sizes), 4)

    def test_source_region_chunks_tall_boxes_in_both_dimensions(self) -> None:
        shape = (2, 11, 1, 1)
        data = np.ones(shape, dtype=np.float64)
        masks = np.ones(shape, dtype=bool)
        factory = FakeImageFactory({"image": data}, {"image": masks})

        result = comparator.source_region_statistics(
            "image",
            [0, 0],
            [1, 10],
            max_elements=4,
            image_factory=factory,
            beam_area_pixels=1.0,
        )

        self.assertEqual(22, result["finite_unmasked_count"])
        self.assertLessEqual(max(factory.chunk_sizes), 4)

    def test_restoring_beam_area_is_converted_to_direction_pixels(self) -> None:
        metadata = {
            "restoring_beam": {
                "major": {"value": 2.0, "unit": "arcsec"},
                "minor": {"value": 1.0, "unit": "arcsec"},
            },
            "coordinates": {
                "direction0": {
                    "cdelt": [-0.5, 0.5],
                    "units": ["arcsec", "arcsec"],
                }
            },
        }

        area = comparator.metadata_beam_area_pixels(metadata)

        self.assertAlmostEqual(math.pi / (4.0 * math.log(2.0)) * 8.0, area)

    def test_taylor_product_families_keep_matching_semantics(self) -> None:
        self.assertEqual(
            (".image.tt1", ".residual.tt1"),
            comparator.model_restoration_suffixes(".model.tt1"),
        )
        self.assertEqual(
            ".psf.tt0",
            comparator.psf_beam_suffix([".image.tt0", ".psf.tt0", ".psf.tt1"]),
        )
        self.assertTrue(comparator.non_spatial_product(".sumwt.tt2"))
        left = np.asarray([[0.0, 0.5], [0.0, 1.0]])
        right = np.asarray([[0.0, 0.4], [0.0, 1.0]])
        mask, description = comparator.structured_difference_mask(
            ".weight.tt0",
            left,
            right,
            np.ones_like(left, dtype=bool),
        )
        self.assertEqual("full_finite_overlap", description["type"])
        self.assertEqual(".weight", description["product_family"])
        self.assertEqual(4, int(np.count_nonzero(mask)))

    def test_low_amplitude_pb_structure_outside_bright_support_cannot_pass(
        self,
    ) -> None:
        right = np.zeros((100, 100), dtype=np.float64)
        right[35:65, 35:65] = 1.0
        left = right.copy()
        outside = right == 0.0
        left[outside] += 1.0e-4
        diff = left - right
        structure = comparator.structured_difference_metrics(
            ".pb.tt0",
            left,
            right,
            diff,
            {
                "status": "estimated_from_psf",
                "coordinate_domain": "native_direction_pixels",
                "beam_block_side_pixels": 4,
            },
        )
        product = {
            "status": "compared",
            "diff_rms_over_right_rms": comparator.rms(diff) / comparator.rms(right),
            "diff_abs_max_over_right_peak": float(np.max(np.abs(diff))),
            "topology_parity": True,
            "structured_difference": structure,
        }
        comparison = apply_tolerance_contract(
            {
                "status": "completed",
                "comparison_mode": "full",
                "products": {".pb.tt0": product},
            },
            comparison_request(),
        )

        self.assertEqual(10_000, structure["analysis_pixels"])
        self.assertNotEqual("good", structure["review"]["label"])
        self.assertEqual("out_of_tolerance", comparison["status"])


def comparison_request(*, tolerances=...):
    if tolerances is ...:
        tolerances = {
            "contract_version": 1,
            "require_full_array": True,
            "default": {
                "diff_rms_over_right_rms": 0.001,
                "require_topology_parity": True,
                "allowed_structure_labels": ["good"],
            },
            "products": {},
        }
    return {
        "mode": "full",
        "left_prefix": "/evidence/left",
        "right_prefix": "/evidence/right",
        "left_label": "left-label",
        "right_label": "right-label",
        "products": [".image.tt0", ".residual.tt0"],
        "max_elements_per_product": 31,
        "full_chunk_elements": 17,
        "require_exact_product_inventory": True,
        "require_metadata_parity": True,
        "source_regions": [
            {
                "id": "source-1",
                "products": [".image.tt0", ".residual.tt0"],
                "blc": [0, 0],
                "trc": [0, 0],
            }
        ],
        "tolerances": tolerances,
        "panel_dir": "/evidence/panels",
        "structure_workspace_dir": "/evidence/structure-workspace",
    }


def comparison_output(request):
    beam_info = {
        "status": "estimated_from_psf",
        "estimation_method": (
            "streamed_native_central_plane_peak_and_native_cross_sections"
        ),
        "coordinate_domain": "native_direction_pixels",
        "native_plane_coverage": {
            "pixels_visited": 1,
            "expected_pixels": 1,
            "coverage_complete": True,
        },
    }
    zero_peak = {"location": [0, 0], "value": 0.0, "abs_value": 0.0}
    zero_operand = {
        "min": 0.0,
        "max": 0.0,
        "sum": 0.0,
        "sum_squares": 0.0,
        "rms": 0.0,
        "integrated_value": 0.0,
        "peak_abs": zero_peak,
    }
    products = {}
    for suffix in request["products"]:
        structure = full_structure_evidence(suffix)
        products[suffix] = {
            "status": "compared",
            "left_path": request["left_prefix"] + suffix,
            "right_path": request["right_prefix"] + suffix,
            "shape": [1, 1],
            "finite_overlap": 1,
            "topology_parity": True,
            "left_min": 0.0,
            "left_max": 0.0,
            "left_rms": 0.0,
            "right_min": 0.0,
            "right_max": 0.0,
            "right_rms": 0.0,
            "left_peak_abs": copy.deepcopy(zero_peak),
            "right_peak_abs": copy.deepcopy(zero_peak),
            "diff_peak_abs": copy.deepcopy(zero_peak),
            "diff_abs_max": 0.0,
            "diff_rms": 0.0,
            "diff_rms_over_right_rms": 0.0,
            "diff_abs_max_over_right_peak": 0.0,
            "correlation": None,
            "metadata_parity_required": request["require_metadata_parity"],
            "metadata": matched_metadata([1, 1]),
            "source_regions": [
                source_region_result(region, request["full_chunk_elements"])
                for region in request["source_regions"]
                if suffix in region["products"]
            ],
            "structured_difference": structure,
        }
    for product in products.values():
        product["full_array"] = {
            "status": "compared",
            "shape": [1, 1],
            "full_chunk_elements": request["full_chunk_elements"],
            "chunks": 1,
            "max_chunk_elements_observed": 1,
            "total_elements": 1,
            "elements_visited": 1,
            "coverage_complete": True,
            "comparison_domain": "left_and_right_pixel_masks_and_finite_values",
            "count": 1,
            "comparison_domain_count": 1,
            "topology": {
                "mask_equal": True,
                "mask_mismatch_count": 0,
                "left_masked_count": 0,
                "right_masked_count": 0,
                "finite_equal": True,
                "finite_topology_mismatch_count": 0,
                "nonfinite_kind_equal": True,
                "nonfinite_kind_mismatch_count": 0,
                "left_finite_count": 1,
                "right_finite_count": 1,
                "left_nonfinite": {
                    "nan": 0,
                    "positive_infinity": 0,
                    "negative_infinity": 0,
                },
                "right_nonfinite": {
                    "nan": 0,
                    "positive_infinity": 0,
                    "negative_infinity": 0,
                },
            },
            "left": copy.deepcopy(zero_operand),
            "right": copy.deepcopy(zero_operand),
            "cross_sum": 0.0,
            "covariance": 0.0,
            "correlation": None,
            "left_integrated_value": 0.0,
            "right_integrated_value": 0.0,
            "diff_integrated_value": 0.0,
            "left_peak_abs": copy.deepcopy(zero_peak),
            "right_peak_abs": copy.deepcopy(zero_peak),
            "diff_peak_abs": copy.deepcopy(zero_peak),
            "difference": {
                "sum": 0.0,
                "sum_squares": 0.0,
                "integrated_value": 0.0,
                "rms": 0.0,
                "abs_max": 0.0,
                "peak_abs": copy.deepcopy(zero_peak),
            },
            "diff_rms": 0.0,
            "diff_abs_max": 0.0,
            "diff_rms_over_right_rms": 0.0,
            "diff_abs_max_over_right_peak": 0.0,
            "structured_difference": copy.deepcopy(product["structured_difference"]),
        }
        product["structured_difference"]["beam_info"] = copy.deepcopy(beam_info)
        product["full_array"]["structured_difference"] = copy.deepcopy(
            product["structured_difference"]
        )
    return {
        "schema_version": request["schema_version"],
        "request_binding": copy.deepcopy(request["request_binding"]),
        "request_sha256": request["request_sha256"],
        "status": "completed",
        "reason": None,
        "comparison_mode": request["mode"],
        "max_elements_per_product": request["max_elements_per_product"],
        "full_chunk_elements": request["full_chunk_elements"],
        "left_prefix": request["left_prefix"],
        "right_prefix": request["right_prefix"],
        "left_label": request["left_label"],
        "right_label": request["right_label"],
        "requested_products": list(request["products"]),
        "require_exact_product_inventory": request["require_exact_product_inventory"],
        "require_metadata_parity": request["require_metadata_parity"],
        "legacy_operand_aliases": request["legacy_operand_aliases"],
        "source_regions": copy.deepcopy(request["source_regions"]),
        "tolerances": copy.deepcopy(request["tolerances"]),
        "panel_dir": request["panel_dir"],
        "structure_workspace_dir": request["structure_workspace_dir"],
        "beam_info": beam_info,
        "product_inventory": {
            "status": "matched",
            "required": request["require_exact_product_inventory"],
            "observed_match": True,
            "expected": sorted(request["products"]),
            "left": sorted(request["products"]),
            "right": sorted(request["products"]),
            "left_missing": [],
            "left_extra": [],
            "right_missing": [],
            "right_extra": [],
            "left_right_equal": True,
        },
        "products": products,
        "structured_difference_review": comparator.summarize_product_reviews(products),
    }


def full_structure_evidence(suffix):
    native = {
        "method": "exact_native_central_plane_disk_backed_memmap",
        "source_shape": [1, 1],
        "storage": "temporary_disk_backed_native_arrays",
        "array_count": 4,
        "temporary_bytes": 25,
        "spatial_pixels_visited": 1,
        "covered_pixels": 1,
        "expected_pixels": 1,
        "overlap_write_pixels": 0,
        "coverage_complete": True,
        "write_chunks": 1,
        "structure_value_domain": (
            "raw_paired_finite_stored_values_before_image_mask_application"
        ),
        "left_raw_finite_pixels": 1,
        "right_raw_finite_pixels": 1,
        "paired_raw_finite_pixels": 1,
        "paired_image_mask_finite_pixels": 1,
        "central_mask_mismatch_pixels": 0,
        "workspace_lifecycle": "remove_on_success_retain_on_failure",
    }
    result = comparator.exact_zero_structure_evidence(
        suffix,
        evidence_scope="full_native_central_spatial_plane_disk_backed",
    )
    result["native_spatial_evidence"] = native
    return result


def matched_metadata(shape):
    operand = {
        "status": "complete",
        "shape": list(shape),
        "unit": "Jy/beam",
        "coordinates": {
            "direction0": {
                "cdelt": [-1.0, 1.0],
                "units": ["arcsec", "arcsec"],
            }
        },
        "restoring_beam": {
            "major": {"value": 2.0, "unit": "arcsec"},
            "minor": {"value": 1.0, "unit": "arcsec"},
            "positionangle": {"value": 0.0, "unit": "deg"},
        },
        "masks": ["mask0"],
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


def source_region_result(region, chunk_elements):
    measurement = {
        "status": "measured",
        "finite_unmasked_count": 1,
        "integrated_pixel_sum": 0.0,
        "beam_area_pixels": 1.0,
        "integrated_flux": 0.0,
        "centroid_pixels": [0.0, 0.0],
        "peak_abs": {"location": [0, 0], "value": 0.0, "abs_value": 0.0},
        "chunks": 1,
        "max_chunk_elements": chunk_elements,
    }
    return {
        **copy.deepcopy(region),
        "method": (
            "finite_unmasked_region_sum_over_restoring_beam_area_"
            "and_abs_weighted_centroid"
        ),
        "left": copy.deepcopy(measurement),
        "right": copy.deepcopy(measurement),
    }


def replace_structure_with_bad_review(output, suffix):
    structure = output["products"][suffix]["structured_difference"]
    normalized_diff_rms = 0.01
    low_order_r2 = None
    large_scale_power = None
    block_decay = None
    classification = comparator.structured_difference_classification(
        normalized_diff_rms,
        low_order_r2,
        large_scale_power,
        block_decay,
    )
    review = comparator.structured_difference_review(
        suffix,
        classification,
        normalized_diff_rms,
        low_order_r2,
        large_scale_power,
        block_decay,
    )
    structure.update(
        {
            "status": "computed",
            "mask": {"type": "finite_overlap"},
            "masked_pixels": 1,
            "analysis_pixels": 1,
            "beam_block_side_pixels": 1,
            "normalization": {
                "type": "casa_support_rms_or_peak",
                "value": 1.0,
            },
            "diff_rms": normalized_diff_rms,
            "normalized_diff_rms": normalized_diff_rms,
            "low_order_r2_quadratic": low_order_r2,
            "large_scale_power_fraction": large_scale_power,
            "scale_offset_gradient_fit": {"status": "insufficient_pixels"},
            "beam_block_rms_by_scale": [],
            "block_rms_decay_slope_vs_independent_beams": block_decay,
            "classification": classification,
            "review": review,
        }
    )
    output["products"][suffix]["full_array"]["structured_difference"] = copy.deepcopy(
        structure
    )
    output["structured_difference_review"] = comparator.summarize_product_reviews(
        output["products"]
    )


def write_pending_structure_workspace(request):
    root = pathlib.Path(request["structure_workspace_dir"])
    root.mkdir()
    for suffix in request["products"]:
        safe_suffix = suffix.strip(".").replace(".", "_") or "image"
        digest = hashlib.sha256(suffix.encode("utf-8")).hexdigest()[:12]
        workspace = root / f"{safe_suffix}-{digest}"
        workspace.mkdir()
        for name in ("left.f64", "right.f64", "diff.f64", "coverage.u8"):
            (workspace / name).write_bytes(b"exact evidence")


def write_protocol_result(kwargs, output, request):
    request_path = kwargs["request_path"]
    output_path = kwargs["output_path"]
    log_path = kwargs["log_path"]
    request_path.write_text(json.dumps(request), encoding="utf-8")
    write_pending_structure_workspace(request)
    payload = (json.dumps(output, sort_keys=True) + "\n").encode()
    output_path.write_bytes(payload)
    log_path.write_text("CASA comparator log\n", encoding="utf-8")
    return CasaProtocolResult(
        status="completed",
        return_code=0,
        output=output,
        output_sha256=hashlib.sha256(payload).hexdigest(),
        reason=None,
        request_path=request_path,
        output_path=output_path,
        log_path=log_path,
    )


if __name__ == "__main__":
    unittest.main()
