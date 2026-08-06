# SPDX-License-Identifier: LGPL-3.0-or-later
"""Fail-closed checks for the finite VLASS merge-recovery contract."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import unittest

from perf_harness.tolerances import validate_tolerance_contract


ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = ROOT / "tools/perf/imager/vlass_recovery_contract.json"
LEDGER_PATH = ROOT / "tools/perf/imager/vlass_recovery_launch_ledger.json"
CATALOG_PATH = ROOT / "tools/perf/imager/vlass_recovery_salvage_catalog.json"
SCIENTIFIC_EQUIVALENCE_PATH = (
    ROOT / "tools/perf/imager/contracts/vlass-scientific-equivalence-v2.json"
)
REDUCED_ALL_FIELDS_CLEAN_PATH = (
    ROOT
    / "tools/perf/imager/workloads/"
    "vlass-fragment-all-fields-clean-4096-four-spw-casa.json"
)


def load_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AssertionError(f"{path} must contain a JSON object")
    return value


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class RecoveryContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_json(CONTRACT_PATH)
        self.ledger = load_json(LEDGER_PATH)
        self.catalog = load_json(CATALOG_PATH)

    def test_scientific_equivalence_contract_is_explicit_and_fail_closed(
        self,
    ) -> None:
        tolerances = load_json(SCIENTIFIC_EQUIVALENCE_PATH)
        acceptance = self.contract["acceptance"]
        assert isinstance(acceptance, dict)

        validate_tolerance_contract(tolerances, source=str(SCIENTIFIC_EQUIVALENCE_PATH))
        self.assertEqual(
            str(SCIENTIFIC_EQUIVALENCE_PATH.relative_to(ROOT)),
            acceptance["scientific_equivalence_contract_path"],
        )
        self.assertEqual(
            sha256(SCIENTIFIC_EQUIVALENCE_PATH),
            acceptance["scientific_equivalence_contract_sha256"],
        )
        self.assertIs(
            True,
            acceptance["structured_difference_labels_are_diagnostic"],
        )
        self.assertEqual(2, tolerances["contract_version"])
        self.assertIs(True, tolerances["require_full_array"])
        self.assertEqual(
            {
                "coherent_block_rms_over_right_rms": 1.0e-4,
                "diff_abs_max_over_right_peak": 5.0e-3,
                "diff_rms_over_right_rms": 1.0e-3,
                "require_topology_parity": True,
            },
            tolerances["default"],
        )
        products = tolerances["products"]
        assert isinstance(products, dict)
        self.assertEqual(
            {
                "beam_area_relative": 1.0e-3,
                "beam_kernel_nrmse": 1.0e-3,
                "centroid_beams": 1.0e-2,
                "integrated_flux_relative": 1.0e-3,
                "peak_relative": 1.0e-3,
            },
            products[".image.tt0"],
        )

    def test_reduced_all_fields_clean_manifest_binds_approved_contract(self) -> None:
        manifest = load_json(REDUCED_ALL_FIELDS_CLEAN_PATH)
        tolerances = load_json(SCIENTIFIC_EQUIVALENCE_PATH)
        imaging = manifest["imaging"]
        comparison = manifest["comparison"]
        run = manifest["run"]
        assert isinstance(imaging, dict)
        assert isinstance(comparison, dict)
        assert isinstance(run, dict)

        self.assertEqual("clean", imaging["mode"])
        self.assertEqual(4096, imaging["imsize"])
        self.assertEqual("2,7,12,17", imaging["spw"])
        self.assertEqual("1107~1127,1512~1532,1542~1562", imaging["field"])
        self.assertEqual(2000, imaging["niter"])
        self.assertIs(True, imaging["usepointing"])
        self.assertEqual(
            "/Volumes/GLENDENNING/casa-rs-vlass/issue-446/masks/"
            "vlass-source-box-4096-spectral.mask",
            imaging["mask_image"],
        )
        self.assertEqual(
            "8490acb911cbbba78f7a20ba4a1d379e227c3a42dfc7eefcc9b7fd5f4139572f",
            imaging["mask_sha256"],
        )
        self.assertEqual(tolerances, comparison["tolerances"])
        self.assertEqual(19, len(comparison["products"]))
        self.assertEqual([575, 2125], comparison["source_regions"][0]["blc"])
        self.assertEqual([638, 2188], comparison["source_regions"][0]["trc"])
        self.assertEqual("warm", run["cf_cache_role"])
        self.assertIs(True, run["preverified_warm_cache"])
        self.assertEqual("1", run["skip_rust"])

    def test_contract_binds_reference_manifests_and_cap20000_delta(self) -> None:
        self.assertEqual(1, self.contract["schema_version"])
        schedule = self.contract["casa_reference_schedule"]
        assert isinstance(schedule, dict)
        rows = schedule["rows"]
        assert isinstance(rows, list)
        self.assertEqual(
            [
                "CASA-B-FRAGMENT63-CLEAN-CAP20000-v1",
                "CASA-B-FRAGMENT63-CLEAN-CAP20000-v2",
                "CASA-A-SINGLE-CLEAN-N2000-v1",
            ],
            schedule["launch_order"],
        )
        by_id = {row["id"]: row for row in rows}
        self.assertEqual(3, len(by_id))
        for row in rows:
            manifest_path = ROOT / row["manifest_path"]
            self.assertEqual(row["manifest_sha256"], sha256(manifest_path))

        fragment = by_id["CASA-B-FRAGMENT63-CLEAN-CAP20000-v1"]
        base_path = ROOT / fragment["base_manifest_path"]
        self.assertEqual(fragment["base_manifest_sha256"], sha256(base_path))
        base = load_json(base_path)
        cap = load_json(ROOT / fragment["manifest_path"])
        for value in (base, cap):
            value.pop("id")
            value.pop("mode_id")
            value.pop("description")
            run = value["run"]
            assert isinstance(run, dict)
            run.pop("run_label")
            run.pop("evidence_role")
            review = value["review"]
            assert isinstance(review, dict)
            review["required_evidence_roles"] = []
        base_imaging = base["imaging"]
        assert isinstance(base_imaging, dict)
        base_imaging["niter"] = 20000
        self.assertEqual(base, cap)

    def test_budgets_are_finite_and_preserve_one_shared_retry(self) -> None:
        casa = self.contract["casa_reference_schedule"]
        candidate = self.contract["casa_rs_candidate_schedule"]
        budget = self.contract["work_budget"]
        assert isinstance(casa, dict)
        assert isinstance(candidate, dict)
        assert isinstance(budget, dict)
        self.assertEqual(3, casa["max_full_geometry_launches"])
        self.assertEqual(1, casa["shared_external_invalidation_retries"])
        self.assertEqual(2, candidate["max_immutable_candidate_freezes"])
        self.assertEqual(5, candidate["max_full_geometry_launches"])
        self.assertEqual(1, candidate["shared_external_invalidation_retries"])
        self.assertEqual(8, budget["salvage_audit_engineer_hours"])
        self.assertEqual(48, budget["total_engineer_hours"])
        self.assertEqual(72, budget["active_window_hours"])

    def test_performance_work_requires_a_frozen_matched_casa_baseline(self) -> None:
        baseline = self.contract["casa_first_performance_baseline"]
        assert isinstance(baseline, dict)
        self.assertTrue(baseline["required_before_casa_rs_performance_changes"])
        self.assertTrue(baseline["generate_missing_baseline_once"])
        self.assertTrue(baseline["freeze_valid_baseline"])
        self.assertEqual(
            [
                "dataset",
                "selection",
                "image_geometry",
                "required_products",
                "timed_boundary",
            ],
            baseline["required_matching_dimensions"],
        )
        self.assertFalse(baseline["end_to_end_timing_may_anchor_component_target"])
        self.assertFalse(baseline["component_timing_may_anchor_end_to_end_target"])
        self.assertFalse(baseline["rerun_when_only_casa_rs_changes"])
        self.assertIn(
            "explicit user approval",
            baseline["unexposed_boundary_policy"],
        )

    def test_launch_ledger_cannot_exceed_the_contract(self) -> None:
        self.assertEqual(self.contract["id"], self.ledger["contract_id"])
        entries = self.ledger["entries"]
        assert isinstance(entries, list)
        casa_entries = [entry for entry in entries if entry["executor"] == "casa"]
        rust_entries = [entry for entry in entries if entry["executor"] == "casa-rs"]
        casa_schedule = self.contract["casa_reference_schedule"]
        rust_schedule = self.contract["casa_rs_candidate_schedule"]
        assert isinstance(casa_schedule, dict)
        assert isinstance(rust_schedule, dict)
        self.assertLessEqual(
            len(casa_entries), casa_schedule["max_full_geometry_launches"]
        )
        self.assertLessEqual(
            len(rust_entries), rust_schedule["max_full_geometry_launches"]
        )
        for executor_entries, schedule in (
            (casa_entries, casa_schedule),
            (rust_entries, rust_schedule),
        ):
            invalidations = sum(
                entry.get("disposition") == "external_invalidation"
                for entry in executor_entries
            )
            self.assertLessEqual(
                invalidations, schedule["shared_external_invalidation_retries"]
            )

        casa_a = next(
            entry
            for entry in casa_entries
            if entry["row_id"] == "CASA-A-SINGLE-CLEAN-N2000-v1"
        )
        self.assertEqual("accepted_reference", casa_a["disposition"])
        self.assertEqual(259200, casa_a["max_wall_seconds"])
        self.assertEqual(
            "5da8ce24c92b2d47e53784e8600976bf37708086309820cb1b61af6f8982bd9e",
            casa_a["manifest_sha256"],
        )
        self.assertEqual(
            "44e6741d093a5ca488d6099990cf4938e4dcf87119a4e7f32aa6ec3f51e7003c",
            casa_a["dry_run_receipt_sha256"],
        )
        self.assertEqual(
            "f9216878e3372ecb4a81f565e33e6b5b2729abf20d0c1d7313892ac4db6a680d",
            casa_a["receipt_sha256"],
        )
        evidence = casa_a["evidence"]
        assert isinstance(evidence, dict)
        self.assertEqual(1103, evidence["actual_minor_iterations"])
        self.assertEqual(8, evidence["minor_cycle_count"])
        self.assertEqual("nsigma", evidence["stop_reason"])
        self.assertEqual(1, evidence["field_count"])
        self.assertEqual(16, evidence["spw_count"])
        self.assertEqual(10400, evidence["selected_rows"])
        self.assertEqual([12150, 12150], evidence["imsize"])
        self.assertEqual(19, evidence["product_count"])
        product_hashes = evidence["product_tree_sha256_by_suffix"]
        assert isinstance(product_hashes, dict)
        self.assertEqual(19, len(product_hashes))
        self.assertEqual(0, evidence["pages_throttled_max"])

    def test_casa_b_v2_corrects_mask_and_records_accepted_reference(self) -> None:
        pending = self.ledger["pending_reference_amendment"]
        assert isinstance(pending, dict)
        self.assertEqual("approved", pending["status"])
        self.assertEqual("Brian Glendenning", pending["approved_by"])
        base_path = ROOT / pending["base_manifest_path"]
        cap_path = ROOT / pending["manifest_path"]
        self.assertEqual(pending["base_manifest_sha256"], sha256(base_path))
        self.assertEqual(pending["manifest_sha256"], sha256(cap_path))

        schedule = self.contract["casa_reference_schedule"]
        assert isinstance(schedule, dict)
        row = next(item for item in schedule["rows"] if item["id"] == pending["row_id"])
        self.assertEqual(pending["manifest_path"], row["manifest_path"])
        self.assertEqual(pending["manifest_sha256"], row["manifest_sha256"])
        self.assertIn(pending["row_id"], schedule["launch_order"])

        launch = next(
            entry
            for entry in self.ledger["entries"]
            if entry["row_id"] == pending["row_id"]
        )
        self.assertEqual("accepted_reference", launch["disposition"])
        self.assertEqual(
            "20260802T191330Z-vlass-fragment-all-fields-clean-cap20000-casa-v2-5a0b3b07",
            launch["run_id"],
        )
        self.assertEqual(pending["manifest_sha256"], launch["manifest_sha256"])
        self.assertEqual(
            "ab7b6c3fa142d0cb3d0f54236b142b08b0aa837f120ffbf4314742723be04b27",
            launch["dry_run_receipt_sha256"],
        )
        self.assertEqual(
            "30aaf60c4c29595eb9789bcfe1fdab5723bb761295d4e647e4632b8eb6c31be6",
            launch["receipt_sha256"],
        )
        evidence = launch["evidence"]
        assert isinstance(evidence, dict)
        self.assertEqual(444, evidence["actual_minor_iterations"])
        self.assertEqual("nsigma", evidence["stop_reason"])
        self.assertEqual(63, evidence["field_count"])
        self.assertEqual(16, evidence["spw_count"])
        self.assertEqual([12150, 12150], evidence["imsize"])
        self.assertEqual(19, evidence["product_count"])
        product_hashes = evidence["product_tree_sha256_by_suffix"]
        assert isinstance(product_hashes, dict)
        self.assertEqual(19, len(product_hashes))
        self.assertEqual(0, evidence["pages_throttled_max"])

        base = load_json(base_path)
        cap = load_json(cap_path)
        for value in (base, cap):
            value.pop("id")
            value.pop("mode_id")
            value.pop("description")
            run = value["run"]
            assert isinstance(run, dict)
            run.pop("run_label")
            run.pop("evidence_role")
            review = value["review"]
            assert isinstance(review, dict)
            review["required_evidence_roles"] = []
        base_imaging = base["imaging"]
        assert isinstance(base_imaging, dict)
        base_imaging["niter"] = 20000
        self.assertEqual(base, cap)

        cap_imaging = cap["imaging"]
        assert isinstance(cap_imaging, dict)
        self.assertEqual(pending["corrected_mask_path"], cap_imaging["mask_image"])
        self.assertEqual(pending["corrected_mask_sha256"], cap_imaging["mask_sha256"])
        comparison = cap["comparison"]
        assert isinstance(comparison, dict)
        source = comparison["source_regions"][0]
        self.assertEqual(pending["corrected_mask_blc"], source["blc"])
        self.assertEqual(pending["corrected_mask_trc"], source["trc"])

    def test_reduced_ladder_records_matched_release_timing_and_promoted_correctness(
        self,
    ) -> None:
        entries = self.ledger["reduced_ladder_entries"]
        assert isinstance(entries, list)
        self.assertEqual(3, len(entries))
        pair = entries[0]
        self.assertEqual(
            "REDUCED-ALL63-DIRTY-4096-4SPW-001",
            pair["pair_id"],
        )
        self.assertEqual(
            "correctness_promoted_performance_below_target",
            pair["disposition"],
        )

        failed_clean = entries[1]
        self.assertEqual(
            "REDUCED-ALL63-CLEAN-4096-4SPW-ATTEMPT-001",
            failed_clean["pair_id"],
        )
        self.assertEqual(
            "rejected_invalid_mask_coordinate_system",
            failed_clean["disposition"],
        )
        correction = failed_clean["correction"]
        assert isinstance(correction, dict)
        self.assertEqual(
            "8490acb911cbbba78f7a20ba4a1d379e227c3a42dfc7eefcc9b7fd5f4139572f",
            correction["mask_sha256"],
        )
        self.assertEqual(
            ["Direction", "Stokes", "Spectral"],
            correction["coordinate_types"],
        )
        self.assertEqual("completed_and_frozen", correction["retry_status"])
        self.assertEqual(
            "REDUCED-ALL63-CLEAN-4096-4SPW-001",
            correction["retry_pair_id"],
        )

        selection = pair["selection"]
        assert isinstance(selection, dict)
        self.assertEqual(63, selection["field_count"])
        self.assertEqual(4, selection["spw_count"])
        self.assertEqual([4096, 4096], selection["imsize"])
        self.assertEqual(18, selection["product_count"])

        casa = pair["casa"]
        rust = pair["casa_rs"]
        performance = pair["performance"]
        comparison = pair["comparison"]
        for value in (casa, rust, performance, comparison):
            assert isinstance(value, dict)
        manifest = ROOT / casa["manifest_path"]
        self.assertEqual(casa["manifest_sha256"], sha256(manifest))
        self.assertEqual(64, len(casa["receipt_sha256"]))
        self.assertEqual(64, len(rust["binary_sha256"]))
        self.assertEqual(64, len(rust["run_log_sha256"]))
        self.assertAlmostEqual(
            casa["tclean_wall_seconds"] / rust["wall_seconds"],
            performance["speedup_casa_over_casa_rs"],
        )
        self.assertEqual("below_target", performance["status"])
        self.assertEqual("comparison_failed", comparison["status"])
        self.assertEqual("matched", comparison["inventory"])
        self.assertEqual(
            "mismatch",
            comparison["restoring_beam_metadata"],
        )
        self.assertEqual(
            [".psf.tt1", ".psf.tt2", ".weight.tt1"],
            comparison["structured_difference_products"],
        )
        scientific = pair["scientific_equivalence"]
        assert isinstance(scientific, dict)
        contract = ROOT / scientific["contract_path"]
        self.assertEqual(scientific["contract_sha256"], sha256(contract))
        self.assertEqual("passed", scientific["status"])
        self.assertEqual(18, scientific["product_count"])
        self.assertEqual([], scientific["failed_checks"])
        self.assertEqual([], scientific["incomplete_checks"])
        self.assertLessEqual(scientific["image_tt0_nrmse"], 1.0e-3)
        self.assertLessEqual(scientific["beam_kernel_nrmse"], 1.0e-3)
        self.assertLessEqual(scientific["beam_area_relative"], 1.0e-3)
        self.assertLessEqual(
            scientific["worst_coherent_block_rms_over_right_rms"],
            1.0e-4,
        )
        self.assertEqual(64, len(scientific["receipt_sha256"]))
        self.assertEqual(64, len(scientific["raw_output_sha256"]))

        clean = entries[2]
        self.assertEqual(
            "REDUCED-ALL63-CLEAN-4096-4SPW-001",
            clean["pair_id"],
        )
        self.assertEqual(
            "correctness_failed_performance_below_target",
            clean["disposition"],
        )
        clean_casa = clean["casa"]
        clean_rust = clean["casa_rs"]
        clean_performance = clean["performance"]
        clean_scientific = clean["scientific_equivalence"]
        for value in (
            clean_casa,
            clean_rust,
            clean_performance,
            clean_scientific,
        ):
            assert isinstance(value, dict)
        self.assertEqual(19, clean["selection"]["product_count"])
        self.assertEqual(193, clean_casa["minor_iterations"])
        self.assertEqual(187, clean_rust["minor_iterations"])
        self.assertEqual(0, clean_rust["swaps"])
        self.assertAlmostEqual(
            clean_casa["tclean_wall_seconds"] / clean_rust["wall_seconds"],
            clean_performance["speedup_casa_over_casa_rs"],
        )
        self.assertEqual("below_target", clean_performance["status"])
        self.assertEqual("failed", clean_scientific["status"])
        self.assertEqual("matched", clean_scientific["inventory"])
        self.assertGreater(clean_scientific["image_tt0_nrmse"], 1.0e-3)
        self.assertGreater(clean_scientific["residual_tt0_nrmse"], 1.0e-3)
        self.assertEqual(
            clean_scientific["contract_sha256"],
            sha256(ROOT / clean_scientific["contract_path"]),
        )
        for field in (
            "comparison_input_sha256",
            "raw_output_sha256",
            "log_sha256",
        ):
            self.assertEqual(64, len(clean_scientific[field]))

    def test_salvage_catalog_selects_at_most_primary_and_reserve(self) -> None:
        self.assertEqual(self.contract["id"], self.catalog["contract_id"])
        entries = self.catalog["entries"]
        assert isinstance(entries, list)
        ids = [entry["id"] for entry in entries]
        self.assertEqual(len(ids), len(set(ids)))
        allowed = {
            "eligible_for_audit",
            "retired",
            "research_only",
            "selected_primary",
            "selected_reserve",
            "included_in_primary",
        }
        self.assertTrue(all(entry["status"] in allowed for entry in entries))
        self.assertLessEqual(
            sum(entry["status"] == "selected_primary" for entry in entries), 1
        )
        self.assertLessEqual(
            sum(entry["status"] == "selected_reserve" for entry in entries), 1
        )
        if self.catalog["audit_status"] == "pending":
            self.assertIsNone(self.catalog["primary_candidate"])
            self.assertIsNone(self.catalog["reserve_candidate"])
        elif self.catalog["audit_status"] == "complete":
            primary = self.catalog["primary_candidate"]
            reserve = self.catalog["reserve_candidate"]
            assert isinstance(primary, dict)
            self.assertEqual(
                primary["id"],
                next(
                    entry["id"]
                    for entry in entries
                    if entry["status"] == "selected_primary"
                ),
            )
            self.assertEqual(40, len(primary["source_seed_commit"]))
            self.assertEqual(
                "c23831b081555423e15c76e6f71215251ee68fd9",
                primary["source_seed_commit"],
            )
            self.assertEqual(
                primary["source_seed_commit"],
                primary["scientific_promotion_commit"],
            )
            self.assertEqual(64, len(primary["candidate_binary_sha256"]))
            validations = [
                evidence
                for evidence in primary["evidence"]
                if evidence["kind"] == "recovery_checkpoint_validation"
            ]
            self.assertEqual(2, len(validations))
            self.assertEqual(
                {
                    "4096-square four-SPW real VLASS clean",
                    "4096-square full-16-SPW real VLASS clean",
                },
                {validation["workload"] for validation in validations},
            )
            for validation in validations:
                self.assertEqual("promote", validation["decision"])
                self.assertEqual(0, validation["swaps"])
                self.assertEqual(64, len(validation["run_log_sha256"]))
                self.assertEqual(64, len(validation["comparison_sha256"]))
                self.assertEqual(64, len(validation["scientific_floor_sha256"]))
            by_id = {entry["id"]: entry for entry in entries}
            self.assertEqual("retired", by_id["obsolete-9a14-source-seed"]["status"])
            self.assertEqual("retired", by_id["db41-obsolete-seed-pr3-trim"]["status"])
            if reserve is not None:
                assert isinstance(reserve, dict)
                self.assertEqual(
                    reserve["id"],
                    next(
                        entry["id"]
                        for entry in entries
                        if entry["status"] == "selected_reserve"
                    ),
                )
                self.assertEqual(40, len(reserve["source_seed_commit"]))
        else:
            self.fail(
                f"unsupported salvage audit status: {self.catalog['audit_status']}"
            )


if __name__ == "__main__":
    unittest.main()
