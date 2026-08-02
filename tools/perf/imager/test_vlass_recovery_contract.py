# SPDX-License-Identifier: LGPL-3.0-or-later
"""Fail-closed checks for the finite VLASS merge-recovery contract."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = ROOT / "tools/perf/imager/vlass_recovery_contract.json"
LEDGER_PATH = ROOT / "tools/perf/imager/vlass_recovery_launch_ledger.json"
CATALOG_PATH = ROOT / "tools/perf/imager/vlass_recovery_salvage_catalog.json"


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

    def test_pending_casa_b_v2_corrects_mask_without_mutating_v1(self) -> None:
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
        self.assertEqual("running", launch["disposition"])
        self.assertEqual(
            "20260802T191330Z-vlass-fragment-all-fields-clean-cap20000-casa-v2-5a0b3b07",
            launch["run_id"],
        )
        self.assertEqual(pending["manifest_sha256"], launch["manifest_sha256"])
        self.assertEqual(
            "ab7b6c3fa142d0cb3d0f54236b142b08b0aa837f120ffbf4314742723be04b27",
            launch["dry_run_receipt_sha256"],
        )

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
            self.assertEqual(
                "retired", by_id["db41-obsolete-seed-pr3-trim"]["status"]
            )
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
