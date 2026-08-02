#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Characterization tests for the one-time checked evidence migration."""

from __future__ import annotations

import json
import pathlib
import tempfile
import unittest

from perf_harness.schema import (
    LEGACY_RUN_RESULT_SCHEMA_VERSION,
    validate_legacy_run_result_v2,
)

import migrate_evidence_v1_to_v2 as migration


class EvidenceMigrationTests(unittest.TestCase):
    def test_exact_checked_in_shapes_migrate_once_to_canonical_v2(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            evidence = root / "evidence" / "artifacts"
            evidence.mkdir(parents=True)
            artifacts = {
                "run.json": {
                    "schema_version": 1,
                    "status": "completed",
                    "workload": {"id": "run-workload"},
                    "results": {"rust": {"timings_seconds": {"median": 1.0}}},
                },
                "comparison.json": {
                    "status": "completed",
                    "structured_difference_review": {
                        "label": "good",
                        "products": {".image": "good"},
                    },
                    "products": {".image": {"status": "compared"}},
                },
                "alternating.json": {
                    "schema_version": 1,
                    "status": "completed",
                    "comparison_id": "alternating-test",
                    "created_at": "2026-07-18T00:00:00Z",
                    "verdict": {"status": "pass", "no_slowdown": True},
                    "schedule": [],
                    "runs": [],
                },
            }
            for name, value in artifacts.items():
                (evidence / name).write_text(json.dumps(value), encoding="utf-8")
            entries = [
                {
                    "artifact_id": "run",
                    "artifact_role": "baseline",
                    "workload_id": "run-workload",
                    "checked_in_path": "evidence/artifacts/run.json",
                    "sha256": "0" * 64,
                },
                {
                    "artifact_id": "comparison",
                    "artifact_role": "product_comparison",
                    "workload_id": "comparison-workload",
                    "checked_in_path": "evidence/artifacts/comparison.json",
                    "sha256": "0" * 64,
                },
                {
                    "artifact_id": "alternating",
                    "artifact_role": "counterbalanced_comparison",
                    "workload_id": "alternating-workload",
                    "checked_in_path": "evidence/artifacts/alternating.json",
                    "sha256": "0" * 64,
                },
            ]
            manifest = root / "evidence" / "manifest.json"
            manifest.write_text(
                json.dumps({"schema_version": 1, "artifacts": entries}),
                encoding="utf-8",
            )

            migration.migrate_manifest(manifest)
            migration.migrate_manifest(manifest)

            run = json.loads((evidence / "run.json").read_text(encoding="utf-8"))
            comparison = json.loads(
                (evidence / "comparison.json").read_text(encoding="utf-8")
            )
            alternating = json.loads(
                (evidence / "alternating.json").read_text(encoding="utf-8")
            )
            for name, value in (
                ("run", run),
                ("comparison", comparison),
                ("alternating", alternating),
            ):
                validate_legacy_run_result_v2(value, source=name)
                self.assertEqual(
                    LEGACY_RUN_RESULT_SCHEMA_VERSION, value["schema_version"]
                )
            self.assertEqual("workload_run", run["kind"])
            self.assertEqual("image_comparison", comparison["kind"])
            self.assertEqual(
                "completed",
                comparison["results"]["product_comparison"]["status"],
            )
            self.assertEqual("alternating_comparison", alternating["kind"])
            updated = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertTrue(
                all(entry["sha256"] != "0" * 64 for entry in updated["artifacts"])
            )


if __name__ == "__main__":
    unittest.main()
