#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later

import json
import tempfile
import unittest
from pathlib import Path

import gui_acceptance as harness


class GuiAcceptanceHarnessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = harness.load_manifest()

    def test_every_journey_has_a_validated_unique_contract(self) -> None:
        journeys = self.manifest["journeys"]
        self.assertEqual(
            [journey["id"] for journey in journeys],
            [
                "gui-test",
                "assistant-live-gui",
                "notebook-roundtrip-gui",
                "tutorial-journey-gui",
            ],
        )
        for journey in journeys:
            with self.subTest(journey=journey["id"]):
                self.assertIn(journey["timeout_class"], self.manifest["timeout_classes"])
                self.assertTrue(journey["artifacts"])

    def test_each_journey_reports_preflight_failure(self) -> None:
        for journey in self.manifest["journeys"]:
            with self.subTest(journey=journey["id"]):
                with self.assertRaises(harness.HarnessError):
                    harness.require_commands(journey, which=lambda _command: None)

    def fake_paths(
        self, root: Path, journey: dict, *, create_project: bool = True
    ) -> harness.JourneyPaths:
        artifact_root = root / "artifacts"
        artifact_root.mkdir()
        result = artifact_root / journey["result_bundle"]
        result.mkdir()
        project = None
        receipt = None
        test_report = None
        published_report = None
        if journey["project"]["policy"] != "none":
            project = root / "project"
            if create_project:
                project.mkdir()
            gate = journey["gate"]
            receipt = project / gate["receipt"]
            if gate.get("report"):
                test_report = project / gate["report"]
                published_report = artifact_root / gate["published_report"]
        return harness.JourneyPaths(
            artifact_root=artifact_root,
            result_bundle=result,
            summary=artifact_root / f"{journey['id']}.evidence.json",
            gate=root / "gate.plist" if journey["gate"] else None,
            project=project,
            receipt=receipt,
            test_report=test_report,
            published_report=published_report,
        )

    def test_failure_retains_projects_and_evidence_for_every_journey(self) -> None:
        for journey in self.manifest["journeys"]:
            with self.subTest(journey=journey["id"]), tempfile.TemporaryDirectory() as temp:
                paths = self.fake_paths(Path(temp), journey)
                harness.finalize_run(journey, paths, "abc123", 1)
                summary = json.loads(paths.summary.read_text())
                self.assertEqual(summary["status"], "failed")
                self.assertEqual(
                    summary["project_retained"],
                    bool(paths.project and paths.project.exists()),
                )

    def test_success_cleans_projects_and_emits_complete_artifacts(self) -> None:
        for journey in self.manifest["journeys"]:
            with self.subTest(journey=journey["id"]), tempfile.TemporaryDirectory() as temp:
                paths = self.fake_paths(Path(temp), journey)
                if paths.receipt:
                    paths.receipt.write_bytes(b"")
                if paths.test_report:
                    paths.test_report.write_text('{"schema_version": 1}\n')
                harness.finalize_run(journey, paths, "abc123", 0)
                if paths.project:
                    self.assertFalse(paths.project.exists())
                for artifact in harness.artifact_paths(journey, paths):
                    self.assertTrue(artifact.exists(), artifact)
                self.assertEqual(json.loads(paths.summary.read_text())["status"], "passed")

    def test_success_rejects_incomplete_evidence_for_every_journey(self) -> None:
        for journey in self.manifest["journeys"]:
            with self.subTest(journey=journey["id"]), tempfile.TemporaryDirectory() as temp:
                paths = self.fake_paths(Path(temp), journey)
                if paths.test_report:
                    paths.test_report.write_text('{"schema_version": 1}\n')
                if not paths.receipt:
                    paths.result_bundle.rmdir()
                with self.assertRaises(harness.HarnessError):
                    harness.finalize_run(journey, paths, "abc123", 0)
                summary = json.loads(paths.summary.read_text())
                self.assertEqual(summary["status"], "failed")
                if paths.project:
                    self.assertTrue(paths.project.exists())

    def test_live_gate_fields_are_complete(self) -> None:
        for journey in self.manifest["journeys"]:
            if not journey["gate"]:
                continue
            with self.subTest(journey=journey["id"]), tempfile.TemporaryDirectory() as temp:
                paths = self.fake_paths(Path(temp), journey)
                values = harness.gate_values(journey, paths, "/usr/bin/python3", "abc123", False)
                self.assertEqual(set(values), set(journey["gate"]["required_fields"]))


if __name__ == "__main__":
    unittest.main()
