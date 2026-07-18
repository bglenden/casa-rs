from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tutorial_parity.commands import run_command
from tutorial_parity.model import RuntimeResources
from tutorial_parity.resources import ResourceError, resolve_resources
from tutorial_parity.runner import manifests, run_section
from tutorial_parity.schema import validate_result


class _Plan:
    outputs: tuple[Path, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {"kind": "test-plan"}


class _Adapter:
    def plan(self, manifest, surface, resources) -> _Plan:
        return _Plan()

    def execute(self, plan: _Plan) -> dict[str, object]:
        return {
            "status": "completed",
            "operations": [plan.as_dict()],
            "artifacts": [],
            "reason": None,
        }


class RuntimeContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = manifests()[0]

    def test_resource_preflight_reports_missing_dataset_by_category(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            native_python = root / "python"
            native_python.touch()
            with self.assertRaisesRegex(ResourceError, "missing casa_image") as raised:
                resolve_resources(
                    self.manifest,
                    pack_root=root,
                    native_python=native_python,
                    casa_python=None,
                    binary_dir=root,
                    ghostty_capture=None,
                    evidence_root=None,
                    require_existing=True,
                )
            self.assertEqual(raised.exception.category, "dataset_missing")

    def test_command_timeout_is_captured_as_typed_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = run_command(
                [sys.executable, "-c", "import time; time.sleep(5)"],
                cwd=Path(directory),
                timeout_seconds=0.05,
            )
        self.assertEqual(result.return_code, 124)
        self.assertTrue(result.timed_out)
        self.assertIn("timed out", result.stderr)
        self.assertTrue(result.as_dict()["timed_out"])

    def test_run_section_updates_typed_evidence_docs_and_screenshot_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            pack_root = Path(directory)
            resources = RuntimeResources(
                repo_root=Path(__file__).resolve().parents[2],
                pack_root=pack_root,
                native_python=Path(sys.executable),
                casa_python=None,
                binary_dir=pack_root,
                ghostty_capture=None,
                evidence_root=pack_root / ".casa-rs" / "evidence" / "tutorial-parity",
                dry_run=False,
            )
            registry = {name: _Adapter() for name in ("casa", "cli", "python", "tui", "gui")}
            with patch("tutorial_parity.runner.adapters", return_value=registry), patch(
                "tutorial_parity.runner.run_comparator",
                return_value={"status": "passed", "plugin": self.manifest.comparison.plugin},
            ):
                result = run_section(
                    self.manifest,
                    resources,
                    selected_surfaces={"gui"},
                    dry_run=False,
                    gui_cache={},
                )

            result_path = pack_root / self.manifest.evidence["result"]
            persisted = validate_result(json.loads(result_path.read_text(encoding="utf-8")), str(result_path))
            self.assertEqual(persisted["status"], "completed")
            self.assertEqual(result["status"], "completed")
            self.assertTrue((pack_root / self.manifest.evidence["review"]).is_file())
            screenshot_spec = json.loads(
                (pack_root / self.manifest.evidence["screenshot_spec"]).read_text(encoding="utf-8")
            )
            self.assertEqual(screenshot_spec["gui_journey"], "tutorial-journey-gui")
            for relative in self.manifest.evidence["documentation"]:
                self.assertTrue((pack_root / relative).is_file())
            self.assertEqual(len(persisted["artifacts"]), 2)


if __name__ == "__main__":
    unittest.main()
