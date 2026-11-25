#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Test CI scope selection without starting any CI jobs."""

import json
import subprocess
import unittest
from unittest.mock import patch

from ci_scope import affected_scope, changed_paths, workspace_packages


class ScopeTests(unittest.TestCase):
    def setUp(self):
        owners = {f"crates/{name}": name for name in (
            "casa-calibration", "casa-task-runtime", "casars-python", "test-util", "other",
        )}
        dependencies = {
            "casa-calibration": set(),
            "casa-task-runtime": {"casa-calibration"},
            "casars-python": {"casa-task-runtime"},
            "test-util": {"casa-calibration"},
            "other": {"test-util"},
        }
        workspace = patch("ci_scope.workspace_packages", return_value=(owners, dependencies))
        workspace.start()
        self.addCleanup(workspace.stop)

    def test_prose_and_tool_changes_only_select_relevant_jobs(self):
        scope = affected_scope(["docs/install.md", "suites/perf/imager/test_run_workload.py"])
        self.assertEqual(scope.rust, "static")
        self.assertTrue(scope.docs and scope.perf)
        self.assertEqual(scope.python, "none")

    def test_new_workspace_roots_select_owning_packages(self):
        owners = {
            "libs/casacore/casa-tables": "casa-tables",
            "dev/casa-test-support": "casa-test-support",
            "crates/casa-ms": "casa-ms",
        }
        deps = {
            "casa-tables": set(),
            "casa-test-support": {"casa-tables"},
            "casa-ms": {"casa-tables"},
        }
        with patch("ci_scope.workspace_packages", return_value=(owners, deps)):
            scope = affected_scope(["libs/casacore/casa-tables/src/lib.rs"])
        self.assertEqual(scope.rust, "affected")
        self.assertEqual(scope.packages.split(), ["casa-ms", "casa-tables", "casa-test-support"])

        with patch("ci_scope.workspace_packages", return_value=(owners, deps)):
            dev_scope = affected_scope(["dev/casa-test-support/src/lib.rs"])
        self.assertEqual(dev_scope.rust, "affected")
        self.assertEqual(dev_scope.packages, "casa-test-support")

    def test_rust_source_reaches_direct_consumers(self):
        scope = affected_scope(["crates/casa-calibration/src/lib.rs"])
        self.assertEqual(scope.rust, "affected")
        self.assertEqual(scope.packages.split(), [
            "casa-calibration", "casa-task-runtime", "test-util",
        ])
        self.assertEqual(scope.python, "none")

    def test_rust_test_only_skips_package_builds(self):
        scope = affected_scope(["crates/casa-calibration/tests/example.rs"])
        self.assertEqual(scope.packages, "casa-calibration")
        self.assertEqual(scope.python, "none")

    def test_direct_python_keeps_rust_contract_tests(self):
        scope = affected_scope(["crates/casars-python/python/casars/tasks.py", "README.md"])
        self.assertEqual(scope.packages, "casars-python")
        self.assertEqual(scope.python, "editable")
        self.assertTrue(scope.docs)

    def test_unknown_or_build_paths_use_full_checks(self):
        for path in ["Cargo.lock", ".github/workflows/ci.yml", "scripts/test-slow.sh"]:
            with self.subTest(path=path):
                scope = affected_scope([path])
                self.assertEqual(scope.rust, "full")
                self.assertEqual(scope.python, "full")

    def test_task_contract_changes_select_editable_python_checks(self):
        scope = affected_scope(["crates/casa-task-runtime/src/lib.rs"])
        self.assertEqual(scope.packages, "casa-task-runtime casars-python")
        self.assertEqual(scope.python, "editable")

    def test_evidence_updates_preserve_scoped_checks(self):
        scope = affected_scope([
            "crates/casa-calibration/src/lib.rs",
            "tools/casa-compat/calibration_initweights_oracle.py",
            "resources/casa-compat/evidence/initweights.json",
        ])
        self.assertEqual(scope.rust, "affected")


class RevisionTests(unittest.TestCase):
    def test_dispatch_requires_a_pinned_base(self):
        with self.assertRaisesRegex(ValueError, "full base commit SHA"):
            changed_paths("main")

    def test_head_cannot_be_its_own_base(self):
        head = "a" * 40
        with patch("ci_scope.command", return_value=head.encode()):
            with self.assertRaisesRegex(ValueError, "must precede"):
                changed_paths(head)

    def test_unrelated_base_cannot_scope_the_revision(self):
        base, head = "a" * 40, b"b" * 40
        with patch("ci_scope.command", side_effect=[head, subprocess.CalledProcessError(1, "git")]):
            with self.assertRaises(subprocess.CalledProcessError):
                changed_paths(base)

    def test_scopes_a_branch_head_against_its_reviewed_base(self):
        base, head = "a" * 40, b"b" * 40
        with patch("ci_scope.command", side_effect=[head, b"", b"docs/install.md\0"]) as git:
            self.assertEqual(changed_paths(base), ["docs/install.md"])
            git.assert_called_with("git", "diff", "--name-only", "--no-renames", "-z", base, "HEAD", "--")


class CargoMetadataTests(unittest.TestCase):
    def test_registry_dependencies_have_no_path(self):
        def package(name, dependencies):
            return {
                "id": name, "name": name, "manifest_path": f"/repo/crates/{name}/Cargo.toml",
                "dependencies": dependencies,
            }

        metadata = {
            "workspace_root": "/repo",
            "workspace_members": ["a", "b"],
            "packages": [package("a", [{"kind": None}]), package("b", [
                {"kind": "build", "path": "/repo/crates/a"},
                {"kind": "dev", "path": "/repo/crates/a"},
            ])],
        }
        with patch("ci_scope.command", return_value=json.dumps(metadata)):
            owners, dependencies = workspace_packages()
        self.assertEqual(owners, {"crates/a": "a", "crates/b": "b"})
        self.assertEqual(dependencies, {"a": set(), "b": {"a"}})


if __name__ == "__main__":
    unittest.main()
