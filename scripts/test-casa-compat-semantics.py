#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Prevent generated implementation labels from overstating evidence (#980)."""

import copy
import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path

import casa_compat_semantics as semantics

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("inventory", ROOT / "scripts/check-casa-inventory.py")
INVENTORY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(INVENTORY)


class SemanticReferenceTests(unittest.TestCase):
    def setUp(self):
        self.record = INVENTORY.load_inventory()["tclean"]
        self.surface = INVENTORY.load_surfaces()["tclean"]
        self.data = semantics.load(ROOT, "tclean")

    def test_every_casa_parameter_has_one_reviewed_row(self):
        semantics.validate(self.data, self.record, self.surface)
        self.assertEqual(len(self.data["parameters"]), 92)
        del self.data["parameters"]["mask"]
        with self.assertRaisesRegex(ValueError, "coverage"):
            semantics.validate(self.data, self.record, self.surface)

    def test_runtime_default_only_cannot_be_promoted_by_an_annotation(self):
        binding = next(b for b in self.surface["bindings"] if b["name"] == "pbmask")
        self.assertEqual(semantics.implementation(binding, {"status": "Implemented"}), "Default only")
        del binding["unimplemented"]
        self.assertEqual(semantics.implementation(binding, {"status": "Partial"}), "Partial")

    def test_conditional_refusals_are_rendered_from_the_runtime_gate(self):
        binding = next(b for b in self.surface["bindings"] if b["name"] == "rotatepastep")
        gate = binding["unimplemented"]["applies_when"]
        self.assertEqual(semantics.condition(gate), "(gridder='awproject' or gridder='awprojectft')")
        gate["predicates"][1]["value"]["value"] = "awphpg"
        self.assertEqual(semantics.condition(gate), "(gridder='awproject' or gridder='awphpg')")

    def test_signature_recording_does_not_become_a_passing_behavior_result(self):
        self.assertNotEqual(self.data["evidence"]["signature"]["state"], "passed")
        self.data["evidence"]["signature"]["state"] = "passed"
        with self.assertRaisesRegex(ValueError, "exact oracle/revision"):
            semantics.validate(self.data, self.record, self.surface)

    def test_unknown_evidence_and_unapproved_deviations_are_rejected(self):
        original = copy.deepcopy(self.data)
        self.data["parameters"]["mask"]["evidence"] = ["invented-pass"]
        with self.assertRaisesRegex(ValueError, "unknown semantic evidence"):
            semantics.validate(self.data, self.record, self.surface)
        self.data = original
        del self.data["decisions"][0]["owner_decision"]
        with self.assertRaisesRegex(ValueError, "owner decision"):
            semantics.validate(self.data, self.record, self.surface)

    def test_evidence_sources_must_exist_at_the_pinned_revision(self):
        self.data["evidence"]["reader"]["links"].append("does-not-exist.rs")
        with self.assertRaisesRegex(ValueError, "absent at its pinned revision"):
            semantics.validate_source_links(ROOT, self.data)

    def test_evidence_pins_must_outlive_their_branch(self):
        with tempfile.TemporaryDirectory() as scratch:
            repo = Path(scratch)

            def git(*args):
                return subprocess.run(
                    ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t",
                     "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgSign=false", *args],
                    text=True, capture_output=True, check=True,
                ).stdout.strip()

            git("init", "-q", "-b", "main")
            git("commit", "-q", "--allow-empty", "-m", "on main")
            on_main = git("rev-parse", "HEAD")
            git("checkout", "-q", "-b", "feature")
            git("commit", "-q", "--allow-empty", "-m", "evidence on a pull request's branch")
            on_branch = git("rev-parse", "HEAD")
            git("checkout", "-q", "main")

            # Before its squash merge, only the pull request's branch holds the pin.
            self.assertEqual(semantics.undurable_pins(repo, {on_main, on_branch}), [on_branch])
            # evidence-pins keeps it: a merge of the pinned commit, as EVIDENCE-PINS.md says.
            keeper = git("commit-tree", f"{on_main}^{{tree}}", "-p", on_main, "-p", on_branch, "-m", "keep")
            git("update-ref", "refs/heads/evidence-pins", keeper)
            git("branch", "-q", "-D", "feature")
            self.assertEqual(semantics.undurable_pins(repo, {on_main, on_branch}), [])
            git("update-ref", "-d", "refs/heads/evidence-pins")
            self.assertEqual(semantics.undurable_pins(repo, {on_main, on_branch}), [on_branch])
            git("branch", "-q", "-m", "main", "trunk")
            with self.assertRaisesRegex(ValueError, "checked against main"):
                semantics.undurable_pins(repo, {on_main})

    def test_valid_mask_nan_cannot_pass_the_paired_product_comparison(self):
        spec = importlib.util.spec_from_file_location("lifecycle", ROOT / "tools/casa-compat/tclean_lifecycle_probe.py")
        probe = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(probe)
        with self.assertRaisesRegex(AssertionError, "nonfinite"):
            probe.require_finite_masked([float("nan")], [True])
        with self.assertRaisesRegex(AssertionError, "empty"):
            probe.require_finite_masked([1.0], [False])
        probe.require_finite_masked([float("nan"), 1.0], [False, True])


if __name__ == "__main__":
    unittest.main()
