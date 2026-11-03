#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Prevent generated implementation labels from overstating evidence (#980, #1053)."""

import copy
import importlib.util
import subprocess
import tempfile
import unittest
from unittest import mock
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


def quanta_review() -> tuple[dict, dict]:
    """A reviewed file for CASA's `quanta` class, every method refused."""
    signatures = INVENTORY.load_tool_signatures()["quanta"]
    evidence = {"signature": {
        "kind": "Signature inventory", "state": "recorded", "scope": "CASA 6.7.6.14's names and defaults.",
        "links": ["crates/casars-python/python/tests/data/casa_quanta_signatures.json"],
    }}
    methods = {name: {"status": "Not implemented", "scope": "Refused.", "evidence": ["signature"], "issue": 1053}
               for name in signatures["methods"]}
    data = {"schema_version": 1, "tool": "quanta", "casa_version": "6.7.6.14", "audit_revision": "0" * 40,
            "methods": methods, "behaviors": [], "decisions": [], "evidence": evidence}
    return data, signatures


class ToolReferenceTests(unittest.TestCase):
    def test_a_tool_review_covers_every_casa_method(self):
        data, signatures = quanta_review()
        semantics.validate_tool(data, signatures)
        del data["methods"]["convert"]
        with self.assertRaisesRegex(ValueError, "method coverage differs: \\['convert'\\]"):
            semantics.validate_tool(data, signatures)

    def test_a_refused_or_partial_method_names_its_issue(self):
        data, signatures = quanta_review()
        data["methods"]["convert"] = {"status": "Partial", "scope": "Some units.", "evidence": ["signature"]}
        with self.assertRaisesRegex(ValueError, "convert: a Partial method names the issue"):
            semantics.validate_tool(data, signatures)
        data["methods"]["convert"]["status"] = "Implemented"
        semantics.validate_tool(data, signatures)

    def test_a_tool_status_is_one_of_the_tool_statuses(self):
        data, signatures = quanta_review()
        data["methods"]["convert"]["status"] = "Default only"
        with self.assertRaisesRegex(ValueError, "invalid semantic implementation status"):
            semantics.validate_tool(data, signatures)

    def test_a_tool_review_names_casas_class_and_version(self):
        data, signatures = quanta_review()
        data["casa_version"] = "6.7.6.13"
        with self.assertRaisesRegex(ValueError, "oracle version mismatch"):
            semantics.validate_tool(data, signatures)

    def test_a_tool_renders_one_row_per_method_with_its_own_anchors(self):
        data, signatures = quanta_review()
        lines = semantics.render_tool(data, signatures)
        rows = [line for line in lines if line.startswith("| `")]
        self.assertEqual(len(rows), len(signatures["methods"]))
        convert = next(row for row in rows if row.startswith("| `convert`"))
        # CASA 6.7.6.14's `qa.convert(v=[], outunit=[])`.
        self.assertIn("| `v=[], outunit=[]` |", convert)
        self.assertIn("Not implemented (#1053)", convert)
        self.assertIn("[signature](#quanta-evidence-signature)", convert)
        self.assertIn('<a id="quanta-evidence-signature"></a>', lines)


class EvidenceStateTests(unittest.TestCase):
    def test_evidence_states_are_enumerated_outside_the_grandfathered_files(self):
        data, _ = quanta_review()
        data["evidence"]["signature"]["state"] = "recorded on the Mac"
        with self.assertRaisesRegex(ValueError, "is not one of"):
            semantics.validate_records(data, "quanta", data["methods"].values())
        semantics.validate_records(data, "tclean", data["methods"].values())

    def test_superseded_evidence_names_its_replacement(self):
        data, _ = quanta_review()
        data["evidence"]["signature"]["state"] = "superseded"
        with self.assertRaisesRegex(ValueError, "superseded_by"):
            semantics.validate_records(data, "quanta", data["methods"].values())
        data["evidence"]["signature"]["superseded_by"] = "signature"
        semantics.validate_records(data, "quanta", data["methods"].values())

    def test_only_the_grandfathered_files_keep_free_text_states(self):
        self.assertEqual(semantics.FREE_TEXT_STATES, {"tclean"})


class DecisionRecordTests(unittest.TestCase):
    def test_a_record_links_to_its_heading_in_the_deviations_document(self):
        # The document's own index links its headings this way.
        text = (ROOT / semantics.DEVIATIONS).read_text(encoding="utf-8")
        self.assertIn("(#smpredict-phases-a-moved-observemany-pointing-on-its-field)", text)
        self.assertEqual(semantics.heading_anchor("`sm.predict` phases a moved `observemany` pointing on its field"),
                         "smpredict-phases-a-moved-observemany-pointing-on-its-field")
        self.assertEqual(semantics.blob("0" * 40, semantics.DEVIATIONS),
                         f"https://github.com/bglenden/casa-rs/blob/{'0' * 40}/docs/CASA%20%28C%2B%2B%29%20bugs.md")

    def test_a_record_must_name_a_heading_at_the_audit_revision(self):
        data, _ = quanta_review()
        data["audit_revision"] = semantics.subprocess.run(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
        data["evidence"]["signature"]["links"] = []
        data["decisions"] = [{"id": "x", "state": "Proposed", "record": "The tools convert quantities whose units do not conform",
                              "inputs": "i", "casa": "c", "ours": "o", "rationale": "r", "impact": "m", "evidence": []}]
        # HEAD is a pull request's own commit in its CI, which the pin-durability check
        # (test_evidence_pins_must_outlive_their_branch) rightly refuses; this law is the records'.
        with mock.patch.object(semantics, "undurable_pins", return_value=[]):
            semantics.validate_source_links(ROOT, data)
            data["decisions"][0]["record"] = "A heading the document lacks"
            with self.assertRaisesRegex(ValueError, "name no heading"):
                semantics.validate_source_links(ROOT, data)


if __name__ == "__main__":
    unittest.main()
