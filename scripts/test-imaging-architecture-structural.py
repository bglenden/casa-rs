#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused structural tests for the imaging architecture checker."""

from __future__ import annotations

import copy
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKER_PATH = REPO_ROOT / "scripts/check-imaging-architecture.py"
POLICY_PATH = REPO_ROOT / "resources/imaging-architecture/dependency-policy.json"
MATRIX_PATH = REPO_ROOT / "resources/imaging-architecture/migration-matrix.json"

SPEC = importlib.util.spec_from_file_location("check_imaging_architecture", CHECKER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load imaging architecture checker")
checker = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = checker
SPEC.loader.exec_module(checker)


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def live_metadata() -> dict:
    return checker.load_cargo_metadata(None)


class PolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = load_json(POLICY_PATH)
        self.metadata = live_metadata()

    def test_live_policy_and_workspace_are_structurally_valid(self) -> None:
        checker.validate_policy(self.policy)
        checker.validate_workspace(self.policy, self.metadata)
        checker.validate_forward_invariants(self.policy, self.metadata)
        checker.validate_source_boundaries(self.policy)

    def test_every_workspace_package_is_classified(self) -> None:
        packages, _edges, _dependencies = checker.workspace_edges(self.metadata)
        self.assertEqual(packages, set(self.policy["workspace_package_classification"]))

    def test_native_dependency_allowlist_rejects_a_new_edge(self) -> None:
        mutation = copy.deepcopy(self.metadata)
        package = next(
            item
            for item in mutation["packages"]
            if item["name"] == "casa-imaging-runtime"
        )
        package["dependencies"].append(
            {
                "name": "casars-imager",
                "path": str(REPO_ROOT / "crates/casars-imager"),
                "kind": None,
            }
        )
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            "forbidden logical imaging edge|undeclared workspace dependencies",
        ):
            checker.validate_workspace(self.policy, mutation)

    def test_frontend_cannot_import_execution_or_device_apis(self) -> None:
        boundary = next(
            item
            for item in self.policy["source_boundaries"]
            if item["id"] == "rust-imaging-frontends"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "frontend.rs").write_text(
                "use casa_imaging_runtime::run;\n", encoding="utf-8"
            )
            mutation = copy.deepcopy(boundary)
            mutation["roots"] = ["frontend.rs"]
            with self.assertRaisesRegex(
                checker.ArchitectureError, "Rust frontend imports an execution"
            ):
                checker.validate_source_boundaries(
                    {"source_boundaries": [mutation]}, root
                )

    def test_selected_observation_requires_injected_measures_provider(self) -> None:
        boundary = next(
            item
            for item in self.policy["source_boundaries"]
            if item["id"] == "t17-selected-observation-provider-injection"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "selected.rs").write_text(
                "MeasuresRuntime::open_discovered()\n", encoding="utf-8"
            )
            mutation = copy.deepcopy(boundary)
            mutation["roots"] = ["selected.rs"]
            with self.assertRaisesRegex(
                checker.ArchitectureError,
                "must receive an injected Measures provider",
            ):
                checker.validate_source_boundaries(
                    {"source_boundaries": [mutation]}, root
                )

    def test_t15_evidence_cannot_be_public_or_redirect_source(self) -> None:
        boundary = next(
            item
            for item in self.policy["source_boundaries"]
            if item["id"] == "t15-private-walking-skeleton"
        )
        for text in ["pub fn leaked() {}\n", "include!(\"foreign.rs\");\n"]:
            with self.subTest(text=text), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "walking.rs").write_text(text, encoding="utf-8")
                mutation = copy.deepcopy(boundary)
                mutation["roots"] = ["walking.rs"]
                with self.assertRaisesRegex(
                    checker.ArchitectureError, "must remain private test code"
                ):
                    checker.validate_source_boundaries(
                        {"source_boundaries": [mutation]}, root
                    )

    def test_runtime_source_cannot_reference_migration_matrix(self) -> None:
        metadata = copy.deepcopy(self.metadata)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            (root / "src/lib.rs").write_text(
                'const MATRIX: &str = include_str!("migration-matrix.json");\n',
                encoding="utf-8",
            )
            package = next(
                item
                for item in metadata["packages"]
                if item["name"] == "casa-imaging-runtime"
            )
            package["manifest_path"] = str(root / "Cargo.toml")
            package["targets"] = []
            with self.assertRaisesRegex(
                checker.ArchitectureError,
                "must not compile or interpret the migration matrix",
            ):
                checker.validate_forward_invariants(self.policy, metadata)


class MatrixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = load_json(POLICY_PATH)
        self.matrix = load_json(MATRIX_PATH)

    def test_live_matrix_is_valid(self) -> None:
        checker.validate_migration_matrix(self.matrix, self.policy)

    def test_fallback_status_is_rejected(self) -> None:
        matrix = copy.deepcopy(self.matrix)
        matrix["rows"][0]["status"] = "Fallback"
        with self.assertRaisesRegex(checker.ArchitectureError, "status must be one of"):
            checker.validate_migration_matrix(
                matrix, self.policy, enforce_accepted_scope=False
            )

    def test_unavailable_row_requires_a_migration_obligation(self) -> None:
        matrix = copy.deepcopy(self.matrix)
        row = next(item for item in matrix["rows"] if item["status"] == "TemporarilyUnavailable")
        row["migration_obligation"] = None
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            "migration_obligation must be a non-empty object",
        ):
            checker.validate_migration_matrix(
                matrix, self.policy, enforce_accepted_scope=False
            )

    def test_native_row_cannot_retain_a_migration_obligation(self) -> None:
        matrix = copy.deepcopy(self.matrix)
        row = next(item for item in matrix["rows"] if item["status"] == "Native")
        row["migration_obligation"] = {"ticket": "T99/#999", "reason": "wrong"}
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            "migration_obligation must be null for Native",
        ):
            checker.validate_migration_matrix(
                matrix, self.policy, enforce_accepted_scope=False
            )

    def test_inventory_and_rows_must_match(self) -> None:
        matrix = copy.deepcopy(self.matrix)
        matrix["rows"].pop()
        with self.assertRaisesRegex(
            checker.ArchitectureError, "inventory and rows differ"
        ):
            checker.validate_migration_matrix(
                matrix, self.policy, enforce_accepted_scope=False
            )

    def test_scientific_error_ceiling_cannot_be_weakened(self) -> None:
        matrix = copy.deepcopy(self.matrix)
        matrix["acceptance_contracts"]["scientific-products-v1"]["comparator"][
            "normalized_rms_ceiling"
        ] = 0.01
        with self.assertRaisesRegex(checker.ArchitectureError, "may not exceed 0.001"):
            checker.validate_migration_matrix(
                matrix, self.policy, enforce_accepted_scope=False
            )

    def test_t37_retains_spectral_operator_ownership(self) -> None:
        checker.validate_t37_spectral_operator_transfer(self.matrix["rows"])

    def test_t37_rejects_displaced_serial_owner_evidence(self) -> None:
        matrix = copy.deepcopy(self.matrix)
        row = next(
            item for item in matrix["rows"] if item["id"] == "capability.standard-gridder"
        )
        row["source_evidence"][0] = (
            "crates/casa-imaging-reconstruction/src/serial_mfs.rs::pub struct CompleteDataOwnerState"
        )
        with self.assertRaisesRegex(checker.ArchitectureError, "lacks spectral-operator"):
            checker.validate_t37_spectral_operator_transfer(matrix["rows"])


if __name__ == "__main__":
    unittest.main()
