#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Contract tests for the executable imaging architecture policy."""

from __future__ import annotations

import copy
import importlib.util
import sys
import unittest
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKER_PATH = REPO_ROOT / "scripts/check-imaging-architecture.py"
POLICY_PATH = REPO_ROOT / "resources/imaging-architecture/dependency-policy.json"
MATRIX_PATH = REPO_ROOT / "resources/imaging-architecture/migration-matrix.json"


def load_checker() -> Any:
    spec = importlib.util.spec_from_file_location("check_imaging_architecture", CHECKER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {CHECKER_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


checker = load_checker()


def load_policy() -> dict[str, Any]:
    policy = checker.load_object(POLICY_PATH, "dependency policy")
    checker.validate_policy(policy)
    return policy


def dependency(target: str, kind: str = "normal", *, external: bool = False) -> dict[str, Any]:
    return {
        "name": target,
        "kind": kind,
        "path": None if external else f"/workspace/crates/{target}",
    }


def metadata_for_policy(policy: dict[str, Any]) -> dict[str, Any]:
    package_names = set(policy["package_layers"])
    package_names.update(policy["native_package_workspace_dependencies"].keys())
    for allowed in policy["native_package_workspace_dependencies"].values():
        package_names.update(allowed)
    for edge in policy["frozen_legacy_workspace_edges"]:
        package_names.add(edge["source"])
        package_names.add(edge["target"])
    dependencies: dict[str, list[dict[str, Any]]] = {
        package: [] for package in package_names
    }
    for edge in policy["frozen_legacy_workspace_edges"]:
        dependencies[edge["source"]].append(
            dependency(edge["target"], edge["kind"])
        )
    return {
        "workspace_root": "/workspace",
        "packages": [
            {
                "name": package,
                "manifest_path": f"/workspace/crates/{package}/Cargo.toml",
                "dependencies": dependencies[package],
            }
            for package in sorted(package_names)
        ],
    }


def package(metadata: dict[str, Any], name: str) -> dict[str, Any]:
    return next(value for value in metadata["packages"] if value["name"] == name)


class ArchitecturePolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = load_policy()

    def test_rejects_every_undeclared_native_layer_edge(self) -> None:
        allowed = {
            (source, target)
            for source, targets in self.policy["allowed_logical_edges"].items()
            for target in targets
        }
        for source in self.policy["layers"]:
            for target in self.policy["layers"]:
                with self.subTest(source=source, target=target):
                    if (source, target) in allowed:
                        checker.validate_logical_edge(self.policy, source, target)
                    else:
                        with self.assertRaisesRegex(
                            checker.ArchitectureError,
                            rf"forbidden logical imaging edge: {source} -> {target}",
                        ):
                            checker.validate_logical_edge(self.policy, source, target)

    def test_workspace_check_rejects_frontend_backend_edge(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["package_layers"]["synthetic-frontend"] = "frontend"
        policy["package_layers"]["synthetic-backend"] = "backend"
        policy["native_package_workspace_dependencies"]["synthetic-frontend"] = [
            "synthetic-backend"
        ]
        policy["native_package_workspace_dependencies"]["synthetic-backend"] = []
        checker.validate_policy(policy)
        metadata = metadata_for_policy(policy)
        package(metadata, "synthetic-frontend")["dependencies"].append(
            dependency("synthetic-backend")
        )
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"forbidden logical imaging edge: frontend -> backend",
        ):
            checker.validate_workspace(policy, metadata)

    def test_workspace_check_freezes_legacy_edges_exactly(self) -> None:
        metadata = metadata_for_policy(self.policy)
        edge = self.policy["frozen_legacy_workspace_edges"][0]
        source = package(metadata, edge["source"])
        source["dependencies"] = [
            value
            for value in source["dependencies"]
            if not (
                value["name"] == edge["target"]
                and (value["kind"] or "normal") == edge["kind"]
            )
        ]
        with self.assertRaisesRegex(checker.ArchitectureError, r"removed=\["):
            checker.validate_workspace(self.policy, metadata)

    def test_device_free_layer_rejects_direct_device_api(self) -> None:
        metadata = metadata_for_policy(self.policy)
        package(metadata, "casa-imaging-model")["dependencies"].append(
            dependency("objc2-metal", external=True)
        )
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"device-free package casa-imaging-model\(science\) imports objc2-metal",
        ):
            checker.validate_workspace(self.policy, metadata)

    def test_native_package_rejects_undeclared_workspace_dependency(self) -> None:
        metadata = metadata_for_policy(self.policy)
        package(metadata, "casa-imaging-runtime")["dependencies"].append(
            dependency("casa-ms")
        )
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"casa-imaging-runtime has undeclared workspace dependencies: \['casa-ms'\]",
        ):
            checker.validate_workspace(self.policy, metadata)


class MigrationMatrixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = load_policy()
        self.matrix = checker.synthetic_matrix(self.policy)

    def test_valid_synthetic_matrix_passes(self) -> None:
        checker.validate_migration_matrix(self.matrix, self.policy)

    def test_required_mutations_fail(self) -> None:
        mutations: list[tuple[str, dict[str, Any], str]] = []
        bad_status = copy.deepcopy(self.matrix)
        bad_status["rows"][0]["status"] = "Fallback"
        mutations.append(("status", bad_status, "status must be one of"))
        missing_contract = copy.deepcopy(self.matrix)
        missing_contract["rows"][0]["acceptance_contract"] = "missing"
        mutations.append(("contract", missing_contract, "unknown acceptance contract"))
        missing_obligation = copy.deepcopy(self.matrix)
        missing_obligation["rows"][0]["migration_obligation"] = None
        mutations.append(("obligation", missing_obligation, "must be a non-empty object"))
        missing_crosswalk = copy.deepcopy(self.matrix)
        for row in missing_crosswalk["rows"]:
            row["evidence_issues"] = [488]
        mutations.append(("crosswalk", missing_crosswalk, "omits required crosswalk issues"))
        changed_crosswalk = copy.deepcopy(self.matrix)
        changed_crosswalk["required_issue_crosswalk"] = [488]
        mutations.append(
            (
                "declared-crosswalk",
                changed_crosswalk,
                "required_issue_crosswalk differs from dependency policy",
            )
        )
        duplicate_row = copy.deepcopy(self.matrix)
        duplicate_row["rows"].append(copy.deepcopy(duplicate_row["rows"][0]))
        mutations.append(("duplicate", duplicate_row, "repeats row id"))

        for name, mutation, message in mutations:
            with self.subTest(name=name):
                with self.assertRaisesRegex(checker.ArchitectureError, message):
                    checker.validate_migration_matrix(mutation, self.policy)

    def test_live_inventory_rejects_a_missing_row(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        matrix["rows"].pop()
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"inventory and rows differ",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_live_contract_rejects_a_weakened_normalized_rms_ceiling(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        matrix["acceptance_contracts"]["scientific-products-v1"]["comparator"][
            "normalized_rms_ceiling"
        ] = 0.01
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"normalized_rms_ceiling may not exceed 0.001",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_live_obligation_requires_a_ticket_and_reason(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        matrix["rows"][0]["migration_obligation"] = {"ticket": "T23/#509"}
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"migration_obligation.reason must be a non-empty string",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_live_source_evidence_rejects_a_renamed_symbol(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        matrix["rows"][0]["source_evidence"] = [
            "crates/casa-imaging/src/types.rs::RenamedImagingRequest"
        ]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"source evidence token .* was not found",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_live_baseline_rejects_a_missing_repository_manifest(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        matrix["rows"][0]["baseline_manifests"] = [
            "repo://tools/perf/imager/evidence/missing.json"
        ]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"cannot read baseline manifest",
        ):
            checker.validate_migration_matrix(matrix, self.policy)


if __name__ == "__main__":
    unittest.main()
