#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Contract tests for the executable imaging architecture policy."""

from __future__ import annotations

import copy
import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKER_PATH = REPO_ROOT / "scripts/check-imaging-architecture.py"
POLICY_PATH = REPO_ROOT / "resources/imaging-architecture/dependency-policy.json"
MATRIX_PATH = REPO_ROOT / "resources/imaging-architecture/migration-matrix.json"


def load_checker() -> Any:
    spec = importlib.util.spec_from_file_location(
        "check_imaging_architecture", CHECKER_PATH
    )
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


def dependency(
    target: str, kind: str = "normal", *, external: bool = False
) -> dict[str, Any]:
    return {
        "name": target,
        "kind": kind,
        "path": None if external else f"/workspace/crates/{target}",
    }


def metadata_for_policy(policy: dict[str, Any]) -> dict[str, Any]:
    package_names = set(policy["package_layers"])
    package_names.update(policy.get("workspace_package_classification", {}))
    package_names.update(policy["native_package_workspace_dependencies"].keys())
    for allowed in policy["native_package_workspace_dependencies"].values():
        package_names.update(allowed)
    for edge in policy["frozen_legacy_workspace_edges"]:
        package_names.add(edge["source"])
        package_names.add(edge["target"])
    for edge in policy["frozen_transitional_workspace_edges"]:
        package_names.add(edge["source"])
        package_names.add(edge["target"])
    dependencies: dict[str, list[dict[str, Any]]] = {
        package: [] for package in package_names
    }
    for edge in policy["frozen_legacy_workspace_edges"]:
        dependencies[edge["source"]].append(dependency(edge["target"], edge["kind"]))
    for edge in policy["frozen_transitional_workspace_edges"]:
        dependencies[edge["source"]].append(dependency(edge["target"], edge["kind"]))
    for source, targets in policy["native_package_workspace_dependencies"].items():
        for target in targets:
            dependencies[source].append(dependency(target))
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

    def test_policy_cannot_authorize_a_new_logical_edge(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["allowed_logical_edges"]["frontend"].append("backend")
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"allowed edges differ from the accepted graph",
        ):
            checker.validate_policy(policy)

    def test_every_surface_package_requires_a_logical_layer(self) -> None:
        policy = copy.deepcopy(self.policy)
        del policy["package_layers"]["casars-frontend-services"]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"imaging surface packages lack logical layers: \['casars-frontend-services'\]",
        ):
            checker.validate_policy(policy)

    def test_surface_classification_and_layer_cannot_be_coordinately_removed(
        self,
    ) -> None:
        policy = copy.deepcopy(self.policy)
        policy["workspace_package_classification"]["casars-python"] = "support"
        del policy["package_layers"]["casars-python"]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"workspace package layers, classifications, native dependencies, or device policy differ",
        ):
            checker.validate_policy(policy)

    def test_native_dependency_allowlist_cannot_authorize_itself(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["native_package_workspace_dependencies"]["casa-imaging-runtime"] = [
            "casa-imaging-model"
        ]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"workspace package layers, classifications, native dependencies, or device policy differ",
        ):
            checker.validate_policy(policy)

    def test_workspace_check_rejects_frontend_backend_edge(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["package_layers"]["synthetic-frontend"] = "frontend"
        policy["package_layers"]["synthetic-backend"] = "backend"
        policy.setdefault("workspace_package_classification", {}).update(
            {"synthetic-frontend": "native", "synthetic-backend": "native"}
        )
        policy["native_package_workspace_dependencies"]["synthetic-frontend"] = [
            "synthetic-backend"
        ]
        policy["native_package_workspace_dependencies"]["synthetic-backend"] = []
        checker.validate_policy(policy, enforce_accepted_scope=False)
        metadata = metadata_for_policy(policy)
        package(metadata, "synthetic-frontend")["dependencies"].append(
            dependency("synthetic-backend")
        )
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"forbidden logical imaging edge: frontend -> backend",
        ):
            checker.validate_workspace(policy, metadata)

    def test_application_cannot_depend_on_a_backend(self) -> None:
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"forbidden logical imaging edge: application -> backend",
        ):
            checker.validate_logical_edge(self.policy, "application", "backend")

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

    def test_policy_cannot_replace_a_frozen_legacy_exception(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["frozen_legacy_workspace_edges"][0]["target"] = "casa-values"
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"differs from the 16 accepted exceptions",
        ):
            checker.validate_policy(policy)

    def test_policy_cannot_replace_a_frozen_transitional_exception(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["frozen_transitional_workspace_edges"][0]["target"] = "casars"
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"frozen_transitional_workspace_edges differs from the accepted exceptions",
        ):
            checker.validate_policy(policy)

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

    def test_workspace_check_rejects_an_unclassified_workspace_package(self) -> None:
        metadata = metadata_for_policy(self.policy)
        metadata["packages"].append(
            {
                "name": "new-imaging-crate",
                "manifest_path": "/workspace/crates/new-imaging-crate/Cargo.toml",
                "dependencies": [],
            }
        )
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"workspace package classification differs from Cargo metadata",
        ):
            checker.validate_workspace(self.policy, metadata)

    def test_real_workspace_package_cannot_become_unmapped(self) -> None:
        metadata = metadata_for_policy(self.policy)
        policy = copy.deepcopy(self.policy)
        del policy["package_layers"]["casars-python"]
        del policy["workspace_package_classification"]["casars-python"]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"workspace package classification differs from Cargo metadata: added=\['casars-python'\]",
        ):
            checker.validate_workspace(policy, metadata)

    def test_real_frontend_package_rejects_a_direct_backend_edge(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["package_layers"]["synthetic-backend"] = "backend"
        policy.setdefault("workspace_package_classification", {})[
            "synthetic-backend"
        ] = "native"
        policy["native_package_workspace_dependencies"]["synthetic-backend"] = []
        checker.validate_policy(policy, enforce_accepted_scope=False)
        metadata = metadata_for_policy(policy)
        if not any(value["name"] == "casars-python" for value in metadata["packages"]):
            metadata["packages"].append(
                {
                    "name": "casars-python",
                    "manifest_path": "/workspace/crates/casars-python/Cargo.toml",
                    "dependencies": [],
                }
            )
        package(metadata, "casars-python")["dependencies"].append(
            dependency("synthetic-backend")
        )
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"forbidden logical imaging edge: frontend -> backend",
        ):
            checker.validate_workspace(policy, metadata)

    def test_frontend_services_rejects_a_direct_execution_edge(self) -> None:
        metadata = metadata_for_policy(self.policy)
        package(metadata, "casars-frontend-services")["dependencies"].append(
            dependency("casa-imaging-runtime")
        )
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"forbidden logical imaging edge: frontend -> execution",
        ):
            checker.validate_workspace(self.policy, metadata)

    def test_rust_science_module_rejects_an_execution_import(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "crates/casa-imaging-model/src"
            source.mkdir(parents=True)
            (source / "lib.rs").write_text(
                "use casa_imaging_runtime::ResourceAuthority;\n",
                encoding="utf-8",
            )
            policy = copy.deepcopy(self.policy)
            policy["source_boundaries"] = [
                boundary
                for boundary in policy.get("source_boundaries", [])
                if boundary.get("id") == "native-science-rust"
            ]
            with self.assertRaisesRegex(
                checker.ArchitectureError,
                r"native science Rust source imports an execution, backend, legacy, or device API",
            ):
                checker.validate_source_boundaries(policy, root)

    def test_swift_frontend_rejects_device_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "apps/casars-mac/Sources/CasarsMacApp"
            source.mkdir(parents=True)
            (source / "View.swift").write_text(
                "import Metal\nlet device = MTLCreateSystemDefaultDevice()\n",
                encoding="utf-8",
            )
            policy = copy.deepcopy(self.policy)
            policy["source_boundaries"] = [
                boundary
                for boundary in policy.get("source_boundaries", [])
                if boundary.get("id") == "swift-imaging-frontends"
            ]
            policy["source_boundaries"][0]["roots"] = [
                "apps/casars-mac/Sources/CasarsMacApp"
            ]
            with self.assertRaisesRegex(
                checker.ArchitectureError,
                r"Swift frontend inspects a device or selects an imaging backend",
            ):
                checker.validate_source_boundaries(policy, root)

    def test_transitional_frontend_boundary_rejects_a_new_backend_reference(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "frontend"
            source.mkdir()
            boundary = {
                "id": "synthetic-transitional-frontend",
                "roots": ["frontend"],
                "extensions": [".rs"],
                "forbidden_patterns": [
                    {
                        "regex": r"\bcasa_imaging::",
                        "message": "frontend imports legacy imaging",
                    }
                ],
                "accepted_violation_digest": checker.stable_digest([]),
            }
            policy = {"source_boundaries": [boundary]}
            checker.validate_source_boundaries(policy, root)
            (source / "lib.rs").write_text(
                "use casa_imaging::StandardMfsBackend;\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(
                checker.ArchitectureError,
                r"differs from its accepted transitional violations",
            ):
                checker.validate_source_boundaries(policy, root)

    def test_transitional_frontend_boundary_rejects_forbidden_symbol_replacement(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "frontend"
            source.mkdir()
            module = source / "lib.rs"
            module.write_text("use casa_imaging::LegacyBackend;\n", encoding="utf-8")
            boundary = {
                "id": "synthetic-transitional-frontend",
                "roots": ["frontend"],
                "extensions": [".rs"],
                "forbidden_patterns": [
                    {
                        "regex": r"\bcasa_imaging::",
                        "message": "frontend imports legacy imaging",
                    }
                ],
                "accepted_violation_digest": None,
            }
            boundary["accepted_violation_digest"] = (
                checker.source_boundary_violation_digest(
                    checker.source_boundary_violations(boundary, root)
                )
            )
            policy = {"source_boundaries": [boundary]}
            checker.validate_source_boundaries(policy, root)

            module.write_text("use casa_imaging::DifferentBackend;\n", encoding="utf-8")
            with self.assertRaisesRegex(
                checker.ArchitectureError,
                r"differs from its accepted transitional violations",
            ):
                checker.validate_source_boundaries(policy, root)

    def test_transitional_frontend_boundary_rejects_unqualified_backend_selection(
        self,
    ) -> None:
        selectors = [
            ("StandardMfsBackend", "Cpu", "Metal"),
            ("StandardMfsMinorCycleBackend", "Cpu", "Metal"),
            ("SinglePlaneAccelerationPolicy", "Cpu", "Metal"),
            ("PerPlaneExecutionBackend", "SerialCpu", "Wave3MetalGrouped"),
        ]
        production_boundary = next(
            boundary
            for boundary in self.policy["source_boundaries"]
            if boundary["id"] == "rust-imaging-frontends"
        )
        for selector, baseline, added in selectors:
            with (
                self.subTest(selector=selector),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                source = root / "frontend"
                source.mkdir()
                module = source / "lib.rs"
                module.write_text(
                    f"let selected = {selector}::{baseline};\n", encoding="utf-8"
                )
                boundary = copy.deepcopy(production_boundary)
                boundary["roots"] = ["frontend"]
                boundary["accepted_violation_digest"] = (
                    checker.source_boundary_violation_digest(
                        checker.source_boundary_violations(boundary, root)
                    )
                )
                policy = {"source_boundaries": [boundary]}
                checker.validate_source_boundaries(policy, root)

                module.write_text(
                    f"let selected = {selector}::{baseline};\n"
                    f"let extra = {selector}::{added};\n",
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(
                    checker.ArchitectureError,
                    r"differs from its accepted transitional violations",
                ):
                    checker.validate_source_boundaries(policy, root)


class MigrationMatrixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = load_policy()
        self.matrix = checker.synthetic_matrix(self.policy)

    def test_valid_synthetic_matrix_passes(self) -> None:
        checker.validate_migration_matrix(
            self.matrix, self.policy, enforce_accepted_scope=False
        )

    def test_cli_requires_the_declared_migration_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing-migration-matrix.json"
            result = subprocess.run(
                [
                    sys.executable,
                    str(CHECKER_PATH),
                    "--migration-matrix",
                    str(missing),
                ],
                cwd=REPO_ROOT,
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("required migration matrix is missing", result.stderr)

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
        mutations.append(
            ("obligation", missing_obligation, "must be a non-empty object")
        )
        missing_crosswalk = copy.deepcopy(self.matrix)
        for row in missing_crosswalk["rows"]:
            row["evidence_issues"] = [488]
        mutations.append(
            ("crosswalk", missing_crosswalk, "omits required crosswalk issues")
        )
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
                    checker.validate_migration_matrix(
                        mutation, self.policy, enforce_accepted_scope=False
                    )

    def test_live_inventory_rejects_a_missing_row(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        matrix["rows"].pop()
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"inventory and rows differ",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_live_inventory_rejects_a_coordinated_row_and_inventory_rename(
        self,
    ) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        old_identifier = matrix["rows"][0]["id"]
        new_identifier = "capability.renamed-continuum"
        matrix["rows"][0]["id"] = new_identifier
        inventory = matrix["inventory"]["capability"]
        inventory[inventory.index(old_identifier)] = new_identifier
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"inventory differs from the canonical imaging inventory",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_live_inventory_rejects_a_coordinated_row_and_inventory_delete(
        self,
    ) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        identifier = "product.alpha-pbcor"
        matrix["rows"] = [row for row in matrix["rows"] if row["id"] != identifier]
        matrix["inventory"]["product"].remove(identifier)
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"inventory differs from the canonical imaging inventory",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_baseline_evidence_digest_must_match_repository_content(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        locator = next(iter(matrix["baseline_manifest_digests"]))
        matrix["baseline_manifest_digests"][locator] = "0" * 64
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"content digest differs",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_mutable_external_baseline_locator_is_rejected(self) -> None:
        matrix = copy.deepcopy(self.matrix)
        matrix["rows"][0]["baseline_manifests"] = [
            "github-issue://bglenden/casa-rs/487"
        ]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"must reference a content-pinned baseline manifest",
        ):
            checker.validate_migration_matrix(
                matrix, self.policy, enforce_accepted_scope=False
            )

    def test_cube_interpolation_inventory_cannot_drop_a_current_variant(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        del matrix["cube_interpolation_inventory"]["Linear"]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"differs from CubeInterpolation",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_standard_mfs_backend_inventory_cannot_alias_a_distinct_family(
        self,
    ) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        matrix["standard_mfs_backend_inventory"]["MetalRowRun"] = (
            "backend.metal-gridder"
        )
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"differs from StandardMfsBackend",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_public_request_solver_and_backend_enum_variants_are_bound(self) -> None:
        cases = [
            ("spectral_mode_inventory", "Cube", "SpectralMode"),
            ("gridder_request_inventory", "Mosaic", "GridderRequest"),
            ("deconvolver_inventory", "Clark", "Deconvolver"),
            ("fft_backend_choice_inventory", "Auto", "FftBackendChoice"),
            (
                "imager_spectral_mode_inventory",
                "Cubedata",
                "ImagerSpectralMode",
            ),
            (
                "imager_deconvolver_inventory",
                "Multiscale",
                "ImagerDeconvolver",
            ),
            (
                "imager_cube_interpolation_inventory",
                "Nearest",
                "ImagerCubeInterpolation",
            ),
            (
                "imaging_fft_backend_policy_inventory",
                "Auto",
                "ImagingFftBackendPolicy",
            ),
            (
                "standard_mfs_acceleration_policy_inventory",
                "Metal",
                "StandardMfsAccelerationPolicy",
            ),
            (
                "standard_mfs_minor_cycle_backend_inventory",
                "Metal",
                "StandardMfsMinorCycleBackend",
            ),
            (
                "single_plane_acceleration_policy_inventory",
                "MultiCpu",
                "SinglePlaneAccelerationPolicy",
            ),
            (
                "per_plane_execution_backend_inventory",
                "Wave3MetalGrouped",
                "PerPlaneExecutionBackend",
            ),
        ]
        for field, variant, enum_name in cases:
            with self.subTest(field=field, variant=variant):
                matrix = checker.load_object(MATRIX_PATH, "migration matrix")
                del matrix[field][variant]
                with self.assertRaisesRegex(
                    checker.ArchitectureError,
                    rf"differs from {enum_name}",
                ):
                    checker.validate_migration_matrix(matrix, self.policy)

    def test_product_kind_inventory_cannot_drop_a_current_variant(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        del matrix["product_kind_inventory"]["Sensitivity"]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"product_kind_inventory differs from ProductKind",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_plane_inventory_cannot_drop_a_raw_correlation(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        del matrix["plane_selection_inventory"]["CorrLL"]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"plane_selection_inventory differs from ImagerPlaneSelection",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_polarization_inventory_cannot_drop_a_cross_hand(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        del matrix["polarization_coordinate_inventory"]["LinearXy"]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"polarization_coordinate_inventory differs from PolarizationCoordinate",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_required_issue_cannot_be_deleted_from_policy_matrix_and_rows(self) -> None:
        policy = copy.deepcopy(self.policy)
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        removed_issue = 54
        policy["required_migration_evidence_issues"].remove(removed_issue)
        matrix["required_issue_crosswalk"].remove(removed_issue)
        matrix["issue_outcomes"] = [
            outcome
            for outcome in matrix.get("issue_outcomes", [])
            if outcome.get("issue") != removed_issue
        ]
        for row in matrix["rows"]:
            row["evidence_issues"] = [
                issue for issue in row["evidence_issues"] if issue != removed_issue
            ]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"required_migration_evidence_issues differs from the accepted issue scope",
        ):
            checker.validate_policy(policy)
            checker.validate_migration_matrix(matrix, policy)

    def test_issue_outcome_cannot_be_weakened_in_place(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        matrix["issue_outcomes"][0]["acceptance_gates"] = ["some evidence"]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"issue_outcomes content differs from the accepted crosswalk outcomes",
        ):
            checker.validate_migration_matrix(matrix, self.policy)

    def test_every_acceptance_law_threshold_and_gate_is_immutable(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        for contract_id, contract in matrix["acceptance_contracts"].items():
            for field in ("laws", "thresholds", "resource_gates"):
                for index in range(len(contract[field])):
                    with self.subTest(contract=contract_id, field=field, index=index):
                        mutation = copy.deepcopy(matrix)
                        mutation["acceptance_contracts"][contract_id][field][index] = (
                            "weakened requirement"
                        )
                        with self.assertRaisesRegex(
                            checker.ArchitectureError,
                            rf"acceptance contract {contract_id}\.{field} differs from the accepted scope",
                        ):
                            checker.validate_migration_matrix(mutation, self.policy)

    def test_acceptance_baseline_comparator_and_evidence_are_immutable(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        mutations = []
        baseline = copy.deepcopy(matrix)
        baseline["acceptance_contracts"]["scientific-products-v1"][
            "baseline_identity"
        ] = "discard the baseline"
        mutations.append(baseline)
        comparator = copy.deepcopy(matrix)
        comparator["acceptance_contracts"]["scientific-products-v1"]["comparator"][
            "preprocessing"
        ] = "discard all samples"
        mutations.append(comparator)
        evidence = copy.deepcopy(matrix)
        evidence["acceptance_contracts"]["scientific-products-v1"]["evidence_tiers"] = [
            "smoke only"
        ]
        mutations.append(evidence)
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                with self.assertRaisesRegex(
                    checker.ArchitectureError,
                    r"acceptance contract content differs from the accepted scope",
                ):
                    checker.validate_migration_matrix(mutation, self.policy)

    def test_live_row_cannot_be_silently_reclassified_native(self) -> None:
        matrix = checker.load_object(MATRIX_PATH, "migration matrix")
        row = next(row for row in matrix["rows"] if row["id"] == "product.psf")
        row["status"] = "Native"
        row["current_owner"] = "arbitrary owner"
        row["transfer_point"] = "claimed without evidence"
        row["deletion_condition"] = "ignored"
        row["migration_obligation"] = None
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"row ledger differs from the accepted scope",
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
        missing = "repo://tools/perf/imager/evidence/missing.json"
        matrix["baseline_manifest_digests"][missing] = "0" * 64
        matrix["rows"][0]["baseline_manifests"] = [missing]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"cannot read baseline manifest",
        ):
            checker.validate_migration_matrix(matrix, self.policy)


if __name__ == "__main__":
    unittest.main()
