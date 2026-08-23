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

    def assert_rustc_accepts(
        self, source_text: str, extra_sources: dict[str, str] | None = None
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            probe = root / "rustc-probe.rs"
            probe.write_text(source_text, encoding="utf-8")
            for relative, contents in (extra_sources or {}).items():
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(contents, encoding="utf-8")
            result = subprocess.run(
                [
                    "rustc",
                    "--crate-name",
                    "t15_policy_probe",
                    "--crate-type",
                    "lib",
                    "--edition",
                    "2024",
                    "--emit",
                    "metadata",
                    "-o",
                    str(root / "probe.rmeta"),
                    str(probe),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

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

    def test_t15_allowlist_cannot_authorize_a_new_item_without_review(self) -> None:
        policy = copy.deepcopy(self.policy)
        boundary = next(
            value
            for value in policy["source_boundaries"]
            if value["id"] == "t15-private-walking-skeleton"
        )
        boundary["rust_allowlist"]["allowed_items"]["fn:synthetic_runner"] = 1
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"source boundaries differ from the accepted policy",
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
            "casa-imaging-model",
            "casa-ms",
        ]
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"workspace package layers, classifications, native dependencies, or device policy differ",
        ):
            checker.validate_policy(policy)

    def test_whole_run_router_owner_is_pinned(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["whole_run_router"]["package"] = "casa-imaging-runtime"
        with self.assertRaisesRegex(
            checker.ArchitectureError,
            r"whole-run migration router differs from the accepted owner",
        ):
            checker.validate_policy(policy)

    def test_whole_run_engine_ports_have_one_source_owner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / self.policy["whole_run_router"]["source"]
            source.parent.mkdir(parents=True)
            source.write_text(
                'const MATRIX: &str = include_str!("migration-matrix.json");\n'
                "pub struct ImagingRouter;\n"
                "impl ImagingRouter {\n"
                "    pub fn dispatch(&self) {}\n"
                "}\n"
                "pub struct NativeEnginePort;\n"
                "pub struct LegacyWholeRunEnginePort;\n",
                encoding="utf-8",
            )
            checker.validate_whole_run_router_source(self.policy, root)

            duplicate = root / "crates/duplicate/src/lib.rs"
            duplicate.parent.mkdir(parents=True)
            duplicate.write_text("pub struct NativeEnginePort;\n", encoding="utf-8")
            with self.assertRaisesRegex(
                checker.ArchitectureError,
                r"whole-run router symbol NativeEnginePort must be owned exactly once",
            ):
                checker.validate_whole_run_router_source(self.policy, root)

    def test_whole_run_router_must_embed_the_authoritative_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / self.policy["whole_run_router"]["source"]
            source.parent.mkdir(parents=True)
            source.write_text(
                "pub struct ImagingRouter;\n"
                "impl ImagingRouter {\n"
                "    pub fn dispatch(&self) {}\n"
                "}\n"
                "pub struct NativeEnginePort;\n"
                "pub struct LegacyWholeRunEnginePort;\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                checker.ArchitectureError,
                r"must embed the authoritative migration matrix",
            ):
                checker.validate_whole_run_router_source(self.policy, root)

    def test_native_runtime_and_router_reject_legacy_imports(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / "crates/casa-imaging-runtime/src"
            router = root / "crates/casa-imaging-router/src"
            runtime.mkdir(parents=True)
            router.mkdir(parents=True)
            (runtime / "lib.rs").write_text("", encoding="utf-8")
            (router / "lib.rs").write_text(
                "use casa_imaging::LegacyBackend;\n", encoding="utf-8"
            )
            boundary = next(
                value
                for value in self.policy["source_boundaries"]
                if value["id"] == "native-runtime-router-legacy-isolation"
            )
            policy = {"source_boundaries": [boundary]}
            with self.assertRaisesRegex(
                checker.ArchitectureError,
                r"native runtime or whole-run router imports a legacy imaging API",
            ):
                checker.validate_source_boundaries(policy, root)

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

    def test_t15_walking_skeleton_boundary_rejects_authority_leaks(self) -> None:
        boundary = next(
            (
                value
                for value in self.policy["source_boundaries"]
                if value["id"] == "t15-private-walking-skeleton"
            ),
            None,
        )
        self.assertIsNotNone(boundary, "T15 must own a test-only source boundary")
        assert boundary is not None
        for source, message in [
            (
                "pub struct SyntheticCompletionAuthority;\n",
                r"T15 walking skeleton must remain private test code",
            ),
            (
                "use casa_imaging_router::ImagingRouter;\n",
                r"T15 walking skeleton imports outside its exact allowlist",
            ),
            (
                "use super::*;\n",
                r"T15 walking skeleton must not use glob imports",
            ),
            (
                "struct SelectedObservationCompletion;\n",
                r"T15 walking skeleton declares an item outside its exact allowlist",
            ),
            (
                "struct ExecutionScheduler;\n",
                r"T15 walking skeleton declares an item outside its exact allowlist",
            ),
            (
                "struct FutureWeightingResult;\n",
                r"T15 walking skeleton declares an item outside its exact allowlist",
            ),
            (
                "fn synthetic_runner() {}\n",
                r"T15 walking skeleton declares an item outside its exact allowlist",
            ),
        ]:
            with (
                self.subTest(source=source),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                fixture = root / boundary["roots"][0]
                if fixture.suffix != ".rs":
                    fixture /= "walking_skeleton.rs"
                fixture.parent.mkdir(parents=True)
                fixture.write_text(source, encoding="utf-8")
                with self.assertRaisesRegex(checker.ArchitectureError, message):
                    checker.validate_source_boundaries(
                        {"source_boundaries": [boundary]}, root
                    )

    def test_t15_walking_skeleton_rejects_every_public_rust_item_form(self) -> None:
        boundary = next(
            value
            for value in self.policy["source_boundaries"]
            if value["id"] == "t15-private-walking-skeleton"
        )
        public_items = [
            "pub(crate) async fn leaked_async() {}\n",
            "pub union LeakedUnion { value: u64 }\n",
            "pub const LEAKED_CONST: usize = 0;\n",
            "pub static LEAKED_STATIC: usize = 0;\n",
            "pub static mut LEAKED_MUT_STATIC: usize = 0;\n",
            "pub(in crate) type LeakedType = usize;\n",
            'pub(crate) unsafe extern "C" fn leaked_ffi() {}\n',
            "pub struct LeakedStruct;\n",
            "pub enum LeakedEnum {}\n",
            "pub trait LeakedTrait {}\n",
            "pub mod leaked_module {}\n",
            "pub use super::private_item;\n",
            "pub /* commented visibility */ fn leaked_commented() {}\n",
            "pub // line-commented visibility\nfn leaked_line_commented() {}\n",
            "pub(\n    in crate\n) fn leaked_multiline() {}\n",
            'unsafe extern "C" {\n    pub safe fn leaked_safe_foreign();\n}\n',
            "#[macro_export]\nmacro_rules! leaked_macro_rules { () => {}; }\n",
            "#[cfg_attr(any(), macro_export)]\nmacro_rules! conditionally_exported { () => {}; }\n",
        ]
        for source_text in public_items:
            with (
                self.subTest(source=source_text),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                fixture = root / boundary["roots"][0]
                if fixture.suffix != ".rs":
                    fixture /= "walking_skeleton.rs"
                fixture.parent.mkdir(parents=True)
                fixture.write_text(source_text, encoding="utf-8")
                with self.assertRaisesRegex(
                    checker.ArchitectureError,
                    r"T15 walking skeleton must remain private test code",
                ):
                    checker.validate_source_boundaries(
                        {"source_boundaries": [boundary]}, root
                    )

    def test_t15_boundaries_reject_rustc_valid_include_authority_injection(
        self,
    ) -> None:
        payload = (
            "pub struct IncludedPublicAuthority;\n"
            "struct FutureWeightingResult;\n"
            "fn synthetic_runner() {}\n"
        )
        for identifier, fixture_name, invocation in [
            (
                "t15-private-walking-skeleton",
                "walking_skeleton.rs",
                'include!("injected.rs");\n',
            ),
            (
                "t15-private-walking-skeleton",
                "walking_skeleton.rs",
                'r#include!("injected.rs");\n',
            ),
            (
                "t15-private-parent-support",
                None,
                'std::include! { "injected.rs" }\n',
            ),
        ]:
            boundary = next(
                value
                for value in self.policy["source_boundaries"]
                if value["id"] == identifier
            )
            with (
                self.subTest(boundary=identifier),
                tempfile.TemporaryDirectory() as directory,
            ):
                self.assert_rustc_accepts(invocation, {"injected.rs": payload})
                root = Path(directory)
                fixture = root / boundary["roots"][0]
                canonical = REPO_ROOT / boundary["roots"][0]
                if fixture_name is not None and fixture.suffix != ".rs":
                    fixture /= fixture_name
                    canonical /= fixture_name
                fixture.parent.mkdir(parents=True, exist_ok=True)
                fixture.write_text(
                    canonical.read_text(encoding="utf-8")
                    + "\n"
                    + invocation.replace("injected.rs", "../../../../injected.rs"),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(
                    checker.ArchitectureError,
                    r"must not include or redirect Rust source",
                ):
                    checker.validate_source_boundaries(
                        {"source_boundaries": [boundary]}, root
                    )

    def test_t15_boundaries_reject_rustc_valid_path_redirects(self) -> None:
        payload = (
            "pub struct RedirectedPublicAuthority;\n"
            "pub struct FutureWeightingResult;\n"
            "pub fn synthetic_runner() {}\n"
        )
        for identifier, attribute in [
            ("t15-private-walking-skeleton", "path"),
            ("t15-private-walking-skeleton", "r#path"),
            ("t15-private-parent-support", "cfg_attr(all(), r#path"),
        ]:
            boundary = next(
                value
                for value in self.policy["source_boundaries"]
                if value["id"] == identifier
            )
            with (
                self.subTest(boundary=identifier),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                if attribute.startswith("cfg_attr"):
                    probe_attribute = f'#[{attribute} = "injected.rs")]\n'
                else:
                    probe_attribute = f'#[{attribute} = "injected.rs"]\n'
                self.assert_rustc_accepts(
                    probe_attribute + "mod redirected;\n",
                    {"injected.rs": payload},
                )
                fixture = root / boundary["roots"][0]
                if fixture.suffix != ".rs":
                    fixture /= "walking_skeleton.rs"
                fixture.parent.mkdir(parents=True, exist_ok=True)
                redirected_attribute = probe_attribute.replace(
                    "injected.rs", "../../../../injected.rs"
                )
                fixture.write_text(
                    redirected_attribute + "mod redirected;\n",
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(
                    checker.ArchitectureError,
                    r"must not include or redirect Rust source",
                ):
                    checker.validate_source_boundaries(
                        {"source_boundaries": [boundary]}, root
                    )

    def test_t15_boundaries_reject_rustc_valid_raw_macro_export_attributes(
        self,
    ) -> None:
        for source_text in [
            "#[r#macro_export]\nmacro_rules! leaked_macro { () => {}; }\n",
            (
                "#[cfg_attr(all(), r#macro_export)]\n"
                "macro_rules! conditionally_leaked_macro { () => {}; }\n"
            ),
        ]:
            self.assert_rustc_accepts(source_text)
            for identifier, label in [
                ("t15-private-walking-skeleton", "T15 walking skeleton"),
                (
                    "t15-private-parent-support",
                    "T15 parent compile_plan_run support",
                ),
            ]:
                boundary = next(
                    value
                    for value in self.policy["source_boundaries"]
                    if value["id"] == identifier
                )
                with (
                    self.subTest(boundary=identifier, source=source_text),
                    tempfile.TemporaryDirectory() as directory,
                ):
                    root = Path(directory)
                    fixture = root / boundary["roots"][0]
                    if fixture.suffix != ".rs":
                        fixture /= "walking_skeleton.rs"
                    fixture.parent.mkdir(parents=True, exist_ok=True)
                    fixture.write_text(source_text, encoding="utf-8")
                    with self.assertRaisesRegex(
                        checker.ArchitectureError,
                        rf"{label} must remain private test code",
                    ):
                        checker.validate_source_boundaries(
                            {"source_boundaries": [boundary]}, root
                        )

    def test_t15_rust_lexer_ignores_policy_words_in_comments_and_literals(self) -> None:
        source = r"""
fn private_probe() {
    let _ = "pub fn string_only() {}";
    let _ = r#"#[macro_export] macro_rules! string_only { () => {}; }"#;
    let _ = "casa_imaging_router::StringOnly";
    let _: &'static str = "lifetime";
}
// pub fn comment_only() {}
/* outer #[macro_export] /* nested pub struct CommentOnly; */ */
"""
        imports, paths, items, privacy, globs = checker.rust_source_inventory(
            source, "synthetic.rs"
        )
        self.assertEqual(imports, [])
        self.assertEqual(paths, [])
        self.assertEqual(items, [("fn:private_probe", 2)])
        self.assertEqual(privacy, [])
        self.assertEqual(globs, [])

    def test_t15_walking_skeleton_rejects_every_glob_import_origin(self) -> None:
        boundary = next(
            value
            for value in self.policy["source_boundaries"]
            if value["id"] == "t15-private-walking-skeleton"
        )
        for source_text in [
            "use super::*;\n",
            "use crate::*;\n",
            "use casa_imaging_runtime::*;\n",
            "use casa_imaging_runtime::{ArtifactIdentity, *};\n",
        ]:
            with (
                self.subTest(source=source_text),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                fixture = root / boundary["roots"][0]
                if fixture.suffix != ".rs":
                    fixture /= "walking_skeleton.rs"
                fixture.parent.mkdir(parents=True)
                fixture.write_text(source_text, encoding="utf-8")
                with self.assertRaisesRegex(
                    checker.ArchitectureError,
                    r"T15 walking skeleton must not use glob imports",
                ):
                    checker.validate_source_boundaries(
                        {"source_boundaries": [boundary]}, root
                    )

    def test_t15_boundaries_reject_every_unallowlisted_private_item_form(self) -> None:
        private_items = [
            "struct RenamedStruct;\n",
            "enum RenamedEnum {}\n",
            "union RenamedUnion { value: u64 }\n",
            "trait RenamedTrait {}\n",
            "type RenamedType = usize;\n",
            "fn renamed_function() {}\n",
            "const RENAMED_CONST: usize = 0;\n",
            "static RENAMED_STATIC: usize = 0;\n",
            "mod renamed_module {}\n",
            "macro_rules! renamed_macro { () => {}; }\n",
        ]
        for identifier, label in [
            ("t15-private-walking-skeleton", "T15 walking skeleton"),
            ("t15-private-parent-support", "T15 parent compile_plan_run support"),
        ]:
            boundary = next(
                value
                for value in self.policy["source_boundaries"]
                if value["id"] == identifier
            )
            for source_text in private_items:
                with (
                    self.subTest(boundary=identifier, source=source_text),
                    tempfile.TemporaryDirectory() as directory,
                ):
                    root = Path(directory)
                    fixture = root / boundary["roots"][0]
                    if fixture.suffix != ".rs":
                        fixture /= "walking_skeleton.rs"
                    fixture.parent.mkdir(parents=True)
                    fixture.write_text(source_text, encoding="utf-8")
                    with self.assertRaisesRegex(
                        checker.ArchitectureError,
                        rf"{label} declares an item outside its exact allowlist",
                    ):
                        checker.validate_source_boundaries(
                            {"source_boundaries": [boundary]}, root
                        )

    def test_t15_boundaries_reject_unallowlisted_qualified_casa_paths(self) -> None:
        for identifier, fixture_name, source_text, label in [
            (
                "t15-private-walking-skeleton",
                "walking_skeleton.rs",
                "fn walking_skeleton() {\n"
                "    let _ = casa_imaging_runtime::FutureWeightingResult;\n"
                "}\n",
                "T15 walking skeleton",
            ),
            (
                "t15-private-parent-support",
                None,
                "fn product_validity() {\n"
                "    let _ = r#casa_imaging_router::RenamedRouter;\n"
                "}\n",
                "T15 parent compile_plan_run support",
            ),
        ]:
            boundary = next(
                value
                for value in self.policy["source_boundaries"]
                if value["id"] == identifier
            )
            with (
                self.subTest(boundary=identifier),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                fixture = root / boundary["roots"][0]
                if fixture_name is not None and fixture.suffix != ".rs":
                    fixture /= fixture_name
                fixture.parent.mkdir(parents=True)
                fixture.write_text(source_text, encoding="utf-8")
                with self.assertRaisesRegex(
                    checker.ArchitectureError,
                    rf"{label} references a qualified CASA path outside its exact allowlist",
                ):
                    checker.validate_source_boundaries(
                        {"source_boundaries": [boundary]}, root
                    )

    def test_t15_parent_support_boundary_rejects_public_items_and_authority_aliases(
        self,
    ) -> None:
        boundary = next(
            value
            for value in self.policy["source_boundaries"]
            if value["id"] == "t15-private-parent-support"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / boundary["roots"][0]
            source.parent.mkdir(parents=True)
            source.write_text("", encoding="utf-8")
            with self.assertRaisesRegex(
                checker.ArchitectureError,
                r"T15 parent compile_plan_run support no longer matches its accepted import and item inventory",
            ):
                checker.validate_source_boundaries(
                    {"source_boundaries": [boundary]}, root
                )
            for source_text, message in [
                (
                    "pub(crate) fn leaked_support() {}\n",
                    r"T15 parent compile_plan_run support must remain private test code",
                ),
                (
                    "use casa_imaging_router::ImagingRouter;\n",
                    r"T15 parent compile_plan_run support imports outside its exact allowlist",
                ),
                (
                    "struct SelectedObservationCompletion;\n",
                    r"T15 parent compile_plan_run support declares an item outside its exact allowlist",
                ),
                (
                    "struct ExecutionScheduler;\n",
                    r"T15 parent compile_plan_run support declares an item outside its exact allowlist",
                ),
                (
                    "struct FutureWeightingResult;\n",
                    r"T15 parent compile_plan_run support declares an item outside its exact allowlist",
                ),
                (
                    "fn synthetic_runner() {}\n",
                    r"T15 parent compile_plan_run support declares an item outside its exact allowlist",
                ),
                (
                    "use casa_imaging_runtime::*;\n",
                    r"T15 parent compile_plan_run support must not use glob imports",
                ),
            ]:
                with self.subTest(source=source_text):
                    source.write_text(source_text, encoding="utf-8")
                    with self.assertRaisesRegex(checker.ArchitectureError, message):
                        checker.validate_source_boundaries(
                            {"source_boundaries": [boundary]}, root
                        )

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
