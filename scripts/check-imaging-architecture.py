#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Enforce the ADR-0009 imaging ownership and dependency graph."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = REPO_ROOT / "resources/imaging-architecture/dependency-policy.json"
VALID_STATUSES = {"Native", "TemporarilyUnavailable"}
MATRIX_KINDS = {"capability", "product", "solver", "frontend", "backend"}
LOCATOR_KEYS = {"commit", "issue", "locator", "path", "receipt", "token", "url"}
PACKAGE_CLASSIFICATIONS = {"native", "surface", "support"}
ACCEPTED_MIGRATION_ISSUES = frozenset(
    {
        35,
        40,
        42,
        45,
        52,
        54,
        55,
        217,
        319,
        445,
        446,
        447,
        448,
        449,
        450,
        462,
        466,
        473,
        478,
    }
)
# Scientific acceptance records retain explicit ratchets. Workspace ownership
# is enforced directly from the small readable policy below, not by opaque
# digests or frozen exception inventories.
ACCEPTED_ISSUE_OUTCOMES_SHA256 = (
    "1d2a77232fdc25a50053097b644b64cbdf0d21e1970590ec4180de6dce29738d"
)
ACCEPTED_ACCEPTANCE_CONTRACTS_SHA256 = (
    "daafa560c0e941fb3f2cea5c02a46de8a3363c2dd327cb839ef8ab2111f09835"
)
ACCEPTED_MATRIX_ROWS_SHA256 = (
    "f0eb3a56db6bec14871eaf2d2f99dda98c2f3623002fcb2745f5f659ed17a0e7"
)
ACCEPTED_BASELINE_MANIFEST_DIGESTS_SHA256 = (
    "e0ab31d0d764e866933a91cd9baee65b649d9a97459a5a8a44d2bc1706a77b55"
)
ACCEPTED_MATRIX_CONTRACT_REVISION = 55
ACCEPTED_CONTRACT_REQUIREMENT_SHA256 = {
    (
        "scientific-products-v1",
        "thresholds",
    ): "5901406c1962290ee48d0469215648626d533eb570a0102ff897b4be7f6617f2",
    (
        "scientific-products-v1",
        "laws",
    ): "6745a2f81613e17391855ab4852e0a96dddb1eeabc2e8a774f40c999a01bd5f2",
    (
        "scientific-products-v1",
        "resource_gates",
    ): "94265d172ea8f06b33f6fb7e12b20950f2c155d76f7ba9b65fadedcc2d21f063",
    (
        "exact-routing-v1",
        "thresholds",
    ): "1b43e32c11597e61403d4fc5ede4096d56757a205a9b10195a3c5ae9fbf56dda",
    (
        "exact-routing-v1",
        "laws",
    ): "3199d2c99bfbe9c54fd69fc38b35edefecbacebea4ad64bd0570de1aa1d1a471",
    (
        "exact-routing-v1",
        "resource_gates",
    ): "eb78168712b4c76af00e3370b5c77ab42093abf7facbbd39dd03d9927bdb54c9",
    (
        "solver-trajectory-v1",
        "thresholds",
    ): "fae7cf28a7684a5ce99918d9dda32a801858937f0010a19482745e3f5429348b",
    (
        "solver-trajectory-v1",
        "laws",
    ): "355f98e0a49fa7aed996fa1b3901dbdeddcce9a63a53739661384b7e9cf9343e",
    (
        "solver-trajectory-v1",
        "resource_gates",
    ): "734da223aa029439cfe88bbd8262317f72ab4f9ef74944837bd90e4ba5367879",
    (
        "surface-roundtrip-v1",
        "thresholds",
    ): "ecb9972ac8b934c2b9899134f520becf2b1f455c4be7fb240ef4d428c69662b6",
    (
        "surface-roundtrip-v1",
        "laws",
    ): "c556b7560741297e5a3ac6fde951586f7b52cfd990d237d7c0e05f5f261fd510",
    (
        "surface-roundtrip-v1",
        "resource_gates",
    ): "0299b84af3b12db3e0606ca191c15b731d46cb1ebd952de7c3e49869200e0301",
    (
        "resource-authority-v1",
        "thresholds",
    ): "dc6fe75624085924abbf6e68b848b137031561589fa319213454dce096222d4e",
    (
        "resource-authority-v1",
        "laws",
    ): "717747298f9e62c82e138de22271f887144ac8b3f2df817db8c25eb28fec044d",
    (
        "resource-authority-v1",
        "resource_gates",
    ): "6d8f3b420bf813fe9707607bc4cdd7ddf351a13a94b4d11e87e56ae39e1eece3",
    (
        "compiled-problem-foundation-v1",
        "thresholds",
    ): "563fc508ff7f1c835fb5388883d4175ddb04dafcceb5b3958f6a0ea3722aa1da",
    (
        "compiled-problem-foundation-v1",
        "laws",
    ): "032d42173b6ab94ae4a48104fbc23bb25683cd9e5a4ace84f7605d914f3795b3",
    (
        "compiled-problem-foundation-v1",
        "resource_gates",
    ): "64c0411b3e62a900072e12f74facbe311a718504883e650a5cc205c9d5b4bd25",
    (
        "model-lifecycle-foundation-v1",
        "thresholds",
    ): "cba7c38e1701f7405003bce8522d2f7acb469c9eec2a2ec21456cd06a3a91aaf",
    (
        "model-lifecycle-foundation-v1",
        "laws",
    ): "320cc7ea3d4f195ff7e43e1c15922ce4c5029a666d5cdbb06cf93bdde3558b58",
    (
        "model-lifecycle-foundation-v1",
        "resource_gates",
    ): "b77d11665914730df97d1aee20b7db613ae2cd285f96a4987cdc79ab1759accc",
    (
        "global-weighting-v1",
        "thresholds",
    ): "ba44ae208fff6e3e499a419dcac95736dc7f58200002f3048df7a664c043f819",
    (
        "global-weighting-v1",
        "laws",
    ): "1aad4038f52298deca0b836ddc81ae59d29ac46d0bcbf44dc18c06f3a4cd582c",
    (
        "global-weighting-v1",
        "resource_gates",
    ): "53b3c9557ac55a8c62531feb41ea072417d5f9bf82ddc54c4c5e57f5b4f332b2",
    (
        "observation-transaction-v1",
        "thresholds",
    ): "68712d1f4ef427a03775f10d0d864daa9ff49ee44a81a39eb7edf33f5eb0f2d4",
    (
        "observation-transaction-v1",
        "laws",
    ): "a40814e45997e423400d832bed908ad4240aab607f27ad3fd2884710bb74ac53",
    (
        "observation-transaction-v1",
        "resource_gates",
    ): "fb8095e5e44341c3dc1ab06aa8a26cd259905f8f3485d4ca88253af1459da70a",
    (
        "resource-authority-foundation-v1",
        "thresholds",
    ): "2bf2c8ef26b906914acaa8402af3465208fa62665bc791e1abc811beaaa129fb",
    (
        "resource-authority-foundation-v1",
        "laws",
    ): "ae7f8f9db6532d96dd1190edf2c3a2999cd803f62e9f64eb93c199bd5740847a",
    (
        "resource-authority-foundation-v1",
        "resource_gates",
    ): "57a2e38fb806ad695f9fdf846690a9e020d48fff59521f3a2b995fcf43b23d5f",
}


class ArchitectureError(RuntimeError):
    """Raised when an imaging architecture contract is invalid."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--policy",
        type=Path,
        default=DEFAULT_POLICY,
        help="dependency policy JSON (default: repository policy)",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        help="read Cargo metadata JSON instead of querying the live workspace",
    )
    parser.add_argument(
        "--migration-matrix",
        type=Path,
        help="override the optional migration-matrix path declared by the policy",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="mutate synthetic contracts to prove that forbidden states fail",
    )
    return parser.parse_args()


def load_object(path: Path, context: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ArchitectureError(
            f"cannot load {context} {display_path(path)}: {error}"
        ) from error
    if not isinstance(value, dict):
        raise ArchitectureError(
            f"{context} {display_path(path)} must contain an object"
        )
    return value


def display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def require_string(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ArchitectureError(f"{context} must be a non-empty string")
    return value


def require_string_list(value: Any, context: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ArchitectureError(f"{context} must be a non-empty array")
    result = [
        require_string(item, f"{context}[{index}]") for index, item in enumerate(value)
    ]
    if len(set(result)) != len(result):
        raise ArchitectureError(f"{context} contains duplicates")
    return result


def validate_policy(
    policy: dict[str, Any], *, enforce_accepted_scope: bool = True
) -> None:
    if policy.get("schema_version") != 1:
        raise ArchitectureError("dependency policy schema_version must be 1")
    require_string(policy.get("decision"), "dependency policy decision")
    layers = require_string_list(policy.get("layers"), "dependency policy layers")
    layer_set = set(layers)

    allowed = policy.get("allowed_logical_edges")
    if not isinstance(allowed, dict) or set(allowed) != layer_set:
        raise ArchitectureError(
            "allowed_logical_edges must define every layer exactly once"
        )
    for source in layers:
        targets = allowed[source]
        if not isinstance(targets, list):
            raise ArchitectureError(f"allowed_logical_edges.{source} must be an array")
        if len(targets) != len(set(targets)):
            raise ArchitectureError(
                f"allowed_logical_edges.{source} contains duplicates"
            )
        unknown = sorted(set(targets) - layer_set)
        if unknown:
            raise ArchitectureError(
                f"allowed_logical_edges.{source} names unknown layers: {unknown}"
            )
        if source in targets:
            raise ArchitectureError(
                f"allowed_logical_edges.{source} may not contain itself"
            )
    if "backend" in allowed["application"]:
        raise ArchitectureError(
            "allowed_logical_edges.application may not contain backend; applications invoke execution plans"
        )
    package_layers = policy.get("package_layers")
    if not isinstance(package_layers, dict) or not package_layers:
        raise ArchitectureError("package_layers must be a non-empty object")
    for package, layer in package_layers.items():
        require_string(package, "package_layers key")
        if layer not in layer_set:
            raise ArchitectureError(f"package {package} names unknown layer {layer!r}")

    classifications = policy.get("workspace_package_classification")
    if not isinstance(classifications, dict) or not classifications:
        raise ArchitectureError(
            "workspace_package_classification must be a non-empty object"
        )
    for package, classification in classifications.items():
        require_string(package, "workspace_package_classification key")
        if classification not in PACKAGE_CLASSIFICATIONS:
            raise ArchitectureError(
                f"workspace package {package} has unknown classification {classification!r}"
            )
    missing_classifications = sorted(set(package_layers) - set(classifications))
    if missing_classifications:
        raise ArchitectureError(
            f"logical imaging packages lack workspace classification: {missing_classifications}"
        )
    unmapped_surfaces = sorted(
        package
        for package, classification in classifications.items()
        if classification == "surface" and package not in package_layers
    )
    if unmapped_surfaces:
        raise ArchitectureError(
            f"imaging surface packages lack logical layers: {unmapped_surfaces}"
        )
    for package, layer in package_layers.items():
        classification = classifications[package]
        if classification not in {"native", "surface"}:
            raise ArchitectureError(
                f"logical imaging package {package} must be classified native or surface"
            )

    native_rules = policy.get("native_package_workspace_dependencies")
    if not isinstance(native_rules, dict):
        raise ArchitectureError(
            "native_package_workspace_dependencies must be an object"
        )
    classified_native = {
        package
        for package, classification in classifications.items()
        if classification == "native"
    }
    if set(native_rules) != classified_native:
        raise ArchitectureError(
            "native_package_workspace_dependencies must define every native package exactly: "
            f"added={sorted(set(native_rules) - classified_native)}, "
            f"removed={sorted(classified_native - set(native_rules))}"
        )
    for package, dependencies in native_rules.items():
        if package_layers.get(package) is None:
            raise ArchitectureError(
                f"native dependency rule {package} is not a native package"
            )
        if not isinstance(dependencies, list) or any(
            not isinstance(dependency, str) or not dependency
            for dependency in dependencies
        ):
            raise ArchitectureError(
                f"native_package_workspace_dependencies.{package} must be a string array"
            )
        if len(dependencies) != len(set(dependencies)):
            raise ArchitectureError(
                f"native_package_workspace_dependencies.{package} contains duplicates"
            )

    prefixes = require_string_list(
        policy.get("device_dependency_prefixes"), "device_dependency_prefixes"
    )
    if any(prefix != prefix.lower() for prefix in prefixes):
        raise ArchitectureError("device_dependency_prefixes must be lowercase")
    device_free = require_string_list(
        policy.get("device_free_layers"), "device_free_layers"
    )
    unknown_device_free = sorted(set(device_free) - layer_set)
    if unknown_device_free:
        raise ArchitectureError(
            f"device_free_layers names unknown layers: {unknown_device_free}"
        )
    source_boundaries = policy.get("source_boundaries")
    validate_source_boundary_policy(source_boundaries)

    require_string(policy.get("migration_matrix"), "migration_matrix")
    required_issues = policy.get("required_migration_evidence_issues")
    if (
        not isinstance(required_issues, list)
        or not required_issues
        or any(not isinstance(issue, int) or issue <= 0 for issue in required_issues)
        or len(required_issues) != len(set(required_issues))
    ):
        raise ArchitectureError(
            "required_migration_evidence_issues must be a unique array of positive integers"
        )
    if set(required_issues) != ACCEPTED_MIGRATION_ISSUES:
        raise ArchitectureError(
            "required_migration_evidence_issues differs from the accepted issue scope: "
            f"added={sorted(set(required_issues) - ACCEPTED_MIGRATION_ISSUES)}, "
            f"removed={sorted(ACCEPTED_MIGRATION_ISSUES - set(required_issues))}"
        )


def validate_source_boundary_policy(value: Any) -> None:
    if not isinstance(value, list) or not value:
        raise ArchitectureError("source_boundaries must be a non-empty array")
    identifiers: set[str] = set()
    for index, boundary in enumerate(value):
        context = f"source_boundaries[{index}]"
        if not isinstance(boundary, dict):
            raise ArchitectureError(f"{context} must be an object")
        required_keys = {
            "id",
            "roots",
            "extensions",
            "forbidden_patterns",
        }
        if set(boundary) != required_keys:
            raise ArchitectureError(
                f"{context} must contain exactly the structural source-boundary keys"
            )
        identifier = require_string(boundary.get("id"), f"{context}.id")
        if identifier in identifiers:
            raise ArchitectureError(f"source_boundaries repeats id {identifier}")
        identifiers.add(identifier)
        roots = require_string_list(boundary.get("roots"), f"{context}.roots")
        for root_text in roots:
            root = Path(root_text)
            if root.is_absolute() or ".." in root.parts:
                raise ArchitectureError(
                    f"{context}.roots must stay inside the repository"
                )
        extensions = require_string_list(
            boundary.get("extensions"), f"{context}.extensions"
        )
        if any(not extension.startswith(".") for extension in extensions):
            raise ArchitectureError(f"{context}.extensions must start with a dot")
        patterns = boundary.get("forbidden_patterns")
        if not isinstance(patterns, list) or not patterns:
            raise ArchitectureError(
                f"{context}.forbidden_patterns must be a non-empty array"
            )
        for pattern_index, pattern in enumerate(patterns):
            pattern_context = f"{context}.forbidden_patterns[{pattern_index}]"
            if not isinstance(pattern, dict) or set(pattern) != {"regex", "message"}:
                raise ArchitectureError(
                    f"{pattern_context} must contain regex and message only"
                )
            expression = require_string(
                pattern.get("regex"), f"{pattern_context}.regex"
            )
            require_string(pattern.get("message"), f"{pattern_context}.message")
            try:
                re.compile(expression)
            except re.error as error:
                raise ArchitectureError(
                    f"{pattern_context}.regex is invalid: {error}"
                ) from error


def validate_logical_edge(policy: dict[str, Any], source: str, target: str) -> None:
    layers = set(policy["layers"])
    if source not in layers or target not in layers:
        raise ArchitectureError(
            f"logical edge names an unknown layer: {source} -> {target}"
        )
    if target not in policy["allowed_logical_edges"][source]:
        raise ArchitectureError(f"forbidden logical imaging edge: {source} -> {target}")


def load_cargo_metadata(path: Path | None) -> dict[str, Any]:
    if path is not None:
        return load_object(path, "Cargo metadata")
    try:
        result = subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        detail = (
            error.stderr.strip()
            if isinstance(error, subprocess.CalledProcessError)
            else str(error)
        )
        raise ArchitectureError(
            f"cannot query live Cargo metadata: {detail}"
        ) from error
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ArchitectureError(
            f"Cargo metadata returned invalid JSON: {error}"
        ) from error
    if not isinstance(value, dict):
        raise ArchitectureError("Cargo metadata must contain an object")
    return value


def workspace_edges(
    metadata: dict[str, Any],
) -> tuple[set[str], set[tuple[str, str, str]], dict[str, list[dict[str, Any]]]]:
    packages = metadata.get("packages")
    if not isinstance(packages, list):
        raise ArchitectureError("Cargo metadata packages must be an array")
    names: set[str] = set()
    edges: set[tuple[str, str, str]] = set()
    dependencies: dict[str, list[dict[str, Any]]] = {}
    for index, package in enumerate(packages):
        if not isinstance(package, dict):
            raise ArchitectureError(
                f"Cargo metadata packages[{index}] must be an object"
            )
        name = require_string(
            package.get("name"), f"Cargo metadata packages[{index}].name"
        )
        if name in names:
            raise ArchitectureError(f"Cargo metadata repeats package {name}")
        names.add(name)
        package_dependencies = package.get("dependencies", [])
        if not isinstance(package_dependencies, list) or any(
            not isinstance(dependency, dict) for dependency in package_dependencies
        ):
            raise ArchitectureError(
                f"Cargo metadata dependencies for {name} must be an array"
            )
        dependencies[name] = package_dependencies
        for dependency in package_dependencies:
            if dependency.get("path") is None:
                continue
            kind = dependency.get("kind") or "normal"
            if kind not in {"normal", "build"}:
                continue
            target = require_string(
                dependency.get("name"), f"Cargo metadata dependency target for {name}"
            )
            edges.add((name, target, kind))
    return names, edges, dependencies


def package_production_sources(metadata: dict[str, Any]) -> dict[str, list[Path]]:
    sources: dict[str, list[Path]] = {}
    for index, package in enumerate(metadata["packages"]):
        name = require_string(
            package.get("name"), f"Cargo metadata packages[{index}].name"
        )
        manifest = Path(
            require_string(
                package.get("manifest_path"),
                f"Cargo metadata packages[{index}].manifest_path",
            )
        )
        package_sources: set[Path] = set()
        root = manifest.parent / "src"
        if root.is_dir():
            package_sources.update(
                path
                for path in sorted(root.rglob("*.rs"))
                if path.name != "tests.rs" and "tests" not in path.relative_to(root).parts
            )
        targets = package.get("targets")
        if not isinstance(targets, list) or any(
            not isinstance(target, dict) for target in targets
        ):
            raise ArchitectureError(
                f"Cargo metadata targets for {name} must be an array"
            )
        for target_index, target in enumerate(targets):
            kinds = target.get("kind")
            if not isinstance(kinds, list) or any(
                not isinstance(kind, str) for kind in kinds
            ):
                raise ArchitectureError(
                    f"Cargo metadata targets[{target_index}].kind for {name} must be an array"
                )
            if "custom-build" not in kinds:
                continue
            package_sources.add(
                Path(
                    require_string(
                        target.get("src_path"),
                        f"Cargo metadata targets[{target_index}].src_path for {name}",
                    )
                )
            )
        sources[name] = sorted(package_sources)
    return sources


def validate_forward_invariants(
    policy: dict[str, Any], metadata: dict[str, Any]
) -> None:
    classifications: dict[str, str] = policy["workspace_package_classification"]
    sources = package_production_sources(metadata)
    matrix_pattern = re.compile(r"migration[-_]matrix\.json", re.IGNORECASE)

    for package, classification in classifications.items():
        if classification not in {"native", "surface"}:
            continue
        for path in sources[package]:
            source = path.read_text(encoding="utf-8")
            if matrix_pattern.search(source):
                raise ArchitectureError(
                    "runtime source must not compile or interpret the migration matrix: "
                    f"{display_path(path)}"
                )


def matches_dependency_prefix(name: str, prefix: str) -> bool:
    normalized = name.lower().replace("_", "-")
    return normalized == prefix or normalized.startswith(prefix + "-")


def validate_workspace(policy: dict[str, Any], metadata: dict[str, Any]) -> None:
    package_names, edges, dependencies = workspace_edges(metadata)
    package_layers: dict[str, str] = policy["package_layers"]
    classifications: dict[str, str] = policy["workspace_package_classification"]
    added_packages = sorted(package_names - set(classifications))
    removed_packages = sorted(set(classifications) - package_names)
    if added_packages or removed_packages:
        raise ArchitectureError(
            "workspace package classification differs from Cargo metadata: "
            f"added={added_packages}, removed={removed_packages}"
        )
    missing = sorted(set(package_layers) - package_names)
    if missing:
        raise ArchitectureError(
            f"policy-owned workspace packages are missing: {missing}"
        )

    for source, target, kind in sorted(edges):
        source_layer = package_layers.get(source)
        target_layer = package_layers.get(target)
        if source_layer is None or target_layer is None:
            continue
        if source_layer == target_layer:
            continue
        try:
            validate_logical_edge(policy, source_layer, target_layer)
        except ArchitectureError as error:
            raise ArchitectureError(
                f"{error}; package edge {source} -> {target}"
            ) from error

    native_rules: dict[str, list[str]] = policy["native_package_workspace_dependencies"]
    for package, allowed in native_rules.items():
        actual = {target for source, target, _kind in edges if source == package}
        unexpected = sorted(actual - set(allowed))
        missing_dependencies = sorted(set(allowed) - actual)
        if unexpected:
            raise ArchitectureError(
                f"native package {package} has undeclared workspace dependencies: {unexpected}"
            )
        if missing_dependencies:
            raise ArchitectureError(
                f"native package {package} is missing declared workspace dependencies: "
                f"{missing_dependencies}"
            )

    device_free = set(policy["device_free_layers"])
    prefixes: list[str] = policy["device_dependency_prefixes"]
    for package, classification in classifications.items():
        layer = package_layers.get(package)
        if classification != "surface" and layer not in device_free:
            continue
        for dependency in dependencies[package]:
            kind = dependency.get("kind") or "normal"
            if kind not in {"normal", "build"}:
                continue
            name = require_string(
                dependency.get("name"),
                f"Cargo metadata dependency target for {package}",
            )
            prefix = next(
                (
                    candidate
                    for candidate in prefixes
                    if matches_dependency_prefix(name, candidate)
                ),
                None,
            )
            if prefix is not None:
                raise ArchitectureError(
                    f"device-free package {package}({layer or classification}) imports {name} "
                    f"(forbidden family {prefix})"
                )


def source_boundary_violations(
    boundary: dict[str, Any], repo_root: Path = REPO_ROOT
) -> list[dict[str, Any]]:
    violations = []
    extensions = set(boundary["extensions"])
    for root_text in boundary["roots"]:
        root = repo_root / root_text
        if root.is_file():
            paths = [root]
        elif root.is_dir():
            paths = sorted(root.rglob("*"))
        else:
            raise ArchitectureError(
                f"source boundary {boundary['id']} cannot read root {root_text}"
            )
        for path in paths:
            if not path.is_file() or path.suffix not in extensions:
                continue
            try:
                source = path.read_text(encoding="utf-8")
            except OSError as error:
                raise ArchitectureError(
                    f"source boundary {boundary['id']} cannot read {path}: {error}"
                ) from error
            relative = str(path.relative_to(repo_root))
            for pattern_index, forbidden in enumerate(boundary["forbidden_patterns"]):
                for match in re.finditer(forbidden["regex"], source):
                    violations.append(
                        {
                            "path": relative,
                            "pattern": pattern_index,
                            "match": match.group(0),
                            "context": normalized_violation_context(source, match),
                            "line": source.count("\n", 0, match.start()) + 1,
                            "message": forbidden["message"],
                        }
                    )
    return violations


def normalized_violation_context(source: str, match: re.Match[str]) -> str:
    line_start = source.rfind("\n", 0, match.start()) + 1
    line_end = source.find("\n", match.end())
    if line_end == -1:
        line_end = len(source)
    first_line = source[line_start:line_end]
    if re.match(r"\s*(?:pub\s+)?(?:use|extern\s+crate)\b", first_line):
        statement_end = source.find(";", match.end())
        if statement_end != -1:
            line_end = statement_end + 1
    return " ".join(source[line_start:line_end].split())


def validate_source_boundaries(
    policy: dict[str, Any], repo_root: Path = REPO_ROOT
) -> None:
    for boundary in policy["source_boundaries"]:
        violations = source_boundary_violations(boundary, repo_root)
        if not violations:
            continue
        violation = violations[0]
        raise ArchitectureError(
            f"{violation['message']}: {violation['path']}:{violation['line']}"
        )


def validate_acceptance_contract(identifier: str, contract: dict[str, Any]) -> None:
    context = f"acceptance contract {identifier}"
    required = {
        "baseline_identity",
        "comparator",
        "thresholds",
        "laws",
        "resource_gates",
        "evidence_tiers",
    }
    if set(contract) != required:
        raise ArchitectureError(f"{context} must contain exactly {sorted(required)}")
    require_string(contract.get("baseline_identity"), f"{context}.baseline_identity")
    comparator = contract.get("comparator")
    if not isinstance(comparator, dict):
        raise ArchitectureError(f"{context}.comparator must be an object")
    comparator_fields = {
        "kind",
        "normalized_rms_ceiling",
        "denominator",
        "preprocessing",
    }
    if set(comparator) != comparator_fields:
        raise ArchitectureError(
            f"{context}.comparator must contain exactly {sorted(comparator_fields)}"
        )
    require_string(comparator.get("kind"), f"{context}.comparator.kind")
    require_string(
        comparator.get("preprocessing"), f"{context}.comparator.preprocessing"
    )
    ceiling = comparator.get("normalized_rms_ceiling")
    if ceiling is not None:
        if (
            isinstance(ceiling, bool)
            or not isinstance(ceiling, (int, float))
            or ceiling < 0
        ):
            raise ArchitectureError(
                f"{context}.comparator.normalized_rms_ceiling must be null or non-negative"
            )
        if ceiling > 0.001:
            raise ArchitectureError(
                f"{context}.comparator.normalized_rms_ceiling may not exceed 0.001"
            )
        require_string(
            comparator.get("denominator"), f"{context}.comparator.denominator"
        )
    elif comparator.get("denominator") is not None:
        raise ArchitectureError(
            f"{context}.comparator.denominator must be null without a normalized RMS ceiling"
        )
    require_string_list(contract.get("thresholds"), f"{context}.thresholds")
    require_string_list(contract.get("laws"), f"{context}.laws")
    require_string_list(contract.get("resource_gates"), f"{context}.resource_gates")
    require_string_list(contract.get("evidence_tiers"), f"{context}.evidence_tiers")


def contract_ids(value: Any) -> set[str]:
    if isinstance(value, dict):
        result = set()
        for identifier, contract in value.items():
            identifier = require_string(identifier, "acceptance_contracts key")
            result.add(identifier)
            if not isinstance(contract, dict):
                raise ArchitectureError(
                    f"acceptance contract {identifier} must be an object"
                )
            validate_acceptance_contract(identifier, contract)
        return result
    if isinstance(value, list):
        result: set[str] = set()
        for index, contract in enumerate(value):
            if not isinstance(contract, dict):
                raise ArchitectureError(
                    f"acceptance_contracts[{index}] must be an object"
                )
            identifier = require_string(
                contract.get("id"), f"acceptance_contracts[{index}].id"
            )
            if identifier in result:
                raise ArchitectureError(f"acceptance_contracts repeats {identifier}")
            result.add(identifier)
            validate_acceptance_contract(identifier, contract)
        return result
    raise ArchitectureError("acceptance_contracts must be an object or array")


def validate_locator(value: Any, context: str) -> None:
    if isinstance(value, str):
        require_string(value, context)
        return
    if isinstance(value, dict):
        if not value or not (set(value) & LOCATOR_KEYS):
            raise ArchitectureError(
                f"{context} must contain at least one locator key: {sorted(LOCATOR_KEYS)}"
            )
        for key in set(value) & LOCATOR_KEYS:
            locator = value[key]
            if isinstance(locator, int) and key == "issue" and locator > 0:
                continue
            require_string(locator, f"{context}.{key}")
        return
    raise ArchitectureError(f"{context} must be a locator string or object")


def validate_locator_collection(value: Any, context: str) -> None:
    if not isinstance(value, list) or not value:
        raise ArchitectureError(f"{context} must be a non-empty array")
    for index, locator in enumerate(value):
        validate_locator(locator, f"{context}[{index}]")


def validate_source_evidence(value: Any, context: str) -> None:
    validate_locator_collection(value, context)
    for index, locator in enumerate(value):
        if not isinstance(locator, str) or "::" not in locator:
            raise ArchitectureError(
                f"{context}[{index}] must be a repository-relative path::token locator"
            )
        path_text, token = locator.rsplit("::", 1)
        path = Path(path_text)
        if path.is_absolute() or ".." in path.parts:
            raise ArchitectureError(
                f"{context}[{index}] path must stay inside the repository"
            )
        require_string(token, f"{context}[{index}] token")
        source_path = REPO_ROOT / path
        try:
            source = source_path.read_text(encoding="utf-8")
        except OSError as error:
            raise ArchitectureError(
                f"{context}[{index}] cannot read source evidence {path_text}: {error}"
            ) from error
        if token not in source:
            raise ArchitectureError(
                f"{context}[{index}] source evidence token {token!r} was not found in {path_text}"
            )


def baseline_manifest_content(locator: str, context: str) -> bytes:
    if not locator.startswith("repo://"):
        raise ArchitectureError(
            f"{context} must be a repository baseline locator with pinned content"
        )
    relative, separator, fragment = locator.removeprefix("repo://").partition("#")
    path = Path(relative)
    if not relative or path.is_absolute() or ".." in path.parts:
        raise ArchitectureError(f"{context} path must stay inside the repository")
    try:
        content = (REPO_ROOT / path).read_bytes()
    except OSError as error:
        raise ArchitectureError(
            f"{context} cannot read baseline manifest {relative}: {error}"
        ) from error
    if separator:
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ArchitectureError(
                f"{context} baseline fragment requires UTF-8 content: {error}"
            ) from error
        if not fragment or fragment not in text:
            raise ArchitectureError(
                f"{context} baseline fragment {fragment!r} was not found in {relative}"
            )
    return content


def validate_baseline_manifest_registry(value: Any) -> dict[str, str]:
    context = "migration matrix baseline_manifest_digests"
    if not isinstance(value, dict) or not value:
        raise ArchitectureError(f"{context} must be a non-empty object")
    for locator, digest in value.items():
        require_string(locator, f"{context} locator")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ArchitectureError(
                f"{context}.{locator} must be a lowercase SHA-256 digest"
            )
        content = baseline_manifest_content(locator, f"{context}.{locator}")
        actual = hashlib.sha256(content).hexdigest()
        if actual != digest:
            raise ArchitectureError(
                f"{context}.{locator} content digest differs: expected {digest}, actual {actual}"
            )
    return value


def validate_baseline_manifests(
    value: Any, context: str, registry: dict[str, str]
) -> set[str]:
    validate_locator_collection(value, context)
    used = set()
    for index, locator in enumerate(value):
        if not isinstance(locator, str) or locator not in registry:
            raise ArchitectureError(
                f"{context}[{index}] must reference a content-pinned baseline manifest"
            )
        used.add(locator)
    return used


def issue_number(value: Any, context: str) -> int:
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str):
        match = re.fullmatch(r"#?(\d+)", value.strip())
        if match and int(match.group(1)) > 0:
            return int(match.group(1))
    raise ArchitectureError(f"{context} must be a positive issue number")


def validate_issue_outcomes(value: Any) -> set[int]:
    if not isinstance(value, list) or not value:
        raise ArchitectureError(
            "migration matrix issue_outcomes must be a non-empty array"
        )
    fields = {
        "issue",
        "required_outcome",
        "current_owner",
        "destination_tickets",
        "acceptance_gates",
        "deletion_or_retention_condition",
    }
    issues: set[int] = set()
    for index, outcome in enumerate(value):
        context = f"migration matrix issue_outcomes[{index}]"
        if not isinstance(outcome, dict) or set(outcome) != fields:
            raise ArchitectureError(f"{context} must contain exactly {sorted(fields)}")
        issue = issue_number(outcome.get("issue"), f"{context}.issue")
        if issue in issues:
            raise ArchitectureError(
                f"migration matrix issue_outcomes repeats issue {issue}"
            )
        issues.add(issue)
        require_string(outcome.get("required_outcome"), f"{context}.required_outcome")
        require_string(outcome.get("current_owner"), f"{context}.current_owner")
        require_string_list(
            outcome.get("destination_tickets"), f"{context}.destination_tickets"
        )
        require_string_list(
            outcome.get("acceptance_gates"), f"{context}.acceptance_gates"
        )
        require_string(
            outcome.get("deletion_or_retention_condition"),
            f"{context}.deletion_or_retention_condition",
        )
    return issues


def stable_digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def rust_function_body(source: str, identifier: str, path: Path) -> str:
    declaration = re.search(
        rf"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?fn\s+{re.escape(identifier)}(?:\s*<[^>]+>)?\s*\(",
        source,
    )
    if declaration is None:
        raise ArchitectureError(
            f"cannot find Rust function {identifier} in {display_path(path)}"
        )
    opening = source.find("{", declaration.end())
    if opening < 0:
        raise ArchitectureError(
            f"Rust function {identifier} in {display_path(path)} has no body"
        )
    depth = 1
    end = opening + 1
    while end < len(source) and depth:
        if source[end] == "{":
            depth += 1
        elif source[end] == "}":
            depth -= 1
        end += 1
    if depth:
        raise ArchitectureError(
            f"Rust function {identifier} in {display_path(path)} has no closing brace"
        )
    return source[opening + 1 : end - 1]


def rust_struct_fields(source: str, identifier: str, path: Path) -> dict[str, str]:
    declaration = re.search(
        rf"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?struct\s+{re.escape(identifier)}(?:\s*<[^>]+>)?\s*\{{",
        source,
    )
    if declaration is None:
        raise ArchitectureError(
            f"cannot find Rust struct {identifier} in {display_path(path)}"
        )
    depth = 1
    end = declaration.end()
    while end < len(source) and depth:
        if source[end] == "{":
            depth += 1
        elif source[end] == "}":
            depth -= 1
        end += 1
    if depth:
        raise ArchitectureError(
            f"Rust struct {identifier} in {display_path(path)} has no closing brace"
        )
    body = source[declaration.end() : end - 1]
    return {
        name: re.sub(r"\s+", "", field_type)
        for name, field_type in re.findall(
            r"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?([a-z][A-Za-z0-9_]*)\s*:\s*([^,\n]+),\s*$",
            body,
        )
    }


def rust_impl_method_body(
    source: str, owner: str, method: str, path: Path
) -> str:
    declaration = re.search(
        rf"(?m)^\s*impl\s+{re.escape(owner)}\s*\{{",
        source,
    )
    if declaration is None:
        raise ArchitectureError(
            f"cannot find Rust impl {owner} in {display_path(path)}"
        )
    depth = 1
    end = declaration.end()
    while end < len(source) and depth:
        if source[end] == "{":
            depth += 1
        elif source[end] == "}":
            depth -= 1
        end += 1
    if depth:
        raise ArchitectureError(
            f"Rust impl {owner} in {display_path(path)} has no closing brace"
        )
    return rust_function_body(source[declaration.end() : end - 1], method, path)


def validate_migration_matrix(
    matrix: dict[str, Any],
    policy: dict[str, Any],
    *,
    enforce_accepted_scope: bool = True,
) -> None:
    if matrix.get("schema_version") != 1:
        raise ArchitectureError("migration matrix schema_version must be 1")
    if enforce_accepted_scope:
        if matrix.get("programme_issue") != 486 or matrix.get("owner_issue") != 487:
            raise ArchitectureError(
                "migration matrix programme_issue and owner_issue must remain #486 and #487"
            )
        if matrix.get("status_values") != [
            "Native",
            "TemporarilyUnavailable",
        ]:
            raise ArchitectureError(
                "migration matrix status_values differ from the accepted dispositions"
            )
    revision = matrix.get("contract_revision")
    if not (
        isinstance(revision, int)
        and revision > 0
        or isinstance(revision, str)
        and revision.strip()
    ):
        raise ArchitectureError(
            "migration matrix contract_revision must be positive or non-empty"
        )
    if enforce_accepted_scope and revision != ACCEPTED_MATRIX_CONTRACT_REVISION:
        raise ArchitectureError(
            "migration matrix contract_revision differs from the accepted revision"
        )
    known_contracts = contract_ids(matrix.get("acceptance_contracts"))
    if not known_contracts:
        raise ArchitectureError("migration matrix must define acceptance contracts")
    if enforce_accepted_scope:
        accepted_contracts = {
            identifier for identifier, _field in ACCEPTED_CONTRACT_REQUIREMENT_SHA256
        }
        if known_contracts != accepted_contracts:
            raise ArchitectureError(
                "migration matrix acceptance contracts differ from the accepted scope"
            )
        for (
            identifier,
            field,
        ), accepted in ACCEPTED_CONTRACT_REQUIREMENT_SHA256.items():
            if (
                stable_digest(matrix["acceptance_contracts"][identifier][field])
                != accepted
            ):
                raise ArchitectureError(
                    f"acceptance contract {identifier}.{field} differs from the accepted scope"
                )
        if (
            stable_digest(matrix["acceptance_contracts"])
            != ACCEPTED_ACCEPTANCE_CONTRACTS_SHA256
        ):
            raise ArchitectureError(
                "migration matrix acceptance contract content differs from the accepted scope"
            )

    declared_crosswalk = matrix.get("required_issue_crosswalk")
    if not isinstance(declared_crosswalk, list) or any(
        not isinstance(issue, int) or issue <= 0 for issue in declared_crosswalk
    ):
        raise ArchitectureError(
            "migration matrix required_issue_crosswalk must be an array of positive integers"
        )
    if len(declared_crosswalk) != len(set(declared_crosswalk)):
        raise ArchitectureError(
            "migration matrix required_issue_crosswalk contains duplicates"
        )
    expected_crosswalk = set(policy["required_migration_evidence_issues"])
    if set(declared_crosswalk) != expected_crosswalk:
        raise ArchitectureError(
            "migration matrix required_issue_crosswalk differs from dependency policy: "
            f"added={sorted(set(declared_crosswalk) - expected_crosswalk)}, "
            f"removed={sorted(expected_crosswalk - set(declared_crosswalk))}"
        )
    if enforce_accepted_scope and set(declared_crosswalk) != ACCEPTED_MIGRATION_ISSUES:
        raise ArchitectureError(
            "migration matrix required_issue_crosswalk differs from the accepted issue scope"
        )

    inventory = matrix.get("inventory")
    if not isinstance(inventory, dict) or set(inventory) != MATRIX_KINDS:
        raise ArchitectureError(
            f"migration matrix inventory must define exactly {sorted(MATRIX_KINDS)}"
        )
    inventory_pairs: set[tuple[str, str]] = set()
    for kind in sorted(MATRIX_KINDS):
        identifiers = require_string_list(
            inventory.get(kind), f"migration matrix inventory.{kind}"
        )
        inventory_pairs.update((kind, identifier) for identifier in identifiers)
    baseline_registry = validate_baseline_manifest_registry(
        matrix.get("baseline_manifest_digests")
    )
    if (
        enforce_accepted_scope
        and stable_digest(baseline_registry)
        != ACCEPTED_BASELINE_MANIFEST_DIGESTS_SHA256
    ):
        raise ArchitectureError(
            "migration matrix baseline manifest digests differ from the accepted evidence"
        )

    outcome_issues = (
        validate_issue_outcomes(matrix.get("issue_outcomes"))
        if enforce_accepted_scope
        else set()
    )
    if enforce_accepted_scope and outcome_issues != ACCEPTED_MIGRATION_ISSUES:
        raise ArchitectureError(
            "migration matrix issue_outcomes differs from the accepted issue scope: "
            f"added={sorted(outcome_issues - ACCEPTED_MIGRATION_ISSUES)}, "
            f"removed={sorted(ACCEPTED_MIGRATION_ISSUES - outcome_issues)}"
        )
    if (
        enforce_accepted_scope
        and stable_digest(matrix["issue_outcomes"]) != ACCEPTED_ISSUE_OUTCOMES_SHA256
    ):
        raise ArchitectureError(
            "migration matrix issue_outcomes content differs from the accepted crosswalk outcomes"
        )

    rows = matrix.get("rows")
    if not isinstance(rows, list) or not rows:
        raise ArchitectureError("migration matrix rows must be a non-empty array")
    row_ids: set[str] = set()
    row_pairs: set[tuple[str, str]] = set()
    covered_issues: set[int] = set()
    used_baselines: set[str] = set()
    required_fields = {
        "id",
        "kind",
        "status",
        "current_owner",
        "destination_tickets",
        "evidence_issues",
        "baseline_manifests",
        "acceptance_contract",
        "transfer_point",
        "deletion_condition",
        "migration_obligation",
        "source_evidence",
    }
    for index, row in enumerate(rows):
        context = f"migration matrix rows[{index}]"
        if not isinstance(row, dict):
            raise ArchitectureError(f"{context} must be an object")
        if set(row) != required_fields:
            raise ArchitectureError(
                f"{context} must contain exactly {sorted(required_fields)}"
            )
        identifier = require_string(row.get("id"), f"{context}.id")
        if identifier in row_ids:
            raise ArchitectureError(f"migration matrix repeats row id {identifier}")
        row_ids.add(identifier)
        kind = require_string(row.get("kind"), f"{context}.kind")
        if kind not in MATRIX_KINDS:
            raise ArchitectureError(
                f"{context}.kind must be one of {sorted(MATRIX_KINDS)}"
            )
        row_pairs.add((kind, identifier))
        status = row.get("status")
        if status not in VALID_STATUSES:
            raise ArchitectureError(
                f"{context}.status must be one of {sorted(VALID_STATUSES)}"
            )
        require_string(row.get("current_owner"), f"{context}.current_owner")
        require_string_list(
            row.get("destination_tickets"), f"{context}.destination_tickets"
        )
        evidence = row.get("evidence_issues")
        if not isinstance(evidence, list) or not evidence:
            raise ArchitectureError(
                f"{context}.evidence_issues must be a non-empty array"
            )
        covered_issues.update(
            issue_number(issue, f"{context}.evidence_issues[{issue_index}]")
            for issue_index, issue in enumerate(evidence)
        )
        used_baselines.update(
            validate_baseline_manifests(
                row.get("baseline_manifests"),
                f"{context}.baseline_manifests",
                baseline_registry,
            )
        )
        contract = require_string(
            row.get("acceptance_contract"), f"{context}.acceptance_contract"
        )
        if contract not in known_contracts:
            raise ArchitectureError(
                f"{context} references unknown acceptance contract {contract}"
            )
        require_string(row.get("transfer_point"), f"{context}.transfer_point")
        require_string(row.get("deletion_condition"), f"{context}.deletion_condition")
        obligation = row.get("migration_obligation")
        if status == "Native":
            if obligation is not None:
                raise ArchitectureError(
                    f"{context}.migration_obligation must be null for Native"
                )
        elif not isinstance(obligation, dict) or not obligation:
            raise ArchitectureError(
                f"{context}.migration_obligation must be a non-empty object until Native"
            )
        else:
            require_string(
                obligation.get("ticket"), f"{context}.migration_obligation.ticket"
            )
            require_string(
                obligation.get("reason"), f"{context}.migration_obligation.reason"
            )
        validate_source_evidence(
            row.get("source_evidence"), f"{context}.source_evidence"
        )

    if enforce_accepted_scope:
        validate_t17_ms_selection_transfer(rows)

    if row_pairs != inventory_pairs:
        raise ArchitectureError(
            "migration matrix inventory and rows differ: "
            f"missing={sorted(inventory_pairs - row_pairs)}, "
            f"extra={sorted(row_pairs - inventory_pairs)}"
        )
    if used_baselines != set(baseline_registry):
        raise ArchitectureError(
            "migration matrix baseline digest registry and rows differ: "
            f"unused={sorted(set(baseline_registry) - used_baselines)}, "
            f"unpinned={sorted(used_baselines - set(baseline_registry))}"
        )

    missing_issues = sorted(expected_crosswalk - covered_issues)
    if missing_issues:
        raise ArchitectureError(
            f"migration matrix omits required crosswalk issues: {missing_issues}"
        )
    if enforce_accepted_scope and stable_digest(rows) != ACCEPTED_MATRIX_ROWS_SHA256:
        raise ArchitectureError(
            "migration matrix row ledger differs from the accepted scope"
        )
    if enforce_accepted_scope:
        validate_t28_model_lifecycle_transfer(rows)
        validate_t18_global_weighting_transfer(rows)
        validate_t36_spectral_sampling_transfer(rows)
        validate_t37_spectral_operator_transfer(rows)


def validate_t36_spectral_sampling_transfer(rows: list[dict[str, Any]]) -> None:
    """Keep T36 coordinate and paired-sampling ownership native."""
    rows_by_id = {row.get("id"): row for row in rows}
    required_evidence = {
        "capability.lsrk-transform": {
            "crates/casa-ms/src/derived/engine.rs::pub(crate) fn spectral_frame_explicit",
            "crates/casa-ms/src/selected_observation/spectral_evaluation.rs::pub struct SelectedObservationTraversalSample",
            "crates/casa-imaging-reconstruction/src/spectral_sampling.rs::pub fn compile_spectral_stencil",
        },
        "capability.nearest-interpolation": {
            "crates/casa-imaging-reconstruction/src/spectral_sampling.rs::fn nearest_terms",
        },
        "capability.linear-interpolation": {
            "crates/casa-imaging-reconstruction/src/spectral_sampling.rs::fn linear_terms",
        },
        "capability.cubic-interpolation": {
            "crates/casa-imaging-reconstruction/src/spectral_sampling.rs::fn cubic_terms",
        },
    }
    for identifier, evidence in required_evidence.items():
        row = rows_by_id.get(identifier)
        if row is None or row.get("status") != "Native":
            raise ArchitectureError(f"T36 matrix row {identifier} must remain Native")
        if row.get("migration_obligation") is not None:
            raise ArchitectureError(
                f"T36 matrix row {identifier} must not retain a migration obligation"
            )
        if not evidence.issubset(set(row.get("source_evidence", []))):
            raise ArchitectureError(
                f"T36 matrix row {identifier} lacks native coordinate/sampling evidence"
            )


def validate_t37_spectral_operator_transfer(rows: list[dict[str, Any]]) -> None:
    """Keep the T37 matrix ownership aligned with the spectral operator cutover."""
    rows_by_id = {row.get("id"): row for row in rows}
    required_statuses = {
        "capability.spectral-cube": "Native",
        "capability.spectral-cubedata": "Native",
        "capability.standard-gridder": "Native",
    }
    required_evidence = {
        "capability.spectral-cube": {
            "crates/casa-imaging-reconstruction/src/spectral_operator.rs::pub struct SpectralOperatorSpecification",
            "crates/casa-imaging-runtime/src/complete_data_operator.rs::pub struct SpectralOperatorState",
        },
        "capability.spectral-cubedata": {
            "crates/casa-imaging-reconstruction/src/spectral_operator.rs::pub struct SpectralOperatorSpecification",
            "crates/casa-imaging-runtime/src/complete_data_operator.rs::pub struct SpectralOperatorState",
        },
        "capability.standard-gridder": {
            "crates/casa-imaging-reconstruction/src/spectral_operator.rs::pub struct CompleteDataOwnerState",
            "crates/casa-imaging-runtime/src/complete_data_operator.rs::pub struct SpectralOperatorState",
        },
    }
    for identifier, status in required_statuses.items():
        row = rows_by_id.get(identifier)
        if row is None or row.get("status") != status:
            raise ArchitectureError(
                f"T37 matrix row {identifier} must remain {status}"
            )
        if not required_evidence[identifier].issubset(
            set(row.get("source_evidence", []))
        ):
            raise ArchitectureError(
                f"T37 matrix row {identifier} lacks spectral-operator ownership evidence"
            )

    matrix_text = json.dumps(rows, sort_keys=True)
    if re.search(r"serial[_-](?:mfs|continuum)", matrix_text, re.IGNORECASE):
        raise ArchitectureError(
            "T37 matrix retains a displaced serial_mfs or serial_continuum owner"
        )

    deleted_paths = (
        "crates/casa-imaging-reconstruction/src/serial_mfs.rs",
        "crates/casa-imaging-runtime/src/serial_continuum.rs",
        "crates/casa-imaging-runtime/src/serial_continuum_plan.rs",
    )
    for relative in deleted_paths:
        if (REPO_ROOT / relative).exists():
            raise ArchitectureError(f"T37 displaced source still exists: {relative}")

    source_paths = (
        REPO_ROOT / "crates/casa-imaging-reconstruction/src/spectral_operator.rs",
        REPO_ROOT / "crates/casa-imaging-runtime/src/complete_data_operator.rs",
        REPO_ROOT / "crates/casa-imaging-runtime/src/spectral_cycle.rs",
    )
    for path in source_paths:
        try:
            source = path.read_text(encoding="utf-8")
        except OSError as error:
            raise ArchitectureError(
                f"cannot inspect T37 spectral owner {display_path(path)}: {error}"
            ) from error
        if re.search(r"\b(?:SerialMfs|SerialContinuum)\b|serial_(?:mfs|continuum)", source):
            raise ArchitectureError(
                f"T37 spectral owner retains a displaced serial symbol: {display_path(path)}"
            )


def validate_t28_model_lifecycle_transfer(rows: list[dict[str, Any]]) -> None:
    row = next((item for item in rows if item.get("id") == "capability.model-lifecycle"), None)
    if row is None or row.get("status") != "Native":
        raise ArchitectureError("T28 must leave capability.model-lifecycle Native")
    required_evidence = {
        "crates/casa-imaging-model/src/model_state.rs::pub struct ModelReprojectedSeedProjection",
        "crates/casa-imaging-model/src/model_state.rs::pub struct ModelLifecycleContract",
        "crates/casa-imaging-model/src/model_state.rs::validate_model_lifecycle_contract_identity",
        "crates/casa-imaging-model/src/model_state.rs::validate_model_reprojection_contract_identity",
        "crates/casa-imaging-model/src/compiled_problem.rs::validate_compiled_problem_identity",
        "crates/casa-imaging-reconstruction/src/lib.rs::pub struct ExecutableModelProblem",
        "crates/casa-imaging-reconstruction/src/lib.rs::pub struct ModelLifecycle",
        "crates/casa-imaging-reconstruction/src/lib.rs::prepare_reprojected_seed",
        "crates/casa-imaging-reconstruction/src/lib.rs::validate_reprojected_seed_proof_identity",
        "crates/casa-imaging-runtime/src/receipt.rs::struct ModelLifecycleProjection",
    }
    if not required_evidence.issubset(set(row.get("source_evidence", []))):
        raise ArchitectureError("T28 lacks the accepted lifecycle/receipt source evidence")

    model_path = REPO_ROOT / "crates/casa-imaging-model/src/model_state.rs"
    reconstruction_path = REPO_ROOT / "crates/casa-imaging-reconstruction/src/lib.rs"
    receipt_path = REPO_ROOT / "crates/casa-imaging-runtime/src/receipt.rs"
    try:
        model = model_path.read_text(encoding="utf-8")
        reconstruction = reconstruction_path.read_text(encoding="utf-8")
        receipt = receipt_path.read_text(encoding="utf-8")
    except OSError as error:
        raise ArchitectureError(f"cannot inspect T28 lifecycle sources: {error}") from error

    validate_t28_model_lifecycle_sources(
        model,
        reconstruction,
        receipt,
        model_path=model_path,
        reconstruction_path=reconstruction_path,
        receipt_path=receipt_path,
    )


def validate_t28_model_lifecycle_sources(
    model: str,
    reconstruction: str,
    receipt: str,
    *,
    model_path: Path,
    reconstruction_path: Path,
    receipt_path: Path,
) -> None:
    lifecycle_fields = rust_struct_fields(model, "ModelLifecycleContract", model_path)
    if lifecycle_fields.get("reprojection_contract") != "LogicalIdentity":
        raise ArchitectureError("T28 lifecycle must retain the typed reprojection contract identity")
    if lifecycle_fields.get("reprojection_policy") != "ModelReprojectionPolicy":
        raise ArchitectureError("T28 lifecycle must retain the compiler-owned reprojection policy")
    policy_requirements = (
        "pub struct ModelReprojectionPolicy",
        "pub const fn canonical()",
        "pub const fn direction_registry",
        "pub const fn basis_registry",
        "pub const fn polarization_registry",
        "pub const fn invalid_contributor",
        "pub const fn uncovered_target",
    )
    if not all(requirement in model for requirement in policy_requirements):
        raise ArchitectureError("T28 model policy must expose one typed canonical owner")
    compiler = re.sub(
        r"\s+", "", rust_function_body(model, "compile_model_lifecycle_contract", model_path)
    )
    if (
        "letreprojection_policy=ModelReprojectionPolicy::canonical();"
        not in compiler
        or "letreprojection=compile_model_reprojection_contract(product_graph,numerics_id,arithmetic_precision,reprojection_policy,);"
        not in compiler
        or "reprojection_contract:LogicalIdentity::from_sha256(reprojection.as_bytes()),"
        not in compiler
        or "reprojection_policy," not in compiler
    ):
        raise ArchitectureError("T28 compiler does not retain the closed reprojection contract")
    validator = re.sub(
        r"\s+",
        "",
        rust_function_body(
            model, "validate_model_reprojection_contract_identity", model_path
        ),
    )
    if (
        "claimed==model_reprojection_contract_identity(product_graph,numerics,conversion_precision,policy,)"
        not in validator
        or "Err(ModelContractError::ReprojectionContractMismatch)" not in validator
    ):
        raise ArchitectureError(
            "T28 model owner does not recompute and validate the reprojection identity"
        )

    commitment_fields = rust_struct_fields(
        model, "ModelReprojectedSeedProjection", model_path
    )
    if commitment_fields != {
        "source": "LogicalIdentity",
        "source_shape": "Box<ModelSourceShape>",
        "preparation_contract": "LogicalIdentity",
        "reprojection": "LogicalIdentity",
        "support": "LogicalIdentity",
        "samples": "LogicalIdentity",
        "stencil": "LogicalIdentity",
        "proof": "LogicalIdentity",
    }:
        raise ArchitectureError("T28 reprojected digest projection is not complete")
    if re.search(r"\bpub\s+fn\s+from_prepared_samples\b", model):
        raise ArchitectureError("T28 model exposes a raw reprojected evidence constructor")
    lifecycle_validator = re.sub(
        r"\s+",
        "",
        rust_function_body(
            model, "validate_model_lifecycle_contract_identity", model_path
        ),
    )
    if not all(
        binding in lifecycle_validator
        for binding in (
            "validate_input_commitment_identity(",
            "letexpected=model_lifecycle_contract_identity(",
            "ifclaimed==expected",
        )
    ):
        raise ArchitectureError(
            "T28 model owner does not canonically revalidate lifecycle input evidence"
        )

    reconstruction_fields = rust_struct_fields(reconstruction, "ModelLifecycle", reconstruction_path)
    if reconstruction_fields.get("contract") != "ModelLifecycleContract":
        raise ArchitectureError("T28 reconstruction owner does not retain ModelLifecycleContract")
    preparation = re.sub(
        r"\s+", "", rust_function_body(reconstruction, "prepare_reprojected_seed", reconstruction_path)
    )
    stencil = re.sub(
        r"\s+", "", rust_function_body(reconstruction, "derive_reprojection_stencil", reconstruction_path)
    )
    preparation_policy_bindings = (
        "letreprojection_policy=target_contract.reprojection_policy();",
        "derive_reprojection_stencil(&source_shape,target_shape,target,precision,reprojection_policy,",
        "matchreprojection_policy.invalid_contributor()",
        "matchreprojection_policy.uncovered_target()",
    )
    stencil_policy_bindings = (
        "matchreprojection_policy.direction_registry()",
        "matchreprojection_policy.basis_registry()",
        "matchreprojection_policy.polarization_registry()",
    )
    if (
        not all(binding in preparation for binding in preparation_policy_bindings)
        or not all(binding in stencil for binding in stencil_policy_bindings)
        or "ModelReprojectionPolicy::canonical()" in reconstruction
    ):
        raise ArchitectureError(
            "T28 reconstruction execution does not consume the exact lifecycle reprojection policy"
        )

    executable_fields = rust_struct_fields(
        reconstruction, "ExecutableModelProblem", reconstruction_path
    )
    if executable_fields != {
        "problem": "CompiledProblem",
        "prepared": "Option<PreparedReprojectedSeed>",
    }:
        raise ArchitectureError("T28 executable problem brand is not reconstruction-owned")
    direct_brand = re.sub(
        r"\s+",
        "",
        rust_impl_method_body(
            reconstruction, "ExecutableModelProblem", "from_compiled", reconstruction_path
        ),
    )
    prepared_brand = re.sub(
        r"\s+", "", rust_function_body(reconstruction, "bind_compiled_problem", reconstruction_path)
    )
    compact_reconstruction = re.sub(r"\s+", "", reconstruction)
    if (
        "ModelInputCommitment::ReprojectedSeed(_)" not in direct_brand
        or "Err(ModelLifecycleError::OwnerPreparationRequired)" not in direct_brand
        or "pubfnbind_compiled_problem(self,problem:CompiledProblem,)->Result<ExecutableModelProblem,ModelLifecycleError>"
        not in compact_reconstruction
        or "prepared:Some(self)" not in prepared_brand
    ):
        raise ArchitectureError("T28 executable brand bypasses reconstruction preparation")
    proof = re.sub(
        r"\s+", "", rust_function_body(reconstruction, "reprojected_seed_proof_identity", reconstruction_path)
    )
    if "encoder.identity(samples.as_bytes());" not in proof or "encoder.identity(stencil.as_bytes());" not in proof:
        raise ArchitectureError("T28 reprojected proof omits samples or ordered stencils")

    lifecycle_projection = rust_struct_fields(receipt, "ModelLifecycleProjection", receipt_path)
    if lifecycle_projection.get("reprojection") != "ModelReprojectionProjection":
        raise ArchitectureError("T28 receipt does not project the reprojection contract")
    reprojection_fields = rust_struct_fields(receipt, "ModelReprojectionProjection", receipt_path)
    expected_fields = {
        "identity",
        "product_graph_identity",
        "numerics_identity",
        "conversion_precision",
        "direction_registry",
        "basis_registry",
        "polarization_registry",
        "invalid_contributor_policy",
        "uncovered_target_policy",
    }
    if set(reprojection_fields) != expected_fields:
        raise ArchitectureError(
            "T28 receipt reprojection projection fields differ from the closed contract"
        )
    if "ModelReprojectionProjection::new(problem)" not in receipt:
        raise ArchitectureError("T28 receipt does not populate the reprojection projection")
    if "let policy = contract.reprojection_policy();" not in receipt:
        raise ArchitectureError("T28 receipt does not consume the compiler-owned reprojection policy")
    if "let policy = ModelReprojectionPolicy::canonical();" not in receipt:
        raise ArchitectureError("T28 receipt validation does not use the canonical reprojection policy")
    receipt_validation = re.sub(
        r"\s+",
        "",
        rust_impl_method_body(
            receipt, "ModelReprojectionProjection", "validate", receipt_path
        ),
    )
    required_identity_validation = (
        "self.product_graph_identity==product_graph_identity",
        "self.numerics_identity==numerics_identity",
        "self.conversion_precision==numeric_precision(conversion_precision)",
        "validate_model_reprojection_contract_identity(LogicalIdentity::from_sha256(parse_digest(&self.identity)),LogicalIdentity::from_sha256(parse_digest(product_graph_identity)),LogicalIdentity::from_sha256(parse_digest(numerics_identity)),conversion_precision,policy,)",
    )
    if not all(binding in receipt_validation for binding in required_identity_validation):
        raise ArchitectureError(
            "T28 receipt validation does not recompute the cross-bound reprojection identity"
        )
    lifecycle_receipt_validation = re.sub(
        r"\s+",
        "",
        rust_impl_method_body(receipt, "ModelLifecycleProjection", "validate", receipt_path),
    )
    if not all(
        binding in lifecycle_receipt_validation
        for binding in (
            "is_nonzero_digest(&self.identity)",
            "validate_model_lifecycle_contract_identity(",
            "&self.input.identity(),",
        )
    ):
        raise ArchitectureError(
            "T28 receipt does not canonically revalidate lifecycle input evidence"
        )
    if not all(
        field in receipt
        for field in (
            "sample_identity: String",
            "stencil_identity: String",
            "proof_identity: String",
            "validate_reprojected_seed_proof_identity(",
            "validate_compiled_problem_identity(",
        )
    ):
        raise ArchitectureError("T28 receipt omits canonical model proof closure")
    for tag in (
        "same_tangent_plane_affine_bilinear_v1",
        "exact_spectral_v1",
        "real_parallel_hands_v1",
        "invalidate_target",
    ):
        if tag in receipt:
            raise ArchitectureError("T28 receipt duplicates a model-owned reprojection policy tag")
    projection = rust_function_body(receipt, "project_model_lifecycle", receipt_path)
    required_audit_fields = (
        "model_lifecycle.reprojection.identity",
        "model_lifecycle.reprojection.product_graph_identity",
        "model_lifecycle.reprojection.numerics_identity",
        "model_lifecycle.reprojection.conversion_precision",
        "model_lifecycle.reprojection.direction_registry",
        "model_lifecycle.reprojection.basis_registry",
        "model_lifecycle.reprojection.polarization_registry",
        "model_lifecycle.reprojection.invalid_contributor_policy",
        "model_lifecycle.reprojection.uncovered_target_policy",
    )
    if not all(field in projection for field in required_audit_fields):
        raise ArchitectureError("T28 receipt projection omits reprojection audit evidence")


def validate_t18_global_weighting_transfer(rows: list[dict[str, Any]]) -> None:
    row = next(
        (item for item in rows if item.get("id") == "capability.global-weighting"),
        None,
    )
    if row is None or row.get("status") != "Native":
        raise ArchitectureError("T18 must leave capability.global-weighting Native")
    required_evidence = {
        "crates/casa-imaging-model/src/measurement_equation.rs::pub struct WeightingOperatorContract",
        "crates/casa-imaging-model/src/selected_observation_sample.rs::pub struct SelectedSpectralContribution",
        "crates/casa-imaging-model/src/selected_observation_sample.rs::pub struct SelectedSpectralContributions",
        "crates/casa-ms/src/derived/engine.rs::pub(crate) fn spectral_frame_explicit",
        "crates/casa-ms/src/spectral_selection.rs::pub(crate) fn convert_frequency_to_frame_with_frames",
        "crates/casa-ms/src/selected_observation/spectral_evaluation.rs::pub struct SelectedObservationTraversalSample",
        "crates/casa-ms/src/selected_observation/bound_observation.rs::pub fn traverse",
        "crates/casa-imaging-reconstruction/src/weighting.rs::pub fn plan_weighting",
        "crates/casa-imaging-reconstruction/src/weighting.rs::pub fn begin_natural_weighting_stream",
        "crates/casa-imaging-reconstruction/src/weighting.rs::pub fn begin_weighting_generation",
        "crates/casa-imaging-reconstruction/src/weighting.rs::pub struct FusedWeightingPhase",
        "crates/casa-imaging-runtime/src/execution.rs::pub enum ClaimLifetime",
        "crates/casa-imaging-runtime/src/execution.rs::fn begin_draining",
        "crates/casa-imaging-runtime/src/execution.rs::fn validate_retained_claims",
        "crates/casa-imaging-runtime/src/observation_transaction.rs::fn derive_observation_reads",
        "crates/casa-imaging-runtime/src/resource_authority.rs::pub(crate) fn quarantine_external_permits",
        "crates/casa-imaging-runtime/src/weighting.rs::pub struct SelectedObservationSourceResources",
        "crates/casa-imaging-runtime/src/weighting.rs::pub struct WeightingPlanFragment",
        "crates/casa-imaging-runtime/src/weighting.rs::pub struct WeightingExecutionState",
        "crates/casa-imaging-runtime/src/weighting.rs::pub fn traverse_density_source",
        "crates/casa-imaging-runtime/src/weighting.rs::pub(crate) fn traverse_initial_bounded_stream",
        "crates/casa-imaging-runtime/src/weighting.rs::pub(crate) fn traverse_reuse_bounded_stream",
        "crates/casa-imaging-runtime/src/weighting.rs::pub struct FrozenWeightingArtifact",
        "crates/casa-imaging-runtime/src/weighting.rs::pub fn release",
        "crates/casa-imaging-runtime/src/weighting.rs::pub struct WeightedObservationBlock",
        "crates/casa-imaging-runtime/src/weighting.rs::pub struct WeightingReplayCompletion",
        "crates/casa-imaging-runtime/src/execution_bindings.rs::pub struct WorkExecutionContext",
        "crates/casa-imaging-runtime/src/execution_bindings.rs::pub const fn is_cleanup",
        "crates/casa-imaging-runtime/src/receipt.rs::pub fn allocation_generation_identities",
        "crates/casa-imaging-runtime/src/receipt.rs::pub fn allocation_uses",
        "crates/casa-imaging-runtime/src/receipt.rs::fn claim_lifetime",
        "crates/casa-imaging-runtime/src/receipt.rs::fn project_weighting",
    }
    required_baselines = {
        "repo://crates/casa-imaging-model/src/measurement_equation.rs",
        "repo://crates/casa-imaging-model/src/selected_observation_sample.rs",
        "repo://crates/casa-ms/src/derived/engine.rs",
        "repo://crates/casa-ms/src/spectral_selection.rs",
        "repo://crates/casa-ms/src/selected_observation/access.rs",
        "repo://crates/casa-ms/src/selected_observation/spectral_evaluation.rs",
        "repo://crates/casa-ms/src/selected_observation/bound_observation.rs",
        "repo://crates/casa-imaging-reconstruction/src/lib.rs",
        "repo://crates/casa-imaging-reconstruction/src/weighting.rs",
        "repo://crates/casa-imaging-runtime/src/lib.rs",
        "repo://crates/casa-imaging-runtime/src/execution.rs",
        "repo://crates/casa-imaging-runtime/src/execution_bindings.rs",
        "repo://crates/casa-imaging-runtime/src/observation_transaction.rs",
        "repo://crates/casa-imaging-runtime/src/resource_authority.rs",
        "repo://crates/casa-imaging-runtime/src/weighting.rs",
        "repo://crates/casa-imaging-runtime/src/receipt.rs",
    }
    if not required_evidence.issubset(set(row.get("source_evidence", []))):
        raise ArchitectureError("T18 lacks the accepted weighting-owner source evidence")
    if not required_baselines.issubset(set(row.get("baseline_manifests", []))):
        raise ArchitectureError("T18 lacks pinned weighting-owner baseline evidence")

    model_path = REPO_ROOT / "crates/casa-imaging-model/src/measurement_equation.rs"
    sample_model_path = (
        REPO_ROOT / "crates/casa-imaging-model/src/selected_observation_sample.rs"
    )
    traversal_sample_path = (
        REPO_ROOT / "crates/casa-ms/src/selected_observation/spectral_evaluation.rs"
    )
    spectral_engine_path = REPO_ROOT / "crates/casa-ms/src/derived/engine.rs"
    spectral_selection_path = REPO_ROOT / "crates/casa-ms/src/spectral_selection.rs"
    bound_observation_path = (
        REPO_ROOT / "crates/casa-ms/src/selected_observation/bound_observation.rs"
    )
    weighting_path = REPO_ROOT / "crates/casa-imaging-reconstruction/src/weighting.rs"
    runtime_weighting_path = REPO_ROOT / "crates/casa-imaging-runtime/src/weighting.rs"
    runtime_execution_path = REPO_ROOT / "crates/casa-imaging-runtime/src/execution.rs"
    receipt_path = REPO_ROOT / "crates/casa-imaging-runtime/src/receipt.rs"
    try:
        model = model_path.read_text(encoding="utf-8")
        sample_model = sample_model_path.read_text(encoding="utf-8")
        traversal_sample = traversal_sample_path.read_text(encoding="utf-8")
        spectral_engine = spectral_engine_path.read_text(encoding="utf-8")
        spectral_selection = spectral_selection_path.read_text(encoding="utf-8")
        bound_observation = bound_observation_path.read_text(encoding="utf-8")
        weighting = weighting_path.read_text(encoding="utf-8")
        runtime_weighting = runtime_weighting_path.read_text(encoding="utf-8")
        runtime_execution = runtime_execution_path.read_text(encoding="utf-8")
        receipt = receipt_path.read_text(encoding="utf-8")
    except OSError as error:
        raise ArchitectureError(f"cannot inspect T18 weighting sources: {error}") from error
    validate_t18_global_weighting_sources(
        model,
        sample_model,
        traversal_sample,
        bound_observation,
        weighting,
        runtime_weighting,
        receipt,
        spectral_engine=spectral_engine,
        spectral_selection=spectral_selection,
        runtime_execution=runtime_execution,
        model_path=model_path,
        sample_model_path=sample_model_path,
        traversal_sample_path=traversal_sample_path,
        bound_observation_path=bound_observation_path,
        weighting_path=weighting_path,
        runtime_weighting_path=runtime_weighting_path,
        receipt_path=receipt_path,
        spectral_engine_path=spectral_engine_path,
        spectral_selection_path=spectral_selection_path,
        runtime_execution_path=runtime_execution_path,
    )


def validate_t18_global_weighting_sources(
    model: str,
    sample_model: str,
    traversal_sample: str,
    bound_observation: str,
    weighting: str,
    runtime_weighting: str,
    receipt: str,
    *,
    spectral_engine: str | None = None,
    spectral_selection: str | None = None,
    runtime_execution: str | None = None,
    model_path: Path,
    sample_model_path: Path,
    traversal_sample_path: Path,
    bound_observation_path: Path,
    weighting_path: Path,
    runtime_weighting_path: Path,
    receipt_path: Path,
    spectral_engine_path: Path | None = None,
    spectral_selection_path: Path | None = None,
    runtime_execution_path: Path | None = None,
) -> None:
    spectral_engine_path = spectral_engine_path or (
        REPO_ROOT / "crates/casa-ms/src/derived/engine.rs"
    )
    spectral_selection_path = spectral_selection_path or (
        REPO_ROOT / "crates/casa-ms/src/spectral_selection.rs"
    )
    runtime_execution_path = runtime_execution_path or (
        REPO_ROOT / "crates/casa-imaging-runtime/src/execution.rs"
    )
    spectral_engine = spectral_engine or spectral_engine_path.read_text(encoding="utf-8")
    spectral_selection = spectral_selection or spectral_selection_path.read_text(
        encoding="utf-8"
    )
    runtime_execution = runtime_execution or runtime_execution_path.read_text(
        encoding="utf-8"
    )
    commitment = re.sub(
        r"\s+", "", rust_function_body(model, "weighting_commitment_id", model_path)
    )
    required_commitment_inputs = (
        "snapshot.as_bytes()",
        "geometry.as_bytes()",
        "sampling",
        "numerics.as_bytes()",
        "weighting.scheme()",
        "weighting.density_scope()",
        "weighting.uv_taper()",
    )
    forbidden_physical_inputs = ("block", "worker", "replay", "backend", "resource", "partition")
    if not all(binding in commitment for binding in required_commitment_inputs) or any(
        binding in commitment.lower() for binding in forbidden_physical_inputs
    ):
        raise ArchitectureError(
            "T18 compiler commitment does not isolate logical weighting from physical execution"
        )

    contribution_fields = rust_struct_fields(
        sample_model, "SelectedSpectralContribution", sample_model_path
    )
    contribution_set_fields = rust_struct_fields(
        sample_model, "SelectedSpectralContributions", sample_model_path
    )
    evaluation_fields = rust_struct_fields(
        sample_model, "SelectedSpectralEvaluation", sample_model_path
    )
    selected_sample_fields = rust_struct_fields(
        sample_model, "SelectedObservationSample", sample_model_path
    )
    if contribution_fields != {
        "output_channel": "u32",
        "factor": "f64",
        "evaluation_frequency_hz": "f64",
    } or (
        contribution_set_fields
        != {"entries": "SmallVec<[SelectedSpectralContribution;4]>"}
    ) or evaluation_fields != {
        "native": "SelectedSpectralInterval",
        "output_frame": "SelectedSpectralInterval",
        "effective_weight": "f64",
        "valid": "bool",
    }:
        raise ArchitectureError(
            "T18/T36 spectral trace and sparse coefficients differ from the accepted paired model"
        )
    if (
        "spectral_contributions" in selected_sample_fields
        or "pub const SCHEMA_VERSION: u32 = 3;" not in sample_model
    ):
        raise ArchitectureError(
            "T18 spectral contributions must remain outside the persisted selected-sample schema"
        )

    traversal_fields = rust_struct_fields(
        traversal_sample, "SelectedObservationTraversalSample", traversal_sample_path
    )
    if traversal_fields != {
        "sample": "SelectedObservationSample",
        "spectral_evaluation": "SelectedSpectralEvaluation",
    }:
        raise ArchitectureError(
            "T18/T36 traversal envelope omits storage-owner spectral evaluation"
        )
    if re.search(
        r"\bpub\s+(?:const\s+)?fn\s+(?:new|from_owner)\b", traversal_sample
    ):
        raise ArchitectureError("T18 traversal envelope construction must remain owner-only")
    derivation = rust_function_body(
        traversal_sample, "derive_spectral_evaluation_cached", traversal_sample_path
    )
    evaluation = rust_function_body(
        traversal_sample, "evaluated_frequency_hz_cached", traversal_sample_path
    )
    compact_evaluation = re.sub(r"\s+", "", evaluation)
    explicit_output_frame = rust_function_body(
        spectral_engine, "spectral_frame_explicit", spectral_engine_path
    )
    two_frame_conversion = rust_function_body(
        spectral_selection,
        "convert_frequency_to_frame_with_frames",
        spectral_selection_path,
    )
    spectral_sampling_path = (
        REPO_ROOT / "crates/casa-imaging-reconstruction/src/spectral_sampling.rs"
    )
    spectral_sampling = spectral_sampling_path.read_text(encoding="utf-8")
    stencil = rust_function_body(
        spectral_sampling, "compile_spectral_stencil", spectral_sampling_path
    )
    channel_local_stencil = rust_function_body(
        spectral_sampling, "channel_local_terms", spectral_sampling_path
    )
    if (
        "convert_frequency_to_frame_with_frames(" not in evaluation
        or "native.centre_hz()" not in derivation
        or "native_boundaries[0]" not in derivation
        or "native_boundaries[1]" not in derivation
        or "spectral.anchor()" not in evaluation
        or "spectral_frame_observatory(" not in evaluation
        or "spectral_frame_explicit(" not in evaluation
        or "sample.coordinates.time.mjd_days()" not in evaluation
        or "sample.metadata.field_id" not in evaluation
        or "source_frame" not in evaluation
        or "output_frame" not in evaluation
        or "Some(source_frame),Some(output_frame)" not in compact_evaluation
        or not all(
            token in explicit_output_frame
            for token in ("with_epoch(", "with_position(", "with_direction(", "with_measures(")
        )
        or "source_frame" not in two_frame_conversion
        or "target_frame" not in two_frame_conversion
        or "direct_frequency_hop_uses_target_frame(" not in two_frame_conversion
        or "channel_local_terms(" not in stencil
        or "SpectralKernel::Cubic" not in channel_local_stencil
        or "SpectralKernel::ChannelIntegration" not in channel_local_stencil
        or "compile_spectral_stencil(" not in runtime_weighting
        or "reported.spectral_evaluation()" not in runtime_weighting
        or "spectral_contributions" in traversal_fields
    ):
        raise ArchitectureError(
            "T18/T36 storage evaluation or reconstruction-owned paired sampling is bypassed"
        )
    traversal = rust_impl_method_body(
        bound_observation, "BoundSelectedObservation", "traverse", bound_observation_path
    )
    if (
        "SpectralEvaluationProjector::new()" not in traversal
        or "spectral_evaluator.project(" not in traversal
        or "source.geometry_engine()" not in traversal
        or "consume_projected_validated_stream(" not in traversal
    ):
        raise ArchitectureError(
            "T18 traversal does not issue its envelope after owner validation"
        )

    frozen_fields = rust_struct_fields(
        runtime_weighting, "FrozenWeightingGeneration", runtime_weighting_path
    )
    artifact_fields = rust_struct_fields(
        runtime_weighting, "FrozenWeightingArtifact", runtime_weighting_path
    )
    if (
        frozen_fields
        != {
            "artifact": "FrozenWeightingArtifact",
            "binding": "WeightingGenerationBinding",
        }
        or artifact_fields
        != {
            "state": "Arc<WeightingAlgorithmState>",
            "source_generation": "SelectedObservationGenerationId",
            "source_sample_count": "u64",
            "continuum_transform": "Option<ContinuumTransformCompletion>",
            "cross_plan_reservation": "Option<Arc<FrozenWeightingReservation>>",
        }
    ):
        raise ArchitectureError(
            "global weighting must retain immutable reconstruction state with exact T17 generation evidence"
        )
    execution_state_fields = rust_struct_fields(
        runtime_weighting, "WeightingExecutionState", runtime_weighting_path
    )
    compact_runtime = re.sub(r"\s+", "", runtime_weighting)
    if (
        execution_state_fields
        != {
            "phase": "WeightingExecutionPhase",
            "retained_observation": "Option<RetainedWeightingObservation>",
            "density": "Option<WeightingDensityPhase>",
            "imported": "Option<FrozenWeightingArtifact>",
            "latest_traversal_measurements": "Option<SelectedObservationTraversalMeasurements>",
            "latest_stream_measurements": "Option<BoundedStreamMeasurements>",
        }
        or "pubfntraverse_density_source(" not in compact_runtime
        or "pub(crate)fntraverse_initial_bounded_stream<" not in compact_runtime
        or "pub(crate)fntraverse_reuse_bounded_stream<" not in compact_runtime
        or "pubfnwith_frozen_artifact(" not in compact_runtime
    ):
        raise ArchitectureError(
            "global weighting must expose one opaque fused-stream lifecycle with frozen reuse"
        )
    density = rust_function_body(
        runtime_weighting, "traverse_density_source", runtime_weighting_path
    )
    initial_stream = rust_function_body(
        runtime_weighting, "traverse_initial_bounded_stream", runtime_weighting_path
    )
    reuse_stream = rust_function_body(
        runtime_weighting, "traverse_reuse_bounded_stream", runtime_weighting_path
    )
    bounded_stream = rust_function_body(
        runtime_weighting, "execute_weighting_block_stream", runtime_weighting_path
    )
    if (
        density.count("execute_bounded(") != 1
        or density.count("selected.into_block_stream(problem)") != 1
        or "begin_weighting_generation(" not in density
    ):
        raise ArchitectureError(
            "density-dependent weighting must use exactly one bounded density prepass"
        )
    if (
        initial_stream.count("execute_weighting_block_stream(") != 1
        or "begin_natural_weighting_stream(" not in initial_stream
        or "finish_into_stream(" not in initial_stream
        or reuse_stream.count("execute_weighting_block_stream(") != 1
        or ".begin_replay(" not in reuse_stream
        or bounded_stream.count("execute_bounded(") != 1
        or bounded_stream.count("selected.into_block_stream(problem)") != 1
        or ".complete(terminal)" not in bounded_stream
    ):
        raise ArchitectureError(
            "initial and later majors must each use the shared bounded terminal selected-payload traversal"
        )
    replay = rust_impl_method_body(
        runtime_weighting, "FrozenWeightingGeneration", "replay", runtime_weighting_path
    )
    if (
        replay.count(".traverse(") != 1
        or ".finish(" not in replay
        or ".authorize_replay(" not in replay
        or "context:WorkExecutionContext<'_>" not in compact_runtime
        or "fragment:&WeightingPlanFragment<'_>" not in compact_runtime
    ):
        raise ArchitectureError(
            "T18 replay must require predecessor authority, its own exhaustive traversal, and exact coverage"
        )
    if "IntoIterator" in weighting or "inspect_selected_observation(" in weighting:
        raise ArchitectureError("T18 reconstruction exposes a bypass around T17 callback traversal")
    if "SelectedObservationGenerationId" in weighting:
        raise ArchitectureError("T18 reconstruction accepts caller-authored T17 completion identity")
    replay_completion = rust_struct_fields(
        runtime_weighting, "WeightingReplayCompletion", runtime_weighting_path
    )
    replay_binding = rust_function_body(
        runtime_weighting, "bind", runtime_weighting_path
    )
    if (
        replay_completion.get("selected_generation")
        != "SelectedObservationGenerationId"
        or replay_completion.get("sample_count") != "u64"
        or replay_completion.get("binding") != "WeightingGenerationBinding"
        or "context.bind(self.owner_completion)" not in re.sub(r"\s+", "", replay_binding)
        or "(WeightingReplayCompletion,AttemptBoundObservationCompletion)"
        not in compact_runtime
    ):
        raise ArchitectureError(
            "T18 replay completion does not bind opaque T17 evidence while returning the scheduler predecessor"
        )
    if re.search(
        r"\bpub\s+(?:const\s+)?fn\s+(?:from_sha256|from_identity|new_generation)\b",
        weighting,
    ):
        raise ArchitectureError("T18 exposes a raw weighting evidence constructor")
    block_fields = rust_struct_fields(
        runtime_weighting, "WeightedObservationBlock", runtime_weighting_path
    )
    replay_chunk_fields = rust_struct_fields(
        weighting, "WeightingReplayChunk", weighting_path
    )
    replay_input_fields = rust_struct_fields(
        weighting, "WeightingReplayInputSample", weighting_path
    )
    replay_phase_fields = rust_struct_fields(
        weighting, "WeightingReplayPhase", weighting_path
    )
    sample_fields = rust_struct_fields(
        runtime_weighting, "WeightedObservationSample", runtime_weighting_path
    )
    replay_consume = rust_impl_method_body(
        weighting, "WeightingReplayPhase<'_>", "consume", weighting_path
    )
    take_block = rust_function_body(weighting, "take_input_block", weighting_path)
    if (
        block_fields.get("generation") != "WeightingGenerationId"
        or block_fields.get("block") != "ReconstructionWeightedBlock"
        or replay_chunk_fields.get("samples") != "Vec<WeightingSampleValue>"
        or replay_input_fields
        != {
            "sample": "SelectedObservationSample",
            "contributions": "SelectedSpectralContributions",
        }
        or replay_phase_fields.get("input") != "Vec<WeightingReplayInputSample>"
        or replay_phase_fields.get("block") != "Vec<WeightingSampleValue>"
        or sample_fields.get("generation") != "WeightingGenerationId"
        or "into_boxed_slice" in take_block
        or "self.input.push(WeightingReplayInputSample" not in replay_consume
        or "for input in &self.input" not in take_block
        or "self.input.clear()" not in take_block
        or "std::mem::take(&mut self.block)" not in take_block
        or "Vec::with_capacity(self.max_block_samples)" not in weighting
        or "size_of::<WeightingReplayInputSample>()" not in weighting
    ):
        raise ArchitectureError(
            "T18 weighted replay does not carry one opaque W generation through real bounded input and output blocks"
        )
    residency_fields = rust_struct_fields(weighting, "WeightingResidency", weighting_path)
    required_residency = {
        "density_grid_bytes",
        "robust_factor_bytes",
        "sum_weight_bytes",
        "deterministic_partial_bytes",
        "reduction_scratch_bytes",
        "replay_read_bytes",
        "weighted_block_bytes",
        "simultaneous_selected_weighted_bytes",
        "peak_bytes",
    }
    if not required_residency.issubset(residency_fields) or any(
        "queue" in field for field in residency_fields
    ):
        raise ArchitectureError("T18 residency omits a weighting-owned buffer class")

    fragment_fields = rust_struct_fields(
        runtime_weighting, "WeightingPlanFragment", runtime_weighting_path
    )
    source_resource_fields = rust_struct_fields(
        runtime_weighting,
        "SelectedObservationSourceResources",
        runtime_weighting_path,
    )
    residency_certificate_fields = rust_struct_fields(
        bound_observation,
        "SelectedObservationResidencyCertificate",
        bound_observation_path,
    )
    residency_certificate_mint = re.sub(
        r"\s+",
        "",
        rust_impl_method_body(
            bound_observation,
            "SelectedObservationResidencyCertificate",
            "mint",
            bound_observation_path,
        ),
    )
    bound_observation_open = re.sub(
        r"\s+",
        "",
        rust_impl_method_body(
            bound_observation,
            "BoundSelectedObservation",
            "open",
            bound_observation_path,
        ),
    )
    compose = rust_function_body(runtime_weighting, "compose_legacy", runtime_weighting_path)
    compact_compose = re.sub(r"\s+", "", compose)
    compose_streaming = rust_function_body(
        runtime_weighting, "compose_streaming", runtime_weighting_path
    )
    allocation_specs = rust_function_body(
        runtime_weighting, "allocation_specs", runtime_weighting_path
    )
    generation_authority = rust_function_body(
        runtime_weighting, "authorize_generation", runtime_weighting_path
    )
    replay_authority = rust_function_body(
        runtime_weighting, "authorize_replay", runtime_weighting_path
    )
    release = rust_impl_method_body(
        runtime_weighting, "WeightingExecutionState", "release", runtime_weighting_path
    )
    source_authority = re.sub(
        r"\s+",
        "",
        rust_function_body(
            runtime_weighting, "authorize_source_observation", runtime_weighting_path
        ),
    )
    source_traversal = re.sub(
        r"\s+",
        "",
        rust_impl_method_body(
            runtime_weighting,
            "WeightingExecutionState",
            "traverse_and_retain_source",
            runtime_weighting_path,
        ),
    )
    source_preflight_position = source_traversal.find(
        "fragment.authorize_source_observation("
    )
    first_sample_position = source_traversal.find("selected.traverse(problem,consume)")
    source_contract = rust_impl_method_body(
        runtime_weighting,
        "SourceTraversalContract",
        "from_source",
        runtime_weighting_path,
    )
    queue_demand = rust_function_body(
        runtime_weighting, "queue_demand_covers", runtime_weighting_path
    )
    drain = rust_function_body(
        runtime_execution, "begin_draining", runtime_execution_path
    )
    retained_validation = rust_function_body(
        runtime_execution, "validate_retained_claims", runtime_execution_path
    )
    retained_completion = rust_function_body(
        runtime_execution, "complete_retained_event", runtime_execution_path
    )
    finish_work = rust_function_body(
        runtime_execution, "finish_work", runtime_execution_path
    )
    finish_draining = rust_function_body(
        runtime_execution, "finish_draining", runtime_execution_path
    )
    if (
        fragment_fields.get("plan") != "&'aWeightingPlan"
        or fragment_fields.get("source_read") != "WorkNodeId"
        or fragment_fields.get("source_resources")
        != "SelectedObservationSourceResources"
        or source_resource_fields.get("residency")
        != "SelectedObservationResidencyCertificate"
        or source_resource_fields.get("allocations") != "BTreeSet<AllocationId>"
        or source_resource_fields.get("queue") != "LeaseResource"
        or residency_certificate_fields.get("identity")
        != "BoundSelectedObservationIdentity"
        or residency_certificate_fields.get("sources")
        != "Vec<SelectedObservationSourceResidency>"
        or residency_certificate_fields.get("aggregate_resident_bytes") != "usize"
        or residency_certificate_fields.get("peak_live_blocks") != "usize"
        or "checked_add(content_budget.available_bytes())"
        not in residency_certificate_mint
        or "peak_live_blocks.max(content_budget.maximum_live_blocks())"
        not in residency_certificate_mint
        or "BoundSelectedObservationIdentity::from_problem(problem)"
        not in residency_certificate_mint
        or "letresidency=SelectedObservationResidencyCertificate::mint(problem,&bindings)?"
        not in bound_observation_open
        or "pubfnmint(" in re.sub(r"\s+", "", bound_observation)
        or "actual!=&self.source_resources.residency" not in source_authority
        or "!actual.matches_problem(problem)" not in source_authority
        or source_preflight_position < 0
        or first_sample_position < 0
        or source_preflight_position > first_sample_position
        or "selected.residency_certificate()" not in source_traversal
        or compose.count("kind: WorkKind::ObservationRead") != 2
        or "WeightingStreamingMode::DensityInitial" not in compose_streaming
        or "WeightingStreamingMode::NaturalInitial" not in compose_streaming
        or "WeightingStreamingMode::Reuse" not in compose_streaming
        or "removed.contains" not in compose_streaming
        or "terminal_fence" not in compose_streaming
        or "kind: WorkKind::Release" not in compose
        or allocation_specs.count("AllocationSpec::new(") != 6
        or "if let Some(bytes) = self.continuum_row_bytes" not in allocation_specs
        or '"continuum-transform-row"' not in allocation_specs
        or "predecessor_observation_completion(&self.source_read)"
        not in generation_authority
        or "predecessor_observation_completion(&self.ids.generation_node)"
        not in replay_authority
        or "validate_work_authority(" not in generation_authority
        or "validate_work_authority(" not in replay_authority
        or "fragment.authorize_release(context)" not in release
        or "context.is_cleanup()" not in release
        or "WeightingExecutionPhase::Empty" not in release
        or "LeaseResource::MeasurementSetLock" not in source_contract
        or "LeaseResource::FileDescriptors" not in source_contract
        or "ClaimLifetime::retained_until(release.clone())" not in source_contract
        or ".chain(source_contract.retained_claims.iter().cloned())" not in compose
        or ".chain(source_contract.release_buffer_claims.iter().cloned())"
        not in compose
        or "source_contract.retained_allocations" not in compact_compose
        or "BTreeSet::from([WorkDependency::Work(self.ids.release_node.clone())])"
        not in compose
        or "source_node.allocations.push(allocation_use(&self.ids.frozen_allocation,io_lifetime))"
        not in compact_compose
        or "self.source_read.clone()" not in allocation_specs
        or "&claim.resource == queue" not in source_contract
        or "claim.amount == required_blocks" not in source_contract
        or "residency.aggregate_resident_bytes()" not in source_contract
        or "residency.peak_live_blocks()" not in source_contract
        or "claimed_bytes != Some(expected_bytes)" not in source_contract
        or "allocated_bytes != Some(expected_bytes)" not in source_contract
        or "selected_content_allocations.is_empty()" not in source_contract
        or "&retained_allocations != selected_content_allocations"
        not in source_contract
        or "queue_demand_covers(" not in source_contract
        or not all(
            demand_kind in queue_demand
            for demand_kind in (
                "LeaseResource::Queue",
                "LeaseResource::StorageQueue",
                "LeaseResource::TransferQueue",
            )
        )
        or "self.has_external_release(allocation)" not in drain
        or "NodeState::CleanupPending" not in drain
        or "RetainedUntil(WorkNodeId)" not in runtime_execution
        or "WorkKind::Release" not in retained_validation
        or "event_strictly_precedes" not in retained_validation
        or "event_precedes" not in retained_validation
        or ".remove(&id)" not in retained_completion
        or ".permit" not in retained_completion
        or ".release()?" not in retained_completion
        or "self.complete_retained_event(&node_id)?" not in finish_work
        or "self.release_all_retained_permits()?" not in finish_draining
        or "quarantine_external_permits" not in finish_draining
    ):
        raise ArchitectureError(
            "T18 production fragment must own five allocations, exact queue authority, continuous retained-source authority, and fail-closed scheduler release"
        )

    plan_projection = rust_impl_method_body(receipt, "PlanProjection", "new", receipt_path)
    node_projection = rust_impl_method_body(receipt, "NodeProjection", "new", receipt_path)
    dag_validation = rust_function_body(
        receipt, "validate_receipt_execution_dag", receipt_path
    )
    if (
        "allocation_generations" not in plan_projection
        or "AllocationProjection::new" not in plan_projection
        or "allocation_uses" not in node_projection
        or "AllocationUseProjection" not in node_projection
        or "generation_identity" not in node_projection
        or "claim_lifetime" not in node_projection
        or "retained_until:" not in receipt
        or "ClaimLifetime::RetainedUntil" not in receipt
        or "receipt_execution_dag(&projection)?" not in plan_projection
        or "physical_work_id()" not in plan_projection
        or "receipt_execution_dag(plan)?" not in dag_validation
        or "hex(&dag.physical_work_id().as_bytes()) == plan.dag_identity"
        not in dag_validation
        or "has_redacted_identity" in receipt
    ):
        raise ArchitectureError(
            "T18 receipts must project weighting allocation generations and exact node uses"
        )
    projection = rust_function_body(receipt, "project_weighting", receipt_path)
    if (
        "weighting.commitment.identity" not in projection
        or "weighting.generation.identity" in projection
    ):
        raise ArchitectureError(
            "T18 receipt conflates compiler commitment with reconstruction generation evidence"
        )


def validate_t17_ms_selection_transfer(rows: list[dict[str, Any]]) -> None:
    row = next(
        (item for item in rows if item.get("id") == "capability.ms-selection"), None
    )
    if row is None or row.get("status") != "Native":
        raise ArchitectureError(
            "T17 must leave capability.ms-selection Native with no migration obligation"
        )
    required_evidence = {
        "crates/casa-imaging-model/src/selected_observation_sample.rs::SelectedObservationGenerationEncoder",
        "crates/casa-imaging-model/src/observation.rs::additional_retained_heap_bytes",
        "crates/casa-ms/src/selected_observation/access.rs::BoundObservationSource",
        "crates/casa-ms/src/selected_observation/bound_observation.rs::binding_graph_initialization_bytes",
        "crates/casa-ms/src/selected_observation/bound_observation.rs::source_slots_retained_bytes",
        "crates/casa-ms/src/selected_observation/content_plan.rs::shared_binding_graph_initialization_bytes",
        "crates/casa-ms/src/selected_observation/content_plan.rs::shared_source_slots_retained_bytes",
        "crates/casa-ms/src/selected_observation/measures.rs::SelectedObservationMeasures",
        "crates/casa-ms/src/selected_observation/measures.rs::provider_state",
        "crates/casa-ms/src/selected_observation/row_access.rs::visit_selected_observation_rows",
        "crates/casa-ms/src/derived/engine.rs::new_selected_observation",
        "crates/casa-measures-data/src/lib.rs::prepare_bounded_state",
        "crates/casa-measures-data/src/lib.rs::scientific_state_identity",
        "crates/casa-types/src/measures/provider.rs::MeasuresProviderState",
        "resources/imaging-architecture/dependency-policy.json::t17-selected-observation-provider-injection",
        "crates/casa-imaging-runtime/src/execution_bindings.rs::ObservationReadCompletionContext",
        "crates/casa-imaging-application/src/continuum_request.rs::fn prepare(",
    }
    if not required_evidence.issubset(set(row.get("source_evidence", []))):
        raise ArchitectureError(
            "capability.ms-selection lacks the accepted T17 traversal/resource/completion evidence"
        )
    required_baselines = {
        "repo://crates/casa-imaging-model/src/selected_observation_sample.rs",
        "repo://resources/imaging-architecture/baselines/selected-observation-generation-v4.txt",
    }
    if not required_baselines.issubset(set(row.get("baseline_manifests", []))):
        raise ArchitectureError(
            "capability.ms-selection lacks pinned T17 generation source and fixture evidence"
        )

    application_path = (
        REPO_ROOT / "crates/casa-imaging-application/src/continuum_request.rs"
    )
    access_path = REPO_ROOT / "crates/casa-ms/src/selected_observation/access.rs"
    bound_path = (
        REPO_ROOT / "crates/casa-ms/src/selected_observation/bound_observation.rs"
    )
    content_plan_path = (
        REPO_ROOT / "crates/casa-ms/src/selected_observation/content_plan.rs"
    )
    measures_path = REPO_ROOT / "crates/casa-ms/src/selected_observation/measures.rs"
    row_access_path = (
        REPO_ROOT / "crates/casa-ms/src/selected_observation/row_access.rs"
    )
    engine_path = REPO_ROOT / "crates/casa-ms/src/derived/engine.rs"
    provider_path = REPO_ROOT / "crates/casa-types/src/measures/provider.rs"
    measures_runtime_path = REPO_ROOT / "crates/casa-measures-data/src/lib.rs"
    observation_path = REPO_ROOT / "crates/casa-imaging-model/src/observation.rs"
    model_path = REPO_ROOT / "crates/casa-imaging-model/src/selected_observation.rs"
    compiled_problem_path = (
        REPO_ROOT / "crates/casa-imaging-model/src/compiled_problem.rs"
    )
    model_lib_path = REPO_ROOT / "crates/casa-imaging-model/src/lib.rs"
    runtime_path = REPO_ROOT / "crates/casa-imaging-runtime/src/execution_bindings.rs"
    try:
        application = application_path.read_text(encoding="utf-8")
        access = access_path.read_text(encoding="utf-8")
        bound = bound_path.read_text(encoding="utf-8")
        content_plan = content_plan_path.read_text(encoding="utf-8")
        measures = measures_path.read_text(encoding="utf-8")
        row_access = row_access_path.read_text(encoding="utf-8")
        engine = engine_path.read_text(encoding="utf-8")
        provider = provider_path.read_text(encoding="utf-8")
        measures_runtime = measures_runtime_path.read_text(encoding="utf-8")
        observation = observation_path.read_text(encoding="utf-8")
        model = model_path.read_text(encoding="utf-8")
        compiled_problem = compiled_problem_path.read_text(encoding="utf-8")
        model_lib = model_lib_path.read_text(encoding="utf-8")
        runtime = runtime_path.read_text(encoding="utf-8")
    except OSError as error:
        raise ArchitectureError(
            f"cannot inspect T17 transfer sources: {error}"
        ) from error

    forbidden_application_patterns = {
        r"\bMsSelection\b": "imaging application retains the displaced selection request",
        r"\bResolvedMsSelectionRow\b": "imaging application retains the displaced resolved-row contract",
        r"\.resolve_selection\s*\(": "imaging application can still reach displaced MS selection evaluation",
    }
    for pattern, message in forbidden_application_patterns.items():
        if re.search(pattern, application):
            raise ArchitectureError(message)
    application_preparation = rust_function_body(
        application, "prepare", application_path
    )
    if ".visit_selected_observation_rows(" not in application_preparation:
        raise ArchitectureError(
            "imaging application must delegate row evaluation to canonical selected-observation access"
        )
    if "fn validate_selected_rows(" in access:
        raise ArchitectureError(
            "selected-observation binding retains a hidden MAIN validation prepass"
        )
    frontend_projection = rust_function_body(
        row_access, "visit_selected_observation_rows", row_access_path
    )
    if (
        "CompiledRowPredicate::new(" not in frontend_projection
        or ".visit_main_row_selection_blocks(" not in frontend_projection
    ):
        raise ArchitectureError(
            "selected-row projection does not use the canonical bounded T17 predicate traversal"
        )
    if (
        ".main_row_selection_cursor(" not in access
        or ".row_predicate" not in access
        or "SelectedRowsBuilder::with_data_description_capacity(" not in access
        or "SelectedMainRow::new(" not in access
        or ".push(row)" not in access
        or ".ordered_main_rows()" in access
        or "struct SelectedMainRows" in access
    ):
        raise ArchitectureError(
            "retained selected-observation access must rebuild and validate the compact manifest during its sole bounded MAIN traversal"
        )
    compact_model = re.sub(r"\s+", "", model)
    compact_compiled_problem = re.sub(r"\s+", "", compiled_problem)
    if (
        "pubstructSelectedObservationInspection<'a>{" not in compact_model
        or "pub(crate)fnnew(" not in compact_model
        or "pubfnpush(" not in compact_model
        or "pubfnfinish(" not in compact_model
        or "pubfnbegin_selected_observation_inspection(" not in compact_compiled_problem
        or not re.search(r"\bSelectedObservationInspection\b", model_lib)
    ):
        raise ArchitectureError(
            "casa-imaging-model must expose only the opaque incremental validator needed by bounded source blocks"
        )
    validate_t17_selected_observation_resource_sources(
        measures,
        measures_path,
        bound,
        bound_path,
        access,
        access_path,
        content_plan,
        content_plan_path,
        engine,
        engine_path,
        provider,
        provider_path,
        measures_runtime,
        measures_runtime_path,
        observation,
        observation_path,
    )
    validate_t17_runtime_completion_source(runtime, runtime_path)


def validate_t17_selected_observation_resource_sources(
    measures: str,
    measures_path: Path,
    bound: str,
    bound_path: Path,
    access: str,
    access_path: Path,
    content_plan: str,
    content_plan_path: Path,
    engine: str,
    engine_path: Path,
    provider: str,
    provider_path: Path,
    measures_runtime: str,
    measures_runtime_path: Path,
    observation: str,
    observation_path: Path,
) -> None:
    measures_fields = rust_struct_fields(
        measures, "SelectedObservationMeasures", measures_path
    )
    if measures_fields != {
        "provider": "Arc<dynMeasuresProvider>",
        "provider_state": "MeasuresProviderState",
        "retained_bytes": "usize",
    }:
        raise ArchitectureError(
            "T17 selected-observation Measures capability must retain one provider-owned immutable state"
        )
    compact_measures = re.sub(r"\s+", "", measures)
    constructor = re.sub(r"\s+", "", rust_function_body(measures, "new", measures_path))
    validate_problem = re.sub(
        r"\s+", "", rust_function_body(measures, "validate_problem", measures_path)
    )
    verify_state = re.sub(
        r"\s+", "", rust_function_body(measures, "verify_state", measures_path)
    )
    if (
        "pubfnnew(provider:Arc<dynMeasuresProvider>,)" not in compact_measures
        or "identity:LogicalIdentity,provider:Arc<dynMeasuresProvider>"
        in compact_measures
        or "provider.prepare_bounded_state()" not in constructor
        or ".ok_or(SelectedObservationMeasuresError::UnaccountedProvider)?"
        not in constructor
        or "LogicalIdentity::from_sha256(self.provider_state.identity_sha256())"
        not in compact_measures
        or "ReferenceDataKind::Measures" not in validate_problem
        or "letactual=self.identity();" not in validate_problem
        or "ifactual!=expected" not in validate_problem
        or "self.verify_state()" not in validate_problem
        or "self.provider.prepare_bounded_state()" not in verify_state
        or "actual!=Some(self.provider_state)" not in verify_state
        or any(
            forbidden in compact_measures
            for forbidden in (
                "open_measures_runtime(",
                "open_discovered(",
                "MeasuresRuntime",
            )
        )
    ):
        raise ArchitectureError(
            "T17 selected-observation Measures capability must acquire and recheck provider-owned identity and residency"
        )
    arc_allocation = re.sub(
        r"\s+", "", rust_function_body(measures, "arc_allocation_bytes", measures_path)
    )
    if (
        "arc_allocation_bytes(provider.as_ref())" not in constructor
        or ".checked_add(provider_state.retained_heap_bytes())" not in constructor
        or "Layout::array::<AtomicUsize>(2)" not in arc_allocation
        or ".extend(Layout::for_value(provider))" not in arc_allocation
        or ".pad_to_align().size()" not in arc_allocation
    ):
        raise ArchitectureError(
            "T17 selected-observation provider allocation must remain alignment-aware and exactly charged"
        )

    provider_preparation = re.sub(
        r"\s+",
        "",
        rust_function_body(provider, "prepare_bounded_state", provider_path),
    )
    if provider_preparation != "Ok(None)":
        raise ArchitectureError(
            "T17 MeasuresProvider bounded state must default to opaque and require explicit provider preparation"
        )

    retained_catalogs = (
        "eop",
        "observatories",
        "sources",
        "spectral_lines",
        "tai_utc",
        "igrf",
    )
    runtime_preparation = re.sub(
        r"\s+",
        "",
        rust_function_body(
            measures_runtime, "build_bounded_state", measures_runtime_path
        ),
    )
    runtime_state_acquisition = re.sub(
        r"\s+",
        "",
        rust_function_body(
            measures_runtime, "prepare_bounded_state", measures_runtime_path
        ),
    )
    runtime_identity = re.sub(
        r"\s+",
        "",
        rust_function_body(
            measures_runtime, "scientific_state_identity", measures_runtime_path
        ),
    )
    compact_runtime = re.sub(r"\s+", "", measures_runtime)
    runtime_declaration = re.search(
        r"pub\s+struct\s+MeasuresRuntime\s*\{(?P<body>.*?)^\}",
        measures_runtime,
        flags=re.MULTILINE | re.DOTALL,
    )
    runtime_cache_fields = (
        set(
            re.findall(
                r"(?m)^\s*([a-z][A-Za-z0-9_]*)\s*:\s*OnceLock<",
                runtime_declaration.group("body"),
            )
        )
        if runtime_declaration is not None
        else set()
    )
    if runtime_cache_fields != {*retained_catalogs, "bounded_state"} or any(
        f"{catalog}:OnceLock<Result<" not in compact_runtime
        or f"let{catalog}=self.{catalog}()?;" not in runtime_preparation
        or f"{catalog}.retained_heap_bytes()" not in runtime_preparation
        for catalog in retained_catalogs
    ):
        raise ArchitectureError(
            "T17 MeasuresRuntime must eagerly stabilize and account every retained catalog"
        )
    identity_fragments = {
        "state.boolean(provenance.is_some());",
        "state.sequence_len(eop.entries.len())?;",
        "state.sequence_len(observatories.entries().len())?;",
        "state.sequence_len(sources.entries().len())?;",
        "state.sequence_len(spectral_lines.entries().len())?;",
        "state.sequence_len(tai_utc.entries.len())?;",
        "state.sequence_len(igrf.years.len())?;",
        "state.sequence_len(igrf.coeffs_by_year.len())?;",
        "state.sequence_len(igrf.secular_variation.len())?;",
        "state.sequence_len(igrf.nmax)?;",
    }
    if (
        "self.bounded_state.get_or_init(" not in runtime_state_acquisition
        or "self.build_bounded_state()" not in runtime_state_acquisition
        or "scientific_state_identity(self.provenance.as_ref(),eop,observatories,sources,spectral_lines,tai_utc,igrf,)?"
        not in runtime_preparation
        or any(fragment not in runtime_identity for fragment in identity_fragments)
        or "MeasuresProviderState::new(identity_sha256,retained_heap_bytes,)"
        not in runtime_preparation
        or "implMeasuresProviderforMeasuresRuntime{" not in compact_runtime
        or "MeasuresRuntime::prepare_bounded_state(self).map(Some)"
        not in compact_runtime
    ):
        raise ArchitectureError(
            "T17 MeasuresRuntime must own one canonical scientific state identity and bounded provider contract"
        )

    source_state_projection = re.sub(
        r"\s+",
        "",
        rust_function_body(
            observation, "additional_retained_heap_bytes", observation_path
        ),
    )
    selected_rows_projection = re.sub(
        r"\s+",
        "",
        rust_function_body(
            observation, "additional_retained_manifest_bytes", observation_path
        ),
    )
    generation_projection = re.sub(
        r"\s+",
        "",
        rust_function_body(observation, "retained_owned_heap_bytes", observation_path),
    )
    compact_observation = re.sub(r"\s+", "", observation)
    if (
        "self.selected_rows.additional_retained_manifest_bytes(already_accounted_rows)?.checked_add(self.generations.retained_owned_heap_bytes()?)"
        not in source_state_projection
        or "Arc::ptr_eq(&self.used_data_description_ids,&rows.used_data_description_ids,)"
        not in selected_rows_projection
        or "ordered_main_rows" in selected_rows_projection
        or selected_rows_projection.count("2*size_of::<usize>()") != 1
        or ".capacity().checked_mul(size_of::<ColumnGeneration>())?"
        not in generation_projection
        or ".capacity().checked_mul(size_of::<MetadataGeneration>())?"
        not in generation_projection
        or "self.additional_retained_manifest_bytes(std::iter::empty::<&Self>())"
        not in compact_observation
    ):
        raise ArchitectureError(
            "T17 current source state must project every unique nested allocation without recharging inline state"
        )

    bound_fields = rust_struct_fields(bound, "BoundSelectedObservation", bound_path)
    bound_open = re.sub(r"\s+", "", rust_function_body(bound, "open", bound_path))
    bound_shared_bytes = re.sub(
        r"\s+", "", rust_function_body(bound, "shared_bytes", bound_path)
    )
    compact_access = re.sub(r"\s+", "", access)
    access_open = re.sub(
        r"\s+", "", rust_function_body(access, "open_with_measures", access_path)
    )
    plan_admission = re.sub(
        r"\s+",
        "",
        rust_function_body(content_plan, "selected_content_plan", content_plan_path),
    )
    retained_metadata = re.sub(
        r"\s+",
        "",
        rust_function_body(content_plan, "retained_metadata_bytes", content_plan_path),
    )
    shared_byte_fields = rust_struct_fields(
        content_plan, "SelectedObservationSharedBytes", content_plan_path
    )
    if (
        bound_fields.get("measures") != "SelectedObservationMeasures"
        or bound_fields.get("sources") != "Vec<BoundObservationSource>"
        or shared_byte_fields
        != {
            "shared_measures_retained_bytes": "usize",
            "shared_source_slots_retained_bytes": "usize",
            "shared_binding_graph_initialization_bytes": "usize",
        }
        or "measures.validate_problem(problem)?;" not in bound_open
        or "letmutsources=Vec::with_capacity(expected.len());" not in bound_open
        or "letfirst_source_shared_bytes=Self::shared_bytes(problem,&measures,&bindings,bindings.capacity(),sources.capacity(),)?;"
        not in bound_open
        or "letbinding_slot_bytes=binding_capacity.checked_mul(size_of::<ObservationSourceBinding>())"
        not in bound_shared_bytes
        or "letbinding_graph_initialization_bytes=bindings.iter().enumerate().try_fold(binding_slot_bytes,"
        not in bound_shared_bytes
        or ".additional_retained_heap_bytes(already_accounted_rows)"
        not in bound_shared_bytes
        or "bindings[..binding_index].iter().map(|prior|prior.current_state.selected_rows())"
        not in bound_shared_bytes
        or "source_capacity.checked_mul(BoundObservationSource::retained_source_slot_bytes())"
        not in bound_shared_bytes
        or "Ok(SelectedObservationSharedBytes::new(measures.retained_bytes(),source_slots_retained_bytes,binding_graph_initialization_bytes,))"
        not in bound_shared_bytes
        or bound_shared_bytes.count("measures.retained_bytes()") != 1
        or bound_open.count("source_index==0") != 1
        or "letshared_bytes=ifsource_index==0{first_source_shared_bytes}else{SelectedObservationSharedBytes::NONE};"
        not in bound_open
        or "measures.retained_bytes()" in bound_open
        or "&measures,shared_bytes,binding.content_budget," not in bound_open
        or "measures.verify_state()?;" not in bound_open
        or "constfnretained_source_slot_bytes()->usize{size_of::<Self>()}"
        not in compact_access
        or "measures.validate_problem(problem)?;" not in access_open
        or "selected_content_plan(&measurement_set,problem,source,shared_bytes,content_budget,)?"
        not in access_open
        or "MsCalEngine::new_selected_observation(&measurement_set,measures.provider(),measures.provider_state(),)?"
        not in access_open
        or "retained_metadata_bytes(measurement_set,problem,source,shared_bytes.shared_measures_retained_bytes,shared_bytes.shared_source_slots_retained_bytes,)?"
        not in plan_admission
        or retained_metadata.count("shared_source_slots_retained_bytes") != 1
        or retained_metadata.count("shared_measures_retained_bytes") != 1
        or "letretained_bytes=shared_source_slots_retained_bytes.checked_add(shared_measures_retained_bytes)"
        not in retained_metadata
        or "coordinate_construction_scratch_bytes.checked_add(shared_bytes.shared_binding_graph_initialization_bytes)"
        not in plan_admission
        or plan_admission.count(
            "shared_bytes.shared_binding_graph_initialization_bytes"
        )
        != 1
        or "validation_scratch_bytes" in plan_admission
        or "current_state" in plan_admission
    ):
        raise ArchitectureError(
            "T17 selected-observation provider, source slots, and complete consumed binding graph must be charged exactly once"
        )

    engine_fields = rust_struct_fields(engine, "MsCalEngine", engine_path)
    engine_constructor = re.sub(
        r"\s+",
        "",
        rust_function_body(engine, "new_selected_observation", engine_path),
    )
    engine_projection = re.sub(
        r"\s+",
        "",
        rust_function_body(
            engine, "selected_observation_retained_heap_bytes", engine_path
        ),
    )
    engine_verify = re.sub(
        r"\s+",
        "",
        rust_function_body(engine, "verify_selected_observation_measures", engine_path),
    )
    if (
        engine_fields.get("antenna_positions") != "Box<[MPosition]>"
        or engine_fields.get("antenna_mount_alt_az") != "Box<[bool]>"
        or engine_fields.get("field_directions") != "Box<[MDirection]>"
        or engine_fields.get("measures") != "Option<Arc<dynMeasuresProvider>>"
        or engine_fields.get("selected_observation_measures_state")
        != "Option<MeasuresProviderState>"
        or any(
            forbidden in engine_constructor
            for forbidden in (
                "open_measures_runtime(",
                "open_discovered(",
                "MeasuresRuntime",
            )
        )
        or engine_constructor.count(".into_boxed_slice()") != 3
        or "measures:Some(measures)" not in engine_constructor
        or "selected_observation_measures_state:Some(measures_state)"
        not in engine_constructor
        or "size_of::<MPosition>()+size_of::<bool>()" not in engine_projection
        or "size_of::<MDirection>()" not in engine_projection
        or ".prepare_bounded_state()" not in engine_verify
        or "actual!=Some(expected)" not in engine_verify
    ):
        raise ArchitectureError(
            "T17 selected-observation geometry must retain only the injected bounded provider in exact fixed slices"
        )


def validate_t17_runtime_completion_source(source: str, path: Path) -> None:
    runtime_authority_fields = {
        "attempt_id": "ExecutionAttemptId",
        "owner_node": "WorkNodeId",
        "settled_fences": "BTreeSet<FenceKind>",
        "lease_epoch": "u64",
    }
    context_fields = rust_struct_fields(
        source, "ObservationReadCompletionContext", path
    )
    if context_fields != {
        **runtime_authority_fields,
        "problem_id": "CompiledProblemId",
        "observation_snapshot_id": "ObservationSnapshotId",
        "observation_provenance_id": "ObservationProvenanceId",
        "commitment_id": "SelectedObservationCommitmentId",
    }:
        raise ArchitectureError(
            "ObservationReadCompletionContext differs from the accepted fresh runtime authority"
        )
    completion_fields = rust_struct_fields(
        source, "AttemptBoundObservationCompletion", path
    )
    if completion_fields != {
        **runtime_authority_fields,
        "owner_completion": "casa_ms::SelectedObservationCompletion",
    }:
        raise ArchitectureError(
            "attempt-bound observation completion must retain casa-ms's concrete owner completion"
        )
    compact_source = re.sub(r"\s+", "", source)
    if (
        "pubfnbind(self,owner_completion:casa_ms::SelectedObservationCompletion,)->Result<AttemptBoundObservationCompletion,ObservationCompletionBindingError>"
        not in compact_source
        or "structAttemptBoundObservationCompletion<" in compact_source
        or "typeObservationReadCompletion;" in compact_source
    ):
        raise ArchitectureError(
            "runtime observation completion must be concrete and cannot accept caller-chosen proof types"
        )
    required_owner_identity_checks = {
        "owner_completion.problem_id()!=self.problem_id",
        "owner_completion.observation_snapshot_id()!=self.observation_snapshot_id",
        "owner_completion.observation_provenance_id()!=self.observation_provenance_id",
        "owner_completion.commitment_id()!=self.commitment_id",
    }
    if not all(check in compact_source for check in required_owner_identity_checks):
        raise ArchitectureError(
            "runtime observation completion must match the exact problem, snapshot, provenance, and commitment"
        )
    completion_declaration = re.search(
        r"#\[derive\(([^)]*)\)\]\s*pub\s+struct\s+AttemptBoundObservationCompletion",
        source,
    )
    if completion_declaration is None or "Clone" in completion_declaration.group(
        1
    ).split(","):
        raise ArchitectureError(
            "attempt-bound observation completion must remain owner-only and non-Clone"
        )

    run_body = re.sub(r"\s+", "", rust_function_body(source, "run_inner", path))
    required_runtime_structure = {
        "letsynchronous_observation_read=work.node().kind.reads_observation()&&work.node().fences.is_empty();",
        "Ok(_)ifsynchronous_observation_read=>",
        "ifsettled==&work.node().fences{Some(ObservationReadCompletionContext{",
        "iffence_transition_succeeded&&letSome(completion)=observation_completion{",
    }
    if (
        not all(fragment in run_body for fragment in required_runtime_structure)
        or run_body.count("implementation.complete_observation_read(completion)") != 2
        or run_body.count("completed_observation_reads.insert(") != 2
    ):
        raise ArchitectureError(
            "runtime must bind owner completion exactly once after synchronous or settled-fence ObservationRead completion"
        )


def run_policy_self_test(policy: dict[str, Any]) -> None:
    allowed = {
        (source, target)
        for source, targets in policy["allowed_logical_edges"].items()
        for target in targets
    }
    for source in policy["layers"]:
        for target in policy["layers"]:
            if (source, target) in allowed:
                continue
            try:
                validate_logical_edge(policy, source, target)
            except ArchitectureError:
                continue
            raise ArchitectureError(
                f"self-test accepted forbidden logical edge {source} -> {target}"
            )


def synthetic_matrix(policy: dict[str, Any]) -> dict[str, Any]:
    baseline = "repo://scripts/check-imaging-architecture.py#class ArchitectureError"
    return {
        "schema_version": 1,
        "contract_revision": 1,
        "required_issue_crosswalk": policy["required_migration_evidence_issues"],
        "acceptance_contracts": {
            "synthetic": {
                "baseline_identity": "synthetic identity",
                "comparator": {
                    "kind": "synthetic",
                    "normalized_rms_ceiling": None,
                    "denominator": None,
                    "preprocessing": "none",
                },
                "thresholds": ["synthetic threshold"],
                "laws": ["synthetic law"],
                "resource_gates": ["synthetic resource gate"],
                "evidence_tiers": ["synthetic evidence tier"],
            }
        },
        "baseline_manifest_digests": {
            baseline: hashlib.sha256(
                (REPO_ROOT / "scripts/check-imaging-architecture.py").read_bytes()
            ).hexdigest()
        },
        "inventory": {
            "capability": ["synthetic-row"],
            "product": ["synthetic-product"],
            "solver": ["synthetic-solver"],
            "frontend": ["synthetic-frontend"],
            "backend": ["synthetic-backend"],
        },
        "rows": [
            {
                "id": "synthetic-row",
                "kind": "capability",
                "status": "TemporarilyUnavailable",
                "current_owner": "synthetic displaced owner",
                "destination_tickets": ["#488"],
                "evidence_issues": policy["required_migration_evidence_issues"],
                "baseline_manifests": [baseline],
                "acceptance_contract": "synthetic",
                "transfer_point": "synthetic transfer",
                "deletion_condition": "synthetic deletion",
                "migration_obligation": {
                    "ticket": "#488",
                    "reason": "synthetic obligation",
                },
                "source_evidence": [
                    "scripts/check-imaging-architecture.py::class ArchitectureError"
                ],
            },
            *[
                {
                    "id": identifier,
                    "kind": kind,
                    "status": "Native",
                    "current_owner": "synthetic native",
                    "destination_tickets": ["#488"],
                    "evidence_issues": policy["required_migration_evidence_issues"],
                    "baseline_manifests": [baseline],
                    "acceptance_contract": "synthetic",
                    "transfer_point": "synthetic transfer",
                    "deletion_condition": "synthetic canonical owner",
                    "migration_obligation": None,
                    "source_evidence": [
                        "scripts/check-imaging-architecture.py::class ArchitectureError"
                    ],
                }
                for kind, identifier in [
                    ("product", "synthetic-product"),
                    ("solver", "synthetic-solver"),
                    ("frontend", "synthetic-frontend"),
                    ("backend", "synthetic-backend"),
                ]
            ],
        ],
    }


def run_matrix_self_test(policy: dict[str, Any]) -> None:
    base = synthetic_matrix(policy)
    validate_migration_matrix(base, policy, enforce_accepted_scope=False)
    mutations = []
    bad_status = copy.deepcopy(base)
    bad_status["rows"][0]["status"] = "Fallback"
    mutations.append(bad_status)
    missing_contract = copy.deepcopy(base)
    missing_contract["rows"][0]["acceptance_contract"] = "missing"
    mutations.append(missing_contract)
    missing_obligation = copy.deepcopy(base)
    missing_obligation["rows"][0]["migration_obligation"] = None
    mutations.append(missing_obligation)
    missing_issue = copy.deepcopy(base)
    for row in missing_issue["rows"]:
        row["evidence_issues"] = [488]
    mutations.append(missing_issue)
    changed_crosswalk = copy.deepcopy(base)
    changed_crosswalk["required_issue_crosswalk"] = [488]
    mutations.append(changed_crosswalk)
    duplicate = copy.deepcopy(base)
    duplicate["rows"].append(copy.deepcopy(duplicate["rows"][0]))
    mutations.append(duplicate)
    for index, mutation in enumerate(mutations):
        try:
            validate_migration_matrix(mutation, policy, enforce_accepted_scope=False)
        except ArchitectureError:
            continue
        raise ArchitectureError(f"migration-matrix self-test mutation {index} passed")


def resolve_input(path: Path, base: Path = REPO_ROOT) -> Path:
    return path if path.is_absolute() else base / path


def main() -> int:
    args = parse_args()
    try:
        policy_path = resolve_input(args.policy)
        policy = load_object(policy_path, "dependency policy")
        validate_policy(policy)
        if args.self_test:
            run_policy_self_test(policy)
            run_matrix_self_test(policy)

        metadata = load_cargo_metadata(
            resolve_input(args.metadata) if args.metadata else None
        )
        validate_workspace(policy, metadata)
        validate_forward_invariants(policy, metadata)
        validate_source_boundaries(policy)

        matrix_path = (
            resolve_input(args.migration_matrix)
            if args.migration_matrix
            else resolve_input(Path(policy["migration_matrix"]))
        )
        if not matrix_path.is_file():
            raise ArchitectureError(
                f"required migration matrix is missing or not a file: {display_path(matrix_path)}"
            )
        matrix = load_object(matrix_path, "migration matrix")
        validate_migration_matrix(matrix, policy)
        matrix_rows = len(matrix["rows"])
        print(
            "imaging-architecture: validated "
            f"{len(metadata['packages'])} workspace packages, "
            f"{len(policy['layers'])} logical layers, "
            f"{matrix_rows} migration rows"
        )
        return 0
    except ArchitectureError as error:
        print(f"imaging-architecture: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
