#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Enforce the ADR-0009 imaging ownership and dependency graph."""

from __future__ import annotations

import argparse
import copy
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = REPO_ROOT / "resources/imaging-architecture/dependency-policy.json"
VALID_STATUSES = {"Native", "LegacyWholeRun", "TemporarilyUnavailable"}
MATRIX_KINDS = {"capability", "product", "solver", "frontend", "backend"}
LOCATOR_KEYS = {"commit", "issue", "locator", "path", "receipt", "token", "url"}


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
        raise ArchitectureError(f"cannot load {context} {display_path(path)}: {error}") from error
    if not isinstance(value, dict):
        raise ArchitectureError(f"{context} {display_path(path)} must contain an object")
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
    result = [require_string(item, f"{context}[{index}]") for index, item in enumerate(value)]
    if len(set(result)) != len(result):
        raise ArchitectureError(f"{context} contains duplicates")
    return result


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != 1:
        raise ArchitectureError("dependency policy schema_version must be 1")
    require_string(policy.get("decision"), "dependency policy decision")
    layers = require_string_list(policy.get("layers"), "dependency policy layers")
    layer_set = set(layers)
    if "legacy" not in layer_set:
        raise ArchitectureError("dependency policy layers must include legacy")

    allowed = policy.get("allowed_logical_edges")
    if not isinstance(allowed, dict) or set(allowed) != layer_set:
        raise ArchitectureError("allowed_logical_edges must define every layer exactly once")
    for source in layers:
        targets = allowed[source]
        if not isinstance(targets, list):
            raise ArchitectureError(f"allowed_logical_edges.{source} must be an array")
        if len(targets) != len(set(targets)):
            raise ArchitectureError(f"allowed_logical_edges.{source} contains duplicates")
        unknown = sorted(set(targets) - layer_set)
        if unknown:
            raise ArchitectureError(
                f"allowed_logical_edges.{source} names unknown layers: {unknown}"
            )
        if source in targets:
            raise ArchitectureError(f"allowed_logical_edges.{source} may not contain itself")
        if source != "legacy" and "legacy" in targets:
            raise ArchitectureError(
                f"native layer {source} may not import legacy; T04 owns the sole router seam"
            )

    package_layers = policy.get("package_layers")
    if not isinstance(package_layers, dict) or not package_layers:
        raise ArchitectureError("package_layers must be a non-empty object")
    for package, layer in package_layers.items():
        require_string(package, "package_layers key")
        if layer not in layer_set:
            raise ArchitectureError(f"package {package} names unknown layer {layer!r}")

    native_rules = policy.get("native_package_workspace_dependencies")
    if not isinstance(native_rules, dict):
        raise ArchitectureError("native_package_workspace_dependencies must be an object")
    for package, dependencies in native_rules.items():
        if package_layers.get(package) in (None, "legacy"):
            raise ArchitectureError(f"native dependency rule {package} is not a native package")
        if not isinstance(dependencies, list) or any(
            not isinstance(dependency, str) or not dependency for dependency in dependencies
        ):
            raise ArchitectureError(
                f"native_package_workspace_dependencies.{package} must be a string array"
            )
        if len(dependencies) != len(set(dependencies)):
            raise ArchitectureError(
                f"native_package_workspace_dependencies.{package} contains duplicates"
            )

    legacy_packages = set(require_string_list(policy.get("legacy_packages"), "legacy_packages"))
    mapped_legacy = {
        package for package, layer in package_layers.items() if layer == "legacy"
    }
    if legacy_packages != mapped_legacy:
        raise ArchitectureError(
            "legacy_packages must exactly match packages assigned to the legacy layer"
        )
    frozen_edges(policy)

    prefixes = require_string_list(
        policy.get("device_dependency_prefixes"), "device_dependency_prefixes"
    )
    if any(prefix != prefix.lower() for prefix in prefixes):
        raise ArchitectureError("device_dependency_prefixes must be lowercase")
    device_free = require_string_list(policy.get("device_free_layers"), "device_free_layers")
    unknown_device_free = sorted(set(device_free) - layer_set)
    if unknown_device_free:
        raise ArchitectureError(f"device_free_layers names unknown layers: {unknown_device_free}")

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


def edge_tuple(edge: Any, context: str) -> tuple[str, str, str]:
    if not isinstance(edge, dict):
        raise ArchitectureError(f"{context} must be an object")
    if set(edge) != {"source", "target", "kind"}:
        raise ArchitectureError(f"{context} must contain source, target, and kind only")
    source = require_string(edge.get("source"), f"{context}.source")
    target = require_string(edge.get("target"), f"{context}.target")
    kind = require_string(edge.get("kind"), f"{context}.kind")
    if kind not in {"normal", "build"}:
        raise ArchitectureError(f"{context}.kind must be normal or build")
    return source, target, kind


def frozen_edges(policy: dict[str, Any]) -> set[tuple[str, str, str]]:
    values = policy.get("frozen_legacy_workspace_edges")
    if not isinstance(values, list) or not values:
        raise ArchitectureError("frozen_legacy_workspace_edges must be a non-empty array")
    result = {
        edge_tuple(edge, f"frozen_legacy_workspace_edges[{index}]")
        for index, edge in enumerate(values)
    }
    if len(result) != len(values):
        raise ArchitectureError("frozen_legacy_workspace_edges contains duplicates")
    legacy = set(policy["legacy_packages"])
    for source, target, _kind in result:
        if source not in legacy and target not in legacy:
            raise ArchitectureError(
                f"frozen legacy edge {source} -> {target} does not touch a legacy package"
            )
    return result


def validate_logical_edge(policy: dict[str, Any], source: str, target: str) -> None:
    layers = set(policy["layers"])
    if source not in layers or target not in layers:
        raise ArchitectureError(f"logical edge names an unknown layer: {source} -> {target}")
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
        detail = error.stderr.strip() if isinstance(error, subprocess.CalledProcessError) else str(error)
        raise ArchitectureError(f"cannot query live Cargo metadata: {detail}") from error
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ArchitectureError(f"Cargo metadata returned invalid JSON: {error}") from error
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
            raise ArchitectureError(f"Cargo metadata packages[{index}] must be an object")
        name = require_string(package.get("name"), f"Cargo metadata packages[{index}].name")
        if name in names:
            raise ArchitectureError(f"Cargo metadata repeats package {name}")
        names.add(name)
        package_dependencies = package.get("dependencies", [])
        if not isinstance(package_dependencies, list) or any(
            not isinstance(dependency, dict) for dependency in package_dependencies
        ):
            raise ArchitectureError(f"Cargo metadata dependencies for {name} must be an array")
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


def matches_dependency_prefix(name: str, prefix: str) -> bool:
    normalized = name.lower().replace("_", "-")
    return normalized == prefix or normalized.startswith(prefix + "-")


def validate_workspace(policy: dict[str, Any], metadata: dict[str, Any]) -> None:
    package_names, edges, dependencies = workspace_edges(metadata)
    package_layers: dict[str, str] = policy["package_layers"]
    missing = sorted(set(package_layers) - package_names)
    if missing:
        raise ArchitectureError(f"policy-owned workspace packages are missing: {missing}")

    legacy = set(policy["legacy_packages"])
    actual_legacy = {
        edge for edge in edges if edge[0] in legacy or edge[1] in legacy
    }
    expected_legacy = frozen_edges(policy)
    added = sorted(actual_legacy - expected_legacy)
    removed = sorted(expected_legacy - actual_legacy)
    if added or removed:
        raise ArchitectureError(
            "frozen legacy workspace edges changed: "
            f"added={format_edges(added)}, removed={format_edges(removed)}"
        )

    for source, target, _kind in sorted(edges):
        source_layer = package_layers.get(source)
        target_layer = package_layers.get(target)
        if source_layer is None or source_layer == "legacy" or target_layer is None:
            continue
        if target_layer == "legacy":
            raise ArchitectureError(
                f"native package imports legacy: {source}({source_layer}) -> {target}(legacy)"
            )
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
        if unexpected:
            raise ArchitectureError(
                f"native package {package} has undeclared workspace dependencies: {unexpected}"
            )

    device_free = set(policy["device_free_layers"])
    prefixes: list[str] = policy["device_dependency_prefixes"]
    for package, layer in package_layers.items():
        if layer not in device_free:
            continue
        for dependency in dependencies[package]:
            kind = dependency.get("kind") or "normal"
            if kind not in {"normal", "build"}:
                continue
            name = require_string(
                dependency.get("name"), f"Cargo metadata dependency target for {package}"
            )
            prefix = next(
                (candidate for candidate in prefixes if matches_dependency_prefix(name, candidate)),
                None,
            )
            if prefix is not None:
                raise ArchitectureError(
                    f"device-free package {package}({layer}) imports {name} "
                    f"(forbidden family {prefix})"
                )


def format_edges(edges: list[tuple[str, str, str]]) -> str:
    return "[" + ", ".join(f"{source}->{target}({kind})" for source, target, kind in edges) + "]"


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
    missing = sorted(required - set(contract))
    if missing:
        raise ArchitectureError(f"{context} is missing fields: {missing}")
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
    comparator_missing = sorted(comparator_fields - set(comparator))
    if comparator_missing:
        raise ArchitectureError(
            f"{context}.comparator is missing fields: {comparator_missing}"
        )
    require_string(comparator.get("kind"), f"{context}.comparator.kind")
    require_string(comparator.get("preprocessing"), f"{context}.comparator.preprocessing")
    ceiling = comparator.get("normalized_rms_ceiling")
    if ceiling is not None:
        if isinstance(ceiling, bool) or not isinstance(ceiling, (int, float)) or ceiling < 0:
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
                raise ArchitectureError(f"acceptance contract {identifier} must be an object")
            validate_acceptance_contract(identifier, contract)
        return result
    if isinstance(value, list):
        result: set[str] = set()
        for index, contract in enumerate(value):
            if not isinstance(contract, dict):
                raise ArchitectureError(f"acceptance_contracts[{index}] must be an object")
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
            raise ArchitectureError(f"{context}[{index}] path must stay inside the repository")
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


def validate_baseline_manifests(value: Any, context: str) -> None:
    validate_locator_collection(value, context)
    for index, locator in enumerate(value):
        if not isinstance(locator, str) or not locator.startswith("repo://"):
            continue
        relative, separator, fragment = locator.removeprefix("repo://").partition("#")
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts:
            raise ArchitectureError(f"{context}[{index}] path must stay inside the repository")
        try:
            content = (REPO_ROOT / path).read_text(encoding="utf-8")
        except OSError as error:
            raise ArchitectureError(
                f"{context}[{index}] cannot read baseline manifest {relative}: {error}"
            ) from error
        if separator and (not fragment or fragment not in content):
            raise ArchitectureError(
                f"{context}[{index}] baseline fragment {fragment!r} was not found in {relative}"
            )


def issue_number(value: Any, context: str) -> int:
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str):
        match = re.fullmatch(r"#?(\d+)", value.strip())
        if match and int(match.group(1)) > 0:
            return int(match.group(1))
    raise ArchitectureError(f"{context} must be a positive issue number")


def validate_migration_matrix(
    matrix: dict[str, Any], policy: dict[str, Any]
) -> None:
    if matrix.get("schema_version") != 1:
        raise ArchitectureError("migration matrix schema_version must be 1")
    revision = matrix.get("contract_revision")
    if not (
        isinstance(revision, int)
        and revision > 0
        or isinstance(revision, str)
        and revision.strip()
    ):
        raise ArchitectureError("migration matrix contract_revision must be positive or non-empty")
    known_contracts = contract_ids(matrix.get("acceptance_contracts"))
    if not known_contracts:
        raise ArchitectureError("migration matrix must define acceptance contracts")

    declared_crosswalk = matrix.get("required_issue_crosswalk")
    if not isinstance(declared_crosswalk, list) or any(
        not isinstance(issue, int) or issue <= 0 for issue in declared_crosswalk
    ):
        raise ArchitectureError(
            "migration matrix required_issue_crosswalk must be an array of positive integers"
        )
    if len(declared_crosswalk) != len(set(declared_crosswalk)):
        raise ArchitectureError("migration matrix required_issue_crosswalk contains duplicates")
    expected_crosswalk = set(policy["required_migration_evidence_issues"])
    if set(declared_crosswalk) != expected_crosswalk:
        raise ArchitectureError(
            "migration matrix required_issue_crosswalk differs from dependency policy: "
            f"added={sorted(set(declared_crosswalk) - expected_crosswalk)}, "
            f"removed={sorted(expected_crosswalk - set(declared_crosswalk))}"
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

    rows = matrix.get("rows")
    if not isinstance(rows, list) or not rows:
        raise ArchitectureError("migration matrix rows must be a non-empty array")
    row_ids: set[str] = set()
    row_pairs: set[tuple[str, str]] = set()
    covered_issues: set[int] = set()
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
        missing = sorted(required_fields - set(row))
        if missing:
            raise ArchitectureError(f"{context} is missing fields: {missing}")
        identifier = require_string(row.get("id"), f"{context}.id")
        if identifier in row_ids:
            raise ArchitectureError(f"migration matrix repeats row id {identifier}")
        row_ids.add(identifier)
        kind = require_string(row.get("kind"), f"{context}.kind")
        if kind not in MATRIX_KINDS:
            raise ArchitectureError(f"{context}.kind must be one of {sorted(MATRIX_KINDS)}")
        row_pairs.add((kind, identifier))
        status = row.get("status")
        if status not in VALID_STATUSES:
            raise ArchitectureError(
                f"{context}.status must be one of {sorted(VALID_STATUSES)}"
            )
        require_string(row.get("current_owner"), f"{context}.current_owner")
        require_string_list(row.get("destination_tickets"), f"{context}.destination_tickets")
        evidence = row.get("evidence_issues")
        if not isinstance(evidence, list) or not evidence:
            raise ArchitectureError(f"{context}.evidence_issues must be a non-empty array")
        covered_issues.update(
            issue_number(issue, f"{context}.evidence_issues[{issue_index}]")
            for issue_index, issue in enumerate(evidence)
        )
        validate_baseline_manifests(
            row.get("baseline_manifests"), f"{context}.baseline_manifests"
        )
        contract = require_string(row.get("acceptance_contract"), f"{context}.acceptance_contract")
        if contract not in known_contracts:
            raise ArchitectureError(f"{context} references unknown acceptance contract {contract}")
        require_string(row.get("transfer_point"), f"{context}.transfer_point")
        require_string(row.get("deletion_condition"), f"{context}.deletion_condition")
        obligation = row.get("migration_obligation")
        if status == "Native":
            if obligation is not None:
                raise ArchitectureError(f"{context}.migration_obligation must be null for Native")
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
        validate_source_evidence(row.get("source_evidence"), f"{context}.source_evidence")

    if row_pairs != inventory_pairs:
        raise ArchitectureError(
            "migration matrix inventory and rows differ: "
            f"missing={sorted(inventory_pairs - row_pairs)}, "
            f"extra={sorted(row_pairs - inventory_pairs)}"
        )

    missing_issues = sorted(expected_crosswalk - covered_issues)
    if missing_issues:
        raise ArchitectureError(
            f"migration matrix omits required crosswalk issues: {missing_issues}"
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
                "status": "LegacyWholeRun",
                "current_owner": "synthetic legacy",
                "destination_tickets": ["#488"],
                "evidence_issues": policy["required_migration_evidence_issues"],
                "baseline_manifests": ["synthetic:baseline"],
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
                    "baseline_manifests": ["synthetic:baseline"],
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
    validate_migration_matrix(base, policy)
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
            validate_migration_matrix(mutation, policy)
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

        metadata = load_cargo_metadata(resolve_input(args.metadata) if args.metadata else None)
        validate_workspace(policy, metadata)

        matrix_path = (
            resolve_input(args.migration_matrix)
            if args.migration_matrix
            else resolve_input(Path(policy["migration_matrix"]))
        )
        matrix_rows = 0
        if matrix_path.exists():
            matrix = load_object(matrix_path, "migration matrix")
            validate_migration_matrix(matrix, policy)
            matrix_rows = len(matrix["rows"])
        print(
            "imaging-architecture: validated "
            f"{len(metadata['packages'])} workspace packages, "
            f"{len(policy['layers'])} logical layers, "
            f"{len(policy['frozen_legacy_workspace_edges'])} frozen legacy edges, "
            f"{matrix_rows} migration rows"
        )
        return 0
    except ArchitectureError as error:
        print(f"imaging-architecture: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
