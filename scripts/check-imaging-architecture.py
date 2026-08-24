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
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = REPO_ROOT / "resources/imaging-architecture/dependency-policy.json"
VALID_STATUSES = {"Native", "LegacyWholeRun", "TemporarilyUnavailable"}
MATRIX_KINDS = {"capability", "product", "solver", "frontend", "backend"}
LOCATOR_KEYS = {"commit", "issue", "locator", "path", "receipt", "token", "url"}
PACKAGE_CLASSIFICATIONS = {"native", "legacy", "surface", "support"}
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
# Independent ratchets for readable policy and matrix scopes. Update a digest
# only when review accepts the corresponding human-readable contract change.
ACCEPTED_FROZEN_LEGACY_EDGES_SHA256 = (
    "93cce3cc2d3979ce6af82b3d76529c7ec8768fb5cfc1b0ce500453d6e128f95f"
)
ACCEPTED_LOGICAL_GRAPH_SHA256 = (
    "7101b6d90196b1ea3d3c750080d703bb5e305e91c8ac19553e1dda7ed58c4e33"
)
ACCEPTED_SOURCE_BOUNDARIES_SHA256 = (
    "94b6a1a2051eea97bc1160a26d96def653fc7f25c31f58fbbee06e6ed4c5d081"
)
ACCEPTED_FROZEN_TRANSITIONAL_EDGES_SHA256 = (
    "0077e28528d2160616d34e17fb7124586f346557917e0bfac99b0dff6739a1d1"
)
ACCEPTED_PACKAGE_POLICY_SHA256 = (
    "276011a251be811a240f9ed7e00bc5b0f0fcb2012be470147db9e29f99d5e1f1"
)
ACCEPTED_WHOLE_RUN_ROUTER_SHA256 = (
    "c855dae5d5b4239e21fa0fe43d1d2f4bbb4114d245374db674fb81713af11a2d"
)
ACCEPTED_MATRIX_INVENTORY_SHA256 = (
    "95f98d0bf3fc1a676bef079d32ae5391a5bac4ff594f211d9f9420949a2e40a6"
)
ACCEPTED_PRODUCT_KIND_INVENTORY_SHA256 = (
    "f4e04101f0d6e89d9bc12584cd580f5f8924f80e71b867ee252422f648fdced5"
)
ACCEPTED_PLANE_SELECTION_INVENTORY_SHA256 = (
    "cea569c11700deee6bd533b79041f7038392bdbadcfaa7468db3263f3d52c7d9"
)
ACCEPTED_POLARIZATION_COORDINATE_INVENTORY_SHA256 = (
    "245f24c2e462b7127fce91a899db1995ce82733ba3d88f5c425a04ec49e32376"
)
ACCEPTED_CUBE_INTERPOLATION_INVENTORY_SHA256 = (
    "c5de582d14d11af38ab9e2ea1833b6a2a222798616466df785b402d96715b8d0"
)
ACCEPTED_STANDARD_MFS_BACKEND_INVENTORY_SHA256 = (
    "28cc3eef3336bac19e51906067f85a0373308e3132d8976a8d19a3aced8432b9"
)
ACCEPTED_SPECTRAL_MODE_INVENTORY_SHA256 = (
    "3a1a8e62103ec316b3bacc996acaf1b426f831b0de2c2db1834b038065d6fe04"
)
ACCEPTED_IMAGER_SPECTRAL_MODE_INVENTORY_SHA256 = (
    "3a1a8e62103ec316b3bacc996acaf1b426f831b0de2c2db1834b038065d6fe04"
)
ACCEPTED_GRIDDER_REQUEST_INVENTORY_SHA256 = (
    "0921c6e8f01dcaebf2c3b32ebc8d34f6811951343f6cbe1b2bee39f3440fe6dc"
)
ACCEPTED_DECONVOLVER_INVENTORY_SHA256 = (
    "57648c06caa082706e5af79f623a54e90b8faccf6cdc2f4105f0bd0718965ca9"
)
ACCEPTED_IMAGER_DECONVOLVER_INVENTORY_SHA256 = (
    "57648c06caa082706e5af79f623a54e90b8faccf6cdc2f4105f0bd0718965ca9"
)
ACCEPTED_IMAGER_CUBE_INTERPOLATION_INVENTORY_SHA256 = (
    "b955f223aebede1f69e75c17ea2ed83bd280557b6cf361dff6a4fd620e673162"
)
ACCEPTED_FFT_BACKEND_CHOICE_INVENTORY_SHA256 = (
    "21bc32c858a3e6f4e13a174b5764cc900dd3d8becce641decae5c3e47e32aecf"
)
ACCEPTED_IMAGING_FFT_BACKEND_POLICY_INVENTORY_SHA256 = (
    "6e72c455ac075eed27b502a635d0c8e0c2ce63c5ece70fea48c13ed2a40ad380"
)
ACCEPTED_STANDARD_MFS_ACCELERATION_POLICY_INVENTORY_SHA256 = (
    "5a746c70358a33c038c9ccf37167ab29998f7b55aef418f43b8d7b1ea4953de3"
)
ACCEPTED_STANDARD_MFS_MINOR_CYCLE_BACKEND_INVENTORY_SHA256 = (
    "d96f704776bd4e88fee221e14e7c863894c5a7c1e6e2d3d0510976ad76a1822e"
)
ACCEPTED_SINGLE_PLANE_ACCELERATION_POLICY_INVENTORY_SHA256 = (
    "d762618f2f100f2af0ea77f2244e19bd7d07d911ae23500432ae667f56bd46c6"
)
ACCEPTED_PER_PLANE_EXECUTION_BACKEND_INVENTORY_SHA256 = (
    "2f622825a283efc3580072ae37c6dbf78dc52530923b9a1c56834a44f23b2583"
)
ACCEPTED_ISSUE_OUTCOMES_SHA256 = (
    "6aa3525971d60dbb09fefa17a201c173c36db3f211aa87bec3a22df957533703"
)
ACCEPTED_ACCEPTANCE_CONTRACTS_SHA256 = (
    "4589711ae6224b94916d05e214df510fab5ba01a16e21a0b913b4b851c1d0e89"
)
ACCEPTED_MATRIX_ROWS_SHA256 = (
    "f9f9c0bbb284a9c52860a308e5f808e7d5a393a3202a7a051e85265d1687211a"
)
ACCEPTED_BASELINE_MANIFEST_DIGESTS_SHA256 = (
    "ed19d0a88b99c1043bf9a951891446f7ea6a06c3cc986669779518f9b7ca6e63"
)
ACCEPTED_MATRIX_CONTRACT_REVISION = 19
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
    ): "640f5af8c6808ebba8df8d36cc2b30fa5422e7e671785c931f5d034940b199b2",
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
    ): "9fb09c33452bcc79cc14a774fa8b0a26f6899e5b2bcd035cbbc87aa58bb006b9",
    (
        "global-weighting-v1",
        "resource_gates",
    ): "53b3c9557ac55a8c62531feb41ea072417d5f9bf82ddc54c4c5e57f5b4f332b2",
    (
        "observation-transaction-v1",
        "thresholds",
    ): "c7df2947ccba63a095abca5b99890c57ed633d2fb739739732fb620fff28757f",
    (
        "observation-transaction-v1",
        "laws",
    ): "418e0eb39337066849b06e25fdef8468ea0b9b3efbe7cd2ccc1c439cf5bb57a9",
    (
        "observation-transaction-v1",
        "resource_gates",
    ): "62ef188eff7d529d52a0dcc401094c82b78133c273b12d2d644afa24b0572523",
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
    if "legacy" not in layer_set:
        raise ArchitectureError("dependency policy layers must include legacy")

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
        if source != "legacy" and "legacy" in targets:
            raise ArchitectureError(
                f"native layer {source} may not import legacy; T04 owns the sole router seam"
            )
    if "backend" in allowed["application"]:
        raise ArchitectureError(
            "allowed_logical_edges.application may not contain backend; applications invoke execution plans"
        )
    accepted_graph = {"layers": layers, "allowed_logical_edges": allowed}
    if stable_digest(accepted_graph) != ACCEPTED_LOGICAL_GRAPH_SHA256:
        raise ArchitectureError(
            "logical imaging layers or allowed edges differ from the accepted graph"
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
        if layer == "legacy" and classification != "legacy":
            raise ArchitectureError(
                f"legacy package {package} must be classified legacy"
            )
        if layer != "legacy" and classification not in {"native", "surface"}:
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
        if package_layers.get(package) in (None, "legacy"):
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

    validate_whole_run_router_policy(
        policy, package_layers, classifications, native_rules
    )

    legacy_packages = set(
        require_string_list(policy.get("legacy_packages"), "legacy_packages")
    )
    mapped_legacy = {
        package for package, layer in package_layers.items() if layer == "legacy"
    }
    if legacy_packages != mapped_legacy:
        raise ArchitectureError(
            "legacy_packages must exactly match packages assigned to the legacy layer"
        )
    classified_legacy = {
        package
        for package, classification in classifications.items()
        if classification == "legacy"
    }
    if classified_legacy != legacy_packages:
        raise ArchitectureError(
            "legacy package classification must exactly match legacy_packages"
        )
    frozen_edges(policy)
    frozen_transitional_edges(policy)

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
    if enforce_accepted_scope:
        package_policy = {
            "package_layers": package_layers,
            "workspace_package_classification": classifications,
            "native_package_workspace_dependencies": native_rules,
            "device_dependency_prefixes": prefixes,
            "device_free_layers": device_free,
        }
        if stable_digest(package_policy) != ACCEPTED_PACKAGE_POLICY_SHA256:
            raise ArchitectureError(
                "workspace package layers, classifications, native dependencies, or device policy differ from the accepted scope"
            )

    source_boundaries = policy.get("source_boundaries")
    validate_source_boundary_policy(source_boundaries)
    if stable_digest(source_boundaries) != ACCEPTED_SOURCE_BOUNDARIES_SHA256:
        raise ArchitectureError("source boundaries differ from the accepted policy")

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


def validate_whole_run_router_policy(
    policy: dict[str, Any],
    package_layers: dict[str, str],
    classifications: dict[str, str],
    native_rules: dict[str, list[str]],
) -> None:
    router = policy.get("whole_run_router")
    required = {
        "package",
        "source",
        "router_type",
        "dispatch_method",
        "engine_ports",
    }
    if not isinstance(router, dict) or set(router) != required:
        raise ArchitectureError(
            f"whole_run_router must contain exactly {sorted(required)}"
        )
    package = require_string(router.get("package"), "whole_run_router.package")
    source = require_string(router.get("source"), "whole_run_router.source")
    require_string(router.get("router_type"), "whole_run_router.router_type")
    require_string(router.get("dispatch_method"), "whole_run_router.dispatch_method")
    require_string_list(router.get("engine_ports"), "whole_run_router.engine_ports")
    source_path = Path(source)
    if (
        source_path.is_absolute()
        or ".." in source_path.parts
        or source_path.suffix != ".rs"
    ):
        raise ArchitectureError(
            "whole_run_router.source must be a repository Rust source"
        )
    if stable_digest(router) != ACCEPTED_WHOLE_RUN_ROUTER_SHA256:
        raise ArchitectureError(
            "whole-run migration router differs from the accepted owner"
        )
    if (
        package_layers.get(package) != "application"
        or classifications.get(package) != "native"
        or native_rules.get(package) != ["casa-imaging-model"]
    ):
        raise ArchitectureError(
            "whole-run migration router must be a native application package with only the imaging model dependency"
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
            "accepted_violation_digest",
        }
        if set(boundary) not in {
            frozenset(required_keys),
            frozenset(required_keys | {"rust_allowlist"}),
        }:
            raise ArchitectureError(
                f"{context} must contain the source-boundary keys and optional rust_allowlist only"
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
        if not isinstance(patterns, list) or (
            not patterns and "rust_allowlist" not in boundary
        ):
            raise ArchitectureError(
                f"{context}.forbidden_patterns must be a non-empty array unless rust_allowlist is present"
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
        rust_allowlist = boundary.get("rust_allowlist")
        if rust_allowlist is not None:
            validate_rust_allowlist_policy(rust_allowlist, f"{context}.rust_allowlist")
        accepted_digest = boundary.get("accepted_violation_digest")
        if accepted_digest is not None and not re.fullmatch(
            r"[0-9a-f]{64}", str(accepted_digest)
        ):
            raise ArchitectureError(
                f"{context}.accepted_violation_digest must be null or a lowercase SHA-256 digest"
            )


def validate_rust_allowlist_policy(value: Any, context: str) -> None:
    required = {
        "allowed_imports",
        "allowed_items",
        "allowed_qualified_paths",
        "allowed_relative_paths",
        "allowed_restricted_visibilities",
        "composition_message",
        "privacy_message",
        "glob_message",
        "import_message",
        "path_message",
        "relative_path_message",
        "item_message",
        "inventory_message",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise ArchitectureError(
            f"{context} must contain exact Rust imports, items, and violation messages"
        )
    imports = require_string_list(
        value.get("allowed_imports"), f"{context}.allowed_imports"
    )
    if any("*" in item or item != item.strip() for item in imports):
        raise ArchitectureError(
            f"{context}.allowed_imports must contain exact non-glob paths"
        )
    items = value.get("allowed_items")
    if not isinstance(items, dict):
        raise ArchitectureError(f"{context}.allowed_items must be an object")
    item_pattern = re.compile(
        r"^(?:struct|enum|union|trait|type|fn|const|static|mod|macro|macro_rules):(?:r#)?[A-Za-z_][A-Za-z0-9_]*$"
    )
    for item, count in items.items():
        if not isinstance(item, str) or item_pattern.fullmatch(item) is None:
            raise ArchitectureError(
                f"{context}.allowed_items contains invalid item {item!r}"
            )
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise ArchitectureError(
                f"{context}.allowed_items[{item!r}] must be a positive integer"
            )
    qualified_paths = value.get("allowed_qualified_paths")
    if not isinstance(qualified_paths, dict):
        raise ArchitectureError(f"{context}.allowed_qualified_paths must be an object")
    qualified_path_pattern = re.compile(
        r"^(?:casa_[A-Za-z0-9_]+|casars(?:_[A-Za-z0-9_]+)?)(?:::(?:r#)?[A-Za-z_][A-Za-z0-9_]*)+$"
    )
    for qualified_path, count in qualified_paths.items():
        if (
            not isinstance(qualified_path, str)
            or qualified_path_pattern.fullmatch(qualified_path) is None
        ):
            raise ArchitectureError(
                f"{context}.allowed_qualified_paths contains invalid path {qualified_path!r}"
            )
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise ArchitectureError(
                f"{context}.allowed_qualified_paths[{qualified_path!r}] must be a positive integer"
            )
    relative_paths = value.get("allowed_relative_paths")
    if not isinstance(relative_paths, dict):
        raise ArchitectureError(f"{context}.allowed_relative_paths must be an object")
    relative_path_pattern = re.compile(
        r"^(?:crate|self|super)(?:::(?:(?:crate|self|super)|(?:r#)?[A-Za-z_][A-Za-z0-9_]*))+$"
    )
    for relative_path, count in relative_paths.items():
        if (
            not isinstance(relative_path, str)
            or relative_path_pattern.fullmatch(relative_path) is None
        ):
            raise ArchitectureError(
                f"{context}.allowed_relative_paths contains invalid path {relative_path!r}"
            )
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise ArchitectureError(
                f"{context}.allowed_relative_paths[{relative_path!r}] must be a positive integer"
            )
    restricted_visibilities = value.get("allowed_restricted_visibilities")
    if not isinstance(restricted_visibilities, dict):
        raise ArchitectureError(
            f"{context}.allowed_restricted_visibilities must be an object"
        )
    restricted_visibility_pattern = re.compile(
        r"^pub \( (?:crate|self|super|in (?:crate|self|super)(?:::(?:(?:crate|self|super)|(?:r#)?[A-Za-z_][A-Za-z0-9_]*))*) \) use$"
    )
    for visibility, count in restricted_visibilities.items():
        if (
            not isinstance(visibility, str)
            or restricted_visibility_pattern.fullmatch(visibility) is None
        ):
            raise ArchitectureError(
                f"{context}.allowed_restricted_visibilities contains invalid visibility {visibility!r}"
            )
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise ArchitectureError(
                f"{context}.allowed_restricted_visibilities[{visibility!r}] must be a positive integer"
            )
    for name in [
        "composition_message",
        "privacy_message",
        "glob_message",
        "import_message",
        "path_message",
        "relative_path_message",
        "item_message",
        "inventory_message",
    ]:
        require_string(value.get(name), f"{context}.{name}")


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
        raise ArchitectureError(
            "frozen_legacy_workspace_edges must be a non-empty array"
        )
    result = {
        edge_tuple(edge, f"frozen_legacy_workspace_edges[{index}]")
        for index, edge in enumerate(values)
    }
    if len(result) != len(values):
        raise ArchitectureError("frozen_legacy_workspace_edges contains duplicates")
    if stable_digest(sorted(result)) != ACCEPTED_FROZEN_LEGACY_EDGES_SHA256:
        raise ArchitectureError(
            "frozen_legacy_workspace_edges differs from the 16 accepted exceptions"
        )
    legacy = set(policy["legacy_packages"])
    for source, target, _kind in result:
        if source not in legacy and target not in legacy:
            raise ArchitectureError(
                f"frozen legacy edge {source} -> {target} does not touch a legacy package"
            )
    return result


def frozen_transitional_edges(
    policy: dict[str, Any],
) -> set[tuple[str, str, str]]:
    values = policy.get("frozen_transitional_workspace_edges")
    if not isinstance(values, list) or not values:
        raise ArchitectureError(
            "frozen_transitional_workspace_edges must be a non-empty array"
        )
    result = {
        edge_tuple(edge, f"frozen_transitional_workspace_edges[{index}]")
        for index, edge in enumerate(values)
    }
    if len(result) != len(values):
        raise ArchitectureError(
            "frozen_transitional_workspace_edges contains duplicates"
        )
    if stable_digest(sorted(result)) != ACCEPTED_FROZEN_TRANSITIONAL_EDGES_SHA256:
        raise ArchitectureError(
            "frozen_transitional_workspace_edges differs from the accepted exceptions"
        )
    package_layers = policy["package_layers"]
    legacy = set(policy["legacy_packages"])
    for source, target, _kind in result:
        if source not in package_layers or target not in package_layers:
            raise ArchitectureError(
                f"frozen transitional edge {source} -> {target} lacks a logical package layer"
            )
        if source in legacy or target in legacy:
            raise ArchitectureError(
                f"frozen transitional edge {source} -> {target} belongs in the legacy ledger"
            )
        try:
            validate_logical_edge(
                policy, package_layers[source], package_layers[target]
            )
        except ArchitectureError:
            continue
        raise ArchitectureError(
            f"frozen transitional edge {source} -> {target} is already allowed by the logical graph"
        )
    return result


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

    legacy = set(policy["legacy_packages"])
    actual_legacy = {edge for edge in edges if edge[0] in legacy or edge[1] in legacy}
    expected_legacy = frozen_edges(policy)
    added = sorted(actual_legacy - expected_legacy)
    removed = sorted(expected_legacy - actual_legacy)
    if added or removed:
        raise ArchitectureError(
            "frozen legacy workspace edges changed: "
            f"added={format_edges(added)}, removed={format_edges(removed)}"
        )

    expected_transitional = frozen_transitional_edges(policy)
    missing_transitional = sorted(expected_transitional - edges)
    if missing_transitional:
        raise ArchitectureError(
            "frozen transitional workspace edges changed: "
            f"removed={format_edges(missing_transitional)}"
        )

    for source, target, kind in sorted(edges):
        if (source, target, kind) in expected_legacy | expected_transitional:
            continue
        source_layer = package_layers.get(source)
        target_layer = package_layers.get(target)
        if source_layer is None or source_layer == "legacy" or target_layer is None:
            continue
        if target_layer == "legacy":
            raise ArchitectureError(
                f"native package imports legacy: {source}({source_layer}) -> {target}(legacy)"
            )
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


@dataclass(frozen=True)
class RustToken:
    text: str
    line: int


def rust_tokens(source: str, path: str) -> list[RustToken]:
    tokens: list[RustToken] = []
    index = 0
    line = 1

    def consume_literal(start: int, prefix_length: int = 0) -> int:
        nonlocal line
        quote = source[start + prefix_length]
        cursor = start + prefix_length + 1
        while cursor < len(source):
            character = source[cursor]
            if character == "\\":
                if cursor + 1 < len(source) and source[cursor + 1] == "\n":
                    line += 1
                cursor += 2
                continue
            if character == quote:
                return cursor + 1
            if character == "\n":
                line += 1
            cursor += 1
        raise ArchitectureError(
            f"cannot lex Rust boundary {path}: unterminated literal"
        )

    def raw_literal_end(start: int, prefix_length: int) -> int | None:
        nonlocal line
        cursor = start + prefix_length
        hashes = 0
        while cursor < len(source) and source[cursor] == "#":
            hashes += 1
            cursor += 1
        if cursor >= len(source) or source[cursor] != '"':
            return None
        cursor += 1
        terminator = '"' + "#" * hashes
        end = source.find(terminator, cursor)
        if end == -1:
            raise ArchitectureError(
                f"cannot lex Rust boundary {path}: unterminated raw literal"
            )
        line += source.count("\n", cursor, end + len(terminator))
        return end + len(terminator)

    while index < len(source):
        character = source[index]
        if character.isspace():
            if character == "\n":
                line += 1
            index += 1
            continue
        if source.startswith("//", index):
            newline = source.find("\n", index + 2)
            if newline == -1:
                break
            index = newline
            continue
        if source.startswith("/*", index):
            depth = 1
            cursor = index + 2
            while cursor < len(source) and depth:
                if source.startswith("/*", cursor):
                    depth += 1
                    cursor += 2
                elif source.startswith("*/", cursor):
                    depth -= 1
                    cursor += 2
                else:
                    if source[cursor] == "\n":
                        line += 1
                    cursor += 1
            if depth:
                raise ArchitectureError(
                    f"cannot lex Rust boundary {path}: unterminated block comment"
                )
            index = cursor
            continue

        token_line = line
        raw_prefix = next(
            (
                prefix
                for prefix in ("br", "cr", "r")
                if source.startswith(prefix, index)
            ),
            None,
        )
        if raw_prefix is not None:
            end = raw_literal_end(index, len(raw_prefix))
            if end is not None:
                tokens.append(RustToken("<literal>", token_line))
                index = end
                continue
        literal_prefix = next(
            (
                prefix
                for prefix in ("b", "c", "")
                if source.startswith(prefix + '"', index)
            ),
            None,
        )
        if literal_prefix is not None:
            index = consume_literal(index, len(literal_prefix))
            tokens.append(RustToken("<literal>", token_line))
            continue
        if source.startswith("b'", index):
            index = consume_literal(index, 1)
            tokens.append(RustToken("<literal>", token_line))
            continue
        if character == "'":
            if (
                index + 2 < len(source)
                and source[index + 2] == "'"
                and source[index + 1] not in {"'", "\\", "\n"}
            ):
                index += 3
                tokens.append(RustToken("<literal>", token_line))
                continue
            if index + 1 < len(source) and source[index + 1] == "\\":
                index = consume_literal(index)
                tokens.append(RustToken("<literal>", token_line))
                continue
            cursor = index + 1
            if cursor < len(source) and (
                source[cursor].isalpha() or source[cursor] == "_"
            ):
                cursor += 1
                while cursor < len(source) and (
                    source[cursor].isalnum() or source[cursor] == "_"
                ):
                    cursor += 1
                tokens.append(RustToken(source[index:cursor], token_line))
                index = cursor
                continue
            index = consume_literal(index)
            tokens.append(RustToken("<literal>", token_line))
            continue
        if (
            source.startswith("r#", index)
            and index + 2 < len(source)
            and (source[index + 2].isalpha() or source[index + 2] == "_")
        ):
            cursor = index + 3
            while cursor < len(source) and (
                source[cursor].isalnum() or source[cursor] == "_"
            ):
                cursor += 1
            tokens.append(RustToken(source[index:cursor], token_line))
            index = cursor
            continue
        if character.isalpha() or character == "_":
            cursor = index + 1
            while cursor < len(source) and (
                source[cursor].isalnum() or source[cursor] == "_"
            ):
                cursor += 1
            tokens.append(RustToken(source[index:cursor], token_line))
            index = cursor
            continue
        punctuation = next(
            (
                value
                for value in (
                    "::",
                    "->",
                    "=>",
                    "..=",
                    "...",
                    "..",
                    "<<",
                    ">>",
                    "&&",
                    "||",
                )
                if source.startswith(value, index)
            ),
            character,
        )
        tokens.append(RustToken(punctuation, token_line))
        index += len(punctuation)
    return tokens


def rust_identifier(token: RustToken) -> bool:
    value = rust_identifier_text(token)
    return value is not None


def rust_identifier_text(token: RustToken) -> str | None:
    value = token.text.removeprefix("r#")
    return (
        value
        if bool(value)
        and (value[0].isalpha() or value[0] == "_")
        and all(character.isalnum() or character == "_" for character in value[1:])
        else None
    )


def rust_attribute_spans(tokens: list[RustToken], path: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    index = 0
    while index < len(tokens):
        if tokens[index].text != "#":
            index += 1
            continue
        opening = index + 1
        if opening < len(tokens) and tokens[opening].text == "!":
            opening += 1
        if opening >= len(tokens) or tokens[opening].text != "[":
            index += 1
            continue
        depth = 1
        cursor = opening + 1
        while cursor < len(tokens) and depth:
            if tokens[cursor].text == "[":
                depth += 1
            elif tokens[cursor].text == "]":
                depth -= 1
            cursor += 1
        if depth:
            raise ArchitectureError(
                f"cannot parse Rust attribute boundary {path}:{tokens[index].line}"
            )
        spans.append((opening + 1, cursor - 1))
        index = cursor
    return spans


def rust_composition_violation(
    tokens: list[RustToken], path: str
) -> tuple[str, int] | None:
    for index, token in enumerate(tokens):
        if (
            rust_identifier_text(token) == "include"
            and index + 1 < len(tokens)
            and tokens[index + 1].text == "!"
        ):
            return "include! invocation", token.line
    for start, end in rust_attribute_spans(tokens, path):
        for index in range(start, end - 1):
            if (
                rust_identifier_text(tokens[index]) == "path"
                and tokens[index + 1].text == "="
            ):
                return "#[path] module target", tokens[index].line
    return None


def rust_source_inventory(
    source: str,
    path: str,
    *,
    tokens: list[RustToken] | None = None,
) -> tuple[
    list[tuple[str, int]],
    list[tuple[str, int]],
    list[tuple[str, int]],
    list[tuple[str, int]],
    list[tuple[str, int]],
    list[int],
    list[int],
]:
    if tokens is None:
        tokens = rust_tokens(source, path)
    imports: list[tuple[str, int]] = []
    items: list[tuple[str, int]] = []
    restricted_visibilities: list[tuple[str, int]] = []
    public_lines = []
    for index, token in enumerate(tokens):
        if token.text != "pub":
            continue
        if index + 1 < len(tokens) and tokens[index + 1].text == "(":
            cursor = index + 1
            depth = 0
            while cursor < len(tokens):
                if tokens[cursor].text == "(":
                    depth += 1
                elif tokens[cursor].text == ")":
                    depth -= 1
                    if depth == 0:
                        break
                cursor += 1
            if cursor >= len(tokens):
                raise ArchitectureError(
                    f"cannot parse Rust visibility boundary {path}:{token.line}"
                )
            if cursor + 1 < len(tokens):
                visibility = " ".join(
                    value.text for value in tokens[index : cursor + 2]
                )
                restricted_visibilities.append((visibility, token.line))
                continue
        public_lines.append(token.line)
    macro_export_lines = [
        tokens[index].line
        for start, end in rust_attribute_spans(tokens, path)
        for index in range(start, end)
        if rust_identifier_text(tokens[index]) == "macro_export"
    ]
    ignored: set[int] = set()

    index = 0
    glob_lines: list[int] = []
    while index < len(tokens):
        if (
            tokens[index].text == "extern"
            and index + 1 < len(tokens)
            and tokens[index + 1].text == "crate"
        ):
            cursor = index + 2
            while cursor < len(tokens) and tokens[cursor].text != ";":
                ignored.add(cursor)
                cursor += 1
            if cursor >= len(tokens):
                raise ArchitectureError(
                    f"cannot parse Rust extern crate boundary {path}:{tokens[index].line}"
                )
            name = tokens[index + 2].text if index + 2 < cursor else "<missing>"
            imports.append((f"extern crate {name}", tokens[index].line))
            ignored.update(range(index, cursor + 1))
            index = cursor + 1
            continue
        if tokens[index].text != "use":
            index += 1
            continue
        cursor = index + 1
        brace_depth = 0
        while cursor < len(tokens):
            if tokens[cursor].text == "{":
                brace_depth += 1
            elif tokens[cursor].text == "}":
                brace_depth -= 1
            elif tokens[cursor].text == ";" and brace_depth == 0:
                break
            cursor += 1
        if cursor >= len(tokens) or brace_depth != 0:
            raise ArchitectureError(
                f"cannot parse Rust use boundary {path}:{tokens[index].line}"
            )
        statement = tokens[index : cursor + 1]
        imports.append(
            (" ".join(token.text for token in statement), tokens[index].line)
        )
        if any(token.text == "*" for token in statement):
            glob_lines.append(tokens[index].line)
        ignored.update(range(index, cursor + 1))
        index = cursor + 1

    qualified_paths: list[tuple[str, int]] = []
    relative_paths: list[tuple[str, int]] = []
    casa_root = re.compile(r"^(?:casa_[A-Za-z0-9_]+|casars(?:_[A-Za-z0-9_]+)?)$")
    for index, token in enumerate(tokens):
        root = rust_identifier_text(token)
        has_separator_before = index > 0 and tokens[index - 1].text == "::"
        leading_absolute_root = has_separator_before and (
            index < 2 or tokens[index - 2].text != "::"
        )
        if (
            index in ignored
            or root is None
            or (
                casa_root.fullmatch(root) is None
                and root not in {"crate", "self", "super"}
            )
            or (has_separator_before and not leading_absolute_root)
            or index + 2 >= len(tokens)
            or tokens[index + 1].text != "::"
            or not rust_identifier(tokens[index + 2])
        ):
            continue
        segments = [root, tokens[index + 2].text]
        cursor = index + 3
        while (
            cursor + 1 < len(tokens)
            and tokens[cursor].text == "::"
            and rust_identifier(tokens[cursor + 1])
        ):
            segments.append(tokens[cursor + 1].text)
            cursor += 2
        if leading_absolute_root:
            segments[0] = f"::{segments[0]}"
        inventory = (
            relative_paths if root in {"crate", "self", "super"} else qualified_paths
        )
        inventory.append(("::".join(segments), token.line))

    item_keywords = {
        "struct",
        "enum",
        "union",
        "trait",
        "type",
        "fn",
        "const",
        "static",
        "mod",
        "macro",
    }
    for index, token in enumerate(tokens):
        if index in ignored:
            continue
        if (
            token.text == "macro_rules"
            and index + 2 < len(tokens)
            and tokens[index + 1].text == "!"
            and rust_identifier(tokens[index + 2])
        ):
            items.append((f"macro_rules:{tokens[index + 2].text}", token.line))
            continue
        if token.text not in item_keywords or index + 1 >= len(tokens):
            continue
        name_index = index + 1
        if token.text == "static" and tokens[name_index].text == "mut":
            name_index += 1
        if name_index < len(tokens) and rust_identifier(tokens[name_index]):
            items.append((f"{token.text}:{tokens[name_index].text}", token.line))
    return (
        imports,
        qualified_paths,
        relative_paths,
        items,
        restricted_visibilities,
        public_lines + macro_export_lines,
        glob_lines,
    )


def rust_allowlist_violations(
    boundary: dict[str, Any], source: str, relative: str
) -> list[dict[str, Any]]:
    policy = boundary["rust_allowlist"]
    tokens = rust_tokens(source, relative)
    composition = rust_composition_violation(tokens, relative)
    if composition is not None:
        match, line = composition
        return [
            {
                "path": relative,
                "pattern": "rust-composition",
                "match": match,
                "context": match,
                "line": line,
                "message": policy["composition_message"],
            }
        ]
    (
        imports,
        qualified_paths,
        relative_paths,
        items,
        restricted_visibilities,
        privacy_lines,
        glob_lines,
    ) = rust_source_inventory(
        source,
        relative,
        tokens=tokens,
    )
    if privacy_lines:
        return [
            {
                "path": relative,
                "pattern": "rust-privacy",
                "match": "public Rust item",
                "context": "public Rust item",
                "line": min(privacy_lines),
                "message": policy["privacy_message"],
            }
        ]
    allowed_restricted_visibilities = Counter(policy["allowed_restricted_visibilities"])
    observed_restricted_visibilities: Counter[str] = Counter()
    for visibility, line in restricted_visibilities:
        observed_restricted_visibilities[visibility] += 1
        if (
            observed_restricted_visibilities[visibility]
            > allowed_restricted_visibilities[visibility]
        ):
            return [
                {
                    "path": relative,
                    "pattern": "rust-privacy",
                    "match": visibility,
                    "context": visibility,
                    "line": line,
                    "message": policy["privacy_message"],
                }
            ]
    if glob_lines:
        return [
            {
                "path": relative,
                "pattern": "rust-glob",
                "match": "glob import",
                "context": "glob import",
                "line": min(glob_lines),
                "message": policy["glob_message"],
            }
        ]

    allowed_imports = Counter(policy["allowed_imports"])
    observed_imports: Counter[str] = Counter()
    for imported, line in imports:
        observed_imports[imported] += 1
        if observed_imports[imported] > allowed_imports[imported]:
            return [
                {
                    "path": relative,
                    "pattern": "rust-import",
                    "match": imported,
                    "context": imported,
                    "line": line,
                    "message": policy["import_message"],
                }
            ]

    allowed_items = Counter(policy["allowed_items"])
    allowed_qualified_paths = Counter(policy["allowed_qualified_paths"])
    observed_qualified_paths: Counter[str] = Counter()
    for qualified_path, line in qualified_paths:
        observed_qualified_paths[qualified_path] += 1
        if (
            observed_qualified_paths[qualified_path]
            > allowed_qualified_paths[qualified_path]
        ):
            return [
                {
                    "path": relative,
                    "pattern": "rust-qualified-path",
                    "match": qualified_path,
                    "context": qualified_path,
                    "line": line,
                    "message": policy["path_message"],
                }
            ]

    allowed_relative_paths = Counter(policy["allowed_relative_paths"])
    observed_relative_paths: Counter[str] = Counter()
    for relative_path, line in relative_paths:
        observed_relative_paths[relative_path] += 1
        if (
            observed_relative_paths[relative_path]
            > allowed_relative_paths[relative_path]
        ):
            return [
                {
                    "path": relative,
                    "pattern": "rust-relative-path",
                    "match": relative_path,
                    "context": relative_path,
                    "line": line,
                    "message": policy["relative_path_message"],
                }
            ]

    observed_items: Counter[str] = Counter()
    for item, line in items:
        observed_items[item] += 1
        if observed_items[item] > allowed_items[item]:
            return [
                {
                    "path": relative,
                    "pattern": "rust-item",
                    "match": item,
                    "context": item,
                    "line": line,
                    "message": policy["item_message"],
                }
            ]
    if (
        observed_imports != allowed_imports
        or observed_qualified_paths != allowed_qualified_paths
        or observed_relative_paths != allowed_relative_paths
        or observed_items != allowed_items
        or observed_restricted_visibilities != allowed_restricted_visibilities
    ):
        return [
            {
                "path": relative,
                "pattern": "rust-inventory",
                "match": "incomplete Rust inventory",
                "context": "incomplete Rust inventory",
                "line": 1,
                "message": policy["inventory_message"],
            }
        ]
    return []


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
            if boundary.get("rust_allowlist") is not None:
                violations.extend(rust_allowlist_violations(boundary, source, relative))
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


def source_boundary_violation_digest(violations: list[dict[str, Any]]) -> str:
    fingerprint = [
        {
            "path": violation["path"],
            "pattern": violation["pattern"],
            "match": violation["match"],
            "context": violation["context"],
        }
        for violation in violations
    ]
    return stable_digest(fingerprint)


def validate_source_boundaries(
    policy: dict[str, Any], repo_root: Path = REPO_ROOT
) -> None:
    for boundary in policy["source_boundaries"]:
        violations = source_boundary_violations(boundary, repo_root)
        accepted_digest = boundary["accepted_violation_digest"]
        if accepted_digest is None:
            if not violations:
                continue
            violation = violations[0]
            raise ArchitectureError(
                f"{violation['message']}: {violation['path']}:{violation['line']}"
            )
        actual_digest = source_boundary_violation_digest(violations)
        if actual_digest != accepted_digest:
            raise ArchitectureError(
                f"source boundary {boundary['id']} differs from its accepted transitional violations"
            )


def validate_whole_run_router_source(
    policy: dict[str, Any], repo_root: Path = REPO_ROOT
) -> None:
    router = policy["whole_run_router"]
    source_relative = Path(router["source"])
    source_path = repo_root / source_relative
    try:
        source = source_path.read_text(encoding="utf-8")
    except OSError as error:
        raise ArchitectureError(
            f"whole-run migration router cannot read {source_relative}: {error}"
        ) from error

    matrix_name = Path(policy["migration_matrix"]).name
    if "include_str!" not in source or matrix_name not in source:
        raise ArchitectureError(
            "whole-run migration router must embed the authoritative migration matrix"
        )

    owner_symbols = [router["router_type"], *router["engine_ports"]]
    owners: dict[str, list[str]] = {symbol: [] for symbol in owner_symbols}
    crates_root = repo_root / "crates"
    if not crates_root.is_dir():
        raise ArchitectureError("whole-run migration router cannot inspect crates")
    for crate in sorted(crates_root.iterdir()):
        source_root = crate / "src"
        if not source_root.is_dir():
            continue
        for path in sorted(source_root.rglob("*.rs")):
            try:
                candidate = path.read_text(encoding="utf-8")
            except OSError as error:
                raise ArchitectureError(
                    f"whole-run migration router cannot read {path}: {error}"
                ) from error
            for symbol in owner_symbols:
                definition = re.compile(
                    rf"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?(?:struct|enum|trait|type)\s+{re.escape(symbol)}\b"
                )
                owners[symbol].extend(
                    str(path.relative_to(repo_root))
                    for _match in definition.finditer(candidate)
                )

    expected_owner = str(source_relative)
    for symbol, actual_owners in owners.items():
        if actual_owners != [expected_owner]:
            raise ArchitectureError(
                f"whole-run router symbol {symbol} must be owned exactly once by "
                f"{expected_owner}: found={actual_owners}"
            )

    dispatch = re.compile(
        rf"(?m)^\s*pub\s+fn\s+{re.escape(router['dispatch_method'])}\b"
    )
    if len(dispatch.findall(source)) != 1:
        raise ArchitectureError(
            "whole-run migration router must expose exactly one accepted dispatch method"
        )


def format_edges(edges: list[tuple[str, str, str]]) -> str:
    return (
        "["
        + ", ".join(f"{source}->{target}({kind})" for source, target, kind in edges)
        + "]"
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


def rust_unit_enum_variants(path: Path, identifier: str) -> set[str]:
    try:
        source = path.read_text(encoding="utf-8")
    except OSError as error:
        raise ArchitectureError(
            f"cannot read Rust enum source {display_path(path)}: {error}"
        ) from error
    declaration = re.search(
        rf"\b(?:pub(?:\([^)]*\))?\s+)?enum\s+{re.escape(identifier)}\s*\{{",
        source,
    )
    if declaration is None:
        raise ArchitectureError(
            f"cannot find Rust enum {identifier} in {display_path(path)}"
        )
    depth = 1
    end = declaration.end()
    while end < len(source) and depth:
        character = source[end]
        if character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
        end += 1
    if depth:
        raise ArchitectureError(
            f"Rust enum {identifier} in {display_path(path)} has no closing brace"
        )
    body = source[declaration.end() : end - 1]
    variants = set(re.findall(r"(?m)^\s*([A-Z][A-Za-z0-9_]*)\s*,\s*$", body))
    if not variants:
        raise ArchitectureError(
            f"Rust enum {identifier} in {display_path(path)} has no unit variants"
        )
    return variants


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


def validate_rust_enum_inventory(
    matrix: dict[str, Any],
    field: str,
    enum_path: Path,
    enum_identifier: str,
    accepted_digest: str,
) -> None:
    inventory = matrix.get(field)
    if not isinstance(inventory, dict) or not inventory:
        raise ArchitectureError(f"migration matrix {field} must be a non-empty object")
    for variant, row in inventory.items():
        require_string(variant, f"migration matrix {field} key")
        require_string(row, f"migration matrix {field}.{variant}")
    variants = rust_unit_enum_variants(enum_path, enum_identifier)
    if set(inventory) != variants:
        raise ArchitectureError(
            f"migration matrix {field} differs from {enum_identifier}: "
            f"added={sorted(set(inventory) - variants)}, "
            f"removed={sorted(variants - set(inventory))}"
        )
    if stable_digest(inventory) != accepted_digest:
        raise ArchitectureError(
            f"migration matrix {field} differs from {enum_identifier}"
        )


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
            "LegacyWholeRun",
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
    if enforce_accepted_scope:
        if stable_digest(inventory) != ACCEPTED_MATRIX_INVENTORY_SHA256:
            raise ArchitectureError(
                "migration matrix inventory differs from the canonical imaging inventory"
            )
        validate_rust_enum_inventory(
            matrix,
            "product_kind_inventory",
            REPO_ROOT / "crates/casa-imaging-model/src/compiled_problem.rs",
            "ProductKind",
            ACCEPTED_PRODUCT_KIND_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "plane_selection_inventory",
            REPO_ROOT / "crates/casars-imager/src/task_contract.rs",
            "ImagerPlaneSelection",
            ACCEPTED_PLANE_SELECTION_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "polarization_coordinate_inventory",
            REPO_ROOT / "crates/casa-imaging-model/src/compiled_problem.rs",
            "PolarizationCoordinate",
            ACCEPTED_POLARIZATION_COORDINATE_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "cube_interpolation_inventory",
            REPO_ROOT / "crates/casa-ms/src/spectral_selection.rs",
            "CubeInterpolation",
            ACCEPTED_CUBE_INTERPOLATION_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "standard_mfs_backend_inventory",
            REPO_ROOT / "crates/casa-imaging/src/lib.rs",
            "StandardMfsBackend",
            ACCEPTED_STANDARD_MFS_BACKEND_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "spectral_mode_inventory",
            REPO_ROOT / "crates/casars-imager/src/lib.rs",
            "SpectralMode",
            ACCEPTED_SPECTRAL_MODE_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "imager_spectral_mode_inventory",
            REPO_ROOT / "crates/casars-imager/src/task_contract.rs",
            "ImagerSpectralMode",
            ACCEPTED_IMAGER_SPECTRAL_MODE_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "gridder_request_inventory",
            REPO_ROOT / "crates/casars-imager/src/lib.rs",
            "GridderRequest",
            ACCEPTED_GRIDDER_REQUEST_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "deconvolver_inventory",
            REPO_ROOT / "crates/casa-imaging/src/types.rs",
            "Deconvolver",
            ACCEPTED_DECONVOLVER_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "imager_deconvolver_inventory",
            REPO_ROOT / "crates/casars-imager/src/task_contract.rs",
            "ImagerDeconvolver",
            ACCEPTED_IMAGER_DECONVOLVER_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "imager_cube_interpolation_inventory",
            REPO_ROOT / "crates/casars-imager/src/task_contract.rs",
            "ImagerCubeInterpolation",
            ACCEPTED_IMAGER_CUBE_INTERPOLATION_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "fft_backend_choice_inventory",
            REPO_ROOT / "crates/casa-imaging/src/fft_backend.rs",
            "FftBackendChoice",
            ACCEPTED_FFT_BACKEND_CHOICE_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "imaging_fft_backend_policy_inventory",
            REPO_ROOT / "crates/casars-imager/src/lib.rs",
            "ImagingFftBackendPolicy",
            ACCEPTED_IMAGING_FFT_BACKEND_POLICY_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "standard_mfs_acceleration_policy_inventory",
            REPO_ROOT / "crates/casars-imager/src/lib.rs",
            "StandardMfsAccelerationPolicy",
            ACCEPTED_STANDARD_MFS_ACCELERATION_POLICY_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "standard_mfs_minor_cycle_backend_inventory",
            REPO_ROOT / "crates/casa-imaging/src/lib.rs",
            "StandardMfsMinorCycleBackend",
            ACCEPTED_STANDARD_MFS_MINOR_CYCLE_BACKEND_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "single_plane_acceleration_policy_inventory",
            REPO_ROOT / "crates/casa-imaging/src/single_plane_plan.rs",
            "SinglePlaneAccelerationPolicy",
            ACCEPTED_SINGLE_PLANE_ACCELERATION_POLICY_INVENTORY_SHA256,
        )
        validate_rust_enum_inventory(
            matrix,
            "per_plane_execution_backend_inventory",
            REPO_ROOT / "crates/casars-imager/src/lib.rs",
            "PerPlaneExecutionBackend",
            ACCEPTED_PER_PLANE_EXECUTION_BACKEND_INVENTORY_SHA256,
        )
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
        "crates/casa-imaging-reconstruction/src/weighting.rs::pub fn plan_weighting",
        "crates/casa-imaging-reconstruction/src/weighting.rs::pub fn begin_weighting_generation",
        "crates/casa-imaging-reconstruction/src/weighting.rs::pub struct WeightingDensityPhase",
        "crates/casa-imaging-reconstruction/src/weighting.rs::pub struct WeightingSumWeightPhase",
        "crates/casa-imaging-reconstruction/src/weighting.rs::pub struct WeightedObservationBlock",
        "crates/casa-imaging-runtime/src/weighting.rs::pub fn freeze_weighting_generation",
        "crates/casa-imaging-runtime/src/weighting.rs::pub struct FrozenWeightingGeneration",
        "crates/casa-imaging-runtime/src/weighting.rs::pub struct WeightingReplayCompletion",
        "crates/casa-imaging-runtime/src/receipt.rs::fn project_weighting",
    }
    required_baselines = {
        "repo://crates/casa-imaging-model/src/measurement_equation.rs",
        "repo://crates/casa-imaging-reconstruction/src/weighting.rs",
        "repo://crates/casa-imaging-runtime/src/weighting.rs",
        "repo://crates/casa-imaging-runtime/src/receipt.rs",
    }
    if not required_evidence.issubset(set(row.get("source_evidence", []))):
        raise ArchitectureError("T18 lacks the accepted weighting-owner source evidence")
    if not required_baselines.issubset(set(row.get("baseline_manifests", []))):
        raise ArchitectureError("T18 lacks pinned weighting-owner baseline evidence")

    model_path = REPO_ROOT / "crates/casa-imaging-model/src/measurement_equation.rs"
    weighting_path = REPO_ROOT / "crates/casa-imaging-reconstruction/src/weighting.rs"
    runtime_weighting_path = REPO_ROOT / "crates/casa-imaging-runtime/src/weighting.rs"
    receipt_path = REPO_ROOT / "crates/casa-imaging-runtime/src/receipt.rs"
    try:
        model = model_path.read_text(encoding="utf-8")
        weighting = weighting_path.read_text(encoding="utf-8")
        runtime_weighting = runtime_weighting_path.read_text(encoding="utf-8")
        receipt = receipt_path.read_text(encoding="utf-8")
    except OSError as error:
        raise ArchitectureError(f"cannot inspect T18 weighting sources: {error}") from error
    validate_t18_global_weighting_sources(
        model,
        weighting,
        runtime_weighting,
        receipt,
        model_path=model_path,
        weighting_path=weighting_path,
        runtime_weighting_path=runtime_weighting_path,
        receipt_path=receipt_path,
    )


def validate_t18_global_weighting_sources(
    model: str,
    weighting: str,
    runtime_weighting: str,
    receipt: str,
    *,
    model_path: Path,
    weighting_path: Path,
    runtime_weighting_path: Path,
    receipt_path: Path,
) -> None:
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

    frozen_fields = rust_struct_fields(runtime_weighting, "FrozenWeightingGeneration", runtime_weighting_path)
    for field in (
        "state",
        "density_completion",
        "sum_weight_completion",
    ):
        if field not in frozen_fields:
            raise ArchitectureError("T18 frozen generation omits reconstruction or T17 evidence")
    if frozen_fields.get("density_completion") != "SelectedObservationCompletion" or frozen_fields.get("sum_weight_completion") != "SelectedObservationCompletion":
        raise ArchitectureError("T18 frozen generation does not retain concrete opaque T17 completions")
    freeze = rust_function_body(runtime_weighting, "freeze_weighting_generation", runtime_weighting_path)
    if freeze.count(".traverse(") != 2:
        raise ArchitectureError(
            "T18 frozen generation must require distinct exhaustive density and sum-weight passes"
        )
    replay = rust_impl_method_body(runtime_weighting, "FrozenWeightingGeneration", "replay", runtime_weighting_path)
    if replay.count(".traverse(") != 1 or ".finish(" not in replay:
        raise ArchitectureError(
            "T18 replay must require its own exhaustive traversal and exact coverage"
        )
    if "IntoIterator" in weighting or "inspect_selected_observation(" in weighting:
        raise ArchitectureError("T18 reconstruction exposes a bypass around T17 callback traversal")
    if "SelectedObservationGenerationId" in weighting:
        raise ArchitectureError("T18 reconstruction accepts caller-authored T17 completion identity")
    replay_completion = rust_struct_fields(runtime_weighting, "WeightingReplayCompletion", runtime_weighting_path)
    if replay_completion.get("owner_completion") != "SelectedObservationCompletion":
        raise ArchitectureError("T18 replay completion does not retain concrete opaque T17 evidence")
    if re.search(
        r"\bpub\s+(?:const\s+)?fn\s+(?:from_sha256|from_identity|new_generation)\b",
        weighting,
    ):
        raise ArchitectureError("T18 exposes a raw weighting evidence constructor")
    block_fields = rust_struct_fields(weighting, "WeightedObservationBlock", weighting_path)
    sample_fields = rust_struct_fields(weighting, "WeightedObservationSample", weighting_path)
    if block_fields.get("generation") != "WeightingGenerationId" or sample_fields.get(
        "generation"
    ) != "WeightingGenerationId":
        raise ArchitectureError("T18 weighted replay does not carry one opaque W generation")
    residency_fields = rust_struct_fields(weighting, "WeightingResidency", weighting_path)
    required_residency = {
        "density_grid_bytes",
        "deterministic_partial_bytes",
        "reduction_scratch_bytes",
        "replay_read_bytes",
        "weighted_block_bytes",
        "queue_bytes",
        "simultaneous_selected_weighted_bytes",
        "peak_bytes",
    }
    if not required_residency.issubset(residency_fields):
        raise ArchitectureError("T18 residency omits a weighting-owned buffer class")
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
        "crates/casars-imager/src/lib.rs::select_main_rows",
    }
    if not required_evidence.issubset(set(row.get("source_evidence", []))):
        raise ArchitectureError(
            "capability.ms-selection lacks the accepted T17 traversal/resource/completion evidence"
        )
    required_baselines = {
        "repo://crates/casa-imaging-model/src/selected_observation_sample.rs",
        "repo://resources/imaging-architecture/baselines/selected-observation-generation-v3.txt",
    }
    if not required_baselines.issubset(set(row.get("baseline_manifests", []))):
        raise ArchitectureError(
            "capability.ms-selection lacks pinned T17 generation source and fixture evidence"
        )

    imager_path = REPO_ROOT / "crates/casars-imager/src/lib.rs"
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
        imager = imager_path.read_text(encoding="utf-8")
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

    forbidden_imager_patterns = {
        r"\bMsSelection\b": "casars-imager retains the legacy selection request",
        r"\bResolvedMsSelectionRow\b": "casars-imager retains the legacy resolved-row contract",
        r"\.resolve_selection\s*\(": "casars-imager can still reach legacy MS selection evaluation",
    }
    for pattern, message in forbidden_imager_patterns.items():
        if re.search(pattern, imager):
            raise ArchitectureError(message)
    imager_selection = rust_function_body(imager, "select_main_rows", imager_path)
    if ".visit_selected_observation_rows(" not in imager_selection:
        raise ArchitectureError(
            "casars-imager must delegate row evaluation to canonical selected-observation access"
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
        or ".main_row_selection_blocks(" not in frontend_projection
    ):
        raise ArchitectureError(
            "frontend row projection does not own the canonical bounded T17 predicate traversal"
        )
    if (
        ".ordered_main_rows()" not in access
        or ".row_predicate" not in access
        or ".main_row_selection_blocks(" in access
        or "struct SelectedMainRows" in access
    ):
        raise ArchitectureError(
            "retained selected-observation access must seek by the exact model manifest and must not scan the MAIN row span"
        )
    forbidden_incremental_evidence_patterns = {
        r"pub\s+struct\s+SelectedObservationInspection\b": (
            "selected-observation incremental evidence state is public"
        ),
        r"pub\s+fn\s+selected_observation_inspection\s*\(": (
            "compiled problem exposes an incremental evidence factory"
        ),
    }
    combined_model_surface = "\n".join((model, compiled_problem))
    for pattern, message in forbidden_incremental_evidence_patterns.items():
        if re.search(pattern, combined_model_surface):
            raise ArchitectureError(message)
    if re.search(r"\bSelectedObservationInspection\b", model_lib):
        raise ArchitectureError(
            "casa-imaging-model re-exports incremental evidence state"
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
        or "Arc::ptr_eq(&self.ordered_main_rows,&rows.ordered_main_rows)"
        not in selected_rows_projection
        or "Arc::ptr_eq(&self.used_data_description_ids,&rows.used_data_description_ids,)"
        not in selected_rows_projection
        or selected_rows_projection.count("2*size_of::<usize>()") != 2
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
        or "letbinding_slot_bytes=bindings.capacity().checked_mul(size_of::<ObservationSourceBinding>())"
        not in bound_open
        or "letbinding_graph_initialization_bytes=bindings.iter().enumerate().try_fold(binding_slot_bytes,"
        not in bound_open
        or ".additional_retained_heap_bytes(already_accounted_rows)" not in bound_open
        or "bindings[..binding_index].iter().map(|prior|prior.current_state.selected_rows())"
        not in bound_open
        or ".capacity().checked_mul(BoundObservationSource::retained_source_slot_bytes())"
        not in bound_open
        or bound_open.count("source_index==0") != 1
        or "letshared_bytes=ifsource_index==0{SelectedObservationSharedBytes::new(measures.retained_bytes(),source_slots_retained_bytes,binding_graph_initialization_bytes,)}else{SelectedObservationSharedBytes::NONE};"
        not in bound_open
        or bound_open.count("measures.retained_bytes()") != 1
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
        "letsynchronous_observation_read=work.node().kind==WorkKind::ObservationRead&&work.node().fences.is_empty();",
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
                "status": "LegacyWholeRun",
                "current_owner": "synthetic legacy",
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
        validate_source_boundaries(policy)
        validate_whole_run_router_source(policy)

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
            f"{len(policy['frozen_legacy_workspace_edges'])} frozen legacy edges, "
            f"{len(policy['frozen_transitional_workspace_edges'])} frozen transitional edges, "
            f"{matrix_rows} migration rows"
        )
        return 0
    except ArchitectureError as error:
        print(f"imaging-architecture: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
