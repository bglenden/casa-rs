#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Checked-in, JSON-file CASA ``tclean`` execution protocol.

The module deliberately keeps CASA imports inside the real execution path.  Its
recipe parsing, request validation, cache receipts, and product inventories are
ordinary Python so benchmark plans can be reviewed and tested without CASA.
"""

from __future__ import annotations

import ast
import copy
import ctypes
import hashlib
import inspect
import json
import math
import os
import pathlib
import platform
import resource
import statistics
import sys
import tempfile
import time
from typing import Any, Callable

try:
    from .casa_runtime_identity import (
        capture_runtime_identity,
        stable_identity_projection,
        stable_identity_sha256,
        validate_identity as validate_runtime_identity,
    )
    from .tree_identity import sha256_file, tree_identity
except ImportError:  # Executed directly by the CASA Python interpreter.
    from casa_runtime_identity import (
        capture_runtime_identity,
        stable_identity_projection,
        stable_identity_sha256,
        validate_identity as validate_runtime_identity,
    )
    from tree_identity import sha256_file, tree_identity


REQUEST_SCHEMA_VERSION = 2
RESULT_SCHEMA_VERSION = 3
CACHE_RECEIPT_SCHEMA_VERSION = 2

REQUEST_KIND = "casa_tclean_request"
RESULT_KIND = "casa_tclean_result"
CACHE_RECEIPT_KIND = "casa_tclean_cf_cache_receipt"

REQUEST_FIELDS = {
    "schema_version",
    "kind",
    "request_id",
    "action",
    "expected_casa_version",
    "recipe",
    "overrides",
    "cache",
    "mask_identity",
}
RECIPE_FIELDS = {"path", "sha256", "task", "parameter_names"}
CACHE_FIELDS = {
    "role",
    "path",
    "plan",
    "plan_sha256",
    "receipt_path",
    "expected_stable_tree_sha256",
}
RESULT_FIELDS = {
    "schema_version",
    "kind",
    "status",
    "request_id",
    "action",
    "casa",
    "recipe",
    "compatibility_normalizations",
    "version_defaults",
    "reproducibility_overrides",
    "effective_kwargs",
    "effective_kwargs_sha256",
    "cache",
    "mask_identity",
    "wall_seconds",
    "stage_timings_seconds",
    "resources",
    "products",
    "tclean_return",
    "failure",
}
LEGACY_CACHE_RECEIPT_FIELDS = {
    "schema_version",
    "kind",
    "cache_path",
    "plan_sha256",
    "stable_tree_sha256",
    "inventory",
}
CACHE_RECEIPT_FIELDS = LEGACY_CACHE_RECEIPT_FIELDS | {
    "producer",
}
PRODUCER_RECEIPT_FIELDS = {
    "request_id",
    "effective_kwargs_sha256",
    "product_inventory",
    "product_inventory_sha256",
}

CF_CACHE_PARAMETER_FIELDS = (
    "field",
    "spw",
    "imsize",
    "cell",
    "phasecenter",
    "stokes",
    "projection",
    "specmode",
    "reffreq",
    "nchan",
    "start",
    "width",
    "outframe",
    "veltype",
    "restfreq",
    "interpolation",
    "gridder",
    "facets",
    "psfphasecenter",
    "wprojplanes",
    "vptable",
    "aterm",
    "psterm",
    "wbawp",
    "conjbeams",
    "usepointing",
    "computepastep",
    "rotatepastep",
    "pointingoffsetsigdev",
    "pblimit",
)

RESOURCE_FIELDS = (
    "user_cpu_seconds",
    "system_cpu_seconds",
    "peak_rss_bytes",
    "minor_page_faults",
    "major_page_faults",
    "block_input_operations",
    "block_output_operations",
    "disk_read_bytes",
    "disk_write_bytes",
    "voluntary_context_switches",
    "involuntary_context_switches",
)
STAGE_TIMING_FIELDS = (
    "protocol_preflight",
    "tclean_task",
    "product_inventory",
    "cache_postcondition",
    "protocol_total",
)

SUPPORTED_ACTIONS = {"plan", "run"}
SUPPORTED_CACHE_ROLES = {"none", "cold", "warm"}
RESULT_STATUSES = {
    "planned",
    "completed",
    "recovered_publication",
    "failed_validation",
    "failed_execution",
    "failed_postcondition",
}

# These are the only science-affecting differences the frozen VLASS evidence
# plan permits between the archived tclean.last and a reproducible invocation.
# Values are still validated below; membership alone does not make an override
# valid.
REPRODUCIBILITY_OVERRIDE_FIELDS = {
    "vis",
    "field",
    "phasecenter",
    "imagename",
    "datacolumn",
    "interactive",
    "parallel",
    "cfcache",
    "restart",
    "niter",
    "imsize",
    "spw",
    "mask",
}

# CASA 6.7.5.9 added these public task parameters after the archived recipe was
# written.  Expanding them explicitly makes the effective-call digest complete,
# while the result keeps them separate from user-approved recipe overrides.
CASA_6_7_5_9_NEW_DEFAULTS: dict[str, Any] = {
    "fullsummary": False,
    "fusedthreshold": 0.0,
    "largestscale": -1,
    "nmajor": -1,
    "psfcutoff": 0.35,
}

VOLATILE_TREE_FILE_NAMES = {"table.lock"}


class ProtocolError(ValueError):
    """A request, recipe, cache receipt, or output violates the protocol."""


def canonical_json_bytes(value: Any) -> bytes:
    """Return the canonical JSON representation used by all protocol hashes."""

    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise ProtocolError(f"value is not canonical JSON: {error}") from error
    return encoded.encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def cf_cache_parameter_identity(
    effective_parameters: dict[str, Any],
) -> dict[str, Any]:
    """Project an effective CASA call onto CF-affecting parameters.

    This projection lives in the subprocess protocol so the untrusted request
    cannot merely assert a self-consistent cache-plan hash.  The CASA process
    independently re-derives the science identity from the call it will make.
    """

    missing = [
        name for name in CF_CACHE_PARAMETER_FIELDS if name not in effective_parameters
    ]
    if missing:
        raise ProtocolError(
            "effective CASA recipe is missing CF identity parameter(s): "
            + ", ".join(missing)
        )
    vptable = effective_parameters["vptable"]
    if not isinstance(vptable, str):
        raise ProtocolError("effective CASA vptable must be a string")
    if vptable:
        raise ProtocolError(
            "non-empty CASA vptable requires a content-addressed CF identity"
        )
    return {
        name: copy.deepcopy(effective_parameters[name])
        for name in CF_CACHE_PARAMETER_FIELDS
    }


def parse_literal_assignment_recipe(
    text: str, *, source: str = "tclean.last"
) -> dict[str, Any]:
    """Parse a CASA ``*.last`` file without executing Python.

    Only one-name assignments whose values are accepted by ``ast.literal_eval``
    are legal.  Comments, including CASA's commented full task invocation, do
    not appear in the AST and therefore cannot execute.
    """

    try:
        tree = ast.parse(text, filename=source, mode="exec")
    except SyntaxError as error:
        raise ProtocolError(
            f"{source}: invalid Python assignment syntax: {error}"
        ) from error

    assignments: dict[str, Any] = {}
    for node in tree.body:
        if not (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        ):
            raise ProtocolError(
                f"{source}:{getattr(node, 'lineno', '?')}: only literal name assignments are allowed"
            )
        name = node.targets[0].id
        if name in assignments:
            raise ProtocolError(
                f"{source}:{node.lineno}: duplicate assignment for {name!r}"
            )
        try:
            value = ast.literal_eval(node.value)
        except (ValueError, TypeError, SyntaxError) as error:
            raise ProtocolError(
                f"{source}:{node.lineno}: {name!r} is not a literal value"
            ) from error
        _validate_json_value(value, source=f"{source}:{node.lineno}:{name}")
        assignments[name] = value

    if not assignments:
        raise ProtocolError(f"{source}: recipe contains no assignments")
    return assignments


def load_validated_recipe(recipe_spec: dict[str, Any]) -> dict[str, Any]:
    _require_exact_fields(recipe_spec, RECIPE_FIELDS, source="request.recipe")
    path = _absolute_path(recipe_spec.get("path"), field="request.recipe.path")
    expected_sha256 = _sha256_value(
        recipe_spec.get("sha256"), field="request.recipe.sha256"
    )
    task = _nonempty_string(recipe_spec.get("task"), field="request.recipe.task")
    if task != "tclean":
        raise ProtocolError("request.recipe.task must be 'tclean'")
    expected_names = _sorted_unique_strings(
        recipe_spec.get("parameter_names"), field="request.recipe.parameter_names"
    )
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise ProtocolError(f"cannot read recipe {path}: {error}") from error
    actual_sha256 = hashlib.sha256(payload).hexdigest()
    if actual_sha256 != expected_sha256:
        raise ProtocolError(
            f"recipe sha256 mismatch: expected {expected_sha256}, got {actual_sha256}"
        )
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ProtocolError(f"recipe {path} is not UTF-8: {error}") from error
    assignments = parse_literal_assignment_recipe(text, source=str(path))
    if assignments.get("taskname") != task:
        raise ProtocolError(
            f"recipe taskname mismatch: expected {task!r}, got {assignments.get('taskname')!r}"
        )
    actual_names = sorted(name for name in assignments if name != "taskname")
    if actual_names != expected_names:
        missing = sorted(set(expected_names) - set(actual_names))
        unexpected = sorted(set(actual_names) - set(expected_names))
        raise ProtocolError(
            "recipe parameter-name mismatch"
            f"; missing={missing or 'none'}; unexpected={unexpected or 'none'}"
        )
    return {
        "path": str(path),
        "sha256": actual_sha256,
        "task": task,
        "parameter_names": actual_names,
        "archived_parameters": {name: assignments[name] for name in actual_names},
    }


def normalize_archived_parameters(
    archived_parameters: dict[str, Any], overrides: dict[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Translate known archived-task API drift and apply approved overrides."""

    parameters = dict(archived_parameters)
    normalizations: list[dict[str, Any]] = []

    if "chanchunks" in parameters:
        chanchunks = parameters.pop("chanchunks")
        if isinstance(chanchunks, bool) or chanchunks != 1:
            raise ProtocolError(
                "archived chanchunks can only be omitted when its value is exactly 1"
            )
        normalizations.append(
            {
                "parameter": "chanchunks",
                "archived_value": 1,
                "effective_action": "omitted",
                "reason": "CASA 6.7.5.9 removed the public parameter and forces chanchunks=1",
            }
        )

    if "pointingoffsetsigdev" in parameters:
        pointing_sigma = parameters["pointingoffsetsigdev"]
        if isinstance(pointing_sigma, bool):
            raise ProtocolError("pointingoffsetsigdev cannot be a boolean")
        if isinstance(pointing_sigma, (int, float)):
            if not math.isfinite(float(pointing_sigma)) or float(pointing_sigma) != 0.0:
                raise ProtocolError(
                    "only archived scalar pointingoffsetsigdev=0.0 has a lossless CASA 6.7.5.9 normalization"
                )
            parameters["pointingoffsetsigdev"] = [0.0]
            normalizations.append(
                {
                    "parameter": "pointingoffsetsigdev",
                    "archived_value": pointing_sigma,
                    "effective_value": [0.0],
                    "reason": "CASA 6.7.5.9 requires a numeric vector",
                }
            )
        elif isinstance(pointing_sigma, list):
            if not all(
                not isinstance(item, bool)
                and isinstance(item, (int, float))
                and math.isfinite(float(item))
                for item in pointing_sigma
            ):
                raise ProtocolError("pointingoffsetsigdev must contain finite numbers")
        else:
            raise ProtocolError("pointingoffsetsigdev must be a scalar or numeric list")

    validated_overrides = validate_reproducibility_overrides(overrides)
    parameters.update(validated_overrides)

    version_defaults: dict[str, Any] = {}
    for name, default in CASA_6_7_5_9_NEW_DEFAULTS.items():
        if name in parameters:
            raise ProtocolError(
                f"archived recipe unexpectedly contains CASA 6.7.5.9-only parameter {name!r}"
            )
        parameters[name] = default
        version_defaults[name] = default

    canonical_json_bytes(parameters)
    return parameters, normalizations, version_defaults


def validate_reproducibility_overrides(overrides: Any) -> dict[str, Any]:
    if not isinstance(overrides, dict):
        raise ProtocolError("request.overrides must be an object")
    unknown = sorted(set(overrides) - REPRODUCIBILITY_OVERRIDE_FIELDS)
    if unknown:
        raise ProtocolError(
            "request.overrides contains non-approved parameter(s): "
            + ", ".join(unknown)
        )

    result = dict(overrides)
    for name in ("vis", "imagename", "cfcache"):
        if name in result:
            result[name] = str(
                _absolute_path(result[name], field=f"request.overrides.{name}")
            )
    if "mask" in result:
        result["mask"] = str(
            _absolute_path(result["mask"], field="request.overrides.mask")
        )
    if "field" in result:
        result["field"] = _nonempty_string(
            result["field"], field="request.overrides.field"
        )
    if "spw" in result:
        result["spw"] = _nonempty_string(result["spw"], field="request.overrides.spw")
    if "phasecenter" in result:
        phasecenter = result["phasecenter"]
        if isinstance(phasecenter, bool) or not isinstance(phasecenter, (int, str)):
            raise ProtocolError(
                "request.overrides.phasecenter must be an integer or string"
            )
        if isinstance(phasecenter, str) and not phasecenter:
            raise ProtocolError("request.overrides.phasecenter must not be empty")
    if "datacolumn" in result and result["datacolumn"] != "data":
        raise ProtocolError("request.overrides.datacolumn must be exactly 'data'")
    for name in ("interactive", "parallel", "restart"):
        if name in result and result[name] is not False:
            raise ProtocolError(f"request.overrides.{name} must be false")
    if "niter" in result:
        value = result["niter"]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ProtocolError(
                "request.overrides.niter must be a non-negative integer"
            )
    if "imsize" in result:
        value = result["imsize"]
        if isinstance(value, bool):
            raise ProtocolError(
                "request.overrides.imsize must be a positive integer or list"
            )
        if isinstance(value, int):
            valid = value > 0
        else:
            valid = (
                isinstance(value, list)
                and len(value) in {1, 2}
                and all(
                    isinstance(item, int) and not isinstance(item, bool) and item > 0
                    for item in value
                )
            )
        if not valid:
            raise ProtocolError(
                "request.overrides.imsize must be a positive integer or 1-2 item list"
            )
    return result


def validate_request(request: Any) -> dict[str, Any]:
    if not isinstance(request, dict):
        raise ProtocolError("request must be a JSON object")
    _require_exact_fields(request, REQUEST_FIELDS, source="request")
    if request.get("schema_version") != REQUEST_SCHEMA_VERSION or isinstance(
        request.get("schema_version"), bool
    ):
        raise ProtocolError(f"request.schema_version must be {REQUEST_SCHEMA_VERSION}")
    if request.get("kind") != REQUEST_KIND:
        raise ProtocolError(f"request.kind must be {REQUEST_KIND!r}")
    _nonempty_string(request.get("request_id"), field="request.request_id")
    action = request.get("action")
    if action not in SUPPORTED_ACTIONS:
        raise ProtocolError("request.action must be 'plan' or 'run'")
    _nonempty_string(
        request.get("expected_casa_version"), field="request.expected_casa_version"
    )
    if not isinstance(request.get("recipe"), dict):
        raise ProtocolError("request.recipe must be an object")
    validate_reproducibility_overrides(request.get("overrides"))
    if not isinstance(request.get("cache"), dict):
        raise ProtocolError("request.cache must be an object")
    validate_cache_spec(request["cache"])
    validate_mask_identity_spec(
        request.get("mask_identity"), overrides=request.get("overrides")
    )
    return dict(request)


def validate_mask_identity_spec(
    value: Any, *, overrides: dict[str, Any]
) -> dict[str, Any] | None:
    """Validate invocation-level mask identity independently of CF reuse."""

    mask = overrides.get("mask") if isinstance(overrides, dict) else None
    if value is None:
        if mask:
            raise ProtocolError(
                "effective deterministic mask has no invocation-level frozen identity"
            )
        return None
    if not isinstance(value, dict) or set(value) != {"kind", "sha256", "identity"}:
        raise ProtocolError(
            "request.mask_identity must contain kind, sha256, and identity"
        )
    if not isinstance(mask, str) or not mask:
        raise ProtocolError("request.mask_identity requires request.overrides.mask")
    kind = value.get("kind")
    if kind not in {"file", "casa_image_tree"}:
        raise ProtocolError(f"unsupported deterministic mask kind: {kind!r}")
    digest = _sha256_value(value.get("sha256"), field="request.mask_identity.sha256")
    identity = value.get("identity")
    if not isinstance(identity, dict):
        raise ProtocolError("request.mask_identity.identity must be an object")
    canonical_json_bytes(identity)
    if kind == "file":
        _require_exact_fields(
            identity, {"size_bytes"}, source="request.mask_identity.identity"
        )
        size = identity.get("size_bytes")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ProtocolError(
                "request.mask_identity.identity.size_bytes must be a non-negative integer"
            )
    else:
        required = {
            "tree_sha256",
            "file_count",
            "size_bytes",
            "excluded_names",
            "excluded_count",
        }
        _require_exact_fields(
            identity, required, source="request.mask_identity.identity"
        )
        if identity.get("tree_sha256") != digest:
            raise ProtocolError(
                "request.mask_identity tree digest does not match identity"
            )
        if identity.get("excluded_names") != ["table.lock"]:
            raise ProtocolError(
                "request.mask_identity tree must exclude exactly table.lock"
            )
        for field in ("file_count", "size_bytes", "excluded_count"):
            item = identity.get(field)
            if isinstance(item, bool) or not isinstance(item, int) or item < 0:
                raise ProtocolError(
                    f"request.mask_identity.identity.{field} must be a non-negative integer"
                )
    return {"kind": kind, "sha256": digest, "identity": dict(identity)}


def validate_cache_spec(cache: dict[str, Any]) -> dict[str, Any]:
    unknown = sorted(set(cache) - CACHE_FIELDS)
    if unknown:
        raise ProtocolError(
            "request.cache contains unknown field(s): " + ", ".join(unknown)
        )
    role = cache.get("role")
    if role not in SUPPORTED_CACHE_ROLES:
        raise ProtocolError("request.cache.role must be none, cold, or warm")
    if role == "none":
        if set(cache) != {"role"}:
            raise ProtocolError("request.cache role none accepts only the role field")
        return {"role": "none"}

    required = {"role", "path", "plan", "plan_sha256", "receipt_path"}
    if role == "warm":
        required.add("expected_stable_tree_sha256")
    missing = sorted(required - set(cache))
    if missing:
        raise ProtocolError("request.cache is missing field(s): " + ", ".join(missing))
    if role == "cold" and "expected_stable_tree_sha256" in cache:
        raise ProtocolError(
            "request.cache.expected_stable_tree_sha256 is valid only for a warm cache"
        )

    cache_path = _absolute_path(cache.get("path"), field="request.cache.path")
    receipt_path = _absolute_path(
        cache.get("receipt_path"), field="request.cache.receipt_path"
    )
    if _path_is_within(receipt_path, cache_path):
        raise ProtocolError("request.cache.receipt_path must be outside the cache tree")
    plan = cache.get("plan")
    if not isinstance(plan, dict):
        raise ProtocolError("request.cache.plan must be an object")
    plan_sha256 = _sha256_value(
        cache.get("plan_sha256"), field="request.cache.plan_sha256"
    )
    actual_plan_sha256 = canonical_sha256(plan)
    if plan_sha256 != actual_plan_sha256:
        raise ProtocolError(
            f"cache plan sha256 mismatch: expected {plan_sha256}, derived {actual_plan_sha256}"
        )
    result = {
        "role": role,
        "path": str(cache_path),
        "plan": plan,
        "plan_sha256": plan_sha256,
        "receipt_path": str(receipt_path),
    }
    if role == "warm":
        result["expected_stable_tree_sha256"] = _sha256_value(
            cache.get("expected_stable_tree_sha256"),
            field="request.cache.expected_stable_tree_sha256",
        )
    return result


def build_invocation_plan(request: dict[str, Any]) -> dict[str, Any]:
    request = validate_request(request)
    recipe = load_validated_recipe(request["recipe"])
    effective_kwargs, normalizations, version_defaults = normalize_archived_parameters(
        recipe["archived_parameters"], request["overrides"]
    )
    cache = validate_cache_spec(request["cache"])
    if cache["role"] == "cold":
        cache_path = pathlib.Path(cache["path"])
        receipt_path = pathlib.Path(cache["receipt_path"])
        request_token = canonical_sha256(request["request_id"])[:16]
        cache["working_path"] = str(
            cache_path.with_name(f".{cache_path.name}.{request_token}.partial")
        )
        cache["working_receipt_path"] = str(
            receipt_path.with_name(f".{receipt_path.name}.{request_token}.partial")
        )
        effective_kwargs["cfcache"] = cache["working_path"]
    elif cache["role"] == "warm":
        cache["working_path"] = cache["path"]
    _validate_cache_matches_effective_call(
        cache,
        effective_kwargs,
        expected_casa_version=request["expected_casa_version"],
        recipe_sha256=recipe["sha256"],
    )
    result = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "kind": RESULT_KIND,
        "status": "planned",
        "request_id": request["request_id"],
        "action": request["action"],
        "casa": {"expected_version": request["expected_casa_version"]},
        "recipe": recipe,
        "compatibility_normalizations": normalizations,
        "version_defaults": version_defaults,
        "reproducibility_overrides": dict(request["overrides"]),
        "effective_kwargs": effective_kwargs,
        "effective_kwargs_sha256": canonical_sha256(effective_kwargs),
        "cache": cache,
        "mask_identity": validate_mask_identity_spec(
            request.get("mask_identity"), overrides=request["overrides"]
        ),
    }
    validate_result_envelope(result)
    return result


def tree_inventory(path: pathlib.Path | str) -> dict[str, Any]:
    """Hash one tree deterministically, excluding only CASA ``table.lock`` files."""

    root = pathlib.Path(path)
    if not root.exists() and not root.is_symlink():
        return {
            "exists": False,
            "root": str(root),
            "kind": "missing",
            "stable_tree_sha256": None,
            "included_file_count": 0,
            "included_directory_count": 0,
            "included_symlink_count": 0,
            "logical_bytes": 0,
            "excluded_volatile": [],
            "entries": [],
        }

    if root.is_dir() and not root.is_symlink():
        paths = sorted(
            root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()
        )
        root_kind = "directory"
    else:
        paths = [root]
        root_kind = "symlink" if root.is_symlink() else "file"

    entries: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    file_count = 0
    directory_count = 0
    symlink_count = 0
    logical_bytes = 0
    for item in paths:
        relative = "." if item == root else item.relative_to(root).as_posix()
        if item.name in VOLATILE_TREE_FILE_NAMES and not item.is_dir():
            size = item.lstat().st_size
            excluded.append(
                {
                    "relative_path": relative,
                    "bytes": int(size),
                    "reason": "CASA table.lock is volatile lock state",
                }
            )
            continue
        if item.is_symlink():
            target = os.readlink(item)
            entries.append(
                {"relative_path": relative, "kind": "symlink", "target": target}
            )
            symlink_count += 1
        elif item.is_dir():
            entries.append({"relative_path": relative, "kind": "directory"})
            directory_count += 1
        elif item.is_file():
            size = item.stat().st_size
            entries.append(
                {
                    "relative_path": relative,
                    "kind": "file",
                    "bytes": int(size),
                    "sha256": _sha256_file(item),
                }
            )
            file_count += 1
            logical_bytes += int(size)
        else:
            raise ProtocolError(
                f"unsupported filesystem entry in evidence tree: {item}"
            )

    return {
        "exists": True,
        "root": str(root),
        "kind": root_kind,
        "stable_tree_sha256": canonical_sha256(entries),
        "included_file_count": file_count,
        "included_directory_count": directory_count,
        "included_symlink_count": symlink_count,
        "logical_bytes": logical_bytes,
        "excluded_volatile": excluded,
        "entries": entries,
    }


def inventory_product_siblings(imagename: pathlib.Path | str) -> list[dict[str, Any]]:
    prefix = pathlib.Path(imagename)
    parent = prefix.parent
    if not parent.is_dir():
        return []
    base = prefix.name
    candidates = sorted(
        (
            item
            for item in parent.iterdir()
            if item.name == base or item.name.startswith(base + ".")
        ),
        key=lambda item: item.name,
    )
    return [
        {
            "path": str(item),
            "suffix": item.name[len(base) :],
            "inventory": tree_inventory(item),
        }
        for item in candidates
    ]


def product_inventory_identity(products: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return a path-independent identity for run-owned CASA products."""

    if not isinstance(products, list) or not products:
        raise ProtocolError("cold CF cache publication requires CASA products")
    identities: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, product in enumerate(products):
        if not isinstance(product, dict):
            raise ProtocolError(f"product inventory item {index} must be an object")
        suffix = product.get("suffix")
        inventory = product.get("inventory")
        if not isinstance(suffix, str) or suffix in seen:
            raise ProtocolError("product inventory suffixes must be unique strings")
        if suffix and not suffix.startswith("."):
            raise ProtocolError(
                "product inventory suffix must be empty or start with '.'"
            )
        if not isinstance(inventory, dict):
            raise ProtocolError(f"product inventory item {index} has no inventory")
        stable_tree_sha256 = _sha256_value(
            inventory.get("stable_tree_sha256"),
            field=f"product inventory item {index}.stable_tree_sha256",
        )
        kind = inventory.get("kind")
        if kind not in {"directory", "file", "symlink"}:
            raise ProtocolError(
                f"product inventory item {index} must identify an existing tree"
            )
        identity: dict[str, Any] = {
            "suffix": suffix,
            "kind": kind,
            "stable_tree_sha256": stable_tree_sha256,
        }
        for name in (
            "included_file_count",
            "included_directory_count",
            "included_symlink_count",
            "logical_bytes",
        ):
            value = inventory.get(name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ProtocolError(
                    f"product inventory item {index}.{name} must be a non-negative integer"
                )
            identity[name] = value
        identities.append(identity)
        seen.add(suffix)
    identities.sort(key=lambda item: item["suffix"])
    return identities


def cold_publication_identity(
    *,
    request_id: str,
    effective_kwargs_sha256: str,
    products: list[dict[str, Any]],
) -> dict[str, Any]:
    product_inventory = product_inventory_identity(products)
    return {
        "request_id": _nonempty_string(request_id, field="producer.request_id"),
        "effective_kwargs_sha256": _sha256_value(
            effective_kwargs_sha256,
            field="producer.effective_kwargs_sha256",
        ),
        "product_inventory": product_inventory,
        "product_inventory_sha256": canonical_sha256(product_inventory),
    }


def _validate_product_identity_receipt(value: list[dict[str, Any]]) -> None:
    fields = {
        "suffix",
        "kind",
        "stable_tree_sha256",
        "included_file_count",
        "included_directory_count",
        "included_symlink_count",
        "logical_bytes",
    }
    suffixes: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ProtocolError(
                f"CF cache receipt product inventory item {index} must be an object"
            )
        _require_exact_fields(
            item,
            fields,
            source=f"CF cache receipt product inventory item {index}",
        )
        suffix = item.get("suffix")
        if not isinstance(suffix, str) or (suffix and not suffix.startswith(".")):
            raise ProtocolError(
                "CF cache receipt product suffix must be empty or start with '.'"
            )
        if item.get("kind") not in {"directory", "file", "symlink"}:
            raise ProtocolError("CF cache receipt product kind is invalid")
        _sha256_value(
            item.get("stable_tree_sha256"),
            field=f"CF cache receipt product inventory item {index}.stable_tree_sha256",
        )
        for name in fields - {"suffix", "kind", "stable_tree_sha256"}:
            count = item.get(name)
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise ProtocolError(
                    f"CF cache receipt product inventory item {index}.{name} "
                    "must be a non-negative integer"
                )
        suffixes.append(suffix)
    if suffixes != sorted(set(suffixes)):
        raise ProtocolError(
            "CF cache receipt product inventory suffixes must be sorted and unique"
        )


def validate_cache_precondition(cache: dict[str, Any]) -> dict[str, Any]:
    role = cache["role"]
    if role == "none":
        return {"role": "none"}
    cache_path = pathlib.Path(cache["path"])
    receipt_path = pathlib.Path(cache["receipt_path"])
    if role == "cold":
        working_path = _cache_working_path(cache)
        working_receipt_path = _cache_working_receipt_path(cache)
        if cache_path.exists() or cache_path.is_symlink():
            raise ProtocolError(
                f"cold CF cache path must be absent, not merely empty: {cache_path}"
            )
        if receipt_path.exists() or receipt_path.is_symlink():
            raise ProtocolError(f"cold CF cache receipt must be absent: {receipt_path}")
        if working_path.exists() or working_path.is_symlink():
            raise ProtocolError(
                f"cold CF cache working path must be absent: {working_path}"
            )
        if working_receipt_path.exists() or working_receipt_path.is_symlink():
            raise ProtocolError(
                f"cold CF cache working receipt must be absent: {working_receipt_path}"
            )
        return {
            "role": role,
            "inventory": tree_inventory(cache_path),
            "working_inventory": tree_inventory(working_path),
            "receipt": None,
        }

    if not cache_path.is_dir() or cache_path.is_symlink():
        raise ProtocolError(
            f"warm CF cache path must be an existing directory: {cache_path}"
        )
    receipt = _load_cache_receipt(receipt_path, allow_legacy_warm=True)
    if receipt["cache_path"] != str(cache_path):
        raise ProtocolError(
            "warm CF cache receipt path does not match request.cache.path"
        )
    if receipt["plan_sha256"] != cache["plan_sha256"]:
        raise ProtocolError("warm CF cache plan does not match its cold receipt")
    inventory = tree_inventory(cache_path)
    expected = cache["expected_stable_tree_sha256"]
    if receipt["stable_tree_sha256"] != expected:
        raise ProtocolError(
            "warm CF cache request digest does not match its cold receipt"
        )
    if inventory["stable_tree_sha256"] != expected:
        raise ProtocolError(
            "warm CF cache contents do not match the requested cold receipt"
        )
    return {"role": role, "inventory": inventory, "receipt": receipt}


def _validate_runtime_identity(
    cache: dict[str, Any], *, runtime_identity: dict[str, Any] | None
) -> dict[str, Any] | None:
    """Bind cache reuse to the exact CASA code and data/model trees.

    Legacy protocol callers may omit this member, but a cache plan that carries
    a frozen runtime identity is fail-closed before ``tclean`` is invoked.
    ``runtime_identity`` is injectable solely so ordinary Python unit tests do
    not need to import CASA.
    """

    expected = cache.get("plan", {}).get("runtime_identity")
    if expected is None:
        return None
    if not isinstance(expected, dict) or set(expected) != {
        "identity",
        "identity_sha256",
    }:
        raise ProtocolError(
            "cache plan runtime_identity must contain identity and identity_sha256"
        )
    expected_identity = expected.get("identity")
    expected_digest = expected.get("identity_sha256")
    if not isinstance(expected_identity, dict):
        raise ProtocolError("cache plan runtime_identity.identity must be an object")
    try:
        validate_runtime_identity(expected_identity, stable=True)
    except ValueError as error:
        raise ProtocolError(
            f"cache plan runtime identity schema is invalid: {error}"
        ) from error
    if not isinstance(
        expected_digest, str
    ) or expected_digest != stable_identity_sha256(expected_identity):
        raise ProtocolError("cache plan runtime identity digest is invalid")

    actual_identity = (
        capture_runtime_identity() if runtime_identity is None else runtime_identity
    )
    if not isinstance(actual_identity, dict):
        raise ProtocolError("captured CASA runtime identity must be an object")
    try:
        validate_runtime_identity(actual_identity, stable=False)
    except ValueError as error:
        raise ProtocolError(
            f"captured CASA runtime identity schema is invalid: {error}"
        ) from error
    actual_stable_identity = stable_identity_projection(actual_identity)
    actual_digest = stable_identity_sha256(actual_identity)
    if actual_digest != expected_digest:
        raise ProtocolError(
            "CASA runtime/data identity mismatch: "
            f"expected {expected_digest}, got {actual_digest}"
        )
    return {
        "status": "matched",
        "identity": actual_stable_identity,
        "identity_sha256": actual_digest,
        "captured_identity": actual_identity,
    }


def _validate_mask_identity(
    expected: dict[str, Any] | None, *, effective_kwargs: dict[str, Any]
) -> dict[str, Any] | None:
    """Rehash a deterministic clean mask immediately before every tclean call."""

    mask_value = effective_kwargs.get("mask")
    if expected is None:
        if mask_value:
            raise ProtocolError(
                "effective deterministic mask has no frozen request-level identity"
            )
        return None
    if not isinstance(expected, dict) or set(expected) != {
        "kind",
        "sha256",
        "identity",
    }:
        raise ProtocolError(
            "invocation mask_identity must contain kind, sha256, and identity"
        )
    if not isinstance(mask_value, str) or not mask_value:
        raise ProtocolError("frozen mask identity requires an effective mask path")
    path = pathlib.Path(mask_value)
    kind = expected.get("kind")
    try:
        if kind == "casa_image_tree":
            actual_identity = tree_identity(path, excluded_names={"table.lock"})
            actual_sha256 = actual_identity["tree_sha256"]
        elif kind == "file":
            if not path.is_file() or path.is_symlink():
                raise ProtocolError(f"deterministic mask is missing or unsafe: {path}")
            actual_identity = {"size_bytes": path.stat().st_size}
            actual_sha256 = sha256_file(path)
        else:
            raise ProtocolError(f"unsupported deterministic mask kind: {kind!r}")
    except (OSError, ValueError) as error:
        raise ProtocolError(
            f"cannot verify deterministic mask {path}: {error}"
        ) from error
    if actual_sha256 != expected.get("sha256") or actual_identity != expected.get(
        "identity"
    ):
        raise ProtocolError(
            "deterministic mask identity mismatch before tclean: "
            f"expected {expected.get('sha256')}, got {actual_sha256}"
        )
    return {
        "status": "matched",
        "path": str(path),
        "kind": kind,
        "sha256": actual_sha256,
        "identity": actual_identity,
    }


def execute_invocation_plan(
    plan: dict[str, Any],
    *,
    tclean_task: Callable[..., Any] | None = None,
    casa_version: str | None = None,
    runtime_identity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute one planned call; injected runtime arguments are for unit tests."""

    if plan.get("status") != "planned" or plan.get("action") != "run":
        raise ProtocolError("execute_invocation_plan requires a run action plan")
    validate_result_envelope(plan)
    protocol_started = time.perf_counter()
    resources_before = resource_snapshot()
    effective_kwargs = plan["effective_kwargs"]
    imagename = pathlib.Path(effective_kwargs.get("imagename", ""))
    if not imagename.is_absolute():
        raise ProtocolError(
            "effective imagename must be an absolute path for execution"
        )
    imagename.parent.mkdir(parents=True, exist_ok=True)
    products_before = inventory_product_siblings(imagename)
    if products_before and _has_recoverable_cold_publication(plan["cache"]):
        return _recover_cold_publication(
            plan,
            products=products_before,
            tclean_task=tclean_task,
            casa_version=casa_version,
            runtime_identity=runtime_identity,
            protocol_started=protocol_started,
            resources_before=resources_before,
        )
    if products_before:
        raise ProtocolError(
            f"run-owned imagename already has product siblings: {imagename}"
        )
    cache_before = validate_cache_precondition(plan["cache"])

    if tclean_task is None:
        tclean_task, casa_version = _load_casa_runtime()
    if casa_version is None:
        raise ProtocolError("CASA runtime did not report a version")
    expected_version = plan["casa"]["expected_version"]
    if casa_version != expected_version:
        raise ProtocolError(
            f"CASA version mismatch: expected {expected_version}, got {casa_version}"
        )
    signature = validate_runtime_signature(tclean_task, effective_kwargs)
    identity_evidence = _validate_runtime_identity(
        plan["cache"], runtime_identity=runtime_identity
    )
    mask_evidence = _validate_mask_identity(
        plan.get("mask_identity"), effective_kwargs=effective_kwargs
    )
    if identity_evidence is not None or mask_evidence is not None:
        casa_evidence = dict(plan["casa"])
        if identity_evidence is not None:
            casa_evidence["runtime_identity"] = identity_evidence
        if mask_evidence is not None:
            casa_evidence["mask_identity"] = mask_evidence
        plan = {
            **plan,
            "casa": casa_evidence,
        }

    preflight_seconds = time.perf_counter() - protocol_started
    tclean_started = time.perf_counter()
    try:
        return_value = tclean_task(**effective_kwargs)
    except KeyboardInterrupt:
        wall_seconds = time.perf_counter() - tclean_started
        product_started = time.perf_counter()
        products_after = _best_effort_product_inventory(imagename)
        product_seconds = time.perf_counter() - product_started
        cache_started = time.perf_counter()
        cache_after = _best_effort_tree_inventory(plan["cache"])
        cache_seconds = time.perf_counter() - cache_started
        resources_after = resource_snapshot()
        return _execution_result(
            plan,
            status="failed_execution",
            casa_version=casa_version,
            signature=signature,
            wall_seconds=wall_seconds,
            stage_timings_seconds=_stage_timings(
                preflight=preflight_seconds,
                tclean=wall_seconds,
                products=product_seconds,
                cache=cache_seconds,
                total=time.perf_counter() - protocol_started,
            ),
            resources_before=resources_before,
            resources_after=resources_after,
            products_before=products_before,
            products_after=products_after,
            cache_before=cache_before,
            cache_after=cache_after,
            return_value=None,
            failure={
                "kind": "operator_interrupt",
                "reason": "CASA tclean was interrupted before completion",
                "exception_type": "KeyboardInterrupt",
            },
        )
    except Exception as error:  # CASA failures must still produce typed evidence.
        wall_seconds = time.perf_counter() - tclean_started
        product_started = time.perf_counter()
        products_after = _best_effort_product_inventory(imagename)
        product_seconds = time.perf_counter() - product_started
        cache_started = time.perf_counter()
        cache_after = _best_effort_tree_inventory(plan["cache"])
        cache_seconds = time.perf_counter() - cache_started
        resources_after = resource_snapshot()
        return _execution_result(
            plan,
            status="failed_execution",
            casa_version=casa_version,
            signature=signature,
            wall_seconds=wall_seconds,
            stage_timings_seconds=_stage_timings(
                preflight=preflight_seconds,
                tclean=wall_seconds,
                products=product_seconds,
                cache=cache_seconds,
                total=time.perf_counter() - protocol_started,
            ),
            resources_before=resources_before,
            resources_after=resources_after,
            products_before=products_before,
            products_after=products_after,
            cache_before=cache_before,
            cache_after=cache_after,
            return_value=None,
            failure={
                "kind": "tclean",
                "reason": str(error),
                "exception_type": type(error).__name__,
            },
        )

    wall_seconds = time.perf_counter() - tclean_started
    product_started = time.perf_counter()
    products_after = inventory_product_siblings(imagename)
    product_seconds = time.perf_counter() - product_started
    if not products_after:
        cache_started = time.perf_counter()
        cache_after = _best_effort_tree_inventory(plan["cache"])
        cache_seconds = time.perf_counter() - cache_started
        resources_after = resource_snapshot()
        return _execution_result(
            plan,
            status="failed_postcondition",
            casa_version=casa_version,
            signature=signature,
            wall_seconds=wall_seconds,
            stage_timings_seconds=_stage_timings(
                preflight=preflight_seconds,
                tclean=wall_seconds,
                products=product_seconds,
                cache=cache_seconds,
                total=time.perf_counter() - protocol_started,
            ),
            resources_before=resources_before,
            resources_after=resources_after,
            products_before=products_before,
            products_after=products_after,
            cache_before=cache_before,
            cache_after=cache_after,
            return_value=return_value,
            failure={
                "kind": "products",
                "reason": "tclean wrote no imagename products",
            },
        )

    cache_started = time.perf_counter()
    try:
        cache_after = validate_cache_postcondition(
            plan["cache"],
            cache_before,
            products=products_after,
            request_id=plan["request_id"],
            effective_kwargs_sha256=plan["effective_kwargs_sha256"],
        )
    except ProtocolError as error:
        cache_seconds = time.perf_counter() - cache_started
        fallback_cache = _best_effort_tree_inventory(plan["cache"])
        resources_after = resource_snapshot()
        return _execution_result(
            plan,
            status="failed_postcondition",
            casa_version=casa_version,
            signature=signature,
            wall_seconds=wall_seconds,
            stage_timings_seconds=_stage_timings(
                preflight=preflight_seconds,
                tclean=wall_seconds,
                products=product_seconds,
                cache=cache_seconds,
                total=time.perf_counter() - protocol_started,
            ),
            resources_before=resources_before,
            resources_after=resources_after,
            products_before=products_before,
            products_after=products_after,
            cache_before=cache_before,
            cache_after=fallback_cache,
            return_value=return_value,
            failure={"kind": "cache", "reason": str(error)},
        )
    cache_seconds = time.perf_counter() - cache_started
    resources_after = resource_snapshot()

    return _execution_result(
        plan,
        status="completed",
        casa_version=casa_version,
        signature=signature,
        wall_seconds=wall_seconds,
        stage_timings_seconds=_stage_timings(
            preflight=preflight_seconds,
            tclean=wall_seconds,
            products=product_seconds,
            cache=cache_seconds,
            total=time.perf_counter() - protocol_started,
        ),
        resources_before=resources_before,
        resources_after=resources_after,
        products_before=products_before,
        products_after=products_after,
        cache_before=cache_before,
        cache_after=cache_after,
        return_value=return_value,
        failure=None,
    )


def _has_recoverable_cold_publication(cache: dict[str, Any]) -> bool:
    """Return whether exact-request replay can enter publication recovery.

    A receipt is the commit-intent marker: it is written only after ``tclean``
    returned and product postconditions passed.  A working cache without that
    marker may instead be partial output from a failed ``tclean`` and must never
    be promoted by recovery.
    """

    if cache["role"] != "cold":
        return False
    cache_paths = (pathlib.Path(cache["path"]), _cache_working_path(cache))
    receipt_paths = (
        pathlib.Path(cache["receipt_path"]),
        _cache_working_receipt_path(cache),
    )
    return any(path.exists() or path.is_symlink() for path in cache_paths) and any(
        path.exists() or path.is_symlink() for path in receipt_paths
    )


def _recover_cold_publication(
    plan: dict[str, Any],
    *,
    products: list[dict[str, Any]],
    tclean_task: Callable[..., Any] | None,
    casa_version: str | None,
    runtime_identity: dict[str, Any] | None,
    protocol_started: float,
    resources_before: dict[str, Any],
) -> dict[str, Any]:
    """Finish an interrupted publication by replaying the exact run request.

    Recovery never invokes ``tclean`` and returns a distinct non-benchmark
    status so its near-zero wall time cannot be accepted as a performance
    sample.  Rebuilding the invocation plan from the same request ID is what
    selects the original request-token-specific staging paths.
    """

    if tclean_task is None:
        tclean_task, casa_version = _load_casa_runtime()
    if casa_version is None:
        raise ProtocolError("CASA runtime did not report a version")
    expected_version = plan["casa"]["expected_version"]
    if casa_version != expected_version:
        raise ProtocolError(
            f"CASA version mismatch: expected {expected_version}, got {casa_version}"
        )
    signature = validate_runtime_signature(tclean_task, plan["effective_kwargs"])
    identity_evidence = _validate_runtime_identity(
        plan["cache"], runtime_identity=runtime_identity
    )
    mask_evidence = _validate_mask_identity(
        plan.get("mask_identity"), effective_kwargs=plan["effective_kwargs"]
    )
    recovery_evidence: dict[str, Any] = {
        "status": "completed",
        "tclean_reinvoked": False,
        "exact_request_replay_required": True,
    }
    casa_evidence = {
        **plan["casa"],
        "publication_recovery": recovery_evidence,
    }
    if identity_evidence is not None:
        casa_evidence["runtime_identity"] = identity_evidence
    if mask_evidence is not None:
        casa_evidence["mask_identity"] = mask_evidence
    plan = {
        **plan,
        "casa": casa_evidence,
    }

    cache = plan["cache"]
    cache_before = {
        "role": "cold",
        "status": "recovering_publication",
        "final_inventory": tree_inventory(cache["path"]),
        "working_inventory": tree_inventory(_cache_working_path(cache)),
        "final_receipt_exists": pathlib.Path(cache["receipt_path"]).is_file(),
        "working_receipt_exists": _cache_working_receipt_path(cache).is_file(),
    }
    preflight_seconds = time.perf_counter() - protocol_started
    cache_started = time.perf_counter()
    cache_after = _publish_cold_cache(
        cache,
        products=products,
        request_id=plan["request_id"],
        effective_kwargs_sha256=plan["effective_kwargs_sha256"],
        recovery=True,
    )
    cache_seconds = time.perf_counter() - cache_started
    product_started = time.perf_counter()
    products_after = inventory_product_siblings(plan["effective_kwargs"]["imagename"])
    product_seconds = time.perf_counter() - product_started
    if not products_after:
        raise ProtocolError("publication recovery lost the run-owned image products")
    resources_after = resource_snapshot()

    return _execution_result(
        plan,
        status="recovered_publication",
        casa_version=casa_version,
        signature=signature,
        wall_seconds=0.0,
        stage_timings_seconds=_stage_timings(
            preflight=preflight_seconds,
            tclean=0.0,
            products=product_seconds,
            cache=cache_seconds,
            total=time.perf_counter() - protocol_started,
        ),
        resources_before=resources_before,
        resources_after=resources_after,
        products_before=products,
        products_after=products_after,
        cache_before=cache_before,
        cache_after=cache_after,
        return_value=None,
        failure=None,
    )


def validate_cache_postcondition(
    cache: dict[str, Any],
    cache_before: dict[str, Any],
    *,
    products: list[dict[str, Any]],
    request_id: str,
    effective_kwargs_sha256: str,
) -> dict[str, Any]:
    role = cache["role"]
    if role == "none":
        return {"role": "none"}
    working_path = _cache_working_path(cache)
    if role == "warm":
        inventory = tree_inventory(working_path)
        if not inventory["exists"] or inventory["kind"] != "directory":
            raise ProtocolError(
                f"CASA did not create a CF cache directory: {working_path}"
            )
        if inventory["included_file_count"] == 0:
            raise ProtocolError(
                f"CASA CF cache contains no stable files: {working_path}"
            )
        before_digest = cache_before["inventory"]["stable_tree_sha256"]
        if inventory["stable_tree_sha256"] != before_digest:
            raise ProtocolError("warm CF cache stable contents changed during tclean")
        return {
            "role": role,
            "inventory": inventory,
            "receipt": cache_before["receipt"],
        }

    return _publish_cold_cache(
        cache,
        products=products,
        request_id=request_id,
        effective_kwargs_sha256=effective_kwargs_sha256,
    )


def _publish_cold_cache(
    cache: dict[str, Any],
    *,
    products: list[dict[str, Any]],
    request_id: str,
    effective_kwargs_sha256: str,
    recovery: bool = False,
) -> dict[str, Any]:
    """Publish or resume one cold CF-cache transaction.

    The validated receipt is published first and the cache directory rename is
    the commit point.  A warm reader therefore either sees no cache and fails
    closed or sees a cache with its receipt already present.  Both publication
    renames are retryable: a failed receipt rename leaves the staged pair, and
    a failed cache rename rolls the receipt back when possible.  If that
    rollback also fails, the published receipt plus staged cache is itself a
    resumable state when the exact run request is replayed through the protocol.

    Recovery accepts only schema-v2 receipts that bind the exact producer
    request and stable product inventory.  Legacy v1 receipts remain usable
    for ordinary warm reads but can never authorize publication recovery.
    """

    cache_path = pathlib.Path(cache["path"])
    working_path = _cache_working_path(cache)
    receipt_path = pathlib.Path(cache["receipt_path"])
    working_receipt_path = _cache_working_receipt_path(cache)

    cache_is_final = cache_path.exists() or cache_path.is_symlink()
    cache_is_staged = working_path.exists() or working_path.is_symlink()
    if cache_is_final and cache_is_staged:
        raise ProtocolError(
            "cold CF cache publication has both final and staged cache trees: "
            f"{cache_path}, {working_path}"
        )
    if not cache_is_final and not cache_is_staged:
        raise ProtocolError(f"CASA did not create a CF cache directory: {working_path}")

    inventory_path = cache_path if cache_is_final else working_path
    inventory = tree_inventory(inventory_path)
    if not inventory["exists"] or inventory["kind"] != "directory":
        raise ProtocolError(
            f"CASA did not create a CF cache directory: {inventory_path}"
        )
    if inventory["included_file_count"] == 0:
        raise ProtocolError(f"CASA CF cache contains no stable files: {inventory_path}")

    final_inventory = {**inventory, "root": str(cache_path)}
    producer = cold_publication_identity(
        request_id=request_id,
        effective_kwargs_sha256=effective_kwargs_sha256,
        products=products,
    )
    receipt = {
        "schema_version": CACHE_RECEIPT_SCHEMA_VERSION,
        "kind": CACHE_RECEIPT_KIND,
        "cache_path": str(cache_path),
        "plan_sha256": cache["plan_sha256"],
        "stable_tree_sha256": inventory["stable_tree_sha256"],
        "inventory": final_inventory,
        "producer": producer,
    }

    receipt_is_final = receipt_path.exists() or receipt_path.is_symlink()
    receipt_is_staged = (
        working_receipt_path.exists() or working_receipt_path.is_symlink()
    )
    if receipt_is_final and receipt_is_staged:
        raise ProtocolError(
            "cold CF cache publication has both final and staged receipts: "
            f"{receipt_path}, {working_receipt_path}"
        )
    if recovery and not receipt_is_final and not receipt_is_staged:
        raise ProtocolError(
            "cold CF cache recovery requires an existing schema-v2 commit-intent "
            "receipt from the exact producer invocation"
        )
    if cache_is_final and not receipt_is_final and not receipt_is_staged:
        raise ProtocolError(
            "cold CF cache final tree has no staged or final receipt and cannot "
            f"be identified safely: {cache_path}"
        )

    if not receipt_is_final:
        if not receipt_is_staged:
            try:
                _write_json_atomic(working_receipt_path, receipt)
            except OSError as error:
                raise ProtocolError(
                    "cannot stage cold CF cache receipt; the staged cache remains "
                    f"at {working_path}: {error}"
                ) from error
        receipt = _validate_matching_cache_receipt(working_receipt_path, receipt)
        try:
            os.replace(working_receipt_path, receipt_path)
        except OSError as error:
            raise ProtocolError(
                "cannot publish cold CF cache receipt; the staged cache and "
                f"receipt remain retryable by replaying the exact run request at "
                f"{working_path} and "
                f"{working_receipt_path}: {error}"
            ) from error
        receipt_is_final = True
    else:
        receipt = _validate_matching_cache_receipt(receipt_path, receipt)

    if not cache_is_final:
        try:
            os.replace(working_path, cache_path)
        except OSError as publish_error:
            try:
                os.replace(receipt_path, working_receipt_path)
            except OSError as rollback_error:
                raise ProtocolError(
                    "cannot publish cold CF cache and receipt rollback also failed; "
                    "the published receipt plus staged cache remain recoverable by "
                    "replaying the exact run request: "
                    f"publish error: {publish_error}; rollback error: {rollback_error}; "
                    f"cache={working_path}; receipt={receipt_path}"
                ) from publish_error
            raise ProtocolError(
                "cannot publish cold CF cache; its receipt was rolled back and the "
                "staged pair remains recoverable by replaying the exact run request: "
                f"{publish_error}"
            ) from publish_error

    promoted_inventory = tree_inventory(cache_path)
    if promoted_inventory["stable_tree_sha256"] != receipt["stable_tree_sha256"]:
        rollback_failures: list[str] = []
        try:
            os.replace(cache_path, working_path)
        except OSError as rollback_error:
            rollback_failures.append(f"cache rollback failed: {rollback_error}")
        else:
            try:
                os.replace(receipt_path, working_receipt_path)
            except OSError as rollback_error:
                rollback_failures.append(f"receipt rollback failed: {rollback_error}")
        detail = "; ".join(rollback_failures)
        if detail:
            detail = f"; {detail}"
        raise ProtocolError(
            "promoted cold CF cache digest changed during atomic publication" + detail
        )

    return {
        "role": "cold",
        "inventory": promoted_inventory,
        "receipt": receipt,
    }


def _validate_matching_cache_receipt(
    path: pathlib.Path, expected: dict[str, Any]
) -> dict[str, Any]:
    actual = _load_cache_receipt(path)
    identity_fields = ("cache_path", "plan_sha256", "stable_tree_sha256")
    identity_matches = all(actual[name] == expected[name] for name in identity_fields)
    actual_inventory = actual["inventory"]
    expected_inventory = expected["inventory"]
    inventory_identity_matches = (
        actual_inventory.get("root") == expected_inventory["root"]
        and actual_inventory.get("stable_tree_sha256")
        == expected_inventory["stable_tree_sha256"]
    )
    producer_identity_matches = actual["producer"] == expected["producer"]
    if (
        not identity_matches
        or not inventory_identity_matches
        or not producer_identity_matches
    ):
        raise ProtocolError(
            "staged or published cold CF cache receipt does not match the exact "
            f"producer request, products, stable cache identity, and plan: {path}"
        )
    return actual


def resource_snapshot() -> dict[str, Any]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    max_rss = int(usage.ru_maxrss)
    if platform.system() != "Darwin":
        max_rss *= 1024
    disk_read_bytes, disk_write_bytes, disk_io_source = _disk_io_bytes()
    return {
        "disk_io_source": disk_io_source,
        "values": {
            "user_cpu_seconds": float(usage.ru_utime),
            "system_cpu_seconds": float(usage.ru_stime),
            "peak_rss_bytes": max_rss,
            "minor_page_faults": int(usage.ru_minflt),
            "major_page_faults": int(usage.ru_majflt),
            "block_input_operations": int(usage.ru_inblock),
            "block_output_operations": int(usage.ru_oublock),
            "disk_read_bytes": disk_read_bytes,
            "disk_write_bytes": disk_write_bytes,
            "voluntary_context_switches": int(usage.ru_nvcsw),
            "involuntary_context_switches": int(usage.ru_nivcsw),
        },
    }


def _disk_io_bytes() -> tuple[int, int, str]:
    system = platform.system()
    if system == "Darwin":
        return _darwin_disk_io_bytes()
    if system == "Linux":
        try:
            fields = {}
            for line in (
                pathlib.Path("/proc/self/io").read_text(encoding="utf-8").splitlines()
            ):
                name, separator, raw_value = line.partition(":")
                if separator:
                    fields[name.strip()] = int(raw_value.strip())
            return (
                fields["read_bytes"],
                fields["write_bytes"],
                "linux_proc_self_io",
            )
        except (OSError, KeyError, ValueError) as error:
            raise ProtocolError(
                f"cannot capture Linux process disk-I/O bytes: {error}"
            ) from error
    raise ProtocolError(
        f"process disk-I/O byte capture is unsupported on {system or 'unknown OS'}"
    )


def _darwin_disk_io_bytes() -> tuple[int, int, str]:
    class RusageInfoV2(ctypes.Structure):
        _fields_ = [
            ("ri_uuid", ctypes.c_uint8 * 16),
            ("ri_user_time", ctypes.c_uint64),
            ("ri_system_time", ctypes.c_uint64),
            ("ri_pkg_idle_wkups", ctypes.c_uint64),
            ("ri_interrupt_wkups", ctypes.c_uint64),
            ("ri_pageins", ctypes.c_uint64),
            ("ri_wired_size", ctypes.c_uint64),
            ("ri_resident_size", ctypes.c_uint64),
            ("ri_phys_footprint", ctypes.c_uint64),
            ("ri_proc_start_abstime", ctypes.c_uint64),
            ("ri_proc_exit_abstime", ctypes.c_uint64),
            ("ri_child_user_time", ctypes.c_uint64),
            ("ri_child_system_time", ctypes.c_uint64),
            ("ri_child_pkg_idle_wkups", ctypes.c_uint64),
            ("ri_child_interrupt_wkups", ctypes.c_uint64),
            ("ri_child_pageins", ctypes.c_uint64),
            ("ri_child_elapsed_abstime", ctypes.c_uint64),
            ("ri_diskio_bytesread", ctypes.c_uint64),
            ("ri_diskio_byteswritten", ctypes.c_uint64),
        ]

    try:
        library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        proc_pid_rusage = library.proc_pid_rusage
        proc_pid_rusage.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.POINTER(RusageInfoV2),
        ]
        proc_pid_rusage.restype = ctypes.c_int
        info = RusageInfoV2()
        if proc_pid_rusage(os.getpid(), 2, ctypes.byref(info)) != 0:
            errno = ctypes.get_errno()
            raise OSError(errno, os.strerror(errno))
    except (AttributeError, OSError) as error:
        raise ProtocolError(
            f"cannot capture Darwin process disk-I/O bytes: {error}"
        ) from error
    return (
        int(info.ri_diskio_bytesread),
        int(info.ri_diskio_byteswritten),
        "darwin_proc_pid_rusage_v2",
    )


def _stage_timings(
    *,
    preflight: float,
    tclean: float,
    products: float,
    cache: float,
    total: float,
) -> dict[str, float]:
    return {
        "protocol_preflight": float(preflight),
        "tclean_task": float(tclean),
        "product_inventory": float(products),
        "cache_postcondition": float(cache),
        "protocol_total": float(total),
    }


def validate_runtime_signature(
    tclean_task: Callable[..., Any], effective_kwargs: dict[str, Any]
) -> dict[str, Any]:
    target = tclean_task
    if not inspect.isfunction(tclean_task) and hasattr(tclean_task, "__call__"):
        target = tclean_task.__call__
    try:
        signature = inspect.signature(target)
    except (TypeError, ValueError) as error:
        raise ProtocolError(f"cannot inspect CASA tclean signature: {error}") from error
    parameters = signature.parameters
    accepts_arbitrary = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )
    accepted = {
        name
        for name, parameter in parameters.items()
        if name != "self"
        and parameter.kind
        in {inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY}
    }
    unsupported = sorted(set(effective_kwargs) - accepted)
    if unsupported and not accepts_arbitrary:
        raise ProtocolError(
            "CASA tclean signature does not accept effective parameter(s): "
            + ", ".join(unsupported)
        )
    if not accepts_arbitrary:
        for name, expected in CASA_6_7_5_9_NEW_DEFAULTS.items():
            parameter = parameters.get(name)
            if parameter is None or parameter.default != expected:
                raise ProtocolError(
                    f"CASA tclean default drift for {name}: expected {expected!r}, "
                    f"got {None if parameter is None else parameter.default!r}"
                )
    return {
        "text": str(signature),
        "accepted_parameter_names": sorted(accepted),
        "accepts_arbitrary_keywords": accepts_arbitrary,
    }


def process_request(
    request: dict[str, Any],
    *,
    tclean_task: Callable[..., Any] | None = None,
    casa_version: str | None = None,
    runtime_identity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plan = build_invocation_plan(request)
    if request["action"] == "plan":
        return plan
    return execute_invocation_plan(
        plan,
        tclean_task=tclean_task,
        casa_version=casa_version,
        runtime_identity=runtime_identity,
    )


def validate_result_envelope(result: dict[str, Any]) -> None:
    if not isinstance(result, dict):
        raise ProtocolError("result must be an object")
    unknown = sorted(set(result) - RESULT_FIELDS)
    if unknown:
        raise ProtocolError("result contains unknown field(s): " + ", ".join(unknown))
    if result.get("schema_version") != RESULT_SCHEMA_VERSION:
        raise ProtocolError(f"result.schema_version must be {RESULT_SCHEMA_VERSION}")
    if result.get("kind") != RESULT_KIND:
        raise ProtocolError(f"result.kind must be {RESULT_KIND!r}")
    if result.get("status") not in RESULT_STATUSES:
        raise ProtocolError("result.status is invalid")
    _nonempty_string(result.get("request_id"), field="result.request_id")
    common = {"schema_version", "kind", "status", "request_id"}
    plan_fields = common | {
        "action",
        "casa",
        "recipe",
        "compatibility_normalizations",
        "version_defaults",
        "reproducibility_overrides",
        "effective_kwargs",
        "effective_kwargs_sha256",
        "cache",
        "mask_identity",
    }
    execution_fields = plan_fields | {
        "wall_seconds",
        "stage_timings_seconds",
        "resources",
        "products",
        "tclean_return",
    }
    status = result["status"]
    if status == "planned":
        _require_exact_fields(result, plan_fields, source="result")
    elif status in {"completed", "recovered_publication"}:
        _require_exact_fields(result, execution_fields, source="result")
    elif status.startswith("failed"):
        if not isinstance(result.get("failure"), dict):
            raise ProtocolError("failed result requires a failure object")
        minimal_failure_fields = common | {"failure"}
        if set(result) == minimal_failure_fields:
            pass
        else:
            _require_exact_fields(
                result, execution_fields | {"failure"}, source="result"
            )
    else:
        raise ProtocolError("result status does not have a valid envelope shape")
    if "effective_kwargs" in result:
        if not isinstance(result["effective_kwargs"], dict):
            raise ProtocolError("result.effective_kwargs must be an object")
        digest = _sha256_value(
            result.get("effective_kwargs_sha256"),
            field="result.effective_kwargs_sha256",
        )
        if digest != canonical_sha256(result["effective_kwargs"]):
            raise ProtocolError(
                "result.effective_kwargs_sha256 does not match effective_kwargs"
            )
    if "wall_seconds" in result:
        _validate_execution_measurements(result)
    canonical_json_bytes(result)


def _validate_execution_measurements(result: dict[str, Any]) -> None:
    wall_seconds = _finite_nonnegative_number(
        result.get("wall_seconds"), field="result.wall_seconds"
    )
    stages = result.get("stage_timings_seconds")
    if not isinstance(stages, dict):
        raise ProtocolError("result.stage_timings_seconds must be an object")
    _require_exact_fields(
        stages, set(STAGE_TIMING_FIELDS), source="result.stage_timings_seconds"
    )
    stage_values = {
        name: _finite_nonnegative_number(
            stages[name], field=f"result.stage_timings_seconds.{name}"
        )
        for name in STAGE_TIMING_FIELDS
    }
    if stage_values["tclean_task"] != wall_seconds:
        raise ProtocolError(
            "result tclean_task stage must exactly match result.wall_seconds"
        )
    component_sum = sum(
        stage_values[name] for name in STAGE_TIMING_FIELDS if name != "protocol_total"
    )
    if stage_values["protocol_total"] + 1e-9 < component_sum:
        raise ProtocolError(
            "result protocol_total stage cannot be shorter than its measured components"
        )

    resources = result.get("resources")
    if not isinstance(resources, dict):
        raise ProtocolError("result.resources must be an object")
    _require_exact_fields(
        resources,
        {
            "schema_version",
            "scope",
            "peak_rss_source",
            "disk_io_source",
            "before",
            "after",
            "delta",
        },
        source="result.resources",
    )
    if resources.get("schema_version") != 1:
        raise ProtocolError("result.resources.schema_version must be 1")
    if resources.get("scope") != "casa_python_process_during_protocol_execution":
        raise ProtocolError("result.resources.scope is invalid")
    if resources.get("peak_rss_source") != "getrusage_rusage_self":
        raise ProtocolError("result.resources.peak_rss_source is invalid")
    if resources.get("disk_io_source") not in {
        "darwin_proc_pid_rusage_v2",
        "linux_proc_self_io",
    }:
        raise ProtocolError("result.resources.disk_io_source is invalid")
    snapshots: dict[str, dict[str, Any]] = {}
    for phase in ("before", "after", "delta"):
        snapshot = resources.get(phase)
        if not isinstance(snapshot, dict):
            raise ProtocolError(f"result.resources.{phase} must be an object")
        _require_exact_fields(
            snapshot, set(RESOURCE_FIELDS), source=f"result.resources.{phase}"
        )
        snapshots[phase] = snapshot
        for name in RESOURCE_FIELDS:
            value = snapshot[name]
            if name in {"user_cpu_seconds", "system_cpu_seconds"}:
                _finite_nonnegative_number(
                    value, field=f"result.resources.{phase}.{name}"
                )
            elif isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ProtocolError(
                    f"result.resources.{phase}.{name} must be a non-negative integer"
                )
    if snapshots["after"]["peak_rss_bytes"] <= 0:
        raise ProtocolError("result.resources.after.peak_rss_bytes must be positive")
    if snapshots["after"]["peak_rss_bytes"] < snapshots["before"]["peak_rss_bytes"]:
        raise ProtocolError("result resource counter decreased for peak_rss_bytes")
    if snapshots["delta"]["peak_rss_bytes"] != snapshots["after"]["peak_rss_bytes"]:
        raise ProtocolError(
            "result.resources.delta.peak_rss_bytes must report the observed process peak"
        )
    for name in RESOURCE_FIELDS:
        if name == "peak_rss_bytes":
            continue
        before = snapshots["before"][name]
        after = snapshots["after"][name]
        if after < before:
            raise ProtocolError(f"result resource counter decreased for {name}")
        expected_delta = after - before
        actual_delta = snapshots["delta"][name]
        if isinstance(expected_delta, float):
            matches = math.isclose(
                actual_delta, expected_delta, rel_tol=0.0, abs_tol=1e-12
            )
        else:
            matches = actual_delta == expected_delta
        if not matches:
            raise ProtocolError(
                f"result.resources.delta.{name} does not match after-before"
            )


def _finite_nonnegative_number(value: Any, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProtocolError(f"{field} must be a finite non-negative number")
    number = float(value)
    if not math.isfinite(number) or number < 0.0:
        raise ProtocolError(f"{field} must be a finite non-negative number")
    return number


def summarize_completed_results(calls: list[dict[str, Any]]) -> dict[str, Any]:
    """Derive measured-call stage/resource evidence from bound protocol results."""

    if not isinstance(calls, list) or not calls:
        raise ProtocolError("CASA evidence summary requires measured calls")
    stage_runs: list[dict[str, Any]] = []
    resource_runs: list[dict[str, Any]] = []
    for index, call in enumerate(calls):
        if not isinstance(call, dict):
            raise ProtocolError(f"CASA measured call {index} must be an object")
        call_name = _nonempty_string(
            call.get("name"), field=f"CASA measured call {index}.name"
        )
        result = call.get("result")
        validate_result_envelope(result)
        if result.get("status") != "completed":
            raise ProtocolError(
                f"CASA measured call {call_name} is not benchmark-complete"
            )
        stages = result["stage_timings_seconds"]
        stage_runs.append({"call": call_name, **stages})
        delta = result["resources"]["delta"]
        products = result["products"]["after"]
        product_logical_bytes = sum(
            int(product["inventory"]["logical_bytes"]) for product in products
        )
        cache_after = result["cache"]["after"]
        cache_inventory = (
            cache_after.get("inventory") if isinstance(cache_after, dict) else None
        )
        if not isinstance(cache_inventory, dict):
            if result["cache"].get("role") == "none":
                cf_cache_logical_bytes = 0
                cf_cache_included_file_count = 0
            else:
                raise ProtocolError(
                    f"CASA measured call {call_name} has no bound cache inventory"
                )
        else:
            cf_cache_logical_bytes = _nonnegative_integer(
                cache_inventory.get("logical_bytes"),
                field=f"CASA measured call {call_name} cache logical bytes",
            )
            cf_cache_included_file_count = _nonnegative_integer(
                cache_inventory.get("included_file_count"),
                field=f"CASA measured call {call_name} cache file count",
            )
        resource_runs.append(
            {
                "call": call_name,
                **delta,
                "product_logical_bytes": product_logical_bytes,
                "cf_cache_logical_bytes": cf_cache_logical_bytes,
                "cf_cache_included_file_count": cf_cache_included_file_count,
            }
        )

    stage_median = {
        name: statistics.median(float(run[name]) for run in stage_runs)
        for name in STAGE_TIMING_FIELDS
    }
    resource_median_fields = (
        "user_cpu_seconds",
        "system_cpu_seconds",
        "block_input_operations",
        "block_output_operations",
        "disk_read_bytes",
        "disk_write_bytes",
        "product_logical_bytes",
    )
    resources: dict[str, Any] = {
        "runs": resource_runs,
        "peak_rss_bytes_max": max(int(run["peak_rss_bytes"]) for run in resource_runs),
        "cf_cache_logical_bytes_max": max(
            int(run["cf_cache_logical_bytes"]) for run in resource_runs
        ),
        "cf_cache_included_file_count_max": max(
            int(run["cf_cache_included_file_count"]) for run in resource_runs
        ),
    }
    for name in resource_median_fields:
        resources[f"{name}_median"] = statistics.median(
            run[name] for run in resource_runs
        )
    return {
        "schema_version": 1,
        "scope": "measured_calls",
        "call_count": len(calls),
        "stage_seconds": {"runs": stage_runs, "median": stage_median},
        "resources": resources,
    }


def _nonnegative_integer(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProtocolError(f"{field} must be a non-negative integer")
    return value


def validate_result_for_request(
    result: dict[str, Any], request: dict[str, Any]
) -> None:
    """Bind a CASA subprocess result to the exact invocation request.

    A completed status is never trusted on its own: the host independently
    rebuilds the effective call, then validates the recipe, cache, mask, and
    product namespace carried by the subprocess result.
    """

    request = validate_request(request)
    validate_result_envelope(result)
    if result.get("request_id") != request["request_id"]:
        raise ProtocolError("result.request_id does not match request.request_id")
    if result["status"].startswith("failed") and set(result) == {
        "schema_version",
        "kind",
        "status",
        "request_id",
        "failure",
    }:
        return
    if result.get("status") == "planned":
        raise ProtocolError("run request returned a planned result")

    expected = build_invocation_plan(request)
    for field in (
        "action",
        "recipe",
        "compatibility_normalizations",
        "version_defaults",
        "reproducibility_overrides",
        "effective_kwargs",
        "effective_kwargs_sha256",
        "mask_identity",
    ):
        if result.get(field) != expected[field]:
            raise ProtocolError(f"result.{field} does not match the request plan")

    casa = result.get("casa")
    if not isinstance(casa, dict):
        raise ProtocolError("result.casa must be an object")
    if casa.get("expected_version") != expected["casa"]["expected_version"]:
        raise ProtocolError("result.casa expected version does not match request")
    if casa.get("actual_version") != request["expected_casa_version"]:
        raise ProtocolError("result.casa actual version does not match request")
    allowed_casa_fields = {
        "expected_version",
        "actual_version",
        "tclean_signature",
        "runtime_identity",
        "mask_identity",
        "publication_recovery",
    }
    unknown_casa = sorted(set(casa) - allowed_casa_fields)
    required_casa = {"expected_version", "actual_version", "tclean_signature"}
    missing_casa = sorted(required_casa - set(casa))
    if unknown_casa or missing_casa:
        raise ProtocolError(
            "result.casa field mismatch; "
            f"missing={missing_casa or 'none'}; unknown={unknown_casa or 'none'}"
        )
    recovery = casa.get("publication_recovery")
    if result["status"] == "recovered_publication":
        if recovery != {
            "status": "completed",
            "tclean_reinvoked": False,
            "exact_request_replay_required": True,
        }:
            raise ProtocolError(
                "recovered result requires exact non-reinvocation publication evidence"
            )
        if result.get("wall_seconds") != 0.0:
            raise ProtocolError("publication recovery cannot carry a benchmark timing")
        if result.get("tclean_return") != {"type": None, "present": False}:
            raise ProtocolError(
                "publication recovery cannot carry a tclean return value"
            )
    elif recovery is not None:
        raise ProtocolError(
            "only recovered_publication may carry publication recovery evidence"
        )
    expected_runtime = expected["cache"].get("plan", {}).get("runtime_identity")
    runtime_evidence = casa.get("runtime_identity")
    if expected_runtime is None:
        if runtime_evidence is not None:
            raise ProtocolError("result.casa has unrequested runtime identity evidence")
    elif (
        not isinstance(runtime_evidence, dict)
        or runtime_evidence.get("status") != "matched"
        or runtime_evidence.get("identity") != expected_runtime["identity"]
        or runtime_evidence.get("identity_sha256")
        != expected_runtime["identity_sha256"]
    ):
        raise ProtocolError("result.casa runtime identity does not match request")
    expected_mask = expected.get("mask_identity")
    mask_evidence = casa.get("mask_identity")
    if expected_mask is None:
        if mask_evidence is not None:
            raise ProtocolError("result.casa has unrequested mask identity evidence")
    elif (
        not isinstance(mask_evidence, dict)
        or mask_evidence.get("status") != "matched"
        or mask_evidence.get("path") != expected["effective_kwargs"].get("mask")
        or any(
            mask_evidence.get(field) != expected_mask[field]
            for field in ("kind", "sha256", "identity")
        )
    ):
        raise ProtocolError("result.casa mask identity does not match request")

    cache = result.get("cache")
    if not isinstance(cache, dict):
        raise ProtocolError("result.cache must be an object")
    expected_cache = expected["cache"]
    for field, value in expected_cache.items():
        if cache.get(field) != value:
            raise ProtocolError(f"result.cache.{field} does not match request")
    if set(cache) != set(expected_cache) | {"before", "after"}:
        raise ProtocolError("result.cache fields do not match execution protocol")

    products = result.get("products")
    if not isinstance(products, dict) or set(products) != {"before", "after"}:
        raise ProtocolError("result.products must contain exactly before and after")
    _validate_product_inventory_binding(
        products["before"], imagename=expected["effective_kwargs"]["imagename"]
    )
    _validate_product_inventory_binding(
        products["after"], imagename=expected["effective_kwargs"]["imagename"]
    )
    if result["status"] == "completed":
        if products["before"] != []:
            raise ProtocolError(
                "completed result products.before must be empty for a run-owned prefix"
            )
        if not products["after"]:
            raise ProtocolError("completed result must contain tclean products")
    elif result["status"] == "recovered_publication":
        if not products["before"] or products["before"] != products["after"]:
            raise ProtocolError(
                "publication recovery must preserve the pre-existing product inventory"
            )
    if expected_cache.get("role") == "cold" and result["status"] in {
        "completed",
        "recovered_publication",
    }:
        cache_after = cache.get("after")
        receipt = cache_after.get("receipt") if isinstance(cache_after, dict) else None
        expected_producer = cold_publication_identity(
            request_id=expected["request_id"],
            effective_kwargs_sha256=expected["effective_kwargs_sha256"],
            products=products["after"],
        )
        if (
            not isinstance(receipt, dict)
            or receipt.get("producer") != expected_producer
        ):
            raise ProtocolError(
                "cold result receipt does not bind the exact producer request and products"
            )


def _validate_product_inventory_binding(value: Any, *, imagename: str) -> None:
    if not isinstance(value, list):
        raise ProtocolError("result.products.after must be a list")
    seen: set[str] = set()
    for index, product in enumerate(value):
        if not isinstance(product, dict) or set(product) != {
            "path",
            "suffix",
            "inventory",
        }:
            raise ProtocolError(
                f"result.products.after[{index}] fields do not match protocol"
            )
        suffix = product.get("suffix")
        if not isinstance(suffix, str) or suffix in seen:
            raise ProtocolError("result product suffixes must be unique strings")
        if suffix and not suffix.startswith("."):
            raise ProtocolError("result product suffix must be empty or start with '.'")
        seen.add(suffix)
        expected_path = imagename + suffix
        if product.get("path") != expected_path:
            raise ProtocolError(
                "result product path is outside the requested imagename"
            )
        inventory = product.get("inventory")
        if not isinstance(inventory, dict) or inventory.get("root") != expected_path:
            raise ProtocolError("result product inventory root does not match its path")


def failure_result(
    *, request_id: str, status: str, kind: str, reason: str, exception_type: str
) -> dict[str, Any]:
    result = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "kind": RESULT_KIND,
        "status": status,
        "request_id": request_id or "unknown",
        "failure": {
            "kind": kind,
            "reason": reason,
            "exception_type": exception_type,
        },
    }
    validate_result_envelope(result)
    return result


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 2:
        print("usage: casa_tclean.py REQUEST.json RESULT.json", file=sys.stderr)
        return 2
    request_path = pathlib.Path(arguments[0])
    output_path = pathlib.Path(arguments[1])
    request_id = "unknown"
    try:
        request = _load_json_object(request_path, description="CASA tclean request")
        raw_request_id = request.get("request_id")
        if isinstance(raw_request_id, str) and raw_request_id:
            request_id = raw_request_id
        result = process_request(request)
    except KeyboardInterrupt:
        result = failure_result(
            request_id=request_id,
            status="failed_execution",
            kind="operator_interrupt",
            reason="CASA tclean protocol was interrupted before completion",
            exception_type="KeyboardInterrupt",
        )
    except ProtocolError as error:
        result = failure_result(
            request_id=request_id,
            status="failed_validation",
            kind="protocol",
            reason=str(error),
            exception_type=type(error).__name__,
        )
    except Exception as error:  # Keep unexpected CASA/import failures machine-readable.
        result = failure_result(
            request_id=request_id,
            status="failed_execution",
            kind="runtime",
            reason=str(error),
            exception_type=type(error).__name__,
        )
    _write_json_atomic(output_path, result)
    return 0


def _execution_result(
    plan: dict[str, Any],
    *,
    status: str,
    casa_version: str,
    signature: dict[str, Any],
    wall_seconds: float,
    stage_timings_seconds: dict[str, float],
    resources_before: dict[str, Any],
    resources_after: dict[str, Any],
    products_before: list[dict[str, Any]],
    products_after: list[dict[str, Any]],
    cache_before: dict[str, Any],
    cache_after: dict[str, Any],
    return_value: Any,
    failure: dict[str, Any] | None,
) -> dict[str, Any]:
    result = dict(plan)
    result["status"] = status
    result["casa"] = {
        **plan["casa"],
        "actual_version": casa_version,
        "tclean_signature": signature,
    }
    result["wall_seconds"] = float(wall_seconds)
    result["stage_timings_seconds"] = stage_timings_seconds
    if resources_before["disk_io_source"] != resources_after["disk_io_source"]:
        raise ProtocolError("process disk-I/O source changed during CASA execution")
    before_values = resources_before["values"]
    after_values = resources_after["values"]
    result["resources"] = {
        "schema_version": 1,
        "scope": "casa_python_process_during_protocol_execution",
        "peak_rss_source": "getrusage_rusage_self",
        "disk_io_source": resources_before["disk_io_source"],
        "before": before_values,
        "after": after_values,
        "delta": _resource_delta(before_values, after_values),
    }
    result["products"] = {"before": products_before, "after": products_after}
    result["cache"] = {
        **plan["cache"],
        "before": cache_before,
        "after": cache_after,
    }
    result["tclean_return"] = {
        "type": type(return_value).__name__ if return_value is not None else None,
        "present": return_value is not None,
    }
    if failure is not None:
        result["failure"] = failure
    validate_result_envelope(result)
    return result


def _resource_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"peak_rss_bytes": after["peak_rss_bytes"]}
    for name in (
        "user_cpu_seconds",
        "system_cpu_seconds",
        "minor_page_faults",
        "major_page_faults",
        "block_input_operations",
        "block_output_operations",
        "disk_read_bytes",
        "disk_write_bytes",
        "voluntary_context_switches",
        "involuntary_context_switches",
    ):
        result[name] = after[name] - before[name]
    return result


def _validate_cache_matches_effective_call(
    cache: dict[str, Any],
    effective_kwargs: dict[str, Any],
    *,
    expected_casa_version: str,
    recipe_sha256: str,
) -> None:
    gridder = str(effective_kwargs.get("gridder", ""))
    if cache["role"] == "none":
        if gridder.startswith("awpr"):
            raise ProtocolError(
                "AWProject execution requires an explicit cold or warm cache role"
            )
        return
    expected_path = str(_cache_working_path(cache))
    if effective_kwargs.get("cfcache") != expected_path:
        raise ProtocolError(
            "effective cfcache parameter must exactly match the cache execution path"
        )
    cache_plan = cache.get("plan")
    if not isinstance(cache_plan, dict):
        raise ProtocolError("cache plan must be an object")
    required_plan_fields = {
        "schema_version",
        "kind",
        "casa_version",
        "dataset",
        "recipe_sha256",
        "cf_parameters",
    }
    optional_plan_fields = {"runtime_identity", "dataset_geometry"}
    unknown = sorted(set(cache_plan) - required_plan_fields - optional_plan_fields)
    missing = sorted(required_plan_fields - set(cache_plan))
    if unknown or missing:
        raise ProtocolError(
            "cache plan field mismatch; "
            f"missing={missing or 'none'}; unknown={unknown or 'none'}"
        )
    if cache_plan.get("schema_version") != 1:
        raise ProtocolError("cache plan schema_version must be 1")
    if cache_plan.get("kind") != "casa_tclean_cf_plan":
        raise ProtocolError("cache plan kind must be 'casa_tclean_cf_plan'")
    if cache_plan.get("casa_version") != expected_casa_version:
        raise ProtocolError(
            "cache plan CASA version does not match the invocation runtime"
        )
    if cache_plan.get("recipe_sha256") != recipe_sha256:
        raise ProtocolError("cache plan recipe digest does not match the loaded recipe")
    dataset = cache_plan.get("dataset")
    if not isinstance(dataset, dict):
        raise ProtocolError("cache plan dataset must be an object")
    dataset_fields = set(dataset)
    if dataset_fields not in ({"key", "path"}, {"key", "identity"}):
        raise ProtocolError(
            "cache plan dataset must contain exactly key plus path or identity"
        )
    _nonempty_string(dataset.get("key"), field="cache plan dataset.key")
    if "path" in dataset:
        dataset_path = _absolute_path(
            dataset.get("path"), field="cache plan dataset.path"
        )
        if str(dataset_path) != effective_kwargs.get("vis"):
            raise ProtocolError(
                "cache plan dataset path does not match effective CASA vis"
            )
    elif not isinstance(dataset.get("identity"), dict):
        raise ProtocolError("cache plan dataset.identity must be an object")
    else:
        canonical_json_bytes(dataset["identity"])
    expected_cf_parameters = cf_cache_parameter_identity(effective_kwargs)
    if cache_plan.get("cf_parameters") != expected_cf_parameters:
        raise ProtocolError(
            "cache plan CF parameters do not match the effective CASA invocation"
        )


def _cache_working_path(cache: dict[str, Any]) -> pathlib.Path:
    if cache["role"] == "none":
        raise ProtocolError("cache role none has no filesystem working path")
    value = cache.get("working_path", cache.get("path"))
    path = _absolute_path(value, field="result.cache.working_path")
    final_path = pathlib.Path(cache["path"])
    if cache["role"] == "cold":
        if path.parent != final_path.parent or not path.name.endswith(".partial"):
            raise ProtocolError(
                "cold CF cache working path must be a .partial sibling of the final path"
            )
        if path == final_path:
            raise ProtocolError(
                "cold CF cache must not write directly to its final path"
            )
    elif path != final_path:
        raise ProtocolError("warm CF cache must execute against its final path")
    return path


def _cache_working_receipt_path(cache: dict[str, Any]) -> pathlib.Path:
    if cache["role"] != "cold":
        raise ProtocolError("only a cold cache has a working receipt path")
    path = _absolute_path(
        cache.get("working_receipt_path"),
        field="result.cache.working_receipt_path",
    )
    final_path = pathlib.Path(cache["receipt_path"])
    if path.parent != final_path.parent or not path.name.endswith(".partial"):
        raise ProtocolError(
            "cold CF cache working receipt must be a .partial sibling of the receipt"
        )
    if path == final_path:
        raise ProtocolError("cold CF cache receipt must be staged before promotion")
    return path


def _load_cache_receipt(
    path: pathlib.Path, *, allow_legacy_warm: bool = False
) -> dict[str, Any]:
    receipt = _load_json_object(path, description="CF cache receipt")
    version = receipt.get("schema_version")
    if version == 1 and allow_legacy_warm:
        _require_exact_fields(
            receipt, LEGACY_CACHE_RECEIPT_FIELDS, source="legacy CF cache receipt"
        )
    elif version == CACHE_RECEIPT_SCHEMA_VERSION:
        _require_exact_fields(receipt, CACHE_RECEIPT_FIELDS, source="CF cache receipt")
    else:
        raise ProtocolError(
            f"CF cache receipt schema_version must be {CACHE_RECEIPT_SCHEMA_VERSION}"
            + (" or legacy warm-only version 1" if allow_legacy_warm else "")
        )
    if receipt.get("kind") != CACHE_RECEIPT_KIND:
        raise ProtocolError(f"CF cache receipt kind must be {CACHE_RECEIPT_KIND!r}")
    _nonempty_string(receipt.get("cache_path"), field="CF cache receipt.cache_path")
    _sha256_value(receipt.get("plan_sha256"), field="CF cache receipt.plan_sha256")
    _sha256_value(
        receipt.get("stable_tree_sha256"), field="CF cache receipt.stable_tree_sha256"
    )
    if not isinstance(receipt.get("inventory"), dict):
        raise ProtocolError("CF cache receipt.inventory must be an object")
    if version == 1:
        return receipt
    producer = receipt.get("producer")
    if not isinstance(producer, dict):
        raise ProtocolError("CF cache receipt.producer must be an object")
    _require_exact_fields(
        producer, PRODUCER_RECEIPT_FIELDS, source="CF cache receipt.producer"
    )
    _nonempty_string(
        producer.get("request_id"), field="CF cache receipt.producer.request_id"
    )
    _sha256_value(
        producer.get("effective_kwargs_sha256"),
        field="CF cache receipt.producer.effective_kwargs_sha256",
    )
    product_inventory = producer.get("product_inventory")
    if not isinstance(product_inventory, list) or not product_inventory:
        raise ProtocolError(
            "CF cache receipt.producer.product_inventory must be a non-empty list"
        )
    _validate_product_identity_receipt(product_inventory)
    product_inventory_sha256 = _sha256_value(
        producer.get("product_inventory_sha256"),
        field="CF cache receipt.producer.product_inventory_sha256",
    )
    if product_inventory_sha256 != canonical_sha256(product_inventory):
        raise ProtocolError(
            "CF cache receipt producer product inventory digest does not match"
        )
    return receipt


def _load_casa_runtime() -> tuple[Callable[..., Any], str]:
    # CASA imports are intentionally lazy: planning, schema checks, and tests do
    # not trigger measures updates, log setup, Matplotlib, or native libraries.
    import casatasks  # type: ignore[import-not-found]
    from casatasks import tclean  # type: ignore[import-not-found]

    return tclean, str(casatasks.version_string())


def _best_effort_product_inventory(imagename: pathlib.Path) -> list[dict[str, Any]]:
    try:
        return inventory_product_siblings(imagename)
    except Exception as error:
        return [{"status": "inventory_failed", "reason": str(error)}]


def _best_effort_tree_inventory(cache: dict[str, Any]) -> dict[str, Any]:
    if cache["role"] == "none":
        return {"role": "none"}
    try:
        path = _cache_working_path(cache)
        return {
            "role": cache["role"],
            "path": str(path),
            "inventory": tree_inventory(path),
        }
    except Exception as error:
        return {
            "role": cache["role"],
            "status": "inventory_failed",
            "reason": str(error),
        }


def _load_json_object(path: pathlib.Path, *, description: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ProtocolError(f"cannot read {description} {path}: {error}") from error
    if not isinstance(value, dict):
        raise ProtocolError(f"{description} must contain a JSON object")
    return value


def _write_json_atomic(path: pathlib.Path, value: Any) -> None:
    validate = value if isinstance(value, dict) else {"value": value}
    canonical_json_bytes(validate)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = pathlib.Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, allow_nan=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _require_exact_fields(
    value: dict[str, Any], expected: set[str], *, source: str
) -> None:
    unknown = sorted(set(value) - expected)
    missing = sorted(expected - set(value))
    if unknown or missing:
        raise ProtocolError(
            f"{source} field mismatch; missing={missing or 'none'}; unknown={unknown or 'none'}"
        )


def _nonempty_string(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ProtocolError(f"{field} must be a non-empty string")
    return value


def _absolute_path(value: Any, *, field: str) -> pathlib.Path:
    text = _nonempty_string(value, field=field)
    path = pathlib.Path(text).expanduser()
    if not path.is_absolute():
        raise ProtocolError(f"{field} must be an absolute path")
    return path


def _sha256_value(value: Any, *, field: str) -> str:
    text = _nonempty_string(value, field=field)
    if len(text) != 64:
        raise ProtocolError(f"{field} must be a lowercase SHA-256 hex digest")
    try:
        parsed = int(text, 16)
    except ValueError as error:
        raise ProtocolError(
            f"{field} must be a lowercase SHA-256 hex digest"
        ) from error
    if format(parsed, "064x") != text:
        raise ProtocolError(f"{field} must be a lowercase SHA-256 hex digest")
    return text


def _sorted_unique_strings(value: Any, *, field: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, str) and item for item in value)
    ):
        raise ProtocolError(f"{field} must be a non-empty string list")
    if value != sorted(set(value)):
        raise ProtocolError(f"{field} must be sorted and contain no duplicates")
    return list(value)


def _validate_json_value(value: Any, *, source: str) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ProtocolError(f"{source}: floating-point values must be finite")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_json_value(item, source=f"{source}[{index}]")
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for key, item in value.items():
            _validate_json_value(item, source=f"{source}.{key}")
        return
    raise ProtocolError(f"{source}: value is not representable by the JSON protocol")


def _path_is_within(path: pathlib.Path, parent: pathlib.Path) -> bool:
    absolute_path = pathlib.Path(os.path.abspath(path))
    absolute_parent = pathlib.Path(os.path.abspath(parent))
    try:
        absolute_path.relative_to(absolute_parent)
    except ValueError:
        return False
    return True


if __name__ == "__main__":
    raise SystemExit(main())
