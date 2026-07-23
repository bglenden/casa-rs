# SPDX-License-Identifier: LGPL-3.0-or-later
"""Recipe-backed CASA ``tclean`` planning and execution workflow."""

from __future__ import annotations

import copy
from dataclasses import dataclass
import hashlib
import json
import os
import pathlib
import re
import statistics
from typing import Any, Callable

from .artifacts import (
    ArtifactError,
    AtomicDirectoryBundle,
    atomic_write_json,
    prepare_atomic_directory_bundle,
    promote_atomic_directory_bundle,
)
from .bundle_integrity import (
    BundleIntegrityError,
    validate_recipe_bound_benchmark_bundle,
    validate_recipe_evidence_bundle,
)
from .casa_tclean import (
    CACHE_RECEIPT_KIND,
    CF_CACHE_PARAMETER_FIELDS as PROTOCOL_CF_CACHE_PARAMETER_FIELDS,
    REQUEST_KIND,
    REQUEST_SCHEMA_VERSION,
    RESULT_SCHEMA_VERSION,
    build_invocation_plan,
    canonical_sha256,
    cf_cache_parameter_identity as protocol_cf_cache_parameter_identity,
    normalize_archived_parameters,
    parse_literal_assignment_recipe,
    ProtocolError,
    summarize_completed_results,
    validate_result_for_request,
)
from .casa_runtime_identity import (
    stable_identity_projection,
    stable_identity_sha256,
    validate_result as validate_runtime_identity_result,
)
from .dataset_selection_identity import (
    bind_frozen_selection,
    validate_frozen_dataset_geometry_identity,
)
from .errors import HarnessError
from .evidence_storage import (
    requirement_for_workload,
    validate_requirement_capacity,
    validate_requirement_paths,
)
from .image_compare import (
    apply_tolerance_contract,
    compare_products as compare_image_products,
    normalize_comparison_request,
    validate_comparison_output,
)
from .host_telemetry import (
    DarwinHostTelemetrySampler,
    HostTelemetryError,
    validate_host_telemetry,
)
from .schema import RUN_RESULT_SCHEMA_VERSION, validate_run_result
from .subprocesses import run_command
from .tree_identity import sha256_file, tree_identity


REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
CASA_TCLEAN_PROTOCOL = pathlib.Path(__file__).with_name("casa_tclean.py")
CASA_ORACLE_VERSION = "6.7.5.9"

# These are the effective CASA parameters that can change convolution-function
# construction or applicability.  Deconvolution, mask, restoration, output,
# and casa-rs execution-policy controls deliberately do not fragment this key.
CF_CACHE_PARAMETER_FIELDS = PROTOCOL_CF_CACHE_PARAMETER_FIELDS


@dataclass(frozen=True)
class ExecutionServices:
    """Generic result/reporting services supplied by the workload dispatcher."""

    utc_now: Callable[[], str]
    empty_results: Callable[..., dict[str, Any]]
    empty_stage_breakdown: Callable[[str], dict[str, Any]]
    build_benchmark_feature_summary: Callable[..., dict[str, Any]]
    comparison_evidence_status: Callable[..., tuple[str, dict[str, Any] | None]]
    human_review_gate: Callable[..., dict[str, Any]]
    compare_image_products: Callable[..., dict[str, Any]] = compare_image_products


def storage_requirement(
    run: dict[str, Any], dataset: dict[str, Any]
) -> dict[str, Any] | None:
    """Resolve the manifest's storage label through the internal policy registry."""

    return requirement_for_workload(
        dataset_key=_required_str(dataset, "key"),
        storage_label=_str_value(run, "storage_label", "script-staged-tempdir"),
    )


def recipe_run_support(
    *,
    workload_id: str,
    imaging: dict[str, Any],
    skip_casa: bool,
    skip_rust: bool,
    reuse_casa: bool = False,
) -> dict[str, Any]:
    missing = rust_missing_capabilities(imaging)
    casa_target = {
        "status": (
            "runnable" if not skip_casa else ("reused" if reuse_casa else "unavailable")
        ),
        "reason": (
            None
            if not skip_casa
            else (
                "frozen CASA products are reused"
                if reuse_casa
                else "run.skip_casa is enabled"
            )
        ),
        "runner": str(CASA_TCLEAN_PROTOCOL),
    }
    rust_target = {
        "status": "unavailable" if skip_rust or missing else "runnable",
        "reason": (
            "; ".join(missing)
            if missing
            else ("run.skip_rust is enabled" if skip_rust else None)
        ),
        "missing_capabilities": missing,
    }
    if missing and not skip_rust:
        status = "dry_run_only"
        reason = (
            f"{workload_id}: casa-rs cannot execute the frozen semantics: "
            + "; ".join(missing)
        )
    elif not skip_rust and skip_casa and not reuse_casa:
        status = "dry_run_only"
        reason = (
            f"{workload_id}: recipe-bound Rust comparison requires frozen CASA "
            "products through run.reuse_casa_prefix"
        )
    elif not skip_rust:
        status = "runnable"
        reason = None
    elif not skip_casa:
        status = "casa_only"
        reason = "CASA oracle is runnable; casa-rs is explicitly skipped"
    else:
        status = "dry_run_only"
        reason = f"{workload_id}: both recipe targets are disabled"
    return {
        "status": status,
        "reason": reason,
        "targets": {"casa": casa_target, "rust": rust_target},
        "bench_script": None,
    }


def rust_missing_capabilities(imaging: dict[str, Any]) -> list[str]:
    """Report actual unsupported recipe semantics at the production boundary.

    The original Wave 1 implementation deliberately listed the whole VLASS
    request as unavailable.  Keep this check fail-closed, but key it to the
    remaining unsupported combinations now that the shared AWProject,
    multi-SPW, POINTING, and MT-MFS paths exist.
    """

    missing: list[str] = []
    gridder = str(imaging.get("gridder", ""))
    if gridder in {"awp2", "awphpg", "widefield"}:
        missing.append(
            f"gridder={gridder!r} is not a Rust AWProject alias; use gridder='awproject'"
        )
    if gridder == "awproject":
        cfcache = imaging.get("cfcache")
        if not isinstance(cfcache, str) or not cfcache:
            missing.append("an explicit existing CASA AWProject CF cache path")
        if imaging.get("specmode") != "mfs" or imaging.get("deconvolver") != "mtmfs":
            missing.append(
                "AWProject currently requires MFS with the MT-MFS deconvolver"
            )
    return list(dict.fromkeys(missing))


def resolve_recipe_path(casa: dict[str, Any]) -> pathlib.Path:
    value = casa.get("recipe_path")
    if not isinstance(value, str) or not value:
        raise HarnessError("casa.recipe_path must be a non-empty string")
    path = pathlib.Path(value).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT / path
    path = path.resolve()
    if not path.is_file():
        raise HarnessError(f"CASA recipe does not exist: {path}")
    expected = casa.get("recipe_sha256")
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != expected:
        raise HarnessError(
            f"CASA recipe SHA-256 mismatch: expected {expected}, got {actual}"
        )
    return path


def load_checked_identity(
    casa: dict[str, Any], *, path_key: str, digest_key: str
) -> tuple[pathlib.Path, dict[str, Any], str] | None:
    """Load a repository identity document only after verifying its file hash."""

    value = casa.get(path_key)
    expected = casa.get(digest_key)
    if value is None and expected is None:
        return None
    if not isinstance(value, str) or not value or not isinstance(expected, str):
        raise HarnessError(f"casa.{path_key} and casa.{digest_key} must be strings")
    path = pathlib.Path(value).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT / path
    path = path.resolve()
    if not path.is_file():
        raise HarnessError(f"CASA identity document does not exist: {path}")
    actual = sha256_file(path)
    if actual != expected:
        raise HarnessError(
            f"CASA identity document SHA-256 mismatch for {path}: "
            f"expected {expected}, got {actual}"
        )
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise HarnessError(
            f"cannot read CASA identity document {path}: {error}"
        ) from error
    if not isinstance(document, dict):
        raise HarnessError(f"CASA identity document must be an object: {path}")
    return path, document, actual


def frozen_runtime_identity(casa: dict[str, Any]) -> dict[str, Any] | None:
    loaded = load_checked_identity(
        casa,
        path_key="runtime_identity_path",
        digest_key="runtime_identity_sha256",
    )
    if loaded is None:
        return None
    path, document, file_digest = loaded
    try:
        validate_runtime_identity_result(
            document, expected_casa_version=CASA_ORACLE_VERSION
        )
    except ValueError as error:
        raise HarnessError(
            f"invalid completed CASA runtime identity {path}: {error}"
        ) from error
    identity = document["identity"]
    identity_digest = stable_identity_sha256(identity)
    if document.get("identity_sha256") != identity_digest:
        raise HarnessError(f"CASA runtime identity payload digest is invalid: {path}")
    actual_version = (
        identity.get("modules", {}).get("casatasks", {}).get("reported_version")
    )
    if actual_version != CASA_ORACLE_VERSION:
        raise HarnessError(
            f"CASA runtime identity version is {actual_version!r}, "
            f"expected {CASA_ORACLE_VERSION}"
        )
    return {
        "source_path": str(path),
        "source_sha256": file_digest,
        "identity": identity,
        "stable_identity": stable_identity_projection(identity),
        "identity_sha256": identity_digest,
    }


def frozen_dataset_geometry(
    casa: dict[str, Any],
    *,
    dataset_path: pathlib.Path,
    imaging: dict[str, Any],
    dry_run: bool,
) -> dict[str, Any] | None:
    loaded = load_checked_identity(
        casa,
        path_key="dataset_geometry_path",
        digest_key="dataset_geometry_sha256",
    )
    if loaded is None:
        return None
    path, document, file_digest = loaded
    try:
        validate_frozen_dataset_geometry_identity(document)
    except HarnessError as error:
        raise HarnessError(
            f"invalid frozen dataset/geometry identity {path}: {error}"
        ) from error
    expected_dataset = document["dataset"]
    required_dataset_fields = {"tree_sha256", "file_count", "size_bytes"}
    if dry_run:
        actual_dataset = None
        dataset_status = "expected_only_dry_run"
    else:
        try:
            actual_dataset = tree_identity(dataset_path)
        except (OSError, ValueError) as error:
            raise HarnessError(
                f"cannot identify frozen MeasurementSet {dataset_path}: {error}"
            ) from error
        mismatches = {
            key: {"expected": expected_dataset[key], "actual": actual_dataset[key]}
            for key in sorted(required_dataset_fields)
            if actual_dataset.get(key) != expected_dataset.get(key)
        }
        if mismatches:
            raise HarnessError(
                "MeasurementSet identity does not match its frozen receipt: "
                f"{mismatches}"
            )
        dataset_status = "matched"

    spw_ids = parse_integer_selection(_str_value(imaging, "spw", ""), label="spw")
    selection_name = casa.get("dataset_selection")
    if not isinstance(selection_name, str) or not selection_name:
        raise HarnessError(
            "casa.dataset_selection is required with a frozen dataset geometry"
        )
    frozen_selection = bind_frozen_selection(
        document,
        selection_name=selection_name,
        imaging=imaging,
        spw_ids=spw_ids,
    )
    spw_rows = document["geometry"].get("spectral_windows")
    if not isinstance(spw_rows, list):
        raise HarnessError(f"frozen geometry lacks spectral-window facts: {path}")
    by_id = {
        row.get("id"): row
        for row in spw_rows
        if isinstance(row, dict) and isinstance(row.get("id"), int)
    }
    missing_spws = sorted(set(spw_ids) - set(by_id))
    if missing_spws:
        raise HarnessError(
            f"frozen geometry has no frequency facts for SPW ids {missing_spws}"
        )
    return {
        "source_path": str(path),
        "source_sha256": file_digest,
        "dataset": {
            "status": dataset_status,
            "expected": expected_dataset,
            "actual": actual_dataset,
        },
        "selection": {
            **frozen_selection,
            "data_description_ids": spw_ids,
            "spectral_windows": [by_id[spw_id] for spw_id in spw_ids],
        },
        "geometry": {
            key: document["geometry"].get(key)
            for key in (
                "main_row_count",
                "field_row_count",
                "pointing_row_count",
                "field_groups",
                "correlations",
            )
        },
        "source_receipts": document.get("source_receipts"),
    }


def parse_integer_selection(value: str, *, label: str) -> list[int]:
    """Expand the integer/range portion of a CASA selection expression."""

    result: list[int] = []
    for item in value.split(","):
        token = item.strip().split(":", 1)[0]
        if not token:
            continue
        match = re.fullmatch(r"([0-9]+)(?:~([0-9]+))?", token)
        if match is None:
            raise HarnessError(
                f"frozen {label} identity requires integer/range selection, got {value!r}"
            )
        start = int(match.group(1))
        end = int(match.group(2) or start)
        if end < start:
            raise HarnessError(f"descending {label} range is unsupported: {token}")
        result.extend(range(start, end + 1))
    if not result:
        raise HarnessError(f"frozen {label} selection must not be empty")
    return list(dict.fromkeys(result))


def verified_mask_identity(imaging: dict[str, Any]) -> dict[str, Any] | None:
    value = imaging.get("mask_image")
    if not isinstance(value, str) or not value:
        return None
    path = pathlib.Path(value).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT / path
    path = path.resolve()
    expected = imaging.get("mask_sha256")
    try:
        if path.is_dir() and not path.is_symlink():
            identity = tree_identity(path, excluded_names={"table.lock"})
            actual = identity["tree_sha256"]
            kind = "casa_image_tree"
        elif path.is_file() and not path.is_symlink():
            identity = {"size_bytes": path.stat().st_size}
            actual = sha256_file(path)
            kind = "file"
        else:
            raise HarnessError(f"deterministic mask is missing or unsafe: {path}")
    except (OSError, ValueError) as error:
        raise HarnessError(
            f"cannot identify deterministic mask {path}: {error}"
        ) from error
    if actual != expected:
        raise HarnessError(
            f"deterministic mask SHA-256 mismatch for {path}: "
            f"expected {expected}, got {actual}"
        )
    return {
        "path": str(path),
        "kind": kind,
        "sha256": actual,
        "identity": identity,
    }


def build_recipe_command_plan(
    *,
    casa: dict[str, Any],
    recipe_path: pathlib.Path,
    dataset: dict[str, Any],
    dataset_path: pathlib.Path,
    imaging: dict[str, Any],
    run_support: dict[str, Any],
    casa_python: str | None,
    dry_run: bool,
) -> dict[str, Any]:
    assignments = parse_literal_assignment_recipe(
        recipe_path.read_text(encoding="utf-8"), source=str(recipe_path)
    )
    parameter_names = sorted(name for name in assignments if name != "taskname")
    validate_recipe_manifest_alignment(assignments, imaging)
    if not casa_python:
        casa_python = "<CASA_RS_CASA_PYTHON>"
    base_overrides: dict[str, Any] = {
        "vis": str(dataset_path),
        "field": _str_value(imaging, "field", ""),
        "phasecenter": _int_value(imaging, "phasecenter_field", 0),
        "datacolumn": _str_value(imaging, "datacolumn", "data"),
        "interactive": False,
        "parallel": False,
        "restart": False,
        "niter": _int_value(imaging, "niter", 0),
        "imsize": [_int_value(imaging, "imsize", 128)] * 2,
        "spw": _str_value(imaging, "spw", "0"),
    }
    mask_identity = verified_mask_identity(imaging)
    mask_image = imaging.get("mask_image")
    if isinstance(mask_image, str) and mask_image:
        mask_path = pathlib.Path(mask_image).expanduser()
        if not mask_path.is_absolute():
            mask_path = REPO_ROOT / mask_path
        base_overrides["mask"] = str(mask_path.resolve())
    runtime_identity = frozen_runtime_identity(casa)
    dataset_geometry = frozen_dataset_geometry(
        casa,
        dataset_path=dataset_path,
        imaging=imaging,
        dry_run=dry_run,
    )
    cache_dataset: dict[str, Any] = {"key": _required_str(dataset, "key")}
    if dataset_geometry is None:
        cache_dataset["path"] = str(dataset_path)
    else:
        cache_dataset["identity"] = dataset_geometry["dataset"]["expected"]
    effective_parameters, _, _ = normalize_archived_parameters(
        {name: value for name, value in assignments.items() if name != "taskname"},
        base_overrides,
    )
    cache_plan = {
        "schema_version": 1,
        "kind": "casa_tclean_cf_plan",
        "casa_version": CASA_ORACLE_VERSION,
        "dataset": cache_dataset,
        "recipe_sha256": str(casa["recipe_sha256"]),
        "cf_parameters": cf_cache_parameter_identity(effective_parameters),
    }
    if runtime_identity is not None:
        cache_plan["runtime_identity"] = {
            "identity": runtime_identity["stable_identity"],
            "identity_sha256": runtime_identity["identity_sha256"],
        }
    if dataset_geometry is not None:
        cache_plan["dataset_geometry"] = cache_geometry_identity(dataset_geometry)
    return {
        "kind": "casa_tclean_protocol",
        "env": {},
        "casa": {
            "python": casa_python,
            "runner": str(CASA_TCLEAN_PROTOCOL),
            "expected_version": CASA_ORACLE_VERSION,
            "recipe": {
                "path": str(recipe_path),
                "sha256": str(casa["recipe_sha256"]),
                "task": "tclean",
                "parameter_names": parameter_names,
            },
            "base_overrides": base_overrides,
            "cache_plan": cache_plan,
            "runtime_identity": runtime_identity,
            "dataset_geometry": dataset_geometry,
            "mask_identity": mask_identity,
        },
        "rust": {
            "status": run_support["targets"]["rust"]["status"],
            "intended_parameters": imaging,
            "missing_capabilities": run_support["targets"]["rust"][
                "missing_capabilities"
            ],
        },
    }


def cache_geometry_identity(dataset_geometry: dict[str, Any]) -> dict[str, Any]:
    """Project revalidation evidence into a path/status-independent cache key."""

    return {
        "source_sha256": dataset_geometry["source_sha256"],
        "dataset": dataset_geometry["dataset"]["expected"],
        "selection": dataset_geometry["selection"],
        "geometry": dataset_geometry["geometry"],
        "source_receipts": dataset_geometry.get("source_receipts"),
    }


def cf_cache_parameter_identity(
    effective_parameters: dict[str, Any],
) -> dict[str, Any]:
    """Project the effective CASA call onto CF-affecting science parameters."""

    try:
        return protocol_cf_cache_parameter_identity(effective_parameters)
    except ProtocolError as error:
        raise HarnessError(str(error)) from error


def validate_recipe_manifest_alignment(
    recipe: dict[str, Any], imaging: dict[str, Any]
) -> None:
    mappings = {
        "specmode": "specmode",
        "casa_gridder": "gridder",
        "stokes": "stokes",
        "projection": "projection",
        "interpolation": "interpolation",
        "uvrange": "uvrange",
        "intent": "intent",
        "weighting": "weighting",
        "robust": "robust",
        "perchanweightdensity": "perchanweightdensity",
        "deconvolver": "deconvolver",
        "nterms": "nterms",
        "scales": "scales",
        "smallscalebias": "smallscalebias",
        "gain": "gain",
        "nsigma": "nsigma",
        "minor_cycle_length": "cycleniter",
        "cyclefactor": "cyclefactor",
        "min_psf_fraction": "minpsffraction",
        "max_psf_fraction": "maxpsffraction",
        "facets": "facets",
        "psfphasecenter": "psfphasecenter",
        "vptable": "vptable",
        "mosweight": "mosweight",
        "aterm": "aterm",
        "psterm": "psterm",
        "wbawp": "wbawp",
        "conjbeams": "conjbeams",
        "usepointing": "usepointing",
        "computepastep": "computepastep",
        "rotatepastep": "rotatepastep",
        "pointingoffsetsigdev": "pointingoffsetsigdev",
        "pblimit": "pblimit",
        "normtype": "normtype",
        "pbcor": "pbcor",
        "restoration": "restoration",
        "restoringbeam": "restoringbeam",
        "usemask": "usemask",
        "savemodel": "savemodel",
        "calcres": "calcres",
        "calcpsf": "calcpsf",
        "wprojplanes": "wprojplanes",
    }
    mismatches = []
    for imaging_name, recipe_name in mappings.items():
        if imaging_name in imaging and imaging[imaging_name] != recipe.get(recipe_name):
            mismatches.append(
                f"{imaging_name}={imaging[imaging_name]!r} "
                f"(recipe {recipe_name}={recipe.get(recipe_name)!r})"
            )
    if (
        "cell_arcsec" in imaging
        and recipe.get("cell") != f"{imaging['cell_arcsec']}arcsec"
    ):
        mismatches.append(
            f"cell_arcsec={imaging['cell_arcsec']!r} "
            f"(recipe cell={recipe.get('cell')!r})"
        )
    if "threshold_jy" in imaging and float(imaging["threshold_jy"]) != float(
        recipe.get("threshold", 0.0)
    ):
        mismatches.append(
            f"threshold_jy={imaging['threshold_jy']!r} "
            f"(recipe threshold={recipe.get('threshold')!r})"
        )
    if mismatches:
        raise HarnessError(
            "manifest changes frozen CASA recipe semantics outside approved overrides: "
            + "; ".join(mismatches)
        )


def attach_output_paths(
    plan: dict[str, Any],
    *,
    output_dir: pathlib.Path,
    artifact_root: pathlib.Path,
    cf_cache_root: pathlib.Path,
    dry_run: bool,
) -> None:
    evidence_role = str(plan["run"]["evidence_role"])
    final_root = artifact_root / plan["workload"]["id"] / evidence_role / plan["run_id"]
    partial_root = final_root.with_name(f"{final_root.name}.partial")
    execution_root = partial_root
    comparison_root = execution_root / "comparisons"
    protocol_root = execution_root / "protocol"
    requirement = plan.get("command", {}).get("evidence_storage")
    required_root = (
        requirement.get("required_root") if isinstance(requirement, dict) else None
    )
    scratch_base = (
        pathlib.Path(required_root)
        if isinstance(required_root, str) and required_root
        else artifact_root.parent
    )
    scratch_root = scratch_base / "scratch" / plan["run_id"]
    validate_requirement_paths(requirement, paths=[scratch_root])
    execution_prefix = (
        execution_root / "casa" / f"measured-{plan['run']['repeats']:03d}" / "casa"
    )
    retained_prefix = (
        final_root / "casa" / f"measured-{plan['run']['repeats']:03d}" / "casa"
    )

    cache_plan = plan["command"]["casa"]["cache_plan"]
    cache_plan_sha256 = canonical_sha256(cache_plan)
    version_root = cf_cache_root / CASA_ORACLE_VERSION
    cache_path = version_root / cache_plan_sha256
    cache_receipt = version_root / "receipts" / f"{cache_plan_sha256}.json"
    cache = cache_request_template(
        role=plan["run"]["cf_cache_role"],
        cache_path=cache_path,
        receipt_path=cache_receipt,
        cache_plan=cache_plan,
        cache_plan_sha256=cache_plan_sha256,
    )
    request_template = build_casa_request(
        plan,
        action="plan" if dry_run else "run",
        request_id=f"{plan['run_id']}-template",
        imagename=execution_prefix,
        cache=cache,
    )
    effective_plan = planned_casa_request(request_template, cache_receipt=cache_receipt)
    planned_request_path = protocol_root / "measured-001.request.json"
    planned_result_path = protocol_root / "measured-001.result.json"
    plan["command"]["casa"].update(
        {
            "request_template": request_template,
            "effective_plan": effective_plan,
            "cache_path": str(cache_path),
            "cache_receipt_path": str(cache_receipt),
            "cache_plan_sha256": cache_plan_sha256,
        }
    )
    plan["command"]["argv"] = [
        str(plan["command"]["casa"]["python"]),
        str(CASA_TCLEAN_PROTOCOL),
        str(planned_request_path),
        str(planned_result_path),
    ]
    plan["products"] = {
        "root": None if dry_run else str(final_root),
        "rust_prefix": None,
        "casa_prefix": None if dry_run else str(retained_prefix),
        "execution_root": str(execution_root),
        "execution_casa_prefix": str(execution_prefix),
    }
    plan["artifacts"] = {
        "root": str(artifact_root),
        "result_dir": str(output_dir),
        "products_root": str(execution_root),
        "comparison_root": str(comparison_root),
        "protocol_root": str(protocol_root),
        "tmp_root": None if dry_run else str(scratch_root),
        "cf_cache_root": str(cf_cache_root),
        "bundle": {
            "state": "planned" if dry_run else "partial",
            "partial_root": str(partial_root),
            "final_root": str(final_root),
            "retained_root": None if dry_run else str(partial_root),
            "execution_to_retained": {
                "from": str(partial_root),
                "to": None if dry_run else str(partial_root),
            },
        },
    }
    if not dry_run:
        try:
            prepare_atomic_directory_bundle(final_root)
        except ArtifactError as error:
            raise HarnessError(str(error)) from error
        protocol_root.mkdir(parents=True, exist_ok=True)
        scratch_root.mkdir(parents=True, exist_ok=True)


def attach_recipe_bound_benchmark_paths(
    plan: dict[str, Any],
    *,
    output_dir: pathlib.Path,
    artifact_root: pathlib.Path,
    dry_run: bool,
) -> None:
    """Attach an atomic bundle to a Rust run bound to a frozen CASA recipe."""

    evidence_role = str(plan["run"]["evidence_role"])
    final_root = artifact_root / plan["workload"]["id"] / evidence_role / plan["run_id"]
    partial_root = final_root.with_name(f"{final_root.name}.partial")
    products_root = partial_root / "products"
    comparison_root = partial_root / "comparisons"
    execution_rust_prefix = products_root / "rust" / "rust"
    retained_rust_prefix = final_root / "products" / "rust" / "rust"
    requirement = plan.get("command", {}).get("evidence_storage")
    required_root = (
        requirement.get("required_root") if isinstance(requirement, dict) else None
    )
    scratch_base = (
        pathlib.Path(required_root)
        if isinstance(required_root, str) and required_root
        else artifact_root.parent
    )
    scratch_root = scratch_base / "scratch" / plan["run_id"]
    validate_requirement_paths(requirement, paths=[scratch_root])
    casa_prefix = plan.get("command", {}).get("env", {}).get(
        "IMAGER_BENCH_REUSE_CASA_PREFIX"
    )
    if not isinstance(casa_prefix, str) or not casa_prefix:
        raise HarnessError(
            "recipe-bound Rust benchmark requires a frozen CASA reuse prefix"
        )

    plan["products"] = {
        "root": None if dry_run else str(final_root / "products"),
        "rust_prefix": None if dry_run else str(retained_rust_prefix),
        "casa_prefix": casa_prefix,
        "execution_root": str(products_root),
        "execution_rust_prefix": str(execution_rust_prefix),
    }
    plan["artifacts"] = {
        "root": str(artifact_root),
        "result_dir": str(output_dir),
        "products_root": str(products_root),
        "comparison_root": str(comparison_root),
        "protocol_root": None,
        "tmp_root": None if dry_run else str(scratch_root),
        "bundle": {
            "state": "planned" if dry_run else "partial",
            "partial_root": str(partial_root),
            "final_root": str(final_root),
            "retained_root": None if dry_run else str(partial_root),
            "execution_to_retained": {
                "from": str(partial_root),
                "to": None if dry_run else str(partial_root),
            },
        },
    }
    if not dry_run:
        try:
            prepare_atomic_directory_bundle(final_root)
        except ArtifactError as error:
            raise HarnessError(str(error)) from error
        comparison_root.mkdir(parents=True, exist_ok=True)
        scratch_root.mkdir(parents=True, exist_ok=True)
        plan["command"]["env"]["IMAGER_BENCH_KEEP_OUTPUT_ROOT"] = str(
            products_root
        )
        plan["command"]["env"]["IMAGER_BENCH_TMP_ROOT"] = str(scratch_root)


def bundle_benchmark_log_path(
    plan: dict[str, Any], fallback: pathlib.Path
) -> pathlib.Path:
    bundle = plan.get("artifacts", {}).get("bundle")
    if not isinstance(bundle, dict) or not isinstance(bundle.get("partial_root"), str):
        return fallback
    return pathlib.Path(bundle["partial_root"]) / "benchmark-summary.log"


def benchmark_log_evidence(path: pathlib.Path | None) -> dict[str, Any]:
    """Bind the benchmark summary log bytes into the run-result envelope."""

    if path is None:
        return {"benchmark_log": None, "benchmark_log_sha256": None}
    if not path.is_file():
        raise HarnessError(f"benchmark summary log is missing: {path}")
    return {
        "benchmark_log": str(path),
        "benchmark_log_sha256": sha256_file(path),
    }


def interrupted_run_result(
    plan: dict[str, Any],
    *,
    log_path: pathlib.Path,
    reason: str,
    services: ExecutionServices,
) -> dict[str, Any]:
    """Build the typed recipe receipt for an operator interruption."""

    return failed_recipe_run_result(
        plan,
        log_path=log_path,
        reason=reason,
        services=services,
        failure_kind="operator_interrupt",
        exit_code=130,
    )


def failed_recipe_run_result(
    plan: dict[str, Any],
    *,
    log_path: pathlib.Path,
    reason: str,
    services: ExecutionServices,
    failure_kind: str = "harness",
    exit_code: int = 2,
) -> dict[str, Any]:
    """Build a typed partial receipt for any recipe-workflow failure."""

    partial_calls = _partial_call_records(plan)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(
        f"status=failed_execution kind={failure_kind} reason={reason}\n",
        encoding="utf-8",
    )
    results = services.empty_results(casa_status="failed", reason=reason)
    results["casa_tclean_calls"] = {"partial": partial_calls}
    results["failure"] = {"kind": failure_kind, "reason": reason}
    return {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": "workload_run",
        "status": "failed_execution",
        **plan,
        "started_at": plan["created_at"],
        "completed_at": services.utc_now(),
        "exit_code": exit_code,
        "logs": benchmark_log_evidence(log_path),
        "results": results,
        "human_review": services.human_review_gate(plan, None),
    }


def failed_recipe_bound_benchmark_result(
    plan: dict[str, Any],
    *,
    log_path: pathlib.Path,
    reason: str,
    services: ExecutionServices,
    failure_kind: str = "harness",
    exit_code: int = 2,
) -> dict[str, Any]:
    """Build a typed partial receipt for a bundled Rust benchmark failure."""

    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(
        f"status=failed_execution kind={failure_kind} reason={reason}\n",
        encoding="utf-8",
    )
    results = services.empty_results(casa_status="blocked", reason=reason)
    results["failure"] = {"kind": failure_kind, "reason": reason}
    return {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": "workload_run",
        "status": "failed_execution",
        **plan,
        "started_at": plan["created_at"],
        "completed_at": services.utc_now(),
        "exit_code": exit_code,
        "logs": benchmark_log_evidence(log_path),
        "results": results,
        "human_review": services.human_review_gate(plan, None),
    }


def _partial_call_records(plan: dict[str, Any]) -> list[dict[str, Any]]:
    protocol_root = pathlib.Path(plan.get("artifacts", {}).get("protocol_root", ""))
    if not protocol_root.is_dir():
        return []
    records: list[dict[str, Any]] = []
    for request_path in sorted(protocol_root.glob("*/request.json")):
        call_root = request_path.parent
        records.append(
            {
                "name": call_root.name,
                "request_path": str(request_path),
                "result_path": str(call_root / "result.json"),
                "stdout_stderr_path": str(call_root / "stdout-stderr.log"),
                "casa_log_paths": sorted(
                    str(path) for path in call_root.glob("casa-*.log")
                ),
            }
        )
    return records


def finalize_bundle_result(result: dict[str, Any]) -> dict[str, Any]:
    """Retain a typed partial bundle or atomically publish a complete one."""

    bundle_value = result.get("artifacts", {}).get("bundle")
    if not isinstance(bundle_value, dict):
        return result
    partial_root = pathlib.Path(str(bundle_value["partial_root"]))
    final_root = pathlib.Path(str(bundle_value["final_root"]))
    eligible = _bundle_promotion_eligible(result)
    verified_result = result
    if eligible:
        try:
            integrity = (
                validate_recipe_bound_benchmark_bundle(result)
                if result.get("command", {}).get("kind")
                == "recipe_bound_benchmark"
                else validate_recipe_evidence_bundle(result)
            )
        except (BundleIntegrityError, OSError, ProtocolError) as error:
            return _failed_bundle_integrity_result(
                result, partial_root=partial_root, reason=str(error)
            )
        verified_result = copy.deepcopy(result)
        verified_result.setdefault("results", {})["bundle_integrity"] = integrity
    retained_root = final_root if eligible else partial_root
    state = "complete" if eligible else "partial"
    published = _bundle_publication_view(
        verified_result, retained_root=retained_root, state=state
    )
    receipt_execution_path = partial_root / "receipt.json"
    try:
        validate_run_result(published, source=str(receipt_execution_path))
        atomic_write_json(receipt_execution_path, published)
        if eligible:
            promote_atomic_directory_bundle(
                AtomicDirectoryBundle(
                    final_path=final_root,
                    partial_path=partial_root,
                )
            )
    except (ArtifactError, OSError, TypeError, ValueError) as error:
        retained_after_failure = (
            partial_root
            if partial_root.is_dir()
            else final_root
            if final_root.is_dir()
            else partial_root
        )
        failed = _bundle_publication_view(
            result,
            retained_root=retained_after_failure,
            state="promotion_failed",
        )
        failed["status"] = "failed_execution"
        failed.setdefault("results", {})["failure"] = {
            "kind": "artifact_promotion",
            "reason": str(error),
        }
        failed["exit_code"] = 1
        receipt_after_failure = retained_after_failure / "receipt.json"
        try:
            atomic_write_json(receipt_after_failure, failed)
        except (OSError, TypeError, ValueError):
            pass
        return failed
    return published


def _failed_bundle_integrity_result(
    result: dict[str, Any], *, partial_root: pathlib.Path, reason: str
) -> dict[str, Any]:
    """Retain a typed partial receipt when pre-promotion revalidation fails."""

    failed = _bundle_publication_view(
        result, retained_root=partial_root, state="integrity_failed"
    )
    failed["status"] = "failed_execution"
    failed.setdefault("results", {})["failure"] = {
        "kind": "artifact_integrity",
        "reason": reason,
    }
    failed["results"]["bundle_integrity"] = {
        "status": "failed",
        "validator_version": 1,
        "reason": reason,
    }
    failed["exit_code"] = 1
    try:
        atomic_write_json(partial_root / "receipt.json", failed)
    except (OSError, TypeError, ValueError):
        pass
    return failed


def _bundle_promotion_eligible(result: dict[str, Any]) -> bool:
    if result.get("status") != "completed":
        return False
    if result.get("command", {}).get("kind") == "recipe_bound_benchmark":
        comparison = result.get("results", {}).get("product_comparison")
        if not isinstance(comparison, dict) or comparison.get("status") != "completed":
            return False
        products = comparison.get("products")
        return bool(products) and all(
            isinstance(product, dict) and product.get("status") == "compared"
            for product in products.values()
        )
    repeatability = result.get("results", {}).get("casa_repeatability_comparison")
    if (
        not isinstance(repeatability, dict)
        or repeatability.get("status") != "completed"
    ):
        return False
    comparisons = repeatability.get("comparisons")
    return bool(comparisons) and all(
        isinstance(comparison, dict) and comparison.get("status") == "completed"
        for comparison in comparisons
    )


def _bundle_publication_view(
    result: dict[str, Any], *, retained_root: pathlib.Path, state: str
) -> dict[str, Any]:
    published = copy.deepcopy(result)
    artifacts = published["artifacts"]
    bundle = artifacts["bundle"]
    execution_root = pathlib.Path(bundle["partial_root"])
    bundle.update(
        {
            "state": state,
            "retained_root": str(retained_root),
            "receipt_path": str(retained_root / "receipt.json"),
            "execution_to_retained": {
                "from": str(execution_root),
                "to": str(retained_root),
            },
        }
    )
    for key in ("products_root", "comparison_root", "protocol_root"):
        execution_value = artifacts.get(key)
        artifacts[f"execution_{key}"] = execution_value
        artifacts[f"retained_{key}"] = _retained_path(
            execution_value, execution_root, retained_root
        )
        artifacts[key] = artifacts[f"retained_{key}"]

    products = published.get("products")
    if isinstance(products, dict):
        execution_products_root = products.get("execution_root") or products.get(
            "root"
        )
        products["execution_root"] = execution_products_root
        products["root"] = _retained_path(
            execution_products_root, execution_root, retained_root
        )
        for implementation in ("rust", "casa"):
            prefix_key = f"{implementation}_prefix"
            execution_key = f"execution_{prefix_key}"
            execution_prefix = products.get(execution_key) or products.get(prefix_key)
            if execution_prefix is not None:
                products[execution_key] = execution_prefix
                products[prefix_key] = _retained_path(
                    execution_prefix, execution_root, retained_root
                )

    logs = published.get("logs")
    if isinstance(logs, dict):
        execution_log = logs.get("execution_benchmark_log") or logs.get("benchmark_log")
        logs["execution_benchmark_log"] = execution_log
        logs["benchmark_log"] = _retained_path(
            execution_log, execution_root, retained_root
        )

    results = published.get("results")
    if isinstance(results, dict):
        product_paths = results.get("product_paths")
        if isinstance(product_paths, dict):
            product_root = product_paths.get("execution_product_root") or product_paths.get(
                "product_root"
            )
            if product_root is not None:
                product_paths["execution_product_root"] = product_root
                product_paths["product_root"] = _retained_path(
                    product_root, execution_root, retained_root
                )
            for implementation in ("rust", "casa"):
                prefix_key = f"{implementation}_prefix"
                execution_key = f"execution_{prefix_key}"
                execution_prefix = product_paths.get(execution_key) or product_paths.get(
                    prefix_key
                )
                if execution_prefix is not None:
                    product_paths[execution_key] = execution_prefix
                    product_paths[prefix_key] = _retained_path(
                        execution_prefix, execution_root, retained_root
                    )
        calls = results.get("casa_tclean_calls")
        if isinstance(calls, dict):
            for records in calls.values():
                if isinstance(records, list):
                    for record in records:
                        _attach_retained_call_paths(
                            record, execution_root, retained_root
                        )
        repeatability = results.get("casa_repeatability_comparison")
        if isinstance(repeatability, dict):
            _attach_retained_product_paths(
                repeatability.get("products"), execution_root, retained_root
            )
            for comparison in repeatability.get("comparisons", []):
                if isinstance(comparison, dict):
                    _attach_retained_comparison_paths(
                        comparison, execution_root, retained_root
                    )
        comparison = results.get("product_comparison")
        if isinstance(comparison, dict) and any(
            comparison.get(key) is not None
            for key in ("input", "output", "log", "panel_dir")
        ):
            _attach_retained_comparison_paths(
                comparison, execution_root, retained_root
            )
    return published


def _attach_retained_call_paths(
    record: Any, execution_root: pathlib.Path, retained_root: pathlib.Path
) -> None:
    if not isinstance(record, dict):
        return
    for key in (
        "prefix",
        "request_path",
        "result_path",
        "stdout_stderr_path",
        "host_telemetry_path",
    ):
        record[f"retained_{key}"] = _retained_path(
            record.get(key), execution_root, retained_root
        )
    casa_logs = record.get("casa_log_paths")
    if isinstance(casa_logs, list):
        record["retained_casa_log_paths"] = [
            _retained_path(path, execution_root, retained_root) for path in casa_logs
        ]
    casa_log_identities = record.get("casa_log_identities")
    if isinstance(casa_log_identities, list):
        record["retained_casa_log_identities"] = [
            {
                **identity,
                "path": _retained_path(
                    identity.get("path"), execution_root, retained_root
                ),
            }
            for identity in casa_log_identities
            if isinstance(identity, dict)
        ]


def _attach_retained_comparison_paths(
    comparison: dict[str, Any],
    execution_root: pathlib.Path,
    retained_root: pathlib.Path,
) -> None:
    comparison["retained_artifacts"] = {
        key: _retained_path(comparison.get(key), execution_root, retained_root)
        for key in ("input", "output", "log", "panel_dir")
        if comparison.get(key) is not None
    }
    for key in ("input", "output", "log", "panel_dir"):
        if comparison.get(key) is not None:
            comparison[f"retained_{key}"] = _retained_path(
                comparison[key], execution_root, retained_root
            )
    for key in ("left_prefix", "right_prefix"):
        if comparison.get(key) is not None:
            comparison[f"retained_{key}"] = _retained_path(
                comparison[key], execution_root, retained_root
            )
    beam_info = comparison.get("beam_info")
    if isinstance(beam_info, dict) and beam_info.get("psf_path") is not None:
        beam_info["retained_psf_path"] = _retained_path(
            beam_info["psf_path"], execution_root, retained_root
        )
    _attach_retained_product_paths(
        comparison.get("products"), execution_root, retained_root
    )


def _attach_retained_product_paths(
    products: Any,
    execution_root: pathlib.Path,
    retained_root: pathlib.Path,
) -> None:
    if not isinstance(products, dict):
        return
    for product in products.values():
        if not isinstance(product, dict):
            continue
        for key in ("left_path", "right_path", "rust_path", "casa_path"):
            if product.get(key) is not None:
                product[f"retained_{key}"] = _retained_path(
                    product[key], execution_root, retained_root
                )
        panel = product.get("review_panel")
        if not isinstance(panel, dict):
            continue
        if panel.get("path") is not None:
            panel["retained_path"] = _retained_path(
                panel["path"], execution_root, retained_root
            )
        zoom_panel = panel.get("zoom_panel")
        if isinstance(zoom_panel, dict) and zoom_panel.get("path") is not None:
            zoom_panel["retained_path"] = _retained_path(
                zoom_panel["path"], execution_root, retained_root
            )


def _retained_path(
    value: Any, execution_root: pathlib.Path, retained_root: pathlib.Path
) -> Any:
    if not isinstance(value, str):
        return value
    execution = str(execution_root)
    if value == execution:
        return str(retained_root)
    prefix = execution + os.sep
    if value.startswith(prefix):
        return str(retained_root / value[len(prefix) :])
    return value


def build_casa_request(
    plan: dict[str, Any],
    *,
    action: str,
    request_id: str,
    imagename: pathlib.Path,
    cache: dict[str, Any],
) -> dict[str, Any]:
    casa = plan["command"]["casa"]
    overrides = dict(casa["base_overrides"])
    overrides["imagename"] = str(imagename)
    if cache["role"] != "none":
        overrides["cfcache"] = cache["path"]
    mask_identity = casa.get("mask_identity")
    request_mask_identity = (
        {
            key: copy.deepcopy(mask_identity[key])
            for key in ("kind", "sha256", "identity")
        }
        if isinstance(mask_identity, dict)
        else None
    )
    return {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "kind": REQUEST_KIND,
        "request_id": request_id,
        "action": action,
        "expected_casa_version": casa["expected_version"],
        "recipe": casa["recipe"],
        "overrides": overrides,
        "cache": cache,
        "mask_identity": request_mask_identity,
    }


def cache_request_template(
    *,
    role: str,
    cache_path: pathlib.Path,
    receipt_path: pathlib.Path,
    cache_plan: dict[str, Any],
    cache_plan_sha256: str,
) -> dict[str, Any]:
    if role == "none":
        return {"role": "none"}
    result: dict[str, Any] = {
        "role": role,
        "path": str(cache_path),
        "plan": cache_plan,
        "plan_sha256": cache_plan_sha256,
        "receipt_path": str(receipt_path),
    }
    if role == "warm":
        digest = cache_receipt_digest(receipt_path, required=False)
        result["expected_stable_tree_sha256"] = (
            digest if digest is not None else "<resolved-from-cold-receipt>"
        )
    return result


def planned_casa_request(
    request: dict[str, Any], *, cache_receipt: pathlib.Path
) -> dict[str, Any]:
    expected = request["cache"].get("expected_stable_tree_sha256")
    if expected == "<resolved-from-cold-receipt>":
        return {
            "status": "requires_cache_receipt",
            "cache_receipt_path": str(cache_receipt),
            "warm_request_template": request,
        }
    return build_invocation_plan(request)


def cache_receipt_digest(path: pathlib.Path, *, required: bool) -> str | None:
    if not path.is_file():
        if required:
            raise HarnessError(f"warm CF cache receipt is missing: {path}")
        return None
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise HarnessError(f"cannot read CF cache receipt {path}: {error}") from error
    if receipt.get("kind") != CACHE_RECEIPT_KIND:
        raise HarnessError(f"invalid CF cache receipt kind in {path}")
    digest = receipt.get("stable_tree_sha256")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise HarnessError(f"invalid stable CF cache digest in {path}")
    return digest


def default_cf_cache_root(
    plan: dict[str, Any], artifact_root: pathlib.Path
) -> pathlib.Path:
    if plan.get("command", {}).get("kind") != "casa_tclean_protocol":
        return artifact_root / "cf-cache"
    if artifact_root.name == "artifacts":
        return artifact_root.parent / "cf-cache"
    return artifact_root / "cf-cache"


def validate_storage_preconditions(
    plan: dict[str, Any],
    *,
    output_dir: pathlib.Path,
    artifact_root: pathlib.Path,
    cf_cache_root: pathlib.Path,
) -> None:
    if plan.get("command", {}).get("kind") not in {
        "casa_tclean_protocol",
        "recipe_bound_benchmark",
    }:
        return
    dataset_path = pathlib.Path(plan["dataset"]["path"])
    if not dataset_path.is_absolute():
        raise HarnessError(f"recipe dataset path must be absolute: {dataset_path}")
    roots = [output_dir, artifact_root, cf_cache_root]
    protected_inputs = [dataset_path]
    mask_identity = plan.get("command", {}).get("casa", {}).get("mask_identity")
    if isinstance(mask_identity, dict):
        mask_path = pathlib.Path(str(mask_identity.get("path", "")))
        if not mask_path.is_absolute():
            raise HarnessError(f"recipe mask path must be absolute: {mask_path}")
        protected_inputs.append(mask_path)
    requirement = plan.get("command", {}).get("evidence_storage")
    validate_requirement_paths(requirement, paths=[*protected_inputs, *roots])
    for path in roots:
        path.mkdir(parents=True, exist_ok=True)
    resolved_inputs = [path.resolve() for path in protected_inputs]
    resolved_dataset = resolved_inputs[0]
    resolved_roots = [path.resolve() for path in roots]
    validate_requirement_paths(requirement, paths=[*resolved_inputs, *resolved_roots])
    device = os.stat(resolved_dataset).st_dev
    for path in resolved_roots:
        if os.stat(path).st_dev != device:
            raise HarnessError(
                f"dataset and evidence path must be on the same mounted device: {path}"
            )
    filesystem = os.statvfs(artifact_root)
    free_bytes = filesystem.f_bavail * filesystem.f_frsize
    validate_requirement_capacity(requirement, available_bytes=free_bytes)


def run_recipe_plan(
    plan: dict[str, Any],
    log_path: pathlib.Path,
    *,
    services: ExecutionServices,
) -> dict[str, Any]:
    started = services.utc_now()
    warmup_calls: list[dict[str, Any]] = []
    measured_calls: list[dict[str, Any]] = []
    role = plan["run"]["cf_cache_role"]
    repeats = int(plan["run"]["repeats"])
    warmups = int(plan["run"]["warmups"])
    if role == "cold" and repeats != 1:
        return direct_recipe_failure(
            plan,
            started=started,
            log_path=log_path,
            reason="cold CF evidence requires exactly one measured call",
            records=[],
            services=services,
        )
    if role == "warm" and not recipe_cache_is_complete(plan):
        return direct_recipe_failure(
            plan,
            started=started,
            log_path=log_path,
            reason=(
                "warm CF evidence requires an independently completed cold cache "
                "and matching receipt"
            ),
            records=[],
            services=services,
        )

    for index in range(1, warmups + 1):
        record = execute_casa_recipe_call(
            plan,
            call_name=f"warmup-{index:03d}",
            call_role=role,
            measured=False,
        )
        warmup_calls.append(record)
        protocol_status = record["result"].get("status")
        if protocol_status == "recovered_publication":
            return recovered_publication_run_result(
                plan,
                started=started,
                log_path=log_path,
                recovery_record=record,
                warmup_calls=warmup_calls,
                measured_calls=measured_calls,
                services=services,
            )
        if protocol_status != "completed":
            return direct_recipe_failure(
                plan,
                started=started,
                log_path=log_path,
                reason=protocol_failure_reason(record),
                records=warmup_calls,
                services=services,
            )

    for index in range(1, repeats + 1):
        record = execute_casa_recipe_call(
            plan,
            call_name=f"measured-{index:03d}",
            call_role=role,
            measured=True,
        )
        measured_calls.append(record)
        protocol_status = record["result"].get("status")
        if protocol_status == "recovered_publication":
            return recovered_publication_run_result(
                plan,
                started=started,
                log_path=log_path,
                recovery_record=record,
                warmup_calls=warmup_calls,
                measured_calls=measured_calls,
                services=services,
            )
        if protocol_status != "completed":
            return direct_recipe_failure(
                plan,
                started=started,
                log_path=log_path,
                reason=protocol_failure_reason(record),
                records=[*warmup_calls, *measured_calls],
                services=services,
            )

    try:
        summarize_completed_results(measured_calls)
    except ProtocolError as error:
        return direct_recipe_failure(
            plan,
            started=started,
            log_path=log_path,
            reason=f"invalid CASA stage/resource evidence: {error}",
            records=[*warmup_calls, *measured_calls],
            services=services,
        )
    write_recipe_summary_log(log_path, warmup_calls, measured_calls)
    repeatability = compare_casa_repeatability(
        plan,
        measured_calls,
        log_path,
        comparison_runner=services.compare_image_products,
    )
    try:
        return completed_recipe_run_result(
            plan,
            started=started,
            warmup_calls=warmup_calls,
            measured_calls=measured_calls,
            repeatability=repeatability,
            services=services,
        )
    except ProtocolError as error:
        return direct_recipe_failure(
            plan,
            started=started,
            log_path=log_path,
            reason=str(error),
            records=[*warmup_calls, *measured_calls],
            services=services,
        )


def completed_recipe_run_result(
    plan: dict[str, Any],
    *,
    started: str,
    warmup_calls: list[dict[str, Any]],
    measured_calls: list[dict[str, Any]],
    repeatability: dict[str, Any],
    services: ExecutionServices,
) -> dict[str, Any]:
    """Build the completed recipe envelope from independently bound artifacts."""

    try:
        evidence_summary = summarize_completed_results(measured_calls)
    except ProtocolError as error:
        raise ProtocolError(f"invalid CASA stage/resource evidence: {error}") from error
    timings = [float(record["result"]["wall_seconds"]) for record in measured_calls]
    role = str(plan["run"]["cf_cache_role"])
    casa_stage_medians_ms = {
        name: seconds * 1000.0
        for name, seconds in evidence_summary["stage_seconds"]["median"].items()
    }
    final_prefix = measured_calls[-1]["prefix"]
    plan["products"]["casa_prefix"] = final_prefix
    status, failure = services.comparison_evidence_status(repeatability, required=True)
    results: dict[str, Any] = {
        "rust": {
            "status": "unavailable",
            "reason": plan["run_support"]["targets"]["rust"]["reason"],
            "timings_seconds": {"runs": [], "median": None},
        },
        "casa": {
            "status": "ran",
            "reason": None,
            "timings_seconds": {
                "runs": timings,
                "median": statistics.median(timings),
            },
            "warmup_count": len(warmup_calls),
            "cache_role": role,
            "evidence_summary": evidence_summary,
        },
        "stage_medians_ms": {"rust": {}, "casa": casa_stage_medians_ms},
        "stage_breakdown": casa_protocol_stage_breakdown(
            casa_stage_medians_ms,
            rust_reason=(
                "casa-rs capability is explicitly unavailable for this workload"
            ),
        ),
        "product_paths": {"casa_prefix": final_prefix},
        "product_comparison": {
            "status": "skipped",
            "reason": "casa-rs capability is explicitly unavailable for this workload",
            "products": {},
        },
        "casa_repeatability_comparison": repeatability,
        "casa_tclean_calls": {
            "warmups": warmup_calls,
            "measured": measured_calls,
        },
    }
    if failure is not None:
        results["failure"] = failure
    completed_plan = dict(plan)
    completed_plan["benchmark_features"] = services.build_benchmark_feature_summary(
        plan, results
    )
    return {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": "workload_run",
        "status": status,
        **completed_plan,
        "started_at": started,
        "completed_at": services.utc_now(),
        "exit_code": 0,
        "results": results,
        "human_review": services.human_review_gate(plan, repeatability),
    }


def casa_protocol_stage_breakdown(
    stage_medians_ms: dict[str, float], *, rust_reason: str
) -> dict[str, Any]:
    descriptions = {
        "protocol_preflight": (
            "Run-owned product and cache preconditions, CASA task loading and "
            "signature checks, runtime identity, and deterministic mask validation."
        ),
        "tclean_task": (
            "Opaque CASA tclean task envelope. It includes CASA's internal MS "
            "selection, CF/AW work, gridding, FFTs, deconvolution, restoration, "
            "and product writes; this protocol does not claim internal attribution."
        ),
        "product_inventory": (
            "Stable discovery and full tree hashing of every run-owned CASA product."
        ),
        "cache_postcondition": (
            "CF-cache validation, stable tree hashing, receipt creation, and cold "
            "publication or warm immutability checks."
        ),
        "protocol_total": (
            "End-to-end checked-in CASA protocol execution through all postconditions."
        ),
    }
    categories = {
        name: {
            "status": "measured" if stage_medians_ms[name] > 0 else "measured_zero",
            "reason": None,
            "total_ms": stage_medians_ms[name],
            "components_ms": {name: stage_medians_ms[name]},
            "source_fields": [name],
            "missing_fields": [],
            "description": descriptions[name],
        }
        for name in descriptions
    }
    return {
        "schema_version": 1,
        "units": "milliseconds",
        "instrumentation_scope": "checked-in-casa-tclean-protocol-boundary",
        "contract_review": (
            "Evidence-only protocol instrumentation; no production provider or UI "
            "parameter contract change."
        ),
        "rust": {"status": "skipped", "reason": rust_reason, "categories": {}},
        "casa": {
            "status": "reported",
            "reason": (
                "Protocol-boundary timings only; CASA internal stages remain opaque."
            ),
            "categories": categories,
        },
    }


def execute_casa_recipe_call(
    plan: dict[str, Any], *, call_name: str, call_role: str, measured: bool
) -> dict[str, Any]:
    protocol_root = pathlib.Path(plan["artifacts"]["protocol_root"])
    product_root = pathlib.Path(plan["artifacts"]["products_root"])
    category = "casa" if measured else "casa-warmups"
    prefix = product_root / category / call_name / "casa"
    call_root = protocol_root / call_name
    call_root.mkdir(parents=True, exist_ok=False)
    request_path = call_root / "request.json"
    result_path = call_root / "result.json"
    stdout_path = call_root / "stdout-stderr.log"
    host_telemetry_path = call_root / "host-telemetry.json"
    cache = runtime_cache_request(plan, call_role)
    if cache["role"] != "none":
        pathlib.Path(cache["path"]).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(cache["receipt_path"]).parent.mkdir(parents=True, exist_ok=True)
    request = build_casa_request(
        plan,
        action="run",
        request_id=f"{plan['run_id']}-{call_name}",
        imagename=prefix,
        cache=cache,
    )
    atomic_write_json(request_path, request)
    casa_python = str(plan["command"]["casa"]["python"])
    env = os.environ.copy()
    scratch_root = pathlib.Path(plan["artifacts"]["tmp_root"])
    env["TMPDIR"] = str(scratch_root)
    env["MPLCONFIGDIR"] = str(scratch_root / "matplotlib")
    host_sampler = DarwinHostTelemetrySampler()
    host_sampler.start()
    try:
        completed = run_command(
            [
                casa_python,
                str(CASA_TCLEAN_PROTOCOL),
                str(request_path),
                str(result_path),
            ],
            cwd=call_root,
            environment=env,
            merge_stderr=True,
            stream_stdout=bool(plan["run"].get("stream_log", False)),
            incremental_output_path=stdout_path,
        )
    finally:
        host_telemetry = host_sampler.stop()
        atomic_write_json(host_telemetry_path, host_telemetry)
    stdout_path.write_text(completed.stdout or "", encoding="utf-8")
    casa_log_paths = sorted(call_root.glob("casa-*.log"))
    if result_path.is_file():
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            result = {
                "status": "failed_execution",
                "failure": {
                    "kind": "protocol_result",
                    "reason": f"invalid JSON result: {error}",
                },
            }
    else:
        result = {
            "status": "failed_execution",
            "failure": {
                "kind": "protocol_result",
                "reason": "CASA tclean protocol did not write its result",
            },
        }
    if completed.returncode != 0 and result.get("status") in {
        "completed",
        "recovered_publication",
    }:
        result = {
            **result,
            "status": "failed_execution",
            "failure": {
                "kind": "subprocess",
                "reason": f"CASA protocol exited {completed.returncode}",
            },
        }
    try:
        validate_result_for_request(result, request)
    except ProtocolError as error:
        result = {
            "schema_version": RESULT_SCHEMA_VERSION,
            "kind": "casa_tclean_result",
            "status": "failed_validation",
            "request_id": request["request_id"],
            "failure": {
                "kind": "protocol_result_binding",
                "reason": str(error),
                "exception_type": type(error).__name__,
            },
        }
    cache_receipt_path = (
        pathlib.Path(cache["receipt_path"]) if cache.get("role") != "none" else None
    )
    record = {
        "name": call_name,
        "role": call_role,
        "measured": measured,
        "prefix": str(prefix),
        "request_path": str(request_path),
        "request_sha256": hashlib.sha256(request_path.read_bytes()).hexdigest(),
        "result_path": str(result_path),
        "result_sha256": (
            hashlib.sha256(result_path.read_bytes()).hexdigest()
            if result_path.is_file()
            else None
        ),
        "stdout_stderr_path": str(stdout_path),
        "stdout_stderr_sha256": sha256_file(stdout_path),
        "host_telemetry_path": str(host_telemetry_path),
        "host_telemetry_sha256": sha256_file(host_telemetry_path),
        "host_telemetry": host_telemetry,
        "exit_code": completed.returncode,
        "casa_log_paths": [str(path) for path in casa_log_paths],
        "casa_log_identities": [
            {"path": str(path), "sha256": sha256_file(path)} for path in casa_log_paths
        ],
        "cache_receipt_sha256": (
            sha256_file(cache_receipt_path)
            if cache_receipt_path is not None and cache_receipt_path.is_file()
            else None
        ),
        "result": result,
    }
    return record


def recover_completed_recipe_run(
    failed_result: dict[str, Any],
    log_path: pathlib.Path,
    *,
    services: ExecutionServices,
) -> dict[str, Any]:
    """Rebuild a completed outer receipt without reinvoking CASA or comparison.

    Recovery is deliberately narrow: the retained receipt must be a valid typed
    failure, every expected protocol call and comparator artifact must already
    exist, and their bytes are rebound to the exact frozen plan before a normal
    completed result is constructed.  Final bundle validation still rehashes all
    products, panels, logs, and the external CF-cache publication.
    """

    validate_run_result(failed_result, source="recipe publication recovery input")
    if failed_result.get("status") != "failed_execution":
        raise ProtocolError("recipe publication recovery requires failed_execution")
    failure = failed_result.get("results", {}).get("failure")
    failure_kind = failure.get("kind") if isinstance(failure, dict) else None
    if failure_kind not in {"artifact_promotion", "harness_internal"}:
        raise ProtocolError(
            "recipe publication recovery only accepts post-processing or "
            "artifact-promotion failures"
        )

    plan = _recipe_plan_from_failed_result(failed_result)
    if plan.get("command", {}).get("kind") != "casa_tclean_protocol":
        raise ProtocolError("recipe publication recovery requires CASA tclean plan")
    bundle = plan.get("artifacts", {}).get("bundle")
    if not isinstance(bundle, dict):
        raise ProtocolError("recipe publication recovery has no artifact bundle")
    partial_root = pathlib.Path(str(bundle.get("partial_root", "")))
    final_root = pathlib.Path(str(bundle.get("final_root", "")))
    if not partial_root.is_dir() or partial_root.is_symlink():
        raise ProtocolError("recipe publication recovery partial bundle is unavailable")
    if final_root.exists():
        raise ProtocolError("recipe publication recovery final bundle already exists")

    role = str(plan["run"]["cf_cache_role"])
    warmup_calls = [
        recover_casa_recipe_call(
            plan,
            call_name=f"warmup-{index:03d}",
            call_role=role,
            measured=False,
        )
        for index in range(1, int(plan["run"]["warmups"]) + 1)
    ]
    measured_calls = [
        recover_casa_recipe_call(
            plan,
            call_name=f"measured-{index:03d}",
            call_role=role,
            measured=True,
        )
        for index in range(1, int(plan["run"]["repeats"]) + 1)
    ]
    if not measured_calls:
        raise ProtocolError("recipe publication recovery has no measured calls")
    repeatability = recover_casa_repeatability(plan, measured_calls)
    status, failure = services.comparison_evidence_status(repeatability, required=True)
    if status != "completed" or failure is not None:
        reason = failure.get("reason") if isinstance(failure, dict) else status
        raise ProtocolError(f"recovered comparison evidence is not accepted: {reason}")

    write_recipe_summary_log(log_path, warmup_calls, measured_calls)
    with log_path.open("a", encoding="utf-8") as stream:
        stream.write(
            "recovery=completed_outer_publication "
            "tclean_reinvoked=false comparator_reinvoked=false\n"
        )
    return completed_recipe_run_result(
        plan,
        started=str(failed_result["started_at"]),
        warmup_calls=warmup_calls,
        measured_calls=measured_calls,
        repeatability=repeatability,
        services=services,
    )


def _recipe_plan_from_failed_result(failed_result: dict[str, Any]) -> dict[str, Any]:
    envelope_fields = {
        "schema_version",
        "kind",
        "status",
        "started_at",
        "completed_at",
        "exit_code",
        "logs",
        "results",
        "human_review",
    }
    plan = copy.deepcopy(
        {
            key: value
            for key, value in failed_result.items()
            if key not in envelope_fields
        }
    )
    artifacts = plan.get("artifacts")
    if isinstance(artifacts, dict):
        for key in ("products_root", "comparison_root", "protocol_root"):
            execution_key = f"execution_{key}"
            if isinstance(artifacts.get(execution_key), str):
                artifacts[key] = artifacts[execution_key]
    products = plan.get("products")
    if isinstance(products, dict):
        if isinstance(products.get("execution_root"), str):
            products["root"] = products["execution_root"]
        if isinstance(products.get("execution_casa_prefix"), str):
            products["casa_prefix"] = products["execution_casa_prefix"]
    return plan


def recover_casa_recipe_call(
    plan: dict[str, Any], *, call_name: str, call_role: str, measured: bool
) -> dict[str, Any]:
    """Rebind one completed protocol call from a retained partial bundle."""

    protocol_root = pathlib.Path(plan["artifacts"]["protocol_root"])
    product_root = pathlib.Path(plan["artifacts"]["products_root"])
    category = "casa" if measured else "casa-warmups"
    prefix = product_root / category / call_name / "casa"
    call_root = protocol_root / call_name
    request_path = call_root / "request.json"
    result_path = call_root / "result.json"
    stdout_path = call_root / "stdout-stderr.log"
    host_telemetry_path = call_root / "host-telemetry.json"
    for label, path in (
        ("request", request_path),
        ("result", result_path),
        ("stdout/stderr", stdout_path),
    ):
        if not path.is_file() or path.is_symlink():
            raise ProtocolError(f"recovered CASA call {call_name} {label} is missing")
    try:
        request = json.loads(request_path.read_text(encoding="utf-8"))
        result = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ProtocolError(
            f"cannot load recovered CASA call {call_name}: {error}"
        ) from error
    cache = runtime_cache_request(plan, call_role)
    expected_request = build_casa_request(
        plan,
        action="run",
        request_id=f"{plan['run_id']}-{call_name}",
        imagename=prefix,
        cache=cache,
    )
    if request != expected_request:
        raise ProtocolError(
            f"recovered CASA call {call_name} request differs from frozen plan"
        )
    validate_result_for_request(result, request)
    if result.get("status") != "completed":
        raise ProtocolError(f"recovered CASA call {call_name} is not completed")
    casa_log_paths = sorted(call_root.glob("casa-*.log"))
    if not casa_log_paths:
        raise ProtocolError(f"recovered CASA call {call_name} has no CASA log")
    cache_receipt_path = (
        pathlib.Path(str(cache["receipt_path"]))
        if cache.get("role") != "none"
        else None
    )
    record = {
        "name": call_name,
        "role": call_role,
        "measured": measured,
        "prefix": str(prefix),
        "request_path": str(request_path),
        "request_sha256": sha256_file(request_path),
        "result_path": str(result_path),
        "result_sha256": sha256_file(result_path),
        "stdout_stderr_path": str(stdout_path),
        "stdout_stderr_sha256": sha256_file(stdout_path),
        "exit_code": 0,
        "casa_log_paths": [str(path) for path in casa_log_paths],
        "casa_log_identities": [
            {"path": str(path), "sha256": sha256_file(path)} for path in casa_log_paths
        ],
        "cache_receipt_sha256": (
            sha256_file(cache_receipt_path)
            if cache_receipt_path is not None and cache_receipt_path.is_file()
            else None
        ),
        "result": result,
    }
    if host_telemetry_path.is_file() and not host_telemetry_path.is_symlink():
        try:
            host_telemetry = json.loads(host_telemetry_path.read_text(encoding="utf-8"))
            validate_host_telemetry(host_telemetry)
        except (OSError, json.JSONDecodeError, HostTelemetryError) as error:
            raise ProtocolError(
                f"recovered CASA call {call_name} host telemetry is invalid: {error}"
            ) from error
        record.update(
            {
                "host_telemetry_path": str(host_telemetry_path),
                "host_telemetry_sha256": sha256_file(host_telemetry_path),
                "host_telemetry": host_telemetry,
            }
        )
    return record


def runtime_cache_request(plan: dict[str, Any], role: str) -> dict[str, Any]:
    if role == "none":
        return {"role": "none"}
    casa = plan["command"]["casa"]
    cache_path = pathlib.Path(casa["cache_path"])
    receipt_path = pathlib.Path(casa["cache_receipt_path"])
    result: dict[str, Any] = {
        "role": role,
        "path": str(cache_path),
        "plan": casa["cache_plan"],
        "plan_sha256": casa["cache_plan_sha256"],
        "receipt_path": str(receipt_path),
    }
    if role == "warm":
        result["expected_stable_tree_sha256"] = cache_receipt_digest(
            receipt_path, required=True
        )
    return result


def recipe_cache_is_complete(plan: dict[str, Any]) -> bool:
    casa = plan["command"]["casa"]
    cache_path = pathlib.Path(casa["cache_path"])
    receipt_path = pathlib.Path(casa["cache_receipt_path"])
    return cache_path.is_dir() and receipt_path.is_file()


def compare_casa_repeatability(
    plan: dict[str, Any],
    measured_calls: list[dict[str, Any]],
    log_path: pathlib.Path,
    *,
    comparison_runner: Callable[..., dict[str, Any]] = compare_image_products,
) -> dict[str, Any]:
    if not measured_calls:
        return {
            "status": "failed_validation",
            "reason": "CASA product contract requires at least one measured call",
            "source_regions": plan["comparison"].get("source_regions", []),
            "tolerances": plan["comparison"].get("tolerances"),
            "products": {},
            "comparisons": [],
        }
    comparison_root = pathlib.Path(plan["artifacts"]["comparison_root"])
    casa_python = str(plan["command"]["casa"]["python"])
    baseline = measured_calls[0]
    targets = measured_calls[1:] or [baseline]
    comparisons: list[dict[str, Any]] = []
    for target in targets:
        target_name = str(target["name"])
        self_contract = target is baseline
        request = casa_repeatability_comparison_request(plan, baseline, target)
        comparison = comparison_runner(
            casa_python=casa_python,
            request=request,
            artifact_prefix=comparison_root / f"casa-{target_name}",
            cwd=REPO_ROOT,
        )
        comparison["panel_dir"] = request["panel_dir"]
        comparison["left_call"] = str(baseline["name"])
        comparison["right_call"] = target_name
        comparison["comparison_kind"] = (
            "single_call_product_contract" if self_contract else "repeatability"
        )
        comparisons.append(comparison)

    return summarize_casa_repeatability(plan, measured_calls, comparisons)


def casa_repeatability_comparison_request(
    plan: dict[str, Any],
    baseline: dict[str, Any],
    target: dict[str, Any],
) -> dict[str, Any]:
    """Return the exact comparator request for one measured CASA call pair."""

    comparison_root = pathlib.Path(plan["artifacts"]["comparison_root"])
    target_name = str(target["name"])
    self_contract = target is baseline or target_name == str(baseline["name"])
    return {
        "left_prefix": baseline["prefix"],
        "right_prefix": target["prefix"],
        "left_label": "CASA measured 1",
        "right_label": (
            "CASA measured 1 product contract"
            if self_contract
            else f"CASA {target_name}"
        ),
        "products": plan["comparison"]["products"],
        "max_elements_per_product": plan["comparison"]["max_elements_per_product"],
        "mode": plan["comparison"]["mode"],
        "full_chunk_elements": plan["comparison"]["full_chunk_elements"],
        "require_exact_product_inventory": plan["comparison"][
            "require_exact_product_inventory"
        ],
        "require_metadata_parity": plan["comparison"]["require_metadata_parity"],
        "source_regions": plan["comparison"].get("source_regions", []),
        "tolerances": plan["comparison"].get("tolerances"),
        "panel_dir": str(comparison_root / f"casa-{target_name}-panels"),
        "structure_workspace_dir": str(
            comparison_root / f"casa-{target_name}-structure-workspace"
        ),
    }


def summarize_casa_repeatability(
    plan: dict[str, Any],
    measured_calls: list[dict[str, Any]],
    comparisons: list[dict[str, Any]],
) -> dict[str, Any]:
    """Derive the repeatability envelope from the exact comparison sequence."""

    baseline = measured_calls[0]
    targets = measured_calls[1:] or [baseline]
    failed = next(
        (
            comparison
            for comparison in comparisons
            if comparison.get("status") != "completed"
        ),
        None,
    )
    labels = [
        comparison.get("structured_difference_review", {}).get("label")
        for comparison in comparisons
        if isinstance(comparison.get("structured_difference_review"), dict)
    ]
    worst_label = next(
        (
            label
            for label in ("bad", "investigate", "unknown", "good")
            if label in labels
        ),
        None,
    )
    last = comparisons[-1]
    return {
        "status": "completed" if failed is None else str(failed.get("status")),
        "reason": None if failed is None else failed.get("reason"),
        "comparison_kind": (
            "single_call_product_contract"
            if len(measured_calls) == 1
            else "all_measured_calls_repeatability"
        ),
        "baseline_call": str(baseline["name"]),
        "compared_calls": [str(call["name"]) for call in targets],
        "comparison_mode": plan["comparison"]["mode"],
        "source_regions": plan["comparison"].get("source_regions", []),
        "tolerances": plan["comparison"].get("tolerances"),
        "products": last.get("products", {}),
        "product_inventory": last.get("product_inventory"),
        "structured_difference_review": (
            {
                "label": worst_label,
                "summary": (
                    f"worst label across {len(comparisons)} comparison(s): "
                    f"{worst_label}"
                ),
            }
            if worst_label is not None
            else None
        ),
        "comparisons": comparisons,
    }


def recover_casa_repeatability(
    plan: dict[str, Any], measured_calls: list[dict[str, Any]]
) -> dict[str, Any]:
    """Rebind already-written comparator artifacts without rerunning CASA Python."""

    comparison_root = pathlib.Path(plan["artifacts"]["comparison_root"])
    baseline = measured_calls[0]
    targets = measured_calls[1:] or [baseline]
    comparisons: list[dict[str, Any]] = []
    for target in targets:
        target_name = str(target["name"])
        artifact_prefix = comparison_root / f"casa-{target_name}"
        input_path = artifact_prefix.with_suffix(".comparison-input.json")
        output_path = artifact_prefix.with_suffix(".comparison.json")
        comparison_log_path = artifact_prefix.with_suffix(".comparison.log")
        for label, path in (
            ("input", input_path),
            ("output", output_path),
            ("log", comparison_log_path),
        ):
            if not path.is_file() or path.is_symlink():
                raise ProtocolError(
                    f"recovered CASA comparison {target_name} {label} is missing"
                )
        try:
            request = json.loads(input_path.read_text(encoding="utf-8"))
            raw_output = json.loads(output_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ProtocolError(
                f"cannot load recovered CASA comparison {target_name}: {error}"
            ) from error
        expected_request = normalize_comparison_request(
            casa_repeatability_comparison_request(plan, baseline, target)
        )
        if request != expected_request:
            raise ProtocolError(
                f"recovered CASA comparison {target_name} request differs from plan"
            )
        try:
            validate_comparison_output(raw_output, request)
        except ValueError as error:
            raise ProtocolError(
                f"recovered CASA comparison {target_name} is invalid: {error}"
            ) from error
        comparison = apply_tolerance_contract(copy.deepcopy(raw_output), request)
        if comparison.get("status") != "completed":
            raise ProtocolError(
                f"recovered CASA comparison {target_name} status is "
                f"{comparison.get('status')}"
            )
        structure_workspace = pathlib.Path(request["structure_workspace_dir"])
        if request["mode"] == "full" and structure_workspace.exists():
            raise ProtocolError(
                f"recovered accepted comparison retained workspace: "
                f"{structure_workspace}"
            )
        comparison.update(
            {
                "input": str(input_path),
                "input_sha256": sha256_file(input_path),
                "output": str(output_path),
                "output_sha256": sha256_file(output_path),
                "log": str(comparison_log_path),
                "log_sha256": sha256_file(comparison_log_path),
                "panel_dir": request["panel_dir"],
                "left_call": str(baseline["name"]),
                "right_call": target_name,
                "comparison_kind": (
                    "single_call_product_contract"
                    if target_name == str(baseline["name"])
                    else "repeatability"
                ),
            }
        )
        comparisons.append(comparison)
    return summarize_casa_repeatability(plan, measured_calls, comparisons)


def protocol_failure_reason(record: dict[str, Any]) -> str:
    failure = record.get("result", {}).get("failure")
    if isinstance(failure, dict) and failure.get("reason"):
        return str(failure["reason"])
    return f"CASA tclean protocol call {record.get('name')} failed"


def recovered_publication_run_result(
    plan: dict[str, Any],
    *,
    started: str,
    log_path: pathlib.Path,
    recovery_record: dict[str, Any],
    warmup_calls: list[dict[str, Any]],
    measured_calls: list[dict[str, Any]],
    services: ExecutionServices,
) -> dict[str, Any]:
    """Preserve successful cache publication recovery as non-benchmark evidence."""

    protocol_result = recovery_record["result"]
    recovery = protocol_result.get("casa", {}).get("publication_recovery")
    if recovery != {
        "status": "completed",
        "tclean_reinvoked": False,
        "exact_request_replay_required": True,
    }:
        return direct_recipe_failure(
            plan,
            started=started,
            log_path=log_path,
            reason="CASA publication recovery record is missing non-reinvocation evidence",
            records=[*warmup_calls, *measured_calls],
            services=services,
        )

    write_recipe_summary_log(log_path, warmup_calls, measured_calls)
    reason = (
        f"{recovery_record['name']} completed cold CF-cache publication recovery; "
        "the invocation is retained as non-benchmark evidence and contributes no "
        "timing sample"
    )
    results = services.empty_results(casa_status="recovered_publication", reason=reason)
    results["casa_tclean_calls"] = {
        "warmups": warmup_calls,
        "measured": measured_calls,
    }
    cache = protocol_result.get("cache", {})
    cache_after = cache.get("after", {})
    inventory = cache_after.get("inventory", {})
    results["publication_recovery"] = {
        "kind": "cold_cf_cache_publication",
        "status": "completed",
        "protocol_status": "recovered_publication",
        "call_name": recovery_record["name"],
        "call_phase": "measured" if recovery_record.get("measured") else "warmup",
        "benchmark_eligible": False,
        "timing_accepted": False,
        "tclean_reinvoked": False,
        "exact_request_replay_required": True,
        "cache_path": cache.get("path"),
        "cache_receipt_path": cache.get("receipt_path"),
        "cache_receipt_sha256": recovery_record.get("cache_receipt_sha256"),
        "stable_tree_sha256": inventory.get("stable_tree_sha256"),
    }
    completed_plan = dict(plan)
    completed_plan["benchmark_features"] = services.build_benchmark_feature_summary(
        plan, results
    )
    return {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": "workload_run",
        "status": "recovered_publication",
        **completed_plan,
        "started_at": started,
        "completed_at": services.utc_now(),
        "exit_code": 0,
        "results": results,
        "human_review": services.human_review_gate(plan, None),
    }


def direct_recipe_failure(
    plan: dict[str, Any],
    *,
    started: str,
    log_path: pathlib.Path,
    reason: str,
    records: list[dict[str, Any]],
    services: ExecutionServices,
) -> dict[str, Any]:
    write_recipe_summary_log(log_path, records, [])
    results = services.empty_results(casa_status="failed", reason=reason)
    results["casa_tclean_calls"] = {
        "warmups": [record for record in records if not record.get("measured")],
        "measured": [record for record in records if record.get("measured")],
    }
    results["failure"] = {"kind": "casa_tclean_protocol", "reason": reason}
    return {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": "workload_run",
        "status": "failed_execution",
        **plan,
        "started_at": started,
        "completed_at": services.utc_now(),
        "exit_code": 1,
        "results": results,
        "human_review": services.human_review_gate(plan, None),
    }


def write_recipe_summary_log(
    path: pathlib.Path,
    warmups: list[dict[str, Any]],
    measured: list[dict[str, Any]],
) -> None:
    lines = []
    for record in [*warmups, *measured]:
        result = record.get("result", {})
        lines.append(
            " ".join(
                [
                    f"call={record.get('name')}",
                    f"role={record.get('role')}",
                    f"measured={str(record.get('measured')).lower()}",
                    f"status={result.get('status')}",
                    f"wall_seconds={result.get('wall_seconds')}",
                    f"result={record.get('result_path')}",
                ]
            )
        )
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _required_str(obj: dict[str, Any], key: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value:
        raise HarnessError(f"{key} must be a non-empty string")
    return value


def _str_value(obj: dict[str, Any], key: str, default: str) -> str:
    value = obj.get(key, default)
    if not isinstance(value, str):
        raise HarnessError(f"{key} must be a string")
    return value


def _int_value(obj: dict[str, Any], key: str, default: int) -> int:
    value = obj.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise HarnessError(f"{key} must be an integer")
    return value
