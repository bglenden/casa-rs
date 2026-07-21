# SPDX-License-Identifier: LGPL-3.0-or-later
"""Fail-closed integrity checks for publishable CASA evidence bundles.

The workflow writes into ``<run-id>.partial`` and calls this module immediately
before the same-parent atomic rename.  Status strings are only eligibility
signals; every protocol, product, comparison, panel, and cache artifact is
independently rebound to the receipt and rehashed here.
"""

from __future__ import annotations

import copy
import json
import pathlib
import re
import statistics
from typing import Any

from .casa_tclean import (
    CACHE_RECEIPT_FIELDS,
    CACHE_RECEIPT_KIND,
    CACHE_RECEIPT_SCHEMA_VERSION,
    ProtocolError,
    VOLATILE_TREE_FILE_NAMES,
    canonical_sha256,
    cold_publication_identity,
    inventory_product_siblings,
    summarize_completed_results,
    tree_inventory,
    validate_result_for_request,
)
from .image_compare import (
    apply_tolerance_contract,
    validate_comparison_output,
)
from .tree_identity import sha256_file


SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
STABLE_INVENTORY_FIELDS = (
    "exists",
    "root",
    "kind",
    "stable_tree_sha256",
    "included_file_count",
    "included_directory_count",
    "included_symlink_count",
    "logical_bytes",
    "entries",
)


class BundleIntegrityError(ValueError):
    """A nominally complete bundle cannot be published safely."""


def validate_recipe_evidence_bundle(result: dict[str, Any]) -> dict[str, Any]:
    """Revalidate a complete recipe bundle and return receipt-ready evidence."""

    if result.get("status") != "completed":
        raise BundleIntegrityError("bundle integrity requires completed run status")
    artifacts = _object(result, "artifacts", "run result")
    bundle = _object(artifacts, "bundle", "run result artifacts")
    partial_root = _absolute_path(
        bundle.get("partial_root"), label="artifact bundle partial_root"
    )
    if not partial_root.is_dir() or partial_root.is_symlink():
        raise BundleIntegrityError(
            f"artifact bundle partial_root is unavailable or unsafe: {partial_root}"
        )

    expected_suffixes = _expected_product_suffixes(result)
    validated_products: dict[str, dict[str, Any]] = {}
    validated_caches: dict[str, dict[str, Any]] = {}
    calls = _validate_calls(
        result,
        partial_root=partial_root,
        expected_suffixes=expected_suffixes,
        validated_products=validated_products,
        validated_caches=validated_caches,
    )
    _validate_measured_evidence_summary(result, calls=calls)
    comparison_count, panel_count = _validate_repeatability_comparisons(
        result,
        partial_root=partial_root,
        expected_suffixes=expected_suffixes,
        calls=calls,
        validated_products=validated_products,
    )
    _validate_benchmark_log(result, partial_root=partial_root)
    return {
        "status": "passed",
        "validator_version": 1,
        "volatile_tree_exclusions": sorted(VOLATILE_TREE_FILE_NAMES),
        "call_count": len(calls),
        "product_tree_count": len(validated_products),
        "comparison_count": comparison_count,
        "written_panel_count": panel_count,
        "cache_tree_count": len(validated_caches),
    }


def _validate_calls(
    result: dict[str, Any],
    *,
    partial_root: pathlib.Path,
    expected_suffixes: list[str],
    validated_products: dict[str, dict[str, Any]],
    validated_caches: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    results = _object(result, "results", "run result")
    groups = _object(results, "casa_tclean_calls", "run results")
    if set(groups) != {"warmups", "measured"}:
        raise BundleIntegrityError(
            "completed CASA call inventory must contain exactly warmups and measured"
        )
    run = _object(result, "run", "run result")
    expected_counts = {
        "warmups": _nonnegative_int(run.get("warmups"), label="run.warmups"),
        "measured": _positive_int(run.get("repeats"), label="run.repeats"),
    }
    expected_role = _string(run.get("cf_cache_role"), label="run.cf_cache_role")
    records_by_name: dict[str, dict[str, Any]] = {}
    for group_name, expected_count in expected_counts.items():
        records = _list(groups, group_name, "CASA call inventory")
        if len(records) != expected_count:
            raise BundleIntegrityError(
                f"CASA {group_name} count mismatch: expected {expected_count}, "
                f"got {len(records)}"
            )
        for index, value in enumerate(records):
            if not isinstance(value, dict):
                raise BundleIntegrityError(
                    f"CASA {group_name}[{index}] must be an object"
                )
            record = value
            name = _string(record.get("name"), label=f"CASA {group_name}[{index}].name")
            if name in records_by_name:
                raise BundleIntegrityError(f"duplicate CASA call name: {name}")
            expected_name = (
                f"warmup-{index + 1:03d}"
                if group_name == "warmups"
                else f"measured-{index + 1:03d}"
            )
            if name != expected_name:
                raise BundleIntegrityError(
                    f"CASA call name mismatch: expected {expected_name}, got {name}"
                )
            if record.get("role") != expected_role:
                raise BundleIntegrityError(f"CASA call {name} cache role mismatch")
            if record.get("measured") is not (group_name == "measured"):
                raise BundleIntegrityError(f"CASA call {name} measured flag mismatch")
            if record.get("exit_code") != 0:
                raise BundleIntegrityError(f"CASA call {name} did not exit cleanly")
            _validate_call(
                record,
                partial_root=partial_root,
                expected_suffixes=expected_suffixes,
                validated_products=validated_products,
                validated_caches=validated_caches,
            )
            records_by_name[name] = record
    return records_by_name


def _validate_call(
    record: dict[str, Any],
    *,
    partial_root: pathlib.Path,
    expected_suffixes: list[str],
    validated_products: dict[str, dict[str, Any]],
    validated_caches: dict[str, dict[str, Any]],
) -> None:
    name = str(record["name"])
    request_path = _bundle_file(
        record.get("request_path"),
        partial_root=partial_root,
        label=f"CASA call {name} request",
    )
    result_path = _bundle_file(
        record.get("result_path"),
        partial_root=partial_root,
        label=f"CASA call {name} result",
    )
    stdout_path = _bundle_file(
        record.get("stdout_stderr_path"),
        partial_root=partial_root,
        label=f"CASA call {name} stdout/stderr",
    )
    _validate_file_digest(
        request_path,
        record.get("request_sha256"),
        label=f"CASA call {name} request",
    )
    _validate_file_digest(
        result_path,
        record.get("result_sha256"),
        label=f"CASA call {name} result",
    )
    _validate_file_digest(
        stdout_path,
        record.get("stdout_stderr_sha256"),
        label=f"CASA call {name} stdout/stderr",
    )
    request = _load_json_object(request_path, label=f"CASA call {name} request")
    protocol_result = _load_json_object(result_path, label=f"CASA call {name} result")
    if record.get("result") != protocol_result:
        raise BundleIntegrityError(
            f"CASA call {name} embedded result differs from result.json"
        )
    request_cache = _object(request, "cache", f"CASA call {name} request")
    if request_cache.get("role") != record.get("role"):
        raise BundleIntegrityError(
            f"CASA call {name} request cache role does not match the run call role"
        )
    try:
        validate_result_for_request(protocol_result, request)
    except ProtocolError as error:
        raise BundleIntegrityError(
            f"CASA call {name} result/request binding failed: {error}"
        ) from error
    if protocol_result.get("status") != "completed":
        raise BundleIntegrityError(
            f"CASA call {name} protocol result is not benchmark-complete"
        )
    prefix = _absolute_path(record.get("prefix"), label=f"CASA call {name} prefix")
    _require_within(prefix, partial_root, label=f"CASA call {name} prefix")
    if request.get("overrides", {}).get("imagename") != str(prefix):
        raise BundleIntegrityError(f"CASA call {name} prefix/request binding failed")
    if protocol_result.get("effective_kwargs", {}).get("imagename") != str(prefix):
        raise BundleIntegrityError(f"CASA call {name} prefix/result binding failed")

    _validate_call_logs(record, partial_root=partial_root, call_name=name)
    _validate_call_products(
        protocol_result,
        prefix=prefix,
        call_name=name,
        expected_suffixes=expected_suffixes,
        validated_products=validated_products,
    )
    _validate_call_cache(
        record,
        request=request,
        protocol_result=protocol_result,
        call_name=name,
        validated_caches=validated_caches,
    )


def _validate_measured_evidence_summary(
    result: dict[str, Any], *, calls: dict[str, dict[str, Any]]
) -> None:
    measured = [
        record for name, record in calls.items() if name.startswith("measured-")
    ]
    try:
        expected = summarize_completed_results(measured)
    except ProtocolError as error:
        raise BundleIntegrityError(
            f"CASA measured stage/resource evidence is invalid: {error}"
        ) from error
    results = _object(result, "results", "run result")
    casa = _object(results, "casa", "run results")
    run = _object(result, "run", "run result")
    timings = [float(record["result"]["wall_seconds"]) for record in measured]
    expected_casa = {
        "status": "ran",
        "reason": None,
        "timings_seconds": {
            "runs": timings,
            "median": statistics.median(timings),
        },
        "warmup_count": len(calls) - len(measured),
        "cache_role": _string(run.get("cf_cache_role"), label="run.cf_cache_role"),
        "evidence_summary": expected,
    }
    if casa != expected_casa:
        raise BundleIntegrityError(
            "CASA headline timings/evidence do not match bound protocol results "
            "and run configuration"
        )
    stage_medians = _object(results, "stage_medians_ms", "run results")
    expected_stage_medians_ms = {
        name: seconds * 1000.0
        for name, seconds in expected["stage_seconds"]["median"].items()
    }
    if stage_medians.get("casa") != expected_stage_medians_ms:
        raise BundleIntegrityError(
            "CASA stage medians do not match bound protocol results"
        )
    benchmark_features = _object(result, "benchmark_features", "run result")
    feature_resources = _object(
        benchmark_features, "resources", "run result benchmark_features"
    )
    summary_resources = expected["resources"]
    mirrors = {
        "casa_peak_rss_bytes": "peak_rss_bytes_max",
        "casa_user_cpu_seconds_median": "user_cpu_seconds_median",
        "casa_system_cpu_seconds_median": "system_cpu_seconds_median",
        "casa_disk_read_bytes_median": "disk_read_bytes_median",
        "casa_disk_write_bytes_median": "disk_write_bytes_median",
        "casa_block_input_operations_median": "block_input_operations_median",
        "casa_block_output_operations_median": "block_output_operations_median",
        "casa_product_logical_bytes_median": "product_logical_bytes_median",
        "casa_cf_cache_logical_bytes_max": "cf_cache_logical_bytes_max",
        "casa_cf_cache_included_file_count_max": ("cf_cache_included_file_count_max"),
    }
    for feature_name, summary_name in mirrors.items():
        if feature_resources.get(feature_name) != summary_resources[summary_name]:
            raise BundleIntegrityError(
                f"benchmark feature {feature_name} does not match CASA evidence summary"
            )


def _validate_call_logs(
    record: dict[str, Any], *, partial_root: pathlib.Path, call_name: str
) -> None:
    raw_paths = record.get("casa_log_paths")
    identities = record.get("casa_log_identities")
    if not isinstance(raw_paths, list) or not raw_paths:
        raise BundleIntegrityError(f"CASA call {call_name} has no recorded CASA log")
    if not isinstance(identities, list) or len(identities) != len(raw_paths):
        raise BundleIntegrityError(
            f"CASA call {call_name} CASA log identity inventory is incomplete"
        )
    identity_paths: list[str] = []
    for index, identity in enumerate(identities):
        if not isinstance(identity, dict) or set(identity) != {"path", "sha256"}:
            raise BundleIntegrityError(
                f"CASA call {call_name} CASA log identity {index} is invalid"
            )
        path = _bundle_file(
            identity.get("path"),
            partial_root=partial_root,
            label=f"CASA call {call_name} CASA log {index}",
        )
        _validate_file_digest(
            path,
            identity.get("sha256"),
            label=f"CASA call {call_name} CASA log {index}",
        )
        identity_paths.append(str(path))
    if raw_paths != identity_paths:
        raise BundleIntegrityError(
            f"CASA call {call_name} CASA log paths and identities disagree"
        )
    call_root = pathlib.Path(
        _string(record.get("request_path"), label=f"CASA call {call_name} request")
    ).parent
    observed = sorted(str(path) for path in call_root.glob("casa-*.log"))
    if observed != sorted(identity_paths):
        raise BundleIntegrityError(
            f"CASA call {call_name} CASA log inventory is not exact"
        )


def _validate_call_products(
    protocol_result: dict[str, Any],
    *,
    prefix: pathlib.Path,
    call_name: str,
    expected_suffixes: list[str],
    validated_products: dict[str, dict[str, Any]],
) -> None:
    products = _object(protocol_result, "products", f"CASA call {call_name} result")
    after = _list(products, "after", f"CASA call {call_name} products")
    recorded_suffixes = [
        _string(product.get("suffix"), label=f"CASA call {call_name} product suffix")
        if isinstance(product, dict)
        else _raise(f"CASA call {call_name} product must be an object")
        for product in after
    ]
    if recorded_suffixes != expected_suffixes:
        raise BundleIntegrityError(
            f"CASA call {call_name} product inventory mismatch: expected "
            f"{expected_suffixes}, got {recorded_suffixes}"
        )
    observed = inventory_product_siblings(prefix)
    if [product["suffix"] for product in observed] != expected_suffixes:
        raise BundleIntegrityError(
            f"CASA call {call_name} on-disk product inventory is not exact"
        )
    observed_by_path = {
        str(product["path"]): product["inventory"] for product in observed
    }
    for product in after:
        assert isinstance(product, dict)
        path = _absolute_path(
            product.get("path"), label=f"CASA call {call_name} product path"
        )
        expected_path = pathlib.Path(f"{prefix}{product['suffix']}")
        if path != expected_path:
            raise BundleIntegrityError(
                f"CASA call {call_name} product path does not match its suffix"
            )
        recorded_inventory = _object(
            product, "inventory", f"CASA call {call_name} product"
        )
        actual_inventory = observed_by_path.get(str(path))
        if actual_inventory is None:
            raise BundleIntegrityError(
                f"CASA call {call_name} product is missing: {path}"
            )
        _validate_tree_inventory(
            recorded_inventory,
            actual_inventory,
            expected_root=path,
            label=f"CASA call {call_name} product {product['suffix']}",
        )
        prior = validated_products.get(str(path))
        if prior is not None and _stable_inventory(prior) != _stable_inventory(
            actual_inventory
        ):
            raise BundleIntegrityError(f"conflicting product identities for {path}")
        validated_products[str(path)] = actual_inventory


def _validate_call_cache(
    record: dict[str, Any],
    *,
    request: dict[str, Any],
    protocol_result: dict[str, Any],
    call_name: str,
    validated_caches: dict[str, dict[str, Any]],
) -> None:
    cache_request = _object(request, "cache", f"CASA call {call_name} request")
    role = cache_request.get("role")
    if role == "none":
        if record.get("cache_receipt_sha256") is not None:
            raise BundleIntegrityError(
                f"CASA call {call_name} unexpectedly records a cache receipt"
            )
        return
    if role not in {"cold", "warm"}:
        raise BundleIntegrityError(f"CASA call {call_name} has invalid cache role")
    cache_path = _absolute_path(
        cache_request.get("path"), label=f"CASA call {call_name} cache path"
    )
    receipt_path = _regular_file(
        cache_request.get("receipt_path"),
        label=f"CASA call {call_name} cache receipt",
    )
    _validate_file_digest(
        receipt_path,
        record.get("cache_receipt_sha256"),
        label=f"CASA call {call_name} cache receipt",
    )
    receipt = _load_json_object(
        receipt_path, label=f"CASA call {call_name} cache receipt"
    )
    if set(receipt) != CACHE_RECEIPT_FIELDS:
        raise BundleIntegrityError(
            f"CASA call {call_name} cache receipt fields do not match protocol"
        )
    if (
        receipt.get("schema_version") != CACHE_RECEIPT_SCHEMA_VERSION
        or receipt.get("kind") != CACHE_RECEIPT_KIND
        or receipt.get("cache_path") != str(cache_path)
    ):
        raise BundleIntegrityError(
            f"CASA call {call_name} cache receipt envelope is invalid"
        )
    producer = _object(receipt, "producer", f"CASA call {call_name} cache receipt")
    product_inventory = producer.get("product_inventory")
    if not isinstance(product_inventory, list) or not product_inventory:
        raise BundleIntegrityError(
            f"CASA call {call_name} cache producer inventory is invalid"
        )
    if not isinstance(producer.get("product_inventory_sha256"), str) or producer.get(
        "product_inventory_sha256"
    ) != canonical_sha256(product_inventory):
        raise BundleIntegrityError(
            f"CASA call {call_name} cache producer inventory digest is invalid"
        )
    plan = cache_request.get("plan")
    plan_digest = _sha256(
        cache_request.get("plan_sha256"),
        label=f"CASA call {call_name} cache plan",
    )
    if (
        canonical_sha256(plan) != plan_digest
        or receipt.get("plan_sha256") != plan_digest
    ):
        raise BundleIntegrityError(f"CASA call {call_name} cache plan binding failed")
    actual_inventory = tree_inventory(cache_path)
    receipt_inventory = _object(
        receipt, "inventory", f"CASA call {call_name} cache receipt"
    )
    _validate_tree_inventory(
        receipt_inventory,
        actual_inventory,
        expected_root=cache_path,
        label=f"CASA call {call_name} cache",
    )
    if receipt.get("stable_tree_sha256") != actual_inventory["stable_tree_sha256"]:
        raise BundleIntegrityError(
            f"CASA call {call_name} cache receipt digest does not match cache"
        )
    if (
        role == "warm"
        and cache_request.get("expected_stable_tree_sha256")
        != actual_inventory["stable_tree_sha256"]
    ):
        raise BundleIntegrityError(
            f"CASA call {call_name} warm cache expectation does not match cache"
        )
    result_cache = _object(protocol_result, "cache", f"CASA call {call_name} result")
    after = _object(result_cache, "after", f"CASA call {call_name} cache result")
    if after.get("receipt") != receipt:
        raise BundleIntegrityError(
            f"CASA call {call_name} result cache receipt differs from disk"
        )
    if role == "cold":
        products = _object(protocol_result, "products", f"CASA call {call_name} result")
        expected_producer = cold_publication_identity(
            request_id=str(protocol_result["request_id"]),
            effective_kwargs_sha256=str(protocol_result["effective_kwargs_sha256"]),
            products=_list(products, "after", f"CASA call {call_name} products"),
        )
        if producer != expected_producer:
            raise BundleIntegrityError(
                f"CASA call {call_name} cold receipt producer binding failed"
            )
    after_inventory = _object(after, "inventory", f"CASA call {call_name} cache result")
    _validate_tree_inventory(
        after_inventory,
        actual_inventory,
        expected_root=cache_path,
        label=f"CASA call {call_name} result cache",
    )
    if role == "warm":
        before = _object(
            result_cache, "before", f"CASA call {call_name} warm cache result"
        )
        if before.get("receipt") != receipt:
            raise BundleIntegrityError(
                f"CASA call {call_name} warm precondition receipt differs from disk"
            )
        _validate_tree_inventory(
            _object(before, "inventory", f"CASA call {call_name} warm cache before"),
            actual_inventory,
            expected_root=cache_path,
            label=f"CASA call {call_name} warm cache before",
        )
    prior = validated_caches.get(str(cache_path))
    if prior is not None and _stable_inventory(prior) != _stable_inventory(
        actual_inventory
    ):
        raise BundleIntegrityError(f"conflicting cache identities for {cache_path}")
    validated_caches[str(cache_path)] = actual_inventory


def _validate_repeatability_comparisons(
    result: dict[str, Any],
    *,
    partial_root: pathlib.Path,
    expected_suffixes: list[str],
    calls: dict[str, dict[str, Any]],
    validated_products: dict[str, dict[str, Any]],
) -> tuple[int, int]:
    results = _object(result, "results", "run result")
    repeatability = _object(results, "casa_repeatability_comparison", "run results")
    if repeatability.get("status") != "completed":
        raise BundleIntegrityError("CASA repeatability comparison is not completed")
    comparisons = _list(repeatability, "comparisons", "CASA repeatability")
    measured = [name for name in calls if name.startswith("measured-")]
    baseline = measured[0]
    targets = measured[1:] or [baseline]
    expected_count = len(targets)
    if len(comparisons) != expected_count:
        raise BundleIntegrityError(
            f"CASA comparison count mismatch: expected {expected_count}, "
            f"got {len(comparisons)}"
        )
    panel_count = 0
    for index, (comparison, target) in enumerate(zip(comparisons, targets)):
        if not isinstance(comparison, dict):
            raise BundleIntegrityError(f"CASA comparison {index} must be an object")
        self_contract = target == baseline
        panel_count += _validate_comparison(
            comparison,
            index=index,
            partial_root=partial_root,
            expected_suffixes=expected_suffixes,
            calls=calls,
            validated_products=validated_products,
            expected_left_call=baseline,
            expected_right_call=target,
            expected_kind=(
                "single_call_product_contract" if self_contract else "repeatability"
            ),
        )
    last = comparisons[-1]
    _validate_exact_comparison_inventory(
        last.get("product_inventory"),
        expected_suffixes=expected_suffixes,
        label="CASA repeatability summary",
    )
    comparison_mode = comparisons[0].get("comparison_mode")
    source_regions = comparisons[0].get("source_regions")
    tolerances = comparisons[0].get("tolerances")
    if any(
        comparison.get("comparison_mode") != comparison_mode
        or comparison.get("source_regions") != source_regions
        or comparison.get("tolerances") != tolerances
        for comparison in comparisons[1:]
    ):
        raise BundleIntegrityError(
            "CASA measured comparison sequence has inconsistent eligibility fields"
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
    expected_repeatability = {
        "status": "completed",
        "reason": None,
        "comparison_kind": (
            "single_call_product_contract"
            if len(measured) == 1
            else "all_measured_calls_repeatability"
        ),
        "baseline_call": baseline,
        "compared_calls": targets,
        "comparison_mode": comparison_mode,
        "source_regions": source_regions,
        "tolerances": tolerances,
        "products": last.get("products"),
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
    if repeatability != expected_repeatability:
        raise BundleIntegrityError(
            "CASA repeatability summary is not derived from the exact measured "
            "comparison sequence"
        )
    return len(comparisons), panel_count


def _validate_comparison(
    comparison: dict[str, Any],
    *,
    index: int,
    partial_root: pathlib.Path,
    expected_suffixes: list[str],
    calls: dict[str, dict[str, Any]],
    validated_products: dict[str, dict[str, Any]],
    expected_left_call: str,
    expected_right_call: str,
    expected_kind: str,
) -> int:
    label = f"CASA comparison {index}"
    if comparison.get("status") != "completed":
        raise BundleIntegrityError(f"{label} is not completed")
    input_path = _bundle_file(
        comparison.get("input"), partial_root=partial_root, label=f"{label} input"
    )
    output_path = _bundle_file(
        comparison.get("output"), partial_root=partial_root, label=f"{label} output"
    )
    log_path = _bundle_file(
        comparison.get("log"), partial_root=partial_root, label=f"{label} log"
    )
    _validate_file_digest(
        input_path, comparison.get("input_sha256"), label=f"{label} input"
    )
    _validate_file_digest(
        output_path, comparison.get("output_sha256"), label=f"{label} output"
    )
    _validate_file_digest(log_path, comparison.get("log_sha256"), label=f"{label} log")
    request = _load_json_object(input_path, label=f"{label} input")
    raw_output = _load_json_object(output_path, label=f"{label} output")
    try:
        validate_comparison_output(raw_output, request)
    except ValueError as error:
        raise BundleIntegrityError(
            f"{label} output/input binding failed: {error}"
        ) from error
    expected = apply_tolerance_contract(copy.deepcopy(raw_output), request)
    expected.update(
        {
            "input": str(input_path),
            "input_sha256": comparison.get("input_sha256"),
            "output": str(output_path),
            "output_sha256": comparison.get("output_sha256"),
            "log": str(log_path),
            "log_sha256": comparison.get("log_sha256"),
        }
    )
    wrapper_fields = {
        "left_call",
        "right_call",
        "comparison_kind",
    }
    observed_core = {
        key: value for key, value in comparison.items() if key not in wrapper_fields
    }
    if observed_core != expected:
        raise BundleIntegrityError(
            f"{label} receipt fields differ from the bound comparator output"
        )
    _validate_passed_tolerance(comparison.get("tolerance_evaluation"), label=label)
    _validate_exact_comparison_inventory(
        comparison.get("product_inventory"),
        expected_suffixes=expected_suffixes,
        label=label,
    )
    products = _object(comparison, "products", label)
    if list(products) != expected_suffixes:
        raise BundleIntegrityError(f"{label} product result inventory is not exact")
    left_call = _string(comparison.get("left_call"), label=f"{label} left_call")
    right_call = _string(comparison.get("right_call"), label=f"{label} right_call")
    comparison_kind = _string(
        comparison.get("comparison_kind"), label=f"{label} comparison_kind"
    )
    if (
        left_call != expected_left_call
        or right_call != expected_right_call
        or comparison_kind != expected_kind
    ):
        raise BundleIntegrityError(
            f"{label} call topology or comparison kind does not match the exact "
            "measured sequence"
        )
    if left_call not in calls or right_call not in calls:
        raise BundleIntegrityError(f"{label} refers to an unknown CASA call")
    if comparison.get("left_prefix") != calls[left_call].get(
        "prefix"
    ) or comparison.get("right_prefix") != calls[right_call].get("prefix"):
        raise BundleIntegrityError(f"{label} call/prefix binding failed")
    if request.get("left_prefix") != comparison.get("left_prefix") or request.get(
        "right_prefix"
    ) != comparison.get("right_prefix"):
        raise BundleIntegrityError(f"{label} prefix/input binding failed")
    _validate_structure_workspace(
        request,
        input_path=input_path,
        partial_root=partial_root,
        right_call=right_call,
        label=label,
    )
    for suffix, product in products.items():
        if not isinstance(product, dict):
            raise BundleIntegrityError(f"{label} product {suffix} must be an object")
        if product.get("status") != "compared":
            raise BundleIntegrityError(f"{label} product {suffix} was not compared")
        for side in ("left_path", "right_path"):
            path = _string(product.get(side), label=f"{label} {suffix} {side}")
            if path not in validated_products:
                raise BundleIntegrityError(
                    f"{label} {suffix} {side} is not a validated CASA product"
                )
        for alias, canonical in (
            ("rust_path", "left_path"),
            ("casa_path", "right_path"),
        ):
            if alias in product and product.get(alias) != product.get(canonical):
                raise BundleIntegrityError(
                    f"{label} {suffix} {alias} does not match {canonical}"
                )
    beam_info = comparison.get("beam_info")
    if isinstance(beam_info, dict) and beam_info.get("psf_path") is not None:
        psf_path = _string(beam_info.get("psf_path"), label=f"{label} beam PSF path")
        if psf_path not in validated_products:
            raise BundleIntegrityError(
                f"{label} beam PSF is not a validated CASA product"
            )
    return _validate_comparison_panels(
        comparison,
        products=products,
        partial_root=partial_root,
        label=label,
    )


def _validate_structure_workspace(
    request: dict[str, Any],
    *,
    input_path: pathlib.Path,
    partial_root: pathlib.Path,
    right_call: str,
    label: str,
) -> None:
    """Require the request-owned structure workspace to be gone before publish."""

    workspace = _absolute_path(
        request.get("structure_workspace_dir"),
        label=f"{label} structure_workspace_dir",
    )
    _require_within(
        workspace,
        partial_root,
        label=f"{label} structure_workspace_dir",
    )
    expected = input_path.parent / f"casa-{right_call}-structure-workspace"
    if workspace != expected:
        raise BundleIntegrityError(
            f"{label} structure workspace does not match the workflow-owned path"
        )
    if workspace.exists() or workspace.is_symlink():
        raise BundleIntegrityError(
            f"{label} structure workspace remains after a completed comparison: "
            f"{workspace}"
        )


def _validate_comparison_panels(
    comparison: dict[str, Any],
    *,
    products: dict[str, Any],
    partial_root: pathlib.Path,
    label: str,
) -> int:
    panel_dir = _absolute_path(comparison.get("panel_dir"), label=f"{label} panel_dir")
    _require_within(panel_dir, partial_root, label=f"{label} panel_dir")
    if not panel_dir.is_dir() or panel_dir.is_symlink():
        raise BundleIntegrityError(f"{label} panel directory is unavailable or unsafe")
    expected_panels: list[pathlib.Path] = []
    for suffix, product in products.items():
        panel = product.get("review_panel") if isinstance(product, dict) else None
        if not isinstance(panel, dict):
            raise BundleIntegrityError(f"{label} {suffix} has no review panel record")
        if panel.get("status") == "written":
            path = _bundle_file(
                panel.get("path"),
                partial_root=partial_root,
                label=f"{label} {suffix} review panel",
            )
            _validate_file_digest(
                path,
                panel.get("sha256"),
                label=f"{label} {suffix} review panel",
            )
            expected_panels.append(path)
        zoom = panel.get("zoom_panel")
        if isinstance(zoom, dict) and zoom.get("status") == "written":
            path = _bundle_file(
                zoom.get("path"),
                partial_root=partial_root,
                label=f"{label} {suffix} zoom panel",
            )
            _validate_file_digest(
                path,
                zoom.get("sha256"),
                label=f"{label} {suffix} zoom panel",
            )
            expected_panels.append(path)
    for index, path in enumerate(expected_panels):
        try:
            path.relative_to(panel_dir)
        except ValueError as error:
            raise BundleIntegrityError(
                f"{label} panel {index} is outside panel_dir"
            ) from error
    entries = list(panel_dir.iterdir())
    unsafe = [path for path in entries if not path.is_file() or path.is_symlink()]
    if unsafe:
        raise BundleIntegrityError(
            f"{label} panel directory contains unsafe or unexpected entries: {unsafe}"
        )
    observed = sorted(str(path) for path in entries)
    if observed != sorted(str(path) for path in expected_panels):
        raise BundleIntegrityError(f"{label} on-disk panel inventory is not exact")
    return len(expected_panels)


def _validate_passed_tolerance(value: Any, *, label: str) -> None:
    if not isinstance(value, dict) or value.get("status") != "passed":
        raise BundleIntegrityError(f"{label} tolerance evaluation did not pass")
    checks = value.get("checks")
    if not isinstance(checks, list) or not checks:
        raise BundleIntegrityError(f"{label} tolerance evaluation has no checks")
    if any(
        not isinstance(check, dict) or check.get("status") != "passed"
        for check in checks
    ):
        raise BundleIntegrityError(f"{label} contains a non-passing tolerance check")
    if value.get("failed_checks") or value.get("incomplete_checks"):
        raise BundleIntegrityError(
            f"{label} tolerance evaluation is internally inconsistent"
        )


def _validate_exact_comparison_inventory(
    value: Any, *, expected_suffixes: list[str], label: str
) -> None:
    if not isinstance(value, dict):
        raise BundleIntegrityError(f"{label} product_inventory must be an object")
    required = {
        "status": "matched",
        "required": True,
        "observed_match": True,
        "left_right_equal": True,
        "expected": expected_suffixes,
        "left": expected_suffixes,
        "right": expected_suffixes,
        "left_missing": [],
        "left_extra": [],
        "right_missing": [],
        "right_extra": [],
    }
    if any(value.get(key) != expected for key, expected in required.items()):
        raise BundleIntegrityError(f"{label} product inventory is not an exact match")


def _validate_tree_inventory(
    recorded: dict[str, Any],
    actual: dict[str, Any],
    *,
    expected_root: pathlib.Path,
    label: str,
) -> None:
    for inventory, source in ((recorded, "recorded"), (actual, "actual")):
        if inventory.get("root") != str(expected_root):
            raise BundleIntegrityError(f"{label} {source} inventory root mismatch")
        if inventory.get("exists") is not True:
            raise BundleIntegrityError(f"{label} {source} inventory is missing")
        if inventory.get("included_symlink_count") != 0:
            raise BundleIntegrityError(f"{label} {source} inventory contains symlinks")
        entries = inventory.get("entries")
        if not isinstance(entries, list) or canonical_sha256(entries) != inventory.get(
            "stable_tree_sha256"
        ):
            raise BundleIntegrityError(f"{label} {source} tree digest is invalid")
        _validate_volatile_exclusions(
            inventory.get("excluded_volatile"), label=f"{label} {source}"
        )
    if _stable_inventory(recorded) != _stable_inventory(actual):
        raise BundleIntegrityError(f"{label} tree identity changed before promotion")


def _validate_volatile_exclusions(value: Any, *, label: str) -> None:
    if not isinstance(value, list):
        raise BundleIntegrityError(f"{label} volatile exclusion inventory is invalid")
    for entry in value:
        if not isinstance(entry, dict) or set(entry) != {
            "relative_path",
            "bytes",
            "reason",
        }:
            raise BundleIntegrityError(f"{label} volatile exclusion is invalid")
        relative = pathlib.PurePosixPath(
            _string(entry.get("relative_path"), label=f"{label} volatile path")
        )
        if relative.name not in VOLATILE_TREE_FILE_NAMES:
            raise BundleIntegrityError(
                f"{label} excludes undocumented volatile file {relative}"
            )
        if entry.get("reason") != "CASA table.lock is volatile lock state":
            raise BundleIntegrityError(f"{label} volatile exclusion reason is invalid")
        _nonnegative_int(entry.get("bytes"), label=f"{label} volatile bytes")


def _stable_inventory(value: dict[str, Any]) -> dict[str, Any]:
    return {key: value.get(key) for key in STABLE_INVENTORY_FIELDS}


def _validate_benchmark_log(
    result: dict[str, Any], *, partial_root: pathlib.Path
) -> None:
    logs = _object(result, "logs", "run result")
    path = _bundle_file(
        logs.get("benchmark_log"),
        partial_root=partial_root,
        label="benchmark summary log",
    )
    _validate_file_digest(
        path,
        logs.get("benchmark_log_sha256"),
        label="benchmark summary log",
    )


def _expected_product_suffixes(result: dict[str, Any]) -> list[str]:
    comparison = _object(result, "comparison", "run result")
    products = comparison.get("products")
    if not isinstance(products, list) or not products:
        raise BundleIntegrityError("run comparison products must be a non-empty list")
    suffixes = [
        _string(value, label="run comparison product suffix") for value in products
    ]
    if len(set(suffixes)) != len(suffixes):
        raise BundleIntegrityError("run comparison product suffixes are not unique")
    return suffixes


def _bundle_file(value: Any, *, partial_root: pathlib.Path, label: str) -> pathlib.Path:
    path = _regular_file(value, label=label)
    _require_within(path, partial_root, label=label)
    return path


def _regular_file(value: Any, *, label: str) -> pathlib.Path:
    path = _absolute_path(value, label=label)
    if not path.is_file() or path.is_symlink():
        raise BundleIntegrityError(f"{label} is missing or unsafe: {path}")
    return path


def _absolute_path(value: Any, *, label: str) -> pathlib.Path:
    text = _string(value, label=label)
    path = pathlib.Path(text)
    if not path.is_absolute():
        raise BundleIntegrityError(f"{label} must be an absolute path: {path}")
    return path


def _require_within(path: pathlib.Path, root: pathlib.Path, *, label: str) -> None:
    try:
        path.resolve().relative_to(root.resolve())
    except (OSError, ValueError) as error:
        raise BundleIntegrityError(
            f"{label} is outside the partial bundle: {path}"
        ) from error


def _validate_file_digest(path: pathlib.Path, value: Any, *, label: str) -> None:
    expected = _sha256(value, label=f"{label} SHA-256")
    try:
        actual = sha256_file(path)
    except OSError as error:
        raise BundleIntegrityError(f"cannot hash {label} {path}: {error}") from error
    if actual != expected:
        raise BundleIntegrityError(
            f"{label} SHA-256 mismatch: expected {expected}, got {actual}"
        )


def _load_json_object(path: pathlib.Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BundleIntegrityError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise BundleIntegrityError(f"{label} must contain a JSON object")
    return value


def _object(value: dict[str, Any], key: str, label: str) -> dict[str, Any]:
    member = value.get(key)
    if not isinstance(member, dict):
        raise BundleIntegrityError(f"{label}.{key} must be an object")
    return member


def _list(value: dict[str, Any], key: str, label: str) -> list[Any]:
    member = value.get(key)
    if not isinstance(member, list):
        raise BundleIntegrityError(f"{label}.{key} must be a list")
    return member


def _string(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise BundleIntegrityError(f"{label} must be a non-empty string")
    return value


def _sha256(value: Any, *, label: str) -> str:
    text = _string(value, label=label)
    if SHA256_PATTERN.fullmatch(text) is None:
        raise BundleIntegrityError(f"{label} must be a lowercase SHA-256 digest")
    return text


def _nonnegative_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BundleIntegrityError(f"{label} must be a non-negative integer")
    return value


def _positive_int(value: Any, *, label: str) -> int:
    result = _nonnegative_int(value, label=label)
    if result == 0:
        raise BundleIntegrityError(f"{label} must be positive")
    return result


def _raise(message: str) -> Any:
    raise BundleIntegrityError(message)
