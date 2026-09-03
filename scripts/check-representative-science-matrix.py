#!/usr/bin/env python3
"""Validate programme #486 representative science evidence and ticket coverage."""

from __future__ import annotations

import json
import hashlib
import math
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "resources/imaging-architecture/representative-science-matrix.json"
SHA256 = re.compile(r"[0-9a-f]{64}")
COMMIT = re.compile(r"[0-9a-f]{40}")
EVIDENCE_TIERS = {
    "diagnostic_law",
    "representative_scientific_acceptance",
    "resource_performance",
    "persistence_interoperability",
}


def valid_digest(value: object) -> bool:
    return isinstance(value, str) and SHA256.fullmatch(value) is not None


def scenario_contract_digest(scenario: dict[str, object]) -> str:
    contract = {
        key: scenario[key]
        for key in (
            "id",
            "image_shape",
            "selected_samples",
            "dimensions",
            "validity_rule",
            "casa_analogue",
        )
        if key in scenario
    }
    encoded = json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def require_mode_fact(
    identifier: str,
    mode: dict[str, object],
    field: str,
    expected: object,
    failures: list[str],
) -> None:
    if mode.get(field) != expected:
        failures.append(f"{identifier}: mode fact {field} must be {expected!r}")


def validate_mode_contract(
    identifier: str, receipt: dict[str, object], failures: list[str]
) -> None:
    mode = receipt.get("mode")
    if not isinstance(mode, dict):
        failures.append(f"{identifier}: mode contract is missing")
        return
    if identifier == "standard-mfs-dirty-vla":
        require_mode_fact(identifier, mode, "weightings", ["briggs", "natural"], failures)
    elif identifier == "standard-mfs-hogbom-vla":
        require_mode_fact(identifier, mode, "deconvolver", "hogbom", failures)
        require_mode_fact(identifier, mode, "worker_counts", [1, 4], failures)
        require_mode_fact(identifier, mode, "worker_science_equal", True, failures)
    elif identifier == "standard-mfs-clark-vla":
        require_mode_fact(identifier, mode, "deconvolver", "clark", failures)
    elif identifier == "standard-mfs-multiscale-vla":
        require_mode_fact(identifier, mode, "deconvolver", "multiscale", failures)
        scales = mode.get("scales")
        if not isinstance(scales, list) or len(scales) < 2:
            failures.append(f"{identifier}: multiple multiscale scales are not bound")
    elif identifier == "standard-mfs-box-mask-vla":
        require_mode_fact(identifier, mode, "mask", "user-box", failures)
        require_mode_fact(identifier, mode, "exact_mask_support", True, failures)
    elif identifier == "standard-mfs-automask-vla":
        require_mode_fact(identifier, mode, "mask", "auto-multithresh", failures)
        require_mode_fact(identifier, mode, "exact_mask_support", True, failures)
    elif identifier == "standard-mfs-model-data-vla":
        for field in (
            "final_prediction",
            "model_data_matches_casa",
            "measurement_set_reopens",
            "flags_unchanged",
            "weights_unchanged",
        ):
            require_mode_fact(identifier, mode, field, True, failures)
    elif identifier == "multidomain-outlier-vla":
        if mode.get("domain_count", 0) < 2:
            failures.append(f"{identifier}: at least two image domains are required")
        require_mode_fact(identifier, mode, "overlap_ownership_exact", True, failures)
    elif identifier == "facets-vla":
        if mode.get("facets", 0) < 4:
            failures.append(f"{identifier}: four facets are not bound")
        require_mode_fact(identifier, mode, "facet_seams_checked", True, failures)
    elif identifier == "full-stokes-alma":
        require_mode_fact(identifier, mode, "stokes", ["I", "Q", "U", "V"], failures)
        require_mode_fact(identifier, mode, "cross_hands_nontrivial", True, failures)
    elif identifier in {"spectral-cube-dirty-vla", "spectral-cube-clean-vla"}:
        require_mode_fact(identifier, mode, "specmode", "cube", failures)
    elif identifier == "continuum-subtracted-cube-alma":
        require_mode_fact(identifier, mode, "fitorder", 1, failures)
        require_mode_fact(identifier, mode, "corrected_data_matches_casa", True, failures)
        require_mode_fact(identifier, mode, "downstream_cube_compared", True, failures)
    elif identifier == "moving-source-cube-alma":
        require_mode_fact(identifier, mode, "moving_source_correction", True, failures)
        require_mode_fact(identifier, mode, "specmode", "cube", failures)
    elif identifier == "mtmfs-alma":
        require_mode_fact(identifier, mode, "nterms", 2, failures)
        if mode.get("spectral_windows", 0) < 2 or mode.get("fractional_frequency_span", 0) < 0.1:
            failures.append(f"{identifier}: meaningful multi-SPW frequency leverage is missing")
        require_mode_fact(identifier, mode, "product_count", 19, failures)
    elif identifier == "joint-continuum-line-alma":
        require_mode_fact(identifier, mode, "analytic_joint_recovery", True, failures)
        require_mode_fact(identifier, mode, "sequential_casa_anchor", True, failures)
    elif identifier == "mosaic-alma":
        if mode.get("pointings", 0) < 2:
            failures.append(f"{identifier}: multiple pointings are not bound")
        require_mode_fact(identifier, mode, "overlapping_primary_beams", True, failures)
        require_mode_fact(identifier, mode, "mfs_compared", True, failures)
        if mode.get("cube_channels", 0) < 16:
            failures.append(f"{identifier}: sixteen-channel mosaic cube is not bound")


def validate_receipt(
    scenario: dict[str, object], contract: dict[str, object], failures: list[str]
) -> None:
    identifier = str(scenario["id"])
    relative = scenario.get("receipt")
    if not isinstance(relative, str) or Path(relative).is_absolute():
        failures.append(f"{identifier}: receipt must be a repository-relative path")
        return
    path = ROOT / relative
    if not path.is_file():
        failures.append(f"{identifier}: checked-in receipt is missing: {relative}")
        return
    try:
        receipt = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        failures.append(f"{identifier}: receipt cannot be read: {error}")
        return
    if receipt.get("schema") != contract["receipt_schema"]:
        failures.append(f"{identifier}: receipt schema is not current")
    if receipt.get("scenario_id") != identifier or receipt.get("status") != "pass":
        failures.append(f"{identifier}: receipt identity/status is not a pass")
    if receipt.get("production_path") is not True:
        failures.append(f"{identifier}: receipt does not bind the production path")
    if not isinstance(receipt.get("tested_commit"), str) or not COMMIT.fullmatch(
        receipt["tested_commit"]
    ):
        failures.append(f"{identifier}: tested commit is missing or malformed")
    if receipt.get("contract_digest") != scenario_contract_digest(scenario):
        failures.append(f"{identifier}: scientific contract digest differs from matrix")
    if receipt.get("dimensions") != scenario.get("dimensions"):
        failures.append(f"{identifier}: receipt dimensions differ from matrix")
    if receipt.get("validity_rule") != scenario.get("validity_rule"):
        failures.append(f"{identifier}: receipt validity rule differs from matrix")

    dataset = receipt.get("dataset")
    if not isinstance(dataset, dict):
        failures.append(f"{identifier}: dataset evidence is missing")
    else:
        if not valid_digest(dataset.get("identity_sha256")):
            failures.append(f"{identifier}: dataset identity is missing")
        if dataset.get("selected_samples") != scenario.get("selected_samples"):
            failures.append(f"{identifier}: selected sample count differs from matrix")
        if dataset.get("image_shape") != scenario.get("image_shape"):
            failures.append(f"{identifier}: image shape differs from matrix")
        for field in ("selected_rows", "selected_channels", "correlations"):
            value = dataset.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                failures.append(f"{identifier}: dataset {field} is missing")
        if "cube" in identifier or identifier in {"moving-source-cube-alma", "mosaic-alma"}:
            if dataset.get("selected_channels", 0) < 16:
                failures.append(f"{identifier}: fewer than sixteen selected channels")
        if identifier == "full-stokes-alma" and dataset.get("correlations") != 4:
            failures.append(f"{identifier}: full-Stokes row does not bind four correlations")

    reference = receipt.get("reference")
    if not isinstance(reference, dict) or reference.get("kind") not in {
        "casa",
        "independent",
        "mixed",
    }:
        failures.append(f"{identifier}: reference kind is missing")
    elif not valid_digest(reference.get("identity_sha256")):
        failures.append(f"{identifier}: reference identity is missing")
    elif reference["kind"] in {"casa", "mixed"} and not reference.get("casa_version"):
        failures.append(f"{identifier}: CASA version is missing")

    comparison = receipt.get("comparison")
    if not isinstance(comparison, dict):
        failures.append(f"{identifier}: comparison evidence is missing")
    else:
        maximum = comparison.get("maximum_normalized_rms")
        if (
            isinstance(maximum, bool)
            or not isinstance(maximum, (int, float))
            or not math.isfinite(maximum)
            or maximum < 0.0
            or maximum > contract["maximum_normalized_rms"]
        ):
            failures.append(f"{identifier}: normalized RMS does not satisfy contract")
        if comparison.get("all_values_finite") is not True:
            failures.append(f"{identifier}: finite-value evidence is missing")
        exact = comparison.get("exact")
        if not isinstance(exact, dict) or any(
            exact.get(field) is not True for field in ("topology", "wcs", "validity")
        ):
            failures.append(f"{identifier}: exact topology/WCS/validity evidence is missing")
        inventory = comparison.get("product_inventory")
        if not isinstance(inventory, dict):
            failures.append(f"{identifier}: product inventory is missing")
        else:
            required = inventory.get("required")
            observed = inventory.get("observed")
            if (
                not isinstance(required, list)
                or not required
                or len(required) != len(set(required))
                or observed != required
            ):
                failures.append(f"{identifier}: required product inventory is not exact")

    external = receipt.get("external_receipts")
    if not isinstance(external, list) or not external:
        failures.append(f"{identifier}: external receipt bindings are missing")
    else:
        for binding in external:
            if (
                not isinstance(binding, dict)
                or not binding.get("role")
                or not valid_digest(binding.get("sha256"))
            ):
                failures.append(f"{identifier}: external receipt binding is malformed")
    repository_evidence = receipt.get("repository_evidence")
    if not isinstance(repository_evidence, list) or not repository_evidence:
        failures.append(f"{identifier}: repository evidence is missing")
    else:
        for evidence in repository_evidence:
            if not isinstance(evidence, str) or not (ROOT / evidence).is_file():
                failures.append(f"{identifier}: repository evidence is missing: {evidence}")
    validate_mode_contract(identifier, receipt, failures)


def main() -> int:
    document = json.loads(MATRIX.read_text())
    failures: list[str] = []
    if document.get("schema") != "casa-rs-representative-science-matrix-v2":
        failures.append("unexpected matrix schema")
    contract = document.get("contract", {})
    scenarios = {scenario["id"]: scenario for scenario in document.get("scenarios", [])}
    if len(scenarios) != len(document.get("scenarios", [])):
        failures.append("scenario identifiers are not unique")
    for scenario in scenarios.values():
        identifier = scenario["id"]
        if scenario.get("status") != "pass":
            failures.append(f"{identifier}: matrix status is not pass")
        shape = scenario.get("image_shape", [])
        if len(shape) < 2 or min(shape[:2]) < contract.get("minimum_image_extent", 512):
            failures.append(f"{identifier}: image is below the representative extent")
        if scenario.get("selected_samples", 0) < contract.get(
            "minimum_selected_correlation_channel_samples", 1000000
        ):
            failures.append(f"{identifier}: selected sample volume is below contract")
        if not scenario.get("dimensions"):
            failures.append(f"{identifier}: defining dimensions are missing")
        validate_receipt(scenario, contract, failures)

    tickets = document.get("tickets", [])
    issues = [ticket.get("issue") for ticket in tickets]
    if len(issues) != len(set(issues)):
        failures.append("ticket issues are not unique")
    required = set(range(504, 534)) | {
        500,
        540,
        574,
        580,
        581,
        583,
        586,
        589,
        590,
        591,
        597,
    }
    missing = sorted(required - set(issues))
    extra = sorted(set(issues) - required)
    if missing or extra:
        failures.append(f"ticket issue set differs: missing={missing} extra={extra}")
    used_scenarios = set()
    for ticket in tickets:
        issue = ticket.get("issue")
        scenario = ticket.get("scenario")
        used_scenarios.add(scenario)
        if scenario not in scenarios:
            failures.append(f"issue #{issue}: scenario is missing")
        tiers = ticket.get("existing_evidence_tiers")
        if not isinstance(tiers, list) or not tiers or not set(tiers) <= EVIDENCE_TIERS:
            failures.append(f"issue #{issue}: existing evidence is unclassified")
        if not ticket.get("capability") or not ticket.get("comparators"):
            failures.append(f"issue #{issue}: capability or comparators are missing")
    unused = sorted(set(scenarios) - used_scenarios)
    if unused:
        failures.append(f"scenarios are not mapped to tickets: {unused}")

    if failures:
        for failure in failures:
            print(f"representative-science-matrix: {failure}", file=sys.stderr)
        return 1
    print(
        "representative-science-matrix: "
        f"{len(tickets)} tickets, {len(scenarios)} checked-in receipts, all pass"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
