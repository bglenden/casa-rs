# SPDX-License-Identifier: LGPL-3.0-or-later
"""Owner-approved, tclean-only weak Taylor WEIGHT comparison (version 1).

This replaces only the numeric comparison of weight.tt1. The measurement
callback must use TaylorMomentAccumulator with aligned products and reference
PB support; errors never authorize an override of a failed raw comparison.
"""
import math

RULE = "tclean-weak-taylor-weight-v1"


def in_scope(parameters):
    return (parameters.get("specmode") == "mfs"
            and parameters.get("stokes") == "I"
            and type(parameters.get("nterms")) is int
            and parameters["nterms"] == 2
            and parameters.get("gridder") in ("mosaic", "awproject"))


def reference_pblimit(parameters):
    value = parameters.get("pblimit")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("recorded reference pblimit must be real")
    value = abs(value)
    if not math.isfinite(value) or not 0 < value < 1:
        raise ValueError("recorded absolute reference pblimit must lie in (0, 1)")
    return value


def evaluate(parameters, raw_passed, measure):
    """Evaluate only the odd-weight numeric gate, preserving ineligible behavior."""
    result = dict(rule=RULE, eligible=False, passed=bool(raw_passed),
                  raw_passed=bool(raw_passed), override=False)
    if not in_scope(parameters):
        return dict(result, reason="outside approved mode; original comparison retained")
    try:
        reference_pblimit(parameters)
        metrics = measure()
        for key in ("rho_l2", "rho_pmax", "e2", "emax"):
            if not math.isfinite(metrics[key]) or metrics[key] < 0:
                raise ValueError("invalid measurement: " + key)
    except (ValueError, KeyError, TypeError, OSError) as error:
        return dict(result, passed=False, reason="guard failure", guard_error=str(error))
    result["metrics"] = metrics
    if metrics["rho_l2"] > 1e-3 or metrics["rho_pmax"] > 1e-3:
        return dict(result, reason="strong reference; original comparison retained")
    passed = bool(metrics["e2"] <= 1e-6 and metrics["emax"] <= 1e-6)
    return dict(result, eligible=True, passed=passed, override=passed and not raw_passed,
                reason="weak-reference matrix-scale comparison")


def apply_to_run(run, gate):
    """Preserve every recorded nonnumeric or unrelated failure, without mutation."""
    product = run.get("products", {}).get("image.weight.tt1", {})
    structural = (product.get("shape_match") is True
                  and product.get("mask_mismatch") == 0
                  and product.get("valid_finite") is True
                  and product.get("beam_within_limits") is True)
    failures = list(run["failures"])
    if gate["passed"] and structural:
        failures = [f for f in failures if f != "image.weight.tt1"]
    elif "image.weight.tt1" not in failures:
        failures.append("image.weight.tt1")
    other_products_ok = all(p.get("passed_recorded_checks") is True
                            for name, p in run.get("products", {}).items()
                            if name != "image.weight.tt1")
    passed = (not failures and structural and other_products_ok
              and run.get("exit_code") == 0 and run.get("inventory_match") is True)
    return dict(gate=gate, failures=failures, passed_recorded_checks=passed,
                original_passed=run["passed"], original_failures=run["failures"])
