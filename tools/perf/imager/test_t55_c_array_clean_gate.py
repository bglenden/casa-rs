# SPDX-License-Identifier: LGPL-3.0-or-later
"""Negative controls for the C-array nonlinear CLEAN acceptance rule."""

import copy
import hashlib
import json

import numpy as np
import pytest

import t55_c_array_clean_gate as gate
from t55_c_array_clean_gate import assess_arrays, strict_comparison_checks


def example_arrays():
    pixels = np.arange(64, dtype=np.float64) - 32
    x, y = np.meshgrid(pixels, pixels, indexing="ij")
    restored_model = 0.01 * np.exp(-(x * x + y * y) / 32)
    residual = np.full((64, 64), 1e-4)
    products = {"image": restored_model + residual, "residual": residual}
    return products, copy.deepcopy(products)


def test_matched_scientific_products_pass():
    native, casa = example_arrays()
    assert assess_arrays(native, casa)["status"] == "passed"


def test_compensated_residual_spike_requires_review_even_if_image_matches():
    native, casa = example_arrays()
    native["residual"][32, 32] += 6e-5
    result = assess_arrays(native, casa)
    assert result["status"] == "review_required"
    assert "residual_difference_max_over_casa_rms" in result["review_triggers"]


def test_distributed_residual_error_fails():
    native, casa = example_arrays()
    native["residual"] += 1e-5
    result = assess_arrays(native, casa)
    assert "residual_difference_rms_over_casa" in result["review_triggers"]


def test_restored_component_difference_uses_only_image_arithmetic():
    native, casa = example_arrays()
    native["image"] += 1e-5
    result = assess_arrays(native, casa)
    assert result["measurements"]["restored_component_difference_rms_over_casa"] > 0


def test_nonfinite_product_is_not_accepted():
    native, casa = example_arrays()
    native["image"][0, 0] = np.nan
    with pytest.raises(ValueError, match="nonfinite"):
        assess_arrays(native, casa)


def comparison_fixture():
    products = {suffix: {"status": "compared", "topology_parity": True,
                         "full_array": {"coverage_complete": True,
                                        "topology": {"mask_equal": True, "finite_equal": True,
                                                     "left_nonfinite": {"nan": 0, "positive_infinity": 0,
                                                                        "negative_infinity": 0},
                                                     "right_nonfinite": {"nan": 0, "positive_infinity": 0,
                                                                         "negative_infinity": 0}}}}
                for suffix in (".image", ".mask", ".model", ".pb", ".psf", ".residual", ".sumwt")}
    checks = [{"name": f"{suffix}.diff_rms_over_right_rms", "actual": 0, "ceiling": 0.001,
               "status": "passed"} for suffix in products]
    checks += [{"name": f".image.{metric}", "actual": 0, "ceiling": 0.001,
                "status": "passed"}
               for metric in ("beam_area_relative", "beam_kernel_nrmse")]
    return {"comparison_mode": "full", "product_inventory": {"status": "matched"},
            "products": products, "status": "completed",
            "require_direction_wcs_parity": True, "require_metadata_parity": True,
            "require_exact_product_inventory": True,
            "tolerance_evaluation": {"checks": checks}}


def beam_fixture(major=2.0):
    return {"major": {"value": major, "unit": "arcsec"},
            "minor": {"value": 1.5, "unit": "arcsec"},
            "positionangle": {"value": 0.0, "unit": "deg"}}


def strict_products_fixture():
    values = np.ones((4, 4), dtype=np.float64)
    mask = np.ones_like(values, dtype=bool)
    return {suffix: {side: (values.copy(), mask.copy()) for side in ("native", "casa")}
            for suffix in gate.STRICT_PLANE_PRODUCTS}


def test_single_plane_deterministic_product_failure_is_not_hidden_by_cube_average():
    products = strict_products_fixture()
    products[".psf"]["native"][0][0, 0] += 0.1
    result = gate.assess_strict_plane_products(
        products, {"native": beam_fixture(), "casa": beam_fixture()})
    assert result["status"] == "failed"
    assert ".psf.diff_rms_over_right_rms" in result["failed"]


def test_single_plane_restored_image_alert_requires_review():
    products = strict_products_fixture()
    products[".image"]["native"][0][0, 0] += 0.01
    result = gate.assess_strict_plane_products(
        products, {"native": beam_fixture(), "casa": beam_fixture()})
    assert result["status"] == "review_required"
    assert result["failed"] == []
    assert result["review_triggers"] == [".image.diff_rms_over_right_rms"]


def test_restored_image_full_cube_alert_is_not_ordinary_acceptance():
    comparison = comparison_fixture()
    comparison["status"] = "out_of_tolerance"
    image_check = next(check for check in comparison["tolerance_evaluation"]["checks"]
                       if check["name"] == ".image.diff_rms_over_right_rms")
    image_check.update(status="failed", actual=0.0011)
    _, triggers = strict_comparison_checks(comparison)
    assert triggers == [".image.diff_rms_over_right_rms"]


def test_single_plane_beam_failure_is_not_hidden_by_cube_average():
    result = gate.assess_strict_plane_products(
        strict_products_fixture(),
        {"native": beam_fixture(major=2.1), "casa": beam_fixture()})
    assert result["status"] == "failed"
    assert ".image.beam_area_relative" in result["failed"]


def test_only_raw_model_and_residual_rms_may_be_diagnostic():
    comparison = comparison_fixture()
    comparison["status"] = "out_of_tolerance"
    for check in comparison["tolerance_evaluation"]["checks"]:
        if check["name"] in {".model.diff_rms_over_right_rms", ".residual.diff_rms_over_right_rms"}:
            check["status"] = "failed"
    diagnostics, triggers = strict_comparison_checks(comparison)
    assert len(diagnostics) == 2 and not triggers
    next(check for check in comparison["tolerance_evaluation"]["checks"]
         if check["name"] == ".psf.diff_rms_over_right_rms")["status"] = "failed"
    with pytest.raises(ValueError, match="retained numerical"):
        strict_comparison_checks(comparison)


def test_inventory_and_coverage_remain_mandatory():
    comparison = comparison_fixture()
    comparison["product_inventory"]["status"] = "mismatched"
    with pytest.raises(ValueError, match="inventory"):
        strict_comparison_checks(comparison)
    comparison = comparison_fixture()
    comparison["products"][".model"]["full_array"]["coverage_complete"] = False
    with pytest.raises(ValueError, match="incomplete coverage"):
        strict_comparison_checks(comparison)
    comparison = comparison_fixture()
    comparison["products"][".model"]["full_array"]["topology"]["mask_equal"] = False
    with pytest.raises(ValueError, match="topology"):
        strict_comparison_checks(comparison)


def test_every_cube_channel_is_assessed_and_one_failure_fails_the_cube(
    tmp_path, monkeypatch
):
    comparison = comparison_fixture()
    comparison.update(left_prefix="native", right_prefix="casa")
    path = tmp_path / "result.json"
    path.write_text(json.dumps(comparison))
    native, casa = example_arrays()
    bad_native = copy.deepcopy(native)
    bad_native["residual"] += 1e-3
    monkeypatch.setattr(gate, "image_shape", lambda path: (64, 64, 1, 4))
    def read_masked(path, plane):
        suffix = path.rsplit(".", 1)[1]
        assert suffix != "model", "the comparison already assesses the model product"
        side = (bad_native if plane == 2 else native) if path.startswith("native") else casa
        values = side.get(suffix, np.ones((64, 64)))
        return values, np.ones((64, 64), dtype=bool)
    monkeypatch.setattr(gate, "read_masked_plane", read_masked)
    monkeypatch.setattr(gate, "read_restoring_beam", lambda prefix, plane: beam_fixture())
    result, identity = gate.assess_saved_comparison(path, 100)
    assert [row["channel"] for row in result] == [100, 101, 102, 103]
    assert [row["status"] for row in result] == ["passed", "passed", "failed", "passed"]
    assert len(identity["products_sha256"]) == 64


def review_group(channels, triggers):
    return {"channels": channels, "triggers": triggers,
            "reviewer": "owner-reviewed acceptance",
            "owner_decision": "accept_numerical_sensitivity",
            "owner_record": "https://github.com/bglenden/casa-rs/issues/541",
            "rationale": "bounded first-divergence and difference-map review",
            "evidence": ["https://github.com/bglenden/casa-rs/issues/541"],
            "visual_review": "reviewed difference panel"}


def test_review_is_bound_to_exact_comparison_and_policy():
    native, casa = example_arrays()
    native["residual"][32, 32] += 6e-5
    assessment = assess_arrays(native, casa)
    row = {"channel": 276, "failed": assessment["failed"],
           "review_triggers": assessment["review_triggers"], "status": assessment["status"]}
    identity = {"comparison_sha256": "a", "products_sha256": "b", "policy_sha256": "c",
                "global_review_triggers": []}
    comparison = [([row], identity)]
    review = {"rule": gate.RULE, "comparisons": [{"identity": identity,
                "reviews": [review_group([276], assessment["review_triggers"])]}]}
    gate.apply_review_manifest(comparison, review)
    assert row["status"] == "accepted_reviewed"
    for key in ("comparison_sha256", "products_sha256", "policy_sha256"):
        stale = copy.deepcopy(review)
        stale["comparisons"][0]["identity"][key] += "stale"
        with pytest.raises(ValueError, match="stale"):
            gate.apply_review_manifest(comparison, stale)


def test_review_product_identity_changes_with_one_array_value():
    values = np.ones((4, 4), dtype=np.float64)
    mask = np.ones_like(values, dtype=bool)
    first = hashlib.sha256()
    gate.hash_product_plane(first, 276, ".residual", "native", values, mask)
    changed = values.copy()
    changed[0, 0] += np.float32(0.01)
    second = hashlib.sha256()
    gate.hash_product_plane(second, 276, ".residual", "native", changed, mask)
    assert first.hexdigest() != second.hexdigest()


def test_review_cannot_override_hard_failure_or_unreviewed_channel():
    identity = {"comparison_sha256": "a", "products_sha256": "b", "policy_sha256": "c",
                "global_review_triggers": []}
    rows = [{"channel": 1, "failed": ["native_peak_residual_over_threshold"],
             "review_triggers": ["residual_difference_max_over_casa_rms"], "status": "failed"},
            {"channel": 2, "failed": [],
             "review_triggers": ["residual_difference_max_over_casa_rms"],
             "status": "review_required"}]
    review = {"rule": gate.RULE, "comparisons": [{"identity": identity,
                "reviews": [review_group([1], rows[0]["review_triggers"])]}]}
    with pytest.raises(ValueError, match="do not match"):
        gate.apply_review_manifest([(rows, identity)], review)
    review["comparisons"][0]["reviews"][0]["channels"] = [1, 2]
    with pytest.raises(ValueError, match="hard failure"):
        gate.apply_review_manifest([(rows, identity)], review)


def saved_review_fixture():
    identity = {"comparison_sha256": "a", "products_sha256": "b", "policy_sha256": "c",
                "global_review_triggers": []}
    report = {"rule": gate.RULE, "status": "review_required",
              "comparisons": [identity], "channels": [
                  {"channel": 1, "comparison": "comparison.json", "status": "passed",
                   "failed": [], "review_triggers": []},
                  {"channel": 2, "comparison": "comparison.json", "status": "review_required",
                   "failed": [], "review_triggers": ["residual_difference_max_over_casa_rms"]},
              ]}
    source = (json.dumps(report, sort_keys=True) + "\n").encode()
    manifest = {"rule": gate.RULE, "assessment_sha256": hashlib.sha256(source).hexdigest(),
                "comparisons": [{"identity": identity, "reviews": [
                    review_group([2], ["residual_difference_max_over_casa_rms"])]}]}
    return source, manifest


def test_review_finalization_uses_frozen_report_without_product_io(monkeypatch):
    source, manifest = saved_review_fixture()
    monkeypatch.setattr(gate, "read_masked_plane", lambda *args: pytest.fail("product reread"))
    reviewed = gate.finalize_saved_report(source, manifest)
    assert reviewed["status"] == "accepted_reviewed"
    assert [row["status"] for row in reviewed["channels"]] == ["passed", "accepted_reviewed"]
    assert reviewed["source_assessment_sha256"] == manifest["assessment_sha256"]


def test_review_finalization_rejects_stale_report_or_hard_failure():
    source, manifest = saved_review_fixture()
    with pytest.raises(ValueError, match="exact saved assessment"):
        gate.finalize_saved_report(source + b" ", manifest)
    report = json.loads(source)
    report["channels"][1]["failed"] = ["native_peak_residual_over_threshold"]
    hard_source = json.dumps(report).encode()
    manifest["assessment_sha256"] = hashlib.sha256(hard_source).hexdigest()
    with pytest.raises(ValueError, match="hard failure"):
        gate.finalize_saved_report(hard_source, manifest)


def test_review_cannot_trigger_a_second_product_pass(tmp_path, monkeypatch):
    manifest = tmp_path / "review.json"
    manifest.write_text("{}")
    monkeypatch.setattr("sys.argv", ["t55_c_array_clean_gate.py", "--comparison",
                                      str(tmp_path / "comparison.json"), "--first-channel", "0",
                                      "--review-manifest", str(manifest), "--output",
                                      str(tmp_path / "result.json")])
    monkeypatch.setattr(gate, "assess_saved_comparison",
                        lambda *args: pytest.fail("duplicate product pass"))
    with pytest.raises(ValueError, match="no second product pass"):
        gate.main()
