# SPDX-License-Identifier: LGPL-3.0-or-later
"""Boundary and veto tests for the approved tclean-only replacement."""
import copy
import importlib.util
from pathlib import Path

import numpy as np
import pytest

from perf_harness.taylor_moment_comparison import TaylorMomentAccumulator
from perf_harness.taylor_weight_gate import apply_to_run, evaluate

PARAMETERS = dict(specmode="mfs", stokes="I", nterms=2, gridder="mosaic", pblimit=0.2)


def metrics(rho=1e-3, error=0.0, pb=None):
    rho = np.atleast_1d(rho).astype(float)
    error = np.broadcast_to(error, rho.shape)
    ones = np.ones(rho.size)
    mask = np.ones(rho.size, dtype=bool)
    acc = TaylorMomentAccumulator(1., 1.)
    acc.add([ones, rho, ones], [ones, rho + error, ones], [mask]*3, [mask]*3,
            mask if pb is None else np.array(pb, dtype=bool))
    return acc.result()


@pytest.mark.parametrize("rho,eligible", [(1e-3, True), (np.nextafter(1e-3, np.inf), False)])
def test_global_weak_boundary(rho, eligible):
    measured = metrics(rho)
    gate = evaluate(PARAMETERS, False, lambda: measured)
    assert gate["eligible"] is eligible
    assert gate["passed"] is eligible


@pytest.mark.parametrize("rho,eligible", [(1e-3, True), (np.nextafter(1e-3, np.inf), False)])
def test_pb_weak_boundary_despite_diluted_global_coupling(rho, eligible):
    gate = evaluate(PARAMETERS, False, lambda: metrics([rho, 0.0]))
    assert gate["metrics"]["rho_l2"] < 1e-3
    assert gate["eligible"] is eligible


@pytest.mark.parametrize("key", ["e2", "emax"])
@pytest.mark.parametrize("value,passed", [(1e-6, True), (np.nextafter(1e-6, np.inf), False)])
def test_error_boundaries(key, value, passed):
    measured = metrics(0.)
    measured[key] = value
    assert evaluate(PARAMETERS, False, lambda: measured)["passed"] is passed


def test_localized_error_passes_l2_but_fails_pointwise():
    error = np.zeros(100)
    error[0] = 5e-6
    gate = evaluate(PARAMETERS, True, lambda: metrics(np.zeros(100), error))
    assert gate["metrics"]["e2"] < 1e-6
    assert not gate["passed"]


def test_global_error_includes_pixels_outside_pb():
    gate = evaluate(PARAMETERS, True, lambda: metrics([0., 0.], [0., 5e-6], [True, False]))
    assert gate["metrics"]["emax"] == 0
    assert not gate["passed"]


@pytest.mark.parametrize("key,value", [("specmode", "cube"), ("stokes", "IQ"),
                                       ("nterms", 3), ("nterms", True), ("gridder", "standard")])
@pytest.mark.parametrize("raw", [False, True])
def test_other_modes_retain_original_comparison_without_measurement(key, value, raw):
    def must_not_measure():
        raise AssertionError("out-of-scope measurement")
    gate = evaluate(dict(PARAMETERS, **{key: value}), raw, must_not_measure)
    assert gate["passed"] is raw
    assert not gate["override"]


def test_strong_reference_preserves_raw_pass_despite_matrix_error():
    assert evaluate(PARAMETERS, True, lambda: metrics(.1, .01))["passed"]
    assert not evaluate(PARAMETERS, False, lambda: metrics(.1, .01))["passed"]


@pytest.mark.parametrize("limit", [None, 0, 1, float("nan"), float("inf"), True, "0.2"])
def test_invalid_recorded_pblimit_cannot_override(limit):
    gate = evaluate(dict(PARAMETERS, pblimit=limit), False, lambda: metrics(0.))
    assert not gate["passed"] and not gate["override"]
    assert gate["reason"] == "guard failure"


def test_negative_recorded_limit_uses_absolute_value():
    assert evaluate(dict(PARAMETERS, pblimit=-.2), False, lambda: metrics(0.))["passed"]


@pytest.mark.parametrize("invalid", ["mask", "psd", "s0", "support"])
def test_actual_measurement_guards_cannot_override(invalid):
    def measure():
        acc = TaylorMomentAccumulator(0. if invalid == "s0" else 1., 1.)
        one = np.ones(2)
        zero = np.zeros(2)
        mask = np.ones(2, dtype=bool)
        wrong = np.array([True, False])
        acc.add([one, zero, one], [one, one * 2 if invalid == "psd" else zero, one],
                [wrong if invalid == "support" else mask]*3,
                [wrong if invalid in ("mask", "support") else mask]*3, mask)
        return acc.result()
    gate = evaluate(PARAMETERS, False, measure)
    assert gate["reason"] == "guard failure"
    assert not gate["passed"] and not gate["override"]


def test_run_keeps_unrelated_failure_and_raw_record_immutable():
    product = dict(shape_match=True, mask_mismatch=0, valid_finite=True,
                   beam_within_limits=True, passed_recorded_checks=False)
    run = dict(products={"image.weight.tt1": product}, failures=["image.weight.tt1", "metadata"],
               exit_code=0, inventory_match=True, passed=False)
    before = copy.deepcopy(run)
    gate = evaluate(PARAMETERS, False, lambda: metrics(0.))
    result = apply_to_run(run, gate)
    assert not result["passed_recorded_checks"] and result["failures"] == ["metadata"]
    assert run == before
    run["failures"] = ["image.weight.tt1"]
    assert apply_to_run(run, gate)["passed_recorded_checks"]
    run["products"]["image.weight.tt1"]["mask_mismatch"] = 1
    assert not apply_to_run(run, gate)["passed_recorded_checks"]


def reader_module():
    path = Path(__file__).resolve().parents[3] / "casa-compat/check_tclean_taylor_weights.py"
    spec = importlib.util.spec_from_file_location("taylor_reader", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeCoords:
    def done(self): pass
    def names(self): return ["Right Ascension", "Declination", "Stokes", "Frequency"]
    def stokes(self): return ["I"]
    def torecord(self): return {"direction": np.array([1., 2.]), "spectral2": {"wcs": {"ctype": "FREQ"}, "restfreq": 1.0}}


class FakeImage:
    reads = []
    weight_shape = (70000, 1, 1, 1)
    def open(self, path): self.path = path; return True
    def done(self): pass
    def shape(self): return (1, 1, 1, 1) if "sumwt" in self.path else self.weight_shape
    def coordsys(self): return FakeCoords()
    def getchunk(self, blc=None, trc=None, getmask=False):
        size = 1 if blc is None else trc[0] - blc[0] + 1
        self.reads.append(size)
        assert size <= 65536
        value = 0. if "weight.tt1" in self.path else 1.
        return np.full(size, True if getmask else value, dtype=bool if getmask else float)


def test_streamed_reader_bounds_and_unsupported_shape():
    reader = reader_module()
    FakeImage.reads = []
    measured = reader.measure_images(Path("reference"), Path("native"), PARAMETERS, FakeImage)
    assert measured["cells"] == 70000 and measured["chunks"] == 2
    assert measured["e2"] == 0 and max(FakeImage.reads) == 65536
    class BadImage(FakeImage): weight_shape = (2, 2, 2, 1)
    gate = evaluate(PARAMETERS, False,
                    lambda: reader.measure_images(Path("r"), Path("n"), PARAMETERS, BadImage))
    assert not gate["passed"] and gate["reason"] == "guard failure"


@pytest.mark.parametrize("fault", ["coordinates", "nonfinite_pb", "mask", "sumwt_mask"])
def test_reader_rejects_alignment_and_payload_guards(fault):
    reader = reader_module()
    class FaultImage(FakeImage):
        weight_shape = (2, 1, 1, 1)
        def coordsys(self):
            coords = FakeCoords()
            if fault == "coordinates" and "native" in self.path:
                original = coords.torecord
                coords.torecord = lambda: dict(original(), direction=np.array([1.001, 2.]))
            return coords
        def getchunk(self, **kwargs):
            values = super().getchunk(**kwargs)
            if fault == "nonfinite_pb" and "pb.tt0" in self.path and not kwargs.get("getmask"):
                values[0] = np.nan
            if kwargs.get("getmask") and ((fault == "mask" and "native" in self.path)
                                         or (fault == "sumwt_mask" and "sumwt" in self.path)):
                values[0] = False
            return values
    gate = evaluate(PARAMETERS, False,
                    lambda: reader.measure_images(Path("reference"), Path("native"), PARAMETERS, FaultImage))
    assert not gate["passed"] and not gate["override"]
    assert gate["reason"] == "guard failure"


def test_reader_reference_pb_support_ignores_candidate_and_uses_inclusive_limit():
    reader = reader_module()
    class PBImage(FakeImage):
        weight_shape = (2, 1, 1, 1)
        def open(self, path):
            assert not ("native" in path and "pb.tt0" in path)
            return super().open(path)
        def getchunk(self, **kwargs):
            values = super().getchunk(**kwargs)
            if not kwargs.get("getmask") and "pb.tt0" in self.path:
                values[:] = [.2, np.nextafter(.2, 0)]
            return values
    result = reader.measure_images(Path("reference"), Path("native"), PARAMETERS, PBImage)
    assert result["valid_cells"] == 2 and result["pb_cells"] == 1


def test_global_eligibility_boundary_is_required_independently_of_pb():
    # A high coupling outside the reference PB must veto the replacement.
    measured = metrics([.002, 0.], pb=[False, True])
    assert measured["rho_pmax"] == 0 and measured["rho_l2"] > 1e-3
    gate = evaluate(PARAMETERS, False, lambda: measured)
    assert not gate["eligible"] and not gate["passed"]


def test_rest_frequency_discrepancy_is_reported_not_accepted_as_metadata():
    reader = reader_module()
    class RestImage(FakeImage):
        weight_shape = (2, 1, 1, 1)
        def coordsys(self):
            coords = FakeCoords()
            original = coords.torecord
            def record():
                value = original()
                value["spectral2"]["restfreq"] = 2. if "native" in self.path else 1.
                return value
            coords.torecord = record
            return coords
    result = reader.measure_images(Path("reference"), Path("native"), PARAMETERS, RestImage)
    assert result["metadata_assessment"]["observed_restfreq_mismatch"]
    assert result["metadata_assessment"]["full_metadata"].startswith("unassessed")


@pytest.mark.parametrize("bad", [float("inf"), float("-inf"), float("nan")])
@pytest.mark.parametrize("location", ["array", "nested_scalar"])
def test_matching_nonfinite_coordinates_cannot_override(bad, location):
    # Matching +/- infinity would satisfy allclose/isclose; equality alone is
    # insufficient even when reference, candidate and PB have identical WCS.
    reader = reader_module()
    class NonfiniteImage(FakeImage):
        weight_shape = (2, 1, 1, 1)
        def coordsys(self):
            coords = FakeCoords()
            original = coords.torecord
            def record():
                value = original()
                if location == "array":
                    value["direction"][0] = bad
                else:
                    value["spectral2"]["wcs"]["crval"] = bad
                return value
            coords.torecord = record
            return coords
    gate = evaluate(PARAMETERS, False,
                    lambda: reader.measure_images(Path("reference"), Path("native"),
                                                  PARAMETERS, NonfiniteImage))
    assert gate["reason"] == "guard failure"
    assert gate["guard_error"] == "coordinate metadata must be finite"
    assert not gate["passed"] and not gate["override"]
