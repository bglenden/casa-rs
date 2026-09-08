# SPDX-License-Identifier: LGPL-3.0-or-later
"""One diagnostic residual refresh through CASA's unchanged major-cycle owner.

Starting images contain physical coefficients. CASA ordinarily treats them as
apparent coefficients under flatnoise. Its own normalizer prepares that input
before the observed call; the check sees the actual physical prediction model
after the native divide/scatter and before any prediction occurs.
"""

from contextlib import contextmanager
import time


@contextmanager
def fixed_model_cycle(imager_type, *, check_physical_model, receipt):
    """Observe exactly one serial MFS major cycle, failing before prediction.

    The two outer intervals are disjoint measured intervals, not estimated
    subtractions. Model preparation and validation remain charged to the outer
    run allowance. This is diagnostic instrumentation, not a tclean protocol
    override or an alternative implementation of normalization.
    """
    original_major = imager_type.runMajorCycle
    original_core = imager_type.runMajorCycleCore
    active = None
    calls = 0

    def run_major(self, isCleanCycle=True):
        nonlocal active, calls
        calls += 1
        if calls != 1 or active is not None or isCleanCycle:
            raise ValueError("expected one non-cleaning fixed-model major cycle")
        if (self.NF != 1 or self.IBtool is not None
                or self.allimpars["0"]["specmode"] != "mfs"
                or self.allimpars["0"]["deconvolver"] != "mtmfs"):
            raise ValueError("fixed-model diagnostic requires serial MFS MT-MFS without a minor cycle")
        receipt.update(completed=False, core_calls=0)
        preparation_start = time.perf_counter()
        self.PStools[0].multiplymodelbyweight()
        receipt["input_preparation_seconds"] = time.perf_counter() - preparation_start
        active = self
        receipt["before_validation_seconds"] = 0.0
        receipt["after_validation_seconds"] = 0.0
        receipt["validation_completed"] = False
        receipt["interval_start"] = time.perf_counter()
        try:
            result = original_major(self, isCleanCycle=False)
            if receipt["core_calls"] != 1 or not receipt["validation_completed"]:
                raise ValueError("native major cycle bypassed physical-model verification")
            receipt["completed"] = True
            return result
        finally:
            ended = time.perf_counter()
            if "after_validation_start" in receipt:
                receipt["after_validation_seconds"] = ended - receipt.pop("after_validation_start")
            receipt.pop("interval_start")
            receipt["normalized_refresh_seconds"] = (
                receipt["before_validation_seconds"] + receipt["after_validation_seconds"]
            )
            active = None

    def run_core(self, lastcycle):
        if active is not self or receipt["core_calls"] != 0 or not lastcycle:
            raise ValueError("unexpected fixed-model core call")
        receipt["core_calls"] += 1
        validation_start = time.perf_counter()
        receipt["before_validation_seconds"] = validation_start - receipt["interval_start"]
        try:
            # Must raise on any model, support, coordinate, or provenance mismatch.
            receipt["physical_model_check"] = check_physical_model(self)
            receipt["validation_completed"] = True
        finally:
            receipt["validation_seconds"] = time.perf_counter() - validation_start
        receipt["after_validation_start"] = time.perf_counter()
        core_start = time.perf_counter()
        try:
            return original_core(self, lastcycle=lastcycle)
        finally:
            receipt["core_seconds"] = time.perf_counter() - core_start

    imager_type.runMajorCycle = run_major
    imager_type.runMajorCycleCore = run_core
    try:
        yield
        if calls != 1 or not receipt.get("completed", False):
            raise ValueError("fixed-model refresh did not complete exactly once")
    finally:
        imager_type.runMajorCycle = original_major
        imager_type.runMajorCycleCore = original_core
