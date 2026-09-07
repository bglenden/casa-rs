# SPDX-License-Identifier: LGPL-3.0-or-later
"""Opt-in serial CASA major-cycle envelopes; neither envelope is a tap kernel."""

from contextlib import contextmanager
import json
import os
import time


@contextmanager
def trace_major_cycles(imager_type=None):
    """Observe the real Python helper without changing task arguments or results.

    The outer envelope includes model normalization, the core call, iteration
    bookkeeping and residual normalization. The nested core includes the C++
    major cycle, not just gridding/degridding. Records are emitted after each
    outer invocation, including an incomplete record when it raises. This is
    a serial, process-local diagnostic; missing records do not prove zero work.
    """
    if "CASA_RS_TRACE_MAJOR_CYCLE_ENVELOPES" not in os.environ:
        yield
        return
    if imager_type is None:
        from casatasks.private.imagerhelpers.imager_base import PySynthesisImager

        imager_type = PySynthesisImager
    original_major = imager_type.runMajorCycle
    original_core = imager_type.runMajorCycleCore
    ordinal = 0
    active = None

    def run_major(self, isCleanCycle=True):
        nonlocal ordinal, active
        ordinal += 1
        record = {
            "ordinal": ordinal,
            "is_clean_cycle": isCleanCycle,
            "core_calls": 0,
            "core_seconds": 0.0,
            "completed": False,
        }
        active = record
        started = time.perf_counter()
        try:
            result = original_major(self, isCleanCycle=isCleanCycle)
            record["completed"] = True
            return result
        finally:
            record["major_cycle_seconds"] = time.perf_counter() - started
            record["outside_core_seconds"] = (
                record["major_cycle_seconds"] - record["core_seconds"]
            )
            active = None
            print("casa_major_cycle_envelope " + json.dumps(record, sort_keys=True), flush=True)

    def run_core(self, lastcycle):
        started = time.perf_counter()
        try:
            return original_core(self, lastcycle)
        finally:
            elapsed = time.perf_counter() - started
            if active is not None:
                active["core_calls"] += 1
                active["core_seconds"] += elapsed

    imager_type.runMajorCycle = run_major
    imager_type.runMajorCycleCore = run_core
    try:
        yield
    finally:
        imager_type.runMajorCycle = original_major
        imager_type.runMajorCycleCore = original_core
