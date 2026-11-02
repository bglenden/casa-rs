# SPDX-License-Identifier: LGPL-3.0-or-later
"""Experimental measurements only; this module defines no acceptance policy.

For two-term Stokes-I MFS mosaic/AW published weights, reconstruct G0=S0*W0,
G1=W1, G2=W2. CASA's base divideWeightBySumwt normalizes only term zero
(see casa-imaging-products/src/taylor.rs and docs/CASA (C++) bugs.md).
The caller supplies aligned, nonoverlapping flat chunks and reference PB support.
All L2 sums use the full valid mask; pointwise maxima use reference PB support.
No arrays survive add(). Inputs must be NumPy arrays so size validation precedes
conversion/copies. At most 65,536 cells are accepted per chunk; float64 scratch
is conservatively below 16 MiB, excluding caller-owned input arrays.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

import numpy as np


MAX_CHUNK_CELLS = 65_536


def _sum_weight(value: object) -> float:
    array = np.asarray(value)
    if array.shape != () or array.dtype.kind not in "fiu":
        raise ValueError("S0 must be a real scalar")
    result = float(array)
    if not math.isfinite(result) or result <= 1e-7:
        raise ValueError("S0 must be finite and strictly greater than 1e-7")
    return result


def _finite(value: float) -> float:
    if not math.isfinite(value):
        raise ValueError("nonfinite intermediate, reduction, or output")
    return value


def _root_ratio(numerator: float, denominator: float) -> float:
    # Divide after taking roots: the squared ratio can underflow or overflow
    # even when its square root is a representable float64 measurement.
    result = _finite(math.sqrt(numerator) / math.sqrt(denominator))
    if numerator > 0 and result == 0:
        raise ValueError("positive root ratio underflowed to zero")
    return result


class TaylorMomentAccumulator:
    """Bounded diagnostic reducer, without pass/fail thresholds or gate wiring.

    All six masks must match exactly. Payload under false masks is ignored;
    reference PB support must be a subset of that common valid mask. Both moment
    families must be finite, have nonnegative diagonals, and obey Cauchy/PSD
    without tolerance. A zero diagonal scale in either family requires both
    odd moments to be zero at that pixel, including outside PB support.
    """

    def __init__(self, reference_s0: object, candidate_s0: object) -> None:
        self.reference_s0 = _sum_weight(reference_s0)
        self.candidate_s0 = _sum_weight(candidate_s0)
        self.cells = self.valid_cells = self.pb_cells = 0
        self.positive_scale_pb_cells = self.chunks = 0
        self.max_chunk_cells = 0
        self.scale_sum = self.error_sum = self.odd_sum = 0.0
        self.error_max = self.rho_max = 0.0

    def add(
        self,
        reference: Sequence[np.ndarray],
        candidate: Sequence[np.ndarray],
        reference_masks: Sequence[np.ndarray],
        candidate_masks: Sequence[np.ndarray],
        reference_pb_support: np.ndarray,
    ) -> None:
        """Add one aligned chunk; invalid input leaves scalar state unchanged."""
        families = (reference, candidate, reference_masks, candidate_masks)
        if any(len(family) != 3 for family in families):
            raise ValueError("exactly three moments and masks per family required")
        arrays = [array for family in families for array in family]
        arrays.append(reference_pb_support)
        # Inspect every size before any conversion, indexed copy, or arithmetic.
        if any(not isinstance(a, np.ndarray) or a.ndim != 1 for a in arrays):
            raise ValueError("all chunks must be one-dimensional NumPy arrays")
        size = arrays[0].size
        if any(a.size > MAX_CHUNK_CELLS for a in arrays):
            raise ValueError("chunk exceeds 65536 cells")
        if any(a.shape != arrays[0].shape for a in arrays):
            raise ValueError("chunk shapes must match exactly")
        masks = [*reference_masks, *candidate_masks]
        if any(a.dtype != np.dtype(bool) for a in [*masks, reference_pb_support]):
            raise ValueError("masks and PB support must have boolean dtype")
        valid = masks[0]
        if any(not np.array_equal(valid, a) for a in masks[1:]):
            raise ValueError("all cross-moment and cross-family masks must match")
        if np.any(reference_pb_support & ~valid):
            raise ValueError("PB support must be a subset of valid pixels")
        if any(a.dtype.kind not in "fiu" for a in [*reference, *candidate]):
            raise ValueError("moments must be real numeric arrays")
        try:
            with np.errstate(over="raise", invalid="raise", divide="raise"):
                values = self._measure(reference, candidate, valid, reference_pb_support)
                totals = [
                    _finite(self.scale_sum + values[0]),
                    _finite(self.error_sum + values[1]),
                    _finite(self.odd_sum + values[2]),
                ]
        except FloatingPointError as error:
            raise ValueError("nonfinite intermediate or reduction") from error
        self.scale_sum, self.error_sum, self.odd_sum = totals
        self.error_max = max(self.error_max, values[3])
        self.rho_max = max(self.rho_max, values[4])
        self.positive_scale_pb_cells += values[5]
        self.cells += size
        self.valid_cells += int(np.count_nonzero(valid))
        self.pb_cells += int(np.count_nonzero(reference_pb_support))
        self.chunks += 1
        self.max_chunk_cells = max(self.max_chunk_cells, size)

    def _measure(self, reference, candidate, valid, pb):
        left = [a[valid].astype(np.float64, copy=False) for a in reference]
        right = [a[valid].astype(np.float64, copy=False) for a in candidate]
        left[0] *= self.reference_s0
        right[0] *= self.candidate_s0
        scales = []
        for family in (left, right):
            if any(not np.all(np.isfinite(a)) for a in family):
                raise ValueError("valid moment payload must be finite")
            if np.any(family[0] < 0) or np.any(family[2] < 0):
                raise ValueError("each even moment must be nonnegative")
            scale = np.sqrt(family[0] * family[2])
            if np.any(np.abs(family[1]) > scale):
                raise ValueError("moment family violates PSD/Cauchy")
            scales.append(scale)
        zero = (scales[0] == 0) | (scales[1] == 0)
        if np.any((left[1][zero] != 0) | (right[1][zero] != 0)):
            raise ValueError("zero diagonal scale requires both odd moments zero")
        difference = right[1] - left[1]
        support = pb[valid] & (scales[0] > 0)
        error_max = rho_max = 0.0
        if np.any(support):
            error_max = float(np.max(np.abs(difference[support]) / scales[0][support]))
            rho_max = float(np.max(np.abs(left[1][support]) / scales[0][support]))
        values = [
            float(np.sum(left[0] * left[2], dtype=np.float64)),
            float(np.sum(difference * difference, dtype=np.float64)),
            float(np.sum(left[1] * left[1], dtype=np.float64)),
            error_max,
            rho_max,
        ]
        if (values[1] == 0 and np.any(difference != 0)) or (
            values[2] == 0 and np.any(left[1] != 0)
        ):
            raise ValueError("squared-moment reduction underflowed to zero")
        return (*[_finite(value) for value in values], int(np.count_nonzero(support)))

    def result(self) -> dict[str, float | int | None]:
        """Return measurements; reject empty or meaningless normalization domains."""
        if not self.valid_cells or not self.positive_scale_pb_cells or self.scale_sum <= 0:
            raise ValueError("need valid pixels, positive-scale PB support and aggregate scale")
        return {
            "e2": _root_ratio(self.error_sum, self.scale_sum),
            "emax": self.error_max,
            "rho_l2": _root_ratio(self.odd_sum, self.scale_sum),
            "rho_pmax": self.rho_max,
            "raw_nrms": (_root_ratio(self.error_sum, self.odd_sum)
                         if self.odd_sum > 0 else None),
            "cells": self.cells,
            "valid_cells": self.valid_cells,
            "pb_cells": self.pb_cells,
            "positive_scale_pb_cells": self.positive_scale_pb_cells,
            "chunks": self.chunks,
            "max_chunk_cells": self.max_chunk_cells,
        }
