#!/usr/bin/env python3

from __future__ import annotations

import struct
import sys
import unittest
from pathlib import Path


EXPERIMENTS = Path(__file__).resolve().parent
sys.path.insert(0, str(EXPERIMENTS))

import vlass_aw_datatogrid_value_compare as subject  # noqa: E402


def receipts(source_count: int) -> tuple[dict, dict]:
    stream = {
        "contract": subject.STREAM_CONTRACT,
        "record_size": subject.RECORD_SIZE,
        "allocated_bytes": source_count * subject.RECORD_SIZE,
        "path": "/unused",
    }
    casa = {
        "schema": subject.CASA_SCHEMA,
        "status": "completed-before-grid",
        "source_count": source_count,
        "role_count": source_count * 2,
        "value_stream": stream,
    }
    casars = {
        "schema": subject.CASARS_SCHEMA,
        "casa_datatogrid_tt0_value_boundary": {
            "source_count": source_count,
            "role_count": source_count * 2,
            "value_stream": stream,
        },
    }
    return casa, casars


def payload(*records: tuple[float, ...]) -> bytes:
    return b"".join(
        subject.RECORD.pack(index, 0, 2, 0, *record)
        for index, record in enumerate(records)
    )


class ValueCompareTests(unittest.TestCase):
    def test_one_ulp_stream_passes(self) -> None:
        casa, casars = receipts(1)
        reference_value = 1.0
        reference_bits = struct.unpack("<I", struct.pack("<f", reference_value))[0]
        candidate_value = struct.unpack(
            "<f", (reference_bits + 1).to_bytes(4, "little")
        )[0]
        reference = payload((reference_value,) * 14)
        candidate = payload((candidate_value,) * 14)

        result = subject.compare(casa, casars, reference, candidate)

        self.assertTrue(result["passed"])
        self.assertLess(result["grid_residual"]["normalized_rms"], 1.0e-3)
        self.assertLess(result["weighted_tt0_value"]["normalized_rms"], 1.0e-3)
        self.assertLess(result["grid_residual"]["exact_component_fraction"], 1.0)

    def test_material_numerical_divergence_fails(self) -> None:
        casa, casars = receipts(1)
        reference = payload((1.0,) * 14)
        candidate = payload((1.01,) * 14)

        result = subject.compare(casa, casars, reference, candidate)

        self.assertFalse(result["passed"])
        self.assertEqual(
            result["classification"],
            "residual-value-stream-numerical-divergence",
        )

    def test_topology_mismatch_fails(self) -> None:
        casa, casars = receipts(2)
        reference = payload((1.0,) * 14, (2.0,) * 14)
        candidate = payload((1.0,) * 14)

        result = subject.compare(casa, casars, reference, candidate)

        self.assertFalse(result["passed"])
        self.assertEqual(
            result["classification"],
            "residual-value-stream-topology-failure",
        )

    def test_nonfinite_fails(self) -> None:
        casa, casars = receipts(1)
        reference = payload((1.0,) * 14)
        candidate = payload((float("nan"),) + (1.0,) * 13)

        result = subject.compare(casa, casars, reference, candidate)

        self.assertFalse(result["passed"])
        self.assertEqual(result["classification"], "residual-value-stream-nonfinite")


if __name__ == "__main__":
    unittest.main()
