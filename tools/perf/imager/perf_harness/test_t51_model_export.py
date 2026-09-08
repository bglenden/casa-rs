# SPDX-License-Identifier: LGPL-3.0-or-later
"""Exact serialization tests; no CASA runtime or imaging workload is used."""

import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch

from .t51_model_export import f64_bits, inspect_export


class ModelExportTests(unittest.TestCase):
    def fixture(self, directory, values=(1.0, 0.1, -0.2, 0.0)):
        support = (1, 1, 1, 0)
        payload = b"".join(struct.pack("<dB", value, valid)
                           for value, valid in zip(values, support))
        encoded = [struct.unpack("<f", struct.pack("<f", value))[0] for value in values]
        errors = [abs(q - value) for q, value in zip(encoded, values)]
        summary = dict(
            samples=4, valid_samples=3,
            nonzero_valid_samples=sum(value != 0 for value in values),
            float32_changed_samples=sum(error != 0 for error in errors),
            maximum_absolute_roundtrip_error=max(errors),
            maximum_relative_roundtrip_error=max(
                (error / abs(value) for error, value in zip(errors, values) if value), default=0),
            payload_sha256=hashlib.sha256(payload).hexdigest(),
            values_sha256=hashlib.sha256(b"".join(struct.pack("<d", v) for v in values)).hexdigest(),
        )
        manifest = dict(
            schema="t51-authoritative-model-v1", record_bytes=9,
            payload="samples.f64-support.bin",
            order="domain, coefficient, polarization, y, x; x fastest",
            coefficients=2, polarizations=["StokesI"], basis="Taylor { terms: 2 }",
            domains=[dict(pixels=[2, 1])], model_generation="same",
            normal_model_generation="same", summary=summary,
        )
        path = Path(directory) / "manifest.json"
        path.write_text(json.dumps(manifest))
        (path.parent / manifest["payload"]).write_bytes(payload)
        return path

    def inspect(self, path, **kwargs):
        return inspect_export(path, expected_shape=(2, 1),
                              absolute_budget=1e-7, relative_budget=1e-3, **kwargs)

    def test_streaming_preserves_values_support_and_reports_unverified_casa_conversion(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.fixture(directory)
            chunks = []
            with patch("perf_harness.t51_model_export.MAX_CHUNK_RECORDS", 2):
                result = self.inspect(path, consume=lambda offset, values, valid:
                                      chunks.append((offset, values.copy(), valid.copy())))
            self.assertEqual([chunk[0] for chunk in chunks], [0, 2])
            self.assertEqual(chunks[1][2].tolist(), [1, 0])
            self.assertEqual(result["encoding_check"]["float32_changed_samples"], 2)
            self.assertFalse(result["coordinates_verified"])
            self.assertFalse(result["post_casa_conversion_verified"])

    def test_corruption_shape_and_zero_model_fail(self):
        for mutation in ("truncate", "corrupt", "generation", "zero"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                path = self.fixture(directory, (0.0,) * 4) if mutation == "zero" else self.fixture(directory)
                payload = path.parent / "samples.f64-support.bin"
                if mutation == "truncate":
                    payload.write_bytes(payload.read_bytes()[:-1])
                elif mutation == "corrupt":
                    data = bytearray(payload.read_bytes())
                    data[0] ^= 1
                    payload.write_bytes(data)
                elif mutation == "generation":
                    manifest = json.loads(path.read_text())
                    manifest["normal_model_generation"] = "foreign"
                    path.write_text(json.dumps(manifest))
                with self.assertRaises(ValueError):
                    self.inspect(path)

    def test_exact_budget_rejects_inexact_float_encoding(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.fixture(directory)
            with self.assertRaisesRegex(ValueError, "exceeds"):
                inspect_export(path, expected_shape=(2, 1), absolute_budget=0, relative_budget=0)

    def test_physical_readback_checks_all_values_and_support(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.fixture(directory)
            actual = [1.0, 0.1, -0.2, 0.0]
            support = [1, 1, 1, 0]
            def read(offset, count):
                return actual[offset:offset + count], support[offset:offset + count]
            result = self.inspect(path, read_physical=read)
            self.assertTrue(result["post_casa_conversion_verified"])
            self.assertEqual(result["physical_model_check"]["samples"], 4)
            actual[1] = 0.2
            with self.assertRaisesRegex(ValueError, "physical model exceeds"):
                self.inspect(path, read_physical=read)
            actual[1] = 0.1
            support[3] = 1
            with self.assertRaisesRegex(ValueError, "support mismatch"):
                self.inspect(path, read_physical=read)

    def test_coordinate_bits_are_exact_and_nonfinite_rejected(self):
        self.assertEqual(f64_bits("3fb999999999999a"), 0.1)
        for value in ("nan", "7ff0000000000000", "g" * 16):
            with self.subTest(value=value), self.assertRaises(ValueError):
                f64_bits(value)
