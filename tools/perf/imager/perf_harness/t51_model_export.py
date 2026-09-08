# SPDX-License-Identifier: LGPL-3.0-or-later
"""Bounded inspection of the diagnostic's authoritative Rust model export.

This checks binary32 serialization, not CASA's subsequent physical-model
normalization. A chunk consumer may create isolated starting images; it must
not initialize or execute imaging until the whole scan has returned success.
"""

import hashlib
import json
from pathlib import Path
import struct

import numpy as np


RECORD = np.dtype([("value", "<f8"), ("valid", "u1")])
MAX_CHUNK_RECORDS = 65536


def f64_bits(value):
    """Decode an exact exported coordinate, rejecting malformed/nonfinite bits."""
    if not isinstance(value, str) or len(value) != 16:
        raise ValueError("invalid F64 coordinate encoding")
    decoded, = struct.unpack(">d", bytes.fromhex(value))
    if not np.isfinite(decoded):
        raise ValueError("nonfinite model coordinate")
    return decoded


def inspect_export(manifest_path, *, expected_shape, absolute_budget, relative_budget,
                   consume=None, read_physical=None):
    """Stream every value/support bit and check the declared conversion budget.

    Budgets must come from the approved existing numerical contract. No extra
    input allowance is invented here. This return value alone cannot qualify
    a matched timing or replace the CASA post-conversion and residual checks.
    """
    if not all(np.isfinite(x) and x >= 0 for x in (absolute_budget, relative_budget)):
        raise ValueError("invalid existing conversion budgets")
    path = Path(manifest_path)
    manifest = json.loads(path.read_text())
    if (
        manifest["schema"] != "t51-authoritative-model-v1"
        or manifest["record_bytes"] != RECORD.itemsize
        or manifest["payload"] != "samples.f64-support.bin"
        or manifest["order"] != "domain, coefficient, polarization, y, x; x fastest"
        or manifest["coefficients"] != 2
        or manifest["polarizations"] != ["StokesI"]
        or manifest["basis"] != "Taylor { terms: 2 }"
        or len(manifest["domains"]) != 1
        or manifest["domains"][0]["pixels"] != list(expected_shape)
        or manifest["model_generation"] != manifest["normal_model_generation"]
    ):
        raise ValueError("model export differs from the declared T51 input")
    width, height = expected_shape
    if width <= 0 or height <= 0:
        raise ValueError("invalid model shape")
    count = width * height * 2
    payload_path = path.parent / manifest["payload"]
    if payload_path.stat().st_size != count * RECORD.itemsize:
        raise ValueError("model payload length differs from its shape")
    totals = dict(samples=0, valid_samples=0, nonzero_valid_samples=0,
                  float32_changed_samples=0, maximum_absolute_roundtrip_error=0.0,
                  maximum_relative_roundtrip_error=0.0)
    payload_hash = hashlib.sha256()
    values_hash = hashlib.sha256()
    physical_check = dict(samples=0, maximum_absolute_error=0.0,
                          maximum_relative_error=0.0)
    with payload_path.open("rb") as source:
        while block := source.read(MAX_CHUNK_RECORDS * RECORD.itemsize):
            if len(block) % RECORD.itemsize:
                raise ValueError("partial model sample record")
            payload_hash.update(block)
            records = np.frombuffer(block, dtype=RECORD)
            values = records["value"]
            valid = records["valid"]
            values_hash.update(values.tobytes())
            if (not np.isfinite(values).all() or (valid > 1).any()
                    or ((values == 0) & np.signbit(values)).any()
                    or (values[valid == 0] != 0).any()):
                raise ValueError("noncanonical model value or support")
            with np.errstate(over="ignore", invalid="ignore"):
                encoded = values.astype("<f4")
            if not np.isfinite(encoded).all():
                raise ValueError("model coefficient overflows CASA Float")
            errors = np.abs(encoded.astype("<f8") - values)
            if (errors > absolute_budget + relative_budget * np.abs(values)).any():
                raise ValueError("model encoding exceeds the existing numerical budget")
            nonzero = (valid == 1) & (values != 0)
            offset = totals["samples"]
            totals["samples"] += len(records)
            totals["valid_samples"] += int(np.count_nonzero(valid))
            totals["nonzero_valid_samples"] += int(np.count_nonzero(nonzero))
            totals["float32_changed_samples"] += int(np.count_nonzero(errors))
            totals["maximum_absolute_roundtrip_error"] = max(
                totals["maximum_absolute_roundtrip_error"], float(errors.max(initial=0)))
            relative = errors[nonzero] / np.abs(values[nonzero])
            totals["maximum_relative_roundtrip_error"] = max(
                totals["maximum_relative_roundtrip_error"], float(relative.max(initial=0)))
            if consume is not None:
                consume(offset, encoded, valid)
            if read_physical is not None:
                actual, actual_support = read_physical(offset, len(records))
                actual = np.asarray(actual)
                actual_support = np.asarray(actual_support)
                if (actual.shape != values.shape or actual_support.shape != valid.shape
                        or not np.isfinite(actual).all()
                        or not np.array_equal(actual_support, valid)):
                    raise ValueError("physical model shape, finiteness, or support mismatch")
                actual_errors = np.abs(actual.astype("<f8") - values)
                if ((actual[valid == 0] != 0).any()
                        or (actual_errors > absolute_budget + relative_budget * np.abs(values)).any()):
                    raise ValueError("physical model exceeds the existing numerical budget")
                physical_check["samples"] += len(records)
                physical_check["maximum_absolute_error"] = max(
                    physical_check["maximum_absolute_error"], float(actual_errors.max(initial=0)))
                physical_check["maximum_relative_error"] = max(
                    physical_check["maximum_relative_error"],
                    float((actual_errors[nonzero] / np.abs(values[nonzero])).max(initial=0)))
    totals["payload_sha256"] = payload_hash.hexdigest()
    totals["values_sha256"] = values_hash.hexdigest()
    if totals != manifest["summary"] or totals["samples"] != count:
        raise ValueError("model export summary/hash verification failed")
    if totals["nonzero_valid_samples"] == 0:
        raise ValueError("zero model is not a matched residual-refresh input")
    return {"manifest": manifest, "encoding_check": totals,
            "coordinates_verified": False,
            "post_casa_conversion_verified": read_physical is not None,
            "physical_model_check": physical_check if read_physical is not None else None}
