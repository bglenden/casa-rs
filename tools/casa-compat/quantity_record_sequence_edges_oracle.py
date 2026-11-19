#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Capture CASA quantity records for the remaining #1170 cases."""

import argparse
import json
from pathlib import Path

import numpy as np
from casaconfig import config

config.data_auto_update = config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402


def decode(value):
    if isinstance(value, list):
        return [decode(item) for item in value]
    if isinstance(value, dict):
        if "tuple" in value:
            return tuple(decode(item) for item in value["tuple"])
        if "array" in value:
            return np.array(value["array"], dtype=value["dtype"])
        if "complex" in value:
            return complex(*value["complex"])
        return np.dtype(value["dtype"]).type(value["scalar"])
    return value


def numpy_scalar(dtype, value):
    return {"dtype": dtype, "scalar": value}


CASES = [
    ("uint8_only", [numpy_scalar("uint8", 255)], "m", None),
    ("uint64_only", [numpy_scalar("uint64", 2**63)], "m", None),
    ("unsigned_only_vector", [numpy_scalar("uint32", 2), numpy_scalar("uint16", 3)], "m", None),
    ("integer_then_unsigned", [1, numpy_scalar("uint64", 2)], "m", "discarded_value"),
    ("unsigned_then_integer", [numpy_scalar("uint64", 2), 1], "m", "discarded_value"),
    ("float_unsigned_tuple", {"tuple": [1.5, numpy_scalar("uint16", 2)]}, "m", "discarded_value"),
    ("float_unsigned_float", [1.5, numpy_scalar("uint32", 2), 3.5], "m", "discarded_value"),
    ("unsigned_nested", [[numpy_scalar("uint64", 1)]], "m", None),
    ("numpy_boolean_float", [numpy_scalar("bool", True), 2.5], "m", "discarded_value"),
    ("boolean_integer", [True, 1], "m", None),
    ("integer_boolean", [1, False], "m", None),
    ("signed_numpy_boolean", [numpy_scalar("int32", 2), False], "m", None),
    ("boolean_float", [True, 2.5], "m", None),
    ("float_boolean_tuple", {"tuple": [2.5, False]}, "m", None),
    ("boolean_numpy_float", [True, numpy_scalar("float32", 2.5)], "m", None),
    ("boolean_numpy_half", [True, numpy_scalar("float16", 2.5)], "m", None),
    ("boolean_numpy_longdouble", [True, numpy_scalar("longdouble", 2.5)], "m", None),
    ("boolean_float_matrix", [[True, 2.5], [1.5, False]], "m", None),
    ("boolean_integer_matrix", [[True, 1]], "m", None),
    ("boolean_complex", [True, {"complex": [2, 3]}], "m", None),
    ("boolean_only", [True, False], "m", None),
    ("booleans_and_invalid_unit", [True, 2.5], "not_a_unit", None),
    ("integer_boolean_invalid_unit", [1, True], "not_a_unit", None),
    ("unsigned_only_invalid_unit", [numpy_scalar("uint64", 1)], "not_a_unit", None),
    ("scalar_overflow", 2**63, "m", "overflow"),
    ("scalar_underflow", -(2**63) - 1, "m", "overflow"),
    ("list_overflow", [2**63], "m", "overflow"),
    ("tuple_underflow", {"tuple": [-(2**63) - 1]}, "m", "overflow"),
    ("nested_overflow", [[2**63]], "m", "overflow"),
    ("arbitrary_precision_overflow", 10**400, "m", "overflow"),
    ("mixed_float_overflow", [True, 2.5, 2**63], "m", "mixed_overflow"),
    ("scalar_max_control", 2**63 - 1, "m", None),
    ("list_min_control", [-(2**63)], "m", None),
    ("singleton_narrowing_control", [2**53 + 1], "m", None),
    ("unsigned_array_control", {"array": [2**63], "dtype": "uint64"}, "m", None),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    assert version_string() == "6.7.6-14"
    qa = quanta()
    cases = []
    for name, encoded, unit, deviation in CASES:
        value = decode(encoded)
        for method in ("quantity", "getunit", "add"):
            call_args = ({"value": value, "unit": unit},)
            if method == "add":
                call_args += ("1m",)
            case = {"name": name, "method": method, "value": encoded, "unit": unit}
            try:
                result = getattr(qa, method)(*call_args)
                if isinstance(result, dict) and isinstance(result["value"], np.ndarray):
                    result["value"] = result["value"].tolist()
                case["result"] = result
            except Exception as exc:
                case["error"] = {"type": type(exc).__name__, "message": str(exc)}
                if exc.__cause__ is not None:
                    case["error"]["cause"] = type(exc.__cause__).__name__
            if deviation in ("overflow", "mixed_overflow"):
                if deviation == "mixed_overflow" and method == "add":
                    assert case["error"]["type"] == "RuntimeError"
                else:
                    assert case["error"]["type"] == "SystemError"
                    assert case["error"]["cause"] == "OverflowError"
                case["native_error"] = {
                    "type": "OverflowError",
                    "message": "Python int outside the signed 64-bit range in a quantity record",
                }
            elif deviation == "discarded_value":
                case["native_error"] = {
                    "type": "RuntimeError",
                    "message": "Error Illegal Quantum record in QuantumHolder::fromRecord\n in converting quantity",
                }
            cases.append(case)
    args.out.write_text(json.dumps({"casatools": version_string(), "cases": cases}, indent=2) + "\n")
    print(f"{len(cases)} CASA quantity-record cases")


if __name__ == "__main__":
    main()
