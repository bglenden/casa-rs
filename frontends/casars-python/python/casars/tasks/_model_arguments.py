# SPDX-License-Identifier: LGPL-3.0-or-later
"""Argument coercion for CASA's public visibility-model task wrappers."""

import os

import numpy as np


_TYPES = {
    "clearcal": {"addmodel": "cBool"},
    "delmod": {"otf": "cBool", "scr": "cBool"},
    "ft": {"model": "cVariant", "nterms": "cInt", "incremental": "cBool", "usescratch": "cBool"},
    "uvsub": {"reverse": "cBool"},
    "uvmodelfit": {"selectdata": "cBool", "uvrange": "cVariant", "niter": "cInt",
                   "sourcepar": "cFloatVec", "varypar": "cBoolVec"},
}
_INTS = (int, np.int32, np.int64)
_NUMBERS = (*_INTS, float, np.float32, np.float64)
_BOOLS = (bool, np.bool_)
_SCALARS = {
    "cStr": (str,), "cInt": _INTS, "cBool": _BOOLS,
    "cVariant": (str, dict, list, np.ndarray, *_NUMBERS),
}
_TOOL_FLAGS = {
    "ft": ("usescratch", "imager_open", 4),
    "uvsub": ("reverse", "ms_uvsub", 2),
}


def _vector(value, scalar_types, element_types):
    if type(value) in scalar_types:
        return [value]
    if isinstance(value, str):
        return None
    try:
        items = list(value)
    except TypeError:
        return None
    return items if all(isinstance(item, element_types) for item in items) else None


def normalize(task, values, arguments):
    errors = {}
    for name, value in arguments.items():
        if name == "vis":
            continue
        schema = _TYPES[task].get(name, "cStr")
        if name in ("complist", "outfile") and isinstance(value, os.PathLike):
            value = os.fspath(value)
        if value is None:
            errors[name] = ["null value not allowed"]
            if name == "comptype":
                errors[name].insert(0, "unallowed value None")
        elif schema == "cFloatVec":
            items = _vector(value, _NUMBERS, _NUMBERS)
            if items is None:
                errors[name] = [f"must be of {schema} type"]
            elif name in values:
                values[name] = [float(item) for item in items]
        elif schema == "cBoolVec":
            items = _vector(value, (bool,), _BOOLS)
            if items is None:
                errors[name] = [f"must be of {schema} type"]
            elif name in values:
                values[name] = [bool(item) for item in items]
        elif not isinstance(value, _SCALARS[schema]):
            errors[name] = [f"must be of {schema} type"]
        elif name == "comptype" and value not in ("P", "G", "D"):
            errors[name] = [f"unallowed value {value}"]
        elif name in values:
            if schema == "cInt":
                values[name] = int(value)
            elif schema == "cStr":
                values[name] = str(value)
            elif name == "uvrange" and isinstance(value, np.int64):
                values[name] = float(value)
            elif name == "model" and isinstance(value, np.ndarray):
                # The wrapper admits an array as a variant, then ft refuses
                # it after attachment. Use the same native non-list refusal.
                values[name] = False
            elif name == "model" and isinstance(value, list):
                values[name] = [str(item) if isinstance(item, str) else False for item in value]
    if errors:
        raise AssertionError(str(dict(sorted(errors.items()))))

    # These NumPy booleans pass the generated schema then fail conversion
    # when entering the C++ method, before the requested mutation.
    if task in _TOOL_FLAGS:
        name, method, index = _TOOL_FLAGS[task]
        if isinstance(arguments[name], np.bool_):
            raise TypeError(f"in method '{method}', argument {index} of type 'bool'")
