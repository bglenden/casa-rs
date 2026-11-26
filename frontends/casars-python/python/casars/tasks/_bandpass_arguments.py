"""Validate the generated CASA argument shapes before invoking the native task."""

import numpy as np


_TYPES = {
    **dict.fromkeys(("antenna", "field", "spw"), ("cStr", "cInt", "cStrVec", "cIntVec")),
    **dict.fromkeys(("basebands", "scans"), ("cInt", "cStr", "cIntVec")),
    **dict.fromkeys(("showpoints", "showlines"), ("cStr", "cBool")),
    **dict.fromkeys(("showatm", "showtsky", "showfdm", "showimage", "showatmpoints", "showflagged",
                     "interactive", "buildpdf", "showBasebandNumber", "figfileSequential",
                     "chanrangeSetXrange", "debug"), ("cBool",)),
    **dict.fromkeys(("edge", "resample", "markersize", "density"), ("cInt",)),
    **dict.fromkeys(("platformingSigma", "platformingThreshold", "solutionTimeThresholdSeconds"), ("cFloat",)),
    "subplot": ("cStr", "cInt"),
    "poln": ("cStrVec", "cStr"),
    "plotrange": ("cFloatVec",),
    "chanrange": ("cStr", "cIntVec"),
    "phase": ("cIntVec", "cStr"),
    "showatmfield": ("cInt", "cStr"),
    "channeldiff": ("cBool", "cFloat"),
    "lo1": ("cStr", "cFloat"),
    "pwv": ("cFloat", "cStr"),
}
_ALLOWED = {
    "xaxis": ("chan", "freq"),
    "yaxis": ("tsys", "bothdb", "ap", "apdb", "phase", "amp", "ampdb", "both"),
    "overlay": ("antenna,time", "antenna", "time,antenna", "baseband", "spw", "", "time"),
    "zoom": ("intersect", ""),
}
_SCALARS = {
    "cStr": (str,),
    "cInt": (int, np.int32, np.int64),
    "cBool": (bool, np.bool_),
    "cFloat": (float, int, np.int32, np.int64, np.float32, np.float64),
}
_CONVERT = {"cStr": str, "cInt": int, "cBool": bool, "cFloat": float}
_INVALID = object()


def _coerce(value, casa_type):
    if not casa_type.endswith("Vec"):
        if isinstance(value, _SCALARS[casa_type]):
            return _CONVERT[casa_type](value)
        return _INVALID

    element = casa_type.removesuffix("Vec")
    if isinstance(value, np.ndarray):
        if value.ndim != 1 or value.dtype.type not in _SCALARS[element]:
            return _INVALID
        values = value
    elif isinstance(value, (list, tuple, dict)):
        values = value
    elif isinstance(value, _SCALARS[element]):
        values = [value]
    else:
        return _INVALID
    if all(isinstance(item, _SCALARS[element]) for item in values):
        return [_CONVERT[element](item) for item in values]
    return _INVALID


def normalize(values, arguments):
    errors = {}
    for name, value in arguments.items():
        if name == "caltable":
            continue
        types = _TYPES.get(name, ("cStr",))
        if value is None:
            reasons = ["null value not allowed"] * (len(types) + (len(types) > 1))
            if name in _ALLOWED:
                reasons.insert(0, "unallowed value None")
            errors[name] = reasons
            continue
        for casa_type in types:
            converted = _coerce(value, casa_type)
            if converted is not _INVALID:
                if name in values:
                    values[name] = converted
                break
        else:
            errors[name] = [f"must be of {casa_type} type" for casa_type in types]
            continue
        if name in _ALLOWED and converted not in _ALLOWED[name]:
            errors[name] = [f"unallowed value {converted}"]
    if errors:
        raise AssertionError(str(dict(sorted(errors.items()))))
