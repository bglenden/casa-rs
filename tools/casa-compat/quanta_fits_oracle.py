#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record the process-wide effect of CASA quanta.fits()."""
import json

from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

aliases = {
    "BEAM": "beam", "DAYS": "d", "DEGREES": "deg", "DEG": "deg",
    "HZ": "Hz", "JY": "Jy", "KELVINS": "K", "KELVIN": "K",
    "KM": "km", "METERS": "m", "M": "m", "PASCAL": "Pa",
    "PIXEL": "pixel", "SECONDS": "s", "SEC": "s", "S": "s",
    "VOLTS": "V", "YEARS": "a", "YEAR": "a", "kHZ": "kHz",
    "JY/BEAM": "Jy/beam", "S.m": "s.m", "S2": "s2", "NO_SUCH_FITS_UNIT": "m",
}
q = quanta()


def read(unit, target, tool=q):
    try:
        value = tool.quantity(1, unit)
        conformant = tool.compare(value, tool.quantity(1, target))
        result = {"quantity": value, "conformant": conformant}
        if conformant:
            result["converted"] = tool.convert(value, target)
        return result
    except Exception as error:
        return {"error": {"type": type(error).__name__, "message": str(error)}}


before = {unit: read(unit, target) for unit, target in aliases.items()}
first = q.fits()
after = {unit: read(unit, target) for unit, target in aliases.items()}
another = quanta()
other = read("DEGREES", "deg", another)
second = another.fits()
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "aliases": aliases, "before": before, "first": first, "after": after, "second": second, "other_instance": other}))
