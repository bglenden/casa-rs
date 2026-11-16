# SPDX-License-Identifier: LGPL-3.0-or-later
"""Independent GSL shape control for CASA's lost limb exponent in list copies."""
import copy
import ctypes
import math
from pathlib import Path

import casatools
import numpy as np

from sm_multiple_models_oracle import CENTRE


def basis(longitude, latitude):
    """Cartesian east, north and radial axes of an equatorial tangent plane."""
    s, c = math.sin(longitude), math.cos(longitude)
    t, d = math.sin(latitude), math.cos(latitude)
    return np.asarray([[-s, c, 0.], [-t*c, -t*s, d], [d*c, d*s, t]])


def add_limb_references(cases):
    library = next((Path(casatools.__file__).parent / "__casac__" / "lib").glob("libgsl.*"))
    gsl = ctypes.CDLL(str(library))
    bessel = gsl.gsl_sf_bessel_Jnu
    bessel.argtypes = [ctypes.c_double, ctypes.c_double]
    bessel.restype = ctypes.c_double
    rotation = basis(CENTRE[0]+6e-6, CENTRE[1]-4e-6) @ basis(*CENTRE).T
    pa = math.radians(37.)
    major, minor = [math.radians(axis/3600.) for axis in (1.8, .7)]
    point = next(case for case in cases if case["name"] == "point_control")

    def reference(case, exponent):
        corrected = copy.deepcopy(case["after"]["rows"])
        order = 1. + float(np.float32(exponent))/2.
        multiplier = 2. if case["spec"].get("incremental") else 1.
        for row, point_row in zip(corrected, point["after"]["rows"]):
            if row["field"] != 0:
                continue
            raw = row["uvw"]
            u, v, _ = rotation @ np.asarray([-raw[0], -raw[1], raw[2]])
            x, y = u*math.cos(pa)-v*math.sin(pa), u*math.sin(pa)+v*math.cos(pa)
            distance = math.hypot(x*minor, y*major)*math.pi/299792458.
            for channel, frequency in enumerate((100e9, 150e9, 210e9, 300e9)):
                radius = distance*frequency
                scale = (math.exp(math.lgamma(order+1.))*(2./radius)**order*bessel(order, radius)
                         if radius else 1.) * multiplier
                for column in ("data", "corrected_data"):
                    for product in range(4):
                        index = 4*channel+product
                        row[column][index] = [value*scale for value in point_row[column][index]]
        return corrected

    default = next(case for case in cases if case["name"] == "limb_0.05")
    independent = reference(default, .05)
    error, norm = 0., 0.
    for actual, expected in zip(default["after"]["rows"], independent):
        if actual["field"] == 0:
            error += sum((a-b)**2 for pair, ref in zip(actual["data"], expected["data"])
                         for a, b in zip(pair, ref))
            norm += sum(value**2 for pair in actual["data"] for value in pair)
    nrms = math.sqrt(error/norm)
    assert nrms < 1e-5, ("GSL/tangent-plane control must first match actual CASA", nrms)
    for case in cases:
        if "exponent" in case["spec"] and "distance" not in case["spec"]:
            case["corrected_rows"] = reference(case, case["spec"]["exponent"])
            case["default_exponent_control_nrms"] = nrms
            case["casa_defect"] = (
                "SimpleComponentFTMachine copies each partition through SkyCompRep::fromRecord, "
                "which retains optionalParameters but does not apply them to the shape. "
                "Every requested limb exponent becomes the default 0.05. Raw CASA DATA is retained; "
                "the corrected control uses actual CASA point DATA and its bundled GSL Jnu with "
                "the requested float32 exponent. The same independent geometry/GSL control first "
                "matches CASA's default-exponent prediction.")
    print("default limb GSL control NRMS", nrms, flush=True)
