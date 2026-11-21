#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA imhead edits of non-zenithal direction coordinates."""

import json
import shutil
import tempfile
from pathlib import Path

from casatasks import casalog, imhead
from casatools import image, quanta, table, version_string
import numpy as np

ROOT = Path("crates/casars-imexplore/tests/data/imhead_crpix")
CASES = [(source, "projection", target)
    for source in ("normal", "projection_pointing", "car", "sfl", "mer", "ait")
    for target in ("SIN", "CAR", "SFL", "MER", "AIT")]
CASES += [(source, key, value) for source in ("car", "sfl", "mer", "ait")
          for key, value in (("crval1", "1deg"), ("crval2", "1deg"), ("crval4", "1.5GHz"))]
CASES += [(source, key, value) for source in ("car_pointing", "sfl_pointing")
          for key, value in (("crval1", "1deg"), ("crval2", "1deg"), ("crval4", "1.5GHz"))]
CASES += [("car", "crval2", "-1e-9deg"), ("car", "crval2", "-1e-11deg")]
CASES += [("car_near_pole", "crval2", "-1e-11deg")]
CASES += [(source, key, value) for source in ("sfl_scaled", "sfl_skewed")
          for key, value in (("crval1", "1deg"), ("crval2", "1deg"), ("crval4", "1.5GHz"))]
CASES += [(source, "projection", target) for source in ("sfl_scaled", "sfl_skewed")
          for target in ("SIN", "CAR")]
SETUP_CASES = [
    (source, key, value, {"conversionSystem": "GALACTIC"})
    for source in ("car", "sfl", "mer", "ait")
    for key, value in (("crval2", "1deg"), ("crval4", "1.5GHz"))
]
SETUP_CASES += [
    (source, "crval4", "1.5GHz", {"cdelt": [-1e-10, 2e-10]})
    for source in ("car", "sfl", "mer", "ait")
]
SETUP_CASES.append(("car", "crval4", "1.5GHz", {"cdelt": [0.0, 2e-10]}))
SETUP_CASES.extend([
    ("car", "crval1", "1deg", {
        "conversionSystem": "GALACTIC", "worldmap0": [1, 0], "crpix": [2.0, 3.0],
    }),
    ("sfl", "crval4", "1.5GHz", {"projection_parameters": [1.0, 0.0, 0.0]}),
])


def set_direction_fields(path, fields):
    tb = table()
    tb.open(str(path), nomodify=False)
    try:
        coords = tb.getkeyword("coords")
        for key, value in fields.items():
            if key == "worldmap0":
                coords[key] = np.asarray(value, dtype=np.int32)
            else:
                coords["direction0"][key] = value
        tb.putkeyword("coords", coords)
    finally:
        tb.close()


def main():
    casalog.setlogfile("/tmp/casa-977-nonzenithal-oracle.log")
    casalog.showconsole(False)
    cases = []
    for projection in ("car", "sfl"):
        path = ROOT / f"{projection}_pointing.im"
        if path.exists():
            shutil.rmtree(path)
        shutil.copytree(ROOT / "projection_pointing.im", path)
        imhead(str(path), mode="put", hdkey="projection", hdvalue=projection.upper())
    for target, source, updates in (
        ("car_near_pole", "car", {"longpole": 89.99}),
        ("sfl_scaled", "sfl", {"pc": np.diag([2.0, 2.0])}),
        ("sfl_skewed", "sfl", {"pc": np.array([[2.0, 1.0], [0.5, 3.0]])}),
    ):
        path = ROOT / f"{target}.im"
        if path.exists():
            shutil.rmtree(path)
        shutil.copytree(ROOT / f"{source}.im", path)
        set_direction_fields(path, updates)
    with tempfile.TemporaryDirectory(prefix="imhead-nonzenithal-") as scratch:
        cases_with_setup = [(*case, {}) for case in CASES] + SETUP_CASES
        for index, (source, key, value, setup) in enumerate(cases_with_setup):
            path = Path(scratch) / f"{index}.im"
            shutil.copytree(ROOT / f"{source}.im", path)
            case = {"image": source, "key": key, "value": value}
            if setup:
                set_direction_fields(path, setup)
                case["setup"] = setup
            try:
                imhead(str(path), mode="put", hdkey=key, hdvalue=value)
                summary = imhead(str(path), mode="summary")
                ia = image(); ia.open(str(path)); coords = ia.coordsys(); record = coords.torecord()
                direction = next(v for k, v in record.items() if k.startswith("direction"))
                direction_axes = [f"*{axis + 1}" for axis in record["worldmap0"]]
                qa = quanta(); worlds = []
                for pair in ([0.0, 0.0], [2.5, 1.0]):
                    result = ia.toworld([pair[0], pair[1], 0.0, 0.0], format="q")["quantity"]
                    worlds.append([qa.convert(result[axis], "rad")["value"] for axis in direction_axes])
                if "conversionSystem" in setup:
                    coords.setconversiontype(direction=direction["system"])
                    case["native_worlds"] = []
                    for pair in ([0.0, 0.0], [2.5, 1.0]):
                        result = coords.toworld([*pair, 0.0, 0.0], format="q")["quantity"]
                        case["native_worlds"].append([
                            qa.convert(result[axis], "rad")["value"] for axis in direction_axes
                        ])
                coords.done(); ia.done()
                case.update({"axisunits": summary["axisunits"].tolist(), "refval": summary["refval"].tolist(),
                    "refpix": summary["refpix"].tolist(), "incr": summary["incr"].tolist(),
                    "projection": direction["projection"], "longpole": direction["longpole"],
                    "latpole": direction["latpole"], "worlds": worlds})
                if setup:
                    case["conversion_system"] = direction["conversionSystem"]
                    case["axisnames"] = summary["axisnames"].tolist()
                if "projection_parameters" in setup:
                    case["projection_parameters"] = direction["projection_parameters"].tolist()
                if "worldmap0" in setup:
                    keys = ("crval1", "crval2", "ctype1", "ctype2", "cdelt1", "crpix1")
                    case["axis_get"] = {
                        keyword: imhead(str(path), mode="get", hdkey=keyword)
                        for keyword in keys
                    }
                    header = imhead(str(path), mode="list")
                    case["axis_list"] = {keyword: header[keyword] for keyword in keys}
            except RuntimeError as error:
                case["error"] = str(error).split(" at File:", 1)[0]
            cases.append(case)
    (ROOT / "nonzenithal_expected.json").write_text(json.dumps(
        {"casa_version": version_string(), "cases": cases}, indent=2,
        default=lambda value: value.tolist()) + "\n")


if __name__ == "__main__":
    main()
