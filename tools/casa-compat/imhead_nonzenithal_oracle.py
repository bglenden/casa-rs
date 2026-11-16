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
        tb = table(); tb.open(str(path), nomodify=False)
        coords = tb.getkeyword("coords"); coords["direction0"].update(updates)
        tb.putkeyword("coords", coords); tb.close()
    with tempfile.TemporaryDirectory(prefix="imhead-nonzenithal-") as scratch:
        for index, (source, key, value) in enumerate(CASES):
            path = Path(scratch) / f"{index}.im"
            shutil.copytree(ROOT / f"{source}.im", path)
            case = {"image": source, "key": key, "value": value}
            try:
                imhead(str(path), mode="put", hdkey=key, hdvalue=value)
                summary = imhead(str(path), mode="summary")
                ia = image(); ia.open(str(path)); coords = ia.coordsys(); record = coords.torecord()
                direction = next(v for k, v in record.items() if k.startswith("direction"))
                qa = quanta(); worlds = []
                for pair in ([0.0, 0.0], [2.5, 1.0]):
                    result = ia.toworld([pair[0], pair[1], 0.0, 0.0], format="q")["quantity"]
                    worlds.append([qa.convert(result[f"*{axis}"], "rad")["value"] for axis in (1, 2)])
                coords.done(); ia.done()
                case.update({"axisunits": summary["axisunits"].tolist(), "refval": summary["refval"].tolist(),
                    "refpix": summary["refpix"].tolist(), "incr": summary["incr"].tolist(),
                    "projection": direction["projection"], "longpole": direction["longpole"],
                    "latpole": direction["latpole"], "worlds": worlds})
            except RuntimeError as error:
                case["error"] = str(error).split(" at File:", 1)[0]
            cases.append(case)
    (ROOT / "nonzenithal_expected.json").write_text(json.dumps(
        {"casa_version": version_string(), "cases": cases}, indent=2,
        default=lambda value: value.tolist()) + "\n")


if __name__ == "__main__":
    main()
