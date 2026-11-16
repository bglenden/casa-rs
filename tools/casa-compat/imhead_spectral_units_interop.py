#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reopen native spectral edits, exports and FITS beam imports in CASA."""

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

import numpy as np
from casatasks import casalog, imhead
from casatools import image

ROOT = Path("crates/casars-imexplore/tests/data/imhead_crpix")
FITS = Path("crates/casars-imexplore/tests/data/imhead_fits")
BIN = Path("target/debug").resolve()


def run(binary, surface, values):
    args = [str(BIN / binary)]
    if binary == "imexplore":
        args.append(surface)
    args += ["--json-run", "-"]
    result = subprocess.run(args, input=json.dumps({"surface": surface, "values": values}),
                            text=True, capture_output=True, check=True)
    reply = json.loads(result.stdout)
    if reply["status"] != "succeeded":
        raise AssertionError(reply)


def main():
    casalog.setlogfile("/tmp/casa-977-native-interop.log")
    casalog.showconsole(False)
    expected = json.loads((ROOT / "spectral_unit_expected.json").read_text())
    with tempfile.TemporaryDirectory(prefix="imhead-native-interop-") as scratch:
        scratch = Path(scratch)
        for index, case in enumerate(expected["cases"]):
            path = scratch / f"{index}.im"
            output = scratch / f"{index}.fits"
            shutil.copytree(ROOT / f"{case['image']}.im", path)
            run("imexplore", "imhead", {"imagename": str(path), "mode": "put",
                "hdkey": f"cunit{case['axis']}", "hdvalue": case["unit"]})
            summary = imhead(str(path), mode="summary")
            for key in ("axisunits", "refval", "incr", "refpix"):
                if key == "axisunits":
                    assert list(summary[key]) == case[key]
                else:
                    np.testing.assert_allclose(summary[key], case[key], rtol=1e-10, atol=1e-12)
            ia = image()
            ia.open(str(path))
            cs = ia.coordsys()
            np.testing.assert_allclose(cs.toworld(case["pixel"])["numeric"], case["world"], rtol=1e-7, atol=1e-10)
            cs.done()
            ia.done()
            if "error" not in case["fits"]:
                run("exportfits", "exportfits", {"imagename": str(path), "fitsimage": str(output)})
                ia.open(str(output))
                assert ia.shape().size == len(case["pixel"])
                ia.done()
                velocity_output = scratch / f"{index}-velocity.fits"
                run("exportfits", "exportfits", {"imagename": str(path),
                    "fitsimage": str(velocity_output), "velocity": True})
                velocity_summary = imhead(str(velocity_output), mode="summary")
                spectral_axis = list(velocity_summary["axisnames"]).index("Frequency")
                cards = case["velocity_fits"]
                expected_frequency = cards["RESTFRQ"] * (1 - cards["CRVAL"] / 299792458.0)
                np.testing.assert_allclose(velocity_summary["refval"][spectral_axis], expected_frequency, rtol=1e-10)

        spellings = json.loads((ROOT / "unit_spellings_expected.json").read_text())
        for index, case in enumerate(spellings["cases"]):
            path = scratch / f"spelling-{index}.im"
            shutil.copytree(ROOT / "normal.im", path)
            run("imexplore", "imhead", {"imagename": str(path), "mode": "put",
                "hdkey": case["key"], "hdvalue": case["unit"]})
            summary = imhead(str(path), mode="summary")
            assert list(summary["axisunits"]) == case["axisunits"]
            ia = image(); ia.open(str(path)); coords = ia.coordsys()
            np.testing.assert_allclose(coords.toworld([1.0, 1.0, 0.0, 1.0])["numeric"],
                case["world"], rtol=1e-7, atol=1e-10)
            coords.done(); ia.done()

        tabular = json.loads((ROOT / "tabular_unit_expected.json").read_text())
        tabular_image = scratch / "tabular.im"
        shutil.copytree(ROOT / "verbose_tabular.im", tabular_image)
        for index, case in enumerate(tabular["cases"]):
            run("imexplore", "imhead", {"imagename": str(tabular_image), "mode": "put",
                "hdkey": "cunit4", "hdvalue": case["unit"]})
            summary = imhead(str(tabular_image), mode="summary")
            assert list(summary["axisunits"]) == case["axisunits"]
            ia = image(); ia.open(str(tabular_image)); coords = ia.coordsys()
            for pixel, expected_world in zip(case["pixels"], case["worlds"]):
                np.testing.assert_allclose(coords.toworld(pixel)["numeric"], expected_world, rtol=1e-7, atol=1e-10)
            coords.done(); ia.done()
            output = scratch / f"tabular-{index}.fits"
            run("exportfits", "exportfits", {"imagename": str(tabular_image), "fitsimage": str(output)})
            ia.open(str(output)); ia.done()

        converted_tabular = scratch / "converted-tabular.im"
        shutil.copytree(ROOT / "tabular_conversion.im", converted_tabular)
        for case in tabular["conversion_cases"]:
            run("imexplore", "imhead", {"imagename": str(converted_tabular), "mode": "put",
                "hdkey": "cunit4", "hdvalue": case["unit"]})
            summary = imhead(str(converted_tabular), mode="summary")
            assert list(summary["axisunits"]) == case["axisunits"]
            ia = image(); ia.open(str(converted_tabular)); coords = ia.coordsys()
            for pixel, expected_world in zip(case["pixels"], case["worlds"]):
                np.testing.assert_allclose(coords.toworld(pixel)["numeric"], expected_world, rtol=1e-7, atol=1e-10)
            coords.done(); ia.done()

        for index, case in enumerate(tabular["edits"]):
            path = scratch / f"tabular-edit-{index}.im"
            shutil.copytree(ROOT / "verbose_tabular.im", path)
            if case["unit"] != "Hz":
                run("imexplore", "imhead", {"imagename": str(path), "mode": "put",
                    "hdkey": "cunit4", "hdvalue": case["unit"]})
            run("imexplore", "imhead", {"imagename": str(path), "mode": "put",
                "hdkey": case["key"], "hdvalue": case["value"]})
            summary = imhead(str(path), mode="summary")
            for key in ("refval", "incr", "refpix"):
                np.testing.assert_allclose(summary[key], case[key], rtol=1e-10, atol=1e-12)
            ia = image(); ia.open(str(path)); coords = ia.coordsys()
            for pixel, expected_world in zip(case["pixels"], case["worlds"]):
                np.testing.assert_allclose(coords.toworld(pixel)["numeric"], expected_world, rtol=1e-7, atol=1e-10)
            coords.done(); ia.done()

        generic = json.loads((ROOT / "generic_tabular_expected.json").read_text())
        for index, case in enumerate(generic["cases"] + generic["linear_cases"]):
            path = scratch / f"generic-tabular-{index}.im"
            source = "generic_tabular.im" if index < len(generic["cases"]) else "generic_linear_tabular.im"
            shutil.copytree(ROOT / source, path)
            if case["unit"] != "m":
                run("imexplore", "imhead", {"imagename": str(path), "mode": "put",
                    "hdkey": "cunit1", "hdvalue": case["unit"]})
            run("imexplore", "imhead", {"imagename": str(path), "mode": "put",
                "hdkey": case["key"], "hdvalue": case["value"]})
            summary = imhead(str(path), mode="summary")
            for key in ("refval", "incr", "refpix"):
                np.testing.assert_allclose(summary[key], case[key], rtol=1e-10, atol=1e-12)
            ia = image(); ia.open(str(path)); coords = ia.coordsys()
            for pixel, expected_world in zip(case["pixels"], case["worlds"]):
                np.testing.assert_allclose(coords.toworld([pixel])["numeric"], [expected_world], rtol=1e-7, atol=1e-10)
            coords.done(); ia.done()

        imported = scratch / "beam-import.im"
        run("importfits", "importfits", {"fitsimage": str((FITS / "beams.fits").resolve()),
            "imagename": str(imported)})
        imported_summary = imhead(str(imported), mode="summary")
        beams = json.loads((FITS / "expected.json").read_text())["beam_cases"]["summary:"]["value"]["perplanebeams"]
        assert imported_summary["perplanebeams"] == beams, (imported_summary["perplanebeams"], beams)
        scaled = scratch / "scaled-import.im"
        run("importfits", "importfits", {"fitsimage": str((FITS / "scaled.fits").resolve()),
            "imagename": str(scaled)})
        rest = imhead(str(scaled), mode="get", hdkey="restfreq")
        assert rest["unit"] == "Hz" and abs(rest["value"] - 1420405751.786) < 1e-3
    print(f"CASA reopened {len(expected['cases'])} linear spectral edits, "
          f"{len(spellings['cases'])} unit spellings, {len(tabular['cases']) + len(tabular['conversion_cases'])} tabular unit edits, "
          f"{len(tabular['edits']) + len(generic['cases']) + len(generic['linear_cases'])} tabular coordinate edits, the frequency and velocity FITS exports, "
          "and both FITS imports")


if __name__ == "__main__":
    main()
