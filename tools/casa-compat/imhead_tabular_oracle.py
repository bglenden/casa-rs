#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA tabular spectral coordinate unit edits and FITS exports."""

import json
import runpy
import shutil
import tempfile
from pathlib import Path

import numpy as np
from casatasks import casalog, exportfits, imhead
from casatools import image, table, version_string

ROOT = Path("crates/casars-imexplore/tests/data/imhead_crpix")
fits_spectral = runpy.run_path("tools/casa-compat/imhead_spectral_units_oracle.py")["fits_spectral"]


def main():
    casalog.setlogfile("/tmp/casa-977-tabular-oracle.log")
    casalog.showconsole(False)
    cases = []
    edits = []
    conversion_cases = []
    with tempfile.TemporaryDirectory(prefix="imhead-tabular-") as scratch:
        source = Path(scratch) / "spectral.im"
        shutil.copytree(ROOT / "verbose_tabular.im", source)
        for index, unit in enumerate(("GHz", "MHz", "Hz")):
            assert imhead(str(source), mode="put", hdkey="cunit4", hdvalue=unit) is None
            summary = imhead(str(source), mode="summary")
            rest = imhead(str(source), mode="get", hdkey="restfreq")
            ia = image(); ia.open(str(source)); coords = ia.coordsys()
            pixels = [[1.0, 1.0, 0.0, pixel] for pixel in (0.0, 1.0, 1.5, 3.0)]
            worlds = [coords.toworld(pixel)["numeric"].tolist() for pixel in pixels]
            coords.done(); ia.done()
            fits = Path(scratch) / f"{index}.fits"
            exportfits(imagename=str(source), fitsimage=str(fits), overwrite=True)
            spectral = fits_spectral(fits)
            exportfits(imagename=str(source), fitsimage=str(fits), velocity=True, overwrite=True)
            velocity = fits_spectral(fits, "VRAD")
            cases.append({"unit": unit, "axisunits": summary["axisunits"].tolist(),
                "refval": summary["refval"].tolist(), "incr": summary["incr"].tolist(),
                "refpix": summary["refpix"].tolist(), "restfreq": rest,
                "pixels": pixels, "worlds": worlds, "fits": spectral, "velocity_fits": velocity})
            if index == 0:
                target = ROOT / "tabular_ghz.im"
                if target.exists(): shutil.rmtree(target)
                shutil.copytree(source, target, ignore=shutil.ignore_patterns("table.lock"))
        for index, (unit, key, value) in enumerate([
            ("Hz", "crval4", "1.45GHz"), ("Hz", "cdelt4", "2MHz"),
            ("Hz", "crpix4", 2.0), ("GHz", "crval4", "1.45GHz"),
            ("GHz", "cdelt4", "2MHz"), ("GHz", "crpix4", 2.0),
        ]):
            source = Path(scratch) / f"edit-{index}.im"
            shutil.copytree(ROOT / "verbose_tabular.im", source)
            if unit != "Hz":
                imhead(str(source), mode="put", hdkey="cunit4", hdvalue=unit)
            imhead(str(source), mode="put", hdkey=key, hdvalue=value)
            summary = imhead(str(source), mode="summary")
            ia = image(); ia.open(str(source)); coords = ia.coordsys()
            pixels = [[1.0, 1.0, 0.0, pixel] for pixel in (0.0, 1.0, 1.5, 3.0)]
            worlds = [coords.toworld(pixel)["numeric"].tolist() for pixel in pixels]
            coords.done(); ia.done()
            fits = Path(scratch) / f"edit-{index}.fits"
            exportfits(imagename=str(source), fitsimage=str(fits), overwrite=True)
            spectral = fits_spectral(fits)
            exportfits(imagename=str(source), fitsimage=str(fits), velocity=True, overwrite=True)
            velocity = fits_spectral(fits, "VRAD")
            edits.append({"unit": unit, "key": key, "value": value,
                "axisunits": summary["axisunits"].tolist(), "refval": summary["refval"].tolist(),
                "incr": summary["incr"].tolist(), "refpix": summary["refpix"].tolist(),
                "restfreq": imhead(str(source), mode="get", hdkey="restfreq"),
                "pixels": pixels, "worlds": worlds, "fits": spectral, "velocity_fits": velocity})
        linear = Path(scratch) / "conversion-source.im"
        tabular = Path(scratch) / "tabular-conversion.im"
        shutil.copytree(ROOT / "conversion.im", linear)
        shutil.copytree(ROOT / "verbose_tabular.im", tabular)
        tb = table(); tb.open(str(linear)); record = tb.getkeyword("coords")
        conversion = next(v for k, v in record.items() if k.startswith("spectral"))["conversion"]
        tb.close(); tb.open(str(tabular), nomodify=False); record = tb.getkeyword("coords")
        spectral_key = next(k for k in record if k.startswith("spectral"))
        record[spectral_key]["conversion"] = conversion
        tb.putkeyword("coords", record); tb.close()
        target = ROOT / "tabular_conversion.im"
        if target.exists(): shutil.rmtree(target)
        shutil.copytree(tabular, target, ignore=shutil.ignore_patterns("table.lock"))
        for index, unit in enumerate(("GHz", "Hz")):
            imhead(str(tabular), mode="put", hdkey="cunit4", hdvalue=unit)
            summary = imhead(str(tabular), mode="summary")
            ia = image(); ia.open(str(tabular)); coords = ia.coordsys()
            pixels = [[1.0, 1.0, 0.0, pixel] for pixel in (0.0, 1.0, 1.5, 3.0)]
            worlds = [coords.toworld(pixel)["numeric"].tolist() for pixel in pixels]
            coords.done(); ia.done()
            fits = Path(scratch) / f"conversion-{index}.fits"
            exportfits(imagename=str(tabular), fitsimage=str(fits), overwrite=True)
            spectral = fits_spectral(fits)
            exportfits(imagename=str(tabular), fitsimage=str(fits), velocity=True, overwrite=True)
            velocity = fits_spectral(fits, "VRAD")
            conversion_cases.append({"unit": unit, "axisunits": summary["axisunits"].tolist(),
                "refval": summary["refval"].tolist(), "incr": summary["incr"].tolist(),
                "refpix": summary["refpix"].tolist(),
                "restfreq": imhead(str(tabular), mode="get", hdkey="restfreq"),
                "pixels": pixels, "worlds": worlds, "fits": spectral, "velocity_fits": velocity})
        nonunit_cases = []
        for name, samples in [("tabular_nonunit.im", [0.0, 2.0, 4.0, 6.0]),
                              ("tabular_shifted.im", [2.0, 4.0, 6.0, 8.0])]:
            nonunit = Path(scratch) / name
            shutil.copytree(ROOT / "verbose_tabular.im", nonunit)
            tb.open(str(nonunit), nomodify=False); record = tb.getkeyword("coords")
            spectral_key = next(k for k in record if k.startswith("spectral"))
            record[spectral_key]["tabular"]["pixelvalues"] = np.array(samples)
            tb.putkeyword("coords", record); tb.close()
            target = ROOT / name
            if target.exists(): shutil.rmtree(target)
            shutil.copytree(nonunit, target, ignore=shutil.ignore_patterns("table.lock"))
            summary = imhead(str(nonunit), mode="summary")
            ia = image(); ia.open(str(nonunit)); coords = ia.coordsys()
            pixels = [[1.0, 1.0, 0.0, pixel] for pixel in (0.0, 1.0, 1.5, 3.0)]
            worlds = [coords.toworld(pixel)["numeric"].tolist() for pixel in pixels]
            coords.done(); ia.done()
            fits = Path(scratch) / f"{name}.fits"
            exportfits(imagename=str(nonunit), fitsimage=str(fits), overwrite=True)
            spectral = fits_spectral(fits)
            exportfits(imagename=str(nonunit), fitsimage=str(fits), velocity=True, overwrite=True)
            velocity = fits_spectral(fits, "VRAD")
            nonunit_cases.append({"image": name, "axisunits": summary["axisunits"].tolist(), "refval": summary["refval"].tolist(),
                "incr": summary["incr"].tolist(), "refpix": summary["refpix"].tolist(),
                "restfreq": imhead(str(nonunit), mode="get", hdkey="restfreq"),
                "pixels": pixels, "worlds": worlds, "fits": spectral, "velocity_fits": velocity})
    (ROOT / "tabular_unit_expected.json").write_text(json.dumps(
        {"casa_version": version_string(), "cases": cases, "edits": edits, "conversion_cases": conversion_cases, "nonunit_cases": nonunit_cases}, indent=2,
        default=lambda value: value.tolist()) + "\n")


if __name__ == "__main__":
    main()
