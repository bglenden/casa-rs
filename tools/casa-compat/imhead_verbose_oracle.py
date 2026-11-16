#!/usr/bin/env python3
"""Create a multibeam image and capture CASA's verbose summary values."""

import json
import re
import shutil
import tempfile
from pathlib import Path

from casatasks import casalog, imhead
from casatools import image, version_string


def plain(value):
    if hasattr(value, "tolist"):
        return value.tolist()
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    return value


def main():
    root = Path("crates/casars-imexplore/tests/data/imhead_crpix")
    target = root / "verbose_beams.im"
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(root / "stokes_four.im", target)
    ia = image()
    ia.open(str(target))
    for channel in range(4):
        for pol in range(4):
            ia.setrestoringbeam(
                major=f"{4 + channel + pol}arcsec",
                minor=f"{1 + channel / 4}arcsec",
                pa=f"{channel * 5 + pol}deg",
                channel=channel,
                polarization=pol,
            )
    ia.done()
    result = {str(verbose).lower(): plain(imhead(str(target), mode="summary", verbose=verbose)) for verbose in (False, True)}
    tabular = root / "verbose_tabular.im"
    if tabular.exists():
        shutil.rmtree(tabular)
    shutil.copytree(target, tabular)
    ia.open(str(tabular))
    coords = ia.coordsys()
    coords.setspectral(frequencies={"value": [1.4e9, 1.41e9, 1.43e9, 1.46e9], "unit": "Hz"})
    ia.setcoordsys(coords.torecord())
    coords.done()
    ia.done()
    with tempfile.TemporaryDirectory(prefix="imhead-verbose-log-") as temp:
        logfile = Path(temp) / "casa.log"
        casalog.setlogfile(str(logfile))
        imhead(str(tabular), mode="summary", verbose=True)
        line = next(line for line in logfile.read_text().splitlines() if "Velocity " in line and "km/s" in line)
        velocity_increment = float(re.search(r"([-+\d.eE]+) km/s\s*$", line).group(1))
    for image_path in (target, tabular):
        for info in image_path.rglob("table.info"):
            info.write_text("\n".join(line.rstrip() for line in info.read_text().splitlines()).rstrip() + "\n")
        for lock in image_path.rglob("table.lock"):
            lock.unlink()
    (root / "verbose_expected.json").write_text(
        json.dumps({"casa_version": version_string(), "summary": result, "tabular_velocity_increment_km_s": velocity_increment}, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
