#!/usr/bin/env python3
"""Capture CASA 6.7.6 image-metadata list messages for existing fixtures."""

import json
import shutil
import tempfile
from pathlib import Path

from casatasks import casalog, imhead
from casatools import version_string


def main():
    root = Path("crates/casars-imexplore/tests/data")
    cases = {
        "imhead_crpix": ["normal", "beam", "stokes_four", "multi", "multi_transposed", "linear", "ghz", "transposed", "projection_pointing"],
        "imhead_beams": ["multiple_mixed", "degrees"],
        "imhead_extrema": ["ordinary", "masked", "partial", "galactic", "complex"],
    }
    results = {}
    with tempfile.TemporaryDirectory(prefix="imhead-list-log-") as temp:
        temp = Path(temp)
        for directory, names in cases.items():
            for name in names:
                image = temp / (name + ".im")
                shutil.copytree(root / directory / (name + ".im"), image)
                log = temp / "casa.log"
                log.unlink(missing_ok=True)
                casalog.setlogfile(str(log))
                imhead(str(image), mode="list")
                results[f"{directory}/{name}"] = [
                    line.split("\t", 3)[3]
                    for line in log.read_text().splitlines()
                    if "\tImageMetaDataBase::_toLog \t" in line
                ]
                shutil.rmtree(image)
    (root / "imhead_crpix/list_log_expected.json").write_text(
        json.dumps({"casa_version": version_string(), "cases": results}, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
