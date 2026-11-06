#!/usr/bin/env python3
"""Capture CASA's complex-valued custom header record."""

import json
import shutil
from pathlib import Path

from casatasks import imhead
from casatools import version_string


def main():
    root = Path("crates/casars-imexplore/tests/data/imhead_crpix")
    image = root / "custom_complex.im"
    if image.exists():
        shutil.rmtree(image)
    shutil.copytree(root / "normal.im", image)
    expected_input = {"scalar": 1.25 - 2.5j, "nested": {"zero": 0j, "imag": -3j}}
    assert imhead(str(image), mode="add", hdkey="custom", hdvalue=expected_input) is None
    result = imhead(str(image), mode="get", hdkey="custom")
    assert result == expected_input
    assert type(result["scalar"]) is complex
    for info in image.rglob("table.info"):
        info.write_text("\n".join(line.rstrip() for line in info.read_text().splitlines()).rstrip() + "\n")
    for lock in image.rglob("table.lock"):
        lock.unlink()
    (root / "custom_complex_expected.json").write_text(json.dumps({
        "casa_version": version_string(),
        "record": {"scalar": [result["scalar"].real, result["scalar"].imag],
                   "nested": {key: [value.real, value.imag] for key, value in result["nested"].items()}},
    }, indent=2) + "\n")


if __name__ == "__main__":
    main()
