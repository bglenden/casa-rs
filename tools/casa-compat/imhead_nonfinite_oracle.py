#!/usr/bin/env python3
"""Create a CASA record containing non-finite scalars and arrays."""

import json
import math
import shutil
from pathlib import Path

from casatasks import imhead
from casatools import version_string


def main():
    root = Path("crates/casars-imexplore/tests/data/imhead_crpix")
    image = root / "custom_nonfinite.im"
    if image.exists():
        shutil.rmtree(image)
    shutil.copytree(root / "normal.im", image)
    record = {"scalar": -float("nan"), "negative": -float("inf"), "array": [float("inf"), -float("nan")], "words": [-float("nan"), "x"]}
    assert imhead(str(image), mode="add", hdkey="custom", hdvalue=record) is None
    actual = imhead(str(image), mode="get", hdkey="custom")
    assert math.isnan(actual["scalar"]) and actual["negative"] == -float("inf")
    assert math.isinf(actual["array"][0]) and math.isnan(actual["array"][1])
    assert actual["words"].tolist() == ["-nan", "x"]
    for info in image.rglob("table.info"):
        info.write_text("\n".join(line.rstrip() for line in info.read_text().splitlines()).rstrip() + "\n")
    for lock in image.rglob("table.lock"):
        lock.unlink()
    (root / "custom_nonfinite_expected.json").write_text(json.dumps({
        "casa_version": version_string(), "array_dtype": str(actual["array"].dtype),
        "array_shape": list(actual["array"].shape), "words": actual["words"].tolist(),
    }, indent=2) + "\n")


if __name__ == "__main__":
    main()
