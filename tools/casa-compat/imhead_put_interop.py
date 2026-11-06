#!/usr/bin/env python3
"""Have CASA reopen images updated by the native imhead executable."""

import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

from casatasks import imhead
from casatools import image, version_string


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=lambda p: Path(p).resolve())
    args = parser.parse_args()
    root = Path("crates/casars-imexplore/tests/data/imhead_put")
    expected = json.loads((root / "expected.json").read_text())
    assert version_string() == expected["casa_version"]
    with tempfile.TemporaryDirectory(prefix="imhead-put-interop-") as directory:
        for index, case in enumerate(expected["cases"]):
            path = Path(directory) / f"case-{index}.im"
            shutil.copytree(root / "base.im", path)
            request = {"surface": "imhead", "values": {
                "imagename": str(path), "mode": "put",
                "hdkey": case["key"], "hdvalue": case["value"],
            }}
            result = subprocess.run([str(args.binary), "imhead", "--json-run", "-"],
                                    input=json.dumps(request), text=True, capture_output=True)
            assert (result.returncode != 0) == ("error" in case), (case, result.stderr)
            actual = imhead(str(path), mode="get", hdkey=case["key"])
            assert actual == case["readback"], (case, actual)
            beam = imhead(str(path), mode="summary")["restoringbeam"]
            assert beam == case["beam"], (case, beam)
            if "error" not in case:
                tool = image()
                tool.open(str(path))
                history = tool.history(list=False)
                tool.done()
                assert history[-3] == case["history"][0], (case, history[-3:])
                assert history[-2].startswith("version: casa-rs "), (case, history[-3:])
                command = case["history"][2].replace("<IMAGE>", str(path))
                assert history[-1] == command, (case, history[-3:])
    print(f"CASA {version_string()} reopened {len(expected['cases'])} native imhead results")


if __name__ == "__main__":
    main()
