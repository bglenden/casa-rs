#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the connected cube depth diagnostic using the retained comparison code.

Dirty-only has no CLEAN mask and is a six-product CPU/Metal diagnostic. CLEAN
uses the unchanged seven-product contract; its CASA acceptance is assessed by
the existing scientific gate, not by this wrapper.
"""

import argparse
import json
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--records", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--left", required=True)
    parser.add_argument("--right", required=True)
    parser.add_argument("--dirty", action="store_true")
    args = parser.parse_args()
    sys.path.insert(0, str(args.snapshot))
    from perf_harness.image_compare import compare_products

    contract = json.loads((args.snapshot / "t55-clark-cube-development.json").read_text())["comparison"]
    if args.dirty:
        contract["products"].remove(".mask")
    directory = args.records / args.label
    directory.mkdir()
    result = compare_products(
        casa_python=sys.executable, cwd=directory, artifact_prefix=directory / "comparison",
        request={**contract, "left_prefix": args.left, "right_prefix": args.right,
                 "left_label": Path(args.left).parent.name,
                 "right_label": Path(args.right).parent.name,
                 "panel_dir": str(directory / "panels"),
                 "structure_workspace_dir": str(directory / "structure")},
    )
    with (directory / "result.json").open("x") as output:
        json.dump(result, output, indent=2)
    assert len(result["products"]) == (6 if args.dirty else 7)
    assert all(p["full_array"]["coverage_complete"] for p in result["products"].values())
    assert result["status"] in ("completed", "out_of_tolerance")
    print(json.dumps({"status": result["status"],
                      "checks": result["tolerance_evaluation"]["checks"]}), flush=True)


if __name__ == "__main__":
    main()
