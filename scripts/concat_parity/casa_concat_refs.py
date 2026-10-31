# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6 concat references for #937 PR B (B.7.7): the unittest/concat
cases inside the core subset, each concatenated from fresh copies of its
inputs (CASA writes a HISTORY row into each appended input).

Usage: python3.12 casa_concat_refs.py INPUTS OUTPUT
"""

import json
import os
import shutil
import sys
import time

from casatasks import concat

CASES = {
    "test1": dict(vis=["part1.ms", "part2.ms", "part3.ms", "part4.ms"]),
    "test2": dict(
        vis=["part1.ms", "part2-mod.ms", "part3.ms"],
        copypointing=False,
        visweightscale=[3.0, 2.0, 1.0],
    ),
    "test3": dict(vis=["part1.ms", "part2-mod2.ms", "part3.ms"]),
    "test8": dict(vis=["sim7.ms", "sim8.ms"], copypointing=False),
    "test16": dict(
        vis=["X39a.pm03.scan3.ms", "X425.pm03.scan4.ms", "X425.pm04.scan4.ms"]
    ),
    "test17": dict(
        vis=[
            "uid___A002_Xab8dc1_X95a-shrunk.ms",
            "uid___A002_Xab8dc1_Xf13-shrunk.ms",
        ],
        copypointing=False,
    ),
}


def main(inputs, output):
    summary = {}
    for name, arguments in CASES.items():
        case = os.path.join(output, name)
        shutil.rmtree(case, ignore_errors=True)
        os.makedirs(case)
        for vis in arguments["vis"]:
            shutil.copytree(os.path.join(inputs, vis), os.path.join(case, vis), symlinks=True)
        os.chdir(case)
        start = time.time()
        try:
            concat(concatvis=name + ".ms", **arguments)
            summary[name] = {"seconds": time.time() - start, "arguments": arguments}
        except Exception as error:  # record and continue with the other cases
            summary[name] = {"error": repr(error), "arguments": arguments}
        print(name, summary[name], flush=True)
    with open(os.path.join(output, "summary.json"), "w") as handle:
        json.dump(summary, handle, indent=2)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
