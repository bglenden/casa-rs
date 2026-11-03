#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""The noise level of CASA's simobserve noise tests over many seeds, CASA and casa-rs (#1053).

CASA's `test_task_simobserve.py` checks a noise test against the level CASA's
own random stream gives for seed 11111, within 5%. casa-rs's stream is its
own, so one run of it is a different draw. On a few hundred samples, two
draws differ by several percent by chance. This script runs a noise test's
`simobserve` call over many seeds with each implementation. It records each
run's level, the suite's `(noisy - original).real.std()`, so the two
distributions can be compared and the recorded value placed in CASA's.

Run with CASA 6.7.6.14's Python:

    CASAPY tools/simulation/simobserve_noise_seeds.py --casatestdata LOCAL_COPY \\
        --casars-bin TARGET/release/simobserve --out OUT [--seeds 20]

casa-rs is asked for an hour angle later by TAI − UTC, as in
`simobserve_parity.py`: CASA stores TAI under a UTC label and casa-rs the
requested UTC (simulation plan decision 5). The receipt goes to
`OUT/receipt.json`.
"""

from __future__ import annotations

import argparse
import datetime
import glob
import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parents[2]

# The suite's noise tests: their simobserve call, the inputs they copy and
# the level the suite holds them to (test_task_simobserve.py, CASA 6.7.6.14).
CASES = {
    "testNZ_sdAtm": {
        "params": dict(project="noise_sd", skymodel="noise_sd/noise_sd.aca.tp.model", setpointings=False,
                       integration="4s", obsmode="", sdantlist="aca.tp.cfg", antennalist="",
                       thermalnoise="tsys-atm", user_pwv=1.0, graphics="none"),
        "inputs": ["flatone.model", "flatone.single.ptg.txt", "noise_sd"],
        "recorded": 1.067681,
    },
    "testNZ_intMan": {
        "params": dict(project="noise_int", skymodel="noise_int/noise_int.aca_cycle1.model", setpointings=False,
                       integration="4s", obsmode="", sdantlist="", antennalist="aca_cycle1.cfg",
                       thermalnoise="tsys-manual", tau0=1.0, graphics="none"),
        "inputs": ["flatone.model", "flatone.single.ptg.txt", "noise_int"],
        "recorded": 9.78451847017,
    },
}


def git(*args: str) -> str:
    return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, text=True, check=True).stdout.strip()


def prepare(source: str, where: str, inputs: list[str]) -> None:
    shutil.rmtree(where, ignore_errors=True)
    os.makedirs(where)
    for item in inputs:
        path = os.path.join(source, item)
        (shutil.copytree if os.path.isdir(path) else shutil.copy)(path, os.path.join(where, item))


def level(tb, where: str, project: str) -> tuple[float, int]:
    """The suite's noise level: (noisy - original).real.std(), and the sample count."""
    mss = glob.glob(os.path.join(where, project, "*.ms"))
    noisy = next(p for p in mss if ".noisy" in p or ".noisier" in p)
    original = next(p for p in mss if ".noisy" not in p and ".noisier" not in p)
    tb.open(noisy)
    noisy_data = tb.getcol("DATA")
    tb.close()
    tb.open(original)
    original_data = tb.getcol("DATA")
    tb.close()
    difference = (noisy_data - original_data).real
    return float(difference.std()), int(difference.size)


def run_casars(binary: str, where: str, params: dict, me) -> None:
    values = dict(params)
    epoch = me.epoch("utc", "2014/01/01/00:00:00")
    values["hourangle"] = f"{(me.measure(epoch, 'tai')['m0']['value'] - epoch['m0']['value']) * 86400.0!r}s"
    request = os.path.join(where, "request.json")
    with open(request, "w", encoding="utf-8") as handle:
        json.dump({"surface": "simobserve", "values": values}, handle)
    result = subprocess.run([binary, "--json-run", request], capture_output=True, text=True, cwd=where)
    reply = json.loads(result.stdout)
    if reply.get("status") != "succeeded":
        raise RuntimeError(reply.get("message") or reply)


def run_casa(simobserve, where: str, params: dict) -> None:
    cwd = os.getcwd()
    os.chdir(where)
    try:
        simobserve(**params)
    finally:
        os.chdir(cwd)


def summary(levels: list[float]) -> dict:
    values = np.array(levels)
    return {"mean": float(values.mean()), "std": float(values.std(ddof=1)), "levels": [round(x, 5) for x in levels]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--casatestdata", required=True, help="a local casatestdata copy")
    parser.add_argument("--casars-bin", required=True, help="casa-rs's simobserve task binary")
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--seeds", type=int, default=20)
    args = parser.parse_args()
    import casatasks
    import casatools
    from casatasks import simobserve

    tb, me = casatools.table(), casatools.measures()
    source = os.path.join(args.casatestdata, "unittest", "simobserve")
    seeds = list(range(1, args.seeds + 1))
    cases = {}
    for name, case in CASES.items():
        runs = {"casa": [], "casa-rs": []}
        samples = 0
        for seed in seeds:
            for implementation in runs:
                where = str(args.out / name / implementation)
                prepare(source, where, case["inputs"])
                params = dict(case["params"], seed=seed)
                if implementation == "casa":
                    run_casa(simobserve, where, params)
                else:
                    run_casars(args.casars_bin, where, params, me)
                value, samples = level(tb, where, case["params"]["project"])
                runs[implementation].append(value)
        casa, ours = summary(runs["casa"]), summary(runs["casa-rs"])
        standard_error = np.hypot(casa["std"], ours["std"]) / np.sqrt(len(seeds))
        cases[name] = {
            "samples_per_run": samples,
            "casa": casa,
            "casa-rs": ours,
            "mean_ratio": ours["mean"] / casa["mean"],
            "mean_difference_in_standard_errors": (ours["mean"] - casa["mean"]) / standard_error,
            "recorded": case["recorded"],
            "recorded_in_casa_standard_deviations": (case["recorded"] - casa["mean"]) / casa["std"],
        }
        print(f"{name}: casa {casa['mean']:.5f} ± {casa['std']:.5f}, casa-rs {ours['mean']:.5f} ± {ours['std']:.5f}",
              file=sys.stderr)
    receipt = {
        "schema": "simobserve-noise-seeds-receipt/1",
        "work_issue": 1053,
        "date": datetime.date.today().isoformat(),
        "oracle": {"casatasks": casatasks.version_string(), "casatools": casatools.version_string()},
        "native_revision": git("rev-parse", "HEAD"),
        "host": platform.platform(),
        "seeds": seeds,
        "command": "CASAPY tools/simulation/simobserve_noise_seeds.py --casatestdata LOCAL_COPY --casars-bin TARGET/release/simobserve --out OUT",
        "cases": cases,
    }
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "receipt.json").write_text(json.dumps(receipt, indent=1) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
