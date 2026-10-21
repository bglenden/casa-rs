#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Simulation Phase 1a (#841): casa-rs prediction against CASA's `sm.predict`.

For each case in the matrix below, this script:

1. runs `casa_sm_predict_oracle.py` under CASA's Python, which writes MSs
   predicted by CASA;
2. runs the casa-rs `predict_vs_casa` example on each MS;
3. writes every result to `summary.json` in the output directory.

Build the comparator first:

    cargo build --release -p casa-simulation --example predict_vs_casa
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
SIMMOS = pathlib.Path("~/.casa/data/alma/simmos").expanduser()

# name -> oracle arguments. alma-autocorr is a known gap: casa-rs does not
# predict autocorrelations yet (Phase 1d).
CASES: dict[str, list[str]] = {
    "alma-centred": [],
    "alma-offset": ["--offset-arcsec", "1.5", "-0.8"],
    "alma-crpix": ["--crpix-shift", "-10", "7"],
    "alma-narrow-mfs": ["--mfs-width-mhz", "20", "--no-vp"],
    "alma-250": ["--imsize", "250"],
    "alma-ghz-axis": ["--spectral-unit", "GHz"],
    "alma-autocorr": ["--autocorr", "--no-vp"],
    "vla-circular": [
        "--config", str(SIMMOS / "vla.d.cfg"),
        "--freq-ghz", "8.4",
        "--chan-width-mhz", "2",
        "--stokes", "RR LL",
        "--cell-arcsec", "1.0",
        "--dec", "+30d00m00.0",
        "--no-vp",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=pathlib.Path)
    parser.add_argument(
        "--casa-python",
        default=os.environ.get("CASA_RS_CASA_PYTHON", "/Applications/CASA.app/Contents/MacOS/python3"),
    )
    parser.add_argument("--comparator", required=True, type=pathlib.Path)
    parser.add_argument("--cases", nargs="*", default=list(CASES))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    summary = []
    for name in args.cases:
        case_dir = args.out / name
        subprocess.run(
            [args.casa_python, str(HERE / "casa_sm_predict_oracle.py"), "--out", str(case_dir), *CASES[name]],
            check=True,
            cwd=args.out,
            stdout=subprocess.DEVNULL,
        )
        manifest = json.loads((case_dir / "manifest.json").read_text(encoding="utf-8"))
        for case in manifest["cases"]:
            command = [str(args.comparator), case["ms"], case["model_input"]]
            if case["vp"]:
                command += ["--vp", "alma"]
            result = subprocess.run(command, check=False, capture_output=True, text=True)
            entry = {"case": name, "variant": case["name"], "casa_timings": case["timings"]}
            if result.returncode == 0:
                entry["casa_rs"] = json.loads(result.stdout)
            else:
                entry["error"] = result.stderr.strip()
            summary.append(entry)
            nrms = entry.get("casa_rs", {}).get("normalized_rms", "error")
            print(f"{name:16s} {case['name']:10s} nrms={nrms}")
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
