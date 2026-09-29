#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run one existing guarded cube workload with explicit spatial backend/depth.

No data generation, CASA restart, parameter sweep, or acceptance relaxation.
"""

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("freeze", "run"))
    parser.add_argument("--records", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--build-log", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--template", type=Path)
    parser.add_argument("--guard", type=Path)
    parser.add_argument("--outputs", type=Path)
    parser.add_argument("--workers", type=int, choices=(1, 4), default=4)
    parser.add_argument("--depth", choices=("dirty", "shallow", "deep"), default="deep")
    parser.add_argument("--major-cycles", type=int, choices=(1, 2, 3), default=3,
                        help="shallow diagnostic stop; the default workload is unchanged")
    parser.add_argument("--native-memory-gib", type=int, choices=(2, 4, 16), default=16,
                        help="lower planning budget for the bounded multi-wave check")
    parser.add_argument("--metal", action="store_true")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[3]
    if args.action == "freeze":
        assert args.build_log and args.binary
        build_log = args.build_log.read_text()
        if not re.search(r"test result: ok\. 1 passed; 0 failed", build_log):
            raise RuntimeError("connected application qualification must pass before freezing")
        match = re.search(r"Running tests/continuum_application.rs \(([^)]+)\)", build_log)
        if not match:
            raise RuntimeError("successful application test executable missing from build log")
        source = Path(match[1])
        if not source.is_absolute():
            source = repo / source
        if args.binary.exists():
            raise FileExistsError(args.binary)
        shutil.copy2(source, args.binary)
        symbols = subprocess.check_output(["nm", str(args.binary)], text=True)
        neon = len(re.findall(r"_fftwf?_codelet_.*_neon$", symbols, re.MULTILINE))
        assert neon > 0, "matched optimized FFTW is required"
        snapshot = args.records / f"{args.label}-source"
        snapshot.mkdir()
        (snapshot / "tracked.patch").write_bytes(subprocess.check_output(
            ["git", "diff", "--binary", "HEAD"], cwd=repo))
        untracked = subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=repo
        ).decode().split("\0")
        for relative in filter(None, untracked):
            destination = snapshot / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(repo / relative, destination)
        with (args.records / f"{args.label}-build.json").open("x") as record:
            json.dump(dict(binary=str(args.binary), build_log=str(args.build_log),
                           checkpoint=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
                           source_snapshot=str(snapshot), uncommitted_candidate=True,
                           neon_codelets=neon), record, indent=2)
        print(f"Frozen {args.binary}, {neon} NEON FFTW codelets", flush=True)
        return
    assert args.binary and args.guard and args.template and args.outputs
    output = args.outputs / args.label
    if output.exists():
        raise FileExistsError(output)
    command = json.loads(args.template.read_text())["command"]
    test = command.index("t55_c_array_turnaround::contiguous_spectral_block")
    command[test - 1] = str(args.binary)
    replacements = {
        "CASA_RS_C_ARRAY_OUTPUT": str(output),
        "CASA_RS_C_ARRAY_WORKERS": str(args.workers),
        "CASA_RS_C_ARRAY_NITER": "32000" if args.depth == "shallow" else "640000",
        "CASA_RS_C_ARRAY_MEMORY_BYTES": str(args.native_memory_gib << 30),
    }
    command = [f"{part.split('=', 1)[0]}={replacements[part.split('=', 1)[0]]}"
               if "=" in part and part.split("=", 1)[0] in replacements else part
               for part in command]
    additions = []
    if args.metal:
        additions.append("CASA_RS_C_ARRAY_METAL=1")
    if args.depth == "dirty":
        additions.append("CASA_RS_C_ARRAY_DIRTY_ONLY=1")
    if args.depth == "shallow":
        additions.append(f"CASA_RS_C_ARRAY_MAX_MAJOR_CYCLES={args.major_cycles}")
    command[test - 1:test - 1] = additions
    result = subprocess.run([sys.executable, str(args.guard), "--records", str(args.records),
                             "--cwd", str(repo), "--label", args.label, "--rss-gib", "16",
                             "--", *command])
    result.check_returncode()


if __name__ == "__main__":
    main()
