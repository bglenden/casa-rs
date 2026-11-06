#!/usr/bin/env python3
"""Measure warm task-process latency for imhead and msconvert.

Build both binaries in release mode, then provide a small CASA image::

    cargo build --release -p casars-imexplore -p casars-msconvert --bins
    python scripts/bench-task-startup.py --image /path/to/image.im

The image and the repository's small MSv2 fixture are copied to a temporary
workspace. Times include process startup and the requested task. Run on an
otherwise idle machine, and compare results from the same host and build mode.
"""

import argparse
import json
import shutil
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MS_FIXTURE = ROOT / "crates/casa-ms/tests/fixtures/ms_trial/ms_trial.ms.tgz"


def measure(command, *, cwd, request, warmup, iterations, cleanup=None):
    elapsed = []
    for run in range(warmup + iterations):
        if cleanup is not None and cleanup.exists():
            shutil.rmtree(cleanup)
        start = time.perf_counter()
        completed = subprocess.run(
            command, cwd=cwd, input=request, text=True, capture_output=True, check=False
        )
        duration = (time.perf_counter() - start) * 1000
        if completed.returncode:
            raise RuntimeError(f"{command}: exit {completed.returncode}\n{completed.stderr}")
        if cleanup is not None and not cleanup.exists():
            raise RuntimeError(f"{command}: expected output {cleanup} was not created")
        if run >= warmup:
            elapsed.append(duration)
    return {"median_ms": round(statistics.median(elapsed), 3),
            "min_ms": round(min(elapsed), 3), "max_ms": round(max(elapsed), 3),
            "samples": len(elapsed)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", type=Path, required=True, help="small CASA image directory")
    parser.add_argument("--binary-dir", type=Path, default=ROOT / "target/release")
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--warmup", type=int, default=3)
    args = parser.parse_args()
    if args.iterations < 1 or args.warmup < 0:
        parser.error("iterations must be positive and warmup non-negative")
    binaries = args.binary_dir.resolve()
    with tempfile.TemporaryDirectory(prefix="casa-task-bench-") as directory:
        root = Path(directory)
        image = root / "image.im"
        shutil.copytree(args.image, image)
        subprocess.run(["tar", "-xzf", str(MS_FIXTURE), "-C", str(root)], check=True)
        ms = root / "ms_trial.ms"
        output = root / "converted.ps.zarr"
        request = json.dumps({"surface": "imhead", "values": {
            "imagename": str(image), "mode": "summary"}})
        cases = [
            ("imhead_help", [str(binaries / "imexplore"), "imhead", "--help"], None, None),
            ("imhead_summary", [str(binaries / "imexplore"), "imhead", "--json-run", "-"], request, None),
            ("msconvert_help", [str(binaries / "msconvert"), "--help"], None, None),
            ("msconvert_run", [str(binaries / "msconvert"), "--no-save-last", "--vis", str(ms),
                               "--outputvis", str(output)], None, output),
        ]
        results = {name: measure(cmd, cwd=root, request=stdin, warmup=args.warmup,
                                 iterations=args.iterations, cleanup=cleanup)
                   for name, cmd, stdin, cleanup in cases}
        print(json.dumps(results, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
