#!/usr/bin/env python3
"""Freeze CASA's internal Briggs visibility-weight density for a benchmark row."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--phase-script", required=True, type=Path)
    parser.add_argument("--casa-python", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--evidence-json", required=True, type=Path)
    parser.add_argument("--weighting", default="briggs")
    parser.add_argument("--robust", default="1.0")
    args = parser.parse_args()

    for path in (args.receipt, args.phase_script, args.casa_python):
        if not path.exists():
            raise RuntimeError(f"required input is missing: {path}")
    for path in (args.output, args.evidence_json):
        if path.exists():
            raise RuntimeError(f"refusing to replace frozen diagnostic: {path}")

    receipt: dict[str, Any] = json.loads(args.receipt.read_text())
    benchmark_env = receipt.get("command", {}).get("env", {})
    dataset_path = receipt.get("dataset", {}).get("path")
    if not isinstance(benchmark_env, dict) or not dataset_path:
        raise RuntimeError("receipt lacks command.env or dataset.path")

    env = dict(os.environ)
    for name, value in benchmark_env.items():
        prefix = "IMAGER_BENCH_"
        if name.startswith(prefix):
            env[f"CASA_RS_BENCH_{name[len(prefix):]}"] = str(value)
    env.update(
        {
            "CASA_RS_BENCH_MS_PATH": str(dataset_path),
            "CASA_RS_BENCH_REPEATS": "1",
            "CASA_RS_BENCH_WARMUPS": "0",
            "CASA_RS_BENCH_WEIGHTING": args.weighting,
            "CASA_RS_BENCH_ROBUST": args.robust,
            "CASA_RS_BENCH_NITER": "0",
            "CASA_RS_BENCH_RESTORATION": "0",
            "CASA_RS_BENCH_CALCPSF": "0",
            "CASA_RS_BENCH_CALCRES": "0",
            "CASA_RS_BENCH_WEIGHT_DENSITY_OUTPUT": str(args.output),
            "CASA_RS_BENCH_WEIGHT_DENSITY_ONLY": "1",
        }
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.evidence_json.parent.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        [str(args.casa_python), str(args.phase_script)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="")
    completed.check_returncode()

    evidence = {
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "kind": "casa_weight_density_oracle",
        "source_receipt": str(args.receipt),
        "source_receipt_sha256": sha256(args.receipt),
        "phase_script": str(args.phase_script),
        "phase_script_sha256": sha256(args.phase_script),
        "casa_python": str(args.casa_python),
        "dataset_path": str(dataset_path),
        "output": str(args.output),
        "weighting": args.weighting,
        "robust": args.robust,
        "spw": env["CASA_RS_BENCH_SPW"],
        "field": env["CASA_RS_BENCH_FIELD"],
        "imsize": env["CASA_RS_BENCH_IMSIZE"],
        "cell_arcsec": env["CASA_RS_BENCH_CELL_ARCSEC"],
        "perchanweightdensity": env["CASA_RS_BENCH_PERCHANWEIGHTDENSITY"],
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
    args.evidence_json.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    print(f"evidence_json={args.evidence_json}")


if __name__ == "__main__":
    main()
