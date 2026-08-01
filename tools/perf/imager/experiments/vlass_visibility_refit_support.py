#!/usr/bin/env python3
"""Freeze the unique MT-MFS atom support selected by a casa-rs VLASS run."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import struct
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any


TRACE_PATTERN = re.compile(
    r"^mtmfs_component_trace "
    r".*scale_index=(?P<scale_index>\d+) "
    r"scale_pixels=(?P<scale_pixels>\S+) "
    r"x=(?P<x>\d+) y=(?P<y>\d+) "
    r".*coefficients=\[(?P<coefficients>[^\]]+)\]"
)


def f32(value: float) -> float:
    """Round one scalar exactly as a Rust f32 arithmetic result."""

    return struct.unpack("<f", struct.pack("<f", value))[0]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def parse_support(log_path: Path, *, gain: float, nterms: int) -> dict[str, Any]:
    if not math.isfinite(gain):
        raise ValueError("gain must be finite")
    if nterms < 1:
        raise ValueError("nterms must be positive")

    atoms: dict[tuple[int, int, int], dict[str, Any]] = {}
    scale_update_counts: dict[int, int] = defaultdict(int)
    trace_components = 0
    with log_path.open("r", encoding="utf-8", errors="strict") as source:
        for line_number, line in enumerate(source, start=1):
            if not line.startswith("mtmfs_component_trace "):
                continue
            match = TRACE_PATTERN.match(line.rstrip("\n"))
            if match is None:
                raise ValueError(
                    f"malformed mtmfs_component_trace at {log_path}:{line_number}"
                )
            coefficients = [
                float(value.strip())
                for value in match.group("coefficients").split(",")
            ]
            if len(coefficients) != nterms or not all(
                math.isfinite(value) for value in coefficients
            ):
                raise ValueError(
                    f"component at {log_path}:{line_number} has "
                    f"{len(coefficients)} finite coefficients; expected {nterms}"
                )
            scale_index = int(match.group("scale_index"))
            scale_pixels = float(match.group("scale_pixels"))
            x = int(match.group("x"))
            y = int(match.group("y"))
            key = (scale_index, x, y)
            atom = atoms.setdefault(
                key,
                {
                    "scale_index": scale_index,
                    "scale_pixels": scale_pixels,
                    "x": x,
                    "y": y,
                    "updates": 0,
                    "coalesced_term_deltas_f64": [0.0] * nterms,
                    "coalesced_term_deltas_f32_sequential": [0.0] * nterms,
                },
            )
            if atom["scale_pixels"] != scale_pixels:
                raise ValueError(f"atom {key} changed scale size within one trace")
            atom["updates"] += 1
            for term, coefficient in enumerate(coefficients):
                applied = f32(f32(gain) * f32(coefficient))
                atom["coalesced_term_deltas_f64"][term] += applied
                atom["coalesced_term_deltas_f32_sequential"][term] = f32(
                    atom["coalesced_term_deltas_f32_sequential"][term] + applied
                )
            trace_components += 1
            scale_update_counts[scale_index] += 1

    if trace_components == 0:
        raise ValueError(f"no mtmfs_component_trace records found in {log_path}")
    ordered_atoms = [atoms[key] for key in sorted(atoms)]
    scale_atom_counts: dict[int, int] = defaultdict(int)
    for atom in ordered_atoms:
        scale_atom_counts[atom["scale_index"]] += 1
    return {
        "schema_version": 1,
        "kind": "casa-rs-vlass-mtmfs-active-support",
        "role": "bounded-visibility-refit-discriminator-input",
        "source_log": {
            "path": str(log_path),
            "sha256": sha256_file(log_path),
        },
        "gain": gain,
        "nterms": nterms,
        "trace_components": trace_components,
        "unique_atoms": len(ordered_atoms),
        "scale_update_counts": {
            str(key): scale_update_counts[key] for key in sorted(scale_update_counts)
        },
        "scale_atom_counts": {
            str(key): scale_atom_counts[key] for key in sorted(scale_atom_counts)
        },
        "atoms": ordered_atoms,
    }


def write_json_exclusive(path: Path, value: dict[str, Any]) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite existing receipt: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(value, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--gain", type=float, default=0.1)
    parser.add_argument("--nterms", type=int, default=2)
    arguments = parser.parse_args()
    if not arguments.log.is_file():
        raise SystemExit(f"input log does not exist: {arguments.log}")
    receipt = parse_support(
        arguments.log,
        gain=arguments.gain,
        nterms=arguments.nterms,
    )
    write_json_exclusive(arguments.output, receipt)
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    main()
