#!/usr/bin/env python3
"""Audit the installed CASA AW degrid complex-division code generation.

This is a read-only binary inspection.  It proves which ``__divsc3`` helper
the installed CASA 6.7.5.18 ``refim::AWVisResampler::GridToData`` calls and
records the ordinary-finite fast path's precision boundaries.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Iterable


GRID_TO_DATA_SYMBOL = (
    "__ZN4casa5refim14AWVisResampler10GridToDataERNS0_7VBStoreERKN8casacore"
    "5ArrayINSt3__17complexIfEEEE"
)
DIVSC3_SYMBOL = "___divsc3"
EXPECTED_FAST_PATH = (
    ("fcvt", "d31, s3"),
    ("fcvt", "d26, s1"),
    ("fcvt", "d27, s2"),
    ("fcvt", "d30, s0"),
    ("fmul", "d28, d31, d31"),
    ("fmul", "d29, d26, d31"),
    ("fmul", "d31, d30, d31"),
    ("fmadd", "d28, d27, d27, d28"),
    ("fmadd", "d29, d30, d27, d29"),
    ("fnmsub", "d31, d26, d27, d31"),
    ("fdiv", "d29, d29, d28"),
    ("fdiv", "d31, d31, d28"),
    ("fcvt", "s29, d29"),
)
INSTRUCTION_RE = re.compile(
    r"^(?P<address>[0-9a-f]{16})\s+"
    r"(?P<mnemonic>[A-Za-z0-9_.]+)"
    r"(?:\s+(?P<operands>.*))?$"
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def parse_functions(
    lines: Iterable[str],
    wanted: set[str],
) -> dict[str, list[dict[str, str]]]:
    functions: dict[str, list[dict[str, str]]] = {}
    current: str | None = None
    for raw_line in lines:
        line = raw_line.rstrip()
        if line.endswith(":") and not line.startswith((" ", "\t")):
            label = line[:-1]
            current = label if label in wanted else None
            if current is not None:
                functions[current] = []
            continue
        if current is None:
            continue
        match = INSTRUCTION_RE.match(line)
        if match is None:
            continue
        functions[current].append(
            {
                "address": match.group("address"),
                "mnemonic": match.group("mnemonic"),
                "operands": match.group("operands") or "",
            }
        )
    return functions


def disassemble_functions(
    library: Path,
    wanted: set[str],
) -> dict[str, list[dict[str, str]]]:
    process = subprocess.Popen(
        ("otool", "-tvV", str(library)),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    functions = parse_functions(process.stdout, wanted)
    _, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"otool failed with {process.returncode}: {stderr}")
    missing = sorted(wanted - functions.keys())
    if missing:
        raise RuntimeError(f"installed library lacks disassembly for {missing}")
    return functions


def audit(library: Path) -> dict[str, object]:
    functions = disassemble_functions(
        library,
        {GRID_TO_DATA_SYMBOL, DIVSC3_SYMBOL},
    )
    grid_instructions = functions[GRID_TO_DATA_SYMBOL]
    helper_instructions = functions[DIVSC3_SYMBOL]
    calls = [
        instruction
        for instruction in grid_instructions
        if instruction["mnemonic"] == "bl"
        and instruction["operands"].endswith(DIVSC3_SYMBOL)
    ]
    if len(calls) != 1:
        raise RuntimeError(
            "installed refim AW GridToData does not contain exactly one "
            f"direct {DIVSC3_SYMBOL} call"
        )
    actual_fast_path = tuple(
        (instruction["mnemonic"], instruction["operands"])
        for instruction in helper_instructions[: len(EXPECTED_FAST_PATH)]
    )
    if actual_fast_path != EXPECTED_FAST_PATH:
        raise RuntimeError("installed CASA __divsc3 ordinary-finite fast path changed")
    analyzer = Path(__file__).resolve()
    return {
        "schema": "casa-rs-vlass-casa-aw-division-codegen-audit-v1",
        "role": "read_only_installed_binary_correctness_evidence",
        "classification": ("official-casa-wide-intermediate-complex-division-codegen"),
        "casa_version": "6.7.5.18",
        "casa_source_commit": "418bb1a26df7c4aba663ff123b038b75a6fa0295",
        "library": {
            "path": str(library),
            "sha256": sha256_file(library),
            "bytes": library.stat().st_size,
        },
        "analyzer": {
            "path": str(analyzer),
            "sha256": sha256_file(analyzer),
        },
        "grid_to_data": {
            "symbol": GRID_TO_DATA_SYMBOL,
            "divsc3_call_count": len(calls),
            "divsc3_call": calls[0],
        },
        "divsc3": {
            "symbol": DIVSC3_SYMBOL,
            "ordinary_finite_fast_path": helper_instructions[: len(EXPECTED_FAST_PATH)],
            "input_boundary": "four_binary32_components_widened_to_binary64",
            "arithmetic": ("binary64_products_fused_sums_and_binary64_divisions"),
            "output_boundary": "each_component_narrowed_once_to_binary32",
            "scope": ("ordinary finite operands taking the audited leading fast path"),
        },
        "prohibited_work": {
            "casa_task_executed": False,
            "measurement_set_read": False,
            "prediction_executed": False,
            "grid_executed": False,
            "fft_executed": False,
            "products_formed": False,
            "clean_executed": False,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--library", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not args.library.is_file():
        raise RuntimeError(f"installed CASA library is missing: {args.library}")
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite receipt: {args.output}")
    result = audit(args.library)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
