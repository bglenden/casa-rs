#!/usr/bin/env python3
"""Plan and optionally remove generated external performance/parity data.

The default mode is a dry run. This tool intentionally targets generated run
trees only; source archives and canonical staged benchmark MeasurementSets are
kept unless a future, explicit policy adds them.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import shutil
import sys
from dataclasses import dataclass
from subprocess import CalledProcessError

from perf_harness.subprocesses import run_command


DEFAULT_ROOT = pathlib.Path("/Volumes/GLENDENNING")
DATED_RUN_RE = re.compile(r"^20\d{6}-.+")
M100_SPLIT_PARITY_RE = re.compile(r"^split-parity-20\d{6}T\d{6}Z$")


class CleanupError(Exception):
    """Error that should be reported without a traceback."""


@dataclass(frozen=True)
class Candidate:
    path: pathlib.Path
    reason: str


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=pathlib.Path,
        default=DEFAULT_ROOT,
        help="external disk root to inspect",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="remove the planned candidates; otherwise only print the plan",
    )
    parser.add_argument(
        "--include-issue175-runs",
        action="store_true",
        help="also remove old issue175 generated run data",
    )
    parser.add_argument(
        "--keep",
        action="append",
        default=[],
        help="candidate basename or absolute path to keep; may be repeated",
    )
    args = parser.parse_args()

    try:
        root = args.root.expanduser().resolve()
        if not root.exists():
            raise CleanupError(f"root does not exist: {root}")
        candidates = collect_candidates(root, include_issue175_runs=args.include_issue175_runs)
        candidates = apply_keep_filters(candidates, args.keep)
        print_plan(candidates, apply=args.apply)
        if args.apply:
            remove_candidates(candidates)
    except CleanupError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None


def collect_candidates(root: pathlib.Path, *, include_issue175_runs: bool) -> list[Candidate]:
    candidates: list[Candidate] = []

    parity_runs = (
        root
        / "casa-rs"
        / "tutorial-data"
        / "tutorial-parity"
        / "vla"
        / "flagging"
        / "parity-runs"
    )
    if parity_runs.is_dir():
        for child in sorted(parity_runs.iterdir()):
            if child.is_dir() and DATED_RUN_RE.match(child.name):
                candidates.append(
                    Candidate(
                        child,
                        "generated VLA flagging tutorial parity/performance run",
                    )
                )

    io_trace = root / "casa-rs-imperformance" / "io-trace"
    if io_trace.is_dir():
        candidates.append(Candidate(io_trace, "generated ImPerformance I/O trace data"))

    m100_work = (
        root
        / "casa-rs"
        / "tutorial-data"
        / "tutorial-parity"
        / "alma"
        / "m100"
        / "band3-combine"
        / "work"
    )
    if m100_work.is_dir():
        split_runs = sorted(
            child
            for child in m100_work.iterdir()
            if child.is_dir() and M100_SPLIT_PARITY_RE.match(child.name)
        )
        for child in split_runs[:-1]:
            candidates.append(
                Candidate(
                    child,
                    "superseded generated M100 split-parity work run",
                )
            )

    if include_issue175_runs:
        issue175 = root / "casa-rs" / "issue175-runs"
        if issue175.is_dir():
            candidates.append(Candidate(issue175, "generated issue175 parity run data"))

    return candidates


def apply_keep_filters(candidates: list[Candidate], keep_values: list[str]) -> list[Candidate]:
    if not keep_values:
        return candidates
    keep = {value for value in keep_values}
    resolved_keep = {
        str(pathlib.Path(value).expanduser().resolve())
        for value in keep_values
        if value.startswith("/") or value.startswith("~")
    }
    filtered = []
    for candidate in candidates:
        if candidate.path.name in keep or str(candidate.path.resolve()) in resolved_keep:
            continue
        filtered.append(candidate)
    return filtered


def print_plan(candidates: list[Candidate], *, apply: bool) -> None:
    mode = "apply" if apply else "dry-run"
    print(f"mode={mode}")
    if not candidates:
        print("no cleanup candidates found")
        return
    total_bytes = 0
    for candidate in candidates:
        size = du_size(candidate.path)
        total_bytes += size
        print(f"{human_size(size)}\t{candidate.path}\t# {candidate.reason}")
    print(f"planned_logical_total={human_size(total_bytes)}")


def du_size(path: pathlib.Path) -> int:
    try:
        completed = run_command(
            ["du", "-sk", str(path)],
            merge_stderr=False,
            check=True,
        )
    except CalledProcessError as error:
        raise CleanupError(f"du failed for {path}: {error}") from error
    return int(completed.stdout.split()[0]) * 1024


def human_size(size: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(size)
    unit = units[0]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            break
        value /= 1024.0
    if unit == "B":
        return f"{int(value)}{unit}"
    return f"{value:.1f}{unit}"


def remove_candidates(candidates: list[Candidate]) -> None:
    for candidate in candidates:
        if not candidate.path.exists():
            continue
        shutil.rmtree(candidate.path)
        print(f"removed\t{candidate.path}")


if __name__ == "__main__":
    main()
