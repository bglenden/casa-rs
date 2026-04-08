#!/usr/bin/env python3

"""Repeatable ratatui smoke harness for the shipped casars apps.

This checks that the terminal UI for the current app shells comes up and renders
the expected high-level structure:

- `msexplore` against a real MeasurementSet
- `tablebrowser` against a real MeasurementSet
- `calibrate` against a real MeasurementSet
- `imexplore` against a real shared CASA image fixture

The harness is intentionally lightweight. It verifies startup and shell-specific
surface markers rather than deep task correctness.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import pathlib
import pty
import re
import select
import subprocess
import struct
import sys
import time
import termios
from dataclasses import dataclass
from typing import Iterable


CSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
OSC_RE = re.compile(r"\x1b\][^\x07]*(?:\x07|\x1b\\)")
DCS_RE = re.compile(r"\x1bP.*?\x1b\\", re.DOTALL)
APC_RE = re.compile(r"\x1b_.*?\x1b\\", re.DOTALL)
PM_RE = re.compile(r"\x1b\^.*?\x1b\\", re.DOTALL)
SINGLE_ESC_RE = re.compile(r"\x1b[@-Z\\-_]")
CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")


@dataclass(frozen=True)
class SmokeCase:
    name: str
    command: list[str]
    expected: tuple[str, ...]
    env: dict[str, str] | None = None
    timeout_s: float = 8.0
    startup_input: bytes = b""


def repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent.parent


def strip_ansi(text: str) -> str:
    cleaned = text
    for pattern in (OSC_RE, DCS_RE, APC_RE, PM_RE, CSI_RE, SINGLE_ESC_RE):
        cleaned = pattern.sub("", cleaned)
    cleaned = CONTROL_RE.sub("", cleaned)
    return cleaned


def normalize_for_match(text: str) -> str:
    return re.sub(r"\s+", "", text)


def resolve_shared_ms(root: pathlib.Path) -> pathlib.Path:
    candidate_roots: list[pathlib.Path] = []
    env_root = os.environ.get("CASA_RS_TESTDATA_ROOT")
    if env_root:
        candidate_roots.append(pathlib.Path(env_root))
    candidate_roots.append((root / "../casatestdata").resolve())
    candidate_roots.append(pathlib.Path.home() / "SoftwareProjects/casatestdata")

    seen: set[pathlib.Path] = set()
    for base in candidate_roots:
        resolved = base.expanduser().resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        candidate = resolved / "measurementset/vla/ngc5921.ms"
        if candidate.exists():
            return candidate

    searched = "\n".join(f"  - {path}" for path in seen)
    raise SystemExit(
        "could not find shared ngc5921.ms fixture; searched:\n"
        f"{searched}\nSet CASA_RS_TESTDATA_ROOT to override."
    )


def resolve_shared_image() -> pathlib.Path:
    candidates = [
        pathlib.Path("/Volumes/home/casatestdata/image/n4826_bima.im"),
        pathlib.Path("/Volumes/home/casatestdata/image/test.clean.image"),
        pathlib.Path("/Volumes/home/casatestdata/image/ngc5921.clean.image"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate

    searched = "\n".join(f"  - {path}" for path in candidates)
    raise SystemExit(
        "could not find shared CASA image fixture; searched:\n"
        f"{searched}"
    )


def read_available(master_fd: int, deadline: float) -> str:
    chunks: list[bytes] = []
    while time.monotonic() < deadline:
        timeout = max(0.0, deadline - time.monotonic())
        ready, _, _ = select.select([master_fd], [], [], min(timeout, 0.2))
        if not ready:
            break
        try:
            chunk = os.read(master_fd, 65536)
        except OSError:
            break
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks).decode("utf-8", errors="replace")


def wait_for_markers(master_fd: int, process: subprocess.Popen[bytes], markers: Iterable[str], timeout_s: float) -> str:
    deadline = time.monotonic() + timeout_s
    wanted = tuple(markers)
    normalized_wanted = tuple(normalize_for_match(marker) for marker in wanted)
    raw = ""
    cleaned = ""
    while time.monotonic() < deadline:
        raw += read_available(master_fd, deadline)
        cleaned = strip_ansi(raw)
        normalized = normalize_for_match(cleaned)
        if all(marker in normalized for marker in normalized_wanted):
            return cleaned
        if process.poll() is not None:
            break
        time.sleep(0.05)
    code = process.poll()
    raise RuntimeError(
        f"timed out waiting for markers {wanted!r} (exit={code})\nCaptured output:\n{cleaned[-4000:]}"
    )


def terminate_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=1.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=1.0)


def run_case(root: pathlib.Path, case: SmokeCase) -> None:
    env = os.environ.copy()
    env.update(
        {
            "TERM": env.get("TERM", "xterm-256color"),
            "COLUMNS": "140",
            "LINES": "40",
        }
    )
    if case.env:
        env.update(case.env)

    master_fd, slave_fd = pty.openpty()
    fcntl.ioctl(slave_fd, termios.TIOCSWINSZ, struct_winsize(rows=40, cols=140))
    process = subprocess.Popen(
        case.command,
        cwd=root,
        env=env,
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        start_new_session=True,
    )
    os.close(slave_fd)
    try:
        if case.startup_input:
            os.write(master_fd, case.startup_input)
        wait_for_markers(master_fd, process, case.expected, case.timeout_s)
    finally:
        try:
            os.close(master_fd)
        except OSError:
            pass
        terminate_process(process)


def build_cases(root: pathlib.Path, ms_path: pathlib.Path, image_path: pathlib.Path | None) -> list[SmokeCase]:
    ms_str = str(ms_path)
    cases = [
        SmokeCase(
            name="msexplore",
            command=["cargo", "run", "-q", "-p", "casars", "--", "msexplore", ms_str],
            expected=("MeasurementSet Pa", "Presets", "Current Plot"),
            timeout_s=15.0,
        ),
        SmokeCase(
            name="tablebrowser",
            command=["cargo", "run", "-q", "-p", "casars", "--", "tablebrowser", ms_str],
            expected=("Tables / Table Browser", "Rows: 22653", "Type: MeasurementSet"),
            timeout_s=15.0,
        ),
        SmokeCase(
            name="calibrate",
            command=["cargo", "run", "-q", "-p", "casars", "--", "calibrate", ms_str],
            expected=("Calibration / Calibrate", "MeasurementSet", "Stages"),
            timeout_s=15.0,
        ),
    ]
    if image_path is not None:
        image_str = str(image_path)
        cases.append(
            SmokeCase(
                name="imexplore",
                command=["cargo", "run", "-q", "-p", "casars", "--", "imexplore", image_str],
                expected=("Images / ImExplore", "Status: ready", "View: Plane"),
                env={
                    "CASARS_ASSUME_BASIC_TERMINAL": "1",
                    "CASARS_IMEXPLORE_DISABLE_DIRECT_OVERLAY": "1",
                },
                timeout_s=10.0,
            )
        )
    return cases


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--app",
        choices=("msexplore", "tablebrowser", "calibrate", "imexplore", "all"),
        default="all",
        help="Run one app smoke case or all of them",
    )
    args = parser.parse_args()

    root = repo_root()
    ms_path = resolve_shared_ms(root)
    image_path = resolve_shared_image() if args.app in {"imexplore", "all"} else None
    cases = build_cases(root, ms_path, image_path)
    if args.app != "all":
        cases = [case for case in cases if case.name == args.app]

    failures: list[tuple[str, str]] = []
    print(f"Using MeasurementSet fixture: {ms_path}")
    if image_path is not None:
        print(f"Using image fixture: {image_path}")
    for case in cases:
        print(f"==> Smoke: {case.name}")
        sys.stdout.flush()
        try:
            run_case(root, case)
        except Exception as exc:  # noqa: BLE001
            failures.append((case.name, str(exc)))
            print(f"FAILED: {case.name}")
        else:
            print(f"OK: {case.name}")

    if failures:
        print("\nSmoke harness failures:")
        for name, message in failures:
            print(f"\n[{name}]\n{message}")
        return 1

    print("\nAll casars ratatui smoke checks passed.")
    return 0


def struct_winsize(*, rows: int, cols: int) -> bytes:
    return struct.pack("HHHH", rows, cols, 0, 0)


if __name__ == "__main__":
    raise SystemExit(main())
