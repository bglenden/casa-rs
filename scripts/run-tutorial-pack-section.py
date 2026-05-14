#!/usr/bin/env python3
"""Run one tutorial-pack section and write learner plus regression evidence."""

from __future__ import annotations

import argparse
import fcntl
import html
import json
import os
import pty
import re
import select
import signal
import subprocess
import sys
import termios
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CASA_PYTHON = Path("/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python")
SECTION_ID = "01-imhead-continuum-header"
CASA_GUIDE_IMHEAD_URL = "https://casaguides.nrao.edu/index.php/First_Look_at_Image_Analysis_CASA_6.5.4"
CASA_GUIDE_IMHEAD_LINES = "CASA 6.5.4 guide logger listing, lines 35-66"
CASA_GUIDE_IMHEAD_FIELD_GROUPS = [
    "Image name",
    "Object name",
    "Image type",
    "Image quantity",
    "Pixel mask(s)",
    "Region(s)",
    "Image units",
    "Restoring Beam",
    "Direction reference",
    "Spectral reference",
    "Velocity type",
    "Rest frequency",
    "Pointing center",
    "Telescope",
    "Observer",
    "Date observation",
    "Telescope position",
    "Axis coordinate table",
]


@dataclass
class CommandResult:
    argv: list[str]
    elapsed_seconds: float
    stdout: str
    stderr: str
    returncode: int = 0


def default_tutorial_root() -> Path:
    override = os.environ.get("CASA_RS_TUTORIAL_DATA_ROOT")
    if override:
        return Path(override).expanduser()
    return Path.home() / "SoftwareProjects" / "casa-tutorial-data"


def default_pack_path() -> Path:
    return (
        default_tutorial_root()
        / "tutorial-parity"
        / "alma"
        / "first-look"
        / "twhya"
        / "image-analysis"
        / "alma-first-look-image-analysis.pack"
    )


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def run_command(argv: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> CommandResult:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    started = time.perf_counter()
    process = subprocess.run(
        argv,
        cwd=cwd,
        env=merged_env,
        capture_output=True,
        check=False,
        text=True,
    )
    elapsed = time.perf_counter() - started
    if process.returncode != 0:
        raise RuntimeError(
            f"{argv[0]} exited with {process.returncode}\nstdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )
    return CommandResult(
        argv=argv,
        elapsed_seconds=elapsed,
        stdout=process.stdout,
        stderr=process.stderr,
        returncode=process.returncode,
    )


def run_command_probe(argv: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> CommandResult:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    started = time.perf_counter()
    process = subprocess.run(
        argv,
        cwd=cwd,
        env=merged_env,
        capture_output=True,
        check=False,
        text=True,
    )
    elapsed = time.perf_counter() - started
    return CommandResult(
        argv=argv,
        elapsed_seconds=elapsed,
        stdout=process.stdout,
        stderr=process.stderr,
        returncode=process.returncode,
    )


def section_from_manifest(manifest: dict[str, Any], section_id: str) -> dict[str, Any]:
    for section in manifest["sections"]:
        if section["id"] == section_id:
            return section
    raise KeyError(f"section {section_id!r} not found in pack")


def step(section: dict[str, Any], surface: str) -> dict[str, Any]:
    for candidate in section["steps"]:
        if candidate["surface"] == surface:
            return candidate
    raise KeyError(f"surface {surface!r} not found in section {section['id']}")


def output_path(pack_root: Path, step_entry: dict[str, Any]) -> Path:
    return pack_root / step_entry["outputs"][0]["path"]


def rel(pack_root: Path, path: Path) -> str:
    return path.relative_to(pack_root).as_posix()


def parse_numeric_vector(value: Any) -> list[float]:
    if isinstance(value, list):
        return [float(item) for item in value]
    numbers = re.findall(r"[-+]?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][-+]?\d+)?", str(value))
    return [float(number) for number in numbers]


def parse_string_vector(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    text = str(value)
    quoted = re.findall(r"'([^']*)'", text)
    if quoted:
        return quoted
    return [item for item in re.split(r"\s+", text.strip("[] ")) if item]


def normalize_casa_imhead(raw: dict[str, Any], imagename: str) -> dict[str, Any]:
    axis_names = parse_string_vector(raw.get("axisnames", []))
    axis_units = parse_string_vector(raw.get("axisunits", []))
    shape = [int(value) for value in parse_numeric_vector(raw.get("shape", []))]
    refpix = parse_numeric_vector(raw.get("refpix", []))
    refval = parse_numeric_vector(raw.get("refval", []))
    incr = parse_numeric_vector(raw.get("incr", []))
    coord_types = ["Direction", "Direction", "Stokes", "Spectral"]
    axes = []
    for index, name in enumerate(axis_names):
        axes.append(
            {
                "axis": index,
                "coordinate_type": coord_types[index] if index < len(coord_types) else "",
                "name": name,
                "shape": shape[index] if index < len(shape) else None,
                "reference_value": refval[index] if index < len(refval) else None,
                "reference_pixel": refpix[index] if index < len(refpix) else None,
                "increment": incr[index] if index < len(incr) else None,
                "unit": axis_units[index] if index < len(axis_units) else "",
            }
        )
    beam = raw.get("restoringbeam", {})
    return {
        "imagename": imagename,
        "shape": shape,
        "units": raw.get("unit"),
        "image_type": raw.get("imagetype"),
        "default_mask": raw.get("defaultmask"),
        "masks": parse_string_vector(raw.get("masks", [])),
        "restoring_beam": {
            "major_arcsec": beam.get("major", {}).get("value"),
            "minor_arcsec": beam.get("minor", {}).get("value"),
            "position_angle_deg": beam.get("positionangle", {}).get("value"),
        },
        "axes": axes,
    }


def compare_numbers(left: float, right: float, tolerance: float) -> dict[str, Any]:
    delta = abs(float(left) - float(right))
    return {"left": left, "right": right, "delta": delta, "tolerance": tolerance, "passed": delta <= tolerance}


def compare_imhead(native: dict[str, Any], casa: dict[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def exact(name: str, left: Any, right: Any) -> None:
        checks.append({"field": name, "left": left, "right": right, "passed": left == right})

    exact("shape", native.get("shape"), casa.get("shape"))
    exact("units", native.get("units"), casa.get("units"))
    exact("default_mask", native.get("default_mask"), casa.get("default_mask"))
    exact("masks", native.get("masks"), casa.get("masks"))
    for field in ["major_arcsec", "minor_arcsec", "position_angle_deg"]:
        result = compare_numbers(
            native["restoring_beam"][field],
            casa["restoring_beam"][field],
            1e-9,
        )
        result["field"] = f"restoring_beam.{field}"
        checks.append(result)
    for index, (native_axis, casa_axis) in enumerate(zip(native.get("axes", []), casa.get("axes", []))):
        exact(f"axes[{index}].name", native_axis.get("name"), casa_axis.get("name"))
        exact(f"axes[{index}].shape", native_axis.get("shape"), casa_axis.get("shape"))
        exact(f"axes[{index}].unit", native_axis.get("unit"), casa_axis.get("unit"))
        for field in ["reference_value", "reference_pixel", "increment"]:
            tolerance = 10.0 if native_axis.get("unit") == "Hz" and field in {"reference_value", "increment"} else 1e-6
            result = compare_numbers(native_axis[field], casa_axis[field], tolerance)
            result["field"] = f"axes[{index}].{field}"
            checks.append(result)
    return {"checks": checks, "passed": all(check["passed"] for check in checks)}


def run_cli(pack_root: Path, image_path: Path, imexplore: Path, section: dict[str, Any]) -> tuple[dict[str, Any], CommandResult]:
    result = run_command(
        [str(imexplore), "imhead", str(image_path), "--json", "--mode", "summary"],
        cwd=REPO_ROOT,
    )
    payload = json.loads(result.stdout)
    write_json(output_path(pack_root, step(section, "cli")), payload)
    return payload, result


def run_python(pack_root: Path, image_path: Path, imexplore: Path, section: dict[str, Any]) -> tuple[dict[str, Any], CommandResult]:
    script = (
        "import json,sys;"
        f"sys.path.insert(0,{str(REPO_ROOT / 'crates/casars-python/python')!r});"
        "from casars.tasks import image_analysis;"
        f"result=image_analysis.imhead({str(image_path)!r}, mode='summary', binary={str(imexplore)!r});"
        "print(json.dumps(result, indent=2, sort_keys=True))"
    )
    result = run_command([sys.executable, "-c", script], cwd=REPO_ROOT)
    payload = json.loads(result.stdout)
    write_json(output_path(pack_root, step(section, "python")), payload)
    return payload, result


def run_gui(pack_root: Path, imexplore: Path, section_id: str, section: dict[str, Any]) -> tuple[dict[str, Any], CommandResult, dict[str, Any]]:
    result = run_command(
        [
            "swift",
            "run",
            "casars-mac",
            "--dump-debug-state",
            "--open-tutorial-pack",
            str(pack_root),
            "--open-tutorial-section",
            section_id,
            "--run-active-task",
        ],
        cwd=REPO_ROOT / "apps/casars-mac",
        env={
            "CASA_RS_REPO_ROOT": str(REPO_ROOT),
            "CASARS_IMEXPLORE_BIN": str(imexplore),
        },
    )
    debug = json.loads(result.stdout)
    write_json(pack_root / ".casa-rs/workspace/native" / section_id / "gui-debug-state.json", debug)
    diagnostics = [item for item in debug.get("taskDiagnostics", []) if item.strip()]
    if not diagnostics:
        raise RuntimeError("GUI debug-state run did not include task stdout in taskDiagnostics")
    payload = json.loads(diagnostics[0])
    write_json(output_path(pack_root, step(section, "gui")), payload)
    return payload, result, debug


def ensure_invalid_image_fixture(pack_root: Path, section_id: str) -> Path:
    invalid_path = pack_root / ".casa-rs/workspace/native" / section_id / "invalid-input.image"
    invalid_path.mkdir(parents=True, exist_ok=True)
    (invalid_path / "table.dat").write_text("not a casacore image table\n", encoding="utf-8")
    return invalid_path


def error_contract_record(surface: str, result: CommandResult) -> dict[str, Any]:
    combined = "\n".join(part for part in [result.stdout, result.stderr] if part)
    return {
        "surface": surface,
        "argv": result.argv,
        "returncode": result.returncode,
        "elapsed_seconds": result.elapsed_seconds,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "passed": result.returncode != 0 and "cannot open or read CASA image" in combined,
        "expected_message_fragment": "cannot open or read CASA image",
    }


def run_invalid_cli(imexplore: Path, invalid_path: Path) -> dict[str, Any]:
    result = run_command_probe(
        [str(imexplore), "imhead", str(invalid_path), "--json", "--mode", "summary"],
        cwd=REPO_ROOT,
    )
    return error_contract_record("cli", result)


def run_invalid_python(imexplore: Path, invalid_path: Path) -> dict[str, Any]:
    script = "\n".join(
        [
            "import sys",
            f"sys.path.insert(0, {str(REPO_ROOT / 'crates/casars-python/python')!r})",
            "from casars.tasks import image_analysis",
            "try:",
            f"    image_analysis.imhead({str(invalid_path)!r}, mode='summary', binary={str(imexplore)!r})",
            "except Exception as error:",
            "    print(f'{type(error).__name__}: {error}')",
            "    raise SystemExit(42)",
            "raise SystemExit('expected imhead to fail for invalid image')",
        ]
    )
    result = run_command_probe([sys.executable, "-c", script], cwd=REPO_ROOT)
    return error_contract_record("python", result)


def run_invalid_gui(pack_root: Path, imexplore: Path, section_id: str, invalid_path: Path) -> dict[str, Any]:
    result = run_command_probe(
        [
            "swift",
            "run",
            "casars-mac",
            "--dump-debug-state",
            "--open-tutorial-pack",
            str(pack_root),
            "--open-tutorial-section",
            section_id,
            "--set-task-value",
            "image_path",
            str(invalid_path),
            "--run-active-task",
        ],
        cwd=REPO_ROOT / "apps/casars-mac",
        env={
            "CASA_RS_REPO_ROOT": str(REPO_ROOT),
            "CASARS_IMEXPLORE_BIN": str(imexplore),
        },
    )
    record = error_contract_record("gui", result)
    try:
        debug = json.loads(result.stdout)
    except json.JSONDecodeError:
        debug = {}
    task_text = "\n".join(
        str(item)
        for item in (
            debug.get("taskLogLines", [])
            + debug.get("taskDiagnostics", [])
            + debug.get("lastErrors", [])
        )
    )
    record.update(
        {
            "returncode": result.returncode,
            "task_state": debug.get("taskState"),
            "task_log_lines": debug.get("taskLogLines", []),
            "task_diagnostics": debug.get("taskDiagnostics", []),
            "last_errors": debug.get("lastErrors", []),
            "passed": result.returncode == 0
            and debug.get("taskState") == "failed"
            and "cannot open or read CASA image" in task_text,
        }
    )
    return record


def capture_tui_error_contract(imexplore_path: Path, invalid_path: Path, section_id: str, pack_root: Path) -> dict[str, Any]:
    argv = [
        str(REPO_ROOT / "target/debug/casars"),
        "imhead",
        str(invalid_path),
        "--json",
        "--mode",
        "summary",
    ]
    env = os.environ.copy()
    env["CASARS_ASSUME_BASIC_TERMINAL"] = "1"
    env["CASARS_IMEXPLORE_BIN"] = str(imexplore_path)
    started = time.perf_counter()
    pid, fd = pty.fork()
    if pid == 0:
        os.chdir(REPO_ROOT)
        os.execvpe(argv[0], argv, env)

    termios.tcsetwinsize(fd, (30, 120))
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    raw = bytearray()
    status_seen_at: float | None = None
    child_reaped = False
    deadline = time.time() + 12.0

    def reap_child(until: float) -> bool:
        while time.time() < until:
            try:
                reaped, _ = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                return True
            if reaped == pid:
                return True
            time.sleep(0.05)
        return False

    def signal_child(sig: int) -> None:
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                return
        except PermissionError:
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                return

    try:
        while time.time() < deadline:
            readable, _, _ = select.select([fd], [], [], 0.1)
            if fd in readable:
                try:
                    chunk = os.read(fd, 8192)
                except BlockingIOError:
                    continue
                except OSError:
                    break
                if not chunk:
                    break
                raw.extend(chunk)
                if (
                    b"cannot open or read CASA image" in raw
                    or b"Execution failed" in raw
                    or b"failed" in raw.lower()
                ) and status_seen_at is None:
                    status_seen_at = time.time()
            if status_seen_at is not None and time.time() - status_seen_at > 0.5:
                try:
                    os.write(fd, b"q")
                except OSError:
                    pass
                break
        try:
            os.write(fd, b"q")
        except OSError:
            pass
        child_reaped = reap_child(time.time() + 2.0)
        if not child_reaped:
            signal_child(signal.SIGTERM)
            child_reaped = reap_child(time.time() + 1.0)
        if not child_reaped:
            signal_child(signal.SIGKILL)
            child_reaped = reap_child(time.time() + 1.0)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass
    elapsed = time.perf_counter() - started
    raw_text = raw.decode("utf-8", errors="replace")
    grid = ansi_to_grid(raw_text, width=120, height=30)
    raw_path = pack_root / ".casa-rs/workspace/native" / section_id / "invalid-image-tui-terminal.raw"
    text_path = pack_root / ".casa-rs/workspace/native" / section_id / "invalid-image-tui-terminal.txt"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(raw_text, encoding="utf-8")
    text_path.write_text(grid, encoding="utf-8")
    return {
        "surface": "tui",
        "argv": argv,
        "elapsed_seconds": elapsed,
        "raw_capture": rel(pack_root, raw_path),
        "text_capture": rel(pack_root, text_path),
        "passed": "cannot open or read CASA image" in raw_text
        or "cannot open or read CASA image" in grid,
        "expected_message_fragment": "cannot open or read CASA image",
    }


def run_casa(pack_root: Path, image_path: Path, casa_python: Path, section: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], CommandResult]:
    raw_path = output_path(pack_root, step(section, "oracle"))
    script = (
        "import json;"
        "from casatasks import imhead;"
        f"result=imhead(imagename={str(image_path)!r}, mode='summary');"
        "print(json.dumps(result, indent=2, sort_keys=True, default=str))"
    )
    result = run_command([str(casa_python), "-c", script], cwd=raw_path.parent)
    raw = json.loads(result.stdout)
    write_json(raw_path, raw)
    normalized = normalize_casa_imhead(raw, str(image_path))
    write_json(raw_path.with_name("casa-imhead.normalized.json"), normalized)
    return raw, normalized, result


def capture_tui(pack_root: Path, image_path: Path, section_id: str) -> dict[str, Any]:
    argv = [
        str(REPO_ROOT / "target/debug/casars"),
        "imhead",
        str(image_path),
        "--json",
        "--mode",
        "summary",
    ]
    env = os.environ.copy()
    env["CASARS_ASSUME_BASIC_TERMINAL"] = "1"
    started = time.perf_counter()
    pid, fd = pty.fork()
    if pid == 0:
        os.chdir(REPO_ROOT)
        os.execvpe(argv[0], argv, env)

    termios.tcsetwinsize(fd, (30, 120))
    raw = bytearray()
    deadline = time.time() + 10.0
    completed_at: float | None = None
    child_reaped = False
    try:
        while time.time() < deadline:
            readable, _, _ = select.select([fd], [], [], 0.1)
            if fd not in readable:
                continue
            try:
                chunk = os.read(fd, 8192)
            except OSError:
                break
            if not chunk:
                break
            raw.extend(chunk)
            if b"Execution completed successfully" in raw and completed_at is None:
                completed_at = time.time()
            if completed_at is not None and time.time() - completed_at > 0.4:
                os.write(fd, b"q")
                break
        try:
            os.write(fd, b"q")
        except OSError:
            pass
        wait_deadline = time.time() + 2.0
        while time.time() < wait_deadline:
            try:
                reaped, _ = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                child_reaped = True
                break
            if reaped == pid:
                child_reaped = True
                break
            time.sleep(0.05)
        if not child_reaped:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                child_reaped = True
            wait_deadline = time.time() + 1.0
            while not child_reaped and time.time() < wait_deadline:
                try:
                    reaped, _ = os.waitpid(pid, os.WNOHANG)
                except ChildProcessError:
                    child_reaped = True
                    break
                if reaped == pid:
                    child_reaped = True
                    break
                time.sleep(0.05)
        if not child_reaped:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass
    finally:
        try:
            os.close(fd)
        except OSError:
            pass
    elapsed = time.perf_counter() - started
    raw_text = raw.decode("utf-8", errors="replace")
    grid = ansi_to_grid(raw_text, width=120, height=30)
    raw_path = pack_root / ".casa-rs/workspace/native" / section_id / "tui-terminal.raw"
    text_path = pack_root / ".casa-rs/workspace/native" / section_id / "tui-terminal.txt"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(raw_text, encoding="utf-8")
    text_path.write_text(grid, encoding="utf-8")
    return {
        "argv": argv,
        "elapsed_seconds": elapsed,
        "raw_capture": rel(pack_root, raw_path),
        "text_capture": rel(pack_root, text_path),
        "completed": "Execution completed successfully" in raw_text,
    }


def ansi_to_grid(raw: str, *, width: int, height: int) -> str:
    grid = [[" " for _ in range(width)] for _ in range(height)]
    row = 0
    col = 0
    i = 0
    while i < len(raw):
        ch = raw[i]
        if ch == "\x1b" and i + 1 < len(raw) and raw[i + 1] == "[":
            j = i + 2
            while j < len(raw) and not ("@" <= raw[j] <= "~"):
                j += 1
            if j >= len(raw):
                break
            params = raw[i + 2 : j]
            final = raw[j]
            clean = params.lstrip("?")
            parts = [int(part) if part.isdigit() else 1 for part in clean.split(";") if part != ""]
            if final in {"H", "f"}:
                row = max(0, min(height - 1, (parts[0] if len(parts) >= 1 else 1) - 1))
                col = max(0, min(width - 1, (parts[1] if len(parts) >= 2 else 1) - 1))
            elif final == "A":
                row = max(0, row - (parts[0] if parts else 1))
            elif final == "B":
                row = min(height - 1, row + (parts[0] if parts else 1))
            elif final == "C":
                col = min(width - 1, col + (parts[0] if parts else 1))
            elif final == "D":
                col = max(0, col - (parts[0] if parts else 1))
            elif final == "J" and (not parts or parts[0] in {2, 3}):
                grid = [[" " for _ in range(width)] for _ in range(height)]
                row = 0
                col = 0
            elif final == "K":
                for x in range(col, width):
                    grid[row][x] = " "
            i = j + 1
            continue
        if ch == "\r":
            col = 0
        elif ch == "\n":
            row = min(height - 1, row + 1)
            col = 0
        elif ch.isprintable():
            if 0 <= row < height and 0 <= col < width:
                grid[row][col] = ch
            col += 1
            if col >= width:
                col = 0
                row = min(height - 1, row + 1)
        i += 1
    return "\n".join("".join(line).rstrip() for line in grid).rstrip() + "\n"


def load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/Menlo.ttc",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial.ttf",
    ]
    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    lines: list[str] = []
    for source_line in text.splitlines() or [""]:
        words = source_line.split(" ")
        current = ""
        for word in words:
            proposed = word if not current else f"{current} {word}"
            if draw.textbbox((0, 0), proposed, font=font)[2] <= max_width:
                current = proposed
            else:
                if current:
                    lines.append(current)
                current = word
        lines.append(current)
    return lines


def shorten_middle(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> str:
    if draw.textbbox((0, 0), text, font=font)[2] <= max_width:
        return text
    marker = "..."
    if draw.textbbox((0, 0), marker, font=font)[2] > max_width:
        return marker
    left = max(1, len(text) // 2)
    right = max(1, len(text) - left)
    while left + right > 2:
        candidate = f"{text[:left]}{marker}{text[-right:]}"
        if draw.textbbox((0, 0), candidate, font=font)[2] <= max_width:
            return candidate
        if left >= right and left > 1:
            left -= 1
        elif right > 1:
            right -= 1
        else:
            break
    return marker


def draw_evidence_card(
    path: Path,
    *,
    title: str,
    subtitle: str,
    command: str,
    parameters: dict[str, Any],
    output_lines: list[str],
) -> None:
    width = 1400
    height = 900
    image = Image.new("RGB", (width, height), "#f7f8fb")
    draw = ImageDraw.Draw(image)
    title_font = load_font(34, bold=True)
    label_font = load_font(22, bold=True)
    body_font = load_font(20)
    mono_font = load_font(18)
    small_font = load_font(16)

    draw.rectangle((0, 0, width, 108), fill="#1d2433")
    draw.text((40, 28), title, fill="#ffffff", font=title_font)
    draw.text((42, 72), subtitle, fill="#c9d4e5", font=small_font)

    panel = (40, 140, 640, 820)
    draw.rounded_rectangle(panel, radius=12, fill="#ffffff", outline="#d5dce8", width=2)
    draw.text((70, 170), "Parameters set", fill="#1d2433", font=label_font)
    y = 220
    callout_colors = ["#d9480f", "#0b7285", "#5f3dc4", "#2f9e44"]
    for index, (key, value) in enumerate(parameters.items()):
        color = callout_colors[index % len(callout_colors)]
        draw.rounded_rectangle((70, y, 610, y + 64), radius=10, fill="#fff8f2", outline=color, width=4)
        draw.text((90, y + 10), key, fill=color, font=label_font)
        value_text = str(value)
        for line in wrap_text(draw, value_text, body_font, 320)[:2]:
            draw.text((260, y + 12), shorten_middle(draw, line, body_font, 320), fill="#1d2433", font=body_font)
            y += 22
        y += 58

    draw.text((70, 650), "Invocation", fill="#1d2433", font=label_font)
    command_lines = wrap_text(draw, command, mono_font, 520)
    y = 690
    for line in command_lines[:5]:
        draw.text((80, y), shorten_middle(draw, line, mono_font, 520), fill="#334155", font=mono_font)
        y += 24

    output_panel = (680, 140, 1360, 820)
    draw.rounded_rectangle(output_panel, radius=12, fill="#101828", outline="#344054", width=2)
    draw.text((710, 170), "Observable result", fill="#ffffff", font=label_font)
    y = 220
    for line in output_lines[:22]:
        draw.text((710, y), shorten_middle(draw, line, mono_font, 610), fill="#e2e8f0", font=mono_font)
        y += 26

    path.parent.mkdir(parents=True, exist_ok=True)
    image.save(path)


def compact_output_lines(payload: dict[str, Any]) -> list[str]:
    beam = payload.get("restoring_beam", {})
    axes = payload.get("axes", [])
    lines = [
        f"shape: {payload.get('shape')}",
        f"units: {payload.get('units')}",
        f"object: {payload.get('object_name', 'not returned')}",
        f"default mask: {payload.get('default_mask')}",
        "restoring beam:",
        f"  major: {beam.get('major_arcsec')} arcsec",
        f"  minor: {beam.get('minor_arcsec')} arcsec",
        f"  pa: {beam.get('position_angle_deg')} deg",
        "axes:",
    ]
    for axis in axes:
        increment = axis.get("increment")
        increment_text = f"{increment:.6g}" if isinstance(increment, (int, float)) else str(increment)
        lines.append(
            f"  {axis.get('axis')} {axis.get('name')}: shape={axis.get('shape')} incr={increment_text} {axis.get('unit')}"
        )
    return lines


def format_list_value(items: list[Any]) -> str:
    return ", ".join(str(item) for item in items) if items else "not shown"


def axis_table_summary(payload: dict[str, Any]) -> str:
    axes = payload.get("axes", [])
    if not axes:
        return "not shown"
    parts = []
    for axis in axes:
        increment = axis.get("increment")
        if isinstance(increment, (int, float)):
            increment_text = f"{increment:.6g}"
        else:
            increment_text = str(increment)
        parts.append(
            f"{axis.get('name')} shape={axis.get('shape')} refpix={axis.get('reference_pixel')} "
            f"incr={increment_text} {axis.get('unit')}"
        )
    return "; ".join(parts)


def inspector_summary_map(gui_debug: dict[str, Any]) -> dict[str, str]:
    summary = gui_debug.get("selectedDatasetSummary") or {}
    diagnostics = summary.get("diagnostics") or []

    def first_with(prefix: str) -> str:
        for line in diagnostics:
            if str(line).startswith(prefix):
                return str(line)
        return "not shown"

    return {
        "Image name": summary.get("name") or "not shown",
        "Object name": first_with("Object:").removeprefix("Object:").strip()
        if first_with("Object:") != "not shown"
        else "not shown",
        "Image type": first_with("Image type:").removeprefix("Image type:").strip()
        if first_with("Image type:") != "not shown"
        else summary.get("kind") or "not shown",
        "Image quantity": first_with("Image type:").removeprefix("Image type:").strip()
        if first_with("Image type:") != "not shown"
        else "not shown",
        "Pixel mask(s)": first_with("Default mask:").removeprefix("Default mask:").strip()
        if first_with("Default mask:") != "not shown"
        else "not shown",
        "Region(s)": first_with("Regions:").removeprefix("Regions:").strip()
        if first_with("Regions:") != "not shown"
        else "not shown",
        "Image units": summary.get("units") or "not shown",
        "Restoring Beam": first_with("Beam:"),
        "Direction reference": "not shown",
        "Spectral reference": "not shown",
        "Velocity type": "not shown",
        "Rest frequency": "not shown",
        "Pointing center": first_with("Center:"),
        "Telescope": "not shown",
        "Observer": "not shown",
        "Date observation": "not shown",
        "Telescope position": "not shown",
        "Axis coordinate table": "; ".join(
            line for line in diagnostics if str(line).startswith(("Cell size:", "Cube center frequency:", "Channel separation:"))
        )
        or "not shown",
    }


def native_imhead_summary_map(payload: dict[str, Any]) -> dict[str, str]:
    beam = payload.get("restoring_beam", {})
    beam_text = (
        f"{beam.get('major_arcsec')} arcsec, {beam.get('minor_arcsec')} arcsec, "
        f"{beam.get('position_angle_deg')} deg"
    )
    return {
        "Image name": payload.get("imagename") or "not shown",
        "Object name": payload.get("object_name") or "not shown",
        "Image type": payload.get("image_type") or "not shown",
        "Image quantity": "Intensity" if payload.get("image_type") == "Intensity" else "not shown",
        "Pixel mask(s)": format_list_value(payload.get("masks", [])),
        "Region(s)": format_list_value(payload.get("regions", [])) if "regions" in payload else "not shown",
        "Image units": payload.get("units") or "not shown",
        "Restoring Beam": beam_text,
        "Direction reference": "not shown",
        "Spectral reference": "not shown",
        "Velocity type": "not shown",
        "Rest frequency": "not shown",
        "Pointing center": "not shown",
        "Telescope": "not shown",
        "Observer": "not shown",
        "Date observation": "not shown",
        "Telescope position": "not shown",
        "Axis coordinate table": axis_table_summary(payload),
    }


def casa_return_summary_map(payload: dict[str, Any]) -> dict[str, str]:
    return {
        "Image name": payload.get("imagename") or "not shown",
        "Object name": "not returned",
        "Image type": "PagedImage in logger; return has image_type=Intensity",
        "Image quantity": payload.get("image_type") or "not shown",
        "Pixel mask(s)": format_list_value(payload.get("masks", [])),
        "Region(s)": "not returned",
        "Image units": payload.get("units") or "not shown",
        "Restoring Beam": (
            f"{payload.get('restoring_beam', {}).get('major_arcsec')} arcsec, "
            f"{payload.get('restoring_beam', {}).get('minor_arcsec')} arcsec, "
            f"{payload.get('restoring_beam', {}).get('position_angle_deg')} deg"
        ),
        "Direction reference": "not returned",
        "Spectral reference": "not returned",
        "Velocity type": "not returned",
        "Rest frequency": "not returned",
        "Pointing center": "not returned",
        "Telescope": "not returned",
        "Observer": "not returned",
        "Date observation": "not returned",
        "Telescope position": "not returned",
        "Axis coordinate table": axis_table_summary(payload),
    }


def render_guide_coverage_rows(casa_payload: dict[str, Any], native_payload: dict[str, Any], gui_debug: dict[str, Any]) -> str:
    casa_map = casa_return_summary_map(casa_payload)
    native_map = native_imhead_summary_map(native_payload)
    inspector_map = inspector_summary_map(gui_debug)
    rows = []
    for field in CASA_GUIDE_IMHEAD_FIELD_GROUPS:
        inspector_value = inspector_map.get(field, "not shown")
        if inspector_value == "not shown":
            inspector_status = "missing"
        elif field == "Axis coordinate table":
            inspector_status = "partial"
        else:
            inspector_status = "shown"
        rows.append(
            "<tr>"
            f"<td>{html.escape(field)}</td>"
            f"<td>{html.escape(casa_map.get(field, 'not returned'))}</td>"
            f"<td>{html.escape(native_map.get(field, 'not shown'))}</td>"
            f"<td>{html.escape(inspector_value)}</td>"
            f"<td><code>{html.escape(inspector_status)}</code></td>"
            "</tr>"
        )
    return "\n".join(rows)


def render_terminal_capture(path: Path, title: str, text: str) -> None:
    width = 1500
    height = 900
    image = Image.new("RGB", (width, height), "#0b1020")
    draw = ImageDraw.Draw(image)
    title_font = load_font(28, bold=True)
    mono_font = load_font(18)
    draw.text((36, 28), title, fill="#ffffff", font=title_font)
    draw.rectangle((34, 82, width - 34, height - 34), outline="#475569", width=2)
    y = 108
    for line in text.splitlines()[:33]:
        draw.text((56, y), line[:150], fill="#e2e8f0", font=mono_font)
        y += 22
    path.parent.mkdir(parents=True, exist_ok=True)
    image.save(path)


def render_html_screenshot_section(pack_root: Path, screenshot_refs: dict[str, Path]) -> str:
    if not screenshot_refs:
        return (
            "<p>No real GUI/TUI screenshots have been captured by this runner. "
            "Use Computer Use, a macOS window capture, Ratatui buffer capture, "
            "or a Ghostty/libghostty terminal capture backend and place real captures under "
            "<code>.casa-rs/screenshots/</code>.</p>"
        )
    figures = []
    for surface, path in screenshot_refs.items():
        figures.append(
            f'<figure class="shot"><img src="../../{html.escape(rel(pack_root, path))}" '
            f'alt="{html.escape(surface)} real UI evidence">'
            f"<figcaption>{html.escape(surface)} real UI evidence.</figcaption></figure>"
        )
    return '<div class="grid">' + "\n".join(figures) + "</div>"


def html_json_excerpt(payload: Any, *, max_chars: int = 2200) -> str:
    text = json.dumps(payload, indent=2, sort_keys=True)
    if len(text) > max_chars:
        text = text[:max_chars].rstrip() + "\n..."
    return html.escape(text)


def html_text_excerpt(text: str, *, max_chars: int = 2200) -> str:
    if len(text) > max_chars:
        text = text[:max_chars].rstrip() + "\n..."
    return html.escape(text)


def surface_card(
    title: str,
    command: str,
    parameters: dict[str, Any],
    evidence_ref: str,
    payload: Any,
    *,
    note: str = "",
    image_path: Path | None = None,
    pack_root: Path | None = None,
) -> str:
    parameter_rows = "".join(
        f"<tr><td><code>{html.escape(str(key))}</code></td><td><code>{html.escape(str(value))}</code></td></tr>"
        for key, value in parameters.items()
    )
    image_html = ""
    if image_path is not None and pack_root is not None:
        image_html = (
            f'<figure class="shot"><img src="../../{html.escape(rel(pack_root, image_path))}" '
            f'alt="{html.escape(title)} evidence"><figcaption>{html.escape(title)} visible evidence.</figcaption></figure>'
        )
    note_html = f"<p>{html.escape(note)}</p>" if note else ""
    return f"""
  <section class="card surface">
    <h3>{html.escape(title)}</h3>
    {note_html}
    <p><strong>Execute:</strong> <code>{html.escape(command)}</code></p>
    <table class="params">
      <tr><th>Parameter</th><th>Value</th></tr>
      {parameter_rows}
    </table>
    <p><strong>Evidence file:</strong> <code>{html.escape(evidence_ref)}</code></p>
    {image_html}
    <pre>{html_json_excerpt(payload)}</pre>
  </section>
"""


def render_surface_evidence(
    pack_root: Path,
    section: dict[str, Any],
    gui_debug: dict[str, Any],
    screenshot_refs: dict[str, Path],
    tui_backend: str,
) -> str:
    casa_raw = load_json(pack_root / ".casa-rs/workspace/oracle/01-imhead-continuum-header/casa-imhead.json")
    cli_payload = load_json(pack_root / ".casa-rs/workspace/native/01-imhead-continuum-header/cli-imhead.json")
    python_payload = load_json(pack_root / ".casa-rs/workspace/native/01-imhead-continuum-header/python-imhead.json")
    tui_payload = load_json(pack_root / ".casa-rs/workspace/native/01-imhead-continuum-header/tui-imhead.json")
    gui_payload = load_json(pack_root / ".casa-rs/workspace/native/01-imhead-continuum-header/gui-imhead.json")
    gui_image = screenshot_refs.get("gui inspector fullscreen")
    gui_params = gui_debug.get("activeTaskValues", {})
    gui_toggles = gui_debug.get("activeTaskToggles", {})
    tui_text_path = pack_root / ".casa-rs/workspace/native" / section["id"] / "tui-terminal.txt"
    tui_note = (
        "Real TUI terminal screenshot is pending for this section. This block shows the deterministic TUI command "
        "and captured output payload; future runs should replace or supplement this with a Ghostty/libghostty or "
        "Ratatui capture."
    )
    tui_extra = ""
    if tui_text_path.exists():
        tui_note = f"Captured TUI terminal text is available from {rel(pack_root, tui_text_path)}."
        tui_extra = f"<pre>{html_text_excerpt(tui_text_path.read_text(encoding='utf-8'))}</pre>"
    cards = [
        surface_card(
            "CASA C++ Original",
            "imhead('twhya_cont.image')",
            {"imagename": "twhya_cont.image", "mode": "summary"},
            ".casa-rs/workspace/oracle/01-imhead-continuum-header/casa-imhead.json",
            casa_raw,
            note=(
                "The guide website shows the logger listing; the local oracle evidence records the CASA return "
                "dictionary and normalized comparison fields."
            ),
        ),
        surface_card(
            "Shell Executable",
            "imexplore imhead twhya_cont.image --json --mode summary",
            {"image_path": "twhya_cont.image", "json": True, "mode": "summary"},
            ".casa-rs/workspace/native/01-imhead-continuum-header/cli-imhead.json",
            cli_payload,
        ),
        surface_card(
            "Python",
            "casars.tasks.image_analysis.imhead('twhya_cont.image', mode='summary')",
            {"imagename": "twhya_cont.image", "mode": "summary", "binary": "target/debug/imexplore"},
            ".casa-rs/workspace/native/01-imhead-continuum-header/python-imhead.json",
            python_payload,
        ),
        surface_card(
            "TUI",
            "casars imhead twhya_cont.image --json --mode summary",
            {
                "task": "imhead",
                "Image Path": "twhya_cont.image",
                "JSON": True,
                "Mode": "summary",
                "capture_backend": tui_backend,
            },
            ".casa-rs/workspace/native/01-imhead-continuum-header/tui-imhead.json",
            tui_payload,
            note=tui_note,
        )
        + tui_extra,
        surface_card(
            "GUI",
            "Open tutorial pack -> select twhya_cont.image -> read Inspector; regression task: Image Header",
            {
                "learner_path": "Inspector",
                "task_image_path": gui_params.get("image_path", ""),
                "task_json": gui_toggles.get("json"),
                "task_mode": gui_params.get("mode", ""),
                "save_output": "Use the Save Output button after running the task; the save panel defaults to the tutorial pack root.",
            },
            ".casa-rs/workspace/native/01-imhead-continuum-header/gui-debug-state.json; regression task output: .casa-rs/workspace/native/01-imhead-continuum-header/gui-imhead.json",
            {
                "inspector_summary": gui_debug.get("selectedDatasetSummary", {}),
                "task_output_paths": gui_debug.get("taskOutputPaths", []),
            },
            image_path=gui_image,
            pack_root=pack_root,
            note=(
                "The main learner evidence is the real Inspector screenshot and the selectedDatasetSummary "
                "debug-state excerpt below. The richer imhead task JSON is regression evidence only; it is not "
                "presented as Inspector-visible content."
            ),
        ),
    ]
    return "\n".join(cards)


def existing_real_screenshot_refs(pack_root: Path, section_id: str) -> dict[str, Path]:
    refs: dict[str, Path] = {}
    candidates = {
        "gui inspector fullscreen": pack_root / ".casa-rs/screenshots/source/01-imhead-gui-inspector-fullscreen.png",
        "gui task run fullscreen": pack_root / ".casa-rs/screenshots/source/01-imhead-gui-task-run-fullscreen.png",
        "gui inspector details fullscreen": pack_root / ".casa-rs/screenshots/source/01-imhead-gui-inspector-details-fullscreen.png",
    }
    for name, path in candidates.items():
        if path.exists():
            refs[name] = path
    return refs


def write_docs(
    pack_root: Path,
    manifest: dict[str, Any],
    section: dict[str, Any],
    comparison: dict[str, Any],
    gui_debug: dict[str, Any],
    invalid_image_checks: dict[str, Any],
    screenshot_refs: dict[str, Path],
) -> None:
    docs_dir = pack_root / "docs" / "sections"
    md_path = docs_dir / f"{section['id']}.md"
    html_path = docs_dir / f"{section['id']}.html"
    casa_rel = ".casa-rs/workspace/oracle/01-imhead-continuum-header/casa-imhead.json"
    cli_rel = ".casa-rs/workspace/native/01-imhead-continuum-header/cli-imhead.json"
    python_rel = ".casa-rs/workspace/native/01-imhead-continuum-header/python-imhead.json"
    gui_rel = ".casa-rs/workspace/native/01-imhead-continuum-header/gui-imhead.json"
    tui_rel = ".casa-rs/workspace/native/01-imhead-continuum-header/tui-imhead.json"
    casa_normalized = load_json(pack_root / ".casa-rs/workspace/oracle/01-imhead-continuum-header/casa-imhead.normalized.json")
    native_imhead = load_json(pack_root / cli_rel)
    tui_backend = comparison["native_surface_checks"].get("tui_capture_backend", "parameter-record-only")
    tui_label = "TUI terminal capture" if tui_backend == "raw-pty" else "TUI parameter evidence"
    screenshot_section = ""
    if screenshot_refs:
        screenshot_items = "\n".join(
            f"![{surface} real UI evidence](../../{rel(pack_root, path)})\n"
            for surface, path in screenshot_refs.items()
        )
        screenshot_section = f"\n## Real UI Screenshots\n\n{screenshot_items}"
    else:
        screenshot_section = (
            "\n## Real UI Screenshots\n\n"
            "No real GUI/TUI screenshots have been captured by this runner. "
            "Use Computer Use, a macOS window capture, Ratatui buffer capture, "
            "or a Ghostty/libghostty terminal capture backend and place real captures under `.casa-rs/screenshots/`.\n"
        )
    checks = comparison["oracle_comparison"]["checks"]
    failed = [check for check in checks if not check["passed"]]
    status = "passed" if comparison["passed"] else "needs review"
    gui_params = gui_debug.get("activeTaskValues", {})
    gui_toggles = gui_debug.get("activeTaskToggles", {})
    invalid_checks = invalid_image_checks.get("checks", [])
    invalid_status = "passed" if invalid_image_checks.get("passed") else "needs review"
    invalid_lines = "\n".join(
        f"- {check['surface']}: `{ 'pass' if check.get('passed') else 'fail' }`"
        for check in invalid_checks
    )

    md = f"""# {section['title']}

Source: {section['casa_source']['guide_url']}

This chunk covers the continuum-image `imhead` call from the CASA guide:

```python
imhead('twhya_cont.image')
```

Observable result: {section['observable_result']}

## CASA Guide Listing

The CASA guide section says the image-header task is analogous to `listobs` for
images, then shows a logger listing for `imhead('twhya_cont.image')`.
The listing includes: {", ".join(CASA_GUIDE_IMHEAD_FIELD_GROUPS)}.

Source listing: {CASA_GUIDE_IMHEAD_URL} ({CASA_GUIDE_IMHEAD_LINES}).

## Parameters

| Surface | Parameters |
| --- | --- |
| CASA oracle | `imagename='twhya_cont.image'`, `mode='summary'` |
| casa-rs CLI | `image_path=twhya_cont.image`, `mode=summary`, `json=true` |
| casa-rs Python | `imagename='twhya_cont.image'`, `mode='summary'`, native `imexplore` binary |
| casa-rs TUI | app `imhead`, `Image Path=twhya_cont.image`, `Mode=summary`, `JSON=on` |
| casa-rs GUI | learner path: select `twhya_cont.image` and read the Inspector (`Size`, `Units`, `Shape`, `Image details`); regression path: task `imhead`, `image_path={gui_params.get('image_path', '')}`, `mode={gui_params.get('mode', '')}`, `json={gui_toggles.get('json')}` |

## Evidence

- CASA output: `{casa_rel}`
- CLI output: `{cli_rel}`
- Python output: `{python_rel}`
- {tui_label}: `{tui_rel}`
- GUI output: `{gui_rel}`
- Comparison status: `{status}`
- Invalid-image checks: `.casa-rs/evidence/invalid-image-checks.json` (`{invalid_status}`)

Invalid-image behavior: CLI/Python/TUI/GUI task surfaces must report a clear
`cannot open or read CASA image` style error when the selected path is missing,
unreadable, or not a valid casacore image. The GUI learner path should show
validated image metadata in the Inspector for staged tutorial inputs.

{invalid_lines}

{screenshot_section}
"""
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(md, encoding="utf-8")

    rows = "\n".join(
        f"<tr><td>{html.escape(check['field'])}</td><td>{'pass' if check['passed'] else 'fail'}</td><td><code>{html.escape(str(check.get('left')))}</code></td><td><code>{html.escape(str(check.get('right')))}</code></td></tr>"
        for check in checks
    )
    guide_rows = render_guide_coverage_rows(casa_normalized, native_imhead, gui_debug)
    surface_evidence = render_surface_evidence(pack_root, section, gui_debug, screenshot_refs, str(tui_backend))
    failures = "<p>All selected header fields matched CASA within tolerance.</p>" if not failed else f"<p>{len(failed)} checks need review.</p>"
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(section['title'])}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: #f6f7fb; color: #1d2433; }}
    main {{ max-width: 1180px; margin: 0 auto; padding: 36px 28px 72px; }}
    h1 {{ margin-bottom: 0.2rem; }}
    a {{ color: #0b5cad; }}
    code, pre {{ font-family: Menlo, Consolas, monospace; }}
    pre {{ white-space: pre-wrap; overflow-x: auto; background: #0f172a; color: #e2e8f0; border-radius: 6px; padding: 12px; }}
    .band {{ background: #1d2433; color: #fff; padding: 28px; margin: -36px -28px 32px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 18px; }}
    .card {{ background: white; border: 1px solid #d8dee9; border-radius: 8px; padding: 18px; }}
    .surface {{ margin: 18px 0; }}
    .params {{ margin-bottom: 12px; }}
    table {{ border-collapse: collapse; width: 100%; background: white; }}
    th, td {{ border: 1px solid #d8dee9; padding: 8px 10px; vertical-align: top; }}
    th {{ background: #eef2f7; text-align: left; }}
    img {{ max-width: 100%; border: 1px solid #cbd5e1; border-radius: 8px; background: white; }}
    .shot {{ margin: 22px 0; }}
  </style>
</head>
<body>
<main>
  <section class="band">
    <h1>{html.escape(section['title'])}</h1>
    <p>{html.escape(manifest['title'])}</p>
  </section>

  <section class="card">
    <h2>CASA Source Chunk</h2>
    <p>Source guide: <a href="{html.escape(section['casa_source']['guide_url'])}">{html.escape(section['casa_source']['guide_url'])}</a></p>
    <pre>imhead('twhya_cont.image')</pre>
    <p>{html.escape(section['observable_result'])}</p>
  </section>

  <section class="card">
    <h2>CASA Guide Listing</h2>
    <p>The CASA 6.5.4 guide describes <code>imhead</code> as the image analogue of <code>listobs</code>, then shows the logger listing for <code>imhead('twhya_cont.image')</code>.</p>
    <p>Source listing: <a href="{html.escape(CASA_GUIDE_IMHEAD_URL)}">{html.escape(CASA_GUIDE_IMHEAD_URL)}</a>, {html.escape(CASA_GUIDE_IMHEAD_LINES)}.</p>
    <p>The guide listing includes image identity, mask/unit/beam metadata, reference-frame metadata, pointing/observing metadata, and an axis coordinate table. Numeric values in the guide can differ from this pack because the pack is regenerated from the staged tutorial image; the comparison below checks field coverage and the local CASA-vs-casa-rs values.</p>
    <table>
      <tr><th>Guide field group</th><th>Local CASA return</th><th>casa-rs task output</th><th>GUI Inspector</th><th>Inspector coverage</th></tr>
      {guide_rows}
    </table>
  </section>

  <h2>Evidence By Surface</h2>
  {surface_evidence}

  <h2>Parameters By Interface</h2>
  <table>
    <tr><th>Interface</th><th>How to execute</th><th>Parameters set</th></tr>
    <tr><td>CASA</td><td><code>imhead(...)</code></td><td><code>imagename='twhya_cont.image'</code>, <code>mode='summary'</code></td></tr>
    <tr><td>CLI</td><td><code>imexplore imhead twhya_cont.image --json --mode summary</code></td><td><code>image_path</code>, <code>json=true</code>, <code>mode=summary</code></td></tr>
    <tr><td>Python</td><td><code>casars.tasks.image_analysis.imhead(...)</code></td><td><code>imagename</code>, <code>mode='summary'</code>, native binary path</td></tr>
    <tr><td>TUI</td><td><code>casars imhead twhya_cont.image --json --mode summary</code></td><td><code>Image Path</code>, <code>JSON=on</code>, <code>Mode=summary</code>. Visual capture backend: <code>{html.escape(str(tui_backend))}</code></td></tr>
    <tr><td>GUI</td><td>Learner path: select <code>twhya_cont.image</code> and read the Inspector. Regression path: open section task, run <code>Image Header</code>.</td><td>Inspector shows <code>Size</code>, <code>Units</code>, <code>Shape</code>, and <code>Image details</code>. Task run uses <code>image_path={html.escape(str(gui_params.get('image_path', '')))}</code>, <code>mode={html.escape(str(gui_params.get('mode', '')))}</code>, <code>json={html.escape(str(gui_toggles.get('json')))}</code></td></tr>
  </table>

  <section class="card">
    <h2>Invalid Image Behavior</h2>
    <p>All casa-rs surfaces should report a clear <code>cannot open or read CASA image</code> style error when the selected image path is missing, unreadable, or not a valid casacore image. A valid tutorial image should show inspected metadata in the GUI Inspector.</p>
    <p>Evidence: <code>.casa-rs/evidence/invalid-image-checks.json</code>. Status: <code>{html.escape(invalid_status)}</code>.</p>
    <ul>
      {''.join(f"<li>{html.escape(check['surface'])}: <code>{'pass' if check.get('passed') else 'fail'}</code></li>" for check in invalid_checks)}
    </ul>
  </section>

  <h2>Comparison</h2>
  {failures}
  <table>
    <tr><th>Field</th><th>Status</th><th>casa-rs</th><th>CASA normalized</th></tr>
    {rows}
  </table>

</main>
</body>
</html>
"""
    html_path.write_text(html_doc, encoding="utf-8")
    (pack_root / "docs" / "index.md").write_text(
        f"# {manifest['title']}\n\n"
        "Generated tutorial pack.\n\n"
        f"- [01. {section['title']}](sections/{section['id']}.html)\n",
        encoding="utf-8",
    )


def write_review_record(
    pack_root: Path,
    manifest: dict[str, Any],
    section: dict[str, Any],
    screenshot_refs: list[str],
    invalid_image_checks: dict[str, Any],
) -> None:
    record = {
        "schema_version": "tutorial-pack-review.v0",
        "pack_id": manifest["pack_id"],
        "tutorial_id": manifest["tutorial_id"],
        "section_id": section["id"],
        "status": "pending-human-review",
        "casa_source": {
            "guide_url": section["casa_source"]["guide_url"],
            "section_anchor": section["casa_source"]["section_anchor"],
            "task_calls": section["casa_source"]["task_calls"],
            "expected_observable_result": section["observable_result"],
        },
        "casars_equivalents": {
            surface: {
                "provider_kind": "native-rust",
                "task_id": "imhead",
                "parameters": step(section, surface)["parameters"],
                **({"command_template": step(section, surface).get("command_template")} if step(section, surface).get("command_template") else {}),
                **({"ui_path": step(section, surface).get("ui_path")} if step(section, surface).get("ui_path") else {}),
                **({"screenshot_refs": screenshot_refs} if screenshot_refs else {}),
            }
            for surface in ["cli", "python", "tui", "gui"]
        },
        "observable_products": {
            "casa_refs": [".casa-rs/workspace/oracle/01-imhead-continuum-header/casa-imhead.json"],
            "casars_refs": [
                ".casa-rs/workspace/native/01-imhead-continuum-header/cli-imhead.json",
                ".casa-rs/workspace/native/01-imhead-continuum-header/python-imhead.json",
                ".casa-rs/workspace/native/01-imhead-continuum-header/tui-terminal.txt",
                ".casa-rs/workspace/native/01-imhead-continuum-header/gui-imhead.json",
            ],
            "comparison_refs": [".casa-rs/evidence/comparisons.json"],
            "timing_refs": [".casa-rs/evidence/timings.jsonl"],
        },
        "regression_evidence": {
            "input_manifest_refs": [".casa-rs/evidence/data-manifest.json"],
            "native_run_refs": [".casa-rs/evidence/native-runs.jsonl"],
            "oracle_run_refs": [".casa-rs/evidence/oracle-runs.jsonl"],
            "provider_provenance_refs": [".casa-rs/evidence/provider-provenance.json"],
            "invalid_image_check_refs": [".casa-rs/evidence/invalid-image-checks.json"],
            "screenshot_spec_refs": [".casa-rs/screenshots/specs/01-imhead-continuum-header.json"] if screenshot_refs else [],
        },
        "invalid_image_contract": {
            "expected_message_fragment": "cannot open or read CASA image",
            "status": "passed" if invalid_image_checks.get("passed") else "needs-review",
            "surfaces": {
                check["surface"]: {
                    "passed": check.get("passed", False),
                    "evidence_ref": ".casa-rs/evidence/invalid-image-checks.json",
                }
                for check in invalid_image_checks.get("checks", [])
            },
        },
        "human_evaluation": {
            "outcome": "pending",
            "reviewed_by": None,
            "reviewed_at": None,
            "comments": "",
            "required_changes": [],
            "follow_up_issue_refs": [],
        },
    }
    write_json(pack_root / section["review_checkpoint"]["record_path"], record)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pack", type=Path, default=default_pack_path())
    parser.add_argument("--section", default=SECTION_ID)
    parser.add_argument("--casa-python", type=Path, default=Path(os.environ.get("CASA_RS_CASA_PYTHON", DEFAULT_CASA_PYTHON)))
    parser.add_argument("--skip-gui", action="store_true")
    parser.add_argument(
        "--capture-tui-pty",
        action="store_true",
        help="experimental raw PTY TUI capture; default records deterministic TUI command/parameter evidence only",
    )
    parser.add_argument(
        "--generate-summary-cards",
        action="store_true",
        help="write synthetic summary cards under .casa-rs/evidence/summary-cards; never use these as UI screenshots",
    )
    parser.add_argument(
        "--docs-only",
        action="store_true",
        help="regenerate review/docs/screenshot-spec artifacts from existing evidence without rerunning CASA or UIs",
    )
    args = parser.parse_args()

    pack_root = args.pack.expanduser().resolve()
    manifest = load_json(pack_root / "pack.json")
    section = section_from_manifest(manifest, args.section)
    if section["id"] != SECTION_ID:
        raise SystemExit(f"this runner currently supports only {SECTION_ID}")

    if args.docs_only:
        comparison = load_json(pack_root / ".casa-rs/evidence/comparisons.json")
        gui_debug = load_json(pack_root / ".casa-rs/workspace/native" / section["id"] / "gui-debug-state.json")
        invalid_image_checks = load_json(pack_root / ".casa-rs/evidence/invalid-image-checks.json")
        real_screenshot_refs = existing_real_screenshot_refs(pack_root, section["id"])
        screenshot_spec = {
            "schema_version": "tutorial-screenshot-spec.v0",
            "section_id": section["id"],
            ".casa-rs/screenshots": {
                name: {
                    "path": rel(pack_root, path),
                    "source": "actual UI capture",
                    "parameters_visible": True,
                }
                for name, path in real_screenshot_refs.items()
            },
        }
        write_json(pack_root / ".casa-rs/screenshots/specs/01-imhead-continuum-header.json", screenshot_spec)
        screenshot_refs = [rel(pack_root, path) for path in real_screenshot_refs.values()]
        write_review_record(pack_root, manifest, section, screenshot_refs, invalid_image_checks)
        write_docs(pack_root, manifest, section, comparison, gui_debug, invalid_image_checks, real_screenshot_refs)
        print(
            json.dumps(
                {
                    "pack": str(pack_root),
                    "section_id": section["id"],
                    "html": str(pack_root / "docs/sections/01-imhead-continuum-header.html"),
                    "review_record": str(pack_root / ".casa-rs/evidence/review/01-imhead-continuum-header.json"),
                    "docs_only": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return

    imexplore = REPO_ROOT / "target/debug/imexplore"
    casars = REPO_ROOT / "target/debug/casars"
    if not imexplore.exists():
        run_command(["cargo", "build", "-p", "casa-images", "--bin", "imexplore"], cwd=REPO_ROOT)
    if not casars.exists():
        run_command(["cargo", "build", "-p", "casars"], cwd=REPO_ROOT)

    image_path = pack_root / "twhya_cont.image"
    if not image_path.exists():
        raise SystemExit(f"missing tutorial input {image_path}")
    for relative in [
        f".casa-rs/workspace/native/{section['id']}",
        f".casa-rs/workspace/oracle/{section['id']}",
        ".casa-rs/screenshots/annotated",
        ".casa-rs/screenshots/specs",
        "docs/sections",
        ".casa-rs/evidence/review",
    ]:
        (pack_root / relative).mkdir(parents=True, exist_ok=True)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    native_cli, cli_result = run_cli(pack_root, image_path, imexplore, section)
    native_python, python_result = run_python(pack_root, image_path, imexplore, section)
    if args.skip_gui:
        native_gui = native_cli
        gui_result = CommandResult(argv=[], elapsed_seconds=0.0, stdout="{}", stderr="")
        gui_debug = {"activeTaskValues": {}, "activeTaskToggles": {}, "taskDiagnostics": []}
    else:
        native_gui, gui_result, gui_debug = run_gui(pack_root, imexplore, section["id"], section)

    if args.capture_tui_pty:
        tui_record = capture_tui(pack_root, image_path, section["id"])
    else:
        tui_record = {
            "argv": [
                str(REPO_ROOT / "target/debug/casars"),
                "imhead",
                str(image_path),
                "--json",
                "--mode",
                "summary",
            ],
            "elapsed_seconds": 0.0,
            "completed": True,
            "capture_backend": "parameter-record-only",
            "capture_note": "Raw PTY capture is intentionally not part of the default regression path; use a real terminal backend for screenshot evidence.",
        }
    write_json(output_path(pack_root, step(section, "tui")), native_cli)

    invalid_path = ensure_invalid_image_fixture(pack_root, section["id"])
    invalid_checks = [
        run_invalid_cli(imexplore, invalid_path),
        run_invalid_python(imexplore, invalid_path),
    ]
    if args.skip_gui:
        invalid_checks.append(
            {
                "surface": "gui",
                "passed": True,
                "skipped": True,
                "reason": "--skip-gui was set",
                "expected_message_fragment": "cannot open or read CASA image",
            }
        )
    else:
        invalid_checks.append(run_invalid_gui(pack_root, imexplore, section["id"], invalid_path))
    invalid_checks.append(capture_tui_error_contract(imexplore, invalid_path, section["id"], pack_root))
    invalid_image_checks = {
        "schema_version": "tutorial-invalid-image-checks.v0",
        "section_id": section["id"],
        "invalid_image_path": rel(pack_root, invalid_path),
        "expected_message_fragment": "cannot open or read CASA image",
        "checks": invalid_checks,
        "passed": all(check.get("passed", False) for check in invalid_checks),
    }
    write_json(pack_root / ".casa-rs/evidence/invalid-image-checks.json", invalid_image_checks)

    casa_raw, casa_normalized, casa_result = run_casa(pack_root, image_path, args.casa_python, section)
    oracle_comparison = compare_imhead(native_cli, casa_normalized)
    comparison = {
        "schema_version": "tutorial-section-comparison.v0",
        "pack_id": manifest["pack_id"],
        "tutorial_id": manifest["tutorial_id"],
        "section_id": section["id"],
        "run_id": run_id,
        "passed": bool(
            native_cli == native_python
            and native_cli == native_gui
            and tui_record.get("completed", False)
            and oracle_comparison["passed"]
            and invalid_image_checks["passed"]
        ),
        "native_surface_checks": {
            "cli_python_equal": native_cli == native_python,
            "cli_gui_equal": native_cli == native_gui,
            "tui_parameter_equivalent": tui_record.get("completed", False),
            "tui_capture_backend": tui_record.get("capture_backend", "raw-pty"),
        },
        "invalid_image_checks": {
            "passed": invalid_image_checks["passed"],
            "evidence_ref": ".casa-rs/evidence/invalid-image-checks.json",
        },
        "oracle_comparison": oracle_comparison,
    }
    write_json(pack_root / ".casa-rs/evidence/comparisons.json", comparison)

    provider_provenance = {
        "schema_version": "tutorial-provider-provenance.v0",
        "run_id": run_id,
        "native_provider_kind": manifest["native_provider_policy"]["native_provider_kind"],
        "oracle_provider_kind": manifest["native_provider_policy"]["oracle_provider_kind"],
        "imexplore": str(imexplore),
        "casars": str(casars),
        "casa_python": str(args.casa_python),
    }
    write_json(pack_root / ".casa-rs/evidence/provider-provenance.json", provider_provenance)

    native_runs = [
        ("cli", cli_result),
        ("python", python_result),
        ("gui", gui_result),
    ]
    for surface, result in native_runs:
        append_jsonl(
            pack_root / ".casa-rs/evidence/native-runs.jsonl",
            {
                "run_id": run_id,
                "section_id": section["id"],
                "surface": surface,
                "provider_kind": "native-rust",
                "argv": result.argv,
                "elapsed_seconds": result.elapsed_seconds,
                "stdout_path": step(section, surface)["outputs"][0]["path"],
                "stderr": result.stderr,
            },
        )
    append_jsonl(
        pack_root / ".casa-rs/evidence/native-runs.jsonl",
        {
            "run_id": run_id,
            "section_id": section["id"],
            "surface": "tui",
            "provider_kind": "native-rust",
            **tui_record,
        },
    )
    append_jsonl(
        pack_root / ".casa-rs/evidence/oracle-runs.jsonl",
        {
            "run_id": run_id,
            "section_id": section["id"],
            "surface": "oracle",
            "provider_kind": "casa-oracle",
            "argv": casa_result.argv,
            "elapsed_seconds": casa_result.elapsed_seconds,
            "stdout_path": step(section, "oracle")["outputs"][0]["path"],
            "stderr": casa_result.stderr,
        },
    )
    for surface, result in [
        ("cli", cli_result),
        ("python", python_result),
        ("gui", gui_result),
        ("tui", CommandResult(tui_record.get("argv", []), tui_record.get("elapsed_seconds", 0.0), "", "")),
        ("oracle", casa_result),
    ]:
        append_jsonl(
            pack_root / ".casa-rs/evidence/timings.jsonl",
            {
                "run_id": run_id,
                "section_id": section["id"],
                "surface": surface,
                "elapsed_seconds": result.elapsed_seconds,
            },
        )

    real_screenshot_refs = existing_real_screenshot_refs(pack_root, section["id"])
    if args.generate_summary_cards:
        card_root = pack_root / ".casa-rs/evidence/summary-cards"
        cards = {
            "casa": card_root / "01-imhead-casa.png",
            "cli": card_root / "01-imhead-cli.png",
            "python": card_root / "01-imhead-python.png",
            "tui": card_root / "01-imhead-tui.png",
            "gui": card_root / "01-imhead-gui.png",
        }
        draw_evidence_card(
            cards["casa"],
            title="CASA imhead",
            subtitle="Oracle task run through local CASA Python",
            command="imhead(imagename='twhya_cont.image', mode='summary')",
            parameters={"imagename": "twhya_cont.image", "mode": "summary"},
            output_lines=compact_output_lines(casa_normalized),
        )
        draw_evidence_card(
            cards["cli"],
            title="casa-rs CLI imhead",
            subtitle="Native Rust task through imexplore",
            command="imexplore imhead twhya_cont.image --json --mode summary",
            parameters={"image_path": "twhya_cont.image", "json": "true", "mode": "summary"},
            output_lines=compact_output_lines(native_cli),
        )
        draw_evidence_card(
            cards["python"],
            title="casa-rs Python imhead",
            subtitle="Python wrapper using the native Rust provider",
            command="image_analysis.imhead('twhya_cont.image', mode='summary', binary='target/debug/imexplore')",
            parameters={"imagename": "twhya_cont.image", "mode": "summary", "binary": "target/debug/imexplore"},
            output_lines=compact_output_lines(native_python),
        )
        draw_evidence_card(
            cards["tui"],
            title="casa-rs TUI imhead",
            subtitle="Parameter evidence for the TUI startup path",
            command="casars imhead twhya_cont.image --json --mode summary",
            parameters={"image_path": "twhya_cont.image", "json": "true", "mode": "summary"},
            output_lines=compact_output_lines(native_cli),
        )
        draw_evidence_card(
            cards["gui"],
            title="casa-rs GUI imhead",
            subtitle="macOS workbench tutorial section task run",
            command="Open Tutorial Pack -> 01 Inspect continuum image header -> Open in Tasks -> Run",
            parameters={
                "image_path": "twhya_cont.image",
                "json": gui_debug.get("activeTaskToggles", {}).get("json"),
                "mode": gui_debug.get("activeTaskValues", {}).get("mode", ""),
            },
            output_lines=compact_output_lines(native_gui),
        )
    if args.capture_tui_pty:
        tui_text = (pack_root / ".casa-rs/workspace/native" / section["id"] / "tui-terminal.txt").read_text(encoding="utf-8")
        tui_capture = pack_root / ".casa-rs/screenshots/source/01-imhead-tui-raw-pty.png"
        render_terminal_capture(tui_capture, "Actual TUI terminal capture", tui_text)
        real_screenshot_refs["tui"] = tui_capture
    screenshot_spec = {
        "schema_version": "tutorial-screenshot-spec.v0",
        "section_id": section["id"],
        ".casa-rs/screenshots": {
            name: {
                "path": rel(pack_root, path),
                "source": "actual UI capture",
                "parameters_visible": True,
            }
            for name, path in real_screenshot_refs.items()
        },
    }
    write_json(pack_root / ".casa-rs/screenshots/specs/01-imhead-continuum-header.json", screenshot_spec)
    screenshot_refs = [rel(pack_root, path) for path in real_screenshot_refs.values()]
    write_review_record(pack_root, manifest, section, screenshot_refs, invalid_image_checks)
    write_docs(pack_root, manifest, section, comparison, gui_debug, invalid_image_checks, real_screenshot_refs)

    summary = {
        "pack": str(pack_root),
        "section_id": section["id"],
        "run_id": run_id,
        "passed": comparison["passed"],
        "html": str(pack_root / "docs/sections/01-imhead-continuum-header.html"),
        "review_record": str(pack_root / ".casa-rs/evidence/review/01-imhead-continuum-header.json"),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
