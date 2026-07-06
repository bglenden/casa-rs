#!/usr/bin/env python3
"""Measure plain filesystem read throughput for a MeasurementSet.

This intentionally does not import or invoke casa-rs imaging code.  It is a
storage sanity check for large-MS performance runs, especially when an external
volume may be disconnecting under load.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time


DEFAULT_CHUNK_BYTES = 64 * 1024 * 1024


class BenchmarkError(Exception):
    """User-facing benchmark setup or runtime failure."""


class ReadFailure(BenchmarkError):
    """Failure while reading one file, with partial byte count."""

    def __init__(self, path: Path, read_bytes: int, error: OSError) -> None:
        self.path = path
        self.read_bytes = read_bytes
        self.error = error
        super().__init__(
            f"read failed after {human_bytes(read_bytes)} from {path}: {error}"
        )


def parse_size(text: str) -> int:
    value = text.strip().lower()
    multipliers = {
        "k": 1_000,
        "kb": 1_000,
        "m": 1_000_000,
        "mb": 1_000_000,
        "g": 1_000_000_000,
        "gb": 1_000_000_000,
        "ki": 1024,
        "kib": 1024,
        "mi": 1024 * 1024,
        "mib": 1024 * 1024,
        "gi": 1024 * 1024 * 1024,
        "gib": 1024 * 1024 * 1024,
    }
    for suffix, multiplier in sorted(multipliers.items(), key=lambda item: -len(item[0])):
        if value.endswith(suffix):
            number = value[: -len(suffix)]
            return int(float(number) * multiplier)
    return int(float(value))


def human_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    scaled = float(value)
    for unit in units:
        if abs(scaled) < 1000.0 or unit == units[-1]:
            return f"{scaled:.2f} {unit}"
        scaled /= 1000.0
    return f"{scaled:.2f} TB"


def require_directory(path: Path, label: str) -> None:
    if not path.exists():
        raise BenchmarkError(f"{label} does not exist: {path}")
    if not path.is_dir():
        raise BenchmarkError(f"{label} is not a directory: {path}")


def collect_regular_files(root: Path) -> list[tuple[Path, int]]:
    files: list[tuple[Path, int]] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        for filename in sorted(filenames):
            path = Path(dirpath) / filename
            try:
                stat = path.stat()
            except OSError as error:
                raise BenchmarkError(f"failed to stat {path}: {error}") from error
            if not path.is_file():
                continue
            if stat.st_size <= 0:
                continue
            files.append((path, stat.st_size))
    if not files:
        raise BenchmarkError(f"no non-empty regular files found under {root}")
    files.sort(key=lambda item: item[1], reverse=True)
    return files


def select_files(
    files: list[tuple[Path, int]],
    max_bytes: int | None,
    top_files: int | None,
) -> list[tuple[Path, int]]:
    selected = files[:top_files] if top_files else list(files)
    if max_bytes is None:
        return selected
    limited: list[tuple[Path, int]] = []
    remaining = max_bytes
    for path, size in selected:
        if remaining <= 0:
            break
        limited.append((path, min(size, remaining)))
        remaining -= size
    return limited


def read_file(path: Path, bytes_to_read: int, chunk_bytes: int) -> int:
    read_bytes = 0
    buffer = bytearray(min(chunk_bytes, max(bytes_to_read, 1)))
    view = memoryview(buffer)
    try:
        with path.open("rb", buffering=0) as handle:
            while read_bytes < bytes_to_read:
                request = min(len(buffer), bytes_to_read - read_bytes)
                count = handle.readinto(view[:request])
                if not count:
                    break
                read_bytes += count
    except OSError as error:
        raise ReadFailure(path, read_bytes, error) from error
    return read_bytes


def benchmark(
    ms_path: Path,
    require_mount: Path | None,
    max_bytes: int | None,
    top_files: int | None,
    chunk_bytes: int,
    quiet: bool,
) -> dict:
    require_directory(ms_path, "MeasurementSet")
    if require_mount is not None:
        require_directory(require_mount, "required mount")
    files = collect_regular_files(ms_path)
    selected = select_files(files, max_bytes, top_files)
    planned_bytes = sum(size for _, size in selected)
    if planned_bytes <= 0:
        raise BenchmarkError("selected read set is empty")

    started = time.perf_counter()
    actual_bytes = 0
    file_reports = []
    for index, (path, bytes_to_read) in enumerate(selected, start=1):
        if require_mount is not None and not require_mount.exists():
            raise BenchmarkError(
                f"required mount disappeared before file {index}: {require_mount}"
            )
        file_started = time.perf_counter()
        read_bytes = read_file(path, bytes_to_read, chunk_bytes)
        file_elapsed = time.perf_counter() - file_started
        actual_bytes += read_bytes
        report = {
            "path": str(path),
            "planned_bytes": bytes_to_read,
            "read_bytes": read_bytes,
            "elapsed_s": file_elapsed,
            "mb_per_s": (read_bytes / 1_000_000.0 / file_elapsed)
            if file_elapsed > 0.0
            else 0.0,
        }
        file_reports.append(report)
        if not quiet:
            print(
                f"read {index}/{len(selected)} {human_bytes(read_bytes)} "
                f"from {path.name} at {report['mb_per_s']:.1f} MB/s",
                file=sys.stderr,
                flush=True,
            )
        if read_bytes != bytes_to_read:
            raise BenchmarkError(
                f"short read from {path}: planned {bytes_to_read}, read {read_bytes}"
            )
    elapsed = time.perf_counter() - started
    mount_present_after = require_mount.exists() if require_mount is not None else None
    if mount_present_after is False:
        raise BenchmarkError(f"required mount disappeared after read: {require_mount}")
    return {
        "measurement_set": str(ms_path),
        "required_mount": str(require_mount) if require_mount is not None else None,
        "mount_present_after": mount_present_after,
        "candidate_file_count": len(files),
        "read_file_count": len(selected),
        "planned_bytes": planned_bytes,
        "read_bytes": actual_bytes,
        "elapsed_s": elapsed,
        "mb_per_s": (actual_bytes / 1_000_000.0 / elapsed) if elapsed > 0.0 else 0.0,
        "mib_per_s": (actual_bytes / 1024.0 / 1024.0 / elapsed) if elapsed > 0.0 else 0.0,
        "files": file_reports,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("measurement_set", type=Path)
    parser.add_argument(
        "--require-mount",
        type=Path,
        help="fail if this mount path is missing before, during, or after the read",
    )
    parser.add_argument(
        "--max-bytes",
        type=parse_size,
        help="cap bytes read, e.g. 32GB; defaults to reading selected files fully",
    )
    parser.add_argument(
        "--top-files",
        type=int,
        help="read only the N largest regular files in the MS",
    )
    parser.add_argument(
        "--chunk-bytes",
        type=parse_size,
        default=DEFAULT_CHUNK_BYTES,
        help="read buffer size, default 64MiB",
    )
    parser.add_argument("--quiet", action="store_true", help="suppress per-file stderr progress")
    parser.add_argument("--output", type=Path, help="write JSON report to this path")
    return parser


def main(argv: list[str]) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.top_files is not None and args.top_files <= 0:
        raise SystemExit("--top-files must be positive")
    if args.max_bytes is not None and args.max_bytes <= 0:
        raise SystemExit("--max-bytes must be positive")
    if args.chunk_bytes <= 0:
        raise SystemExit("--chunk-bytes must be positive")
    try:
        report = benchmark(
            args.measurement_set,
            args.require_mount,
            args.max_bytes,
            args.top_files,
            args.chunk_bytes,
            args.quiet,
        )
    except BenchmarkError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
