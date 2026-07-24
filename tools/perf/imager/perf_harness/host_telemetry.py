# SPDX-License-Identifier: LGPL-3.0-or-later
"""Bounded host memory/swap telemetry for long CASA benchmark subprocesses."""

from __future__ import annotations

import datetime as dt
import platform
import re
import subprocess
import threading
import time
from typing import Any, Callable


SCHEMA_VERSION = 1
SCOPE = "darwin_host_during_casa_protocol_subprocess"
VM_STAT_FIELDS = {
    "Pages free": "pages_free",
    "Pages active": "pages_active",
    "Pages inactive": "pages_inactive",
    "Pages speculative": "pages_speculative",
    "Pages throttled": "pages_throttled",
    "Pages wired down": "pages_wired_down",
    "Pages purgeable": "pages_purgeable",
    "Pages stored in compressor": "pages_stored_in_compressor",
    "Pages occupied by compressor": "pages_occupied_by_compressor",
    "Pageins": "pageins",
    "Pageouts": "pageouts",
    "Swapins": "swapins",
    "Swapouts": "swapouts",
}
SAMPLE_FIELDS = {
    "observed_at",
    "elapsed_seconds",
    "physical_memory_bytes",
    "memory_free_percent",
    "page_size_bytes",
    *VM_STAT_FIELDS.values(),
}
SUMMARY_FIELDS = {
    "duration_seconds",
    "sample_count",
    "memory_free_percent_min",
    "memory_free_percent_end",
    "pages_throttled_max",
    "pageouts_delta",
    "swapins_delta",
    "swapouts_delta",
    "swapin_bytes_delta",
    "swapout_bytes_delta",
    "swap_io_bytes_per_second_max",
}


class HostTelemetryError(ValueError):
    """Host telemetry is malformed or cannot be sampled."""


class DarwinHostTelemetrySampler:
    """Sample global memory pressure and swap counters on a bounded interval."""

    def __init__(
        self,
        *,
        interval_seconds: float = 5.0,
        command_runner: Callable[
            ..., subprocess.CompletedProcess[str]
        ] = subprocess.run,
        monotonic: Callable[[], float] = time.monotonic,
        utc_now: Callable[[], str] | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("host telemetry interval must be positive")
        self.interval_seconds = float(interval_seconds)
        self._command_runner = command_runner
        self._monotonic = monotonic
        self._utc_now = utc_now or _utc_now
        self._started = 0.0
        self._samples: list[dict[str, Any]] = []
        self._errors: list[str] = []
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("host telemetry sampler is already started")
        self._started = self._monotonic()
        self._capture()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> dict[str, Any]:
        if self._thread is None:
            raise RuntimeError("host telemetry sampler was not started")
        self._stop_event.set()
        self._thread.join()
        self._capture()
        result = build_host_telemetry_result(
            interval_seconds=self.interval_seconds,
            samples=self._samples,
            errors=self._errors,
        )
        validate_host_telemetry(result)
        return result

    def _sample_loop(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            self._capture()

    def _capture(self) -> None:
        if platform.system() != "Darwin":
            reason = f"unsupported host platform: {platform.system()}"
            if reason not in self._errors:
                self._errors.append(reason)
            return
        try:
            snapshot = read_darwin_host_snapshot(
                command_runner=self._command_runner,
            )
        except (HostTelemetryError, OSError, subprocess.SubprocessError) as error:
            self._errors.append(f"{type(error).__name__}: {error}")
            return
        snapshot.update(
            {
                "observed_at": self._utc_now(),
                "elapsed_seconds": max(0.0, self._monotonic() - self._started),
            }
        )
        self._samples.append(snapshot)


def read_darwin_host_snapshot(
    *,
    command_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    vm_stat = command_runner(
        ["/usr/bin/vm_stat"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=10.0,
        check=True,
    ).stdout
    memory_pressure = command_runner(
        ["/usr/bin/memory_pressure", "-Q"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=10.0,
        check=True,
    ).stdout
    page_match = re.search(r"page size of (\d+) bytes", vm_stat)
    physical_match = re.search(r"system has (\d+)", memory_pressure)
    free_match = re.search(r"memory free percentage:\s*(\d+)%", memory_pressure)
    if page_match is None or physical_match is None or free_match is None:
        raise HostTelemetryError("Darwin memory command output is unrecognized")
    values: dict[str, Any] = {
        "physical_memory_bytes": int(physical_match.group(1)),
        "memory_free_percent": int(free_match.group(1)),
        "page_size_bytes": int(page_match.group(1)),
    }
    observed: dict[str, int] = {}
    for raw_line in vm_stat.splitlines()[1:]:
        match = re.match(r'\s*"?([^":]+)"?:\s*([0-9]+)\.?\s*$', raw_line)
        if match is not None:
            observed[match.group(1)] = int(match.group(2))
    missing = [name for name in VM_STAT_FIELDS if name not in observed]
    if missing:
        raise HostTelemetryError(
            "vm_stat omitted required field(s): " + ", ".join(missing)
        )
    values.update(
        {target: observed[source] for source, target in VM_STAT_FIELDS.items()}
    )
    return values


def build_host_telemetry_result(
    *,
    interval_seconds: float,
    samples: list[dict[str, Any]],
    errors: list[str],
) -> dict[str, Any]:
    copied_samples = [dict(sample) for sample in samples]
    if not copied_samples:
        return {
            "schema_version": SCHEMA_VERSION,
            "scope": SCOPE,
            "status": "unavailable",
            "interval_seconds": interval_seconds,
            "sampling_errors": list(errors) or ["no host telemetry samples"],
            "samples": [],
            "summary": None,
        }
    first = copied_samples[0]
    last = copied_samples[-1]
    page_size = int(first["page_size_bytes"])
    rates = []
    for left, right in zip(copied_samples, copied_samples[1:]):
        elapsed = float(right["elapsed_seconds"]) - float(left["elapsed_seconds"])
        page_delta = max(0, int(right["swapins"]) - int(left["swapins"])) + max(
            0, int(right["swapouts"]) - int(left["swapouts"])
        )
        if elapsed > 0:
            rates.append(page_delta * page_size / elapsed)
    summary = {
        "duration_seconds": max(
            0.0, float(last["elapsed_seconds"]) - float(first["elapsed_seconds"])
        ),
        "sample_count": len(copied_samples),
        "memory_free_percent_min": min(
            int(sample["memory_free_percent"]) for sample in copied_samples
        ),
        "memory_free_percent_end": int(last["memory_free_percent"]),
        "pages_throttled_max": max(
            int(sample["pages_throttled"]) for sample in copied_samples
        ),
        "pageouts_delta": max(0, int(last["pageouts"]) - int(first["pageouts"])),
        "swapins_delta": max(0, int(last["swapins"]) - int(first["swapins"])),
        "swapouts_delta": max(0, int(last["swapouts"]) - int(first["swapouts"])),
        "swapin_bytes_delta": max(0, int(last["swapins"]) - int(first["swapins"]))
        * page_size,
        "swapout_bytes_delta": max(0, int(last["swapouts"]) - int(first["swapouts"]))
        * page_size,
        "swap_io_bytes_per_second_max": max(rates, default=0.0),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "scope": SCOPE,
        "status": "measured" if len(copied_samples) >= 2 else "partial",
        "interval_seconds": interval_seconds,
        "sampling_errors": list(errors),
        "samples": copied_samples,
        "summary": summary,
    }


def validate_host_telemetry(value: Any) -> None:
    if not isinstance(value, dict):
        raise HostTelemetryError("host telemetry must be an object")
    expected = {
        "schema_version",
        "scope",
        "status",
        "interval_seconds",
        "sampling_errors",
        "samples",
        "summary",
    }
    if set(value) != expected:
        raise HostTelemetryError("host telemetry fields are not exact")
    if value["schema_version"] != SCHEMA_VERSION or value["scope"] != SCOPE:
        raise HostTelemetryError("host telemetry identity is invalid")
    status = value["status"]
    if status not in {"measured", "partial", "unavailable"}:
        raise HostTelemetryError("host telemetry status is invalid")
    interval = value["interval_seconds"]
    if (
        isinstance(interval, bool)
        or not isinstance(interval, (int, float))
        or interval <= 0
    ):
        raise HostTelemetryError("host telemetry interval must be positive")
    errors = value["sampling_errors"]
    if not isinstance(errors, list) or not all(
        isinstance(error, str) and error for error in errors
    ):
        raise HostTelemetryError("host telemetry errors must be strings")
    samples = value["samples"]
    if not isinstance(samples, list):
        raise HostTelemetryError("host telemetry samples must be a list")
    previous_elapsed = -1.0
    for sample in samples:
        if not isinstance(sample, dict) or set(sample) != SAMPLE_FIELDS:
            raise HostTelemetryError("host telemetry sample fields are not exact")
        if not isinstance(sample["observed_at"], str) or not sample["observed_at"]:
            raise HostTelemetryError("host telemetry timestamp is invalid")
        elapsed = sample["elapsed_seconds"]
        if (
            isinstance(elapsed, bool)
            or not isinstance(elapsed, (int, float))
            or elapsed < previous_elapsed
        ):
            raise HostTelemetryError("host telemetry elapsed time is invalid")
        previous_elapsed = float(elapsed)
        for field in SAMPLE_FIELDS - {"observed_at", "elapsed_seconds"}:
            member = sample[field]
            if isinstance(member, bool) or not isinstance(member, int) or member < 0:
                raise HostTelemetryError(f"host telemetry sample {field} is invalid")
    summary = value["summary"]
    if status == "unavailable":
        if samples or summary is not None or not errors:
            raise HostTelemetryError("unavailable host telemetry is inconsistent")
        return
    if not samples or not isinstance(summary, dict) or set(summary) != SUMMARY_FIELDS:
        raise HostTelemetryError("measured host telemetry summary is invalid")
    for field, member in summary.items():
        if (
            isinstance(member, bool)
            or not isinstance(member, (int, float))
            or member < 0
        ):
            raise HostTelemetryError(f"host telemetry summary {field} is invalid")
    if summary["sample_count"] != len(samples):
        raise HostTelemetryError("host telemetry sample count is inconsistent")
    if status == "measured" and len(samples) < 2:
        raise HostTelemetryError("measured host telemetry requires two samples")


def _utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
