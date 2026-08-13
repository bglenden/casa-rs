# SPDX-License-Identifier: LGPL-3.0-or-later
"""Bounded host and process telemetry for long imaging benchmark subprocesses."""

from __future__ import annotations

import ctypes
import ctypes.util
import datetime as dt
import os
import platform
import plistlib
import re
import subprocess
import threading
import time
from typing import Any, Callable


LEGACY_SCHEMA_VERSION = 1
SCHEMA_VERSION = 2
LEGACY_SCOPE = "darwin_host_during_casa_protocol_subprocess"
SCOPE = "darwin_host_during_imaging_subprocess"
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
LEGACY_SAMPLE_FIELDS = {
    "observed_at",
    "elapsed_seconds",
    "physical_memory_bytes",
    "memory_free_percent",
    "page_size_bytes",
    *VM_STAT_FIELDS.values(),
}
SAMPLE_FIELDS = {
    *LEGACY_SAMPLE_FIELDS,
    "host_compressed_memory_bytes",
    "swap_used_bytes",
    "process_pid",
    "process_physical_footprint_bytes",
    "process_physical_footprint_bytes_lifetime_peak",
    "process_resident_memory_bytes",
    "process_page_faults",
    "process_disk_read_bytes",
    "process_disk_write_bytes",
    "spill_volume_path",
    "spill_volume_device",
    "spill_volume_read_bytes",
    "spill_volume_write_bytes",
}
LEGACY_SUMMARY_FIELDS = {
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
SUMMARY_FIELDS = {
    *LEGACY_SUMMARY_FIELDS,
    "host_compressed_memory_bytes_peak",
    "host_compressed_memory_bytes_end",
    "swap_used_bytes_peak",
    "swap_used_bytes_end",
    "process_physical_footprint_bytes_peak",
    "process_resident_memory_bytes_peak",
    "process_page_faults_delta",
    "process_disk_read_bytes_delta",
    "process_disk_write_bytes_delta",
    "spill_volume_read_bytes_delta",
    "spill_volume_write_bytes_delta",
}

_OPTIONAL_SAMPLE_INT_FIELDS = {
    "process_pid",
    "process_physical_footprint_bytes",
    "process_physical_footprint_bytes_lifetime_peak",
    "process_resident_memory_bytes",
    "process_page_faults",
    "process_disk_read_bytes",
    "process_disk_write_bytes",
    "spill_volume_read_bytes",
    "spill_volume_write_bytes",
}
_OPTIONAL_SAMPLE_STRING_FIELDS = {"spill_volume_path", "spill_volume_device"}
_OPTIONAL_SUMMARY_FIELDS = {
    "process_physical_footprint_bytes_peak",
    "process_resident_memory_bytes_peak",
    "process_page_faults_delta",
    "process_disk_read_bytes_delta",
    "process_disk_write_bytes_delta",
    "spill_volume_read_bytes_delta",
    "spill_volume_write_bytes_delta",
}


class HostTelemetryError(ValueError):
    """Host telemetry is malformed or cannot be sampled."""


class DarwinHostTelemetrySampler:
    """Sample host and attached subprocess resources on a bounded interval."""

    def __init__(
        self,
        *,
        interval_seconds: float = 5.0,
        command_runner: Callable[
            ..., subprocess.CompletedProcess[str]
        ] = subprocess.run,
        monotonic: Callable[[], float] = time.monotonic,
        utc_now: Callable[[], str] | None = None,
        platform_system: Callable[[], str] = platform.system,
        process_snapshot_reader: (Callable[[int], dict[str, int | None]] | None) = None,
        volume_snapshot_reader: (Callable[[str], dict[str, int | str]] | None) = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("host telemetry interval must be positive")
        self.interval_seconds = float(interval_seconds)
        self._command_runner = command_runner
        self._monotonic = monotonic
        self._utc_now = utc_now or _utc_now
        self._platform_system = platform_system
        self._process_snapshot_reader = (
            process_snapshot_reader or read_darwin_process_snapshot
        )
        self._volume_snapshot_reader = volume_snapshot_reader
        self._started = 0.0
        self._samples: list[dict[str, Any]] = []
        self._errors: list[str] = []
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._capture_lock = threading.Lock()
        self._process_pid: int | None = None
        self._process_pid_file: str | None = None
        self._spill_volume_path: str | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("host telemetry sampler is already started")
        self._stop_event.clear()
        self._started = self._monotonic()
        self._capture()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def attach_targets(
        self,
        *,
        process_pid: int | None = None,
        process_pid_file: os.PathLike[str] | str | None = None,
        spill_volume_path: os.PathLike[str] | str | None = None,
    ) -> None:
        """Attach targets after spawn and immediately establish their baseline."""

        if process_pid is None and process_pid_file is None and spill_volume_path is None:
            raise ValueError("at least one telemetry target must be supplied")
        if process_pid is not None and process_pid_file is not None:
            raise ValueError(
                "attach either a process PID or a process PID receipt, not both"
            )
        if process_pid is not None and (
            isinstance(process_pid, bool)
            or not isinstance(process_pid, int)
            or process_pid <= 0
        ):
            raise ValueError("telemetry process PID must be a positive integer")
        normalized_pid_file = (
            os.fspath(process_pid_file) if process_pid_file is not None else None
        )
        if normalized_pid_file is not None and (
            not isinstance(normalized_pid_file, str) or not normalized_pid_file
        ):
            raise ValueError("telemetry process PID receipt path must not be empty")
        normalized_path = (
            os.fspath(spill_volume_path) if spill_volume_path is not None else None
        )
        if normalized_path is not None and (
            not isinstance(normalized_path, str) or not normalized_path
        ):
            raise ValueError("telemetry spill-volume path must not be empty")
        with self._lock:
            if (
                process_pid is not None
                and self._process_pid is not None
                and process_pid != self._process_pid
            ):
                raise RuntimeError("host telemetry process target is already attached")
            if (
                normalized_pid_file is not None
                and self._process_pid_file is not None
                and normalized_pid_file != self._process_pid_file
            ):
                raise RuntimeError(
                    "host telemetry process PID receipt is already attached"
                )
            if (
                normalized_path is not None
                and self._spill_volume_path is not None
                and normalized_path != self._spill_volume_path
            ):
                raise RuntimeError("host telemetry volume target is already attached")
            if process_pid is not None:
                self._process_pid = process_pid
            if normalized_pid_file is not None:
                self._process_pid_file = normalized_pid_file
            if normalized_path is not None:
                self._spill_volume_path = normalized_path
            started = self._thread is not None
        if started:
            self._capture()

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
        with self._capture_lock:
            self._capture_once()

    def _capture_once(self) -> None:
        system = self._platform_system()
        if system != "Darwin":
            self._record_error(f"unsupported host platform: {system}")
            return
        try:
            snapshot = read_darwin_host_snapshot(
                command_runner=self._command_runner,
            )
        except (HostTelemetryError, OSError, subprocess.SubprocessError) as error:
            self._record_error(f"{type(error).__name__}: {error}")
            return
        with self._lock:
            process_pid = self._process_pid
            process_pid_file = self._process_pid_file
            spill_volume_path = self._spill_volume_path
        if process_pid_file is not None:
            try:
                process_pid = read_process_pid_receipt(process_pid_file)
            except (HostTelemetryError, OSError) as error:
                self._record_error(
                    f"process PID receipt {process_pid_file}: "
                    f"{type(error).__name__}: {error}"
                )
        snapshot.update(
            {
                "process_pid": process_pid,
                "process_physical_footprint_bytes": None,
                "process_physical_footprint_bytes_lifetime_peak": None,
                "process_resident_memory_bytes": None,
                "process_page_faults": None,
                "process_disk_read_bytes": None,
                "process_disk_write_bytes": None,
                "spill_volume_path": spill_volume_path,
                "spill_volume_device": None,
                "spill_volume_read_bytes": None,
                "spill_volume_write_bytes": None,
            }
        )
        if process_pid is not None:
            try:
                snapshot.update(self._process_snapshot_reader(process_pid))
            except (HostTelemetryError, OSError) as error:
                self._record_error(
                    f"process {process_pid}: {type(error).__name__}: {error}"
                )
        if spill_volume_path is not None:
            try:
                if self._volume_snapshot_reader is None:
                    volume_snapshot = read_darwin_volume_snapshot(
                        spill_volume_path,
                        command_runner=self._command_runner,
                    )
                else:
                    volume_snapshot = self._volume_snapshot_reader(spill_volume_path)
                snapshot.update(volume_snapshot)
            except (HostTelemetryError, OSError, subprocess.SubprocessError) as error:
                self._record_error(
                    f"volume {spill_volume_path}: {type(error).__name__}: {error}"
                )
        snapshot.update(
            {
                "observed_at": self._utc_now(),
                "elapsed_seconds": max(0.0, self._monotonic() - self._started),
            }
        )
        with self._lock:
            self._samples.append(snapshot)

    def _record_error(self, reason: str) -> None:
        with self._lock:
            if reason not in self._errors:
                self._errors.append(reason)


def read_process_pid_receipt(path: os.PathLike[str] | str) -> int | None:
    """Read a benchmark child's PID receipt.

    A missing or empty receipt means the child has not reached `exec` yet.
    Once populated, the receipt must contain exactly one positive integer.
    """

    normalized = os.fspath(path)
    if not isinstance(normalized, str) or not normalized:
        raise ValueError("process PID receipt path must not be empty")
    try:
        with open(normalized, encoding="utf-8") as handle:
            text = handle.read().strip()
    except FileNotFoundError:
        return None
    if not text:
        return None
    if not re.fullmatch(r"[1-9][0-9]*", text):
        raise HostTelemetryError("process PID receipt must contain one positive integer")
    return int(text)


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
    swap_usage = command_runner(
        ["/usr/sbin/sysctl", "-n", "vm.swapusage"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=10.0,
        check=True,
    ).stdout
    page_match = re.search(r"page size of (\d+) bytes", vm_stat)
    physical_match = re.search(r"system has (\d+)", memory_pressure)
    free_match = re.search(r"memory free percentage:\s*(\d+)%", memory_pressure)
    swap_used_match = re.search(
        r"\bused\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*([BKMGT])",
        swap_usage,
        flags=re.IGNORECASE,
    )
    if (
        page_match is None
        or physical_match is None
        or free_match is None
        or swap_used_match is None
    ):
        raise HostTelemetryError("Darwin memory command output is unrecognized")
    page_size = int(page_match.group(1))
    values: dict[str, Any] = {
        "physical_memory_bytes": int(physical_match.group(1)),
        "memory_free_percent": int(free_match.group(1)),
        "page_size_bytes": page_size,
        "swap_used_bytes": _scaled_bytes(
            swap_used_match.group(1), swap_used_match.group(2)
        ),
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
    values["host_compressed_memory_bytes"] = (
        values["pages_occupied_by_compressor"] * page_size
    )
    return values


class _RusageInfoV4(ctypes.Structure):
    _fields_ = [
        ("ri_uuid", ctypes.c_uint8 * 16),
        *(
            (name, ctypes.c_uint64)
            for name in (
                "ri_user_time",
                "ri_system_time",
                "ri_pkg_idle_wkups",
                "ri_interrupt_wkups",
                "ri_pageins",
                "ri_wired_size",
                "ri_resident_size",
                "ri_phys_footprint",
                "ri_proc_start_abstime",
                "ri_proc_exit_abstime",
                "ri_child_user_time",
                "ri_child_system_time",
                "ri_child_pkg_idle_wkups",
                "ri_child_interrupt_wkups",
                "ri_child_pageins",
                "ri_child_elapsed_abstime",
                "ri_diskio_bytesread",
                "ri_diskio_byteswritten",
                "ri_cpu_time_qos_default",
                "ri_cpu_time_qos_maintenance",
                "ri_cpu_time_qos_background",
                "ri_cpu_time_qos_utility",
                "ri_cpu_time_qos_legacy",
                "ri_cpu_time_qos_user_initiated",
                "ri_cpu_time_qos_user_interactive",
                "ri_billed_system_time",
                "ri_serviced_system_time",
                "ri_logical_writes",
                "ri_lifetime_max_phys_footprint",
                "ri_instructions",
                "ri_cycles",
                "ri_billed_energy",
                "ri_serviced_energy",
                "ri_interval_max_phys_footprint",
                "ri_runnable_time",
            )
        ),
    ]


class _ProcTaskInfo(ctypes.Structure):
    _fields_ = [
        ("pti_virtual_size", ctypes.c_uint64),
        ("pti_resident_size", ctypes.c_uint64),
        ("pti_total_user", ctypes.c_uint64),
        ("pti_total_system", ctypes.c_uint64),
        ("pti_threads_user", ctypes.c_uint64),
        ("pti_threads_system", ctypes.c_uint64),
        ("pti_policy", ctypes.c_int32),
        ("pti_faults", ctypes.c_int32),
        ("pti_pageins", ctypes.c_int32),
        ("pti_cow_faults", ctypes.c_int32),
        ("pti_messages_sent", ctypes.c_int32),
        ("pti_messages_received", ctypes.c_int32),
        ("pti_syscalls_mach", ctypes.c_int32),
        ("pti_syscalls_unix", ctypes.c_int32),
        ("pti_csw", ctypes.c_int32),
        ("pti_threadnum", ctypes.c_int32),
        ("pti_numrunning", ctypes.c_int32),
        ("pti_priority", ctypes.c_int32),
    ]


def read_darwin_process_snapshot(pid: int) -> dict[str, int | None]:
    """Read low-overhead, cumulative process counters from Darwin libproc."""

    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise ValueError("process PID must be a positive integer")
    library_name = ctypes.util.find_library("proc") or "/usr/lib/libproc.dylib"
    try:
        libproc = ctypes.CDLL(library_name, use_errno=True)
    except OSError as error:
        raise HostTelemetryError(f"cannot load Darwin libproc: {error}") from error

    proc_pid_rusage = libproc.proc_pid_rusage
    proc_pid_rusage.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.POINTER(_RusageInfoV4),
    ]
    proc_pid_rusage.restype = ctypes.c_int
    usage = _RusageInfoV4()
    if proc_pid_rusage(pid, 4, ctypes.byref(usage)) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number), pid)

    proc_pidinfo = libproc.proc_pidinfo
    proc_pidinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    proc_pidinfo.restype = ctypes.c_int
    task_info = _ProcTaskInfo()
    copied = proc_pidinfo(
        pid,
        4,
        0,
        ctypes.byref(task_info),
        ctypes.sizeof(task_info),
    )
    page_faults: int | None
    if copied == ctypes.sizeof(task_info):
        page_faults = max(0, int(task_info.pti_faults))
    else:
        page_faults = None
    return {
        "process_physical_footprint_bytes": int(usage.ri_phys_footprint),
        "process_physical_footprint_bytes_lifetime_peak": int(
            usage.ri_lifetime_max_phys_footprint
        ),
        "process_resident_memory_bytes": int(usage.ri_resident_size),
        "process_page_faults": page_faults,
        "process_disk_read_bytes": int(usage.ri_diskio_bytesread),
        "process_disk_write_bytes": int(usage.ri_diskio_byteswritten),
    }


def read_darwin_volume_snapshot(
    spill_volume_path: os.PathLike[str] | str,
    *,
    command_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, int | str]:
    """Read cumulative physical-device counters for a mounted spill path."""

    normalized_path = os.fspath(spill_volume_path)
    if not isinstance(normalized_path, str) or not normalized_path:
        raise ValueError("spill-volume path must not be empty")
    disk_info_output = command_runner(
        ["/usr/sbin/diskutil", "info", "-plist", normalized_path],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=10.0,
        check=True,
    ).stdout
    try:
        disk_info = plistlib.loads(disk_info_output.encode("utf-8"))
    except (plistlib.InvalidFileException, UnicodeEncodeError) as error:
        raise HostTelemetryError("diskutil plist output is unrecognized") from error
    devices = _physical_disk_identifiers(disk_info)
    if not devices:
        raise HostTelemetryError("spill volume has no physical whole-disk identifier")

    ioreg_output = command_runner(
        ["/usr/sbin/ioreg", "-r", "-c", "IOBlockStorageDriver", "-l", "-w", "0"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=10.0,
        check=True,
    ).stdout
    counters = [
        _parse_ioreg_storage_statistics(ioreg_output, device) for device in devices
    ]
    return {
        "spill_volume_device": ",".join(devices),
        "spill_volume_read_bytes": sum(pair[0] for pair in counters),
        "spill_volume_write_bytes": sum(pair[1] for pair in counters),
    }


def _physical_disk_identifiers(disk_info: dict[str, Any]) -> list[str]:
    physical_candidates: list[Any] = []
    physical_stores = disk_info.get("APFSPhysicalStores")
    if isinstance(physical_stores, list):
        for store in physical_stores:
            if isinstance(store, str):
                physical_candidates.append(store)
            elif isinstance(store, dict):
                physical_candidates.extend(
                    store.get(field)
                    for field in (
                        "APFSPhysicalStore",
                        "DeviceIdentifier",
                    )
                )
    candidates = physical_candidates or [
        disk_info.get("ParentWholeDisk"),
        disk_info.get("DeviceIdentifier"),
    ]
    devices: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        match = re.fullmatch(r"(disk[0-9]+)(?:s[0-9]+)*", candidate)
        if match is not None and match.group(1) not in devices:
            devices.append(match.group(1))
    return devices


def _parse_ioreg_storage_statistics(output: str, device: str) -> tuple[int, int]:
    blocks = re.split(r"(?m)(?=^\+-o IOBlockStorageDriver\b)", output)
    device_pattern = re.compile(rf'"BSD Name"\s*=\s*"{re.escape(device)}"')
    for block in blocks:
        if device_pattern.search(block) is None:
            continue
        statistics = re.search(r'"Statistics"\s*=\s*\{([^}]*)\}', block)
        if statistics is None:
            continue
        read_match = re.search(r'"Bytes \(Read\)"\s*=\s*([0-9]+)', statistics.group(1))
        write_match = re.search(
            r'"Bytes \(Write\)"\s*=\s*([0-9]+)', statistics.group(1)
        )
        if read_match is not None and write_match is not None:
            return int(read_match.group(1)), int(write_match.group(1))
    raise HostTelemetryError(
        f"ioreg omitted read/write counters for physical device {device}"
    )


def _scaled_bytes(value: str, unit: str) -> int:
    exponent = {"B": 0, "K": 1, "M": 2, "G": 3, "T": 4}.get(unit.upper())
    if exponent is None:
        raise HostTelemetryError(f"unrecognized byte unit: {unit}")
    return round(float(value) * (1024**exponent))


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
        "host_compressed_memory_bytes_peak": max(
            int(sample["host_compressed_memory_bytes"]) for sample in copied_samples
        ),
        "host_compressed_memory_bytes_end": int(last["host_compressed_memory_bytes"]),
        "swap_used_bytes_peak": max(
            int(sample["swap_used_bytes"]) for sample in copied_samples
        ),
        "swap_used_bytes_end": int(last["swap_used_bytes"]),
        "process_physical_footprint_bytes_peak": _optional_max(
            copied_samples,
            "process_physical_footprint_bytes_lifetime_peak",
        ),
        "process_resident_memory_bytes_peak": _optional_max(
            copied_samples, "process_resident_memory_bytes"
        ),
        "process_page_faults_delta": _optional_counter_delta(
            copied_samples, "process_page_faults"
        ),
        "process_disk_read_bytes_delta": _optional_counter_delta(
            copied_samples, "process_disk_read_bytes"
        ),
        "process_disk_write_bytes_delta": _optional_counter_delta(
            copied_samples, "process_disk_write_bytes"
        ),
        "spill_volume_read_bytes_delta": _optional_counter_delta(
            copied_samples, "spill_volume_read_bytes"
        ),
        "spill_volume_write_bytes_delta": _optional_counter_delta(
            copied_samples, "spill_volume_write_bytes"
        ),
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


def _optional_max(samples: list[dict[str, Any]], field: str) -> int | None:
    observed = [int(sample[field]) for sample in samples if sample[field] is not None]
    return max(observed) if observed else None


def _optional_counter_delta(samples: list[dict[str, Any]], field: str) -> int | None:
    observed = [int(sample[field]) for sample in samples if sample[field] is not None]
    if not observed:
        return None
    return max(0, observed[-1] - observed[0])


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
    schema_version = value["schema_version"]
    if schema_version == LEGACY_SCHEMA_VERSION:
        expected_scope = LEGACY_SCOPE
        sample_fields = LEGACY_SAMPLE_FIELDS
        summary_fields = LEGACY_SUMMARY_FIELDS
    elif schema_version == SCHEMA_VERSION:
        expected_scope = SCOPE
        sample_fields = SAMPLE_FIELDS
        summary_fields = SUMMARY_FIELDS
    else:
        raise HostTelemetryError("host telemetry schema version is invalid")
    if value["scope"] != expected_scope:
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
        if not isinstance(sample, dict) or set(sample) != sample_fields:
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
        for field in sample_fields - {"observed_at", "elapsed_seconds"}:
            member = sample[field]
            if (
                schema_version == SCHEMA_VERSION
                and field in _OPTIONAL_SAMPLE_INT_FIELDS
                and member is None
            ):
                continue
            if (
                schema_version == SCHEMA_VERSION
                and field in _OPTIONAL_SAMPLE_STRING_FIELDS
            ):
                if member is not None and (not isinstance(member, str) or not member):
                    raise HostTelemetryError(
                        f"host telemetry sample {field} is invalid"
                    )
                continue
            if isinstance(member, bool) or not isinstance(member, int) or member < 0:
                raise HostTelemetryError(f"host telemetry sample {field} is invalid")
    summary = value["summary"]
    if status == "unavailable":
        if samples or summary is not None or not errors:
            raise HostTelemetryError("unavailable host telemetry is inconsistent")
        return
    if not samples or not isinstance(summary, dict) or set(summary) != summary_fields:
        raise HostTelemetryError("measured host telemetry summary is invalid")
    for field, member in summary.items():
        if (
            schema_version == SCHEMA_VERSION
            and field in _OPTIONAL_SUMMARY_FIELDS
            and member is None
        ):
            continue
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
