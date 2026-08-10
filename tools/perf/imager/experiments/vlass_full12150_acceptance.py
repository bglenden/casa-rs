#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run the single bounded 12,150-pixel all-field VLASS acceptance row.

This is deliberately a direct ``casars-imager`` boundary.  It does not use the
generic memory campaign or benchmark shell because those paths do not carry the
complete CASA-B-v2 science and grouped-Metal execution contract.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Callable


SCRIPT_DIR = Path(__file__).resolve().parent
IMAGER_DIR = SCRIPT_DIR.parent
REPO_ROOT = SCRIPT_DIR.parents[3]
if str(IMAGER_DIR) not in sys.path:
    sys.path.insert(0, str(IMAGER_DIR))

from perf_harness.casa_tclean import (  # noqa: E402
    VOLATILE_TREE_FILE_NAMES,
    canonical_sha256,
)
from perf_harness.host_telemetry import (  # noqa: E402
    HostTelemetryError,
    read_darwin_host_snapshot,
    read_darwin_process_snapshot,
)
from perf_harness.image_compare import compare_products  # noqa: E402


GIB = 1024**3
MIB = 1024**2
CASA_WALL_SECONDS = 82_351.83281429205
MINIMUM_SPEEDUP = 10.0
RUST_WALL_LIMIT_SECONDS = CASA_WALL_SECONDS / MINIMUM_SPEEDUP
MINIMUM_NO_SWAP_HEADROOM_BYTES = 24_000_000_000
HOST_RESERVE_BYTES = 2 * GIB
MAX_TARGET_MIB = 32_000
MIN_INTERNAL_FREE_BYTES = 20 * GIB
MIN_OUTPUT_FREE_BYTES = 100 * GIB
MONITOR_INTERVAL_SECONDS = 15.0
PRESSURE_EXPERIMENT_MONITOR_INTERVAL_SECONDS = 5.0
NO_PROGRESS_SECONDS = 1_800.0
TERMINATE_GRACE_SECONDS = 10.0
NORMAL_MEMORY_PRESSURE_LEVEL = 1
DARWIN_F_NOCACHE = 48

ALL_FIELDS = "1107~1127,1512~1532,1542~1562"
EXPECTED_PRODUCTS = (
    ".alpha",
    ".alpha.error",
    ".image.tt0",
    ".image.tt1",
    ".mask",
    ".model.tt0",
    ".model.tt1",
    ".pb.tt0",
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
    ".residual.tt0",
    ".residual.tt1",
    ".sumwt.tt0",
    ".sumwt.tt1",
    ".sumwt.tt2",
    ".weight.tt0",
    ".weight.tt1",
    ".weight.tt2",
)

MANIFEST_RELATIVE = Path(
    "tools/perf/imager/workloads/vlass-fragment-all-fields-clean-cap20000-casa-v2.json"
)
CONTRACT_RELATIVE = Path(
    "tools/perf/imager/contracts/vlass-scientific-equivalence-v2-mask-topology.json"
)
MANIFEST_SHA256 = "63948fe140d5c06c00b924eea407e5afe8ccb2f99e2c927290d9de4644002053"
CONTRACT_SHA256 = "58cece2f388f6098058598e19e00d4998a8c321f238d062ca8d567cafd29143a"
MS_TREE_SHA256 = "037db124913cdf66de670698536f1bb38c9dbac3725a561fd79eee8bb055fd91"
CF_TREE_SHA256 = "87e550b46efbfc18c09c6d809f0c1f0316876dde358b35de11b727e218c3683b"
MASK_TREE_SHA256 = "fabf361e6609a4d66c251458c2ed31bc80978d936e78a39a8f449bd1a63dc322"
CASA_RECEIPT_SHA256 = "30aaf60c4c29595eb9789bcfe1fdab5723bb761295d4e647e4632b8eb6c31be6"

DEFAULT_ROOT = Path("/Volumes/GLENDENNING/casa-rs-vlass/issue-446")
MS_RELATIVE = Path(
    "data/frozen-clean-b80d5e87487a/"
    "VLASS1.2.sb36484946.eb36542800.58574.4235612037_"
    "ptgfix_split_bright_source.ms"
)
CF_RELATIVE = Path(
    "cf-cache/6.7.5.9/f6f947c5104f8da579f9411dd7087dd331c9e59034073a9fd68b5d6132cd281d"
)
MASK_RELATIVE = Path("masks/vlass-single-field-peak-box.mask")
CASA_RECEIPT_RELATIVE = Path(
    "recovery-references/casa-b-fragment63-clean-cap20000-v2/receipts/"
    "20260802T191330Z-vlass-fragment-all-fields-clean-cap20000-casa-v2-"
    "5a0b3b07.json"
)
CASA_PREFIX_RELATIVE = Path(
    "recovery-references/casa-b-fragment63-clean-cap20000-v2/artifacts/"
    "vlass-fragment-all-fields-clean-cap20000-casa-v2/"
    "casa_deterministic_clean_cap20000_v2_fiducial/"
    "20260802T191330Z-vlass-fragment-all-fields-clean-cap20000-casa-v2-"
    "5a0b3b07/casa/measured-001/casa"
)
DEFAULT_CASA_PYTHON = Path(
    "/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/bin/python"
)
DEFAULT_FFTW_DIR = Path("/opt/homebrew/opt/fftw/lib")

EXPERIMENT_ENVIRONMENT = {
    "CASA_RS_VLASS_EXPERIMENT_RUNNER": "1",
    "CASA_RS_FFTW_THREADS": "8",
    "CASA_RS_STANDARD_MFS_PROFILE_DETAIL": "1",
    "CASA_RS_EXPERIMENTAL_MT_MFS_SPARSE_RHS": "1",
    "CASA_RS_EXPERIMENTAL_MT_MFS_CASA_FFT0": "1",
    "CASA_RS_EXPERIMENTAL_AWPROJECT_PREDIVISION_SOURCE_PHASE": "1",
    "CASA_RS_EXPERIMENTAL_AWPROJECT_RAW_FRAME_TAYLOR": "1",
    "CASA_RS_EXPERIMENTAL_AWPROJECT_RESIDUAL_ONLY_REFRESH": "1",
    "CASA_RS_EXPERIMENTAL_AWPROJECT_REPLAY_RETENTION_BYTES": "0",
    "CASA_RS_EXPERIMENTAL_AWPROJECT_IMAGE_RESPONSE_CACHE": "1",
    "CASA_RS_EXPERIMENTAL_SPARSE_AWPROJECT_MODEL_PREP": "1",
    "CASA_RS_EXPERIMENTAL_SPARSE_MASK_PEAK_SEARCH": "1",
    "CASA_RS_EXPERIMENTAL_CACHE_REFRESHED_NSIGMA": "1",
    "CASA_RS_EXPERIMENTAL_RADIX_MADFM": "1",
    "CASA_RS_EXPERIMENTAL_PARALLEL_MODEL_TERM_FFT": "1",
    "CASA_RS_EXPERIMENTAL_PARALLEL_RESIDUAL_TERM_FFT": "1",
    "CASA_RS_EXPERIMENTAL_AWPROJECT_RESIDUAL_LIVE_CFS_ONLY": "1",
}


class AcceptanceError(RuntimeError):
    """The acceptance contract was not satisfied."""


@dataclass(frozen=True)
class Paths:
    root: Path
    ms: Path
    cf_cache: Path
    mask: Path
    casa_receipt: Path
    casa_prefix: Path
    manifest: Path
    contract: Path
    casa_python: Path
    fftw_dir: Path


@dataclass(frozen=True)
class Baseline:
    first: dict[str, Any]
    second: dict[str, Any]
    pressure_level: int
    no_swap_headroom_bytes: int
    target_mib: int


@dataclass(frozen=True)
class FrozenBinary:
    source_commit: str
    source_tree: str
    cargo_lock_sha256: str
    build_command: list[str]
    path: str
    sha256: str
    size_bytes: int
    mtime_ns: int
    linked_libraries: str


@dataclass(frozen=True)
class MonitorResult:
    exit_code: int
    wall_seconds: float
    stop_reason: str | None
    samples: list[dict[str, Any]]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_file_uncached(path: Path) -> str:
    """Hash a large immutable input without polluting the launch file cache."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        try:
            fcntl.fcntl(handle.fileno(), DARWIN_F_NOCACHE, 1)
        except OSError as error:
            raise AcceptanceError(
                f"cannot disable caching for {path}: {error}"
            ) from error
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def compact_tree_identity_uncached(
    root: Path, *, excluded_names: set[str] | None = None
) -> dict[str, Any]:
    """Match ``tree_identity`` while reading file payloads without caching."""

    excluded_names = excluded_names or set()
    root = root.resolve()
    if not root.is_dir() or root.is_symlink():
        raise AcceptanceError(f"tree root must be a real directory: {root}")
    digest = hashlib.sha256()
    file_count = 0
    total_bytes = 0
    excluded_count = 0
    for path in sorted(root.rglob("*"), key=lambda value: value.as_posix()):
        if path.is_dir() and not path.is_symlink():
            continue
        if path.name in excluded_names and not path.is_dir():
            excluded_count += 1
            continue
        if not path.is_file() or path.is_symlink():
            raise AcceptanceError(f"tree contains a non-regular file: {path}")
        relative = path.relative_to(root).as_posix()
        size = path.stat().st_size
        file_digest = sha256_file_uncached(path)
        digest.update(f"{relative}\0{size}\0{file_digest}\n".encode())
        file_count += 1
        total_bytes += size
    return {
        "tree_sha256": digest.hexdigest(),
        "file_count": file_count,
        "size_bytes": total_bytes,
        "excluded_names": sorted(excluded_names),
        "excluded_count": excluded_count,
    }


def casa_tree_inventory_uncached(root: Path) -> dict[str, Any]:
    """Match the CASA CF inventory while avoiding a 23 GB cache warm-up."""

    root = root.resolve()
    if not root.is_dir() or root.is_symlink():
        raise AcceptanceError(f"CF tree root must be a real directory: {root}")
    entries: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    file_count = 0
    directory_count = 0
    symlink_count = 0
    logical_bytes = 0
    for item in sorted(
        root.rglob("*"), key=lambda path: path.relative_to(root).as_posix()
    ):
        relative = item.relative_to(root).as_posix()
        if item.name in VOLATILE_TREE_FILE_NAMES and not item.is_dir():
            excluded.append(
                {
                    "relative_path": relative,
                    "bytes": int(item.lstat().st_size),
                    "reason": "CASA table.lock is volatile lock state",
                }
            )
            continue
        if item.is_symlink():
            entries.append(
                {
                    "relative_path": relative,
                    "kind": "symlink",
                    "target": os.readlink(item),
                }
            )
            symlink_count += 1
        elif item.is_dir():
            entries.append({"relative_path": relative, "kind": "directory"})
            directory_count += 1
        elif item.is_file():
            size = item.stat().st_size
            entries.append(
                {
                    "relative_path": relative,
                    "kind": "file",
                    "bytes": int(size),
                    "sha256": sha256_file_uncached(item),
                }
            )
            file_count += 1
            logical_bytes += int(size)
        else:
            raise AcceptanceError(f"unsupported filesystem entry: {item}")
    return {
        "exists": True,
        "root": str(root),
        "kind": "directory",
        "stable_tree_sha256": canonical_sha256(entries),
        "included_file_count": file_count,
        "included_directory_count": directory_count,
        "included_symlink_count": symlink_count,
        "logical_bytes": logical_bytes,
        "excluded_volatile": excluded,
        "entries": entries,
        "darwin_f_nocache_applied": True,
    }


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    temporary.replace(path)


def command_output(command: list[str], *, cwd: Path = REPO_ROOT) -> str:
    return subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=True,
        timeout=120.0,
    ).stdout.strip()


def default_paths(root: Path = DEFAULT_ROOT) -> Paths:
    return Paths(
        root=root,
        ms=root / MS_RELATIVE,
        cf_cache=root / CF_RELATIVE,
        mask=root / MASK_RELATIVE,
        casa_receipt=root / CASA_RECEIPT_RELATIVE,
        casa_prefix=root / CASA_PREFIX_RELATIVE,
        manifest=REPO_ROOT / MANIFEST_RELATIVE,
        contract=REPO_ROOT / CONTRACT_RELATIVE,
        casa_python=DEFAULT_CASA_PYTHON,
        fftw_dir=DEFAULT_FFTW_DIR,
    )


def no_swap_headroom_bytes(snapshot: dict[str, Any]) -> int:
    page_size = int(snapshot["page_size_bytes"])
    return (
        sum(
            int(snapshot[name])
            for name in ("pages_free", "pages_inactive", "pages_speculative")
        )
        * page_size
    )


def target_mib_for_headroom(
    headroom_bytes: int, *, allow_pressure_experiment: bool = False
) -> int:
    if allow_pressure_experiment:
        return MAX_TARGET_MIB
    if headroom_bytes < MINIMUM_NO_SWAP_HEADROOM_BYTES:
        raise AcceptanceError(
            f"no-swap headroom {headroom_bytes} is below "
            f"{MINIMUM_NO_SWAP_HEADROOM_BYTES}"
        )
    available = headroom_bytes - HOST_RESERVE_BYTES
    return min(MAX_TARGET_MIB, available // MIB)


def memory_pressure_level(
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> int:
    completed = runner(
        ["/usr/sbin/sysctl", "-n", "kern.memorystatus_vm_pressure_level"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=10.0,
        check=True,
    )
    text = completed.stdout.strip()
    if not text.isdigit():
        raise AcceptanceError(f"unrecognized memory pressure level: {text!r}")
    return int(text)


def validate_baseline_samples(
    first: dict[str, Any],
    second: dict[str, Any],
    pressure_level: int,
    *,
    allow_pressure_experiment: bool = False,
) -> Baseline:
    if pressure_level != NORMAL_MEMORY_PRESSURE_LEVEL:
        raise AcceptanceError(f"memory pressure is not normal: level={pressure_level}")
    if int(second["swapouts"]) != int(first["swapouts"]):
        raise AcceptanceError("swapout activity occurred during the launch baseline")
    if int(second["pages_throttled"]) != int(first["pages_throttled"]):
        raise AcceptanceError("throttled pages increased during the launch baseline")
    headroom = min(no_swap_headroom_bytes(first), no_swap_headroom_bytes(second))
    return Baseline(
        first=first,
        second=second,
        pressure_level=pressure_level,
        no_swap_headroom_bytes=headroom,
        target_mib=target_mib_for_headroom(
            headroom, allow_pressure_experiment=allow_pressure_experiment
        ),
    )


def capture_baseline(
    *,
    allow_pressure_experiment: bool = False,
    snapshot_reader: Callable[[], dict[str, Any]] = read_darwin_host_snapshot,
    sleeper: Callable[[float], None] = time.sleep,
) -> Baseline:
    first = snapshot_reader()
    sleeper(5.0)
    second = snapshot_reader()
    return validate_baseline_samples(
        first,
        second,
        memory_pressure_level(),
        allow_pressure_experiment=allow_pressure_experiment,
    )


def validate_manifest(paths: Paths) -> dict[str, Any]:
    for path in (
        paths.ms,
        paths.cf_cache,
        paths.mask,
        paths.casa_receipt,
        paths.casa_prefix.parent,
        paths.manifest,
        paths.contract,
        paths.casa_python,
        paths.fftw_dir,
    ):
        if not path.exists():
            raise AcceptanceError(f"required acceptance input does not exist: {path}")
    if sha256_file(paths.manifest) != MANIFEST_SHA256:
        raise AcceptanceError("frozen CASA-B-v2 manifest hash differs")
    if sha256_file(paths.contract) != CONTRACT_SHA256:
        raise AcceptanceError("scientific-equivalence contract hash differs")
    if sha256_file(paths.casa_receipt) != CASA_RECEIPT_SHA256:
        raise AcceptanceError("frozen CASA-B receipt hash differs")
    manifest = json.loads(paths.manifest.read_text(encoding="utf-8"))
    imaging = manifest.get("imaging", {})
    comparison = manifest.get("comparison", {})
    required = {
        "field": ALL_FIELDS,
        "spw": "2~17",
        "imsize": 12_150,
        "channel_count": 64,
        "niter": 20_000,
        "usepointing": True,
        "mask_sha256": MASK_TREE_SHA256,
    }
    if any(imaging.get(key) != value for key, value in required.items()):
        raise AcceptanceError(
            "frozen manifest no longer matches the acceptance science"
        )
    if tuple(comparison.get("products", ())) != EXPECTED_PRODUCTS:
        raise AcceptanceError(
            "frozen manifest product inventory is not the ordered 19-set"
        )
    contract = json.loads(paths.contract.read_text(encoding="utf-8"))
    # The CASA run manifest predates the approved v2 scientific-equivalence
    # contract. Its science and product inventory stay immutable, while the
    # comparison uses the separately hash-bound v2 contract.
    comparison["tolerances"] = contract
    return manifest


def validate_input_identities(paths: Paths) -> dict[str, dict[str, Any]]:
    observed = {
        # The frozen MS archive identity includes its 21 inert 2020 table.lock
        # files. The warm CF cache receipt uses the CASA inventory algorithm,
        # while the corrected mask uses the compact tree identity with its two
        # volatile table.lock files excluded. Keep those three earned identity
        # policies distinct.
        "ms": compact_tree_identity_uncached(paths.ms),
        "cf_cache": casa_tree_inventory_uncached(paths.cf_cache),
        "mask": compact_tree_identity_uncached(
            paths.mask, excluded_names={"table.lock"}
        ),
    }
    expected = {
        "ms": MS_TREE_SHA256,
        "cf_cache": CF_TREE_SHA256,
        "mask": MASK_TREE_SHA256,
    }
    for name, expected_hash in expected.items():
        hash_field = "stable_tree_sha256" if name == "cf_cache" else "tree_sha256"
        if observed[name].get(hash_field) != expected_hash:
            raise AcceptanceError(
                f"{name} tree identity differs: "
                f"{observed[name].get(hash_field)} != {expected_hash}"
            )
    return observed


def validate_disk_headroom(paths: Paths) -> None:
    internal = shutil.disk_usage(REPO_ROOT).free
    output = shutil.disk_usage(paths.root).free
    if internal < MIN_INTERNAL_FREE_BYTES:
        raise AcceptanceError(f"internal disk has only {internal} free bytes")
    if output < MIN_OUTPUT_FREE_BYTES:
        raise AcceptanceError(f"output disk has only {output} free bytes")


def clean_source_identity() -> tuple[str, str]:
    if command_output(["git", "status", "--porcelain"]):
        raise AcceptanceError("release evidence requires a clean source worktree")
    return (
        command_output(["git", "rev-parse", "HEAD"]),
        command_output(["git", "rev-parse", "HEAD^{tree}"]),
    )


def build_and_freeze_binary(run_root: Path) -> FrozenBinary:
    source_commit, source_tree = clean_source_identity()
    build_command = [
        "cargo",
        "build",
        "--locked",
        "--release",
        "-p",
        "casars-imager",
        "--bin",
        "casars-imager",
    ]
    environment = dict(os.environ)
    environment["CARGO_INCREMENTAL"] = "0"
    subprocess.run(build_command, cwd=REPO_ROOT, env=environment, check=True)
    if clean_source_identity() != (source_commit, source_tree):
        raise AcceptanceError("source identity changed during the release build")
    source = REPO_ROOT / "target/release/casars-imager"
    frozen_dir = run_root / "frozen"
    frozen_dir.mkdir(parents=True, exist_ok=False)
    frozen = frozen_dir / f"casars-imager-{source_commit}"
    shutil.copy2(source, frozen)
    frozen.chmod(0o555)
    stat = frozen.stat()
    return FrozenBinary(
        source_commit=source_commit,
        source_tree=source_tree,
        cargo_lock_sha256=sha256_file(REPO_ROOT / "Cargo.lock"),
        build_command=build_command,
        path=str(frozen),
        sha256=sha256_file(frozen),
        size_bytes=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        linked_libraries=command_output(["/usr/bin/otool", "-L", str(frozen)]),
    )


def restricted_environment(paths: Paths) -> dict[str, str]:
    environment = {
        "PATH": "/opt/homebrew/bin:/usr/bin:/bin",
        "HOME": str(Path.home()),
        "CASA_RS_MEASURESPATH": str(Path.home() / ".casa/data"),
        "CASA_RS_FFTW_LIBRARY_DIR": str(paths.fftw_dir),
        "DYLD_LIBRARY_PATH": str(paths.fftw_dir),
    }
    environment.update(EXPERIMENT_ENVIRONMENT)
    return environment


def common_imager_command(
    binary: Path,
    paths: Paths,
    output_prefix: Path,
    target_mib: int,
    *,
    memory_pressure_policy: str = "conservative-no-swap",
) -> list[str]:
    return [
        str(binary),
        "--ms",
        str(paths.ms),
        "--imagename",
        str(output_prefix),
        "--imsize",
        "12150",
        "--cell-arcsec",
        "0.6",
        "--field",
        ALL_FIELDS,
        "--phasecenter-field",
        "1525",
        "--spw",
        "2~17",
        "--channel-start",
        "0",
        "--channel-count",
        "64",
        "--specmode",
        "mfs",
        "--gridder",
        "awproject",
        "--interpolation",
        "linear",
        "--projection",
        "SIN",
        "--datacolumn",
        "data",
        "--stokes",
        "I",
        "--uvrange",
        "<12km",
        "--intent",
        "OBSERVE_TARGET#UNSPECIFIED",
        "--usepointing",
        "--weighting",
        "briggs",
        "--robust",
        "1.0",
        "--perchanweightdensity",
        "--deconvolver",
        "mtmfs",
        "--standard-mfs-acceleration",
        "metal",
        "--standard-mfs-residual-backend",
        "metal-row-run-grouped",
        "--imaging-fft-precision",
        "f64",
        "--imaging-fft-backend",
        "fftw",
        "--parallel",
        "--standard-mfs-grid-threads",
        "7",
        "--imaging-memory-target-mb",
        str(target_mib),
        "--imaging-memory-pressure-policy",
        memory_pressure_policy,
        "--imaging-prepare-workers",
        "1",
        "--imaging-read-ahead-blocks",
        "1",
        "--hogbom-iteration-mode",
        "strict",
        "--nterms",
        "2",
        "--scales",
        "0,5,12",
        "--niter",
        "20000",
        "--gain",
        "0.1",
        "--threshold-jy",
        "0.0",
        "--nsigma",
        "5.0",
        "--psfcutoff",
        "0.35",
        "--pblimit",
        "0.0001",
        "--write-pb",
        "--minor-cycle-length",
        "2000",
        "--cyclefactor",
        "3.0",
        "--minpsffraction",
        "0.05",
        "--maxpsffraction",
        "0.8",
        "--wterm",
        "wproject",
        "--wprojplanes",
        "32",
        "--cfcache",
        str(paths.cf_cache),
        "--cf-resident-mb",
        "256",
        "--facets",
        "1",
        "--computepastep",
        "360.0",
        "--rotatepastep",
        "360.0",
        "--pointingoffsetsigdev",
        "0.0",
        "--normtype",
        "flatnoise",
        "--aterm",
        "--no-psterm",
        "--wbawp",
        "--conjbeams",
        "--no-mosweight",
        "--smallscalebias",
        "0.0",
        "--usemask",
        "user",
        "--mask-image",
        str(paths.mask),
        "--savemodel",
        "none",
        "--restoringbeam",
        "common",
        "--no-preview-pngs",
    ]


def probe_command(common: list[str]) -> list[str]:
    return [*common, "--standard-mfs-plan-probe", "true"]


def run_command(common: list[str], progress_path: Path) -> list[str]:
    return [
        *common,
        "--progress",
        "true",
        "--progress-jsonl",
        str(progress_path),
        "--progress-detail",
        "diagnostic",
        "--progress-min-interval-ms",
        "30000",
    ]


def key_values(line: str) -> dict[str, str]:
    return dict(re.findall(r"(?:^|\s)([A-Za-z0-9_]+)=([^\s]+)", line))


def matching_lines(text: str, prefix: str) -> list[dict[str, str]]:
    return [key_values(line) for line in text.splitlines() if line.startswith(prefix)]


def validate_probe_log(
    text: str,
    target_mib: int,
    *,
    memory_pressure_policy: str = "conservative-no-swap",
    require_target_within_headroom: bool = True,
) -> dict[str, Any]:
    preflight = matching_lines(text, "standard_mfs_planner_preflight ")
    resources = matching_lines(text, "standard_mfs_planning_resources ")
    runtime = matching_lines(text, "standard_mfs_runtime_plan ")
    decisions = matching_lines(text, "standard_mfs_execution_decision ")
    grouped = matching_lines(text, "awproject_grouped_replay_plan ")
    if len(preflight) != 1 or preflight[0].get("status") != "admitted":
        raise AcceptanceError("plan probe did not emit one admitted preflight")
    expected_preflight = {
        "grouped_metal_status": "admitted",
        "rows_total": "655200",
        "ddids": "16",
        "selected_channels": "64",
        "correlations": "4",
        "memory_pressure_policy": memory_pressure_policy,
        "visibility_streamed": "false",
        "replay_compiled": "false",
        "grids_allocated": "false",
        "products_materialized": "false",
    }
    if any(preflight[0].get(key) != value for key, value in expected_preflight.items()):
        raise AcceptanceError("plan probe topology or allocation-free receipt differs")
    if len(resources) != 1:
        raise AcceptanceError("plan probe omitted its planning-resource receipt")
    target_bytes = target_mib * MIB
    if resources[0].get("memory_target_bytes") != str(target_bytes):
        raise AcceptanceError("plan probe changed the requested memory target")
    expected_target_origin = (
        "cli-intentional-oversubscription"
        if memory_pressure_policy == "oversubscribe"
        else "cli-imaging"
    )
    if resources[0].get("memory_target_origin") != expected_target_origin:
        raise AcceptanceError("plan probe capped or relabeled the requested target")
    headroom = int(resources[0].get("no_swap_headroom_bytes", "-1"))
    if require_target_within_headroom and target_bytes > headroom:
        raise AcceptanceError("plan probe target exceeds its fresh no-swap headroom")
    if (
        len(runtime) != 1
        or runtime[0].get("initial_dirty_backend") != "metal-row-run-grouped"
    ):
        raise AcceptanceError(
            "plan probe did not select the source-major Metal initial grid"
        )
    if runtime[0].get("residual_backend") != "metal-row-run-grouped":
        raise AcceptanceError("plan probe did not select grouped Metal residual replay")
    if len(grouped) != 1:
        raise AcceptanceError("plan probe omitted the grouped replay plan")
    if grouped[0].get("architecture") != "source-order-grouped-tile-v1":
        raise AcceptanceError("grouped replay architecture differs")
    if grouped[0].get("tile_side") != "11":
        raise AcceptanceError("grouped replay tile side differs")
    if float(grouped[0].get("omitted_squared_l2_energy", "nan")) != 0.0:
        raise AcceptanceError("grouped replay is not exact-support")
    by_name = {decision.get("name"): decision.get("value") for decision in decisions}
    expected_decisions = {
        "awproject_selected_field_count": "63",
        "awproject_initial_grid_backend": "source-major-grouped-metal-f64",
        "awproject_source_major_architecture": "direct-source-major-v3-high-only-initial",
        "awproject_source_major_initial_accumulation": "high-limb-only",
        "awproject_source_major_initial_grid_bytes": "9447840000",
        "awproject_multifield_initial_grid_admission": "admitted",
        "awproject_grouped_replay_replaced_generic_caches": "true",
        "awproject_grouped_metal_generic_scratch_bytes": "0",
        "awproject_grouped_metal_residual_output_bytes": "2361960000",
        "awproject_grouped_metal_residual_compensation_bytes": "2361960000",
        "awproject_grouped_metal_model_wrapper_bytes": "2361960000",
        "awproject_grouped_metal_safety_reserve_bytes": str(64 * MIB),
    }
    if any(by_name.get(key) != value for key, value in expected_decisions.items()):
        raise AcceptanceError("plan probe grouped-Metal decisions differ")
    return {
        "preflight": preflight[0],
        "resources": resources[0],
        "runtime": runtime[0],
        "decisions": by_name,
        "grouped": grouped[0],
    }


def run_probe(
    command: list[str],
    environment: dict[str, str],
    log_path: Path,
    target_mib: int,
    *,
    memory_pressure_policy: str = "conservative-no-swap",
    require_target_within_headroom: bool = True,
) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=900.0,
        check=False,
    )
    log_path.write_text(completed.stdout, encoding="utf-8")
    if completed.returncode != 0:
        raise AcceptanceError(f"plan probe exited {completed.returncode}")
    return validate_probe_log(
        completed.stdout,
        target_mib,
        memory_pressure_policy=memory_pressure_policy,
        require_target_within_headroom=require_target_within_headroom,
    )


def terminate_group(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=TERMINATE_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        process.wait()


def _drain_output(stream: Any, log: Any, activity: queue.SimpleQueue[float]) -> None:
    for line in iter(stream.readline, ""):
        log.write(line)
        log.flush()
        print(line, end="", flush=True)
        activity.put(time.monotonic())
    stream.close()


def monitor_stop_reason(
    *,
    baseline: Baseline,
    sample: dict[str, Any],
    pressure_level: int,
    swap_used_growth_samples: int,
    max_compressed_growth_bytes: int | None = None,
) -> str | None:
    if pressure_level != NORMAL_MEMORY_PRESSURE_LEVEL:
        return f"memory pressure escalated to level {pressure_level}"
    if int(sample["pages_throttled"]) > int(baseline.second["pages_throttled"]):
        return "Pages throttled increased"
    if int(sample["swapouts"]) > int(baseline.second["swapouts"]):
        return "swapout activity began"
    if swap_used_growth_samples >= 2:
        return "swap-used bytes increased in two consecutive samples"
    if max_compressed_growth_bytes is not None:
        compressed_growth = int(sample["host_compressed_memory_bytes"]) - int(
            baseline.second["host_compressed_memory_bytes"]
        )
        if compressed_growth > max_compressed_growth_bytes:
            return (
                "host compressed-memory growth exceeded "
                f"{max_compressed_growth_bytes} bytes"
            )
    if no_swap_headroom_bytes(sample) < HOST_RESERVE_BYTES:
        return "remaining no-swap headroom fell below 2 GiB"
    return None


def monitor_run(
    command: list[str],
    *,
    environment: dict[str, str],
    baseline: Baseline,
    log_path: Path,
    progress_path: Path,
    telemetry_path: Path,
    interval_seconds: float = MONITOR_INTERVAL_SECONDS,
    max_compressed_growth_bytes: int | None = None,
    wall_limit_seconds: float = RUST_WALL_LIMIT_SECONDS,
    snapshot_reader: Callable[[], dict[str, Any]] = read_darwin_host_snapshot,
) -> MonitorResult:
    samples: list[dict[str, Any]] = []
    activity: queue.SimpleQueue[float] = queue.SimpleQueue()
    started = time.monotonic()
    last_activity = started
    last_progress_mtime = 0
    low_activity_samples = 0
    swap_used_growth_samples = 0
    stop_reason: str | None = None
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            cwd=REPO_ROOT,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            start_new_session=True,
        )
        assert process.stdout is not None
        reader = threading.Thread(
            target=_drain_output,
            args=(process.stdout, log, activity),
            daemon=True,
        )
        reader.start()
        last_swap_used = int(baseline.second["swap_used_bytes"])
        while process.poll() is None:
            time.sleep(interval_seconds)
            if process.poll() is not None:
                break
            now = time.monotonic()
            while True:
                try:
                    last_activity = max(last_activity, activity.get_nowait())
                except queue.Empty:
                    break
            if progress_path.exists():
                mtime = progress_path.stat().st_mtime_ns
                if mtime > last_progress_mtime:
                    last_activity = now
                    last_progress_mtime = mtime
            try:
                sample = snapshot_reader()
                level = memory_pressure_level()
                process_sample = read_darwin_process_snapshot(process.pid)
            except (HostTelemetryError, OSError, subprocess.SubprocessError) as error:
                stop_reason = f"required telemetry failed: {error}"
                terminate_group(process)
                break
            swap_used = int(sample["swap_used_bytes"])
            swap_used_growth_samples = (
                swap_used_growth_samples + 1 if swap_used > last_swap_used else 0
            )
            last_swap_used = swap_used
            record = {
                "elapsed_seconds": now - started,
                "pressure_level": level,
                "no_swap_headroom_bytes": no_swap_headroom_bytes(sample),
                **sample,
                **process_sample,
            }
            samples.append(record)
            atomic_json(telemetry_path, {"samples": samples})
            stop_reason = monitor_stop_reason(
                baseline=baseline,
                sample=sample,
                pressure_level=level,
                swap_used_growth_samples=swap_used_growth_samples,
                max_compressed_growth_bytes=max_compressed_growth_bytes,
            )
            if stop_reason is None and now - started >= wall_limit_seconds:
                stop_reason = "10x acceptance wall exceeded"
            disk_idle = len(samples) < 2 or (
                record.get("process_disk_read_bytes")
                == samples[-2].get("process_disk_read_bytes")
                and record.get("process_disk_write_bytes")
                == samples[-2].get("process_disk_write_bytes")
            )
            if now - last_activity >= NO_PROGRESS_SECONDS and disk_idle:
                low_activity_samples += 1
            else:
                low_activity_samples = 0
            if stop_reason is None and low_activity_samples >= 2:
                stop_reason = (
                    "semantic progress and process I/O were idle for 30 minutes"
                )
            if stop_reason is not None:
                terminate_group(process)
                break
        exit_code = process.wait()
        reader.join(timeout=5.0)
    return MonitorResult(
        exit_code=exit_code,
        wall_seconds=time.monotonic() - started,
        stop_reason=stop_reason,
        samples=samples,
    )


def validate_runtime_log(text: str) -> dict[str, Any]:
    source_blocks = matching_lines(text, "awproject_source_major_block ")
    initial_readback = matching_lines(text, "awproject_metal_initial_readback ")
    sealed = matching_lines(text, "awproject_grouped_metal_admission phase=sealed ")
    runtime = matching_lines(text, "awproject_grouped_metal_admission phase=runtime ")
    host = matching_lines(text, "awproject_grouped_metal_host_lifetime ")
    support = matching_lines(text, "awproject_effective_support ")
    aot = matching_lines(text, "awproject_aot_grouped_tile_receipt ")
    retention = matching_lines(text, "awproject_metal_grouped_replay_retention ")
    summaries = matching_lines(text, "awproject_metal_resident_grouped_replay_summary ")
    if not source_blocks:
        raise AcceptanceError("runtime log omitted source-major initial blocks")
    if {int(entry["source_block"]) for entry in source_blocks} != set(
        range(len(source_blocks))
    ):
        raise AcceptanceError(
            "source-major initial blocks are not unique and contiguous"
        )
    for entry in source_blocks:
        if (
            entry.get("architecture") != "direct-source-major-v3-high-only-initial"
            or entry.get("initial_accumulation") != "high-limb-only"
            or entry.get("initial_partitions") != "2"
            or entry.get("initial_grid_bytes") != "9447840000"
            or entry.get("initial_compensation_bytes") != "0"
            or entry.get("spill_bytes") != "0"
            or entry.get("reload_bytes") != "0"
        ):
            raise AcceptanceError("source-major high-only initial receipt differs")
    if (
        len(initial_readback) != 1
        or initial_readback[0].get("residency") != "metal-shared-high-limb-only-grid"
        or initial_readback[0].get("resident_bytes") != "9447840000"
    ):
        raise AcceptanceError("high-only initial readback receipt differs")
    if not sealed:
        raise AcceptanceError("runtime log omitted sealed grouped segments")
    segments = {int(entry["segment"]) for entry in sealed}
    if segments != set(range(len(sealed))):
        raise AcceptanceError("sealed grouped segments are not unique and contiguous")
    for entry in sealed:
        if entry.get("all_fit") != "true":
            raise AcceptanceError("a sealed grouped segment did not fit")
        if int(entry["source_boundary_upper_bytes"]) < int(
            entry["exact_additional_bytes"]
        ):
            raise AcceptanceError("sealed exact bytes exceed the source-bound upper")
    if {int(entry["segment"]) for entry in support} != segments:
        raise AcceptanceError("exact-support receipts do not cover every segment")
    for entry in support:
        if float(entry.get("omitted_energy_fraction", "nan")) != 0.0:
            raise AcceptanceError("runtime support compiler omitted CF energy")
        if float(entry.get("max_omitted_energy_fraction", "nan")) != 0.0:
            raise AcceptanceError(
                "runtime support compiler reported an omitted stencil"
            )
        for name in ("prediction_cropped_plans", "tile_cropped_plans"):
            if entry.get(name) != "0":
                raise AcceptanceError("exact-support compiler cropped a plan")
        for prefix in ("prediction", "tile"):
            if int(entry.get(f"{prefix}_plans", "0")) <= 0:
                raise AcceptanceError("exact-support compiler reported no plans")
            original = int(entry.get(f"{prefix}_original_tap_visits", "-1"))
            retained = int(entry.get(f"{prefix}_retained_tap_visits", "-2"))
            if original <= 0 or original != retained:
                raise AcceptanceError("exact-support compiler changed tap visits")
        kernel_before = int(entry.get("resident_kernel_bytes_before", "0"))
        if kernel_before <= 0 or entry.get("resident_kernel_bytes_after") != str(
            kernel_before
        ):
            raise AcceptanceError("exact-support compiler changed kernel residency")
    if {int(entry["segment"]) for entry in aot} != segments:
        raise AcceptanceError("AOT receipts do not cover every segment")
    if any(entry.get("omitted_energy_fraction_bits") != "0" for entry in aot):
        raise AcceptanceError("AOT receipt is not exact-support")
    for entry in aot:
        if entry.get("grouped_plans_hash_prefix") != entry.get(
            "legacy_grouped_plans_hash_prefix"
        ):
            raise AcceptanceError("AOT grouped plans differ from the exact compiler")
        if entry.get("grouped_route_hash_prefix") != entry.get(
            "legacy_grouped_route_hash_prefix"
        ):
            raise AcceptanceError("AOT grouped route differs from the exact compiler")
        if int(entry.get("compile_transient_bytes_peak_estimated", "-1")) > int(
            entry.get("compile_admission_limit_bytes", "-2")
        ):
            raise AcceptanceError("AOT compiler exceeded its admitted memory")
    if len(retention) != 1 or retention[0].get("decision") != "resident-complete":
        raise AcceptanceError("complete resident grouped retention was not established")
    if int(retention[0].get("segments", "-1")) != len(segments):
        raise AcceptanceError("resident segment count differs from sealed count")
    if not runtime or not host or not summaries:
        raise AcceptanceError("runtime grouped replay receipts are incomplete")
    expected_dispatches = len(segments) * len(summaries)
    if len(runtime) != expected_dispatches or len(host) != expected_dispatches:
        raise AcceptanceError("not every resident refresh covered every sealed segment")
    if any(int(entry.get("segments", "-1")) != len(segments) for entry in summaries):
        raise AcceptanceError("a resident refresh omitted a sealed segment")
    retained_program_bytes = retention[0].get("program_bytes")
    if any(entry.get("program_bytes") != retained_program_bytes for entry in summaries):
        raise AcceptanceError("resident refresh bytes differ from retained bytes")
    for entry in runtime:
        if entry.get("all_fit") != "true" or entry.get("prechecks") != "fit":
            raise AcceptanceError("runtime grouped Metal check failed")
        if entry.get("postchecks") != "fit":
            raise AcceptanceError("runtime grouped Metal postcheck failed")
        if entry.get("host_bytes_retained_during_tile") != "0":
            raise AcceptanceError("host prediction bytes overlapped tile dispatch")
        for phase in ("persistent", "prediction", "tile"):
            if int(entry[f"{phase}_post_combined_bytes"]) > int(
                entry[f"{phase}_maximum_current_bytes"]
            ):
                raise AcceptanceError(
                    f"{phase} postallocation check exceeded its bound"
                )
    for entry in host:
        required = (
            "dispatch_released_before_tile",
            "candidate_auxiliary_released_before_tile",
            "candidate_result_released_before_tile",
        )
        if any(entry.get(name) != "true" for name in required):
            raise AcceptanceError(
                "host prediction lifetime was not released before tile"
            )
        if entry.get("host_bytes_retained_during_tile") != "0":
            raise AcceptanceError("host lifetime receipt retained tile overlap")
        if entry.get("candidate_audit_allocation_bytes") != "0":
            raise AcceptanceError(
                "production grouped replay allocated a candidate audit"
            )
    for entry in summaries:
        if entry.get("spill_read_bytes") != "0":
            raise AcceptanceError("resident grouped replay read spill during refresh")
        for name in (
            "runtime_grouping_builds",
            "runtime_sort_builds",
            "runtime_route_builds",
        ):
            if entry.get(name) != "0":
                raise AcceptanceError(
                    "resident grouped replay rebuilt runtime topology"
                )
    products = matching_lines(text, "image_product_write ")
    suffixes = tuple(entry.get("suffix") for entry in products)
    if suffixes != EXPECTED_PRODUCTS:
        raise AcceptanceError("runtime did not write the ordered 19-product inventory")
    for entry in products:
        expected_shape = (
            "1x1x1x1"
            if entry.get("suffix", "").startswith(".sumwt")
            else "12150x12150x1x1"
        )
        if entry.get("shape") != expected_shape:
            raise AcceptanceError("runtime product geometry differs")
    return {
        "segment_count": len(segments),
        "runtime_dispatch_count": len(runtime),
        "refresh_count": len(summaries),
        "product_count": len(products),
    }


def comparison_request(
    manifest: dict[str, Any], rust_prefix: Path, casa_prefix: Path, run_root: Path
) -> dict[str, Any]:
    comparison = manifest["comparison"]
    return {
        "rust_prefix": str(rust_prefix),
        "casa_prefix": str(casa_prefix),
        "products": comparison["products"],
        "max_elements_per_product": comparison["max_elements_per_product"],
        "mode": comparison["mode"],
        "full_chunk_elements": comparison["full_chunk_elements"],
        "require_exact_product_inventory": comparison[
            "require_exact_product_inventory"
        ],
        "require_metadata_parity": comparison["require_metadata_parity"],
        "source_regions": comparison["source_regions"],
        "tolerances": comparison["tolerances"],
        "panel_dir": str(run_root / "comparison/panels"),
        "structure_workspace_dir": str(run_root / "comparison/structure-workspace"),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="after all immutable and planner gates, run the single bounded row",
    )
    parser.add_argument(
        "--allow-pressure-experiment",
        action="store_true",
        help=(
            "bypass only the unvalidated 24 GB host-headroom floor, request the "
            "32,000 MiB oversubscribe planner envelope, and retain fail-fast "
            "runtime pressure, compression, throttling, and swap guards"
        ),
    )
    return parser.parse_args(argv)


def run_acceptance(args: argparse.Namespace) -> Path:
    paths = default_paths(args.root.resolve())
    run_root = paths.root / "recovery-candidates/acceptance" / args.run_id
    if run_root.exists():
        raise AcceptanceError(f"refusing to overwrite acceptance run root: {run_root}")
    run_root.mkdir(parents=True)
    manifest = validate_manifest(paths)
    validate_disk_headroom(paths)
    # Build and all multi-gigabyte identity scans precede the fresh baseline.
    # Neither their memory pressure nor their cache warming can make the
    # conservative launch snapshot appear healthier than it is.
    frozen = build_and_freeze_binary(run_root)
    binary = Path(frozen.path)
    identities_before = validate_input_identities(paths)
    baseline = capture_baseline(
        allow_pressure_experiment=args.allow_pressure_experiment
    )
    output_prefix = run_root / "products/rust"
    output_prefix.parent.mkdir(parents=True)
    memory_pressure_policy = (
        "oversubscribe" if args.allow_pressure_experiment else "conservative-no-swap"
    )
    common = common_imager_command(
        binary,
        paths,
        output_prefix,
        baseline.target_mib,
        memory_pressure_policy=memory_pressure_policy,
    )
    environment = restricted_environment(paths)
    probe_log = run_root / "probe.log"
    probe = run_probe(
        probe_command(common),
        environment,
        probe_log,
        baseline.target_mib,
        memory_pressure_policy=memory_pressure_policy,
        require_target_within_headroom=not args.allow_pressure_experiment,
    )
    if sha256_file(binary) != frozen.sha256:
        raise AcceptanceError("frozen binary changed during the plan probe")
    preflight_receipt = {
        "kind": "vlass_full12150_all63_acceptance_preflight",
        "status": "admitted",
        "execute_requested": args.execute,
        "evidence_class": (
            "pressure-experiment" if args.allow_pressure_experiment else "acceptance"
        ),
        "acceptance_eligible": not args.allow_pressure_experiment,
        "memory_pressure_policy": memory_pressure_policy,
        "paths": {key: str(value) for key, value in asdict(paths).items()},
        "manifest_sha256": MANIFEST_SHA256,
        "contract_sha256": CONTRACT_SHA256,
        "casa_receipt_sha256": CASA_RECEIPT_SHA256,
        "casa_wall_seconds": CASA_WALL_SECONDS,
        "binary": asdict(frozen),
        "identities_before": identities_before,
        "baseline": asdict(baseline),
        "environment": environment,
        "common_command": common,
        "probe_command": probe_command(common),
        "probe": probe,
        "probe_log_sha256": sha256_file(probe_log),
    }
    atomic_json(run_root / "preflight.json", preflight_receipt)
    if not args.execute:
        return run_root / "preflight.json"

    # The executable replans from a fresh snapshot.  The immutable CLI target
    # remains identical, so any reduced headroom makes the real process fail
    # before allocation rather than silently selecting a different plan.
    launch_baseline = capture_baseline(
        allow_pressure_experiment=args.allow_pressure_experiment
    )
    if (
        not args.allow_pressure_experiment
        and launch_baseline.target_mib < baseline.target_mib
    ):
        raise AcceptanceError("host headroom worsened after the accepted plan probe")
    if sha256_file(binary) != frozen.sha256:
        raise AcceptanceError("frozen binary changed before timed execution")
    progress_path = run_root / "progress.jsonl"
    run_log = run_root / "casa-rs.log"
    monitor = monitor_run(
        run_command(common, progress_path),
        environment=environment,
        baseline=launch_baseline,
        log_path=run_log,
        progress_path=progress_path,
        telemetry_path=run_root / "telemetry.json",
        interval_seconds=(
            PRESSURE_EXPERIMENT_MONITOR_INTERVAL_SECONDS
            if args.allow_pressure_experiment
            else MONITOR_INTERVAL_SECONDS
        ),
        # Compression is measured, but is not destructive by itself. The
        # pressure experiment still fails immediately on swapout, throttling,
        # non-normal pressure, or loss of the 2 GiB no-swap reserve.
        max_compressed_growth_bytes=None,
    )
    execution = {
        "monitor": asdict(monitor),
        "binary_sha256_after": sha256_file(binary),
        "log_sha256": sha256_file(run_log),
        "speedup": CASA_WALL_SECONDS / monitor.wall_seconds,
    }
    atomic_json(run_root / "execution.json", execution)
    if monitor.stop_reason is not None or monitor.exit_code != 0:
        raise AcceptanceError(
            f"bounded execution failed: exit={monitor.exit_code}, "
            f"stop={monitor.stop_reason}"
        )
    if monitor.wall_seconds > RUST_WALL_LIMIT_SECONDS:
        raise AcceptanceError("successful execution missed the 10x wall")
    runtime = validate_runtime_log(run_log.read_text(encoding="utf-8"))
    identities_after = validate_input_identities(paths)
    if identities_after != identities_before:
        raise AcceptanceError("an immutable input identity changed during execution")
    request = comparison_request(manifest, output_prefix, paths.casa_prefix, run_root)
    comparison = compare_products(
        casa_python=paths.casa_python,
        request=request,
        artifact_prefix=run_root / "comparison/result",
        cwd=REPO_ROOT,
    )
    evaluation = comparison.get("tolerance_evaluation", {})
    if comparison.get("status") != "completed" or evaluation.get("status") != "passed":
        atomic_json(run_root / "comparison.json", comparison)
        raise AcceptanceError("the 19-product scientific comparison did not pass")
    receipt = {
        "kind": "vlass_full12150_all63_acceptance",
        "status": (
            "passed-pressure-experiment" if args.allow_pressure_experiment else "passed"
        ),
        "acceptance_eligible": not args.allow_pressure_experiment,
        "preflight": preflight_receipt,
        "launch_baseline": asdict(launch_baseline),
        "execution": execution,
        "runtime_contract": runtime,
        "identities_after": identities_after,
        "comparison_request": request,
        "comparison": comparison,
    }
    atomic_json(run_root / "receipt.json", receipt)
    return run_root / "receipt.json"


def main(argv: list[str] | None = None) -> int:
    try:
        path = run_acceptance(parse_args(argv))
    except (
        AcceptanceError,
        HostTelemetryError,
        OSError,
        subprocess.SubprocessError,
    ) as error:
        print(f"vlass full12150 acceptance: {error}", file=sys.stderr)
        return 2
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
