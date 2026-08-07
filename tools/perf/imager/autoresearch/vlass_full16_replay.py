#!/usr/bin/env python3
"""Capture and benchmark the frozen VLASS exact AW replay fixtures."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import struct
import subprocess
import sys
import time
import uuid
from typing import Any


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
IMAGER_TOOLS_ROOT = SCRIPT_DIR.parent
REPO_ROOT = SCRIPT_DIR.parents[3]
if str(IMAGER_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(IMAGER_TOOLS_ROOT))

from perf_harness.artifacts import atomic_write_json  # noqa: E402
from perf_harness.host_telemetry import DarwinHostTelemetrySampler  # noqa: E402
from perf_harness.tree_identity import tree_identity  # noqa: E402


EXTERNAL_ROOT = pathlib.Path(
    "/Volumes/GLENDENNING/casa-rs-vlass/issue-446/autoresearch/vlass-full16-replay-v1"
)
FIXTURE_ROOT = EXTERNAL_ROOT / "fixtures"
RUNS_ROOT = EXTERNAL_ROOT / "runs"
TARGET_DIR = EXTERNAL_ROOT / "target"
BASELINE_PATH = EXTERNAL_ROOT / "baseline.json"
LATEST_PATH = EXTERNAL_ROOT / "latest.json"
AOT_GROUPED_SIDECAR_FULL16_PREFIX_ENV = (
    "CASA_RS_EXPERIMENTAL_AWPROJECT_AOT_GROUPED_SIDECAR_FULL16_PREFIX"
)
AOT_GROUPED_SIDECAR_FOUR_SPW_PREFIX_ENV = (
    "CASA_RS_EXPERIMENTAL_AWPROJECT_AOT_GROUPED_SIDECAR_FOUR_SPW_PREFIX"
)
AOT_GROUPED_RAW_PAYLOAD_SHA256_ENV = (
    "CASA_RS_EXPERIMENTAL_AWPROJECT_AOT_GROUPED_RAW_PAYLOAD_SHA256"
)
AOT_GROUPED_COMPILE_RAW_PREFIX_ENV = (
    "CASA_RS_EXPERIMENTAL_AWPROJECT_AOT_GROUPED_COMPILE_RAW_PREFIX"
)
AOT_GROUPED_COMPILE_SIDECAR_PREFIX_ENV = (
    "CASA_RS_EXPERIMENTAL_AWPROJECT_AOT_GROUPED_COMPILE_SIDECAR_PREFIX"
)
AOT_GROUPED_COMPILER_BINARY_SHA256_ENV = (
    "CASA_RS_EXPERIMENTAL_AWPROJECT_AOT_GROUPED_COMPILER_BINARY_SHA256"
)
AOT_GROUPED_SIDECAR_SCHEMA = "casa-rs-vlass-aw-replay-aot-grouped-tile-sidecar-v3"
AOT_GROUPED_COMPILER_CONTRACT = (
    "effective-support-1e-6-incumbent-groups-source-role-map-v1"
)
AOT_GROUPED_SIDECAR_SUFFIX = "aot-grouped-tile-1e-6-v3"
AOT_GROUPED_THRESHOLD_BITS = struct.unpack(">Q", struct.pack(">d", 1.0e-6))[0]
MEASURES_DIR = pathlib.Path.home() / ".casa/data"
FFTW_DIR = pathlib.Path(
    "/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/"
    "venv-py312/lib/python3.12/site-packages/casatools/__casac__/lib"
)
ISSUE_ROOT = pathlib.Path("/Volumes/GLENDENNING/casa-rs-vlass/issue-446")
MS = (
    ISSUE_ROOT / "data/frozen-clean-b80d5e87487a/"
    "VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
)
MASK = ISSUE_ROOT / "masks/vlass-source-box-4096-spectral.mask"
FIELD_SELECTION = "1107~1127,1512~1532,1542~1562"
FIELD_IDS = [
    *range(1107, 1128),
    *range(1512, 1533),
    *range(1542, 1563),
]
FULL16 = {
    "id": "full16",
    "spw_selection": "2~17",
    "spw_ids": list(range(2, 18)),
    "expected_samples": 25_030_848,
    "expected_segments": 10,
    "cf_cache": ISSUE_ROOT / "cf-cache/6.7.5.9/"
    "f6f947c5104f8da579f9411dd7087dd331c9e59034073a9fd68b5d6132cd281d",
    "model_prefix": ISSUE_ROOT / "recovery-candidates/runs/"
    "20260805T-polyphase-x-contiguous-full16-v36/rust",
    "source_log_sha256": "e1ddd52083e0e09b70f87c041a3f25f3ed5d00472db0b9fedf4428bc3d96395f",
}
FOUR_SPW = {
    "id": "four-spw",
    "spw_selection": "2,7,12,17",
    "spw_ids": [2, 7, 12, 17],
    "expected_samples": 6_416_526,
    "expected_segments": 1,
    "cf_cache": ISSUE_ROOT / "cf-cache/6.7.5.9/"
    "db96e297401b0f5c90f1494844fd9a1d49ad5023be44987ce7076afac513d856",
    "model_prefix": ISSUE_ROOT
    / "recovery-candidates/runs/20260805T-polyphase-x-contiguous-v34/rust",
    "source_log_sha256": "622fb9dee7bc3fdb819116df40651e1e18953d85e8a8c7547f6990734f036d11",
}
VARIANTS = (FULL16, FOUR_SPW)
TEST_NAME = "vlass_replay_fixture::tests::full16_exact_replay_benchmark"
AOT_COMPILE_TEST_NAME = "vlass_replay_fixture::tests::compile_aot_grouped_tile_sidecar"
GIB = 1024**3
FULL16_PROMOTION_MAX_SECONDS = 63.148921725
FOUR_SPW_PROMOTION_MAX_SECONDS = 8.256052394
AOT_COMPILE_ADMISSION_LIMIT_BYTES = 32 * GIB
AOT_HASHMAP_MINIMUM_RESERVE_BYTES = 64 * 1024**2
AOT_HASHMAP_RESERVE_MULTIPLIER = 4


class ContractError(RuntimeError):
    """The frozen replay benchmark contract was not satisfied."""


def sha256_file(path: pathlib.Path, *, block_bytes: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(block_bytes):
            digest.update(block)
    return digest.hexdigest()


def sha256_section(
    path: pathlib.Path,
    *,
    offset: int,
    byte_len: int,
    block_bytes: int = 8 * 1024 * 1024,
) -> str:
    digest = hashlib.sha256()
    remaining = byte_len
    with path.open("rb") as stream:
        stream.seek(offset)
        while remaining:
            block = stream.read(min(block_bytes, remaining))
            if not block:
                raise ContractError(
                    f"fixture payload ended before section [{offset}, {offset + byte_len})"
                )
            digest.update(block)
            remaining -= len(block)
    return digest.hexdigest()


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


def new_run_root(role: str) -> pathlib.Path:
    RUNS_ROOT.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    path = RUNS_ROOT / f"{stamp}-{role}-{uuid.uuid4().hex[:12]}"
    path.mkdir(parents=False, exist_ok=False)
    return path


def minimal_environment() -> dict[str, str]:
    return {
        "PATH": "/opt/homebrew/bin:/usr/bin:/bin",
        "HOME": str(pathlib.Path.home()),
        "CASA_RS_MEASURESPATH": str(MEASURES_DIR),
        "CASA_RS_FFTW_LIBRARY_DIR": str(FFTW_DIR),
        "DYLD_LIBRARY_PATH": str(FFTW_DIR),
        "CASA_RS_FFTW_THREADS": "8",
        "CASA_RS_STANDARD_MFS_PROFILE_DETAIL": "1",
    }


def validate_common_inputs() -> None:
    required = [MS, MASK, MEASURES_DIR, FFTW_DIR]
    for variant in VARIANTS:
        required.extend(
            [
                variant["cf_cache"],
                pathlib.Path(f"{variant['model_prefix']}.model.tt0"),
                pathlib.Path(f"{variant['model_prefix']}.model.tt1"),
            ]
        )
    missing = [str(path) for path in required if not pathlib.Path(path).exists()]
    if missing:
        raise ContractError(f"missing frozen input: {missing[0]}")


def build_capture_binary(run_root: pathlib.Path) -> dict[str, Any]:
    command = [
        "cargo",
        "build",
        "--locked",
        "--release",
        "-p",
        "casars-imager",
        "--bin",
        "casars-imager",
    ]
    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(TARGET_DIR)
    environment["CARGO_INCREMENTAL"] = "0"
    started = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )
    log = run_root / "release-build.log"
    log.write_text(completed.stdout + completed.stderr, encoding="utf-8")
    if completed.returncode != 0:
        raise ContractError(f"release build failed; see {log}")
    binary = TARGET_DIR / "release/casars-imager"
    if not binary.is_file():
        raise ContractError(f"release build did not create {binary}")
    return {
        "profile": "release",
        "command": command,
        "seconds": time.monotonic() - started,
        "binary": str(binary),
        "binary_sha256": sha256_file(binary),
        "log": str(log),
        "log_sha256": sha256_file(log),
    }


def build_benchmark_binary(run_root: pathlib.Path) -> dict[str, Any]:
    command = [
        "cargo",
        "test",
        "--locked",
        "--release",
        "-p",
        "casa-imaging",
        "--lib",
        "--no-run",
        "--message-format=json",
    ]
    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(TARGET_DIR)
    environment["CARGO_INCREMENTAL"] = "0"
    started = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )
    log = run_root / "release-build.log"
    log.write_text(completed.stdout + completed.stderr, encoding="utf-8")
    if completed.returncode != 0:
        raise ContractError(f"release test build failed; see {log}")
    executable: pathlib.Path | None = None
    for line in completed.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        target = message.get("target", {})
        profile = message.get("profile", {})
        if (
            message.get("reason") == "compiler-artifact"
            and target.get("name") == "casa_imaging"
            and profile.get("test") is True
            and message.get("executable")
        ):
            executable = pathlib.Path(message["executable"])
    if executable is None or not executable.is_file():
        raise ContractError(
            "release test build did not report the casa-imaging executable"
        )
    return {
        "profile": "release",
        "command": command,
        "seconds": time.monotonic() - started,
        "binary": str(executable),
        "binary_sha256": sha256_file(executable),
        "log": str(log),
        "log_sha256": sha256_file(log),
    }


def current_build_binary_sha256(build: dict[str, Any]) -> str:
    binary = pathlib.Path(str(build.get("binary", "")))
    expected = build.get("binary_sha256")
    if not binary.is_file() or not _valid_sha256(expected):
        raise ContractError("release test executable identity is invalid")
    actual = sha256_file(binary)
    if actual != expected:
        raise ContractError("release test executable changed after build")
    return actual


def capture_command(
    binary: pathlib.Path, variant: dict[str, Any], output: pathlib.Path
) -> list[str]:
    return [
        str(binary),
        "--ms",
        str(MS),
        "--imagename",
        str(output),
        "--imsize",
        "4096",
        "--cell-arcsec",
        "0.6",
        "--field",
        FIELD_SELECTION,
        "--phasecenter-field",
        "1525",
        "--spw",
        str(variant["spw_selection"]),
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
        "--standard-mfs-initial-dirty-backend",
        "cpu",
        "--standard-mfs-residual-backend",
        "metal",
        "--imaging-fft-precision",
        "f64",
        "--imaging-fft-backend",
        "fftw",
        "--parallel",
        "--standard-mfs-grid-threads",
        "7",
        "--imaging-memory-target-mb",
        "16384",
        "--imaging-memory-pressure-policy",
        "auto",
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
        "2000",
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
        str(variant["cf_cache"]),
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
        str(MASK),
        "--savemodel",
        "none",
        "--restoringbeam",
        "common",
        "--no-preview-pngs",
    ]


def capture_environment(
    *,
    variant: dict[str, Any],
    run_root: pathlib.Path,
    fixture_prefix: pathlib.Path,
    provenance: pathlib.Path,
) -> dict[str, str]:
    environment = minimal_environment()
    environment.update(
        {
            "CASA_RS_VLASS_EXPERIMENT_RUNNER": "1",
            "CASA_RS_EXPERIMENTAL_MT_MFS_SPARSE_RHS": "1",
            "CASA_RS_EXPERIMENTAL_MT_MFS_CASA_FFT0": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_PREDIVISION_SOURCE_PHASE": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_RAW_FRAME_TAYLOR": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_RESIDUAL_ONLY_REFRESH": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_REPLAY_RETENTION_BYTES": "25769803776",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_IMAGE_RESPONSE_CACHE": "1",
            "CASA_RS_EXPERIMENTAL_SPARSE_AWPROJECT_MODEL_PREP": "1",
            "CASA_RS_EXPERIMENTAL_SPARSE_MASK_PEAK_SEARCH": "1",
            "CASA_RS_EXPERIMENTAL_CACHE_REFRESHED_NSIGMA": "1",
            "CASA_RS_EXPERIMENTAL_RADIX_MADFM": "1",
            "CASA_RS_EXPERIMENTAL_PARALLEL_MODEL_TERM_FFT": "1",
            "CASA_RS_EXPERIMENTAL_PARALLEL_RESIDUAL_TERM_FFT": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_RESIDUAL_LIVE_CFS_ONLY": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_FROZEN_MODEL_PREFIX": str(
                variant["model_prefix"]
            ),
            "CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_GLOBAL_TILE_REPLAY": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_GPU_RESIDUAL_REPLAY": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_RESIDENT_PROGRAM_COMPACTION": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_HYBRID_CLEAN": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_TILE_SIDE": "16",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_GLOBAL_REPLAY_SEGMENT_BYTES": "8589934592",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_GLOBAL_REPLAY_SPILL_DIR": str(run_root),
            "CASA_RS_EXPERIMENTAL_AWPROJECT_PRIME_REPLAY_INITIAL_DIRTY": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_RAW_CF_GLOBAL_REPLAY": "1",
            "CASA_RS_AWPROJECT_TAPLESS_PHASE_EXPERIMENT": "1",
            "CASA_RS_AWPROJECT_TAP_BUDGET_MB_EXPERIMENT": "512",
            "CASA_RS_AWPROJECT_DYNAMIC_SPARSE_TILE_TASKS_EXPERIMENT": "1",
            "CASA_RS_AWPROJECT_SPARSE_TILES_EXPERIMENT": "1",
            "CASA_RS_AWPROJECT_SPATIAL_TILE_SIDE_EXPERIMENT": "192",
            "CASA_RS_AWPROJECT_NEON_2X2_EXPERIMENT": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_REPLAY_FIXTURE_CAPTURE_PREFIX": str(
                fixture_prefix
            ),
            "CASA_RS_EXPERIMENTAL_AWPROJECT_REPLAY_FIXTURE_PROVENANCE": str(provenance),
        }
    )
    return environment


def fixture_prefix(variant: dict[str, Any]) -> pathlib.Path:
    return FIXTURE_ROOT / str(variant["id"])


def aot_grouped_sidecar_prefix(variant: dict[str, Any]) -> pathlib.Path:
    return FIXTURE_ROOT / f"{variant['id']}.{AOT_GROUPED_SIDECAR_SUFFIX}"


def aot_grouped_compiler_key_is_valid(
    key: dict[str, Any], compiler_binary_sha256: str
) -> bool:
    return (
        _valid_sha256(compiler_binary_sha256)
        and key.get("compiler_binary_sha256") == compiler_binary_sha256
    )


def fixture_provenance(
    variant: dict[str, Any], build: dict[str, Any]
) -> dict[str, Any]:
    model_identities = [
        tree_identity(
            pathlib.Path(f"{variant['model_prefix']}.model.tt{term}"),
            excluded_names={"table.lock"},
        )
        for term in range(2)
    ]
    return {
        "schema": "casa-rs-vlass-replay-fixture-provenance-v1",
        "variant": variant["id"],
        "field_selection": FIELD_SELECTION,
        "field_ids": FIELD_IDS,
        "spw_selection": variant["spw_selection"],
        "spw_ids": variant["spw_ids"],
        "use_pointing": True,
        "imsize": 4096,
        "channel_start": 0,
        "channel_count": 64,
        "nterms": 2,
        "wprojplanes": 32,
        "aterm": True,
        "wbawp": True,
        "conjbeams": True,
        "exact_source_order": True,
        "raw_cf_replay": True,
        "expected_samples": variant["expected_samples"],
        "expected_segments": variant["expected_segments"],
        "source_model_prefix": str(variant["model_prefix"]),
        "source_model_tree_sha256": [
            identity["tree_sha256"] for identity in model_identities
        ],
        "source_terminal_log_sha256": variant["source_log_sha256"],
        "capture_binary_sha256": build["binary_sha256"],
    }


def validate_manifest(manifest: dict[str, Any], variant: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    programs = manifest.get("programs")
    provenance = manifest.get("provenance", {})
    if manifest.get("schema") != "casa-rs-vlass-full16-aw-replay-private-v1":
        errors.append("fixture schema changed")
    if not isinstance(programs, list):
        errors.append("fixture program inventory is missing")
        return errors
    samples = sum(
        int(program.get("prediction_samples", {}).get("len", -1))
        for program in programs
    )
    section_names = (
        "prediction_samples",
        "source_sample_indices",
        "kernels",
        "prediction_phases",
        "tile_samples",
        "tile_phases",
        "term_weights",
        "active_tile_ids",
        "tile_fragment_offsets",
        "fragments",
    )
    program_payload_bytes = 0
    source_sample_index_bytes = 0
    for program in programs:
        section_bytes = 0
        for name in section_names:
            section = program.get(name, {})
            byte_len = section.get("byte_len")
            sha256 = section.get("sha256")
            if not isinstance(byte_len, int) or byte_len < 0:
                errors.append(f"fixture {name} byte count is invalid")
            else:
                section_bytes += byte_len
            if (
                not isinstance(sha256, str)
                or len(sha256) != 64
                or any(character not in "0123456789abcdef" for character in sha256)
            ):
                errors.append(f"fixture {name} SHA-256 is invalid")
        if section_bytes != program.get("payload_bytes"):
            errors.append("fixture program payload-byte accounting changed")
        program_payload_bytes += section_bytes
        prediction_samples = program.get("prediction_samples", {}).get("len")
        source_indices = program.get("source_sample_indices", {})
        source_index_count = source_indices.get("len")
        source_index_byte_len = source_indices.get("byte_len")
        source_index_sha256 = source_indices.get("sha256")
        if (
            not isinstance(prediction_samples, int)
            or prediction_samples <= 0
            or not isinstance(source_index_count, int)
            or source_index_count < 0
        ):
            errors.append("fixture source-sample index cardinality is invalid")
        elif source_index_count not in (0, prediction_samples):
            errors.append("fixture source-sample index cardinality changed")
        if (
            isinstance(source_index_count, int)
            and source_index_count >= 0
            and source_index_byte_len != source_index_count * 4
        ):
            errors.append("fixture source-sample index byte count changed")
        if source_index_count == 0:
            if source_index_sha256 != hashlib.sha256(b"").hexdigest():
                errors.append("fixture absent source-sample index hash changed")
        elif (
            isinstance(source_index_count, int)
            and source_index_count > 0
            and source_index_sha256 == hashlib.sha256(b"").hexdigest()
        ):
            errors.append("fixture retained source-sample index hash changed")
        if isinstance(source_index_byte_len, int) and source_index_byte_len >= 0:
            source_sample_index_bytes += source_index_byte_len
    if len(programs) != variant["expected_segments"]:
        errors.append("fixture segment count changed")
    if samples != variant["expected_samples"]:
        errors.append("fixture accepted-sample count changed")
    if provenance.get("field_ids") != FIELD_IDS:
        errors.append("fixture field topology changed")
    if provenance.get("spw_ids") != variant["spw_ids"]:
        errors.append("fixture SPW topology changed")
    if provenance.get("use_pointing") is not True:
        errors.append("fixture POINTING semantics changed")
    payload_bytes = manifest.get("payload", {}).get("bytes")
    ledger_bytes = manifest.get("byte_ledger", {}).get("payload_bytes")
    ledger_source_sample_index_bytes = (
        manifest.get("byte_ledger", {})
        .get("by_section", {})
        .get("source_sample_indices")
    )
    if not isinstance(payload_bytes, int) or payload_bytes <= 0:
        errors.append("fixture file-byte accounting is missing")
    if not isinstance(ledger_bytes, int) or ledger_bytes <= 0:
        errors.append("fixture section-byte accounting is missing")
    elif ledger_bytes != program_payload_bytes:
        errors.append("fixture aggregate payload-byte accounting changed")
    if ledger_source_sample_index_bytes != source_sample_index_bytes:
        errors.append("fixture aggregate source-sample index bytes changed")
    for collection in ("model_grids", "baseline_residual_grids"):
        sections = manifest.get(collection)
        if not isinstance(sections, list) or len(sections) != 2:
            errors.append(f"fixture {collection} inventory changed")
            continue
        for section in sections:
            offset = section.get("offset")
            byte_len = section.get("byte_len")
            sha256 = section.get("sha256")
            if (
                not isinstance(offset, int)
                or offset < 0
                or not isinstance(byte_len, int)
                or byte_len <= 0
                or not isinstance(payload_bytes, int)
                or offset + byte_len > payload_bytes
            ):
                errors.append(f"fixture {collection} section bounds changed")
            if (
                not isinstance(sha256, str)
                or len(sha256) != 64
                or sha256 == "0" * 64
                or any(character not in "0123456789abcdef" for character in sha256)
            ):
                errors.append(f"fixture {collection} section SHA-256 is invalid")
    return errors


def _valid_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def sealed_sha256(
    path: pathlib.Path,
    expected: str | None = None,
    *,
    update_seal: bool = True,
) -> tuple[str, str]:
    """Hash an immutable artifact once and bind reuse to its filesystem identity."""
    stat = path.stat()
    seal_path = pathlib.Path(f"{path}.sha256-seal.json")
    identity = {
        "path": str(path),
        "bytes": stat.st_size,
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "mtime_ns": stat.st_mtime_ns,
    }
    if seal_path.is_file():
        try:
            seal = json.loads(seal_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            seal = {}
        digest = seal.get("sha256")
        if (
            seal.get("schema") == "casa-rs-immutable-artifact-sha256-seal-v1"
            and seal.get("identity") == identity
            and _valid_sha256(digest)
            and (expected is None or digest == expected)
        ):
            return digest, "reused"
    digest = sha256_file(path)
    if expected is not None and digest != expected:
        raise ContractError(f"immutable artifact SHA-256 changed: {path}")
    if update_seal:
        atomic_write_json(
            seal_path,
            {
                "schema": "casa-rs-immutable-artifact-sha256-seal-v1",
                "recorded_at": utc_now(),
                "identity": identity,
                "sha256": digest,
            },
        )
    return digest, "hashed"


def validate_aot_grouped_sidecar(
    variant: dict[str, Any],
    compiler_binary_sha256: str,
    *,
    update_hash_seals: bool = True,
    defer_sidecar_payload_hash: bool = False,
) -> tuple[list[str], dict[str, Any]]:
    errors: list[str] = []
    raw_prefix = fixture_prefix(variant)
    raw_manifest_path = pathlib.Path(f"{raw_prefix}.json")
    raw_manifest = json.loads(raw_manifest_path.read_text(encoding="utf-8"))
    raw_payload_path = pathlib.Path(
        str(raw_manifest.get("payload", {}).get("path", ""))
    )
    prefix = aot_grouped_sidecar_prefix(variant)
    manifest_path = pathlib.Path(f"{prefix}.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload_path = pathlib.Path(str(manifest.get("payload", {}).get("path", "")))
    if manifest.get("schema") != AOT_GROUPED_SIDECAR_SCHEMA:
        errors.append("AOT grouped-tile sidecar schema changed")
    key = manifest.get("key", {})
    raw_manifest_sha256 = sha256_file(raw_manifest_path)
    if key.get("raw_manifest_sha256") != raw_manifest_sha256:
        errors.append("AOT grouped-tile raw manifest key changed")
    raw_payload_sha256 = key.get("raw_payload_sha256")
    if not _valid_sha256(raw_payload_sha256):
        errors.append("AOT grouped-tile raw payload SHA-256 key is invalid")
        raw_payload_sha256 = None
    if (
        not raw_payload_path.is_file()
        or key.get("raw_payload_bytes") != raw_payload_path.stat().st_size
    ):
        errors.append("AOT grouped-tile raw payload identity changed")
    if key.get("omitted_energy_fraction_bits") != AOT_GROUPED_THRESHOLD_BITS:
        errors.append("AOT grouped-tile threshold key changed")
    if key.get("private_layout") != raw_manifest.get("private_layout"):
        errors.append("AOT grouped-tile private-layout key changed")
    aot_private_layout = key.get("aot_private_layout")
    if not isinstance(aot_private_layout, dict) or any(
        not isinstance(aot_private_layout.get(name), int)
        or aot_private_layout[name] <= 0
        for name in (
            "grouped_plan_bytes",
            "grouped_plan_alignment",
            "sample_role_group_bytes",
            "sample_role_group_alignment",
        )
    ):
        errors.append("AOT grouped-tile private AOT layout changed")
    if key.get("compiler_contract") != AOT_GROUPED_COMPILER_CONTRACT:
        errors.append("AOT grouped-tile compiler contract changed")
    if not aot_grouped_compiler_key_is_valid(key, compiler_binary_sha256):
        errors.append("AOT grouped-tile compiler executable changed")
    if manifest.get("grid_shape") != raw_manifest.get("grid_shape"):
        errors.append("AOT grouped-tile grid geometry changed")

    payload = manifest.get("payload", {})
    sidecar_payload_sha256 = payload.get("sha256")
    if not _valid_sha256(sidecar_payload_sha256):
        errors.append("AOT grouped-tile sidecar payload SHA-256 is invalid")
        sidecar_payload_sha256 = None
    if (
        not payload_path.is_file()
        or payload.get("bytes") != payload_path.stat().st_size
    ):
        errors.append("AOT grouped-tile sidecar payload identity changed")
    programs = manifest.get("programs")
    if not isinstance(programs, list) or len(programs) != variant["expected_segments"]:
        errors.append("AOT grouped-tile segment inventory changed")
        programs = []
    raw_programs = raw_manifest.get("programs")
    if not isinstance(raw_programs, list) or len(raw_programs) != len(programs):
        errors.append("AOT grouped-tile raw segment inventory changed")
        raw_programs = []
    section_names = (
        "prediction_samples",
        "grouped_plans",
        "sample_role_groups",
        "active_tile_ids",
        "tile_fragment_offsets",
        "fragments",
    )
    specialized_section_bytes = 0
    sample_count = 0
    raw_prediction_sample_bytes = 0
    cropped_prediction_sample_bytes = 0
    payload_bytes = payload.get("bytes")
    for program_index, program in enumerate(programs):
        receipt = program.get("receipt", {})
        program_section_bytes = 0
        for name in section_names:
            section = program.get(name, {})
            offset = section.get("offset")
            byte_len = section.get("byte_len")
            digest = section.get("sha256")
            if (
                not isinstance(offset, int)
                or offset < 0
                or not isinstance(byte_len, int)
                or byte_len < 0
                or not isinstance(payload_bytes, int)
                or offset + byte_len > payload_bytes
            ):
                errors.append(f"AOT grouped-tile {name} section bounds changed")
            else:
                program_section_bytes += byte_len
            if not _valid_sha256(digest) or digest == "0" * 64:
                errors.append(f"AOT grouped-tile {name} section SHA-256 is invalid")
        if program.get("payload_bytes") != program_section_bytes:
            errors.append("AOT grouped-tile program byte ledger changed")
        specialized_section_bytes += program_section_bytes
        samples = receipt.get("sample_count")
        groups = receipt.get("group_count")
        if not isinstance(samples, int) or samples <= 0:
            errors.append("AOT grouped-tile sample count changed")
            samples = 0
        if not isinstance(groups, int) or groups <= 0:
            errors.append("AOT grouped-tile group count changed")
        sample_count += samples
        if program.get("prediction_samples", {}).get("len") != samples:
            errors.append("AOT grouped-tile prediction cardinality changed")
        prediction_bytes = program.get("prediction_samples", {}).get("byte_len")
        raw_prediction_bytes = (
            raw_programs[program_index].get("prediction_samples", {}).get("byte_len")
            if program_index < len(raw_programs)
            else None
        )
        if not isinstance(prediction_bytes, int) or prediction_bytes <= 0:
            errors.append("AOT grouped-tile cropped prediction byte ledger changed")
            prediction_bytes = 0
        if not isinstance(raw_prediction_bytes, int) or raw_prediction_bytes <= 0:
            errors.append("AOT grouped-tile raw prediction byte ledger changed")
            raw_prediction_bytes = 0
        raw_prediction_sample_bytes += raw_prediction_bytes
        cropped_prediction_sample_bytes += prediction_bytes
        if program.get("sample_role_groups", {}).get("len") != samples:
            errors.append("AOT grouped-tile sample-role cardinality changed")
        if program.get("grouped_plans", {}).get("len") != groups:
            errors.append("AOT grouped-tile group cardinality changed")
        if receipt.get("omitted_energy_fraction_bits") != AOT_GROUPED_THRESHOLD_BITS:
            errors.append("AOT grouped-tile receipt threshold changed")
        for name in (
            "crop_decisions_sha256",
            "grouped_plans_sha256",
            "sample_role_groups_sha256",
            "grouped_route_sha256",
            "legacy_grouped_plans_sha256",
            "legacy_grouped_route_sha256",
        ):
            if not _valid_sha256(receipt.get(name)) or receipt.get(name) == "0" * 64:
                errors.append(f"AOT grouped-tile receipt {name} is invalid")
        if receipt.get("grouped_plans_sha256") != receipt.get(
            "legacy_grouped_plans_sha256"
        ):
            errors.append("AOT grouped-tile plans differ from the incumbent compiler")
        if receipt.get("grouped_route_sha256") != receipt.get(
            "legacy_grouped_route_sha256"
        ):
            errors.append("AOT grouped-tile route differs from the incumbent compiler")
        if receipt.get("grouped_plans_sha256") != program.get("grouped_plans", {}).get(
            "sha256"
        ):
            errors.append("AOT grouped-tile plan section hash changed")
        if receipt.get("sample_role_groups_sha256") != program.get(
            "sample_role_groups", {}
        ).get("sha256"):
            errors.append("AOT grouped-tile sample-role section hash changed")
        if not effective_support_segment_receipt_is_valid(
            program.get("effective_support")
        ):
            errors.append("AOT grouped-tile effective-support receipt changed")
        ledger = receipt.get("ledger", {})
        if (
            ledger.get("raw_prediction_sample_bytes_replaced") != raw_prediction_bytes
            or ledger.get("cropped_prediction_sample_bytes") != prediction_bytes
            or raw_prediction_bytes != prediction_bytes
        ):
            errors.append("AOT grouped-tile prediction replacement equation changed")
        support = program.get("effective_support", {})
        if (
            ledger.get("effective_support_hashmap_estimated_bytes")
            != support.get("index_estimated_bytes")
            or ledger.get("effective_support_prefix_scratch_bytes")
            != support.get("prefix_scratch_peak_bytes")
            or ledger.get("effective_support_scratch_estimated_bytes")
            != (
                support.get("index_estimated_bytes", -1)
                + support.get("prefix_scratch_peak_bytes", -1)
            )
        ):
            errors.append("AOT grouped-tile support scratch estimate changed")
        if not aot_grouped_segment_receipt_is_valid(receipt):
            errors.append("AOT grouped-tile compile admission ledger changed")
        grouped_route_bytes = sum(
            program.get(name, {}).get("byte_len", 0)
            for name in ("active_tile_ids", "tile_fragment_offsets", "fragments")
        )
        persisted_tile_bytes = sum(
            program.get(name, {}).get("byte_len", 0)
            for name in (
                "grouped_plans",
                "sample_role_groups",
                "active_tile_ids",
                "tile_fragment_offsets",
                "fragments",
            )
        )
        if ledger.get("grouped_route_bytes") != grouped_route_bytes:
            errors.append("AOT grouped-tile route byte ledger changed")
        if ledger.get("persisted_tile_bytes") != persisted_tile_bytes:
            errors.append("AOT grouped-tile persisted byte ledger changed")
        if not isinstance(ledger.get("raw_tile_sample_bytes_released"), int):
            errors.append("AOT grouped-tile released-sample byte ledger is missing")
        if not isinstance(ledger.get("raw_route_bytes_released"), int):
            errors.append("AOT grouped-tile released-route byte ledger is missing")

    if sample_count != variant["expected_samples"]:
        errors.append("AOT grouped-tile aggregate sample count changed")
    lifetime = manifest.get("byte_lifetime_ledger", {})
    if (
        lifetime.get("raw_prediction_sample_bytes_replaced_at_compile")
        != raw_prediction_sample_bytes
        or lifetime.get("cropped_prediction_sample_bytes_persisted")
        != cropped_prediction_sample_bytes
        or raw_prediction_sample_bytes != cropped_prediction_sample_bytes
        or lifetime.get("raw_prediction_sample_bytes_retained_for_replay") != 0
        or lifetime.get("raw_prediction_sample_bytes_read_during_replay") != 0
        or lifetime.get("prediction_replacement_equation")
        != "raw_prediction_sample_bytes_replaced_at_compile == "
        "cropped_prediction_sample_bytes_persisted"
    ):
        errors.append("AOT grouped-tile aggregate prediction replacement changed")
    if lifetime.get("specialized_sidecar_section_bytes") != specialized_section_bytes:
        errors.append("AOT grouped-tile aggregate section-byte ledger changed")
    if lifetime.get("specialized_sidecar_file_bytes") != payload_bytes:
        errors.append("AOT grouped-tile aggregate file-byte ledger changed")
    for name in (
        "runtime_grouping_builds",
        "runtime_sort_builds",
        "runtime_route_builds",
    ):
        if lifetime.get(name) != 0:
            errors.append(f"AOT grouped-tile {name} is not zero")
    expected_references = [
        "source_sample_indices",
        "kernels",
        "prediction_phases",
        "tile_phases",
        "term_weights",
    ]
    if lifetime.get("raw_sections_referenced_not_copied") != expected_references:
        errors.append("AOT grouped-tile raw-section reference contract changed")
    expected_replaced = [
        "prediction_samples",
        "tile_samples",
        "active_tile_ids",
        "tile_fragment_offsets",
        "fragments",
    ]
    if lifetime.get("raw_sections_replaced_not_read") != expected_replaced:
        errors.append("AOT grouped-tile raw-section replacement contract changed")

    hash_receipt: dict[str, Any] = {}
    if not errors and raw_payload_sha256 is not None:
        raw_digest, raw_status = sealed_sha256(
            raw_payload_path,
            expected=raw_payload_sha256,
            update_seal=update_hash_seals,
        )
        hash_receipt["raw_payload"] = {
            "path": str(raw_payload_path),
            "sha256": raw_digest,
            "status": raw_status,
        }
    if not errors and sidecar_payload_sha256 is not None:
        if defer_sidecar_payload_hash:
            sidecar_digest = sidecar_payload_sha256
            sidecar_status = "deferred-to-timed-benchmark"
        else:
            sidecar_digest, sidecar_status = sealed_sha256(
                payload_path,
                expected=sidecar_payload_sha256,
                update_seal=update_hash_seals,
            )
        hash_receipt["sidecar_payload"] = {
            "path": str(payload_path),
            "sha256": sidecar_digest,
            "status": sidecar_status,
        }
    return errors, {
        "variant": variant["id"],
        "compiler_binary_sha256": compiler_binary_sha256,
        "manifest": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "payload": str(payload_path),
        "payload_bytes": payload_bytes,
        "hashes": hash_receipt,
    }


def ensure_aot_grouped_sidecars(
    build: dict[str, Any], run_root: pathlib.Path
) -> list[dict[str, Any]]:
    compiler_binary_sha256 = current_build_binary_sha256(build)
    receipts: list[dict[str, Any]] = []
    for variant in (FOUR_SPW, FULL16):
        raw_prefix = fixture_prefix(variant)
        raw_manifest = json.loads(
            pathlib.Path(f"{raw_prefix}.json").read_text(encoding="utf-8")
        )
        raw_payload = pathlib.Path(str(raw_manifest["payload"]["path"]))
        prefix = aot_grouped_sidecar_prefix(variant)
        manifest_path = pathlib.Path(f"{prefix}.json")
        payload_path = pathlib.Path(f"{prefix}.payload")
        partial_path = pathlib.Path(f"{prefix}.payload.partial")
        if manifest_path.is_file() and payload_path.is_file():
            status = "reused"
            compile_receipt = None
            log_path = None
        elif manifest_path.exists() or payload_path.exists() or partial_path.exists():
            raise ContractError(
                f"incomplete AOT grouped-tile sidecar exists for {variant['id']}"
            )
        else:
            raw_payload_sha256, _ = sealed_sha256(raw_payload)
            environment = minimal_environment()
            environment.update(
                {
                    AOT_GROUPED_RAW_PAYLOAD_SHA256_ENV: raw_payload_sha256,
                    AOT_GROUPED_COMPILE_RAW_PREFIX_ENV: str(raw_prefix),
                    AOT_GROUPED_COMPILE_SIDECAR_PREFIX_ENV: str(prefix),
                    AOT_GROUPED_COMPILER_BINARY_SHA256_ENV: compiler_binary_sha256,
                }
            )
            command = [
                build["binary"],
                "--exact",
                AOT_COMPILE_TEST_NAME,
                "--ignored",
                "--nocapture",
            ]
            log_path = run_root / f"{variant['id']}-aot-sidecar-compile.log"
            started = time.monotonic()
            try:
                with log_path.open("w", encoding="utf-8") as log:
                    completed = subprocess.run(
                        command,
                        cwd=run_root,
                        env=environment,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        text=True,
                        timeout=1800,
                    )
            except subprocess.TimeoutExpired as error:
                raise ContractError(
                    f"{variant['id']} AOT grouped-tile compilation timed out; "
                    f"see {log_path}"
                ) from error
            if completed.returncode != 0:
                raise ContractError(
                    f"{variant['id']} AOT grouped-tile compilation failed; "
                    f"see {log_path}"
                )
            marker = "VLASS_AOT_GROUPED_SIDECAR_JSON "
            candidates = []
            for line in log_path.read_text(encoding="utf-8").splitlines():
                if marker in line:
                    candidates.append(json.loads(line.split(marker, 1)[1]))
            if len(candidates) != 1:
                raise ContractError(
                    f"{variant['id']} AOT compiler emitted {len(candidates)} receipts"
                )
            if candidates[0].get("compiler_binary_sha256") != compiler_binary_sha256:
                raise ContractError(
                    f"{variant['id']} AOT compiler executable receipt changed"
                )
            compile_receipt = {
                "command": command,
                "seconds": time.monotonic() - started,
                "log": str(log_path),
                "log_sha256": sha256_file(log_path),
                "result": candidates[0],
            }
            status = "compiled"
        errors, validation = validate_aot_grouped_sidecar(
            variant,
            compiler_binary_sha256,
            defer_sidecar_payload_hash=True,
        )
        if errors:
            raise ContractError(f"{variant['id']} AOT sidecar: {errors[0]}")
        receipts.append(
            {
                "variant": variant["id"],
                "status": status,
                "compile": compile_receipt,
                "validation": validation,
            }
        )
    return receipts


def seal_fixtures() -> int:
    run_root = new_run_root("seal")
    receipts: list[dict[str, Any]] = []
    for variant in VARIANTS:
        manifest_path = pathlib.Path(f"{fixture_prefix(variant)}.json")
        if not manifest_path.is_file():
            raise ContractError(f"missing frozen {variant['id']} replay manifest")
        original = manifest_path.read_bytes()
        manifest = json.loads(original)
        payload_path = pathlib.Path(str(manifest.get("payload", {}).get("path", "")))
        if not payload_path.is_file():
            raise ContractError(f"missing frozen {variant['id']} replay payload")
        backup = run_root / f"{variant['id']}-manifest-before.json"
        backup.write_bytes(original)
        section_receipts: list[dict[str, Any]] = []
        changed = False
        for collection in ("model_grids", "baseline_residual_grids"):
            sections = manifest.get(collection)
            if not isinstance(sections, list):
                raise ContractError(f"{variant['id']} fixture is missing {collection}")
            for ordinal, section in enumerate(sections):
                offset = section.get("offset")
                byte_len = section.get("byte_len")
                if (
                    not isinstance(offset, int)
                    or offset < 0
                    or not isinstance(byte_len, int)
                    or byte_len <= 0
                ):
                    raise ContractError(
                        f"{variant['id']} fixture has invalid {collection} bounds"
                    )
                actual = sha256_section(payload_path, offset=offset, byte_len=byte_len)
                previous = section.get("sha256")
                if previous not in (actual, "0" * 64):
                    raise ContractError(
                        f"{variant['id']} fixture {collection}[{ordinal}] hash differs"
                    )
                if previous != actual:
                    section["sha256"] = actual
                    changed = True
                section_receipts.append(
                    {
                        "collection": collection,
                        "ordinal": ordinal,
                        "offset": offset,
                        "byte_len": byte_len,
                        "sha256": actual,
                    }
                )
        errors = validate_manifest(manifest, variant)
        if errors:
            raise ContractError(f"{variant['id']} fixture: {errors[0]}")
        if changed:
            atomic_write_json(manifest_path, manifest)
        receipts.append(
            {
                "variant": variant["id"],
                "status": "sealed" if changed else "already-sealed",
                "manifest": str(manifest_path),
                "manifest_sha256_before": hashlib.sha256(original).hexdigest(),
                "manifest_sha256_after": sha256_file(manifest_path),
                "manifest_backup": str(backup),
                "manifest_backup_sha256": sha256_file(backup),
                "payload": str(payload_path),
                "payload_bytes": payload_path.stat().st_size,
                "sections": section_receipts,
            }
        )
    receipt = {
        "schema": "casa-rs-vlass-replay-fixture-seal-v1",
        "recorded_at": utc_now(),
        "fixtures": receipts,
    }
    receipt_path = run_root / "receipt.json"
    atomic_write_json(receipt_path, receipt)
    print(
        json.dumps(
            {
                "receipt": str(receipt_path),
                "receipt_sha256": sha256_file(receipt_path),
            },
            sort_keys=True,
        )
    )
    return 0


def capture() -> int:
    validate_common_inputs()
    FIXTURE_ROOT.mkdir(parents=True, exist_ok=True)
    run_root = new_run_root("capture")
    build = build_capture_binary(run_root)
    receipts: list[dict[str, Any]] = []
    for variant in VARIANTS:
        prefix = fixture_prefix(variant)
        manifest_path = pathlib.Path(f"{prefix}.json")
        payload_path = pathlib.Path(f"{prefix}.payload")
        if manifest_path.exists() and payload_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            errors = validate_manifest(manifest, variant)
            if errors:
                raise ContractError(f"existing {variant['id']} fixture: {errors[0]}")
            receipts.append(
                {
                    "variant": variant["id"],
                    "status": "reused",
                    "manifest": str(manifest_path),
                    "manifest_sha256": sha256_file(manifest_path),
                    "payload": str(payload_path),
                    "payload_bytes": payload_path.stat().st_size,
                }
            )
            continue
        if manifest_path.exists() or payload_path.exists():
            raise ContractError(f"incomplete fixture exists for {variant['id']}")
        variant_root = run_root / str(variant["id"])
        variant_root.mkdir()
        provenance_path = variant_root / "provenance.json"
        atomic_write_json(provenance_path, fixture_provenance(variant, build))
        command = capture_command(
            pathlib.Path(build["binary"]), variant, variant_root / "rust"
        )
        environment = capture_environment(
            variant=variant,
            run_root=variant_root,
            fixture_prefix=prefix,
            provenance=provenance_path,
        )
        log_path = variant_root / "capture.log"
        sampler = DarwinHostTelemetrySampler(interval_seconds=5.0)
        sampler.start()
        started = time.monotonic()
        timed_out = False
        try:
            with log_path.open("w", encoding="utf-8") as log:
                process = subprocess.Popen(
                    command,
                    cwd=variant_root,
                    env=environment,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                sampler.attach_targets(
                    process_pid=process.pid, spill_volume_path="/Volumes/GLENDENNING"
                )
                try:
                    exit_code = process.wait(
                        timeout=14_400 if variant["id"] == "full16" else 7_200
                    )
                except subprocess.TimeoutExpired:
                    timed_out = True
                    process.terminate()
                    try:
                        exit_code = process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        exit_code = process.wait()
        finally:
            telemetry = sampler.stop()
        wall_seconds = time.monotonic() - started
        log_text = log_path.read_text(encoding="utf-8")
        marker = "awproject_vlass_replay_fixture status=frozen"
        terminal = (
            "VLASS AWProject replay fixture capture completed "
            "before remaining imaging stages"
        )
        if (
            timed_out
            or exit_code == 0
            or marker not in log_text
            or terminal not in log_text
        ):
            raise ContractError(
                f"{variant['id']} capture did not reach its fail-closed boundary; "
                f"see {log_path}"
            )
        if not manifest_path.is_file() or not payload_path.is_file():
            raise ContractError(f"{variant['id']} capture did not publish both files")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        errors = validate_manifest(manifest, variant)
        if errors:
            raise ContractError(f"{variant['id']} fixture: {errors[0]}")
        receipts.append(
            {
                "variant": variant["id"],
                "status": "captured",
                "wall_seconds": wall_seconds,
                "expected_nonzero_exit": exit_code,
                "command": command,
                "environment": environment,
                "log": str(log_path),
                "log_sha256": sha256_file(log_path),
                "provenance": str(provenance_path),
                "provenance_sha256": sha256_file(provenance_path),
                "manifest": str(manifest_path),
                "manifest_sha256": sha256_file(manifest_path),
                "payload": str(payload_path),
                "payload_bytes": payload_path.stat().st_size,
                "telemetry": telemetry,
            }
        )
    receipt = {
        "schema": "casa-rs-vlass-replay-fixture-capture-receipt-v1",
        "recorded_at": utc_now(),
        "build": build,
        "fixtures": receipts,
    }
    receipt_path = run_root / "receipt.json"
    atomic_write_json(receipt_path, receipt)
    print(json.dumps({"receipt": str(receipt_path)}, sort_keys=True))
    return 0


def benchmark_environment(compiler_binary_sha256: str) -> dict[str, str]:
    if not _valid_sha256(compiler_binary_sha256):
        raise ContractError("benchmark compiler executable SHA-256 is invalid")
    environment = minimal_environment()
    environment.update(
        {
            "CASA_RS_EXPERIMENTAL_AWPROJECT_HYBRID_CLEAN": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_PREDIVISION_SOURCE_PHASE": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_RAW_FRAME_TAYLOR": "1",
            "CASA_RS_EXPERIMENTAL_AWPROJECT_REPLAY_FIXTURE_BENCHMARK_PREFIX": str(
                fixture_prefix(FULL16)
            ),
            "CASA_RS_EXPERIMENTAL_AWPROJECT_REPLAY_FIXTURE_FOUR_SPW_PREFIX": str(
                fixture_prefix(FOUR_SPW)
            ),
            AOT_GROUPED_SIDECAR_FULL16_PREFIX_ENV: str(
                aot_grouped_sidecar_prefix(FULL16)
            ),
            AOT_GROUPED_SIDECAR_FOUR_SPW_PREFIX_ENV: str(
                aot_grouped_sidecar_prefix(FOUR_SPW)
            ),
            AOT_GROUPED_COMPILER_BINARY_SHA256_ENV: compiler_binary_sha256,
        }
    )
    return environment


def parse_benchmark_result(output: str) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    marker = "VLASS_REPLAY_BENCHMARK_JSON "
    for line in output.splitlines():
        if marker not in line:
            continue
        line = line.split(marker, 1)[1].strip()
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if value.get("schema") == "casa-rs-vlass-full16-aw-replay-campaign-v1":
            candidates.append(value)
    if len(candidates) != 1:
        raise ContractError(
            f"expected one replay benchmark JSON result, found {len(candidates)}"
        )
    return candidates[0]


def effective_support_segment_receipt_is_valid(receipt: Any) -> bool:
    if not isinstance(receipt, dict):
        return False

    def integer(key: str) -> int | None:
        value = receipt.get(key)
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    def number(key: str) -> float | None:
        value = receipt.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return None
        value = float(value)
        return value if math.isfinite(value) else None

    threshold = number("omitted_energy_fraction")
    max_omitted = number("max_omitted_energy_fraction")
    unique_stencils = integer("unique_stencils")
    stencil_lookups = integer("stencil_lookups")
    crop_evaluations = integer("crop_evaluations")
    index_peak_entries = integer("index_peak_entries")
    index_estimated_bytes = integer("index_estimated_bytes")
    prefix_scratch_peak_bytes = integer("prefix_scratch_peak_bytes")
    compile_seconds = number("compile_seconds")
    resident_before = integer("resident_kernel_bytes_before")
    resident_after = integer("resident_kernel_bytes_after")
    if (
        threshold is None
        or not 0.0 < threshold <= 1.0e-4
        or max_omitted is None
        or not 0.0 <= max_omitted <= threshold
        or unique_stencils is None
        or unique_stencils <= 0
        or crop_evaluations != unique_stencils
        or index_peak_entries != unique_stencils
        or index_estimated_bytes is None
        or index_estimated_bytes <= 0
        or prefix_scratch_peak_bytes is None
        or prefix_scratch_peak_bytes <= 0
        or compile_seconds is None
        or compile_seconds < 0.0
        or resident_before is None
        or resident_before <= 0
        or resident_after != resident_before
    ):
        return False

    roles: list[dict[str, Any]] = []
    for key in ("prediction", "tile"):
        role = receipt.get(key)
        if not isinstance(role, dict):
            return False
        role_values = [
            role.get(name)
            for name in (
                "plan_count",
                "unique_stencils",
                "original_tap_visits",
                "retained_tap_visits",
                "cropped_plans",
            )
        ]
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in role_values
        ):
            return False
        if (
            role["plan_count"] <= 0
            or role["unique_stencils"] > unique_stencils
            or role["retained_tap_visits"] > role["original_tap_visits"]
            or role["cropped_plans"] > role["plan_count"]
        ):
            return False
        roles.append(role)
    total_plans = sum(role["plan_count"] for role in roles)
    if stencil_lookups != total_plans:
        return False

    fallback_counts = receipt.get("fallback_counts")
    if not isinstance(fallback_counts, dict):
        return False
    if any(
        not isinstance(reason, str)
        or not isinstance(count, int)
        or isinstance(count, bool)
        or count < 0
        for reason, count in fallback_counts.items()
    ):
        return False
    return sum(fallback_counts.values()) <= total_plans


def aot_grouped_segment_receipt_is_valid(receipt: Any) -> bool:
    if not isinstance(receipt, dict):
        return False
    if (
        receipt.get("omitted_energy_fraction_bits") != AOT_GROUPED_THRESHOLD_BITS
        or not isinstance(receipt.get("sample_count"), int)
        or receipt["sample_count"] <= 0
        or not isinstance(receipt.get("group_count"), int)
        or receipt["group_count"] <= 0
    ):
        return False
    hashes = [
        receipt.get(name)
        for name in (
            "crop_decisions_sha256",
            "grouped_plans_sha256",
            "sample_role_groups_sha256",
            "grouped_route_sha256",
            "legacy_grouped_plans_sha256",
            "legacy_grouped_route_sha256",
        )
    ]
    if any(not _valid_sha256(value) or value == "0" * 64 for value in hashes):
        return False
    if (
        receipt["grouped_plans_sha256"] != receipt["legacy_grouped_plans_sha256"]
        or receipt["grouped_route_sha256"] != receipt["legacy_grouped_route_sha256"]
    ):
        return False
    ledger = receipt.get("ledger")
    if not isinstance(ledger, dict):
        return False
    names = (
        "raw_resident_bytes_before_compile",
        "raw_prediction_sample_bytes_replaced",
        "cropped_prediction_sample_bytes",
        "raw_tile_sample_bytes_released",
        "raw_route_bytes_released",
        "grouped_plan_bytes",
        "sample_role_group_bytes",
        "grouped_route_bytes",
        "canonical_group_plan_capacity_bytes",
        "canonical_group_sum_capacity_bytes",
        "canonical_hashmap_estimated_bytes",
        "tile_planner_known_peak_bytes",
        "sample_role_group_capacity_bytes",
        "final_hashmap_estimated_bytes",
        "aot_group_sum_bytes",
        "fixed_scale_bytes",
        "effective_support_hashmap_estimated_bytes",
        "effective_support_prefix_scratch_bytes",
        "effective_support_scratch_estimated_bytes",
        "compile_transient_bytes_peak_estimated",
        "hashmap_uncertainty_reserve_bytes",
        "compile_admission_bytes",
        "compile_admission_limit_bytes",
        "persisted_tile_bytes",
    )
    if any(not isinstance(ledger.get(name), int) or ledger[name] < 0 for name in names):
        return False
    persisted = (
        ledger["grouped_plan_bytes"]
        + ledger["sample_role_group_bytes"]
        + ledger["grouped_route_bytes"]
    )
    hashmap_estimated = (
        ledger["canonical_hashmap_estimated_bytes"]
        + ledger["final_hashmap_estimated_bytes"]
        + ledger["effective_support_hashmap_estimated_bytes"]
    )
    estimated_peak = (
        ledger["raw_resident_bytes_before_compile"]
        + ledger["canonical_group_plan_capacity_bytes"]
        + ledger["canonical_group_sum_capacity_bytes"]
        + ledger["tile_planner_known_peak_bytes"]
        + ledger["sample_role_group_capacity_bytes"]
        + ledger["aot_group_sum_bytes"]
        + ledger["fixed_scale_bytes"]
        + ledger["effective_support_prefix_scratch_bytes"]
        + hashmap_estimated
    )
    reserve = max(
        AOT_HASHMAP_MINIMUM_RESERVE_BYTES,
        hashmap_estimated * AOT_HASHMAP_RESERVE_MULTIPLIER,
    )
    return (
        ledger["raw_resident_bytes_before_compile"] > 0
        and ledger["raw_prediction_sample_bytes_replaced"] > 0
        and ledger["raw_prediction_sample_bytes_replaced"]
        == ledger["cropped_prediction_sample_bytes"]
        and ledger["raw_tile_sample_bytes_released"] > 0
        and ledger["persisted_tile_bytes"] == persisted
        and ledger["canonical_group_plan_capacity_bytes"]
        >= ledger["grouped_plan_bytes"]
        and ledger["sample_role_group_capacity_bytes"]
        >= ledger["sample_role_group_bytes"]
        and ledger["effective_support_scratch_estimated_bytes"]
        == ledger["effective_support_hashmap_estimated_bytes"]
        + ledger["effective_support_prefix_scratch_bytes"]
        and ledger["compile_transient_bytes_peak_estimated"] == estimated_peak
        and ledger["hashmap_uncertainty_reserve_bytes"] == reserve
        and ledger["compile_admission_bytes"] == estimated_peak + reserve
        and ledger["compile_admission_limit_bytes"] == AOT_COMPILE_ADMISSION_LIMIT_BYTES
        and ledger["compile_admission_bytes"] <= ledger["compile_admission_limit_bytes"]
    )


def result_errors(
    result: dict[str, Any],
    telemetry: dict[str, Any],
    *,
    four_spw_baseline_seconds: float | None,
) -> list[str]:
    errors: list[str] = []
    for key, expected_samples, expected_segments, expected_spws in [
        ("full16", 25_030_848, 10, list(range(2, 18))),
        ("four_spw", 6_416_526, 1, [2, 7, 12, 17]),
    ]:
        value = result.get(key, {})
        if value.get("samples") != expected_samples:
            errors.append(f"{key} sample count changed")
        if value.get("segments") != expected_segments:
            errors.append(f"{key} segment count changed")
        if value.get("rejected_samples") != 0:
            errors.append(f"{key} rejected samples")
        seconds = value.get("seconds")
        if (
            not isinstance(seconds, (int, float))
            or isinstance(seconds, bool)
            or not math.isfinite(seconds)
            or seconds <= 0.0
        ):
            errors.append(f"{key} replay timing changed")
        promotion_limit = (
            FULL16_PROMOTION_MAX_SECONDS
            if key == "full16"
            else FOUR_SPW_PROMOTION_MAX_SECONDS
        )
        if (
            isinstance(seconds, (int, float))
            and not isinstance(seconds, bool)
            and math.isfinite(seconds)
            and seconds > promotion_limit
        ):
            errors.append(f"{key} replay exceeded hard promotion limit")
        provenance = value.get("provenance", {})
        if provenance.get("field_ids") != FIELD_IDS:
            errors.append(f"{key} field topology changed")
        if provenance.get("spw_ids") != expected_spws:
            errors.append(f"{key} SPW topology changed")
        if provenance.get("use_pointing") is not True:
            errors.append(f"{key} POINTING semantics changed")
        nrmse = value.get("nrmse")
        if (
            not isinstance(nrmse, list)
            or len(nrmse) != 2
            or any(
                not isinstance(item, (int, float))
                or isinstance(item, bool)
                or not math.isfinite(item)
                or item > 1e-3
                for item in nrmse
            )
        ):
            errors.append(f"{key} NRMSE exceeds 1e-3")
        ledger = value.get("byte_ledger", {})
        required_bytes = [
            "payload_bytes",
            "kernel_payload_bytes",
            "unique_kernel_bytes",
            "duplicated_kernel_bytes",
            "segment_local_non_kernel_bytes",
        ]
        if any(not isinstance(ledger.get(name), int) for name in required_bytes):
            errors.append(f"{key} byte ledger is incomplete")
        raw_reload_bytes = value.get("raw_reload_bytes")
        sidecar_reload_bytes = value.get("sidecar_reload_bytes")
        payload_bytes = value.get("payload_bytes")
        if (
            not isinstance(raw_reload_bytes, int)
            or not isinstance(sidecar_reload_bytes, int)
            or not isinstance(payload_bytes, int)
            or raw_reload_bytes + sidecar_reload_bytes != payload_bytes
            or value.get("reload_bytes") != payload_bytes
        ):
            errors.append(f"{key} replay reload-byte accounting changed")
        verification = value.get("sidecar_payload_verification", {})
        verification_bytes = verification.get("bytes")
        verification_seconds = verification.get("seconds")
        if (
            not isinstance(verification_bytes, int)
            or verification_bytes <= 0
            or not isinstance(verification_seconds, (int, float))
            or isinstance(verification_seconds, bool)
            or not math.isfinite(verification_seconds)
            or verification_seconds < 0.0
            or verification.get("included_in_seconds") is not True
            or value.get("timed_io_bytes")
            != value.get("reload_bytes", -1) + verification_bytes
        ):
            errors.append(f"{key} timed sidecar payload verification changed")
        segments = value.get("segment_receipts")
        if not isinstance(segments, list) or len(segments) != expected_segments:
            errors.append(f"{key} per-segment timing inventory changed")
        support = value.get("effective_support", {})
        if support.get("requested") is not True:
            errors.append(f"{key} effective-support requested state changed")
        if support.get("decision") != "enabled":
            errors.append(f"{key} effective-support decision changed")
        if support.get("reason") is not None:
            errors.append(f"{key} effective-support reason changed")
        if support.get("segment_count") != expected_segments:
            errors.append(f"{key} effective-support segment count changed")
        if support.get("compiled_segment_count") != 0:
            errors.append(f"{key} effective-support compiled segment count changed")
        if value.get("effective_support_telemetry_markers") != 0:
            errors.append(f"{key} effective-support telemetry marker count changed")
        if isinstance(segments, list) and len(segments) == expected_segments:
            receipts_valid = True
            segment_raw_bytes = 0
            segment_sidecar_bytes = 0
            segment_raw_prediction_bytes = 0
            segment_cropped_prediction_bytes = 0
            for segment in segments:
                if not isinstance(segment, dict):
                    receipts_valid = False
                    continue
                receipts_valid &= effective_support_segment_receipt_is_valid(
                    segment.get("effective_support")
                )
                receipts_valid &= aot_grouped_segment_receipt_is_valid(
                    segment.get("aot_grouped_tile")
                )
                receipts_valid &= segment.get("samples") == segment.get(
                    "aot_grouped_tile", {}
                ).get("sample_count")
                segment_aot_ledger = segment.get("aot_grouped_tile", {}).get(
                    "ledger", {}
                )
                raw_prediction_bytes = segment.get(
                    "raw_prediction_sample_bytes_not_read"
                )
                cropped_prediction_bytes = segment.get(
                    "sidecar_cropped_prediction_sample_bytes_read"
                )
                if isinstance(raw_prediction_bytes, int) and isinstance(
                    cropped_prediction_bytes, int
                ):
                    segment_raw_prediction_bytes += raw_prediction_bytes
                    segment_cropped_prediction_bytes += cropped_prediction_bytes
                    receipts_valid &= (
                        raw_prediction_bytes
                        == cropped_prediction_bytes
                        == segment_aot_ledger.get(
                            "raw_prediction_sample_bytes_replaced"
                        )
                        == segment_aot_ledger.get("cropped_prediction_sample_bytes")
                    )
                else:
                    receipts_valid = False
                receipts_valid &= segment.get("raw_replaced_section_bytes_read") == 0
                segment_raw = segment.get("raw_reload_bytes")
                segment_sidecar = segment.get("sidecar_reload_bytes")
                segment_payload = segment.get("payload_bytes")
                if isinstance(segment_raw, int) and isinstance(segment_sidecar, int):
                    segment_raw_bytes += segment_raw
                    segment_sidecar_bytes += segment_sidecar
                    receipts_valid &= (
                        segment_raw + segment_sidecar
                        == segment.get("reload_bytes")
                        == segment_payload
                    )
                else:
                    receipts_valid = False
            receipts_valid &= segment_raw_bytes == raw_reload_bytes
            receipts_valid &= segment_sidecar_bytes == sidecar_reload_bytes
            receipts_valid &= segment_raw_prediction_bytes == result.get(key, {}).get(
                "raw_prediction_sample_bytes_not_read"
            )
            receipts_valid &= segment_cropped_prediction_bytes == result.get(
                key, {}
            ).get("sidecar_cropped_prediction_sample_bytes_read")
            receipts_valid &= (
                segment_raw_prediction_bytes == segment_cropped_prediction_bytes
            )
            receipts_valid &= value.get("raw_replaced_section_bytes_read") == 0
            if not receipts_valid:
                errors.append(f"{key} specialized segment receipt changed")
        total_compile_seconds = support.get("total_compile_seconds")
        if (
            not isinstance(total_compile_seconds, (int, float))
            or not math.isfinite(total_compile_seconds)
            or total_compile_seconds != 0.0
        ):
            errors.append(f"{key} effective-support compile timing changed")
        for timing in ["initial_prepare_seconds", "prefetch_wait_seconds"]:
            value_seconds = support.get(timing)
            if (
                not isinstance(value_seconds, (int, float))
                or not math.isfinite(value_seconds)
                or value_seconds < 0.0
            ):
                errors.append(f"{key} effective-support {timing} changed")
        aot = value.get("aot_grouped_tile", {})
        if aot.get("enabled") is not True:
            errors.append(f"{key} AOT grouped-tile replay is disabled")
        if aot.get("use_count") != expected_segments:
            errors.append(f"{key} AOT grouped-tile use count changed")
        for name in (
            "runtime_grouping_builds",
            "runtime_sort_builds",
            "runtime_route_builds",
        ):
            if aot.get(name) != 0:
                errors.append(f"{key} AOT grouped-tile {name} is not zero")
        if (
            not isinstance(aot.get("sidecar_artifact_bytes"), int)
            or aot["sidecar_artifact_bytes"] <= 0
        ):
            errors.append(f"{key} AOT grouped-tile artifact bytes are missing")
        lifetime = aot.get("byte_lifetime_ledger", {})
        if (
            not isinstance(
                lifetime.get("raw_prediction_sample_bytes_replaced_at_compile"),
                int,
            )
            or lifetime["raw_prediction_sample_bytes_replaced_at_compile"] <= 0
            or lifetime.get("raw_prediction_sample_bytes_replaced_at_compile")
            != value.get("raw_prediction_sample_bytes_not_read")
            or lifetime.get("cropped_prediction_sample_bytes_persisted")
            != value.get("sidecar_cropped_prediction_sample_bytes_read")
            or lifetime.get("raw_prediction_sample_bytes_retained_for_replay") != 0
            or lifetime.get("raw_prediction_sample_bytes_read_during_replay") != 0
            or lifetime.get("prediction_replacement_equation")
            != "raw_prediction_sample_bytes_replaced_at_compile == "
            "cropped_prediction_sample_bytes_persisted"
            or lifetime.get("raw_sections_replaced_not_read")
            != [
                "prediction_samples",
                "tile_samples",
                "active_tile_ids",
                "tile_fragment_offsets",
                "fragments",
            ]
            or lifetime.get("raw_sections_referenced_not_copied")
            != [
                "source_sample_indices",
                "kernels",
                "prediction_phases",
                "tile_phases",
                "term_weights",
            ]
            or not isinstance(
                lifetime.get("raw_tile_sample_bytes_released_at_compile"), int
            )
            or lifetime["raw_tile_sample_bytes_released_at_compile"] <= 0
            or not isinstance(
                lifetime.get("raw_ungrouped_route_bytes_released_at_compile"), int
            )
            or not isinstance(lifetime.get("specialized_sidecar_section_bytes"), int)
            or lifetime["specialized_sidecar_section_bytes"] <= 0
            or lifetime.get("specialized_sidecar_file_bytes")
            != aot.get("sidecar_artifact_bytes")
            or verification_bytes != aot.get("sidecar_artifact_bytes")
        ):
            errors.append(f"{key} AOT grouped-tile lifetime ledger changed")
    summary = telemetry.get("summary", {})
    footprint = summary.get("process_physical_footprint_bytes_peak")
    if not isinstance(footprint, int):
        errors.append("process physical-footprint telemetry is missing")
    elif footprint > 32 * GIB:
        errors.append("process physical footprint exceeded 32 GiB")
    if four_spw_baseline_seconds is not None:
        four_seconds = result.get("four_spw", {}).get("seconds")
        if (
            not isinstance(four_seconds, (int, float))
            or four_seconds > four_spw_baseline_seconds * 1.05
        ):
            errors.append("four-SPW replay regressed by more than 5%")
    return errors


def run_benchmark(build: dict[str, Any], run_root: pathlib.Path) -> dict[str, Any]:
    compiler_binary_sha256 = current_build_binary_sha256(build)
    command = [
        build["binary"],
        "--exact",
        TEST_NAME,
        "--ignored",
        "--nocapture",
    ]
    environment = benchmark_environment(compiler_binary_sha256)
    log_path = run_root / "benchmark.log"
    sampler = DarwinHostTelemetrySampler(interval_seconds=1.0)
    sampler.start()
    started = time.monotonic()
    timed_out = False
    try:
        with log_path.open("w", encoding="utf-8") as log:
            process = subprocess.Popen(
                command,
                cwd=run_root,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            sampler.attach_targets(
                process_pid=process.pid, spill_volume_path="/Volumes/GLENDENNING"
            )
            try:
                exit_code = process.wait(timeout=900)
            except subprocess.TimeoutExpired:
                timed_out = True
                process.terminate()
                try:
                    exit_code = process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    exit_code = process.wait()
    finally:
        telemetry = sampler.stop()
    if timed_out:
        raise ContractError(f"replay benchmark timed out; see {log_path}")
    process_seconds = time.monotonic() - started
    if exit_code != 0:
        raise ContractError(f"release replay benchmark failed; see {log_path}")
    result = parse_benchmark_result(log_path.read_text(encoding="utf-8"))
    return {
        "command": command,
        "environment": environment,
        "wall_seconds": process_seconds,
        "exit_code": exit_code,
        "log": str(log_path),
        "log_sha256": sha256_file(log_path),
        "result": result,
        "telemetry": telemetry,
    }


def load_baseline_seconds() -> float | None:
    if not BASELINE_PATH.is_file():
        return None
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    value = baseline.get("four_spw_seconds")
    if not isinstance(value, (int, float)) or value <= 0:
        raise ContractError("frozen baseline has invalid four_spw_seconds")
    return float(value)


def measure() -> int:
    validate_common_inputs()
    for variant in VARIANTS:
        manifest = pathlib.Path(f"{fixture_prefix(variant)}.json")
        payload = pathlib.Path(f"{fixture_prefix(variant)}.payload")
        if not manifest.is_file() or not payload.is_file():
            raise ContractError(f"missing frozen {variant['id']} replay fixture")
        errors = validate_manifest(
            json.loads(manifest.read_text(encoding="utf-8")), variant
        )
        if errors:
            raise ContractError(f"{variant['id']} fixture: {errors[0]}")
    run_root = new_run_root("measure")
    build = build_benchmark_binary(run_root)
    sidecars = ensure_aot_grouped_sidecars(build, run_root)
    benchmark = run_benchmark(build, run_root)
    errors = result_errors(
        benchmark["result"],
        benchmark["telemetry"],
        four_spw_baseline_seconds=load_baseline_seconds(),
    )
    receipt = {
        "schema": "casa-rs-vlass-full16-replay-measurement-v1",
        "recorded_at": utc_now(),
        "source_head": subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
        "source_status": subprocess.run(
            ["git", "status", "--short", "--untracked-files=all"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout,
        "build": build,
        "aot_grouped_sidecars": sidecars,
        "benchmark": benchmark,
        "guard_errors": errors,
    }
    receipt_path = run_root / "receipt.json"
    atomic_write_json(receipt_path, receipt)
    receipt_sha256 = sha256_file(receipt_path)
    atomic_write_json(
        LATEST_PATH,
        {
            "schema": "casa-rs-vlass-full16-replay-latest-v1",
            "receipt": str(receipt_path),
            "receipt_sha256": receipt_sha256,
        },
    )
    seconds = benchmark["result"]["seconds"]
    if errors:
        print(
            json.dumps(
                {
                    "guard_passed": False,
                    "guard_error": errors[0],
                    "receipt": str(receipt_path),
                    "receipt_sha256": receipt_sha256,
                },
                sort_keys=True,
            )
        )
        raise ContractError(
            "measurement is ineligible for controller retention: " + errors[0]
        )
    print(
        json.dumps(
            {
                "seconds": seconds,
                "four_spw_seconds": benchmark["result"]["four_spw"]["seconds"],
                "guard_passed": True,
                "receipt": str(receipt_path),
                "receipt_sha256": receipt_sha256,
            },
            sort_keys=True,
        )
    )
    return 0


def load_latest_receipt() -> tuple[dict[str, Any], pathlib.Path]:
    if not LATEST_PATH.is_file():
        raise ContractError("no full-16-SPW replay measurement exists")
    latest = json.loads(LATEST_PATH.read_text(encoding="utf-8"))
    receipt_path = pathlib.Path(str(latest.get("receipt", "")))
    expected_sha = latest.get("receipt_sha256")
    if not receipt_path.is_file() or sha256_file(receipt_path) != expected_sha:
        raise ContractError("latest replay receipt identity changed")
    return json.loads(receipt_path.read_text(encoding="utf-8")), receipt_path


def freeze_baseline() -> int:
    if BASELINE_PATH.exists():
        raise ContractError(f"refusing to overwrite frozen baseline {BASELINE_PATH}")
    receipt, receipt_path = load_latest_receipt()
    result = receipt["benchmark"]["result"]
    errors = result_errors(
        result,
        receipt["benchmark"]["telemetry"],
        four_spw_baseline_seconds=None,
    )
    if errors:
        raise ContractError(f"baseline is not guardable: {errors[0]}")
    atomic_write_json(
        BASELINE_PATH,
        {
            "schema": "casa-rs-vlass-full16-replay-baseline-v1",
            "recorded_at": utc_now(),
            "receipt": str(receipt_path),
            "receipt_sha256": sha256_file(receipt_path),
            "full16_seconds": result["full16"]["seconds"],
            "four_spw_seconds": result["four_spw"]["seconds"],
            "release_binary_sha256": receipt["build"]["binary_sha256"],
        },
    )
    print(json.dumps({"baseline": str(BASELINE_PATH)}, sort_keys=True))
    return 0


def guard() -> int:
    receipt, receipt_path = load_latest_receipt()
    result = receipt["benchmark"]["result"]
    compiler_binary_sha256 = current_build_binary_sha256(receipt["build"])
    errors = result_errors(
        result,
        receipt["benchmark"]["telemetry"],
        four_spw_baseline_seconds=load_baseline_seconds(),
    )
    recorded_sidecars = {
        sidecar.get("variant"): sidecar.get("validation", {})
        for sidecar in receipt.get("aot_grouped_sidecars", [])
        if isinstance(sidecar, dict)
    }
    for variant in VARIANTS:
        sidecar_errors, current = validate_aot_grouped_sidecar(
            variant,
            compiler_binary_sha256,
            update_hash_seals=False,
        )
        errors.extend(
            f"{variant['id']} AOT sidecar: {error}" for error in sidecar_errors
        )
        recorded = recorded_sidecars.get(variant["id"], {})
        identity = (
            "compiler_binary_sha256",
            "manifest_sha256",
            "payload",
            "payload_bytes",
        )
        if any(current.get(name) != recorded.get(name) for name in identity):
            errors.append(f"{variant['id']} AOT sidecar receipt identity changed")
        for role in ("raw_payload", "sidecar_payload"):
            if current.get("hashes", {}).get(role, {}).get("sha256") != recorded.get(
                "hashes", {}
            ).get(role, {}).get("sha256"):
                errors.append(f"{variant['id']} AOT sidecar {role} hash changed")
    current_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    current_status = subprocess.run(
        ["git", "status", "--short", "--untracked-files=all"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if receipt.get("source_head") != current_head:
        errors.append("source HEAD changed after measurement")
    if receipt.get("source_status") != current_status:
        errors.append("source worktree changed after measurement")
    if errors:
        raise ContractError(f"replay guard failed: {errors[0]}")
    print(
        json.dumps(
            {
                "guard": "passed",
                "receipt": str(receipt_path),
                "full16_seconds": result["full16"]["seconds"],
                "four_spw_seconds": result["four_spw"]["seconds"],
            },
            sort_keys=True,
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action",
        choices=("capture", "seal-fixtures", "measure", "freeze-baseline", "guard"),
    )
    args = parser.parse_args()
    try:
        if args.action == "capture":
            return capture()
        if args.action == "seal-fixtures":
            return seal_fixtures()
        if args.action == "measure":
            return measure()
        if args.action == "freeze-baseline":
            return freeze_baseline()
        return guard()
    except (ContractError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
