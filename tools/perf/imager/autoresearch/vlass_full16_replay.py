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
EFFECTIVE_SUPPORT_ENV = "CASA_RS_EXPERIMENTAL_AWPROJECT_EFFECTIVE_SUPPORT"
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
GIB = 1024**3


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
    if not isinstance(payload_bytes, int) or payload_bytes <= 0:
        errors.append("fixture file-byte accounting is missing")
    if not isinstance(ledger_bytes, int) or ledger_bytes <= 0:
        errors.append("fixture section-byte accounting is missing")
    elif ledger_bytes != program_payload_bytes:
        errors.append("fixture aggregate payload-byte accounting changed")
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


def benchmark_environment() -> dict[str, str]:
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
        }
    )
    if EFFECTIVE_SUPPORT_ENV in os.environ:
        environment[EFFECTIVE_SUPPORT_ENV] = os.environ[EFFECTIVE_SUPPORT_ENV]
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
        role_values = [role.get(name) for name in (
            "plan_count",
            "unique_stencils",
            "original_tap_visits",
            "retained_tap_visits",
            "cropped_plans",
        )]
        if any(
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
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


def result_errors(
    result: dict[str, Any],
    telemetry: dict[str, Any],
    *,
    four_spw_baseline_seconds: float | None,
    effective_support_requested: bool,
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
            or any(not isinstance(item, (int, float)) or item > 1e-3 for item in nrmse)
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
        if value.get("reload_bytes") != value.get("payload_bytes"):
            errors.append(f"{key} replay reload-byte accounting changed")
        segments = value.get("segment_receipts")
        if not isinstance(segments, list) or len(segments) != expected_segments:
            errors.append(f"{key} per-segment timing inventory changed")
        support = value.get("effective_support", {})
        if effective_support_requested and expected_segments >= 2:
            expected_decision = "enabled"
            expected_reason = None
            expected_compiled_segments = expected_segments
            expected_markers = expected_segments
        elif effective_support_requested:
            expected_decision = "rejected"
            expected_reason = "single_segment_no_prefetch_overlap"
            expected_compiled_segments = 0
            expected_markers = 0
        else:
            expected_decision = "not_requested"
            expected_reason = None
            expected_compiled_segments = 0
            expected_markers = 0
        if support.get("requested") is not effective_support_requested:
            errors.append(f"{key} effective-support requested state changed")
        if support.get("decision") != expected_decision:
            errors.append(f"{key} effective-support decision changed")
        if support.get("reason") != expected_reason:
            errors.append(f"{key} effective-support reason changed")
        if support.get("segment_count") != expected_segments:
            errors.append(f"{key} effective-support segment count changed")
        if support.get("compiled_segment_count") != expected_compiled_segments:
            errors.append(f"{key} effective-support compiled segment count changed")
        if value.get("effective_support_telemetry_markers") != expected_markers:
            errors.append(f"{key} effective-support telemetry marker count changed")
        if isinstance(segments, list) and len(segments) == expected_segments:
            receipts_valid = True
            for segment in segments:
                segment_support = (
                    segment.get("effective_support")
                    if isinstance(segment, dict)
                    else None
                )
                if expected_compiled_segments > 0:
                    receipts_valid &= effective_support_segment_receipt_is_valid(
                        segment_support
                    )
                else:
                    receipts_valid &= segment_support is None
            if not receipts_valid:
                errors.append(f"{key} effective-support segment receipt changed")
        total_compile_seconds = support.get("total_compile_seconds")
        if (
            not isinstance(total_compile_seconds, (int, float))
            or not math.isfinite(total_compile_seconds)
            or total_compile_seconds < 0.0
            or (expected_compiled_segments == 0 and total_compile_seconds != 0.0)
            or (expected_compiled_segments > 0 and total_compile_seconds <= 0.0)
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
    command = [
        build["binary"],
        "--exact",
        TEST_NAME,
        "--ignored",
        "--nocapture",
    ]
    environment = benchmark_environment()
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
    benchmark = run_benchmark(build, run_root)
    errors = result_errors(
        benchmark["result"],
        benchmark["telemetry"],
        four_spw_baseline_seconds=load_baseline_seconds(),
        effective_support_requested=EFFECTIVE_SUPPORT_ENV in benchmark["environment"],
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
    print(
        json.dumps(
            {
                "seconds": seconds,
                "four_spw_seconds": benchmark["result"]["four_spw"]["seconds"],
                "guard_passed": not errors,
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
        effective_support_requested=EFFECTIVE_SUPPORT_ENV
        in receipt["benchmark"]["environment"],
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
    errors = result_errors(
        result,
        receipt["benchmark"]["telemetry"],
        four_spw_baseline_seconds=load_baseline_seconds(),
        effective_support_requested=EFFECTIVE_SUPPORT_ENV
        in receipt["benchmark"]["environment"],
    )
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
