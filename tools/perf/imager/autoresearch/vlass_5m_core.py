#!/usr/bin/env python3
"""Pure contracts and log parsing for the bounded VLASS autoresearch row."""

from __future__ import annotations

import hashlib
import json
import math
import pathlib
import re
import shlex
from dataclasses import dataclass
from typing import Any, Iterable


SCHEMA_VERSION = 1
REPLAY_PREFIX = "mosaic_mtmfs_stream_replay "
PROGRAM_PREFIX = "awproject_metal_resident_tile_chain "
COMPACT_PREFIX = "awproject_compact_source_order "
CACHE_PREFIX = "awproject_cache "
GRID_PREFIX = "awproject_metal_grid_summary "
READBACK_PREFIX = "awproject_metal_compensated_residual_readback "
PRODUCT_PREFIX = "image_product_write "
FRONTEND_PREFIX = "frontend stage=run_summary "
CORE_PREFIX = "core stage=run_summary "
PLAN_PREFIX = "awproject_plan "
DDID_PLAN_PREFIX = "mfs_ddid_execution_plan "
FROZEN_MODEL_PREFIX = "awproject_frozen_model_refresh "
FROZEN_SUPPORT_PREFIX = "awproject_frozen_model_support "
FINAL_REFRESH_PREFIX = "mosaic_mtmfs_final_residual_refresh "
WRITE_PREFIX = "Wrote CASA-compatible products at prefix "


class ContractError(ValueError):
    """A workload contract, log, or receipt is not promotable."""


@dataclass(frozen=True)
class ParsedLine:
    prefix: str
    values: dict[str, Any]
    text: str


def sha256_file(path: pathlib.Path, *, block_bytes: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(block_bytes), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_json_object(path: pathlib.Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ContractError(f"read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise ContractError(f"{label} must contain a JSON object: {path}")
    return value


def load_contract(path: pathlib.Path) -> dict[str, Any]:
    contract = load_json_object(path, label="VLASS autoresearch contract")
    if contract.get("schema_version") != SCHEMA_VERSION:
        raise ContractError(
            f"unsupported VLASS autoresearch schema {contract.get('schema_version')!r}"
        )
    if contract.get("workload_id") != "vlass-aw-residual-refresh-5m-v1":
        raise ContractError("unexpected VLASS autoresearch workload_id")
    if contract.get("build", {}).get("profile") != "release":
        raise ContractError("the VLASS autoresearch build profile must be release")
    command = contract.get("build", {}).get("command")
    if not isinstance(command, list) or "--release" not in command:
        raise ContractError(
            "the VLASS autoresearch build command must contain --release"
        )
    workload = contract.get("workload", {})
    if workload.get("imsize") != 4096:
        raise ContractError(
            "the VLASS autoresearch image geometry must remain 4096-square"
        )
    if len(workload.get("field_ids", [])) != 63:
        raise ContractError(
            "the VLASS autoresearch workload must contain all 63 fields"
        )
    if workload.get("spw_ids") != [2, 7, 12, 17]:
        raise ContractError("the VLASS autoresearch workload must use SPWs 2,7,12,17")
    if workload.get("channel_count") != 24:
        raise ContractError(
            "the VLASS autoresearch workload must use 24 channels per SPW"
        )
    if workload.get("products") is None or len(workload["products"]) != 19:
        raise ContractError("the VLASS autoresearch workload must emit 19 products")
    return contract


def parse_value(value: str) -> Any:
    if value in {"true", "false"}:
        return value == "true"
    if value == "unavailable":
        return None
    if re.fullmatch(r"-?[0-9]+", value):
        return int(value)
    if re.fullmatch(r"-?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?", value):
        number = float(value)
        if math.isfinite(number):
            return number
    return value


def parse_key_values(text: str, prefix: str) -> ParsedLine:
    if not text.startswith(prefix):
        raise ContractError(f"log line does not start with {prefix!r}")
    values: dict[str, Any] = {}
    for token in shlex.split(text[len(prefix) :]):
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        values[key] = parse_value(value.rstrip(","))
    return ParsedLine(prefix=prefix, values=values, text=text)


def lines_with_prefix(lines: Iterable[str], prefix: str) -> list[ParsedLine]:
    return [parse_key_values(line, prefix) for line in lines if line.startswith(prefix)]


def require_last(lines: list[str], prefix: str) -> ParsedLine:
    matches = lines_with_prefix(lines, prefix)
    if not matches:
        raise ContractError(f"required runtime telemetry is missing: {prefix.strip()}")
    return matches[-1]


def _sum_numeric(rows: Iterable[ParsedLine], field: str) -> float:
    total = 0.0
    for row in rows:
        value = row.values.get(field)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ContractError(
                f"runtime telemetry field is missing: {row.prefix}{field}"
            )
        total += float(value)
    return total


def _max_numeric(rows: Iterable[ParsedLine], field: str) -> float:
    values = [row.values.get(field) for row in rows]
    if not values or any(
        not isinstance(value, (int, float)) or isinstance(value, bool)
        for value in values
    ):
        raise ContractError(f"runtime telemetry field is missing: {field}")
    return max(float(value) for value in values)


def _integer(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(f"runtime telemetry field must be an integer: {field}")
    return value


def _number(value: Any, *, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise ContractError(f"runtime telemetry field must be finite: {field}")
    return float(value)


def _last_residual_segment(lines: list[str]) -> tuple[ParsedLine, list[str]]:
    replay_indices = [
        index for index, line in enumerate(lines) if line.startswith(REPLAY_PREFIX)
    ]
    residual_indices = [
        index
        for index in replay_indices
        if parse_key_values(lines[index], REPLAY_PREFIX).values.get("pass")
        == "ResidualRefresh"
    ]
    if not residual_indices:
        raise ContractError("no production ResidualRefresh replay was observed")
    selected = residual_indices[-1]
    prior = max((index for index in replay_indices if index < selected), default=-1)
    following = min(
        (index for index in replay_indices if index > selected), default=len(lines)
    )
    return (
        parse_key_values(lines[selected], REPLAY_PREFIX),
        lines[prior + 1 : following],
    )


def parse_runtime_log(text: str) -> dict[str, Any]:
    """Extract the timed residual refresh and its cache-pressure signature."""

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    residual, segment = _last_residual_segment(lines)
    programs = lines_with_prefix(segment, PROGRAM_PREFIX)
    compact = lines_with_prefix(segment, COMPACT_PREFIX)
    caches = lines_with_prefix(segment, CACHE_PREFIX)
    grids = [
        row
        for row in lines_with_prefix(segment, GRID_PREFIX)
        if row.values.get("pass") == "residual_refresh"
    ]
    readbacks = lines_with_prefix(segment, READBACK_PREFIX)
    if not programs:
        raise ContractError(
            "residual refresh did not build compact Metal replay programs"
        )
    if not compact:
        raise ContractError(
            "residual refresh did not report compact source-order windows"
        )
    if not caches:
        raise ContractError(
            "residual refresh did not report application-cache telemetry"
        )
    if not grids:
        raise ContractError("residual refresh did not report Metal gridding telemetry")
    if not readbacks:
        raise ContractError("residual refresh did not report FFT readback telemetry")

    cache = caches[-1].values
    cache_loads = _integer(cache.get("loads"), field="awproject_cache.loads")
    cache_hits = _integer(cache.get("hits"), field="awproject_cache.hits")
    cache_evictions = _integer(
        cache.get("evictions"), field="awproject_cache.evictions"
    )
    cache_attempted = _integer(
        cache.get("attempted_samples"), field="awproject_cache.attempted_samples"
    )
    cache_accepted = _integer(
        cache.get("accepted_samples"), field="awproject_cache.accepted_samples"
    )
    rejection_fields = sorted(
        key for key in cache if isinstance(key, str) and key.startswith("rejected_")
    )
    rejected = sum(
        _integer(cache.get(field), field=f"awproject_cache.{field}")
        for field in rejection_fields
    )
    cache_accesses = cache_loads + cache_hits

    products = lines_with_prefix(lines, PRODUCT_PREFIX)
    product_inventory = [str(row.values.get("suffix")) for row in products]
    product_shapes = {
        str(row.values.get("suffix")): row.values.get("shape") for row in products
    }
    final_refreshes = lines_with_prefix(lines, FINAL_REFRESH_PREFIX)
    frozen_models = lines_with_prefix(lines, FROZEN_MODEL_PREFIX)
    frozen_supports = lines_with_prefix(lines, FROZEN_SUPPORT_PREFIX)
    plans = lines_with_prefix(lines, PLAN_PREFIX)
    ddid_plans = lines_with_prefix(lines, DDID_PLAN_PREFIX)
    frontend = require_last(lines, FRONTEND_PREFIX)
    core = require_last(lines, CORE_PREFIX)
    writes = [line for line in lines if line.startswith(WRITE_PREFIX)]
    if not writes:
        raise ContractError("CASA-compatible product completion marker is missing")

    mueller_match = re.search(r"\bcf_mueller=\[([^\]]+)\]", plans[-1].text)
    mueller = (
        [int(value.strip()) for value in mueller_match.group(1).split(",")]
        if mueller_match
        else None
    )

    metric_ms = _number(residual.values.get("elapsed_ms"), field="replay.elapsed_ms")
    return {
        "schema_version": SCHEMA_VERSION,
        "metric": {
            "name": "production_residual_refresh_seconds",
            "seconds": metric_ms / 1000.0,
            "source": residual.text,
        },
        "replay": {
            **residual.values,
            "residual_refresh_count": len(
                [
                    row
                    for row in lines_with_prefix(lines, REPLAY_PREFIX)
                    if row.values.get("pass") == "ResidualRefresh"
                ]
            ),
        },
        "compact_programs": {
            "builds": len(programs),
            "logical_program_bytes": int(_sum_numeric(programs, "program_bytes")),
            "max_program_bytes": int(_max_numeric(programs, "program_bytes")),
            "build_ms": _sum_numeric(programs, "build_ms"),
            "dispatch_wait_ms": _sum_numeric(programs, "dispatch_wait_ms"),
            "total_ms": _sum_numeric(programs, "total_ms"),
        },
        "source_order": {
            "blocks": len(compact),
            "windows": int(_sum_numeric(compact, "windows")),
            "largest_window_samples": int(
                _max_numeric(compact, "largest_window_samples")
            ),
            "routed_samples": int(_sum_numeric(compact, "routed_samples")),
            "plan_ms": _sum_numeric(compact, "plan_ms"),
            "materialize_ms": _sum_numeric(compact, "materialize_ms"),
            "cache_load_worker_ms": _sum_numeric(compact, "cache_load_worker_ms"),
            "tap_pack_ms": _sum_numeric(compact, "tap_pack_ms"),
            "prepare_ms": _sum_numeric(compact, "prepare_ms"),
            "grid_including_tile_plan_ms": _sum_numeric(
                compact, "grid_including_tile_plan_ms"
            ),
            "spatial_tile_sides": sorted(
                {
                    _integer(
                        row.values.get("spatial_tile_side"),
                        field="compact.spatial_tile_side",
                    )
                    for row in compact
                }
            ),
        },
        "application_cache": {
            **cache,
            "rejected_samples": rejected,
            "hit_rate": cache_hits / cache_accesses if cache_accesses else None,
            "eviction_load_ratio": (
                cache_evictions / cache_loads if cache_loads else None
            ),
            "accepted_attempted_ratio": (
                cache_accepted / cache_attempted if cache_attempted else None
            ),
        },
        "metal_grid": grids[-1].values,
        "readback": readbacks[-1].values,
        "awproject_plan": {
            **plans[-1].values,
            "cf_mueller": mueller,
        },
        "ddid_plan": ddid_plans[-1].values,
        "frozen_model": {
            "refresh_count": len(frozen_models),
            "support_positions": (
                frozen_supports[-1].values.get("positions") if frozen_supports else None
            ),
        },
        "final_refresh_count": len(final_refreshes),
        "products": {
            "inventory": product_inventory,
            "shapes": product_shapes,
        },
        "frontend": frontend.values,
        "core": core.values,
        "completion": writes[-1],
        "fallback_markers": [
            line
            for line in lines
            if re.search(r"(?:^|[ _])fallback=(?:true|1)(?:$| )", line)
            or "status=fallback" in line
        ],
    }


def comparison_request(
    contract: dict[str, Any],
    *,
    candidate_prefix: pathlib.Path,
    baseline_prefix: pathlib.Path,
    run_root: pathlib.Path,
) -> dict[str, Any]:
    comparison = contract["comparison"]
    ceiling = comparison["normalized_rms_ceiling"]
    return {
        "mode": "sampled",
        "left_prefix": str(candidate_prefix),
        "right_prefix": str(baseline_prefix),
        "left_label": "casa-rs candidate",
        "right_label": "casa-rs frozen proxy baseline",
        "products": comparison["products"],
        "max_elements_per_product": comparison["max_elements_per_product"],
        "require_exact_product_inventory": False,
        "require_metadata_parity": comparison["require_metadata_parity"],
        "source_regions": [],
        "tolerances": {
            "contract_version": 1,
            "require_full_array": False,
            "default": {
                "diff_rms_over_right_rms": ceiling,
                "require_topology_parity": comparison["require_topology_parity"],
            },
            "products": {},
        },
        "panel_dir": str(run_root / "comparison-panels"),
        "structure_workspace_dir": str(run_root / "comparison-structure-workspace"),
    }


def runtime_environment(contract: dict[str, Any], *, home: str) -> dict[str, str]:
    fftw = contract["inputs"]["fftw_library_dir"]
    return {
        "PATH": "/opt/homebrew/bin:/usr/bin:/bin",
        "HOME": home,
        "CASA_RS_MEASURESPATH": contract["inputs"]["measures_dir"],
        "CASA_RS_FFTW_LIBRARY_DIR": fftw,
        "DYLD_LIBRARY_PATH": fftw,
        "CASA_RS_VLASS_EXPERIMENT_RUNNER": "1",
        "CASA_RS_FFTW_THREADS": "8",
        "CASA_RS_STANDARD_MFS_PROFILE_DETAIL": "1",
        "CASA_RS_EXPERIMENTAL_MT_MFS_SPARSE_RHS": "1",
        "CASA_RS_EXPERIMENTAL_MT_MFS_CASA_FFT0": "1",
        "CASA_RS_EXPERIMENTAL_AWPROJECT_WINDOWED_HYBRID_CLEAN": "1",
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
        "CASA_RS_EXPERIMENTAL_AWPROJECT_FROZEN_MODEL_PREFIX": contract["inputs"][
            "frozen_model_prefix"
        ],
    }


def runtime_command(
    contract: dict[str, Any], *, binary: pathlib.Path, output: pathlib.Path
) -> list[str]:
    workload = contract["workload"]
    dataset = contract["dataset"]
    return [
        str(binary),
        "--ms",
        dataset["measurement_set"],
        "--imagename",
        str(output),
        "--imsize",
        str(workload["imsize"]),
        "--cell-arcsec",
        str(workload["cell_arcsec"]),
        "--field",
        workload["field_selection"],
        "--phasecenter-field",
        str(workload["phasecenter_field"]),
        "--spw",
        ",".join(str(value) for value in workload["spw_ids"]),
        "--channel-start",
        str(workload["channel_start"]),
        "--channel-count",
        str(workload["channel_count"]),
        "--specmode",
        "mfs",
        "--gridder",
        workload["gridder"],
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
        workload["weighting"],
        "--robust",
        str(workload["weighting_robust"]),
        "--perchanweightdensity",
        "--deconvolver",
        workload["deconvolver"],
        "--standard-mfs-acceleration",
        "metal",
        "--imaging-fft-precision",
        "f64",
        "--imaging-fft-backend",
        "fftw",
        "--parallel",
        "--standard-mfs-grid-threads",
        "2",
        "--imaging-memory-target-mb",
        str(workload["memory_target_mb"]),
        "--imaging-memory-pressure-policy",
        workload["memory_pressure_policy"],
        "--imaging-prepare-workers",
        "1",
        "--imaging-read-ahead-blocks",
        "1",
        "--hogbom-iteration-mode",
        "strict",
        "--nterms",
        str(workload["nterms"]),
        "--scales",
        ",".join(str(value) for value in workload["scales"]),
        "--niter",
        str(workload["niter"]),
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
        str(workload["wprojplanes"]),
        "--cfcache",
        dataset["cf_cache"],
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
        dataset["mask"],
        "--savemodel",
        "none",
        "--restoringbeam",
        "common",
        "--no-preview-pngs",
    ]


def evaluate_receipt(
    contract: dict[str, Any],
    receipt: dict[str, Any],
    *,
    expected_receipt_sha256: str,
    actual_receipt_sha256: str,
    current_source_state_sha256: str,
) -> list[str]:
    """Return fail-closed guard violations without mutating any evidence."""

    errors: list[str] = []

    def expect(condition: bool, message: str) -> None:
        if not condition:
            errors.append(message)

    expect(
        actual_receipt_sha256 == expected_receipt_sha256,
        "latest pointer receipt SHA-256 does not match the receipt",
    )
    expect(receipt.get("schema_version") == SCHEMA_VERSION, "receipt schema mismatch")
    expect(
        receipt.get("workload_id") == contract["workload_id"],
        "receipt workload mismatch",
    )
    expect(
        receipt.get("source", {}).get("state_sha256") == current_source_state_sha256,
        "source state changed after the measured release build",
    )
    build = receipt.get("build", {})
    expect(
        build.get("profile") == "release", "timed executable was not release optimized"
    )
    expect(
        build.get("command") == contract["build"]["command"],
        "release build command changed",
    )
    expect(
        pathlib.Path(str(build.get("binary", ""))).parent.name == "release",
        "receipt-bound executable is not from a release directory",
    )
    expect(build.get("timed_build_seconds") == 0.0, "build time entered timed region")
    expect(
        bool(re.fullmatch(r"[0-9a-f]{64}", str(build.get("binary_sha256", "")))),
        "release executable SHA-256 is missing",
    )
    expect(
        build.get("completed_before_timed_region") is True,
        "release build did not complete before timing",
    )
    process = receipt.get("process", {})
    expect(process.get("exit_code") == 0, "imager exited nonzero")
    process_command = process.get("command")
    expect(
        isinstance(process_command, list)
        and bool(process_command)
        and process_command[0] == build.get("binary"),
        "timed command is not bound to the frozen release executable",
    )
    expect(
        isinstance(process.get("wall_seconds"), (int, float))
        and not isinstance(process.get("wall_seconds"), bool)
        and 0.0
        < float(process["wall_seconds"])
        <= float(contract["metric"]["max_process_wall_seconds"]),
        "timed process wall duration is invalid",
    )
    runtime = receipt.get("runtime", {})
    metric = runtime.get("metric", {})
    expect(
        isinstance(metric.get("seconds"), (int, float))
        and not isinstance(metric.get("seconds"), bool)
        and float(metric["seconds"]) > 0.0,
        "timed residual-refresh metric is invalid",
    )
    replay = runtime.get("replay", {})
    expect(replay.get("pass") == "ResidualRefresh", "metric is not ResidualRefresh")
    expect(
        replay.get("residual_refresh_count") == 1,
        "proxy must execute exactly one residual refresh",
    )
    expect(replay.get("blocks") == 4, "residual refresh must include four SPW blocks")
    pressure = contract["guard"]["pressure"]
    samples = replay.get("samples")
    expect(
        isinstance(samples, int)
        and pressure["accepted_samples_min"]
        <= samples
        <= pressure["accepted_samples_max"],
        "accepted sample pressure is outside the frozen proxy range",
    )
    programs = runtime.get("compact_programs", {})
    expect(
        programs.get("builds", 0) >= pressure["program_builds_min"],
        "compact replay program-build pressure is too low",
    )
    expect(
        programs.get("logical_program_bytes", 0)
        >= pressure["logical_program_bytes_min"],
        "logical replay-program pressure is too low",
    )
    source_order = runtime.get("source_order", {})
    expect(
        source_order.get("windows", 0) >= pressure["compact_windows_min"],
        "compact source-order segmentation pressure is too low",
    )
    cache = runtime.get("application_cache", {})
    cache_guard = contract["guard"]["application_cache"]
    expect(cache.get("loads", 0) >= cache_guard["loads_min"], "cache loads are too low")
    expect(
        cache.get("hit_rate") is not None
        and cache["hit_rate"] <= cache_guard["hit_rate_max"],
        "application-cache hit rate no longer reproduces the miss-heavy regime",
    )
    expect(
        cache.get("eviction_load_ratio") is not None
        and cache["eviction_load_ratio"] >= cache_guard["eviction_load_ratio_min"],
        "application-cache eviction pressure is too low",
    )
    resident = cache.get("resident_bytes")
    expect(
        isinstance(resident, int)
        and cache_guard["resident_bytes_min"]
        <= resident
        <= cache_guard["resident_bytes_max"],
        "application-cache residency is outside the frozen range",
    )
    expect(
        cache.get("rejected_samples") == pressure["rejected_samples_max"],
        "scientific samples were rejected by the AWProject cache",
    )
    expect(runtime.get("fallback_markers") == [], "runtime used a fallback path")
    plan = runtime.get("awproject_plan", {})
    workload = contract["workload"]
    expect(plan.get("image_shape") == "4096x4096", "runtime image shape changed")
    expect(
        plan.get("wplanes") == workload["wprojplanes"], "runtime W-plane count changed"
    )
    expect(plan.get("aterm") is True, "A-term is disabled")
    expect(plan.get("wbawp") is True, "wideband A projection is disabled")
    expect(plan.get("conjbeams") is True, "conjugate beams are disabled")
    expect(plan.get("usepointing") is True, "POINTING behavior is disabled")
    expect(
        plan.get("cf_mueller") == workload["mueller_elements"], "Mueller terms changed"
    )
    expect(
        plan.get("cf_metadata_key") == contract["dataset"]["cf_metadata_key"],
        "CF metadata identity changed",
    )
    ddid = runtime.get("ddid_plan", {})
    expect(ddid.get("ddids") == "2,7,12,17", "DDID selection changed")
    expect(ddid.get("spws") == "2,7,12,17", "SPW selection changed")
    frozen = runtime.get("frozen_model", {})
    expect(frozen.get("refresh_count") == 1, "frozen model was not loaded exactly once")
    expect(
        isinstance(frozen.get("support_positions"), int)
        and frozen["support_positions"] > 0,
        "frozen model is empty",
    )
    expect(runtime.get("final_refresh_count") == 1, "final residual marker is missing")
    products = runtime.get("products", {})
    expect(
        products.get("inventory") == workload["products"],
        "19-product inventory or order changed",
    )
    expected_shape = "4096x4096x1x1"
    for suffix, shape in products.get("shapes", {}).items():
        if suffix.startswith(".sumwt"):
            expect(shape == "1x1x1x1", f"{suffix} shape changed")
        else:
            expect(shape == expected_shape, f"{suffix} shape changed")
    selection = receipt.get("selection", {})
    expect(
        selection.get("field_ids") == workload["field_ids"],
        "63-field selection changed",
    )
    expect(selection.get("spw_ids") == workload["spw_ids"], "selection SPWs changed")
    expect(
        selection.get("accounting_sha256")
        == contract["dataset"]["selection_accounting_sha256"],
        "MS selection-accounting identity changed",
    )
    comparison = receipt.get("comparison")
    if contract["baseline"]["status"] == "frozen":
        expect(
            isinstance(comparison, dict)
            and comparison.get("status") == "completed"
            and comparison.get("tolerance_evaluation", {}).get("status") == "passed",
            "proxy output comparison failed",
        )
    else:
        expect(comparison is None, "qualification run unexpectedly compared a baseline")
    telemetry = receipt.get("host_telemetry", {})
    summary = telemetry.get("summary", {})
    expect(
        telemetry.get("status") in {"measured", "partial"},
        "host telemetry was not measured",
    )
    memory_guard = contract["guard"]["memory"]
    footprint = summary.get("process_physical_footprint_bytes_peak")
    expect(
        isinstance(footprint, int)
        and footprint <= memory_guard["process_physical_footprint_bytes_max"],
        "process physical footprint exceeded the proxy ceiling",
    )
    swap_io = int(summary.get("swapin_bytes_delta") or 0) + int(
        summary.get("swapout_bytes_delta") or 0
    )
    expect(swap_io <= memory_guard["swap_io_bytes_max"], "swap I/O was destructive")
    return errors
