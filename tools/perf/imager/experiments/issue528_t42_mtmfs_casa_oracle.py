#!/usr/bin/env python3
"""Reproduce the focused CASA #528 MT-MFS oracle outside the repository.

The generated NPZ intentionally remains an external test artifact because it
contains selected visibility data.  This recipe and the checked-in receipt are
the durable, redistributable record.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
import platform
import shutil
import struct
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[4]
DATASET_RELATIVE = Path("measurementset/vla/ref_vlass_wtsp_creation.ms")
NPZ_NAME = "casa-mtmfs-two-spw-oracle.npz"
MANIFEST_NAME = "manifest.json"

np: Any
casatasks: Any
casatools: Any
image_tool: Any
ms_tool: Any
quanta_tool: Any
table_tool: Any

PRODUCT_SUFFIXES = {
    "psf_moment_0": ".psf.tt0",
    "psf_moment_1": ".psf.tt1",
    "psf_moment_2": ".psf.tt2",
    "dirty_taylor_0": ".residual.tt0",
    "dirty_taylor_1": ".residual.tt1",
    "sum_weight_0": ".sumwt.tt0",
    "sum_weight_1": ".sumwt.tt1",
    "sum_weight_2": ".sumwt.tt2",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=REPO,
        help="casa-rs checkout containing scripts/bench-imager-vs-casa.sh",
    )
    parser.add_argument(
        "--testdata-root",
        type=Path,
        help="explicit casatestdata root; otherwise use the shared slow-parity candidates",
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=REPO / "target" / "t42-casa-oracle",
        help="external output directory for the generated NPZ and local manifest",
    )
    parser.add_argument(
        "--casa-prefix",
        type=Path,
        help="CASA product prefix; defaults to ARTIFACT_ROOT/raw-run-local/casa/casa",
    )
    parser.add_argument(
        "--casa-log",
        type=Path,
        required=True,
        help="successful CASA log from the matched tclean run",
    )
    parser.add_argument(
        "--tclean-wall-seconds",
        type=float,
        help="optional tclean wall time from the matched runner receipt",
    )
    parser.add_argument(
        "--runner-wall-seconds",
        type=float,
        help="optional end-to-end runner wall time from the matched runner receipt",
    )
    parser.add_argument(
        "--created-utc",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        help="receipt timestamp; override to reproduce a frozen receipt exactly",
    )
    return parser.parse_args()


def load_casa_modules() -> None:
    global np, casatasks, casatools, image_tool, ms_tool, quanta_tool, table_tool
    try:
        import numpy as numpy_module
        import casatasks as casatasks_module
        import casatools as casatools_module
        from casatools import image, ms, quanta, table
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "run this recipe with the matched CASA Python containing numpy, "
            "casatasks, and casatools"
        ) from error
    np = numpy_module
    casatasks = casatasks_module
    casatools = casatools_module
    image_tool = image
    ms_tool = ms
    quanta_tool = quanta
    table_tool = table


def resolve_testdata_root(repo: Path, explicit: Path | None) -> Path:
    configured = explicit
    if configured is None and (value := os.environ.get("CASA_RS_TESTDATA_ROOT")):
        configured = Path(value)
    if configured is not None:
        root = configured.expanduser().resolve()
        if (root / DATASET_RELATIVE).is_dir():
            return root
        raise RuntimeError(
            f"configured casatestdata root lacks {DATASET_RELATIVE}: {root}"
        )

    candidates = [
        repo.parent / "casatestdata",
        Path.home() / "SoftwareProjects" / "casatestdata",
        Path("/Volumes/home/casatestdata"),
    ]
    for candidate in candidates:
        if (candidate / DATASET_RELATIVE).is_dir():
            return candidate.resolve()
    searched = ", ".join(str(candidate) for candidate in candidates)
    raise RuntimeError(
        "slow-parity casatestdata preflight failed for "
        f"{DATASET_RELATIVE}; searched {searched}; set CASA_RS_TESTDATA_ROOT or "
        "pass --testdata-root"
    )


def git_value(repo: Path, *args: str) -> str | None:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    value = result.stdout.strip()
    return value if result.returncode == 0 and value else None


def json_safe(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        return [json_safe(item) for item in value.tolist()]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, complex):
        return [float(value.real), float(value.imag)]
    if isinstance(value, float) and not math.isfinite(value):
        return "NaN" if math.isnan(value) else ("+Infinity" if value > 0 else "-Infinity")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_tree(path: Path, *, excluded_names: set[str] | None = None) -> tuple[str, int, int]:
    excluded_names = excluded_names or set()
    digest = hashlib.sha256()
    count = 0
    size = 0
    for candidate in sorted(item for item in path.rglob("*") if item.is_file()):
        if candidate.name in excluded_names:
            continue
        relative = candidate.relative_to(path).as_posix().encode("utf-8")
        payload_hash = sha256_file(candidate)
        payload_size = candidate.stat().st_size
        digest.update(struct.pack("<Q", len(relative)))
        digest.update(relative)
        digest.update(struct.pack("<Q", payload_size))
        digest.update(bytes.fromhex(payload_hash))
        count += 1
        size += payload_size
    return digest.hexdigest(), count, size


def bytes_sha256(array: np.ndarray) -> str:
    contiguous = np.ascontiguousarray(array)
    return hashlib.sha256(contiguous.tobytes(order="C")).hexdigest()


def array_receipt(array: np.ndarray, mask: np.ndarray | None = None) -> dict[str, Any]:
    values = np.asarray(array)
    finite = np.isfinite(values)
    support = finite if mask is None else finite & np.asarray(mask, dtype=np.bool_)
    selected = values[support]
    receipt = {
        "shape": list(values.shape),
        "dtype": values.dtype.str,
        "bytes_sha256": bytes_sha256(values),
        "finite_count": int(np.count_nonzero(finite)),
        "support_count": int(np.count_nonzero(support)),
    }
    if np.iscomplexobj(selected):
        magnitudes = np.abs(selected.astype(np.complex128))
        receipt.update(
            {
                "min_abs": float(np.min(magnitudes)) if selected.size else None,
                "max_abs": float(np.max(magnitudes)) if selected.size else None,
                "rms_abs": float(np.sqrt(np.mean(np.square(magnitudes))))
                if selected.size
                else None,
            }
        )
    else:
        real = selected.astype(np.float64)
        receipt.update(
            {
                "min": float(np.min(real)) if selected.size else None,
                "max": float(np.max(real)) if selected.size else None,
                "rms": float(np.sqrt(np.mean(np.square(real))))
                if selected.size
                else None,
            }
        )
    return receipt


def close_tool(tool: Any) -> None:
    closer = getattr(tool, "done", None) or getattr(tool, "close", None)
    if closer is not None:
        closer()


def read_image(path: Path) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    tool = image_tool()
    coordinates = None
    try:
        if not tool.open(str(path)):
            raise RuntimeError(f"cannot open CASA image {path}")
        values = np.asarray(tool.getchunk(dropdeg=False), dtype=np.float32)
        mask = np.asarray(tool.getchunk(dropdeg=False, getmask=True), dtype=np.bool_)
        coordinates = tool.coordsys()
        coordinate_record = json_safe(coordinates.torecord())
        reference_value = json_safe(coordinates.referencevalue(format="n"))
        axis_types = [str(value) for value in coordinates.axiscoordinatetypes()]
        units = [str(value) for value in coordinates.units()]
        metadata = {
            "shape": [int(value) for value in tool.shape()],
            "brightness_unit": str(tool.brightnessunit()),
            "mask_names": json_safe(tool.maskhandler("get")),
            "axis_coordinate_types": axis_types,
            "axis_units": units,
            "reference_value": reference_value,
            "coordinate_record": coordinate_record,
            "restoring_beam": json_safe(tool.restoringbeam()),
        }
    finally:
        if coordinates is not None:
            close_tool(coordinates)
        tool.close()
    return values, mask, metadata


def scalar_column(
    tool: Any, name: str, dtype: Any, rows: np.ndarray | None = None
) -> np.ndarray:
    if rows is None:
        return np.asarray(tool.getcol(name), dtype=dtype).reshape(-1)
    return np.asarray(
        [tool.getcell(name, int(row)) for row in rows], dtype=dtype
    ).reshape(-1)


def row_cells(
    tool: Any, name: str, dtype: Any, rows: np.ndarray | None = None
) -> np.ndarray:
    selected = range(tool.nrows()) if rows is None else rows
    return np.stack(
        [np.asarray(tool.getcell(name, int(row)), dtype=dtype) for row in selected],
        axis=0,
    )


def read_ms_fixture(staged_ms: Path) -> tuple[dict[str, np.ndarray], dict[str, Any]]:
    main = table_tool()
    try:
        main.open(str(staged_ms), nomodify=True)
        physical_row_count = int(main.nrows())
        all_field_ids = scalar_column(main, "FIELD_ID", np.int32)
        all_data_description_ids = scalar_column(main, "DATA_DESC_ID", np.int32)
        selected_rows = np.flatnonzero(
            (all_field_ids == 0) & np.isin(all_data_description_ids, [0, 1])
        ).astype(np.int64)
        row_count = int(selected_rows.size)
        if row_count != 24:
            raise RuntimeError(
                f"expected exact CASA selection to contain 24 rows, found {row_count}"
            )
        arrays: dict[str, np.ndarray] = {
            "ms_row_id": selected_rows,
            "visibility_data_complex64": row_cells(
                main, "DATA", np.complex64, selected_rows
            ),
            "channel_flag_bool": row_cells(main, "FLAG", np.bool_, selected_rows),
            "row_flag_bool": scalar_column(
                main, "FLAG_ROW", np.bool_, selected_rows
            ),
            "input_weight_f32": row_cells(
                main, "WEIGHT", np.float32, selected_rows
            ),
            "uvw_m_f64": row_cells(main, "UVW", np.float64, selected_rows),
            "time_mjd_seconds_f64": scalar_column(
                main, "TIME", np.float64, selected_rows
            ),
            "time_centroid_mjd_seconds_f64": scalar_column(
                main, "TIME_CENTROID", np.float64, selected_rows
            ),
            "interval_seconds_f64": scalar_column(
                main, "INTERVAL", np.float64, selected_rows
            ),
            "exposure_seconds_f64": scalar_column(
                main, "EXPOSURE", np.float64, selected_rows
            ),
        }
        for column in (
            "FIELD_ID",
            "DATA_DESC_ID",
            "ANTENNA1",
            "ANTENNA2",
            "FEED1",
            "FEED2",
            "SCAN_NUMBER",
            "STATE_ID",
            "OBSERVATION_ID",
            "ARRAY_ID",
        ):
            arrays[f"main_{column.lower()}_i32"] = scalar_column(
                main, column, np.int32, selected_rows
            )
        main_columns = sorted(str(value) for value in main.colnames())
    finally:
        main.close()

    spectral_window = table_tool()
    try:
        spectral_window.open(str(staged_ms / "SPECTRAL_WINDOW"), nomodify=True)
        selected_spws = np.asarray([0, 1], dtype=np.int32)
        topo_frequencies = np.stack(
            [
                np.asarray(spectral_window.getcell("CHAN_FREQ", int(spw)), dtype=np.float64)
                for spw in selected_spws
            ]
        )
        channel_widths = np.stack(
            [
                np.asarray(spectral_window.getcell("CHAN_WIDTH", int(spw)), dtype=np.float64)
                for spw in selected_spws
            ]
        )
        arrays["selected_spw_id_i32"] = selected_spws
        arrays["frequency_topo_hz_f64"] = topo_frequencies
        arrays["channel_width_hz_f64"] = channel_widths
        spw_metadata = [
            {
                "spw_id": int(spw),
                "name": str(spectral_window.getcell("NAME", int(spw))),
                "num_channels": int(spectral_window.getcell("NUM_CHAN", int(spw))),
                "measurement_frequency_reference_code": int(
                    spectral_window.getcell("MEAS_FREQ_REF", int(spw))
                ),
                "reference_frequency_hz": float(
                    spectral_window.getcell("REF_FREQUENCY", int(spw))
                ),
                "total_bandwidth_hz": float(
                    spectral_window.getcell("TOTAL_BANDWIDTH", int(spw))
                ),
            }
            for spw in selected_spws
        ]
    finally:
        spectral_window.close()

    data_description = table_tool()
    try:
        data_description.open(str(staged_ms / "DATA_DESCRIPTION"), nomodify=True)
        arrays["data_description_spw_id_i32"] = scalar_column(
            data_description, "SPECTRAL_WINDOW_ID", np.int32
        )
        arrays["data_description_polarization_id_i32"] = scalar_column(
            data_description, "POLARIZATION_ID", np.int32
        )
    finally:
        data_description.close()

    polarization = table_tool()
    try:
        polarization.open(str(staged_ms / "POLARIZATION"), nomodify=True)
        arrays["correlation_type_code_i32"] = np.asarray(
            polarization.getcell("CORR_TYPE", 0), dtype=np.int32
        )
        arrays["correlation_product_i32"] = np.asarray(
            polarization.getcell("CORR_PRODUCT", 0), dtype=np.int32
        )
    finally:
        polarization.close()

    field = table_tool()
    try:
        field.open(str(staged_ms / "FIELD"), nomodify=True)
        arrays["field_phase_direction_rad_f64"] = np.asarray(
            field.getcell("PHASE_DIR", 0), dtype=np.float64
        )
        field_name = str(field.getcell("NAME", 0))
        field_reference = json_safe(field.getcolkeyword("PHASE_DIR", "MEASINFO"))
    finally:
        field.close()

    # The MT-MFS C++ owner obtains vb.getFrequencies(0), narrows each frequency
    # to Float, evaluates (float(freq)-double(reffreq))/double(reffreq), and
    # narrows the result to Float.  Capture the LSRK frequency grid at every
    # selected row's epoch so the oracle proves whether Float bits vary in time.
    ms = ms_tool()
    qa = quanta_tool()
    try:
        ms.open(str(staged_ms), nomodify=True)
        row_spw = arrays["data_description_spw_id_i32"][arrays["main_data_desc_id_i32"]]
        lsrk_frequencies = np.empty(
            (row_count, topo_frequencies.shape[1]), dtype=np.float64
        )
        obstime_strings: list[str] = []
        for row in range(row_count):
            obstime = qa.time(
                qa.quantity(float(arrays["time_mjd_seconds_f64"][row]), "s"),
                form="ymd",
                prec=12,
            )[0]
            obstime_strings.append(str(obstime))
            lsrk_frequencies[row] = np.asarray(
                ms.cvelfreqs(
                    spwids=[int(row_spw[row])],
                    fieldids=[int(arrays["main_field_id_i32"][row])],
                    obstime=str(obstime),
                    mode="channel",
                    nchan=int(topo_frequencies.shape[1]),
                    start=0,
                    width=1,
                    phasec=int(arrays["main_field_id_i32"][row]),
                    outframe="LSRK",
                    veltype="radio",
                    verbose=True,
                ),
                dtype=np.float64,
            )
    finally:
        ms.close()
        close_tool(qa)
    arrays["frequency_lsrk_by_row_hz_f64"] = lsrk_frequencies

    metadata = {
        "row_count": row_count,
        "physical_row_count": physical_row_count,
        "selected_physical_row_ids": [int(value) for value in selected_rows],
        "selection_rule": "FIELD_ID == 0 and DATA_DESC_ID in [0,1], matching CASA spw='0~1:0~15'",
        "main_columns": main_columns,
        "spectral_windows": spw_metadata,
        "field_name": field_name,
        "field_phase_direction_reference": field_reference,
        "obstime_strings_by_row": obstime_strings,
    }
    return arrays, metadata


def exact_reference_frequency(metadata: dict[str, Any]) -> float:
    axis_types = metadata["axis_coordinate_types"]
    units = metadata["axis_units"]
    numeric = metadata["reference_value"]["numeric"]
    spectral_axis = axis_types.index("Spectral")
    if units[spectral_axis] != "Hz":
        raise RuntimeError(f"unexpected spectral coordinate unit {units[spectral_axis]!r}")
    return float(numeric[spectral_axis])


def main() -> None:
    args = parse_args()
    load_casa_modules()
    repo = args.repo_root.expanduser().resolve()
    testdata_root = resolve_testdata_root(repo, args.testdata_root)
    ms_source = testdata_root / DATASET_RELATIVE
    artifact_root = args.artifact_root.expanduser().resolve()
    artifact_root.mkdir(parents=True, exist_ok=True)
    temp_root = artifact_root / "tmp"
    temp_root.mkdir(exist_ok=True)
    raw_prefix = (
        args.casa_prefix.expanduser().resolve()
        if args.casa_prefix is not None
        else artifact_root / "raw-run-local" / "casa" / "casa"
    )
    npz_path = artifact_root / NPZ_NAME
    manifest_path = artifact_root / MANIFEST_NAME
    casa_log_source = args.casa_log.expanduser().resolve()
    casa_log_copy = artifact_root / casa_log_source.name
    runner = repo / "scripts" / "bench-imager-vs-casa.sh"

    for suffix in PRODUCT_SUFFIXES.values():
        if not Path(str(raw_prefix) + suffix).is_dir():
            raise RuntimeError(f"missing focused CASA oracle product {raw_prefix}{suffix}")
    if not casa_log_source.is_file():
        raise RuntimeError(f"missing successful CASA log {casa_log_source}")
    if not runner.is_file():
        raise RuntimeError(f"missing matched CASA runner {runner}")

    if casa_log_source != casa_log_copy:
        shutil.copy2(casa_log_source, casa_log_copy)

    product_values: dict[str, np.ndarray] = {}
    product_masks: dict[str, np.ndarray] = {}
    product_metadata: dict[str, Any] = {}
    for name, suffix in PRODUCT_SUFFIXES.items():
        values, mask, metadata = read_image(Path(str(raw_prefix) + suffix))
        product_values[name] = values
        product_masks[name] = mask
        product_metadata[name] = metadata

    reference_frequency_hz = exact_reference_frequency(product_metadata["psf_moment_0"])
    for name, metadata in product_metadata.items():
        if exact_reference_frequency(metadata).hex() != reference_frequency_hz.hex():
            raise RuntimeError(f"spectral reference frequency differs for {name}")

    psf = np.stack(
        [np.squeeze(product_values[f"psf_moment_{term}"]) for term in range(3)]
    ).astype(np.float32)
    psf_masks = np.stack(
        [np.squeeze(product_masks[f"psf_moment_{term}"]) for term in range(3)]
    ).astype(np.bool_)
    dirty = np.stack(
        [np.squeeze(product_values[f"dirty_taylor_{term}"]) for term in range(2)]
    ).astype(np.float32)
    dirty_masks = np.stack(
        [np.squeeze(product_masks[f"dirty_taylor_{term}"]) for term in range(2)]
    ).astype(np.bool_)
    sum_weights = np.asarray(
        [float(np.squeeze(product_values[f"sum_weight_{term}"])) for term in range(3)],
        dtype=np.float32,
    )
    sum_weight_masks = np.asarray(
        [bool(np.squeeze(product_masks[f"sum_weight_{term}"])) for term in range(3)],
        dtype=np.bool_,
    )
    valid_support = (
        np.all(psf_masks, axis=0)
        & np.all(dirty_masks, axis=0)
        & np.all(np.isfinite(psf), axis=0)
        & np.all(np.isfinite(dirty), axis=0)
    )

    with tempfile.TemporaryDirectory(prefix="t42-ms-stage-", dir=str(temp_root)) as td:
        staged_ms = Path(td) / "fixture.ms"
        shutil.copytree(ms_source, staged_ms)
        ms_arrays, ms_metadata = read_ms_fixture(staged_ms)

    lsrk = ms_arrays["frequency_lsrk_by_row_hz_f64"]
    frequency_as_float_then_double = lsrk.astype(np.float32).astype(np.float64)
    taylor_x = (
        (frequency_as_float_then_double - reference_frequency_hz)
        / reference_frequency_hz
    ).astype(np.float32)
    taylor_basis = np.stack([np.ones_like(taylor_x), taylor_x]).astype(np.float32)
    taylor_basis_bits = taylor_basis.view(np.uint32)
    basis_bits_by_spw_channel: dict[str, list[int]] = {}
    basis_variant_counts_by_spw_channel: dict[str, list[int]] = {}
    row_spw = ms_arrays["data_description_spw_id_i32"][ms_arrays["main_data_desc_id_i32"]]
    for spw in ms_arrays["selected_spw_id_i32"]:
        rows = np.flatnonzero(row_spw == spw)
        if rows.size == 0:
            raise RuntimeError(f"no selected rows for SPW {int(spw)}")
        basis_bits_by_spw_channel[str(int(spw))] = [
            int(value) for value in taylor_basis_bits[1, rows[0]]
        ]
        basis_variant_counts_by_spw_channel[str(int(spw))] = [
            int(np.unique(taylor_basis_bits[1, rows, channel]).size)
            for channel in range(taylor_basis_bits.shape[2])
        ]

    npz_arrays: dict[str, np.ndarray] = {
        **ms_arrays,
        "reference_frequency_hz_f64": np.asarray(reference_frequency_hz, dtype=np.float64),
        "reference_frequency_f64_bits": np.asarray(
            reference_frequency_hz, dtype=np.float64
        ).view(np.uint64),
        "taylor_basis_f32": taylor_basis,
        "taylor_basis_f32_bits": taylor_basis_bits,
        "psf_moments_normalized_f32": psf,
        "psf_masks_bool": psf_masks,
        "dirty_taylor_normalized_f32": dirty,
        "dirty_masks_bool": dirty_masks,
        "sum_weights_f32": sum_weights,
        "sum_weight_masks_bool": sum_weight_masks,
        "valid_support_bool": valid_support,
    }
    np.savez_compressed(npz_path, **npz_arrays)

    raw_product_root = raw_prefix.parent
    product_tree_sha256, product_file_count, product_byte_count = sha256_tree(
        raw_product_root, excluded_names={"table.lock"}
    )
    ms_tree_sha256, ms_file_count, ms_byte_count = sha256_tree(
        ms_source, excluded_names={"table.lock"}
    )

    support_count = int(np.count_nonzero(valid_support))
    psf0_supported = psf[0][valid_support]
    psf0_peak = float(np.max(psf0_supported))
    psf0_peak_flat = int(np.flatnonzero(valid_support & (psf[0] == psf0_peak))[0])
    psf0_peak_xy = list(np.unravel_index(psf0_peak_flat, valid_support.shape))

    comparator = {
        "basis": {
            "formula": "x=f32((f64(f32(frequency_lsrk_hz))-reference_frequency_hz)/reference_frequency_hz); basis=[f32(1),x]",
            "comparison": "term 0 and each implementation's own formula are exact; CASA versus casa-rs term 1 uses the common normalized-RMS ceiling of 0.001",
        },
        "normal_block_mapping": {
            "H[0,0]": "P0",
            "H[0,1]": "P1",
            "H[1,0]": "P1",
            "H[1,1]": "P2",
        },
        "valid_support": {
            "formula": "logical AND of CASA psf.tt0/1/2 and residual.tt0/1 masks and finite payloads",
            "array": "valid_support_bool",
            "required_candidate_rule": "compare only where both candidate support and CASA valid_support_bool are true; report support disagreement separately",
        },
        "normalized_rms": {
            "formula": "sqrt(mean((candidate-reference)^2 on shared valid support))/max(sqrt(mean(reference^2 on shared valid support)), reference_scale*1e-7)",
            "ceiling": 0.001,
            "reference_scale": {
                "psf_moments": "positive peak of CASA P0",
                "dirty_taylor_terms": "positive absolute peak of CASA D0",
            },
            "reason_for_floor": "avoid an unstable relative denominator for symmetry-cancelled odd Taylor terms while retaining a scale-bound absolute test",
        },
        "sum_weights": {
            "formula": "abs(candidate-reference)/max(abs(reference),abs(W0)*1e-7)",
            "ceiling": 0.001,
        },
    }

    testdata_head = git_value(testdata_root, "rev-parse", "HEAD")
    dataset_tree = git_value(
        testdata_root,
        "rev-parse",
        f"HEAD:{DATASET_RELATIVE.as_posix()}",
    )
    dataset_last_change = git_value(
        testdata_root,
        "log",
        "-1",
        "--format=%H",
        "--",
        DATASET_RELATIVE.as_posix(),
    )
    recipe_path = Path(__file__).resolve()
    recipe_relative = recipe_path.relative_to(repo).as_posix()
    raw_prefix_relative = (
        raw_prefix.relative_to(artifact_root).as_posix()
        if raw_prefix.is_relative_to(artifact_root)
        else "<external-casa-product-prefix>"
    )
    timings = {
        name: value
        for name, value in {
            "tclean_wall_seconds": args.tclean_wall_seconds,
            "runner_wall_seconds": args.runner_wall_seconds,
        }.items()
        if value is not None
    }

    manifest = {
        "schema": "casa-rs-t42-casa-mtmfs-oracle-v1",
        "role": "focused_small_multi_spw_casa_correctness_oracle_not_performance_evidence",
        "issue": 528,
        "ticket": "T42",
        "status": "complete",
        "created_utc": args.created_utc,
        "casa": {
            "casatasks_version": casatasks.version_string(),
            "casatools_version": casatools.version_string(),
            "python": sys.version,
            "python_executable_name": Path(sys.executable).name,
            "platform": platform.platform(),
            "successful_log_file": casa_log_copy.name,
            "successful_log_sha256": sha256_file(casa_log_copy),
            "timings": timings,
        },
        "repository": {
            "head": git_value(repo, "rev-parse", "HEAD"),
            "runner": runner.relative_to(repo).as_posix(),
            "runner_sha256": sha256_file(runner),
            "recipe": recipe_relative,
            "recipe_sha256": sha256_file(recipe_path),
            "casa_source_normalization": {
                "multi_term": "CASA/casatools/src/code/synthesis/ImagerObjects/SIImageStoreMultiTerm.cc:845,891",
                "common": "CASA/casatools/src/code/synthesis/ImagerObjects/SIImageStore.cc:2923,2971",
                "taylor_weighting": "CASA/casatools/src/code/synthesis/TransformMachines2/MultiTermFTNew.cc:217",
            },
        },
        "run": {
            "preflight": {
                "command": "cargo run -q -p casa-test-support --bin casatestdata-preflight -- --tier slow-parity --require measurementset/vla/ref_vlass_wtsp_creation.ms",
                "resolver": "CASA_RS_TESTDATA_ROOT",
            },
            "runner": "scripts/bench-imager-vs-casa.sh",
            "runner_environment": {
                "BENCH_REPEATS": "1",
                "IMAGER_BENCH_SKIP_RUST": "1",
                "IMAGER_BENCH_SKIP_PROFILE": "1",
                "IMAGER_BENCH_KEEP_OUTPUT_ROOT": "<ARTIFACT_ROOT>/raw-run-local",
                "IMAGER_BENCH_FIELD": "0",
                "IMAGER_BENCH_PHASECENTER_FIELD": "0",
                "IMAGER_BENCH_SPW": "0~1",
                "IMAGER_BENCH_CHANNEL_START": "0",
                "IMAGER_BENCH_CHANNEL_COUNT": "16",
                "IMAGER_BENCH_SPECMODE": "mfs",
                "IMAGER_BENCH_GRIDDER": "standard",
                "IMAGER_BENCH_CASA_GRIDDER": "standard",
                "IMAGER_BENCH_WEIGHTING": "natural",
                "IMAGER_BENCH_DECONVOLVER": "mtmfs",
                "IMAGER_BENCH_NTERMS": "2",
                "IMAGER_BENCH_MODE": "dirty",
                "IMAGER_BENCH_NITER": "0",
                "IMAGER_BENCH_IMSIZE": "128",
                "IMAGER_BENCH_CELL_ARCSEC": "2.5",
                "IMAGER_BENCH_MS_STAGING": "copy",
                "IMAGER_BENCH_TMP_ROOT": "<ARTIFACT_ROOT>/tmp",
            },
            "runner_argument": "<CASA_RS_TESTDATA_ROOT>/measurementset/vla/ref_vlass_wtsp_creation.ms",
            "parameters": {
                "field": "0",
                "phasecenter_field": 0,
                "spw": "0~1:0~15",
                "datacolumn": "data",
                "stokes": "I",
                "specmode": "mfs",
                "outframe": "LSRK",
                "gridder": "standard",
                "weighting": "natural",
                "perchanweightdensity": False,
                "deconvolver": "mtmfs",
                "nterms": 2,
                "niter": 0,
                "imsize": [128, 128],
                "cell_arcsec": [2.5, 2.5],
                "pblimit": 0.2,
                "normtype": "flatnoise",
                "parallel": False,
            },
        },
        "input_fixture": {
            "resolver": "CASA_RS_TESTDATA_ROOT",
            "relative_path": DATASET_RELATIVE.as_posix(),
            "casatestdata_head": testdata_head,
            "dataset_git_tree": dataset_tree,
            "dataset_last_change_commit": dataset_last_change,
            "tree_sha256_excluding_table_lock": ms_tree_sha256,
            "file_count_excluding_table_lock": ms_file_count,
            "bytes_excluding_table_lock": ms_byte_count,
            **ms_metadata,
        },
        "spectral": {
            "reference_frequency_hz": reference_frequency_hz,
            "reference_frequency_f64_bits": int(
                np.asarray(reference_frequency_hz, dtype=np.float64).view(np.uint64)
            ),
            "reference_frequency_hex": reference_frequency_hz.hex(),
            "input_frequency_frame": "TOPO",
            "imaging_frequency_frame": "LSRK",
            "taylor_basis_f32_bits_by_spw_channel": basis_bits_by_spw_channel,
            "taylor_basis_f32_variant_counts_by_spw_channel": basis_variant_counts_by_spw_channel,
            "taylor_basis_f32_values_by_spw_channel": {
                spw: [
                    float(np.asarray(bits, dtype=np.uint32).view(np.float32))
                    for bits in values
                ]
                for spw, values in basis_bits_by_spw_channel.items()
            },
        },
        "normalization": {
            "psf_moments": "CASA divides P0, P1, and P2 by the positive maximum of unnormalized P0, processing P2 to P0; persisted P0 peak is one",
            "dirty_taylor_terms": "CASA divides every residual/dirty Taylor term by sumwt.tt0; standard gridding has no additional spatial weight-image division",
            "sum_weights": "persisted raw Float moment sums W0, W1, W2",
            "observed_psf0_positive_peak": psf0_peak,
            "observed_psf0_peak_xy_casa_axis_order": psf0_peak_xy,
            "observed_psf_centres": [float(psf[term, *psf0_peak_xy]) for term in range(3)],
            "observed_sum_weight_ratios_to_w0": [
                float(value / sum_weights[0]) for value in sum_weights
            ],
        },
        "comparator": comparator,
        "oracle": {
            "npz_file": npz_path.name,
            "npz_sha256": sha256_file(npz_path),
            "arrays": {
                name: array_receipt(array)
                for name, array in sorted(npz_arrays.items())
            },
            "raw_casa_product_prefix": raw_prefix_relative,
            "raw_product_tree_sha256_excluding_table_lock": product_tree_sha256,
            "raw_product_file_count_excluding_table_lock": product_file_count,
            "raw_product_bytes_excluding_table_lock": product_byte_count,
            "products": {
                name: {
                    "suffix": suffix,
                    "metadata": product_metadata[name],
                    "values": array_receipt(product_values[name], product_masks[name]),
                    "mask": {
                        "shape": list(product_masks[name].shape),
                        "dtype": product_masks[name].dtype.str,
                        "bytes_sha256": bytes_sha256(product_masks[name]),
                        "true_count": int(np.count_nonzero(product_masks[name])),
                    },
                }
                for name, suffix in PRODUCT_SUFFIXES.items()
            },
            "valid_support": {
                "shape": list(valid_support.shape),
                "bytes_sha256": bytes_sha256(valid_support),
                "true_count": support_count,
                "false_count": int(valid_support.size - support_count),
            },
        },
    }
    manifest_path.write_text(
        json.dumps(json_safe(manifest), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "status": "complete",
                "manifest": str(manifest_path),
                "manifest_sha256": sha256_file(manifest_path),
                "npz": str(npz_path),
                "npz_sha256": sha256_file(npz_path),
                "reference_frequency_hz": reference_frequency_hz,
                "sum_weights": [float(value) for value in sum_weights],
                "valid_support": support_count,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
