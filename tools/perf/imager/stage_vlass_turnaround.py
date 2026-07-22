#!/usr/bin/env python3
"""Stage a reduced, non-final VLASS AWProject turnaround fixture.

The fixture is derived from CASA's two-pointing EVLA AWProject regression MS.
It preserves the seed's two fields and POINTING table, expands it to four
S-band spectral windows, and repeats the MAIN rows in whole immutable blocks.
The resulting workload is useful for rapid correctness and profiling loops,
but it must never be reported as the frozen 12,150-pixel VLASS acceptance run.

Run this script with the CASA 6.7.5.18 Python executable.  CASA imports remain
lazy so the pure planning helpers can be tested by the normal host Python.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import sys
import time
import uuid
from typing import Any, Iterable

from perf_harness import atomic_write_json
from perf_harness.tree_identity import TreeIdentityError, tree_identity


TOOL_DIR = pathlib.Path(__file__).resolve().parent
DEFAULT_SOURCE_MS = pathlib.Path(
    "/Volumes/home/casatestdata/measurementset/evla/"
    "refim_mawproject_twopointings.ms"
)
DATASET_NAME = "vlass-turnaround.ms"
CF_CACHE_NAME = "vlass-turnaround.cf"
REFERENCE_NAME = "casa-reference"
RECEIPT_NAME = "turnaround-receipt.json"
DEFAULT_ROW_REPETITIONS = 8
DEFAULT_IMAGE_PIXELS = 1024
DEFAULT_CELL_ARCSEC = 10.0
DEFAULT_SPW_CENTERS_HZ = (2.2e9, 2.7e9, 3.2e9, 3.7e9)
CHANNEL_OFFSET_HZ = 1.5e8
EXPECTED_SEED_MAIN_ROWS = 13_608
EXPECTED_SEED_POINTING_ROWS = 4_536
EXPECTED_PRODUCTS = (
    ".alpha",
    ".alpha.error",
    ".image.tt0",
    ".image.tt1",
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


class StagingError(ValueError):
    """The requested turnaround fixture cannot be staged safely."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-ms", type=pathlib.Path, default=DEFAULT_SOURCE_MS)
    parser.add_argument("--output-root", type=pathlib.Path, required=True)
    parser.add_argument(
        "--row-repetitions", type=int, default=DEFAULT_ROW_REPETITIONS
    )
    parser.add_argument("--imsize", type=int, default=DEFAULT_IMAGE_PIXELS)
    parser.add_argument("--cell-arcsec", type=float, default=DEFAULT_CELL_ARCSEC)
    parser.add_argument(
        "--spw-centers-hz",
        default=",".join(str(value) for value in DEFAULT_SPW_CENTERS_HZ),
        help="comma-separated S-band center frequencies",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        receipt = stage_turnaround(
            source_ms=args.source_ms,
            output_root=args.output_root,
            row_repetitions=args.row_repetitions,
            imsize=args.imsize,
            cell_arcsec=args.cell_arcsec,
            spw_centers_hz=parse_spw_centers(args.spw_centers_hz),
        )
    except (OSError, RuntimeError, StagingError, TreeIdentityError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None
    print(receipt)


def stage_turnaround(
    *,
    source_ms: pathlib.Path,
    output_root: pathlib.Path,
    row_repetitions: int,
    imsize: int,
    cell_arcsec: float,
    spw_centers_hz: tuple[float, ...],
) -> pathlib.Path:
    validate_plan(
        row_repetitions=row_repetitions,
        imsize=imsize,
        cell_arcsec=cell_arcsec,
        spw_centers_hz=spw_centers_hz,
    )
    source_ms = source_ms.expanduser().resolve()
    output_root = output_root.expanduser().resolve()
    if not source_ms.is_dir():
        raise StagingError(f"source MeasurementSet does not exist: {source_ms}")
    if os.path.lexists(output_root):
        raise StagingError(f"output root already exists: {output_root}")

    output_root.parent.mkdir(parents=True, exist_ok=True)
    partial = output_root.with_name(
        f"{output_root.name}.{uuid.uuid4().hex}.partial"
    )
    partial.mkdir()
    started = time.monotonic()
    try:
        seed_ms = partial / "seed.ms"
        staged_ms = partial / DATASET_NAME
        shutil.copytree(source_ms, seed_ms)
        shutil.copytree(source_ms, staged_ms)

        seed_facts = inspect_seed(seed_ms)
        validate_seed_facts(seed_facts)
        expand_measurement_set(
            seed_ms=seed_ms,
            staged_ms=staged_ms,
            row_repetitions=row_repetitions,
            spw_centers_hz=spw_centers_hz,
        )
        shutil.rmtree(seed_ms)

        reference_prefix = partial / REFERENCE_NAME
        cf_cache = partial / CF_CACHE_NAME
        build_casa_reference(
            ms_path=staged_ms,
            reference_prefix=reference_prefix,
            cf_cache=cf_cache,
            imsize=imsize,
            cell_arcsec=cell_arcsec,
            spw_count=len(spw_centers_hz),
        )
        products = collect_product_inventory(reference_prefix)
        require_exact_product_inventory(products)

        from casatools import version_string

        receipt_value = {
            "schema_version": 1,
            "kind": "vlass_awproject_turnaround_fixture",
            "evidence_role": "reduced_turnaround_only_not_final_acceptance",
            "casa_version": version_string(),
            "source": {
                "path": str(source_ms),
                "seed_facts": seed_facts,
            },
            "fixture": {
                "dataset": DATASET_NAME,
                "cf_cache": CF_CACHE_NAME,
                "reference_prefix": REFERENCE_NAME,
                "row_repetitions": row_repetitions,
                "main_rows": EXPECTED_SEED_MAIN_ROWS * row_repetitions,
                "field_ids": [0, 1],
                "pointing_rows": EXPECTED_SEED_POINTING_ROWS,
                "spw_centers_hz": list(spw_centers_hz),
                "channel_frequencies_hz": [
                    list(channel_frequencies(center)) for center in spw_centers_hz
                ],
                "intent": "OBSERVE_TARGET#UNSPECIFIED",
                "imsize": imsize,
                "cell_arcsec": cell_arcsec,
                "wprojplanes": 32,
                "products": products,
            },
            "identities": {
                "dataset": stable_tree_identity(staged_ms),
                "cf_cache": stable_tree_identity(cf_cache),
                "reference_products": [
                    {
                        "suffix": suffix,
                        **stable_tree_identity(pathlib.Path(path)),
                    }
                    for suffix, path in product_paths(reference_prefix)
                ],
            },
            "publication": {
                "partial_root": str(partial),
                "final_root": str(output_root),
                "same_parent_atomic_promotion": True,
            },
            "elapsed_seconds": time.monotonic() - started,
        }
        atomic_write_json(partial / RECEIPT_NAME, receipt_value)
        os.replace(partial, output_root)
    except Exception as error:
        raise StagingError(
            f"turnaround staging failed; partial state retained at {partial}: {error}"
        ) from error
    return output_root / RECEIPT_NAME


def parse_spw_centers(value: str) -> tuple[float, ...]:
    try:
        centers = tuple(float(part.strip()) for part in value.split(","))
    except ValueError as error:
        raise StagingError("SPW centers must be comma-separated numbers") from error
    if not centers or any(not 2.0e9 <= center <= 4.0e9 for center in centers):
        raise StagingError("every SPW center must be within the 2-4 GHz S band")
    if tuple(sorted(centers)) != centers or len(set(centers)) != len(centers):
        raise StagingError("SPW centers must be unique and strictly increasing")
    return centers


def validate_plan(
    *,
    row_repetitions: int,
    imsize: int,
    cell_arcsec: float,
    spw_centers_hz: tuple[float, ...],
) -> None:
    parse_spw_centers(",".join(str(value) for value in spw_centers_hz))
    if isinstance(row_repetitions, bool) or row_repetitions < len(spw_centers_hz):
        raise StagingError("row repetitions must be at least the SPW count")
    if row_repetitions % len(spw_centers_hz) != 0:
        raise StagingError("row repetitions must be an exact multiple of the SPW count")
    if isinstance(imsize, bool) or imsize < 64:
        raise StagingError("imsize must be an integer >= 64")
    if not 0.0 < cell_arcsec <= 60.0:
        raise StagingError("cell arcseconds must be in (0, 60]")


def channel_frequencies(center_hz: float) -> tuple[float, float, float]:
    return (
        center_hz - CHANNEL_OFFSET_HZ,
        center_hz,
        center_hz + CHANNEL_OFFSET_HZ,
    )


def inspect_seed(ms_path: pathlib.Path) -> dict[str, Any]:
    from casatools import table

    main = table()
    main.open(str(ms_path))
    try:
        main_rows = int(main.nrows())
        field_ids = sorted(int(value) for value in set(main.getcol("FIELD_ID")))
        data_desc_ids = sorted(
            int(value) for value in set(main.getcol("DATA_DESC_ID"))
        )
        data_shape = list(main.getcell("DATA", 0).shape)
    finally:
        main.close()

    def table_rows(name: str) -> int:
        tool = table()
        tool.open(str(ms_path / name))
        try:
            return int(tool.nrows())
        finally:
            tool.close()

    spw = table()
    spw.open(str(ms_path / "SPECTRAL_WINDOW"))
    try:
        spw_rows = int(spw.nrows())
        channel_counts = [int(value) for value in spw.getcol("NUM_CHAN")]
    finally:
        spw.close()

    return {
        "main_rows": main_rows,
        "field_ids": field_ids,
        "data_desc_ids": data_desc_ids,
        "data_shape": data_shape,
        "spw_rows": spw_rows,
        "channel_counts": channel_counts,
        "data_description_rows": table_rows("DATA_DESCRIPTION"),
        "pointing_rows": table_rows("POINTING"),
        "state_rows": table_rows("STATE"),
        "source_rows": table_rows("SOURCE"),
    }


def validate_seed_facts(facts: dict[str, Any]) -> None:
    expected = {
        "main_rows": EXPECTED_SEED_MAIN_ROWS,
        "field_ids": [0, 1],
        "data_desc_ids": [0],
        "data_shape": [2, 3],
        "spw_rows": 1,
        "channel_counts": [3],
        "data_description_rows": 1,
        "pointing_rows": EXPECTED_SEED_POINTING_ROWS,
        "state_rows": 1,
        "source_rows": 2,
    }
    if facts != expected:
        raise StagingError(
            "source MS does not match the frozen CASA two-pointing seed: "
            f"expected={expected}, observed={facts}"
        )


def expand_measurement_set(
    *,
    seed_ms: pathlib.Path,
    staged_ms: pathlib.Path,
    row_repetitions: int,
    spw_centers_hz: tuple[float, ...],
) -> None:
    from casatools import table
    import numpy as np

    spw_count = len(spw_centers_hz)
    clone_rows(
        seed_ms / "SPECTRAL_WINDOW",
        staged_ms / "SPECTRAL_WINDOW",
        repetitions=spw_count,
    )
    clone_rows(
        seed_ms / "DATA_DESCRIPTION",
        staged_ms / "DATA_DESCRIPTION",
        repetitions=spw_count,
    )
    clone_rows(
        seed_ms / "SOURCE",
        staged_ms / "SOURCE",
        repetitions=spw_count,
    )
    clone_rows(seed_ms, staged_ms, repetitions=row_repetitions)

    spw = table()
    spw.open(str(staged_ms / "SPECTRAL_WINDOW"), nomodify=False)
    try:
        for index, center_hz in enumerate(spw_centers_hz):
            frequencies = np.asarray(channel_frequencies(center_hz), dtype=float)
            widths = np.full(3, CHANNEL_OFFSET_HZ, dtype=float)
            spw.putcell("MEAS_FREQ_REF", index, 5)
            spw.putcell("CHAN_FREQ", index, frequencies)
            spw.putcell("REF_FREQUENCY", index, float(frequencies[0]))
            spw.putcell("CHAN_WIDTH", index, widths)
            spw.putcell("EFFECTIVE_BW", index, widths)
            spw.putcell("RESOLUTION", index, widths)
            spw.putcell("NAME", index, f"VLASS_TURNAROUND_SPW{index}")
            spw.putcell("NUM_CHAN", index, 3)
            spw.putcell("TOTAL_BANDWIDTH", index, 3.0 * CHANNEL_OFFSET_HZ)
    finally:
        spw.close()

    description = table()
    description.open(str(staged_ms / "DATA_DESCRIPTION"), nomodify=False)
    try:
        description.putcol(
            "SPECTRAL_WINDOW_ID", np.arange(spw_count, dtype=np.int32)
        )
        description.putcol("POLARIZATION_ID", np.zeros(spw_count, dtype=np.int32))
    finally:
        description.close()

    source = table()
    source.open(str(staged_ms / "SOURCE"), nomodify=False)
    try:
        rows_per_spw = 2
        for spw_id, center_hz in enumerate(spw_centers_hz):
            for row in range(spw_id * rows_per_spw, (spw_id + 1) * rows_per_spw):
                source.putcell("SPECTRAL_WINDOW_ID", row, spw_id)
                source.putcell(
                    "REST_FREQUENCY",
                    row,
                    np.asarray([channel_frequencies(center_hz)[0]], dtype=float),
                )
    finally:
        source.close()

    state = table()
    state.open(str(staged_ms / "STATE"), nomodify=False)
    try:
        state.putcell("OBS_MODE", 0, "OBSERVE_TARGET#UNSPECIFIED")
    finally:
        state.close()

    main = table()
    main.open(str(staged_ms), nomodify=False)
    try:
        for repetition in range(row_repetitions):
            main.putcol(
                "DATA_DESC_ID",
                np.full(
                    EXPECTED_SEED_MAIN_ROWS,
                    repetition % spw_count,
                    dtype=np.int32,
                ),
                startrow=repetition * EXPECTED_SEED_MAIN_ROWS,
                nrow=EXPECTED_SEED_MAIN_ROWS,
            )
    finally:
        main.close()


def clone_rows(
    source_path: pathlib.Path, target_path: pathlib.Path, *, repetitions: int
) -> None:
    from casatools import table

    if repetitions < 1:
        raise StagingError("table clone repetitions must be >= 1")
    source = table()
    source.open(str(source_path))
    try:
        source_rows = int(source.nrows())
        for _ in range(repetitions - 1):
            if not source.copyrows(
                str(target_path), startrowin=0, startrowout=-1, nrow=source_rows
            ):
                raise StagingError(f"CASA could not clone rows into {target_path}")
    finally:
        source.close()


def build_casa_reference(
    *,
    ms_path: pathlib.Path,
    reference_prefix: pathlib.Path,
    cf_cache: pathlib.Path,
    imsize: int,
    cell_arcsec: float,
    spw_count: int,
) -> None:
    from casatasks import tclean

    tclean(
        vis=str(ms_path),
        imagename=str(reference_prefix),
        field="0,1",
        phasecenter=0,
        spw=f"0~{spw_count - 1}:0~2",
        datacolumn="data",
        uvrange="<12km",
        intent="OBSERVE_TARGET#UNSPECIFIED",
        imsize=[imsize, imsize],
        cell=[f"{cell_arcsec}arcsec", f"{cell_arcsec}arcsec"],
        stokes="I",
        projection="SIN",
        specmode="mfs",
        interpolation="linear",
        gridder="awproject",
        cfcache=str(cf_cache),
        wprojplanes=32,
        facets=1,
        psfphasecenter="",
        vptable="",
        aterm=True,
        psterm=False,
        wbawp=True,
        conjbeams=True,
        usepointing=True,
        computepastep=360.0,
        rotatepastep=360.0,
        pointingoffsetsigdev=0.0,
        mosweight=False,
        normtype="flatnoise",
        weighting="briggs",
        robust=1.0,
        perchanweightdensity=True,
        deconvolver="mtmfs",
        nterms=2,
        scales=[0, 5, 12],
        smallscalebias=0.0,
        niter=0,
        gain=0.1,
        threshold="0Jy",
        nsigma=5.0,
        cycleniter=2000,
        cyclefactor=3.0,
        minpsffraction=0.05,
        maxpsffraction=0.8,
        pblimit=0.0001,
        pbcor=False,
        restoration=True,
        restoringbeam="common",
        interactive=False,
        usemask="user",
        restart=False,
        savemodel="none",
        calcres=True,
        calcpsf=True,
        parallel=False,
    )


def collect_product_inventory(reference_prefix: pathlib.Path) -> list[str]:
    return [suffix for suffix, _path in product_paths(reference_prefix)]


def product_paths(reference_prefix: pathlib.Path) -> list[tuple[str, pathlib.Path]]:
    prefix = f"{reference_prefix.name}."
    return sorted(
        (
            (f".{path.name.removeprefix(prefix)}", path)
            for path in reference_prefix.parent.iterdir()
            if path.is_dir() and path.name.startswith(prefix)
        ),
        key=lambda item: item[0],
    )


def require_exact_product_inventory(products: Iterable[str]) -> None:
    observed = tuple(sorted(products))
    expected = tuple(sorted(EXPECTED_PRODUCTS))
    if observed != expected:
        raise StagingError(
            "CASA reference product inventory drifted: "
            f"expected={expected}, observed={observed}"
        )


def stable_tree_identity(path: pathlib.Path) -> dict[str, Any]:
    return tree_identity(path, excluded_names={"table.lock"})


if __name__ == "__main__":
    main()
