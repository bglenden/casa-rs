#!/usr/bin/env python3
"""Enter a retained CASA major cycle and hash its first AW residual input block."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from casatasks import casalog, tclean

from vlass_common_model_major_cycle import (
    IMMUTABLE_SUFFIXES,
    MODEL_SUFFIXES,
    effective_parameters_from_request,
    prefixed_directories,
)


def require_interposer() -> None:
    output = os.environ.get("CASA_AW_INPUT_HASH_OUTPUT")
    values_output = os.environ.get("CASA_AW_INPUT_VALUES_OUTPUT")
    metadata_output = os.environ.get("CASA_AW_INPUT_METADATA_OUTPUT")
    prediction_metadata_output = os.environ.get(
        "CASA_AW_PREDICTION_METADATA_OUTPUT"
    )
    expected_nxy = os.environ.get("CASA_AW_INPUT_HASH_EXPECT_NXY")
    inserted = os.environ.get("DYLD_INSERT_LIBRARIES", "")
    if not output or not Path(output).is_absolute():
        raise RuntimeError(
            "CASA_AW_INPUT_HASH_OUTPUT must name a new absolute receipt"
        )
    if Path(output).exists():
        raise RuntimeError(f"refusing to overwrite DataToGrid hash receipt: {output}")
    if expected_nxy != "4096":
        raise RuntimeError("CASA_AW_INPUT_HASH_EXPECT_NXY must be exactly 4096")
    if not values_output or not os.path.isabs(values_output):
        raise RuntimeError(
            "CASA_AW_INPUT_VALUES_OUTPUT must name a new absolute value stream"
        )
    if os.path.exists(values_output):
        raise RuntimeError(
            f"refusing to overwrite CASA value stream: {values_output}"
        )
    if not metadata_output or not os.path.isabs(metadata_output):
        raise RuntimeError(
            "CASA_AW_INPUT_METADATA_OUTPUT must name a new absolute metadata stream"
        )
    if os.path.exists(metadata_output):
        raise RuntimeError(
            f"refusing to overwrite CASA metadata stream: {metadata_output}"
        )
    if not prediction_metadata_output or not os.path.isabs(
        prediction_metadata_output
    ):
        raise RuntimeError(
            "CASA_AW_PREDICTION_METADATA_OUTPUT must name a new absolute "
            "metadata stream"
        )
    if os.path.exists(prediction_metadata_output):
        raise RuntimeError(
            "refusing to overwrite CASA prediction metadata stream: "
            f"{prediction_metadata_output}"
        )
    if "casa_aw_datatogrid_input_hash_interpose" not in inserted:
        raise RuntimeError("the DataToGrid input-hash interposer is not inserted")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request-json", required=True, type=Path)
    parser.add_argument("--prepared-prefix", required=True, type=Path)
    parser.add_argument("--casa-log", required=True, type=Path)
    args = parser.parse_args()

    require_interposer()
    if args.casa_log.exists():
        raise RuntimeError(f"refusing to overwrite CASA log: {args.casa_log}")
    if not prefixed_directories(args.prepared_prefix):
        raise RuntimeError(f"prepared CASA bundle is missing: {args.prepared_prefix}.*")
    for suffix in MODEL_SUFFIXES + IMMUTABLE_SUFFIXES:
        path = Path(f"{args.prepared_prefix}{suffix}")
        if not path.is_dir():
            raise RuntimeError(f"prepared CASA product is missing: {path}")

    parameters = effective_parameters_from_request(args.request_json)
    parameters.update(
        {
            "imagename": str(args.prepared_prefix),
            "niter": 0,
            "cycleniter": 1,
            "nmajor": 0,
            "calcres": True,
            "calcpsf": False,
            "restoration": False,
            "restart": True,
            "savemodel": "none",
            "fullsummary": False,
        }
    )
    args.casa_log.parent.mkdir(parents=True, exist_ok=True)
    casalog.setlogfile(str(args.casa_log))
    casalog.filter("INFO")
    tclean(**parameters)
    raise RuntimeError(
        "tclean returned normally; the fail-closed DataToGrid hash interposer "
        "was not reached"
    )


if __name__ == "__main__":
    main()
