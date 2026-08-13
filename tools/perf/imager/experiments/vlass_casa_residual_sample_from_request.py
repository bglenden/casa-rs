#!/usr/bin/env python3
"""Enter one retained CASA major cycle and stop at its residual-grid input."""

from __future__ import annotations

import argparse
import ctypes
import os
from pathlib import Path

from casatasks import casalog, tclean

from vlass_common_model_major_cycle import (
    IMMUTABLE_SUFFIXES,
    MODEL_SUFFIXES,
    effective_parameters_from_request,
    prefixed_directories,
)


READY_MAGIC = 0x4341534141575331


def require_interposer() -> None:
    output = os.environ.get("CASA_AW_DATATOGRID_SAMPLE_OUTPUT")
    inserted = os.environ.get("DYLD_INSERT_LIBRARIES", "")
    if not output or not Path(output).is_absolute():
        raise RuntimeError(
            "CASA_AW_DATATOGRID_SAMPLE_OUTPUT must name a new absolute receipt"
        )
    if Path(output).exists():
        raise RuntimeError(f"refusing to overwrite residual-sample receipt: {output}")
    if not inserted:
        raise RuntimeError("DYLD_INSERT_LIBRARIES is required")
    process = ctypes.CDLL(None)
    try:
        ready = process.casa_aw_datatogrid_sample_ready_v1
    except AttributeError as error:
        raise RuntimeError("the DataToGrid sample interposer is not resident") from error
    ready.argtypes = []
    ready.restype = ctypes.c_uint64
    if ready() != READY_MAGIC:
        raise RuntimeError("the DataToGrid sample interposer failed its ABI marker")


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
        "tclean returned normally; the fail-closed DataToGrid sample interposer "
        "was not reached"
    )


if __name__ == "__main__":
    main()
