#!/usr/bin/env python3
"""Capture a bounded CASA MT-MFS term-degrid boundary from a frozen request."""

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


READY_MAGIC = 0x434153414D544431


def require_interposer() -> None:
    binary = os.environ.get("CASA_MTMFS_TERM_DEGRID_BINARY")
    receipt = os.environ.get("CASA_MTMFS_TERM_DEGRID_RECEIPT")
    inserted = os.environ.get("DYLD_INSERT_LIBRARIES", "")
    for label, value in (("binary", binary), ("receipt", receipt)):
        if not value or not Path(value).is_absolute():
            raise RuntimeError(
                f"CASA_MTMFS_TERM_DEGRID_{label.upper()} must name a new "
                "absolute artifact"
            )
        if Path(value).exists():
            raise RuntimeError(f"refusing to overwrite term-degrid {label}: {value}")
    if not inserted:
        raise RuntimeError("DYLD_INSERT_LIBRARIES is required")
    process = ctypes.CDLL(None)
    try:
        ready = process.casa_mtmfs_term_degrid_oracle_ready_v1
    except AttributeError as error:
        raise RuntimeError(
            "the CASA MT-MFS term-degrid interposer is not resident"
        ) from error
    ready.argtypes = []
    ready.restype = ctypes.c_uint64
    if ready() != READY_MAGIC:
        raise RuntimeError(
            "the resident CASA MT-MFS term-degrid interposer failed its ABI marker"
        )


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
        raise RuntimeError(
            f"prepared CASA bundle is missing: {args.prepared_prefix}.*"
        )
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
        "tclean returned normally; the fail-closed MT-MFS term-degrid "
        "interposer was not reached"
    )


if __name__ == "__main__":
    main()
