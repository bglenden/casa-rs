"""Raw ImportVLA-provider request/result object API.

This module intentionally does not load profiles or update managed Last state.
Use :func:`casars.tasks.profiles.importvla` for the unified CASA-named
parameter lifecycle.
"""

from __future__ import annotations

from collections.abc import Sequence
import inspect
import os
from os import PathLike
from typing import Any, Literal, TypeAlias

from .._task_runtime import (
    ProtocolInfo,
    configure_importvla_binary,
    fetch_importvla_schema,
    get_importvla_protocol_info,
    invoke_importvla_task,
)
StrPath: TypeAlias = str | PathLike[str]

TaskResult: TypeAlias = dict[str, Any]
AntennaNameScheme: TypeAlias = Literal["new", "old"]


def configure(*, binary: StrPath | None) -> None:
    """Configure the default importvla binary override for this module."""

    configure_importvla_binary(binary)


def protocol_info(*, binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol information for the selected binary."""

    return get_importvla_protocol_info(binary=binary)


def schema(*, binary: StrPath | None = None) -> dict[str, Any]:
    """Return the Rust-emitted request/result schema bundle."""

    return fetch_importvla_schema(binary=binary)


def scan(
    archivefiles: Sequence[StrPath],
    *,
    vis: StrPath | None = None,
    bandname: str | None = None,
    frequencytol_hz: float = 150000.0,
    project: str | None = None,
    starttime: str | None = None,
    stoptime: str | None = None,
    applytsys: bool = True,
    autocorr: bool = False,
    antnamescheme: AntennaNameScheme = "new",
    keepblanks: bool = False,
    evlabands: bool = False,
    binary: StrPath | None = None,
) -> TaskResult:
    """Scan one or more VLA export archives without writing a MeasurementSet."""

    return invoke_importvla_task(
        kind="scan",
        request={"options": _options_request(
            archivefiles=archivefiles,
            vis=vis,
            bandname=bandname,
            frequencytol_hz=frequencytol_hz,
            project=project,
            starttime=starttime,
            stoptime=stoptime,
            applytsys=applytsys,
            autocorr=autocorr,
            antnamescheme=antnamescheme,
            keepblanks=keepblanks,
            evlabands=evlabands,
        )},
        binary=binary,
    )


def import_archive(
    archivefiles: Sequence[StrPath],
    vis: StrPath,
    *,
    bandname: str | None = None,
    frequencytol_hz: float = 150000.0,
    project: str | None = None,
    starttime: str | None = None,
    stoptime: str | None = None,
    applytsys: bool = True,
    autocorr: bool = False,
    antnamescheme: AntennaNameScheme = "new",
    keepblanks: bool = False,
    evlabands: bool = False,
    binary: StrPath | None = None,
) -> TaskResult:
    """Import one or more VLA export archives into a MeasurementSet."""

    return invoke_importvla_task(
        kind="import",
        request={"options": _options_request(
            archivefiles=archivefiles,
            vis=vis,
            bandname=bandname,
            frequencytol_hz=frequencytol_hz,
            project=project,
            starttime=starttime,
            stoptime=stoptime,
            applytsys=applytsys,
            autocorr=autocorr,
            antnamescheme=antnamescheme,
            keepblanks=keepblanks,
            evlabands=evlabands,
        )},
        binary=binary,
    )


def validate_signature_parity(*, binary: StrPath | None = None) -> None:
    """Fail if the Python wrapper metadata drifts from the Rust task schema."""

    definitions = schema(binary=binary)["request_schema"]["definitions"]
    options_properties = definitions["ImportVlaOptions"]["properties"]
    expected_option_fields = set(_OPTION_FIELD_NAMES)
    actual_option_fields = set(options_properties)
    if actual_option_fields != expected_option_fields:
        raise AssertionError(
            f"ImportVlaOptions fields drifted: expected {sorted(expected_option_fields)}, "
            f"got {sorted(actual_option_fields)}"
        )

    for function_name, contract in _WRAPPER_CONTRACTS.items():
        parameters = list(inspect.signature(globals()[function_name]).parameters.values())
        actual_names = [parameter.name for parameter in parameters]
        if actual_names != contract["signature"]:
            raise AssertionError(
                f"{function_name} signature drifted: expected {contract['signature']}, got {actual_names}"
            )
        for parameter in parameters:
            if parameter.name in contract["defaults"]:
                expected_default = contract["defaults"][parameter.name]
                if parameter.default != expected_default:
                    raise AssertionError(
                        f"{function_name}.{parameter.name} default drifted: "
                        f"expected {expected_default!r}, got {parameter.default!r}"
                    )


def _options_request(
    *,
    archivefiles: Sequence[StrPath],
    vis: StrPath | None,
    bandname: str | None,
    frequencytol_hz: float,
    project: str | None,
    starttime: str | None,
    stoptime: str | None,
    applytsys: bool,
    autocorr: bool,
    antnamescheme: AntennaNameScheme,
    keepblanks: bool,
    evlabands: bool,
) -> dict[str, Any]:
    return {
        "archivefiles": [os.fspath(path) for path in archivefiles],
        "vis": None if vis is None else os.fspath(vis),
        "bandname": _encode_bandname(bandname),
        "frequencytol_hz": float(frequencytol_hz),
        "project": project,
        "starttime": starttime,
        "stoptime": stoptime,
        "applytsys": applytsys,
        "autocorr": autocorr,
        "antnamescheme": _encode_antnamescheme(antnamescheme),
        "keepblanks": keepblanks,
        "evlabands": evlabands,
    }


def _encode_bandname(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    uppercase = normalized.upper()
    mapping = {
        "4": "Four",
        "P": "P",
        "L": "L",
        "S": "S",
        "C": "C",
        "X": "X",
        "U": "U",
        "K": "K",
        "KA": "Ka",
        "Q": "Q",
    }
    return mapping[uppercase]


def _encode_antnamescheme(value: AntennaNameScheme) -> str:
    return {"new": "New", "old": "Old"}[value]


_OPTION_FIELD_NAMES = [
    "archivefiles",
    "vis",
    "bandname",
    "frequencytol_hz",
    "project",
    "starttime",
    "stoptime",
    "applytsys",
    "autocorr",
    "antnamescheme",
    "keepblanks",
    "evlabands",
]


_WRAPPER_CONTRACTS: dict[str, dict[str, Any]] = {
    "scan": {
        "signature": [
            "archivefiles",
            "vis",
            "bandname",
            "frequencytol_hz",
            "project",
            "starttime",
            "stoptime",
            "applytsys",
            "autocorr",
            "antnamescheme",
            "keepblanks",
            "evlabands",
            "binary",
        ],
        "defaults": {
            "vis": None,
            "bandname": None,
            "frequencytol_hz": 150000.0,
            "project": None,
            "starttime": None,
            "stoptime": None,
            "applytsys": True,
            "autocorr": False,
            "antnamescheme": "new",
            "keepblanks": False,
            "evlabands": False,
            "binary": None,
        },
    },
    "import_archive": {
        "signature": [
            "archivefiles",
            "vis",
            "bandname",
            "frequencytol_hz",
            "project",
            "starttime",
            "stoptime",
            "applytsys",
            "autocorr",
            "antnamescheme",
            "keepblanks",
            "evlabands",
            "binary",
        ],
        "defaults": {
            "bandname": None,
            "frequencytol_hz": 150000.0,
            "project": None,
            "starttime": None,
            "stoptime": None,
            "applytsys": True,
            "autocorr": False,
            "antnamescheme": "new",
            "keepblanks": False,
            "evlabands": False,
            "binary": None,
        },
    },
}


__all__ = [
    "TaskResult",
    "configure",
    "protocol_info",
    "schema",
    "scan",
    "import_archive",
    "validate_signature_parity",
]
