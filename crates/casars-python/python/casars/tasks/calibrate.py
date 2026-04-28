"""Calibration task wrappers backed by the canonical Rust JSON contract."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
import inspect
import os
from os import PathLike
from typing import Any, Literal, TypeAlias

from .._task_runtime import (
    ProtocolInfo,
    configure_calibrate_binary,
    fetch_calibration_schema,
    get_protocol_info,
    invoke_calibration_task,
)
StrPath: TypeAlias = str | PathLike[str]

TaskResult: TypeAlias = dict[str, Any]
ApplyMode: TypeAlias = Literal["calflag", "calonly", "trial"]
InterpolationMode: TypeAlias = Literal["nearest", "linear", "nearest,linear"]
GainType: TypeAlias = Literal["g", "t"]
GainSolveMode: TypeAlias = Literal["p", "ap"]
GainSolveModelSource: TypeAlias = Literal["point", "point_source", "model_column"]
GainSolveInterval: TypeAlias = Literal["inf", "int"] | float
BandpassType: TypeAlias = Literal["b", "bpoly"]
GainFieldValue: TypeAlias = int | str | Literal["nearest"]
ReferenceAntenna: TypeAlias = int | str
StatsAxis: TypeAlias = Literal["amp", "phase", "real", "imag"] | str


@dataclass(frozen=True, slots=True)
class Selection:
    """Structured MeasurementSet selection shared across calibration stages."""

    selectdata: bool = True
    field: str | None = None
    spw: str | None = None
    timerange: str | None = None
    uvrange: str | None = None
    antenna: str | None = None
    scan: str | None = None
    correlation: str | None = None
    array: str | None = None
    observation: str | None = None
    intent: str | None = None
    feed: str | None = None
    msselect: str | None = None

    def to_request(self) -> dict[str, Any]:
        return {
            "selectdata": self.selectdata,
            "field": self.field,
            "spw": self.spw,
            "timerange": self.timerange,
            "uvrange": self.uvrange,
            "antenna": self.antenna,
            "scan": self.scan,
            "correlation": self.correlation,
            "array": self.array,
            "observation": self.observation,
            "intent": self.intent,
            "feed": self.feed,
            "msselect": self.msselect,
        }


@dataclass(frozen=True, slots=True)
class ApplyTableSelection:
    """Optional applicability filter for one calibration table in an apply chain."""

    field_ids: Sequence[int] = ()
    spectral_window_ids: Sequence[int] = ()
    observation_ids: Sequence[int] = ()

    def to_request(self) -> dict[str, Any]:
        return {
            "field_ids": list(self.field_ids),
            "spectral_window_ids": list(self.spectral_window_ids),
            "observation_ids": list(self.observation_ids),
        }


@dataclass(frozen=True, slots=True)
class SolveCombine:
    """Scan/field combine flags reused by gain and bandpass solves."""

    scans: bool = False
    fields: bool = False

    def to_request(self) -> dict[str, bool]:
        return {"scans": self.scans, "fields": self.fields}


@dataclass(frozen=True, slots=True)
class CalibrationTableSpec:
    """One calibration table in an apply or solve-preapply chain."""

    path: StrPath
    apply_to: ApplyTableSelection = field(default_factory=ApplyTableSelection)
    gainfield: GainFieldValue | None = None
    spwmap: Sequence[int] = ()
    interp: InterpolationMode = "nearest"
    calwt: bool = False

    def to_request(self) -> dict[str, Any]:
        return {
            "path": os.fspath(self.path),
            "apply_to": self.apply_to.to_request(),
            "gainfield": _encode_gainfield(self.gainfield),
            "spwmap": list(self.spwmap),
            "interp": _encode_interp(self.interp),
            "calwt": self.calwt,
        }


def configure(*, binary: StrPath | None) -> None:
    """Configure the default calibrate binary override for this module."""

    configure_calibrate_binary(binary)


def protocol_info(*, binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol information for the selected binary."""

    return get_protocol_info(binary=binary)


def schema(*, binary: StrPath | None = None) -> dict[str, Any]:
    """Return the Rust-emitted request/result schema bundle."""

    return fetch_calibration_schema(binary=binary)


def summary(paths: Sequence[StrPath], *, binary: StrPath | None = None) -> TaskResult:
    """Summarize one or more calibration tables."""

    return invoke_calibration_task(
        kind="summary",
        request={"paths": [os.fspath(path) for path in paths]},
        binary=binary,
    )


def stats(
    path: StrPath,
    *,
    axis: StatsAxis = "amp",
    datacolumn: str | None = None,
    use_flags: bool = False,
    binary: StrPath | None = None,
) -> TaskResult:
    """Compute ``calstat``-style statistics for one calibration table."""

    return invoke_calibration_task(
        kind="stats",
        request={
            "path": os.fspath(path),
            "axis": _encode_stats_axis(axis),
            "datacolumn": datacolumn,
            "use_flags": use_flags,
        },
        binary=binary,
    )


def plan_apply(
    measurement_set: StrPath,
    calibration_tables: Sequence[CalibrationTableSpec | StrPath],
    *,
    selection: Selection | None = None,
    parang: bool = False,
    binary: StrPath | None = None,
) -> TaskResult:
    """Build an ``applycal`` plan without mutating the MeasurementSet."""

    return invoke_calibration_task(
        kind="plan_apply",
        request={
            "measurement_set": os.fspath(measurement_set),
            "selection": _selection_request(selection),
            "calibration_tables": _table_specs_request(calibration_tables),
            "parang": parang,
        },
        binary=binary,
    )


def execute_apply(
    measurement_set: StrPath,
    calibration_tables: Sequence[CalibrationTableSpec | StrPath],
    *,
    apply_mode: ApplyMode = "calflag",
    selection: Selection | None = None,
    parang: bool = False,
    binary: StrPath | None = None,
) -> TaskResult:
    """Execute an ``applycal``-style calibration chain."""

    return invoke_calibration_task(
        kind="execute_apply",
        request={
            "measurement_set": os.fspath(measurement_set),
            "selection": _selection_request(selection),
            "calibration_tables": _table_specs_request(calibration_tables),
            "apply_mode": _encode_apply_mode(apply_mode),
            "parang": parang,
        },
        binary=binary,
    )


def export_corrected_data(
    input_ms: StrPath,
    output_ms: StrPath,
    *,
    selection: Selection | None = None,
    binary: StrPath | None = None,
) -> TaskResult:
    """Create an imaging-ready MS with ``CORRECTED_DATA`` copied into ``DATA``."""

    return invoke_calibration_task(
        kind="export_corrected_data",
        request={
            "input_ms": os.fspath(input_ms),
            "output_ms": os.fspath(output_ms),
            "selection": _selection_request(selection),
        },
        binary=binary,
    )


def solve_gain(
    measurement_set: StrPath,
    output_table: StrPath,
    *,
    refant: ReferenceAntenna,
    selection: Selection | None = None,
    gain_type: GainType = "g",
    solve_mode: GainSolveMode = "p",
    solve_interval: GainSolveInterval = "inf",
    combine: SolveCombine = SolveCombine(),
    prior_calibration_tables: Sequence[CalibrationTableSpec | StrPath] = (),
    parang: bool = False,
    model_source: GainSolveModelSource = "point",
    normalize_average_amplitude: bool = False,
    min_snr: float = 3.0,
    smodel: Sequence[float] = (1.0, 0.0, 0.0, 0.0),
    binary: StrPath | None = None,
) -> TaskResult:
    """Run the first-wave ``gaincal`` solver."""

    return invoke_calibration_task(
        kind="solve_gain",
        request={
            "measurement_set": os.fspath(measurement_set),
            "selection": _selection_request(selection),
            "output_table": os.fspath(output_table),
            "gain_type": _encode_gain_type(gain_type),
            "solve_mode": _encode_gain_solve_mode(solve_mode),
            "solve_interval": _encode_gain_solve_interval(solve_interval),
            "combine": combine.to_request(),
            "refant": _encode_refant(refant),
            "prior_calibration_tables": _table_specs_request(prior_calibration_tables),
            "parang": parang,
            "model_source": _encode_gain_solve_model_source(model_source),
            "normalize_average_amplitude": normalize_average_amplitude,
            "min_snr": float(min_snr),
            "smodel": _encode_smodel(smodel),
        },
        binary=binary,
    )


def solve_bandpass(
    measurement_set: StrPath,
    output_table: StrPath,
    *,
    refant: ReferenceAntenna,
    selection: Selection | None = None,
    prior_calibration_tables: Sequence[CalibrationTableSpec | StrPath] = (),
    parang: bool = False,
    combine: SolveCombine = SolveCombine(),
    band_type: BandpassType = "b",
    normalize_average_amplitude: bool = False,
    amplitude_degree: int = 3,
    phase_degree: int = 3,
    smodel: Sequence[float] = (1.0, 0.0, 0.0, 0.0),
    binary: StrPath | None = None,
) -> TaskResult:
    """Run the first-wave ``bandpass`` solver."""

    return invoke_calibration_task(
        kind="solve_bandpass",
        request={
            "measurement_set": os.fspath(measurement_set),
            "selection": _selection_request(selection),
            "output_table": os.fspath(output_table),
            "refant": _encode_refant(refant),
            "prior_calibration_tables": _table_specs_request(prior_calibration_tables),
            "parang": parang,
            "combine": combine.to_request(),
            "band_type": _encode_band_type(band_type),
            "normalize_average_amplitude": normalize_average_amplitude,
            "amplitude_degree": amplitude_degree,
            "phase_degree": phase_degree,
            "smodel": _encode_smodel(smodel),
        },
        binary=binary,
    )


def fluxscale(
    input_table: StrPath,
    output_table: StrPath,
    *,
    reference_fields: Sequence[str],
    transfer_fields: Sequence[str] = (),
    refspwmap: Sequence[int] = (),
    gainthreshold: float | None = None,
    incremental: bool = False,
    binary: StrPath | None = None,
) -> TaskResult:
    """Run the first-wave ``fluxscale`` stage."""

    return invoke_calibration_task(
        kind="flux_scale",
        request={
            "input_table": os.fspath(input_table),
            "output_table": os.fspath(output_table),
            "reference_fields": list(reference_fields),
            "transfer_fields": list(transfer_fields),
            "refspwmap": list(refspwmap),
            "gainthreshold": gainthreshold,
            "incremental": incremental,
        },
        binary=binary,
    )


def validate_signature_parity(*, binary: StrPath | None = None) -> None:
    """Fail if the Python wrapper metadata drifts from the Rust task schema."""

    definitions = schema(binary=binary)["request_schema"]["definitions"]
    for function_name, contract in _WRAPPER_CONTRACTS.items():
        properties = definitions[contract["schema"]]["properties"]
        expected_fields = set(contract["request_fields"])
        actual_fields = set(properties)
        if actual_fields != expected_fields:
            raise AssertionError(
                f"{function_name} request fields drifted: expected {sorted(expected_fields)}, "
                f"got {sorted(actual_fields)}"
            )

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


def _selection_request(selection: Selection | None) -> dict[str, Any]:
    return Selection().to_request() if selection is None else selection.to_request()


def _table_specs_request(
    specs: Sequence[CalibrationTableSpec | StrPath],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for spec in specs:
        if isinstance(spec, CalibrationTableSpec):
            normalized.append(spec.to_request())
        else:
            normalized.append(CalibrationTableSpec(path=spec).to_request())
    return normalized


def _encode_apply_mode(value: ApplyMode) -> str:
    mapping = {"calflag": "CalFlag", "calonly": "CalOnly", "trial": "Trial"}
    return mapping[value]


def _encode_interp(value: InterpolationMode) -> str:
    mapping = {
        "nearest": "Nearest",
        "linear": "Linear",
        "nearest,linear": "NearestLinear",
    }
    return mapping[value]


def _encode_gainfield(value: GainFieldValue | None) -> Any:
    if value is None:
        return None
    if isinstance(value, int):
        return {"FieldId": value}
    if value == "nearest":
        return "Nearest"
    return {"FieldName": value}


def _encode_refant(value: ReferenceAntenna) -> dict[str, Any]:
    if isinstance(value, int):
        return {"AntennaId": value}
    return {"AntennaName": value}


def _encode_gain_type(value: GainType) -> str:
    return value.upper()


def _encode_gain_solve_mode(value: GainSolveMode) -> str:
    return {"p": "Phase", "ap": "AmplitudePhase"}[value]


def _encode_gain_solve_model_source(value: GainSolveModelSource) -> str:
    normalized = str(value).lower()
    if normalized in {"point", "point_source", "point-source", "smodel"}:
        return "PointSource"
    if normalized in {"model", "model_column", "model-column", "model_data", "model-data"}:
        return "ModelColumn"
    raise ValueError(f"unsupported gain solve model source: {value!r}")


def _encode_gain_solve_interval(value: GainSolveInterval) -> Any:
    if value == "inf":
        return "Infinite"
    if value == "int":
        return "Integration"
    return {"Seconds": float(value)}


def _encode_band_type(value: BandpassType) -> str:
    return {"b": "B", "bpoly": "BPoly"}[value]


def _encode_stats_axis(value: StatsAxis) -> Any:
    lowered = value.lower()
    if lowered in {"amp", "amplitude"}:
        return "Amplitude"
    if lowered == "phase":
        return "Phase"
    if lowered == "real":
        return "Real"
    if lowered in {"imag", "imaginary"}:
        return "Imaginary"
    return {"Column": value.upper()}


def _encode_smodel(value: Sequence[float]) -> list[float]:
    encoded = [float(component) for component in value]
    if len(encoded) != 4:
        raise ValueError(f"smodel must have four Stokes components, got {len(encoded)}")
    return encoded


_WRAPPER_CONTRACTS: dict[str, dict[str, Any]] = {
    "summary": {
        "schema": "SummaryTaskRequest",
        "request_fields": ["paths"],
        "signature": ["paths", "binary"],
        "defaults": {"binary": None},
    },
    "stats": {
        "schema": "StatsTaskRequest",
        "request_fields": ["path", "axis", "datacolumn", "use_flags"],
        "signature": ["path", "axis", "datacolumn", "use_flags", "binary"],
        "defaults": {"axis": "amp", "datacolumn": None, "use_flags": False, "binary": None},
    },
    "plan_apply": {
        "schema": "PlanApplyTaskRequest",
        "request_fields": ["measurement_set", "selection", "calibration_tables", "parang"],
        "signature": ["measurement_set", "calibration_tables", "selection", "parang", "binary"],
        "defaults": {"selection": None, "parang": False, "binary": None},
    },
    "execute_apply": {
        "schema": "ExecuteApplyTaskRequest",
        "request_fields": [
            "measurement_set",
            "selection",
            "calibration_tables",
            "apply_mode",
            "parang",
        ],
        "signature": [
            "measurement_set",
            "calibration_tables",
            "apply_mode",
            "selection",
            "parang",
            "binary",
        ],
        "defaults": {
            "apply_mode": "calflag",
            "selection": None,
            "parang": False,
            "binary": None,
        },
    },
    "export_corrected_data": {
        "schema": "ExportCorrectedDataTaskRequest",
        "request_fields": ["input_ms", "output_ms", "selection"],
        "signature": ["input_ms", "output_ms", "selection", "binary"],
        "defaults": {"selection": None, "binary": None},
    },
    "solve_gain": {
        "schema": "SolveGainTaskRequest",
        "request_fields": [
            "measurement_set",
            "selection",
            "output_table",
            "gain_type",
            "solve_mode",
            "solve_interval",
            "combine",
            "refant",
            "prior_calibration_tables",
            "parang",
            "model_source",
            "normalize_average_amplitude",
            "min_snr",
            "smodel",
        ],
        "signature": [
            "measurement_set",
            "output_table",
            "refant",
            "selection",
            "gain_type",
            "solve_mode",
            "solve_interval",
            "combine",
            "prior_calibration_tables",
            "parang",
            "model_source",
            "normalize_average_amplitude",
            "min_snr",
            "smodel",
            "binary",
        ],
        "defaults": {
            "selection": None,
            "gain_type": "g",
            "solve_mode": "p",
            "solve_interval": "inf",
            "combine": SolveCombine(),
            "prior_calibration_tables": (),
            "parang": False,
            "model_source": "point",
            "normalize_average_amplitude": False,
            "min_snr": 3.0,
            "smodel": (1.0, 0.0, 0.0, 0.0),
            "binary": None,
        },
    },
    "solve_bandpass": {
        "schema": "SolveBandpassTaskRequest",
        "request_fields": [
            "measurement_set",
            "selection",
            "output_table",
            "refant",
            "prior_calibration_tables",
            "parang",
            "combine",
            "band_type",
            "normalize_average_amplitude",
            "amplitude_degree",
            "phase_degree",
            "smodel",
        ],
        "signature": [
            "measurement_set",
            "output_table",
            "refant",
            "selection",
            "prior_calibration_tables",
            "parang",
            "combine",
            "band_type",
            "normalize_average_amplitude",
            "amplitude_degree",
            "phase_degree",
            "smodel",
            "binary",
        ],
        "defaults": {
            "selection": None,
            "prior_calibration_tables": (),
            "parang": False,
            "combine": SolveCombine(),
            "band_type": "b",
            "normalize_average_amplitude": False,
            "amplitude_degree": 3,
            "phase_degree": 3,
            "smodel": (1.0, 0.0, 0.0, 0.0),
            "binary": None,
        },
    },
    "fluxscale": {
        "schema": "FluxScaleRequest",
        "request_fields": [
            "input_table",
            "output_table",
            "reference_fields",
            "transfer_fields",
            "refspwmap",
            "gainthreshold",
            "incremental",
        ],
        "signature": [
            "input_table",
            "output_table",
            "reference_fields",
            "transfer_fields",
            "refspwmap",
            "gainthreshold",
            "incremental",
            "binary",
        ],
        "defaults": {
            "transfer_fields": (),
            "refspwmap": (),
            "gainthreshold": None,
            "incremental": False,
            "binary": None,
        },
    },
}

__all__ = [
    "ApplyMode",
    "ApplyTableSelection",
    "BandpassType",
    "CalibrationTableSpec",
    "GainFieldValue",
    "GainSolveInterval",
    "GainSolveMode",
    "GainSolveModelSource",
    "GainType",
    "InterpolationMode",
    "ProtocolInfo",
    "ReferenceAntenna",
    "Selection",
    "SolveCombine",
    "StatsAxis",
    "TaskResult",
    "configure",
    "execute_apply",
    "export_corrected_data",
    "fluxscale",
    "plan_apply",
    "protocol_info",
    "schema",
    "solve_bandpass",
    "solve_gain",
    "stats",
    "summary",
    "validate_signature_parity",
]
