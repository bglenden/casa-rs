"""Strict versioned schemas for tutorial-parity sections and results."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .model import Comparison, Operation, SectionManifest, Surface


SECTION_SCHEMA_VERSION = 1
RESULT_SCHEMA_VERSION = 1
SURFACES = {"casa", "cli", "python", "tui", "gui"}
RESULT_STATUSES = {"completed", "dry_run", "unavailable", "failed"}

TOP_FIELDS = {
    "schema_version", "id", "pack", "title", "source", "prerequisites",
    "surfaces", "comparison", "evidence",
}
PACK_FIELDS = {"id", "relative_path"}
SOURCE_FIELDS = {"url", "anchor"}
PREREQUISITE_FIELDS = {"kind", "path"}
SURFACE_FIELDS = {"operations", "screenshot", "input_events", "journey", "required_artifacts"}
OPERATION_REQUIRED_FIELDS = {"task", "parameters", "outputs"}
OPERATION_FIELDS = OPERATION_REQUIRED_FIELDS | {"capture_stdout"}
INPUT_EVENT_FIELDS = {"after_ms", "text"}
COMPARISON_FIELDS = {"plugin", "config", "inputs"}
EVIDENCE_FIELDS = {"result", "review", "documentation", "screenshot_spec"}

TASK_PARAMETERS: dict[str, set[str]] = {
    "imhead": {"imagename", "mode", "json"},
    "imstat": {"imagename", "region", "box", "chans", "stokes", "mask", "axes", "json"},
    "immoments": {"imagename", "outfile", "moments", "axis", "region", "box", "chans", "stokes", "mask", "includepix", "excludepix", "overwrite"},
    "exportfits": {"imagename", "fitsimage", "velocity", "optical", "bitpix", "minpix", "maxpix", "overwrite", "dropstokes", "stokeslast", "history", "dropdeg"},
    "listobs": {"vis", "listfile", "overwrite", "verbose", "listunfl", "cachesize", "json"},
    "msexplore": {"vis", "format", "output", "preset", "avgchannel", "avgspw", "avgtime", "avgscan", "color_by", "iteraxis", "plot_output", "plot_format", "plot_width", "plot_height", "overwrite", "json"},
    "plotms": {"vis", "field", "xaxis", "yaxis", "avgchannel", "avgspw", "avgtime", "avgscan", "coloraxis", "iteraxis", "plotfile", "expformat", "exprange", "overwrite", "showgui"},
    "imager": {"vis", "imagename", "imsize", "cell_arcsec", "cell", "field", "phasecenter_field", "spw", "specmode", "gridder", "deconvolver", "weighting", "robust", "niter", "threshold_jy", "threshold", "mask_box", "mask", "pblimit", "write_pb", "pbcor", "dirty_only", "interactive", "overwrite"},
    "tclean": {"vis", "imagename", "imsize", "cell", "field", "spw", "specmode", "gridder", "deconvolver", "weighting", "robust", "niter", "threshold", "mask", "pblimit", "pbcor", "interactive"},
    "split": {"vis", "outputvis", "field", "spw", "width", "datacolumn", "keepflags", "timebin", "intent"},
    "impbcor": {"imagename", "pbimage", "outfile", "mode", "cutoff", "overwrite"},
}
COMPARATORS: dict[str, set[str]] = {
    "imhead": {"beam_tolerance", "axis_tolerance", "spectral_tolerance_hz"},
    "imstat": {"fields", "position_fields", "absolute_tolerance", "flux_tolerance"},
    "json_fields": {"fields", "absolute_tolerance", "relative_tolerance"},
    "image_products": {"products", "absolute_tolerance", "relative_tolerance", "metadata_fields"},
    "measurement_set": {"fields", "absolute_tolerance", "relative_tolerance"},
    "plot_products": {"fields", "absolute_tolerance", "relative_tolerance", "require_png", "panel_labels"},
    "fits_products": {"products", "absolute_tolerance", "relative_tolerance", "header_fields"},
}


class ContractError(ValueError):
    pass


def load_section(path: Path) -> SectionManifest:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ContractError(f"{path}: cannot load section manifest: {error}") from error
    return parse_section(value, path)


def parse_section(value: Any, source: Path | str = "<section>") -> SectionManifest:
    obj = _object(value, str(source))
    _exact_fields(obj, TOP_FIELDS, str(source))
    if obj["schema_version"] != SECTION_SCHEMA_VERSION:
        raise ContractError(f"{source}: schema_version must be {SECTION_SCHEMA_VERSION}")
    section_id = _nonempty_string(obj["id"], f"{source}.id")
    pack = _object(obj["pack"], f"{source}.pack")
    _exact_fields(pack, PACK_FIELDS, f"{source}.pack")
    source_ref = _object(obj["source"], f"{source}.source")
    _exact_fields(source_ref, SOURCE_FIELDS, f"{source}.source")
    source_values = {key: _nonempty_string(source_ref[key], f"{source}.source.{key}") for key in SOURCE_FIELDS}

    prerequisites_value = _list(obj["prerequisites"], f"{source}.prerequisites")
    prerequisites = []
    for index, item in enumerate(prerequisites_value):
        prereq = _object(item, f"{source}.prerequisites[{index}]")
        _exact_fields(prereq, PREREQUISITE_FIELDS, f"{source}.prerequisites[{index}]")
        prerequisites.append({
            "kind": _choice(prereq["kind"], {"file", "directory", "measurement_set", "casa_image"}, f"{source}.prerequisites[{index}].kind"),
            "path": _relative_path(prereq["path"], f"{source}.prerequisites[{index}].path"),
        })

    surface_values = _object(obj["surfaces"], f"{source}.surfaces")
    if set(surface_values) != SURFACES:
        raise ContractError(f"{source}.surfaces: must define exactly {sorted(SURFACES)}")
    surfaces = {
        name: _parse_surface(name, surface_values[name], f"{source}.surfaces.{name}")
        for name in sorted(SURFACES)
    }

    comparison_value = _object(obj["comparison"], f"{source}.comparison")
    _exact_fields(comparison_value, COMPARISON_FIELDS, f"{source}.comparison")
    plugin = _choice(comparison_value["plugin"], set(COMPARATORS), f"{source}.comparison.plugin")
    config = _object(comparison_value["config"], f"{source}.comparison.config")
    unknown_config = set(config) - COMPARATORS[plugin]
    if unknown_config:
        raise ContractError(f"{source}.comparison.config: unknown fields {sorted(unknown_config)}")
    inputs = _object(comparison_value["inputs"], f"{source}.comparison.inputs")
    for key, path_value in inputs.items():
        _nonempty_string(key, f"{source}.comparison.inputs key")
        if isinstance(path_value, str):
            _relative_path(path_value, f"{source}.comparison.inputs.{key}")
        elif isinstance(path_value, list):
            for index, item in enumerate(path_value):
                _relative_path(item, f"{source}.comparison.inputs.{key}[{index}]")
        else:
            raise ContractError(f"{source}.comparison.inputs.{key}: expected path or path list")

    evidence = _object(obj["evidence"], f"{source}.evidence")
    _exact_fields(evidence, EVIDENCE_FIELDS, f"{source}.evidence")
    evidence_values = {
        "result": _relative_path(evidence["result"], f"{source}.evidence.result"),
        "review": _relative_path(evidence["review"], f"{source}.evidence.review"),
        "documentation": tuple(
            _relative_path(item, f"{source}.evidence.documentation[{index}]")
            for index, item in enumerate(_list(evidence["documentation"], f"{source}.evidence.documentation"))
        ),
        "screenshot_spec": _relative_path(evidence["screenshot_spec"], f"{source}.evidence.screenshot_spec"),
    }
    return SectionManifest(
        path=Path(source),
        section_id=section_id,
        pack_id=_nonempty_string(pack["id"], f"{source}.pack.id"),
        pack_relative_path=_relative_path(pack["relative_path"], f"{source}.pack.relative_path"),
        title=_nonempty_string(obj["title"], f"{source}.title"),
        source=source_values,
        prerequisites=tuple(prerequisites),
        surfaces=surfaces,
        comparison=Comparison(plugin=plugin, config=dict(config), inputs=dict(inputs)),
        evidence=evidence_values,
    )


def validate_result(value: Any, source: str = "<result>") -> dict[str, Any]:
    obj = _object(value, source)
    required = {"schema_version", "kind", "section_id", "status", "created_at", "resources", "surfaces", "comparison", "artifacts", "failure"}
    _exact_fields(obj, required, source)
    if obj["schema_version"] != RESULT_SCHEMA_VERSION or obj["kind"] != "tutorial_parity_result":
        raise ContractError(f"{source}: unsupported result contract")
    status = _choice(obj["status"], RESULT_STATUSES, f"{source}.status")
    _nonempty_string(obj["section_id"], f"{source}.section_id")
    _nonempty_string(obj["created_at"], f"{source}.created_at")
    _object(obj["resources"], f"{source}.resources")
    surfaces = _object(obj["surfaces"], f"{source}.surfaces")
    if not set(surfaces).issubset(SURFACES):
        raise ContractError(f"{source}.surfaces: unknown surfaces")
    for name, surface in surfaces.items():
        surface_obj = _object(surface, f"{source}.surfaces.{name}")
        _exact_fields(surface_obj, {"status", "operations", "artifacts", "reason"}, f"{source}.surfaces.{name}")
        _choice(surface_obj["status"], RESULT_STATUSES, f"{source}.surfaces.{name}.status")
    _object(obj["comparison"], f"{source}.comparison")
    _list(obj["artifacts"], f"{source}.artifacts")
    if status == "failed" and not isinstance(obj["failure"], dict):
        raise ContractError(f"{source}: failed result requires failure object")
    if status != "failed" and obj["failure"] is not None:
        raise ContractError(f"{source}: non-failed result must not include failure")
    return obj


def _parse_surface(name: str, value: Any, source: str) -> Surface:
    obj = _object(value, source)
    allowed = set(SURFACE_FIELDS)
    unknown = set(obj) - allowed
    if unknown:
        raise ContractError(f"{source}: unknown fields {sorted(unknown)}")
    operations_value = _list(obj.get("operations", []), f"{source}.operations")
    operations = []
    for index, item in enumerate(operations_value):
        op_source = f"{source}.operations[{index}]"
        operation = _object(item, op_source)
        missing = OPERATION_REQUIRED_FIELDS - set(operation)
        unknown = set(operation) - OPERATION_FIELDS
        if missing or unknown:
            raise ContractError(f"{op_source}: fields missing={sorted(missing)} unknown={sorted(unknown)}")
        task = _choice(operation["task"], set(TASK_PARAMETERS), f"{op_source}.task")
        parameters = _object(operation["parameters"], f"{op_source}.parameters")
        unknown_parameters = set(parameters) - TASK_PARAMETERS[task]
        if unknown_parameters:
            raise ContractError(f"{op_source}.parameters: unknown {task} fields {sorted(unknown_parameters)}")
        outputs = tuple(
            _relative_path(path, f"{op_source}.outputs[{output_index}]")
            for output_index, path in enumerate(_list(operation["outputs"], f"{op_source}.outputs"))
        )
        capture_stdout = operation.get("capture_stdout")
        if capture_stdout is not None:
            capture_stdout = _relative_path(capture_stdout, f"{op_source}.capture_stdout")
            if capture_stdout not in outputs:
                raise ContractError(f"{op_source}.capture_stdout: must also be declared in outputs")
        operations.append(Operation(task=task, parameters=dict(parameters), outputs=outputs, capture_stdout=capture_stdout))

    screenshot = obj.get("screenshot")
    if screenshot is not None:
        screenshot = _relative_path(screenshot, f"{source}.screenshot")
    events = []
    for index, item in enumerate(_list(obj.get("input_events", []), f"{source}.input_events")):
        event_source = f"{source}.input_events[{index}]"
        event = _object(item, event_source)
        _exact_fields(event, INPUT_EVENT_FIELDS, event_source)
        after_ms = event["after_ms"]
        text = event["text"]
        if not isinstance(after_ms, int) or after_ms < 0:
            raise ContractError(f"{event_source}.after_ms: expected non-negative integer")
        if text not in {"r", "y", "\r", "\n", "\t"}:
            raise ContractError(f"{event_source}.text: unsupported bounded UI event")
        events.append((after_ms, text))
    journey = obj.get("journey")
    if journey is not None and journey != "tutorial-journey-gui":
        raise ContractError(f"{source}.journey: unsupported GUI journey")
    required_artifacts = tuple(
        _relative_path(path, f"{source}.required_artifacts[{index}]")
        for index, path in enumerate(_list(obj.get("required_artifacts", []), f"{source}.required_artifacts"))
    )
    if name == "gui":
        if operations or screenshot or events or journey is None:
            raise ContractError(f"{source}: GUI must delegate only to the approved XCUITest journey")
    elif journey is not None or required_artifacts:
        raise ContractError(f"{source}: journey fields are GUI-only")
    elif not operations:
        raise ContractError(f"{source}: surface requires at least one operation")
    if name == "tui" and screenshot is None:
        raise ContractError(f"{source}: TUI requires a Ghostty screenshot path")
    return Surface(
        name=name,
        operations=tuple(operations),
        screenshot=screenshot,
        input_events=tuple(events),
        journey=journey,
        required_artifacts=required_artifacts,
    )


def _object(value: Any, source: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(f"{source}: expected object")
    return value


def _list(value: Any, source: str) -> list[Any]:
    if not isinstance(value, list):
        raise ContractError(f"{source}: expected array")
    return value


def _exact_fields(obj: dict[str, Any], expected: set[str], source: str) -> None:
    missing = expected - set(obj)
    unknown = set(obj) - expected
    if missing or unknown:
        raise ContractError(f"{source}: fields missing={sorted(missing)} unknown={sorted(unknown)}")


def _nonempty_string(value: Any, source: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{source}: expected non-empty string")
    return value


def _choice(value: Any, choices: set[str], source: str) -> str:
    text = _nonempty_string(value, source)
    if text not in choices:
        raise ContractError(f"{source}: expected one of {sorted(choices)}")
    return text


def _relative_path(value: Any, source: str) -> str:
    text = _nonempty_string(value, source)
    path = Path(text)
    if path.is_absolute() or ".." in path.parts:
        raise ContractError(f"{source}: path must remain relative to its declared root")
    return text
