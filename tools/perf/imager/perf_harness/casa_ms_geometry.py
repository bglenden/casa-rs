#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA-side, bounded MeasurementSet geometry evidence protocol.

Every selector, row-count expectation, and sampling limit is carried by the
request. CASA is imported only for an ``inspect`` action, so plans and schema
tests run in an ordinary Python interpreter.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import pathlib
import sys
import tempfile
from typing import Any, Callable, Iterable


REQUEST_SCHEMA_VERSION = 1
RESULT_SCHEMA_VERSION = 1
REQUEST_KIND = "casa_ms_geometry_request"
RESULT_KIND = "casa_ms_geometry_result"

REQUEST_FIELDS = {
    "schema_version",
    "kind",
    "request_id",
    "action",
    "expected_casa_version",
    "dataset",
    "selections",
    "limits",
}
DATASET_FIELDS = {"path"}
SELECTION_FIELDS = {
    "id",
    "field",
    "spw",
    "uvrange",
    "intent",
    "expected_selected_rows",
}
LIMIT_FIELDS = {
    "table_block_rows",
    "max_representative_rows",
    "ms_range_block_mb",
}
RESULT_FIELDS = {
    "schema_version",
    "kind",
    "status",
    "request_id",
    "action",
    "request_sha256",
    "casa",
    "dataset",
    "selections",
    "limits",
    "geometry",
    "selection_results",
    "failure",
}

SUPPORTED_ACTIONS = {"plan", "inspect"}
RESULT_STATUSES = {
    "planned",
    "completed",
    "failed_validation",
    "failed_execution",
    "failed_postcondition",
}

GEOMETRY_FIELDS = {
    "main",
    "field",
    "spectral_window",
    "data_description",
    "polarization",
    "pointing",
}
MAIN_GEOMETRY_FIELDS = {
    "row_count",
    "column_names",
    "scan",
    "time_seconds",
    "interval_seconds",
    "field_id",
    "data_desc_id",
    "scan_number",
    "uvw_meters",
}
FIELD_GEOMETRY_FIELDS = {
    "row_count",
    "column_names",
    "direction_reference",
    "sampling",
    "rows",
}
SAMPLED_TABLE_GEOMETRY_FIELDS = {
    "row_count",
    "column_names",
    "sampling",
    "rows",
}
POINTING_GEOMETRY_FIELDS = {
    "row_count",
    "column_names",
    "scan",
    "time_seconds",
    "interval_seconds",
    "antenna_id",
    "direction_reference",
    "sampling",
    "rows",
}
DIRECTION_REFERENCE_FIELDS = {
    "type",
    "fixed_reference",
    "variable_reference_column",
    "reference_codes",
    "quantum_units",
}


class ProtocolError(ValueError):
    """The request or evidence result violates the versioned protocol."""


def canonical_json_bytes(value: Any) -> bytes:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise ProtocolError(f"value is not canonical JSON: {error}") from error
    return encoded.encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def validate_request(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProtocolError("request must be a JSON object")
    _require_exact_fields(value, REQUEST_FIELDS, source="request")
    if isinstance(value["schema_version"], bool) or value["schema_version"] != 1:
        raise ProtocolError("request.schema_version must be 1")
    if value["kind"] != REQUEST_KIND:
        raise ProtocolError(f"request.kind must be {REQUEST_KIND!r}")
    _nonempty_string(value["request_id"], field="request.request_id")
    if value["action"] not in SUPPORTED_ACTIONS:
        raise ProtocolError("request.action must be 'plan' or 'inspect'")
    _nonempty_string(
        value["expected_casa_version"], field="request.expected_casa_version"
    )

    dataset = _object(value["dataset"], field="request.dataset")
    _require_exact_fields(dataset, DATASET_FIELDS, source="request.dataset")
    _absolute_path(dataset["path"], field="request.dataset.path")

    selections = value["selections"]
    if not isinstance(selections, list) or not selections:
        raise ProtocolError("request.selections must be a non-empty list")
    identifiers: set[str] = set()
    for index, selection_value in enumerate(selections):
        selection = _object(selection_value, field=f"request.selections[{index}]")
        source = f"request.selections[{index}]"
        _require_exact_fields(selection, SELECTION_FIELDS, source=source)
        identifier = _nonempty_string(selection["id"], field=f"{source}.id")
        if identifier in identifiers:
            raise ProtocolError(f"{source}.id duplicates {identifier!r}")
        identifiers.add(identifier)
        for name in ("field", "spw", "uvrange", "intent"):
            _nonempty_string(selection[name], field=f"{source}.{name}")
        _positive_integer(
            selection["expected_selected_rows"],
            field=f"{source}.expected_selected_rows",
        )

    limits = _object(value["limits"], field="request.limits")
    _require_exact_fields(limits, LIMIT_FIELDS, source="request.limits")
    block_rows = _positive_integer(
        limits["table_block_rows"], field="request.limits.table_block_rows"
    )
    representative_rows = _positive_integer(
        limits["max_representative_rows"],
        field="request.limits.max_representative_rows",
    )
    range_mb = _positive_integer(
        limits["ms_range_block_mb"], field="request.limits.ms_range_block_mb"
    )
    if block_rows > 1_000_000:
        raise ProtocolError("request.limits.table_block_rows must be <= 1000000")
    if representative_rows > 4096:
        raise ProtocolError("request.limits.max_representative_rows must be <= 4096")
    if range_mb > 1024:
        raise ProtocolError("request.limits.ms_range_block_mb must be <= 1024")
    canonical_json_bytes(value)
    return dict(value)


def build_inspection_plan(request: dict[str, Any]) -> dict[str, Any]:
    request = validate_request(request)
    result = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "kind": RESULT_KIND,
        "status": "planned",
        "request_id": request["request_id"],
        "action": request["action"],
        "request_sha256": canonical_sha256(request),
        "casa": {"expected_version": request["expected_casa_version"]},
        "dataset": dict(request["dataset"]),
        "selections": [dict(item) for item in request["selections"]],
        "limits": dict(request["limits"]),
    }
    validate_result(result)
    return result


def process_request(
    request: dict[str, Any],
    *,
    inspector: Callable[[str, list[dict[str, Any]], dict[str, int]], dict[str, Any]]
    | None = None,
    casa_version: str | None = None,
) -> dict[str, Any]:
    """Plan or inspect one dataset; injected arguments keep tests CASA-free."""

    plan = build_inspection_plan(request)
    if request["action"] == "plan":
        return plan

    if inspector is None:
        inspector, casa_version = _load_casa_runtime()
    if casa_version is None:
        return _failed_from_plan(
            plan,
            status="failed_execution",
            kind="runtime",
            reason="CASA runtime did not report a version",
            exception_type="ProtocolError",
        )
    expected_version = request["expected_casa_version"]
    if casa_version != expected_version:
        return _failed_from_plan(
            plan,
            status="failed_validation",
            kind="casa_version",
            reason=(
                f"CASA version mismatch: expected {expected_version}, got "
                f"{casa_version}"
            ),
            exception_type="ProtocolError",
        )

    try:
        inspection = inspector(
            request["dataset"]["path"], request["selections"], request["limits"]
        )
        validate_inspection(inspection)
        _validate_inspection_matches_request(inspection, request)
    except ProtocolError as error:
        return _failed_from_plan(
            plan,
            status="failed_validation",
            kind="inspection_protocol",
            reason=str(error),
            exception_type=type(error).__name__,
            casa_version=casa_version,
        )
    except Exception as error:
        return _failed_from_plan(
            plan,
            status="failed_execution",
            kind="inspection",
            reason=str(error),
            exception_type=type(error).__name__,
            casa_version=casa_version,
        )

    selection_results = inspection["selection_results"]
    mismatches = [
        {
            "selection_id": item["id"],
            "expected_selected_rows": item["expected_selected_rows"],
            "actual_selected_rows": item["actual_selected_rows"],
        }
        for item in selection_results
        if not item["matches_expected"]
    ]
    result = dict(plan)
    result["casa"] = {
        "expected_version": expected_version,
        "actual_version": casa_version,
    }
    result["geometry"] = inspection["geometry"]
    result["selection_results"] = selection_results
    if mismatches:
        result["status"] = "failed_postcondition"
        result["failure"] = {
            "kind": "selected_row_count",
            "reason": "one or more selected-row counts differ from the request",
            "mismatches": mismatches,
        }
    else:
        result["status"] = "completed"
    validate_result(result)
    return result


def inspect_ms_geometry(
    dataset_path: str,
    selections: list[dict[str, Any]],
    limits: dict[str, int],
) -> dict[str, Any]:
    """Read bounded table blocks and apply selectors through CASA MSSelection."""

    # Deliberately lazy: importing this module must not initialize CASA or NumPy.
    import numpy as np  # type: ignore[import-not-found]
    from casatools import ms as ms_factory  # type: ignore[import-not-found]
    from casatools import table as table_factory  # type: ignore[import-not-found]

    path = pathlib.Path(dataset_path)
    if not path.is_dir():
        raise ProtocolError(f"MeasurementSet is not a directory: {path}")

    selection_results: list[dict[str, Any]] = []
    focus_fields: set[int] = set()
    focus_spws: set[int] = set()
    focus_ddids: set[int] = set()
    range_block_mb = int(limits["ms_range_block_mb"])
    for selection in selections:
        handle = ms_factory()
        try:
            handle.open(str(path), nomodify=True)
            expressions = {
                "field": selection["field"],
                "spw": selection["spw"],
                "uvdist": selection["uvrange"],
                "scanintent": selection["intent"],
            }
            handle.msselect(expressions, onlyparse=False)
            actual_rows = int(handle.nrow(selected=True))
            indices = handle.msselectedindices()
            ranges = handle.range(
                ["time", "uvdist", "field_id", "data_desc_id"],
                useflags=False,
                blocksize=range_block_mb,
            )
        finally:
            _close_casa_tool(handle)

        field_ids = _integer_values(indices.get("field", []), np)
        spw_ids = _integer_values(indices.get("spw", []), np)
        ddids = _integer_values(ranges.get("data_desc_id", []), np)
        focus_fields.update(field_ids)
        focus_spws.update(spw_ids)
        focus_ddids.update(ddids)
        selection_results.append(
            {
                "id": selection["id"],
                "expressions": {
                    "field": selection["field"],
                    "spw": selection["spw"],
                    "uvrange": selection["uvrange"],
                    "intent": selection["intent"],
                },
                "expected_selected_rows": selection["expected_selected_rows"],
                "actual_selected_rows": actual_rows,
                "matches_expected": actual_rows == selection["expected_selected_rows"],
                "selected_indices": {
                    "field_ids": field_ids,
                    "spw_ids": spw_ids,
                    "data_desc_ids": ddids,
                    "channel_selection": _bounded_json_array(
                        indices.get("channel", []),
                        np,
                        int(limits["max_representative_rows"]),
                    ),
                },
                "range": {
                    "time_seconds": _range_pair(ranges.get("time", []), np),
                    "uvdistance_meters": _range_pair(ranges.get("uvdist", []), np),
                },
                # Filled once the DDID/SPW/polarization geometry is known.
                "channels_and_correlations": [],
            }
        )

    block_rows = int(limits["table_block_rows"])
    sample_limit = int(limits["max_representative_rows"])
    main = _inspect_main(table_factory, np, path, block_rows, sample_limit)
    data_description = _inspect_data_description(
        table_factory, path, focus_ddids, focus_spws, sample_limit
    )
    if not focus_spws:
        focus_spws.update(row["spectral_window_id"] for row in data_description["rows"])
    spectral_window = _inspect_spectral_window(
        table_factory, np, path, focus_spws, sample_limit
    )
    focus_polarizations = {
        row["polarization_id"]
        for row in data_description["rows"]
        if row["row_id"] in focus_ddids or not focus_ddids
    }
    polarization = _inspect_polarization(
        table_factory, np, path, focus_polarizations, sample_limit
    )
    field = _inspect_field(table_factory, np, path, focus_fields, sample_limit)
    pointing = _inspect_pointing(table_factory, np, path, block_rows, sample_limit)

    ddid_rows = {row["row_id"]: row for row in data_description["rows"]}
    spw_rows = {row["row_id"]: row for row in spectral_window["rows"]}
    pol_rows = {row["row_id"]: row for row in polarization["rows"]}
    for result in selection_results:
        # ``ms.range(data_desc_id)`` is allowed to return either unique IDs or
        # only extrema depending on CASA build.  The selected SPW IDs plus the
        # complete bounded DATA_DESCRIPTION map are the authoritative way to
        # recover every selected DDID for channel/correlation accounting.
        selected_spws = set(result["selected_indices"]["spw_ids"])
        derived_ddids = {
            ddid
            for ddid, mapping in ddid_rows.items()
            if mapping["spectral_window_id"] in selected_spws
            and not mapping["flag_row"]
        }
        result["selected_indices"]["data_desc_ids"] = sorted(
            set(result["selected_indices"]["data_desc_ids"]).union(derived_ddids)
        )
        channel_geometry: list[dict[str, Any]] = []
        for ddid in result["selected_indices"]["data_desc_ids"]:
            mapping = ddid_rows.get(ddid)
            if mapping is None:
                continue
            spw_id = mapping["spectral_window_id"]
            pol_id = mapping["polarization_id"]
            spw_row = spw_rows.get(spw_id)
            pol_row = pol_rows.get(pol_id)
            if spw_row is None or pol_row is None:
                continue
            channel_geometry.append(
                {
                    "data_desc_id": ddid,
                    "spectral_window_id": spw_id,
                    "polarization_id": pol_id,
                    "num_channels": spw_row["num_channels"],
                    "num_correlations": pol_row["num_correlations"],
                    "correlation_types": pol_row["correlation_types"],
                    "correlation_names": pol_row["correlation_names"],
                }
            )
        result["channels_and_correlations"] = channel_geometry

    inspection = {
        "geometry": {
            "main": main,
            "field": field,
            "spectral_window": spectral_window,
            "data_description": data_description,
            "polarization": polarization,
            "pointing": pointing,
        },
        "selection_results": selection_results,
    }
    validate_inspection(inspection)
    return inspection


def validate_inspection(value: Any) -> None:
    inspection = _object(value, field="inspection")
    _require_exact_fields(
        inspection, {"geometry", "selection_results"}, source="inspection"
    )
    geometry = _object(inspection["geometry"], field="inspection.geometry")
    _require_exact_fields(geometry, GEOMETRY_FIELDS, source="inspection.geometry")
    for name in GEOMETRY_FIELDS:
        _object(geometry[name], field=f"inspection.geometry.{name}")

    main = geometry["main"]
    _require_exact_fields(main, MAIN_GEOMETRY_FIELDS, source="inspection.geometry.main")
    _nonnegative_integer(main["row_count"], field="inspection.geometry.main.row_count")
    _string_list(main["column_names"], field="inspection.geometry.main.column_names")
    _validate_scan(main["scan"], source="inspection.geometry.main.scan")
    _validate_numeric_summary(
        main["time_seconds"], source="inspection.geometry.main.time_seconds"
    )
    _validate_numeric_summary(
        main["interval_seconds"],
        source="inspection.geometry.main.interval_seconds",
    )
    for name in ("field_id", "data_desc_id", "scan_number"):
        _validate_integer_summary(main[name], source=f"inspection.geometry.main.{name}")
    uvw = _object(main["uvw_meters"], field="inspection.geometry.main.uvw_meters")
    _require_exact_fields(
        uvw, {"u", "v", "w"}, source="inspection.geometry.main.uvw_meters"
    )
    for name in ("u", "v", "w"):
        _validate_numeric_summary(
            uvw[name], source=f"inspection.geometry.main.uvw_meters.{name}"
        )

    field = geometry["field"]
    _require_exact_fields(
        field, FIELD_GEOMETRY_FIELDS, source="inspection.geometry.field"
    )
    _validate_sampled_table(field, source="inspection.geometry.field")
    _validate_direction_reference(
        field["direction_reference"],
        source="inspection.geometry.field.direction_reference",
    )
    for name in ("spectral_window", "data_description", "polarization"):
        table = geometry[name]
        _require_exact_fields(
            table,
            SAMPLED_TABLE_GEOMETRY_FIELDS,
            source=f"inspection.geometry.{name}",
        )
        _validate_sampled_table(table, source=f"inspection.geometry.{name}")
    pointing = geometry["pointing"]
    _require_exact_fields(
        pointing,
        POINTING_GEOMETRY_FIELDS,
        source="inspection.geometry.pointing",
    )
    _validate_sampled_table(pointing, source="inspection.geometry.pointing")
    _validate_scan(pointing["scan"], source="inspection.geometry.pointing.scan")
    _validate_numeric_summary(
        pointing["time_seconds"],
        source="inspection.geometry.pointing.time_seconds",
    )
    _validate_numeric_summary(
        pointing["interval_seconds"],
        source="inspection.geometry.pointing.interval_seconds",
    )
    _validate_integer_summary(
        pointing["antenna_id"], source="inspection.geometry.pointing.antenna_id"
    )
    _validate_direction_reference(
        pointing["direction_reference"],
        source="inspection.geometry.pointing.direction_reference",
    )

    results = inspection["selection_results"]
    if not isinstance(results, list) or not results:
        raise ProtocolError("inspection.selection_results must be a non-empty list")
    required = {
        "id",
        "expressions",
        "expected_selected_rows",
        "actual_selected_rows",
        "matches_expected",
        "selected_indices",
        "range",
        "channels_and_correlations",
    }
    for index, result_value in enumerate(results):
        result = _object(result_value, field=f"inspection.selection_results[{index}]")
        _require_exact_fields(
            result, required, source=f"inspection.selection_results[{index}]"
        )
        _nonempty_string(result["id"], field=f"selection_results[{index}].id")
        expected = _positive_integer(
            result["expected_selected_rows"],
            field=f"selection_results[{index}].expected_selected_rows",
        )
        actual = _nonnegative_integer(
            result["actual_selected_rows"],
            field=f"selection_results[{index}].actual_selected_rows",
        )
        if not isinstance(result["matches_expected"], bool):
            raise ProtocolError(
                f"selection_results[{index}].matches_expected must be boolean"
            )
        if result["matches_expected"] != (actual == expected):
            raise ProtocolError(
                f"selection_results[{index}].matches_expected is inconsistent"
            )
        if not isinstance(result["channels_and_correlations"], list):
            raise ProtocolError(
                f"selection_results[{index}].channels_and_correlations must be a list"
            )
        expressions = _object(
            result["expressions"], field=f"selection_results[{index}].expressions"
        )
        _require_exact_fields(
            expressions,
            {"field", "spw", "uvrange", "intent"},
            source=f"selection_results[{index}].expressions",
        )
        for name in ("field", "spw", "uvrange", "intent"):
            _nonempty_string(
                expressions[name],
                field=f"selection_results[{index}].expressions.{name}",
            )
        selected = _object(
            result["selected_indices"],
            field=f"selection_results[{index}].selected_indices",
        )
        _require_exact_fields(
            selected,
            {"field_ids", "spw_ids", "data_desc_ids", "channel_selection"},
            source=f"selection_results[{index}].selected_indices",
        )
        for name in ("field_ids", "spw_ids", "data_desc_ids"):
            _integer_list(
                selected[name],
                field=f"selection_results[{index}].selected_indices.{name}",
            )
        channel_selection = _object(
            selected["channel_selection"],
            field=f"selection_results[{index}].selected_indices.channel_selection",
        )
        _require_exact_fields(
            channel_selection,
            {"shape", "rows", "truncated"},
            source=f"selection_results[{index}].selected_indices.channel_selection",
        )
        _integer_list(
            channel_selection["shape"],
            field=f"selection_results[{index}].selected_indices.channel_selection.shape",
        )
        if not isinstance(channel_selection["rows"], list):
            raise ProtocolError(
                f"selection_results[{index}].selected_indices.channel_selection.rows "
                "must be a list"
            )
        if not isinstance(channel_selection["truncated"], bool):
            raise ProtocolError(
                f"selection_results[{index}].selected_indices.channel_selection.truncated "
                "must be boolean"
            )
        range_value = _object(
            result["range"], field=f"selection_results[{index}].range"
        )
        _require_exact_fields(
            range_value,
            {"time_seconds", "uvdistance_meters"},
            source=f"selection_results[{index}].range",
        )
        for name in ("time_seconds", "uvdistance_meters"):
            _optional_range_pair(
                range_value[name], field=f"selection_results[{index}].range.{name}"
            )
        for channel_index, channel_value in enumerate(
            result["channels_and_correlations"]
        ):
            channel = _object(
                channel_value,
                field=(
                    f"selection_results[{index}].channels_and_correlations"
                    f"[{channel_index}]"
                ),
            )
            _require_exact_fields(
                channel,
                {
                    "data_desc_id",
                    "spectral_window_id",
                    "polarization_id",
                    "num_channels",
                    "num_correlations",
                    "correlation_types",
                    "correlation_names",
                },
                source=(
                    f"selection_results[{index}].channels_and_correlations"
                    f"[{channel_index}]"
                ),
            )
            for name in (
                "data_desc_id",
                "spectral_window_id",
                "polarization_id",
            ):
                _nonnegative_integer(
                    channel[name],
                    field=f"channels_and_correlations[{channel_index}].{name}",
                )
            _positive_integer(
                channel["num_channels"],
                field=f"channels_and_correlations[{channel_index}].num_channels",
            )
            correlations = _positive_integer(
                channel["num_correlations"],
                field=f"channels_and_correlations[{channel_index}].num_correlations",
            )
            correlation_types = _integer_list(
                channel["correlation_types"],
                field=f"channels_and_correlations[{channel_index}].correlation_types",
            )
            correlation_names = _string_list(
                channel["correlation_names"],
                field=f"channels_and_correlations[{channel_index}].correlation_names",
            )
            if (
                len(correlation_types) != correlations
                or len(correlation_names) != correlations
            ):
                raise ProtocolError(
                    f"channels_and_correlations[{channel_index}] correlation counts differ"
                )
    canonical_json_bytes(inspection)


def _validate_inspection_matches_request(inspection, request) -> None:
    actual = inspection["selection_results"]
    expected = request["selections"]
    if len(actual) != len(expected):
        raise ProtocolError(
            "inspection selection-result count does not match request selections"
        )
    for index, (result, selection) in enumerate(zip(actual, expected)):
        if result["id"] != selection["id"]:
            raise ProtocolError(
                f"inspection selection_results[{index}].id does not match request"
            )
        if result["expected_selected_rows"] != selection["expected_selected_rows"]:
            raise ProtocolError(
                f"inspection selection_results[{index}] changed expected row count"
            )
        expected_expressions = {
            "field": selection["field"],
            "spw": selection["spw"],
            "uvrange": selection["uvrange"],
            "intent": selection["intent"],
        }
        if result["expressions"] != expected_expressions:
            raise ProtocolError(
                f"inspection selection_results[{index}] changed selection expressions"
            )


def _validate_sampled_table(value, *, source) -> None:
    _nonnegative_integer(value["row_count"], field=f"{source}.row_count")
    _string_list(value["column_names"], field=f"{source}.column_names")
    if not isinstance(value["rows"], list):
        raise ProtocolError(f"{source}.rows must be a list")
    sampling = _object(value["sampling"], field=f"{source}.sampling")
    _require_exact_fields(
        sampling,
        {
            "method",
            "limit",
            "sample_count",
            "row_count",
            "focus_row_count",
            "truncated",
        },
        source=f"{source}.sampling",
    )
    _nonempty_string(sampling["method"], field=f"{source}.sampling.method")
    _positive_integer(sampling["limit"], field=f"{source}.sampling.limit")
    sample_count = _nonnegative_integer(
        sampling["sample_count"], field=f"{source}.sampling.sample_count"
    )
    row_count = _nonnegative_integer(
        sampling["row_count"], field=f"{source}.sampling.row_count"
    )
    _nonnegative_integer(
        sampling["focus_row_count"], field=f"{source}.sampling.focus_row_count"
    )
    if not isinstance(sampling["truncated"], bool):
        raise ProtocolError(f"{source}.sampling.truncated must be boolean")
    if row_count != value["row_count"] or sample_count != len(value["rows"]):
        raise ProtocolError(f"{source}.sampling counts are inconsistent")


def _validate_scan(value, *, source) -> None:
    scan = _object(value, field=source)
    _require_exact_fields(scan, {"block_rows", "blocks_read"}, source=source)
    _positive_integer(scan["block_rows"], field=f"{source}.block_rows")
    _nonnegative_integer(scan["blocks_read"], field=f"{source}.blocks_read")


def _validate_direction_reference(value, *, source) -> None:
    reference = _object(value, field=source)
    _require_exact_fields(reference, DIRECTION_REFERENCE_FIELDS, source=source)
    for name in ("type", "fixed_reference", "variable_reference_column"):
        item = reference[name]
        if item is not None and not isinstance(item, str):
            raise ProtocolError(f"{source}.{name} must be null or a string")
    _string_list(reference["quantum_units"], field=f"{source}.quantum_units")
    codes = reference["reference_codes"]
    if not isinstance(codes, list):
        raise ProtocolError(f"{source}.reference_codes must be a list")
    for index, code_value in enumerate(codes):
        code = _object(code_value, field=f"{source}.reference_codes[{index}]")
        _require_exact_fields(
            code, {"code", "name"}, source=f"{source}.reference_codes[{index}]"
        )
        _nonnegative_integer(
            code["code"], field=f"{source}.reference_codes[{index}].code"
        )
        _nonempty_string(code["name"], field=f"{source}.reference_codes[{index}].name")


def _validate_numeric_summary(value, *, source) -> None:
    summary = _object(value, field=source)
    _require_exact_fields(summary, {"count", "min", "max"}, source=source)
    count = _nonnegative_integer(summary["count"], field=f"{source}.count")
    minimum = summary["min"]
    maximum = summary["max"]
    if count == 0:
        if minimum is not None or maximum is not None:
            raise ProtocolError(f"{source} empty summary must have null extrema")
        return
    for name, item in (("min", minimum), ("max", maximum)):
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise ProtocolError(f"{source}.{name} must be a finite number")
        if not math.isfinite(float(item)):
            raise ProtocolError(f"{source}.{name} must be a finite number")
    if float(minimum) > float(maximum):
        raise ProtocolError(f"{source}.min must not exceed max")


def _validate_integer_summary(value, *, source) -> None:
    summary = _object(value, field=source)
    _require_exact_fields(
        summary,
        {
            "count",
            "min",
            "max",
            "representative_values",
            "representative_values_truncated",
        },
        source=source,
    )
    count = _nonnegative_integer(summary["count"], field=f"{source}.count")
    values = _integer_list(
        summary["representative_values"],
        field=f"{source}.representative_values",
    )
    if values != sorted(set(values)):
        raise ProtocolError(f"{source}.representative_values must be sorted unique")
    if not isinstance(summary["representative_values_truncated"], bool):
        raise ProtocolError(f"{source}.representative_values_truncated must be boolean")
    if count == 0:
        if summary["min"] is not None or summary["max"] is not None or values:
            raise ProtocolError(f"{source} empty summary has non-empty values")
        return
    minimum = _integer(summary["min"], field=f"{source}.min")
    maximum = _integer(summary["max"], field=f"{source}.max")
    if minimum > maximum:
        raise ProtocolError(f"{source}.min must not exceed max")


def _optional_range_pair(value, *, field) -> None:
    if value is None:
        return
    if not isinstance(value, list) or len(value) != 2:
        raise ProtocolError(f"{field} must be null or a two-number list")
    converted = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise ProtocolError(f"{field} must be null or a two-number list")
        number = float(item)
        if not math.isfinite(number):
            raise ProtocolError(f"{field} values must be finite")
        converted.append(number)
    if converted[0] > converted[1]:
        raise ProtocolError(f"{field} minimum must not exceed maximum")


def validate_result(value: Any) -> None:
    result = _object(value, field="result")
    unknown = sorted(set(result) - RESULT_FIELDS)
    if unknown:
        raise ProtocolError("result contains unknown field(s): " + ", ".join(unknown))
    if result.get("schema_version") != RESULT_SCHEMA_VERSION:
        raise ProtocolError("result.schema_version must be 1")
    if result.get("kind") != RESULT_KIND:
        raise ProtocolError(f"result.kind must be {RESULT_KIND!r}")
    status = result.get("status")
    if status not in RESULT_STATUSES:
        raise ProtocolError("result.status is invalid")
    _nonempty_string(result.get("request_id"), field="result.request_id")
    if status.startswith("failed") and "action" not in result:
        _require_exact_fields(
            result,
            {"schema_version", "kind", "status", "request_id", "failure"},
            source="validation failure result",
        )
    else:
        required = {
            "schema_version",
            "kind",
            "status",
            "request_id",
            "action",
            "request_sha256",
            "casa",
            "dataset",
            "selections",
            "limits",
        }
        missing = sorted(required - set(result))
        if missing:
            raise ProtocolError("result missing field(s): " + ", ".join(missing))
        _sha256_value(result["request_sha256"], field="result.request_sha256")
    if status in {"completed", "failed_postcondition"}:
        if "geometry" not in result or "selection_results" not in result:
            raise ProtocolError(
                f"{status} result requires geometry and selection_results"
            )
        validate_inspection(
            {
                "geometry": result["geometry"],
                "selection_results": result["selection_results"],
            }
        )
    if status.startswith("failed"):
        _object(result.get("failure"), field="result.failure")
    elif "failure" in result:
        raise ProtocolError("non-failed result must not contain failure")
    canonical_json_bytes(result)


def failure_result(
    *, request_id: str, status: str, kind: str, reason: str, exception_type: str
) -> dict[str, Any]:
    result = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "kind": RESULT_KIND,
        "status": status,
        "request_id": request_id or "unknown",
        "failure": {
            "kind": kind,
            "reason": reason,
            "exception_type": exception_type,
        },
    }
    validate_result(result)
    return result


def representative_indices(row_count: int, limit: int) -> list[int]:
    """Return deterministic, endpoint-preserving, bounded row indices."""

    if row_count < 0 or limit <= 0:
        raise ProtocolError("row_count must be non-negative and limit positive")
    if row_count <= limit:
        return list(range(row_count))
    if limit == 1:
        return [0]
    return sorted(
        {(position * (row_count - 1)) // (limit - 1) for position in range(limit)}
    )


def first_direction(value: Any) -> dict[str, Any]:
    """Summarize one CASA direction cell without retaining the full cell."""

    shape = _nested_shape(value)
    if hasattr(value, "shape") and hasattr(value, "tolist"):
        shape = [int(item) for item in value.shape]
        value = value.tolist()
    try:
        if len(shape) >= 2 and shape[0] >= 2:
            pair = [float(value[0][0]), float(value[1][0])]
        else:
            pair = [float(value[0]), float(value[1])]
    except (IndexError, KeyError, TypeError, ValueError) as error:
        raise ProtocolError(f"direction cell has no numeric coordinate pair: {error}")
    if not all(math.isfinite(item) for item in pair):
        raise ProtocolError("direction cell contains non-finite coordinates")
    return {"shape": shape, "first_direction_rad": pair}


def _inspect_main(table_factory, np, path, block_rows: int, sample_limit: int):
    handle = _open_table(table_factory, path)
    try:
        row_count = int(handle.nrows())
        columns = sorted(str(item) for item in handle.colnames())
        numeric = {
            "time_seconds": _NumericAccumulator(),
            "interval_seconds": _NumericAccumulator(),
            "u_meters": _NumericAccumulator(),
            "v_meters": _NumericAccumulator(),
            "w_meters": _NumericAccumulator(),
        }
        integers = {
            "field_id": _IntegerAccumulator(sample_limit),
            "data_desc_id": _IntegerAccumulator(sample_limit),
            "scan_number": _IntegerAccumulator(sample_limit),
        }
        blocks = 0
        for start in range(0, row_count, block_rows):
            count = min(block_rows, row_count - start)
            numeric["time_seconds"].add(handle.getcol("TIME", start, count), np)
            numeric["interval_seconds"].add(handle.getcol("INTERVAL", start, count), np)
            integers["field_id"].add(handle.getcol("FIELD_ID", start, count), np)
            integers["data_desc_id"].add(
                handle.getcol("DATA_DESC_ID", start, count), np
            )
            integers["scan_number"].add(handle.getcol("SCAN_NUMBER", start, count), np)
            uvw = np.asarray(handle.getcol("UVW", start, count), dtype=np.float64)
            if uvw.ndim != 2 or uvw.shape[0] != 3:
                raise ProtocolError(f"MAIN.UVW has unexpected block shape {uvw.shape}")
            numeric["u_meters"].add(uvw[0, :], np)
            numeric["v_meters"].add(uvw[1, :], np)
            numeric["w_meters"].add(uvw[2, :], np)
            blocks += 1
    finally:
        _close_casa_tool(handle)
    return {
        "row_count": row_count,
        "column_names": columns,
        "scan": {"block_rows": block_rows, "blocks_read": blocks},
        "time_seconds": numeric["time_seconds"].result(),
        "interval_seconds": numeric["interval_seconds"].result(),
        "field_id": integers["field_id"].result(),
        "data_desc_id": integers["data_desc_id"].result(),
        "scan_number": integers["scan_number"].result(),
        "uvw_meters": {
            name[0]: numeric[name].result()
            for name in ("u_meters", "v_meters", "w_meters")
        },
    }


def _inspect_field(table_factory, np, path, focus, limit):
    handle = _open_table(table_factory, path / "FIELD")
    try:
        count = int(handle.nrows())
        sample_ids = _focused_indices(count, focus, limit)
        columns = sorted(str(item) for item in handle.colnames())
        rows = []
        for row_id in sample_ids:
            row = {
                "row_id": row_id,
                "name": str(handle.getcell("NAME", row_id)),
                "phase_dir": first_direction(handle.getcell("PHASE_DIR", row_id)),
                "delay_dir": first_direction(handle.getcell("DELAY_DIR", row_id)),
                "reference_dir": first_direction(
                    handle.getcell("REFERENCE_DIR", row_id)
                ),
            }
            reference_columns = {
                "phase": "PhaseDir_Ref",
                "delay": "DelayDir_Ref",
                "reference": "RefDir_Ref",
            }
            if all(name in columns for name in reference_columns.values()):
                row["direction_reference_codes"] = {
                    name: int(handle.getcell(column, row_id))
                    for name, column in reference_columns.items()
                }
            rows.append(row)
        reference = _measure_reference(handle, "PHASE_DIR")
    finally:
        _close_casa_tool(handle)
    return {
        "row_count": count,
        "column_names": columns,
        "direction_reference": reference,
        "sampling": _sampling_receipt(count, sample_ids, limit, focus),
        "rows": rows,
    }


def _inspect_spectral_window(table_factory, np, path, focus, limit):
    handle = _open_table(table_factory, path / "SPECTRAL_WINDOW")
    try:
        count = int(handle.nrows())
        sample_ids = _focused_indices(count, focus, limit)
        rows = []
        for row_id in sample_ids:
            frequencies = np.asarray(
                handle.getcell("CHAN_FREQ", row_id), dtype=np.float64
            ).reshape(-1)
            widths = np.asarray(
                handle.getcell("CHAN_WIDTH", row_id), dtype=np.float64
            ).reshape(-1)
            num_chan = int(handle.getcell("NUM_CHAN", row_id))
            if frequencies.size != num_chan or widths.size != num_chan:
                raise ProtocolError(
                    f"SPECTRAL_WINDOW row {row_id} channel geometry is inconsistent"
                )
            rows.append(
                {
                    "row_id": row_id,
                    "name": str(handle.getcell("NAME", row_id)),
                    "num_channels": num_chan,
                    "channel_frequency_hz": _array_summary(frequencies, np),
                    "channel_width_hz": _array_summary(widths, np),
                    "reference_frequency_hz": float(
                        handle.getcell("REF_FREQUENCY", row_id)
                    ),
                    "total_bandwidth_hz": float(
                        handle.getcell("TOTAL_BANDWIDTH", row_id)
                    ),
                }
            )
        columns = sorted(str(item) for item in handle.colnames())
    finally:
        _close_casa_tool(handle)
    return {
        "row_count": count,
        "column_names": columns,
        "sampling": _sampling_receipt(count, sample_ids, limit, focus),
        "rows": rows,
    }


def _inspect_data_description(table_factory, path, focus, focus_spws, limit):
    handle = _open_table(table_factory, path / "DATA_DESCRIPTION")
    try:
        count = int(handle.nrows())
        relevant = {int(item) for item in focus}
        selected_spws = {int(item) for item in focus_spws}
        if selected_spws:
            for row_id in range(count):
                if int(handle.getcell("SPECTRAL_WINDOW_ID", row_id)) in selected_spws:
                    relevant.add(row_id)
                    if len(relevant) > limit:
                        raise ProtocolError(
                            "selected DATA_DESCRIPTION rows exceed "
                            "max_representative_rows"
                        )
        sample_ids = _focused_indices(count, relevant, limit)
        rows = [
            {
                "row_id": row_id,
                "spectral_window_id": int(handle.getcell("SPECTRAL_WINDOW_ID", row_id)),
                "polarization_id": int(handle.getcell("POLARIZATION_ID", row_id)),
                "flag_row": bool(handle.getcell("FLAG_ROW", row_id)),
            }
            for row_id in sample_ids
        ]
        columns = sorted(str(item) for item in handle.colnames())
    finally:
        _close_casa_tool(handle)
    return {
        "row_count": count,
        "column_names": columns,
        "sampling": _sampling_receipt(count, sample_ids, limit, focus),
        "rows": rows,
    }


def _inspect_polarization(table_factory, np, path, focus, limit):
    handle = _open_table(table_factory, path / "POLARIZATION")
    try:
        count = int(handle.nrows())
        sample_ids = _focused_indices(count, focus, limit)
        rows = []
        for row_id in sample_ids:
            corr_types = _integer_values(handle.getcell("CORR_TYPE", row_id), np)
            num_corr = int(handle.getcell("NUM_CORR", row_id))
            if len(corr_types) != num_corr:
                raise ProtocolError(
                    f"POLARIZATION row {row_id} correlation geometry is inconsistent"
                )
            rows.append(
                {
                    "row_id": row_id,
                    "num_correlations": num_corr,
                    "correlation_types": corr_types,
                    "correlation_names": [
                        _STOKES_NAMES.get(value, f"UNKNOWN_{value}")
                        for value in corr_types
                    ],
                    "correlation_product_shape": [
                        int(item)
                        for item in np.asarray(
                            handle.getcell("CORR_PRODUCT", row_id)
                        ).shape
                    ],
                }
            )
        columns = sorted(str(item) for item in handle.colnames())
    finally:
        _close_casa_tool(handle)
    return {
        "row_count": count,
        "column_names": columns,
        "sampling": _sampling_receipt(count, sample_ids, limit, focus),
        "rows": rows,
    }


def _inspect_pointing(table_factory, np, path, block_rows, limit):
    handle = _open_table(table_factory, path / "POINTING")
    try:
        count = int(handle.nrows())
        columns = sorted(str(item) for item in handle.colnames())
        time = _NumericAccumulator()
        interval = _NumericAccumulator()
        antenna = _IntegerAccumulator(limit)
        blocks = 0
        for start in range(0, count, block_rows):
            size = min(block_rows, count - start)
            time.add(handle.getcol("TIME", start, size), np)
            interval.add(handle.getcol("INTERVAL", start, size), np)
            antenna.add(handle.getcol("ANTENNA_ID", start, size), np)
            blocks += 1
        sample_ids = representative_indices(count, limit)
        samples = []
        for row_id in sample_ids:
            sample = {
                "row_id": row_id,
                "antenna_id": int(handle.getcell("ANTENNA_ID", row_id)),
                "time_seconds": float(handle.getcell("TIME", row_id)),
                "interval_seconds": float(handle.getcell("INTERVAL", row_id)),
                "direction": first_direction(handle.getcell("DIRECTION", row_id)),
            }
            if "TARGET" in columns:
                sample["target"] = first_direction(handle.getcell("TARGET", row_id))
            samples.append(sample)
        reference = _measure_reference(handle, "DIRECTION")
    finally:
        _close_casa_tool(handle)
    return {
        "row_count": count,
        "column_names": columns,
        "scan": {"block_rows": block_rows, "blocks_read": blocks},
        "time_seconds": time.result(),
        "interval_seconds": interval.result(),
        "antenna_id": antenna.result(),
        "direction_reference": reference,
        "sampling": _sampling_receipt(count, sample_ids, limit, set()),
        "rows": samples,
    }


class _NumericAccumulator:
    def __init__(self) -> None:
        self.count = 0
        self.minimum: float | None = None
        self.maximum: float | None = None

    def add(self, values: Any, np) -> None:
        array = np.asarray(values, dtype=np.float64).reshape(-1)
        if array.size == 0:
            return
        if not bool(np.all(np.isfinite(array))):
            raise ProtocolError("MeasurementSet geometry contains non-finite values")
        minimum = float(array.min())
        maximum = float(array.max())
        self.minimum = minimum if self.minimum is None else min(self.minimum, minimum)
        self.maximum = maximum if self.maximum is None else max(self.maximum, maximum)
        self.count += int(array.size)

    def result(self) -> dict[str, Any]:
        return {"count": self.count, "min": self.minimum, "max": self.maximum}


class _IntegerAccumulator:
    def __init__(self, distinct_limit: int) -> None:
        self.count = 0
        self.minimum: int | None = None
        self.maximum: int | None = None
        self.values: set[int] = set()
        self.distinct_limit = distinct_limit
        self.truncated = False

    def add(self, values: Any, np) -> None:
        array = np.asarray(values, dtype=np.int64).reshape(-1)
        if array.size == 0:
            return
        minimum = int(array.min())
        maximum = int(array.max())
        self.minimum = minimum if self.minimum is None else min(self.minimum, minimum)
        self.maximum = maximum if self.maximum is None else max(self.maximum, maximum)
        self.count += int(array.size)
        for value in array:
            integer = int(value)
            if integer in self.values:
                continue
            if len(self.values) < self.distinct_limit:
                self.values.add(integer)
            else:
                self.truncated = True

    def result(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "min": self.minimum,
            "max": self.maximum,
            "representative_values": sorted(self.values),
            "representative_values_truncated": self.truncated,
        }


def _focused_indices(row_count: int, focus: Iterable[int], limit: int) -> list[int]:
    focused = sorted({int(item) for item in focus if 0 <= int(item) < row_count})
    if len(focused) > limit:
        raise ProtocolError(
            "selected focus rows exceed max_representative_rows; increase the "
            "explicit request limit instead of truncating required geometry"
        )
    remaining = limit - len(focused)
    if remaining == 0:
        return focused
    general = representative_indices(row_count, min(row_count, remaining))
    result = sorted(set(focused).union(general))
    if len(result) <= limit:
        return result
    # Overlap can only reduce the count, but retain a defensive bound.
    return result[:limit]


def _sampling_receipt(row_count, sample_ids, limit, focus):
    in_range_focus = {int(item) for item in focus if 0 <= int(item) < row_count}
    return {
        "method": "selected-focus-plus-evenly-spaced-endpoints",
        "limit": limit,
        "sample_count": len(sample_ids),
        "row_count": row_count,
        "focus_row_count": len(in_range_focus),
        "truncated": len(sample_ids) < row_count,
    }


def _array_summary(value, np):
    accumulator = _NumericAccumulator()
    accumulator.add(value, np)
    result = accumulator.result()
    array = np.asarray(value, dtype=np.float64).reshape(-1)
    result["first"] = float(array[0]) if array.size else None
    result["last"] = float(array[-1]) if array.size else None
    return result


def _range_pair(value, np):
    array = np.asarray(value, dtype=np.float64).reshape(-1)
    if array.size == 0:
        return None
    if not bool(np.all(np.isfinite(array))):
        raise ProtocolError("CASA range contains non-finite values")
    return [float(array.min()), float(array.max())]


def _integer_values(value, np):
    return sorted({int(item) for item in np.asarray(value).reshape(-1)})


def _bounded_json_array(value, np, limit):
    array = np.asarray(value)
    if array.size == 0:
        return {
            "shape": [int(item) for item in array.shape],
            "rows": [],
            "truncated": False,
        }
    if array.ndim == 1:
        indices = representative_indices(
            int(array.shape[0]), min(int(array.shape[0]), limit)
        )
        rows = [[_json_scalar(array[index])] for index in indices]
    else:
        leading = int(array.shape[0])
        rows = [
            [_json_scalar(item) for item in np.asarray(array[index]).reshape(-1)]
            for index in representative_indices(leading, min(leading, limit))
        ]
    return {
        "shape": [int(item) for item in array.shape],
        "rows": rows,
        "truncated": int(array.shape[0]) > limit,
    }


def _json_scalar(value):
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, (bool, int, str)) or value is None:
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ProtocolError("CASA array contains a non-finite value")
        return value
    return str(value)


def _nested_shape(value: Any) -> list[int]:
    shape: list[int] = []
    current = value
    while isinstance(current, (list, tuple)):
        shape.append(len(current))
        if not current:
            break
        current = current[0]
    return shape


def _measure_reference(handle, column):
    empty = {
        "type": None,
        "fixed_reference": None,
        "variable_reference_column": None,
        "reference_codes": [],
        "quantum_units": [],
    }
    try:
        keywords = handle.getcolkeywords(column)
    except Exception:
        return empty
    if not isinstance(keywords, dict):
        return empty
    measure_info = keywords.get("MEASINFO")
    if not isinstance(measure_info, dict):
        measure_info = {}
    raw_codes = measure_info.get("TabRefCodes", [])
    raw_types = measure_info.get("TabRefTypes", [])
    if hasattr(raw_codes, "tolist"):
        raw_codes = raw_codes.tolist()
    if hasattr(raw_types, "tolist"):
        raw_types = raw_types.tolist()
    codes = list(raw_codes) if isinstance(raw_codes, (list, tuple)) else []
    names = list(raw_types) if isinstance(raw_types, (list, tuple)) else []
    if len(codes) != len(names):
        raise ProtocolError(f"{column} direction reference code/name counts differ")
    raw_units = keywords.get("QuantumUnits", [])
    if hasattr(raw_units, "tolist"):
        raw_units = raw_units.tolist()
    units = list(raw_units) if isinstance(raw_units, (list, tuple)) else []
    return {
        "type": (
            str(measure_info["type"]) if measure_info.get("type") is not None else None
        ),
        "fixed_reference": (
            str(measure_info["Ref"]) if measure_info.get("Ref") is not None else None
        ),
        "variable_reference_column": (
            str(measure_info["VarRefCol"])
            if measure_info.get("VarRefCol") is not None
            else None
        ),
        "reference_codes": [
            {"code": int(code), "name": str(name)} for code, name in zip(codes, names)
        ],
        "quantum_units": [str(item) for item in units],
    }


def _open_table(table_factory, path):
    handle = table_factory()
    handle.open(str(path), nomodify=True)
    return handle


def _close_casa_tool(handle) -> None:
    for name in ("done", "close"):
        method = getattr(handle, name, None)
        if method is None:
            continue
        try:
            method()
        except Exception:
            pass
        return


def _load_casa_runtime():
    import casatasks  # type: ignore[import-not-found]

    return inspect_ms_geometry, str(casatasks.version_string())


def _failed_from_plan(
    plan,
    *,
    status,
    kind,
    reason,
    exception_type,
    casa_version=None,
):
    result = dict(plan)
    result["status"] = status
    if casa_version is not None:
        result["casa"] = {
            **plan["casa"],
            "actual_version": casa_version,
        }
    result["failure"] = {
        "kind": kind,
        "reason": reason,
        "exception_type": exception_type,
    }
    validate_result(result)
    return result


def _load_json_object(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ProtocolError(f"cannot read request {path}: {error}") from error
    return _object(value, field="request")


def _write_json_atomic(path: pathlib.Path, value: Any) -> None:
    canonical_json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = pathlib.Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, allow_nan=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 2:
        print("usage: casa_vlass_dataset.py REQUEST.json RESULT.json", file=sys.stderr)
        return 2
    request_path = pathlib.Path(arguments[0])
    result_path = pathlib.Path(arguments[1])
    request_id = "unknown"
    try:
        request = _load_json_object(request_path)
        raw_request_id = request.get("request_id")
        if isinstance(raw_request_id, str) and raw_request_id:
            request_id = raw_request_id
        result = process_request(request)
    except ProtocolError as error:
        result = failure_result(
            request_id=request_id,
            status="failed_validation",
            kind="protocol",
            reason=str(error),
            exception_type=type(error).__name__,
        )
    except Exception as error:
        result = failure_result(
            request_id=request_id,
            status="failed_execution",
            kind="runtime",
            reason=str(error),
            exception_type=type(error).__name__,
        )
    _write_json_atomic(result_path, result)
    return 0


def _require_exact_fields(value, expected, *, source):
    unknown = sorted(set(value) - expected)
    missing = sorted(expected - set(value))
    if unknown or missing:
        raise ProtocolError(
            f"{source} field mismatch; missing={missing or 'none'}; "
            f"unknown={unknown or 'none'}"
        )


def _object(value, *, field):
    if not isinstance(value, dict):
        raise ProtocolError(f"{field} must be an object")
    return value


def _nonempty_string(value, *, field):
    if not isinstance(value, str) or not value:
        raise ProtocolError(f"{field} must be a non-empty string")
    return value


def _string_list(value, *, field):
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ProtocolError(f"{field} must be a string list")
    return value


def _integer_list(value, *, field):
    if not isinstance(value, list):
        raise ProtocolError(f"{field} must be an integer list")
    for item in value:
        _integer(item, field=field)
    return value


def _integer(value, *, field):
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProtocolError(f"{field} must be an integer")
    return value


def _absolute_path(value, *, field):
    text = _nonempty_string(value, field=field)
    path = pathlib.Path(text).expanduser()
    if not path.is_absolute():
        raise ProtocolError(f"{field} must be an absolute path")
    return path


def _positive_integer(value, *, field):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ProtocolError(f"{field} must be a positive integer")
    return value


def _nonnegative_integer(value, *, field):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProtocolError(f"{field} must be a non-negative integer")
    return value


def _sha256_value(value, *, field):
    text = _nonempty_string(value, field=field)
    if len(text) != 64 or any(
        character not in "0123456789abcdef" for character in text
    ):
        raise ProtocolError(f"{field} must be a lowercase SHA-256 digest")
    return text


_STOKES_NAMES = {
    1: "I",
    2: "Q",
    3: "U",
    4: "V",
    5: "RR",
    6: "RL",
    7: "LR",
    8: "LL",
    9: "XX",
    10: "XY",
    11: "YX",
    12: "YY",
    13: "RX",
    14: "RY",
    15: "LX",
    16: "LY",
    17: "XR",
    18: "XL",
    19: "YR",
    20: "YL",
    21: "PP",
    22: "PQ",
    23: "QP",
    24: "QQ",
}


if __name__ == "__main__":
    raise SystemExit(main())
