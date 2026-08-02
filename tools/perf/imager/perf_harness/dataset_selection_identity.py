# SPDX-License-Identifier: LGPL-3.0-or-later
"""Strict binding between frozen MS geometry and one imaging selection."""

from __future__ import annotations

import copy
import math
import re
from typing import Any

from .errors import HarnessError


IDENTITY_KIND = "frozen_dataset_geometry_identity"
ROOT_FIELDS = {
    "schema_version",
    "kind",
    "dataset",
    "geometry",
    "selections",
    "source_receipts",
}
DATASET_FIELDS = {"archive_sha256", "tree_sha256", "file_count", "size_bytes"}
GEOMETRY_FIELDS = {
    "correlations",
    "data_description_ids",
    "field_groups",
    "field_row_count",
    "main_row_count",
    "pointing_row_count",
    "spectral_windows",
}
FIELD_GROUP_FIELDS = {"id", "field"}
SPECTRAL_WINDOW_FIELDS = {
    "id",
    "channels",
    "first_hz",
    "last_hz",
    "width_hz",
}


SELECTION_FIELDS = {
    "field",
    "spw",
    "uvrange",
    "intent",
    "selected_rows",
    "spw_ids",
    "channel_start",
    "channel_count",
    "correlations",
}
SOURCE_RECEIPT_FIELDS = {"dataset_receipt_sha256", "geometry_receipts"}
GEOMETRY_RECEIPT_FIELDS = {"request_sha256", "result_sha256"}
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


def validate_frozen_dataset_geometry_identity(document: dict[str, Any]) -> None:
    """Validate the complete, generic frozen geometry identity contract."""

    _require_exact_fields(document, ROOT_FIELDS, label="frozen dataset geometry")
    if (
        isinstance(document["schema_version"], bool)
        or not isinstance(document["schema_version"], int)
        or document["schema_version"] != 1
    ):
        raise HarnessError("frozen dataset geometry schema_version must be 1")
    if document["kind"] != IDENTITY_KIND:
        raise HarnessError(f"frozen dataset geometry kind must be {IDENTITY_KIND!r}")

    dataset = _require_object(document["dataset"], label="dataset")
    _require_exact_fields(dataset, DATASET_FIELDS, label="dataset")
    for name in ("archive_sha256", "tree_sha256"):
        _require_sha256(dataset[name], label=f"dataset.{name}")
    _require_positive_int(dataset["file_count"], label="dataset.file_count")
    _require_positive_int(dataset["size_bytes"], label="dataset.size_bytes")

    geometry = _require_object(document["geometry"], label="geometry")
    _require_exact_fields(geometry, GEOMETRY_FIELDS, label="geometry")
    correlations = _require_string_list(
        geometry["correlations"], label="geometry.correlations", allow_empty=False
    )
    _require_unique(correlations, label="geometry.correlations")
    data_description_ids = _require_int_list(
        geometry["data_description_ids"],
        label="geometry.data_description_ids",
        allow_empty=False,
    )
    _require_unique(data_description_ids, label="geometry.data_description_ids")
    _require_positive_int(geometry["field_row_count"], label="geometry.field_row_count")
    _require_positive_int(geometry["main_row_count"], label="geometry.main_row_count")
    _require_nonnegative_int(
        geometry["pointing_row_count"], label="geometry.pointing_row_count"
    )
    field_groups = geometry["field_groups"]
    if not isinstance(field_groups, list):
        raise HarnessError("geometry.field_groups must be an array")
    field_group_ids: list[str] = []
    for index, value in enumerate(field_groups):
        label = f"geometry.field_groups[{index}]"
        field_group = _require_object(value, label=label)
        _require_exact_fields(field_group, FIELD_GROUP_FIELDS, label=label)
        field_group_ids.append(
            _require_nonempty_string(field_group["id"], label=f"{label}.id")
        )
        _require_nonempty_string(field_group["field"], label=f"{label}.field")
    _require_unique(field_group_ids, label="geometry field-group ids")

    spectral_windows = geometry["spectral_windows"]
    if not isinstance(spectral_windows, list) or not spectral_windows:
        raise HarnessError("geometry.spectral_windows must be a nonempty array")
    spectral_window_ids: list[int] = []
    channels_by_spw: dict[int, int] = {}
    for index, value in enumerate(spectral_windows):
        label = f"geometry.spectral_windows[{index}]"
        row = _require_object(value, label=label)
        _require_exact_fields(row, SPECTRAL_WINDOW_FIELDS, label=label)
        spw_id = _require_nonnegative_int(row["id"], label=f"{label}.id")
        channels = _require_positive_int(row["channels"], label=f"{label}.channels")
        _require_finite_number(row["first_hz"], label=f"{label}.first_hz")
        _require_finite_number(row["last_hz"], label=f"{label}.last_hz")
        width_hz = _require_finite_number(row["width_hz"], label=f"{label}.width_hz")
        if width_hz == 0.0:
            raise HarnessError(f"{label}.width_hz must be nonzero")
        spectral_window_ids.append(spw_id)
        channels_by_spw[spw_id] = channels
    _require_unique(spectral_window_ids, label="geometry spectral-window ids")

    selections = _require_object(document["selections"], label="selections")
    if not selections:
        raise HarnessError("selections must contain at least one named selection")
    for selection_name, value in selections.items():
        if not isinstance(selection_name, str) or not selection_name:
            raise HarnessError("selection names must be nonempty strings")
        label = f"selections.{selection_name}"
        selection = _require_object(value, label=label)
        _require_exact_fields(selection, SELECTION_FIELDS, label=label)
        for name in ("field", "spw", "uvrange", "intent"):
            _require_nonempty_string(selection[name], label=f"{label}.{name}")
        _require_positive_int(
            selection["selected_rows"], label=f"{label}.selected_rows"
        )
        spw_ids = _require_int_list(
            selection["spw_ids"], label=f"{label}.spw_ids", allow_empty=False
        )
        _require_unique(spw_ids, label=f"{label}.spw_ids")
        unknown_spws = sorted(set(spw_ids) - set(spectral_window_ids))
        if unknown_spws:
            raise HarnessError(f"{label}.spw_ids are not in geometry: {unknown_spws}")
        channel_start = _require_nonnegative_int(
            selection["channel_start"], label=f"{label}.channel_start"
        )
        channel_count = _require_positive_int(
            selection["channel_count"], label=f"{label}.channel_count"
        )
        selection_correlations = _require_string_list(
            selection["correlations"],
            label=f"{label}.correlations",
            allow_empty=False,
        )
        _require_unique(selection_correlations, label=f"{label}.correlations")
        if selection_correlations != correlations:
            raise HarnessError(f"{label}.correlations must equal geometry.correlations")
        channel_end = channel_start + channel_count
        oversized_spws = [
            spw_id for spw_id in spw_ids if channel_end > channels_by_spw[spw_id]
        ]
        if oversized_spws:
            raise HarnessError(
                f"{label} channel window exceeds SPW channel counts: {oversized_spws}"
            )

    receipts = _require_object(document["source_receipts"], label="source_receipts")
    _require_exact_fields(receipts, SOURCE_RECEIPT_FIELDS, label="source_receipts")
    _require_sha256(
        receipts["dataset_receipt_sha256"],
        label="source_receipts.dataset_receipt_sha256",
    )
    geometry_receipts = _require_object(
        receipts["geometry_receipts"], label="source_receipts.geometry_receipts"
    )
    if not geometry_receipts:
        raise HarnessError(
            "source_receipts.geometry_receipts must contain a named receipt"
        )
    for receipt_id, value in geometry_receipts.items():
        if not isinstance(receipt_id, str) or not receipt_id:
            raise HarnessError("geometry receipt ids must be nonempty strings")
        label = f"source_receipts.geometry_receipts.{receipt_id}"
        binding = _require_object(value, label=label)
        _require_exact_fields(binding, GEOMETRY_RECEIPT_FIELDS, label=label)
        for name in sorted(GEOMETRY_RECEIPT_FIELDS):
            _require_sha256(binding[name], label=f"{label}.{name}")


def _require_object(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise HarnessError(f"{label} must be an object")
    return value


def _require_exact_fields(
    value: dict[str, Any], expected: set[str], *, label: str
) -> None:
    unknown = sorted(set(value) - expected)
    missing = sorted(expected - set(value))
    if unknown or missing:
        raise HarnessError(
            f"{label} has invalid fields; "
            f"missing={missing or 'none'}; unknown={unknown or 'none'}"
        )


def _require_nonempty_string(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise HarnessError(f"{label} must be a nonempty string")
    return value


def _require_sha256(value: Any, *, label: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise HarnessError(f"{label} must be a lowercase SHA-256 digest")


def _require_nonnegative_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise HarnessError(f"{label} must be a nonnegative integer")
    return value


def _require_positive_int(value: Any, *, label: str) -> int:
    result = _require_nonnegative_int(value, label=label)
    if result == 0:
        raise HarnessError(f"{label} must be a positive integer")
    return result


def _require_finite_number(value: Any, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise HarnessError(f"{label} must be a finite number")
    result = float(value)
    if not math.isfinite(result):
        raise HarnessError(f"{label} must be a finite number")
    return result


def _require_string_list(value: Any, *, label: str, allow_empty: bool) -> list[str]:
    if (
        not isinstance(value, list)
        or (not allow_empty and not value)
        or not all(isinstance(item, str) and item for item in value)
    ):
        emptiness = "" if allow_empty else " nonempty"
        raise HarnessError(f"{label} must be a{emptiness} string array")
    return value


def _require_int_list(value: Any, *, label: str, allow_empty: bool) -> list[int]:
    if (
        not isinstance(value, list)
        or (not allow_empty and not value)
        or any(
            isinstance(item, bool) or not isinstance(item, int) or item < 0
            for item in value
        )
    ):
        emptiness = "" if allow_empty else " nonempty"
        raise HarnessError(f"{label} must be a{emptiness} nonnegative integer array")
    return value


def _require_unique(values: list[Any], *, label: str) -> None:
    if len(values) != len(set(values)):
        raise HarnessError(f"{label} must not contain duplicates")


def bind_frozen_selection(
    document: dict[str, Any],
    *,
    selection_name: str,
    imaging: dict[str, Any],
    spw_ids: list[int],
) -> dict[str, Any]:
    """Return one validated selection or fail on any semantic drift."""

    selections = document.get("selections")
    if not isinstance(selections, dict) or not selections:
        raise HarnessError("frozen dataset geometry requires named selections")
    value = selections.get(selection_name)
    if not isinstance(value, dict):
        raise HarnessError(
            f"frozen dataset selection {selection_name!r} does not exist"
        )
    unknown = sorted(set(value) - SELECTION_FIELDS)
    missing = sorted(SELECTION_FIELDS - set(value))
    if unknown or missing:
        raise HarnessError(
            f"frozen dataset selection {selection_name!r} has invalid fields; "
            f"missing={missing or 'none'}; unknown={unknown or 'none'}"
        )

    for name in ("field", "spw", "uvrange", "intent"):
        expected = value[name]
        actual = imaging.get(name)
        if not isinstance(expected, str) or not expected:
            raise HarnessError(
                f"frozen dataset selection {selection_name!r}.{name} must be nonempty"
            )
        if actual != expected:
            raise HarnessError(
                f"imaging {name}={actual!r} does not match frozen selection "
                f"{selection_name!r} value {expected!r}"
            )

    selected_rows = value["selected_rows"]
    if (
        isinstance(selected_rows, bool)
        or not isinstance(selected_rows, int)
        or selected_rows < 1
    ):
        raise HarnessError(
            f"frozen dataset selection {selection_name!r}.selected_rows must be positive"
        )
    frozen_spw_ids = value["spw_ids"]
    if (
        not isinstance(frozen_spw_ids, list)
        or any(
            isinstance(item, bool) or not isinstance(item, int)
            for item in frozen_spw_ids
        )
        or frozen_spw_ids != spw_ids
    ):
        raise HarnessError(
            f"frozen dataset selection {selection_name!r}.spw_ids do not match {spw_ids}"
        )

    for name in ("channel_start", "channel_count"):
        expected = value[name]
        actual = imaging.get(name)
        if (
            isinstance(expected, bool)
            or not isinstance(expected, int)
            or expected < (0 if name == "channel_start" else 1)
        ):
            raise HarnessError(
                f"frozen dataset selection {selection_name!r}.{name} is invalid"
            )
        if actual != expected:
            raise HarnessError(
                f"imaging {name}={actual!r} does not match frozen selection "
                f"{selection_name!r} value {expected!r}"
            )

    correlations = value["correlations"]
    geometry = document.get("geometry")
    geometry_correlations = (
        geometry.get("correlations") if isinstance(geometry, dict) else None
    )
    if (
        not isinstance(correlations, list)
        or not correlations
        or not all(isinstance(item, str) and item for item in correlations)
        or correlations != geometry_correlations
    ):
        raise HarnessError(
            f"frozen dataset selection {selection_name!r}.correlations are invalid"
        )

    spectral_windows = (
        geometry.get("spectral_windows") if isinstance(geometry, dict) else None
    )
    if not isinstance(spectral_windows, list):
        raise HarnessError("frozen dataset geometry lacks spectral-window facts")
    by_id = {
        item.get("id"): item for item in spectral_windows if isinstance(item, dict)
    }
    channel_end = value["channel_start"] + value["channel_count"]
    for spw_id in spw_ids:
        spw = by_id.get(spw_id)
        channels = spw.get("channels") if isinstance(spw, dict) else None
        if isinstance(channels, bool) or not isinstance(channels, int):
            raise HarnessError(f"frozen spectral window {spw_id} lacks channel facts")
        if channel_end > channels:
            raise HarnessError(
                f"frozen selection channels [0,{channel_end}) exceed SPW {spw_id} "
                f"channel count {channels}"
            )

    return {"name": selection_name, **copy.deepcopy(value)}
