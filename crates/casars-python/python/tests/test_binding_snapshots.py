"""Snapshots of what the Rust binding returns to Python (#931).

`casars.parameters`, `casars.data` and `casars.tasks` reach Rust through one
native boundary. These tests pin what Python sees across it: the dictionary
shapes of `parameters.catalog()`, `definition()` and `contract_bundle()`, the
MeasurementSet plot data for every preset, the task-completion records, and
the exceptions with their messages. A change of binding mechanism is then
checked rather than asserted.

Regenerate the fixture only for an intended change, and review its diff:

    python crates/casars-python/python/tests/test_binding_snapshots.py
"""

from __future__ import annotations

import copy
import json
import math
import os
from pathlib import Path
import re
import sys
import tarfile
import tempfile
from typing import Any, Callable

import numpy as np
import pytest

from casars import data, parameters, tasks

REPO_ROOT = Path(__file__).resolve().parents[4]
FIXTURE = Path(__file__).parent / "data" / "binding_snapshots.json"
SMALL_MS_ARCHIVE = REPO_ROOT / "crates/casa-ms/tests/fixtures/mssel_test_small.ms.tgz"

# The relative tolerance of every float in the fixture.
REL_TOL = 1e-9
# The keys of `_array_summary`, in order.
ARRAY_SUMMARY_KEYS = ["count", "finite", "min", "max", "sum"]

# String fields whose values are a projected Rust enum vocabulary.
VOCABULARY_KEYS = (
    "adapter",
    "context_role",
    "dimension",
    "persistence_class",
    "resource_kind",
    "semantic_role",
    "unit_dimension",
)

# Every preset, in the order the unknown-preset error lists them.
PLOT_PRESETS = (
    "uv_coverage",
    "antenna_layout",
    "scan_timeline",
    "spectral_window_coverage",
    "phase_vs_time",
    "amplitude_phase_vs_time_stacked",
    "weight_vs_time",
    "sigma_vs_time",
    "flag_vs_time",
    "weight_spectrum_vs_time",
    "sigma_spectrum_vs_time",
    "flag_row_vs_time",
    "elevation_vs_time",
    "azimuth_vs_time",
    "hour_angle_vs_time",
    "parallactic_angle_vs_time",
    "azimuth_vs_elevation",
    "amplitude_vs_frequency",
    "amplitude_vs_channel",
    "phase_vs_channel",
    "phase_vs_frequency",
    "amplitude_vs_velocity",
    "phase_vs_velocity",
    "amplitude_vs_uv_distance",
    "amplitude_vs_time",
    "real_vs_imaginary",
)

# Request options beyond a bare preset, so each request field is exercised.
PLOT_VARIANTS = {
    "amplitude_vs_time+field1+avgtime+color_spw": {
        "preset": "amplitude_vs_time",
        "selection": {"field": "1"},
        "avgtime": 30.0,
        "color_by": "spw",
    },
    "amplitude_vs_time+spw2": {"preset": "amplitude_vs_time", "selection": {"spw": "2"}},
    "amplitude_vs_channel+avgchannel+scalar": {
        "preset": "amplitude_vs_channel",
        "avgchannel": 2,
        "scalar": True,
        "color_by": None,
    },
    "amplitude_vs_time+iteraxis_spw": {
        "preset": "amplitude_vs_time",
        "iteraxis": "spw",
    },
    "amplitude_vs_time+avgscan+avgantenna": {
        "preset": "amplitude_vs_time",
        "avgtime": 60.0,
        "avgscan": True,
        "avgantenna": True,
        "max_plot_points": 50,
    },
    "amplitude_vs_time+avgfield+avgbaseline+avgspw": {
        "preset": "amplitude_vs_time",
        "avgtime": 60.0,
        "avgfield": True,
        "avgbaseline": True,
        "avgspw": True,
        "color_by": "none",
    },
    "flagrow-vs-time alias": {"preset": "FLAGROW-vs-time"},
    "error: fully flagged spw": {"preset": "amplitude_vs_time", "selection": {"spw": "0"}},
    "error: avgscan without avgtime": {"preset": "amplitude_vs_time", "avgscan": True},
    "error: unmatched correlation": {
        "preset": "amplitude_vs_time",
        "selection": {"correlation": "RR"},
    },
    "error: color by an averaged axis": {
        "preset": "amplitude_vs_time",
        "avgtime": 60.0,
        "avgfield": True,
    },
    "error: missing data column": {"preset": "amplitude_vs_time", "data_column": "corrected"},
    "error: unknown data column": {"preset": "amplitude_vs_time", "data_column": "nope"},
}


def _merge(target: dict[str, Any], source: dict[str, Any]) -> None:
    for token, detail in source.items():
        if token not in target:
            target[token] = copy.deepcopy(detail)
        elif token == "list":
            _merge(target[token], detail)
        elif token.startswith("dict"):
            for key, field in detail.items():
                _merge(target[token][key], field)


def _shape(value: Any) -> dict[str, Any]:
    """The type set of a value.

    Scalars are tokens, a list maps to the merged type set of its elements,
    and a dictionary's token names its `kind` tag, if any, and its keys in
    order, mapping each key to the type set of its value.
    """

    if value is None:
        return {"null": None}
    if isinstance(value, bool):
        return {"bool": None}
    if isinstance(value, int):
        return {"int": None}
    if isinstance(value, float):
        return {"float": None}
    if isinstance(value, str):
        return {"str": None}
    if isinstance(value, list):
        elements: dict[str, Any] = {}
        for item in value:
            _merge(elements, _shape(item))
        return {"list": elements}
    if isinstance(value, dict):
        kind = value.get("kind")
        tag = f"[{kind}]" if isinstance(kind, str) else ""
        token = f"dict{tag}({','.join(value)})"
        return {token: {key: _shape(item) for key, item in value.items()}}
    raise AssertionError(f"unexpected {type(value).__name__} in a contract dictionary")


def _canonical(type_set: dict[str, Any]) -> dict[str, Any]:
    """Order a type set's tokens, keeping each dictionary's key order."""

    canonical: dict[str, Any] = {}
    for token in sorted(type_set):
        detail = type_set[token]
        if token == "list":
            detail = _canonical(detail)
        elif token.startswith("dict"):
            detail = {key: _canonical(field) for key, field in detail.items()}
        canonical[token] = detail
    return canonical


def _merged_shape(values: list[Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for value in values:
        _merge(merged, _shape(value))
    return _canonical(merged)


def _vocabulary(value: Any, found: dict[str, set[str]]) -> dict[str, set[str]]:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in VOCABULARY_KEYS and isinstance(item, str):
                found.setdefault(key, set()).add(item)
            _vocabulary(item, found)
    elif isinstance(value, list):
        for item in value:
            _vocabulary(item, found)
    return found


def _contract_snapshot() -> dict[str, Any]:
    catalog = parameters.catalog()
    surfaces = [surface["id"] for surface in catalog["surfaces"]]
    return {
        "catalog": _canonical(_shape(catalog)),
        "definition": _merged_shape([parameters.definition(surface) for surface in surfaces]),
        "contract_bundle": _merged_shape(
            [parameters.contract_bundle(surface) for surface in surfaces]
        ),
        "vocabulary": {
            key: sorted(values) for key, values in sorted(_vocabulary(catalog, {}).items())
        },
    }


def _state_snapshot() -> dict[str, Any]:
    """Field types of every surface's default states and diagnostics."""

    values = []
    diagnostics = []
    origins = set()
    for surface in (item["id"] for item in parameters.catalog()["surfaces"]):
        draft = parameters.defaults(surface)
        for state in draft.states.values():
            values.append(state.value)
            origins.add(state.origin.value)
            assert type(state.active) is bool
            assert type(state.required) is bool
            assert type(state.explicit) is bool
        diagnostics.extend(
            {
                "level": item.level,
                "code": item.code,
                "message": item.message,
                "parameter": item.parameter,
                "line": item.line,
                "column": item.column,
                "suggestions": list(item.suggestions),
            }
            for item in draft.diagnostics
        )
    return {
        "values": _merged_shape(values),
        "origins": sorted(origins),
        "diagnostics": _merged_shape(diagnostics),
        "diagnostic_codes": sorted({item["code"] for item in diagnostics}),
    }


def _normalize(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, str):
        for old, new in replacements.items():
            value = value.replace(old, new)
        return re.sub(r"<([\w.]+\.)?(\w+) object at 0x[0-9a-f]+>", r"<\2 object>", value)
    if isinstance(value, dict):
        return {key: _normalize(item, replacements) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(item, replacements) for item in value]
    return value


def _array_summary(array: np.ndarray) -> dict[str, Any]:
    assert isinstance(array, np.ndarray)
    assert array.dtype == np.float64
    assert array.ndim == 1
    assert array.flags.c_contiguous
    assert array.flags.writeable
    finite = array[np.isfinite(array)]
    return {
        "count": int(array.size),
        "finite": int(finite.size),
        "min": float(finite.min()) if finite.size else None,
        "max": float(finite.max()) if finite.size else None,
        "sum": float(finite.sum()) if finite.size else None,
    }


def _plot_summary(plot: data.MeasurementSetPlotData) -> dict[str, Any]:
    return {
        "title": plot.title,
        "summary": plot.summary,
        "header_lines": list(plot.header_lines),
        "show_legend": plot.show_legend,
        "measurement_set": plot.measurement_set,
        "preset": plot.preset,
        "selection_summary": plot.selection_summary,
        "panels": [
            {
                "id": panel.id,
                "title": panel.title,
                "axes": [
                    {
                        "id": axis.id,
                        "label": axis.label,
                        "unit": axis.unit,
                        "lower": axis.lower,
                        "upper": axis.upper,
                    }
                    for axis in panel.axes
                ],
                "series": [
                    {
                        "label": series.label,
                        "color_group": series.color_group,
                        "y_axis_id": series.y_axis_id,
                        "x": _array_summary(series.x),
                        "y": _array_summary(series.y),
                        "provenance_count": len(series.provenance),
                        "provenance_ends": [
                            dict(series.provenance[0]),
                            dict(series.provenance[-1]),
                        ]
                        if series.provenance
                        else [],
                    }
                    for series in panel.series
                ],
            }
            for panel in plot.panels
        ],
    }


def _outcome(call: Callable[[], Any], summarize: Callable[[Any], Any]) -> dict[str, Any]:
    try:
        result = call()
    except Exception as error:  # noqa: BLE001 - the snapshot records every failure
        return {"error": type(error).__name__, "message": str(error)}
    return {"result": summarize(result)}


def _plot_snapshot(ms_path: Path) -> dict[str, Any]:
    outcomes = {
        preset: _outcome(
            lambda preset=preset: data.measurement_set_plot(
                ms_path, preset=preset, max_plot_points=10_000
            ),
            _plot_summary,
        )
        for preset in PLOT_PRESETS
    }
    for name, options in PLOT_VARIANTS.items():
        outcomes[name] = _outcome(
            lambda options=options: data.measurement_set_plot(ms_path, **options),
            _plot_summary,
        )
    return _normalize(outcomes, {str(ms_path): "<ms>"})


def _fake_casars(path: Path, stdout: str) -> Path:
    path.write_text(
        "#!/usr/bin/env python3\n"
        f"print({stdout!r})\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def _record(value: Any) -> dict[str, Any]:
    fields = {}
    for name, item in vars(value).items():
        if isinstance(item, list):
            fields[name] = [_record(element) for element in item] if item and hasattr(
                item[0], "__dict__"
            ) else list(item)
        elif hasattr(item, "name") and hasattr(item, "value") and not isinstance(item, str):
            fields[name] = {"enum": type(item).__name__, "name": item.name, "value": item.value}
        else:
            fields[name] = item
    return {
        "class": type(value).__name__,
        "fields": fields,
        "str": str(value),
        "hashable": type(value).__hash__ is not None,
    }


def _completion_snapshot(root: Path) -> dict[str, Any]:
    (root / "out.ms").mkdir()
    binary = _fake_casars(root / "casars", json.dumps({"kind": "split", "report": {}}))
    completion = tasks.run(
        "split", workspace=root, binary=binary, vis="in.ms", outputvis="out.ms"
    )
    result = completion.result
    assert result == result
    assert completion.products == tuple(result.products)
    return _normalize(_record(result), {str(root.resolve()): "<workspace>"})


def _error_snapshot(root: Path) -> dict[str, Any]:
    class BytesPath(os.PathLike):
        def __fspath__(self) -> bytes:
            return b"image"

    draft = parameters.defaults("imstat")
    corrupt = root / "corrupt"
    (corrupt / ".casa-rs/parameters/imstat").mkdir(parents=True)
    (corrupt / ".casa-rs/parameters/imstat/last.toml").write_text(
        "not valid profile TOML", encoding="utf-8"
    )
    bad_profile = root / "bad.toml"
    bad_profile.write_text("not = [valid", encoding="utf-8")
    malformed = _fake_casars(root / "malformed-casars", "runner stdout")
    cases: dict[str, Callable[[], Any]] = {
        "definition unknown surface": lambda: parameters.definition("nope"),
        "contract_bundle unknown surface": lambda: parameters.contract_bundle("nope"),
        "documented_template unknown surface": lambda: parameters.documented_template("nope"),
        "defaults unknown surface": lambda: parameters.defaults("nope"),
        "last missing": lambda: parameters.last("imstat", workspace=root),
        "last_successful missing": lambda: parameters.last_successful("imstat", workspace=root),
        "last corrupt": lambda: parameters.last("imstat", workspace=corrupt),
        "load unparsable profile": lambda: parameters.load(bad_profile),
        "set wrong type": lambda: draft.__setitem__("imagename", 3),
        "set unknown parameter": lambda: draft.__setitem__("nonexistent", 1),
        "set bytes path": lambda: draft.__setitem__("imagename", BytesPath()),
        "set bytes": lambda: draft.__setitem__("imagename", b"image"),
        "set table with a non-string key": lambda: draft.__setitem__("imagename", {1: "a"}),
        "set table value": lambda: draft.__setitem__("imagename", {"a": 1}),
        "reset unknown parameter": lambda: draft.reset("nonexistent"),
        "plot missing measurement set": lambda: data.measurement_set_plot("missing.ms"),
        "plot unknown preset": lambda: data.measurement_set_plot("missing.ms", preset="nope"),
        "plot unknown preset with a quote": lambda: data.measurement_set_plot(
            "missing.ms", preset="it's"
        ),
        "malformed successful task result": lambda: tasks.imager(
            vis="input.ms", imagename="dirty", workspace=root, binary=malformed
        ),
    }
    snapshot = {
        name: _outcome(call, lambda result: type(result).__name__) for name, call in cases.items()
    }
    return _normalize(snapshot, {str(root.resolve()): "<workspace>", str(root): "<workspace>"})


def _unpack_small_ms(root: Path) -> Path:
    with tarfile.open(SMALL_MS_ARCHIVE, "r:gz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(root, **extract)
    return root / "mssel_test_small.ms"


def _current_snapshot(root: Path) -> dict[str, Any]:
    (root / "plot").mkdir()
    (root / "completion").mkdir()
    (root / "errors").mkdir()
    return {
        "contract": _contract_snapshot(),
        "states": _state_snapshot(),
        "plots": _plot_snapshot(_unpack_small_ms(root / "plot")),
        "completion": _completion_snapshot(root / "completion"),
        "errors": _error_snapshot(root / "errors"),
    }


def _abs_tol(summary: dict[str, Any], key: str) -> float:
    """The absolute tolerance for the float `summary[key]`.

    A sum's rounding error scales with its terms, not with the sum, so a
    series sum is compared at `REL_TOL` of `finite * max(|min|, |max|)`, which
    bounds the sum of the terms' magnitudes. Where the terms cancel, a
    platform difference far below `REL_TOL` in each term is a large relative
    error in the sum: the antenna-layout north offsets, of thousands of
    metres, sum to 0.02 m, and macOS's libm `cos`, one ulp from glibc's,
    moves each offset by 1e-13 of itself and the sum by 2e-7.
    """

    if key == "sum" and list(summary) == ARRAY_SUMMARY_KEYS and summary["finite"]:
        return REL_TOL * summary["finite"] * max(abs(summary["min"]), abs(summary["max"]))
    return 1e-12


def _assert_matches(actual: Any, expected: Any, path: str = "$", abs_tol: float = 1e-12) -> None:
    if isinstance(expected, float) and isinstance(actual, (int, float)):
        assert math.isclose(actual, expected, rel_tol=REL_TOL, abs_tol=abs_tol), (
            f"{path}: {actual!r} != {expected!r}"
        )
    elif isinstance(expected, dict):
        assert isinstance(actual, dict), f"{path}: {actual!r} is not a dictionary"
        assert list(actual) == list(expected), f"{path}: keys {list(actual)} != {list(expected)}"
        for key, item in expected.items():
            _assert_matches(actual[key], item, f"{path}.{key}", _abs_tol(expected, key))
    elif isinstance(expected, list):
        assert isinstance(actual, list), f"{path}: {actual!r} is not a list"
        assert len(actual) == len(expected), f"{path}: {len(actual)} != {len(expected)} items"
        for index, (item, expected_item) in enumerate(zip(actual, expected)):
            _assert_matches(item, expected_item, f"{path}[{index}]")
    else:
        assert actual == expected, f"{path}: {actual!r} != {expected!r}"


@pytest.fixture(scope="module")
def snapshot(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    # Round-trip through JSON so the comparison sees the fixture's types.
    current = _current_snapshot(tmp_path_factory.mktemp("binding-snapshots"))
    return json.loads(json.dumps(current))


@pytest.fixture(scope="module")
def expected() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


@pytest.mark.parametrize("section", ["catalog", "definition", "contract_bundle", "vocabulary"])
def test_contract_dictionaries_keep_their_shape(
    snapshot: dict[str, Any], expected: dict[str, Any], section: str
) -> None:
    _assert_matches(snapshot["contract"][section], expected["contract"][section])


def test_definitions_and_bundles_agree_with_the_catalog() -> None:
    catalog = parameters.catalog()
    concepts = {concept["id"]: concept for concept in catalog["catalog"]["concepts"]}
    for surface in catalog["surfaces"]:
        assert parameters.definition(surface["id"]) == surface
        bundle = parameters.contract_bundle(surface["id"])
        assert bundle["surface"] == surface
        assert bundle["schema_version"] == catalog["schema_version"]
        for concept in bundle["catalog"]["concepts"]:
            assert concept == concepts[concept["id"]]


def test_parameter_states_keep_their_types(
    snapshot: dict[str, Any], expected: dict[str, Any]
) -> None:
    _assert_matches(snapshot["states"], expected["states"])


@pytest.mark.parametrize("case", [*PLOT_PRESETS, *PLOT_VARIANTS])
def test_measurement_set_plots_are_unchanged(
    snapshot: dict[str, Any], expected: dict[str, Any], case: str
) -> None:
    _assert_matches(snapshot["plots"][case], expected["plots"][case], case)


def test_a_series_sum_is_compared_at_the_precision_of_its_terms() -> None:
    expected = {"count": 3, "finite": 2, "min": -1000.0, "max": 1000.0, "sum": 0.02}
    _assert_matches({**expected, "sum": 0.02 + 1e-7}, expected)
    with pytest.raises(AssertionError, match=r"^\$\.sum"):
        _assert_matches({**expected, "sum": 0.02 + 3e-6}, expected)
    with pytest.raises(AssertionError, match=r"^\$\.min"):
        _assert_matches({**expected, "min": -1000.0 * (1 + 1e-8)}, expected)
    with pytest.raises(AssertionError, match=r"^\$\.sum"):
        _assert_matches({"sum": 0.02 + 1e-7}, {"sum": 0.02})
    empty = {"count": 1, "finite": 0, "min": None, "max": None, "sum": None}
    _assert_matches(dict(empty), empty)


def test_plot_arrays_outlive_the_rust_result() -> None:
    with tempfile.TemporaryDirectory() as root:
        plot = data.measurement_set_plot(_unpack_small_ms(Path(root)), max_plot_points=10_000)
    series = plot.panels[0].series[0]
    x = series.x
    del plot, series
    x[0] = 1.0
    assert x[0] == 1.0


def test_task_completion_records_are_unchanged(
    snapshot: dict[str, Any], expected: dict[str, Any]
) -> None:
    _assert_matches(snapshot["completion"], expected["completion"])


@pytest.mark.parametrize(
    "case",
    [
        "definition unknown surface",
        "contract_bundle unknown surface",
        "documented_template unknown surface",
        "defaults unknown surface",
        "last missing",
        "last_successful missing",
        "last corrupt",
        "load unparsable profile",
        "set wrong type",
        "set unknown parameter",
        "set bytes path",
        "set bytes",
        "set table with a non-string key",
        "set table value",
        "reset unknown parameter",
        "plot missing measurement set",
        "plot unknown preset",
        "plot unknown preset with a quote",
        "malformed successful task result",
    ],
)
def test_errors_keep_their_type_and_message(
    snapshot: dict[str, Any], expected: dict[str, Any], case: str
) -> None:
    _assert_matches(snapshot["errors"][case], expected["errors"][case], case)


def test_an_unsupported_parameter_value_names_itself() -> None:
    draft = parameters.defaults("imstat")
    value = object()
    with pytest.raises(TypeError) as error:
        draft["imagename"] = value
    assert str(error.value) == f"unsupported parameter value {value!r}"
    assert "imagename" not in draft or draft.origins["imagename"].value == "default"


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"avgchannel": -1}, ValueError),
        ({"max_plot_points": -1}, ValueError),
        ({"max_plot_points": 2**64}, ValueError),
        ({"avgchannel": "2"}, TypeError),
        ({"avgchannel": 2.5}, TypeError),
        ({"avgtime": "x"}, TypeError),
        ({"data_column": None}, TypeError),
        ({"color_by": 3}, TypeError),
        ({"iteraxis": 3}, TypeError),
        ({"selection": {"field": 3}}, TypeError),
    ],
)
def test_plot_arguments_of_the_wrong_type_or_range_are_refused(
    options: dict[str, Any], error: type[Exception]
) -> None:
    with pytest.raises(error):
        data.measurement_set_plot("missing.ms", **options)


@pytest.mark.parametrize(
    "options",
    [
        {"avgscan": 1},
        {"avgscan": 0},
        {"avgtime": 3},
        {"avgtime": np.float32(3.0)},
        {"avgchannel": np.int64(2)},
        {"max_plot_points": np.int64(10)},
    ],
)
def test_plot_arguments_python_coerces_are_accepted(options: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="is not a MeasurementSet directory"):
        data.measurement_set_plot("missing.ms", **options)


def regenerate() -> None:
    with tempfile.TemporaryDirectory() as root:
        current = _current_snapshot(Path(root))
    FIXTURE.write_text(json.dumps(current, indent=1, sort_keys=False) + "\n", encoding="utf-8")
    print(f"wrote {FIXTURE}")


if __name__ == "__main__":
    regenerate()
