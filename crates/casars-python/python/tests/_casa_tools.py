"""Shared checks of the `casars` CASA classes against CASA's (ADR-0019, #841).

`tools/simulation/dump_casa_tool_signatures.py` records each installed CASA
class's signatures in `data/casa_<tool>_signatures.json`; these helpers
compare a casa-rs class with that record, with a live CASA when one is
importable, and with the object-surface bundle built from the same Rust table.
"""

from __future__ import annotations

import inspect
import json
import math
from pathlib import Path
from typing import Any

from casars.data import schema_bundle

DATA = Path(__file__).parent / "data"


def casa_signatures(tool: str) -> dict[str, Any]:
    """The checked-in record of CASA's class `tool`."""
    return json.loads((DATA / f"casa_{tool}_signatures.json").read_text(encoding="utf-8"))


def public_methods(cls: type) -> set[str]:
    return {name for name in dir(cls) if not name.startswith("_") and callable(getattr(cls, name))}


def describe(function: Any) -> list[dict[str, str | None]]:
    """Parameters without `self`, as the dump script records them."""
    described = []
    for parameter in inspect.signature(function).parameters.values():
        if parameter.name == "self":
            continue
        default = None if parameter.default is inspect.Parameter.empty else repr(parameter.default)
        described.append({"name": parameter.name, "kind": parameter.kind.name, "default": default})
    return described


def assert_casa_signatures(cls: type, casa: dict[str, Any]) -> None:
    """Every CASA method exists on `cls` with CASA's signature, and no other."""
    assert public_methods(cls) == set(casa["methods"])
    for name, parameters in casa["methods"].items():
        assert describe(getattr(cls, name)) == parameters, name


def assert_live_signatures(cls: type, live: type, methods: set[str] | None = None) -> None:
    """`cls` has the live CASA class's signatures (for `methods`, or all)."""
    names = methods or {
        name for name, _ in inspect.getmembers(live, inspect.isfunction) if not name.startswith("_")
    }
    if methods is None:
        assert public_methods(cls) == names
    for name in names:
        assert describe(getattr(cls, name)) == describe(getattr(live, name)), name


def assert_bundle_describes(cls: type, name: str, statuses: dict[str, set[str]]) -> None:
    """The bundle's entry for tool `name` matches the class's signatures, and
    records which methods casa-rs implements."""
    bundle = schema_bundle()
    contract = {entry["name"]: entry for entry in bundle["semantic"]["objects"]}[name]
    projection = next(
        entry for entry in bundle["projections"]["python"]["objects"] if entry["name"] == name
    )
    assert (projection["module"], projection["class_name"]) == ("casars.tools", name)
    methods = {method["name"]: method for method in contract["methods"]}
    assert set(methods) == public_methods(cls)
    assert set(projection["method_parameters"]) == set(methods)
    for method_name, method in methods.items():
        schema = method["parameters_schema"]
        properties = schema.get("properties", {})
        order = projection["method_parameters"][method_name]
        assert set(order) == set(properties), method_name
        found = [p for p in inspect.signature(getattr(cls, method_name)).parameters.values() if p.name != "self"]
        assert [p.name for p in found] == order, method_name
        for parameter in found:
            assert parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
            if parameter.default is inspect.Parameter.empty:
                assert parameter.name in schema.get("required", []), (method_name, parameter.name)
                assert "default" not in properties[parameter.name], (method_name, parameter.name)
                continue
            default = properties[parameter.name]["default"]
            assert parameter.default == default, (method_name, parameter.name)
            assert type(parameter.default) is type(default), (method_name, parameter.name)
    status = bundle["annotations"]["python"]["objects"][name]["method_status"]
    for value, expected in statuses.items():
        assert {method for method, found in status.items() if found == value} == expected, value


def close(found: Any, expected: Any, rel: float = 0.0, abs_: float = 0.0, path: str = "") -> None:
    """`found` equals `expected`, floats within `rel` (relative) or `abs_`."""
    if isinstance(expected, dict):
        assert isinstance(found, dict) and set(found) == set(expected), (path, found, expected)
        for key in expected:
            close(found[key], expected[key], rel, abs_, f"{path}.{key}")
    elif isinstance(expected, float):
        assert isinstance(found, float), (path, found)
        assert math.isclose(found, expected, rel_tol=rel, abs_tol=abs_), (path, found, expected)
    else:
        assert found == expected, (path, found, expected)
