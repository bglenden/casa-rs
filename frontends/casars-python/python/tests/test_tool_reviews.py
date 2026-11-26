"""The reviewed CASA-equivalence files of the tools agree with the bindings (#1053).

`resources/casa-compat/semantics/tools/<tool>.json` gives each method of a
CASA class a status, which the generated reference
(`docs/reference/casa-compatibility.md`) publishes. `scripts/` checks a file's
form without building casa-rs; this checks its statuses against the built
classes: the object-surface bundle's method status for the CASA tools, and
for `simutil`, which is not in the bundle, the stubs' own docstring. A
method reviewed as not implemented raises `NotImplementedError` naming the
issue the review gives.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from casars import simutil, tools
from casars.data import schema_bundle

REVIEWS = Path(__file__).resolve().parents[4] / "resources" / "casa-compat" / "semantics" / "tools"
FILES = sorted(REVIEWS.glob("*.json"))
# The bundle's method status for each reviewed status.
BUNDLE_STATUS = {
    "Implemented": "implemented",
    "Partial": "implemented",
    "Not implemented": "not_yet_implemented",
    "Disabled in CASA": "disabled_in_casa",
    "Deprecated in CASA": "deprecated_in_casa",
}
# The docstring of a generated stub (`src/tools/<tool>/stubs.rs`).
STUB = "Not yet implemented in casa-rs."


def review(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def instance(tool: str) -> Any:
    return simutil.simutil() if tool == "simutil" else getattr(tools, tool)()


def test_the_simulation_classes_are_reviewed() -> None:
    assert {path.stem for path in FILES} >= {"simulator", "componentlist", "measures", "quanta", "simutil"}


@pytest.mark.parametrize("path", FILES, ids=lambda path: path.stem)
def test_a_reviewed_status_is_the_bindings(path: Path) -> None:
    data = review(path)
    tool = data["tool"]
    methods = data["methods"]
    if tool == "simutil":
        cls = simutil.simutil
        for name, entry in methods.items():
            stubbed = (getattr(cls, name).__doc__ or "").strip().startswith(STUB)
            assert stubbed == (entry["status"] == "Not implemented"), name
        return
    status = schema_bundle()["annotations"]["python"]["objects"][tool]["method_status"]
    assert set(status) == set(methods)
    for name, entry in methods.items():
        assert status[name] == BUNDLE_STATUS[entry["status"]], (name, entry["status"], status[name])


@pytest.mark.parametrize("path", FILES, ids=lambda path: path.stem)
def test_a_method_reviewed_as_not_implemented_refuses_naming_its_issue(path: Path) -> None:
    data = review(path)
    tool = data["tool"]
    signatures = json.loads(
        (Path(__file__).resolve().parent / "data" / f"casa_{tool}_signatures.json").read_text(encoding="utf-8")
    )
    try:
        obj = instance(tool)
    except RuntimeError as error:
        pytest.skip(f"{tool} needs its data: {error}")
    for name, entry in data["methods"].items():
        if entry["status"] != "Not implemented":
            continue
        required = [object()] * sum(p["default"] is None for p in signatures["methods"][name])
        with pytest.raises(NotImplementedError, match=rf"\(#{entry['issue']}\)$"):
            getattr(obj, name)(*required)
