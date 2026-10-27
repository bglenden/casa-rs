"""casa-rs's CASA tasks take CASA's parameters (simulation Phase 5c, #841).

`casars.tasks.simobserve` takes CASA 6.7.6's `casatasks.simobserve`
parameters, by name and in CASA's order, and the catalog resolves each to
CASA's default, recorded in `data/casa_simobserve_task_signature.json` by
`tools/simulation/dump_casa_tool_signatures.py --task simobserve`. So a CASA
script runs with only its imports changed.
"""

from __future__ import annotations

import ast
import inspect
import json
from pathlib import Path

from casars import tasks
from casars.parameters import TaskParameters

DATA = Path(__file__).resolve().parent / "data"


def casa_signature(task: str) -> list[dict[str, str]]:
    record = json.loads((DATA / f"casa_{task}_task_signature.json").read_text(encoding="utf-8"))
    assert record["casa_version"].startswith("6.7.6")
    return record["parameters"]


def test_simobserve_takes_casas_parameters_in_casas_order() -> None:
    casa = [parameter["name"] for parameter in casa_signature("simobserve")]
    ours = list(inspect.signature(tasks.simobserve).parameters)
    assert ours[: len(casa)] == casa


def test_simobserve_defaults_are_casas(tmp_path: Path) -> None:
    defaults = TaskParameters.defaults("simobserve", workspace=tmp_path)
    for parameter in casa_signature("simobserve"):
        expected = ast.literal_eval(parameter["default"])
        value = defaults[parameter["name"]]
        if isinstance(expected, list):
            value = list(value)
        assert value == expected, parameter["name"]
