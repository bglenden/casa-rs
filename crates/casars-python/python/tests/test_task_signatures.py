"""casa-rs's CASA tasks present CASA's signatures (#931).

Every task that mimics a CASA task takes CASA 6.7.6.14's parameters by name,
in CASA's order, positionally, with the defaults CASA's signature shows,
then casa-rs's own controls as keyword-only arguments, and returns CASA's
value. The CASA signatures are the pinned inventory in
`resources/casa-compat/casa-6.7.6.14/` (#931 PR 4a). A CASA-named task that
is neither checked here nor listed as pending fails, so a new one cannot
slip in with a casa-rs signature. So a CASA script runs with only its
imports changed.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
import sys

import pytest

from casars import tasks

REPO = Path(__file__).resolve().parents[4]
INVENTORY = REPO / "resources" / "casa-compat" / "casa-6.7.6.14" / "tasks"
SURFACES = REPO / "crates" / "casa-provider-contracts" / "resources" / "parameter-surfaces.json"

# casa-rs tasks with no CASA counterpart, outside the mimic rule.
CASA_RS_ONLY = {"calibrate", "msexplore"}
# CASA-named tasks not yet CASA-exact: each family's PR in #931 empties its
# entries, and plotms waits for its own decision. This list only shrinks.
PENDING = {
    "applycal": "#931 PR 4d",
    "bandpass": "#931 PR 4d",
    "flagdata": "#931 PR 4c",
    "flagmanager": "#931 PR 4c",
    "fluxscale": "#931 PR 4d",
    "gaincal": "#931 PR 4d",
    "gencal": "#931 PR 4d",
    "imager": "#931 PR 4e, as tclean",
    "importvla": "#931 PR 4c",
    "mstransform": "#931 PR 4c",
    "plotms": "its own decision (#931)",
    "split": "#931 PR 4c",
}


def surfaces() -> dict[str, dict]:
    document = json.loads(SURFACES.read_text(encoding="utf-8"))
    return {surface["id"]: surface for surface in document["surfaces"]}


def casa_tasks() -> list[str]:
    known = surfaces()
    return [name for name in tasks.TASK_SURFACES if known[name].get("casa_task")]


def casa_parameters(task: str) -> list[dict]:
    record = json.loads((INVENTORY / f"{task}.json").read_text(encoding="utf-8"))
    assert record["casa_version"] == "6.7.6.14"
    return record["parameters"]


def test_every_casa_named_task_is_casa_exact_or_pending() -> None:
    known = surfaces()
    for name in tasks.TASK_SURFACES:
        if name in CASA_RS_ONLY:
            assert not known[name].get("casa_task"), name
        elif name in PENDING:
            assert not known[name].get("casa_task"), f"{name} is CASA-exact; drop it from PENDING"
        else:
            assert known[name].get("casa_task"), f"{name} is CASA-named but not CASA-exact"
    assert set(PENDING) <= set(tasks.TASK_SURFACES)


@pytest.mark.parametrize("name", casa_tasks())
def test_a_casa_task_takes_casas_parameters_then_keyword_only_controls(name: str) -> None:
    casa_task = surfaces()[name]["casa_task"]
    ours = list(inspect.signature(getattr(tasks, name)).parameters.values())
    theirs = casa_parameters(casa_task)
    assert [parameter.name for parameter in ours[: len(theirs)]] == [
        parameter["name"] for parameter in theirs
    ]
    for parameter, casa in zip(ours, theirs):
        assert parameter.kind.name == casa["kind"] == "POSITIONAL_OR_KEYWORD", parameter.name
        assert repr(parameter.default) == casa["default"], parameter.name
    for parameter in ours[len(theirs) :]:
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY, parameter.name


FAKE_TASK = """#!{python}
import json, os, sys
request = json.load(sys.stdin)
with open(os.environ["FAKE_TASK_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps(request) + "\\n")
result = {{"run": {{"stem": request["values"].get("project", "sim")}}, "artifacts": []}}
print(json.dumps({{"status": "succeeded", "surface": request["surface"],
                  "value": {{"kind": "none"}}, "result": result}}))
"""


@pytest.fixture
def requests(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Fake simobserve's executable; the requests it receives."""

    log = tmp_path / "requests.jsonl"
    fake = tmp_path / "fake-simobserve"
    fake.write_text(FAKE_TASK.format(python=sys.executable), encoding="utf-8")
    fake.chmod(0o755)
    monkeypatch.setenv("CASARS_SIMOBSERVE_BIN", str(fake))
    monkeypatch.setenv("FAKE_TASK_LOG", str(log))
    monkeypatch.chdir(tmp_path)

    def read() -> list[dict]:
        return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]

    return read


def test_a_positional_call_sends_only_what_the_caller_gave(requests) -> None:
    assert tasks.simobserve("disk", "disk.fits", "", "", "0.1arcsec") is None
    (request,) = requests()
    assert request["surface"] == "simobserve"
    assert set(request["values"]) == {"project", "skymodel", "incell"}
    assert request["values"]["project"] == "disk"
    assert request["values"]["incell"] == "0.1arcsec"


def test_mutable_defaults_stay_fresh(requests) -> None:
    signature = inspect.signature(tasks.simobserve)
    mapsize = ["1arcmin", "1arcmin"]
    tasks.simobserve(project="a", mapsize=mapsize)
    tasks.simobserve(project="b")
    first, second = requests()
    assert first["values"]["mapsize"] == ["1arcmin", "1arcmin"]
    assert "mapsize" not in second["values"]
    assert mapsize == ["1arcmin", "1arcmin"]
    assert signature.parameters["mapsize"].default == ["", ""]
    assert signature.parameters["direction"].default == []
