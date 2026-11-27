"""casa-rs's CASA tasks present CASA's signatures (#931).

Every task that mimics a CASA task takes CASA 6.7.6.14's parameters by name,
in CASA's order and argument kinds, with the defaults CASA's signature shows,
then casa-rs's own controls as keyword-only arguments, and returns CASA's
value. The CASA signatures are the pinned inventory in
`resources/casa-compat/casa-6.7.6.14/` (#931 PR 4a). A CASA-named task that
is neither checked here nor listed as pending fails, so a new one cannot
slip in with a casa-rs signature. These tests establish the call surface;
behavioral and scientific equivalence require their own evidence (#980).
"""

from __future__ import annotations

import inspect
import builtins
import json
from pathlib import Path

import pytest

from casars import tasks
from casars.tasks import _runner

REPO = Path(__file__).resolve().parents[4]
INVENTORY = REPO / "resources" / "casa-compat" / "casa-6.7.6.14" / "tasks"
SURFACES = REPO / "surfaces" / "casa-contracts" / "resources" / "parameter-surfaces.json"

# casa-rs tasks with no CASA counterpart, outside the mimic rule.
CASA_RS_ONLY = {"calibrate", "msconvert", "msexplore"}
# CASA-named tasks whose Python signatures still need migration under #931.
PENDING: dict[str, str] = {}


def surfaces() -> dict[str, dict]:
    document = json.loads(SURFACES.read_text(encoding="utf-8"))
    return {surface["id"]: surface for surface in document["surfaces"]}


def casa_tasks() -> list[str]:
    known = surfaces()
    return [name for name in tasks.TASK_SURFACES if known[name].get("casa_task")]


def casa_parameters(task: str) -> list[dict]:
    record = json.loads((INVENTORY / f"{task}.json").read_text(encoding="utf-8"))
    assert record["casa_version"] == "6.7.6.14"
    return [
        parameter
        for parameter in record["parameters"]
        if parameter["kind"] not in ("VAR_POSITIONAL", "VAR_KEYWORD")
    ]


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
        # sdcal's intent is keyword-only in the pinned CASA signature.
        assert parameter.kind.name == casa["kind"], parameter.name
        assert repr(parameter.default) == casa["default"], parameter.name
    for parameter in ours[len(theirs) :]:
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY, parameter.name


@pytest.fixture
def requests(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Capture explicit arguments at the native task boundary."""
    from types import SimpleNamespace
    from casars.parameters import _native

    requests = []

    def run(surface, workspace, values, originals, options):
        requests.append({"surface": surface, "values": values})
        return SimpleNamespace(status="succeeded", logs="", message=None, parameter=None,
                               value=lambda: None, completion=lambda *_: None, plots=lambda *_: [])

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(_native(), "run_native_task", run)
    monkeypatch.setattr(_runner, "_require_plotting", lambda: None)
    return lambda: requests


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


def test_missing_plot_dependency_fails_before_starting_the_task(tmp_path, monkeypatch):
    original = builtins.__import__

    def missing_plot(name, *args, **kwargs):
        if name == "matplotlib" or name.startswith("matplotlib."):
            raise ModuleNotFoundError("plot dependency unavailable")
        return original(name, *args, **kwargs)

    def unexpected_task(*args):
        pytest.fail("the task started without its rendering dependency")

    monkeypatch.setattr(builtins, "__import__", missing_plot)
    monkeypatch.setattr(_runner, "_execute_task", unexpected_task)
    with pytest.raises(ModuleNotFoundError, match="plot dependency unavailable"):
        tasks.run("simobserve", workspace=tmp_path, project="untouched", graphics="file")
    assert list(tmp_path.iterdir()) == []
