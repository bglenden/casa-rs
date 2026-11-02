from __future__ import annotations

import ast
import importlib.util
import inspect
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from casars import parameters, tasks
from casars.tasks import _runner


REPO_ROOT = Path(__file__).resolve().parents[4]


def test_catalog_generates_one_wrapper_for_every_task() -> None:
    surfaces = parameters.catalog()["surfaces"]
    assert {surface["kind"] for surface in surfaces} == {"task"}
    task_ids = tuple(surface["id"] for surface in surfaces)
    assert len(task_ids) == 27
    assert tasks.TASK_SURFACES == task_ids
    assert all(callable(getattr(tasks, name)) for name in tasks.TASK_SURFACES)
    assert not hasattr(tasks, "plotcal")


def test_a_wrapper_takes_casas_parameters_and_no_runner_plumbing() -> None:
    # #931: the runner's arguments left the CASA-named signatures; a task
    # runs in the current directory, and a profile is passed as values.
    for name in tasks.TASK_SURFACES:
        signature = inspect.signature(getattr(tasks, name))
        bindings = parameters.definition(name)["bindings"]
        assert list(signature.parameters) == [
            binding["name"]
            for binding in sorted(bindings, key=lambda binding: binding["order"])
        ], name


def _capture_runs(monkeypatch) -> dict[str, Any]:
    """Fake the runner's `run`, which a CASA-signature wrapper calls; what
    the last call sent."""

    captured: dict[str, Any] = {}

    def fake_run(task: str, **values: Any) -> object:
        captured.update(task=task, overrides=values)
        return SimpleNamespace(value={"iterdone": 0})

    monkeypatch.setattr(_runner, "run", fake_run)
    return captured


def test_generated_task_wrapper_forwards_only_explicit_casa_names(monkeypatch) -> None:
    # tclean takes CASA's signature (#931 PR 4e): an argument left at, or
    # given, CASA's default is not sent, and CASA's value comes back.
    captured = _capture_runs(monkeypatch)
    assert tasks.tclean(
        vis="target.ms",
        imagename="target",
        imsize=1024,
        niter=0,
    ) == {"iterdone": 0}

    assert captured["task"] == "tclean"
    assert captured["overrides"] == {
        "vis": "target.ms",
        "imagename": "target",
        "imsize": 1024,
    }


def test_importvla_wrapper_preserves_the_archive_path_list(monkeypatch) -> None:
    # importvla takes CASA's signature (#931 PR 4c): its wrapper runs the
    # task through the runner's `run` and returns CASA's value.
    captured: dict[str, Any] = {}

    def fake_run(task: str, **values: Any) -> object:
        captured.update(task=task, overrides=values)
        return SimpleNamespace(value=None)

    monkeypatch.setattr(_runner, "run", fake_run)
    archivefiles = ["raw/one.exp", "raw/two.xp1"]
    assert tasks.importvla(archivefiles=archivefiles) is None

    assert captured["task"] == "importvla"
    assert captured["overrides"] == {"archivefiles": archivefiles}


def test_generated_tclean_wrapper_preserves_vlass_awproject_controls(monkeypatch) -> None:
    captured = _capture_runs(monkeypatch)
    tasks.tclean(
        vis="vlass.ms",
        imagename="vlass",
        field="1107~1127,1512~1532,1542~1562",
        spw="2~17",
        uvrange="<12km",
        intent="OBSERVE_TARGET#UNSPECIFIED",
        stokes="I",
        gridder="awproject",
        wprojplanes=32,
        cfcache="cf-cache/vlass",
        cf_resident_mb=256,
        usepointing=True,
        pointingoffsetsigdev=[0.0],
        conjbeams=True,
        normtype="flatnoise",
        deconvolver="mtmfs",
        nterms=2,
        scales=[0, 5, 12],
        parallel=False,
        gridprecision="f64",
    )

    assert captured["task"] == "tclean"
    overrides = captured["overrides"]
    assert overrides["field"] == "1107~1127,1512~1532,1542~1562"
    # CASA's defaults, stokes='I' and parallel=False, are left for the task
    # to resolve.
    assert "stokes" not in overrides and "parallel" not in overrides
    assert overrides["gridder"] == "awproject"
    assert overrides["cfcache"] == "cf-cache/vlass"
    assert overrides["usepointing"] is True
    assert overrides["conjbeams"] is True
    assert overrides["scales"] == [0, 5, 12]
    assert overrides["gridprecision"] == "f64"


def test_generated_tclean_wrapper_preserves_explicit_native_evla_source(monkeypatch) -> None:
    captured = _capture_runs(monkeypatch)
    tasks.tclean(
        vis="vlass.ms",
        imagename="native",
        gridder="awproject",
        aw_cf_source="native-evla",
        native_cf_cache="private/native-cache",
        evla_surface="models/EVLA.surface",
        native_cf_policy="generate-missing",
        native_cf_working_size=256,
        native_cf_oversampling=20,
        native_cf_cache_bytes=2147483648,
        native_cf_maximum_cells=1024,
    )
    assert captured["overrides"] == {
        "vis": "vlass.ms", "imagename": "native", "gridder": "awproject",
        "aw_cf_source": "native-evla", "native_cf_cache": "private/native-cache",
        "evla_surface": "models/EVLA.surface", "native_cf_policy": "generate-missing",
        "native_cf_working_size": 256, "native_cf_oversampling": 20,
        "native_cf_cache_bytes": 2147483648, "native_cf_maximum_cells": 1024,
    }


def _stub_functions(path: Path) -> dict[str, ast.FunctionDef]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    }


def _annotations(function: ast.FunctionDef) -> dict[str, str]:
    """A wrapper's annotations: a CASA signature's positional parameters as
    well as the keyword-only ones."""

    return {
        argument.arg: ast.unparse(argument.annotation)
        for argument in [*function.args.args, *function.args.kwonlyargs]
        if argument.annotation is not None
    }


def test_generated_stubs_use_catalog_domains_and_surface_refinements() -> None:
    task_functions = _stub_functions(
        REPO_ROOT / "crates/casars-python/python/casars/tasks/_catalog.pyi"
    )

    for surface in parameters.catalog()["surfaces"]:
        annotations = _annotations(task_functions[surface["id"]])
        for binding in surface["bindings"]:
            # Surface-specific wrappers must expose useful types rather than
            # falling back to the recursive catch-all accepted by Mapping APIs.
            assert annotations[binding["name"]] != "ParameterData"
            explicit = binding["projections"]["python"].get("type_hint")
            if explicit is not None:
                assert annotations[binding["name"]] == ast.unparse(
                    ast.parse(explicit, mode="eval")
                )

    tclean = _annotations(task_functions["tclean"])
    assert tclean["imsize"] == "int | list[int] | tuple[int, ...]"
    assert tclean["cell"] == "str | list[str] | tuple[str, ...]"
    assert tclean["write_pb"] == "bool"

    importvla = _annotations(task_functions["importvla"])
    assert importvla["archivefiles"] == "StrPath | list[StrPath] | tuple[StrPath, ...]"

    for plot_task in ("msexplore", "plotms"):
        plot = _annotations(task_functions[plot_task])
        assert "plot_output" in plot
        assert "plot_width" not in plot
        assert "plot_height" not in plot


def test_generator_honors_explicit_python_type_hint_metadata() -> None:
    script = REPO_ROOT / "scripts/generate-python-parameter-wrappers.py"
    spec = importlib.util.spec_from_file_location("parameter_wrapper_generator", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    binding = {
        "name": "example",
        "projections": {"python": {"name": "example", "type_hint": "bytes"}},
    }
    assert module.binding_annotation(binding, {}) == "bytes"
