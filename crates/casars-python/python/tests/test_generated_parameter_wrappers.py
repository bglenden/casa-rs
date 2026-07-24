from __future__ import annotations

import ast
import importlib.util
from pathlib import Path
from typing import Any

from casars import _session_catalog, parameters, sessions, tasks
from casars.tasks import _catalog


REPO_ROOT = Path(__file__).resolve().parents[4]


def test_catalog_generates_one_wrapper_for_every_task() -> None:
    task_ids = tuple(
        surface["id"]
        for surface in parameters.catalog()["surfaces"]
        if surface["kind"] == "task"
    )
    assert len(task_ids) == 40
    assert tasks.TASK_SURFACES == task_ids
    assert all(callable(getattr(tasks, name)) for name in tasks.TASK_SURFACES)


def test_generated_task_wrapper_forwards_only_explicit_casa_names(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(task: str, **options: Any) -> object:
        captured.update(task=task, **options)
        return object()

    monkeypatch.setattr(_catalog, "_run", fake_run)
    tasks.imager(
        vis="target.ms",
        imagename="target",
        imsize=1024,
        profile="imager.toml",
        base_source="defaults",
        notebook="Analysis.md",
        save_last=False,
        record_notebook=False,
    )

    assert captured["task"] == "imager"
    assert captured["overrides"] == {
        "vis": "target.ms",
        "imagename": "target",
        "imsize": 1024,
    }
    assert captured["profile"] == "imager.toml"
    assert captured["base_source"] == "defaults"
    assert captured["notebook"] == "Analysis.md"
    assert captured["save_last"] is False
    assert captured["record_notebook"] is False


def test_importvla_wrapper_preserves_the_archive_path_list(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(task: str, **options: Any) -> object:
        captured.update(task=task, **options)
        return object()

    monkeypatch.setattr(_catalog, "_run", fake_run)
    archivefiles = ["raw/one.exp", "raw/two.xp1"]
    tasks.importvla(archivefiles=archivefiles)

    assert captured["task"] == "importvla"
    assert captured["overrides"] == {"archivefiles": archivefiles}


def test_generated_imager_wrapper_preserves_vlass_awproject_controls(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(task: str, **options: Any) -> object:
        captured.update(task=task, **options)
        return object()

    monkeypatch.setattr(_catalog, "_run", fake_run)
    tasks.imager(
        vis="vlass.ms",
        imagename="vlass",
        field="1107~1127,1512~1532,1542~1562",
        spw="2~17",
        uvrange="<12km",
        intent="OBSERVE_TARGET#UNSPECIFIED",
        stokes="I",
        projection="SIN",
        gridder="awproject",
        wprojplanes=32,
        cfcache="cf-cache/vlass",
        cf_resident_mb=256,
        facets=1,
        aterm=True,
        psterm=False,
        wbawp=True,
        conjbeams=True,
        usepointing=True,
        computepastep=360.0,
        rotatepastep=360.0,
        pointingoffsetsigdev="0",
        mosweight=False,
        normtype="flatnoise",
        deconvolver="mtmfs",
        nterms=2,
        scales="0,5,12",
        parallel=False,
        imaging_memory_target_mb=16384,
        imaging_prepare_workers=4,
        imaging_fft_backend="metal-mpsgraph",
        imaging_fft_precision="f32",
    )

    assert captured["task"] == "imager"
    overrides = captured["overrides"]
    assert overrides["field"] == "1107~1127,1512~1532,1542~1562"
    assert overrides["stokes"] == "I"
    assert overrides["gridder"] == "awproject"
    assert overrides["cfcache"] == "cf-cache/vlass"
    assert overrides["psterm"] is False
    assert overrides["parallel"] is False
    assert overrides["imaging_memory_target_mb"] == 16384
    assert overrides["imaging_fft_backend"] == "metal-mpsgraph"
    assert overrides["imaging_fft_precision"] == "f32"


def test_catalog_generates_one_wrapper_for_every_session(monkeypatch) -> None:
    session_ids = tuple(
        surface["id"]
        for surface in parameters.catalog()["surfaces"]
        if surface["kind"] == "session"
    )
    assert len(session_ids) == 2
    assert _session_catalog.SESSION_SURFACES == session_ids
    assert sessions.imexplore is _session_catalog.imexplore
    assert sessions.tablebrowser is _session_catalog.tablebrowser

    captured: dict[str, Any] = {}

    def fake_open(surface: str, **options: Any) -> object:
        captured.update(surface=surface, **options)
        return object()

    monkeypatch.setattr(sessions, "open", fake_open)
    sessions.imexplore(image="cube.image", colormap="gray", source="defaults")

    assert captured["surface"] == "imexplore"
    assert captured["start"] == "defaults"
    assert captured["overrides"] == {"image": "cube.image", "colormap": "gray"}


def _stub_functions(path: Path) -> dict[str, ast.FunctionDef]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    }


def _annotations(function: ast.FunctionDef) -> dict[str, str]:
    return {
        argument.arg: ast.unparse(argument.annotation)
        for argument in function.args.kwonlyargs
        if argument.annotation is not None
    }


def test_generated_stubs_use_catalog_domains_and_surface_refinements() -> None:
    task_functions = _stub_functions(
        REPO_ROOT / "crates/casars-python/python/casars/tasks/_catalog.pyi"
    )
    session_functions = _stub_functions(
        REPO_ROOT / "crates/casars-python/python/casars/_session_catalog.pyi"
    )

    for surface in parameters.catalog()["surfaces"]:
        functions = task_functions if surface["kind"] == "task" else session_functions
        annotations = _annotations(functions[surface["id"]])
        for binding in surface["bindings"]:
            # Surface-specific wrappers must expose useful types rather than
            # falling back to the recursive catch-all accepted by Mapping APIs.
            assert annotations[binding["name"]] != "ParameterData"
            explicit = binding["projections"]["python"].get("type_hint")
            if explicit is not None:
                assert annotations[binding["name"]] == ast.unparse(
                    ast.parse(explicit, mode="eval")
                )

    imager = _annotations(task_functions["imager"])
    assert imager["imsize"] == "int | list[int] | tuple[int, ...] | Literal['auto']"
    assert imager["cell"] == "str | list[str] | tuple[str, ...]"
    assert imager["write_pb"] == "bool"

    importvla = _annotations(task_functions["importvla"])
    assert importvla["archivefiles"] == "StrPath | list[StrPath] | tuple[StrPath, ...]"

    tablebrowser = _annotations(session_functions["tablebrowser"])
    assert tablebrowser["view"] == "Literal['columns', 'keywords', 'rows', 'summary']"
    assert tablebrowser["rowstart"] == "int"
    imexplore = _annotations(session_functions["imexplore"])
    assert imexplore["view"] == "Literal['coordinates', 'metadata', 'plane', 'spectrum']"
    assert imexplore["contentmode"] == "Literal['raster', 'spreadsheet']"
    assert imexplore["image"] == "StrPath"


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
