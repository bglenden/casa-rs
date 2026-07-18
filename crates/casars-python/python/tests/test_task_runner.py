from __future__ import annotations

import json
from pathlib import Path

import pytest

from casars import tasks
from casars.parameters import SessionParameters, TaskParameters


def _fake_casars(path: Path) -> Path:
    path.write_text(
        """#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

arguments = sys.argv[1:]
profile_path = Path(arguments[arguments.index("--params") + 1])
Path(os.environ["FAKE_CASARS_LOG"]).write_text(json.dumps({
    "argv": arguments,
    "cwd": os.getcwd(),
    "profile": profile_path.read_text(encoding="utf-8"),
}), encoding="utf-8")
print(os.environ.get("FAKE_CASARS_STDOUT", "runner stdout"))
print("runner stderr", file=sys.stderr)
raise SystemExit(int(os.environ.get("FAKE_CASARS_EXIT", "0")))
""",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def test_run_uses_canonical_profile_and_forwards_runtime_controls(
    tmp_path: Path,
) -> None:
    binary = _fake_casars(tmp_path / "casars")
    log = tmp_path / "invocation.json"
    parameters = TaskParameters.defaults("flagmanager", workspace=tmp_path)
    parameters["vis"] = Path("base.ms")

    completion = tasks.run(
        "flagmanager",
        parameters,
        workspace=tmp_path,
        notebook="Analysis.md",
        binary=binary,
        save_last=False,
        record_notebook=False,
        confirm_overwrite=True,
        confirm_mutation=True,
        env={"FAKE_CASARS_LOG": str(log)},
        comment="from generic runner",
    )

    recorded = json.loads(log.read_text(encoding="utf-8"))
    assert recorded["argv"][:3] == ["run", "flagmanager", "--params"]
    assert recorded["argv"][4:] == [
        "--workspace",
        str(tmp_path),
        "--initiating-surface",
        "python",
        "--notebook",
        "Analysis.md",
        "--no-save-last",
        "--no-notebook-recording",
        "--confirm-overwrite",
        "--confirm-mutation",
    ]
    assert recorded["cwd"] == str(tmp_path)
    assert 'vis = ["base.ms"]' in recorded["profile"]
    assert 'comment = "from generic runner"' in recorded["profile"]
    assert "mode =" not in recorded["profile"]

    assert completion.successful
    assert completion.returncode == 0
    assert completion.stdout == "runner stdout\n"
    assert completion.stderr == "runner stderr\n"
    assert completion.parameters_toml == recorded["profile"]
    assert completion.workspace == tmp_path
    assert completion.result is not None
    assert completion.result.surface_id == "flagmanager"
    assert completion.products == ()
    assert not Path(completion.command[4]).exists()
    assert parameters["comment"] == "none"


def test_task_parameters_run_delegates_to_canonical_runner(tmp_path: Path) -> None:
    binary = _fake_casars(tmp_path / "casars")
    values = TaskParameters.defaults("flagmanager", workspace=tmp_path)
    values["vis"] = "delegated.ms"
    completion = values.run(
        workspace=tmp_path,
        binary=binary,
        env={"FAKE_CASARS_LOG": str(tmp_path / "invocation.json")},
    )
    assert completion.successful
    assert 'vis = ["delegated.ms"]' in completion.parameters_toml


def test_run_accepts_profile_and_managed_base_sources(tmp_path: Path) -> None:
    binary = _fake_casars(tmp_path / "casars")
    log = tmp_path / "invocation.json"
    environment = {"FAKE_CASARS_LOG": str(log)}
    parameters = TaskParameters.defaults("flagmanager", workspace=tmp_path)
    parameters.set_many({"vis": "saved.ms", "comment": "from file"})
    profile = parameters.save(tmp_path / "flagmanager.toml")

    from_file = tasks.run(
        "flagmanager",
        profile=profile,
        workspace=tmp_path,
        binary=binary,
        env=environment,
        overrides={"comment": "file override"},
    )
    assert 'comment = "file override"' in from_file.parameters_toml

    parameters.write_last()
    from_last = tasks.run(
        "flagmanager",
        base_source="last",
        workspace=tmp_path,
        binary=binary,
        env=environment,
    )
    assert 'vis = ["saved.ms"]' in from_last.parameters_toml
    assert 'comment = "from file"' in from_last.parameters_toml


def test_run_preserves_failure_output_with_and_without_check(tmp_path: Path) -> None:
    binary = _fake_casars(tmp_path / "casars")
    environment = {
        "FAKE_CASARS_LOG": str(tmp_path / "invocation.json"),
        "FAKE_CASARS_EXIT": "7",
    }

    completion = tasks.run(
        "flagmanager",
        workspace=tmp_path,
        binary=binary,
        env=environment,
        check=False,
        vis="failed.ms",
    )
    assert not completion.successful
    assert completion.returncode == 7
    assert completion.stdout == "runner stdout\n"
    assert completion.stderr == "runner stderr\n"

    with pytest.raises(tasks.TaskExecutionError) as failure:
        tasks.run(
            "flagmanager",
            workspace=tmp_path,
            binary=binary,
            env=environment,
            vis="failed.ms",
        )
    assert failure.value.completion.returncode == 7
    assert failure.value.completion.stderr == "runner stderr\n"


def test_run_rejects_session_parameters_and_ambiguous_sources(tmp_path: Path) -> None:
    session = SessionParameters.defaults("imexplore", workspace=tmp_path)
    with pytest.raises(TypeError, match="requires task parameters"):
        tasks.run("flagmanager", session, workspace=tmp_path)

    profile = TaskParameters.defaults("flagmanager", workspace=tmp_path)
    profile["vis"] = "input.ms"
    path = profile.save(tmp_path / "flagmanager.toml")
    with pytest.raises(ValueError, match="mutually exclusive"):
        tasks.run(
            "flagmanager",
            profile=path,
            base_source="last",
            workspace=tmp_path,
        )


def test_generated_surface_returns_rust_typed_products(tmp_path: Path) -> None:
    binary = _fake_casars(tmp_path / "casars")
    output = tmp_path / "selected.ms"
    output.mkdir()

    completion = tasks.split(
        vis="input.ms",
        outputvis=output,
        workspace=tmp_path,
        binary=binary,
        env={"FAKE_CASARS_LOG": str(tmp_path / "invocation.json")},
    )

    assert completion.result is not None
    assert completion.result.surface_id == "split"
    assert len(completion.products) == 1
    assert completion.products[0].path == str(output)
    assert completion.products[0].exists is True


def test_successful_process_with_malformed_managed_result_is_rejected(
    tmp_path: Path,
) -> None:
    binary = _fake_casars(tmp_path / "casars")
    with pytest.raises(tasks.TaskResultError, match="successful but invalid result"):
        tasks.imager(
            vis="input.ms",
            imagename="dirty",
            workspace=tmp_path,
            binary=binary,
            env={"FAKE_CASARS_LOG": str(tmp_path / "invocation.json")},
        )
