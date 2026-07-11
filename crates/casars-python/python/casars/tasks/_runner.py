"""Catalog-driven generic task execution through the canonical ``casars`` CLI."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from os import PathLike
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from typing import Literal, TypeAlias

from .._task_runtime import _resolve_task_binary
from ..parameters import ParameterData, SurfaceParameters, TaskParameters

StrPath: TypeAlias = str | PathLike[str]
TaskBaseSource: TypeAlias = Literal["defaults", "last", "last_successful"]

CASARS_BINARY_ENVVAR = "CASARS_BIN"
CASARS_BINARY_NAME = "casars"


class CasarsBinaryNotFoundError(FileNotFoundError):
    """Raised when the canonical ``casars`` executable cannot be resolved."""


class TaskInvocationError(RuntimeError):
    """Raised when the canonical runner could not start."""


class TaskExecutionError(RuntimeError):
    """Raised for a non-zero task completion when ``check=True``."""

    def __init__(self, completion: TaskCompletion) -> None:
        self.completion = completion
        detail = completion.stderr.strip() or completion.stdout.strip() or "no output"
        super().__init__(
            f"{completion.task} exited with status {completion.returncode}: {detail}"
        )


@dataclass(frozen=True, slots=True)
class TaskCompletion:
    """Completed generic task invocation, including exact captured output."""

    task: str
    command: tuple[str, ...]
    workspace: Path
    returncode: int
    stdout: str
    stderr: str
    parameters_toml: str

    @property
    def successful(self) -> bool:
        """Whether the ``casars`` process and provider completed successfully."""

        return self.returncode == 0

    def check_returncode(self) -> TaskCompletion:
        """Raise :class:`TaskExecutionError` for a failed completion."""

        if not self.successful:
            raise TaskExecutionError(self)
        return self


def run(
    task: str,
    parameters: SurfaceParameters | None = None,
    *,
    profile: StrPath | None = None,
    base_source: TaskBaseSource = "defaults",
    overrides: Mapping[str, ParameterData] | None = None,
    workspace: StrPath | None = None,
    notebook: str | None = None,
    save_last: bool = True,
    record_notebook: bool = True,
    confirm_overwrite: bool = False,
    confirm_mutation: bool = False,
    binary: StrPath | None = None,
    check: bool = True,
    timeout: float | None = None,
    env: Mapping[str, str] | None = None,
    **casa_overrides: ParameterData,
) -> TaskCompletion:
    """Run any catalog task through ``casars run``.

    ``parameters``, ``profile``, and ``base_source`` select one base draft.
    CASA-named values may be supplied through ``overrides`` or keyword
    arguments.  All values are normalized and rendered to a temporary
    current-contract TOML profile by the Rust runtime; Python never projects
    values into provider flags.

    Runtime-only authorization remains explicit through
    ``confirm_overwrite`` and ``confirm_mutation`` and is never persisted in
    the profile. ``record_notebook=False`` is the visible one-run recording
    bypass. ``notebook`` explicitly routes the receipt to a named notebook
    filename or stable ID; otherwise the project default receives it. The CLI
    owns automatic Last, Last Successful, and notebook receipt updates.
    Captured stdout and stderr are retained on the returned completion and on
    :class:`TaskExecutionError`.
    """

    resolved_workspace = _resolve_workspace(workspace, parameters)
    resolved = _resolve_parameters(
        task,
        parameters=parameters,
        profile=profile,
        base_source=base_source,
        workspace=resolved_workspace,
    )
    merged_overrides = _merge_overrides(overrides, casa_overrides)
    if merged_overrides:
        resolved.set_many(merged_overrides)

    executable = _resolve_casars_binary(binary)
    with TemporaryDirectory(prefix=f"casars-{task}-parameters-") as temporary:
        profile_path = Path(temporary) / f"{task}.toml"
        resolved.save(profile_path)
        parameters_toml = profile_path.read_text(encoding="utf-8")
        command = [
            executable,
            "run",
            task,
            "--params",
            str(profile_path),
            "--workspace",
            str(resolved_workspace),
            "--initiating-surface",
            "python",
        ]
        if notebook is not None:
            command.extend(["--notebook", notebook])
        if not save_last:
            command.append("--no-save-last")
        if not record_notebook:
            command.append("--no-notebook-recording")
        if confirm_overwrite:
            command.append("--confirm-overwrite")
        if confirm_mutation:
            command.append("--confirm-mutation")

        process_env = None
        if env is not None:
            process_env = os.environ.copy()
            process_env.update(env)
        try:
            process = subprocess.run(
                command,
                cwd=resolved_workspace,
                capture_output=True,
                check=False,
                text=True,
                timeout=timeout,
                env=process_env,
            )
        except OSError as error:
            raise TaskInvocationError(
                f"failed to start the canonical casars runner {executable!r}: {error}"
            ) from error

    completion = TaskCompletion(
        task=task,
        command=tuple(command),
        workspace=resolved_workspace,
        returncode=process.returncode,
        stdout=process.stdout,
        stderr=process.stderr,
        parameters_toml=parameters_toml,
    )
    if check:
        completion.check_returncode()
    return completion


def _resolve_workspace(
    workspace: StrPath | None,
    parameters: SurfaceParameters | None,
) -> Path:
    if workspace is None and parameters is not None:
        root = parameters.workspace
    else:
        root = Path.cwd() if workspace is None else Path(workspace)
    root = root.resolve()
    if not root.is_dir():
        raise FileNotFoundError(
            f"task workspace does not exist or is not a directory: {root}"
        )
    return root


def _resolve_parameters(
    task: str,
    *,
    parameters: SurfaceParameters | None,
    profile: StrPath | None,
    base_source: str,
    workspace: Path,
) -> SurfaceParameters:
    source = base_source.lower().replace("-", "_")
    if source not in {"defaults", "last", "last_successful"}:
        raise ValueError("base_source must be 'defaults', 'last', or 'last_successful'")
    if parameters is not None and profile is not None:
        raise ValueError("pass parameters or profile, not both")
    if (parameters is not None or profile is not None) and source != "defaults":
        raise ValueError(
            "parameters/profile and a non-default base_source are mutually exclusive"
        )

    if parameters is not None:
        if parameters.kind != "task":
            raise TypeError(
                f"casars.tasks.run requires task parameters; {parameters.surface!r} is a session"
            )
        if parameters.surface != task:
            raise ValueError(
                f"parameters target {parameters.surface!r}, not requested task {task!r}"
            )
        return parameters.copy()
    if profile is not None:
        return TaskParameters.load(task, profile, workspace=workspace)
    if source == "last":
        return TaskParameters.last(task, workspace=workspace)
    if source == "last_successful":
        return TaskParameters.last_successful(task, workspace=workspace)
    return TaskParameters.defaults(task, workspace=workspace)


def _merge_overrides(
    overrides: Mapping[str, ParameterData] | None,
    keyword_overrides: Mapping[str, ParameterData],
) -> dict[str, ParameterData]:
    merged: dict[str, ParameterData] = {}
    if overrides is not None:
        for name, value in overrides.items():
            if not isinstance(name, str):
                raise TypeError("CASA parameter override names must be strings")
            merged[name] = value
    duplicate = merged.keys() & keyword_overrides.keys()
    if duplicate:
        names = ", ".join(sorted(duplicate))
        raise ValueError(f"duplicate CASA parameter overrides: {names}")
    merged.update(keyword_overrides)
    return merged


def _resolve_casars_binary(binary: StrPath | None) -> str:
    return _resolve_task_binary(
        binary=binary,
        configured_binary=None,
        envvar=CASARS_BINARY_ENVVAR,
        binary_name=CASARS_BINARY_NAME,
        missing_error_cls=CasarsBinaryNotFoundError,
        description="casars task runner",
    )


__all__ = [
    "CasarsBinaryNotFoundError",
    "TaskCompletion",
    "TaskExecutionError",
    "TaskInvocationError",
    "TaskBaseSource",
    "run",
]
