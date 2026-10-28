"""Run a catalog task by starting its executable directly (ADR-0017).

Python resolves the task's parameters through the Rust parameter runtime,
finds the task's executable through the Rust resolver, and starts it with
``--json-run -``: the CASA-named values set are written to its stdin, its
log lines are passed to ``sys.stderr`` as they arrive, and its typed reply on
stdout says how it ended. A Python task call neither loads nor saves Last.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from os import PathLike
from pathlib import Path
import signal
import subprocess
import sys
import threading
from typing import Any, TypeAlias

from ..parameters import (
    ParameterData,
    ProviderInvocation,
    SurfaceParameters,
    TaskParameters,
    _native,
)
from ._completion import completion_projection

StrPath: TypeAlias = str | PathLike[str]

# How long a task has to stop after Python forwards it an interrupt.
_INTERRUPT_GRACE_SECONDS = 10.0


class TaskBinaryNotFoundError(FileNotFoundError):
    """Raised when a task's executable cannot be found.

    The message names the step of the search that applied and what it
    lacked: the task's ``CASARS_<APP>_BIN`` override, the suite at
    ``CASARS_SUITE_ROOT`` or the installed suite, or a development
    checkout's build.
    """


class TaskInvocationError(RuntimeError):
    """Raised when a task's executable was found but could not be started."""


class TaskResultError(RuntimeError):
    """Raised when Rust rejects a successful task's result."""


class TaskExecutionError(RuntimeError):
    """Raised when a task does not succeed; ``completion`` says how it ended."""

    def __init__(self, completion: TaskCompletion) -> None:
        self.completion = completion
        super().__init__(completion.describe())


@dataclass(frozen=True, slots=True)
class TaskCompletion:
    """One finished task process: how it ended and what it wrote.

    ``status`` is the task's outcome: ``succeeded``, ``refused`` (its
    parameters, before it changed anything), ``failed``, ``interrupted``,
    ``signalled``, or ``malformed_reply``. ``stdout`` is the task's reply and
    ``stderr`` its log, which was also written to ``sys.stderr`` as the task
    ran.
    """

    task: str
    command: tuple[str, ...]
    workspace: Path
    returncode: int
    stdout: str
    stderr: str
    provider_invocation: ProviderInvocation
    status: str
    message: str | None
    parameter: str | None
    value: Any
    result: object | None

    @property
    def successful(self) -> bool:
        """Whether the task succeeded."""

        return self.status == "succeeded"

    @property
    def products(self) -> tuple[object, ...]:
        """Typed contract-declared products from the Rust completion decoder."""

        if self.result is None:
            return ()
        return tuple(self.result.products)  # type: ignore[attr-defined]

    def describe(self) -> str:
        """One line saying how the task ended."""

        at = f" (parameter {self.parameter!r})" if self.parameter else ""
        if self.status == "succeeded":
            return f"{self.task} succeeded"
        if self.status == "refused":
            return f"{self.task} refused its parameters{at}: {self.message}"
        if self.status == "failed":
            return f"{self.task} failed{at}: {self.message}"
        if self.status == "interrupted":
            return f"{self.task} was interrupted: {self.message}"
        detail = self.stderr.strip().splitlines()[-1:] or ["no log output"]
        if self.status == "signalled":
            return f"{self.task} was ended by signal {-self.returncode}: {detail[0]}"
        return f"{self.task} gave no valid reply ({self.message}): {detail[0]}"


def run(
    task: str,
    parameters: SurfaceParameters | None = None,
    /,
    *,
    workspace: StrPath | None = None,
    **values: ParameterData,
) -> TaskCompletion:
    """Run any catalog task, with CASA-named ``values``.

    ``parameters`` is a draft to start from, such as a loaded profile; the
    contract's defaults otherwise. The task runs in ``workspace``: the
    draft's, or the current directory. Values are normalized and checked by
    Rust before the task starts, so a value the contract refuses raises
    ``ValueError`` and starts nothing.

    The call loads and saves no Last profile: those record command-line runs.
    :meth:`TaskParameters.last` and :meth:`TaskParameters.write_last` stay
    explicit. CASA's own parameters, such as ``overwrite``, are the consent
    to replace or change data. A task that does not succeed raises
    :class:`TaskExecutionError`; an interrupt (Ctrl-C) is passed to the task,
    which is given time to stop, and then raised here.
    """

    resolved_workspace = _resolve_workspace(workspace, parameters)
    if parameters is not None:
        if parameters.surface != task:
            raise ValueError(
                f"parameters target {parameters.surface!r}, not requested task {task!r}"
            )
        resolved = parameters.copy()
    else:
        resolved = TaskParameters.defaults(task, workspace=resolved_workspace)
    if values:
        resolved.set_many(values)
    return _execute(task, resolved, resolved_workspace)


def _run(task: str, values: Mapping[str, ParameterData]) -> TaskCompletion:
    """Run ``task`` in the current directory: the CASA-named wrappers' call."""

    return run(task, **values)


def _execute(task: str, resolved: SurfaceParameters, workspace: Path) -> TaskCompletion:
    invocation = resolved.provider_invocation()
    executable = _task_executable(task)
    command = (executable, *invocation.args)
    try:
        # The task runs in its own session, so a terminal's Ctrl-C reaches
        # Python alone, which passes it on once, in a terminal as in a
        # notebook: a second interrupt ends the imager at once.
        process = subprocess.Popen(
            command,
            cwd=workspace,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            start_new_session=True,
        )
    except OSError as error:
        raise TaskInvocationError(f"cannot start {task} ({executable}): {error}") from error
    stdout, stderr = _communicate(process, invocation.stdin or "")

    values = [
        (name, _display_value(state.value))
        for name, state in sorted(resolved.states.items())
        if state.value is not None
    ]
    try:
        outcome = _native().task_outcome(
            task, process.returncode, stdout, str(workspace), values
        )
    except ValueError as error:
        raise TaskResultError(
            f"{task} produced a successful but invalid result: {error}"
        ) from error
    completion = TaskCompletion(
        task=task,
        command=command,
        workspace=workspace,
        returncode=process.returncode,
        stdout=stdout,
        stderr=stderr,
        provider_invocation=invocation,
        status=outcome["status"],
        message=outcome.get("message", outcome.get("reason")),
        parameter=outcome.get("parameter"),
        value=outcome.get("value"),
        result=(
            completion_projection(outcome["completion"])
            if "completion" in outcome
            else None
        ),
    )
    if not completion.successful:
        raise TaskExecutionError(completion)
    return completion


def _communicate(process: subprocess.Popen[str], request: str) -> tuple[str, str]:
    """Send ``request``, pass the log on as it arrives, and collect the reply.

    Each pipe has its own thread, so none fills while another is waited on,
    and the log keeps draining while an interrupted task shuts down. The main
    thread only waits for the task; anything that stops the wait, an
    interrupt above all, is passed to the task first.
    """

    reply: list[str] = []
    log: list[str] = []

    def send() -> None:
        try:
            assert process.stdin is not None
            process.stdin.write(request)
            process.stdin.close()
        except (BrokenPipeError, OSError):
            pass

    def collect() -> None:
        assert process.stdout is not None
        reply.append(process.stdout.read())

    def relay() -> None:
        assert process.stderr is not None
        for line in process.stderr:
            log.append(line)
            terminal = sys.stderr
            if terminal is None:
                continue
            # A terminal that fails to write must not stop the drain.
            try:
                terminal.write(line)
                terminal.flush()
            except Exception:
                pass

    threads = [threading.Thread(target=target) for target in (send, collect, relay)]
    for thread in threads:
        thread.daemon = True
        thread.start()
    try:
        process.wait()
    except BaseException:
        _stop(process)
        raise
    finally:
        for thread in threads:
            thread.join(timeout=_INTERRUPT_GRACE_SECONDS)
    return "".join(reply), "".join(log)


def _stop(process: subprocess.Popen[str]) -> None:
    """Interrupt a running task as Ctrl-C would, and kill it if it stays.

    The task has the grace period to stop, its log still draining; a second
    interrupt in that time kills it at once.
    """

    if process.poll() is not None:
        return
    try:
        process.send_signal(signal.SIGINT)
        process.wait(timeout=_INTERRUPT_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    except BaseException:
        process.kill()
        process.wait()
        raise


def _task_executable(task: str) -> str:
    """The executable that runs ``task``, found by the Rust resolver."""

    try:
        path = _native().task_executable(task, dict(os.environ), sys.prefix)
    except FileNotFoundError as error:
        raise TaskBinaryNotFoundError(str(error)) from None
    return os.fspath(path)


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


def _display_value(value: ParameterData) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return format(value, ".15g")
    if isinstance(value, (str, int)):
        return str(value)
    if isinstance(value, (list, tuple)):
        return ",".join(_display_value(item) for item in value)
    if isinstance(value, Mapping):
        import json

        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return str(value)


__all__ = [
    "TaskBinaryNotFoundError",
    "TaskCompletion",
    "TaskExecutionError",
    "TaskInvocationError",
    "TaskResultError",
    "run",
]
