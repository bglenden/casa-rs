"""The Python runner starts each task's executable directly (#931 PR 3).

Each test fakes the task's executable through its catalog override,
``CASARS_<APP>_BIN``: a script that records what it was given, writes log
lines to stderr and replies as the shared task front end does.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import sys

import pytest

from casars import parameters, tasks
from casars.parameters import TaskParameters

FAKE_TASK = """#!/usr/bin/env python3
import json
import os
from pathlib import Path
import signal
import sys
import time

request = json.load(sys.stdin)
Path(os.environ["FAKE_TASK_LOG"]).write_text(json.dumps({
    "argv": sys.argv[1:],
    "cwd": os.getcwd(),
    "session": os.getsid(0),
    "request": request,
}), encoding="utf-8")
surface = request["surface"]
reply = os.environ.get("FAKE_TASK_REPLY", "succeeded")


def answer(status, exit_code, **fields):
    print(json.dumps({"status": status, "surface": surface, **fields}))
    raise SystemExit(exit_code)


def interrupted(*_):
    Path(os.environ["FAKE_TASK_LOG"] + ".interrupted").write_text("", encoding="utf-8")
    answer("failed", 130, kind="interrupted", message="stopped at a major cycle")


signal.signal(signal.SIGINT, interrupted)
print(f"{surface} started", file=sys.stderr, flush=True)
if "FAKE_TASK_WAIT_FOR" in os.environ:
    # Reply only once the caller has seen the first log line.
    go = Path(os.environ["FAKE_TASK_WAIT_FOR"])
    deadline = time.monotonic() + 20
    while not go.exists():
        if time.monotonic() > deadline:
            answer("failed", 1, kind="execution", message="the log was not streamed")
        time.sleep(0.01)
print(f"{surface} completed", file=sys.stderr, flush=True)
if reply == "succeeded":
    result = json.loads(os.environ.get("FAKE_TASK_RESULT", "{}"))
    answer("succeeded", 0, value={"npts": [12.0]}, result=result)
if reply == "refused":
    answer("failed", 2, kind="validation", message="no such image", parameter="vis")
if reply == "failed":
    answer("failed", 1, kind="execution", message="cannot read the table")
if reply == "signalled":
    os.kill(os.getpid(), signal.SIGKILL)
if reply == "malformed":
    print("not a reply")
    raise SystemExit(0)
"""


@pytest.fixture
def fake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Fake the executables of ``variables`` (catalog override names)."""

    log = tmp_path / "invocation.json"
    monkeypatch.setenv("FAKE_TASK_LOG", str(log))

    def install(*variables: str) -> Path:
        path = tmp_path / "fake-task"
        path.write_text(FAKE_TASK, encoding="utf-8")
        path.chmod(0o755)
        for variable in variables:
            monkeypatch.setenv(variable, str(path))
        return log

    return install


def recorded(log: Path) -> dict:
    return json.loads(log.read_text(encoding="utf-8"))


def test_a_task_runs_its_executable_with_the_values_set(
    tmp_path: Path, fake, capsys: pytest.CaptureFixture[str]
) -> None:
    log = fake("CASARS_FLAGMANAGER_BIN")
    draft = TaskParameters.defaults("flagmanager", workspace=tmp_path)
    draft["vis"] = Path("base.ms")

    completion = tasks.run("flagmanager", draft, comment="from generic runner")

    invocation = recorded(log)
    assert invocation["argv"] == ["--json-run", "-"]
    assert invocation["cwd"] == str(tmp_path)
    assert invocation["request"]["surface"] == "flagmanager"
    values = invocation["request"]["values"]
    assert values["vis"] == ["base.ms"]
    assert values["comment"] == "from generic runner"
    assert completion.successful
    assert completion.status == "succeeded"
    assert completion.value == {"npts": [12.0]}
    assert completion.returncode == 0
    assert completion.workspace == tmp_path
    assert completion.result is not None
    assert completion.result.surface_id == "flagmanager"
    assert completion.products == ()
    # The task's log reached stderr, and the completion keeps a copy.
    assert completion.stderr == "flagmanager started\nflagmanager completed\n"
    assert capsys.readouterr().err == completion.stderr
    # The draft is the caller's, unchanged.
    assert draft["comment"] == "none"


def test_task_parameters_run_delegates_to_the_runner(tmp_path: Path, fake) -> None:
    log = fake("CASARS_FLAGMANAGER_BIN")
    draft = TaskParameters.defaults("flagmanager", workspace=tmp_path)
    draft["vis"] = "delegated.ms"
    assert draft.run(comment="delegated").successful
    assert recorded(log)["request"]["values"]["comment"] == "delegated"


def test_a_wrapper_runs_in_the_current_directory(
    tmp_path: Path, fake, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = fake("CASARS_MSTRANSFORM_BIN")
    output = tmp_path / "selected.ms"
    output.mkdir()
    monkeypatch.chdir(tmp_path)

    completion = tasks.split(vis="input.ms", outputvis="selected.ms")

    invocation = recorded(log)
    # mstransform hosts split too; the request names the surface to run.
    assert invocation["argv"] == ["--json-run", "-"]
    assert invocation["request"]["surface"] == "split"
    assert invocation["cwd"] == str(tmp_path)
    assert completion.result.surface_id == "split"
    assert len(completion.products) == 1
    assert completion.products[0].path == str(output)
    assert completion.products[0].exists is True


def test_a_profile_runs_as_the_wrappers_values(
    tmp_path: Path, fake, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = fake("CASARS_CALIBRATE_BIN")
    monkeypatch.setenv("FAKE_TASK_RESULT", '{"kind": "solve_gain", "report": {}}')
    monkeypatch.chdir(tmp_path)
    draft = TaskParameters.defaults("gaincal", workspace=tmp_path)
    draft.set_many(
        {"vis": "target.ms", "out_table": "target.gcal", "refant": "ea01", "solint": "60s"}
    )
    path = draft.save(tmp_path / "gaincal.toml")

    assert tasks.gaincal(**parameters.load(path)).successful

    invocation = recorded(log)
    # calibrate hosts gaincal among its surfaces and is told which runs.
    assert invocation["argv"] == ["gaincal", "--json-run", "-"]
    values = invocation["request"]["values"]
    assert values["vis"] == ["target.ms"]
    assert values["out_table"] == "target.gcal"
    assert values["solint"] == "60s"


def test_the_log_streams_while_the_task_runs(
    tmp_path: Path, fake, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake("CASARS_FLAGMANAGER_BIN")
    go = tmp_path / "go"
    monkeypatch.setenv("FAKE_TASK_WAIT_FOR", str(go))

    class Terminal:
        """sys.stderr that lets the task finish once it sees the first line."""

        def __init__(self) -> None:
            self.lines: list[str] = []

        def write(self, text: str) -> int:
            self.lines.append(text)
            go.touch()
            return len(text)

        def flush(self) -> None:
            pass

    terminal = Terminal()
    monkeypatch.setattr(sys, "stderr", terminal)
    completion = tasks.run("flagmanager", workspace=tmp_path, vis="streamed.ms")
    assert completion.successful, completion.describe()
    assert terminal.lines == ["flagmanager started\n", "flagmanager completed\n"]


@pytest.mark.parametrize(
    ("reply", "status", "message", "parameter"),
    [
        ("refused", "refused", "no such image", "vis"),
        ("failed", "failed", "cannot read the table", None),
        ("signalled", "signalled", None, None),
        ("malformed", "malformed_reply", "the task's reply is not valid", None),
    ],
)
def test_a_task_that_does_not_succeed_raises_with_its_outcome(
    tmp_path: Path,
    fake,
    monkeypatch: pytest.MonkeyPatch,
    reply: str,
    status: str,
    message: str | None,
    parameter: str | None,
) -> None:
    fake("CASARS_FLAGMANAGER_BIN")
    monkeypatch.setenv("FAKE_TASK_REPLY", reply)
    with pytest.raises(tasks.TaskExecutionError) as failure:
        tasks.run("flagmanager", workspace=tmp_path, vis="failed.ms")
    completion = failure.value.completion
    assert completion.status == status
    assert not completion.successful
    assert completion.parameter == parameter
    if message is not None:
        assert completion.message.startswith(message), completion.message
    assert "flagmanager started" in completion.stderr
    if reply == "signalled":
        assert completion.returncode == -signal.SIGKILL
        assert "ended by signal 9" in str(failure.value)


@pytest.mark.parametrize("outcome", ["success", "failure", "interruption"])
def test_a_python_task_call_leaves_existing_last_files_untouched(
    tmp_path: Path, fake, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    log = fake("CASARS_FLAGMANAGER_BIN")
    saved = TaskParameters.defaults("flagmanager", workspace=tmp_path)
    saved["vis"] = "saved.ms"
    last = saved.write_last()
    last_successful = saved.write_last(successful=True)
    before = (last.read_bytes(), last_successful.read_bytes())
    if outcome == "failure":
        monkeypatch.setenv("FAKE_TASK_REPLY", "failed")
    if outcome == "interruption":
        # Ctrl-C once the task has started: Python's own SIGINT.
        go = tmp_path / "go"
        monkeypatch.setenv("FAKE_TASK_WAIT_FOR", str(go))

        class Terminal:
            def write(self, text: str) -> int:
                os.kill(os.getpid(), signal.SIGINT)
                return len(text)

            def flush(self) -> None:
                pass

        monkeypatch.setattr(sys, "stderr", Terminal())

    def call() -> tasks.TaskCompletion:
        return tasks.run("flagmanager", workspace=tmp_path, vis="other.ms")

    if outcome == "success":
        assert call().successful
    elif outcome == "failure":
        with pytest.raises(tasks.TaskExecutionError):
            call()
    else:
        with pytest.raises(KeyboardInterrupt):
            call()
        # The interrupt reached the task, which stopped. The task has its
        # own session, so a terminal's Ctrl-C reaches it only through Python.
        assert Path(str(log) + ".interrupted").exists()
        assert recorded(log)["session"] != os.getsid(0)
    assert (last.read_bytes(), last_successful.read_bytes()) == before
    # Reading Last stays an explicit choice.
    assert TaskParameters.last("flagmanager", workspace=tmp_path)["vis"] == ["saved.ms"]


def test_a_missing_executable_names_its_remedy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASARS_FLAGMANAGER_BIN", raising=False)
    monkeypatch.setenv("CASARS_SUITE_ROOT", str(tmp_path))
    with pytest.raises(tasks.TaskBinaryNotFoundError, match="CASARS_SUITE_ROOT"):
        tasks.run("flagmanager", workspace=tmp_path, vis="input.ms")

    # Without a suite, a development checkout names the build it lacks; an
    # install built outside a checkout names the suite and the override.
    monkeypatch.delenv("CASARS_SUITE_ROOT")
    monkeypatch.setenv("CARGO_TARGET_DIR", str(tmp_path / "empty-target"))
    with pytest.raises(tasks.TaskBinaryNotFoundError) as missing:
        tasks.run("flagmanager", workspace=tmp_path, vis="input.ms")
    message = str(missing.value)
    assert "CASARS_FLAGMANAGER_BIN" in message
    assert (
        "`cargo build -p casa-ms --bin flagmanager`" in message
        or "CASARS_SUITE_ROOT" in message
    ), message

    monkeypatch.setenv("CASARS_FLAGMANAGER_BIN", str(tmp_path / "missing"))
    with pytest.raises(tasks.TaskBinaryNotFoundError, match="not a file"):
        tasks.run("flagmanager", workspace=tmp_path, vis="input.ms")


def test_run_rejects_parameters_for_another_task(tmp_path: Path) -> None:
    other = TaskParameters.defaults("imstat", workspace=tmp_path)
    with pytest.raises(ValueError, match="not requested task"):
        tasks.run("flagmanager", other, workspace=tmp_path)


def test_a_successful_process_with_a_malformed_result_is_rejected(
    tmp_path: Path, fake, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake("CASARS_IMAGER_BIN")
    monkeypatch.setenv("FAKE_TASK_RESULT", '"runner stdout"')
    monkeypatch.chdir(tmp_path)
    with pytest.raises(tasks.TaskResultError, match="successful but invalid result"):
        tasks.imager(vis="input.ms", imagename="dirty")


def test_a_parameter_its_gridder_does_not_read_is_refused_before_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CASARS_IMAGER_BIN", str(tmp_path / "missing-imager"))
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="wprojplanes"):
        tasks.imager(vis="input.ms", imagename="products/image", wprojplanes=4)
