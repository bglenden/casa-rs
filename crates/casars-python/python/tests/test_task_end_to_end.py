"""Python runs a real task executable (#931 PR 3).

The resolver finds the executable itself: the override, an installed suite,
or this development checkout's build. Without one the tests skip, unless
``CASARS_REQUIRE_TASK_BINARIES`` is set, as in CI's job that builds them;
then a missing executable fails the run.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys
import tarfile

import pytest

from casars import tasks
from casars.parameters import TaskParameters
from casars.tasks import _runner

REPO_ROOT = Path(__file__).resolve().parents[4]
SMALL_MS_ARCHIVE = Path(
    os.environ.get(
        "CASARS_SMALL_MS_ARCHIVE",
        REPO_ROOT / "crates/casa-ms/tests/fixtures/mssel_test_small.ms.tgz",
    )
)


@pytest.fixture
def flagmanager() -> str:
    try:
        return _runner._task_executable("flagmanager")
    except tasks.TaskBinaryNotFoundError as missing:
        if os.environ.get("CASARS_REQUIRE_TASK_BINARIES"):
            raise
        pytest.skip(f"no flagmanager executable: {missing}")


@pytest.fixture
def small_ms(tmp_path: Path) -> Path:
    with tarfile.open(SMALL_MS_ARCHIVE, "r:gz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    return tmp_path / "mssel_test_small.ms"


def test_a_task_runs_its_real_executable(
    flagmanager: str,
    small_ms: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workspace = small_ms.parent
    saved = TaskParameters.defaults("flagmanager", workspace=workspace)
    saved.set_many({"vis": small_ms.name, "mode": "list"})
    last = saved.write_last()
    last_successful = saved.write_last(successful=True)
    before = (last.read_bytes(), last_successful.read_bytes())
    monkeypatch.chdir(workspace)
    # The task logs at INFO, which stderr shows only when asked to.
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "INFO")

    completion = tasks.flagmanager(vis=small_ms.name, mode="list", comment="from Python")

    assert completion.successful, completion.describe()
    assert completion.command[0] == flagmanager
    assert completion.command[1:] == ("--json-run", "-")
    # flagmanager returns the versions in list mode, the main table first.
    assert completion.value[0]["name"] == "main"
    # The log streamed to stderr as the task ran.
    assert "flagmanager started" in completion.stderr
    assert "flagmanager completed" in completion.stderr
    assert completion.stderr in capsys.readouterr().err
    # A Python call neither loads nor saves Last.
    assert (last.read_bytes(), last_successful.read_bytes()) == before


def test_a_real_refusal_names_its_parameter(
    flagmanager: str, small_ms: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(small_ms.parent)
    with pytest.raises(tasks.TaskExecutionError) as failure:
        tasks.flagmanager(vis=small_ms.name, mode="restore")
    completion = failure.value.completion
    assert completion.status == "refused", completion.describe()
    assert completion.parameter == "versionname"
    assert completion.returncode == 2
