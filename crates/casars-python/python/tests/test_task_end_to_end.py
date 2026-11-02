"""Python runs a real task executable (#931 PR 3).

The resolver finds the executable itself: the override, an installed suite,
or this development checkout's build. Without one the tests skip, unless
``CASARS_REQUIRE_TASK_BINARIES`` is set, as in CI's job that builds them;
then a missing executable fails the run.
"""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tarfile

import pytest

from casars import tasks
from casars.data import Image
from casars.parameters import TaskParameters
from casars.tasks import _runner

REPO_ROOT = Path(__file__).resolve().parents[4]
SMALL_MS_ARCHIVE = Path(
    os.environ.get(
        "CASARS_SMALL_MS_ARCHIVE",
        REPO_ROOT / "crates/casa-ms/tests/fixtures/mssel_test_small.ms.tgz",
    )
)


def task_executable(name: str) -> str:
    try:
        return _runner._task_executable(name)
    except tasks.TaskBinaryNotFoundError as missing:
        if os.environ.get("CASARS_REQUIRE_TASK_BINARIES"):
            raise
        pytest.skip(f"no {name} executable: {missing}")


@pytest.fixture
def flagmanager() -> str:
    return task_executable("flagmanager")


@pytest.fixture
def flagdata() -> str:
    return task_executable("flagdata")


def require_executables(*names: str) -> None:
    for name in names:
        try:
            _runner._task_executable(name)
        except tasks.TaskBinaryNotFoundError as missing:
            if os.environ.get("CASARS_REQUIRE_TASK_BINARIES"):
                raise
            pytest.skip(f"no {name} executable: {missing}")


@pytest.fixture
def image(tmp_path: Path) -> Path:
    """A small Jy/beam image, written by the Rust fixture example."""
    root = tmp_path / "fixtures"
    example = ["--example", "make_python_fixtures", "--", str(root)]
    subprocess.run(
        ["cargo", "run", "-q", "-p", "casars-python", *example], cwd=REPO_ROOT, check=True
    )
    return root / "python_fixture.image"


@pytest.fixture
def calibrate() -> str:
    return task_executable("gencal")


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

    completion = tasks.run("flagmanager", vis=small_ms.name, mode="list", comment="from Python")

    assert completion.successful, completion.describe()
    assert completion.command[0] == flagmanager
    assert completion.command[1:] == ("--json-run", "-")
    # flagmanager returns CASA's version list: no saved versions yet, and
    # the MeasurementSet's absolute path.
    assert completion.value == {"MS": str(small_ms.resolve())}
    # The log streamed to stderr as the task ran.
    assert "flagmanager started" in completion.stderr
    assert "flagmanager completed" in completion.stderr
    assert completion.stderr in capsys.readouterr().err
    # A Python call neither loads nor saves Last.
    assert (last.read_bytes(), last_successful.read_bytes()) == before


def test_flagdata_returns_casas_summary_and_empty_dictionary(
    flagdata: str, small_ms: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # #931 PR 4c: CASA's flagdata returns its summary in summary mode and
    # {} in its other modes.
    monkeypatch.chdir(small_ms.parent)
    summary = tasks.flagdata(vis=small_ms.name, mode="summary")
    assert list(summary) == [
        "antenna",
        "array",
        "correlation",
        "field",
        "flagged",
        "name",
        "observation",
        "scan",
        "spw",
        "total",
        "type",
    ]
    assert (summary["name"], summary["type"]) == ("Summary", "summary")
    assert isinstance(summary["total"], float) and summary["total"] > 0
    assert all(set(counts) == {"flagged", "total"} for counts in summary["antenna"].values())

    assert tasks.flagdata(vis=small_ms.name, mode="manual", action="") == {}
    assert tasks.flagdata(vis=small_ms.name, mode="summary")["flagged"] == summary["flagged"]


def test_gencal_takes_casas_signature_and_returns_none(
    calibrate: str, small_ms: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # #931 PR 4d: the calibration tasks take CASA's parameters; gencal
    # returns None, as CASA's does, and writes its table.
    monkeypatch.chdir(small_ms.parent)
    assert (
        tasks.gencal(small_ms.name, "opacity.cal", "opac", spw="1", parameter=[0.05]) is None
    )
    assert (small_ms.parent / "opacity.cal").is_dir()
    # A CASA caltype casa-rs does not implement yet is refused naming #979.
    with pytest.raises(tasks.TaskParameterError) as failure:
        tasks.gencal(vis=small_ms.name, caltable="amp.cal", caltype="amp", parameter=[2.0])
    assert failure.value.completion.parameter == "caltype"
    assert "#979" in str(failure.value)


def test_a_real_refusal_names_its_parameter(
    flagmanager: str, small_ms: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(small_ms.parent)
    # A refusal is a ValueError (#931).
    with pytest.raises(tasks.TaskParameterError) as failure:
        tasks.flagmanager(vis=small_ms.name, mode="restore")
    assert isinstance(failure.value, ValueError)
    completion = failure.value.completion
    assert completion.status == "refused", completion.describe()
    assert completion.parameter == "versionname"
    assert completion.returncode == 2


def test_casas_spellings_of_a_default_run_as_casa_runs_them(
    image: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    require_executables("immath", "imhead")
    monkeypatch.chdir(image.parent)
    # CASA reads varnames=[] as its '', and ignores sigma outside its
    # polarization modes (#931).
    common = {"imagename": [image.name], "expr": "IM0*2"}
    assert tasks.immath(outfile="names.im", varnames=[], **common) is True
    assert tasks.immath(outfile="sigma.im", sigma="0Jy/beam", **common) is True
    # Any other value is refused before the task starts, as a ValueError.
    with pytest.raises(ValueError, match="varnames.*#977"):
        tasks.immath(imagename=[image.name], outfile="a.im", expr="A*2", varnames=["A"])
    assert not (image.parent / "a.im").exists()
    # hdvalue='' is CASA's default and a value: it clears the unit.
    assert Image.open(image).units == "Jy/beam"
    assert tasks.imhead(image.name, mode="put", hdkey="bunit", hdvalue="") is None
    assert Image.open(image).units == ""


def test_a_missing_input_image_is_a_parameter_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    require_executables("imstat")
    monkeypatch.chdir(tmp_path)
    # CASA's imstat('does-not-exist.im') is a parameter error (#931).
    with pytest.raises(tasks.TaskParameterError) as failure:
        tasks.imstat("does-not-exist.im")
    completion = failure.value.completion
    assert completion.parameter == "imagename"
    assert completion.returncode == 2


def test_msconvert_converts_both_ways_and_reports_its_losses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    require_executables("msconvert")
    fixtures = REPO_ROOT / "crates/casa-ms/tests/fixtures/ms_trial"
    with tarfile.open(fixtures / "ms_trial.ms.tgz", "r:gz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    monkeypatch.chdir(tmp_path)

    # Without an outputvis, the output is named after vis for its format.
    converted = tasks.msconvert(vis="ms_trial.ms")
    assert converted.successful, converted.describe()
    assert converted.value["informat"] == "msv2"
    assert converted.value["outformat"] == "msv4"
    output = Path(converted.value["outputvis"]).resolve()
    assert output == (tmp_path / "ms_trial.ms4").resolve()
    assert converted.value["partitions"] == 1
    # What MSv4 cannot hold is taken and reported.
    assert "SIGMA is not carried" in converted.value["losses"]
    assert "HISTORY is not carried" in converted.value["losses"]
    assert (tmp_path / "ms_trial.ms4" / "zarr.json").is_file()

    # Back to MSv2, the other format; an existing output needs overwrite.
    back = tasks.msconvert(vis="ms_trial.ms4")
    assert back.value["outformat"] == "msv2", back.describe()
    assert (tmp_path / "ms_trial.ms2" / "table.dat").is_file()
    with pytest.raises(tasks.TaskParameterError) as failure:
        tasks.msconvert(vis="ms_trial.ms4")
    assert failure.value.completion.parameter == "outputvis"
    assert tasks.msconvert(vis="ms_trial.ms4", overwrite=True).successful
