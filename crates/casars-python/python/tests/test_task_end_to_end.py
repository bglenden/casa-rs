"""Python runs a real task executable (#931 PR 3).

The resolver finds the executable itself: the override, an installed suite,
or this development checkout's build. Without one the tests skip, unless
``CASARS_REQUIRE_TASK_BINARIES`` is set, as in CI's job that builds them;
then a missing executable fails the run.
"""

from __future__ import annotations

import os
import shutil
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


def test_imhead_preserves_python_scalar_header_types(
    image: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    require_executables("imhead")
    monkeypatch.chdir(image.parent)
    assert tasks.imhead(image.name, mode="put", hdkey="count", hdvalue=42) is None
    assert tasks.imhead(image.name, mode="put", hdkey="real", hdvalue=1.25) is None
    assert tasks.imhead(image.name, mode="put", hdkey="literal", hdvalue="  42  ") is None
    count = tasks.imhead(image.name, mode="get", hdkey="count")
    real = tasks.imhead(image.name, mode="get", hdkey="real")
    assert type(count) is int and count == 42
    assert type(real) is float and real == 1.25
    assert tasks.imhead(image.name, mode="get", hdkey="literal") == "  42  "
    assert tasks.imhead(image.name, mode="put", hdkey="crpix1", hdvalue=3.5) is None
    assert tasks.imhead(image.name, mode="get", hdkey="crpix1") == 3.5
    assert tasks.imhead(image.name, mode="put", hdkey="ctype1", hdvalue="new axis") is None
    assert tasks.imhead(image.name, mode="get", hdkey="ctype1") == "new axis"


def test_imhead_history_is_visible_with_default_logging(
    image: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    require_executables("imhead")
    monkeypatch.chdir(image.parent)
    for name in ("CASA_RS_LOG_TABLE", "CASA_RS_LOG_TABLE_PRIORITY", "CASA_RS_LOG_STDERR_PRIORITY"):
        monkeypatch.delenv(name, raising=False)
    assert tasks.imhead(image.name, mode="put", hdkey="object", hdvalue="M31") is None
    capsys.readouterr()
    assert tasks.imhead(image.name, mode="history") is None
    assert 'Modified object from "python-fixture" to "M31"' in capsys.readouterr().err


def test_imhead_custom_array_from_python(image: Path) -> None:
    require_executables("imhead")
    assert tasks.imhead(str(image), mode="add", hdkey="values", hdvalue=[1, 2.5]) is None
    with pytest.raises(tasks.TaskExecutionError, match="Unhandled data type Array<double>"):
        tasks.imhead(str(image), mode="get", hdkey="values")
    assert tasks.imhead(str(image), mode="del", hdkey="values", hdvalue=[1, 2]) is None


def test_imhead_custom_records_from_python(image: Path) -> None:
    import numpy as np

    require_executables("imhead")
    value = {"nested": {"values": [1, 2], "label": "test", "matrix": [[1, 2], [3, 4]]}}
    assert tasks.imhead(str(image), mode="put", hdkey="custom", hdvalue=value) is None
    nested = tasks.imhead(str(image), mode="get", hdkey="custom")["nested"]
    assert nested["label"] == "test"
    np.testing.assert_array_equal(nested["values"], np.array([1, 2], dtype=np.int64))
    np.testing.assert_array_equal(nested["matrix"], [[1, 2], [3, 4]])


def test_imhead_casa_record_multidimensional_arrays(tmp_path: Path) -> None:
    import numpy as np

    require_executables("imhead")
    fixture = REPO_ROOT / "crates/casars-imexplore/tests/data/imhead_crpix/custom_record.im"
    image = tmp_path / "record.im"
    shutil.copytree(fixture, image)
    record = tasks.imhead(str(image), mode="get", hdkey="custom")
    np.testing.assert_array_equal(record["matrix"], [[1, 2, 3], [4, 5, 6]])
    np.testing.assert_array_equal(record["complex"], [[1 + 2j, 3 + 4j], [5 + 6j, 7 + 8j]])


def test_imhead_put_accepts_rest_frequency_quantity_record(tmp_path: Path) -> None:
    require_executables("imhead")
    fixture = REPO_ROOT / "crates/casars-imexplore/tests/data/imhead_crpix/normal.im"
    image = tmp_path / "spectral.im"
    shutil.copytree(fixture, image)
    assert tasks.imhead(str(image), mode="put", hdkey="restfreq", hdvalue={"value": 1.5, "unit": "GHz"}) is None
    assert tasks.imhead(str(image), mode="get", hdkey="restfreq") == {"value": 1.5e9, "unit": "Hz"}
    assert tasks.imhead(str(image), mode="put", hdkey="crval4", hdvalue={"value": 1.5, "unit": "GHz"}) is None
    assert tasks.imhead(str(image), mode="get", hdkey="crval4") == {"value": 1.5e9, "unit": "Hz"}
    assert tasks.imhead(str(image), mode="put", hdkey="cunit1", hdvalue="deg") is None
    assert tasks.imhead(str(image), mode="get", hdkey="cunit1") == "deg"
    assert tasks.imhead(str(image), mode="get", hdkey="restfreq") == {"value": 1.5e9, "unit": "Hz"}
    assert tasks.imhead(str(image), mode="put", hdkey="reffreqtype", hdvalue="B") is None
    assert tasks.imhead(str(image), mode="get", hdkey="reffreqtype") == "BARY"


def test_imhead_edits_and_removes_a_restoring_beam(tmp_path: Path) -> None:
    require_executables("imhead")
    fixture = REPO_ROOT / "crates/casars-imexplore/tests/data/imhead_crpix/beam.im"
    image = tmp_path / "beam.im"
    shutil.copytree(fixture, image)
    assert tasks.imhead(str(image), mode="put", hdkey="bmaj", hdvalue={"value": 40.0, "unit": "arcsec"}) is None
    assert tasks.imhead(str(image), mode="get", hdkey="bmaj") == {"value": 40.0, "unit": "arcsec"}
    assert tasks.imhead(str(image), mode="del", hdkey="bpa", hdvalue=[1, 2]) is None
    assert "restoringbeam" not in tasks.imhead(str(image), mode="summary")


def test_msconvert_converts_both_ways_and_reports_its_losses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    require_executables("msconvert")
    fixtures = REPO_ROOT / "crates/casa-ms/tests/fixtures/ms_trial"
    with tarfile.open(fixtures / "ms_trial.ms.tgz", "r:gz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    monkeypatch.chdir(tmp_path)

    # Without an outputvis, the output is named after vis as XRADIO names
    # a processing set.
    converted = tasks.msconvert(vis="ms_trial.ms")
    assert converted.successful, converted.describe()
    assert converted.value["informat"] == "msv2"
    assert converted.value["outformat"] == "msv4"
    output = Path(converted.value["outputvis"]).resolve()
    assert output == (tmp_path / "ms_trial.ps.zarr").resolve()
    assert converted.value["partitions"] == 1
    # What MSv4 cannot hold is taken and reported.
    assert "SIGMA is not carried" in converted.value["losses"]
    assert "HISTORY is not carried" in converted.value["losses"]
    assert (tmp_path / "ms_trial.ps.zarr" / "ms_trial_0" / "pointing_xds").is_dir()

    # Back to MSv2: its default name is the original's, which is kept.
    with pytest.raises(tasks.TaskParameterError) as failure:
        tasks.msconvert(vis="ms_trial.ps.zarr")
    assert failure.value.completion.parameter == "outputvis"
    back = tasks.msconvert(vis="ms_trial.ps.zarr", outputvis="back.ms")
    assert back.value["outformat"] == "msv2", back.describe()
    assert (tmp_path / "back.ms" / "table.dat").is_file()

    # XRADIO's partition_scheme and with_pointing; an existing output
    # needs overwrite.
    plain = tasks.msconvert(
        vis="ms_trial.ms",
        partitionscheme=["FIELD_ID", "SCAN_NUMBER"],
        withpointing=False,
        overwrite=True,
    )
    assert plain.successful, plain.describe()
    assert not (tmp_path / "ms_trial.ps.zarr" / "ms_trial_0" / "pointing_xds").exists()
