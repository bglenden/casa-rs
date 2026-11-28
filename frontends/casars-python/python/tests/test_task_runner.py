"""Python dispatch, workspace isolation, and interrupt cleanup."""

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import shutil
import signal
import sys
from types import SimpleNamespace

import pytest
import numpy as np

from casars import parameters, tasks, tools
from casars.parameters import TaskParameters, _native
from test_image_saved_expression import _manifest
from test_tools_msmetadata import measures, native_dataset


@pytest.fixture
def native_calls(monkeypatch):
    calls = []

    def run(surface, workspace, values, originals, options):
        calls.append((surface, workspace, values, originals, options))
        return SimpleNamespace(
            status="succeeded", logs="", message=None, parameter=None,
            value=lambda: None, completion=lambda *_: None, details=dict,
        )

    monkeypatch.setattr(_native(), "run_native_task", run)
    return calls


def test_draft_and_overrides_reach_the_native_task(native_calls, tmp_path):
    draft = TaskParameters.defaults("flagmanager", workspace=tmp_path)
    draft["vis"] = Path("base.ms")

    completion = draft.run(comment="delegated")
    [(surface, workspace, values, _, _)] = native_calls
    assert completion.successful
    assert surface == "flagmanager"
    assert workspace == tmp_path.resolve()
    assert values == {"vis": ["base.ms"], "comment": "delegated"}
    assert draft["comment"] == ""


def test_loaded_profile_preserves_explicit_values(native_calls, tmp_path):
    draft = TaskParameters.defaults("msexplore", workspace=tmp_path)
    draft.set_many({"vis": "input.ms", "page_spec": "page.json"})
    loaded = parameters.load(draft.save(tmp_path / "msexplore.toml"))

    tasks.run("msexplore", loaded)
    tasks.msexplore(**loaded, datacolumn="data")

    first, second = native_calls
    assert set(first[2]) == {"page_spec", "vis"}
    assert set(second[2]) == {"datacolumn", "page_spec", "vis"}


def test_applycal_only_requests_a_value_from_the_casa_signature(native_calls, tmp_path):
    vis = tmp_path / "target.ms"
    vis.mkdir()

    assert tasks.applycal(vis=vis, applymode="trial", flagbackup=False) is None
    assert tasks.run("applycal", vis=vis, applymode="trial", flagbackup=False).successful

    wrapper, generic = native_calls
    assert wrapper[2] == generic[2]
    assert wrapper[4]["applycal_value_only"] is True
    assert generic[4]["applycal_value_only"] is False


def test_plotms_sends_only_the_parameters_the_wrapper_received(native_calls, tmp_path):
    caltable = tmp_path / "gain.cal"
    caltable.mkdir()

    assert tasks.plotms(caltable=str(caltable), xaxis="frequency", showgui=False) is None
    [(surface, _, values, _, _)] = native_calls
    assert surface == "plotms"
    assert values == {"caltable": str(caltable), "xaxis": "frequency", "showgui": False}


def test_run_rejects_a_draft_for_another_task(tmp_path):
    other = TaskParameters.defaults("imstat", workspace=tmp_path)
    with pytest.raises(ValueError, match="not requested task"):
        tasks.run("flagmanager", other, workspace=tmp_path)


def test_simultaneous_relative_paths_use_each_workspace(native_dataset, tmp_path):
    cwd = Path.cwd()
    workspaces = [tmp_path / "first", tmp_path / "second"]
    for workspace in workspaces:
        workspace.mkdir()
        shutil.copytree(native_dataset, workspace / "input.ms")

    def calibrate(workspace):
        return tasks.run("gencal", workspace=workspace, vis="input.ms",
                         caltable="output.cal", caltype="opac", parameter=[0.05])

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert all(result.successful for result in pool.map(calibrate, workspaces))
    assert all((workspace / "output.cal" / "table.dat").is_file() for workspace in workspaces)
    assert Path.cwd() == cwd


@pytest.mark.parametrize("saved_mask", [False, True])
def test_relative_image_expressions_use_workspace_with_quotes(tmp_path, saved_mask):
    workspace = tmp_path / "workspace with ' and \""
    workspace.mkdir()
    pixels = np.array([[0, 2], [1, 3]], dtype=np.float32)
    image = tools.image()
    assert image.fromarray(str(workspace / "input.im"), pixels)
    assert image.done()
    cwd = Path.cwd()

    assert tasks.run("immath", workspace=workspace, mode="evalexpr",
                     expr='"input.im"*2', outfile="math.im").successful
    assert image.open(str(workspace / "math.im"))
    np.testing.assert_array_equal(image.getchunk(), pixels * 2)
    assert image.done()

    mask = '"input.im">1'
    if saved_mask:
        _manifest(workspace / "cut.im", ImageExpr='"input.im"*2')
        mask = '"cut.im">2'
    assert tasks.run("imsubimage", workspace=workspace, imagename="input.im",
                     mask=mask, outfile="masked.im").successful
    assert image.open(str(workspace / "masked.im"))
    np.testing.assert_array_equal(image.getchunk(getmask=True), pixels > 1)
    assert image.done()
    assert Path.cwd() == cwd


def test_flagdata_list_and_threshold_files_use_the_workspace(native_dataset, monkeypatch):
    workspace = native_dataset.parent
    caller = workspace / "caller"
    caller.mkdir()
    monkeypatch.chdir(caller)
    (workspace / "commands.txt").write_text("mode='manual'\n")
    (workspace / "bad-threshold.txt").write_text("invalid threshold file\n")
    with pytest.raises(tasks.TaskExecutionError, match="devthreshold"):
        tasks.run("flagdata", workspace=workspace, vis=native_dataset.name,
                  mode="rflag", action="apply", timedev="bad-threshold.txt", flagbackup=False)
    result = tasks.run("flagdata", workspace=workspace, vis=native_dataset.name,
                       mode="list", action="apply", inpfile="commands.txt", flagbackup=False)
    assert result.successful
    table = tools.table()
    assert table.open(str(native_dataset))
    assert table.getcol("FLAG").all()
    assert table.done()
    assert Path.cwd() == caller


def test_simanalyze_ignores_visibilities_in_the_callers_directory(native_dataset, monkeypatch):
    workspace = native_dataset.parent / "workspace"
    project = workspace / "analysis"
    project.mkdir(parents=True)
    monkeypatch.chdir(native_dataset.parent)
    with pytest.raises(tasks.TaskExecutionError, match="No MS found to image"):
        tasks.run("simanalyze", workspace=workspace, project=project.name,
                  vis=native_dataset.name, image=True, graphics="none")


def test_sigint_cleans_up_and_second_native_run_succeeds(native_dataset, monkeypatch):
    workspace = native_dataset.parent
    draft = TaskParameters.defaults("tclean", workspace=workspace)
    draft.set_many({"vis": native_dataset.name, "imagename": "last"})
    last = draft.write_last()
    successful = draft.write_last(successful=True)
    before = last.read_bytes(), successful.read_bytes()
    stderr = sys.stderr
    sent = False

    class Terminal:
        def write(self, message):
            nonlocal sent
            if not sent and "tclean started" in message:
                sent = True
                os.kill(os.getpid(), signal.SIGINT)
            return stderr.write(message)

        def flush(self):
            stderr.flush()

    imaging = {
        "vis": native_dataset.name,
        "imsize": 64,
        "cell": "30arcsec",
        "niter": 0,
        "backend": "cpu",
    }
    with monkeypatch.context() as patch:
        patch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "INFO")
        patch.setenv("CASA_RS_LOG_TABLE", str(workspace / "imaging.logtable"))
        patch.setattr(sys, "stderr", Terminal())
        with pytest.raises(KeyboardInterrupt):
            tasks.run("tclean", workspace=workspace, imagename="cancelled", **imaging)

    assert sent
    log = tools.table()
    assert log.open(str(workspace / "imaging.logtable"))
    assert any(message.startswith("tclean interrupted") for message in log.getcol("MESSAGE"))
    assert log.done()
    assert not list(workspace.glob("cancelled.*"))
    for prefix in ("first", "second"):
        completion = tasks.run("tclean", workspace=workspace, imagename=prefix, **imaging)
        assert completion.successful
        assert (workspace / f"{prefix}.image" / "table.dat").is_file()
    assert (last.read_bytes(), successful.read_bytes()) == before
