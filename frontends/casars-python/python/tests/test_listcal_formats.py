# SPDX-License-Identifier: LGPL-3.0-or-later

from pathlib import Path
import sys
import tarfile

import pytest

from casars import tasks


TRIAL = (
    Path(__file__).resolve().parents[4]
    / "libs/casacore/casa-ms/tests/fixtures/ms_trial"
)


@pytest.fixture
def paired_calibration(tmp_path, monkeypatch):
    ms_v2 = "ms_trial_lines.ms"
    ms_v4 = "ms_trial_lines.ps.zarr"
    for store in (ms_v2, ms_v4):
        with tarfile.open(TRIAL / f"{store}.tgz") as archive:
            kwargs = {"filter": "data"} if sys.version_info >= (3, 12) else {}
            archive.extractall(tmp_path, **kwargs)
    monkeypatch.chdir(tmp_path)
    assert tasks.gencal(vis=ms_v2, caltable="input.cal", caltype="amp", parameter=[2.0]) is None
    return ms_v2, ms_v4


@pytest.mark.parametrize("spw", ["", "*"])
def test_default_file_listing_uses_both_msv4_windows(paired_calibration, spw):
    ms_v2, ms_v4 = paired_calibration
    for vis, output in ((ms_v2, "reference.txt"), (ms_v4, "candidate.txt")):
        assert tasks.listcal(
            vis=vis, caltable="input.cal", listfile=output, spw=spw, pagerows=0,
        ) is None

    expected = Path("reference.txt").read_text()
    actual = Path("candidate.txt").read_text()
    assert "SpwID = 0" in actual and "SpwID = 1" in actual
    assert "Listed 8 antenna solutions." in actual
    path_label = f"MS name = {Path(ms_v4).absolute()}"
    assert actual.count(path_label) == 2
    assert actual == expected.replace(
        f"MS name = {Path(ms_v2).absolute()}", path_label,
    )


def test_selected_msv4_file_listing_returns_typed_refusal(paired_calibration):
    _, ms_v4 = paired_calibration
    with pytest.raises(tasks.TaskParameterError, match="default selectors") as failure:
        tasks.listcal(vis=ms_v4, caltable="input.cal", listfile="out.txt", field="0", pagerows=0)
    assert failure.value.completion.parameter == "vis"
    assert not Path("out.txt").exists()
