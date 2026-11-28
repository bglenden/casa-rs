# SPDX-License-Identifier: LGPL-3.0-or-later

from pathlib import Path
import sys
import tarfile

import pytest

from casars import tasks, tools


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


@pytest.mark.parametrize(("selectors_v2", "selectors_v4"), [
    (("0", "1", "1:0"), ("0", "1", "1:0")),
    (("ppdisk", "VLA02", "high:0"), ("ppdisk_0", "VLA02_N02", "high_1:0")),
])
def test_selected_file_listing_uses_the_same_physical_rows(
    paired_calibration, selectors_v2, selectors_v4,
):
    ms_v2, ms_v4 = paired_calibration
    table = tools.table()
    assert table.open("input.cal", nomodify=False)
    for row in range(table.nrows()):
        assert table.putcell("FIELD_ID", row, 0)
    assert table.close()

    for vis, selectors, out in (
        (ms_v2, selectors_v2, "reference.txt"), (ms_v4, selectors_v4, "candidate.txt"),
    ):
        field, antenna, spw = selectors
        assert tasks.listcal(
            vis=vis, caltable="input.cal", listfile=out,
            field=field, antenna=antenna, spw=spw, pagerows=0,
        ) is None
    actual = Path("candidate.txt").read_text()
    assert "SpwID = 0" not in actual and "SpwID = 1" in actual
    assert "Ant = VLA02" in actual and "ppdisk" in actual
    assert "Ant = VLA01" not in actual and "Ant = VLA03" not in actual
    assert actual == Path("reference.txt").read_text().replace(
        f"MS name = {Path(ms_v2).absolute()}", f"MS name = {Path(ms_v4).absolute()}",
    )


@pytest.mark.parametrize(("subtable", "column", "selector", "identity"), [
    ("ANTENNA", "POSITION", {"antenna": "0"}, "antenna identity"),
    ("FIELD", "PHASE_DIR", {"field": "0"}, "field identity"),
    ("FIELD", "NUM_POLY", {"field": "0"}, "polynomial FIELD"),
])
def test_selected_msv4_listing_refuses_incompatible_calibration_identity(
    paired_calibration, subtable, column, selector, identity,
):
    _, ms_v4 = paired_calibration
    table = tools.table()
    assert table.open(f"input.cal/{subtable}", nomodify=False)
    before = table.getcell(column, 0)
    assert table.putcell(column, 0, before + 1)
    assert table.close()

    with pytest.raises(tasks.TaskParameterError, match=identity) as failure:
        tasks.listcal(vis=ms_v4, caltable="input.cal", listfile="out.txt", pagerows=0, **selector)
    assert failure.value.completion.parameter == "vis"
    assert not Path("out.txt").exists()


def test_paged_msv4_file_listing_returns_typed_refusal(paired_calibration):
    _, ms_v4 = paired_calibration
    with pytest.raises(tasks.TaskParameterError, match="pagerows=0") as failure:
        tasks.listcal(vis=ms_v4, caltable="input.cal", listfile="out.txt", field="0", pagerows=1)
    assert failure.value.completion.parameter == "vis"
    assert not Path("out.txt").exists()


def test_unreadable_msv2_listing_preserves_the_same_diagnostic(paired_calibration):
    directory = Path("unreadable.ms")
    directory.mkdir()
    messages = []
    for pagerows in (0, 1):
        with pytest.raises(tasks.TaskExecutionError) as failure:
            tasks.listcal(
                vis=str(directory), caltable="input.cal", listfile="out.txt", pagerows=pagerows,
            )
        assert failure.value.completion.parameter == "vis"
        messages.append(failure.value.completion.message)
        assert not Path("out.txt").exists()
    assert messages[0] == messages[1]
    assert messages[0].startswith("storage error: table control file is missing")
