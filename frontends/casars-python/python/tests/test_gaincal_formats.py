# SPDX-License-Identifier: LGPL-3.0-or-later

import json
from pathlib import Path
import sys
import tarfile

import numpy as np
import pytest

from casars import tasks, tools


TRIAL = (
    Path(__file__).resolve().parents[4]
    / "libs/casacore/casa-ms/tests/fixtures/ms_trial"
)


def prepare_store(tmp_path, monkeypatch, store):
    with tarfile.open(TRIAL / f"{store}.tgz") as archive:
        extract_opts = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract_opts)
    monkeypatch.chdir(tmp_path)


@pytest.mark.parametrize("store", ["ms_trial.ms", "ms_trial.ps.zarr"])
def test_time_binned_point_source_gaincal(tmp_path, monkeypatch, store):
    prepare_store(tmp_path, monkeypatch, store)
    result = tasks.gaincal(
        vis=store, caltable="solved.cal", gaintype="G", calmode="ap", solint="60s",
        refant="0", refantmode="strict", smodel=[1.0, 0.0, 0.0, 0.0], minsnr=3.0,
    )

    selected = result["selectvis"]
    np.testing.assert_array_equal(selected["antennas"], np.arange(5))
    for axis in ("field", "observation", "spw"):
        np.testing.assert_array_equal(selected[axis], [0])
    np.testing.assert_array_equal(selected["scan"], [1])
    np.testing.assert_array_equal(selected["intents"], ["TARGET.ON_SOURCE"])
    np.testing.assert_array_equal(result["solvestats"]["expected"], [2, 2])

    table = tools.table()
    assert table.open("solved.cal")
    try:
        assert table.nrows() == 10
        times = table.getcol("TIME")
        antennas = table.getcol("ANTENNA1")
        gains = table.getcol("CPARAM")[:, 0, :].T
        assert np.unique(times).size == 2
        assert not table.getcol("FLAG").any()
        for column in ("ANTENNA2", "FIELD_ID", "SPECTRAL_WINDOW_ID", "OBSERVATION_ID"):
            np.testing.assert_array_equal(table.getcol(column), np.zeros(10))
        np.testing.assert_array_equal(table.getcol("SCAN_NUMBER"), np.ones(10))
    finally:
        table.close()

    fixture = json.loads((TRIAL / "manifest.json").read_text())
    truth = fixture["corruption"]["true_gains"]
    true_gains = np.array(truth["re"]) + 1j * np.array(truth["im"])
    reference = true_gains[:, :1, :]
    referred = true_gains * reference.conj() / np.abs(reference)
    interval = np.searchsorted(np.unique(times), times)
    ratio = gains / referred[interval, antennas, :]
    # The fixture has a second faint source outside the supplied point model.
    assert np.max(np.abs(np.angle(ratio))) < 0.2
    assert np.max(np.abs(np.abs(ratio) - 1.0)) < 0.2


@pytest.mark.parametrize("store", ["ms_trial.ms", "ms_trial.ps.zarr"])
def test_time_binned_gaincal_uses_casas_interval_origin(tmp_path, monkeypatch, store):
    prepare_store(tmp_path, monkeypatch, store)
    result = tasks.gaincal(
        vis=store, caltable="solved.cal", gaintype="G", calmode="ap", solint="10.0005s",
        refant="0", refantmode="strict", smodel=[1.0, 0.0, 0.0, 0.0], minsnr=0, minblperant=1,
    )
    # CASA offsets the first solution interval by 10 ms. For 10 s
    # integrations and this interval width it expects all 12 intervals.
    np.testing.assert_array_equal(result["solvestats"]["expected"], [12, 12])


def test_gaincal_refuses_an_unavailable_msv4_mode_before_publication(tmp_path, monkeypatch):
    store = "ms_trial.ps.zarr"
    prepare_store(tmp_path, monkeypatch, store)

    with pytest.raises(tasks.TaskParameterError, match="outside the time-binned G") as failure:
        tasks.gaincal(
            vis=store, caltable="solved.cal", gaintype="G", calmode="ap", solint="int",
            refant="0", smodel=[1.0, 0.0, 0.0, 0.0],
        )

    assert failure.value.completion.parameter == "vis"
    assert not Path("solved.cal").exists()
