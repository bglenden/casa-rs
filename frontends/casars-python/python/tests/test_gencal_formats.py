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


@pytest.fixture(params=["ms_trial.ms", "ms_trial.ps.zarr"])
def store(request, tmp_path, monkeypatch):
    with tarfile.open(TRIAL / f"{request.param}.tgz") as archive:
        kwargs = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **kwargs)
    monkeypatch.chdir(tmp_path)
    return request.param


def read_parameters(path, column, source):
    table = tools.table()
    assert table.open(path)
    try:
        assert table.getkeyword("MSName") == source
        np.testing.assert_array_equal(table.getcol("ANTENNA1"), np.arange(5))
        np.testing.assert_array_equal(table.getcol("SPECTRAL_WINDOW_ID"), np.zeros(5))
        assert not table.getcol("FLAG").any()
        return table.getcol(column)[:, 0, :]
    finally:
        assert table.close()


def test_specified_gain_create_and_phase_append(store):
    assert tasks.gencal(
        vis=store, caltable="gain.cal", caltype="amp", spw="0",
        antenna="0~1", pol="X", parameter=[0.8, 1.2],
    ) is None
    gains = np.ones((2, 5), dtype=np.complex64)
    gains[0, :2] = [0.8, 1.2]
    np.testing.assert_allclose(read_parameters("gain.cal", "CPARAM", store), gains)
    assert tasks.gencal(
        vis=store, caltable="gain.cal", caltype="ph", spw="0",
        antenna="1", pol="X", parameter=[20.0],
    ) is None
    gains[0, 1] *= np.exp(1j * np.deg2rad(20.0))
    np.testing.assert_allclose(read_parameters("gain.cal", "CPARAM", store), gains, rtol=3e-7)


@pytest.mark.parametrize(("mode", "value", "polarization", "receptor_count"), [
    ("sbd", 2.0, "Y", 2),
    ("mbd", 3.0, "Y", 2),
    ("opac", 0.02, "", 1),
])
def test_specified_real_prior(store, mode, value, polarization, receptor_count):
    assert tasks.gencal(
        vis=store, caltable="prior.cal", caltype=mode, spw="0",
        antenna="1", pol=polarization, parameter=[value],
    ) is None
    values = np.zeros((receptor_count, 5), dtype=np.float32)
    values[-1, 1] = value
    np.testing.assert_allclose(read_parameters("prior.cal", "FPARAM", store), values)


@pytest.mark.parametrize("store", ["ms_trial.ps.zarr"], indirect=True)
def test_specified_prior_refuses_unrepresentable_msv4_namespace(store):
    header = (
        Path(store)
        / "ms_trial_0/field_and_source_base_xds/FIELD_PHASE_CENTER_DIRECTION/zarr.json"
    )
    metadata = json.loads(header.read_text())
    metadata["attributes"]["frame"] = "icrs"
    header.write_text(json.dumps(metadata))
    with pytest.raises(tasks.TaskParameterError, match="UVW frame") as failure:
        tasks.gencal(vis=store, caltable="prior.cal", caltype="amp", parameter=[1.0])
    assert failure.value.completion.parameter == "vis"
    assert not Path("prior.cal").exists()
