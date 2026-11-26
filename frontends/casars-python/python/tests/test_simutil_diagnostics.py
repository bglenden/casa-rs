# SPDX-License-Identifier: LGPL-3.0-or-later
"""Simulation-planning calculations against CASA 6.7.6.14 and its equations."""

import json
import math
from pathlib import Path

import pytest

from casars.simutil import simutil


CONFIGS = Path(__file__).resolve().parents[3] / "casa-simulation/tests/data/readantenna"
CASES = json.loads((Path(__file__).parent / "data/casa_simutil_diagnostics.json").read_text())["cases"]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_numerical_diagnostics_match_casa(case):
    params = dict(case["params"])
    if "antennalist" in params:
        params["antennalist"] = str(CONFIGS / params["antennalist"])
    method = getattr(simutil(), case["method"])
    if "error" in case:
        with pytest.raises((ValueError, RuntimeError)) as error:
            method(**params)
        assert str(error.value) == case["error"]
    else:
        value = method(**params)
        expected = case["expected"]
        if isinstance(expected, dict):
            assert value["unit"] == expected["unit"] == "arcsec"
            assert isinstance(value["value"], float)
            assert value["value"] == pytest.approx(expected["value"], rel=1e-9)
        else:
            assert isinstance(value, float)
            assert value == pytest.approx(expected, rel=1e-3)


def test_identical_dish_sensitivity_obeys_radiometer_scaling():
    params = dict(freq="8GHz", bandwidth="20MHz", etime="60s", elevation="45deg",
                  telescope="VLA", diam=25.0, nant=27, method="tsys-manual", tau0=0.065, t_sky=20.0)
    value = simutil().sensitivity(**params)
    longer = simutil().sensitivity(**dict(params, etime="240s", bandwidth="40MHz"))
    assert longer / value == pytest.approx(1.0 / math.sqrt(8.0), rel=1e-6)
    with pytest.raises(RuntimeError, match="at least two antennas"):
        simutil().sensitivity(**dict(params, nant=1))
    with pytest.raises(ValueError):
        simutil().sensitivity(**dict(params, freq="3m"))


def test_optional_noise_image_returns_casa_rms():
    value = simutil().sensitivity(freq="8GHz", bandwidth="20MHz", etime="60s", elevation="45deg",
        integration="1s", antennalist=str(CONFIGS / "vla.d.cfg"), method="tsys-manual",
        tau0=0.065, t_sky=20.0, doimnoise=True)
    assert isinstance(value, tuple) and len(value) == 2
    assert all(isinstance(item, float) for item in value)
    assert value[0] == pytest.approx(0.00041926041093113013, rel=1e-3)
    # Independent CASA tclean on this deterministic simulated MS, not a
    # golden generated from the native image (image NRMS 1.91e-7).
    assert value[1] == pytest.approx(0.00035715280424348145, rel=1e-3)


@pytest.mark.parametrize("configured", [False, True])
@pytest.mark.parametrize("change, message", [
    ({"tau0": -100.0}, "Tsys"),
    ({"tau0": 100.0}, "Tsys"),
    ({"tau0": float("nan")}, "opacity must be finite"),
    ({"elevation": {"value": float("nan"), "unit": "deg"}}, "elevation must be finite"),
    ({"elevation": {"value": float("inf"), "unit": "deg"}}, "elevation must be finite"),
])
def test_invalid_noise_model_never_becomes_unit_gain_sensitivity(configured, change, message):
    params = dict(freq="8GHz", bandwidth="20MHz", etime="60s", elevation="45deg",
                  method="tsys-manual", tau0=0.065, t_sky=20.0)
    if configured:
        params["antennalist"] = str(CONFIGS / "vla.d.cfg")
    else:
        params.update(telescope="VLA", diam=25.0, nant=27)
    params.update(change)
    with pytest.raises(RuntimeError, match=message):
        simutil().sensitivity(**params)
