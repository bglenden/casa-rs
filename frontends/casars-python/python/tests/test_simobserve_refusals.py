"""CASA 6.7.6.14's simobserve type refusals (#1099)."""

import json
from pathlib import Path

import pytest

from casars import tasks


CASES = json.loads((Path(__file__).parent / "data" / "casa_simobserve_refusals.json").read_text())


@pytest.mark.parametrize("case", [case for case in CASES if case["type"] == "AssertionError"])
def test_integer_type_refusal_matches_casa(case: dict) -> None:
    # casa-rs raises TypeError for invalid parameter types by the #931 decision.
    with pytest.raises(TypeError) as error:
        tasks.simobserve(**case["args"])
    assert str(error.value) == case["message"]


@pytest.mark.parametrize("values, diagnostic", [
    ({"obsmode": "sd"}, "obsmode='int'"),
    ({"thermalnoise": "tsys-atm"}, "thermalnoise='' and leakage=0"),
    ({"leakage": 0.01}, "thermalnoise='' and leakage=0"),
    ({"graphics": "file"}, "graphics='none'"),
])
def test_msv4_incompatible_options_refuse_before_the_project(tmp_path, monkeypatch, values, diagnostic):
    from casars.tasks import _runner

    def renderer_unavailable():
        raise AssertionError("the MSv4 task must refuse before importing a renderer")

    monkeypatch.setattr(_runner, "_require_plotting", renderer_unavailable)
    project = tmp_path / "sim"
    parameters = dict(thermalnoise="", graphics="none", project=str(project), outformat="msv4")
    parameters.update(values)
    with pytest.raises(tasks.TaskParameterError, match=diagnostic) as error:
        tasks.simobserve(**parameters)
    assert error.value.completion.parameter == "outformat"
    assert not project.exists()
