"""`casars.simutil`'s small helpers against CASA 6.7.6.14 (#1053).

`data/casa_simutil_helpers.json` is what CASA's `simutil` returned, logged or
raised (`tools/simulation/simutil_helpers_oracle.py`) for `is4d` and
`spectral` on the sky-model inputs, `dir_s2m`, `msg`, the report helpers and
`setcfg` against a stand-in simulator that records the call.
"""

from __future__ import annotations

import json
import logging
import tarfile
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from casars import simutil

from _casa_tools import close

HELPERS = json.loads((Path(__file__).resolve().parent / "data" / "casa_simutil_helpers.json").read_text(encoding="utf-8"))
INPUTS = Path(__file__).resolve().parents[3] / "casa-simulation" / "tests" / "data" / "skymodel_inputs.tar.gz"
MESSAGE = "a message with WARNING and ERROR in it"


@pytest.fixture(scope="module")
def images(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("helpers")
    with tarfile.open(INPUTS) as archive:
        archive.extractall(root, filter="data")
    return root / "inputs"


def test_the_record_is_casas() -> None:
    assert HELPERS["casa_version"] == "6.7.6-14"


@pytest.mark.parametrize("name", sorted(HELPERS["is4d"]))
def test_is4d_and_spectral_read_images_as_casa_does(images: Path, name: str) -> None:
    u = simutil.simutil()
    assert u.is4d(str(images / name)) is HELPERS["is4d"][name]["value"]
    casa = HELPERS["spectral"][name]["value"]
    found = u.spectral(str(images / name))
    if casa == 0.0:
        assert type(found) is float and found == 0.0
        return
    (channels, width) = casa["tuple"]
    assert isinstance(found, tuple) and len(found) == 2
    assert isinstance(found[0], np.ndarray) and found[0].dtype == np.int64
    assert found[0].tolist() == channels["ndarray"]
    assert found[1] == width


@pytest.mark.parametrize("case", HELPERS["dir_s2m"], ids=lambda case: f"{case['direction']}-{case['verbose']}")
def test_dir_s2m_makes_casas_measure(case: dict[str, Any], caplog: pytest.LogCaptureFixture) -> None:
    if case["direction"] is None:
        u = simutil.simutil(direction=case["self_direction"])
        call = u.dir_s2m
    else:
        u = simutil.simutil(verbose=case["verbose"])
        call = lambda: u.dir_s2m(case["direction"])  # noqa: E731
    with caplog.at_level(logging.INFO, logger="casars"):
        found = call()
    close(found, case["result"]["value"], rel=1e-14, abs_=1e-15)
    # CASA posts a verbose message twice, once in colour; casa-rs once.
    assumed = [line for line in case["result"]["log"] if line[1].startswith("simutil.s2m")]
    assert [(r.name, r.getMessage()) for r in caplog.records] == [
        ("casars.simutil.s2m", message) for _, _, message in assumed]


def expected_record(case: dict[str, Any]) -> tuple[str, str, str] | None:
    """The one log record casa-rs makes where CASA logs: CASA's level and
    message under the origin's logger; verbose, CASA's plain line at INFO."""
    logger = f"casars.{case['origin'] or 'simutil'}"
    if case["verbose"]:
        return (logger, "INFO", MESSAGE)
    lines = case["result"]["log"]
    if not lines:
        return None
    ((priority, _, message),) = lines
    return (logger, {"INFO": "INFO", "WARN": "WARNING"}[priority], message)


@pytest.mark.parametrize("case", HELPERS["msg"],
                         ids=lambda case: f"{case['priority']}-{case['origin']}-{case['verbose']}")
def test_msg_logs_and_raises_as_casa_does(case: dict[str, Any], caplog: pytest.LogCaptureFixture) -> None:
    u = simutil.simutil(verbose=case["verbose"])
    with caplog.at_level(logging.INFO, logger="casars"):
        if "raises" in case["result"]:
            with pytest.raises(RuntimeError, match=case["result"]["message"]):
                u.msg(MESSAGE, origin=case["origin"], priority=case["priority"])
        else:
            assert u.msg(MESSAGE, origin=case["origin"], priority=case["priority"]) is None
    found = [(r.name, r.levelname, r.getMessage()) for r in caplog.records]
    wanted = expected_record(case) if "raises" not in case["result"] else None
    assert found == ([] if wanted is None else [wanted])


def test_debug_messages_log_below_info(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.DEBUG, logger="casars"):
        simutil.simutil().msg("quiet", priority="debug2")
    assert [(r.levelname, r.getMessage()) for r in caplog.records] == [("DEBUG", "quiet")]


RAISED = {"Exception": RuntimeError, "ValueError": ValueError, "AttributeError": AttributeError}


def test_the_report_is_written_as_casa_writes_it(tmp_path: Path) -> None:
    record = HELPERS["report"]
    steps = dict(record["steps"])
    path = tmp_path / "project.report.txt"
    u = simutil.simutil(verbose=True)

    def step(name: str, call: Any) -> None:
        casa = steps[name]
        if "raises" in casa:
            with pytest.raises(RAISED[casa["raises"]], match=casa["message"]):
                call()
        else:
            assert call() == casa["value"], name

    step("isreport, no reportfile", u.isreport)
    step("openreport, no reportfile", u.openreport)
    u.reportfile = str(path)
    step("isreport, closed", u.isreport)
    step("openreport", u.openreport)
    step("isreport, open", u.isreport)
    step("msg to the report", lambda: u.msg("first line", origin="simanalyze"))
    step("msg without origin", lambda: u.msg("second line"))
    step("closereport", u.closereport)
    step("isreport, after closereport", u.isreport)
    step("msg after closereport", lambda: u.msg("third line"))
    step("openreport, file exists", u.openreport)
    step("msg, reopened", lambda: u.msg("fourth line", origin="simalma"))
    step("closereport again", u.closereport)
    quiet = simutil.simutil()
    quiet.reportfile = str(path)
    quiet.openreport()
    step("msg, not verbose", lambda: quiet.msg("not in the report"))
    quiet.closereport()
    step("closereport, never opened", simutil.simutil().closereport)
    assert path.read_text(encoding="utf-8") == record["report"]


class RecordingSimulator:
    def __init__(self, accept: bool):
        self.accept = accept
        self.calls: list[Any] = []

    def setconfig(self, **kwargs: Any) -> bool:
        self.calls.append(["setconfig", kwargs])
        return self.accept

    def close(self) -> bool:
        self.calls.append(["close"])
        return True


@pytest.mark.parametrize("case", HELPERS["setcfg"], ids=lambda case: f"{case['telescope']}-{case['mounttype']}")
def test_setcfg_configures_the_simulator_as_casa_does(case: dict[str, Any]) -> None:
    sm = RecordingSimulator(accept=not case.get("refused"))
    diam = [12.0, 12.0, 7.0] if case.get("diam_list") else np.array([12.0, 12.0, 7.0])
    posobs = {"m0": {"unit": "rad", "value": -1.1825465955049892},
              "m1": {"unit": "rad", "value": -0.4018251640113072},
              "m2": {"unit": "m", "value": 5056.8}, "refer": "WGS84", "type": "position"}
    args = (sm, case["telescope"], [1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0], diam,
            ["A001", "A002", "A003"], posobs, case["mounttype"])
    casa = case["result"]
    if "raises" in casa:
        with pytest.raises(RAISED[casa["raises"]], match=casa["message"]):
            simutil.simutil().setcfg(*args)
    else:
        assert simutil.simutil().setcfg(*args) == casa["value"]
    assert len(sm.calls) == len(case["calls"])
    for found, wanted in zip(sm.calls, case["calls"]):
        assert found[0] == wanted[0]
        if found[0] == "setconfig":
            kwargs = dict(found[1])
            close(kwargs.pop("referencelocation"), wanted[1]["referencelocation"], rel=1e-15)
            wanted = {key: value for key, value in wanted[1].items() if key != "referencelocation"}
            assert kwargs == wanted
