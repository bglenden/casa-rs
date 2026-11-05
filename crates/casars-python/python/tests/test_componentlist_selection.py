"""CASA's component selection and integer-vector conversion."""
import json
from pathlib import Path

import numpy as np
import pytest

from casars.tools import componentlist


def test_selection_matches_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_componentlist_selection.json").read_text())
    for case in fixture["cases"]:
        cl = componentlist()
        for _ in range(3):
            assert cl.addcomponent(flux=1, dir="J2000 0deg 0deg")
        assert cl.select([1])
        argument = case["input"]
        if isinstance(argument, dict):
            if "scalar_numpy" in argument:
                argument = np.dtype(argument["dtype"]).type(argument["scalar_numpy"])
            else:
                argument = np.array(argument["numpy"], dtype=argument["dtype"])
        method = getattr(cl, case["method"])
        if "error" in case:
            expected = case["error"]
            error_type = {"RuntimeError": RuntimeError, "TypeError": TypeError, "SystemError": SystemError}[expected["type"]]
            with pytest.raises(error_type) as error:
                method(argument)
            # CASA inserts timestamps and logger prefixes in out-of-range errors.
            if expected["type"] == "RuntimeError":
                assert expected["message"] in str(error.value), case
            else:
                assert str(error.value) == expected["message"], case
        else:
            assert method(argument) is case["result"], case
        actual = cl.selected()
        assert actual.dtype == np.dtype(case["dtype"]), case
        assert actual.tolist() == case["selected"], case
        cl.done()


def test_selection_lifecycle_and_casa_crash_refusal(tmp_path: Path) -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_componentlist_selection.json").read_text())["lifecycle"]
    cl = componentlist()
    assert cl.addcomponent(flux=1, dir="J2000 0deg 0deg")
    assert cl.select([-1])
    assert cl.addcomponent(flux=1, dir="J2000 0deg 0deg")
    assert cl.selected().tolist() == fixture["after_add"]
    path = tmp_path / "selection.cl"
    assert cl.rename(str(path))
    assert cl.selected().tolist() == fixture["after_rename"]
    assert cl.done()
    assert cl.open(str(path))
    assert cl.selected().tolist() == fixture["after_reopen"]
    with pytest.raises(SystemError, match="componentlist_select"):
        cl.select(np.int64(2))
    assert cl.selected().tolist() == []
    assert cl.select([float("nan")])
    assert cl.selected().tolist() == [0]
    assert cl.deselect([-1])
    assert cl.select([float("inf")])
    assert cl.selected().tolist() == [0, 1]
    assert cl.deselect([-1])
    for value in (float("nan"), float("inf")):
        with pytest.raises(ValueError, match="CASA crash: #1176"):
            cl.select(value)
    assert cl.selected().tolist() == []
    cl.done()
