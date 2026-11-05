"""CASA physicality checks and the approved empty-list crash correction."""
import json
from pathlib import Path

import pytest

from casars.tools import componentlist


def test_isphysical_matches_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_componentlist_isphysical.json").read_text())
    cl = componentlist()
    assert cl.isphysical([]) is fixture["empty_vector"]
    with pytest.raises(RuntimeError, match="empty list"):
        cl.isphysical()
    with pytest.raises(RuntimeError, match="empty list"):
        cl.isphysical([-1])
    for flux in fixture["fluxes"]:
        assert cl.addcomponent(dir="J2000 0deg 0deg", flux=flux)
    for case in fixture["cases"]:
        if "error" in case:
            expected = case["error"]
            error_type = {"RuntimeError": RuntimeError, "TypeError": TypeError}[expected["type"]]
            with pytest.raises(error_type) as error:
                cl.isphysical(case["which"])
            assert expected["message"] in str(error.value)
        else:
            assert cl.isphysical(case["which"]) is case["result"]
    assert cl.isphysical() is False
    cl.done()
