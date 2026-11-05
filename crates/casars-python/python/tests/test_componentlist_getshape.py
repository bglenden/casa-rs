"""CASA shape records before and after a component-list table round trip."""
import json
from pathlib import Path

import pytest

from casars.tools import componentlist


def test_getshape_matches_casa(tmp_path: Path) -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_componentlist_getshape.json").read_text())
    cl = componentlist()
    current = "empty"
    filename = str(tmp_path / "components")
    for case in fixture["cases"]:
        stage = case["stage"]
        if stage != current:
            if stage == "added":
                for component in fixture["components"]:
                    assert cl.addcomponent(dir="J2000 0deg 0deg", flux=1.0, **component)
            elif stage == "renamed":
                assert cl.rename(filename)
            elif stage == "reopened":
                assert cl.close()
                assert cl.open(filename)
            current = stage
        if "error" in case:
            error_type = {"TypeError": TypeError, "RuntimeError": RuntimeError}[case["error"]["type"]]
            with pytest.raises(error_type) as error:
                cl.getshape(case["which"])
            assert str(error.value).endswith(case["error"]["message"]), case
        else:
            actual = cl.getshape(case["which"])
            expected = case["result"]
            assert actual.keys() == expected.keys(), case
            for key, wanted in expected.items():
                assert actual[key]["unit"] == wanted["unit"], case
                assert actual[key]["value"] == pytest.approx(wanted["value"], rel=1e-14, abs=1e-15), case
    cl.done()
