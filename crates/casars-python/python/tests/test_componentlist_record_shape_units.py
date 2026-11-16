"""CASA shape record units before and after saving a component list."""
import json
from pathlib import Path

import pytest

from casars.tools import componentlist


def test_shape_records_match_casa(tmp_path: Path) -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_componentlist_record_shape_units.json").read_text())
    cl = componentlist()
    for component in fixture["components"]:
        assert cl.addcomponent(dir="J2000 0deg 0deg", flux=1.0, **component)
    filename = str(tmp_path / "components")
    for case in fixture["cases"]:
        if case["stage"] == "renamed":
            assert cl.rename(filename)
        elif case["stage"] == "reopened":
            assert cl.close()
            assert cl.open(filename)
        record = cl.torecord()
        for index, expected in enumerate(case["shapes"]):
            shape = record[f"component{index}"]["shape"]
            assert "direction" in shape
            assert shape["type"] == expected["type"]
            for name, quantity in expected.items():
                if name == "type":
                    continue
                assert shape[name]["unit"] == quantity["unit"], (case["stage"], index, name)
                assert shape[name]["value"] == pytest.approx(quantity["value"], rel=1e-14, abs=1e-15)
                getter = cl.getshape(index)[name]
                assert shape[name]["unit"] == getter["unit"]
                assert shape[name]["value"] == pytest.approx(getter["value"], rel=1e-14, abs=1e-15)
    cl.done()
