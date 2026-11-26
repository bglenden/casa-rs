"""CASA component removal and recovery, including retained component values."""
import json
from pathlib import Path

import pytest
from casars.tools import componentlist


FIXTURE = json.loads((Path(__file__).parent / "data" / "casa_componentlist_removal.json").read_text())


def make_list():
    cl = componentlist()
    for i in range(4):
        if i == 1:
            assert cl.addcomponent(label=str(i), flux=i + 1, shape="Gaussian", majoraxis="2arcmin", minoraxis="45arcsec", positionangle="15deg")
        elif i == 3:
            assert cl.addcomponent(label=str(i), flux=i + 1, shape="Gaussian", majoraxis="0.05deg", minoraxis="1arcmin", positionangle="0.5rad")
        else:
            assert cl.addcomponent(label=str(i), flux=i + 1)
    return cl


def check_snapshot(cl, expected):
    assert [cl.getlabel(i) for i in range(cl.length())] == expected["labels"]
    assert list(cl.selected()) == expected["selected"]
    assert [float(cl.getfluxvalue(i)[0].real) for i in range(cl.length())] == pytest.approx(expected["fluxes"])
    for index in range(cl.length()):
        shape = cl.getshape(index)
        units = expected["shape_units"][index]
        if units is None:
            assert shape == {}
        else:
            keys = ("majoraxis", "minoraxis", "positionangle")
            assert [shape[key]["unit"] for key in keys] == units
            assert [shape[key]["value"] for key in keys] == pytest.approx(expected["shape_values"][index])


@pytest.mark.parametrize("scenario", [s for s in FIXTURE["scenarios"] if s["name"] != "duplicate_corrupts"], ids=lambda s: s["name"])
def test_removal_matches_casa(scenario):
    cl = make_list()
    if "select" in scenario:
        assert cl.select(scenario["select"])
    check_snapshot(cl, scenario["before"])
    for (method, args), step in zip(scenario["actions"], scenario["steps"], strict=True):
        if "error" in step:
            expected = step["error"]
            with pytest.raises({"TypeError": TypeError, "SystemError": SystemError, "RuntimeError": RuntimeError}[expected["type"]]) as error:
                getattr(cl, method)(*args)
            assert str(error.value) == expected["message"]
        else:
            assert getattr(cl, method)(*args) is step["result"]
        check_snapshot(cl, step["after"])
    cl.done()


def test_duplicate_indices_cannot_discard_a_different_component():
    casa = next(s for s in FIXTURE["scenarios"] if s["name"] == "duplicate_corrupts")
    assert casa["steps"][-1]["after"]["labels"] == ["0", "3", "1", "1"]
    cl = make_list()
    assert cl.select([0, 1, 3])
    assert cl.remove([1, 1])
    assert [cl.getlabel(i) for i in range(cl.length())] == ["0", "2", "3"]
    assert list(cl.selected()) == [0, 2]
    assert cl.recover() is False
    assert [cl.getlabel(i) for i in range(cl.length())] == ["0", "2", "3", "1"]
    assert [float(cl.getfluxvalue(i)[0].real) for i in range(cl.length())] == [1, 3, 4, 2]


def test_many_removals_preserve_order_and_selection():
    cl = componentlist()
    for i in range(60):
        assert cl.addcomponent(label=str(i), flux=i + 1)
    assert cl.select([1, 2, 30, 59])
    removed = list(range(58, -1, -2))
    assert cl.remove(removed)
    assert [int(cl.getlabel(i)) for i in range(cl.length())] == list(range(1, 60, 2))
    assert list(cl.selected()) == [0, 29]
    assert cl.recover() is False
    assert [int(cl.getlabel(i)) for i in range(cl.length())] == list(range(1, 60, 2)) + list(reversed(removed))


@pytest.mark.parametrize("args", [(), ([-1],)])
def test_empty_remove_is_catchable_instead_of_crashing(args):
    assert FIXTURE["empty_crashes"] == {"c.remove()": -11, "c.remove([-1])": -11}
    cl = componentlist()
    with pytest.raises(RuntimeError, match="empty list would crash CASA"):
        cl.remove(*args)
    assert cl.length() == 0
    assert cl.remove([]) is True


def test_removed_components_and_bin_across_table_close(tmp_path):
    path = str(tmp_path / "components")
    cl = make_list()
    assert cl.rename(path)
    assert cl.remove([1, 3])
    assert cl.close()
    assert cl.open(path)
    assert [cl.getlabel(i) for i in range(cl.length())] == ["0", "2"]
    assert cl.recover() is False
    assert [cl.getlabel(i) for i in range(cl.length())] == ["0", "2"]
    assert cl.done()
