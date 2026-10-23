"""`casars.tools.componentlist`: CASA's `cl` tool (ADR-0019, #841 Phase 2).

Signatures are compared with CASA 6.7.6's installed binding
(`data/casa_componentlist_signatures.json`, and a live CASA when importable)
and with the object-surface bundle. Behaviour is compared with what CASA
6.7.6 returned for a fixed script (`data/casa_componentlist_tool.json`,
written by `tools/simulation/componentlist_oracle.py tool`): each
`addcomponent` call's outcome and the list's length after it, then the
records and accessors of every component.

Records are compared as quantities: CASA gives a shape it built in memory in
the units it was given, and casa-rs gives every shape in arcmin and degrees,
as CASA gives one read from a table. CASA's `setflux` holds flux values in
single precision, so they are compared to 1e-7. One call diverges by
design: CASA stores `polarization='circular'` values as Stokes, and casa-rs
keeps them circular.

When to write the table follows CASA as probed (2026-10-23): `rename`
writes at once; later changes reach the table at `close`, `done`, a
replacing `open` or the tool's deletion; `open(nomodify=False)` then `close`
rewrites; a list opened with `nomodify=True` is never written.
"""

from __future__ import annotations

import gc
import json
import logging
import math
import os
from pathlib import Path
from typing import Any

import numpy as np
import pytest

import casars
from casars.tools import componentlist

from _casa_tools import (
    DATA,
    assert_bundle_describes,
    assert_casa_signatures,
    assert_live_signatures,
    casa_signatures,
)

LOGGER = "casars.tools.componentlist"
CASA = casa_signatures("componentlist")
TOOL = json.loads((DATA / "casa_componentlist_tool.json").read_text(encoding="utf-8"))

IMPLEMENTED = {
    "addcomponent", "close", "done", "getcomponent", "getfluxunit", "getfluxvalue", "getlabel",
    "getrefdir", "getspectrum", "length", "open", "rename", "shapetype", "spectrumtype",
    "torecord",
}
NOT_YET_IMPLEMENTED = set(CASA["methods"]) - IMPLEMENTED

#: The script call whose flux CASA stores as Stokes and casa-rs keeps circular.
CIRCULAR_CALL = 3

#: Radians, hertz or janskys in each unit the records use.
SI = {
    "rad": 1.0, "deg": math.pi / 180.0, "arcmin": math.pi / 10800.0,
    "arcsec": math.pi / 648000.0, "mas": math.pi / 648000000.0,
    "Hz": 1.0, "GHz": 1.0e9, "Jy": 1.0, "mJy": 1.0e-3,
}


def test_the_class_is_casas_componentlist() -> None:
    assert casars.tools.componentlist is componentlist
    assert componentlist.__module__ == "casars.tools"


def test_every_casa_method_exists_with_casas_signature() -> None:
    assert_casa_signatures(componentlist, CASA)
    assert len(CASA["methods"]) == 61


def test_signatures_match_a_live_casa_when_one_is_importable() -> None:
    casatools = pytest.importorskip("casatools")
    assert_live_signatures(componentlist, casatools.componentlist)


def test_the_bundle_describes_the_class() -> None:
    assert_bundle_describes(
        componentlist,
        "componentlist",
        {"implemented": IMPLEMENTED, "not_yet_implemented": NOT_YET_IMPLEMENTED},
    )


def test_methods_casa_rs_does_not_implement_yet_raise() -> None:
    cl = componentlist()
    with pytest.raises(NotImplementedError, match="componentlist.setflux is not yet implemented"):
        cl.setflux(0, value=1.0)
    with pytest.raises(NotImplementedError, match="componentlist.fromrecord"):
        cl.fromrecord({})


def replay() -> componentlist:
    """CASA's script, replayed with casars, each call checked against CASA."""
    cl = componentlist()
    for number, call in enumerate(TOOL["calls"]):
        if "raised" in call:
            with pytest.raises(RuntimeError) as raised:
                cl.addcomponent(**call["kwargs"])
            # CASA's first line, which carries its log prefix or its source
            # location.
            first = str(raised.value).split("\n", 1)[0].rstrip(".")
            assert first in call["raised"], (number, str(raised.value))
        else:
            assert cl.addcomponent(**call["kwargs"]) == call["returned"], number
        assert cl.length() == call["length"], number
    return cl


def test_addcomponent_follows_casa_call_by_call(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        replay()
    # `setshape`'s refusals are CASA's SEVERE messages, logged and survived.
    errors = [record.getMessage() for record in caplog.records if record.levelno == logging.ERROR]
    assert errors[0].startswith("Could not translate the shape type to a known value.")
    assert any(error.startswith("Limbdarkeneddisk is not implemented yet.") for error in errors)
    assert len(errors) == 4


def si(value: Any) -> Any:
    """`value` with every quantity record in SI units, and arrays as lists."""
    if isinstance(value, dict):
        if set(value) == {"unit", "value"} and isinstance(value["unit"], str):
            return {"si": float(value["value"]) * SI[value["unit"]]}
        return {key: si(item) for key, item in value.items()}
    if isinstance(value, np.ndarray):
        return [si(item) for item in value.tolist()]
    if isinstance(value, list):
        return [si(item) for item in value]
    if isinstance(value, complex):
        return [value.real, value.imag]
    return value


def assert_same(found: Any, expected: Any, path: str, flux: bool = False) -> None:
    """`found` equals CASA's `expected` as quantities: numbers to 1e-12
    relative, and flux values to 1e-7 (CASA's single precision)."""
    if isinstance(expected, dict):
        assert isinstance(found, dict) and set(found) == set(expected), (path, found, expected)
        for key in expected:
            assert_same(found[key], expected[key], f"{path}.{key}", flux or key == "flux")
    elif isinstance(expected, list):
        assert isinstance(found, list) and len(found) == len(expected), (path, found, expected)
        for index, (item, wanted) in enumerate(zip(found, expected)):
            assert_same(item, wanted, f"{path}[{index}]", flux)
    elif isinstance(expected, float) and not isinstance(expected, bool):
        tolerance = 1e-7 if flux else 1e-12
        assert math.isclose(found, expected, rel_tol=tolerance, abs_tol=1e-300), (path, found, expected)
    else:
        assert found == expected, (path, found, expected)


def test_records_and_accessors_are_casas() -> None:
    cl = replay()
    assert len(TOOL["components"]) == cl.length()
    for index, casa in enumerate(TOOL["components"]):
        for accessor, outcome in casa.items():
            found = si(getattr(cl, accessor)(index))
            expected = si(outcome["returned"])
            if index == CIRCULAR_CALL and accessor == "getcomponent":
                # casa-rs keeps the circular values CASA stores as Stokes.
                assert found["flux"]["polarisation"] == "Circular"
                assert found["flux"]["value"] == [[1.0, 0.0], [0.1, 0.0], [0.2, 0.0], [0.9, 0.0]]
                found["flux"] = expected["flux"]
            elif index == CIRCULAR_CALL and accessor == "getfluxvalue":
                assert found == [1.0, 0.1, 0.2, 0.9]
                found = expected
            assert_same(found, expected, f"component {index} {accessor}",
                        flux=accessor == "getfluxvalue")
    record = si(cl.torecord())
    casa_record = si(TOOL["torecord"]["returned"])
    assert set(record) == set(casa_record)
    assert record["nelements"] == casa_record["nelements"] == cl.length()


def test_index_errors_are_casas() -> None:
    cl = replay()
    for edge, method, which in [
        ("getcomponent_out_of_range", "getcomponent", cl.length()),
        ("getfluxvalue_out_of_range", "getfluxvalue", -1),
        ("getrefdir_out_of_range", "getrefdir", cl.length()),
    ]:
        with pytest.raises(RuntimeError) as raised:
            getattr(cl, method)(which)
        assert str(raised.value) in TOOL["edges"][edge]["raised"]
    # The methods CASA does not check fail casacore's assertion, as CASA's do.
    for method in ("shapetype", "spectrumtype", "getspectrum"):
        with pytest.raises(RuntimeError, match="Failed AlwaysAssert index < nelements"):
            getattr(cl, method)(99)


def test_an_empty_list_has_no_record(caplog: pytest.LogCaptureFixture) -> None:
    cl = componentlist()
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert cl.torecord() == TOOL["edges"]["torecord_empty"]["returned"] == {}
        assert cl.length() == TOOL["edges"]["length_empty"]["returned"] == 0
        assert cl.close() is True
    assert caplog.messages == ["componentlist is empty", "componentlist closed"]


def add_point(cl: componentlist, flux: float) -> None:
    assert cl.addcomponent(flux=flux, dir="J2000 10h00m00.0 -30d00m00.0", freq="230GHz")


def fluxes(path: Path) -> list[float]:
    """The Stokes I fluxes of the list at `path`, read without changing it."""
    reader = componentlist()
    assert reader.open(str(path), nomodify=True)
    values = [float(reader.getfluxvalue(index)[0]) for index in range(reader.length())]
    reader.done()
    return values


def test_rename_writes_at_once_and_close_writes_the_rest(tmp_path: Path) -> None:
    path = tmp_path / "list.cl"
    cl = componentlist()
    add_point(cl, 1.0)
    assert cl.rename(str(path)) is True
    assert fluxes(path) == [1.0]
    add_point(cl, 2.0)
    assert fluxes(path) == [1.0]
    assert cl.close(log=False) is True
    assert fluxes(path) == [1.0, 2.0]
    assert cl.length() == 0
    with pytest.raises(RuntimeError, match="already exists"):
        add_point(cl, 3.0)
        cl.rename(str(path))


def test_an_update_rewrites_and_a_read_only_list_is_never_written(tmp_path: Path) -> None:
    path = tmp_path / "list.cl"
    cl = componentlist()
    add_point(cl, 1.0)
    cl.rename(str(path))
    cl.done()
    before = os.stat(path / "table.dat").st_mtime_ns
    # Opened read-only: changed in memory, never written.
    assert cl.open(str(path), nomodify=True) is True
    add_point(cl, 5.0)
    assert cl.length() == 2
    cl.close(log=False)
    assert fluxes(path) == [1.0]
    assert os.stat(path / "table.dat").st_mtime_ns == before
    with pytest.raises(RuntimeError, match="itsROFlag"):
        cl.open(str(path), nomodify=True)
        cl.rename(str(tmp_path / "other.cl"))
    cl.done()
    # Opened for update: rewritten at close, even unchanged, and with what
    # was added.
    assert cl.open(str(path)) is True
    cl.close(log=False)
    assert os.stat(path / "table.dat").st_mtime_ns != before
    cl.open(str(path))
    add_point(cl, 7.0)
    cl.done()
    assert fluxes(path) == [1.0, 7.0]


def test_an_open_releases_the_list_it_replaces(tmp_path: Path) -> None:
    first, second = tmp_path / "first.cl", tmp_path / "second.cl"
    for path, flux in ((first, 1.0), (second, 2.0)):
        cl = componentlist()
        add_point(cl, flux)
        cl.rename(str(path))
        cl.done()
    cl = componentlist()
    cl.open(str(first))
    add_point(cl, 3.0)
    cl.open(str(second))
    assert fluxes(first) == [1.0, 3.0]
    assert cl.length() == 1


def test_a_dropped_tool_writes_its_list(tmp_path: Path) -> None:
    path = tmp_path / "list.cl"
    cl = componentlist()
    add_point(cl, 1.0)
    cl.rename(str(path))
    add_point(cl, 2.0)
    del cl
    gc.collect()
    assert fluxes(path) == [1.0, 2.0]


def test_open_refuses_what_is_not_a_component_list(tmp_path: Path) -> None:
    cl = componentlist()
    missing = tmp_path / "missing.cl"
    with pytest.raises(RuntimeError, match=f"^{missing} is non existant or is not a componentlist table$"):
        cl.open(str(missing))
    with pytest.raises(RuntimeError, match="is non existant or is not a componentlist table"):
        cl.open("")


def test_casa_reads_what_casars_writes_when_casa_is_importable(tmp_path: Path) -> None:
    casatools = pytest.importorskip("casatools")
    path = tmp_path / "list.cl"
    cl = replay()
    cl.rename(str(path))
    records = [si(cl.getcomponent(index)) for index in range(cl.length())]
    cl.done()
    casa = casatools.componentlist()
    casa.open(str(path), nomodify=True)
    assert casa.length() == len(records)
    for index, record in enumerate(records):
        assert_same(si(casa.getcomponent(index)), record, f"component {index}")
    casa.done()
