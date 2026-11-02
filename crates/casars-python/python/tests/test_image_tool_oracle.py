"""Unit tests for the image/coordsys CASA-oracle recorder (#1027)."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import numpy as np
import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
ORACLE = REPO_ROOT / "tools/casa-compat/image_tool_oracle.py"


@pytest.fixture
def oracle(monkeypatch: pytest.MonkeyPatch):
    """Load the recorder with a small CASA-tool stand-in."""

    class Image:
        def open(self, infile: str) -> None:
            raise RuntimeError(f"File {infile} does not exist")

        def done(self) -> None:
            pass

    class CoordinateSystem:
        def done(self) -> None:
            pass

    fake_casatools = types.ModuleType("casatools")
    fake_casatools.image = Image
    fake_casatools.coordsys = CoordinateSystem
    monkeypatch.setitem(sys.modules, "casatools", fake_casatools)
    spec = importlib.util.spec_from_file_location("image_tool_oracle_test", ORACLE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_encoding_round_trips_string_arrays_and_tuples(oracle) -> None:
    """The replay encoding keeps NumPy's reconstructible string dtype."""

    value = ("label", np.array([["abc", "d"]], dtype="<U3"))
    encoded = oracle.encode(value)

    assert encoded["tuple"][1]["dtype"] == value[1].dtype.name
    assert encoded["tuple"][1]["dtype_descriptor"] == value[1].dtype.str

    decoded = oracle.decode(encoded, "/tmp/unused")
    assert decoded[0] == value[0]
    assert decoded[1].dtype == value[1].dtype
    np.testing.assert_array_equal(decoded[1], value[1])


def test_recorded_errors_replace_the_temporary_directory(oracle) -> None:
    """A negative oracle fixture can be recorded reproducibly."""

    outcome = oracle.run_script([oracle.ia("open", infile="{tmp}/missing.im")])[0]

    assert outcome["raises"] == ["RuntimeError", "File {tmp}/missing.im does not exist"]
