"""`casars.simutil`: CASA's `simutil.readantenna` (#841).

`simutil` is CASA's Python helper class (`casatasks.private.simutil`), not a
tool: casa-rs declares only `readantenna`, and the class is not in the
object-surface bundle. The signatures are compared with CASA 6.7.6's
(`data/casa_simutil_signatures.json`); the expected values are CASA 6.7.6's
for the same files (probed 2026-10-21).
"""

from __future__ import annotations

import inspect
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from casars import simutil
from casars.data import schema_bundle

from _casa_tools import casa_signatures, close, describe

CASA = casa_signatures("simutil")


@pytest.fixture(scope="module")
def measures_data(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Skip when no measures runtime is installed: `readantenna` loads one."""
    path = tmp_path_factory.mktemp("probe") / "probe.cfg"
    path.write_text("# coordsys=XYZ\n1 2 3 4\n", encoding="utf-8")
    try:
        simutil.simutil().readantenna(str(path))
    except RuntimeError as error:
        if "measures data" not in str(error):
            raise
        pytest.skip(f"no measures runtime: {error}")


needs_measures_data = pytest.mark.usefixtures("measures_data")


def write(tmp_path: Path, name: str, text: str) -> str:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return str(path)


def position(longitude: float, latitude: float, height: float) -> dict[str, Any]:
    return {
        "m0": {"unit": "rad", "value": longitude}, "m1": {"unit": "rad", "value": latitude},
        "m2": {"unit": "m", "value": height}, "refer": "WGS84", "type": "position",
    }


def test_the_class_has_casas_signatures() -> None:
    assert simutil.simutil.__module__ == "casars.simutil"
    assert describe(simutil.simutil) == CASA["constructor"]
    assert describe(simutil.simutil.readantenna) == CASA["methods"]["readantenna"]
    assert set(CASA["methods"]) == {"readantenna"}


def test_simutil_is_not_a_bundled_tool() -> None:
    names = {entry["name"] for entry in schema_bundle()["semantic"]["objects"]}
    assert "simutil" not in names


def test_signatures_match_a_live_casa_when_one_is_importable() -> None:
    live = pytest.importorskip("casatasks.private.simutil").simutil
    assert describe(simutil.simutil) == describe(live.__init__)
    assert describe(simutil.simutil.readantenna) == describe(live.readantenna)


@needs_measures_data
def test_an_xyz_file_reads_as_casa_reads_it(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    path = write(tmp_path, "xyz.cfg", (
        "# observatory=VLA\n# coordsys=XYZ\n"
        "-1601185.4 -5041977.5 3554875.9 25 W01\n"
        "-1601155.4 -5041987.5 3554885.9 25 W02 A2\n"
    ))
    with caplog.at_level(logging.INFO, logger="casars.simutil"):
        found = simutil.simutil().readantenna(path)
    assert caplog.messages == [f"Reading antenna positions from '{path}'"]
    assert isinstance(found, tuple) and len(found) == 8
    x, y, z, d, pads, names, telescope, posobs = found
    assert (x, y, z) == ([-1601185.4, -1601155.4], [-5041977.5, -5041987.5], [3554875.9, 3554885.9])
    assert all(type(value) is float for value in x + y + z)
    assert isinstance(d, np.ndarray) and d.dtype == np.float64 and d.tolist() == [25.0, 25.0]
    assert (pads, names, telescope) == (["W01", "W02"], ["W01", "A2"], "VLA")
    # CASA's me.measure(me.observatory('VLA'), 'WGS84').
    close(posobs, position(-1.8782942581394362, 0.5947874979128073, 2114.890229039825), rel=2e-12)


@needs_measures_data
def test_local_coordinates_are_placed_at_the_observatory(tmp_path: Path) -> None:
    path = write(tmp_path, "loc.cfg", (
        "# observatory=ALMA\n# coordsys=LOC (local tangent plane)\n"
        "-5.850273514 -125.9985379 -1.590364043 12. A058 DA41\n"
        "-19.90369337 52.82680653 -1.892119601 12. A023 DA42\n"
    ))
    x, y, z, d, pads, names, telescope, posobs = simutil.simutil().readantenna(path)
    np.testing.assert_allclose(x, [2225117.556232891, 2225130.920129482], rtol=0, atol=1e-6)
    np.testing.assert_allclose(y, [-5440262.620061224, -5440332.416275699], rtol=0, atol=1e-6)
    np.testing.assert_allclose(z, [-2481145.1924431063, -2480980.4927521856], rtol=0, atol=1e-6)
    assert (d.tolist(), pads, names, telescope) == ([12.0, 12.0], ["A058", "A023"], ["DA41", "DA42"], "ALMA")
    assert posobs == position(-1.1825465955049892, -0.4018251640113072, 5056.8)


@needs_measures_data
def test_cofa_places_an_unknown_observatory(tmp_path: Path) -> None:
    path = write(tmp_path, "cofa.cfg", "# observatory=MYTEL\n# COFA=-67.75,-23.02\n# coordsys=LOC\n10 20 0 7 P1 N1\n")
    x, y, z, _, _, _, telescope, posobs = simutil.simutil().readantenna(path)
    np.testing.assert_allclose([x[0], y[0], z[0]], [2223907.4933591345, -5435927.580256435,
                                                     -2478739.5821347213], rtol=0, atol=1e-6)
    assert telescope == "MYTEL"
    # casacore keeps a zero height as a 1e-6 m vector.
    assert posobs == position(-1.1824605682261582, -0.4017747938090946, 1.0000000000000002e-06)


@needs_measures_data
@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("# observatory=VLA\n1 2 3 4\n", "Must specify XYZ, UTM or LOC coordinate system in antenna file header"),
        ("# observatory=NOWHERE\n# coordsys=LOC\n1 2 3 4\n",
         "To use local coordinates in the antenna position file, you must either use a known observatory "
         "name, or provide the COFA explicitly"),
    ],
)
def test_casa_errors_raise_with_casas_message(tmp_path: Path, text: str, message: str) -> None:
    with pytest.raises(RuntimeError) as raised:
        simutil.simutil().readantenna(write(tmp_path, "bad.cfg", text))
    assert str(raised.value) == message


@needs_measures_data
def test_missing_files_raise_os_errors(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        simutil.simutil().readantenna(str(tmp_path / "missing.cfg"))
    with pytest.raises(TypeError):
        simutil.simutil().readantenna()


def test_the_constructor_takes_casas_arguments() -> None:
    simutil.simutil("J2000 10h00m00 -30d00m00", "100GHz", "2GHz", "1h", True)
    assert inspect.signature(simutil.simutil).parameters["verbose"].default is False
