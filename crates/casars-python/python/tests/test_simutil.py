"""`casars.simutil`: CASA's `simutil` helper class (#841, #1053).

`simutil` is CASA's Python helper class (`casatasks.private.simutil`), not a
tool, and is not in the object-surface bundle. casa-rs implements
`readantenna`, the direction, pointing, receiver and baseline helpers
`simobserve` uses, and its sky-model helpers `modifymodel` and `flatimage`
(`IMPLEMENTED`); CASA's other public methods refuse, naming #1053. Every
signature is compared with CASA 6.7.6.14's
(`data/casa_simutil_signatures.json`). The expected values are CASA
6.7.6's: `readantenna`'s for the same files (probed 2026-10-21), and the rest
from casa-simulation's CASA fixtures (`tools/simulation/simutil_oracle.py`
and `skymodel_oracle.py`), which the Rust tests compare in full; here a
sample checks the binding's arguments, results and state.
"""

from __future__ import annotations

import gzip
import inspect
import json
import logging
import shutil
import tarfile
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from casars import simutil
from casars.data import Image, schema_bundle

from _casa_tools import casa_signatures, close, describe

CASA = casa_signatures("simutil")

#: The methods casa-rs implements; CASA's others refuse, naming #1053.
IMPLEMENTED = {
    "applyRotate", "approxBeam", "average_direction", "bandname", "baselineLengths", "calc_pointings2", "cellsize",
    "closereport", "convimage", "dir_m2s", "dir_s2m", "direction_splitter", "flatimage", "getdatum",
    "getBaseTriangularTiling", "getShiftTriangularTiling", "getTrianglePoints", "getTriangularTiling",
    "is4d", "isEven", "isdirection", "ismstp", "isquantity", "isreport", "itrf2loc", "itrf2locname",
    "locxyz2itrf", "long2xyz", "median_direction", "modifymodel", "msg", "needsFiller", "noisetemp", "openreport",
    "polsettings", "read_pointings", "readantenna", "setcfg", "spectral", "statim", "tconpc",
    "newfig", "nextfig", "endfig", "plotants", "ephemeris",
    "tmgeod", "utm2long", "utm2xyz", "wrapang", "write_pointings", "xyz2long", "xyz2long_old",
}


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
    declared = {name for name in dir(simutil.simutil) if not name.startswith("_")
                and callable(getattr(simutil.simutil, name))}
    assert declared == set(CASA["methods"])
    for name, parameters in CASA["methods"].items():
        assert describe(getattr(simutil.simutil, name)) == parameters, name


def test_casas_whole_class_is_recorded() -> None:
    # CASA 6.7.6.14's `simutil` has 57 public methods.
    assert len(CASA["methods"]) == 57
    assert IMPLEMENTED < set(CASA["methods"])


def test_direct_tilings_match_casa() -> None:
    cases = json.loads((Path(__file__).resolve().parent / "data" / "casa_simutil_tiling.json").read_text())
    u = simutil.simutil()
    for case in cases:
        if "error" in case:
            assert case["error"]["type"] == "ZeroDivisionError"
            with pytest.raises(ZeroDivisionError, match=case["error"]["message"]):
                getattr(u, case["method"])(*case["args"])
            continue
        actual = getattr(u, case["method"])(*case["args"])
        assert isinstance(actual, tuple)
        assert all(isinstance(axis, list) for axis in actual)
        np.testing.assert_allclose(actual, case["result"], rtol=1e-13, atol=1e-13)


def test_arithmetic_helpers_match_casa() -> None:
    cases = json.loads((Path(__file__).parent / "data" / "casa_simutil_arithmetic.json").read_text())
    u = simutil.simutil()

    def value(spec: Any) -> Any:
        if isinstance(spec, dict) and "array" in spec:
            return np.array(spec["array"], dtype=spec["dtype"])
        if isinstance(spec, dict) and "scalar" in spec:
            return np.dtype(spec["dtype"]).type(spec["scalar"])
        return spec

    def describe_result(result: Any) -> dict[str, Any]:
        if isinstance(result, tuple):
            return {"type": "tuple", "items": [describe_result(item) for item in result]}
        if isinstance(result, np.ndarray):
            return {"type": "ndarray", "dtype": str(result.dtype), "value": result.tolist()}
        if isinstance(result, np.generic):
            return {"type": type(result).__name__, "value": result.item()}
        return {"type": type(result).__name__, "value": result}

    for case in cases:
        method = getattr(u, case["method"])
        arguments = [value(arg) for arg in case["args"]]
        if "error" in case:
            error_type = {"TypeError": TypeError, "ValueError": ValueError}[case["error"]["type"]]
            with pytest.raises(error_type) as error:
                method(*arguments)
            assert str(error.value) == case["error"]["message"]
        else:
            assert describe_result(method(*arguments)) == case["result"], case

@pytest.mark.parametrize("method", sorted(set(CASA["methods"]) - IMPLEMENTED))
def test_methods_casa_rs_does_not_implement_yet_refuse_naming_the_issue(method: str) -> None:
    arguments = [object()] * sum(parameter["default"] is None for parameter in CASA["methods"][method])
    with pytest.raises(NotImplementedError, match=rf"^simutil\.{method} is not yet implemented in casa-rs \(#1053\)$"):
        getattr(simutil.simutil(), method)(*arguments)


def test_simutil_is_not_a_bundled_tool() -> None:
    names = {entry["name"] for entry in schema_bundle()["semantic"]["objects"]}
    assert "simutil" not in names


def test_signatures_match_a_live_casa_when_one_is_importable() -> None:
    live = pytest.importorskip("casatasks.private.simutil").simutil
    assert describe(simutil.simutil) == describe(live.__init__)
    for name in CASA["methods"]:
        assert describe(getattr(simutil.simutil, name)) == describe(getattr(live, name)), name


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


# The CASA fixture casa-simulation's Rust tests compare in full.
FIXTURE = json.loads(
    (Path(__file__).resolve().parents[3] / "casa-simulation" / "tests" / "data" / "simutil_casa.json")
    .read_text(encoding="utf-8")
)
CONFIGS = Path(__file__).resolve().parents[3] / "casa-simulation" / "tests" / "data" / "readantenna"


def test_the_constructor_keeps_casas_state() -> None:
    u = simutil.simutil()
    assert u.direction == ""
    assert u.centerfreq == {"unit": "GHz", "value": 245.0}
    assert (u.bandwidth, u.totaltime, u.verbose) == ({"unit": "GHz", "value": 1.0}, {"unit": "h", "value": 0.0},
                                                    False)
    assert u.pointings is None and u.telescopename is None
    u.direction = "J2000 10h00m00 -30d00m00"
    assert u.direction == "J2000 10h00m00 -30d00m00"


def test_pointings_are_casas_and_kept() -> None:
    cases = [case for case in FIXTURE["calc_pointings2"] if "pointings" in case and isinstance(case["direction"], str)]
    assert len(cases) > 600
    for case in cases[::25] + [case for case in cases if any(isinstance(s, dict) for s in case["size"])]:
        u = simutil.simutil()
        found = u.calc_pointings2(case["spacing"], case["size"], maptype=case["maptype"],
                                  direction=case["direction"], relmargin=case["relmargin"], beam=case["beam"])
        assert found == case["pointings"], case
        assert u.pointings == found and u.direction == case["direction"]


def test_pointings_default_to_the_objects_direction() -> None:
    u = simutil.simutil("J2000 19h00m00 -23d00m00")
    case = next(c for c in FIXTURE["calc_pointings2"]
                if c["direction"] == "J2000 19h00m00 -23d00m00" and c["maptype"] == "hexagonal")
    assert u.calc_pointings2(case["spacing"], case["size"], "hexagonal", beam=case["beam"]) == case["pointings"]
    # One size string is used for both axes.
    assert u.calc_pointings2("10arcsec", "40arcsec") == u.calc_pointings2("10arcsec", ["40arcsec", "40arcsec"])


def test_a_list_of_directions_is_returned_as_given() -> None:
    directions = ["J2000 10h00m00 -30d00m00", "J2000 10h00m01 -30d00m00"]
    assert simutil.simutil().calc_pointings2("10arcsec", "30arcsec", direction=directions) == directions


def test_a_list_of_directions_is_checked_before_it_is_returned() -> None:
    # #913 review: CASA's `isdirection` refuses the list before the list is
    # returned as given.
    for directions in (["not-a-direction", "also-not-a-direction"],
                       ["J2000 10h00m00 -30d00m00", "also-not-a-direction"]):
        with pytest.raises(RuntimeError, match=r"can't interpret '\[.*\]' as a direction"):
            simutil.simutil().calc_pointings2("10arcsec", ["30arcsec", "30arcsec"], direction=directions)


def test_lists_of_one_direction_are_centred_at_their_rounded_mean() -> None:
    # #913 review: CASA splits a list, even of one direction, at its mean,
    # which it writes at precision 5 and reads back.
    cases = [case for case in FIXTURE["calc_pointings2"]
             if isinstance(case["direction"], list) and len(case["direction"]) == 1]
    assert len(cases) >= 9
    for case in cases:
        u = simutil.simutil()
        found = u.calc_pointings2(case["spacing"], case["size"], maptype=case["maptype"],
                                  direction=case["direction"], relmargin=case["relmargin"], beam=case["beam"])
        assert found == case["pointings"], case
        assert u.direction == case["direction"]
    d = "J2000 18h00m00.123456789s -23d00m00.123456789s"
    listed = simutil.simutil().calc_pointings2("10arcsec", ["30arcsec", "30arcsec"], maptype="square", direction=[d])
    one = simutil.simutil().calc_pointings2("10arcsec", ["30arcsec", "30arcsec"], maptype="square", direction=d)
    assert listed[2].startswith("J2000 18:00:00.84769") and one[2].startswith("J2000 18:00:00.84768")


def test_spacings_casa_cannot_count_are_refused() -> None:
    # #913 review: where CASA's integer conversions raise, casa-rs raises
    # Python's message rather than panicking.
    cases = [case for case in FIXTURE["calc_pointings2"]
             if "refused" in case and isinstance(case["direction"], str)]
    assert len(cases) >= 5
    for case in cases:
        message = case["refused"].split(": ", 1)[1]
        with pytest.raises(RuntimeError, match=message):
            simutil.simutil().calc_pointings2(case["spacing"], case["size"], maptype=case["maptype"],
                                              direction=case["direction"], relmargin=case["relmargin"],
                                              beam=case["beam"])
    with pytest.raises(RuntimeError, match="more than 10000000 pointings"):
        simutil.simutil().calc_pointings2("1e-300arcsec", "30arcsec", direction="J2000 19h00m00 -23d00m00")


@needs_measures_data
def test_readantenna_keeps_the_telescope_noisetemp_defaults_to() -> None:
    # #913 review: CASA's `readantenna` sets `self.telescopename`.
    u = simutil.simutil()
    assert u.readantenna(str(CONFIGS / "vla.a.cfg"))[6] == "VLA"
    assert u.telescopename == "VLA"
    # CASA 6.7.6's for the same two calls (probed 2026-10-27).
    assert u.noisetemp(freq="44GHz") == pytest.approx((0.7362803630484018, 0.95, 0.9997880064, 0.72, 0.86, 110.0),
                                                      rel=1e-12)
    for case in [c for c in FIXTURE["noisetemp"] if c["telescope"] == "VLA" and c["correlator"] is None
                 and "result" in c and c.get("diam") is None and c.get("epsilon") is None][:3]:
        assert u.noisetemp(freq=case["freq"]) == pytest.approx(tuple(case["result"]), rel=1e-12), case
    # baselineLengths and approxBeam read the file as readantenna does.
    for method, arguments in (("baselineLengths", ()), ("approxBeam", (100.0,))):
        v = simutil.simutil()
        getattr(v, method)(str(CONFIGS / "alma.out01.cfg"), *arguments)
        assert v.telescopename == "ALMA", method


def test_pointing_files_read_and_write_as_casas(tmp_path: Path) -> None:
    for case in FIXTURE["read_pointings"]:
        path = write(tmp_path, case["name"] + ".ptg.txt", case["text"])
        u = simutil.simutil()
        assert u.read_pointings(path) == (case["n"], case["pointings"], case["times"])
        assert u.pointings == case["pointings"]
    with pytest.raises(RuntimeError, match="No valid lines found in pointing file"):
        simutil.simutil().read_pointings(write(tmp_path, "empty.ptg.txt", "# none\n"))
    with pytest.raises(FileNotFoundError):
        simutil.simutil().read_pointings(str(tmp_path / "missing.ptg.txt"))
    for case in FIXTURE["write_pointings"]:
        path = tmp_path / "out.ptg.txt"
        simutil.simutil().write_pointings(str(path), case["pointings"], time=case["time"])
        assert path.read_text(encoding="utf-8") == case["text"], case["time"]


def test_directions_split_and_centre_as_casas() -> None:
    u = simutil.simutil()
    for case in FIXTURE["direction_splitter"]:
        if "error" in case:
            with pytest.raises(RuntimeError):
                u.direction_splitter(case["direction"])
            continue
        epoch, x, y = u.direction_splitter(case["direction"])
        assert epoch == case["epoch"]
        assert x == {"unit": "deg", "value": pytest.approx(case["x"], rel=1e-12)}
        assert y == {"unit": "deg", "value": pytest.approx(case["y"], rel=1e-12, abs=1e-12)}
    for method in ("average_direction", "median_direction"):
        for case in FIXTURE[method]:
            centre, offsets = getattr(u, method)(case["directions"])
            assert centre == case["direction"]
            assert isinstance(offsets, np.ndarray) and offsets.shape == (2, len(case["directions"]))
            np.testing.assert_allclose(offsets, case["offsets"], rtol=0, atol=1e-12)
    for case in FIXTURE["wrapang"]:
        assert u.wrapang(case["ang"], case["target"], case["period"]) == pytest.approx(case["out"], abs=1e-12)


def test_dir_m2s_writes_a_measure_as_simutil_does() -> None:
    measure = {"type": "direction", "refer": "J2000", "m0": {"unit": "deg", "value": 270.000129167},
               "m1": {"unit": "deg", "value": -22.999888889}}
    assert simutil.simutil().dir_m2s(measure) == "J2000 18:00:00.03100 -022.59.59.600000"
    with pytest.raises(RuntimeError, match="converting direction measure"):
        simutil.simutil().dir_m2s({"type": "epoch"})


def test_receivers_bands_and_feeds_are_casas() -> None:
    u = simutil.simutil()
    for case in FIXTURE["noisetemp"][::7]:
        if "error" in case or case["freq"] in ("35GHz", "1GHz"):
            continue
        found = u.noisetemp(telescope=case["telescope"], freq=case["freq"], diam=case.get("diam"),
                            epsilon=case.get("epsilon"), correlator=case["correlator"])
        assert found == pytest.approx(tuple(case["result"]), rel=1e-12), case
    with pytest.raises(AttributeError):
        u.noisetemp()
    u.telescopename = "ALMA"
    assert u.noisetemp() == u.noisetemp(telescope="ALMA", freq="245GHz")
    for case in FIXTURE["bandname"]:
        assert u.bandname(case["freq"]) == case["band"]
    for case in FIXTURE["polsettings"]:
        assert list(u.polsettings(case["telescope"], poltype=case["poltype"])) == case["out"]
    assert u.polsettings("VLA", listall=True) == {"circular": ("RR LL", "perfect R L"),
                                                 "linear": ("XX YY", "perfect X Y"), "RR": ("RR", "perfect R L")}


def test_quantities_and_directions_are_tested_as_casa_tests_them() -> None:
    u = simutil.simutil()
    assert u.isquantity("10arcsec") and u.isquantity(["1GHz", {"unit": "s", "value": 2.0}])
    assert not u.isquantity("", halt=False) and not u.isquantity("abc", halt=False)
    with pytest.raises(RuntimeError, match="can't interpret 'abc' as a CASA quantity"):
        u.isquantity("abc")
    assert u.isdirection("J2000 10h00m00 -30d00m00")
    assert not u.isdirection("10h00m00 -30d00m00", halt=False)
    with pytest.raises(RuntimeError, match="as a direction"):
        u.isdirection("nowhere")


def test_local_positions_are_east_north_up() -> None:
    lat, lon, el = simutil.simutil().itrf2loc([2225117.5], [-5440262.6], [-2481145.2],
                                              2225061.0, -5440057.0, -2481681.0)
    assert all(isinstance(axis, np.ndarray) and axis.shape == (1,) for axis in (lat, lon, el))
    assert np.hypot(np.hypot(lat[0], lon[0]), el[0]) == pytest.approx(
        np.hypot(np.hypot(56.5, -205.6), 535.8), rel=1e-9)


@needs_measures_data
def test_beams_are_casas() -> None:
    u = simutil.simutil()
    for case in FIXTURE["beams"]:
        path = str(CONFIGS / case["config"])
        if "beam_arcsec" in case:
            assert u.approxBeam(path, case["freq_ghz"]) == pytest.approx(case["beam_arcsec"], rel=1e-9)
        else:
            lengths = u.baselineLengths(path)
            assert isinstance(lengths, np.ndarray) and len(lengths) == case["baselines"]
            assert lengths.sum() == pytest.approx(case["sum_m"], rel=1e-9)


# The sky-model fixture casa-simulation's Rust tests compare in full: CASA's
# inputs, and what its modifymodel and flatimage made of them.
SKYMODEL = Path(__file__).resolve().parents[3] / "casa-simulation" / "tests" / "data"


@pytest.fixture(scope="module")
def skymodel(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict[str, Any], Path]:
    with gzip.open(SKYMODEL / "skymodel_casa.json.gz", "rt", encoding="utf-8") as handle:
        record = json.load(handle)
    inputs = tmp_path_factory.mktemp("skymodel")
    # The "data" filter is in Python from 3.10.12 and 3.11.4.
    safe = {"filter": "data"} if hasattr(tarfile, "data_filter") else {}
    with tarfile.open(SKYMODEL / "skymodel_inputs.tar.gz") as archive:
        archive.extractall(inputs, **safe)
    return record, inputs / "inputs"


def skymodel_case(record: dict[str, Any], kind: str, name: str) -> dict[str, Any]:
    return next(case for case in record[kind] if case["name"] == name)


def same(found: Any, expected: Any, path: str = "") -> None:
    """`found` is CASA's `expected`: lists and tuples item by item, floats to
    1e-12."""
    if isinstance(expected, list):
        assert isinstance(found, (list, tuple)) and len(found) == len(expected), (path, found, expected)
        for index, (item, wanted) in enumerate(zip(found, expected)):
            same(item, wanted, f"{path}[{index}]")
    else:
        close(found, expected, rel=1e-12, path=path)


def modify(record: dict[str, Any], inputs: Path, name: str, out: Path, **options: Any) -> Any:
    case = skymodel_case(record, "modify", name)
    params = case["params"]
    return simutil.simutil().modifymodel(
        str(inputs / case["input"]), str(out), params["inbright"], params["indirection"], params["incell"],
        params["incenter"], params["inwidth"], -1, **options)


def pixels(path: Path) -> np.ndarray:
    image = Image.open(path)
    return image.get_slice([0] * len(image.shape), list(image.shape)).flatten(order="F")


def assert_casas_pixels(path: Path, expected: dict[str, Any]) -> None:
    assert list(Image.open(path).shape) == expected["shape"]
    wanted = np.array([np.nan if value is None else value for value in expected["pixels"]])
    assert np.allclose(pixels(path), wanted, rtol=0.0, atol=1e-6 * np.nanmax(np.abs(wanted)))


@pytest.mark.parametrize("name", ["dsf", "dsf_overrides", "dsf_incenter_mhz", "dfs_cube", "iquv_cube",
                                  "galactic", "dsf_indirection_list", "fits_cube"])
def test_modifymodel_returns_casas_tuple_and_image(skymodel: tuple[dict[str, Any], Path], tmp_path: Path,
                                                   name: str) -> None:
    record, inputs = skymodel
    case = skymodel_case(record, "modify", name)
    out = tmp_path / f"{name}.skymodel"
    found = modify(record, inputs, name, out)
    assert isinstance(found, tuple) and len(found) == 8
    same(found, case["result"])
    assert Image.open(out).units == "Jy/pixel"
    assert_casas_pixels(out, case["image"])


def test_modifymodel_refuses_as_casa_does(skymodel: tuple[dict[str, Any], Path], tmp_path: Path,
                                          caplog: pytest.LogCaptureFixture) -> None:
    record, inputs = skymodel
    with caplog.at_level(logging.INFO, logger="casars.simutil"):
        assert modify(record, inputs, "dsf_indirection_two", tmp_path / "two.skymodel") is False
    assert "should be a single direction string" in caplog.text
    for name in ("plain2d", "rotated", "linear2d"):
        with pytest.raises(RuntimeError) as raised:
            modify(record, inputs, name, tmp_path / f"{name}.skymodel")
        assert str(raised.value) == skymodel_case(record, "modify", name)["error"]["message"]


def test_flatimage_writes_casas_flat_image(skymodel: tuple[dict[str, Any], Path], tmp_path: Path) -> None:
    record, inputs = skymodel
    for name, complist in (("dfs_cube", None), ("dsf", "shapes.cl")):
        out = tmp_path / f"{name}.skymodel"
        modify(record, inputs, name, out)
        u = simutil.simutil()
        assert u.flatimage(str(out), complist=str(inputs / complist) if complist else "") is None
        flat = next(case for case in record["flat"] if case["name"] == name and case["complist"] == complist)
        assert_casas_pixels(Path(f"{out}.flat"), flat["image"])


def test_modifymodel_makes_the_flat_image_when_asked(skymodel: tuple[dict[str, Any], Path],
                                                     tmp_path: Path) -> None:
    record, inputs = skymodel
    out = tmp_path / "iquv.skymodel"
    modify(record, inputs, "iquv_cube", out, flatimage=True)
    flat = next(case for case in record["flat"] if case["name"] == "iquv_cube")
    assert_casas_pixels(Path(f"{out}.flat"), flat["image"])
    with pytest.raises(RuntimeError, match="not found in flatimage"):
        simutil.simutil().flatimage(str(out), complist=str(tmp_path / "missing.cl"))


# The analysis fixture casa-simulation's Rust tests compare in full: CASA's
# statim, cellsize and convimage on its input images.
@pytest.fixture(scope="module")
def analysis(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict[str, Any], Path]:
    with gzip.open(SKYMODEL / "simanalyze_analysis_casa.json.gz", "rt", encoding="utf-8") as handle:
        record = json.load(handle)
    assert record["casa_version"] == "6.7.6-14"
    root = tmp_path_factory.mktemp("analysis")
    with tarfile.open(SKYMODEL / "simanalyze_analysis_inputs.tar.gz") as archive:
        archive.extractall(root, filter="data")
    return record, root / "inputs"


def test_statim_gives_casas_statistics(analysis: tuple[dict[str, Any], Path]) -> None:
    record, inputs = analysis
    u = simutil.simutil()
    for name, casa in record["statim"].items():
        if name == "beamless.flat":
            # CASA scales a Jy/beam image without a beam by its pixel area
            # and casa-rs does not (docs/CASA (C++) bugs.md); the Rust test
            # compares it with that factor.
            continue
        if "error" in casa:
            with pytest.raises(RuntimeError) as raised:
                u.statim(str(inputs / name), plot=False)
            assert f"Exception: {raised.value}" == casa["error"], name
            continue
        found = u.statim(str(inputs / name), plot=False)
        assert isinstance(found, tuple) and len(found) == 4, name
        for value, key in zip(found[:3], ("min", "max", "rms")):
            # CASA's np.squeeze leaves 0-d float64 arrays.
            assert isinstance(value, np.ndarray) and value.shape == () and value.dtype == np.float64, (name, key)
            assert float(value) == pytest.approx(casa[key], rel=1e-6), (name, key)
        assert found[3] == casa["unit"], name


def test_cellsize_gives_casas_cells(analysis: tuple[dict[str, Any], Path]) -> None:
    record, inputs = analysis
    u = simutil.simutil()
    for name, casa in record["cellsize"].items():
        if "error" in casa:
            with pytest.raises(RuntimeError) as raised:
                u.cellsize(str(inputs / name))
            assert f"Exception: {raised.value}" == casa["error"], name
            continue
        found = u.cellsize(str(inputs / name))
        assert isinstance(found, list) and [cell["unit"] for cell in found] == casa["units"], name
        assert [cell["value"] for cell in found] == pytest.approx(casa["cell"], rel=1e-15), name


def assert_casas_masked_pixels(path: Path, expected: dict[str, Any]) -> None:
    assert list(Image.open(path).shape) == expected["shape"]
    wanted = np.array([np.nan if value is None else value for value in expected["pixels"]])
    found = pixels(path)
    valid = np.isfinite(wanted)
    assert np.allclose(found[valid], wanted[valid], rtol=0.0, atol=1e-6 * np.nanmax(np.abs(wanted)))


def test_convimage_regrids_and_convolves_as_casa_does(analysis: tuple[dict[str, Any], Path],
                                                      tmp_path: Path) -> None:
    record, inputs = analysis
    u = simutil.simutil()
    for case in ("aligned", "offset"):
        casa = record["convimage"][case]
        model = tmp_path / case / casa["model"]
        shutil.copytree(inputs / casa["model"], model)
        assert u.convimage(str(model), str(inputs / casa["image"])) is None
        assert_casas_masked_pixels(Path(f"{model}.regrid"), casa["regrid"])
        assert_casas_masked_pixels(Path(f"{model}.regrid.conv"), casa["convolved"])
        assert Image.open(Path(f"{model}.regrid.conv")).units == casa["convolved"]["unit"]
    rotated = record["convimage"]["rotated"]
    model = tmp_path / "rotated" / rotated["model"]
    shutil.copytree(inputs / rotated["model"], model)
    with pytest.raises(RuntimeError) as raised:
        u.convimage(str(model), str(inputs / rotated["image"]))
    assert f"Exception: {raised.value}" == rotated["error"]
    with pytest.raises(NotImplementedError, match=r"complist is not yet implemented in casa-rs \(#1053\)"):
        u.convimage(str(model), str(inputs / "image.flat"), complist="x.cl")


def simulated_ms(path: Path, antennas: int) -> str:
    """A MeasurementSet of `antennas` VLA dishes, observed for 20 s."""
    from casars.tools import simulator

    sm = simulator()
    assert sm.open(str(path))
    x = [-1601185.4 + 30.0 * i for i in range(antennas)]
    assert sm.setconfig(telescopename="VLA", x=x, y=[-5041977.5] * antennas, z=[3554875.9] * antennas,
                        dishdiameter=[25.0], mount=["ALT-AZ"], antname=[f"A{i}" for i in range(antennas)],
                        padname=[f"P{i}" for i in range(antennas)], coordsystem="global")
    assert sm.setspwindow(spwname="L", freq="1.4GHz", deltafreq="1MHz", freqresolution="1MHz", nchannels=1,
                          stokes="RR LL")
    assert sm.settimes(integrationtime="10s", usehourangle=False,
                       referencetime={"m0": {"unit": "d", "value": 58849.0}, "refer": "UTC", "type": "epoch"})
    assert sm.setfield(sourcename="F", sourcedirection="J2000 12h00m00 45d00m00")
    assert sm.observe("F", "L", starttime="0s", stoptime="20s")
    assert sm.close()
    return str(path)


@needs_measures_data
def test_ismstp_tells_total_power_as_casa_does(tmp_path: Path) -> None:
    u = simutil.simutil()
    single = simulated_ms(tmp_path / "single.ms", 1)
    array = simulated_ms(tmp_path / "array.ms", 3)
    # One antenna is total power; an array with nonzero UVW is not.
    assert u.ismstp(single) is True
    assert u.ismstp(array) is False
    with pytest.raises(RuntimeError, match=rf"^input file '{array}' is not a totalpower ms$"):
        u.ismstp(array, halt=True)
    missing = str(tmp_path / "missing.ms")
    assert u.ismstp(missing) is False
    with pytest.raises(RuntimeError, match=rf"^can't understand the file '{missing}'$"):
        u.ismstp(missing, halt=True)


#: CASA 6.7.6.14's simutil geodesy on a grid of inputs
#: (tools/simulation/simutil_geodesy_oracle.py).
GEODESY = json.loads((Path(__file__).resolve().parent / "data" / "casa_simutil_geodesy.json").read_text(encoding="utf-8"))


def test_getdatum_gives_casas_table() -> None:
    assert GEODESY["casa_version"] == "6.7.6-14"
    u = simutil.simutil()
    for code, casa in GEODESY["getdatum"].items():
        if "error" in casa:
            with pytest.raises(RuntimeError, match=f"^{casa['error']}$"):
                u.getdatum(code)
            continue
        found = u.getdatum(code)
        assert isinstance(found, tuple) and list(found) == casa, code
        # CASA's table writes whole offsets as ints.
        assert [type(value) for value in found] == [type(value) for value in casa], code


def test_tconpc_and_tmgeod_are_casas() -> None:
    u = simutil.simutil()
    for case in GEODESY["tconpc"]:
        assert u.tconpc(*case["args"]) == pytest.approx(tuple(case["result"]), rel=1e-12, abs=1e-15)
    for case in GEODESY["tmgeod"]:
        assert u.tmgeod(*case["args"]) == pytest.approx(tuple(case["result"]), rel=1e-12, abs=1e-15)


def test_utm_and_geodetic_conversions_are_casas() -> None:
    u = simutil.simutil()
    for case in GEODESY["utm2long"]:
        assert u.utm2long(*case["args"]) == pytest.approx(tuple(case["result"]), rel=1e-12), case["args"]
    for name in ("utm2xyz", "long2xyz"):
        for case in GEODESY[name]:
            found = getattr(u, name)(*case["args"])
            assert found == pytest.approx(tuple(case["result"]), rel=0.0, abs=1e-6), (name, case["args"])
    for case in GEODESY["locxyz2itrf"]:
        assert u.locxyz2itrf(*case["args"]) == pytest.approx(tuple(case["result"]), rel=0.0, abs=1e-6)


def test_xyz2long_inverts_long2xyz_where_casa_slips() -> None:
    """CASA's xyz2long subtracts the datum's x offset from y, and
    xyz2long_old its x offset from z; casa-rs subtracts dy and dz
    (docs/CASA (C++) bugs.md). Where the offsets agree they give CASA's
    values; elsewhere casa-rs's inverts long2xyz."""
    u = simutil.simutil()
    for forward, casa, old in zip(GEODESY["long2xyz"], GEODESY["xyz2long"], GEODESY["xyz2long_old"]):
        lon, lat, height, code = forward["args"]
        dx, dy, dz, _, _ = u.getdatum(code)
        found = u.xyz2long(*casa["args"])
        if dx == dy:
            assert found == pytest.approx(tuple(casa["result"]), rel=1e-12, abs=1e-9), code
        assert found == pytest.approx((lon, lat, height), rel=0.0, abs=1e-6), code
        found_old = u.xyz2long_old(*old["args"])
        assert found_old[0] == pytest.approx(old["result"][0], rel=1e-12, abs=1e-15), code
        if dx == dz:
            assert found_old == pytest.approx(tuple(old["result"]), rel=1e-12, abs=1e-15), code


@needs_measures_data
@pytest.mark.parametrize("case", GEODESY["itrf2locname"], ids=lambda case: case["args"][3])
def test_itrf2locname_places_positions_about_casas_observatory(case: dict[str, Any]) -> None:
    u = simutil.simutil()
    found = u.itrf2locname(*case["args"])
    assert isinstance(found, tuple) and all(isinstance(axis, np.ndarray) for axis in found)
    for axis, casa in zip(found, case["result"]):
        assert axis == pytest.approx(casa["ndarray"], rel=0.0, abs=1e-6), case["args"][3]
