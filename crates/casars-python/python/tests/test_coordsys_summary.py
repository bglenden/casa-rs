"""The public coordinate report and its choice of return or logging."""

import logging
import re

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def with_metadata(cs, mjd=59000.0):
    assert cs.settelescope("VLA")
    assert cs.setobserver("Analyst")
    assert cs.setepoch({"type": "epoch", "refer": "UTC", "m0": {"value": mjd, "unit": "d"}})
    return cs


def test_summary_returns_one_report_or_posts_one_log(caplog):
    cs = with_metadata(tools.coordsys().newcoordsys(linear=2))
    report, = cs.summary(list=False)
    assert report.startswith("\nTelescope           : VLA\nObserver            : Analyst\n")
    assert "Date observation    : 2020/05/31/00:00:00.000000\n" in report
    assert "0    0     Linear LinAxis0" in report
    assert report.endswith("1.000000e+00 km")
    with caplog.at_level(logging.INFO, logger="casars.tools.coordsys"):
        assert cs.summary() == ["T"]
    assert caplog.records[-1].message == report
    for v in (None, 1, [], "false", np.bool_(True)):
        with pytest.raises(TypeError, match="argument 3 of type 'bool'"):
            cs.summary(list=v)


@pytest.mark.parametrize("doppler", ["RADIO", "optical", "beta", b"ratio", "bogus"])
def test_complete_report_matches_casa(casa, doppler, caplog):
    def setup(api):
        cs = with_metadata(api.coordsys().newcoordsys(
            direction=True, stokes="I Q", spectral=True, linear=1, tabular=True,
        ))
        assert cs.setrestfrequency(["1.42GHz", "1.43GHz"])
        assert cs.setconversiontype(direction="GALACTIC")
        return cs

    expected, = setup(casa).summary(doppler, list=False)
    actual, = setup(tools).summary(doppler, list=False)
    assert actual == expected
    assert actual.splitlines()[1] == "Direction reference : J2000 (-> GALACTIC)"
    assert "Rest frequency      : 1.42e+09 [1.43e+09] Hz" in actual
    assert re.search(r"\bStokes\s+Stokes\s+I Q$", actual, re.MULTILINE)
    assert "2020/05/31/00:00:00.000000" in actual
    if doppler == "bogus":
        assert "Illegal doppler type, using RADIO" in caplog.text


@pytest.mark.parametrize(
    "frame,position",
    [
        ("J2000", "01:32:00.000"),
        ("GALACTIC", "23.000"),
        ("ECLIPTIC", "23.000"),
        ("HADEC", "+01:32:00.000"),
        ("AZEL", "23.000"),
        ("ITRF", "01:32:00.000"),
    ],
)
def test_direction_report_units_and_precision(casa, frame, position):
    def setup(api):
        cs = with_metadata(api.coordsys().newcoordsys(direction=True), mjd=0.0)
        assert cs.setdirection(
            refcode=frame, refval=["23deg", "-36deg"], incr=["0.25deg", "0.5deg"],
            refpix=[10.23, -4.555],
        )
        return cs

    expected, = setup(casa).summary(list=False)
    actual, = setup(tools).summary(list=False)
    assert actual == expected
    assert position in actual
    assert "Date observation    : UNKNOWN" in actual


def test_native_writer_reader_pairs_report_the_mapped_grid(casa, tmp_path):
    source = with_metadata(casa.coordsys().newcoordsys(direction=True, spectral=True))
    assert source.setreferencepixel([5.0, 6.0, 2.0])
    assert source.transpose([2, 0, 1])
    record = source.torecord()
    expected, = source.summary(list=False)
    rows = expected.splitlines()[-4:]
    assert re.match(r"0\s+1\s+Spectral\s+Frequency", rows[0])
    assert re.match(r"1\s+0\s+Direction\s+Right Ascension", rows[2])
    for writer in (tools, casa):
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        assert image.fromshape(path, [5, 14, 16], record)
        assert image.done()
        for reader in (tools, casa):
            opened = reader.image()
            assert opened.open(path)
            try:
                actual, = opened.coordsys().summary(list=False)
                assert actual == expected
            finally:
                opened.done()


def test_retained_world_axis_uses_the_complete_replacement_pixel(casa):
    source = with_metadata(casa.coordsys().newcoordsys(direction=True, spectral=True))
    assert source.setunits(["deg", "deg", "Hz"])
    assert source.setreferencepixel([2.0, 3.0, 4.0])
    assert source.setreferencevalue([25.0, 17.0, 1.0e9])
    assert source.setincrement([-0.1, 0.1, 1000.0])
    record = source.torecord()
    record["pixelmap0"] = np.array([-1, 0], dtype=np.int32)
    record["pixelmap1"] = np.array([1], dtype=np.int32)
    record["pixelreplace0"] = np.array([5.0, 8.0])
    got = []
    for api in (tools, casa):
        cs = api.coordsys()
        assert cs.fromrecord(record)
        got.append(cs.summary(list=False))
    assert got[0] == got[1]
    row = next(line for line in got[0][0].splitlines() if line.startswith(".."))
    assert "Right Ascension" in row
    assert "01:38:44.506     5.00" in row
