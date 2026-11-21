"""CASA Quality coordinates restored from native records and images."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def quality_record(labels=("DATA", "ERROR")):
    seed = tools.coordsys().newcoordsys(linear=1)
    assert seed.settelescope("VLA")
    assert seed.setobserver("Analyst")
    assert seed.setepoch({"type": "epoch", "refer": "UTC", "m0": {"value": 59000., "unit": "d"}})
    record = seed.torecord()
    record.pop("linear0")
    record["quality0"] = {"axes": ["Quality"], "quality": list(labels)}
    return record


def restored(api, record):
    cs = api.coordsys()
    assert cs.fromrecord(record)
    return cs


@pytest.mark.parametrize(
    "labels, canonical, codes",
    [
        (("Error", "data"), ("ERROR", "DATA"), (2., 1.)),
        (("DATA", "ERROR"), ("DATA", "ERROR"), (1., 2.)),
        (("unknown",), ("??",), (0.,)),
    ],
)
def test_quality_record_and_coordinate_transforms(casa, labels, canonical, codes):
    record = quality_record(labels)
    actual = restored(tools, record)
    reference = restored(casa, record)
    _assert_value(actual.torecord()["quality0"], reference.torecord()["quality0"])
    _assert_value(actual.copy().torecord()["quality0"], reference.torecord()["quality0"])
    assert actual.torecord()["quality0"]["quality"].tolist() == list(canonical)
    assert list(actual.coordinatetype()) == ["Quality"]
    assert list(actual.units()) == [""]
    assert not actual.findcoordinate("quality")["return"]
    assert actual.summary(list=False) == reference.summary(list=False)
    assert " ".join(canonical) in actual.summary(list=False)[0]
    for pixel, (label, world) in enumerate(zip(canonical, codes)):
        for format in ("n", "q", "s", "nqs"):
            _assert_value(actual.toworld([pixel], format), reference.toworld([pixel], format))
        assert actual.toworld([pixel])["numeric"].tolist() == [world]
        assert actual.toworld([pixel], "s")["string"].tolist() == [label]
        quantities = actual.toworld([pixel], "q")
        assert quantities["quantity"]["*1"] == {"unit": "", "value": world}
        assert actual.topixel(quantities)["numeric"].tolist() == [float(pixel)]
        assert actual.topixel([str(world)])["numeric"].tolist() == [float(pixel)]
        relative = actual.torel(actual.toworld([pixel]))
        assert relative["numeric"].tolist() == [world]
        _assert_value(actual.toabs(relative), actual.toworld([pixel]))
    with pytest.raises(RuntimeError, match="Cannot handle Coordinates of type Quality"):
        actual.toworld([0], "m")


def test_quality_errors_metadata_and_selected_subimage(casa):
    record = quality_record()
    cs = restored(tools, record)
    reference = restored(casa, record)
    before = cs.torecord()
    for label, value, match in [
        ("pixel", [1.5], r"Pixel 2 is out of range \[0\.\.1\]"),
        ("world", [1.5], "Quality value ERROR is not contained"),
        ("string", ["ERROR"], r"Failed to convert string formatted world \[ERROR\] to double"),
    ]:
        for api in (cs, reference):
            with pytest.raises(RuntimeError, match=match):
                (api.toworld if label == "pixel" else api.topixel)(value)
    for api in (cs, reference):
        with pytest.raises(RuntimeError, match="same Quality more than once"):
            api.fromrecord(quality_record(("DATA", "data")))
        with pytest.raises(RuntimeError, match="Absolute pixel 2 is out of range"):
            api.torel([1.5], isworld=False)
    _assert_value(cs.torecord(), before)
    for api in (cs, reference):
        assert api.setreferencevalue([9.0])
        assert api.setreferencepixel([2.0])
        assert api.setincrement([5.0])
        assert api.setunits(["km"])
    _assert_value(cs.torecord()["quality0"], before["quality0"])
    _assert_value(cs.torecord()["quality0"], reference.torecord()["quality0"])
    assert cs.convert([1], unitsin=["pix"], unitsout=["Hz"]).tolist() == [2.]
    assert cs.setnames(["Error plane"])
    assert list(cs.names()) == ["Error plane"]
    assert reference.setnames(["Error plane"])
    for origin, count, labels in [(0.49, [1], ["DATA"]), (0.6, [1], ["ERROR"]), (1.0, [], ["ERROR"])]:
        narrowed = cs.subimage([origin], count)
        _assert_value(narrowed["quality0"], reference.subimage([origin], count)["quality0"])
        assert narrowed["quality0"]["quality"].tolist() == labels
    with pytest.raises(RuntimeError, match="Illegal origin shift"):
        cs.subimage([2], [1])
    with pytest.raises(RuntimeError, match="New shape is invalid"):
        cs.subimage([1], [2])


def test_native_writer_reader_pairs_keep_labels_and_values(casa, tmp_path):
    pinned = restored(casa, quality_record(("ERROR", "DATA")))
    record = pinned.torecord()
    expected_report, = pinned.summary(list=False)
    data = np.array([19., -7.], dtype=np.float32)
    for writer in (tools, casa):
        name = str(tmp_path / f"{writer.__name__}.im")
        output = writer.image()
        assert output.fromarray(name, data, csys=record)
        assert output.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(name)
            try:
                _assert_value(image.getchunk(), data)
                coords = image.coordsys()
                try:
                    assert coords.summary(list=False) == expected_report
                    assert coords.toworld([0])["numeric"].tolist() == [2.]
                    assert coords.toworld([1], "s")["string"].tolist() == ["DATA"]
                finally:
                    coords.done()
                summary = image.summary(list=False)
                assert summary["axisnames"].tolist() == ["Quality"]
                assert summary["refval"].tolist() == [2.]
                assert any("ERROR DATA" in message for message in summary["messages"])
            finally:
                image.done()


def test_fits_export_refuses_to_discard_quality_planes(tmp_path):
    source = tools.image()
    data = np.array([7., 19.], dtype=np.float32)
    assert source.fromarray(pixels=data, csys=quality_record())
    path = tmp_path / "quality.fits"
    try:
        with pytest.raises(RuntimeError, match="FITS export cannot represent a native Quality"):
            source.tofits(str(path))
        assert not path.exists()
        _assert_value(source.getchunk(), data)
        assert source.coordsys().toworld([1])["numeric"].tolist() == [2.]
    finally:
        source.done()


def test_image_section_selects_quality_labels_with_the_pixels(casa, tmp_path):
    seed = casa.coordsys().newcoordsys(linear=1, tabular=True)
    record = seed.torecord()
    record.pop("tabular1")
    record["quality1"] = {"axes": ["Quality"], "quality": ["DATA", "ERROR"]}
    data = np.arange(6, dtype=np.float32).reshape(3, 2)
    source = str(tmp_path / "source.im")
    writer = casa.image()
    assert writer.fromarray(source, data, csys=record)
    writer.done()
    region_maker = casa.regionmanager()
    region = region_maker.box(blc=[0, 1], trc=[2, 1])
    region_maker.done()
    for api in (tools, casa):
        parent = api.image()
        assert parent.open(source)
        child = parent.subimage(
            str(tmp_path / f"section-{api.__name__}.im"), region=region, list=False,
        )
        try:
            np.testing.assert_array_equal(child.getchunk(), data[:, 1:])
            coords = child.coordsys()
            try:
                assert coords.torecord()["quality1"]["quality"].tolist() == ["ERROR"]
                assert coords.toworld([0, 0])["numeric"].tolist() == [0., 2.]
            finally:
                coords.done()
        finally:
            child.done()
            parent.done()
