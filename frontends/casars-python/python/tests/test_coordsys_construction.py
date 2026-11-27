"""Pinned CASA comparisons for standalone coordinate construction and records."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    package = pytest.importorskip("casatools")
    assert package.version_string().replace("-", ".") == "6.7.6.14"
    return package


@pytest.fixture(params=["casa", "casars"])
def validation_backend(request):
    return tools if request.param == "casars" else request.getfixturevalue("casa")


@pytest.mark.parametrize(("field", "value"), [
    ("pc", np.zeros((2, 2), dtype=np.float64)),
    ("pc", np.asarray([[0., 2.], [0., 3.]])),
    ("cdelt", [0., 5.]),
])
def test_rejected_linear_records_keep_the_existing_transform(validation_backend, tmp_path, field, value):
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    api = validation_backend
    coordinate = api.coordsys().newcoordsys(linear=2)
    assert coordinate.setincrement([3., 5.])
    good = coordinate.torecord()
    invalid = coordinate.torecord()
    invalid["linear0"][field] = value
    with pytest.raises(RuntimeError, match="Linear transformation matrix is singular"):
        coordinate.fromrecord(invalid)
    np.testing.assert_array_equal(coordinate.toworld([2., 3.])["numeric"], [6., 15.])

    image = api.image()
    path = str(tmp_path / f"{api.__name__}.im")
    try:
        assert image.fromarray(path, pixels, csys=good)
        history = image.history()
        with pytest.raises(RuntimeError, match="Linear transformation matrix is singular"):
            image.setcoordsys(invalid)
        assert image.isopen()
        np.testing.assert_array_equal(image.toworld([2., 3.])["numeric"], [6., 15.])
        np.testing.assert_array_equal(image.getchunk(), pixels)
        assert image.history() == history
        assert image.done()
        assert image.open(path)
        np.testing.assert_array_equal(image.toworld([2., 3.])["numeric"], [6., 15.])
        assert image.history() == history
    finally:
        image.done()


def test_linear_record_with_dependent_nonzero_rows_keeps_the_casa_forward_map(validation_backend):
    api = validation_backend
    coordinate = api.coordsys().newcoordsys(linear=2)
    record = coordinate.torecord()
    record["linear0"]["pc"] = np.asarray([[1., 2.], [2., 4.]])
    assert coordinate.fromrecord(record)
    np.testing.assert_array_equal(coordinate.toworld([2., 3.])["numeric"], [8., 16.])


@pytest.mark.parametrize("method", ["fromshape", "fromarray", "newimagefromshape", "newimagefromarray"])
def test_invalid_linear_record_cannot_overwrite_an_existing_image(validation_backend, tmp_path, method):
    api = validation_backend
    coordinate = api.coordsys().newcoordsys(linear=2)
    invalid = coordinate.torecord()
    invalid["linear0"]["cdelt"] = [0., 5.]
    image = api.image()
    path = str(tmp_path / f"{api.__name__}.im")
    supplied = {"shape": [2, 2]} if method.endswith("shape") else {"pixels": [[1., 2.], [3., 4.]]}
    try:
        assert image.fromarray(path, [18., 34.])
        assert image.done()
        assert image.fromarray(pixels=[8., 9.])
        with pytest.raises(RuntimeError, match="Linear transformation matrix is singular"):
            getattr(image, method)(outfile=path, csys=invalid, overwrite=True, **supplied)
        assert image.isopen() == method.startswith("new")
        if image.isopen():
            np.testing.assert_array_equal(image.getchunk(), [8., 9.])
        assert image.open(path)
        np.testing.assert_array_equal(image.getchunk(), [18., 34.])
    finally:
        image.done()


def check_record(actual, expected):
    observed = actual.torecord()
    reference = expected.torecord()
    rust_epoch = observed.pop("obsdate")
    casa_epoch = reference.pop("obsdate")
    assert rust_epoch["type"] == casa_epoch["type"] == "epoch"
    assert rust_epoch["refer"] == casa_epoch["refer"]
    assert rust_epoch["m0"]["unit"] == casa_epoch["m0"]["unit"] == "d"
    assert rust_epoch["m0"]["value"] == pytest.approx(casa_epoch["m0"]["value"], rel=0, abs=2e-5)
    _assert_value(observed, reference)
    _assert_value(actual.epoch(), rust_epoch)


@pytest.mark.parametrize(
    "args,kwargs",
    [
        ((), {}),
        ((True, True, "I,Q,RR", 2, True), {}),
        ((True, False, "I Q U V", 0, False), {}),
        ((), {"tabular": True, "linear": -2}),
        ((), {"stokes": ["RX", "PP", "RCircular", "LCircular", "Linear", "Pangle"]}),
        ((), {"stokes": ["unrecognized", "XY"]}),
        ((), {"stokes": ["I", "Yy"], "linear": 1}),
        ((), {"stokes": ""}),
        ((), {"stokes": "I, Q"}),
        ((), {"stokes": "I  Q  RR "}),
        ((), {"stokes": ",I,,QQ,"}),
        ((), {"stokes": "I\tQ"}),
        ((), {"stokes": ("I", "Q")}),
        ((), {"stokes": np.array(["I", "Q"])}),
        ((), {"stokes": None}),
    ],
)
def test_newcoordsys_creates_a_matching_independent_child(casa, args, kwargs):
    root = tools.coordsys()
    oracle = casa.coordsys()
    assert root.setparentname("caller") and oracle.setparentname("caller")
    actual = root.newcoordsys(*args, **kwargs)
    expected = oracle.newcoordsys(*args, **kwargs)
    check_record(actual, expected)
    check_record(root, oracle)
    assert root.parentname() == oracle.parentname() == "caller"
    assert actual.parentname() == expected.parentname() == "unknown"
    assert actual.addcoordinate(linear=2) and expected.addcoordinate(linear=2)
    check_record(actual, expected)
    assert actual.naxes() == root.naxes() + 2


def test_addcoordinate_successive_groups_and_duplicates(casa):
    for make in (tools.coordsys, casa.coordsys):
        cs = make()
        assert cs.addcoordinate(True, False, ["I", "Q"], 0, False)
        assert cs.addcoordinate(tabular=True, spectral=True, linear=2)
        assert cs.addcoordinate(linear=1)
        assert cs.coordinatetype() == ["Direction", "Stokes", "Spectral", "Linear", "Tabular", "Linear"]
        np.testing.assert_array_equal(cs.findcoordinate("linear", 1)["pixel"], [7])
        with pytest.raises(TypeError, match="list _stokes must contain strings"):
            cs.addcoordinate(direction=True, stokes=["I", 1])
        assert cs.naxes() == 8
        with pytest.raises(RuntimeError, match="same Stokes more than once"):
            cs.addcoordinate(direction=True, stokes="RR RR")
        assert cs.coordinatetype(-1)[-1] == "Direction"
        assert cs.naxes() == 10
        with pytest.raises(RuntimeError, match="Unable to create new coordsys"):
            cs.newcoordsys(stokes=["I", "I"])
        assert cs.naxes() == 0
        with pytest.raises(RuntimeError, match="Unable to create new coordsys"):
            cs.newcoordsys(direction=True, stokes=["I", "I"])
        assert cs.naxes() == 2
        assert cs.telescope() == cs.observer() == "UNKNOWN"
        assert cs.epoch() == {"type":"epoch", "refer":"LAST", "m0":{"unit":"d", "value":0.0}}


def test_complete_stokes_code_domain(casa):
    labels = [
        "??", "I", "Q", "U", "V", "RR", "RL", "LR", "LL", "XX", "XY", "YX", "YY",
        "RX", "RY", "LX", "LY", "XR", "XL", "YR", "YL", "PP", "PQ", "QP", "QQ",
        "RCircular", "LCircular", "Linear", "Ptotal", "Plinear", "PFtotal",
        "PFlinear", "Pangle",
    ]
    actual = tools.coordsys().newcoordsys(stokes=labels)
    expected = casa.coordsys().newcoordsys(stokes=labels)
    assert actual.stokes() == expected.stokes() == labels
    check_record(actual, expected)


def test_casa_and_rust_exchange_edited_coordinate_records(casa, tmp_path):
    source = casa.coordsys().newcoordsys(direction=True, stokes=["I", "RR"], linear=1, tabular=True)
    assert source.setreferencepixel([3.5, 2.5], type="Direction")

    rust = tools.coordsys()
    assert rust.setparentname("stored parent")
    assert rust.fromrecord(source.torecord())
    assert rust.parentname() == "stored parent"
    check_record(rust, source)
    assert rust.setreferencepixel([1.5], type="linear")

    readback = casa.coordsys()
    record = rust.torecord()
    assert readback.fromrecord(dict(record, parentName="external"))
    assert readback.parentname() == "external"
    check_record(rust, readback)
    np.testing.assert_array_equal(readback.referencepixel("linear")["numeric"], [1.5])

    target = str(tmp_path / "mixed.im")
    writer = tools.image()
    assert writer.fromshape(target, [5, 6, 2, 5, 4], csys=record)
    assert writer.done()
    reader = casa.image()
    assert reader.open(target)
    check_record(reader.coordsys(), readback)
    assert reader.done()


def test_empty_record_restores_casa_observation_defaults(casa):
    rust = tools.coordsys()
    expected = casa.coordsys()
    for record in ({}, {"telescope": "ALMA"}, {"observer": "unknown field"},
                   {"unused": 123}, {"parentName": "sender"}):
        assert rust.fromrecord(record) and expected.fromrecord(record)
        assert rust.parentname() == expected.parentname()
        assert rust.naxes() == expected.naxes() == 0
        check_record(rust, expected)


def test_constructing_after_a_record_transpose_preserves_existing_maps(casa):
    expected = casa.coordsys().newcoordsys(direction=True, stokes=["I", "Q"], spectral=True)
    assert expected.transpose([2, 3, 0, 1])
    actual = tools.coordsys()
    assert actual.fromrecord(expected.torecord())
    assert actual.addcoordinate(linear=2, tabular=True)
    assert expected.addcoordinate(linear=2, tabular=True)
    check_record(actual, expected)
    np.testing.assert_array_equal(actual.findcoordinate("direction")["pixel"], [2, 3])
    np.testing.assert_array_equal(actual.findcoordinate("linear")["pixel"], [4, 5])


def test_malformed_record_retains_previous_coordinate_state():
    cs = tools.coordsys().newcoordsys(direction=True)
    assert cs.setparentname("source")
    with pytest.raises(RuntimeError, match="parentName"):
        cs.fromrecord({"parentName": 4})
    assert cs.naxes() == 2
    assert cs.parentname() == "source"
    assert cs.addcoordinate(stokes=" ")
    assert cs.naxes() == 2


def test_restore_canonicalizes_stokes_and_replacement_vectors(casa):
    expected = casa.coordsys().newcoordsys(stokes="I,RR")
    actual = tools.coordsys()
    for names in (["rr", "yy"], ["unknown", "I"]):
        record = expected.torecord()
        record["stokes0"]["stokes"] = names
        record["stokes0"]["pc"] = [[2.0]]
        record["worldreplace0"] = [1, 2]
        assert actual.fromrecord(record)
        assert expected.fromrecord(record)
        check_record(actual, expected)


def test_restore_rejects_stokes_and_replacement_fields_that_casa_rejects(casa):
    for make in (tools.coordsys, casa.coordsys):
        coordinate = make().newcoordsys(direction=True)
        for names in (["I", "I"], [13, 17]):
            record = make().newcoordsys(stokes="RR").torecord()
            record["stokes0"]["stokes"] = names
            with pytest.raises(RuntimeError):
                coordinate.fromrecord(record)
            assert coordinate.coordinatetype() == ["Direction"]
        record = make().newcoordsys(stokes="RR").torecord()
        record["worldreplace0"] = "no"
        with pytest.raises(RuntimeError, match="worldreplace0"):
            coordinate.fromrecord(record)
        assert coordinate.coordinatetype() == ["Direction"]


def test_restore_rejects_sparse_coordinate_indices_and_invalid_replacements():
    coordinate = tools.coordsys().newcoordsys(direction=True)
    linear = tools.coordsys().newcoordsys(linear=1).torecord()
    for field in ("linear", "pixelmap", "worldmap", "pixelreplace", "worldreplace"):
        linear[field + "1"] = linear.pop(field + "0")
    with pytest.raises(RuntimeError, match="numbering must be contiguous"):
        coordinate.fromrecord(linear)
    assert coordinate.coordinatetype() == ["Direction"]
    record = tools.coordsys().newcoordsys(stokes="I").torecord()
    record["worldreplace0"] = [[1.0]]
    with pytest.raises(RuntimeError, match="worldreplace0.*numeric vector"):
        coordinate.fromrecord(record)
    assert coordinate.coordinatetype() == ["Direction"]
