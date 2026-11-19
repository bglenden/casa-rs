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
