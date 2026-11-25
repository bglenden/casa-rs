"""Attach CASA comet tables and read the persisted field through both tools."""

from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_tools_ms_putdata import writer_api
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import ephemeris_dataset, measures, native_dataset


def test_addephemeris_creates_the_optional_column(ephemeris_dataset, tmp_path, writer_api):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    dataset, _ = ephemeris_dataset
    field = dataset / "FIELD"
    source = tmp_path / "comet source.tab"
    reference = casa.ms()
    assert reference.open(str(dataset))
    expected = reference.getfielddirmeas("EPHEMERIS_DIR", 0)
    assert reference.done()
    (field / "EPHEM0_TEST.tab").rename(source)
    table = casa.table()
    assert table.open(str(field), nomodify=False)
    when = table.getcell("TIME", 0)
    assert table.removecols("EPHEMERIS_ID")
    assert table.done()

    tool = writer_api.ms()
    assert tool.open(str(dataset), nomodify=False)
    assert tool.addephemeris(ephemerisname=str(source), comment="portable", field="F")
    assert tool.done()
    assert source.is_dir()
    assert (field / "EPHEM0_portable.tab").is_dir()

    for api in (casa, tools):
        table = api.table()
        assert table.open(str(field))
        np.testing.assert_array_equal(table.getcol("EPHEMERIS_ID"), [0])
        assert table.getcell("TIME", 0) == when
        assert table.done()
        reader = api.ms()
        assert reader.open(str(dataset))
        direction = reader.getfielddirmeas("EPHEMERIS_DIR", 0)
        assert reader.done()
        assert direction["refer"] == expected["refer"] == "ICRS"
        for coord in ("m0", "m1"):
            assert direction[coord]["unit"] == expected[coord]["unit"] == "rad"
            assert direction[coord]["value"] == pytest.approx(expected[coord]["value"], abs=2e-11)

    assert table.open(str(dataset / "HISTORY"))
    last = table.nrows() - 1
    assert table.getcell("MESSAGE", last) == "Added ephemeris to FIELD table."
    assert table.getcell("ORIGIN", last) == "ms::addephemeris()"
    assert table.getcell("APPLICATION", last) == "ms"
    assert table.getcell("OBSERVATION_ID", last) == -1
    assert table.getcell("OBJECT_ID", last) == 0
    assert table.getcell("PRIORITY", last) == "INFO"
    np.testing.assert_array_equal(table.getcell("APP_PARAMS", last), [""])
    np.testing.assert_array_equal(
        table.getcell("CLI_COMMAND", last),
        [f"field=F id=-1 name='{source}' coment='portable'"],
    )
    assert table.done()


def test_addephemeris_readonly_unselected_copy(ephemeris_dataset, tmp_path, writer_api):
    dataset, _ = ephemeris_dataset
    field = dataset / "FIELD"
    candidate = tmp_path / "ephemeris.tab"
    copytree(field / "EPHEM0_TEST.tab", candidate)
    tool = writer_api.ms()
    assert tool.open(str(dataset))
    assert tool.addephemeris(ephemerisname=str(candidate), comment="unused", field=-1)
    assert tool.done()
    assert {path.name for path in field.glob("EPHEM*.tab")} == {
        "EPHEM0_TEST.tab",
        "EPHEM1_unused.tab",
    }


def test_addephemeris_preserves_an_already_attached_source(ephemeris_dataset):
    dataset, _ = ephemeris_dataset
    source = dataset / "FIELD" / "EPHEM0_TEST.tab"
    tables = {path.name: path.read_bytes() for path in source.iterdir() if path.name != "table.lock"}
    tool = tools.ms()
    assert tool.open(str(dataset), nomodify=False)
    assert tool.addephemeris(0, str(source), "TEST", [0])
    assert tool.done()
    assert tables == {
        path.name: path.read_bytes() for path in source.iterdir() if path.name != "table.lock"
    }


def test_addephemeris_keeps_the_ephemeris_for_an_unselected_field(
    ephemeris_dataset, tmp_path, writer_api,
):
    casa = pytest.importorskip("casatools")
    dataset, _ = ephemeris_dataset
    field = dataset / "FIELD"
    external = tmp_path / "new-target.tab"
    copytree(field / "EPHEM0_TEST.tab", external)
    table = casa.table()
    assert table.open(str(field), nomodify=False)
    original = {
        column: table.getcell(column, 0)
        for column in table.colnames()
        if table.iscelldefined(column, 0)
    }
    assert table.addrows(1)
    for column, value in original.items():
        assert table.putcell(column, 1, "other" if column == "NAME" else value)
    assert table.done()

    tool = writer_api.ms()
    assert tool.open(str(dataset), nomodify=False)
    assert tool.addephemeris(-1, str(external), "selected", field=[0, 99])
    assert tool.done()
    assert (field / "EPHEM0_TEST.tab").is_dir()
    assert (field / "EPHEM1_selected.tab").is_dir()
    assert table.open(str(field))
    np.testing.assert_array_equal(table.getcol("EPHEMERIS_ID"), [1, 0])
    assert table.done()


def test_addephemeris_refuses_readonly_assignments_before_writing(ephemeris_dataset, tmp_path):
    dataset, _ = ephemeris_dataset
    field = dataset / "FIELD"
    external = tmp_path / "new-source.tab"
    copytree(field / "EPHEM0_TEST.tab", external)
    tool = tools.ms()
    assert tool.open(str(dataset))
    with pytest.raises(RuntimeError, match="FIELD table is not writable"):
        tool.addephemeris(0, str(external), "uncommitted", field=[0])
    assert tool.done()
    assert {path.name for path in field.glob("EPHEM*.tab")} == {"EPHEM0_TEST.tab"}


def test_addephemeris_preserves_field_time_when_the_ephemeris_starts_outside_observation(
    ephemeris_dataset, tmp_path,
):
    dataset, _ = ephemeris_dataset
    field = dataset / "FIELD"
    external = tmp_path / "new-source.tab"
    copytree(field / "EPHEM0_TEST.tab", external)
    table = tools.table()
    assert table.open(str(field), nomodify=False)
    assert table.putcell("TIME", 0, 0.0)
    assert table.done()
    tool = tools.ms()
    assert tool.open(str(dataset), nomodify=False)
    assert tool.addephemeris(0, str(external), "time-check", 0)
    assert tool.done()
    assert table.open(str(field))
    assert table.getcell("TIME", 0) == 0.0
    assert table.done()
