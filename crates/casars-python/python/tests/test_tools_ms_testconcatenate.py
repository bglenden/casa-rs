"""Subtable-only concatenation through both MeasurementSet tool packages."""

from shutil import copytree, rmtree

import numpy as np
import pytest

from casars import tools
from test_tools_ms_createmultims import pinned_casa
from test_tools_msmetadata import ephemeris_dataset, measures, native_dataset


@pytest.fixture(params=["casa", "native"])
def concat_api(request):
    return pinned_casa() if request.param == "casa" else tools


def copy_inputs(dataset, directory):
    attached = directory / "attached.ms"
    donor = directory / "donor.ms"
    copytree(dataset, attached)
    copytree(dataset, donor)
    return attached, donor


def counts(api, path, names):
    table = api.table()
    result = []
    for name in names:
        assert table.open(str(path / name))
        result.append(table.nrows())
        assert table.close()
    return result


@pytest.mark.parametrize(("options", "merged"), [
    ({}, False),
    ({"freqtol": None, "dirtol": None}, False),
    ({"freqtol": [], "dirtol": ()}, False),
    ({"freqtol": np.array([]), "dirtol": np.array([])}, False),
    ({"freqtol": "", "dirtol": ""}, True),
    ({"freqtol": "1Hz", "dirtol": "1mas"}, True),
    ({"freqtol": {"value": 1.0, "unit": "Hz"}, "dirtol": {"value": 1.0, "unit": "mas"}}, True),
])
def test_tolerance_forms_persist_field_and_window_mappings(
    native_dataset, tmp_path, concat_api, options, merged,
):
    attached, donor = copy_inputs(native_dataset, tmp_path)
    tab = tools.table()
    assert tab.open(str(donor / "SPECTRAL_WINDOW"), nomodify=False)
    for column in ("CHAN_FREQ", "REF_FREQUENCY"):
        assert tab.putcell(column, 0, tab.getcell(column, 0) + 0.5)
    assert tab.done()

    tool = concat_api.ms()
    assert tool.testconcatenate(str(tmp_path / "absent.ms")) is True
    assert tool.open(str(attached), nomodify=False)
    nrows = tool.nrow()
    assert tool.testconcatenate(str(donor), **options) is True
    assert tool.nrow() == nrows
    assert tool.done()

    expected_count = 1 if merged else 2
    for reader in (tools, pinned_casa()):
        assert counts(reader, attached, ("FIELD", "SPECTRAL_WINDOW", "POINTING")) == [
            expected_count, expected_count, 12,
        ]
        source = reader.table()
        assert source.open(str(attached / "SOURCE"))
        np.testing.assert_array_equal(source.getcol("SPECTRAL_WINDOW_ID"), range(expected_count))
        assert source.done()
        assert source.open(str(donor / "HISTORY"))
        last = source.nrows() - 1
        assert source.getcell("ORIGIN", last) == "ms::testconcatenate()"
        assert source.getcell("MESSAGE", last) == f"Subtables from {donor} appended to those from {attached}"
        assert source.done()


@pytest.mark.parametrize("empty_input", ["attached", "donor"])
def test_state_rows_preserved_with_one_empty_input(native_dataset, tmp_path, concat_api, empty_input):
    attached, donor = copy_inputs(native_dataset, tmp_path)
    selected = attached if empty_input == "attached" else donor
    table = tools.table()
    assert table.open(str(selected / "STATE"), nomodify=False)
    assert table.removerows([0])
    assert table.done()

    tool = concat_api.ms()
    assert tool.open(str(attached), nomodify=False)
    assert tool.testconcatenate(str(donor)) is True
    assert tool.done()
    for reader in (tools, pinned_casa()):
        assert counts(reader, selected, ("STATE",)) == [0]
        other = donor if selected == attached else attached
        assert counts(reader, other, ("STATE",)) == [1]


def test_duplicate_observation_preserves_attached_rows_and_appends_history(native_dataset, tmp_path):
    attached, donor = copy_inputs(native_dataset, tmp_path)
    table = tools.table()
    assert table.open(str(attached / "OBSERVATION"), nomodify=False)
    original = {name: table.getcell(name, 0) for name in table.colnames() if table.iscelldefined(name, 0)}
    assert table.addrows(1)
    for name, value in original.items():
        assert table.putcell(name, 1, value)
    assert table.done()
    tool = tools.ms()
    assert tool.open(str(attached), nomodify=False)
    assert tool.testconcatenate(str(donor), "1Hz", "1mas")
    assert tool.done()
    for reader in (tools, pinned_casa()):
        assert counts(reader, attached, ("OBSERVATION", "FIELD")) == [2, 1]
        table = reader.table()
        assert table.open(str(attached / "OBSERVATION"))
        for name, value in original.items():
            for row in (0, 1):
                np.testing.assert_array_equal(table.getcell(name, row), value, strict=True)
        assert table.done()
        assert table.open(str(donor / "HISTORY"))
        assert table.getcell("ORIGIN", table.nrows() - 1) == "ms::testconcatenate()"
        assert table.done()


def make_optional_calibration_tables(path):
    api = pinned_casa()
    table = api.table()
    for name in ("GAIN_CURVE", "PHASE_CAL", "EARTH_ORIENTATION"):
        destination = path / name
        copytree(path / "DATA_DESCRIPTION", destination)
        assert table.open(str(destination), nomodify=False)
        column = "OBSERVATION_ID" if name == "EARTH_ORIENTATION" else "ANTENNA_ID"
        assert table.addcols({column: {"valueType": "int"}})
        assert table.putcell(column, 0, 0)
        assert table.done()
    assert table.open(str(path), nomodify=False)
    for name in ("GAIN_CURVE", "PHASE_CAL", "EARTH_ORIENTATION"):
        assert table.putkeyword(name, f"Table: {path / name}")
    assert table.done()


def test_calibration_rows_follow_observation_antenna_and_window_mappings(
    native_dataset, tmp_path, concat_api,
):
    attached, donor = copy_inputs(native_dataset, tmp_path)
    for path in (attached, donor):
        make_optional_calibration_tables(path)
    table = tools.table()
    for subtable, column, value in (
        ("OBSERVATION", "PROJECT", "donor-project"),
        ("ANTENNA", "NAME", "donor-antenna"),
    ):
        assert table.open(str(donor / subtable), nomodify=False)
        assert table.putcell(column, 0, value)
        if subtable == "ANTENNA":
            assert table.putcell("POSITION", 0, table.getcell("POSITION", 0) + 50.0)
        assert table.done()
    assert table.open(str(donor / "SPECTRAL_WINDOW"), nomodify=False)
    assert table.putcell("CHAN_FREQ", 0, table.getcell("CHAN_FREQ", 0) + 20.0)
    assert table.done()

    tool = concat_api.ms()
    assert tool.open(str(attached), nomodify=False)
    assert tool.testconcatenate(str(donor), "1Hz", "1mas")
    assert tool.done()
    for api in (tools, pinned_casa()):
        table = api.table()
        for name, expectations in (
            ("GAIN_CURVE", {"ANTENNA_ID": [0, 4], "SPECTRAL_WINDOW_ID": [0, 1]}),
            ("PHASE_CAL", {"ANTENNA_ID": [0, 4], "SPECTRAL_WINDOW_ID": [0, 1]}),
            ("EARTH_ORIENTATION", {"OBSERVATION_ID": [0, 1]}),
        ):
            assert table.open(str(attached / name))
            for column, expected in expectations.items():
                np.testing.assert_array_equal(table.getcol(column), expected)
            assert table.done()


def test_matched_field_removes_unreferenced_donor_source(native_dataset, tmp_path, concat_api):
    attached, donor = copy_inputs(native_dataset, tmp_path)
    table = tools.table()
    assert table.open(str(donor / "SOURCE"), nomodify=False)
    assert table.putcell("NAME", 0, "different source metadata")
    assert table.done()
    tool = concat_api.ms()
    assert tool.open(str(attached), nomodify=False)
    assert tool.testconcatenate(str(donor), "1Hz", "1mas")
    assert tool.done()
    for reader in (tools, pinned_casa()):
        assert counts(reader, attached, ("FIELD", "SOURCE")) == [1, 1]
        table = reader.table()
        assert table.open(str(attached / "SOURCE"))
        assert table.getcell("NAME", 0) == "F"
        assert table.getcell("SOURCE_ID", 0) == 0
        assert table.done()


def test_self_concatenation_refused_before_appending_rows(native_dataset, tmp_path):
    alias = tmp_path / "alias.ms"
    alias.symlink_to(native_dataset.name, target_is_directory=True)
    names = ("OBSERVATION", "SOURCE", "FIELD", "HISTORY")
    before = counts(tools, native_dataset, names)
    tool = tools.ms()
    assert tool.open(str(native_dataset), nomodify=False)
    nrows = tool.nrow()
    for donor in (native_dataset, alias):
        with pytest.raises(RuntimeError, match="separate input MeasurementSets"):
            tool.testconcatenate(str(donor), "1Hz", "1mas")
        assert tool.nrow() == nrows
    assert tool.done()
    for api in (tools, pinned_casa()):
        assert counts(api, native_dataset, names) == before


def test_history_uses_the_original_attached_and_donor_names(native_dataset, tmp_path, concat_api):
    attached, donor = copy_inputs(native_dataset, tmp_path)
    a = tmp_path / "attached-alias.ms"
    b = tmp_path / "donor-alias.ms"
    a.symlink_to(attached.name, target_is_directory=True)
    b.symlink_to(donor.name, target_is_directory=True)
    tool = concat_api.ms()
    assert tool.open(str(a), nomodify=False)
    assert tool.testconcatenate(str(b), "1Hz", "1mas")
    assert tool.done()
    for api in (tools, pinned_casa()):
        tab = api.table()
        assert tab.open(str(donor / "HISTORY"))
        last = tab.nrows() - 1
        assert tab.getcell("MESSAGE", last) == f"Subtables from {b} appended to those from {a}"
        assert tab.done()


def test_fixed_visibility_shape_rejected_before_subtable_changes(native_dataset, tmp_path, concat_api):
    attached, donor = copy_inputs(native_dataset, tmp_path)
    target = tmp_path / "fixed.ms"
    tab = pinned_casa().table()
    assert tab.open(str(attached))
    desc, info = tab.getdesc(), tab.info()
    assert tab.close()
    desc["DATA"].update(option=4, shape=np.array([2, 5], dtype=np.int32))
    desc["_define_hypercolumn_"]["FixedData"] = {
        "HCndim": 3,
        "HCdatanames": np.array(["DATA"]),
        "HCcoordnames": np.array([], dtype="U5"),
        "HCidnames": np.array([], dtype="U5"),
    }
    assert tab.create(str(target), desc, nrow=0)
    assert tab.putinfo(info)
    assert tab.done()

    names = ("FIELD", "SOURCE", "SPECTRAL_WINDOW", "HISTORY")
    before = [counts(tools, path, names) for path in (attached, donor)]
    tool = concat_api.ms()
    assert tool.open(str(target), nomodify=False)
    assert tool.nrow() == 0
    with pytest.raises(RuntimeError, match="different number of channels"):
        tool.testconcatenate(str(donor), "1Hz", "1mas")
    assert tool.nrow() == 0
    assert tool.done()
    assert [counts(tools, path, names) for path in (attached, donor)] == before


def test_donor_ephemeris_is_transferred_and_relocatable(ephemeris_dataset, tmp_path, concat_api):
    dataset, _ = ephemeris_dataset
    attached, donor = copy_inputs(dataset, tmp_path)
    tab = tools.table()
    assert tab.open(str(attached / "FIELD"), nomodify=False)
    assert tab.removecols("EPHEMERIS_ID")
    assert tab.done()
    rmtree(attached / "FIELD" / "EPHEM0_TEST.tab")

    original = pinned_casa().ms()
    assert original.open(str(donor))
    expected = original.getfielddirmeas("EPHEMERIS_DIR", 0)
    assert original.done()
    tool = concat_api.ms()
    assert tool.open(str(attached), nomodify=False)
    assert tool.testconcatenate(str(donor), "1Hz", "1mas")
    assert tool.done()

    relocated = attached.rename(tmp_path / "relocated.ms")
    assert (relocated / "FIELD" / "EPHEM0_TEST.tab").is_dir()
    assert (donor / "FIELD" / "EPHEM0_TEST.tab").is_dir()
    for api in (tools, pinned_casa()):
        tab = api.table()
        assert tab.open(str(relocated / "FIELD"))
        np.testing.assert_array_equal(tab.getcol("EPHEMERIS_ID"), [-1, 0])
        assert tab.done()
        tool = api.ms()
        assert tool.open(str(relocated))
        actual = tool.getfielddirmeas("EPHEMERIS_DIR", 1)
        assert tool.done()
        assert actual["refer"] == expected["refer"] == "ICRS"
        for axis in ("m0", "m1"):
            assert actual[axis]["unit"] == expected[axis]["unit"] == "rad"
            assert actual[axis]["value"] == pytest.approx(expected[axis]["value"], abs=2e-11)


def test_missing_donor_history_preserves_attached_metadata(native_dataset, tmp_path):
    attached, donor = copy_inputs(native_dataset, tmp_path)
    names = ("FIELD", "SOURCE", "SPECTRAL_WINDOW", "POINTING")
    before = counts(tools, attached, names)
    table = tools.table()
    assert table.open(str(donor), nomodify=False)
    assert table.removekeyword("HISTORY")
    assert table.done()
    rmtree(donor / "HISTORY")

    tool = tools.ms()
    assert tool.open(str(attached), nomodify=False)
    nrows = tool.nrow()
    with pytest.raises(RuntimeError, match="HISTORY"):
        tool.testconcatenate(str(donor))
    assert tool.nrow() == nrows
    assert tool.done()
    for reader in (tools, pinned_casa()):
        assert counts(reader, attached, names) == before
