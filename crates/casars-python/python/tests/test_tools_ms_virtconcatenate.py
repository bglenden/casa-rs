"""CASA virtual concatenation, donor reindexing, and auxiliary scan files."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms_concatenate import change_table
from test_tools_ms_createmultims import pinned_casa
from test_tools_ms_testconcatenate import concat_api, copy_inputs, counts
from test_tools_msmetadata import measures, native_dataset


def seed_ids(first, donor):
    change_table(donor / "OBSERVATION", lambda table: table.putcell(
        "PROJECT", 0, "other science project",
    ))
    change_table(donor / "FIELD", lambda table: table.putcell(
        "NAME", 0, "another field",
    ))
    for path, scan in ((first, 11), (donor, 1)):
        change_table(path, lambda table: table.putcol(
            "SCAN_NUMBER", np.full(table.nrows(), scan, dtype=np.int32),
        ))


@pytest.mark.parametrize("mode", ["omitted", "new", "existing", "linked", "absent_parent"])
def test_donor_row_ids_and_scan_file(native_dataset, tmp_path, concat_api, mode):
    first, donor = copy_inputs(native_dataset, tmp_path)
    seed_ids(first, donor)
    scan_file = tmp_path / "scan.txt"
    options = {"freqtol": "1Hz", "dirtol": "1mas"}
    if mode in ("existing", "linked"):
        referent = tmp_path / ("referent.txt" if mode == "linked" else "scan.txt")
        referent.write_text("1\n0\n6\n15\n15\n")
        if mode == "linked":
            scan_file.symlink_to(referent.name)
    if mode != "omitted":
        if mode == "absent_parent":
            scan_file = tmp_path / "no-parent" / scan_file.name
        options["auxfilename"] = str(scan_file)

    m = concat_api.ms()
    assert m.virtconcatenate(str(tmp_path / "absent.ms")) is True
    assert m.open(str(first), nomodify=False)
    original = m.nrow()
    assert m.virtconcatenate(str(donor), **options) is True
    assert m.nrow() == original
    assert m.range(["fields"])["fields"].tolist() == ["F", "another field"]
    assert m.close()

    if mode in ("omitted", "absent_parent"):
        assert not scan_file.exists()
    elif mode == "new":
        assert scan_file.read_text() == "2\n0\n11\n11\n1\n12\n12\n12\n"
    else:
        assert scan_file.read_text() == "2\n0\n6\n15\n1\n16\n16\n16\n"
        assert scan_file.is_symlink() is (mode == "linked")

    expected_observation = 0 if mode == "omitted" else 1
    expected_scan = {"omitted": 1, "new": 12, "existing": 16, "linked": 16, "absent_parent": 12}[mode]
    for reader in (tools, pinned_casa()):
        assert counts(reader, first, (".", "OBSERVATION", "FIELD", "HISTORY")) == [original, 2, 2, 0]
        assert counts(reader, donor, (".", "OBSERVATION", "FIELD", "HISTORY")) == [original, 1, 1, 1]
        table = reader.table()
        assert table.open(str(donor))
        for column, expected in (("FIELD_ID", 1), ("OBSERVATION_ID", expected_observation),
                                 ("SCAN_NUMBER", expected_scan)):
            values = table.getcol(column)
            np.testing.assert_array_equal(values, np.full(original, expected, dtype=np.int32))
            assert table.getcoldesc(column)["valueType"] == "int"
            assert values.dtype.kind == "i"
        assert table.close()
        assert table.open(str(donor / "HISTORY"))
        assert table.getcell("ORIGIN", 0) == "ms::virtconcatenate()"
        assert table.getcell("MESSAGE", 0) == f"{donor} virtually concatenated with {first}"
        assert list(table.getcell("CLI_COMMAND", 0)) == [
            f"msfile='{donor}' freqTol='1 Hz' dirTol='1 mas' respectname='1'"
        ]
        assert table.close()


def test_sigma_spectrum_uses_its_own_cells(native_dataset, tmp_path, concat_api):
    first, donor = copy_inputs(native_dataset, tmp_path)
    casa = pinned_casa()

    def spectra(table):
        for name, value in (("WEIGHT_SPECTRUM", 4.0), ("SIGMA_SPECTRUM", 2.0)):
            if name not in table.colnames():
                schema = table.getcoldesc("DATA")
                schema.update(valueType="float", dataManagerType="StandardStMan", dataManagerGroup=name)
                assert table.addcols({name: schema})
            for row in range(table.nrows()):
                assert table.putcell(name, row, np.full((2, 4), value, dtype=np.float32))

    change_table(donor, spectra)
    m = concat_api.ms()
    assert m.open(str(first), nomodify=False)
    assert m.virtconcatenate(str(donor), "", "1Hz", "1mas", 4.0)
    assert m.close()
    for reader in (tools, casa):
        table = reader.table()
        assert table.open(str(donor))
        np.testing.assert_array_equal(table.getcell("WEIGHT_SPECTRUM", 0), np.full((2, 4), 16))
        expected = 8 if concat_api is casa else 1
        np.testing.assert_array_equal(table.getcell("SIGMA_SPECTRUM", 0), np.full((2, 4), expected))
        assert table.close()


def test_malformed_scan_file_preserves_both_measurement_sets(native_dataset, tmp_path):
    first, donor = copy_inputs(native_dataset, tmp_path)
    seed_ids(first, donor)
    scan_file = tmp_path / "scan.txt"
    incomplete = "2\n0\n6\n15\n"
    scan_file.write_text(incomplete)
    m = tools.ms()
    assert m.open(str(first), nomodify=False)
    with pytest.raises(RuntimeError, match="invalid virtual concatenation scan file"):
        m.virtconcatenate(str(donor), str(scan_file))
    assert m.close()
    assert scan_file.read_text() == incomplete
    for reader in (tools, pinned_casa()):
        assert counts(reader, first, ("OBSERVATION", "FIELD", "HISTORY")) == [1, 1, 0]
        assert counts(reader, donor, ("OBSERVATION", "FIELD", "HISTORY")) == [1, 1, 0]
        table = reader.table()
        assert table.open(str(donor))
        np.testing.assert_array_equal(table.getcol("FIELD_ID"), np.zeros(table.nrows(), dtype=np.int32))
        assert table.close()


def test_new_scan_file_uses_the_selected_view(native_dataset, tmp_path, concat_api):
    first, donor = copy_inputs(native_dataset, tmp_path)
    seed_ids(first, donor)
    def first_two_scans(table):
        for row in range(2):
            assert table.putcell("SCAN_NUMBER", row, 2)
    change_table(first, first_two_scans)
    output = tmp_path / "scan.txt"
    m = concat_api.ms()
    assert m.open(str(first), nomodify=False)
    assert m.msselect({"taql": "ROWID() < 2"})
    assert m.virtconcatenate(str(donor), str(output), "1Hz", "1mas")
    assert m.nrow(selected=True) == 2
    assert m.close()
    assert output.read_text() == "2\n0\n2\n2\n1\n3\n3\n3\n"
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(donor))
        np.testing.assert_array_equal(table.getcol("SCAN_NUMBER"), np.full(table.nrows(), 3))
        assert table.close()
