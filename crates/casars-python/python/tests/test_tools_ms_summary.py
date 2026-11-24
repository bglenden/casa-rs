"""The public summary record, interval edges and report output."""

import logging
import re

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


def compare_summary(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected:
            compare_summary(actual[key], expected[key])
    elif isinstance(expected, np.ndarray):
        assert actual.dtype == expected.dtype
        np.testing.assert_array_equal(actual, expected)
    elif isinstance(expected, float):
        assert type(actual) is float
        assert actual == pytest.approx(expected, abs=1e-10, rel=1e-13)
    else:
        assert type(actual) is type(expected)
        assert actual == expected


def antenna_geometry(report):
    _, rows = report.split("ITRF Geocentric coordinates", 1)
    result = {}
    for line in rows.splitlines():
        parts = line.split()
        if len(parts) == 13 and parts[0].isdigit():
            result[int(parts[0])] = [float(value) for value in parts[7:13]]
    assert result
    return result


@pytest.mark.parametrize("selected", [False, True])
def test_ms_summary_matches_pinned_record_and_reports(native_dataset, tmp_path, caplog, selected):
    casa = pinned_casa()
    outputs = []
    offsets = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.summary() == {}
        assert ms.open(str(native_dataset))
        if selected:
            assert ms.select({"rows": [3, 1, 0, 3]})
        filename = tmp_path / f"{api.__name__}.txt"
        with caplog.at_level(logging.INFO, logger="casars.tools.ms"):
            record = ms.summary(verbose=True, listunfl=True, cachesize=np.float32(12),
                                listfile=bytes(filename))
        assert record["numrecords"] == (3 if selected else ms.nrow())
        assert record["timeref"] == "UTC"
        assert record["nfields"] == 1
        assert record["IntegrationTime"] > 0
        report = filename.read_text()
        assert "MeasurementSet Name:" in report
        assert str(native_dataset) in report
        assert "nUnflRows" in report
        assert "ScanIntent" in report
        offsets.append(antenna_geometry(report))
        with pytest.raises(RuntimeError, match="already exists"):
            ms.summary(listfile=str(filename))
        assert filename.read_text() == report
        assert ms.summary(listfile=str(filename), overwrite=True, wantreturn=False) == {}
        assert "ScanIntent" not in filename.read_text()
        assert ms.summary(verbose=False, wantreturn=False) == {}
        assert ms.done()
        outputs.append(record)
    compare_summary(outputs[1], outputs[0])
    assert offsets[1] == offsets[0]


def test_ms_summary_rejects_invalid_argument_forms():
    ms = tools.ms()
    for keyword in ("verbose", "listunfl", "overwrite", "wantreturn"):
        with pytest.raises(TypeError):
            ms.summary(**{keyword: 1})
    with pytest.raises(TypeError, match="argument _listfile"):
        ms.summary(listfile=None)
    with pytest.raises(TypeError):
        ms.summary(cachesize="50")


def test_ms_summary_groups_interleaved_fields_within_one_scan(native_dataset, tmp_path):
    casa = pinned_casa()
    name = "Second field has more than twenty characters"
    marked = name[:19] + "*" + name[20:]
    table = casa.table()
    assert table.open(str(native_dataset / "FIELD"), nomodify=False)
    first_field = {
        column: table.getcell(column, 0)
        for column in table.colnames()
        if table.iscelldefined(column, 0)
    }
    assert table.addrows(1)
    first_field["NAME"] = name
    for column, value in first_field.items():
        assert table.putcell(column, 1, value)
    assert table.done()
    assert table.open(str(native_dataset), nomodify=False)
    fields = table.getcol("FIELD_ID")
    assert len(fields) >= 4
    fields[1::2] = 1
    assert table.putcol("FIELD_ID", fields)
    assert table.done()

    for verbose in (False, True):
        records = []
        for api in (casa, tools):
            ms = api.ms()
            assert ms.open(str(native_dataset))
            output = tmp_path / f"{api.__name__}-{verbose}.txt"
            record = ms.summary(verbose=verbose, listfile=str(output))
            assert record["nfields"] == 2
            assert record["field_1"]["name"] == marked
            scans = [scan for key, value in record.items() if key.startswith("scan_")
                     for scan in value.values()]
            assert len(scans) == 2
            assert [scan["FieldId"] for scan in scans] == [0, 1]
            assert scans[1]["FieldName"] == (marked if verbose else name)
            assert sum(scan["nRow"] for scan in scans) == len(fields)
            assert ms.done()
            records.append(record)
        compare_summary(records[1], records[0])


def test_ms_summary_reports_a_failed_file_write(native_dataset, tmp_path):
    rejected = tmp_path / "absent" / "summary.txt"
    ms = tools.ms()
    assert ms.open(str(native_dataset))
    count = ms.nrow()
    with pytest.raises(RuntimeError, match="cannot write"):
        ms.summary(listfile=str(rejected))
    assert not rejected.exists()
    assert ms.summary()["numrecords"] == count
    assert ms.done()


def append_subtable_row(casa, path, updates):
    table = casa.table()
    assert table.open(str(path), nomodify=False)
    index = table.nrows()
    row = {
        name: table.getcell(name, 0)
        for name in table.colnames()
        if table.iscelldefined(name, 0)
    }
    row.update(updates)
    assert table.addrows(1)
    for name, value in row.items():
        assert table.putcell(name, index, value)
    assert table.done()


def test_ms_summary_reports_physical_metadata_for_selected_rows(native_dataset, tmp_path):
    casa = pinned_casa()
    table = casa.table()
    append_subtable_row(casa, native_dataset / "OBSERVATION", {
        "TELESCOPE_NAME": "ALMA",
        "OBSERVER": "Other observer",
        "PROJECT": "Other project",
    })
    append_subtable_row(casa, native_dataset / "POLARIZATION", {})
    append_subtable_row(casa, native_dataset / "DATA_DESCRIPTION", {"POLARIZATION_ID": 1})

    assert table.open(str(native_dataset / "ANTENNA"), nomodify=False)
    metres = table.getcol("POSITION")
    assert table.putcol("POSITION", metres * np.array([[1e-3], [1.0], [1e2]]))
    assert table.putcolkeyword("POSITION", "QuantumUnits", ["km", "m", "cm"])
    assert table.done()
    assert table.open(str(native_dataset / "FIELD"), nomodify=False)
    degrees = np.rad2deg(table.getcell("PHASE_DIR", 0))
    assert table.putcell("PHASE_DIR", 0, degrees)
    assert table.putcolkeyword("PHASE_DIR", "QuantumUnits", ["deg", "deg"])
    assert table.done()
    assert table.open(str(native_dataset), nomodify=False)
    selected = list(range(4))
    for column, values in {
        "OBSERVATION_ID": np.isin(np.arange(table.nrows()), selected).astype(np.int32),
        "DATA_DESC_ID": (~np.isin(np.arange(table.nrows()), selected)).astype(np.int32),
        "EXPOSURE": np.full(table.nrows(), 4.0),
        "INTERVAL": np.full(table.nrows(), 10.0),
        "FLAG_ROW": np.full(table.nrows(), True),
    }.items():
        assert table.putcol(column, values)
    assert table.putcol("FLAG", np.zeros_like(table.getcol("FLAG")))
    assert table.done()

    offsets = []
    records = []
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(native_dataset))
        assert ms.select({"rows": selected})
        path = tmp_path / f"{api.__name__}-physical.txt"
        records.append(ms.summary(verbose=True, listunfl=True, listfile=str(path)))
        report = path.read_text()
        assert "Observation: VLA" in report
        assert "Other observer" in report and "Other project" in report
        assert re.search(r"ObservationID\s*=\s*1", report)
        assert re.search(r"F\s+4\s+4(?:\.00)?\s+\[0\]\s+\[10\]", report)
        assert re.search(r"23:30:00\.000000\s+\+34\.00\.00\.00000\s+J2000", report)
        assert "1 unique polarization setups" in report
        scan, = [part for key, value in records[-1].items() if key.startswith("scan_")
                 for part in value.values()]
        assert scan["IntegrationTime"] == 4.0
        offsets.append(antenna_geometry(report))
        assert ms.done()
    compare_summary(records[1], records[0])
    assert offsets[1] == offsets[0]


def test_ms_summary_validates_telescope_only_for_verbose_antenna_report(native_dataset):
    casa = pinned_casa()
    append_subtable_row(casa, native_dataset / "OBSERVATION", {
        "TELESCOPE_NAME": "unrecognized summary test telescope"
    })
    for api in (casa, tools):
        ms = api.ms()
        assert ms.open(str(native_dataset))
        assert ms.summary()["numrecords"] == ms.nrow()
        with pytest.raises(RuntimeError, match="is not recognized by CASA"):
            ms.summary(verbose=True)
        assert ms.summary()["numrecords"] == ms.nrow()
        assert ms.done()
