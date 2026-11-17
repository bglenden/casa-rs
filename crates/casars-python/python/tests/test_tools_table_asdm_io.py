# SPDX-License-Identifier: LGPL-3.0-or-later
"""The legacy XML table converter preserves ASDM values and schema."""
import numpy as np
import pytest

from casars.tools import table


def test_fromasdm_scan_xml(tmp_path):
    xml = tmp_path / "Scan.xml"
    xml.write_text(
        '<?xml version="1.0"?>\n'
        '<ScanTable xmlns="urn:alma:asdm:ScanTable">'
        '<Entity entityId="uid://abc" /><row>'
        '<execBlockId>ExecBlock_1</execBlockId><scanNumber>12</scanNumber>'
        '<startTime>10000000261</startTime><scanIntent>1 2 OBSERVE_TARGET CALIBRATE_PHASE</scanIntent>'
        '<flagRow>true</flagRow><sourceName>Alpha &amp; Beta</sourceName>'
        '<fieldName>1 2 "target one" "target two"</fieldName>'
        '</row><row><scanNumber>14</scanNumber></row></ScanTable>'
    )
    tb = table()
    destination = tmp_path / "Scan.table"
    assert tb.fromASDM(str(destination), str(xml))
    assert tb.name() == ""
    assert tb.open(str(destination))
    assert tb.colnames() == [
        "execBlockId", "scanNumber", "startTime", "endTime", "numSubScan",
        "numIntent", "scanIntent", "flagRow", "numField", "sourceName", "fieldName",
    ]
    np.testing.assert_array_equal(tb.getcol("scanNumber"), [12, 14])
    assert tb.getcell("startTime", 0) == "10000000261"
    assert tb.getcell("sourceName", 0) == "Alpha & Beta"
    assert tb.getcell("flagRow", 0)
    assert list(tb.getcell("scanIntent", 0)) == ["OBSERVE_TARGET", "CALIBRATE_PHASE"]
    assert list(tb.getcell("fieldName", 0)) == ["target one", "target two"]
    assert not tb.iscelldefined("fieldName", 1)


def test_fromasdm_manifest_and_parse_failure(tmp_path):
    xml = tmp_path / "ASDM.xml"
    xml.write_text("<ASDM><Table><Name>Scan</Name><NumberRows>30</NumberRows></Table></ASDM>")
    tb = table()
    target = tmp_path / "manifest.table"
    assert tb.fromASDM(str(target), str(xml))
    assert tb.open(str(target))
    assert tb.getcell("Name", 0) == "Scan"
    assert tb.getcell("NumberRows", 0) == 30
    xml.write_text("<ASDM><Table><Name>Bad")
    with pytest.raises(RuntimeError):
        tb.fromASDM(str(target), str(xml))
    assert tb.getcell("Name", 0) == "Scan"
