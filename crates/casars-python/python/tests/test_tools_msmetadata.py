"""Attachment and direct table queries through the public metadata tool."""

import os

import numpy as np
import pytest

from casars import tools
from test_tools_simulator import configure, needs_measures
from test_tools_simulator import measures as measures


def test_msmetadata_detached_queries():
    metadata = tools.msmetadata()
    assert metadata.close() is True
    assert metadata.done() is True
    with pytest.raises(RuntimeError, match="Tool is not attached to an MS"):
        metadata.name()
    with pytest.raises(RuntimeError, match="Tool is not attached to an MS"):
        metadata.nantennas()


@pytest.fixture
def native_dataset(tmp_path, measures):
    dataset = tmp_path / "metadata.ms"
    simulator = tools.simulator()
    assert simulator.open(str(dataset))
    configure(simulator)
    assert simulator.observe("F", "L", starttime="0s", stoptime="30s")
    assert simulator.close()
    return dataset


@needs_measures
def test_msmetadata_reads_native_dataset_and_preserves_attachment_on_open_error(
    native_dataset, tmp_path, monkeypatch
):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    dataset = native_dataset
    alias = tmp_path / "alias.ms"
    alias.symlink_to(dataset.name, target_is_directory=True)
    results = []
    for module in (casatools, tools):
        metadata = module.msmetadata()
        try:
            assert metadata.open(os.path.relpath(dataset), maxcache=0.0)
            original_name = metadata.name()
            assert original_name == str(dataset)
            assert metadata.fieldnames() == ["F"]
            assert metadata.nantennas() == 4
            assert metadata.nfields() == 1
            with pytest.raises(RuntimeError):
                metadata.open(str(tmp_path / "missing.ms"))
            assert metadata.name() == original_name
            assert metadata.open(str(alias))
            assert metadata.name() == str(alias)
            with monkeypatch.context() as patch:
                patch.chdir(dataset)
                assert metadata.open()
                assert metadata.name() == str(dataset)
            results.append((
                metadata.nobservations(), metadata.nstates(), metadata.observatorynames(),
                metadata.projects(), metadata.observers(),
            ))
        finally:
            assert metadata.done()
    assert results[0] == results[1]


def test_msmetadata_spectral_queries_and_data_description_dtypes(native_dataset):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    results = []
    for module in (casatools, tools):
        metadata = module.msmetadata()
        try:
            assert metadata.open(str(native_dataset))
            assert metadata.nchan(0) == 4
            with pytest.raises(RuntimeError):
                metadata.nchan(-1)
            with pytest.raises(RuntimeError):
                metadata.chanwidths(0, "km/s")
            with pytest.raises(RuntimeError):
                metadata.datadescids(spw=100)
            assert metadata.spwfordatadesc(0) == 0
            assert metadata.polidfordatadesc(0) == 0
            assert metadata.ncorrforpol(0) == 2
            results.append((
                metadata.sideband(0), metadata.reffreq(0),
                metadata.meanfreq(0, "MHz"), metadata.chanfreqs(0, "MHz"),
                metadata.chanfreqs(0, "cm"), metadata.chanwidths(0, "MHz"),
                metadata.chanwidths(0, "m"),
                metadata.datadescids(), metadata.spwfordatadesc(), metadata.polidfordatadesc(),
                metadata.ncorrforpol(), metadata.corrtypesforpol(0), metadata.corrprodsforpol(0),
            ))
        finally:
            assert metadata.done()
    assert results[0][:2] == results[1][:2]
    for expected, actual in zip(results[0][2:], results[1][2:]):
        if isinstance(expected, np.ndarray):
            assert actual.dtype == expected.dtype
            assert actual.shape == expected.shape
        np.testing.assert_allclose(actual, expected, rtol=1e-13, atol=0)


def test_msmetadata_water_vapor_windows(native_dataset):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    table = tools.table()
    assert table.open(str(native_dataset / "SPECTRAL_WINDOW"), nomodify=False)
    assert table.putcell("NAME", 0, "WVR#NOMINAL")
    assert table.done()
    for module in (casatools, tools):
        metadata = module.msmetadata()
        try:
            assert metadata.open(str(native_dataset))
            assert metadata.nspw() == 1
            assert metadata.nspw(includewvr=False) == 0
            np.testing.assert_array_equal(metadata.wvrspws(), np.array([0], dtype=np.int64), strict=True)
            assert len(metadata.wvrspws(complement=True)) == 0
        finally:
            assert metadata.done()


def test_msmetadata_classification_and_channel_velocity(native_dataset):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    table = tools.table()
    assert table.open(str(native_dataset / "SPECTRAL_WINDOW"), nomodify=False)
    assert table.addcols({"BBC_NO": {"valueType": "int"}, "SDM_CORR_BIT": {"valueType": "string"}})
    assert table.putcell("BBC_NO", 0, 4)
    assert table.putcell("SDM_CORR_BIT", 0, "4x4")
    assert table.putcell("NAME", 0, "RB_09#BB_4#SQLD#SW-02")
    assert table.done()

    for module in (casatools, tools):
        metadata = module.msmetadata()
        try:
            assert metadata.open(str(native_dataset))
            assert metadata.baseband(0) == 4
            assert metadata.corrbit(0) == "4x4"
            assert metadata.spwsforbaseband(sqldmode="exclude")["4"].size == 0
            for result, values in (
                (metadata.almaspws(fdm=True, sqld=True), [0]),
                (metadata.almaspws(sqld=True, complement=True), []),
                (metadata.fdmspws(), [0]),
                (metadata.tdmspws(), []),
                (metadata.spwsforbaseband(4, "Only"), [0]),
                (metadata.rxbands([0, 0]), [9, 9]),
                (metadata.subwindows(np.array(0)), [2]),
            ):
                np.testing.assert_array_equal(result, np.array(values, dtype=np.int64), strict=True)
            np.testing.assert_array_equal(metadata.corrbit(), np.array(["4x4"]), strict=True)
            np.testing.assert_array_equal(metadata.corrbit(np.array([], dtype=np.int64)),
                                          np.array([], dtype="U5"), strict=True)
            with pytest.raises(RuntimeError):
                metadata.corrbit(None)
            with pytest.raises(RuntimeError):
                metadata.subwindows(-1)
            with pytest.raises(RuntimeError):
                metadata.spwsforbaseband(sqldmode="bad")
            for query in (metadata.chaneffbws, metadata.chanres):
                widths_hz = query(0)
                for unit, scale in (("", 1e-3), ("m/s", 1)):
                    velocity = query(0, unit, asvel=True)
                    assert velocity.dtype == widths_hz.dtype == np.float64
                    expected = 299_792_458 * scale * widths_hz / metadata.meanfreq(0)
                    np.testing.assert_allclose(velocity, expected, rtol=1e-14, atol=0)
                with pytest.raises(RuntimeError):
                    query(0, "Hz", asvel=True)
        finally:
            assert metadata.done()


def test_msmetadata_name_queries_preserve_selector_order_and_array_types(native_dataset):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    results = []
    for module in (casatools, tools):
        metadata = module.msmetadata()
        try:
            assert metadata.open(str(native_dataset))
            assert metadata.antennanames() == ["A1", "A2", "A3", "A4"]
            assert metadata.antennanames(np.array([[0, 1], [2, 3]])) == ["A1", "A3", "A2", "A4"]
            assert metadata.antennanames([[0, 1], [2, 3]]) == ["A1", "A3", "A2", "A4"]
            assert metadata.antennanames([True, 0]) == ["A2", "A1"]
            assert metadata.antennanames([False]) == ["A1", "A2", "A3", "A4"]
            assert metadata.namesforfields("") == ["F"]
            assert metadata.namesforfields(np.array([0, 0], dtype=np.uint64)) == ["F", "F"]
            assert metadata.namesforspws(np.array([[0, 0]])) == ["L", "L"]
            with pytest.raises(RuntimeError):
                metadata.namesforfields([False])
            with pytest.raises(RuntimeError):
                metadata.antennanames([0.0])
            with pytest.raises(RuntimeError):
                metadata.bandwidths([0, -1])
            assert type(metadata.bandwidths(0)) is float
            results.append((
                metadata.sourceidforfield(0), metadata.sourcenames(), metadata.nsources(),
                metadata.bandwidths(), metadata.bandwidths([0]), metadata.sourceidsfromsourcetable(),
            ))
        finally:
            assert metadata.done()
    assert results[0][:3] == results[1][:3]
    for expected, actual in zip(results[0][3:], results[1][3:]):
        np.testing.assert_array_equal(actual, expected, strict=True)


def test_msmetadata_empty_integer_arrays_select_all_without_aborting(native_dataset):
    metadata = tools.msmetadata()
    assert metadata.open(str(native_dataset))
    try:
        empty = np.array([], dtype=np.int64)
        assert metadata.namesforfields(empty) == metadata.namesforfields()
        assert metadata.namesforspws(empty) == metadata.namesforspws()
        np.testing.assert_array_equal(metadata.bandwidths(empty), metadata.bandwidths(), strict=True)
        np.testing.assert_array_equal(metadata.rxbands(empty), metadata.rxbands(), strict=True)
        np.testing.assert_array_equal(metadata.subwindows(empty), metadata.subwindows(), strict=True)
    finally:
        assert metadata.done()


@pytest.mark.parametrize("maxcache", [0.0, 50.0])
def test_msmetadata_scan_membership_and_baseline_matrix(native_dataset, maxcache):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    results = []
    for module in (casatools, tools):
        metadata = module.msmetadata()
        try:
            assert metadata.open(str(native_dataset), maxcache)
            scans = metadata.scannumbers()
            assert metadata.nscans() == len(scans) == 1
            assert metadata.narrays() == 1
            scan = int(scans[0])
            np.testing.assert_array_equal(metadata.fieldsforname("f"), np.array([0]), strict=True)
            np.testing.assert_array_equal(metadata.scansforfield("f"), scans, strict=True)
            np.testing.assert_array_equal(metadata.scansforfields()["0"], scans, strict=True)
            np.testing.assert_array_equal(metadata.fieldsforscans(float(scan) + 0.9), np.array([0]), strict=True)
            np.testing.assert_array_equal(metadata.fieldsforscans([[scan, scan]], asnames=True),
                                          np.array(["F"]), strict=True)
            for selector in (np.array([f"{scan}x"]), np.array([scan + 2j]), [str(scan)]):
                np.testing.assert_array_equal(metadata.fieldsforscans(selector), np.array([0]), strict=True)
            np.testing.assert_array_equal(metadata.scansforfield(np.int32(0)), scans, strict=True)
            for field in (np.uint32(0), np.array(0)):
                with pytest.raises(RuntimeError):
                    metadata.scansforfield(field)
            np.testing.assert_array_equal(metadata.fieldsforscans([scan], obsid=0, arrayid=0, asmap=True)[str(scan)],
                                          np.array([0]), strict=True)
            with pytest.raises(RuntimeError):
                metadata.fieldsforscan(scan + 100, obsid=0, arrayid=0)
            with pytest.raises(RuntimeError):
                metadata.scansforfield("missing")
            matrix = metadata.baselines()
            assert matrix.dtype == np.bool_
            assert matrix.shape == (4, 4)
            assert metadata.nbaselines() == np.count_nonzero(np.triu(matrix, 1))
            assert metadata.nbaselines(ac=True) == np.count_nonzero(np.triu(matrix))
            results.append((scans, metadata.antennasforscan(scan), matrix))
        finally:
            assert metadata.done()
    for expected, actual in zip(*results):
        np.testing.assert_array_equal(actual, expected, strict=True)


def test_msmetadata_explicit_array_selection_and_cache_reopen(native_dataset):
    metadata = tools.msmetadata()
    assert metadata.open(str(native_dataset))
    try:
        scans = metadata.scannumbers()
        assert metadata.narrays() == 1
        np.testing.assert_array_equal(metadata.fieldsforscans(scans, obsid=0, arrayid=0),
                                      np.array([0]), strict=True)
        table = tools.table()
        assert table.open(str(native_dataset), nomodify=False)
        assert table.putcol("ARRAY_ID", np.full(table.nrows(), 2, dtype=np.int32))
        assert table.done()
        assert metadata.open(str(native_dataset))
        assert metadata.narrays() == 3
        np.testing.assert_array_equal(metadata.scannumbers(obsid=0, arrayid=2), scans, strict=True)
        np.testing.assert_array_equal(metadata.fieldsforscans(scans, obsid=0, arrayid=2),
                                      np.array([0]), strict=True)
        assert metadata.scannumbers(obsid=0, arrayid=0).size == 0
        with pytest.raises(RuntimeError):
            metadata.fieldsforscans(scans, obsid=0, arrayid=0)
    finally:
        assert metadata.done()


def test_msmetadata_empty_main_has_no_arrays_or_scans(tmp_path):
    dataset = str(tmp_path / "empty.ms")
    simulator = tools.simulator()
    assert simulator.open(dataset)
    assert simulator.close()
    metadata = tools.msmetadata()
    assert metadata.open(dataset)
    try:
        assert metadata.narrays() == metadata.nscans() == 0
        np.testing.assert_array_equal(metadata.scannumbers(), np.array([], dtype=np.int64), strict=True)
    finally:
        assert metadata.done()


def test_msmetadata_reads_selected_rows_from_casa_reference_ms(native_dataset, tmp_path):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    reference = tmp_path / "selected.ms"
    table = casatools.table()
    assert table.open(str(native_dataset))
    selected = table.query("ROWNUMBER() < 3", name=str(reference))
    assert 0 < selected.nrows() < table.nrows()
    selected.done()
    assert table.done()
    assert not (reference / "FIELD").exists()

    results = []
    for module in (casatools, tools):
        metadata = module.msmetadata()
        assert metadata.open(str(reference))
        try:
            scans = metadata.scannumbers()
            assert metadata.nscans() == scans.size == 1
            results.append((scans, metadata.fieldsforscans(scans),
                            metadata.antennasforscan(int(scans[0])), metadata.baselines()))
        finally:
            assert metadata.done()
    for expected, actual in zip(*results):
        np.testing.assert_array_equal(actual, expected, strict=True)


def test_msmetadata_rejects_unsupported_scan_dtypes_instead_of_selecting_all(native_dataset):
    metadata = tools.msmetadata()
    assert metadata.open(str(native_dataset))
    try:
        for dtype in (np.float16, object):
            with pytest.raises(TypeError, match="unsupported NumPy scan-selector dtype"):
                metadata.fieldsforscans(np.array([99], dtype=dtype))
    finally:
        assert metadata.done()


def test_msmetadata_scan_time_state_and_spectral_associations(native_dataset):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    results = []
    for module in (casatools, tools):
        metadata = module.msmetadata()
        assert metadata.open(str(native_dataset))
        try:
            scans = metadata.scannumbers()
            scan = int(scans[0])
            times = metadata.timesforscan(scan)
            np.testing.assert_array_equal(metadata.fieldsfortimes(times[0]), np.array([0]), strict=True)
            np.testing.assert_array_equal(metadata.scansfortimes(times[0]), scans, strict=True)
            np.testing.assert_array_equal(metadata.timesforscan(scan, perspw=True)["0"], times, strict=True)
            np.testing.assert_array_equal(metadata.timesforscans(scans), times, strict=True)
            np.testing.assert_array_equal(metadata.timesforspws(), times, strict=True)
            np.testing.assert_array_equal(metadata.timesforspws([0, 0])["0"], times, strict=True)
            np.testing.assert_array_equal(metadata.timesforfield(0), times, strict=True)
            np.testing.assert_array_equal(metadata.spwsforfields()["0"], np.array([0], dtype=np.uint64), strict=True)
            np.testing.assert_array_equal(metadata.spwsfornames("L")["L"], np.array([0], dtype=np.uint64), strict=True)
            np.testing.assert_array_equal(metadata.spwsforscan(scan), np.array([0]), strict=True)
            np.testing.assert_array_equal(metadata.spwsforscans()[str(scan)], np.array([0]), strict=True)
            np.testing.assert_array_equal(metadata.scansforspws()["0"], scans, strict=True)
            np.testing.assert_array_equal(metadata.scansforspw(0), scans, strict=True)
            assert set(metadata.spwsfornames(["L", "missing"])) == {"L"}
            assert metadata.statesforscan(scan + 100).size == 0
            np.testing.assert_array_equal(metadata.statesforscans()[str(scan)], metadata.statesforscan(scan), strict=True)
            with pytest.raises(RuntimeError):
                metadata.fieldsfortimes(times[0], -1)
            with pytest.raises(RuntimeError):
                metadata.scansforstate(-1)
            results.append((times, metadata.statesforscan(scan), metadata.scansforstate(0),
                            metadata.spwsforfield("F"), metadata.fieldsforspw(0, asnames=True)))
        finally:
            assert metadata.done()
    for expected, actual in zip(*results):
        np.testing.assert_array_equal(actual, expected, strict=True)


def test_msmetadata_safe_time_selection_boundaries(native_dataset):
    metadata = tools.msmetadata()
    assert metadata.open(str(native_dataset))
    try:
        scans = metadata.scannumbers()
        times = metadata.timesforscan(int(scans[0]))
        assert times[0] > 1.0
        np.testing.assert_array_equal(metadata.fieldsfortimes(0), np.array([], dtype=np.int64), strict=True)
        empty = np.array([], dtype=np.int64)
        np.testing.assert_array_equal(metadata.timesforscans(empty), times, strict=True)
        np.testing.assert_array_equal(metadata.timesforspws(empty), times, strict=True)
        np.testing.assert_array_equal(metadata.timesforscans(scans, obsid=0, arrayid=0), times, strict=True)
        with pytest.raises(RuntimeError):
            metadata.timesforspws(4096)
    finally:
        assert metadata.done()


def test_msmetadata_window_name_and_numpy_time_selector_forms(native_dataset):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    table = casatools.table()
    assert table.open(str(native_dataset / "SPECTRAL_WINDOW"), nomodify=False)
    assert table.nrows() == 1
    assert table.addrows(1)
    for column in table.colnames():
        if table.iscelldefined(column, 0):
            table.putcell(column, 1, table.getcell(column, 0))
    assert table.done()

    for module in (casatools, tools):
        metadata = module.msmetadata()
        assert metadata.open(str(native_dataset))
        try:
            for selector in (np.array("L"), np.array([["L", "missing"]]),
                             np.array([b"L", b"missing"]), [b"L"]):
                selected = metadata.spwsfornames(selector)
                assert list(selected) == ["L"]
                np.testing.assert_array_equal(selected["L"], np.array([0, 1]), strict=True)
            np.testing.assert_array_equal(metadata.spwsforfield(b"F"), np.array([0]), strict=True)
            all_times = metadata.timesforspws()
            assert set(all_times) == {"0", "1"}
            assert set(metadata.timesforspws(np.bool_(False))) == set(all_times)
            for spw in (np.int32(0), np.array([0], dtype=np.uint32)):
                np.testing.assert_array_equal(metadata.timesforspws(spw), all_times["0"], strict=True)
            scan = int(metadata.scannumbers()[0])
            for scans in (np.int32(scan), np.array([scan], dtype=np.uint32)):
                np.testing.assert_array_equal(metadata.timesforscans(scans), all_times["0"], strict=True)
            with pytest.raises(RuntimeError):
                metadata.timesforscans(np.uint32(scan))
            unsigned = metadata.timesforspws(np.uint32(0))
            if module is casatools:
                assert set(unsigned) == set(all_times)
            else:
                np.testing.assert_array_equal(unsigned, all_times["0"], strict=True)
        finally:
            assert metadata.done()


def test_msmetadata_intents_keep_the_actual_state_time_relationship(native_dataset):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    table = tools.table()
    assert table.open(str(native_dataset / "STATE"), nomodify=False)
    assert table.nrows() == 1
    assert table.addrows(2)
    for row, mode in enumerate(("OBSERVE+TARGET#ON_SOURCE,CALIBRATE_WVR#ON_SOURCE",
                                "CALIBRATE_PHASE#ON_SOURCE", "UNUSED")):
        assert table.putcell("OBS_MODE", row, mode)
    assert table.done()
    assert table.open(str(native_dataset), nomodify=False)
    times = table.getcol("TIME")
    assert len(np.unique(times)) > 1
    state = (times > times.min()).astype(np.int32)
    assert table.putcol("STATE_ID", state)
    assert table.done()

    for module in (casatools, tools):
        metadata = module.msmetadata()
        assert metadata.open(str(native_dataset))
        try:
            scans = metadata.scannumbers()
            expected = ["CALIBRATE_PHASE#ON_SOURCE", "CALIBRATE_WVR#ON_SOURCE",
                        "OBSERVE+TARGET#ON_SOURCE"]
            assert metadata.intents() == [*expected, "UNUSED"]
            assert metadata.intentsforfield("F") == expected
            assert metadata.intentsforfield("f") == []
            for field in (b"F", np.bytes_("F"), np.str_("F"), np.int32(0)):
                assert metadata.intentsforfield(field) == expected
            for invalid in (99, -1, np.uint32(0), False, None, [0]):
                with pytest.raises(RuntimeError):
                    metadata.intentsforfield(invalid)
            assert metadata.intentsforscan(int(scans[0])) == expected
            assert metadata.intentsforspw(0) == expected
            for pattern in ("OBSERVE*", b"OBSERVE*", np.str_("OBSERVE*"), np.bytes_("OBSERVE*")):
                np.testing.assert_array_equal(metadata.fieldsforintent(pattern, asnames=True),
                                              np.array(["F"]), strict=True)
            with pytest.raises(TypeError):
                metadata.fieldsforintent(None)
            literal = metadata.fieldsforintent("OBSERVE+*", asnames=True)
            expected_literal = np.array([], dtype="U5") if module is casatools else np.array(["F"])
            np.testing.assert_array_equal(literal, expected_literal, strict=True)
            np.testing.assert_array_equal(metadata.scansforintent("*WVR*"), scans, strict=True)
            np.testing.assert_array_equal(metadata.spwsforintent("CALIBRATE*"), np.array([0]), strict=True)
            np.testing.assert_array_equal(metadata.timesforintent("CALIBRATE_PHASE#ON_SOURCE"),
                                          np.unique(times[state == 1]), strict=True)
            np.testing.assert_array_equal(metadata.timesforintent("OBSERVE+TARGET#ON_SOURCE"),
                                          np.unique(times[state == 0]), strict=True)
            assert metadata.timesforintent("CALIBRATE*").size == 0
            assert metadata.scansforintent("UNUSED").size == 0
            with pytest.raises(RuntimeError):
                metadata.scansforintent("missing")
        finally:
            assert metadata.done()


def test_msmetadata_source_measures_use_source_units_and_optional_cells(native_dataset):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    source = tools.table()
    assert source.open(str(native_dataset / "SOURCE"), nomodify=False)
    assert source.nrows() == 1
    assert source.putcell("SPECTRAL_WINDOW_ID", 0, 0)
    assert source.putcell("TIME", 0, 4.8e9)
    assert source.putcell("PROPER_MOTION", 0, np.array([1.25e-11, -2.5e-11]))
    assert source.putcolkeyword("REST_FREQUENCY", "QuantumUnits", ["GHz"])
    assert source.putcell("REST_FREQUENCY", 0, np.array([1.25, 2.5]))
    assert source.putcell("TRANSITION", 0, ["A", "LINE B"])
    assert source.done()

    results = []
    for module in (casatools, tools):
        metadata = module.msmetadata()
        assert metadata.open(str(native_dataset))
        try:
            assert metadata.sourcetimes()["0"] == {"value": 4.8e9, "unit": "s"}
            assert metadata.propermotions()["0"] == {
                "longitude": {"value": 1.25e-11, "unit": "rad/s"},
                "latitude": {"value": -2.5e-11, "unit": "rad/s"},
            }
            frequencies = metadata.restfreqs()
            assert frequencies["0"] == {
                "type": "frequency", "refer": "LSRK", "m0": {"value": 1.25e9, "unit": "Hz"},
            }
            assert frequencies["1"]["m0"] == {"value": 2.5e9, "unit": "Hz"}
            np.testing.assert_array_equal(metadata.transitions(), np.array(["A", "LINE B"]), strict=True)
            np.testing.assert_array_equal(metadata.fieldsforsources()["0"], np.array([0]), strict=True)
            np.testing.assert_array_equal(metadata.fieldsforsource(0, True), np.array(["F"]), strict=True)
            assert metadata.fieldsforsource(999, True).shape == (0,)
            results.append(metadata.sourcedirs()["0"])
            for bad_source, bad_spw in [(-1, 0), (0, -1), (999, 0), (0, 999)]:
                with pytest.raises(RuntimeError):
                    metadata.restfreqs(bad_source, bad_spw)
                with pytest.raises(RuntimeError):
                    metadata.transitions(bad_source, bad_spw)
        finally:
            assert metadata.done()

    reference, native = results
    assert (native["type"], native["refer"]) == (reference["type"], reference["refer"])
    for axis in ("m0", "m1"):
        assert native[axis]["unit"] == reference[axis]["unit"]
        assert native[axis]["value"] == pytest.approx(reference[axis]["value"], abs=2e-15)

    assert source.open(str(native_dataset / "SOURCE"), nomodify=False)
    assert source.addrows(1)
    assert source.putcell("SOURCE_ID", 1, 0)
    assert source.putcell("SPECTRAL_WINDOW_ID", 1, 0)
    assert source.done()
    for module in (casatools, tools):
        metadata = module.msmetadata()
        assert metadata.open(str(native_dataset))
        try:
            with pytest.raises(RuntimeError, match="duplicate SOURCE table keys"):
                metadata.restfreqs()
            with pytest.raises(RuntimeError, match="duplicate SOURCE table keys"):
                metadata.transitions()
        finally:
            assert metadata.done()


def test_msmetadata_antenna_observation_selectors_and_measures(native_dataset):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    table = tools.table()
    assert table.open(str(native_dataset / "ANTENNA"), nomodify=False)
    assert table.putcol("DISH_DIAMETER", np.array([25., 18., 30., 25.]))
    position = table.getcol("POSITION")
    assert table.putcol("POSITION", position / np.array([[1000.], [1.], [0.01]]))
    assert table.putcolkeyword("POSITION", "QuantumUnits", ["km", "m", "cm"])
    assert table.done()
    assert table.open(str(native_dataset / "OBSERVATION"), nomodify=False)
    assert table.putcell("SCHEDULE", 0, ["schedule A", "schedule B"])
    assert table.putcell("TIME_RANGE", 0, np.array([4.8e9, 4.8e9 + 600.]) / 86400.)
    assert table.putcolkeyword("TIME_RANGE", "QuantumUnits", ["d"])
    assert table.done()

    results = []
    for module in (casatools, tools):
        metadata = module.msmetadata()
        assert metadata.open(str(native_dataset))
        try:
            assert metadata.antennadiameter("A2") == {"unit": "m", "value": 18.0}
            assert metadata.antennadiameter(np.int32(2)) == {"unit": "m", "value": 30.0}
            assert metadata.antennadiameter()["0"] == {"unit": "m", "value": 25.0}
            np.testing.assert_array_equal(metadata.antennaids(), np.arange(4), strict=True)
            np.testing.assert_array_equal(metadata.antennaids("A*", mindiameter="20m",
                                                             maxdiameter={"value": 25., "unit": "m"}),
                                          np.array([0, 3]), strict=True)
            np.testing.assert_array_equal(metadata.antennaids(np.array(["A3", "A1", "A3"])),
                                          np.array([2, 0]), strict=True)
            np.testing.assert_array_equal(metadata.antennaids("absent*", obsid=0),
                                          np.array([], dtype="int64"), strict=True)
            assert metadata.antennastations() == ["P1", "P2", "P3", "P4"]
            assert metadata.antennastations([2, 0, 2]) == ["P3", "P1", "P3"]
            assert metadata.antennastations(np.array([b"A4", b"A3"])) == ["P4", "P3"]
            assert metadata.antennastations("A1", obsid=0) == ["P1"]
            for dtype in ("U1", "S1"):
                empty = np.array([], dtype=dtype)
                np.testing.assert_array_equal(metadata.antennaids(empty), np.array([], dtype=np.int64), strict=True)
                assert metadata.antennastations(empty) == []
            if module is casatools:
                for selector in (np.uint32(1), np.bool_(True)):
                    np.testing.assert_array_equal(metadata.antennaids(selector), np.arange(4), strict=True)
                    assert metadata.antennastations(selector) == ["P1", "P2", "P3", "P4"]
                    assert metadata.antennaposition(selector) == metadata.antennaposition(0)
            else:
                assert metadata.antennastations(np.uint32(1)) == ["P2"]
                assert metadata.antennaposition(np.uint32(1)) == metadata.antennaposition(1)
                with pytest.raises(RuntimeError):
                    metadata.antennaids(np.uint32(1))
                for query in (metadata.antennaids, metadata.antennastations, metadata.antennaposition):
                    with pytest.raises(RuntimeError):
                        query(np.bool_(True))
            assert metadata.schedule(0) == ["schedule A", "schedule B"]
            results.append([
                metadata.antennaposition(), metadata.antennaposition(b"A2"),
                metadata.antennaoffset("A3"), metadata.observatoryposition(), metadata.timerangeforobs(0),
            ])
            for query, name in ((metadata.antennadiameter, "absent"), (metadata.antennaposition, 99),
                                (metadata.antennaoffset, -1), (metadata.schedule, -1),
                                (metadata.timerangeforobs, 1), (metadata.observatoryposition, 1)):
                with pytest.raises(RuntimeError):
                    query(name)
        finally:
            assert metadata.done()

    def compare(expected, actual):
        assert expected.keys() == actual.keys()
        for key, value in expected.items():
            if isinstance(value, dict):
                compare(value, actual[key])
            elif isinstance(value, float):
                assert actual[key] == pytest.approx(value, rel=1e-12, abs=1e-7)
            else:
                assert actual[key] == value

    for expected, actual in zip(*results):
        compare(expected, actual)


def test_msmetadata_stations_without_an_observed_match(native_dataset):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    table = tools.table()
    assert table.open(str(native_dataset), nomodify=False)
    for column in ("ANTENNA1", "ANTENNA2"):
        ids = table.getcol(column)
        assert np.any(ids == 3)
        assert table.putcol(column, np.where(ids == 3, 0, ids).astype(np.int32))
    assert table.done()
    for module in (casatools, tools):
        metadata = module.msmetadata()
        assert metadata.open(str(native_dataset))
        try:
            assert metadata.antennastations(3, obsid=0) == []
            expected = ["P1", "P2", "P3", "P4"] if module is casatools else []
            assert metadata.antennastations("A4", obsid=0) == expected
            assert metadata.antennastations(["A4"], obsid=0) == expected
            assert metadata.antennastations("A1", obsid=0) == ["P1"]
        finally:
            assert metadata.done()


def test_msmetadata_field_polynomial_direction_records(native_dataset):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    table = tools.table()
    assert table.open(str(native_dataset / "FIELD"), nomodify=False)
    origin = 4.8e9
    assert table.putcell("TIME", 0, origin)
    assert table.putcell("NUM_POLY", 0, 1)
    phase = (-0.12, 0.41)
    reference = (-0.13, 0.45)
    rates = (2e-5, -3e-5)
    for name, constant in (("PHASE_DIR", phase), ("REFERENCE_DIR", reference)):
        assert table.putcell(name, 0, np.asfortranarray(np.rad2deg(np.array([constant, rates]).T)))
        assert table.putcolkeyword(name, "QuantumUnits", ["deg", "deg"])
    assert table.done()

    def compare(direction, expected):
        assert (direction["type"], direction["refer"]) == ("direction", "J2000")
        for index, value in enumerate(expected):
            component = direction[f"m{index}"]
            assert component["unit"] == "rad"
            assert component["value"] == pytest.approx(value, abs=2e-12)

    epoch = {"type": "epoch", "refer": "UTC", "m0": {"value": origin + 20, "unit": "s"}}
    for module in (casatools, tools):
        metadata = module.msmetadata()
        assert metadata.open(str(native_dataset))
        try:
            compare(metadata.phasecenter(), phase)
            compare(metadata.refdir("F"), reference)
            compare(metadata.refdir(b"F"), reference)
            compare(metadata.phasecenter(0, epoch), tuple(offset + 20 * rate for offset, rate in zip(phase, rates)))
            compare(metadata.refdir(np.int32(0), epoch), tuple(offset + 20 * rate for offset, rate in zip(reference, rates)))
            relative_epoch = {
                "type": "epoch", "refer": "UTC", "m0": {"value": 20.0, "unit": "s"},
                "offset": {"type": "epoch", "refer": "UTC", "m0": {"value": origin, "unit": "s"}},
            }
            compare(metadata.phasecenter(0, relative_epoch),
                    tuple(offset + 20 * rate for offset, rate in zip(phase, rates)))
            with pytest.raises(RuntimeError):
                metadata.phasecenter(99)
            with pytest.raises(RuntimeError):
                metadata.refdir("absent")
            with pytest.raises(RuntimeError):
                metadata.refdir(0, {"type": "direction", "refer": "J2000",
                                    "m0": {"value": 0., "unit": "rad"}, "m1": {"value": 0., "unit": "rad"}})
        finally:
            assert metadata.done()


def test_msmetadata_attached_ephemeris_direction_records(native_dataset):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    table = tools.table()
    assert table.open(str(native_dataset), nomodify=True)
    start = float(table.getcell("TIME", 0))
    assert table.done()
    mjd = np.floor(start / 86400.0)
    field = native_dataset / "FIELD"
    assert table.open(str(field), nomodify=False)
    if "EPHEMERIS_ID" not in table.colnames():
        assert table.addcols({"EPHEMERIS_ID": {"valueType": "int"}})
    assert table.putcell("EPHEMERIS_ID", 0, 0)
    assert table.putcell("TIME", 0, start)
    assert table.putcell("PHASE_DIR", 0, np.array([[0.001], [0.002]]))
    assert table.putcell("REFERENCE_DIR", 0, np.array([[0.003], [0.004]]))
    assert table.done()

    columns = {name: {"valueType": "double"} for name in ("MJD", "RA", "DEC", "Rho", "RadVel")}
    assert table.create(str(field / "EPHEM0_TEST.tab"), columns, nrow=5)
    assert table.putinfo({"type": "IERS", "subType": "ephemeris"})
    for name, unit, values in (
        ("MJD", "d", np.arange(mjd - 2, mjd + 3)),
        ("RA", "deg", 15.0 + np.arange(5) / 10.0),
        ("DEC", "deg", -30.0 + np.arange(5) / 10.0),
        ("Rho", "AU", np.full(5, 2.0)),
        ("RadVel", "AU/d", np.full(5, 0.001)),
    ):
        assert table.putcolkeyword(name, "UNIT", unit)
        assert table.putcol(name, values)
    assert table.putkeywords({
        "VS_CREATE": "2026/11/22/00:00", "VS_DATE": "2026/11/22/00:00",
        "VS_VERSION": "0001.0001", "VS_TYPE": "Table of comet/planetary positions",
        "NAME": "TEST", "posrefsys": "ICRF/ICRS", "obsloc": "GEOCENTRIC", "GeoDist": 0.,
        "GeoLong": 0., "GeoLat": 0., "dMJD": 1., "MJD0": mjd - 3,
    })
    assert table.done()

    def epoch(day):
        return {"type": "epoch", "refer": "UTC", "m0": {"value": day, "unit": "d"}}

    outcomes = []
    for module in (casatools, tools):
        metadata = module.msmetadata()
        assert metadata.open(str(native_dataset))
        try:
            at_origin = metadata.phasecenter()
            later = metadata.phasecenter(0, epoch(mjd + 0.75))
            ref = metadata.refdir(0, epoch(mjd + 0.75))
            assert at_origin["m0"]["value"] != later["m0"]["value"]
            assert later["m1"]["value"] != ref["m1"]["value"]
            with pytest.raises(RuntimeError):
                metadata.phasecenter(0, epoch(mjd + 20))
            outcomes.append([at_origin, later, ref])
        finally:
            assert metadata.done()
    for expected, actual in zip(*outcomes):
        assert expected["refer"] == actual["refer"]
        for axis in ("m0", "m1"):
            assert expected[axis]["unit"] == actual[axis]["unit"]
            assert expected[axis]["value"] == pytest.approx(actual[axis]["value"], abs=2e-11)

    # The reference MAIN table has no local FIELD directory. Both tools
    # must resolve the field's ephemeris through the inherited FIELD link.
    view = native_dataset.with_name("ephemeris-view.ms")
    reference = casatools.table()
    assert reference.open(str(native_dataset))
    selection = reference.selectrows(np.array([0], dtype=np.int64), str(view))
    assert selection.done()
    assert reference.done()
    assert not (view / "FIELD").exists()
    for module, expected in zip((casatools, tools), outcomes):
        metadata = module.msmetadata()
        assert metadata.open(str(view))
        try:
            assert metadata.refdir(0, epoch(mjd + 0.75)) == expected[2]
        finally:
            assert metadata.done()


def test_msmetadata_pointing_interpolation_uses_each_antenna(native_dataset):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    table = tools.table()
    assert table.open(str(native_dataset), nomodify=False)
    first, second = table.getcol("ANTENNA1"), table.getcol("ANTENNA2")
    row = int(np.flatnonzero(first != second)[0])
    antennas = [int(first[row]), int(second[row])]
    time = float(table.getcell("TIME", row))
    assert table.putcell("INTERVAL", row, 2.0)
    assert table.done()
    assert table.open(str(native_dataset / "POINTING"), nomodify=False)
    if table.nrows():
        assert table.removerows(list(range(table.nrows())))
    assert table.addrows(4)
    assert table.putcolkeyword("DIRECTION", "QuantumUnits", ["deg", "deg"])
    assert table.putcolkeyword("TARGET", "QuantumUnits", ["deg", "deg"])
    for index, antenna in enumerate(antennas):
        for sample in range(2):
            record = 2 * index + sample
            assert table.putcell("ANTENNA_ID", record, antenna)
            assert table.putcell("TIME", record, time - 1 + sample * 10)
            assert table.putcell("INTERVAL", record, 10.0)
            assert table.putcell("NUM_POLY", record, 0)
            direction = np.array([[-0.12 + 0.03 * index + 0.01 * sample],
                                  [0.46 + 0.02 * index - 0.02 * sample]])
            assert table.putcell("DIRECTION", record, np.rad2deg(direction))
            assert table.putcell("TARGET", record, np.rad2deg(direction))
    assert table.done()

    for module in (casatools, tools):
        metadata = module.msmetadata()
        assert metadata.open(str(native_dataset))
        try:
            for interpolate in (False, True):
                result = metadata.pointingdirection(row, interpolate=interpolate, initialrow=2)
                assert result["time"] == time
                for index, antenna in enumerate(antennas):
                    entry = result[f"antenna{index + 1}"]
                    assert entry["id"] == antenna
                    direction = entry["pointingdirection"]
                    assert direction["m0"]["value"] == pytest.approx(
                        -0.12 + 0.03 * index + 0.001 * interpolate, abs=1e-13)
                    assert direction["m1"]["value"] == pytest.approx(
                        0.46 + 0.02 * index - 0.002 * interpolate, abs=1e-13)
            with pytest.raises(RuntimeError):
                metadata.pointingdirection(-1)
        finally:
            assert metadata.done()
