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
