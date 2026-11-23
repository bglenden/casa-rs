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
