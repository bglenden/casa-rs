"""Attachment and direct table queries through the public metadata tool."""

import os

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


@needs_measures
def test_msmetadata_reads_native_dataset_and_preserves_attachment_on_open_error(tmp_path):
    casatools = pytest.importorskip("casatools")
    assert casatools.version_string().replace("-", ".") == "6.7.6.14"
    dataset = tmp_path / "metadata.ms"
    simulator = tools.simulator()
    assert simulator.open(str(dataset))
    configure(simulator)
    assert simulator.observe("F", "L", starttime="0s", stoptime="30s")
    assert simulator.close()
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
            results.append((
                metadata.nobservations(), metadata.nstates(), metadata.observatorynames(),
                metadata.projects(), metadata.observers(),
            ))
        finally:
            assert metadata.done()
    assert results[0] == results[1]
