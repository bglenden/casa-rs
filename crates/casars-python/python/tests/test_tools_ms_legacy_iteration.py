"""Pinned controls for the legacy cursor, block view and persisted sort cache."""

import os
from pathlib import Path
from shutil import copyfile, copytree
import subprocess
import sys

import numpy as np
import pytest

from casars import tools
from test_tools_ms_putdata import assert_main_columns, make_multi_ms, writer_api
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


def test_legacy_cursor_exposes_positioned_rows_ranges_and_unchanged_modern_view(
    baseline_fixture, native_dataset, writer_api,
):
    _, _, original = baseline_fixture
    rows = np.flatnonzero(original["time"] == original["time"][0])[:8]
    assert len(rows) == 8
    reader = writer_api.ms()
    for control in (reader.iteroriginold, reader.iternextold, reader.iterendold):
        assert control() is False
    assert reader.open(str(native_dataset))
    assert reader.selectold({"rows": rows})
    assert reader.iterinitold(["TIME"], maxrows=np.int64(3), adddefaultsortcolumns=False)
    assert reader.nrowold(True) == len(rows)
    assert reader.iteroriginold()
    for first in range(0, len(rows), 3):
        chunk = rows[first:first + 3]
        assert reader.nrowold(True) == len(chunk)
        assert reader.nrow(True) == len(original["time"])
        actual = reader.rangeold(["rows", "time"])
        np.testing.assert_array_equal(actual["rows"], chunk, strict=True)
        np.testing.assert_array_equal(
            actual["time"], np.full(2, original["time"][chunk[0]]), strict=True,
        )
        current = reader.getdataold(["data", "flag"])
        for name in current:
            np.testing.assert_array_equal(current[name], original[name][..., chunk], strict=True)
        assert reader.iternextold() is (first + 3 < len(rows))
    assert reader.nrowold(True) == len(rows)
    assert reader.iternextold() is False
    assert reader.iterendold() is True
    assert reader.iteroriginold() is True
    assert reader.nrowold(True) == 3
    assert reader.done()


def test_legacy_iterator_persists_a_readable_sorted_reference_and_writes_only_the_active_block(
    baseline_fixture, native_dataset, writer_api,
):
    casa, _, original = baseline_fixture
    writer = writer_api.ms()
    assert writer.open(str(native_dataset), nomodify=False)
    keys = ["ANTENNA2", "TIME"]
    assert writer.iterinitold(keys, maxrows=3, adddefaultsortcolumns=False)
    assert writer.iteroriginold()
    assert writer.iterinitold(["missing_column"], adddefaultsortcolumns=False) is False
    assert writer.iternextold()
    active = writer.rangeold("rows")["rows"]
    selected = writer.getdataold("data")["data"] * 2
    assert 0 < len(active) <= 3
    assert writer.putdataold({"data": selected})
    assert writer.iterendold()
    assert writer.nrowold(True) == len(original["time"])
    assert writer.done()

    expected = original["data"].copy()
    expected[..., active] = selected
    assert_main_columns(casa, native_dataset, {"DATA": expected, "FLAG": original["flag"]})
    order = np.lexsort((original["time"], original["antenna2"]))
    for api in (casa, tools):
        table = api.table()
        assert table.open(str(native_dataset))
        assert table.getkeyword("SORT_COLUMNS").tolist() == keys
        assert "SORTED_TABLE" in table.getkeyword("SORTED_TABLE")
        assert table.done()
        assert table.open(str(native_dataset / "SORTED_TABLE"))
        np.testing.assert_array_equal(table.getcol("DATA"), expected[..., order], strict=True)
        assert table.done()

    moved = native_dataset.rename(native_dataset.with_name("relocated.ms"))
    for api in (casa, tools):
        table = api.table()
        assert table.open(str(moved / "SORTED_TABLE"))
        np.testing.assert_array_equal(table.getcol("DATA"), expected[..., order], strict=True)
        assert table.done()


def test_legacy_cursor_omits_the_empty_block_after_an_exact_multiple(
    baseline_fixture, native_dataset,
):
    casa, _, original = baseline_fixture
    rows = np.flatnonzero(original["time"] == original["time"][0])[:8]
    assert len(rows) == 8
    for api in (casa, tools):
        reader = api.ms()
        assert reader.open(str(native_dataset))
        assert reader.selectold({"rows": rows})
        assert reader.iterinitold(["TIME"], maxrows=4, adddefaultsortcolumns=False)
        assert reader.iteroriginold()
        counts = [reader.nrowold(True)]
        while reader.iternextold():
            counts.append(reader.nrowold(True))
        assert counts == ([4, 4, 0] if api is casa else [4, 4])
        assert reader.nrowold(True) == 8
        assert reader.done()


def test_legacy_writable_multims_uses_a_cursor_without_a_saved_sort_reference(
    baseline_fixture, native_dataset, tmp_path, writer_api, monkeypatch,
):
    casa, _, original = baseline_fixture
    members = [tmp_path / name for name in ("part-z.ms", "part-a.ms")]
    for member in members:
        copytree(native_dataset, member)
    path = make_multi_ms(casa, tmp_path / "multi.ms", members)
    scratch = tmp_path / "cwd"
    scratch.mkdir()
    monkeypatch.chdir(scratch)
    reader = writer_api.ms()
    assert reader.open(path, nomodify=False)
    assert reader.iterinitold(["TIME"], adddefaultsortcolumns=False)
    assert reader.iteroriginold()
    assert reader.nrowold(True) == 2 * len(original["time"])
    record = reader.getdataold("time")
    np.testing.assert_array_equal(record["time"], np.sort(np.tile(original["time"], 2)), strict=True)
    assert reader.iternextold() is False
    assert reader.iterendold()
    assert reader.done()
    assert not list(tmp_path.rglob("SORTED_TABLE"))
    stray = list(scratch.glob("*SORTED_TABLE"))
    assert len(stray) == (1 if writer_api is casa else 0)
    table = casa.table()
    assert table.open(path)
    assert not {"SORT_COLUMNS", "SORTED_TABLE"}.intersection(table.keywordnames())
    assert table.done()


def test_writable_legacy_reference_caches_root_rows_and_restores_view_order(
    baseline_fixture, native_dataset, writer_api,
):
    casa, view, original = baseline_fixture
    rows = np.array([13, 7, 0, 2, 5], dtype=np.int64)
    path = view("ordered-reference.ms", rows)
    reader = writer_api.ms()
    assert reader.open(path, nomodify=False)
    assert reader.iterinitold(["TIME", "ANTENNA1"], adddefaultsortcolumns=False)
    assert reader.iteroriginold()
    np.testing.assert_array_equal(reader.rangeold("rows")["rows"], np.array([0, 2]), strict=True)
    assert reader.iterendold()
    np.testing.assert_array_equal(reader.rangeold("rows")["rows"], rows, strict=True)
    assert reader.done()

    expected = rows[np.lexsort((original["antenna1"][rows], original["time"][rows]))]
    for api in (casa, tools):
        table = api.table()
        assert table.open(str(native_dataset / "SORTED_TABLE"))
        np.testing.assert_array_equal(table.getcol("DATA"), original["data"][..., expected], strict=True)
        assert table.done()


def test_legacy_iterator_rejects_a_rebound_write_and_keeps_positioned_reads(
    baseline_fixture, native_dataset,
):
    casa, view, original = baseline_fixture
    path = Path(view("positioned-legacy.ms", [0, 1, 2, 3, 4, 5]))
    replacement = Path(view("replacement-legacy.ms", [10, 11, 12, 13, 14, 15]))
    writer = tools.ms()
    assert writer.open(str(path), nomodify=False)
    assert writer.iterinitold(["TIME"], maxrows=3, adddefaultsortcolumns=False)
    assert writer.iteroriginold()
    prior = writer.getdataold("data")["data"]
    rows = writer.rangeold("rows")["rows"]
    pending = path / "pending.dat"
    copyfile(replacement / "table.dat", pending)
    pending.replace(path / "table.dat")
    np.testing.assert_array_equal(writer.getdataold("data")["data"], prior, strict=True)
    np.testing.assert_array_equal(writer.rangeold("rows")["rows"], rows, strict=True)
    with pytest.raises(RuntimeError, match="row layout changed"):
        writer.putdataold({"data": prior * 2})
    assert writer.done()
    assert_main_columns(casa, native_dataset, {"DATA": original["data"]})


@pytest.mark.parametrize("replace", [False, True])
def test_legacy_sort_publication_keeps_prior_links_and_artifact_on_failure(
    baseline_fixture, native_dataset, replace,
):
    _, _, _ = baseline_fixture
    reader = tools.ms()
    assert reader.open(str(native_dataset), nomodify=False)
    output = native_dataset / "SORTED_TABLE"
    if replace:
        assert reader.iterinitold(["TIME"], adddefaultsortcolumns=False)
    data = native_dataset / "table.dat"
    original_metadata = data.read_bytes()
    original_output = (output / "table.dat").read_bytes() if replace else None
    permissions = data.stat().st_mode
    try:
        data.chmod(0o444)
        with pytest.raises(RuntimeError):
            reader.iterinitold(["ANTENNA2", "TIME"], adddefaultsortcolumns=False)
    finally:
        data.chmod(permissions)
    assert data.read_bytes() == original_metadata
    if replace:
        assert (output / "table.dat").read_bytes() == original_output
    else:
        assert not output.exists()
    assert reader.done()


def test_legacy_sort_changes_only_the_root_keywords(baseline_fixture, native_dataset):
    casa, _, _ = baseline_fixture
    reader = tools.ms()
    assert reader.open(str(native_dataset), nomodify=False)
    info = native_dataset / "table.info"
    payload = info.read_bytes()
    permissions = info.stat().st_mode
    try:
        info.chmod(0o444)
        assert reader.iterinitold(["TIME"], adddefaultsortcolumns=False)
    finally:
        info.chmod(permissions)
    assert reader.done()
    assert info.read_bytes() == payload
    table = casa.table()
    assert table.open(str(native_dataset))
    assert table.getkeyword("SORT_COLUMNS").tolist() == ["TIME"]
    assert table.done()


def test_legacy_sort_cache_reuse_respects_input_row_order(
    baseline_fixture, native_dataset, writer_api,
):
    casa, _, original = baseline_fixture
    reader = writer_api.ms()
    assert reader.open(str(native_dataset), nomodify=False)
    keys = ["TIME"]
    assert reader.iterinitold(keys, adddefaultsortcolumns=False)
    later = np.flatnonzero(original["time"] > original["time"][0])[0]
    assert reader.selectold({"rows": np.array([later, 0], dtype=np.int64)})
    assert reader.iterinitold(keys, adddefaultsortcolumns=False)
    assert reader.done()
    table = casa.table()
    assert table.open(str(native_dataset / "SORTED_TABLE"))
    np.testing.assert_array_equal(table.getcol("DATA"), original["data"][..., [0, later]],
                                  strict=True)
    assert table.done()


def test_legacy_sort_preserves_projected_columns_and_the_modern_cursor(
    baseline_fixture, native_dataset, tmp_path, writer_api,
):
    casa, _, original = baseline_fixture
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    assert table.addcols({"MODEL_DATA": table.getcoldesc("DATA")})
    assert table.putcol("MODEL_DATA", original["data"].astype(np.complex64))
    columns = [name for name in table.colnames() if name != "MODEL_DATA"]
    path = str(tmp_path / "projected-legacy.ms")
    reference = table.query("", name=path, columns=", ".join(columns))
    assert reference.done()
    assert table.done()
    reader = writer_api.ms()
    assert reader.open(path, nomodify=False)
    assert reader.iterinit()
    assert reader.iterorigin()
    before = reader.getdata("time")["time"]
    assert reader.iterinitold(["ANTENNA2", "TIME"], adddefaultsortcolumns=False)
    np.testing.assert_array_equal(reader.getdata("time")["time"], before, strict=True)
    assert reader.done()
    output = str(native_dataset / "SORTED_TABLE")
    assert table.open(output)
    assert table.colnames() == columns
    assert table.done()
    if writer_api is tools:
        assert reader.open(output, nomodify=False)
        metadata = (native_dataset / "SORTED_TABLE" / "table.dat").read_bytes()
        assert reader.iterinitold(["TIME"], adddefaultsortcolumns=False)
        assert reader.iteroriginold()
        assert reader.getdataold("time")["time"].size
        assert reader.done()
        assert (native_dataset / "SORTED_TABLE" / "table.dat").read_bytes() == metadata


@pytest.mark.skipif(os.name != "posix", reason="casacore process-lock request list")
def test_legacy_sort_rechecks_keys_after_waiting_for_a_casa_writer(
    baseline_fixture, native_dataset,
):
    casa, _, _ = baseline_fixture
    reader = tools.ms()
    assert reader.open(str(native_dataset), nomodify=False)
    writer_script = """
from pathlib import Path
import sys
from time import monotonic, sleep

import casatools

path = Path(sys.argv[1])
t = casatools.table()
assert t.open(str(path), nomodify=False, lockoptions={"option": "user"})
assert t.lock(write=True)
# Keep the descriptor open until unlock so closing it cannot release our lock.
with (path / "table.lock").open("rb", buffering=0) as stream:
    print("ready", flush=True)
    deadline = monotonic() + 10
    while monotonic() < deadline:
        stream.seek(0)
        if int.from_bytes(stream.read(4), "big") > 0:
            break
        sleep(0.01)
    else:
        raise AssertionError("legacy iterator did not reach the root's write lock")
    previous = (path / "table.dat").read_bytes()
    assert t.putcell("TIME", 0, t.getcell("TIME", t.nrows() - 1) + 1000)
    assert t.flush()
    assert (path / "table.dat").read_bytes() == previous
    assert t.unlock()
    assert t.done()
"""
    with subprocess.Popen(
        [sys.executable, "-c", writer_script, str(native_dataset)],
        stdout=subprocess.PIPE, text=True,
    ) as writer:
        assert writer.stdout.readline().strip() == "ready"
        assert reader.iterinitold(["TIME"], adddefaultsortcolumns=False)
        assert writer.wait(timeout=15) == 0
    assert reader.done()
    table = casa.table()
    assert table.open(str(native_dataset / "SORTED_TABLE"))
    ordered = table.getcol("TIME")
    assert ordered[-1] > ordered[-2]
    assert np.all(ordered[1:] >= ordered[:-1])
    assert table.done()
