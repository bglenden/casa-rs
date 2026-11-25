"""Attach CASA comet tables and read the persisted field through both tools."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from shutil import copytree
import subprocess
import sys
from time import monotonic, sleep

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


@pytest.mark.parametrize("view_kind", ["reference", "multims"])
def test_addephemeris_targets_the_inherited_field(
    ephemeris_dataset, tmp_path, writer_api, view_kind,
):
    casa = pytest.importorskip("casatools")
    dataset, _ = ephemeris_dataset
    external = tmp_path / "view-source.tab"
    copytree(dataset / "FIELD" / "EPHEM0_TEST.tab", external)
    view = tmp_path / "view.ms"
    if view_kind == "reference":
        table = casa.table()
        assert table.open(str(dataset))
        rows = table.selectrows(np.array([0], dtype=np.int64), str(view))
        assert rows.done()
        assert table.done()
        field = dataset / "FIELD"
        another = None
    else:
        other = tmp_path / "other.ms"
        copytree(dataset, other)
        tool = casa.ms()
        assert tool.createmultims(
            str(view), list(map(str, (dataset, other))), ["ANTENNA", "DATA_DESCRIPTION", "FEED"]
        )
        assert tool.done()
        assert tool.open(str(view))
        first, second = tool.getreferencedtables()
        field = Path(first) / "FIELD"
        another = Path(second) / "FIELD"
        assert tool.done()

    tool = writer_api.ms()
    assert tool.open(str(view), nomodify=False)
    assert tool.addephemeris(0, str(external), "view", [0])
    direction = tool.getfielddirmeas("EPHEMERIS_DIR", 0)
    assert direction["refer"] == "ICRS"
    assert tool.done()
    assert (field / "EPHEM0_view.tab").is_dir()
    if another is not None:
        assert {path.name for path in another.glob("EPHEM*.tab")} == {"EPHEM0_TEST.tab"}


@pytest.mark.parametrize("unselected", [False, True])
def test_addephemeris_with_a_retained_lock(ephemeris_dataset, tmp_path, writer_api, unselected):
    dataset, _ = ephemeris_dataset
    external = tmp_path / "locked-source.tab"
    copytree(dataset / "FIELD" / "EPHEM0_TEST.tab", external)
    tool = writer_api.ms()
    assert tool.open(str(dataset), nomodify=unselected, lock=True)
    assert tool.addephemeris(-1, str(external), "locked", [] if unselected else [0])
    if writer_api is tools:
        assert_external_write_refused(dataset / "FIELD")
    assert tool.done()
    ephemeris_id = 1 if unselected else 0
    assert (dataset / "FIELD" / f"EPHEM{ephemeris_id}_locked.tab").is_dir()


def test_addephemeris_refreshes_an_existing_metadata_tool(ephemeris_dataset, tmp_path, writer_api):
    casa = pytest.importorskip("casatools")
    dataset, _ = ephemeris_dataset
    external = tmp_path / "shifted.tab"
    copytree(dataset / "FIELD" / "EPHEM0_TEST.tab", external)
    table = casa.table()
    assert table.open(str(external), nomodify=False)
    assert table.putcol("RA", table.getcol("RA") + 3.0)
    assert table.done()

    tool = writer_api.ms()
    assert tool.open(str(dataset), nomodify=False)
    metadata = tool.metadata()
    previous = metadata.phasecenter()
    assert tool.addephemeris(0, str(external), "shifted", [0])
    current = metadata.phasecenter()
    assert metadata.done()
    assert tool.done()
    fresh = casa.msmetadata()
    assert fresh.open(str(dataset))
    assert fresh.phasecenter() == current
    assert fresh.done()
    assert current["m0"]["value"] - previous["m0"]["value"] == pytest.approx(
        np.deg2rad(3.0), abs=2e-11,
    )


def assert_external_write_refused(path):
    probe = (
        "import sys; from casars.tools import table; t=table(); "
        "assert t.open(sys.argv[1], lockoptions={'option': 'user'}, nomodify=False); "
        "assert not t.lock(write=True, nattempts=1); assert t.done()"
    )
    subprocess.run([sys.executable, "-c", probe, str(path)], check=True, timeout=10)


def test_addephemeris_holds_the_new_field_while_history_waits(ephemeris_dataset, tmp_path):
    dataset, _ = ephemeris_dataset
    external = tmp_path / "locked-source.tab"
    copytree(dataset / "FIELD" / "EPHEM0_TEST.tab", external)
    tool = tools.ms()
    assert tool.open(str(dataset), nomodify=False)
    holder = (
        "import sys; from casars.tools import table; t=table(); "
        "assert t.open(sys.argv[1], lockoptions={'option':'user'}, nomodify=False); "
        "assert t.lock(write=True, nattempts=1); print('ready', flush=True); "
        "sys.stdin.readline(); assert t.done()"
    )
    with subprocess.Popen(
        [sys.executable, "-c", holder, str(dataset / "HISTORY")],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    ) as process:
        try:
            assert process.stdout.readline().strip() == "ready"
            with ThreadPoolExecutor(max_workers=1) as pool:
                result = pool.submit(tool.addephemeris, 0, str(external), "pending", [0])
                try:
                    replacement = dataset / "FIELD" / "EPHEM0_pending.tab"
                    limit = monotonic() + 10
                    while not replacement.is_dir() and monotonic() < limit and not result.done():
                        sleep(0.01)
                    assert replacement.is_dir()
                    assert not result.done()
                    assert_external_write_refused(dataset / "FIELD")
                    assert_external_write_refused(replacement)
                finally:
                    process.communicate("\n", timeout=10)
                assert result.result(timeout=10)
        finally:
            if process.poll() is None:
                process.kill()
    assert tool.done()


def test_addephemeris_restores_field_after_history_failure(ephemeris_dataset, tmp_path):
    dataset, _ = ephemeris_dataset
    field = dataset / "FIELD"
    original = field / "EPHEM0_TEST.tab"
    source = tmp_path / "source.tab"
    copytree(original, source)
    before = (original / "table.f0").read_bytes()
    table = tools.table()
    assert table.open(str(dataset / "HISTORY"), nomodify=False)
    assert table.removecols("MESSAGE")
    assert table.done()
    tool = tools.ms()
    assert tool.open(str(dataset), nomodify=False, lock=True)
    with pytest.raises(RuntimeError):
        tool.addephemeris(0, str(source), "uncommitted", [0])
    assert (original / "table.f0").read_bytes() == before
    assert not (field / "EPHEM0_uncommitted.tab").exists()
    assert_external_write_refused(field)
    assert tool.done()


def test_addephemeris_copies_a_linked_source_payload(ephemeris_dataset, tmp_path):
    casa = pytest.importorskip("casatools")
    dataset, _ = ephemeris_dataset
    source = tmp_path / "ephemeris.tab"
    copytree(dataset / "FIELD" / "EPHEM0_TEST.tab", source)
    data = source / "table.f0"
    shared = tmp_path / "shared"
    shared.mkdir()
    data.rename(shared / "payload")
    data.symlink_to(Path("../shared/payload"))
    tool = tools.ms()
    assert tool.open(str(dataset), nomodify=False)
    assert tool.addephemeris(0, str(source), "materialized", [0])
    assert tool.done()
    assert data.is_symlink()
    actual = dataset / "FIELD" / "EPHEM0_materialized.tab" / "table.f0"
    assert not actual.is_symlink()
    assert actual.read_bytes() == data.read_bytes()
    tool = casa.ms()
    assert tool.open(str(dataset))
    assert tool.getfielddirmeas("EPHEMERIS_DIR", 0)["refer"] == "ICRS"
    assert tool.done()


def test_addephemeris_preserves_distinct_exact_time_and_metadata_views(ephemeris_dataset, tmp_path):
    dataset, _ = ephemeris_dataset
    source = tmp_path / "selected-source.tab"
    copytree(dataset / "FIELD" / "EPHEM0_TEST.tab", source)
    tool = tools.ms()
    assert tool.open(str(dataset), nomodify=False)
    times = tool.getdata("time")["time"]
    assert tool.selectinit()
    assert tool.select({"times": [float(times[0])]})
    selected = tool.getdata("time")["time"]
    assert selected.size < times.size
    metadata = tool.metadata()
    assert metadata.nrows(autoc=True) == times.size
    assert tool.addephemeris(0, str(source), "selected", [0])
    assert metadata.nrows(autoc=True) == times.size
    np.testing.assert_array_equal(tool.getdata("time")["time"], selected, strict=True)
    assert metadata.done()
    assert tool.done()


def test_addephemeris_checks_the_source_before_readonly_permission(
    ephemeris_dataset, tmp_path, writer_api,
):
    dataset, _ = ephemeris_dataset
    table = tools.table()
    assert table.open(str(dataset / "FIELD"), nomodify=False)
    assert table.removecols("EPHEMERIS_ID")
    assert table.done()
    tool = writer_api.ms()
    assert tool.open(str(dataset))
    missing = tmp_path / "missing.tab"
    assert tool.addephemeris(ephemerisname=str(missing)) is False
    missing.mkdir()
    assert tool.addephemeris(ephemerisname=str(missing)) is False
    with pytest.raises(RuntimeError):
        tool.addephemeris(ephemerisname=str(dataset / "FIELD"))
    if writer_api is tools:
        with pytest.raises(RuntimeError, match="requires an ephemeris table name"):
            tool.addephemeris()
    assert tool.done()
