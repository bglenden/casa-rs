"""Safety and persistence laws for table creation (#1028)."""

import shutil
import sys
import tarfile
from pathlib import Path

import numpy as np
import pytest

from casars import tools


DESCRIPTION = {"id": {"valueType": "int"}}


def contents(path: Path) -> dict[str, bytes]:
    return {
        str(file.relative_to(path)): file.read_bytes()
        for file in path.rglob("*")
        if file.is_file() and file.name != "table.lock"
    }


def test_invalid_lock_does_not_replace_existing_table(tmp_path: Path) -> None:
    path = tmp_path / "existing.table"
    tool = tools.table()
    assert tool.create(str(path), DESCRIPTION, nrow=1)
    assert tool.putcell("id", 0, 42)
    assert tool.close()
    before = contents(path)

    with pytest.raises(RuntimeError, match="unknown lock option"):
        tool.create(str(path), DESCRIPTION, lockoptions={"option": "not-a-lock"})

    assert contents(path) == before
    assert tool.open(str(path))
    assert tool.getcell("id", 0) == 42
    tool.done()


def test_create_refuses_to_replace_an_open_table(tmp_path: Path) -> None:
    path = tmp_path / "open.table"
    owner, other = tools.table(), tools.table()
    assert owner.create(str(path), DESCRIPTION, nrow=1)
    assert owner.putcell("id", 0, 42)
    before = contents(path)

    with pytest.raises(RuntimeError, match="still open"):
        other.create(str(path), DESCRIPTION)

    assert contents(path) == before
    assert owner.getcell("id", 0) == 42
    owner.done()
    other.done()


def test_create_does_not_delete_an_unrelated_directory(tmp_path: Path) -> None:
    path = tmp_path / "not-a-table"
    path.mkdir()
    marker = path / "keep.txt"
    marker.write_text("keep this")
    tool = tools.table()

    with pytest.raises(RuntimeError, match="not a true table directory"):
        tool.create(str(path), DESCRIPTION)

    assert marker.read_text() == "keep this"
    tool.done()


def test_create_rejects_unsupported_manager_before_replacement(tmp_path: Path) -> None:
    path = tmp_path / "existing.table"
    tool = tools.table()
    assert tool.create(str(path), DESCRIPTION, nrow=1)
    assert tool.putcell("id", 0, 42)
    assert tool.close()
    before = contents(path)
    manager = {"*1": {"TYPE": "StandardStMan", "NAME": "custom", "COLUMNS": ["id"], "SPEC": {}}}

    with pytest.raises(NotImplementedError, match="dminfo"):
        tool.create(str(path), DESCRIPTION, dminfo=manager)

    assert contents(path) == before
    assert tool.open(str(path))
    assert tool.getcell("id", 0) == 42
    tool.done()


def test_memory_table_structural_writes_stay_in_memory(tmp_path: Path) -> None:
    path = tmp_path / "memory.table"
    tool = tools.table()
    assert tool.create(str(path), DESCRIPTION, memtype="memory", nrow=2)
    assert tool.renamecol("id", "renamed")
    assert tool.addcols({"v": {"valueType": "double"}})
    assert tool.colnames() == ["renamed", "v"]
    assert tool.putcell("v", 0, 3.5)
    assert tool.getcell("v", 0) == 3.5
    assert tool.removerows([1])
    assert tool.nrows() == 1
    assert tool.removecols("v")
    assert tool.colnames() == ["renamed"]
    assert tool.done()
    assert not path.exists()


def test_multitable_does_not_replace_or_nest_its_input(tmp_path: Path) -> None:
    path = tmp_path / "input.table"
    tool = tools.table()
    assert tool.create(str(path), DESCRIPTION, nrow=1)
    assert tool.putcell("id", 0, 42)
    assert tool.close()
    before = contents(path)

    assert not tool.createmultitable(str(path), [str(path)], "")
    assert not tool.createmultitable(str(path / "nested"), [str(path)], "")
    assert contents(path) == before
    assert tool.open(str(path))
    assert tool.getcell("id", 0) == 42
    tool.done()


def test_multitable_move_rejects_duplicate_names_without_losing_input(tmp_path: Path) -> None:
    paths = [tmp_path / "one" / "input.table", tmp_path / "two" / "input.table"]
    tool = tools.table()
    for index, path in enumerate(paths):
        path.parent.mkdir()
        assert tool.create(str(path), DESCRIPTION, nrow=1)
        assert tool.putcell("id", 0, index + 40)
        assert tool.close()
    before = [contents(path) for path in paths]

    assert not tool.createmultitable(str(tmp_path / "joined"), [str(path) for path in paths], "parts")
    assert [contents(path) for path in paths] == before
    assert not (tmp_path / "joined").exists()
    tool.done()


def test_multitable_move_rejects_open_input(tmp_path: Path) -> None:
    path = tmp_path / "input.table"
    owner, other = tools.table(), tools.table()
    assert owner.create(str(path), DESCRIPTION, nrow=1)
    assert owner.putcell("id", 0, 42)
    before = contents(path)

    assert not other.createmultitable(str(tmp_path / "joined"), [str(path)], "parts")
    assert contents(path) == before
    assert owner.getcell("id", 0) == 42
    owner.done()
    other.done()


def test_multitable_move_restores_inputs_if_writing_output_fails(tmp_path: Path) -> None:
    path = tmp_path / "input.table"
    tool = tools.table()
    assert tool.create(str(path), DESCRIPTION, nrow=1)
    assert tool.putcell("id", 0, 42)
    assert tool.close()
    before = contents(path)

    # This subdirectory collides with the new table's required control file.
    assert not tool.createmultitable(str(tmp_path / "joined"), [str(path)], "table.dat")
    assert contents(path) == before
    assert not (tmp_path / "joined").exists()
    assert tool.open(str(path))
    assert tool.getcell("id", 0) == 42
    tool.done()


REPO_ROOT = Path(__file__).resolve().parents[4]
CASA_TABLES_DATA = REPO_ROOT / "libs/casacore/casa-tables/tests/data"


def test_create_default_manager_and_dminfo_specs(tmp_path: Path) -> None:
    """Default `create` picks StandardStMan (`IndexLength: 0` before close, `130`
    in `casa-rs` SSMIndex v2 after reopen), updates `getdminfo()` after
    `removecols`, and exposes CASA-compatible `SPEC`s across Standard,
    Incremental, and Tiled storage managers (#1028)."""
    created = tmp_path / "default_ssm.table"
    tool = tools.table()
    assert tool.create(
        str(created),
        {"id": {"valueType": "int"}, "extra": {"valueType": "double"}},
        nrow=2,
    )
    live = tool.getdminfo()["*1"]
    assert live["TYPE"] == "StandardStMan"
    assert live["NAME"] == "StandardStMan"
    assert live["COLUMNS"].tolist() == ["extra", "id"]
    assert live["SPEC"] == {
        "BUCKETSIZE": 384,
        "IndexLength": 0,
        "MaxCacheSize": 2,
        "PERSCACHESIZE": 2,
    }
    assert tool.close()

    assert tool.open(str(created), nomodify=False)
    reopened = tool.getdminfo()["*1"]
    assert reopened["SPEC"] == {
        "BUCKETSIZE": 384,
        "IndexLength": 130,
        "MaxCacheSize": 2,
        "PERSCACHESIZE": 2,
    }
    assert tool.removecols(["extra"])
    assert tool.getdminfo()["*1"]["COLUMNS"].tolist() == ["id"]
    tool.done()

    ism_copy = tmp_path / "ism"
    shutil.copytree(CASA_TABLES_DATA / "ism_variable_shapes", ism_copy)
    assert tool.open(str(ism_copy))
    ism = tool.getdminfo()["*1"]
    assert ism["TYPE"] == "IncrementalStMan"
    assert ism["SPEC"] == {"BUCKETSIZE": 32768, "MaxCacheSize": 1, "PERSCACHESIZE": 1}
    tool.done()

    tiled_data_copy = tmp_path / "tiled_data"
    shutil.copytree(CASA_TABLES_DATA / "tiled_data_stman/little", tiled_data_copy)
    assert tool.open(str(tiled_data_copy))
    tsm_data = tool.getdminfo()["*1"]
    assert tsm_data["TYPE"] == "TiledDataStMan"
    assert tsm_data["SPEC"]["DEFAULTTILESHAPE"].tolist() == []
    assert tsm_data["SPEC"]["HYPERCUBES"]["*1"]["BucketSize"] == 24
    assert tsm_data["SPEC"]["HYPERCUBES"]["*1"]["CellShape"].tolist() == [2, 3]
    assert tsm_data["SPEC"]["HYPERCUBES"]["*1"]["ID"] == {"id": 10}
    assert tsm_data["SPEC"]["HYPERCUBES"]["*2"]["BucketSize"] == 48
    tool.done()

    tiled_col_copy = tmp_path / "tiled_col"
    shutil.copytree(CASA_TABLES_DATA / "tiled_tile_order/column_nodef/little", tiled_col_copy)
    assert tool.open(str(tiled_col_copy))
    tsm_col = tool.getdminfo()["*1"]
    assert tsm_col["TYPE"] == "TiledColumnStMan"
    assert tsm_col["SPEC"]["DEFAULTTILESHAPE"].tolist() == [2, 3, 2]
    assert tsm_col["SPEC"]["HYPERCUBES"]["*1"]["BucketSize"] == 194
    assert tsm_col["SPEC"]["HYPERCUBES"]["*1"]["CellShape"].tolist() == [2, 3]
    assert tsm_col["SPEC"]["HYPERCUBES"]["*1"]["CubeShape"].tolist() == [2, 3, 3]
    tool.done()

    ms_dir = tmp_path / "ms"
    with tarfile.open(REPO_ROOT / "crates/casa-ms/tests/fixtures/mssel_test_small.ms.tgz", "r:gz") as arc:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        arc.extractall(ms_dir, **extract)
    assert tool.open(str(ms_dir / "mssel_test_small.ms"))
    by_name = {dm["NAME"]: dm for dm in tool.getdminfo().values()}
    flag_cat = by_name["TiledFlagCategory"]
    assert flag_cat["TYPE"] == "TiledShapeStMan"
    assert flag_cat["SPEC"]["HYPERCUBES"] == {}
    assert flag_cat["SPEC"]["IndexSize"] == 0
    tiled_data = by_name["TiledDATA"]
    assert set(tiled_data["SPEC"]["HYPERCUBES"]) == {"*1", "*2"}
    assert tiled_data["SPEC"]["IndexSize"] == 8
    assert np.array_equal(tiled_data["SPEC"]["DEFAULTTILESHAPE"], [1, 4, 32768])
    tool.done()


def test_append_to_mixed_measurement_set_preserves_manager_layout(tmp_path: Path) -> None:
    fixture = REPO_ROOT / "crates/casa-ms/tests/fixtures/mssel_test_small.ms.tgz"
    with tarfile.open(fixture, "r:gz") as archive:
        archive.extractall(tmp_path, filter="data")
    path = tmp_path / "mssel_test_small.ms"
    tool = tools.table()
    assert tool.open(str(path), nomodify=False)
    before = tool.getdminfo()
    rows = tool.nrows()
    last_scan = tool.getcell("SCAN_NUMBER", rows - 1)
    last_time = tool.getcell("TIME", rows - 1)
    last_uvw = tool.getcell("UVW", rows - 1)
    data = (path / "table.f17_TSM1").read_bytes()
    assert tool.addrows(2)
    assert tool.nrows() == rows + 2
    assert tool.close()
    assert tool.open(str(path))
    after = tool.getdminfo()
    fields = ("TYPE", "NAME", "SEQNR", "COLUMNS")
    assert [[before[key][f] for f in fields] for key in before] == [
        [after[key][f] for f in fields] for key in before
    ]
    assert (path / "table.f17_TSM1").read_bytes() == data
    np.testing.assert_array_equal(tool.getcell("UVW", rows - 1), last_uvw)
    for row in (rows, rows + 1):
        assert tool.getcell("SCAN_NUMBER", row) == last_scan
        assert tool.getcell("TIME", row) == last_time
        assert tool.getcell("ANTENNA1", row) == 0
        assert not tool.getcell("FLAG_ROW", row)
        assert not tool.iscelldefined("DATA", row)
        assert not tool.iscelldefined("FLAG", row)
        np.testing.assert_array_equal(tool.getcell("UVW", row), [0.0, 0.0, 0.0])
    tool.done()


def test_column_addition_changes_only_the_selected_manager(tmp_path: Path) -> None:
    fixture = REPO_ROOT / "crates/casa-ms/tests/fixtures/mssel_test_small.ms.tgz"
    with tarfile.open(fixture, "r:gz") as archive:
        archive.extractall(tmp_path, filter="data")
    path = tmp_path / "mssel_test_small.ms"
    tool = tools.table()
    assert tool.open(str(path), nomodify=False)
    before = contents(path)
    standard = min(
        (manager for manager in tool.getdminfo().values() if manager["TYPE"] == "StandardStMan"),
        key=lambda manager: manager["SEQNR"],
    )
    assert tool.addcols({"PROBE": {"valueType": "int"}})
    assert tool.getcol("PROBE").tolist() == [0] * tool.nrows()
    assert next(dm["NAME"] for dm in tool.getdminfo().values() if "PROBE" in dm["COLUMNS"]) == standard["NAME"]
    first_manager = f"table.f{standard['SEQNR']}"
    after = contents(path)
    assert {name for name in before if before[name] != after[name]} == {
        "table.dat", first_manager
    }
    assert tool.addcols(
        {"PROBE_VEC": {"valueType": "float", "ndim": 1, "shape": [2]}},
        {"*1": {"TYPE": "TiledColumnStMan", "NAME": "TiledProbe",
                "SPEC": {"DEFAULTTILESHAPE": [2, 8]}}},
    )
    assert next(dm["NAME"] for dm in tool.getdminfo().values() if "PROBE_VEC" in dm["COLUMNS"]) == "TiledProbe"
    np.testing.assert_array_equal(tool.getcell("PROBE_VEC", 0), [0.0, 0.0])
    assert tool.close()
    new = contents(path)
    assert {name for name in after if after[name] != new[name]} == {"table.dat"}
    assert tool.open(str(path))
    assert tool.getcell("PROBE", 0) == 0
    np.testing.assert_array_equal(tool.getcell("PROBE_VEC", 1057), [0.0, 0.0])
    tool.done()


def test_remove_rows_from_standard_and_aipsio(tmp_path: Path) -> None:
    path = tmp_path / "mixed.table"
    tool = tools.table()
    assert tool.create(str(path), {"id": {"valueType": "int"}}, nrow=5)
    assert tool.addcols(
        {"mark": {"valueType": "int"}},
        {"TYPE": "StManAipsIO", "NAME": "secondary"},
    )
    assert tool.putcol("id", np.array([0, 1, 2, 3, 4], dtype=np.int32))
    assert tool.putcol("mark", np.array([10, 11, 12, 13, 14], dtype=np.int32))
    before = contents(path)
    assert tool.removerows([0, 2])
    assert tool.nrows() == 3
    np.testing.assert_array_equal(tool.getcol("id"), [1, 3, 4])
    np.testing.assert_array_equal(tool.getcol("mark"), [11, 13, 14])
    assert tool.close()
    assert set(contents(path)) == set(before)
    assert tool.open(str(path))
    assert tool.nrows() == 3
    assert {dm["TYPE"] for dm in tool.getdminfo().values()} == {"StandardStMan", "StManAipsIO"}
    assert next(dm["NAME"] for dm in tool.getdminfo().values() if dm["TYPE"] == "StManAipsIO") == "secondary"
    np.testing.assert_array_equal(tool.getcol("id"), [1, 3, 4])
    np.testing.assert_array_equal(tool.getcol("mark"), [11, 13, 14])
    tool.done()


@pytest.mark.parametrize("kind", ["StandardStMan", "IncrementalStMan"])
def test_added_bucket_manager_keeps_its_settings_after_first_append(tmp_path: Path, kind: str) -> None:
    path = tmp_path / "empty.table"
    tool = tools.table()
    assert tool.create(str(path), {"id": {"valueType": "int"}})
    assert tool.addcols(
        {"probe": {"valueType": "int"}},
        {"TYPE": kind, "NAME": "Custom", "SPEC": {"BUCKETSIZE": 4096, "PERSCACHESIZE": 7}},
    )
    assert tool.addrows(2)
    assert tool.close()
    assert tool.open(str(path))
    manager = next(dm for dm in tool.getdminfo().values() if dm["NAME"] == "Custom")
    assert manager["SPEC"]["BUCKETSIZE"] == 4096
    assert manager["SPEC"]["PERSCACHESIZE"] == 7
    assert tool.getcol("probe").tolist() == [0, 0]
    tool.done()


def test_append_undefined_tiled_cell(tmp_path: Path) -> None:
    path = tmp_path / "tiled-cell.table"
    tool = tools.table()
    assert tool.create(str(path), {"id": {"valueType": "int"}}, nrow=1)
    assert tool.addcols(
        {"VARY": {"valueType": "double", "ndim": 1}},
        {"TYPE": "TiledCellStMan", "NAME": "Variable"},
    )
    assert tool.addrows(1)
    assert tool.close()
    assert tool.open(str(path))
    assert tool.nrows() == 2
    assert not tool.iscelldefined("VARY", 0)
    assert not tool.iscelldefined("VARY", 1)
    tool.done()


def test_add_direct_column_wider_than_one_shared_bucket_stream(tmp_path: Path) -> None:
    path = tmp_path / "bucket-streams.table"
    tool = tools.table()
    assert tool.create(str(path), {"base": {"valueType": "int"}}, nrow=4)
    assert tool.addcols(
        {"probe": {"valueType": "int"}},
        {"TYPE": "StandardStMan", "NAME": "Small", "SPEC": {"BUCKETSIZE": 128}},
    )
    # Close the original manager, so the first eligible manager is Small.
    assert tool.removecols(["base"])
    assert tool.putcol("probe", [11, 22, 33, 44])
    assert tool.addcols(
        {"wide": {"valueType": "float", "ndim": 1, "shape": [32], "option": 1}},
    )
    assert tool.putcell("wide", 2, np.arange(32, dtype=np.float32))
    assert tool.addrows(1)
    assert tool.removerows([0])
    assert tool.close()
    assert tool.open(str(path))
    assert tool.getcol("probe").tolist() == [22, 33, 44, 0]
    np.testing.assert_array_equal(tool.getcell("wide", 1), np.arange(32, dtype=np.float32))
    np.testing.assert_array_equal(tool.getcell("wide", 3), np.zeros(32, dtype=np.float32))
    assert next(iter(tool.getdminfo().values()))["SPEC"]["BUCKETSIZE"] == 128
    tool.done()


def test_bucket_settings_are_checked_and_cache_setting_stands_alone(tmp_path: Path) -> None:
    path = tmp_path / "managers.table"
    tool = tools.table()
    assert tool.create(str(path), {"id": {"valueType": "int"}}, nrow=2)
    before = contents(path)
    with pytest.raises(RuntimeError, match="bucket size"):
        tool.addcols(
            {"rejected": {"valueType": "int"}},
            {"TYPE": "IncrementalStMan", "NAME": "Tiny", "SPEC": {"BUCKETSIZE": 1}},
        )
    assert contents(path) == before
    assert tool.addcols(
        {"probe": {"valueType": "int"}},
        {"TYPE": "StandardStMan", "NAME": "Cached", "SPEC": {"PERSCACHESIZE": 7}},
    )
    manager = next(dm for dm in tool.getdminfo().values() if dm["NAME"] == "Cached")
    assert manager["SPEC"]["PERSCACHESIZE"] == 7
    tool.done()


def test_append_to_casa_tiled_column_extends_row_coordinate(tmp_path: Path) -> None:
    """CASA 6.7.6.14 created this tiled VALUE column and its TIME coordinate."""
    fixture = Path(__file__).with_name("data") / "tiled_column_coords.tgz"
    with tarfile.open(fixture, "r:gz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    path = tmp_path / "coord.table"
    tool = tools.table()
    assert tool.open(str(path), nomodify=False)
    assert tool.addrows(1)
    assert tool.close()
    assert tool.open(str(path))
    assert tool.nrows() == 3
    assert tool.getdminfo()["*1"]["NAME"] == "WithTime"
    cube = tool.getdminfo()["*1"]["SPEC"]["HYPERCUBES"]["*1"]
    np.testing.assert_array_equal(cube["ID"]["TIME"], [10.5, 21.5, 0.0])
    np.testing.assert_array_equal(tool.getcell("VALUE", 1), [2.0, 3.0])
    np.testing.assert_array_equal(tool.getcell("VALUE", 2), [0.0, 0.0])
    tool.done()


def test_packed_tiled_putcol_publishes_complete_tiles(tmp_path: Path) -> None:
    path = tmp_path / "tiled-complex.table"
    tool = tools.table()
    assert tool.create(str(path), DESCRIPTION, nrow=6)
    assert tool.addcols(
        {"DATA": {"valueType": "complex", "ndim": 2, "shape": [4, 6], "option": 4}},
        {
            "TYPE": "TiledShapeStMan",
            "NAME": "TiledData",
            "SPEC": {"DEFAULTTILESHAPE": [2, 3, 2]},
        },
    )
    assert tool.putcol("id", np.arange(6, dtype=np.int32))
    assert tool.close()

    # Keep a reader open across the write and prime its cell and tile caches.
    reader = tools.table()
    assert reader.open(str(path))
    np.testing.assert_array_equal(reader.getcell("DATA", 2), np.zeros((4, 6)))
    assert tool.open(str(path), nomodify=False)
    np.testing.assert_array_equal(tool.getcell("DATA", 2), np.zeros((4, 6)))

    values = np.arange(48, dtype=np.float32).reshape(4, 6, 2)
    packed = np.asfortranarray(values - (values + 1) * np.complex64(1j))
    assert tool.putcol("DATA", packed, startrow=2, nrow=2)
    for handle in [tool, reader]:
        np.testing.assert_array_equal(handle.getcell("DATA", 2), packed[..., 0])
        all_rows = handle.getcol("DATA")
        np.testing.assert_array_equal(all_rows[..., 2:4], packed)
        np.testing.assert_array_equal(all_rows[..., [0, 1, 4, 5]], np.zeros((4, 6, 4)))
        np.testing.assert_array_equal(handle.getcol("id"), np.arange(6))

    # A Fortran-contiguous input can still require the partial-tile writer.
    partial = np.asfortranarray(packed[..., :1] * np.complex64(2 - 3j))
    assert tool.putcol("DATA", partial, startrow=3, nrow=1)
    np.testing.assert_array_equal(
        reader.getcol("DATA")[..., 2:4],
        np.concatenate([packed[..., :1], partial], axis=2),
    )

    # A noncontiguous NumPy input also uses ordinary writes.
    assert tool.putcol("DATA", packed[::-1, :, :1], startrow=3, nrow=1)
    np.testing.assert_array_equal(reader.getcell("DATA", 3), packed[::-1, :, 0])
    with pytest.raises(RuntimeError, match="conformance"):
        tool.putcol("DATA", packed[:2].copy(order="F"), startrow=2, nrow=2)
    np.testing.assert_array_equal(reader.getcell("DATA", 2), packed[..., 0])
    assert tool.close()
    assert reader.close()
