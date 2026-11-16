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
CASA_TABLES_DATA = REPO_ROOT / "crates/casa-tables/tests/data"


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
