"""Exercise casacore MultiFile output through the legacy MS selector."""

from contextlib import closing
from importlib.util import find_spec
from pathlib import Path
import os
import struct
import subprocess
import sys

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


@pytest.mark.parametrize(
    "mode,endian,blocksize",
    [("plain", "big", 4096), ("scratch", "little", 8192), ("plain", "little", None)],
)
def test_legacy_taql_multifile_outputs_cross_read_and_preserve_edits(
    native_dataset, tmp_path, mode, endian, blocksize,
):
    casa = pinned_casa()
    columns = ("DATA", "FLAG", "FLAG_ROW", "UVW", "WEIGHT", "SIGMA")
    with closing(casa.table()) as source:
        assert source.open(str(native_dataset))
        source_rows = source.nrows()
        expected = {column: source.getcol(column, 3, 4)[..., ::-1] for column in columns}
        initial_flags = source.getcol("FLAG")
    fields = f'type="{mode}",storage="multifile",endian="{endian}"'
    if blocksize is not None:
        fields += f",blocksize={blocksize} AS I4"
    for writer in (casa, tools):
        target = tmp_path / f"{writer.__name__}.ms"
        with closing(writer.ms()) as selector:
            assert selector.open(str(native_dataset), nomodify=False)
            assert selector.selectold({"rows": [3, 4, 5, 6]})
            predicate = 'True ORDERBY ROWID() DESC GIVING'
            query = f'{predicate} "{target}" AS [{fields},overwrite=false]'
            assert selector.selecttaqlold(query)
            assert selector.nrow() == source_rows and selector.nrowold(True) == 4
            np.testing.assert_array_equal(selector.rangeold("rows")["rows"], np.arange(4))
            assert selector.selectinitold(reset=True)
            assert selector.selectold({"rows": [3, 4, 5, 6]})
            with pytest.raises(RuntimeError):
                selector.selecttaqlold(query)
            np.testing.assert_array_equal(selector.rangeold("rows")["rows"], [3, 4, 5, 6])
        header = target / "table.mf"
        with header.open("rb") as stream:
            assert struct.unpack_from(">Q", stream.read(64), 40)[0] == (blocksize or 4 * 1024 * 1024)
        moved = target.rename(tmp_path / f"{writer.__name__}-moved.ms")
        for reader in (casa, tools):
            with closing(reader.table()) as table, closing(reader.table()) as antenna:
                assert table.open(str(moved)) and table.endianformat() == endian
                for column, values in expected.items():
                    np.testing.assert_array_equal(table.getcol(column), values, strict=True)
                link = table.getkeyword("ANTENNA").removeprefix("Table: ")
                assert Path(link) == moved / "ANTENNA" and antenna.open(link)
        with closing(tools.ms()) as selector:
            assert selector.open(str(moved), nomodify=False)
            current = selector.getdataold(["flag", "flag_row"])
            updates = {field: np.logical_not(array) for field, array in current.items()}
            assert selector.putdataold(updates)
        for reader in (casa, tools):
            with closing(reader.table()) as table:
                assert table.open(str(moved))
                for field, values in updates.items():
                    np.testing.assert_array_equal(table.getcol(field.upper()), values, strict=True)
    with closing(casa.table()) as source:
        assert source.open(str(native_dataset))
        np.testing.assert_array_equal(source.getcol("FLAG"), initial_flags, strict=True)


@pytest.mark.parametrize("module", ["casars.tools", "casatools"])
def test_legacy_taql_multifile_uses_resource_default_block_size(native_dataset, tmp_path, module):
    interpreter = sys.executable
    if module == "casatools":
        oracle = os.environ.get("CASA_RS_CASA_PYTHON")
        if oracle:
            interpreter = oracle
        elif find_spec("casatools") is None:
            pytest.skip("CASA oracle is unavailable")
    resources = tmp_path / "casarc"
    resources.write_text("table.storage.option: multifile\ntable.storage.blocksize: 8192\n")
    runner = """
from contextlib import closing
from importlib import import_module
import sys

module = import_module(sys.argv[1])
if sys.argv[1] == "casatools":
    assert module.version_string().replace("-", ".") == "6.7.6.14"
with closing(module.ms()) as selector:
    assert selector.open(sys.argv[2])
    assert selector.selecttaqlold(f'True LIMIT 2 GIVING "{sys.argv[3]}" AS PLAIN')
"""
    output = tmp_path / f"{module}.ms"
    subprocess.run(
        [interpreter, "-c", runner, module, str(native_dataset), str(output)],
        env=dict(os.environ, CASARCFILES=str(resources)),
        check=True,
    )
    with (output / "table.mf").open("rb") as stream:
        assert struct.unpack_from(">Q", stream.read(64), 40)[0] == 8192


def test_legacy_taql_memory_to_multifile_and_failed_hdf5_preserve_selection(
    native_dataset, tmp_path,
):
    casa = pinned_casa()
    with closing(casa.table()) as source:
        assert source.open(str(native_dataset))
        source_rows = source.nrows()
        expected = source.getcol("DATA", source_rows - 5, 5)[..., ::-1][..., 1:3]
    for writer in (casa, tools):
        output = tmp_path / f"{writer.__name__}.ms"
        with closing(writer.ms()) as selector:
            assert selector.open(str(native_dataset))
            assert selector.selecttaqlold("True ORDERBY ROWID() DESC LIMIT 5 GIVING AS MEMORY")
            query = f'True LIMIT 2 OFFSET 1 GIVING "{output}" AS '
            with pytest.raises(RuntimeError, match="(?i)hdf5"):
                selector.selecttaqlold(f'{query}[type="plain",storage="multihdf5"]')
            assert not output.exists() and selector.nrowold(True) == 5
            assert selector.selecttaqlold(
                f'{query}[type="plain",storage="multifile",blocksize=64]'
            )
            assert selector.nrow() == source_rows and selector.nrowold(True) == 2
            np.testing.assert_array_equal(selector.rangeold("rows")["rows"], [0, 1])
            np.testing.assert_array_equal(
                selector.getdataold(["data"])["data"], expected.astype(np.complex128), strict=True,
            )
        with (output / "table.mf").open("rb") as stream:
            first_continuation = struct.unpack_from(">Q", stream.read(64), 8)[0]
            assert first_continuation > 0
        for reader in (casa, tools):
            with closing(reader.table()) as table:
                assert table.open(str(output))
                np.testing.assert_array_equal(table.getcol("DATA"), expected, strict=True)


def test_legacy_taql_multifile_relocks_existing_casa_reader(native_dataset, tmp_path):
    casa = pinned_casa()
    output = tmp_path / "packed.ms"
    with closing(casa.ms()) as writer:
        assert writer.open(str(native_dataset))
        assert writer.selecttaqlold(
            f'True LIMIT 3 GIVING "{output}" AS [type="plain",storage="multifile",blocksize=64]'
        )
    editor = """
from contextlib import closing
import sys
import numpy as np
from casars import tools
with closing(tools.ms()) as selector:
    assert selector.open(sys.argv[1], nomodify=False)
    data = selector.getdataold(['flag', 'flag_row'])
    assert selector.putdataold({name: np.logical_not(value) for name, value in data.items()})
"""
    with closing(casa.table()) as retained:
        assert retained.open(str(output), nomodify=False, lockoptions={"option": "user"})
        assert retained.lock(write=False, nattempts=1)
        data = retained.getcol("DATA")
        flags = retained.getcol("FLAG")
        saved_rows = retained.getcol("FLAG_ROW")
        retained.unlock()
        subprocess.run([sys.executable, "-c", editor, str(output)], check=True, timeout=30)
        assert retained.lock(write=True, nattempts=1)
        np.testing.assert_array_equal(retained.getcol("DATA"), data, strict=True)
        np.testing.assert_array_equal(retained.getcol("FLAG"), np.logical_not(flags), strict=True)
        np.testing.assert_array_equal(
            retained.getcol("FLAG_ROW"), np.logical_not(saved_rows), strict=True,
        )
        retained.putcell("FLAG_ROW", 1, bool(saved_rows[1]))
        retained.unlock()
    with closing(tools.table()) as reader:
        assert reader.open(str(output))
        expected = np.logical_not(saved_rows)
        expected[1] = saved_rows[1]
        np.testing.assert_array_equal(reader.getcol("FLAG_ROW"), expected, strict=True)


def test_legacy_taql_multifile_respects_read_only_archive(native_dataset, tmp_path):
    casa = pinned_casa()
    output = tmp_path / "packed.ms"
    with closing(tools.ms()) as writer:
        assert writer.open(str(native_dataset))
        assert writer.selecttaqlold(
            f'True LIMIT 3 GIVING "{output}" AS [type="plain",storage="multifile",blocksize=64]'
        )
    archive = output / "table.mf"
    contents = archive.read_bytes()
    mode = archive.stat().st_mode
    with closing(tools.ms()) as held:
        assert held.open(str(output), nomodify=False)
        archive.chmod(0o444)
        try:
            if os.access(archive, os.W_OK):
                pytest.skip("the effective user can write a mode-0444 file")
            for engine in (casa, tools):
                with closing(engine.ms()) as reader:
                    assert reader.open(str(output))
                    assert reader.nrow() == 3
                    reader.close()
                    with pytest.raises(RuntimeError, match="(?i)permission denied"):
                        reader.open(str(output), nomodify=False)
            with pytest.raises(RuntimeError, match="(?i)permission denied"):
                held.putdataold({"flag_row": np.ones(3, dtype=bool)})
            assert archive.read_bytes() == contents
        finally:
            archive.chmod(mode)
