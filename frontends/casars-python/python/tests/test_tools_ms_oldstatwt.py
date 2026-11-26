"""Parity of legacy integration weights, flag preservation and persisted MAIN cells."""

from contextlib import closing
from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


OUTPUT = ("WEIGHT", "SIGMA", "FLAG", "FLAG_ROW", "DATA")


def read_columns(api, path):
    with closing(api.table()) as table:
        assert table.open(str(path))
        return {column: table.getcol(column) for column in OUTPUT}


@pytest.mark.parametrize("options", [
    {"datacolumn": "data"},
    {"datacolumn": "OBSERVED", "dorms": True, "minsamp": 0, "fitspw": "0:0~2"},
    {"datacolumn": "data", "fitspw": "0:0~3^2", "byantenna": False, "sepacs": False,
     "fitcorr": ["RR"], "correlation": "LL", "minsamp": 2, "timebin": "10Hz"},
    {"datacolumn": "data", "field": ["F"], "spw": "0:0", "scan": [1], "array": 0,
     "obs": "0", "antenna": "0&1", "combine": "obs,scan,state,spw", "timebin": 25},
    {"datacolumn": "unknown", "fitspw": [-1], "minsamp": -1},
    {"datacolumn": "model", "dorms": True},
    {},
])
def test_oldstatwt_fits_and_persists_weights(baseline_fixture, native_dataset, tmp_path, options):
    casa, _, expected = baseline_fixture
    with closing(casa.table()) as table:
        assert table.open(str(native_dataset), nomodify=False)
        spec = table.getcoldesc("DATA")
        assert table.addcols({"CORRECTED_DATA": spec})
        assert table.putcol("CORRECTED_DATA", expected["data"] * -2.5j)
    initial = read_columns(casa, native_dataset)
    paths = [tmp_path / f"{backend}.ms" for backend in ("reference", "native")]
    results = []
    for api, path in zip((casa, tools), paths):
        copytree(native_dataset, path)
        with closing(api.ms()) as tool:
            assert tool.open(str(path), nomodify=False)
            arguments = options
            if api is casa and options.get("fitspw") == "0:0~3^2":
                # Enumerate the intended [0, 2] mask because CASA's legacy
                # strided-slice implementation runs past the window.
                arguments = options | {"fitspw": "0:0;2"}
            assert tool.oldstatwt(**arguments) is True
            assert tool.name() == str(path)
            in_place = tool.getdata(["weight", "sigma"])
        columns = read_columns(casa, path)
        for name in ("WEIGHT", "SIGMA"):
            values = columns[name]
            assert values.dtype == np.float64
            assert values.shape == initial[name].shape
            np.testing.assert_array_equal(values, in_place[name.lower()], strict=True)
        for name in ("FLAG_ROW", "DATA"):
            np.testing.assert_array_equal(columns[name], initial[name], strict=True)
        wanted_flags = initial["FLAG"].copy()
        if api is casa:
            rows = slice(None)
            if "antenna" in options:
                rows = (expected["antenna1"] == 0) & (expected["antenna2"] == 1)
            # CASA collapses the two correlation flags with OR and writes
            # that result to both correlations on every selected row.
            wanted_flags[..., rows] = wanted_flags[..., rows].any(axis=0)
        np.testing.assert_array_equal(columns["FLAG"], wanted_flags, strict=True)
        for name, values in read_columns(tools, path).items():
            np.testing.assert_array_equal(values, columns[name], strict=True)
        results.append(columns)
    for name in ("WEIGHT", "SIGMA", "FLAG_ROW", "DATA"):
        np.testing.assert_allclose(results[1][name], results[0][name], rtol=2e-6, atol=0,
                                   strict=True, err_msg=name)


def test_oldstatwt_errors_preserve_attachment_and_main_table(baseline_fixture, native_dataset):
    casa, _, _ = baseline_fixture
    before = read_columns(casa, native_dataset)
    with closing(tools.ms()) as tool:
        with pytest.raises(RuntimeError, match="Null table object"):
            tool.oldstatwt(datacolumn="data")
        assert tool.open(str(native_dataset))
        with pytest.raises(RuntimeError, match="not writable"):
            tool.oldstatwt(datacolumn="data")
        assert tool.name() == str(native_dataset)
        assert not tool.iswritable()
    for api in (casa, tools):
        with closing(api.ms()) as tool:
            assert tool.open(str(native_dataset), nomodify=False)
            assert tool.oldstatwt(datacolumn="data", scan="9999") is False
            for args in ({"field": "absent"}, {"correlation": "Q"}, {"timebin": "bad"},
                         {"fitspw": "21"}, {"datacolumn": "FLOAT_DATA"},
                         {"datacolumn": "LAG_DATA"}):
                with pytest.raises(RuntimeError):
                    tool.oldstatwt(**({"datacolumn": "data"} | args))
            assert tool.name() == str(native_dataset)
    with closing(tools.ms()) as tool:
        assert tool.open(str(native_dataset), nomodify=False)
        with pytest.raises(RuntimeError, match="CORRECTED_DATA"):
            tool.oldstatwt()
        assert tool.name() == str(native_dataset)
    for name, values in read_columns(casa, native_dataset).items():
        np.testing.assert_array_equal(before[name], values, strict=True)


@pytest.mark.parametrize("permanent", [False, True], ids=["automatic", "permanent"])
@pytest.mark.parametrize("multiple", [False, True], ids=["single-ms", "multi-ms"])
def test_oldstatwt_saves_only_the_reference_rows(
    baseline_fixture, native_dataset, tmp_path, permanent, multiple,
):
    casa, _, raw = baseline_fixture
    row_count = raw["data"].shape[-1]
    members = [str(native_dataset)]
    parent = str(native_dataset)
    if multiple:
        second = tmp_path / "second.ms"
        copytree(native_dataset, second)
        with closing(casa.table()) as table:
            assert table.open(str(second), nomodify=False)
            assert table.putcol("DATA", raw["data"] * (-2.5j))
        parent = str(tmp_path / "parent.mms")
        with closing(casa.ms()) as tool:
            assert tool.createmultims(parent, [str(native_dataset), str(second)],
                                      ["ANTENNA", "DATA_DESCRIPTION", "FEED"])
        with closing(casa.ms()) as tool:
            assert tool.open(parent)
            members = tool.getreferencedtables()
    selected = [13, 1, 22, 22, 6] if not multiple else [row_count + 13, 1, 22, row_count + 13, 6]
    reference = str(tmp_path / "saved.ms")
    materialized = tmp_path / "materialized.ms"
    with closing(casa.table()) as table:
        assert table.open(parent)
        assert table.selectrows(np.array(selected, dtype=np.int64), reference).done()
        assert table.done()
        assert table.open(reference)
        with closing(table.copy(str(materialized), deep=True, valuecopy=True, returnobject=True)) as copied:
            if "SORTED_TABLE" in copied.keywordnames():
                assert copied.removekeyword("SORTED_TABLE")
    original = [read_columns(casa, member) for member in members]
    options = {"datacolumn": "data", "fitspw": "0:1~3", "minsamp": 2}
    with closing(casa.ms()) as tool:
        assert tool.open(str(materialized), nomodify=False)
        assert tool.oldstatwt(**options)
    expected = read_columns(casa, materialized)
    with closing(tools.ms()) as tool:
        assert tool.open(reference, nomodify=False, lock=permanent)
        assert tool.oldstatwt(**options)
        assert tool.name() == reference
        assert tool.ismultims() == multiple
        assert tool.nrow() == len(selected)
    for reader in (casa, tools):
        for index, member in enumerate(members):
            persisted = read_columns(reader, member)
            for name in ("WEIGHT", "SIGMA"):
                wanted = original[index][name].copy()
                for position, row in enumerate(selected):
                    part, local = divmod(row, row_count)
                    if part == index:
                        wanted[..., local] = expected[name][..., position]
                np.testing.assert_allclose(persisted[name], wanted, strict=True, rtol=2e-6, atol=0)
            for name in ("DATA", "FLAG", "FLAG_ROW"):
                np.testing.assert_array_equal(persisted[name], original[index][name], strict=True)


def test_oldstatwt_combines_simultaneous_rows_from_multi_ms_members(
    baseline_fixture, native_dataset, tmp_path,
):
    casa, _, initial = baseline_fixture
    options = {"datacolumn": "data", "fitspw": "0:1~3", "minsamp": 2}
    snapshots = []
    for api, engine in ((casa, "reference"), (tools, "native")):
        members = [tmp_path / f"{engine}-{index}.ms" for index in range(2)]
        for member in members:
            copytree(native_dataset, member)
        with closing(casa.table()) as table:
            assert table.open(str(members[1]), nomodify=False)
            assert table.putcol("DATA", (2 - 3j) * initial["data"])
        output = tmp_path / f"{engine}.mms"
        with closing(casa.ms()) as tool:
            assert tool.createmultims(str(output), list(map(str, members)),
                                      ["ANTENNA", "DATA_DESCRIPTION", "FEED"])
        with closing(api.ms()) as tool:
            assert tool.open(str(output), nomodify=False)
            assert tool.oldstatwt(**options)
            assert tool.ismultims()
            physical = tool.getreferencedtables()
        snapshots.append([read_columns(casa, member) for member in physical])
        for index, member in enumerate(physical):
            other = read_columns(tools, member)
            for name, values in snapshots[-1][index].items():
                np.testing.assert_array_equal(values, other[name], strict=True)
    for index in range(2):
        for name in ("WEIGHT", "SIGMA", "FLAG_ROW", "DATA"):
            np.testing.assert_allclose(snapshots[1][index][name], snapshots[0][index][name],
                                       strict=True, rtol=2e-6, atol=0)
    np.testing.assert_array_equal(snapshots[1][0]["FLAG"], initial["flag"], strict=True)
    np.testing.assert_array_equal(snapshots[1][1]["FLAG"], initial["flag"], strict=True)
