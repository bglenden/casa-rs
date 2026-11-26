"""Parity of legacy integration weights, flag preservation and persisted MAIN cells."""

from contextlib import closing
from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset
from test_tools_simulator import configure


OUTPUT = ("WEIGHT", "SIGMA", "FLAG", "FLAG_ROW", "DATA")


def read_columns(api, path):
    with closing(api.table()) as table:
        assert table.open(str(path))
        return {column: table.getcol(column) for column in OUTPUT}


@pytest.mark.parametrize("permanent", [False, True])
def test_oldstatwt_refuses_to_write_a_replaced_attachment(
    baseline_fixture, native_dataset, tmp_path, permanent,
):
    casa, _, _ = baseline_fixture
    replacement = tmp_path / "replacement.ms"
    copytree(native_dataset, replacement)
    initial = read_columns(casa, replacement)
    moved = tmp_path / "original.ms"
    with closing(tools.ms()) as ms:
        assert ms.open(str(native_dataset), nomodify=False, lock=permanent)
        native_dataset.rename(moved)
        replacement.rename(native_dataset)
        with pytest.raises(RuntimeError, match="original MeasurementSet attachment"):
            ms.oldstatwt(datacolumn="data")
        assert ms.name() == str(native_dataset)
    for path in (native_dataset, moved):
        for name, values in read_columns(casa, path).items():
            np.testing.assert_array_equal(initial[name], values, strict=True, err_msg=name)


def test_oldstatwt_revalidates_the_retained_lock(baseline_fixture, native_dataset, tmp_path):
    casa, _, _ = baseline_fixture
    initial = read_columns(casa, native_dataset)
    with closing(tools.ms()) as ms:
        assert ms.open(str(native_dataset), nomodify=False, lock=True)
        lock = native_dataset / "table.lock"
        replaced = tmp_path / "detached.lock"
        lock.rename(replaced)
        lock.write_bytes(replaced.read_bytes())
        with pytest.raises(RuntimeError, match="physical table or write lock changed"):
            ms.oldstatwt(datacolumn="data")
    for name, values in read_columns(casa, native_dataset).items():
        np.testing.assert_array_equal(initial[name], values, strict=True, err_msg=name)


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


@pytest.mark.parametrize("via", ["msselect", "selecttaql", "empty"])
def test_oldstatwt_only_updates_the_active_selection(baseline_fixture, native_dataset, tmp_path, via):
    casa, _, raw = baseline_fixture
    original = read_columns(casa, native_dataset)
    selected = (raw["antenna1"] == 0) & (raw["antenna2"] == 1) & (via != "empty")
    if selected.any():
        reference = tmp_path / "reference.ms"
        copytree(native_dataset, reference)
        with closing(casa.ms()) as tool:
            assert tool.open(str(reference), nomodify=False)
            assert tool.oldstatwt(datacolumn="data", antenna="0&1")
        expected = read_columns(casa, reference)
    expression = "ANTENNA1==0 && ANTENNA2==1" if selected.any() else "ROWID()<0"
    for api, name in ((casa, "oracle"), (tools, "native")):
        path = tmp_path / f"{name}.ms"
        copytree(native_dataset, path)
        with closing(api.ms()) as tool:
            assert tool.open(str(path), nomodify=False)
            ok = tool.selecttaql(expression) if via == "selecttaql" else tool.msselect({"taql": expression})
            assert ok is bool(selected.any())
            assert tool.nrow(True) == np.count_nonzero(selected)
            if api is casa:
                with pytest.raises(RuntimeError, match="does not exist"):
                    tool.oldstatwt(datacolumn="data")
            else:
                assert tool.oldstatwt(datacolumn="data") is bool(selected.any())
            assert tool.name() == str(path) and tool.nrow(True) == np.count_nonzero(selected)
        persisted = read_columns(casa, path)
        for name, initial in original.items():
            wanted = initial.copy()
            if api is tools and selected.any() and name in ("WEIGHT", "SIGMA"):
                wanted[..., selected] = expected[name][..., selected]
            np.testing.assert_allclose(persisted[name], wanted, strict=True, rtol=2e-6, atol=0,
                                       err_msg=name)


@pytest.mark.parametrize("float_only", [False, True])
def test_oldstatwt_observed_prefers_float_data(baseline_fixture, native_dataset, tmp_path, float_only):
    casa, _, raw = baseline_fixture
    values = (raw["data"].real * 7 - 33).astype(np.float32)
    saved = []
    for api, name in ((casa, "oracle"), (tools, "native")):
        path = tmp_path / f"{name}.ms"
        copytree(native_dataset, path)
        with closing(casa.table()) as table:
            assert table.open(str(path), nomodify=False)
            assert table.addcols({"FLOAT_DATA": {**table.getcoldesc("DATA"), "valueType": "float"}})
            assert table.putcol("FLOAT_DATA", values)
            if float_only:
                assert table.removecols("DATA")
        with closing(api.ms()) as tool:
            assert tool.open(str(path), nomodify=False)
            assert tool.oldstatwt(datacolumn="data")
            with pytest.raises(RuntimeError, match="FLOAT_DATA"):
                tool.oldstatwt(datacolumn="FLOAT_DATA")
        with closing(casa.table()) as table:
            assert table.open(str(path))
            np.testing.assert_array_equal(table.getcol("FLOAT_DATA"), values.astype(np.float64), strict=True)
            saved.append([table.getcol(name) for name in ("WEIGHT", "SIGMA")])
    for actual, expected in zip(saved[1], saved[0]):
        np.testing.assert_allclose(actual, expected, strict=True, rtol=2e-6, atol=0)


def test_oldstatwt_returns_success_for_unused_fit_and_output_windows(tmp_path, measures):
    casa = pinned_casa()
    path = tmp_path / "only-lower.ms"
    with closing(tools.simulator()) as sm:
        assert sm.open(str(path))
        configure(sm)
        assert sm.setspwindow(spwname="H", freq="1.5GHz", deltafreq="1MHz",
                              freqresolution="1MHz", nchannels=4, stokes="RR LL")
        assert sm.observe("F", "L", starttime="0s", stoptime="20s")
    initial = read_columns(casa, path)
    for api in (casa, tools):
        with closing(api.ms()) as tool:
            assert tool.open(str(path), nomodify=False)
            assert tool.oldstatwt(datacolumn="data", fitspw="1", spw="1") is True
        for name, values in read_columns(casa, path).items():
            np.testing.assert_array_equal(initial[name], values, strict=True, err_msg=name)


def sparse_integrations(casa, parent, output, polarizations, windows, times):
    """Make a materialized MS with the specified data descriptions and times."""
    with closing(tools.simulator()) as sm:
        assert sm.open(str(parent))
        configure(sm)
        for index, stokes in enumerate(polarizations, 1):
            assert sm.setspwindow(spwname=f"band{index}", freq=f"{1.4 + index / 10}GHz",
                                  deltafreq="1MHz", freqresolution="1MHz", nchannels=4, stokes=stokes)
        for index in range(len(polarizations) + 1):
            assert sm.observe("F", "L" if index == 0 else f"band{index}",
                              starttime=f"{10 * index}s", stoptime=f"{10 * (index + 1)}s")
    with closing(casa.table()) as table:
        assert table.open(str(parent))
        description = table.getcol("DATA_DESC_ID")
        antennas = (table.getcol("ANTENNA1") == 0) & (table.getcol("ANTENNA2") == 1)
        rows = np.array([np.flatnonzero((description == window) & antennas)[0] for window in windows],
                        dtype=np.int64)
        selection = parent.with_name("selection.ms")
        assert table.selectrows(rows, str(selection)).done()
        assert table.done() and table.open(str(selection))
        with closing(table.copy(str(output), deep=True, valuecopy=True, returnobject=True)):
            pass
    with closing(casa.table()) as table:
        assert table.open(str(output), nomodify=False)
        assert table.putcol("SCAN_NUMBER", np.ones(len(rows), dtype=np.int32))
        for column in ("TIME", "TIME_CENTROID"):
            assert table.putcol(column, table.getcell(column, 0) + np.asarray(times))
        shape = (2, 4, len(rows))
        data = np.arange(1, 1 + np.prod(shape), dtype=np.float32).reshape(shape, order="F")
        assert table.putcol("DATA", data.astype(np.complex64))
        assert table.putcol("FLAG", np.zeros(shape, dtype=bool))
        assert table.putcol("FLAG_ROW", np.zeros(len(rows), dtype=bool))


def test_non_fit_window_still_breaks_a_legacy_integration(tmp_path, measures):
    casa = pinned_casa()
    template = tmp_path / "template.ms"
    sparse_integrations(casa, tmp_path / "source.ms", template, ("RR LL", "RR LL"),
                        (0, 1, 2), (0, 10, 0))
    initial = read_columns(casa, template)
    for api, name in ((casa, "oracle"), (tools, "native")):
        path = tmp_path / f"{name}.ms"
        copytree(template, path)
        with closing(api.ms()) as tool:
            assert tool.open(str(path), nomodify=False)
            assert tool.oldstatwt(datacolumn="data", fitspw="0", spw="2")
        saved = read_columns(casa, path)
        for column, missing in (("WEIGHT", 0.0), ("SIGMA", -1.0)):
            np.testing.assert_array_equal(saved[column][..., :2], initial[column][..., :2], strict=True)
            np.testing.assert_array_equal(saved[column][..., 2], np.full(2, missing), strict=True)


def test_incompatible_correlation_setups_fail_before_any_native_weights_change(tmp_path, measures):
    casa = pinned_casa()
    template = tmp_path / "native.ms"
    sparse_integrations(casa, tmp_path / "source.ms", template, ("XX YY",), (0, 0, 1), (0, 10, 10))
    initial = read_columns(casa, template)
    oracle = tmp_path / "oracle.ms"
    copytree(template, oracle)
    with closing(casa.ms()) as tool:
        assert tool.open(str(oracle), nomodify=False)
        assert tool.oldstatwt(datacolumn="data", combine="spw") is True
    partial = read_columns(casa, oracle)
    assert np.any(partial["WEIGHT"][..., 0] != initial["WEIGHT"][..., 0])
    np.testing.assert_array_equal(partial["WEIGHT"][..., 1:], initial["WEIGHT"][..., 1:], strict=True)
    with closing(tools.ms()) as tool:
        assert tool.open(str(template), nomodify=False)
        with pytest.raises(RuntimeError, match="different correlation setups"):
            tool.oldstatwt(datacolumn="data", combine="spw")
    for name, values in read_columns(casa, template).items():
        np.testing.assert_array_equal(initial[name], values, strict=True, err_msg=name)


def test_reversed_antenna_baselines_remain_independent(tmp_path, measures):
    casa = pinned_casa()
    template = tmp_path / "template.ms"
    sparse_integrations(casa, tmp_path / "source.ms", template, (), (0, 0), (0, 0))
    with closing(casa.table()) as table:
        assert table.open(str(template), nomodify=False)
        assert table.putcol("ANTENNA1", np.array([3, 2], dtype=np.int32))
        assert table.putcol("ANTENNA2", np.array([1, 3], dtype=np.int32))
        assert table.putcol("DATA", np.broadcast_to(np.array([1.0, 2.0], dtype=np.complex64), (2, 4, 2)))
    for api, name, expected in ((casa, "oracle", [0.75, 0.75]), (tools, "native", [1.75, 0.4375])):
        path = tmp_path / f"{name}.ms"
        copytree(template, path)
        with closing(api.ms()) as tool:
            assert tool.open(str(path), nomodify=False)
            assert tool.oldstatwt(datacolumn="data", dorms=True)
        np.testing.assert_array_equal(read_columns(casa, path)["WEIGHT"], [expected, expected], strict=True)


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
