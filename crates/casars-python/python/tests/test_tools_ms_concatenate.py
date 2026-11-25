"""Physical MeasurementSet concatenation and persisted native/CASA exchange."""

from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_tools_ms_createmultims import pinned_casa
from test_tools_ms_testconcatenate import concat_api, copy_inputs, counts
from test_tools_msmetadata import measures, native_dataset


def main_cells(api, path, row):
    table = api.table()
    assert table.open(str(path))
    columns = ("DATA", "FLAG", "WEIGHT", "SIGMA", "UVW", "SCAN_NUMBER", "ANTENNA1", "ANTENNA2")
    result = {column: table.getcell(column, row) for column in columns}
    assert table.close()
    return result


@pytest.mark.parametrize("named", [False, True])
@pytest.mark.parametrize("mode", [0, 1, 2, 3])
def test_physical_modes_and_named_replacement(native_dataset, tmp_path, concat_api, named, mode):
    first, donor = copy_inputs(native_dataset, tmp_path)
    output = tmp_path / "named.ms"
    if named:
        copytree(native_dataset, output)
    expected = main_cells(tools, donor, 1)
    m = concat_api.ms()
    assert m.concatenate(str(tmp_path / "absent.ms")) is True
    assert m.open(str(first), nomodify=False)
    original = m.nrow()
    options = {
        "handling": mode, "freqtol": "1Hz", "dirtol": "1mas",
        "destmsfile": str(output) if named else "",
    }
    assert m.concatenate(str(donor), **options) is True
    expected_main = original if named or mode % 2 else original * 2
    assert m.nrow() == expected_main
    assert m.close()
    if named:
        relocated = tmp_path / "relocated.ms"
        output.rename(relocated)
        output = relocated
    for reader in (tools, pinned_casa()):
        assert counts(reader, first, (".", "POINTING")) == [
            expected_main, 24 if mode < 2 else 12,
        ]
        assert counts(reader, donor, (".", "POINTING")) == [original, 12]
        if named:
            assert counts(reader, output, (".", "POINTING")) == [
                original if mode % 2 == 0 else 0, 24 if mode < 2 else 12,
            ]
        if mode % 2 == 0:
            cells = main_cells(reader, output if named else first, 1 if named else original + 1)
            for name, cell in cells.items():
                np.testing.assert_array_equal(cell, expected[name])
                assert np.asarray(cell).dtype == np.asarray(expected[name]).dtype
        history = reader.table()
        assert history.open(str(donor / "HISTORY"))
        last = history.nrows() - 1
        assert history.getcell("ORIGIN", last) == "ms::concatenate()"
        assert history.getcell("MESSAGE", last) == f"{donor} appended to {first}"
        command = history.getcell("CLI_COMMAND", last)
        assert list(command) == [
            f"msfile='{donor}' freqTol='1 Hz' dirTol='1 mas' respectname='0'"
            f" handling= {mode} destmsfile='{options['destmsfile']}'"
        ]
        assert history.close()


def change_table(path, change):
    table = tools.table()
    assert table.open(str(path), nomodify=False)
    change(table)
    assert table.close()


def swap_first_two_antennas(table):
    for column in table.colnames():
        if table.iscelldefined(column, 0) and table.iscelldefined(column, 1):
            a, b = (table.getcell(column, row) for row in (0, 1))
            assert table.putcell(column, 0, b)
            assert table.putcell(column, 1, a)


@pytest.mark.parametrize("mode", ["physical", "virtual", "casa-virtual"])
def test_visibility_axes_consistent_when_both_baseline_and_channels_reverse(
    native_dataset, tmp_path, mode,
):
    first, donor = copy_inputs(native_dataset, tmp_path)
    output = tmp_path / "paired.ms"

    def crosshands(table):
        assert table.putcell("CORR_TYPE", 0, np.array([6, 7], dtype=np.int32))
        assert table.putcell("CORR_PRODUCT", 0, np.array([[0, 1], [1, 0]], dtype=np.int32))

    def seed_visibility(table):
        base = np.arange(8, dtype=np.float32).reshape(2, 4)
        assert table.putcell("DATA", 1, base + 1j * (10 + base))
        assert table.putcell("FLAG", 1, base % 3 == 0)
        assert table.putcell("WEIGHT", 1, np.array([4, 9], dtype=np.float32))
        assert table.putcell("SIGMA", 1, np.array([0.5, 0.25], dtype=np.float32))

    def reverse_channels(table):
        return table.putcell("CHAN_FREQ", 0, table.getcell("CHAN_FREQ", 0)[::-1].copy())

    for path in (first, donor):
        change_table(path / "POLARIZATION", crosshands)
    change_table(donor / "ANTENNA", swap_first_two_antennas)
    change_table(donor / "SPECTRAL_WINDOW", reverse_channels)
    change_table(donor, seed_visibility)
    incoming = main_cells(tools, donor, 1)

    m = (pinned_casa() if mode == "casa-virtual" else tools).ms()
    assert m.open(str(first), nomodify=False)
    if mode == "physical":
        assert m.concatenate(str(donor), "1Hz", "1mas", 2.0, 0, str(output))
    else:
        assert m.virtconcatenate(str(donor), "", "1Hz", "1mas", 2.0)
        output = donor
    assert m.close()
    for reader in (tools, pinned_casa()):
        actual = main_cells(reader, output, 1)
        np.testing.assert_array_equal(actual["DATA"], incoming["DATA"][::-1, ::-1].conj())
        # CASA virtual concatenation swaps the polarization but leaves flags
        # in their original channel order even when DATA reverses channels.
        expected_flags = incoming["FLAG"][::-1]
        if mode != "casa-virtual":
            expected_flags = expected_flags[:, ::-1]
        np.testing.assert_array_equal(actual["FLAG"], expected_flags)
        np.testing.assert_array_equal(actual["UVW"], -incoming["UVW"])
        np.testing.assert_array_equal(actual["WEIGHT"], incoming["WEIGHT"][::-1] * 2)
        np.testing.assert_allclose(actual["SIGMA"], incoming["SIGMA"][::-1] / np.sqrt(2), rtol=1e-7)
        assert [actual["ANTENNA1"], actual["ANTENNA2"]] == [0, 1]


def test_invalid_named_output_preserves_inputs(native_dataset, tmp_path):
    first, donor = copy_inputs(native_dataset, tmp_path)
    occupied = tmp_path / "occupied"
    occupied.mkdir()
    marker = occupied / "keep"
    marker.write_text("unchanged")
    initial_main = counts(tools, first, (".", "FIELD", "POINTING"))
    initial_history = counts(tools, donor, ("HISTORY",))
    m = tools.ms()
    assert m.open(str(first), nomodify=False)
    for destination in (occupied, first, first / "FIELD"):
        with pytest.raises(RuntimeError):
            m.concatenate(str(donor), destmsfile=str(destination))
    for options in ({"handling": 4}, {"weightscale": -1.0}):
        with pytest.raises(RuntimeError):
            m.concatenate(str(donor), **options)
    assert m.close()
    assert marker.read_text() == "unchanged"
    assert counts(tools, first, (".", "FIELD", "POINTING")) == initial_main
    assert counts(tools, donor, ("HISTORY",)) == initial_history


@pytest.mark.parametrize("empty_state", ["attached", "donor"])
def test_incomplete_state_and_model_metadata(native_dataset, tmp_path, concat_api, empty_state):
    first, donor = copy_inputs(native_dataset, tmp_path)

    def seed_model(table):
        assert table.addcols({"MODEL_DATA": table.getcoldesc("DATA")})
        assert table.putcol("MODEL_DATA", table.getcol("DATA") * np.complex64(2j))
        assert table.putcolkeyword("MODEL_DATA", "CHANNEL_SELECTION", np.array([[2], [1]], dtype=np.int32))

    for path in (first, donor):
        change_table(path, seed_model)
    target = first if empty_state == "attached" else donor
    change_table(target / "STATE", lambda table: table.removerows([0]))
    change_table(target, lambda table: table.putcol("STATE_ID", np.full(table.nrows(), -1, dtype=np.int32)))

    m = concat_api.ms()
    assert m.open(str(first), nomodify=False)
    original = m.nrow()
    assert m.concatenate(str(donor), "1Hz", "1mas")
    assert m.close()
    for reader in (tools, pinned_casa()):
        tab = reader.table()
        assert tab.open(str(first))
        np.testing.assert_array_equal(tab.getcol("STATE_ID"), np.full(2 * original, -1, dtype=np.int32))
        np.testing.assert_array_equal(
            tab.getcolkeyword("MODEL_DATA", "CHANNEL_SELECTION"),
            np.array([[0], [4]], dtype=np.int32),
        )
        np.testing.assert_array_equal(tab.getcell("MODEL_DATA", original + 1), tab.getcell("DATA", original + 1) * 2j)
        assert tab.close()


@pytest.mark.parametrize("destination", ["child", "empty"])
def test_named_destination_open_forms(native_dataset, tmp_path, destination):
    first, donor = copy_inputs(native_dataset, tmp_path)
    output = first / "child.ms" if destination == "child" else tmp_path / "empty.ms"
    if destination == "empty":
        output.mkdir()
    m = tools.ms()
    assert m.open(str(first), nomodify=False)
    original = m.nrow()
    assert m.concatenate(str(donor), destmsfile=str(output))
    assert m.nrow() == original
    assert m.close()
    for reader in (tools, pinned_casa()):
        assert counts(reader, output, (".", "POINTING")) == [original, 24]


def test_optional_spectra_follow_reversed_channels(native_dataset, tmp_path):
    first, donor = copy_inputs(native_dataset, tmp_path)
    base = np.arange(8, dtype=np.float32).reshape(2, 4)
    weights, sigma, flags = base + 1, base + 2, (base % 3 == 0)[..., None]

    def seed_arrays(table):
        for name in ("WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"):
            if name not in table.colnames():
                descriptor = table.getcoldesc("DATA")
                descriptor["valueType"] = "float"
                descriptor["dataManagerGroup"] = name
                descriptor["dataManagerType"] = "StandardStMan"
                assert table.addcols({name: descriptor})
        for row in range(table.nrows()):
            assert table.putcell("WEIGHT_SPECTRUM", row, weights)
            assert table.putcell("SIGMA_SPECTRUM", row, sigma)
            assert table.putcell("FLAG_CATEGORY", row, flags)

    for path in (first, donor):
        change_table(path, seed_arrays)
    change_table(donor / "SPECTRAL_WINDOW", lambda table: table.putcell(
        "CHAN_FREQ", 0, table.getcell("CHAN_FREQ", 0)[::-1].copy(),
    ))

    m = tools.ms()
    assert m.open(str(first), nomodify=False)
    original = m.nrow()
    assert m.concatenate(str(donor), "1Hz", "1mas", 2.0)
    assert m.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(first))
        np.testing.assert_array_equal(table.getcell("WEIGHT_SPECTRUM", original), weights[:, ::-1] * 2)
        np.testing.assert_allclose(table.getcell("SIGMA_SPECTRUM", original), sigma[:, ::-1] / np.sqrt(2), rtol=1e-7)
        np.testing.assert_array_equal(table.getcell("FLAG_CATEGORY", original), flags[:, ::-1, :])
        assert table.close()


def copy_empty_main(incoming, output):
    table = pinned_casa().table()
    assert table.open(str(incoming))
    selection = table.query("ROWID() < 0")
    empty = selection.copy(str(output), deep=True, valuecopy=True)
    assert empty.close()
    assert selection.close()
    assert table.close()


def test_empty_main_donor_appends_no_rows(native_dataset, tmp_path):
    first, incoming = copy_inputs(native_dataset, tmp_path)
    donor = tmp_path / "empty.ms"
    copy_empty_main(incoming, donor)

    m = tools.ms()
    assert m.open(str(first), nomodify=False)
    original = m.nrow()
    assert m.concatenate(str(donor), "1Hz", "1mas")
    assert m.nrow() == original
    assert m.close()
    for reader in (tools, pinned_casa()):
        assert counts(reader, first, (".",)) == [original]
        assert counts(reader, donor, (".",)) == [0]


def test_can_append_to_empty_main(native_dataset, tmp_path, concat_api):
    original, donor = copy_inputs(native_dataset, tmp_path)
    first = tmp_path / "empty.ms"
    copy_empty_main(original, first)
    m = concat_api.ms()
    assert m.open(str(first), nomodify=False)
    assert m.nrow() == 0
    assert m.concatenate(str(donor), "1Hz", "1mas")
    assert m.close()
    for reader in (tools, pinned_casa()):
        assert counts(reader, first, (".", "POINTING")) == counts(reader, original, (".",)) + [24]
        expected = main_cells(tools, donor, 1)
        actual = main_cells(reader, first, 1)
        for column, value in actual.items():
            np.testing.assert_array_equal(value, expected[column])


def test_named_output_retains_donor_keywords(native_dataset, tmp_path, concat_api):
    first, donor = copy_inputs(native_dataset, tmp_path)
    output = tmp_path / "result.ms"
    for path, note in ((first, "original"), (donor, "incoming")):
        def seed(table):
            assert table.putkeyword("PROJECT_NOTE", note)
            assert table.putcolkeyword("DATA", "PROJECT_NOTE", note)
        change_table(path, seed)

    m = concat_api.ms()
    assert m.open(str(first), nomodify=False)
    assert m.concatenate(str(donor), "1Hz", "1mas", destmsfile=str(output))
    assert m.close()
    for reader in (tools, pinned_casa()):
        tab = reader.table()
        assert tab.open(str(output))
        assert tab.getkeyword("PROJECT_NOTE") == "incoming"
        assert tab.getcolkeyword("DATA", "PROJECT_NOTE") == "incoming"
        assert tab.close()


@pytest.mark.parametrize("named", [True, False])
def test_empty_destination_copies_fixed_optional_spectrum(native_dataset, tmp_path, concat_api, named):
    original, donor = copy_inputs(native_dataset, tmp_path)
    spectrum = np.full((2, 4), 7.0, dtype=np.float32)

    def seed_fixed_spectrum(table):
        name = "WEIGHT_SPECTRUM"
        if name in table.colnames():
            assert table.removecols([name])
        descriptor = {
            "valueType": "float",
            "dataManagerType": "StandardStMan",
            "dataManagerGroup": "fixed-weight",
            "option": 4,
            "ndim": 2,
            "shape": np.array([2, 4], dtype=np.int32),
        }
        assert table.addcols({name: descriptor})
        for row in range(table.nrows()):
            assert table.putcell(name, row, spectrum)

    for path in (original, donor):
        change_table(path, seed_fixed_spectrum)
    output = tmp_path / "output.ms"
    if named:
        attached = original
    else:
        attached = output
        copy_empty_main(original, attached)

    m = concat_api.ms()
    assert m.open(str(attached), nomodify=False)
    assert m.concatenate(str(donor), "1Hz", "1mas", destmsfile=str(output) if named else "")
    assert m.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(output))
        np.testing.assert_array_equal(table.getcell("WEIGHT_SPECTRUM", 0), spectrum)
        assert table.close()


@pytest.mark.parametrize("link_representation", ["string", "fallback"])
def test_named_output_normalizes_legacy_subtable_links(native_dataset, tmp_path, link_representation):
    first, donor = copy_inputs(native_dataset, tmp_path)
    for path in (first, donor):
        def seed(table):
            assert table.removekeyword("FIELD")
            if link_representation == "string":
                assert table.putkeyword("FIELD", "././FIELD")
        change_table(path, seed)

    m = tools.ms()
    assert m.open(str(first), nomodify=False)
    output = tmp_path / "named.ms"
    assert m.concatenate(str(donor), "1Hz", "1mas", destmsfile=str(output))
    assert m.close()
    relocated = tmp_path / "relocated.ms"
    output.rename(relocated)
    assert (relocated / "FIELD" / "table.dat").is_file()
    for reader in (tools, pinned_casa()):
        probe = reader.ms()
        assert probe.open(str(relocated))
        assert probe.range(["fields"])["fields"].tolist() == ["F"]
        assert probe.close()


@pytest.mark.parametrize("codec", ["fixed", "automatic", "manual"])
def test_existing_compressed_model_data_accepts_new_rows(native_dataset, tmp_path, concat_api, codec):
    first, donor = copy_inputs(native_dataset, tmp_path)
    before = np.full((2, 4), 1 + 2j, dtype=np.complex64)
    incoming = np.full((2, 4), 3 + 4j, dtype=np.complex64)
    casa = pinned_casa()

    for path in (first, donor):
        table = casa.table()
        assert table.open(str(path), nomodify=False)
        model = table.getcoldesc("DATA")
        model.update(dataManagerType="StandardStMan", dataManagerGroup="model-storage",
                     option=4, ndim=2, shape=np.array([2, 4], dtype=np.int32))
        if path == first:
            packed = dict(model, valueType="int", dataManagerGroup="packed-model")
            assert table.addcols({"PACKED": packed})
            spec = {"SOURCENAME": "MODEL_DATA", "TARGETNAME": "PACKED", "AUTOSCALE": False}
            if codec == "fixed":
                spec.update(SCALE=1.0, OFFSET=0.0)
            else:
                spec.update(SCALENAME="PACK_SCALE", OFFSETNAME="PACK_OFFSET",
                            AUTOSCALE=codec == "automatic")
                for name in ("PACK_SCALE", "PACK_OFFSET"):
                    scalar = dict(table.getcoldesc("EXPOSURE"), valueType="float",
                                  dataManagerType="StandardStMan", dataManagerGroup=f"physical-{name}")
                    assert table.addcols({name: scalar})
                if codec == "manual":
                    assert table.putcol("PACK_SCALE", np.ones(table.nrows(), dtype=np.float32))
            assert table.addcols({"MODEL_DATA": model}, {
                "TYPE": "CompressComplex",
                "NAME": "compressed-model",
                "COLUMNS": ["MODEL_DATA"],
                "SPEC": spec,
            })
        else:
            assert table.addcols({"MODEL_DATA": model})
        for row in range(table.nrows()):
            assert table.putcell("MODEL_DATA", row, before if path == first else incoming)
        assert table.close()

    m = concat_api.ms()
    assert m.open(str(first), nomodify=False)
    original = m.nrow()
    assert m.concatenate(str(donor), "1Hz", "1mas")
    assert m.close()
    expected = np.zeros_like(incoming) if codec == "manual" and concat_api is casa else incoming
    for reader in (tools, casa):
        table = reader.table()
        assert table.open(str(first))
        np.testing.assert_allclose(table.getcell("MODEL_DATA", 0), before, atol=3e-5, rtol=0)
        np.testing.assert_allclose(table.getcell("MODEL_DATA", original), expected, atol=3e-5, rtol=0)
        assert table.close()


def test_selected_view_does_not_mutate_physical_root(native_dataset, tmp_path, concat_api):
    first, donor = copy_inputs(native_dataset, tmp_path)
    before = counts(tools, first, (".", "POINTING", "FIELD"))
    history = counts(tools, donor, ("HISTORY",))
    m = concat_api.ms()
    assert m.open(str(first), nomodify=False)
    assert m.msselect({"taql": "ROWID() < 2"})
    assert m.nrow(selected=True) == 2
    with pytest.raises(RuntimeError):
        m.concatenate(str(donor), "1Hz", "1mas")
    assert m.close()
    for reader in (tools, pinned_casa()):
        assert counts(reader, first, (".", "POINTING", "FIELD")) == before
        assert counts(reader, donor, ("HISTORY",)) == history


def test_encoded_append_after_a_materialized_rewrite(native_dataset, tmp_path, concat_api):
    first, donor = copy_inputs(native_dataset, tmp_path)
    reverse = tmp_path / "reverse.ms"
    copytree(native_dataset, reverse)
    pattern = np.arange(8, dtype=np.float32).reshape(2, 4).astype(np.complex64)

    def data(table):
        for row in range(table.nrows()):
            assert table.putcell("DATA", row, pattern)

    for path in (first, donor, reverse):
        change_table(path, data)
    change_table(reverse / "SPECTRAL_WINDOW", lambda table: table.putcell(
        "CHAN_FREQ", 0, table.getcell("CHAN_FREQ", 0)[::-1].copy(),
    ))

    m = concat_api.ms()
    assert m.open(str(first), nomodify=False)
    original = m.nrow()
    for index, path in enumerate((donor, reverse, donor), start=2):
        assert m.concatenate(str(path), "1Hz", "1mas")
        assert m.nrow() == index * original
    assert m.close()
    for reader in (tools, pinned_casa()):
        table = reader.table()
        assert table.open(str(first))
        for block in range(4):
            np.testing.assert_array_equal(
                table.getcell("DATA", block * original + 1),
                pattern[:, ::-1] if block == 2 else pattern,
            )
        assert table.close()
