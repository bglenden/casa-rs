#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public mstransform spectral-window combination with CASA 6.7.6.14."""

from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from ms_data_columns_oracle import add_float_and_lag
from ms_hanning_oracle import NOISE, compare_table, fixture
from ms_selection_tasks_oracle import MAIN, SUBTABLES


def make_pair(root, name, offset, *, counts=(6, 6), spectra=False, time_offset=0, descending=False):
    left, right = (root / f"{name}-{side}.ms" for side in ("left", "right"))
    fixture(left, counts[0], spectra=spectra)
    fixture(right, counts[1], spectra=spectra, frequency=f"{100e9 + offset * 1e6:.0f}Hz")
    if descending:
        with table(str(right / "SPECTRAL_WINDOW"), readonly=False, ack=False) as ms:
            ms.putcell("CHAN_FREQ", 0, ms.getcell("CHAN_FREQ", 0)[::-1])
            ms.putcell("CHAN_WIDTH", 0, -ms.getcell("CHAN_WIDTH", 0)[::-1])
    source = root / f"{name}.ms"
    casatasks.concat(vis=[str(left), str(right)], concatvis=str(source))
    with table(str(source), readonly=False, ack=False) as ms:
        right_rows = ms.getcol("DATA_DESC_ID") == 1
        assert right_rows.sum() * 2 == ms.nrows()
        for name in ("TIME", "TIME_CENTROID", "SCAN_NUMBER", "STATE_ID", "OBSERVATION_ID", "ARRAY_ID"):
            values = ms.getcol(name)
            values[right_rows] = values[~right_rows]
            if name in ("TIME", "TIME_CENTROID"):
                values[right_rows] += time_offset
            ms.putcol(name, values)
        for row in np.flatnonzero(right_rows):
            for name, offset in (("DATA", 1000), ("CORRECTED_DATA", 3000), ("MODEL_DATA", 200)):
                ms.putcell(name, int(row), ms.getcell(name, int(row)) + offset)
    return source


def paired(root, source, case, *, datacolumn="data", usewtspectrum=True, deviations=None,
           spectral_deviations=None, **options):
    reference, native = (root / f"{case}-{side}.ms" for side in ("casa", "native"))
    for engine, path in ((casatasks, reference), (tasks, native)):
        assert engine.mstransform(vis=str(source), outputvis=str(path), datacolumn=datacolumn,
                                 combinespws=True, usewtspectrum=usewtspectrum, **options) is None
    columns = set(MAIN) | {"TIME_CENTROID", "INTERVAL", "EXPOSURE"}
    with table(str(reference), ack=False) as casa_ms, table(str(native), ack=False) as native_ms:
        extras = {*NOISE, "CORRECTED_DATA", "MODEL_DATA"}
        expected = extras & set(casa_ms.colnames())
        assert expected == extras & set(native_ms.colnames()), case
    compare_table(reference, native, columns | expected, case, deviations)
    for name in ("SPECTRAL_WINDOW", "DATA_DESCRIPTION", "POLARIZATION", "FIELD", "SOURCE", "FEED"):
        columns = SUBTABLES[name]
        if name == "SPECTRAL_WINDOW":
            columns = (*columns, "MEAS_FREQ_REF")
        compare_table(reference / name, native / name, columns, f"{case} {name}",
                      spectral_deviations if name == "SPECTRAL_WINDOW" else None)
    compare_table(reference / "FLAG_CMD", native / "FLAG_CMD", ("COMMAND", "APPLIED"), case)
    with table(str(native / "HISTORY"), ack=False) as history:
        assert any("".join(line.split()) == "combinespws=True" for line in history.getcol("MESSAGE"))
    print(f"passed {case}", flush=True)
    return reference, native


def unequal(root, source, selector):
    for engine in (casatasks, tasks):
        path = root / f"{source.stem}-unequal-{engine.__name__}.ms"
        try:
            engine.mstransform(vis=str(source), outputvis=str(path), datacolumn="data",
                              spw=selector, combinespws=True)
        except RuntimeError as error:
            assert "the number of channels is the same for all the spw's selected" in str(error)
            if engine is tasks:
                assert not path.exists()
        else:
            raise AssertionError(f"{engine.__name__} accepted spectral windows of different lengths")
    print(f"passed unequal {source.stem} {selector}", flush=True)


def varying_exposure(root):
    source = make_pair(root, "exposure-input", 4, spectra=True)
    with table(str(source), readonly=False, ack=False) as ms:
        right = ms.getcol("DATA_DESC_ID") == 1
        exposure = ms.getcol("EXPOSURE")
        exposure[right] /= 2
        ms.putcol("EXPOSURE", exposure)
        data = ms.getcol("DATA")
        flags = ms.getcol("FLAG")
        observed_weight = 1.0 / ms.getcol("SIGMA_SPECTRUM") ** 2
    rows = len(data) // 2
    weighted = np.zeros((rows, 10, 2), dtype=np.complex64)
    unweighted = np.zeros_like(weighted)
    weights = np.zeros(weighted.shape, dtype=np.float32)
    counts = np.zeros_like(weights)
    for window, is_right in ((slice(0, 6), False), (slice(4, 10), True)):
        selected = right == is_right
        admitted = ~flags[selected]
        w = observed_weight[selected] * admitted
        weighted[:, window] += data[selected] * w
        unweighted[:, window] += data[selected] * admitted
        weights[:, window] += w
        counts[:, window] += admitted
    np.divide(weighted, weights, out=weighted, where=weights != 0)
    np.divide(unweighted, counts, out=unweighted, where=counts != 0)
    assert np.any(np.abs(weighted - unweighted) > 1)

    def compare(row, native, reference):
        np.testing.assert_allclose(native, weighted[row], atol=5e-7, rtol=3e-6)
        np.testing.assert_allclose(reference.getcell("DATA", row), unweighted[row], atol=5e-7, rtol=3e-6)

    paired(root, source, "unequal-exposures", deviations={"DATA": compare})


def mixed_row_flags(root):
    source = make_pair(root, "mixed-row-flags", 6)
    with table(str(source), readonly=False, ack=False) as ms:
        right = ms.getcol("DATA_DESC_ID") == 1
        flags = ms.getcol("FLAG_ROW")
        left_rows = np.flatnonzero(~right)
        right_rows = np.flatnonzero(right)
        flags[left_rows[0]] = flags[right_rows[1]] = True
        flags[right_rows[0]] = flags[left_rows[1]] = False
        ms.putcol("FLAG_ROW", flags)
    expected = flags[left_rows] & flags[right_rows]

    def compare(row, native, reference):
        assert native == expected[row]
        assert reference.getcell("FLAG_ROW", row) == flags[right_rows[row]]

    paired(root, source, "row-flag-reduction", deviations={"FLAG_ROW": compare})


def array_width_refusal(root, source):
    for engine in (casatasks, tasks):
        output = root / f"array-bin-{engine.__name__}.ms"
        try:
            engine.mstransform(vis=str(source), outputvis=str(output), datacolumn="data",
                              combinespws=True, chanaverage=True, chanbin=[2, 3])
        except RuntimeError as error:
            assert "chabin cannot be an array" in str(error), str(error)
            if engine is tasks:
                assert not output.exists()
        else:
            raise AssertionError(f"{engine.__name__} accepted per-window bins while combining SPWs")
    print("passed combined array width refusal", flush=True)


def aligned_fractional_width(root):
    source = make_pair(root, "aligned-fractional-width", 4, spectra=True)
    width = 300000.4
    with table(str(source / "SPECTRAL_WINDOW"), readonly=False, ack=False) as ms:
        for window in range(2):
            frequencies = 1e11 + np.arange(4 * window, 4 * window + 6) * width
            ms.putcell("CHAN_FREQ", window, frequencies)
            ms.putcell("REF_FREQUENCY", window, frequencies[0])
            ms.putcell("TOTAL_BANDWIDTH", window, 6 * width)
            for name in ("CHAN_WIDTH", "EFFECTIVE_BW", "RESOLUTION"):
                ms.putcell(name, window, np.full(6, width))

    def same_frequency(name):
        return lambda row, got, ref: np.testing.assert_allclose(
            got, ref.getcell(name, row), atol=1e-4, rtol=0
        )

    paired(root, source, "full-overlap-fractional-width", spectral_deviations={
        name: same_frequency(name) for name in (
            "CHAN_FREQ", "REF_FREQUENCY", "TOTAL_BANDWIDTH", "CHAN_WIDTH", "EFFECTIVE_BW", "RESOLUTION"
        )
    })


def differing_time_average_epochs(root):
    source = make_pair(root, "different-centroids", 6)
    with table(str(source), ack=False, readonly=False) as ms:
        time = ms.getcol("TIME")
        right = ms.getcol("DATA_DESC_ID") == 1
        shift = right & (time == np.unique(time)[1])
        for name in ("TIME", "TIME_CENTROID"):
            values = ms.getcol(name)
            values[shift] += 3
            ms.putcol(name, values)
    reference, native = paired(root, source, "time-average-distinct-epochs",
                               timeaverage=True, timebin="20s")
    with table(str(reference), ack=False) as left, table(str(native), ack=False) as right:
        np.testing.assert_allclose(left.getcol("TIME"), right.getcol("TIME"), rtol=0, atol=2e-6)


def category_and_association_metadata(root):
    source = make_pair(root, "category-and-association", 6)
    with table(str(source), ack=False, readonly=False) as ms:
        flags = ms.getcol("FLAG")
        categories = np.stack((flags, ~flags), axis=1)
        right = np.flatnonzero(ms.getcol("DATA_DESC_ID") == 1)
        ms.putcolkeyword("FLAG_CATEGORY", "CATEGORY", ["line", "shadow"])
        for row in right:
            ms.putcell("FLAG_CATEGORY", int(row), categories[row])
    with table(str(source / "SPECTRAL_WINDOW"), ack=False, readonly=False) as ms:
        ms.addcols(maketabdesc([
            makearrcoldesc("ASSOC_SPW_ID", 0, ndim=1, valuetype="int"),
            makearrcoldesc("ASSOC_NATURE", "", ndim=1, valuetype="string"),
        ]))
        ms.putcell("ASSOC_SPW_ID", 0, [1])
        ms.putcell("ASSOC_NATURE", 0, ["CHANNEL-ZERO"])
        ms.putcell("ASSOC_SPW_ID", 1, [0])
        ms.putcell("ASSOC_NATURE", 1, ["CHANNEL-ZERO"])
    _, native = paired(root, source, "category-association-metadata")
    with table(str(native), ack=False) as ms:
        expected = np.concatenate((np.zeros_like(categories[right]), categories[right]), axis=2)
        np.testing.assert_array_equal(ms.getcol("FLAG_CATEGORY"), expected)
    with table(str(native / "SPECTRAL_WINDOW"), ack=False) as spw:
        assert len(spw.getcell("ASSOC_SPW_ID", 0)) == 0
        assert len(spw.getcell("ASSOC_NATURE", 0)) == 0


def reject_incompatible_lags(root):
    source = make_pair(root, "incompatible-lag", 6)
    add_float_and_lag(source)
    output = root / "rejected-lags.ms"
    try:
        tasks.mstransform(vis=str(source), outputvis=str(output), datacolumn="lag_data,data",
                          combinespws=True)
    except RuntimeError as error:
        assert "different LAG_DATA cells" in str(error)
        assert not output.exists()
    else:
        raise AssertionError("SPW combination discarded differing lag data")
    print("passed incompatible lag refusal", flush=True)


def main():
    assert casatasks.version() == [6, 7, 6, 14], casatasks.version()
    with TemporaryDirectory(prefix="mstransform-combine-spws-") as scratch:
        root = Path(scratch)
        touching = make_pair(root, "touching-input", 6)
        overlap = make_pair(root, "overlap-input", 4, spectra=True)
        for case, source, options in (
            ("touching", touching, {}),
            ("unforced", touching, dict(usewtspectrum=False)),
            ("overlap", overlap, {}),
            ("calibrated-overlap", overlap, dict(datacolumn="corrected")),
            ("all-overlap", overlap, dict(datacolumn="all")),
            ("fractional-overlap", make_pair(root, "fractional", 4.5), {}),
            ("gap", make_pair(root, "gap", 8), {}),
            ("missing-window-rows", make_pair(root, "time-offset", 6, time_offset=3), {}),
            ("sub-millisecond-rows", make_pair(root, "submillisecond-offset", 6, time_offset=0.0003), {}),
            ("descending", make_pair(root, "descending", 5, counts=(5, 5), descending=True), {}),
            ("selection", touching, dict(spw="0:1~4,1:1~4", field="PHASE", correlation="YY")),
            ("single-window", touching, dict(spw="1")),
            ("keepflags", touching, dict(keepflags=False)),
            ("hanning", touching, dict(hanning=True)),
            ("channel-average", touching, dict(chanaverage=True, chanbin=2)),
            ("time-average", touching, dict(timeaverage=True, timebin="20s")),
            ("both-averages", touching, dict(timeaverage=True, timebin="20s",
                                             chanaverage=True, chanbin=2, hanning=True)),
        ):
            paired(root, source, case, **options)
        unequal(root, make_pair(root, "different-counts", 8, counts=(6, 4)), "")
        unequal(root, touching, "0:0~2,1")
        array_width_refusal(root, touching)
        varying_exposure(root)
        mixed_row_flags(root)
        aligned_fractional_width(root)
        differing_time_average_epochs(root)
        category_and_association_metadata(root)
        reject_incompatible_lags(root)
        for product in paired(root, overlap, "reopen"):
            output = subprocess.check_output(
                ["taql", f"select NUM_CHAN from [{product}/SPECTRAL_WINDOW]"], text=True
            )
            assert output.splitlines()[-1] == "10", output


if __name__ == "__main__":
    main()
