#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare TFCrop time averaging and flags on the contributing integrations."""

from contextlib import chdir
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_tfcrop_channelavg_oracle import run_product, verify_flags
from flagdata_tfcrop_fit_oracle import make_fixture
from flagdata_tfcrop_sources_oracle import (
    check_science_state, put_samples, science_state, seed_fourier_models,
)
from ms_selection_tasks_oracle import casa_table


OPTIONS = dict(mode="tfcrop", antenna="0&1", timeavg=True, flagdimension="time", timefit="poly",
               timecutoff=2.5, freqcutoff=1.5, maxnpieces=3, extendflags=False)


def verify_row_flags(path, expected_samples, expected_rows=None):
    actual, rows = flags(path)
    np.testing.assert_array_equal(actual, expected_samples)
    with casa_table(path) as casa:
        np.testing.assert_array_equal(casa.getcol("FLAG").transpose(2, 1, 0), expected_samples)
        np.testing.assert_array_equal(casa.getcol("FLAG_ROW"), rows)
    if expected_rows is not None:
        np.testing.assert_array_equal(rows, expected_rows)
    return rows


def check_time_products(libraries):
    source = "tfcrop-time.ms"
    empty, narrow, _ = make_fixture(source)
    with table(source, ack=False) as main:
        times = main.getcol("TIME")
        integration = np.searchsorted(np.unique(times), times)
    full = range(32)
    cases = (
        (dict(timebin="0s"), (6,), full, (0,)),
        (dict(timebin="10s"), (6,), full, (0,)),
        (dict(timebin="20s"), (6, 7), full, (0,)),
        (dict(timebin="20s\0ignored"), (6, 7), full, (0,)),
        (dict(timebin="30s"), (), full, (0,)),
        (dict(timebin="40s"), (), full, (0,)),
        (dict(timebin="240s"), (), full, (0,)),
        (dict(timebin=""), (6,), full, (0,)),
        (dict(timebin="-10s"), (6,), full, (0,)),
        (dict(timebin="garbage"), (), full, (0,)),
        (dict(timebin="20s", spw="0:8~19"), (6, 7), range(8, 20), (0,)),
        (dict(timebin="20s", spw="0:8~19", channelavg=True, chanbin=2), (6, 7), range(8, 20), (0,)),
        (dict(timebin="20s", spw="0:8~19", extendflags=True), (6, 7), range(8, 20), (0, 1)),
        (dict(timebin="20s", flagdimension="freq"), range(21), (16,), (1,)),
        (dict(timebin="20s", flagdimension="freq", channelavg=True, chanbin=2),
         range(21), (16, 17), (1,)),
        (dict(timebin="20s", ntime="65s"), (), full, (0,)),
        (dict(timebin="20s", timeavg=np.bool_(True)), (6,), full, (0,)),
    )
    for index, (overrides, integrations, channels, planes) in enumerate(cases):
        rows = narrow & np.isin(integration, integrations)
        expected = empty.copy()
        for plane in planes:
            for channel in channels:
                expected[rows, channel, plane] = True
        for name, task in libraries:
            path = f"time-{index}-{name}.ms"
            copytree(source, path)
            run_product(name, task, OPTIONS | dict(vis=path) | overrides, empty, expected)
    return len(cases)


def check_vector_refusal(libraries):
    vectors = ([True], np.array(True), np.array([], dtype=bool), [np.bool_(True)])
    for index, value in enumerate(vectors):
        for name, task in libraries:
            path = f"time-vector-{index}-{name}.ms"
            copytree("tfcrop-time.ms", path)
            with table(path, ack=False) as main:
                seeded = main.getcol("FLAG")
            original, auxiliary = science_state(path), auxiliary_content(path)
            for action in ("calculate", "apply"):
                options = dict(vis=path, timeavg=value, timebin="20s", action=action)
                try:
                    task(**(OPTIONS | options))
                except RuntimeError as error:
                    assert str(error) == "RecordInterface::asBool - invalid data type"
                else:
                    raise AssertionError((name, options))
                verify_flags(path, seeded)
                check_science_state(path, original)
                if action == "calculate":
                    assert auxiliary_content(path) == auxiliary
                else:
                    verify_flags(str(Path(path + ".flagversions", "flags.flagdata_1")), seeded)
    return len(vectors)


def check_weights_sources_and_preflags(libraries):
    empty, narrow, broad = make_fixture("time-weights-source.ms")
    with table("time-weights-source.ms", readonly=False, ack=False) as main:
        times = main.getcol("TIME")
        integration = np.searchsorted(np.unique(times), times)
        data = main.getcol("DATA")
        put_samples(main, "CORRECTED_DATA", data + 1.)
        put_samples(main, "MODEL_DATA", np.full_like(data, 0.5))
        ones = np.ones(data.shape, dtype=np.float32)
        put_samples(main, "SIGMA_SPECTRUM", ones.copy())
        put_samples(main, "WEIGHT_SPECTRUM", ones.copy())
    next_broad = narrow & (integration == 7)
    base_data = empty.copy()

    def build_weighted(path, setup):
        copytree("time-weights-source.ms", path)
        with table(path, readonly=False, ack=False) as main:
            seeded = base_data.copy()
            if setup == "data-sigma":
                sigma = main.getcol("SIGMA_SPECTRUM")
                sigma[broad, :, 0] = 1e10
                main.putcol("SIGMA_SPECTRUM", sigma)
            elif setup == "data-weight-ignored":
                weight = main.getcol("WEIGHT_SPECTRUM")
                weight[broad, :, 0] = 1e-12
                main.putcol("WEIGHT_SPECTRUM", weight)
            elif setup == "corrected-weight":
                weight = main.getcol("WEIGHT_SPECTRUM")
                weight[broad, :, 0] = 1e-12
                main.putcol("WEIGHT_SPECTRUM", weight)
            elif setup == "corrected-sigma-ignored":
                sigma = main.getcol("SIGMA_SPECTRUM")
                sigma[broad, :, 0] = 1e10
                main.putcol("SIGMA_SPECTRUM", sigma)
            elif setup == "preflag-broad":
                seeded[broad, :, 0] = True
                main.putcol("FLAG", seeded)
            elif setup == "phase-cancel":
                d = main.getcol("DATA")
                d[next_broad, :, 0] = -28.
                main.putcol("DATA", d)
            elif setup == "model-unweighted":
                m = main.getcol("MODEL_DATA")
                m[broad, :, 0] = 30.
                main.putcol("MODEL_DATA", m)
                w = main.getcol("WEIGHT_SPECTRUM")
                w[broad, :, 0] = 1e-12
                main.putcol("WEIGHT_SPECTRUM", w)
            elif setup == "residual-cancel":
                m = main.getcol("MODEL_DATA")
                m[broad, :, 0] = 29.5
                main.putcol("MODEL_DATA", m)
            elif setup == "residual-downweight":
                w = main.getcol("WEIGHT_SPECTRUM")
                w[broad, :, 0] = 1e-12
                main.putcol("WEIGHT_SPECTRUM", w)
            elif setup == "combined-unequal":
                # ch 0: int 6 (11, w=1) + int 7 (1, w=1) -> avg=6.0, mass=2
                # ch 1: int 6 (-1, w=1) + int 7 (-1, w=4) -> avg=-1.0, mass=5
                # channel bin [0, 1] averages to (6*2 + (-1)*5)/7 = 1.0 exact.
                d = np.ones_like(main.getcol("DATA"))
                d[broad, 0, 0] = 11.
                d[broad, 1, 0] = -1.
                d[next_broad, 1, 0] = -1.
                main.putcol("DATA", d)
                sigma = np.ones(d.shape, dtype=np.float32)
                sigma[next_broad, 1, 0] = 0.5
                main.putcol("SIGMA_SPECTRUM", sigma)
            elif setup == "stokes-i":
                d = np.ones_like(main.getcol("DATA"))
                d[broad, :, 0] = 30.
                d[broad, :, 1] = 30.
                main.putcol("DATA", d)
            return seeded

    paired_cases = (
        ("data-sigma", dict(datacolumn="data"), ()),
        ("data-weight-ignored", dict(datacolumn="data"), (6, 7)),
        ("corrected-weight", dict(datacolumn="corrected"), ()),
        ("corrected-sigma-ignored", dict(datacolumn="corrected"), (6, 7)),
        ("preflag-broad", dict(datacolumn="data"), ()),
        ("phase-cancel", dict(datacolumn="data"), ()),
        ("model-unweighted", dict(datacolumn="model"), (6, 7)),
        ("residual-cancel", dict(datacolumn="residual"), ()),
        ("corrected-sigma-ignored", dict(datacolumn="residual"), (6, 7)),
        ("residual-downweight", dict(datacolumn="residual"), ()),
        ("combined-unequal", dict(datacolumn="data", spw="0:0~1", channelavg=True, chanbin=2), ()),
        ("stokes-i", dict(datacolumn="data", correlation="ABS_I"), (6, 7)),
    )
    for index, (setup, overrides, integrations) in enumerate(paired_cases):
        for name, task in libraries:
            path = f"weighted-{index}-{name}.ms"
            seeded = build_weighted(path, setup)
            expected = seeded.copy()
            rows = narrow & np.isin(integration, integrations)
            planes = (0, 1) if overrides.get("correlation") == "ABS_I" else (0,)
            channels = (0, 1) if overrides.get("spw") == "0:0~1" else range(32)
            for plane in planes:
                for channel in channels:
                    expected[rows, channel, plane] = True
            options = OPTIONS | dict(vis=path, timebin="20s") | overrides
            if setup == "stokes-i":
                rows_mask = narrow & np.isin(integration, (6, 7))
                if name == "native":
                    calculated = tasks.run("flagdata", action="calculate", **options)
                    from json import loads
                    assert loads(calculated.stdout)["result"]["changed_samples"] == expected.sum() - seeded.sum()
                else:
                    assert task(action="calculate", **options) == {}
                verify_row_flags(path, seeded, expected_rows=np.zeros_like(rows_mask))
                assert task(**options) == {}
                verify_row_flags(path, expected, expected_rows=rows_mask)
                verify_row_flags(str(Path(path + ".flagversions", "flags.flagdata_1")), seeded)
            else:
                run_product(name, task, options, seeded, expected)

    # Columns where CASA 6.7.6.14 FlagDataHandler::setTimeAverageIter substitutes zero.
    trailing_singletons = narrow & np.isin(integration, (20, 41))
    zero_sub_cases = ("residual_data", "float_data", "weight", "weight_spectrum")
    for index, column in enumerate(zero_sub_cases):
        for name, task in libraries:
            path = f"zero-sub-{index}-{name}.ms"
            copytree("time-weights-source.ms", path)
            with table(path, readonly=False, ack=False) as main:
                if column == "float_data":
                    put_samples(main, "FLOAT_DATA", main.getcol("DATA").real.copy())
                    main.removecols("DATA")
                elif column == "weight":
                    if "WEIGHT_SPECTRUM" in main.colnames():
                        main.removecols("WEIGHT_SPECTRUM")
                    weight = np.ones_like(main.getcol("WEIGHT"))
                    weight[trailing_singletons, :] = 2.
                    weight[broad, 0] = 30.
                    main.putcol("WEIGHT", weight)
                elif column == "weight_spectrum":
                    weight = np.ones(empty.shape, dtype=np.float32)
                    weight[trailing_singletons, :, :] = 2.
                    weight[broad, :, 0] = 30.
                    put_samples(main, "WEIGHT_SPECTRUM", weight)
            expected = empty.copy()
            if name == "native":
                rows = narrow & np.isin(integration, (6, 7))
                expected[rows, :, 0] = True
            options = OPTIONS | dict(vis=path, timebin="20s", datacolumn=column)
            run_product(name, task, options, empty, expected)
    return len(paired_cases) + len(zero_sub_cases)


def check_virtual_fourier_models(libraries):
    empty, narrow, model = seed_fourier_models()
    with table("model-source.ms", ack=False) as main:
        times = main.getcol("TIME")
        integration = np.searchsorted(np.unique(times), times)
    row_excl = narrow & (integration == 0)
    row_unflagged = narrow & (integration == 1)
    rows_spike = narrow & np.isin(integration, (6, 7))
    # Verify preflagged integration 0 and preflagged channel 6 in integration 1 are excluded
    # from Fourier residual roundoff across both timeavg and timeavg+channelavg.
    cases = (
        ("residual", 30., 0., False),
        ("residual", 0.001, 1e12, False),
        ("residual", 0., 0., False),
        ("residual", 0.001, 1e12, True),
        ("residual_data", 0.001, 1e12, False),
        ("residual_data", 0.001, 1e12, True),
    )
    verified = 0
    for index, (column, amplitude, excluded_amp, chanavg) in enumerate(cases):
        seeded = empty.copy()
        seeded[row_excl, :16, 0] = True
        seeded[row_unflagged, 6, 0] = True
        observed = model + 1.
        observed[narrow & (integration == 6), :16, 0] += amplitude
        observed[row_excl, :16, 0] += excluded_amp
        observed[row_unflagged, 6, 0] += excluded_amp
        corrected = observed.copy()
        expected = seeded.copy()
        # CASA ignores non-zero residual_data due to time-average iterator zero substitution.
        for source in ("scratch", "virtual"):
            for name, task in libraries:
                path = f"time-fourier-{index}-{source}-{name}.ms"
                copytree(f"model-{source}.ms", path)
                with table(path, readonly=False, ack=False) as main:
                    put_samples(main, "DATA", observed)
                    put_samples(main, "CORRECTED_DATA", corrected)
                    main.putcol("FLAG", seeded)
                case_expected = seeded.copy()
                if amplitude > 0. and not (name == "casa" and column == "residual_data"):
                    case_expected[rows_spike, :16, 0] = True
                options = dict(
                    vis=path,
                    mode="tfcrop",
                    field="0",
                    antenna="0&1",
                    spw="0:0~15",
                    timeavg=True,
                    timebin="20s",
                    channelavg=chanavg,
                    chanbin=2,
                    datacolumn=column,
                    correlation="ABS_XX",
                    flagdimension="time",
                    timefit="poly",
                    timecutoff=2.5,
                    maxnpieces=3,
                    extendflags=False,
                )
                run_product(name, task, options, seeded, case_expected)
                verified += 1
    return verified


def check_row_flags(libraries):
    empty, narrow, _ = make_fixture("time-rowflags-source.ms")
    with table("time-rowflags-source.ms", ack=False) as main:
        times = main.getcol("TIME")
        integration = np.searchsorted(np.unique(times), times)
    affected = narrow & np.isin(integration, (6, 7))

    # Case A: full window with extendflags=True flags all cells of rows (6, 7); native sets FLAG_ROW=True.
    expected_full = empty.copy()
    expected_full[affected, :, :] = True
    for name, task in libraries:
        path = f"rowflag-full-{name}.ms"
        copytree("time-rowflags-source.ms", path)
        options = OPTIONS | dict(vis=path, timebin="20s", extendflags=True, flagbackup=False)
        assert task(**options) == {}
        rows = verify_row_flags(path, expected_full, expected_rows=affected if name == "native" else None)
        assert not rows[~affected].any()

    # Case B: partial spw with channelavg=True + extendflags=True leaves unselected channels clear;
    # native keeps FLAG_ROW=False while CASA can set FLAG_ROW=True.
    expected_partial = empty.copy()
    expected_partial[affected, 8:20, :] = True
    for name, task in libraries:
        path = f"rowflag-partial-{name}.ms"
        copytree("time-rowflags-source.ms", path)
        options = OPTIONS | dict(
            vis=path, timebin="20s", spw="0:8~19", channelavg=True, chanbin=2,
            extendflags=True, flagbackup=False,
        )
        assert task(**options) == {}
        rows = verify_row_flags(path, expected_partial, expected_rows=np.zeros_like(affected) if name == "native" else None)
        assert not rows[~affected].any()

    # Case C: pre-existing FLAG_ROW=True on an integration in an affected partial-plane bin is preserved by native.
    preexisting = narrow & (integration == 6)
    expected_partial_plane = empty.copy()
    expected_partial_plane[affected, :, 0] = True
    for name, task in libraries:
        path = f"rowflag-preserve-{name}.ms"
        copytree("time-rowflags-source.ms", path)
        with table(path, readonly=False, ack=False) as main:
            main.putcol("FLAG_ROW", preexisting)
        options = OPTIONS | dict(vis=path, timebin="20s", extendflags=False, flagbackup=False)
        assert task(**options) == {}
        verify_row_flags(path, expected_partial_plane, expected_rows=preexisting if name == "native" else None)
    return 3


def check_combined_scans_and_iterator_edges(libraries):
    empty, narrow, _ = make_fixture("time-scans-source.ms")
    with table("time-scans-source.ms", readonly=False, ack=False) as main:
        times = main.getcol("TIME")
        integration = np.searchsorted(np.unique(times), times)
        scans = np.where(integration <= 6, 1, 2).astype(np.int32)
        main.putcol("SCAN_NUMBER", scans)

    scan_cases = (
        (dict(combinescans=False), ()),
        (dict(combinescans=True), (6, 7)),
        (dict(combinescans=True, ntime="65s"), ()),
    )
    for index, (overrides, flagged_ints) in enumerate(scan_cases):
        expected = empty.copy()
        expected[narrow & np.isin(integration, flagged_ints), :, 0] = True
        for name, task in libraries:
            path = f"scans-{index}-{name}.ms"
            copytree("time-scans-source.ms", path)
            options = OPTIONS | dict(vis=path, field="0", timebin="20s") | overrides
            run_product(name, task, options, empty, expected)

    # Surviving extension iterator controls on rejected fit vs malformed timebin.
    iter_cases = (
        (dict(timebin="20s", maxnpieces=0, extendflags=True), (6,), ()),
        (dict(timebin="20s", maxnpieces=0, extendflags=True), (6, 7), (6, 7)),
        (dict(timebin="garbage", maxnpieces=3, extendflags=True), (6,), (6,)),
    )
    for index, (overrides, seeded_ints, flagged_ints) in enumerate(iter_cases):
        seeded = empty.copy()
        seeded[narrow & np.isin(integration, seeded_ints), 5, 0] = True
        expected = seeded.copy()
        expected[narrow & np.isin(integration, flagged_ints), 5, :] = True
        for name, task in libraries:
            path = f"time-iter-{index}-{name}.ms"
            copytree("tfcrop-time.ms", path)
            with table(path, readonly=False, ack=False) as main:
                main.putcol("DATA", np.ones_like(main.getcol("DATA")))
                main.putcol("FLAG", seeded)
            options = OPTIONS | dict(vis=path, timecutoff=1e5) | overrides
            run_product(name, task, options, seeded, expected)
    return len(scan_cases) + len(iter_cases)


def check_backup_restoration_and_interop(libraries):
    empty, narrow, _ = make_fixture("time-backup-source.ms")
    with table("time-backup-source.ms", readonly=False, ack=False) as main:
        times = main.getcol("TIME")
        integration = np.searchsorted(np.unique(times), times)
        flags_arr = empty.copy()
        flags_arr[0, 3, 1] = True
        row_flags_arr = np.zeros(main.nrows(), dtype=bool)
        row_flags_arr[0] = True
        main.putcol("FLAG", flags_arr)
        main.putcol("FLAG_ROW", row_flags_arr)
    affected = narrow & np.isin(integration, (6, 7))
    expected = flags_arr.copy()
    expected[affected, :, :] = True
    original = science_state("time-backup-source.ms")
    for name, task in libraries:
        path = f"backup-restore-{name}.ms"
        copytree("time-backup-source.ms", path)
        assert task(**(OPTIONS | dict(vis=path, timebin="20s", extendflags=True, flagbackup=True))) == {}
        verify_row_flags(path, expected, expected_rows=(row_flags_arr | affected) if name == "native" else None)
        check_science_state(path, original)
        restorer = tasks.flagmanager if name == "native" else casatasks.flagmanager
        assert restorer(vis=path, mode="restore", versionname="flagdata_1") is None
        verify_row_flags(path, flags_arr, expected_rows=row_flags_arr)
        check_science_state(path, original)
        # Cross-engine restore: native backup restored by casatasks.flagmanager and vice versa.
        copy_path = f"backup-cross-{name}.ms"
        copytree("time-backup-source.ms", copy_path)
        assert task(**(OPTIONS | dict(vis=copy_path, timebin="20s", extendflags=True, flagbackup=True))) == {}
        cross_restorer = casatasks.flagmanager if name == "native" else tasks.flagmanager
        assert cross_restorer(vis=copy_path, mode="restore", versionname="flagdata_1") is None
        verify_row_flags(copy_path, flags_arr, expected_rows=row_flags_arr)
        check_science_state(copy_path, original)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-tfcrop-timeavg-") as root, chdir(root):
        products = check_time_products(libraries)
        refusals = check_vector_refusal(libraries)
        weighted = check_weights_sources_and_preflags(libraries)
        fourier = check_virtual_fourier_models(libraries)
        row_flags = check_row_flags(libraries)
        scans = check_combined_scans_and_iterator_edges(libraries)
        check_backup_restoration_and_interop(libraries)
    print(f"{products + weighted + fourier + row_flags + scans} TFCrop time averaging products "
          f"(including {fourier} Fourier controls, {row_flags} FLAG_ROW controls, {scans} scan/iterator "
          f"controls), {refusals} vector refusals, and backup restoration verified")


if __name__ == "__main__":
    main()
