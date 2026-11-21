#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare TFCrop time-chunk membership and baseline order against CASA."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_summary_oracle import snapshot
from flagdata_tfcrop_fit_oracle import make_fixture


def labels(path):
    with table(path, ack=False) as main:
        times = main.getcol("TIME")
        integrations = np.searchsorted(np.unique(times), times)
        baseline = (main.getcol("ANTENNA1") == 0) & (main.getcol("ANTENNA2") == 1)
    return integrations, baseline


def split_header_at_scan(path, column):
    subtable = {"OBSERVATION_ID": "OBSERVATION", "DATA_DESC_ID": "DATA_DESCRIPTION"}.get(column)
    index = 1
    if subtable:
        with table(str(Path(path, subtable)), readonly=False, ack=False) as related:
            index = related.nrows()
            related.addrows(1)
            related.copyrows(related, startrowin=0, startrowout=index, nrow=1)
    integration, baseline = labels(path)
    with table(path, readonly=False, ack=False) as main:
        if column == "baseline":
            first, second = main.getcol("ANTENNA1"), main.getcol("ANTENNA2")
            changed = baseline & (integration >= 21)
            first[changed], second[changed] = 1, 0
            main.putcol("ANTENNA1", first)
            main.putcol("ANTENNA2", second)
        else:
            main.putcol(column, np.where(integration < 21, 0, index).astype(np.int32))


def check_cases(libraries):
    make_fixture("source.ms")
    with table("source.ms", readonly=False, ack=False) as main:
        main.putcol("FIELD_ID", np.zeros(main.nrows(), dtype=np.int32))
        order = np.arange(main.nrows() - 1, -1, -1)
        with main.selectrows(order) as shuffled:
            with shuffled.copy("reversed.ms", deep=True, valuecopy=True):
                pass

    cases = (
        ("scan", False, False, "source", ((6, 0),)),
        ("scan", True, False, "source", ((6, 0),)),
        ("50s", False, False, "source", ()),
        (50, True, False, "source", ()),
        (100., False, False, "source", ()),
        ("100s", True, False, "source", ((20, 1), (22, 1))),
        ("210s", False, False, "source", ((6, 0),)),
        ("3.5min", True, False, "source", ((6, 0),)),
        ("100s", True, False, "reversed", ((20, 1), (22, 1))),
        ("100s", True, True, "source", ((20, 0), (22, 0), (20, 1), (22, 1))),
        ("100s", False, True, "source", ()),
        *(("100s", True, False, column, ()) for column in
          ("FIELD_ID", "OBSERVATION_ID", "ARRAY_ID", "DATA_DESC_ID", "baseline")),
    )
    for index, (interval, combine, extend, variant, flagged_times) in enumerate(cases):
        for name, task in libraries:
            path = f"{name}-{index}.ms"
            copytree("reversed.ms" if variant == "reversed" else "source.ms", path)
            if variant not in ("source", "reversed"):
                split_header_at_scan(path, variant)
            integration, baseline = labels(path)
            empty, original_rows = flags(path)
            expected = empty.copy()
            for time, corr in flagged_times:
                expected[baseline & (integration == time), :, corr] = True
            original, auxiliary = snapshot(path), auxiliary_content(path)
            options = dict(vis=path, mode="tfcrop", antenna="0&1", datacolumn="data",
                           flagdimension="time", timefit="poly", timecutoff=2.5,
                           maxnpieces=3, ntime=interval, combinescans=combine,
                           extendflags=extend)
            assert task(action="calculate", **options) == {}
            if name == "native":
                calculated = tasks.run("flagdata", action="calculate", **options)
                assert loads(calculated.stdout)["result"]["changed_samples"] == expected.sum(), options
            assert_preserved(path, original, auxiliary)
            assert task(**options) == {}
            samples, rows = flags(path)
            np.testing.assert_array_equal(samples, expected, err_msg=str(options))
            np.testing.assert_array_equal(rows, original_rows)
            prior, prior_rows = flags(str(Path(path + ".flagversions", "flags.flagdata_1")))
            np.testing.assert_array_equal(prior, empty)
            np.testing.assert_array_equal(prior_rows, original_rows)
            for before, after in zip(original, snapshot(path), strict=True):
                np.testing.assert_array_equal(after[2], before[2])
    return len(cases)


def check_saved_commands(libraries):
    commands = []
    for name, task in libraries:
        path, output = f"{name}-commands.ms", Path(f"{name}.txt")
        copytree("source.ms", path)
        original, auxiliary = snapshot(path), auxiliary_content(path)
        assert task(vis=path, mode="tfcrop", ntime="100s", combinescans=True,
                    timefit="poly", action="none", savepars=True, outfile=str(output)) == {}
        assert_preserved(path, original, auxiliary)
        written = output.read_text()
        assert written.startswith("ntime=100.0 combinescans=True "), written
        commands.append(written)
    if len(commands) == 2:
        assert commands[0] == commands[1], commands


def check_selected_time_extension(libraries):
    source = "time-extension-source.ms"
    copytree("source.ms", source)
    integration, baseline = labels(source)
    seeded_rows = baseline & (integration >= 20) & (integration < 25)
    extended_rows = baseline & (integration >= 20) & (integration < 30)
    seeded, original_rows = flags(source)
    seeded[seeded_rows, 10, 0] = True
    with table(source, readonly=False, ack=False) as main:
        main.putcol("DATA", np.ones_like(main.getcol("DATA")))
        main.putcol("FLAG", seeded)
    for combine in (False, True):
        expected = seeded.copy()
        expected[extended_rows if combine else seeded_rows, 10, :] = True
        for name, task in libraries:
            path = f"time-extension-{name}-{combine}.ms"
            copytree(source, path)
            original, auxiliary = snapshot(path), auxiliary_content(path)
            options = dict(vis=path, mode="tfcrop", antenna="0&1", spw="0:8~15", datacolumn="data",
                           ntime="100s", combinescans=combine, extendflags=True)
            assert task(action="calculate", **options) == {}
            if name == "native":
                calculated = tasks.run("flagdata", action="calculate", **options)
                assert loads(calculated.stdout)["result"]["changed_samples"] == expected.sum() - seeded.sum()
            assert_preserved(path, original, auxiliary)
            assert task(**options) == {}
            samples, rows = flags(path)
            np.testing.assert_array_equal(samples, expected, err_msg=str(options))
            np.testing.assert_array_equal(rows, original_rows)
            prior, prior_rows = flags(str(Path(path + ".flagversions", "flags.flagdata_1")))
            np.testing.assert_array_equal(prior, seeded)
            np.testing.assert_array_equal(prior_rows, original_rows)
    return 2


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-tfcrop-iterator-") as root, chdir(root):
        count = check_cases(libraries)
        count += check_selected_time_extension(libraries)
        check_saved_commands(libraries)
    print(f"{count} TFCrop iterator products and saved commands verified")


if __name__ == "__main__":
    main()
