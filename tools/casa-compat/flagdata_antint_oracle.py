#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check antint's reference-antenna masks, thresholds and artifacts with pinned CASA."""

from contextlib import chdir
from json import loads
from os import environ
from pathlib import Path
from shutil import copytree
from signal import SIGABRT, SIGSEGV
import subprocess
import sys
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_summary_oracle import snapshot
from record_casa_returns import make_cal_ms


def products(path):
    with table(path, ack=False) as main:
        first, second = main.getcol("ANTENNA1"), main.getcol("ANTENNA2")
        times = main.getcol("TIME")
        time_index = np.searchsorted(np.unique(times), times)
        samples = np.zeros_like(main.getcol("FLAG"))
    return first, second, time_index, samples


def write_seed(path, samples):
    with table(path, ack=False, readonly=False) as main:
        main.putcol("FLAG", samples)
        main.putcol("FLAG_ROW", np.zeros(len(samples), dtype=bool))


def check_working_state(path, samples, expected, row_flags, backup_expected):
    actual, actual_rows = flags(path)
    np.testing.assert_array_equal(actual, expected)
    np.testing.assert_array_equal(actual_rows, row_flags)
    backup_path = Path(path + ".flagversions", "flags.flagdata_1")
    if backup_expected:
        backed_up, original_rows = flags(str(backup_path))
        np.testing.assert_array_equal(backed_up, samples)
        assert not original_rows.any()
    else:
        assert not backup_path.exists()


def check_thresholds(libraries):
    make_cal_ms("source.ms", channels=8, scan_seconds=40)
    first, second, time_index, empty = products("source.ms")

    def ref_rows(ref, time):
        return np.flatnonzero((time_index == time) & ((first == ref) | (second == ref)))

    cases = (
        ("all", {}, (1,), slice(None), slice(None)),
        ("all", {"minchanfrac": 5 / 8}, (), slice(None), slice(None)),
        ("all", {"minchanfrac": 4 / 8, "verbose": True}, (1,), slice(None), slice(None)),
        ("none", {"minchanfrac": -0.1}, tuple(range(8)), slice(None), slice(None)),
        ("all", {"antenna": "0&1"}, (), slice(None), slice(None)),
        ("all", {"correlation": "XX"}, (1,), slice(None), 0),
        ("all", {"correlation": "ABS_XX"}, (1,), slice(None), 0),
        ("all", {"spw": "0:1~3"}, (1,), slice(1, 4), slice(None)),
        ("all", {"spw": "0:0", "minchanfrac": 1.0}, (), slice(None), slice(None)),
        ("all", {"antint_ref_antenna": "a0"}, (), slice(None), slice(None)),
        ("other", {"antint_ref_antenna": "A3"}, (1,), slice(None), slice(None)),
        ("complement", {}, (), slice(None), slice(None)),
        ("partial", {}, (), slice(None), slice(None)),
        ("none", {"minchanfrac": float("nan")}, tuple(range(8)), slice(None), slice(None)),
        ("all", {"minchanfrac": float("inf")}, (), slice(None), slice(None)),
    )
    for index, (seed_kind, settings, integrations, channels, corr) in enumerate(cases):
        samples = empty.copy()
        if seed_kind in ("all", "partial", "other"):
            baseline_rows = ref_rows(3 if seed_kind == "other" else 0, 1)
            assert len(baseline_rows) == 4
            if seed_kind == "partial":
                baseline_rows = baseline_rows[:-1]
            for plane, row in enumerate(baseline_rows):
                samples[row, :5, plane % 2] = True
        elif seed_kind == "complement":
            samples[ref_rows(0, 1)[:2], :5, 0] = True
            samples[ref_rows(0, 2)[2:], :5, 1] = True
        affected = np.isin(time_index, integrations)
        for name, task in libraries:
            path = f"threshold-{index}-{name}.ms"
            copytree("source.ms", path)
            write_seed(path, samples)
            expected = samples.copy()
            # Pinned CASA widens decorated selection. Explicit XX supplies
            # the intended scope, with YY already seeded on some rows.
            widened = name == "casa" and settings.get("correlation") == "ABS_XX"
            expected[affected, channels, slice(None) if widened else corr] = True
            expected_rows = affected if widened or not {"spw", "correlation"} & settings.keys() else np.zeros_like(affected)
            original, auxiliary = snapshot(path), auxiliary_content(path)
            options = {"vis": path, "mode": "antint", "antint_ref_antenna": "A0", **settings}
            assert task(action="calculate", **options) == {}
            assert_preserved(path, original, auxiliary)
            if name == "native":
                outcome = tasks.run("flagdata", action="calculate", **options)
                assert loads(outcome.stdout)["result"]["changed_samples"] == (expected != samples).sum()
            assert task(**options) == {}
            check_working_state(path, samples, expected, expected_rows, True)
            for before, after in zip(original, snapshot(path), strict=True):
                np.testing.assert_array_equal(before[2], after[2])
    return len(cases)


def check_single_channel(libraries):
    make_cal_ms("one-channel.ms", channels=1, scan_seconds=20)
    first, second, times, samples = products("one-channel.ms")
    ref_rows = ((first == 0) | (second == 0)) & (times == 0)
    assert ref_rows.sum() == 4
    samples[ref_rows, 0, 1] = True
    expected = samples.copy()
    expected[times == 0] = True
    for name, task in libraries:
        path = f"one-channel-{name}.ms"
        copytree("one-channel.ms", path)
        write_seed(path, samples)
        assert task(vis=path, mode="antint", antint_ref_antenna="A0", minchanfrac=1., flagbackup=False) == {}
        check_working_state(path, samples, expected, times == 0, False)


def check_autocorrelation(libraries):
    path = "with-autocorrelation.ms"
    copytree("source.ms", path)
    first, second, times, samples = products(path)
    target = times == 0
    samples[target & ((first == 0) | (second == 0)), :5, 0] = True
    write_seed(path, samples)
    with table(path, ack=False, readonly=False) as main:
        new = main.nrows()
        fields = {name: main.getcell(name, 0) for name in main.colnames() if main.iscelldefined(name, 0)}
        main.addrows(1)
        for column, value in fields.items():
            main.putcell(column, new, value)
        main.putcell("ANTENNA1", new, 0)
        main.putcell("ANTENNA2", new, 0)
    samples = np.concatenate((samples, samples[:1]))
    target = np.append(target, True)
    expected = samples.copy()
    expected[target] = True
    if any(name == "casa" for name, _ in libraries):
        copytree(path, "pinned-autocorrelation.ms")
        probe = subprocess.run(
            [sys.executable, "-c", "import resource; "
             "resource.setrlimit(resource.RLIMIT_CORE, (0, 0)); "
             "from casatasks import flagdata; "
             "flagdata(vis='pinned-autocorrelation.ms',mode='antint',antint_ref_antenna='A0',flagbackup=False)"],
            capture_output=True, timeout=20, check=False,
        )
        assert probe.returncode in (-SIGSEGV, -SIGABRT), (probe.returncode, probe.stderr.decode())
    for name, task in libraries:
        if name == "native":
            assert task(vis=path, mode="antint", antint_ref_antenna="A0", flagbackup=False) == {}
            check_working_state(path, samples, expected, target, False)


def append_metadata_row(metadata, changed):
    index = metadata.nrows()
    record = {name: metadata.getcell(name, 0) for name in metadata.colnames()
              if metadata.iscelldefined(name, 0)}
    record.update(changed)
    metadata.addrows(1)
    for name, value in record.items():
        metadata.putcell(name, index, value)


def check_spectral_windows(libraries):
    copytree("source.ms", "two-windows.ms")
    with table("two-windows.ms/SPECTRAL_WINDOW", ack=False, readonly=False) as windows:
        append_metadata_row(windows, {"NAME": "SPW1"})
    with table("two-windows.ms/DATA_DESCRIPTION", ack=False, readonly=False) as descriptions:
        append_metadata_row(descriptions, {"SPECTRAL_WINDOW_ID": np.int32(1)})
    with table("two-windows.ms", ack=False, readonly=False) as main, table("source.ms", ack=False) as source:
        count = source.nrows()
        source.copyrows(main)
        main.putcol("DATA_DESC_ID", np.ones(count, dtype=np.int32), startrow=count, nrow=count)
    first, second, time_index, empty = products("two-windows.ms")
    with table("two-windows.ms", ack=False) as main:
        windows = main.getcol("DATA_DESC_ID")
    for seeded_window in (0, 1):
        seeds = empty.copy()
        at_time = time_index == 0
        reference = (first == 0) | (second == 0)
        seeds[at_time & reference & (windows == seeded_window), :5, 0] = True
        for explicit in (False, True):
            options = {"spw": str(seeded_window)} if explicit else {}
            for name, task in libraries:
                path = f"spectral-window-{seeded_window}-{explicit}-{name}.ms"
                copytree("two-windows.ms", path)
                write_seed(path, seeds)
                assert task(vis=path, mode="antint", antint_ref_antenna="A0", flagbackup=False, **options) == {}
                # CASA remembers the first window's decision by timestamp.
                # Explicit selections provide the reference for each window.
                both = name == "casa" and seeded_window == 0 and not explicit
                affected = at_time if both else at_time & (windows == seeded_window)
                expected = seeds.copy()
                expected[affected] = True
                row_flags = np.zeros_like(affected) if explicit else affected
                check_working_state(path, seeds, expected, row_flags, False)


def check_saved_commands(libraries):
    written = []
    for name, task in libraries:
        path, out = f"commands-{name}.ms", Path(f"commands-{name}.txt")
        copytree("source.ms", path)
        original, auxiliary = snapshot(path), auxiliary_content(path)
        options = dict(vis=path, mode="antint", antint_ref_antenna="A0", minchanfrac=0.5,
                       verbose=True, savepars=True)
        assert task(action="none", outfile=str(out), **options) == {}
        assert_preserved(path, original, auxiliary)
        assert task(flagbackup=False, **options) == {}
        with table(f"{path}/FLAG_CMD", ack=False) as commands:
            assert commands.nrows() == 1 and commands.getcell("APPLIED", 0)
            command = commands.getcell("COMMAND", 0)
        assert command == "antint_ref_antenna='A0' minchanfrac=0.5 verbose=True mode='antint'"
        written.append((out.read_text(), command))
    if len(written) == 2:
        assert written[0] == written[1]


def check_inputs(libraries):
    cases = (
        ("antint_ref_antenna", 0, "must be of cStr type"),
        ("antint_ref_antenna", None, "null value not allowed"),
        ("minchanfrac", "0.6", "must be of cFloat type"),
        ("minchanfrac", np.array(0.6), "must be of cFloat type"),
        ("verbose", [True], "must be of cBool type"),
    )
    for name, task in libraries:
        path = f"invalid-{name}.ms"
        copytree("source.ms", path)
        before, auxiliary = snapshot(path), auxiliary_content(path)
        for parameter, value, reason in cases:
            options = {"antint_ref_antenna": "A0", parameter: value}
            try:
                task(vis=path, mode="antint", **options)
            except AssertionError as error:
                assert str(error) == str({parameter: [reason]})
            else:
                raise AssertionError((name, parameter, value))
            assert_preserved(path, before, auxiliary)
    return len(cases)


def check_provider_and_cli_thresholds():
    path = "typed-threshold.ms"
    copytree("source.ms", path)
    original, auxiliary = snapshot(path), auxiliary_content(path)
    output = Path("invalid-commands.txt")
    for value in ("bad", [0.5]):
        try:
            tasks.run("flagdata", vis=path, mode="antint", antint_ref_antenna="A0",
                      action="none", savepars=True, minchanfrac=value, outfile=str(output))
        except tasks.TaskParameterError as error:
            assert error.completion.parameter == "minchanfrac"
        else:
            raise AssertionError(value)
        assert not output.exists()
        assert_preserved(path, original, auxiliary)
    binary = environ.get("CASARS_FLAGDATA_BIN", "flagdata")
    arguments = [binary, "--vis", path, "--mode", "antint", "--antint_ref_antenna", "A0"]
    invalid = subprocess.run(
        [*arguments, "--minchanfrac", "bad", "--action", "none", "--savepars", "--outfile", str(output)],
        capture_output=True, check=False,
    )
    assert invalid.returncode != 0 and not output.exists()
    assert_preserved(path, original, auxiliary)

    _, _, _, empty = products(path)
    for threshold in ("inf", "NaN"):
        write_seed(path, empty)
        subprocess.run([*arguments, "--minchanfrac", threshold, "--no-flagbackup"],
                       capture_output=True, check=True)
        samples, rows = flags(path)
        expected = threshold == "NaN"
        assert np.all(samples == expected)
        assert np.all(rows == expected)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-antint-") as directory, chdir(directory):
        count = check_thresholds(libraries)
        check_single_channel(libraries)
        check_autocorrelation(libraries)
        check_spectral_windows(libraries)
        check_saved_commands(libraries)
        invalid = check_inputs(libraries)
        check_provider_and_cli_thresholds()
    print(f"{count} antint products, physical single-channel control, autocorrelation, SPW isolation, "
          f"saved commands, {invalid} type refusals, and raw provider and CLI thresholds verified")


if __name__ == "__main__":
    main()
