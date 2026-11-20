#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare manual autocorrelation flagging against CASA 6.7.6.14."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content, changed_samples, make_fixture
from flagdata_quack_oracle import assert_equal
from flagdata_scan_baseline_oracle import with_autocorrelations
from flagdata_summary_oracle import snapshot


def make_mixed_fixture(path):
    with_autocorrelations(path)
    with table(path, readonly=False, ack=False) as ms:
        antenna1, antenna2 = ms.getcol("ANTENNA1"), ms.getcol("ANTENNA2")
        original = int(np.flatnonzero((antenna1 == 1) & (antenna1 != antenna2))[0])
        new_row = ms.nrows()
        ms.addrows(1)
        for name in ms.colnames():
            if ms.iscelldefined(name, original):
                value = 1 if name == "ANTENNA2" else ms.getcell(name, original)
                ms.putcell(name, new_row, value)
        ms.putcell("FLAG", new_row, np.zeros_like(ms.getcell("FLAG", original)))
        ms.putcell("FLAG_ROW", new_row, False)

        cross = int(np.flatnonzero(antenna1 != antenna2)[0])
        ms.putcell("FLAG", cross, np.ones_like(ms.getcell("FLAG", cross)))
        ms.putcell("FLAG_ROW", cross, True)


def antenna_facts(path):
    with table(path, ack=False) as ms:
        return ms.getcol("ANTENNA1"), ms.getcol("ANTENNA2"), ms.getcol("SCAN_NUMBER")


def commands_and_versions(path):
    # CASA can record each apply in HISTORY. The repeated flag check covers
    # the visibility data and the separate command and version catalogs.
    directories, files = auxiliary_content(path)
    history = Path(path, "HISTORY")
    return directories[1:], {file: digest for file, digest in files.items()
                             if history not in file.parents}


def assert_same_visibilities(path, source):
    for before, after in zip(source, snapshot(path), strict=True):
        for expected, actual in zip(before, after, strict=True):
            np.testing.assert_array_equal(actual, expected)


def compare_apply(vis, source, facts):
    first, second, scans = facts
    auto = first == second
    cases = (
        ({"autocorr": True}, auto, slice(None)),
        ({"autocorr": True, "mode": "manualflag"}, auto, slice(None)),
        ({"autocorr": True, "scan": "1"}, auto & (scans == 1), slice(None)),
        ({"autocorr": True, "scan": "2", "spw": "0:1~2"}, auto & (scans == 2), slice(1, 3)),
        ({"autocorr": True, "antenna": "A0&&&"}, auto & (first == 0), slice(None)),
        ({"autocorr": True, "antenna": "A0&A1"}, np.zeros(auto.shape, dtype=bool), slice(None)),
        ({"autocorr": False}, np.ones(auto.shape, dtype=bool), slice(None)),
        ({"autocorr": np.bool_(False)}, np.ones(auto.shape, dtype=bool), slice(None)),
    )
    for index, (options, selected, channels) in enumerate(cases):
        reference, native = f"reference-{index}.ms", f"native-{index}.ms"
        for path, module in ((reference, casatasks), (native, tasks)):
            copytree(vis, path)
            assert module.flagdata(vis=path, flagbackup=False, **options) == {}, options
            if index == 0:
                applied, catalogs = snapshot(path), commands_and_versions(path)
                assert module.flagdata(vis=path, flagbackup=False, **options) == {}
                assert_same_visibilities(path, applied)
                assert commands_and_versions(path) == catalogs
        assert_equal(reference, native)
        for row, (before, after) in enumerate(zip(source, snapshot(native), strict=True)):
            expected = before[1].copy()
            if selected[row]:
                expected[channels] = True
            else:
                assert after[0] == before[0], row
            np.testing.assert_array_equal(after[1], expected, err_msg=str(options))
            np.testing.assert_array_equal(after[2], before[2])
    return len(cases)


def compare_numpy_protection(vis, source, facts):
    auto = facts[0] == facts[1]
    wrong, reference, native = "numpy-casa.ms", "boolean-casa.ms", "numpy-native.ms"
    for path in (wrong, reference, native):
        copytree(vis, path)
    assert casatasks.flagdata(vis=wrong, autocorr=np.bool_(True), flagbackup=False) == {}
    assert casatasks.flagdata(vis=reference, autocorr=True, flagbackup=False) == {}
    assert tasks.flagdata(vis=native, autocorr=np.bool_(True), flagbackup=False) == {}
    assert_equal(reference, native)
    wrong_flags = changed_samples(source, snapshot(wrong))
    native_flags = changed_samples(source, snapshot(native))
    assert any(change for row, change in enumerate(wrong_flags) if not auto[row])
    assert set(np.flatnonzero(native_flags)) == set(np.flatnonzero(auto))


def compare_other_modes(vis, source):
    for mode, extra in (("unflag", {}), ("clip", {"clipzeros": True})):
        reference, native = f"{mode}-reference.ms", f"{mode}-native.ms"
        for path, module in ((reference, casatasks), (native, tasks)):
            copytree(vis, path)
            assert module.flagdata(vis=path, mode=mode, autocorr=True, flagbackup=False, **extra) == {}
        assert_equal(reference, native)
        assert any(changed_samples(source, snapshot(native)))

    auxiliary = auxiliary_content(vis)
    args = dict(vis=vis, mode="summary", autocorr=True, basecnt=True)
    summary = casatasks.flagdata(**args)
    assert summary["total"] == sum(flags.size for _, flags, _ in source)
    assert tasks.flagdata(**args) == summary
    assert_preserved(vis, source, auxiliary)
    for module in (casatasks, tasks):
        assert module.flagdata(vis=vis, autocorr=True, action="none") == {}
        assert_preserved(vis, source, auxiliary)


def compare_preview(vis, source):
    auxiliary = auxiliary_content(vis)
    reference = "calculate-reference.ms"
    copytree(vis, reference)
    options = {"autocorr": True, "scan": "1", "spw": "0:1~2"}
    assert casatasks.flagdata(vis=reference, flagbackup=False, **options) == {}
    deltas = changed_samples(source, snapshot(reference))
    assert sum(deltas) > 0
    assert casatasks.flagdata(vis=vis, action="calculate", **options) == {}
    result = tasks.run("flagdata", vis=vis, action="calculate", **options)
    assert result.value == {}
    report = loads(result.stdout)["result"]
    assert report["changed_samples"] == sum(deltas)
    assert report["changed_rows"] == sum(delta > 0 for delta in deltas)
    assert report["backup_version"] is None
    assert_preserved(vis, source, auxiliary)


def compare_time_defaults(vis, facts):
    source_path = "two-days.ms"
    copytree(vis, source_path)
    auto = facts[0] == facts[1]
    with table(source_path, readonly=False, ack=False) as ms:
        midnight = (float(ms.getcell("TIME", 0)) // 86400) * 86400
        noon = midnight + 12 * 3600
        times = np.where(auto, noon + 86400, noon)
        for name in ("TIME", "TIME_CENTROID"):
            ms.putcol(name, times)
    before = snapshot(source_path)
    for module in (casatasks, tasks):
        catalogs = commands_and_versions(source_path)
        # The C++ agent applies autocorr after MSSelection has taken its
        # calendar defaults from the first day, which has cross-correlations.
        assert module.flagdata(vis=source_path, timerange="12:00:00",
                               autocorr=True, flagbackup=False) == {}
        assert_same_visibilities(source_path, before)
        assert commands_and_versions(source_path) == catalogs


def compare_backups(vis, source):
    cross = "cross-only.ms"
    make_fixture(cross)
    for stem, input_path, before in (("mixed", vis, source), ("cross", cross, snapshot(cross))):
        names = []
        for suffix, module in (("reference", casatasks), ("native", tasks)):
            path = f"{stem}-backup-{suffix}.ms"
            copytree(input_path, path)
            assert module.flagdata(vis=path, autocorr=True, flagbackup=True) == {}
            changes = changed_samples(before, snapshot(path))
            assert (sum(changes) > 0) == (stem == "mixed")
            entries = casatasks.flagmanager(vis=path, mode="list")
            backups = [entry["name"] for entry in entries.values()
                       if isinstance(entry, dict) and entry["name"].startswith("flagdata_")]
            assert len(backups) == 1, entries
            names.append(backups[0])
            assert casatasks.flagmanager(vis=path, mode="restore", versionname=backups[0]) is None
            assert_same_visibilities(path, before)
        assert names[0] == names[1]


def compare_refusals(vis, source):
    auxiliary = auxiliary_content(vis)
    cases = (
        {"autocorr": 1},
        {"autocorr": "true"},
        {"autocorr": []},
        {"autocorr": np.array(True)},
        {"autocorr": None},
        {"autocorr": 1, "mode": "summary"},
        {"autocorr": 1, "action": "none"},
    )
    for options in cases:
        errors = []
        for module in (casatasks, tasks):
            try:
                module.flagdata(vis=vis, **options)
            except AssertionError as error:
                errors.append(str(error))
            else:
                raise AssertionError(f"{module.__name__} accepted {options}")
            assert_preserved(vis, source, auxiliary)
        assert errors[0] == errors[1], (options, errors)
    return len(cases)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-autocorr-") as work, chdir(work):
        vis = "source.ms"
        make_mixed_fixture(vis)
        source, facts = snapshot(vis), antenna_facts(vis)
        products = compare_apply(vis, source, facts)
        compare_numpy_protection(vis, source, facts)
        compare_other_modes(vis, source)
        compare_preview(vis, source)
        compare_time_defaults(vis, facts)
        compare_backups(vis, source)
        refusals = compare_refusals(vis, source)
    print(f"{products} manual flag products, three other modes, one preview, one inactive "
          f"call, two backup restorations and {refusals} refusals compared with CASA, "
          "plus repeated application, logical date defaults and one verified NumPy switch protection")


if __name__ == "__main__":
    main()
