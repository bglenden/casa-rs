#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare flagdata STATE and UV-distance selection against CASA 6.7.6.14."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_array_feed_oracle import assert_flags, fixture as array_feed_fixture
from flagdata_preview_oracle import assert_preserved, auxiliary_content, changed_samples
from flagdata_quack_oracle import assert_equal
from flagdata_summary_oracle import snapshot
from ms_selection_tasks_oracle import add_second_window


def make_fixture(vis):
    arrays, _, slots = array_feed_fixture(vis)
    add_second_window(Path(vis))
    with table(f"{vis}/STATE", readonly=False, ack=False) as states:
        if states.nrows() == 1:
            states.addrows(1)
            for name in states.colnames():
                if states.iscelldefined(name, 0):
                    states.putcell(name, 1, states.getcell(name, 0))
        states.putcell("OBS_MODE", 0, "OBSERVE_TARGET#ON_SOURCE,CALIBRATE_PHASE#ON_SOURCE")
        states.putcell("OBS_MODE", 1, "CALIBRATE_BANDPASS#ON_SOURCE")
    with table(vis, readonly=False, ack=False) as ms:
        state_ids = ms.getcol("FIELD_ID")
        ms.putcol("STATE_ID", state_ids)
        distance = np.linalg.norm(ms.getcol("UVW")[:, :2], axis=1)
        windows = ms.getcol("DATA_DESC_ID")
    with table(f"{vis}/SPECTRAL_WINDOW", ack=False) as spw:
        wavelengths = distance * spw.getcol("REF_FREQUENCY")[windows] / 299792458.0
    assert np.any((np.abs(distance - 14.9) < 1e-10) & (wavelengths < 5000))
    assert np.any((np.abs(distance - 14.9) < 1e-10) & (wavelengths > 5000))
    return arrays, slots, state_ids, distance, wavelengths


def compare_products(vis, metadata):
    arrays, slots, states, meters, wavelengths = metadata
    all_rows = np.ones(states.shape, dtype=bool)
    states_0 = states == 0
    states_1 = states == 1
    within_lambda = (5000 <= wavelengths) & (wavelengths <= 10000)
    cases = (
        ({"intent": "OBSERVE_TARGET#ON_SOURCE"}, states_0),
        ({"intent": "CALIBRATE_PHASE#ON_SOURCE"}, states_0),
        ({"intent": "CALIBRATE_BANDPASS#ON_SOURCE"}, states_1),
        ({"intent": "CALIBRATE_*"}, all_rows),
        ({"intent": np.str_("*PHASE*")}, states_0),
        ({"intent": "1"}, states_1),
        ({"intent": "0~1"}, all_rows),
        ({"intent": "<1"}, states_0),
        ({"intent": "(0,1)"}, all_rows),
        ({"intent": '("CALIBRATE_{PHASE,BANDPASS}*")'}, all_rows),
        ({"intent": "/CALIBRATE_(PHASE|BANDPASS)#ON_SOURCE/"}, all_rows),
        ({"intent": "0,99"}, states_0),
        ({"intent": "OBSERVE*,missing"}, states_0),
        ({"intent": "missing,OBSERVE*"}, states_0),
        ({"intent": "OBSERVE*, "}, states_0),
        ({"intent": "OBSERVE*, \t"}, states_0),
        ({"intent": "OBSERVE*&missing,1"}, states_1),
        ({"intent": ">0&<1,0"}, states_0),
        ({"intent": ">1,0"}, states_0),
        ({"intent": "OBSERVE*\u00a0,CALIBRATE_BAND*"}, states_0),
        ({"intent": "OBSERVE*\x00$ignored"}, states_0),
        ({"intent": "\x00"}, all_rows),
        ({"uvrange": "0~5m"}, meters <= 5.0),
        ({"uvrange": np.str_("<10m")}, meters <= 10.0),
        ({"uvrange": ">10m"}, meters > 10.0),
        ({"uvrange": "5~15"}, (5.0 <= meters) & (meters <= 15.0)),
        ({"uvrange": "0~0.005km"}, meters <= 5.0),
        ({"uvrange": "5~10klambda"}, within_lambda),
        ({"uvrange": "0~5m,5~10klambda"}, (meters <= 5.0) | within_lambda),
        ({"uvrange": "5~10m:50%"}, (2.5 <= meters) & (meters <= 15.0)),
        ({"uvrange": "5m:100%"}, meters == 0.0),
        ({"uvrange": "2.5e1~3.5e1m"}, (25.0 <= meters) & (meters <= 35.0)),
        ({"uvrange": "e-3~5m"}, meters <= 5.0),
        ({"uvrange": "0~5m\x00$ignored"}, meters <= 5.0),
        ({"uvrange": "0~5m\u00a0,5~10m"}, meters <= 5.0),
        ({"uvrange": "\x00"}, all_rows),
        ({"uvrange": "5~10klambda", "intent": "CALIBRATE_BANDPASS*"},
         within_lambda & states_1),
        ({"uvrange": "<15m", "intent": "OBSERVE*", "array": "2,4", "timerange": "12:00:00"},
         (meters <= 15.0) & states_0 & np.isin(arrays, [2, 4]) & (slots == 0)),
    )
    before = snapshot(vis)
    for index, (options, mask) in enumerate(cases):
        reference, native = f"reference-{index}.ms", f"native-{index}.ms"
        for path, module in ((reference, casatasks), (native, tasks)):
            copytree(vis, path)
            assert module.flagdata(vis=path, flagbackup=False, **options) == {}, options
        assert_equal(reference, native)
        assert_flags(native, before, set(np.flatnonzero(mask)))
    return len(cases)


def compare_reports(vis, metadata):
    _, _, states, meters, _ = metadata
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    for options, selected in (
        ({"intent": "missing,OBSERVE*"}, states == 0),
        ({"uvrange": "0~5m"}, meters <= 5),
        ({"intent": "1", "uvrange": ">10m"}, (states == 1) & (meters > 10)),
    ):
        args = dict(vis=vis, mode="summary", spwchan=True, basecnt=True, **options)
        want = casatasks.flagdata(**args)
        assert want["total"] == sum(before[row][1].size for row in np.flatnonzero(selected))
        assert tasks.flagdata(**args) == want, options
        assert_preserved(vis, before, auxiliary)
    for intent, message in (
        ("missing,OBSERVE*", 'No match found for "missing"'),
        ("OBSERVE*, ", 'No match found for " "'),
        (">1,0", "No state ID found >1"),
        (">0&<1,0", "No state found in the range [0,1]"),
        ("0,99", "Partial or no match for State ID list [99]"),
    ):
        warning = tasks.run("flagdata", vis=vis, mode="summary", intent=intent)
        assert "State Expression: " + message in warning.stderr, warning.stderr
        assert_preserved(vis, before, auxiliary)

    options = {"intent": "1", "uvrange": ">10m", "spw": "1:1~2"}
    reference = "preview-reference.ms"
    copytree(vis, reference)
    assert casatasks.flagdata(vis=reference, flagbackup=False, **options) == {}
    changes = changed_samples(before, snapshot(reference))
    assert set(np.flatnonzero(changes)) == set(np.flatnonzero((states == 1) & (meters > 10)))
    result = tasks.run("flagdata", vis=vis, action="calculate", flagbackup=True, **options)
    assert result.value == {}
    report = loads(result.stdout)["result"]
    assert report["changed_samples"] == sum(changes)
    assert report["changed_rows"] == sum(count > 0 for count in changes)
    assert report["backup_version"] is None
    assert_preserved(vis, before, auxiliary)

    flagged = "flagged-state.ms"
    copytree(vis, flagged)
    with table(f"{flagged}/STATE", readonly=False, ack=False) as table_state:
        table_state.putcell("FLAG_ROW", 0, True)
    state_before, state_auxiliary = snapshot(flagged), auxiliary_content(flagged)
    for intent, selected in (("0", states == 0), ("0~1", states >= 0),
                             ("<2", states == 1), ("OBSERVE*,1", states == 1)):
        want = casatasks.flagdata(vis=flagged, mode="summary", intent=intent)
        assert want["total"] == sum(before[row][1].size for row in np.flatnonzero(selected))
        assert tasks.flagdata(vis=flagged, mode="summary", intent=intent) == want
        assert_preserved(flagged, state_before, state_auxiliary)


def compare_refusals(vis):
    cases = (
        ({"intent": "missing"}, RuntimeError, "No match found for state expression:"),
        ({"intent": "OBSERVE_TARGET"}, RuntimeError, "No match found for state expression:"),
        ({"intent": "OBSERVE*&missing"}, RuntimeError, "No match found for state expression:"),
        ({"intent": "99"}, RuntimeError, "No match found for state expression:"),
        ({"intent": "none"}, RuntimeError, "No match found for state expression:"),
        ({"intent": "auto"}, RuntimeError, "No match found for state expression:"),
        ({"intent": "OBSERVE*,"}, RuntimeError, None),
        ({"intent": "OBSERVE*,\t"}, RuntimeError, None),
        ({"intent": "OBSERVE*,\n"}, RuntimeError, None),
        ({"intent": "OBSERVE*,,1"}, RuntimeError, None),
        ({"intent": "0&1"}, RuntimeError, None),
        ({"intent": ["OBSERVE*"]}, TypeError, None),
        ({"intent": []}, TypeError, None),
        ({"intent": np.array("OBSERVE*")}, TypeError, None),
        ({"intent": 0}, AssertionError, None),
        ({"intent": None}, AssertionError, None),
        ({"uvrange": ">9999m"}, RuntimeError, "The selected table has zero rows"),
        ({"uvrange": "5m"}, RuntimeError, "The selected table has zero rows"),
        ({"uvrange": "5~1m"}, RuntimeError, "The selected table has zero rows"),
        ({"uvrange": "5~10KM"}, RuntimeError, "Unrecognized units k found"),
        ({"uvrange": "none"}, RuntimeError, None),
        ({"uvrange": "auto"}, RuntimeError, None),
        ({"uvrange": " "}, RuntimeError, None),
        ({"uvrange": ["0~5m"]}, TypeError, None),
        ({"uvrange": np.array("0~5m")}, TypeError, None),
        ({"uvrange": np.int64(5)}, AssertionError, None),
        ({"uvrange": None}, AssertionError, None),
        ({"uvrange": ["0~5m"], "intent": ["OBSERVE*"]}, TypeError, None),
        ({"intent": "1", "uvrange": ">9999m", "scan": "<2"}, RuntimeError,
         "The selected table has zero rows"),
    )
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    for options, kind, diagnostic in cases:
        errors = []
        for module in (casatasks, tasks):
            try:
                module.flagdata(vis=vis, action="calculate", **options)
            except kind as error:
                errors.append(str(error))
            else:
                raise AssertionError(f"{module.__name__} accepted {options}")
            assert_preserved(vis, before, auxiliary)
        if kind is not RuntimeError:
            assert errors[0] == errors[1], (options, errors)
        elif diagnostic:
            assert all(diagnostic in error for error in errors), (options, errors)
    for module in (casatasks, tasks):
        assert module.flagdata(vis=vis, action="none", intent=["OBSERVE*"], uvrange=["0~5m"]) == {}
    assert_preserved(vis, before, auxiliary)
    return len(cases)


def compare_unicode(vis, metadata):
    _, _, states, _, _ = metadata
    named = "unicode-state.ms"
    copytree(vis, named)
    with table(f"{named}/STATE", readonly=False, ack=False) as table_state:
        table_state.putcell("OBS_MODE", 1, "CALIBRATE_É#ON_SOURCE")
    before, auxiliary = snapshot(named), auxiliary_content(named)
    rows = set(np.flatnonzero(states == 1))
    for intent in ('"CALIBRATE_É#ON_SOURCE"', "/CALIBRATE_É#ON_SOURCE/",
                   '"CALIBRATE_É*"\u00a0,OBSERVE*'):
        args = dict(vis=named, mode="summary", intent=intent)
        want = casatasks.flagdata(**args)
        assert want["total"] == sum(before[row][1].size for row in rows)
        assert tasks.flagdata(**args) == want, intent
        assert_preserved(named, before, auxiliary)
    for options in ({"intent": "\u00a0OBSERVE*"}, {"uvrange": "\u00a00~5m"}):
        for module, kind in ((casatasks, UnicodeDecodeError), (tasks, RuntimeError)):
            try:
                module.flagdata(vis=named, action="calculate", **options)
            except kind:
                pass
            else:
                raise AssertionError(f"{module.__name__} accepted {options}")
            assert_preserved(named, before, auxiliary)


def compare_blank_mode(vis, metadata):
    _, _, states, _, _ = metadata
    blank = "blank-state.ms"
    copytree(vis, blank)
    with table(f"{blank}/STATE", readonly=False, ack=False) as state:
        state.putcell("OBS_MODE", 0, "")
    before = snapshot(blank)
    selected = set(np.flatnonzero(states == 1))
    for index, intent in enumerate(("/.*/", '"*"')):
        reference, native = f"blank-reference-{index}.ms", f"blank-native-{index}.ms"
        for path, module in ((reference, casatasks), (native, tasks)):
            copytree(blank, path)
            assert module.flagdata(vis=path, intent=intent, flagbackup=False) == {}
        assert_equal(reference, native)
        assert_flags(native, before, selected)
    return 2


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-intent-uv-") as work, chdir(work):
        vis = "data.ms"
        facts = make_fixture(vis)
        products = compare_products(vis, facts)
        products += compare_blank_mode(vis, facts)
        compare_reports(vis, facts)
        refusals = compare_refusals(vis)
        compare_unicode(vis, facts)
    print(f"{products} flag products, ten summaries, one preview, one inactive call "
          f"and {refusals} refusals compared with CASA, plus two decoding-failure cases")


if __name__ == "__main__":
    main()
