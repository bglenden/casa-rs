#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare field selection through public flagdata in CASA and casa-rs."""

from contextlib import chdir
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content, make_fixture
from flagdata_quack_oracle import assert_equal
from flagdata_summary_oracle import snapshot


def compare_apply():
    cases = (
        ("FLUX", {0}, {}),
        ("PH*", {1}, {}),
        ("/FL.*/", {0}, {}),
        ('"FL*"', {0}, {}),
        ("0,PHASE", {0, 1}, {}),
        ("0,99", {0}, {}),
        ("0~5", {0, 1}, {}),
        (">0", {1}, {}),
        ("  ", {0, 1}, {}),
        (" \u00a0 ", {0, 1}, {}),
        ("*", {0, 1}, {}),
        (np.str_("FLUX"), {0}, {}),
        ("FLUX,PHASE", {0}, {"scan": "1"}),
        ("PHASE", {1}, {"spw": "0:1~2"}),
    )
    for index, (field, expected_fields, options) in enumerate(cases):
        reference, native = f"reference-{index}.ms", f"native-{index}.ms"
        source = snapshot("data.ms")
        for path, module in ((reference, casatasks), (native, tasks)):
            copytree("data.ms", path)
            assert module.flagdata(vis=path, field=field, flagbackup=False, **options) == {}
        assert_equal(reference, native)
        with table(native, ack=False) as ms:
            field_ids = ms.getcol("FIELD_ID")
        for row, (original, final) in enumerate(zip(source, snapshot(native), strict=True)):
            np.testing.assert_array_equal(final[2], original[2])
            if field_ids[row] not in expected_fields:
                np.testing.assert_array_equal(final[1], original[1])
                assert final[0] == original[0]
            elif "spw" not in options:
                assert final[1].all() and final[0]
    return len(cases)


def compare_read_only():
    original, auxiliary = snapshot("data.ms"), auxiliary_content("data.ms")
    for field in ("FLUX", "PH*", "0,PHASE", "  ", ""):
        options = {"vis": "data.ms", "mode": "summary", "field": field}
        assert tasks.flagdata(**options) == casatasks.flagdata(**options)
        assert_preserved("data.ms", original, auxiliary)
    for field in (["FLUX"], []):
        for action in ("none", ""):
            for module in (casatasks, tasks):
                assert module.flagdata(vis="data.ms", field=field, action=action) == {}
            assert_preserved("data.ms", original, auxiliary)


def compare_refusals():
    original, auxiliary = snapshot("data.ms"), auxiliary_content("data.ms")
    cases = (
        ("flux", RuntimeError),
        ("FLUX,bogus", RuntimeError),
        ("99", RuntimeError),
        ("0~", RuntimeError),
        ("\t", RuntimeError),
        ("\n", RuntimeError),
        (" \t ", RuntimeError),
        ("none", RuntimeError),
        ("auto", RuntimeError),
        ("FLUX", RuntimeError),
        (["FLUX"], TypeError),
        ([], TypeError),
        (np.array(["FLUX"]), TypeError),
        (np.array("FLUX"), TypeError),
        (0, AssertionError),
        ([0], AssertionError),
        (np.array("FLUX", dtype=object), AssertionError),
        (None, AssertionError),
    )
    for field, kind in cases:
        options = {"scan": "2"} if isinstance(field, str) and field == "FLUX" else {}
        errors = []
        for module in (casatasks, tasks):
            try:
                module.flagdata(vis="data.ms", field=field, action="calculate", **options)
            except kind as error:
                errors.append(str(error))
            else:
                raise AssertionError(f"{module.__name__} accepted {field!r}")
            assert_preserved("data.ms", original, auxiliary)
        if kind is not RuntimeError:
            assert errors[0] == errors[1], (field, errors)
        elif options:
            for message in errors:
                assert "The selected table has zero rows" in message
        elif isinstance(field, str) and field == "99":
            assert "Field Expression" in errors[0]
            assert "field selector" in errors[1]
            assert all("zero rows" not in message for message in errors)
    return len(cases)


def compare_invalid_unicode():
    original, auxiliary = snapshot("data.ms"), auxiliary_content("data.ms")
    # The pinned binder cannot decode the first byte of this invalid FIELD
    # token in its diagnostic. The native task returns a readable error.
    for module, kind in ((casatasks, UnicodeDecodeError), (tasks, RuntimeError)):
        try:
            module.flagdata(vis="data.ms", field="\u00a0", flagbackup=False)
        except kind:
            assert_preserved("data.ms", original, auxiliary)
        else:
            raise AssertionError(f"{module.__name__} accepted a bare nonbreaking space")


def compare_literal_names():
    vis = "literal-names.ms"
    copytree("data.ms", vis)
    cases = (
        (("none", "auto"), (("none", "none"), ("auto", "auto"))),
        (("CODE$", "OTHER"), (("CODE$", "CODE$"), ('"CODE$"', "CODE$"), ("/OTHER$/", "OTHER"))),
    )
    for names, selectors in cases:
        with table(f"{vis}/FIELD", readonly=False, ack=False) as fields:
            fields.putcol("NAME", names)
        original, auxiliary = snapshot(vis), auxiliary_content(vis)
        for selector, name in selectors:
            args = {"vis": vis, "mode": "summary", "field": selector}
            expected = casatasks.flagdata(**args)
            assert set(expected["field"]) == {name}
            assert tasks.flagdata(**args) == expected
            assert_preserved(vis, original, auxiliary)


def compare_backup():
    vis = "backed-up.ms"
    copytree("data.ms", vis)
    original = snapshot(vis)
    assert tasks.flagdata(vis=vis, field="PHASE", flagbackup=True) == {}
    assert any(not np.array_equal(before[1], after[1]) for before, after in zip(original, snapshot(vis), strict=True))
    casatasks.flagmanager(vis=vis, mode="restore", versionname="flagdata_1")
    for before, after in zip(original, snapshot(vis), strict=True):
        for expected, actual in zip(before, after, strict=True):
            np.testing.assert_array_equal(actual, expected)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-field-") as work, chdir(work):
        make_fixture("data.ms")
        products = compare_apply()
        compare_read_only()
        compare_literal_names()
        refusals = compare_refusals()
        compare_invalid_unicode()
        compare_backup()
    print(f"{products} field-selected products, ten summaries, four inactive calls and {refusals} refusals matched CASA")
    print("Invalid UTF-8 diagnostics left flags unchanged. CASA restored the native backup to the original flags.")


if __name__ == "__main__":
    main()
