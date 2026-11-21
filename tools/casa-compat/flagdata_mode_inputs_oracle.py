#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare flagging task argument rejection and data-column scope."""

from contextlib import chdir
import os
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory
from time import time
from unittest.mock import patch

import casatasks
from casacore.tables import table
from casatools import ctsys
import numpy as np

from casars import tasks
from flagdata_history_oracle import MJD_UNIX_EPOCH_S, compare_new_rows
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_summary_oracle import snapshot
from record_casa_returns import make_cal_ms


def assert_failure(task, error_type, options):
    try:
        task(**options)
    except error_type as error:
        assert type(error) is error_type, (options, type(error))
        return str(error)
    raise AssertionError(options)


def check_wrong_schema():
    cases = {
        "flagdata": (
            {"action": "UNKNOWN"}, {"action": "APPLY", "savepars": True},
            {"action": None}, {"action": True, "mode": None}, {"mode": ""},
            {"mode": np.array("manual")}, {"flagbackup": 1}, {"savepars": None},
            {"overwrite": "true"}, {"outfile": None}, {"cmdreason": None},
            {"datacolumn": None}, {"datacolumn": [1]}, {"vis": None},
            {"vis": ""}, {"vis": "missing.ms"}, {"vis": False},
        ),
        "flagmanager": (
            {"mode": None}, {"mode": "BOGUS"}, {"mode": True},
            {"mode": "list", "merge": None}, {"mode": "list", "comment": np.int64(10)},
            {"mode": "save", "versionname": "science", "comment": None},
            {"mode": "save", "versionname": "science", "merge": True},
            {"vis": None}, {"vis": ""}, {"vis": "missing.ms"}, {"vis": False},
        ),
    }
    before, sidecars = snapshot("source.ms"), auxiliary_content("source.ms")
    for name, options in cases.items():
        for arguments in options:
            failures = []
            for library in (casatasks, tasks):
                failures.append(assert_failure(getattr(library, name), AssertionError,
                                               {"vis": "source.ms", **arguments}))
                assert_preserved("source.ms", before, sidecars)
            assert failures[0] == failures[1], (name, arguments, failures)
    return sum(map(len, cases.values()))


def check_inactive_columns():
    with table("source.ms/HISTORY", ack=False) as history:
        original = history.nrows()
    cases = (("manual", ["DATA"]), ("unflag", ["FLOAT_DATA"]), ("quack", ("MODEL",)),
             ("summary", ["DATA"]), ("manual", "$unknown"), ("manual", "  MODEL_DATA "),
             ("manual", np.array("MODEL")), ("manual", np.array(["MODEL"])))
    for index, (mode, column) in enumerate(cases):
        paths = (f"{index}-casa.ms", f"{index}-native.ms")
        before, output = time() + MJD_UNIX_EPOCH_S, []
        for task, path in zip((casatasks.flagdata, tasks.flagdata), paths, strict=True):
            copytree("source.ms", path)
            output.append(task(vis=path, datacolumn=column, mode=mode, flagbackup=False))
        assert output[0] == output[1], (mode, column)
        for expected, actual in zip(flags(paths[0]), flags(paths[1]), strict=True):
            np.testing.assert_array_equal(actual, expected, err_msg=repr((mode, column)))
        if mode != "summary":
            compare_new_rows(paths, original, 1, (before, time() + MJD_UNIX_EPOCH_S))
    return len(cases)


def check_data_root():
    results = []
    previous = ctsys.getpath()
    try:
        for name, library in (("casa", casatasks), ("native", tasks)):
            root = Path(name).resolve()
            root.mkdir()
            path = root / "nested.ms"
            copytree("source.ms", path)
            ctsys.setpath([str(root), *previous])
            before = snapshot(str(path))
            with patch.dict(os.environ, {"CASA_RS_MEASURESPATH": str(root)}):
                results.append(library.flagdata(vis="nested.ms", mode="summary"))
                assert library.flagmanager(vis="nested.ms") == {"MS": str(path)}
            assert Path(str(path) + ".flagversions", "FLAG_VERSION_LIST").read_bytes() == b""
            for actual, expected in zip(snapshot(str(path)), before, strict=True):
                for actual_cell, expected_cell in zip(actual, expected, strict=True):
                    np.testing.assert_array_equal(actual_cell, expected_cell)
        assert results[0] == results[1] and results[0]["total"] > 0
    finally:
        ctsys.setpath(previous)


def check_active_column_errors():
    cases = (
        ("clip", "apply", "", ["DATA"], AttributeError, "list"),
        ("clip", "calculate", "", ["DATA"], AttributeError, "list"),
        ("clip", "none", "", ["DATA"], AttributeError, "list"),
        ("clip", "apply", "", np.array("DATA"), AttributeError, "numpy.ndarray"),
        ("manual", "apply", "report", ["DATA"], AttributeError, "list"),
        ("manual", "calculate", "data", ["DATA"], AttributeError, "list"),
        ("clip", "apply", "", "", ValueError, None),
        ("clip", "apply", "", " DATA ", ValueError, None),
        ("clip", "apply", "", "$invalid", ValueError, None),
        ("clip", "apply", "", np.array("DATA"), AttributeError, "numpy.ndarray",
         {"correlation": ["XX"]}),
        ("manual", "apply", "report", ["DATA"], TypeError, None, {"field": ["0"]}),
    )
    for index, (mode, action, display, column, error, container, *other_options) in enumerate(cases):
        errors = []
        for library in (casatasks, tasks):
            path = f"{index}-{library.__name__}.ms"
            copytree("source.ms", path)
            before, auxiliary = snapshot(path), auxiliary_content(path)
            extra = other_options[0] if other_options else {}
            errors.append(assert_failure(library.flagdata, error,
                                         dict(vis=path, mode=mode, action=action,
                                              display=display, datacolumn=column, **extra)))
            for actual, expected in zip(snapshot(path), before, strict=True):
                for actual_cell, expected_cell in zip(actual, expected, strict=True):
                    np.testing.assert_array_equal(actual_cell, expected_cell)
            backup = Path(f"{path}.flagversions/flags.flagdata_1")
            assert backup.is_dir() == (action == "apply")
            if backup.is_dir():
                samples, rows = flags(str(backup))
                np.testing.assert_array_equal(samples, np.stack([row[1] for row in before]))
                np.testing.assert_array_equal(rows, [row[0] for row in before])
                roots, content = auxiliary_content(path)
                assert roots[:2] == auxiliary[0][:2]
                assert {file: digest for file, digest in content.items() if file.is_relative_to(path)} == auxiliary[1]
            else:
                assert_preserved(path, before, auxiliary)
        if container:
            detail = f"'{container}' object has no attribute 'upper'"
        elif error is TypeError:
            detail = "argument _field must be a string"
        else:
            detail = "Failed to parse parameters for mode clip"
        assert errors[0] == errors[1] == detail
    return len(cases)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-mode-inputs-") as directory, chdir(directory):
        make_cal_ms("source.ms", channels=4, scan_seconds=20)
        schema = check_wrong_schema()
        inactive = check_inactive_columns()
        check_data_root()
        active = check_active_column_errors()
    print(f"{schema} schema rejections, {inactive} inactive data-column products, {active} active failures, and data-root lookup verified")


if __name__ == "__main__":
    main()
