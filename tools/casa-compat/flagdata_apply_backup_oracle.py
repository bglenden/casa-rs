#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Verify automatic flag versions created before routine selection failures."""

from contextlib import chdir
from datetime import datetime
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_autocorr_oracle import assert_same_visibilities
from flagdata_preview_oracle import auxiliary_content
from flagdata_summary_oracle import snapshot
from flagmanager_rename_oracle import assert_flags, set_flags
from ms_selection_tasks_oracle import add_second_window, casa_table
from record_casa_returns import make_cal_ms


def check_spw_grammar():
    vis = Path("windows.ms")
    make_cal_ms(str(vis), channels=4, scan_seconds=20)
    add_second_window(vis)
    with table(str(vis / "SPECTRAL_WINDOW"), readonly=False, ack=False) as windows:
        for row, name in enumerate(("SCIENCE_LOW", "SCIENCE_HIGH")):
            windows.putcell("NAME", row, name)
    cases = (
        ("SCIENCE_LOW", ((0, 1, 2, 3), ())),
        ("SCIENCE_HIGH:1~2", ((), (1, 2))),
        ("SCIENCE_*:1", ((1,), (1,))),
        ("*:2", ((2,), (2,))),
        ("0:899", ((3,), ())),
        ("0:1~99", ((1, 2, 3), ())),
        ("0:1~2,0:3", ((1, 2, 3), ())),
        ("0:0,0", ((0, 1, 2, 3), ())),
        ("0,0:0", ((0, 1, 2, 3), ())),
        ("0:0;2", ((0, 2), ())),
        ("(1:2)", ((), (2,))),
        ("0:0,99", ((0,), ())),
        (">0", ((), (0, 1, 2, 3))),
        ("<1", ((0, 1, 2, 3), ())),
        ("100GHz~100.003GHz", ((0, 1, 2, 3), ())),
        ("*:100.0005GHz~100.0025GHz", ((1, 2), ())),
        ("0:0,1:100GHz~100.003GHz", ((0,), ())),
    )
    for index, (spw, expected) in enumerate((*cases, ("0:0~3^2", ((0, 2), ())))):
        for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
            path = f"spw-{index}-{name}.ms"
            copytree(vis, path)
            assert task(vis=path, spw=spw, flagbackup=False) == {}
            channels = ((0, 1, 2, 3), ()) if spw == "0:0~3^2" and name == "casa" else expected
            with table(path, ack=False) as ms, casa_table(path) as casa:
                for row, ddid in enumerate(ms.getcol("DATA_DESC_ID")):
                    flags = ms.getcell("FLAG", row)
                    wanted = np.zeros_like(flags)
                    wanted[list(channels[ddid]), :] = True
                    np.testing.assert_array_equal(flags, wanted, err_msg=f"{name} {spw} row {row}")
                    np.testing.assert_array_equal(casa.getcell("FLAG", row).T, wanted)

    for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
        path = f"parenthesized-{name}.ms"
        copytree(vis, path)
        original = snapshot(path)
        frequency = task(vis=path, mode="summary", spw="*:100.0005GHz~100.0025GHz")
        assert frequency["total"] == 80 and frequency["flagged"] == 0, (name, frequency)
        combined = {"vis": path, "field": "0", "spw": "(1:2)"}
        assert task(**combined, flagbackup=False) == {}
        summary = task(**combined, mode="summary")
        assert summary["total"] == 0 and summary["flagged"] == 0, (name, summary)
        assert_same_visibilities(path, original)

        with table(path, readonly=False, ack=False) as ms:
            ms.putcol("DATA_DESC_ID", np.zeros(ms.nrows(), dtype=np.int32))
        original = snapshot(path)
        try:
            task(vis=path, spw="1")
        except RuntimeError as error:
            assert "MSSelectionNullSelection" in str(error), (name, error)
        else:
            raise AssertionError((name, "unparenthesized window with no MAIN rows"))
        assert task(vis=path, spw="(1)") == {}
        listing = casatasks.flagmanager(vis=path, mode="list")
        assert [listing[i]["name"] for i in range(2)] == ["flagdata_1", "flagdata_2"]
        assert_same_visibilities(path, original)
    return len(cases) + 5


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-apply-backup-") as directory, chdir(directory):
        make_cal_ms("source.ms", channels=4, scan_seconds=20)
        input_flags = set_flags("source.ms", 0, shared_row=4)
        original = snapshot("source.ms")
        cases = (
            ({"field": "unknown"}, RuntimeError, True),
            ({"spw": "99"}, RuntimeError, True),
            ({"spw": "0:never"}, RuntimeError, True),
            ({"scan": "999"}, RuntimeError, True),
            ({"antenna": "unknown"}, RuntimeError, True),
            ({"array": "999"}, RuntimeError, True),
            ({"timerange": "then~now"}, RuntimeError, True),
            ({"correlation": ["XX"]}, AttributeError, True),
            ({"mode": "clip", "datacolumn": "unavailable"}, ValueError, True),
            ({"mode": "clip", "timeavg": [True]}, RuntimeError, True),
            ({"mode": "summary", "field": "unknown"}, RuntimeError, False),
            ({"action": "calculate", "field": "unknown"}, RuntimeError, False),
            ({"action": "apply", "field": "unknown", "flagbackup": False}, RuntimeError, False),
            ({"field": ["FLUX"]}, TypeError, True),
            ({"spw": ["0"]}, TypeError, True),
            ({"array": ["0"]}, TypeError, True),
            ({"feed": ["0"]}, TypeError, True),
            ({"scan": ["1"]}, TypeError, True),
            ({"antenna": ["EA01"]}, TypeError, True),
            ({"uvrange": ["0~100m"]}, TypeError, True),
            ({"timerange": ["00:01:00"]}, TypeError, True),
            ({"intent": ["CALIBRATE_PHASE"]}, TypeError, True),
            ({"field": []}, TypeError, True),
            ({"field": np.array("FLUX")}, TypeError, True),
            ({"mode": "summary", "field": ["FLUX"]}, TypeError, False),
            ({"action": "calculate", "field": ["FLUX"]}, TypeError, False),
            ({"field": ["FLUX"], "correlation": ["XX"]}, AttributeError, True),
            ({"field": [0]}, AssertionError, False),
        )
        for index, (options, error_type, backup) in enumerate(cases):
            for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
                vis = f"failure-{index}-{name}.ms"
                copytree("source.ms", vis)
                previous = auxiliary_content(vis)
                before = datetime.now().replace(microsecond=0)
                try:
                    task(vis=vis, **options)
                except error_type as error:
                    if error_type is TypeError:
                        selector = next(key for key, value in options.items()
                                        if isinstance(value, (list, np.ndarray)))
                        assert str(error) == f"argument _{selector} must be a string"
                else:
                    raise AssertionError((name, options))
                after = datetime.now().replace(microsecond=0)
                assert_same_visibilities(vis, original)
                sidecar = Path(f"{vis}.flagversions")
                assert sidecar.exists() == backup, (name, options)
                if backup:
                    listing = casatasks.flagmanager(vis=vis, mode="list")
                    version = listing[0]
                    assert len(listing) == 2 and version["name"] == "flagdata_1"
                    comment = version["comment"]
                    assert comment.startswith("Flags autosave on ")
                    recorded = datetime.strptime(comment, "Flags autosave on %Y-%m-%d %H:%M:%S")
                    assert before <= recorded <= after
                    assert_flags(sidecar / "flags.flagdata_1", input_flags)
                current = auxiliary_content(vis)
                assert current[0][:2] == previous[0][:2]
                assert {path: checksum for path, checksum in current[1].items()
                        if sidecar not in path.parents} == previous[1]
        products = check_spw_grammar()
    print(f"{len(cases)} automatic-backup outcomes and {products} SPW products checked against CASA")


if __name__ == "__main__":
    main()
