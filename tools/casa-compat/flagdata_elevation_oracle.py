#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check elevation flagging with CASA, casacore, and independent geometry."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
from casatools import measures
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_summary_oracle import snapshot
from record_casa_returns import make_cal_ms


def make_fixture(path):
    make_cal_ms(path, channels=4, scan_seconds=30, stokes="XX XY YX YY")
    with table(f"{path}/FIELD", readonly=False, ack=False) as field:
        direction = field.getcell("PHASE_DIR", 1)
        direction[0, 0] += np.pi
        field.putcell("PHASE_DIR", 1, direction)
    with table(f"{path}/ANTENNA", readonly=False, ack=False) as antennas:
        position = antennas.getcell("POSITION", 4)
        position[0] += 1e5
        antennas.putcell("POSITION", 4, position)


def expected_elevation(path):
    with table(f"{path}/ANTENNA", ack=False) as antennas:
        positions = antennas.getcol("POSITION")
    with table(f"{path}/FIELD", ack=False) as fields:
        directions = fields.getcol("PHASE_DIR")[:, 0, :]
        reference = fields.getcolkeyword("PHASE_DIR", "MEASINFO")["Ref"]
    with table(path, ack=False) as ms:
        ant1, ant2, ids, times = [ms.getcol(name) for name in
                                 ("ANTENNA1", "ANTENNA2", "FIELD_ID", "TIME")]
        time_reference = ms.getcolkeyword("TIME", "MEASINFO")["Ref"]
    coordinates = measures()
    elevations = {}
    for field, seconds in sorted(set(zip(ids, times, strict=True))):
        coordinates.doframe(coordinates.epoch(time_reference, f"{seconds:.17g}s"))
        direction = coordinates.direction(reference, *(f"{angle:.17g}rad" for angle in directions[field]))
        for antenna, xyz in enumerate(positions):
            coordinates.doframe(coordinates.position("ITRF", *(f"{metres:.17g}m" for metres in xyz)))
            horizon = coordinates.measure(direction, "AZEL")
            assert horizon["m1"]["unit"] == "rad"
            elevations[field, seconds, antenna] = np.degrees(horizon["m1"]["value"])
    both = np.array([(elevations[field, seconds, left], elevations[field, seconds, right])
                     for field, seconds, left, right in zip(ids, times, ant1, ant2, strict=True)])
    return ids, both


def check_masks(task_pairs):
    fields, elevations = expected_elevation("source.ms")
    high_field = elevations[fields == 0]
    cutoff = (high_field.min() + high_field.max()) / 2
    assert np.any((high_field[:, 0] < cutoff) != (high_field[:, 1] < cutoff))
    cases = (
        ({}, 0, 90, np.ones_like(fields, dtype=bool), slice(None), slice(None)),
        ({}, -90, 0, np.ones_like(fields, dtype=bool), slice(None), slice(None)),
        ({}, -90, 90, np.ones_like(fields, dtype=bool), slice(None), slice(None)),
        ({}, 90, -90, np.ones_like(fields, dtype=bool), slice(None), slice(None)),
        ({}, float("nan"), 90, np.ones_like(fields, dtype=bool), slice(None), slice(None)),
        ({}, -90, float("-inf"), np.ones_like(fields, dtype=bool), slice(None), slice(None)),
        ({"field": "FLUX"}, cutoff, 90, fields == 0, slice(None), slice(None)),
        ({"field": "FLUX"}, -90, cutoff, fields == 0, slice(None), slice(None)),
        ({"field": "PHASE", "spw": "0:1~2", "correlation": "YY"},
         0, 90, fields == 1, slice(1, 3), slice(3, 4)),
    )
    for index, (selections, lower, upper, row_scope, channels, correlations) in enumerate(cases):
        outside = np.any((elevations < lower) | (elevations > upper), axis=1) & row_scope
        expected = np.zeros((fields.size, 4, 4), dtype=bool)
        expected[outside, channels, correlations] = True
        for name, task in task_pairs:
            path = f"elevation-{index}-{name}.ms"
            copytree("source.ms", path)
            original, sidecars = snapshot(path), auxiliary_content(path)
            options = dict(vis=path, mode="elevation", lowerlimit=lower, upperlimit=upper, **selections)
            assert task(action="calculate", **options) == {}
            if name == "native":
                report = loads(tasks.run("flagdata", action="calculate", **options).stdout)["result"]
                assert report["changed_samples"] == expected.sum()
                assert report["changed_rows"] == outside.sum()
                assert report["backup_version"] is None
            assert_preserved(path, original, sidecars)
            assert task(**options) == {}
            samples, rows = flags(path)
            np.testing.assert_array_equal(samples, expected, err_msg=str(options))
            np.testing.assert_array_equal(rows, expected.all(axis=(1, 2)), err_msg=str(options))
            backup = Path(f"{path}.flagversions", "flags.flagdata_1")
            backup_samples, backup_rows = flags(str(backup))
            assert not backup_samples.any() and not backup_rows.any()
            for prior, after in zip(original, snapshot(path), strict=True):
                np.testing.assert_array_equal(after[2], prior[2])
    return len(cases)


def check_limit_inputs(task_pairs):
    invalid = (("lowerlimit", "2"), ("lowerlimit", None), ("upperlimit", np.array(2)),
               ("upperlimit", [2]), ("upperlimit", np.float16(2)))
    for name, task in task_pairs:
        path = f"invalid-limits-{name}.ms"
        copytree("source.ms", path)
        original, sidecars = snapshot(path), auxiliary_content(path)
        for key, value in invalid:
            try:
                task(vis=path, mode="elevation", **{key: value})
            except AssertionError as error:
                reason = "null value not allowed" if value is None else "must be of cFloat type"
                assert str(error) == str({key: [reason]})
            else:
                raise AssertionError((name, key, value))
            assert_preserved(path, original, sidecars)


def check_commands(task_pairs):
    written = []
    for name, task in task_pairs:
        path = f"command-{name}.ms"
        outfile = Path(f"command-{name}.txt")
        copytree("source.ms", path)
        original, sidecars = snapshot(path), auxiliary_content(path)
        limits = {"lowerlimit": np.float32(5), "upperlimit": np.int64(80)}
        options = dict(vis=path, mode="elevation", field="FLUX", savepars=True, **limits)
        assert task(action="none", outfile=str(outfile), **options) == {}
        assert_preserved(path, original, sidecars)
        assert task(flagbackup=False, **options) == {}
        with table(f"{path}/FLAG_CMD", ack=False) as command_table:
            assert command_table.nrows() == 1 and command_table.getcell("APPLIED", 0)
            command = command_table.getcell("COMMAND", 0)
        assert "lowerlimit=5.0" in command and "upperlimit=80.0" in command
        assert "mode='elevation'" in command and "field='FLUX'" in command
        written.append((outfile.read_text(), command))
    if len(written) == 2:
        assert written[0] == written[1]


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    tasks_to_check = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-elevation-") as directory, chdir(directory):
        make_fixture("source.ms")
        count = check_masks(tasks_to_check)
        check_limit_inputs(tasks_to_check)
        check_commands(tasks_to_check)
    print(f"{count} geometric elevation products, five type refusals and saved commands verified")


if __name__ == "__main__":
    main()
