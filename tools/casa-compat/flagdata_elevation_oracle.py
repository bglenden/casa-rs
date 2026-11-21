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
from casaconfig import config
from casatools import measures, ms, msmetadata
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


def expected_elevation(path, phase_at=None):
    with table(f"{path}/ANTENNA", ack=False) as antennas:
        positions = antennas.getcol("POSITION")
    with table(f"{path}/FIELD", ack=False) as fields:
        directions = [fields.getcell("PHASE_DIR", row)[0] for row in range(fields.nrows())]
        reference = fields.getcolkeyword("PHASE_DIR", "MEASINFO")["Ref"]
    with table(path, ack=False) as ms:
        ant1, ant2, ids, times = [ms.getcol(name) for name in
                                 ("ANTENNA1", "ANTENNA2", "FIELD_ID", "TIME")]
        time_reference = ms.getcolkeyword("TIME", "MEASINFO")["Ref"]
    coordinates = measures()
    elevations = {}
    for field, seconds in sorted(set(zip(ids, times, strict=True))):
        coordinates.doframe(coordinates.epoch(time_reference, f"{seconds:.17g}s"))
        direction = (phase_at(field, seconds) if phase_at else
                     coordinates.direction(reference, *(f"{angle:.17g}rad" for angle in directions[field])))
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
        ({"correlation": "ABS_ALL"}, 0, 90, np.ones_like(fields, dtype=bool), slice(None), slice(None)),
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


def check_row_flags(task_pairs):
    with table("source.ms", ack=False) as main:
        selected = main.getcol("FIELD_ID") == 0
    for spw in ("", "0"):
        for name, task in task_pairs:
            path = f"row-flags-{bool(spw)}-{name}.ms"
            copytree("source.ms", path)
            with table(path, readonly=False, ack=False) as main:
                samples = main.getcol("FLAG")
                samples[np.flatnonzero(selected)[0]] = True
                main.putcol("FLAG", samples)
            assert task(vis=path, mode="elevation", field="FLUX", spw=spw,
                        lowerlimit=90, upperlimit=-90, flagbackup=False) == {}
            actual, rows = flags(path)
            np.testing.assert_array_equal(actual, np.broadcast_to(selected[:, None, None], samples.shape))
            np.testing.assert_array_equal(rows, selected if not spw else np.zeros_like(selected))


def check_decorated_selection():
    masks = []
    cases = (("plain", casatasks.flagdata, "YY"),
             ("legacy", casatasks.flagdata, "ABS_YY"),
             ("native", tasks.flagdata, "ABS_YY"))
    for name, task, expression in cases:
        path = f"correlation-{name}.ms"
        copytree("source.ms", path)
        assert task(vis=path, mode="elevation", field="PHASE",
                    correlation=expression, flagbackup=False) == {}
        masks.append(flags(path))
    plain, legacy, native = masks
    for expected, actual in zip(plain, native, strict=True):
        np.testing.assert_array_equal(actual, expected)
    assert plain[0][:, :, 3].any() and not plain[0][:, :, :3].any()
    np.testing.assert_array_equal(legacy[0], np.repeat(plain[0][:, :, 3:], 4, axis=2))
    np.testing.assert_array_equal(legacy[1], legacy[0].all(axis=(1, 2)))


def check_polynomial(task_pairs):
    source = "polynomial.ms"
    copytree("source.ms", source)
    with table(source, readonly=False, ack=False) as main:
        times, scans = main.getcol("TIME"), main.getcol("SCAN_NUMBER")
        main.putcol("FIELD_ID", np.zeros(main.nrows(), dtype=np.int32))
    origin = times.min() - 30
    with table(source + "/FIELD", readonly=False, ack=False) as fields:
        fields.putcell("NUM_POLY", 0, 1)
        fields.putcell("TIME", 0, origin)
        for name in ("PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"):
            fields.putcell(name, 0, np.array([[0., -np.pi / 6], [.02, 0.]]))
    converter = measures()
    cases = (({}, 45, times.min()), ({"ntime": "20s"}, 45, times.min()),
             ({}, 70, times.min()), ({"scan": str(scans.max())}, 45, times[scans == scans.max()].min()))
    for index, (options, lower, anchor) in enumerate(cases):
        ra = .02 * (anchor - origin)
        direction = converter.direction("J2000", f"{ra:.17g}rad", "-30deg")
        _, elevations = expected_elevation(source, lambda _field, _time: direction)
        outside = np.any(elevations < lower, axis=1)
        if "scan" in options:
            outside &= scans == int(options["scan"])
        for name, task in task_pairs:
            path = f"polynomial-{index}-{name}.ms"
            copytree(source, path)
            assert task(vis=path, mode="elevation", lowerlimit=lower, flagbackup=False, **options) == {}
            samples, rows = flags(path)
            np.testing.assert_array_equal(samples, np.broadcast_to(outside[:, None, None], samples.shape))
            np.testing.assert_array_equal(rows, outside)


def check_ephemeris(task_pairs):
    source = "ephemeris.ms"
    copytree("source.ms", source)
    with table(source, readonly=False, ack=False) as main:
        times = main.getcol("TIME")
        anchor = times.min()
        stretched = anchor + (times - anchor) * 172800.
        for column in ("TIME", "TIME_CENTROID"):
            main.putcol(column, stretched)
    with table(source + "/FIELD", readonly=False, ack=False) as fields:
        fields.putcell("TIME", 0, anchor)
        for column in ("PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"):
            fields.putcell(column, 0, np.zeros((1, 2)))
    ephemeris = Path(config.measurespath) / "ephemerides/JPL-Horizons/Uranus_55197-59214dUTC_J2000.tab"
    tool = ms()
    assert tool.open(source, nomodify=False)
    try:
        assert tool.addephemeris(0, str(ephemeris), "Uranus", [0])
    finally:
        tool.done()
    metadata, converter = msmetadata(), measures()
    assert metadata.open(source)
    try:
        def center(field, seconds):
            epoch = converter.epoch("UTC", f"{seconds:.17g}s")
            return metadata.phasecenter(int(field), epoch)

        ids, frozen = expected_elevation(source, lambda field, _time: center(field, anchor))
        _, tracked = expected_elevation(source, center)
    finally:
        metadata.done()
    selected = ids == 0
    at_first_time = frozen[selected].min(axis=1)
    at_each_time = tracked[selected].min(axis=1)
    different = np.argmax(abs(at_first_time - at_each_time))
    assert abs(at_first_time[different] - at_each_time[different]) > 0.02
    lower = (at_first_time[different] + at_each_time[different]) / 2
    outside = selected & np.any(frozen < lower, axis=1)
    for name, task in task_pairs:
        path = f"ephemeris-{name}.ms"
        copytree(source, path)
        assert task(vis=path, mode="elevation", field="0", lowerlimit=lower, flagbackup=False) == {}
        samples, rows = flags(path)
        np.testing.assert_array_equal(samples, np.broadcast_to(outside[:, None, None], samples.shape))
        np.testing.assert_array_equal(rows, outside)


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
        check_row_flags(tasks_to_check)
        check_decorated_selection()
        check_polynomial(tasks_to_check)
        check_ephemeris(tasks_to_check)
        check_limit_inputs(tasks_to_check)
        check_commands(tasks_to_check)
    print(f"{count} fixed-field elevation products, row flags, five moving-field products, five type refusals and saved commands verified")


if __name__ == "__main__":
    main()
