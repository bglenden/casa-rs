#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare native antenna shadow flagging with pinned CASA and exact baseline masks."""

from contextlib import chdir
from json import loads
from itertools import groupby
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casatools import measures
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_summary_oracle import snapshot
from record_casa_returns import make_cal_ms


def make_shadow_fixture():
    make_cal_ms("source.ms", channels=8, scan_seconds=40, telescope="VLA", frequency="1.4GHz")
    with table("source.ms", readonly=False, ack=False) as main:
        fields = main.getcol("FIELD_ID")
        times = main.getcol("TIME_CENTROID")
        first = main.getcol("ANTENNA1")
        second = main.getcol("ANTENNA2")
        uniq = np.unique(times)
        index = np.searchsorted(uniq, times)
        assert len(uniq) == 8
        assert np.all(np.bincount(index) == 10)
        uvw = np.tile(np.array([200., 0., 0.]), (len(times), 1))
        primary = (first == 0) & (second == 1)
        uvw[primary, :2] = [12., 16.]
        uvw[primary, 2] = np.array([11., -11., 0., 11., -11., 0., 11., -11.])[index[primary]]
        secondary = (first == 3) & (second == 4)
        uvw[secondary, :] = [15., 20., -10.]
        raw = np.zeros_like(main.getcol("FLAG"))
        main.putcol("UVW", uvw)
        main.putcol("FLAG", raw)
        main.putcol("FLAG_ROW", np.zeros(len(times), dtype=bool))
    with table("source.ms/ANTENNA", ack=False) as antennas:
        np.testing.assert_array_equal(antennas.getcol("DISH_DIAMETER"), np.repeat(25., 5))
    return raw, fields, index, first, second


def check_shadow(libraries):
    raw, fields, index, first, second = make_shadow_fixture()
    shadow_primary = np.array([1, 0, 1, 1, 0, 1, 1, 0])[index]
    primary = (first == shadow_primary) | (second == shadow_primary)
    secondary = (first == 3) | (second == 3)
    cases = (
        ({}, primary, slice(None), slice(None)),
        ({"tolerance": 5.}, np.zeros_like(primary), slice(None), slice(None)),
        ({"tolerance": -1.}, primary | secondary, slice(None), slice(None)),
        ({"tolerance": np.float32(4), "field": "FLUX"}, primary & (fields == 0), slice(None), slice(None)),
        ({"spw": "0"}, primary, slice(None), slice(None)),
        ({"spw": "0:2~4"}, primary, slice(2, 5), slice(None)),
        ({"correlation": "YY"}, primary, slice(None), 1),
        ({"spw": "0:2~4", "correlation": "YY"}, primary, slice(2, 5), 1),
        ({"correlation": "ABS_YY"}, primary, slice(None), 1),
        ({"correlation": "ABS_UNKNOWN"}, np.zeros_like(primary), slice(None), slice(None)),
        ({"tolerance": float("inf")}, np.zeros_like(primary), slice(None), slice(None)),
        ({"tolerance": float("nan")}, np.zeros_like(primary), slice(None), slice(None)),
    )
    for case, (settings, affected, channels, planes) in enumerate(cases):
        wanted = raw.copy()
        wanted[affected, channels, planes] = True
        for name, task in libraries:
            path = f"shadow-{case}-{name}.ms"
            copytree("source.ms", path)
            original, auxiliary = snapshot(path), auxiliary_content(path)
            options = dict(vis=path, mode="shadow", **settings)
            assert task(action="calculate", **options) == {}
            assert_preserved(path, original, auxiliary)
            if name == "native":
                result = tasks.run("flagdata", action="calculate", **options)
                assert loads(result.stdout)["result"]["changed_samples"] == wanted.sum()
            assert task(**options) == {}
            samples, rows = flags(path)
            expected = wanted
            if name == "casa" and settings.get("correlation", "").startswith("ABS_"):
                # CASA widens decorated expressions to all stored planes.
                expected = np.broadcast_to(primary[:, None, None], raw.shape)
            expected_rows = expected.all(axis=(1, 2))
            if "spw" in settings:
                expected_rows = np.zeros_like(expected_rows)
            np.testing.assert_array_equal(samples, expected, err_msg=repr(options))
            np.testing.assert_array_equal(rows, expected_rows, err_msg=repr(options))
            backup, backup_rows = flags(str(Path(path + ".flagversions", "flags.flagdata_1")))
            np.testing.assert_array_equal(backup, raw)
            assert not backup_rows.any()
            for before, after in zip(original, snapshot(path), strict=True):
                np.testing.assert_array_equal(before[2], after[2])
    return len(cases)


def verify_projected_dish(path, origin, extra):
    converter = measures()
    assert converter.doframe(converter.position("ITRF", *(f"{x:.17g}m" for x in origin)))
    with table(path, ack=False) as main, table(path + "/FIELD", ack=False) as fields:
        for field in range(2):
            ra, dec = fields.getcell("PHASE_DIR", field)[0]
            assert converter.doframe(converter.direction("J2000", f"{ra}rad", f"{dec}rad"))
            times = main.getcol("TIME_CENTROID")[main.getcol("FIELD_ID") == field]
            baseline = converter.baseline("ITRF", *(f"{x:.17g}m" for x in origin - extra))
            for seconds in np.unique(times):
                assert converter.doframe(converter.epoch("UT1", f"{seconds:.17g}s"))
                u, v, w = converter.touvw(baseline)[1]["value"]
                assert np.hypot(u, v) < 12. and w < -4.


def check_projected_pairs(libraries):
    copytree("source.ms", "far.ms")
    with table("far.ms", readonly=False, ack=False) as main:
        first, second = main.getcol("ANTENNA1"), main.getcol("ANTENNA2")
        main.putcol("UVW", np.repeat(np.array([[200., 0., 0.]]), len(first), axis=0))
    with table("far.ms/ANTENNA", readonly=False, ack=False) as antennas:
        origin = antennas.getcell("POSITION", 0)
        locations = origin + np.linspace(0., 4000., 5)[:, None] * np.array([[1., 0., 0.]])
        antennas.putcol("POSITION", locations)
    x, y, z = map(float, origin)
    near = int(z) - 10
    extra = np.array([x, y, float(near)])
    verify_projected_dish("far.ms", origin, extra)

    entry = dict(name="ea99", diameter=25., position=list(map(float, extra)))
    Path("additional.txt").write_text(f"name=ea99\ndiameter=25.0\nposition=[{x}, {y}, {near}]\n")
    Path("additional-padded.txt").write_text(f"name=ea99\ndiameter=25.0\nposition=[{x}, {y}, {near} ]\n")
    affected = (first == 0) | (second == 0)
    clear = np.zeros_like(affected)
    cases = (
        (False, {"addantenna": {"0": entry}}, affected),
        (False, {"addantenna": "additional-padded.txt"}, affected),
        (False, {"addantenna": "additional.txt"}, affected),
        (True, {}, clear),
        (True, {"antenna": "0&2"}, (first == 0) & (second == 2)),
        (True, {"antenna": "1&2"}, clear),
    )
    for index, (ant1_close, options, selected) in enumerate(cases):
        for name, task in libraries:
            path = f"projected-{index}-{name}.ms"
            copytree("far.ms", path)
            if ant1_close:
                with table(path + "/ANTENNA", readonly=False, ack=False) as antennas:
                    antennas.putcell("POSITION", 1, extra)
            assert task(vis=path, mode="shadow", flagbackup=False, **options) == {}
            # Pinned CASA strips the last digit of an unpadded Z coordinate.
            # The padded file and record give the reference mask.
            expected = clear if name == "casa" and options.get("addantenna") == "additional.txt" else selected
            samples, rows = flags(path)
            np.testing.assert_array_equal(samples, np.broadcast_to(expected[:, None, None], samples.shape))
            np.testing.assert_array_equal(rows, expected)
    return len(cases)


def check_inputs(libraries):
    failures = (
        ("tolerance", None, AssertionError, "{'tolerance': ['null value not allowed']}", False),
        ("tolerance", "1.0", AssertionError, "{'tolerance': ['must be of cFloat type']}", False),
        ("tolerance", np.array(1.), AssertionError, "{'tolerance': ['must be of cFloat type']}", False),
        ("addantenna", 3, AssertionError, "{'addantenna': ['must be of cStr type', 'must be of cDict type']}", False),
        ("addantenna", None, AssertionError,
         "{'addantenna': ['null value not allowed', 'null value not allowed', 'null value not allowed']}", False),
        ("addantenna", "missing.txt", Exception, "File missing.txt not found - please verify the name", True),
        ("addantenna", "wrong-number.txt", ValueError, "could not convert string to float: 'oops'", True),
        ("addantenna", "empty-name.txt", IndexError, "list index out of range", True),
    )
    Path("wrong-number.txt").write_text("name=ea99\ndiameter=oops\nposition=[1.0, 2.0, 3.0 ]\n")
    Path("empty-name.txt").write_text("name=\ndiameter=25.0\nposition=[1.0, 2.0, 3.0 ]\n")
    for index, (key, value, error_type, message, creates_backup) in enumerate(failures):
        for name, task in libraries:
            path = f"input-{index}-{name}.ms"
            copytree("source.ms", path)
            before, auxiliaries = snapshot(path), auxiliary_content(path)
            try:
                task(vis=path, mode="shadow", **{key: value})
            except Exception as error:
                assert type(error) is error_type and str(error) == message, (key, value, error)
            else:
                raise AssertionError((name, key, value))
            if not creates_backup:
                assert_preserved(path, before, auxiliaries)
            else:
                backup = Path(path + ".flagversions", "flags.flagdata_1")
                assert backup.is_dir()
                for original, after in zip(before, snapshot(path), strict=True):
                    for cell, readback in zip(original, after, strict=True):
                        np.testing.assert_array_equal(readback, cell)
    return len(failures)


def check_provider_refusals():
    path = "raw-invalid.ms"
    copytree("source.ms", path)
    original, auxiliaries = snapshot(path), auxiliary_content(path)
    for parameter, value in (("addantenna", 3), ("tolerance", "2")):
        try:
            tasks.run("flagdata", vis=path, mode="shadow", **{parameter: value})
        except tasks.TaskParameterError as error:
            assert error.completion.parameter == parameter
        else:
            raise AssertionError((parameter, value))
        assert_preserved(path, original, auxiliaries)


def check_shuffled_rows(libraries):
    copytree("source.ms", "before-shuffle.ms")
    with table("before-shuffle.ms/ANTENNA", ack=False, readonly=False) as antennas:
        origin = antennas.getcell("POSITION", 0)
        antennas.putcol("POSITION", origin + np.arange(5)[:, None] * np.array([[10000., 0., 0.]]))
    with table("before-shuffle.ms", ack=False, readonly=False) as main:
        first, second = main.getcol("ANTENNA1"), main.getcol("ANTENNA2")
        times = main.getcol("TIME")
        main.putcol("TIME_CENTROID", times + 0.2 * ((first + second) % 3))
        with main.selectrows(np.random.default_rng(42).permutation(len(times))) as shuffled:
            with shuffled.copy("shuffled.ms", deep=True, valuecopy=True):
                pass
    with table("shuffled.ms", ack=False) as main:
        time, centroid = main.getcol("TIME"), main.getcol("TIME_CENTROID")
        first, second, uvw = main.getcol("ANTENNA1"), main.getcol("ANTENNA2"), main.getcol("UVW")
    affected = np.zeros(len(time), dtype=bool)
    for _, integration in groupby(np.argsort(time, kind="stable"), key=time.__getitem__):
        rows = list(integration)
        previous, shadowed = None, set()
        for slot, row in enumerate(rows):
            if centroid[row] != previous:
                previous = centroid[row]
                shadowed = set()
                for current in rows[slot:]:
                    if centroid[current] > previous:
                        break
                    u, v, w = uvw[current]
                    if np.hypot(u, v) < 25.:
                        shadowed.add(first[current] if w < 0. else second[current])
            affected[row] = first[row] in shadowed or second[row] in shadowed
    assert affected.sum() == 11
    for name, task in libraries:
        path = f"shuffled-{name}.ms"
        copytree("shuffled.ms", path)
        assert task(vis=path, mode="shadow", flagbackup=False) == {}
        samples, row_flags = flags(path)
        np.testing.assert_array_equal(samples, np.broadcast_to(affected[:, None, None], samples.shape))
        np.testing.assert_array_equal(row_flags, affected)


def check_commands(libraries):
    written = []
    for name, task in libraries:
        path = f"command-{name}.ms"
        out = Path(f"command-{name}.txt")
        copytree("far.ms", path)
        original, auxiliaries = snapshot(path), auxiliary_content(path)
        options = dict(vis=path, mode="shadow", addantenna="additional-padded.txt",
                       tolerance=np.float32(1.2), savepars=True)
        assert task(action="none", outfile=str(out), **options) == {}
        assert_preserved(path, original, auxiliaries)
        assert task(flagbackup=False, **options) == {}
        with table(f"{path}/FLAG_CMD", ack=False) as commands:
            assert commands.nrows() == 1 and commands.getcell("APPLIED", 0)
            command = commands.getcell("COMMAND", 0)
        assert "tolerance=1.2000000476837158" in command
        assert "mode='shadow'" in command and "addantenna={'0': {'name': 'ea99'" in command
        with table(f"{path}/HISTORY", ack=False) as history:
            messages = [line.split("=", 1)[1].strip() for line in history.getcol("MESSAGE")[-78:]
                        if line.startswith("addantenna ")]
        assert messages == ['"additional-padded.txt"']
        written.append((out.read_text(), command))
    if len(written) == 2:
        assert written[0] == written[1]


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-shadow-") as directory, chdir(directory):
        stored = check_shadow(libraries)
        projected = check_projected_pairs(libraries)
        failures = check_inputs(libraries)
        check_provider_refusals()
        check_shuffled_rows(libraries)
        check_commands(libraries)
    print(f"{stored} stored-UVW products, {projected} projected products, {failures} public refusals, "
          "two provider refusals, shuffled integration rows and saved shadow commands verified")


if __name__ == "__main__":
    main()
