#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public CASA 6.7.6.14 and native VLA archive ingest on AT166."""

import argparse
import ast
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import subprocess
from tempfile import TemporaryDirectory

import casatasks
from casatools import ms as casa_ms
from casacore.tables import table
import numpy as np

from casars import tasks


MAIN = (
    "TIME", "TIME_CENTROID", "EXPOSURE", "INTERVAL", "ANTENNA1", "ANTENNA2", "SCAN_NUMBER",
    "ARRAY_ID", "FIELD_ID", "DATA_DESC_ID", "OBSERVATION_ID", "FEED1", "FEED2", "STATE_ID",
    "UVW", "DATA", "CORRECTED_DATA", "MODEL_DATA", "FLAG", "FLAG_ROW", "FLAG_CATEGORY",
    "SIGMA", "WEIGHT",
)
SUBTABLES = {
    "ANTENNA": ("NAME", "STATION", "POSITION", "DISH_DIAMETER", "FLAG_ROW"),
    "DATA_DESCRIPTION": ("SPECTRAL_WINDOW_ID", "POLARIZATION_ID", "FLAG_ROW"),
    "FIELD": ("NAME", "CODE", "SOURCE_ID", "PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"),
    "FEED": ("ANTENNA_ID", "FEED_ID", "NUM_RECEPTORS", "POLARIZATION_TYPE", "RECEPTOR_ANGLE"),
    "OBSERVATION": ("PROJECT", "TELESCOPE_NAME", "TIME_RANGE", "RELEASE_DATE",
                    "FLAG_ROW", "LOG", "OBSERVER", "SCHEDULE", "SCHEDULE_TYPE"),
    "POLARIZATION": ("NUM_CORR", "CORR_TYPE", "CORR_PRODUCT", "FLAG_ROW"),
    "SOURCE": ("SOURCE_ID", "SPECTRAL_WINDOW_ID", "NAME", "DIRECTION", "NUM_LINES"),
    "SPECTRAL_WINDOW": ("NAME", "NUM_CHAN", "REF_FREQUENCY", "CHAN_FREQ", "CHAN_WIDTH",
                        "EFFECTIVE_BW", "RESOLUTION", "TOTAL_BANDWIDTH", "MEAS_FREQ_REF"),
}


def mjd_seconds(seconds):
    return (datetime(1858, 11, 17) + timedelta(seconds=seconds)).strftime("%Y/%m/%d/%H:%M:%S")


def paired_columns(reference, native, columns, label):
    with table(str(reference), ack=False) as left, table(str(native), ack=False) as right:
        assert left.nrows() == right.nrows(), (label, left.nrows(), right.nrows())
        for name in columns:
            if not left.nrows():
                continue
            expected, actual = map(np.asarray, (left.getcol(name), right.getcol(name)))
            if expected.dtype.kind in "fc":
                limits = (0, 1e-5) if name in ("TIME", "TIME_CENTROID", "TIME_RANGE", "RELEASE_DATE") else (1e-6, 1e-5)
                if name in ("REF_FREQUENCY", "CHAN_FREQ", "CHAN_WIDTH", "EFFECTIVE_BW",
                            "RESOLUTION", "TOTAL_BANDWIDTH"):
                    limits = (0, 1e-2)
                np.testing.assert_allclose(actual, expected, rtol=limits[0], atol=limits[1],
                                           err_msg=f"{label} {name}")
            else:
                np.testing.assert_array_equal(actual, expected, err_msg=f"{label} {name}")


def history(ms):
    with table(str(ms / "HISTORY"), ack=False) as tab:
        rows = {column: tab.getcol(column) for column in (
            "APPLICATION", "ORIGIN", "PRIORITY", "CLI_COMMAND", "APP_PARAMS", "MESSAGE",
        )}
    messages = rows.pop("MESSAGE")
    assert messages[0] == "taskname=importvla" and messages[1].startswith("version:")
    args = {}
    for row in messages[2:]:
        name, value = row.split("=", 1)
        args[name.strip()] = ast.literal_eval(value.strip())
    assert args.pop("vis") == str(ms)
    return rows, args


def flag_version(ms):
    root = Path(f"{ms}.flagversions")
    assert (root / "FLAG_VERSION_LIST").read_text().splitlines() == [
        "Original : Original flags at import into CASA",
    ]
    paired_columns(ms, root / "flags.Original", ["FLAG", "FLAG_ROW"], "Original flag snapshot")
    return root / "flags.Original"


def paired(root, archives, label, reference_options=None, **options):
    expected, actual = (root / f"{label}-{side}.ms" for side in ("casa", "native"))
    files = [str(path) for path in archives]
    assert casatasks.importvla(archivefiles=files, vis=str(expected),
                              **(reference_options or options)) is None
    assert tasks.importvla(archivefiles=files, vis=str(actual), **options) is None
    paired_columns(expected, actual, MAIN, label)
    for name, columns in SUBTABLES.items():
        paired_columns(expected / name, actual / name, columns, f"{label} {name}")
    with table(str(expected), ack=False) as left, table(str(actual), ack=False) as right:
        np.testing.assert_equal(right.info(), left.info(), err_msg=f"{label} table info")
        np.testing.assert_array_equal(right.getcolkeyword("SCAN_NUMBER", "LAST_SCAN"),
                                      left.getcolkeyword("SCAN_NUMBER", "LAST_SCAN"))
        for name in ("TIME", "UVW", "DATA"):
            for key in ("MEASINFO", "QuantumUnits"):
                a, b = (ms.getcolkeywords(name).get(key) for ms in (left, right))
                np.testing.assert_array_equal(a, b, err_msg=f"{label} {name} {key}")
        count = right.nrows()
        first_antenna = left.getcell("ANTENNA1", 0)
    casa, native = (history(ms) for ms in (expected, actual))
    if reference_options is not None:
        assert casa[1].pop("frequencytol") == reference_options["frequencytol"]
        assert native[1].pop("frequencytol") == options["frequencytol"]
    assert casa[1] == native[1], (label, casa[1], native[1])
    for key in casa[0]:
        np.testing.assert_array_equal(casa[0][key], native[0][key], err_msg=f"{label} HISTORY {key}")
    paired_columns(flag_version(expected), flag_version(actual), ["FLAG", "FLAG_ROW"], f"{label} CASA version")
    if label == "band-c":
        assert count == 7722
        output = subprocess.check_output(["taql", f"select ANTENNA1 from [{actual}] limit 1"], text=True)
        assert int(output.splitlines()[-1]) == first_antenna
        reader = casa_ms()
        try:
            assert reader.open(str(actual))
            assert reader.nrow() == count
        finally:
            reader.done()
        snapshot = flag_version(actual)
        with table(str(actual), readonly=False, ack=False) as tab:
            tab.putcell("FLAG", 0, ~tab.getcell("FLAG", 0))
        casatasks.flagmanager(vis=str(actual), mode="restore", versionname="Original")
        paired_columns(actual, snapshot, ["FLAG", "FLAG_ROW"], "CASA restored native Original")
    if label == "evla":
        assert count > 7722
    print(f"passed {label} ({count} rows)", flush=True)


def failures(root, archive):
    def refuse(engine, vis, needle, error_type=Exception, **options):
        try:
            engine.importvla(archivefiles=[str(archive)], vis=str(vis), **options)
        except error_type as error:
            assert needle in str(error), str(error)
        else:
            raise AssertionError(f"import accepted {options}")

    expected, actual = (root / f"bad-{side}.ms" for side in ("casa", "native"))
    for engine in (casatasks, tasks):
        try:
            engine.importvla(archivefiles=[str(archive)])
        except RuntimeError as error:
            assert "Need valid visibility file name" in str(error)
        else:
            raise AssertionError("import accepted a missing output name")
    refuse(casatasks, expected, "visibility file is empty", bandname="L")
    assert expected.is_dir()
    refuse(tasks, actual, "visibility file is empty", bandname="L")
    assert not actual.exists() and not Path(f"{actual}.flagversions").exists()
    refuse(casatasks, root / "future-casa.ms", "visibility file is empty", starttime="50000")
    refuse(tasks, root / "future-native.ms", "visibility file is empty", starttime="50000")
    refuse(casatasks, root / "project-casa.ms", "VLAProjectFilter", RuntimeError, project="TOOLONG")
    refuse(tasks, root / "project-native.ms", "project name", RuntimeError, project="TOOLONG")
    missing = root / "missing.arc"
    for engine, output in ((casatasks, root / "missing-casa.ms"), (tasks, actual)):
        try:
            engine.importvla(archivefiles=[str(archive), str(missing)], vis=str(output), bandname="C")
        except AssertionError as error:
            assert str(missing) in str(error)
        else:
            raise AssertionError("import accepted a missing second archive")
        assert not output.exists()
    unreadable = root / "directory.arc"
    unreadable.mkdir()
    reference = root / "incomplete.ms"
    try:
        casatasks.importvla(archivefiles=[str(archive), str(unreadable)], vis=str(reference), bandname="C")
    except RuntimeError as error:
        assert "cannot read from" in str(error)
    else:
        raise AssertionError("CASA accepted a directory as the second archive")
    assert reference.is_dir() and not Path(f"{reference}.flagversions").exists()
    try:
        tasks.importvla(archivefiles=[str(archive), str(unreadable)], vis=str(actual), bandname="C")
    except RuntimeError as error:
        assert str(unreadable) in str(error)
    else:
        raise AssertionError("import accepted a directory as the second archive")
    assert not actual.exists() and not Path(f"{actual}.flagversions").exists()
    assert not list(root.glob(".importvla-*"))
    assert tasks.importvla(archivefiles=[str(archive)], vis=str(actual), bandname="C") is None
    digest = lambda: hashlib.sha256((actual / "table.dat").read_bytes()).digest()
    before = digest()
    refuse(tasks, actual, "already exists", bandname="C")
    assert digest() == before
    flag_version(actual)
    orphan = root / "protected.ms"
    orphan_flags = Path(f"{orphan}.flagversions")
    orphan_flags.mkdir()
    (orphan_flags / "marker").write_text("preexisting catalog")
    refuse(tasks, orphan, "already exists", bandname="C")
    assert not orphan.exists()
    assert (orphan_flags / "marker").read_text() == "preexisting catalog"
    print("passed rejected names and project, empty selection, missing and unreadable archives, same-path retry and protected output", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path, help="public casatestdata AT166_1 VLA archive")
    parser.add_argument("cases", nargs="*")
    args = parser.parse_args()
    archive = args.archive.resolve(strict=True)
    assert casatasks.version() == [6, 7, 6, 14]
    selected = lambda name: not args.cases or name in args.cases
    for label, options in (
        ("default", {}),
        ("band-c", dict(bandname="C")),
        ("time", dict(starttime=mjd_seconds(4281838800), stoptime=mjd_seconds(4281838930))),
        ("boundary", dict(starttime=mjd_seconds(4281838905), stoptime=mjd_seconds(4281839005))),
        ("malformed-time", dict(starttime="junk", stoptime=mjd_seconds(4281838800))),
        ("seconds-stop", dict(stoptime="4281838800s")),
        ("old-band", dict(bandname="X")),
        ("evla", dict(bandname="C", evlabands=True)),
        ("project", dict(project="at166", bandname="C")),
        ("all-projects", dict(project="all", bandname="C")),
        ("uncalibrated", dict(applytsys=False, bandname="C")),
        ("antennas-autocorr", dict(antnamescheme="old", autocorr=True, bandname="C", keepblanks=True)),
        ("frequency-tolerance", dict(frequencytol="500MHz", bandname="C")),
        ("wide-frequency-tolerance", dict(frequencytol=3e9)),
        ("zero-frequency-tolerance", dict(frequencytol=0.0)),
        ("frequency-quantity", dict(frequencytol="3000MHz", reference_options={"frequencytol": 3e9})),
    ):
        if selected(label):
            with TemporaryDirectory(prefix="importvla-") as temporary:
                paired(Path(temporary), [archive], label, **options)
    if selected("multiple"):
        with TemporaryDirectory(prefix="importvla-") as temporary:
            root = Path(temporary)
            second = root / "second-archive"
            shutil.copyfile(archive, second)
            paired(root, [archive, second], "multiple", bandname="C")
    if selected("failures"):
        with TemporaryDirectory(prefix="importvla-") as temporary:
            failures(Path(temporary), archive)


if __name__ == "__main__":
    main()
