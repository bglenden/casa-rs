#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public CASA 6.7.6.14 and native VLA archive ingest on AT166."""

import argparse
import ast
from contextlib import contextmanager, redirect_stderr
import hashlib
from io import StringIO
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import struct
import subprocess
from tempfile import TemporaryDirectory
import os

import casatasks
from casatools import ms as casa_ms
from casatools import table as casa_table
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


@contextmanager
def table(path, readonly=True):
    tab = casa_table()
    assert tab.open(str(path), nomodify=readonly), path
    try:
        yield tab
    finally:
        tab.close()


def paired_columns(reference, native, columns, label):
    with table(str(reference)) as left, table(str(native)) as right:
        assert left.nrows() == right.nrows(), (label, left.nrows(), right.nrows())
        for name in columns:
            if not left.nrows():
                continue
            description = left.getcoldesc(name)
            # CASA bulk reads require identically shaped cells. Compare
            # variable columns across identical shape runs of at most 256.
            if description.get("ndim", 0) != 0:
                shapes = left.getcolshapestring(name)
                np.testing.assert_array_equal(right.getcolshapestring(name), shapes)
                if len(set(shapes)) > 1:
                    start = 0
                    while start < left.nrows():
                        end = start + 1
                        while end < min(left.nrows(), start + 256) and shapes[end] == shapes[start]:
                            end += 1
                        compare_values(name, left.getcol(name, start, end - start),
                                       right.getcol(name, start, end - start), label)
                        start = end
                    continue
            compare_values(name, left.getcol(name), right.getcol(name), label)


def compare_values(name, expected, actual, label):
    expected, actual = map(np.asarray, (expected, actual))
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
    with table(str(ms / "HISTORY")) as tab:
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


def paired(root, archives, label, reference_options=None, outformat=None, **options):
    expected, actual = (root / f"{label}-{side}.ms" for side in ("casa", "native"))
    files = [str(path) for path in archives]
    assert casatasks.importvla(archivefiles=files, vis=str(expected),
                              **(reference_options or options)) is None
    explicit = {"outformat": outformat} if outformat else {}
    assert tasks.importvla(archivefiles=files, vis=str(actual), **options, **explicit) is None
    paired_columns(expected, actual, MAIN, label)
    for name, columns in SUBTABLES.items():
        paired_columns(expected / name, actual / name, columns, f"{label} {name}")
    with table(str(expected)) as left, table(str(actual)) as right:
        np.testing.assert_equal(right.info(), left.info(), err_msg=f"{label} table info")
        np.testing.assert_array_equal(right.getcolkeyword("SCAN_NUMBER", "LAST_SCAN"),
                                      left.getcolkeyword("SCAN_NUMBER", "LAST_SCAN"))
        for name in ("TIME", "UVW", "DATA"):
            for key in ("MEASINFO", "QuantumUnits"):
                a, b = (ms.getcolkeywords(name).get(key) for ms in (left, right))
                np.testing.assert_array_equal(a, b, err_msg=f"{label} {name} {key}")
        expected_categories = ("ONLINE_1", "ONLINE_2", "ONLINE_4", "ONLINE_8", "SHADOW", "FLAG_CMD")
        for tab in (left, right):
            np.testing.assert_array_equal(
                tab.getcolkeyword("FLAG_CATEGORY", "CATEGORY"), expected_categories
            )
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
        with table(str(actual), readonly=False) as tab:
            tab.putcell("FLAG", 0, ~tab.getcell("FLAG", 0))
        casatasks.flagmanager(vis=str(actual), mode="restore", versionname="Original")
        paired_columns(actual, snapshot, ["FLAG", "FLAG_ROW"], "CASA restored native Original")
    if label == "evla":
        assert count > 7722
    print(f"passed {label} ({count} rows)", flush=True)
    return expected


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


def mixed_shapes(root, continuum, spectral):
    # Reuse the public sample's raw visibilities and flags. Set the
    # continuum test record's direction tag to J2000 so both archive
    # inputs can fill one synthetic product. The original archive is
    # read only. Neither dataset's sky position is used in a science test.
    scratch = bytearray(continuum.read_bytes())
    sector = 2048
    record = 13 * sector
    start = 0
    while start < len(scratch):
        number, blocks, words = struct.unpack_from(">HHi", scratch, start)
        if number != 1 or not 0 < blocks <= 39 or words <= 0:
            start += sector
            continue
        # Physical header, RCA's SDA word offset, then SDA direction epoch.
        offset = 2 * struct.unpack_from(">I", scratch, start + 4 + 2 * 12)[0]
        epoch = start + 4 + offset + 2 * 161
        assert struct.unpack_from(">h", scratch, epoch) == (1950,)
        struct.pack_into(">h", scratch, epoch, 2000)
        tail = 2 * words - (blocks - 1) * (record - 4)
        start += (blocks - 1) * record + (tail + sector - 1) // sector * sector

    amended = root / "test-continuum"
    amended.write_bytes(scratch)
    expected = paired(root, [amended, spectral], "variable-shapes", outformat="msv2")
    with table(expected) as tab:
        assert set(tab.getcolshapestring("DATA")) == {"[4, 1]", "[1, 127]"}
        assert set(tab.getcolshapestring("FLAG_CATEGORY")) == {"[4, 1, 6]", "[1, 127, 6]"}


def format_cases(root, continuum, spectral, xradio_python):
    expected = paired(root, [spectral], "explicit-v2", outformat="msv2")
    output = root / "explicit-v4.ms"
    errors = StringIO()
    with redirect_stderr(errors):
        assert tasks.importvla(archivefiles=[spectral], vis=output, outformat="msv4") is None
    assert (output / "zarr.json").is_file()
    assert not (output / "table.dat").exists() and not Path(f"{output}.flagversions").exists()
    for loss in ("FLAG_CATEGORY is not carried", "FLAG_ROW is folded into FLAG",
                 "SIGMA is not carried", "Original flag-version snapshot is not carried"):
        assert loss in errors.getvalue(), errors.getvalue()
    compare = Path(__file__).resolve().parents[1] / "ms-trial" / "check_importvla.py"
    # The pinned XRADIO interpreter uses its own dependencies and never
    # loads the CASA/PyO3 modules installed for this interpreter.
    child_env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    subprocess.run([str(xradio_python), str(compare), str(expected), str(output)],
                   env=child_env, check=True)
    original = (output / "zarr.json").read_bytes()
    try:
        tasks.importvla(archivefiles=[spectral], vis=output, outformat="msv4")
    except RuntimeError as error:
        assert "already exists" in str(error), str(error)
    else:
        raise AssertionError("the second import replaced an existing MSv4")
    assert (output / "zarr.json").read_bytes() == original

    refused = root / "b1950.ps.zarr"
    try:
        tasks.importvla(archivefiles=[continuum], vis=refused, outformat="msv4", bandname="C")
    except tasks.TaskParameterError as error:
        assert error.completion.parameter == "outformat", str(error)
        assert "UVW frame" in str(error), str(error)
    else:
        raise AssertionError("this B1950_VLA archive unexpectedly published an MSv4")
    assert not refused.exists() and not Path(f"{refused}.flagversions").exists()
    assert not list(root.glob(".*.staging-*"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path, help="public casatestdata AT166_1 VLA archive")
    parser.add_argument("cases", nargs="*")
    parser.add_argument("--spectral", type=Path, help="public AS758_C030425.xp1 VLA archive")
    parser.add_argument("--xradio-python", type=Path, help="pinned XRADIO oracle interpreter")
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
    if selected("variable-shapes") and args.spectral:
        with TemporaryDirectory(prefix="importvla-") as temporary:
            mixed_shapes(Path(temporary), archive, args.spectral.resolve(strict=True))
    if selected("formats") and args.spectral:
        if args.xradio_python is None:
            parser.error("--spectral requires --xradio-python")
        with TemporaryDirectory(prefix="importvla-") as temporary:
            format_cases(Path(temporary), archive, args.spectral.resolve(strict=True),
                         args.xradio_python.absolute())
    if selected("failures"):
        with TemporaryDirectory(prefix="importvla-") as temporary:
            failures(Path(temporary), archive)


if __name__ == "__main__":
    main()
