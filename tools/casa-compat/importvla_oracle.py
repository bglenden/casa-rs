#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public CASA 6.7.6.14 and native VLA archive ingest on AT166."""

import argparse
import ast
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, redirect_stderr
import hashlib
import math
from io import StringIO
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import struct
import subprocess
from tempfile import TemporaryDirectory
from time import monotonic, sleep
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
    "FIELD": ("NAME", "CODE", "TIME", "SOURCE_ID", "PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"),
    "FEED": ("ANTENNA_ID", "FEED_ID", "NUM_RECEPTORS", "POLARIZATION_TYPE", "RECEPTOR_ANGLE"),
    "OBSERVATION": ("PROJECT", "TELESCOPE_NAME", "TIME_RANGE", "RELEASE_DATE",
                    "FLAG_ROW", "LOG", "OBSERVER", "SCHEDULE", "SCHEDULE_TYPE"),
    "POLARIZATION": ("NUM_CORR", "CORR_TYPE", "CORR_PRODUCT", "FLAG_ROW"),
    "SOURCE": ("SOURCE_ID", "SPECTRAL_WINDOW_ID", "NAME", "DIRECTION", "NUM_LINES"),
    "SPECTRAL_WINDOW": ("NAME", "NUM_CHAN", "REF_FREQUENCY", "CHAN_FREQ", "CHAN_WIDTH",
                        "EFFECTIVE_BW", "RESOLUTION", "TOTAL_BANDWIDTH", "MEAS_FREQ_REF",
                        "IF_CONV_CHAIN", "FREQ_GROUP", "FREQ_GROUP_NAME"),
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


SECTOR_BYTES = 2048
RECORD_BYTES = 13 * SECTOR_BYTES
HEADER_BYTES = 4


def logical_record_starts(contents):
    start = 0
    while start < len(contents):
        number, blocks, words = struct.unpack_from(">HHi", contents, start)
        if number != 1 or not 0 < blocks <= 39 or words <= 0:
            start += SECTOR_BYTES
            continue
        tail = 2 * words - (blocks - 1) * (RECORD_BYTES - HEADER_BYTES)
        end = start + (blocks - 1) * RECORD_BYTES + (
            (tail + SECTOR_BYTES - 1) // SECTOR_BYTES * SECTOR_BYTES
        )
        yield start + HEADER_BYTES, end
        start = end


def mixed_shapes(root, continuum, spectral):
    # Reuse the public sample's raw visibilities and flags. Set the
    # continuum test record's direction tag to J2000 so both archive
    # inputs can fill one synthetic product. The original archive is
    # read only. Neither dataset's sky position is used in a science test.
    scratch = bytearray(continuum.read_bytes())
    for rca, _ in logical_record_starts(scratch):
        offset = 2 * struct.unpack_from(">I", scratch, rca + 2 * 12)[0]
        epoch = rca + offset + 2 * 161
        assert struct.unpack_from(">h", scratch, epoch) == (1950,)
        struct.pack_into(">h", scratch, epoch, 2000)

    amended = root / "test-continuum"
    amended.write_bytes(scratch)
    expected = paired(root, [amended, spectral], "variable-shapes", outformat="msv2")
    with table(expected) as tab:
        assert set(tab.getcolshapestring("DATA")) == {"[4, 1]", "[1, 127]"}
        assert set(tab.getcolshapestring("FLAG_CATEGORY")) == {"[4, 1, 6]", "[1, 127, 6]"}


def changed_archive(root, source):
    # Change a channel value of the last spectral-line integration's
    # first cross baseline during replay. The record layout and ordinal
    # identifiers still agree with what the first pass declared.
    raw = source.read_bytes()
    location = None
    for rca, _ in logical_record_starts(raw):
        antennas = struct.unpack_from(">H", raw, rca + 2 * 17)[0]
        for cda in range(4):
            base = rca + 8 * cda
            offset = 2 * struct.unpack_from(">I", raw, base + 2 * 18)[0]
            width = 2 * struct.unpack_from(">H", raw, base + 2 * 21)[0]
            if offset and width >= 512 and antennas > 2:
                # This fixture has 128 archived channels. The first
                # non-average channel is 24 bytes into the baseline.
                block, within = divmod(offset + width * antennas + 24,
                                       RECORD_BYTES - HEADER_BYTES)
                location = rca + block * RECORD_BYTES + within
                break
    assert location is not None
    archive = root / source.name
    archive.write_bytes(raw)
    output = root / "changed.ps.zarr"

    def mutate():
        deadline = monotonic() + 25
        while not list(root.glob(f".{output.name}.staging-*")):
            if monotonic() >= deadline:
                raise AssertionError("MSv4 staging directory never appeared")
            sleep(0.002)
        with archive.open("r+b") as stream:
            stream.seek(location)
            stream.write(bytes([raw[location] ^ 16]))

    with ThreadPoolExecutor() as pool:
        writing = pool.submit(mutate)
        try:
            tasks.importvla(archivefiles=[archive], vis=output, outformat="msv4")
        except RuntimeError as error:
            assert "changed between MSv4 declaration and write" in str(error), str(error)
        else:
            raise AssertionError("the archive changed between passes and was published")
        finally:
            writing.result()
    assert not output.exists() and not list(root.glob(".*.staging-*"))
    print("passed changed archive refusal before publication", flush=True)


def one_spectral_integration(source):
    contents = source.read_bytes()
    for rca, end in logical_record_starts(contents):
        sda = rca + 2 * struct.unpack_from(">I", contents, rca + 2 * 12)[0]
        if contents[sda + 30:sda + 32] == b"  " and contents[sda + 314:sda + 318] == b"1A  ":
            return bytearray(contents[rca - HEADER_BYTES:end])
    raise AssertionError("no standard 1A integration found in the public spectral fixture")


def sda_offset(record):
    return HEADER_BYTES + 2 * struct.unpack_from(">I", record, HEADER_BYTES + 2 * 12)[0]


def move_first_antenna(record, metres):
    ada = HEADER_BYTES + 2 * struct.unpack_from(">I", record, HEADER_BYTES + 2 * 14)[0]
    offset = ada + 2 * 42
    encoded = struct.unpack_from(">q", record, offset)[0]
    magnitude = abs(encoded)
    # ModComp stores a 54-bit fraction and a nine-bit biased exponent.
    fraction = magnitude & ((1 << 54) - 1)
    value = math.ldexp(fraction, (magnitude >> 54) - 310)
    value = math.copysign(value, encoded) + metres / 0.299792458
    significand, exponent = math.frexp(abs(value))
    encoded = ((exponent + 256) << 54) | round(significand * (1 << 54))
    struct.pack_into(">q", record, offset, encoded if value >= 0 else -encoded)


def refuse_msv4(root, archives, expected):
    output = root / "protected.ps.zarr"
    for path in (output, Path(f"{output}.flagversions")):
        assert not path.exists()
    try:
        tasks.importvla(archivefiles=archives, vis=output, outformat="msv4")
    except tasks.TaskParameterError as error:
        assert error.completion.parameter == "outformat" and expected in str(error), str(error)
    else:
        raise AssertionError(f"MSv4 published an input requiring refusal for {expected}")
    assert not output.exists() and not Path(f"{output}.flagversions").exists()
    assert not list(root.glob(".*.staging-*"))


def review_layout_cases(root, continuum, spectral, xradio_python):
    single = one_spectral_integration(spectral)
    first, second = root / "archive-1", root / "archive-2"
    for path in (first, second):
        path.write_bytes(single)

    # CASA and casa-rs MSv2 preserve each archive as its own observation,
    # including when the bytes of the two archives match.
    reference = paired(root, [first, second], "two-sessions", outformat="msv2")
    with table(reference / "OBSERVATION") as tab:
        assert tab.nrows() == 2
        assert len(set(tab.getcol("PROJECT"))) == 1
    with table(reference) as tab:
        assert set(tab.getcol("OBSERVATION_ID")) == {0, 1}
    output = root / "two-sessions.ps.zarr"
    assert tasks.importvla(archivefiles=[first, second], vis=output, outformat="msv4") is None
    compare_xradio(xradio_python, reference, output, "--sessions", "2")
    reopened = root / "two-reopened.ms"
    tasks.msconvert(vis=output, outputvis=reopened, outformat="msv2", padding="omitted")
    with table(reopened / "OBSERVATION") as tab:
        assert tab.nrows() == 2
    with table(reopened) as tab:
        assert set(tab.getcol("OBSERVATION_ID")) == {0, 1}

    unknown = bytearray(single)
    struct.pack_into(">h", unknown, sda_offset(unknown) + 2 * 161, 0)
    second.write_bytes(unknown)
    legacy = root / "unknown-epoch-casa.ms"
    assert casatasks.importvla(archivefiles=[str(second)], vis=str(legacy)) is None
    with table(legacy) as tab:
        assert tab.getcolkeyword("UVW", "MEASINFO")["Ref"] == "B1950_VLA"
    refuse_msv4(root, [second], "unknown SDA epoch 0")
    refuse_msv4(root, [first, continuum], "B1950_VLA")

    moved = bytearray(single)
    move_first_antenna(moved, 2.0)
    second.write_bytes(moved)
    ms_v2 = root / "moved-antenna.ms"
    tasks.importvla(archivefiles=[first, second], vis=ms_v2, outformat="msv2")
    with table(ms_v2 / "ANTENNA") as tab:
        antennas = {}
        for name, station, position in zip(tab.getcol("NAME"), tab.getcol("STATION"),
                                           tab.getcol("POSITION").T, strict=True):
            antennas.setdefault((name, station), []).append(position)
        placements = [pair for pair in antennas.values() if len(pair) == 2]
        assert len(placements) == 1
        left, right = placements[0]
        np.testing.assert_allclose(right - left, [0.0, 0.0, 2.0], atol=1e-8, rtol=0)
    refuse_msv4(root, [first, second], "has two positions")
    print("passed distinct sessions and typed frame/antenna refusals", flush=True)


def compare_xradio(xradio_python, reference, output, *options):
    compare = Path(__file__).resolve().parents[1] / "ms-trial" / "check_importvla.py"
    # The pinned XRADIO interpreter owns its dependency environment.
    child_env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    subprocess.run([str(xradio_python), str(compare), str(reference), str(output), *options],
                   env=child_env, check=True)


def format_cases(root, continuum, spectral, xradio_python):
    expected = paired(root, [spectral], "explicit-v2", outformat="msv2")
    output = root / "explicit-v4.ms"
    errors = StringIO()
    with redirect_stderr(errors):
        assert tasks.importvla(archivefiles=[spectral], vis=output, outformat="msv4") is None
    assert (output / "zarr.json").is_file()
    assert not (output / "table.dat").exists() and not Path(f"{output}.flagversions").exists()
    with table(expected / "FIELD") as tab:
        assert "A" in tab.getcol("CODE") and len(set(tab.getcol("TIME"))) > 1
    with table(expected / "SPECTRAL_WINDOW") as tab:
        assert tab.getcell("IF_CONV_CHAIN", 0) == 0
        assert tab.getcell("FREQ_GROUP", 0) == 1
        assert tab.getcell("FREQ_GROUP_NAME", 0) == "Group 1"
    for loss in ("FLAG_CATEGORY is not carried", "FLAG_ROW is folded into FLAG",
                 "SIGMA is not carried", "Original flag-version snapshot is not carried",
                 "FIELD.CODE is not carried", "FIELD.TIME is not carried",
                 "SPECTRAL_WINDOW.IF_CONV_CHAIN is not carried",
                 "SPECTRAL_WINDOW.FREQ_GROUP is not carried",
                 "SPECTRAL_WINDOW.FREQ_GROUP_NAME is not carried"):
        assert loss in errors.getvalue(), errors.getvalue()
    compare_xradio(xradio_python, expected, output)
    original = (output / "zarr.json").read_bytes()
    try:
        tasks.importvla(archivefiles=[spectral], vis=output, outformat="msv4")
    except RuntimeError as error:
        assert "already exists" in str(error), str(error)
    else:
        raise AssertionError("the second import replaced an existing MSv4")
    assert (output / "zarr.json").read_bytes() == original

    with TemporaryDirectory(prefix="unsupported-") as directory:
        refuse_msv4(Path(directory), [continuum], "B1950_VLA")


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
    if "changed-archive" in args.cases:
        if args.spectral is None:
            parser.error("changed-archive requires --spectral")
        with TemporaryDirectory(prefix="importvla-") as temporary:
            changed_archive(Path(temporary), args.spectral.resolve(strict=True))
    if "review-layouts" in args.cases:
        if args.spectral is None or args.xradio_python is None:
            parser.error("review-layouts requires --spectral and --xradio-python")
        with TemporaryDirectory(prefix="importvla-") as temporary:
            review_layout_cases(Path(temporary), archive, args.spectral.resolve(strict=True),
                                args.xradio_python.absolute())
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
