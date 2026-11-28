#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare unpaged listcal files from both formats with CASA."""

import argparse
import json
from pathlib import Path
import re
import shutil
import tarfile

import casatasks as casa
from casatools import table
import numpy as np

from calibration_smoothcal_oracle import fixture
from listcal_oracle import same_text


TRIAL = (
    Path(__file__).resolve().parents[2]
    / "libs/casacore/casa-ms/tests/fixtures/ms_trial"
)


def list_file(engine, vis, caltable, output, **selectors):
    assert engine.listcal(
        vis=str(vis), caltable=str(caltable), listfile=str(output), pagerows=0, **selectors,
    ) is None
    return output.read_bytes()


def flag_rows(source, subtable, rows):
    tb = table()
    assert tb.open(str(source / subtable), nomodify=False)
    try:
        for row in rows:
            assert tb.putcell("FLAG_ROW", row, True)
    finally:
        tb.close()


def produce_bandpass(source, root):
    noisy = root / "bandpass-source.ms"
    shutil.copytree(source, noisy)
    rng = np.random.default_rng(17)
    tb = table()
    assert tb.open(str(noisy), nomodify=False)
    try:
        for row in range(tb.nrows()):
            data = tb.getcell("DATA", row)
            noise = rng.normal(size=data.shape) + 1j * rng.normal(size=data.shape)
            assert tb.putcell("DATA", row, (data + 1e-3 * noise).astype(data.dtype))
    finally:
        tb.close()
    caltable = root / "bandpass.cal"
    casa.bandpass(
        vis=str(noisy), caltable=str(caltable), refant="0", minblperant=2,
        minsnr=0.0, solint="inf", smodel=[1, 0, 0, 0],
    )
    assert tb.open(str(caltable / "SPECTRAL_WINDOW"))
    try:
        np.testing.assert_array_equal(tb.getcol("NUM_CHAN"), [4, 3])
    finally:
        tb.close()
    return caltable


def renumbered_msv4(source, root):
    reordered = root / "reordered.ps.zarr"
    shutil.copytree(source, reordered)
    first, second = sorted(directory for directory in reordered.iterdir() if directory.is_dir())
    holding = reordered / "holding"
    first.rename(holding)
    second.rename(first)
    holding.rename(second)

    missing = root / "missing.ps.zarr"
    shutil.copytree(source, missing)
    shutil.rmtree(missing / second.name)
    return reordered, missing


def selected_file(root, tag, ms_v2, ms_v4, caltable, native, reference, candidate):
    def opts(selectors):
        return dict(zip(("field", "antenna", "spw"), selectors, strict=True))

    expected = list_file(casa, ms_v2, caltable, root / f"{tag}-casa.txt", **opts(reference))
    old = list_file(native, ms_v2, caltable, root / f"{tag}-msv2.txt", **opts(reference))
    actual = list_file(native, ms_v4, caltable, root / f"{tag}-msv4.txt", **opts(candidate))
    assert old == expected
    expected_v4 = expected.replace(f"MS name = {ms_v2}".encode(), f"MS name = {ms_v4}".encode())
    same_text(actual.decode(), expected_v4.decode())
    assert actual == expected_v4
    return actual.decode()


def check_selected_bandpass(root, ms_v2, ms_v4, caltable, native):
    cases = (
        ("stride", ("0", "2,0", "0:0~3^2"), None, ["0", "2"]),
        ("names", ("ppdisk", "VLA02", "high:2"), ("ppdisk_0", "VLA02_N02", "high_1:2"), ["2"]),
        ("windows", ("0", "0~2", "1:1,0:3"), None, ["3", "1"]),
        ("first-range", ("0", "0", "0:1~1;3~3"), None, ["1"]),
    )
    return {
        key: assert_selected_channels(selected_file(
            root, key, ms_v2, ms_v4, caltable, native, reference, candidate or reference,
        ), channels)
        for key, reference, candidate, channels in cases
    }


def assert_selected_channels(listing, expected):
    channels = re.findall(r"^\S+ +ppdisk +(\d+)\|", listing, flags=re.MULTILINE)
    assert channels == expected, (channels, expected)
    return channels


def check_bandpass(root, ms_v2, ms_v4, native):
    caltable = produce_bandpass(ms_v2, root)
    expected = list_file(casa, ms_v2, caltable, root / "casa-bandpass.txt")
    actual = list_file(native, ms_v4, caltable, root / "msv4-bandpass.txt")
    assert actual == expected.replace(f"MS name = {ms_v2}".encode(), f"MS name = {ms_v4}".encode())
    windows = re.findall(r"SpwID = (\d+)", actual.decode())
    assert windows == ["0", "1"]

    result = {"bandpass-compatible": len(actual)}
    reordered, missing = renumbered_msv4(ms_v4, root)
    for source in (reordered, missing):
        output = root / f"{source.name}.txt"
        try:
            list_file(native, source, caltable, output)
        except native.TaskParameterError as error:
            assert error.completion.parameter == "vis"
            assert "spectral-window identity" in error.completion.message
            assert not output.exists()
        else:
            raise AssertionError(f"listed the incompatible {source.name} store")
        result[source.name] = "refused before publication"
    result.update(check_selected_bandpass(root, ms_v2, ms_v4, caltable, native))
    output = root / "explicit-reordered.txt"
    try:
        list_file(native, reordered, caltable, output, spw="0:1")
    except native.TaskParameterError as error:
        assert error.completion.parameter == "vis"
        assert "spectral-window identity" in error.completion.message
        assert not output.exists()
    else:
        raise AssertionError("listed a selected window on the wrong physical grid")
    selected = ("0", "1", "0:2")
    result["explicit-missing-unselected"] = assert_selected_channels(selected_file(
        root, "missing-unselected", ms_v2, missing, caltable, native, selected, selected,
    ), ["2"])
    return result


def check_field_selection(root, native):
    root.mkdir()
    ms_v2, ms_v4, caltable = (root / name for name in ("source.ms", "converted.ps.zarr", "gain.cal"))
    fixture(ms_v2, caltable)
    conversion = native.msconvert(
        vis=str(ms_v2), outputvis=str(ms_v4), outformat="msv4", withpointing=False,
    )
    assert conversion.successful, conversion.describe()
    full = list_file(casa, ms_v2, caltable, root / "unfiltered.txt", antenna="0", spw="0:0")
    assert b"FLUX" in full and b"PHASE" in full
    selected = selected_file(
        root, "phase-field", ms_v2, ms_v4, caltable, native,
        ("PHASE", "0", "0:0"), ("PHASE_1", "0", "0:0"),
    )
    assert "PHASE" in selected and "FLUX" not in selected
    return "PHASE only"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--msv2-only", action="store_true")
    args = parser.parse_args()
    assert casa.version_string() == "6.7.6.14"
    from casars import tasks as native

    root = args.work.resolve()
    root.mkdir()
    ms_v2 = root / "ms_trial_lines.ms"
    ms_v4 = root / "ms_trial_lines.ps.zarr"
    for source in (ms_v2,) if args.msv2_only else (ms_v2, ms_v4):
        with tarfile.open(TRIAL / f"{source.name}.tgz") as archive:
            archive.extractall(root, filter="data")
    caltable = root / "gain.cal"
    assert casa.gencal(
        vis=str(ms_v2), caltable=str(caltable), caltype="amp", parameter=[2.0],
    ) is None
    expected = list_file(casa, ms_v2, caltable, root / "casa.txt")
    direct = list_file(native, ms_v2, caltable, root / "msv2.txt")
    assert direct == expected
    old_label = f"MS name = {ms_v2}".encode()
    assert expected.count(old_label) == 2
    result = {"MSv2": len(direct)}
    if not args.msv2_only:
        actual = list_file(native, ms_v4, caltable, root / "msv4.txt")
        new_label = f"MS name = {ms_v4}".encode()
        same_text(actual.decode(), expected.replace(old_label, new_label).decode())
        assert actual == expected.replace(old_label, new_label)
        result["MSv4"] = len(actual)
        result.update(check_bandpass(root, ms_v2, ms_v4, native))
        result["two-fields"] = check_field_selection(root / "two-fields", native)

    for subtable in ("SPECTRAL_WINDOW", "DATA_DESCRIPTION"):
        for rows in ((1,), (0, 1)):
            label = f"{subtable}-{len(rows)}"
            source = root / f"{label}.ms"
            shutil.copytree(ms_v2, source)
            flag_rows(source, subtable, rows)
            reference, candidate = (root / f"{label}-{engine}.txt" for engine in ("casa", "native"))
            if len(rows) == 1:
                expected = list_file(casa, source, caltable, reference)
                assert expected == list_file(native, source, caltable, candidate)
                result[label] = re.findall(r"SpwID = (\d+)", expected.decode())
                assert result[label] == ["0", "1"]
            else:
                for engine, output, failure in (
                    (casa, reference, RuntimeError),
                    (native, candidate, native.TaskExecutionError),
                ):
                    try:
                        list_file(engine, source, caltable, output)
                    except failure:
                        assert not output.exists()
                    else:
                        raise AssertionError(f"{engine} accepted {label}")
                result[label] = "refused before publication"
    print(json.dumps(result))


if __name__ == "__main__":
    main()
