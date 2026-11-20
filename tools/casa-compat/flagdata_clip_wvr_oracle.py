#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare flagdata visibility clipping of stored-I buffers with CASA 6.7.6.14."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from flagdata_autocorr_oracle import assert_same_visibilities
from flagdata_preview_oracle import assert_preserved, auxiliary_content, changed_samples
from flagdata_quack_oracle import assert_equal
from flagdata_summary_oracle import snapshot
from record_casa_returns import make_cal_ms

SAMPLES = np.array([-3-4j, .5-.5j, 2j, 1], dtype="c8")
FLOAT_SAMPLES = np.array([-3, -1, 0, 1], dtype="f4")


def make_inputs():
    make_cal_ms("stored-i.ms", channels=4, scan_seconds=20, stokes="I", frequency="105GHz")
    make_cal_ms("linear.ms", channels=4, scan_seconds=20, stokes="XX YY")
    make_cal_ms("circular.ms", channels=4, scan_seconds=20, stokes="RR LL", frequency="110GHz")
    make_cal_ms("stored-q.ms", channels=4, scan_seconds=20, stokes="Q", frequency="115GHz")
    casatasks.concat(vis=["linear.ms", "stored-i.ms", "circular.ms"],
                     concatvis="mixed.ms", freqtol="1Hz")
    casatasks.concat(vis=["stored-i.ms", "stored-q.ms"], concatvis="mixed-q.ms", freqtol="1Hz")
    copytree("stored-i.ms", "float-i.ms")
    with table("float-i.ms", readonly=False, ack=False) as ms:
        ms.addcols(maketabdesc(makearrcoldesc("FLOAT_DATA", 0.0, ndim=2, valuetype="float")))
        ms.putcol("FLOAT_DATA", np.broadcast_to(FLOAT_SAMPLES[None, :, None],
                                               (ms.nrows(), 4, 1)).copy())
    metadata = {}
    for path in ("stored-i", "float-i", "mixed", "mixed-q"):
        vis = f"{path}.ms"
        with table(f"{vis}/POLARIZATION", ack=False) as pol:
            products = [tuple(pol.getcell("CORR_TYPE", row)) for row in range(pol.nrows())]
        with table(f"{vis}/DATA_DESCRIPTION", ack=False) as desc:
            mapping = [products[index] for index in desc.getcol("POLARIZATION_ID")]
            windows = desc.getcol("SPECTRAL_WINDOW_ID")
        with table(vis, readonly=False, ack=False) as ms:
            ddids = ms.getcol("DATA_DESC_ID")
            for row in range(ms.nrows()):
                shape = ms.getcell("FLAG", row).shape
                ms.putcell("DATA", row, np.broadcast_to(SAMPLES[:, None], shape).copy())
                ms.putcell("FLAG", row, np.zeros(shape, dtype=bool))
                ms.putcell("FLAG_ROW", row, False)
        metadata[path] = ([mapping[ddid] for ddid in ddids], windows[mapping.index((1,))])
    return metadata


def compare_products(metadata):
    amplitude = np.abs(SAMPLES)
    reality = SAMPLES.real
    imaginary = SAMPLES.imag
    phase = np.angle(SAMPLES)
    power = SAMPLES.real**2 + SAMPLES.imag**2
    linear, wvr = (9, 12), (1,)
    whole = slice(None)
    cases = (
        ("stored-i", "", (), amplitude, (1, 2), whole),
        ("stored-i", "\0ABS_WVR", (), amplitude, (1, 2), whole),
        ("stored-i", "ABS_ALL", (), amplitude, (1, 2), whole),
        ("stored-i", "ABS_I", (), amplitude, (1, 2), whole),
        ("stored-i", "WVR", (), amplitude, (1, 2), whole),
        ("stored-i", "I", (), amplitude, (1, 2), whole),
        ("stored-i", "WVR_I", wvr, amplitude, (1, 2), whole),
        ("stored-i", "ABS_WVR", wvr, amplitude, (1, 2), whole),
        ("stored-i", "REAL_WVR", wvr, reality, (1, 2), whole),
        ("stored-i", "IMAG_WVR", wvr, imaginary, (1, 2), whole),
        ("stored-i", "ARG_WVR", wvr, phase, (-2, 2), whole),
        ("stored-i", "NORM_WVR", wvr, power, (1, 2), whole),
        ("mixed", "", linear, amplitude, (1, 2), whole),
        ("mixed", "ABS_ALL", linear, amplitude, (1, 2), whole),
        ("mixed", "WVR,XX", (), amplitude, (1, 2), whole),
        ("mixed", "WVR_I", wvr, amplitude, (1, 2), whole),
        ("mixed", "ABS_WVR,XX", wvr, amplitude, (1, 2), whole),
        ("mixed", "ABS_WVR", wvr, amplitude, (1, 2), slice(1, 3)),
        ("mixed-q", "", (2,), amplitude, (1, 2), whole),
        ("mixed-q", "ABS_WVR", wvr, amplitude, (1, 2), whole),
        ("float-i", "", (), FLOAT_SAMPLES, (1, 2), whole),
        ("float-i", "ABS_WVR", wvr, FLOAT_SAMPLES, (1, 2), whole),
        ("float-i", "REAL_WVR", wvr, FLOAT_SAMPLES, (1, 2), whole),
    )
    for index, (source, expression, products, values, bounds, channels) in enumerate(cases):
        row_products, wvr_window = metadata[source]
        reference, native = f"reference-{index}.ms", f"native-{index}.ms"
        options = dict(mode="clip", correlation=expression, clipminmax=list(bounds),
                       clipoutside=False, flagbackup=False,
                       datacolumn="FLOAT_DATA" if source == "float-i" else "DATA")
        if channels.start is not None:
            options["spw"] = f"{wvr_window}:{channels.start}~{channels.stop - 1}"
        for path, task in ((reference, casatasks.flagdata), (native, tasks.flagdata)):
            copytree(f"{source}.ms", path)
            assert task(vis=path, **options) == {}, options
        assert_equal(reference, native)
        selected_channels = (values >= bounds[0]) & (values <= bounds[1])
        for labels, (flag_row, flags, data) in zip(row_products, snapshot(native), strict=True):
            expected = np.zeros_like(flags)
            if labels == products:
                expected[channels, :] = selected_channels[channels, None]
            np.testing.assert_array_equal(flags, expected, err_msg=str(options))
            np.testing.assert_array_equal(data, np.broadcast_to(SAMPLES[:, None], data.shape))
            assert not flag_row
    return len(cases)


def compare_preview_and_backup():
    vis = "mixed.ms"
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    options = dict(mode="clip", correlation="IMAG_WVR", clipminmax=[1, 2], clipoutside=False)
    copytree(vis, "preview-reference.ms")
    assert casatasks.flagdata(vis="preview-reference.ms", flagbackup=False, **options) == {}
    expected = changed_samples(before, snapshot("preview-reference.ms"))
    assert casatasks.flagdata(vis=vis, action="calculate", **options) == {}
    preview = tasks.run("flagdata", vis=vis, action="calculate", **options)
    assert preview.value == {}
    report = loads(preview.stdout)["result"]
    assert report["changed_samples"] == sum(expected)
    assert report["changed_rows"] == sum(change > 0 for change in expected)
    assert report["backup_version"] is None
    assert_preserved(vis, before, auxiliary)

    for name, task in (("reference", casatasks.flagdata), ("native", tasks.flagdata)):
        path = f"backup-{name}.ms"
        copytree(vis, path)
        assert task(vis=path, mode="clip", correlation="WVR", clipminmax=[1, 2]) == {}
        assert_same_visibilities(path, before)
        assert Path(f"{path}.flagversions/flags.flagdata_1").is_dir()


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-clip-wvr-") as directory, chdir(directory):
        products = compare_products(make_inputs())
        compare_preview_and_backup()
    print(f"{products} clip WVR and default products, one preview and one backup matched CASA 6.7.6.14")


if __name__ == "__main__":
    main()
