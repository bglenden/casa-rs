#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA fixture pilot for the approved MFS sky; no production implementation."""

import argparse
import json
import math
from pathlib import Path
import subprocess
import time

import numpy as np

from prepare_mfs_4096 import ARCSEC, sky_components, spectral_windows


def numpy_json(value):
    if isinstance(value, (np.ndarray, np.generic)):
        return value.tolist()
    raise TypeError(f"Not JSON serializable: {type(value).__name__}")


def save(path, value):
    encoded = json.dumps(value, indent=2, default=numpy_json)
    with path.open("x") as stream:
        stream.write(encoded + "\n")


def direction(east_arcsec, north_arcsec):
    """Inverse SIN projection, matching the truth renderer's direction cosines."""
    east, north = east_arcsec * ARCSEC, north_arcsec * ARCSEC
    radial = math.sqrt(1 - east * east - north * north)
    dec0 = math.radians(30)
    return (
        math.pi + math.atan2(east, radial * math.cos(dec0) - north * math.sin(dec0)),
        math.asin(north * math.cos(dec0) + radial * math.sin(dec0)),
    )


def geometry(args):
    from casatasks import casalog, concat, version_string
    from casatools import measures, simulator

    args.output.mkdir(parents=True, exist_ok=False)
    casalog.setlogfile(str(args.output / "geometry-casa.log"))
    windows = spectral_windows()
    # This pilot changes time-sample count, not exposure length or channelization.
    centers = np.linspace(-3 * 3600, 3 * 3600, args.integrations)
    source_name = "mfs-4096-source"
    datasets = []
    started = time.perf_counter()
    for config, epoch in (("A", "2024/01/15/00:00:00"), ("C", "2024/04/15/00:00:00")):
        ms = args.output / f"{config}.ms"
        positions = np.loadtxt(args.arrays / f"vla.{config.lower()}.cfg", dtype=str)
        xyz = positions[:, :3].astype(float)
        sm, me = simulator(), measures()
        try:
            sm.open(str(ms))
            sm.setconfig(
                telescopename="EVLA",
                x=xyz[:, 0],
                y=xyz[:, 1],
                z=xyz[:, 2],
                dishdiameter=positions[:, 3].astype(float),
                mount=["ALT-AZ"] * 27,
                antname=[f"{config}-{s}" for s in positions[:, -1]],
                coordsystem="global",
                referencelocation=me.observatory("VLA"),
            )
            sm.setfield(
                sourcename=source_name,
                sourcedirection=me.direction("J2000", "12h00m00s", "30deg"),
            )
            sm.setfeed(mode="perfect R L")
            sm.setauto(autocorrwt=0)
            sm.setlimits(shadowlimit=0.01, elevationlimit="10deg")
            sm.settimes(
                integrationtime="2s",
                usehourangle=True,
                referencetime=me.epoch("UTC", epoch),
            )
            for w in windows:
                sm.setspwindow(
                    spwname=f"spw-{w['spw_id']:02}",
                    freq=f"{w['first_center_hz']}Hz",
                    deltafreq="2MHz",
                    freqresolution="2MHz",
                    refcode="LSRK",
                    nchannels=64,
                    stokes="RR RL LR LL",
                )
            for w in windows:
                if not sm.observemany(
                    sourcenames=[source_name] * args.integrations,
                    spwname=f"spw-{w['spw_id']:02}",
                    starttimes=[f"{v - 1:.9f}s" for v in centers],
                    stoptimes=[f"{v + 1:.9f}s" for v in centers],
                    add_observation=False,
                    project="casa-rs MFS pilot",
                ):
                    raise RuntimeError(f"CASA observe failed: {config} {w['spw_id']}")
            print(f"geometry {config}: {args.integrations} times x 32 SPWs", flush=True)
        finally:
            sm.done()
            me.done()
        datasets.append(str(ms))

    combined = args.output / "pilot.ms"
    concat(
        vis=datasets,
        concatvis=str(combined),
        timesort=True,
        copypointing=True,
        freqtol="1Hz",
        dirtol="0.001arcsec",
    )
    save(
        args.output / "geometry-created.json",
        dict(
            casa_version=version_string(),
            seconds=time.perf_counter() - started,
            datasets=datasets,
            combined_ms=str(combined),
        ),
    )
    finish_geometry(args)


def finish_geometry(args):
    """Finish a newly created pilot, including one interrupted before prediction."""
    from casatasks import version_string
    from casatools import componentlist, table

    started = time.perf_counter()
    combined = args.output / "pilot.ms"
    windows = spectral_windows()
    tb = table()
    tb.open(str(combined / "DATA_DESCRIPTION"))
    try:
        dd_to_spw = tb.getcol("SPECTRAL_WINDOW_ID")
    finally:
        tb.close()
    tb.open(str(combined), nomodify=False)
    try:
        # Add only the explicitly proposed subband/band-edge flags.
        for start in range(0, tb.nrows(), 4096):
            count = min(4096, tb.nrows() - start)
            ddids = tb.getcol("DATA_DESC_ID", start, count)
            flags = tb.getcol("FLAG", start, count)
            for ddid in np.unique(ddids):
                flags[:, windows[int(dd_to_spw[ddid])]["flagged_channels"], :] |= (
                    ddids == ddid
                )[None, None, :]
            tb.putcol("FLAG", flags, start, count)
    finally:
        tb.close()
        tb.done()

    cl = componentlist()
    try:
        for i, component in enumerate(sky_components()):
            ra, dec = direction(component["east_arcsec"], component["north_arcsec"])
            kwargs = dict(
                flux=component["flux_jy"],
                fluxunit="Jy",
                polarization="Stokes",
                dir=["J2000", f"{ra:.16g}rad", f"{dec:.16g}rad"],
                freq="6GHz",
                spectrumtype="spectral index",
                index=component["spectral_index"],
                label=component["name"],
            )
            if component["major_arcsec"]:
                kwargs.update(
                    shape="Gaussian",
                    majoraxis=f"{component['major_arcsec']}arcsec",
                    minoraxis=f"{component['minor_arcsec']}arcsec",
                    positionangle=f"{component['pa_degrees']}deg",
                )
            else:
                kwargs["shape"] = "point"
            cl.addcomponent(**kwargs)
            cl.setfreqframe(i, "LSRK")
        cl.rename(str(args.output / "intrinsic-sky.cl"))
    finally:
        cl.done()
    save(
        args.output / "geometry-result.json",
        dict(
            casa_version=version_string(),
            finishing_seconds=time.perf_counter() - started,
            integrations_per_configuration=args.integrations,
            integration_seconds=2,
            combined_ms=str(combined),
            noise=False,
            prediction_status="not predicted; DATA are geometry placeholders",
            sampling="CASA hour-angle setup from -3h to +3h; validate actual epochs/UVW",
            components=325,
            source="mfs-4096-source",
        ),
    )
    inspect(args)


def inspect(args):
    from casatools import table

    tb = table()
    path = args.output / "pilot.ms"
    subtables = {}
    for name, columns in {
        "ANTENNA": ["NAME", "POSITION"],
        "SPECTRAL_WINDOW": ["NUM_CHAN", "CHAN_FREQ", "CHAN_WIDTH", "MEAS_FREQ_REF"],
        "DATA_DESCRIPTION": ["SPECTRAL_WINDOW_ID", "POLARIZATION_ID"],
        "POLARIZATION": ["CORR_TYPE"],
        "OBSERVATION": ["TELESCOPE_NAME", "TIME_RANGE"],
        "FIELD": ["PHASE_DIR"],
    }.items():
        tb.open(str(path / name))
        try:
            subtables[name] = dict(
                rows=tb.nrows(), **{c: tb.getcol(c).tolist() for c in columns}
            )
        finally:
            tb.close()
    dd_to_spw = np.array(subtables["DATA_DESCRIPTION"]["SPECTRAL_WINDOW_ID"])
    rows_by_spw = np.zeros(32, dtype=np.int64)
    times, exposures, intervals, configurations = set(), set(), set(), set()
    flagged, cross_array, nonfinite, weight_min, weight_max = (
        0,
        0,
        0,
        math.inf,
        -math.inf,
    )
    antenna_names = subtables["ANTENNA"]["NAME"]
    tb.open(str(path))
    try:
        rows = tb.nrows()
        columns = tb.colnames()
        for start in range(0, rows, 4096):
            count = min(4096, rows - start)
            ddids = tb.getcol("DATA_DESC_ID", start, count)
            rows_by_spw += np.bincount(dd_to_spw[ddids], minlength=32)
            times.update(tb.getcol("TIME", start, count).tolist())
            exposures.update(tb.getcol("EXPOSURE", start, count).tolist())
            intervals.update(tb.getcol("INTERVAL", start, count).tolist())
            a1, a2 = (
                tb.getcol("ANTENNA1", start, count),
                tb.getcol("ANTENNA2", start, count),
            )
            for first, second in zip(a1, a2):
                configurations.add(antenna_names[first][0])
                cross_array += antenna_names[first][0] != antenna_names[second][0]
            flagged += int(tb.getcol("FLAG", start, count).sum())
            weights = tb.getcol("WEIGHT", start, count)
            weight_min, weight_max = (
                min(weight_min, float(weights.min())),
                max(weight_max, float(weights.max())),
            )
            nonfinite += int((~np.isfinite(tb.getcol("UVW", start, count))).sum())
            nonfinite += int((~np.isfinite(weights)).sum())
    finally:
        tb.close()
        tb.done()
    assert rows == args.integrations * 2 * 351 * 32, rows
    assert subtables["ANTENNA"]["rows"] == 54
    assert subtables["SPECTRAL_WINDOW"]["rows"] == 32
    assert set(subtables["SPECTRAL_WINDOW"]["NUM_CHAN"]) == {64}
    assert set(subtables["SPECTRAL_WINDOW"]["MEAS_FREQ_REF"]) == {1}  # CASA LSRK enum.
    assert set(subtables["DATA_DESCRIPTION"]["SPECTRAL_WINDOW_ID"]) == set(range(32))
    assert len(times) == args.integrations * 2, len(times)
    assert exposures == intervals == {2.0}, (exposures, intervals)
    assert cross_array == nonfinite == 0
    assert weight_min > 0 and weight_min == weight_max
    result = dict(
        rows=rows,
        columns=columns,
        rows_per_spw=rows_by_spw.tolist(),
        unique_times=len(times),
        exposure_seconds=sorted(exposures),
        interval_seconds=sorted(intervals),
        configurations=sorted(configurations),
        cross_configuration_baselines=cross_array,
        nonfinite_metadata=nonfinite,
        flagged_samples=flagged,
        total_samples=rows * 64 * 4,
        weights_min_max=[weight_min, weight_max],
        subtables=subtables,
    )
    save(args.output / "metadata-check.json", result)
    print(json.dumps({k: v for k, v in result.items() if k != "subtables"}), flush=True)


def psf(args):
    from casatasks import casalog, tclean, version_string
    from casatools import image

    target = args.output / "casa-uniform-psf"
    target.mkdir()
    casalog.setlogfile(str(target / "casa.log"))
    request = dict(
        vis=str(args.output / "pilot.ms"),
        imagename=str(target / "image"),
        datacolumn="data",
        imsize=[4096, 4096],
        cell="0.05arcsec",
        phasecenter="J2000 12h00m00s +30d00m00s",
        stokes="I",
        specmode="mfs",
        gridder="wproject",
        wprojplanes=32,
        weighting="uniform",
        niter=0,
        calcres=False,
        calcpsf=True,
        restoration=False,
        pbcor=False,
        savemodel="none",
        parallel=False,
        restart=False,
    )
    save(target / "request.json", request)
    started = time.perf_counter()
    tclean(**request)
    ia = image()
    ia.open(str(target / "image.psf"))
    try:
        beam = ia.restoringbeam()
        shape = ia.shape().tolist()
        cut = ia.getchunk(blc=[1998, 1998, 0, 0], trc=[2098, 2098, 0, 0]).squeeze()
    finally:
        ia.close()
        ia.done()
    result = dict(
        casa_version=version_string(),
        seconds=time.perf_counter() - started,
        beam=beam,
        shape=shape,
        psf_center=float(cut[50, 50]),
        note="CASA wproject PSF only, not sky prediction or deconvolution acceptance",
    )
    save(target / "result.json", result)
    print(json.dumps(result), flush=True)


def predict(args):
    """CASA center-sampled comparison fixture, not finite-bin sky truth."""
    from casatasks import casalog, version_string
    from casatools import simulator, table

    casalog.setlogfile(str(args.output / "prediction-casa.log"))
    sm = simulator()
    started = time.perf_counter()
    try:
        sm.openfromms(str(args.output / "pilot.ms"))
        sm.setdata(spwid=list(range(32)), fieldid=[0])
        sm.setoptions(ftmachine="ft", cache=64 * 1024 * 1024)
        sm.setvp(dovp=True, usedefaultvp=True, dosquint=False, parangleinc="5deg")
        if not sm.predict(
            complist=str(args.output / "intrinsic-sky.cl"), incremental=False
        ):
            raise RuntimeError("CASA component prediction failed")
    finally:
        sm.done()
    data_only(args)
    tb = table()
    tb.open(str(args.output / "pilot.ms"))
    finite, peak, crosshand_peak, parallel_difference = True, 0.0, 0.0, 0.0
    try:
        for start in range(0, tb.nrows(), 4096):
            data = tb.getcol("DATA", start, min(4096, tb.nrows() - start))
            finite = finite and bool(np.isfinite(data).all())
            peak = max(peak, float(np.abs(data).max()))
            crosshand_peak = max(crosshand_peak, float(np.abs(data[1:3]).max()))
            parallel_difference = max(
                parallel_difference, float(np.abs(data[0] - data[3]).max())
            )
    finally:
        tb.close()
        tb.done()
    assert finite and peak > 0
    assert crosshand_peak == parallel_difference == 0
    result = dict(
        casa_version=version_string(),
        seconds=time.perf_counter() - started,
        finite=finite,
        peak_jy=peak,
        crosshand_peak_jy=crosshand_peak,
        parallel_hand_difference_jy=parallel_difference,
        primary_beam="CASA default EVLA, frequency dependent, no squint; component-center approximation",
        averaging="channel/time center samples, NOT finite 2-MHz/2-s integration",
        acceptance="idealized matched-application comparison; not finite-bin or spatially PB-integrated intrinsic-sky truth",
    )
    save(args.output / "prediction-result.json", result)
    print(json.dumps(result), flush=True)


def data_only(args):
    """Discard CASA simulator scratch columns after its last open handle closes."""
    from casatools import table

    assert (args.output / "geometry-created.json").is_file(), "generated fixture only"
    inventory = []
    for name in ("A.ms", "C.ms", "pilot.ms"):
        path = args.output / name
        before_kib = int(subprocess.check_output(["du", "-sk", str(path)]).split()[0])
        tb = table()
        tb.open(str(path), nomodify=False)
        try:
            rows = tb.nrows()
            sample_rows = sorted({0, rows // 2, rows - 1})
            samples = [tb.getcell("DATA", row) for row in sample_rows]
            removed = [
                c for c in ("MODEL_DATA", "CORRECTED_DATA") if c in tb.colnames()
            ]
            if removed:
                tb.removecols(removed)
        finally:
            tb.close()
        tb.open(str(path))
        try:
            retained = tb.colnames()
            assert "DATA" in retained
            assert not set(retained).intersection(("MODEL_DATA", "CORRECTED_DATA"))
            assert tb.nrows() == rows
            for row, expected in zip(sample_rows, samples):
                np.testing.assert_array_equal(tb.getcell("DATA", row), expected)
        finally:
            tb.done()
        after_kib = int(subprocess.check_output(["du", "-sk", str(path)]).split()[0])
        inventory.append(
            dict(
                ms=str(path),
                removed=removed,
                retained=retained,
                rows=rows,
                sampled_DATA_unchanged=True,
                allocated_bytes_before=before_kib * 1024,
                allocated_bytes_after=after_kib * 1024,
            )
        )
    save(args.output / "data-column-inventory.json", inventory)
    print(json.dumps(dict(data_column_inventory=inventory)), flush=True)


def image_pilot(args):
    """Run the real CASA task on the explicitly idealized comparison input."""
    from casatasks import casalog, tclean, version_string

    target = args.output / args.label
    target.mkdir()
    casalog.setlogfile(str(target / "casa.log"))
    request = dict(
        vis=str(args.output / "pilot.ms"),
        imagename=str(target / "image"),
        datacolumn="data",
        spw=args.spw,
        imsize=[4096, 4096],
        cell="0.05arcsec",
        phasecenter="J2000 12h00m00s +30d00m00s",
        stokes="I",
        specmode="mfs",
        reffreq="6GHz",
        gridder=args.gridder,
        wprojplanes=32 if args.gridder == "wproject" else 1,
        weighting="uniform",
        deconvolver="clark" if args.terms == 1 else "mtmfs",
        nterms=args.terms,
        scales=[0],
        niter=args.niter,
        cycleniter=1000,
        gain=0.1,
        threshold="0.005Jy",
        pblimit=-0.2,
        normtype="flatnoise",
        usemask="user",
        mask="",
        restoration=True,
        pbcor=False,
        savemodel="none",
        parallel=False,
        restart=False,
    )
    save(target / "request.json", request)
    started = time.perf_counter()
    result = tclean(**request)
    summary = dict(
        casa_version=version_string(),
        seconds=time.perf_counter() - started,
        scope="application smoke, not spectral or sky-model acceptance",
        task_result=result,
    )
    save(target / "result.json", summary)
    print(json.dumps(summary, default=numpy_json), flush=True)


def compare(args):
    """Compare all saved products; report differences without inventing gates."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import AsinhNorm, Normalize
    from casatools import image
    from perf_harness.casa_image_compare import (
        compare_direction_wcs,
        compare_image_metadata,
    )

    target = args.output / args.label
    target.mkdir()
    prefixes = [
        args.native / "image",
        (args.reference or args.output / "casa-dirty-v1") / "image",
    ]
    inventories = [
        sorted(p.name[6:] for p in prefix.parent.glob("image.*") if p.is_dir())
        for prefix in prefixes
    ]
    save(
        target / "inventory.json",
        dict(candidate=inventories[0], reference=inventories[1]),
    )
    principal = ".tt0" if args.terms > 1 else ""
    panel_products = [f"{family}{principal}" for family in ("residual", "psf", "pb")]
    if args.niter:
        panel_products.insert(0, f"image{principal}")
    metrics = {}
    fig, axes = plt.subplots(
        len(panel_products),
        3,
        figsize=(15, 4.6 * len(panel_products)),
        layout="constrained",
    )
    for suffix in sorted(set(inventories[0]) | set(inventories[1])):
        if not all(suffix in inventory for inventory in inventories):
            metrics[suffix] = dict(
                status="missing",
                candidate=suffix in inventories[0],
                reference=suffix in inventories[1],
            )
            continue
        paths = [f"{prefix}.{suffix}" for prefix in prefixes]
        metadata = compare_image_metadata(*paths)
        direction_wcs = compare_direction_wcs(*paths)
        planes, masks = [], []
        for path in paths:
            ia = image()
            ia.open(path)
            try:
                values = ia.getchunk().squeeze()
                planes.append(values)
                masks.append(ia.getchunk(getmask=True).squeeze())
            finally:
                ia.done()
        native, casa = planes
        assert native.shape == casa.shape, (suffix, native.shape, casa.shape)
        assert np.isfinite(native).all() and np.isfinite(casa).all(), suffix
        difference = native.astype(np.float64) - casa
        norm = float(np.linalg.norm(casa))
        metrics[suffix] = dict(
            status="measured",
            metadata=metadata,
            direction_wcs=direction_wcs,
            compared_pixels=int(native.size),
            mask_mismatch_pixels=int(np.count_nonzero(masks[0] != masks[1])),
            candidate_valid_pixels=int(np.count_nonzero(masks[0])),
            reference_valid_pixels=int(np.count_nonzero(masks[1])),
            arrays_equal=bool(np.array_equal(native, casa)),
            max_abs_difference=float(np.abs(difference).max()),
            rms_difference=float(np.sqrt(np.mean(difference**2))),
            casa_peak=float(np.abs(casa).max()),
            native_peak=float(np.abs(native).max()),
            relative_l2=float(np.linalg.norm(difference) / norm) if norm else None,
            native_peak_pixel=np.unravel_index(np.argmax(native), native.shape),
            casa_peak_pixel=np.unravel_index(np.argmax(casa), casa.shape),
        )
        if suffix in panel_products:
            row = panel_products.index(suffix)
            family = suffix.split(".")[0]
            region = (
                (slice(1948, 2149),) * 2
                if family == "psf"
                else (slice(None, None, 4),) * 2
            )
            extent = tuple(
                bound
                for selection, size in zip(region, casa.shape)
                for bound in (selection.start or 0, selection.stop or size)
            )
            view_label = (
                "central 201×201 crop"
                if family == "psf"
                else "full field; every 4th pixel"
            )
            amplitude = float(np.abs(casa[region]).max())
            difference_amplitude = float(np.abs(difference[region]).max())
            for column, (name, values) in enumerate(
                zip(
                    (
                        args.candidate_label,
                        args.reference_label,
                        "candidate minus reference",
                    ),
                    (native, casa, difference),
                )
            ):
                limit = (difference_amplitude if column == 2 else amplitude) or 1e-15
                if family == "pb":
                    pb_difference_limit = difference_amplitude or 1.0
                    norm = (
                        Normalize(0, 1)
                        if column != 2
                        else Normalize(-pb_difference_limit, pb_difference_limit)
                    )
                    cmap = "viridis" if column != 2 else "RdBu_r"
                else:
                    norm = AsinhNorm(linear_width=limit / 1000, vmin=-limit, vmax=limit)
                    cmap = "RdBu_r"
                axis = axes[row, column]
                im = axis.imshow(
                    values[region].T,
                    origin="lower",
                    extent=extent,
                    norm=norm,
                    cmap=cmap,
                    interpolation="nearest",
                )
                display_name = (
                    "dirty image" if family == "residual" and not args.niter else suffix
                )
                scale_label = "linear" if family == "pb" else "asinh"
                axis.set_title(
                    f"{name}: {display_name} ({scale_label})\n{view_label}", fontsize=10
                )
                axis.set_xlabel("image x (original pixels)")
                axis.set_ylabel("image y (original pixels)")
                if family != "psf":
                    axis.set_xticks(np.linspace(0, casa.shape[0], 5))
                    axis.set_yticks(np.linspace(0, casa.shape[1], 5))
                colorbar = fig.colorbar(im, ax=axis, shrink=0.7)
                if family == "pb" and column == 2 and difference_amplitude == 0:
                    colorbar.set_ticks([0])
                    axis.text(
                        0.5,
                        0.5,
                        "Exactly zero",
                        ha="center",
                        va="center",
                        transform=axis.transAxes,
                    )
    fig.suptitle(
        f"4096-square, SPWs {args.spw}, {args.integrations} times/config — "
        f"{args.terms} Taylor term(s), niter ceiling {args.niter}; idealized comparison"
    )
    fig.savefig(target / "dirty-comparison.png", dpi=160)
    plt.close(fig)
    save(target / "metrics.json", metrics)
    print(
        json.dumps(
            {
                key: {
                    k: v
                    for k, v in value.items()
                    if k not in ("metadata", "direction_wcs")
                }
                for key, value in metrics.items()
            },
            default=numpy_json,
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=(
            "geometry",
            "finish-geometry",
            "psf",
            "predict",
            "data-only",
            "image",
            "compare",
        ),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--arrays", type=Path)
    parser.add_argument("--integrations", type=int, default=36)
    parser.add_argument("--terms", type=int, choices=range(1, 5), default=1)
    parser.add_argument("--spw", default="0~31", help="CASA SPW selection for imaging")
    parser.add_argument(
        "--gridder", choices=("standard", "wproject"), default="standard"
    )
    parser.add_argument("--niter", type=int, default=0)
    parser.add_argument("--label", default="casa-dirty-v1")
    parser.add_argument("--native", type=Path)
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--candidate-label", default="casa-rs")
    parser.add_argument("--reference-label", default="CASA")
    arguments = parser.parse_args()
    {
        "geometry": geometry,
        "finish-geometry": finish_geometry,
        "psf": psf,
        "predict": predict,
        "data-only": data_only,
        "image": image_pilot,
        "compare": compare,
    }[arguments.action](arguments)
