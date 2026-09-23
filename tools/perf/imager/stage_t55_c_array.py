#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Build the explicitly separate T55 C-array single-pointing science fixture."""

import argparse
import copy
from contextlib import ExitStack
import hashlib
import json
import math
import os
from pathlib import Path
import time

import numpy as np

from stage_wave1_datasets import antennas_from_casa_array_config, format_card, pad_block

PIXELS = 1024
CELL_ARCSEC = 0.06
CHANNELS = 512
START_HZ = 44e9
STEP_HZ = 2e6
REFERENCE_HZ = START_HZ + 256 * STEP_HZ
PHASE_CENTER = [math.pi, math.radians(34.07875)]
SKY_FILES = ("sky-v2.fits", "continuum-v2.fits", "line-v2.fits")


def save(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2)


def prepare_geometry(args):
    args.records.mkdir(parents=True, exist_ok=True)
    args.outputs.mkdir(parents=True, exist_ok=True)
    array = args.array_config.read_bytes()
    with (args.records / "vla.c.cfg").open("xb") as stream:
        stream.write(array)
    antennas = antennas_from_casa_array_config(args.array_config)
    assert len(antennas) == 27
    request = json.loads(args.base_request.read_text())
    request["request"].update(
        antennas=antennas, model_image=str(args.outputs / "sky.fits"),
        model_peak_jy_per_pixel=None, field_name="t55-c-array-single",
        phase_center_rad=PHASE_CENTER, fields=[], overwrite=False,
        predict_model=False, corruption=None,
        output_ms=str(args.outputs / "geometry.ms"),
        spectral_setup=dict(name="q-band-endpoints", start_frequency_hz=START_HZ,
                            channel_width_hz=511 * STEP_HZ, channel_count=2))
    save(args.records / "geometry-request.json", request)
    distances = [math.dist(a["position_m"], b["position_m"])
                 for i, a in enumerate(antennas) for b in antennas[i + 1:]]
    save(args.records / "geometry-provenance.json", dict(
        array_source=str(args.array_config), array_sha256=hashlib.sha256(array).hexdigest(),
        antenna_count=len(antennas), physical_baseline_min_max_m=[min(distances), max(distances)],
        image_size=PIXELS, cell_arcsec=CELL_ARCSEC, field_arcsec=PIXELS * CELL_ARCSEC,
        scope="C-array revision; all old D-array datasets and evidence remain unchanged",
        probe="same full observing times/rows, two frequency endpoints, no sky prediction"))


def measure_psf(args):
    from casatasks import casalog, tclean, version_string
    from casatools import image, table

    label = f"uniform-psf-{PIXELS}"
    output = args.outputs / label
    output.mkdir()
    os.chdir(output)
    casalog.setlogfile(str(output / "casa.log"))
    kwargs = dict(vis=str(args.outputs / "geometry.ms"), imagename=str(output / "image"),
                  datacolumn="data", field="0", spw="0", imsize=PIXELS,
                  cell=f"{CELL_ARCSEC}arcsec", stokes="I", specmode="cube", outframe="LSRK",
                  nchan=2, start=0, width=1, interpolation="nearest", gridder="standard",
                  weighting="uniform", perchanweightdensity=True, niter=0,
                  calcres=False, calcpsf=True, restoration=False, pbcor=False,
                  savemodel="none", parallel=False, pblimit=-0.2, restart=False)
    save(args.records / (label + "-request.json"), kwargs)
    started = time.perf_counter()
    tclean(**kwargs)
    seconds = time.perf_counter() - started
    ia = image()
    ia.open(str(output / "image.psf"))
    try:
        beams = ia.restoringbeam()
        shape = ia.shape().tolist()
    finally:
        ia.close()
        ia.done()
    tb = table()
    tb.open(str(args.outputs / "geometry.ms"), nomodify=True)
    try:
        rows = tb.nrows()
        flagged = 0
        uv_min = float("inf")
        uv_max = 0.0
        for start in range(0, rows, 65536):
            count = min(65536, rows - start)
            flags = tb.getcol("FLAG_ROW", start, count)
            uv = tb.getcol("UVW", start, count)[:2, ~flags]
            radius = np.hypot(uv[0], uv[1])
            uv_min, uv_max = min(uv_min, float(radius.min())), max(uv_max, float(radius.max()))
            flagged += int(flags.sum())
    finally:
        tb.close()
        tb.done()
    result = dict(casa_version=version_string(), seconds=seconds, restoring_beams=beams,
                  shape=shape, rows=rows, flagged_rows=flagged,
                  unflagged_projected_baseline_min_max_m=[uv_min, uv_max],
                  cell_arcsec=CELL_ARCSEC, field_arcsec=PIXELS * CELL_ARCSEC,
                  note="Actual CASA uniform PSFs at both band endpoints; no CLEAN or residual calculation")
    save(args.records / (label + "-result.json"), result)
    print(json.dumps(result), flush=True)


def spectral_line(velocity, center, sigma):
    """Gaussian with a smooth 4–5 sigma taper and exact line-free wings."""
    z = np.abs((velocity - center) / sigma)
    taper = 0.5 * (1 + np.cos(np.pi * np.clip(z - 4, 0, 1)))
    return np.exp(-0.5 * z * z) * np.where(z < 5, taper, 0)


def spatial_templates(pixels=PIXELS, cell=CELL_ARCSEC):
    coordinates = (np.arange(pixels) - pixels / 2) * cell
    y, x = np.meshgrid(coordinates, coordinates, indexing="ij")
    radius = np.hypot(x, y)
    taper = 0.5 * (1 + np.cos(np.pi * np.clip((radius - 24) / 3.5, 0, 1)))
    taper[radius >= 27.5] = 0

    def normalized(values):
        values = values * taper
        return values / values.sum()

    def gaussian(cx, cy, major, minor, angle=0):
        a = np.deg2rad(angle)
        u = (x - cx) * np.cos(a) + (y - cy) * np.sin(a)
        v = -(x - cx) * np.sin(a) + (y - cy) * np.cos(a)
        return normalized(np.exp(-4 * np.log(2) * ((u / major)**2 + (v / minor)**2)))

    def point(cx, cy):
        values = np.zeros_like(x)
        px, py = cx / cell + pixels / 2, cy / cell + pixels / 2
        ix, iy = int(np.floor(px)), int(np.floor(py))
        for dx, wx in ((0, 1 - (px - ix)), (1, px - ix)):
            for dy, wy in ((0, 1 - (py - iy)), (1, py - iy)):
                values[iy + dy, ix + dx] = wx * wy
        return normalized(values)

    continuum = [
        ("central_core", 0.8, 0.0, point(-2, 1)),
        ("companion", 0.35, -0.7, gaussian(12, -8, 0.6, 0.35, -20)),
        ("outer_steep_control", 0.18, -0.8, point(-19, 13)),
        ("outer_flat_control", 0.12, 0.0, point(21, 14)),
        ("resolved_jet", 0.4, -0.85, gaussian(-6, 1, 7, 0.7, 35)),
        ("diffuse_continuum", 0.6, -0.5, gaussian(-2, 1, 10, 6, 25)),
    ]
    angle = np.deg2rad(30)
    u = (x + 2) * np.cos(angle) + (y - 1) * np.sin(angle)
    v = (-(x + 2) * np.sin(angle) + (y - 1) * np.cos(angle)) / 0.6
    r = np.hypot(u, v)
    theta = np.arctan2(v, u)
    ring = np.exp(-0.5 * ((r - 4.5) / 0.85)**2)
    disk = normalized((0.7 * ring + 0.3 * np.exp(-0.5 * (r / 4.3)**2))
                      * (1 + 0.45 * np.cos(2 * theta - 0.45 * r)))
    disk_velocity = 420 * np.tanh(r / 2) * u / np.maximum(r, 1e-9)
    filament = normalized(np.exp(-0.5 * ((y + 11 - 0.045 * (x + 12)**2) / 0.35)**2)
                          * np.exp(-4 * np.log(2) * ((x + 12) / 11)**2))
    return dict(x=x, y=y, support=taper, continuum=continuum, disk=disk,
                disk_velocity=disk_velocity, filament=filament,
                filament_velocity=np.clip(55 * (x + 12) - 350, -750, 300),
                companion_line=gaussian(12, -8, 3, 2, -20),
                red_outflow=gaussian(-10, 6, 8, 2, 40),
                blue_outflow=gaussian(6, -2, 6, 2.5, 40),
                outer_line=gaussian(17, 11, 1, 0.7, 15))


def sky_plane(channel, templates):
    frequency = START_HZ + channel * STEP_HZ
    velocity = 299792.458 * (REFERENCE_HZ - frequency) / REFERENCE_HZ
    continuum = np.zeros_like(templates["disk"])
    for _, flux, alpha, spatial in templates["continuum"]:
        continuum += flux * (frequency / REFERENCE_HZ)**alpha * spatial
    emission = (8 * templates["disk"] * spectral_line(velocity, templates["disk_velocity"], 90)
                + 1.2 * templates["filament"] * spectral_line(velocity, templates["filament_velocity"], 65)
                + 0.8 * templates["companion_line"] * spectral_line(velocity, -300, 55)
                + 0.6 * templates["red_outflow"] * spectral_line(velocity, 750, 150)
                + 0.5 * templates["blue_outflow"] * spectral_line(velocity, -650, 150)
                + 0.35 * templates["outer_line"] * spectral_line(velocity, 450, 50))
    absorption = (-0.55 * 0.8 * templates["continuum"][0][3]
                  * spectral_line(velocity, 150, 65))
    line = emission + absorption
    return continuum, line, emission


def fits_header(pixels, cell, channels, phase_center=PHASE_CENTER):
    cards = [("SIMPLE", "T"), ("BITPIX", "-32"), ("NAXIS", "4"),
             ("NAXIS1", str(pixels)), ("NAXIS2", str(pixels)), ("NAXIS3", "1"),
             ("NAXIS4", str(channels)), ("CTYPE1", "'RA---SIN'"),
             ("CTYPE2", "'DEC--SIN'"), ("CUNIT1", "'deg'"), ("CUNIT2", "'deg'"),
             ("RADESYS", "'FK5'"), ("EQUINOX", "2000.0"),
             ("CRPIX1", str(pixels / 2 + 1)), ("CRPIX2", str(pixels / 2 + 1)),
             ("CRVAL1", str(math.degrees(phase_center[0]))),
             ("CRVAL2", str(math.degrees(phase_center[1]))),
             ("CDELT1", f"{-cell / 3600:.14E}"), ("CDELT2", f"{cell / 3600:.14E}"),
             ("CTYPE3", "'STOKES'"), ("CRPIX3", "1"), ("CRVAL3", "1"), ("CDELT3", "1"),
             ("CTYPE4", "'FREQ'"), ("CUNIT4", "'Hz'"), ("SPECSYS", "'LSRK'"),
             ("CRPIX4", "1"), ("CRVAL4", str(START_HZ)), ("CDELT4", str(STEP_HZ)),
             ("BUNIT", "'Jy/pixel'"), ("OBJECT", "'T55 C-ARRAY SINGLE POINTING'")]
    return pad_block(("".join(format_card(k, v) for k, v in cards) + "END".ljust(80)).encode("ascii"))


def build_sky(args):
    # The geometry gate is a measured CASA result, not the earlier D-array estimate.
    beam_record = json.loads((args.records / f"uniform-psf-{PIXELS}-result.json").read_text())
    beam_members = beam_record["restoring_beams"]["beams"].values()
    beam_quantities = [p["minor"] for c in beam_members for p in c.values()]
    assert all(q["unit"] == "arcsec" for q in beam_quantities)
    minors = [q["value"] for q in beam_quantities]
    assert min(minors) / CELL_ARCSEC >= 5, "Revise sampling from measured C-array PSF first"
    templates = spatial_templates()
    profile = []
    selected = {}
    moment = np.zeros((PIXELS, PIXELS))
    with ExitStack() as stack:
        streams = [stack.enter_context((args.outputs / name).open("xb"))
                   for name in SKY_FILES]
        for stream in streams:
            stream.write(fits_header(PIXELS, CELL_ARCSEC, CHANNELS))
        for channel in range(CHANNELS):
            continuum, line, emission = sky_plane(channel, templates)
            total = continuum + line
            assert np.isfinite(total).all() and total.min() >= 0
            assert np.count_nonzero(total[templates["support"] == 0]) == 0
            if channel <= 127 or channel >= 384:
                assert np.count_nonzero(line) == 0
            for stream, plane in zip(streams, (total, continuum, line)):
                stream.write(np.asarray(plane, dtype=">f4").tobytes(order="C"))
            positive_flux = float(emission.sum())
            profile.append(dict(channel=channel, frequency_hz=START_HZ + channel * STEP_HZ,
                                continuum_jy=float(continuum.sum()), line_jy=float(line.sum()),
                                emission_jy=positive_flux, total_jy=float(total.sum()),
                                peak_jy_per_pixel=float(total.max()),
                                emission_centroid_arcsec=([float((emission * templates[k]).sum() / positive_flux)
                                                          for k in ("x", "y")]
                                                         if positive_flux > 0 else None)))
            moment += line
            if channel in (0, 204, 234, 256, 278, 300, 511):
                selected[channel] = (continuum.copy(), line.copy(), total.copy())
            if channel % 64 == 0:
                print(f"sky channels={channel}/{CHANNELS}", flush=True)
        for stream in streams:
            stream.write(b"\0" * (-(PIXELS * PIXELS * CHANNELS * 4) % 2880))
    normalized = [selected[c][1] / np.max(np.abs(selected[c][1])) for c in (204, 234, 256, 278, 300)]
    morphology_change = max(float(np.max(np.abs(p - normalized[2]))) for p in normalized)
    assert morphology_change > 0.1
    nonzero = [p for p in profile if p["emission_jy"] > 0.01]
    centroid_range = [max(p["emission_centroid_arcsec"][i] for p in nonzero)
                      - min(p["emission_centroid_arcsec"][i] for p in nonzero) for i in (0, 1)]
    assert min(centroid_range) > 2
    result = dict(model_kind="idealized independent line-plus-continuum diagnostic sky, not an observed target",
                  shape=[PIXELS, PIXELS, 1, CHANNELS], cell_arcsec=CELL_ARCSEC,
                  support_diameter_arcsec=55, field_arcsec=PIXELS * CELL_ARCSEC,
                  spatial_taper="unity inside radius24arcsec, raised cosine to zero at27.5arcsec",
                  line_free_channel_ranges_inclusive=[[0, 127], [384, 511]],
                  velocity_origin_hz=REFERENCE_HZ,
                  velocity_note="Diagnostic radio-velocity coordinate, not an identified transition/rest frequency",
                  continuum_components=[dict(name=n, reference_flux_jy=f, spectral_index=a)
                                        for n, f, a, _ in templates["continuum"]],
                  line_components=["inclined rotating disk/spiral arms", "curved velocity-gradient filament",
                                   "narrow companion", "red and blue outflow lobes", "outer narrow knot",
                                   "absorption only against central continuum core"],
                  primary_beam="FITS files are intrinsic. Existing native VLA Q-band simulator applies its frequency-dependent PB power once. Do not multiply input sky by a second PB.",
                  morphology_change=morphology_change, emission_centroid_range_arcsec=centroid_range,
                  profiles=profile)
    save(args.records / "sky-v2-truth.json", result)
    request = json.loads((args.records / "geometry-request.json").read_text())
    request["request"].update(predict_model=True, model_image=str(args.outputs / SKY_FILES[0]),
        spectral_setup=dict(name="q-band-c-array-512", start_frequency_hz=START_HZ,
                            channel_width_hz=STEP_HZ, channel_count=CHANNELS),
        corruption=dict(seed=212530740382147, noise=dict(mode="simplenoise", simplenoise_jy=0.05)))
    for name, integration in (("turnaround", 243.0), ("full", 10.0)):
        current = copy.deepcopy(request)
        current["request"].update(output_ms=str(args.outputs / (name + ".ms")), integration_seconds=integration)
        save(args.records / (name + "-v2-request.json"), current)
    render_sky(args.records, selected, moment, profile)
    print(json.dumps({k: v for k, v in result.items() if k != "profiles"}), flush=True)


def render_sky(records, selected, moment, profile):
    os.environ["MPLCONFIGDIR"] = str(records / "matplotlib")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import AsinhNorm, SymLogNorm
    from scipy.ndimage import gaussian_filter

    half = PIXELS * CELL_ARCSEC / 2
    extent = [-half, half, -half, half]
    fig, axes = plt.subplots(2, 4, figsize=(16, 8), layout="constrained")
    panels = [("Continuum-only channel 0", selected[0][0], False),
              ("Continuum-subtracted sum", moment, True),
              ("Total sky, channel 256", selected[256][2], False)]
    for channel in (204, 234, 256, 278, 300):
        velocity = 299792.458 * (REFERENCE_HZ - (START_HZ + channel * STEP_HZ)) / REFERENCE_HZ
        panels.append((f"Line ch {channel}, v={velocity:+.0f} km/s", selected[channel][1], True))
    for ax, (title, plane, signed) in zip(axes.flat, panels):
        plane = gaussian_filter(plane, sigma=0.38 / CELL_ARCSEC / math.sqrt(8 * math.log(2)))
        limit = float(np.max(np.abs(plane)))
        norm = (SymLogNorm(linthresh=limit * 0.002, vmin=-limit, vmax=limit)
                if signed else AsinhNorm(linear_width=limit * 0.002, vmin=0, vmax=limit))
        ax.imshow(plane, origin="lower", extent=extent, cmap="RdBu_r" if signed else "magma", norm=norm)
        ax.add_patch(plt.Circle((0, 0), 27.5, edgecolor="gray", fill=False, ls="--", lw=0.7))
        ax.set(title=title, xlabel="West offset (arcsec)", ylabel="North offset (arcsec)")
    fig.suptitle("New C-array sky — display smoothed to 0.38 arcsec; independent stretches; not reconstructed images")
    fig.savefig(records / "sky-panels-beam-display.png", dpi=130)
    plt.close(fig)
    fig, axes = plt.subplots(2, 1, figsize=(11, 7), layout="constrained", sharex=True)
    frequency = np.array([p["frequency_hz"] for p in profile]) / 1e9
    for name in ("continuum_jy", "line_jy", "emission_jy", "total_jy"):
        axes[0].plot(frequency, [p[name] for p in profile], label=name.replace("_jy", ""))
    axes[0].set(ylabel="Integrated flux (Jy)", title="Spatially integrated intrinsic spectra")
    axes[0].legend()
    for flux, alpha, absorbed, name in ((0.8, 0, True, "central core with absorption"),
                                       (0.18, -0.8, False, "outer steep continuum"),
                                       (0.12, 0, False, "outer flat continuum")):
        spectrum = flux * (frequency * 1e9 / REFERENCE_HZ)**alpha
        if absorbed:
            velocities = 299792.458 * (REFERENCE_HZ - frequency * 1e9) / REFERENCE_HZ
            spectrum *= 1 - 0.55 * spectral_line(velocities, 150, 65)
        axes[1].plot(frequency, spectrum, label=name)
    for ax in axes:
        ax.axvspan(frequency[0], frequency[127], color="green", alpha=0.08)
        ax.axvspan(frequency[384], frequency[-1], color="green", alpha=0.08)
        ax.grid(alpha=0.2)
    axes[1].set(xlabel="Frequency (GHz); green regions are exactly line-free", ylabel="Component flux (Jy)")
    axes[1].legend()
    fig.savefig(records / "sky-spectra.png", dpi=140)
    plt.close(fig)


def verify_sky(args):
    from astropy.io import fits
    from astropy.wcs import WCS

    truth = json.loads((args.records / "sky-v2-truth.json").read_text())
    profile = truth["profiles"]
    selected = {}
    moment = np.zeros((PIXELS, PIXELS))
    radius = np.hypot(*np.meshgrid((np.arange(PIXELS) - PIXELS / 2) * CELL_ARCSEC,
                                  (np.arange(PIXELS) - PIXELS / 2) * CELL_ARCSEC))
    outside = radius >= 27.5
    max_identity_error = 0.0
    with ExitStack() as stack:
        images = [stack.enter_context(fits.open(args.outputs / name, memmap=False))
                  for name in SKY_FILES]
        for hdus in images:
            hdus.verify("exception")
            header = hdus[0].header
            assert [header[f"NAXIS{i}"] for i in range(1, 5)] == [PIXELS, PIXELS, 1, CHANNELS]
            assert header["BUNIT"] == "Jy/pixel"
            world = WCS(header).all_pix2world([[PIXELS / 2, PIXELS / 2, 0, 256]], 0)[0]
            np.testing.assert_allclose(world, [180, 34.07875, 1, REFERENCE_HZ], rtol=0, atol=1e-6)
        for channel in range(CHANNELS):
            total, continuum, line = [np.array(h[0].section[channel, 0, :, :], dtype=np.float64)
                                      for h in images]
            error = float(np.max(np.abs(total - continuum - line)))
            max_identity_error = max(max_identity_error, error)
            assert error < 1e-7
            assert np.isfinite(total).all() and total.min() >= 0
            assert np.count_nonzero(total[outside]) == 0
            if channel <= 127 or channel >= 384:
                assert np.count_nonzero(line) == 0
            for plane, key in ((total, "total_jy"), (continuum, "continuum_jy"), (line, "line_jy")):
                assert abs(float(plane.sum()) - profile[channel][key]) < 1e-6
            moment += line
            if channel in (0, 204, 234, 256, 278, 300, 511):
                selected[channel] = (continuum.copy(), line.copy(), total.copy())
    render_sky(args.records, selected, moment, profile)
    result = dict(status="passed", reader="Astropy FITS sections and WCS; all512 channels in all3 files",
                  sky_equals_continuum_plus_line_max_abs_jy_per_pixel=max_identity_error,
                  exact_line_free_channels=256, support_diameter_arcsec=55,
                  shape=[PIXELS, PIXELS, 1, CHANNELS], cell_arcsec=CELL_ARCSEC,
                  truth_bytes={name: (args.outputs / name).stat().st_size
                               for name in SKY_FILES})
    save(args.records / "sky-v2-readback.json", result)
    print(json.dumps(result), flush=True)


def first_middle_last_panel(args):
    from astropy.io import fits
    from casatools import image
    from scipy.ndimage import gaussian_filter
    os.environ["MPLCONFIGDIR"] = str(args.records / "matplotlib")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import AsinhNorm
    from matplotlib.lines import Line2D

    ia = image()
    pb_path = args.outputs / f"uniform-psf-{PIXELS}" / "image.pb"
    ia.open(str(pb_path))
    try:
        pb = ia.getchunk()
    finally:
        ia.close()
        ia.done()
    assert list(pb.shape) == [PIXELS, PIXELS, 1, 2]
    channels = [0, 256, 511]
    fwhm = 0.38
    sigma_pixels = fwhm / CELL_ARCSEC / math.sqrt(8 * math.log(2))
    gaussian_area_pixels = 2 * math.pi * sigma_pixels**2
    with fits.open(args.outputs / SKY_FILES[0], memmap=False) as hdus:
        hdus.verify("exception")
        planes = [gaussian_filter(np.array(hdus[0].section[c, 0, :, :], dtype=np.float64), sigma_pixels)
                  * gaussian_area_pixels * 1000 for c in channels]
    maximum = max(float(p.max()) for p in planes)
    norm = AsinhNorm(linear_width=1.0, vmin=0, vmax=maximum)
    coordinates = (np.arange(PIXELS) - PIXELS / 2) * CELL_ARCSEC
    extent = [coordinates[0] - CELL_ARCSEC/2, coordinates[-1] + CELL_ARCSEC/2,
              coordinates[0] - CELL_ARCSEC/2, coordinates[-1] + CELL_ARCSEC/2]
    fig, axes = plt.subplots(1, 3, figsize=(14.4, 5.7), layout="constrained", sharex=True, sharey=True)
    radii = []
    for ax, title, channel, plane in zip(axes, ("First", "Middle", "Last"), channels, planes):
        fraction = channel / 511
        power = ((1 - fraction) * pb[:, :, 0, 0] + fraction * pb[:, :, 0, 1]).T
        radius = float(np.interp(0.7, power[PIXELS//2, PIXELS//2:][::-1], coordinates[PIXELS//2:][::-1]))
        radii.append(radius)
        shown = ax.imshow(plane, origin="lower", extent=extent, cmap="magma", norm=norm, interpolation="nearest")
        ax.contour(coordinates, coordinates, power, levels=[0.7], colors=["#5de4e6"], linestyles="--", linewidths=1.4)
        ax.set(title=f"{title}: channel {channel} · {(START_HZ + channel * STEP_HZ)/1e9:.3f} GHz",
               xlabel="West offset (arcsec)", xlim=(-30.72, 30.72), ylim=(-30.72, 30.72))
        ax.set_xticks([-30, -15, 0, 15, 30])
        ax.set_yticks([-30, -15, 0, 15, 30])
        ax.text(0.04, 0.96, f"70% PB radius {radius:.1f}″", transform=ax.transAxes,
                color="#5de4e6", va="top", fontsize=10)
    axes[0].set_ylabel("North offset (arcsec)")
    axes[0].legend(handles=[Line2D([0], [0], color="#5de4e6", ls="--", label="Primary-beam power = 0.70")],
                   loc="lower left", fontsize=9, framealpha=0.8)
    colorbar = fig.colorbar(shown, ax=axes, shrink=0.82, pad=0.015,
                           label="mJy per 0.38″ display beam · shared asinh scale")
    colorbar.set_ticks([0, 1, 10, 100, 500], labels=["0", "1", "10", "100", "500"])
    fig.suptitle("C-array simulated sky: before primary-beam attenuation\n0.38″ Gaussian smoothing for display only — not reconstructed images", fontsize=13)
    fig.savefig(args.records / "first-middle-last-pb70-final.png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    result = dict(channels_zero_based=channels, model=str(args.outputs / SKY_FILES[0]),
                  primary_beam_source=str(pb_path), power_contour=0.7, radius_arcsec=radii,
                  middle_beam="linear interpolation of CASA endpoint PB power maps",
                  display_fwhm_arcsec=fwhm, display="intrinsic sky; common asinh stretch; no PB attenuation applied")
    save(args.records / "first-middle-last-pb70-final.json", result)
    print(json.dumps(result), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("geometry", "psf", "sky", "verify", "panel"))
    parser.add_argument("--records", type=Path, required=True)
    parser.add_argument("--outputs", type=Path, required=True)
    parser.add_argument("--base-request", type=Path)
    parser.add_argument("--array-config", type=Path)
    args = parser.parse_args()
    for path in (args.records, args.outputs):
        assert path.is_absolute()
        assert not any(part in ("private", "tmp", "temp") for part in path.resolve().parts)
    if args.stage == "geometry":
        assert args.base_request and args.array_config
        prepare_geometry(args)
    elif args.stage == "psf":
        measure_psf(args)
    elif args.stage == "sky":
        build_sky(args)
    elif args.stage == "verify":
        verify_sky(args)
    else:
        first_middle_last_panel(args)


if __name__ == "__main__":
    main()
