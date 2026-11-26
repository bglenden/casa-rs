#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Prepare a continuum sky and geometric preview; never generate an MS or run CLEAN."""

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
from scipy import fft

PIXELS = 4096
CELL = 0.05
ARCSEC = math.pi / (180 * 3600)
C = 299792458.0
OMEGA = 2 * math.pi / 86164.0905


def spectral_windows():
    """Explicit simulator tuning on two 2048-MHz basebands, not an RCT export."""
    windows = []
    for baseband, center in enumerate((5e9, 7e9)):
        for subband in range(16):
            edge = center - 1024e6 + subband * 128e6
            channels = edge + (np.arange(64) + 0.5) * 2e6
            flags = (channels < 4e9) | (channels > 8e9)
            flags[:2] = True
            flags[-2:] = True
            windows.append(
                dict(
                    spw_id=len(windows),
                    baseband=baseband,
                    lower_edge_hz=edge,
                    first_center_hz=float(channels[0]),
                    channel_width_hz=2e6,
                    channels=64,
                    flagged_channels=np.flatnonzero(flags).tolist(),
                )
            )
    return windows


def sky_components():
    """Analytic components: no repeated image template or common spectrum."""
    rng = np.random.default_rng(5414096)
    components = []

    def add(name, east, north, flux, alpha, major=0.0, minor=0.0, pa=0.0):
        components.append(
            dict(
                name=name,
                east_arcsec=float(east),
                north_arcsec=float(north),
                flux_jy=float(flux),
                spectral_index=float(alpha),
                major_arcsec=float(major),
                minor_arcsec=float(minor),
                pa_degrees=float(pa),
            )
        )

    for i in range(72):
        radius = 88 * np.sqrt(rng.uniform(0.015, 1))
        angle = rng.uniform(0, 2 * np.pi)
        add(
            f"point-{i:02}",
            radius * np.cos(angle),
            radius * np.sin(angle),
            10 ** rng.uniform(-3.3, -1.15),
            rng.uniform(-1.2, 0.3),
        )
    for i, (x, y, f, a) in enumerate(
        (
            (-12, 9, 0.4, 0),
            (53, -38, 0.15, -0.8),
            (-66, -41, 0.08, 0.2),
            (74, 41, 0.06, -1.1),
        )
    ):
        add(f"control-{i}", x, y, f, a)
    centers = [
        (-45, 42),
        (5, 52),
        (49, 34),
        (-58, 0),
        (-15, 9),
        (26, 4),
        (62, -14),
        (-40, -38),
        (3, -48),
        (39, -48),
        (-4, -17),
        (-18, 73),
    ]
    for group, (x, y) in enumerate(centers):
        kind = group % 4
        if kind == 0:
            for k, t in enumerate(np.linspace(0, 2 * np.pi, 32, endpoint=False)):
                add(
                    f"ring-{group}-{k}",
                    x + 6 * np.cos(t),
                    y + 4 * np.sin(t),
                    0.012 * (1 + 0.3 * np.cos(3 * t)),
                    -0.8 + 0.15 * np.sin(t),
                    1.1,
                    0.8,
                )
        elif kind == 1:
            for k, t in enumerate(np.linspace(-1, 1, 19)):
                add(
                    f"jet-{group}-{k}",
                    x + 8 * t,
                    y + 2.2 * np.sin(2 * t),
                    0.008 + 0.012 * abs(t),
                    -0.4 - 0.65 * abs(t),
                    1.2,
                    0.65,
                    65,
                )
            add(f"jet-core-{group}", x, y, 0.09, -0.05)
        elif kind == 2:
            for k, t in enumerate(np.linspace(-1, 1, 25)):
                add(
                    f"filament-{group}-{k}",
                    x + 8 * t,
                    y + 3 * t * t,
                    0.010 * (1 + 0.35 * np.cos(5 * t)),
                    -0.9 + 0.2 * t,
                    1.2,
                    0.65,
                    70,
                )
        else:
            add(f"diffuse-{group}", x, y, 0.55, -0.65, 14, 7, 20 + group * 11)
            for k in range(5):
                add(
                    f"knot-{group}-{k}",
                    x + rng.uniform(-5, 5),
                    y + rng.uniform(-2, 2),
                    0.035,
                    -0.2 - 0.17 * k,
                    0.4 + 0.25 * k,
                    0.3,
                    group * 19,
                )
    return components


def render(components, frequency_hz, pixels=PIXELS, cell=CELL, beam_fwhm=0.0):
    """Sample compact Gaussian patches; points are bilinear pixel integrals."""
    result = np.zeros((pixels, pixels), dtype=np.float32)
    for comp in components:
        x = -comp["east_arcsec"] / cell + pixels / 2
        y = comp["north_arcsec"] / cell + pixels / 2
        flux = comp["flux_jy"] * (frequency_hz / 6e9) ** comp["spectral_index"]
        major = math.hypot(comp["major_arcsec"], beam_fwhm)
        minor = math.hypot(comp["minor_arcsec"], beam_fwhm)
        if major == 0:
            ix, iy = math.floor(x), math.floor(y)
            for dx in (0, 1):
                for dy in (0, 1):
                    if 0 <= ix + dx < pixels and 0 <= iy + dy < pixels:
                        wx = x - ix if dx else 1 - (x - ix)
                        wy = y - iy if dy else 1 - (y - iy)
                        result[iy + dy, ix + dx] += flux * wx * wy
            continue
        radius = 3 * major / cell
        x0, x1 = max(0, math.floor(x - radius)), min(pixels, math.ceil(x + radius) + 1)
        y0, y1 = max(0, math.floor(y - radius)), min(pixels, math.ceil(y + radius) + 1)
        east = -(np.arange(x0, x1, dtype=np.float32)[None, :] - x) * cell
        north = (np.arange(y0, y1, dtype=np.float32)[:, None] - y) * cell
        angle = math.radians(comp["pa_degrees"])
        along = east * math.sin(angle) + north * math.cos(angle)
        across = east * math.cos(angle) - north * math.sin(angle)
        patch = np.exp(-4 * np.log(2) * ((along / major) ** 2 + (across / minor) ** 2))
        # The analytic Gaussian integral is flux; do not renormalize edge clipping.
        patch *= flux * 4 * np.log(2) * cell**2 / (np.pi * major * minor)
        result[y0:y1, x0:x1] += patch
    return result


def uvw_geometry(config_path, configurations, integrations_per_configuration=360):
    """Sample a six-hour track uniformly, keeping every exposure at two seconds."""
    if integrations_per_configuration < 2:
        raise ValueError("at least two integrations are needed to span the track")
    hour_angles = np.linspace(-3, 3, integrations_per_configuration)
    local_h = hour_angles * math.pi / 12
    uvws, summaries = [], []
    for configuration in configurations:
        path = config_path / f"vla.{configuration.lower()}.cfg"
        xyz = np.loadtxt(path, comments="#", usecols=(0, 1, 2))
        assert xyz.shape == (27, 3)
        i, j = np.triu_indices(27, 1)
        b = xyz[j] - xyz[i]
        longitude = math.atan2(xyz[:, 1].mean(), xyz[:, 0].mean())
        h = local_h[:, None] - longitude
        dec = math.radians(30)
        u = np.sin(h) * b[:, 0] + np.cos(h) * b[:, 1]
        v = (
            -math.sin(dec) * np.cos(h) * b[:, 0]
            + math.sin(dec) * np.sin(h) * b[:, 1]
            + math.cos(dec) * b[:, 2]
        )
        w = (
            math.cos(dec) * np.cos(h) * b[:, 0]
            - math.cos(dec) * np.sin(h) * b[:, 1]
            + math.sin(dec) * b[:, 2]
        )
        uvw = np.stack((u, v, w), axis=-1).reshape(-1, 3)
        uvws.append(uvw)
        summaries.append(
            dict(
                configuration=configuration,
                config_path=str(path),
                sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                physical_baseline_min_max_m=[
                    float(np.linalg.norm(b, axis=1).min()),
                    float(np.linalg.norm(b, axis=1).max()),
                ],
                baseline_times=len(uvw),
                time_samples=len(local_h),
                hour_angle_centers_hours=hour_angles.tolist(),
                integration_seconds=2,
                cadence_seconds=float((local_h[1] - local_h[0]) / OMEGA),
            )
        )
    return uvws, summaries


def density_psf(uvws, frequencies, uniform=False):
    """Planning-only nearest-cell Fourier PSF, not a CASA gridding/beam result."""
    density = np.zeros((PIXELS, PIXELS), dtype=np.float32)
    step = 1 / (PIXELS * CELL * ARCSEC)
    for uvw in uvws:
        for frequency in frequencies:
            uv = np.rint(uvw[:, :2] * (frequency / C / step)).astype(np.int32)
            if np.any(np.abs(uv) >= PIXELS // 2):
                raise ValueError("UV samples exceed the image Nyquist limit")
            for sign in (-1, 1):
                xy = (sign * uv + PIXELS // 2) % PIXELS
                np.add.at(density, (xy[:, 1], xy[:, 0]), 1)
    if uniform:
        density = (density > 0).astype(np.float32)
    psf = fft.fftshift(fft.ifft2(fft.ifftshift(density), workers=1).real)
    psf /= psf[PIXELS // 2, PIXELS // 2]
    return psf


def half_width(row):
    center = len(row) // 2
    right = row[center:]
    k = int(np.flatnonzero(right < 0.5)[0])
    return float(2 * (k - 1 + (right[k - 1] - 0.5) / (right[k - 1] - right[k])) * CELL)


def save(path, data):
    encoded = json.dumps(data, indent=2)
    with path.open("x") as handle:
        handle.write(encoded + "\n")


def preview(output, config_path, configurations=("A", "C"), integrations=360):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import AsinhNorm

    output.mkdir(parents=True, exist_ok=False)
    components, windows = sky_components(), spectral_windows()
    save(
        output / "sky-components.json",
        dict(reference_frequency_hz=6e9, components=components),
    )
    uvws, geometry = uvw_geometry(config_path, configurations, integrations)
    used_freq = np.concatenate(
        [
            w["first_center_hz"]
            + 2e6 * np.array([k for k in range(64) if k not in w["flagged_channels"]])
            for w in windows
        ]
    )
    baseline_times = sum(len(u) for u in uvws)
    rows = baseline_times * 32
    samples = rows * 64 * 4
    projected = np.concatenate(uvws)
    extent = (-(PIXELS / 2 + 0.5) * CELL, (PIXELS / 2 - 0.5) * CELL) * 2
    fig, axes = plt.subplots(1, 3, figsize=(17, 6), constrained_layout=True)
    fluxes = []
    for ax, frequency in zip(axes, (4e9, 6e9, 8e9)):
        plane = render(components, frequency, beam_fwhm=0.35)
        beam_area = np.pi * 0.35**2 / (4 * np.log(2) * CELL**2)
        shown = plane * beam_area * 1000
        im = ax.imshow(
            shown,
            origin="lower",
            extent=extent,
            cmap="magma",
            norm=AsinhNorm(linear_width=0.02, vmin=0, vmax=400),
        )
        ax.set(
            title=f"{frequency / 1e9:.0f} GHz intrinsic sky",
            xlabel="West offset (arcsec)",
            ylabel="North offset (arcsec)",
        )
        ax.add_patch(plt.Circle((0, 0), 90, fill=False, ls=":", color="cyan", lw=0.7))
        pb70 = (42 / 8 * 60) * np.sqrt(-np.log(0.7) / (4 * np.log(2)))
        ax.add_patch(
            plt.Circle((0, 0), pb70, fill=False, ls="--", color="white", lw=0.7)
        )
        fluxes.append(
            dict(
                frequency_hz=frequency,
                integrated_jy=float(plane.sum()),
                analytic_integrated_jy=sum(
                    c["flux_jy"] * (frequency / 6e9) ** c["spectral_index"]
                    for c in components
                ),
            )
        )
    fig.colorbar(im, ax=axes, label="mJy / 0.35-arcsec display beam")
    fig.suptitle(
        "Proposed continuum truth — 4096², 0.05 arcsec/pixel\nDisplay-smoothed, no PB applied; cyan: 90″ radius; white dashed: approximate 8-GHz PB 70%"
    )
    fig.savefig(output / "sky-preview.png", dpi=150)
    plt.close(fig)

    fig, axes = plt.subplots(2, 2, figsize=(12, 10), constrained_layout=True)
    for index, (configuration, uvw) in enumerate(zip(configurations, uvws)):
        axes[0, 0].scatter(
            uvw[::10, 0] * 6e9 / C / 1000,
            uvw[::10, 1] * 6e9 / C / 1000,
            s=0.15,
            alpha=0.4,
            label=configuration,
            color=f"C{index}",
        )
        axes[0, 0].scatter(
            -uvw[::10, 0] * 6e9 / C / 1000,
            -uvw[::10, 1] * 6e9 / C / 1000,
            s=0.15,
            alpha=0.4,
            color=f"C{index}",
        )
    axes[0, 0].set(
        title="6-GHz UV coverage (1/10 shown)", xlabel="u (kλ)", ylabel="v (kλ)"
    )
    axes[0, 0].legend(markerscale=10)
    axes[0, 0].set_aspect("equal")
    for configuration, uvw in zip(configurations, uvws):
        axes[0, 1].hist(
            np.hypot(uvw[:, 0], uvw[:, 1]),
            bins=np.geomspace(10, 40000, 60),
            histtype="step",
            label=configuration,
        )
    axes[0, 1].set(
        xscale="log",
        title="Projected baseline distribution",
        xlabel="Metres",
        ylabel="Baseline-time samples",
    )
    axes[0, 1].legend()
    psf_results = []
    for ax, uniform in zip(axes[1], (False, True)):
        psf = density_psf(
            uvws, [w["first_center_hz"] + 31.5 * 2e6 for w in windows], uniform
        )
        center = PIXELS // 2
        cut = psf[center - 100 : center + 101, center - 100 : center + 101]
        ax.imshow(
            cut[:, ::-1],
            origin="lower",
            extent=(-5.025, 5.025, -5.025, 5.025),
            vmin=-0.15,
            vmax=1,
            cmap="RdBu_r",
        )
        label = "uniform-density" if uniform else "natural-density"
        ax.set(
            title=f"{label} geometric PSF",
            xlabel="West offset (arcsec)",
            ylabel="North offset (arcsec)",
        )
        psf_results.append(
            dict(
                weighting=label,
                axis_cut_fwhm_arcsec=[
                    half_width(psf[center]),
                    half_width(psf[:, center]),
                ],
                approximation="nearest UV cell, SPW centers only; no w correction or CASA convolution; not fitted restoring beam",
            )
        )
    fig.suptitle(
        f"{' + '.join(configurations)} planning diagnostic — {integrations} separate 2-s integrations each, evenly across HA ±3h"
    )
    fig.savefig(output / "uv-psf-preview.png", dpi=150)
    plt.close(fig)

    radius_rad = 90 * ARCSEC
    bmax = max(g["physical_baseline_min_max_m"][1] for g in geometry)
    delay_bound = bmax * radius_rad / C
    frequency_smearing_bound = 1 - float(np.sinc(2e6 * delay_bound))
    time_smearing_bound = 1 - float(np.sinc(2 * OMEGA * 8e9 * delay_bound))
    w_phase = (
        2
        * np.pi
        * np.abs(projected[:, 2]).max()
        * 8e9
        / C
        * (1 - np.sqrt(1 - radius_rad**2))
    )
    alphas = np.array([c["spectral_index"] for c in components])
    x = (used_freq - 6e9) / 6e9
    spectra = (used_freq[:, None] / 6e9) ** alphas[None, :]
    errors = {}
    for terms in (2, 3, 4):
        design = np.vander(x, terms, increasing=True)
        coefficients = np.linalg.lstsq(design, spectra, rcond=None)[0]
        relative = (design @ coefficients - spectra) / spectra
        errors[str(terms)] = dict(
            max_relative_error=float(np.abs(relative).max()),
            rms_relative_error=float(np.sqrt(np.mean(relative**2))),
        )
    summary = dict(
        status="preview_only_not_generated_ms",
        image_pixels=PIXELS,
        cell_arcsec=CELL,
        field_arcsec=PIXELS * CELL,
        phase_center_j2000_deg=[180, 30],
        configurations=geometry,
        integrations_per_configuration=integrations,
        hour_angle_span_hours=[-3, 3],
        integration_seconds=2,
        on_source_seconds=len(configurations) * integrations * 2,
        sampling="uniformly time-sampled six-hour tracks; no averaging across the gaps",
        thermal_noise=False,
        sensitivity_caveat="Noise-free algorithmic fixture, not a prediction of real 24-minute observing sensitivity; retain finite equal weights, not infinite inverse-noise weights",
        spectral_reference="LSRK, preserving the approved simulator convention; explicit synthetic tuning, not a historical raw TOPO observation or a relabelled MS",
        frequency_tuning_status="explicit 5/7-GHz baseband design derived from NRAO nominal defaults; not an exported RCT resource",
        spectral_windows=windows,
        total_channels=2048,
        unflagged_channels_including_overlap=len(used_freq),
        unique_unflagged_channel_centers=len(np.unique(used_freq)),
        correlations=["RR", "RL", "LR", "LL"],
        ms_rows=rows,
        complex_samples_stored=samples,
        unflagged_complex_samples=baseline_times * len(used_freq) * 4,
        unflagged_parallel_hand_samples_for_stokes_i=baseline_times
        * len(used_freq)
        * 2,
        data_bytes=samples * 8,
        flag_uncompressed_bytes=samples,
        approximate_fixed_metadata_bytes=rows * 160,
        minimal_column_payload_estimate_bytes=samples * 9 + rows * 160,
        storage_caveat="No MODEL_DATA or CORRECTED_DATA; excludes table/tile overhead, flags may be packed; not measured on disk",
        point_sources=sum(c["major_arcsec"] == 0 for c in components),
        extended_complexes=12,
        analytic_components=len(components),
        fluxes=fluxes,
        geometric_psfs=psf_results,
        max_point_radius_arcsec=max(
            math.hypot(c["east_arcsec"], c["north_arcsec"])
            for c in components
            if c["major_arcsec"] == 0
        ),
        pb_8ghz_gaussian_fwhm_arcsec=42 / 8 * 60,
        pb_at_radius90_8ghz_gaussian_approx=float(
            np.exp(-4 * np.log(2) * (90 / (42 / 8 * 60)) ** 2)
        ),
        bounds_at_radius90=dict(
            bandwidth_peak_loss_fraction=frequency_smearing_bound,
            time_peak_loss_fraction=time_smearing_bound,
            max_sampled_ignored_w_phase_rad=float(w_phase),
            caveat="Conservative rectangular-bin/time bounds, not simulated averaging; use exact beam and integration in MS generation",
        ),
        intrinsic_powerlaw_taylor_fit_errors=errors,
        simulation_gaps=[
            "native simulator has one SPW per request and fixed LSRK setup",
            "multi-SPW, multi-configuration scan assembly needs a fixture driver; no production API changes authorized",
            "chromatic PB and w term must be retained; geometric diagnostic is not application science acceptance",
            "a CASA geometry-only PSF and generation pilot remain before bulk MS creation",
        ],
    )
    save(output / "recipe-and-checks.json", summary)
    print(
        json.dumps(
            {
                k: summary[k]
                for k in (
                    "status",
                    "ms_rows",
                    "complex_samples_stored",
                    "minimal_column_payload_estimate_bytes",
                    "point_sources",
                    "extended_complexes",
                    "geometric_psfs",
                    "bounds_at_radius90",
                    "intrinsic_powerlaw_taylor_fit_errors",
                )
            },
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--array-root", required=True, type=Path)
    parser.add_argument("--configurations", nargs="+", default=["A", "C"])
    parser.add_argument("--integrations-per-configuration", type=int, default=360)
    args = parser.parse_args()
    preview(
        args.output,
        args.array_root,
        args.configurations,
        args.integrations_per_configuration,
    )
