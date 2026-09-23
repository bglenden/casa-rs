#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Three-channel CASA reconstruction of the C-array diagnostic simulation.

Run each stage under the existing sampled RSS guard. This is a sky-recovery
diagnostic, not a native/CASA parity or full-cube acceptance gate.
"""

import argparse
import json
import os
from pathlib import Path
import time

import numpy as np

from stage_t55_c_array import CELL_ARCSEC, PIXELS, SKY_FILES, START_HZ, STEP_HZ, save

CHANNELS = (0, 256, 511)


def image_channels(args):
    from casatasks import casalog, tclean, version_string
    from casatools import image
    from t55_full_size_validation import preflight

    preflight(argparse.Namespace(input=args.outputs / "turnaround.ms", rows=168480,
                                records=args.records, label="turnaround"))
    output = args.outputs / "turnaround-casa"
    output.mkdir()
    os.chdir(output)
    casalog.setlogfile(str(output / "casa.log"))
    results = []
    for channel in CHANNELS:
        prefix = output / f"ch{channel:03}"
        kwargs = dict(
            vis=str(args.outputs / "turnaround.ms"), imagename=str(prefix),
            datacolumn="data", field="0", spw=f"0:{channel}",
            imsize=PIXELS, cell=f"{CELL_ARCSEC}arcsec", stokes="I",
            specmode="cube", outframe="LSRK", nchan=1,
            start=f"{START_HZ + channel * STEP_HZ:.0f}Hz", width=f"{STEP_HZ:.0f}Hz",
            interpolation="nearest", gridder="standard", weighting="uniform",
            perchanweightdensity=True, deconvolver="multiscale", scales=[0, 6, 24, 72],
            niter=20000, cycleniter=1000, gain=0.1, threshold="0.0005Jy",
            usemask="user", mask="circle[[512pix,512pix],27.5arcsec]",
            interactive=False, calcres=True, calcpsf=True, restoration=True,
            pbcor=False, savemodel="none", parallel=False, pblimit=-0.2,
            restart=False, fullsummary=True)
        save(args.records / f"turnaround-ch{channel:03}-casa-request.json", kwargs)
        print(f"stage=CASA channel={channel} frequency_hz={START_HZ + channel * STEP_HZ}", flush=True)
        started = time.perf_counter()
        returned = tclean(**kwargs)
        seconds = time.perf_counter() - started
        ia = image()
        ia.open(str(prefix) + ".image")
        try:
            cs = ia.coordsys()
            try:
                reference = cs.referencevalue()["numeric"]
                pixels = cs.referencepixel()["numeric"]
                increments = cs.increment()["numeric"]
            finally:
                cs.done()
            assert list(ia.shape()) == [PIXELS, PIXELS, 1, 1]
            assert abs(reference[3] - (START_HZ + channel * STEP_HZ)) < 1
            np.testing.assert_allclose(pixels[:2], [512, 512], atol=1e-7)
            np.testing.assert_allclose(reference[:2], [np.pi, np.deg2rad(34.07875)], atol=1e-10)
            np.testing.assert_allclose(increments[:2], np.deg2rad([-CELL_ARCSEC/3600, CELL_ARCSEC/3600]))
            result = dict(channel=channel, frequency_hz=float(reference[3]), seconds=seconds,
                          casa_version=version_string(), beam=ia.restoringbeam(),
                          iterations=int(returned["iterdone"]), majors=int(returned["nmajordone"]),
                          stopcode=int(returned["stopcode"]), prefix=str(prefix))
        finally:
            ia.done()
        save(args.records / f"turnaround-ch{channel:03}-casa-result.json", result)
        results.append(result)
        print(json.dumps(result), flush=True)
    save(args.records / "turnaround-casa-results.json", results)


def read_image(path):
    from casatools import image
    ia = image()
    ia.open(str(path))
    try:
        values = ia.getchunk()
        mask = ia.getchunk(getmask=True)
        cs = ia.coordsys()
        try:
            coordinates = cs.torecord()
        finally:
            cs.done()
        return values, mask, coordinates
    finally:
        ia.done()


def compare(args):
    os.chdir(args.outputs / "turnaround-casa")
    from astropy.io import fits
    from casatasks import casalog
    from casatools import image

    casalog.setlogfile(str(args.outputs / "turnaround-casa/comparison-casa.log"))

    results = json.loads((args.records / "turnaround-casa-results.json").read_text())
    summaries = []
    with fits.open(args.outputs / SKY_FILES[0], memmap=False) as hdus:
        hdus.verify("exception")
        for result in results:
            channel = result["channel"]
            prefix = Path(result["prefix"])
            reconstructed, valid, coordinates = read_image(str(prefix) + ".image")
            pb, pb_valid, _ = read_image(str(prefix) + ".pb")
            residual, _, _ = read_image(str(prefix) + ".residual")
            intrinsic = np.asarray(hdus[0].section[channel, 0, :, :], dtype=np.float32).T[:, :, None, None]
            assert reconstructed.shape == intrinsic.shape == pb.shape
            # Attenuate before convolution: the CLEAN image is the apparent sky.
            apparent = intrinsic * pb
            ia = image()
            ia.fromarray(outfile=str(prefix) + ".truth-apparent-v3", pixels=apparent,
                         csys=coordinates, overwrite=False)
            try:
                ia.setbrightnessunit("Jy/pixel")
                beam = result["beam"]
                convolved = ia.convolve2d(outfile=str(prefix) + ".truth-restored-v3",
                                         major=beam["major"], minor=beam["minor"],
                                         pa=beam["positionangle"])
                try:
                    assert convolved.brightnessunit() == "Jy/beam"
                    truth = convolved.getchunk()
                finally:
                    convolved.done()
            finally:
                ia.done()
            assert np.isfinite(reconstructed).all() and np.isfinite(truth).all()
            valid = valid & pb_valid
            difference = reconstructed - truth
            support = np.hypot(*np.meshgrid((np.arange(PIXELS)-512)*CELL_ARCSEC,
                                           (np.arange(PIXELS)-512)*CELL_ARCSEC)) <= 27.5
            interior = support[:, :, None, None]
            assert valid[interior].all(), "All simulated source support must be covered"
            offsource = ~interior & valid
            beam = result["beam"]
            assert beam["major"]["unit"] == beam["minor"]["unit"] == "arcsec"
            area = np.pi * beam["major"]["value"] * beam["minor"]["value"] / (4*np.log(2)*CELL_ARCSEC**2)
            summary = dict(result, units="Jy/beam", comparison="apparent sky times CASA PB, then CASA restoring-beam convolution",
                           valid_comparison_pixels=int(valid.sum()), source_support_fully_valid=True,
                           truth_peak_jy_beam=float(truth.max()), image_peak_jy_beam=float(reconstructed.max()),
                           difference_rms_jy_beam=float(np.sqrt(np.mean(difference[interior]**2))),
                           difference_max_abs_jy_beam=float(np.max(np.abs(difference))),
                           normalized_rms=float(np.linalg.norm(difference[interior]) / np.linalg.norm(truth[interior])),
                           residual_offsource_rms_jy_beam=float(np.sqrt(np.mean(residual[offsource]**2))),
                           residual_max_abs_jy_beam=float(np.max(np.abs(residual))),
                           apparent_input_flux_jy=float(apparent.sum()),
                           recovered_aperture_flux_jy=float(reconstructed[interior].sum()/area),
                           beam_area_pixels=float(area))
            # Small selected-plane arrays only; no full cube or MS materialization.
            np.savez_compressed(args.records / f"turnaround-ch{channel:03}-comparison.npz",
                                truth=truth[:, :, 0, 0].T, image=reconstructed[:, :, 0, 0].T,
                                difference=difference[:, :, 0, 0].T,
                                residual=residual[:, :, 0, 0].T, pb=pb[:, :, 0, 0].T,
                                valid=valid[:, :, 0, 0].T)
            summaries.append(summary)
            print(json.dumps(summary), flush=True)
    save(args.records / "turnaround-comparison.json", summaries)


def panels(args):
    os.environ["MPLCONFIGDIR"] = str(args.records / "matplotlib")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import AsinhNorm
    from matplotlib.patches import Ellipse

    summaries = json.loads((args.records / "turnaround-comparison.json").read_text())
    data = [dict(np.load(args.records / f"turnaround-ch{c:03}-comparison.npz")) for c in CHANNELS]
    maximum = max(float(d[k].max()) for d in data for k in ("truth", "image")) * 1000
    delta = max(float(np.max(np.abs(d["difference"]))) for d in data) * 1000
    signal_norm = AsinhNorm(linear_width=0.3, vmin=-0.5, vmax=maximum)
    difference_norm = AsinhNorm(linear_width=0.15, vmin=-delta, vmax=delta)
    coordinates = (np.arange(PIXELS) - 512) * CELL_ARCSEC
    extent = [coordinates[0] - CELL_ARCSEC/2, coordinates[-1] + CELL_ARCSEC/2] * 2
    output_paths = []
    for label, summary, values in zip(("First", "Middle", "Last"), summaries, data):
        fig, axes = plt.subplots(1, 3, figsize=(15, 5.8), layout="constrained", sharex=True, sharey=True)
        for ax, key, title in zip(axes, ("truth", "image", "difference"),
                                 ("Input sky × PB, beam-matched", "CASA multiscale CLEAN", "CASA − beam-matched input")):
            shown = ax.imshow(np.where(values["valid"], values[key] * 1000, np.nan), origin="lower", extent=extent,
                              cmap="RdBu_r" if key == "difference" else "magma",
                              norm=difference_norm if key == "difference" else signal_norm)
            ax.contour(coordinates, coordinates, values["pb"], levels=[0.7],
                       colors=["#22d3ee"], linestyles="--", linewidths=1)
            beam = summary["beam"]
            ax.add_patch(Ellipse((-26, -26), width=beam["minor"]["value"],
                                 height=beam["major"]["value"], angle=beam["positionangle"]["value"],
                                 facecolor="#22d3ee", edgecolor="none"))
            ax.set(title=title, xlabel="West offset (arcsec)", xlim=(-30.72, 30.72), ylim=(-30.72, 30.72))
            ax.set_xticks([-30, -15, 0, 15, 30])
            ax.set_yticks([-30, -15, 0, 15, 30])
            if key == "image":
                signal = shown
        axes[0].set_ylabel("North offset (arcsec)")
        colorbar = fig.colorbar(signal, ax=axes[:2], orientation="horizontal", fraction=0.07,
                               pad=0.08, label="mJy/beam · identical signal scale for all planes")
        colorbar.set_ticks([0, 1, 10, 100, 500], labels=["0", "1", "10", "100", "500"])
        difference_bar = fig.colorbar(shown, ax=axes[2], orientation="horizontal", fraction=0.07, pad=0.08,
                                     label="mJy/beam · signed difference, separate shared scale")
        difference_bar.set_ticks([-1, -0.3, 0, 0.3, 1], labels=["−1", "−0.3", "0", "0.3", "1"])
        fig.suptitle(f"{label}: channel {summary['channel']} · {summary['frequency_hz']/1e9:.3f} GHz\n"
                     f"168,480 rows / 480 time samples · uniform weighting · dashed: 70% primary beam", fontsize=13)
        filename = args.records / f"turnaround-{label.lower()}-comparison-final.png"
        fig.savefig(filename, dpi=150, bbox_inches="tight")
        plt.close(fig)
        output_paths.append(str(filename))
    residual_limit = max(float(np.max(np.abs(d["residual"]))) for d in data) * 1000
    fig, axes = plt.subplots(1, 3, figsize=(14, 5.3), layout="constrained", sharex=True, sharey=True)
    for ax, channel, values in zip(axes, CHANNELS, data):
        shown = ax.imshow(values["residual"] * 1000, origin="lower", extent=extent,
                          cmap="RdBu_r", norm=AsinhNorm(linear_width=0.1,
                          vmin=-residual_limit, vmax=residual_limit))
        ax.contour(coordinates, coordinates, values["pb"], levels=[0.7],
                   colors=["#22d3ee"], linestyles="--", linewidths=1)
        ax.set(title=f"Channel {channel}", xlabel="West offset (arcsec)")
        ax.set_xticks([-30, -15, 0, 15, 30])
        ax.set_yticks([-30, -15, 0, 15, 30])
    axes[0].set_ylabel("North offset (arcsec)")
    residual_bar = fig.colorbar(shown, ax=axes, shrink=0.8, label="Residual mJy/dirty beam · shared signed scale")
    residual_bar.set_ticks([-0.5, -0.2, 0, 0.2, 0.5], labels=["−0.5", "−0.2", "0", "0.2", "0.5"])
    fig.suptitle("CASA residuals after CLEAN — not the sky-comparison differences", fontsize=13)
    residual_path = args.records / "turnaround-clean-residuals-final.png"
    fig.savefig(residual_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    output_paths.append(str(residual_path))
    save(args.records / "turnaround-panels-final.json", dict(paths=output_paths,
         signal_range_mjy_beam=[-0.5, maximum], difference_range_mjy_beam=[-delta, delta],
         conventions="West right, north up; apparent sky; no alignment fit or flux rescaling"))
    print(json.dumps(dict(panels=output_paths)), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("image", "compare", "panels"))
    parser.add_argument("--records", type=Path, required=True)
    parser.add_argument("--outputs", type=Path, required=True)
    args = parser.parse_args()
    for path in (args.records, args.outputs):
        assert path.is_absolute() and path.is_dir()
        assert not any(part in ("private", "tmp", "temp") for part in path.resolve().parts)
    {"image": image_channels, "compare": compare, "panels": panels}[args.stage](args)


if __name__ == "__main__":
    main()
