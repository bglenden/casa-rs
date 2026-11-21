"""Render a prepared predictcomp visibility plot."""

from pathlib import Path


def render_prediction(plot, workspace):
    from matplotlib import pyplot as plt
    from matplotlib.ticker import ScalarFormatter

    with plt.ioff():
        figure, axis = plt.subplots(layout="constrained")
        shown = False
        try:
            for frequency, amps in zip(plot["frequencies_ghz"], plot["amplitudes_jy"], strict=True):
                axis.plot(plot["x"], amps, plot["symbol"], label=f"{frequency:.3g} GHz")
            axis.set_xlabel(f"Baseline length ({plot['axis_unit']})")
            axis.set_ylabel("Visibility amplitude (Jy)")
            axis.yaxis.set_major_formatter(ScalarFormatter(useOffset=False))
            if plot["amplitude_from_zero"]:
                axis.set_ylim(bottom=0)
            if plot["baseline_from_zero"]:
                axis.set_xlim(left=0)

            az = plot["azimuth_deg"]
            el = plot["elevation_deg"]
            title = f"{az:.0f}° az, {el:.0f}° el"
            flux = plot["zero_baseline_flux"]
            if flux is not None:
                title += f"\nZero-baseline flux {flux:.3f} Jy"
                if len(plot["frequencies_ghz"]) > 1:
                    title += f" at {plot['frequencies_ghz'][0]:.3g} GHz"
            axis.legend(title=title)
            axis.set_title(f"{plot['epoch']} · {plot['antennas']}")
            figure.suptitle(f"{plot['object']} ({plot['standard']})")

            if plot["save"]:
                output = workspace / Path(plot["save"])
                figure.savefig(output)
            if plot["show"]:
                plt.show(block=False)
                shown = True
        finally:
            if not shown:
                plt.close(figure)
