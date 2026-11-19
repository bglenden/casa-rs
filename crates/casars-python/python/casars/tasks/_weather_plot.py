"""Lay out the native weather summary with the shared plot renderer."""

from .._plot_document import render_document


def render_weather(plot, workspace):
    from matplotlib import pyplot as plt
    from matplotlib.ticker import MaxNLocator

    with plt.ioff():
        figure = plt.figure(figsize=(8.5, 10))
        try:
            bounds = [(0.13, bottom, 0.8, height) for bottom, height in [
                (0.8, 0.15), (0.65, 0.15), (0.5, 0.15), (0.35, 0.15), (0.1, 0.2),
            ]]
            axes = [figure.add_axes(position) for position in bounds]
            render_document(plot["document"], figure=figure, axes=axes)
            axes[2].autoscale(axis="y")
            axes[4].autoscale(axis="x")
            axes[4].legend(loc="upper left", borderaxespad=0)
            axes[0].set_yticks([-90, -45, 0, 45, 90])
            for index, axis in enumerate(axes):
                axis.yaxis.set_label_position("right")
                axis.yaxis.label.set_rotation(270)
                axis.yaxis.label.set_position((0, 0.5))
                if index > 0:
                    axis.yaxis.set_major_locator(MaxNLocator(6 if index == 4 else 5))
                labels = axis.yaxis.get_ticklabels()
                if len(labels) > 3:
                    labels[0].set_alpha(0)
                    labels[-1].set_alpha(0)
                if index < 4:
                    tick_labels = plot["time_labels"] if index == 3 else [""] * 3
                    axis.set_xticks(plot["time_ticks"], labels=tick_labels)
            for arrow, rotation in zip(axes[1].texts, plot["wind_rotations"], strict=True):
                arrow.set_color("purple")
                arrow.set_rotation(rotation)
            axes[1].lines[0].set_markersize(2)
            figure.savefig(workspace / plot["output_file"], dpi=150)
        finally:
            plt.close(figure)
