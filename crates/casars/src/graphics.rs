// SPDX-License-Identifier: LGPL-3.0-or-later
use image::DynamicImage;
use plotters::prelude::*;
use ratatui_graphics::PlottersBitmap;

use crate::config::ThemeMode;

pub(crate) const UV_PLOT_ASPECT_WIDTH: u32 = 1;
pub(crate) const UV_PLOT_ASPECT_HEIGHT: u32 = 1;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UvPlotRenderInput {
    pub coverage: casacore_ms::ListObsUvCoverage,
    pub theme_mode: ThemeMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UvPlotTheme {
    pub background: RGBColor,
    pub axis: RGBColor,
    pub label: RGBColor,
    pub grid: RGBColor,
    pub track: RGBColor,
    pub mirror: RGBColor,
}

pub(crate) fn uv_plot_theme(theme_mode: ThemeMode) -> UvPlotTheme {
    match theme_mode {
        ThemeMode::DenseAnsi => UvPlotTheme {
            background: RGBColor(16, 18, 20),
            axis: RGBColor(210, 214, 220),
            label: RGBColor(170, 176, 186),
            grid: RGBColor(64, 72, 82),
            track: RGBColor(88, 196, 221),
            mirror: RGBColor(244, 167, 89),
        },
        ThemeMode::RichPanel => UvPlotTheme {
            background: RGBColor(248, 246, 239),
            axis: RGBColor(38, 45, 55),
            label: RGBColor(87, 94, 103),
            grid: RGBColor(207, 213, 220),
            track: RGBColor(32, 111, 145),
            mirror: RGBColor(191, 96, 58),
        },
    }
}

pub(crate) fn render_uv_plot(
    width: u32,
    height: u32,
    input: &UvPlotRenderInput,
) -> Result<DynamicImage, String> {
    let theme = uv_plot_theme(input.theme_mode);
    let mut canvas = PlottersBitmap::new(width, height).map_err(|error| error.to_string())?;
    let root = canvas.backend().into_drawing_area();
    root.fill(&theme.background)
        .map_err(|error| error.to_string())?;

    let extent = input.coverage.max_abs_uv_lambda.max(1.0);
    let range = -extent..extent;

    let mut chart = ChartBuilder::on(&root)
        .margin(16)
        .x_label_area_size(42)
        .y_label_area_size(52)
        .build_cartesian_2d(range.clone(), range)
        .map_err(|error| error.to_string())?;

    chart
        .configure_mesh()
        .x_desc("u (lambda)")
        .y_desc("v (lambda)")
        .axis_desc_style(("sans-serif", 18).into_font().color(&theme.axis))
        .axis_style(theme.axis)
        .label_style(("sans-serif", 14).into_font().color(&theme.label))
        .light_line_style(theme.grid.mix(0.55))
        .bold_line_style(theme.grid)
        .draw()
        .map_err(|error| error.to_string())?;

    for track in &input.coverage.tracks {
        let points = track
            .samples
            .iter()
            .map(|sample| (sample.u_lambda, sample.v_lambda))
            .collect::<Vec<_>>();
        if points.len() >= 2 {
            chart
                .draw_series(LineSeries::new(
                    points.iter().copied(),
                    theme.track.stroke_width(2),
                ))
                .map_err(|error| error.to_string())?;
            chart
                .draw_series(LineSeries::new(
                    points.iter().map(|(u, v)| (-u, -v)),
                    theme.mirror.mix(0.65).stroke_width(1),
                ))
                .map_err(|error| error.to_string())?;
        } else if let Some(point) = points.first().copied() {
            chart
                .draw_series(PointSeries::of_element(
                    std::iter::once(point),
                    3,
                    theme.track.filled(),
                    &|coord, size, style| {
                        EmptyElement::at(coord) + Circle::new((0, 0), size, style)
                    },
                ))
                .map_err(|error| error.to_string())?;
            chart
                .draw_series(PointSeries::of_element(
                    std::iter::once((-point.0, -point.1)),
                    2,
                    theme.mirror.mix(0.65).filled(),
                    &|coord, size, style| {
                        EmptyElement::at(coord) + Circle::new((0, 0), size, style)
                    },
                ))
                .map_err(|error| error.to_string())?;
        }
    }

    root.present().map_err(|error| error.to_string())?;
    drop(chart);
    drop(root);
    canvas
        .into_dynamic_image()
        .map_err(|error| error.to_string())
}

pub(crate) fn uv_plot_summary(coverage: &casacore_ms::ListObsUvCoverage) -> String {
    format!(
        "UV coverage in lambda. Tracks={} Samples={} Mirrored=yes Max |u,v|={:.1}",
        coverage.tracks.len(),
        coverage.sample_count,
        coverage.max_abs_uv_lambda
    )
}
