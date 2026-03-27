// SPDX-License-Identifier: LGPL-3.0-or-later
use image::DynamicImage;
use plotters::prelude::*;
use ratatui_graphics::PlottersBitmap;

use crate::config::ThemeMode;

#[derive(Debug, Clone, Copy, PartialEq)]
struct UvAxisScale {
    unit_label: &'static str,
    lambda_scale: f64,
}

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

    let axis_scale = uv_axis_scale(input.coverage.max_abs_uv_lambda);
    let extent = (input.coverage.max_abs_uv_lambda / axis_scale.lambda_scale).max(1.0);
    let range = -extent..extent;
    let min_dimension = width.min(height).max(1);
    let base_margin = ((min_dimension as f32) * 0.03).round() as u32;
    let x_label_area = ((height as f32) * 0.12).round() as u32;
    let y_label_area = ((width as f32) * 0.14).round() as u32;
    let axis_font_size = ((min_dimension as f32) * 0.045).round() as i32;
    let tick_font_size = ((min_dimension as f32) * 0.032).round() as i32;
    let base_margin = base_margin.clamp(8, 18);
    let x_label_area = x_label_area.clamp(28, 56);
    let y_label_area = y_label_area.clamp(42, 82);
    let axis_font_size = axis_font_size.clamp(24, 40);
    let tick_font_size = tick_font_size.clamp(18, 28);
    let plot_width_budget = width.saturating_sub(y_label_area + base_margin.saturating_mul(2));
    let plot_height_budget = height.saturating_sub(x_label_area + base_margin.saturating_mul(2));
    let plot_side = plot_width_budget.min(plot_height_budget).max(1);
    let extra_width = plot_width_budget.saturating_sub(plot_side);
    let extra_height = plot_height_budget.saturating_sub(plot_side);
    let margin_left = base_margin + extra_width / 2;
    let margin_right = base_margin + extra_width.saturating_sub(extra_width / 2);
    let margin_top = base_margin + extra_height / 2;
    let margin_bottom = base_margin + extra_height.saturating_sub(extra_height / 2);

    let mut chart = ChartBuilder::on(&root)
        .margin_left(margin_left)
        .margin_right(margin_right)
        .margin_top(margin_top)
        .margin_bottom(margin_bottom)
        .x_label_area_size(x_label_area)
        .y_label_area_size(y_label_area)
        .build_cartesian_2d(range.clone(), range)
        .map_err(|error| error.to_string())?;

    chart
        .configure_mesh()
        .x_desc(format!("u ({})", axis_scale.unit_label))
        .y_desc(format!("v ({})", axis_scale.unit_label))
        .axis_desc_style(
            ("sans-serif", axis_font_size)
                .into_font()
                .color(&theme.axis),
        )
        .axis_style(theme.axis)
        .label_style(
            ("sans-serif", tick_font_size)
                .into_font()
                .color(&theme.label),
        )
        .light_line_style(theme.grid.mix(0.55))
        .bold_line_style(theme.grid)
        .draw()
        .map_err(|error| error.to_string())?;

    for track in &input.coverage.tracks {
        let points = track
            .samples
            .iter()
            .map(|sample| {
                (
                    sample.u_lambda / axis_scale.lambda_scale,
                    sample.v_lambda / axis_scale.lambda_scale,
                )
            })
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
    let axis_scale = uv_axis_scale(coverage.max_abs_uv_lambda);
    format!(
        "UV coverage in {}. Tracks={} Samples={} Mirrored=yes Max |u,v|={:.1}",
        axis_scale.unit_label,
        coverage.tracks.len(),
        coverage.sample_count,
        coverage.max_abs_uv_lambda / axis_scale.lambda_scale
    )
}

fn uv_axis_scale(max_abs_uv_lambda: f64) -> UvAxisScale {
    if max_abs_uv_lambda >= 1_000_000.0 {
        UvAxisScale {
            unit_label: "Mλ",
            lambda_scale: 1_000_000.0,
        }
    } else if max_abs_uv_lambda >= 1_000.0 {
        UvAxisScale {
            unit_label: "kλ",
            lambda_scale: 1_000.0,
        }
    } else {
        UvAxisScale {
            unit_label: "λ",
            lambda_scale: 1.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{render_uv_plot, uv_axis_scale, uv_plot_summary, uv_plot_theme};
    use crate::config::ThemeMode;
    use casacore_ms::{ListObsOptions, ListObsUvCoverage, ListObsUvPoint, ListObsUvTrack};

    fn sample_uv_coverage() -> ListObsUvCoverage {
        ListObsUvCoverage {
            schema_version: 1,
            options: ListObsOptions::default(),
            measurement_set_path: Some("/tmp/demo.ms".to_string()),
            axis_unit: "lambda".to_string(),
            mirrored_display: true,
            sample_count: 3,
            max_abs_uv_lambda: 1_250_000.0,
            tracks: vec![
                ListObsUvTrack {
                    antenna1: 0,
                    antenna2: 1,
                    field_id: 0,
                    spectral_window_id: 0,
                    center_frequency_hz: 1.4e9,
                    samples: vec![
                        ListObsUvPoint {
                            row: 0,
                            time_mjd_seconds: 0.0,
                            u_lambda: -800_000.0,
                            v_lambda: 250_000.0,
                            w_lambda: 0.0,
                        },
                        ListObsUvPoint {
                            row: 1,
                            time_mjd_seconds: 10.0,
                            u_lambda: 900_000.0,
                            v_lambda: -400_000.0,
                            w_lambda: 0.0,
                        },
                    ],
                },
                ListObsUvTrack {
                    antenna1: 2,
                    antenna2: 3,
                    field_id: 1,
                    spectral_window_id: 0,
                    center_frequency_hz: 1.4e9,
                    samples: vec![ListObsUvPoint {
                        row: 2,
                        time_mjd_seconds: 20.0,
                        u_lambda: 150_000.0,
                        v_lambda: 600_000.0,
                        w_lambda: 0.0,
                    }],
                },
            ],
        }
    }

    #[test]
    fn uv_axis_scale_uses_lambda_for_small_extents() {
        assert_eq!(
            uv_axis_scale(875.0),
            super::UvAxisScale {
                unit_label: "λ",
                lambda_scale: 1.0,
            }
        );
    }

    #[test]
    fn uv_axis_scale_uses_klambda_for_thousands() {
        assert_eq!(
            uv_axis_scale(12_345.0),
            super::UvAxisScale {
                unit_label: "kλ",
                lambda_scale: 1_000.0,
            }
        );
    }

    #[test]
    fn uv_axis_scale_uses_mlambda_for_millions() {
        assert_eq!(
            uv_axis_scale(3_200_000.0),
            super::UvAxisScale {
                unit_label: "Mλ",
                lambda_scale: 1_000_000.0,
            }
        );
    }

    #[test]
    fn uv_plot_summary_reports_scaled_units() {
        let mut coverage = sample_uv_coverage();
        coverage.sample_count = 42;
        coverage.tracks.clear();
        let summary = uv_plot_summary(&coverage);
        assert!(summary.contains("UV coverage in Mλ."));
        assert!(summary.contains("Max |u,v|=1.2"));
    }

    #[test]
    fn uv_plot_theme_uses_distinct_palettes() {
        let dense = uv_plot_theme(ThemeMode::DenseAnsi);
        let rich = uv_plot_theme(ThemeMode::RichPanel);

        assert_ne!(dense.background, rich.background);
        assert_ne!(dense.track, rich.track);
        assert_ne!(dense.mirror, rich.mirror);
    }

    #[test]
    fn render_uv_plot_renders_tracks_for_both_themes() {
        let coverage = sample_uv_coverage();
        for theme_mode in [ThemeMode::DenseAnsi, ThemeMode::RichPanel] {
            match render_uv_plot(
                160,
                120,
                &super::UvPlotRenderInput {
                    coverage: coverage.clone(),
                    theme_mode,
                },
            ) {
                Ok(image) => {
                    assert_eq!(image.width(), 160);
                    assert_eq!(image.height(), 120);
                }
                Err(error) => {
                    assert!(
                        error.contains("FontUnavailable"),
                        "unexpected render failure: {error}"
                    );
                }
            }
        }
    }

    #[test]
    fn render_uv_plot_rejects_zero_dimensions() {
        let err = render_uv_plot(
            0,
            120,
            &super::UvPlotRenderInput {
                coverage: sample_uv_coverage(),
                theme_mode: ThemeMode::DenseAnsi,
            },
        )
        .expect_err("zero-width plot should fail");
        assert!(err.contains("non-zero"));
    }
}
