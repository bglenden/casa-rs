// SPDX-License-Identifier: LGPL-3.0-or-later
use image::{DynamicImage, RgbImage};
use plotters::backend::BitMapBackend;
use plotters::drawing::IntoDrawingArea;
use plotters::prelude::{ChartBuilder, LineSeries, RGBColor, Rectangle};
#[cfg(not(target_os = "macos"))]
use plotters::style::FontStyle;
#[cfg(not(target_os = "macos"))]
use plotters::style::register_font;
use plotters::style::{Color, IntoFont};
use std::path::PathBuf;
#[cfg(not(target_os = "macos"))]
use std::sync::OnceLock;

use crate::config::ThemeMode;

#[cfg(not(target_os = "macos"))]
static PLOTTERS_SANS_FONT: OnceLock<Result<(), &'static str>> = OnceLock::new();

fn ensure_plotters_font() -> Result<(), String> {
    #[cfg(not(target_os = "macos"))]
    {
        PLOTTERS_SANS_FONT
            .get_or_init(|| {
                register_font(
                    "sans-serif",
                    FontStyle::Normal,
                    include_bytes!("../../casa-ms/assets/NotoSans-Regular.ttf"),
                )
                .map_err(|_| "failed to register bundled sans-serif font")
            })
            .as_ref()
            .map_err(|message| (*message).to_string())?;
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MsExplorePlotRenderInput {
    pub payload: casa_ms::MsPlotPayload,
    pub theme_mode: ThemeMode,
    pub terminal_cell_px: (u16, u16),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PlotRenderInput {
    MsExplore(MsExplorePlotRenderInput),
    Imaging(ImagingPlotRenderInput),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ImagingPlotPayload {
    ArtifactPreview {
        title: String,
        image_path: PathBuf,
    },
    ChannelSeries {
        title: String,
        y_label: String,
        series: Vec<ImagingPlotSeries>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ImagingPlotSeries {
    pub label: String,
    pub points: Vec<(usize, f64)>,
    pub color_index: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ImagingPlotRenderInput {
    pub payload: ImagingPlotPayload,
    pub theme_mode: ThemeMode,
}
pub(crate) use casa_images::{
    BrowserRenderTheme, ImagePlaneColormap, ImagePlaneOverlayMarker, ImagePlaneRenderInput,
    ImageSpectrumOverlaySeries, ImageSpectrumRenderInput, image_plane_layout,
    image_spectrum_layout, render_image_plane_image, render_image_spectrum_image,
};

pub(crate) fn plot_theme(theme_mode: ThemeMode) -> casa_ms::MeasurementSetPlotTheme {
    match theme_mode {
        ThemeMode::DenseAnsi => casa_ms::MeasurementSetPlotTheme::dark(),
        ThemeMode::RichPanel => casa_ms::MeasurementSetPlotTheme::light(),
    }
}

pub(crate) fn render_plot_image(
    width: u32,
    height: u32,
    input: &PlotRenderInput,
) -> Result<DynamicImage, String> {
    match input {
        PlotRenderInput::MsExplore(input) => casa_ms::render_msexplore_plot_image(
            &input.payload,
            plot_theme(input.theme_mode),
            width,
            height,
        ),
        PlotRenderInput::Imaging(input) => {
            render_imaging_plot_image(width, height, &input.payload, input.theme_mode)
        }
    }
}

fn render_imaging_plot_image(
    width: u32,
    height: u32,
    payload: &ImagingPlotPayload,
    theme_mode: ThemeMode,
) -> Result<DynamicImage, String> {
    match payload {
        ImagingPlotPayload::ArtifactPreview { image_path, .. } => image::open(image_path)
            .map(|image| image.thumbnail(width.max(1), height.max(1)))
            .map_err(|error| format!("open preview {}: {error}", image_path.display())),
        ImagingPlotPayload::ChannelSeries {
            title,
            y_label,
            series,
        } => render_imaging_channel_series(width, height, title, y_label, series, theme_mode),
    }
}

fn render_imaging_channel_series(
    width: u32,
    height: u32,
    title: &str,
    y_label: &str,
    series: &[ImagingPlotSeries],
    theme_mode: ThemeMode,
) -> Result<DynamicImage, String> {
    ensure_plotters_font()?;
    let width = width.max(1);
    let height = height.max(1);
    let mut buffer = vec![0u8; width as usize * height as usize * 3];
    let background = match theme_mode {
        ThemeMode::DenseAnsi => RGBColor(15, 23, 42),
        ThemeMode::RichPanel => RGBColor(248, 250, 252),
    };
    let foreground = match theme_mode {
        ThemeMode::DenseAnsi => RGBColor(226, 232, 240),
        ThemeMode::RichPanel => RGBColor(15, 23, 42),
    };
    let grid = match theme_mode {
        ThemeMode::DenseAnsi => RGBColor(71, 85, 105),
        ThemeMode::RichPanel => RGBColor(203, 213, 225),
    };
    let root = BitMapBackend::with_buffer(&mut buffer, (width, height)).into_drawing_area();
    root.fill(&background).map_err(|error| error.to_string())?;

    let max_channel = series
        .iter()
        .flat_map(|series| series.points.iter().map(|(x, _)| *x))
        .max()
        .unwrap_or(0);
    let y_values = series
        .iter()
        .flat_map(|series| series.points.iter().map(|(_, y)| *y))
        .collect::<Vec<_>>();
    let y_max = y_values.iter().copied().fold(0.0_f64, f64::max).max(1.0);
    let y_min = y_values.iter().copied().fold(y_max, f64::min).min(0.0);

    let mut chart = ChartBuilder::on(&root)
        .margin(16)
        .caption(title, ("sans-serif", 24).into_font().color(&foreground))
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(0usize..max_channel.max(1), y_min..y_max)
        .map_err(|error| error.to_string())?;

    chart
        .configure_mesh()
        .x_desc("Channel")
        .y_desc(y_label)
        .axis_style(foreground)
        .label_style(("sans-serif", 16).into_font().color(&foreground))
        .light_line_style(grid.mix(0.5))
        .bold_line_style(grid)
        .draw()
        .map_err(|error| error.to_string())?;

    let colors = [
        RGBColor(59, 130, 246),
        RGBColor(239, 68, 68),
        RGBColor(16, 185, 129),
        RGBColor(245, 158, 11),
    ];
    for line in series {
        let color = colors[line.color_index % colors.len()];
        chart
            .draw_series(LineSeries::new(
                line.points.iter().map(|(x, y)| (*x, *y)),
                &color,
            ))
            .map_err(|error| error.to_string())?
            .label(line.label.clone())
            .legend(move |(x, y)| Rectangle::new([(x, y - 5), (x + 18, y + 5)], color.filled()));
    }
    chart
        .configure_series_labels()
        .background_style(background.mix(0.8))
        .border_style(foreground)
        .label_font(("sans-serif", 15).into_font().color(&foreground))
        .draw()
        .map_err(|error| error.to_string())?;

    drop(chart);
    drop(root);
    let image = RgbImage::from_raw(width, height, buffer)
        .ok_or_else(|| "build diagnostic image buffer".to_string())?;
    Ok(DynamicImage::ImageRgb8(image))
}
#[cfg(test)]
mod tests {
    use super::plot_theme;
    use crate::config::ThemeMode;

    #[test]
    fn theme_mapping_uses_distinct_backgrounds() {
        assert_ne!(
            plot_theme(ThemeMode::DenseAnsi).background,
            plot_theme(ThemeMode::RichPanel).background
        );
    }
}
