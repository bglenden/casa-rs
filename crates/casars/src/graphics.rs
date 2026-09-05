// SPDX-License-Identifier: LGPL-3.0-or-later
use image::DynamicImage;
use std::path::PathBuf;

use crate::config::ThemeMode;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MsExplorePlotRenderInput {
    pub payload: casa_ms::MsPlotPayload,
    pub theme_mode: ThemeMode,
    pub terminal_cell_px: (u16, u16),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PlotRenderInput {
    MsExplore(Box<MsExplorePlotRenderInput>),
    Imaging(ImagingPlotRenderInput),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ImagingPlotPayload {
    ArtifactPreview { title: String, image_path: PathBuf },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ImagingPlotRenderInput {
    pub payload: ImagingPlotPayload,
    pub theme_mode: ThemeMode,
}
pub(crate) use crate::browser_render::{
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
    _theme_mode: ThemeMode,
) -> Result<DynamicImage, String> {
    match payload {
        ImagingPlotPayload::ArtifactPreview { image_path, .. } => image::open(image_path)
            .map(|image| image.thumbnail(width.max(1), height.max(1)))
            .map_err(|error| format!("open preview {}: {error}", image_path.display())),
    }
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
