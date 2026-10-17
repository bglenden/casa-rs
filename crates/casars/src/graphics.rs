// SPDX-License-Identifier: LGPL-3.0-or-later
use image::DynamicImage;

use crate::config::ThemeMode;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MsExplorePlotRenderInput {
    pub payload: casa_ms::MsPlotPayload,
    pub theme_mode: ThemeMode,
    pub terminal_cell_px: (u16, u16),
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
    input: &MsExplorePlotRenderInput,
) -> Result<DynamicImage, String> {
    casa_ms::render_msexplore_plot_image(
        &input.payload,
        plot_theme(input.theme_mode),
        width,
        height,
    )
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
