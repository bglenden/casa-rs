// SPDX-License-Identifier: LGPL-3.0-or-later
use image::DynamicImage;

use crate::config::ThemeMode;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ListObsPlotRenderInput {
    pub payload: casacore_ms::ListObsPlotPayload,
    pub theme_mode: ThemeMode,
    pub terminal_cell_px: (u16, u16),
}

pub(crate) fn plot_theme(theme_mode: ThemeMode) -> casacore_ms::ListObsPlotTheme {
    match theme_mode {
        ThemeMode::DenseAnsi => casacore_ms::ListObsPlotTheme::dark(),
        ThemeMode::RichPanel => casacore_ms::ListObsPlotTheme::light(),
    }
}

pub(crate) fn render_plot_image(
    width: u32,
    height: u32,
    input: &ListObsPlotRenderInput,
) -> Result<DynamicImage, String> {
    casacore_ms::render_listobs_plot_image_with_style(
        &input.payload,
        plot_theme(input.theme_mode),
        width,
        height,
        casacore_ms::ListObsPlotRenderStyle::for_terminal_cells(
            input.terminal_cell_px.0,
            input.terminal_cell_px.1,
        ),
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
