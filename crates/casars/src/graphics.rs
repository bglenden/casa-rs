// SPDX-License-Identifier: LGPL-3.0-or-later
use casacore_imagebrowser_protocol::{
    ImageBrowserProbe, ImageDisplayAxisState, ImagePlaneRaster, ImageProfilePayload,
    ImageRegionOverlayShapeState,
};
use casacore_types::measures::direction::{format_declination, format_right_ascension};
use casacore_types::quanta::{Quantity, Unit};
use image::{DynamicImage, Rgb, RgbImage};
use plotters::backend::BitMapBackend;
use plotters::drawing::IntoDrawingArea;
use plotters::prelude::{FontTransform, PathElement, RGBColor, Text};
use plotters::style::IntoFont;
use plotters::style::text_anchor::{HPos, Pos, VPos};

use crate::config::ThemeMode;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ListObsPlotRenderInput {
    pub payload: casacore_ms::ListObsPlotPayload,
    pub theme_mode: ThemeMode,
    pub terminal_cell_px: (u16, u16),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MsExplorePlotRenderInput {
    pub payload: casacore_ms::MsPlotPayload,
    pub theme_mode: ThemeMode,
    pub terminal_cell_px: (u16, u16),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PlotRenderInput {
    ListObs(ListObsPlotRenderInput),
    MsExplore(MsExplorePlotRenderInput),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ImagePlaneOverlayMarker {
    pub sample: (usize, usize),
    pub color_index: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ImagePlaneRenderInput {
    pub cache_key: u64,
    pub raster: ImagePlaneRaster,
    pub cursor_sample: Option<(usize, usize)>,
    pub sampled_shape: Option<(usize, usize)>,
    pub display_axes: Vec<ImageDisplayAxisState>,
    pub probe: Option<ImageBrowserProbe>,
    pub overlay_markers: Vec<ImagePlaneOverlayMarker>,
    pub region_overlay_shapes: Vec<ImageRegionOverlayShapeState>,
    pub display_aspect_ratio: Option<f64>,
    pub colormap: ImagePlaneColormap,
    pub invert: bool,
    pub theme_mode: ThemeMode,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ImageSpectrumOverlaySeries {
    pub label: String,
    pub profile: ImageProfilePayload,
    pub color_index: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ImageSpectrumRenderInput {
    pub cache_key: u64,
    pub profile: ImageProfilePayload,
    pub overlay_profiles: Vec<ImageSpectrumOverlaySeries>,
    pub theme_mode: ThemeMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImagePlaneColormap {
    Grayscale,
    Viridis,
    Inferno,
}

impl ImagePlaneColormap {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Grayscale => "grayscale",
            Self::Viridis => "viridis",
            Self::Inferno => "inferno",
        }
    }

    pub(crate) const fn next(self) -> Self {
        match self {
            Self::Grayscale => Self::Viridis,
            Self::Viridis => Self::Inferno,
            Self::Inferno => Self::Grayscale,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ImagePlaneDrawGeometry {
    pub x: u32,
    pub y: u32,
    pub width: u32,
    pub height: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ImagePlaneLayout {
    pub image: ImagePlaneDrawGeometry,
    pub left_gutter: u32,
    pub top_gutter: u32,
    pub right_gutter: u32,
    pub bottom_gutter: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ImageSpectrumLayout {
    pub plot: ImagePlaneDrawGeometry,
    pub left_gutter: u32,
    pub top_gutter: u32,
    pub right_gutter: u32,
    pub bottom_gutter: u32,
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
    input: &PlotRenderInput,
) -> Result<DynamicImage, String> {
    match input {
        PlotRenderInput::ListObs(input) => casacore_ms::render_listobs_plot_image_with_style(
            &input.payload,
            plot_theme(input.theme_mode),
            width,
            height,
            casacore_ms::ListObsPlotRenderStyle::for_terminal_cells(
                input.terminal_cell_px.0,
                input.terminal_cell_px.1,
            ),
        ),
        PlotRenderInput::MsExplore(input) => casacore_ms::render_msexplore_plot_image(
            &input.payload,
            plot_theme(input.theme_mode),
            width,
            height,
        ),
    }
}

pub(crate) fn render_image_plane_image(
    width: u32,
    height: u32,
    input: &ImagePlaneRenderInput,
) -> Result<DynamicImage, String> {
    let width = width.max(1);
    let height = height.max(1);
    let raster_width = input.raster.width.max(1);
    let raster_height = input.raster.height.max(1);
    let mut image = RgbImage::new(width, height);
    let layout = image_plane_layout(
        width,
        height,
        input.display_aspect_ratio,
        input.display_axes.len() >= 2,
    );
    let geometry = layout.image;
    let highlight = image_browser_highlight(input.theme_mode);
    let marker_halo = match input.theme_mode {
        ThemeMode::DenseAnsi => [0, 0, 0],
        ThemeMode::RichPanel => [15, 23, 42],
    };
    let marker_center = match input.theme_mode {
        ThemeMode::DenseAnsi => [0, 0, 0],
        ThemeMode::RichPanel => [248, 250, 252],
    };

    for out_y in geometry.y..geometry.y + geometry.height {
        let relative_y = out_y - geometry.y;
        let raster_y = ((relative_y as usize) * raster_height / geometry.height as usize)
            .min(raster_height.saturating_sub(1));
        for out_x in geometry.x..geometry.x + geometry.width {
            let relative_x = out_x - geometry.x;
            let raster_x = ((relative_x as usize) * raster_width / geometry.width as usize)
                .min(raster_width.saturating_sub(1));
            let value = input
                .raster
                .pixels_u8
                .get(raster_y * raster_width + raster_x)
                .copied()
                .unwrap_or_default();
            image.put_pixel(
                out_x,
                out_y,
                Rgb(image_plane_pixel_color(value, input.colormap, input.invert)),
            );
        }
    }

    if let Some((sampled_width, sampled_height)) = input.sampled_shape {
        let region_color = Rgb(image_region_overlay_color(input.theme_mode));
        let region_halo = Rgb(marker_halo);
        for shape in &input.region_overlay_shapes {
            if shape.vertices.len() < 2 {
                continue;
            }
            let screen_vertices = shape
                .vertices
                .iter()
                .map(|vertex| {
                    (
                        sampled_coordinate_to_screen_x(
                            geometry,
                            sampled_width as u32,
                            vertex.sampled_x,
                        ),
                        sampled_coordinate_to_screen_y(
                            geometry,
                            sampled_height as u32,
                            vertex.sampled_y,
                        ),
                    )
                })
                .collect::<Vec<_>>();
            for index in 0..screen_vertices.len().saturating_sub(1) {
                let left = screen_vertices[index];
                let right = screen_vertices[index + 1];
                draw_region_segment(&mut image, left, right, region_halo, region_color);
            }
            if shape.closed
                && let (Some(first), Some(last)) = (
                    screen_vertices.first().copied(),
                    screen_vertices.last().copied(),
                )
            {
                draw_region_segment(&mut image, last, first, region_halo, region_color);
            }
            for &(x, y) in &screen_vertices {
                draw_probe_marker(&mut image, x, y, 3, region_halo, region_color);
            }
        }
    }

    if let Some((cursor_x, cursor_y)) = input.cursor_sample {
        let (sampled_width, sampled_height) =
            input.sampled_shape.unwrap_or((raster_width, raster_height));
        let marker = plane_cursor_marker_geometry(
            geometry,
            sampled_width as u32,
            sampled_height as u32,
            cursor_x as u32,
            cursor_y as u32,
        );
        let center_x = marker.x + marker.width.saturating_sub(1) / 2;
        let center_y = marker.y + marker.height.saturating_sub(1) / 2;
        let guide_radius = (geometry.width.min(geometry.height) / 18).clamp(4, 12);
        draw_cursor_guides(
            &mut image,
            geometry,
            center_x,
            center_y,
            guide_radius,
            Rgb(marker_halo),
            Rgb(highlight),
        );
        draw_rect_outline(
            &mut image,
            marker.x,
            marker.y,
            marker.width,
            marker.height,
            Rgb(highlight),
        );
        let radius = if marker.width.max(marker.height) >= 8 {
            2
        } else {
            1
        };
        let reticle = guide_radius.max(radius + 2);
        let image_width = image.width();
        let image_height = image.height();
        let outer_x = center_x.saturating_sub(reticle + 1);
        let outer_y = center_y.saturating_sub(reticle + 1);
        let outer_width = (reticle * 2 + 3).min(image_width.saturating_sub(outer_x));
        let outer_height = (reticle * 2 + 3).min(image_height.saturating_sub(outer_y));
        draw_rect_outline(
            &mut image,
            outer_x,
            outer_y,
            outer_width,
            outer_height,
            Rgb(marker_halo),
        );
        let inner_x = center_x.saturating_sub(reticle);
        let inner_y = center_y.saturating_sub(reticle);
        let inner_width = (reticle * 2 + 1).min(image_width.saturating_sub(inner_x));
        let inner_height = (reticle * 2 + 1).min(image_height.saturating_sub(inner_y));
        draw_rect_outline(
            &mut image,
            inner_x,
            inner_y,
            inner_width,
            inner_height,
            Rgb(highlight),
        );
        draw_cross(&mut image, center_x, center_y, radius, Rgb(highlight));
        image.put_pixel(center_x, center_y, Rgb(marker_center));
    }

    if let Some((sampled_width, sampled_height)) = input.sampled_shape {
        let probe_radius = probe_marker_radius(geometry);
        for overlay in &input.overlay_markers {
            let marker = plane_cursor_marker_geometry(
                geometry,
                sampled_width as u32,
                sampled_height as u32,
                overlay.sample.0 as u32,
                overlay.sample.1 as u32,
            );
            let color = Rgb(image_probe_overlay_color(
                input.theme_mode,
                overlay.color_index,
            ));
            let center_x = marker.x + marker.width.saturating_sub(1) / 2;
            let center_y = marker.y + marker.height.saturating_sub(1) / 2;
            draw_probe_marker(
                &mut image,
                center_x,
                center_y,
                probe_radius,
                Rgb(marker_halo),
                color,
            );
        }
    }

    if input.display_axes.len() >= 2 {
        draw_plane_axis_annotations(&mut image, layout, input)?;
    }
    if layout.right_gutter >= 48 && !input.raster.no_finite_values {
        draw_plane_scale_sidebar(&mut image, layout, input)?;
    }

    Ok(DynamicImage::ImageRgb8(image))
}

pub(crate) fn render_image_spectrum_image(
    width: u32,
    height: u32,
    input: &ImageSpectrumRenderInput,
) -> Result<DynamicImage, String> {
    let width = width.max(1);
    let height = height.max(1);
    let mut image = RgbImage::new(width, height);
    let highlight = image_browser_highlight(input.theme_mode);
    let (background, border, axis_text, series, marker, guide_line, zero_line) =
        match input.theme_mode {
            ThemeMode::DenseAnsi => (
                [0, 0, 0],
                [96, 96, 96],
                [224, 224, 224],
                [96, 208, 255],
                highlight,
                highlight,
                [48, 48, 48],
            ),
            ThemeMode::RichPanel => (
                [15, 23, 42],
                [94, 234, 212],
                [226, 232, 240],
                [125, 211, 252],
                highlight,
                highlight,
                [55, 65, 81],
            ),
        };
    fill_rect(&mut image, 0, 0, width, height, Rgb(background));

    if width < 4 || height < 4 {
        return Ok(DynamicImage::ImageRgb8(image));
    }

    let layout = image_spectrum_layout(width, height);
    let plot_left = layout.plot.x;
    let plot_top = layout.plot.y;
    let plot_width = layout.plot.width;
    let plot_height = layout.plot.height;
    draw_rect_outline(
        &mut image,
        plot_left,
        plot_top,
        plot_width,
        plot_height,
        Rgb(border),
    );

    let x_domain = spectrum_x_domain(input);
    let Some((mut min_value, mut max_value)) = spectrum_value_range(input) else {
        return Ok(DynamicImage::ImageRgb8(image));
    };
    if (max_value - min_value).abs() < f64::EPSILON {
        min_value -= 1.0;
        max_value += 1.0;
    }

    if min_value <= 0.0 && max_value >= 0.0 {
        let zero_y = plot_value_y(0.0, min_value, max_value, plot_top, plot_height);
        draw_horizontal_line(
            &mut image,
            plot_left,
            plot_left + plot_width.saturating_sub(1),
            zero_y,
            Rgb(zero_line),
        );
    }

    for overlay in &input.overlay_profiles {
        draw_profile_series(
            &mut image,
            &overlay.profile,
            plot_left,
            plot_top,
            plot_width,
            plot_height,
            x_domain,
            min_value,
            max_value,
            Rgb(image_probe_overlay_color(
                input.theme_mode,
                overlay.color_index,
            )),
        );
    }
    draw_profile_series(
        &mut image,
        &input.profile,
        plot_left,
        plot_top,
        plot_width,
        plot_height,
        x_domain,
        min_value,
        max_value,
        Rgb(series),
    );

    if let Some(selected) = input
        .profile
        .samples
        .get(input.profile.selected_sample_index)
        .filter(|sample| !sample.masked && sample.finite && sample.value.is_finite())
    {
        let x_value = profile_sample_x_value(selected);
        let marker_x = plot_value_x(x_value, x_domain.0, x_domain.1, plot_left, plot_width);
        let marker_y = plot_value_y(selected.value, min_value, max_value, plot_top, plot_height);
        draw_vertical_line(
            &mut image,
            marker_x,
            plot_top,
            plot_top + plot_height.saturating_sub(1),
            Rgb(guide_line),
        );
        if marker_x > plot_left {
            draw_vertical_line(
                &mut image,
                marker_x - 1,
                plot_top,
                plot_top + plot_height.saturating_sub(1),
                Rgb(guide_line),
            );
        }
        draw_cross(&mut image, marker_x, marker_y, 3, Rgb(marker));
    }

    draw_spectrum_axes(
        &mut image, layout, input, x_domain, min_value, max_value, border, axis_text,
    )?;

    Ok(DynamicImage::ImageRgb8(image))
}

#[allow(clippy::too_many_arguments)]
fn draw_spectrum_axes(
    image: &mut RgbImage,
    layout: ImageSpectrumLayout,
    input: &ImageSpectrumRenderInput,
    x_domain: (f64, f64),
    min_value: f64,
    max_value: f64,
    border: [u8; 3],
    axis_text: [u8; 3],
) -> Result<(), String> {
    let x_scale = spectrum_x_scale(input, x_domain);
    let y_step = nice_numeric_step((max_value - min_value).abs() / 4.0);
    let y_ticks = numeric_ticks(min_value, max_value, y_step, layout.plot.height)
        .into_iter()
        .map(|tick| SpectrumAxisTick {
            value: tick.value,
            label: format_spectrum_axis_numeric(tick.value, &input.profile.value_unit, y_step),
            position_px: plot_value_y(tick.value, min_value, max_value, 0, layout.plot.height)
                .min(layout.plot.height.saturating_sub(1)),
        })
        .collect::<Vec<_>>();
    let x_ticks = spectrum_x_ticks(&x_scale, x_domain, layout.plot.width);

    let border = RGBColor(border[0], border[1], border[2]);
    let axis_text = RGBColor(axis_text[0], axis_text[1], axis_text[2]);
    let image_width = image.width();
    let image_height = image.height();
    let area =
        BitMapBackend::with_buffer(image.as_mut(), (image_width, image_height)).into_drawing_area();
    let left_x = layout.plot.x as i32;
    let bottom_y = (layout.plot.y + layout.plot.height.saturating_sub(1)) as i32;
    let tick_font_size = spectrum_tick_font_size(layout.plot);
    let axis_font_size = spectrum_axis_font_size(layout.plot);
    let tick_font = ("sans-serif", tick_font_size).into_font().color(&axis_text);
    let axis_font = ("sans-serif", axis_font_size).into_font().color(&axis_text);
    let tick_length = spectrum_tick_length(layout.plot) as i32;

    for tick in &x_ticks {
        let x = (layout.plot.x + tick.position_px) as i32;
        area.draw(&PathElement::new(
            vec![(x, bottom_y), (x, bottom_y + tick_length)],
            border,
        ))
        .map_err(|error| error.to_string())?;
        area.draw(&Text::new(
            tick.label.clone(),
            (x, bottom_y + tick_length + 4),
            tick_font.clone().pos(Pos::new(HPos::Center, VPos::Top)),
        ))
        .map_err(|error| error.to_string())?;
    }
    for tick in &y_ticks {
        let y = (layout.plot.y + tick.position_px) as i32;
        area.draw(&PathElement::new(
            vec![(left_x - tick_length, y), (left_x, y)],
            border,
        ))
        .map_err(|error| error.to_string())?;
        area.draw(&Text::new(
            tick.label.clone(),
            (left_x - tick_length - 4, y),
            tick_font.clone().pos(Pos::new(HPos::Right, VPos::Center)),
        ))
        .map_err(|error| error.to_string())?;
    }

    area.draw(&Text::new(
        spectrum_x_axis_title(&x_scale, input),
        (
            (layout.plot.x + layout.plot.width / 2) as i32,
            image_height.saturating_sub(4) as i32,
        ),
        axis_font.clone().pos(Pos::new(HPos::Center, VPos::Bottom)),
    ))
    .map_err(|error| error.to_string())?;
    let y_title_x = spectrum_y_title_x(layout, tick_length, tick_font_size, axis_font_size);
    area.draw(&Text::new(
        spectrum_y_axis_title(input),
        (y_title_x, (layout.plot.y + layout.plot.height / 2) as i32),
        ("sans-serif", axis_font_size)
            .into_font()
            .transform(FontTransform::Rotate270)
            .color(&axis_text)
            .pos(Pos::new(HPos::Center, VPos::Center)),
    ))
    .map_err(|error| error.to_string())?;
    area.present().map_err(|error| error.to_string())
}

pub(crate) fn image_plane_draw_geometry(
    width: u32,
    height: u32,
    display_aspect_ratio: Option<f64>,
) -> ImagePlaneDrawGeometry {
    let width = width.max(1);
    let height = height.max(1);
    let Some(desired_aspect) =
        display_aspect_ratio.filter(|ratio| ratio.is_finite() && *ratio > 0.0)
    else {
        return ImagePlaneDrawGeometry {
            x: 0,
            y: 0,
            width,
            height,
        };
    };
    let canvas_aspect = width as f64 / height as f64;
    if (canvas_aspect - desired_aspect).abs() < f64::EPSILON {
        return ImagePlaneDrawGeometry {
            x: 0,
            y: 0,
            width,
            height,
        };
    }
    if canvas_aspect > desired_aspect {
        let draw_width = ((height as f64 * desired_aspect).round() as u32).clamp(1, width);
        ImagePlaneDrawGeometry {
            x: (width - draw_width) / 2,
            y: 0,
            width: draw_width,
            height,
        }
    } else {
        let draw_height = ((width as f64 / desired_aspect).round() as u32).clamp(1, height);
        ImagePlaneDrawGeometry {
            x: 0,
            y: (height - draw_height) / 2,
            width,
            height: draw_height,
        }
    }
}

pub(crate) fn image_spectrum_layout(width: u32, height: u32) -> ImageSpectrumLayout {
    let width = width.max(1);
    let height = height.max(1);
    let left_gutter = if width >= 180 {
        72
    } else if width >= 120 {
        56
    } else {
        (width / 4).clamp(16, 32)
    }
    .min(width.saturating_sub(2));
    let right_gutter = 8.min(width.saturating_sub(left_gutter + 1));
    let top_gutter = 8.min(height.saturating_sub(2));
    let bottom_gutter = if height >= 120 {
        36
    } else if height >= 72 {
        28
    } else {
        (height / 4).clamp(12, 18)
    }
    .min(height.saturating_sub(top_gutter + 1));
    let plot_width = width.saturating_sub(left_gutter + right_gutter).max(1);
    let plot_height = height.saturating_sub(top_gutter + bottom_gutter).max(1);
    ImageSpectrumLayout {
        plot: ImagePlaneDrawGeometry {
            x: left_gutter,
            y: top_gutter,
            width: plot_width,
            height: plot_height,
        },
        left_gutter,
        top_gutter,
        right_gutter,
        bottom_gutter,
    }
}

pub(crate) fn image_plane_layout(
    width: u32,
    height: u32,
    display_aspect_ratio: Option<f64>,
    axis_annotations: bool,
) -> ImagePlaneLayout {
    let width = width.max(1);
    let height = height.max(1);
    let show_axes = axis_annotations && width >= 160 && height >= 120;
    let (left_gutter, top_gutter, right_gutter, bottom_gutter) = if show_axes {
        (
            (width / 5).clamp(88, 144),
            12,
            (width / 6).clamp(88, 132),
            (height / 4).clamp(52, 84),
        )
    } else {
        (0, 0, 0, 0)
    };
    let inner_width = width.saturating_sub(left_gutter + right_gutter).max(1);
    let inner_height = height.saturating_sub(top_gutter + bottom_gutter).max(1);
    let base = image_plane_draw_geometry(inner_width, inner_height, display_aspect_ratio);
    ImagePlaneLayout {
        image: ImagePlaneDrawGeometry {
            x: base.x + left_gutter,
            y: base.y + top_gutter,
            width: base.width,
            height: base.height,
        },
        left_gutter,
        top_gutter,
        right_gutter,
        bottom_gutter,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlaneAxisTick {
    pixel: usize,
    label: String,
    position_px: u32,
}

#[derive(Debug, Clone, PartialEq)]
struct SpectrumAxisTick {
    value: f64,
    label: String,
    position_px: u32,
}

#[derive(Debug, Clone, PartialEq)]
struct AxisDisplayScale {
    kind: AxisDisplayKind,
    probe_display: f64,
    increment_display: f64,
}

#[derive(Debug, Clone, PartialEq)]
enum AxisDisplayKind {
    RightAscensionSeconds,
    DeclinationArcseconds,
    Frequency { unit: &'static str, scale: f64 },
    Generic { unit: String },
}

fn draw_plane_axis_annotations(
    image: &mut RgbImage,
    layout: ImagePlaneLayout,
    input: &ImagePlaneRenderInput,
) -> Result<(), String> {
    let Some(x_axis) = input.display_axes.first() else {
        return Ok(());
    };
    let Some(y_axis) = input.display_axes.get(1) else {
        return Ok(());
    };
    let x_ticks = plane_axis_ticks(x_axis, input.probe.as_ref(), layout.image.width, false);
    let y_ticks = plane_axis_ticks(y_axis, input.probe.as_ref(), layout.image.height, true);
    if x_ticks.is_empty() && y_ticks.is_empty() {
        return Ok(());
    }

    let (axis_line, axis_text) = image_plane_axis_colors(input.theme_mode);
    let axis_line = RGBColor(axis_line[0], axis_line[1], axis_line[2]);
    let axis_text = RGBColor(axis_text[0], axis_text[1], axis_text[2]);
    let image_width = image.width();
    let image_height = image.height();
    let area =
        BitMapBackend::with_buffer(image.as_mut(), (image_width, image_height)).into_drawing_area();

    let bottom_y = (layout.image.y + layout.image.height.saturating_sub(1)) as i32;
    let left_x = layout.image.x as i32;
    area.draw(&PathElement::new(
        vec![
            (left_x, bottom_y),
            (
                (layout.image.x + layout.image.width.saturating_sub(1)) as i32,
                bottom_y,
            ),
        ],
        axis_line,
    ))
    .map_err(|error| error.to_string())?;
    area.draw(&PathElement::new(
        vec![
            (left_x, layout.image.y as i32),
            (
                left_x,
                (layout.image.y + layout.image.height.saturating_sub(1)) as i32,
            ),
        ],
        axis_line,
    ))
    .map_err(|error| error.to_string())?;

    let tick_font = ("sans-serif", plane_tick_font_size(layout.image))
        .into_font()
        .color(&axis_text);
    let tick_font_size = plane_tick_font_size(layout.image);
    let axis_font_size = plane_axis_font_size(layout.image);
    let axis_font = ("sans-serif", axis_font_size).into_font().color(&axis_text);

    let tick_length = plane_tick_length(layout.image) as i32;
    let x_label_offset = tick_length + 8;
    let y_label_offset = tick_length + 8;
    let x_axis_title_y = image_height.saturating_sub(4) as i32;
    let y_axis_title_x = left_x
        - tick_length
        - y_label_offset
        - tick_font_size
        - axis_font_size.saturating_div(2)
        - 6;
    let gutter_left_x = layout.image.x.saturating_sub(layout.left_gutter) as i32;
    let y_axis_title_x = y_axis_title_x.min(gutter_left_x + axis_font_size.saturating_div(2) + 6);

    for tick in &x_ticks {
        let x = (layout.image.x + tick.position_px) as i32;
        area.draw(&PathElement::new(
            vec![(x, bottom_y), (x, bottom_y + tick_length)],
            axis_line,
        ))
        .map_err(|error| error.to_string())?;
        area.draw(&Text::new(
            tick.label.clone(),
            (x, bottom_y + x_label_offset),
            tick_font.clone().pos(Pos::new(HPos::Center, VPos::Top)),
        ))
        .map_err(|error| error.to_string())?;
    }
    for tick in &y_ticks {
        let y = (layout.image.y + tick.position_px) as i32;
        area.draw(&PathElement::new(
            vec![(left_x - tick_length, y), (left_x, y)],
            axis_line,
        ))
        .map_err(|error| error.to_string())?;
        area.draw(&Text::new(
            tick.label.clone(),
            (left_x - y_label_offset, y),
            tick_font.clone().pos(Pos::new(HPos::Right, VPos::Center)),
        ))
        .map_err(|error| error.to_string())?;
    }

    area.draw(&Text::new(
        plane_axis_title(x_axis),
        (
            (layout.image.x + layout.image.width / 2) as i32,
            x_axis_title_y,
        ),
        axis_font.pos(Pos::new(HPos::Center, VPos::Bottom)),
    ))
    .map_err(|error| error.to_string())?;
    area.draw(&Text::new(
        plane_axis_title(y_axis),
        (
            y_axis_title_x,
            (layout.image.y + layout.image.height / 2) as i32,
        ),
        ("sans-serif", axis_font_size)
            .into_font()
            .transform(FontTransform::Rotate270)
            .color(&axis_text)
            .pos(Pos::new(HPos::Center, VPos::Center)),
    ))
    .map_err(|error| error.to_string())?;

    area.present().map_err(|error| error.to_string())
}

fn image_plane_axis_colors(theme_mode: ThemeMode) -> ([u8; 3], [u8; 3]) {
    match theme_mode {
        ThemeMode::DenseAnsi => ([96, 96, 96], [224, 224, 224]),
        ThemeMode::RichPanel => ([100, 116, 139], [226, 232, 240]),
    }
}

fn draw_plane_scale_sidebar(
    image: &mut RgbImage,
    layout: ImagePlaneLayout,
    input: &ImagePlaneRenderInput,
) -> Result<(), String> {
    let sidebar_x = layout.image.x + layout.image.width + 12;
    if sidebar_x >= image.width() {
        return Ok(());
    }
    let available_width = image
        .width()
        .saturating_sub(sidebar_x)
        .saturating_sub(8)
        .max(1);
    let available_height = layout.image.height.max(1);
    let label_width = if available_width >= 96 {
        56
    } else if available_width >= 76 {
        48
    } else if available_width >= 60 {
        40
    } else {
        28.min(available_width / 2)
    };
    let wedge_width = if available_width <= label_width + 12 {
        available_width.saturating_sub(label_width + 4).max(8)
    } else {
        available_width
            .saturating_sub(label_width + 12)
            .clamp(12, 18)
    };
    let histogram_width = available_width.saturating_sub(wedge_width + label_width + 12);
    let wedge_x = sidebar_x;
    let wedge_y = layout.image.y;
    let histogram_x = wedge_x + wedge_width + 8;
    let histogram_y = wedge_y;
    let (axis_line, axis_text) = image_plane_axis_colors(input.theme_mode);
    let border = Rgb(axis_line);
    let data_min = input.raster.data_min;
    let data_max = input.raster.data_max;

    for offset in 0..available_height {
        let sample = if let Some(value) =
            plane_sidebar_value_for_offset(offset, available_height, data_min, data_max)
        {
            plane_scale_sample_for_value(value, input.raster.clip_min, input.raster.clip_max)
        } else {
            255
        };
        let color = Rgb(image_plane_pixel_color(
            sample,
            input.colormap,
            input.invert,
        ));
        fill_rect(image, wedge_x, wedge_y + offset, wedge_width, 1, color);
    }
    draw_rect_outline(
        image,
        wedge_x,
        wedge_y,
        wedge_width,
        available_height,
        border,
    );

    if histogram_width >= 8 && !input.raster.histogram_bins.is_empty() {
        let max_count = input
            .raster
            .histogram_bins
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
            .max(1);
        for offset in 0..available_height {
            let bin_index = if available_height <= 1 {
                input.raster.histogram_bins.len().saturating_sub(1)
            } else {
                ((available_height - 1 - offset) as usize * input.raster.histogram_bins.len()
                    / available_height as usize)
                    .min(input.raster.histogram_bins.len().saturating_sub(1))
            };
            let count = input.raster.histogram_bins[bin_index];
            let bar_width = if max_count == 0 {
                0
            } else {
                ((count as f64 / max_count as f64) * histogram_width as f64).round() as u32
            };
            if bar_width > 0 {
                let palette_value =
                    plane_sidebar_value_for_offset(offset, available_height, data_min, data_max)
                        .map(|value| {
                            plane_scale_sample_for_value(
                                value,
                                input.raster.clip_min,
                                input.raster.clip_max,
                            )
                        })
                        .unwrap_or(255);
                fill_rect(
                    image,
                    histogram_x,
                    histogram_y + offset,
                    bar_width,
                    1,
                    Rgb(image_plane_pixel_color(
                        palette_value,
                        input.colormap,
                        input.invert,
                    )),
                );
            }
        }
        draw_rect_outline(
            image,
            histogram_x,
            histogram_y,
            histogram_width,
            available_height,
            border,
        );
        if (data_max - data_min).abs() > f64::EPSILON {
            for marker_value in [input.raster.clip_min, input.raster.clip_max] {
                let y = histogram_y
                    + plane_sidebar_y_for_value(marker_value, available_height, data_min, data_max)
                        .unwrap_or(0);
                fill_rect(
                    image,
                    histogram_x,
                    y.saturating_sub(1),
                    histogram_width,
                    2,
                    border,
                );
            }
            if let Some(probe) = input
                .probe
                .as_ref()
                .filter(|probe| probe.finite && !probe.masked)
            {
                let highlight = Rgb(image_browser_highlight(input.theme_mode));
                if let Some(y) =
                    plane_sidebar_y_for_value(probe.value, available_height, data_min, data_max)
                {
                    fill_rect(
                        image,
                        wedge_x,
                        histogram_y + y.saturating_sub(1),
                        wedge_width,
                        2,
                        highlight,
                    );
                    fill_rect(
                        image,
                        histogram_x,
                        histogram_y + y.saturating_sub(1),
                        histogram_width,
                        2,
                        highlight,
                    );
                }
            }
        }
    }

    let image_width = image.width();
    let image_height = image.height();
    let area =
        BitMapBackend::with_buffer(image.as_mut(), (image_width, image_height)).into_drawing_area();
    let axis_text = RGBColor(axis_text[0], axis_text[1], axis_text[2]);
    let label_font = ("sans-serif", plane_scale_font_size(layout.image))
        .into_font()
        .color(&axis_text);
    let title_font = ("sans-serif", plane_scale_title_font_size(layout.image))
        .into_font()
        .color(&axis_text);

    let label_x = if histogram_width >= 8 {
        (histogram_x + histogram_width + 6) as i32
    } else {
        (wedge_x + wedge_width + 6) as i32
    };
    let title_y = (wedge_y + 6).min(wedge_y + available_height.saturating_sub(2)) as i32;
    area.draw(&Text::new(
        format_plane_value_axis_title(&input.raster.value_unit),
        ((wedge_x + available_width / 2) as i32, title_y),
        title_font.pos(Pos::new(HPos::Center, VPos::Top)),
    ))
    .map_err(|error| error.to_string())?;
    let tick_line = RGBColor(axis_line[0], axis_line[1], axis_line[2]);
    for tick in plane_sidebar_ticks(data_min, data_max, available_height) {
        let y = wedge_y
            + plane_sidebar_y_for_value(tick.value, available_height, data_min, data_max)
                .unwrap_or(0);
        area.draw(&PathElement::new(
            vec![
                ((wedge_x + wedge_width.saturating_sub(4)) as i32, y as i32),
                ((wedge_x + wedge_width) as i32, y as i32),
            ],
            tick_line,
        ))
        .map_err(|error| error.to_string())?;
        area.draw(&Text::new(
            format_plane_sidebar_tick_value(tick.value, &input.raster.value_unit),
            (label_x + 6, y as i32),
            label_font.clone().pos(Pos::new(HPos::Left, VPos::Center)),
        ))
        .map_err(|error| error.to_string())?;
    }
    area.present().map_err(|error| error.to_string())
}

fn plane_axis_ticks(
    axis: &ImageDisplayAxisState,
    probe: Option<&ImageBrowserProbe>,
    geometry_len: u32,
    reverse: bool,
) -> Vec<PlaneAxisTick> {
    if axis.sampled_len == 0 || geometry_len == 0 {
        return Vec::new();
    }
    if let Some(ticks) = plane_axis_nice_ticks(axis, probe, geometry_len, reverse) {
        if !ticks.is_empty() {
            return ticks;
        }
    }
    let tick_count: usize = if geometry_len >= 520 {
        5
    } else if geometry_len >= 300 {
        4
    } else {
        3
    };
    let max_index = axis.sampled_len.saturating_sub(1);
    let mut indices = (0..tick_count)
        .map(|step| {
            if tick_count == 1 {
                0
            } else {
                ((step as f64 * max_index as f64) / (tick_count.saturating_sub(1) as f64)).round()
                    as usize
            }
        })
        .collect::<Vec<_>>();
    indices.dedup();
    indices
        .into_iter()
        .map(|sample_index| {
            let pixel = axis
                .blc
                .saturating_add(sample_index.saturating_mul(axis.inc.max(1)));
            let position_px = if axis.sampled_len <= 1 {
                geometry_len / 2
            } else {
                ((sample_index as f64) * geometry_len.saturating_sub(1) as f64 / max_index as f64)
                    .round() as u32
            };
            let position_px = if reverse {
                geometry_len.saturating_sub(1).saturating_sub(position_px)
            } else {
                position_px
            };
            PlaneAxisTick {
                pixel,
                label: plane_axis_tick_label(axis, probe, pixel),
                position_px,
            }
        })
        .collect()
}

fn plane_axis_nice_ticks(
    axis: &ImageDisplayAxisState,
    probe: Option<&ImageBrowserProbe>,
    geometry_len: u32,
    reverse: bool,
) -> Option<Vec<PlaneAxisTick>> {
    let probe = probe?;
    let probe_world = probe.world_axes.get(axis.axis)?.value;
    let probe_pixel = *probe.pixel_indices.get(axis.axis)? as f64;
    let increment = axis.world_increment?;
    if !probe_world.is_finite() || !increment.is_finite() || increment == 0.0 {
        return None;
    }
    let scale = plane_axis_scale(axis, probe_world, increment)?;
    let start_pixel = axis.blc as f64;
    let end_pixel = axis.trc as f64;
    let start_display = scale.display_at_pixel(probe_pixel, start_pixel);
    let end_display = scale.display_at_pixel(probe_pixel, end_pixel);
    let min_display = start_display.min(end_display);
    let max_display = start_display.max(end_display);
    let step = scale.nice_step(max_display - min_display)?;
    if !step.is_finite() || step <= 0.0 {
        return None;
    }
    let epsilon = step * 1e-6;
    let first = (min_display / step).ceil() * step;
    let mut value = first;
    let mut ticks = Vec::new();
    while value <= max_display + epsilon {
        let pixel = scale.pixel_at_display(probe_pixel, value);
        let sample_index = (pixel - axis.blc as f64) / axis.inc.max(1) as f64;
        if sample_index >= -epsilon
            && sample_index <= axis.sampled_len.saturating_sub(1) as f64 + epsilon
        {
            let position_px = if axis.sampled_len <= 1 {
                geometry_len / 2
            } else {
                ((sample_index.clamp(0.0, axis.sampled_len.saturating_sub(1) as f64)
                    / axis.sampled_len.saturating_sub(1) as f64)
                    * geometry_len.saturating_sub(1) as f64)
                    .round() as u32
            };
            let position_px = if reverse {
                geometry_len.saturating_sub(1).saturating_sub(position_px)
            } else {
                position_px
            };
            ticks.push(PlaneAxisTick {
                pixel: pixel.round().max(0.0) as usize,
                label: scale.label(value, step),
                position_px,
            });
        }
        value += step;
    }
    if ticks.len() >= 2 { Some(ticks) } else { None }
}

fn plane_axis_tick_label(
    axis: &ImageDisplayAxisState,
    probe: Option<&ImageBrowserProbe>,
    pixel: usize,
) -> String {
    plane_axis_world_value(axis, probe, pixel)
        .map(|value| format_plane_axis_world_value(axis, value))
        .unwrap_or_else(|| pixel.to_string())
}

impl AxisDisplayScale {
    fn display_at_pixel(&self, probe_pixel: f64, pixel: f64) -> f64 {
        self.probe_display + (pixel - probe_pixel) * self.increment_display
    }

    fn pixel_at_display(&self, probe_pixel: f64, display_value: f64) -> f64 {
        probe_pixel + (display_value - self.probe_display) / self.increment_display
    }

    fn nice_step(&self, span: f64) -> Option<f64> {
        let span = span.abs();
        if !span.is_finite() || span == 0.0 {
            return None;
        }
        let target = span / 4.0;
        match self.kind {
            AxisDisplayKind::RightAscensionSeconds | AxisDisplayKind::DeclinationArcseconds => {
                choose_preferred_step(
                    target,
                    &[
                        0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0,
                        900.0, 1800.0, 3600.0,
                    ],
                )
            }
            AxisDisplayKind::Frequency { .. } | AxisDisplayKind::Generic { .. } => {
                Some(nice_numeric_step(target))
            }
        }
    }

    fn label(&self, display_value: f64, step: f64) -> String {
        match &self.kind {
            AxisDisplayKind::RightAscensionSeconds => {
                let radians = display_value * std::f64::consts::TAU / 86_400.0;
                format_right_ascension(radians, decimals_for_step(step))
            }
            AxisDisplayKind::DeclinationArcseconds => {
                let radians = (display_value / 3_600.0).to_radians();
                format_declination(radians, decimals_for_step(step))
            }
            AxisDisplayKind::Frequency { unit, .. } => {
                format_spectrum_axis_numeric(display_value, unit, step)
            }
            AxisDisplayKind::Generic { unit } => {
                format_spectrum_axis_numeric(display_value, unit, step)
            }
        }
    }
}

fn plane_axis_scale(
    axis: &ImageDisplayAxisState,
    probe_world: f64,
    increment: f64,
) -> Option<AxisDisplayScale> {
    if is_right_ascension_axis(&axis.name) {
        return Some(AxisDisplayScale {
            kind: AxisDisplayKind::RightAscensionSeconds,
            probe_display: probe_world * 86_400.0 / std::f64::consts::TAU,
            increment_display: increment * 86_400.0 / std::f64::consts::TAU,
        });
    }
    if is_declination_axis(&axis.name) {
        let scale = 180.0 / std::f64::consts::PI * 3_600.0;
        return Some(AxisDisplayScale {
            kind: AxisDisplayKind::DeclinationArcseconds,
            probe_display: probe_world * scale,
            increment_display: increment * scale,
        });
    }
    if let Some((unit, scale)) = choose_frequency_display_scale(&axis.unit, probe_world, increment)
    {
        return Some(AxisDisplayScale {
            kind: AxisDisplayKind::Frequency { unit, scale },
            probe_display: probe_world / scale,
            increment_display: increment / scale,
        });
    }
    Some(AxisDisplayScale {
        kind: AxisDisplayKind::Generic {
            unit: axis.unit.clone(),
        },
        probe_display: probe_world,
        increment_display: increment,
    })
}

fn choose_preferred_step(target: f64, preferred: &[f64]) -> Option<f64> {
    preferred
        .iter()
        .copied()
        .find(|value| *value >= target)
        .or_else(|| preferred.last().copied())
}

fn nice_numeric_step(target: f64) -> f64 {
    if !target.is_finite() || target <= 0.0 {
        return 1.0;
    }
    let exponent = target.log10().floor();
    let base = 10f64.powf(exponent);
    let fraction = target / base;
    let nice_fraction = if fraction <= 1.0 {
        1.0
    } else if fraction <= 2.0 {
        2.0
    } else if fraction <= 5.0 {
        5.0
    } else {
        10.0
    };
    nice_fraction * base
}

fn decimals_for_step(step: f64) -> usize {
    if step >= 1.0 {
        0
    } else if step >= 0.1 {
        1
    } else if step >= 0.01 {
        2
    } else if step >= 0.001 {
        3
    } else {
        4
    }
}

fn plane_axis_world_value(
    axis: &ImageDisplayAxisState,
    probe: Option<&ImageBrowserProbe>,
    pixel: usize,
) -> Option<f64> {
    let increment = axis.world_increment?;
    let probe = probe?;
    let probe_world = probe.world_axes.get(axis.axis)?.value;
    let probe_pixel = *probe.pixel_indices.get(axis.axis)? as f64;
    Some(probe_world + (pixel as f64 - probe_pixel) * increment)
}

fn format_plane_axis_world_value(axis: &ImageDisplayAxisState, value: f64) -> String {
    if is_right_ascension_axis(&axis.name) {
        return format_right_ascension(value, 2);
    }
    if is_declination_axis(&axis.name) {
        return format_declination(value, 1);
    }
    if let Some(frequency) = format_frequency_quantity_auto(value, &axis.unit) {
        return frequency;
    }
    if axis.unit.is_empty() {
        trim_float_text(format!("{value:.4}"))
    } else {
        format!("{} {}", trim_float_text(format!("{value:.4}")), axis.unit)
    }
}

fn plane_axis_title(axis: &ImageDisplayAxisState) -> String {
    if is_right_ascension_axis(&axis.name) {
        return "Right Ascension".to_string();
    }
    if is_declination_axis(&axis.name) {
        return "Declination".to_string();
    }
    if axis.unit.is_empty() {
        axis.name.clone()
    } else if let Some(display_unit) = frequency_display_unit(&axis.unit) {
        format!("{} [{display_unit}]", axis.name)
    } else {
        format!("{} [{}]", axis.name, axis.unit)
    }
}

fn frequency_display_unit(unit: &str) -> Option<&'static str> {
    let hz = Unit::new("Hz").ok()?;
    let axis = Unit::new(unit).ok()?;
    axis.conformant(&hz).then_some("GHz")
}

fn choose_frequency_display_scale(
    unit: &str,
    reference_value: f64,
    increment: f64,
) -> Option<(&'static str, f64)> {
    let quantity = Quantity::new(reference_value, unit).ok()?;
    let increment_quantity = Quantity::new(increment, unit).ok()?;
    let hz = Unit::new("Hz").ok()?;
    if !quantity.unit().conformant(&hz) || !increment_quantity.unit().conformant(&hz) {
        return None;
    }
    let reference_hz = quantity.get_value_in(&hz).ok()?;
    let increment_hz = increment_quantity.get_value_in(&hz).ok()?;
    let max_abs_hz = reference_hz.abs().max((reference_hz + increment_hz).abs());
    let (unit, scale) = if max_abs_hz >= 1e9 {
        ("GHz", 1e9)
    } else if max_abs_hz >= 1e6 {
        ("MHz", 1e6)
    } else if max_abs_hz >= 1e3 {
        ("kHz", 1e3)
    } else {
        ("Hz", 1.0)
    };
    Some((unit, scale))
}

fn spectrum_x_scale(input: &ImageSpectrumRenderInput, x_domain: (f64, f64)) -> AxisDisplayScale {
    if let Some(first_axis) = input
        .profile
        .samples
        .iter()
        .find_map(|sample| sample.world_axis.as_ref())
        && let Some((unit, scale)) =
            choose_frequency_display_scale(&first_axis.unit, x_domain.0, x_domain.1 - x_domain.0)
    {
        return AxisDisplayScale {
            kind: AxisDisplayKind::Frequency { unit, scale },
            probe_display: x_domain.0 / scale,
            increment_display: (x_domain.1 - x_domain.0) / scale,
        };
    }
    AxisDisplayScale {
        kind: AxisDisplayKind::Generic {
            unit: input.profile.axis_unit.clone(),
        },
        probe_display: x_domain.0,
        increment_display: x_domain.1 - x_domain.0,
    }
}

fn spectrum_x_ticks(
    scale: &AxisDisplayScale,
    x_domain: (f64, f64),
    geometry_len: u32,
) -> Vec<SpectrumAxisTick> {
    let start_display = match scale.kind {
        AxisDisplayKind::Frequency { scale: factor, .. } => x_domain.0 / factor,
        _ => x_domain.0,
    };
    let end_display = match scale.kind {
        AxisDisplayKind::Frequency { scale: factor, .. } => x_domain.1 / factor,
        _ => x_domain.1,
    };
    let step = scale
        .nice_step((end_display - start_display).abs())
        .unwrap_or(1.0);
    numeric_ticks(
        start_display.min(end_display),
        start_display.max(end_display),
        step,
        geometry_len,
    )
    .into_iter()
    .map(|tick| SpectrumAxisTick {
        value: tick.value,
        label: scale.label(tick.value, step),
        position_px: tick.position_px,
    })
    .collect()
}

fn numeric_ticks(min: f64, max: f64, step: f64, geometry_len: u32) -> Vec<SpectrumAxisTick> {
    if !min.is_finite() || !max.is_finite() || !step.is_finite() || step <= 0.0 || geometry_len == 0
    {
        return Vec::new();
    }
    let first = (min / step).ceil() * step;
    let epsilon = step * 1e-6;
    let mut value = first;
    let mut ticks = Vec::new();
    while value <= max + epsilon {
        let position_px = if (max - min).abs() < f64::EPSILON {
            geometry_len / 2
        } else {
            (((value - min) / (max - min)).clamp(0.0, 1.0) * geometry_len.saturating_sub(1) as f64)
                .round() as u32
        };
        ticks.push(SpectrumAxisTick {
            value,
            label: String::new(),
            position_px,
        });
        value += step;
    }
    ticks
}

fn spectrum_x_axis_title(scale: &AxisDisplayScale, input: &ImageSpectrumRenderInput) -> String {
    match &scale.kind {
        AxisDisplayKind::Frequency { unit, .. } => format!("{} [{unit}]", input.profile.axis_name),
        AxisDisplayKind::Generic { unit } if unit.is_empty() => input.profile.axis_name.clone(),
        AxisDisplayKind::Generic { unit } => format!("{} [{unit}]", input.profile.axis_name),
        AxisDisplayKind::RightAscensionSeconds => format!("{} [hms]", input.profile.axis_name),
        AxisDisplayKind::DeclinationArcseconds => format!("{} [dms]", input.profile.axis_name),
    }
}

fn spectrum_y_axis_title(input: &ImageSpectrumRenderInput) -> String {
    if input.profile.value_unit.is_empty() {
        "Value".to_string()
    } else {
        format!("Value [{}]", input.profile.value_unit)
    }
}

fn spectrum_y_title_x(
    layout: ImageSpectrumLayout,
    tick_length: i32,
    tick_font_size: i32,
    axis_font_size: i32,
) -> i32 {
    let left_x = layout.plot.x as i32;
    let preferred = left_x - tick_length - tick_font_size - (axis_font_size / 2) - 8;
    preferred.max(axis_font_size + 6)
}

fn format_spectrum_axis_numeric(value: f64, _unit: &str, step: f64) -> String {
    let decimals = decimals_for_step(step);
    if value.abs() >= 1e4 || (value.abs() > 0.0 && value.abs() < 1e-2) {
        format!("{value:.1e}")
    } else {
        trim_float_text(format!("{value:.decimals$}"))
    }
}

fn spectrum_tick_font_size(geometry: ImagePlaneDrawGeometry) -> i32 {
    (geometry.width.min(geometry.height) / 14).clamp(16, 24) as i32
}

fn spectrum_axis_font_size(geometry: ImagePlaneDrawGeometry) -> i32 {
    (geometry.width.min(geometry.height) / 11).clamp(18, 28) as i32
}

fn spectrum_tick_length(geometry: ImagePlaneDrawGeometry) -> u32 {
    (geometry.width.min(geometry.height) / 18).clamp(8, 14)
}

fn format_frequency_quantity_auto(value: f64, unit: &str) -> Option<String> {
    let quantity = Quantity::new(value, unit).ok()?;
    let hz = Unit::new("Hz").ok()?;
    if !quantity.unit().conformant(&hz) {
        return None;
    }
    let abs_hz = quantity.get_value_in(&hz).ok()?.abs();
    let display_unit = if abs_hz >= 1e9 {
        "GHz"
    } else if abs_hz >= 1e6 {
        "MHz"
    } else if abs_hz >= 1e3 {
        "kHz"
    } else {
        "Hz"
    };
    let converted = quantity.get_value_in(&Unit::new(display_unit).ok()?).ok()?;
    Some(trim_float_text(format!("{converted:.4}"))).map(|value| format!("{value} {display_unit}"))
}

fn format_plane_value_axis_title(unit: &str) -> String {
    if unit.trim().is_empty() {
        "Intensity".into()
    } else {
        format!("Intensity [{unit}]")
    }
}

fn format_plane_sidebar_tick_value(value: f64, unit: &str) -> String {
    let _ = unit;
    if value == 0.0 {
        return "0".into();
    }
    if value.abs() >= 1e4 || value.abs() < 1e-3 {
        return format!("{value:.3e}");
    }
    trim_float_text(format!("{value:.4}"))
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct PlaneSidebarTick {
    offset: f64,
    value: f64,
}

fn plane_sidebar_value_for_offset(
    offset: u32,
    available_height: u32,
    data_min: f64,
    data_max: f64,
) -> Option<f64> {
    if !data_min.is_finite() || !data_max.is_finite() {
        return None;
    }
    if available_height <= 1 || (data_max - data_min).abs() <= f64::EPSILON {
        return Some(data_max);
    }
    let frac = (available_height.saturating_sub(1).saturating_sub(offset) as f64)
        / available_height.saturating_sub(1) as f64;
    Some(data_min + frac * (data_max - data_min))
}

fn plane_sidebar_y_for_value(
    value: f64,
    available_height: u32,
    data_min: f64,
    data_max: f64,
) -> Option<u32> {
    if !value.is_finite() || !data_min.is_finite() || !data_max.is_finite() {
        return None;
    }
    if available_height == 0 {
        return None;
    }
    if available_height == 1 || (data_max - data_min).abs() <= f64::EPSILON {
        return Some(0);
    }
    let scaled = ((data_max - value) / (data_max - data_min)).clamp(0.0, 1.0);
    Some(
        ((scaled * available_height.saturating_sub(1) as f64).round() as u32)
            .min(available_height.saturating_sub(1)),
    )
}

fn plane_scale_sample_for_value(value: f64, clip_min: f64, clip_max: f64) -> u8 {
    if !value.is_finite() {
        return 0;
    }
    if (clip_max - clip_min).abs() < f64::EPSILON {
        return 128;
    }
    let scaled = ((value - clip_min) / (clip_max - clip_min)).clamp(0.0, 1.0);
    (scaled * 255.0).round() as u8
}

fn plane_sidebar_ticks(
    clip_min: f64,
    clip_max: f64,
    available_height: u32,
) -> Vec<PlaneSidebarTick> {
    if !clip_min.is_finite() || !clip_max.is_finite() {
        return Vec::new();
    }
    if (clip_max - clip_min).abs() <= f64::EPSILON {
        return vec![PlaneSidebarTick {
            offset: 0.5,
            value: clip_min,
        }];
    }
    let tick_count: usize = if available_height >= 220 {
        5
    } else if available_height >= 150 {
        4
    } else {
        3
    };
    let range = (clip_max - clip_min).abs();
    let tick_intervals = if tick_count > 1 { tick_count - 1 } else { 1 };
    let target_step = range / tick_intervals as f64;
    let mut step = nice_linear_step(target_step);
    let mut ticks = nice_ticks_with_step(clip_min, clip_max, step);
    if ticks.len() < 2 {
        step /= 2.0;
        ticks = nice_ticks_with_step(clip_min, clip_max, step);
    }
    if ticks.is_empty() {
        return vec![
            PlaneSidebarTick {
                offset: 0.0,
                value: clip_max,
            },
            PlaneSidebarTick {
                offset: 1.0,
                value: clip_min,
            },
        ];
    }
    ticks
}

fn nice_ticks_with_step(clip_min: f64, clip_max: f64, step: f64) -> Vec<PlaneSidebarTick> {
    if !step.is_finite() || step <= 0.0 {
        return Vec::new();
    }
    let epsilon = step * 1.0e-6;
    let start = (clip_min / step).ceil() * step;
    let end = (clip_max / step).floor() * step;
    if start > end + epsilon {
        return Vec::new();
    }
    let mut ticks = Vec::new();
    let mut value = start;
    while value <= end + epsilon {
        let clamped = value.clamp(clip_min, clip_max);
        let offset = ((clip_max - clamped) / (clip_max - clip_min)).clamp(0.0, 1.0);
        ticks.push(PlaneSidebarTick {
            offset,
            value: normalize_nice_tick_value(clamped, step),
        });
        value += step;
    }
    ticks.sort_by(|left, right| left.offset.total_cmp(&right.offset));
    ticks
}

fn normalize_nice_tick_value(value: f64, step: f64) -> f64 {
    if step == 0.0 {
        return value;
    }
    let decimals = (-step.abs().log10().floor()).max(0.0) as i32 + 1;
    let scale = 10f64.powi(decimals);
    (value * scale).round() / scale
}

fn nice_linear_step(raw_step: f64) -> f64 {
    if !raw_step.is_finite() || raw_step <= 0.0 {
        return 1.0;
    }
    let exponent = raw_step.log10().floor();
    let base = 10f64.powf(exponent);
    let fraction = raw_step / base;
    let nice_fraction = if fraction <= 1.0 {
        1.0
    } else if fraction <= 2.0 {
        2.0
    } else if fraction <= 5.0 {
        5.0
    } else {
        10.0
    };
    nice_fraction * base
}

fn trim_float_text(mut text: String) -> String {
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    if text == "-0" { "0".into() } else { text }
}

fn is_right_ascension_axis(name: &str) -> bool {
    name.eq_ignore_ascii_case("Right Ascension") || name.eq_ignore_ascii_case("RA")
}

fn is_declination_axis(name: &str) -> bool {
    name.eq_ignore_ascii_case("Declination") || name.eq_ignore_ascii_case("DEC")
}

fn plane_tick_font_size(geometry: ImagePlaneDrawGeometry) -> i32 {
    (geometry.width.min(geometry.height) / 14).clamp(18, 28) as i32
}

fn plane_axis_font_size(geometry: ImagePlaneDrawGeometry) -> i32 {
    (geometry.width.min(geometry.height) / 10).clamp(22, 34) as i32
}

fn plane_tick_length(geometry: ImagePlaneDrawGeometry) -> u32 {
    (geometry.width.min(geometry.height) / 20).clamp(10, 18)
}

fn plane_scale_font_size(geometry: ImagePlaneDrawGeometry) -> i32 {
    plane_tick_font_size(geometry)
}

fn plane_scale_title_font_size(geometry: ImagePlaneDrawGeometry) -> i32 {
    (geometry.width.min(geometry.height) / 14).clamp(16, 24) as i32
}

fn image_browser_highlight(theme_mode: ThemeMode) -> [u8; 3] {
    match theme_mode {
        ThemeMode::DenseAnsi => [255, 208, 96],
        ThemeMode::RichPanel => [196, 64, 32],
    }
}

fn image_plane_pixel_color(value: u8, colormap: ImagePlaneColormap, invert: bool) -> [u8; 3] {
    let value = if invert {
        255u8.saturating_sub(value)
    } else {
        value
    };
    match colormap {
        ImagePlaneColormap::Grayscale => [value, value, value],
        ImagePlaneColormap::Viridis => interpolate_color_stops(
            value,
            &[
                [68, 1, 84],
                [59, 82, 139],
                [33, 145, 140],
                [94, 201, 98],
                [253, 231, 37],
            ],
        ),
        ImagePlaneColormap::Inferno => interpolate_color_stops(
            value,
            &[
                [0, 0, 4],
                [87, 15, 109],
                [187, 55, 84],
                [249, 142, 8],
                [252, 255, 164],
            ],
        ),
    }
}

fn interpolate_color_stops(value: u8, stops: &[[u8; 3]]) -> [u8; 3] {
    if stops.is_empty() {
        return [value, value, value];
    }
    if stops.len() == 1 {
        return stops[0];
    }
    let segment_count = stops.len() - 1;
    let scaled = usize::from(value) * segment_count * 256 / 255;
    let segment = (scaled / 256).min(segment_count - 1);
    let fraction = (scaled % 256) as u16;
    let start = stops[segment];
    let end = stops[segment + 1];
    [
        interpolate_channel(start[0], end[0], fraction),
        interpolate_channel(start[1], end[1], fraction),
        interpolate_channel(start[2], end[2], fraction),
    ]
}

fn interpolate_channel(start: u8, end: u8, fraction: u16) -> u8 {
    let start = u16::from(start);
    let end = u16::from(end);
    ((start * (256 - fraction) + end * fraction) / 256) as u8
}

fn image_probe_overlay_color(theme_mode: ThemeMode, color_index: usize) -> [u8; 3] {
    const DENSE: &[[u8; 3]] = &[
        [255, 96, 96],
        [255, 224, 64],
        [255, 96, 220],
        [128, 255, 96],
        [64, 224, 255],
        [176, 128, 255],
    ];
    const RICH: &[[u8; 3]] = &[
        [239, 68, 68],
        [245, 158, 11],
        [217, 70, 239],
        [132, 204, 22],
        [34, 211, 238],
        [139, 92, 246],
    ];
    let palette = match theme_mode {
        ThemeMode::DenseAnsi => DENSE,
        ThemeMode::RichPanel => RICH,
    };
    palette[color_index % palette.len()]
}

fn image_region_overlay_color(theme_mode: ThemeMode) -> [u8; 3] {
    match theme_mode {
        ThemeMode::DenseAnsi => [255, 255, 0],
        ThemeMode::RichPanel => [255, 82, 82],
    }
}

fn plane_cursor_marker_geometry(
    geometry: ImagePlaneDrawGeometry,
    raster_width: u32,
    raster_height: u32,
    cursor_x: u32,
    cursor_y: u32,
) -> ImagePlaneDrawGeometry {
    let start_x = geometry.x
        + ((u64::from(cursor_x) * u64::from(geometry.width)) / u64::from(raster_width.max(1)))
            as u32;
    let next_x = geometry.x
        + ((u64::from(cursor_x.saturating_add(1)) * u64::from(geometry.width))
            / u64::from(raster_width.max(1))) as u32;
    let start_y = geometry.y
        + ((u64::from(cursor_y) * u64::from(geometry.height)) / u64::from(raster_height.max(1)))
            as u32;
    let next_y = geometry.y
        + ((u64::from(cursor_y.saturating_add(1)) * u64::from(geometry.height))
            / u64::from(raster_height.max(1))) as u32;

    ImagePlaneDrawGeometry {
        x: start_x.min(geometry.x + geometry.width.saturating_sub(1)),
        y: start_y.min(geometry.y + geometry.height.saturating_sub(1)),
        width: next_x.saturating_sub(start_x).max(1),
        height: next_y.saturating_sub(start_y).max(1),
    }
}

fn sampled_coordinate_to_screen_x(
    geometry: ImagePlaneDrawGeometry,
    sampled_width: u32,
    sampled_x: f64,
) -> u32 {
    let width = sampled_width.max(1) as f64;
    let normalized = ((sampled_x + 0.5) / width).clamp(0.0, 1.0);
    (geometry.x as f64 + normalized * geometry.width.max(1) as f64)
        .round()
        .clamp(
            geometry.x as f64,
            (geometry.x + geometry.width.saturating_sub(1)) as f64,
        ) as u32
}

fn sampled_coordinate_to_screen_y(
    geometry: ImagePlaneDrawGeometry,
    sampled_height: u32,
    sampled_y: f64,
) -> u32 {
    let height = sampled_height.max(1) as f64;
    let normalized = ((sampled_y + 0.5) / height).clamp(0.0, 1.0);
    (geometry.y as f64 + normalized * geometry.height.max(1) as f64)
        .round()
        .clamp(
            geometry.y as f64,
            (geometry.y + geometry.height.saturating_sub(1)) as f64,
        ) as u32
}

fn probe_marker_radius(geometry: ImagePlaneDrawGeometry) -> u32 {
    (geometry.width.min(geometry.height) / 72).clamp(4, 7)
}

fn draw_probe_marker(
    image: &mut RgbImage,
    center_x: u32,
    center_y: u32,
    radius: u32,
    halo: Rgb<u8>,
    color: Rgb<u8>,
) {
    let image_width = image.width();
    let image_height = image.height();
    let outer_x = center_x.saturating_sub(radius + 1);
    let outer_y = center_y.saturating_sub(radius + 1);
    let outer_width = (radius * 2 + 3).min(image_width.saturating_sub(outer_x));
    let outer_height = (radius * 2 + 3).min(image_height.saturating_sub(outer_y));
    draw_rect_outline(image, outer_x, outer_y, outer_width, outer_height, halo);

    let inner_x = center_x.saturating_sub(radius);
    let inner_y = center_y.saturating_sub(radius);
    let inner_width = (radius * 2 + 1).min(image_width.saturating_sub(inner_x));
    let inner_height = (radius * 2 + 1).min(image_height.saturating_sub(inner_y));
    draw_rect_outline(image, inner_x, inner_y, inner_width, inner_height, color);

    let tick_radius = radius.saturating_sub(1).max(2);
    draw_cross(image, center_x, center_y, tick_radius, halo);
    draw_cross(
        image,
        center_x,
        center_y,
        tick_radius.saturating_sub(1),
        color,
    );
}

fn draw_region_segment(
    image: &mut RgbImage,
    start: (u32, u32),
    end: (u32, u32),
    halo: Rgb<u8>,
    color: Rgb<u8>,
) {
    for (dx, dy) in [(-1i32, 0i32), (1, 0), (0, -1), (0, 1)] {
        draw_line(
            image,
            offset_coord(start.0, dx),
            offset_coord(start.1, dy),
            offset_coord(end.0, dx),
            offset_coord(end.1, dy),
            halo,
        );
    }
    draw_line(image, start.0, start.1, end.0, end.1, halo);
    draw_line(image, start.0, start.1, end.0, end.1, color);
}

fn offset_coord(value: u32, delta: i32) -> u32 {
    if delta.is_negative() {
        value.saturating_sub(delta.unsigned_abs())
    } else {
        value.saturating_add(delta as u32)
    }
}

fn draw_cursor_guides(
    image: &mut RgbImage,
    geometry: ImagePlaneDrawGeometry,
    center_x: u32,
    center_y: u32,
    radius: u32,
    halo: Rgb<u8>,
    highlight: Rgb<u8>,
) {
    let left = geometry.x;
    let right = geometry.x + geometry.width.saturating_sub(1);
    let top = geometry.y;
    let bottom = geometry.y + geometry.height.saturating_sub(1);

    for offset in [1u32, 2u32] {
        draw_horizontal_line(image, left, right, center_y.saturating_sub(offset), halo);
        draw_horizontal_line(
            image,
            left,
            right,
            (center_y + offset).min(image.height().saturating_sub(1)),
            halo,
        );
        draw_vertical_line(image, center_x.saturating_sub(offset), top, bottom, halo);
        draw_vertical_line(
            image,
            (center_x + offset).min(image.width().saturating_sub(1)),
            top,
            bottom,
            halo,
        );
    }

    draw_horizontal_line(image, left, right, center_y, highlight);
    draw_vertical_line(image, center_x, top, bottom, highlight);

    let arm_left = center_x.saturating_sub(radius);
    let arm_right = (center_x + radius).min(right);
    let arm_top = center_y.saturating_sub(radius);
    let arm_bottom = (center_y + radius).min(bottom);
    draw_horizontal_line(image, arm_left, arm_right, center_y, halo);
    draw_vertical_line(image, center_x, arm_top, arm_bottom, halo);
    draw_horizontal_line(
        image,
        arm_left.saturating_add(1),
        arm_right.saturating_sub(1),
        center_y,
        highlight,
    );
    draw_vertical_line(
        image,
        center_x,
        arm_top.saturating_add(1),
        arm_bottom.saturating_sub(1),
        highlight,
    );
}

fn spectrum_x_domain(input: &ImageSpectrumRenderInput) -> (f64, f64) {
    let mut values = input
        .overlay_profiles
        .iter()
        .map(|overlay| &overlay.profile)
        .chain(std::iter::once(&input.profile))
        .flat_map(|profile| profile.samples.iter())
        .filter_map(|sample| {
            sample
                .world_axis
                .as_ref()
                .map(|axis| axis.value)
                .filter(|value| value.is_finite())
        })
        .collect::<Vec<_>>();
    if values.len() >= 2 {
        values.sort_by(|left, right| left.total_cmp(right));
        let min = values[0];
        let max = values[values.len().saturating_sub(1)];
        if (max - min).abs() >= f64::EPSILON {
            return (min, max);
        }
    }
    let max_len = input
        .overlay_profiles
        .iter()
        .map(|overlay| overlay.profile.samples.len())
        .chain(std::iter::once(input.profile.samples.len()))
        .max()
        .unwrap_or(1);
    (0.0, max_len.saturating_sub(1) as f64)
}

fn spectrum_value_range(input: &ImageSpectrumRenderInput) -> Option<(f64, f64)> {
    let mut min_value = f64::INFINITY;
    let mut max_value = f64::NEG_INFINITY;
    let mut found = false;
    for sample in input
        .overlay_profiles
        .iter()
        .map(|overlay| &overlay.profile)
        .chain(std::iter::once(&input.profile))
        .flat_map(|profile| profile.samples.iter())
        .filter(|sample| !sample.masked && sample.finite && sample.value.is_finite())
    {
        min_value = min_value.min(sample.value);
        max_value = max_value.max(sample.value);
        found = true;
    }
    found.then_some((min_value, max_value))
}

#[allow(clippy::too_many_arguments)]
fn draw_profile_series(
    image: &mut RgbImage,
    profile: &ImageProfilePayload,
    plot_left: u32,
    plot_top: u32,
    plot_width: u32,
    plot_height: u32,
    x_domain: (f64, f64),
    min_value: f64,
    max_value: f64,
    color: Rgb<u8>,
) {
    let mut previous = None::<(u32, u32)>;
    for sample in &profile.samples {
        if sample.masked || !sample.finite || !sample.value.is_finite() {
            previous = None;
            continue;
        }
        let x_value = profile_sample_x_value(sample);
        let point = (
            plot_value_x(x_value, x_domain.0, x_domain.1, plot_left, plot_width),
            plot_value_y(sample.value, min_value, max_value, plot_top, plot_height),
        );
        if let Some(prev) = previous {
            draw_line(image, prev.0, prev.1, point.0, point.1, color);
        }
        previous = Some(point);
    }
    if let Some(selected) = profile
        .samples
        .get(profile.selected_sample_index)
        .filter(|sample| !sample.masked && sample.finite && sample.value.is_finite())
    {
        let x_value = profile_sample_x_value(selected);
        let marker_x = plot_value_x(x_value, x_domain.0, x_domain.1, plot_left, plot_width);
        let marker_y = plot_value_y(selected.value, min_value, max_value, plot_top, plot_height);
        draw_cross(image, marker_x, marker_y, 1, color);
    }
}

fn profile_sample_x_value(sample: &casacore_imagebrowser_protocol::ImageProfileSampleState) -> f64 {
    sample
        .world_axis
        .as_ref()
        .map(|axis| axis.value)
        .filter(|value| value.is_finite())
        .unwrap_or(sample.sample_index as f64)
}

fn plot_value_x(value: f64, min: f64, max: f64, left: u32, width: u32) -> u32 {
    if width <= 1 || (max - min).abs() < f64::EPSILON {
        return left;
    }
    let relative = ((value - min) / (max - min)).clamp(0.0, 1.0);
    left + (relative * (width.saturating_sub(1)) as f64).round() as u32
}

fn plot_value_y(value: f64, min: f64, max: f64, top: u32, height: u32) -> u32 {
    if height <= 1 || (max - min).abs() < f64::EPSILON {
        return top;
    }
    let relative = ((max - value) / (max - min)).clamp(0.0, 1.0);
    top + (relative * (height.saturating_sub(1)) as f64).round() as u32
}

fn fill_rect(image: &mut RgbImage, x: u32, y: u32, width: u32, height: u32, color: Rgb<u8>) {
    for py in y..y.saturating_add(height) {
        for px in x..x.saturating_add(width) {
            image.put_pixel(px, py, color);
        }
    }
}

fn draw_rect_outline(
    image: &mut RgbImage,
    x: u32,
    y: u32,
    width: u32,
    height: u32,
    color: Rgb<u8>,
) {
    if width == 0 || height == 0 {
        return;
    }
    draw_horizontal_line(image, x, x + width.saturating_sub(1), y, color);
    draw_horizontal_line(
        image,
        x,
        x + width.saturating_sub(1),
        y + height.saturating_sub(1),
        color,
    );
    draw_vertical_line(image, x, y, y + height.saturating_sub(1), color);
    draw_vertical_line(
        image,
        x + width.saturating_sub(1),
        y,
        y + height.saturating_sub(1),
        color,
    );
}

fn draw_horizontal_line(image: &mut RgbImage, x0: u32, x1: u32, y: u32, color: Rgb<u8>) {
    let y = y.min(image.height().saturating_sub(1));
    for x in x0.min(x1)..=x0.max(x1).min(image.width().saturating_sub(1)) {
        image.put_pixel(x, y, color);
    }
}

fn draw_vertical_line(image: &mut RgbImage, x: u32, y0: u32, y1: u32, color: Rgb<u8>) {
    let x = x.min(image.width().saturating_sub(1));
    for y in y0.min(y1)..=y0.max(y1).min(image.height().saturating_sub(1)) {
        image.put_pixel(x, y, color);
    }
}

fn draw_cross(image: &mut RgbImage, x: u32, y: u32, radius: u32, color: Rgb<u8>) {
    let x = x.min(image.width().saturating_sub(1));
    let y = y.min(image.height().saturating_sub(1));
    for dx in 0..=radius {
        image.put_pixel(x.saturating_sub(dx), y, color);
        image.put_pixel((x + dx).min(image.width().saturating_sub(1)), y, color);
    }
    for dy in 0..=radius {
        image.put_pixel(x, y.saturating_sub(dy), color);
        image.put_pixel(x, (y + dy).min(image.height().saturating_sub(1)), color);
    }
}

fn draw_line(image: &mut RgbImage, x0: u32, y0: u32, x1: u32, y1: u32, color: Rgb<u8>) {
    let mut x0 = x0 as i32;
    let mut y0 = y0 as i32;
    let x1 = x1 as i32;
    let y1 = y1 as i32;
    let dx = (x1 - x0).abs();
    let sx = if x0 < x1 { 1 } else { -1 };
    let dy = -(y1 - y0).abs();
    let sy = if y0 < y1 { 1 } else { -1 };
    let mut error = dx + dy;

    loop {
        if x0 >= 0 && y0 >= 0 && (x0 as u32) < image.width() && (y0 as u32) < image.height() {
            image.put_pixel(x0 as u32, y0 as u32, color);
        }
        if x0 == x1 && y0 == y1 {
            break;
        }
        let twice_error = 2 * error;
        if twice_error >= dy {
            error += dy;
            x0 += sx;
        }
        if twice_error <= dx {
            error += dx;
            y0 += sy;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ImagePlaneColormap, ImagePlaneDrawGeometry, ImagePlaneOverlayMarker, ImagePlaneRenderInput,
        ImageSpectrumOverlaySeries, ImageSpectrumRenderInput, image_plane_draw_geometry,
        image_plane_layout, image_spectrum_layout, nice_linear_step, numeric_ticks,
        plane_axis_tick_label, plane_scale_sample_for_value, plane_sidebar_ticks,
        plane_sidebar_y_for_value, plot_theme, plot_value_y, render_image_plane_image,
        render_image_spectrum_image, spectrum_value_range, spectrum_y_title_x,
    };
    use crate::config::ThemeMode;
    use casacore_imagebrowser_protocol::{
        ImageBrowserAxisValue, ImageBrowserProbe, ImageDisplayAxisState, ImagePlaneRaster,
        ImageProfilePayload, ImageProfileSampleState,
    };

    #[test]
    fn theme_mapping_uses_distinct_backgrounds() {
        assert_ne!(
            plot_theme(ThemeMode::DenseAnsi).background,
            plot_theme(ThemeMode::RichPanel).background
        );
    }

    #[test]
    fn image_plane_render_highlights_selected_cell() {
        let image = render_image_plane_image(
            8,
            8,
            &ImagePlaneRenderInput {
                cache_key: 0,
                raster: ImagePlaneRaster {
                    width: 2,
                    height: 2,
                    pixels_u8: vec![0, 64, 128, 255],
                    clip_min: 0.0,
                    clip_max: 1.0,
                    data_min: 0.0,
                    data_max: 1.0,
                    value_unit: "Jy/beam".into(),
                    histogram_bins: vec![1, 1, 1, 1],
                    masked_or_non_finite_count: 0,
                    no_finite_values: false,
                },
                cursor_sample: Some((1, 1)),
                sampled_shape: Some((2, 2)),
                display_axes: Vec::new(),
                probe: None,
                overlay_markers: Vec::new(),
                region_overlay_shapes: Vec::new(),
                display_aspect_ratio: None,
                colormap: ImagePlaneColormap::Grayscale,
                invert: false,
                theme_mode: ThemeMode::DenseAnsi,
            },
        )
        .expect("render image plane");
        let rgb = image.to_rgb8();
        assert_eq!(rgb.get_pixel(5, 5).0, [0, 0, 0]);
        assert_eq!(rgb.get_pixel(5, 2).0, [255, 208, 96]);
        assert_eq!(rgb.get_pixel(2, 5).0, [255, 208, 96]);
    }

    #[test]
    fn image_plane_draw_geometry_letterboxes_wider_canvas() {
        assert_eq!(
            image_plane_draw_geometry(10, 10, Some(2.0)),
            ImagePlaneDrawGeometry {
                x: 0,
                y: 2,
                width: 10,
                height: 5,
            }
        );
        assert_eq!(
            image_plane_draw_geometry(12, 6, Some(1.0)),
            ImagePlaneDrawGeometry {
                x: 3,
                y: 0,
                width: 6,
                height: 6,
            }
        );
    }

    #[test]
    fn image_plane_render_preserves_requested_aspect_ratio() {
        let image = render_image_plane_image(
            10,
            10,
            &ImagePlaneRenderInput {
                cache_key: 0,
                raster: ImagePlaneRaster {
                    width: 2,
                    height: 2,
                    pixels_u8: vec![255, 255, 255, 255],
                    clip_min: 0.0,
                    clip_max: 1.0,
                    data_min: 0.0,
                    data_max: 1.0,
                    value_unit: "Jy/beam".into(),
                    histogram_bins: vec![0, 4, 0, 0],
                    masked_or_non_finite_count: 0,
                    no_finite_values: false,
                },
                cursor_sample: None,
                sampled_shape: None,
                display_axes: Vec::new(),
                probe: None,
                overlay_markers: Vec::new(),
                region_overlay_shapes: Vec::new(),
                display_aspect_ratio: Some(2.0),
                colormap: ImagePlaneColormap::Grayscale,
                invert: false,
                theme_mode: ThemeMode::DenseAnsi,
            },
        )
        .expect("render image plane");
        let rgb = image.to_rgb8();
        assert_eq!(rgb.get_pixel(5, 0).0, [0, 0, 0]);
        assert_eq!(rgb.get_pixel(5, 2).0, [255, 255, 255]);
        assert_eq!(rgb.get_pixel(5, 7).0, [0, 0, 0]);
    }

    #[test]
    fn image_plane_render_applies_selected_colormap() {
        let image = render_image_plane_image(
            4,
            4,
            &ImagePlaneRenderInput {
                cache_key: 0,
                raster: ImagePlaneRaster {
                    width: 1,
                    height: 1,
                    pixels_u8: vec![128],
                    clip_min: 0.0,
                    clip_max: 1.0,
                    data_min: 0.0,
                    data_max: 1.0,
                    value_unit: "Jy/beam".into(),
                    histogram_bins: vec![0, 1, 0, 0],
                    masked_or_non_finite_count: 0,
                    no_finite_values: false,
                },
                cursor_sample: None,
                sampled_shape: None,
                display_axes: Vec::new(),
                probe: None,
                overlay_markers: Vec::new(),
                region_overlay_shapes: Vec::new(),
                display_aspect_ratio: None,
                colormap: ImagePlaneColormap::Viridis,
                invert: false,
                theme_mode: ThemeMode::DenseAnsi,
            },
        )
        .expect("render image plane");
        let rgb = image.to_rgb8();
        assert_ne!(rgb.get_pixel(1, 1).0, [128, 128, 128]);
    }

    #[test]
    fn image_plane_render_invert_flips_grayscale_values() {
        let image = render_image_plane_image(
            2,
            2,
            &ImagePlaneRenderInput {
                cache_key: 0,
                raster: ImagePlaneRaster {
                    width: 1,
                    height: 1,
                    pixels_u8: vec![0],
                    clip_min: 0.0,
                    clip_max: 1.0,
                    data_min: 0.0,
                    data_max: 1.0,
                    value_unit: "Jy/beam".into(),
                    histogram_bins: vec![1, 0, 0, 0],
                    masked_or_non_finite_count: 0,
                    no_finite_values: false,
                },
                cursor_sample: None,
                sampled_shape: None,
                display_axes: Vec::new(),
                probe: None,
                overlay_markers: Vec::new(),
                region_overlay_shapes: Vec::new(),
                display_aspect_ratio: None,
                colormap: ImagePlaneColormap::Grayscale,
                invert: true,
                theme_mode: ThemeMode::DenseAnsi,
            },
        )
        .expect("render image plane");
        let rgb = image.to_rgb8();
        assert_eq!(rgb.get_pixel(0, 0).0, [255, 255, 255]);
    }

    #[test]
    fn image_spectrum_render_highlights_selected_sample() {
        let image = render_image_spectrum_image(
            32,
            16,
            &ImageSpectrumRenderInput {
                cache_key: 0,
                profile: ImageProfilePayload {
                    axis: 2,
                    axis_name: "Frequency".into(),
                    axis_unit: "Hz".into(),
                    value_unit: "Jy/beam".into(),
                    coord_type: "Spectral".into(),
                    selected_sample_index: 1,
                    samples: vec![
                        ImageProfileSampleState {
                            sample_index: 0,
                            pixel_index: 0,
                            value: 1.0,
                            masked: false,
                            finite: true,
                            world_axis: Some(ImageBrowserAxisValue {
                                name: "Frequency".into(),
                                unit: "Hz".into(),
                                value: 1.0,
                            }),
                        },
                        ImageProfileSampleState {
                            sample_index: 1,
                            pixel_index: 1,
                            value: 4.0,
                            masked: false,
                            finite: true,
                            world_axis: Some(ImageBrowserAxisValue {
                                name: "Frequency".into(),
                                unit: "Hz".into(),
                                value: 2.0,
                            }),
                        },
                        ImageProfileSampleState {
                            sample_index: 2,
                            pixel_index: 2,
                            value: 2.0,
                            masked: false,
                            finite: true,
                            world_axis: Some(ImageBrowserAxisValue {
                                name: "Frequency".into(),
                                unit: "Hz".into(),
                                value: 3.0,
                            }),
                        },
                    ],
                },
                overlay_profiles: Vec::new(),
                theme_mode: ThemeMode::DenseAnsi,
            },
        )
        .expect("render image spectrum");
        let rgb = image.to_rgb8();
        assert!(rgb.pixels().any(|pixel| pixel.0 == [255, 208, 96]));
        let guide_pixels = rgb
            .pixels()
            .filter(|pixel| pixel.0 == [255, 208, 96])
            .count();
        assert!(guide_pixels >= 3);
    }

    #[test]
    fn image_plane_render_draws_overlay_markers() {
        let image = render_image_plane_image(
            16,
            16,
            &ImagePlaneRenderInput {
                cache_key: 0,
                raster: ImagePlaneRaster {
                    width: 4,
                    height: 4,
                    pixels_u8: vec![96; 16],
                    clip_min: 0.0,
                    clip_max: 1.0,
                    data_min: 0.0,
                    data_max: 1.0,
                    value_unit: "Jy/beam".into(),
                    histogram_bins: vec![0, 16, 0, 0],
                    masked_or_non_finite_count: 0,
                    no_finite_values: false,
                },
                cursor_sample: None,
                sampled_shape: Some((4, 4)),
                display_axes: Vec::new(),
                probe: None,
                overlay_markers: vec![ImagePlaneOverlayMarker {
                    sample: (1, 2),
                    color_index: 1,
                }],
                region_overlay_shapes: Vec::new(),
                display_aspect_ratio: None,
                colormap: ImagePlaneColormap::Grayscale,
                invert: false,
                theme_mode: ThemeMode::DenseAnsi,
            },
        )
        .expect("render image plane");
        let rgb = image.to_rgb8();
        let marker_pixels = rgb
            .pixels()
            .filter(|pixel| pixel.0 == [255, 224, 64])
            .count();
        assert!(marker_pixels >= 8);
    }

    #[test]
    fn image_plane_layout_reserves_axis_gutters_when_annotations_are_enabled() {
        let layout = image_plane_layout(320, 220, Some(1.0), true);
        assert!(layout.left_gutter > 0);
        assert!(layout.bottom_gutter > 0);
        assert!(layout.image.x >= layout.left_gutter);
        assert!(layout.image.y >= layout.top_gutter);
    }

    #[test]
    fn plane_axis_tick_label_formats_direction_axes_as_astronomical_values() {
        let axis = ImageDisplayAxisState {
            axis: 0,
            name: "Right Ascension".into(),
            unit: "rad".into(),
            blc: 0,
            trc: 255,
            inc: 1,
            sampled_len: 256,
            world_increment: Some(-1.0e-4),
        };
        let probe = ImageBrowserProbe {
            pixel_indices: vec![128, 128],
            pixel_axes: vec![
                ImageBrowserAxisValue {
                    name: "Right Ascension".into(),
                    unit: "px".into(),
                    value: 128.0,
                },
                ImageBrowserAxisValue {
                    name: "Declination".into(),
                    unit: "px".into(),
                    value: 128.0,
                },
            ],
            value: 1.0,
            masked: false,
            finite: true,
            world_axes: vec![
                ImageBrowserAxisValue {
                    name: "Right Ascension".into(),
                    unit: "rad".into(),
                    value: 3.0,
                },
                ImageBrowserAxisValue {
                    name: "Declination".into(),
                    unit: "rad".into(),
                    value: 0.5,
                },
            ],
        };
        let label = plane_axis_tick_label(&axis, Some(&probe), 128);
        assert!(label.contains(':'));
        assert!(!label.contains("rad"));
    }

    #[test]
    fn image_plane_render_draws_axis_annotations_in_reserved_gutters() {
        let axis_state = |axis: usize, name: &str, unit: &str| ImageDisplayAxisState {
            axis,
            name: name.into(),
            unit: unit.into(),
            blc: 0,
            trc: 255,
            inc: 1,
            sampled_len: 256,
            world_increment: Some(if axis == 0 { -1.0e-4 } else { 1.0e-4 }),
        };
        let probe = ImageBrowserProbe {
            pixel_indices: vec![128, 128],
            pixel_axes: vec![
                ImageBrowserAxisValue {
                    name: "Right Ascension".into(),
                    unit: "px".into(),
                    value: 128.0,
                },
                ImageBrowserAxisValue {
                    name: "Declination".into(),
                    unit: "px".into(),
                    value: 128.0,
                },
            ],
            value: 1.0,
            masked: false,
            finite: true,
            world_axes: vec![
                ImageBrowserAxisValue {
                    name: "Right Ascension".into(),
                    unit: "rad".into(),
                    value: 3.0,
                },
                ImageBrowserAxisValue {
                    name: "Declination".into(),
                    unit: "rad".into(),
                    value: 0.5,
                },
            ],
        };
        let image = render_image_plane_image(
            320,
            220,
            &ImagePlaneRenderInput {
                cache_key: 0,
                raster: ImagePlaneRaster {
                    width: 32,
                    height: 32,
                    pixels_u8: vec![128; 32 * 32],
                    clip_min: 0.0,
                    clip_max: 1.0,
                    data_min: 0.0,
                    data_max: 1.0,
                    value_unit: "Jy/beam".into(),
                    histogram_bins: vec![0, 32, 0, 0],
                    masked_or_non_finite_count: 0,
                    no_finite_values: false,
                },
                cursor_sample: None,
                sampled_shape: Some((32, 32)),
                display_axes: vec![
                    axis_state(0, "Right Ascension", "rad"),
                    axis_state(1, "Declination", "rad"),
                ],
                probe: Some(probe),
                overlay_markers: Vec::new(),
                region_overlay_shapes: Vec::new(),
                display_aspect_ratio: Some(1.0),
                colormap: ImagePlaneColormap::Grayscale,
                invert: false,
                theme_mode: ThemeMode::DenseAnsi,
            },
        )
        .expect("render image plane");
        let rgb = image.to_rgb8();
        let layout = image_plane_layout(320, 220, Some(1.0), true);
        let gutter_pixels = rgb
            .enumerate_pixels()
            .filter(|(x, y, pixel)| {
                (*x) < layout.image.x
                    || (*y) >= layout.image.y + layout.image.height
                    || pixel.0 != [0, 0, 0]
            })
            .count();
        assert!(gutter_pixels > 0);
    }

    #[test]
    fn image_plane_render_draws_scale_sidebar_in_right_gutter() {
        let image = render_image_plane_image(
            320,
            220,
            &ImagePlaneRenderInput {
                cache_key: 0,
                raster: ImagePlaneRaster {
                    width: 32,
                    height: 32,
                    pixels_u8: (0..32 * 32).map(|value| (value % 256) as u8).collect(),
                    clip_min: 1.0,
                    clip_max: 4.0,
                    data_min: 0.0,
                    data_max: 5.0,
                    value_unit: "Jy/beam".into(),
                    histogram_bins: vec![1, 3, 6, 9, 6, 3, 1],
                    masked_or_non_finite_count: 0,
                    no_finite_values: false,
                },
                cursor_sample: None,
                sampled_shape: Some((32, 32)),
                display_axes: vec![
                    ImageDisplayAxisState {
                        axis: 0,
                        name: "Right Ascension".into(),
                        unit: "rad".into(),
                        blc: 0,
                        trc: 31,
                        inc: 1,
                        sampled_len: 32,
                        world_increment: Some(-1.0e-4),
                    },
                    ImageDisplayAxisState {
                        axis: 1,
                        name: "Declination".into(),
                        unit: "rad".into(),
                        blc: 0,
                        trc: 31,
                        inc: 1,
                        sampled_len: 32,
                        world_increment: Some(1.0e-4),
                    },
                ],
                probe: Some(ImageBrowserProbe {
                    pixel_indices: vec![16, 16],
                    pixel_axes: vec![],
                    value: 2.5,
                    masked: false,
                    finite: true,
                    world_axes: vec![
                        ImageBrowserAxisValue {
                            name: "Right Ascension".into(),
                            unit: "rad".into(),
                            value: 3.0,
                        },
                        ImageBrowserAxisValue {
                            name: "Declination".into(),
                            unit: "rad".into(),
                            value: 0.5,
                        },
                    ],
                }),
                overlay_markers: Vec::new(),
                region_overlay_shapes: Vec::new(),
                display_aspect_ratio: Some(1.0),
                colormap: ImagePlaneColormap::Viridis,
                invert: false,
                theme_mode: ThemeMode::DenseAnsi,
            },
        )
        .expect("render image plane");
        let rgb = image.to_rgb8();
        let layout = image_plane_layout(320, 220, Some(1.0), true);
        let sidebar_pixels = rgb
            .enumerate_pixels()
            .filter(|(x, y, pixel)| {
                *x >= layout.image.x + layout.image.width
                    && *y >= layout.image.y
                    && *y < layout.image.y + layout.image.height
                    && pixel.0 != [0, 0, 0]
            })
            .count();
        assert!(sidebar_pixels > 0);
        let label_pixels = rgb
            .enumerate_pixels()
            .filter(|(x, y, pixel)| {
                *x >= layout.image.x + layout.image.width + 24
                    && *x < rgb.width().saturating_sub(8)
                    && *y >= layout.image.y + 8
                    && *y < layout.image.y + layout.image.height.saturating_sub(8)
                    && pixel.0 != [0, 0, 0]
            })
            .count();
        assert!(label_pixels > 0);
        let histogram_unique_colors = rgb
            .enumerate_pixels()
            .filter(|(x, y, pixel)| {
                *x >= layout.image.x + layout.image.width + 24
                    && *x < rgb.width().saturating_sub(56)
                    && *y >= layout.image.y + 8
                    && *y < layout.image.y + layout.image.height.saturating_sub(8)
                    && pixel.0 != [0, 0, 0]
            })
            .map(|(_, _, pixel)| pixel.0)
            .collect::<std::collections::BTreeSet<_>>();
        assert!(histogram_unique_colors.len() > 4);
    }

    #[test]
    fn plane_sidebar_ticks_cover_clip_range_with_intermediate_labels() {
        let ticks = plane_sidebar_ticks(-2.0, 8.0, 220);
        let values: Vec<f64> = ticks.iter().map(|tick| tick.value).collect();
        assert_eq!(values, vec![5.0, 0.0]);
        assert!(ticks.first().expect("top tick").offset > 0.0);
        assert!(ticks.last().expect("bottom tick").offset < 1.0);
        assert!(ticks[0].offset < ticks[1].offset);
    }

    #[test]
    fn plane_sidebar_ticks_choose_nice_values() {
        let ticks = plane_sidebar_ticks(0.0, 0.017, 220);
        let values: Vec<f64> = ticks.iter().map(|tick| tick.value).collect();
        assert_eq!(nice_linear_step(0.00425), 0.005);
        assert!(values.contains(&0.0));
        assert!(values.contains(&0.005));
        assert!(values.contains(&0.01));
        assert!(values.contains(&0.015));
    }

    #[test]
    fn plane_sidebar_mapping_uses_full_data_range_with_clip_saturation() {
        assert_eq!(
            plane_sidebar_y_for_value(
                1.1713570356369019,
                200,
                -0.41392940282821655,
                1.1713570356369019
            ),
            Some(0)
        );
        assert_eq!(
            plane_sidebar_y_for_value(
                -0.41392940282821655,
                200,
                -0.41392940282821655,
                1.1713570356369019
            ),
            Some(199)
        );
        assert_eq!(
            plane_scale_sample_for_value(0.841566, -0.2428683042526245, 0.3157331645488739),
            255
        );
        assert_eq!(
            plane_scale_sample_for_value(
                -0.41392940282821655,
                -0.2428683042526245,
                0.3157331645488739
            ),
            0
        );
    }

    #[test]
    fn spectrum_y_axis_ticks_use_plot_value_mapping() {
        let ticks = numeric_ticks(-1.0, 1.0, 0.5, 100);
        let mapped = ticks
            .into_iter()
            .map(|tick| (tick.value, plot_value_y(tick.value, -1.0, 1.0, 0, 100)))
            .collect::<Vec<_>>();
        assert!(mapped.iter().any(|(value, y)| *value == 1.0 && *y == 0));
        assert!(mapped.iter().any(|(value, y)| *value == -1.0 && *y == 99));
        let zero = mapped
            .iter()
            .find(|(value, _)| *value == 0.0)
            .expect("zero tick");
        assert!(zero.1 >= 49 && zero.1 <= 50);
    }

    #[test]
    fn spectrum_y_title_anchor_stays_inside_left_gutter() {
        let layout = image_spectrum_layout(320, 180);
        let x = spectrum_y_title_x(layout, 12, 20, 24);
        assert!(x >= 24);
        assert!(x < layout.plot.x as i32);
    }

    #[test]
    fn image_spectrum_render_draws_overlay_profiles() {
        let overlay_profile = ImageProfilePayload {
            selected_sample_index: 2,
            samples: vec![
                ImageProfileSampleState {
                    sample_index: 0,
                    pixel_index: 0,
                    value: 3.0,
                    masked: false,
                    finite: true,
                    world_axis: Some(ImageBrowserAxisValue {
                        name: "Frequency".into(),
                        unit: "Hz".into(),
                        value: 1.0,
                    }),
                },
                ImageProfileSampleState {
                    sample_index: 1,
                    pixel_index: 1,
                    value: 2.0,
                    masked: false,
                    finite: true,
                    world_axis: Some(ImageBrowserAxisValue {
                        name: "Frequency".into(),
                        unit: "Hz".into(),
                        value: 2.0,
                    }),
                },
                ImageProfileSampleState {
                    sample_index: 2,
                    pixel_index: 2,
                    value: 4.0,
                    masked: false,
                    finite: true,
                    world_axis: Some(ImageBrowserAxisValue {
                        name: "Frequency".into(),
                        unit: "Hz".into(),
                        value: 3.0,
                    }),
                },
            ],
            ..ImageProfilePayload {
                axis: 2,
                axis_name: "Frequency".into(),
                axis_unit: "Hz".into(),
                value_unit: "Jy/beam".into(),
                coord_type: "Spectral".into(),
                selected_sample_index: 1,
                samples: Vec::new(),
            }
        };
        let image = render_image_spectrum_image(
            40,
            20,
            &ImageSpectrumRenderInput {
                cache_key: 0,
                profile: ImageProfilePayload {
                    axis: 2,
                    axis_name: "Frequency".into(),
                    axis_unit: "Hz".into(),
                    value_unit: "Jy/beam".into(),
                    coord_type: "Spectral".into(),
                    selected_sample_index: 1,
                    samples: vec![
                        ImageProfileSampleState {
                            sample_index: 0,
                            pixel_index: 0,
                            value: 1.0,
                            masked: false,
                            finite: true,
                            world_axis: Some(ImageBrowserAxisValue {
                                name: "Frequency".into(),
                                unit: "Hz".into(),
                                value: 1.0,
                            }),
                        },
                        ImageProfileSampleState {
                            sample_index: 1,
                            pixel_index: 1,
                            value: 4.0,
                            masked: false,
                            finite: true,
                            world_axis: Some(ImageBrowserAxisValue {
                                name: "Frequency".into(),
                                unit: "Hz".into(),
                                value: 2.0,
                            }),
                        },
                        ImageProfileSampleState {
                            sample_index: 2,
                            pixel_index: 2,
                            value: 2.0,
                            masked: false,
                            finite: true,
                            world_axis: Some(ImageBrowserAxisValue {
                                name: "Frequency".into(),
                                unit: "Hz".into(),
                                value: 3.0,
                            }),
                        },
                    ],
                },
                overlay_profiles: vec![ImageSpectrumOverlaySeries {
                    label: "P1".into(),
                    profile: overlay_profile,
                    color_index: 2,
                }],
                theme_mode: ThemeMode::DenseAnsi,
            },
        )
        .expect("render image spectrum");
        let rgb = image.to_rgb8();
        assert!(rgb.pixels().any(|pixel| pixel.0 == [255, 96, 220]));
    }

    #[test]
    fn image_spectrum_value_range_uses_overlay_profiles() {
        let input = ImageSpectrumRenderInput {
            cache_key: 0,
            profile: ImageProfilePayload {
                axis: 2,
                axis_name: "Frequency".into(),
                axis_unit: "Hz".into(),
                value_unit: "Jy/beam".into(),
                coord_type: "Spectral".into(),
                selected_sample_index: 0,
                samples: vec![ImageProfileSampleState {
                    sample_index: 0,
                    pixel_index: 0,
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axis: None,
                }],
            },
            overlay_profiles: vec![ImageSpectrumOverlaySeries {
                label: "P1".into(),
                profile: ImageProfilePayload {
                    axis: 2,
                    axis_name: "Frequency".into(),
                    axis_unit: "Hz".into(),
                    value_unit: "Jy/beam".into(),
                    coord_type: "Spectral".into(),
                    selected_sample_index: 0,
                    samples: vec![ImageProfileSampleState {
                        sample_index: 0,
                        pixel_index: 0,
                        value: 9.0,
                        masked: false,
                        finite: true,
                        world_axis: None,
                    }],
                },
                color_index: 0,
            }],
            theme_mode: ThemeMode::DenseAnsi,
        };
        assert_eq!(spectrum_value_range(&input), Some((1.0, 9.0)));
    }
}
