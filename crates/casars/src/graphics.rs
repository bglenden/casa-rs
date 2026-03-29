// SPDX-License-Identifier: LGPL-3.0-or-later
use casacore_imagebrowser_protocol::{ImagePlaneRaster, ImageProfilePayload};
use image::{DynamicImage, Rgb, RgbImage};

use crate::config::ThemeMode;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ListObsPlotRenderInput {
    pub payload: casacore_ms::ListObsPlotPayload,
    pub theme_mode: ThemeMode,
    pub terminal_cell_px: (u16, u16),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ImagePlaneRenderInput {
    pub raster: ImagePlaneRaster,
    pub cursor_sample: Option<(usize, usize)>,
    pub sampled_shape: Option<(usize, usize)>,
    pub display_aspect_ratio: Option<f64>,
    pub theme_mode: ThemeMode,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ImageSpectrumRenderInput {
    pub profile: ImageProfilePayload,
    pub theme_mode: ThemeMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ImagePlaneDrawGeometry {
    pub x: u32,
    pub y: u32,
    pub width: u32,
    pub height: u32,
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
    let geometry = image_plane_draw_geometry(width, height, input.display_aspect_ratio);
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
            image.put_pixel(out_x, out_y, Rgb([value, value, value]));
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
    let (background, border, series, marker, guide_line, zero_line) = match input.theme_mode {
        ThemeMode::DenseAnsi => (
            [0, 0, 0],
            [96, 96, 96],
            [96, 208, 255],
            highlight,
            highlight,
            [48, 48, 48],
        ),
        ThemeMode::RichPanel => (
            [15, 23, 42],
            [94, 234, 212],
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

    let plot_left = 2;
    let plot_top = 1;
    let plot_width = width.saturating_sub(4).max(1);
    let plot_height = height.saturating_sub(3).max(1);
    draw_rect_outline(
        &mut image,
        plot_left,
        plot_top,
        plot_width,
        plot_height,
        Rgb(border),
    );

    let x_domain = profile_x_domain(&input.profile);
    let valid_samples = input
        .profile
        .samples
        .iter()
        .filter(|sample| !sample.masked && sample.finite && sample.value.is_finite())
        .collect::<Vec<_>>();
    if valid_samples.is_empty() {
        return Ok(DynamicImage::ImageRgb8(image));
    }
    let mut min_value = valid_samples
        .iter()
        .map(|sample| sample.value)
        .fold(f64::INFINITY, f64::min);
    let mut max_value = valid_samples
        .iter()
        .map(|sample| sample.value)
        .fold(f64::NEG_INFINITY, f64::max);
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

    let mut previous = None::<(u32, u32)>;
    for sample in &input.profile.samples {
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
            draw_line(&mut image, prev.0, prev.1, point.0, point.1, Rgb(series));
        }
        previous = Some(point);
    }

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

    Ok(DynamicImage::ImageRgb8(image))
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

fn image_browser_highlight(theme_mode: ThemeMode) -> [u8; 3] {
    match theme_mode {
        ThemeMode::DenseAnsi => [255, 208, 96],
        ThemeMode::RichPanel => [196, 64, 32],
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

fn profile_x_domain(profile: &ImageProfilePayload) -> (f64, f64) {
    let mut values = profile
        .samples
        .iter()
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
    (0.0, profile.samples.len().saturating_sub(1) as f64)
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
        ImagePlaneDrawGeometry, ImagePlaneRenderInput, ImageSpectrumRenderInput,
        image_plane_draw_geometry, plot_theme, render_image_plane_image,
        render_image_spectrum_image,
    };
    use crate::config::ThemeMode;
    use casacore_imagebrowser_protocol::{
        ImageBrowserAxisValue, ImagePlaneRaster, ImageProfilePayload, ImageProfileSampleState,
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
                raster: ImagePlaneRaster {
                    width: 2,
                    height: 2,
                    pixels_u8: vec![0, 64, 128, 255],
                    clip_min: 0.0,
                    clip_max: 1.0,
                    masked_or_non_finite_count: 0,
                    no_finite_values: false,
                },
                cursor_sample: Some((1, 1)),
                sampled_shape: Some((2, 2)),
                display_aspect_ratio: None,
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
                raster: ImagePlaneRaster {
                    width: 2,
                    height: 2,
                    pixels_u8: vec![255, 255, 255, 255],
                    clip_min: 0.0,
                    clip_max: 1.0,
                    masked_or_non_finite_count: 0,
                    no_finite_values: false,
                },
                cursor_sample: None,
                sampled_shape: None,
                display_aspect_ratio: Some(2.0),
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
    fn image_spectrum_render_highlights_selected_sample() {
        let image = render_image_spectrum_image(
            32,
            16,
            &ImageSpectrumRenderInput {
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
        assert!(guide_pixels >= 7);
    }
}
