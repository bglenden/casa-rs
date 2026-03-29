// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only image browser backend primitives.

use std::path::{Path, PathBuf};

use casacore_coordinates::{CoordinateSystem, CoordinateType, ObsInfo, StokesType};
use casacore_types::measures::direction::{
    angular_increment_arcseconds, declination_increment_arcseconds, format_declination_labeled,
    format_right_ascension_labeled,
};
use casacore_types::measures::position::PositionRef;
use casacore_types::quanta::{MvTime, Quantity, Unit};
use casacore_types::{ArrayD, ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::{Array2, Axis, Ix2, IxDyn};

use crate::beam::{GaussianBeam, ImageBeamSet};
use crate::error::ImageError;
use crate::image::{AnyPagedImage, ImagePixelType};

/// Capability flags exposed by the read-only image browser backend.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ImageViewCapabilities {
    pub renderable_plane: bool,
    pub world_coords_available: bool,
    pub pixel_only_mode: bool,
    pub non_display_axis_selectors: bool,
    pub mask_present: bool,
    pub complex_unsupported: bool,
}

/// Axis-selection model for the current image browser session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageAxisModel {
    pub display_axes: Option<[usize; 2]>,
    pub non_display_axes: Vec<usize>,
}

/// An 8-bit grayscale raster ready for UI transport.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaneRaster {
    pub width: usize,
    pub height: usize,
    pub pixels_u8: Vec<u8>,
    pub clip_min: f64,
    pub clip_max: f64,
    pub data_min: f64,
    pub data_max: f64,
    pub value_unit: String,
    pub histogram_bins: Vec<u32>,
    pub masked_or_non_finite_count: usize,
    pub no_finite_values: bool,
}

/// Backend-controlled stretch preset for 2D plane rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlaneStretchPreset {
    Percentile99,
    Percentile95,
    MinMax,
    ZScale,
    Manual,
}

/// Autoscaling policy for plane rendering across cube stepping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlaneAutoscaleMode {
    PerPlane,
    Frozen,
}

/// Normalized plane stretch settings applied by the image browser backend.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlaneStretchSettings {
    pub preset: PlaneStretchPreset,
    pub autoscale: PlaneAutoscaleMode,
    pub manual_clip: Option<(f64, f64)>,
}

impl Default for PlaneStretchSettings {
    fn default() -> Self {
        Self {
            preset: PlaneStretchPreset::Percentile99,
            autoscale: PlaneAutoscaleMode::PerPlane,
            manual_clip: None,
        }
    }
}

/// A named numeric axis value.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageAxisValue {
    pub name: String,
    pub unit: String,
    pub value: f64,
}

/// Display-axis metadata for the active rendered plane window.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageDisplayAxis {
    pub axis: usize,
    pub name: String,
    pub unit: String,
    pub blc: usize,
    pub trc: usize,
    pub inc: usize,
    pub sampled_len: usize,
    /// World-coordinate increment per source pixel, in the native axis units.
    pub world_increment: Option<f64>,
}

/// Non-display axis metadata for the active rendered plane window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageNonDisplayAxis {
    pub axis: usize,
    pub name: String,
    pub index: usize,
    pub length: usize,
    pub pixel: usize,
}

/// Cursor probe output for the current image plane.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageProbe {
    pub pixel_indices: Vec<usize>,
    pub pixel_axes: Vec<ImageAxisValue>,
    pub value: f64,
    pub masked: bool,
    pub finite: bool,
    pub world_axes: Vec<ImageAxisValue>,
}

/// A single sample in a 1D spectrum/profile extraction.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageProfileSample {
    pub sample_index: usize,
    pub pixel_index: usize,
    pub value: f64,
    pub masked: bool,
    pub finite: bool,
    pub world_axis: Option<ImageAxisValue>,
}

/// A 1D spectrum/profile through the active cursor position.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageProfile {
    pub axis: usize,
    pub axis_name: String,
    pub axis_unit: String,
    pub value_unit: String,
    pub coord_type: CoordinateType,
    pub selected_sample_index: usize,
    pub samples: Vec<ImageProfileSample>,
}

/// A titled metadata section for inspector rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageMetadataSection {
    pub title: String,
    pub lines: Vec<String>,
}

/// Normalized BLC/TRC/INC pixel-selection state for an opened image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageViewWindow {
    blc: Vec<usize>,
    trc: Vec<usize>,
    inc: Vec<usize>,
}

impl ImageViewWindow {
    /// Returns the inclusive bottom-left-corner pixel indices.
    pub fn blc(&self) -> &[usize] {
        &self.blc
    }

    /// Returns the inclusive top-right-corner pixel indices.
    pub fn trc(&self) -> &[usize] {
        &self.trc
    }

    /// Returns the per-axis pixel increments.
    pub fn inc(&self) -> &[usize] {
        &self.inc
    }

    /// Returns the normalized BLC text shown in the UI.
    pub fn format_blc(&self) -> String {
        format_axis_list(&self.blc)
    }

    /// Returns the normalized TRC text shown in the UI.
    pub fn format_trc(&self) -> String {
        format_axis_list(&self.trc)
    }

    /// Returns the normalized INC text shown in the UI.
    pub fn format_inc(&self) -> String {
        format_axis_list(&self.inc)
    }

    pub(crate) fn sampled_axis_len(&self, axis: usize) -> usize {
        sampled_axis_len(self.blc[axis], self.trc[axis], self.inc[axis])
    }

    pub(crate) fn sampled_axis_value(&self, axis: usize, sample_index: usize) -> Option<usize> {
        (sample_index < self.sampled_axis_len(axis))
            .then_some(self.blc[axis] + sample_index * self.inc[axis])
    }

    pub(crate) fn sampled_axis_values(&self, axis: usize) -> Vec<usize> {
        (0..self.sampled_axis_len(axis))
            .map(|index| self.blc[axis] + index * self.inc[axis])
            .collect()
    }

    pub(crate) fn nearest_sample_index(&self, axis: usize, pixel: usize) -> usize {
        let start = self.blc[axis];
        let end = self.trc[axis];
        if pixel <= start {
            return 0;
        }
        if pixel >= end {
            return self.sampled_axis_len(axis).saturating_sub(1);
        }
        ((pixel - start) / self.inc[axis]).min(self.sampled_axis_len(axis).saturating_sub(1))
    }
}

/// Read-only image browser backend built on top of [`AnyPagedImage`].
#[derive(Debug)]
pub struct OpenedImageView {
    path: PathBuf,
    image: AnyPagedImage,
    capabilities: ImageViewCapabilities,
    axis_model: ImageAxisModel,
}

impl OpenedImageView {
    /// Opens a persistent image and prepares browser-facing metadata.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ImageError> {
        let path = path.as_ref().to_path_buf();
        let image = AnyPagedImage::open(&path)?;
        let axis_model = determine_axis_model(&image);
        let world_coords_available =
            coordinates_cover_image_axes(image_coordinates(&image), image.shape())
                && image_coordinates(&image)
                    .to_world(&vec![0.0; image.shape().len()])
                    .is_ok();
        let capabilities = ImageViewCapabilities {
            renderable_plane: matches!(
                image.pixel_type(),
                ImagePixelType::Float32 | ImagePixelType::Float64
            ) && axis_model.display_axes.is_some(),
            world_coords_available,
            pixel_only_mode: !world_coords_available,
            non_display_axis_selectors: !axis_model.non_display_axes.is_empty(),
            mask_present: image_has_pixel_mask(&image),
            complex_unsupported: matches!(
                image.pixel_type(),
                ImagePixelType::Complex32 | ImagePixelType::Complex64
            ),
        };

        Ok(Self {
            path,
            image,
            capabilities,
            axis_model,
        })
    }

    /// Returns the opened image path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the detected image pixel type.
    pub fn pixel_type(&self) -> ImagePixelType {
        self.image.pixel_type()
    }

    /// Returns the image shape.
    pub fn shape(&self) -> &[usize] {
        self.image.shape()
    }

    /// Returns the derived capability flags.
    pub fn capabilities(&self) -> &ImageViewCapabilities {
        &self.capabilities
    }

    /// Returns the brightness unit string for image pixel values.
    pub fn brightness_unit(&self) -> &str {
        image_units(&self.image)
    }

    /// Returns the display and hidden-axis model.
    pub fn axis_model(&self) -> &ImageAxisModel {
        &self.axis_model
    }

    /// Returns the default full-image selection window.
    pub fn default_window(&self) -> ImageViewWindow {
        ImageViewWindow {
            blc: vec![0; self.shape().len()],
            trc: self
                .shape()
                .iter()
                .map(|len| len.saturating_sub(1))
                .collect(),
            inc: vec![1; self.shape().len()],
        }
    }

    /// Parses and validates user-facing `blc`, `trc`, and `inc` text fields.
    pub fn window_from_text(
        &self,
        blc_text: &str,
        trc_text: &str,
        inc_text: &str,
    ) -> Result<ImageViewWindow, ImageError> {
        let defaults = self.default_window();
        let blc = parse_window_axis_values("BLC", blc_text, self.shape(), defaults.blc())?;
        let trc = parse_window_axis_values("TRC", trc_text, self.shape(), defaults.trc())?;
        let inc = parse_window_axis_values("INC", inc_text, self.shape(), defaults.inc())?;
        validate_window(self.shape(), &blc, &trc, &inc)?;
        Ok(ImageViewWindow { blc, trc, inc })
    }

    /// Returns a short status line describing degraded or unsupported modes.
    pub fn status_line(&self) -> String {
        if self.capabilities.complex_unsupported {
            "complex images unsupported in wave 1".into()
        } else if !self.capabilities.renderable_plane {
            "viewer requires at least two displayable axes for Plane view".into()
        } else if self.capabilities.pixel_only_mode {
            "pixel-only mode: coordinate reconstruction unavailable".into()
        } else {
            "ready".into()
        }
    }

    /// Returns metadata/header/coordinate text sections for the inspector.
    pub fn metadata_sections(&self) -> Result<Vec<ImageMetadataSection>, ImageError> {
        let axis_descriptors = build_axis_descriptors(image_coordinates(&self.image), self.shape());
        let default_mask_name =
            image_default_mask_name(&self.image).unwrap_or_else(|| "none".into());
        let history = image_history(&self.image)?;
        let image_info = image_image_info(&self.image)?;
        let beam_lines = format_beam_summary_lines(&image_info.beam_set)?;

        let mut sections = Vec::new();
        let mut summary_lines = vec![
            format!("path: {}", self.path.display()),
            format!("pixel type: {:?}", self.pixel_type()),
            format!("shape: {:?}", self.shape()),
            format!("units: {}", image_units(&self.image)),
            format!("default mask: {default_mask_name}"),
            format!("history entries: {}", history.len()),
            format!("image type: {}", image_info.image_type),
            format!(
                "object name: {}",
                if image_info.object_name.is_empty() {
                    "<none>"
                } else {
                    &image_info.object_name
                }
            ),
        ];
        summary_lines.extend(beam_lines);
        summary_lines.push(format!("status: {}", self.status_line()));
        sections.push(ImageMetadataSection {
            title: "Summary".into(),
            lines: summary_lines,
        });
        sections.push(ImageMetadataSection {
            title: "Observation".into(),
            lines: build_observation_lines(image_coordinates(&self.image).obs_info()),
        });
        sections.push(ImageMetadataSection {
            title: "Axes".into(),
            lines: axis_descriptors
                .iter()
                .enumerate()
                .map(|(index, axis)| {
                    format!(
                        "axis {index}: name={} type={} unit={} len={} ref_pix={} ref_val={} incr={}",
                        axis.name,
                        axis.coord_type,
                        if axis.unit.is_empty() { "<none>" } else { &axis.unit },
                        self.shape().get(index).copied().unwrap_or_default(),
                        format_reference_pixel_for_display(axis.reference_pixel),
                        format_axis_value_for_display(
                            axis.reference_value,
                            &axis.name,
                            &axis.unit,
                        ),
                        format_axis_increment_for_display(axis.increment, &axis.name, &axis.unit),
                    )
                })
                .collect(),
        });

        let mut coordinate_lines = Vec::new();
        if self.capabilities.pixel_only_mode {
            coordinate_lines.push("pixel-only mode: coordinate reconstruction unavailable".into());
        }
        if let Some(display_axes) = self.axis_model.display_axes {
            coordinate_lines.push(format!(
                "display axes: {} ({}) and {} ({})",
                axis_descriptors[display_axes[0]].name,
                display_axes[0],
                axis_descriptors[display_axes[1]].name,
                display_axes[1],
            ));
        }
        for &axis in &self.axis_model.non_display_axes {
            coordinate_lines.push(format!(
                "non-display axis: {} ({axis}), index range 0..{}",
                axis_names(&self.image)
                    .get(axis)
                    .map(String::as_str)
                    .unwrap_or("Axis"),
                self.shape()
                    .get(axis)
                    .copied()
                    .unwrap_or_default()
                    .saturating_sub(1),
            ));
        }
        coordinate_lines.extend(build_coordinate_summary_lines(
            image_coordinates(&self.image),
            self.shape(),
        ));
        sections.push(ImageMetadataSection {
            title: "Coordinates".into(),
            lines: coordinate_lines,
        });

        let misc = image_misc_info(&self.image);
        let misc_lines = if misc.is_empty() {
            vec!["<none>".into()]
        } else {
            misc.fields()
                .iter()
                .map(|field| format!("{}: {}", field.name, format_value(&field.value)))
                .collect()
        };
        sections.push(ImageMetadataSection {
            title: "Misc".into(),
            lines: misc_lines,
        });

        Ok(sections)
    }

    /// Renders the current 2D plane as an 8-bit grayscale raster.
    pub fn render_plane(
        &self,
        viewport: (usize, usize),
        hidden_index: usize,
    ) -> Result<PlaneRaster, ImageError> {
        let window = self.default_window();
        self.render_plane_with_window(viewport, &window, hidden_index)
    }

    /// Renders the current 2D plane for an explicit `blc`/`trc`/`inc` window.
    pub fn render_plane_with_window(
        &self,
        viewport: (usize, usize),
        window: &ImageViewWindow,
        hidden_index: usize,
    ) -> Result<PlaneRaster, ImageError> {
        let mut indices = self.default_non_display_sample_indices();
        if let Some(first) = indices.first_mut() {
            *first = hidden_index;
        }
        self.render_plane_with_window_and_axes(viewport, window, &indices)
    }

    /// Renders the current 2D plane for an explicit `blc`/`trc`/`inc` window and
    /// non-display-axis sample selections.
    pub fn render_plane_with_window_and_axes(
        &self,
        viewport: (usize, usize),
        window: &ImageViewWindow,
        non_display_indices: &[usize],
    ) -> Result<PlaneRaster, ImageError> {
        self.render_plane_with_window_and_axes_and_stretch(
            viewport,
            window,
            non_display_indices,
            &PlaneStretchSettings::default(),
            None,
        )
    }

    /// Renders the current 2D plane for an explicit `blc`/`trc`/`inc` window,
    /// non-display-axis selections, and explicit stretch settings.
    pub(crate) fn render_plane_with_window_and_axes_and_stretch(
        &self,
        viewport: (usize, usize),
        window: &ImageViewWindow,
        non_display_indices: &[usize],
        stretch: &PlaneStretchSettings,
        clip_override: Option<(f64, f64)>,
    ) -> Result<PlaneRaster, ImageError> {
        if !self.capabilities.renderable_plane {
            return Err(ImageError::InvalidMetadata(self.status_line()));
        }
        let (plane, mask) = self.read_plane(window, non_display_indices)?;
        let plane_stats = collect_plane_stats(&plane, mask.as_ref());
        let width = viewport.0.max(1).min(plane.shape()[0].max(1));
        let height = viewport.1.max(1).min(plane.shape()[1].max(1));
        let (clip_min, clip_max) = clip_override
            .or_else(|| plane_stats.clip_bounds_for(stretch))
            .unwrap_or((0.0, 0.0));

        let mut pixels_u8 = Vec::with_capacity(width * height);
        for y in 0..height {
            let y0 = y * plane.shape()[1] / height;
            let y1 = ((y + 1) * plane.shape()[1]).div_ceil(height);
            for x in 0..width {
                let x0 = x * plane.shape()[0] / width;
                let x1 = ((x + 1) * plane.shape()[0]).div_ceil(width);
                let mut sum = 0.0;
                let mut count = 0usize;
                for src_x in x0..x1.max(x0 + 1) {
                    for src_y in y0..y1.max(y0 + 1) {
                        if !plane_stats.is_valid(mask.as_ref(), src_x, src_y) {
                            continue;
                        }
                        sum += plane[[src_x, src_y]];
                        count += 1;
                    }
                }
                let sample = if count == 0 || plane_stats.no_finite_values {
                    0
                } else if (clip_max - clip_min).abs() < f64::EPSILON {
                    128
                } else {
                    let scaled = ((sum / count as f64) - clip_min) / (clip_max - clip_min);
                    (scaled.clamp(0.0, 1.0) * 255.0).round() as u8
                };
                pixels_u8.push(sample);
            }
        }

        Ok(PlaneRaster {
            width,
            height,
            pixels_u8,
            clip_min,
            clip_max,
            data_min: plane_stats.data_min.unwrap_or(0.0),
            data_max: plane_stats.data_max.unwrap_or(0.0),
            value_unit: image_units(&self.image).to_string(),
            histogram_bins: plane_stats.histogram_bins(48),
            masked_or_non_finite_count: plane_stats.masked_or_non_finite_count,
            no_finite_values: plane_stats.no_finite_values,
        })
    }

    /// Returns a spreadsheet-style exact-value window centered on the cursor.
    pub fn render_plane_value_grid(
        &self,
        viewport_chars: (usize, usize),
        hidden_index: usize,
        cursor_xy: (usize, usize),
    ) -> Result<Vec<String>, ImageError> {
        let window = self.default_window();
        self.render_plane_value_grid_with_window(viewport_chars, &window, hidden_index, cursor_xy)
    }

    /// Returns a spreadsheet-style exact-value window for an explicit view window.
    pub fn render_plane_value_grid_with_window(
        &self,
        viewport_chars: (usize, usize),
        window: &ImageViewWindow,
        hidden_index: usize,
        cursor_xy: (usize, usize),
    ) -> Result<Vec<String>, ImageError> {
        let mut indices = self.default_non_display_sample_indices();
        if let Some(first) = indices.first_mut() {
            *first = hidden_index;
        }
        self.render_plane_value_grid_with_window_and_axes(
            viewport_chars,
            window,
            &indices,
            cursor_xy,
        )
    }

    /// Returns a spreadsheet-style exact-value window for an explicit view
    /// window and non-display-axis sample selections.
    pub fn render_plane_value_grid_with_window_and_axes(
        &self,
        viewport_chars: (usize, usize),
        window: &ImageViewWindow,
        non_display_indices: &[usize],
        cursor_xy: (usize, usize),
    ) -> Result<Vec<String>, ImageError> {
        if !self.capabilities.renderable_plane {
            return Err(ImageError::InvalidMetadata(self.status_line()));
        }
        let (plane, mask) = self.read_plane(window, non_display_indices)?;
        let plane_width = plane.shape()[0];
        let plane_height = plane.shape()[1];
        if plane_width == 0 || plane_height == 0 {
            return Ok(Vec::new());
        }
        let min_cell_width = 7usize;
        let preferred_cell_width = 11usize;
        let _viewport_width = viewport_chars.0;
        let cols = plane_width;
        let cell_width = preferred_cell_width.max(min_cell_width);
        let rows = viewport_chars.1.saturating_sub(1).max(1).min(plane_height);
        let display_axes = self
            .axis_model
            .display_axes
            .ok_or_else(|| ImageError::InvalidMetadata(self.status_line()))?;
        let x_pixels = window.sampled_axis_values(display_axes[0]);
        let y_pixels = window.sampled_axis_values(display_axes[1]);
        let row_label_width = index_label_width(
            x_pixels
                .last()
                .copied()
                .unwrap_or_default()
                .max(y_pixels.last().copied().unwrap_or_default())
                .saturating_add(1),
        );

        let cursor_x = cursor_xy.0.min(plane_width.saturating_sub(1));
        let cursor_y = cursor_xy.1.min(plane_height.saturating_sub(1));
        let y_start = window_start(cursor_y, rows, plane_height);

        let mut lines = Vec::with_capacity(rows + 1);
        let mut header = format!("{:>width$} |", "y/x", width = row_label_width);
        for &x_pixel in &x_pixels {
            header.push(' ');
            header.push_str(&format!("{:>width$}", x_pixel, width = cell_width));
        }
        lines.push(header);

        for (y, &y_pixel) in y_pixels.iter().enumerate().skip(y_start).take(rows) {
            let mut line = format!("{:>width$} |", y_pixel, width = row_label_width);
            for x in 0..cols {
                let text = plane_cell_text(&plane, mask.as_ref(), x, y);
                line.push(' ');
                line.push_str(&render_grid_cell(
                    &text,
                    cell_width,
                    x == cursor_x && y == cursor_y,
                ));
            }
            lines.push(line);
        }

        Ok(lines)
    }

    /// Returns the current cursor probe for the active 2D plane.
    pub fn probe(
        &self,
        pixel_xy: (usize, usize),
        hidden_index: usize,
    ) -> Result<ImageProbe, ImageError> {
        let window = self.default_window();
        self.probe_with_window(pixel_xy, &window, hidden_index)
    }

    /// Returns the current cursor probe for an explicit view window.
    pub fn probe_with_window(
        &self,
        pixel_xy: (usize, usize),
        window: &ImageViewWindow,
        hidden_index: usize,
    ) -> Result<ImageProbe, ImageError> {
        let mut indices = self.default_non_display_sample_indices();
        if let Some(first) = indices.first_mut() {
            *first = hidden_index;
        }
        self.probe_with_window_and_axes(pixel_xy, window, &indices)
    }

    /// Returns the current cursor probe for an explicit view window and
    /// non-display-axis sample selections.
    pub fn probe_with_window_and_axes(
        &self,
        pixel_xy: (usize, usize),
        window: &ImageViewWindow,
        non_display_indices: &[usize],
    ) -> Result<ImageProbe, ImageError> {
        let display_axes = self
            .axis_model
            .display_axes
            .ok_or_else(|| ImageError::InvalidMetadata(self.status_line()))?;
        let sampled_width = window.sampled_axis_len(display_axes[0]);
        let sampled_height = window.sampled_axis_len(display_axes[1]);
        if pixel_xy.0 >= sampled_width || pixel_xy.1 >= sampled_height {
            return Err(ImageError::ShapeMismatch {
                expected: vec![sampled_width, sampled_height],
                got: vec![pixel_xy.0, pixel_xy.1],
            });
        }
        let mut pixel_indices = window.blc.clone();
        pixel_indices[display_axes[0]] = window
            .sampled_axis_value(display_axes[0], pixel_xy.0)
            .ok_or_else(|| ImageError::ShapeMismatch {
                expected: vec![sampled_width],
                got: vec![pixel_xy.0],
            })?;
        pixel_indices[display_axes[1]] = window
            .sampled_axis_value(display_axes[1], pixel_xy.1)
            .ok_or_else(|| ImageError::ShapeMismatch {
                expected: vec![sampled_height],
                got: vec![pixel_xy.1],
            })?;
        apply_non_display_axis_selections(
            &mut pixel_indices,
            window,
            &self.axis_model.non_display_axes,
            non_display_indices,
        )?;
        for (axis, &pixel) in pixel_indices.iter().enumerate() {
            if pixel >= self.shape()[axis] {
                return Err(ImageError::ShapeMismatch {
                    expected: self.shape().to_vec(),
                    got: pixel_indices.clone(),
                });
            }
        }

        let value = image_real_get_at(&self.image, &pixel_indices)?;
        let mask = image_get_mask(&self.image)?;
        let masked = mask
            .as_ref()
            .is_some_and(|data| !data[IxDyn(&pixel_indices)]);
        let axis_descriptors = build_axis_descriptors(image_coordinates(&self.image), self.shape());
        let pixel_axes = pixel_indices
            .iter()
            .enumerate()
            .map(|(index, &pixel)| ImageAxisValue {
                name: axis_descriptors[index].name.clone(),
                unit: "px".into(),
                value: pixel as f64,
            })
            .collect();
        let world_axes = if self.capabilities.world_coords_available {
            let world = image_coordinates(&self.image)
                .to_world(
                    &pixel_indices
                        .iter()
                        .map(|&pixel| pixel as f64)
                        .collect::<Vec<_>>(),
                )
                .map_err(ImageError::from)?;
            axis_descriptors
                .iter()
                .enumerate()
                .map(|(index, axis)| ImageAxisValue {
                    name: axis.name.clone(),
                    unit: axis.unit.clone(),
                    value: world[index],
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok(ImageProbe {
            pixel_indices,
            pixel_axes,
            value,
            masked,
            finite: value.is_finite(),
            world_axes,
        })
    }

    /// Returns display-axis metadata for the current rendered plane window.
    pub fn display_axes_with_window(&self, window: &ImageViewWindow) -> Vec<ImageDisplayAxis> {
        let Some(display_axes) = self.axis_model.display_axes else {
            return Vec::new();
        };
        let descriptors = build_axis_descriptors(image_coordinates(&self.image), self.shape());
        display_axes
            .into_iter()
            .map(|axis| ImageDisplayAxis {
                axis,
                name: descriptors
                    .get(axis)
                    .map(|descriptor| descriptor.name.clone())
                    .unwrap_or_else(|| format!("Axis{axis}")),
                unit: descriptors
                    .get(axis)
                    .map(|descriptor| descriptor.unit.clone())
                    .unwrap_or_default(),
                blc: window.blc()[axis],
                trc: window.trc()[axis],
                inc: window.inc()[axis],
                sampled_len: window.sampled_axis_len(axis),
                world_increment: descriptors
                    .get(axis)
                    .and_then(|descriptor| descriptor.increment),
            })
            .collect()
    }

    /// Returns non-display-axis metadata for the current rendered plane window.
    pub fn non_display_axes_with_window(
        &self,
        window: &ImageViewWindow,
        non_display_indices: &[usize],
    ) -> Result<Vec<ImageNonDisplayAxis>, ImageError> {
        let axis_names = axis_names(&self.image);
        selected_non_display_axes(
            window,
            &self.axis_model.non_display_axes,
            non_display_indices,
        )
        .map(|selections| {
            selections
                .into_iter()
                .map(|(axis, index, pixel)| ImageNonDisplayAxis {
                    axis,
                    name: axis_names
                        .get(axis)
                        .cloned()
                        .unwrap_or_else(|| format!("Axis{axis}")),
                    index,
                    length: window.sampled_axis_len(axis),
                    pixel,
                })
                .collect()
        })
    }

    /// Returns the preferred non-display axis for a 1D spectrum/profile.
    pub fn preferred_profile_axis(&self) -> Option<usize> {
        let descriptors = build_axis_descriptors(image_coordinates(&self.image), self.shape());
        self.axis_model
            .non_display_axes
            .iter()
            .copied()
            .find(|&axis| {
                descriptors
                    .get(axis)
                    .is_some_and(|descriptor| descriptor.coord_type == CoordinateType::Spectral)
            })
            .or_else(|| self.axis_model.non_display_axes.first().copied())
    }

    /// Extracts a 1D spectrum/profile along the requested non-display axis.
    pub fn profile_with_window_and_axes(
        &self,
        pixel_xy: (usize, usize),
        window: &ImageViewWindow,
        non_display_indices: &[usize],
        profile_axis: usize,
    ) -> Result<ImageProfile, ImageError> {
        let display_axes = self
            .axis_model
            .display_axes
            .ok_or_else(|| ImageError::InvalidMetadata(self.status_line()))?;
        let sampled_width = window.sampled_axis_len(display_axes[0]);
        let sampled_height = window.sampled_axis_len(display_axes[1]);
        if pixel_xy.0 >= sampled_width || pixel_xy.1 >= sampled_height {
            return Err(ImageError::ShapeMismatch {
                expected: vec![sampled_width, sampled_height],
                got: vec![pixel_xy.0, pixel_xy.1],
            });
        }
        let profile_position = self
            .axis_model
            .non_display_axes
            .iter()
            .position(|axis| *axis == profile_axis)
            .ok_or_else(|| {
                ImageError::InvalidMetadata(format!(
                    "axis {profile_axis} is not a non-display axis in this view"
                ))
            })?;
        let selections = selected_non_display_axes(
            window,
            &self.axis_model.non_display_axes,
            non_display_indices,
        )?;
        let descriptors = build_axis_descriptors(image_coordinates(&self.image), self.shape());
        let profile_descriptor =
            descriptors
                .get(profile_axis)
                .cloned()
                .unwrap_or_else(|| AxisDescriptor {
                    coord_type: CoordinateType::Linear,
                    name: format!("Axis{profile_axis}"),
                    unit: String::new(),
                    reference_value: None,
                    reference_pixel: None,
                    increment: None,
                });
        let selected_sample_index = selections
            .get(profile_position)
            .map(|(_, index, _)| *index)
            .unwrap_or_default();

        let mut fixed_pixel_indices = window.blc.clone();
        fixed_pixel_indices[display_axes[0]] = window
            .sampled_axis_value(display_axes[0], pixel_xy.0)
            .ok_or_else(|| ImageError::ShapeMismatch {
                expected: vec![sampled_width],
                got: vec![pixel_xy.0],
            })?;
        fixed_pixel_indices[display_axes[1]] = window
            .sampled_axis_value(display_axes[1], pixel_xy.1)
            .ok_or_else(|| ImageError::ShapeMismatch {
                expected: vec![sampled_height],
                got: vec![pixel_xy.1],
            })?;
        for &(axis, _index, pixel) in &selections {
            fixed_pixel_indices[axis] = pixel;
        }

        let axis_len = window.sampled_axis_len(profile_axis);
        let samples = (0..axis_len)
            .map(|sample_index| {
                let pixel = window
                    .sampled_axis_value(profile_axis, sample_index)
                    .ok_or_else(|| ImageError::ShapeMismatch {
                        expected: vec![axis_len],
                        got: vec![sample_index],
                    })?;
                let mut pixel_indices = fixed_pixel_indices.clone();
                pixel_indices[profile_axis] = pixel;
                let value = image_real_get_at(&self.image, &pixel_indices)?;
                let mask = image_get_mask(&self.image)?;
                let masked = mask
                    .as_ref()
                    .is_some_and(|data| !data[IxDyn(&pixel_indices)]);
                let world_axis = if self.capabilities.world_coords_available {
                    let world = image_coordinates(&self.image)
                        .to_world(
                            &pixel_indices
                                .iter()
                                .map(|&pixel| pixel as f64)
                                .collect::<Vec<_>>(),
                        )
                        .map_err(ImageError::from)?;
                    Some(ImageAxisValue {
                        name: profile_descriptor.name.clone(),
                        unit: profile_descriptor.unit.clone(),
                        value: world[profile_axis],
                    })
                } else {
                    None
                };
                Ok(ImageProfileSample {
                    sample_index,
                    pixel_index: pixel,
                    value,
                    masked,
                    finite: value.is_finite(),
                    world_axis,
                })
            })
            .collect::<Result<Vec<_>, ImageError>>()?;

        Ok(ImageProfile {
            axis: profile_axis,
            axis_name: profile_descriptor.name,
            axis_unit: profile_descriptor.unit,
            value_unit: image_units(&self.image).to_string(),
            coord_type: profile_descriptor.coord_type,
            selected_sample_index,
            samples,
        })
    }

    fn default_non_display_sample_indices(&self) -> Vec<usize> {
        vec![0; self.axis_model.non_display_axes.len()]
    }

    fn read_plane(
        &self,
        window: &ImageViewWindow,
        non_display_indices: &[usize],
    ) -> Result<(Array2<f64>, Option<Array2<bool>>), ImageError> {
        let display_axes = self
            .axis_model
            .display_axes
            .ok_or_else(|| ImageError::InvalidMetadata(self.status_line()))?;
        let mut start = window.blc.clone();
        let mut shape = window
            .blc
            .iter()
            .zip(window.trc.iter())
            .map(|(blc, trc)| trc - blc + 1)
            .collect::<Vec<_>>();
        for (axis, _index, pixel) in selected_non_display_axes(
            window,
            &self.axis_model.non_display_axes,
            non_display_indices,
        )? {
            start[axis] = pixel;
            shape[axis] = 1;
        }
        let squeeze_axes = squeezed_axes(self.shape(), display_axes);
        for &axis in &squeeze_axes {
            shape[axis] = 1;
        }

        let plane = squeeze_plane_axes(
            image_real_get_slice(&self.image, &start, &shape)?,
            &squeeze_axes,
        );
        let plane = plane.into_dimensionality::<Ix2>().map_err(|_| {
            ImageError::InvalidMetadata(format!("expected 2D plane for axes {:?}", display_axes))
        })?;
        let plane = sample_plane_axes(
            &plane,
            window.inc[display_axes[0]],
            window.inc[display_axes[1]],
        );

        let mask = match image_get_mask(&self.image)? {
            Some(mask_data) => {
                Some(self.read_mask_plane(mask_data, window, display_axes, non_display_indices)?)
            }
            None => None,
        };

        Ok((plane, mask))
    }

    fn read_mask_plane(
        &self,
        mask_data: ArrayD<bool>,
        window: &ImageViewWindow,
        display_axes: [usize; 2],
        non_display_indices: &[usize],
    ) -> Result<Array2<bool>, ImageError> {
        let x_pixels = window.sampled_axis_values(display_axes[0]);
        let y_pixels = window.sampled_axis_values(display_axes[1]);
        let non_display_pixels = selected_non_display_axes(
            window,
            &self.axis_model.non_display_axes,
            non_display_indices,
        )?;
        Ok(Array2::from_shape_fn(
            (x_pixels.len(), y_pixels.len()),
            |(x_index, y_index)| {
                let mut indices = window.blc.clone();
                indices[display_axes[0]] = x_pixels[x_index];
                indices[display_axes[1]] = y_pixels[y_index];
                for &(axis, _index, pixel) in &non_display_pixels {
                    indices[axis] = pixel;
                }
                mask_data[IxDyn(&indices)]
            },
        ))
    }
}

#[derive(Debug, Clone)]
struct AxisDescriptor {
    coord_type: CoordinateType,
    name: String,
    unit: String,
    reference_value: Option<f64>,
    reference_pixel: Option<f64>,
    increment: Option<f64>,
}

#[derive(Debug)]
struct PlaneStats {
    finite_values: Vec<f64>,
    data_min: Option<f64>,
    data_max: Option<f64>,
    masked_or_non_finite_count: usize,
    no_finite_values: bool,
}

impl PlaneStats {
    fn clip_bounds_for(&self, stretch: &PlaneStretchSettings) -> Option<(f64, f64)> {
        let values = self.sorted_finite_values()?;
        let (min_value, max_value) = (*values.first()?, *values.last()?);
        match stretch.preset {
            PlaneStretchPreset::Percentile99 => Some(percentile_clip_bounds(&values, 0.01, 0.99)),
            PlaneStretchPreset::Percentile95 => Some(percentile_clip_bounds(&values, 0.05, 0.95)),
            PlaneStretchPreset::MinMax => Some((min_value, max_value)),
            PlaneStretchPreset::ZScale => zscale_like_clip_bounds(&values),
            PlaneStretchPreset::Manual => stretch.manual_clip,
        }
    }

    fn is_valid(&self, mask: Option<&Array2<bool>>, x: usize, y: usize) -> bool {
        mask.is_none_or(|data| data[[x, y]])
    }

    fn histogram_bins(&self, bins: usize) -> Vec<u32> {
        if bins == 0 || self.finite_values.is_empty() {
            return Vec::new();
        }
        let Some(min_value) = self.data_min else {
            return Vec::new();
        };
        let Some(max_value) = self.data_max else {
            return Vec::new();
        };
        let mut histogram = vec![0u32; bins];
        if (max_value - min_value).abs() < f64::EPSILON {
            histogram[bins / 2] = self.finite_values.len() as u32;
            return histogram;
        }
        for &value in &self.finite_values {
            let scaled = ((value - min_value) / (max_value - min_value)).clamp(0.0, 1.0);
            let index = ((scaled * bins.saturating_sub(1) as f64).round() as usize)
                .min(bins.saturating_sub(1));
            histogram[index] = histogram[index].saturating_add(1);
        }
        histogram
    }

    fn sorted_finite_values(&self) -> Option<Vec<f64>> {
        if self.finite_values.is_empty() {
            return None;
        }
        let mut values = self.finite_values.clone();
        values.sort_by(f64::total_cmp);
        Some(values)
    }
}

fn percentile_index(len: usize, percentile: f64) -> usize {
    ((len.saturating_sub(1)) as f64 * percentile).round() as usize
}

fn percentile_clip_bounds(values: &[f64], low_percentile: f64, high_percentile: f64) -> (f64, f64) {
    let low = percentile_index(values.len(), low_percentile);
    let high = percentile_index(values.len(), high_percentile);
    (values[low], values[high])
}

fn zscale_like_clip_bounds(values: &[f64]) -> Option<(f64, f64)> {
    let median = *values.get(values.len() / 2)?;
    let mut deviations = values
        .iter()
        .map(|value| (value - median).abs())
        .collect::<Vec<_>>();
    deviations.sort_by(f64::total_cmp);
    let mad = *deviations.get(deviations.len() / 2)?;
    let sigma = (1.4826 * mad).max(f64::EPSILON);
    let min_value = *values.first()?;
    let max_value = *values.last()?;
    let clip_min = (median - 2.5 * sigma).max(min_value);
    let clip_max = (median + 2.5 * sigma).min(max_value);
    Some(if clip_min <= clip_max {
        (clip_min, clip_max)
    } else {
        (min_value, max_value)
    })
}

fn index_label_width(len: usize) -> usize {
    len.saturating_sub(1).to_string().len().max(3)
}

fn format_axis_list(values: &[usize]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn sampled_axis_len(blc: usize, trc: usize, inc: usize) -> usize {
    (trc - blc) / inc + 1
}

fn selected_non_display_axes(
    window: &ImageViewWindow,
    non_display_axes: &[usize],
    non_display_indices: &[usize],
) -> Result<Vec<(usize, usize, usize)>, ImageError> {
    if non_display_axes.len() != non_display_indices.len() {
        return Err(ImageError::ShapeMismatch {
            expected: vec![non_display_axes.len()],
            got: vec![non_display_indices.len()],
        });
    }
    non_display_axes
        .iter()
        .copied()
        .zip(non_display_indices.iter().copied())
        .map(|(axis, index)| {
            let length = window.sampled_axis_len(axis);
            if index >= length {
                return Err(ImageError::ShapeMismatch {
                    expected: vec![length],
                    got: vec![index],
                });
            }
            let pixel = window.sampled_axis_value(axis, index).ok_or_else(|| {
                ImageError::ShapeMismatch {
                    expected: vec![length],
                    got: vec![index],
                }
            })?;
            Ok((axis, index, pixel))
        })
        .collect()
}

fn apply_non_display_axis_selections(
    pixel_indices: &mut [usize],
    window: &ImageViewWindow,
    non_display_axes: &[usize],
    non_display_indices: &[usize],
) -> Result<(), ImageError> {
    for (axis, _index, pixel) in
        selected_non_display_axes(window, non_display_axes, non_display_indices)?
    {
        pixel_indices[axis] = pixel;
    }
    Ok(())
}

fn parse_window_axis_values(
    label: &str,
    text: &str,
    shape: &[usize],
    default: &[usize],
) -> Result<Vec<usize>, ImageError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(default.to_vec());
    }
    let values = trimmed
        .split(|ch: char| ch == ',' || ch.is_whitespace())
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<usize>().map_err(|_| {
                ImageError::InvalidMetadata(format!(
                    "{label} expects comma-separated integer pixel indices"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.len() != shape.len() {
        return Err(ImageError::InvalidMetadata(format!(
            "{label} expects {} value(s), received {}",
            shape.len(),
            values.len()
        )));
    }
    Ok(values)
}

fn validate_window(
    shape: &[usize],
    blc: &[usize],
    trc: &[usize],
    inc: &[usize],
) -> Result<(), ImageError> {
    if blc.len() != shape.len() || trc.len() != shape.len() || inc.len() != shape.len() {
        return Err(ImageError::InvalidMetadata(
            "BLC/TRC/INC dimensionality does not match image shape".into(),
        ));
    }
    for axis in 0..shape.len() {
        if shape[axis] == 0 {
            return Err(ImageError::InvalidMetadata(format!(
                "axis {axis} has zero length"
            )));
        }
        if inc[axis] == 0 {
            return Err(ImageError::InvalidMetadata(format!(
                "INC axis {axis} must be >= 1"
            )));
        }
        if blc[axis] >= shape[axis] {
            return Err(ImageError::InvalidMetadata(format!(
                "BLC axis {axis}={} is outside image length {}",
                blc[axis], shape[axis]
            )));
        }
        if trc[axis] >= shape[axis] {
            return Err(ImageError::InvalidMetadata(format!(
                "TRC axis {axis}={} is outside image length {}",
                trc[axis], shape[axis]
            )));
        }
        if blc[axis] > trc[axis] {
            return Err(ImageError::InvalidMetadata(format!(
                "BLC axis {axis}={} must be <= TRC {}",
                blc[axis], trc[axis]
            )));
        }
    }
    Ok(())
}

fn window_start(cursor: usize, window: usize, full_len: usize) -> usize {
    cursor
        .saturating_sub(window / 2)
        .min(full_len.saturating_sub(window))
}

fn plane_cell_text(plane: &Array2<f64>, mask: Option<&Array2<bool>>, x: usize, y: usize) -> String {
    if mask.is_some_and(|mask_data| !mask_data[[x, y]]) {
        return "masked".into();
    }
    format_plane_value(plane[[x, y]])
}

fn format_plane_value(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value == f64::INFINITY {
        return "Inf".into();
    }
    if value == f64::NEG_INFINITY {
        return "-Inf".into();
    }
    let abs = value.abs();
    let candidates = if abs >= 1.0e4 || (abs > 0.0 && abs < 1.0e-3) {
        [
            format!("{value:.5e}"),
            format!("{value:.4e}"),
            format!("{value:.3e}"),
            format!("{value:.2e}"),
        ]
    } else {
        [
            trim_float_text(format!("{value:.7}")),
            trim_float_text(format!("{value:.5}")),
            trim_float_text(format!("{value:.4}")),
            trim_float_text(format!("{value:.3}")),
        ]
    };
    candidates
        .into_iter()
        .min_by_key(|candidate| candidate.len())
        .unwrap_or_else(|| value.to_string())
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

fn render_grid_cell(text: &str, width: usize, cursor: bool) -> String {
    if cursor && width >= 3 {
        let inner_width = width - 2;
        let inner = fit_cell_text(text, inner_width);
        format!("[{:>width$}]", inner, width = inner_width)
    } else {
        format!("{:>width$}", fit_cell_text(text, width), width = width)
    }
}

fn fit_cell_text(text: &str, width: usize) -> String {
    let chars = text.chars().count();
    if chars <= width {
        return text.to_string();
    }
    if width == 0 {
        return String::new();
    }
    if width == 1 {
        return "~".into();
    }
    let mut fitted = text.chars().take(width - 1).collect::<String>();
    fitted.push('~');
    fitted
}

fn sample_plane_axes<T: Clone>(plane: &Array2<T>, x_step: usize, y_step: usize) -> Array2<T> {
    let width = sampled_axis_len(0, plane.shape()[0].saturating_sub(1), x_step);
    let height = sampled_axis_len(0, plane.shape()[1].saturating_sub(1), y_step);
    Array2::from_shape_fn((width, height), |(x, y)| {
        plane[[x * x_step, y * y_step]].clone()
    })
}

fn determine_axis_model(image: &AnyPagedImage) -> ImageAxisModel {
    let display_axes = select_display_axes(image_coordinates(image), image.shape());
    let descriptors = build_axis_descriptors(image_coordinates(image), image.shape());
    let non_display_axes = display_axes
        .map(|display_axes| {
            let mut axes = non_singleton_axes(image.shape())
                .into_iter()
                .filter(|axis| !display_axes.contains(axis))
                .collect::<Vec<_>>();
            axes.sort_by_key(|axis| {
                let coord_type = descriptors
                    .get(*axis)
                    .map(|descriptor| descriptor.coord_type)
                    .unwrap_or(CoordinateType::Linear);
                (
                    match coord_type {
                        CoordinateType::Spectral => 0u8,
                        CoordinateType::Linear => 1,
                        CoordinateType::Direction => 2,
                        CoordinateType::Stokes => 3,
                        CoordinateType::Tabular => 4,
                    },
                    *axis,
                )
            });
            axes
        })
        .unwrap_or_default();
    ImageAxisModel {
        display_axes,
        non_display_axes,
    }
}

fn direction_axes(coords: &CoordinateSystem, shape: &[usize]) -> Option<[usize; 2]> {
    let mut offset = 0usize;
    for index in 0..coords.n_coordinates() {
        let coord = coords.coordinate(index);
        let n_axes = coord.n_pixel_axes();
        if coord.coordinate_type() == CoordinateType::Direction
            && n_axes == 2
            && offset + n_axes <= shape.len()
        {
            return Some([offset, offset + 1]);
        }
        offset += n_axes;
    }
    None
}

fn select_display_axes(coords: &CoordinateSystem, shape: &[usize]) -> Option<[usize; 2]> {
    if shape.len() < 2 {
        return None;
    }

    let mut candidates = Vec::new();
    if let Some(direction_axes) = direction_axes(coords, shape) {
        candidates.push(direction_axes);
    }
    let preferred_axes = preferred_display_axis_order(shape);
    for (index, &left) in preferred_axes.iter().enumerate() {
        for &right in preferred_axes.iter().skip(index + 1) {
            candidates.push([left, right]);
        }
    }

    candidates.into_iter().next()
}

fn preferred_display_axis_order(shape: &[usize]) -> Vec<usize> {
    let mut axes = non_singleton_axes(shape);
    axes.extend(
        shape
            .iter()
            .enumerate()
            .filter_map(|(axis, &len)| (len == 1).then_some(axis)),
    );
    axes
}

fn non_singleton_axes(shape: &[usize]) -> Vec<usize> {
    shape
        .iter()
        .enumerate()
        .filter_map(|(axis, &len)| (len > 1).then_some(axis))
        .collect()
}

fn squeezed_axes(shape: &[usize], display_axes: [usize; 2]) -> Vec<usize> {
    (0..shape.len())
        .filter(|axis| !display_axes.contains(axis))
        .collect()
}

fn squeeze_plane_axes<T: Clone>(mut array: ArrayD<T>, squeeze_axes: &[usize]) -> ArrayD<T> {
    let mut axes = squeeze_axes.to_vec();
    axes.sort_unstable_by(|left, right| right.cmp(left));
    for axis in axes {
        array = array.index_axis_move(Axis(axis), 0);
    }
    array
}

fn build_axis_descriptors(coords: &CoordinateSystem, shape: &[usize]) -> Vec<AxisDescriptor> {
    let mut descriptors = Vec::new();
    for index in 0..coords.n_coordinates() {
        let coord = coords.coordinate(index);
        let names = coord.axis_names();
        let units = coord.axis_units();
        let reference_values = coord.reference_value();
        let reference_pixels = coord.reference_pixel();
        let increments = coord.increment();
        for axis in 0..coord.n_pixel_axes() {
            descriptors.push(AxisDescriptor {
                coord_type: coord.coordinate_type(),
                name: names
                    .get(axis)
                    .cloned()
                    .unwrap_or_else(|| format!("Axis{}", descriptors.len())),
                unit: units.get(axis).cloned().unwrap_or_default(),
                reference_value: reference_values.get(axis).copied(),
                reference_pixel: reference_pixels.get(axis).copied(),
                increment: increments.get(axis).copied(),
            });
        }
    }
    while descriptors.len() < shape.len() {
        descriptors.push(AxisDescriptor {
            coord_type: CoordinateType::Linear,
            name: format!("Axis{}", descriptors.len()),
            unit: String::new(),
            reference_value: None,
            reference_pixel: None,
            increment: None,
        });
    }
    descriptors.truncate(shape.len());
    descriptors
}

fn coordinates_cover_image_axes(coords: &CoordinateSystem, shape: &[usize]) -> bool {
    coords.n_coordinates() > 0 && coords.n_pixel_axes() == shape.len()
}

fn collect_plane_stats(plane: &Array2<f64>, mask: Option<&Array2<bool>>) -> PlaneStats {
    let mut finite_values = Vec::new();
    let mut data_min = None::<f64>;
    let mut data_max = None::<f64>;
    let mut masked_or_non_finite_count = 0usize;
    for x in 0..plane.shape()[0] {
        for y in 0..plane.shape()[1] {
            let value = plane[[x, y]];
            let valid_mask = mask.is_none_or(|mask_data| mask_data[[x, y]]);
            if !valid_mask || !value.is_finite() {
                masked_or_non_finite_count += 1;
                continue;
            }
            data_min = Some(match data_min {
                Some(current) => current.min(value),
                None => value,
            });
            data_max = Some(match data_max {
                Some(current) => current.max(value),
                None => value,
            });
            finite_values.push(value);
        }
    }
    let no_finite_values = finite_values.is_empty();
    PlaneStats {
        finite_values,
        data_min,
        data_max,
        masked_or_non_finite_count,
        no_finite_values,
    }
}

fn image_coordinates(image: &AnyPagedImage) -> &CoordinateSystem {
    match image {
        AnyPagedImage::Float32(image) => image.coordinates(),
        AnyPagedImage::Float64(image) => image.coordinates(),
        AnyPagedImage::Complex32(image) => image.coordinates(),
        AnyPagedImage::Complex64(image) => image.coordinates(),
    }
}

fn image_units(image: &AnyPagedImage) -> &str {
    match image {
        AnyPagedImage::Float32(image) => image.units(),
        AnyPagedImage::Float64(image) => image.units(),
        AnyPagedImage::Complex32(image) => image.units(),
        AnyPagedImage::Complex64(image) => image.units(),
    }
}

fn image_misc_info(image: &AnyPagedImage) -> RecordValue {
    match image {
        AnyPagedImage::Float32(image) => image.misc_info(),
        AnyPagedImage::Float64(image) => image.misc_info(),
        AnyPagedImage::Complex32(image) => image.misc_info(),
        AnyPagedImage::Complex64(image) => image.misc_info(),
    }
}

fn image_image_info(image: &AnyPagedImage) -> Result<crate::image_info::ImageInfo, ImageError> {
    match image {
        AnyPagedImage::Float32(image) => image.image_info(),
        AnyPagedImage::Float64(image) => image.image_info(),
        AnyPagedImage::Complex32(image) => image.image_info(),
        AnyPagedImage::Complex64(image) => image.image_info(),
    }
}

fn image_history(image: &AnyPagedImage) -> Result<Vec<String>, ImageError> {
    match image {
        AnyPagedImage::Float32(image) => image.history(),
        AnyPagedImage::Float64(image) => image.history(),
        AnyPagedImage::Complex32(image) => image.history(),
        AnyPagedImage::Complex64(image) => image.history(),
    }
}

fn image_has_pixel_mask(image: &AnyPagedImage) -> bool {
    match image {
        AnyPagedImage::Float32(image) => image.has_pixel_mask(),
        AnyPagedImage::Float64(image) => image.has_pixel_mask(),
        AnyPagedImage::Complex32(image) => image.has_pixel_mask(),
        AnyPagedImage::Complex64(image) => image.has_pixel_mask(),
    }
}

fn image_default_mask_name(image: &AnyPagedImage) -> Option<String> {
    match image {
        AnyPagedImage::Float32(image) => image.default_mask_name(),
        AnyPagedImage::Float64(image) => image.default_mask_name(),
        AnyPagedImage::Complex32(image) => image.default_mask_name(),
        AnyPagedImage::Complex64(image) => image.default_mask_name(),
    }
}

fn image_get_mask(image: &AnyPagedImage) -> Result<Option<ArrayD<bool>>, ImageError> {
    match image {
        AnyPagedImage::Float32(image) => image.get_mask(),
        AnyPagedImage::Float64(image) => image.get_mask(),
        AnyPagedImage::Complex32(image) => image.get_mask(),
        AnyPagedImage::Complex64(image) => image.get_mask(),
    }
}

fn image_real_get_slice(
    image: &AnyPagedImage,
    start: &[usize],
    shape: &[usize],
) -> Result<ArrayD<f64>, ImageError> {
    match image {
        AnyPagedImage::Float32(image) => Ok(image.get_slice(start, shape)?.mapv(f64::from)),
        AnyPagedImage::Float64(image) => image.get_slice(start, shape),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => Err(
            ImageError::InvalidMetadata("complex images unsupported in wave 1".into()),
        ),
    }
}

fn image_real_get_at(image: &AnyPagedImage, position: &[usize]) -> Result<f64, ImageError> {
    match image {
        AnyPagedImage::Float32(image) => Ok(f64::from(image.get_at(position)?)),
        AnyPagedImage::Float64(image) => image.get_at(position),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => Err(
            ImageError::InvalidMetadata("complex images unsupported in wave 1".into()),
        ),
    }
}

fn axis_names(image: &AnyPagedImage) -> Vec<String> {
    build_axis_descriptors(image_coordinates(image), image.shape())
        .into_iter()
        .map(|axis| axis.name)
        .collect()
}

fn build_observation_lines(obs_info: &ObsInfo) -> Vec<String> {
    vec![
        format!(
            "telescope: {}",
            if obs_info.telescope.is_empty() {
                "<none>"
            } else {
                &obs_info.telescope
            }
        ),
        format!(
            "observer: {}",
            if obs_info.observer.is_empty() {
                "<none>"
            } else {
                &obs_info.observer
            }
        ),
        format!(
            "obs date: {}",
            obs_info
                .date
                .as_ref()
                .map_or_else(|| "<none>".into(), format_epoch_for_display)
        ),
        format!(
            "telescope position: {}",
            obs_info
                .telescope_position
                .as_ref()
                .map_or_else(|| "<none>".into(), format_position_for_display)
        ),
        format!(
            "pointing center: {}",
            format_pointing_center_for_display(obs_info)
        ),
    ]
}

fn format_single_beam_line(beam: crate::beam::GaussianBeam) -> Result<String, ImageError> {
    Ok(format!("beam: {}", format_beam_descriptor(beam)?))
}

fn format_beam_summary_lines(beam_set: &ImageBeamSet) -> Result<Vec<String>, ImageError> {
    if let Some(beam) = beam_set.single_beam() {
        return Ok(vec![format_single_beam_line(beam)?]);
    }
    if beam_set.is_empty() {
        return Ok(vec!["beam: none".into()]);
    }

    let (nchan, nstokes) = beam_set.shape();
    let mut lines = vec![format!(
        "beam: {} per-plane beams (channels={} stokes={})",
        beam_set.size(),
        nchan,
        nstokes,
    )];

    if let Some(min_beam) = beam_set.min_area_beam().copied() {
        let suffix = find_beam_position(beam_set, min_beam)
            .map(|(chan, stokes)| format!(" at chan={chan} stokes={stokes}"))
            .unwrap_or_default();
        lines.push(format!(
            "beam min area: {}{}",
            format_beam_descriptor(min_beam)?,
            suffix,
        ));
    }
    if let Some(median_beam) = beam_set.median_area_beam() {
        let suffix = find_beam_position(beam_set, median_beam)
            .map(|(chan, stokes)| format!(" at chan={chan} stokes={stokes}"))
            .unwrap_or_default();
        lines.push(format!(
            "beam median area: {}{}",
            format_beam_descriptor(median_beam)?,
            suffix,
        ));
    }
    if let Some(max_beam) = beam_set.max_area_beam().copied() {
        let suffix = find_beam_position(beam_set, max_beam)
            .map(|(chan, stokes)| format!(" at chan={chan} stokes={stokes}"))
            .unwrap_or_default();
        lines.push(format!(
            "beam max area: {}{}",
            format_beam_descriptor(max_beam)?,
            suffix,
        ));
    }

    Ok(lines)
}

fn format_beam_descriptor(beam: GaussianBeam) -> Result<String, ImageError> {
    Ok(format!(
        "major={} arcsec minor={} arcsec pa={} deg",
        trim_float_text(format!("{:.6}", beam.major_in("arcsec")?)),
        trim_float_text(format!("{:.6}", beam.minor_in("arcsec")?)),
        trim_float_text(format!("{:.6}", beam.position_angle_in("deg")?)),
    ))
}

fn find_beam_position(beam_set: &ImageBeamSet, target: GaussianBeam) -> Option<(usize, usize)> {
    for chan in 0..beam_set.nchan() {
        for stokes in 0..beam_set.nstokes() {
            if *beam_set.beam(chan, stokes) == target {
                return Some((chan, stokes));
            }
        }
    }
    None
}

fn format_epoch_for_display(epoch: &casacore_types::measures::epoch::MEpoch) -> String {
    let mjd = epoch.value().as_mjd();
    let civil = MvTime::from_mjd_days(mjd).format_dmy(0);
    format!(
        "{} {} ({} MJD)",
        civil,
        epoch.refer(),
        trim_float_text(format!("{:.11}", mjd)),
    )
}

fn format_reference_pixel_for_display(value: Option<f64>) -> String {
    value.map_or_else(|| "<none>".into(), |value| format!("{value} px"))
}

fn format_axis_value_for_display(value: Option<f64>, axis_name: &str, axis_unit: &str) -> String {
    let Some(value) = value else {
        return "<none>".into();
    };
    if is_right_ascension_axis(axis_name) {
        return format_right_ascension_labeled(value, 6);
    }
    if is_declination_axis(axis_name) {
        return format_declination_labeled(value, 5);
    }
    format_numeric_value_with_unit(value, fallback_axis_unit(axis_name, axis_unit))
}

fn format_axis_increment_for_display(
    value: Option<f64>,
    axis_name: &str,
    axis_unit: &str,
) -> String {
    let Some(value) = value else {
        return "<none>".into();
    };
    if is_right_ascension_axis(axis_name) {
        return format!(
            "{} arcsec/pixel",
            trim_float_text(format!(
                "{:.6}",
                angular_increment_arcseconds(value).value()
            ))
        );
    }
    if is_declination_axis(axis_name) {
        return format!(
            "{} arcsec/pixel",
            trim_float_text(format!(
                "{:.6}",
                declination_increment_arcseconds(value).value()
            ))
        );
    }
    let unit = fallback_axis_unit(axis_name, axis_unit);
    if unit == "unitless" {
        format!("{value} unitless/pixel")
    } else if let Some(formatted) = format_frequency_quantity_auto(value, unit) {
        format!("{formatted}/pixel")
    } else {
        format!("{value} {unit}/pixel")
    }
}

pub(crate) fn format_numeric_value_with_unit(value: f64, unit: &str) -> String {
    format_frequency_quantity_auto(value, unit).unwrap_or_else(|| format!("{value} {unit}"))
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
    Some(format!(
        "{} {display_unit}",
        trim_float_text(format!("{converted:.6}"))
    ))
}

fn fallback_axis_unit<'a>(axis_name: &'a str, axis_unit: &'a str) -> &'a str {
    if !axis_unit.is_empty() {
        axis_unit
    } else if axis_name.eq_ignore_ascii_case("Stokes") {
        "code"
    } else {
        "unitless"
    }
}

fn is_right_ascension_axis(axis_name: &str) -> bool {
    axis_name.eq_ignore_ascii_case("Right Ascension") || axis_name.eq_ignore_ascii_case("RA")
}

fn is_declination_axis(axis_name: &str) -> bool {
    axis_name.eq_ignore_ascii_case("Declination") || axis_name.eq_ignore_ascii_case("DEC")
}

fn format_position_for_display(position: &casacore_types::measures::position::MPosition) -> String {
    match position.refer() {
        PositionRef::ITRF => {
            let [x, y, z] = position.as_itrf();
            format!(
                "frame=ITRF x={} m y={} m z={} m",
                trim_float_text(format!("{x:.3}")),
                trim_float_text(format!("{y:.3}")),
                trim_float_text(format!("{z:.3}")),
            )
        }
        PositionRef::WGS84 => {
            let [lon_rad, lat_rad, height_m] = position.values();
            format!(
                "frame=WGS84 lon={} deg lat={} deg height={} m",
                trim_float_text(format!("{:.6}", lon_rad.to_degrees())),
                trim_float_text(format!("{:.6}", lat_rad.to_degrees())),
                trim_float_text(format!("{height_m:.3}")),
            )
        }
    }
}

fn format_pointing_center_for_display(obs_info: &ObsInfo) -> String {
    if obs_info.pointing_center_initial
        && obs_info.pointing_center_rad[0].abs() < f64::EPSILON
        && obs_info.pointing_center_rad[1].abs() < f64::EPSILON
    {
        return "<initial>".into();
    }

    let center = format!(
        "{}, {}",
        format_right_ascension_labeled(obs_info.pointing_center_rad[0], 6),
        format_declination_labeled(obs_info.pointing_center_rad[1], 5),
    );
    if obs_info.pointing_center_initial {
        format!("{center} (initial)")
    } else {
        center
    }
}

fn build_coordinate_summary_lines(coords: &CoordinateSystem, shape: &[usize]) -> Vec<String> {
    let mut lines = Vec::new();
    let mut pixel_axis_offset = 0usize;
    for index in 0..coords.n_coordinates() {
        let coord = coords.coordinate(index);
        let axis_names = coord.axis_names();
        let axis_units = coord.axis_units();
        let reference_values = coord.reference_value();
        let reference_pixels = coord.reference_pixel();
        let increments = coord.increment();
        let record = coord.to_record();

        if !lines.is_empty() {
            lines.push(String::new());
        }
        lines.push(format!(
            "{} {}: {}",
            coord.coordinate_type(),
            index,
            coordinate_header_details(coord.coordinate_type(), &record)
        ));
        for axis in 0..coord.n_world_axes() {
            let axis_name = axis_names
                .get(axis)
                .cloned()
                .unwrap_or_else(|| format!("Axis{}", pixel_axis_offset + axis));
            let axis_unit = axis_units.get(axis).cloned().unwrap_or_default();
            let axis_len = shape
                .get(pixel_axis_offset + axis)
                .copied()
                .unwrap_or_default();
            lines.push(format!(
                "  axis {}: name={} unit={} len={} ref_pix={} ref_val={} incr={}",
                pixel_axis_offset + axis,
                axis_name,
                if axis_unit.is_empty() {
                    "<none>".to_string()
                } else {
                    axis_unit.clone()
                },
                axis_len,
                format_reference_pixel_for_display(reference_pixels.get(axis).copied()),
                format_axis_value_for_display(
                    reference_values.get(axis).copied(),
                    &axis_name,
                    &axis_unit,
                ),
                format_axis_increment_for_display(
                    increments.get(axis).copied(),
                    &axis_name,
                    &axis_unit,
                ),
            ));
        }
        pixel_axis_offset += coord.n_pixel_axes();
    }
    lines
}

fn coordinate_header_details(coord_type: CoordinateType, record: &RecordValue) -> String {
    match coord_type {
        CoordinateType::Direction => {
            let frame = record_string(record, "direction_ref").unwrap_or("unknown");
            let projection = record_string(record, "projection").unwrap_or("unknown");
            format!("frame={frame} projection={projection}")
        }
        CoordinateType::Spectral => {
            let native_frame = record_string(record, "frequency_ref").unwrap_or("unknown");
            let frame = record_subrecord(record, "conversion")
                .and_then(|conversion| record_string(conversion, "system"))
                .unwrap_or(native_frame);
            let restfreq = record_f64(record, "restfreq")
                .map(|value| format!(" restfreq={}", format_numeric_value_with_unit(value, "Hz")))
                .unwrap_or_default();
            if frame == native_frame {
                format!("frame={frame}{restfreq}")
            } else {
                format!("frame={frame} native={native_frame}{restfreq}")
            }
        }
        CoordinateType::Stokes => {
            let stokes = record_stokes_values(record);
            if stokes.is_empty() {
                "values=<unknown>".into()
            } else {
                format!("values={}", stokes.join(","))
            }
        }
        CoordinateType::Linear => "linear mapping".into(),
        CoordinateType::Tabular => {
            let name = record_string(record, "name").unwrap_or("lookup");
            format!("lookup={name}")
        }
    }
}

fn record_string<'a>(record: &'a RecordValue, key: &str) -> Option<&'a str> {
    match record.get(key) {
        Some(Value::Scalar(ScalarValue::String(value))) => Some(value.as_str()),
        _ => None,
    }
}

fn record_subrecord<'a>(record: &'a RecordValue, key: &str) -> Option<&'a RecordValue> {
    match record.get(key) {
        Some(Value::Record(value)) => Some(value),
        _ => None,
    }
}

fn record_f64(record: &RecordValue, key: &str) -> Option<f64> {
    match record.get(key) {
        Some(Value::Scalar(ScalarValue::Float64(value))) => Some(*value),
        Some(Value::Scalar(ScalarValue::Float32(value))) => Some(f64::from(*value)),
        _ => None,
    }
}

fn record_stokes_values(record: &RecordValue) -> Vec<String> {
    match record.get("stokes") {
        Some(Value::Array(ArrayValue::Int32(values))) => values
            .iter()
            .map(|value| {
                StokesType::from_code(*value)
                    .map(|stokes| stokes.to_string())
                    .unwrap_or_else(|| value.to_string())
            })
            .collect(),
        Some(Value::Array(ArrayValue::String(values))) => values.iter().cloned().collect(),
        _ => Vec::new(),
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Scalar(ScalarValue::Bool(value)) => value.to_string(),
        Value::Scalar(ScalarValue::UInt8(value)) => value.to_string(),
        Value::Scalar(ScalarValue::UInt16(value)) => value.to_string(),
        Value::Scalar(ScalarValue::UInt32(value)) => value.to_string(),
        Value::Scalar(ScalarValue::Int16(value)) => value.to_string(),
        Value::Scalar(ScalarValue::Int32(value)) => value.to_string(),
        Value::Scalar(ScalarValue::Int64(value)) => value.to_string(),
        Value::Scalar(ScalarValue::Float32(value)) => value.to_string(),
        Value::Scalar(ScalarValue::Float64(value)) => value.to_string(),
        Value::Scalar(ScalarValue::Complex32(value)) => format!("{value:?}"),
        Value::Scalar(ScalarValue::Complex64(value)) => format!("{value:?}"),
        Value::Scalar(ScalarValue::String(value)) => value.clone(),
        Value::Array(array) => format!("{:?} array {:?}", array.primitive_type(), array.shape()),
        Value::Record(record) => format!("record({} fields)", record.len()),
        Value::TableRef(path) => format!("table:{path}"),
    }
}

#[cfg(test)]
mod tests {
    use casacore_coordinates::{
        CoordinateSystem, DirectionCoordinate, ObsInfo, Projection, ProjectionType,
        SpectralCoordinate,
    };
    use casacore_test_support::casatestdata_path;
    use casacore_types::measures::direction::DirectionRef;
    use casacore_types::measures::epoch::{EpochRef, MEpoch};
    use casacore_types::measures::frequency::FrequencyRef;
    use casacore_types::measures::position::MPosition;
    use casacore_types::quanta::{MvTime, Quantity, Unit};

    use super::*;
    use crate::beam::{GaussianBeam, ImageBeamSet};
    use crate::image::PagedImage;
    use crate::image_info::{ImageInfo, ImageType};

    fn direction_spectral_coords() -> CoordinateSystem {
        let mut coords = CoordinateSystem::new();
        coords.add_coordinate(Box::new(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, std::f64::consts::FRAC_PI_4],
            [-1e-4, 1e-4],
            [1.0, 1.0],
        )));
        coords.add_coordinate(Box::new(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1.42e9,
            1.0e6,
            0.0,
            1.42040575e9,
        )));
        coords
    }

    fn direction_tabular_spectral_coords() -> CoordinateSystem {
        let mut coords = CoordinateSystem::new();
        coords.add_coordinate(Box::new(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, std::f64::consts::FRAC_PI_4],
            [-1e-4, 1e-4],
            [1.0, 1.0],
        )));
        coords.add_coordinate(Box::new(
            SpectralCoordinate::from_tabular(
                FrequencyRef::LSRK,
                vec![0.0, 1.0, 3.0, 4.0],
                vec![1.42e9, 1.4205e9, 1.422e9, 1.423e9],
                1.42e9,
                5.0e5,
                0.0,
                1.42040575e9,
            )
            .unwrap(),
        ));
        coords
    }

    fn direction_spectral_coords_with_obs_info() -> CoordinateSystem {
        direction_spectral_coords().with_obs_info(
            ObsInfo::new("ALMA")
                .with_observer("Test Observer")
                .with_date(MEpoch::from_mjd(59000.25, EpochRef::UTC))
                .with_telescope_position(MPosition::new_itrf(
                    2_225_142.18,
                    -5_440_307.37,
                    -2_481_029.85,
                ))
                .with_pointing_center(0.0, std::f64::consts::FRAC_PI_4),
        )
    }

    #[test]
    fn open_real_images_and_probe_pixel_only_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pixel_only.image");
        let mut image =
            PagedImage::<f32>::create(vec![4, 3], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[4, 3]),
                    (0..12).map(|value| value as f32).collect(),
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let opened = OpenedImageView::open(&path).unwrap();
        assert_eq!(opened.pixel_type(), ImagePixelType::Float32);
        assert!(opened.capabilities().renderable_plane);
        assert!(opened.capabilities().pixel_only_mode);

        let probe = opened.probe((2, 1), 0).unwrap();
        assert_eq!(probe.pixel_indices, vec![2, 1]);
        assert_eq!(probe.value, 7.0);
        assert!(probe.world_axes.is_empty());
    }

    #[test]
    fn window_parameters_apply_roi_and_sampling_to_plane_and_probe() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("windowed.image");
        let values = (0..5)
            .flat_map(|x| (0..4).map(move |y| (x * 10 + y) as f32))
            .collect::<Vec<_>>();
        let mut image =
            PagedImage::<f32>::create(vec![5, 4], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&[5, 4]), values).unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let opened = OpenedImageView::open(&path).unwrap();
        let window = opened.window_from_text("1,1", "4,3", "2,1").unwrap();
        assert_eq!(window.format_blc(), "1,1");
        assert_eq!(window.format_trc(), "4,3");
        assert_eq!(window.format_inc(), "2,1");

        let probe = opened.probe_with_window((1, 2), &window, 0).unwrap();
        assert_eq!(probe.pixel_indices, vec![3, 3]);
        assert_eq!(probe.value, 33.0);

        let raster = opened.render_plane_with_window((8, 8), &window, 0).unwrap();
        assert_eq!(raster.width, 2);
        assert_eq!(raster.height, 3);

        let grid = opened
            .render_plane_value_grid_with_window((80, 6), &window, 0, (1, 1))
            .unwrap();
        assert!(grid.first().is_some_and(|line| line.contains("1")));
        assert!(grid.first().is_some_and(|line| line.contains("3")));
        assert!(grid.iter().any(|line| line.starts_with("  3 |")));
    }

    #[test]
    fn profile_extraction_follows_cursor_along_selected_non_display_axis() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("profile.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2, 3], direction_spectral_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let opened = OpenedImageView::open(&path).unwrap();
        let window = opened.default_window();
        let profile = opened
            .profile_with_window_and_axes((1, 1), &window, &[1], 2)
            .unwrap();
        assert_eq!(profile.axis, 2);
        assert_eq!(profile.axis_name, "Frequency");
        assert_eq!(profile.selected_sample_index, 1);
        assert_eq!(profile.samples.len(), 3);
        assert_eq!(profile.samples[0].pixel_index, 0);
        assert_eq!(profile.samples[0].value, 4.0);
        assert_eq!(profile.samples[1].value, 40.0);
        assert_eq!(profile.samples[2].value, 400.0);
        assert!(profile.samples[1].world_axis.is_some());
    }

    #[test]
    fn render_plane_downsamples_and_applies_nan_and_mask_rules() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("render.image");
        let mut image =
            PagedImage::<f64>::create(vec![4, 4], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[4, 4]),
                    vec![
                        0.0,
                        1.0,
                        2.0,
                        3.0,
                        4.0,
                        f64::NAN,
                        6.0,
                        7.0,
                        8.0,
                        9.0,
                        10.0,
                        11.0,
                        12.0,
                        13.0,
                        14.0,
                        15.0,
                    ],
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.make_mask("quality", true, true).unwrap();
        let mut mask = image.get_named_mask("quality").unwrap();
        mask[[0, 0]] = false;
        image.put_mask("quality", &mask).unwrap();
        image.save().unwrap();

        let opened = OpenedImageView::open(&path).unwrap();
        let raster = opened.render_plane((2, 2), 0).unwrap();

        assert_eq!(raster.width, 2);
        assert_eq!(raster.height, 2);
        assert_eq!(raster.pixels_u8.len(), 4);
        assert_eq!(raster.masked_or_non_finite_count, 2);
        assert!(!raster.no_finite_values);
    }

    #[test]
    fn render_plane_manual_stretch_preserves_requested_clip_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manual_stretch.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![0.0, 5.0, 10.0, 20.0]).unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let opened = OpenedImageView::open(&path).unwrap();
        let raster = opened
            .render_plane_with_window_and_axes_and_stretch(
                (2, 2),
                &opened.default_window(),
                &[],
                &PlaneStretchSettings {
                    preset: PlaneStretchPreset::Manual,
                    autoscale: PlaneAutoscaleMode::PerPlane,
                    manual_clip: Some((5.0, 10.0)),
                },
                None,
            )
            .unwrap();

        assert_eq!(raster.clip_min, 5.0);
        assert_eq!(raster.clip_max, 10.0);
        assert_eq!(raster.data_min, 0.0);
        assert_eq!(raster.data_max, 20.0);
        assert!(raster.value_unit.is_empty());
        assert_eq!(raster.histogram_bins.iter().sum::<u32>(), 4);
    }

    #[test]
    fn non_display_axis_selectors_render_3d_cubes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cube.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2, 3], direction_spectral_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let opened = OpenedImageView::open(&path).unwrap();
        assert!(opened.capabilities().non_display_axis_selectors);
        assert_eq!(opened.axis_model().non_display_axes, vec![2]);
        assert_eq!(opened.axis_model().display_axes, Some([0, 1]));

        let plane0 = opened.render_plane((2, 2), 0).unwrap();
        let plane2 = opened.render_plane((2, 2), 2).unwrap();
        assert_eq!(plane0.width, 2);
        assert_eq!(plane2.width, 2);

        let probe0 = opened.probe((1, 1), 0).unwrap();
        let probe2 = opened.probe((1, 1), 2).unwrap();
        assert_eq!(probe2.pixel_indices, vec![1, 1, 2]);
        assert_eq!(probe0.value, 4.0);
        assert_eq!(probe2.value, 400.0);
        assert_eq!(probe2.world_axes.len(), 3);
    }

    #[test]
    fn degenerate_axes_are_squeezed_for_plane_rendering() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("degenerate.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2, 1, 3], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 1, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let opened = OpenedImageView::open(&path).unwrap();
        assert!(opened.capabilities().renderable_plane);
        assert!(opened.capabilities().non_display_axis_selectors);
        assert_eq!(opened.axis_model().display_axes, Some([0, 1]));
        assert_eq!(opened.axis_model().non_display_axes, vec![3]);

        let plane = opened.render_plane((2, 2), 2).unwrap();
        assert_eq!(plane.width, 2);
        assert_eq!(plane.height, 2);

        let probe = opened.probe((1, 1), 2).unwrap();
        assert_eq!(probe.pixel_indices, vec![1, 1, 0, 2]);
        assert_eq!(probe.value, 400.0);
    }

    #[test]
    fn leading_singleton_axes_do_not_block_plane_selection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("leading_singleton.image");
        let mut image =
            PagedImage::<f32>::create(vec![1, 2, 2, 3], CoordinateSystem::new(), &path).unwrap();
        image.save().unwrap();

        let opened = OpenedImageView::open(&path).unwrap();
        assert!(opened.capabilities().renderable_plane);
        assert_eq!(opened.axis_model().display_axes, Some([1, 2]));
        assert_eq!(opened.axis_model().non_display_axes, vec![3]);
    }

    #[test]
    fn complex_images_degrade_but_multi_axis_images_stay_renderable() {
        let dir = tempfile::tempdir().unwrap();
        let complex_path = dir.path().join("complex.image");
        let mut complex_image = PagedImage::<casacore_types::Complex32>::create(
            vec![2, 2],
            CoordinateSystem::new(),
            &complex_path,
        )
        .unwrap();
        complex_image.save().unwrap();

        let complex_view = OpenedImageView::open(&complex_path).unwrap();
        assert!(complex_view.capabilities().complex_unsupported);
        assert!(!complex_view.capabilities().renderable_plane);

        let hyper_path = dir.path().join("hyper.image");
        let mut hyper_image =
            PagedImage::<f32>::create(vec![2, 2, 2, 2], CoordinateSystem::new(), &hyper_path)
                .unwrap();
        hyper_image.save().unwrap();

        let hyper_view = OpenedImageView::open(&hyper_path).unwrap();
        assert!(hyper_view.capabilities().renderable_plane);
        assert!(hyper_view.capabilities().non_display_axis_selectors);
        assert_eq!(hyper_view.axis_model().non_display_axes, vec![2, 3]);
        assert_eq!(
            hyper_view.status_line(),
            "pixel-only mode: coordinate reconstruction unavailable"
        );
    }

    #[test]
    fn no_finite_plane_renders_black() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nan.image");
        let mut image =
            PagedImage::<f64>::create(vec![2, 2], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2]),
                    vec![f64::NAN, f64::NAN, f64::NAN, f64::NAN],
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let opened = OpenedImageView::open(&path).unwrap();
        let raster = opened.render_plane((2, 2), 0).unwrap();
        assert!(raster.no_finite_values);
        assert!(raster.pixels_u8.iter().all(|&pixel| pixel == 0));
    }

    #[test]
    fn metadata_sections_include_coordinate_status() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata.image");
        let mut image = PagedImage::<f32>::create(
            vec![2, 2, 2],
            direction_spectral_coords_with_obs_info(),
            &path,
        )
        .unwrap();
        image.save().unwrap();

        let opened = OpenedImageView::open(&path).unwrap();
        let sections = opened.metadata_sections().unwrap();
        assert!(sections.iter().any(|section| section.title == "Summary"));
        assert!(
            sections
                .iter()
                .any(|section| section.title == "Observation")
        );
        assert!(sections.iter().any(|section| section.title == "Axes"));
        assert!(
            sections
                .iter()
                .any(|section| section.title == "Coordinates")
        );
    }

    #[test]
    fn plane_value_formatter_shows_one_more_significant_figure() {
        assert_eq!(format_plane_value(1.23456), "1.235");
        assert_eq!(format_plane_value(12.3456), "12.346");
        assert_eq!(format_plane_value(12_345.6), "1.23e4");
    }

    #[test]
    fn metadata_sections_format_radec_reference_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("radec-metadata.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2, 2], direction_spectral_coords(), &path).unwrap();
        image.save().unwrap();

        let axes = OpenedImageView::open(&path)
            .unwrap()
            .metadata_sections()
            .unwrap()
            .into_iter()
            .find(|section| section.title == "Axes")
            .unwrap();
        assert!(axes.lines.iter().any(|line| {
            line.contains("Right Ascension") && line.contains("ref_val=00:00:00.000000 hms")
        }));
        assert!(axes.lines.iter().any(|line| {
            line.contains("Declination") && line.contains("ref_val=+45.00.00.00000 dms")
        }));
        assert!(axes.lines.iter().any(|line| {
            line.contains("Right Ascension") && line.contains("incr=-20.626481 arcsec/pixel")
        }));
        assert!(axes.lines.iter().any(|line| {
            line.contains("Declination") && line.contains("incr=20.626481 arcsec/pixel")
        }));
    }

    #[test]
    fn metadata_sections_include_observation_details() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("observation-details.image");
        let mut image = PagedImage::<f32>::create(
            vec![2, 2, 2],
            direction_spectral_coords_with_obs_info(),
            &path,
        )
        .unwrap();
        image.save().unwrap();

        let observation = OpenedImageView::open(&path)
            .unwrap()
            .metadata_sections()
            .unwrap()
            .into_iter()
            .find(|section| section.title == "Observation")
            .unwrap();
        assert!(
            observation
                .lines
                .iter()
                .any(|line| line == "telescope: ALMA")
        );
        assert!(
            observation
                .lines
                .iter()
                .any(|line| line == "observer: Test Observer")
        );
        assert!(observation.lines.iter().any(|line| {
            line == &format!(
                "obs date: {} UTC (59000.25 MJD)",
                MvTime::from_mjd_days(59000.25).format_dmy(0)
            )
        }));
        assert!(observation.lines.iter().any(|line| {
            line.contains(
                "telescope position: frame=ITRF x=2225142.18 m y=-5440307.37 m z=-2481029.85 m",
            )
        }));
        assert!(observation.lines.iter().any(|line| {
            line.contains("pointing center: 00:00:00.000000 hms, +45.00.00.00000 dms")
        }));
    }

    #[test]
    fn wgs84_position_formatter_preserves_units_and_frame() {
        let rendered = format_position_for_display(&MPosition::new_wgs84(
            -107.618_334_f64.to_radians(),
            34.078_749_f64.to_radians(),
            2_124.0,
        ));
        assert!(rendered.starts_with("frame=WGS84 "));
        assert!(rendered.contains(" lon="));
        assert!(rendered.contains(" lat="));
        assert!(rendered.contains(" deg "));
        assert!(rendered.contains(" height=2124 m"));
    }

    #[test]
    fn summary_formats_single_beam_in_arcsec_and_degrees() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("beam-summary.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2], CoordinateSystem::new(), &path).unwrap();
        let arcsec = Unit::new("arcsec").unwrap();
        let deg = Unit::new("deg").unwrap();
        let beam = crate::beam::GaussianBeam::new(
            Quantity::new(3.5, "arcsec")
                .unwrap()
                .get_value_in(&Unit::new("rad").unwrap())
                .unwrap(),
            Quantity::new(2.25, "arcsec")
                .unwrap()
                .get_value_in(&Unit::new("rad").unwrap())
                .unwrap(),
            Quantity::new(171.3, "deg")
                .unwrap()
                .get_value_in(&Unit::new("rad").unwrap())
                .unwrap(),
        );
        image
            .set_image_info(&ImageInfo {
                beam_set: ImageBeamSet::new(beam),
                image_type: ImageType::Intensity,
                object_name: String::new(),
            })
            .unwrap();
        image.save().unwrap();

        let summary = OpenedImageView::open(&path)
            .unwrap()
            .metadata_sections()
            .unwrap()
            .into_iter()
            .find(|section| section.title == "Summary")
            .unwrap();
        let line = summary
            .lines
            .iter()
            .find(|line| line.starts_with("beam: "))
            .unwrap();
        assert!(line.contains("arcsec"));
        assert!(line.contains("deg"));
        assert!(line.contains(&trim_float_text(format!(
            "{:.6}",
            beam.major_in(arcsec.name()).unwrap()
        ))));
        assert!(line.contains(&trim_float_text(format!(
            "{:.6}",
            beam.position_angle_in(deg.name()).unwrap()
        ))));
    }

    #[test]
    fn coordinates_section_includes_direction_and_spectral_details() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordinates-details.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2, 2], direction_spectral_coords(), &path).unwrap();
        image.save().unwrap();

        let coordinates = OpenedImageView::open(&path)
            .unwrap()
            .metadata_sections()
            .unwrap()
            .into_iter()
            .find(|section| section.title == "Coordinates")
            .unwrap();
        assert!(
            coordinates
                .lines
                .iter()
                .any(|line| line.contains("Direction 0: frame=J2000 projection=SIN"))
        );
        assert!(coordinates.lines.iter().any(|line| {
            line.contains("axis 0")
                && line.contains("Right Ascension")
                && line.contains("ref_val=00:00:00.000000 hms")
        }));
        assert!(coordinates.lines.iter().any(|line| {
            line.contains("axis 1")
                && line.contains("Declination")
                && line.contains("ref_val=+45.00.00.00000 dms")
        }));
        assert!(coordinates.lines.iter().any(|line| {
            line.contains("axis 0") && line.contains("incr=-20.626481 arcsec/pixel")
        }));
        assert!(coordinates.lines.iter().any(|line| {
            line.contains("axis 1") && line.contains("incr=20.626481 arcsec/pixel")
        }));
        assert!(
            coordinates
                .lines
                .iter()
                .any(|line| line.contains("Spectral 1: frame=LSRK restfreq=1.420406 GHz"))
        );
        assert!(coordinates.lines.iter().any(|line| line.contains("axis 2")
            && line.contains("Frequency")
            && line.contains("incr=1 MHz/pixel")));
    }

    #[test]
    fn tabular_spectral_images_keep_world_coordinates_and_spectral_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tabular-spectral.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2, 5], direction_tabular_spectral_coords(), &path)
                .unwrap();
        image.save().unwrap();

        let opened = OpenedImageView::open(&path).unwrap();
        assert!(opened.capabilities().world_coords_available);
        assert!(!opened.capabilities().pixel_only_mode);

        let probe = opened.probe((1, 1), 2).unwrap();
        assert_eq!(probe.world_axes.len(), 3);
        assert!((probe.world_axes[2].value - 1.42125e9).abs() < 1.0);

        let coordinates = opened
            .metadata_sections()
            .unwrap()
            .into_iter()
            .find(|section| section.title == "Coordinates")
            .unwrap();
        assert!(
            coordinates
                .lines
                .iter()
                .any(|line| line.contains("Spectral 1: frame=LSRK restfreq=1.420406 GHz"))
        );
    }

    #[test]
    fn summary_includes_multi_beam_statistics_with_units() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multibeam-summary.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2], CoordinateSystem::new(), &path).unwrap();
        let beams = ImageBeamSet::from_grid(vec![
            vec![GaussianBeam::new(
                Quantity::new(3.0, "arcsec")
                    .unwrap()
                    .get_value_in(&Unit::new("rad").unwrap())
                    .unwrap(),
                Quantity::new(2.0, "arcsec")
                    .unwrap()
                    .get_value_in(&Unit::new("rad").unwrap())
                    .unwrap(),
                Quantity::new(10.0, "deg")
                    .unwrap()
                    .get_value_in(&Unit::new("rad").unwrap())
                    .unwrap(),
            )],
            vec![GaussianBeam::new(
                Quantity::new(5.0, "arcsec")
                    .unwrap()
                    .get_value_in(&Unit::new("rad").unwrap())
                    .unwrap(),
                Quantity::new(2.0, "arcsec")
                    .unwrap()
                    .get_value_in(&Unit::new("rad").unwrap())
                    .unwrap(),
                Quantity::new(30.0, "deg")
                    .unwrap()
                    .get_value_in(&Unit::new("rad").unwrap())
                    .unwrap(),
            )],
            vec![GaussianBeam::new(
                Quantity::new(4.0, "arcsec")
                    .unwrap()
                    .get_value_in(&Unit::new("rad").unwrap())
                    .unwrap(),
                Quantity::new(2.0, "arcsec")
                    .unwrap()
                    .get_value_in(&Unit::new("rad").unwrap())
                    .unwrap(),
                Quantity::new(20.0, "deg")
                    .unwrap()
                    .get_value_in(&Unit::new("rad").unwrap())
                    .unwrap(),
            )],
        ]);
        image
            .set_image_info(&ImageInfo {
                beam_set: beams,
                image_type: ImageType::Intensity,
                object_name: String::new(),
            })
            .unwrap();
        image.save().unwrap();

        let summary = OpenedImageView::open(&path)
            .unwrap()
            .metadata_sections()
            .unwrap()
            .into_iter()
            .find(|section| section.title == "Summary")
            .unwrap();
        assert!(
            summary
                .lines
                .iter()
                .any(|line| { line == "beam: 3 per-plane beams (channels=3 stokes=1)" })
        );
        assert!(summary.lines.iter().any(|line| {
            line.contains(
                "beam min area: major=3 arcsec minor=2 arcsec pa=10 deg at chan=0 stokes=0",
            )
        }));
        assert!(summary.lines.iter().any(|line| {
            line.contains(
                "beam median area: major=4 arcsec minor=2 arcsec pa=20 deg at chan=2 stokes=0",
            )
        }));
        assert!(summary.lines.iter().any(|line| {
            line.contains(
                "beam max area: major=5 arcsec minor=2 arcsec pa=30 deg at chan=1 stokes=0",
            )
        }));
    }

    #[test]
    fn shared_ngc5921_image_exposes_world_coordinates_when_testdata_is_available() {
        let Some(path) = casatestdata_path("unittest/imhead/ngc5921.clean.image") else {
            return;
        };
        if !path.exists() {
            return;
        }

        let opened = OpenedImageView::open(&path).unwrap();
        assert!(opened.capabilities().world_coords_available);
        assert!(!opened.capabilities().pixel_only_mode);
        let probe = opened.probe((128, 128), 0).unwrap();
        assert!(!probe.world_axes.is_empty());
    }

    #[test]
    fn shared_n4826_image_exposes_world_coordinates_when_testdata_is_available() {
        let Some(path) = casatestdata_path("unittest/imval/n4826_bima.im") else {
            return;
        };
        if !path.exists() {
            return;
        }

        let opened = OpenedImageView::open(&path).unwrap();
        assert!(opened.capabilities().world_coords_available);
        assert!(!opened.capabilities().pixel_only_mode);
        let probe = opened.probe((128, 128), 0).unwrap();
        assert!(!probe.world_axes.is_empty());
        let spectral = probe.world_axes.last().unwrap();
        assert_eq!(spectral.name, "Frequency");
        assert!((spectral.value - 115_022_033_339.319_76).abs() < 1_000.0);

        let coordinates = opened
            .metadata_sections()
            .unwrap()
            .into_iter()
            .find(|section| section.title == "Coordinates")
            .unwrap();
        assert!(coordinates.lines.iter().any(|line| {
            line.contains("Spectral 2: frame=LSRK native=LSRD restfreq=115.2712 GHz")
        }));

        let observation = opened
            .metadata_sections()
            .unwrap()
            .into_iter()
            .find(|section| section.title == "Observation")
            .unwrap();
        assert!(
            observation
                .lines
                .iter()
                .any(|line| line == "telescope: BIMA")
        );
        assert!(observation.lines.iter().any(|line| {
            line.starts_with("obs date: ")
                && line.contains(" UTC (")
                && line.contains(" MJD)")
                && line.contains('/')
        }));
        assert!(observation.lines.iter().any(|line| {
            line.contains("telescope position: frame=ITRF ")
                && line.contains(" x=")
                && line.contains(" y=")
                && line.contains(" z=")
        }));
        assert!(observation.lines.iter().any(|line| {
            line.contains("pointing center: ") && line.contains("hms") && line.contains("dms")
        }));

        let summary = opened
            .metadata_sections()
            .unwrap()
            .into_iter()
            .find(|section| section.title == "Summary")
            .unwrap();
        assert!(summary.lines.iter().any(|line| {
            line.starts_with("beam: ")
                && line.contains("arcsec")
                && line.contains("deg")
                && !line.contains(" rad")
        }));
    }
}
