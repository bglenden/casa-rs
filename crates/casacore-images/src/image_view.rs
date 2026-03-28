// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only image browser backend primitives.

use std::path::{Path, PathBuf};

use casacore_coordinates::{CoordinateSystem, CoordinateType};
use casacore_types::measures::direction::{format_declination, format_right_ascension};
use casacore_types::{ArrayD, RecordValue, ScalarValue, Value};
use ndarray::{Array2, Axis, Ix2, IxDyn};

use crate::error::ImageError;
use crate::image::{AnyPagedImage, ImagePixelType};

/// Capability flags exposed by the read-only image browser backend.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ImageViewCapabilities {
    pub renderable_plane: bool,
    pub world_coords_available: bool,
    pub pixel_only_mode: bool,
    pub single_hidden_axis_stepper: bool,
    pub mask_present: bool,
    pub complex_unsupported: bool,
}

/// Axis-selection model for the current image browser session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageAxisModel {
    pub display_axes: Option<[usize; 2]>,
    pub hidden_axis: Option<usize>,
    pub hidden_axis_name: Option<String>,
    pub hidden_axis_len: Option<usize>,
}

/// An 8-bit grayscale raster ready for UI transport.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaneRaster {
    pub width: usize,
    pub height: usize,
    pub pixels_u8: Vec<u8>,
    pub clip_min: f64,
    pub clip_max: f64,
    pub masked_or_non_finite_count: usize,
    pub no_finite_values: bool,
}

/// A named numeric axis value.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageAxisValue {
    pub name: String,
    pub unit: String,
    pub value: f64,
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

/// A titled metadata section for inspector rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageMetadataSection {
    pub title: String,
    pub lines: Vec<String>,
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
        let supported_hidden_axes = supported_hidden_axes(image.shape(), axis_model.display_axes);
        let world_coords_available =
            coordinates_cover_image_axes(image_coordinates(&image), image.shape())
                && image_coordinates(&image)
                    .to_world(&vec![0.0; image.shape().len()])
                    .is_ok();
        let capabilities = ImageViewCapabilities {
            renderable_plane: matches!(
                image.pixel_type(),
                ImagePixelType::Float32 | ImagePixelType::Float64
            ) && axis_model.display_axes.is_some()
                && supported_hidden_axes <= 1,
            world_coords_available,
            pixel_only_mode: !world_coords_available,
            single_hidden_axis_stepper: axis_model.hidden_axis.is_some(),
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

    /// Returns the display and hidden-axis model.
    pub fn axis_model(&self) -> &ImageAxisModel {
        &self.axis_model
    }

    /// Returns a short status line describing degraded or unsupported modes.
    pub fn status_line(&self) -> String {
        if self.capabilities.complex_unsupported {
            "complex images unsupported in wave 1".into()
        } else if !self.capabilities.renderable_plane {
            "viewer supports one 2D plane plus at most one non-degenerate hidden axis in wave 1"
                .into()
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
        let beam_line = if let Some(beam) = image_info.beam_set.single_beam() {
            format!(
                "beam: major={} rad minor={} rad pa={} rad",
                beam.major, beam.minor, beam.position_angle
            )
        } else if image_info.beam_set.is_multi() {
            format!("beam: {} per-plane beams", image_info.beam_set.size())
        } else {
            "beam: none".into()
        };

        let mut sections = Vec::new();
        sections.push(ImageMetadataSection {
            title: "Summary".into(),
            lines: vec![
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
                beam_line,
                format!("status: {}", self.status_line()),
            ],
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
                        format_optional_f64(axis.reference_pixel),
                        format_axis_value_for_display(axis.reference_value, &axis.name),
                        format_optional_f64(axis.increment),
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
        if let Some(hidden_axis) = self.axis_model.hidden_axis {
            coordinate_lines.push(format!(
                "hidden axis: {} ({hidden_axis}), index range 0..{}",
                self.axis_model
                    .hidden_axis_name
                    .as_deref()
                    .unwrap_or("Axis"),
                self.axis_model
                    .hidden_axis_len
                    .unwrap_or_default()
                    .saturating_sub(1),
            ));
        }
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
        if !self.capabilities.renderable_plane {
            return Err(ImageError::InvalidMetadata(self.status_line()));
        }
        let (plane, mask) = self.read_plane(hidden_index)?;
        let plane_stats = collect_plane_stats(&plane, mask.as_ref());
        let width = viewport.0.max(1).min(plane.shape()[0].max(1));
        let height = viewport.1.max(1).min(plane.shape()[1].max(1));
        let (clip_min, clip_max) = plane_stats.clip_bounds().unwrap_or((0.0, 0.0));

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
                let sample = if count == 0 {
                    0
                } else if plane_stats.no_finite_values {
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
        if !self.capabilities.renderable_plane {
            return Err(ImageError::InvalidMetadata(self.status_line()));
        }
        let (plane, mask) = self.read_plane(hidden_index)?;
        let plane_width = plane.shape()[0];
        let plane_height = plane.shape()[1];
        if plane_width == 0 || plane_height == 0 {
            return Ok(Vec::new());
        }

        let row_label_width = index_label_width(plane_height.max(plane_width));
        let min_cell_width = 7usize;
        let preferred_cell_width = 11usize;
        let _viewport_width = viewport_chars.0;
        let cols = plane_width;
        let cell_width = preferred_cell_width.max(min_cell_width);
        let rows = viewport_chars.1.saturating_sub(1).max(1).min(plane_height);

        let cursor_x = cursor_xy.0.min(plane_width.saturating_sub(1));
        let cursor_y = cursor_xy.1.min(plane_height.saturating_sub(1));
        let y_start = window_start(cursor_y, rows, plane_height);

        let mut lines = Vec::with_capacity(rows + 1);
        let mut header = format!("{:>width$} |", "y/x", width = row_label_width);
        for x in 0..cols {
            header.push(' ');
            header.push_str(&format!("{:>width$}", x, width = cell_width));
        }
        lines.push(header);

        for y in y_start..y_start + rows {
            let mut line = format!("{:>width$} |", y, width = row_label_width);
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
        let display_axes = self
            .axis_model
            .display_axes
            .ok_or_else(|| ImageError::InvalidMetadata(self.status_line()))?;
        let mut pixel_indices = vec![0usize; self.shape().len()];
        pixel_indices[display_axes[0]] = pixel_xy.0;
        pixel_indices[display_axes[1]] = pixel_xy.1;
        if let Some(hidden_axis) = self.axis_model.hidden_axis {
            let hidden_len = self.shape()[hidden_axis];
            if hidden_index >= hidden_len {
                return Err(ImageError::ShapeMismatch {
                    expected: vec![hidden_len],
                    got: vec![hidden_index],
                });
            }
            pixel_indices[hidden_axis] = hidden_index;
        }
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

    fn read_plane(
        &self,
        hidden_index: usize,
    ) -> Result<(Array2<f64>, Option<Array2<bool>>), ImageError> {
        let display_axes = self
            .axis_model
            .display_axes
            .ok_or_else(|| ImageError::InvalidMetadata(self.status_line()))?;
        let mut start = vec![0usize; self.shape().len()];
        let mut shape = self.shape().to_vec();
        let squeeze_axes = squeezed_axes(self.shape(), display_axes, self.axis_model.hidden_axis);
        if let Some(hidden_axis) = self.axis_model.hidden_axis {
            let hidden_len = self.shape()[hidden_axis];
            if hidden_index >= hidden_len {
                return Err(ImageError::ShapeMismatch {
                    expected: vec![hidden_len],
                    got: vec![hidden_index],
                });
            }
            start[hidden_axis] = hidden_index;
            shape[hidden_axis] = 1;
        }
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

        let mask = match image_get_mask(&self.image)? {
            Some(mask_data) => Some(
                squeeze_plane_axes(mask_data, &squeeze_axes)
                    .into_dimensionality::<Ix2>()
                    .map_err(|_| ImageError::InvalidMetadata("mask plane is not 2D".into()))?,
            ),
            None => None,
        };

        Ok((plane, mask))
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
    masked_or_non_finite_count: usize,
    no_finite_values: bool,
}

impl PlaneStats {
    fn clip_bounds(&self) -> Option<(f64, f64)> {
        if self.finite_values.is_empty() {
            return None;
        }
        let mut values = self.finite_values.clone();
        values.sort_by(f64::total_cmp);
        let low = percentile_index(values.len(), 0.01);
        let high = percentile_index(values.len(), 0.99);
        Some((values[low], values[high]))
    }

    fn is_valid(&self, mask: Option<&Array2<bool>>, x: usize, y: usize) -> bool {
        mask.is_none_or(|data| data[[x, y]])
    }
}

fn percentile_index(len: usize, percentile: f64) -> usize {
    ((len.saturating_sub(1)) as f64 * percentile).round() as usize
}

fn index_label_width(len: usize) -> usize {
    len.saturating_sub(1).to_string().len().max(3)
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
            format!("{value:.4e}"),
            format!("{value:.3e}"),
            format!("{value:.2e}"),
            format!("{value:.1e}"),
        ]
    } else {
        [
            trim_float_text(format!("{value:.6}")),
            trim_float_text(format!("{value:.4}")),
            trim_float_text(format!("{value:.3}")),
            trim_float_text(format!("{value:.2}")),
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

fn determine_axis_model(image: &AnyPagedImage) -> ImageAxisModel {
    let display_axes = select_display_axes(image_coordinates(image), image.shape());
    let hidden_axis = display_axes.and_then(|display_axes| {
        let mut hidden = non_singleton_axes(image.shape())
            .into_iter()
            .filter(|axis| !display_axes.contains(axis));
        let hidden_axis = hidden.next()?;
        if hidden.next().is_some() {
            None
        } else {
            Some(hidden_axis)
        }
    });
    ImageAxisModel {
        display_axes,
        hidden_axis,
        hidden_axis_name: hidden_axis.map(|axis| axis_names(image)[axis].clone()),
        hidden_axis_len: hidden_axis.map(|axis| image.shape()[axis]),
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

    candidates
        .into_iter()
        .find(|axes| supported_hidden_axes(shape, Some(*axes)) <= 1)
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

fn supported_hidden_axes(shape: &[usize], display_axes: Option<[usize; 2]>) -> usize {
    let Some(display_axes) = display_axes else {
        return usize::MAX;
    };
    non_singleton_axes(shape)
        .into_iter()
        .filter(|axis| !display_axes.contains(axis))
        .count()
}

fn squeezed_axes(
    shape: &[usize],
    display_axes: [usize; 2],
    hidden_axis: Option<usize>,
) -> Vec<usize> {
    (0..shape.len())
        .filter(|axis| !display_axes.contains(axis) && Some(*axis) != hidden_axis)
        .chain(hidden_axis)
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
    let mut masked_or_non_finite_count = 0usize;
    for x in 0..plane.shape()[0] {
        for y in 0..plane.shape()[1] {
            let value = plane[[x, y]];
            let valid_mask = mask.is_none_or(|mask_data| mask_data[[x, y]]);
            if !valid_mask || !value.is_finite() {
                masked_or_non_finite_count += 1;
                continue;
            }
            finite_values.push(value);
        }
    }
    let no_finite_values = finite_values.is_empty();
    PlaneStats {
        finite_values,
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

fn format_optional_f64(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value}"))
        .unwrap_or_else(|| "<none>".into())
}

fn format_axis_value_for_display(value: Option<f64>, axis_name: &str) -> String {
    let Some(value) = value else {
        return "<none>".into();
    };
    if is_right_ascension_axis(axis_name) {
        return format_right_ascension(value, 6);
    }
    if is_declination_axis(axis_name) {
        return format_declination(value, 5);
    }
    format!("{value}")
}

fn is_right_ascension_axis(axis_name: &str) -> bool {
    axis_name.eq_ignore_ascii_case("Right Ascension") || axis_name.eq_ignore_ascii_case("RA")
}

fn is_declination_axis(axis_name: &str) -> bool {
    axis_name.eq_ignore_ascii_case("Declination") || axis_name.eq_ignore_ascii_case("DEC")
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
        CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    };
    use casacore_test_support::casatestdata_path;
    use casacore_types::measures::direction::DirectionRef;
    use casacore_types::measures::frequency::FrequencyRef;

    use super::*;
    use crate::image::PagedImage;

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
    fn hidden_axis_stepper_renders_3d_cubes() {
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
        assert!(opened.capabilities().single_hidden_axis_stepper);
        assert_eq!(opened.axis_model().hidden_axis, Some(2));
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
        assert!(opened.capabilities().single_hidden_axis_stepper);
        assert_eq!(opened.axis_model().display_axes, Some([0, 1]));
        assert_eq!(opened.axis_model().hidden_axis, Some(3));

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
        assert_eq!(opened.axis_model().hidden_axis, Some(3));
    }

    #[test]
    fn complex_and_4d_images_open_in_metadata_mode() {
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
        assert!(!hyper_view.capabilities().renderable_plane);
        assert_eq!(
            hyper_view.status_line(),
            "viewer supports one 2D plane plus at most one non-degenerate hidden axis in wave 1"
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
        let mut image =
            PagedImage::<f32>::create(vec![2, 2, 2], direction_spectral_coords(), &path).unwrap();
        image.save().unwrap();

        let opened = OpenedImageView::open(&path).unwrap();
        let sections = opened.metadata_sections().unwrap();
        assert!(sections.iter().any(|section| section.title == "Summary"));
        assert!(sections.iter().any(|section| section.title == "Axes"));
        assert!(
            sections
                .iter()
                .any(|section| section.title == "Coordinates")
        );
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
            line.contains("Right Ascension") && line.contains("ref_val=00:00:00.000000")
        }));
        assert!(axes.lines.iter().any(|line| {
            line.contains("Declination") && line.contains("ref_val=+45.00.00.00000")
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
    }
}
