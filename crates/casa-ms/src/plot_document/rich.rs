// SPDX-License-Identifier: LGPL-3.0-or-later
//! Renderer-neutral image, circle and file-backed array layers (#1025).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The little-endian numeric representation in the caller's payload file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlotArrayDtype {
    /// IEEE float32, least-significant byte first.
    F32Le,
    /// IEEE float64, least-significant byte first.
    F64Le,
}

impl PlotArrayDtype {
    /// Bytes per element.
    #[must_use]
    pub const fn bytes(self) -> u64 {
        match self {
            Self::F32Le => 4,
            Self::F64Le => 8,
        }
    }
}

/// An array view into one raw payload file supplied by the calling renderer.
/// No filename is repeated here: all arrays belong to that one file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PlotBinaryArray {
    /// Byte offset of element zero.
    pub offset_bytes: u64,
    /// One dimension for points or two for an image, in C order.
    pub shape: Vec<u64>,
    /// Byte strides, or empty for contiguous C order. Zero strides permit
    /// a constant radius without writing the value once per circle.
    pub strides_bytes: Vec<u64>,
    /// Numeric representation.
    pub dtype: PlotArrayDtype,
}

/// A malformed array descriptor, rejected before a renderer maps its bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid plot array: {0}")]
pub struct PlotArrayError(pub &'static str);

impl PlotBinaryArray {
    /// Verify rank, strides, representability and the complete referenced
    /// byte range. A zero-length view may start at the end of the file.
    ///
    /// # Errors
    ///
    /// Invalid rank, stride count, arithmetic overflow or bytes outside the file.
    pub fn validate(&self, file_bytes: u64) -> Result<(), PlotArrayError> {
        let invalid = |message| PlotArrayError(message);
        if !(1..=2).contains(&self.shape.len()) {
            return Err(invalid("rank must be one or two"));
        }
        if self.shape.iter().any(|&dim| dim > isize::MAX as u64)
            || self
                .strides_bytes
                .iter()
                .any(|&stride| stride > isize::MAX as u64)
        {
            return Err(invalid("shape or stride exceeds the host address space"));
        }
        let count = self
            .shape
            .iter()
            .try_fold(1_u64, |n, &dim| n.checked_mul(dim))
            .ok_or(invalid("element count overflows"))?;
        if count > isize::MAX as u64 / self.dtype.bytes() {
            return Err(invalid("array exceeds the host address space"));
        }
        let contiguous;
        let strides = if self.strides_bytes.is_empty() {
            let mut stride = self.dtype.bytes();
            let mut values = Vec::with_capacity(self.shape.len());
            for &dim in self.shape.iter().rev() {
                values.push(stride);
                stride = stride.checked_mul(dim).ok_or(invalid("stride overflows"))?;
            }
            values.reverse();
            contiguous = values;
            &contiguous
        } else {
            &self.strides_bytes
        };
        if strides.len() != self.shape.len() {
            return Err(invalid("stride rank differs from the shape"));
        }
        let span = if count == 0 {
            0
        } else {
            self.shape
                .iter()
                .zip(strides)
                .try_fold(self.dtype.bytes(), |n, (&dim, &stride)| {
                    (dim - 1)
                        .checked_mul(stride)
                        .and_then(|extent| n.checked_add(extent))
                })
                .ok_or(invalid("byte extent overflows"))?
        };
        let end = self
            .offset_bytes
            .checked_add(span)
            .ok_or(invalid("byte offset overflows"))?;
        if end > isize::MAX as u64 {
            return Err(invalid("byte extent exceeds the host address space"));
        }
        if end > file_bytes {
            return Err(invalid("view extends beyond the payload file"));
        }
        Ok(())
    }
}

/// File-backed contents of a plot layer, independent of Matplotlib.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlotLayerData {
    /// Point coordinates, drawn as a line or markers according to the layer.
    Points {
        /// Horizontal coordinates, one per point.
        x: PlotBinaryArray,
        /// Vertical coordinates, paired with `x`.
        y: PlotBinaryArray,
    },
    /// A complete two-dimensional image, already in displayed row order.
    Image {
        /// Pixels in displayed row and column order.
        pixels: PlotBinaryArray,
        /// Left, right, bottom and top image edges in data coordinates.
        extent: [f64; 4],
        /// Lower and upper colour limits, computed by the native producer.
        range: [f64; 2],
        /// Renderer colour-map name, such as `jet`.
        color_map: String,
        /// Renderer interpolation name, such as `bilinear`.
        interpolation: String,
        /// Whether row zero is at the `upper` or `lower` edge.
        origin: String,
        /// Draw an adjacent colour scale.
        color_bar: bool,
    },
    /// Circle centres and radii in the layer's data coordinates.
    Circles {
        /// Horizontal centres.
        x: PlotBinaryArray,
        /// Vertical centres, paired with `x`.
        y: PlotBinaryArray,
        /// Radii in data units, paired with the centres.
        radii: PlotBinaryArray,
        /// Fill circles with the layer colour, otherwise draw their outlines.
        filled: bool,
        /// Put this layer in a lower-left anchored data-scale legend.
        #[serde(default)]
        anchored: bool,
    },
}

/// Optional presentation choices for lines and markers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PlotLayerStyle {
    /// Renderer colour name or hexadecimal RGB value.
    pub color: Option<String>,
    /// Renderer marker name, for example `o` or `,`.
    pub marker: Option<String>,
    /// Renderer line style, for example `-` or `None`.
    pub line_style: Option<String>,
}

/// Optional placement and style of an annotation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PlotTextStyle {
    /// Interpret the annotation position as fractions of the axes rectangle.
    pub axes_relative: bool,
    /// Anchor horizontally at `left`, `center` or `right`.
    pub horizontal_alignment: Option<String>,
    /// Anchor vertically at `top`, `center`, `bottom` or `baseline`.
    pub vertical_alignment: Option<String>,
    /// Renderer font-size name (such as `x-small`) or a point-size string.
    pub font_size: Option<String>,
    /// Optional background colour.
    pub background: Option<String>,
}

/// Scale of x and y data units on the page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlotAspect {
    /// Fill the available panel rectangle.
    Auto,
    /// Draw one data unit at the same scale on both axes.
    Equal,
    /// Equal data-unit scales while filling the panel, allowing Matplotlib
    /// to adjust limits at draw time (CASA's `axis('equal')`).
    EqualDataLimits,
}

/// Panel grid and optional subplot margins for a complete figure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PlotLayout {
    /// Number of rows in the panel grid.
    pub rows: u32,
    /// Number of columns in the panel grid.
    pub columns: u32,
    /// Aspect for the top-level single panel; individual panels have their own.
    pub aspect: Option<PlotAspect>,
    /// left, right, bottom, top, horizontal gap, vertical gap.
    pub margins: Option<[f64; 6]>,
    /// Renderer font-size name for labels and ticks; absent keeps defaults.
    pub font_size: Option<String>,
}

/// One figure for a host renderer. Numeric arrays refer to the raw payload
/// file the caller supplied to the native producer, never a path in this reply.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PlotFigure {
    /// PNG destination, absent for screen-only graphics.
    pub output_file: Option<std::path::PathBuf>,
    /// Keep the rendered figure visible in the calling Python session.
    pub show: bool,
    /// Complete native-prepared panels and artists.
    pub document: super::PlotDocument,
}

impl super::PlotDocument {
    /// Check all file-backed views before the host maps a payload file.
    /// Point coordinates and circle radii must have the same one-dimensional
    /// shape; image pixels must be two-dimensional.
    ///
    /// # Errors
    ///
    /// A malformed view or incompatible shapes in one layer.
    pub fn validate_payload(&self, file_bytes: u64) -> Result<(), PlotArrayError> {
        for layer in self
            .layers
            .iter()
            .chain(self.panels.iter().flat_map(|p| &p.layers))
        {
            match &layer.data {
                Some(PlotLayerData::Points { x, y }) => {
                    x.validate(file_bytes)?;
                    y.validate(file_bytes)?;
                    if x.shape.len() != 1 || x.shape != y.shape {
                        return Err(PlotArrayError("point coordinates have incompatible shapes"));
                    }
                }
                Some(PlotLayerData::Image { pixels, .. }) => {
                    pixels.validate(file_bytes)?;
                    if pixels.shape.len() != 2 {
                        return Err(PlotArrayError("image pixels must have two dimensions"));
                    }
                }
                Some(PlotLayerData::Circles { x, y, radii, .. }) => {
                    for array in [x, y, radii] {
                        array.validate(file_bytes)?;
                    }
                    if x.shape.len() != 1 || x.shape != y.shape || x.shape != radii.shape {
                        return Err(PlotArrayError("circle arrays have incompatible shapes"));
                    }
                }
                None => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_views_cover_interleaved_points_images_and_broadcast_radii() {
        let mut view = PlotBinaryArray {
            offset_bytes: 8,
            shape: vec![3],
            strides_bytes: vec![16],
            dtype: PlotArrayDtype::F64Le,
        };
        view.validate(48).expect("last interleaved coordinate");
        assert!(view.validate(47).is_err());
        view.strides_bytes = vec![0];
        view.validate(16).expect("one stored radius, three circles");
        assert!(view.validate(15).is_err());
        view.offset_bytes = 0;
        view.shape = vec![3, 4];
        view.strides_bytes.clear();
        view.dtype = PlotArrayDtype::F32Le;
        view.validate(48)
            .expect("three rows of four float32 pixels");
        assert!(view.validate(47).is_err());
    }

    #[test]
    fn malformed_views_are_refused_without_overflow_or_out_of_bounds_mapping() {
        let mut view = PlotBinaryArray {
            offset_bytes: 0,
            shape: vec![2],
            strides_bytes: vec![],
            dtype: PlotArrayDtype::F64Le,
        };
        for shape in [vec![], vec![1, 1, 1], vec![u64::MAX, 2]] {
            view.shape = shape;
            assert!(view.validate(u64::MAX).is_err());
        }
        view.shape = vec![2];
        view.strides_bytes = vec![u64::MAX];
        assert!(view.validate(u64::MAX).is_err());
        view.strides_bytes = vec![8, 8];
        assert!(view.validate(16).is_err());
        view.strides_bytes.clear();
        view.offset_bytes = u64::MAX - 8;
        assert!(view.validate(u64::MAX).is_err());
        view.shape = vec![0];
        view.offset_bytes = 16;
        view.validate(16).expect("empty view at EOF");
        assert!(view.validate(15).is_err());
    }
}
