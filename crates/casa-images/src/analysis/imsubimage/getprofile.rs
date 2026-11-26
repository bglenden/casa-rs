// SPDX-License-Identifier: LGPL-3.0-or-later

mod coordinates;

use std::path::Path;

use casa_core::{RecordValue, quanta::Quantity};

use crate::{ImageError, ImageInterface};

use super::{CollapseFunction, CollapsePixel, ImageCollapse, ImageCollapseOptions, SubimageRegion};

pub use coordinates::ProfileFrequencyFrame;
use coordinates::{ProfileCoordinate, SpectralDisplay};

/// Parameters for a masked one-dimensional reduction of an image.
pub struct ReadProfileOptions<'a> {
    /// Zero-based pixel axis retained in the profile.
    pub axis: usize,
    /// Statistic applied to each plane perpendicular to the profile axis.
    pub function: CollapseFunction,
    /// Pixel, saved or textual region.
    pub region: Option<SubimageRegion<'a>>,
    /// Optional LEL expression to combine with the pixel mask.
    pub mask: Option<&'a str>,
    /// Allow broadcasting compatible expression masks.
    pub stretch: bool,
    /// Display unit, or empty to retain the input world units.
    pub unit: &'a str,
    /// CASA spectral display name or prefix.
    pub spectral_type: &'a str,
    /// Optional rest frequency overriding the source metadata for velocity.
    pub rest_frequency: Option<&'a Quantity>,
    /// Optional conversion frame and the observation context required for it.
    pub spectral_frame: Option<ProfileFrequencyFrame>,
    /// Optional text report destination, checked against named mask operands.
    pub logfile: Option<&'a Path>,
}

/// Collapsed profile samples and their original image positions.
pub struct ImageReadProfile<T> {
    /// Profile values retaining the source's pixel type.
    pub values: Vec<T>,
    /// Validity of each reduction result.
    pub mask: Vec<bool>,
    /// The number of unmasked input samples contributing to each plane.
    pub counts: Vec<usize>,
    /// Profile coordinates in `x_unit`.
    pub coordinates: Vec<f64>,
    /// Original image pixel numbers for the sampled planes.
    pub planes: Vec<f64>,
    /// The profile-coordinate unit.
    pub x_unit: String,
    /// Brightness unit of the reduced values.
    pub y_unit: String,
    /// Profile's original axis name for the text report.
    pub axis_name: String,
}

/// Read an image profile along a retained axis, keeping values, masks and counts together.
///
/// # Errors
/// An invalid selection, coordinate system, display unit or image read.
pub fn read_image_profile<T: CollapsePixel>(
    source: &dyn ImageInterface<T>,
    native: RecordValue,
    beams: &RecordValue,
    options: ReadProfileOptions<'_>,
) -> Result<ImageReadProfile<T>, ImageError> {
    let axis = options.axis;
    if axis >= source.ndim() {
        return Err(invalid(format!(
            "Specified zero-based axis ({axis}) must be less than the number of image axes ({})",
            source.ndim()
        )));
    }
    let spec = SpectralDisplay::parse(options.spectral_type)?;
    let reduced: Vec<_> = (0..source.ndim()).filter(|&index| index != axis).collect();
    let collapse = ImageCollapse::new(
        source,
        native,
        ImageCollapseOptions {
            function: options.function,
            axes: &reduced,
            region: options.region,
            spatial: None,
            chans: None,
            stokes: None,
            mask: options.mask,
            stretch: options.stretch,
            destination: options.logfile,
        },
        beams,
    )?;
    let plan = &collapse.section;
    let start = plan.selection.start[axis];
    let stride = plan
        .selection
        .stride
        .as_ref()
        .map_or(1, |stride| stride[axis]);
    let planes: Vec<_> = (0..plan.shape[axis])
        .map(|index| (start + index * stride) as f64)
        .collect();
    let coordinate = ProfileCoordinate::new(source.coordinates(), &plan.coordinates, axis)?;
    let (coordinates, x_unit, axis_name) = coordinate.evaluate(
        &planes,
        options.unit,
        spec,
        options.rest_frequency,
        options.spectral_frame,
    )?;
    let samples = collapse.collect_profile(source, axis)?;
    Ok(ImageReadProfile {
        values: samples.values,
        mask: samples.mask,
        counts: samples.counts,
        coordinates,
        planes,
        x_unit,
        y_unit: collapse.profile_unit().to_owned(),
        axis_name,
    })
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}
