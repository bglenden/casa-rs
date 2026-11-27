// SPDX-License-Identifier: LGPL-3.0-or-later

//! Normalized RMS comparison of persisted CASA images for test suites.

use std::path::Path;

use casa_images::PagedImage;

/// Default normalized RMS tolerance for comparing equivalent imaging outputs.
pub const IMAGING_NRMS_TOLERANCE: f64 = 1.0e-6;

/// The normalized RMS difference of the image at `other` from the one at
/// `reference`, over every pixel: `rms(other − reference) / rms(reference)`.
/// Pixels NaN in both (masked) are equal; a pixel NaN in one only, any
/// infinite pixel or a difference from an all-zero reference counts as
/// infinite, so a comparison never reads a missing value as agreement.
///
/// # Errors
///
/// When either image cannot be read, or their shapes differ.
pub fn image_nrms(reference: &Path, other: &Path) -> Result<f64, String> {
    let read = |path: &Path| {
        PagedImage::<f32>::open(path)
            .and_then(|image| image.get())
            .map_err(|error| format!("{}: {error}", path.display()))
    };
    let (reference, other) = (read(reference)?, read(other)?);
    if reference.shape() != other.shape() {
        return Err(format!(
            "shapes differ: {:?} against {:?}",
            reference.shape(),
            other.shape()
        ));
    }
    let (mut error, mut scale) = (0.0_f64, 0.0_f64);
    for (a, b) in reference.iter().zip(&other) {
        if a.is_nan() && b.is_nan() {
            continue;
        }
        if !(a.is_finite() && b.is_finite()) {
            return Ok(f64::INFINITY);
        }
        error += (f64::from(*a) - f64::from(*b)).powi(2);
        scale += f64::from(*a).powi(2);
    }
    Ok(if scale == 0.0 {
        if error == 0.0 { 0.0 } else { f64::INFINITY }
    } else {
        (error / scale).sqrt()
    })
}
