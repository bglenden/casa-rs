// SPDX-License-Identifier: LGPL-3.0-or-later
//! Imaging port step 4's route law (#932): a request imaged on the
//! observation-batch route ([`SourceRoute::ObservationBatches`]) gives the
//! products the selected-observation route gives, each to a normalized RMS
//! of [`TOLERANCE`] (owner decision 4 of `imaging-port.md` §3). The two
//! routes image the same samples with the same geometry, so they differ
//! only by summation order. Evidence for the port only: it goes with the
//! selected-observation route at the cutover.

use std::path::{Path, PathBuf};

use casa_images::PagedImage;

use crate::{ImagingRequest, SourceRoute};

/// The law's bound on each product's normalized RMS difference.
pub const TOLERANCE: f64 = 1.0e-6;

/// `request` on the observation-batch route, its products named
/// `<imagename>.batches*` beside the request's own.
#[must_use]
pub fn on_batches(request: &ImagingRequest) -> ImagingRequest {
    let mut batches = request.clone();
    batches.source_route = SourceRoute::ObservationBatches;
    batches.imagename = batch_prefix(&request.imagename);
    batches
}

/// The product prefix [`on_batches`] gives `imagename`.
#[must_use]
pub fn batch_prefix(imagename: &Path) -> PathBuf {
    let mut prefix = imagename.as_os_str().to_owned();
    prefix.push(".batches");
    PathBuf::from(prefix)
}

/// Whether the batch route can run `request`: it writes no model or
/// corrected column yet (step 5).
#[must_use]
pub fn applies_to(request: &ImagingRequest) -> bool {
    !(request.savemodel || request.save_continuum_residual)
}

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
