// SPDX-License-Identifier: LGPL-3.0-or-later
//! Imaging port step 4's route law (#932): a request imaged on the
//! observation-batch route ([`SourceRoute::ObservationBatches`]) gives the
//! products the selected-observation route gives, each to a normalized RMS
//! of [`TOLERANCE`] (owner decision 4 of `imaging-port.md` §3). The two
//! routes image the same samples with the same geometry, so they differ
//! only by summation order. A request that writes visibilities back writes
//! them, on the batch route, to a copy of the MeasurementSet taken before
//! either route wrote ([`batch_copy`]); each column written then agrees
//! to the same bound ([`column_nrms`]), over every cell. Evidence for the
//! port only: it goes with the selected-observation route at the cutover.

use std::path::{Path, PathBuf};

use casa_images::PagedImage;
use casa_ms::MeasurementSet;
use casa_types::ArrayValue;

use crate::{ImagingRequest, SourceRoute};

/// The law's bound on each product's normalized RMS difference.
pub const TOLERANCE: f64 = 1.0e-6;

/// `request` on the observation-batch route, its products named
/// `<imagename>.batches*` beside the request's own; one that writes
/// visibilities back reads and writes [`batch_copy`] of its MeasurementSet.
#[must_use]
pub fn on_batches(request: &ImagingRequest) -> ImagingRequest {
    let mut batches = request.clone();
    batches.source_route = SourceRoute::ObservationBatches;
    batches.imagename = batch_prefix(&request.imagename);
    if !written_columns(request).is_empty() {
        batches.vis = batch_copy(&request.vis);
    }
    batches
}

/// The columns `request` writes back: `MODEL_DATA` for `savemodel`,
/// `CORRECTED_DATA` for the continuum residual.
#[must_use]
pub fn written_columns(request: &ImagingRequest) -> Vec<&'static str> {
    let mut columns = Vec::new();
    if request.savemodel {
        columns.push("MODEL_DATA");
    }
    if request.save_continuum_residual {
        columns.push("CORRECTED_DATA");
    }
    columns
}

/// The copy of `measurement_set` a writing request's batch-route run
/// writes: `<measurement_set>.batches`, beside it.
#[must_use]
pub fn batch_copy(measurement_set: &Path) -> PathBuf {
    let mut copy = measurement_set.as_os_str().to_owned();
    copy.push(".batches");
    PathBuf::from(copy)
}

/// Copy `measurement_set` to [`batch_copy`], replacing an earlier copy:
/// what a writing request's batch-route run writes, taken before either
/// route writes.
///
/// # Errors
///
/// When the directory cannot be copied.
pub fn copy_for_batches(measurement_set: &Path) -> std::io::Result<PathBuf> {
    let copy = batch_copy(measurement_set);
    if copy.exists() {
        std::fs::remove_dir_all(&copy)?;
    }
    copy_directory(measurement_set, &copy)?;
    Ok(copy)
}

fn copy_directory(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_directory(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

/// The normalized RMS difference of `column` of the MeasurementSet at
/// `other` from the one at `reference`, over every MAIN cell, as
/// [`image_nrms`] compares pixels: `rms(other − reference) /
/// rms(reference)`, a cell only one holds or a non-finite value counting
/// as infinite.
///
/// # Errors
///
/// When either column cannot be read, or the MeasurementSets' rows or
/// cell shapes differ.
pub fn column_nrms(reference: &Path, other: &Path, column: &str) -> Result<f64, String> {
    let read = |path: &Path| {
        let ms =
            MeasurementSet::open(path).map_err(|error| format!("{}: {error}", path.display()))?;
        let main = ms.main_table();
        let rows: Vec<usize> = (0..main.row_count()).collect();
        main.column_accessor(column)
            .and_then(|cells| cells.array_cells_owned_uncached(&rows))
            .map_err(|error| format!("{} {column}: {error}", path.display()))
    };
    let (reference, other) = (read(reference)?, read(other)?);
    if reference.len() != other.len() {
        return Err(format!("{} rows against {}", reference.len(), other.len()));
    }
    let (mut error, mut scale) = (0.0_f64, 0.0_f64);
    for (row, (a, b)) in reference.iter().zip(&other).enumerate() {
        let (Some(ArrayValue::Complex32(a)), Some(ArrayValue::Complex32(b))) = (a, b) else {
            if a.is_none() && b.is_none() {
                continue;
            }
            return Ok(f64::INFINITY);
        };
        if a.shape() != b.shape() {
            return Err(format!(
                "row {row}: cell shapes differ: {:?} against {:?}",
                a.shape(),
                b.shape()
            ));
        }
        for (a, b) in a.iter().zip(b) {
            if !(a.re.is_finite() && a.im.is_finite() && b.re.is_finite() && b.im.is_finite()) {
                return Ok(f64::INFINITY);
            }
            error += f64::from((a - b).norm_sqr());
            scale += f64::from(a.norm_sqr());
        }
    }
    Ok(if scale == 0.0 {
        if error == 0.0 { 0.0 } else { f64::INFINITY }
    } else {
        (error / scale).sqrt()
    })
}

/// The product prefix [`on_batches`] gives `imagename`.
#[must_use]
pub fn batch_prefix(imagename: &Path) -> PathBuf {
    let mut prefix = imagename.as_os_str().to_owned();
    prefix.push(".batches");
    PathBuf::from(prefix)
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
