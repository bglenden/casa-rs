// SPDX-License-Identifier: LGPL-3.0-or-later
//! Products of two runs compared by normalized RMS: images pixel for pixel
//! and a MeasurementSet column cell for cell, for the laws that hold two
//! runs of one request equal (waves against a resident cube, one store
//! against its conversion) to [`TOLERANCE`], owner decision 4 of
//! `imaging-port.md` §3. Runs that image the same samples differ only by
//! summation order.

use std::path::Path;

use casa_images::PagedImage;
use casa_ms::MeasurementSet;
use casa_types::ArrayValue;

/// The laws' bound on each product's normalized RMS difference.
pub const TOLERANCE: f64 = 1.0e-6;

/// MAIN rows [`column_nrms`] reads at a time, so a column is never held
/// whole.
const COMPARED_ROWS: usize = 1024;

/// The normalized RMS difference of `column` of the MeasurementSet at
/// `other` from the one at `reference`, over every MAIN cell, as
/// [`image_nrms`] compares pixels: `rms(other − reference) /
/// rms(reference)`, a cell only one holds or a non-finite value counting
/// as infinite. The cells are read 1,024 rows at a time.
///
/// # Errors
///
/// When either column cannot be read, or the MeasurementSets' rows or
/// cell shapes differ.
pub fn column_nrms(reference: &Path, other: &Path, column: &str) -> Result<f64, String> {
    let open = |path: &Path| {
        MeasurementSet::open(path).map_err(|error| format!("{}: {error}", path.display()))
    };
    let (reference, other) = ((reference, open(reference)?), (other, open(other)?));
    let (rows, other_rows) = (reference.1.row_count(), other.1.row_count());
    if rows != other_rows {
        return Err(format!("{rows} rows against {other_rows}"));
    }
    let mut sums = (0.0_f64, 0.0_f64);
    let mut chunk = Vec::with_capacity(COMPARED_ROWS.min(rows));
    for start in (0..rows).step_by(COMPARED_ROWS) {
        chunk.clear();
        chunk.extend(start..rows.min(start + COMPARED_ROWS));
        let read = |(path, ms): &(&Path, MeasurementSet)| {
            ms.main_table()
                .column_accessor(column)
                .and_then(|cells| cells.array_cells_owned_uncached(&chunk))
                .map_err(|error| format!("{} {column}: {error}", path.display()))
        };
        if !accumulate(start, &read(&reference)?, &read(&other)?, &mut sums)? {
            return Ok(f64::INFINITY);
        }
    }
    let (error, scale) = sums;
    Ok(if scale == 0.0 {
        if error == 0.0 { 0.0 } else { f64::INFINITY }
    } else {
        (error / scale).sqrt()
    })
}

/// Add the squared differences of the cells of rows `first..` in
/// `reference` and `other`, and the squared reference values, to `sums`:
/// false when a cell only one holds or a non-finite value makes the
/// difference infinite.
fn accumulate(
    first: usize,
    reference: &[Option<ArrayValue>],
    other: &[Option<ArrayValue>],
    sums: &mut (f64, f64),
) -> Result<bool, String> {
    for (row, (a, b)) in (first..).zip(reference.iter().zip(other)) {
        let (Some(ArrayValue::Complex32(a)), Some(ArrayValue::Complex32(b))) = (a, b) else {
            if a.is_none() && b.is_none() {
                continue;
            }
            return Ok(false);
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
                return Ok(false);
            }
            sums.0 += f64::from((a - b).norm_sqr());
            sums.1 += f64::from(a.norm_sqr());
        }
    }
    Ok(true)
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
