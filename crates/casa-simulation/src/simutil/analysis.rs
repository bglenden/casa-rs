// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `simutil` analysis that `simanalyze` and `simalma` run on a simulated
//! image (simulation Phase 6f, #841): CASA 6.7.6's `simutil.py` and the
//! analysis section of `task_simanalyze.py`, ported as their specification
//! (ADR-0018).
//!
//! | CASA | casa-rs |
//! |---|---|
//! | `cellsize` | [`cell_size`] |
//! | `statim(plot=False)` | [`statim`] |
//! | `ismstp` | [`is_total_power`] |
//! | `convimage` | [`convimage()`] |
//! | `simanalyze`'s difference, fidelity and scalar fidelity | [`fidelity()`] |
//! | `flatimage` | [`flat_image`](super::skymodel::flat_image) |
//!
//! `simanalyze` compares the image it made with the sky model it simulated:
//! - both are flattened to one Stokes I plane (`.flat`, the channel mean of a
//!   cube);
//! - [`convimage()`] regrids the model's flat onto the image's flat and
//!   convolves it with the image's restoring beam, giving the model as the
//!   telescope would see it at full sensitivity;
//! - [`fidelity()`] subtracts the image from that and divides the convolved
//!   model by the difference, floored at the difference's median absolute
//!   deviation over √2.
//!
//! **Where casa-rs differs**, each recorded in `docs/CASA (C++) bugs.md`:
//! - `simanalyze` writes the fidelity's floor into its image expression with
//!   `%f`, six decimal places: a floor under 5e-7 becomes 0, and one of
//!   7.07e-6 becomes 7e-6. casa-rs uses the floor itself.
//! - CASA's fidelity image carries the convolved model's Jy/beam; casa-rs
//!   gives the ratio no unit.
//! - CASA computes the difference and fidelity with image expressions,
//!   whose evaluator labels any expression containing "pa" as a position
//!   angle (unit deg, Stokes axis `Pangle`), so a project whose path
//!   contains "pa" gets them so labelled. casa-rs computes them directly and
//!   keeps the Stokes axis.
//! - `statim` multiplies the statistics of a Jy/beam image without a
//!   restoring beam by the pixel area in square arcseconds, and still reports
//!   them as Jy/beam. casa-rs reports the image's own values.
//! - `ismstp` tests `uvw.all() == 0`, which is true when any UVW value is
//!   zero; casa-rs tests what was meant, that all are.
//!
//! The plotting parts of `statim` (the display range from the cumulative
//! histogram, and the figure) go with the graphics (Phase 6i).

mod convimage;
mod fidelity;

use std::path::{Path, PathBuf};

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateSystem, CoordinateType};
use casa_images::{GaussianBeam, ImageError, ImageInfo, ImageInterface, PagedImage};
use casa_lattices::LatticeError;
use casa_ms::MeasurementSet;
use casa_types::ArrayValue;
use ndarray::ArrayD;
use thiserror::Error;

pub use convimage::{ConvolvedModel, convimage};
pub use fidelity::{Fidelity, fidelity};

use super::skymodel::pixel_axes;

/// What the analysis functions refuse.
#[derive(Debug, Error)]
pub enum AnalysisError {
    /// A refusal CASA makes with its own message (`simutil.msg` at error
    /// priority, which raises), or one casa-rs makes where CASA fails
    /// without a message of its own.
    #[error("{0}")]
    Analysis(String),
    /// An image that cannot be read or written.
    #[error(transparent)]
    Image(#[from] ImageError),
    /// Statistics that cannot be computed.
    #[error(transparent)]
    Lattice(#[from] LatticeError),
    /// A MeasurementSet table that cannot be read.
    #[error("{}: {reason}", table.display())]
    Table {
        /// The table.
        table: PathBuf,
        /// The failure.
        reason: String,
    },
    /// A file-system operation that failed.
    #[error("{}: {source}", path.display())]
    Io {
        /// The path.
        path: PathBuf,
        /// The failure.
        source: std::io::Error,
    },
}

/// `simutil.cellsize`: the image's cell on the sky in radians, each
/// direction increment's magnitude scaled by the magnitude of its diagonal
/// element of the coordinate's linear transform.
///
/// # Errors
///
/// No direction coordinate, or one rotated by more than an off-diagonal
/// element of 1e-4, which CASA refuses.
pub fn cell_size(coordinates: &CoordinateSystem) -> Result<[f64; 2], AnalysisError> {
    let (index, _) = pixel_axes(coordinates, CoordinateType::Direction)
        .ok_or_else(|| AnalysisError::Analysis("the image has no direction coordinate".into()))?;
    let CoordinateModel::Direction(direction) = coordinates.coordinate(index) else {
        unreachable!("the coordinate found is a direction coordinate");
    };
    let pc = direction.pc_matrix();
    if pc[[0, 1]].abs().max(pc[[1, 0]].abs()) > 1e-4 {
        return Err(AnalysisError::Analysis(
            "Your image is rotated with respect to Lat/Lon.  I can't cope with that yet".into(),
        ));
    }
    let increment = direction.increment();
    Ok([
        increment[0].abs() * pc[[0, 0]].abs(),
        increment[1].abs() * pc[[1, 1]].abs(),
    ])
}

/// What `simutil.statim` returns: the image's extremes, the rms of its
/// lower-left corner, and its brightness unit.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageStatistics {
    /// The smallest unmasked pixel; 0 when every pixel is masked.
    pub min: f64,
    /// The largest unmasked pixel; 1 when every pixel is masked.
    pub max: f64,
    /// The rms of the unmasked pixels of the first quarter of each direction
    /// axis, through every plane; 0 when all of those are masked.
    pub rms: f64,
    /// The brightness unit.
    pub unit: String,
}

/// `simutil.statim(image, plot=False)`.
///
/// Like CASA, it first takes the image's [`cell_size`], so it refuses an
/// image whose direction axes are rotated. The values are the image's own.
/// CASA scales them by a flux-per-pixel factor, which is 1 for Jy/pixel, for
/// Jy/beam with a restoring beam and for the units it does not know. For
/// Jy/beam without a beam the factor is the pixel area in square arcseconds,
/// while CASA still reports the unit as Jy/beam; casa-rs does not scale them
/// (`docs/CASA (C++) bugs.md`). The corner is CASA's `rg.box([0, 0],
/// [nx/4, ny/4])`: pixels 0 to `nx/4` rounded half up, inclusive, on the
/// image's first two axes.
///
/// # Errors
///
/// An image that cannot be read, or that [`cell_size`] refuses.
pub fn statim(image: &Path) -> Result<ImageStatistics, AnalysisError> {
    let image = PagedImage::<f32>::open(image)?;
    cell_size(image.coordinates())?;
    let shape = image.shape().to_vec();
    let pixels = image.get()?;
    let mask = image.default_mask()?;
    let corner = [0, 1].map(|axis| {
        let top = (shape[axis] as f64 * 0.25 + 0.5).floor() as usize;
        top.min(shape[axis] - 1)
    });
    let (mut min, mut max) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut sum_squares, mut count) = (0.0, 0_usize);
    for (position, value) in pixels.indexed_iter() {
        if mask.as_ref().is_some_and(|mask| !mask[&position]) {
            continue;
        }
        let value = f64::from(*value);
        min = min.min(value);
        max = max.max(value);
        if position[0] <= corner[0] && position[1] <= corner[1] {
            sum_squares += value * value;
            count += 1;
        }
    }
    let any = min <= max;
    Ok(ImageStatistics {
        min: if any { min } else { 0.0 },
        max: if any { max } else { 1.0 },
        rms: if count == 0 {
            0.0
        } else {
            (sum_squares / count as f64).sqrt()
        },
        unit: image.units().to_string(),
    })
}

/// `simutil.ismstp`: the MeasurementSet is total power when its first
/// antenna is named `…TP…`, when it has one antenna, or when every UVW
/// value is zero. CASA tests `uvw.all() == 0`, true when any value is zero;
/// casa-rs tests what was meant.
///
/// # Errors
///
/// A MeasurementSet whose ANTENNA table or UVW column cannot be read.
pub fn is_total_power(ms: &Path) -> Result<bool, AnalysisError> {
    let failed = |reason: &dyn std::fmt::Display| AnalysisError::Table {
        table: ms.to_path_buf(),
        reason: reason.to_string(),
    };
    let set = MeasurementSet::open(ms).map_err(|error| failed(&error))?;
    let antennas = set.antenna().map_err(|error| failed(&error))?;
    let names = (0..antennas.row_count())
        .map(|row| antennas.name(row))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| failed(&error))?;
    if names.first().is_some_and(|name| name.contains("TP")) || names.len() == 1 {
        return Ok(true);
    }
    let main = set.main_table();
    let column = main
        .column_accessor("UVW")
        .map_err(|error| failed(&error))?;
    let rows: Vec<usize> = (0..main.row_count()).collect();
    for chunk in rows.chunks(1 << 16) {
        for cell in column
            .array_cells_owned(chunk)
            .map_err(|error| failed(&error))?
        {
            if let Some(ArrayValue::Float64(uvw)) = cell
                && uvw.iter().any(|&value| value != 0.)
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// The one restoring beam of an image's information, which `convimage`
/// reads as `ia.restoringbeam()`.
fn single_beam(info: &ImageInfo, image: &Path) -> Result<GaussianBeam, AnalysisError> {
    info.beam_set
        .single_beam()
        .filter(|beam| !beam.is_null())
        .ok_or_else(|| {
            AnalysisError::Analysis(format!(
                "{}: convimage needs an image with one restoring beam",
                image.display()
            ))
        })
}

/// The element-wise AND of two optional masks: what an image expression of
/// two images keeps.
fn both(a: Option<ArrayD<bool>>, b: Option<ArrayD<bool>>) -> Option<ArrayD<bool>> {
    match (a, b) {
        (Some(mut a), Some(b)) => {
            a.zip_mut_with(&b, |a, b| *a = *a && *b);
            Some(a)
        }
        (a, b) => a.or(b),
    }
}

/// Write `pixels` as a new single-precision image at `path`, replacing what
/// is there, with `info` and, when given, `mask` as the default `mask0`.
fn write_product(
    path: &Path,
    pixels: &ArrayD<f32>,
    coordinates: CoordinateSystem,
    units: &str,
    info: &ImageInfo,
    mask: Option<&ArrayD<bool>>,
) -> Result<PagedImage<f32>, AnalysisError> {
    let removed = if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else if path.exists() {
        std::fs::remove_file(path)
    } else {
        Ok(())
    };
    removed.map_err(|source| AnalysisError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let shape = pixels.shape().to_vec();
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path)?;
    image.put_slice(pixels, &vec![0; shape.len()])?;
    image.set_units(units)?;
    image.set_image_info(info)?;
    if let Some(mask) = mask {
        image.put_mask("mask0", mask)?;
        image.set_default_mask("mask0")?;
    }
    image.save()?;
    Ok(image)
}

/// The path `<image><suffix>`, as CASA names its products.
fn suffixed(image: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", image.display()))
}
