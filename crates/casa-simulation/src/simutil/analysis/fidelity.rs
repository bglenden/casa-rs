// SPDX-License-Identifier: LGPL-3.0-or-later
//! `simanalyze`'s difference and fidelity images.

use std::path::{Path, PathBuf};

use casa_images::{ImageInterface, PagedImage};
use casa_lattices::{ArrayLattice, LatticeStatistics, Statistic};
use ndarray::{ArrayD, Zip};

use super::{AnalysisError, both, suffixed, write_product};

/// What [`fidelity()`] wrote and measured.
#[derive(Debug, Clone, PartialEq)]
pub struct Fidelity {
    /// `<image>.diff`: the convolved model less the image, in Jy/beam.
    pub difference: PathBuf,
    /// `<image>.fidelity`: the convolved model's magnitude over the
    /// difference's, floored; a ratio, without a unit.
    pub fidelity: PathBuf,
    /// The difference's median absolute deviation from its median, over its
    /// unmasked pixels; 0 when it has none. CASA's log calls it the rms
    /// difference.
    pub medabsdevmed: f64,
    /// The scalar fidelity: the largest magnitude of the convolved model
    /// over [`medabsdevmed`](Self::medabsdevmed) (infinite when that is 0, as
    /// numpy divides).
    pub scalar: f64,
}

/// `simanalyze`'s analysis of `image_root.image` against the sky model:
/// with `convolved` the [`convimage`](super::convimage()) output and
/// `out_flat` the image's flat, write
/// - `<image_root>.diff`, `convolved − out_flat`, labelled Jy/beam;
/// - `<image_root>.fidelity`, `|convolved| / max(|diff|, d/√2)`, where `d`
///   is the difference's median absolute deviation;
///
/// and return `d` and the scalar fidelity. Both images take the convolved
/// model's coordinates and beam, and are masked where either input is, as
/// CASA's image expressions are. The arithmetic is in single precision, as
/// those expressions compute on single-precision images.
///
/// Two departures (`docs/CASA (C++) bugs.md`):
/// - CASA writes `d/√2` into its expression with `%f`, rounding it to six
///   decimal places; casa-rs uses the value itself.
/// - CASA's fidelity carries the convolved model's unit, Jy/beam, which its
///   image expression copies; it is a ratio, and casa-rs gives it no unit.
///
/// # Errors
///
/// Inputs of different shapes, or a failure to read or write an image or to
/// compute the statistics.
pub fn fidelity(
    convolved: &Path,
    out_flat: &Path,
    image_root: &Path,
) -> Result<Fidelity, AnalysisError> {
    let model = PagedImage::<f32>::open(convolved)?;
    let image = PagedImage::<f32>::open(out_flat)?;
    if model.shape() != image.shape() {
        return Err(AnalysisError::Analysis(format!(
            "{} has shape {:?} and {} has shape {:?}; the difference needs them the same",
            convolved.display(),
            model.shape(),
            out_flat.display(),
            image.shape()
        )));
    }
    let model_pixels = model.get()?;
    let model_mask = model.default_mask()?;
    let mask = both(model_mask.clone(), image.default_mask()?);
    let mut difference = model_pixels.clone();
    difference.zip_mut_with(&image.get()?, |model, image| *model -= *image);
    drop(image);
    let coordinates = model.coordinates().clone();
    let info = model.image_info()?;

    let difference_path = suffixed(image_root, ".diff");
    write_product(
        &difference_path,
        &difference,
        coordinates.clone(),
        "Jy/beam",
        &info,
        mask.as_ref(),
    )?;
    let medabsdevmed = medabsdevmed(&difference, mask.as_ref())?;

    let floor = (medabsdevmed / 2f64.sqrt()) as f32;
    let mut fidelity = model_pixels.clone();
    Zip::from(&mut fidelity)
        .and(&difference)
        .for_each(|model, difference| *model = model.abs() / difference.abs().max(floor));
    let fidelity_path = suffixed(image_root, ".fidelity");
    write_product(
        &fidelity_path,
        &fidelity,
        coordinates,
        "",
        &info,
        mask.as_ref(),
    )?;

    Ok(Fidelity {
        difference: difference_path,
        fidelity: fidelity_path,
        medabsdevmed,
        scalar: largest_magnitude(&model_pixels, model_mask.as_ref()) / medabsdevmed,
    })
}

/// The largest magnitude of the pixels `mask` keeps (`ia.statistics` of
/// `abs(image)`), or 0 when it keeps none.
fn largest_magnitude(pixels: &ArrayD<f32>, mask: Option<&ArrayD<bool>>) -> f64 {
    let magnitude = |value: &f32| f64::from(value.abs());
    let largest = match mask {
        Some(mask) => {
            Zip::from(pixels)
                .and(mask)
                .fold(None, |largest: Option<f64>, value, valid| {
                    if *valid {
                        Some(largest.map_or(magnitude(value), |l| l.max(magnitude(value))))
                    } else {
                        largest
                    }
                })
        }
        None => pixels.iter().map(magnitude).reduce(f64::max),
    };
    largest.unwrap_or(0.0)
}

/// `ia.statistics(robust=True)['medabsdevmed']` of `pixels` where `mask`
/// keeps them, read as `simanalyze` reads it: 0 when it is not a number or
/// no pixel is kept.
fn medabsdevmed(pixels: &ArrayD<f32>, mask: Option<&ArrayD<bool>>) -> Result<f64, AnalysisError> {
    if mask.is_some_and(|mask| !mask.iter().any(|valid| *valid)) {
        return Ok(0.0);
    }
    let lattice = ArrayLattice::new(pixels.clone());
    let mut statistics = LatticeStatistics::new(&lattice);
    if let Some(mask) = mask {
        statistics.set_pixel_mask(mask.clone());
    }
    let value = statistics
        .get_statistic(Statistic::MedAbsDevMed)?
        .iter()
        .next()
        .copied()
        .unwrap_or(0.0);
    Ok(if value.is_nan() { 0.0 } else { value })
}
