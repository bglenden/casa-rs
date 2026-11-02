// SPDX-License-Identifier: LGPL-3.0-or-later
//! `simutil.convimage`: the sky model as the image sees it.

use std::path::{Path, PathBuf};

use casa_images::{
    GaussianBeam, ImageBeamSet, PagedImage, RegridMethod, RegridTemplate, convolve2d, regrid,
};

use super::{AnalysisError, cell_size, single_beam, suffixed, write_product};

/// What [`convimage()`] wrote.
#[derive(Debug, Clone, PartialEq)]
pub struct ConvolvedModel {
    /// `<model>.regrid`: the model on the image's grid, in Jy/pixel.
    pub regrid: PathBuf,
    /// `<model>.regrid.conv`: that convolved with the image's beam, in
    /// Jy/beam.
    pub convolved: PathBuf,
    /// The image's restoring beam, which both carry.
    pub beam: GaussianBeam,
}

/// `simutil.convimage(modelflat, outflat)`: regrid the flat sky model onto
/// the flat image's coordinates and shape, and convolve it with the image's
/// restoring beam.
///
/// As CASA does:
/// - the model's flat image is given the image's restoring beam, in place;
/// - `<model>.regrid` is the model regridded linearly (`ia.regrid`'s
///   default), scaled by the pixel area as casacore scales an image in
///   Jy/pixel, and labelled Jy/pixel;
/// - `<model>.regrid.conv` is that convolved with the beam
///   (`ia.convolve2d`), labelled Jy/beam with the image's beam, and masked
///   where the regrid is.
///
/// CASA also computes a second pixel-area factor and multiplies the regrid
/// by it, but takes both of its cells from the image, so the factor is 1.
/// casa-rs leaves it out.
///
/// # Errors
///
/// An image without one restoring beam, or whose direction coordinate is
/// rotated (CASA's refusal); a model the regrid refuses; a failure to read
/// or write an image.
pub fn convimage(model_flat: &Path, out_flat: &Path) -> Result<ConvolvedModel, AnalysisError> {
    let out = PagedImage::<f32>::open(out_flat)?;
    let template = RegridTemplate::of(&out);
    let beam = single_beam(&out.image_info()?, out_flat)?;
    cell_size(&template.coordinates)?;
    drop(out);

    let mut model = PagedImage::<f32>::open(model_flat)?;
    let mut info = model.image_info()?;
    info.beam_set = ImageBeamSet::new(beam);
    model.set_image_info(&info)?;
    model.save()?;
    let regridded = regrid(&model, &template, RegridMethod::Linear)?;
    drop(model);

    let regrid_path = suffixed(model_flat, ".regrid");
    let masked = regridded.mask.iter().any(|valid| !valid);
    let regrid_image = write_product(
        &regrid_path,
        &regridded.data,
        regridded.coordinates.clone(),
        "Jy/pixel",
        &info,
        masked.then_some(&regridded.mask),
    )?;
    let convolved = convolve2d(&regrid_image, &beam)?;
    let convolved_path = suffixed(model_flat, ".regrid.conv");
    write_product(
        &convolved_path,
        &convolved.data,
        regrid_image.coordinates().clone(),
        "Jy/beam",
        &info,
        convolved.mask.as_ref(),
    )?;
    Ok(ConvolvedModel {
        regrid: regrid_path,
        convolved: convolved_path,
        beam,
    })
}
