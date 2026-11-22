// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_types::{ArrayD, Complex32};
use ndarray::{IxDyn, Zip};

use crate::analysis::lel_mask::{evaluate_mask, evaluate_replacement_pixels};
use crate::expr_parser::ReplacementPixels;
use crate::{ImageError, ImageInterface, ImagePixel, MutableImageInterface};

use super::{SubimageRegion, and_opt_mask, region::SubimageSelection};

/// The pixels and validity selection for an in-place mask edit.
pub struct MaskReplacementOptions<'a> {
    pub expression: &'a str,
    pub region: Option<SubimageRegion<'a>>,
    pub mask: &'a str,
    pub update: bool,
    pub stretch: bool,
}

/// The result after writing replacement pixels.
pub enum MaskReplacementOutcome {
    Applied,
    /// An external or regional mask supplied the invalid pixels, but the
    /// image has no native pixel mask for the requested update. CASA reports
    /// this after writing the replacement values.
    MissingNativeMask,
}

/// Check an edit's region before acquiring the image write lock.
///
/// # Errors
/// Returns an error if the region cannot be resolved or is compound.
pub fn validate_mask_replacement_region<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    region: SubimageRegion<'_>,
) -> Result<(), ImageError> {
    replacement_selection(image, Some(region)).map(|_| ())
}

fn replacement_selection<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    region: Option<SubimageRegion<'_>>,
) -> Result<SubimageSelection, ImageError> {
    let selection = SubimageSelection::resolve(image, None, region, None, None)?;
    if selection.has_multiple_regions {
        return Err(ImageError::CasaSelection(
            "Exception: Only a single n-dimensional rectangular region is supported.".into(),
        ));
    }
    Ok(selection)
}

/// Replace invalid single-precision real pixels with a numerical expression.
pub fn replace_masked_real_pixels(
    image: &mut dyn MutableImageInterface<f32>,
    options: MaskReplacementOptions<'_>,
) -> Result<MaskReplacementOutcome, ImageError> {
    replace_masked(
        image,
        options,
        |expression, shape| match evaluate_replacement_pixels(expression, shape, false)? {
            ReplacementPixels::Real(values) => Ok(values.mapv(|value| value as f32)),
            ReplacementPixels::Complex(_) => unreachable!("real replacement expression"),
        },
    )
}

/// Replace invalid single-precision complex pixels with a numerical expression.
pub fn replace_masked_complex_pixels(
    image: &mut dyn MutableImageInterface<Complex32>,
    options: MaskReplacementOptions<'_>,
) -> Result<MaskReplacementOutcome, ImageError> {
    replace_masked(image, options, |expression, shape| {
        Ok(
            match evaluate_replacement_pixels(expression, shape, true)? {
                ReplacementPixels::Real(values) => {
                    values.mapv(|value| Complex32::new(value as f32, 0.0))
                }
                ReplacementPixels::Complex(values) => {
                    values.mapv(|value| Complex32::new(value.re as f32, value.im as f32))
                }
            },
        )
    })
}

fn replace_masked<T: ImagePixel>(
    image: &mut dyn MutableImageInterface<T>,
    options: MaskReplacementOptions<'_>,
    values: impl FnOnce(&str, &[usize]) -> Result<ArrayD<T>, ImageError>,
) -> Result<MaskReplacementOutcome, ImageError> {
    if options.expression.is_empty() {
        return Err(ImageError::CasaSelection(
            "You must specify an expression".into(),
        ));
    }
    let selection = replacement_selection(image, options.region)?;
    let stride = vec![1; image.ndim()];
    let mut valid = image.get_mask_slice(&selection.start, &selection.shape, &stride)?;
    selection.apply_selection_masks(&mut valid);
    if let Some(external) = evaluate_mask(
        Some(options.mask),
        image.shape(),
        image.coordinates(),
        options.stretch,
        (&selection.start, &selection.shape),
    )? {
        and_opt_mask(&mut valid, external);
    }
    let valid = valid.ok_or_else(|| {
        ImageError::CasaSelection("Exception: Selected region of image has no mask.".into())
    })?;
    if valid.iter().all(|&pixel| pixel) {
        return Err(ImageError::CasaSelection(
            "Exception: Mask for selected region has no bad pixels.".into(),
        ));
    }
    let replacements = values(options.expression, &selection.shape)?;
    let mut pixels = image.get_slice(&selection.start, &selection.shape, &stride)?;
    if replacements.ndim() == 0 {
        let replacement = replacements[IxDyn(&[])];
        Zip::from(&mut pixels)
            .and(&valid)
            .for_each(|pixel, &valid| {
                if !valid {
                    *pixel = replacement;
                }
            });
    } else {
        Zip::from(&mut pixels)
            .and(&valid)
            .and(&replacements)
            .for_each(|pixel, &valid, &replacement| {
                if !valid {
                    *pixel = replacement;
                }
            });
    }
    image.put_slice(&pixels, &selection.start)?;
    if options.update {
        let Some(name) = image.active_pixel_mask_name() else {
            return Ok(MaskReplacementOutcome::MissingNativeMask);
        };
        let all_valid = ArrayD::from_elem(IxDyn(&selection.shape), true);
        image.put_mask_slice(&name, &all_valid, &selection.start)?;
    }
    Ok(MaskReplacementOutcome::Applied)
}
