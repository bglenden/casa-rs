// SPDX-License-Identifier: LGPL-3.0-or-later

//! Encode reduced MVC Taylor products as a normal-state pass.

use casa_imaging_deconvolution::PassImages;

use crate::ProductsError;

/// Turn normalized two-term MVC planes into a unity-normalized Float pass
/// consumed by a Taylor normal state. The input vectors move into the pass;
/// no additional image-sized buffers are allocated. The caller admits the
/// vectors, the subsequently widened normal state, and their overlap.
///
/// CASA's reduced images already have unity normal-state normalization. The
/// principal normal weight is one and the other normal-weight moments are
/// scaled relative to it; published weight moments remain unchanged. The
/// product caller binds a unity Taylor residual divisor so the distinct
/// published weight does not renormalize those residual pixels.
///
/// # Errors
///
/// Invalid shape, nonfinite planes or weights, or nonpositive principal weight.
pub fn encode_mvc_taylor_planes(
    shape: [usize; 2],
    psf: Vec<f32>,
    residual: Vec<f32>,
    sumwt: [f32; 3],
) -> Result<PassImages, ProductsError> {
    let cells = shape[0]
        .checked_mul(shape[1])
        .ok_or(ProductsError::InvalidTaylorMomentReduction)?;
    if cells == 0
        || cells.checked_mul(3) != Some(psf.len())
        || cells.checked_mul(2) != Some(residual.len())
        || sumwt.iter().any(|value| !value.is_finite())
        || sumwt[0] <= 0.0
    {
        return Err(ProductsError::InvalidTaylorMomentReduction);
    }
    if psf.iter().chain(&residual).any(|value| !value.is_finite()) {
        return Err(ProductsError::InvalidTaylorMomentReduction);
    }
    let principal = f64::from(sumwt[0]);
    let normal_weights = sumwt
        .iter()
        .map(|&weight| f64::from(weight) / principal)
        .collect();
    let published_weights = sumwt.into_iter().map(f64::from).collect();
    Ok(PassImages {
        domain: 0,
        shape,
        channels: 0..1,
        polarizations: 1,
        residual,
        psf: Some(psf),
        sum_weights: normal_weights,
        published_sum_weights: published_weights,
        weight: None,
    })
}

/// Encode CASA's entirely blank MVC Taylor moments after Float underflow.
/// This is reserved for an unrestored dirty MVC family; it does not relax
/// the positive-weight contract of ordinary Taylor reconstruction.
///
/// # Errors
///
/// The shape is invalid or any input moment is nonzero or nonfinite.
pub fn encode_mvc_blank_taylor_planes(
    shape: [usize; 2],
    psf: Vec<f32>,
    residual: Vec<f32>,
    sumwt: [f32; 3],
) -> Result<PassImages, ProductsError> {
    let cells = shape[0]
        .checked_mul(shape[1])
        .ok_or(ProductsError::InvalidTaylorMomentReduction)?;
    if cells == 0
        || cells.checked_mul(3) != Some(psf.len())
        || cells.checked_mul(2) != Some(residual.len())
        || sumwt.iter().any(|value| *value != 0.0)
        || psf.iter().chain(&residual).any(|value| *value != 0.0)
    {
        return Err(ProductsError::InvalidTaylorMomentReduction);
    }
    Ok(PassImages {
        domain: 0,
        shape,
        channels: 0..1,
        polarizations: 1,
        residual,
        psf: Some(psf),
        // The internal unity divisor is independent of the all-zero CASA
        // published weight; no inversion of the blank PSF is attempted.
        sum_weights: vec![1.0, 0.0, 0.0],
        published_sum_weights: sumwt.into_iter().map(f64::from).collect(),
        weight: None,
    })
}
