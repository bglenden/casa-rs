// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded CASA MVC cube-to-Taylor PSF and residual sums.

use crate::ProductsError;

/// Accumulate the two-term MVC Taylor PSF, residual and sum-weight moments.
///
/// The caller owns and admits all storage. `psf` holds three contiguous
/// x-major planes, `residual` two, and `mean_pb` one: no channel cube or
/// channel plane is retained. Add published, cutoff-applied cube PB planes,
/// normalized cube PSFs, PB-adjusted cube residuals and their published Float
/// sum weights in channel order. Finish divides by their total weight and,
/// for images, by the supplied mean PB above the strict cutoff.
///
/// An error poisons the accumulator; discard the partially written buffers.
pub struct TaylorMomentAccumulator<'a> {
    mean_pb: &'a [f32],
    psf: &'a mut [f32],
    residual: &'a mut [f32],
    sumwt: &'a mut [f32; 3],
    reference_hz: f64,
    cutoff: f32,
    weight: f32,
    failed: bool,
}

impl<'a> TaylorMomentAccumulator<'a> {
    /// Clear and borrow the caller's two-term output planes.
    ///
    /// # Errors
    ///
    /// The shape, reference frequency, cutoff or common PB is invalid, or
    /// the common PB has no positive pixel (CASA refuses a zero maximum).
    pub fn new(
        mean_pb: &'a [f32],
        psf: &'a mut [f32],
        residual: &'a mut [f32],
        sumwt: &'a mut [f32; 3],
        reference_hz: f64,
        cutoff: f32,
    ) -> Result<Self, ProductsError> {
        let cells = mean_pb.len();
        if cells == 0
            || cells.checked_mul(3) != Some(psf.len())
            || cells.checked_mul(2) != Some(residual.len())
            || !reference_hz.is_finite()
            || reference_hz <= 0.0
            || !cutoff.is_finite()
            || cutoff < 0.0
            || mean_pb
                .iter()
                .any(|value| !value.is_finite() || *value < 0.0)
            || !mean_pb.iter().any(|value| *value > 0.0)
        {
            return Err(ProductsError::InvalidTaylorMomentReduction);
        }
        psf.fill(0.0);
        residual.fill(0.0);
        sumwt.fill(0.0);
        Ok(Self {
            mean_pb,
            psf,
            residual,
            sumwt,
            reference_hz,
            cutoff,
            weight: 0.0,
            failed: false,
        })
    }

    /// Add one normalized cube channel and its published weight and frequency.
    ///
    /// # Errors
    ///
    /// Shapes, weight, frequency, pixels or published PB support are invalid,
    /// arithmetic is non-finite, or an earlier call failed.
    pub fn add(
        &mut self,
        psf: &[f32],
        residual: &[f32],
        pb: &[f32],
        weight: f32,
        frequency_hz: f64,
    ) -> Result<(), ProductsError> {
        let cells = self.mean_pb.len();
        if self.failed
            || psf.len() != cells
            || residual.len() != cells
            || pb.len() != cells
            || !weight.is_finite()
            || weight < 0.0
            || !frequency_hz.is_finite()
            || frequency_hz <= 0.0
        {
            self.failed = true;
            return Err(ProductsError::InvalidTaylorMomentReduction);
        }
        let x = (frequency_hz - self.reference_hz) / self.reference_hz;
        let factors = [1.0_f64, x, x * x].map(|factor| (factor * f64::from(weight)) as f32);
        let total = self.weight + weight;
        if !total.is_finite() || factors.iter().any(|factor| !factor.is_finite()) {
            self.failed = true;
            return Err(ProductsError::InvalidTaylorMomentReduction);
        }
        for (term, &factor) in factors.iter().enumerate() {
            self.sumwt[term] += factor * weight;
            if !self.sumwt[term].is_finite() {
                self.failed = true;
                return Err(ProductsError::InvalidTaylorMomentReduction);
            }
        }
        for pixel in 0..cells {
            let channel_pb = pb[pixel];
            if !channel_pb.is_finite()
                || channel_pb < 0.0
                || (channel_pb > 0.0 && channel_pb <= self.cutoff)
                || !psf[pixel].is_finite()
                || !residual[pixel].is_finite()
            {
                self.failed = true;
                return Err(ProductsError::InvalidTaylorMomentReduction);
            }
            let psf_pb = channel_pb * psf[pixel];
            let residual_pb = channel_pb * residual[pixel];
            for (term, &factor) in factors.iter().enumerate() {
                let psf = &mut self.psf[term * cells + pixel];
                *psf += factor * psf_pb;
                if !psf.is_finite() {
                    self.failed = true;
                    return Err(ProductsError::InvalidTaylorMomentReduction);
                }
                if term < 2 {
                    let residual = &mut self.residual[term * cells + pixel];
                    *residual += factor * residual_pb;
                    if !residual.is_finite() {
                        self.failed = true;
                        return Err(ProductsError::InvalidTaylorMomentReduction);
                    }
                }
            }
        }
        self.weight = total;
        Ok(())
    }

    /// Normalize the output moments, blanking pixels at or below the PB cutoff.
    ///
    /// # Errors
    ///
    /// No positive weight was added, a previous channel failed, or the
    /// normalization generated a non-finite value.
    pub fn finish(self) -> Result<(), ProductsError> {
        if self.failed || self.weight <= 0.0 {
            return Err(ProductsError::InvalidTaylorMomentReduction);
        }
        let cells = self.mean_pb.len();
        for (index, value) in self
            .psf
            .iter_mut()
            .chain(self.residual.iter_mut())
            .enumerate()
        {
            let beam = self.mean_pb[index % cells];
            *value = if beam > self.cutoff {
                *value / self.weight / beam
            } else {
                0.0
            };
            if !value.is_finite() {
                return Err(ProductsError::InvalidTaylorMomentReduction);
            }
        }
        for value in self.sumwt {
            *value /= self.weight;
        }
        Ok(())
    }
}
