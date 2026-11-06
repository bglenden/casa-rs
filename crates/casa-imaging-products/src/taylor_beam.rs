// SPDX-License-Identifier: LGPL-3.0-or-later

//! A bounded sumweight-weighted mean of published cube PB planes.

use crate::ProductsError;

/// Accumulate CASA MVC's Taylor-zero primary beam in one borrowed Float plane.
///
/// Add each published cube PB plane and its published sum of weights in
/// channel order, then finish to divide by their sum. The caller owns and
/// admits the output buffer and each channel plane; this accumulator holds
/// no heap allocation and does not retain a channel plane. The input planes
/// are the cube's PB products, including their own cutoff-applied zeros.
///
/// An error poisons the accumulator: discard its partially written buffer.
/// This prevents an incomplete reduction from being mistaken for a PB.
pub struct TaylorPrimaryBeamAccumulator<'a> {
    pixels: &'a mut [f32],
    weight: f32,
    failed: bool,
}

impl<'a> TaylorPrimaryBeamAccumulator<'a> {
    /// Clear and borrow the caller's output plane.
    ///
    /// # Errors
    ///
    /// The plane is empty.
    pub fn new(pixels: &'a mut [f32]) -> Result<Self, ProductsError> {
        if pixels.is_empty() {
            return Err(ProductsError::InvalidTaylorBeamReduction);
        }
        pixels.fill(0.0);
        Ok(Self {
            pixels,
            weight: 0.0,
            failed: false,
        })
    }

    /// Add one cutoff-applied cube PB plane and its non-negative sum weight.
    ///
    /// # Errors
    ///
    /// The plane length differs, the weight or a value is negative or
    /// non-finite, arithmetic overflows, or an earlier call failed. After an
    /// error the partially written output must be discarded.
    pub fn add(&mut self, plane: &[f32], weight: f32) -> Result<(), ProductsError> {
        if self.failed || plane.len() != self.pixels.len() || weight < 0.0 {
            self.failed = true;
            return Err(ProductsError::InvalidTaylorBeamReduction);
        }
        let total = self.weight + weight;
        if !total.is_finite() {
            self.failed = true;
            return Err(ProductsError::InvalidTaylorBeamReduction);
        }
        for (sum, &value) in self.pixels.iter_mut().zip(plane) {
            if value < 0.0 {
                self.failed = true;
                return Err(ProductsError::InvalidTaylorBeamReduction);
            }
            *sum += weight * value;
            if !sum.is_finite() {
                self.failed = true;
                return Err(ProductsError::InvalidTaylorBeamReduction);
            }
        }
        self.weight = total;
        Ok(())
    }

    /// Divide the accumulated plane by the total sum weight.
    ///
    /// # Errors
    ///
    /// No positive weight was added or a prior channel failed.
    pub fn finish(self) -> Result<&'a [f32], ProductsError> {
        if self.failed || self.weight <= 0.0 {
            return Err(ProductsError::InvalidTaylorBeamReduction);
        }
        for pixel in self.pixels.iter_mut() {
            *pixel /= self.weight;
        }
        Ok(self.pixels)
    }
}
