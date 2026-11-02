// SPDX-License-Identifier: LGPL-3.0-or-later
//! A normalized two-float sum for accurate CPU f32 gridding (#991).

use crate::accumulator::GridScalar;
use num_complex::{Complex, Complex32};

/// `high + low` equals the unevaluated sum, absent overflow.
#[inline]
fn two_sum(a: f32, b: f32) -> (f32, f32) {
    let high = a + b;
    let virtual_b = high - a;
    (high, (a - (high - virtual_b)) + (b - virtual_b))
}

#[inline]
fn component(high: f32, low: &mut f32, value: f32) -> f32 {
    let (sum, error) = two_sum(high, value);
    // Preserve IEEE propagation; an infinity must not poison a later
    // finite addition through inf - inf in the rounding-loss calculation.
    if !sum.is_finite() {
        *low = 0.0;
        return sum;
    }
    let (high, remainder) = two_sum(sum, *low + error);
    *low = remainder;
    high
}

/// Const dispatch removes the correction access and all extra arithmetic
/// from f64 loops. The f64 multiplication order is intact.
#[inline]
pub(crate) fn add<T: GridScalar, const COMPENSATED: bool>(
    cell: &mut Complex<T>,
    corrections: &mut [Complex32],
    index: usize,
    value: Complex<T>,
) {
    if COMPENSATED {
        let low = &mut corrections[index];
        cell.re = T::from_f32(component(
            cell.re.into_f32(),
            &mut low.re,
            value.re.into_f32(),
        ));
        cell.im = T::from_f32(component(
            cell.im.into_f32(),
            &mut low.im,
            value.im.into_f32(),
        ));
    } else {
        *cell = *cell + value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_retains_small_terms_and_nonfinite_values_propagate() {
        let mut cell = Complex32::default();
        let mut low = [Complex32::default()];
        for value in [1.0e8, 1.0, -1.0e8] {
            add::<f32, true>(&mut cell, &mut low, 0, Complex32::new(value, -value));
        }
        assert_eq!(cell, Complex32::new(1.0, -1.0));
        for value in [f32::INFINITY, 2.0] {
            add::<f32, true>(&mut cell, &mut low, 0, Complex32::new(value, 0.0));
        }
        assert_eq!(cell.re, f32::INFINITY);
        add::<f32, true>(
            &mut cell,
            &mut low,
            0,
            Complex32::new(f32::NEG_INFINITY, 0.0),
        );
        assert!(cell.re.is_nan());
    }
}
