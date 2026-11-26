// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA JonesDiag/JonesScal invert followed by setMatByOk.

use casa_core::Complex32;

/// An invalid antenna contributes identity and a flag. Its baseline partner
/// and other calibration terms still correct data and scale the weights.
pub(super) fn correcting_gain(value: Complex32, flagged: bool) -> (Complex32, bool) {
    let nonzero = !flagged
        && if value.re.is_finite() && value.im.is_finite() {
            value.re != 0. || value.im != 0.
        } else {
            value.norm() > 0.
        };
    if nonzero {
        (value, false)
    } else {
        (Complex32::new(1., 0.), true)
    }
}

/// A defined zero is permitted when multiplying a sky model by a Jones term.
pub(super) fn forward_gain(value: Complex32, flagged: bool) -> (Complex32, bool) {
    if flagged {
        (Complex32::new(1., 0.), true)
    } else {
        (value, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correcting_gain_preserves_norm_based_validity() {
        let components = [
            0.,
            -0.,
            f32::from_bits(1),
            -f32::from_bits(1),
            f32::MIN_POSITIVE,
            -1.,
            1.,
            f32::MAX,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::NAN,
        ];
        for re in components {
            for im in components {
                let value = Complex32::new(re, im);
                for flagged in [false, true] {
                    let valid = !flagged && value.norm() > 0.;
                    let expected = if valid { value } else { Complex32::new(1., 0.) };
                    let (corrected, flagged) = correcting_gain(value, flagged);
                    assert_eq!(flagged, !valid, "{value:?}");
                    assert_eq!(corrected.re.to_bits(), expected.re.to_bits());
                    assert_eq!(corrected.im.to_bits(), expected.im.to_bits());
                }
            }
        }
    }
}
