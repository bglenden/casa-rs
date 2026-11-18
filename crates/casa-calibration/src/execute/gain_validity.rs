// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA JonesDiag/JonesScal invert followed by setMatByOk.

use casa_types::Complex32;

/// An invalid antenna contributes identity and a flag. Its baseline partner
/// and other calibration terms still correct data and scale the weights.
pub(super) fn correcting_gain(value: Complex32, flagged: bool) -> (Complex32, bool) {
    if !flagged && value.norm() > 0. {
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
