// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fields and sources: `sm.setfield`.
//!
//! `NewMSSimulator::initFields` appends a FIELD row and a SOURCE row per
//! call. The SOURCE row takes the reference time in force at the call and,
//! if a spectral window exists by then, the first window's line information.

/// One FIELD row, with its SOURCE row's values.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Field {
    pub(crate) name: String,
    /// CODE (`calcode`).
    pub(crate) code: String,
    /// The J2000 direction, radians: DELAY_DIR, PHASE_DIR and REFERENCE_DIR.
    pub(crate) direction_rad: [f64; 2],
    /// `distance`, metres; zero for a source at infinity. Used by `predict`.
    pub(crate) distance_m: f64,
    /// The SOURCE row's TIME: the reference time when `setfield` was
    /// called, UTC seconds.
    pub(crate) source_time_s: f64,
    /// The SOURCE row's single line, at the first spectral window's
    /// REF_FREQUENCY, if one existed when `setfield` was called.
    pub(crate) rest_frequency_hz: Option<f64>,
}
