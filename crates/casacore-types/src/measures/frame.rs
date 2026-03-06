// SPDX-License-Identifier: LGPL-3.0-or-later
//! Conversion context for measure transformations.
//!
//! [`MeasFrame`] collects the auxiliary data needed by measure reference-frame
//! conversions — an epoch, an observatory position, the UT1−UTC offset,
//! and optionally an IERS EOP table for automatic dUT1/polar-motion lookup.
//!
//! It mirrors C++ `MeasFrame` but stores only the subset of fields relevant to
//! MEpoch, MPosition, MDirection, and MFrequency conversions.

use std::sync::Arc;

use casacore_measures_data::EopTable;

use super::direction::MDirection;
use super::epoch::MEpoch;
use super::position::MPosition;
use super::radial_velocity::MRadialVelocity;

/// IAU precession/nutation model selection.
///
/// Controls which IAU precession and nutation models are used for
/// direction conversions (J2000 ↔ JMEAN ↔ JTRUE ↔ APP) and
/// ITRF transformations.
///
/// The default is [`Iau1976_1980`](IauModel::Iau1976_1980), matching C++
/// casacore's `Precession::STANDARD` (IAU 1976) and `Nutation::STANDARD`
/// (IAU 1980).
///
/// # Example
///
/// ```
/// use casacore_types::measures::{MeasFrame, IauModel};
///
/// // Match C++ casacore defaults (IAU 1976/1980)
/// let frame = MeasFrame::new();
///
/// // Use modern IAU 2006/2000A models
/// let frame = MeasFrame::new().with_iau_model(IauModel::Iau2006_2000A);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IauModel {
    /// IAU 1976 precession (Lieske) + IAU 1980 nutation (Wahr).
    ///
    /// This is the default, matching C++ casacore's `Precession::STANDARD`
    /// and `Nutation::STANDARD`. Uses GMST82 + equation of equinoxes (1994)
    /// for sidereal time, and the equinox-based celestial-to-terrestrial
    /// matrix for ITRF.
    #[default]
    Iau1976_1980,

    /// IAU 2006 precession (Capitaine) + IAU 2000A nutation (Mathews).
    ///
    /// The modern standard adopted by IAU Resolution B1 (2006). Uses
    /// GST06A for sidereal time and the CIO-based celestial-to-terrestrial
    /// matrix for ITRF. More accurate than IAU 1976/1980 but produces
    /// slightly different results (~0.01 arcsec at J2000, growing with time).
    Iau2006_2000A,
}

/// Context data required by measure conversions.
///
/// Many epoch conversions (e.g. UTC ↔ UT1, GMST1 ↔ LMST) need external data
/// that is not part of the measure value itself. `MeasFrame` bundles these
/// together, following the same pattern as C++ `casa::MeasFrame`.
///
/// # EOP data
///
/// Attach an IERS EOP table via [`with_eop`](MeasFrame::with_eop) or
/// [`with_bundled_eop`](MeasFrame::with_bundled_eop) to enable automatic
/// dUT1 and polar-motion lookup. Explicit [`with_dut1`](MeasFrame::with_dut1)
/// values take priority over EOP table lookups.
///
/// # Builder pattern
///
/// ```
/// use casacore_types::measures::{MeasFrame, MEpoch, EpochRef, MjdHighPrec};
///
/// let frame = MeasFrame::new()
///     .with_bundled_eop()
///     .with_dut1(0.3);  // explicit override takes priority
/// ```
#[derive(Debug, Clone)]
pub struct MeasFrame {
    epoch: Option<MEpoch>,
    position: Option<MPosition>,
    direction: Option<MDirection>,
    radial_velocity: Option<MRadialVelocity>,
    dut1_seconds: Option<f64>,
    eop_table: Option<Arc<EopTable>>,
    iau_model: IauModel,
}

impl MeasFrame {
    /// Creates an empty frame with no context data set.
    pub fn new() -> Self {
        Self {
            epoch: None,
            position: None,
            direction: None,
            radial_velocity: None,
            dut1_seconds: None,
            eop_table: None,
            iau_model: IauModel::default(),
        }
    }

    /// Sets the reference epoch and returns `self` (builder pattern).
    pub fn with_epoch(mut self, epoch: MEpoch) -> Self {
        self.epoch = Some(epoch);
        self
    }

    /// Sets the observatory position and returns `self` (builder pattern).
    pub fn with_position(mut self, position: MPosition) -> Self {
        self.position = Some(position);
        self
    }

    /// Sets the observation direction and returns `self` (builder pattern).
    ///
    /// The direction is used by frequency conversions to compute the radial
    /// velocity component along the line of sight.
    pub fn with_direction(mut self, direction: MDirection) -> Self {
        self.direction = Some(direction);
        self
    }

    /// Sets the source radial velocity and returns `self` (builder pattern).
    ///
    /// The radial velocity is used by frequency conversions involving the REST
    /// frame to compute the Doppler shift to the source rest frame.
    pub fn with_radial_velocity(mut self, rv: MRadialVelocity) -> Self {
        self.radial_velocity = Some(rv);
        self
    }

    /// Sets an explicit UT1−UTC offset in seconds (builder pattern).
    ///
    /// This value takes priority over EOP table lookups in [`dut1_for_mjd`].
    /// It is useful when you know the exact dUT1 for your epoch, or for
    /// testing against a specific value.
    ///
    /// [`dut1_for_mjd`]: MeasFrame::dut1_for_mjd
    pub fn with_dut1(mut self, dut1_seconds: f64) -> Self {
        self.dut1_seconds = Some(dut1_seconds);
        self
    }

    /// Selects the IAU precession/nutation model (builder pattern).
    ///
    /// Default is [`IauModel::Iau1976_1980`], matching C++ casacore.
    pub fn with_iau_model(mut self, model: IauModel) -> Self {
        self.iau_model = model;
        self
    }

    /// Attaches an IERS EOP table for automatic dUT1/polar-motion lookup.
    ///
    /// The table is used by [`dut1_for_mjd`] and [`polar_motion_for_mjd`]
    /// to interpolate values at any epoch within the table range.
    ///
    /// [`dut1_for_mjd`]: MeasFrame::dut1_for_mjd
    /// [`polar_motion_for_mjd`]: MeasFrame::polar_motion_for_mjd
    pub fn with_eop(mut self, eop: Arc<EopTable>) -> Self {
        self.eop_table = Some(eop);
        self
    }

    /// Attaches the bundled IERS EOP data (convenience method).
    ///
    /// Equivalent to `self.with_eop(Arc::new(EopTable::bundled().clone()))`,
    /// but uses a shared static reference internally.
    pub fn with_bundled_eop(self) -> Self {
        // Clone the bundled table into an Arc so the frame owns it.
        // The actual parse is done only once (LazyLock in bundled.rs).
        self.with_eop(Arc::new(EopTable::bundled().clone()))
    }

    /// Returns the reference epoch, if set.
    pub fn epoch(&self) -> Option<&MEpoch> {
        self.epoch.as_ref()
    }

    /// Returns the observatory position, if set.
    pub fn position(&self) -> Option<&MPosition> {
        self.position.as_ref()
    }

    /// Returns the observation direction, if set.
    pub fn direction(&self) -> Option<&MDirection> {
        self.direction.as_ref()
    }

    /// Returns the source radial velocity, if set.
    pub fn radial_velocity(&self) -> Option<&MRadialVelocity> {
        self.radial_velocity.as_ref()
    }

    /// Returns the selected IAU precession/nutation model.
    pub fn iau_model(&self) -> IauModel {
        self.iau_model
    }

    /// Returns the explicit UT1−UTC offset in seconds, if set.
    ///
    /// Prefer [`dut1_for_mjd`] which also consults the EOP table.
    ///
    /// [`dut1_for_mjd`]: MeasFrame::dut1_for_mjd
    pub fn dut1_seconds(&self) -> Option<f64> {
        self.dut1_seconds
    }

    /// Returns the attached EOP table, if any.
    pub fn eop_table(&self) -> Option<&EopTable> {
        self.eop_table.as_deref()
    }

    /// Get dUT1 (UT1−UTC) for a given MJD.
    ///
    /// Priority:
    /// 1. Explicit [`with_dut1()`](MeasFrame::with_dut1) override
    /// 2. EOP table interpolation at the given MJD
    /// 3. `None`
    pub fn dut1_for_mjd(&self, mjd: f64) -> Option<f64> {
        // Explicit override wins
        if let Some(dut1) = self.dut1_seconds {
            return Some(dut1);
        }
        // Try EOP table
        if let Some(eop) = &self.eop_table {
            if let Some(vals) = eop.interpolate(mjd) {
                return Some(vals.dut1_seconds);
            }
        }
        None
    }

    /// Get polar motion (xp, yp) in arcseconds for a given MJD.
    ///
    /// Returns `None` if no EOP table is attached or the MJD is outside
    /// the table range.
    pub fn polar_motion_for_mjd(&self, mjd: f64) -> Option<(f64, f64)> {
        let eop = self.eop_table.as_ref()?;
        let vals = eop.interpolate(mjd)?;
        Some((vals.x_arcsec, vals.y_arcsec))
    }
}

impl Default for MeasFrame {
    fn default() -> Self {
        Self::new()
    }
}
