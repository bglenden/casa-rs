// SPDX-License-Identifier: LGPL-3.0-or-later
//! Astronomical measures: typed physical quantities with reference frames.
//!
//! This module provides the casacore *measures* system — physical quantities
//! (epochs, positions, directions, frequencies, Doppler shifts) that carry
//! both a numeric value and a reference frame. Reference-frame conversions
//! use the IAU SOFA algorithms via the [`sofars`] crate.
//!
//! ## Key types
//!
//! - [`MEpoch`] — a time instant in one of 12 time scales (UTC, TAI, TT, etc.),
//!   equivalent to C++ `casa::MEpoch`.
//! - [`MPosition`] — a 3D position in ITRF or WGS84, equivalent to C++
//!   `casa::MPosition`.
//! - [`MDirection`](direction::MDirection) — a sky direction in J2000, GALACTIC,
//!   etc., equivalent to C++ `casa::MDirection`.
//! - [`MFrequency`](frequency::MFrequency) — a spectral frequency in LSRK,
//!   BARY, TOPO, etc., equivalent to C++ `casa::MFrequency`.
//! - [`MDoppler`](doppler::MDoppler) — a Doppler shift in RADIO, Z, BETA, etc.,
//!   equivalent to C++ `casa::MDoppler`.
//! - [`MeasFrame`] — conversion context carrying epoch, position, direction,
//!   and UT1−UTC offset.
//! - [`MjdHighPrec`] — high-precision Modified Julian Date (day + fraction),
//!   equivalent to C++ `MVEpoch`.
//!
//! ## Record serialization
//!
//! The [`record`] submodule provides `to_record` / `from_record` functions
//! that produce [`RecordValue`](crate::RecordValue) in the same layout as C++
//! `MeasureHolder`, ensuring on-disk compatibility.
//!
//! ## Examples
//!
//! ```
//! use casacore_types::measures::{MEpoch, EpochRef, MeasFrame};
//!
//! // Convert J2000.0 from UTC to TAI
//! let utc = MEpoch::from_mjd(51544.5, EpochRef::UTC);
//! let frame = MeasFrame::new();
//! let tai = utc.convert_to(EpochRef::TAI, &frame).unwrap();
//! let diff_s = (tai.value().as_mjd() - utc.value().as_mjd()) * 86400.0;
//! assert!((diff_s - 32.0).abs() < 0.01);
//! ```

pub mod direction;
pub mod doppler;
pub mod epoch;
pub mod error;
pub mod frame;
pub mod frequency;
pub mod position;
pub mod radial_velocity;
pub mod record;

// Re-exports for convenience.
pub use epoch::{EpochRef, MEpoch, MjdHighPrec};
pub use error::MeasureError;
pub use frame::{IauModel, MeasFrame};
pub use position::{MPosition, PositionRef};
pub use record::{
    direction_from_record, direction_to_record, doppler_from_record, doppler_to_record,
    epoch_from_record, epoch_to_record, frequency_from_record, frequency_to_record,
    position_from_record, position_to_record, radial_velocity_from_record,
    radial_velocity_to_record,
};
