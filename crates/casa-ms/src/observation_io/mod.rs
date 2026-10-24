// SPDX-License-Identifier: LGPL-3.0-or-later
//! Storage adapters behind the shared observation contracts
//! (`casa-observation`): MeasurementSet v2 here, MSv4 with the `msv4`
//! feature.
//!
//! **Trial status.** These adapters implement the cases the MSv2/MSv4
//! trial's examples need (`docs/measurement-set/trial/README.md`); the
//! production readers and writers are unchanged.

mod grouping;
mod msv2;
#[cfg(feature = "msv4")]
mod msv4;

pub use msv2::{Msv2Cursor, Msv2Observation};
#[cfg(feature = "msv4")]
pub use msv4::{
    Msv4Codec, Msv4Cursor, Msv4DatasetSpec, Msv4Observation, Msv4WriteCounters, Msv4Writer,
};
