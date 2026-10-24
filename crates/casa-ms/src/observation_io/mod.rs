// SPDX-License-Identifier: LGPL-3.0-or-later
//! Storage adapters behind the shared observation contracts
//! (`casa-observation`): MeasurementSet v2 here, MSv4 with the `msv4`
//! feature.
//!
//! [`open`] is the one place a store's format is chosen: it returns the
//! shared [`Observation`] interface, behind which each adapter dispatches
//! once per bind and once per batch. Algorithm code takes that interface
//! and imports neither concrete adapter.
//!
//! **Trial status.** These adapters implement the cases the MSv2/MSv4
//! trial's examples need (`docs/measurement-set/trial/README.md`); the
//! production readers and writers are unchanged.

mod grouping;
mod msv2;
#[cfg(feature = "msv4")]
mod msv4;

use std::path::Path;

use casa_observation::{Observation, ObservationError};

pub use msv2::{Msv2Cursor, Msv2Observation};
#[cfg(feature = "msv4")]
pub use msv4::{
    Msv4Codec, Msv4Cursor, Msv4Observation, Msv4StorageOptions, Msv4WriteCounters, Msv4Writer,
};

/// When a cursor announces that a scientific group's input is complete.
///
/// A source chooses; consumers handle both. The choice is an opening
/// option only so that tests can exercise the deferred path that a source
/// without lookahead (interleaved partitions, streaming input) must take.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum GroupClosure {
    /// Mark the closing fragment `Last` or `Whole` whenever the traversal
    /// can already prove it at delivery.
    #[default]
    AtDelivery,
    /// Never mark fragments closing; send `ReadEvent::GroupEnd` once the
    /// traversal has read past the group. A test mechanism.
    Deferred,
}

/// Options fixed when a store is opened, outside any algorithm.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OpenOptions {
    /// How group completion is announced; see [`GroupClosure`].
    pub group_closure: GroupClosure,
}

/// Open the observation at `path`, choosing its adapter from the store:
/// a Zarr processing set (a `zarr.json` at its root) is MSv4, a casacore
/// table directory is MSv2.
///
/// # Errors
///
/// When the store cannot be opened by its adapter, or is MSv4 and the
/// `msv4` feature is off.
#[cfg(unix)]
pub fn open(
    path: impl AsRef<Path>,
    options: &OpenOptions,
) -> Result<Box<dyn Observation>, ObservationError> {
    let path = path.as_ref();
    if path.join("zarr.json").is_file() {
        #[cfg(feature = "msv4")]
        return Ok(Box::new(Msv4Observation::open_with(path, options)?));
        #[cfg(not(feature = "msv4"))]
        return Err(ObservationError::Unsupported {
            what: "an MSv4 store without casa-ms's msv4 feature",
        });
    }
    Ok(Box::new(Msv2Observation::open_with(path, options)?))
}
