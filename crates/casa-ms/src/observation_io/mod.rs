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
//! trial's examples need (`docs/measurement-set/trial/README.md`), at the
//! trial's second revision (`revision-2-design.md`): the MSv2 adapter grids
//! rows onto time × baseline at opening and partitions them as XRADIO's
//! converter does, so both adapters deliver the same batches. The
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

/// How the MSv2 adapter divides MAIN rows into partitions, beyond the keys
/// every partition has: data description, intents (`STATE::OBS_MODE`),
/// observation, ephemeris and feed pair (XRADIO's mandatory
/// `partition_scheme`, plus the feed pair MSv4 has no axis for). An MSv4
/// store is already partitioned and ignores this.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PartitionScheme {
    /// Also split by `FIELD_ID`, as XRADIO's `partition_scheme=["FIELD_ID"]`.
    pub fields: bool,
    /// Also split by `SCAN_NUMBER`.
    pub scans: bool,
}

/// Options fixed when a store is opened, outside any algorithm.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OpenOptions {
    /// How group completion is announced; see [`GroupClosure`].
    pub group_closure: GroupClosure,
    /// How an MSv2 is partitioned; see [`PartitionScheme`].
    pub partition_scheme: PartitionScheme,
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

/// The distinct `values`, sorted: a partition's fields or scans from its
/// per-time identities.
fn distinct<T: Ord + Copy>(values: &[T]) -> Vec<T> {
    let mut distinct = values.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    distinct
}
