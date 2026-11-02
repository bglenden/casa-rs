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

#[cfg(all(feature = "msv4", unix))]
pub mod convert;
#[cfg(unix)]
pub mod creation;
mod grouping;
mod heap;
mod msv2;
#[cfg(feature = "msv4")]
mod msv4;
mod namespace;
mod update;

use std::path::Path;

use casa_observation::{Observation, ObservationError};

pub use msv2::{Msv2Cursor, Msv2Observation};
#[cfg(feature = "msv4")]
pub use msv4::{
    Msv4Codec, Msv4Cursor, Msv4Observation, Msv4ProcessingSetWriter, Msv4StorageOptions,
    Msv4WriteCounters, Msv4Writer,
};
pub use namespace::{
    NamespaceAntenna, NamespaceField, NamespaceRows, NamespaceWindow, PersistedNamespace,
    persisted_namespace,
};
pub use update::UpdateSession;
#[cfg(unix)]
pub use update::open_update;

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

/// Whether the store at `path` is an MSv4 processing set: a Zarr group,
/// with a `zarr.json` at its root. Anything else is taken for a
/// MeasurementSet v2, a casacore table directory; this is the one rule
/// [`open`] chooses an adapter by.
#[must_use]
pub fn is_msv4_store(path: &Path) -> bool {
    path.join("zarr.json").is_file()
}

/// Whether the directory at `path` holds a MeasurementSet, as a reader
/// would take it: an MSv4 processing set (a Zarr group whose root
/// attributes say `type: processing_set`), or a MeasurementSet v2 (a
/// casacore table whose `table.info` says `Type = Measurement Set`). A
/// subtable, an image, a calibration table or a dataset inside a
/// processing set is none.
#[must_use]
pub fn is_measurement_set(path: &Path) -> bool {
    if is_msv4_store(path) {
        return std::fs::read(path.join("zarr.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .is_some_and(|root| root["attributes"]["type"] == "processing_set");
    }
    std::fs::read_to_string(path.join("table.info")).is_ok_and(|info| {
        casa_tables::TableInfo::parse(&info).table_type == crate::ms::CASACORE_MS_TABLE_TYPE
    })
}

/// Open the observation at `path`, choosing its adapter from the store
/// ([`is_msv4_store`]): a Zarr processing set is MSv4, a casacore table
/// directory is MSv2.
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
    if is_msv4_store(path) {
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
