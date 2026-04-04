// SPDX-License-Identifier: LGPL-3.0-or-later
//! Observatory catalog data derived from casacore `geodetic/Observatories`.
//!
//! The checked-in bundled snapshot is a Rust-native JSON export generated from
//! the installed casacore-data table via:
//!
//! ```text
//! cargo run -p casa-measures-tools --bin import_observatories -- \
//!   --input /path/to/geodetic/Observatories \
//!   --output crates/casa-measures-data/data/observatories.json
//! ```
//!
//! Runtime code reads only the Rust-native snapshot, never the raw casacore
//! table files.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::bundled_observatory_catalog;

/// A single observatory row from casacore `geodetic/Observatories`.
///
/// All columns from the source table are preserved in the bundled snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservatoryEntry {
    /// Table `MJD` column.
    pub mjd: f64,
    /// Table `Name` column.
    pub name: String,
    /// Table `Type` column.
    pub observatory_type: String,
    /// Table `Long` column, in degrees.
    pub longitude_deg: f64,
    /// Table `Lat` column, in degrees.
    pub latitude_deg: f64,
    /// Table `Height` column, in metres.
    pub height_m: f64,
    /// Table `X` column, in metres.
    pub x_m: f64,
    /// Table `Y` column, in metres.
    pub y_m: f64,
    /// Table `Z` column, in metres.
    pub z_m: f64,
    /// Table `Source` column.
    pub source: String,
    /// Table `Comment` column.
    pub comment: String,
    /// Table `AntennaResponses` column.
    pub antenna_responses: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ObservatorySnapshot {
    schema_version: u32,
    entries: Vec<ObservatoryEntry>,
}

/// In-memory catalog of observatory metadata.
///
/// The bundled source of truth is
/// `crates/casa-measures-data/data/observatories.json`, generated from the
/// installed casacore-data `geodetic/Observatories` table by the internal
/// importer tool.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservatoryCatalog {
    entries: Vec<ObservatoryEntry>,
    by_name: HashMap<String, usize>,
}

impl ObservatoryCatalog {
    /// Snapshot schema version.
    pub const SCHEMA_VERSION: u32 = 1;

    /// Build a catalog from explicitly provided entries.
    pub fn from_entries(entries: Vec<ObservatoryEntry>) -> Self {
        let by_name = entries
            .iter()
            .enumerate()
            .fold(HashMap::new(), |mut map, (index, entry)| {
                map.entry(normalize_name(&entry.name)).or_insert(index);
                map
            });
        Self { entries, by_name }
    }

    /// Parse a catalog from a JSON snapshot string.
    pub fn from_json_str(content: &str) -> Result<Self, String> {
        let snapshot: ObservatorySnapshot =
            serde_json::from_str(content).map_err(|error| error.to_string())?;
        if snapshot.schema_version != Self::SCHEMA_VERSION {
            return Err(format!(
                "unsupported observatory snapshot schema version {}; expected {}",
                snapshot.schema_version,
                Self::SCHEMA_VERSION
            ));
        }
        Ok(Self::from_entries(snapshot.entries))
    }

    /// Load a catalog from a JSON snapshot file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, String> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|error| format!("{}: {error}", path.as_ref().display()))?;
        Self::from_json_str(&content)
    }

    /// Serialize the catalog into a stable pretty-printed JSON snapshot.
    pub fn to_json_pretty(&self) -> Result<String, String> {
        serde_json::to_string_pretty(&ObservatorySnapshot {
            schema_version: Self::SCHEMA_VERSION,
            entries: self.entries.clone(),
        })
        .map_err(|error| error.to_string())
    }

    /// Return the bundled catalog.
    pub fn bundled() -> &'static Self {
        bundled_observatory_catalog()
    }

    /// Iterate over the catalog entries in source order.
    pub fn iter(&self) -> impl Iterator<Item = &ObservatoryEntry> {
        self.entries.iter()
    }

    /// Borrow all entries.
    pub fn entries(&self) -> &[ObservatoryEntry] {
        &self.entries
    }

    /// Look up an observatory by case-insensitive name.
    pub fn get(&self, name: &str) -> Option<&ObservatoryEntry> {
        self.by_name
            .get(&normalize_name(name))
            .and_then(|index| self.entries.get(*index))
    }
}

fn normalize_name(name: &str) -> String {
    name.trim().to_ascii_uppercase()
}
