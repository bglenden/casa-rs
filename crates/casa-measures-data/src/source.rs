// SPDX-License-Identifier: LGPL-3.0-or-later
//! Source catalog data loaded from casacore `ephemerides/Sources`.

use std::collections::HashMap;

/// A single source row from casacore `ephemerides/Sources`.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceEntry {
    /// Table `MJD` column.
    pub mjd: f64,
    /// Table `Name` column.
    pub name: String,
    /// Table `Type` column.
    pub direction_type: String,
    /// Table `Long` column, in degrees.
    pub longitude_deg: f64,
    /// Table `Lat` column, in degrees.
    pub latitude_deg: f64,
    /// Table `Source` column.
    pub source: String,
    /// Table `Comment` column.
    pub comment: String,
}

impl SourceEntry {
    /// Return the longitude in radians.
    pub fn longitude_rad(&self) -> f64 {
        self.longitude_deg.to_radians()
    }

    /// Return the latitude in radians.
    pub fn latitude_rad(&self) -> f64 {
        self.latitude_deg.to_radians()
    }
}

/// In-memory catalog of named-source metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceCatalog {
    entries: Vec<SourceEntry>,
    by_name: HashMap<String, usize>,
}

impl SourceCatalog {
    /// Build a catalog from explicitly provided entries.
    pub fn from_entries(entries: Vec<SourceEntry>) -> Self {
        let by_name = entries
            .iter()
            .enumerate()
            .fold(HashMap::new(), |mut map, (index, entry)| {
                map.entry(normalize_name(&entry.name)).or_insert(index);
                map
            });
        Self { entries, by_name }
    }

    /// Return the packaged/runtime standard catalog.
    pub fn bundled() -> &'static Self {
        crate::load_sources().0
    }

    /// Iterate over the catalog entries in source order.
    pub fn iter(&self) -> impl Iterator<Item = &SourceEntry> {
        self.entries.iter()
    }

    /// Borrow all entries.
    pub fn entries(&self) -> &[SourceEntry] {
        &self.entries
    }

    /// Look up a source by case-insensitive name.
    pub fn get(&self, name: &str) -> Option<&SourceEntry> {
        self.by_name
            .get(&normalize_name(name))
            .and_then(|index| self.entries.get(*index))
    }
}

fn normalize_name(name: &str) -> String {
    name.trim().to_ascii_uppercase()
}
