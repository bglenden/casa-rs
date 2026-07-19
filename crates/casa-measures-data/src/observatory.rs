// SPDX-License-Identifier: LGPL-3.0-or-later
//! Observatory catalog data loaded from casacore `geodetic/Observatories`.

use std::collections::HashMap;

/// A single observatory row from casacore `geodetic/Observatories`.
#[derive(Debug, Clone, PartialEq)]
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

/// In-memory catalog of observatory metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservatoryCatalog {
    entries: Vec<ObservatoryEntry>,
    by_name: HashMap<String, usize>,
}

impl ObservatoryCatalog {
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
