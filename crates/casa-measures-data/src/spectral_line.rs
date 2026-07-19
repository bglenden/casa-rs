// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spectral-line catalog data loaded from casacore `ephemerides/Lines`.

use std::collections::HashMap;

/// A single spectral-line row from casacore `ephemerides/Lines`.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralLineEntry {
    /// Table `MJD` column.
    pub mjd: f64,
    /// Table `Name` column.
    pub name: String,
    /// Table `Type` column.
    pub frequency_type: String,
    /// Table `Freq` column, in GHz.
    pub frequency_ghz: f64,
    /// Table `Source` column.
    pub source: String,
    /// Table `Comment` column.
    pub comment: String,
}

impl SpectralLineEntry {
    /// Return the rest frequency in Hz.
    pub fn frequency_hz(&self) -> f64 {
        self.frequency_ghz * 1.0e9
    }
}

/// In-memory catalog of named spectral-line metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralLineCatalog {
    entries: Vec<SpectralLineEntry>,
    by_name: HashMap<String, usize>,
}

impl SpectralLineCatalog {
    /// Build a catalog from explicitly provided entries.
    pub fn from_entries(entries: Vec<SpectralLineEntry>) -> Self {
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
    pub fn iter(&self) -> impl Iterator<Item = &SpectralLineEntry> {
        self.entries.iter()
    }

    /// Borrow all entries.
    pub fn entries(&self) -> &[SpectralLineEntry] {
        &self.entries
    }

    /// Look up a spectral line by case-insensitive name.
    pub fn get(&self, name: &str) -> Option<&SpectralLineEntry> {
        self.by_name
            .get(&normalize_name(name))
            .and_then(|index| self.entries.get(*index))
    }
}

fn normalize_name(name: &str) -> String {
    name.trim().to_ascii_uppercase()
}
