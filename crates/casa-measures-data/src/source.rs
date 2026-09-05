// SPDX-License-Identifier: LGPL-3.0-or-later
//! Source catalog data loaded from casacore `ephemerides/Sources`.

use std::mem::size_of;

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
}

impl SourceCatalog {
    /// Build a catalog from explicitly provided entries.
    pub fn from_entries(entries: Vec<SourceEntry>) -> Self {
        Self { entries }
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
        let name = name.trim();
        self.entries
            .iter()
            .find(|entry| entry.name.trim().eq_ignore_ascii_case(name))
    }

    pub(crate) fn retained_heap_bytes(&self) -> Option<usize> {
        let mut bytes = self
            .entries
            .capacity()
            .checked_mul(size_of::<SourceEntry>())?;
        for entry in &self.entries {
            bytes = bytes
                .checked_add(entry.name.capacity())?
                .checked_add(entry.direction_type.capacity())?
                .checked_add(entry.source.capacity())?
                .checked_add(entry.comment.capacity())?;
        }
        Some(bytes)
    }
}
