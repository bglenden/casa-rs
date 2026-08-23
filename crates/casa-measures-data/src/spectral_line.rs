// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spectral-line catalog data loaded from casacore `ephemerides/Lines`.

use std::mem::size_of;

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
}

impl SpectralLineCatalog {
    /// Build a catalog from explicitly provided entries.
    pub fn from_entries(entries: Vec<SpectralLineEntry>) -> Self {
        Self { entries }
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
        let name = name.trim();
        self.entries
            .iter()
            .find(|entry| entry.name.trim().eq_ignore_ascii_case(name))
    }

    pub(crate) fn retained_heap_bytes(&self) -> Option<usize> {
        let mut bytes = self
            .entries
            .capacity()
            .checked_mul(size_of::<SpectralLineEntry>())?;
        for entry in &self.entries {
            bytes = bytes
                .checked_add(entry.name.capacity())?
                .checked_add(entry.frequency_type.capacity())?
                .checked_add(entry.source.capacity())?
                .checked_add(entry.comment.capacity())?;
        }
        Some(bytes)
    }
}
