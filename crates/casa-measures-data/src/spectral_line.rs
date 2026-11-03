// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spectral-line catalog data loaded from casacore `ephemerides/Lines`.

use std::mem::size_of;

use casa_types::quanta::unique_prefix_position;

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

/// casacore's `QC::HI()`, 1420.405751786 MHz, in Hz.
const HI_HZ: f64 = 1_420_405_751.786;

impl SpectralLineEntry {
    /// The rest frequency in Hz, as casacore's `MeasTable::Line` gives it:
    /// the `Freq` column, except for the entry named `HI`, which casacore
    /// takes from its constant `QC::HI()` whatever the table holds (the
    /// measures data's own `HI` row agrees with it).
    pub fn frequency_hz(&self) -> f64 {
        if self.name == "HI" {
            HI_HZ
        } else {
            self.frequency_ghz * 1.0e9
        }
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

    /// The spectral line `name` finds, as casacore's `MeasTable` finds it: the
    /// entry of that name ignoring case, else the one entry the name agrees
    /// with over the shorter of their lengths ([`unique_prefix_position`]).
    pub fn get(&self, name: &str) -> Option<&SpectralLineEntry> {
        unique_prefix_position(name, &self.entries, |entry| entry.name.as_str())
            .map(|at| &self.entries[at])
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
