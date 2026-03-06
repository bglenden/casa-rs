// SPDX-License-Identifier: LGPL-3.0-or-later
//! Enumeration of all MS subtables.
//!
//! The MeasurementSet standard defines 12 required subtables and 5 optional
//! subtables. Each subtable is stored as a keyword in the main table pointing
//! to a subdirectory.
//!
//! Cf. C++ `MSMainEnums::PredefinedKeywords` and `MeasurementSet::createTable`.

use std::fmt;

/// Identifies a MeasurementSet subtable.
///
/// Each variant corresponds to a subtable directory stored as a table keyword
/// in the MS main table. The 12 required subtables must always be present;
/// the 5 optional subtables may be absent.
///
/// The keyword name used in the main table matches the variant name in
/// SCREAMING_SNAKE_CASE (e.g. `SubtableId::Antenna` -> keyword `"ANTENNA"`).
///
/// Cf. C++ `MSMainEnums::PredefinedKeywords`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubtableId {
    // -- Required (12) --
    /// Antenna characteristics. Cf. C++ `MSAntenna`.
    Antenna,
    /// Spectral window / polarization pairing. Cf. C++ `MSDataDescription`.
    DataDescription,
    /// Feed characteristics (one per antenna per spectral window). Cf. C++ `MSFeed`.
    Feed,
    /// Field positions (phase center, etc.). Cf. C++ `MSField`.
    Field,
    /// Flag commands. Cf. C++ `MSFlagCmd`.
    FlagCmd,
    /// History log. Cf. C++ `MSHistory`.
    History,
    /// Observation metadata. Cf. C++ `MSObservation`.
    Observation,
    /// Antenna pointing information. Cf. C++ `MSPointing`.
    Pointing,
    /// Polarization setup. Cf. C++ `MSPolarization`.
    Polarization,
    /// Backend processor info. Cf. C++ `MSProcessor`.
    Processor,
    /// Spectral window setup (channel frequencies, widths). Cf. C++ `MSSpectralWindow`.
    SpectralWindow,
    /// Observing state (sig/ref, cal, sub-scan). Cf. C++ `MSState`.
    State,

    // -- Optional (5) --
    /// Doppler tracking info. Cf. C++ `MSDoppler`.
    Doppler,
    /// Frequency offset per antenna pair. Cf. C++ `MSFreqOffset`.
    FreqOffset,
    /// Source catalog entries. Cf. C++ `MSSource`.
    Source,
    /// System calibration (Tsys, Tcal, etc.). Cf. C++ `MSSysCal`.
    SysCal,
    /// Weather data. Cf. C++ `MSWeather`.
    Weather,
}

impl SubtableId {
    /// All 12 required subtables, in canonical order.
    pub const ALL_REQUIRED: &[SubtableId] = &[
        SubtableId::Antenna,
        SubtableId::DataDescription,
        SubtableId::Feed,
        SubtableId::Field,
        SubtableId::FlagCmd,
        SubtableId::History,
        SubtableId::Observation,
        SubtableId::Pointing,
        SubtableId::Polarization,
        SubtableId::Processor,
        SubtableId::SpectralWindow,
        SubtableId::State,
    ];

    /// All 5 optional subtables, in canonical order.
    pub const ALL_OPTIONAL: &[SubtableId] = &[
        SubtableId::Doppler,
        SubtableId::FreqOffset,
        SubtableId::Source,
        SubtableId::SysCal,
        SubtableId::Weather,
    ];

    /// Return the keyword/directory name used in the MS main table.
    ///
    /// These names match the C++ casacore convention exactly
    /// (e.g. `"SPECTRAL_WINDOW"`, not `"SpectralWindow"`).
    pub const fn name(&self) -> &'static str {
        match self {
            SubtableId::Antenna => "ANTENNA",
            SubtableId::DataDescription => "DATA_DESCRIPTION",
            SubtableId::Feed => "FEED",
            SubtableId::Field => "FIELD",
            SubtableId::FlagCmd => "FLAG_CMD",
            SubtableId::History => "HISTORY",
            SubtableId::Observation => "OBSERVATION",
            SubtableId::Pointing => "POINTING",
            SubtableId::Polarization => "POLARIZATION",
            SubtableId::Processor => "PROCESSOR",
            SubtableId::SpectralWindow => "SPECTRAL_WINDOW",
            SubtableId::State => "STATE",
            SubtableId::Doppler => "DOPPLER",
            SubtableId::FreqOffset => "FREQ_OFFSET",
            SubtableId::Source => "SOURCE",
            SubtableId::SysCal => "SYSCAL",
            SubtableId::Weather => "WEATHER",
        }
    }

    /// Whether this subtable is required by the MS standard.
    pub const fn is_required(&self) -> bool {
        matches!(
            self,
            SubtableId::Antenna
                | SubtableId::DataDescription
                | SubtableId::Feed
                | SubtableId::Field
                | SubtableId::FlagCmd
                | SubtableId::History
                | SubtableId::Observation
                | SubtableId::Pointing
                | SubtableId::Polarization
                | SubtableId::Processor
                | SubtableId::SpectralWindow
                | SubtableId::State
        )
    }
}

impl fmt::Display for SubtableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_count() {
        assert_eq!(SubtableId::ALL_REQUIRED.len(), 12);
    }

    #[test]
    fn optional_count() {
        assert_eq!(SubtableId::ALL_OPTIONAL.len(), 5);
    }

    #[test]
    fn all_required_are_required() {
        for id in SubtableId::ALL_REQUIRED {
            assert!(id.is_required(), "{id} should be required");
        }
    }

    #[test]
    fn all_optional_are_not_required() {
        for id in SubtableId::ALL_OPTIONAL {
            assert!(!id.is_required(), "{id} should be optional");
        }
    }

    #[test]
    fn no_duplicate_names() {
        let all: Vec<&str> = SubtableId::ALL_REQUIRED
            .iter()
            .chain(SubtableId::ALL_OPTIONAL.iter())
            .map(|id| id.name())
            .collect();
        for (i, name) in all.iter().enumerate() {
            assert!(
                !all[i + 1..].contains(name),
                "duplicate subtable name: {name}"
            );
        }
    }
}
