// SPDX-License-Identifier: LGPL-3.0-or-later
//! MS table and subtable schema definitions.
//!
//! Each submodule exports `REQUIRED_COLUMNS` and `OPTIONAL_COLUMNS` arrays
//! of `ColumnDef` constants. The
//! [`main_table`] module additionally exports keyword definitions.
//!
//! The [`SubtableId`] enum identifies the 17 MS subtables (12 required,
//! 5 optional) and provides `name()` / `is_required()` accessors.

mod subtable_id;

pub mod antenna;
pub mod data_description;
pub mod doppler;
pub mod feed;
pub mod field;
pub mod flag_cmd;
pub mod freq_offset;
pub mod history;
pub mod main_table;
pub mod observation;
pub mod pointing;
pub mod polarization;
pub mod processor;
pub mod source;
pub mod spectral_window;
pub mod state;
pub mod syscal;
pub mod weather;

pub use subtable_id::SubtableId;

use crate::column_def::ColumnDef;

/// Return the required columns for a given subtable.
pub fn required_columns(id: SubtableId) -> &'static [ColumnDef] {
    match id {
        SubtableId::Antenna => antenna::REQUIRED_COLUMNS,
        SubtableId::DataDescription => data_description::REQUIRED_COLUMNS,
        SubtableId::Feed => feed::REQUIRED_COLUMNS,
        SubtableId::Field => field::REQUIRED_COLUMNS,
        SubtableId::FlagCmd => flag_cmd::REQUIRED_COLUMNS,
        SubtableId::History => history::REQUIRED_COLUMNS,
        SubtableId::Observation => observation::REQUIRED_COLUMNS,
        SubtableId::Pointing => pointing::REQUIRED_COLUMNS,
        SubtableId::Polarization => polarization::REQUIRED_COLUMNS,
        SubtableId::Processor => processor::REQUIRED_COLUMNS,
        SubtableId::SpectralWindow => spectral_window::REQUIRED_COLUMNS,
        SubtableId::State => state::REQUIRED_COLUMNS,
        SubtableId::Doppler => doppler::REQUIRED_COLUMNS,
        SubtableId::FreqOffset => freq_offset::REQUIRED_COLUMNS,
        SubtableId::Source => source::REQUIRED_COLUMNS,
        SubtableId::SysCal => syscal::REQUIRED_COLUMNS,
        SubtableId::Weather => weather::REQUIRED_COLUMNS,
    }
}

/// Return the optional columns for a given subtable.
pub fn optional_columns(id: SubtableId) -> &'static [ColumnDef] {
    match id {
        SubtableId::Antenna => antenna::OPTIONAL_COLUMNS,
        SubtableId::DataDescription => data_description::OPTIONAL_COLUMNS,
        SubtableId::Feed => feed::OPTIONAL_COLUMNS,
        SubtableId::Field => field::OPTIONAL_COLUMNS,
        SubtableId::FlagCmd => flag_cmd::OPTIONAL_COLUMNS,
        SubtableId::History => history::OPTIONAL_COLUMNS,
        SubtableId::Observation => observation::OPTIONAL_COLUMNS,
        SubtableId::Pointing => pointing::OPTIONAL_COLUMNS,
        SubtableId::Polarization => polarization::OPTIONAL_COLUMNS,
        SubtableId::Processor => processor::OPTIONAL_COLUMNS,
        SubtableId::SpectralWindow => spectral_window::OPTIONAL_COLUMNS,
        SubtableId::State => state::OPTIONAL_COLUMNS,
        SubtableId::Doppler => doppler::OPTIONAL_COLUMNS,
        SubtableId::FreqOffset => freq_offset::OPTIONAL_COLUMNS,
        SubtableId::Source => source::OPTIONAL_COLUMNS,
        SubtableId::SysCal => syscal::OPTIONAL_COLUMNS,
        SubtableId::Weather => weather::OPTIONAL_COLUMNS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;
    use std::collections::HashSet;

    #[test]
    fn all_subtable_schemas_build() {
        let all_ids: Vec<SubtableId> = SubtableId::ALL_REQUIRED
            .iter()
            .chain(SubtableId::ALL_OPTIONAL.iter())
            .copied()
            .collect();

        for id in &all_ids {
            build_table_schema(required_columns(*id))
                .unwrap_or_else(|e| panic!("{}: {e}", id.name()));
        }
    }

    #[test]
    fn no_duplicate_column_names_within_subtables() {
        let all_ids: Vec<SubtableId> = SubtableId::ALL_REQUIRED
            .iter()
            .chain(SubtableId::ALL_OPTIONAL.iter())
            .copied()
            .collect();

        for id in &all_ids {
            let mut names = HashSet::new();
            for col in required_columns(*id)
                .iter()
                .chain(optional_columns(*id).iter())
            {
                assert!(
                    names.insert(col.name),
                    "{}: duplicate column {}",
                    id.name(),
                    col.name
                );
            }
        }
    }

    #[test]
    fn column_names_match_expected() {
        // Spot-check a few critical column names for C++ interop
        assert!(
            antenna::REQUIRED_COLUMNS
                .iter()
                .any(|c| c.name == "POSITION")
        );
        assert!(
            antenna::REQUIRED_COLUMNS
                .iter()
                .any(|c| c.name == "DISH_DIAMETER")
        );
        assert!(
            spectral_window::REQUIRED_COLUMNS
                .iter()
                .any(|c| c.name == "CHAN_FREQ")
        );
        assert!(
            spectral_window::REQUIRED_COLUMNS
                .iter()
                .any(|c| c.name == "MEAS_FREQ_REF")
        );
        assert!(
            field::REQUIRED_COLUMNS
                .iter()
                .any(|c| c.name == "PHASE_DIR")
        );
        assert!(
            field::REQUIRED_COLUMNS
                .iter()
                .any(|c| c.name == "DELAY_DIR")
        );
        assert!(
            field::REQUIRED_COLUMNS
                .iter()
                .any(|c| c.name == "REFERENCE_DIR")
        );
        assert!(
            polarization::REQUIRED_COLUMNS
                .iter()
                .any(|c| c.name == "CORR_TYPE")
        );
        assert!(
            polarization::REQUIRED_COLUMNS
                .iter()
                .any(|c| c.name == "CORR_PRODUCT")
        );
    }
}
