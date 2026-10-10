// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    FlagPolicy, IdSelection, IntentSelection, ObservationSelection, ObservationSnapshot,
    ObservationSnapshotInput, ObservationSourceInput, ObservationSourceProvenance, RowSelection,
    SelectedColumns, SelectedMainRow, SelectedRows, SpectralWindowSelection, UvSelection,
    VisibilityColumn, WeightColumn, compile_observation,
};

pub fn observation_snapshot(observation: u8) -> ObservationSnapshot {
    compile_observation(ObservationSnapshotInput::new(vec![observation_source(
        observation,
    )]))
    .expect("compile test observation")
}

pub fn observation_source(observation: u8) -> ObservationSourceInput {
    observation_source_with_corrected_data(observation, false)
}

/// A one-row source; `corrected_data_present` says whether MAIN has
/// `CORRECTED_DATA`.
pub fn observation_source_with_corrected_data(
    observation: u8,
    corrected_data_present: bool,
) -> ObservationSourceInput {
    // The observation seed sets the MeasurementSet's row count, so sources
    // from different seeds are different observations.
    let selection = ObservationSelection::new(
        SelectedRows::from_ordered_main_rows(
            1 + u64::from(observation),
            [SelectedMainRow::new(0, 0)],
        )
        .expect("single selected MAIN row fixture"),
        RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All),
        vec![DataDescriptionSelection::new(0, 0, 0)],
        vec![SpectralWindowSelection::new(0, vec![0])],
        vec![CorrelationSelection::new(
            0,
            vec![CorrelationProduct::new(0, CorrelationType::StokesI)],
        )],
    );
    ObservationSourceInput::new(
        ObservationSourceProvenance::new(format!("fixture://observation/{observation}")),
        selection,
        SelectedColumns::new(
            VisibilityColumn::Data,
            FlagPolicy::FlagOrFlagRow,
            WeightColumn::Weight,
        ),
        corrected_data_present,
    )
}
