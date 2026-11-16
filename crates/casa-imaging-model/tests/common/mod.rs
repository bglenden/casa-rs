// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CorrelationProduct, FlagPolicy, IdSelection, IntentSelection, ObservationSelection,
    ObservationSnapshot, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, PartitionId, PartitionSelection, RowSelection, SelectedColumns,
    SpectralWindowSelection, UvSelection, VisibilityColumn, WeightColumn, compile_observation,
};
use casa_types::Correlation;

pub fn observation_snapshot(observation: u8) -> ObservationSnapshot {
    compile_observation(ObservationSnapshotInput::new(vec![observation_source(
        observation,
    )]))
    .expect("compile test observation")
}

pub fn observation_source(observation: u8) -> ObservationSourceInput {
    observation_source_with_corrected_data(observation, false)
}

/// A one-partition source; `corrected_data_present` says whether MAIN has
/// `CORRECTED_DATA`.
pub fn observation_source_with_corrected_data(
    observation: u8,
    corrected_data_present: bool,
) -> ObservationSourceInput {
    // The observation seed sets the selected samples, so sources from
    // different seeds are different observations.
    let selection = ObservationSelection::new(
        1 + u64::from(observation),
        RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All),
        vec![PartitionSelection::new(
            PartitionId(0),
            0,
            vec![CorrelationProduct::new(0, Correlation::I)],
        )],
        vec![SpectralWindowSelection::new(0, vec![0])],
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
