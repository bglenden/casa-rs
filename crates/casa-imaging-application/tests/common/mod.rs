// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CorrelationProduct, FlagPolicy, IdSelection, IntentSelection, ModelBounds,
    ModelLifecycleRequirements, NumericPrecision, ObservationSelection, ObservationSnapshot,
    ObservationSnapshotInput, ObservationSourceInput, ObservationSourceProvenance, PartitionId,
    PartitionSelection, RowSelection, SelectedColumns, SpectralWindowSelection, UvSelection,
    VisibilityColumn, WeightColumn, compile_observation,
};
use casa_types::Correlation;

pub fn model_lifecycle() -> ModelLifecycleRequirements {
    ModelLifecycleRequirements::new(
        ModelBounds::new(10_000_000, 10_000_000, 1.0e30, 1.0e30)
            .expect("valid model lifecycle fixture bounds"),
        NumericPrecision::F64,
    )
}

pub fn observation() -> ObservationSnapshot {
    let selection = ObservationSelection::new(
        1,
        RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All),
        vec![PartitionSelection::new(
            PartitionId(0),
            0,
            vec![CorrelationProduct::new(0, Correlation::I)],
        )],
        vec![SpectralWindowSelection::new(0, vec![0])],
    );
    compile_observation(ObservationSnapshotInput::new(vec![
        ObservationSourceInput::new(
            ObservationSourceProvenance::new("fixture://router.ms".to_string()),
            selection,
            SelectedColumns::new(
                VisibilityColumn::Data,
                FlagPolicy::FlagOrFlagRow,
                WeightColumn::Weight,
            ),
            false,
        ),
    ]))
    .expect("compile router test observation")
}
