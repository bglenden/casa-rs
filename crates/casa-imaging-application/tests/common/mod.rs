// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    FlagPolicy, IdSelection, IntentSelection, ModelBounds, ModelLifecycleRequirements,
    NumericPrecision, ObservationSelection, ObservationSnapshot, ObservationSnapshotInput,
    ObservationSourceInput, ObservationSourceProvenance, RowSelection, SelectedColumns,
    SelectedMainRow, SelectedRows, SpectralWindowSelection, UvSelection, VisibilityColumn,
    WeightColumn, compile_observation,
};

pub fn model_lifecycle() -> ModelLifecycleRequirements {
    ModelLifecycleRequirements::new(
        ModelBounds::new(10_000_000, 10_000_000, 1.0e30, 1.0e30)
            .expect("valid model lifecycle fixture bounds"),
        NumericPrecision::F64,
    )
}

pub fn observation() -> ObservationSnapshot {
    let selection = ObservationSelection::new(
        SelectedRows::from_ordered_main_rows(1, [SelectedMainRow::new(0, 0)])
            .expect("single selected MAIN row fixture"),
        RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All),
        vec![DataDescriptionSelection::new(0, 0, 0)],
        vec![SpectralWindowSelection::new(0, vec![0])],
        vec![CorrelationSelection::new(
            0,
            vec![CorrelationProduct::new(0, CorrelationType::StokesI)],
        )],
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
