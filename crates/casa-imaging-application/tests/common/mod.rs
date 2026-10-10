// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AntennaSelection, CorrelationProduct, CorrelationSelection, CorrelationType,
    DataDescriptionSelection, FlagPolicy, IdSelection, IntentSelection, LogicalIdentity,
    ModelBounds, ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity,
    NumericPrecision, ObservationSelection, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, ProblemInputIdentities, ReferenceDataKind, RowSelection,
    SelectedColumns, SelectedMainRow, SelectedRows, SpectralWindowSelection, TimeSelection,
    UvSelection, VisibilityColumn, WeightColumn, compile_observation,
};

fn identity(scope: u8) -> LogicalIdentity {
    LogicalIdentity::from_bytes([scope; 32])
}

pub fn model_lifecycle() -> ModelLifecycleRequirements {
    ModelLifecycleRequirements::new(
        ModelBounds::new(
            10_000_000, 10_000_000, 10_000_000, 10_000_000, 1.0e30, 1.0e30,
        )
        .expect("valid model lifecycle fixture bounds"),
        NumericPrecision::F64,
        ModelInputCommitment::Empty,
    )
}

pub fn problem_inputs(
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
) -> ProblemInputIdentities {
    let selection = ObservationSelection::new(
        SelectedRows::from_ordered_main_rows(1, [SelectedMainRow::new(0, 0)])
            .expect("single selected MAIN row fixture"),
        RowSelection::new(
            IdSelection::All,
            TimeSelection::All,
            UvSelection::All,
            AntennaSelection::All,
            IdSelection::All,
            IdSelection::All,
            IntentSelection::All,
            IdSelection::All,
        ),
        vec![DataDescriptionSelection::new(0, 0, 0)],
        vec![SpectralWindowSelection::new(0, vec![0])],
        vec![CorrelationSelection::new(
            0,
            vec![CorrelationProduct::new(0, CorrelationType::StokesI)],
        )],
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![ObservationSourceInput::new(
            ObservationSourceProvenance::new("fixture://router.ms".to_string(), identity(3)),
            selection,
            SelectedColumns::new(
                VisibilityColumn::Data,
                FlagPolicy::FlagOrFlagRow,
                WeightColumn::Weight,
            ),
            false,
        )],
        reference_data,
        ModelStateIdentity::Empty,
    ))
    .expect("compile router test observation");
    ProblemInputIdentities::new(snapshot)
}
