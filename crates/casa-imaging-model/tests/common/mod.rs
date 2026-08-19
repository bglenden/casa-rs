// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AntennaSelection, ColumnGeneration, ConsistencyToken, CorrelationProduct, CorrelationSelection,
    CorrelationType, FlagPolicy, IdSelection, IntentSelection, LogicalIdentity,
    MeasurementSetIdentity, MetadataGeneration, MetadataTableKind, ModelColumnState,
    ModelStateIdentity, MsColumnKind, ObservationSelection, ObservationSnapshot,
    ObservationSnapshotInput, ObservationSourceInput, ObservationSourceProvenance,
    ProblemInputIdentities, ReferenceDataKind, RowSelection, SelectedColumns, SelectedRows,
    SourceGenerations, SpectralWindowSelection, TimeSelection, UvSelection, VisibilityColumn,
    WeightColumn, compile_observation,
};

pub fn identity(byte: u8) -> LogicalIdentity {
    LogicalIdentity::from_sha256([byte; 32])
}

fn scoped_identity(seed: u8, scope: u8) -> LogicalIdentity {
    let mut digest = [seed; 32];
    digest[0] = scope;
    LogicalIdentity::from_sha256(digest)
}

pub fn observation_snapshot(
    observation: u8,
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
    model: ModelStateIdentity,
) -> ObservationSnapshot {
    compile_observation(ObservationSnapshotInput::new(
        vec![observation_source(observation)],
        reference_data,
        model,
    ))
    .expect("compile test observation")
}

pub fn observation_source(observation: u8) -> ObservationSourceInput {
    observation_source_with_model_generation(observation, None)
}

pub fn observation_source_with_model_generation(
    observation: u8,
    model_generation: Option<LogicalIdentity>,
) -> ObservationSourceInput {
    observation_source_with_model_state(
        observation,
        model_generation.map_or(ModelColumnState::Absent, ModelColumnState::Present),
        model_generation,
    )
}

pub fn observation_source_with_model_state(
    observation: u8,
    model_column: ModelColumnState,
    consumed_model_generation: Option<LogicalIdentity>,
) -> ObservationSourceInput {
    let column_kinds = [
        MsColumnKind::Data,
        MsColumnKind::Flag,
        MsColumnKind::FlagRow,
        MsColumnKind::Weight,
        MsColumnKind::Uvw,
        MsColumnKind::Time,
        MsColumnKind::TimeCentroid,
        MsColumnKind::Interval,
        MsColumnKind::Exposure,
        MsColumnKind::FieldId,
        MsColumnKind::DataDescriptionId,
        MsColumnKind::Antenna1,
        MsColumnKind::Antenna2,
        MsColumnKind::Feed1,
        MsColumnKind::Feed2,
        MsColumnKind::ScanNumber,
        MsColumnKind::StateId,
        MsColumnKind::ObservationId,
        MsColumnKind::ArrayId,
    ];
    let mut columns = column_kinds
        .into_iter()
        .enumerate()
        .map(|(index, kind)| {
            ColumnGeneration::new(kind, scoped_identity(observation, 20 + index as u8))
        })
        .collect::<Vec<_>>();
    if let Some(generation) = consumed_model_generation {
        columns.push(ColumnGeneration::new(MsColumnKind::ModelData, generation));
    }
    let metadata_kinds = [
        MetadataTableKind::Antenna,
        MetadataTableKind::DataDescription,
        MetadataTableKind::Feed,
        MetadataTableKind::Field,
        MetadataTableKind::Observation,
        MetadataTableKind::Pointing,
        MetadataTableKind::Polarization,
        MetadataTableKind::SpectralWindow,
        MetadataTableKind::State,
    ];
    let metadata = metadata_kinds
        .into_iter()
        .enumerate()
        .map(|(index, kind)| {
            MetadataGeneration::new(kind, scoped_identity(observation, 60 + index as u8))
        })
        .collect();
    let selection = ObservationSelection::new(
        SelectedRows::new(1, 1, scoped_identity(observation, 2)),
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
        vec![SpectralWindowSelection::new(0, vec![0], vec![0])],
        vec![CorrelationSelection::new(
            0,
            vec![CorrelationProduct::new(0, CorrelationType::StokesI)],
        )],
    );
    ObservationSourceInput::new(
        MeasurementSetIdentity::new(scoped_identity(observation, 1)),
        ObservationSourceProvenance::new(
            format!("fixture://observation/{observation}"),
            scoped_identity(observation, 3),
        ),
        selection,
        SourceGenerations::new(
            ConsistencyToken::new(scoped_identity(observation, 4)),
            SelectedColumns::new(
                VisibilityColumn::Data,
                FlagPolicy::FlagOrFlagRow,
                WeightColumn::Weight,
                columns,
            ),
            metadata,
            model_column,
        ),
    )
}

#[allow(dead_code)]
pub fn problem_inputs(
    observation: u8,
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
    model: ModelStateIdentity,
) -> ProblemInputIdentities {
    ProblemInputIdentities::new(observation_snapshot(observation, reference_data, model))
}
