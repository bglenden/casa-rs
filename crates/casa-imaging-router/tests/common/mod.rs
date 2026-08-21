// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AntennaSelection, ColumnGeneration, ConsistencyToken, CorrelationProduct, CorrelationSelection,
    CorrelationType, DataDescriptionSelection, FlagPolicy, IdSelection, IntentSelection,
    LogicalIdentity, MeasurementSetIdentity, MetadataGeneration, MetadataTableKind,
    ModelColumnState, ModelStateIdentity, MsColumnKind, ObservationSelection,
    ObservationSnapshotInput, ObservationSourceInput, ObservationSourceProvenance,
    ProblemInputIdentities, ReferenceDataKind, RowSelection, SelectedColumns, SelectedMainRow,
    SelectedRows, SourceGenerations, SpectralWindowSelection, TimeSelection, UvSelection,
    VisibilityColumn, WeightColumn, compile_observation,
};

fn scoped_identity(seed: u8, scope: u8) -> LogicalIdentity {
    let mut digest = [seed; 32];
    digest[0] = scope;
    LogicalIdentity::from_sha256(digest)
}

pub fn problem_inputs(
    observation: u8,
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
    model: ModelStateIdentity,
) -> ProblemInputIdentities {
    let columns = [
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
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| {
        ColumnGeneration::new(kind, scoped_identity(observation, 20 + index as u8))
    })
    .collect();
    let metadata = [
        MetadataTableKind::Antenna,
        MetadataTableKind::DataDescription,
        MetadataTableKind::Feed,
        MetadataTableKind::Field,
        MetadataTableKind::Observation,
        MetadataTableKind::Pointing,
        MetadataTableKind::Polarization,
        MetadataTableKind::SpectralWindow,
        MetadataTableKind::State,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| {
        MetadataGeneration::new(kind, scoped_identity(observation, 60 + index as u8))
    })
    .collect();
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
                ModelColumnState::Absent,
            ),
        )],
        reference_data,
        model,
    ))
    .expect("compile router test observation");
    ProblemInputIdentities::new(snapshot)
}
