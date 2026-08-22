// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AntennaSelection, ColumnGeneration, ConsistencyToken, CorrelationProduct, CorrelationSelection,
    CorrelationType, FlagPolicy, IdSelection, IntentSelection, LogicalIdentity,
    MeasurementSetIdentity, MetadataGeneration, MetadataTableKind, ModelColumnState,
    ModelStateIdentity, MsColumnKind, ObservationSelection, ObservationSnapshotInput,
    ObservationSourceInput, ObservationSourceProvenance, ProblemInputIdentities, ReferenceDataKind,
    RowSelection, SelectedColumns, SelectedRows, SourceGenerations, SpectralWindowSelection,
    TimeSelection, UvSelection, VisibilityColumn, WeightColumn, compile_observation,
};

fn identity(scope: u8) -> LogicalIdentity {
    LogicalIdentity::from_sha256([scope; 32])
}

pub fn problem_inputs(
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
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
    .map(|(index, kind)| ColumnGeneration::new(kind, identity(20 + index as u8)))
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
    .map(|(index, kind)| MetadataGeneration::new(kind, identity(60 + index as u8)))
    .collect();
    let selection = ObservationSelection::new(
        SelectedRows::new(1, 1, identity(2)),
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
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![ObservationSourceInput::new(
            MeasurementSetIdentity::new(identity(1)),
            ObservationSourceProvenance::new("fixture://router.ms".to_string(), identity(3)),
            selection,
            SourceGenerations::new(
                ConsistencyToken::new(identity(4)),
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
        ModelStateIdentity::Empty,
    ))
    .expect("compile router test observation");
    ProblemInputIdentities::new(snapshot)
}
