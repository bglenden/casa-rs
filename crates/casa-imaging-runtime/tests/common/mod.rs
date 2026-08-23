// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    path::{Path, PathBuf},
    sync::OnceLock,
};

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
use casa_ms::{
    SyntheticObservationRequest, SyntheticPolarizationBasis, SyntheticPolarizationSetup,
    SyntheticWorkerPolicy, generate_synthetic_observation_ms, tutorial_vla_a_antennas,
};

pub fn identity(byte: u8) -> LogicalIdentity {
    LogicalIdentity::from_sha256([byte; 32])
}

fn scoped_identity(seed: u8, scope: u8) -> LogicalIdentity {
    let mut digest = identity(seed).as_bytes();
    digest[0] = scope;
    LogicalIdentity::from_sha256(digest)
}

pub fn problem_inputs(
    observation: u8,
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
    model: ModelStateIdentity,
) -> ProblemInputIdentities {
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
    let columns = column_kinds
        .into_iter()
        .enumerate()
        .map(|(index, kind)| {
            ColumnGeneration::new(kind, scoped_identity(observation, 20 + index as u8))
        })
        .collect();
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
            vec![CorrelationProduct::new(0, CorrelationType::CircularRr)],
        )],
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![ObservationSourceInput::new(
            MeasurementSetIdentity::new(scoped_identity(observation, 1)),
            ObservationSourceProvenance::new(
                runtime_observation_fixture().display().to_string(),
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
    .expect("compile test observation");
    ProblemInputIdentities::new(snapshot)
}

struct RuntimeObservationFixture {
    _directory: tempfile::TempDir,
    path: PathBuf,
}

fn runtime_observation_fixture() -> &'static Path {
    static FIXTURE: OnceLock<RuntimeObservationFixture> = OnceLock::new();
    &FIXTURE
        .get_or_init(|| {
            let directory = tempfile::tempdir().expect("runtime observation fixture directory");
            let path = directory.path().join("runtime-observation.ms");
            let mut antennas = tutorial_vla_a_antennas();
            antennas.truncate(2);
            let mut request =
                SyntheticObservationRequest::vla_ppdisk("unused.fits", &path, antennas);
            request.predict_model = false;
            request.allow_below_elevation_limit = true;
            request.duration_seconds = 1.0;
            request.integration_seconds = 1.0;
            request.polarization_setup =
                SyntheticPolarizationSetup::new(SyntheticPolarizationBasis::Circular, 1)
                    .expect("one-correlation runtime fixture");
            request.worker_policy = SyntheticWorkerPolicy::Fixed;
            request.row_workers = Some(1);
            request.channel_workers = Some(1);
            generate_synthetic_observation_ms(&request)
                .expect("generate runtime selected-observation fixture");
            RuntimeObservationFixture {
                _directory: directory,
                path,
            }
        })
        .path
}
