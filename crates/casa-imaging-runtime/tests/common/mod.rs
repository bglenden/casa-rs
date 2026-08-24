// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    path::{Path, PathBuf},
    sync::OnceLock,
};

use casa_imaging_model::{
    AntennaSelection, ColumnGeneration, ConsistencyToken, CorrelationProduct, CorrelationSelection,
    CorrelationType, DataDescriptionSelection, FlagPolicy, IdSelection, IntentSelection,
    LogicalIdentity, MeasurementSetIdentity, MetadataGeneration, MetadataTableKind, ModelBounds,
    ModelColumnState, ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity,
    MsColumnKind, NumericPrecision, ObservationSelection, ObservationSnapshotInput,
    ObservationSourceInput, ObservationSourceProvenance, ProblemInputIdentities, ReferenceDataKind,
    RowSelection, SelectedColumns, SelectedMainRow, SelectedRows, SourceGenerations,
    SpectralWindowSelection, TimeSelection, UvSelection, VisibilityColumn, WeightColumn,
    compile_observation,
};
use casa_ms::{
    SyntheticObservationRequest, SyntheticPolarizationBasis, SyntheticPolarizationSetup,
    SyntheticWorkerPolicy, generate_synthetic_observation_ms, tutorial_vla_a_antennas,
};

pub fn identity(byte: u8) -> LogicalIdentity {
    LogicalIdentity::from_sha256([byte; 32])
}

pub fn model_lifecycle(model: ModelStateIdentity) -> ModelLifecycleRequirements {
    let input = match model {
        ModelStateIdentity::Empty => ModelInputCommitment::Empty,
        ModelStateIdentity::Seed(source) => ModelInputCommitment::AlignedSeed {
            source,
            support: identity(0xa5),
        },
        ModelStateIdentity::Generation(generation) => ModelInputCommitment::Generation(generation),
    };
    ModelLifecycleRequirements::new(
        ModelBounds::new(
            10_000_000, 10_000_000, 10_000_000, 10_000_000, 1.0e30, 1.0e30,
        )
        .expect("valid model lifecycle fixture bounds"),
        NumericPrecision::F64,
        input,
    )
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
    problem_inputs_with_source_count(observation, reference_data, model, 1)
}

pub fn problem_inputs_with_source_count(
    observation: u8,
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
    model: ModelStateIdentity,
    source_count: usize,
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
    let columns: Vec<_> = column_kinds
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
    let metadata: Vec<_> = metadata_kinds
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
    let sources = (0..source_count)
        .map(|source_index| {
            let source_index = u8::try_from(source_index).expect("bounded fixture source count");
            ObservationSourceInput::new(
                MeasurementSetIdentity::new(scoped_identity(observation, 1 + source_index)),
                ObservationSourceProvenance::new(
                    runtime_observation_fixture().display().to_string(),
                    scoped_identity(observation, 3 + source_index),
                ),
                selection.clone(),
                SourceGenerations::new(
                    ConsistencyToken::new(scoped_identity(observation, 4 + source_index)),
                    SelectedColumns::new(
                        VisibilityColumn::Data,
                        FlagPolicy::FlagOrFlagRow,
                        WeightColumn::Weight,
                        columns.clone(),
                    ),
                    metadata.clone(),
                    ModelColumnState::Absent,
                ),
            )
        })
        .collect();
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        sources,
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
