// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AntennaBaseline, AntennaSelection, ColumnGeneration, CompileObservationError, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    FlagPolicy, IdSelection, IntentSelection, LogicalIdentity, MeasurementSetIdentity,
    MetadataGeneration, MetadataTableKind, ModelColumnState, ModelStateIdentity, MsColumnKind,
    ObservationConsistencyError, ObservationSelection, ObservationSnapshot, ObservationSnapshotId,
    ObservationSnapshotInput, ObservationSourceInput, ObservationSourceProvenance,
    ObservationSourceState, ObservationState, ReferenceDataKind, ResolvedIntent, RowSelection,
    SelectedColumns, SelectedMainRow, SelectedRowManifestValidationError, SelectedRowSequenceError,
    SelectedRowSequenceId, SelectedRows, SelectionBound, SourceGenerations,
    SpectralWindowSelection, TimeRange, TimeSelection, UvDistanceRange, UvDistanceUnit,
    UvSelection, VisibilityColumn, WeightColumn, compile_observation,
};

fn identity(byte: u8) -> LogicalIdentity {
    LogicalIdentity::from_sha256([byte; 32])
}

fn selected_rows(row_variant: u8) -> SelectedRows {
    let mut rows = (0_u64..10).collect::<Vec<_>>();
    rows.push(10 + u64::from(row_variant) % 90);
    SelectedRows::from_ordered_main_rows(
        100,
        rows.into_iter().map(|row| SelectedMainRow::new(row, 1)),
    )
    .expect("canonical selected-row fixture")
}

fn main_rows<const N: usize>(rows: [u64; N]) -> [SelectedMainRow; N] {
    rows.map(|row| SelectedMainRow::new(row, 0))
}

struct InexactRowCount<I> {
    rows: I,
    declared_len: usize,
}

impl<I: Iterator<Item = SelectedMainRow>> Iterator for InexactRowCount<I> {
    type Item = SelectedMainRow;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.declared_len, Some(self.declared_len))
    }
}

impl<I: Iterator<Item = SelectedMainRow>> ExactSizeIterator for InexactRowCount<I> {
    fn len(&self) -> usize {
        self.declared_len
    }
}

#[test]
fn selected_row_sequence_manifest_is_storage_owner_reproducible() {
    let planned = SelectedRows::from_ordered_main_rows(12, main_rows([0, 3, 7, 11]))
        .expect("compiler-selected physical MAIN rows");
    let reopened = SelectedRows::from_ordered_main_rows(12, main_rows([0, 3, 7, 11]))
        .expect("storage owner re-resolved the same physical MAIN rows");
    let changed = SelectedRows::from_ordered_main_rows(12, main_rows([0, 3, 8, 11]))
        .expect("different valid physical MAIN rows");
    let empty = SelectedRows::from_ordered_main_rows(12, main_rows([]))
        .expect("one source may contribute no selected rows");
    let empty_larger_source = SelectedRows::from_ordered_main_rows(20, main_rows([]))
        .expect("empty row identity excludes the separately retained source count");

    assert_eq!(planned, reopened);
    assert_ne!(planned, changed);
    assert_eq!(planned.source_row_count(), 12);
    assert_eq!(planned.selected_row_count(), 4);
    assert_eq!(empty.selected_row_count(), 0);
    assert_eq!(empty.sequence_id(), empty_larger_source.sequence_id());
    assert_ne!(empty, empty_larger_source);
    assert_eq!(SelectedRowSequenceId::SCHEMA_VERSION, 2);
    assert_eq!(
        planned.sequence_id().as_bytes(),
        [
            130, 199, 211, 154, 223, 41, 223, 172, 141, 120, 225, 121, 188, 85, 28, 237, 187, 229,
            98, 77, 157, 11, 182, 75, 139, 206, 31, 38, 9, 148, 240, 42,
        ]
    );
    assert_eq!(
        SelectedRows::from_ordered_main_rows(12, main_rows([0, 3, 3, 11])),
        Err(SelectedRowSequenceError::DuplicatePhysicalRow { row: 3 })
    );
    assert_eq!(
        SelectedRows::from_ordered_main_rows(12, main_rows([0, 7, 3, 11])),
        Err(SelectedRowSequenceError::DescendingPhysicalRow {
            previous_row: 7,
            row: 3,
        })
    );
    assert_eq!(
        SelectedRows::from_ordered_main_rows(12, main_rows([7, 3, 12])),
        Err(SelectedRowSequenceError::DescendingPhysicalRow {
            previous_row: 7,
            row: 3,
        }),
        "a later out-of-range row does not replace the first encountered failure"
    );
    assert_eq!(
        SelectedRows::from_ordered_main_rows(12, main_rows([3, 2, 3])),
        Err(SelectedRowSequenceError::DescendingPhysicalRow {
            previous_row: 3,
            row: 2,
        }),
        "a non-adjacent repeat necessarily violates ascending order first"
    );
    assert_eq!(
        SelectedRows::from_ordered_main_rows(12, main_rows([0, 3, 7, 12])),
        Err(SelectedRowSequenceError::PhysicalRowOutOfRange {
            row: 12,
            source_row_count: 12,
        })
    );
}

#[test]
fn selected_main_row_manifest_binds_data_description_to_physical_row() {
    let planned = SelectedRows::from_ordered_main_rows(
        12,
        [
            SelectedMainRow::new(0, 2),
            SelectedMainRow::new(3, 5),
            SelectedMainRow::new(7, 5),
        ],
    )
    .expect("compiler-selected MAIN row coordinates");
    let reopened = SelectedRows::from_ordered_main_rows(
        12,
        [
            SelectedMainRow::new(0, 2),
            SelectedMainRow::new(3, 5),
            SelectedMainRow::new(7, 5),
        ],
    )
    .expect("storage owner reproduced the same MAIN row coordinates");
    let substituted = SelectedRows::from_ordered_main_rows(
        12,
        [
            SelectedMainRow::new(0, 2),
            SelectedMainRow::new(3, 2),
            SelectedMainRow::new(7, 5),
        ],
    )
    .expect("same physical rows with a different DATA_DESC_ID association");

    assert_eq!(planned, reopened);
    assert_ne!(planned.sequence_id(), substituted.sequence_id());
    assert_eq!(SelectedRowSequenceId::SCHEMA_VERSION, 2);
}

#[test]
fn selected_main_row_manifest_validates_a_fallible_bounded_replay() {
    let planned = SelectedRows::from_ordered_main_rows(
        12,
        [
            SelectedMainRow::new(0, 2),
            SelectedMainRow::new(3, 5),
            SelectedMainRow::new(7, 5),
        ],
    )
    .expect("compiler-selected MAIN row coordinates");

    planned
        .validate_ordered_main_rows(
            [
                SelectedMainRow::new(0, 2),
                SelectedMainRow::new(3, 5),
                SelectedMainRow::new(7, 5),
            ]
            .into_iter()
            .map(Ok::<_, std::io::Error>),
        )
        .expect("the retained source reproduced the exact compact manifest");

    let mismatch = planned
        .validate_ordered_main_rows(
            [
                SelectedMainRow::new(0, 2),
                SelectedMainRow::new(3, 2),
                SelectedMainRow::new(7, 5),
            ]
            .into_iter()
            .map(Ok::<_, std::io::Error>),
        )
        .expect_err("same-count DDID substitution must not validate");
    assert!(matches!(
        mismatch,
        SelectedRowManifestValidationError::ManifestMismatch {
            expected_row_count: 3,
            observed_row_count: 3,
            ..
        }
    ));

    let source_failure = planned
        .validate_ordered_main_rows([
            Ok(SelectedMainRow::new(0, 2)),
            Err(std::io::Error::other("retained MAIN read failed")),
        ])
        .expect_err("a storage failure must not become missing science");
    match source_failure {
        SelectedRowManifestValidationError::Source(source) => {
            assert_eq!(source.kind(), std::io::ErrorKind::Other);
            assert_eq!(source.to_string(), "retained MAIN read failed");
        }
        other => panic!("expected the original storage failure, got {other}"),
    }
}

#[test]
fn selected_row_sequence_rejects_inexact_iterator_length() {
    let rows = InexactRowCount {
        rows: main_rows([0, 3, 7]).into_iter(),
        declared_len: 4,
    };

    assert_eq!(
        SelectedRows::from_ordered_main_rows(12, rows),
        Err(SelectedRowSequenceError::DeclaredRowCountMismatch {
            declared_row_count: 4,
            observed_row_count: 3,
        })
    );
}

fn columns(seed: u8, reverse: bool) -> SelectedColumns {
    let kinds = [
        MsColumnKind::CorrectedData,
        MsColumnKind::Flag,
        MsColumnKind::FlagRow,
        MsColumnKind::WeightSpectrum,
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
    let mut generations = kinds
        .into_iter()
        .enumerate()
        .map(|(offset, kind)| ColumnGeneration::new(kind, identity(seed + offset as u8)))
        .collect::<Vec<_>>();
    if reverse {
        generations.reverse();
    }
    SelectedColumns::new(
        VisibilityColumn::CorrectedData,
        FlagPolicy::FlagOrFlagRow,
        WeightColumn::WeightSpectrum,
        generations,
    )
}

fn metadata(seed: u8, reverse: bool) -> Vec<MetadataGeneration> {
    let kinds = [
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
    let mut generations = kinds
        .into_iter()
        .enumerate()
        .map(|(offset, kind)| MetadataGeneration::new(kind, identity(seed + offset as u8)))
        .collect::<Vec<_>>();
    if reverse {
        generations.reverse();
    }
    generations
}

fn selection(row_digest: u8, reverse: bool) -> ObservationSelection {
    let mut fields = vec![7, 2];
    let mut scans = vec![12, 4];
    let mut observations = vec![3, 1];
    let mut arrays = vec![2, 0];
    let mut baselines = vec![AntennaBaseline::new(5, 1), AntennaBaseline::new(4, 2)];
    let mut intents = vec![
        ResolvedIntent::new(8, "OBSERVE_TARGET#ON_SOURCE".to_string()),
        ResolvedIntent::new(3, "CALIBRATE_PHASE#ON_SOURCE".to_string()),
    ];
    let mut data_descriptions = vec![
        DataDescriptionSelection::new(6, 9, 5),
        DataDescriptionSelection::new(4, 2, 5),
        DataDescriptionSelection::new(1, 2, 1),
    ];
    let mut spectral_windows = vec![
        SpectralWindowSelection::new(9, vec![7, 5, 3]),
        SpectralWindowSelection::new(2, vec![8, 4, 0]),
    ];
    let mut correlations = vec![
        CorrelationSelection::new(
            5,
            vec![
                CorrelationProduct::new(1, CorrelationType::LinearYy),
                CorrelationProduct::new(0, CorrelationType::LinearXx),
            ],
        ),
        CorrelationSelection::new(
            1,
            vec![
                CorrelationProduct::new(1, CorrelationType::CircularLl),
                CorrelationProduct::new(0, CorrelationType::CircularRr),
            ],
        ),
    ];
    if reverse {
        fields.reverse();
        scans.reverse();
        observations.reverse();
        arrays.reverse();
        baselines.reverse();
        intents.reverse();
        data_descriptions.reverse();
        spectral_windows.reverse();
        correlations.reverse();
    }

    ObservationSelection::new(
        selected_rows(row_digest),
        RowSelection::new(
            IdSelection::Only(fields),
            TimeSelection::Ranges(vec![TimeRange::new(
                Some(SelectionBound::inclusive(5_000_000_000.0)),
                Some(SelectionBound::exclusive(5_000_000_010.0)),
            )]),
            UvSelection::Ranges(vec![UvDistanceRange::new(
                Some(SelectionBound::inclusive(10.0)),
                Some(SelectionBound::inclusive(1_000.0)),
                UvDistanceUnit::Wavelengths,
            )]),
            AntennaSelection::Only(baselines),
            IdSelection::Only(scans),
            IdSelection::Only(observations),
            IntentSelection::Only(intents),
            IdSelection::Only(arrays),
        ),
        data_descriptions,
        spectral_windows,
        correlations,
    )
}

fn source(
    source_id: u8,
    row_digest: u8,
    generation_seed: u8,
    locator: &str,
    reverse: bool,
) -> ObservationSourceInput {
    source_with_model_column(
        source_id,
        row_digest,
        generation_seed,
        locator,
        reverse,
        ModelColumnState::Absent,
    )
}

fn source_with_model_column(
    source_id: u8,
    row_digest: u8,
    generation_seed: u8,
    locator: &str,
    reverse: bool,
    model_column: ModelColumnState,
) -> ObservationSourceInput {
    ObservationSourceInput::new(
        MeasurementSetIdentity::new(identity(source_id)),
        ObservationSourceProvenance::new(locator.to_string(), identity(source_id + 40)),
        selection(row_digest, reverse),
        SourceGenerations::new(
            ConsistencyToken::new(identity(source_id + 50)),
            columns(generation_seed, reverse),
            metadata(generation_seed + 24, reverse),
            model_column,
        ),
    )
}

fn snapshot(reverse: bool, left_locator: &str, right_locator: &str) -> ObservationSnapshot {
    let mut sources = vec![
        source(11, 31, 60, left_locator, reverse),
        source(12, 32, 100, right_locator, reverse),
    ];
    let mut references = vec![
        (ReferenceDataKind::Observatory, identity(201)),
        (ReferenceDataKind::Ephemeris, identity(202)),
        (ReferenceDataKind::Measures, identity(203)),
    ];
    if reverse {
        sources.reverse();
        references.reverse();
    }
    compile_observation(ObservationSnapshotInput::new(
        sources,
        references,
        ModelStateIdentity::Seed(identity(204)),
    ))
    .expect("compile observation snapshot")
}

#[test]
fn all_defined_measurement_set_correlation_coordinates_are_lossless() {
    let correlation_types = [
        CorrelationType::StokesI,
        CorrelationType::StokesQ,
        CorrelationType::StokesU,
        CorrelationType::StokesV,
        CorrelationType::CircularRr,
        CorrelationType::CircularRl,
        CorrelationType::CircularLr,
        CorrelationType::CircularLl,
        CorrelationType::LinearXx,
        CorrelationType::LinearXy,
        CorrelationType::LinearYx,
        CorrelationType::LinearYy,
        CorrelationType::MixedRx,
        CorrelationType::MixedRy,
        CorrelationType::MixedLx,
        CorrelationType::MixedLy,
        CorrelationType::MixedXr,
        CorrelationType::MixedXl,
        CorrelationType::MixedYr,
        CorrelationType::MixedYl,
        CorrelationType::QuasiOrthogonalPp,
        CorrelationType::QuasiOrthogonalPq,
        CorrelationType::QuasiOrthogonalQp,
        CorrelationType::QuasiOrthogonalQq,
        CorrelationType::RightCircular,
        CorrelationType::LeftCircular,
        CorrelationType::Linear,
        CorrelationType::PolarizedIntensity,
        CorrelationType::LinearPolarizedIntensity,
        CorrelationType::FractionalPolarizedIntensity,
        CorrelationType::FractionalLinearPolarizedIntensity,
        CorrelationType::PolarizationAngle,
    ];
    let base = selection(31, false);
    let exact_selection = ObservationSelection::new(
        base.rows().clone(),
        base.rows_filter().clone(),
        base.data_descriptions()
            .iter()
            .map(|entry| {
                DataDescriptionSelection::new(
                    entry.data_description_id(),
                    entry.spectral_window_id(),
                    7,
                )
            })
            .collect(),
        base.spectral_windows().to_vec(),
        vec![CorrelationSelection::new(
            7,
            correlation_types
                .iter()
                .enumerate()
                .map(|(index, correlation)| CorrelationProduct::new(index as u32, *correlation))
                .collect(),
        )],
    );
    let compiled = compile_observation(ObservationSnapshotInput::new(
        vec![ObservationSourceInput::new(
            MeasurementSetIdentity::new(identity(11)),
            ObservationSourceProvenance::new(
                "/archive/all-correlations.ms".to_string(),
                identity(51),
            ),
            exact_selection,
            SourceGenerations::new(
                ConsistencyToken::new(identity(61)),
                columns(60, false),
                metadata(84, false),
                ModelColumnState::Absent,
            ),
        )],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile all MeasurementSet correlation coordinates");

    let compiled_types = compiled.sources()[0].selection().correlations()[0]
        .products()
        .iter()
        .map(|product| product.correlation_type())
        .collect::<Vec<_>>();
    assert_eq!(compiled_types, correlation_types);
}

#[test]
fn data_description_catalog_binds_spw_and_polarization_pairing() {
    let compile = |mut data_descriptions: Vec<DataDescriptionSelection>| {
        let base = selection(31, false);
        let selected = ObservationSelection::new(
            base.rows().clone(),
            base.rows_filter().clone(),
            data_descriptions.clone(),
            vec![
                SpectralWindowSelection::new(2, vec![0, 4, 8]),
                SpectralWindowSelection::new(9, vec![3, 5, 7]),
            ],
            vec![
                CorrelationSelection::new(
                    1,
                    vec![
                        CorrelationProduct::new(0, CorrelationType::CircularRr),
                        CorrelationProduct::new(1, CorrelationType::CircularLl),
                    ],
                ),
                CorrelationSelection::new(
                    5,
                    vec![
                        CorrelationProduct::new(0, CorrelationType::LinearXx),
                        CorrelationProduct::new(1, CorrelationType::LinearYy),
                    ],
                ),
            ],
        );
        data_descriptions.sort_unstable_by_key(|entry| entry.data_description_id());
        let snapshot = compile_observation(ObservationSnapshotInput::new(
            vec![ObservationSourceInput::new(
                MeasurementSetIdentity::new(identity(11)),
                ObservationSourceProvenance::new(
                    "/archive/data-description.ms".to_string(),
                    identity(51),
                ),
                selected,
                SourceGenerations::new(
                    ConsistencyToken::new(identity(61)),
                    columns(60, false),
                    metadata(84, false),
                    ModelColumnState::Absent,
                ),
            )],
            Vec::new(),
            ModelStateIdentity::Empty,
        ))
        .expect("compile exact DATA_DESCRIPTION catalog");
        (snapshot, data_descriptions)
    };

    let canonical = vec![
        DataDescriptionSelection::new(1, 2, 1),
        DataDescriptionSelection::new(4, 2, 5),
        DataDescriptionSelection::new(6, 9, 5),
    ];
    let mut reversed = canonical.clone();
    reversed.reverse();
    let swapped = vec![
        DataDescriptionSelection::new(1, 2, 5),
        DataDescriptionSelection::new(4, 2, 5),
        DataDescriptionSelection::new(6, 9, 1),
    ];

    let (expected, expected_catalog) = compile(canonical);
    let (reordered, _) = compile(reversed);
    let (different_pairing, _) = compile(swapped);

    assert_eq!(
        expected.sources()[0].selection().data_descriptions(),
        expected_catalog
    );
    assert_eq!(expected.snapshot_id(), reordered.snapshot_id());
    assert_ne!(expected.snapshot_id(), different_pairing.snapshot_id());
    assert_eq!(ObservationSnapshotId::SCHEMA_VERSION, 4);
    assert_eq!(
        expected.snapshot_id().as_bytes(),
        [
            254, 192, 119, 69, 16, 184, 111, 194, 219, 159, 152, 108, 97, 127, 136, 119, 201, 109,
            10, 97, 239, 111, 199, 28, 218, 236, 219, 202, 131, 81, 136, 223,
        ]
    );
}

#[test]
fn data_description_catalog_rejects_duplicate_ddid() {
    let base = selection(31, false);
    let mut data_descriptions = base.data_descriptions().to_vec();
    data_descriptions.push(DataDescriptionSelection::new(1, 2, 1));
    let invalid = ObservationSelection::new(
        base.rows().clone(),
        base.rows_filter().clone(),
        data_descriptions,
        base.spectral_windows().to_vec(),
        base.correlations().to_vec(),
    );

    assert_eq!(
        compile_observation(ObservationSnapshotInput::new(
            vec![ObservationSourceInput::new(
                MeasurementSetIdentity::new(identity(11)),
                ObservationSourceProvenance::new(
                    "/archive/duplicate-ddid.ms".to_string(),
                    identity(51),
                ),
                invalid,
                SourceGenerations::new(
                    ConsistencyToken::new(identity(61)),
                    columns(60, false),
                    metadata(84, false),
                    ModelColumnState::Absent,
                ),
            )],
            Vec::new(),
            ModelStateIdentity::Empty,
        )),
        Err(CompileObservationError::DuplicateDataDescription {
            data_description_id: 1,
        })
    );
}

#[test]
fn selected_main_row_manifest_must_reference_the_compiled_catalog() {
    let base = selection(31, false);
    let invalid = ObservationSelection::new(
        SelectedRows::from_ordered_main_rows(100, [SelectedMainRow::new(0, 99)])
            .expect("well-formed but catalog-inconsistent MAIN row manifest"),
        base.rows_filter().clone(),
        base.data_descriptions().to_vec(),
        base.spectral_windows().to_vec(),
        base.correlations().to_vec(),
    );

    assert!(matches!(
        compile_observation(ObservationSnapshotInput::new(
            vec![ObservationSourceInput::new(
                MeasurementSetIdentity::new(identity(11)),
                ObservationSourceProvenance::new(
                    "/archive/inconsistent-row-ddid.ms".to_string(),
                    identity(51),
                ),
                invalid,
                SourceGenerations::new(
                    ConsistencyToken::new(identity(61)),
                    columns(60, false),
                    metadata(84, false),
                    ModelColumnState::Absent,
                ),
            )],
            Vec::new(),
            ModelStateIdentity::Empty,
        )),
        Err(CompileObservationError::SelectedRowDataDescriptionMissing {
            data_description_id: 99,
        })
    ));
}

#[test]
fn data_description_catalog_rejects_missing_and_unresolved_joins() {
    let base = selection(31, false);
    let compile = |data_descriptions| {
        compile_observation(ObservationSnapshotInput::new(
            vec![ObservationSourceInput::new(
                MeasurementSetIdentity::new(identity(11)),
                ObservationSourceProvenance::new(
                    "/archive/inexact-data-description.ms".to_string(),
                    identity(51),
                ),
                ObservationSelection::new(
                    base.rows().clone(),
                    base.rows_filter().clone(),
                    data_descriptions,
                    base.spectral_windows().to_vec(),
                    base.correlations().to_vec(),
                ),
                SourceGenerations::new(
                    ConsistencyToken::new(identity(61)),
                    columns(60, false),
                    metadata(84, false),
                    ModelColumnState::Absent,
                ),
            )],
            Vec::new(),
            ModelStateIdentity::Empty,
        ))
    };

    assert_eq!(
        compile(Vec::new()),
        Err(CompileObservationError::NoDataDescriptionSelection)
    );
    assert_eq!(
        compile(vec![DataDescriptionSelection::new(
            i32::MAX as u32 + 1,
            2,
            1,
        )]),
        Err(
            CompileObservationError::DataDescriptionIdOutsideMainDomain {
                data_description_id: i32::MAX as u32 + 1,
            }
        )
    );
    assert_eq!(
        compile(vec![DataDescriptionSelection::new(1, 99, 1)]),
        Err(
            CompileObservationError::UnknownDataDescriptionSpectralWindow {
                data_description_id: 1,
                spectral_window_id: 99,
            }
        )
    );
    assert_eq!(
        compile(vec![DataDescriptionSelection::new(1, 2, 99)]),
        Err(
            CompileObservationError::UnknownDataDescriptionPolarization {
                data_description_id: 1,
                polarization_id: 99,
            }
        )
    );
    assert_eq!(
        compile(vec![
            DataDescriptionSelection::new(1, 2, 1),
            DataDescriptionSelection::new(4, 2, 5),
        ]),
        Err(CompileObservationError::OrphanSpectralWindowSelection {
            spectral_window_id: 9,
        })
    );
    assert_eq!(
        compile(vec![
            DataDescriptionSelection::new(1, 2, 1),
            DataDescriptionSelection::new(4, 2, 1),
            DataDescriptionSelection::new(6, 9, 1),
        ]),
        Err(CompileObservationError::OrphanCorrelationSelection { polarization_id: 5 })
    );
}

#[test]
fn model_column_state_is_snapshot_identity_bearing() {
    let compile = |state| {
        compile_observation(ObservationSnapshotInput::new(
            vec![source_with_model_column(
                11,
                31,
                60,
                "/archive/a.ms",
                false,
                state,
            )],
            Vec::new(),
            ModelStateIdentity::Empty,
        ))
        .expect("compile explicit MODEL_DATA state")
    };

    let absent = compile(ModelColumnState::Absent);
    let present = compile(ModelColumnState::Present(identity(210)));

    assert_ne!(absent.snapshot_id(), present.snapshot_id());
    assert_eq!(
        present.sources()[0].generations().model_column(),
        ModelColumnState::Present(identity(210))
    );
}

#[test]
fn model_column_state_must_match_a_consumed_model_generation() {
    let mut selected = columns(60, false).generations().to_vec();
    selected.push(ColumnGeneration::new(
        MsColumnKind::ModelData,
        identity(210),
    ));
    let source = ObservationSourceInput::new(
        MeasurementSetIdentity::new(identity(11)),
        ObservationSourceProvenance::new("/archive/a.ms".to_string(), identity(51)),
        selection(31, false),
        SourceGenerations::new(
            ConsistencyToken::new(identity(61)),
            SelectedColumns::new(
                VisibilityColumn::CorrectedData,
                FlagPolicy::FlagOrFlagRow,
                WeightColumn::WeightSpectrum,
                selected,
            ),
            metadata(84, false),
            ModelColumnState::Present(identity(211)),
        ),
    );

    assert_eq!(
        compile_observation(ObservationSnapshotInput::new(
            vec![source],
            Vec::new(),
            ModelStateIdentity::Empty,
        )),
        Err(CompileObservationError::InconsistentModelColumnState)
    );
}

#[test]
fn content_identity_is_canonical_but_provenance_retains_origin_and_request_order() {
    let first = snapshot(false, "/archive/a.ms", "/archive/b.ms");
    let reordered = snapshot(true, "/mirror/a.ms", "/mirror/b.ms");

    assert_eq!(first.snapshot_id(), reordered.snapshot_id());
    assert_ne!(first.provenance_id(), reordered.provenance_id());
    assert_eq!(
        first.sources()[0].identity(),
        MeasurementSetIdentity::new(identity(11))
    );
    assert_eq!(
        first.sources()[1].identity(),
        MeasurementSetIdentity::new(identity(12))
    );
    assert_eq!(first.sources()[0].input_ordinal(), 0);
    assert_eq!(reordered.sources()[0].input_ordinal(), 1);
    assert_eq!(casa_imaging_model::ObservationSnapshotId::SCHEMA_VERSION, 4);
}

#[test]
fn snapshot_exposes_exact_selection_and_generation_semantics_without_bulk_samples() {
    let compiled = snapshot(false, "/archive/a.ms", "/archive/b.ms");
    let source = &compiled.sources()[0];
    let selection = source.selection();

    assert_eq!(selection.rows().source_row_count(), 100);
    assert_eq!(selection.rows().selected_row_count(), 11);
    assert_eq!(
        selection.rows().sequence_id(),
        selected_rows(31).sequence_id()
    );
    assert_eq!(selection.rows_filter().fields().ids(), Some(&[2, 7][..]));
    assert_eq!(selection.rows_filter().scans().ids(), Some(&[4, 12][..]));
    assert_eq!(
        selection.rows_filter().times(),
        &TimeSelection::Ranges(vec![TimeRange::new(
            Some(SelectionBound::inclusive(5_000_000_000.0)),
            Some(SelectionBound::exclusive(5_000_000_010.0)),
        )])
    );
    assert_eq!(
        selection.rows_filter().uv_distances(),
        &UvSelection::Ranges(vec![UvDistanceRange::new(
            Some(SelectionBound::inclusive(10.0)),
            Some(SelectionBound::inclusive(1_000.0)),
            UvDistanceUnit::Wavelengths,
        )])
    );
    assert_eq!(
        selection.rows_filter().antennas(),
        &AntennaSelection::Only(vec![AntennaBaseline::new(1, 5), AntennaBaseline::new(2, 4),])
    );
    assert_eq!(
        selection.rows_filter().observations().ids(),
        Some(&[1, 3][..])
    );
    assert_eq!(selection.rows_filter().arrays().ids(), Some(&[0, 2][..]));
    assert_eq!(
        selection.rows_filter().intents(),
        &IntentSelection::Only(vec![
            ResolvedIntent::new(3, "CALIBRATE_PHASE#ON_SOURCE".to_string()),
            ResolvedIntent::new(8, "OBSERVE_TARGET#ON_SOURCE".to_string()),
        ])
    );
    assert_eq!(
        selection.data_descriptions(),
        &[
            DataDescriptionSelection::new(1, 2, 1),
            DataDescriptionSelection::new(4, 2, 5),
            DataDescriptionSelection::new(6, 9, 5),
        ]
    );
    assert_eq!(selection.spectral_windows()[0].spectral_window_id(), 2);
    assert_eq!(
        selection.spectral_windows()[0].channel_indices(),
        &[0, 4, 8]
    );
    assert_eq!(selection.correlations()[0].polarization_id(), 1);
    assert_eq!(
        selection.correlations()[0].products()[0],
        CorrelationProduct::new(0, CorrelationType::CircularRr)
    );

    let columns = source.generations().columns();
    assert_eq!(columns.visibility(), VisibilityColumn::CorrectedData);
    assert_eq!(columns.flags(), FlagPolicy::FlagOrFlagRow);
    assert_eq!(columns.weights(), WeightColumn::WeightSpectrum);
    assert!(columns.generation(MsColumnKind::Flag).is_some());
    assert!(columns.generation(MsColumnKind::FlagRow).is_some());
    assert!(columns.generation(MsColumnKind::Uvw).is_some());
    assert!(
        source
            .generations()
            .metadata(MetadataTableKind::Feed)
            .is_some()
    );
    assert!(
        source
            .generations()
            .metadata(MetadataTableKind::Antenna)
            .is_some()
    );
    assert!(
        source
            .generations()
            .metadata(MetadataTableKind::Pointing)
            .is_some()
    );
    assert_eq!(compiled.reference_data().len(), 3);
    assert_eq!(compiled.model(), ModelStateIdentity::Seed(identity(204)));
}

#[test]
fn compilation_fails_closed_on_incomplete_or_ambiguous_manifests() {
    let incomplete_columns = SelectedColumns::new(
        VisibilityColumn::CorrectedData,
        FlagPolicy::FlagOrFlagRow,
        WeightColumn::WeightSpectrum,
        columns(60, false)
            .generations()
            .iter()
            .filter(|generation| generation.kind() != MsColumnKind::Flag)
            .copied()
            .collect(),
    );
    let invalid_source = ObservationSourceInput::new(
        MeasurementSetIdentity::new(identity(11)),
        ObservationSourceProvenance::new("/archive/a.ms".to_string(), identity(51)),
        selection(31, false),
        SourceGenerations::new(
            ConsistencyToken::new(identity(61)),
            incomplete_columns,
            metadata(84, false),
            ModelColumnState::Absent,
        ),
    );

    assert!(matches!(
        compile_observation(ObservationSnapshotInput::new(
            vec![invalid_source],
            vec![],
            ModelStateIdentity::Empty,
        )),
        Err(CompileObservationError::MissingColumnGeneration {
            column: MsColumnKind::Flag,
            ..
        })
    ));

    let empty_source = ObservationSourceInput::new(
        MeasurementSetIdentity::new(identity(11)),
        ObservationSourceProvenance::new("/archive/a.ms".to_string(), identity(51)),
        ObservationSelection::new(
            SelectedRows::from_ordered_main_rows(100, main_rows([]))
                .expect("empty source row selection"),
            selection(31, false).rows_filter().clone(),
            selection(31, false).data_descriptions().to_vec(),
            selection(31, false).spectral_windows().to_vec(),
            selection(31, false).correlations().to_vec(),
        ),
        SourceGenerations::new(
            ConsistencyToken::new(identity(61)),
            columns(60, false),
            metadata(84, false),
            ModelColumnState::Absent,
        ),
    );
    assert!(matches!(
        compile_observation(ObservationSnapshotInput::new(
            vec![empty_source],
            vec![],
            ModelStateIdentity::Empty,
        )),
        Err(CompileObservationError::EmptySelection)
    ));
}

#[test]
fn consistency_validation_attributes_disallowed_input_mutation() {
    let compiled = snapshot(false, "/archive/a.ms", "/archive/b.ms");
    let current_sources = compiled
        .sources()
        .iter()
        .map(|source| {
            ObservationSourceState::new(
                source.identity(),
                source.selection().rows().clone(),
                source.generations().clone(),
            )
        })
        .collect::<Vec<_>>();
    let current = ObservationState::new(
        current_sources.clone(),
        compiled.reference_data().to_vec(),
        compiled.model(),
    );
    compiled
        .validate_consistency(current)
        .expect("unchanged inputs remain valid");

    let mut changed_sources = current_sources.clone();
    changed_sources[0] = ObservationSourceState::new(
        changed_sources[0].identity(),
        selected_rows(249),
        changed_sources[0].generations().clone(),
    );
    assert!(matches!(
        compiled.validate_consistency(ObservationState::new(
            changed_sources,
            compiled.reference_data().to_vec(),
            compiled.model(),
        )),
        Err(ObservationConsistencyError::SelectedRowsChanged { .. })
    ));

    let changed_columns = SelectedColumns::new(
        VisibilityColumn::CorrectedData,
        FlagPolicy::FlagOrFlagRow,
        WeightColumn::WeightSpectrum,
        columns(60, false)
            .generations()
            .iter()
            .map(|generation| {
                if generation.kind() == MsColumnKind::Flag {
                    ColumnGeneration::new(MsColumnKind::Flag, identity(250))
                } else {
                    *generation
                }
            })
            .collect(),
    );
    let mut changed_sources = current_sources.clone();
    changed_sources[0] = ObservationSourceState::new(
        changed_sources[0].identity(),
        changed_sources[0].selected_rows().clone(),
        SourceGenerations::new(
            changed_sources[0].generations().consistency_token(),
            changed_columns,
            changed_sources[0]
                .generations()
                .metadata_generations()
                .to_vec(),
            changed_sources[0].generations().model_column(),
        ),
    );
    assert!(matches!(
        compiled.validate_consistency(ObservationState::new(
            changed_sources,
            compiled.reference_data().to_vec(),
            compiled.model(),
        )),
        Err(ObservationConsistencyError::ColumnGenerationChanged {
            column: MsColumnKind::Flag,
            ..
        })
    ));

    let mut changed_sources = current_sources.clone();
    changed_sources[0] = ObservationSourceState::new(
        changed_sources[0].identity(),
        changed_sources[0].selected_rows().clone(),
        SourceGenerations::new(
            changed_sources[0].generations().consistency_token(),
            changed_sources[0].generations().columns().clone(),
            changed_sources[0]
                .generations()
                .metadata_generations()
                .to_vec(),
            ModelColumnState::Present(identity(253)),
        ),
    );
    assert!(matches!(
        compiled.validate_consistency(ObservationState::new(
            changed_sources,
            compiled.reference_data().to_vec(),
            compiled.model(),
        )),
        Err(ObservationConsistencyError::ModelColumnStateChanged { .. })
    ));

    let mut changed_sources = current_sources.clone();
    let changed_metadata = changed_sources[0]
        .generations()
        .metadata_generations()
        .iter()
        .map(|generation| {
            if generation.kind() == MetadataTableKind::Field {
                MetadataGeneration::new(MetadataTableKind::Field, identity(250))
            } else {
                *generation
            }
        })
        .collect();
    changed_sources[0] = ObservationSourceState::new(
        changed_sources[0].identity(),
        changed_sources[0].selected_rows().clone(),
        SourceGenerations::new(
            changed_sources[0].generations().consistency_token(),
            changed_sources[0].generations().columns().clone(),
            changed_metadata,
            changed_sources[0].generations().model_column(),
        ),
    );
    assert!(matches!(
        compiled.validate_consistency(ObservationState::new(
            changed_sources,
            compiled.reference_data().to_vec(),
            compiled.model(),
        )),
        Err(ObservationConsistencyError::MetadataGenerationChanged {
            table: MetadataTableKind::Field,
            ..
        })
    ));

    let mut changed_sources = current_sources.clone();
    changed_sources[0] = ObservationSourceState::new(
        changed_sources[0].identity(),
        changed_sources[0].selected_rows().clone(),
        SourceGenerations::new(
            ConsistencyToken::new(identity(251)),
            changed_sources[0].generations().columns().clone(),
            changed_sources[0]
                .generations()
                .metadata_generations()
                .to_vec(),
            changed_sources[0].generations().model_column(),
        ),
    );
    assert!(matches!(
        compiled.validate_consistency(ObservationState::new(
            changed_sources,
            compiled.reference_data().to_vec(),
            compiled.model(),
        )),
        Err(ObservationConsistencyError::ConsistencyTokenChanged { .. })
    ));

    let mut changed_references = compiled.reference_data().to_vec();
    changed_references[0].1 = identity(252);
    assert_eq!(
        compiled.validate_consistency(ObservationState::new(
            current_sources.clone(),
            changed_references,
            compiled.model(),
        )),
        Err(ObservationConsistencyError::ReferenceDataChanged)
    );
    assert_eq!(
        compiled.validate_consistency(ObservationState::new(
            current_sources.clone(),
            compiled.reference_data().to_vec(),
            ModelStateIdentity::Generation(identity(253)),
        )),
        Err(ObservationConsistencyError::ModelStateChanged)
    );
    assert_eq!(
        compiled.validate_consistency(ObservationState::new(
            current_sources[..1].to_vec(),
            compiled.reference_data().to_vec(),
            compiled.model(),
        )),
        Err(ObservationConsistencyError::SourceSetChanged)
    );
}
