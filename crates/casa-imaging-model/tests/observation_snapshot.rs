// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AntennaBaseline, AntennaSelection, ColumnGeneration, CompileObservationError, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, FlagPolicy, IdSelection,
    IntentSelection, LogicalIdentity, MeasurementSetIdentity, MetadataGeneration,
    MetadataTableKind, ModelColumnState, ModelStateIdentity, MsColumnKind,
    ObservationConsistencyError, ObservationSelection, ObservationSnapshot,
    ObservationSnapshotInput, ObservationSourceInput, ObservationSourceProvenance,
    ObservationSourceState, ObservationState, ReferenceDataKind, ResolvedIntent, RowSelection,
    SelectedColumns, SelectedRows, SelectionBound, SourceGenerations, SpectralWindowSelection,
    TimeRange, TimeSelection, UvDistanceRange, UvDistanceUnit, UvSelection, VisibilityColumn,
    WeightColumn, compile_observation,
};

fn identity(byte: u8) -> LogicalIdentity {
    LogicalIdentity::from_sha256([byte; 32])
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
    let mut spectral_windows = vec![
        SpectralWindowSelection::new(9, vec![6], vec![7, 5, 3]),
        SpectralWindowSelection::new(2, vec![4, 1], vec![8, 4, 0]),
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
        spectral_windows.reverse();
        correlations.reverse();
    }

    ObservationSelection::new(
        SelectedRows::new(100, 11, identity(row_digest)),
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
    assert_eq!(casa_imaging_model::ObservationSnapshotId::SCHEMA_VERSION, 2);
}

#[test]
fn snapshot_exposes_exact_selection_and_generation_semantics_without_bulk_samples() {
    let compiled = snapshot(false, "/archive/a.ms", "/archive/b.ms");
    let source = &compiled.sources()[0];
    let selection = source.selection();

    assert_eq!(selection.rows().source_row_count(), 100);
    assert_eq!(selection.rows().selected_row_count(), 11);
    assert_eq!(selection.rows().canonical_sequence_identity(), identity(31));
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
    assert_eq!(selection.spectral_windows()[0].spectral_window_id(), 2);
    assert_eq!(
        selection.spectral_windows()[0].data_description_ids(),
        &[1, 4]
    );
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
            SelectedRows::new(100, 0, identity(31)),
            selection(31, false).rows_filter().clone(),
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
        SelectedRows::new(100, 11, identity(249)),
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
