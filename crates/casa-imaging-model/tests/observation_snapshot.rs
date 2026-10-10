// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AntennaBaseline, AntennaSelection, CompileObservationError, CorrelationProduct,
    CorrelationSelection, CorrelationType, DataDescriptionSelection, FlagPolicy, IdSelection,
    IntentSelection, LogicalIdentity, ModelStateIdentity, ObservationSelection,
    ObservationSnapshot, ObservationSnapshotId, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, ReferenceDataKind, ResolvedIntent, RowSelection, SelectedColumns,
    SelectedMainRow, SelectedRowManifestValidationError, SelectedRowSequenceError,
    SelectedRowSequenceId, SelectedRows, SelectionBound, SpectralWindowCoordinateCatalog,
    SpectralWindowSelection, TimeRange, TimeSelection, UvDistanceRange, UvDistanceUnit,
    UvSelection, VisibilityColumn, WeightColumn, compile_observation,
};

fn identity(byte: u8) -> LogicalIdentity {
    LogicalIdentity::from_bytes([byte; 32])
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
    planned
        .validate_ordered_main_rows(
            main_rows([0, 3, 7, 11])
                .into_iter()
                .map(Ok::<_, std::io::Error>),
        )
        .expect("compact manifest validates the same storage replay");
    assert_eq!(empty.selected_row_count(), 0);
    assert_eq!(empty.sequence_id(), empty_larger_source.sequence_id());
    assert_ne!(empty, empty_larger_source);
    assert_eq!(SelectedRowSequenceId::SCHEMA_VERSION, 3);
    assert_eq!(
        planned.sequence_id().as_bytes(),
        [
            107, 25, 169, 178, 186, 205, 209, 116, 41, 37, 11, 89, 11, 165, 218, 52, 157, 86, 239,
            193, 78, 29, 84, 0, 20, 80, 199, 182, 11, 227, 95, 183,
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
    assert_eq!(SelectedRowSequenceId::SCHEMA_VERSION, 3);
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

fn columns() -> SelectedColumns {
    SelectedColumns::new(
        VisibilityColumn::CorrectedData,
        FlagPolicy::FlagOrFlagRow,
        WeightColumn::WeightSpectrum,
    )
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

fn source(source_id: u8, row_digest: u8, locator: &str, reverse: bool) -> ObservationSourceInput {
    ObservationSourceInput::new(
        ObservationSourceProvenance::new(locator.to_string(), identity(source_id + 40)),
        selection(row_digest, reverse),
        columns(),
        false,
    )
}

fn snapshot(reverse: bool, left_locator: &str, right_locator: &str) -> ObservationSnapshot {
    // Sources keep request order; `reverse` reorders only what compilation
    // canonicalizes.
    let sources = vec![
        source(11, 31, left_locator, reverse),
        source(12, 32, right_locator, reverse),
    ];
    let mut references = vec![
        (ReferenceDataKind::Observatory, identity(201)),
        (ReferenceDataKind::Ephemeris, identity(202)),
        (ReferenceDataKind::Measures, identity(203)),
    ];
    if reverse {
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
            ObservationSourceProvenance::new(
                "/archive/all-correlations.ms".to_string(),
                identity(51),
            ),
            exact_selection,
            columns(),
            false,
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
                ObservationSourceProvenance::new(
                    "/archive/data-description.ms".to_string(),
                    identity(51),
                ),
                selected,
                columns(),
                false,
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
    assert_eq!(ObservationSnapshotId::SCHEMA_VERSION, 6);
    assert_eq!(
        expected.snapshot_id().as_bytes(),
        [
            11, 113, 28, 252, 184, 105, 157, 61, 36, 246, 243, 167, 120, 60, 167, 182, 139, 81,
            176, 173, 82, 171, 15, 33, 252, 150, 64, 3, 214, 236, 213, 35,
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
                ObservationSourceProvenance::new(
                    "/archive/duplicate-ddid.ms".to_string(),
                    identity(51),
                ),
                invalid,
                columns(),
                false,
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
fn full_spectral_coordinate_catalog_is_exact_identity_bearing_selection_state() {
    let compile = |first_frequency_hz| {
        let base = selection(31, false);
        let spectral_windows = base
            .spectral_windows()
            .iter()
            .cloned()
            .map(|spectral_window| {
                if spectral_window.spectral_window_id() != 2 {
                    return spectral_window;
                }
                spectral_window.with_coordinate_catalog(
                    SpectralWindowCoordinateCatalog::new(
                        vec![
                            first_frequency_hz,
                            1.001e9,
                            1.003e9,
                            1.006e9,
                            1.010e9,
                            1.015e9,
                            1.021e9,
                            1.028e9,
                            1.036e9,
                        ],
                        -1.25e6,
                    )
                    .expect("valid nonuniform catalog"),
                )
            })
            .collect();
        let exact = ObservationSelection::new(
            base.rows().clone(),
            base.rows_filter().clone(),
            base.data_descriptions().to_vec(),
            spectral_windows,
            base.correlations().to_vec(),
        );
        compile_observation(ObservationSnapshotInput::new(
            vec![ObservationSourceInput::new(
                ObservationSourceProvenance::new("/archive/full-spw.ms".to_string(), identity(51)),
                exact,
                columns(),
                false,
            )],
            Vec::new(),
            ModelStateIdentity::Empty,
        ))
        .expect("compile exact spectral coordinate catalog")
    };

    let first = compile(1.0e9);
    let changed_unselected_coordinate = compile(1.000_1e9);
    let catalog = first.sources()[0].selection().spectral_windows()[0]
        .coordinate_catalog()
        .expect("catalog is retained");
    assert_eq!(
        catalog.channel_frequencies_hz(),
        &[
            1.0e9, 1.001e9, 1.003e9, 1.006e9, 1.010e9, 1.015e9, 1.021e9, 1.028e9, 1.036e9,
        ]
    );
    assert_eq!(catalog.first_channel_width_hz(), -1.25e6);
    assert_ne!(
        first.snapshot_id(),
        changed_unselected_coordinate.snapshot_id(),
        "an unselected physical coordinate changes CASA mosaic response planning"
    );
}

#[test]
fn selected_channel_must_exist_in_its_full_coordinate_catalog() {
    let base = selection(31, false);
    let spectral_windows = base
        .spectral_windows()
        .iter()
        .cloned()
        .map(|spectral_window| {
            if spectral_window.spectral_window_id() != 2 {
                return spectral_window;
            }
            spectral_window.with_coordinate_catalog(
                SpectralWindowCoordinateCatalog::new(vec![1.0e9; 8], 1.0e6)
                    .expect("valid but too-short catalog"),
            )
        })
        .collect();
    let invalid = ObservationSelection::new(
        base.rows().clone(),
        base.rows_filter().clone(),
        base.data_descriptions().to_vec(),
        spectral_windows,
        base.correlations().to_vec(),
    );

    assert!(matches!(
        compile_observation(ObservationSnapshotInput::new(
            vec![ObservationSourceInput::new(
                ObservationSourceProvenance::new("/archive/short-spw.ms".to_string(), identity(51),),
                invalid,
                columns(),
                false,
            )],
            Vec::new(),
            ModelStateIdentity::Empty,
        )),
        Err(
            CompileObservationError::SpectralWindowCoordinateCatalogMismatch {
                spectral_window_id: 2
            }
        )
    ));
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
                ObservationSourceProvenance::new(
                    "/archive/inconsistent-row-ddid.ms".to_string(),
                    identity(51),
                ),
                invalid,
                columns(),
                false,
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
                columns(),
                false,
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
fn corrected_data_presence_is_snapshot_identity_bearing() {
    let compile = |corrected_data_present| {
        compile_observation(ObservationSnapshotInput::new(
            vec![ObservationSourceInput::new(
                ObservationSourceProvenance::new("/archive/a.ms".to_string(), identity(51)),
                selection(31, false),
                columns(),
                corrected_data_present,
            )],
            Vec::new(),
            ModelStateIdentity::Empty,
        ))
        .expect("compile explicit CORRECTED_DATA presence")
    };

    let absent = compile(false);
    let present = compile(true);

    assert_ne!(absent.snapshot_id(), present.snapshot_id());
    assert!(present.sources()[0].corrected_data_present());
}

#[test]
fn content_identity_is_canonical_but_provenance_retains_origin_and_request_order() {
    let first = snapshot(false, "/archive/a.ms", "/archive/b.ms");
    let reordered = snapshot(true, "/mirror/a.ms", "/mirror/b.ms");

    assert_eq!(first.snapshot_id(), reordered.snapshot_id());
    assert_ne!(first.provenance_id(), reordered.provenance_id());
    for snapshot in [&first, &reordered] {
        assert_eq!(snapshot.sources()[0].input_ordinal(), 0);
        assert_eq!(snapshot.sources()[1].input_ordinal(), 1);
    }
    assert_eq!(
        first.sources()[1].provenance().locator(),
        "/archive/b.ms",
        "sources keep their request order"
    );
    assert_eq!(casa_imaging_model::ObservationSnapshotId::SCHEMA_VERSION, 6);
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

    let columns = source.columns();
    assert_eq!(columns.visibility(), VisibilityColumn::CorrectedData);
    assert_eq!(columns.flags(), FlagPolicy::FlagOrFlagRow);
    assert_eq!(columns.weights(), WeightColumn::WeightSpectrum);
    assert_eq!(compiled.reference_data().len(), 3);
    assert_eq!(compiled.model(), ModelStateIdentity::Seed(identity(204)));
}

#[test]
fn compilation_fails_closed_on_an_empty_selection() {
    let empty_source = ObservationSourceInput::new(
        ObservationSourceProvenance::new("/archive/a.ms".to_string(), identity(51)),
        ObservationSelection::new(
            SelectedRows::from_ordered_main_rows(100, main_rows([]))
                .expect("empty source row selection"),
            selection(31, false).rows_filter().clone(),
            selection(31, false).data_descriptions().to_vec(),
            selection(31, false).spectral_windows().to_vec(),
            selection(31, false).correlations().to_vec(),
        ),
        columns(),
        false,
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
