// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CompileObservationError, CorrelationProduct, CorrelationSelection, CorrelationType,
    DataDescriptionSelection, FlagPolicy, IdSelection, IntentSelection, ObservationSelection,
    ObservationSnapshot, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, ResolvedIntent, RowSelection, SelectedColumns, SelectedMainRow,
    SelectedRowSequenceError, SelectedRows, SelectionBound, SpectralWindowCoordinateCatalog,
    SpectralWindowSelection, UvDistanceRange, UvDistanceUnit, UvSelection, VisibilityColumn,
    WeightColumn, compile_observation,
};

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

#[test]
fn selected_row_sequence_manifest_is_storage_owner_reproducible() {
    let planned = SelectedRows::from_ordered_main_rows(12, main_rows([0, 3, 7, 11]))
        .expect("compiler-selected physical MAIN rows");
    let reopened = SelectedRows::from_ordered_main_rows(12, main_rows([0, 3, 7, 11]))
        .expect("storage owner re-resolved the same physical MAIN rows");
    let empty = SelectedRows::from_ordered_main_rows(12, main_rows([]))
        .expect("one source may contribute no selected rows");
    let empty_larger_source = SelectedRows::from_ordered_main_rows(20, main_rows([]))
        .expect("an empty selection still retains its source row count");

    assert_eq!(planned, reopened);
    assert_eq!(planned.source_row_count(), 12);
    assert_eq!(planned.selected_row_count(), 4);
    assert_eq!(empty.selected_row_count(), 0);
    assert_ne!(empty, empty_larger_source);
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

fn columns() -> SelectedColumns {
    SelectedColumns::new(
        VisibilityColumn::CorrectedData,
        FlagPolicy::FlagOrFlagRow,
        WeightColumn::WeightSpectrum,
    )
}

fn selection(row_digest: u8, reverse: bool) -> ObservationSelection {
    let mut fields = vec![7, 2];
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
        intents.reverse();
        data_descriptions.reverse();
        spectral_windows.reverse();
        correlations.reverse();
    }

    ObservationSelection::new(
        selected_rows(row_digest),
        RowSelection::new(
            IdSelection::Only(fields),
            UvSelection::Ranges(vec![UvDistanceRange::new(
                Some(SelectionBound::inclusive(10.0)),
                Some(SelectionBound::inclusive(1_000.0)),
                UvDistanceUnit::Wavelengths,
            )]),
            IntentSelection::Only(intents),
        ),
        data_descriptions,
        spectral_windows,
        correlations,
    )
}

fn source(row_digest: u8, locator: &str, reverse: bool) -> ObservationSourceInput {
    ObservationSourceInput::new(
        ObservationSourceProvenance::new(locator.to_string()),
        selection(row_digest, reverse),
        columns(),
        false,
    )
}

fn snapshot(reverse: bool, left_locator: &str, right_locator: &str) -> ObservationSnapshot {
    // Sources keep request order; `reverse` reorders only what compilation
    // canonicalizes.
    let sources = vec![
        source(31, left_locator, reverse),
        source(32, right_locator, reverse),
    ];
    compile_observation(ObservationSnapshotInput::new(sources))
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
    let compiled = compile_observation(ObservationSnapshotInput::new(vec![
        ObservationSourceInput::new(
            ObservationSourceProvenance::new("/archive/all-correlations.ms".to_string()),
            exact_selection,
            columns(),
            false,
        ),
    ]))
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
        let snapshot = compile_observation(ObservationSnapshotInput::new(vec![
            ObservationSourceInput::new(
                ObservationSourceProvenance::new("/archive/data-description.ms".to_string()),
                selected,
                columns(),
                false,
            ),
        ]))
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
    assert_eq!(expected, reordered);
    assert_ne!(expected, different_pairing);
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
        compile_observation(ObservationSnapshotInput::new(vec![
            ObservationSourceInput::new(
                ObservationSourceProvenance::new("/archive/duplicate-ddid.ms".to_string()),
                invalid,
                columns(),
                false,
            ),
        ])),
        Err(CompileObservationError::DuplicateDataDescription {
            data_description_id: 1,
        })
    );
}

#[test]
fn full_spectral_coordinate_catalog_is_exact_selection_state() {
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
        compile_observation(ObservationSnapshotInput::new(vec![
            ObservationSourceInput::new(
                ObservationSourceProvenance::new("/archive/full-spw.ms".to_string()),
                exact,
                columns(),
                false,
            ),
        ]))
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
        first, changed_unselected_coordinate,
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
        compile_observation(ObservationSnapshotInput::new(vec![
            ObservationSourceInput::new(
                ObservationSourceProvenance::new("/archive/short-spw.ms".to_string()),
                invalid,
                columns(),
                false,
            ),
        ])),
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
        compile_observation(ObservationSnapshotInput::new(vec![
            ObservationSourceInput::new(
                ObservationSourceProvenance::new("/archive/inconsistent-row-ddid.ms".to_string()),
                invalid,
                columns(),
                false,
            ),
        ])),
        Err(CompileObservationError::SelectedRowDataDescriptionMissing {
            data_description_id: 99,
        })
    ));
}

#[test]
fn data_description_catalog_rejects_missing_and_unresolved_joins() {
    let base = selection(31, false);
    let compile = |data_descriptions| {
        compile_observation(ObservationSnapshotInput::new(vec![
            ObservationSourceInput::new(
                ObservationSourceProvenance::new(
                    "/archive/inexact-data-description.ms".to_string(),
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
            ),
        ]))
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
fn corrected_data_presence_is_snapshot_state() {
    let compile = |corrected_data_present| {
        compile_observation(ObservationSnapshotInput::new(vec![
            ObservationSourceInput::new(
                ObservationSourceProvenance::new("/archive/a.ms".to_string()),
                selection(31, false),
                columns(),
                corrected_data_present,
            ),
        ]))
        .expect("compile explicit CORRECTED_DATA presence")
    };

    let absent = compile(false);
    let present = compile(true);

    assert_ne!(absent, present);
    assert!(present.sources()[0].corrected_data_present());
}

#[test]
fn selection_content_is_canonical_but_provenance_retains_origin_and_request_order() {
    let first = snapshot(false, "/archive/a.ms", "/archive/b.ms");
    let reordered = snapshot(true, "/mirror/a.ms", "/mirror/b.ms");

    for (first, reordered) in first.sources().iter().zip(reordered.sources()) {
        assert_eq!(first.selection(), reordered.selection());
        assert_ne!(first.provenance(), reordered.provenance());
    }
    for snapshot in [&first, &reordered] {
        assert_eq!(snapshot.sources()[0].input_ordinal(), 0);
        assert_eq!(snapshot.sources()[1].input_ordinal(), 1);
    }
    assert_eq!(
        first.sources()[1].provenance().locator(),
        "/archive/b.ms",
        "sources keep their request order"
    );
}

#[test]
fn snapshot_exposes_exact_selection_semantics_without_bulk_samples() {
    let compiled = snapshot(false, "/archive/a.ms", "/archive/b.ms");
    let source = &compiled.sources()[0];
    let selection = source.selection();

    assert_eq!(selection.rows().source_row_count(), 100);
    assert_eq!(selection.rows().selected_row_count(), 11);
    assert_eq!(selection.rows(), &selected_rows(31));
    assert_eq!(selection.rows_filter().fields().ids(), Some(&[2, 7][..]));
    assert_eq!(
        selection.rows_filter().uv_distances(),
        &UvSelection::Ranges(vec![UvDistanceRange::new(
            Some(SelectionBound::inclusive(10.0)),
            Some(SelectionBound::inclusive(1_000.0)),
            UvDistanceUnit::Wavelengths,
        )])
    );
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
}

#[test]
fn compilation_fails_closed_on_an_empty_selection() {
    let empty_source = ObservationSourceInput::new(
        ObservationSourceProvenance::new("/archive/a.ms".to_string()),
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
        compile_observation(ObservationSnapshotInput::new(vec![empty_source])),
        Err(CompileObservationError::EmptySelection)
    ));
}
