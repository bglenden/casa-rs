// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CompileObservationError, CorrelationProduct, FlagPolicy, IdSelection, IntentSelection,
    ObservationSelection, ObservationSnapshot, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, PartitionId, PartitionSelection, RowSelection, SelectedColumns,
    SelectionBound, SpectralWindowCoordinateCatalog, SpectralWindowSelection, UvDistanceRange,
    UvDistanceUnit, UvSelection, VisibilityColumn, WeightColumn, compile_observation,
};
use casa_types::Correlation;

fn columns() -> SelectedColumns {
    SelectedColumns::new(
        VisibilityColumn::CorrectedData,
        FlagPolicy::FlagOrFlagRow,
        WeightColumn::WeightSpectrum,
    )
}

/// Partition `id` in window `window` reading `products` at their stored
/// positions, as given.
fn partition(id: u32, window: u32, products: &[(u32, Correlation)]) -> PartitionSelection {
    PartitionSelection::new(
        PartitionId(id),
        window,
        products
            .iter()
            .map(|(index, correlation)| CorrelationProduct::new(*index, *correlation))
            .collect(),
    )
}

/// Three partitions, two in window 2 (one circular, one linear) and one in
/// window 9, every list given out of canonical order (or reversed with
/// `reverse`).
fn partitions(reverse: bool) -> Vec<PartitionSelection> {
    let mut partitions = vec![
        partition(6, 9, &[(1, Correlation::YY), (0, Correlation::XX)]),
        partition(4, 2, &[(1, Correlation::YY), (0, Correlation::XX)]),
        partition(1, 2, &[(1, Correlation::LL), (0, Correlation::RR)]),
    ];
    if reverse {
        partitions.reverse();
    }
    partitions
}

fn selection(samples: u64, reverse: bool) -> ObservationSelection {
    let mut fields = vec![7, 2];
    let mut intents = vec![
        "OBSERVE_TARGET#ON_SOURCE".to_string(),
        "CALIBRATE_PHASE#ON_SOURCE".to_string(),
    ];
    let mut spectral_windows = vec![
        SpectralWindowSelection::new(9, vec![7, 5, 3]),
        SpectralWindowSelection::new(2, vec![8, 4, 0]),
    ];
    if reverse {
        fields.reverse();
        intents.reverse();
        spectral_windows.reverse();
    }

    ObservationSelection::new(
        samples,
        RowSelection::new(
            IdSelection::Only(fields),
            UvSelection::Ranges(vec![UvDistanceRange::new(
                Some(SelectionBound::inclusive(10.0)),
                Some(SelectionBound::inclusive(1_000.0)),
                UvDistanceUnit::Wavelengths,
            )]),
            IntentSelection::Only(intents),
        ),
        partitions(reverse),
        spectral_windows,
    )
}

/// `selection` with its partitions or windows replaced.
fn with(
    partitions: Option<Vec<PartitionSelection>>,
    spectral_windows: Option<Vec<SpectralWindowSelection>>,
) -> ObservationSelection {
    let base = selection(31, false);
    ObservationSelection::new(
        base.samples(),
        base.rows_filter().clone(),
        partitions.unwrap_or_else(|| base.partitions().to_vec()),
        spectral_windows.unwrap_or_else(|| base.spectral_windows().to_vec()),
    )
}

/// `base` with its per-sample predicates replaced.
fn with_rows(base: &ObservationSelection, rows_filter: RowSelection) -> ObservationSelection {
    ObservationSelection::new(
        base.samples(),
        rows_filter,
        base.partitions().to_vec(),
        base.spectral_windows().to_vec(),
    )
}

fn compile_one(
    selection: ObservationSelection,
    locator: &str,
) -> Result<ObservationSnapshot, CompileObservationError> {
    compile_observation(ObservationSnapshotInput::new(vec![
        ObservationSourceInput::new(
            ObservationSourceProvenance::new(locator.to_string()),
            selection,
            columns(),
            false,
        ),
    ]))
}

fn source(samples: u64, locator: &str, reverse: bool) -> ObservationSourceInput {
    ObservationSourceInput::new(
        ObservationSourceProvenance::new(locator.to_string()),
        selection(samples, reverse),
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
        Correlation::I,
        Correlation::Q,
        Correlation::U,
        Correlation::V,
        Correlation::RR,
        Correlation::RL,
        Correlation::LR,
        Correlation::LL,
        Correlation::XX,
        Correlation::XY,
        Correlation::YX,
        Correlation::YY,
        Correlation::RX,
        Correlation::RY,
        Correlation::LX,
        Correlation::LY,
        Correlation::XR,
        Correlation::XL,
        Correlation::YR,
        Correlation::YL,
        Correlation::PP,
        Correlation::PQ,
        Correlation::QP,
        Correlation::QQ,
        Correlation::RCircular,
        Correlation::LCircular,
        Correlation::Linear,
        Correlation::Ptotal,
        Correlation::Plinear,
        Correlation::PFtotal,
        Correlation::PFlinear,
        Correlation::Pangle,
    ];
    let exact_selection = with(
        Some(vec![PartitionSelection::new(
            PartitionId(0),
            2,
            correlation_types
                .iter()
                .enumerate()
                .map(|(index, correlation)| CorrelationProduct::new(index as u32, *correlation))
                .collect(),
        )]),
        Some(vec![SpectralWindowSelection::new(2, vec![0])]),
    );
    let compiled = compile_one(exact_selection, "/archive/all-correlations.ms")
        .expect("compile all MeasurementSet correlation coordinates");

    let compiled_types = compiled.sources()[0].selection().partitions()[0]
        .products()
        .iter()
        .map(|product| product.correlation_type())
        .collect::<Vec<_>>();
    assert_eq!(compiled_types, correlation_types);
}

#[test]
fn partitions_bind_their_window_and_products_in_canonical_order() {
    let expected = compile_one(selection(31, false), "/archive/partitions.ms")
        .expect("compile partition selection");
    let reordered = compile_one(selection(31, true), "/archive/partitions.ms")
        .expect("compile reordered partition selection");
    let mut moved = partitions(false);
    moved[1] = partition(4, 9, &[(0, Correlation::XX), (1, Correlation::YY)]);
    let different_window =
        compile_one(with(Some(moved), None), "/archive/partitions.ms").expect("compile moved");

    let selected = expected.sources()[0].selection().partitions();
    assert_eq!(
        selected
            .iter()
            .map(|partition| (partition.partition(), partition.spectral_window_id()))
            .collect::<Vec<_>>(),
        [
            (PartitionId(1), 2),
            (PartitionId(4), 2),
            (PartitionId(6), 9)
        ]
    );
    assert_eq!(
        selected[0].products(),
        &[
            CorrelationProduct::new(0, Correlation::RR),
            CorrelationProduct::new(1, Correlation::LL),
        ]
    );
    assert_eq!(expected, reordered);
    assert_ne!(expected, different_window);
}

#[test]
fn partition_selection_rejects_inconsistent_partitions() {
    let compile = |partitions| compile_one(with(Some(partitions), None), "/archive/bad.ms");
    let mut duplicate = partitions(false);
    duplicate.push(partition(4, 2, &[(0, Correlation::XX)]));
    assert_eq!(
        compile(duplicate),
        Err(CompileObservationError::DuplicatePartition { partition: 4 })
    );
    assert_eq!(
        compile(Vec::new()),
        Err(CompileObservationError::NoPartitionSelection)
    );
    let mut unknown_window = partitions(false);
    unknown_window.push(partition(8, 99, &[(0, Correlation::XX)]));
    assert_eq!(
        compile(unknown_window),
        Err(CompileObservationError::UnknownPartitionSpectralWindow {
            partition: 8,
            spectral_window_id: 99,
        })
    );
    assert_eq!(
        compile(vec![
            partition(1, 2, &[(0, Correlation::RR)]),
            partition(4, 2, &[(0, Correlation::XX)]),
        ]),
        Err(CompileObservationError::OrphanSpectralWindowSelection {
            spectral_window_id: 9,
        })
    );
    let mut products = partitions(false);
    products[0] = partition(6, 9, &[]);
    assert_eq!(
        compile(products.clone()),
        Err(CompileObservationError::EmptyCorrelationSelection { partition: 6 })
    );
    products[0] = partition(6, 9, &[(0, Correlation::XX), (0, Correlation::YY)]);
    assert_eq!(
        compile(products.clone()),
        Err(CompileObservationError::DuplicateCorrelationIndex { partition: 6 })
    );
    products[0] = partition(6, 9, &[(0, Correlation::XX), (1, Correlation::XX)]);
    assert_eq!(
        compile(products),
        Err(CompileObservationError::DuplicateCorrelationType { partition: 6 })
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
        compile_one(with(None, Some(spectral_windows)), "/archive/full-spw.ms")
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
    assert!(matches!(
        compile_one(with(None, Some(spectral_windows)), "/archive/short-spw.ms"),
        Err(
            CompileObservationError::SpectralWindowCoordinateCatalogMismatch {
                spectral_window_id: 2
            }
        )
    ));
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

    assert_eq!(selection.samples(), 31);
    assert_eq!(
        selection.rows_filter().fields().explicit_ids(),
        Some(&[2, 7][..])
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
        selection.rows_filter().intents(),
        &IntentSelection::Only(vec![
            "CALIBRATE_PHASE#ON_SOURCE".to_string(),
            "OBSERVE_TARGET#ON_SOURCE".to_string(),
        ])
    );
    assert_eq!(
        selection
            .partitions()
            .iter()
            .map(PartitionSelection::partition)
            .collect::<Vec<_>>(),
        [PartitionId(1), PartitionId(4), PartitionId(6)]
    );
    assert_eq!(selection.spectral_windows()[0].spectral_window_id(), 2);
    assert_eq!(
        selection.spectral_windows()[0].channel_indices(),
        &[0, 4, 8]
    );
    assert_eq!(
        selection.partitions()[0].products()[0],
        CorrelationProduct::new(0, Correlation::RR)
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
        selection(0, false),
        columns(),
        false,
    );
    assert!(matches!(
        compile_observation(ObservationSnapshotInput::new(vec![empty_source])),
        Err(CompileObservationError::EmptySelection)
    ));
}

#[test]
fn scan_and_observation_ids_are_canonical_and_charge_owned_capacity() {
    let base = selection(31, false);
    let mut scans = Vec::with_capacity(17);
    scans.extend([3, 1, 3]);
    let mut observations = Vec::with_capacity(11);
    observations.extend([1, 0, 1]);
    let bytes = (scans.capacity() + observations.capacity()) * size_of::<u32>();
    let selected = with_rows(
        &base,
        base.rows_filter()
            .clone()
            .with_scan_observation(IdSelection::Only(scans), IdSelection::Only(observations)),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(vec![
        ObservationSourceInput::new(
            ObservationSourceProvenance::new("selected.ms".to_string()),
            selected,
            columns(),
            false,
        ),
    ]))
    .unwrap();
    let rows = snapshot.sources()[0].selection().rows_filter();
    assert_eq!(rows.scans().explicit_ids(), Some(&[1, 3][..]));
    assert_eq!(rows.observations().explicit_ids(), Some(&[0, 1][..]));
    // Canonicalization dedups in place: the owned capacity is what is held.
    assert_eq!(
        rows.scans().allocated_bytes().unwrap() + rows.observations().allocated_bytes().unwrap(),
        bytes
    );
}

#[test]
fn empty_exact_scan_and_observation_sets_are_not_unconstrained() {
    for (scans, observations, expected) in [
        (IdSelection::Only(vec![]), IdSelection::All, "scan"),
        (IdSelection::All, IdSelection::Only(vec![]), "observation"),
        (IdSelection::Ranges(vec![]), IdSelection::All, "scan"),
        (IdSelection::All, IdSelection::Ranges(vec![]), "observation"),
    ] {
        let base = selection(31, false);
        let selected = with_rows(
            &base,
            base.rows_filter()
                .clone()
                .with_scan_observation(scans, observations),
        );
        assert!(
            matches!(compile_observation(ObservationSnapshotInput::new(vec![
            ObservationSourceInput::new(ObservationSourceProvenance::new("selected.ms".to_string()), selected, columns(), false),
        ])), Err(CompileObservationError::EmptyIdSelection { selector }) if selector == expected)
        );
    }
}

#[test]
fn numeric_intervals_canonicalize_and_charge_spare_capacity() {
    let base = selection(31, false);
    let mut ranges = Vec::with_capacity(19);
    ranges.extend([(u32::MAX, 10), (4, 2), (5, 9), (2, 3)]);
    let bytes = ranges.capacity() * size_of::<(u32, u32)>();
    let selected = with_rows(
        &base,
        base.rows_filter()
            .clone()
            .with_scan_observation(IdSelection::Ranges(ranges), IdSelection::All),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(vec![
        ObservationSourceInput::new(
            ObservationSourceProvenance::new("selected.ms".to_string()),
            selected,
            columns(),
            false,
        ),
    ]))
    .unwrap();
    let scans = snapshot.sources()[0].selection().rows_filter().scans();
    assert_eq!(scans, &IdSelection::Ranges(vec![(2, u32::MAX)]));
    assert_eq!(scans.allocated_bytes(), Some(bytes));
    for value in [2, 10, i64::from(u32::MAX)] {
        assert!(scans.holds(value));
    }
    for value in [-1, 0, 1, i64::from(u32::MAX) + 1] {
        assert!(!scans.holds(value));
    }
}
