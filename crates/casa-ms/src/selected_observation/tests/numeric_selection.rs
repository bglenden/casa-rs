// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;

fn relabel(path: &std::path::Path, scans: &[i32], observations: &[i32]) {
    let mut ms = MeasurementSet::open(path).unwrap();
    let observation = ms
        .subtable(crate::SubtableId::Observation)
        .unwrap()
        .rows()
        .unwrap()[0]
        .clone();
    ms.subtable_mut(crate::SubtableId::Observation)
        .unwrap()
        .add_row(observation)
        .unwrap();
    for (row, (&scan, &observation)) in scans.iter().zip(observations).enumerate() {
        for (column, value) in [("SCAN_NUMBER", scan), ("OBSERVATION_ID", observation)] {
            ms.main_table_mut()
                .row_accessor_mut()
                .set_cell(row, column, Value::Scalar(ScalarValue::Int32(value)))
                .unwrap();
        }
    }
    ms.save().unwrap();
}

fn rows_selected(
    ms: &MeasurementSet,
    ddids: &[i32],
    scan: Option<&str>,
    observation: Option<&str>,
) -> (
    crate::SelectedObservationRowSelection,
    Vec<SelectedObservationRow>,
) {
    let selection = ms
        .selected_observation_row_selection(ddids, None, None, None)
        .unwrap()
        .with_numeric_row_selectors(scan, observation)
        .unwrap();
    let mut rows = Vec::new();
    ms.visit_selected_observation_rows(
        &selection,
        MsSelectionIoBudget {
            available_bytes: 3 * SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            maximum_live_blocks: 1,
            requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: Some(3),
        },
        |row| rows.push(row),
    )
    .unwrap();
    (selection, rows)
}

#[test]
fn scan_observation_survey_and_repeated_streams_select_the_same_sparse_rows() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("numeric-selection.ms");
    generate_fixture_with_rows(&path, 6);
    relabel(&path, &[1, 2, 3, 1, 2, 3], &[0, 1, 0, 1, 0, 1]);
    let dense = stream_rows(&compiled_problem(&path, 6), 1);
    let ms = MeasurementSet::open(&path).unwrap();
    for (scan, observation, expected) in [
        (Some("1,3"), Some("0"), vec![0, 2]),
        (None, Some("1"), vec![1, 3, 5]),
        (Some("1~3,2"), None, vec![0, 1, 2, 3, 4, 5]),
        (Some("2~2147483647"), Some("0~2147483647"), vec![1, 2, 4, 5]),
    ] {
        let (selection, rows) = rows_selected(&ms, &[0], scan, observation);
        assert_eq!(
            rows.iter()
                .map(|row| row.physical_row())
                .collect::<Vec<_>>(),
            expected
        );
        assert!(
            rows.iter()
                .all(|row| row.scan_number().is_some() == scan.is_some())
        );
        let selected_rows = SelectedRows::from_ordered_main_rows(
            6,
            rows.iter()
                .map(|row| SelectedMainRow::new(row.physical_row() as u64, 0)),
        )
        .unwrap();
        let snapshot =
            compile_observation(ObservationSnapshotInput::new(vec![fixture_source_input(
                &path,
                fixture_selection(selected_rows, selection.rows().clone()),
            )]))
            .unwrap();
        let problem = compile(ProblemInput::new(
            specification(),
            geometry(),
            snapshot,
            model_lifecycle(),
        ))
        .unwrap();
        let expected_samples = dense
            .iter()
            .filter(|sample| expected.contains(&(sample.row.physical_row as usize)))
            .cloned()
            .collect::<Vec<_>>();
        for block in [1, 3] {
            let block = block.min(expected.len());
            let source = &problem.observation().sources()[0];
            let observation = open_observation(
                &problem,
                source,
                content_budget_for_rows(&problem, source, block, 1),
            )
            .unwrap();
            let (observation, first) = stream(&problem, observation).unwrap();
            let (_, second) = stream(&problem, observation).unwrap();
            assert_eq!(first, expected_samples, "{scan:?}: {block}-row blocks");
            assert_eq!(second, first, "repeat preserves physical rows and samples");
        }
    }
    for unsupported in [">1", "*", "1^2", "1~1", "3~1"] {
        assert!(
            ms.selected_observation_row_selection(&[0], None, None, None)
                .unwrap()
                .with_numeric_row_selectors(Some(unsupported), None)
                .is_err()
        );
    }
}

#[test]
fn scan_selection_reaches_both_multi_description_walk_passes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("scan-walk.ms");
    generate_fixture(&path);
    extend_fixture_with_second_spw(&path);
    relabel(&path, &[1, 3, 3, 1], &[0, 0, 0, 0]);
    let all = compile_observation(ObservationSnapshotInput::new(vec![multi_spw_source_input(
        &path,
    )]))
    .unwrap();
    let base = all.sources()[0].selection();
    let ms = MeasurementSet::open(&path).unwrap();
    let (selection, rows) = rows_selected(&ms, &[0, 1], Some("1"), Some("0"));
    assert_eq!(
        rows.iter()
            .map(|row| row.physical_row())
            .collect::<Vec<_>>(),
        [0, 3]
    );
    let selected_rows = SelectedRows::from_ordered_main_rows(
        4,
        rows.iter().map(|row| {
            SelectedMainRow::new(row.physical_row() as u64, row.data_description_id() as u32)
        }),
    )
    .unwrap();
    let selected = ObservationSelection::new(
        selected_rows,
        selection.rows().clone(),
        base.data_descriptions().to_vec(),
        base.spectral_windows().to_vec(),
        base.correlations().to_vec(),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(vec![fixture_source_input(
        &path, selected,
    )]))
    .unwrap();
    let problem = compile(ProblemInput::new(
        specification(),
        geometry(),
        snapshot,
        model_lifecycle(),
    ))
    .unwrap();
    let source = &problem.observation().sources()[0];
    for block in [1, 2] {
        let observation = open_observation(
            &problem,
            source,
            content_budget_for_rows(&problem, source, block, 1),
        )
        .unwrap();
        let (observation, first) = stream(&problem, observation).unwrap();
        let (_, second) = stream(&problem, observation).unwrap();
        let mut actual = first
            .iter()
            .map(|sample| sample.row.physical_row)
            .collect::<Vec<_>>();
        actual.dedup();
        assert_eq!(actual, [0, 3]);
        assert_eq!(first, second);
    }
}

#[test]
fn frontend_row_projection_uses_the_canonical_bounded_observation_evaluator() {
    assert_eq!(SelectedObservationRow::STORAGE_BYTES_PER_ROW, 73);
    let directory = tempfile::tempdir().expect("temporary row-projection fixture");
    let path = directory.path().join("row-projection.ms");
    generate_fixture(&path);
    let measurement_set = MeasurementSet::open(&path).expect("open row-projection fixture");
    let selection = measurement_set
        .selected_observation_row_selection(&[0], None, None, None)
        .expect("resolve frontend selectors to the native row contract");
    let mut rows = Vec::new();

    measurement_set
        .visit_selected_observation_rows(
            &selection,
            MsSelectionIoBudget {
                available_bytes: 2 * SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                maximum_live_blocks: 2,
                requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                storage_alignment_rows: None,
            },
            |row| rows.push(row),
        )
        .expect("visit canonical selected rows");
    let expected_time_centroids = (0..2)
        .map(|row| {
            match measurement_set
                .main_table()
                .cell_accessor(row, "TIME_CENTROID")
                .and_then(|cell| cell.scalar())
                .expect("stored MAIN.TIME_CENTROID")
            {
                ScalarValue::Float64(value) => *value,
                other => panic!(
                    "MAIN.TIME_CENTROID must be Float64, found {:?}",
                    other.primitive_type()
                ),
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .map(|row| (row.physical_row(), row.data_description_id()))
            .collect::<Vec<_>>(),
        vec![(0, 0), (1, 0)]
    );
    assert!(rows.iter().all(|row| !row.flag_row()));
    assert!(rows.iter().all(|row| row.observation_id() == 0));
    assert_eq!(
        rows.iter()
            .map(|row| row.time_centroid_mjd_seconds())
            .collect::<Vec<_>>(),
        expected_time_centroids
    );
}
