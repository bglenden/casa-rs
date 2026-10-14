// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn a_multi_spw_selection_is_walked_one_data_description_at_a_time() {
    let directory = tempfile::tempdir().expect("temporary multi-SPW fixture");
    let path = directory.path().join("interleaved-spw.ms");
    generate_fixture(&path);
    extend_fixture_with_second_spw(&path);
    // Interleave the windows in storage: rows 0 and 2 in DATA_DESCRIPTION
    // 0, rows 1 and 3 in 1.
    let mut measurement_set = MeasurementSet::open(&path).expect("open multi-SPW fixture");
    for (row, data_description_id) in [(1, 1), (2, 0)] {
        measurement_set
            .main_table_mut()
            .cell_accessor_mut(row, "DATA_DESC_ID")
            .expect("open DATA_DESC_ID for mutation")
            .set(Value::Scalar(ScalarValue::Int32(data_description_id)))
            .expect("relabel the row's DATA_DESCRIPTION");
    }
    measurement_set.save().expect("save interleaved fixture");
    drop(measurement_set);
    let snapshot =
        compile_observation(ObservationSnapshotInput::new(vec![multi_spw_source_input(
            &path,
        )]))
        .expect("compile interleaved observation");
    let problem = compile(ProblemInput::new(
        specification(),
        geometry(),
        snapshot,
        model_lifecycle(),
    ))
    .expect("compile interleaved problem");
    let source = &problem.observation().sources()[0];
    let mut walks = Vec::new();
    for rows in [1, 3] {
        let observation = open_observation(
            &problem,
            source,
            content_budget_for_rows(&problem, source, rows, 1),
        )
        .expect("bind the interleaved stream");
        let (observation, first) = stream(&problem, observation).expect("first traversal");
        let (_, second) = stream(&problem, observation).expect("second traversal");
        assert_eq!(first, second, "every traversal takes the same walk");
        let mut order: Vec<(u64, i32)> = Vec::new();
        for sample in &first {
            let row = (sample.row.physical_row, sample.row.data_description_id);
            if order.last() != Some(&row) {
                order.push(row);
            }
        }
        assert_eq!(
            order,
            vec![(0, 0), (2, 0), (1, 1), (3, 1)],
            "{rows}-row blocks"
        );
        walks.push(first);
    }
    assert_eq!(walks[0], walks[1], "the walk does not depend on the block");
}
