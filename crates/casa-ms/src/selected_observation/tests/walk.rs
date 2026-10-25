// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use casa_imaging_model::{SelectionBound, UvDistanceRange, UvDistanceUnit};

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

/// Append a SPECTRAL_WINDOW at `frequencies_hz` and a DATA_DESCRIPTION over
/// it to the fixture at `path`, with a copy of every MAIN row of
/// DATA_DESCRIPTION 0 relabelled to it; returns its id.
fn add_data_description(path: &std::path::Path, frequencies_hz: [f64; 3]) -> i32 {
    let mut measurement_set = MeasurementSet::open(path).expect("open the fixture");
    let windows = measurement_set
        .spectral_window()
        .expect("SPECTRAL_WINDOW")
        .table()
        .rows()
        .expect("SPECTRAL_WINDOW rows")
        .to_vec();
    let mut spectral_window = windows[0].clone();
    spectral_window.upsert(
        "CHAN_FREQ",
        Value::Array(ArrayValue::Float64(
            ArrayD::from_shape_vec(vec![3], frequencies_hz.to_vec()).expect("frequency shape"),
        )),
    );
    spectral_window.upsert(
        "REF_FREQUENCY",
        Value::Scalar(ScalarValue::Float64(frequencies_hz[1])),
    );
    measurement_set
        .spectral_window_mut()
        .expect("mutable SPECTRAL_WINDOW")
        .table_mut()
        .add_row(spectral_window)
        .expect("append a SPECTRAL_WINDOW");
    let descriptions = measurement_set
        .data_description()
        .expect("DATA_DESCRIPTION")
        .table()
        .rows()
        .expect("DATA_DESCRIPTION rows")
        .to_vec();
    let id = i32::try_from(descriptions.len()).expect("DATA_DESCRIPTION id");
    let mut data_description = descriptions[0].clone();
    data_description.upsert(
        "SPECTRAL_WINDOW_ID",
        Value::Scalar(ScalarValue::Int32(
            i32::try_from(windows.len()).expect("SPECTRAL_WINDOW id"),
        )),
    );
    measurement_set
        .data_description_mut()
        .expect("mutable DATA_DESCRIPTION")
        .table_mut()
        .add_row(data_description)
        .expect("append a DATA_DESCRIPTION");
    let first = measurement_set
        .main_table()
        .rows()
        .expect("MAIN rows")
        .iter()
        .filter(|row| row.get("DATA_DESC_ID") == Some(&Value::Scalar(ScalarValue::Int32(0))))
        .cloned()
        .collect::<Vec<_>>();
    for mut row in first {
        row.upsert("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(id)));
        measurement_set
            .main_table_mut()
            .add_row(row)
            .expect("append a MAIN row");
    }
    measurement_set.save().expect("save the fixture");
    id
}

/// #718 (Fable nit 15, Astra gate answer 4): a walk over several selected
/// DATA_DESCRIPTIONs drops what the row predicate drops, here a third
/// DATA_DESCRIPTION the selection leaves out and the rows a uv range
/// excludes. Its samples are, as a multiset, the samples of one selection
/// per DATA_DESCRIPTION together, and each traversal completes, so it
/// selected the rows its manifest counts.
#[test]
fn a_multi_spw_walk_drops_what_the_selection_drops() {
    let directory = tempfile::tempdir().expect("temporary fixture");
    let path = directory.path().join("three-spw.ms");
    generate_fixture(&path);
    let second = add_data_description(&path, [1.5e9, 1.501e9, 1.502e9]);
    let third = add_data_description(&path, [1.6e9, 1.601e9, 1.602e9]);
    assert_eq!((second, third), (1, 2));
    // MAIN: two times in each DATA_DESCRIPTION, rows 0-1, 2-3 and 4-5. A uv
    // range between the two times' baselines keeps the shorter.
    let measurement_set = MeasurementSet::open(&path).expect("open the fixture");
    let distance = |row: usize| {
        let Some(Value::Array(ArrayValue::Float64(uvw))) =
            measurement_set.main_table().rows().expect("MAIN rows")[row].get("UVW")
        else {
            panic!("UVW is a float array");
        };
        uvw[0].hypot(uvw[1])
    };
    let (earlier, later) = (distance(0), distance(1));
    assert!(earlier != later, "the two times have distinct baselines");
    let kept = usize::from(later < earlier);
    let cut = UvSelection::Ranges(vec![UvDistanceRange::new(
        None,
        Some(SelectionBound::inclusive(earlier.min(later))),
        UvDistanceUnit::Meters,
    )]);
    drop(measurement_set);
    let windows = [
        (
            DataDescriptionSelection::new(0, 0, 0),
            SpectralWindowSelection::new(0, vec![0, 2]),
        ),
        (
            DataDescriptionSelection::new(1, 1, 0),
            SpectralWindowSelection::new(1, vec![1, 2]),
        ),
    ];
    // The samples of a selection of `which` windows, sorted.
    let walk = |which: &[usize]| {
        let rows = which
            .iter()
            .map(|&window| SelectedMainRow::new((2 * window + kept) as u64, window as u32))
            .collect::<Vec<_>>();
        let selection = ObservationSelection::new(
            SelectedRows::from_ordered_main_rows(6, rows).expect("selected row manifest"),
            RowSelection::new(IdSelection::All, cut.clone(), IntentSelection::All),
            which.iter().map(|&window| windows[window].0).collect(),
            which
                .iter()
                .map(|&window| windows[window].1.clone())
                .collect(),
            vec![CorrelationSelection::new(
                0,
                vec![
                    CorrelationProduct::new(0, Correlation::RR),
                    CorrelationProduct::new(1, Correlation::LL),
                ],
            )],
        );
        let snapshot =
            compile_observation(ObservationSnapshotInput::new(vec![fixture_source_input(
                &path, selection,
            )]))
            .expect("compile the observation");
        let problem = compile(ProblemInput::new(
            specification(),
            geometry(),
            snapshot,
            model_lifecycle(),
        ))
        .expect("compile the problem");
        let mut samples = stream_rows(&problem, 1);
        samples.sort_by_key(|sample| {
            (
                sample.row.physical_row,
                sample.channel.channel_index,
                sample.correlation.correlation_index(),
            )
        });
        samples
    };
    let both = walk(&[0, 1]);
    let mut apart = walk(&[0]);
    apart.extend(walk(&[1]));
    assert!(!both.is_empty(), "the selection holds samples");
    assert_eq!(both, apart, "the walk is the single selections together");
    let rows = both
        .iter()
        .map(|sample| (sample.row.physical_row, sample.row.data_description_id))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        rows.into_iter().collect::<Vec<_>>(),
        vec![(kept as u64, 0), (2 + kept as u64, 1)],
        "neither the third DATA_DESCRIPTION nor the excluded time is walked"
    );
}
