// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

fn accumulator(receptors: usize) -> SolveAccumulator {
    let mut group = SolveAccumulator::new(0, 0, 0);
    group
        .receptor_accumulations
        .resize_with(receptors, HashMap::new);
    group
}

fn add(group: &mut SolveAccumulator, correlation_index: usize, sample: Complex32) {
    let accumulation = group.receptor_accumulations[0]
        .entry(SolveAccumulationKey {
            baseline: unaveraged_baseline((0, 1)),
            correlation_index,
            parallel_hand: true,
        })
        .or_insert_with(|| vec![SolveEdgeAccumulation::default()]);
    let accumulation = &mut accumulation[0];
    accumulation.weighted_sum += sample;
    accumulation.weight += 1.0;
    accumulation.model_weight += 1.0;
    accumulation.weighted_sample_power += f64::from(sample.norm_sqr());
    accumulation.sample_count += 1;
}

fn unaveraged_baseline(antennas: (i32, i32)) -> AveragedBaseline {
    AveragedBaseline {
        antennas,
        field_id: 0,
        data_desc_id: 0,
        start_bits: 0,
    }
}

#[test]
fn finalize_for_phase_solve_collapses_before_unit_amplitude_normalization() {
    let mut group = accumulator(1);
    add(&mut group, 0, Complex32::new(2.0, 0.0));
    add(&mut group, 0, Complex32::new(0.0, 2.0));

    group.finalize_for_solve(GainSolveMode::Phase, false);

    let graph = &group.channel_graphs[0][0];
    assert!((graph.weights[&(0, 1)] - 4.0).abs() < 1.0e-6);
    let edge = graph.edges[&(0, 1)];
    let expected = 4.0_f32 / 2.0_f32.sqrt();
    assert!((edge.re - expected).abs() < 1.0e-6);
    assert!((edge.im - expected).abs() < 1.0e-6);
    assert_eq!(graph.stats[&(0, 1)].sample_count, 2);
}

#[test]
fn finalize_for_phase_solve_keeps_correlations_separate_for_scalar_t() {
    let mut group = accumulator(1);
    add(&mut group, 0, Complex32::new(2.0, 0.0));
    add(&mut group, 3, Complex32::new(0.0, 2.0));

    group.finalize_for_solve(GainSolveMode::Phase, false);

    let graph = &group.channel_graphs[0][0];
    assert!((graph.weights[&(0, 1)] - 8.0).abs() < 1.0e-6);
    let edge = graph.edges[&(0, 1)];
    assert!((edge.re - 4.0).abs() < 1.0e-6);
    assert!((edge.im - 4.0).abs() < 1.0e-6);
    assert_eq!(graph.stats[&(0, 1)].sample_count, 2);
}

#[test]
fn solution_time_weights_baseline_centroids_by_solve_weight() {
    // Baseline 0-1 (rows at 10 s and 12 s) carries three times the
    // weight of 0-2 (one row at 20 s).
    let mut group = accumulator(1);
    for (baseline, weight) in [((0, 1), 3.0_f32), ((0, 2), 1.0)] {
        group.receptor_accumulations[0].insert(
            SolveAccumulationKey {
                baseline: unaveraged_baseline(baseline),
                correlation_index: 0,
                parallel_hand: true,
            },
            vec![SolveEdgeAccumulation {
                weighted_sum: Complex32::new(weight, 0.0),
                weight,
                model_weight: weight,
                weighted_sample_power: f64::from(weight),
                sample_count: 1,
            }],
        );
    }
    let mut first = WeightedTime::default();
    first.observe(10.0, 1.0);
    first.observe(12.0, 1.0);
    group
        .baseline_centroids
        .insert(unaveraged_baseline((0, 1)), first);
    let mut second = WeightedTime::default();
    second.observe(20.0, 1.0);
    group
        .baseline_centroids
        .insert(unaveraged_baseline((0, 2)), second);

    group.finalize_for_solve(GainSolveMode::AmplitudePhase, false);

    assert!((group.solution_time - (3.0 * 11.0 + 20.0) / 4.0).abs() < 1.0e-12);
}

/// A cross-correlation row of field `field_id`, scan `scan_number` and
/// data description `data_desc_id` (on window 0) at `time_seconds`.
fn row(
    row_index: usize,
    field_id: i32,
    scan_number: i32,
    data_desc_id: i32,
    time_seconds: f64,
) -> SelectedSolveRow {
    SelectedSolveRow {
        row_index,
        array_id: 0,
        field_id,
        observation_id: 0,
        data_desc_id,
        data_spw_id: 0,
        channel_selection: None,
        antenna1: 0,
        antenna2: 1,
        time_seconds,
        time_centroid_seconds: time_seconds,
        interval_seconds: 1.0,
        scan_number,
        flag_row: false,
    }
}

#[test]
fn combined_intervals_start_at_the_earliest_row_whatever_the_field_order() {
    // Field 1 is observed first, in scan 1; field 0 thirty seconds
    // later, in scan 2. Sorting by field first would start the 20 s
    // intervals at field 0's scan and fold field 1's into its first.
    let rows = [130.0, 131.0, 132.0]
        .map(|time| (0, 2, time))
        .into_iter()
        .chain([100.0, 101.0, 102.0].map(|time| (1, 1, time)))
        .enumerate()
        .map(|(index, (field, scan, time))| row(index, field, scan, 0, time))
        .collect::<Vec<_>>();
    let buckets = |combine| {
        bucketed_rows(
            &rows,
            GainSolveInterval::Seconds(20.0),
            combine,
            false,
            false,
            false,
        )
        .into_iter()
        .map(|(row, base, bucket)| (row.time_seconds, base.field_id, bucket))
        .collect::<Vec<_>>()
    };
    let combined = buckets(GainSolveCombine {
        scans: true,
        fields: true,
        ..GainSolveCombine::default()
    });
    let times = combined.iter().map(|(time, ..)| *time).collect::<Vec<_>>();
    assert_eq!(times, [100.0, 101.0, 102.0, 130.0, 131.0, 132.0]);
    let intervals = combined
        .iter()
        .map(|(.., bucket)| *bucket)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        intervals,
        BTreeSet::from([SolveBucketKey::Seconds(0), SolveBucketKey::Seconds(1)])
    );
    // Kept apart, each field's intervals start at its own first row.
    for (time, field, bucket) in buckets(GainSolveCombine::default()) {
        assert_eq!(bucket, SolveBucketKey::Seconds(0), "{time} {field}");
    }
}

#[test]
fn bandpass_still_rejects_data_descriptions_sharing_a_window() {
    use crate::session::refuse_shared_spectral_windows;
    // Two descriptions on window 0, as a duplicated description or one
    // holding the cross hands would give.
    let rows = [row(0, 0, 1, 0, 100.0), row(1, 0, 1, 1, 100.0)];
    let error = refuse_shared_spectral_windows(&rows).unwrap_err();
    assert!(
        matches!(
            &error,
            GainSolveError::SharedSpectralWindow { spw_id: 0, data_desc_ids }
                if data_desc_ids == &[0, 1]
        ),
        "{error}"
    );
    assert!(error.to_string().contains("#979"), "{error}");
    assert!(refuse_shared_spectral_windows(&rows[..1]).is_ok());
}

#[test]
fn median_with_even_mean_matches_casacore_partial_medians() {
    assert_eq!(median_with_even_mean(&mut []), 0.0);
    assert_eq!(median_with_even_mean(&mut [3.0, 0.0, 1.0]), 1.0);
    assert_eq!(median_with_even_mean(&mut [4.0, 0.0, 4.0, 4.0]), 4.0);
    assert_eq!(median_with_even_mean(&mut [2.0, 0.0]), 1.0);
}
