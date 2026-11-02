// SPDX-License-Identifier: LGPL-3.0-or-later
//! The bandpass solve's per-channel cuts and the `BPOLY` fit's channels.

use ndarray::Array2;

use super::*;
use crate::solve::grouping::{SolveSampleRow, SolveSamples};

const CHANNELS: usize = 40;

/// A one-interval, one-channel group over four antennas' six baselines,
/// each sample 1 but baseline 0-1's, `perturbed`.
fn four_antenna_group(perturbed: Complex32) -> SolveAccumulator {
    let options = SolveGroupOptions {
        gain_type: GainType::G,
        model_source: GainSolveModelSource::PointSource,
        stokes_i: 1.0,
        solve_interval: GainSolveInterval::Infinite,
        combine: GainSolveCombine::default(),
        channelized: true,
    };
    let mut group = SolveAccumulator::new(0, 0, 0);
    for antenna1 in 0..4 {
        for antenna2 in antenna1 + 1..4 {
            let value = if (antenna1, antenna2) == (0, 1) {
                perturbed
            } else {
                Complex32::new(1.0, 0.0)
            };
            let data = Array2::from_elem((2, 1), value);
            let flags = Array2::from_elem((2, 1), false);
            let weights = Array2::from_elem((2, 1), 1.0_f32);
            let row = SolveSampleRow {
                field_id: 0,
                data_desc_id: 0,
                antenna1,
                antenna2,
                time_seconds: 100.0,
                time_centroid_seconds: 100.0,
                interval_seconds: 1.0,
                scan_number: 1,
            };
            let samples = SolveSamples {
                data: data.view(),
                flags: flags.view(),
                weights: weights.view(),
                model: None,
            };
            group
                .observe_samples(&row, &samples, &[5, 8], &[0], &options)
                .expect("observe");
        }
    }
    group.finalize_for_solve(GainSolveMode::AmplitudePhase);
    group
}

/// The antennas `cuts` leave an unflagged first-receptor solution.
fn solved_antennas(group: &SolveAccumulator, cuts: BandpassSolveCuts) -> BTreeSet<i32> {
    let (rows, _) = solve_bandpass_group(
        group,
        &BTreeSet::from([0, 1, 2, 3]),
        0,
        BandpassSolveCombine::default(),
        false,
        Some(cuts),
    );
    rows.iter()
        .filter(|row| !row.flags[0])
        .map(|row| row.antenna_id)
        .collect()
}

#[test]
fn channel_solves_apply_casas_baseline_and_snr_cuts() {
    let group = four_antenna_group(Complex32::new(1.2, 0.1));
    // Each antenna has three baselines, which CASA's default of four does
    // not solve.
    assert!(solved_antennas(&group, BandpassSolveCuts::CASA_DEFAULTS).is_empty());
    let three = BandpassSolveCuts {
        min_baselines_per_antenna: 3,
        min_snr: 0.0,
        ..BandpassSolveCuts::CASA_DEFAULTS
    };
    assert_eq!(solved_antennas(&group, three), BTreeSet::from([0, 1, 2, 3]));

    // Baseline 0-1 does not close with the others, so each SNR is finite;
    // a minsnr between them flags the antennas at or below it.
    let solved = solve_graphs(
        &group.channel_graphs[0],
        GainType::G,
        GainSolveMode::AmplitudePhase,
        SolveGraphOptions {
            refant_id: 0,
            min_baselines_per_antenna: 3,
        },
        None,
    )
    .expect("solve");
    let snr = &solved.receptors[0].snr;
    let (low, high) = snr
        .values()
        .fold((f32::INFINITY, 0.0_f32), |(low, high), snr| {
            (low.min(*snr), high.max(*snr))
        });
    let threshold = (low + high) / 2.0;
    let above = snr
        .iter()
        .filter(|(_, snr)| **snr > threshold)
        .map(|(antenna_id, _)| *antenna_id)
        .collect::<BTreeSet<_>>();
    assert!(!above.is_empty() && above.len() < 4, "{snr:?}");
    let cuts = BandpassSolveCuts {
        min_snr: threshold,
        ..three
    };
    assert_eq!(solved_antennas(&group, cuts), above);
}

/// A solution over `CHANNELS` channels of unit amplitude and phase
/// `0.5 * 0.2 + 0.6 x`, in BPOLY's Chebyshev terms over the window, with the
/// `wild` channels far off and the `flagged` ones flagged.
fn solution(wild: &[usize], flagged: &[usize]) -> (BandpassSolutionRow, Vec<f64>) {
    let frequencies = (0..CHANNELS)
        .map(|channel| 100.0e9 + 1.0e6 * channel as f64)
        .collect::<Vec<_>>();
    let (low, high) = (frequencies[0], frequencies[CHANNELS - 1]);
    let (mut gains, mut flags) = (Vec::new(), Vec::new());
    for (channel, frequency) in frequencies.iter().enumerate() {
        let x = (2.0 * frequency - low - high) / (high - low);
        let gain = if wild.contains(&channel) {
            Complex32::from_polar(3.0, 2.5)
        } else {
            Complex32::from_polar(1.0, (0.5 * 0.2 + 0.6 * x) as f32)
        };
        for _receptor in 0..2 {
            gains.push(gain);
            flags.push(flagged.contains(&channel));
        }
    }
    let row = BandpassSolutionRow {
        time_seconds: 0.0,
        interval_seconds: 1.0,
        field_id: 0,
        spw_id: 0,
        antenna_id: 1,
        scan_number: 1,
        observation_id: 0,
        channel_count: CHANNELS,
        gains,
        flags,
    };
    (row, frequencies)
}

/// The first receptor's linear phase terms.
fn phase_terms(
    row: &BandpassSolutionRow,
    frequencies: &[f64],
    cuts: Option<BandpassSolveCuts>,
) -> Vec<f64> {
    fit_bpoly_row(row, frequencies, [1, 1], cuts)
        .expect("fit")
        .phase_coefficients[0]
        .clone()
}

fn assert_line(terms: &[f64]) {
    assert!(
        (terms[0] - 0.2).abs() < 1e-5 && (terms[1] - 0.6).abs() < 1e-5,
        "{terms:?}"
    );
}

#[test]
fn bpoly_fits_leave_out_casas_edge_channels() {
    let cuts = BandpassSolveCuts::CASA_DEFAULTS;
    let masked = |count: usize| {
        (0..count)
            .filter(|channel| cuts.masks_edge_channel(*channel, count))
            .collect::<Vec<_>>()
    };
    assert_eq!(masked(CHANNELS), [0, 1, 39]);
    // Twenty channels mask only the first: 20 * 0.95 is not below 19.
    assert_eq!(masked(20), [0]);
    let (row, frequencies) = solution(&masked(CHANNELS), &[]);
    assert_line(&phase_terms(&row, &frequencies, Some(cuts)));
    let unmasked = phase_terms(&row, &frequencies, None);
    assert!((unmasked[1] - 0.6).abs() > 1e-2, "{unmasked:?}");
}

#[test]
fn bpoly_fits_leave_out_flagged_channels() {
    let (row, frequencies) = solution(&[20], &[20]);
    assert_line(&phase_terms(&row, &frequencies, None));
    // With every channel flagged a receptor keeps a unit gain.
    let (row, frequencies) = solution(&[], &(0..CHANNELS).collect::<Vec<_>>());
    let fit = fit_bpoly_row(&row, &frequencies, [3, 3], None).expect("fit");
    assert_eq!(fit.amp_coefficients[0], [0.0; 4]);
    assert_eq!(fit.phase_coefficients[0], [0.0; 4]);
}
