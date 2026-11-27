// SPDX-License-Identifier: LGPL-3.0-or-later

const CLIP_ZEROS: ClipOptions<'static> = ClipOptions {
    zeros: true,
    signed_zeros: false,
    bounds: None,
    outside: true,
    projections: None,
    metric: None,
};

use super::*;
use ndarray::array;

#[test]
fn tfcrop_angle_roundoff_covers_the_principal_phase_branch_cut() {
    let above = Complex64::new(-1.0, 1.0e-8);
    let below = Complex64::new(-1.0, -1.0e-8);
    let bound = tfcrop_metric_roundoff(ClipMetric::Phase, above, 1.0e-6);
    assert!(f64::from(bound) >= (above.arg() - below.arg()).abs());
    let away_from_cut = tfcrop_metric_roundoff(ClipMetric::Phase, Complex64::new(1.0, 0.0), 1.0e-6);
    assert!((1.0e-6..=1.1e-6).contains(&away_from_cut));
}

fn sample(index: usize, row: usize, corr: usize, chan: usize, real: f64, flag: bool) -> Sample {
    Sample {
        index,
        row,
        corr,
        chan,
        amp: real.abs(),
        real,
        imag: 0.0,
        flag,
        field: 0,
        spw: 0,
        scan: 1,
        ant1: 0,
        ant2: 1,
    }
}

#[test]
fn clip_zero_and_sample_loading_helpers_cover_numeric_families() {
    assert!(is_casa_clip(f64::NAN, true));
    assert!(is_casa_clip(f64::from(f32::EPSILON), true));
    assert!(!is_casa_clip(f64::from(f32::EPSILON) * 2.0, true));
    assert!(is_casa_clip_complex32(Complex32::new(f32::NAN, 0.0), true));
    assert!(is_casa_clip_complex64(Complex64::new(0.0, f64::NAN), true));
    assert!(!is_casa_clip_complex64(Complex64::new(1.0, 0.0), true));
    // Without clipzeros CASA's clip mode flags NaNs alone (#997 F4).
    assert!(is_casa_clip(f64::NAN, false));
    assert!(!is_casa_clip(0.0, false));
    assert!(is_casa_clip_complex32(Complex32::new(0.0, f32::NAN), false));
    assert!(!is_casa_clip_complex64(Complex64::new(0.0, 0.0), false));

    let flags = ArrayD::from_elem(IxDyn(&[2, 3]), false);
    let mut changed = Vec::new();
    let c64 = ArrayValue::Complex64(
        array![
            [
                Complex64::new(0.0, 0.0),
                Complex64::new(2.0, 0.0),
                Complex64::new(f64::NAN, 0.0)
            ],
            [
                Complex64::new(3.0, 0.0),
                Complex64::new(0.0, 0.0),
                Complex64::new(4.0, 0.0)
            ]
        ]
        .into_dyn(),
    );
    apply_clip_array(
        &c64,
        &flags,
        Some(&[0, 2]),
        CLIP_ZEROS,
        "<test>",
        4,
        &mut changed,
    )
    .expect("clip complex64");
    assert_eq!(changed, vec![(0, 0), (0, 2)]);
    changed.clear();
    apply_clip_array(
        &c64,
        &flags,
        Some(&[0, 2]),
        ClipOptions {
            zeros: false,
            ..CLIP_ZEROS
        },
        "<test>",
        4,
        &mut changed,
    )
    .expect("clip complex64 NaNs");
    assert_eq!(changed, vec![(0, 2)]);

    changed.clear();
    let flags_2x2 = ArrayD::from_elem(IxDyn(&[2, 2]), false);
    let f32s = ArrayValue::Float32(array![[0.0_f32, 1.0], [f32::EPSILON, 2.0]].into_dyn());
    apply_clip_array(
        &f32s,
        &flags_2x2,
        None,
        CLIP_ZEROS,
        "<test>",
        5,
        &mut changed,
    )
    .expect("clip float32");
    assert_eq!(changed, vec![(0, 0), (1, 0)]);

    changed.clear();
    let f64s = ArrayValue::Float64(array![[0.0_f64, 5.0], [f64::NAN, 6.0]].into_dyn());
    apply_clip_array(
        &f64s,
        &flags_2x2,
        None,
        CLIP_ZEROS,
        "<test>",
        6,
        &mut changed,
    )
    .expect("clip float64");
    assert_eq!(changed, vec![(0, 0), (1, 0)]);

    let mut samples = Vec::new();
    let metadata = RowSampleMetadata {
        row: 9,
        field: 2,
        spw: 3,
        scan: 4,
        ant1: 5,
        ant2: 6,
    };
    push_row_samples(
        &c64,
        &flags,
        &[1, 2],
        metadata,
        "<test>",
        "DATA",
        &mut samples,
    )
    .expect("push complex64");
    push_row_samples(
        &f32s,
        &flags_2x2,
        &[0],
        metadata,
        "<test>",
        "DATA",
        &mut samples,
    )
    .expect("push float32");
    push_row_samples(
        &f64s,
        &flags_2x2,
        &[1],
        metadata,
        "<test>",
        "DATA",
        &mut samples,
    )
    .expect("push float64");
    assert_eq!(samples.len(), 8);
    assert!(
        samples
            .iter()
            .any(|s| s.field == 2 && s.spw == 3 && s.amp == 5.0)
    );
}

#[test]
fn tfcrop_cubic_fits_narrow_support_at_high_channels() {
    let mut data = [0.0; 103];
    let mut flag = [true; 103];
    let mut fitted = [0.0; 103];
    data[99..].copy_from_slice(&[0.25, 1.75, 0.125, 0.5]);
    flag[99..].fill(false);
    tfcrop_poly_fit(&data, &flag, &mut fitted, 99, 102, 3, true);

    // C++ casacore LinearFit followed by CASA's Float accumulation.
    let expected = [0.201156, 1.80258, 0.216449, 0.567763];
    for (actual, expected) in fitted[99..].iter().zip(expected) {
        assert!((actual - expected).abs() < 2.0e-5, "{actual} != {expected}");
    }
}

#[test]
fn tfcrop_fitting_helpers_cover_zero_interpolation_and_fallbacks() {
    let data = [1.0, 0.0, 3.0, 4.0, 40.0, 6.0, 7.0, 8.0, 9.0];
    let mut flags = vec![false; data.len()];
    let fit = tfcrop_fit_piecewise_poly(&data, &mut flags, 7, 4, false);
    assert_eq!(fit.len(), data.len());
    assert!(fit.iter().all(|value| value.is_finite()));
    assert!(flags.iter().any(|flag| *flag));

    let mut all_flagged_fit = vec![0.0; 3];
    tfcrop_line_fit(
        &[2.0, 2.0, 2.0],
        &[true, true, true],
        &mut all_flagged_fit,
        0,
        2,
        false,
    );
    assert_eq!(all_flagged_fit, vec![0.0, 0.0, 0.0]);

    let mut fallback_fit = vec![0.0; 3];
    tfcrop_poly_fit(
        &[1.0, 2.0, 3.0],
        &[false, true, true],
        &mut fallback_fit,
        0,
        2,
        3,
        false,
    );
    assert_eq!(fallback_fit, vec![1.0, 1.0, 1.0]);
    assert_eq!(tfcrop_mean(&[1.0, 3.0], &[false, false]), 2.0);
    assert_eq!(
        tfcrop_std_about_mean(&[1.0, 3.0], &[false, false], 2.0),
        1.0
    );
    assert_eq!(
        tfcrop_std_about_fit(&[1.0, 3.0], &[false, false], &[1.0, 1.0]),
        2.0_f32.sqrt()
    );
    assert_eq!(median(Vec::new()), 0.0);
    assert_eq!(median(vec![1.0, 4.0, 2.0, 3.0]), 2.5);
}

#[test]
fn rflag_thresholds_and_group_flags_cover_time_and_spectral_paths() {
    let samples = vec![
        sample(0, 0, 0, 0, 1.0, false),
        sample(1, 0, 0, 1, 1.0, false),
        sample(2, 1, 0, 0, 1.0, false),
        sample(3, 1, 0, 1, 1.0, false),
        sample(4, 2, 0, 0, 10.0, false),
        sample(5, 2, 0, 1, 1.0, false),
        sample(6, 0, 1, 0, 2.0, true),
        sample(7, 0, 1, 1, 2.0, true),
    ];
    let groups = group_samples(&samples)
        .into_values()
        .map(|group| (AutoflagChunk::default(), group))
        .collect::<Vec<_>>();
    let request = FlagDataRequest {
        timedev: Some(1.0),
        freqdev: Some(2.0),
        timedevscale: 1.0,
        freqdevscale: 1.0,
        spectralmax: 0.1,
        spectralmin: 0.0,
        ..FlagDataRequest::default()
    };
    let thresholds = rflag_threshold_maps_for_groups(&groups, &request, RflagContext::default())
        .expect("rflag thresholds")
        .scaled(&request, false);
    assert_eq!(thresholds.representative_pair(), (1.0, 2.0));
    // CASA scales a given threshold as it scales a computed one.
    let scaled_request = FlagDataRequest {
        timedevscale: 5.0,
        freqdevscale: 3.0,
        ..request.clone()
    };
    let scaled = rflag_threshold_maps_for_groups(&groups, &scaled_request, RflagContext::default())
        .expect("scaled rflag thresholds")
        .scaled(&scaled_request, false);
    assert_eq!(scaled.representative_pair(), (5.0, 6.0));
    assert_eq!(
        format_threshold_map(&thresholds.timedev).get("field0_spw0"),
        Some(&1.0)
    );

    let mut modified = vec![false; samples.len()];
    let mut time_flags = vec![false; samples.len()];
    let mut freq_flags = vec![false; samples.len()];
    for (_, group) in &groups {
        let mut masks = TfcropFitMasks {
            modified: &mut modified,
            freq_flags: &mut freq_flags,
            time_flags: &mut time_flags,
        };
        rflag_group_flags(
            None,
            group,
            &thresholds,
            &request,
            RflagContext::default(),
            &mut Vec::new(),
            &mut masks,
        )
        .expect("rflag group flags");
    }
    assert!(time_flags[0] || time_flags[2] || time_flags[4]);
    assert!(freq_flags[4] || freq_flags[5]);
    assert!(!freq_flags[6]);

    assert_eq!(
        rflag_time_windows(0, 3).collect::<Vec<_>>(),
        Vec::<(usize, usize)>::new()
    );
    assert_eq!(rflag_time_windows(1, 3).collect::<Vec<_>>(), vec![(0, 0)]);
    assert_eq!(
        rflag_time_windows(4, 3).collect::<Vec<_>>(),
        vec![(0, 2), (1, 3)]
    );
    assert_eq!(
        rflag_time_std_total(&samples[6..8], false, RflagRoundoff::Slice(&[])),
        Some(0.0)
    );
    assert_eq!(
        rflag_spectral_stats(&samples[6..8], false, RflagRoundoff::Slice(&[])).sum_weight_real,
        0.0
    );

    let mut histogram = ChannelHistogram::new(2);
    histogram.add(0, 1.0);
    histogram.add(0, 3.0);
    histogram.add(1, 9.0);
    assert!(histogram.threshold().is_finite());
    assert_eq!(representative_threshold(&BTreeMap::new()), f64::INFINITY);
}

#[test]
fn grouping_extension_and_merge_helpers_cover_boundary_cases() {
    let mut samples = Vec::new();
    for row in 0..2 {
        for corr in 0..2 {
            for chan in 0..3 {
                let index = samples.len();
                samples.push(sample(
                    index,
                    row,
                    corr,
                    chan,
                    (row + chan + 1) as f64,
                    false,
                ));
            }
        }
    }
    samples[0].flag = true;
    let groups = group_samples(&samples);
    let mut modified = vec![false; samples.len()];
    modified[1] = true;
    modified[2] = true;
    modified[3] = true;
    modified[4] = true;
    extend_autoflag_grouped(groups.values().map(Vec::as_slice), &samples, &mut modified);
    assert!(modified[0]);
    assert!(modified[5]);

    assert_eq!(samples_by_corr(&samples).len(), 2);
    assert_eq!(samples_by_chan_dense(&samples).len(), 3);
    assert_eq!(sample_row_runs(&samples).len(), 2);
    assert_eq!(unique_sample_rows(&samples), vec![0, 1]);
    assert_eq!(unique_sample_chans(&samples), vec![0, 1, 2]);
    assert_eq!(samples_by_row_chan(&samples).len(), 6);
    assert_eq!(percent_modified(&samples, &modified), 100.0);
    assert_eq!(count_mask(&modified), 12);
    assert_eq!(
        summary_after_flag_mask(&samples, 2, true).flagged_samples,
        3
    );
    assert_eq!(
        flag_mask_counts_by_spw_corr(&modified, &samples).get("spw0_corr0"),
        Some(&6)
    );

    assert!(merge_bool(true, false, FlagMerge::Or));
    assert!(!merge_bool(true, false, FlagMerge::And));
    let source = ArrayD::from_elem(IxDyn(&[1, 2]), true);
    let dest = ArrayD::from_elem(IxDyn(&[1, 2]), false);
    let merged = merge_bool_arrays(&source, &dest, FlagMerge::Replace).expect("merge arrays");
    assert!(merged.iter().all(|flag| *flag));
    let mismatched = ArrayD::from_elem(IxDyn(&[2, 1]), false);
    assert!(merge_bool_arrays(&source, &mismatched, FlagMerge::Or).is_err());
}

#[test]
fn clip_sample_and_threshold_helpers_cover_errors_and_defaults() {
    let flags_rank1 = ArrayD::from_elem(IxDyn(&[2]), false);
    let data_rank2 = ArrayValue::Float32(array![[0.0_f32, 1.0]].into_dyn());
    let mut changed = Vec::new();
    assert!(
        apply_clip_array(
            &data_rank2,
            &flags_rank1,
            None,
            CLIP_ZEROS,
            "<test>",
            12,
            &mut changed
        )
        .is_err()
    );

    let flags_rank2 = ArrayD::from_elem(IxDyn(&[1, 2]), false);
    let data_rank1 = ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[2]), 1.0_f64));
    assert!(
        apply_clip_array(
            &data_rank1,
            &flags_rank2,
            None,
            CLIP_ZEROS,
            "<test>",
            13,
            &mut changed
        )
        .is_err()
    );

    let metadata = RowSampleMetadata {
        row: 14,
        field: 0,
        spw: 0,
        scan: 0,
        ant1: 2,
        ant2: 1,
    };
    let mut samples = Vec::new();
    assert!(
        push_row_samples(
            &data_rank2,
            &flags_rank1,
            &[0],
            metadata,
            "<test>",
            "DATA",
            &mut samples,
        )
        .is_err()
    );
    assert!(
        push_row_samples(
            &data_rank1,
            &flags_rank2,
            &[0],
            metadata,
            "<test>",
            "DATA",
            &mut samples,
        )
        .is_err()
    );

    let strings = ArrayValue::String(ArrayD::from_elem(IxDyn(&[1, 1]), "x".to_string()));
    push_row_samples(
        &strings,
        &flags_rank2,
        &[0],
        metadata,
        "<test>",
        "DATA",
        &mut samples,
    )
    .expect("ignore non-numeric arrays");
    assert!(samples.is_empty());

    let groups = vec![(
        AutoflagChunk::default(),
        vec![
            sample(0, 0, 0, 0, 1.0, false),
            sample(1, 1, 0, 0, 3.0, false),
            sample(2, 2, 0, 0, 5.0, false),
        ],
    )];
    let thresholds = rflag_threshold_maps_for_groups(
        &groups,
        &FlagDataRequest {
            timedev: None,
            freqdev: None,
            timedevscale: 2.0,
            freqdevscale: 3.0,
            ..FlagDataRequest::default()
        },
        RflagContext::default(),
    )
    .expect("rflag thresholds");
    assert!(thresholds.representative_pair().0.is_finite());
    assert!(thresholds.representative_pair().1.is_finite());
    assert_eq!(
        format_threshold_map(&thresholds.timedev)
            .keys()
            .collect::<Vec<_>>(),
        vec![&"field0_spw0".to_string()]
    );
}

#[test]
fn tfcrop_flagger_handles_empty_all_flagged_and_missing_axis_cases() {
    let mut modified = Vec::new();
    let mut direction_flags = Vec::new();
    tfcrop_fit_base_and_flag(
        &[],
        TfcropDirection::Freq,
        1.0,
        &TfcropOptions::default(),
        TfcropArithmetic::default(),
        &mut modified,
        &mut direction_flags,
    );

    let samples = vec![
        sample(0, 0, 0, 0, 1.0, true),
        sample(1, 0, 0, 2, 1.0, true),
        sample(2, 2, 0, 0, 1.0, true),
    ];
    let mut modified = vec![false; samples.len()];
    let mut direction_flags = vec![false; samples.len()];
    tfcrop_fit_base_and_flag(
        &samples,
        TfcropDirection::Time,
        1.0,
        &TfcropOptions::default(),
        TfcropArithmetic::default(),
        &mut modified,
        &mut direction_flags,
    );
    assert_eq!(modified, vec![false; samples.len()]);
    assert_eq!(direction_flags, vec![false; samples.len()]);

    let mut flagged = vec![false, true, false, true];
    let fit = tfcrop_fit_piecewise_poly(&[0.0, 0.0, 2.0, 0.0], &mut flagged, 3, 2, false);
    assert_eq!(fit.len(), 4);
    assert!(fit.iter().all(|value| value.is_finite()));

    let mut all_zero_flags = vec![false, false];
    let all_zero_fit = tfcrop_fit_piecewise_poly(&[0.0, 0.0], &mut all_zero_flags, 1, 1, false);
    assert_eq!(all_zero_fit, vec![0.0, 0.0]);
    assert_eq!(all_zero_flags, vec![true, true]);
}

#[test]
fn rflag_roundoff_storage_is_scoped_per_series_and_clamps_jitter() {
    let mut samples = Vec::new();
    for baseline in 0..2 {
        for t in 0..3 {
            let row = baseline * 3 + t;
            for chan in 0..3 {
                for corr in 0..2 {
                    let mut real = 1.0;
                    if baseline == 0 && t == 1 && corr == 0 && chan == 1 {
                        real += 5.0e-4;
                    } else if baseline == 1
                        && ((t == 0 && corr == 0 && chan == 1)
                            || (t == 1 && corr == 1 && chan == 1))
                    {
                        real = 10.0;
                    }
                    let mut entry = sample(samples.len(), row, corr, chan, real, false);
                    entry.ant1 = 0;
                    entry.ant2 = (baseline + 1) as i32;
                    samples.push(entry);
                }
            }
        }
    }
    let model_roundoff = vec![2.0e-3_f32; samples.len()];
    let groups = group_samples(&samples)
        .into_values()
        .map(|group| (AutoflagChunk::default(), group))
        .collect::<Vec<_>>();
    assert_eq!(groups.len(), 2);

    let planes = samples_by_corr(&groups[0].1);
    let mut roundoff_buf = vec![0.0; samples.len()];
    let stored_series = rflag_projection_series(
        &planes,
        ClipProjection::Stored(0),
        &model_roundoff,
        &mut roundoff_buf,
    )
    .expect("stored projection");
    assert!(stored_series.roundoff.len() <= groups[0].1.len());
    assert_eq!(stored_series.roundoff.len(), 9);
    assert!(stored_series.roundoff.len() < samples.len());

    let stokes_series = rflag_projection_series(
        &planes,
        ClipProjection::Stokes(0, 1, correlation::StokesCombination::Sum),
        &model_roundoff,
        &mut roundoff_buf,
    )
    .expect("stokes I projection");
    assert!(stokes_series.roundoff.len() <= groups[0].1.len());
    assert_eq!(stokes_series.roundoff.len(), 9);
    let max_stored_roundoff = stokes_series
        .roundoff
        .values()
        .copied()
        .fold(0.0_f32, f32::max);
    assert!((max_stored_roundoff - 2.0e-3).abs() <= 1.0e-5);

    let request = FlagDataRequest {
        timedev: Some(1.0e-5),
        freqdev: Some(1.0e-5),
        timedevscale: 1.0,
        freqdevscale: 1.0,
        rflag_window: Some(3),
        ..FlagDataRequest::default()
    };
    let mut modified = vec![false; samples.len()];
    let mut time_flags = vec![false; samples.len()];
    let mut freq_flags = vec![false; samples.len()];
    fit_rflag_chunks(
        &groups,
        &request,
        RflagContext {
            casa_arithmetic: false,
            correlations: None,
            calibration: None,
            model_roundoff: &model_roundoff,
        },
        &mut TfcropFitMasks {
            modified: &mut modified,
            freq_flags: &mut freq_flags,
            time_flags: &mut time_flags,
        },
    )
    .expect("fit rflag chunks with roundoff");

    assert!(
        !modified[..18].iter().any(|&flag| flag),
        "roundoff-bounded jitter should not be flagged"
    );
    let freq_spike_index = samples
        .iter()
        .position(|entry| entry.ant2 == 2 && entry.row == 3 && entry.corr == 0 && entry.chan == 1)
        .expect("freq spike sample");
    let time_spike_index = samples
        .iter()
        .position(|entry| entry.ant2 == 2 && entry.row == 4 && entry.corr == 1 && entry.chan == 1)
        .expect("time spike sample");
    assert!(modified[freq_spike_index] && freq_flags[freq_spike_index]);
    assert!(modified[time_spike_index] && time_flags[time_spike_index]);
}
