// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use casa_core::measures::frequency::MFrequency;

fn test_engine() -> MsCalEngine {
    let observatory =
        casa_core::measures::position::MPosition::new_itrf(-1_601_185.4, -5_041_977.5, 3_554_875.9);
    let direction = casa_core::measures::direction::MDirection::from_angles(
        0.0,
        std::f64::consts::FRAC_PI_4,
        casa_core::measures::direction::DirectionRef::J2000,
    );
    MsCalEngine::from_parts(
        vec![observatory.clone()],
        vec![direction],
        observatory,
        casa_test_support::deterministic_measures_provider(),
    )
}

#[test]
fn parse_cube_axis_value_channel() {
    assert_eq!(
        CubeAxisValue::parse("5", DopplerRef::RADIO).unwrap(),
        CubeAxisValue::Channel(5)
    );
}

#[test]
fn parse_cube_axis_value_frequency() {
    assert_eq!(
        CubeAxisValue::parse("1.1GHz", DopplerRef::RADIO).unwrap(),
        CubeAxisValue::FrequencyHz {
            hz: 1.1e9,
            frame: None,
        }
    );
}

#[test]
fn parse_cube_axis_value_velocity() {
    assert_eq!(
        CubeAxisValue::parse("11991.7km/s", DopplerRef::RADIO).unwrap(),
        CubeAxisValue::VelocityMs {
            ms: 11_991_700.0,
            frame: None,
        }
    );
}

#[test]
fn parse_cube_axis_value_dimensionless_doppler_and_empty_input() {
    assert_eq!(
        CubeAxisValue::parse("0.25", DopplerRef::RADIO).unwrap(),
        CubeAxisValue::Doppler {
            value: 0.25,
            convention: DopplerRef::RADIO,
        }
    );
    assert!(CubeAxisValue::parse("", DopplerRef::RADIO).is_err());
}

#[test]
fn channel_edges_cover_boundary_cases() {
    assert!(channel_edges_from_centers(&[]).is_err());
    assert_eq!(channel_edges_from_centers(&[5.0]).unwrap(), vec![4.5, 5.5]);
    assert_eq!(
        channel_edges_from_centers(&[10.0, 20.0, 30.0]).unwrap(),
        vec![5.0, 15.0, 25.0, 35.0]
    );
}

#[test]
fn axis_mode_helpers_cover_native_interpolation_and_explicit_frames() {
    let mut axis_config = CubeAxisConfig {
        specmode: CubeSpecMode::Cube,
        outframe: FrequencyRef::TOPO,
        veltype: DopplerRef::Z,
        interpolation: CubeInterpolation::Linear,
        rest_frequency_hz: Some(1.25e9),
        start: Some(CubeAxisValue::VelocityMs {
            ms: 1.0,
            frame: Some(FrequencyRef::BARY),
        }),
        width: Some(CubeAxisValue::VelocityMs {
            ms: -2.0,
            frame: Some(FrequencyRef::BARY),
        }),
        selected_band_hz: None,
    };
    let native_axis_config = CubeAxisConfig {
        start: None,
        width: Some(CubeAxisValue::VelocityMs {
            ms: -2.0,
            frame: None,
        }),
        ..axis_config.clone()
    };
    assert!(uses_native_source_interpolation(&native_axis_config));
    assert_eq!(
        effective_output_frequency_ref(FrequencyRef::TOPO, &axis_config).unwrap(),
        FrequencyRef::BARY
    );
    assert_eq!(
        cube_axis_explicit_frame(axis_config.start.as_ref()),
        Some(FrequencyRef::BARY)
    );
    axis_config.width = Some(CubeAxisValue::FrequencyHz {
        hz: 1.0,
        frame: Some(FrequencyRef::LSRK),
    });
    let error = effective_output_frequency_ref(FrequencyRef::TOPO, &axis_config).unwrap_err();
    assert!(error.to_string().contains("disagree"));
    axis_config.specmode = CubeSpecMode::Cubedata;
    assert_eq!(
        effective_output_frequency_ref(FrequencyRef::TOPO, &axis_config).unwrap(),
        FrequencyRef::TOPO
    );
}

#[test]
fn output_selection_helpers_cover_nearest_and_interpolation_cases() {
    assert_eq!(nearest_channel_index(&[], 1.0), None);
    assert_eq!(nearest_channel_index(&[10.0], 12.0), Some(0));
    assert_eq!(nearest_channel_index(&[10.0, 20.0, 30.0], 19.0), Some(1));
    assert_eq!(nearest_channel_index(&[10.0, 20.0, 30.0], 100.0), None);

    let nearest = spectral_interpolation_contributions(
        &[10.0, 20.0, 30.0],
        &[10.0, 20.0, 30.0],
        &[10.0, 10.0, 10.0],
        CubeInterpolation::Nearest,
        19.0,
    );
    assert_eq!(
        nearest,
        vec![CubeChannelContribution {
            source_channel: 1,
            source_frequency_hz: 20.0,
            factor: 1.0,
        }]
    );

    let linear = spectral_interpolation_contributions(
        &[10.0, 20.0, 30.0],
        &[10.0, 20.0, 30.0],
        &[10.0, 10.0, 10.0],
        CubeInterpolation::Linear,
        15.0,
    );
    assert_eq!(linear.len(), 2);

    let single = linear_channel_contributions(&[10.0], &[10.0], &[4.0], 11.0);
    assert_eq!(single.len(), 1);
}

#[test]
fn casa_grid_frequency_channel_map_uses_casa_intermediate_grid_thresholds() {
    let source = (0..12).map(|index| index as f64 * 10.0).collect::<Vec<_>>();
    let linear_direct = vec![5.0, 20.0, 35.0];
    let (grid, channel_map) =
        casa_grid_frequency_channel_map(&source, &linear_direct, &[], CubeInterpolation::Linear);
    assert_eq!(grid, linear_direct);
    assert_eq!(channel_map, vec![Some(0), Some(1), Some(2)]);

    let linear_threshold = vec![0.0, 20.0, 40.0];
    let (grid, channel_map) =
        casa_grid_frequency_channel_map(&source, &linear_threshold, &[], CubeInterpolation::Linear);
    assert_eq!(grid, linear_threshold);
    assert_eq!(channel_map, vec![Some(0), Some(1), Some(2)]);

    let linear_wide = vec![0.0, 25.0, 50.0];
    let (grid, channel_map) =
        casa_grid_frequency_channel_map(&source, &linear_wide, &[], CubeInterpolation::Linear);
    assert_ne!(grid, linear_wide);
    assert!(grid.len() > linear_wide.len());
    assert!(channel_map.iter().any(Option::is_some));

    let cubic_direct = vec![0.0, 40.0, 80.0];
    let (grid, channel_map) =
        casa_grid_frequency_channel_map(&source, &cubic_direct, &[], CubeInterpolation::Cubic);
    assert_eq!(grid, cubic_direct);
    assert_eq!(channel_map, vec![Some(0), Some(1), Some(2)]);
}

#[test]
fn parse_rest_frequency_supports_bare_mhz_and_frequency_units() {
    assert_eq!(
        parse_rest_frequency_hz("1420.405752").unwrap(),
        1.420405752e9
    );
    assert_eq!(parse_rest_frequency_hz("1.25GHz").unwrap(), 1.25e9);
}

#[test]
fn velocity_ms_from_frequency_matches_radio_definition() {
    let velocity_ms = velocity_ms_from_frequency_hz(1.2e9, 1.25e9, DopplerRef::RADIO).unwrap();
    assert!((velocity_ms - 11_991_698.32).abs() < 1.0);
}

#[test]
fn resolve_contiguous_channel_selection_defaults_to_full_spw() {
    let resolved = resolve_contiguous_channel_selection(&[1.0, 2.0, 3.0], None, None).unwrap();
    assert_eq!(
        resolved,
        ResolvedChannelSelection {
            indices: vec![0, 1, 2],
            frequencies_hz: vec![1.0, 2.0, 3.0],
        }
    );
}

#[test]
fn resolve_contiguous_channel_selection_checks_bounds() {
    let error =
        resolve_contiguous_channel_selection(&[1.0, 2.0, 3.0], Some(2), Some(2)).unwrap_err();
    assert!(error.to_string().contains("outside spectral window"));
}

#[test]
fn resolve_channel_selector_selection_supports_gaps_and_stride() {
    let selector = ChannelSelection {
        segments: vec![
            crate::selection::syntax::ChannelSelectionSegment {
                start: 4,
                end: 9,
                stride: 1,
            },
            crate::selection::syntax::ChannelSelectionSegment {
                start: 12,
                end: 14,
                stride: 1,
            },
            crate::selection::syntax::ChannelSelectionSegment {
                start: 18,
                end: 22,
                stride: 2,
            },
        ],
    };
    let all = (0..24).map(|channel| channel as f64).collect::<Vec<_>>();
    let resolved = resolve_channel_selector_selection(&all, &selector).unwrap();
    assert_eq!(
        resolved.indices,
        vec![4, 5, 6, 7, 8, 9, 12, 13, 14, 18, 20, 22]
    );
    assert_eq!(
        resolved.frequencies_hz,
        vec![
            4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 12.0, 13.0, 14.0, 18.0, 20.0, 22.0
        ]
    );
}

#[test]
fn linear_contributions_split_between_adjacent_ascending_channels() {
    let contributions = linear_channel_contributions(
        &[10.0, 20.0, 30.0],
        &[10.0, 20.0, 30.0],
        &[10.0, 10.0, 10.0],
        15.0,
    );
    assert_eq!(
        contributions,
        vec![
            CubeChannelContribution {
                source_channel: 0,
                source_frequency_hz: 10.0,
                factor: 0.5,
            },
            CubeChannelContribution {
                source_channel: 1,
                source_frequency_hz: 20.0,
                factor: 0.5,
            },
        ]
    );
}

#[test]
fn linear_contributions_split_between_adjacent_descending_channels() {
    let contributions = linear_channel_contributions(
        &[30.0, 20.0, 10.0],
        &[30.0, 20.0, 10.0],
        &[10.0, 10.0, 10.0],
        15.0,
    );
    assert_eq!(
        contributions,
        vec![
            CubeChannelContribution {
                source_channel: 1,
                source_frequency_hz: 20.0,
                factor: 0.5,
            },
            CubeChannelContribution {
                source_channel: 2,
                source_frequency_hz: 10.0,
                factor: 0.5,
            },
        ]
    );
}

#[test]
fn output_channel_contributions_use_transformed_query_frequencies() {
    let contributions = build_output_channel_contributions(
        &[100.0, 200.0, 300.0],
        &[100.0, 200.0, 300.0],
        &[100.0, 100.0, 100.0],
        &[100.0, 200.0, 300.0],
        CubeInterpolation::Linear,
    );
    assert_eq!(
        contributions,
        vec![
            vec![CubeChannelContribution {
                source_channel: 0,
                source_frequency_hz: 100.0,
                factor: 1.0,
            }],
            vec![CubeChannelContribution {
                source_channel: 1,
                source_frequency_hz: 200.0,
                factor: 1.0,
            }],
            vec![CubeChannelContribution {
                source_channel: 2,
                source_frequency_hz: 300.0,
                factor: 1.0,
            }],
        ]
    );
}

#[test]
fn linear_contributions_clip_below_lowest_source_channel_edge() {
    let contributions = linear_channel_contributions(
        &[1.0e9, 1.05e9, 1.10e9],
        &[1.0e9, 1.05e9, 1.10e9],
        &[5.0e7, 5.0e7, 5.0e7],
        0.974_988_750_387e9,
    );
    assert!(contributions.is_empty());
}

#[test]
fn linear_contributions_do_not_extrapolate_below_first_channel_center() {
    let contributions = linear_channel_contributions(
        &[1.0e9, 1.05e9, 1.10e9],
        &[1.0e9, 1.05e9, 1.10e9],
        &[5.0e7, 5.0e7, 5.0e7],
        0.999_988_750_387e9,
    );
    assert!(contributions.is_empty());
}

#[test]
fn linear_contributions_tolerate_sub_hz_roundoff_at_low_edge() {
    let contributions = linear_channel_contributions(
        &[
            999_988_750.387_0,
            1_049_988_187.881_7,
            1_099_987_625.376_4,
            1_149_987_062.871_2,
            1_199_986_500.365_9,
        ],
        &[
            999_988_750.387_0,
            1_049_988_187.881_7,
            1_099_987_625.376_4,
            1_149_987_062.871_2,
            1_199_986_500.365_9,
        ],
        &[
            49_999_437.494_7,
            49_999_437.494_7,
            49_999_437.494_7,
            49_999_437.494_7,
            49_999_437.494_7,
        ],
        999_988_749.894_262_6,
    );
    assert!(contributions.is_empty());
}

#[test]
fn linear_model_contributions_clip_below_first_channel_center_within_edge() {
    let contributions = linear_channel_model_contributions(
        &[1.0e9, 1.05e9, 1.10e9],
        &[1.0e9, 1.05e9, 1.10e9],
        &[5.0e7, 5.0e7, 5.0e7],
        0.999_988_750_387e9,
    );
    assert_eq!(
        contributions,
        vec![CubeChannelContribution {
            source_channel: 0,
            source_frequency_hz: 1.0e9,
            factor: 1.0,
        }]
    );
}

#[test]
fn linear_model_contributions_clip_above_highest_channel_center_within_edge() {
    let contributions = linear_channel_model_contributions(
        &[1.0e9, 1.05e9, 1.10e9],
        &[1.0e9, 1.05e9, 1.10e9],
        &[5.0e7, 5.0e7, 5.0e7],
        1.100_011_249_613e9,
    );
    assert_eq!(
        contributions,
        vec![CubeChannelContribution {
            source_channel: 2,
            source_frequency_hz: 1.10e9,
            factor: 1.0,
        }]
    );
}

#[test]
fn casa_channel_mode_output_centers_shift_for_multi_channel_width() {
    let all: Vec<_> = (0..20).map(|index| 1.0e9 + index as f64 * 50.0e6).collect();
    let widths = vec![50.0e6; all.len()];
    let centers =
        casa_transformed_channel_mode_output_centers(&all, &widths, 0, 2, Some(3)).unwrap();
    assert_eq!(centers, vec![1.025e9, 1.125e9, 1.225e9]);
}

#[test]
fn casa_channel_mode_output_centers_support_negative_multi_channel_width() {
    let all: Vec<_> = (0..20).map(|index| 1.0e9 + index as f64 * 50.0e6).collect();
    let widths = vec![50.0e6; all.len()];
    let centers =
        casa_transformed_channel_mode_output_centers(&all, &widths, 9, -2, Some(2)).unwrap();
    assert_eq!(centers, vec![1.425e9, 1.325e9]);
}

#[test]
fn cubedata_axis_lands_on_casa_frequencies_for_inexact_source_channels() {
    // refim_point.ms stores channel 0 one ulp below 1 GHz. CASA's bound
    // arithmetic gives chanFreq[0] = 1 GHz and chanFreq[1] − chanFreq[0]
    // = 50 MHz exactly, so the linear image axis is 1 GHz + 50 MHz·k, its
    // last plane coincides with the last source channel and every plane
    // interpolates inside the source range.
    let mut all: Vec<_> = (0..20).map(|index| 1.0e9 + index as f64 * 50.0e6).collect();
    all[0] = 999_999_999.999_999_9;
    let widths = vec![50.0e6; all.len()];
    let axis = casa_image_spectral_axis_hz(
        casa_transformed_channel_mode_output_centers(&all, &widths, 0, 1, Some(20)).unwrap(),
        DopplerRef::RADIO,
    );
    assert_eq!(axis[0], 1.0e9);
    assert_eq!(axis[1] - axis[0], 50.0e6);
    assert_eq!(axis[19], all[19]);
}

#[test]
fn channel_mode_single_output_width_uses_selected_channel_bin() {
    let all: Vec<_> = (0..20).map(|index| 1.0e9 + index as f64 * 50.0e6).collect();
    let widths = vec![50.0e6; all.len()];
    assert_eq!(
        channel_mode_single_output_width_hz(&all, &widths, 0, 1).unwrap(),
        50.0e6
    );
    assert_eq!(
        channel_mode_single_output_width_hz(&all, &widths, 0, 2).unwrap(),
        100.0e6
    );
    assert_eq!(
        channel_mode_single_output_width_hz(&all, &widths, 9, -2).unwrap(),
        100.0e6
    );
}

#[test]
fn casa_transformed_channel_mode_output_centers_use_uniform_output_spacing() {
    let centers = casa_transformed_channel_mode_output_centers(
        &[10.0, 11.0, 12.001, 13.003],
        &[1.0, 1.001, 1.002, 1.003],
        1,
        1,
        Some(3),
    )
    .unwrap();
    assert_eq!(centers, vec![10.9995, 11.9995, 12.9995]);
}

#[test]
fn casa_transformed_channel_mode_output_centers_round_like_casa_regrid_bounds() {
    // refim_point.ms: native channel 0 in LSRK at the first row time and
    // its transformed width. CASA's bound sequence puts output channel 0
    // one ulp below the source frequency (tclean's image reference value
    // 999988750.387257218 Hz), so the rows at that time fall outside
    // the interpolation range and CASA leaves the channel blank. The
    // bounds are casacore `lDouble` (80-bit on x86-64 CASA, 64-bit on
    // Apple arm64), so these bitwise values hold for the local arm64
    // oracle only.
    let first_hz = 999_988_750.387_257_3;
    let width_hz = 49_999_437.519_362_69;
    let frequencies_hz = (0..20)
        .map(|channel| first_hz + f64::from(channel) * width_hz)
        .collect::<Vec<_>>();
    let centers = casa_transformed_channel_mode_output_centers(
        &frequencies_hz,
        &[width_hz; 20],
        0,
        1,
        Some(20),
    )
    .unwrap();
    assert_eq!(centers[0], 999_988_750.387_257_2);
    assert!(centers[0] < first_hz);
    let axis = casa_image_spectral_axis_hz(centers, DopplerRef::RADIO);
    assert_eq!(axis[19], 1_949_978_063.255_148_4);
}

#[test]
fn casa_transformed_channel_mode_output_centers_support_negative_width() {
    let centers = casa_transformed_channel_mode_output_centers(
        &[1.0e9, 1.05e9, 1.10e9, 1.15e9, 1.20e9],
        &[5.0e7; 5],
        3,
        -1,
        Some(3),
    )
    .unwrap();
    assert_eq!(centers, vec![1.15e9, 1.10e9, 1.05e9]);
}

#[test]
fn channel_mode_image_axis_is_linear_from_first_two_regrid_centres() {
    // CASA grids a channel-mode cube on the linear SpectralCoordinate
    // that `SynthesisParamsImage::buildCoordinateSystemCore` builds from
    // chanFreq[0] and chanFreq[1] − chanFreq[0] (`FTMachine::initMaps`
    // takes plane k at `toWorld(k)`), not on every regrid centre. With a
    // 1/3 MHz channel the `regridChanBounds` stepping accumulates
    // rounding that the linear axis does not share from plane 2 on, in
    // ascending order and after either reversal. The regrid bounds are
    // casacore `lDouble`, which is 80-bit on x86-64 CASA and 64-bit on
    // Apple arm64, so the bitwise stepped centres match the local arm64
    // oracle only; the linear law holds on both.
    let engine = test_engine();
    let width_hz = 1.0e6 / 3.0;
    let ascending_hz = (0..16)
        .map(|channel| 1.0e9 + f64::from(channel) * width_hz)
        .collect::<Vec<_>>();
    let descending_hz = ascending_hz.iter().rev().copied().collect::<Vec<_>>();
    let widths_hz = vec![width_hz; ascending_hz.len()];
    for (source_hz, start, width) in [
        (&ascending_hz, 0, 1),
        (&ascending_hz, 15, -1),
        (&descending_hz, 0, 1),
    ] {
        let axis_config = CubeAxisConfig {
            specmode: CubeSpecMode::Cubedata,
            start: Some(CubeAxisValue::Channel(start)),
            width: Some(CubeAxisValue::Channel(width)),
            ..CubeAxisConfig::default()
        };
        let build = |axis_config: &CubeAxisConfig| {
            CubeSpectralSetup::for_casa_cube_axis(
                FrequencyRef::TOPO,
                source_hz,
                &widths_hz,
                Some(source_hz.len()),
                axis_config,
                59_000.0 * 86_400.0,
                0,
                None,
                &engine,
            )
            .unwrap()
            .output_channel_frequencies_hz
        };
        let chan_freqs_hz = casa_transformed_channel_mode_output_centers(
            source_hz,
            &widths_hz,
            start,
            width,
            Some(source_hz.len()),
        )
        .unwrap();
        let increment_hz = chan_freqs_hz[1] - chan_freqs_hz[0];

        let axis_hz = build(&axis_config);
        for (plane, frequency_hz) in axis_hz.iter().enumerate() {
            assert_eq!(
                *frequency_hz,
                chan_freqs_hz[0] + plane as f64 * increment_hz,
                "start {start} width {width} plane {plane}"
            );
        }
        assert!(
            (2..axis_hz.len()).any(|plane| axis_hz[plane] != chan_freqs_hz[plane]),
            "start {start} width {width}: stepped and linear axes must differ"
        );

        // A non-linear velocity convention makes CASA's coordinate
        // tabular on chanFreq itself.
        let tabular_config = CubeAxisConfig {
            veltype: DopplerRef::Z,
            ..axis_config.clone()
        };
        assert_eq!(build(&tabular_config), chan_freqs_hz);
    }
}

#[test]
fn casa_regridding_input_widths_preserve_single_channel_width() {
    let widths_hz = casa_regridding_input_widths(&[1.420_405_752e9], &[2.5e6]).unwrap();
    assert_eq!(widths_hz, vec![2.5e6]);
}

#[test]
fn casa_regridding_input_widths_preserve_measured_channel_widths() {
    let widths_hz =
        casa_regridding_input_widths(&[1.0e9, 1.000_001e9, 1.000_002e9], &[900.0, -950.0, 975.0])
            .unwrap();
    assert_eq!(widths_hz, vec![900.0, 950.0, 975.0]);
}

#[test]
fn explicit_measure_frame_overrides_outframe() {
    let axis_config = CubeAxisConfig {
        specmode: CubeSpecMode::Cube,
        outframe: FrequencyRef::TOPO,
        veltype: DopplerRef::RADIO,
        interpolation: CubeInterpolation::Linear,
        rest_frequency_hz: Some(1.25e9),
        start: Some(CubeAxisValue::VelocityMs {
            ms: 11_977_600.0,
            frame: Some(FrequencyRef::BARY),
        }),
        width: None,
        selected_band_hz: None,
    };
    assert_eq!(
        effective_output_frequency_ref(FrequencyRef::TOPO, &axis_config).unwrap(),
        FrequencyRef::BARY
    );
}

#[test]
fn conflicting_measure_frames_are_rejected() {
    let axis_config = CubeAxisConfig {
        specmode: CubeSpecMode::Cube,
        outframe: FrequencyRef::LSRK,
        veltype: DopplerRef::RADIO,
        interpolation: CubeInterpolation::Linear,
        rest_frequency_hz: Some(1.25e9),
        start: Some(CubeAxisValue::VelocityMs {
            ms: 11_977_600.0,
            frame: Some(FrequencyRef::BARY),
        }),
        width: Some(CubeAxisValue::VelocityMs {
            ms: 11_991_700.0,
            frame: Some(FrequencyRef::TOPO),
        }),
        selected_band_hz: None,
    };
    let error = effective_output_frequency_ref(FrequencyRef::TOPO, &axis_config).unwrap_err();
    assert!(error.to_string().contains("disagree"));
}

#[test]
fn cubedata_mode_uses_native_source_frequency_frame() {
    let axis_config = CubeAxisConfig {
        specmode: CubeSpecMode::Cubedata,
        outframe: FrequencyRef::LSRK,
        veltype: DopplerRef::RADIO,
        interpolation: CubeInterpolation::Linear,
        rest_frequency_hz: Some(1.25e9),
        start: Some(CubeAxisValue::VelocityMs {
            ms: 11_977_600.0,
            frame: Some(FrequencyRef::BARY),
        }),
        width: None,
        selected_band_hz: None,
    };
    assert_eq!(
        effective_output_frequency_ref(FrequencyRef::TOPO, &axis_config).unwrap(),
        FrequencyRef::TOPO
    );
}

#[test]
fn asymmetric_frame_conversion_matches_manual_direct_hops() {
    let measures = casa_test_support::deterministic_measures_provider();
    let source_frame = MeasFrame::new()
        .with_measures(measures.clone())
        .with_epoch(casa_core::measures::epoch::MEpoch::from_mjd(
            59_000.0,
            casa_core::measures::epoch::EpochRef::UTC,
        ))
        .with_position(casa_core::measures::position::MPosition::new_itrf(
            -1_601_185.4,
            -5_041_977.5,
            3_554_875.9,
        ))
        .with_direction(casa_core::measures::direction::MDirection::from_angles(
            1.0,
            0.5,
            casa_core::measures::direction::DirectionRef::J2000,
        ));
    let target_frame = MeasFrame::new()
        .with_measures(measures)
        .with_epoch(casa_core::measures::epoch::MEpoch::from_mjd(
            59_001.0,
            casa_core::measures::epoch::EpochRef::UTC,
        ))
        .with_position(casa_core::measures::position::MPosition::new_itrf(
            -1_601_185.4,
            -5_041_977.5,
            3_554_875.9,
        ))
        .with_direction(casa_core::measures::direction::MDirection::from_angles(
            1.1,
            0.55,
            casa_core::measures::direction::DirectionRef::J2000,
        ));
    let frequency_hz = 6.667_582_296e9;

    let converted = convert_frequency_to_frame_with_frames(
        FrequencyRef::TOPO,
        FrequencyRef::LSRK,
        frequency_hz,
        Some(&source_frame),
        Some(&target_frame),
    )
    .unwrap();

    let manual = MFrequency::new(frequency_hz, FrequencyRef::TOPO)
        .convert_to(FrequencyRef::GEO, &source_frame)
        .unwrap()
        .convert_to(FrequencyRef::BARY, &source_frame)
        .unwrap()
        .convert_to(FrequencyRef::LSRK, &target_frame)
        .unwrap()
        .hz();

    let source_only = convert_frequency_to_frame_with_frame(
        FrequencyRef::TOPO,
        FrequencyRef::LSRK,
        frequency_hz,
        Some(&source_frame),
    )
    .unwrap();

    assert_eq!(converted.to_bits(), manual.to_bits());
    assert!((converted - source_only).abs() > 1.0e-6);
}

#[test]
fn cross_frame_frequency_conversion_requires_structured_frame_context() {
    let missing_source = convert_frequency_to_frame_with_frames(
        FrequencyRef::TOPO,
        FrequencyRef::LSRK,
        6.667_582_296e9,
        None,
        Some(&MeasFrame::new()),
    )
    .unwrap_err();
    assert!(
        missing_source
            .to_string()
            .contains("missing frame data: source frame")
    );

    let missing_target = convert_frequency_to_frame_with_frames(
        FrequencyRef::TOPO,
        FrequencyRef::LSRK,
        6.667_582_296e9,
        Some(&MeasFrame::new()),
        None,
    )
    .unwrap_err();
    assert!(
        missing_target
            .to_string()
            .contains("missing frame data: target frame")
    );

    assert_eq!(
        convert_frequency_to_frame_with_frames(
            FrequencyRef::LSRK,
            FrequencyRef::LSRK,
            6.667_582_296e9,
            None,
            None,
        )
        .unwrap(),
        6.667_582_296e9
    );
}

#[test]
fn parse_cube_axis_value_rejects_empty_and_unknown_units() {
    let empty = CubeAxisValue::parse("   ", DopplerRef::RADIO).unwrap_err();
    assert!(empty.to_string().contains("empty"));

    let invalid = CubeAxisValue::parse("1kg", DopplerRef::RADIO).unwrap_err();
    assert!(invalid.to_string().contains("neither a channel id"));
}

#[test]
fn casa_channel_mode_output_centers_reject_zero_width_and_mismatched_arrays() {
    let all = vec![1.0e9, 1.05e9, 1.10e9];
    let zero_width =
        casa_transformed_channel_mode_output_centers(&all, &[5.0e7; 3], 0, 0, Some(1)).unwrap_err();
    assert!(zero_width.to_string().contains("must not be zero"));

    let mismatched =
        casa_transformed_channel_mode_output_centers(&all, &[5.0e7; 2], 0, 1, Some(1)).unwrap_err();
    assert!(
        mismatched
            .to_string()
            .contains("matching frequency/width arrays")
    );
}

#[test]
fn for_casa_cube_axis_rejects_zero_output_channels() {
    let engine = test_engine();
    let error = CubeSpectralSetup::for_casa_cube_axis(
        FrequencyRef::TOPO,
        &[1.0e9, 1.05e9],
        &[5.0e7, 5.0e7],
        Some(0),
        &CubeAxisConfig::default(),
        59_000.0 * 86_400.0,
        0,
        None,
        &engine,
    )
    .unwrap_err();
    assert!(error.to_string().contains("must be positive"));
}

#[test]
fn for_casa_cube_channel_axis_keeps_single_output_channel_width() {
    let engine = test_engine();
    let axis_config = CubeAxisConfig {
        specmode: CubeSpecMode::Cubedata,
        start: Some(CubeAxisValue::Channel(0)),
        width: Some(CubeAxisValue::Channel(1)),
        ..CubeAxisConfig::default()
    };
    let setup = CubeSpectralSetup::for_casa_cube_axis(
        FrequencyRef::TOPO,
        &[1.0e9, 1.05e9, 1.10e9],
        &[5.0e7, 5.0e7, 5.0e7],
        Some(1),
        &axis_config,
        59_000.0 * 86_400.0,
        0,
        None,
        &engine,
    )
    .unwrap();
    assert_eq!(setup.output_channel_frequencies_hz, vec![1.0e9]);
    assert_eq!(setup.output_channel_widths_hz, vec![5.0e7]);
}

#[test]
fn regrid_request_follows_casa_spec_mode_and_veltype() {
    let config = |start, width, veltype| CubeAxisConfig {
        veltype,
        rest_frequency_hz: Some(1.25e9),
        start,
        width,
        ..CubeAxisConfig::default()
    };
    let velocity = Some(CubeAxisValue::VelocityMs {
        ms: 1.0e6,
        frame: None,
    });
    let frequency = Some(CubeAxisValue::FrequencyHz {
        hz: 1.1e9,
        frame: None,
    });
    let quantity = |start, width, veltype| {
        casa_regrid_request(&config(start, width, veltype), Some(3))
            .unwrap()
            .quantity
    };
    assert_eq!(
        quantity(velocity.clone(), None, DopplerRef::RADIO),
        regrid::RegridQuantity::RadioVelocity
    );
    assert_eq!(
        quantity(None, velocity.clone(), DopplerRef::Z),
        regrid::RegridQuantity::OpticalVelocity
    );
    // `convertGridPars` regrids every other convention in radio velocity.
    assert_eq!(
        quantity(velocity.clone(), None, DopplerRef::BETA),
        regrid::RegridQuantity::RadioVelocity
    );
    assert_eq!(
        quantity(frequency.clone(), None, DopplerRef::Z),
        regrid::RegridQuantity::Frequency
    );
    for (start, width) in [
        (frequency.clone(), velocity.clone()),
        (Some(CubeAxisValue::Channel(2)), frequency.clone()),
    ] {
        let error =
            casa_regrid_request(&config(start, width, DopplerRef::RADIO), Some(3)).unwrap_err();
        assert!(error.to_string().contains("Cannot mix start and width"));
    }
    let doppler = Some(CubeAxisValue::Doppler {
        value: 0.1,
        convention: DopplerRef::RADIO,
    });
    let request = casa_regrid_request(&config(doppler, None, DopplerRef::Z), Some(3)).unwrap();
    let start = request.start.unwrap();
    assert!((start - SPEED_OF_LIGHT_M_S * (1.0 / 0.9 - 1.0)).abs() < 1e-6);
    let missing_rest = CubeAxisConfig {
        rest_frequency_hz: None,
        ..config(velocity, None, DopplerRef::RADIO)
    };
    let error = casa_regrid_request(&missing_rest, Some(3)).unwrap_err();
    assert!(error.to_string().contains("requires a rest frequency"));
}

#[test]
fn for_casa_cube_velocity_axis_starts_where_casa_does() {
    // test_cube_11 and, at test_cube_12's start, its TOPO counterpart:
    // CASA wrote 1.2 GHz (the band centre clamped into the data) and
    // 1200058783.6669328 Hz (the start kept exactly).
    let engine = test_engine();
    let mut source_hz = (0..20)
        .map(|channel| 1.0e9 + f64::from(channel) * 5.0e7)
        .collect::<Vec<_>>();
    source_hz[0] = 999_999_999.999_999_9;
    source_hz[1] = 1_049_999_999.999_999_9;
    for (start_ms, first_hz) in [
        (11_991_700.0, 1.2e9),
        (11_977_600.0, 1_200_058_783.666_932_8),
    ] {
        let axis_config = CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            rest_frequency_hz: Some(1.25e9),
            start: Some(CubeAxisValue::VelocityMs {
                ms: start_ms,
                frame: None,
            }),
            ..CubeAxisConfig::default()
        };
        let setup = CubeSpectralSetup::for_casa_cube_axis(
            FrequencyRef::TOPO,
            &source_hz,
            &[5.0e7; 20],
            Some(10),
            &axis_config,
            59_000.0 * 86_400.0,
            0,
            None,
            &engine,
        )
        .unwrap();
        assert_eq!(
            setup.output_channel_frequencies_hz,
            linear_spectral_axis_hz(first_hz, -5.0e7, 10)
        );
    }
}

#[test]
fn an_optical_image_uses_the_last_table_step_past_the_stored_entries() {
    let table = regrid::CasaChannelFrequencies {
        centres_hz: vec![140.0, 120.0, 107.0, 100.0],
        steps_hz: vec![-20.0, -20.0, -13.0, -7.0],
    };
    let coordinates = |nchan| {
        let request = regrid::RegridRequest {
            quantity: regrid::RegridQuantity::OpticalVelocity,
            start: None,
            width: None,
            nchan,
            rest_frequency_hz: 120.0,
        };
        casa_regridded_image_axis_hz(&table, &request, DopplerRef::Z)
    };
    assert_eq!(coordinates(None), [140.0, 120.0, 107.0, 100.0]);
    assert_eq!(
        coordinates(Some(8)),
        [140.0, 120.0, 107.0, 100.0, 93.0, 86.0, 79.0, 72.0]
    );
}

#[test]
fn for_casa_cube_axis_with_unset_start_begins_at_the_selected_band() {
    // test_cube_23: spw='0:4~13', start=''; CASA's cube starts at 1.2 GHz.
    let engine = test_engine();
    let source_hz = (0..20)
        .map(|channel| 1.0e9 + f64::from(channel) * 5.0e7)
        .collect::<Vec<_>>();
    for width in [None, Some(CubeAxisValue::Channel(1))] {
        let axis_config = CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            width,
            selected_band_hz: Some([1.175e9, 1.675e9]),
            ..CubeAxisConfig::default()
        };
        let setup = CubeSpectralSetup::for_casa_cube_axis(
            FrequencyRef::TOPO,
            &source_hz,
            &[5.0e7; 20],
            Some(10),
            &axis_config,
            59_000.0 * 86_400.0,
            0,
            None,
            &engine,
        )
        .unwrap();
        assert_eq!(
            setup.output_channel_frequencies_hz,
            linear_spectral_axis_hz(1.2e9, 5.0e7, 10)
        );
    }
    // A frequency width with no start starts from the band's lower edge.
    let axis_config = CubeAxisConfig {
        outframe: FrequencyRef::TOPO,
        width: Some(CubeAxisValue::FrequencyHz {
            hz: 1.0e8,
            frame: None,
        }),
        selected_band_hz: Some([1.175e9, 1.675e9]),
        ..CubeAxisConfig::default()
    };
    let setup = CubeSpectralSetup::for_casa_cube_axis(
        FrequencyRef::TOPO,
        &source_hz,
        &[5.0e7; 20],
        Some(3),
        &axis_config,
        59_000.0 * 86_400.0,
        0,
        None,
        &engine,
    )
    .unwrap();
    assert_eq!(
        setup.output_channel_frequencies_hz,
        linear_spectral_axis_hz(1.225e9, 1.0e8, 3)
    );
}

#[test]
fn for_casa_cube_frequency_axis_keeps_single_output_channel_width() {
    let engine = test_engine();
    let axis_config = CubeAxisConfig {
        outframe: FrequencyRef::LSRK,
        start: Some(CubeAxisValue::FrequencyHz {
            hz: 44.0e9,
            frame: None,
        }),
        width: Some(CubeAxisValue::FrequencyHz {
            hz: 2.0e6,
            frame: None,
        }),
        ..CubeAxisConfig::default()
    };
    let setup = CubeSpectralSetup::for_casa_cube_axis(
        FrequencyRef::LSRK,
        &[44.0e9, 44.002e9, 44.004e9],
        &[2.0e6; 3],
        Some(1),
        &axis_config,
        59_000.0 * 86_400.0,
        0,
        None,
        &engine,
    )
    .unwrap();
    assert_eq!(setup.output_channel_frequencies_hz, vec![44.0e9]);
    assert_eq!(setup.output_channel_widths_hz, vec![2.0e6]);
}

#[test]
fn for_casa_cube_channel_axis_rejects_frequency_like_channel_inputs() {
    let engine = test_engine();
    let axis_config = CubeAxisConfig {
        start: Some(CubeAxisValue::FrequencyHz {
            hz: 1.0e9,
            frame: None,
        }),
        ..CubeAxisConfig::default()
    };
    let error = CubeSpectralSetup::for_casa_cube_channel_axis(
        FrequencyRef::TOPO,
        &[1.0e9, 1.05e9],
        &[5.0e7, 5.0e7],
        Some(1),
        &axis_config,
        59_000.0 * 86_400.0,
        0,
        None,
        &engine,
    )
    .unwrap_err();
    assert!(error.to_string().contains("channel-valued start"));
}

#[test]
fn row_output_channel_contributions_batch_rejects_width_mismatch() {
    let setup = CubeSpectralSetup {
        source_freq_ref: FrequencyRef::TOPO,
        output_freq_ref: FrequencyRef::TOPO,
        interpolation: CubeInterpolation::Linear,
        interpolation_uses_native_source_frequencies: false,
        output_frame_reference_time_mjd_sec: 59_000.0 * 86_400.0,
        output_frame_field_id: 0,
        output_channel_frequencies_hz: vec![1.0e9],
        output_channel_table_hz: None,
        output_channel_widths_hz: vec![5.0e7],
    };
    let engine = test_engine();
    let error = setup
        .row_output_channel_contributions_batch(&[1.0e9], &[], 59_000.0 * 86_400.0, 0, &engine)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("matching frequency/width arrays")
    );
}

#[test]
fn row_output_channel_contributions_batch_uses_native_interpolation_when_requested() {
    let setup = CubeSpectralSetup {
        source_freq_ref: FrequencyRef::TOPO,
        output_freq_ref: FrequencyRef::LSRK,
        interpolation: CubeInterpolation::Nearest,
        interpolation_uses_native_source_frequencies: true,
        output_frame_reference_time_mjd_sec: 59_000.0 * 86_400.0,
        output_frame_field_id: 0,
        output_channel_frequencies_hz: vec![1.05e9],
        output_channel_table_hz: None,
        output_channel_widths_hz: vec![5.0e7],
    };
    let engine = test_engine();
    let contributions = setup
        .row_output_channel_contributions_batch(
            &[1.0e9, 1.05e9, 1.10e9],
            &[5.0e7, 5.0e7, 5.0e7],
            59_000.0 * 86_400.0,
            0,
            &engine,
        )
        .unwrap();
    assert_eq!(contributions[0].len(), 1);
    assert_eq!(contributions[0][0].source_channel, 1);
    assert_eq!(contributions[0][0].source_frequency_hz, 1.05e9);
}

#[test]
fn nearest_channel_index_requires_points_to_stay_within_half_spacing() {
    assert_eq!(nearest_channel_index(&[10.0, 20.0, 30.0], 25.0), Some(1));
    assert_eq!(nearest_channel_index(&[10.0, 20.0, 30.0], 35.1), None);
}

#[test]
fn cube_source_support_is_empty_for_output_planes_outside_the_source_axis() {
    let setup = CubeSpectralSetup {
        source_freq_ref: FrequencyRef::TOPO,
        output_freq_ref: FrequencyRef::TOPO,
        interpolation: CubeInterpolation::Linear,
        interpolation_uses_native_source_frequencies: false,
        output_frame_reference_time_mjd_sec: 59_000.0 * 86_400.0,
        output_frame_field_id: 0,
        output_channel_frequencies_hz: vec![2.0e9],
        output_channel_table_hz: None,
        output_channel_widths_hz: vec![1.0e6],
    };

    let support = setup
        .source_channel_support(
            &[1.0e9, 1.01e9, 1.02e9],
            &[1.0e6, 1.0e6, 1.0e6],
            [59_000.0 * 86_400.0, 59_000.1 * 86_400.0],
            0,
            &test_engine(),
        )
        .unwrap();

    assert!(support.indices.is_empty());
    assert!(support.frequencies_hz.is_empty());
}

#[test]
fn spectral_interpolation_contributions_cover_nearest_and_cubic_paths() {
    let nearest = spectral_interpolation_contributions(
        &[10.0, 20.0, 30.0],
        &[10.0, 20.0, 30.0],
        &[10.0, 10.0, 10.0],
        CubeInterpolation::Nearest,
        19.0,
    );
    assert_eq!(nearest[0].source_channel, 1);

    let cubic = spectral_interpolation_contributions(
        &[10.0, 20.0, 30.0],
        &[10.0, 20.0, 30.0],
        &[10.0, 10.0, 10.0],
        CubeInterpolation::Cubic,
        15.0,
    );
    assert_eq!(cubic.len(), 2);
    assert!((cubic[0].factor + cubic[1].factor - 1.0).abs() < 1.0e-6);
}

#[test]
fn linear_channel_contributions_cover_single_channel_and_invalid_shapes() {
    let inside = linear_channel_contributions(&[42.0], &[42.0], &[4.0], 43.0);
    assert_eq!(inside.len(), 1);
    assert_eq!(inside[0].source_channel, 0);

    let outside = linear_channel_contributions(&[42.0], &[42.0], &[4.0], 45.0);
    assert!(outside.is_empty());

    let invalid = linear_channel_contributions(&[42.0], &[42.0, 43.0], &[4.0, 4.0], 42.0);
    assert!(invalid.is_empty());
}
