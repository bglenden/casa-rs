// SPDX-License-Identifier: LGPL-3.0-or-later
//! `AbsorptionProfile` against ATM's `RefractiveIndexProfile` and the
//! `SkyStatus` accessors that scale it: for CASA's simulator profile, a
//! humid sea-level one and the VLA in winter, over twelve windows from
//! 22 to 988 GHz (line centres, odd and even channel counts, one channel,
//! a negative separation), every per-channel opacity, phase delay and path
//! length, the window averages and the per-layer absorption coefficients.

mod common;

use casa_atmosphere::absorption::{
    Absorber, AbsorptionChannel, channel_absorption, channel_totals,
};
use casa_atmosphere::{AbsorptionProfile, ChannelAbsorption, Sky};
use common::{Agreement, json, num, nums, peak_floor, sky_atmosphere, sky_grid};

const REL: f64 = 1e-12;

type Full = AbsorptionProfile<ChannelAbsorption>;
type PerChannel = fn(&Full, usize, f64, f64) -> f64;

/// Fixture key, and the port's value for a channel given the oracle's
/// other water column `w` and the profile's own `g`.
const CHANNELS: &[(&str, PerChannel)] = &[
    ("dry", |p, c, _, _| p.dry_opacity(c)),
    ("dry_continuum", |p, c, _, _| p.dry_continuum_opacity(c)),
    ("o2", |p, c, _, _| p.absorber_opacity(c, Absorber::O2Lines)),
    ("o2_ground", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O2Ground)
    }),
    ("o2_vibrational", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O2Vibrational)
    }),
    ("o2_isotope18", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O2Isotope18)
    }),
    ("o2_isotope17", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O2Isotope17)
    }),
    ("o3", |p, c, _, _| p.absorber_opacity(c, Absorber::O3Lines)),
    ("o3_ground", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O3Ground)
    }),
    ("o3_nu1", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O3Nu1)
    }),
    ("o3_nu2", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O3Nu2)
    }),
    ("o3_nu3", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O3Nu3)
    }),
    ("o3_668", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O3Isotope668)
    }),
    ("o3_667", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O3Isotope667)
    }),
    ("o3_686", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O3Isotope686)
    }),
    ("o3_676", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O3Isotope676)
    }),
    ("co", |p, c, _, _| p.absorber_opacity(c, Absorber::CoLines)),
    ("n2o", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::N2oLines)
    }),
    ("no2", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::No2Lines)
    }),
    ("so2", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::So2Lines)
    }),
    ("hcl", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::HclLines)
    }),
    ("hcn", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::HcnLines)
    }),
    ("wet_ground", |p, c, _, g| p.wet_opacity(c, g)),
    ("h2o_lines_ground", |p, c, _, g| p.h2o_lines_opacity(c, g)),
    ("h2o_continuum_ground", |p, c, _, g| {
        p.h2o_continuum_opacity(c, g)
    }),
    ("h2o_ground_state_ground", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::H2oGround)
    }),
    ("wet", |p, c, w, _| p.wet_opacity(c, w)),
    ("h2o_lines", |p, c, w, _| p.h2o_lines_opacity(c, w)),
    ("h2o_continuum", |p, c, w, _| p.h2o_continuum_opacity(c, w)),
    ("dispersive_h2o_phase", |p, c, w, _| {
        p.dispersive_h2o_phase_delay_deg(c, w)
    }),
    ("dispersive_h2o_path", |p, c, w, _| {
        p.dispersive_h2o_path_length_m(c, w)
    }),
    ("non_dispersive_h2o_phase", |p, c, w, _| {
        p.non_dispersive_h2o_phase_delay_deg(c, w)
    }),
    ("non_dispersive_h2o_path", |p, c, w, _| {
        p.non_dispersive_h2o_path_length_m(c, w)
    }),
    ("dispersive_dry_phase", |p, c, _, _| {
        p.dispersive_dry_phase_delay_deg(c)
    }),
    ("dispersive_dry_path", |p, c, _, _| {
        p.dispersive_dry_path_length_m(c)
    }),
    ("non_dispersive_dry_phase", |p, c, _, _| {
        p.non_dispersive_dry_phase_delay_deg(c)
    }),
    ("non_dispersive_dry_path", |p, c, _, _| {
        p.non_dispersive_dry_path_length_m(c)
    }),
    ("o2_phase", |p, c, _, _| {
        p.absorber_phase_delay_deg(c, Absorber::O2Lines)
    }),
    ("o2_path", |p, c, _, _| {
        p.absorber_path_length_m(c, Absorber::O2Lines)
    }),
    ("o3_phase", |p, c, _, _| {
        p.absorber_phase_delay_deg(c, Absorber::O3Lines)
    }),
    ("o3_path", |p, c, _, _| {
        p.absorber_path_length_m(c, Absorber::O3Lines)
    }),
    ("co_phase", |p, c, _, _| {
        p.absorber_phase_delay_deg(c, Absorber::CoLines)
    }),
    ("co_path", |p, c, _, _| {
        p.absorber_path_length_m(c, Absorber::CoLines)
    }),
    ("n2o_phase", |p, c, _, _| {
        p.absorber_phase_delay_deg(c, Absorber::N2oLines)
    }),
    ("n2o_path", |p, c, _, _| {
        p.absorber_path_length_m(c, Absorber::N2oLines)
    }),
    ("no2_phase", |p, c, _, _| {
        p.absorber_phase_delay_deg(c, Absorber::No2Lines)
    }),
    ("so2_phase", |p, c, _, _| {
        p.absorber_phase_delay_deg(c, Absorber::So2Lines)
    }),
    ("hcl_phase", |p, c, _, _| {
        p.absorber_phase_delay_deg(c, Absorber::HclLines)
    }),
    ("hcn_phase", |p, c, _, _| {
        p.absorber_phase_delay_deg(c, Absorber::HcnLines)
    }),
    // SkyStatus's accessors, with its water column reset to the profile's
    // own as the `at` tool does.
    ("sky_wet", |p, c, _, g| p.wet_opacity(c, g)),
    ("sky_h2o_lines", |p, c, _, g| p.h2o_lines_opacity(c, g)),
    ("sky_h2o_continuum", |p, c, _, g| {
        p.h2o_continuum_opacity(c, g)
    }),
    ("sky_h2o_ground_state", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::H2oGround)
    }),
    ("sky_h2o_bending", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::H2oBending)
    }),
    ("sky_h2o_isotope17", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::H2oIsotope17)
    }),
    ("sky_h2o_isotope18", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::H2oIsotope18)
    }),
    ("sky_hdo", |p, c, _, _| p.absorber_opacity(c, Absorber::Hdo)),
    ("sky_dispersive_phase", |p, c, _, g| {
        p.dispersive_h2o_phase_delay_deg(c, g)
    }),
    ("sky_dispersive_path", |p, c, _, g| {
        p.dispersive_h2o_path_length_m(c, g)
    }),
    ("sky_non_dispersive_phase", |p, c, _, g| {
        p.non_dispersive_h2o_phase_delay_deg(c, g)
    }),
    ("sky_non_dispersive_path", |p, c, _, g| {
        p.non_dispersive_h2o_path_length_m(c, g)
    }),
    ("tebb", |p, c, _, g| p.tebb_sky(c, &Sky::zenith(g)).unwrap()),
    ("trj", |p, c, _, g| p.trj_sky(c, &Sky::zenith(g)).unwrap()),
];

/// Fixture key, and the per-channel value a window average is taken of.
const AVERAGES: &[(&str, PerChannel)] = &[
    ("average_dry", |p, c, _, _| p.dry_opacity(c)),
    ("average_dry_continuum", |p, c, _, _| {
        p.dry_continuum_opacity(c)
    }),
    ("average_o2", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O2Lines)
    }),
    ("average_o3", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::O3Lines)
    }),
    ("average_co", |p, c, _, _| {
        p.absorber_opacity(c, Absorber::CoLines)
    }),
    ("average_wet", |p, c, w, _| p.wet_opacity(c, w)),
    ("average_h2o_lines", |p, c, w, _| p.h2o_lines_opacity(c, w)),
    ("average_h2o_continuum", |p, c, w, _| {
        p.h2o_continuum_opacity(c, w)
    }),
    ("average_dispersive_h2o_phase", |p, c, w, _| {
        p.dispersive_h2o_phase_delay_deg(c, w)
    }),
    ("average_dispersive_h2o_path", |p, c, w, _| {
        p.dispersive_h2o_path_length_m(c, w)
    }),
    ("average_non_dispersive_h2o_path", |p, c, w, _| {
        p.non_dispersive_h2o_path_length_m(c, w)
    }),
    ("average_dispersive_dry_phase", |p, c, _, _| {
        p.dispersive_dry_phase_delay_deg(c)
    }),
    ("average_dispersive_dry_path", |p, c, _, _| {
        p.dispersive_dry_path_length_m(c)
    }),
    ("average_non_dispersive_dry_phase", |p, c, _, _| {
        p.non_dispersive_dry_phase_delay_deg(c)
    }),
    ("average_non_dispersive_dry_path", |p, c, _, _| {
        p.non_dispersive_dry_path_length_m(c)
    }),
    ("sky_average_dispersive_phase", |p, c, _, g| {
        p.dispersive_h2o_phase_delay_deg(c, g)
    }),
    ("sky_average_non_dispersive_phase", |p, c, _, g| {
        p.non_dispersive_h2o_phase_delay_deg(c, g)
    }),
    ("sky_average_dispersive_path", |p, c, _, g| {
        p.dispersive_h2o_path_length_m(c, g)
    }),
    ("sky_average_non_dispersive_path", |p, c, _, g| {
        p.non_dispersive_h2o_path_length_m(c, g)
    }),
    ("sky_average_wet", |p, c, _, g| p.wet_opacity(c, g)),
];

fn profiles() -> (serde_json::Value, Vec<(serde_json::Value, Full)>) {
    let fixture = json("absorption.json");
    let profiles = fixture["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|profile| {
            let atmosphere = sky_atmosphere(profile);
            let grid = sky_grid(&fixture["windows"]);
            (profile.clone(), AbsorptionProfile::new(atmosphere, grid))
        })
        .collect();
    (fixture, profiles)
}

#[test]
fn channel_quantities_match_atm() {
    let (fixture, profiles) = profiles();
    let water = num(&fixture["water_column_m"]);
    let mut agreement = Agreement::new("per-channel opacities, delays, paths and Tebb", REL);
    for (fixture, profile) in &profiles {
        let name = fixture["altitude_m"].to_string();
        assert_eq!(
            profile.atmosphere().layers().len() as f64,
            num(&fixture["layers"])
        );
        let ground = profile.ground_water_column_m();
        agreement.check(ground, num(&fixture["ground_water_column_m"]), 0.0, || {
            format!("{name} ground water")
        });
        for (key, value) in CHANNELS {
            let expected = nums(&fixture[*key]);
            assert_eq!(expected.len(), profile.channels().len(), "{key}");
            let floor = peak_floor(&expected);
            for (c, cpp) in expected.iter().enumerate() {
                agreement.check(value(profile, c, water, ground), *cpp, floor, || {
                    format!("{name} {key} channel {c}")
                });
            }
        }
    }
    agreement.finish();
}

#[test]
fn window_averages_match_atm() {
    let (fixture, profiles) = profiles();
    let water = num(&fixture["water_column_m"]);
    let mut agreement = Agreement::new("window averages", REL);
    for (fixture, profile) in &profiles {
        let ground = profile.ground_water_column_m();
        for (key, value) in AVERAGES {
            let expected = nums(&fixture[*key]);
            let floor = peak_floor(&expected);
            for (w, cpp) in expected.iter().enumerate() {
                let rust = profile.window_average(w, |c| value(profile, c, water, ground));
                agreement.check(rust, *cpp, floor, || format!("{key} window {w}"));
            }
        }
        let sky = |spill| Sky {
            spill_temperature_k: spill,
            ..Sky::zenith(ground)
        };
        let ground_t = profile.atmosphere().parameters().ground_temperature_k;
        for (key, trj) in [("average_tebb", false), ("average_trj", true)] {
            for (w, cpp) in nums(&fixture[key]).iter().enumerate() {
                let rust = if trj {
                    profile.average_trj_sky(w, &sky(ground_t))
                } else {
                    profile.average_tebb_sky(w, &sky(ground_t))
                };
                agreement.check(rust.unwrap(), *cpp, 0.0, || format!("{key} window {w}"));
            }
        }
        // ATM's RefractiveIndexProfile average of the non-dispersive wet
        // phase passes each channel's flat index where a water column is
        // expected, so it averages channel 0's phase for columns of 0, 1,
        // 2 … metres. The port takes the mean of the channels, as
        // SkyStatus's average does (checked above); this reproduces the
        // defect exactly to show what ATM computes instead.
        for (w, cpp) in nums(&fixture["rip_average_non_dispersive_h2o_phase"])
            .iter()
            .enumerate()
        {
            let defect = profile.window_average(w, |c| {
                profile.non_dispersive_h2o_phase_delay_deg(0, c as f64)
            }) * (water / ground);
            agreement.check(defect, *cpp, 0.0, || {
                format!("ATM's non-dispersive average defect, window {w}")
            });
            let mean =
                profile.window_average(w, |c| profile.non_dispersive_h2o_phase_delay_deg(c, water));
            assert!(
                (defect - mean).abs() > 0.5 * mean.abs(),
                "window {w}: the defect should not be the mean"
            );
        }
    }
    agreement.finish();
}

#[test]
fn layer_coefficients_match_atm() {
    let (_, profiles) = profiles();
    let mut agreement = Agreement::new("per-layer absorption coefficients", REL);
    for (fixture, profile) in &profiles {
        let layers = profile.atmosphere().layers().len();
        let totals: Vec<usize> = nums(&fixture["layer_channels_totals"])
            .iter()
            .map(|&c| c as usize)
            .collect();
        let species: Vec<usize> = nums(&fixture["layer_channels_species"])
            .iter()
            .map(|&c| c as usize)
            .collect();
        let check = |agreement: &mut Agreement,
                     key: &str,
                     channels: &[usize],
                     value: &dyn Fn(usize, usize) -> f64| {
            let expected = nums(&fixture[key]);
            assert_eq!(expected.len(), channels.len() * layers, "{key}");
            let floor = peak_floor(&expected);
            for (k, &c) in channels.iter().enumerate() {
                for l in 0..layers {
                    agreement.check(value(c, l), expected[k * layers + l], floor, || {
                        format!("{key} channel {c} layer {l}")
                    });
                }
            }
        };
        check(&mut agreement, "layer_total_dry", &totals, &|c, l| {
            profile.layer_absorption(c, l).0
        });
        check(&mut agreement, "layer_total_wet", &totals, &|c, l| {
            profile.layer_absorption(c, l).1
        });
        check(&mut agreement, "layer_o2", &species, &|c, l| {
            profile.layer(c, l).o2.im
        });
        check(&mut agreement, "layer_dry_continuum", &species, &|c, l| {
            profile.layer(c, l).dry_continuum.im
        });
        check(&mut agreement, "layer_o3", &species, &|c, l| {
            profile.layer(c, l).o3.im
        });
        check(&mut agreement, "layer_co", &species, &|c, l| {
            profile.layer(c, l).co.im
        });
        check(&mut agreement, "layer_n2o", &species, &|c, l| {
            profile.layer(c, l).n2o.im
        });
        check(&mut agreement, "layer_h2o", &species, &|c, l| {
            profile.layer(c, l).h2o.im
        });
        check(&mut agreement, "layer_h2o_continuum", &species, &|c, l| {
            profile.layer(c, l).h2o_continuum.im
        });
    }
    agreement.finish();
}

#[test]
fn compact_and_parallel_profiles_equal_the_serial_full_one() {
    let (fixture, profiles) = profiles();
    let water = num(&fixture["water_column_m"]);
    for (_, full) in &profiles {
        let atmosphere = full.atmosphere().clone();
        let grid = full.grid().clone();
        let compact = AbsorptionProfile::new_totals(atmosphere.clone(), grid.clone());
        // Channels computed on four threads, assembled afterwards.
        let frequencies: Vec<f64> = grid.frequencies_hz().collect();
        let chunks: Vec<&[f64]> = frequencies.chunks(frequencies.len().div_ceil(4)).collect();
        let channels: Vec<_> = std::thread::scope(|scope| {
            let handles: Vec<_> = chunks
                .iter()
                .map(|chunk| {
                    let atmosphere = &atmosphere;
                    scope.spawn(move || {
                        chunk
                            .iter()
                            .map(|&f| channel_absorption(atmosphere, f))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect()
        });
        let parallel =
            AbsorptionProfile::from_channels(atmosphere.clone(), grid.clone(), channels).unwrap();
        assert_eq!(&parallel, full);
        let compact_channels = grid
            .frequencies_hz()
            .map(|f| channel_totals(&atmosphere, f))
            .collect();
        assert_eq!(
            AbsorptionProfile::from_channels(atmosphere.clone(), grid.clone(), compact_channels)
                .unwrap(),
            compact
        );
        for c in 0..full.channels().len() {
            assert_eq!(
                compact.dry_opacity(c).to_bits(),
                full.dry_opacity(c).to_bits()
            );
            assert_eq!(
                compact.wet_opacity(c, water).to_bits(),
                full.wet_opacity(c, water).to_bits()
            );
            assert_eq!(
                compact.dispersive_h2o_phase_delay_deg(c, water).to_bits(),
                full.dispersive_h2o_phase_delay_deg(c, water).to_bits()
            );
            let sky = Sky::zenith(water);
            assert_eq!(
                compact.tebb_sky(c, &sky).unwrap().to_bits(),
                full.tebb_sky(c, &sky).unwrap().to_bits()
            );
            assert_eq!(
                compact.channels()[c].layer_count(),
                atmosphere.layers().len()
            );
        }
    }
}

#[test]
fn profile_bytes_count_what_a_profile_keeps() {
    let (_, profiles) = profiles();
    let (_, full) = &profiles[0];
    let layers = full.atmosphere().layers().len();
    let channels = full.grid().total_channels() as u64;
    let full_bytes = Full::bytes(full.grid(), layers);
    let compact_bytes =
        AbsorptionProfile::<casa_atmosphere::ChannelTotals>::bytes(full.grid(), layers);
    // 28 complex values a layer against 4, plus what both keep.
    assert_eq!(
        full_bytes - compact_bytes,
        channels * layers as u64 * 24 * 16
    );
    assert!(compact_bytes >= channels * layers as u64 * 64);
    // A profile with the wrong channels is refused.
    let atmosphere = full.atmosphere().clone();
    let mut channels: Vec<_> = full.channels().to_vec();
    channels.pop();
    assert!(AbsorptionProfile::from_channels(atmosphere, full.grid().clone(), channels).is_err());
}
