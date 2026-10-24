// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sky brightness against ATM's `SkyStatus`: Tebb and Trj over airmass,
//! water column, sky coupling and spillover, and a `SkyStatus` call
//! sequence in which setters change the state the defaulted overloads read,
//! replayed with explicit [`Sky`] conditions.

mod common;

use casa_atmosphere::{AbsorptionProfile, ChannelTotals, Sky};
use common::{Agreement, json, num, nums, sky_atmosphere, sky_grid};

const REL: f64 = 1e-12;

fn compact(profile: usize) -> AbsorptionProfile<ChannelTotals> {
    let fixture = json("absorption.json");
    let atmosphere = sky_atmosphere(&fixture["profiles"][profile]);
    AbsorptionProfile::new_totals(atmosphere, sky_grid(&fixture["windows"]))
}

#[test]
fn tebb_and_trj_sweeps_match_atm() {
    let fixture = json("sky.json");
    let mut tebb = Agreement::new("Tebb sweep", REL);
    let mut trj = Agreement::new("Trj sweep", REL);
    for sweep in fixture["sweeps"].as_array().unwrap() {
        let profile = compact(usize::try_from(sweep["profile"].as_u64().unwrap()).unwrap());
        for row in nums(&sweep["rows"]).chunks(7) {
            let channel = row[0] as usize;
            let sky = Sky {
                water_column_m: row[2],
                airmass: row[1],
                sky_coupling: row[3],
                spill_temperature_k: row[4],
                background_temperature_k: 2.73,
            };
            let at = || format!("channel {channel} {sky:?}");
            tebb.check(profile.tebb_sky(channel, &sky).unwrap(), row[5], 0.0, at);
            trj.check(profile.trj_sky(channel, &sky).unwrap(), row[6], 0.0, at);
        }
    }
    tebb.finish();
    trj.finish();
}

#[test]
fn skystatus_call_sequence_replays_with_explicit_conditions() {
    let fixture = json("sky.json");
    let profile = compact(0);
    // SkyStatus starts with a 1 mm water column, airmass 1 and a 2.73 K
    // background; the sequence then sets 2 mm, airmass 1.7 and 5 K.
    let initial = num(&fixture["initial_user_water_m"]);
    assert_eq!(initial, 1e-3);
    let ground_t = profile.atmosphere().parameters().ground_temperature_k;
    let start = Sky::zenith(initial);
    let set = Sky {
        water_column_m: 2e-3,
        airmass: 1.7,
        background_temperature_k: 5.0,
        ..Sky::zenith(2e-3)
    };
    let mut agreement = Agreement::new("SkyStatus call sequence", REL);
    for step in nums(&fixture["sequence"]).chunks(4) {
        let (window, cpp) = (step[1] as usize, step[3]);
        let channel = profile.grid().flat_index(window, step[2] as usize).unwrap();
        let rust = match step[0] as u32 {
            0 => profile.tebb_sky(channel, &start),
            1 | 3 => profile.tebb_sky(channel, &set),
            2 => profile.tebb_sky(
                channel,
                &Sky {
                    water_column_m: 1e-3,
                    ..set
                },
            ),
            4 => profile.trj_sky(channel, &set),
            5 => profile.average_tebb_sky(
                window,
                &Sky {
                    spill_temperature_k: ground_t,
                    ..set
                },
            ),
            6 => profile.average_trj_sky(
                window,
                &Sky {
                    spill_temperature_k: ground_t,
                    ..set
                },
            ),
            7 => Ok(profile.wet_opacity(channel, set.water_column_m)),
            8 => Ok(profile.dispersive_h2o_phase_delay_deg(channel, set.water_column_m)),
            9 => profile.tebb_sky(
                channel,
                &Sky {
                    sky_coupling: 0.93,
                    spill_temperature_k: 280.0,
                    ..set
                },
            ),
            other => panic!("unknown step {other}"),
        }
        .unwrap();
        agreement.check(rust, cpp, 0.0, || {
            format!("step {} window {window}", step[0])
        });
    }
    agreement.finish();
}

#[test]
fn invalid_sky_conditions_are_refused_as_atm_refuses_them() {
    let profile = compact(0);
    let good = Sky::zenith(1e-3);
    for bad in [
        Sky {
            water_column_m: -1e-3,
            ..good
        },
        Sky {
            airmass: 0.9,
            ..good
        },
        Sky {
            sky_coupling: 1.1,
            ..good
        },
        Sky {
            spill_temperature_k: 400.0,
            ..good
        },
        Sky {
            water_column_m: f64::NAN,
            ..good
        },
    ] {
        assert!(profile.tebb_sky(0, &bad).is_err(), "{bad:?}");
        assert!(profile.trj_sky(0, &bad).is_err(), "{bad:?}");
    }
    // ATM's window averages do not check the coupling.
    let coupling = Sky {
        sky_coupling: 1.1,
        ..good
    };
    assert!(profile.average_tebb_sky(0, &coupling).is_ok());
    assert!(
        profile
            .average_tebb_sky(
                0,
                &Sky {
                    airmass: 0.5,
                    ..good
                }
            )
            .is_err()
    );
}
