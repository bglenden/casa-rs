// SPDX-License-Identifier: LGPL-3.0-or-later
//! `AtmosphereProfile` against ATM's `AtmProfile`: every layer field of
//! every reference atmosphere, at sea level and at Chajnantor, plus sites,
//! pressure steps (in and out of range), a profile topped above ATM's
//! reference levels and a user temperature profile.

mod common;

use casa_atmosphere::{AtmosphereProfile, AtmosphereType, Layer, ProfileParameters};
use common::{Agreement, json, num, nums};

const REL: f64 = 1e-12;

fn parameters(case: &serde_json::Value) -> ProfileParameters {
    let number = u32::try_from(case["atmosphere"].as_u64().unwrap()).unwrap();
    ProfileParameters {
        altitude_m: num(&case["altitude_m"]),
        ground_pressure_hpa: num(&case["ground_pressure_hpa"]),
        ground_temperature_k: num(&case["ground_temperature_k"]),
        lapse_rate_k_per_km: num(&case["lapse_rate_k_per_km"]),
        relative_humidity_percent: num(&case["relative_humidity_percent"]),
        water_vapour_scale_height_km: num(&case["water_vapour_scale_height_km"]),
        pressure_step_hpa: num(&case["pressure_step_hpa"]),
        top_altitude_km: num(&case["top_altitude_km"]),
        atmosphere: AtmosphereType::from_number(number).unwrap(),
    }
}

fn build(case: &serde_json::Value) -> AtmosphereProfile {
    let levels: Vec<(f64, f64)> = case["levels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|level| (num(&level[0]), num(&level[1])))
        .collect();
    let parameters = parameters(case);
    if levels.is_empty() {
        AtmosphereProfile::new(&parameters).unwrap()
    } else {
        AtmosphereProfile::with_temperature_levels(&parameters, &levels).unwrap()
    }
}

type Field = (&'static str, fn(&Layer) -> f64);

const FIELDS: [Field; 17] = [
    ("thickness_m", |l| l.thickness_m),
    ("temperature_k", |l| l.temperature_k),
    ("bottom_temperature_k", |l| l.bottom_temperature_k),
    ("top_temperature_k", |l| l.top_temperature_k),
    ("pressure_hpa", |l| l.pressure_hpa),
    ("bottom_pressure_hpa", |l| l.bottom_pressure_hpa),
    ("top_pressure_hpa", |l| l.top_pressure_hpa),
    ("water_vapour_kg_m3", |l| l.water_vapour_kg_m3),
    ("bottom_water_vapour_kg_m3", |l| l.bottom_water_vapour_kg_m3),
    ("top_water_vapour_kg_m3", |l| l.top_water_vapour_kg_m3),
    ("o3_per_m3", |l| l.o3_per_m3),
    ("co_per_m3", |l| l.co_per_m3),
    ("n2o_per_m3", |l| l.n2o_per_m3),
    ("no2_per_m3", |l| l.no2_per_m3),
    ("so2_per_m3", |l| l.so2_per_m3),
    ("hcl_per_m3", |l| l.hcl_per_m3),
    ("hcn_per_m3", |l| l.hcn_per_m3),
];

#[test]
fn profiles_match_atm_layer_by_layer() {
    let cases = json("profiles.json");
    let mut summary = Agreement::new("profile scalars", REL);
    let mut layers = Agreement::new("profile layers", REL);
    // Every case the oracle wrote, so a truncated fixture fails rather than
    // covers less (#894 review).
    assert_eq!(cases.as_array().unwrap().len(), 29, "profile cases");
    for case in cases.as_array().unwrap() {
        let name = format!(
            "{} (type {})",
            case["name"].as_str().unwrap(),
            case["atmosphere"]
        );
        let profile = build(case);
        let count = usize::try_from(case["num_layers"].as_u64().unwrap()).unwrap();
        assert_eq!(profile.layers().len(), count, "{name}: layer count");
        assert_eq!(
            profile.tropopause_layer(),
            usize::try_from(case["tropopause_layer"].as_u64().unwrap()).unwrap(),
            "{name}: tropopause layer"
        );
        let scalars = [
            (
                "pressure step",
                profile.parameters().pressure_step_hpa,
                "corrected_step_hpa",
            ),
            (
                "tropopause temperature",
                profile.tropopause_temperature_k(),
                "tropopause_temperature_k",
            ),
            (
                "tropopause altitude",
                profile.tropopause_altitude_m(),
                "tropopause_altitude_m",
            ),
            (
                "ground water column",
                profile.ground_water_column_m(),
                "ground_water_column_m",
            ),
        ];
        for (label, rust, key) in scalars {
            summary.check(rust, num(&case[key]), 0.0, || format!("{name} {label}"));
        }
        for (field, get) in FIELDS {
            let expected = nums(&case[field]);
            assert_eq!(expected.len(), count, "{name}: {field} has a value a layer");
            for (j, (layer, cpp)) in profile.layers().iter().zip(&expected).enumerate() {
                layers.check(get(layer), *cpp, 0.0, || {
                    format!("{name} layer {j} {field}")
                });
            }
        }
    }
    summary.finish();
    layers.finish();
}

#[test]
fn invalid_parameters_are_refused() {
    let good = ProfileParameters::default();
    let cases = [
        ProfileParameters {
            ground_pressure_hpa: 0.0,
            ..good.clone()
        },
        ProfileParameters {
            ground_temperature_k: f64::NAN,
            ..good.clone()
        },
        ProfileParameters {
            water_vapour_scale_height_km: -1.0,
            ..good.clone()
        },
        ProfileParameters {
            top_altitude_km: 4.0,
            ..good.clone()
        },
    ];
    for parameters in cases {
        assert!(
            AtmosphereProfile::new(&parameters).is_err(),
            "{parameters:?} was accepted"
        );
    }
    // A user temperature level that is not finite or not positive, or
    // levels whose mean overflows (#894 review), refused as the parameters
    // are.
    let levels: [&[(f64, f64)]; 5] = [
        &[(5_000.0, f64::INFINITY)],
        &[(f64::NAN, 250.0)],
        &[(5_000.0, 0.0)],
        &[(5_000.0, -20.0)],
        &[(5_000.0, f64::MAX), (5_001.0, f64::MAX)],
    ];
    for levels in levels {
        assert!(
            AtmosphereProfile::with_temperature_levels(&good, levels).is_err(),
            "{levels:?} was accepted"
        );
    }
    assert!(AtmosphereProfile::with_temperature_levels(&good, &[(5_000.0, 260.0)]).is_ok());
}

#[test]
fn atmosphere_numbers_are_atms() {
    for number in [1, 2, 3, 4, 5, 6, 11, 12, 13, 14] {
        assert_eq!(
            AtmosphereType::from_number(number).map(|t| t as u32),
            Some(number)
        );
    }
    for number in [0, 7, 10, 15] {
        assert_eq!(AtmosphereType::from_number(number), None);
    }
}
