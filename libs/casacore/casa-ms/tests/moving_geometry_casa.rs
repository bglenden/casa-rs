// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::f64::consts::{PI, TAU};
use std::path::PathBuf;

use casa_core::ScalarValue;
use casa_ms::MeasurementSet;
use casa_ms::derived::columns::DerivedColumns;
use serde::Deserialize;

#[derive(Deserialize)]
struct GeometryCase {
    ms: String,
    epochs: Vec<(f64, i32, [f64; 11])>,
}

#[test]
fn moving_columns_match_casa_at_each_selected_epoch() {
    let Some(root) = std::env::var_os("CASARS_CASA_EPHEMERIS_GEOMETRY") else {
        eprintln!(
            "run calibration_ephemeris_geometry_oracle.py and set CASARS_CASA_EPHEMERIS_GEOMETRY to its work path"
        );
        return;
    };
    let root = PathBuf::from(root);
    let cases: BTreeMap<String, GeometryCase> =
        serde_json::from_slice(&std::fs::read(root.join("derived.json")).unwrap()).unwrap();
    assert_eq!(cases.len(), 3);
    for (mount, case) in cases {
        let ms = MeasurementSet::open(root.join(case.ms)).unwrap();
        let derived = DerivedColumns::from_measurement_set(&ms).unwrap();
        let main = ms.main_table();
        let time = main.column_accessor("TIME").unwrap();
        let field = main.column_accessor("FIELD_ID").unwrap();
        let first = main.column_accessor("ANTENNA1").unwrap();
        let second = main.column_accessor("ANTENNA2").unwrap();
        let mut largest_angle = 0.0_f64;
        let mut largest_uvw = 0.0_f64;
        for (epoch, antenna, expected) in case.epochs {
            let row = (0..main.row_count())
                .find(|&row| {
                    time.scalar_cell(row).unwrap() == &ScalarValue::Float64(epoch)
                        && field.scalar_cell(row).unwrap() == &ScalarValue::Int32(0)
                        && first.scalar_cell(row).unwrap() == &ScalarValue::Int32(antenna)
                        && second.scalar_cell(row).unwrap() == &ScalarValue::Int32(antenna + 1)
                })
                .expect("the sampled field, baseline and epoch");
            let (az, el) = derived.azel1(row).unwrap();
            let (ha, dec) = derived.hadec1(row).unwrap();
            let pa = derived.pa1(row).unwrap();
            let (array_az, array_el) = derived.azel(row).unwrap();
            let array_ha = derived.ha(row).unwrap();
            for (index, actual) in [az, el, ha, dec, pa, array_az, array_el, array_ha]
                .into_iter()
                .enumerate()
            {
                let error = (actual - expected[index] + PI).rem_euclid(TAU) - PI;
                largest_angle = largest_angle.max(error.abs());
                assert!(
                    error.abs() < 2e-6,
                    "{mount} angle {index} at {epoch}: {error:.7e} rad"
                );
            }
            let uvw = derived.uvw_j2000(row).unwrap();
            let error = uvw
                .iter()
                .zip(&expected[8..])
                .map(|(&actual, reference)| (actual - reference).powi(2))
                .sum::<f64>();
            let scale = expected[8..].iter().map(|value| value.powi(2)).sum::<f64>();
            let error = (error / scale).sqrt();
            largest_uvw = largest_uvw.max(error);
            assert!(
                error < 1e-3,
                "{mount} UVW at {epoch}: {error:.7e} normalized RMS"
            );
        }
        println!(
            "{mount}: largest angle error {largest_angle:.7e} rad, largest UVW normalized RMS {largest_uvw:.7e}"
        );
    }
}
