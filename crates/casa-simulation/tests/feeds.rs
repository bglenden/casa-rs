// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA multi-feed metadata and per-integration offset geometry.
mod predict_support;
use casa_simulation::{
    ArrayConfig, CoordinateSystem, Simulator, SimulatorOptions, SpectralWindowSpec,
};
use casa_tables::{Table, TableOptions};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, frequency::FrequencyRef};
use casa_types::{ArrayValue, ScalarValue};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::{io::Read, path::Path};

fn reference() -> Value {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_feeds_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let result: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(result["casa_version"], "6.7.6.14");
    result
}

fn configured(path: &Path, case: &Value, workers: usize) -> Simulator {
    let mut sm = Simulator::open(
        path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: predict_support::resources(workers, 512 << 20),
            now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
        },
    );
    let centre = [-1601185., -5041977., 3554875.];
    let axis = |axis: usize, step: f64| (0..8).map(|i| centre[axis] + step * i as f64).collect();
    sm.set_config(&ArrayConfig {
        telescope: "ALMA".into(),
        x: axis(0, 20.),
        y: axis(1, -6.),
        z: axis(2, 12.),
        dish_diameter: vec![12.],
        offset: vec![0.],
        mount: vec![case["mount"].as_str().unwrap().into()],
        antenna_names: vec!["DA".into()],
        pad_names: vec!["A".into()],
        coordinate_system: CoordinateSystem::Global,
    })
    .unwrap();
    sm.set_spectral_window(&SpectralWindowSpec {
        name: "band6".into(),
        start_frequency_hz: 230e9,
        channel_width_hz: 15.625e6,
        resolution_hz: 15.625e6,
        channel_count: 4,
        stokes: "XX XY YX YY".into(),
        frame: FrequencyRef::TOPO,
    })
    .unwrap();
    let count = case["count"].as_u64().unwrap() as usize;
    sm.set_feed(
        "list",
        &[0., 0.0002, -0.0001][..count],
        &[0., -0.0001, 0.0003][..count],
        &["X".into(), "R".into(), "other".into()][..count],
    )
    .unwrap();
    sm.set_field(
        "target",
        &MDirection::from_angles(
            predict_support::CENTRE_RAD[0],
            predict_support::CENTRE_RAD[1],
            DirectionRef::J2000,
        ),
        "",
        0.,
    )
    .unwrap();
    sm.set_times(
        10.,
        false,
        MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
    )
    .unwrap();
    sm
}

fn flatten(value: &Value, output: &mut Vec<f64>) {
    if let Some(values) = value.as_array() {
        for v in values {
            flatten(v, output);
        }
    } else {
        output.push(value.as_f64().unwrap());
    }
}

fn check_uvw(table: &Table, case: &Value, expected: &Value, native: bool) {
    let mut actual = Vec::new();
    let mut target = Vec::new();
    for row in 0..table.row_count() {
        let ArrayValue::Float64(v) = table.cell_accessor(row, "UVW").unwrap().array().unwrap()
        else {
            panic!("UVW");
        };
        actual.extend(v.iter().copied());
        flatten(&expected[row], &mut target);
    }
    let nrms = |a: &[f64], b: &[f64]| {
        (a.iter().zip(b).map(|(a, b)| (a - b).powi(2)).sum::<f64>()
            / b.iter().map(|v| v * v).sum::<f64>())
        .sqrt()
    };
    // Absolute geometry uses the ordinary agreement tolerance. The feed-offset
    // delta below removes the baseline's existing reference-frame residual.
    let absolute = actual
        .iter()
        .zip(&target)
        .map(|(a, b)| (a - b).abs())
        .fold(0.0_f64, f64::max);
    println!("max absolute UVW residual {absolute:e} m");
    assert!(absolute <= 1e-3, "preserve the existing 1-mm UVW allowance");
    let residual = nrms(&actual, &target);
    println!("absolute UVW NRMS {residual:e}");
    assert!(residual < 1e-3);
    if native {
        let block = actual.len() / case["count"].as_u64().unwrap() as usize;
        for feed in 1..case["count"].as_u64().unwrap() as usize {
            let delta = |v: &[f64]| {
                (0..block)
                    .map(|i| v[feed * block + i] - v[i])
                    .collect::<Vec<_>>()
            };
            let residual = nrms(&delta(&actual), &delta(&target));
            println!("feed {feed} offset delta NRMS {residual:e}");
            assert!(residual < 1e-3);
        }
    }
}

fn check(path: &Path, case: &Value, native: bool) {
    for name in ["MAIN", "FEED", "POINTING"] {
        let table = Table::open(TableOptions::new(if name == "MAIN" {
            path.to_path_buf()
        } else {
            path.join(name)
        }))
        .unwrap();
        let columns = case[name].as_object().unwrap();
        assert_eq!(
            table.row_count(),
            columns.values().next().unwrap().as_array().unwrap().len()
        );
        let mut worst = 0.0_f64;
        for (column, expected) in columns {
            let expected = if native && column == "UVW" {
                case.get("corrected_uvw").unwrap_or(expected)
            } else {
                expected
            };
            if column == "UVW" {
                check_uvw(&table, case, expected, native);
                continue;
            }
            for row in 0..table.row_count() {
                let cell = table.cell_accessor(row, column).unwrap();
                let expected = &expected[row];
                if let Ok(value) = cell.scalar() {
                    let actual = match value {
                        ScalarValue::Int32(v) => serde_json::json!(v),
                        ScalarValue::Int64(v) => serde_json::json!(v),
                        ScalarValue::Float64(v) => serde_json::json!(v),
                        ScalarValue::Bool(v) => serde_json::json!(v),
                        other => panic!("scalar {other:?}"),
                    };
                    assert_eq!(&actual, expected, "{name}:{column}:{row}");
                } else {
                    match cell.array().unwrap() {
                        ArrayValue::String(v) => assert_eq!(
                            v.iter().cloned().collect::<Vec<_>>(),
                            expected
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|v| v.as_str().unwrap().to_string())
                                .collect::<Vec<_>>()
                        ),
                        value => {
                            let actual = match value {
                                ArrayValue::Float64(v) => v.iter().copied().collect::<Vec<_>>(),
                                ArrayValue::Float32(v) => v.iter().map(|v| f64::from(*v)).collect(),
                                other => panic!("array {other:?}"),
                            };
                            let mut target = Vec::new();
                            flatten(expected, &mut target);
                            assert_eq!(actual.len(), target.len());
                            for (a, b) in actual.iter().zip(target) {
                                let error = (*a - b).abs();
                                worst = worst.max(error);
                                assert!(error < 1e-5, "{name}:{column}:{row}: {a} vs {b}");
                            }
                        }
                    }
                }
            }
        }
        println!("{name}: {} rows, max residual {worst:e}", table.row_count());
    }
    let table = Table::open(TableOptions::new(path)).unwrap();
    for row in 0..table.row_count() {
        let ArrayValue::Complex32(data) =
            table.cell_accessor(row, "DATA").unwrap().array().unwrap()
        else {
            panic!("DATA");
        };
        assert!(data.iter().all(|v| v.norm_sqr() == 0.));
    }
}

#[test]
fn list_feeds_match_casas_metadata_rows_and_instantaneous_offsets() {
    for case in reference()["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["split"] == false)
    {
        for workers in [1, 3] {
            let export = (workers == 1)
                .then(|| std::env::var_os("CASA_RS_FEEDS_OUT"))
                .flatten();
            let root = if let Some(base) = &export {
                std::fs::create_dir_all(base).unwrap();
                tempfile::tempdir_in(base).unwrap()
            } else {
                tempfile::tempdir().unwrap()
            };
            let path = root.path().join("data.ms");
            let mut sm = configured(&path, case, workers);
            sm.observe(&predict_support::scan("target", 0., 40.))
                .unwrap();
            sm.close().unwrap();
            check(&path, case, true);
            if export.is_some() {
                std::fs::write(
                    root.path().join("case.json"),
                    serde_json::to_vec(case).unwrap(),
                )
                .unwrap();
                let _kept = root.keep();
            }
        }
    }
}

#[test]
fn rust_reads_the_actual_casa_multi_feed_measurement_sets() {
    let Some(root) = std::env::var_os("CASA_RS_FEEDS_CPP_ROOT") else {
        eprintln!("skipped: actual CASA feed controls not selected");
        return;
    };
    for case in reference()["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["split"] == false)
    {
        let name = format!(
            "{}-{}-False.ms",
            case["count"],
            case["mount"].as_str().unwrap()
        );
        check(&Path::new(&root).join(name), case, false);
    }
}
