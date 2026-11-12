// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent frequency-table input matching the stated CASA recipe.
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Complex32, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::Complex64;
use serde_json::Value as Json;
use std::path::Path;

pub(super) fn is_frequency(kind: &str) -> bool {
    matches!(kind, "B" | "Tf" | "Df")
}

fn values(kind: &str, antenna: usize, elapsed: f64) -> Vec<Complex32> {
    let base = super::receptor_values(if kind == "Df" { "D" } else { "G" }, antenna, elapsed);
    let factors = [
        Complex64::new(1.0, 0.0),
        Complex64::new(1.1, 0.7),
        Complex64::new(-0.6, 1.5),
        Complex64::new(-1.4, -0.3),
    ];
    let offsets = [
        Complex64::new(0.0, 0.0),
        Complex64::new(0.06, 0.04),
        Complex64::new(-0.16, 0.10),
        Complex64::new(0.24, -0.08),
    ];
    (0..4)
        .flat_map(|channel| {
            (0..if kind == "Tf" { 1 } else { 2 }).map(move |receptor| {
                let base =
                    Complex64::new(f64::from(base[receptor].re), f64::from(base[receptor].im));
                let value = if kind == "Df" {
                    base + offsets[channel]
                } else {
                    base * factors[channel]
                };
                Complex32::new(value.re as f32, value.im as f32)
            })
        })
        .collect()
}

pub(super) fn prepare(path: &Path, case: &Json) {
    let kind = case["kind"].as_str().unwrap();
    let receptors = if kind == "Tf" { 1 } else { 2 };
    let first = case["calibration"]["first_time_s"].as_f64().unwrap();
    let mut table = Table::open(TableOptions::new(path)).unwrap();
    for row in 0..table.row_count() {
        let ScalarValue::Int32(antenna) = table
            .cell_accessor(row, "ANTENNA1")
            .unwrap()
            .scalar()
            .unwrap()
        else {
            panic!("antenna")
        };
        let ScalarValue::Float64(time) =
            table.cell_accessor(row, "TIME").unwrap().scalar().unwrap()
        else {
            panic!("time")
        };
        let (antenna, elapsed) = (*antenna as usize, *time - first);
        let shape = [receptors, 4];
        let values = if case["probe"] == "spline_analytic" {
            vec![1.0, 2.0, 0.5, 1.25]
                .into_iter()
                .map(|amplitude| Complex32::new(amplitude, 0.0))
                .collect()
        } else {
            values(kind, antenna, elapsed)
        };
        let parameters =
            ArrayValue::Complex32(ArrayD::from_shape_vec(IxDyn(&shape).f(), values).unwrap());
        table
            .cell_accessor_mut(row, "CPARAM")
            .unwrap()
            .set(Value::Array(parameters))
            .unwrap();
        let mut flags = vec![false; receptors * 4];
        if antenna == 0 {
            let probe = case["probe"].as_str().unwrap();
            if probe.starts_with("flag_channel") {
                flags[receptors] = true;
            } else if probe == "all_flagged" {
                flags.fill(true);
            } else if probe == "one_valid" {
                flags.fill(true);
                flags[2 * receptors..3 * receptors].fill(false);
            }
        }
        table
            .cell_accessor_mut(row, "FLAG")
            .unwrap()
            .set(Value::Array(ArrayValue::Bool(
                ArrayD::from_shape_vec(IxDyn(&[receptors, 4]).f(), flags).unwrap(),
            )))
            .unwrap();
        for column in ["PARAMERR", "SNR"] {
            table
                .cell_accessor_mut(row, column)
                .unwrap()
                .set(Value::Array(ArrayValue::Float32(ArrayD::from_elem(
                    IxDyn(&[receptors, 4]).f(),
                    0.0,
                ))))
                .unwrap();
        }
    }
    let subtype = format!("{kind} Jones");
    let mut info = table.info().clone();
    info.sub_type = subtype.clone();
    table.set_info(info);
    table
        .keywords_mut()
        .upsert("VisCal", Value::Scalar(ScalarValue::String(subtype)));
    table.save(TableOptions::new(path)).unwrap();
    let path = path.join("SPECTRAL_WINDOW");
    let mut window = Table::open(TableOptions::new(&path)).unwrap();
    let ArrayValue::Float64(frequencies) = window
        .cell_accessor(0, "CHAN_FREQ")
        .unwrap()
        .array()
        .unwrap()
    else {
        panic!("frequencies")
    };
    let probe = case["probe"].as_str().unwrap();
    let mut frequencies = frequencies
        .iter()
        .map(|frequency| {
            frequency
                + if probe == "spline_analytic" {
                    -7_812_500.0
                } else if probe == "tie_shifted" {
                    7_812_500.0
                } else if probe.contains("shifted") {
                    250_000.0
                } else {
                    0.0
                }
        })
        .collect::<Vec<_>>();
    if probe == "descending_shifted" {
        frequencies.reverse();
    }
    let frequencies = ArrayD::from_shape_vec(IxDyn(&[frequencies.len()]), frequencies).unwrap();
    assert_eq!(
        frequencies.iter().copied().collect::<Vec<_>>(),
        case["calibration"]["frequencies_hz"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_f64().unwrap())
            .collect::<Vec<_>>()
    );
    window
        .cell_accessor_mut(0, "CHAN_FREQ")
        .unwrap()
        .set(Value::Array(ArrayValue::Float64(frequencies)))
        .unwrap();
    window.save(TableOptions::new(&path)).unwrap();
}
