// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent full-polarized input recipes, without native output expectations.
use super::{input, predict_support, support};
use casa_calibration::{
    SimulatedJones, SimulatedSolution, SolutionValues, write_simulated_jones_table,
};
use casa_simulation::{ApplySpec, Simulator, SimulatorOptions};
use casa_tables::{Table, TableOptions};
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::{ArrayValue, Complex32, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use serde_json::Value as Json;
use std::path::{Path, PathBuf};

fn parameters(kind: &str, antenna: usize, channel: usize) -> Vec<Complex32> {
    let a = antenna as f64;
    let values: Vec<(f64, f64)> = if matches!(kind, "J" | "Jf") {
        vec![
            (1.1 + 0.03 * a, 0.2),
            (0.02 * a, 0.03),
            (-0.01 * a, 0.01),
            (0.8 + 0.02 * a, -0.1),
        ]
    } else {
        vec![(0.02 + 0.01 * a, 0.03), (-0.01 * a, 0.01)]
    };
    values
        .into_iter()
        .map(|(real, imaginary)| {
            let scale = 1. + 0.05 * channel as f64;
            Complex32::new((real * scale) as f32, (imaginary * scale) as f32)
        })
        .collect()
}

fn prepare(path: &Path, calibration: &Path, case: &Json) {
    input::prepare(path);
    let mut table = Table::open(TableOptions::new(path)).unwrap();
    let base = [
        Complex32::new(3., 4.),
        Complex32::new(-2., 0.5),
        Complex32::new(1.5, -0.2),
        Complex32::new(4., -1.),
    ];
    let data = ArrayD::from_shape_fn(IxDyn(&[4, 4]).f(), |index| {
        base[index[0]] * [1., 1.1, 1.2, 1.3][index[1]]
    });
    let mut times = Vec::new();
    for row in 0..table.row_count() {
        let ScalarValue::Float64(time) =
            table.cell_accessor(row, "TIME").unwrap().scalar().unwrap()
        else {
            panic!("TIME");
        };
        let time = *time;
        let ScalarValue::Int32(field) = table
            .cell_accessor(row, "FIELD_ID")
            .unwrap()
            .scalar()
            .unwrap()
        else {
            panic!("FIELD");
        };
        if times.last().is_none_or(|&(previous, _)| previous != time) {
            times.push((time, *field));
        }
        table
            .cell_accessor_mut(row, "DATA")
            .unwrap()
            .set(Value::Array(ArrayValue::Complex32(data.clone())))
            .unwrap();
    }
    table.save(TableOptions::new(path)).unwrap();
    let ms = casa_ms::MeasurementSet::open(path).unwrap();
    let rows = times.into_iter().flat_map(|(time_s, field_id)| {
        (0..predict_support::ANTENNAS).map(move |antenna| SimulatedSolution {
            time_s,
            field_id,
            spectral_window_id: 0,
            antenna: antenna as i32,
            values: SolutionValues::Receptors([Complex32::new(1., 0.); 2]),
        })
    });
    write_simulated_jones_table(&ms, calibration, SimulatedJones::Gain, rows).unwrap();
    let mut table = Table::open(TableOptions::new(calibration)).unwrap();
    let kind = case["kind"].as_str().unwrap();
    let channels = case["channels"].as_u64().unwrap() as usize;
    for row in 0..table.row_count() {
        let ScalarValue::Int32(antenna) = table
            .cell_accessor(row, "ANTENNA1")
            .unwrap()
            .scalar()
            .unwrap()
        else {
            panic!("ANTENNA");
        };
        let antenna = *antenna as usize;
        let count = parameters(kind, antenna, 0).len();
        let values: Vec<_> = (0..channels)
            .flat_map(|channel| parameters(kind, antenna, channel))
            .collect();
        let shape = IxDyn(&[count, channels]);
        let mut flags = ArrayD::from_elem(shape.clone().f(), false);
        if case["flag"] == true && antenna == 2 {
            flags[[1, 0]] = true;
        }
        for (column, value) in [
            (
                "CPARAM",
                ArrayValue::Complex32(ArrayD::from_shape_vec(shape.clone().f(), values).unwrap()),
            ),
            ("FLAG", ArrayValue::Bool(flags)),
            (
                "PARAMERR",
                ArrayValue::Float32(ArrayD::zeros(shape.clone().f())),
            ),
            ("SNR", ArrayValue::Float32(ArrayD::ones(shape.f()))),
        ] {
            table
                .cell_accessor_mut(row, column)
                .unwrap()
                .set(Value::Array(value))
                .unwrap();
        }
    }
    let mut info = table.info().clone();
    info.sub_type = format!("{kind} Jones");
    table.set_info(info);
    table.keywords_mut().upsert(
        "VisCal",
        Value::Scalar(ScalarValue::String(format!("{kind} Jones"))),
    );
    table.save(TableOptions::new(calibration)).unwrap();
    let path = calibration.join("SPECTRAL_WINDOW");
    let mut table = Table::open(TableOptions::new(&path)).unwrap();
    table
        .cell_accessor_mut(0, "NUM_CHAN")
        .unwrap()
        .set(Value::Scalar(ScalarValue::Int32(channels as i32)))
        .unwrap();
    for column in ["CHAN_FREQ", "CHAN_WIDTH", "RESOLUTION", "EFFECTIVE_BW"] {
        let values: Vec<f64> = (0..channels)
            .map(|channel| {
                if column == "CHAN_FREQ" {
                    predict_support::FREQUENCY_HZ
                        + predict_support::CHANNEL_HZ
                            * if channels == 1 { 1.5 } else { channel as f64 }
                } else {
                    predict_support::CHANNEL_HZ * if channels == 1 { 4. } else { 1. }
                }
            })
            .collect();
        table
            .cell_accessor_mut(0, column)
            .unwrap()
            .set(Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[channels]), values).unwrap(),
            )))
            .unwrap();
    }
    table.save(TableOptions::new(&path)).unwrap();
}

pub(super) fn run(case: &Json, workers: usize) -> Vec<Vec<Complex32>> {
    let export = std::env::var_os("CASA_RS_APPLY_MATRICES_OUT");
    let root = if let Some(out) = &export {
        std::fs::create_dir_all(out).unwrap();
        tempfile::tempdir_in(out).unwrap()
    } else {
        tempfile::tempdir().unwrap()
    };
    let path = root.path().join("data.ms");
    let calibration = root.path().join("terms.cal");
    let resources = predict_support::resources(workers, 2 << 30);
    let mut sm = predict_support::session(&path, "ALMA", "XX XY YX YY", resources);
    for (source, start, stop) in [("target", 0., 20.), ("offset", 20., 40.)] {
        sm.observe(&predict_support::scan(source, start, stop))
            .unwrap();
    }
    sm.close().unwrap();
    prepare(&path, &calibration, case);
    let mut sm = Simulator::open_from_ms(
        &path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources,
            now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .unwrap();
    let calibration = if let Some(casa) = std::env::var_os("CASA_RS_APPLY_MATRICES_CASA_TABLES") {
        assert!(export.is_none());
        PathBuf::from(casa)
            .join(case["name"].as_str().unwrap())
            .join("terms.cal")
    } else {
        calibration
    };
    sm.set_apply(&ApplySpec {
        table: calibration.to_string_lossy().into_owned(),
        kind: String::new(),
        interval_s: 0.,
        interp: "linear,nearest".into(),
        calwt: false,
        spwmap: vec![-1],
        opacity: 0.,
    })
    .unwrap();
    sm.corrupt().unwrap();
    sm.close().unwrap();
    let rows = support::compare(&path, case);
    if export.is_some() {
        std::fs::write(
            root.path().join("case.json"),
            serde_json::json!({"name":case["name"],"workers":workers}).to_string(),
        )
        .unwrap();
        let _kept = root.keep();
    }
    rows
}
