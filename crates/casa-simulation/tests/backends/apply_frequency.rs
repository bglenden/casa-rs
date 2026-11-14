// SPDX-License-Identifier: LGPL-3.0-or-later
//! Actual Metal prediction composes with a frequency-dependent calibration term.
use super::*;
use casa_calibration::{
    SimulatedJones, SimulatedSolution, SolutionValues, write_simulated_jones_table,
};
use casa_simulation::ApplySpec;

#[test]
fn metal_prediction_composes_with_channel_dependent_table_application() {
    check_time_mode("linear", false);
}

#[test]
fn metal_prediction_composes_with_higher_time_interpolation() {
    check_time_mode("cubic", false);
    check_time_mode("spline", false);
}

#[test]
fn metal_prediction_composes_with_full_polarized_table_application() {
    check_time_mode("linear", true);
}

fn check_time_mode(mode: &str, full: bool) {
    if !metal_device() {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let reference = directory.path().join("reference.ms");
    let mut sm = session_with_channels(
        &reference,
        "ALMA",
        "XX XY YX YY",
        CHANNELS,
        resources(BackendRequest::Cpu),
    );
    sm.observe(&scan("target", 0.0, 20.0)).unwrap();
    sm.close().unwrap();
    let ms = casa_ms::MeasurementSet::open(&reference).unwrap();
    let table = directory.path().join("frequency.cal");
    let casa_types::ScalarValue::Float64(first) = ms
        .main_table()
        .cell_accessor(0, "TIME")
        .unwrap()
        .scalar()
        .unwrap()
    else {
        panic!("TIME")
    };
    let first = *first - 5.;
    let rows = (0..if mode == "linear" { 1 } else { 4 }).flat_map(|index| {
        (0..predict_support::ANTENNAS).map(move |antenna| SimulatedSolution {
            time_s: first + 10. * index as f64,
            field_id: 0,
            spectral_window_id: 0,
            antenna: antenna as i32,
            values: SolutionValues::Channels(
                (0..CHANNELS)
                    .map(|channel| {
                        Complex32::from_polar(
                            (1.2 + 0.01 * channel as f32) * [1., 2., 0.5, 1.25][index],
                            0.03 * (antenna * channel) as f32 + 0.1 * index as f32,
                        )
                    })
                    .collect(),
            ),
        })
    });
    write_simulated_jones_table(&ms, &table, SimulatedJones::Troposphere, rows, None).unwrap();
    if full {
        full_leakage(&table);
    }
    let model = cube(directory.path());
    let run = |name: &str, backend: BackendRequest, calibrated: bool| {
        let path = directory.path().join(name);
        let mut sm =
            session_with_channels(&path, "ALMA", "XX XY YX YY", CHANNELS, resources(backend));
        sm.observe(&scan("target", 0.0, 40.0)).unwrap();
        sm.predict(&predict_spec(&model, false)).unwrap();
        if calibrated {
            sm.set_apply(&ApplySpec {
                table: table.to_string_lossy().into_owned(),
                kind: if full { "Dfgen" } else { "Tf" }.into(),
                interval_s: 0.0,
                interp: format!("{mode},linear"),
                calwt: true,
                spwmap: vec![-1],
                opacity: 0.0,
            })
            .unwrap();
            sm.corrupt().unwrap();
        }
        let ((), summary) = capture(|| sm.close().unwrap());
        (read_main(&path).0, summary.unwrap())
    };
    let (plain, _) = run("plain.ms", BackendRequest::Cpu, false);
    let (cpu, cpu_pass) = run("cpu.ms", BackendRequest::Cpu, true);
    let (metal, metal_pass) = run("metal.ms", BackendRequest::Metal, true);
    assert_eq!(cpu_pass.backend, "Cpu");
    assert_eq!(
        metal_pass.backend, "Metal",
        "prediction actually used the device"
    );
    assert!(
        relative_difference(&cpu, &plain) > 0.05,
        "calibration is observable"
    );
    let difference = relative_difference(&metal, &cpu);
    println!(
        "{mode} time/frequency table after actual Metal prediction: max-relative {difference:e}"
    );
    assert!(difference <= TOLERANCE);
    for (cpu, metal) in cpu.iter().zip(&metal) {
        assert_eq!(cpu.antennas, metal.antennas);
        assert_eq!(cpu.flag_row, metal.flag_row);
    }
}

fn full_leakage(path: &std::path::Path) {
    use casa_tables::{Table, TableOptions};
    use casa_types::{ArrayValue, ScalarValue, Value};
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};
    let mut table = Table::open(TableOptions::new(path)).unwrap();
    for row in 0..table.row_count() {
        let ArrayValue::Complex32(values) =
            table.cell_accessor(row, "CPARAM").unwrap().array().unwrap()
        else {
            panic!("CPARAM");
        };
        let shape = IxDyn(&[2, CHANNELS]);
        let values = ArrayD::from_shape_fn(shape.clone().f(), |index| {
            values[[0, index[1]]] * if index[0] == 0 { 0.2 } else { -0.15 }
        });
        for (column, value) in [
            ("CPARAM", ArrayValue::Complex32(values)),
            (
                "FLAG",
                ArrayValue::Bool(ArrayD::from_elem(shape.clone().f(), false)),
            ),
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
    info.sub_type = "Dfgen Jones".into();
    table.set_info(info);
    table.keywords_mut().upsert(
        "VisCal",
        Value::Scalar(ScalarValue::String("Dfgen Jones".into())),
    );
    table.save(TableOptions::new(path)).unwrap();
}
