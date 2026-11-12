// SPDX-License-Identifier: LGPL-3.0-or-later
//! Actual Metal prediction composes with a frequency-dependent calibration term.
use super::*;
use casa_calibration::{
    SimulatedJones, SimulatedSolution, SolutionValues, write_simulated_jones_table,
};
use casa_simulation::ApplySpec;

#[test]
fn metal_prediction_composes_with_channel_dependent_table_application() {
    check_time_mode("linear");
}

#[test]
fn metal_prediction_composes_with_higher_time_interpolation() {
    check_time_mode("cubic");
    check_time_mode("spline");
}

fn check_time_mode(mode: &str) {
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
    write_simulated_jones_table(&ms, &table, SimulatedJones::Troposphere, rows).unwrap();
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
                kind: "Tf".into(),
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
