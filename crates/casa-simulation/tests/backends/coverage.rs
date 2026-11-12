// SPDX-License-Identifier: LGPL-3.0-or-later
//! The device predicts both windows; uncovered corruption remains absent.
use super::*;
use casa_calibration::{
    SimulatedJones, SimulatedSolution, SolutionValues, write_simulated_jones_table,
};
use casa_simulation::{ApplySpec, SpectralWindowSpec};
use casa_types::measures::frequency::FrequencyRef;

#[test]
fn actual_metal_prediction_preserves_whole_step_window_coverage() {
    if !metal_device() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let reference = root.path().join("reference.ms");
    let mut sm = session_with_channels(
        &reference,
        "ALMA",
        "XX XY YX YY",
        CHANNELS,
        resources(BackendRequest::Cpu),
    );
    sm.observe(&scan("target", 0.0, 20.0)).unwrap();
    sm.close().unwrap();
    let table = root.path().join("g.cal");
    let ms = casa_ms::MeasurementSet::open(&reference).unwrap();
    write_simulated_jones_table(
        &ms,
        &table,
        SimulatedJones::Gain,
        (0..predict_support::ANTENNAS).map(|antenna| SimulatedSolution {
            time_s: 0.0,
            field_id: 0,
            spectral_window_id: 0,
            antenna: antenna as i32,
            values: SolutionValues::Receptors([Complex32::new(2.0, 0.0); 2]),
        }),
    )
    .unwrap();
    let model = cube(root.path());
    let run = |name: &str, backend: BackendRequest, calibrated: bool| {
        let path = root.path().join(name);
        let mut sm =
            session_with_channels(&path, "ALMA", "XX XY YX YY", CHANNELS, resources(backend));
        sm.observe(&scan("target", 0.0, 20.0)).unwrap();
        if calibrated {
            sm.set_apply(&ApplySpec {
                table: table.to_string_lossy().into_owned(),
                kind: String::new(),
                interval_s: 0.0,
                interp: "nearest".into(),
                calwt: false,
                spwmap: vec![-1],
                opacity: 0.0,
            })
            .unwrap();
            sm.set_seed(5);
            sm.set_noise(&NoiseSpec::simple(0.1)).unwrap();
        }
        sm.set_spectral_window(&SpectralWindowSpec {
            name: "later".into(),
            start_frequency_hz: predict_support::FREQUENCY_HZ,
            channel_width_hz: predict_support::CHANNEL_HZ,
            resolution_hz: predict_support::CHANNEL_HZ,
            frame: FrequencyRef::TOPO,
            channel_count: CHANNELS,
            stokes: "XX XY YX YY".into(),
        })
        .unwrap();
        let mut later = scan("target", 20.0, 30.0);
        later.spectral_window = "later".into();
        sm.observe(&later).unwrap();
        sm.predict(&predict_spec(&model, false)).unwrap();
        if calibrated {
            sm.corrupt().unwrap();
        }
        let ((), summary) = capture(|| sm.close().unwrap());
        (read_main(&path).0, summary.unwrap())
    };
    let (plain, _) = run("plain.ms", BackendRequest::Cpu, false);
    let (cpu, cpu_pass) = run("cpu.ms", BackendRequest::Cpu, true);
    let (metal, metal_pass) = run("metal.ms", BackendRequest::Metal, true);
    assert_eq!(cpu_pass.backend, "Cpu");
    assert_eq!(metal_pass.backend, "Metal");
    assert_eq!(plain.len(), 108);
    assert_eq!(plain.len(), cpu.len());
    assert!(
        plain[72..]
            .iter()
            .flat_map(|row| &row.data)
            .any(|value| value.norm() > 0.1)
    );
    assert!(relative_difference(&cpu[..72], &plain[..72]) > 0.5);
    for (a, b) in plain[72..].iter().zip(&cpu[72..]) {
        assert_eq!(
            a.data, b.data,
            "uncovered gain and noise must both be skipped"
        );
    }
    let difference = relative_difference(&metal, &cpu);
    println!("whole-step coverage after actual Metal prediction: max-relative {difference:e}");
    assert!(difference <= TOLERANCE);
}
