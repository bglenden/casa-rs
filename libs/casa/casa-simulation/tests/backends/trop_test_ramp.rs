// SPDX-License-Identifier: LGPL-3.0-or-later
//! An image predicted on the device still receives its source-defined test ramp.
use super::*;
use casa_simulation::TroposphereSpec;

#[test]
fn metal_prediction_composes_with_the_test_troposphere() {
    if !metal_device() {
        return;
    }
    for simint_s in [-1., 25.] {
        compare(simint_s);
    }
}

fn compare(simint_s: f64) {
    let root = tempfile::tempdir().unwrap();
    let model = cube(root.path());
    let run = |name: &str, backend| {
        let path = root.path().join(name);
        let mut sm =
            session_with_channels(&path, "ALMA", "XX XY YX YY", CHANNELS, resources(backend));
        sm.observe(&scan("target", 0., 40.)).unwrap();
        sm.predict(&predict_spec(&model, false)).unwrap();
        sm.set_seed(19);
        sm.set_trop(&TroposphereSpec {
            mode: "test".into(),
            simint_s,
            water_vapour_mm: 0.01,
            ..TroposphereSpec::default()
        })
        .unwrap();
        sm.corrupt().unwrap();
        sm.corrupt().unwrap();
        let ((), summary) = capture(|| sm.close().unwrap());
        (read_main(&path).0, summary.unwrap())
    };
    let (cpu, host) = run("cpu.ms", BackendRequest::Cpu);
    let (metal, device) = run("metal.ms", BackendRequest::Metal);
    assert_eq!(host.backend, "Cpu");
    assert_eq!(device.backend, "Metal");
    let scale = cpu
        .iter()
        .flat_map(|r| &r.data)
        .map(|v| v.norm())
        .fold(0., f32::max);
    let error = cpu
        .iter()
        .zip(&metal)
        .flat_map(|(a, b)| a.data.iter().zip(&b.data))
        .map(|(a, b)| (*a - *b).norm())
        .fold(0., f32::max);
    println!("test ramp CPU/Metal relative residual {}", error / scale);
    assert!(error <= 1e-4 * scale);
}
