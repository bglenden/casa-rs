// SPDX-License-Identifier: LGPL-3.0-or-later
//! An image predicted on the device still receives its saved additive field.
use super::*;

#[test]
fn metal_prediction_composes_with_saved_additive_noise() {
    if !metal_device() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let model = cube(root.path());
    for mode in ["simplenoise", "tsys-manual", "tsys-atm"] {
        let run = |name: &str, backend| {
            let path = root.path().join(format!("{mode}-{name}"));
            let mut sm =
                session_with_channels(&path, "ALMA", "XX XY YX YY", CHANNELS, resources(backend));
            sm.observe(&scan("target", 0., 40.)).unwrap();
            sm.predict(&predict_spec(&model, false)).unwrap();
            sm.set_seed(19);
            sm.set_noise(&NoiseSpec {
                mode: mode.into(),
                water_vapour_mm: 1.,
                simple_noise_jy: 0.25,
                on_the_fly: false,
                ..NoiseSpec::default()
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
        println!("saved noise CPU/Metal relative residual {}", error / scale);
        assert!(error <= 1e-4 * scale);
    }
}
