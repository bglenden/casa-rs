// SPDX-License-Identifier: LGPL-3.0-or-later
//! Feed-specific UVW streams reach image prediction on both backends.
use super::*;

#[test]
fn multiple_feed_predictions_and_noise_agree_on_actual_metal() {
    if !metal_device() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let model = cube(root.path());
    let run = |name: &str, backend| {
        let path = root.path().join(name);
        let mut sm =
            session_with_channels(&path, "ALMA", "XX XY YX YY", CHANNELS, resources(backend));
        sm.set_feed(
            "list",
            &[0., 0.0002],
            &[0., -0.0001],
            &["X".into(), "X".into()],
        )
        .unwrap();
        sm.observe(&scan("target", 0., 40.)).unwrap();
        sm.predict(&predict_spec(&model, false)).unwrap();
        sm.set_seed(19);
        sm.set_noise(&NoiseSpec::simple(0.1)).unwrap();
        sm.corrupt().unwrap();
        let ((), pass) = capture(|| sm.close().unwrap());
        (read_main(&path).0, pass.unwrap())
    };
    let (cpu, host) = run("cpu.ms", BackendRequest::Cpu);
    let (metal, device) = run("metal.ms", BackendRequest::Metal);
    assert_eq!(cpu.len(), 288);
    assert_eq!(host.backend, "Cpu");
    assert_eq!(device.backend, "Metal");
    let difference = relative_difference(&metal, &cpu);
    println!("multi-feed actual Metal relative residual {difference:e}");
    assert!(difference < TOLERANCE);
    for feed in cpu.chunks_exact(144) {
        assert!(feed.iter().any(|r| r.data.iter().any(|v| v.norm() > 0.5)));
    }
}
