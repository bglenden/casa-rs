// SPDX-License-Identifier: LGPL-3.0-or-later
//! A session predicts the same DATA on the host's Metal device as on the
//! CPU (simulation Phase 1d, #841), to the T0 Metal law's tolerance:
//! `max |metal − cpu| ≤ 1e-4 · max |cpu|` over every sample.
//!
//! The sessions are ALMA's, with 2 and with 4 linear correlations, the
//! default voltage pattern (`setvp`), a cube model of one plane per channel
//! and autocorrelations written:
//! - scan 1, on the model's centre, has rows enough for several prediction
//!   blocks and two write batches, so the device's buffers are reused;
//! - scan 2 is below the elevation limit, every row flagged;
//! - a replacing `predict` covers both, then scan 3 is observed on another
//!   field and an incremental `predict` covers all three: scans 1 and 2 sum
//!   two image domains, scan 3 has one, and the predictor is rebuilt
//!   between them, from three sets of grids each with its device copy.
//!
//! Beside the tolerance, Metal leaves flagged rows and the cross-hands of
//! 4 correlations exactly zero, as the CPU does, and the pass reports that
//! it predicted on Metal.
//!
//! Noise is drawn on the host, so a session that only corrupts writes the
//! same MeasurementSet whatever backend it asks for.
//!
//! On a host without a Metal device the Metal comparisons print `skipped:
//! no Metal device` and pass: run them outside command sandboxes on Apple
//! silicon (`TESTING.md`).

mod pass_summary;
mod predict_support;

use std::path::Path;

use casa_imaging_runtime::{HostResources, ResourcePolicy};
use casa_simulation::{BackendRequest, NoiseSpec, SessionResources, Simulator, VoltagePatternSpec};
use casa_test_support::ms_digest::measurement_set_manifest;
use num_complex::Complex32;
use pass_summary::{PassSummary, capture};
use predict_support::{N, Row, predict_spec, read_main, scan, session_with_channels, write_model};

/// The T0 Metal law's tolerance, relative to the CPU's largest sample.
const TOLERANCE: f32 = 1.0e-4;

/// Channels of the sessions, and planes of the model.
const CHANNELS: usize = 32;

/// Integrations of scan 1, of 10 s: with 36 rows each, several prediction
/// blocks, and more rows than a write batch of 32 channels and 2
/// correlations (about 44,000 rows of the writer's 32 MiB target, which a
/// budget that holds the device predictor reaches).
const LONG_SCAN_INTEGRATIONS: usize = 1_500;

/// Whether this host has a Metal device, saying so when it has none.
fn metal_device() -> bool {
    let metal = HostResources::detect().expect("host").metal;
    if !metal {
        eprintln!("skipped: no Metal device");
    }
    metal
}

/// A pinned eight-thread host, with this host's Metal device when it has
/// one, and a policy of three workers and 2 GiB asking for `backend`.
fn resources(backend: BackendRequest) -> SessionResources {
    SessionResources::pinned(
        HostResources {
            threads: 8,
            performance_cores: 8,
            available_memory: 8 << 30,
            metal: HostResources::detect().expect("host").metal,
        },
        ResourcePolicy::Explicit {
            workers: 3,
            memory: 2 << 30,
        },
        backend,
    )
    .expect("resources")
}

/// A cube of one plane per channel: two points whose fluxes vary across the
/// band, one off the beam's centre.
fn cube(directory: &Path) -> std::path::PathBuf {
    let planes = (0..CHANNELS)
        .map(|plane| {
            vec![
                (N / 2 - 3, N / 2 + 5, 1.0 + plane as f32 * 0.05),
                (N / 2 + 12, N / 2 + 1, 0.4 - plane as f32 * 0.005),
                (N / 2 + 1, N / 2 - 9, 0.25),
            ]
        })
        .collect::<Vec<_>>();
    write_model(directory, "cube.image", &planes)
}

/// Write the predicting session of `stokes` to `path` with `backend`, and
/// read MAIN back with the pass's summary.
fn predicted(
    path: &Path,
    stokes: &str,
    model: &Path,
    backend: BackendRequest,
) -> (Vec<Row>, PassSummary) {
    let mut sm = session_with_channels(path, "ALMA", stokes, CHANNELS, resources(backend));
    let long_s = 10.0 * LONG_SCAN_INTEGRATIONS as f64;
    sm.observe(&scan("target", 0.0, long_s)).expect("observe");
    // Above the zenith: every row is flagged.
    sm.set_limits(1.0e-6, 91.0_f64.to_radians());
    sm.observe(&scan("target", long_s, long_s + 100.0))
        .expect("observe");
    sm.set_limits(1.0e-6, 8.0_f64.to_radians());
    sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
    sm.predict(&predict_spec(model, false)).expect("predict");
    sm.observe(&scan("offset", long_s + 100.0, long_s + 1100.0))
        .expect("observe");
    sm.predict(&predict_spec(model, true)).expect("predict");
    let ((), summary) = capture(|| sm.close().expect("close"));
    (read_main(path).0, summary.expect("the pass's summary"))
}

/// `max |metal − cpu| / max |cpu|` over every sample of every row, all of
/// which must be finite: `f32::max` drops a NaN (#853 review).
fn relative_difference(metal: &[Row], cpu: &[Row]) -> f32 {
    let samples = |rows: &[Row]| {
        rows.iter()
            .flat_map(|row| row.data.clone())
            .collect::<Vec<_>>()
    };
    let (metal, cpu) = (samples(metal), samples(cpu));
    assert_eq!(metal.len(), cpu.len(), "the same samples");
    for (backend, samples) in [("Metal", &metal), ("CPU", &cpu)] {
        assert!(
            samples.iter().all(|value| value.is_finite()),
            "every {backend} sample is finite"
        );
    }
    let scale = cpu
        .iter()
        .fold(0.0_f32, |peak, value| peak.max(value.norm()));
    assert!(scale > 0.0, "the model reaches the rows");
    let worst = metal
        .iter()
        .zip(&cpu)
        .fold(0.0_f32, |worst, (m, c)| worst.max((m - c).norm()));
    worst / scale
}

/// Whether `value` is zero, of either sign.
fn zero(value: &Complex32) -> bool {
    value.re == 0.0 && value.im == 0.0
}

#[test]
fn metal_predicts_the_cpus_data_within_the_t0_tolerance() {
    if !metal_device() {
        return;
    }
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = cube(directory.path());
    for (stokes, cross_hands) in [("XX YY", vec![]), ("XX XY YX YY", vec![1, 2])] {
        let name = stokes.replace(' ', "");
        let (cpu, cpu_pass) = predicted(
            &directory.path().join(format!("{name}-cpu.ms")),
            stokes,
            &model,
            BackendRequest::Cpu,
        );
        let (metal, metal_pass) = predicted(
            &directory.path().join(format!("{name}-metal.ms")),
            stokes,
            &model,
            BackendRequest::Metal,
        );
        assert_eq!(cpu_pass.backend, "Cpu", "{stokes}");
        assert_eq!(
            metal_pass.backend, "Metal",
            "{stokes}: the pass predicted on Metal"
        );
        assert_eq!(metal_pass.batch_rows, cpu_pass.batch_rows, "{stokes}");
        assert!(metal_pass.batches > 1, "{stokes}: {metal_pass:?}");
        assert_eq!(metal.len(), cpu.len(), "{stokes}: rows");
        let ratio = relative_difference(&metal, &cpu);
        println!(
            "{stokes}: max |metal - cpu| / max |cpu| = {ratio:.3e} over {} rows in {} batches",
            metal.len(),
            metal_pass.batches
        );
        assert!(
            ratio <= TOLERANCE,
            "{stokes}: max |metal - cpu| is {ratio:e} of max |cpu|"
        );
        // Device arithmetic differs from the host's in rounding: an exact
        // match means the CPU predicted both.
        assert!(ratio > 0.0, "{stokes}: Metal predicted the CPU's bits");
        let flagged = metal.iter().filter(|row| row.flag_row).count();
        assert!(
            flagged >= 360,
            "{stokes}: scan 2 is flagged ({flagged} rows)"
        );
        let correlations = stokes.split(' ').count();
        for (metal, cpu) in metal.iter().zip(&cpu) {
            assert_eq!(metal.flag_row, cpu.flag_row, "{stokes}: FLAG_ROW");
            if metal.flag_row {
                assert!(
                    metal.data.iter().all(zero),
                    "{stokes}: a flagged row predicts zero"
                );
            }
            for &hand in &cross_hands {
                assert!(
                    metal.data.iter().skip(hand).step_by(correlations).all(zero),
                    "{stokes}: cross-hand {hand} is exactly zero"
                );
            }
        }
    }
}

/// A session of 4 linear correlations, corrupted and never predicted.
fn noise_only(path: &Path, backend: BackendRequest) -> String {
    let mut sm: Simulator =
        session_with_channels(path, "ALMA", "XX XY YX YY", 4, resources(backend));
    sm.observe(&scan("target", 0.0, 200.0)).expect("observe");
    sm.set_noise(&NoiseSpec {
        mode: "simplenoise".to_string(),
        simple_noise_jy: 0.1,
        on_the_fly: true,
    })
    .expect("setnoise");
    sm.corrupt().expect("corrupt");
    sm.observe(&scan("offset", 200.0, 300.0)).expect("observe");
    sm.close().expect("close");
    measurement_set_manifest(path)
}

#[test]
fn noise_is_the_same_whatever_backend_is_asked_for() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = |name: &str| directory.path().join(name);
    let cpu = noise_only(&path("cpu.ms"), BackendRequest::Cpu);
    assert_eq!(
        noise_only(&path("auto.ms"), BackendRequest::Auto),
        cpu,
        "auto writes the CPU's MeasurementSet"
    );
    if metal_device() {
        assert_eq!(
            noise_only(&path("metal.ms"), BackendRequest::Metal),
            cpu,
            "metal writes the CPU's MeasurementSet"
        );
    }
}
