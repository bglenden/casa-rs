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
//! it predicted on Metal. Through the mosaic machine, a session of 12 m and
//! 7 m dishes on two fields predicts within the same tolerance.
//!
//! A session that only corrupts writes the same MeasurementSet whatever
//! backend it asks for: rows Metal does not predict get the host's noise.
//! Rows it predicts get their noise on the device, added to the model there
//! (#855), with the host's draws: with one noise term, a Metal session
//! that predicts and corrupts writes, bit for bit, the sum of the Metal
//! session that only predicts and the CPU session that only corrupts. That
//! holds too for a σ so small that some draws are subnormal, which the
//! host then draws as the device would flush them, and a σ of zero leaves
//! the model as it is.
//!
//! A patch after `openfromms` adds the DATA on disk after the prediction,
//! and its noise last, on Metal as on the CPU (#886 review): the device
//! leaves the noise to the host, so a large value on disk cancelled after
//! it cannot round it away.
//!
//! On a host without a Metal device the Metal comparisons print `skipped:
//! no Metal device` and pass: run them outside command sandboxes on Apple
//! silicon (`TESTING.md`).

mod pass_summary;
mod predict_support;

use std::path::Path;

use casa_imaging_runtime::{HostResources, ResourcePolicy};
use casa_simulation::{
    BackendRequest, NoiseSpec, PredictionOptions, SessionResources, Simulator, SimulatorOptions,
    VoltagePatternSpec,
};
use casa_test_support::ms_digest::measurement_set_manifest;
use casa_types::measures::{EpochRef, MEpoch};
use num_complex::Complex32;
use pass_summary::{PassSummary, capture};
use predict_support::{
    N, REFERENCE_MJD, Row, bits, data, predict_spec, read_main, scan, session_with_channels,
    session_with_dishes, write_model,
};

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

/// The mosaic session of 12 m and 7 m dishes in turn, its scans on the
/// model's centre and on the `offset` field (a pointing ramp), predicted
/// through the mosaic machine with `backend`: MAIN and the pass's summary.
fn predicted_mosaic(path: &Path, model: &Path, backend: BackendRequest) -> (Vec<Row>, PassSummary) {
    let dishes = [12.0, 7.0, 12.0, 7.0, 12.0, 7.0, 12.0, 7.0];
    let mut sm = session_with_dishes(path, "ALMA", "XX YY", CHANNELS, &dishes, resources(backend));
    sm.observe(&scan("target", 0.0, 600.0)).expect("observe");
    sm.observe(&scan("offset", 600.0, 1200.0)).expect("observe");
    sm.set_options(&PredictionOptions {
        ftmachine: "mosaic".to_string(),
        ..PredictionOptions::default()
    })
    .expect("setoptions");
    sm.predict(&predict_spec(model, false)).expect("predict");
    let ((), summary) = capture(|| sm.close().expect("close"));
    (read_main(path).0, summary.expect("the pass's summary"))
}

/// Metal predicts through the mosaic machine's dish-pair cells and their
/// pointing ramps as the CPU does, to the T0 tolerance (#841 Phase 2).
#[test]
fn metal_predicts_the_cpus_mosaic_within_the_t0_tolerance() {
    if !metal_device() {
        return;
    }
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = cube(directory.path());
    let (cpu, cpu_pass) = predicted_mosaic(
        &directory.path().join("cpu.ms"),
        &model,
        BackendRequest::Cpu,
    );
    let (metal, metal_pass) = predicted_mosaic(
        &directory.path().join("metal.ms"),
        &model,
        BackendRequest::Metal,
    );
    assert_eq!(cpu_pass.backend, "Cpu");
    assert_eq!(metal_pass.backend, "Metal", "the pass predicted on Metal");
    let ratio = relative_difference(&metal, &cpu);
    println!(
        "mosaic: max |metal - cpu| / max |cpu| = {ratio:.3e} over {} rows",
        metal.len()
    );
    assert!(
        ratio <= TOLERANCE,
        "max |metal - cpu| is {ratio:e} of max |cpu|"
    );
    assert!(ratio > 0.0, "Metal predicted the CPU's bits");
}

/// A session of 4 linear correlations, corrupted and never predicted.
fn noise_only(path: &Path, backend: BackendRequest) -> String {
    let mut sm: Simulator =
        session_with_channels(path, "ALMA", "XX XY YX YY", 4, resources(backend));
    sm.observe(&scan("target", 0.0, 200.0)).expect("observe");
    sm.set_noise(&NoiseSpec::simple(0.1)).expect("setnoise");
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

/// A session of 4 linear correlations with `backend` that observes a scan
/// of several write batches, predicts `model` into it when given one and
/// corrupts it with one noise term of σ `noise_jy` when given one, then
/// observes a scan it neither predicts nor corrupts; MAIN read back with the
/// pass's summary.
fn predicted_and_corrupted(
    path: &Path,
    model: Option<&Path>,
    noise_jy: Option<f64>,
    backend: BackendRequest,
) -> (Vec<Row>, PassSummary) {
    let mut sm = session_with_channels(path, "ALMA", "XX XY YX YY", CHANNELS, resources(backend));
    let long_s = 10.0 * LONG_SCAN_INTEGRATIONS as f64;
    sm.observe(&scan("target", 0.0, long_s)).expect("observe");
    if let Some(model) = model {
        sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
        sm.predict(&predict_spec(model, false)).expect("predict");
    }
    if let Some(simple_noise_jy) = noise_jy {
        sm.set_noise(&NoiseSpec::simple(simple_noise_jy))
            .expect("setnoise");
        sm.corrupt().expect("corrupt");
    }
    sm.observe(&scan("offset", long_s, long_s + 500.0))
        .expect("observe");
    let ((), summary) = capture(|| sm.close().expect("close"));
    (read_main(path).0, summary.expect("the pass's summary"))
}

#[test]
fn metal_adds_the_hosts_noise_to_the_model_on_the_device() {
    if !metal_device() {
        return;
    }
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = |name: &str| directory.path().join(name);
    let model = cube(directory.path());
    let (model_only, model_pass) =
        predicted_and_corrupted(&path("model.ms"), Some(&model), None, BackendRequest::Metal);
    // A σ the device draws exactly; one whose draws are partly subnormal,
    // which the device would flush, so the host draws it (#856 review); and
    // σ = 0, which adds nothing on either side, signed zeros included.
    for sigma in [0.1, 1.0e-36, 0.0] {
        let (both, both_pass) = predicted_and_corrupted(
            &path("both.ms"),
            Some(&model),
            Some(sigma),
            BackendRequest::Metal,
        );
        let (noise, _) =
            predicted_and_corrupted(&path("noise.ms"), None, Some(sigma), BackendRequest::Cpu);
        for ms in ["both.ms", "noise.ms"] {
            std::fs::remove_dir_all(path(ms)).expect("remove the run");
        }
        assert_eq!(
            both_pass.backend, "Metal",
            "σ {sigma}: the pass predicted on Metal"
        );
        assert!(both_pass.batches > 1, "σ {sigma}: {both_pass:?}");
        assert_eq!(both_pass.block_rows, model_pass.block_rows, "σ {sigma}");
        assert_eq!(both.len(), model_only.len(), "σ {sigma}: rows");
        assert_eq!(both.len(), noise.len(), "σ {sigma}: rows");
        let (mut predicted, mut subnormal) = (0, 0);
        for (index, ((both, model), noise)) in both.iter().zip(&model_only).zip(&noise).enumerate()
        {
            predicted += usize::from(model.data.iter().any(|sample| !zero(sample)));
            for (sample, ((found, model), noise)) in both
                .data
                .iter()
                .zip(&model.data)
                .zip(&noise.data)
                .enumerate()
            {
                // One term rounds once on either side: (model + noise); a
                // term of σ = 0 is skipped, leaving the model as it is.
                let expected = if sigma == 0.0 { *model } else { model + noise };
                subnormal += usize::from(found.re.is_subnormal() || found.im.is_subnormal());
                assert!(
                    found.re.to_bits() == expected.re.to_bits()
                        && found.im.to_bits() == expected.im.to_bits(),
                    "σ {sigma}, row {index}, sample {sample}: {found} against model {model} + \
                     noise {noise}"
                );
            }
        }
        assert!(predicted > 0, "σ {sigma}: the model reaches the rows");
        let noisy = noise
            .iter()
            .any(|row| row.data.iter().any(|sample| !zero(sample)));
        assert_eq!(noisy, sigma != 0.0, "σ {sigma}: the noise reaches the rows");
        if sigma == 1.0e-36 {
            assert!(subnormal > 0, "σ {sigma}: some noise is subnormal");
        }
    }
}

/// A flux whose samples' unit in the last place, 8 Jy, rounds away noise
/// of [`CANCELLED_NOISE_JY`] added to it.
const CANCELLED_JY: f32 = 1.0e8;

/// σ of the noise-order law's noise, Jy.
const CANCELLED_NOISE_JY: f64 = 0.25;

/// `sm.setnoise(simplenoise=σ)` of [`CANCELLED_NOISE_JY`], then
/// `sm.corrupt()`.
fn corrupt_cancelled(sm: &mut Simulator) {
    sm.set_noise(&NoiseSpec::simple(CANCELLED_NOISE_JY))
        .expect("setnoise");
    sm.corrupt().expect("corrupt");
}

/// The mean power of the real parts of `a − b`'s samples, Jy².
fn real_power(a: &[Row], b: &[Row]) -> f64 {
    let (mut power, mut samples) = (0.0, 0_usize);
    for (a, b) in a.iter().zip(b) {
        for (x, y) in a.data.iter().zip(&b.data) {
            power += f64::from(x.re - y.re).powi(2);
            samples += 1;
        }
    }
    power / samples as f64
}

/// That `power`, the real noise power of a run, is σ² of the noise-order
/// law's noise, to a tenth.
fn assert_noise_power(power: f64, what: &str) {
    let expected = CANCELLED_NOISE_JY * CANCELLED_NOISE_JY;
    assert!(
        (power - expected).abs() < 0.1 * expected,
        "{what}: real noise power {power}, against σ² {expected}"
    );
}

/// On Metal, as on the CPU, the noise of a patch after `openfromms`
/// follows the DATA it reads from disk (#886 review): a prediction of
/// +1e8 Jy added to one of −1e8 Jy closed before, and corrupted, writes
/// what closing the prediction and then corrupting in a third session
/// writes, bit for bit, keeping the noise. Added on the device before the
/// DATA on disk, it rounded away.
#[test]
fn metal_adds_the_noise_after_the_data_on_disk() {
    if !metal_device() {
        return;
    }
    let directory = tempfile::tempdir().expect("temporary directory");
    let model =
        |name: &str, flux: f32| write_model(directory.path(), name, &[vec![(N / 2, N / 2, flux)]]);
    let (minus, plus) = (
        model("minus.image", -CANCELLED_JY),
        model("plus.image", CANCELLED_JY),
    );
    let observed = |name: &str| {
        let path = directory.path().join(name);
        let backend = resources(BackendRequest::Metal);
        let mut sm = session_with_channels(&path, "ALMA", "XX YY", CHANNELS, backend);
        sm.observe(&scan("target", 0.0, 600.0)).expect("observe");
        sm.predict(&predict_spec(&minus, false)).expect("predict");
        sm.close().expect("close");
        path
    };
    let reopened = |path: &Path| {
        let options = SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: resources(BackendRequest::Metal),
            now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
        };
        Simulator::open_from_ms(path, options).expect("openfromms")
    };
    let add = |sm: &mut Simulator| sm.predict(&predict_spec(&plus, true)).expect("predict");
    let one = observed("one.ms");
    let mut sm = reopened(&one);
    add(&mut sm);
    corrupt_cancelled(&mut sm);
    let ((), pass) = capture(|| sm.close().expect("close"));
    assert_eq!(
        pass.expect("the patch's summary").backend,
        "Some(Metal)",
        "the patch predicted on Metal"
    );
    let two = observed("two.ms");
    let mut sm = reopened(&two);
    add(&mut sm);
    sm.close().expect("close");
    let sum = read_main(&two).0;
    let mut sm = reopened(&two);
    corrupt_cancelled(&mut sm);
    sm.close().expect("close");
    let (one, two) = (read_main(&one).0, read_main(&two).0);
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
    assert_noise_power(real_power(&one, &sum), "the reopened prediction");
}
