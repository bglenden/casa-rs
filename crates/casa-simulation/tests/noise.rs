// SPDX-License-Identifier: LGPL-3.0-or-later
//! Thermal noise through the session: `setseed`, `setnoise`, `corrupt` and
//! `reset`, read back from the MeasurementSets `close` writes (simulation
//! Phase 1d, #841).
//!
//! The expected behaviour is CASA 6.7.6's, as probed for Phase 1d: every
//! correlation of every row gets noise, flagged rows included, with
//! independent real and imaginary parts of variance σ² on cross-correlations
//! and real-only noise on autocorrelations; WEIGHT and SIGMA are unchanged;
//! two terms with the same seed add coherently (4σ²) and a term applied twice
//! independently (2σ²); a step covers only the scans observed before it.
//! Variances are checked within five standard errors.

use std::path::Path;

use casa_imaging_runtime::{HostResources, ResourcePolicy};
use casa_ms::MeasurementSet;
use casa_ms::columns::data_columns::DataColumn;
use casa_ms::columns::flag_columns::FlagRowColumn;
use casa_ms::columns::main_ids::{antenna1, antenna2};
use casa_ms::columns::weight_columns::{SigmaColumn, WeightColumn};
use casa_simulation::{
    ArrayConfig, BackendRequest, CasaOutcome, CoordinateSystem, DEFAULT_SEED, NoiseSpec,
    ObserveSpec, SessionResources, Simulator, SimulatorOptions, SimulatorResult,
    SpectralWindowSpec, StateSpec,
};
use casa_types::ArrayValue;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch};
use ndarray::IxDyn;
use num_complex::Complex32;

/// 2024-06-01 00:00 UTC.
const REFERENCE_MJD: f64 = 60_462.0;
const SIGMA_JY: f64 = 0.1;
const ANTENNAS: usize = 10;
const CHANNELS: usize = 4;

fn options(resources: SessionResources) -> SimulatorOptions {
    SimulatorOptions {
        measures: casa_test_support::deterministic_measures_provider(),
        resources,
        now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
    }
}

/// A pinned eight-thread host, with a policy of `workers` workers (the
/// writer's thread among them) and 1 GiB.
fn resources_with(workers: usize) -> SessionResources {
    SessionResources::pinned(
        HostResources {
            threads: 8,
            performance_cores: 8,
            available_memory: 4 << 30,
            metal: false,
        },
        ResourcePolicy::Explicit {
            workers,
            memory: 1 << 30,
        },
        BackendRequest::Cpu,
    )
    .expect("resources")
}

fn system_resources() -> SessionResources {
    resources_with(1)
}

/// `count` 25 m antennas at the VLA, 100 m apart.
fn vla_config(count: usize) -> ArrayConfig {
    let centre = [-1_601_185.0, -5_041_977.0, 3_554_875.0];
    let along = |axis: usize, step: f64| {
        (0..count)
            .map(|index| centre[axis] + step * index as f64)
            .collect()
    };
    ArrayConfig {
        telescope: "VLA".to_string(),
        x: along(0, 100.0),
        y: along(1, -30.0),
        z: along(2, 60.0),
        dish_diameter: vec![25.0],
        offset: vec![0.0],
        mount: vec!["alt-az".to_string()],
        antenna_names: vec!["ea".to_string()],
        pad_names: vec!["W".to_string()],
        coordinate_system: CoordinateSystem::Global,
    }
}

fn scan(start_s: f64, stop_s: f64) -> ObserveSpec {
    ObserveSpec {
        source: "target".to_string(),
        spectral_window: "LBand".to_string(),
        start_s,
        stop_s,
        add_observation: false,
        state: StateSpec {
            sig: true,
            reference: false,
            cal: 0.0,
            load: 0.0,
            sub_scan: 0,
            obs_mode: "OBSERVE_TARGET.ON_SOURCE".to_string(),
        },
        observer: "tester".to_string(),
        project: "noise".to_string(),
    }
}

/// A session with `ANTENNAS` antennas (autocorrelations written), one
/// `CHANNELS`-channel window of `stokes`, and one field, before `observe`.
fn configured(path: &Path, stokes: &str, resources: SessionResources) -> Simulator {
    let mut session = Simulator::open(path, options(resources));
    session
        .set_config(&vla_config(ANTENNAS))
        .expect("setconfig");
    session
        .set_spectral_window(&SpectralWindowSpec {
            name: "LBand".to_string(),
            start_frequency_hz: 1.4e9,
            channel_width_hz: 1.0e6,
            resolution_hz: 1.0e6,
            frame: FrequencyRef::TOPO,
            channel_count: CHANNELS,
            stokes: stokes.to_string(),
        })
        .expect("setspwindow");
    session
        .set_field(
            "target",
            &MDirection::from_angles(
                150.0_f64.to_radians(),
                34.0_f64.to_radians(),
                DirectionRef::J2000,
            ),
            "",
            0.0,
        )
        .expect("setfield");
    session
        .set_times(10.0, false, MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC))
        .expect("settimes");
    session
}

fn simple_noise(sigma_jy: f64) -> NoiseSpec {
    NoiseSpec {
        mode: "simplenoise".to_string(),
        simple_noise_jy: sigma_jy,
        on_the_fly: true,
    }
}

/// MAIN as the tests read it.
struct Main {
    correlations: usize,
    /// Each row's DATA, correlation fastest.
    data: Vec<Vec<Complex32>>,
    autocorrelation: Vec<bool>,
    flag_row: Vec<bool>,
    weight: Vec<ArrayValue>,
    sigma: Vec<ArrayValue>,
}

fn read_main(path: &Path) -> Main {
    let ms = MeasurementSet::open(path).expect("open");
    let main = ms.main_table();
    let data = DataColumn::data(main).expect("DATA");
    let (first, second) = (antenna1(main), antenna2(main));
    let flag_row = FlagRowColumn::new(main);
    let (weight, sigma) = (WeightColumn::new(main), SigmaColumn::new(main));
    let mut read = Main {
        correlations: 0,
        data: Vec::new(),
        autocorrelation: Vec::new(),
        flag_row: Vec::new(),
        weight: Vec::new(),
        sigma: Vec::new(),
    };
    for row in 0..ms.row_count() {
        let ArrayValue::Complex32(cell) = data.get(row).expect("DATA cell") else {
            panic!("DATA is not Complex32");
        };
        let [correlations, channels] = cell.shape() else {
            panic!("DATA cells are matrices");
        };
        read.correlations = *correlations;
        read.data.push(
            (0..*channels)
                .flat_map(|channel| (0..*correlations).map(move |c| (c, channel)))
                .map(|(c, channel)| cell[IxDyn(&[c, channel])])
                .collect(),
        );
        read.autocorrelation
            .push(first.get(row).expect("ANTENNA1") == second.get(row).expect("ANTENNA2"));
        read.flag_row.push(flag_row.get(row).expect("FLAG_ROW"));
        read.weight.push(weight.get(row).expect("WEIGHT").clone());
        read.sigma.push(sigma.get(row).expect("SIGMA").clone());
    }
    read
}

/// Every DATA sample, rows back to back.
fn samples(main: &Main) -> Vec<Complex32> {
    main.data.iter().flatten().copied().collect()
}

fn variance(values: &[f64]) -> f64 {
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n
}

/// The variance is `expected` within five standard errors.
fn assert_variance(values: &[f64], expected: f64, what: &str) {
    let measured = variance(values);
    let tolerance = 5.0 * expected * (2.0 / values.len() as f64).sqrt();
    assert!(
        (measured - expected).abs() < tolerance,
        "{what}: variance {measured:.6}, expected {expected:.6} ± {tolerance:.6} over {} samples",
        values.len()
    );
}

/// Per-correlation real and imaginary parts of the cross-correlation or the
/// autocorrelation samples.
fn class(main: &Main, correlation: usize, autocorrelations: bool) -> (Vec<f64>, Vec<f64>) {
    let mut re = Vec::new();
    let mut im = Vec::new();
    for (cell, &auto) in main.data.iter().zip(&main.autocorrelation) {
        if auto != autocorrelations {
            continue;
        }
        for sample in cell.iter().skip(correlation).step_by(main.correlations) {
            re.push(f64::from(sample.re));
            im.push(f64::from(sample.im));
        }
    }
    (re, im)
}

/// Write one session: `steps` runs after the first 100 s scan is observed.
fn write(
    path: &Path,
    stokes: &str,
    steps: impl FnOnce(&mut Simulator) -> SimulatorResult<()>,
) -> Main {
    write_with(path, stokes, system_resources(), steps)
}

fn write_with(
    path: &Path,
    stokes: &str,
    resources: SessionResources,
    steps: impl FnOnce(&mut Simulator) -> SimulatorResult<()>,
) -> Main {
    let mut session = configured(path, stokes, resources);
    session.observe(&scan(0.0, 1000.0)).expect("observe");
    steps(&mut session).expect("steps");
    session.close().expect("close");
    read_main(path)
}

fn once(session: &mut Simulator) -> SimulatorResult<()> {
    session.set_noise(&simple_noise(SIGMA_JY))?;
    session.corrupt()
}

#[test]
fn every_correlation_gets_noise_of_variance_sigma_squared_and_autos_only_real() {
    let directory = tempfile::tempdir().expect("temporary directory");
    for stokes in ["XX YY", "XX XY YX YY", "RR"] {
        let main = write(&directory.path().join(format!("{stokes}.ms")), stokes, once);
        let expected = SIGMA_JY * SIGMA_JY;
        for correlation in 0..main.correlations {
            let what = format!("{stokes} correlation {correlation}");
            let (re, im) = class(&main, correlation, false);
            assert_variance(&re, expected, &format!("{what} cross real"));
            assert_variance(&im, expected, &format!("{what} cross imaginary"));
            let (auto_re, auto_im) = class(&main, correlation, true);
            assert_variance(&auto_re, expected, &format!("{what} auto real"));
            assert!(
                auto_im.iter().all(|&v| v.to_bits() == 0),
                "{what}: autocorrelation imaginary parts stay exactly zero"
            );
        }
    }
}

#[test]
fn corrupt_leaves_weight_and_sigma_and_corrupts_flagged_rows() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let flagged = |session: &mut Simulator| {
        // Above the zenith: every row is flagged.
        session.set_limits(1.0e-6, 91.0_f64.to_radians());
        session.observe(&scan(1000.0, 1100.0))?;
        once(session)
    };
    let noisy = write(&directory.path().join("noisy.ms"), "XX YY", flagged);
    let clean = write(&directory.path().join("clean.ms"), "XX YY", |session| {
        session.set_limits(1.0e-6, 91.0_f64.to_radians());
        session.observe(&scan(1000.0, 1100.0))
    });
    assert_eq!(noisy.weight, clean.weight, "WEIGHT is unchanged");
    assert_eq!(noisy.sigma, clean.sigma, "SIGMA is unchanged");
    assert_eq!(noisy.flag_row, clean.flag_row);
    let flagged_rows = noisy
        .data
        .iter()
        .zip(&noisy.flag_row)
        .filter(|(_, flagged)| **flagged)
        .collect::<Vec<_>>();
    assert!(flagged_rows.len() >= 10 * ANTENNAS * (ANTENNAS + 1) / 2);
    assert!(
        flagged_rows
            .iter()
            .all(|(cell, _)| cell.iter().all(|sample| sample.re != 0.0)),
        "flagged rows get noise"
    );
    assert!(
        samples(&clean)
            .iter()
            .all(|sample| sample.norm_sqr() == 0.0)
    );
}

#[test]
fn terms_with_one_seed_add_coherently_and_repeated_corrupts_independently() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = |name: &str| directory.path().join(name);
    let one = samples(&write(&path("one.ms"), "XX YY", once));
    let two_terms = samples(&write(&path("two-terms.ms"), "XX YY", |session| {
        session.set_noise(&simple_noise(SIGMA_JY))?;
        session.set_noise(&simple_noise(SIGMA_JY))?;
        session.corrupt()
    }));
    // σ·n rounds to the same f32 for both terms, and doubling is exact.
    assert!(
        two_terms
            .iter()
            .zip(&one)
            .all(|(two, one)| *two == *one * 2.0),
        "two terms with one seed draw the same noise (variance 4σ²)"
    );
    let twice = samples(&write(&path("twice.ms"), "XX YY", |session| {
        session.set_noise(&simple_noise(SIGMA_JY))?;
        session.corrupt()?;
        session.corrupt()
    }));
    let real = |values: &[Complex32]| values.iter().map(|v| f64::from(v.re)).collect::<Vec<_>>();
    let imaginary =
        |values: &[Complex32]| values.iter().map(|v| f64::from(v.im)).collect::<Vec<_>>();
    // Autocorrelation imaginary parts are zero: compare the real parts, and
    // the imaginary parts against their own (smaller) count.
    let expected = SIGMA_JY * SIGMA_JY;
    assert_variance(&real(&twice), 2.0 * expected, "corrupt twice");
    let cross_im = imaginary(&twice)
        .into_iter()
        .filter(|v| *v != 0.0)
        .collect::<Vec<_>>();
    assert_variance(&cross_im, 2.0 * expected, "corrupt twice, imaginary");
    // A second term with the first's seed, set after one corrupt: its first
    // draws repeat the first term's, so 2·A₀ + A₁, as CASA's streams give.
    let staggered = samples(&write(&path("staggered.ms"), "XX YY", |session| {
        session.set_noise(&simple_noise(SIGMA_JY))?;
        session.corrupt()?;
        session.set_noise(&simple_noise(SIGMA_JY))?;
        session.corrupt()
    }));
    assert_variance(&real(&staggered), 5.0 * expected, "staggered terms");
    let other_seed = samples(&write(&path("other-seed.ms"), "XX YY", |session| {
        session.set_noise(&simple_noise(SIGMA_JY))?;
        session.set_seed(DEFAULT_SEED + 1);
        session.set_noise(&simple_noise(SIGMA_JY))?;
        session.corrupt()
    }));
    assert_variance(&real(&other_seed), 2.0 * expected, "two seeds");
}

#[test]
fn reset_removes_the_terms_but_not_what_corrupt_applied() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = |name: &str| directory.path().join(name);
    let one = samples(&write(&path("one.ms"), "XX YY", once));
    let reset = samples(&write(&path("reset.ms"), "XX YY", |session| {
        once(session)?;
        session.reset();
        session.corrupt()
    }));
    assert_eq!(reset, one, "corrupt after reset adds nothing");
    let none = samples(&write(&path("none.ms"), "XX YY", Simulator::corrupt));
    assert!(none.iter().all(|sample| sample.norm_sqr() == 0.0));
}

#[test]
fn a_step_covers_only_the_scans_observed_before_it() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("two-scans.ms");
    let main = write(&path, "XX YY", |session| {
        once(session)?;
        session.observe(&scan(1000.0, 1100.0))
    });
    let first_scan_rows = 100 * ANTENNAS * (ANTENNAS + 1) / 2;
    assert_eq!(
        main.data.len(),
        first_scan_rows + 10 * ANTENNAS * (ANTENNAS + 1) / 2
    );
    assert!(
        main.data[..first_scan_rows]
            .iter()
            .all(|cell| cell.iter().all(|sample| sample.re != 0.0))
    );
    assert!(
        main.data[first_scan_rows..]
            .iter()
            .all(|cell| cell.iter().all(|sample| sample.norm_sqr() == 0.0)),
        "the scan observed after corrupt is not corrupted"
    );
}

/// Batch sizes are covered with whole-MeasurementSet digests in the
/// pipeline's own tests (`write::tests`).
#[test]
fn draws_are_the_same_for_the_same_seed_and_any_worker_count() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = |name: &str| directory.path().join(name);
    let seeded = |session: &mut Simulator| {
        session.set_seed(7);
        once(session)?;
        session.set_noise(&simple_noise(0.3))?;
        session.corrupt()
    };
    let first = samples(&write(&path("first.ms"), "XX YY", seeded));
    let again = samples(&write(&path("again.ms"), "XX YY", seeded));
    let workers = samples(&write_with(
        &path("workers.ms"),
        "XX YY",
        resources_with(5),
        seeded,
    ));
    let bits = |values: &[Complex32]| {
        values
            .iter()
            .map(|v| (v.re.to_bits(), v.im.to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        bits(&again),
        bits(&first),
        "the same seed draws the same noise"
    );
    assert_eq!(
        bits(&workers),
        bits(&first),
        "four workers draw the same noise as one"
    );
    let other = samples(&write(&path("other.ms"), "XX YY", |session| {
        session.set_seed(8);
        once(session)
    }));
    assert_ne!(other[..8], first[..8], "another seed draws other noise");
    let default = samples(&write(&path("default.ms"), "XX YY", once));
    let explicit = samples(&write(&path("explicit.ms"), "XX YY", |session| {
        session.set_seed(DEFAULT_SEED);
        once(session)
    }));
    assert_eq!(
        bits(&default),
        bits(&explicit),
        "the default seed is CASA's 11111"
    );
}

fn outcome(result: SimulatorResult<()>) -> (CasaOutcome, String) {
    let error = result.expect_err("refused");
    (error.outcome(), error.message().to_string())
}

#[test]
fn setnoise_refuses_as_casa_does_and_names_later_phases() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut session = configured(&directory.path().join("x.ms"), "XX YY", system_resources());
    // Before observe: no rows to size the term on, so no term.
    let (before, message) = outcome(session.set_noise(&simple_noise(SIGMA_JY)));
    assert_eq!(before, CasaOutcome::ReturnsFalse);
    assert!(
        message.contains("Error in Simulator::create_corrupt"),
        "{message}"
    );
    session.observe(&scan(0.0, 100.0)).expect("observe");
    let mode = |mode: &str| NoiseSpec {
        mode: mode.to_string(),
        ..simple_noise(SIGMA_JY)
    };
    assert_eq!(
        outcome(session.set_noise(&mode("bogus"))),
        (
            CasaOutcome::ReturnsFalse,
            "Caught exception: unsupported mode bogus in setnoise()".to_string()
        )
    );
    for (name, phase) in [("tsys-manual", "Phase 3"), ("tsys-atm", "Phase 4")] {
        let (outcome, message) = outcome(session.set_noise(&mode(name)));
        assert_eq!(outcome, CasaOutcome::NotYetImplemented);
        assert!(
            message.contains(name) && message.contains(phase),
            "{message}"
        );
    }
    let table = NoiseSpec {
        on_the_fly: false,
        ..simple_noise(SIGMA_JY)
    };
    assert_eq!(
        outcome(session.set_noise(&table)).0,
        CasaOutcome::NotYetImplemented
    );
    // None of these added a term.
    session.corrupt().expect("corrupt");
    session.close().expect("close");
    let main = read_main(&directory.path().join("x.ms"));
    assert!(samples(&main).iter().all(|sample| sample.norm_sqr() == 0.0));
}

#[test]
fn corrupt_raises_for_three_correlations_once_there_is_a_term() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut session = configured(
        &directory.path().join("x.ms"),
        "XX XY YY",
        system_resources(),
    );
    session.observe(&scan(0.0, 100.0)).expect("observe");
    session.corrupt().expect("no terms: nothing to corrupt");
    session
        .set_noise(&simple_noise(SIGMA_JY))
        .expect("setnoise");
    let (raised, message) = outcome(session.corrupt());
    assert_eq!(raised, CasaOutcome::Raises);
    assert!(message.starts_with("No such VisVector type"), "{message}");
}
