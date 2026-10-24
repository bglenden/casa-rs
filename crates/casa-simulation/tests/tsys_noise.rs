// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setnoise(mode='tsys-manual')` (#841 Phase 3): thermal noise whose σ
//! comes from each row's system temperature, read back from the
//! MeasurementSets `close` writes.
//!
//! The expected σ is CASA 6.7.6's (`AtmosCorruptor`, probed for Phase 3 and
//! documented in the crate's `corrupt::tsys`):
//! `σ_ij = c a / sqrt(Δν t) · sqrt(Tsys(A_i) Tsys(A_j)) / (D_i D_j)` per real
//! and imaginary draw, each antenna's airmass `A` at the field at the row's
//! time, and an antenna whose Tsys overflows single precision (below the
//! horizon) scaling by 1. The tests compute it independently, in double
//! precision, from the antennas' elevations.
//!
//! A `tsys-manual` term draws what a `simplenoise` term of the same seed
//! draws with σ = 1 Jy, scaled by its row's σ, as CASA's unit-amplitude
//! `ANoise` scaled by its `T`. So the ratio of the two MeasurementSets' DATA
//! is each row's σ, which the first law checks row by row: every row, the
//! flagged ones and the autocorrelations included, two spectral windows of
//! different frequency and channel width, two integration times, dishes of
//! 12 m and 7 m, and elevations from below the horizon to 26°.

mod predict_support;

use std::collections::HashMap;
use std::path::Path;

use casa_imaging_runtime::{HostResources, ResourcePolicy};
use casa_ms::MeasurementSet;
use casa_ms::columns::data_columns::DataColumn;
use casa_ms::columns::exposure_interval::ExposureColumn;
use casa_ms::columns::flag_columns::FlagRowColumn;
use casa_ms::columns::main_ids::{antenna1, antenna2, data_desc_id};
use casa_ms::columns::time_columns::TimeColumn;
use casa_ms::columns::weight_columns::{SigmaColumn, WeightColumn};
use casa_simulation::{
    ArrayConfig, BackendRequest, CasaOutcome, CoordinateSystem, NoiseSpec, ObserveSpec,
    SessionResources, Simulator, SimulatorOptions, SimulatorResult, SpectralWindowSpec,
    StateSpec, TsysParameters,
};
use casa_types::ArrayValue;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame};
use ndarray::IxDyn;
use num_complex::Complex32;

/// 2024-06-01 00:00 UTC.
const REFERENCE_MJD: f64 = 60_462.0;

/// The dishes: four of 12 m and two of 7 m.
const DISHES: [f64; 6] = [12.0, 12.0, 12.0, 12.0, 7.0, 7.0];

/// Channels of both windows.
const CHANNELS: usize = 16;

/// The windows: name, first channel's frequency and channel width (one
/// decreasing), Hz.
const WINDOWS: [(&str, f64, f64); 2] = [("a", 230.0e9, 15.625e6), ("b", 100.0e9, -62.5e6)];

/// The field, J2000 radians: at declination −30° it transits the VLA's
/// latitude at 26° elevation and sets about 4.5 h from transit.
fn field() -> [f64; 2] {
    [150.0_f64.to_radians(), (-30.0_f64).to_radians()]
}

/// The `tsys-manual` parameters of the tests: τ₀ = 0.2, so Tsys overflows
/// single precision below the horizon, and CASA's other defaults.
fn tsys_parameters() -> TsysParameters {
    TsysParameters {
        tau: 0.2,
        ..TsysParameters::default()
    }
}

fn tsys(seed: i32) -> impl Fn(&mut Simulator) -> SimulatorResult<()> {
    move |sm: &mut Simulator| {
        sm.set_seed(seed);
        sm.set_noise(&NoiseSpec::tsys_manual(tsys_parameters()))?;
        sm.corrupt()
    }
}

fn simple(seed: i32, sigma_jy: f64) -> impl Fn(&mut Simulator) -> SimulatorResult<()> {
    move |sm: &mut Simulator| {
        sm.set_seed(seed);
        sm.set_noise(&NoiseSpec::simple(sigma_jy))?;
        sm.corrupt()
    }
}

/// A pinned eight-thread host, with this host's Metal device when it has
/// one, and a policy of `workers` workers and 2 GiB asking for `backend`.
fn resources(workers: usize, backend: BackendRequest) -> SessionResources {
    SessionResources::pinned(
        HostResources {
            threads: 8,
            performance_cores: 8,
            available_memory: 8 << 30,
            metal: HostResources::detect().expect("host").metal,
        },
        ResourcePolicy::Explicit {
            workers,
            memory: 2 << 30,
        },
        backend,
    )
    .expect("resources")
}

fn options(workers: usize, backend: BackendRequest) -> SimulatorOptions {
    SimulatorOptions {
        measures: casa_test_support::deterministic_measures_provider(),
        resources: resources(workers, backend),
        now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
    }
}

/// The antennas' ITRF positions, metres: about 75 m apart at the VLA.
fn positions() -> Vec<[f64; 3]> {
    let centre = [-1_601_185.0, -5_041_977.0, 3_554_875.0];
    (0..DISHES.len())
        .map(|index| {
            let step = index as f64;
            [
                centre[0] + 60.0 * step,
                centre[1] - 20.0 * step,
                centre[2] + 40.0 * step,
            ]
        })
        .collect()
}

fn scan(window: &str, start_s: f64, stop_s: f64) -> ObserveSpec {
    ObserveSpec {
        source: "target".to_string(),
        spectral_window: window.to_string(),
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
        project: "tsys".to_string(),
    }
}

/// A session at `path` that has observed two scans, by hour angle: window
/// `a` in 60 s integrations from 5 h to 3 h before transit (the field rises
/// during it), then window `b` in 20 s integrations in the hour before
/// transit; autocorrelations written, the elevation limit at the horizon.
fn observed(path: &Path, workers: usize, backend: BackendRequest) -> Simulator {
    let mut sm = Simulator::open(path, options(workers, backend));
    let positions = positions();
    sm.set_config(&ArrayConfig {
        telescope: "ALMA".to_string(),
        x: positions.iter().map(|p| p[0]).collect(),
        y: positions.iter().map(|p| p[1]).collect(),
        z: positions.iter().map(|p| p[2]).collect(),
        dish_diameter: DISHES.to_vec(),
        offset: vec![0.0],
        mount: vec!["alt-az".to_string()],
        antenna_names: vec!["DA".to_string()],
        pad_names: vec!["A".to_string()],
        coordinate_system: CoordinateSystem::Global,
    })
    .expect("setconfig");
    for (name, frequency_hz, width_hz) in WINDOWS {
        sm.set_spectral_window(&SpectralWindowSpec {
            name: name.to_string(),
            start_frequency_hz: frequency_hz,
            channel_width_hz: width_hz,
            resolution_hz: width_hz.abs(),
            frame: FrequencyRef::TOPO,
            channel_count: CHANNELS,
            stokes: "XX YY".to_string(),
        })
        .expect("setspwindow");
    }
    let [ra, dec] = field();
    sm.set_field(
        "target",
        &MDirection::from_angles(ra, dec, DirectionRef::J2000),
        "",
        0.0,
    )
    .expect("setfield");
    sm.set_auto(1.0);
    sm.set_limits(1e-6, 0.0);
    let reference = MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC);
    sm.set_times(60.0, true, reference.clone())
        .expect("settimes");
    sm.observe(&scan("a", -5.0 * 3600.0, -3.0 * 3600.0))
        .expect("observe");
    sm.set_times(20.0, true, reference).expect("settimes");
    sm.observe(&scan("b", -3600.0, 0.0)).expect("observe");
    sm
}

/// Write the observed session at `path` after `steps`.
fn write(
    path: &Path,
    steps: &dyn Fn(&mut Simulator) -> SimulatorResult<()>,
) -> Main {
    write_with(path, 2, BackendRequest::Cpu, steps)
}

fn write_with(
    path: &Path,
    workers: usize,
    backend: BackendRequest,
    steps: &dyn Fn(&mut Simulator) -> SimulatorResult<()>,
) -> Main {
    let mut sm = observed(path, workers, backend);
    steps(&mut sm).expect("steps");
    sm.close().expect("close");
    read_main(path)
}

/// One MAIN row as the tests read it.
struct Row {
    /// DATA, channel by channel, correlation fastest.
    data: Vec<Complex32>,
    antennas: [usize; 2],
    time_s: f64,
    exposure_s: f64,
    window: usize,
    flagged: bool,
    weight: ArrayValue,
    sigma: ArrayValue,
}

impl Row {
    fn autocorrelation(&self) -> bool {
        self.antennas[0] == self.antennas[1]
    }
}

type Main = Vec<Row>;

fn read_main(path: &Path) -> Main {
    let ms = MeasurementSet::open(path).expect("open");
    let main = ms.main_table();
    let data = DataColumn::data(main).expect("DATA");
    let (first, second, description) = (antenna1(main), antenna2(main), data_desc_id(main));
    let (time, exposure) = (TimeColumn::new(main), ExposureColumn::new(main));
    let flag_row = FlagRowColumn::new(main);
    let (weight, sigma) = (WeightColumn::new(main), SigmaColumn::new(main));
    (0..ms.row_count())
        .map(|row| {
            let ArrayValue::Complex32(cell) = data.get(row).expect("DATA cell") else {
                panic!("DATA is not Complex32");
            };
            let [correlations, channels] = cell.shape() else {
                panic!("DATA cells are matrices");
            };
            let (correlations, channels) = (*correlations, *channels);
            Row {
                data: (0..channels)
                    .flat_map(|channel| (0..correlations).map(move |c| (c, channel)))
                    .map(|(c, channel)| cell[IxDyn(&[c, channel])])
                    .collect(),
                antennas: [
                    first.get(row).expect("ANTENNA1") as usize,
                    second.get(row).expect("ANTENNA2") as usize,
                ],
                time_s: time.get_mjd_seconds(row).expect("TIME"),
                exposure_s: exposure.get(row).expect("EXPOSURE"),
                window: description.get(row).expect("DATA_DESC_ID") as usize,
                flagged: flag_row.get(row).expect("FLAG_ROW"),
                weight: weight.get(row).expect("WEIGHT").clone(),
                sigma: sigma.get(row).expect("SIGMA").clone(),
            }
        })
        .collect()
}

/// Each antenna's elevation of the field at stored TIME `time_s`, radians,
/// as `close` reads TIME (UT1) for the rows' geometry.
fn elevations(time_s: f64) -> Vec<f64> {
    let [ra, dec] = field();
    let direction = MDirection::from_angles(ra, dec, DirectionRef::J2000);
    let measures = casa_test_support::deterministic_measures_provider();
    positions()
        .iter()
        .map(|&[x, y, z]| {
            let frame = MeasFrame::new()
                .with_epoch(MEpoch::from_mjd(time_s / 86_400.0, EpochRef::UT1))
                .with_position(MPosition::new_itrf(x, y, z))
                .with_measures(measures.clone());
            direction
                .convert_to(DirectionRef::AZEL, &frame)
                .expect("AZEL")
                .as_angles()
                .1
        })
        .collect()
}

/// CASA's σ of `row`'s draws for `parameters`, in double precision: the
/// formula of the module documentation, an antenna whose Tsys overflows
/// single precision scaling by 1.
fn expected_sigma(parameters: &TsysParameters, row: &Row, elevations: &[f64]) -> f64 {
    let (_, frequency_hz, width_hz) = WINDOWS[row.window];
    let h_nu_k = 4.80145e-11 * frequency_hz;
    let radiance = |t: f64| 1.0 / ((0.04799274551e-9 * frequency_hz / t).exp() - 1.0);
    let spill = parameters.spillover_efficiency;
    let tsys = |elevation: f64| {
        let airmass = if elevation > 0.0 {
            1.0 / elevation.sin()
        } else {
            1000.0
        };
        let tau = parameters.tau * airmass;
        let r = spill * radiance(parameters.cmb_k)
            + tau.exp()
                * (spill * (1.0 - (-tau).exp()) * radiance(parameters.atmosphere_k)
                    + (1.0 - spill) * radiance(parameters.ground_k)
                    + parameters.receiver_k / h_nu_k);
        h_nu_k * r
    };
    let jansky_per_kelvin = std::f64::consts::FRAC_1_SQRT_2 * 8.0 * 1.38062e-16 * 1e23 * 1e-4
        / (parameters.antenna_efficiency * parameters.correlator_efficiency * std::f64::consts::PI);
    let factor = jansky_per_kelvin / (width_hz.abs() * row.exposure_s).sqrt();
    let scale = |antenna: usize| {
        let t = tsys(elevations[antenna]);
        if t > f64::from(f32::MAX) {
            1.0
        } else {
            factor.sqrt() * t.sqrt() / DISHES[antenna]
        }
    };
    scale(row.antennas[0]) * scale(row.antennas[1])
}

/// The σ a row's samples were scaled by from `unit`'s: Σ a·b / Σ b² over
/// the real parts, and the imaginary parts of a cross-correlation.
fn measured_sigma(row: &Row, unit: &Row) -> f64 {
    let (mut cross, mut norm) = (0.0, 0.0);
    for (a, b) in row.data.iter().zip(&unit.data) {
        cross += f64::from(a.re) * f64::from(b.re);
        norm += f64::from(b.re) * f64::from(b.re);
        if !row.autocorrelation() {
            cross += f64::from(a.im) * f64::from(b.im);
            norm += f64::from(b.im) * f64::from(b.im);
        }
    }
    cross / norm
}

/// The bits of every DATA sample, (real, imaginary).
fn bits(main: &Main) -> Vec<(u32, u32)> {
    main.iter()
        .flat_map(|row| row.data.iter())
        .map(|sample| (sample.re.to_bits(), sample.im.to_bits()))
        .collect()
}

/// The variance of `values` about zero is 1 within five standard errors.
fn assert_unit_variance(values: &[f64], what: &str) {
    let n = values.len() as f64;
    let variance = values.iter().map(|v| v * v).sum::<f64>() / n;
    let tolerance = 5.0 * (2.0 / n).sqrt();
    println!("{what}: normalized variance {variance:.4} ± {tolerance:.4} over {n} samples");
    assert!(
        (variance - 1.0).abs() < tolerance,
        "{what}: normalized variance {variance:.5}, expected 1 ± {tolerance:.5} over {n} samples"
    );
}

/// Each row's noise is the unit draws of a `simplenoise` term of the same
/// seed scaled by CASA's σ for the row: every row, flagged or not, cross or
/// auto, in both windows and at both integration times, to 10⁻⁵ of σ.
#[test]
fn each_row_is_scaled_by_casas_tsys_sigma() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let noisy = write(&directory.path().join("tsys.ms"), &tsys(4321));
    let unit = write(&directory.path().join("unit.ms"), &simple(4321, 1.0));
    let parameters = tsys_parameters();
    let mut by_time: HashMap<u64, Vec<f64>> = HashMap::new();
    let (mut flagged, mut below_horizon, mut windows) = (0, 0, [0; 2]);
    let (mut lowest, mut highest) = (f64::INFINITY, f64::NEG_INFINITY);
    for (index, (row, unit)) in noisy.iter().zip(&unit).enumerate() {
        let elevations = by_time
            .entry(row.time_s.to_bits())
            .or_insert_with(|| elevations(row.time_s));
        let expected = expected_sigma(&parameters, row, elevations);
        let measured = measured_sigma(row, unit);
        assert!(
            (measured / expected - 1.0).abs() < 1e-5,
            "row {index} (antennas {:?}, window {}, exposure {} s, elevations {:.2}°, {:.2}°): \
             σ {measured} against {expected}",
            row.antennas,
            row.window,
            row.exposure_s,
            elevations[row.antennas[0]].to_degrees(),
            elevations[row.antennas[1]].to_degrees(),
        );
        flagged += usize::from(row.flagged);
        windows[row.window] += 1;
        let elevation = elevations[row.antennas[0]].min(elevations[row.antennas[1]]);
        below_horizon += usize::from(elevation <= 0.0);
        if elevation > 0.0 {
            lowest = lowest.min(elevation);
        }
        highest = highest.max(elevation);
        if elevation <= 0.0 {
            assert_eq!(expected, 1.0, "below the horizon CASA does not scale");
        }
    }
    println!(
        "{} rows: {flagged} flagged, {below_horizon} below the horizon, windows {windows:?}, \
         elevations {:.1}° to {:.1}°",
        noisy.len(),
        lowest.to_degrees(),
        highest.to_degrees()
    );
    assert!(below_horizon > 0 && flagged >= below_horizon);
    assert!(windows.iter().all(|&rows| rows > 0));
    assert!(lowest.to_degrees() < 5.0 && highest.to_degrees() > 25.0);
}

/// The draws themselves: each real and imaginary part of a cross-correlation,
/// and each real part of an autocorrelation, divided by the row's σ, has
/// unit variance; autocorrelations keep their imaginary parts zero; WEIGHT
/// and SIGMA stay as `observe` wrote them.
#[test]
fn normalized_noise_has_unit_variance_and_weights_stay() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let noisy = write(&directory.path().join("tsys.ms"), &tsys(77));
    let clean = write(&directory.path().join("clean.ms"), &|_| Ok(()));
    let parameters = tsys_parameters();
    let (mut re, mut im, mut auto) = (Vec::new(), Vec::new(), Vec::new());
    let mut by_time: HashMap<u64, Vec<f64>> = HashMap::new();
    for (row, clean) in noisy.iter().zip(&clean) {
        assert_eq!((&row.weight, &row.sigma), (&clean.weight, &clean.sigma));
        let elevations = by_time
            .entry(row.time_s.to_bits())
            .or_insert_with(|| elevations(row.time_s));
        let sigma = expected_sigma(&parameters, row, elevations);
        for sample in &row.data {
            if row.autocorrelation() {
                assert_eq!(sample.im.to_bits(), 0);
                auto.push(f64::from(sample.re) / sigma);
            } else {
                re.push(f64::from(sample.re) / sigma);
                im.push(f64::from(sample.im) / sigma);
            }
        }
    }
    assert_unit_variance(&re, "cross real");
    assert_unit_variance(&im, "cross imaginary");
    assert_unit_variance(&auto, "auto real");
}

/// Each term adds its own noise, as `simplenoise` terms do: two
/// `tsys-manual` terms of one seed add coherently (twice the draws), and a
/// `simplenoise` term adds its draws to a `tsys-manual` term's unscaled.
/// CASA instead multiplies every term's noise by every `tsys-manual` term's
/// σ (`docs/CASA (C++) bugs.md`).
#[test]
fn each_term_adds_its_own_noise() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = |name: &str| directory.path().join(name);
    let one = write(&path("one.ms"), &tsys(5));
    let two = write(&path("two.ms"), &|sm: &mut Simulator| {
        sm.set_seed(5);
        sm.set_noise(&NoiseSpec::tsys_manual(tsys_parameters()))?;
        sm.set_noise(&NoiseSpec::tsys_manual(tsys_parameters()))?;
        sm.corrupt()
    });
    let doubled: Vec<(u32, u32)> = one
        .iter()
        .flat_map(|row| row.data.iter())
        .map(|sample| ((2.0 * sample.re).to_bits(), (2.0 * sample.im).to_bits()))
        .collect();
    assert_eq!(bits(&two), doubled);
    let simple_only = write(&path("simple.ms"), &simple(6, 0.5));
    let both = write(&path("both.ms"), &|sm: &mut Simulator| {
        sm.set_seed(5);
        sm.set_noise(&NoiseSpec::tsys_manual(tsys_parameters()))?;
        sm.set_seed(6);
        sm.set_noise(&NoiseSpec::simple(0.5))?;
        sm.corrupt()
    });
    let summed: Vec<(u32, u32)> = one
        .iter()
        .zip(&simple_only)
        .flat_map(|(a, b)| a.data.iter().zip(&b.data))
        .map(|(a, b)| ((a.re + b.re).to_bits(), (a.im + b.im).to_bits()))
        .collect();
    assert_eq!(bits(&both), summed);
}

/// The draws depend only on the seed, the row, the channel and the
/// correlation: the same for any worker count; `reset` removes the term,
/// so a later `corrupt` adds nothing.
#[test]
fn draws_are_the_same_for_any_worker_count_and_reset_removes_the_term() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = |name: &str| directory.path().join(name);
    let one = write_with(&path("one.ms"), 1, BackendRequest::Cpu, &tsys(9));
    let three = write_with(&path("three.ms"), 3, BackendRequest::Cpu, &tsys(9));
    assert_eq!(bits(&one), bits(&three));
    let other = write(&path("other.ms"), &tsys(10));
    assert_ne!(bits(&one), bits(&other));
    let reset = write(&path("reset.ms"), &|sm: &mut Simulator| {
        sm.set_noise(&NoiseSpec::tsys_manual(tsys_parameters()))?;
        sm.reset();
        sm.corrupt()
    });
    assert!(
        reset
            .iter()
            .all(|row| row.data.iter().all(|sample| sample.norm_sqr() == 0.0))
    );
}

/// A `tsys-manual` `corrupt` after `openfromms` writes the DATA it writes in
/// the session that observed, bit for bit: TIME, EXPOSURE, the windows'
/// REF_FREQUENCY and TOTAL_BANDWIDTH, the dishes, the antennas' positions
/// and the phase centre read back are all its σ takes.
#[test]
fn a_tsys_corrupt_after_openfromms_writes_the_same_data() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let one = directory.path().join("one.ms");
    let session = write(&one, &tsys(11));
    let two = directory.path().join("two.ms");
    write(&two, &|_| Ok(()));
    let mut sm =
        Simulator::open_from_ms(&two, options(2, BackendRequest::Cpu)).expect("openfromms");
    tsys(11)(&mut sm).expect("tsys after openfromms");
    sm.close().expect("close the opened MeasurementSet");
    assert!(
        session
            .iter()
            .any(|row| row.data.iter().any(|sample| sample.norm() > 0.0))
    );
    assert_eq!(bits(&session), bits(&read_main(&two)));
}

fn outcome(result: SimulatorResult<()>) -> (CasaOutcome, String) {
    let error = result.expect_err("refused");
    (error.outcome(), error.message().to_string())
}

/// What CASA refuses, or casa-rs does not yet do, adds no term: `tsys-atm`
/// (Phase 4), a `table` (CASA writes the `T` term's), `OTF=False`, a water
/// vapour above 100 mm and an undefined zenith Tsys, which CASA refuses
/// after adding a unit-σ term (`docs/CASA (C++) bugs.md`).
#[test]
fn refusals_add_no_term() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("refused.ms");
    let mut sm = observed(&path, 2, BackendRequest::Cpu);
    let spec = |change: &dyn Fn(&mut NoiseSpec)| {
        let mut spec = NoiseSpec::tsys_manual(tsys_parameters());
        change(&mut spec);
        spec
    };
    let atm = spec(&|spec| spec.mode = "tsys-atm".to_string());
    let (refused, message) = outcome(sm.set_noise(&atm));
    assert_eq!(refused, CasaOutcome::NotYetImplemented, "{message}");
    let table = spec(&|spec| spec.table = "noisy.cal".to_string());
    let (refused, message) = outcome(sm.set_noise(&table));
    assert_eq!(refused, CasaOutcome::NotYetImplemented);
    assert!(message.contains("noisy.T.cal"), "{message}");
    let otf = spec(&|spec| spec.on_the_fly = false);
    assert_eq!(
        outcome(sm.set_noise(&otf)).0,
        CasaOutcome::NotYetImplemented
    );
    let wet = spec(&|spec| spec.water_vapour_mm = 200.0);
    assert_eq!(
        outcome(sm.set_noise(&wet)),
        (
            CasaOutcome::ReturnsFalse,
            "Caught exception:  you input PWV=200mm. The most common reason for this error is \
             forgetting to set units, which default to meters"
                .to_string()
        )
    );
    let opaque = spec(&|spec| spec.tsys.tau = 50.0);
    let (refused, message) = outcome(sm.set_noise(&opaque));
    assert_eq!(refused, CasaOutcome::ReturnsFalse);
    assert!(
        message.starts_with("Caught exception: error in ATM setup - Tsys")
            && message.ends_with("poorly defined - check inputs"),
        "{message}"
    );
    // A table of one character, and the water vapour CASA allows, are taken.
    let allowed = spec(&|spec| {
        spec.table = "x".to_string();
        spec.water_vapour_mm = 100.0;
    });
    sm.set_noise(&allowed).expect("setnoise");
    sm.reset();
    sm.corrupt().expect("corrupt");
    sm.close().expect("close");
    assert!(
        read_main(&path)
            .iter()
            .all(|row| row.data.iter().all(|sample| sample.norm_sqr() == 0.0))
    );
    // Before any row: CASA cannot size the term.
    let mut empty = Simulator::open(
        directory.path().join("empty.ms"),
        options(1, BackendRequest::Cpu),
    );
    let (refused, message) = outcome(empty.set_noise(&NoiseSpec::tsys_manual(tsys_parameters())));
    assert_eq!(refused, CasaOutcome::ReturnsFalse);
    assert!(message.contains("create_corrupt"), "{message}");
}

/// Whether this host has a Metal device, saying so when it has none.
fn metal_device() -> bool {
    let metal = HostResources::detect().expect("host").metal;
    if !metal {
        eprintln!("skipped: no Metal device");
    }
    metal
}

/// The host draws `tsys-manual` noise whatever the backend: the device's
/// noise terms hold one σ each, so a Metal session predicting the model
/// leaves the noise to the host, which adds the CPU's draws to the device's
/// prediction. Noisy minus noise-free DATA is the same on both backends to
/// the rounding of the sum, and Metal predicted.
#[test]
fn metal_predicts_and_the_host_adds_the_tsys_noise() {
    if !metal_device() {
        return;
    }
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = predict_support::extended_model(directory.path(), "model.image");
    let run = |name: &str, backend: BackendRequest, noise: bool| {
        let path = directory.path().join(name);
        let mut sm = predict_support::session_with_dishes(
            &path,
            "ALMA",
            "XX YY",
            predict_support::CHANNELS,
            &[12.0, 12.0, 12.0, 12.0, 7.0, 7.0, 7.0, 7.0],
            resources(3, backend),
        );
        sm.observe(&predict_support::scan("target", 0.0, 600.0))
            .expect("observe");
        sm.predict(&predict_support::predict_spec(&model, false))
            .expect("predict");
        if noise {
            tsys(13)(&mut sm).expect("tsys-manual");
        }
        sm.close().expect("close");
        predict_support::data(&predict_support::read_main(&path).0)
    };
    let cpu = (
        run("cpu.ms", BackendRequest::Cpu, true),
        run("cpu-clean.ms", BackendRequest::Cpu, false),
    );
    let metal = (
        run("metal.ms", BackendRequest::Metal, true),
        run("metal-clean.ms", BackendRequest::Metal, false),
    );
    let samples = |(noisy, clean): &(Vec<Vec<Complex32>>, Vec<Vec<Complex32>>)| {
        noisy
            .iter()
            .flatten()
            .zip(clean.iter().flatten())
            .map(|(noisy, clean)| (*noisy - *clean, clean.norm() + noisy.norm()))
            .collect::<Vec<_>>()
    };
    let (cpu_noise, metal_noise) = (samples(&cpu), samples(&metal));
    let mut largest: f32 = 0.0;
    for ((cpu, size), (metal, _)) in cpu_noise.iter().zip(&metal_noise) {
        let difference = (*cpu - *metal).norm();
        assert!(
            difference <= 4.0 * f32::EPSILON * size,
            "noise {cpu} on the CPU, {metal} on Metal"
        );
        largest = largest.max(cpu.norm());
    }
    assert!(largest > 0.0, "the session has noise");
    assert_ne!(
        predict_support::bits(&cpu.1),
        predict_support::bits(&metal.1),
        "Metal predicted the model"
    );
}
