// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.settrop` then `sm.corrupt` (#841 Phase 4d): the tropospheric phase
//! as a session applies it to the rows observed so far, and the `Tf Jones`
//! table it writes. The screens themselves are the crate's
//! `corrupt::troposphere` module's and its unit tests'; these laws hold
//! what `settrop`, `corrupt` and `close` make of them:
//! - on a 1 Jy point at the phase centre every cross row of every channel
//!   is `g_p(c)·conj(g_q(c))` from the table, and an autocorrelation is
//!   left as it was;
//! - each table row's phase is one water-vapour excess δ times the whole
//!   wet phase delay of its channel's own frequency (ATM through
//!   `casa-atmosphere`, at `settrop`'s site and `pwv`), across the 183 GHz
//!   water line, in a later window and in a one-channel window;
//! - each antenna's δ over the slots has the root-mean-square `deltapwv`
//!   asked for (at most 1);
//! - DATA is bitwise the same for any worker count, for a budget that
//!   splits integrations across batches, and after `openfromms`.

mod pass_summary;
mod predict_support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_atmosphere::{AbsorptionProfile, AtmosphereProfile, ProfileParameters, SpectralGrid};
use casa_ms::MeasurementSet;
use casa_ms::columns::time_columns::TimeColumn;
use casa_simulation::{
    CasaOutcome, PredictSpec, Simulator, SimulatorOptions, SimulatorResult, SpectralWindowSpec,
    TroposphereSpec,
};
use casa_tables::{Table, TableOptions};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::{ArrayValue, ScalarValue, Value};
use num_complex::Complex32;
use pass_summary::capture;
use predict_support::{
    ANTENNAS, CENTRE_RAD, REFERENCE_MJD, Row, bits, data, read_main, resources, scan,
    session_with_channels, write_point_list,
};

/// Channels of the 230 GHz window and of the water line's.
const CHANNELS: usize = 16;

/// The window after the first, across the 183.31 GHz water line. (casa-rs
/// does not yet write windows of different shapes in one MeasurementSet,
/// so a one-channel window is a session's own.)
const WATER_WINDOW: (&str, f64, f64) = ("water", 182.5e9, 125.0e6);

/// The scans: a minute in each window, in turn.
const SCANS: [(&str, f64, f64); 2] = [("band6", 0.0, 60.0), ("water", 60.0, 120.0)];

/// Integrations of 2 s: five to each 10 s slot.
const INTEGRATION_S: f64 = 2.0;

/// `settrop` of `mode` writing `table`, 1 mm of water vapour fluctuating by
/// 1 % (phases well inside ±π at 230 GHz), CASA's other defaults.
fn trop(mode: &str, table: &Path) -> TroposphereSpec {
    TroposphereSpec {
        mode: mode.to_string(),
        table: table.to_string_lossy().into_owned(),
        water_vapour_mm: 1.0,
        delta_pwv: 0.01,
        ..TroposphereSpec::default()
    }
}

fn point(directory: &Path) -> PathBuf {
    let path = directory.join("point.cl");
    write_point_list(&path, CENTRE_RAD, 1.0);
    path
}

/// A session of [`ANTENNAS`] antennas observing [`SCANS`] in 2 s
/// integrations, with a 1 Jy point at the phase centre predicted.
fn observed(path: &Path, options: SimulatorOptions, point: &Path) -> Simulator {
    observed_in(path, (options, CHANNELS, INTEGRATION_S), point)
}

/// [`observed`] with windows of `channels` channels and integrations of
/// `integration_s`: with one channel, a minute of the 230 GHz window alone.
fn observed_in(
    path: &Path,
    (options, channels, integration_s): (SimulatorOptions, usize, f64),
    point: &Path,
) -> Simulator {
    let mut sm = session_with_channels(path, "ALMA", "XX YY", channels, options.resources);
    let (name, start_hz, width_hz) = WATER_WINDOW;
    sm.set_spectral_window(&SpectralWindowSpec {
        name: name.to_string(),
        start_frequency_hz: start_hz,
        channel_width_hz: width_hz,
        resolution_hz: width_hz,
        frame: FrequencyRef::TOPO,
        channel_count: channels,
        stokes: "XX YY".to_string(),
    })
    .expect("setspwindow");
    sm.set_times(
        integration_s,
        false,
        MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
    )
    .expect("settimes");
    let scans = if channels == 1 {
        &SCANS[..1]
    } else {
        &SCANS[..]
    };
    for &(window, start_s, stop_s) in scans {
        sm.observe(&scan_in(window, start_s, stop_s))
            .expect("observe");
    }
    sm.predict(&PredictSpec {
        images: Vec::new(),
        component_lists: vec![point.to_path_buf()],
        incremental: false,
    })
    .expect("predict");
    sm
}

fn scan_in(window: &str, start_s: f64, stop_s: f64) -> casa_simulation::ObserveSpec {
    casa_simulation::ObserveSpec {
        spectral_window: window.to_string(),
        ..scan("target", start_s, stop_s)
    }
}

fn options(workers: usize, memory: u64) -> SimulatorOptions {
    SimulatorOptions {
        measures: casa_test_support::deterministic_measures_provider(),
        resources: resources(workers, memory),
        now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
    }
}

/// One row of a `Tf Jones` table.
#[derive(Debug, Clone)]
struct Solution {
    time_s: f64,
    window: usize,
    antenna: usize,
    /// CPARAM, one value per channel.
    values: Vec<Complex32>,
}

/// A `Tf Jones` table's MAIN rows, checking its layout as CASA 6.7.6
/// writes it, and its spectral windows' channel frequencies.
fn read_table(path: &Path) -> (Vec<Solution>, Vec<Vec<f64>>) {
    let table = Table::open(TableOptions::new(path)).expect("open the calibration table");
    let solutions = table
        .rows()
        .expect("the rows")
        .iter()
        .map(|row| {
            let int = |name: &str| match row.get(name) {
                Some(&Value::Scalar(ScalarValue::Int32(value))) => value,
                other => panic!("{name}: {other:?}"),
            };
            let time_s = match row.get("TIME") {
                Some(&Value::Scalar(ScalarValue::Float64(value))) => value,
                other => panic!("TIME: {other:?}"),
            };
            let values = match row.get("CPARAM") {
                Some(Value::Array(ArrayValue::Complex32(values))) => {
                    assert_eq!(values.shape()[0], 1, "one parameter");
                    values.iter().copied().collect()
                }
                other => panic!("CPARAM: {other:?}"),
            };
            assert_eq!(
                (int("ANTENNA2"), int("SCAN_NUMBER"), int("OBSERVATION_ID")),
                (-1, -1, 0)
            );
            Solution {
                time_s,
                window: int("SPECTRAL_WINDOW_ID") as usize,
                antenna: int("ANTENNA1") as usize,
                values,
            }
        })
        .collect();
    let windows = Table::open(TableOptions::new(path.join("SPECTRAL_WINDOW")))
        .expect("SPECTRAL_WINDOW")
        .rows()
        .expect("its rows")
        .iter()
        .map(|row| match row.get("CHAN_FREQ") {
            Some(Value::Array(ArrayValue::Float64(values))) => values.iter().copied().collect(),
            other => panic!("CHAN_FREQ: {other:?}"),
        })
        .collect();
    (solutions, windows)
}

/// MAIN's TIME of each row.
fn read_times(path: &Path) -> Vec<f64> {
    let ms = MeasurementSet::open(path).expect("open");
    let time = TimeColumn::new(ms.main_table());
    (0..ms.row_count())
        .map(|row| time.get_mjd_seconds(row).expect("TIME"))
        .collect()
}

/// The whole zenith wet phase delay, radians, of 1 mm of water vapour at
/// each of `frequencies_hz` at `settrop`'s site (CASA's simulator
/// defaults).
fn wet_delays(frequencies_hz: &[f64]) -> Vec<f64> {
    let atmosphere = AtmosphereProfile::new(&ProfileParameters::default()).expect("profile");
    let mut grid = SpectralGrid::new();
    grid.add_channels(frequencies_hz, 0).expect("grid");
    let profile = AbsorptionProfile::new_totals(atmosphere, grid);
    (0..frequencies_hz.len())
        .map(|channel| {
            (profile.dispersive_h2o_phase_delay_deg(channel, 1e-3)
                + profile.non_dispersive_h2o_phase_delay_deg(channel, 1e-3))
            .to_radians()
        })
        .collect()
}

/// The even slot of `width_s` nearest `time_s` among `count` from
/// `start_s`, the earlier of two as near (`CalCorruptor::setCurrTime`).
fn slot(time_s: f64, start_s: f64, width_s: f64, count: usize) -> usize {
    (0..count)
        .min_by(|&a, &b| {
            let distance = |slot: usize| (start_s + (slot as f64 + 0.5) * width_s - time_s).abs();
            distance(a).total_cmp(&distance(b))
        })
        .expect("a slot")
}

/// A session of `mode` simulated and closed: its table and MAIN.
fn simulate(directory: &Path, mode: &str) -> (Vec<Solution>, Vec<Vec<f64>>, Vec<Row>, Vec<f64>) {
    let path = directory.join(format!("{mode}.ms"));
    let table = directory.join(format!("{mode}.cal"));
    let mut sm = observed(&path, options(2, 2 << 30), &point(directory));
    sm.set_trop(&trop(mode, &table)).expect("settrop");
    sm.corrupt().expect("corrupt");
    sm.close().expect("close");
    let (solutions, windows) = read_table(&table);
    (solutions, windows, read_main(&path).0, read_times(&path))
}

/// Both modes: DATA is the table's `g_p(c)·conj(g_q(c))`, each table row's
/// phase is one excess times each channel's whole wet delay at its own
/// frequency, and each antenna's excess has the root-mean-square asked for.
#[test]
fn each_channel_turns_by_the_whole_wet_delay_of_its_frequency() {
    for mode in ["individual", "screen"] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let (solutions, windows, rows, times) = simulate(directory.path(), mode);
        // A row per integration, window and antenna, as CASA's table.
        let integrations: usize = SCANS
            .iter()
            .map(|(_, start, stop)| ((stop - start) / INTEGRATION_S) as usize)
            .sum();
        assert_eq!(solutions.len(), integrations * ANTENNAS, "{mode}");
        assert_eq!(
            windows.iter().map(Vec::len).collect::<Vec<_>>(),
            [CHANNELS, CHANNELS],
            "{mode}: the table keeps the windows' channels"
        );
        let delays: Vec<Vec<f64>> = windows.iter().map(|window| wet_delays(window)).collect();
        // The water line's delay changes across the window by far more than
        // the check resolves, so a delay from the wrong frequency fails it.
        let water = &delays[1];
        let (low, high) = water
            .iter()
            .fold((f64::INFINITY, 0.0_f64), |(low, high), &d| {
                (low.min(d), high.max(d))
            });
        assert!(high / low - 1.0 > 1e-2, "{water:?}");
        let first = solutions
            .iter()
            .map(|s| s.time_s)
            .fold(f64::INFINITY, f64::min);
        let last = solutions.iter().map(|s| s.time_s).fold(0.0, f64::max);
        let count = ((last - first) / 10.0 + 1.0) as usize;
        let mut excess: BTreeMap<(usize, usize), f64> = BTreeMap::new();
        for solution in &solutions {
            let delay = &delays[solution.window];
            let deltas: Vec<f64> = solution
                .values
                .iter()
                .zip(delay)
                .map(|(value, delay)| {
                    assert!((value.norm() - 1.0).abs() < 1e-6, "phase only");
                    f64::from(value.arg()) / delay
                })
                .collect();
            for delta in &deltas {
                assert!(
                    (delta - deltas[0]).abs() <= 1e-5 * deltas[0].abs() + 1e-7,
                    "{mode}: one excess across the channels: {deltas:?}"
                );
            }
            let slot = slot(solution.time_s, first, 10.0, count);
            let previous = excess.insert((slot, solution.antenna), deltas[0]);
            if let Some(previous) = previous {
                assert!((previous - deltas[0]).abs() < 1e-6, "{mode}: one per slot");
            }
        }
        // Every slot is read, so each antenna's series has the
        // root-mean-square asked for.
        assert_eq!(excess.len(), count * ANTENNAS, "{mode}");
        if mode == "individual" {
            for antenna in 0..ANTENNAS {
                let series: Vec<f64> = (0..count).map(|slot| excess[&(slot, antenna)]).collect();
                let mean = series.iter().sum::<f64>() / count as f64;
                let rms =
                    (series.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / count as f64).sqrt();
                assert!((rms - 0.01).abs() < 1e-6, "antenna {antenna}: rms {rms}");
            }
        }
        // DATA: on the point, g_p(c)·conj(g_q(c)) in every correlation; an
        // autocorrelation stays 1.
        for (row, &time_s) in rows.iter().zip(&times) {
            let window = solutions
                .iter()
                .find(|s| s.time_s == time_s)
                .expect("the row's solutions")
                .window;
            let gains = |antenna: u32| {
                &solutions
                    .iter()
                    .find(|s| s.time_s == time_s && s.antenna == antenna as usize)
                    .expect("the antenna's solution")
                    .values
            };
            let [p, q] = row.antennas;
            for (channel, sample) in row.data.as_chunks::<2>().0.iter().enumerate() {
                let expected = gains(p)[channel] * gains(q)[channel].conj();
                for value in sample {
                    assert!(
                        (value - expected).norm() < 2e-6,
                        "{mode}: window {window} channel {channel}: {value} against {expected}"
                    );
                }
            }
        }
    }
}

/// A one-channel window turns by its channel's delay, and `deltapwv` above
/// 1 is held to 1, as CASA holds it.
#[test]
fn a_one_channel_window_turns_and_deltapwv_is_at_most_one() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let table = directory.path().join("wild.cal");
    let path = directory.path().join("wild.ms");
    let mut sm = observed_in(
        &path,
        (options(2, 2 << 30), 1, INTEGRATION_S),
        &point(directory.path()),
    );
    sm.set_trop(&TroposphereSpec {
        delta_pwv: 3.0,
        water_vapour_mm: 0.01,
        ..trop("individual", &table)
    })
    .expect("settrop");
    sm.close().expect("close");
    let (solutions, windows) = read_table(&table);
    assert!(
        solutions
            .iter()
            .all(|s| s.values.len() == 1 && s.window == 0)
    );
    // 0.01 mm of water vapour (in single precision, as the corruptor holds
    // it): a hundredth of 1 mm's delay.
    let delay = wet_delays(&windows[0])[0] * f64::from(0.01_f32);
    // 30 integrations, 58 s from the first to the last: six slots, each
    // read last 8 s after the first integration's time and every 10 s on.
    let first = solutions[0].time_s;
    let count = 6;
    for antenna in 0..ANTENNAS {
        // One value per 10 s slot: the slot's last integration.
        let series: Vec<f64> = solutions
            .iter()
            .filter(|s| s.antenna == antenna && (s.time_s - first) % 10.0 == 8.0)
            .map(|s| f64::from(s.values[0].arg()) / delay)
            .collect();
        assert_eq!(series.len(), count);
        let mean = series.iter().sum::<f64>() / count as f64;
        let rms = (series.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / count as f64).sqrt();
        assert!(
            (rms - 1.0).abs() < 1e-4,
            "antenna {antenna}: rms {rms}, not 3"
        );
    }
}

/// An exponent far from CASA's 1.1, either way, gives finite phases that
/// vary (the spectrum is taken relative to its largest amplitude, so no
/// power overflows; #899 review); and a screen varies on a track as short
/// as a series may not (four slots of 30 s), its pixels apart.
#[test]
fn extreme_exponents_and_a_short_screen_give_finite_varying_phases() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let cases: [(&str, f64, f64); 5] = [
        ("individual", 1000.0, -1.0),
        ("individual", -1000.0, -1.0),
        ("screen", 1000.0, -1.0),
        ("screen", -1000.0, -1.0),
        ("screen", 1.1, 30.0),
    ];
    for (index, (mode, beta, simint_s)) in cases.into_iter().enumerate() {
        let table = directory.path().join(format!("{index}.cal"));
        let mut sm = observed(
            &directory.path().join(format!("{index}.ms")),
            options(2, 2 << 30),
            &point,
        );
        sm.set_trop(&TroposphereSpec {
            beta,
            simint_s,
            ..trop(mode, &table)
        })
        .expect("settrop");
        sm.close().expect("close");
        let (solutions, _) = read_table(&table);
        let phases: Vec<f32> = solutions
            .iter()
            .flat_map(|solution| solution.values.iter().map(|value| value.arg()))
            .collect();
        assert!(
            phases.iter().all(|phase| phase.is_finite()),
            "{mode} beta {beta}: finite phases"
        );
        assert!(
            phases.iter().any(|phase| phase.abs() > 1e-6),
            "{mode} beta {beta} simint {simint_s}: the phases vary"
        );
    }
}

/// DATA is bitwise the same for one worker or four, for a budget whose
/// batches split integrations, and after `openfromms`.
#[test]
fn settrop_data_are_the_same_for_any_workers_budget_or_opening() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    // Integrations of a quarter second: 17,280 rows, more than a small
    // budget's batch.
    let observe = |path: &Path, options: SimulatorOptions| {
        observed_in(path, (options, CHANNELS, 0.25), &point)
    };
    let run = |name: &str, workers: usize, memory: u64| {
        let path = directory.path().join(name);
        let mut sm = observe(&path, options(workers, memory));
        sm.set_trop(&trop("screen", Path::new("")))
            .expect("settrop");
        sm.corrupt().expect("corrupt");
        let ((), pass) = capture(|| sm.close().expect("close"));
        (data(&read_main(&path).0), pass.expect("the pass summary"))
    };
    let (one, whole) = run("one.ms", 1, 2 << 30);
    let (four, _) = run("four.ms", 4, 2 << 30);
    let (small, split) = run("small.ms", 3, 96 << 20);
    println!(
        "{} rows in {} batches of {}, and in {} of {}",
        whole.rows, whole.batches, whole.batch_rows, split.batches, split.batch_rows
    );
    let integration_rows = (ANTENNAS * (ANTENNAS + 1) / 2) as u64;
    assert!(split.batches > whole.batches && split.batch_rows % integration_rows != 0);
    assert_eq!(bits(&one), bits(&four));
    assert_eq!(bits(&one), bits(&small));
    // The same steps after `openfromms`: the observed MeasurementSet's model
    // is predicted again and corrupted.
    let path = directory.path().join("opened.ms");
    observe(&path, options(2, 2 << 30)).close().expect("close");
    let mut sm = Simulator::open_from_ms(&path, options(2, 2 << 30)).expect("openfromms");
    sm.predict(&PredictSpec {
        images: Vec::new(),
        component_lists: vec![point.clone()],
        incremental: false,
    })
    .expect("predict");
    sm.set_trop(&trop("screen", Path::new("")))
        .expect("settrop");
    sm.corrupt().expect("corrupt");
    sm.close().expect("close the opened MeasurementSet");
    assert_eq!(bits(&one), bits(&data(&read_main(&path).0)));
}

/// A change to a `settrop` spec, named by the argument it changes.
type Change = (&'static str, fn(&mut TroposphereSpec));

fn outcome(result: SimulatorResult<()>) -> (CasaOutcome, String) {
    let error = result.expect_err("refused");
    (error.outcome(), error.to_string())
}

/// What CASA refuses, or casa-rs does not do, adds no term: an unknown
/// mode, `mode='test'`, a `pwv` that is not positive, integrations spanning
/// less than one slot, an argument that is not a finite number, a negative
/// wind for a screen, and a table in the MeasurementSet.
#[test]
fn settrop_refusals_add_no_term() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("refused.ms");
    let mut sm = observed(&path, options(2, 2 << 30), &point(directory.path()));
    let none = Path::new("");
    let (refused, message) = outcome(sm.set_trop(&trop("1d", none)));
    assert_eq!(refused, CasaOutcome::ReturnsFalse);
    assert!(
        message.contains("unsupported mode 1d in settrop()"),
        "{message}"
    );
    let (refused, _) = outcome(sm.set_trop(&trop("test", none)));
    assert_eq!(refused, CasaOutcome::NotYetImplemented);
    let (refused, message) = outcome(sm.set_trop(&TroposphereSpec {
        water_vapour_mm: 0.0,
        ..trop("individual", none)
    }));
    assert_eq!(refused, CasaOutcome::ReturnsFalse);
    assert!(message.contains("undefined PWV"), "{message}");
    let (refused, message) = outcome(sm.set_trop(&TroposphereSpec {
        simint_s: 600.0,
        ..trop("individual", none)
    }));
    assert_eq!(refused, CasaOutcome::ReturnsFalse);
    assert!(message.contains("setEvenSlots"), "{message}");
    // Not a number, or infinite in the single precision CASA holds it in, in
    // any argument; and a wind blowing the screen west (which CASA's checks
    // let through).
    let bad: [Change; 9] = [
        ("pwv", |spec| spec.water_vapour_mm = f64::NAN),
        ("pwv", |spec| spec.water_vapour_mm = 1e100),
        ("deltapwv", |spec| spec.delta_pwv = f64::NAN),
        ("beta", |spec| spec.beta = f64::INFINITY),
        ("beta", |spec| spec.beta = 1e300),
        ("windspeed", |spec| spec.windspeed_m_s = f64::NAN),
        ("windspeed", |spec| spec.windspeed_m_s = 1e100),
        ("simint", |spec| spec.simint_s = f64::NEG_INFINITY),
        ("windspeed", |spec| spec.windspeed_m_s = -7.0),
    ];
    for (name, change) in bad {
        let mut spec = trop("screen", none);
        change(&mut spec);
        let (refused, message) = outcome(sm.set_trop(&spec));
        assert_eq!(refused, CasaOutcome::ReturnsFalse, "{name}");
        assert!(message.contains(&format!("settrop's {name}")), "{message}");
    }
    // A wind whose run no screen can hold, and a series too short to vary
    // (four slots of 30 s over the two minutes): refused, where CASA makes
    // or indexes past its screen, or reads its series' last slots from
    // past their transform.
    let (refused, message) = outcome(sm.set_trop(&TroposphereSpec {
        windspeed_m_s: 1e9,
        ..trop("screen", none)
    }));
    assert_eq!(refused, CasaOutcome::ReturnsFalse);
    assert!(message.contains("cannot be made"), "{message}");
    let (refused, message) = outcome(sm.set_trop(&TroposphereSpec {
        simint_s: 30.0,
        ..trop("individual", none)
    }));
    assert_eq!(refused, CasaOutcome::ReturnsFalse);
    assert!(message.contains("at least 6 slots"), "{message}");
    let inside = path.join("trop.cal");
    let (refused, _) = outcome(sm.set_trop(&trop("individual", &inside)));
    assert_eq!(refused, CasaOutcome::Raises);
    sm.corrupt().expect("corrupt");
    sm.close().expect("close");
    // The point, untouched.
    for row in read_main(&path).0 {
        for value in row.data {
            assert!((value - Complex32::new(1.0, 0.0)).norm() < 1e-6, "{value}");
        }
    }
}
