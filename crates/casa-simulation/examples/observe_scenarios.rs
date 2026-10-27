// SPDX-License-Identifier: LGPL-3.0-or-later
//! Run the CASA oracle's observe-only `sm` scenarios through the casa-rs
//! session (simulation Phase 1c, #841).
//!
//! `tools/simulation/casa_sm_observe_oracle.py` writes CASA's MSs and a
//! manifest of every call with its arguments resolved to numbers. This
//! example replays each scenario's calls and writes `<out>/<scenario>.ms`
//! for `tools/simulation/compare_sm_ms.py`.
//!
//! casa-rs stores TIME as UTC (plan decision 5) where CASA stores TAI under
//! a UTC label. So that the rows compare at equal stored TIME,
//! `--casa-time-shift-s` (default 37, TAI − UTC since 2017) is added to every
//! observe's start and stop. The reference time is left unchanged, so the
//! hour-angle offset and SOURCE.TIME stay the same.
//!
//! Scenarios may also call `setseed`, `setnoise` (`simplenoise` in Jy, or
//! `tsys-manual` or `tsys-atm` with their parameters; `pwv` in mm, `pground`
//! in mbar, `altitude` and `waterheight` in m),
//! `corrupt`, `reset`, `setoptions`, `setvp` and `predict` (Phase 1d;
//! `tools/simulation/sm_predict_scenarios.json`), and, in two stages,
//! `close` and then `openfromms` (Phase 2;
//! `tools/simulation/sm_openfromms_scenarios.json`): the first stage is
//! closed, and its MS, or a copy of CASA's when the scenario's
//! `openfromms_ms` is `casa`, reopened; and `setdata`, `setgain` and
//! `setleakage` (Phase 3; `tools/simulation/sm_corrupt_scenarios.json`)
//! and `settrop` (Phase 4d; `tools/simulation/sm_trop_scenarios.json`),
//! whose `table` is written under the output directory, as the oracle
//! writes CASA's under its own. Each of those must end as
//! CASA's did: `True`, `False`, or raised, as the manifest records; a call
//! that does not stops the run. `--simplenoise-jy` adds a `setnoise` and a
//! `corrupt` after every scenario's own calls, with the seed of `--seed`
//! when given, so a noise-free scenario can be timed with noise.
//!
//! `--workers`, `--memory-budget` (bytes) and `--backend` are the Python
//! tool's constructor controls; the session's streaming workers are printed,
//! and each `close`'s pass summary (workers, bytes, stage times) goes to
//! standard error.
//!
//! ```text
//! cargo run --release -p casa-simulation --example observe_scenarios -- \
//!     <casa-oracle-dir>/manifest.json <out-dir> [--casa-time-shift-s 37] \
//!     [--simplenoise-jy 0.1 [--seed 11111]] [--workers 4] \
//!     [--memory-budget 1073741824] [--backend auto]
//! ```

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use casa_simulation::{
    ApplySpec, ArrayConfig, AtmosphereSpec, BackendRequest, CasaOutcome, CoordinateSystem,
    ExecutionControls, GainSpec, LeakageSpec, NoiseSpec, ObserveManySpec, ObserveSpec, Pointing,
    PredictSpec, PredictionOptions, SessionResources, Simulator, SimulatorOptions, SimulatorResult,
    SpectralWindowSpec, StateSpec, TroposphereSpec, TsysParameters, VoltagePatternSpec,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch, MPosition};
use serde_json::Value;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn number(call: &Value, key: &str) -> f64 {
    call[key]
        .as_f64()
        .unwrap_or_else(|| panic!("{key} is a number in {call}"))
}

fn text<'a>(call: &'a Value, key: &str, default: &'a str) -> &'a str {
    call.get(key).and_then(Value::as_str).unwrap_or(default)
}

fn numbers(call: &Value, key: &str) -> Vec<f64> {
    call[key]
        .as_array()
        .unwrap_or_else(|| panic!("{key} is a list in {call}"))
        .iter()
        .map(|value| value.as_f64().expect("a number"))
        .collect()
}

fn strings(call: &Value, key: &str) -> Vec<String> {
    call[key]
        .as_array()
        .unwrap_or_else(|| panic!("{key} is a list in {call}"))
        .iter()
        .map(|value| value.as_str().expect("a string").to_string())
        .collect()
}

fn now_utc() -> MEpoch {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs_f64();
    MEpoch::from_mjd(40_587.0 + seconds / 86_400.0, EpochRef::UTC)
}

fn set_config(session: &mut Simulator, call: &Value) {
    let reference = numbers(call, "referencelocation");
    let coordinate_system = match text(call, "coordsystem", "global") {
        "global" => CoordinateSystem::Global,
        "local" => CoordinateSystem::Local {
            reference: MPosition::new_itrf(reference[0], reference[1], reference[2]),
        },
        other => panic!("coordsystem {other}"),
    };
    let config = ArrayConfig {
        telescope: text(call, "telescopename", "VLA").to_string(),
        x: numbers(call, "x"),
        y: numbers(call, "y"),
        z: numbers(call, "z"),
        dish_diameter: numbers(call, "dishdiameter"),
        offset: vec![0.0],
        mount: strings(call, "mount"),
        antenna_names: strings(call, "antname"),
        pad_names: strings(call, "padname"),
        coordinate_system,
    };
    session.set_config(&config).expect("setconfig");
}

fn observe(session: &mut Simulator, call: &Value, shift_s: f64) {
    let spec = ObserveSpec {
        source: text(call, "sourcename", "").to_string(),
        spectral_window: text(call, "spwname", "").to_string(),
        start_s: number(call, "starttime") + shift_s,
        stop_s: number(call, "stoptime") + shift_s,
        add_observation: call
            .get("add_observation")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        state: StateSpec {
            sig: true,
            reference: false,
            cal: 0.0,
            load: 0.0,
            sub_scan: 0,
            obs_mode: text(call, "state_obs_mode", "OBSERVE_TARGET.ON_SOURCE").to_string(),
        },
        observer: text(call, "observer", "CASA simulator").to_string(),
        project: text(call, "project", "CASA simulation").to_string(),
    };
    session.observe(&spec).expect("observe");
}

/// Replay `observemany` from its resolved lists. One source name, or one
/// direction, is padded to every pointing with empty ones, as casatools pads
/// them (`casars.tools.simulator` does the same).
fn observe_many(session: &mut Simulator, call: &Value, shift_s: f64) -> SimulatorResult<()> {
    let starts = numbers(call, "starttimes");
    let stops = numbers(call, "stoptimes");
    let sources: Vec<String> = match &call["sourcenames"] {
        Value::String(name) => vec![name.clone()],
        Value::Array(values) => values
            .iter()
            .map(|value| value.as_str().expect("a name").to_string())
            .collect(),
        other => panic!("sourcenames is {other}"),
    };
    let directions = call["directions"].as_array().cloned().unwrap_or_default();
    let direction = |index: usize| {
        let value = directions.get(index)?;
        let angles = value.as_array()?;
        Some(MDirection::from_angles(
            angles[0].as_f64().expect("ra"),
            angles[1].as_f64().expect("dec"),
            DirectionRef::J2000,
        ))
    };
    let pointings = starts
        .iter()
        .zip(&stops)
        .enumerate()
        .map(|(index, (start, stop))| Pointing {
            source: sources.get(index).cloned().unwrap_or_default(),
            start_s: start + shift_s,
            stop_s: stop + shift_s,
            direction: direction(index),
        })
        .collect();
    let base = ObserveSpec {
        source: String::new(),
        spectral_window: text(call, "spwname", "").to_string(),
        start_s: 0.0,
        stop_s: 0.0,
        add_observation: call
            .get("add_observation")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        state: StateSpec {
            sig: true,
            reference: false,
            cal: 0.0,
            load: 0.0,
            sub_scan: 0,
            obs_mode: text(call, "state_obs_mode", "OBSERVE_TARGET#ON_SOURCE").to_string(),
        },
        observer: text(call, "observer", "CASA simulator").to_string(),
        project: text(call, "project", "CASA simulation").to_string(),
    };
    let spec = ObserveManySpec {
        pointings,
        ..ObserveManySpec::from(&base)
    };
    session.observe_many(&spec)
}

/// Noise added after a scenario's own calls (`--simplenoise-jy`, `--seed`).
#[derive(Clone, Copy)]
struct ExtraNoise {
    sigma_jy: f64,
    seed: Option<i32>,
}

fn simple_noise(sigma_jy: f64) -> NoiseSpec {
    NoiseSpec::simple(sigma_jy)
}

/// `setnoise` with the call's arguments, CASA's defaults for the others:
/// `simplenoise` in Jy, `pwv` in mm, `pground` in mbar, `altitude` and
/// `waterheight` in m, the other parameters as numbers.
fn set_noise(session: &mut Simulator, call: &Value) -> SimulatorResult<()> {
    let or = |key: &str, default: f64| call.get(key).and_then(Value::as_f64).unwrap_or(default);
    let defaults = TsysParameters::default();
    session.set_noise(&NoiseSpec {
        mode: text(call, "mode", "simplenoise").to_string(),
        simple_noise_jy: or("simplenoise", 0.0),
        table: text(call, "table", "").to_string(),
        water_vapour_mm: or("pwv", 0.0),
        atmosphere: AtmosphereSpec {
            ground_pressure_hpa: or("pground", 0.0),
            relative_humidity_percent: or("relhum", 20.0),
            altitude_m: or("altitude", 0.0),
            water_scale_height_m: or("waterheight", 0.0),
        },
        tsys: TsysParameters {
            tau: or("tau", defaults.tau),
            atmosphere_k: or("tatmos", defaults.atmosphere_k),
            antenna_efficiency: or("antefficiency", defaults.antenna_efficiency),
            spillover_efficiency: or("spillefficiency", defaults.spillover_efficiency),
            correlator_efficiency: or("correfficiency", defaults.correlator_efficiency),
            receiver_k: or("trx", defaults.receiver_k),
            ground_k: or("tground", defaults.ground_k),
            cmb_k: or("tcmb", defaults.cmb_k),
            sensitivity: or("senscoeff", defaults.sensitivity),
            receiver_type: call
                .get("rxtype")
                .and_then(Value::as_i64)
                .unwrap_or(defaults.receiver_type),
        },
        on_the_fly: call.get("OTF").and_then(Value::as_bool).unwrap_or(true),
    })
}

/// The numbers of `key`, or `default` when the call has none.
fn numbers_or(call: &Value, key: &str, default: &[f64]) -> Vec<f64> {
    if call.get(key).is_some() {
        numbers(call, key)
    } else {
        default.to_vec()
    }
}

/// `setdata`, with CASA's defaults (`spwid=[0]`, `fieldid=[0]`).
fn set_data(session: &mut Simulator, call: &Value) -> SimulatorResult<()> {
    let ids = |key: &str| -> Vec<i64> {
        numbers_or(call, key, &[0.0])
            .into_iter()
            .map(|id| id as i64)
            .collect()
    };
    session.set_data(&ids("spwid"), &ids("fieldid"), text(call, "msselect", ""))
}

/// `setapply` of the table the manifest names (CASA's, which both sides
/// apply), with CASA's defaults.
fn set_apply(session: &mut Simulator, call: &Value) -> SimulatorResult<()> {
    session.set_apply(&ApplySpec {
        table: text(call, "table", "").to_string(),
        kind: text(call, "type", "").to_string(),
        interval_s: call.get("t").and_then(Value::as_f64).unwrap_or(0.0),
        interp: text(call, "interp", "linear").to_string(),
        calwt: flag(call, "calwt", false),
        spwmap: numbers_or(call, "spwmap", &[-1.0])
            .into_iter()
            .map(|window| window as i64)
            .collect(),
        opacity: call.get("opacity").and_then(Value::as_f64).unwrap_or(0.0),
    })
}

/// A `setgain` or `setleakage` table, named in the manifest, under `out`.
fn table_under(call: &Value, out: &Path) -> String {
    match text(call, "table", "") {
        "" => String::new(),
        name => out.join(name).to_string_lossy().into_owned(),
    }
}

fn set_gain(session: &mut Simulator, call: &Value, out: &Path) -> SimulatorResult<()> {
    session.set_gain(&GainSpec {
        mode: text(call, "mode", "fbm").to_string(),
        table: table_under(call, out),
        amplitude: numbers_or(call, "amplitude", &[0.01]),
    })
}

fn set_trop(session: &mut Simulator, call: &Value, out: &Path) -> SimulatorResult<()> {
    let defaults = TroposphereSpec::default();
    let value = |key: &str, default: f64| call.get(key).and_then(Value::as_f64).unwrap_or(default);
    session.set_trop(&TroposphereSpec {
        mode: text(call, "mode", &defaults.mode).to_string(),
        table: table_under(call, out),
        water_vapour_mm: value("pwv", defaults.water_vapour_mm),
        delta_pwv: value("deltapwv", defaults.delta_pwv),
        beta: value("beta", defaults.beta),
        windspeed_m_s: value("windspeed", defaults.windspeed_m_s),
        simint_s: value("simint", defaults.simint_s),
    })
}

fn set_leakage(session: &mut Simulator, call: &Value, out: &Path) -> SimulatorResult<()> {
    session.set_leakage(&LeakageSpec {
        table: table_under(call, out),
        amplitude: numbers_or(call, "amplitude", &[0.01]),
        offset: numbers_or(call, "offset", &[0.0]),
    })
}

fn flag(call: &Value, key: &str, default: bool) -> bool {
    call.get(key).and_then(Value::as_bool).unwrap_or(default)
}

fn predict(session: &mut Simulator, call: &Value) -> SimulatorResult<()> {
    let images = match call.get("imagename") {
        Some(Value::String(name)) => vec![PathBuf::from(name)],
        Some(Value::Array(names)) => names
            .iter()
            .map(|name| PathBuf::from(name.as_str().expect("an image name")))
            .collect(),
        _ => Vec::new(),
    };
    session.predict(&PredictSpec {
        images,
        component_lists: vec![PathBuf::from(text(call, "complist", ""))],
        incremental: flag(call, "incremental", false),
    })
}

fn set_options(session: &mut Simulator, call: &Value) -> SimulatorResult<()> {
    let defaults = PredictionOptions::default();
    session.set_options(&PredictionOptions {
        ftmachine: text(call, "ftmachine", &defaults.ftmachine).to_string(),
        grid_function: text(call, "gridfunction", &defaults.grid_function).to_string(),
        padding: call
            .get("padding")
            .and_then(Value::as_f64)
            .unwrap_or(defaults.padding),
        w_planes: call
            .get("wprojplanes")
            .and_then(Value::as_i64)
            .unwrap_or(defaults.w_planes),
    })
}

fn set_vp(session: &mut Simulator, call: &Value) -> SimulatorResult<()> {
    let defaults = VoltagePatternSpec::default();
    session.set_vp(&VoltagePatternSpec {
        dovp: flag(call, "dovp", defaults.dovp),
        use_default_vp: flag(call, "usedefaultvp", defaults.use_default_vp),
        vp_table: text(call, "vptable", "").to_string(),
        do_squint: flag(call, "dosquint", defaults.do_squint),
        ..defaults
    })
}

/// Panic unless `result` is what CASA's call returned (`True` or `False`)
/// or raised, as the manifest's `entry` records it.
fn expect_casas_outcome(entry: &Value, result: SimulatorResult<()>) {
    let op = entry["op"].as_str().unwrap_or("?");
    let casa = match (&entry["returned"], entry.get("raised")) {
        (Value::Bool(returned), _) => format!("returned {returned}"),
        (_, Some(raised)) => format!("raised {raised}"),
        (other, None) => format!("returned {other}"),
    };
    let ours = match &result {
        Ok(()) => "returned true".to_string(),
        Err(error) if error.outcome() == CasaOutcome::ReturnsFalse => "returned false".to_string(),
        Err(error) => format!("raised {error}"),
    };
    let agree = casa == ours || (casa.starts_with("raised") && ours.starts_with("raised"));
    assert!(agree, "{op}: CASA {casa}, casa-rs {ours}");
}

/// Replace the directory `to` with a copy of `from`.
fn replace_with_copy(from: &Path, to: &Path) {
    if to.exists() {
        std::fs::remove_dir_all(to).expect("remove the first stage");
    }
    std::fs::create_dir_all(to).expect("the copy's directory");
    for entry in std::fs::read_dir(from).expect("read CASA's first stage") {
        let entry = entry.expect("an entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("its type").is_dir() {
            replace_with_copy(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy a file");
        }
    }
}

fn run_scenario(
    scenario: &Value,
    out: &Path,
    shift_s: f64,
    extra_noise: Option<ExtraNoise>,
    resources: SessionResources,
) {
    let name = scenario["name"].as_str().expect("scenario name");
    let options = SimulatorOptions {
        measures: casa_ms::open_measures_runtime().expect("measures runtime"),
        resources,
        now: now_utc(),
    };
    let path = out.join(format!("{name}.ms"));
    let mut session = Some(Simulator::open(&path, options.clone()));
    let calls = scenario["calls"].as_array().expect("calls");
    for (index, entry) in calls.iter().enumerate() {
        let call = &entry["resolved"];
        let op = entry["op"].as_str().expect("op");
        match op {
            // The last call is the final close, made below.
            "open" => continue,
            "close" if index + 1 == calls.len() => continue,
            "close" => {
                let first = session.take().expect("an open session");
                first.close().expect("close the first stage");
                continue;
            }
            "openfromms" => {
                if scenario["openfromms_ms"] == "casa" {
                    let stage_one = calls[index - 1]["resolved"]["stage1"]
                        .as_str()
                        .expect("CASA's first stage");
                    replace_with_copy(Path::new(stage_one), &path);
                }
                match Simulator::open_from_ms(&path, options.clone()) {
                    Ok(opened) => {
                        expect_casas_outcome(entry, Ok(()));
                        session = Some(opened);
                    }
                    Err(error) => expect_casas_outcome(entry, Err(error)),
                }
                continue;
            }
            _ => {}
        }
        let session = session.as_mut().expect("an open session");
        match op {
            "setconfig" => set_config(session, call),
            "setspwindow" => session
                .set_spectral_window(&SpectralWindowSpec {
                    name: text(call, "spwname", "XBAND").to_string(),
                    start_frequency_hz: number(call, "freq"),
                    channel_width_hz: number(call, "deltafreq"),
                    resolution_hz: number(call, "freqresolution"),
                    frame: FrequencyRef::from_str(text(call, "refcode", "TOPO")).expect("refcode"),
                    channel_count: number(call, "nchannels") as usize,
                    stokes: text(call, "stokes", "RR LL").to_string(),
                })
                .expect("setspwindow"),
            "setfeed" => session
                .set_feed(text(call, "mode", ""), &[0.0], &[0.0], &["R".to_string()])
                .expect("setfeed"),
            "setauto" => session.set_auto(number(call, "autocorrwt") as f32),
            "setlimits" => {
                session.set_limits(number(call, "shadowlimit"), number(call, "elevationlimit"))
            }
            "settimes" => session
                .set_times(
                    number(call, "integrationtime"),
                    call["usehourangle"].as_bool().expect("usehourangle"),
                    MEpoch::from_mjd(number(call, "referencetime"), EpochRef::UTC),
                )
                .expect("settimes"),
            "setfield" => {
                let direction = numbers(call, "sourcedirection");
                session
                    .set_field(
                        text(call, "sourcename", "SOURCE"),
                        &MDirection::from_angles(direction[0], direction[1], DirectionRef::J2000),
                        text(call, "calcode", ""),
                        0.0,
                    )
                    .expect("setfield");
            }
            "observe" => observe(session, call, shift_s),
            "observemany" => expect_casas_outcome(entry, observe_many(session, call, shift_s)),
            "setseed" => session.set_seed(number(call, "seed") as i32),
            "setnoise" => expect_casas_outcome(entry, set_noise(session, call)),
            "setdata" => expect_casas_outcome(entry, set_data(session, call)),
            "setapply" => expect_casas_outcome(entry, set_apply(session, call)),
            "setgain" => expect_casas_outcome(entry, set_gain(session, call, out)),
            "setleakage" => expect_casas_outcome(entry, set_leakage(session, call, out)),
            "settrop" => expect_casas_outcome(entry, set_trop(session, call, out)),
            "corrupt" => expect_casas_outcome(entry, session.corrupt()),
            "reset" => session.reset(),
            "setoptions" => expect_casas_outcome(entry, set_options(session, call)),
            "setvp" => expect_casas_outcome(entry, set_vp(session, call)),
            "predict" => expect_casas_outcome(entry, predict(session, call)),
            other => panic!("unsupported call {other}"),
        }
    }
    let mut session = session.expect("an open session");
    if let Some(extra) = extra_noise {
        if let Some(seed) = extra.seed {
            session.set_seed(seed);
        }
        session
            .set_noise(&simple_noise(extra.sigma_jy))
            .expect("setnoise");
        session.corrupt().expect("corrupt");
    }
    let started = std::time::Instant::now();
    session.close().expect("close");
    println!(
        "{name}: written in {:.2} s",
        started.elapsed().as_secs_f64()
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let manifest = PathBuf::from(args.next().expect("manifest.json"));
    let out = PathBuf::from(args.next().expect("output directory"));
    let mut shift_s = 37.0;
    let mut sigma_jy = None;
    let mut seed = None;
    let mut controls = ExecutionControls::default();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--casa-time-shift-s" => {
                shift_s = args.next().expect("seconds").parse().expect("a number");
            }
            "--simplenoise-jy" => {
                sigma_jy = Some(args.next().expect("Jy").parse().expect("a number"));
            }
            "--seed" => seed = Some(args.next().expect("seed").parse().expect("an integer")),
            "--workers" => {
                let workers: usize = args.next().expect("workers").parse().expect("a count");
                controls.workers = Some(NonZeroUsize::new(workers).expect("positive"));
            }
            "--memory-budget" => {
                controls.memory_budget =
                    Some(args.next().expect("bytes").parse().expect("a byte count"));
            }
            "--backend" => {
                controls.backend = BackendRequest::from_str(&args.next().expect("backend"))
                    .expect("auto, cpu or metal");
            }
            other => panic!("unknown argument {other}"),
        }
    }
    // The pass summary `close` reports (workers, bytes and stage times), and
    // each Metal predictor's dispatches and device time.
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(std::io::stderr),
        )
        .with(
            Targets::new()
                .with_target("casa_simulation::write", tracing::Level::INFO)
                .with_target("casa_imaging::metal_stage_timing", tracing::Level::INFO)
                .with_target("casa_tables::streamed_tiled_write", tracing::Level::DEBUG),
        )
        .init();
    let resources = SessionResources::from_controls(&controls).expect("session resources");
    println!(
        "workers: {} streaming beside the writer, {} when close predicts on Metal ({:?})",
        resources.close_cores(false).streaming_workers,
        resources.close_cores(true).streaming_workers,
        resources.policy()
    );
    let extra_noise = sigma_jy.map(|sigma_jy| ExtraNoise { sigma_jy, seed });
    let manifest: Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).expect("read manifest"))
            .expect("manifest JSON");
    std::fs::create_dir_all(&out).expect("output directory");
    for scenario in manifest["scenarios"].as_array().expect("scenarios") {
        run_scenario(scenario, &out, shift_s, extra_noise, resources);
    }
}
