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
//! Scenarios may also call `setseed`, `setnoise` (`simplenoise` in Jy),
//! `corrupt` and `reset` (Phase 1d). `--simplenoise-jy` adds a `setnoise`
//! and a `corrupt` after every scenario's own calls, with the seed of
//! `--seed` when given, so a noise-free scenario can be timed with noise.
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
    ArrayConfig, BackendRequest, CoordinateSystem, ExecutionControls, NoiseSpec, ObserveSpec,
    SessionResources, Simulator, SimulatorOptions, SpectralWindowSpec, StateSpec,
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

/// Noise added after a scenario's own calls (`--simplenoise-jy`, `--seed`).
#[derive(Clone, Copy)]
struct ExtraNoise {
    sigma_jy: f64,
    seed: Option<i32>,
}

fn simple_noise(sigma_jy: f64) -> NoiseSpec {
    NoiseSpec {
        mode: "simplenoise".to_string(),
        simple_noise_jy: sigma_jy,
        on_the_fly: true,
    }
}

fn set_noise(session: &mut Simulator, call: &Value) {
    session
        .set_noise(&NoiseSpec {
            mode: text(call, "mode", "simplenoise").to_string(),
            on_the_fly: call.get("OTF").and_then(Value::as_bool).unwrap_or(true),
            ..simple_noise(
                call.get("simplenoise")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0),
            )
        })
        .expect("setnoise");
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
    let mut session = Simulator::open(out.join(format!("{name}.ms")), options);
    for entry in scenario["calls"].as_array().expect("calls") {
        let call = &entry["resolved"];
        match entry["op"].as_str().expect("op") {
            "open" | "close" => {}
            "setconfig" => set_config(&mut session, call),
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
            "observe" => observe(&mut session, call, shift_s),
            "setseed" => session.set_seed(number(call, "seed") as i32),
            "setnoise" => set_noise(&mut session, call),
            "corrupt" => session.corrupt().expect("corrupt"),
            "reset" => session.reset(),
            other => panic!("unsupported call {other}"),
        }
    }
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
    // The pass summary `close` reports: workers, bytes and stage times.
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(std::io::stderr),
        )
        .with(Targets::new().with_target("casa_simulation::write", tracing::Level::INFO))
        .init();
    let resources = SessionResources::from_controls(&controls).expect("session resources");
    println!(
        "workers: {} streaming beside the writer ({:?})",
        resources.cores().streaming_workers,
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
