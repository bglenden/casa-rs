// SPDX-License-Identifier: LGPL-3.0-or-later

//! Diagnostics (IF-9, plan section 8): the request's switches add structured
//! `tracing` events through `casa-logging`, and nothing else does. With
//! `stage_timing` each phase logs one event carrying the phase record of the
//! run summary; with `science_trace` the controller logs what it reads and
//! decides; with `metal_stage_timing` each Metal backend logs its timing.
//! With every switch off, a run logging at INFO emits none of them.

use std::path::Path;
use std::process::Command;

use serde_json::Value;

use super::fixture::{Component, Observation};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};

const STAGE_TIMING: &str = "casa_imaging::stage_timing";
const SCIENCE_TRACE: &str = "casa_imaging::science_trace";
const METAL_STAGE_TIMING: &str = "casa_imaging::metal_stage_timing";

/// Run a Högbom clean of `observation` named `name` with the extra `flags`,
/// logging INFO to stderr: its stderr, or why it failed.
fn clean(observation: &Observation, name: &str, flags: &[&str]) -> Result<String, String> {
    clean_logging(observation, name, "INFO", flags)
}

/// [`clean`], logging `priority` and above to stderr.
fn clean_logging(
    observation: &Observation,
    name: &str,
    priority: &str,
    flags: &[&str],
) -> Result<String, String> {
    let directory = observation.scratch("diagnostics");
    std::fs::create_dir_all(&directory).expect("run directory");
    let geometry = observation.geometry();
    // The CPU unless `flags` name a backend: the catalog's `auto` would move
    // a law onto the GPU wherever the host has one.
    let backend: &[&str] = if flags.contains(&"--backend") {
        &[]
    } else {
        &["--backend", "cpu"]
    };
    let output = Command::new(env!("CARGO_BIN_EXE_casars-imager"))
        .args([
            "--ms",
            observation.measurement_set().to_str().expect("UTF-8 path"),
            "--imagename",
            name,
            "--imsize",
            &geometry.image_size[0].to_string(),
            "--cell-arcsec",
            &geometry.cell_arcsec[0].to_string(),
            "--deconvolver",
            "hogbom",
            "--niter",
            "200",
            "--threshold-jy",
            "0.005",
            "--log-stderr-priority",
            priority,
        ])
        .args(backend)
        .args(flags)
        .current_dir(&directory)
        .output()
        .expect("start casars-imager");
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if output.status.success() {
        Ok(stderr)
    } else {
        Err(stderr)
    }
}

/// The summary `casars-imager` wrote for `name` in `observation`'s run
/// directory.
fn summary(observation: &Observation, name: &str) -> Value {
    let path = observation
        .scratch("diagnostics")
        .join(format!("{name}.summary.json"));
    serde_json::from_slice(&std::fs::read(Path::new(&path)).expect("summary"))
        .expect("summary JSON")
}

/// The stderr lines logged under `target`.
fn events<'a>(stderr: &'a str, target: &str) -> Vec<&'a str> {
    stderr
        .lines()
        .filter(|line| line.contains(target))
        .collect()
}

/// The major-cycle passes the run of `summary` made: one per `major cycle`
/// phase.
fn passes(summary: &Value) -> usize {
    summary["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .filter(|phase| {
            phase["name"]
                .as_str()
                .is_some_and(|name| name.starts_with("major cycle "))
        })
        .count()
}

/// The `source stream` events each wave of each pass logged, pass by pass
/// and wave by wave in the order `stderr` gives them. A wave ends with its
/// `wave stages` line and a pass with its `imaging major cycle:` line,
/// which the thread that streamed its source logs after it; an event
/// after the last pass is a wave of its own.
fn source_streams_by_pass_and_wave(stderr: &str) -> Vec<Vec<usize>> {
    let mut passes = Vec::new();
    let mut waves = Vec::new();
    let mut streams = 0;
    for line in stderr.lines() {
        if line.contains(METAL_STAGE_TIMING) && line.contains(" source stream") {
            streams += 1;
        } else if line.contains(" wave stages") {
            waves.push(std::mem::take(&mut streams));
        } else if line.contains(" imaging major cycle: ") {
            passes.push(std::mem::take(&mut waves));
        }
    }
    if streams > 0 {
        waves.push(streams);
    }
    if !waves.is_empty() {
        passes.push(waves);
    }
    passes
}

/// Assert that each wave of every pass the run of `summary` made logged one
/// `source stream` event in `stderr`, and that each pass made from one wave
/// to one a plane of its `planes`: the summary counts the passes, and the
/// log each pass's waves (#830).
fn assert_one_source_stream_per_pass_and_wave(stderr: &str, summary: &Value, planes: usize) {
    let streams = source_streams_by_pass_and_wave(stderr);
    let passes = passes(summary);
    assert!(passes > 0, "the run made a pass: {summary}");
    assert_eq!(
        streams.len(),
        passes,
        "one `imaging major cycle:` line a pass of the summary: {streams:?}\n{stderr}"
    );
    for (pass, waves) in streams.iter().enumerate() {
        assert!(
            (1..=planes).contains(&waves.len()),
            "pass {pass} made one wave to one a plane: {streams:?}\n{stderr}"
        );
        assert!(
            waves.iter().all(|streams| *streams == 1),
            "each wave of pass {pass} logs one source stream: {streams:?}\n{stderr}"
        );
    }
    eprintln!(
        "{passes} passes of {:?} waves, one source stream each",
        streams.iter().map(Vec::len).collect::<Vec<_>>()
    );
}

/// The value of `name=` in a structured log line; a quoted value without
/// its quotes.
fn field<'a>(line: &'a str, name: &str) -> &'a str {
    let start = line
        .find(&format!(" {name}="))
        .unwrap_or_else(|| panic!("{name} in {line}"))
        + name.len()
        + 2;
    let rest = &line[start..];
    match rest.strip_prefix('"') {
        Some(quoted) => &quoted[..quoted.find('"').expect("closing quote")],
        None => rest.split(' ').next().expect("a value"),
    }
}

#[test]
fn stage_timing_logs_each_phase_record_and_switches_off_log_nothing() {
    let observation = Observation::synthesise(&[("point", POINT)]);

    let stderr = clean(
        &observation,
        "timed",
        &["--stage-timing", "--science-trace"],
    )
    .unwrap_or_else(|stderr| panic!("timed run failed: {stderr}"));
    let summary = summary(&observation, "timed");
    let phases = summary["phases"].as_array().expect("phases");
    let lines = events(&stderr, STAGE_TIMING);
    assert_eq!(lines.len(), phases.len(), "one event per phase:\n{stderr}");
    for (line, phase) in lines.iter().zip(phases) {
        assert_eq!(field(line, "phase"), phase["name"], "{line}");
        assert_eq!(field(line, "completed"), "true", "{line}");
        assert_eq!(
            field(line, "seconds").parse::<f64>().ok(),
            phase["seconds"].as_f64(),
            "{line}"
        );
        for bytes in [
            "process_peak_rss_bytes",
            "peak_reserved_bytes",
            "reserved_bytes",
        ] {
            assert_eq!(
                field(line, bytes).parse::<u64>().ok(),
                phase[bytes].as_u64(),
                "{bytes} in {line}"
            );
        }
    }
    let science = events(&stderr, SCIENCE_TRACE);
    let minor_cycles = summary["minor_cycles"].as_u64().expect("minor cycles");
    assert!(minor_cycles > 0, "the clean ran a minor cycle: {summary}");
    // An event's message precedes its first field.
    let count = |start: &str| {
        science
            .iter()
            .filter(|line| line.contains(&format!(" {start}")))
            .count() as u64
    };
    assert_eq!(count("minor cycle cycle="), minor_cycles, "{stderr}");
    assert_eq!(
        count("minor cycle controls cycle="),
        minor_cycles,
        "{stderr}"
    );
    // Each minor cycle follows a decision, and one more decision stops.
    assert!(count("residual major_cycle=") > minor_cycles, "{stderr}");
    assert!(events(&stderr, METAL_STAGE_TIMING).is_empty(), "a CPU run");

    let quiet = clean(&observation, "quiet", &[])
        .unwrap_or_else(|stderr| panic!("quiet run failed: {stderr}"));
    assert!(
        quiet.contains("imaging minor cycle 1"),
        "the control logs at INFO:\n{quiet}"
    );
    for target in [STAGE_TIMING, SCIENCE_TRACE, METAL_STAGE_TIMING] {
        assert!(events(&quiet, target).is_empty(), "{target}:\n{quiet}");
    }
}

/// Each Metal backend logs its timing: the gridding owners of a continuum
/// clean, and in a linearly interpolated cube's later major cycle the row
/// chunks' prediction backends, so the native-channel residual's model is
/// degridded on the device. Each pass logs one source stream a wave, for
/// every pass the summary records (#830).
#[test]
fn metal_stage_timing_logs_each_metal_backend() {
    let metal = ["--backend", "gpu", "--metal-stage-timing"];
    let observation = Observation::synthesise(&[("point", POINT)]);
    let stderr = match clean(&observation, "metal-timed", &metal) {
        Ok(stderr) => stderr,
        Err(stderr) if !cfg!(target_os = "macos") || stderr.contains("Metal 3 device") => {
            eprintln!("skipped: no Metal device");
            return;
        }
        Err(stderr) => panic!("Metal run failed: {stderr}"),
    };
    let check = |stderr: &str| {
        // Each Metal backend logs its usage as it drops, and each pass its
        // source stream's waits.
        let (streams, lines): (Vec<&str>, Vec<&str>) = events(stderr, METAL_STAGE_TIMING)
            .into_iter()
            .partition(|line| line.contains("source stream"));
        assert!(!lines.is_empty(), "{stderr}");
        for line in &streams {
            assert!(field(line, "blocks").parse::<u64>().expect("blocks") > 0);
            field(line, "consumer_wait_seconds")
                .parse::<f64>()
                .expect("a wait in seconds");
        }
        for line in &lines {
            assert_eq!(field(line, "completed"), "true", "{line}");
            assert!(field(line, "applies").parse::<u64>().expect("applies") > 0);
            assert!(field(line, "samples").parse::<u64>().expect("samples") > 0);
            field(line, "wait_seconds")
                .parse::<f64>()
                .expect("a wait in seconds");
        }
        for target in [STAGE_TIMING, SCIENCE_TRACE] {
            assert!(events(stderr, target).is_empty(), "{target}:\n{stderr}");
        }
        lines
            .iter()
            .map(|line| field(line, "predicted").parse::<u64>().expect("predicted"))
            .sum::<u64>()
    };
    check(&stderr);
    // A continuum clean has one plane, so one wave a pass.
    assert_one_source_stream_per_pass_and_wave(&stderr, &summary(&observation, "metal-timed"), 1);

    let band = Observation::synthesise_band(4, &[("point", POINT)]);
    let cube = [
        "--specmode",
        "cube",
        "--channel-count",
        "4",
        "--nmajor",
        "2",
    ];
    let flags = [&metal[..], &cube[..]].concat();
    let stderr = clean(&band, "metal-cube-timed", &flags)
        .unwrap_or_else(|stderr| panic!("Metal cube failed: {stderr}"));
    assert!(
        check(&stderr) > 0,
        "the cube's later major cycle predicts on the device:\n{stderr}"
    );
    assert_one_source_stream_per_pass_and_wave(&stderr, &summary(&band, "metal-cube-timed"), 4);
}

/// `backend = auto` grids standard imaging on the GPU where the host has
/// one and on the CPU where it has not, and the run summary names the
/// device. W projection, which the GPU does not grid, and an explicit
/// `gridprecision = f64`, which it cannot, stay on the CPU.
#[test]
fn auto_grids_on_the_gpu_where_it_can_and_the_summary_says_which() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let device = match clean(&observation, "gpu-probe", &["--backend", "gpu"]) {
        Ok(_) => "metal",
        Err(stderr) if !cfg!(target_os = "macos") || stderr.contains("Metal 3 device") => "cpu",
        Err(stderr) => panic!("GPU probe failed: {stderr}"),
    };
    for (name, flags, expected) in [
        ("auto", &["--backend", "auto"][..], device),
        (
            "auto-wproject",
            &[
                "--backend",
                "auto",
                "--gridder",
                "wproject",
                "--wprojplanes",
                "4",
            ][..],
            "cpu",
        ),
        (
            "auto-f64",
            &["--backend", "auto", "--gridprecision", "f64"][..],
            "cpu",
        ),
    ] {
        clean(&observation, name, flags).unwrap_or_else(|stderr| panic!("{name}: {stderr}"));
        assert_eq!(summary(&observation, name)["backend"], expected, "{name}");
    }
}

/// The imaging log does not grow with the planes a cube cleans (plan
/// section 10 rule 6): with every diagnostic off and DEBUG logging, a
/// four-channel cube logs as many imaging events as a one-channel cube
/// whose run has the same phases.
#[test]
fn imaging_logs_do_not_grow_with_the_planes_of_a_cube() {
    let observation = Observation::synthesise_band(4, &[("point", POINT)]);
    let run = |channels: usize| {
        let name = format!("planes-{channels}");
        let count = channels.to_string();
        let flags = [
            "--specmode",
            "cube",
            "--channel-count",
            &count,
            "--nmajor",
            "1",
        ];
        let stderr = clean_logging(&observation, &name, "DEBUG", &flags)
            .unwrap_or_else(|stderr| panic!("{channels}-channel cube failed: {stderr}"));
        let phases = summary(&observation, &name)["phases"]
            .as_array()
            .expect("phases")
            .iter()
            .map(|phase| phase["name"].clone())
            .collect::<Vec<_>>();
        let imaging = stderr
            .lines()
            .filter(|line| {
                line.split_whitespace()
                    .nth(1)
                    .is_some_and(|target| target.starts_with("casa_imaging"))
            })
            .count();
        (phases, imaging, stderr)
    };
    let (one_phases, one, one_log) = run(1);
    let (four_phases, four, four_log) = run(4);
    assert_eq!(one_phases, four_phases, "the runs have the same phases");
    assert!(one > 0, "{one_log}");
    assert_eq!(
        one, four,
        "one channel:\n{one_log}\nfour channels:\n{four_log}"
    );
}
