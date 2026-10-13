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
    let output = Command::new(env!("CARGO_BIN_EXE_casars-imager"))
        .args([
            "--ms",
            observation.measurement_set().to_str().expect("UTF-8 path"),
            "--imagename",
            name,
            "--imsize",
            &geometry.image_size.to_string(),
            "--cell-arcsec",
            &geometry.cell_arcsec.to_string(),
            "--deconvolver",
            "hogbom",
            "--niter",
            "200",
            "--threshold-jy",
            "0.005",
            "--log-stderr-priority",
            priority,
        ])
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
/// degridded on the device.
#[test]
fn metal_stage_timing_logs_each_metal_backend() {
    let metal = ["--backend", "metal", "--metal-stage-timing"];
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
        assert!(
            !streams.is_empty(),
            "each pass logs its source stream:\n{stderr}"
        );
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
