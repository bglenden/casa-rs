// SPDX-License-Identifier: LGPL-3.0-or-later

//! SIGINT during a run (IF-6): `casars-imager` stops at the next block
//! boundary of a pass or before its next phase, exits with status 130 and
//! leaves nothing it wrote: no product, no staged member, no paged cube
//! state and no run summary.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::json;

use super::fixture::{Component, Observation};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};

/// Entries of `directory` the run wrote: anything named after the image or
/// private to casa-rs (staged members, paged cube state).
fn written(directory: &Path, image: &str) -> Vec<String> {
    std::fs::read_dir(directory)
        .expect("run directory")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name.starts_with(image) || name.contains(".casa-rs-"))
        .collect()
}

/// Wait up to `limit` for `child` to exit.
fn wait(child: &mut Child, limit: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("child status") {
            return Some(status);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

/// A cube clean that would not end on its own, one component per major
/// cycle, interrupted once its paged cube state exists.
#[test]
fn an_interrupted_cube_clean_exits_130_and_leaves_nothing_behind() {
    let observation = Observation::synthesise_band(4, &[("point", POINT)]);
    let image_name = observation.scratch("interrupted");
    let directory = image_name.parent().expect("run directory").to_path_buf();
    let geometry = observation.geometry();
    let request = json!({
        "kind": "run",
        "request": {
            "measurement_set": observation.measurement_set(),
            "image_name": image_name,
            "image_size": geometry.image_size,
            "cell_arcsec": geometry.cell_arcsec,
            "spectral_mode": "cube",
            "channel_count": 4,
            "deconvolver": "hogbom",
            "niter": 1_000_000,
            "minor_cycle_length": 1,
            "gain": 0.01,
            "threshold_jy": 0.0,
        },
    });
    let request_path = observation.scratch("request.json");
    std::fs::write(&request_path, request.to_string()).expect("request file");
    let mut child = Command::new(env!("CARGO_BIN_EXE_casars-imager"))
        .arg("--json-run")
        .arg(&request_path)
        .current_dir(&directory)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start casars-imager");
    // The paged cube state appears once the imaging weights are formed; the
    // passes and minor cycles follow.
    let deadline = Instant::now() + Duration::from_secs(120);
    while !written(&directory, "interrupted")
        .iter()
        .any(|name| name.contains(".casa-rs-managed-cube-"))
    {
        assert!(
            child.try_wait().expect("child status").is_none(),
            "the run ended before it was interrupted: {:?}",
            child.wait_with_output().expect("output")
        );
        assert!(
            Instant::now() < deadline,
            "the run never reached its cycles"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(300));
    // SAFETY: `kill` only sends a signal to the child process.
    let sent = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) };
    assert_eq!(sent, 0, "SIGINT sent");
    let interrupted = Instant::now();
    let status = wait(&mut child, Duration::from_secs(60)).unwrap_or_else(|| {
        child.kill().expect("kill");
        panic!("the interrupted run did not stop");
    });
    let stopped = interrupted.elapsed();
    assert_eq!(status.code(), Some(130), "{status:?}");
    assert!(
        stopped < Duration::from_secs(10),
        "stopped after {stopped:?}"
    );
    assert_eq!(
        written(&directory, "interrupted"),
        Vec::<String>::new(),
        "nothing of the run remains"
    );
}
