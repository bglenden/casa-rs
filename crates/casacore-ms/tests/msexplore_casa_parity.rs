// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::process::Command;

use tempfile::tempdir;

mod common;

use common::casa_plotms::{discover_casa_python, ngc5921_ms_path, skip_reason};

#[test]
fn amplitude_vs_time_txt_manifest_tracks_casa_plotms_line_count() {
    if !plotms_environment_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "amplitude_vs_time",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "amp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_eq!(count_rust_points(&rust), count_casa_points(&casa));
}

#[test]
fn amplitude_vs_channel_avgchannel_txt_manifest_tracks_casa_plotms_line_count() {
    if !plotms_environment_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "amplitude_vs_channel",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
        "--avgchannel",
        "8",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "chan"),
        ("yaxis", "amp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
        ("avgchannel", "8"),
    ])
    .expect("run casa plotms");

    assert_eq!(count_rust_points(&rust), count_casa_points(&casa));
}

fn plotms_environment_available() -> bool {
    discover_casa_python().is_some_and(|python| python.plotms_available)
        && ngc5921_ms_path().is_some()
}

fn run_rust_msexplore(extra_args: &[&str]) -> Result<String, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let output = temp.path().join("rust-msexplore.txt");
    let result = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--plot-output"])
        .arg(&output)
        .args(["--plot-format", "txt"])
        .args(extra_args)
        .arg(&ms_path)
        .output()
        .map_err(|error| format!("spawn rust msexplore: {error}"))?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).to_string());
    }
    fs::read_to_string(output).map_err(|error| format!("read rust manifest: {error}"))
}

fn run_casa_plotms(kwargs: &[(&str, &str)]) -> Result<String, String> {
    let casa = discover_casa_python().ok_or_else(|| skip_reason(true))?;
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let output = temp.path().join("casa-plotms.txt");
    let mut script = String::from(
        r#"
import os
try:
    from casatasks import plotms
except Exception:
    from casaplotms import plotms

kwargs = {
    "vis": os.environ["CASA_VIS"],
    "plotfile": os.environ["CASA_OUT"],
    "expformat": "txt",
    "overwrite": True,
    "showgui": False,
}
"#,
    );
    for (key, value) in kwargs {
        script.push_str(&format!("kwargs[{key:?}] = {value:?}\n"));
    }
    script.push_str("plotms(**kwargs)\n");

    let result = Command::new(&casa.program)
        .arg("-c")
        .arg(&script)
        .env("CASA_VIS", &ms_path)
        .env("CASA_OUT", &output)
        // `casaplotms` checks only for the presence of DISPLAY, even when
        // exporting with `showgui=False` on macOS.
        .env(
            "DISPLAY",
            std::env::var("DISPLAY").unwrap_or_else(|_| ":0".to_string()),
        )
        .output()
        .map_err(|error| format!("spawn casa plotms: {error}"))?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).to_string());
    }
    fs::read_to_string(output).map_err(|error| format!("read casa txt export: {error}"))
}

fn count_rust_points(text: &str) -> usize {
    text.lines()
        .filter(|line| {
            !line.starts_with('#') && !line.starts_with("series_key") && !line.trim().is_empty()
        })
        .count()
}

fn count_casa_points(text: &str) -> usize {
    text.lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty()
                && !trimmed.starts_with('#')
                && trimmed
                    .split_whitespace()
                    .filter(|token| token.parse::<f64>().is_ok())
                    .count()
                    >= 2
        })
        .count()
}
