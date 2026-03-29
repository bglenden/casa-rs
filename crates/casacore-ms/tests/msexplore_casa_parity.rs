// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cmp::Ordering;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::{Mutex, OnceLock};

use image::{GenericImageView, ImageReader};
use tempfile::tempdir;

mod common;

use common::casa_plotms::{discover_casa_python, ngc5921_ms_path, skip_reason};

fn casa_plotms_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn amplitude_vs_time_txt_manifest_tracks_casa_plotms_line_count() {
    if !plotms_shared_dataset_available() {
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

    let rust_points = count_rust_points(&rust);
    let casa_points = count_casa_points(&casa);
    assert!(
        rust_points > 0,
        "expected rust manifest to contain plotted points"
    );
    assert!(
        casa_points > 0,
        "expected CASA txt export to contain plotted points"
    );
    assert_eq!(rust_points, casa_points);
}

#[test]
fn amplitude_vs_channel_avgchannel_txt_manifest_tracks_casa_plotms_line_count() {
    if !plotms_shared_dataset_available() {
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

    let rust_points = count_rust_points(&rust);
    let casa_points = count_casa_points(&casa);
    assert!(
        rust_points > 0,
        "expected rust manifest to contain plotted points"
    );
    assert!(
        casa_points > 0,
        "expected CASA txt export to contain plotted points"
    );
    assert_eq!(rust_points, casa_points);
}

#[test]
fn u_vs_v_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis", "u", "--yaxis", "v", "--field", "0", "--spw", "0", "--scan", "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "u"),
        ("yaxis", "v"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn amplitude_vs_w_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis",
        "w",
        "--yaxis",
        "amplitude",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "w"),
        ("yaxis", "amp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn amplitude_vs_velocity_png_export_tracks_casa_plotms_ranges() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis",
        "velocity",
        "--yaxis",
        "amplitude",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
        "--freqframe",
        "LSRK",
        "--restfreq",
        "1.420405752GHz",
        "--veldef",
        "RADIO",
    ])
    .expect("run rust msexplore");
    let rust_points = rust_xy_points(&rust);
    let rust_x = numeric_range(rust_points.iter().map(|point| point.0)).expect("rust x range");
    let rust_y = numeric_range(rust_points.iter().map(|point| point.1)).expect("rust y range");
    let rust_png = run_rust_msexplore_png(&[
        "--xaxis",
        "velocity",
        "--yaxis",
        "amplitude",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
        "--freqframe",
        "LSRK",
        "--restfreq",
        "1.420405752GHz",
        "--veldef",
        "RADIO",
    ])
    .expect("run rust msexplore png");
    let casa = run_casa_plotms_png(&[
        ("xaxis", "velocity"),
        ("yaxis", "amp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
        ("freqframe", "LSRK"),
        ("restfreq", "1.420405752GHz"),
        ("veldef", "RADIO"),
    ])
    .expect("run casa plotms png");

    assert_same_image_dimensions(&rust_png, &casa.body);

    let point_count = parse_casa_point_count(&casa.log).expect("CASA point count");
    assert_eq!(rust_points.len(), point_count, "point count mismatch");

    let casa_x = parse_casa_axis_range(&casa.log, "Velocity").expect("CASA velocity range");
    let casa_y = parse_casa_axis_range(&casa.log, "Amp:data").expect("CASA amp range");
    assert!(
        (rust_x.0 - casa_x.0).abs() <= 0.05,
        "velocity min mismatch: rust={} casa={}",
        rust_x.0,
        casa_x.0
    );
    assert!(
        (rust_x.1 - casa_x.1).abs() <= 0.05,
        "velocity max mismatch: rust={} casa={}",
        rust_x.1,
        casa_x.1
    );
    assert!(
        (rust_y.0 - casa_y.0).abs() <= 1.0e-3,
        "amplitude min mismatch: rust={} casa={}",
        rust_y.0,
        casa_y.0
    );
    assert!(
        (rust_y.1 - casa_y.1).abs() <= 1.0e-3,
        "amplitude max mismatch: rust={} casa={}",
        rust_y.1,
        casa_y.1
    );
}

#[test]
fn amplitude_vs_time_iteraxis_scan_txt_manifest_tracks_separate_casa_plotms_scans() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "amplitude_vs_time",
        "--spw",
        "0",
        "--iteraxis",
        "scan",
    ])
    .expect("run rust msexplore");

    let panels = rust_iterated_xy_points(&rust);
    assert!(panels.len() > 1, "expected multiple scan panels");
    for (panel_key, panel_points) in panels {
        let scan = panel_key
            .strip_prefix("scan-")
            .expect("scan panel key prefix");
        let casa = run_casa_plotms(&[
            ("xaxis", "time"),
            ("yaxis", "amp"),
            ("spw", "0"),
            ("scan", scan),
        ])
        .expect("run casa plotms");
        assert_points_match(&panel_points, &casa_xy_points(&casa));
    }
}

#[test]
fn amplitude_vs_time_iteraxis_field_txt_manifest_tracks_separate_casa_plotms_fields() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "amplitude_vs_time",
        "--spw",
        "0",
        "--iteraxis",
        "field",
    ])
    .expect("run rust msexplore");

    let panels = rust_iterated_xy_points(&rust);
    assert!(panels.len() > 1, "expected multiple field panels");
    for (panel_key, panel_points) in panels {
        let field = panel_key
            .strip_prefix("field-")
            .expect("field panel key prefix");
        let casa = run_casa_plotms(&[
            ("xaxis", "time"),
            ("yaxis", "amp"),
            ("field", field),
            ("spw", "0"),
        ])
        .expect("run casa plotms");
        assert_points_match(&panel_points, &casa_xy_points(&casa));
    }
}

#[test]
fn weight_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis", "time", "--yaxis", "weight", "--field", "0", "--spw", "0", "--scan", "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "wt"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn sigma_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis", "time", "--yaxis", "sigma", "--field", "0", "--spw", "0", "--scan", "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "sigma"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn flag_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis", "time", "--yaxis", "flag", "--field", "0", "--spw", "0", "--scan", "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "flag"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn weight_spectrum_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis",
        "time",
        "--yaxis",
        "weight_spectrum",
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
        ("yaxis", "wtsp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn sigma_spectrum_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis",
        "time",
        "--yaxis",
        "sigma_spectrum",
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
        ("yaxis", "sigmasp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn flagrow_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis", "time", "--yaxis", "flagrow", "--field", "0", "--spw", "0", "--scan", "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "flagrow"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn elevation_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "elevation_vs_time",
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
        ("yaxis", "elevation"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn hour_angle_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "hour_angle_vs_time",
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
        ("yaxis", "hourang"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn parallactic_angle_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "parallactic_angle_vs_time",
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
        ("yaxis", "parang"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn azimuth_vs_elevation_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "azimuth_vs_elevation",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "elevation"),
        ("yaxis", "azimuth"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

fn plotms_available() -> bool {
    discover_casa_python().is_some_and(|python| python.plotms_available)
}

fn plotms_shared_dataset_available() -> bool {
    plotms_available() && ngc5921_ms_path().is_some()
}

fn run_rust_msexplore(extra_args: &[&str]) -> Result<String, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    run_rust_msexplore_on(&ms_path, extra_args)
}

fn run_rust_msexplore_png(extra_args: &[&str]) -> Result<Vec<u8>, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let output = temp.path().join("rust-msexplore.png");
    let result = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--plot-output"])
        .arg(&output)
        .args(["--plot-format", "png"])
        .args(["--plot-width", "1600", "--plot-height", "900"])
        .args(extra_args)
        .arg(ms_path)
        .output()
        .map_err(|error| format!("spawn rust msexplore png: {error}"))?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).to_string());
    }
    fs::read(output).map_err(|error| format!("read rust png export: {error}"))
}

fn run_rust_msexplore_on(ms_path: &Path, extra_args: &[&str]) -> Result<String, String> {
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let output = temp.path().join("rust-msexplore.txt");
    let result = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--plot-output"])
        .arg(&output)
        .args(["--plot-format", "txt"])
        .args(extra_args)
        .arg(ms_path)
        .output()
        .map_err(|error| format!("spawn rust msexplore: {error}"))?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).to_string());
    }
    fs::read_to_string(output).map_err(|error| format!("read rust manifest: {error}"))
}

fn run_casa_plotms(kwargs: &[(&str, &str)]) -> Result<String, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    Ok(
        String::from_utf8(run_casa_plotms_export_on(&ms_path, kwargs, "txt")?.body)
            .map_err(|error| format!("decode CASA txt export: {error}"))?,
    )
}

fn run_casa_plotms_png(kwargs: &[(&str, &str)]) -> Result<CasaPlotmsExport, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    run_casa_plotms_export_on(&ms_path, kwargs, "png")
}

fn run_casa_plotms_export_on(
    ms_path: &Path,
    kwargs: &[(&str, &str)],
    expformat: &str,
) -> Result<CasaPlotmsExport, String> {
    let _guard = casa_plotms_lock().lock().expect("lock CASA plotms");
    let casa = discover_casa_python().ok_or_else(|| skip_reason(false))?;
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let output = temp.path().join(match expformat {
        "png" => "casa-plotms.png",
        _ => "casa-plotms.txt",
    });
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
    "expformat": os.environ["CASA_EXPFORMAT"],
    "overwrite": True,
    "showgui": False,
    "verbose": True,
}
"#,
    );
    if expformat == "png" {
        script.push_str("kwargs[\"width\"] = 1600\n");
        script.push_str("kwargs[\"height\"] = 900\n");
    }
    for (key, value) in kwargs {
        script.push_str(&format!("kwargs[{key:?}] = {value:?}\n"));
    }
    script.push_str("plotms(**kwargs)\n");

    let result = Command::new(&casa.program)
        .current_dir(temp.path())
        .arg("-c")
        .arg(&script)
        .env("CASA_VIS", ms_path)
        .env("CASA_OUT", &output)
        .env("CASA_EXPFORMAT", expformat)
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
    let body = fs::read(&output).map_err(|error| format!("read casa export: {error}"))?;
    let log = read_casa_log(temp.path())?;
    Ok(CasaPlotmsExport { body, log })
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

fn rust_xy_points(text: &str) -> Vec<(f64, f64)> {
    let mut points = text
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty() && !trimmed.starts_with('#') && !trimmed.starts_with("series_key")
        })
        .map(|line| {
            let mut parts = line.split('\t');
            let _key = parts.next().expect("series key");
            let _label = parts.next().expect("series label");
            let x = parts
                .next()
                .expect("x value")
                .parse::<f64>()
                .expect("parse rust x");
            let y = parts
                .next()
                .expect("y value")
                .parse::<f64>()
                .expect("parse rust y");
            (x, y)
        })
        .collect::<Vec<_>>();
    points.sort_by(point_order);
    points
}

fn rust_iterated_xy_points(text: &str) -> std::collections::BTreeMap<String, Vec<(f64, f64)>> {
    let mut panels = std::collections::BTreeMap::<String, Vec<(f64, f64)>>::new();
    for line in text.lines().filter(|line| {
        let trimmed = line.trim();
        !trimmed.is_empty() && !trimmed.starts_with('#') && !trimmed.starts_with("panel_key")
    }) {
        let mut parts = line.split('\t');
        let panel_key = parts.next().expect("panel key").to_string();
        let _panel_label = parts.next().expect("panel label");
        let _series_key = parts.next().expect("series key");
        let _series_label = parts.next().expect("series label");
        let x = parts
            .next()
            .expect("x value")
            .parse::<f64>()
            .expect("parse rust x");
        let y = parts
            .next()
            .expect("y value")
            .parse::<f64>()
            .expect("parse rust y");
        panels.entry(panel_key).or_default().push((x, y));
    }
    for points in panels.values_mut() {
        points.sort_by(point_order);
    }
    panels
}

fn casa_xy_points(text: &str) -> Vec<(f64, f64)> {
    let mut points = text
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }
            let mut parts = trimmed.split_whitespace();
            let x = parts.next()?.parse::<f64>().ok()?;
            let y = parts.next()?.parse::<f64>().ok()?;
            Some((x, y))
        })
        .collect::<Vec<_>>();
    points.sort_by(point_order);
    points
}

fn assert_points_match(rust: &[(f64, f64)], casa: &[(f64, f64)]) {
    const CASA_TXT_X_TOLERANCE: f64 = 1.1e-3;
    const CASA_TXT_Y_TOLERANCE: f64 = 1.1e-3;

    assert!(
        !rust.is_empty(),
        "expected rust manifest to contain plotted points"
    );
    assert!(
        !casa.is_empty(),
        "expected CASA txt export to contain plotted points"
    );
    let mut rust = rust
        .iter()
        .copied()
        .map(normalize_casa_txt_point)
        .collect::<Vec<_>>();
    let mut casa = casa
        .iter()
        .copied()
        .map(normalize_casa_txt_point)
        .collect::<Vec<_>>();
    rust.sort_by(point_order);
    casa.sort_by(point_order);

    assert_eq!(rust.len(), casa.len(), "point count mismatch");
    if points_match_pairwise(&rust, &casa, CASA_TXT_X_TOLERANCE, CASA_TXT_Y_TOLERANCE) {
        return;
    }

    let (rust_x, rust_y): (Vec<_>, Vec<_>) = rust.iter().copied().unzip();
    let (casa_x, casa_y): (Vec<_>, Vec<_>) = casa.iter().copied().unzip();
    assert_sorted_axis_matches("x", &rust_x, &casa_x, CASA_TXT_X_TOLERANCE);
    assert_sorted_axis_matches("y", &rust_y, &casa_y, CASA_TXT_Y_TOLERANCE);
}

fn points_match_pairwise(
    rust: &[(f64, f64)],
    casa: &[(f64, f64)],
    x_tolerance: f64,
    y_tolerance: f64,
) -> bool {
    rust.iter()
        .zip(casa.iter())
        .all(|(rust_point, casa_point)| {
            (rust_point.0 - casa_point.0).abs() <= x_tolerance
                && (rust_point.1 - casa_point.1).abs() <= y_tolerance
        })
}

fn assert_sorted_axis_matches(label: &str, rust: &[f64], casa: &[f64], tolerance: f64) {
    assert_eq!(rust.len(), casa.len(), "{label}-axis point count mismatch");
    let mut rust = rust.to_vec();
    let mut casa = casa.to_vec();
    rust.sort_by(f64::total_cmp);
    casa.sort_by(f64::total_cmp);

    for (index, (rust_value, casa_value)) in rust.iter().zip(casa.iter()).enumerate() {
        assert!(
            (rust_value - casa_value).abs() <= tolerance,
            "{label} mismatch at sorted index {index}: rust={} casa={}",
            rust_value,
            casa_value
        );
    }
}

fn point_order(left: &(f64, f64), right: &(f64, f64)) -> Ordering {
    left.0
        .total_cmp(&right.0)
        .then_with(|| left.1.total_cmp(&right.1))
}

fn normalize_casa_txt_point(point: (f64, f64)) -> (f64, f64) {
    (
        normalize_casa_txt_value(point.0),
        normalize_casa_txt_value(point.1),
    )
}

fn normalize_casa_txt_value(value: f64) -> f64 {
    (value * 1_000.0).round() / 1_000.0
}

#[derive(Debug)]
struct CasaPlotmsExport {
    body: Vec<u8>,
    log: String,
}

fn read_casa_log(dir: &Path) -> Result<String, String> {
    let mut logs = fs::read_dir(dir)
        .map_err(|error| format!("read casa temp dir: {error}"))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("casa-") && name.ends_with(".log"))
        })
        .collect::<Vec<_>>();
    logs.sort();
    let path = logs
        .pop()
        .ok_or_else(|| "missing CASA log file".to_string())?;
    fs::read_to_string(path).map_err(|error| format!("read casa log: {error}"))
}

fn parse_casa_point_count(log: &str) -> Option<usize> {
    let marker = "Data selection will yield a total of ";
    log.lines().find_map(|line| {
        let start = line.find(marker)? + marker.len();
        let rest = &line[start..];
        let digits = rest
            .chars()
            .take_while(|ch| ch.is_ascii_digit())
            .collect::<String>();
        digits.parse::<usize>().ok()
    })
}

fn parse_casa_axis_range(log: &str, axis_label: &str) -> Option<(f64, f64)> {
    let marker = format!("{axis_label}: ");
    log.lines().find_map(|line| {
        let start = line.find(&marker)? + marker.len();
        let rest = &line[start..];
        let (lower, rest) = rest.split_once(" to ")?;
        let upper = rest.split_whitespace().next()?;
        Some((lower.parse::<f64>().ok()?, upper.parse::<f64>().ok()?))
    })
}

fn numeric_range(values: impl Iterator<Item = f64>) -> Option<(f64, f64)> {
    let mut values = values;
    let first = values.next()?;
    let mut min = first;
    let mut max = first;
    for value in values {
        min = min.min(value);
        max = max.max(value);
    }
    Some((min, max))
}

fn assert_same_image_dimensions(left: &[u8], right: &[u8]) {
    let left = ImageReader::new(std::io::Cursor::new(left))
        .with_guessed_format()
        .expect("guess rust png format")
        .decode()
        .expect("decode rust png");
    let right = ImageReader::new(std::io::Cursor::new(right))
        .with_guessed_format()
        .expect("guess CASA png format")
        .decode()
        .expect("decode CASA png");
    assert_eq!(
        left.dimensions(),
        right.dimensions(),
        "image dimension mismatch"
    );
}
