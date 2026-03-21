// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use casacore_test_support::casatestdata_path;
use tempfile::tempdir;

const CASA_PYTHON: &str = "/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python";

#[derive(Debug)]
struct RunResult {
    text: String,
    elapsed_ms: u128,
}

#[test]
fn listobs_verbose_matches_casa_core_sections() {
    if !casa_environment_available() {
        eprintln!("skipping CASA parity test: CASA python or dataset is unavailable");
        return;
    }

    let rust = run_rust_listobs(&[]).expect("run rust listobs");
    let casa = run_casa_listobs(true, None, None, None, false).expect("run casa listobs");

    assert_eq!(
        extract_prefixed_count(&rust.text, "Data records:"),
        extract_prefixed_count(&casa.text, "Data records:")
    );
    assert_eq!(
        extract_prefixed_count(&rust.text, "Fields:"),
        extract_prefixed_count(&casa.text, "Fields:")
    );
    assert_eq!(
        extract_prefixed_count(&rust.text, "Sources:"),
        extract_prefixed_count(&casa.text, "Sources:")
    );
    assert_eq!(
        extract_prefixed_count(&rust.text, "Antennas:"),
        extract_prefixed_count(&casa.text, "Antennas:")
    );
    assert_eq!(
        extract_scan_line_count(&rust.text),
        extract_scan_line_count(&casa.text)
    );
    assert_eq!(
        extract_field_names(&rust.text),
        extract_field_names(&casa.text)
    );
    assert_contains_same_markers(
        &rust.text,
        &casa.text,
        &[
            "ObservationID = 0",
            "1331+30500002_0",
            "1445+09900002_0",
            "N5921_2",
        ],
    );

    let rust_samples = measure_rust_case(&[]);
    let casa_samples = measure_casa_case(true, None, None, None, false);
    assert_no_gross_perf_regression("default verbose", &rust_samples, &casa_samples);
}

#[test]
fn listobs_terse_matches_casa_core_sections() {
    if !casa_environment_available() {
        eprintln!("skipping CASA parity test: CASA python or dataset is unavailable");
        return;
    }

    let rust = run_rust_listobs(&["--no-verbose"]).expect("run rust listobs");
    let casa = run_casa_listobs(false, None, None, None, false).expect("run casa listobs");

    assert_eq!(
        extract_prefixed_count(&rust.text, "Fields:"),
        extract_prefixed_count(&casa.text, "Fields:")
    );
    assert_eq!(
        extract_prefixed_count(&rust.text, "Antennas:"),
        extract_prefixed_count(&casa.text, "Antennas:")
    );
    assert_eq!(
        extract_field_names(&rust.text),
        extract_field_names(&casa.text)
    );
    assert_eq!(
        rust.text.contains("Sources:"),
        casa.text.contains("Sources:")
    );
    assert_eq!(
        rust.text.contains("ObservationID ="),
        casa.text.contains("ObservationID =")
    );

    let rust_samples = measure_rust_case(&["--no-verbose"]);
    let casa_samples = measure_casa_case(false, None, None, None, false);
    assert_no_gross_perf_regression("terse", &rust_samples, &casa_samples);
}

#[test]
fn listobs_field_selection_matches_casa_core_sections() {
    if !casa_environment_available() {
        eprintln!("skipping CASA parity test: CASA python or dataset is unavailable");
        return;
    }

    let rust = run_rust_listobs(&["--field", "1"]).expect("run rust listobs");
    let casa = run_casa_listobs(true, Some("1"), None, None, false).expect("run casa listobs");

    assert_eq!(
        extract_prefixed_count(&rust.text, "Data records:"),
        extract_prefixed_count(&casa.text, "Data records:")
    );
    assert_eq!(
        extract_prefixed_count(&rust.text, "Fields:"),
        extract_prefixed_count(&casa.text, "Fields:")
    );
    assert_eq!(
        extract_scan_line_count(&rust.text),
        extract_scan_line_count(&casa.text)
    );
    assert_eq!(
        extract_field_names(&rust.text),
        extract_field_names(&casa.text)
    );
    assert_contains_same_markers(&rust.text, &casa.text, &["1445+09900002_0"]);

    let rust_samples = measure_rust_case(&["--field", "1"]);
    let casa_samples = measure_casa_case(true, Some("1"), None, None, false);
    assert_no_gross_perf_regression("field=1", &rust_samples, &casa_samples);
}

#[test]
fn listobs_uvrange_selection_matches_casa_core_sections() {
    if !casa_environment_available() {
        eprintln!("skipping CASA parity test: CASA python or dataset is unavailable");
        return;
    }

    let rust = run_rust_listobs(&["--uvrange", "0~100m"]).expect("run rust listobs");
    let casa = run_casa_listobs(true, None, Some("0~100m"), None, false).expect("run casa listobs");

    assert_eq!(
        extract_prefixed_count(&rust.text, "Data records:"),
        extract_prefixed_count(&casa.text, "Data records:")
    );
    assert_eq!(
        extract_prefixed_count(&rust.text, "Fields:"),
        extract_prefixed_count(&casa.text, "Fields:")
    );
    assert_eq!(
        extract_scan_line_count(&rust.text),
        extract_scan_line_count(&casa.text)
    );
    assert_eq!(
        extract_field_names(&rust.text),
        extract_field_names(&casa.text)
    );

    let rust_samples = measure_rust_case(&["--uvrange", "0~100m"]);
    let casa_samples = measure_casa_case(true, None, Some("0~100m"), None, false);
    assert_no_gross_perf_regression("uvrange=0~100m", &rust_samples, &casa_samples);
}

#[test]
fn listobs_listunfl_matches_casa_core_sections() {
    if !casa_environment_available() {
        eprintln!("skipping CASA parity test: CASA python or dataset is unavailable");
        return;
    }

    let rust = run_rust_listobs(&["--listunfl"]).expect("run rust listobs");
    let casa = run_casa_listobs(true, None, None, None, true).expect("run casa listobs");

    assert_eq!(
        rust.text.contains("nUnflRows"),
        casa.text.contains("nUnflRows")
    );
    assert_eq!(
        extract_prefixed_count(&rust.text, "Data records:"),
        extract_prefixed_count(&casa.text, "Data records:")
    );
    assert_eq!(
        extract_prefixed_count(&rust.text, "Fields:"),
        extract_prefixed_count(&casa.text, "Fields:")
    );
    assert_eq!(
        extract_scan_line_count(&rust.text),
        extract_scan_line_count(&casa.text)
    );
    assert_eq!(
        extract_field_names(&rust.text),
        extract_field_names(&casa.text)
    );
    assert_contains_same_markers(&rust.text, &casa.text, &["nUnflRows", "4509", "6804"]);

    let rust_samples = measure_rust_case(&["--listunfl"]);
    let casa_samples = measure_casa_case(true, None, None, None, true);
    assert_no_gross_perf_regression("listunfl", &rust_samples, &casa_samples);
}

#[test]
fn listobs_timerange_selection_matches_casa_core_sections() {
    if !casa_environment_available() {
        eprintln!("skipping CASA parity test: CASA python or dataset is unavailable");
        return;
    }

    let rust = run_rust_listobs(&["--timerange", "09:27:15~09:29:45"]).expect("run rust listobs");
    let casa = run_casa_listobs(true, None, None, Some("09:27:15~09:29:45"), false)
        .expect("run casa listobs");

    assert_eq!(
        extract_prefixed_count(&rust.text, "Data records:"),
        extract_prefixed_count(&casa.text, "Data records:")
    );
    assert_eq!(
        extract_prefixed_count(&rust.text, "Fields:"),
        extract_prefixed_count(&casa.text, "Fields:")
    );
    assert_eq!(
        extract_scan_line_count(&rust.text),
        extract_scan_line_count(&casa.text)
    );
    assert_eq!(
        extract_field_names(&rust.text),
        extract_field_names(&casa.text)
    );
    assert_contains_same_markers(&rust.text, &casa.text, &["1445+09900002_0", "1890"]);

    let rust_samples = measure_rust_case(&["--timerange", "09:27:15~09:29:45"]);
    let casa_samples = measure_casa_case(true, None, None, Some("09:27:15~09:29:45"), false);
    assert_no_gross_perf_regression("timerange=09:27:15~09:29:45", &rust_samples, &casa_samples);
}

fn casa_environment_available() -> bool {
    Path::new(CASA_PYTHON).is_file() && ngc5921_ms_path().is_some()
}

fn run_rust_listobs(extra_args: &[&str]) -> Result<RunResult, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| missing_testdata_message())?;
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let output = temp.path().join("rust-listobs.txt");
    let start = Instant::now();
    let result = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(extra_args)
        .args(["--listfile"])
        .arg(&output)
        .args(["--overwrite"])
        .arg(&ms_path)
        .output()
        .map_err(|error| format!("spawn rust listobs: {error}"))?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).to_string());
    }
    let elapsed_ms = start.elapsed().as_millis();
    let text = fs::read_to_string(&output).map_err(|error| format!("read rust output: {error}"))?;
    Ok(RunResult { text, elapsed_ms })
}

fn run_casa_listobs(
    verbose: bool,
    field: Option<&str>,
    uvrange: Option<&str>,
    timerange: Option<&str>,
    listunfl: bool,
) -> Result<RunResult, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| missing_testdata_message())?;
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let output = temp.path().join("casa-listobs.txt");
    let script = r#"
import os
from casatasks import listobs

kwargs = {
    "vis": os.environ["CASA_VIS"],
    "listfile": os.environ["CASA_OUT"],
    "overwrite": True,
    "verbose": os.environ["CASA_VERBOSE"] == "1",
    "listunfl": os.environ["CASA_LISTUNFL"] == "1",
}
field = os.environ.get("CASA_FIELD", "")
if field:
    kwargs["field"] = field
uvrange = os.environ.get("CASA_UVRANGE", "")
if uvrange:
    kwargs["uvrange"] = uvrange
timerange = os.environ.get("CASA_TIMERANGE", "")
if timerange:
    kwargs["timerange"] = timerange
listobs(**kwargs)
"#;
    let start = Instant::now();
    let result = Command::new(CASA_PYTHON)
        .arg("-c")
        .arg(script)
        .env("CASA_VIS", &ms_path)
        .env("CASA_OUT", &output)
        .env("CASA_VERBOSE", if verbose { "1" } else { "0" })
        .env("CASA_FIELD", field.unwrap_or(""))
        .env("CASA_UVRANGE", uvrange.unwrap_or(""))
        .env("CASA_TIMERANGE", timerange.unwrap_or(""))
        .env("CASA_LISTUNFL", if listunfl { "1" } else { "0" })
        .output()
        .map_err(|error| format!("spawn casa listobs: {error}"))?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).to_string());
    }
    let elapsed_ms = start.elapsed().as_millis();
    let text = fs::read_to_string(&output).map_err(|error| format!("read casa output: {error}"))?;
    Ok(RunResult { text, elapsed_ms })
}

fn ngc5921_ms_path() -> Option<PathBuf> {
    casatestdata_path("measurementset/vla/ngc5921.ms").filter(|path| path.exists())
}

fn missing_testdata_message() -> String {
    "missing ngc5921.ms under CASA_RS_TESTDATA_ROOT or ../casatestdata".to_string()
}

fn measure_rust_case(extra_args: &[&str]) -> Vec<u128> {
    let _ = run_rust_listobs(extra_args).expect("warm rust listobs");
    (0..3)
        .map(|_| {
            run_rust_listobs(extra_args)
                .expect("measure rust listobs")
                .elapsed_ms
        })
        .collect()
}

fn measure_casa_case(
    verbose: bool,
    field: Option<&str>,
    uvrange: Option<&str>,
    timerange: Option<&str>,
    listunfl: bool,
) -> Vec<u128> {
    let _ =
        run_casa_listobs(verbose, field, uvrange, timerange, listunfl).expect("warm casa listobs");
    (0..3)
        .map(|_| {
            run_casa_listobs(verbose, field, uvrange, timerange, listunfl)
                .expect("measure casa listobs")
                .elapsed_ms
        })
        .collect()
}

fn assert_no_gross_perf_regression(case: &str, rust_samples: &[u128], casa_samples: &[u128]) {
    let rust_median = median_ms(rust_samples);
    let casa_median = median_ms(casa_samples);
    let ratio = rust_median as f64 / casa_median.max(1) as f64;

    eprintln!(
        "listobs {case}: rust median={}ms casa median={}ms ratio={ratio:.2}x",
        rust_median, casa_median
    );

    assert!(
        !(ratio > 2.0 && rust_median > casa_median + 1_000),
        "gross perf regression for {case}: rust median={}ms casa median={}ms ratio={ratio:.2}x",
        rust_median,
        casa_median
    );
}

fn median_ms(values: &[u128]) -> u128 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

fn assert_contains_same_markers(rust_text: &str, casa_text: &str, markers: &[&str]) {
    for marker in markers {
        assert_eq!(
            rust_text.contains(marker),
            casa_text.contains(marker),
            "marker presence differs for {marker:?}"
        );
    }
}

fn extract_prefixed_count(text: &str, prefix: &str) -> usize {
    let line = text
        .lines()
        .find(|line| line.trim_start().starts_with(prefix))
        .unwrap_or_else(|| panic!("missing line with prefix {prefix:?}"));
    line.trim_start_matches(|ch: char| ch.is_whitespace())
        .trim_start_matches(prefix)
        .chars()
        .skip_while(|ch| ch.is_whitespace())
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>()
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("parse count from line {line:?}"))
}

fn extract_scan_line_count(text: &str) -> usize {
    let mut in_scans = false;
    let mut count = 0;
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("Date        Timerange") {
            in_scans = true;
            continue;
        }
        if in_scans && trimmed.contains("(nRows = Total number of rows per scan)") {
            break;
        }
        if in_scans && line.contains(" - ") {
            count += 1;
        }
    }
    count
}

fn extract_field_names(text: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut in_fields = false;
    for line in text.lines() {
        let trimmed = line.trim_end();
        if trimmed.starts_with("  ID   Code Name") {
            in_fields = true;
            continue;
        }
        if in_fields
            && (trimmed.is_empty()
                || trimmed.starts_with("Spectral Windows:")
                || trimmed.starts_with("Sources:")
                || trimmed.starts_with("Antennas:"))
        {
            break;
        }
        if in_fields
            && trimmed
                .trim_start()
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_digit())
        {
            let name = if trimmed.len() >= 32 {
                trimmed[12..32].trim().to_string()
            } else {
                trimmed.split_whitespace().nth(2).unwrap_or("").to_string()
            };
            if !name.is_empty() {
                names.push(name);
            }
        }
    }
    names
}
