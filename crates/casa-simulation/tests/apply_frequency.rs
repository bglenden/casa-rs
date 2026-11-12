// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frequency-dependent B/Tf/Df application against independent CASA (#1053).
#[path = "apply_parameters/input.rs"]
mod input;
mod predict_support;
#[path = "apply_parameters/support.rs"]
mod support;
use flate2::read::GzDecoder;
use serde_json::Value;
use std::io::Read;

#[test]
fn frequency_tables_match_casa_with_phase_wraps_and_gaps() {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_frequency_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 76);
    for kind in ["B", "Tf", "Df"] {
        let case = |mode: &str| {
            cases
                .iter()
                .find(|case| case["name"] == format!("{kind}_nearest_{mode}_shifted"))
                .unwrap()
        };
        for candidate in ["nearest", "cubic"] {
            let (mut error, mut power) = (0.0, 0.0);
            for (a, b) in case(candidate)["rows"]
                .as_array()
                .unwrap()
                .iter()
                .zip(case("linear")["rows"].as_array().unwrap())
            {
                for (a, b) in a["data"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .zip(b["data"].as_array().unwrap())
                {
                    for i in 0..2 {
                        let (a, b) = (a[i].as_f64().unwrap(), b[i].as_f64().unwrap());
                        error += (a - b).powi(2);
                        power += b.powi(2);
                    }
                }
            }
            assert!(
                (error / power).sqrt() > 2e-3,
                "{kind} {candidate}/linear frequency discriminator"
            );
        }
    }
    for case in cases {
        assert_eq!(
            support::run(case, 1),
            support::run(case, 3),
            "workers for {}",
            case["name"]
        );
    }
}

#[test]
fn invalid_mapping_interpolation_and_short_cubic_raise_atomically_as_casa_does() {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_frequency_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    let refusals = fixture["refusals"].as_array().unwrap();
    assert_eq!(refusals.len(), 13);
    for case in refusals {
        refused(case);
    }
}

/// Refusal class/stage and DATA atomicity are supplied by the independent CASA run.
fn refused(case: &Value) {
    use casa_simulation::CasaOutcome;
    assert_eq!(case["exception_class"], "RuntimeError");
    assert_eq!(case["unchanged_data"], true);
    let directory = tempfile::tempdir().unwrap();
    let (path, mut sm, spec) = support::prepared(directory.path(), case, 1);
    let error = if case["stage"] == "setapply" {
        sm.set_apply(&spec).expect_err("CASA refuses setapply")
    } else {
        sm.set_apply(&spec).unwrap();
        sm.corrupt().expect_err("CASA refuses corrupt")
    };
    assert_eq!(
        error.outcome(),
        CasaOutcome::Raises,
        "{}: {error}",
        case["name"]
    );
    sm.close().unwrap();
    let (rows, _) = predict_support::read_main(&path);
    for row in rows {
        assert!(
            row.data
                .iter()
                .all(|&value| value == num_complex::Complex32::new(3.0, 4.0))
        );
    }
}

#[test]
fn external_frequency_grid_is_loaded_and_held_at_setapply() {
    use casa_tables::{Table, TableOptions};
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_frequency_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "Tf_nearest_linear_shifted")
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let (path, mut sm, spec) = support::prepared(directory.path(), case, 1);
    let calibration = std::path::Path::new(&spec.table);
    let external = directory.path().join("external-window");
    std::fs::rename(calibration.join("SPECTRAL_WINDOW"), &external).unwrap();
    let mut table = Table::open(TableOptions::new(calibration)).unwrap();
    table.keywords_mut().upsert(
        "SPECTRAL_WINDOW",
        casa_types::Value::TableRef(external.to_string_lossy().into_owned()),
    );
    table.save(TableOptions::new(calibration)).unwrap();
    drop(table);
    sm.set_apply(&spec).unwrap();
    // Like the CPARAM snapshot, a loaded frequency axis survives removal
    // of its input table before any later corrupt call.
    std::fs::remove_dir_all(external).unwrap();
    sm.corrupt().unwrap();
    sm.close().unwrap();
    support::compare(&path, case);
}

#[test]
fn unavailable_table_window_stays_explicit_and_preserves_data() {
    use casa_simulation::CasaOutcome;
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_apply_frequency_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    let mut case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "B_spw_broadcast")
        .unwrap()
        .clone();
    // This MS uses window1; the table has actual solutions only in0.
    // Identity mapping therefore reaches the unfinished whole-step scope.
    case["options"]["spwmap"] = serde_json::json!([-1]);
    let directory = tempfile::tempdir().unwrap();
    let (path, mut sm, spec) = support::prepared(directory.path(), &case, 1);
    sm.set_apply(&spec).unwrap();
    let error = sm
        .corrupt()
        .expect_err("whole-window coverage is unfinished");
    assert_eq!(error.outcome(), CasaOutcome::NotYetImplemented);
    assert!(error.message().contains("#1053"));
    sm.close().unwrap();
    for row in predict_support::read_main(&path).0 {
        assert!(
            row.data
                .iter()
                .all(|&value| value == num_complex::Complex32::new(3.0, 4.0))
        );
    }
}
