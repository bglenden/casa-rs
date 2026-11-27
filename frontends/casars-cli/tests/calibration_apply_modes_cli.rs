// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration apply-mode CLI history test moved from `casa-calibration/tests/apply_modes.rs`.

#[path = "../../../crates/casa-calibration/tests/common/mod.rs"]
mod common;

use casa_core::{Complex32, ScalarValue};
use casa_tables::{Table, TableOptions};

#[test]
fn public_applycal_records_all_parameters_including_trial_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = common::create_apply_fixture_ms(dir.path(), false);
    let rows = [0, 1].map(|antenna| common::SyntheticGainSolutionRow {
        time_seconds: 100.,
        field_id: 0,
        spectral_window_id: 0,
        antenna_id: antenna,
        gains: vec![Complex32::new(2., 0.); 2],
        flags: vec![false; 2],
    });
    let gain =
        common::create_apply_gain_caltable(&dir.path().join("gain.gcal"), &["TARGET0"], &rows);
    for (index, mode) in ["calflag", "trial"].into_iter().enumerate() {
        let request = dir.path().join("request.json");
        std::fs::write(
            &request,
            serde_json::to_vec(&serde_json::json!({"surface":"applycal","values":{
                "vis":path,"gaintable":[gain],"spw":"0","applymode":mode,"flagbackup":false
            }}))
            .unwrap(),
        )
        .unwrap();
        let start = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
            + 40587. * 86400.;
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_calibrate"))
            .args(["--json-run", request.to_str().unwrap()])
            .current_dir(dir.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let end = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
            + 40587. * 86400.;
        let history = Table::open(TableOptions::new(path.join("HISTORY"))).unwrap();
        assert_eq!(history.row_count(), 23 * (index + 1));
        let text = |row, name| match history.cell_accessor(row, name).unwrap().scalar().unwrap() {
            ScalarValue::String(text) => text.clone(),
            _ => panic!("{name}"),
        };
        assert_eq!(text(23 * index, "MESSAGE"), "taskname=applycal");
        assert_eq!(
            text(23 * index + 2, "MESSAGE"),
            format!("vis         = \"{}\"", path.display())
        );
        assert_eq!(text(23 * index + 3, "MESSAGE"), "field       = \"\"");

        assert_eq!(
            text(23 * index + 21, "MESSAGE"),
            format!("applymode   = \"{mode}\"")
        );
        for row in 23 * index..23 * (index + 1) {
            assert_eq!(text(row, "APPLICATION"), "ms");
            assert_eq!(text(row, "ORIGIN"), "applycal");
            assert_eq!(text(row, "PRIORITY"), "INFO");
            assert_eq!(
                history
                    .cell_accessor(row, "OBJECT_ID")
                    .unwrap()
                    .scalar()
                    .unwrap(),
                &ScalarValue::Int32(0)
            );
            let ScalarValue::Float64(time) = history
                .cell_accessor(row, "TIME")
                .unwrap()
                .scalar()
                .unwrap()
            else {
                panic!("TIME")
            };
            assert!(*time >= start - 1. && *time <= end + 1.);
        }
    }
}
