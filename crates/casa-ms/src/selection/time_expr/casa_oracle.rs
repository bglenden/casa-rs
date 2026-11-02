// SPDX-License-Identifier: LGPL-3.0-or-later
//! Every case of the frozen CASA 6.7.6.14 `timerange` oracle
//! (`tools/casa-compat/time_selection_oracle.py`), replayed through the port:
//! curated forms, seeded random expressions, first-logical-row and later-term
//! defaults, and the defaults rebuilt from assorted sub-second row times.

use serde_json::Value;

use super::*;

const ORACLE: &str = include_str!(
    "../../../../../resources/casa-compat/evidence/msselection-time-casa-20261102.json"
);

struct Row {
    time_s: f64,
    exposure_s: f64,
    field: i64,
    flag_row: bool,
}

/// The MAIN rows the oracle's segments describe.
fn rows(segments: &Value) -> Vec<Row> {
    let mut rows = Vec::new();
    for segment in segments.as_array().unwrap() {
        let number = |key: &str| segment[key].as_f64().unwrap();
        for index in 0..segment["rows"].as_u64().unwrap() {
            #[expect(clippy::cast_precision_loss, reason = "a row index below 2^53")]
            let step = number("step_s") * index as f64;
            rows.push(Row {
                time_s: number("time0_s") + step,
                exposure_s: number("exposure_s"),
                field: segment["field"].as_i64().unwrap(),
                flag_row: segment["flag_row"].as_bool().unwrap(),
            });
        }
    }
    rows
}

fn defaults(row: &Row) -> TimeDefaults {
    TimeDefaults {
        time_mjd_s: row.time_s,
        exposure_s: row.exposure_s,
    }
}

/// The rows `MSSelection` keeps: the FIELD ids selected, ANDed with the time
/// selection resolved from MSSelection's default rows, as `[first, last]` runs.
fn selected(rows: &[Row], select: &Value, text: &str) -> MsResult<Vec<[u64; 2]>> {
    let fields = select.get("field").map(|ids| {
        ids.as_str()
            .unwrap()
            .split(',')
            .map(|id| id.parse::<i64>().unwrap())
            .collect::<Vec<_>>()
    });
    let other = |row: usize| {
        fields
            .as_ref()
            .is_none_or(|ids| ids.contains(&rows[row].field))
    };
    let selector = TimeSelector::parse(text)?;
    let time = if selector.selects_all() {
        TimeSelection::all()
    } else {
        let first = if fields.is_some() {
            msselection_first_logical_row(
                rows.len(),
                LogicalRowFilter::OtherSelections(&mut |row| other(row)),
            )?
        } else {
            msselection_first_logical_row(
                rows.len(),
                LogicalRowFilter::FlagRow(&mut |row| rows[row].flag_row),
            )?
        };
        selector.resolve(TimeSelectorDefaults {
            first_term: defaults(&rows[first]),
            later_terms: defaults(&rows[0]),
        })?
    };
    let mut runs: Vec<[u64; 2]> = Vec::new();
    for row in (0..rows.len()).filter(|&row| other(row) && time.holds(rows[row].time_s)) {
        let row = row as u64;
        match runs.last_mut() {
            Some(run) if run[1] + 1 == row => run[1] = row,
            _ => runs.push([row, row]),
        }
    }
    Ok(runs)
}

fn runs_of(value: &Value) -> Vec<[u64; 2]> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|run| [run[0].as_u64().unwrap(), run[1].as_u64().unwrap()])
        .collect()
}

fn oracle() -> (Value, std::collections::BTreeMap<String, Vec<Row>>) {
    let oracle: Value = serde_json::from_str(ORACLE).unwrap();
    assert_eq!(oracle["casa_tools"], "6.7.6-14");
    assert_eq!(
        oracle["casacore_commit"],
        "aaf72eb7edd5a5fecefa78ca04713233b7b6ffd6"
    );
    let datasets = oracle["datasets"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, segments)| (name.clone(), rows(segments)))
        .collect();
    (oracle, datasets)
}

#[test]
fn a_refused_bracketed_range_leaves_no_state_behind() {
    // CASA keeps `[`'s edge state in process globals that a refused bracketed
    // range leaves set (MSTimeGram.yy:93-94, 151), so its next unbracketed
    // range is edge-inclusive. Each parse here starts afresh.
    let (oracle, datasets) = oracle();
    let leaks = oracle["bracket_state_leaks"].as_array().unwrap();
    assert!(!leaks.is_empty());
    for leak in leaks {
        let rows = &datasets[leak["dataset"].as_str().unwrap()];
        let refused = leak["after_refused"].as_str().unwrap();
        assert!(selected(rows, &Value::Null, refused).is_err(), "{refused}");
        let text = leak["timerange"].as_str().unwrap();
        let port = selected(rows, &Value::Null, text).unwrap();
        assert_eq!(port, runs_of(&leak["rows"]), "{text}");
        assert_ne!(port, runs_of(&leak["casa_rows_after_refusal"]), "{text}");
    }
}

#[test]
fn every_frozen_casa_case_selects_casas_rows_or_is_refused() {
    let (oracle, datasets) = oracle();
    let cases = oracle["cases"].as_array().unwrap();
    let mut failures = Vec::new();
    for case in cases {
        let text = case["timerange"].as_str().unwrap();
        let rows = &datasets[case["dataset"].as_str().unwrap()];
        let got = selected(rows, &case["select"], text);
        let agrees = match (&got, case.get("rows")) {
            (Ok(runs), Some(expected)) => *runs == runs_of(expected),
            (Err(_), None) => true,
            _ => false,
        };
        if !agrees {
            failures.push(format!("{case}\n    port: {got:?}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} CASA cases differ:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
    assert!(cases.len() > 1500);
}
