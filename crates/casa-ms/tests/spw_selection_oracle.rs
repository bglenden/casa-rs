// SPDX-License-Identifier: LGPL-3.0-or-later
//! Every case of the frozen CASA 6.7.6.14 `spw` oracle
//! (`tools/casa-compat/spw_selection_oracle.py`), replayed through
//! `casa_ms::resolve_spw_selector`: the windows, their data descriptions,
//! the channel list, the selected rows and the warnings CASA logs, or the
//! refusal.

use std::collections::BTreeMap;

use casa_ms::{DataDescriptionRow, SpectralWindowRow, SpwSelection, resolve_spw_selector};
use serde_json::Value;

const ORACLE: &str =
    include_str!("../../../resources/casa-compat/evidence/msselection-spw-casa-20261102.json");

struct Window {
    name: String,
    freqs: Vec<f64>,
    widths: Vec<f64>,
    reference: f64,
    total: f64,
    flag: bool,
}

struct Dataset {
    windows: Vec<Window>,
    descriptions: Vec<DataDescriptionRow>,
}

impl Dataset {
    fn resolve(&self, text: &str) -> casa_ms::MsResult<SpwSelection> {
        let rows = self
            .windows
            .iter()
            .map(|w| SpectralWindowRow {
                name: &w.name,
                chan_freq_hz: &w.freqs,
                chan_width_hz: &w.widths,
                ref_frequency_hz: w.reference,
                total_bandwidth_hz: w.total,
                flag_row: w.flag,
            })
            .collect::<Vec<_>>();
        resolve_spw_selector(&rows, &self.descriptions, text)
    }
}

fn floats(value: &Value) -> Vec<f64> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_f64().unwrap())
        .collect()
}

fn oracle() -> (Value, BTreeMap<String, Dataset>) {
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
        .map(|(name, spec)| {
            let windows = spec["windows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|w| Window {
                    name: w["name"].as_str().unwrap().to_string(),
                    freqs: floats(&w["chan_freq_hz"]),
                    widths: floats(&w["chan_width_hz"]),
                    reference: w["ref_frequency_hz"].as_f64().unwrap(),
                    total: w["total_bandwidth_hz"].as_f64().unwrap(),
                    flag: w["flag_row"].as_bool().unwrap(),
                })
                .collect();
            let descriptions = spec["data_descriptions"]
                .as_array()
                .unwrap()
                .iter()
                .map(|d| DataDescriptionRow {
                    spectral_window_id: i32::try_from(d["spw"].as_i64().unwrap()).unwrap(),
                    flag_row: d["flag_row"].as_bool().unwrap(),
                })
                .collect();
            (
                name.clone(),
                Dataset {
                    windows,
                    descriptions,
                },
            )
        })
        .collect();
    (oracle, datasets)
}

fn integers(value: &Value) -> Vec<i64> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect()
}

/// The port's answer in the oracle's terms. The oracle reads the selected
/// data descriptions from the number of selected MAIN rows (data
/// description `k` holds `2^k` of them), so it sees only those the
/// simulator wrote rows for, one per window; the later ones have none.
fn observed(selection: &SpwSelection, dataset: &Dataset) -> Value {
    let channel = selection
        .channel_list()
        .iter()
        .map(|r| serde_json::json!([r.spw_id, r.start, r.stop, r.step]))
        .collect::<Vec<_>>();
    let rows = selection
        .row_data_descriptions()
        .map_or(serde_json::json!("all"), |rows| {
            let with_rows = rows
                .iter()
                .filter(|&&ddid| ddid < dataset.windows.len())
                .collect::<Vec<_>>();
            serde_json::json!(with_rows)
        });
    // `ms.msselect` resolves a selection without a row condition twice
    // (`MSSelection::getSelectedMS` parses again when the condition is
    // null), so CASA logged its warnings twice.
    let mut warnings = selection.warnings().to_vec();
    if !selection.rows_restricted() {
        warnings.extend_from_slice(selection.warnings());
    }
    serde_json::json!({
        "spw": selection.spw_ids(),
        "spwdd": selection.data_description_ids(),
        "channel": channel,
        "rows": rows,
        "warnings": warnings,
    })
}

#[test]
fn every_frozen_casa_case_resolves_as_casa_resolves_it() {
    let (oracle, datasets) = oracle();
    let cases = oracle["cases"].as_array().unwrap();
    let mut failures = Vec::new();
    for case in cases {
        let text = case["expression"].as_str().unwrap();
        let dataset = &datasets[case["dataset"].as_str().unwrap()];
        let got = dataset.resolve(text);
        let agrees = match (&got, case.get("error")) {
            (Err(_), Some(_)) => true,
            (Ok(selection), None) => {
                let observed = observed(selection, dataset);
                ["spw", "spwdd", "channel", "rows", "warnings"]
                    .iter()
                    .all(|key| observed[key] == case[key])
            }
            _ => false,
        };
        if !agrees {
            let port = got.map(|selection| observed(&selection, dataset).to_string());
            failures.push(format!("{case}\n    port: {port:?}"));
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
    let accepted = cases
        .iter()
        .filter(|case| case.get("error").is_none())
        .count();
    assert!(accepted > 400, "{accepted} accepted cases");
}

#[test]
fn cases_where_casa_is_undefined_or_never_returns_are_refused() {
    let (oracle, datasets) = oracle();
    for key in ["undefined_tokens", "not_returning"] {
        let cases = oracle[key].as_array().unwrap();
        assert!(!cases.is_empty(), "{key}");
        for case in cases {
            let text = case["expression"].as_str().unwrap();
            let dataset = &datasets[case["dataset"].as_str().unwrap()];
            assert!(dataset.resolve(text).is_err(), "{key}: {text:?}");
        }
    }
}

#[test]
fn the_oracle_datasets_number_their_rows_and_windows() {
    let (_, datasets) = oracle();
    let bands = &datasets["bands"];
    assert_eq!(bands.windows.len(), 10);
    assert_eq!(bands.descriptions.len(), 12);
    assert_eq!(
        integers(&serde_json::json!(
            bands
                .descriptions
                .iter()
                .map(|d| d.spectral_window_id)
                .collect::<Vec<_>>()
        )),
        [0, 1, 2, 3, 4, 5, 6, 7, 9, 9, 2, 0]
    );
}
