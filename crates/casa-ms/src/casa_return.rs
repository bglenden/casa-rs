// SPDX-License-Identifier: LGPL-3.0-or-later
//! What CASA 6.7.6.14's flagging tasks return (#931), built from casa-rs's
//! results. `tools/casa-compat/record_casa_returns.py` records the
//! structure of CASA's own returns, which the task tests compare these with.
//!
//! - `flagdata` in summary mode returns its summary dictionary; its other
//!   modes return an empty dictionary.
//! - `flagmanager` in list mode returns its flag versions keyed `0`, `1`...
//!   with the MeasurementSet's path under `'MS'`; its other modes return
//!   `None`.

use std::collections::BTreeMap;
use std::path::Path;

use casa_provider_contracts::{CasaEntry, CasaKey, CasaValue};

use crate::{FlagCounts, FlagSummaryBreakdown, FlagVersionEntry};

/// The name CASA's summary carries: its `name` parameter, `'Summary'` by
/// default.
pub const FLAGDATA_SUMMARY_NAME: &str = "Summary";

/// `flagdata`'s summary-mode return: CASA's dictionary of flagged and
/// total samples, overall and per antenna, array, correlation, field,
/// observation, scan and spectral window, in CASA's key order.
pub fn flagdata_summary_record(summary: &FlagSummaryBreakdown) -> CasaValue {
    let axis = |counts: &BTreeMap<String, FlagCounts>| {
        CasaValue::dict(
            counts
                .iter()
                .map(|(key, counts)| (key.clone(), counts_record(*counts))),
        )
    };
    CasaValue::dict([
        ("antenna", axis(&summary.antenna)),
        ("array", axis(&summary.array)),
        ("correlation", axis(&summary.correlation)),
        ("field", axis(&summary.field)),
        ("flagged", CasaValue::float(summary.overall.flagged as f64)),
        ("name", CasaValue::string(FLAGDATA_SUMMARY_NAME)),
        ("observation", axis(&summary.observation)),
        ("scan", axis(&summary.scan)),
        ("spw", axis(&summary.spw)),
        ("total", CasaValue::float(summary.overall.total as f64)),
        ("type", CasaValue::string("summary")),
    ])
}

fn counts_record(counts: FlagCounts) -> CasaValue {
    CasaValue::dict([
        ("flagged", CasaValue::float(counts.flagged as f64)),
        ("total", CasaValue::float(counts.total as f64)),
    ])
}

/// `flagdata`'s return in every mode but summary: an empty dictionary.
pub fn flagdata_empty_record() -> CasaValue {
    CasaValue::Dict {
        entries: Vec::new(),
    }
}

/// `flagmanager`'s list-mode return: each saved flag version, keyed by its
/// position, as `{'name': ..., 'comment': ...}`, and the MeasurementSet's
/// absolute path under `'MS'`. The `main` entry, the working flags, is not
/// a saved version and is left out, as CASA leaves it out.
pub fn flagmanager_list_record(versions: &[FlagVersionEntry], ms: &Path) -> CasaValue {
    let mut entries = versions
        .iter()
        .filter(|version| version.name != "main")
        .enumerate()
        .map(|(index, version)| CasaEntry {
            key: CasaKey::Int(index as i64),
            value: CasaValue::dict([
                ("name", CasaValue::string(version.name.clone())),
                ("comment", CasaValue::string(version.comment.clone())),
            ]),
        })
        .collect::<Vec<_>>();
    let ms = std::path::absolute(ms).unwrap_or_else(|_| ms.to_path_buf());
    entries.push(CasaEntry {
        key: CasaKey::String("MS".to_string()),
        value: CasaValue::string(ms.display().to_string()),
    });
    CasaValue::Dict { entries }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(value: &CasaValue) -> Vec<String> {
        let CasaValue::Dict { entries } = value else {
            panic!("{value:?}");
        };
        entries
            .iter()
            .map(|entry| match &entry.key {
                CasaKey::String(key) => key.clone(),
                CasaKey::Int(key) => key.to_string(),
            })
            .collect()
    }

    #[test]
    fn a_summary_record_has_casas_keys_in_casas_order() {
        let mut summary = FlagSummaryBreakdown::default();
        let counts = FlagCounts {
            flagged: 48,
            total: 288,
        };
        summary.antenna.insert("A0".into(), counts);
        summary.overall = counts;
        let record = flagdata_summary_record(&summary);
        assert_eq!(
            keys(&record),
            [
                "antenna",
                "array",
                "correlation",
                "field",
                "flagged",
                "name",
                "observation",
                "scan",
                "spw",
                "total",
                "type"
            ]
        );
        assert!(record.is_consistent());
        assert_eq!(
            record.to_plain_json()["antenna"]["A0"],
            serde_json::json!({"flagged": 48.0, "total": 288.0})
        );
    }

    #[test]
    fn a_version_list_skips_the_working_copy_and_names_the_ms() {
        let versions = [
            FlagVersionEntry {
                name: "main".into(),
                comment: "working copy in main table".into(),
            },
            FlagVersionEntry {
                name: "flagdata_1".into(),
                comment: "Flags autosave".into(),
            },
        ];
        let record = flagmanager_list_record(&versions, Path::new("/data/target.ms"));
        assert_eq!(keys(&record), ["0", "MS"]);
        let CasaValue::Dict { entries } = &record else {
            unreachable!()
        };
        assert_eq!(entries[0].key, CasaKey::Int(0));
        assert_eq!(
            entries[1].value,
            CasaValue::string("/data/target.ms".to_string())
        );
    }
}
