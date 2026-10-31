// SPDX-License-Identifier: LGPL-3.0-or-later
//! The data a calibration solve selected, as CASA's `gaincal` and
//! `bandpass` report it in their return value's `selectvis`.

use std::collections::BTreeSet;

use casa_ms::MeasurementSet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::solve::GainSolveError;
use crate::solve::grouping::SelectedSolveRow;

/// The data a solve selected: the sorted, distinct values each axis of its
/// cross-correlation rows takes, and the intents the MeasurementSet names.
///
/// CASA's `gaincal` and `bandpass` return this as `selectvis`. Their
/// selection always leaves out autocorrelations (`ANTENNA1!=ANTENNA2`), so a
/// row whose antennas are equal does not count here either. Flags do not
/// narrow it: an antenna whose data are all flagged is still selected.
/// The intents are not narrowed by the selection at all: CASA 6.7.6 lists
/// every `OBS_MODE` of the `STATE` table, split at its commas, sorted and
/// distinct, and none when `STATE` has no rows. An empty `OBS_MODE` names
/// none, though an empty item between two commas is kept.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SelectedData {
    /// The antennas of the selected baselines, `ANTENNA1` and `ANTENNA2`.
    pub antennas: Vec<i32>,
    /// The selected `FIELD_ID`s.
    pub fields: Vec<i32>,
    /// The intents the `STATE` table names.
    pub intents: Vec<String>,
    /// The selected `OBSERVATION_ID`s.
    pub observations: Vec<i32>,
    /// The selected `SCAN_NUMBER`s.
    pub scans: Vec<i32>,
    /// The selected spectral windows, through `DATA_DESCRIPTION`.
    pub spectral_windows: Vec<i32>,
}

impl SelectedData {
    /// The data `rows` of `ms` select.
    pub(crate) fn of(
        ms: &MeasurementSet,
        rows: &[SelectedSolveRow],
    ) -> Result<Self, GainSolveError> {
        let mut antennas = BTreeSet::new();
        let mut fields = BTreeSet::new();
        let mut observations = BTreeSet::new();
        let mut scans = BTreeSet::new();
        let mut spectral_windows = BTreeSet::new();
        for row in rows.iter().filter(|row| row.antenna1 != row.antenna2) {
            antennas.extend([row.antenna1, row.antenna2]);
            fields.insert(row.field_id);
            observations.insert(row.observation_id);
            scans.insert(row.scan_number);
            spectral_windows.insert(row.data_spw_id);
        }
        Ok(Self {
            antennas: antennas.into_iter().collect(),
            fields: fields.into_iter().collect(),
            intents: intents(ms)?,
            observations: observations.into_iter().collect(),
            scans: scans.into_iter().collect(),
            spectral_windows: spectral_windows.into_iter().collect(),
        })
    }
}

/// Every intent `ms`'s `STATE` table names, sorted and distinct; none when
/// the MeasurementSet has no `STATE` table.
fn intents(ms: &MeasurementSet) -> Result<Vec<String>, GainSolveError> {
    let Ok(state) = ms.state() else {
        return Ok(Vec::new());
    };
    let failed = |source| GainSolveError::OpenMeasurementSet {
        path: ms
            .path()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<in-memory>".to_string()),
        source,
    };
    let modes = (0..state.row_count())
        .map(|row| state.string(row, "OBS_MODE").map_err(failed))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(intents_of(modes.iter().map(String::as_str)))
}

/// The intents `modes` name, as CASA 6.7.6.14 lists them: each mode split
/// at its commas, untrimmed, sorted and distinct. An empty mode names none,
/// though an empty item between two commas is kept.
fn intents_of<'a>(modes: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    modes
        .into_iter()
        .filter(|mode| !mode.is_empty())
        .flat_map(|mode| mode.split(','))
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::intents_of;

    #[test]
    fn intents_are_casas_split_sorted_and_distinct() {
        // Each case as CASA 6.7.6.14's gaincal lists it in `selectvis`.
        for (modes, expected) in [
            (&["B,A", "C", "B"][..], &["A", "B", "C"][..]),
            (&[""], &[]),
            (&["", "A"], &["A"]),
            (&["A, B"], &[" B", "A"]),
            (&["A,,B"], &["", "A", "B"]),
        ] {
            assert_eq!(intents_of(modes.iter().copied()), expected, "{modes:?}");
        }
    }
}
