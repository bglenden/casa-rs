// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stored correlation planes selected by the CASA manual and quack agents.

use std::collections::BTreeSet;

use crate::listobs::stokes_name;
use crate::{MeasurementSet, MsError};

use super::{FlagDataMode, FlaggingError, display_ms_path};

pub(super) struct CorrelationSelection {
    by_description: Vec<Vec<usize>>,
}

impl CorrelationSelection {
    pub(super) fn read(
        ms: &MeasurementSet,
        mode: FlagDataMode,
        expression: &str,
    ) -> Result<Option<Self>, FlaggingError> {
        let text = expression
            .split('\0')
            .next()
            .unwrap_or_default()
            .to_uppercase();
        if text.is_empty() {
            return Ok(None);
        }
        if !matches!(mode, FlagDataMode::Manual | FlagDataMode::Quack)
            || ["ABS_", "ARG_", "REAL_", "IMAG_", "NORM_", "WVR"]
                .iter()
                .any(|prefix| text.contains(prefix))
        {
            return Err(FlaggingError::MutateFlags {
                path: display_ms_path(ms),
                reason: format!(
                    "flagdata correlation {expression:?} is unimplemented in this mode (#978)"
                ),
            });
        }

        let failed = |source: MsError| FlaggingError::MutateFlags {
            path: display_ms_path(ms),
            reason: format!("resolve stored flagging correlations: {source}"),
        };
        let polarization = ms.polarization().map_err(failed)?;
        let modes = (0..polarization.row_count())
            .map(|row| {
                polarization
                    .corr_type(row)
                    .map(|codes| codes.into_iter().map(stokes_name).collect::<Vec<_>>())
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(failed)?;
        // CASA finds names in the expression using the whole POLARIZATION
        // table, then requires each selected buffer to contain every match.
        let required: BTreeSet<_> = modes
            .iter()
            .flatten()
            .copied()
            .filter(|name| *name != "??" && text.contains(*name))
            .collect();
        let by_polarization: Vec<Vec<usize>> = modes
            .iter()
            .map(|names| {
                if !required.iter().all(|name| names.contains(name)) {
                    return Vec::new();
                }
                names
                    .iter()
                    .enumerate()
                    .filter_map(|(plane, name)| required.contains(name).then_some(plane))
                    .collect()
            })
            .collect();

        let descriptions = ms.data_description().map_err(failed)?;
        let by_description = (0..descriptions.row_count())
            .map(|row| {
                if descriptions.flag_row(row).map_err(failed)? {
                    return Ok(Vec::new());
                }
                let id = descriptions.polarization_id(row).map_err(failed)?;
                by_polarization.get(id as usize).cloned().ok_or_else(|| {
                    FlaggingError::MutateFlags {
                        path: display_ms_path(ms),
                        reason: format!("DATA_DESCRIPTION row {row} has no polarization {id}"),
                    }
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Some(Self { by_description }))
    }

    pub(super) fn planes(&self, ddid: i32) -> &[usize] {
        self.by_description
            .get(ddid as usize)
            .map_or(&[], Vec::as_slice)
    }
}
