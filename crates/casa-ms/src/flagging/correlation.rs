// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stored correlation planes selected by the CASA flagging agents.

use std::collections::BTreeSet;

use crate::listobs::stokes_name;
use crate::{MeasurementSet, MsError};

use super::{FlagDataMode, FlaggingError, display_ms_path};

// CASA checks the first named receptor product in this order before
// constructing its clipping visibility mapper for a buffer.
const RECEPTOR_PRODUCTS: [&str; 8] = ["XX", "YY", "XY", "YX", "RR", "LL", "LR", "RL"];

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
        let unsupported = || FlaggingError::MutateFlags {
            path: display_ms_path(ms),
            reason: format!(
                "flagdata correlation {expression:?} is unimplemented in this mode (#978)"
            ),
        };
        let has_operator = ["ABS", "ARG", "REAL", "IMAG", "NORM"]
            .iter()
            .any(|operator| text.contains(operator));
        // The summary agent reads stored flags and does not evaluate
        // visibility expressions. CASA leaves its planes unfiltered here.
        if mode == FlagDataMode::Summary && has_operator {
            return Ok(None);
        }
        if mode != FlagDataMode::Summary
            && (!matches!(
                mode,
                FlagDataMode::Manual | FlagDataMode::Quack | FlagDataMode::Clip
            ) || has_operator
                || text.contains("WVR"))
        {
            return Err(unsupported());
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
            .filter(|name| {
                (RECEPTOR_PRODUCTS.contains(name) || *name == "I") && text.contains(*name)
            })
            .collect();
        // CASA disables the clip agent when no stored product matches the
        // in-row filter, before it interprets a visibility expression.
        let clip_products = if mode == FlagDataMode::Clip && !required.is_empty() {
            let visibility = if text.contains("ALL") {
                modes
                    .iter()
                    .flatten()
                    .copied()
                    .filter(|name| {
                        RECEPTOR_PRODUCTS.contains(name) || ["Q", "U", "V"].contains(name)
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            } else {
                text
            };
            if visibility.contains(['I', 'Q', 'U', 'V']) {
                return Err(unsupported());
            }
            Some(
                RECEPTOR_PRODUCTS
                    .into_iter()
                    .filter(|name| visibility.contains(*name))
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };
        let by_polarization: Vec<Vec<usize>> = modes
            .iter()
            .map(|names| {
                if !required.iter().all(|name| names.contains(name)) {
                    return Vec::new();
                }
                if let Some(products) = &clip_products {
                    return clip_planes(products, names);
                }
                let mut seen = BTreeSet::new();
                names
                    .iter()
                    .enumerate()
                    .filter_map(|(plane, name)| {
                        (required.contains(name) && seen.insert(name)).then_some(plane)
                    })
                    .collect()
            })
            .collect();

        let windows = ms.spectral_window().map_err(failed)?;
        let descriptions = ms.data_description().map_err(failed)?;
        let descriptions = (0..descriptions.row_count())
            .map(|row| {
                let id = descriptions.polarization_id(row)?;
                let spw = descriptions.spectral_window_id(row)?;
                let active = !descriptions.flag_row(row)? && !windows.flag_row(spw as usize)?;
                Ok((id, active))
            })
            .collect::<Result<Vec<_>, MsError>>()
            .map_err(failed)?;
        // The implicit SPW wildcard for polarization parsing ignores flagged
        // windows and descriptions when admitting a setup. The resulting mask
        // is used by polarization ID, including other descriptions of it.
        let active: BTreeSet<_> = descriptions
            .iter()
            .filter_map(|&(id, active)| active.then_some(id))
            .collect();
        let by_description = descriptions
            .into_iter()
            .enumerate()
            .map(|(row, (id, _))| {
                if !active.contains(&id) {
                    return Ok(Vec::new());
                }
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

    pub(super) fn has_setup(&self) -> bool {
        self.by_description.iter().any(|planes| !planes.is_empty())
    }
}

fn clip_planes(products: &[&str], stored: &[&str]) -> Vec<usize> {
    if stored.contains(&"I") || products.first().is_none_or(|name| !stored.contains(name)) {
        return Vec::new();
    }
    // The visibility mapper resolves duplicate labels to the last stored
    // plane. Direct manual masks use the first occurrence.
    products
        .iter()
        .filter_map(|name| stored.iter().rposition(|stored| stored == name))
        .collect()
}
