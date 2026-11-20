// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stored correlation planes selected by the CASA flagging agents.

use std::collections::BTreeSet;

use crate::listobs::stokes_name;
use crate::{MeasurementSet, MsError};

use super::{FlagDataColumn, FlagDataMode, FlaggingError, display_ms_path};

// CASA checks the first named receptor product in this order before
// constructing its clipping visibility mapper for a buffer.
const RECEPTOR_PRODUCTS: [&str; 8] = ["XX", "YY", "XY", "YX", "RR", "LL", "LR", "RL"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ClipMetric {
    Amplitude,
    Real,
    Imaginary,
    Phase,
    SquaredMagnitude,
}

const CLIP_OPERATORS: [(&str, ClipMetric); 5] = [
    ("REAL", ClipMetric::Real),
    ("IMAG", ClipMetric::Imaginary),
    ("ARG", ClipMetric::Phase),
    ("ABS", ClipMetric::Amplitude),
    ("NORM", ClipMetric::SquaredMagnitude),
];

pub(super) struct CorrelationSelection {
    by_description: Vec<Vec<usize>>,
    clip_metric: Option<ClipMetric>,
    unknown_clip_expression: Option<(String, Vec<bool>)>,
}

impl CorrelationSelection {
    pub(super) fn read(
        ms: &MeasurementSet,
        mode: FlagDataMode,
        data_column: FlagDataColumn,
        expression: &str,
    ) -> Result<Option<Self>, FlaggingError> {
        let mut text = expression
            .split('\0')
            .next()
            .unwrap_or_default()
            .to_uppercase();
        let default_clip = mode == FlagDataMode::Clip && text.is_empty();
        if default_clip {
            text.push_str(if data_column == FlagDataColumn::FloatData {
                "REAL_ALL"
            } else {
                "ABS_ALL"
            });
        } else if text.is_empty() {
            return Ok(None);
        }
        let unsupported = || FlaggingError::MutateFlags {
            path: display_ms_path(ms),
            reason: format!(
                "flagdata correlation {expression:?} is unimplemented in this mode (#978)"
            ),
        };
        let has_operator = CLIP_OPERATORS.iter().any(|(name, _)| text.contains(*name));
        let clip_operator = if mode == FlagDataMode::Clip {
            CLIP_OPERATORS.iter().find_map(|(name, metric)| {
                let (prefix, suffix) = text.split_once(*name)?;
                // The operator can appear on either side of the product.
                // Separate the pieces to preserve stored product boundaries.
                let products = format!("{prefix} {suffix}");
                (!CLIP_OPERATORS
                    .iter()
                    .any(|(name, _)| products.contains(*name)))
                .then_some((*metric, products))
            })
        } else {
            None
        };
        // The summary agent reads stored flags and does not evaluate
        // visibility expressions. CASA leaves its planes unfiltered here.
        if mode == FlagDataMode::Summary && has_operator {
            return Ok(None);
        }
        if mode != FlagDataMode::Summary
            && (!matches!(
                mode,
                FlagDataMode::Manual | FlagDataMode::Quack | FlagDataMode::Clip
            ) || (has_operator && clip_operator.is_none())
                || (mode != FlagDataMode::Clip && text.contains("WVR")))
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
                clip_operator.is_none()
                    && (RECEPTOR_PRODUCTS.contains(name) || *name == "I")
                    && text.contains(*name)
            })
            .collect();
        // CASA disables the clip agent when no stored product matches the
        // in-row filter, before it interprets a visibility expression.
        let mut unknown_clip_expression = None;
        let clip_products = if mode == FlagDataMode::Clip
            && (clip_operator.is_some() || !required.is_empty())
        {
            let visibility = clip_operator
                .as_ref()
                .map_or(text.as_str(), |(_, expression)| expression.as_str());
            // The REAL fallback uses the suffix after CASA's separator.
            let float_suffix = (data_column == FlagDataColumn::FloatData
                && clip_operator
                    .as_ref()
                    .is_some_and(|(metric, _)| *metric != ClipMetric::Real))
            .then(|| {
                text.split_once('_')
                    .or_else(|| text.split_once(' '))
                    .map_or("", |(_, rest)| rest)
            });
            let visibility = float_suffix.unwrap_or(visibility);
            let visibility = if visibility.contains("ALL") {
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
                visibility.to_owned()
            };
            let products = if visibility.contains("WVR") {
                wvr_clip_products(&visibility, &modes).ok_or_else(unsupported)?
            } else if (clip_operator.is_none() && required.contains("I"))
                || modes.iter().all(|stored| stored.contains(&"I"))
            {
                // Without WVR, the mapper skips every buffer with stored I.
                Vec::new()
            } else {
                if !default_clip && visibility.contains(['I', 'Q', 'U', 'V']) {
                    return Err(unsupported());
                }
                RECEPTOR_PRODUCTS
                    .into_iter()
                    .chain(["Q", "U", "V"].into_iter().filter(|_| default_clip))
                    .filter(|name| visibility.contains(*name))
                    .collect()
            };
            if clip_operator.is_some() && products.is_empty() {
                unknown_clip_expression = Some(
                    float_suffix.map_or_else(|| text.clone(), |suffix| format!("REAL {suffix}")),
                );
            }
            Some(products)
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
                let active = clip_operator.is_some()
                    || (!descriptions.flag_row(row)? && !windows.flag_row(spw as usize)?);
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
        let unknown_clip_expression = unknown_clip_expression.map(|expression| {
            // CASA skips a stored-I buffer before checking its expression.
            let report_on_description = descriptions
                .iter()
                .map(|&(id, _)| {
                    modes
                        .get(id as usize)
                        .is_none_or(|names| !names.contains(&"I"))
                })
                .collect();
            (expression, report_on_description)
        });
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
        Ok(Some(Self {
            by_description,
            clip_metric: clip_operator.map(|(metric, _)| metric),
            unknown_clip_expression,
        }))
    }

    pub(super) fn planes(&self, ddid: i32) -> &[usize] {
        self.by_description
            .get(ddid as usize)
            .map_or(&[], Vec::as_slice)
    }

    pub(super) fn has_setup(&self) -> bool {
        self.by_description.iter().any(|planes| !planes.is_empty())
    }

    pub(super) fn clip_metric(&self) -> Option<ClipMetric> {
        self.clip_metric
    }

    pub(super) fn clip_planes_for(&self, ddid: i32, path: &str) -> Result<&[usize], FlaggingError> {
        if let Some((expression, report_on_description)) = &self.unknown_clip_expression
            && report_on_description.get(ddid as usize) == Some(&true)
        {
            return Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!(
                    "Unknown polarization requested, ({expression}) supported types are: \
                     XX,YY,XY,YX,RR,LL,RL,LR,I,Q,U,V"
                ),
            });
        }
        Ok(self.planes(ddid))
    }
}

fn wvr_clip_products(visibility: &str, modes: &[Vec<&str>]) -> Option<Vec<&'static str>> {
    let derived = modes
        .iter()
        .filter(|stored| stored.contains(&"I"))
        .any(|stored| {
            let has_pair = |a, b| stored.contains(&a) && stored.contains(&b);
            (visibility.contains('Q')
                && !stored.contains(&"Q")
                && (has_pair("XX", "YY") || has_pair("RL", "LR")))
                || (visibility.contains('U')
                    && !stored.contains(&"U")
                    && (has_pair("XY", "YX") || has_pair("RL", "LR")))
        });
    if derived {
        return None;
    }
    // WVR admits a stored-I buffer. CASA's visibility mapper also adds
    // other requested stored products, with V suppressed for WVR.
    Some(
        std::iter::once("I")
            .chain(
                RECEPTOR_PRODUCTS
                    .into_iter()
                    .chain(["Q", "U"])
                    .filter(|name| visibility.contains(*name)),
            )
            .collect(),
    )
}

fn clip_planes(products: &[&str], stored: &[&str]) -> Vec<usize> {
    let Some(first) = products.first() else {
        return Vec::new();
    };
    if !stored.contains(first) || (stored.contains(&"I") && *first != "I") {
        return Vec::new();
    }
    // The visibility mapper resolves duplicate labels to the last stored
    // plane. Direct manual masks use the first occurrence.
    products
        .iter()
        .filter_map(|name| stored.iter().rposition(|stored| stored == name))
        .collect()
}
