// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stored correlation planes and Stokes products selected by flagging agents.

use std::collections::BTreeSet;

use crate::listobs::stokes_name;
use crate::{MeasurementSet, MsError};

use super::{FlagDataColumn, FlagDataMode, FlaggingError, display_ms_path};

// CASA checks the first named receptor product in this order before
// constructing its clipping visibility mapper for a buffer.
const RECEPTOR_PRODUCTS: [&str; 8] = ["XX", "YY", "XY", "YX", "RR", "LL", "LR", "RL"];
const STOKES_PRODUCTS: [&str; 4] = ["I", "Q", "U", "V"];

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

#[derive(Debug, Clone, Copy)]
pub(super) enum StokesCombination {
    Sum,
    Difference,
    ImaginaryDifference,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ClipProjection {
    Stored(usize),
    Stokes(usize, usize, StokesCombination),
}

#[derive(Clone)]
enum SelectedCorrelations {
    Stored(Vec<usize>),
    Clip(Vec<ClipProjection>),
}

pub(super) struct CorrelationSelection {
    by_description: Vec<SelectedCorrelations>,
    clip_metric: Option<ClipMetric>,
    unknown_clip_expression: Option<(String, Vec<bool>)>,
    decorated_stored_products: bool,
}

fn clip_visibility_operator(text: &str) -> Option<(ClipMetric, String)> {
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
}

fn stored_products_with_operator(mut text: String) -> Option<String> {
    // Manual and quack use stored flags. Keep the explicitly requested
    // planes after peeling the visibility metric from the expression.
    for (operator, _) in CLIP_OPERATORS {
        text = text.replace(operator, " ");
    }
    let mut products = Vec::new();
    for product in text.split(|ch: char| !ch.is_ascii_alphanumeric()) {
        match product {
            "ALL" => return None,
            "WVR" => products.push("I"),
            _ if RECEPTOR_PRODUCTS.contains(&product) || STOKES_PRODUCTS.contains(&product) => {
                products.push(product);
            }
            _ => {}
        }
    }
    Some(products.join(","))
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
        if mode == FlagDataMode::Clip && text.is_empty() {
            text.push_str(if data_column.is_real() {
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
        let clip_operator = (mode == FlagDataMode::Clip)
            .then(|| clip_visibility_operator(&text))
            .flatten();
        // The summary agent reads stored flags and does not evaluate
        // visibility expressions. CASA leaves its planes unfiltered here.
        if mode == FlagDataMode::Summary && has_operator {
            return Ok(None);
        }
        if mode != FlagDataMode::Summary
            && (!matches!(
                mode,
                FlagDataMode::Manual
                    | FlagDataMode::Quack
                    | FlagDataMode::Clip
                    | FlagDataMode::Elevation
            ) || (mode == FlagDataMode::Clip && has_operator && clip_operator.is_none()))
        {
            return Err(unsupported());
        }
        let decorated_stored_products = has_operator
            && matches!(
                mode,
                FlagDataMode::Manual | FlagDataMode::Quack | FlagDataMode::Elevation
            );
        if decorated_stored_products {
            let Some(products) = stored_products_with_operator(text) else {
                return Ok(None);
            };
            text = products;
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
                    && (RECEPTOR_PRODUCTS.contains(name)
                        || *name == "I"
                        || (decorated_stored_products && STOKES_PRODUCTS.contains(name)))
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
            let float_suffix = (data_column.is_real()
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
            let wvr = visibility.contains("WVR");
            let products = if !wvr
                && ((clip_operator.is_none() && required.contains("I"))
                    || modes.iter().all(|stored| stored.contains(&"I")))
            {
                // Without WVR, the mapper skips every buffer with stored I.
                Vec::new()
            } else {
                // WVR admits stored I and suppresses V. Other products can
                // still be evaluated in the same buffer.
                let stokes = if wvr {
                    &["Q", "U"][..]
                } else {
                    &STOKES_PRODUCTS
                };
                wvr.then_some("I")
                    .into_iter()
                    .chain(
                        RECEPTOR_PRODUCTS
                            .into_iter()
                            .chain(stokes.iter().copied())
                            .filter(|name| visibility.contains(*name)),
                    )
                    .collect()
            };
            if clip_operator.is_some() && products.is_empty() {
                unknown_clip_expression = Some(
                    float_suffix.map_or_else(|| text.clone(), |suffix| format!("REAL {suffix}")),
                );
            }
            Some((products, wvr))
        } else {
            None
        };
        let by_polarization: Vec<SelectedCorrelations> = modes
            .iter()
            .map(|names| {
                if !required.iter().all(|name| names.contains(name)) {
                    return SelectedCorrelations::Stored(Vec::new());
                }
                if let Some((products, wvr)) = &clip_products {
                    return SelectedCorrelations::Clip(clip_projections(products, names, *wvr));
                }
                let mut seen = BTreeSet::new();
                SelectedCorrelations::Stored(
                    names
                        .iter()
                        .enumerate()
                        .filter_map(|(plane, name)| {
                            (required.contains(name) && seen.insert(name)).then_some(plane)
                        })
                        .collect(),
                )
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
                    return Ok(SelectedCorrelations::Stored(Vec::new()));
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
            clip_metric: clip_operator
                .map(|(metric, _)| metric)
                .or((mode == FlagDataMode::Clip).then_some(ClipMetric::Amplitude)),
            unknown_clip_expression,
            decorated_stored_products,
        }))
    }

    pub(super) fn decorated_stored_products(&self) -> bool {
        self.decorated_stored_products
    }

    pub(super) fn planes(&self, ddid: i32) -> &[usize] {
        match self.by_description.get(ddid as usize) {
            Some(SelectedCorrelations::Stored(planes)) => planes,
            _ => &[],
        }
    }

    pub(super) fn has_setup(&self) -> bool {
        self.by_description.iter().any(|selection| {
            matches!(selection, SelectedCorrelations::Stored(planes) if !planes.is_empty())
        })
    }

    pub(super) fn clip_metric(&self) -> Option<ClipMetric> {
        self.clip_metric
    }

    pub(super) fn clip_projections_for(
        &self,
        ddid: i32,
        path: &str,
    ) -> Result<&[ClipProjection], FlaggingError> {
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
        Ok(match self.by_description.get(ddid as usize) {
            Some(SelectedCorrelations::Clip(projections)) => projections,
            _ => &[],
        })
    }
}

fn clip_projections(products: &[&str], stored: &[&str], wvr: bool) -> Vec<ClipProjection> {
    let Some(first) = products.first() else {
        return Vec::new();
    };
    if stored.contains(&"I") != wvr || clip_projection(first, stored).is_none() {
        return Vec::new();
    }
    products
        .iter()
        .filter_map(|name| clip_projection(name, stored))
        .collect()
}

fn clip_projection(product: &str, stored: &[&str]) -> Option<ClipProjection> {
    use StokesCombination::{Difference, ImaginaryDifference, Sum};

    // The visibility mapper uses the last plane if a label is stored twice.
    let plane = |name| stored.iter().rposition(|&stored| stored == name);
    if let Some(index) = plane(product) {
        return Some(ClipProjection::Stored(index));
    }
    let bases = match product {
        "I" => [("XX", "YY", Sum), ("RR", "LL", Sum)],
        "Q" => [("XX", "YY", Difference), ("RL", "LR", Sum)],
        "U" => [("XY", "YX", Sum), ("RL", "LR", ImaginaryDifference)],
        "V" => [("XY", "YX", ImaginaryDifference), ("RR", "LL", Difference)],
        _ => return None,
    };
    bases.into_iter().find_map(|(left, right, combination)| {
        Some(ClipProjection::Stokes(
            plane(left)?,
            plane(right)?,
            combination,
        ))
    })
}
