// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stored correlation planes and Stokes products selected by flagging agents.

use std::collections::BTreeSet;

use casa_ms::stokes_name;
use casa_ms::{MeasurementSet, MsError};

use super::{FlagDataColumn, FlagDataMode, FlaggingError, display_ms_path};

// CASA checks the first named receptor product in this order before
// constructing its clipping visibility mapper for a buffer.
pub(super) const RECEPTOR_PRODUCTS: [&str; 8] = ["XX", "YY", "XY", "YX", "RR", "LL", "LR", "RL"];
pub(super) const STOKES_PRODUCTS: [&str; 4] = ["I", "Q", "U", "V"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ClipMetric {
    Amplitude,
    Real,
    Imaginary,
    Phase,
    SquaredMagnitude,
}

pub(super) const CLIP_OPERATORS: [(&str, ClipMetric); 5] = [
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
    // Keep the explicitly requested planes after peeling the visibility
    // metric from the expression.
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

fn stored_correlation_modes(ms: &MeasurementSet) -> Result<Vec<Vec<&'static str>>, MsError> {
    let polarization = ms.polarization()?;
    (0..polarization.row_count())
        .map(|row| {
            polarization
                .corr_type(row)
                .map(|codes| codes.into_iter().map(stokes_name).collect())
        })
        .collect()
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
        let uses_stored_products = matches!(
            mode,
            FlagDataMode::Manual
                | FlagDataMode::Quack
                | FlagDataMode::Elevation
                | FlagDataMode::Extend
                | FlagDataMode::Shadow
                | FlagDataMode::Antint
        );
        if match mode {
            FlagDataMode::Summary => false,
            FlagDataMode::Clip => has_operator && clip_operator.is_none(),
            _ => !uses_stored_products,
        } {
            return Err(unsupported());
        }
        let decorated_stored_products = has_operator && uses_stored_products;
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
        let modes = stored_correlation_modes(ms).map_err(failed)?;
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

    pub(super) fn has_active_products(&self) -> bool {
        self.unknown_clip_expression.is_some()
            || self.by_description.iter().any(|selection| match selection {
                SelectedCorrelations::Stored(planes) => !planes.is_empty(),
                SelectedCorrelations::Clip(projections) => !projections.is_empty(),
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

pub(super) struct TfcropCorrelations {
    pub(super) metric: ClipMetric,
    pub(super) restrict_extension: bool,
    by_description: Vec<Option<TfcropProducts>>,
    unknown_product: Option<String>,
}

struct TfcropProducts {
    fits: Vec<ClipProjection>,
    extension_planes: Vec<usize>,
}

impl TfcropCorrelations {
    pub(super) fn read(
        ms: &MeasurementSet,
        expression: Option<&str>,
        column: FlagDataColumn,
    ) -> Result<Self, FlaggingError> {
        let text = expression
            .unwrap_or_default()
            .split('\0')
            .next()
            .unwrap_or_default()
            .to_uppercase();
        let all = text.is_empty() || text.contains("ALL");
        let operator = CLIP_OPERATORS
            .iter()
            .find_map(|(name, metric)| text.contains(*name).then_some(*metric));
        let metric = if column.is_real() {
            ClipMetric::Real
        } else {
            operator.unwrap_or(ClipMetric::Amplitude)
        };
        let bare_all = all && !text.is_empty() && operator.is_none();
        let expression = if !all
            && column.is_real()
            && CLIP_OPERATORS[1..]
                .iter()
                .any(|(name, _)| text.contains(*name))
        {
            tracing::warn!("Unsupported visibility expression: {text}; selecting REAL by default");
            let suffix = text
                .split_once('_')
                .or_else(|| text.split_once(' '))
                .map_or("", |(_, suffix)| suffix);
            format!("REAL {suffix}")
        } else {
            text
        };
        let wvr = expression.contains("WVR");
        let failed = |source: MsError| FlaggingError::MutateFlags {
            path: display_ms_path(ms),
            reason: format!("resolve stored TFCrop correlations: {source}"),
        };
        let modes = stored_correlation_modes(ms).map_err(failed)?;
        // Each ALL agent is generated from the full polarization table
        // before a selected data description resolves its available operands.
        let mut known = BTreeSet::new();
        let global_products = modes
            .iter()
            .flatten()
            .copied()
            .filter(|name| {
                (RECEPTOR_PRODUCTS.contains(name) || STOKES_PRODUCTS.contains(name))
                    && (known.insert(*name) || *name == "I")
            })
            .collect::<Vec<_>>();
        let operands = CLIP_OPERATORS
            .iter()
            .fold(expression.clone(), |expression, (name, _)| {
                expression.replace(*name, " ")
            });
        let requested = if operator.is_some() && wvr {
            vec!["I"]
        } else if operator.is_some() {
            RECEPTOR_PRODUCTS
                .into_iter()
                .chain(STOKES_PRODUCTS)
                .filter(|name| operands.contains(*name))
                .collect()
        } else {
            RECEPTOR_PRODUCTS
                .into_iter()
                .chain(["I"])
                .filter(|name| expression.contains(*name) && known.contains(name))
                .collect()
        };
        let unknown_product =
            (!all && operator.is_some() && requested.is_empty()).then_some(expression);
        let descriptions = ms.data_description().map_err(failed)?;
        let mut active = BTreeSet::new();
        if operator.is_none() {
            let windows = ms.spectral_window().map_err(failed)?;
            for row in 0..descriptions.row_count() {
                let spw = descriptions.spectral_window_id(row).map_err(failed)?;
                if !descriptions.flag_row(row).map_err(failed)?
                    && !windows.flag_row(spw as usize).map_err(failed)?
                {
                    active.insert(descriptions.polarization_id(row).map_err(failed)?);
                }
            }
        }
        let mut by_description = Vec::with_capacity(descriptions.row_count());
        for row in 0..descriptions.row_count() {
            let id = descriptions.polarization_id(row).map_err(failed)?;
            let names = modes
                .get(id as usize)
                .ok_or_else(|| FlaggingError::MutateFlags {
                    path: display_ms_path(ms),
                    reason: format!("DATA_DESCRIPTION row {row} has no polarization {id}"),
                })?;
            if names.contains(&"I") != wvr {
                by_description.push(None);
                continue;
            }
            let fits = if all {
                global_products
                    .iter()
                    .filter(|name| {
                        !bare_all || (active.contains(&id) && RECEPTOR_PRODUCTS.contains(name))
                    })
                    .filter_map(|name| clip_projection(name, names))
                    .collect()
            } else if operator.is_some()
                || (!requested.is_empty()
                    && active.contains(&id)
                    && requested.iter().all(|name| names.contains(name)))
            {
                requested
                    .first()
                    .and_then(|name| clip_projection(name, names))
                    .into_iter()
                    .collect()
            } else {
                Vec::new()
            };
            let mut extension_planes = Vec::new();
            if !all {
                for projection in requested
                    .iter()
                    .filter_map(|name| clip_projection(name, names))
                {
                    match projection {
                        ClipProjection::Stored(plane) => extension_planes.push(plane),
                        ClipProjection::Stokes(first, second, _) => {
                            extension_planes.extend([first, second])
                        }
                    }
                }
                extension_planes.sort_unstable();
                extension_planes.dedup();
            }
            by_description.push(Some(TfcropProducts {
                fits,
                extension_planes,
            }));
        }
        Ok(Self {
            metric,
            restrict_extension: !all,
            by_description,
            unknown_product,
        })
    }

    pub(super) fn for_description(&self, ddid: i32) -> Result<&[ClipProjection], FlaggingError> {
        let products = self
            .by_description
            .get(ddid as usize)
            .and_then(Option::as_ref);
        if products.is_some()
            && let Some(text) = &self.unknown_product
        {
            return Err(FlaggingError::UnknownTfcropProduct(text.clone()));
        }
        Ok(products.map_or(&[], |products| products.fits.as_slice()))
    }

    pub(super) fn extension_planes(&self, ddid: i32) -> &[usize] {
        self.by_description
            .get(ddid as usize)
            .and_then(Option::as_ref)
            .map_or(&[], |products| products.extension_planes.as_slice())
    }

    pub(super) fn has_setup(&self) -> bool {
        self.unknown_product.is_some()
            || self
                .by_description
                .iter()
                .flatten()
                .any(|products| !products.fits.is_empty() || !products.extension_planes.is_empty())
    }
}

pub(super) struct RflagCorrelations {
    pub(super) admitted: bool,
    by_description: Vec<Option<Vec<ClipProjection>>>,
    unknown_product: Option<String>,
}

impl RflagCorrelations {
    pub(super) fn read(
        ms: &MeasurementSet,
        expression: Option<&str>,
        column: FlagDataColumn,
    ) -> Result<Self, FlaggingError> {
        let text = expression
            .unwrap_or_default()
            .split('\0')
            .next()
            .unwrap_or_default()
            .to_uppercase();
        let failed = |source: MsError| FlaggingError::MutateFlags {
            path: display_ms_path(ms),
            reason: format!("resolve stored RFlag correlations: {source}"),
        };
        let modes = stored_correlation_modes(ms).map_err(failed)?;
        let descriptions = ms.data_description().map_err(failed)?;
        let windows = ms.spectral_window().map_err(failed)?;
        let mut active = BTreeSet::new();
        for row in 0..descriptions.row_count() {
            let spw = descriptions.spectral_window_id(row).map_err(failed)?;
            if !descriptions.flag_row(row).map_err(failed)?
                && !windows.flag_row(spw as usize).map_err(failed)?
            {
                active.insert(descriptions.polarization_id(row).map_err(failed)?);
            }
        }
        let has_operator = CLIP_OPERATORS.iter().any(|(name, _)| text.contains(*name));
        let bare_matched = (!text.is_empty() && !has_operator).then(|| {
            RECEPTOR_PRODUCTS
                .into_iter()
                .chain(["I"])
                .filter(|name| text.contains(*name))
                .collect::<Vec<_>>()
        });
        if let Some(matched) = &bare_matched
            && (matched.is_empty()
                || !active.iter().any(|&id| {
                    modes
                        .get(id as usize)
                        .is_some_and(|names| matched.iter().all(|name| names.contains(name)))
                }))
        {
            tracing::warn!(
                "Cannot create agent Rflag: None of the requested correlation products ({text}) is available"
            );
            return Ok(Self {
                admitted: false,
                by_description: Vec::new(),
                unknown_product: None,
            });
        }
        let normalized = if text.is_empty() {
            if column.is_real() {
                "REAL ALL".to_string()
            } else {
                "ABS ALL".to_string()
            }
        } else if let Some(matched) = &bare_matched {
            let prefix = if column.is_real() { "REAL" } else { "ABS" };
            if text.contains("ALL") {
                format!("{prefix} ALL")
            } else if text.contains("WVR") {
                format!("{prefix} WVR,{}", matched.join(","))
            } else {
                format!("{prefix} {}", matched.join(","))
            }
        } else if column.is_real()
            && CLIP_OPERATORS[1..]
                .iter()
                .any(|(name, _)| text.contains(*name))
        {
            tracing::warn!("Unsupported visibility expression: {text}; selecting REAL by default");
            let suffix = text
                .split_once('_')
                .or_else(|| text.split_once(' '))
                .map_or("", |(_, suffix)| suffix);
            format!("REAL {suffix}")
        } else {
            text
        };
        let all = normalized.contains("ALL");
        let expanded = if all {
            let replacement = modes
                .iter()
                .flatten()
                .copied()
                .filter(|name| RECEPTOR_PRODUCTS.contains(name) || ["Q", "U", "V"].contains(name))
                .collect::<Vec<_>>()
                .join(",");
            normalized.replace("ALL", &replacement)
        } else {
            normalized.clone()
        };
        let wvr = !all && expanded.contains("WVR");
        let vis_tokens: Vec<&str> = if wvr {
            Vec::new()
        } else {
            RECEPTOR_PRODUCTS
                .into_iter()
                .chain(STOKES_PRODUCTS)
                .filter(|name| expanded.contains(*name))
                .collect()
        };
        let has_cal_token = ["SOL1", "SOL2", "SOL3", "SOL4"]
            .into_iter()
            .any(|name| expanded.contains(name));
        let unknown_product =
            (!all && !wvr && vis_tokens.is_empty() && !has_cal_token).then_some(normalized);
        let mut by_description = Vec::with_capacity(descriptions.row_count());
        for row in 0..descriptions.row_count() {
            let id = descriptions.polarization_id(row).map_err(failed)?;
            let names = modes
                .get(id as usize)
                .ok_or_else(|| FlaggingError::MutateFlags {
                    path: display_ms_path(ms),
                    reason: format!("DATA_DESCRIPTION row {row} has no polarization {id}"),
                })?;
            if let Some(matched) = &bare_matched
                && (!active.contains(&id) || !matched.iter().all(|name| names.contains(name)))
            {
                by_description.push(None);
                continue;
            }
            if names.contains(&"I") != wvr {
                by_description.push(None);
                continue;
            }
            let projections = if wvr {
                clip_projection("I", names).into_iter().collect()
            } else if vis_tokens
                .first()
                .is_some_and(|first| clip_projection(first, names).is_none())
            {
                Vec::new()
            } else {
                vis_tokens
                    .iter()
                    .filter(|name| !all || **name == "I" || names.contains(name))
                    .filter_map(|name| clip_projection(name, names))
                    .collect()
            };
            by_description.push(Some(projections));
        }
        Ok(Self {
            admitted: true,
            by_description,
            unknown_product,
        })
    }

    pub(super) fn for_description(&self, ddid: i32) -> Result<&[ClipProjection], FlaggingError> {
        let products = self
            .by_description
            .get(ddid as usize)
            .and_then(Option::as_ref);
        if products.is_some()
            && let Some(text) = &self.unknown_product
        {
            return Err(FlaggingError::UnknownTfcropProduct(text.clone()));
        }
        Ok(products.map_or(&[], Vec::as_slice))
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
