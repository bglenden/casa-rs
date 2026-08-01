// SPDX-License-Identifier: LGPL-3.0-or-later
//! Production-inert flexible-GCR algebra for the VLASS exact-replay race.
//!
//! The imaging controller supplies sparse real model directions and exact
//! compact-replay response vectors.  This module deliberately knows nothing
//! about how those directions are generated or how the response is evaluated.
//! In particular, it never assumes that the response operator is Hermitian or
//! positive definite.

use std::collections::{BTreeMap, BTreeSet};

use ndarray::Array2;

use crate::ImagingError;

#[derive(Debug, Clone)]
pub(crate) struct SparseModelPixel {
    pub(crate) position: (usize, usize),
    pub(crate) terms: Vec<f32>,
}

#[derive(Debug, Clone)]
pub(crate) struct ExactResponseDirection {
    pub(crate) model: Vec<SparseModelPixel>,
    pub(crate) response: Vec<f32>,
    pub(crate) response_norm_squared: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct OrthogonalizationReceipt {
    pub(crate) first_pass_coefficients: Vec<f64>,
    pub(crate) second_pass_coefficients: Vec<f64>,
    pub(crate) response_norm_before: f64,
    pub(crate) response_norm_after: f64,
}

fn invalid(message: impl Into<String>) -> ImagingError {
    ImagingError::InvalidRequest(message.into())
}

pub(crate) fn pack_full_terms(terms: &[Array2<f32>]) -> Result<Vec<f32>, ImagingError> {
    let Some(shape) = terms.first().map(Array2::dim) else {
        return Err(invalid("flexible GCR requires at least one Taylor term"));
    };
    if terms.iter().any(|term| term.dim() != shape) {
        return Err(invalid(
            "flexible GCR Taylor terms do not share one image shape",
        ));
    }
    let pixels = shape.0.saturating_mul(shape.1);
    let mut packed = Vec::with_capacity(terms.len().saturating_mul(pixels));
    for term in terms {
        packed.extend(term.iter().copied());
    }
    Ok(packed)
}

pub(crate) fn exact_response_from_residual(
    dirty: &[f32],
    direction_residual: &[f32],
) -> Result<Vec<f32>, ImagingError> {
    if dirty.len() != direction_residual.len() || dirty.is_empty() {
        return Err(invalid(
            "flexible GCR dirty and direction-residual vectors differ",
        ));
    }
    Ok(dirty
        .iter()
        .zip(direction_residual)
        .map(|(dirty, residual)| dirty - residual)
        .collect())
}

pub(crate) fn sparse_model_from_dense(
    model_terms: &[Array2<f32>],
    support: &BTreeSet<(usize, usize)>,
) -> Result<Vec<SparseModelPixel>, ImagingError> {
    let Some(shape) = model_terms.first().map(Array2::dim) else {
        return Err(invalid("flexible GCR requires at least one model term"));
    };
    if model_terms.iter().any(|term| term.dim() != shape) {
        return Err(invalid(
            "flexible GCR model terms do not share one image shape",
        ));
    }
    let mut sparse = Vec::with_capacity(support.len());
    for &position in support {
        if position.0 >= shape.0 || position.1 >= shape.1 {
            return Err(invalid(
                "flexible GCR sparse model support escapes the image shape",
            ));
        }
        let terms = model_terms
            .iter()
            .map(|term| term[position])
            .collect::<Vec<_>>();
        if terms.iter().any(|value| *value != 0.0) {
            sparse.push(SparseModelPixel { position, terms });
        }
    }
    Ok(sparse)
}

fn validate_metric(metric: &[Vec<f32>], terms: usize) -> Result<(), ImagingError> {
    if metric.len() != terms || metric.iter().any(|row| row.len() != terms) {
        return Err(invalid(
            "flexible GCR residual metric does not match the Taylor term count",
        ));
    }
    if metric.iter().flatten().any(|value| !value.is_finite()) {
        return Err(invalid(
            "flexible GCR residual metric contains a non-finite value",
        ));
    }
    Ok(())
}

pub(crate) fn response_metric_dot(
    left: &[f32],
    right: &[f32],
    positions: usize,
    metric: &[Vec<f32>],
) -> Result<f64, ImagingError> {
    if positions == 0 || left.len() != right.len() || left.len() % positions != 0 {
        return Err(invalid(
            "flexible GCR response vectors have an invalid packed shape",
        ));
    }
    let terms = left.len() / positions;
    validate_metric(metric, terms)?;
    let mut total = 0.0_f64;
    for position in 0..positions {
        for left_term in 0..terms {
            let left_value = f64::from(left[left_term * positions + position]);
            for right_term in 0..terms {
                total += left_value
                    * f64::from(metric[left_term][right_term])
                    * f64::from(right[right_term * positions + position]);
            }
        }
    }
    if !total.is_finite() {
        return Err(invalid(
            "flexible GCR response metric produced a non-finite value",
        ));
    }
    Ok(total)
}

fn response_axpy(target: &mut [f32], scale: f64, source: &[f32]) -> Result<(), ImagingError> {
    if target.len() != source.len() || !scale.is_finite() {
        return Err(invalid(
            "flexible GCR response update has incompatible inputs",
        ));
    }
    for (target, source) in target.iter_mut().zip(source) {
        *target = (f64::from(*target) + scale * f64::from(*source)) as f32;
    }
    Ok(())
}

fn sparse_model_axpy(
    target: &mut BTreeMap<(usize, usize), Vec<f64>>,
    scale: f64,
    source: &[SparseModelPixel],
    terms: usize,
) -> Result<(), ImagingError> {
    if !scale.is_finite() {
        return Err(invalid(
            "flexible GCR sparse model update scale is non-finite",
        ));
    }
    for pixel in source {
        if pixel.terms.len() != terms {
            return Err(invalid(
                "flexible GCR sparse model term counts are inconsistent",
            ));
        }
        let target_terms = target
            .entry(pixel.position)
            .or_insert_with(|| vec![0.0; terms]);
        for (target, source) in target_terms.iter_mut().zip(&pixel.terms) {
            *target += scale * f64::from(*source);
        }
    }
    Ok(())
}

fn sparse_model_map(
    model: &[SparseModelPixel],
) -> Result<BTreeMap<(usize, usize), Vec<f64>>, ImagingError> {
    let Some(terms) = model.first().map(|pixel| pixel.terms.len()) else {
        return Err(invalid(
            "flexible GCR generated an empty sparse model direction",
        ));
    };
    let mut map = BTreeMap::new();
    sparse_model_axpy(&mut map, 1.0, model, terms)?;
    Ok(map)
}

fn finish_sparse_model(
    model: BTreeMap<(usize, usize), Vec<f64>>,
) -> Result<Vec<SparseModelPixel>, ImagingError> {
    let mut sparse = Vec::with_capacity(model.len());
    for (position, values) in model {
        if values.iter().any(|value| !value.is_finite()) {
            return Err(invalid(
                "flexible GCR orthogonalized model contains a non-finite value",
            ));
        }
        let terms = values
            .into_iter()
            .map(|value| value as f32)
            .collect::<Vec<_>>();
        if terms.iter().any(|value| *value != 0.0) {
            sparse.push(SparseModelPixel { position, terms });
        }
    }
    if sparse.is_empty() {
        return Err(invalid(
            "flexible GCR orthogonalization eliminated the model direction",
        ));
    }
    Ok(sparse)
}

pub(crate) fn orthogonalize_exact_direction(
    raw_model: Vec<SparseModelPixel>,
    mut raw_response: Vec<f32>,
    previous: &[ExactResponseDirection],
    positions: usize,
    metric: &[Vec<f32>],
) -> Result<(ExactResponseDirection, OrthogonalizationReceipt), ImagingError> {
    let terms = metric.len();
    let response_norm_before =
        response_metric_dot(&raw_response, &raw_response, positions, metric)?;
    if !(response_norm_before.is_finite() && response_norm_before > 0.0) {
        return Err(invalid(
            "flexible GCR raw exact direction has zero or invalid response norm",
        ));
    }
    let mut model = sparse_model_map(&raw_model)?;
    let mut coefficients = [
        Vec::with_capacity(previous.len()),
        Vec::with_capacity(previous.len()),
    ];
    for pass_coefficients in &mut coefficients {
        for prior in previous {
            if !(prior.response_norm_squared.is_finite() && prior.response_norm_squared > 0.0) {
                return Err(invalid(
                    "flexible GCR retained an invalid prior response norm",
                ));
            }
            let coefficient =
                response_metric_dot(&prior.response, &raw_response, positions, metric)?
                    / prior.response_norm_squared;
            response_axpy(&mut raw_response, -coefficient, &prior.response)?;
            sparse_model_axpy(&mut model, -coefficient, &prior.model, terms)?;
            pass_coefficients.push(coefficient);
        }
    }
    let response_norm_after = response_metric_dot(&raw_response, &raw_response, positions, metric)?;
    if !(response_norm_after.is_finite() && response_norm_after > 0.0) {
        return Err(invalid(
            "flexible GCR orthogonalized exact direction has zero or invalid response norm",
        ));
    }
    Ok((
        ExactResponseDirection {
            model: finish_sparse_model(model)?,
            response: raw_response,
            response_norm_squared: response_norm_after,
        },
        OrthogonalizationReceipt {
            first_pass_coefficients: coefficients[0].clone(),
            second_pass_coefficients: coefficients[1].clone(),
            response_norm_before,
            response_norm_after,
        },
    ))
}

pub(crate) fn minimum_residual_step(
    direction: &ExactResponseDirection,
    residual: &[f32],
    positions: usize,
    metric: &[Vec<f32>],
) -> Result<f64, ImagingError> {
    if direction.response.len() != residual.len() {
        return Err(invalid("flexible GCR residual and response vectors differ"));
    }
    let alpha = response_metric_dot(&direction.response, residual, positions, metric)?
        / direction.response_norm_squared;
    if !alpha.is_finite() {
        return Err(invalid(
            "flexible GCR minimum-residual coefficient is non-finite",
        ));
    }
    Ok(alpha)
}

pub(crate) fn apply_model_direction(
    model_terms: &mut [Array2<f32>],
    support: &mut BTreeSet<(usize, usize)>,
    direction: &[SparseModelPixel],
    alpha: f64,
) -> Result<(), ImagingError> {
    let Some(shape) = model_terms.first().map(Array2::dim) else {
        return Err(invalid("flexible GCR has no model terms to update"));
    };
    if !alpha.is_finite() || model_terms.iter().any(|term| term.dim() != shape) {
        return Err(invalid("flexible GCR model update has invalid inputs"));
    }
    for pixel in direction {
        if pixel.position.0 >= shape.0
            || pixel.position.1 >= shape.1
            || pixel.terms.len() != model_terms.len()
        {
            return Err(invalid(
                "flexible GCR sparse direction does not match the model",
            ));
        }
        for (term, value) in model_terms.iter_mut().zip(&pixel.terms) {
            term[pixel.position] =
                (f64::from(term[pixel.position]) + alpha * f64::from(*value)) as f32;
        }
        support.insert(pixel.position);
    }
    Ok(())
}

pub(crate) fn apply_full_response_direction(
    residual_terms: &mut [Array2<f32>],
    response: &[f32],
    alpha: f64,
) -> Result<(), ImagingError> {
    let Some(shape) = residual_terms.first().map(Array2::dim) else {
        return Err(invalid("flexible GCR has no residual terms to update"));
    };
    if residual_terms.iter().any(|term| term.dim() != shape) {
        return Err(invalid(
            "flexible GCR residual terms do not share one image shape",
        ));
    }
    let pixels = shape.0.saturating_mul(shape.1);
    let expected = residual_terms.len().saturating_mul(pixels);
    if response.len() != expected || !alpha.is_finite() {
        return Err(invalid(
            "flexible GCR response update does not match the residual domain",
        ));
    }
    for (term_index, term) in residual_terms.iter_mut().enumerate() {
        let offset = term_index * pixels;
        for (value, direction) in term
            .iter_mut()
            .zip(&response[offset..offset.saturating_add(pixels)])
        {
            *value = (f64::from(*value) - alpha * f64::from(*direction)) as f32;
        }
    }
    Ok(())
}

pub(crate) fn relative_metric_error(
    candidate: &[f32],
    reference: &[f32],
    positions: usize,
    metric: &[Vec<f32>],
) -> Result<f64, ImagingError> {
    if candidate.len() != reference.len() {
        return Err(invalid("flexible GCR validation vectors differ in length"));
    }
    let difference = candidate
        .iter()
        .zip(reference)
        .map(|(candidate, reference)| candidate - reference)
        .collect::<Vec<_>>();
    let numerator = response_metric_dot(&difference, &difference, positions, metric)?;
    let denominator = response_metric_dot(reference, reference, positions, metric)?;
    if !(denominator.is_finite() && denominator > 0.0 && numerator >= 0.0) {
        return Err(invalid("flexible GCR validation metric is invalid"));
    }
    Ok((numerator / denominator).sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity_metric() -> Vec<Vec<f32>> {
        vec![vec![1.0, 0.0], vec![0.0, 1.0]]
    }

    #[test]
    fn response_space_orthogonalization_preserves_model_response_pairing() {
        let positions = 2;
        let metric = identity_metric();
        let first = ExactResponseDirection {
            model: vec![SparseModelPixel {
                position: (0, 0),
                terms: vec![1.0, 0.0],
            }],
            response: vec![1.0, 0.0, 0.0, 0.0],
            response_norm_squared: 1.0,
        };
        let raw_model = vec![
            SparseModelPixel {
                position: (0, 0),
                terms: vec![2.0, 0.0],
            },
            SparseModelPixel {
                position: (1, 0),
                terms: vec![1.0, 1.0],
            },
        ];
        let raw_response = vec![2.0, 1.0, 0.0, 1.0];
        let (direction, receipt) =
            orthogonalize_exact_direction(raw_model, raw_response, &[first], positions, &metric)
                .unwrap();
        assert_eq!(receipt.first_pass_coefficients, vec![2.0]);
        assert_eq!(receipt.second_pass_coefficients, vec![0.0]);
        assert_eq!(direction.response, vec![0.0, 1.0, 0.0, 1.0]);
        assert_eq!(direction.model.len(), 1);
        assert_eq!(direction.model[0].position, (1, 0));
        assert_eq!(direction.model[0].terms, vec![1.0, 1.0]);
        assert_eq!(direction.response_norm_squared, 2.0);
    }

    #[test]
    fn minimum_residual_step_decreases_a_nonhermitian_response_objective() {
        // H = [[1, 2], [0, 1]] is deliberately non-Hermitian.  The solver
        // receives only z and q=Hz and therefore makes no symmetry assumption.
        let positions = 1;
        let metric = identity_metric();
        let direction = ExactResponseDirection {
            model: vec![SparseModelPixel {
                position: (0, 0),
                terms: vec![1.0, 0.0],
            }],
            response: vec![1.0, 0.0],
            response_norm_squared: 1.0,
        };
        let residual = vec![3.0, 4.0];
        let before = response_metric_dot(&residual, &residual, positions, &metric).unwrap();
        let alpha = minimum_residual_step(&direction, &residual, positions, &metric).unwrap();
        assert_eq!(alpha, 3.0);
        let after_residual = vec![
            residual[0] - alpha as f32 * direction.response[0],
            residual[1] - alpha as f32 * direction.response[1],
        ];
        let after =
            response_metric_dot(&after_residual, &after_residual, positions, &metric).unwrap();
        assert!(after < before);
        assert_eq!(after, 16.0);
    }

    #[test]
    fn exact_response_is_dirty_minus_direction_residual() {
        assert_eq!(
            exact_response_from_residual(&[4.0, -2.0], &[1.5, -3.0]).unwrap(),
            vec![2.5, 1.0]
        );
    }

    #[test]
    fn full_response_pack_and_update_cover_every_pixel_and_term() {
        let mut terms = vec![
            Array2::from_shape_vec((2, 2), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
            Array2::from_shape_vec((2, 2), vec![5.0, 6.0, 7.0, 8.0]).unwrap(),
        ];
        assert_eq!(
            pack_full_terms(&terms).unwrap(),
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        );
        apply_full_response_direction(&mut terms, &[0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0], 2.0)
            .unwrap();
        assert_eq!(
            pack_full_terms(&terms).unwrap(),
            vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        );
    }

    #[test]
    fn packed_validation_error_uses_the_configured_metric() {
        let metric = vec![vec![2.0, 0.0], vec![0.0, 0.5]];
        let reference = vec![2.0, 0.0, 0.0, 2.0];
        let candidate = vec![2.0, 0.0, 0.0, 1.0];
        let relative = relative_metric_error(&candidate, &reference, 2, &metric).unwrap();
        assert!((relative - (0.5_f64 / 10.0_f64).sqrt()).abs() < 1.0e-12);
    }
}
