// SPDX-License-Identifier: LGPL-3.0-or-later

//! Borrowed block ingress for the existing constant-MFS numerical operator.

use super::*;
use crate::streaming_cube::input::{NativeBlockView, NativeLayout};

impl SpectralOperatorSpecification {
    /// Ordinary single-field MFS supported by the bulk numerical ingress.
    pub fn supports_bulk_mfs(&self) -> bool {
        matches!(self.basis, SpectralBasisPlan::Polynomial(plan) if plan.coefficient_term_count() == 1)
            && self.domains.len() == 1
            && self.charts.len() == 1
            && self.charts[0].window.origin() == [0, 0]
            && self.charts[0].geometry.image_shape == self.image_shape
            && self.w_projection.is_none()
            && self.aw_projection.is_none()
            && self.instrument_model.is_none()
            && !self.mosaic
    }
}

impl CompleteDataOwnerState {
    /// Consume source-owned values and derived natural weights without building
    /// weighted sample objects. Coverage is supplied only by the completed source
    /// traversal; these counters also require every row and sample to be consumed.
    pub fn consume_bulk_mfs(
        &mut self,
        block: NativeBlockView<'_>,
        layout: &NativeLayout,
    ) -> Result<(), SpectralOperatorError> {
        if !self.specification.supports_bulk_mfs()
            || self.operators.len() != 1
            || self.emit_final_visibilities
            || block.channels != layout.channels.len()
            || block.correlations != layout.correlations.len()
        {
            return Err(SpectralOperatorError::UnsupportedProblem);
        }
        let correlations = layout
            .correlations
            .iter()
            .map(|(_, c)| *c)
            .collect::<SmallVec<[_; 4]>>();
        let polarization = self
            .specification
            .direction_independent_polarization(&correlations)?;
        let predicts_residual = self
            .model_binding
            .is_some_and(ReconstructionModelBinding::is_evaluated);
        let operator = &mut self.operators[0];
        for (row, metadata) in block.metadata.iter().enumerate() {
            for channel in 0..block.channels {
                let cell = row * block.channels + channel;
                let range = cell * block.correlations..(cell + 1) * block.correlations;
                let visibilities = block.values[range.clone()]
                    .iter()
                    .map(|value| Complex64::new(f64::from(value.re), f64::from(value.im)))
                    .collect::<SmallVec<[_; 4]>>();
                let weights = block.weights[range.clone()]
                    .iter()
                    .map(|&v| f64::from(v))
                    .collect::<SmallVec<[_; 4]>>();
                let flags = polarization_effective_flags(
                    polarization,
                    block.flags[range].iter().copied().collect(),
                );
                let frequency = block.frequencies_hz[cell];
                let stencil = [SpectralOperatorSample::new(
                    0,
                    metadata.uvw_m,
                    frequency,
                    metadata.phase_shift_m,
                    [0.0; 2],
                    1.0,
                    1.0,
                )?];
                let mut model_prediction = SmallVec::<[Complex64; 4]>::new();
                for coordinate in 0..polarization.model_coordinates().len() {
                    model_prediction.push(if predicts_residual {
                        operator.predict_stencil_polarization(&stencil, coordinate)?
                    } else {
                        Complex64::default()
                    });
                }
                let predicted = polarization
                    .predict(&model_prediction)
                    .map_err(|_| SpectralOperatorError::GeneratedNonfinite)?;
                let observed_adjoint = polarization
                    .weighted_adjoint(&visibilities, &weights, &flags)
                    .map_err(|_| SpectralOperatorError::InvalidSample)?;
                let predicted_adjoint = polarization
                    .weighted_adjoint(&predicted, &weights, &flags)
                    .map_err(|_| SpectralOperatorError::InvalidSample)?;
                let diagonal = polarization_diagonal(polarization, &weights, &flags);
                let published = polarization_published_weights(polarization, &weights, &flags);
                for coordinate in 0..polarization.model_coordinates().len() {
                    let weight = diagonal[coordinate];
                    let direct = (polarization.feed_basis()
                        == crate::polarization_operator::FeedBasis::Stokes)
                        .then(|| {
                            polarization
                                .coefficients()
                                .chunks_exact(polarization.model_coordinates().len())
                                .position(|row| row[coordinate] == Complex64::new(1.0, 0.0))
                        })
                        .flatten();
                    let observed = if weight == 0.0 {
                        Complex64::default()
                    } else if let Some(row) = direct {
                        visibilities[row]
                    } else {
                        observed_adjoint[coordinate] / weight
                    };
                    let predicted = if weight == 0.0 {
                        Complex64::default()
                    } else if let Some(row) = direct {
                        predicted[row]
                    } else {
                        predicted_adjoint[coordinate] / weight
                    };
                    let sample = SpectralOperatorSample::new(
                        0,
                        metadata.uvw_m,
                        frequency,
                        metadata.phase_shift_m,
                        [observed.re, observed.im],
                        weight,
                        1.0,
                    )?
                    .with_published_weight(published[coordinate])?;
                    if predicts_residual {
                        operator.push_with_residual_polarization(sample, predicted, coordinate)?;
                    } else {
                        operator.push_polarization(sample, coordinate)?;
                    }
                }
            }
        }
        self.sample_count = self
            .sample_count
            .checked_add(block.values.len() as u64)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        self.next_block_sequence = self
            .next_block_sequence
            .checked_add(block.metadata.len() as u64)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        Ok(())
    }
}
