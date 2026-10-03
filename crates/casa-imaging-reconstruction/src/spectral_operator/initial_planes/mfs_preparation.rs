// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pure initial MFS correlation-group preparation before ordered region append.

use super::*;
use std::ops::Range;

pub(super) struct PreparationWork<'a> {
    pub(super) specification: &'a SpectralOperatorSpecification,
    pub(super) finite_values: FiniteValuePolicy,
    pub(super) values: &'a [crate::weighting::WeightingSampleValue],
    pub(super) groups: &'a [Range<usize>],
    pub(super) samples: &'a mut [Option<SpectralOperatorSample>],
    pub(super) executed: bool,
    pub(super) completed: bool,
}

impl PreparationWork<'_> {
    pub(super) fn execute(&mut self) -> Result<(), SpectralOperatorError> {
        if self.executed {
            return Err(SpectralOperatorError::BlockSequence);
        }
        self.executed = true;
        for (group, sample) in self.groups.iter().zip(self.samples.iter_mut()) {
            *sample = prepare_group(
                self.specification,
                &self.values[group.clone()],
                self.finite_values,
            )?;
        }
        self.completed = true;
        Ok(())
    }
}

fn prepare_group(
    specification: &SpectralOperatorSpecification,
    group: &[crate::weighting::WeightingSampleValue],
    finite_values: FiniteValuePolicy,
) -> Result<Option<SpectralOperatorSample>, SpectralOperatorError> {
    let first = group.first().ok_or(SpectralOperatorError::InvalidSample)?;
    let selected = first.selected();
    let CorrelationInputs {
        polarization,
        visibilities,
        flags,
    } = correlation_inputs(specification, group, finite_values)?;
    let chart = &specification.charts[0];
    let (uvw, phase) = selected_model_projection(
        selected,
        specification.chart_count(),
        chart.domain_ordinal,
        chart.facet_ordinal,
    )?;
    match first.spectral_values().count() {
        0 => return Ok(None),
        1 => {}
        _ => return Err(SpectralOperatorError::InvalidSample),
    }
    let (spectral, weights) = correlation_spectral_weights(group, 0)?;
    let published = polarization_published_weights(polarization, &weights, &flags);
    let adjoint = polarization
        .weighted_adjoint(&visibilities, &weights, &flags)
        .map_err(|_| SpectralOperatorError::InvalidSample)?;
    let diagonal = polarization_diagonal(polarization, &weights, &flags);
    let weight = diagonal[0];
    let direct = (polarization.feed_basis() == crate::polarization_operator::FeedBasis::Stokes)
        .then(|| {
            polarization
                .coefficients()
                .iter()
                .position(|value| *value == Complex64::new(1.0, 0.0))
        })
        .flatten();
    let observed = if weight == 0.0 {
        Complex64::default()
    } else if let Some(row) = direct {
        visibilities[row]
    } else {
        adjoint[0] / weight
    };
    let contribution = spectral.contribution();
    Ok(Some(
        SpectralOperatorSample::new(
            usize::try_from(contribution.output_channel())
                .map_err(|_| SpectralOperatorError::InvalidSample)?,
            uvw,
            contribution.evaluation_frequency_hz(),
            phase,
            [observed.re, observed.im],
            weight,
            contribution.factor(),
        )?
        .with_mosaic_route(selected.field_id(), selected.pointing_directions())
        .with_published_weight(published[0])?,
    ))
}
