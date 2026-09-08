// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compile the standard operator's complete correlation predictions separately
//! from its weighted accumulation stencil, including CASA row interpolation.

use super::*;
use crate::{
    spectral_operator::direction_independent_polarization,
    weighting::{WeightingSampleValue, WeightingSelectedSample},
};

type CorrelationPredictions = SmallVec<[Vec<ReducedRecordKey>; 4]>;

#[derive(Clone, Copy)]
struct RecordStencil {
    output_channel: usize,
    frequency_hz: f64,
    factor: f64,
    role: RecordRole,
    imaging_weight: f64,
}

impl GriddedNormalOperatorCompiler {
    pub(super) fn construct_standard_record_keys(
        &mut self,
        block: &WeightingReplayChunk,
    ) -> Result<
        (
            Vec<Vec<ReducedRecordKey>>,
            GriddedNormalOperatorBlockMeasurements,
        ),
        SpectralOperatorError,
    > {
        let mut groups = Vec::new();
        let mut measurements = GriddedNormalOperatorBlockMeasurements::default();
        for correlations in block.correlation_groups() {
            let first = correlations
                .first()
                .ok_or(SpectralOperatorError::InvalidSample)?;
            let operator = direction_independent_polarization(
                self.specification.polarization_coordinates(),
                &correlations
                    .iter()
                    .map(|sample| sample.selected().address().correlation_type)
                    .collect::<SmallVec<[_; 4]>>(),
            )?;
            let predicted = self.standard_predictions(correlations, &operator)?;
            if self
                .specification
                .uses_casa_linear_resampling(correlations)?
            {
                let native = NativeSpectralGroup {
                    key: NativeSpectralRowKey::from_sample(first.selected()),
                    frequency_hz: first.selected().output_frame_frequency_hz(),
                    samples: correlations.iter().cloned().collect(),
                    // Observations remain exclusively in the initial normal state.
                    observed: std::iter::repeat_n(Complex64::default(), correlations.len())
                        .collect(),
                    predicted,
                };
                let output = self.specification.casa_linear_output_grid()?;
                let mut rows =
                    std::mem::replace(&mut self.linear_rows, CasaLinearRowResampler::new());
                let result = rows.push(
                    native,
                    output,
                    self.finite_values,
                    interpolate_predictions,
                    |resampled| {
                        self.resampled_record_groups(resampled, &mut groups, &mut measurements)
                    },
                );
                self.linear_rows = rows;
                result?;
            } else {
                let flags = correlations
                    .iter()
                    .map(|sample| {
                        accept_polarization_input(sample.selected(), self.finite_values)
                            .map(|accepted| !accepted)
                    })
                    .collect::<Result<SmallVec<[_; 4]>, _>>()?;
                let flags = polarization_effective_flags(&operator, flags);
                let columns = operator.model_coordinates().len();
                for (row, prediction) in predicted.into_iter().enumerate() {
                    if flags[row] {
                        continue;
                    }
                    let mut accumulation = Vec::new();
                    for (ordinal, spectral) in correlations[row].spectral_values().enumerate() {
                        let contribution = spectral.contribution();
                        if first
                            .spectral_values()
                            .nth(ordinal)
                            .map(|value| value.contribution())
                            != Some(contribution)
                        {
                            return Err(SpectralOperatorError::InvalidSample);
                        }
                        self.append_standard_stencil(
                            &mut accumulation,
                            first.selected(),
                            RecordStencil {
                                output_channel: usize::try_from(contribution.output_channel())
                                    .map_err(|_| SpectralOperatorError::InvalidSample)?,
                                frequency_hz: contribution.evaluation_frequency_hz(),
                                factor: contribution.factor(),
                                role: RecordRole::Accumulation,
                                imaging_weight: spectral.imaging_weight(),
                            },
                            &operator.coefficients()[row * columns..(row + 1) * columns],
                        )?;
                    }
                    append_group(prediction, accumulation, &mut groups, &mut measurements)?;
                }
            }
        }
        Ok((groups, measurements))
    }

    fn standard_predictions(
        &self,
        correlations: &[WeightingSampleValue],
        operator: &PolarizationOperator,
    ) -> Result<CorrelationPredictions, SpectralOperatorError> {
        let first = correlations
            .first()
            .ok_or(SpectralOperatorError::InvalidSample)?;
        operator
            .coefficients()
            .chunks_exact(operator.model_coordinates().len())
            .map(|coefficients| {
                let mut records = Vec::new();
                for contribution in self.specification.prediction_contributions(first)? {
                    self.append_standard_stencil(
                        &mut records,
                        first.selected(),
                        RecordStencil {
                            output_channel: usize::try_from(contribution.output_channel())
                                .map_err(|_| SpectralOperatorError::InvalidSample)?,
                            frequency_hz: contribution.evaluation_frequency_hz(),
                            factor: contribution.factor(),
                            role: RecordRole::Prediction,
                            imaging_weight: 0.0,
                        },
                        coefficients,
                    )?;
                }
                Ok(records)
            })
            .collect()
    }

    fn resampled_record_groups(
        &self,
        resampled: CasaResampledGroup<CorrelationPredictions>,
        groups: &mut Vec<Vec<ReducedRecordKey>>,
        measurements: &mut GriddedNormalOperatorBlockMeasurements,
    ) -> Result<(), SpectralOperatorError> {
        let operator = direction_independent_polarization(
            self.specification.polarization_coordinates(),
            &resampled.correlations,
        )?;
        let flags = polarization_effective_flags(&operator, resampled.flags);
        let columns = operator.model_coordinates().len();
        for (row, prediction) in resampled.predicted.into_iter().enumerate() {
            if flags[row] {
                continue;
            }
            let mut accumulation = Vec::new();
            self.append_standard_stencil(
                &mut accumulation,
                &resampled.selected,
                RecordStencil {
                    output_channel: resampled.output_channel,
                    frequency_hz: resampled.frequency_hz,
                    factor: 1.0,
                    role: RecordRole::Accumulation,
                    imaging_weight: resampled.weights[row],
                },
                &operator.coefficients()[row * columns..(row + 1) * columns],
            )?;
            append_group(prediction, accumulation, groups, measurements)?;
        }
        Ok(())
    }

    fn append_standard_stencil(
        &self,
        records: &mut Vec<ReducedRecordKey>,
        selected: &WeightingSelectedSample,
        stencil: RecordStencil,
        coefficients: &[Complex64],
    ) -> Result<(), SpectralOperatorError> {
        if stencil.role == RecordRole::Accumulation && stencil.imaging_weight == 0.0 {
            return Ok(());
        }
        if stencil.output_channel >= self.specification.slab().total_channels()
            || !stencil.frequency_hz.is_finite()
            || stencil.frequency_hz <= 0.0
            || !stencil.factor.is_finite()
            || stencil.factor == 0.0
        {
            return Err(SpectralOperatorError::InvalidSample);
        }
        for (chart_ordinal, chart) in self.specification.charts().iter().enumerate() {
            let (uvw_m, phase_shift_m) = selected_model_projection(
                selected,
                self.specification.chart_count(),
                chart.domain_ordinal(),
                chart.facet_ordinal(),
            )?;
            let scale = stencil.frequency_hz / SPEED_OF_LIGHT_M_PER_S;
            let Some(taps) = self.gridders[chart_ordinal].taps(uvw_m.map(|value| value * scale))
            else {
                continue;
            };
            let phase = std::f64::consts::TAU * phase_shift_m * scale;
            for (polarization, coefficient) in coefficients.iter().copied().enumerate() {
                if coefficient == Complex64::default() {
                    continue;
                }
                let forward = Complex64::from_polar(stencil.factor, -phase) * coefficient;
                let output_plane = stencil
                    .output_channel
                    .checked_mul(self.specification.polarization_count())
                    .and_then(|value| value.checked_add(polarization))
                    .and_then(|value| u32::try_from(value).ok())
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                records.push(ReducedRecordKey {
                    chart_ordinal: u32::try_from(chart_ordinal)
                        .map_err(|_| SpectralOperatorError::ResidencyOverflow)?,
                    output_channel: output_plane,
                    taps: encode_taps(taps)?,
                    forward_real: canonical_zero_bits(forward.re),
                    forward_imaginary: canonical_zero_bits(forward.im),
                    imaging_weight: canonical_zero_bits(stencil.imaging_weight),
                    role: stencil.role,
                    aw: None,
                });
            }
        }
        Ok(())
    }
}

fn interpolate_predictions(
    left: &CorrelationPredictions,
    right: &CorrelationPredictions,
    factors: [f64; 2],
) -> Result<CorrelationPredictions, SpectralOperatorError> {
    if left.len() != right.len() {
        return Err(SpectralOperatorError::InvalidSample);
    }
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let mut records = Vec::with_capacity(left.len() + right.len());
            for (parent, factor) in [(left, factors[0]), (right, factors[1])] {
                if factor == 0.0 {
                    continue;
                }
                for term in parent {
                    let mut term = term.clone();
                    term.forward_real =
                        canonical_zero_bits(f64::from_bits(term.forward_real) * factor);
                    term.forward_imaginary =
                        canonical_zero_bits(f64::from_bits(term.forward_imaginary) * factor);
                    records.push(term);
                }
            }
            Ok(records)
        })
        .collect()
}

fn append_group(
    prediction: Vec<ReducedRecordKey>,
    accumulation: Vec<ReducedRecordKey>,
    groups: &mut Vec<Vec<ReducedRecordKey>>,
    measurements: &mut GriddedNormalOperatorBlockMeasurements,
) -> Result<(), SpectralOperatorError> {
    if accumulation.is_empty() || prediction.is_empty() {
        return Ok(());
    }
    let same_stencil = prediction.len() == accumulation.len()
        && prediction.iter().zip(&accumulation).all(|(left, right)| {
            left.chart_ordinal == right.chart_ordinal
                && left.output_channel == right.output_channel
                && left.taps == right.taps
                && left.forward_real == right.forward_real
                && left.forward_imaginary == right.forward_imaginary
        });
    let capacity = if same_stencil {
        prediction.len()
    } else {
        prediction.len() + accumulation.len()
    };
    let mut group = Vec::with_capacity(capacity);
    if same_stencil {
        group.extend(accumulation.into_iter().map(|mut record| {
            record.role = RecordRole::Both;
            record
        }));
    } else {
        group.extend(prediction);
        group.extend(accumulation);
    }
    record_vector_growth(
        0,
        group.capacity(),
        size_of::<ReducedRecordKey>(),
        &mut measurements.source_group_vector_allocations,
        &mut measurements.source_group_capacity_growth_bytes,
    )?;
    groups.push(group);
    Ok(())
}
