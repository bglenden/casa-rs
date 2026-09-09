// SPDX-License-Identifier: LGPL-3.0-or-later

//! Stream complete correlation atoms using two native prediction banks.

use super::*;
use crate::weighting::{WeightingSampleValue, WeightingSelectedSample};
use std::ops::Range;

#[derive(Debug, Default)]
struct NativePredictionBank {
    records: Vec<ReducedRecordKey>,
    correlations: Vec<Range<usize>>,
}

#[derive(Debug, Default)]
pub(super) struct StandardRecordScratch {
    banks: [NativePredictionBank; 2],
    atom: Vec<ReducedRecordKey>,
    next_bank: usize,
    maximum_native_terms_per_correlation: usize,
}

impl StandardRecordScratch {
    pub(super) fn workspace_bytes(
        maximum_correlations: usize,
        maximum_native_terms_per_correlation: usize,
        maximum_atom_records: usize,
    ) -> Result<usize, SpectralOperatorError> {
        maximum_correlations
            .checked_mul(maximum_native_terms_per_correlation)
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_add(maximum_atom_records))
            .and_then(|n| n.checked_mul(size_of::<ReducedRecordKey>()))
            .and_then(|n| {
                maximum_correlations
                    .checked_mul(2)
                    .and_then(|q| q.checked_mul(size_of::<Range<usize>>()))
                    .and_then(|ranges| n.checked_add(ranges))
            })
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    }

    pub(super) fn new(
        maximum_correlations: usize,
        maximum_native_terms_per_correlation: usize,
        maximum_atom_records: usize,
    ) -> Result<Self, SpectralOperatorError> {
        Self::workspace_bytes(
            maximum_correlations,
            maximum_native_terms_per_correlation,
            maximum_atom_records,
        )?;
        let native_capacity = maximum_correlations
            .checked_mul(maximum_native_terms_per_correlation)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        Ok(Self {
            banks: [
                NativePredictionBank {
                    records: fixed_records(native_capacity)?,
                    correlations: fixed_records(maximum_correlations)?,
                },
                NativePredictionBank {
                    records: fixed_records(native_capacity)?,
                    correlations: fixed_records(maximum_correlations)?,
                },
            ],
            atom: fixed_records(maximum_atom_records)?,
            next_bank: 0,
            maximum_native_terms_per_correlation,
        })
    }
}

fn fixed_records<T>(capacity: usize) -> Result<Vec<T>, SpectralOperatorError> {
    let mut records = Vec::new();
    records
        .try_reserve_exact(capacity)
        .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
    if records.capacity() != capacity {
        return Err(SpectralOperatorError::ResidencyOverflow);
    }
    Ok(records)
}

fn push_fixed<T>(records: &mut Vec<T>, record: T) -> Result<(), SpectralOperatorError> {
    if records.len() == records.capacity() {
        return Err(SpectralOperatorError::ResidencyOverflow);
    }
    records.push(record);
    Ok(())
}

#[derive(Clone, Copy)]
struct RecordStencil {
    output_channel: usize,
    frequency_hz: f64,
    factor: f64,
    role: RecordRole,
    imaging_weight: f64,
}

struct InterpolatedPredictions {
    banks: [usize; 2],
    factors: [f64; 2],
}

impl GriddedNormalOperatorCompiler {
    pub(super) fn construct_standard_record_keys(
        &mut self,
        block: &WeightingReplayChunk,
        emit: &mut impl FnMut(&[ReducedRecordKey]) -> Result<(), SpectralOperatorError>,
    ) -> Result<GriddedNormalSourceCardinality, SpectralOperatorError> {
        let mut scratch = std::mem::take(&mut self.standard_scratch);
        let mut rows = std::mem::replace(&mut self.linear_rows, CasaLinearRowResampler::new());
        let result = (|| {
            let mut cardinality = GriddedNormalSourceCardinality::default();
            for correlations in block.correlation_groups() {
                let first = correlations
                    .first()
                    .ok_or(SpectralOperatorError::InvalidSample)?;
                let operator = self.specification.direction_independent_polarization(
                    &correlations
                        .iter()
                        .map(|sample| sample.selected().address().correlation_type)
                        .collect::<SmallVec<[_; 4]>>(),
                )?;
                let bank = scratch.next_bank;
                self.standard_predictions(
                    correlations,
                    &operator,
                    &mut scratch.banks[bank],
                    scratch.maximum_native_terms_per_correlation,
                )?;
                if self
                    .specification
                    .uses_casa_linear_resampling(correlations)?
                {
                    let observed = std::iter::repeat_n(Complex64::default(), correlations.len())
                        .collect::<SmallVec<[_; 4]>>();
                    let native = NativeSpectralGroup {
                        frequency_hz: first.selected().output_frame_frequency_hz(),
                        samples: correlations,
                        observed: &observed,
                        predicted: bank,
                    };
                    scratch.next_bank ^= 1;
                    rows.push(
                        native,
                        self.specification.casa_linear_output_grid()?,
                        self.finite_values,
                        self.specification.cube_native_weight_transfer,
                        |left, right, factors| {
                            Ok(InterpolatedPredictions {
                                banks: [*left, *right],
                                factors,
                            })
                        },
                        |resampled| {
                            self.resampled_record_groups(
                                resampled,
                                &mut scratch,
                                emit,
                                &mut cardinality,
                            )
                        },
                    )?;
                } else {
                    rows.finish()?;
                    let flags = correlations
                        .iter()
                        .map(|sample| {
                            accept_polarization_input(sample.selected(), self.finite_values)
                                .map(|accepted| !accepted)
                        })
                        .collect::<Result<SmallVec<[_; 4]>, _>>()?;
                    let flags = polarization_effective_flags(&operator, flags);
                    let columns = operator.model_coordinates().len();
                    for (row, flagged) in flags.into_iter().enumerate() {
                        if flagged {
                            continue;
                        }
                        scratch.atom.clear();
                        for record in &scratch.banks[bank].records
                            [scratch.banks[bank].correlations[row].clone()]
                        {
                            push_fixed(&mut scratch.atom, *record)?;
                        }
                        let prediction_len = scratch.atom.len();
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
                                &mut scratch.atom,
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
                        emit_atom(&mut scratch.atom, prediction_len, emit, &mut cardinality)?;
                    }
                }
            }
            Ok(cardinality)
        })();
        self.linear_rows = rows;
        self.standard_scratch = scratch;
        result
    }

    fn standard_predictions(
        &self,
        correlations: &[WeightingSampleValue],
        operator: &PolarizationOperator,
        bank: &mut NativePredictionBank,
        maximum_native_terms_per_correlation: usize,
    ) -> Result<(), SpectralOperatorError> {
        let first = correlations
            .first()
            .ok_or(SpectralOperatorError::InvalidSample)?;
        if correlations.len() > bank.correlations.capacity() {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        bank.records.clear();
        bank.correlations.clear();
        let linear = self
            .specification
            .uses_casa_linear_resampling(correlations)?;
        let linear_terms = if linear {
            self.specification.prediction_contributions(first)?
        } else {
            SmallVec::new()
        };
        for coefficients in operator
            .coefficients()
            .chunks_exact(operator.model_coordinates().len())
        {
            let start = bank.records.len();
            let mut append = |contribution: casa_imaging_model::SelectedSpectralContribution| {
                self.append_standard_stencil(
                    &mut bank.records,
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
                if bank.records.len() - start > maximum_native_terms_per_correlation {
                    return Err(SpectralOperatorError::ResidencyOverflow);
                }
                Ok(())
            };
            if linear {
                for contribution in &linear_terms {
                    append(*contribution)?;
                }
            } else {
                for spectral in first.spectral_values() {
                    append(spectral.contribution())?;
                }
            }
            push_fixed(&mut bank.correlations, start..bank.records.len())?;
        }
        Ok(())
    }

    fn resampled_record_groups(
        &self,
        resampled: CasaResampledGroup<InterpolatedPredictions>,
        scratch: &mut StandardRecordScratch,
        emit: &mut impl FnMut(&[ReducedRecordKey]) -> Result<(), SpectralOperatorError>,
        cardinality: &mut GriddedNormalSourceCardinality,
    ) -> Result<(), SpectralOperatorError> {
        let operator = self
            .specification
            .direction_independent_polarization(&resampled.correlations)?;
        let flags = polarization_effective_flags(&operator, resampled.flags);
        let columns = operator.model_coordinates().len();
        for (row, flagged) in flags.into_iter().enumerate() {
            if flagged {
                continue;
            }
            scratch.atom.clear();
            for (bank, factor) in resampled
                .predicted
                .banks
                .into_iter()
                .zip(resampled.predicted.factors)
            {
                if factor == 0.0 {
                    continue;
                }
                let bank = &scratch.banks[bank];
                let range = bank
                    .correlations
                    .get(row)
                    .ok_or(SpectralOperatorError::InvalidSample)?;
                for term in &bank.records[range.clone()] {
                    let mut term = *term;
                    term.forward_real =
                        canonical_zero_bits(f64::from_bits(term.forward_real) * factor);
                    term.forward_imaginary =
                        canonical_zero_bits(f64::from_bits(term.forward_imaginary) * factor);
                    push_fixed(&mut scratch.atom, term)?;
                }
            }
            let prediction_len = scratch.atom.len();
            self.append_standard_stencil(
                &mut scratch.atom,
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
            emit_atom(&mut scratch.atom, prediction_len, emit, cardinality)?;
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
                push_fixed(
                    records,
                    ReducedRecordKey {
                        chart_ordinal: u32::try_from(chart_ordinal)
                            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?,
                        output_channel: output_plane,
                        taps: encode_taps(taps)?,
                        forward_real: canonical_zero_bits(forward.re),
                        forward_imaginary: canonical_zero_bits(forward.im),
                        imaging_weight: canonical_zero_bits(stencil.imaging_weight),
                        role: stencil.role,
                        aw: None,
                    },
                )?;
            }
        }
        Ok(())
    }
}

fn emit_atom(
    atom: &mut Vec<ReducedRecordKey>,
    prediction_len: usize,
    emit: &mut impl FnMut(&[ReducedRecordKey]) -> Result<(), SpectralOperatorError>,
    cardinality: &mut GriddedNormalSourceCardinality,
) -> Result<(), SpectralOperatorError> {
    let accumulation_len = atom.len() - prediction_len;
    if prediction_len == 0 || accumulation_len == 0 {
        return Ok(());
    }
    let same_stencil = prediction_len == accumulation_len
        && atom[..prediction_len]
            .iter()
            .zip(&atom[prediction_len..])
            .all(|(left, right)| {
                left.chart_ordinal == right.chart_ordinal
                    && left.output_channel == right.output_channel
                    && left.taps == right.taps
                    && left.forward_real == right.forward_real
                    && left.forward_imaginary == right.forward_imaginary
            });
    if same_stencil {
        atom.copy_within(prediction_len.., 0);
        atom.truncate(accumulation_len);
        for record in atom.iter_mut() {
            record.role = RecordRole::Both;
        }
    }
    cardinality.groups = cardinality
        .groups
        .checked_add(1)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    cardinality.records = cardinality
        .records
        .checked_add(
            u64::try_from(atom.len()).map_err(|_| SpectralOperatorError::ResidencyOverflow)?,
        )
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    emit(atom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_workspace_rejects_growth_and_overflow() {
        let mut scratch = StandardRecordScratch::new(4, 6, 15).unwrap();
        let actual = scratch
            .banks
            .iter()
            .map(|bank| {
                bank.records.capacity() * size_of::<ReducedRecordKey>()
                    + bank.correlations.capacity() * size_of::<Range<usize>>()
            })
            .sum::<usize>()
            + scratch.atom.capacity() * size_of::<ReducedRecordKey>();
        assert_eq!(
            actual,
            StandardRecordScratch::workspace_bytes(4, 6, 15).unwrap()
        );
        for _ in 0..4 {
            push_fixed(&mut scratch.banks[0].correlations, 0..0).unwrap();
        }
        assert!(push_fixed(&mut scratch.banks[0].correlations, 0..0).is_err());
        assert_eq!(scratch.banks[0].correlations.capacity(), 4);
        assert!(StandardRecordScratch::workspace_bytes(usize::MAX, 2, 0).is_err());
    }

    #[test]
    fn atom_collapse_preserves_accumulation_weights_without_growth() {
        let prediction = ReducedRecordKey {
            chart_ordinal: 0,
            output_channel: 0,
            taps: 0,
            forward_real: 1.0_f64.to_bits(),
            forward_imaginary: 0,
            imaging_weight: 0,
            role: RecordRole::Prediction,
            aw: None,
        };
        let mut accumulation = prediction;
        accumulation.role = RecordRole::Accumulation;
        accumulation.imaging_weight = 3.0_f64.to_bits();
        let mut atom = fixed_records(2).unwrap();
        push_fixed(&mut atom, prediction).unwrap();
        push_fixed(&mut atom, accumulation).unwrap();
        let mut cardinality = GriddedNormalSourceCardinality::default();
        let mut calls = 0;
        emit_atom(
            &mut atom,
            1,
            &mut |records| {
                calls += 1;
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].role, RecordRole::Both);
                assert_eq!(records[0].imaging_weight, 3.0_f64.to_bits());
                Ok(())
            },
            &mut cardinality,
        )
        .unwrap();
        assert_eq!(calls, 1);
        assert_eq!(
            cardinality,
            GriddedNormalSourceCardinality {
                groups: 1,
                records: 1
            }
        );
        assert_eq!(atom.capacity(), 2);
    }
}
