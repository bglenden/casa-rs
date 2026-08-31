// SPDX-License-Identifier: LGPL-3.0-or-later

//! Runtime composition of the row-bounded sequential continuum transform.

use std::{collections::BTreeMap, mem::size_of};

use casa_imaging_model::{
    CompiledProblem, ContinuumChannelUse, ContinuumFitWeightGenerationId,
    ContinuumTransformContractId, ContinuumTransformGenerationId, SelectedInputWeightGroup,
    SelectedObservationGenerationId, SelectedObservationSample, SelectedObservationSampleView,
    SelectedSpectralEvaluation, SelectedVisibilitySample, SequentialContinuumTransform,
};
use casa_imaging_reconstruction::{
    ContinuumFitError, ContinuumFitStatus, ContinuumRowInput, ContinuumSample,
    fit_and_subtract_continuum,
};
use num_complex::Complex64;
use sha2::{Digest, Sha256};
use thiserror::Error;

/// One transformed sample retaining its source spectral evaluation and role.
#[derive(Debug, Clone, PartialEq)]
pub struct ContinuumTransformedSample {
    selected: SelectedObservationSample,
    input_weight_group: SelectedInputWeightGroup,
    spectral_evaluation: SelectedSpectralEvaluation,
    use_role: ContinuumChannelUse,
    prediction: Complex64,
}

impl ContinuumTransformedSample {
    /// Return the selected sample with residual visibility and unchanged metadata.
    #[must_use]
    pub const fn selected(&self) -> &SelectedObservationSample {
        &self.selected
    }

    /// Borrow the transformed sample with its raw imaging-weight group.
    #[must_use]
    pub(crate) const fn selected_view(&self) -> SelectedObservationSampleView<'_> {
        self.selected
            .as_view()
            .with_input_weight_group(self.input_weight_group)
    }

    /// Return the source/output spectral coordinate evaluation.
    #[must_use]
    pub const fn spectral_evaluation(&self) -> SelectedSpectralEvaluation {
        self.spectral_evaluation
    }

    /// Return the channel's fit/application role.
    #[must_use]
    pub const fn use_role(&self) -> ContinuumChannelUse {
        self.use_role
    }

    /// Return the fitted continuum in reconstruction precision.
    #[must_use]
    pub const fn prediction(&self) -> Complex64 {
        self.prediction
    }
}

/// Terminal evidence for one exhaustive transformed stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinuumTransformCompletion {
    generation: ContinuumTransformGenerationId,
    contract: ContinuumTransformContractId,
    source_generation: SelectedObservationGenerationId,
    fit_weight_generation: ContinuumFitWeightGenerationId,
    sample_count: u64,
    output_sample_count: u64,
    row_count: u64,
    peak_row_samples: usize,
    peak_row_bytes: usize,
}

impl ContinuumTransformCompletion {
    /// Return the transformed-stream generation.
    #[must_use]
    pub const fn generation_id(self) -> ContinuumTransformGenerationId {
        self.generation
    }

    /// Return the canonical transform contract.
    #[must_use]
    pub const fn contract_id(self) -> ContinuumTransformContractId {
        self.contract
    }

    /// Return the raw T17 selected-observation generation.
    #[must_use]
    pub const fn source_generation(self) -> SelectedObservationGenerationId {
        self.source_generation
    }

    /// Return the ordered fit-role, flag, and effective-weight generation.
    #[must_use]
    pub const fn fit_weight_generation(self) -> ContinuumFitWeightGenerationId {
        self.fit_weight_generation
    }

    /// Return exhaustive transformed sample count.
    #[must_use]
    pub const fn sample_count(self) -> u64 {
        self.sample_count
    }

    /// Return samples whose compiled role contributes to line imaging/output.
    #[must_use]
    pub const fn output_sample_count(self) -> u64 {
        self.output_sample_count
    }

    /// Return physical MAIN row count.
    #[must_use]
    pub const fn row_count(self) -> u64 {
        self.row_count
    }

    /// Return peak one-row residency in samples.
    #[must_use]
    pub const fn peak_row_samples(self) -> usize {
        self.peak_row_samples
    }

    /// Return peak one-row adapter residency in bytes.
    #[must_use]
    pub const fn peak_row_bytes(self) -> usize {
        self.peak_row_bytes
    }
}

#[derive(Debug, Clone)]
struct PendingSample {
    selected: SelectedObservationSample,
    input_weight_group: SelectedInputWeightGroup,
    spectral_evaluation: SelectedSpectralEvaluation,
}

/// Exact one-row residency required by a compiled visibility transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinuumTransformRowPlan {
    contract: ContinuumTransformContractId,
    maximum_samples: usize,
    bytes: usize,
}

impl ContinuumTransformRowPlan {
    /// Return the maximum selected samples in one physical row.
    #[must_use]
    pub const fn maximum_samples(self) -> usize {
        self.maximum_samples
    }

    /// Return the exact preallocated adapter bytes.
    #[must_use]
    pub const fn bytes(self) -> usize {
        self.bytes
    }
}

/// Derive transform residency from the compiled DDID/SPW/polarization selection.
pub fn plan_continuum_transform_row(
    problem: &CompiledProblem,
) -> Result<Option<ContinuumTransformRowPlan>, ContinuumTransformError> {
    let Some(transform) = problem.visibility_transform() else {
        return Ok(None);
    };
    let mut maximum_samples = 0_usize;
    for source in problem.inputs().observation_snapshot().sources() {
        let selection = source.selection();
        for data_description in selection.data_descriptions() {
            let channels = selection
                .spectral_windows()
                .iter()
                .find(|selected| {
                    selected.spectral_window_id() == data_description.spectral_window_id()
                })
                .ok_or(ContinuumTransformError::InvalidSelectedShape)?
                .channel_indices()
                .len();
            let correlations = selection
                .correlations()
                .iter()
                .find(|selected| selected.polarization_id() == data_description.polarization_id())
                .ok_or(ContinuumTransformError::InvalidSelectedShape)?
                .products()
                .len();
            maximum_samples = maximum_samples.max(
                channels
                    .checked_mul(correlations)
                    .ok_or(ContinuumTransformError::Overflow)?,
            );
        }
    }
    if maximum_samples == 0 {
        return Err(ContinuumTransformError::InvalidSelectedShape);
    }
    let bytes = maximum_samples
        .checked_mul(
            size_of::<PendingSample>()
                .checked_add(size_of::<ContinuumTransformedSample>())
                .ok_or(ContinuumTransformError::Overflow)?,
        )
        .ok_or(ContinuumTransformError::Overflow)?;
    Ok(Some(ContinuumTransformRowPlan {
        contract: transform.contract_id(),
        maximum_samples,
        bytes,
    }))
}

/// One-row streaming adapter independent of physical input block boundaries.
#[derive(Debug)]
pub struct ContinuumTransformStream<'a> {
    contract: &'a SequentialContinuumTransform,
    pending_row: Option<(casa_imaging_model::MeasurementSetIdentity, u64)>,
    pending: Vec<PendingSample>,
    digest: Sha256,
    fit_weight_digest: Sha256,
    sample_count: u64,
    output_sample_count: u64,
    row_count: u64,
    peak_row_samples: usize,
    plan: ContinuumTransformRowPlan,
}

impl<'a> ContinuumTransformStream<'a> {
    /// Begin a bounded transform stream.
    pub fn new(
        contract: &'a SequentialContinuumTransform,
        plan: ContinuumTransformRowPlan,
    ) -> Result<Self, ContinuumTransformError> {
        if plan.contract != contract.contract_id() {
            return Err(ContinuumTransformError::ContractMismatch);
        }
        let mut digest = Sha256::new();
        digest.update(b"casa-rs-continuum-transform-generation");
        digest.update(1_u32.to_le_bytes());
        digest.update(contract.contract_id().as_bytes());
        let mut fit_weight_digest = Sha256::new();
        fit_weight_digest.update(b"casa-rs-continuum-fit-weight-generation");
        fit_weight_digest.update(1_u32.to_le_bytes());
        fit_weight_digest.update(contract.contract_id().as_bytes());
        Ok(Self {
            contract,
            pending_row: None,
            pending: Vec::with_capacity(plan.maximum_samples),
            digest,
            fit_weight_digest,
            sample_count: 0,
            output_sample_count: 0,
            row_count: 0,
            peak_row_samples: 0,
            plan,
        })
    }

    /// Push one canonical selected sample, returning a completed prior row if present.
    pub fn push(
        &mut self,
        selected: SelectedObservationSample,
        spectral_evaluation: SelectedSpectralEvaluation,
    ) -> Result<Vec<ContinuumTransformedSample>, ContinuumTransformError> {
        self.push_view(selected.as_view(), spectral_evaluation)
    }

    pub(crate) fn push_view(
        &mut self,
        selected: SelectedObservationSampleView<'_>,
        spectral_evaluation: SelectedSpectralEvaluation,
    ) -> Result<Vec<ContinuumTransformedSample>, ContinuumTransformError> {
        let input_weight_group = selected.input_weight_group();
        let selected = selected.to_owned();
        let row = (
            selected.address.measurement_set,
            selected.address.physical_row,
        );
        let completed = if self.pending_row.is_some_and(|pending| pending != row) {
            self.flush_row()?
        } else {
            Vec::new()
        };
        self.pending_row = Some(row);
        if self.pending.len() == self.plan.maximum_samples {
            return Err(ContinuumTransformError::RowCapacityExceeded);
        }
        self.pending.push(PendingSample {
            selected,
            input_weight_group,
            spectral_evaluation,
        });
        self.peak_row_samples = self.peak_row_samples.max(self.pending.len());
        Ok(completed)
    }

    /// Finish the last physical row.
    pub fn finish_rows(
        &mut self,
    ) -> Result<Vec<ContinuumTransformedSample>, ContinuumTransformError> {
        self.flush_row()
    }

    /// Bind the transformed content to the authoritative raw traversal generation.
    pub fn complete(
        mut self,
        source_generation: SelectedObservationGenerationId,
    ) -> Result<ContinuumTransformCompletion, ContinuumTransformError> {
        if !self.pending.is_empty() || self.pending_row.is_some() {
            return Err(ContinuumTransformError::UnflushedRow);
        }
        self.digest.update(source_generation.as_bytes());
        self.fit_weight_digest.update(source_generation.as_bytes());
        self.digest.update(self.sample_count.to_le_bytes());
        self.digest.update(self.output_sample_count.to_le_bytes());
        self.digest.update(self.row_count.to_le_bytes());
        Ok(ContinuumTransformCompletion {
            generation: ContinuumTransformGenerationId::from_owner_digest(
                self.digest.finalize().into(),
            ),
            contract: self.contract.contract_id(),
            source_generation,
            fit_weight_generation: ContinuumFitWeightGenerationId::from_owner_digest(
                self.fit_weight_digest.finalize().into(),
            ),
            sample_count: self.sample_count,
            output_sample_count: self.output_sample_count,
            row_count: self.row_count,
            peak_row_samples: self.peak_row_samples,
            peak_row_bytes: self
                .peak_row_samples
                .checked_mul(
                    size_of::<PendingSample>()
                        .checked_add(size_of::<ContinuumTransformedSample>())
                        .ok_or(ContinuumTransformError::Overflow)?,
                )
                .ok_or(ContinuumTransformError::Overflow)?,
        })
    }

    fn flush_row(&mut self) -> Result<Vec<ContinuumTransformedSample>, ContinuumTransformError> {
        if self.pending.is_empty() {
            return Ok(Vec::new());
        }
        let first = &self.pending[0].selected;
        let field = first.metadata.field_id;
        let spw = first.address.spectral_window_id;
        if self.pending.iter().any(|pending| {
            pending.selected.metadata.field_id != field
                || pending.selected.address.spectral_window_id != spw
        }) {
            return Err(ContinuumTransformError::MixedRowDomain);
        }
        let rule = self.contract.rule(field, spw);
        let mut transformed = self
            .pending
            .iter()
            .map(|pending| ContinuumTransformedSample {
                selected: pending.selected.clone(),
                input_weight_group: pending.input_weight_group,
                spectral_evaluation: pending.spectral_evaluation,
                use_role: ContinuumChannelUse::ApplyOnly,
                prediction: Complex64::new(0.0, 0.0),
            })
            .collect::<Vec<_>>();
        if let Some(rule) = rule {
            let mut correlations = BTreeMap::<u32, Vec<usize>>::new();
            for (index, pending) in self.pending.iter().enumerate() {
                correlations
                    .entry(pending.selected.address.correlation_index)
                    .or_default()
                    .push(index);
            }
            for indices in correlations.into_values() {
                let frequencies = indices
                    .iter()
                    .map(|&index| self.pending[index].selected.address.frequency_centre_hz)
                    .collect::<Vec<_>>();
                let samples = indices
                    .iter()
                    .map(|&index| {
                        let selected = &self.pending[index].selected;
                        let use_role = rule
                            .channel_use(selected.address.channel_index)
                            .ok_or(ContinuumTransformError::UndeclaredChannel)?;
                        Ok(ContinuumSample::new(
                            selected_visibility(selected.visibility),
                            selected.channel_flag
                                || selected.parallel_hand_group_flag
                                || selected.row_flag,
                            f64::from(selected.input_weight),
                            use_role,
                        ))
                    })
                    .collect::<Result<Vec<_>, ContinuumTransformError>>()?;
                let result = fit_and_subtract_continuum(ContinuumRowInput::new(
                    &frequencies,
                    &samples,
                    rule.requested_order(),
                ))?;
                encode_fit_weights(&mut self.fit_weight_digest, &samples);
                for (offset, &index) in indices.iter().enumerate() {
                    transformed[index].selected.visibility = residual_visibility(
                        transformed[index].selected.visibility,
                        result.residual()[offset],
                    );
                    transformed[index].use_role = samples[offset].use_role();
                    transformed[index].prediction = result.prediction()[offset];
                }
                encode_fit(
                    &mut self.digest,
                    result.status(),
                    result.coefficients(),
                    result.chi_squared(),
                );
            }
        }
        for output in &transformed {
            encode_output(&mut self.digest, output);
        }
        self.output_sample_count = self
            .output_sample_count
            .checked_add(
                u64::try_from(
                    transformed
                        .iter()
                        .filter(|output| output.use_role.contributes_to_output())
                        .count(),
                )
                .map_err(|_| ContinuumTransformError::Overflow)?,
            )
            .ok_or(ContinuumTransformError::Overflow)?;
        self.sample_count = self
            .sample_count
            .checked_add(
                u64::try_from(transformed.len()).map_err(|_| ContinuumTransformError::Overflow)?,
            )
            .ok_or(ContinuumTransformError::Overflow)?;
        self.row_count = self
            .row_count
            .checked_add(1)
            .ok_or(ContinuumTransformError::Overflow)?;
        self.pending.clear();
        self.pending_row = None;
        Ok(transformed)
    }
}

/// Invalid stream ordering, contract binding, or row fit.
#[derive(Debug, Error)]
pub enum ContinuumTransformError {
    /// A physical row mixed field or SPW domains.
    #[error("one physical selected-observation row mixed field or spectral-window domains")]
    MixedRowDomain,
    /// A selected channel was absent from the resolved transform contract.
    #[error("selected continuum-transform channel has no compiled fit/application role")]
    UndeclaredChannel,
    /// Reconstruction rejected the row fit.
    #[error(transparent)]
    Fit(#[from] ContinuumFitError),
    /// A bounded evidence counter overflowed.
    #[error("continuum-transform evidence counter overflowed")]
    Overflow,
    /// Completion was requested before the terminal row was flushed.
    #[error("continuum-transform completion requires the terminal row to be flushed")]
    UnflushedRow,
    /// The compiled selected-observation shape could not bound a row.
    #[error("compiled selected-observation shape cannot bound one transform row")]
    InvalidSelectedShape,
    /// A row exceeded the exact capacity claimed by the plan.
    #[error("continuum-transform row exceeded its planned sample capacity")]
    RowCapacityExceeded,
    /// A row plan from another transform contract was substituted.
    #[error("continuum-transform row plan belongs to another contract")]
    ContractMismatch,
}

fn selected_visibility(value: SelectedVisibilitySample) -> Complex64 {
    match value {
        SelectedVisibilitySample::Float32(value) => Complex64::new(f64::from(value), 0.0),
        SelectedVisibilitySample::Complex32([real, imaginary]) => {
            Complex64::new(f64::from(real), f64::from(imaginary))
        }
    }
}

fn residual_visibility(
    storage: SelectedVisibilitySample,
    value: Complex64,
) -> SelectedVisibilitySample {
    match storage {
        SelectedVisibilitySample::Float32(_) => SelectedVisibilitySample::Float32(value.re as f32),
        SelectedVisibilitySample::Complex32(_) => {
            SelectedVisibilitySample::Complex32([value.re as f32, value.im as f32])
        }
    }
}

fn encode_fit(
    digest: &mut Sha256,
    status: ContinuumFitStatus,
    coefficients: &[Complex64],
    chi_squared: [f64; 2],
) {
    match status {
        ContinuumFitStatus::Fitted { effective_order } => digest.update([0, effective_order]),
        ContinuumFitStatus::NoValidFitSamples => digest.update([1, 0]),
    }
    digest.update((coefficients.len() as u64).to_le_bytes());
    for coefficient in coefficients {
        digest.update(coefficient.re.to_bits().to_le_bytes());
        digest.update(coefficient.im.to_bits().to_le_bytes());
    }
    for value in chi_squared {
        digest.update(value.to_bits().to_le_bytes());
    }
}

fn encode_fit_weights(digest: &mut Sha256, samples: &[ContinuumSample]) {
    digest.update((samples.len() as u64).to_le_bytes());
    for sample in samples {
        digest.update([sample.flag().into()]);
        digest.update(sample.weight().to_bits().to_le_bytes());
        digest.update([match sample.use_role() {
            ContinuumChannelUse::FitOnly => 0,
            ContinuumChannelUse::ApplyOnly => 1,
            ContinuumChannelUse::FitAndApply => 2,
        }]);
    }
}

fn encode_output(digest: &mut Sha256, output: &ContinuumTransformedSample) {
    digest.update(output.selected.address.physical_row.to_le_bytes());
    digest.update(output.selected.address.channel_index.to_le_bytes());
    digest.update(output.selected.address.correlation_index.to_le_bytes());
    digest.update([match output.use_role {
        ContinuumChannelUse::FitOnly => 0,
        ContinuumChannelUse::ApplyOnly => 1,
        ContinuumChannelUse::FitAndApply => 2,
    }]);
    match output.selected.visibility {
        SelectedVisibilitySample::Float32(value) => digest.update(value.to_bits().to_le_bytes()),
        SelectedVisibilitySample::Complex32([real, imaginary]) => {
            digest.update(real.to_bits().to_le_bytes());
            digest.update(imaginary.to_bits().to_le_bytes());
        }
    }
    digest.update(output.prediction.re.to_bits().to_le_bytes());
    digest.update(output.prediction.im.to_bits().to_le_bytes());
}

#[cfg(test)]
mod tests {
    use casa_imaging_model::{
        ContinuumChannelRole, ContinuumFitRule, CorrelationType, DirectionFrame, Epoch,
        FrequencyFrame, LogicalIdentity, MeasurementSetIdentity, SelectedImageDomainProjections,
        SelectedPhaseCentreProjection, SelectedPointingDirections, SelectedPredictionTarget,
        SelectedSampleAddress, SelectedSampleCoordinates, SelectedSampleMetadata,
        SelectedSpectralInterval, SkyDirection, TimeScale, UvwCoordinateLaw,
    };

    use super::*;

    #[test]
    fn fit_only_channels_remain_in_coverage_without_becoming_output() {
        let contract = contract_for_field(0);
        let mut stream = ContinuumTransformStream::new(&contract, row_plan(&contract, 3))
            .expect("planned stream");
        let inputs = row_samples(0);
        for input in inputs {
            assert!(
                stream
                    .push(input.clone(), evaluation(&input))
                    .expect("push")
                    .is_empty(),
                "one physical row is retained until its final member"
            );
        }
        let output = stream.finish_rows().expect("finish physical row");
        assert_eq!(output.len(), 3);
        assert_eq!(output[0].use_role(), ContinuumChannelUse::FitOnly);
        assert!(!output[0].use_role().contributes_to_output());
        assert_eq!(output[1].use_role(), ContinuumChannelUse::ApplyOnly);
        assert!(output[1].use_role().contributes_to_output());
        let SelectedVisibilitySample::Complex32([real, imaginary]) =
            output[1].selected().visibility
        else {
            panic!("complex residual");
        };
        assert!((real - 7.0).abs() < 1.0e-5);
        assert!((imaginary + 2.0).abs() < 1.0e-5);
    }

    #[test]
    fn physical_block_boundaries_do_not_change_transformed_rows() {
        let contract = contract_for_field(0);
        let inputs = row_samples(0);
        let one_block = transform_partitions(&contract, &[&inputs]);
        let mid_row = transform_partitions(&contract, &[&inputs[..1], &inputs[1..]]);
        let every_sample =
            transform_partitions(&contract, &[&inputs[..1], &inputs[1..2], &inputs[2..]]);
        assert_eq!(one_block, mid_row);
        assert_eq!(one_block, every_sample);
    }

    #[test]
    fn field_without_a_rule_passes_through_with_output_role() {
        let contract = contract_for_field(0);
        let inputs = row_samples(1);
        let output = transform_partitions(&contract, &[&inputs]);
        assert_eq!(output.len(), inputs.len());
        for (actual, input) in output.iter().zip(inputs) {
            assert_eq!(actual.selected().visibility, input.visibility);
            assert_eq!(actual.prediction(), Complex64::new(0.0, 0.0));
            assert_eq!(actual.use_role(), ContinuumChannelUse::ApplyOnly);
        }
    }

    #[test]
    fn traversal_weight_group_survives_continuum_buffering() {
        let contract = contract_for_field(0);
        let input = sample(1, 0, 200.0, [1.0, -1.0]);
        let group = SelectedInputWeightGroup::parallel_hands(3.0, 7.0).with_density_owner(false);
        let mut stream = ContinuumTransformStream::new(&contract, row_plan(&contract, 1))
            .expect("planned stream");

        assert!(
            stream
                .push_view(
                    input.as_view().with_input_weight_group(group),
                    evaluation(&input),
                )
                .expect("buffer selected sample")
                .is_empty()
        );
        let output = stream.finish_rows().expect("finish selected row");

        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0].selected_view().input_weight_group().endpoints(),
            (3.0, Some(7.0))
        );
        assert!(
            !output[0]
                .selected_view()
                .input_weight_group()
                .is_density_owner()
        );
    }

    #[test]
    fn row_plan_from_another_transform_contract_is_rejected() {
        let first = contract_for_field(0);
        let second = contract_for_field(1);
        let error = ContinuumTransformStream::new(&second, row_plan(&first, 3))
            .expect_err("foreign transform row plan");
        assert!(matches!(error, ContinuumTransformError::ContractMismatch));
    }

    #[test]
    fn planned_row_bytes_bound_the_exact_transform_buffer() {
        let contract = contract_for_field(0);
        let plan = row_plan(&contract, 3);
        let mut stream = ContinuumTransformStream::new(&contract, plan).expect("planned stream");
        for input in row_samples(0) {
            stream
                .push(input.clone(), evaluation(&input))
                .expect("push row");
        }
        let output = stream.finish_rows().expect("finish planned row");

        assert_eq!(stream.pending.capacity(), plan.maximum_samples());
        assert!(
            stream.pending.capacity() * size_of::<PendingSample>()
                + output.capacity() * size_of::<ContinuumTransformedSample>()
                <= plan.bytes(),
            "resource claim must cover retained input and transformed handoff buffers"
        );
    }

    fn contract_for_field(field_id: i32) -> SequentialContinuumTransform {
        SequentialContinuumTransform::new(vec![
            ContinuumFitRule::new(
                field_id,
                0,
                1,
                vec![
                    ContinuumChannelRole::new(0, ContinuumChannelUse::FitOnly),
                    ContinuumChannelRole::new(1, ContinuumChannelUse::ApplyOnly),
                    ContinuumChannelRole::new(2, ContinuumChannelUse::FitAndApply),
                ],
            )
            .expect("resolved rule"),
        ])
        .expect("transform contract")
    }

    fn row_samples(field_id: i32) -> [SelectedObservationSample; 3] {
        [
            sample(field_id, 0, 200.0, [-1.0, -1.5]),
            sample(field_id, 1, 130.0, [10.2, -2.8]),
            sample(field_id, 2, 100.0, [5.0, -0.5]),
        ]
    }

    fn transform_partitions(
        contract: &SequentialContinuumTransform,
        partitions: &[&[SelectedObservationSample]],
    ) -> Vec<ContinuumTransformedSample> {
        let mut stream =
            ContinuumTransformStream::new(contract, row_plan(contract, 3)).expect("planned stream");
        let mut output = Vec::new();
        for partition in partitions {
            for input in *partition {
                output.extend(stream.push(input.clone(), evaluation(input)).expect("push"));
            }
        }
        output.extend(stream.finish_rows().expect("finish"));
        output
    }

    fn sample(
        field_id: i32,
        channel: u32,
        frequency_hz: f64,
        visibility: [f32; 2],
    ) -> SelectedObservationSample {
        let direction = SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5);
        SelectedObservationSample {
            address: SelectedSampleAddress {
                measurement_set: MeasurementSetIdentity::new(LogicalIdentity::from_sha256([7; 32])),
                physical_row: 4,
                data_description_id: 0,
                spectral_window_id: 0,
                channel_index: channel,
                frequency_centre_hz: frequency_hz,
                frequency_lower_hz: frequency_hz - 1.0,
                frequency_upper_hz: frequency_hz + 1.0,
                channel_width_hz: 2.0,
                frequency_frame: FrequencyFrame::Topocentric,
                polarization_id: 0,
                correlation_index: 0,
                correlation_type: CorrelationType::CircularRr,
            },
            visibility: SelectedVisibilitySample::Complex32(visibility),
            prediction_target: SelectedPredictionTarget::NotRequested,
            channel_flag: false,
            parallel_hand_group_flag: false,
            row_flag: false,
            input_weight: 1.0,
            coordinates: SelectedSampleCoordinates {
                raw_uvw_m: [1.0, 2.0, 3.0],
                density_uvw_m: [1.0, 2.0, 3.0],
                transformed_uvw_m: [1.0, 2.0, 3.0],
                phase_shift_m: 0.0,
                uvw_law: UvwCoordinateLaw::PhaseTrackingCentre,
                time: Epoch::new(59_000.0, TimeScale::Utc),
                time_centroid: Epoch::new(59_000.0, TimeScale::Utc),
                interval_seconds: 1.0,
                exposure_seconds: 1.0,
                phase_direction: direction,
                delay_direction: direction,
                pointing_directions: SelectedPointingDirections {
                    antenna1: direction,
                    antenna2: direction,
                },
            },
            domain_projections: SelectedImageDomainProjections::one_domain_with_shared_psf(
                SelectedPhaseCentreProjection::new([1.0, 2.0, 3.0], 0.0)
                    .expect("finite one-domain projection"),
            ),
            metadata: SelectedSampleMetadata {
                field_id,
                antenna1: 0,
                antenna2: 1,
                feed1: 0,
                feed2: 0,
                scan_number: 1,
                state_id: 0,
                observation_id: 0,
                array_id: 0,
            },
        }
    }

    fn row_plan(
        contract: &SequentialContinuumTransform,
        maximum_samples: usize,
    ) -> ContinuumTransformRowPlan {
        ContinuumTransformRowPlan {
            contract: contract.contract_id(),
            maximum_samples,
            bytes: maximum_samples
                * (size_of::<PendingSample>() + size_of::<ContinuumTransformedSample>()),
        }
    }

    fn evaluation(sample: &SelectedObservationSample) -> SelectedSpectralEvaluation {
        let interval = SelectedSpectralInterval::new(
            sample.address.frequency_centre_hz,
            sample.address.frequency_lower_hz,
            sample.address.frequency_upper_hz,
        )
        .expect("finite interval");
        SelectedSpectralEvaluation::new(interval, interval, 1.0, true).expect("valid evaluation")
    }
}
