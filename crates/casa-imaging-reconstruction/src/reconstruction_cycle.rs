// SPDX-License-Identifier: LGPL-3.0-or-later

//! Shared continuum and channel-local reconstruction-cycle orchestration.

use std::fmt;

use casa_imaging_model::{CompiledProblemId, LogicalIdentity, ModelCell, ModelDeltaTerm};
use thiserror::Error;

use crate::{
    ComponentDivergence, Encoder, FinalNormalState, MinorCycleError, MinorCycleEvidence,
    MinorCycleModelPlane, MinorCycleProgram, MinorCycleStopReason, ModelDelta, ModelGeneration,
    ModelLifecycle, ModelLifecycleError, ModelSupport, SpectralChannelValidity,
    minor_cycle::{
        run_image_domain_minor_cycle, run_joint_minor_cycle, run_minor_cycle, run_minor_cycle_plane,
    },
};

const RECONSTRUCTION_CYCLE_EVIDENCE_DOMAIN: &[u8] = b"casa-rs-reconstruction-cycle-evidence";
const RECONSTRUCTION_CYCLE_EVIDENCE_VERSION: u32 = 4;

/// Stable identity of one ordered continuum or channel-local solve.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReconstructionCycleEvidenceId(LogicalIdentity);

impl ReconstructionCycleEvidenceId {
    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for ReconstructionCycleEvidenceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReconstructionCycleEvidenceId(")?;
        crate::write_hex(formatter, &self.as_bytes())?;
        formatter.write_str(")")
    }
}

/// Scientific coupling admitted by the channel-local cycle owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelCyclePolicy {
    /// Each channel is solved against its own normal-state plane and stopping evidence.
    Independent,
    /// Solve one declared coupled Taylor block as a shared spatial/scale selection.
    ///
    /// Non-Taylor normal-state catalogs fail typed rather than silently
    /// running independent channel cycles.
    Coupled,
}

/// One channel's ordered result within a shared reconstruction cycle.
#[derive(Debug)]
pub struct ChannelCycleEvidence {
    output_channel: usize,
    polarization: usize,
    validity: SpectralChannelValidity,
    minor_cycle: Option<MinorCycleEvidence>,
}

impl ChannelCycleEvidence {
    /// Return the absolute output-channel ordinal.
    #[must_use]
    pub const fn output_channel(&self) -> usize {
        self.output_channel
    }

    /// Return the compiled polarization ordinal solved for this channel.
    #[must_use]
    pub const fn polarization(&self) -> usize {
        self.polarization
    }

    /// Return the normal-state validity of this channel.
    #[must_use]
    pub const fn validity(&self) -> SpectralChannelValidity {
        self.validity
    }

    /// Return minor-cycle evidence for a valid channel.
    ///
    /// Blank and unmapped channels are represented explicitly and never
    /// manufacture a solver stop or model update.
    #[must_use]
    pub const fn minor_cycle(&self) -> Option<&MinorCycleEvidence> {
        self.minor_cycle.as_ref()
    }
}

/// First component-sequence divergence in deterministic channel order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChannelComponentDivergence {
    output_channel: usize,
    component: ComponentDivergence,
}

impl ChannelComponentDivergence {
    /// Return the first channel whose component sequence differs.
    #[must_use]
    pub const fn output_channel(self) -> usize {
        self.output_channel
    }

    /// Return the first component mismatch within that channel.
    #[must_use]
    pub const fn component(self) -> ComponentDivergence {
        self.component
    }
}

/// Ordered owner evidence for one shared reconstruction cycle.
#[derive(Debug)]
pub struct ReconstructionCycleEvidence {
    evidence_id: ReconstructionCycleEvidenceId,
    problem: CompiledProblemId,
    policy: ChannelCyclePolicy,
    channels: Box<[ChannelCycleEvidence]>,
}

impl ReconstructionCycleEvidence {
    /// Return the stable ordered-cycle identity.
    #[must_use]
    pub const fn evidence_id(&self) -> ReconstructionCycleEvidenceId {
        self.evidence_id
    }

    /// Return the compiled problem this cycle consumed.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem
    }

    /// Return the explicit channel-coupling policy.
    #[must_use]
    pub const fn channel_policy(&self) -> ChannelCyclePolicy {
        self.policy
    }

    /// Return all channel outcomes in absolute output-channel order.
    #[must_use]
    pub const fn channels(&self) -> &[ChannelCycleEvidence] {
        &self.channels
    }

    /// Return the total number of accepted component updates.
    #[must_use]
    pub fn iterations(&self) -> usize {
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .map(MinorCycleEvidence::iterations)
            .sum()
    }

    /// Return the component count charged to the reported controller budget.
    #[must_use]
    pub fn controller_iterations(&self) -> usize {
        let polarizations = self
            .channels
            .iter()
            .map(ChannelCycleEvidence::polarization)
            .max()
            .map_or(0, |maximum| maximum + 1);
        let mut totals = vec![0_usize; polarizations];
        for channel in &self.channels {
            if let Some(evidence) = channel.minor_cycle() {
                totals[channel.polarization] =
                    totals[channel.polarization].saturating_add(evidence.controller_iterations());
            }
        }
        totals.into_iter().max().unwrap_or(0)
    }

    /// Return cumulative absolute accepted component flux across channels.
    #[must_use]
    pub fn total_flux(&self) -> f64 {
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .map(MinorCycleEvidence::total_flux)
            .sum()
    }

    /// Return the maximum normalized entry peak across valid channels.
    #[must_use]
    pub fn initial_peak_flux(&self) -> f64 {
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .map(MinorCycleEvidence::initial_peak_flux)
            .fold(0.0, f64::max)
    }

    /// Return the maximum final normalized peak across valid channels.
    #[must_use]
    pub fn final_peak_flux(&self) -> f64 {
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .map(MinorCycleEvidence::final_peak_flux)
            .fold(0.0, f64::max)
    }

    /// Return the maximum robust RMS used by any channel, when enabled.
    #[must_use]
    pub fn noise_rms(&self) -> Option<f64> {
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .filter_map(MinorCycleEvidence::noise_rms)
            .reduce(f64::max)
    }

    /// Return the maximum effective threshold across valid channels.
    #[must_use]
    pub fn effective_threshold(&self) -> f64 {
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .map(MinorCycleEvidence::effective_threshold)
            .fold(0.0, f64::max)
    }

    /// Return the maximum global absolute/noise threshold across valid channels.
    #[must_use]
    pub fn global_threshold(&self) -> f64 {
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .map(MinorCycleEvidence::global_threshold)
            .fold(0.0, f64::max)
    }

    /// Return the maximum PSF-derived cycle threshold, when configured.
    #[must_use]
    pub fn cycle_threshold(&self) -> Option<f64> {
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .filter_map(MinorCycleEvidence::cycle_threshold)
            .reduce(f64::max)
    }

    /// Return a deterministic aggregate stop reason for existing one-plane consumers.
    ///
    /// Detailed cube consumers must inspect [`Self::channels`]. The aggregate
    /// selects the first non-threshold stop in channel order.
    #[must_use]
    pub fn stop_reason(&self) -> MinorCycleStopReason {
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .map(MinorCycleEvidence::stop_reason)
            .find(|reason| !matches!(reason, MinorCycleStopReason::ThresholdReached))
            .unwrap_or(MinorCycleStopReason::ThresholdReached)
    }

    /// Return the total exact Clark refresh count across channels.
    #[must_use]
    pub fn clark_refreshes(&self) -> usize {
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .map(MinorCycleEvidence::clark_refreshes)
            .sum()
    }

    /// Whether any valid channel requires complete-data reconciliation.
    #[must_use]
    pub fn requests_reconciliation(&self) -> bool {
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .any(MinorCycleEvidence::requests_reconciliation)
    }

    /// Whether every valid channel's cycle threshold reduced to its global floor.
    #[must_use]
    pub fn cycle_threshold_is_global(&self) -> bool {
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .all(MinorCycleEvidence::cycle_threshold_is_global)
    }

    /// Iterate accepted components in deterministic channel and component order.
    ///
    /// The ordered per-channel evidence remains authoritative; this projection
    /// exists for current reporting consumers that present one combined list.
    pub fn recorded_components(&self) -> impl Iterator<Item = &crate::MinorCycleComponent> {
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .filter_map(MinorCycleEvidence::recorded_component_sequence)
            .flatten()
    }

    /// Report the first component mismatch without turning diagnostics into a gate.
    #[must_use]
    pub fn first_divergence(&self, baseline: &Self) -> Option<ChannelComponentDivergence> {
        for (candidate_channel, expected_channel) in self.channels.iter().zip(&baseline.channels) {
            if candidate_channel.output_channel != expected_channel.output_channel {
                return None;
            }
            if candidate_channel.polarization != expected_channel.polarization {
                return None;
            }
            match (
                candidate_channel.minor_cycle(),
                expected_channel.minor_cycle(),
            ) {
                (Some(candidate), Some(expected)) => {
                    if let Some(component) = candidate.first_divergence(expected) {
                        return Some(ChannelComponentDivergence {
                            output_channel: candidate_channel.output_channel,
                            component,
                        });
                    }
                }
                (None, None) => {}
                _ => return None,
            }
        }
        None
    }
}

/// One reconstruction-owned continuum or channel-local solve.
#[derive(Debug, Clone)]
pub struct ReconstructionCycle {
    policy: ChannelCyclePolicy,
    program: MinorCycleProgram,
}

impl ReconstructionCycle {
    /// Construct a shared cycle from explicit scientific policy and controls.
    #[must_use]
    pub const fn new(policy: ChannelCyclePolicy, program: MinorCycleProgram) -> Self {
        Self { policy, program }
    }

    /// Run one CASA-style Högbom minor-cycle set across all image domains.
    ///
    /// Every polarization runs independently in canonical order. Within each
    /// polarization every field receives the same per-set iteration budget and
    /// runs its own controller in canonical domain order. All valid planes share
    /// one set-entry cycle threshold, and every accepted term is minted as one
    /// combined model delta for the shared Major-Cycle lineage.
    pub fn run_domains(
        &self,
        lifecycle: &ModelLifecycle,
        base: &ModelGeneration,
        normal: &FinalNormalState,
        masks: &crate::ImageDomainReconstructionMasks,
    ) -> Result<ReconstructionCycleResult, ReconstructionCycleError> {
        if self.policy != ChannelCyclePolicy::Independent {
            return Err(ReconstructionCycleError::UnsupportedCoupledPolicy);
        }
        let validities = image_domain_polarization_validities(normal)?;
        let shared_cycle_threshold =
            shared_image_domain_cycle_threshold(&self.program, normal, base, masks, &validities)?;
        let mut terms = Vec::<ModelDeltaTerm>::new();
        let mut channels = Vec::with_capacity(normal.polarization_count());
        let polarization_results = run_admitted_image_domain_polarizations(
            &validities,
            shared_cycle_threshold,
            |polarization, threshold| {
                let program = self
                    .program
                    .clone()
                    .with_fixed_cycle_threshold(threshold)
                    .on_model_plane(MinorCycleModelPlane::new(0, 0, polarization));
                run_image_domain_minor_cycle(lifecycle, base, normal, masks, program)
                    .map(|result| result.into_parts())
                    .map_err(ReconstructionCycleError::from)
            },
        )?;
        for (polarization, (validity, result)) in
            validities.into_iter().zip(polarization_results).enumerate()
        {
            let minor_cycle = result.map(|(delta, evidence)| {
                if let Some(delta) = delta {
                    terms.extend_from_slice(delta.terms());
                }
                evidence
            });
            channels.push(ChannelCycleEvidence {
                output_channel: normal.slab().core_range().start,
                polarization,
                validity,
                minor_cycle,
            });
        }
        let delta = (!terms.is_empty())
            .then(|| lifecycle.compile_delta(base, terms))
            .transpose()?;
        let evidence_id =
            reconstruction_cycle_evidence_id(lifecycle, normal, self.policy, &channels);
        Ok(ReconstructionCycleResult {
            delta,
            evidence: ReconstructionCycleEvidence {
                evidence_id,
                problem: lifecycle.problem(),
                policy: self.policy,
                channels: channels.into_boxed_slice(),
            },
        })
    }

    /// Run all valid planes in deterministic output-channel order.
    pub fn run(
        &self,
        lifecycle: &ModelLifecycle,
        base: &ModelGeneration,
        normal: &FinalNormalState,
        mask: &crate::ReconstructionMask,
    ) -> Result<ReconstructionCycleResult, ReconstructionCycleError> {
        if self.policy == ChannelCyclePolicy::Coupled {
            if !matches!(
                normal.catalog(),
                crate::NormalStateCatalog::UnnormalizedTaylorBlockV1
                    | crate::NormalStateCatalog::UnnormalizedJointBlockV1
            ) {
                return Err(ReconstructionCycleError::UnsupportedCoupledPolicy);
            }
            let validity = normal
                .support_validity()
                .unwrap_or(SpectralChannelValidity::Unmapped);
            if validity != SpectralChannelValidity::Valid {
                let channels = vec![ChannelCycleEvidence {
                    output_channel: normal.slab().core_range().start,
                    polarization: 0,
                    validity,
                    minor_cycle: None,
                }];
                let evidence_id =
                    reconstruction_cycle_evidence_id(lifecycle, normal, self.policy, &channels);
                return Ok(ReconstructionCycleResult {
                    delta: None,
                    evidence: ReconstructionCycleEvidence {
                        evidence_id,
                        problem: lifecycle.problem(),
                        policy: self.policy,
                        channels: channels.into_boxed_slice(),
                    },
                });
            }
            let result = run_minor_cycle(lifecycle, base, normal, mask, self.program.clone())?;
            let (delta, minor_cycle) = result.into_parts();
            let channels = vec![ChannelCycleEvidence {
                output_channel: normal.slab().core_range().start,
                polarization: 0,
                validity,
                minor_cycle: Some(minor_cycle),
            }];
            let evidence_id =
                reconstruction_cycle_evidence_id(lifecycle, normal, self.policy, &channels);
            return Ok(ReconstructionCycleResult {
                delta,
                evidence: ReconstructionCycleEvidence {
                    evidence_id,
                    problem: lifecycle.problem(),
                    policy: self.policy,
                    channels: channels.into_boxed_slice(),
                },
            });
        }
        let shared_cycle_threshold = shared_cycle_threshold(&self.program, normal)?;
        let mut terms = Vec::<ModelDeltaTerm>::new();
        let mut channels = Vec::with_capacity(normal.channel_count() * normal.polarization_count());
        for polarization in 0..normal.polarization_count() {
            for local_channel in 0..normal.channel_count() {
                let plane = normal
                    .polarization_plane(local_channel, polarization)
                    .ok_or(ReconstructionCycleError::InvalidNormalStateSlab)?;
                let validity = plane.validity();
                let minor_cycle = if validity == SpectralChannelValidity::Valid {
                    let program = self
                        .program
                        .clone()
                        .with_fixed_cycle_threshold(shared_cycle_threshold)
                        .on_model_plane(MinorCycleModelPlane::new(
                            0,
                            plane.output_channel(),
                            polarization,
                        ));
                    let result = run_minor_cycle_plane(lifecycle, base, plane, mask, program)?;
                    let (delta, evidence) = result.into_parts();
                    if let Some(delta) = delta {
                        terms.extend_from_slice(delta.terms());
                    }
                    Some(evidence)
                } else {
                    None
                };
                channels.push(ChannelCycleEvidence {
                    output_channel: plane.output_channel(),
                    polarization,
                    validity,
                    minor_cycle,
                });
            }
        }
        let delta = (!terms.is_empty())
            .then(|| lifecycle.compile_delta(base, terms))
            .transpose()?;
        let evidence_id =
            reconstruction_cycle_evidence_id(lifecycle, normal, self.policy, &channels);
        Ok(ReconstructionCycleResult {
            delta,
            evidence: ReconstructionCycleEvidence {
                evidence_id,
                problem: lifecycle.problem(),
                policy: self.policy,
                channels: channels.into_boxed_slice(),
            },
        })
    }

    /// Run one joint continuum-line solve with independently committed masks.
    pub fn run_coupled(
        &self,
        lifecycle: &ModelLifecycle,
        base: &ModelGeneration,
        normal: &FinalNormalState,
        masks: &crate::CoupledReconstructionMask,
    ) -> Result<ReconstructionCycleResult, ReconstructionCycleError> {
        if self.policy != ChannelCyclePolicy::Coupled
            || normal.catalog() != crate::NormalStateCatalog::UnnormalizedJointBlockV1
        {
            return Err(ReconstructionCycleError::UnsupportedCoupledPolicy);
        }
        if normal
            .channel_validity()
            .iter()
            .any(|validity| *validity != SpectralChannelValidity::Valid)
        {
            return Err(ReconstructionCycleError::InvalidJointSupport);
        }
        let result = run_joint_minor_cycle(lifecycle, base, normal, masks, self.program.clone())?;
        let (delta, evidence) = result.into_parts();
        let channels = vec![ChannelCycleEvidence {
            output_channel: normal.slab().core_range().start,
            polarization: 0,
            validity: SpectralChannelValidity::Valid,
            minor_cycle: Some(evidence),
        }];
        let evidence_id =
            reconstruction_cycle_evidence_id(lifecycle, normal, self.policy, &channels);
        Ok(ReconstructionCycleResult {
            delta,
            evidence: ReconstructionCycleEvidence {
                evidence_id,
                problem: lifecycle.problem(),
                policy: self.policy,
                channels: channels.into_boxed_slice(),
            },
        })
    }
}

fn run_admitted_image_domain_polarizations<T, E>(
    validities: &[SpectralChannelValidity],
    shared_cycle_threshold: Option<f64>,
    mut execute: impl FnMut(usize, Option<f64>) -> Result<T, E>,
) -> Result<Vec<Option<T>>, E> {
    validities
        .iter()
        .copied()
        .enumerate()
        .map(|(polarization, validity)| {
            (validity == SpectralChannelValidity::Valid)
                .then(|| execute(polarization, shared_cycle_threshold))
                .transpose()
        })
        .collect()
}

fn image_domain_polarization_validities(
    normal: &FinalNormalState,
) -> Result<Vec<SpectralChannelValidity>, ReconstructionCycleError> {
    (0..normal.polarization_count())
        .map(|polarization| {
            normal
                .domains()
                .map(|domain| {
                    domain
                        .polarization_plane(0, polarization)
                        .map(|plane| plane.validity())
                        .ok_or(ReconstructionCycleError::InvalidNormalStateSlab)
                })
                .find_map(|validity| match validity {
                    Ok(SpectralChannelValidity::Valid) => None,
                    outcome => Some(outcome),
                })
                .unwrap_or(Ok(SpectralChannelValidity::Valid))
        })
        .collect()
}

fn shared_image_domain_cycle_threshold(
    program: &MinorCycleProgram,
    normal: &FinalNormalState,
    base: &ModelGeneration,
    masks: &crate::ImageDomainReconstructionMasks,
    validities: &[SpectralChannelValidity],
) -> Result<Option<f64>, MinorCycleError> {
    if normal.polarization_count() == 1 {
        return Ok(None);
    }
    if normal.domain_count() != masks.len()
        || normal.domain_count() != base.shape().domains().len()
        || validities.len() != normal.polarization_count()
    {
        return Err(MinorCycleError::ModelShapeMismatch);
    }

    let mut global_peak = 0.0_f64;
    let mut maximum_sidelobe = 0.0_f64;
    for (polarization, validity) in validities.iter().copied().enumerate() {
        if validity != SpectralChannelValidity::Valid {
            continue;
        }
        for (domain, mask) in normal.domains().zip(masks.iter()) {
            let plane = domain
                .polarization_plane(0, polarization)
                .ok_or(MinorCycleError::ModelShapeMismatch)?;
            let shape = plane.shape();
            if mask.shape() != shape
                || base
                    .shape()
                    .domains()
                    .get(domain.ordinal())
                    .is_none_or(|model_domain| model_domain.pixels() != shape)
            {
                return Err(MinorCycleError::ModelShapeMismatch);
            }

            let mut psf_peak = None::<f64>;
            for value in plane.normal_approximation() {
                if psf_peak.is_none_or(|peak| value.re.abs() > peak.abs()) {
                    psf_peak = Some(value.re);
                }
            }
            let psf_peak = psf_peak.ok_or(MinorCycleError::InvalidPsfPeak)?;
            if !psf_peak.is_finite() || psf_peak <= 0.0 {
                return Err(MinorCycleError::InvalidPsfPeak);
            }

            let mut plane_peak = 0.0_f64;
            for (index, value) in plane.residual().iter().enumerate() {
                if !value.re.is_finite() {
                    return Err(MinorCycleError::GeneratedNonfinite);
                }
                let pixel = [index / shape[1], index % shape[1]];
                let cell = ModelCell::new(domain.ordinal(), 0, polarization, pixel);
                let supported = base
                    .shape()
                    .flat_index(cell)
                    .and_then(|flat| base.samples().get(flat))
                    .is_some_and(|sample| sample.support() == ModelSupport::Valid);
                if mask.contains(pixel) && supported {
                    plane_peak = plane_peak.max(value.re.abs() / psf_peak);
                }
            }
            global_peak = global_peak.max(plane_peak);
            let psf = plane
                .normal_approximation()
                .iter()
                .map(|value| value.re as f32)
                .collect::<Vec<_>>();
            maximum_sidelobe =
                maximum_sidelobe.max(crate::fitted_psf_sidelobe_fraction(&psf, shape)?);
        }
    }
    Ok(shared_cycle_threshold_from_statistics(
        program,
        [(global_peak, maximum_sidelobe)],
    ))
}

fn shared_cycle_threshold_from_statistics(
    program: &MinorCycleProgram,
    statistics: impl IntoIterator<Item = (f64, f64)>,
) -> Option<f64> {
    let (global_peak, maximum_sidelobe) = statistics
        .into_iter()
        .fold((0.0_f64, 0.0_f64), |(peak, sidelobe), current| {
            (peak.max(current.0), sidelobe.max(current.1))
        });
    program.cycle_threshold_for(global_peak, maximum_sidelobe)
}

fn shared_cycle_threshold(
    program: &MinorCycleProgram,
    normal: &FinalNormalState,
) -> Result<Option<f64>, MinorCycleError> {
    if normal.channel_count() * normal.polarization_count() == 1 {
        return Ok(None);
    }
    let mut global_peak = 0.0_f64;
    let mut maximum_sidelobe = 0.0_f64;
    for polarization in 0..normal.polarization_count() {
        for local_channel in 0..normal.channel_count() {
            let plane = normal
                .polarization_plane(local_channel, polarization)
                .ok_or(MinorCycleError::ModelShapeMismatch)?;
            if plane.validity() != SpectralChannelValidity::Valid {
                continue;
            }
            let psf = plane
                .normal_approximation()
                .iter()
                .map(|value| value.re as f32)
                .collect::<Vec<_>>();
            let psf_peak = psf
                .iter()
                .map(|value| f64::from(value.abs()))
                .fold(0.0_f64, f64::max);
            if !(psf_peak.is_finite() && psf_peak > 0.0) {
                return Err(MinorCycleError::InvalidPsfPeak);
            }
            let peak = plane
                .residual()
                .iter()
                .map(|value| value.re.abs() / psf_peak)
                .fold(0.0_f64, f64::max);
            global_peak = global_peak.max(peak);
            maximum_sidelobe =
                maximum_sidelobe.max(crate::fitted_psf_sidelobe_fraction(&psf, plane.shape())?);
        }
    }
    Ok(shared_cycle_threshold_from_statistics(
        program,
        [(global_peak, maximum_sidelobe)],
    ))
}

/// Owner-minted model update plus ordered channel evidence.
#[derive(Debug)]
pub struct ReconstructionCycleResult {
    delta: Option<ModelDelta>,
    evidence: ReconstructionCycleEvidence,
}

impl ReconstructionCycleResult {
    /// Borrow the combined base-bound update across every valid channel.
    #[must_use]
    pub const fn delta(&self) -> Option<&ModelDelta> {
        self.delta.as_ref()
    }

    /// Borrow ordered cycle evidence.
    #[must_use]
    pub const fn evidence(&self) -> &ReconstructionCycleEvidence {
        &self.evidence
    }

    /// Consume the result into its update and evidence.
    #[must_use]
    pub fn into_parts(self) -> (Option<ModelDelta>, ReconstructionCycleEvidence) {
        (self.delta, self.evidence)
    }
}

/// Exact reason the shared reconstruction cycle failed closed.
#[derive(Debug, Error)]
pub enum ReconstructionCycleError {
    /// No jointly coupled channel solver is approved by T38.
    #[error("coupled channel reconstruction requires an approved joint solver")]
    UnsupportedCoupledPolicy,
    /// At least one declared anchor or line channel lacks positive weighted support.
    #[error("joint reconstruction requires positive weighted support on every declared channel")]
    InvalidJointSupport,
    /// The normal-state slab cannot expose all of its declared core planes.
    #[error("normal-state slab storage does not match its declared channel interval")]
    InvalidNormalStateSlab,
    /// A channel-local minor solve failed.
    #[error(transparent)]
    Minor(#[from] MinorCycleError),
    /// The model owner rejected the combined channel update.
    #[error(transparent)]
    Lifecycle(#[from] ModelLifecycleError),
}

fn reconstruction_cycle_evidence_id(
    lifecycle: &ModelLifecycle,
    normal: &FinalNormalState,
    policy: ChannelCyclePolicy,
    channels: &[ChannelCycleEvidence],
) -> ReconstructionCycleEvidenceId {
    let mut encoder = Encoder::new(
        RECONSTRUCTION_CYCLE_EVIDENCE_DOMAIN,
        RECONSTRUCTION_CYCLE_EVIDENCE_VERSION,
    );
    encoder.identity(lifecycle.authority().as_bytes());
    encoder.identity(normal.completion_id().as_bytes());
    encoder.u8(match policy {
        ChannelCyclePolicy::Independent => 0,
        ChannelCyclePolicy::Coupled => 1,
    });
    encoder.usize(channels.len());
    for channel in channels {
        encoder.usize(channel.output_channel);
        encoder.usize(channel.polarization);
        encoder.u8(match channel.validity {
            SpectralChannelValidity::Valid => 0,
            SpectralChannelValidity::Blank => 1,
            SpectralChannelValidity::Unmapped => 2,
        });
        if let Some(evidence) = &channel.minor_cycle {
            encoder.u8(1);
            encoder.identity(evidence.evidence_id().as_bytes());
        } else {
            encoder.u8(0);
        }
    }
    ReconstructionCycleEvidenceId(LogicalIdentity::from_sha256(encoder.finish()))
}

#[cfg(test)]
mod tests {
    use casa_imaging_model::ReconstructionControls;

    use super::*;

    #[derive(Debug, PartialEq)]
    struct SyntheticPolarizationResult {
        delta_polarization: usize,
        evidence_polarization: usize,
        cycle_threshold: Option<f64>,
    }

    #[test]
    fn two_domain_two_polarization_schedule_shares_threshold_and_keeps_both_results() {
        let program = MinorCycleProgram::from_compiled(
            ReconstructionControls::new(20, 0.1, 0.0)
                .with_cycle_limits(20, None)
                .with_cycle_threshold(0.5, 0.1, 0.8),
        )
        .expect("valid Högbom controls");
        // Canonical (polarization, domain) statistics. Polarization one owns
        // the global peak while its second domain does not own the maximum
        // sidelobe, so a per-polarization derivation would yield a different
        // threshold from the required all-plane value.
        let threshold = shared_cycle_threshold_from_statistics(
            &program,
            [(2.0, 0.1), (4.0, 0.2), (8.0, 0.5), (6.0, 0.3)],
        );
        assert_eq!(threshold, Some(2.0));

        let results = run_admitted_image_domain_polarizations(
            &[
                SpectralChannelValidity::Valid,
                SpectralChannelValidity::Valid,
            ],
            threshold,
            |polarization, cycle_threshold| {
                Ok::<_, std::convert::Infallible>(SyntheticPolarizationResult {
                    delta_polarization: polarization,
                    evidence_polarization: polarization,
                    cycle_threshold,
                })
            },
        )
        .expect("synthetic independent polarization execution");

        assert_eq!(
            results,
            vec![
                Some(SyntheticPolarizationResult {
                    delta_polarization: 0,
                    evidence_polarization: 0,
                    cycle_threshold: Some(2.0),
                }),
                Some(SyntheticPolarizationResult {
                    delta_polarization: 1,
                    evidence_polarization: 1,
                    cycle_threshold: Some(2.0),
                }),
            ]
        );
    }
}
