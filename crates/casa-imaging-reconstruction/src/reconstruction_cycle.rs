// SPDX-License-Identifier: LGPL-3.0-or-later

//! Shared continuum and channel-local reconstruction-cycle orchestration.

use std::fmt;

use casa_imaging_model::{CompiledProblemId, LogicalIdentity, ModelDeltaTerm};
use thiserror::Error;

use crate::{
    ComponentDivergence, Encoder, FinalNormalState, MinorCycleError, MinorCycleEvidence,
    MinorCycleModelPlane, MinorCycleProgram, MinorCycleStopReason, ModelDelta, ModelGeneration,
    ModelLifecycle, ModelLifecycleError, SpectralChannelValidity,
    minor_cycle::{run_minor_cycle, run_minor_cycle_plane},
};

const RECONSTRUCTION_CYCLE_EVIDENCE_DOMAIN: &[u8] = b"casa-rs-reconstruction-cycle-evidence";
const RECONSTRUCTION_CYCLE_EVIDENCE_VERSION: u32 = 2;

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
    validity: SpectralChannelValidity,
    budget_exhausted: bool,
    minor_cycle: Option<MinorCycleEvidence>,
}

impl ChannelCycleEvidence {
    /// Return the absolute output-channel ordinal.
    #[must_use]
    pub const fn output_channel(&self) -> usize {
        self.output_channel
    }

    /// Return the normal-state validity of this channel.
    #[must_use]
    pub const fn validity(&self) -> SpectralChannelValidity {
        self.validity
    }

    /// Whether this valid channel was not entered because the ordered
    /// cube-wide iteration budget had already been consumed.
    #[must_use]
    pub const fn budget_exhausted(&self) -> bool {
        self.budget_exhausted
    }

    /// Return minor-cycle evidence for a valid channel.
    ///
    /// Blank, unmapped, and budget-exhausted channels are represented
    /// explicitly and never manufacture a solver stop or model update.
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
        self.channels
            .iter()
            .filter_map(ChannelCycleEvidence::minor_cycle)
            .map(MinorCycleEvidence::controller_iterations)
            .sum()
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

    /// Run all valid planes in deterministic output-channel order.
    pub fn run(
        &self,
        lifecycle: &ModelLifecycle,
        base: &ModelGeneration,
        normal: &FinalNormalState,
        mask: &crate::ReconstructionMask,
    ) -> Result<ReconstructionCycleResult, ReconstructionCycleError> {
        if self.policy == ChannelCyclePolicy::Coupled {
            if normal.catalog() != crate::NormalStateCatalog::UnnormalizedTaylorBlockV1 {
                return Err(ReconstructionCycleError::UnsupportedCoupledPolicy);
            }
            let validity = normal
                .support_validity()
                .unwrap_or(SpectralChannelValidity::Unmapped);
            if validity != SpectralChannelValidity::Valid {
                let channels = vec![ChannelCycleEvidence {
                    output_channel: normal.slab().core_range().start,
                    validity,
                    budget_exhausted: false,
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
                validity,
                budget_exhausted: false,
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
        let mut remaining_iterations = self.program.max_iterations();
        let mut remaining_valid_channels = normal
            .channel_validity()
            .iter()
            .filter(|validity| **validity == SpectralChannelValidity::Valid)
            .count();
        let mut terms = Vec::<ModelDeltaTerm>::new();
        let mut channels = Vec::with_capacity(normal.channel_count());
        for local_channel in 0..normal.channel_count() {
            let plane = normal
                .plane(local_channel)
                .ok_or(ReconstructionCycleError::InvalidNormalStateSlab)?;
            let validity = plane.validity();
            let budget_exhausted =
                validity == SpectralChannelValidity::Valid && remaining_iterations == 0;
            let minor_cycle = if validity == SpectralChannelValidity::Valid && !budget_exhausted {
                let channel_limit = remaining_iterations.div_ceil(remaining_valid_channels);
                let program = self
                    .program
                    .clone()
                    .limit_iterations(channel_limit)?
                    .with_fixed_cycle_threshold(shared_cycle_threshold)
                    .on_model_plane(MinorCycleModelPlane::new(0, plane.output_channel(), 0));
                let result = run_minor_cycle_plane(lifecycle, base, plane, mask, program)?;
                let (delta, evidence) = result.into_parts();
                remaining_iterations =
                    remaining_iterations.saturating_sub(evidence.controller_iterations());
                if let Some(delta) = delta {
                    terms.extend_from_slice(delta.terms());
                }
                Some(evidence)
            } else {
                None
            };
            if validity == SpectralChannelValidity::Valid {
                remaining_valid_channels = remaining_valid_channels.saturating_sub(1);
            }
            channels.push(ChannelCycleEvidence {
                output_channel: plane.output_channel(),
                validity,
                budget_exhausted,
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
}

fn shared_cycle_threshold(
    program: &MinorCycleProgram,
    normal: &FinalNormalState,
) -> Result<Option<f64>, MinorCycleError> {
    if normal.channel_count() == 1 {
        return Ok(None);
    }
    let mut global_peak = 0.0_f64;
    let mut maximum_sidelobe = 0.0_f64;
    for local_channel in 0..normal.channel_count() {
        let plane = normal
            .plane(local_channel)
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
    Ok(program.cycle_threshold_for(global_peak, maximum_sidelobe))
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
        encoder.u8(match channel.validity {
            SpectralChannelValidity::Valid => 0,
            SpectralChannelValidity::Blank => 1,
            SpectralChannelValidity::Unmapped => 2,
        });
        encoder.u8(u8::from(channel.budget_exhausted));
        if let Some(evidence) = &channel.minor_cycle {
            encoder.u8(1);
            encoder.identity(evidence.evidence_id().as_bytes());
        } else {
            encoder.u8(0);
        }
    }
    ReconstructionCycleEvidenceId(LogicalIdentity::from_sha256(encoder.finish()))
}
