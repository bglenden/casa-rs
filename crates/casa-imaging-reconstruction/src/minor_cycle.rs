// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded point and multiscale Minor Cycles over authoritative Normal State views.
//!
//! The Minor Cycle is a pure scientific operation: it consumes one immutable
//! view of an authoritative Final Normal State (the T20 residual paired with
//! its normal approximation), one named authoritative model generation, an
//! explicit valid-support window, and explicit gain, threshold, iteration,
//! and staleness controls. It accumulates Högbom components against the PSF
//! exactly like the casacore `LatticeCleaner` HOGBOM loop — peak residual
//! normalized by the PSF peak, gain-scaled PSF subtraction — and mints the
//! resulting sparse update only through the T28 model lifecycle owner as one
//! base-bound Model Delta. Authoritative model and residual state are never
//! mutated; the solver works on private buffers.
//!
//! Every non-converged stop (`IterationBound`, `StalenessBound`) records that
//! the linear approximation envelope is exhausted and explicitly requests
//! Major-Cycle reconciliation. Component-sequence recording is optional
//! diagnostic evidence: first-divergence comparisons between two recorded
//! sequences are informational and never gate acceptance.

use std::{collections::BTreeMap, fmt};

use casa_imaging_model::{
    CompiledProblem, CompiledProblemId, HogbomIterationAccounting, LogicalIdentity, ModelCell,
    ModelDeltaTerm, ModelExecutionAttemptId, ModelSupport, ModelValue, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionControls,
};
use casa_numerics::solve_symmetric_ldlt_casacore_dynamic;
use thiserror::Error;

use crate::{
    CoupledReconstructionMask, Encoder, FinalNormalState, FinalNormalStateCompletionId,
    ImageDomainReconstructionMasks, ModelDelta, ModelGeneration, ModelGenerationId, ModelLifecycle,
    ModelLifecycleError, ReconstructionMask, ReconstructionMaskGenerationId, ScienceTraceDigest,
    imaging_science_trace_enabled, major_cycle::FinalNormalStatePlane, trace_real_values,
};

const MINOR_CYCLE_EVIDENCE_DOMAIN: &[u8] = b"casa-rs-minor-cycle-evidence";
const MINOR_CYCLE_EVIDENCE_VERSION: u32 = 8;

/// Return the hard resident-memory envelope for one solver-owned Minor Cycle.
///
/// The envelope covers private residual planes, robust-statistics scratch,
/// MT-MFS scale kernels and Taylor systems, candidate vectors, the bounded
/// sparse Model Delta accumulator, and optional component diagnostics. Normal
/// State and model-generation storage are owned by their existing plan slots
/// and are therefore not counted again here.
#[must_use]
pub fn minor_cycle_workspace_bytes(
    shape: [usize; 2],
    basis: ReconstructionBasis,
    algorithm: &ReconstructionAlgorithm,
    maximum_iterations: usize,
    recorded_components: usize,
) -> u64 {
    let cells = sat_u64(shape[0]).saturating_mul(sat_u64(shape[1]));
    let scalar_workspace = cells.saturating_mul(16);
    let (terms, scales_px) = match (basis, algorithm) {
        (
            ReconstructionBasis::Taylor { terms }
            | ReconstructionBasis::TaylorViaChannelMajor { terms, .. },
            ReconstructionAlgorithm::Mtmfs { scales_px, .. },
        ) => (terms, scales_px),
        (
            ReconstructionBasis::JointContinuumLine {
                continuum_terms,
                line_terms,
            },
            ReconstructionAlgorithm::JointContinuumLine { scales_px, .. },
        ) => (continuum_terms.saturating_add(line_terms), scales_px),
        _ => return scalar_workspace,
    };
    let terms = sat_u64(terms);
    let effective_sample_counts = scales_px
        .iter()
        .copied()
        .filter(|scale| *scale <= (shape[0] / 2) as f64 && *scale <= (shape[1] / 2) as f64)
        .map(|scale| {
            if scale == 0.0 {
                1
            } else {
                let diameter = (scale.ceil() as u64).saturating_mul(2).saturating_add(1);
                diameter.saturating_mul(diameter)
            }
        })
        .collect::<Vec<_>>();
    let scale_count = sat_u64(effective_sample_counts.len());
    let kernel_samples = effective_sample_counts
        .iter()
        .copied()
        .fold(0_u64, u64::saturating_add);
    let maximum_kernel_samples = effective_sample_counts.iter().copied().max().unwrap_or(1);
    let term_cells = terms.saturating_mul(cells);
    let possible_sparse_terms = sat_u64(maximum_iterations)
        .saturating_mul(terms)
        .saturating_mul(maximum_kernel_samples)
        .min(term_cells);

    // A BTreeMap entry's allocator/node overhead is implementation-private.
    // Eight key/value pairs per possible term is a conservative 64-bit host
    // envelope and deliberately dominates the current standard-library node.
    const SPARSE_TERM_ENTRY_BOUND_BYTES: u64 = 128;
    let residual_planes = term_cells.saturating_mul(size_of_u64::<f64>());
    let plane_scratch = cells.saturating_mul(size_of_u64::<f64>());
    let kernel_storage = kernel_samples
        .saturating_mul(size_of_u64::<([isize; 2], f64)>())
        .saturating_add(scale_count.saturating_mul(size_of_u64::<ScaleKernel>()));
    let square_terms = terms.saturating_mul(terms);
    let scale_systems = scale_count.saturating_mul(
        square_terms
            .saturating_mul(size_of_u64::<f64>())
            .saturating_add(size_of_u64::<TaylorScaleSystem>()),
    );
    let system_and_candidate_scratch = square_terms
        .saturating_mul(size_of_u64::<f64>())
        .saturating_mul(4)
        .saturating_add(terms.saturating_mul(size_of_u64::<f64>()).saturating_mul(6));
    let sparse_delta = possible_sparse_terms.saturating_mul(SPARSE_TERM_ENTRY_BOUND_BYTES);
    let diagnostics = sat_u64(recorded_components)
        .min(sat_u64(maximum_iterations).saturating_mul(terms))
        .saturating_mul(size_of_u64::<MinorCycleComponent>());
    let container_overhead = terms
        .saturating_add(scale_count.saturating_mul(2))
        .saturating_add(8)
        .saturating_mul(size_of_u64::<Vec<u8>>());

    residual_planes
        .saturating_add(plane_scratch)
        .saturating_add(kernel_storage)
        .saturating_add(scale_systems)
        .saturating_add(system_and_candidate_scratch)
        .saturating_add(sparse_delta)
        .saturating_add(diagnostics)
        .saturating_add(container_overhead)
}

fn sat_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn size_of_u64<T>() -> u64 {
    sat_u64(std::mem::size_of::<T>())
}

macro_rules! minor_cycle_identity {
    ($name:ident, $version:ident, $summary:literal) => {
        #[doc = $summary]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(LogicalIdentity);

        impl $name {
            /// Identity schema version.
            pub const SCHEMA_VERSION: u32 = $version;

            /// Return the exact SHA-256 digest.
            #[must_use]
            pub const fn as_bytes(self) -> [u8; 32] {
                self.0.as_bytes()
            }

            /// Return this typed value as a logical identity.
            #[must_use]
            pub const fn identity(self) -> LogicalIdentity {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "("))?;
                crate::write_hex(formatter, &self.as_bytes())?;
                formatter.write_str(")")
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                crate::write_hex(formatter, &self.as_bytes())
            }
        }
    };
}

minor_cycle_identity!(
    MinorCycleEvidenceId,
    MINOR_CYCLE_EVIDENCE_VERSION,
    "Stable identity of one bounded Minor-Cycle solve."
);

/// One reconstruction-owned minor-cycle program.
///
/// The validity contract states whether the compiled normal-state view remains
/// exact for the full solve or requires reconciliation at a proven update
/// envelope. The common Högbom path is exact; bounded validity is explicit and
/// owner-supplied rather than inferred from an arbitrary flux constant.
#[derive(Debug, Clone, PartialEq)]
pub struct MinorCycleProgram {
    problem: Option<CompiledProblemId>,
    algorithm: ReconstructionAlgorithm,
    model_plane: MinorCycleModelPlane,
    gain: f64,
    threshold: f64,
    noise_sigma: Option<f64>,
    max_iterations: usize,
    hogbom_iteration_accounting: HogbomIterationAccounting,
    validity: MinorCycleValidity,
    cycle_threshold: Option<CycleThresholdControls>,
    fixed_cycle_threshold: Option<f64>,
    component_sequence_limit: Option<usize>,
    maximum_condition_number: Option<f64>,
}

/// Validity of the reconstruction-owned normal-state view used by one solve.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MinorCycleValidity {
    /// The view remains scientifically valid for the full minor cycle.
    Exact,
    /// Reconciliation is required before cumulative absolute updates exceed the bound.
    Bounded {
        /// Maximum cumulative absolute component update accepted by one minor cycle.
        maximum_absolute_update: f64,
    },
}

/// Typed model-space plane updated by one shared minor-cycle control loop.
///
/// The solver mathematics remain two-dimensional; this coordinate chooses
/// which domain, spectral coefficient, and polarization plane receives the
/// resulting sparse delta. Later composition tickets may construct their own
/// normal-state planes and reuse this loop without duplicating its stopping,
/// masking, or component-accounting behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MinorCycleModelPlane {
    domain: usize,
    coefficient: usize,
    polarization: usize,
}

impl MinorCycleModelPlane {
    /// The primary continuum Stokes-I constant-basis plane.
    pub const PRIMARY: Self = Self::new(0, 0, 0);

    /// Construct one typed model-plane coordinate.
    #[must_use]
    pub const fn new(domain: usize, coefficient: usize, polarization: usize) -> Self {
        Self {
            domain,
            coefficient,
            polarization,
        }
    }

    /// Return the image-domain ordinal.
    #[must_use]
    pub const fn domain(self) -> usize {
        self.domain
    }

    /// Return the spectral-basis coefficient ordinal.
    #[must_use]
    pub const fn coefficient(self) -> usize {
        self.coefficient
    }

    /// Return the polarization-plane ordinal.
    #[must_use]
    pub const fn polarization(self) -> usize {
        self.polarization
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct CycleThresholdControls {
    factor: f64,
    minimum_psf_fraction: f64,
    maximum_psf_fraction: f64,
}

impl MinorCycleProgram {
    /// Derive the executable minor-cycle controls from the compiled contract.
    ///
    pub fn for_algorithm(
        mut algorithm: ReconstructionAlgorithm,
        controls: ReconstructionControls,
    ) -> Result<Self, MinorCycleError> {
        if matches!(algorithm, ReconstructionAlgorithm::Mtmfs { .. }) {
            return Err(MinorCycleError::CompiledProblemRequired);
        }
        Self::for_contract(None, &mut algorithm, controls)
    }

    /// Derive an identity-bound program from the authoritative Compiled Problem.
    pub fn for_problem(problem: &CompiledProblem) -> Result<Self, MinorCycleError> {
        let mut algorithm = problem.reconstruction().algorithm().clone();
        let mut program = Self::for_contract(
            Some(problem.problem_id()),
            &mut algorithm,
            problem.reconstruction().controls(),
        )?;
        program.maximum_condition_number = problem
            .reconstruction()
            .joint_continuum_line()
            .map(|contract| contract.maximum_condition_number());
        Ok(program)
    }

    fn for_contract(
        problem: Option<CompiledProblemId>,
        algorithm: &mut ReconstructionAlgorithm,
        controls: ReconstructionControls,
    ) -> Result<Self, MinorCycleError> {
        if !matches!(
            algorithm,
            ReconstructionAlgorithm::Hogbom
                | ReconstructionAlgorithm::Clark
                | ReconstructionAlgorithm::Multiscale { .. }
                | ReconstructionAlgorithm::Mtmfs { .. }
                | ReconstructionAlgorithm::JointContinuumLine { .. }
        ) {
            return Err(MinorCycleError::UnsupportedAlgorithm);
        }
        if let ReconstructionAlgorithm::Multiscale { scales_px, .. }
        | ReconstructionAlgorithm::Mtmfs { scales_px, .. }
        | ReconstructionAlgorithm::JointContinuumLine { scales_px, .. } = algorithm
        {
            if scales_px.is_empty()
                || scales_px
                    .iter()
                    .any(|scale| !scale.is_finite() || *scale < 0.0)
            {
                return Err(MinorCycleError::InvalidScale);
            }
            scales_px.sort_by(f64::total_cmp);
            scales_px.dedup_by(|left, right| left.to_bits() == right.to_bits());
        }
        Self::new_for_algorithm(
            algorithm.clone(),
            controls.gain(),
            controls.threshold_jy_per_beam(),
            controls
                .cycle_iteration_limit()
                .unwrap_or(controls.max_minor_iterations())
                .min(controls.max_minor_iterations()),
            MinorCycleValidity::Exact,
        )
        .map(|mut program| {
            program.noise_sigma = controls.noise_sigma();
            program.hogbom_iteration_accounting =
                if matches!(&program.algorithm, ReconstructionAlgorithm::Hogbom) {
                    controls.hogbom_iteration_accounting()
                } else {
                    HogbomIterationAccounting::Strict
                };
            program.cycle_threshold =
                controls
                    .cycle_factor()
                    .map(|factor| CycleThresholdControls {
                        factor,
                        minimum_psf_fraction: controls
                            .minimum_psf_fraction()
                            .expect("compiled cycle threshold is complete"),
                        maximum_psf_fraction: controls
                            .maximum_psf_fraction()
                            .expect("compiled cycle threshold is complete"),
                    });
            program.problem = problem;
            program
        })
    }

    /// Derive the Högbom program used by the point-clean baseline.
    pub fn from_compiled(controls: ReconstructionControls) -> Result<Self, MinorCycleError> {
        Self::for_algorithm(ReconstructionAlgorithm::Hogbom, controls)
    }

    /// Construct an exact Högbom view.
    ///
    /// # Errors
    ///
    /// Rejects a gain outside `(0, 1]`, a negative or non-finite threshold, or
    /// a zero iteration bound.
    pub fn new(gain: f64, threshold: f64, max_iterations: usize) -> Result<Self, MinorCycleError> {
        Self::new_for_algorithm(
            ReconstructionAlgorithm::Hogbom,
            gain,
            threshold,
            max_iterations,
            MinorCycleValidity::Exact,
        )
    }

    /// Construct a Högbom view with an owner-proven finite validity envelope.
    pub fn new_bounded(
        gain: f64,
        threshold: f64,
        max_iterations: usize,
        maximum_absolute_update: f64,
    ) -> Result<Self, MinorCycleError> {
        Self::new_for_algorithm(
            ReconstructionAlgorithm::Hogbom,
            gain,
            threshold,
            max_iterations,
            MinorCycleValidity::Bounded {
                maximum_absolute_update,
            },
        )
    }

    fn new_for_algorithm(
        algorithm: ReconstructionAlgorithm,
        gain: f64,
        threshold: f64,
        max_iterations: usize,
        validity: MinorCycleValidity,
    ) -> Result<Self, MinorCycleError> {
        if !(gain > 0.0 && gain <= 1.0) {
            return Err(MinorCycleError::InvalidGain);
        }
        if !threshold.is_finite() || threshold < 0.0 {
            return Err(MinorCycleError::InvalidThreshold);
        }
        if max_iterations == 0 {
            return Err(MinorCycleError::InvalidIterationBound);
        }
        validate_validity(validity)?;
        Ok(Self {
            problem: None,
            algorithm,
            model_plane: MinorCycleModelPlane::PRIMARY,
            gain,
            threshold,
            noise_sigma: None,
            max_iterations,
            hogbom_iteration_accounting: HogbomIterationAccounting::Strict,
            validity,
            cycle_threshold: None,
            fixed_cycle_threshold: None,
            component_sequence_limit: None,
            maximum_condition_number: None,
        })
    }

    /// Return the selected reconstruction algorithm.
    #[must_use]
    pub const fn algorithm(&self) -> &ReconstructionAlgorithm {
        &self.algorithm
    }

    /// Select the typed model plane updated by this shared solver loop.
    #[must_use]
    pub const fn on_model_plane(mut self, model_plane: MinorCycleModelPlane) -> Self {
        self.model_plane = model_plane;
        self
    }

    /// Return the selected typed model plane.
    #[must_use]
    pub const fn model_plane(&self) -> MinorCycleModelPlane {
        self.model_plane
    }

    /// Record the first accepted components as diagnostic evidence.
    ///
    /// Recording is capped so a long clean cannot retain unbounded component
    /// lists; cleaning itself continues past the cap.
    ///
    /// # Errors
    ///
    /// Rejects a zero limit.
    pub fn record_component_sequence(mut self, limit: usize) -> Result<Self, MinorCycleError> {
        if limit == 0 {
            return Err(MinorCycleError::InvalidRecordingLimit);
        }
        self.component_sequence_limit = Some(limit);
        Ok(self)
    }

    /// Return the loop gain fraction applied to each normalized peak.
    #[must_use]
    pub const fn gain(&self) -> f64 {
        self.gain
    }

    /// Return the absolute normalized-flux stopping threshold.
    #[must_use]
    pub const fn threshold(&self) -> f64 {
        self.threshold
    }

    /// Return the optional robust-RMS stopping multiplier.
    #[must_use]
    pub const fn noise_sigma(&self) -> Option<f64> {
        self.noise_sigma
    }

    /// Tighten this cycle to the remaining total iteration budget.
    pub fn limit_iterations(mut self, remaining: usize) -> Result<Self, MinorCycleError> {
        self.max_iterations = self.max_iterations.min(remaining);
        if self.max_iterations == 0 {
            return Err(MinorCycleError::InvalidIterationBound);
        }
        Ok(self)
    }

    /// Return the reported task/controller iteration bound.
    #[must_use]
    pub const fn max_iterations(&self) -> usize {
        self.max_iterations
    }

    /// Return Högbom's iteration-accounting policy.
    #[must_use]
    pub const fn hogbom_iteration_accounting(&self) -> HogbomIterationAccounting {
        self.hogbom_iteration_accounting
    }

    fn actual_iteration_limit(&self) -> usize {
        if matches!(&self.algorithm, ReconstructionAlgorithm::Hogbom)
            && self.hogbom_iteration_accounting == HogbomIterationAccounting::CasaInclusive
        {
            self.max_iterations.saturating_add(1)
        } else {
            self.max_iterations
        }
    }

    fn controller_iterations(
        &self,
        actual_iterations: usize,
        stop_reason: MinorCycleStopReason,
    ) -> usize {
        if self.hogbom_iteration_accounting == HogbomIterationAccounting::CasaInclusive
            && stop_reason == MinorCycleStopReason::IterationBound
        {
            actual_iterations.min(self.max_iterations)
        } else {
            actual_iterations
        }
    }

    pub(crate) fn with_fixed_cycle_threshold(mut self, threshold: Option<f64>) -> Self {
        self.fixed_cycle_threshold = threshold;
        self
    }

    pub(crate) fn cycle_threshold_for(
        &self,
        initial_peak: f64,
        maximum_sidelobe: f64,
    ) -> Option<f64> {
        self.cycle_threshold.map(|cycle| {
            initial_peak
                * (cycle.factor * maximum_sidelobe)
                    .clamp(cycle.minimum_psf_fraction, cycle.maximum_psf_fraction)
        })
    }

    /// Return the validity contract for this normal-state view.
    #[must_use]
    pub const fn validity(&self) -> MinorCycleValidity {
        self.validity
    }

    /// Replace the owner-proven validity envelope for this solve.
    pub fn with_validity(mut self, validity: MinorCycleValidity) -> Result<Self, MinorCycleError> {
        validate_validity(validity)?;
        self.validity = validity;
        Ok(self)
    }

    /// Return the optional recorded-component-sequence capacity.
    #[must_use]
    pub const fn component_sequence_limit(&self) -> Option<usize> {
        self.component_sequence_limit
    }
}

fn validate_validity(validity: MinorCycleValidity) -> Result<(), MinorCycleError> {
    if let MinorCycleValidity::Bounded {
        maximum_absolute_update,
    } = validity
        && (!maximum_absolute_update.is_finite() || maximum_absolute_update <= 0.0)
    {
        return Err(MinorCycleError::InvalidValidityBound);
    }
    Ok(())
}

struct MinorCycleController {
    iteration_limit: usize,
    validity: MinorCycleValidity,
    effective_threshold: f64,
    iterations: usize,
    total_flux: f64,
    stop_reason: Option<MinorCycleStopReason>,
}

impl MinorCycleController {
    fn new(
        controls: &MinorCycleProgram,
        effective_threshold: f64,
        has_valid_support: bool,
    ) -> Self {
        Self {
            iteration_limit: if has_valid_support {
                controls.actual_iteration_limit()
            } else {
                0
            },
            validity: controls.validity(),
            effective_threshold,
            iterations: 0,
            total_flux: 0.0,
            stop_reason: (!has_valid_support).then_some(MinorCycleStopReason::ThresholdReached),
        }
    }

    const fn iteration_limit(&self) -> usize {
        self.iteration_limit
    }

    const fn iterations(&self) -> usize {
        self.iterations
    }

    fn admit(&mut self, strength: f64, charged_update: f64, inclusive_threshold: bool) -> bool {
        let reached = strength == 0.0
            || if inclusive_threshold {
                strength.abs() <= self.effective_threshold
            } else {
                strength.abs() < self.effective_threshold
            };
        if reached {
            self.stop_reason = Some(MinorCycleStopReason::ThresholdReached);
            return false;
        }
        if let MinorCycleValidity::Bounded {
            maximum_absolute_update,
        } = self.validity
            && self.total_flux + charged_update > maximum_absolute_update
        {
            self.stop_reason = Some(MinorCycleStopReason::StalenessBound);
            return false;
        }
        true
    }

    fn accepted(&mut self, charged_update: f64) {
        self.iterations += 1;
        self.total_flux += charged_update;
    }

    fn stop(&mut self, reason: MinorCycleStopReason) {
        self.stop_reason = Some(reason);
    }

    fn finish(self) -> (usize, f64, MinorCycleStopReason) {
        (
            self.iterations,
            self.total_flux,
            self.stop_reason
                .unwrap_or(MinorCycleStopReason::IterationBound),
        )
    }
}

/// One accepted Högbom component in canonical typed model coordinates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MinorCycleComponent {
    cell: ModelCell,
    flux: f64,
    scale_px: f64,
}

impl MinorCycleComponent {
    /// Return the component's typed model cell.
    #[must_use]
    pub const fn cell(&self) -> ModelCell {
        self.cell
    }

    /// Return the signed component flux in model units.
    #[must_use]
    pub const fn flux(&self) -> f64 {
        self.flux
    }

    /// Return the selected component scale in pixels (`0` for point CLEAN).
    #[must_use]
    pub const fn scale_px(&self) -> f64 {
        self.scale_px
    }

    fn same_value(&self, other: &Self) -> bool {
        self.cell == other.cell
            && self.flux.to_bits() == other.flux.to_bits()
            && self.scale_px.to_bits() == other.scale_px.to_bits()
    }
}

/// Informational first divergence between two recorded component sequences.
///
/// This record never gates acceptance; it exists so parity trajectories can
/// report where two solvers first disagree without failing either side.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ComponentDivergence {
    index: usize,
    baseline: Option<MinorCycleComponent>,
    candidate: Option<MinorCycleComponent>,
}

impl ComponentDivergence {
    /// Return the first divergent sequence index.
    #[must_use]
    pub const fn index(&self) -> usize {
        self.index
    }

    /// Return the baseline entry, or `None` when the baseline ended here.
    #[must_use]
    pub const fn baseline(&self) -> Option<MinorCycleComponent> {
        self.baseline
    }

    /// Return the candidate entry, or `None` when the candidate ended here.
    #[must_use]
    pub const fn candidate(&self) -> Option<MinorCycleComponent> {
        self.candidate
    }
}

/// Why one minor-cycle solve stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinorCycleStopReason {
    /// The normalized residual peak fell strictly below the explicit
    /// threshold (the casacore HOGBOM convention; a peak exactly at the
    /// threshold still cleans one component).
    ThresholdReached,
    /// The policy-derived loop bound was reached with work potentially remaining.
    IterationBound,
    /// Accepting the next candidate would exceed the reconstruction owner's
    /// proven validity envelope. The rejected candidate left no trace in the
    /// delta or evidence.
    StalenessBound,
    /// A multiscale candidate grew by more than 50 percent after accepted progress.
    MultiscaleDivergence,
}

/// Owner-minted evidence of one minor-cycle solve.
///
/// The evidence names the exact consumed approximation (the Final Normal
/// State completion and content identities) and the exact input model
/// generation, and reports whether Major-Cycle reconciliation is explicitly
/// requested. It carries no product meaning and mints no publication
/// authority.
#[derive(Debug)]
pub struct MinorCycleEvidence {
    evidence_id: MinorCycleEvidenceId,
    problem: CompiledProblemId,
    attempt: ModelExecutionAttemptId,
    epoch: u64,
    input_generation: ModelGenerationId,
    normal_state_completion: FinalNormalStateCompletionId,
    normal_state_content: LogicalIdentity,
    iterations: usize,
    controller_iterations: usize,
    total_flux: f64,
    initial_peak_flux: f64,
    final_peak_flux: f64,
    noise_rms: Option<f64>,
    global_threshold: f64,
    effective_threshold: f64,
    stop_reason: MinorCycleStopReason,
    clark_approximation: Option<ClarkApproximation>,
    clark_refreshes: usize,
    recorded: Option<Box<[MinorCycleComponent]>>,
    cycle_threshold: Option<f64>,
    image_domain_runs: Option<Box<[ImageDomainMinorCycleEvidence]>>,
}

/// One image field's contribution to a shared multi-domain minor-cycle set.
///
/// CASA gives every image field the same per-set iteration budget and shared
/// absolute cycle threshold, but each field owns its own residual peak, noise
/// threshold, controller, and stop decision. These records remain in canonical
/// compiled-domain order and explain why the aggregate iteration count may
/// exceed the requested per-set budget.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImageDomainMinorCycleEvidence {
    domain_ordinal: usize,
    iterations: usize,
    controller_iterations: usize,
    total_flux: f64,
    initial_peak_flux: f64,
    final_peak_flux: f64,
    noise_rms: Option<f64>,
    global_threshold: f64,
    effective_threshold: f64,
    cycle_threshold: Option<f64>,
    stop_reason: MinorCycleStopReason,
}

impl ImageDomainMinorCycleEvidence {
    /// Return the canonical compiled image-domain ordinal.
    #[must_use]
    pub const fn domain_ordinal(self) -> usize {
        self.domain_ordinal
    }

    /// Return the number of components applied in this field.
    #[must_use]
    pub const fn iterations(self) -> usize {
        self.iterations
    }

    /// Return the iterations charged to this field's controller budget.
    #[must_use]
    pub const fn controller_iterations(self) -> usize {
        self.controller_iterations
    }

    /// Return the cumulative absolute component flux for this field.
    #[must_use]
    pub const fn total_flux(self) -> f64 {
        self.total_flux
    }

    /// Return this field's normalized residual peak at entry.
    #[must_use]
    pub const fn initial_peak_flux(self) -> f64 {
        self.initial_peak_flux
    }

    /// Return this field's terminal normalized residual peak.
    #[must_use]
    pub const fn final_peak_flux(self) -> f64 {
        self.final_peak_flux
    }

    /// Return this field's robust RMS when an `nsigma` stop was requested.
    #[must_use]
    pub const fn noise_rms(self) -> Option<f64> {
        self.noise_rms
    }

    /// Return this field's absolute/noise threshold before cycle limiting.
    #[must_use]
    pub const fn global_threshold(self) -> f64 {
        self.global_threshold
    }

    /// Return this field's effective threshold after cycle limiting.
    #[must_use]
    pub const fn effective_threshold(self) -> f64 {
        self.effective_threshold
    }

    /// Return the shared set-entry PSF-sidelobe-derived cycle threshold.
    #[must_use]
    pub const fn cycle_threshold(self) -> Option<f64> {
        self.cycle_threshold
    }

    /// Return why this field's controller stopped.
    #[must_use]
    pub const fn stop_reason(self) -> MinorCycleStopReason {
        self.stop_reason
    }
}

impl MinorCycleEvidence {
    /// Return the stable solve identity.
    #[must_use]
    pub const fn evidence_id(&self) -> MinorCycleEvidenceId {
        self.evidence_id
    }

    /// Return the compiled problem the solve executed against.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem
    }

    /// Return the execution attempt that owns the solve.
    #[must_use]
    pub const fn attempt(&self) -> ModelExecutionAttemptId {
        self.attempt
    }

    /// Return the generation epoch within the attempt.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Return the exact named input model generation.
    #[must_use]
    pub const fn input_generation(&self) -> ModelGenerationId {
        self.input_generation
    }

    /// Return the consumed Final Normal State completion identity.
    #[must_use]
    pub const fn normal_state_completion(&self) -> FinalNormalStateCompletionId {
        self.normal_state_completion
    }

    /// Return the consumed normal-state approximation content identity.
    #[must_use]
    pub const fn normal_state_content(&self) -> LogicalIdentity {
        self.normal_state_content
    }

    /// Return the number of components actually applied.
    #[must_use]
    pub const fn iterations(&self) -> usize {
        self.iterations
    }

    /// Return the count charged to the reported task/controller budget.
    #[must_use]
    pub const fn controller_iterations(&self) -> usize {
        self.controller_iterations
    }

    /// Return the cumulative absolute component flux.
    #[must_use]
    pub const fn total_flux(&self) -> f64 {
        self.total_flux
    }

    /// Return the normalized residual peak at entry to this minor cycle.
    #[must_use]
    pub const fn initial_peak_flux(&self) -> f64 {
        self.initial_peak_flux
    }

    /// Return the last evaluated normalized residual peak magnitude.
    #[must_use]
    pub const fn final_peak_flux(&self) -> f64 {
        self.final_peak_flux
    }

    /// Return the robust RMS used by an `nsigma` stop.
    ///
    /// Multi-domain evidence returns the maximum of the independently derived
    /// per-field values; use [`Self::image_domain_runs`] for each controller's
    /// exact value.
    #[must_use]
    pub const fn noise_rms(&self) -> Option<f64> {
        self.noise_rms
    }

    /// Return the actual threshold after combining absolute and `nsigma` controls.
    ///
    /// Multi-domain evidence returns the maximum per-field effective threshold.
    #[must_use]
    pub const fn effective_threshold(&self) -> f64 {
        self.effective_threshold
    }

    /// Return the absolute/noise threshold before applying any cycle threshold.
    ///
    /// Multi-domain evidence returns the maximum per-field value; there is no
    /// single shared field threshold.
    #[must_use]
    pub const fn global_threshold(&self) -> f64 {
        self.global_threshold
    }

    /// Return the PSF-sidelobe-derived cycle threshold, when configured.
    ///
    /// Multi-domain evidence returns the shared set-entry threshold applied to
    /// every field controller.
    #[must_use]
    pub const fn cycle_threshold(&self) -> Option<f64> {
        self.cycle_threshold
    }

    /// Whether CASA's cycle threshold was no stronger than the global
    /// absolute/noise threshold at this boundary.
    #[must_use]
    pub fn cycle_threshold_is_global(&self) -> bool {
        self.cycle_threshold
            .is_none_or(|threshold| threshold <= self.global_threshold)
    }

    /// Return why the solve stopped.
    #[must_use]
    pub const fn stop_reason(&self) -> MinorCycleStopReason {
        self.stop_reason
    }

    /// Return Clark's derived PSF-patch approximation, when Clark ran.
    #[must_use]
    pub const fn clark_approximation(&self) -> Option<ClarkApproximation> {
        self.clark_approximation
    }

    /// Return the number of exact full-residual refreshes between Clark subcycles.
    #[must_use]
    pub const fn clark_refreshes(&self) -> usize {
        self.clark_refreshes
    }

    /// Whether the outcome explicitly requests Major-Cycle reconciliation.
    ///
    /// A threshold stop needs no reconciliation only when it accepted no
    /// component and therefore minted no delta. Every accepted component must
    /// pass through the model owner and a fresh complete-data reconciliation,
    /// even when the working residual subsequently falls below threshold.
    /// Iteration and staleness stops request reconciliation unconditionally.
    #[must_use]
    pub const fn requests_reconciliation(&self) -> bool {
        self.iterations != 0 || !matches!(self.stop_reason, MinorCycleStopReason::ThresholdReached)
    }

    /// Return the optionally recorded leading component sequence.
    #[must_use]
    pub fn recorded_component_sequence(&self) -> Option<&[MinorCycleComponent]> {
        self.recorded.as_deref()
    }

    /// Return per-field controller evidence for a multi-domain cycle set.
    ///
    /// Scalar/cube/Taylor solves return `None`. Image-domain collection
    /// execution returns one record per compiled domain, including N=1.
    #[must_use]
    pub fn image_domain_runs(&self) -> Option<&[ImageDomainMinorCycleEvidence]> {
        self.image_domain_runs.as_deref()
    }

    /// Compare this evidence's recorded sequence against another baseline.
    ///
    /// Returns the first index where the sequences differ in pixel or exact
    /// flux bits, or where one sequence ends early. Comparison is purely
    /// informational and never gates acceptance; `None` means the sequences
    /// agree or cannot be compared (either side unrecorded).
    #[must_use]
    pub fn first_divergence(&self, baseline: &Self) -> Option<ComponentDivergence> {
        let candidate = self.recorded.as_deref()?;
        let baseline = baseline.recorded.as_deref()?;
        let shared = candidate.len().min(baseline.len());
        for index in 0..shared {
            if !baseline[index].same_value(&candidate[index]) {
                return Some(ComponentDivergence {
                    index,
                    baseline: Some(baseline[index]),
                    candidate: Some(candidate[index]),
                });
            }
        }
        if baseline.len() != candidate.len() {
            let index = shared;
            return Some(ComponentDivergence {
                index,
                baseline: baseline.get(index).copied(),
                candidate: candidate.get(index).copied(),
            });
        }
        None
    }
}

/// Scientific approximation used by one Clark active-set solve.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClarkApproximation {
    radius: [usize; 2],
    maximum_exterior_sidelobe: f64,
}

struct ClarkWorkState {
    cutoff: f64,
    active: Vec<bool>,
    refreshes: usize,
}

impl ClarkApproximation {
    /// Return the symmetric PSF-patch radius in pixels.
    #[must_use]
    pub const fn radius(self) -> [usize; 2] {
        self.radius
    }

    /// Return the largest absolute PSF value outside the patch.
    #[must_use]
    pub const fn maximum_exterior_sidelobe(self) -> f64 {
        self.maximum_exterior_sidelobe
    }
}

/// One Högbom solve: the owner-minted Model Delta plus its evidence.
#[derive(Debug)]
pub struct MinorCycleResult {
    delta: Option<ModelDelta>,
    evidence: MinorCycleEvidence,
}

impl MinorCycleResult {
    /// Return the validated base-bound Model Delta, or `None` when no
    /// component was accepted (an already-converged view).
    #[must_use]
    pub const fn delta(&self) -> Option<&ModelDelta> {
        self.delta.as_ref()
    }

    /// Borrow the solve evidence.
    #[must_use]
    pub const fn evidence(&self) -> &MinorCycleEvidence {
        &self.evidence
    }

    /// Consume the solve into its delta and evidence.
    #[must_use]
    pub fn into_parts(self) -> (Option<ModelDelta>, MinorCycleEvidence) {
        (self.delta, self.evidence)
    }
}

/// Exact reason a minor-cycle solve failed closed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MinorCycleError {
    /// The selected reconstruction algorithm has no minor-cycle implementation.
    #[error("reconstruction algorithm has no minor-cycle implementation")]
    UnsupportedAlgorithm,
    /// T31 image-domain execution admits only one-channel constant-basis Högbom.
    #[error("image-domain reconstruction requires one-channel constant-basis Högbom")]
    UnsupportedImageDomainCycle,
    /// MT-MFS controls were not derived from their authoritative Compiled Problem.
    #[error("MT-MFS minor-cycle programs require an authoritative Compiled Problem")]
    CompiledProblemRequired,
    /// The selected solver and authoritative Normal State catalogs disagree.
    #[error("minor-cycle algorithm does not match the Normal State catalog")]
    InvalidNormalStateCatalog,
    /// Joint reconstruction requires both immutable spatial supports.
    #[error("joint continuum-line reconstruction requires distinct continuum and line masks")]
    CoupledMaskRequired,
    /// The coupled Taylor normal block is singular or numerically dependent.
    #[error("MT-MFS normal block is singular or numerically dependent")]
    SingularTaylorNormalBlock,
    /// The applicable joint continuum-line normal sub-block is singular.
    #[error("joint continuum-line normal block is singular or numerically dependent")]
    SingularJointNormalBlock,
    /// A multiscale program omitted scales or supplied a negative/non-finite scale.
    #[error("multiscale CLEAN requires finite non-negative scales")]
    InvalidScale,
    /// The Högbom gain was outside `(0, 1]`.
    #[error("Högbom gain must lie in (0, 1]")]
    InvalidGain,
    /// The stopping threshold was negative or non-finite.
    #[error("Högbom threshold must be finite and non-negative")]
    InvalidThreshold,
    /// The iteration bound was zero.
    #[error("Högbom iteration bound must be non-zero")]
    InvalidIterationBound,
    /// Summed multi-domain iteration evidence exceeded the representable count.
    #[error("multi-domain minor-cycle iteration count overflowed")]
    IterationCountOverflow,
    /// A bounded view supplied a non-positive or non-finite update envelope.
    #[error("bounded minor-cycle validity requires a finite positive update envelope")]
    InvalidValidityBound,
    /// Component-sequence recording requested a zero capacity.
    #[error("component-sequence recording requires a non-zero limit")]
    InvalidRecordingLimit,
    /// The reconstruction mask belongs to another problem, model, or normal state.
    #[error("reconstruction mask lineage does not match the minor-cycle input")]
    ForeignMask,
    /// The reconstruction mask shape differs from the normal-state plane.
    #[error("reconstruction mask shape differs from the normal-state plane")]
    MaskShapeMismatch,
    /// The reconstruction mask intersects no valid model support.
    #[error("reconstruction mask contains no valid model support")]
    EmptyValidSupport,
    /// The selected model-space plane does not match the normal-state geometry.
    #[error("selected model plane does not match the normal-state plane")]
    ModelShapeMismatch,
    /// A multi-channel state must execute through the shared reconstruction cycle.
    #[error("multi-channel normal state requires ReconstructionCycle")]
    ChannelCycleRequired,
    /// The named generation is not the view's final model generation.
    #[error("named generation is not the normal state's final model generation")]
    ForeignNormalState,
    /// The normal approximation lacked a positive finite peak.
    #[error("normal-state approximation lacks a positive finite PSF peak")]
    InvalidPsfPeak,
    /// The CASA-style fitted-Gaussian PSF sidelobe measurement failed.
    #[error(transparent)]
    PsfBeam(#[from] crate::PsfBeamFitError),
    /// Solver arithmetic produced a non-finite value.
    #[error("minor-cycle arithmetic generated a non-finite value")]
    GeneratedNonfinite,
    /// The model lifecycle owner rejected delta validation or minting.
    #[error(transparent)]
    Lifecycle(#[from] crate::ModelLifecycleError),
    /// Reconstruction-mask materialization failed.
    #[error(transparent)]
    Mask(#[from] crate::MaskError),
}

impl From<casa_imaging_model::ModelContractError> for MinorCycleError {
    fn from(error: casa_imaging_model::ModelContractError) -> Self {
        Self::Lifecycle(ModelLifecycleError::Contract(error))
    }
}

/// Run one reconstruction-owned Minor Cycle.
///
/// Consumes an immutable view of one authoritative Final Normal State and the
/// exact named final model generation, and returns the sparse Model Delta
/// minted through the model lifecycle owner plus solver evidence. Neither the
/// authoritative residual planes nor the model generation are mutated.
///
/// The loop preserves the casacore HOGBOM semantics: find the maximum-
/// absolute residual over the valid-support window, normalize by the PSF
/// peak, stop when below threshold, otherwise subtract `gain * strength`
/// scaled by the PSF centered on the peak and accumulate the same flux at the
/// peak cell. The current normal-state scalar convention uses the real plane,
/// matching the Float-image reference behavior; the typed model-plane
/// coordinate determines where the resulting sparse delta is accumulated.
///
/// # Errors
///
/// Fails closed on mismatched lineage or geometry, invalid controls or
/// windows, a degenerate PSF peak, non-finite arithmetic, and every model
/// owner rejection (term bounds, value bounds, or foreign lineage).
pub fn run_minor_cycle(
    lifecycle: &ModelLifecycle,
    base: &ModelGeneration,
    view: &FinalNormalState,
    mask: &ReconstructionMask,
    controls: MinorCycleProgram,
) -> Result<MinorCycleResult, MinorCycleError> {
    if view.catalog() == crate::NormalStateCatalog::UnnormalizedJointBlockV1 {
        return Err(MinorCycleError::CoupledMaskRequired);
    }
    if view.catalog() == crate::NormalStateCatalog::UnnormalizedTaylorBlockV1 {
        if controls.problem != Some(view.problem_id()) {
            return Err(MinorCycleError::CompiledProblemRequired);
        }
        return run_taylor_minor_cycle(lifecycle, base, view, mask, controls);
    }
    if view.channel_count() != 1 {
        return Err(MinorCycleError::ChannelCycleRequired);
    }
    run_minor_cycle_plane(
        lifecycle,
        base,
        view.polarization_plane(0, controls.model_plane().polarization())
            .ok_or(MinorCycleError::ModelShapeMismatch)?,
        mask,
        controls,
    )
}

struct ImageDomainHogbomWork<'a> {
    domain_ordinal: usize,
    shape: [usize; 2],
    psf: &'a [num_complex::Complex64],
    model_plane: MinorCycleModelPlane,
    psf_peak: f64,
    psf_peak_pixel: [usize; 2],
    valid_support: Box<[bool]>,
    residual: Vec<f64>,
    noise_rms: Option<f64>,
}

impl ImageDomainHogbomWork<'_> {
    fn candidate(&self) -> Option<(usize, f64)> {
        find_peak_abs(
            &self.residual,
            self.shape,
            |value| *value,
            |pixel| self.valid_support[pixel[0] * self.shape[1] + pixel[1]],
        )
        .map(|index| (index, self.residual[index] / self.psf_peak))
    }
}

/// Run one CASA-style image-domain Högbom set under per-field controllers.
///
/// Fields execute in canonical compiled-domain order. Each receives the same
/// per-set iteration budget and owns its own peak, noise threshold, and stop
/// decision. CASA derives one absolute cycle threshold from the set's global
/// entry peak and maximum PSF sidelobe, then applies it to every field.
/// Accepted terms from every controller are still minted atomically as one
/// model delta for the shared Major-Cycle lineage.
pub(crate) fn run_image_domain_minor_cycle(
    lifecycle: &ModelLifecycle,
    base: &ModelGeneration,
    view: &FinalNormalState,
    masks: &ImageDomainReconstructionMasks,
    controls: MinorCycleProgram,
) -> Result<MinorCycleResult, MinorCycleError> {
    if !matches!(controls.algorithm(), ReconstructionAlgorithm::Hogbom)
        || view.catalog() != crate::NormalStateCatalog::UnnormalizedPlaneV1
        || view.channel_count() != 1
    {
        return Err(MinorCycleError::UnsupportedImageDomainCycle);
    }
    if view.domain_count() != masks.len()
        || view.domain_count() != base.shape().domains().len()
        || controls.model_plane().coefficient() != 0
        || controls.model_plane().polarization() >= base.shape().polarizations()
    {
        return Err(MinorCycleError::ModelShapeMismatch);
    }
    if base.generation_id() != view.final_model_generation() {
        return Err(MinorCycleError::ForeignNormalState);
    }

    let mut work = Vec::with_capacity(view.domain_count());
    let mut maximum_sidelobe = 0.0_f64;
    for (domain, mask) in view.domains().zip(masks.iter()) {
        let plane = domain
            .polarization_plane(0, controls.model_plane().polarization())
            .ok_or(MinorCycleError::ModelShapeMismatch)?;
        let shape = plane.shape();
        let model_plane =
            MinorCycleModelPlane::new(domain.ordinal(), 0, controls.model_plane().polarization());
        if base
            .shape()
            .domains()
            .get(domain.ordinal())
            .is_none_or(|model_domain| model_domain.pixels() != shape)
            || mask.shape() != shape
        {
            return Err(MinorCycleError::ModelShapeMismatch);
        }
        if mask.problem_id() != view.problem_id()
            || mask.model_generation() != base.generation_id()
            || mask
                .normal_state_completion()
                .is_some_and(|completion| completion != view.completion_id())
        {
            return Err(MinorCycleError::ForeignMask);
        }
        let psf_peak_index = find_peak_abs(
            plane.normal_approximation(),
            shape,
            |value| value.re,
            |_| true,
        )
        .ok_or(MinorCycleError::InvalidPsfPeak)?;
        let psf_peak = plane.normal_approximation()[psf_peak_index].re;
        if !psf_peak.is_finite() || psf_peak <= 0.0 {
            return Err(MinorCycleError::InvalidPsfPeak);
        }
        let mut residual = Vec::with_capacity(shape[0] * shape[1]);
        for value in plane.residual() {
            if !value.re.is_finite() {
                return Err(MinorCycleError::GeneratedNonfinite);
            }
            residual.push(value.re);
        }
        let noise_rms = controls
            .noise_sigma()
            .map(|_| robust_masked_rms(&residual, shape, base, model_plane, mask))
            .transpose()?
            .map(|rms| rms / psf_peak);
        let psf = plane
            .normal_approximation()
            .iter()
            .map(|value| value.re as f32)
            .collect::<Vec<_>>();
        maximum_sidelobe = maximum_sidelobe.max(crate::fitted_psf_sidelobe_fraction(&psf, shape)?);
        let valid_support = (0..shape[0] * shape[1])
            .map(|index| {
                let pixel = plane_pixel(index, shape);
                mask.contains(pixel) && valid_support(base, shape, model_plane, pixel)
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        work.push(ImageDomainHogbomWork {
            domain_ordinal: domain.ordinal(),
            shape,
            psf: plane.normal_approximation(),
            model_plane,
            psf_peak,
            psf_peak_pixel: plane_pixel(psf_peak_index, shape),
            valid_support,
            residual,
            noise_rms,
        });
    }

    let global_initial_peak = work
        .iter()
        .filter_map(ImageDomainHogbomWork::candidate)
        .map(|(_, strength)| strength.abs())
        .fold(0.0_f64, f64::max);
    let shared_cycle_threshold = controls
        .fixed_cycle_threshold
        .or_else(|| controls.cycle_threshold_for(global_initial_peak, maximum_sidelobe));
    let mut terms = BTreeMap::<ModelCell, f64>::new();
    let mut recorded = Vec::with_capacity(
        controls
            .component_sequence_limit()
            .unwrap_or(0)
            .min(controls.actual_iteration_limit().saturating_mul(work.len())),
    );
    let domain_runs = run_image_domain_hogbom_controllers(
        &mut work,
        &controls,
        shared_cycle_threshold,
        &mut terms,
        &mut recorded,
    )?;
    let iterations = domain_runs
        .iter()
        .try_fold(0_usize, |total, run| total.checked_add(run.iterations))
        .ok_or(MinorCycleError::IterationCountOverflow)?;
    let controller_iterations = domain_runs
        .iter()
        .try_fold(0_usize, |total, run| {
            total.checked_add(run.controller_iterations)
        })
        .ok_or(MinorCycleError::IterationCountOverflow)?;
    let total_flux = domain_runs.iter().map(|run| run.total_flux).sum::<f64>();
    if !total_flux.is_finite() {
        return Err(MinorCycleError::GeneratedNonfinite);
    }
    let initial_peak_flux = domain_runs
        .iter()
        .map(|run| run.initial_peak_flux)
        .fold(0.0_f64, f64::max);
    let final_peak_flux = domain_runs
        .iter()
        .map(|run| run.final_peak_flux)
        .fold(0.0_f64, f64::max);
    let maximum_noise_rms = domain_runs
        .iter()
        .filter_map(|run| run.noise_rms)
        .reduce(f64::max);
    let global_threshold = domain_runs
        .iter()
        .map(|run| run.global_threshold)
        .fold(0.0_f64, f64::max);
    let effective_threshold = domain_runs
        .iter()
        .map(|run| run.effective_threshold)
        .fold(0.0_f64, f64::max);
    let cycle_threshold = domain_runs
        .iter()
        .filter_map(|run| run.cycle_threshold)
        .reduce(f64::max);
    let stop_reason = domain_runs
        .iter()
        .map(|run| run.stop_reason)
        .max_by_key(|reason| image_domain_stop_priority(*reason))
        .unwrap_or(MinorCycleStopReason::ThresholdReached);
    terms.retain(|_, flux| *flux != 0.0);
    let delta = if terms.is_empty() {
        None
    } else {
        let mut deltas = terms
            .iter()
            .map(|(cell, flux)| {
                let flat = base
                    .shape()
                    .flat_index(*cell)
                    .ok_or(MinorCycleError::ModelShapeMismatch)?;
                Ok((flat, ModelDeltaTerm::new(*cell, ModelValue::new(*flux)?)))
            })
            .collect::<Result<Vec<_>, MinorCycleError>>()?;
        deltas.sort_unstable_by_key(|(flat, _)| *flat);
        Some(lifecycle.compile_delta(base, deltas.into_iter().map(|(_, term)| term))?)
    };
    let mask_generations = masks
        .iter()
        .map(ReconstructionMask::generation_id)
        .collect::<Vec<_>>();
    let evidence_id = minor_cycle_evidence_id(
        lifecycle.authority(),
        lifecycle.attempt(),
        lifecycle.epoch(),
        base.generation_id(),
        view.completion_id(),
        view.content_identity(),
        &mask_generations,
        &controls,
        iterations,
        controller_iterations,
        total_flux,
        final_peak_flux,
        maximum_noise_rms,
        effective_threshold,
        stop_reason,
        None,
        0,
        Some(&domain_runs),
    );
    Ok(MinorCycleResult {
        delta,
        evidence: MinorCycleEvidence {
            evidence_id,
            problem: lifecycle.problem(),
            attempt: lifecycle.attempt(),
            epoch: lifecycle.epoch(),
            input_generation: base.generation_id(),
            normal_state_completion: view.completion_id(),
            normal_state_content: view.content_identity(),
            iterations,
            controller_iterations,
            total_flux,
            initial_peak_flux,
            final_peak_flux,
            noise_rms: maximum_noise_rms,
            global_threshold,
            effective_threshold,
            cycle_threshold,
            stop_reason,
            clark_approximation: None,
            clark_refreshes: 0,
            recorded: (!recorded.is_empty()).then(|| recorded.into_boxed_slice()),
            image_domain_runs: Some(domain_runs.into_boxed_slice()),
        },
    })
}

fn run_image_domain_hogbom_controllers(
    work: &mut [ImageDomainHogbomWork<'_>],
    controls: &MinorCycleProgram,
    shared_cycle_threshold: Option<f64>,
    terms: &mut BTreeMap<ModelCell, f64>,
    recorded: &mut Vec<MinorCycleComponent>,
) -> Result<Vec<ImageDomainMinorCycleEvidence>, MinorCycleError> {
    let mut outcomes = Vec::with_capacity(work.len());
    for domain in work {
        let initial_peak_flux = domain.candidate().map_or(0.0, |(_, value)| value.abs());
        let global_threshold = domain
            .noise_rms
            .zip(controls.noise_sigma())
            .map_or(controls.threshold(), |(rms, sigma)| {
                controls.threshold().max(rms * sigma)
            });
        let cycle_threshold = shared_cycle_threshold;
        let effective_threshold = cycle_threshold.map_or(global_threshold, |threshold| {
            global_threshold.max(threshold)
        });
        let mut controller =
            MinorCycleController::new(controls, effective_threshold, domain.candidate().is_some());
        for _ in 0..controller.iteration_limit() {
            let Some((peak_index, strength)) = domain.candidate() else {
                controller.stop(MinorCycleStopReason::ThresholdReached);
                break;
            };
            if !strength.is_finite() {
                return Err(MinorCycleError::GeneratedNonfinite);
            }
            let flux = controls.gain() * strength;
            if !flux.is_finite() {
                return Err(MinorCycleError::GeneratedNonfinite);
            }
            if !controller.admit(strength, flux.abs(), false) {
                break;
            }
            let peak_pixel = plane_pixel(peak_index, domain.shape);
            subtract_psf(
                &mut domain.residual,
                domain.psf,
                domain.shape,
                peak_pixel,
                domain.psf_peak_pixel,
                flux,
            )?;
            controller.accepted(flux.abs());
            let cell = model_cell(domain.model_plane, domain.shape, peak_pixel)
                .expect("selected domain candidate lies inside its model plane");
            *terms.entry(cell).or_insert(0.0) += flux;
            if let Some(limit) = controls.component_sequence_limit()
                && recorded.len() < limit
            {
                recorded.push(MinorCycleComponent {
                    cell,
                    flux,
                    scale_px: 0.0,
                });
            }
        }
        let (iterations, total_flux, stop_reason) = controller.finish();
        outcomes.push(ImageDomainMinorCycleEvidence {
            domain_ordinal: domain.domain_ordinal,
            iterations,
            controller_iterations: controls.controller_iterations(iterations, stop_reason),
            total_flux,
            initial_peak_flux,
            final_peak_flux: domain.candidate().map_or(0.0, |(_, value)| value.abs()),
            noise_rms: domain.noise_rms,
            global_threshold,
            effective_threshold,
            cycle_threshold,
            stop_reason,
        });
    }
    Ok(outcomes)
}

const fn image_domain_stop_priority(reason: MinorCycleStopReason) -> u8 {
    match reason {
        MinorCycleStopReason::ThresholdReached => 0,
        MinorCycleStopReason::IterationBound => 1,
        MinorCycleStopReason::StalenessBound => 2,
        MinorCycleStopReason::MultiscaleDivergence => 3,
    }
}

/// Run one atomic joint continuum-plus-line Minor Cycle.
///
/// The two spatial supports remain distinct while every admitted component is
/// solved against the applicable principal sub-block of the same dense normal
/// operator. The returned delta contains both coefficient families and is
/// minted once through the shared model lifecycle.
pub fn run_joint_minor_cycle(
    lifecycle: &ModelLifecycle,
    base: &ModelGeneration,
    view: &FinalNormalState,
    masks: &CoupledReconstructionMask,
    controls: MinorCycleProgram,
) -> Result<MinorCycleResult, MinorCycleError> {
    if view.catalog() != crate::NormalStateCatalog::UnnormalizedJointBlockV1 {
        return Err(MinorCycleError::InvalidNormalStateCatalog);
    }
    if controls.problem != Some(view.problem_id()) {
        return Err(MinorCycleError::CompiledProblemRequired);
    }
    run_joint_block_minor_cycle(lifecycle, base, view, masks, controls)
}

#[allow(clippy::too_many_lines)]
fn run_joint_block_minor_cycle(
    lifecycle: &ModelLifecycle,
    base: &ModelGeneration,
    view: &FinalNormalState,
    masks: &CoupledReconstructionMask,
    controls: MinorCycleProgram,
) -> Result<MinorCycleResult, MinorCycleError> {
    let ReconstructionAlgorithm::JointContinuumLine {
        scales_px,
        small_scale_bias,
    } = controls.algorithm()
    else {
        return Err(MinorCycleError::InvalidNormalStateCatalog);
    };
    let shape = view.shape();
    let terms_count = view.coefficient_term_count();
    let continuum_terms = view
        .joint_continuum_term_count()
        .ok_or(MinorCycleError::InvalidNormalStateCatalog)?;
    let primary = controls.model_plane();
    if continuum_terms == 0
        || continuum_terms >= terms_count
        || view.normal_moment_count() != terms_count * terms_count
        || base.shape().coefficients() != terms_count
        || base
            .shape()
            .domains()
            .get(primary.domain())
            .is_none_or(|domain| domain.pixels() != shape)
        || primary.coefficient() != 0
        || primary.polarization() >= base.shape().polarizations()
        || base.samples().len() != base.shape().sample_count()
    {
        return Err(MinorCycleError::ModelShapeMismatch);
    }
    if base.generation_id() != view.final_model_generation() {
        return Err(MinorCycleError::ForeignNormalState);
    }
    validate_joint_mask(masks.continuum(), view, base, shape)?;
    validate_joint_mask(masks.line(), view, base, shape)?;

    let effective_scales = scales_px
        .iter()
        .copied()
        .filter(|scale| *scale <= (shape[0] / 2) as f64 && *scale <= (shape[1] / 2) as f64)
        .collect::<Vec<_>>();
    if effective_scales.is_empty() {
        return Err(MinorCycleError::InvalidScale);
    }
    let moment_zero = view
        .normal_block(0, 0)
        .ok_or(MinorCycleError::ModelShapeMismatch)?;
    let psf_peak_index = taylor_psf_peak_index(
        moment_zero.normal_approximation(),
        shape,
        effective_scales.last().copied().unwrap_or(0.0),
    )
    .ok_or(MinorCycleError::InvalidPsfPeak)?;
    let psf_peak = moment_zero.normal_approximation()[psf_peak_index].re;
    if !psf_peak.is_finite() || psf_peak <= 0.0 {
        return Err(MinorCycleError::InvalidPsfPeak);
    }
    let psf_peak_pixel = plane_pixel(psf_peak_index, shape);
    let psf_support = taylor_psf_support(shape, effective_scales.last().copied().unwrap_or(0.0));
    let kernels = build_scale_kernels(&effective_scales, *small_scale_bias);
    let systems = build_joint_scale_systems(
        view,
        shape,
        psf_peak_pixel,
        &kernels,
        continuum_terms,
        controls
            .maximum_condition_number
            .ok_or(MinorCycleError::CompiledProblemRequired)?,
    )?;
    let mut residuals = (0..terms_count)
        .map(|term| {
            view.coefficient_term(term)
                .ok_or(MinorCycleError::ModelShapeMismatch)?
                .residual()
                .iter()
                .map(|value| {
                    value
                        .re
                        .is_finite()
                        .then_some(value.re)
                        .ok_or(MinorCycleError::GeneratedNonfinite)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, MinorCycleError>>()?;
    let mut initial_candidate = select_joint_candidate(
        &residuals,
        shape,
        base,
        primary,
        continuum_terms,
        masks,
        &kernels,
        &systems,
        None,
    );
    if initial_candidate.is_none() {
        return finish_taylor_minor_cycle(
            lifecycle,
            base,
            view,
            masks.continuum(),
            Some(masks.line()),
            controls,
            0,
            0.0,
            0.0,
            0.0,
            None,
            0.0,
            None,
            MinorCycleStopReason::ThresholdReached,
            BTreeMap::new(),
            Vec::new(),
        );
    }
    let initial_peak = joint_candidate_peak(initial_candidate.as_ref().expect("checked"));
    let noise_rms = controls
        .noise_sigma()
        .map(|_| {
            robust_masked_rms(
                &residuals[0],
                shape,
                base,
                MinorCycleModelPlane::new(primary.domain(), 0, primary.polarization()),
                masks.continuum(),
            )
            .map(|rms| rms / psf_peak)
        })
        .transpose()?;
    let global_threshold = noise_rms
        .zip(controls.noise_sigma())
        .map_or(controls.threshold(), |(rms, sigma)| {
            controls.threshold().max(rms * sigma)
        });
    let cycle_threshold = if controls.fixed_cycle_threshold.is_some() {
        controls.fixed_cycle_threshold
    } else if controls.cycle_threshold.is_some() {
        let psf = moment_zero
            .normal_approximation()
            .iter()
            .map(|value| value.re as f32)
            .collect::<Vec<_>>();
        let sidelobe = crate::fitted_psf_sidelobe_fraction(&psf, shape)?;
        controls.cycle_threshold_for(initial_peak, sidelobe)
    } else {
        None
    };
    let effective_threshold =
        cycle_threshold.map_or(global_threshold, |value| value.max(global_threshold));
    let mut model_terms = BTreeMap::<usize, f64>::new();
    let mut recorded = Vec::new();
    let mut controller = MinorCycleController::new(&controls, effective_threshold, true);
    let mut search_window = None;

    for _ in 0..controller.iteration_limit() {
        let candidate = initial_candidate.take().or_else(|| {
            select_joint_candidate(
                &residuals,
                shape,
                base,
                primary,
                continuum_terms,
                masks,
                &kernels,
                &systems,
                search_window,
            )
        });
        let Some(candidate) = candidate else {
            controller.stop(MinorCycleStopReason::ThresholdReached);
            break;
        };
        let current_peak = joint_candidate_peak(&candidate);
        let updates = candidate
            .coefficients
            .iter()
            .map(|value| controls.gain() * value)
            .collect::<Vec<_>>();
        if updates.iter().any(|value| !value.is_finite()) {
            return Err(MinorCycleError::GeneratedNonfinite);
        }
        let charged = updates.iter().map(|value| value.abs()).sum::<f64>();
        if !controller.admit(current_peak, charged, false) {
            break;
        }
        let pixel = plane_pixel(candidate.index, shape);
        let kernel = &kernels[candidate.scale_index];
        for (residual_term, residual) in residuals.iter_mut().enumerate() {
            for (coefficient, update) in updates.iter().enumerate() {
                if *update == 0.0 {
                    continue;
                }
                let psf = view
                    .normal_block(residual_term, coefficient)
                    .ok_or(MinorCycleError::ModelShapeMismatch)?;
                subtract_scaled_psf(
                    residual,
                    psf.normal_approximation(),
                    shape,
                    pixel,
                    psf_peak_pixel,
                    kernel,
                    *update,
                )?;
            }
        }
        for (coefficient, update) in updates.into_iter().enumerate() {
            if update == 0.0 {
                continue;
            }
            let model_plane =
                MinorCycleModelPlane::new(primary.domain(), coefficient, primary.polarization());
            add_scaled_terms(
                &mut model_terms,
                base,
                model_plane,
                shape,
                pixel,
                kernel,
                update,
            );
            if controls
                .component_sequence_limit()
                .is_some_and(|limit| recorded.len() < limit)
            {
                recorded.push(MinorCycleComponent {
                    cell: model_cell(model_plane, shape, pixel)
                        .expect("selected joint pixel is valid"),
                    flux: update,
                    scale_px: kernel.scale_px,
                });
            }
        }
        search_window = Some(TaylorSearchWindow::around(pixel, psf_support, shape));
        controller.accepted(charged);
    }
    if !model_terms.is_empty() {
        refresh_taylor_residuals(
            &mut residuals,
            view,
            shape,
            psf_peak_pixel,
            base,
            &model_terms,
        )?;
    }
    let final_peak = select_joint_candidate(
        &residuals,
        shape,
        base,
        primary,
        continuum_terms,
        masks,
        &kernels,
        &systems,
        None,
    )
    .as_ref()
    .map_or(0.0, joint_candidate_peak);
    let (iterations, total_flux, stop_reason) = controller.finish();
    finish_taylor_minor_cycle(
        lifecycle,
        base,
        view,
        masks.continuum(),
        Some(masks.line()),
        controls,
        iterations,
        total_flux,
        initial_peak,
        final_peak,
        noise_rms,
        global_threshold,
        cycle_threshold,
        stop_reason,
        model_terms,
        recorded,
    )
}

fn validate_joint_mask(
    mask: &ReconstructionMask,
    view: &FinalNormalState,
    base: &ModelGeneration,
    shape: [usize; 2],
) -> Result<(), MinorCycleError> {
    if mask.shape() != shape {
        return Err(MinorCycleError::MaskShapeMismatch);
    }
    if mask.problem_id() != view.problem_id()
        || mask.model_generation() != base.generation_id()
        || mask
            .normal_state_completion()
            .is_some_and(|id| id != view.completion_id())
    {
        return Err(MinorCycleError::ForeignMask);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn run_taylor_minor_cycle(
    lifecycle: &ModelLifecycle,
    base: &ModelGeneration,
    view: &FinalNormalState,
    mask: &ReconstructionMask,
    controls: MinorCycleProgram,
) -> Result<MinorCycleResult, MinorCycleError> {
    let ReconstructionAlgorithm::Mtmfs {
        scales_px,
        small_scale_bias,
    } = controls.algorithm()
    else {
        return Err(MinorCycleError::InvalidNormalStateCatalog);
    };
    let shape = view.shape();
    let cells = shape[0] * shape[1];
    let terms_count = view.coefficient_term_count();
    let primary = controls.model_plane();
    if terms_count < 2
        || view.normal_moment_count() != 2 * terms_count - 1
        || base.shape().coefficients() != terms_count
        || base
            .shape()
            .domains()
            .get(primary.domain())
            .is_none_or(|domain| domain.pixels() != shape)
        || primary.coefficient() != 0
        || primary.polarization() >= base.shape().polarizations()
        || base.samples().len() != base.shape().sample_count()
    {
        return Err(MinorCycleError::ModelShapeMismatch);
    }
    if base.generation_id() != view.final_model_generation() {
        return Err(MinorCycleError::ForeignNormalState);
    }
    if mask.shape() != shape {
        return Err(MinorCycleError::MaskShapeMismatch);
    }
    if mask.problem_id() != view.problem_id()
        || mask.model_generation() != base.generation_id()
        || mask
            .normal_state_completion()
            .is_some_and(|id| id != view.completion_id())
    {
        return Err(MinorCycleError::ForeignMask);
    }

    let moment_zero = view
        .normal_moment(0)
        .ok_or(MinorCycleError::ModelShapeMismatch)?;
    let effective_scales = scales_px
        .iter()
        .copied()
        .filter(|scale| *scale <= (shape[0] / 2) as f64 && *scale <= (shape[1] / 2) as f64)
        .collect::<Vec<_>>();
    if effective_scales.is_empty() {
        return Err(MinorCycleError::InvalidScale);
    }
    let psf_peak_index = taylor_psf_peak_index(
        moment_zero.normal_approximation(),
        shape,
        effective_scales.last().copied().unwrap_or(0.0),
    )
    .ok_or(MinorCycleError::InvalidPsfPeak)?;
    let psf_peak = moment_zero.normal_approximation()[psf_peak_index].re;
    if !psf_peak.is_finite() || psf_peak <= 0.0 {
        return Err(MinorCycleError::InvalidPsfPeak);
    }
    let psf_peak_pixel = plane_pixel(psf_peak_index, shape);
    let psf_support = taylor_psf_support(shape, effective_scales.last().copied().unwrap_or(0.0));
    let kernels = build_scale_kernels(&effective_scales, *small_scale_bias);
    let scale_systems = build_taylor_scale_systems(view, shape, psf_peak_pixel, &kernels)?;

    let mut residuals = (0..terms_count)
        .map(|term| {
            view.coefficient_term(term)
                .ok_or(MinorCycleError::ModelShapeMismatch)?
                .residual()
                .iter()
                .map(|value| {
                    value
                        .re
                        .is_finite()
                        .then_some(value.re)
                        .ok_or(MinorCycleError::GeneratedNonfinite)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, MinorCycleError>>()?;
    if imaging_science_trace_enabled() {
        for (term, residual) in residuals.iter().enumerate() {
            let label = match term {
                0 => "minor_residual_tt0_enter",
                1 => "minor_residual_tt1_enter",
                _ => "minor_residual_tt_other_enter",
            };
            trace_real_values(label, residual);
        }
    }
    let primary_plane = MinorCycleModelPlane::new(primary.domain(), 0, primary.polarization());
    let has_valid_support = (0..cells).any(|index| {
        let pixel = plane_pixel(index, shape);
        mask.contains(pixel) && valid_taylor_support(base, shape, primary, terms_count, pixel)
    });
    if !has_valid_support {
        return finish_taylor_minor_cycle(
            lifecycle,
            base,
            view,
            mask,
            None,
            controls,
            0,
            0.0,
            0.0,
            0.0,
            None,
            0.0,
            None,
            MinorCycleStopReason::ThresholdReached,
            BTreeMap::new(),
            Vec::new(),
        );
    }

    let initial_peak = principal_taylor_peak(
        &residuals[0],
        shape,
        base,
        primary,
        terms_count,
        mask,
        &kernels[0],
    ) / scale_systems[0].h00;
    let noise_rms = controls
        .noise_sigma()
        .map(|_| {
            robust_masked_rms(&residuals[0], shape, base, primary_plane, mask)
                .map(|rms| rms / scale_systems[0].h00)
        })
        .transpose()?;
    let global_threshold = noise_rms
        .zip(controls.noise_sigma())
        .map_or(controls.threshold(), |(rms, sigma)| {
            controls.threshold().max(rms * sigma)
        });
    let cycle_threshold = if controls.fixed_cycle_threshold.is_some() {
        controls.fixed_cycle_threshold
    } else if controls.cycle_threshold.is_some() {
        let psf = moment_zero
            .normal_approximation()
            .iter()
            .map(|v| v.re as f32)
            .collect::<Vec<_>>();
        let sidelobe = crate::fitted_psf_sidelobe_fraction(&psf, shape)?;
        controls.cycle_threshold_for(initial_peak, sidelobe)
    } else {
        None
    };
    let effective_threshold =
        cycle_threshold.map_or(global_threshold, |value| value.max(global_threshold));
    let mut model_terms = BTreeMap::<usize, f64>::new();
    let mut recorded = Vec::new();
    let mut controller = MinorCycleController::new(&controls, effective_threshold, true);
    let mut search_window = None;

    for _ in 0..controller.iteration_limit() {
        let current_peak = principal_taylor_peak(
            &residuals[0],
            shape,
            base,
            primary,
            terms_count,
            mask,
            &kernels[0],
        ) / scale_systems[0].h00;
        if controller.iterations() > 0 && current_peak > initial_peak * 1.5 {
            controller.stop(MinorCycleStopReason::MultiscaleDivergence);
            break;
        }
        let candidate = select_taylor_candidate(
            &residuals,
            shape,
            base,
            primary,
            terms_count,
            mask,
            &kernels,
            &scale_systems,
            search_window,
        )
        .ok_or(MinorCycleError::EmptyValidSupport)?;
        let updates = candidate
            .coefficients
            .iter()
            .map(|value| controls.gain() * value)
            .collect::<Vec<_>>();
        if updates.iter().any(|value| !value.is_finite()) {
            return Err(MinorCycleError::GeneratedNonfinite);
        }
        let charged = updates.iter().map(|value| value.abs()).sum::<f64>();
        if !controller.admit(current_peak, charged, false) {
            break;
        }
        let pixel = plane_pixel(candidate.index, shape);
        let kernel = &kernels[candidate.scale_index];
        for (residual_term, residual) in residuals.iter_mut().enumerate() {
            for (coefficient, update) in updates.iter().enumerate() {
                let psf = view
                    .normal_block(residual_term, coefficient)
                    .ok_or(MinorCycleError::ModelShapeMismatch)?;
                subtract_scaled_psf(
                    residual,
                    psf.normal_approximation(),
                    shape,
                    pixel,
                    psf_peak_pixel,
                    kernel,
                    *update,
                )?;
            }
        }
        for (coefficient, update) in updates.into_iter().enumerate() {
            let model_plane =
                MinorCycleModelPlane::new(primary.domain(), coefficient, primary.polarization());
            add_scaled_terms(
                &mut model_terms,
                base,
                model_plane,
                shape,
                pixel,
                kernel,
                update,
            );
            if controls
                .component_sequence_limit()
                .is_some_and(|limit| recorded.len() < limit)
            {
                recorded.push(MinorCycleComponent {
                    cell: model_cell(model_plane, shape, pixel)
                        .expect("selected Taylor pixel is valid"),
                    flux: update,
                    scale_px: kernel.scale_px,
                });
            }
        }
        search_window = Some(TaylorSearchWindow::around(pixel, psf_support, shape));
        controller.accepted(charged);
    }
    if !model_terms.is_empty() {
        refresh_taylor_residuals(
            &mut residuals,
            view,
            shape,
            psf_peak_pixel,
            base,
            &model_terms,
        )?;
    }
    let final_peak = principal_taylor_peak(
        &residuals[0],
        shape,
        base,
        primary,
        terms_count,
        mask,
        &kernels[0],
    ) / scale_systems[0].h00;
    let (iterations, total_flux, stop_reason) = controller.finish();
    finish_taylor_minor_cycle(
        lifecycle,
        base,
        view,
        mask,
        None,
        controls,
        iterations,
        total_flux,
        initial_peak,
        final_peak,
        noise_rms,
        global_threshold,
        cycle_threshold,
        stop_reason,
        model_terms,
        recorded,
    )
}

#[allow(clippy::too_many_arguments)]
fn finish_taylor_minor_cycle(
    lifecycle: &ModelLifecycle,
    base: &ModelGeneration,
    view: &FinalNormalState,
    mask: &ReconstructionMask,
    secondary_mask: Option<&ReconstructionMask>,
    controls: MinorCycleProgram,
    iterations: usize,
    total_flux: f64,
    initial_peak_flux: f64,
    final_peak_flux: f64,
    noise_rms: Option<f64>,
    global_threshold: f64,
    cycle_threshold: Option<f64>,
    stop_reason: MinorCycleStopReason,
    mut terms: BTreeMap<usize, f64>,
    recorded: Vec<MinorCycleComponent>,
) -> Result<MinorCycleResult, MinorCycleError> {
    terms.retain(|_, value| *value != 0.0);
    if imaging_science_trace_enabled() {
        let mut digests = (0..base.shape().coefficients())
            .map(|coefficient| {
                let label = match coefficient {
                    0 => "minor_model_tt0_leave",
                    1 => "minor_model_tt1_leave",
                    _ => "minor_model_tt_other_leave",
                };
                (label, ScienceTraceDigest::new())
            })
            .collect::<Vec<_>>();
        for (flat, value) in &terms {
            let cell = base
                .shape()
                .cell_at(*flat)
                .ok_or(MinorCycleError::ModelShapeMismatch)?;
            digests[cell.coefficient()]
                .1
                .push_indexed_real(*flat, *value);
        }
        for (label, digest) in digests {
            digest.emit(label);
        }
    }
    let delta = if terms.is_empty() {
        None
    } else {
        let values = terms
            .iter()
            .map(|(flat, value)| {
                let cell = base
                    .shape()
                    .cell_at(*flat)
                    .ok_or(MinorCycleError::ModelShapeMismatch)?;
                Ok(ModelDeltaTerm::new(cell, ModelValue::new(*value)?))
            })
            .collect::<Result<Vec<_>, MinorCycleError>>()?;
        Some(lifecycle.compile_delta(base, values)?)
    };
    let effective_threshold =
        cycle_threshold.map_or(global_threshold, |value| value.max(global_threshold));
    let controller_iterations = controls.controller_iterations(iterations, stop_reason);
    let mask_generations = secondary_mask.map_or_else(
        || vec![mask.generation_id()],
        |secondary| vec![mask.generation_id(), secondary.generation_id()],
    );
    let evidence_id = minor_cycle_evidence_id(
        lifecycle.authority(),
        lifecycle.attempt(),
        lifecycle.epoch(),
        base.generation_id(),
        view.completion_id(),
        view.content_identity(),
        &mask_generations,
        &controls,
        iterations,
        controller_iterations,
        total_flux,
        final_peak_flux,
        noise_rms,
        effective_threshold,
        stop_reason,
        None,
        0,
        None,
    );
    Ok(MinorCycleResult {
        delta,
        evidence: MinorCycleEvidence {
            evidence_id,
            problem: lifecycle.problem(),
            attempt: lifecycle.attempt(),
            epoch: lifecycle.epoch(),
            input_generation: base.generation_id(),
            normal_state_completion: view.completion_id(),
            normal_state_content: view.content_identity(),
            iterations,
            controller_iterations,
            total_flux,
            initial_peak_flux,
            final_peak_flux,
            noise_rms,
            global_threshold,
            effective_threshold,
            cycle_threshold,
            stop_reason,
            clark_approximation: None,
            clark_refreshes: 0,
            recorded: (!recorded.is_empty()).then(|| recorded.into_boxed_slice()),
            image_domain_runs: None,
        },
    })
}

pub(crate) fn run_minor_cycle_plane(
    lifecycle: &ModelLifecycle,
    base: &ModelGeneration,
    plane: FinalNormalStatePlane<'_>,
    mask: &ReconstructionMask,
    controls: MinorCycleProgram,
) -> Result<MinorCycleResult, MinorCycleError> {
    let view = plane.owner();
    let shape = view.shape();
    let cells = shape[0] * shape[1];
    let model_plane = controls.model_plane();
    if base
        .shape()
        .domains()
        .get(model_plane.domain())
        .is_none_or(|domain| domain.pixels() != shape)
        || model_plane.coefficient() >= base.shape().coefficients()
        || model_plane.polarization() >= base.shape().polarizations()
        || base.samples().len() != base.shape().sample_count()
    {
        return Err(MinorCycleError::ModelShapeMismatch);
    }
    if base.generation_id() != view.final_model_generation() {
        return Err(MinorCycleError::ForeignNormalState);
    }
    if mask.shape() != shape {
        return Err(MinorCycleError::MaskShapeMismatch);
    }
    if mask.problem_id() != view.problem_id()
        || mask.model_generation() != base.generation_id()
        || mask
            .normal_state_completion()
            .is_some_and(|completion| completion != view.completion_id())
    {
        return Err(MinorCycleError::ForeignMask);
    }

    // The PSF peak normalization follows the reference cleaner: peaks are
    // reported in model units regardless of the accumulated weight scale.
    let psf_peak_index = find_peak_abs(plane.normal_approximation(), shape, |v| v.re, |_| true)
        .ok_or(MinorCycleError::InvalidPsfPeak)?;
    let psf_peak = plane.normal_approximation()[psf_peak_index].re;
    if !psf_peak.is_finite() || psf_peak <= 0.0 {
        return Err(MinorCycleError::InvalidPsfPeak);
    }
    let psf_peak_pixel = plane_pixel(psf_peak_index, shape);

    let clark = match controls.algorithm() {
        ReconstructionAlgorithm::Hogbom => None,
        ReconstructionAlgorithm::Clark => Some(derive_clark_approximation(
            plane.normal_approximation(),
            shape,
            psf_peak_pixel,
        )?),
        ReconstructionAlgorithm::Multiscale { .. } => None,
        ReconstructionAlgorithm::Mtmfs { .. } => {
            return Err(MinorCycleError::InvalidNormalStateCatalog);
        }
        _ => return Err(MinorCycleError::UnsupportedAlgorithm),
    };

    // Private working copy: authoritative state is never mutated.
    let mut residual = Vec::with_capacity(cells);
    for value in plane.residual() {
        let real = value.re;
        if !real.is_finite() {
            return Err(MinorCycleError::GeneratedNonfinite);
        }
        residual.push(real);
    }
    let noise_rms = controls
        .noise_sigma()
        .map(|_| {
            robust_masked_rms(&residual, shape, base, model_plane, mask).map(|rms| rms / psf_peak)
        })
        .transpose()?;
    let global_threshold = noise_rms
        .zip(controls.noise_sigma())
        .map_or(controls.threshold(), |(rms, sigma)| {
            controls.threshold().max(rms * sigma)
        });
    let initial_peak = residual
        .iter()
        .fold(0.0_f64, |peak, value| peak.max(value.abs()))
        / psf_peak;
    let cycle_threshold = if controls.fixed_cycle_threshold.is_some() {
        controls.fixed_cycle_threshold
    } else if let Some(cycle) = controls.cycle_threshold {
        let psf = plane
            .normal_approximation()
            .iter()
            .map(|value| value.re as f32)
            .collect::<Vec<_>>();
        let maximum_sidelobe = crate::fitted_psf_sidelobe_fraction(&psf, shape)?;
        Some(
            initial_peak
                * (cycle.factor * maximum_sidelobe)
                    .clamp(cycle.minimum_psf_fraction, cycle.maximum_psf_fraction),
        )
    } else {
        None
    };
    let effective_threshold = cycle_threshold.map_or(global_threshold, |threshold| {
        global_threshold.max(threshold)
    });
    let mut clark_state = clark.map(|approximation| {
        let initial_peak = residual
            .iter()
            .fold(0.0_f64, |peak, value| peak.max(value.abs()))
            / psf_peak;
        let cutoff = (initial_peak * approximation.maximum_exterior_sidelobe / psf_peak / 3.0)
            .max(effective_threshold);
        let active = residual
            .iter()
            .map(|value| value.abs() / psf_peak >= cutoff)
            .collect::<Vec<_>>();
        ClarkWorkState {
            cutoff,
            active,
            refreshes: 0,
        }
    });
    let multiscale = match controls.algorithm() {
        ReconstructionAlgorithm::Multiscale {
            scales_px,
            small_scale_bias,
        } => Some(build_scale_kernels(scales_px, *small_scale_bias)),
        _ => None,
    };

    let mut terms = BTreeMap::<usize, f64>::new();
    let mut recorded = Vec::with_capacity(
        controls
            .component_sequence_limit()
            .unwrap_or(0)
            .min(controls.actual_iteration_limit()),
    );
    let mut initial_multiscale_component = None::<f64>;
    let has_valid_support = (0..cells).any(|index| {
        let pixel = plane_pixel(index, shape);
        mask.contains(pixel) && valid_support(base, shape, model_plane, pixel)
    });
    let mut controller =
        MinorCycleController::new(&controls, effective_threshold, has_valid_support);

    for _ in 0..controller.iteration_limit() {
        let (peak_index, strength, scale_index) = if let Some(kernels) = multiscale.as_ref() {
            let candidate = select_multiscale_candidate(
                &residual,
                plane.normal_approximation(),
                shape,
                psf_peak_pixel,
                base,
                model_plane,
                mask,
                kernels,
            )
            .ok_or(MinorCycleError::EmptyValidSupport)?;
            (
                candidate.index,
                candidate.strength,
                Some(candidate.scale_index),
            )
        } else {
            let peak_index = find_peak_abs(
                &residual,
                shape,
                |value| *value,
                |pixel| {
                    let index = pixel[0] * shape[1] + pixel[1];
                    mask.contains(pixel)
                        && valid_support(base, shape, model_plane, pixel)
                        && clark_state.as_ref().is_none_or(|state| state.active[index])
                },
            )
            .ok_or(MinorCycleError::EmptyValidSupport)?;
            (peak_index, residual[peak_index] / psf_peak, None)
        };
        let peak_pixel = plane_pixel(peak_index, shape);
        if !strength.is_finite() {
            return Err(MinorCycleError::GeneratedNonfinite);
        }
        if scale_index.is_some() {
            match initial_multiscale_component {
                Some(initial)
                    if multiscale_diverged(initial, strength, controller.iterations()) =>
                {
                    controller.stop(MinorCycleStopReason::MultiscaleDivergence);
                    break;
                }
                None => initial_multiscale_component = Some(strength.abs()),
                Some(_) => {}
            }
        }
        // The casacore HOGBOM cleaner stops only when the normalized peak is
        // strictly below the threshold (`lattices/LatticeMath/
        // LatticeCleaner.tcc`, stopping rule 1: "stop if below threshold",
        // tested as `abs(itsStrengthOptimum) < threshold()`), so a peak
        // exactly at the threshold still cleans one component. A zero peak
        // has no flux to clean and converges trivially.
        let flux = controls.gain() * strength;
        if !flux.is_finite() {
            return Err(MinorCycleError::GeneratedNonfinite);
        }
        if !controller.admit(
            strength,
            flux.abs(),
            matches!(controls.algorithm(), ReconstructionAlgorithm::Clark),
        ) {
            break;
        }
        match (clark, scale_index) {
            (_, Some(scale_index)) => subtract_scaled_psf(
                &mut residual,
                plane.normal_approximation(),
                shape,
                peak_pixel,
                psf_peak_pixel,
                &multiscale.as_ref().expect("scale candidate has kernels")[scale_index],
                flux,
            )?,
            (Some(approximation), None) => subtract_psf_patch(
                &mut residual,
                plane.normal_approximation(),
                shape,
                peak_pixel,
                psf_peak_pixel,
                approximation.radius,
                flux,
            )?,
            (None, None) => subtract_psf(
                &mut residual,
                plane.normal_approximation(),
                shape,
                peak_pixel,
                psf_peak_pixel,
                flux,
            )?,
        }
        controller.accepted(flux.abs());
        let cell = model_cell(model_plane, shape, peak_pixel)
            .expect("a scanned peak pixel lies inside the plane");
        let scale_px = if let Some(scale_index) = scale_index {
            add_scaled_terms(
                &mut terms,
                base,
                model_plane,
                shape,
                peak_pixel,
                &multiscale.as_ref().expect("scale candidate has kernels")[scale_index],
                flux,
            );
            multiscale.as_ref().expect("scale candidate has kernels")[scale_index].scale_px
        } else {
            *terms.entry(canonical_flat(base, cell)).or_insert(0.0) += flux;
            0.0
        };
        if let Some(limit) = controls.component_sequence_limit()
            && recorded.len() < limit
        {
            recorded.push(MinorCycleComponent {
                cell,
                flux,
                scale_px,
            });
        }
        if let Some(state) = clark_state.as_mut() {
            // SDAlgorithmClarkClean2 configures ClarkCleanLatModel with
            // speedup=-1. Its uncertainty limit therefore closes the current
            // patch subcycle after an accepted component and recomputes the
            // exact residual before selecting the next active set. Preserve
            // that behavior explicitly instead of allowing approximate patch
            // errors to accumulate across the public cycle boundary.
            refresh_point_residual(
                &mut residual,
                plane.residual(),
                plane.normal_approximation(),
                shape,
                psf_peak_pixel,
                base,
                &terms,
            )?;
            state.refreshes += 1;
            let global_peak = find_peak_abs(
                &residual,
                shape,
                |value| *value,
                |pixel| mask.contains(pixel) && valid_support(base, shape, model_plane, pixel),
            )
            .ok_or(MinorCycleError::EmptyValidSupport)?;
            let global_strength = residual[global_peak].abs() / psf_peak;
            let approximation = clark.expect("Clark state has approximation");
            state.cutoff =
                (global_strength * approximation.maximum_exterior_sidelobe / psf_peak / 3.0)
                    .max(effective_threshold);
            for (index, active) in state.active.iter_mut().enumerate() {
                *active = residual[index].abs() / psf_peak >= state.cutoff;
            }
        }
    }
    let (iterations, total_flux, stop_reason) = controller.finish();
    let controller_iterations = controls.controller_iterations(iterations, stop_reason);
    let clark_refreshes = clark_state.as_ref().map_or(0, |state| state.refreshes);
    if multiscale.is_some() && !terms.is_empty() {
        // MatrixCleaner uses finite subregions while selecting a bounded
        // multiscale component sequence, then finalizes the cycle with its
        // full-image FFT residual. Refresh that terminal evidence from the
        // accepted model delta so the controller observes the same circular
        // normal-operator boundary instead of the last finite work patch.
        refresh_circular_residual(
            &mut residual,
            plane.residual(),
            plane.normal_approximation(),
            shape,
            psf_peak_pixel,
            base,
            &terms,
        )?;
    }
    let final_peak_flux = find_peak_abs(
        &residual,
        shape,
        |value| *value,
        |pixel| mask.contains(pixel) && valid_support(base, shape, model_plane, pixel),
    )
    .map_or(0.0, |index| residual[index].abs() / psf_peak);

    terms.retain(|_, flux| *flux != 0.0);
    let delta = if terms.is_empty() {
        None
    } else {
        let deltas = terms
            .iter()
            .map(|(flat, flux)| {
                let cell = base
                    .shape()
                    .cell_at(*flat)
                    .expect("accumulated flat keys stay inside the base shape");
                Ok(ModelDeltaTerm::new(cell, ModelValue::new(*flux)?))
            })
            .collect::<Result<Vec<_>, MinorCycleError>>()?;
        Some(lifecycle.compile_delta(base, deltas)?)
    };

    let evidence_id = minor_cycle_evidence_id(
        lifecycle.authority(),
        lifecycle.attempt(),
        lifecycle.epoch(),
        base.generation_id(),
        view.completion_id(),
        view.content_identity(),
        &[mask.generation_id()],
        &controls,
        iterations,
        controller_iterations,
        total_flux,
        final_peak_flux,
        noise_rms,
        effective_threshold,
        stop_reason,
        clark,
        clark_refreshes,
        None,
    );
    Ok(MinorCycleResult {
        delta,
        evidence: MinorCycleEvidence {
            evidence_id,
            problem: lifecycle.problem(),
            attempt: lifecycle.attempt(),
            epoch: lifecycle.epoch(),
            input_generation: base.generation_id(),
            normal_state_completion: view.completion_id(),
            normal_state_content: view.content_identity(),
            iterations,
            controller_iterations,
            total_flux,
            initial_peak_flux: initial_peak,
            final_peak_flux,
            noise_rms,
            global_threshold,
            effective_threshold,
            cycle_threshold,
            stop_reason,
            clark_approximation: clark,
            clark_refreshes,
            recorded: (!recorded.is_empty()).then(|| recorded.into_boxed_slice()),
            image_domain_runs: None,
        },
    })
}

/// Canonical pixel of one two-dimensional plane-storage index.
///
/// Normal-state planes are stored x-major (`finish_bound` pushes x outer, y
/// inner), so index `p` maps to pixel `[p / H, p % H]`.
fn plane_pixel(index: usize, shape: [usize; 2]) -> [usize; 2] {
    [index / shape[1], index % shape[1]]
}

fn model_cell(
    model_plane: MinorCycleModelPlane,
    shape: [usize; 2],
    pixel: [usize; 2],
) -> Option<ModelCell> {
    if pixel[0] < shape[0] && pixel[1] < shape[1] {
        Some(ModelCell::new(
            model_plane.domain(),
            model_plane.coefficient(),
            model_plane.polarization(),
            pixel,
        ))
    } else {
        None
    }
}

fn canonical_flat(base: &ModelGeneration, cell: ModelCell) -> usize {
    base.shape()
        .flat_index(cell)
        .expect("component pixels stay inside the base shape")
}

fn valid_support(
    base: &ModelGeneration,
    shape: [usize; 2],
    model_plane: MinorCycleModelPlane,
    pixel: [usize; 2],
) -> bool {
    let Some(cell) = model_cell(model_plane, shape, pixel) else {
        return false;
    };
    base.samples()[canonical_flat(base, cell)].support() == ModelSupport::Valid
}

/// Find the maximum-abs real plane value passing `accept`, scanning in
/// canonical storage order so ties deterministically keep the first peak.
fn find_peak_abs<T>(
    plane: &[T],
    shape: [usize; 2],
    magnitude: impl Fn(&T) -> f64,
    accept: impl Fn([usize; 2]) -> bool,
) -> Option<usize> {
    let mut best: Option<(f64, usize)> = None;
    for (index, value) in plane.iter().enumerate() {
        let magnitude = magnitude(value).abs();
        if best.is_some_and(|(best_magnitude, _)| magnitude <= best_magnitude) {
            continue;
        }
        let pixel = plane_pixel(index, shape);
        if accept(pixel) {
            best = Some((magnitude, index));
        }
    }
    best.map(|(_, index)| index)
}

/// Subtract `flux * psf` centered on `peak` from the working residual.
fn subtract_psf(
    residual: &mut [f64],
    psf: &[num_complex::Complex64],
    shape: [usize; 2],
    peak: [usize; 2],
    psf_peak: [usize; 2],
    flux: f64,
) -> Result<(), MinorCycleError> {
    // psf_shifted(x, y) = psf(x - peak + psf_peak, y - peak + psf_peak),
    // clipped to the plane exactly like the reference cleaner's subregion.
    let x_range = overlap(peak[0], psf_peak[0], shape[0]);
    let y_range = overlap(peak[1], psf_peak[1], shape[1]);
    for y in y_range.clone() {
        for x in x_range.clone() {
            let source = [x + psf_peak[0] - peak[0], y + psf_peak[1] - peak[1]];
            let index = source[0] * shape[1] + source[1];
            let target = x * shape[1] + y;
            let updated = residual[target] - flux * psf[index].re;
            if !updated.is_finite() {
                return Err(MinorCycleError::GeneratedNonfinite);
            }
            residual[target] = updated;
        }
    }
    Ok(())
}

/// Subtract one PSF component using the full-image circular FFT convention.
fn subtract_psf_circular(
    residual: &mut [f64],
    psf: &[num_complex::Complex64],
    shape: [usize; 2],
    peak: [usize; 2],
    psf_peak: [usize; 2],
    flux: f64,
) -> Result<(), MinorCycleError> {
    for source_x in 0..shape[0] {
        let target_x = (source_x + peak[0] + shape[0] - psf_peak[0]) % shape[0];
        for source_y in 0..shape[1] {
            let target_y = (source_y + peak[1] + shape[1] - psf_peak[1]) % shape[1];
            let source = source_x * shape[1] + source_y;
            let target = target_x * shape[1] + target_y;
            let updated = residual[target] - flux * psf[source].re;
            if !updated.is_finite() {
                return Err(MinorCycleError::GeneratedNonfinite);
            }
            residual[target] = updated;
        }
    }
    Ok(())
}

fn refresh_point_residual(
    residual: &mut [f64],
    original: &[num_complex::Complex64],
    psf: &[num_complex::Complex64],
    shape: [usize; 2],
    psf_peak: [usize; 2],
    base: &ModelGeneration,
    terms: &BTreeMap<usize, f64>,
) -> Result<(), MinorCycleError> {
    for (target, source) in residual.iter_mut().zip(original) {
        *target = source.re;
    }
    for (flat, flux) in terms {
        let pixel = base
            .shape()
            .cell_at(*flat)
            .ok_or(MinorCycleError::ModelShapeMismatch)?
            .pixel();
        subtract_psf(residual, psf, shape, pixel, psf_peak, *flux)?;
    }
    Ok(())
}

fn refresh_circular_residual(
    residual: &mut [f64],
    original: &[num_complex::Complex64],
    psf: &[num_complex::Complex64],
    shape: [usize; 2],
    psf_peak: [usize; 2],
    base: &ModelGeneration,
    terms: &BTreeMap<usize, f64>,
) -> Result<(), MinorCycleError> {
    for (target, source) in residual.iter_mut().zip(original) {
        *target = source.re;
    }
    for (flat, flux) in terms {
        let pixel = base
            .shape()
            .cell_at(*flat)
            .ok_or(MinorCycleError::ModelShapeMismatch)?
            .pixel();
        subtract_psf_circular(residual, psf, shape, pixel, psf_peak, *flux)?;
    }
    Ok(())
}

fn robust_masked_rms(
    residual: &[f64],
    shape: [usize; 2],
    base: &ModelGeneration,
    model_plane: MinorCycleModelPlane,
    mask: &ReconstructionMask,
) -> Result<f64, MinorCycleError> {
    let mut values = residual
        .iter()
        .enumerate()
        .filter_map(|(index, value)| {
            let pixel = plane_pixel(index, shape);
            (mask.contains(pixel) && valid_support(base, shape, model_plane, pixel))
                .then_some(*value)
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Err(MinorCycleError::EmptyValidSupport);
    }
    values.sort_by(f64::total_cmp);
    let median = values[values.len() / 2];
    for value in &mut values {
        *value = (*value - median).abs();
    }
    values.sort_by(f64::total_cmp);
    Ok(1.482_602_218_505_602 * values[values.len() / 2])
}

#[derive(Debug)]
struct ScaleKernel {
    scale_px: f64,
    samples: Vec<([isize; 2], f64)>,
    bias: f64,
    search_border: usize,
}

#[derive(Debug)]
struct TaylorScaleSystem {
    inverse: Vec<f64>,
    h00: f64,
}

#[derive(Debug)]
struct TaylorCandidate {
    index: usize,
    scale_index: usize,
    coefficients: Vec<f64>,
    score: f64,
}

#[derive(Debug)]
struct ActiveBlockSystem {
    coefficients: Box<[usize]>,
    inverse: Vec<f64>,
}

#[derive(Debug)]
struct JointScaleSystems {
    continuum: ActiveBlockSystem,
    line: ActiveBlockSystem,
    full: ActiveBlockSystem,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TaylorSearchWindow {
    start: [usize; 2],
    end_exclusive: [usize; 2],
}

impl TaylorSearchWindow {
    fn around(position: [usize; 2], support: usize, shape: [usize; 2]) -> Self {
        let axis = |position: usize, extent: usize| {
            let length = support.min(extent);
            let start = position.saturating_sub(length / 2).min(extent - length);
            (start, start + length)
        };
        let (start_x, end_x) = axis(position[0], shape[0]);
        let (start_y, end_y) = axis(position[1], shape[1]);
        Self {
            start: [start_x, start_y],
            end_exclusive: [end_x, end_y],
        }
    }

    fn contains(self, pixel: [usize; 2]) -> bool {
        pixel[0] >= self.start[0]
            && pixel[0] < self.end_exclusive[0]
            && pixel[1] >= self.start[1]
            && pixel[1] < self.end_exclusive[1]
    }
}

fn build_taylor_scale_systems(
    view: &FinalNormalState,
    shape: [usize; 2],
    psf_peak: [usize; 2],
    kernels: &[ScaleKernel],
) -> Result<Vec<TaylorScaleSystem>, MinorCycleError> {
    let count = view.coefficient_term_count();
    kernels
        .iter()
        .map(|kernel| {
            let mut normal = vec![0.0; count * count];
            for row in 0..count {
                for column in row..count {
                    let block = view
                        .normal_block(row, column)
                        .ok_or(MinorCycleError::ModelShapeMismatch)?;
                    normal[row * count + column] = multiscale_normalization(
                        block.normal_approximation(),
                        shape,
                        psf_peak,
                        kernel,
                    );
                }
            }
            let h00 = normal[0];
            if !h00.is_finite() || h00 <= 0.0 {
                return Err(MinorCycleError::InvalidPsfPeak);
            }
            if taylor_rows_nearly_dependent(&normal, count) {
                return Err(MinorCycleError::SingularTaylorNormalBlock);
            }
            let mut inverse = vec![0.0; count * count];
            for column in 0..count {
                let mut unit = vec![0.0; count];
                unit[column] = 1.0;
                let solution = solve_symmetric_ldlt_casacore_dynamic(normal.clone(), &unit)
                    .ok_or(MinorCycleError::SingularTaylorNormalBlock)?;
                for row in 0..count {
                    inverse[row * count + column] = solution[row];
                }
            }
            Ok(TaylorScaleSystem { inverse, h00 })
        })
        .collect()
}

fn build_joint_scale_systems(
    view: &FinalNormalState,
    shape: [usize; 2],
    psf_peak: [usize; 2],
    kernels: &[ScaleKernel],
    continuum_terms: usize,
    maximum_condition_number: f64,
) -> Result<Vec<JointScaleSystems>, MinorCycleError> {
    let terms = view.coefficient_term_count();
    let continuum = (0..continuum_terms).collect::<Vec<_>>();
    let line = (continuum_terms..terms).collect::<Vec<_>>();
    let full = (0..terms).collect::<Vec<_>>();
    kernels
        .iter()
        .map(|kernel| {
            Ok(JointScaleSystems {
                continuum: build_active_block_system(
                    view,
                    shape,
                    psf_peak,
                    kernel,
                    &continuum,
                    maximum_condition_number,
                )?,
                line: build_active_block_system(
                    view,
                    shape,
                    psf_peak,
                    kernel,
                    &line,
                    maximum_condition_number,
                )?,
                full: build_active_block_system(
                    view,
                    shape,
                    psf_peak,
                    kernel,
                    &full,
                    maximum_condition_number,
                )?,
            })
        })
        .collect()
}

fn build_active_block_system(
    view: &FinalNormalState,
    shape: [usize; 2],
    psf_peak: [usize; 2],
    kernel: &ScaleKernel,
    coefficients: &[usize],
    maximum_condition_number: f64,
) -> Result<ActiveBlockSystem, MinorCycleError> {
    let count = coefficients.len();
    let mut normal = vec![0.0; count * count];
    for (local_row, &row) in coefficients.iter().enumerate() {
        for (local_column, &column) in coefficients.iter().enumerate().skip(local_row) {
            let block = view
                .normal_block(row, column)
                .ok_or(MinorCycleError::ModelShapeMismatch)?;
            normal[local_row * count + local_column] =
                multiscale_normalization(block.normal_approximation(), shape, psf_peak, kernel);
            normal[local_column * count + local_row] = normal[local_row * count + local_column];
        }
    }
    if taylor_rows_nearly_dependent(&normal, count) {
        return Err(MinorCycleError::SingularJointNormalBlock);
    }
    let mut inverse = vec![0.0; count * count];
    for column in 0..count {
        let mut unit = vec![0.0; count];
        unit[column] = 1.0;
        let solution = solve_symmetric_ldlt_casacore_dynamic(normal.clone(), &unit)
            .ok_or(MinorCycleError::SingularJointNormalBlock)?;
        for row in 0..count {
            inverse[row * count + column] = solution[row];
        }
    }
    let normal_norm = normal
        .chunks_exact(count)
        .map(|row| row.iter().map(|value| value.abs()).sum::<f64>())
        .fold(0.0_f64, f64::max);
    let inverse_norm = (0..count)
        .map(|row| {
            (0..count)
                .map(|column| inverse[row * count + column].abs())
                .sum::<f64>()
        })
        .fold(0.0_f64, f64::max);
    let condition = normal_norm * inverse_norm;
    if !condition.is_finite() || condition > maximum_condition_number {
        return Err(MinorCycleError::SingularJointNormalBlock);
    }
    Ok(ActiveBlockSystem {
        coefficients: coefficients.into(),
        inverse,
    })
}

#[allow(clippy::too_many_arguments)]
fn select_joint_candidate(
    residuals: &[Vec<f64>],
    shape: [usize; 2],
    base: &ModelGeneration,
    primary: MinorCycleModelPlane,
    continuum_terms: usize,
    masks: &CoupledReconstructionMask,
    kernels: &[ScaleKernel],
    systems: &[JointScaleSystems],
    search_window: Option<TaylorSearchWindow>,
) -> Option<TaylorCandidate> {
    let mut best = None;
    for (scale_index, (kernel, systems)) in kernels.iter().zip(systems).enumerate() {
        let mut scale_best = None;
        for index in 0..residuals[0].len() {
            let pixel = plane_pixel(index, shape);
            if search_window.is_some_and(|window| !window.contains(pixel)) {
                continue;
            }
            let continuum = joint_kernel_fits(
                base,
                primary,
                shape,
                pixel,
                0..continuum_terms,
                masks.continuum(),
                kernel,
            );
            let line = joint_kernel_fits(
                base,
                primary,
                shape,
                pixel,
                continuum_terms..residuals.len(),
                masks.line(),
                kernel,
            );
            let system = match (continuum, line) {
                (true, true) => &systems.full,
                (true, false) => &systems.continuum,
                (false, true) => &systems.line,
                (false, false) => continue,
            };
            let rhs = system
                .coefficients
                .iter()
                .map(|&coefficient| convolve_at(&residuals[coefficient], shape, pixel, kernel))
                .collect::<Vec<_>>();
            let active = system.coefficients.len();
            let mut coefficients = vec![0.0; residuals.len()];
            for (local_row, &coefficient) in system.coefficients.iter().enumerate() {
                coefficients[coefficient] = (0..active)
                    .map(|column| system.inverse[local_row * active + column] * rhs[column])
                    .sum();
            }
            let score = system
                .coefficients
                .iter()
                .enumerate()
                .map(|(local, &coefficient)| coefficients[coefficient] * rhs[local])
                .sum::<f64>();
            let candidate = TaylorCandidate {
                index,
                scale_index,
                coefficients,
                score,
            };
            if scale_best
                .as_ref()
                .is_none_or(|current| prefer_taylor_within_scale(&candidate, current))
            {
                scale_best = Some(candidate);
            }
        }
        if let Some(candidate) = scale_best
            && best
                .as_ref()
                .is_none_or(|current| prefer_taylor_across_scales(&candidate, current, kernels))
        {
            best = Some(candidate);
        }
    }
    best
}

fn joint_candidate_peak(candidate: &TaylorCandidate) -> f64 {
    candidate
        .coefficients
        .iter()
        .map(|value| value.abs())
        .fold(0.0, f64::max)
}

#[allow(clippy::too_many_arguments)]
fn joint_kernel_fits(
    base: &ModelGeneration,
    primary: MinorCycleModelPlane,
    shape: [usize; 2],
    centre: [usize; 2],
    coefficients: std::ops::Range<usize>,
    mask: &ReconstructionMask,
    kernel: &ScaleKernel,
) -> bool {
    if !within_multiscale_border(centre, shape, kernel.search_border) {
        return false;
    }
    let overlap = kernel
        .samples
        .iter()
        .filter_map(|(offset, weight)| {
            offset_pixel(centre, *offset, shape)
                .filter(|pixel| {
                    mask.contains(*pixel)
                        && coefficients.clone().all(|coefficient| {
                            valid_support(
                                base,
                                shape,
                                MinorCycleModelPlane::new(
                                    primary.domain(),
                                    coefficient,
                                    primary.polarization(),
                                ),
                                *pixel,
                            )
                        })
                })
                .map(|_| *weight)
        })
        .sum::<f64>();
    overlap > CASA_MTMFS_SCALE_MASK_MINIMUM_OVERLAP
}

fn taylor_rows_nearly_dependent(normal: &[f64], count: usize) -> bool {
    let value = |row: usize, column: usize| {
        let (upper_row, upper_column) = if row <= column {
            (row, column)
        } else {
            (column, row)
        };
        normal[upper_row * count + upper_column]
    };
    (0..count.saturating_sub(1)).any(|row| {
        let ratios = (0..count)
            .map(|column| value(row, column) / value(row + 1, column))
            .collect::<Vec<_>>();
        ratios
            .windows(2)
            .map(|pair| (pair[0] - pair[1]).abs())
            .sum::<f64>()
            / ((count - 1) as f64)
            < 1.0e-4
    })
}

fn taylor_psf_peak_index(
    psf: &[num_complex::Complex64],
    shape: [usize; 2],
    maximum_scale_px: f64,
) -> Option<usize> {
    let support = taylor_psf_support(shape, maximum_scale_px);
    let low = [
        shape[0].saturating_sub(support) / 2,
        shape[1].saturating_sub(support) / 2,
    ];
    let high = [
        if shape[0] > support {
            shape[0] / 2 + support / 2
        } else {
            shape[0]
        },
        if shape[1] > support {
            shape[1] / 2 + support / 2
        } else {
            shape[1]
        },
    ];
    let mut best = None::<(f64, usize)>;
    for x in low[0]..high[0] {
        for y in low[1]..high[1] {
            let index = x * shape[1] + y;
            let magnitude = psf.get(index)?.re.abs();
            if best.is_none_or(|(current, _)| magnitude > current) {
                best = Some((magnitude, index));
            }
        }
    }
    best.map(|(_, index)| index)
}

fn taylor_psf_support(shape: [usize; 2], maximum_scale_px: f64) -> usize {
    let mut support = ((16.0 + maximum_scale_px * maximum_scale_px).sqrt() * 20.0) as usize;
    support = support.max(80).min(shape[0]).min(shape[1]);
    if support % 2 != 0 {
        support -= 1;
    }
    support
}

#[allow(clippy::too_many_arguments)]
fn select_taylor_candidate(
    residuals: &[Vec<f64>],
    shape: [usize; 2],
    base: &ModelGeneration,
    primary: MinorCycleModelPlane,
    term_count: usize,
    mask: &ReconstructionMask,
    kernels: &[ScaleKernel],
    systems: &[TaylorScaleSystem],
    search_window: Option<TaylorSearchWindow>,
) -> Option<TaylorCandidate> {
    let mut best = None;
    for (scale_index, (kernel, system)) in kernels.iter().zip(systems).enumerate() {
        let mut scale_best = None;
        for index in 0..residuals[0].len() {
            let pixel = plane_pixel(index, shape);
            if search_window.is_some_and(|window| !window.contains(pixel)) {
                continue;
            }
            if !taylor_kernel_fits(base, primary, shape, pixel, term_count, mask, kernel) {
                continue;
            }
            let rhs = residuals
                .iter()
                .map(|residual| convolve_at(residual, shape, pixel, kernel))
                .collect::<Vec<_>>();
            let coefficients = (0..term_count)
                .map(|row| {
                    (0..term_count)
                        .map(|column| system.inverse[row * term_count + column] * rhs[column])
                        .sum::<f64>()
                })
                .collect::<Vec<_>>();
            let score = coefficients
                .iter()
                .zip(&rhs)
                .map(|(coefficient, response)| coefficient * response)
                .sum::<f64>();
            let candidate = TaylorCandidate {
                index,
                scale_index,
                coefficients,
                score,
            };
            if scale_best
                .as_ref()
                .is_none_or(|current| prefer_taylor_within_scale(&candidate, current))
            {
                scale_best = Some(candidate);
            }
        }
        if let Some(candidate) = scale_best
            && best
                .as_ref()
                .is_none_or(|current| prefer_taylor_across_scales(&candidate, current, kernels))
        {
            best = Some(candidate);
        }
    }
    best
}

fn prefer_taylor_within_scale(candidate: &TaylorCandidate, current: &TaylorCandidate) -> bool {
    candidate.score.abs() > current.score.abs()
        || (candidate.score.abs() == current.score.abs() && candidate.score > current.score)
}

fn prefer_taylor_across_scales(
    candidate: &TaylorCandidate,
    current: &TaylorCandidate,
    kernels: &[ScaleKernel],
) -> bool {
    candidate.score * kernels[candidate.scale_index].bias
        > current.score * kernels[current.scale_index].bias
}

fn convolve_at(plane: &[f64], shape: [usize; 2], pixel: [usize; 2], kernel: &ScaleKernel) -> f64 {
    kernel
        .samples
        .iter()
        .filter_map(|(offset, weight)| {
            offset_pixel(pixel, *offset, shape)
                .map(|sample| plane[sample[0] * shape[1] + sample[1]] * weight)
        })
        .sum()
}

#[allow(clippy::too_many_arguments)]
fn principal_taylor_peak(
    residual: &[f64],
    shape: [usize; 2],
    base: &ModelGeneration,
    primary: MinorCycleModelPlane,
    term_count: usize,
    mask: &ReconstructionMask,
    kernel: &ScaleKernel,
) -> f64 {
    (0..residual.len())
        .filter_map(|index| {
            let pixel = plane_pixel(index, shape);
            taylor_kernel_fits(base, primary, shape, pixel, term_count, mask, kernel)
                .then(|| convolve_at(residual, shape, pixel, kernel).abs())
        })
        .fold(0.0, f64::max)
}

fn valid_taylor_support(
    base: &ModelGeneration,
    shape: [usize; 2],
    primary: MinorCycleModelPlane,
    term_count: usize,
    pixel: [usize; 2],
) -> bool {
    (0..term_count).all(|coefficient| {
        valid_support(
            base,
            shape,
            MinorCycleModelPlane::new(primary.domain(), coefficient, primary.polarization()),
            pixel,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn taylor_kernel_fits(
    base: &ModelGeneration,
    primary: MinorCycleModelPlane,
    shape: [usize; 2],
    centre: [usize; 2],
    term_count: usize,
    mask: &ReconstructionMask,
    kernel: &ScaleKernel,
) -> bool {
    if !within_multiscale_border(centre, shape, kernel.search_border) {
        return false;
    }
    let overlap = kernel
        .samples
        .iter()
        .filter_map(|(offset, weight)| {
            offset_pixel(centre, *offset, shape)
                .filter(|pixel| {
                    mask.contains(*pixel)
                        && valid_taylor_support(base, shape, primary, term_count, *pixel)
                })
                .map(|_| *weight)
        })
        .sum::<f64>();
    overlap > CASA_MTMFS_SCALE_MASK_MINIMUM_OVERLAP
}

fn refresh_taylor_residuals(
    residuals: &mut [Vec<f64>],
    view: &FinalNormalState,
    shape: [usize; 2],
    psf_peak: [usize; 2],
    base: &ModelGeneration,
    model_terms: &BTreeMap<usize, f64>,
) -> Result<(), MinorCycleError> {
    for (term, residual) in residuals.iter_mut().enumerate() {
        let original = view
            .coefficient_term(term)
            .ok_or(MinorCycleError::ModelShapeMismatch)?;
        for (target, source) in residual.iter_mut().zip(original.residual()) {
            *target = source.re;
        }
    }
    for (flat, flux) in model_terms {
        let cell = base
            .shape()
            .cell_at(*flat)
            .ok_or(MinorCycleError::ModelShapeMismatch)?;
        for (residual_term, residual) in residuals.iter_mut().enumerate() {
            let psf = view
                .normal_block(residual_term, cell.coefficient())
                .ok_or(MinorCycleError::ModelShapeMismatch)?;
            subtract_psf_circular(
                residual,
                psf.normal_approximation(),
                shape,
                cell.pixel(),
                psf_peak,
                *flux,
            )?;
        }
    }
    Ok(())
}

const CASA_SCALE_MASK_MINIMUM_OVERLAP: f64 = 0.9;
const CASA_MTMFS_SCALE_MASK_MINIMUM_OVERLAP: f64 = 0.1;

#[derive(Debug, Clone, Copy)]
struct MultiscaleCandidate {
    index: usize,
    scale_index: usize,
    strength: f64,
    score: f64,
}

fn build_scale_kernels(scales_px: &[f64], small_scale_bias: f64) -> Vec<ScaleKernel> {
    let largest = scales_px.last().copied().unwrap_or(0.0);
    scales_px
        .iter()
        .copied()
        .map(|scale_px| {
            let samples = if scale_px == 0.0 {
                vec![([0, 0], 1.0)]
            } else {
                let radius = scale_px.ceil() as isize;
                let mut samples = Vec::new();
                let mut volume = 0.0;
                for x in -radius..=radius {
                    for y in -radius..=radius {
                        let r2 = (x as f64 / scale_px).powi(2) + (y as f64 / scale_px).powi(2);
                        if r2 < 1.0 {
                            let value = (1.0 - r2) * multiscale_spheroidal(r2.sqrt());
                            samples.push(([x, y], value));
                            volume += value;
                        }
                    }
                }
                for (_, value) in &mut samples {
                    *value /= volume;
                }
                samples
            };
            let bias = if largest == 0.0 {
                1.0
            } else {
                1.0 - small_scale_bias * scale_px / largest
            };
            ScaleKernel {
                scale_px,
                samples,
                bias,
                // MatrixCleaner::makeScaleMasks excludes a 1.5-scale border
                // before peak selection, even when the user mask is full.
                search_border: (scale_px * 1.5) as usize,
            }
        })
        .collect()
}

fn multiscale_spheroidal(nu: f64) -> f64 {
    if nu <= 0.0 {
        return 1.0;
    }
    if nu >= 1.0 {
        return 0.0;
    }
    let (p, q, endpoint) = if nu < 0.75 {
        (
            [
                0.082_033_43,
                -0.364_470_5,
                0.627_866,
                -0.533_558_1,
                0.231_275_6,
            ],
            [1.0, 0.821_201_8, 0.207_804_3],
            0.75,
        )
    } else {
        (
            [
                0.004_028_559,
                -0.036_977_68,
                0.102_133_2,
                -0.120_143_6,
                0.064_127_74,
            ],
            [1.0, 0.959_910_2, 0.291_872_4],
            1.0,
        )
    };
    let delta = nu.powi(2) - endpoint * endpoint;
    let numerator = p
        .iter()
        .enumerate()
        .map(|(power, coefficient)| coefficient * delta.powi(power as i32))
        .sum::<f64>();
    let denominator = q
        .iter()
        .enumerate()
        .map(|(power, coefficient)| coefficient * delta.powi(power as i32))
        .sum::<f64>();
    numerator / denominator
}

#[allow(clippy::too_many_arguments)]
fn select_multiscale_candidate(
    residual: &[f64],
    psf: &[num_complex::Complex64],
    shape: [usize; 2],
    psf_peak: [usize; 2],
    base: &ModelGeneration,
    model_plane: MinorCycleModelPlane,
    mask: &ReconstructionMask,
    kernels: &[ScaleKernel],
) -> Option<MultiscaleCandidate> {
    let mut best = None;
    for (scale_index, kernel) in kernels.iter().enumerate() {
        let normalization = multiscale_normalization(psf, shape, psf_peak, kernel);
        if !normalization.is_finite() || normalization <= 0.0 {
            continue;
        }
        for index in 0..residual.len() {
            let pixel = plane_pixel(index, shape);
            if !kernel_fits(base, model_plane, shape, pixel, mask, kernel) {
                continue;
            }
            let dirty = kernel
                .samples
                .iter()
                .map(|(offset, weight)| {
                    let sample = offset_pixel(pixel, *offset, shape)
                        .expect("kernel fit was checked before convolution");
                    residual[sample[0] * shape[1] + sample[1]] * weight
                })
                .sum::<f64>();
            let strength = dirty / normalization;
            let candidate = MultiscaleCandidate {
                index,
                scale_index,
                strength,
                // MatrixCleaner ranks the scale-normalized dirty response by
                // `bias * dirty * strength`; comparing absolute scores keeps
                // signed component recovery separate from scale selection.
                score: (dirty * strength * kernel.bias).abs(),
            };
            if best
                .as_ref()
                .is_none_or(|current: &MultiscaleCandidate| candidate.score > current.score)
            {
                best = Some(candidate);
            }
        }
    }
    best
}

fn multiscale_diverged(initial: f64, current: f64, iterations: usize) -> bool {
    iterations > 0 && current.abs() > initial.abs() * 1.5
}

fn multiscale_normalization(
    psf: &[num_complex::Complex64],
    shape: [usize; 2],
    peak: [usize; 2],
    kernel: &ScaleKernel,
) -> f64 {
    kernel
        .samples
        .iter()
        .flat_map(|(left_offset, left_weight)| {
            kernel
                .samples
                .iter()
                .filter_map(move |(right_offset, right_weight)| {
                    let offset = [
                        left_offset[0] - right_offset[0],
                        left_offset[1] - right_offset[1],
                    ];
                    offset_pixel(peak, offset, shape).map(|pixel| {
                        left_weight * right_weight * psf[pixel[0] * shape[1] + pixel[1]].re
                    })
                })
        })
        .sum()
}

fn kernel_fits(
    base: &ModelGeneration,
    model_plane: MinorCycleModelPlane,
    shape: [usize; 2],
    centre: [usize; 2],
    mask: &ReconstructionMask,
    kernel: &ScaleKernel,
) -> bool {
    if !within_multiscale_border(centre, shape, kernel.search_border) {
        return false;
    }
    let overlap = kernel
        .samples
        .iter()
        .filter_map(|(offset, weight)| {
            offset_pixel(centre, *offset, shape)
                .filter(|pixel| {
                    mask.contains(*pixel) && valid_support(base, shape, model_plane, *pixel)
                })
                .map(|_| *weight)
        })
        .sum::<f64>();
    overlap > CASA_SCALE_MASK_MINIMUM_OVERLAP
}

fn within_multiscale_border(centre: [usize; 2], shape: [usize; 2], border: usize) -> bool {
    if border == 0 {
        return centre[0] < shape[0] && centre[1] < shape[1];
    }
    centre.into_iter().zip(shape).all(|(coordinate, extent)| {
        coordinate > border && coordinate < extent.saturating_sub(border).saturating_sub(1)
    })
}

fn offset_pixel(pixel: [usize; 2], offset: [isize; 2], shape: [usize; 2]) -> Option<[usize; 2]> {
    let x = pixel[0].checked_add_signed(offset[0])?;
    let y = pixel[1].checked_add_signed(offset[1])?;
    (x < shape[0] && y < shape[1]).then_some([x, y])
}

fn add_scaled_terms(
    terms: &mut BTreeMap<usize, f64>,
    base: &ModelGeneration,
    model_plane: MinorCycleModelPlane,
    shape: [usize; 2],
    centre: [usize; 2],
    kernel: &ScaleKernel,
    flux: f64,
) {
    for (offset, weight) in &kernel.samples {
        let pixel =
            offset_pixel(centre, *offset, shape).expect("selected scale fits model support");
        let cell =
            model_cell(model_plane, shape, pixel).expect("scale pixel lies inside the model plane");
        *terms.entry(canonical_flat(base, cell)).or_insert(0.0) += flux * weight;
    }
}

fn subtract_scaled_psf(
    residual: &mut [f64],
    psf: &[num_complex::Complex64],
    shape: [usize; 2],
    centre: [usize; 2],
    psf_peak: [usize; 2],
    kernel: &ScaleKernel,
    flux: f64,
) -> Result<(), MinorCycleError> {
    for (offset, weight) in &kernel.samples {
        let pixel =
            offset_pixel(centre, *offset, shape).expect("selected scale fits model support");
        subtract_psf(residual, psf, shape, pixel, psf_peak, flux * weight)?;
    }
    Ok(())
}

/// Derive CASA's Clark patch from the fitted restoring beam.
///
/// `SDAlgorithmClarkClean2` chooses at least four pixels, otherwise the
/// ceiling of the fitted major/minor FWHM in pixels, then requests a
/// `3*ncent+1` square capped by the PSF/model shape. The normal-state plane
/// uses unit pixel coordinates here, so the shared CASA-style beam fitter can
/// supply that same width without crossing the reconstruction-owner boundary.
fn derive_clark_approximation(
    psf: &[num_complex::Complex64],
    shape: [usize; 2],
    peak: [usize; 2],
) -> Result<ClarkApproximation, crate::PsfBeamFitError> {
    let real_psf = psf.iter().map(|value| value.re as f32).collect::<Vec<_>>();
    let beam =
        crate::fit_restoring_beam(&real_psf, shape, [1.0, 1.0], crate::DEFAULT_PSF_FIT_CUTOFF)?;
    let central_width = 4_usize
        .max(beam.major_fwhm_rad().ceil() as usize)
        .max(beam.minor_fwhm_rad().ceil() as usize);
    let requested = central_width.saturating_mul(3).saturating_add(1);
    let radius = [requested.min(shape[0]) / 2, requested.min(shape[1]) / 2];
    let maximum_exterior_sidelobe = psf
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            let pixel = plane_pixel(*index, shape);
            pixel[0].abs_diff(peak[0]) > radius[0] || pixel[1].abs_diff(peak[1]) > radius[1]
        })
        .fold(0.0_f64, |maximum, (_, value)| maximum.max(value.re.abs()));
    Ok(ClarkApproximation {
        radius,
        maximum_exterior_sidelobe,
    })
}

fn subtract_psf_patch(
    residual: &mut [f64],
    psf: &[num_complex::Complex64],
    shape: [usize; 2],
    peak: [usize; 2],
    psf_peak: [usize; 2],
    radius: [usize; 2],
    flux: f64,
) -> Result<(), MinorCycleError> {
    let x_range = overlap(peak[0], psf_peak[0], shape[0]);
    let y_range = overlap(peak[1], psf_peak[1], shape[1]);
    for y in y_range {
        for x in x_range.clone() {
            let source = [x + psf_peak[0] - peak[0], y + psf_peak[1] - peak[1]];
            if source[0].abs_diff(psf_peak[0]) > radius[0]
                || source[1].abs_diff(psf_peak[1]) > radius[1]
            {
                continue;
            }
            let target = x * shape[1] + y;
            let updated = residual[target] - flux * psf[source[0] * shape[1] + source[1]].re;
            if !updated.is_finite() {
                return Err(MinorCycleError::GeneratedNonfinite);
            }
            residual[target] = updated;
        }
    }
    Ok(())
}

/// Inclusive-clipped axis range where a PSF centered at `psf_peak` overlaps
/// the plane when recentered on `peak`.
fn overlap(peak: usize, psf_peak: usize, length: usize) -> std::ops::Range<usize> {
    let shift = psf_peak as isize - peak as isize;
    let low = (-shift).max(0) as usize;
    let high_exclusive = (length as isize - shift)
        .min(length as isize)
        .max(low as isize) as usize;
    low..high_exclusive
}

#[allow(clippy::too_many_arguments)]
fn minor_cycle_evidence_id(
    authority: LogicalIdentity,
    attempt: ModelExecutionAttemptId,
    epoch: u64,
    input_generation: ModelGenerationId,
    normal_state_completion: FinalNormalStateCompletionId,
    normal_state_content: LogicalIdentity,
    mask_generations: &[ReconstructionMaskGenerationId],
    controls: &MinorCycleProgram,
    iterations: usize,
    controller_iterations: usize,
    total_flux: f64,
    final_peak_flux: f64,
    noise_rms: Option<f64>,
    effective_threshold: f64,
    stop_reason: MinorCycleStopReason,
    clark: Option<ClarkApproximation>,
    clark_refreshes: usize,
    image_domain_runs: Option<&[ImageDomainMinorCycleEvidence]>,
) -> MinorCycleEvidenceId {
    let mut encoder = Encoder::new(MINOR_CYCLE_EVIDENCE_DOMAIN, MINOR_CYCLE_EVIDENCE_VERSION);
    encoder.identity(authority.as_bytes());
    encoder.identity(attempt.identity().as_bytes());
    encoder.u64(epoch);
    encoder.identity(input_generation.as_bytes());
    encoder.identity(normal_state_completion.as_bytes());
    encoder.identity(normal_state_content.as_bytes());
    encoder.usize(mask_generations.len());
    for generation in mask_generations {
        encoder.identity(generation.as_bytes());
    }
    encoder.usize(controls.model_plane().domain());
    encoder.usize(controls.model_plane().coefficient());
    encoder.usize(controls.model_plane().polarization());
    match controls.algorithm() {
        ReconstructionAlgorithm::Hogbom => encoder.u8(0),
        ReconstructionAlgorithm::Clark => encoder.u8(1),
        ReconstructionAlgorithm::Multiscale {
            scales_px,
            small_scale_bias,
        } => {
            encoder.u8(2);
            encoder.usize(scales_px.len());
            for scale in scales_px {
                encoder.u64(crate::canonical_f64_bits(*scale));
            }
            encoder.u64(crate::canonical_f64_bits(*small_scale_bias));
        }
        ReconstructionAlgorithm::Mtmfs {
            scales_px,
            small_scale_bias,
        } => {
            encoder.u8(3);
            encoder.usize(scales_px.len());
            for scale in scales_px {
                encoder.u64(crate::canonical_f64_bits(*scale));
            }
            encoder.u64(crate::canonical_f64_bits(*small_scale_bias));
        }
        ReconstructionAlgorithm::JointContinuumLine {
            scales_px,
            small_scale_bias,
        } => {
            encoder.u8(4);
            encoder.usize(scales_px.len());
            for scale in scales_px {
                encoder.u64(crate::canonical_f64_bits(*scale));
            }
            encoder.u64(crate::canonical_f64_bits(*small_scale_bias));
        }
        _ => unreachable!("minor-cycle programs admit only implemented solvers"),
    }
    encoder.u64(crate::canonical_f64_bits(controls.gain()));
    encoder.u64(crate::canonical_f64_bits(controls.threshold()));
    match controls.noise_sigma() {
        Some(sigma) => {
            encoder.u8(1);
            encoder.u64(crate::canonical_f64_bits(sigma));
        }
        None => encoder.u8(0),
    }
    encoder.usize(controls.max_iterations());
    encoder.u8(match controls.hogbom_iteration_accounting() {
        HogbomIterationAccounting::Strict => 0,
        HogbomIterationAccounting::CasaInclusive => 1,
    });
    match controls.validity() {
        MinorCycleValidity::Exact => encoder.u8(0),
        MinorCycleValidity::Bounded {
            maximum_absolute_update,
        } => {
            encoder.u8(1);
            encoder.u64(crate::canonical_f64_bits(maximum_absolute_update));
        }
    }
    match controls.cycle_threshold {
        Some(cycle) => {
            encoder.u8(1);
            encoder.u64(crate::canonical_f64_bits(cycle.factor));
            encoder.u64(crate::canonical_f64_bits(cycle.minimum_psf_fraction));
            encoder.u64(crate::canonical_f64_bits(cycle.maximum_psf_fraction));
        }
        None => encoder.u8(0),
    }
    match controls.fixed_cycle_threshold {
        Some(threshold) => {
            encoder.u8(1);
            encoder.u64(crate::canonical_f64_bits(threshold));
        }
        None => encoder.u8(0),
    }
    match controls.component_sequence_limit() {
        None => encoder.u8(0),
        Some(limit) => {
            encoder.u8(1);
            encoder.usize(limit);
        }
    }
    match controls.maximum_condition_number {
        Some(limit) => {
            encoder.u8(1);
            encoder.u64(crate::canonical_f64_bits(limit));
        }
        None => encoder.u8(0),
    }
    encoder.usize(iterations);
    encoder.usize(controller_iterations);
    encoder.u64(crate::canonical_f64_bits(total_flux));
    encoder.u64(crate::canonical_f64_bits(final_peak_flux));
    match noise_rms {
        Some(rms) => {
            encoder.u8(1);
            encoder.u64(crate::canonical_f64_bits(rms));
        }
        None => encoder.u8(0),
    }
    encoder.u64(crate::canonical_f64_bits(effective_threshold));
    encoder.u8(match stop_reason {
        MinorCycleStopReason::ThresholdReached => 0,
        MinorCycleStopReason::IterationBound => 1,
        MinorCycleStopReason::StalenessBound => 2,
        MinorCycleStopReason::MultiscaleDivergence => 3,
    });
    match clark {
        None => encoder.u8(0),
        Some(approximation) => {
            encoder.u8(1);
            encoder.usize(approximation.radius[0]);
            encoder.usize(approximation.radius[1]);
            encoder.u64(crate::canonical_f64_bits(
                approximation.maximum_exterior_sidelobe,
            ));
        }
    }
    encoder.usize(clark_refreshes);
    match image_domain_runs {
        None => encoder.u8(0),
        Some(runs) => {
            encoder.u8(1);
            encoder.usize(runs.len());
            for run in runs {
                encoder.usize(run.domain_ordinal);
                encoder.usize(run.iterations);
                encoder.usize(run.controller_iterations);
                encoder.u64(crate::canonical_f64_bits(run.total_flux));
                encoder.u64(crate::canonical_f64_bits(run.initial_peak_flux));
                encoder.u64(crate::canonical_f64_bits(run.final_peak_flux));
                match run.noise_rms {
                    Some(rms) => {
                        encoder.u8(1);
                        encoder.u64(crate::canonical_f64_bits(rms));
                    }
                    None => encoder.u8(0),
                }
                encoder.u64(crate::canonical_f64_bits(run.global_threshold));
                encoder.u64(crate::canonical_f64_bits(run.effective_threshold));
                match run.cycle_threshold {
                    Some(threshold) => {
                        encoder.u8(1);
                        encoder.u64(crate::canonical_f64_bits(threshold));
                    }
                    None => encoder.u8(0),
                }
                encoder.u8(image_domain_stop_priority(run.stop_reason));
            }
        }
    }
    MinorCycleEvidenceId(LogicalIdentity::from_sha256(encoder.finish()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use casa_imaging_model::{
        HogbomIterationAccounting, ModelCell, ReconstructionAlgorithm, ReconstructionBasis,
        ReconstructionControls,
    };
    use num_complex::Complex64;

    use super::{
        ImageDomainHogbomWork, MinorCycleComponent, MinorCycleModelPlane, MinorCycleProgram,
        MinorCycleStopReason, TaylorCandidate, TaylorSearchWindow, build_scale_kernels,
        minor_cycle_workspace_bytes, model_cell, multiscale_diverged, prefer_taylor_across_scales,
        prefer_taylor_within_scale, run_image_domain_hogbom_controllers, subtract_psf,
        subtract_psf_circular, taylor_psf_peak_index, taylor_rows_nearly_dependent,
        within_multiscale_border,
    };

    fn point_domain_work<'a>(
        domain_ordinal: usize,
        residual: f64,
        psf: &'a [Complex64],
    ) -> ImageDomainHogbomWork<'a> {
        ImageDomainHogbomWork {
            domain_ordinal,
            shape: [1, 1],
            psf,
            model_plane: MinorCycleModelPlane::new(domain_ordinal, 0, 0),
            psf_peak: 1.0,
            psf_peak_pixel: [0, 0],
            valid_support: vec![true].into_boxed_slice(),
            residual: vec![residual],
            noise_rms: None,
        }
    }

    fn casa_inclusive_program(max_iterations: usize, gain: f64) -> MinorCycleProgram {
        MinorCycleProgram::from_compiled(
            ReconstructionControls::new(max_iterations, gain, 0.0)
                .with_cycle_limits(max_iterations, None)
                .with_hogbom_iteration_accounting(HogbomIterationAccounting::CasaInclusive),
        )
        .expect("CASA-inclusive point-clean program")
        .record_component_sequence(32)
        .expect("bounded diagnostic component sequence")
    }

    #[test]
    fn image_domains_share_the_set_threshold_but_each_complete_the_full_set_budget() {
        let psf = [Complex64::new(1.0, 0.0)];
        let main_peak = 1.038_444_757_f64;
        let outlier_peak = 5.569_267_75_f64;
        let shared_cycle_threshold = 0.771_931_6_f64;
        let mut work = [
            point_domain_work(0, main_peak, &psf),
            point_domain_work(1, outlier_peak, &psf),
        ];
        let controls = casa_inclusive_program(10, 0.1);
        let mut terms = BTreeMap::new();
        let mut recorded = Vec::<MinorCycleComponent>::new();

        let evidence = run_image_domain_hogbom_controllers(
            &mut work,
            &controls,
            Some(shared_cycle_threshold),
            &mut terms,
            &mut recorded,
        )
        .expect("two canonical field controllers");

        assert_eq!(evidence.len(), 2);
        assert_eq!(
            (
                evidence[0].iterations(),
                evidence[0].controller_iterations(),
                evidence[0].stop_reason(),
            ),
            (3, 3, MinorCycleStopReason::ThresholdReached)
        );
        assert_eq!(
            (
                evidence[1].iterations(),
                evidence[1].controller_iterations(),
                evidence[1].stop_reason(),
            ),
            (11, 10, MinorCycleStopReason::IterationBound)
        );
        assert!(
            evidence
                .iter()
                .all(|run| run.cycle_threshold() == Some(shared_cycle_threshold))
        );
        assert_eq!(
            evidence.iter().map(|run| run.iterations()).sum::<usize>(),
            14
        );
        assert_eq!(
            evidence
                .iter()
                .map(|run| run.controller_iterations())
                .sum::<usize>(),
            13,
            "CASA compares niter only after every field finishes its controller"
        );
        let main_model = terms[&ModelCell::new(0, 0, 0, [0, 0])];
        let outlier_model = terms[&ModelCell::new(1, 0, 0, [0, 0])];
        assert!((main_model - main_peak * (1.0 - 0.9_f64.powi(3))).abs() < 1.0e-12);
        assert!((outlier_model - outlier_peak * (1.0 - 0.9_f64.powi(11))).abs() < 1.0e-12);
        assert_eq!(
            recorded
                .iter()
                .map(|component| component.cell().domain())
                .collect::<Vec<_>>(),
            [vec![0; 3], vec![1; 11]].concat()
        );
    }

    #[test]
    fn one_domain_collection_uses_the_same_shared_threshold_controller_path() {
        let psf = [Complex64::new(1.0, 0.0)];
        let peak = 1.038_444_757_f64;
        let shared_cycle_threshold = 0.771_931_6_f64;
        let mut work = [point_domain_work(0, peak, &psf)];
        let controls = casa_inclusive_program(10, 0.1);
        let mut terms = BTreeMap::new();
        let mut recorded = Vec::new();

        let evidence = run_image_domain_hogbom_controllers(
            &mut work,
            &controls,
            Some(shared_cycle_threshold),
            &mut terms,
            &mut recorded,
        )
        .expect("one-domain collection controller");

        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].domain_ordinal(), 0);
        assert_eq!(evidence[0].iterations(), 3);
        assert_eq!(evidence[0].controller_iterations(), 3);
        assert_eq!(evidence[0].cycle_threshold(), Some(shared_cycle_threshold));
        assert!(
            (terms[&ModelCell::new(0, 0, 0, [0, 0])] - peak * (1.0 - 0.9_f64.powi(3))).abs()
                < 1.0e-12
        );
        assert_eq!(recorded.len(), 3);
    }

    #[test]
    fn mtmfs_workspace_claim_scales_with_terms_and_effective_kernels() {
        let point = ReconstructionAlgorithm::Mtmfs {
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        };
        let multiscale = ReconstructionAlgorithm::Mtmfs {
            scales_px: vec![0.0, 5.0],
            small_scale_bias: 0.0,
        };
        let two_terms = minor_cycle_workspace_bytes(
            [128, 128],
            ReconstructionBasis::Taylor { terms: 2 },
            &point,
            8,
            64,
        );
        let three_terms = minor_cycle_workspace_bytes(
            [128, 128],
            ReconstructionBasis::Taylor { terms: 3 },
            &point,
            8,
            64,
        );
        let three_terms_multiscale = minor_cycle_workspace_bytes(
            [128, 128],
            ReconstructionBasis::Taylor { terms: 3 },
            &multiscale,
            8,
            64,
        );

        assert!(three_terms > two_terms);
        assert!(three_terms_multiscale > three_terms);
        assert!(three_terms >= 128 * 128 * 3 * std::mem::size_of::<f64>() as u64);
    }

    #[test]
    fn model_plane_coordinates_are_preserved_in_component_cells() {
        let cell = model_cell(MinorCycleModelPlane::new(2, 3, 1), [8, 8], [4, 5])
            .expect("pixel lies in the selected model plane");

        assert_eq!(cell.domain(), 2);
        assert_eq!(cell.coefficient(), 3);
        assert_eq!(cell.polarization(), 1);
        assert_eq!(cell.pixel(), [4, 5]);
    }

    #[test]
    fn multiscale_search_excludes_the_casa_one_and_a_half_scale_border() {
        assert!(within_multiscale_border([0, 0], [64, 64], 0));
        assert!(within_multiscale_border([63, 63], [64, 64], 0));
        assert!(!within_multiscale_border([10, 20], [64, 64], 10));
        assert!(within_multiscale_border([11, 20], [64, 64], 10));
        assert!(within_multiscale_border([52, 20], [64, 64], 10));
        assert!(!within_multiscale_border([53, 20], [64, 64], 10));
    }

    #[test]
    fn mtmfs_rejects_casa_nearly_dependent_hessian_rows() {
        assert!(taylor_rows_nearly_dependent(&[1.0, 2.0, 0.0, 4.0], 2));
        assert!(!taylor_rows_nearly_dependent(&[4.0, 1.0, 0.0, 3.0], 2));
        assert!(!taylor_rows_nearly_dependent(&[1.0, 0.0, 0.0, 1.0], 2));
    }

    #[test]
    fn mtmfs_psf_peak_is_confined_to_the_central_beam_patch() {
        let shape = [128, 128];
        let mut psf = vec![Complex64::new(0.0, 0.0); shape[0] * shape[1]];
        psf[0] = Complex64::new(2.0, 0.0);
        psf[64 * shape[1] + 64] = Complex64::new(1.0, 0.0);
        assert_eq!(
            taylor_psf_peak_index(&psf, shape, 0.0),
            Some(64 * shape[1] + 64)
        );
    }

    #[test]
    fn mtmfs_followup_search_is_confined_to_the_casa_psf_patch() {
        let window = TaylorSearchWindow::around([20, 20], 80, [256, 256]);
        assert!(window.contains([0, 0]));
        assert!(window.contains([79, 79]));
        assert!(!window.contains([80, 20]));
        assert!(!window.contains([200, 200]));

        let full_plane = TaylorSearchWindow::around([114, 49], 128, [128, 128]);
        assert_eq!(full_plane.start, [0, 0]);
        assert_eq!(full_plane.end_exclusive, [128, 128]);
        assert!(full_plane.contains([10, 96]));
    }

    #[test]
    fn mtmfs_scale_selection_preserves_casa_signed_two_stage_order() {
        let kernels = build_scale_kernels(&[0.0, 2.0], 0.0);
        let negative_large = TaylorCandidate {
            index: 0,
            scale_index: 0,
            coefficients: Vec::new(),
            score: -10.0,
        };
        let negative_small = TaylorCandidate {
            index: 1,
            scale_index: 1,
            coefficients: Vec::new(),
            score: -2.0,
        };
        assert!(prefer_taylor_across_scales(
            &negative_small,
            &negative_large,
            &kernels
        ));
        let positive_tie = TaylorCandidate {
            index: 2,
            scale_index: 0,
            coefficients: Vec::new(),
            score: 10.0,
        };
        assert!(prefer_taylor_within_scale(&positive_tie, &negative_large));
    }

    #[test]
    fn multiscale_divergence_boundary_requires_prior_progress_and_exceeds_one_half() {
        assert!(!multiscale_diverged(2.0, 4.0, 0));
        assert!(!multiscale_diverged(2.0, 3.0, 1));
        assert!(multiscale_diverged(2.0, 3.000_000_000_1, 1));
    }

    #[test]
    fn clark_subcycle_refresh_recomputes_the_exact_full_residual() {
        let shape = [3, 3];
        let dirty = vec![Complex64::new(0.0, 0.0); 9];
        let mut dirty = dirty;
        dirty[0] = Complex64::new(3.0, 0.0);
        let mut psf = vec![Complex64::new(0.0, 0.0); 9];
        psf[4] = Complex64::new(1.0, 0.0);
        psf[3] = Complex64::new(0.25, 0.0);
        let mut refreshed = dirty.iter().map(|value| value.re).collect::<Vec<_>>();
        subtract_psf(&mut refreshed, &psf, shape, [0, 0], [1, 1], 2.0)
            .expect("first exact full subtraction");
        subtract_psf(&mut refreshed, &psf, shape, [2, 2], [1, 1], -1.5)
            .expect("second exact full subtraction");
        assert_eq!(
            refreshed,
            vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.375, 1.5]
        );
    }

    #[test]
    fn multiscale_kernels_are_compact_and_unit_normalized() {
        let kernels = build_scale_kernels(&[0.0, 3.0], 0.6);
        assert_eq!(kernels[0].samples.as_slice(), &[([0, 0], 1.0)]);
        assert!(
            (kernels[1]
                .samples
                .iter()
                .map(|(_, value)| value)
                .sum::<f64>()
                - 1.0)
                .abs()
                < 1.0e-12
        );
        assert!(kernels[1].samples.iter().all(|(offset, _)| {
            (offset[0] as f64 / 3.0).powi(2) + (offset[1] as f64 / 3.0).powi(2) < 1.0
        }));
    }

    #[test]
    fn shifted_psf_overlap_can_start_before_the_component_peak() {
        let mut residual = vec![0.0; 9];
        let psf = vec![Complex64::new(1.0, 0.0); 9];

        subtract_psf(&mut residual, &psf, [3, 3], [2, 2], [1, 1], 1.0)
            .expect("valid clipped negative shift");

        assert_eq!(
            residual,
            vec![0.0, 0.0, 0.0, 0.0, -1.0, -1.0, 0.0, -1.0, -1.0]
        );
    }

    #[test]
    fn multiscale_terminal_refresh_uses_casa_circular_psf_convolution() {
        let mut residual = vec![0.0; 9];
        let psf = (1..=9)
            .map(|value| Complex64::new(f64::from(value), 0.0))
            .collect::<Vec<_>>();

        subtract_psf_circular(&mut residual, &psf, [3, 3], [0, 0], [1, 1], 1.0)
            .expect("finite circular subtraction");

        assert_eq!(
            residual,
            vec![-5.0, -6.0, -4.0, -8.0, -9.0, -7.0, -2.0, -3.0, -1.0]
        );
    }
}
