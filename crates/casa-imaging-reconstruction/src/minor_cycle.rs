// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded Högbom Minor Cycle over authoritative Normal State views.
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
    CompiledProblemId, LogicalIdentity, ModelCell, ModelDeltaTerm, ModelExecutionAttemptId,
    ModelSupport, ModelValue, ReconstructionControls,
};
use thiserror::Error;

use crate::{
    Encoder, FinalNormalState, FinalNormalStateCompletionId, ModelDelta, ModelGeneration,
    ModelGenerationId, ModelLifecycle, ModelLifecycleError,
};

const MINOR_CYCLE_EVIDENCE_DOMAIN: &[u8] = b"casa-rs-minor-cycle-evidence";
const MINOR_CYCLE_EVIDENCE_VERSION: u32 = 1;

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
    "Stable identity of one bounded Högbom Minor-Cycle solve."
);

/// Explicit Högbom Minor-Cycle controls.
///
/// Every control is explicit and validated; there are no defaults that could
/// silently change deconvolution semantics. `maximum_model_update` is the
/// error/staleness bound of the linear view envelope: before any component
/// is applied, the candidate cumulative absolute flux is checked against it,
/// and a candidate that would exceed the bound is rejected without touching
/// the residual, the delta, recorded diagnostics, or evidence counters. The
/// solve then stops and requests Major-Cycle reconciliation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HogbomControls {
    gain: f64,
    threshold: f64,
    max_iterations: usize,
    maximum_model_update: f64,
    component_sequence_limit: Option<usize>,
}

impl HogbomControls {
    /// Derive the executable minor-cycle controls from the compiled contract.
    ///
    /// Unlike the general model constructor, this production seam requires an
    /// explicit staleness envelope and never invents an unbounded default.
    pub fn from_compiled(controls: ReconstructionControls) -> Result<Self, MinorCycleError> {
        let maximum_model_update = controls
            .maximum_model_update()
            .ok_or(MinorCycleError::MissingMaximumModelUpdate)?;
        Self::new(
            controls.gain(),
            controls.threshold_jy_per_beam(),
            controls.max_minor_iterations(),
            maximum_model_update,
        )
    }

    /// Construct validated controls.
    ///
    /// # Errors
    ///
    /// Rejects a gain outside `(0, 1]`, a negative or non-finite threshold, a
    /// zero iteration bound, and a non-positive or non-finite maximum model
    /// update.
    pub fn new(
        gain: f64,
        threshold: f64,
        max_iterations: usize,
        maximum_model_update: f64,
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
        if !maximum_model_update.is_finite() || maximum_model_update <= 0.0 {
            return Err(MinorCycleError::InvalidMaximumModelUpdate);
        }
        Ok(Self {
            gain,
            threshold,
            max_iterations,
            maximum_model_update,
            component_sequence_limit: None,
        })
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

    /// Return the hard iteration bound.
    #[must_use]
    pub const fn max_iterations(&self) -> usize {
        self.max_iterations
    }

    /// Return the cumulative absolute component flux that exhausts the view
    /// envelope and forces an explicit reconciliation request.
    #[must_use]
    pub const fn maximum_model_update(&self) -> f64 {
        self.maximum_model_update
    }

    /// Return the optional recorded-component-sequence capacity.
    #[must_use]
    pub const fn component_sequence_limit(&self) -> Option<usize> {
        self.component_sequence_limit
    }
}

/// Explicit valid-support window constraining component placement.
///
/// Bounds are inclusive pixel coordinates `[x, y]`. Only window cells whose
/// named model-generation sample has valid support may host a component,
/// because only the model owner accepts delta terms on valid support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanWindow {
    blc: [usize; 2],
    trc: [usize; 2],
}

impl CleanWindow {
    /// Construct an inclusive window with `blc <= trc` on both axes.
    ///
    /// # Errors
    ///
    /// Rejects an inverted window.
    pub fn new(blc: [usize; 2], trc: [usize; 2]) -> Result<Self, MinorCycleError> {
        if blc[0] > trc[0] || blc[1] > trc[1] {
            return Err(MinorCycleError::InvertedWindow);
        }
        Ok(Self { blc, trc })
    }

    /// The CASA default inner-quarter window for one plane shape.
    #[must_use]
    pub fn inner_quarter(shape: [usize; 2]) -> Self {
        let half = [shape[0] / 2, shape[1] / 2];
        let quarter = [shape[0] / 4, shape[1] / 4];
        let mut blc = quarter;
        let mut trc = [
            quarter[0].saturating_add(half[0]).saturating_sub(1),
            quarter[1].saturating_add(half[1]).saturating_sub(1),
        ];
        for axis in 0..2 {
            trc[axis] = trc[axis].min(shape[axis] - 1);
            blc[axis] = blc[axis].min(trc[axis]);
        }
        Self { blc, trc }
    }

    /// The full plane for one shape.
    #[must_use]
    pub fn full_plane(shape: [usize; 2]) -> Self {
        Self {
            blc: [0, 0],
            trc: [shape[0] - 1, shape[1] - 1],
        }
    }

    /// Return the inclusive lower bound.
    #[must_use]
    pub const fn blc(&self) -> [usize; 2] {
        self.blc
    }

    /// Return the inclusive upper bound.
    #[must_use]
    pub const fn trc(&self) -> [usize; 2] {
        self.trc
    }

    fn contains(&self, pixel: [usize; 2]) -> bool {
        pixel[0] >= self.blc[0]
            && pixel[0] <= self.trc[0]
            && pixel[1] >= self.blc[1]
            && pixel[1] <= self.trc[1]
    }
}

/// One accepted Högbom component in canonical typed model coordinates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HogbomComponent {
    cell: ModelCell,
    flux: f64,
}

impl HogbomComponent {
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

    fn same_value(&self, other: &Self) -> bool {
        self.cell == other.cell && self.flux.to_bits() == other.flux.to_bits()
    }
}

/// Informational first divergence between two recorded component sequences.
///
/// This record never gates acceptance; it exists so parity trajectories can
/// report where two solvers first disagree without failing either side.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ComponentDivergence {
    index: usize,
    baseline: Option<HogbomComponent>,
    candidate: Option<HogbomComponent>,
}

impl ComponentDivergence {
    /// Return the first divergent sequence index.
    #[must_use]
    pub const fn index(&self) -> usize {
        self.index
    }

    /// Return the baseline entry, or `None` when the baseline ended here.
    #[must_use]
    pub const fn baseline(&self) -> Option<HogbomComponent> {
        self.baseline
    }

    /// Return the candidate entry, or `None` when the candidate ended here.
    #[must_use]
    pub const fn candidate(&self) -> Option<HogbomComponent> {
        self.candidate
    }
}

/// Why one bounded Högbom solve stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinorCycleStopReason {
    /// The normalized residual peak fell strictly below the explicit
    /// threshold (the casacore HOGBOM convention; a peak exactly at the
    /// threshold still cleans one component).
    ThresholdReached,
    /// The hard iteration bound was reached with work potentially remaining.
    IterationBound,
    /// Accepting the next candidate would push the cumulative component
    /// flux past the maximum model update, so continuing would extrapolate
    /// the frozen approximation beyond its validity envelope. The rejected
    /// candidate left no trace in the delta or evidence.
    StalenessBound,
}

/// Owner-minted evidence of one bounded Högbom solve.
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
    total_flux: f64,
    final_peak_flux: f64,
    stop_reason: MinorCycleStopReason,
    recorded: Option<Box<[HogbomComponent]>>,
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

    /// Return the number of applied Högbom components.
    #[must_use]
    pub const fn iterations(&self) -> usize {
        self.iterations
    }

    /// Return the cumulative absolute component flux.
    #[must_use]
    pub const fn total_flux(&self) -> f64 {
        self.total_flux
    }

    /// Return the last evaluated normalized residual peak magnitude.
    #[must_use]
    pub const fn final_peak_flux(&self) -> f64 {
        self.final_peak_flux
    }

    /// Return why the solve stopped.
    #[must_use]
    pub const fn stop_reason(&self) -> MinorCycleStopReason {
        self.stop_reason
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
    pub fn recorded_component_sequence(&self) -> Option<&[HogbomComponent]> {
        self.recorded.as_deref()
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

/// One bounded Högbom solve: the owner-minted Model Delta plus its evidence.
#[derive(Debug)]
pub struct HogbomMinorCycle {
    delta: Option<ModelDelta>,
    evidence: MinorCycleEvidence,
}

impl HogbomMinorCycle {
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

/// Exact reason a bounded Högbom solve failed closed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MinorCycleError {
    /// The compiled problem omitted the mandatory linear-view envelope.
    #[error("compiled Högbom controls require an explicit maximum model update")]
    MissingMaximumModelUpdate,
    /// The Högbom gain was outside `(0, 1]`.
    #[error("Högbom gain must lie in (0, 1]")]
    InvalidGain,
    /// The stopping threshold was negative or non-finite.
    #[error("Högbom threshold must be finite and non-negative")]
    InvalidThreshold,
    /// The iteration bound was zero.
    #[error("Högbom iteration bound must be non-zero")]
    InvalidIterationBound,
    /// The maximum model update was non-positive or non-finite.
    #[error("Högbom maximum model update must be finite and positive")]
    InvalidMaximumModelUpdate,
    /// Component-sequence recording requested a zero capacity.
    #[error("component-sequence recording requires a non-zero limit")]
    InvalidRecordingLimit,
    /// The window bounds were inverted.
    #[error("clean window requires blc <= trc on both axes")]
    InvertedWindow,
    /// The window lay partly or wholly outside the normal-state plane.
    #[error("clean window lies outside the normal-state plane")]
    WindowOutsidePlane,
    /// The window contained no valid model support at all.
    #[error("clean window contains no valid model support")]
    EmptyValidSupport,
    /// The named generation does not match the single-plane constant-basis
    /// normal-state geometry.
    #[error("model generation does not match the normal-state plane")]
    ModelShapeMismatch,
    /// The named generation is not the view's final model generation.
    #[error("named generation is not the normal state's final model generation")]
    ForeignNormalState,
    /// The normal approximation lacked a positive finite peak.
    #[error("normal-state approximation lacks a positive finite PSF peak")]
    InvalidPsfPeak,
    /// Solver arithmetic produced a non-finite value.
    #[error("minor-cycle arithmetic generated a non-finite value")]
    GeneratedNonfinite,
    /// The model lifecycle owner rejected delta validation or minting.
    #[error(transparent)]
    Lifecycle(#[from] crate::ModelLifecycleError),
}

impl From<casa_imaging_model::ModelContractError> for MinorCycleError {
    fn from(error: casa_imaging_model::ModelContractError) -> Self {
        Self::Lifecycle(ModelLifecycleError::Contract(error))
    }
}

/// Run one bounded Högbom Minor Cycle.
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
/// peak cell. Scalar Stokes-I convention uses the real plane, matching the
/// Float-image reference behavior.
///
/// # Errors
///
/// Fails closed on mismatched lineage or geometry, invalid controls or
/// windows, a degenerate PSF peak, non-finite arithmetic, and every model
/// owner rejection (term bounds, value bounds, closed lifecycle).
pub fn hogbom_minor_cycle(
    lifecycle: &ModelLifecycle,
    base: &ModelGeneration,
    view: &FinalNormalState,
    window: CleanWindow,
    controls: HogbomControls,
) -> Result<HogbomMinorCycle, MinorCycleError> {
    let shape = view.shape();
    let cells = shape[0] * shape[1];
    if base.shape().domains().len() != 1
        || base.shape().coefficients() != 1
        || base.shape().polarizations() != 1
        || base.shape().domains()[0].pixels() != shape
        || base.samples().len() != cells
    {
        return Err(MinorCycleError::ModelShapeMismatch);
    }
    if base.generation_id() != view.final_model_generation() {
        return Err(MinorCycleError::ForeignNormalState);
    }
    if window.trc()[0] >= shape[0] || window.trc()[1] >= shape[1] {
        return Err(MinorCycleError::WindowOutsidePlane);
    }

    // The PSF peak normalization follows the reference cleaner: peaks are
    // reported in model units regardless of the accumulated weight scale.
    let psf_peak_index = find_peak_abs(view.normal_approximation(), shape, |v| v.re, |_| true)
        .ok_or(MinorCycleError::InvalidPsfPeak)?;
    let psf_peak = view.normal_approximation()[psf_peak_index].re;
    if !psf_peak.is_finite() || psf_peak <= 0.0 {
        return Err(MinorCycleError::InvalidPsfPeak);
    }
    let psf_peak_pixel = plane_pixel(psf_peak_index, shape);

    // Private working copy: authoritative state is never mutated.
    let mut residual = Vec::with_capacity(cells);
    for value in view.residual() {
        let real = value.re;
        if !real.is_finite() {
            return Err(MinorCycleError::GeneratedNonfinite);
        }
        residual.push(real);
    }

    let mut terms = BTreeMap::<usize, f64>::new();
    let mut recorded = Vec::with_capacity(
        controls
            .component_sequence_limit()
            .unwrap_or(0)
            .min(controls.max_iterations()),
    );
    let mut iterations = 0_usize;
    let mut total_flux = 0.0_f64;
    let mut final_peak_flux = 0.0_f64;
    let mut stop_reason = None;

    for _ in 0..controls.max_iterations() {
        let Some(peak_index) = find_peak_abs(
            &residual,
            shape,
            |value| *value,
            |pixel| window.contains(pixel) && valid_support(base, shape, pixel),
        ) else {
            return Err(MinorCycleError::EmptyValidSupport);
        };
        let peak_pixel = plane_pixel(peak_index, shape);
        let strength = residual[peak_index] / psf_peak;
        if !strength.is_finite() {
            return Err(MinorCycleError::GeneratedNonfinite);
        }
        final_peak_flux = strength.abs();
        // The casacore HOGBOM cleaner stops only when the normalized peak is
        // strictly below the threshold (`lattices/LatticeMath/
        // LatticeCleaner.tcc`, stopping rule 1: "stop if below threshold",
        // tested as `abs(itsStrengthOptimum) < threshold()`), so a peak
        // exactly at the threshold still cleans one component. A zero peak
        // has no flux to clean and converges trivially.
        if strength == 0.0 || strength.abs() < controls.threshold() {
            stop_reason = Some(MinorCycleStopReason::ThresholdReached);
            break;
        }
        let flux = controls.gain() * strength;
        if !flux.is_finite() {
            return Err(MinorCycleError::GeneratedNonfinite);
        }
        // Envelope guard: a candidate whose acceptance would push the
        // cumulative absolute update past the maximum model update is
        // rejected before it can touch the working residual, the accumulated
        // delta terms, the recorded component sequence, or any evidence
        // counter. Accepted components therefore never exceed the linear
        // view envelope.
        if total_flux + flux.abs() > controls.maximum_model_update() {
            stop_reason = Some(MinorCycleStopReason::StalenessBound);
            break;
        }
        subtract_psf(
            &mut residual,
            view.normal_approximation(),
            shape,
            peak_pixel,
            psf_peak_pixel,
            flux,
        )?;
        iterations += 1;
        total_flux += flux.abs();
        let cell =
            model_cell(shape, peak_pixel).expect("a scanned peak pixel lies inside the plane");
        *terms.entry(canonical_flat(base, cell)).or_insert(0.0) += flux;
        if let Some(limit) = controls.component_sequence_limit()
            && recorded.len() < limit
        {
            recorded.push(HogbomComponent { cell, flux });
        }
    }
    let stop_reason = stop_reason.unwrap_or(MinorCycleStopReason::IterationBound);

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
        &window,
        &controls,
        iterations,
        total_flux,
        final_peak_flux,
        stop_reason,
    );
    Ok(HogbomMinorCycle {
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
            total_flux,
            final_peak_flux,
            stop_reason,
            recorded: (!recorded.is_empty()).then(|| recorded.into_boxed_slice()),
        },
    })
}

/// Canonical single-plane pixel of one plane-storage index.
///
/// Normal-state planes are stored x-major (`finish_bound` pushes x outer, y
/// inner), so index `p` maps to pixel `[p / H, p % H]`.
fn plane_pixel(index: usize, shape: [usize; 2]) -> [usize; 2] {
    [index / shape[1], index % shape[1]]
}

fn model_cell(shape: [usize; 2], pixel: [usize; 2]) -> Option<ModelCell> {
    if pixel[0] < shape[0] && pixel[1] < shape[1] {
        Some(ModelCell::new(0, 0, 0, pixel))
    } else {
        None
    }
}

fn canonical_flat(base: &ModelGeneration, cell: ModelCell) -> usize {
    base.shape()
        .flat_index(cell)
        .expect("component pixels stay inside the base shape")
}

fn valid_support(base: &ModelGeneration, shape: [usize; 2], pixel: [usize; 2]) -> bool {
    let Some(cell) = model_cell(shape, pixel) else {
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
            let source = [(x - peak[0] + psf_peak[0]), (y - peak[1] + psf_peak[1])];
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
    window: &CleanWindow,
    controls: &HogbomControls,
    iterations: usize,
    total_flux: f64,
    final_peak_flux: f64,
    stop_reason: MinorCycleStopReason,
) -> MinorCycleEvidenceId {
    let mut encoder = Encoder::new(MINOR_CYCLE_EVIDENCE_DOMAIN, MINOR_CYCLE_EVIDENCE_VERSION);
    encoder.identity(authority.as_bytes());
    encoder.identity(attempt.identity().as_bytes());
    encoder.u64(epoch);
    encoder.identity(input_generation.as_bytes());
    encoder.identity(normal_state_completion.as_bytes());
    encoder.identity(normal_state_content.as_bytes());
    encoder.usize(window.blc()[0]);
    encoder.usize(window.blc()[1]);
    encoder.usize(window.trc()[0]);
    encoder.usize(window.trc()[1]);
    encoder.u64(crate::canonical_f64_bits(controls.gain()));
    encoder.u64(crate::canonical_f64_bits(controls.threshold()));
    encoder.usize(controls.max_iterations());
    encoder.u64(crate::canonical_f64_bits(controls.maximum_model_update()));
    match controls.component_sequence_limit() {
        None => encoder.u8(0),
        Some(limit) => {
            encoder.u8(1);
            encoder.usize(limit);
        }
    }
    encoder.usize(iterations);
    encoder.u64(crate::canonical_f64_bits(total_flux));
    encoder.u64(crate::canonical_f64_bits(final_peak_flux));
    encoder.u8(match stop_reason {
        MinorCycleStopReason::ThresholdReached => 0,
        MinorCycleStopReason::IterationBound => 1,
        MinorCycleStopReason::StalenessBound => 2,
    });
    MinorCycleEvidenceId(LogicalIdentity::from_sha256(encoder.finish()))
}
