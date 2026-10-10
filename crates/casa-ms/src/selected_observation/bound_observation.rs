// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{CompiledProblem, LogicalIdentity};
use std::{mem::size_of, sync::Arc};
use thiserror::Error;

use crate::selected_pointing::SelectedPointingQueryDomain;

use super::access::{BoundObservationReferenceData, SelectedRowReplay};
use super::{
    BoundObservationSource, BoundObservationSourceError, SelectedObservationBlock,
    SelectedObservationContentBudget, SelectedObservationContentRequirements,
    SelectedObservationMeasures, SelectedObservationMeasuresError,
    content_plan::SelectedObservationSharedBytes,
};

/// One snapshot source and its bounded-content budget.
///
/// The content budget is the sole physical blocking authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationSourceBinding {
    measurement_set: usize,
    content_budget: SelectedObservationContentBudget,
    ephemeris: Option<Arc<crate::SelectedObservationEphemeris>>,
    pointing_query_domain: Option<SelectedPointingQueryDomain>,
}

/// Opaque storage-owner certificate for one complete selected-observation residency contract.
///
/// The certificate is derived only from the compiler's canonical source set and
/// every source binding supplied to [`BoundSelectedObservation::open`]. Callers
/// can inspect the aggregate hard bound and peak queue depth needed by a
/// scheduler, but cannot construct or alter the per-source facts that bind those
/// values to the retained owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedObservationResidencyCertificate {
    sources: Vec<SelectedObservationSourceResidency>,
    aggregate_resident_bytes: usize,
    aggregate_reference_data_bytes: usize,
    peak_live_blocks: usize,
    maximum_pointing_polynomial_terms: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SelectedObservationSourceResidency {
    measurement_set: usize,
    content_budget: SelectedObservationContentBudget,
    reference_data_bytes: usize,
}

impl SelectedObservationResidencyCertificate {
    fn mint(
        problem: &CompiledProblem,
        bindings: &[ObservationSourceBinding],
    ) -> Result<Self, BoundSelectedObservationError> {
        let expected = problem.inputs().observation_snapshot().sources();
        if bindings.len() != expected.len() {
            return Err(BoundSelectedObservationError::BindingSetMismatch);
        }
        let mut aggregate_resident_bytes = 0_usize;
        let mut aggregate_reference_data_bytes = 0_usize;
        let mut peak_live_blocks = 0_usize;
        let mut maximum_pointing_polynomial_terms = 0_usize;
        let mut sources = Vec::with_capacity(expected.len());
        for source in expected {
            let measurement_set = source.input_ordinal();
            let binding = source_binding(bindings, measurement_set)?;
            let expected_ephemeris = problem.geometry().ephemeris_reference();
            let actual_ephemeris = binding.ephemeris_identity();
            if actual_ephemeris != expected_ephemeris {
                return Err(BoundSelectedObservationError::EphemerisReferenceMismatch {
                    measurement_set,
                    expected: expected_ephemeris,
                    actual: actual_ephemeris,
                });
            }
            let content_budget = binding.content_budget();
            let reference_data_bytes = binding.reference_data_bytes();
            if reference_data_bytes > content_budget.available_bytes() {
                return Err(BoundSelectedObservationError::ReferenceDataBudgetExceeded {
                    measurement_set,
                    required_bytes: reference_data_bytes,
                    available_bytes: content_budget.available_bytes(),
                });
            }
            aggregate_resident_bytes = aggregate_resident_bytes
                .checked_add(content_budget.available_bytes())
                .ok_or(BoundSelectedObservationError::ResidencyByteOverflow)?;
            aggregate_reference_data_bytes = aggregate_reference_data_bytes
                .checked_add(reference_data_bytes)
                .ok_or(BoundSelectedObservationError::ReferenceDataByteOverflow)?;
            peak_live_blocks = peak_live_blocks.max(content_budget.maximum_live_blocks());
            maximum_pointing_polynomial_terms = maximum_pointing_polynomial_terms
                .max(content_budget.maximum_pointing_polynomial_terms());
            sources.push(SelectedObservationSourceResidency {
                measurement_set,
                content_budget,
                reference_data_bytes,
            });
        }
        Ok(Self {
            sources,
            aggregate_resident_bytes,
            aggregate_reference_data_bytes,
            peak_live_blocks,
            maximum_pointing_polynomial_terms,
        })
    }

    /// Return the aggregate hard byte ceiling across every retained source owner.
    #[must_use]
    pub const fn aggregate_resident_bytes(&self) -> usize {
        self.aggregate_resident_bytes
    }

    /// Return the exact immutable reference-data allocation retained by all bindings.
    #[must_use]
    pub const fn aggregate_reference_data_bytes(&self) -> usize {
        self.aggregate_reference_data_bytes
    }

    /// Return the peak simultaneously live selected-content block count.
    ///
    /// Sources are traversed serially in canonical order, so this is the maximum
    /// source-local queue depth rather than the sum of mutually exclusive depths.
    #[must_use]
    pub const fn peak_live_blocks(&self) -> usize {
        self.peak_live_blocks
    }

    /// Return the largest source-local POINTING polynomial term ceiling.
    #[must_use]
    pub const fn maximum_pointing_polynomial_terms(&self) -> usize {
        self.maximum_pointing_polynomial_terms
    }

    /// Return the exact source-local budget certified for one snapshot source.
    #[must_use]
    pub fn content_budget(
        &self,
        measurement_set: usize,
    ) -> Option<SelectedObservationContentBudget> {
        self.sources.iter().find_map(|source| {
            (source.measurement_set == measurement_set).then_some(source.content_budget)
        })
    }

    /// Return one source binding's exact retained reference-data allocation.
    #[must_use]
    pub fn reference_data_bytes(&self, measurement_set: usize) -> Option<usize> {
        self.sources.iter().find_map(|source| {
            (source.measurement_set == measurement_set).then_some(source.reference_data_bytes)
        })
    }
}

/// Find the one binding for a snapshot source.
fn source_binding(
    bindings: &[ObservationSourceBinding],
    measurement_set: usize,
) -> Result<&ObservationSourceBinding, BoundSelectedObservationError> {
    let mut matches = bindings
        .iter()
        .filter(|binding| binding.measurement_set == measurement_set);
    let binding = matches
        .next()
        .ok_or(BoundSelectedObservationError::MissingSourceBinding { measurement_set })?;
    if matches.next().is_some() {
        return Err(BoundSelectedObservationError::DuplicateSourceBinding { measurement_set });
    }
    Ok(binding)
}

impl ObservationSourceBinding {
    /// Bind one snapshot source, by its position, to an explicit content budget.
    #[must_use]
    pub const fn new(
        measurement_set: usize,
        content_budget: SelectedObservationContentBudget,
    ) -> Self {
        Self {
            measurement_set,
            content_budget,
            ephemeris: None,
            pointing_query_domain: None,
        }
    }

    /// Attach immutable moving-source data owned by this selected source.
    #[must_use]
    pub fn with_ephemeris(
        mut self,
        ephemeris: Option<crate::SelectedObservationEphemeris>,
    ) -> Self {
        self.ephemeris = ephemeris.map(Arc::new);
        self
    }

    pub(crate) fn with_pointing_query_domain(
        mut self,
        pointing_query_domain: SelectedPointingQueryDomain,
    ) -> Self {
        self.pointing_query_domain = Some(pointing_query_domain);
        self
    }

    /// Return the bound source's position in the snapshot.
    #[must_use]
    pub const fn measurement_set(&self) -> usize {
        self.measurement_set
    }

    /// Return the explicit selected-content memory budget.
    #[must_use]
    pub const fn content_budget(&self) -> SelectedObservationContentBudget {
        self.content_budget
    }

    pub(crate) fn set_content_budget(&mut self, budget: SelectedObservationContentBudget) {
        self.content_budget = budget;
    }

    fn ephemeris_identity(&self) -> Option<LogicalIdentity> {
        self.ephemeris
            .as_deref()
            .map(crate::SelectedObservationEphemeris::identity)
    }

    pub(crate) fn pointing_query_domain(&self) -> Option<&SelectedPointingQueryDomain> {
        self.pointing_query_domain.as_ref()
    }

    fn additional_retained_heap_bytes(&self) -> usize {
        self.pointing_query_domain
            .as_ref()
            .map_or(0, SelectedPointingQueryDomain::retained_bytes)
    }

    /// Return the exact ephemeris allocation retained by this source binding.
    #[must_use]
    pub fn reference_data_bytes(&self) -> usize {
        self.ephemeris
            .as_deref()
            .map_or(0, |ephemeris| ephemeris.retained_bytes())
    }
}

/// An unopened selected-observation capability for an admitted source-read operation.
///
/// It retains the source bindings and Measures capability, but no MeasurementSet
/// locks, prepared POINTING catalogs, or selected-content blocks. Multi-source
/// bindings keep the same canonical ordering and validation as
/// [`BoundSelectedObservation::open`].
pub struct DeferredSelectedObservationAccess {
    measures: SelectedObservationMeasures,
    bindings: Vec<ObservationSourceBinding>,
}

impl DeferredSelectedObservationAccess {
    /// Defer binding until its source-read allocation exists.
    #[must_use]
    pub fn new(
        measures: SelectedObservationMeasures,
        bindings: Vec<ObservationSourceBinding>,
    ) -> Self {
        Self { measures, bindings }
    }

    /// Derive the unchanged aggregate source-residency certificate without opening tables.
    pub fn certify_residency(
        &self,
        problem: &CompiledProblem,
    ) -> Result<SelectedObservationResidencyCertificate, BoundSelectedObservationError> {
        BoundSelectedObservation::certify_residency(problem, &self.bindings)
    }

    /// Open under fresh read locks only when execution admits this source owner.
    #[cfg(unix)]
    pub fn open(
        self,
        problem: &CompiledProblem,
    ) -> Result<BoundSelectedObservation, BoundSelectedObservationError> {
        BoundSelectedObservation::open(problem, self.measures, self.bindings)
    }
}

/// Retained read-locked access to every source in one compiled selected observation.
///
/// Each traversal is one ordered stream of row blocks; completing the stream
/// returns this access for the next traversal. Storage buffers and POINTING
/// lookup primitives are owner-internal.
///
/// ```compile_fail
/// use casa_ms::{SelectedObservationBuffer, SelectedObservationBufferRequest};
///
/// let _ = std::mem::size_of::<SelectedObservationBuffer>();
/// let _ = std::mem::size_of::<SelectedObservationBufferRequest>();
/// ```
pub struct BoundSelectedObservation {
    residency: SelectedObservationResidencyCertificate,
    measures: SelectedObservationMeasures,
    sources: Vec<BoundObservationSource>,
}

impl BoundSelectedObservation {
    /// The distinct CASA aperture classes of the selected sources' antennas
    /// in class order (`HetArrayConvFunc::findAntennaSizes`: one class per
    /// distinct dish), empty when no antenna is an ALMA or ACA dish.
    #[must_use]
    pub fn antenna_response_classes(&self) -> Vec<casa_imaging_model::AntennaResponseClass> {
        let mut classes = self
            .sources
            .iter()
            .flat_map(|source| source.antenna_response_classes().iter().flatten().copied())
            .collect::<Vec<_>>();
        classes.sort_unstable();
        classes.dedup();
        classes
    }

    #[cfg(unix)]
    pub(crate) fn single_source_content_requirements(
        problem: &CompiledProblem,
        measures: &SelectedObservationMeasures,
        binding: &ObservationSourceBinding,
    ) -> Result<SelectedObservationContentRequirements, BoundSelectedObservationError> {
        let expected = problem.inputs().observation_snapshot().sources();
        if expected.len() != 1 {
            return Err(BoundSelectedObservationError::BindingSetMismatch);
        }
        let source = &expected[0];
        if source.input_ordinal() != binding.measurement_set() {
            return Err(BoundSelectedObservationError::MissingSourceBinding {
                measurement_set: source.input_ordinal(),
            });
        }
        // Resolved access opens with vec![binding] and one prospective source slot.
        let shared = Self::shared_bytes(measures, std::slice::from_ref(binding), 1, 1)?;
        BoundObservationSource::content_requirements(problem, source, binding, measures, shared)
            .map_err(|error| BoundSelectedObservationError::Source {
                measurement_set: source.input_ordinal(),
                error: Box::new(error),
            })
    }

    /// Mint the opaque aggregate residency contract for a complete source-binding set.
    ///
    /// The same canonical derivation is repeated and retained by [`Self::open`],
    /// allowing a scheduler to plan before opening while execution still fails
    /// closed if a different owner or budget set is later supplied.
    pub fn certify_residency(
        problem: &CompiledProblem,
        bindings: &[ObservationSourceBinding],
    ) -> Result<SelectedObservationResidencyCertificate, BoundSelectedObservationError> {
        SelectedObservationResidencyCertificate::mint(problem, bindings)
    }

    fn shared_bytes(
        measures: &SelectedObservationMeasures,
        bindings: &[ObservationSourceBinding],
        binding_capacity: usize,
        source_capacity: usize,
    ) -> Result<SelectedObservationSharedBytes, BoundSelectedObservationError> {
        let binding_slot_bytes = binding_capacity
            .checked_mul(size_of::<ObservationSourceBinding>())
            .ok_or(BoundSelectedObservationError::BindingGraphByteOverflow)?;
        let binding_graph_initialization_bytes =
            bindings
                .iter()
                .try_fold(binding_slot_bytes, |bytes, binding| {
                    bytes
                        .checked_add(binding.additional_retained_heap_bytes())
                        .ok_or(BoundSelectedObservationError::BindingGraphByteOverflow)
                })?;
        let reference_data_retained_bytes =
            bindings.iter().try_fold(0_usize, |bytes, binding| {
                bytes
                    .checked_add(binding.reference_data_bytes())
                    .ok_or(BoundSelectedObservationError::ReferenceDataByteOverflow)
            })?;
        let source_slots_retained_bytes = source_capacity
            .checked_mul(BoundObservationSource::retained_source_slot_bytes())
            .ok_or(BoundSelectedObservationError::SourceSlotByteOverflow)?;
        Ok(SelectedObservationSharedBytes::new(
            measures.retained_bytes(),
            reference_data_retained_bytes,
            source_slots_retained_bytes,
            binding_graph_initialization_bytes,
        ))
    }

    /// Open every compiled source under retained read locks and its content budget.
    ///
    /// Caller plan order is irrelevant. Sources are retained and replayed only in the compiler's
    /// canonical read-set order.
    #[cfg(unix)]
    pub fn open(
        problem: &CompiledProblem,
        measures: SelectedObservationMeasures,
        bindings: Vec<ObservationSourceBinding>,
    ) -> Result<Self, BoundSelectedObservationError> {
        measures.validate_problem(problem)?;
        let residency = SelectedObservationResidencyCertificate::mint(problem, &bindings)?;
        let expected = problem.inputs().observation_snapshot().sources();
        if bindings.len() != expected.len() {
            return Err(BoundSelectedObservationError::BindingSetMismatch);
        }
        let mut sources = Vec::with_capacity(expected.len());
        let first_source_shared_bytes = Self::shared_bytes(
            &measures,
            &bindings,
            bindings.capacity(),
            sources.capacity(),
        )?;
        for (source_index, source) in expected.iter().enumerate() {
            let measurement_set = source.input_ordinal();
            let binding = source_binding(&bindings, measurement_set)?;
            let shared_bytes = if source_index == 0 {
                first_source_shared_bytes
            } else {
                SelectedObservationSharedBytes::NONE
            };
            sources.push(
                BoundObservationSource::open_with_measures(
                    problem,
                    source,
                    &measures,
                    shared_bytes,
                    binding.content_budget,
                    BoundObservationReferenceData::new(
                        binding.ephemeris.as_ref(),
                        binding.pointing_query_domain(),
                    ),
                )
                .map_err(|error| BoundSelectedObservationError::Source {
                    measurement_set,
                    error: Box::new(error),
                })?,
            );
        }
        measures.verify_state()?;
        Ok(Self {
            residency,
            measures,
            sources,
        })
    }

    /// Return the exact aggregate residency certificate retained by this owner.
    #[must_use]
    pub const fn residency_certificate(&self) -> &SelectedObservationResidencyCertificate {
        &self.residency
    }

    #[cfg(test)]
    pub(crate) fn source_content_plan(
        &self,
        source_index: usize,
    ) -> Option<super::SelectedObservationContentPlan> {
        self.sources
            .get(source_index)
            .map(BoundObservationSource::content_plan)
    }

    #[cfg(test)]
    pub(crate) fn source(&self, source_index: usize) -> &BoundObservationSource {
        &self.sources[source_index]
    }

    #[cfg(test)]
    pub(crate) fn source_slot_allocation_bytes(&self) -> usize {
        self.sources.capacity() * BoundObservationSource::retained_source_slot_bytes()
    }

    /// Stream every selected row of every source, in canonical compiler order,
    /// as refillable blocks.
    pub fn into_block_stream(
        self,
        problem: &CompiledProblem,
    ) -> Result<SelectedObservationBlockSource<'_>, BoundSelectedObservationError> {
        self.into_block_stream_with_window(problem, None)
    }

    /// Stream every selected row, reading only the channels whose
    /// output-frame frequencies reach `frequency_bounds_hz`, with the
    /// straddling channel kept at each edge so interpolation keeps both
    /// partners. A row block with no such channel is skipped without reading
    /// its payload.
    pub fn into_windowed_block_stream(
        self,
        problem: &CompiledProblem,
        frequency_bounds_hz: [f64; 2],
    ) -> Result<SelectedObservationBlockSource<'_>, BoundSelectedObservationError> {
        self.into_block_stream_with_window(problem, Some(frequency_bounds_hz))
    }

    fn into_block_stream_with_window(
        self,
        problem: &CompiledProblem,
        window: Option<[f64; 2]>,
    ) -> Result<SelectedObservationBlockSource<'_>, BoundSelectedObservationError> {
        self.measures.verify_state()?;
        let maximum_rows = self
            .sources
            .iter()
            .map(BoundObservationSource::rows_per_block)
            .max()
            .unwrap_or(0);
        Ok(SelectedObservationBlockSource {
            problem,
            observation: self,
            source_index: 0,
            row_replay: None,
            exhausted: false,
            maximum_rows,
            window,
        })
    }
}

/// Ordered refillable source for one retained selected-observation pass.
pub struct SelectedObservationBlockSource<'a> {
    problem: &'a CompiledProblem,
    observation: BoundSelectedObservation,
    source_index: usize,
    row_replay: Option<SelectedRowReplay>,
    exhausted: bool,
    maximum_rows: usize,
    window: Option<[f64; 2]>,
}

impl SelectedObservationBlockSource<'_> {
    /// Exact row ceiling from the freshly opened source's physical content plan.
    /// Consumers may reduce an already admitted scratch bound to this size.
    pub const fn maximum_rows_per_block(&self) -> usize {
        self.maximum_rows
    }

    /// Create one empty block sized for this stream.
    #[must_use]
    pub fn create_storage(&self) -> SelectedObservationBlock {
        SelectedObservationBlock::new(self.maximum_rows)
    }

    /// Fill `block` with the next canonical row block; `false` once every
    /// source is exhausted.
    pub fn fill_next(
        &mut self,
        block: &mut SelectedObservationBlock,
    ) -> Result<bool, BoundObservationSourceError> {
        block.invalidate();
        if self.exhausted {
            return Ok(false);
        }
        loop {
            let Some(source) = self.observation.sources.get(self.source_index) else {
                self.exhausted = true;
                return Ok(false);
            };
            let logical_source = self
                .problem
                .selected_observation()
                .read_set()
                .sources()
                .get(self.source_index)
                .ok_or(BoundObservationSourceError::ProblemSourceMismatch)?;
            if self.row_replay.is_none() {
                self.row_replay = Some(source.selected_row_replay()?);
            }
            if source.fill_next_selected_block(
                self.problem,
                logical_source,
                self.row_replay
                    .as_mut()
                    .expect("selected-row replay initialized for current source"),
                block,
                self.window,
            )? {
                return Ok(true);
            }
            self.source_index += 1;
            self.row_replay = None;
        }
    }

    /// Return the retained access once every block has been read.
    pub fn complete(self) -> Result<BoundSelectedObservation, BoundObservationSourceError> {
        if !self.exhausted {
            return Err(BoundObservationSourceError::IncompleteBlockTraversal);
        }
        Ok(self.observation)
    }
}

/// Failure to bind or replay a complete compiled selected observation.
#[derive(Debug, Error)]
pub enum BoundSelectedObservationError {
    /// The injected Measures provider is missing, stale, or unaccounted.
    #[error(transparent)]
    Measures(#[from] SelectedObservationMeasuresError),
    /// The supplied binding count or membership differs from the compiled source set.
    #[error("source binding set does not match the compiled selected observation")]
    BindingSetMismatch,
    /// One compiled source has no current state and budget binding.
    #[error("compiled source {measurement_set} has no retained-access binding")]
    MissingSourceBinding {
        /// Source missing a binding.
        measurement_set: usize,
    },
    /// One compiled source was assigned more than one binding.
    #[error("compiled source {measurement_set} has duplicate retained-access bindings")]
    DuplicateSourceBinding {
        /// Source with duplicate bindings.
        measurement_set: usize,
    },
    /// A source binding omitted, substituted, or unexpectedly supplied ephemeris data.
    #[error(
        "compiled source {measurement_set} ephemeris reference mismatch: expected {expected:?}, actual {actual:?}"
    )]
    EphemerisReferenceMismatch {
        /// Source whose reference-data binding differs from compiled geometry.
        measurement_set: usize,
        /// Compiler-owned ephemeris identity, or absence for fixed geometry.
        expected: Option<LogicalIdentity>,
        /// Supplied source-binding identity, or absence when none was supplied.
        actual: Option<LogicalIdentity>,
    },
    /// A source's retained reference data exceeds its selected-content ceiling.
    #[error(
        "compiled source {measurement_set} retains {required_bytes} reference-data bytes but its content budget has {available_bytes} bytes"
    )]
    ReferenceDataBudgetExceeded {
        /// Source whose exact reference-data charge exceeds its budget.
        measurement_set: usize,
        /// Exact retained reference-data allocation.
        required_bytes: usize,
        /// Bytes authorized by the source content budget.
        available_bytes: usize,
    },
    /// One retained source could not be bound under its state and budget.
    #[error("bind compiled source {measurement_set}: {error}")]
    Source {
        /// Source whose binding failed.
        measurement_set: usize,
        /// Exact source-level failure.
        #[source]
        error: Box<BoundObservationSourceError>,
    },
    /// The retained source-slot allocation exceeded the host byte domain.
    #[error("selected-observation source-slot byte projection overflowed")]
    SourceSlotByteOverflow,
    /// The consumed source-binding graph exceeded the host byte domain.
    #[error("selected-observation binding-graph byte projection overflowed")]
    BindingGraphByteOverflow,
    /// Aggregate selected-source residency exceeded the host byte domain.
    #[error("selected-observation aggregate residency projection overflowed")]
    ResidencyByteOverflow,
    /// Aggregate retained reference data exceeded the host byte domain.
    #[error("selected-observation reference-data residency projection overflowed")]
    ReferenceDataByteOverflow,
}
