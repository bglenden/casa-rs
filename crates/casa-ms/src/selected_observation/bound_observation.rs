// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::CompiledProblem;
use std::sync::Arc;
use thiserror::Error;

use crate::selected_pointing::SelectedPointingQueryDomain;

use super::access::{BoundObservationReferenceData, SelectedRowReplay};
use super::{
    BoundObservationSource, BoundObservationSourceError, FilledObservationBlock,
    SelectedObservationBlock, SelectedObservationContentBudget,
    SelectedObservationContentRequirements, SelectedObservationMeasures,
    SelectedObservationMeasuresError, content_plan::SelectedObservationSharedBytes,
};

/// One snapshot source and its bounded-content budget.
///
/// The content budget is the sole physical blocking authority.
#[derive(Debug, Clone)]
pub struct ObservationSourceBinding {
    measurement_set: usize,
    content_budget: SelectedObservationContentBudget,
    ephemeris: Option<Arc<crate::SelectedObservationEphemeris>>,
    pointing_query_domain: Option<SelectedPointingQueryDomain>,
}

/// Check that a binding's reference data fits its content budget.
fn check_reference_data(
    binding: &ObservationSourceBinding,
) -> Result<(), BoundSelectedObservationError> {
    let available_bytes = binding.content_budget.available_bytes();
    let required_bytes = binding.reference_data_bytes();
    if required_bytes > available_bytes {
        return Err(BoundSelectedObservationError::ReferenceDataBudgetExceeded {
            measurement_set: binding.measurement_set,
            required_bytes,
            available_bytes,
        });
    }
    Ok(())
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

    pub(crate) fn pointing_query_domain(&self) -> Option<&SelectedPointingQueryDomain> {
        self.pointing_query_domain.as_ref()
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
        let expected = problem.observation().sources();
        if expected.len() != 1 {
            return Err(BoundSelectedObservationError::BindingSetMismatch);
        }
        let source = &expected[0];
        if source.input_ordinal() != binding.measurement_set() {
            return Err(BoundSelectedObservationError::MissingSourceBinding {
                measurement_set: source.input_ordinal(),
            });
        }
        let shared = Self::shared_bytes(measures, std::slice::from_ref(binding))?;
        BoundObservationSource::content_requirements(problem, source, binding, shared).map_err(
            |error| BoundSelectedObservationError::Source {
                measurement_set: source.input_ordinal(),
                error: Box::new(error),
            },
        )
    }

    /// The Measures provider and every binding's reference data, charged
    /// once, to the first source.
    fn shared_bytes(
        measures: &SelectedObservationMeasures,
        bindings: &[ObservationSourceBinding],
    ) -> Result<SelectedObservationSharedBytes, BoundSelectedObservationError> {
        let reference_data_retained_bytes =
            bindings.iter().try_fold(0_usize, |bytes, binding| {
                bytes
                    .checked_add(binding.reference_data_bytes())
                    .ok_or(BoundSelectedObservationError::ReferenceDataByteOverflow)
            })?;
        Ok(SelectedObservationSharedBytes::new(
            measures.retained_bytes(),
            reference_data_retained_bytes,
        ))
    }

    /// Open every compiled source under retained read locks and its content budget.
    ///
    /// Caller binding order is irrelevant. Sources are retained and streamed only in the
    /// compiler's canonical read-set order.
    #[cfg(unix)]
    pub fn open(
        problem: &CompiledProblem,
        measures: SelectedObservationMeasures,
        bindings: Vec<ObservationSourceBinding>,
    ) -> Result<Self, BoundSelectedObservationError> {
        let expected = problem.observation().sources();
        if bindings.len() != expected.len() {
            return Err(BoundSelectedObservationError::BindingSetMismatch);
        }
        for source in expected {
            check_reference_data(source_binding(&bindings, source.input_ordinal())?)?;
        }
        let mut sources = Vec::with_capacity(expected.len());
        let first_source_shared_bytes = Self::shared_bytes(&measures, &bindings)?;
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
        Ok(Self { sources })
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

    /// Stream every selected row of every source, in canonical compiler order,
    /// as refillable blocks.
    #[must_use]
    pub fn into_block_stream(
        self,
        problem: &CompiledProblem,
    ) -> SelectedObservationBlockSource<'_> {
        self.into_block_stream_with_window(problem, None)
    }

    /// Stream every selected row, reading only the channels whose
    /// output-frame frequencies reach `frequency_bounds_hz`, with the
    /// straddling channel kept at each edge so interpolation keeps both
    /// partners. A row block with no such channel is skipped without reading
    /// its payload.
    #[must_use]
    pub fn into_windowed_block_stream(
        self,
        problem: &CompiledProblem,
        frequency_bounds_hz: [f64; 2],
    ) -> SelectedObservationBlockSource<'_> {
        self.into_block_stream_with_window(problem, Some(frequency_bounds_hz))
    }

    fn into_block_stream_with_window(
        self,
        problem: &CompiledProblem,
        window: Option<[f64; 2]>,
    ) -> SelectedObservationBlockSource<'_> {
        let maximum_rows = self
            .sources
            .iter()
            .map(BoundObservationSource::rows_per_block)
            .max()
            .unwrap_or(0);
        SelectedObservationBlockSource {
            problem,
            observation: self,
            source_index: 0,
            row_replay: None,
            selected_rows: 0,
            exhausted: false,
            maximum_rows,
            window,
        }
    }
}

/// Ordered refillable source for one retained selected-observation pass.
pub struct SelectedObservationBlockSource<'a> {
    problem: &'a CompiledProblem,
    observation: BoundSelectedObservation,
    source_index: usize,
    row_replay: Option<SelectedRowReplay>,
    /// Rows the predicate selected in the sources already walked.
    selected_rows: u64,
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

    /// Fill `block` with the next canonical row block and borrow it as filled;
    /// `None` once every source is exhausted.
    pub fn fill_next<'b>(
        &mut self,
        block: &'b mut SelectedObservationBlock,
    ) -> Result<Option<FilledObservationBlock<'b>>, BoundObservationSourceError> {
        if self.exhausted {
            return Ok(None);
        }
        loop {
            let Some(source) = self.observation.sources.get(self.source_index) else {
                self.exhausted = true;
                return Ok(None);
            };
            let logical_source = self
                .problem
                .observation_transaction()
                .read_set()
                .sources()
                .get(self.source_index)
                .ok_or(BoundObservationSourceError::ProblemSourceMismatch)?;
            if self.row_replay.is_none() {
                self.row_replay = Some(source.selected_row_replay()?);
            }
            let replay = self
                .row_replay
                .as_mut()
                .expect("selected-row replay initialized for current source");
            if let Some(binding) = source.fill_next_selected_block(
                self.problem,
                logical_source,
                replay,
                block,
                self.window,
            )? {
                return Ok(Some(FilledObservationBlock::new(block, binding)));
            }
            self.selected_rows += replay.selected_rows();
            self.source_index += 1;
            self.row_replay = None;
        }
    }

    /// Return the retained access once every block has been read.
    ///
    /// The stream must be exhausted, and its walk of MAIN must have selected
    /// exactly the rows the compiled selection counted: the content plan and
    /// the model were sized from that count, so a MAIN that has since gained
    /// or lost selected rows is refused. A channel window skips payload, not
    /// rows, so a windowed stream is held to the same count.
    pub fn complete(self) -> Result<BoundSelectedObservation, BoundObservationSourceError> {
        if !self.exhausted {
            return Err(BoundObservationSourceError::IncompleteBlockTraversal);
        }
        let expected = self
            .problem
            .observation()
            .sources()
            .iter()
            .map(|source| source.selection().rows().selected_row_count())
            .sum();
        if self.selected_rows != expected {
            return Err(BoundObservationSourceError::SelectedRowCountMismatch {
                expected,
                delivered: self.selected_rows,
            });
        }
        Ok(self.observation)
    }
}

/// Failure to bind a complete compiled selected observation.
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
    /// Aggregate retained reference data exceeded the host byte domain.
    #[error("selected-observation reference-data residency projection overflowed")]
    ReferenceDataByteOverflow,
}
