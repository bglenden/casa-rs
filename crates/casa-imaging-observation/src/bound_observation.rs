// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CompiledProblem, CompiledProblemId, MeasurementSetIdentity, SelectedObservationSample,
};
use casa_ms::MsReadPlan;
use thiserror::Error;

use crate::{
    BoundObservationSamples, BoundObservationSource, BoundObservationSourceError,
    SelectedObservationContentBudget,
};

/// One typed physical row plan for a compiled MeasurementSet source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationSourceReadPlan {
    measurement_set: MeasurementSetIdentity,
    row_plan: MsReadPlan,
    content_budget: SelectedObservationContentBudget,
}

impl ObservationSourceReadPlan {
    /// Bind a physical row plan to one canonical logical MeasurementSet identity.
    #[must_use]
    pub const fn new(
        measurement_set: MeasurementSetIdentity,
        row_plan: MsReadPlan,
        content_budget: SelectedObservationContentBudget,
    ) -> Self {
        Self {
            measurement_set,
            row_plan,
            content_budget,
        }
    }

    /// Return the canonical logical MeasurementSet identity.
    #[must_use]
    pub const fn measurement_set(self) -> MeasurementSetIdentity {
        self.measurement_set
    }

    /// Return the exact physical row plan.
    #[must_use]
    pub const fn row_plan(self) -> MsReadPlan {
        self.row_plan
    }

    /// Return the explicit selected-content memory budget.
    #[must_use]
    pub const fn content_budget(self) -> SelectedObservationContentBudget {
        self.content_budget
    }
}

/// Retained read-locked access to every source in one compiled selected observation.
pub struct BoundSelectedObservation {
    problem_id: CompiledProblemId,
    sources: Vec<BoundObservationSource>,
}

impl BoundSelectedObservation {
    /// Open every compiled source under its typed physical plan.
    ///
    /// Caller plan order is irrelevant. Sources are retained and replayed only in the compiler's
    /// canonical read-set order.
    #[cfg(unix)]
    pub fn open(
        problem: &CompiledProblem,
        mut plans: Vec<ObservationSourceReadPlan>,
    ) -> Result<Self, BoundSelectedObservationError> {
        let expected = problem.inputs().observation_snapshot().sources();
        if plans.len() != expected.len() {
            return Err(BoundSelectedObservationError::PlanSetMismatch);
        }
        let mut sources = Vec::with_capacity(expected.len());
        for source in expected {
            let identity = source.identity();
            let Some(position) = plans
                .iter()
                .position(|candidate| candidate.measurement_set == identity)
            else {
                return Err(BoundSelectedObservationError::MissingSourcePlan {
                    measurement_set: identity,
                });
            };
            if plans[position + 1..]
                .iter()
                .any(|candidate| candidate.measurement_set == identity)
            {
                return Err(BoundSelectedObservationError::DuplicateSourcePlan {
                    measurement_set: identity,
                });
            }
            let plan = plans.remove(position);
            sources.push(
                BoundObservationSource::open(source, plan.row_plan, plan.content_budget).map_err(
                    |error| BoundSelectedObservationError::Source {
                        measurement_set: identity,
                        error: Box::new(error),
                    },
                )?,
            );
        }
        if !plans.is_empty() {
            return Err(BoundSelectedObservationError::PlanSetMismatch);
        }
        Ok(Self {
            problem_id: problem.problem_id(),
            sources,
        })
    }

    /// Return the compiled problem identity bound by this retained source set.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return retained sources in canonical compiler order.
    #[must_use]
    pub fn sources(&self) -> &[BoundObservationSource] {
        &self.sources
    }

    /// Stream every source in canonical compiler order.
    pub fn selected_samples<'a>(
        &'a self,
        problem: &'a CompiledProblem,
    ) -> Result<BoundSelectedObservationSamples<'a>, BoundSelectedObservationError> {
        if problem.problem_id() != self.problem_id {
            return Err(BoundSelectedObservationError::ProblemMismatch);
        }
        Ok(BoundSelectedObservationSamples {
            observation: self,
            problem,
            source_index: 0,
            current: None,
            finished: false,
        })
    }
}

/// One fallible canonical sample stream over every retained source.
pub struct BoundSelectedObservationSamples<'a> {
    observation: &'a BoundSelectedObservation,
    problem: &'a CompiledProblem,
    source_index: usize,
    current: Option<BoundObservationSamples<'a>>,
    finished: bool,
}

impl Iterator for BoundSelectedObservationSamples<'_> {
    type Item = Result<SelectedObservationSample, BoundObservationSourceError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            if let Some(current) = &mut self.current {
                if let Some(sample) = current.next() {
                    if sample.is_err() {
                        self.finished = true;
                    }
                    return Some(sample);
                }
                self.current = None;
                self.source_index += 1;
            }
            let Some(source) = self.observation.sources.get(self.source_index) else {
                self.finished = true;
                return None;
            };
            match source.selected_samples(self.problem) {
                Ok(samples) => self.current = Some(samples),
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

/// Failure to bind or replay a complete compiled selected observation.
#[derive(Debug, Error)]
pub enum BoundSelectedObservationError {
    /// The supplied plan count or membership differs from the compiled source set.
    #[error("physical source-plan set does not match the compiled selected observation")]
    PlanSetMismatch,
    /// One compiled source has no physical plan.
    #[error("compiled source {measurement_set} has no physical row plan")]
    MissingSourcePlan {
        /// Source missing a plan.
        measurement_set: MeasurementSetIdentity,
    },
    /// One compiled source was assigned more than one physical plan.
    #[error("compiled source {measurement_set} has duplicate physical row plans")]
    DuplicateSourcePlan {
        /// Source with duplicate plans.
        measurement_set: MeasurementSetIdentity,
    },
    /// One retained source could not be bound under its plan.
    #[error("bind compiled source {measurement_set}: {error}")]
    Source {
        /// Source whose binding failed.
        measurement_set: MeasurementSetIdentity,
        /// Exact source-level failure.
        #[source]
        error: Box<BoundObservationSourceError>,
    },
    /// A different compiled problem was supplied for replay.
    #[error("retained selected observation belongs to a different compiled problem")]
    ProblemMismatch,
}
