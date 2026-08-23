// SPDX-License-Identifier: LGPL-3.0-or-later

use std::alloc::Layout;
use std::sync::{Arc, atomic::AtomicUsize};

use casa_imaging_model::{CompiledProblem, LogicalIdentity, ReferenceDataKind};
use casa_types::measures::{MeasuresProvider, MeasuresProviderState};
use thiserror::Error;

/// One explicitly acquired Measures provider bound to its immutable reference snapshot.
///
/// Construction rejects providers that cannot eagerly stabilize and project all
/// retained cache state. The bound observation owns this capability once and
/// passes shared provider references inward to its geometry engines.
#[derive(Debug)]
pub struct SelectedObservationMeasures {
    provider: Arc<dyn MeasuresProvider>,
    provider_state: MeasuresProviderState,
    retained_bytes: usize,
}

impl SelectedObservationMeasures {
    /// Acquire the authoritative logical snapshot owned by one provider.
    pub fn new(
        provider: Arc<dyn MeasuresProvider>,
    ) -> Result<Self, SelectedObservationMeasuresError> {
        let provider_state = provider
            .prepare_bounded_state()
            .map_err(SelectedObservationMeasuresError::ProviderPreparation)?
            .ok_or(SelectedObservationMeasuresError::UnaccountedProvider)?;
        let retained_bytes = arc_allocation_bytes(provider.as_ref())
            .ok_or(SelectedObservationMeasuresError::ByteOverflow)?
            .checked_add(provider_state.retained_heap_bytes())
            .ok_or(SelectedObservationMeasuresError::ByteOverflow)?;
        Ok(Self {
            provider,
            provider_state,
            retained_bytes,
        })
    }

    /// Return the provider-owned reference-data identity.
    #[must_use]
    pub const fn identity(&self) -> LogicalIdentity {
        LogicalIdentity::from_sha256(self.provider_state.identity_sha256())
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(crate) const fn provider_state(&self) -> MeasuresProviderState {
        self.provider_state
    }

    pub(crate) fn provider(&self) -> Arc<dyn MeasuresProvider> {
        Arc::clone(&self.provider)
    }

    pub(crate) fn validate_problem(
        &self,
        problem: &CompiledProblem,
    ) -> Result<(), SelectedObservationMeasuresError> {
        let expected = problem
            .inputs()
            .reference_data()
            .iter()
            .find_map(|(kind, identity)| {
                (*kind == ReferenceDataKind::Measures).then_some(*identity)
            })
            .ok_or(SelectedObservationMeasuresError::MissingMeasuresReference)?;
        let actual = self.identity();
        if actual != expected {
            return Err(
                SelectedObservationMeasuresError::ReferenceIdentityMismatch { expected, actual },
            );
        }
        self.verify_state()
    }

    pub(crate) fn verify_state(&self) -> Result<(), SelectedObservationMeasuresError> {
        let actual = self
            .provider
            .prepare_bounded_state()
            .map_err(SelectedObservationMeasuresError::ProviderPreparation)?;
        if actual != Some(self.provider_state) {
            return Err(SelectedObservationMeasuresError::ProviderStateChanged {
                expected: self.provider_state,
                actual,
            });
        }
        Ok(())
    }
}

fn arc_allocation_bytes(provider: &dyn MeasuresProvider) -> Option<usize> {
    let header = Layout::array::<AtomicUsize>(2).ok()?;
    let (allocation, _) = header.extend(Layout::for_value(provider)).ok()?;
    Some(allocation.pad_to_align().size())
}

/// Failure to bind a Measures provider into bounded Selected Observation access.
#[derive(Debug, Error)]
pub enum SelectedObservationMeasuresError {
    /// The compiled input snapshot omitted the Measures reference used by geometry evaluation.
    #[error("compiled selected observation has no Measures reference-data identity")]
    MissingMeasuresReference,
    /// The acquired provider belongs to a different immutable Measures snapshot.
    #[error("Measures provider identity {actual} does not match compiled identity {expected}")]
    ReferenceIdentityMismatch {
        /// Identity required by the compiled problem.
        expected: LogicalIdentity,
        /// Identity supplied by the provider acquisition owner.
        actual: LogicalIdentity,
    },
    /// The provider could not stabilize its bounded cache state.
    #[error("Measures provider bounded preparation failed: {0}")]
    ProviderPreparation(String),
    /// The provider exposes opaque retained state and cannot enter a bounded operation.
    #[error("Measures provider does not expose bounded retained residency")]
    UnaccountedProvider,
    /// Provider scientific identity or cache residency changed after acquisition.
    #[error("Measures provider state changed from {expected:?} to {actual:?}")]
    ProviderStateChanged {
        /// Provider-owned state admitted during acquisition.
        expected: MeasuresProviderState,
        /// Provider-owned state observed on the later stability check.
        actual: Option<MeasuresProviderState>,
    },
    /// The provider allocation and cache projection overflowed the host byte domain.
    #[error("Measures provider retained-residency projection overflowed")]
    ByteOverflow,
}

#[cfg(test)]
pub(crate) fn test_selected_observation_measures(
    problem: &CompiledProblem,
) -> Result<SelectedObservationMeasures, SelectedObservationMeasuresError> {
    let identity = problem
        .inputs()
        .reference_data()
        .iter()
        .find_map(|(kind, identity)| (*kind == ReferenceDataKind::Measures).then_some(*identity))
        .ok_or(SelectedObservationMeasuresError::MissingMeasuresReference)?;
    SelectedObservationMeasures::new(
        casa_test_support::deterministic_measures_provider_for_identity(identity.as_bytes()),
    )
}
