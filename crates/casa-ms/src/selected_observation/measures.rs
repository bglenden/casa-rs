// SPDX-License-Identifier: LGPL-3.0-or-later

use std::alloc::Layout;
use std::sync::{Arc, atomic::AtomicUsize};

use casa_types::measures::MeasuresProvider;
use thiserror::Error;

/// One explicitly acquired Measures provider.
///
/// Construction rejects providers that cannot eagerly stabilize and project all
/// retained cache state. Resolution acquires it once; the bound observation
/// owns it from then on and passes shared provider references inward to its
/// geometry engines, so every pass uses the provider the problem was resolved
/// with.
#[derive(Debug)]
pub struct SelectedObservationMeasures {
    provider: Arc<dyn MeasuresProvider>,
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
            retained_bytes,
        })
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(crate) fn provider(&self) -> Arc<dyn MeasuresProvider> {
        Arc::clone(&self.provider)
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
    /// The provider could not stabilize its bounded cache state.
    #[error("Measures provider bounded preparation failed: {0}")]
    ProviderPreparation(String),
    /// The provider exposes opaque retained state and cannot enter a bounded operation.
    #[error("Measures provider does not expose bounded retained residency")]
    UnaccountedProvider,
    /// The provider allocation and cache projection overflowed the host byte domain.
    #[error("Measures provider retained-residency projection overflowed")]
    ByteOverflow,
}

#[cfg(test)]
pub(crate) fn test_selected_observation_measures()
-> Result<SelectedObservationMeasures, SelectedObservationMeasuresError> {
    SelectedObservationMeasures::new(
        casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
    )
}
