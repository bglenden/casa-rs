// SPDX-License-Identifier: LGPL-3.0-or-later
//! Cooperative cancellation for synchronous workers.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Per-run cooperative cancellation shared between a caller and its workers.
/// Clones observe the same request. Workers poll at boundaries where they
/// can stop safely, and a new run starts with a fresh token.
#[derive(Clone, Debug, Default)]
pub struct Cancellation(Arc<AtomicBool>);

impl Cancellation {
    /// A token that is not cancelled.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Request cancellation.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Whether cancellation was requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}
