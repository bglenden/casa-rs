// SPDX-License-Identifier: LGPL-3.0-or-later
//! The "simulation pass" summary `close` emits, as a test reads it: a
//! subscriber that sees only that event, on the thread that closes.

#![allow(dead_code, reason = "each test file reads part of the summary")]

use std::fmt::Debug;
use std::sync::{Arc, Mutex, PoisonError};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

/// The fields of one "simulation pass" event.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PassSummary {
    /// The backend predicted on, as `Debug` prints it (`Cpu`, `Metal`).
    pub backend: String,
    pub rows: u64,
    pub batch_rows: u64,
    pub batches: u64,
    pub workers: u64,
    /// Rows of a prediction block.
    pub block_rows: u64,
    /// What `close` was admitted, and the most the process's reservations
    /// held at once while it ran.
    pub reserved_bytes: u64,
    pub peak_reserved_bytes: u64,
    /// Bytes the per-field model grids could hold, and the sets prepared.
    pub grid_bytes: u64,
    pub grid_preparations: u64,
}

impl Visit for PassSummary {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "rows" => self.rows = value,
            "batch_rows" => self.batch_rows = value,
            "batches" => self.batches = value,
            "workers" => self.workers = value,
            "block_rows" => self.block_rows = value,
            "reserved_bytes" => self.reserved_bytes = value,
            "peak_reserved_bytes" => self.peak_reserved_bytes = value,
            "grid_bytes" => self.grid_bytes = value,
            "grid_preparations" => self.grid_preparations = value,
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "backend" {
            self.backend = format!("{value:?}");
        }
    }
}

/// Keeps the last pass summary; every other event and span is disabled.
struct Capture(Arc<Mutex<Option<PassSummary>>>);

impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event() && metadata.fields().field("peak_reserved_bytes").is_some()
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut summary = PassSummary::default();
        event.record(&mut summary);
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(summary);
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

/// Run `work` on this thread, returning what it returns and the summary of
/// the last pass it closed, if any.
pub fn capture<T>(work: impl FnOnce() -> T) -> (T, Option<PassSummary>) {
    let slot = Arc::new(Mutex::new(None));
    let result = tracing::subscriber::with_default(Capture(Arc::clone(&slot)), work);
    let summary = slot.lock().unwrap_or_else(PoisonError::into_inner).take();
    (result, summary)
}
