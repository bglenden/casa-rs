// SPDX-License-Identifier: LGPL-3.0-or-later
//! The memory a plan holds, admitted against
//! [`CreationOptions::memory_bytes`](super::CreationOptions::memory_bytes)
//! before it is allocated.
//!
//! Making a plan holds each input's opening (its row index, asked for as
//! the index is built and then trimmed to what it keeps, and its catalogue),
//! the merged and written catalogues, the scans that find losses and
//! pointing, each output partition's grids while they are combined, their
//! combined times until the rows are counted and, with padding omitted, a
//! weights batch and the cells acquired. Writing then holds, beside what
//! the plan keeps, the subtables and the `POINTING` table until the save
//! (#950), the pointing reads, the MAIN writers and one batch of every
//! selected group. Each is admitted before it is allocated, so a plan that
//! would exceed the budget is refused
//! ([`ObservationError::OverBudget`]) with nothing more held, and the plan
//! states what it admitted ([`MemoryCharges`]).
//!
//! The catalogues' charges are estimates from their entities and arrays
//! ([`catalogue_bytes`]); the counting laws (`creation_memory.rs`) hold
//! the plan to its charges.

use casa_observation::{Catalogue, FieldId, ObservationError, OrderedBaseline, ScanId};

use crate::MsSelectionIoBudget;
use crate::observation_io::msv2::GRID_READ_ROWS;
use crate::observation_io::update::request_error;

/// Bytes allowed each name, intent and other string a catalogue holds.
const STRING_BYTES: usize = 64;

/// Bytes allowed each antenna's receptors in a partition.
const RECEPTOR_BYTES: usize = 256;

/// The memory a plan admitted ([`super::CreationPlan::charges`]), in bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemoryCharges {
    /// What the plan keeps while it lives: each input's opening (its
    /// index and catalogue) and the merged and written catalogues.
    pub kept: usize,
    /// The most making the plan held, `kept` included.
    pub planning: usize,
    /// What writing holds beyond `kept`: the subtables and the `POINTING`
    /// table until the save, then the larger of the pointing reads and the
    /// MAIN writers with one batch.
    pub writing: usize,
    /// Of `writing`, the MAIN writers': their tile bands and I/O buffers,
    /// and the keys of one batch's rows.
    pub writer: usize,
    /// Of `writing`, one time range's batches of every selected group.
    pub batch: usize,
    /// The least [`CreationOptions::memory_bytes`](super::CreationOptions)
    /// the plan is made with: its planning, and its writing with the MAIN
    /// writers at their least.
    pub least: usize,
}

/// The bytes admitted so far against a budget, and the most at once.
pub(super) struct Ledger {
    budget: usize,
    held: usize,
    peak: usize,
}

impl Ledger {
    pub(super) fn new(budget: usize) -> Self {
        Self {
            budget,
            held: 0,
            peak: 0,
        }
    }

    /// Admit `bytes` more for `what`, refused beyond the budget.
    pub(super) fn admit(
        &mut self,
        what: &'static str,
        bytes: usize,
    ) -> Result<(), ObservationError> {
        let required = self.held.saturating_add(bytes);
        if required > self.budget {
            return Err(ObservationError::OverBudget {
                what,
                required,
                budget: self.budget,
            });
        }
        self.held = required;
        self.peak = self.peak.max(required);
        Ok(())
    }

    /// Release `bytes` admitted earlier.
    pub(super) fn release(&mut self, bytes: usize) {
        self.held = self.held.saturating_sub(bytes);
    }

    /// Admit `bytes` for `what` while `run` runs.
    pub(super) fn during<T>(
        &mut self,
        what: &'static str,
        bytes: usize,
        run: impl FnOnce(&mut Self) -> Result<T, ObservationError>,
    ) -> Result<T, ObservationError> {
        self.admit(what, bytes)?;
        let value = run(self);
        self.release(bytes);
        value
    }

    pub(super) fn held(&self) -> usize {
        self.held
    }

    pub(super) fn peak(&self) -> usize {
        self.peak
    }

    pub(super) fn budget(&self) -> usize {
        self.budget
    }
}

/// The memory a creation may hold by default: an eighth of the host's
/// physical memory, the share casa-ms's readers and writers plan with
/// ([`MsSelectionIoBudget::from_system_memory`]).
///
/// # Errors
///
/// When the host's physical memory is unknown.
pub fn default_memory_bytes() -> Result<usize, ObservationError> {
    MsSelectionIoBudget::from_system_memory(1, 1, None)
        .map(|budget| budget.available_bytes)
        .map_err(|error| request_error(error.to_string()))
}

/// The bytes an owned copy of a partition's grid of `times` times and
/// `baselines` baselines holds while it is made: its times, fields and
/// scans and its baselines, and the MSv2 adapter's chunk of row numbers
/// and intervals ([`GRID_READ_ROWS`] at most).
pub(super) fn grid_bytes(times: usize, baselines: usize) -> usize {
    let per_time = size_of::<f64>() + size_of::<FieldId>() + size_of::<ScanId>();
    let read = times.saturating_mul(baselines).min(GRID_READ_ROWS)
        * (size_of::<usize>() + size_of::<f64>());
    times * per_time + baselines * size_of::<OrderedBaseline>() + read
}

/// About the heap `catalogue` holds, twice what its entities and their
/// arrays hold at their lengths (for spare capacity and map nodes), each
/// string allowed [`STRING_BYTES`].
pub(super) fn catalogue_bytes(catalogue: &Catalogue) -> usize {
    let strings = |count: usize| count * (size_of::<String>() + STRING_BYTES);
    let optional = |values: Option<usize>| values.unwrap_or(0);
    let mut bytes = size_of_val(catalogue.antennas.as_slice())
        + strings(3 * catalogue.antennas.len())
        + size_of_val(catalogue.fields.as_slice())
        + strings(3 * catalogue.fields.len())
        + size_of_val(catalogue.states.as_slice())
        + strings(catalogue.states.len())
        + size_of_val(catalogue.processors.as_slice())
        + strings(2 * catalogue.processors.len());
    for scan in &catalogue.scans {
        bytes += size_of_val(scan) + strings(1 + scan.intents.len());
    }
    for observation in &catalogue.observations {
        bytes += size_of_val(observation) + strings(5 + observation.observers.len());
    }
    for window in &catalogue.windows {
        bytes += size_of_val(window)
            + strings(2)
            + optional(window.effective_bandwidths_hz.as_deref().map(size_of_val))
            + optional(window.resolutions_hz.as_deref().map(size_of_val));
    }
    for source in &catalogue.sources {
        bytes += size_of_val(source) + strings(1);
        for record in &source.records {
            bytes += size_of_val(record)
                + strings(1 + record.transitions.as_ref().map_or(0, Vec::len))
                + optional(record.rest_frequencies.as_deref().map(size_of_val))
                + optional(record.systemic_velocities.as_deref().map(size_of_val));
        }
    }
    for partition in &catalogue.partitions {
        bytes += size_of_val(partition)
            + strings(1 + partition.intents.len() + 4 * partition.data_groups.len())
            + size_of_val(partition.frequencies_hz.as_slice())
            + size_of_val(partition.channel_widths_hz.as_slice())
            + size_of_val(partition.correlations.as_slice())
            + size_of_val(partition.fields.as_slice())
            + size_of_val(partition.scans.as_slice())
            + size_of_val(partition.baselines.as_slice())
            + size_of_val(partition.data_groups.as_slice())
            + size_of_val(partition.states.as_slice())
            + partition.receptors.len() * RECEPTOR_BYTES;
    }
    2 * bytes
}

#[cfg(test)]
mod tests {
    use casa_observation::ObservationError;

    use super::Ledger;

    #[test]
    fn a_ledger_refuses_beyond_its_budget_with_nothing_more_held() {
        let mut ledger = Ledger::new(100);
        ledger.admit("first", 60).expect("within");
        let refused = ledger.admit("second", 41);
        assert!(matches!(
            refused,
            Err(ObservationError::OverBudget {
                what: "second",
                required: 101,
                budget: 100
            })
        ));
        assert_eq!(ledger.held(), 60);
        ledger
            .during("transient", 40, |ledger| {
                assert_eq!(ledger.held(), 100);
                Ok(())
            })
            .expect("within");
        assert_eq!((ledger.held(), ledger.peak()), (60, 100));
        ledger.release(60);
        assert_eq!(ledger.held(), 0);
    }
}
