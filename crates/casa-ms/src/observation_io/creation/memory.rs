// SPDX-License-Identifier: LGPL-3.0-or-later
//! The memory a plan holds, admitted against
//! [`CreationOptions::memory_bytes`](super::CreationOptions::memory_bytes)
//! before it is allocated.
//!
//! Making a plan holds the `HISTORY` rows it copies, each input's opening
//! (its tables, the rows its readers load, its row index and catalogue),
//! the merged and written catalogues, the scans that find losses and
//! pointing, each output partition's grids while they are combined, their
//! combined times until the rows are counted and, with padding omitted, a
//! weights batch and the cells acquired. Writing then holds, beside what
//! the plan keeps, the subtables, `HISTORY` and `POINTING` until the save
//! (#950), the pointing reads, the MAIN writers and one batch of every
//! selected group. Each is admitted before it is allocated, so a plan that
//! would exceed the budget is refused
//! ([`ObservationError::OverBudget`]) with nothing more held, and the plan
//! states what it admitted ([`MemoryCharges`]).
//!
//! What only building something gives its size is admitted as a bound
//! first and trimmed to what was built: an opening's tables and catalogue
//! (bounded by their files and by the rows the readers load), the merge
//! (by the inputs' catalogues together) and the written catalogue (by the
//! merged one). Catalogues are measured at their strings' and arrays'
//! capacities (`heap::catalogue_bytes`); the counting law
//! (`creation_plan_memory.rs`) holds the plan to its charges.

use casa_observation::{Catalogue, FieldId, ObservationError, OrderedBaseline, ScanId, StateEntry};

use super::combine::Combined;
use super::msv2_rows::{Placement, RowMap, StateIds};
use super::{HistoryRow, Loss};
use crate::MsSelectionIoBudget;
use crate::observation_io::heap::{entries, plain, strings, units_bytes};
use crate::observation_io::msv2::GRID_READ_ROWS;
use crate::observation_io::update::request_error;

pub(super) use crate::observation_io::heap::catalogue_bytes;

/// Bytes a `HISTORY` row's record holds in its table beside its strings:
/// its nine fields' names and values, and the arrays' headers.
const HISTORY_ROW_BYTES: usize = 1 << 10;

/// Bytes a subtable row's record holds in memory beside the strings and
/// arrays of its entity: its fields' names, values and array headers.
const SUBTABLE_ROW_BYTES: usize = 2 << 10;

/// The memory a plan admitted ([`super::CreationPlan::charges`]), in bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemoryCharges {
    /// What the plan keeps while it lives: the `HISTORY` rows to write,
    /// each input's opening (its tables, index and catalogue) and the
    /// merged and written catalogues.
    pub kept: usize,
    /// The most making the plan held, `kept` included.
    pub planning: usize,
    /// What writing holds beyond `kept`: the subtables, `HISTORY` and
    /// `POINTING` until the save, then the largest of the pointing reads,
    /// the MAIN writers with one batch, and an ephemeris table.
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
    /// The least [`CreationOptions::batch_bytes`](super::CreationOptions):
    /// one integration (one time of every group read) of the widest input
    /// partition.
    pub integration: usize,
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

    /// Trim `bound` bytes, admitted for `what` before what they bound was
    /// allocated, to the `held` bytes it holds.
    pub(super) fn trim(
        &mut self,
        what: &'static str,
        bound: usize,
        held: usize,
    ) -> Result<(), ObservationError> {
        if held <= bound {
            self.release(bound - held);
            Ok(())
        } else {
            self.admit(what, held - bound)
        }
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

/// What the merge and the combination of catalogues holding
/// `catalogues` bytes together may hold: the merged catalogue, the
/// combination's partitions beside it until it finishes, and the maps and
/// notices. Each holds no more than the inputs' catalogues, whose entities
/// it matches, appends or combines as clones.
pub(super) fn merge_bound(catalogues: usize) -> usize {
    3 * catalogues
}

/// What the subtables written from `catalogue` hold in memory, until
/// they are saved: a record per row ([`SUBTABLE_ROW_BYTES`]; `FEED`'s an
/// antenna's in each window, and a data description, a polarization and a
/// state of intents for each partition) and the entities' strings and
/// arrays again.
pub(super) fn subtables_bytes(catalogue: &Catalogue) -> usize {
    let windows = catalogue.windows.len();
    let records: usize = catalogue
        .sources
        .iter()
        .map(|source| source.records.len())
        .sum();
    let rows = catalogue.antennas.len() * (2 + windows)
        + catalogue.fields.len()
        + windows
        + 3 * catalogue.partitions.len()
        + records
        + catalogue.states.len()
        + catalogue.processors.len()
        + catalogue.observations.len();
    rows * SUBTABLE_ROW_BYTES + catalogue_bytes(catalogue)
}

/// What the catalogue a plan states it writes may hold
/// ([`super::CreationPlan::output`]): the merged catalogue's entities with
/// a state for each partition's intents, twice for the capacities reading
/// them back leaves.
pub(super) fn written_bound(combined: &Combined) -> usize {
    let states: usize = combined
        .intents
        .iter()
        .zip(&combined.modes)
        .filter(|(intents, _)| **intents)
        .map(|(_, mode)| size_of::<StateEntry>() + mode.len())
        .sum();
    2 * (catalogue_bytes(&combined.catalogue) + states)
}

/// The heap `combined` holds: its catalogue ([`catalogue_bytes`]), each
/// input's row map, and the output partitions' keys, contributors, units,
/// losses and the merge's notices.
pub(super) fn combined_bytes(combined: &Combined) -> usize {
    let Combined {
        catalogue,
        rows,
        intents,
        modes,
        contributors,
        units,
        losses,
        notices,
    } = combined;
    catalogue_bytes(catalogue)
        + entries(rows, |map| {
            let RowMap {
                antennas,
                fields,
                scans,
                states,
                partitions,
                weight_scale: _,
            } = map;
            plain(antennas)
                + plain(fields)
                + plain(scans)
                + plain(states)
                + entries(partitions, placement_bytes)
        })
        + plain(intents)
        + strings(modes)
        + entries(contributors, plain)
        + entries(units, units_bytes)
        + entries(losses, |loss| match loss {
            Loss::DataGroup(what)
            | Loss::Table(what)
            | Loss::Column(what)
            | Loss::Defaulted(what) => what.capacity(),
            Loss::Sigma
            | Loss::SigmaDerived
            | Loss::RowFlags
            | Loss::FlagCategories
            | Loss::UnusedAntennas(_) => 0,
        })
        + strings(notices)
}

fn placement_bytes(placement: &Placement) -> usize {
    let Placement {
        output: _,
        state,
        processor: _,
        scan_offset: _,
        groups,
        columns,
    } = placement;
    let states = match state {
        StateIds::PerRow(states) => plain(states),
        StateIds::One(_) | StateIds::Intents => 0,
    };
    states + strings(groups) + plain(columns)
}

/// The heap a copy of `history` holds: its rows and their strings.
pub(super) fn history_bytes(history: &[HistoryRow]) -> usize {
    history
        .iter()
        .map(|row| {
            let HistoryRow {
                time_s: _,
                observation: _,
                message,
                priority,
                origin,
                application,
                parameters,
                command,
            } = row;
            size_of::<HistoryRow>()
                + message.len()
                + priority.len()
                + origin.len()
                + application.len()
                + parameters
                    .iter()
                    .chain(command)
                    .map(|line| size_of::<String>() + line.len())
                    .sum::<usize>()
        })
        .sum()
}

/// What the created `HISTORY` table holds of `history` until it is saved:
/// each row's record ([`HISTORY_ROW_BYTES`]) and its strings, and the
/// strings again as the save encodes them.
pub(super) fn history_table_bytes(history: &[HistoryRow]) -> usize {
    history.len() * HISTORY_ROW_BYTES + 2 * history_bytes(history)
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
        ledger.admit("bounded", 30).expect("within");
        ledger
            .trim("bounded", 30, 10)
            .expect("a bound trimmed down");
        assert_eq!(ledger.held(), 70);
        ledger.release(70);
        assert_eq!(ledger.held(), 0);
    }
}
