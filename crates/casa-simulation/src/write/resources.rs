// SPDX-License-Identifier: LGPL-3.0-or-later
//! What `close` holds while it writes MAIN, admitted against the session's
//! resource policy before anything is written (plan §4: the runtime's
//! budget, not a second one).
//!
//! In order:
//! 1. **The writer.** The MeasurementSet write plan is sized from half of
//!    what the policy leaves free, and its resident bytes are admitted. Its
//!    batches are tile bands of about [`casa_ms::TARGET_BATCH_BYTES`], so
//!    the budget is a ceiling that only a small budget reaches.
//! 2. **The pipeline.** The batch being filled, the geometry of the
//!    integrations it spans and each worker's scratch, for as many of the
//!    policy's streaming workers as fit what is then free (#737's rule:
//!    workers shrink first). With one worker that does not fit, `close` is
//!    refused.
//!
//! A refusal is a typed [`Admission`] carried by the session error.

use casa_imaging_runtime::{Admission, CoreAllocation, Demand, Reservation, admit, free_memory};
use casa_ms::{MeasurementSetWriteError, MeasurementSetWritePlan, MeasurementSetWriteResources};
use casa_tables::STREAMING_TILED_COLUMN_BUFFER_BYTES;
use casa_types::Complex32;

use super::geometry::RowGeometry;
use super::pipeline::Integration;
use crate::session::SessionResources;
use crate::session::error::{SimulatorError, SimulatorResult};

/// The writer's phase, as an admission refusal names it.
pub(super) const WRITER_PHASE: &str = "the simulator's MeasurementSet writer";

/// The pipeline's phase, as an admission refusal names it.
pub(super) const PIPELINE_PHASE: &str = "the simulator's geometry and noise";

/// Batches the writer holds at once: the one it writes and one queued.
const WRITER_LIVE_BATCHES: usize = 2;

/// A writer budget larger than any plan's needs, to tell a budget too small
/// from a plan that cannot be made.
const UNBOUNDED_WRITER_BYTES: usize = 1 << 50;

/// Scratch of one worker's integration, per antenna: the antennas' uvw, and
/// their shadow and elevation flags, with room for the measures frames.
const ANTENNA_SCRATCH_BYTES: u64 = 64;

/// The MAIN table `close` writes.
#[derive(Debug, Clone, Copy)]
pub(super) struct MainShape<'a> {
    pub(super) rows: usize,
    pub(super) correlations: usize,
    pub(super) channels: usize,
    pub(super) telescope: &'a str,
    /// Rows of the session's largest integration.
    pub(super) largest_integration: usize,
    /// Antennas of the session's largest scan.
    pub(super) antennas: usize,
}

/// What `close` was admitted: the write plan, the workers that fit, and the
/// memory, held until the write ends.
#[derive(Debug)]
pub(super) struct MainAdmission {
    pub(super) plan: MeasurementSetWritePlan,
    pub(super) cores: CoreAllocation,
    /// The policy's allocation, before admission shrank it.
    pub(super) policy_cores: CoreAllocation,
    pub(super) reservation: Reservation,
}

/// Admit `close`'s writer and pipeline for `shape` within `resources`.
///
/// # Errors
///
/// [`SimulatorError::admission`] when the writer, or the pipeline with one
/// worker, does not fit what the policy leaves free; a raised error when the
/// MAIN write cannot be planned at all.
pub(super) fn admit_main_write(
    resources: &SessionResources,
    shape: &MainShape<'_>,
) -> SimulatorResult<MainAdmission> {
    let (host, policy) = (resources.host(), resources.policy());
    let free = free_memory(host, policy);
    let plan = writer_plan(shape, to_usize(free / 2)).map_err(|error| {
        let required =
            minimum_writer_bytes(shape).map_or(u64::MAX, |bytes| (bytes as u64).saturating_mul(2));
        refusal_or(error, WRITER_PHASE, required, free)
    })?;
    let mut reservation = admit(
        host,
        policy,
        &Demand {
            phase: WRITER_PHASE,
            memory: plan.maximum_resident_bytes() as u64,
        },
    )
    .map_err(SimulatorError::admission)?;
    let policy_cores = resources.cores();
    let free = free_memory(host, policy);
    let demand = |cores: CoreAllocation| pipeline_bytes(shape, plan.batch_rows(), cores);
    let cores = (1..=policy_cores.streaming_workers)
        .rev()
        .map(|workers| policy_cores.at_most(workers))
        .find(|cores| demand(*cores) <= free)
        .unwrap_or_else(|| policy_cores.at_most(1));
    let pipeline = admit(
        host,
        policy,
        &Demand {
            phase: PIPELINE_PHASE,
            memory: demand(cores),
        },
    )
    .map_err(SimulatorError::admission)?;
    reservation.join(pipeline);
    Ok(MainAdmission {
        plan,
        cores,
        policy_cores,
        reservation,
    })
}

/// The MAIN write plan with `available_bytes` for the writer.
pub(super) fn writer_plan(
    shape: &MainShape<'_>,
    available_bytes: usize,
) -> Result<MeasurementSetWritePlan, MeasurementSetWriteError> {
    MeasurementSetWritePlan::visibility_creation(
        shape.rows,
        shape.correlations,
        shape.channels,
        shape.telescope,
        MeasurementSetWriteResources {
            available_bytes,
            maximum_live_batches: WRITER_LIVE_BATCHES,
            tiled_column_buffer_bytes: STREAMING_TILED_COLUMN_BUFFER_BYTES,
        },
    )
}

/// The fewest writer bytes that plan `shape`'s MAIN write; `None` when no
/// budget does.
fn minimum_writer_bytes(shape: &MainShape<'_>) -> Option<usize> {
    writer_plan(shape, UNBOUNDED_WRITER_BYTES).ok()?;
    // Whether a budget plans is monotone in the budget.
    let (mut refused, mut planned) = (0, UNBOUNDED_WRITER_BYTES);
    while planned - refused > 1 {
        let middle = refused + (planned - refused) / 2;
        if writer_plan(shape, middle).is_ok() {
            planned = middle;
        } else {
            refused = middle;
        }
    }
    Some(planned)
}

/// A plan refused for its budget as an [`Admission`] of `required` bytes
/// against `free`, and any other planning failure as raised.
fn refusal_or(
    error: MeasurementSetWriteError,
    phase: &'static str,
    required: u64,
    free: u64,
) -> SimulatorError {
    if required == u64::MAX {
        return SimulatorError::raised(format!("plan the MAIN write: {error}"));
    }
    SimulatorError::admission(Admission {
        phase,
        required,
        available: free,
    })
}

/// Bytes the pipeline holds beside the writer, with `cores`' streaming
/// workers: the batch being filled, the geometry of the integrations it
/// spans (with the unwritten rows of the one before), the list of those
/// integrations, one integration's scalar runs, and each worker's scratch.
pub(super) fn pipeline_bytes(
    shape: &MainShape<'_>,
    batch_rows: usize,
    cores: CoreAllocation,
) -> u64 {
    let samples = (shape.correlations * shape.channels) as u64;
    let rows = batch_rows as u64;
    let largest = shape.largest_integration as u64;
    let size = |bytes: usize| bytes as u64;
    // DATA, FLAG_ROW, UVW, WEIGHT and SIGMA.
    let row_bytes = samples * size(size_of::<Complex32>()) + 1 + 24 + 4 + 4;
    let batch = rows * row_bytes;
    let geometry = (rows + 2 * largest) * size(size_of::<RowGeometry>());
    let integrations = (rows + 2) * size(size_of::<Integration>());
    let scalars = largest * (4 + 4 + 1);
    let workers = cores.streaming_workers as u64 * shape.antennas as u64 * ANTENNA_SCRATCH_BYTES;
    batch + geometry + integrations + scalars + workers
}

fn to_usize(bytes: u64) -> usize {
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use casa_imaging_runtime::{HostResources, ResourcePolicy};

    use super::*;
    use crate::session::BackendRequest;

    fn shape() -> MainShape<'static> {
        MainShape {
            rows: 100_000,
            correlations: 2,
            channels: 64,
            telescope: "ALMA",
            largest_integration: 1_000,
            antennas: 44,
        }
    }

    fn resources(memory: u64, workers: usize) -> SessionResources {
        SessionResources::pinned(
            HostResources {
                threads: 8,
                performance_cores: 8,
                available_memory: 8 << 30,
                metal: false,
            },
            ResourcePolicy::Explicit { workers, memory },
            BackendRequest::Cpu,
        )
        .expect("resources")
    }

    #[test]
    fn the_writer_takes_half_the_budget_and_the_pipeline_fits_the_rest() {
        let admitted = admit_main_write(&resources(1 << 30, 5), &shape()).expect("fits");
        assert_eq!(admitted.cores.streaming_workers, 4);
        let writer = admitted.plan.maximum_resident_bytes() as u64;
        assert!(writer <= 1 << 29);
        assert_eq!(
            admitted.reservation.memory(),
            writer + pipeline_bytes(&shape(), admitted.plan.batch_rows(), admitted.cores)
        );
    }

    #[test]
    fn a_budget_too_small_for_the_writer_is_a_typed_refusal() {
        let error = admit_main_write(&resources(1 << 20, 4), &shape()).expect_err("too small");
        let refusal = error.admission_refused().expect("an admission refusal");
        assert_eq!(refusal.phase, WRITER_PHASE);
        assert_eq!(refusal.available, 1 << 20);
        let minimum = minimum_writer_bytes(&shape()).expect("plannable") as u64;
        assert_eq!(refusal.required, 2 * minimum);
        assert!(writer_plan(&shape(), to_usize(minimum)).is_ok());
        assert!(writer_plan(&shape(), to_usize(minimum - 1)).is_err());
    }

    /// Admission state is the process's, so these laws assume no other
    /// test holds memory at once (nextest runs each test in its own
    /// process).
    #[test]
    fn workers_shrink_before_the_pipeline_is_refused() {
        // A budget of twice the writer's minimum, and arrays wide enough
        // that each worker's scratch is a third of what the writer leaves:
        // two workers fit, four do not.
        let writer = minimum_writer_bytes(&shape()).expect("plannable");
        let plan = writer_plan(&shape(), writer).expect("plan");
        let budget = 2 * writer as u64;
        let left = budget - plan.maximum_resident_bytes() as u64;
        let shape = MainShape {
            antennas: to_usize(left / 3 / ANTENNA_SCRATCH_BYTES),
            ..shape()
        };
        let four = CoreAllocation {
            workers: 5,
            streaming_workers: 4,
        };
        let two = pipeline_bytes(&shape, plan.batch_rows(), four.at_most(2));
        assert!(two <= left, "two workers fit: {two} of {left}");
        assert!(pipeline_bytes(&shape, plan.batch_rows(), four.at_most(3)) > left);
        let admitted = admit_main_write(&resources(budget, 5), &shape).expect("two fit");
        assert_eq!(admitted.policy_cores, four);
        assert_eq!(admitted.cores, four.at_most(2));
        drop(admitted);
        let error = admit_main_write(
            &resources(budget, 5),
            &MainShape {
                antennas: shape.antennas * 4,
                ..shape
            },
        )
        .expect_err("one worker does not fit");
        let refusal = error.admission_refused().expect("an admission refusal");
        assert_eq!(refusal.phase, PIPELINE_PHASE);
    }
}
