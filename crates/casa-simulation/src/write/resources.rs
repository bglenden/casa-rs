// SPDX-License-Identifier: LGPL-3.0-or-later
//! What `close` holds while it writes MAIN, admitted against the session's
//! resource policy before anything is written (plan §4: the runtime's
//! budget, not a second one).
//!
//! In order:
//! 1. **The writer.** The MeasurementSet write plan is sized from half of
//!    what the policy leaves free, and its resident bytes are admitted with
//!    the subtable columns its save generates (POINTING's runs, a few
//!    hundred bytes a scan). Its batches are tile bands of about
//!    [`casa_ms::TARGET_BATCH_BYTES`], so the budget is a ceiling that only
//!    a small budget reaches.
//! 2. **The pipeline.** The batch being filled, the geometry of the
//!    integrations it spans, each worker's scratch and, when the session
//!    predicts, the predictor, its block of native rows and (on Metal) the
//!    device copies of the model grids. A block is the whole batch when
//!    that fits with the policy's streaming workers; otherwise blocks are
//!    small, and as many workers as fit what is then free are admitted
//!    (#737's rule: workers shrink first). With one worker that does not
//!    fit, `close` is refused.
//!
//! The model grids themselves were admitted by `predict`, and are held.
//! A refusal is a typed [`Admission`] carried by the session error.

use casa_imaging_runtime::pass::BackendChoice;
use casa_imaging_runtime::{Admission, CoreAllocation, Demand, Reservation, admit, free_memory};
use casa_ms::{MeasurementSetWriteError, MeasurementSetWritePlan, MeasurementSetWriteResources};
use casa_tables::STREAMING_TILED_COLUMN_BUFFER_BYTES;
use casa_types::Complex32;

use super::geometry::RowGeometry;
use super::pipeline::Integration;
use super::prediction::PredictionStage;
use crate::corrupt::jones::JonesTerm;
use crate::corrupt::{BatchScales, NoiseScratch, TsysAtm};
use crate::session::SessionResources;
use crate::session::error::{SimulatorError, SimulatorResult};
use crate::session::plan::DataPlan;

/// The writer's phase, as an admission refusal names it.
pub(super) const WRITER_PHASE: &str = "the simulator's MeasurementSet writer";

/// The pipeline's phase, as an admission refusal names it.
pub(super) const PIPELINE_PHASE: &str = "the simulator's geometry, prediction and noise";

/// Batches the writer holds at once: the one it writes and one queued.
const WRITER_LIVE_BATCHES: usize = 2;

/// A writer budget larger than any plan's needs, to tell a budget too small
/// from a plan that cannot be made.
const UNBOUNDED_WRITER_BYTES: usize = 1 << 50;

/// Scratch of one worker's integration, per antenna: the antennas' uvw,
/// their shadow flags and elevations, and for `tsys-manual` noise their
/// airmasses (and elevations at a moved pointing's phase centre), with room
/// for the measures frames.
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
    /// Heap of the subtable columns the save generates (POINTING's), which
    /// grows with the scans.
    pub(super) generated_bytes: u64,
    /// What a batch's `tsys-atm` noise holds beside the cells.
    pub(super) noise_scales: NoiseScales,
}

/// What a batch's `tsys-atm` noise and `settrop` terms hold beside the
/// cells: their antenna scales and phasors ([`BatchScales`]), and each
/// worker's σ of a row ([`NoiseScratch`]).
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct NoiseScales {
    /// The plan's `tsys-atm` terms.
    terms: usize,
    /// The plan's `settrop` terms.
    tropospheric_terms: usize,
    /// Channels of the cells.
    channels: usize,
    /// Rows of the smallest integration, and antennas of the largest, when
    /// the rows come in whole integrations ([`BatchScales::most_entries`]).
    integrations: Option<(usize, usize)>,
}

impl NoiseScales {
    /// What `plan`'s corruptions hold, for cells of `channels` channels and
    /// the rows' `integrations`. Every `corrupt` covers scan 0, so its
    /// corruptions are all the plan's.
    pub(super) fn of(
        plan: &DataPlan,
        channels: usize,
        integrations: Option<(usize, usize)>,
    ) -> Self {
        let mut models: Vec<*const TsysAtm> = plan
            .noise_for_scan(0)
            .iter()
            .filter_map(|application| application.tsys_atm().map(std::ptr::from_ref))
            .collect();
        models.sort_unstable();
        models.dedup();
        let mut tropospheric: Vec<*const JonesTerm> = plan
            .corruptions_for_scan(0)
            .flat_map(|(_, jones)| jones)
            .filter(|term| term.tropospheric_phase().is_some())
            .map(|term| std::ptr::from_ref::<JonesTerm>(term))
            .collect();
        tropospheric.sort_unstable();
        tropospheric.dedup();
        Self {
            terms: models.len(),
            tropospheric_terms: tropospheric.len(),
            channels,
            integrations,
        }
    }

    /// The most antenna-scale and phasor entries a batch of `rows` rows
    /// needs: the room its table reserves.
    pub(super) fn entries(&self, rows: usize) -> (usize, usize) {
        (
            BatchScales::most_entries(self.terms, rows, self.integrations),
            BatchScales::most_entries(self.tropospheric_terms, rows, self.integrations),
        )
    }

    /// The most a batch of `rows` rows holds with `workers` workers.
    pub(super) fn bytes(&self, rows: usize, workers: usize) -> u64 {
        let scratch = if self.terms == 0 {
            0
        } else {
            workers as u64 * NoiseScratch::bytes(self.channels)
        };
        BatchScales::bytes(self.entries(rows), self.channels) + scratch
    }
}

/// What `close` was admitted: the write plan, the workers that fit, and the
/// memory, held until the write ends.
#[derive(Debug)]
pub(super) struct MainAdmission {
    pub(super) plan: MeasurementSetWritePlan,
    pub(super) cores: CoreAllocation,
    /// The policy's allocation, before admission shrank it.
    pub(super) policy_cores: CoreAllocation,
    /// Rows of a prediction block: the write batch's, or fewer when the
    /// budget is small.
    pub(super) block_rows: usize,
    pub(super) reservation: Reservation,
}

/// Admit `close`'s writer and pipeline for `shape` within `resources`, the
/// pipeline predicting with `prediction` when the session predicts.
///
/// # Errors
///
/// [`SimulatorError::admission`] when the writer, or the pipeline with one
/// worker, does not fit what the policy leaves free; a raised error when the
/// MAIN write cannot be planned at all.
pub(super) fn admit_main_write(
    resources: &SessionResources,
    shape: &MainShape<'_>,
    prediction: Option<&PredictionStage<'_>>,
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
            memory: plan.maximum_resident_bytes() as u64 + shape.generated_bytes,
        },
    )
    .map_err(SimulatorError::admission)?;
    let policy_cores = resources
        .close_cores(prediction.is_some_and(|stage| stage.backend() == BackendChoice::Metal));
    let free = free_memory(host, policy);
    let batch_rows = plan.batch_rows();
    let demand = |cores: CoreAllocation, block_rows: usize| {
        pipeline_bytes(shape, batch_rows, cores)
            + prediction.map_or(0, |stage| stage.bytes(block_rows, cores.streaming_workers))
    };
    // A batch predicted as one block when that fits with the policy's
    // workers; else small blocks, and the workers shrink first (#737).
    let (cores, block_rows) = if demand(policy_cores, batch_rows) <= free {
        (policy_cores, batch_rows)
    } else {
        let small = prediction.map_or(batch_rows, |stage| stage.small_block_rows(batch_rows));
        let cores = (1..=policy_cores.streaming_workers)
            .rev()
            .map(|workers| policy_cores.at_most(workers))
            .find(|cores| demand(*cores, small) <= free)
            .unwrap_or_else(|| policy_cores.at_most(1));
        (cores, small)
    };
    let pipeline = admit(
        host,
        policy,
        &Demand {
            phase: PIPELINE_PHASE,
            memory: demand(cores, block_rows),
        },
    )
    .map_err(SimulatorError::admission)?;
    reservation.join(pipeline);
    Ok(MainAdmission {
        plan,
        cores,
        policy_cores,
        block_rows,
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
/// integrations, one integration's scalar runs, each worker's scratch, and
/// the batch's `tsys-atm` antenna scales.
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
    let noise = shape
        .noise_scales
        .bytes(batch_rows, cores.streaming_workers);
    batch + geometry + integrations + scalars + workers + noise
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
            generated_bytes: 0,
            noise_scales: NoiseScales::default(),
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
        let admitted = admit_main_write(&resources(1 << 30, 5), &shape(), None).expect("fits");
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
        let error =
            admit_main_write(&resources(1 << 20, 4), &shape(), None).expect_err("too small");
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
        let admitted = admit_main_write(&resources(budget, 5), &shape, None).expect("two fit");
        assert_eq!(admitted.policy_cores, four);
        assert_eq!(admitted.cores, four.at_most(2));
        drop(admitted);
        let error = admit_main_write(
            &resources(budget, 5),
            &MainShape {
                antennas: shape.antennas * 4,
                ..shape
            },
            None,
        )
        .expect_err("one worker does not fit");
        let refusal = error.admission_refused().expect("an admission refusal");
        assert_eq!(refusal.phase, PIPELINE_PHASE);
    }
}
