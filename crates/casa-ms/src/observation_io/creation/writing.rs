// SPDX-License-Identifier: LGPL-3.0-or-later
//! What writing a plan holds, admitted when the plan is made: beside what
//! the plan keeps, the subtables and the `POINTING` table until the save,
//! then the larger of the pointing reads (each input's in turn) and the
//! MAIN writers with one batch of every selected group. Every input
//! partition's batch must hold one integration; the MAIN writers assemble
//! their tile bands in memory when the budget holds them, and otherwise
//! write each tile row in place.

use casa_observation::{ObservationError, Roles};

use super::combine::Combined;
use super::inputs::Input;
use super::lockstep::Sizing;
use super::memory::{Ledger, MemoryCharges, catalogue_bytes};
use super::msv2_pointing::TABLE_ROW_BYTES;
use super::msv2_rows::{self, Occupancy};
use super::pointing_plan::PointingPlan;
use crate::observation_io::update::storage;
use crate::write_session::MeasurementSetWritePlan;

/// Bytes allowed the created MeasurementSet's tables beside their rows,
/// from creation through the save: their descriptions and storage managers
/// and the save's encoding (about 2 MB for an MSv2's subtables).
const CREATED_TABLES_BYTES: usize = 3 << 20;

/// MAIN's write plan, and what the plan admitted.
pub(super) struct Writing {
    pub(super) plan: MeasurementSetWritePlan,
    pub(super) charges: MemoryCharges,
}

/// The batches writing reads.
struct Batches {
    /// The largest, of `batch_bytes` at most.
    bytes: usize,
    /// The most samples one holds.
    samples: usize,
    /// The largest integration, one time of every group a partition reads.
    integration: usize,
}

/// The batches writing reads, of `batch_bytes` at most; refused where an
/// input partition's integration exceeds `batch_bytes`.
fn batches(
    inputs: &[Input],
    combined: &Combined,
    batch_bytes: usize,
) -> Result<Batches, ObservationError> {
    let mut batches = Batches {
        bytes: 0,
        samples: 0,
        integration: 0,
    };
    for (input, map) in inputs.iter().zip(&combined.rows) {
        let observation = input.observation.as_ref();
        for id in observation.catalogue().partition_ids() {
            let placement = &map.partitions[id.0 as usize];
            let sizing = Sizing::of(observation, id, &placement.groups, Roles::ALL)?;
            batches.integration = batches.integration.max(sizing.integration());
            let times = sizing.times_per_batch(batch_bytes, None);
            batches.bytes = batches.bytes.max(times * sizing.integration());
            batches.samples = batches.samples.max(times * sizing.samples_per_time);
        }
    }
    if batches.integration > batch_bytes {
        return Err(ObservationError::OverBudget {
            what: "a batch of one integration of every selected group",
            required: batches.integration,
            budget: batch_bytes,
        });
    }
    Ok(batches)
}

/// Admit to `ledger` what writing `combined`'s `occupancy` from `inputs`
/// (with their `pointing`) and `data_columns` holds, in batches of
/// `batch_bytes`; MAIN's write plan, and the plan's charges.
///
/// # Errors
///
/// [`ObservationError::OverBudget`] when a batch cannot hold one
/// integration, or writing beside what the plan keeps exceeds the budget
/// with the MAIN writers at their least.
pub(super) fn admit(
    ledger: &mut Ledger,
    (inputs, combined, pointing): (&[Input], &Combined, &[PointingPlan]),
    (data_columns, occupancy): (&[&'static str], &Occupancy),
    batch_bytes: usize,
) -> Result<Writing, ObservationError> {
    let (planning, kept) = (ledger.peak(), ledger.held());
    let Batches {
        bytes: batch,
        samples,
        integration,
    } = batches(inputs, combined, batch_bytes)?;
    let catalogue = &combined.catalogue;
    let columns = msv2_rows::array_columns(catalogue, data_columns, occupancy)?;
    let (least, buffered) =
        MeasurementSetWritePlan::variable_array_column_bytes(&columns).map_err(storage)?;
    let cell = catalogue
        .partitions
        .iter()
        .map(|entry| entry.frequencies_hz.len() * entry.correlations.len())
        .max()
        .unwrap_or(0);
    let rows = msv2_rows::held_bytes(samples, cell);
    let held = CREATED_TABLES_BYTES
        + catalogue_bytes(catalogue)
        + pointing
            .iter()
            .map(|plan| plan.gathered().0 * TABLE_ROW_BYTES)
            .sum::<usize>();
    let reads = pointing.iter().map(PointingPlan::peak).max().unwrap_or(0);
    let writing = |plan: &MeasurementSetWritePlan| {
        let writer = plan.maximum_resident_bytes() + rows;
        (writer, held + reads.max(writer + batch))
    };
    let least_plan = msv2_rows::write_plan(columns.clone(), occupancy, least)?;
    let buffered_plan = msv2_rows::write_plan(columns, occupancy, buffered)?;
    let least_writing = writing(&least_plan).1;
    let plan = if kept + writing(&buffered_plan).1 <= ledger.budget() {
        buffered_plan
    } else {
        least_plan
    };
    let (writer, total) = writing(&plan);
    ledger.admit("writing the MeasurementSet", total)?;
    Ok(Writing {
        plan,
        charges: MemoryCharges {
            kept,
            planning,
            writing: total,
            writer,
            batch,
            least: planning.max(kept + least_writing),
            integration,
        },
    })
}
