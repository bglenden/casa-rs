// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! `msconvert`: convert between MeasurementSet v2 and MSv4 (XRADIO's
//! processing set), in either direction, on the shared task front end.
//!
//! The task is casa-rs's own: CASA has none, and XRADIO's
//! `convert_msv2_to_processing_set` converts one way only. It is a surface
//! over casa-ms's converter
//! ([`casa_ms::observation_io::convert`]), which reads the source through
//! the observation boundary as any consumer reads it and creates the
//! destination from those batches: every acquisition and every selected
//! data group, flags, weights, UVW and time auxiliaries are carried
//! exactly, and padding stays padding.
//!
//! What the destination cannot hold is inventoried before anything is
//! written. The task takes every such loss, as converting into the other
//! format asks it to, and reports each: logged as a warning, and returned.
//! Every conversion between the two formats loses `SIGMA`: MSv4 holds
//! none, and an MSv2 made from an MSv4 states it as `1/√WEIGHT`. Most also
//! lose subtables MSv4 does not hold (`HISTORY`, `WEATHER`, ...), or state
//! in an MSv2 what an MSv4 leaves unstated, as MSv2's defaults.
//!
//! Stores are named as XRADIO names them. Without an `outputvis`, the
//! output is written beside `vis`, named after it with `.ps.zarr` for an
//! MSv4 or `.ms` for an MSv2 ([`default_outputvis`]): `obs.ms` converts to
//! `obs.ps.zarr`, and `obs.ps.zarr` back to `obs.ms`. An MSv4 `outputvis`
//! not ending in `ps.zarr` gains `.ps.zarr`, as XRADIO's converter names
//! it ([`processing_set_path`]).
//!
//! An MSv4 destination is named and laid out as XRADIO's converter lays
//! out an MSv2, with its options: one dataset per partition, the MSv2
//! partitioned further by `partitionscheme` (XRADIO's `partition_scheme`),
//! its pointing converted unless `withpointing` is off (`with_pointing`),
//! and by default each dataset chunked as
//! XRADIO chunks it
//! ([`xradio_chunks`](casa_ms::observation_io::convert::xradio_chunks)).
//! Its partitions are converted `nthreads` at a time, by default as many as
//! the cores and the memory allow. An MSv2 destination is written by one
//! thread.
//!
//! The task returns a record of what it did: the formats, the partitions
//! converted and the losses taken.

mod convert;
mod request;
mod task_contract;

use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};

pub use convert::msconvert;
pub use request::{
    MsconvertRequest, StoreFormat, default_outputvis, processing_set_path, request_from,
};
pub use task_contract::*;

/// The most bytes one time range of every selected group's batches holds:
/// the trial's measured budget.
const BATCH_BYTES: usize = 256 << 20;

/// What one thread converting a partition into an MSv4 may hold, as the
/// default thread count budgets it: its batches ([`BATCH_BYTES`]), the
/// partial chunks it holds toward completion and their encoding.
const THREAD_BYTES: usize = 4 * BATCH_BYTES;

/// The front end of the `msconvert` binary, which hosts `msconvert`.
pub fn task_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new(
        "msconvert",
        msconvert_task_schema_bundle(),
        &["msconvert"],
        run_task,
    )
    .expect("the msconvert contract carries its surface")
}

/// Run one resolved msconvert request.
fn run_task(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let request = request_from(task)?;
    let result = msconvert(&request)?;
    // Each loss is logged as a warning when it is taken.
    let text = format!(
        "Converted {} ({}) to {} ({}): partitions {}, losses {}\n",
        result.vis,
        result.informat,
        result.outputvis,
        result.outformat,
        result.partitions,
        result.losses.len()
    );
    Ok(TaskSuccess::new(return_value(&result), &result)?.with_text(text))
}

/// What the task returns to Python: the formats, the paths, the
/// partitions converted and the losses taken.
#[must_use]
pub fn return_value(result: &MsconvertTaskResult) -> CasaValue {
    let int = |value: usize| CasaValue::Int {
        value: i64::try_from(value).unwrap_or(i64::MAX),
    };
    CasaValue::dict([
        ("vis", CasaValue::string(&result.vis)),
        ("outputvis", CasaValue::string(&result.outputvis)),
        ("informat", CasaValue::string(&result.informat)),
        ("outformat", CasaValue::string(&result.outformat)),
        ("partitions", int(result.partitions)),
        (
            "losses",
            CasaValue::List {
                items: result.losses.iter().map(CasaValue::string).collect(),
            },
        ),
    ])
}
