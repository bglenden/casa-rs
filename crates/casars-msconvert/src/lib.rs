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
//! written, and every loss of a kind not named in `acceptloss` refuses the
//! conversion, naming all of them. `acceptloss` defaults to `sigma`, the
//! loss every conversion between the two formats takes: MSv4 holds no
//! `SIGMA`, and an MSv2 made from an MSv4 states `SIGMA` as `1/√WEIGHT`.
//!
//! An MSv4 destination is named and laid out as XRADIO's converter lays
//! out an MSv2: one dataset per partition, and by default XRADIO's chunk
//! shape ([`default_chunks`]). Its partitions are converted `nthreads` at
//! a time. An MSv2 destination is written by one thread.
//!
//! The task returns a record of what it did: the formats, the partitions
//! converted and the losses taken.

mod chunks;
mod convert;
mod request;
mod task_contract;

use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};

pub use chunks::{BLOSC_MAX_BUFFER_BYTES, DEFAULT_CHUNK_BYTES, PayloadShape, default_chunks};
pub use convert::msconvert;
pub use request::{MsconvertRequest, StoreFormat, request_from};
pub use task_contract::*;

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
    // Each accepted loss is logged as a warning when it is taken.
    let text = format!(
        "Converted {} ({}) to {} ({}): partitions {}, accepted losses {}\n",
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
