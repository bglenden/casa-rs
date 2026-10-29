// SPDX-License-Identifier: LGPL-3.0-or-later
//! Create a MeasurementSet v2 from several observations with the creation
//! plan (#937 PR B) and report what it did, for the comparisons with CASA's
//! `concat`.
//!
//! ```text
//! ms_create_from DESTINATION INPUT[=SCALE]... [--sort] [--batch-mib M]
//!     [--accept KIND,...]
//! ```
//!
//! Each `INPUT` may carry its weight scale (`part2.ms=2.0`, CASA's
//! `visweightscale`). `--sort` orders the inputs by their earliest time, as
//! CASA's `concat` task does. Padding is omitted. `KIND` is `sigma`,
//! `row-flags`, `flag-categories`, `data-groups`, `tables`, `columns` or
//! `defaulted`. Prints one JSON object: elapsed seconds, the inputs in the
//! order created, the report and the merge's notices.

use std::path::PathBuf;
use std::time::Instant;

use casa_ms::observation_io::creation::{
    CreationInput, CreationOptions, CreationPlan, Destination, LossKind, PaddingRows,
};
use casa_ms::observation_io::{OpenOptions, open};
use casa_observation::MergeTolerance;
use serde_json::json;

type Error = Box<dyn std::error::Error>;

fn kind(name: &str) -> Result<LossKind, Error> {
    Ok(match name {
        "sigma" => LossKind::Sigma,
        "row-flags" => LossKind::RowFlags,
        "flag-categories" => LossKind::FlagCategories,
        "data-groups" => LossKind::DataGroups,
        "tables" => LossKind::Tables,
        "columns" => LossKind::Columns,
        "defaulted" => LossKind::Defaulted,
        other => return Err(format!("unknown loss kind {other}").into()),
    })
}

/// The earliest time of the store at `input`, MJD seconds.
fn earliest(input: &CreationInput) -> Result<f64, Error> {
    let observation = open(&input.path, &OpenOptions::default())?;
    let catalogue = observation.catalogue();
    let mut earliest = f64::INFINITY;
    for id in catalogue.partition_ids() {
        let Some(group) = catalogue.partition(id).data_groups.first() else {
            continue;
        };
        let grid = observation.grid(id, &group.name)?;
        if let Some(first) = grid.times_s.first() {
            let origin = grid.time_origin;
            earliest = earliest.min((origin.day() + origin.frac()) * 86_400.0 + first);
        }
    }
    Ok(earliest)
}

fn main() -> Result<(), Error> {
    let mut args = std::env::args().skip(1);
    let destination = PathBuf::from(args.next().ok_or("a destination")?);
    let mut inputs = Vec::new();
    let mut sort = false;
    let mut options = CreationOptions {
        data_groups: None,
        accept: Vec::new(),
        batch_bytes: 256 << 20,
        padding: PaddingRows::Omitted,
        tolerance: MergeTolerance::default(),
        history: Vec::new(),
    };
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--sort" => sort = true,
            "--batch-mib" => {
                let value = args.next().ok_or("--batch-mib needs a value")?;
                options.batch_bytes = value.parse::<usize>()? << 20;
            }
            "--accept" => {
                let value = args.next().ok_or("--accept needs a value")?;
                options.accept = value.split(',').map(kind).collect::<Result<_, _>>()?;
            }
            input => {
                let (path, scale) = match input.split_once('=') {
                    Some((path, scale)) => (path, scale.parse()?),
                    None => (input, 1.0),
                };
                inputs.push(CreationInput {
                    path: PathBuf::from(path),
                    weight_scale: scale,
                });
            }
        }
    }
    if sort {
        let mut timed = inputs
            .into_iter()
            .map(|input| earliest(&input).map(|time| (time, input)))
            .collect::<Result<Vec<_>, _>>()?;
        timed.sort_by(|a, b| a.0.total_cmp(&b.0));
        inputs = timed.into_iter().map(|(_, input)| input).collect();
    }
    let started = Instant::now();
    let plan = CreationPlan::new(&inputs, &options)?;
    let notices = plan.notices().to_vec();
    let report = plan.write(&destination, Destination::New)?;
    println!(
        "{}",
        json!({
            "seconds": started.elapsed().as_secs_f64(),
            "inputs": inputs
                .iter()
                .map(|input| format!("{}={}", input.path.display(), input.weight_scale))
                .collect::<Vec<_>>(),
            "partitions": report.partitions,
            "batches": report.batches,
            "samples": report.samples,
            "rows": report.rows,
            "losses": report.losses.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "notices": notices,
        })
    );
    Ok(())
}
