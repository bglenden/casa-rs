// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convert a store with the trial converter and report what it did, for
//! the measurements in `docs/measurement-set/trial/README.md`.
//!
//! ```text
//! ms_trial_convert to-msv4|to-msv2 SOURCE DESTINATION [--workers N]
//!     [--batch-mib M] [--chunk-times T] [--codec blosc|zstd|none]
//!     [--padding flagged|omitted] [--accept KIND,...]
//! ```
//!
//! `KIND` is `sigma`, `row-flags`, `flag-categories`, `data-groups`,
//! `tables` or `columns`. Prints one JSON object: elapsed seconds and the
//! conversion report.

use std::path::PathBuf;
use std::time::Instant;

use casa_ms::observation_io::Msv4Codec;
use casa_ms::observation_io::convert::{
    ConversionOptions, LossKind, PaddingRows, to_msv2, to_msv4,
};
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
        other => return Err(format!("unknown loss kind {other}").into()),
    })
}

fn main() -> Result<(), Error> {
    let mut args = std::env::args().skip(1);
    let direction = args.next().ok_or("a direction: to-msv4 or to-msv2")?;
    let source = PathBuf::from(args.next().ok_or("a source")?);
    let destination = PathBuf::from(args.next().ok_or("a destination")?);
    let mut options = ConversionOptions {
        data_groups: None,
        accept: Vec::new(),
        batch_bytes: 256 << 20,
        workers: 1,
        chunks: [0, 0, 0, 0],
        codec: Msv4Codec::BloscLz4,
        padding: PaddingRows::Flagged,
        creator: "casa-rs ms_trial_convert".to_string(),
        creation_date: "2026-10-26T00:00:00+00:00".to_string(),
    };
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--workers" => options.workers = value.parse()?,
            "--batch-mib" => options.batch_bytes = value.parse::<usize>()? << 20,
            "--chunk-times" => options.chunks[0] = value.parse()?,
            "--codec" => {
                options.codec = match value.as_str() {
                    "blosc" => Msv4Codec::BloscLz4,
                    "zstd" => Msv4Codec::Zstd,
                    "none" => Msv4Codec::None,
                    other => return Err(format!("unknown codec {other}").into()),
                }
            }
            "--padding" => {
                options.padding = match value.as_str() {
                    "flagged" => PaddingRows::Flagged,
                    "omitted" => PaddingRows::Omitted,
                    other => return Err(format!("unknown padding {other}").into()),
                }
            }
            "--accept" => {
                options.accept = value.split(',').map(kind).collect::<Result<_, _>>()?;
            }
            other => return Err(format!("unknown option {other}").into()),
        }
    }
    let started = Instant::now();
    let report = match direction.as_str() {
        "to-msv4" => to_msv4(&source, &destination, &options)?,
        "to-msv2" => to_msv2(&source, &destination, &options)?,
        other => return Err(format!("unknown direction {other}").into()),
    };
    let seconds = started.elapsed().as_secs_f64();
    println!(
        "{}",
        json!({
            "direction": direction,
            "seconds": seconds,
            "workers": options.workers,
            "batch_mib": options.batch_bytes >> 20,
            "chunk_times": options.chunks[0],
            "partitions": report.partitions,
            "batches": report.batches,
            "samples": report.samples,
            "rows": report.rows,
            "elements": report.elements,
            "held_elements": report.held_elements,
            "losses": report.losses.iter().map(ToString::to_string).collect::<Vec<_>>(),
        })
    );
    Ok(())
}
