// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rewrite every partition's `base` visibilities of a store with the values
//! it holds, through an update session, and report the pass's time, the
//! time its updaters' finishes took (an MSv4 updater synchronizes the
//! directories it changed there) and the update reports: the cost of
//! write-back's atomic chunk replacement and synchronization at scale, for
//! the measurements in `docs/measurement-set/trial/README.md`.
//!
//! ```text
//! ms_trial_update_pass STORE [--batch-mib M]
//! ```
//!
//! The store is changed in place (its values rewritten unchanged): run it on
//! a copy. Prints one JSON object.

use std::path::PathBuf;
use std::time::Instant;

use casa_ms::observation_io::{OpenOptions, open_update};
use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, Grouping, PatchValues, ReadEvent, Roles,
    TimeSelection, UpdateTarget, WritableRole,
};
use serde_json::json;

type Error = Box<dyn std::error::Error>;

fn main() -> Result<(), Error> {
    let mut args = std::env::args().skip(1);
    let store = PathBuf::from(args.next().ok_or("a store")?);
    let mut budget = 256usize << 20;
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--batch-mib" => budget = value.parse::<usize>()? << 20,
            other => return Err(format!("unknown option {other}").into()),
        }
    }
    let started = Instant::now();
    let session = open_update(
        &store,
        &OpenOptions::default(),
        &[UpdateTarget::new("base", WritableRole::Visibilities)],
    )?;
    let opened = started.elapsed().as_secs_f64();
    let catalogue = session.observation().catalogue().clone();
    let (mut finishing, mut samples, mut units_written, mut units_read, mut held) =
        (0.0, 0, 0, 0, 0);
    for id in catalogue.partition_ids() {
        let request = AccessRequest {
            partitions: vec![id],
            data_group: "base".to_string(),
            roles: Roles::ALL,
            channels: 0..catalogue.partition(id).frequencies_hz.len(),
            times: TimeSelection::All,
            correlations: CorrelationRequest::ALL,
            grouping: Grouping::None,
            batch_bytes: budget,
        };
        let mut traversal = session.observation().bind_read(&request)?;
        let mut updater = session.updater(&request, &[0])?;
        let mut buffers = BatchBuffers::default();
        while let Some(event) = traversal.next(&mut buffers)? {
            if let ReadEvent::Batch(batch) = event {
                let values = batch.group.values.ok_or("visibilities")?;
                updater.write(&batch, &[PatchValues::Visibilities(values)])?;
            }
        }
        let finish = Instant::now();
        let report = updater.finish()?;
        finishing += finish.elapsed().as_secs_f64();
        samples += report.samples;
        units_written += report.units_written;
        units_read += report.units_read;
        held = held.max(report.held_bytes);
    }
    println!(
        "{}",
        json!({
            "seconds": started.elapsed().as_secs_f64(),
            "open_seconds": opened,
            "finish_seconds": finishing,
            "partitions": catalogue.partitions.len(),
            "samples": samples,
            "units_written": units_written,
            "units_read": units_read,
            "held_bytes": held,
        })
    );
    Ok(())
}
