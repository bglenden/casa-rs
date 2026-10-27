// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read every partition of a store, every role, several times in one
//! process, and report each pass's time and the process's peak resident
//! memory after it: whether what the store stack keeps (zarrs_filesystem's
//! per-key locks, codec scratch) plateaus over passes, for the measurements
//! in `docs/measurement-set/trial/README.md`.
//!
//! ```text
//! ms_trial_read_passes SOURCE [--passes N] [--workers W] [--batch-mib M]
//! ```
//!
//! Partitions are read `W` at a time, each by its own thread. Prints one
//! JSON object per pass.

use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use casa_ms::observation_io::{OpenOptions, open};
use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, Grouping, Observation, PartitionId, ReadEvent,
    Roles,
};
use serde_json::json;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// The process's peak resident set in bytes.
fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage fills the struct it is given.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let max = usage.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        max
    } else {
        max * 1024
    }
}

/// Read partition `id` whole; the samples and batches delivered.
fn read(
    observation: &dyn Observation,
    id: PartitionId,
    budget: usize,
) -> Result<(usize, usize), Error> {
    let entry = observation.catalogue().partition(id);
    let request = AccessRequest {
        partitions: vec![id],
        data_group: "base".to_string(),
        roles: Roles::ALL,
        channels: 0..entry.frequencies_hz.len(),
        correlations: CorrelationRequest::ALL,
        grouping: Grouping::None,
        batch_bytes: budget,
    };
    let mut traversal = observation.bind_read(&request)?;
    let mut buffers = BatchBuffers::default();
    let (mut samples, mut batches) = (0, 0);
    while let Some(event) = traversal.next(&mut buffers)? {
        if let ReadEvent::Batch(batch) = event {
            samples += batch.samples();
            batches += 1;
        }
    }
    Ok((samples, batches))
}

fn main() -> Result<(), Error> {
    let mut args = std::env::args().skip(1);
    let source = PathBuf::from(args.next().ok_or("a source")?);
    let (mut passes, mut workers, mut budget) = (3usize, 1usize, 256usize << 20);
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--passes" => passes = value.parse()?,
            "--workers" => workers = value.parse()?,
            "--batch-mib" => budget = value.parse::<usize>()? << 20,
            other => return Err(format!("unknown option {other}").into()),
        }
    }
    let opened = Instant::now();
    let observation = open(&source, &OpenOptions::default()).map_err(|e| e.to_string())?;
    let ids = observation.catalogue().partition_ids();
    println!(
        "{}",
        json!({"opened_seconds": opened.elapsed().as_secs_f64(), "partitions": ids.len(), "peak_rss_bytes": peak_rss_bytes()})
    );
    for pass in 0..passes {
        let started = Instant::now();
        let next = AtomicUsize::new(0);
        let totals = Mutex::new((0usize, 0usize));
        let failure = Mutex::new(None);
        std::thread::scope(|scope| {
            for _ in 0..workers.clamp(1, ids.len().max(1)) {
                scope.spawn(|| {
                    while let Some(id) = ids.get(next.fetch_add(1, Ordering::Relaxed)) {
                        match read(observation.as_ref(), *id, budget) {
                            Ok((samples, batches)) => {
                                let mut totals = totals.lock().expect("totals");
                                totals.0 += samples;
                                totals.1 += batches;
                            }
                            Err(error) => {
                                *failure.lock().expect("failure") = Some(error.to_string());
                                next.store(ids.len(), Ordering::Relaxed);
                            }
                        }
                    }
                });
            }
        });
        if let Some(error) = failure.into_inner().expect("failure") {
            return Err(error.into());
        }
        let (samples, batches) = totals.into_inner().expect("totals");
        println!(
            "{}",
            json!({
                "pass": pass,
                "seconds": started.elapsed().as_secs_f64(),
                "workers": workers,
                "samples": samples,
                "batches": batches,
                "peak_rss_bytes": peak_rss_bytes(),
            })
        );
    }
    Ok(())
}
