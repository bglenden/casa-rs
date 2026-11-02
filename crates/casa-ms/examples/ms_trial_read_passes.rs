// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read every partition of a store, every role, several times in one
//! process, and report each pass's time and the process's peak resident
//! memory after it: whether what the store stack keeps (zarrs_filesystem's
//! per-key locks, codec scratch) plateaus over passes, for the measurements
//! in `docs/measurement-set/trial/README.md`.
//!
//! ```text
//! ms_trial_read_passes SOURCE [--passes N] [--workers W] [--batch-mib M]
//!     [--split partitions|times] [--only I] [--weights stored|sigma]
//! ```
//!
//! Work is read `W` units at a time, each by its own thread: whole
//! partitions (the default), or with `--split times` each partition's
//! times cut into `W` slices aligned to the store's natural time blocks
//! (#910), so the workers share every partition. `--only I` reads partition
//! `I` alone, where only a time split lets workers share the work.
//! `--weights sigma` reads weights as 1/σ² of the sigmas (an MSv4 falls back
//! to its stored weights). Prints one JSON object per pass, with the decode
//! units the reads touched.

use std::ops::Range;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use casa_ms::observation_io::{OpenOptions, open};
use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, Grouping, Observation, PartitionId, ReadEvent,
    Roles, SigmaFallback, TimeRange, TimeSelection, WeightRequest,
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

/// One unit of work: a partition, whole or a slice of its times.
#[derive(Clone)]
struct Unit {
    partition: PartitionId,
    times: TimeSelection,
}

/// How every unit is read.
#[derive(Clone, Copy)]
struct Reading {
    weights: WeightRequest,
    budget: usize,
}

/// The work over partitions `ids`: each whole, or cut into `slices` slices
/// of its times, each a whole number of the store's natural time blocks.
fn units(
    observation: &dyn Observation,
    ids: Vec<PartitionId>,
    slices: Option<usize>,
    reading: Reading,
) -> Result<Vec<Unit>, Error> {
    let mut units = Vec::new();
    for id in ids {
        let Some(slices) = slices else {
            units.push(Unit {
                partition: id,
                times: TimeSelection::All,
            });
            continue;
        };
        let grid = observation.grid(id, "base")?;
        let block = observation
            .estimate(&request(observation, id, TimeSelection::All, reading))?
            .partitions[0]
            .natural_blocks
            .times
            .max(1);
        // The partition's whole blocks spread as evenly as they go.
        let times = grid.times_s.len();
        let blocks = times.div_ceil(block);
        let slices = slices.min(blocks).max(1);
        for slice in 0..slices {
            let range: Range<usize> = (slice * blocks / slices * block).min(times)
                ..((slice + 1) * blocks / slices * block).min(times);
            units.push(Unit {
                partition: id,
                times: TimeSelection::Within(TimeRange::of_grid(&grid, range)),
            });
        }
    }
    Ok(units)
}

fn request(
    observation: &dyn Observation,
    id: PartitionId,
    times: TimeSelection,
    reading: Reading,
) -> AccessRequest {
    let entry = observation.catalogue().partition(id);
    AccessRequest {
        fields: casa_observation::FieldSelection::All,
        partitions: vec![id],
        data_group: "base".to_string(),
        roles: Roles::ALL,
        channels: 0..entry.frequencies_hz.len(),
        times,
        weights: reading.weights,
        correlations: CorrelationRequest::ALL,
        grouping: Grouping::None,
        batch_bytes: reading.budget,
    }
}

/// Read `unit`; the samples, batches and decode units delivered.
fn read(
    observation: &dyn Observation,
    unit: &Unit,
    reading: Reading,
) -> Result<(usize, usize, usize), Error> {
    let request = request(observation, unit.partition, unit.times, reading);
    let mut traversal = observation.bind_read(&request)?;
    let mut buffers = BatchBuffers::default();
    let (mut samples, mut batches) = (0, 0);
    while let Some(event) = traversal.next(&mut buffers)? {
        if let ReadEvent::Batch(batch) = event {
            samples += batch.samples();
            batches += 1;
        }
    }
    Ok((samples, batches, traversal.counters().decode_units))
}

fn main() -> Result<(), Error> {
    let mut args = std::env::args().skip(1);
    let source = PathBuf::from(args.next().ok_or("a source")?);
    let (mut passes, mut workers, mut budget) = (3usize, 1usize, 256usize << 20);
    let mut split_times = false;
    let mut only = None;
    let mut weights = WeightRequest::Stored;
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--passes" => passes = value.parse()?,
            "--workers" => workers = value.parse()?,
            "--batch-mib" => budget = value.parse::<usize>()? << 20,
            "--split" => {
                split_times = match value.as_str() {
                    "partitions" => false,
                    "times" => true,
                    other => return Err(format!("unknown split {other}").into()),
                }
            }
            "--only" => only = Some(PartitionId(value.parse()?)),
            "--weights" => {
                weights = match value.as_str() {
                    "stored" => WeightRequest::Stored,
                    "sigma" => WeightRequest::InverseSigmaSquared {
                        fallback: SigmaFallback::StoredWeights,
                    },
                    other => return Err(format!("unknown weights {other}").into()),
                }
            }
            other => return Err(format!("unknown option {other}").into()),
        }
    }
    let reading = Reading { weights, budget };
    let opened = Instant::now();
    let observation = open(&source, &OpenOptions::default()).map_err(|e| e.to_string())?;
    let partitions = match only {
        Some(id) => vec![id],
        None => observation.catalogue().partition_ids(),
    };
    let ids = units(
        observation.as_ref(),
        partitions,
        split_times.then_some(workers.max(1)),
        reading,
    )?;
    println!(
        "{}",
        json!({"opened_seconds": opened.elapsed().as_secs_f64(), "units": ids.len(), "peak_rss_bytes": peak_rss_bytes()})
    );
    for pass in 0..passes {
        let started = Instant::now();
        let next = AtomicUsize::new(0);
        let totals = Mutex::new((0usize, 0usize, 0usize));
        let failure = Mutex::new(None);
        std::thread::scope(|scope| {
            for _ in 0..workers.clamp(1, ids.len().max(1)) {
                scope.spawn(|| {
                    while let Some(unit) = ids.get(next.fetch_add(1, Ordering::Relaxed)) {
                        match read(observation.as_ref(), unit, reading) {
                            Ok((samples, batches, units)) => {
                                let mut totals = totals.lock().expect("totals");
                                totals.0 += samples;
                                totals.1 += batches;
                                totals.2 += units;
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
        let (samples, batches, decode_units) = totals.into_inner().expect("totals");
        println!(
            "{}",
            json!({
                "pass": pass,
                "seconds": started.elapsed().as_secs_f64(),
                "workers": workers,
                "split": if split_times { "times" } else { "partitions" },
                "weights": if weights == WeightRequest::Stored { "stored" } else { "sigma" },
                "samples": samples,
                "batches": batches,
                "decode_units": decode_units,
                "peak_rss_bytes": peak_rss_bytes(),
            })
        );
    }
    Ok(())
}
