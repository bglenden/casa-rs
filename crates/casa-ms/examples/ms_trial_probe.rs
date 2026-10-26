// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv2/MSv4 trial's scale probe (`docs/measurement-set/trial/README.md`):
//! opens a store through the trial adapters and reports what opening cost,
//! what the estimate predicts for each partition, and what one traversal of
//! every partition reads.
//!
//! ```text
//! cargo run --release -p casa-ms --example ms_trial_probe -- STORE [--open-only] [--batch-mib N] [--channels N] [--products XX,YY] [--parallel]
//! ```
//!
//! Run it under `/usr/bin/time -l` (macOS) or `/usr/bin/time -v` (Linux) for
//! the peak resident memory; `--open-only` separates the opening index from
//! the traversal. Each partition is read in its first data group, every
//! role, its first `--channels` channels (default all) and every product or
//! the `--products` listed, with a `--batch-mib` budget (default 256).
//! `--parallel` then reads every partition again, its cursors shared by one
//! worker thread per core, and checks each partition's samples and
//! checksum against the sequential read.

use std::sync::Mutex;
use std::time::Instant;

use casa_ms::observation_io::{OpenOptions, open};
use casa_observation::{
    AccessRequest, BatchBuffers, BoundCursor, Correlation, CorrelationRequest, Deliver, FlagsFrom,
    Grouping, PartitionId, ReadEvent, Roles, VisibilityCursor,
};

const USAGE: &str = "usage: ms_trial_probe STORE [--open-only] [--batch-mib N] [--channels N] \
                     [--products XX,YY] [--parallel]";
const MIB: f64 = 1024.0 * 1024.0;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// What one partition's traversal read: samples, and a checksum of its
/// values and flags in delivery order.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Read {
    samples: usize,
    checksum: f64,
}

/// Read `cursor` to its end, with the checksum when asked: it touches
/// every value, so the timed reads leave it out.
fn traverse(cursor: &mut dyn VisibilityCursor, checksum: bool) -> Result<Read, Error> {
    let mut buffers = BatchBuffers::default();
    let mut read = Read {
        samples: 0,
        checksum: 0.0,
    };
    while let Some(event) = cursor.next(&mut buffers)? {
        if let ReadEvent::Batch(batch) = event {
            read.samples += batch.samples();
            if !checksum {
                continue;
            }
            if let Some(values) = batch.group.values {
                for value in values.iter().filter(|value| value.is_finite()) {
                    read.checksum += f64::from(value.re) + f64::from(value.im);
                }
            }
            if let Some(flags) = batch.group.flags {
                read.checksum += flags.iter().filter(|flag| **flag).count() as f64;
            }
        }
    }
    Ok(read)
}

/// Read every cursor of `request`, shared by `workers` threads; the reads
/// by partition and the seconds taken.
fn read_parallel(
    observation: &dyn casa_observation::Observation,
    request: &AccessRequest,
    workers: usize,
    checksum: bool,
) -> Result<(Vec<Option<Read>>, f64), Error> {
    let started = Instant::now();
    let traversal = observation.bind_read(request)?;
    let partitions = traversal.cursors.len();
    let queue: Mutex<Vec<(usize, BoundCursor<'_>)>> =
        Mutex::new(traversal.cursors.into_iter().enumerate().rev().collect());
    let results = Mutex::new(vec![None; partitions]);
    std::thread::scope(|scope| -> Result<(), Error> {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| -> Result<(), Error> {
                    loop {
                        let next = queue.lock().map_err(|_| "poisoned")?.pop();
                        let Some((index, mut cursor)) = next else {
                            return Ok(());
                        };
                        let read = traverse(cursor.as_mut(), checksum)?;
                        results.lock().map_err(|_| "poisoned")?[index] = Some(read);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().map_err(|_| "a worker panicked")??;
        }
        Ok(())
    })?;
    let elapsed = started.elapsed().as_secs_f64();
    Ok((results.into_inner().map_err(|_| "poisoned")?, elapsed))
}

fn main() -> Result<(), Error> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or(USAGE)?;
    let (mut open_only, mut batch_mib, mut channel_limit) = (false, 256_usize, usize::MAX);
    let (mut correlations, mut parallel) = (CorrelationRequest::ALL, false);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--open-only" => open_only = true,
            "--batch-mib" => batch_mib = args.next().ok_or(USAGE)?.parse()?,
            "--channels" => channel_limit = args.next().ok_or(USAGE)?.parse()?,
            "--products" => {
                let products = args
                    .next()
                    .ok_or(USAGE)?
                    .split(',')
                    .map(|label| Correlation::from_label(label).ok_or(USAGE))
                    .collect::<Result<Vec<_>, _>>()?;
                correlations = CorrelationRequest {
                    deliver: Deliver::These(products),
                    flags_from: FlagsFrom::Delivered,
                };
            }
            "--parallel" => parallel = true,
            _ => return Err(USAGE.into()),
        }
    }

    let started = Instant::now();
    let observation = open(&path, &OpenOptions::default())?;
    let catalogue = observation.catalogue();
    println!(
        "open {:.3} s: {} partitions",
        started.elapsed().as_secs_f64(),
        catalogue.partitions.len()
    );

    let (mut samples, mut delivered, mut seconds) = (0_usize, 0_usize, 0.0_f64);
    let mut sequential = Vec::new();
    for (index, entry) in catalogue.partitions.iter().enumerate() {
        let request = AccessRequest {
            partitions: vec![PartitionId(u32::try_from(index)?)],
            data_group: entry.data_groups[0].name.clone(),
            roles: Roles::ALL,
            channels: 0..entry.frequencies_hz.len().min(channel_limit),
            correlations: correlations.clone(),
            grouping: Grouping::None,
            batch_bytes: batch_mib << 20,
        };
        let estimate = observation.estimate(&request)?;
        let partition = &estimate.partitions[0];
        println!(
            "partition {index} ({}): {} times x {} baselines x {} channels x {:?}, \
             padded {:.4}; {} B/sample (+{} scratch), {} times/batch, {} batches of at most {:.1} MiB held; \
             {} decode units, decoded/selected {:.3}",
            entry.spectral_window,
            partition.times,
            partition.baselines,
            request.channels.len(),
            entry
                .correlations
                .iter()
                .map(|correlation| correlation.label())
                .collect::<Vec<_>>(),
            partition.padded_fraction,
            partition.bytes_per_sample,
            partition.scratch_bytes_per_sample,
            partition.times_per_batch,
            partition.batches,
            estimate.peak_held_bytes() as f64 / MIB,
            partition.decode_units,
            partition.decoded_elements as f64 / partition.selected_elements.max(1) as f64,
        );
        if open_only {
            continue;
        }
        let started = Instant::now();
        let mut traversal = observation.bind_read(&request)?;
        let result = traverse(traversal.cursors[0].as_mut(), false)?;
        let read = result.samples;
        sequential.push(result);
        let elapsed = started.elapsed().as_secs_f64();
        let counters = traversal.counters();
        println!(
            "  read {read} samples, {:.1} MiB in {elapsed:.3} s ({:.0} MiB/s); \
             {} read calls, {} copied elements, {} buffer growths",
            counters.delivered_bytes as f64 / MIB,
            counters.delivered_bytes as f64 / MIB / elapsed.max(1e-9),
            counters.read_calls,
            counters.copied_elements,
            counters.buffer_growths,
        );
        samples += read;
        delivered += counters.delivered_bytes;
        seconds += elapsed;
    }
    if !open_only {
        println!(
            "total: {samples} samples, {:.1} MiB in {seconds:.3} s ({:.0} MiB/s)",
            delivered as f64 / MIB,
            delivered as f64 / MIB / seconds.max(1e-9),
        );
    }
    if parallel && !open_only {
        let channels = catalogue
            .partitions
            .iter()
            .map(|entry| entry.frequencies_hz.len())
            .min()
            .unwrap_or(0)
            .min(channel_limit);
        let request = AccessRequest {
            partitions: catalogue.partition_ids(),
            data_group: catalogue.partitions[0].data_groups[0].name.clone(),
            roles: Roles::ALL,
            channels: 0..channels,
            correlations,
            grouping: Grouping::None,
            batch_bytes: batch_mib << 20,
        };
        let workers = std::thread::available_parallelism().map_or(1, usize::from);
        let (results, elapsed) = read_parallel(&*observation, &request, workers, false)?;
        // Untimed: the checksums, read concurrently and in order.
        let (concurrent, _) = read_parallel(&*observation, &request, workers, true)?;
        let in_order = observation
            .bind_read(&request)?
            .cursors
            .iter_mut()
            .map(|cursor| traverse(cursor.as_mut(), true).map(Some))
            .collect::<Result<Vec<_>, _>>()?;
        let same = channels
            == catalogue.partitions[0]
                .frequencies_hz
                .len()
                .min(channel_limit)
            && results
                .iter()
                .zip(&sequential)
                .all(|(parallel, sequential)| *parallel == Some(*sequential))
            && concurrent == in_order;
        println!(
            "parallel: {workers} workers, {:.1} MiB in {elapsed:.3} s ({:.0} MiB/s); \
             samples and checksums equal to the sequential read: {same}",
            delivered as f64 / MIB,
            delivered as f64 / MIB / elapsed.max(1e-9),
        );
        if !same {
            return Err("the parallel read differs from the sequential one".into());
        }
    }
    Ok(())
}
