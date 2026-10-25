// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv2/MSv4 trial's scale probe (`docs/measurement-set/trial/README.md`):
//! opens a store through the trial adapters and reports what opening cost,
//! what the estimate predicts for each partition, and what one traversal of
//! every partition reads.
//!
//! ```text
//! cargo run --release -p casa-ms --example ms_trial_probe -- STORE [--open-only] [--batch-mib N] [--channels N]
//! ```
//!
//! Run it under `/usr/bin/time -l` (macOS) or `/usr/bin/time -v` (Linux) for
//! the peak resident memory; `--open-only` separates the opening index from
//! the traversal. Each partition is read in its first data group, every
//! role and product, its first `--channels` channels (default all), with a
//! `--batch-mib` budget (default 256).

use std::time::Instant;

use casa_ms::observation_io::{OpenOptions, open};
use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, Grouping, PartitionId, ReadEvent, Roles,
};

const USAGE: &str = "usage: ms_trial_probe STORE [--open-only] [--batch-mib N] [--channels N]";
const MIB: f64 = 1024.0 * 1024.0;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or(USAGE)?;
    let (mut open_only, mut batch_mib, mut channel_limit) = (false, 256_usize, usize::MAX);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--open-only" => open_only = true,
            "--batch-mib" => batch_mib = args.next().ok_or(USAGE)?.parse()?,
            "--channels" => channel_limit = args.next().ok_or(USAGE)?.parse()?,
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
    for (index, entry) in catalogue.partitions.iter().enumerate() {
        let request = AccessRequest {
            partitions: vec![PartitionId(u32::try_from(index)?)],
            data_group: entry.data_groups[0].name.clone(),
            roles: Roles::ALL,
            channels: 0..entry.frequencies_hz.len().min(channel_limit),
            correlations: CorrelationRequest::ALL,
            grouping: Grouping::None,
            batch_bytes: batch_mib << 20,
        };
        let estimate = observation.estimate(&request)?;
        let partition = &estimate.partitions[0];
        println!(
            "partition {index} ({}): {} times x {} baselines x {} channels x {} products, \
             padded {:.4}; {} B/sample (+{} scratch), {} times/batch, {} batches of at most {:.1} MiB held; \
             {} decode units, decoded/selected {:.3}",
            entry.spectral_window,
            partition.times,
            partition.baselines,
            request.channels.len(),
            entry.correlations.len(),
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
        let mut buffers = BatchBuffers::default();
        let mut read = 0;
        while let Some(event) = traversal.next(&mut buffers)? {
            if let ReadEvent::Batch(batch) = event {
                read += batch.shape()[0];
            }
        }
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
    Ok(())
}
