// SPDX-License-Identifier: LGPL-3.0-or-later
//! Times building an [`AbsorptionProfile`] for CASA's simulator atmosphere
//! and N channels over 2 GHz at 345 GHz: the full and compact forms on one
//! thread, and the compact form with its channels computed on every core
//! and assembled with `from_channels`, as a caller's worker team would:
//!
//! ```text
//! cargo run --release -p casa-atmosphere --example profile_timing -- 3840
//! ```
//!
//! `tools/atmosphere/build-atm-oracle.sh <ATM directory> --time-profile N`
//! times ATM's `RefractiveIndexProfile` for the same grid.

use casa_atmosphere::absorption::channel_totals;
use casa_atmosphere::{
    AbsorptionProfile, AtmosphereProfile, ChannelTotals, ProfileParameters, SpectralGrid,
};
use std::time::Instant;

fn main() {
    let channels: usize = std::env::args()
        .nth(1)
        .map_or(2048, |n| n.parse().expect("a channel count"));
    let atmosphere =
        AtmosphereProfile::new(&ProfileParameters::default()).expect("the default profile");
    let mut grid = SpectralGrid::new();
    grid.add_regular(channels, channels / 2, 345.0e9, 2.0e9 / channels as f64)
        .expect("a grid");
    let layers = atmosphere.layers().len();
    let per_channel = |seconds: f64| 1e6 * seconds / channels as f64;

    let start = Instant::now();
    let full = AbsorptionProfile::new(atmosphere.clone(), grid.clone());
    let serial_full = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let compact = AbsorptionProfile::new_totals(atmosphere.clone(), grid.clone());
    let serial = start.elapsed().as_secs_f64();

    let threads = std::thread::available_parallelism().map_or(1, usize::from);
    let start = Instant::now();
    let frequencies: Vec<f64> = grid.frequencies_hz().collect();
    let parts: Vec<Vec<ChannelTotals>> = std::thread::scope(|scope| {
        let handles: Vec<_> = frequencies
            .chunks(channels.div_ceil(threads))
            .map(|chunk| {
                let atmosphere = &atmosphere;
                scope.spawn(move || {
                    chunk
                        .iter()
                        .map(|&f| channel_totals(atmosphere, f))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("a worker"))
            .collect()
    });
    let parallel_profile = AbsorptionProfile::from_channels(
        atmosphere.clone(),
        grid.clone(),
        parts.into_iter().flatten().collect(),
    )
    .expect("matching channels");
    let parallel = start.elapsed().as_secs_f64();
    assert_eq!(parallel_profile, compact);

    let mid = channels / 2;
    println!(
        "rust full, 1 thread: {channels} channels x {layers} layers: {serial_full:.3} s ({:.1} us per channel), {:.1} MB [{}]",
        per_channel(serial_full),
        AbsorptionProfile::<casa_atmosphere::ChannelAbsorption>::bytes(&grid, layers) as f64 / 1e6,
        full.dry_opacity(mid)
    );
    println!(
        "rust compact, 1 thread: {serial:.3} s ({:.1} us per channel), {:.1} MB [{}]",
        per_channel(serial),
        AbsorptionProfile::<ChannelTotals>::bytes(&grid, layers) as f64 / 1e6,
        compact.dry_opacity(mid)
    );
    println!(
        "rust compact, {threads} threads: {parallel:.3} s ({:.1}x)",
        serial / parallel
    );
}
