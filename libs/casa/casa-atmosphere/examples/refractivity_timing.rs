// SPDX-License-Identifier: LGPL-3.0-or-later
//! Times every species' refractivity once per (layer, channel) for the
//! default Chajnantor profile and N channels over 2 GHz at 345 GHz, on one
//! thread and with the channels split over every core:
//!
//! ```text
//! cargo run --release -p casa-atmosphere --example refractivity_timing -- 1024
//! ```
//!
//! `tools/atmosphere/build-atm-oracle.sh <ATM directory> --time 1024` times
//! the same work in CASA's C++.

use casa_atmosphere::refractivity::{Species, specific_refractivity};
use casa_atmosphere::{AtmosphereProfile, ProfileParameters};
use std::time::Instant;

/// Sums the absorption of every species over `channels` of `total`, so the
/// work cannot be optimised away.
fn work(profile: &AtmosphereProfile, channels: std::ops::Range<usize>, total: usize) -> f64 {
    let mut sink = 0.0;
    for c in channels {
        let nu = 344.0 + 2.0 * c as f64 / total as f64;
        for layer in profile.layers() {
            let (t, p) = (layer.temperature_k, layer.pressure_hpa);
            let e = layer.water_vapour_kg_m3 * 1000.0 * t / 217.0;
            for species in Species::ALL {
                sink += specific_refractivity(species, t, p, e, nu).im;
            }
        }
    }
    sink
}

fn main() {
    let channels: usize = std::env::args()
        .nth(1)
        .map_or(1024, |n| n.parse().expect("a channel count"));
    let profile =
        AtmosphereProfile::new(&ProfileParameters::default()).expect("the default profile");
    let layers = profile.layers().len();

    let start = Instant::now();
    let sink = work(&profile, 0..channels, channels);
    let serial = start.elapsed().as_secs_f64();
    println!(
        "rust, 1 thread: {channels} channels x {layers} layers x 25 species: {serial:.3} s ({:.1} us per channel) [{sink:e}]",
        1e6 * serial / channels as f64
    );

    let threads = std::thread::available_parallelism().map_or(1, usize::from);
    let start = Instant::now();
    let chunk = channels.div_ceil(threads);
    let parallel_sink: f64 = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|k| {
                let range = (k * chunk).min(channels)..((k + 1) * chunk).min(channels);
                let profile = &profile;
                scope.spawn(move || work(profile, range, channels))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("a worker"))
            .sum()
    });
    let parallel = start.elapsed().as_secs_f64();
    println!(
        "rust, {threads} threads: {parallel:.3} s ({:.1}x) [{parallel_sink:e}]",
        serial / parallel
    );
}
