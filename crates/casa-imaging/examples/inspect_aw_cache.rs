// SPDX-License-Identifier: LGPL-3.0-or-later
//! Inspect a CASA AWProject convolution-function cache.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    process::ExitCode,
};

use casa_imaging::AwConvolutionFunctionCache;

fn main() -> ExitCode {
    let mut args = env::args_os();
    let program = args.next().unwrap_or_default();
    let Some(path) = args.next() else {
        eprintln!("usage: {} <CASA-CF-CACHE>", program.to_string_lossy());
        return ExitCode::from(2);
    };
    if args.next().is_some() {
        eprintln!("expected exactly one CASA CF-cache path");
        return ExitCode::from(2);
    }

    let cache = match AwConvolutionFunctionCache::open(&path) {
        Ok(cache) => cache,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let inventory = cache.inventory();
    println!("root={}", cache.root().display());
    println!("paired_cells={}", inventory.paired_cells);
    println!("frequency_bins={}", inventory.frequencies_hz.len());
    println!("frequencies_hz={:?}", inventory.frequencies_hz);
    println!("w_bins={}", inventory.w_values_lambda.len());
    println!("mueller_elements={:?}", inventory.mueller_elements);
    println!(
        "parallactic_angle_bins={}",
        inventory.parallactic_angles_deg.len()
    );
    let mut polarizations_by_mueller = BTreeMap::new();
    let mut conjugate_polarizations_by_mueller = BTreeMap::new();
    let mut conjugate_frequency_by_frequency = BTreeMap::new();
    for key in cache.keys() {
        let metadata = cache
            .metadata(key)
            .expect("validated cache key must have metadata");
        polarizations_by_mueller
            .entry(key.mueller_element)
            .or_insert_with(BTreeSet::new)
            .insert(format!("{:?}", metadata.imaging.polarization));
        conjugate_polarizations_by_mueller
            .entry(key.mueller_element)
            .or_insert_with(BTreeSet::new)
            .insert(metadata.imaging.conjugate_polarization);
        conjugate_frequency_by_frequency
            .entry(key.frequency_hz.to_bits())
            .or_insert(metadata.imaging.conjugate_frequency_hz);
    }
    println!("coordinate_polarizations_by_mueller={polarizations_by_mueller:?}");
    println!("conjugate_polarizations_by_mueller={conjugate_polarizations_by_mueller:?}");
    println!(
        "conjugate_frequency_by_frequency={:?}",
        conjugate_frequency_by_frequency
            .into_iter()
            .map(|(frequency_bits, conjugate_frequency_hz)| {
                (f64::from_bits(frequency_bits), conjugate_frequency_hz)
            })
            .collect::<Vec<_>>()
    );

    let Some(first_key) = cache.keys().first().copied() else {
        eprintln!("validated cache unexpectedly has no keys");
        return ExitCode::FAILURE;
    };
    let cell = match cache.load(first_key) {
        Ok(cell) => cell,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    println!("first_key={first_key:?}");
    println!("first_imaging_shape={:?}", cell.imaging.dim());
    println!("first_weight_shape={:?}", cell.weight.dim());
    ExitCode::SUCCESS
}
